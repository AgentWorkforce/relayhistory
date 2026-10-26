//! Object-storage destination adapters for RelayHistory raw session delivery.
//!
//! One contract, [`ObjectStore`], with four implementations:
//!
//! | Type               | Backend                                   | Feature |
//! | ------------------ | ----------------------------------------- | ------- |
//! | [`S3Store`]        | Amazon S3, Cloudflare R2, MinIO, any S3 API | `s3`    |
//! | [`GcsStore`]       | Google Cloud Storage                      | `gcs`   |
//! | [`AzureBlobStore`] | Azure Blob Storage                        | `azure` |
//! | [`FsStore`]        | Local directory (tests, air-gapped hosts) | always  |
//!
//! The cloud transports come from the Apache `object_store` crate; this crate
//! adds the semantics the delivery coordinator relies on and that `object_store`
//! leaves to callers:
//!
//! - **Idempotent append-only chunks.** `put` with `if_none_match` that finds an
//!   existing object with the *same* SHA-256 succeeds with
//!   [`PutAck::created`]` == false`; different content is
//!   [`Error::AlreadyExists`]. Re-delivering a chunk after a crash is therefore
//!   always safe.
//! - **Compare-and-swap manifests.** `put` with `if_match` maps to `If-Match`
//!   (S3, Azure), `ifGenerationMatch` (GCS) or a file lock (filesystem), and a
//!   lost race is [`Error::PreconditionFailed`].
//! - **Integrity.** Every object carries its SHA-256 in object metadata
//!   ([`META_CONTENT_SHA256`]); a caller-supplied digest is checked before any
//!   byte leaves the process.
//! - **Tracing.** Metadata carries the session `trace_id` ([`META_TRACE_ID`],
//!   surfaced as `x-amz-meta-trace-id` / `x-goog-meta-trace-id` /
//!   `x-ms-meta-trace_id` — Azure metadata names must be C# identifiers, so
//!   the Azure adapter maps `-` to `_` on the wire and back on read), and calls
//!   run inside `deliver.put` / `fetch` spans.
//!
//! This crate is a delivery-side plugin. It is not a member of the root
//! workspace and must never be linked into the local read/write hot path.

mod codec;
mod core;
mod credentials;
mod delegate;
mod error;
mod fs;
mod key;
pub mod layout;
pub mod manifest;
mod metadata;
mod upload;

#[cfg(feature = "azure")]
mod azure;
#[cfg(feature = "gcs")]
mod gcs;
#[cfg(feature = "s3")]
mod s3;

#[cfg(feature = "conformance")]
pub mod conformance;

use std::collections::BTreeMap;
use std::ops::Range;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use url::Url;

#[cfg(feature = "azure")]
pub use azure::{AzureBlobConfig, AzureBlobStore, AzureCredentials};
pub use codec::{compress_zstd, decompress_zstd, sha256_hex, CONTENT_TYPE_ZSTD};
pub use credentials::{CredentialScope, CredentialVendor, VendedCredential};
pub use error::{Error, Result};
pub use fs::{FsConfig, FsStore};
#[cfg(feature = "gcs")]
pub use gcs::{GcsConfig, GcsCredentials, GcsStore};
pub use key::ObjectKey;
#[cfg(feature = "s3")]
pub use s3::{S3Config, S3Credentials, S3Store};

/// Metadata key carrying the session trace id (UUIDv7).
pub const META_TRACE_ID: &str = "trace-id";
/// Metadata key carrying the lowercase hex SHA-256 of the stored body. Written
/// by every adapter; callers cannot override it.
pub const META_CONTENT_SHA256: &str = "content-sha256";

/// Longest presigned URL lifetime accepted (the S3 SigV4 ceiling).
pub const MAX_PRESIGN_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Default and maximum number of entries in one [`Page`].
pub const DEFAULT_PAGE_SIZE: usize = 1000;

/// A stream of body bytes from [`ObjectStore::get`].
pub type ByteStream = BoxStream<'static, Result<Bytes>>;

/// User metadata stored with an object. Keys are lowercase `[a-z][a-z0-9-]*`.
pub type Metadata = BTreeMap<String, String>;

/// Opaque version token of a stored object, used for `if_match`.
///
/// S3 and Azure compare `e_tag`; GCS compares `version` (the object
/// generation); the filesystem compares `e_tag`. Treat the fields as opaque and
/// pass back what [`ObjectStore::head`] or [`ObjectStore::put`] returned.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ObjectVersion {
    pub e_tag: Option<String>,
    pub version: Option<String>,
}

/// Options for [`ObjectStore::put`].
#[derive(Debug, Clone, Default)]
pub struct PutOpts {
    /// Create only: fail unless no object exists (`If-None-Match: *`). An
    /// existing object with identical content is an idempotent success.
    pub if_none_match: bool,
    /// Replace only the given version (compare-and-swap).
    pub if_match: Option<ObjectVersion>,
    /// Expected lowercase hex SHA-256 of `body`; verified before upload.
    pub content_sha256: Option<String>,
    /// User metadata (see [`META_TRACE_ID`]).
    pub metadata: Metadata,
    /// Content type; inferred from the key extension when absent
    /// (`.zst` → `application/zstd`, `.json` → `application/json`).
    pub content_type: Option<String>,
}

impl PutOpts {
    /// Create-only put, the mode for immutable raw chunks.
    pub fn create() -> Self {
        Self {
            if_none_match: true,
            ..Self::default()
        }
    }

    /// Compare-and-swap put, the mode for the mutable manifest.
    pub fn replace(version: ObjectVersion) -> Self {
        Self {
            if_match: Some(version),
            ..Self::default()
        }
    }

    pub fn with_trace_id(mut self, trace_id: impl Into<String>) -> Self {
        self.metadata.insert(META_TRACE_ID.into(), trace_id.into());
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

/// Options for [`ObjectStore::multipart`]. Multipart uploads are always
/// unconditional: S3, GCS and Azure do not offer a portable precondition on
/// completion, so use them for large immutable objects whose key is already
/// content-addressed.
#[derive(Debug, Clone, Default)]
pub struct MultipartOpts {
    /// Expected lowercase hex SHA-256 of the assembled object. Stored as
    /// [`META_CONTENT_SHA256`] (metadata must be fixed when the upload
    /// starts) and verified before completing; on mismatch the upload is
    /// aborted. Without it the object carries no stored digest.
    pub content_sha256: Option<String>,
    pub metadata: Metadata,
    pub content_type: Option<String>,
}

/// Acknowledgment of a durable write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutAck {
    pub key: ObjectKey,
    pub version: ObjectVersion,
    /// Lowercase hex SHA-256 of the stored body.
    pub content_sha256: String,
    pub size: u64,
    /// `false` when an `if_none_match` put found identical content already
    /// stored (an idempotent re-put); the object was not rewritten.
    pub created: bool,
}

/// Object attributes from [`ObjectStore::head`] or [`ObjectStore::list`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    pub key: ObjectKey,
    pub size: u64,
    pub last_modified: DateTime<Utc>,
    pub version: ObjectVersion,
    /// User metadata. Populated by `head`; empty in `list` results (no
    /// backend returns user metadata from a listing).
    pub metadata: Metadata,
}

impl ObjectMeta {
    pub fn content_sha256(&self) -> Option<&str> {
        self.metadata.get(META_CONTENT_SHA256).map(String::as_str)
    }

    pub fn trace_id(&self) -> Option<&str> {
        self.metadata.get(META_TRACE_ID).map(String::as_str)
    }
}

/// Body and attributes from one [`ObjectStore::get_object`] response.
pub struct GetObject {
    /// Attributes of the whole object (not of the range), including its
    /// version and user metadata.
    pub meta: ObjectMeta,
    pub body: ByteStream,
}

impl std::fmt::Debug for GetObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GetObject")
            .field("meta", &self.meta)
            .finish_non_exhaustive()
    }
}

/// One page of a listing. Pass `next_cursor` back to continue; `None` means
/// the listing is complete. Cursors are opaque.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

/// An in-progress multipart upload. Every part except the last must be at
/// least 5 MiB (the S3 minimum). Dropping without `complete` or `abort` leaves
/// an incomplete upload for the bucket lifecycle rule to collect.
#[async_trait]
pub trait MultipartUpload: Send {
    /// Upload the next part. Parts are assembled in call order.
    async fn put_part(&mut self, part: Bytes) -> Result<()>;
    /// Commit the object.
    async fn complete(self: Box<Self>) -> Result<PutAck>;
    /// Discard uploaded parts.
    async fn abort(self: Box<Self>) -> Result<()>;
}

/// The storage contract the object-storage destination is written against.
#[async_trait]
pub trait ObjectStore: Send + Sync + std::fmt::Debug {
    /// Backend name for logs and errors (`s3`, `gcs`, `azure`, `fs`).
    fn backend(&self) -> &'static str;

    async fn put(&self, key: &ObjectKey, body: Bytes, opts: PutOpts) -> Result<PutAck>;

    /// `Ok(None)` when the object does not exist.
    async fn head(&self, key: &ObjectKey) -> Result<Option<ObjectMeta>>;

    /// Stream the body, or the half-open byte `range` of it.
    /// [`Error::NotFound`] when the object does not exist.
    async fn get(&self, key: &ObjectKey, range: Option<Range<u64>>) -> Result<ByteStream> {
        Ok(self.get_object(key, range).await?.body)
    }

    /// Like [`ObjectStore::get`], plus the attributes and version **of the
    /// same response**. A compare-and-swap writer (the manifest protocol) must
    /// take `meta.version` from here: a separate `head` could describe a newer
    /// version than the body it read, and the next `if_match` write would then
    /// silently discard the other writer's change.
    async fn get_object(&self, key: &ObjectKey, range: Option<Range<u64>>) -> Result<GetObject>;

    /// List objects whose key starts with `prefix` on a `/` boundary, in
    /// lexicographic key order, at most [`DEFAULT_PAGE_SIZE`] per page.
    async fn list(&self, prefix: &ObjectKey, cursor: Option<String>) -> Result<Page<ObjectMeta>>;

    /// A URL that grants `GET` on `key` for `ttl` (at most [`MAX_PRESIGN_TTL`]).
    async fn presign_get(&self, key: &ObjectKey, ttl: Duration) -> Result<Url>;

    async fn multipart(
        &self,
        key: &ObjectKey,
        opts: MultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>>;
}
