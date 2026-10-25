//! Backend-independent semantics shared by every adapter.
//!
//! A [`Backend`] is a thin transport over an `object_store` implementation.
//! [`Core`] layers the contract from `lib.rs` on top: digest verification,
//! idempotent create, metadata validation, cursor pagination and error mapping.
//! The public adapters are newtypes over `Core<SomeBackend>`.

use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{
    GetOptions, GetRange, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    UpdateVersion,
};
use sha2::{Digest, Sha256};
use tracing::Instrument;
use url::Url;

use crate::codec::{infer_content_type, sha256_hex};
use crate::metadata::{attributes, checked_user_metadata, user_metadata, with_digest};
use crate::{
    ByteStream, Error, Metadata, MultipartOpts, MultipartUpload, ObjectKey, ObjectMeta,
    ObjectVersion, Page, PutAck, PutOpts, Result, DEFAULT_PAGE_SIZE, MAX_PRESIGN_TTL,
    META_CONTENT_SHA256, META_TRACE_ID,
};

/// Transport hooks. Defaults go straight to `object_store`; `FsStore`
/// overrides the ones the local filesystem implementation lacks.
#[async_trait]
pub(crate) trait Backend: Send + Sync + std::fmt::Debug + 'static {
    const NAME: &'static str;

    /// Separator used for `-` in metadata keys on the wire. Azure metadata
    /// names must be C# identifiers, so it stores `trace-id` as `trace_id`;
    /// the adapter translates both ways so callers always see `trace-id`.
    const META_SEPARATOR: char = '-';

    fn store(&self) -> &dyn object_store::ObjectStore;

    async fn put_object(
        &self,
        path: &Path,
        body: Bytes,
        mode: PutMode,
        metadata: &Metadata,
        content_type: &str,
    ) -> object_store::Result<PutResult> {
        let opts = PutOptions {
            mode,
            attributes: attributes(metadata, content_type, Self::META_SEPARATOR),
            ..PutOptions::default()
        };
        self.store()
            .put_opts(path, PutPayload::from_bytes(body), opts)
            .await
    }

    /// Object attributes plus user metadata.
    async fn head_object(
        &self,
        path: &Path,
    ) -> object_store::Result<(object_store::ObjectMeta, Metadata)> {
        let opts = GetOptions {
            head: true,
            ..GetOptions::default()
        };
        let result = self.store().get_opts(path, opts).await?;
        Ok((
            result.meta.clone(),
            user_metadata(&result.attributes, Self::META_SEPARATOR),
        ))
    }

    /// Objects under `prefix` with keys strictly greater than `offset`, in
    /// ascending key order. Cloud listings are already ordered.
    fn list_objects(
        &self,
        prefix: &Path,
        offset: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        match offset {
            Some(offset) => self.store().list_with_offset(Some(prefix), offset),
            None => self.store().list(Some(prefix)),
        }
    }

    async fn presign(&self, path: &Path, ttl: Duration) -> Result<Url>;

    async fn start_multipart(
        &self,
        path: &Path,
        metadata: &Metadata,
        content_type: &str,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        let opts = PutMultipartOptions {
            attributes: attributes(metadata, content_type, Self::META_SEPARATOR),
            ..PutMultipartOptions::default()
        };
        self.store().put_multipart_opts(path, opts).await
    }

    /// Called after a multipart upload committed.
    async fn finish_multipart(&self, _path: &Path, _metadata: &Metadata) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct Core<B> {
    backend: Arc<B>,
    page_size: usize,
}

impl<B: Backend> Core<B> {
    pub(crate) fn new(backend: B) -> Self {
        Self {
            backend: Arc::new(backend),
            page_size: DEFAULT_PAGE_SIZE,
        }
    }

    pub(crate) fn name(&self) -> &'static str {
        B::NAME
    }

    pub(crate) fn backend(&self) -> &B {
        &self.backend
    }

    /// Override the page size (tests use small pages to exercise cursors).
    pub(crate) fn set_page_size(&mut self, page_size: usize) {
        self.page_size = page_size.clamp(1, DEFAULT_PAGE_SIZE);
    }

    fn err(&self, key: &ObjectKey, err: object_store::Error) -> Error {
        Error::from_store(B::NAME, key.as_str(), err)
    }

    pub(crate) async fn put(&self, key: &ObjectKey, body: Bytes, opts: PutOpts) -> Result<PutAck> {
        let trace_id = opts
            .metadata
            .get(META_TRACE_ID)
            .cloned()
            .unwrap_or_default();
        let span = tracing::info_span!(
            "deliver.put",
            trace_id = %trace_id,
            backend = B::NAME,
            key = %key,
            size = body.len(),
            created = tracing::field::Empty,
        );
        let ack = self
            .put_inner(key, body, opts)
            .instrument(span.clone())
            .await?;
        span.record("created", ack.created);
        Ok(ack)
    }

    async fn put_inner(&self, key: &ObjectKey, body: Bytes, opts: PutOpts) -> Result<PutAck> {
        let mode = match (opts.if_none_match, opts.if_match) {
            (true, Some(_)) => {
                return Err(Error::InvalidArgument(
                    "if_none_match and if_match are mutually exclusive".into(),
                ))
            }
            (true, None) => PutMode::Create,
            (false, Some(v)) => PutMode::Update(UpdateVersion {
                e_tag: v.e_tag,
                version: v.version,
            }),
            (false, None) => PutMode::Overwrite,
        };
        let sha = sha256_hex(&body);
        if let Some(expected) = opts.content_sha256 {
            if !expected.eq_ignore_ascii_case(&sha) {
                return Err(Error::ContentMismatch {
                    key: key.to_string(),
                    expected,
                    actual: sha,
                });
            }
        }
        let metadata = with_digest(opts.metadata, &sha)?;
        let content_type = opts
            .content_type
            .unwrap_or_else(|| infer_content_type(key.as_str()).to_string());
        let size = body.len() as u64;
        let path = key.to_path();
        let is_create = matches!(mode, PutMode::Create);
        let is_update = matches!(mode, PutMode::Update(_));

        match self
            .backend
            .put_object(&path, body, mode, &metadata, &content_type)
            .await
        {
            Ok(result) => Ok(PutAck {
                key: key.clone(),
                version: version_of_put(&result),
                content_sha256: sha,
                size,
                created: true,
            }),
            // S3 answers a failed `If-None-Match: *` with 412, so a create can
            // surface as either variant depending on the backend.
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) if is_create => self.reconcile_existing(key, &sha, size).await,
            // S3 reports `If-Match` against a missing object as 404; the
            // contract is "your version is stale" either way.
            Err(object_store::Error::NotFound { .. }) if is_update => {
                Err(Error::PreconditionFailed {
                    key: key.to_string(),
                })
            }
            Err(err) => Err(self.err(key, err)),
        }
    }

    /// An `if_none_match` put hit an existing object: succeed if it holds the
    /// same bytes, otherwise report the conflict.
    async fn reconcile_existing(&self, key: &ObjectKey, sha: &str, size: u64) -> Result<PutAck> {
        let Some(existing) = self.head(key).await? else {
            // Deleted between the failed create and the head; the caller's
            // retry will create it.
            return Err(Error::Transient {
                backend: B::NAME,
                source: format!("{key} vanished while reconciling a create").into(),
            });
        };
        let stored_sha = match existing.content_sha256() {
            Some(stored) => stored.to_string(),
            // Written by another tool or a backend that drops metadata: hash
            // the stored body instead. Chunks are bounded, so this is cheap.
            None => sha256_hex(&self.read_all(key).await?),
        };
        if existing.size == size && stored_sha.eq_ignore_ascii_case(sha) {
            tracing::debug!(key = %key, "idempotent re-put of identical content");
            Ok(PutAck {
                key: key.clone(),
                version: existing.version,
                content_sha256: sha.to_string(),
                size,
                created: false,
            })
        } else {
            Err(Error::AlreadyExists {
                key: key.to_string(),
            })
        }
    }

    async fn read_all(&self, key: &ObjectKey) -> Result<Bytes> {
        let chunks: Vec<Bytes> = self.get(key, None).await?.try_collect().await?;
        Ok(chunks.concat().into())
    }

    pub(crate) async fn head(&self, key: &ObjectKey) -> Result<Option<ObjectMeta>> {
        let span = tracing::debug_span!("objstore.head", backend = B::NAME, key = %key);
        match self
            .backend
            .head_object(&key.to_path())
            .instrument(span)
            .await
        {
            Ok((meta, metadata)) => Ok(Some(object_meta(key.clone(), &meta, metadata))),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(err) => Err(self.err(key, err)),
        }
    }

    pub(crate) async fn get(
        &self,
        key: &ObjectKey,
        range: Option<Range<u64>>,
    ) -> Result<ByteStream> {
        if let Some(r) = &range {
            if r.start >= r.end {
                return Err(Error::InvalidArgument(format!("empty range {r:?}")));
            }
        }
        let span = tracing::info_span!("fetch", backend = B::NAME, key = %key, range = ?range);
        let opts = GetOptions {
            range: range.map(GetRange::Bounded),
            ..GetOptions::default()
        };
        let result = self
            .backend
            .store()
            .get_opts(&key.to_path(), opts)
            .instrument(span)
            .await
            .map_err(|e| self.err(key, e))?;
        let key = key.to_string();
        Ok(result
            .into_stream()
            .map_err(move |e| Error::from_store(B::NAME, &key, e))
            .boxed())
    }

    pub(crate) async fn list(
        &self,
        prefix: &ObjectKey,
        cursor: Option<String>,
    ) -> Result<Page<ObjectMeta>> {
        let offset = cursor.map(ObjectKey::new).transpose()?;
        let offset_path = offset.as_ref().map(ObjectKey::to_path);
        let mut stream = self
            .backend
            .list_objects(&prefix.to_path(), offset_path.as_ref());
        // Read one past the page to learn whether another page exists.
        let mut items = Vec::with_capacity(self.page_size + 1);
        while items.len() <= self.page_size {
            match stream.next().await {
                Some(meta) => {
                    let meta = meta.map_err(|e| self.err(prefix, e))?;
                    match ObjectKey::from_path(&meta.location) {
                        Ok(key) => items.push(object_meta(key, &meta, Metadata::new())),
                        // Written by another tool; it cannot be addressed
                        // through this contract, so it must not end the listing.
                        Err(err) => tracing::warn!(%err, "skipping unaddressable object"),
                    }
                }
                None => break,
            }
        }
        let next_cursor = if items.len() > self.page_size {
            items.truncate(self.page_size);
            items.last().map(|m| m.key.to_string())
        } else {
            None
        };
        Ok(Page { items, next_cursor })
    }

    pub(crate) async fn presign_get(&self, key: &ObjectKey, ttl: Duration) -> Result<Url> {
        if ttl.is_zero() || ttl > MAX_PRESIGN_TTL {
            return Err(Error::InvalidArgument(format!(
                "presign ttl must be in (0, {}s], got {}s",
                MAX_PRESIGN_TTL.as_secs(),
                ttl.as_secs()
            )));
        }
        let span = tracing::info_span!("fetch", backend = B::NAME, key = %key, presign = true);
        self.backend
            .presign(&key.to_path(), ttl)
            .instrument(span)
            .await
    }

    pub(crate) async fn multipart(
        &self,
        key: &ObjectKey,
        opts: MultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        let expected_sha = opts.content_sha256.map(|s| s.to_ascii_lowercase());
        let mut metadata = checked_user_metadata(opts.metadata)?;
        if let Some(sha) = &expected_sha {
            metadata.insert(META_CONTENT_SHA256.into(), sha.clone());
        }
        let content_type = opts
            .content_type
            .unwrap_or_else(|| infer_content_type(key.as_str()).to_string());
        let path = key.to_path();
        let inner = self
            .backend
            .start_multipart(&path, &metadata, &content_type)
            .await
            .map_err(|e| self.err(key, e))?;
        Ok(Box::new(CoreUpload {
            backend: Arc::clone(&self.backend),
            inner,
            key: key.clone(),
            path,
            metadata,
            expected_sha,
            hasher: Sha256::new(),
            size: 0,
        }))
    }
}

struct CoreUpload<B> {
    backend: Arc<B>,
    inner: Box<dyn object_store::MultipartUpload>,
    key: ObjectKey,
    path: Path,
    metadata: Metadata,
    expected_sha: Option<String>,
    hasher: Sha256,
    size: u64,
}

#[async_trait]
impl<B: Backend> MultipartUpload for CoreUpload<B> {
    async fn put_part(&mut self, part: Bytes) -> Result<()> {
        if part.is_empty() {
            return Err(Error::InvalidArgument("empty multipart part".into()));
        }
        self.hasher.update(&part);
        self.size += part.len() as u64;
        self.inner
            .put_part(PutPayload::from_bytes(part))
            .await
            .map_err(|e| Error::from_store(B::NAME, self.key.as_str(), e))
    }

    async fn complete(mut self: Box<Self>) -> Result<PutAck> {
        let trace_id = self
            .metadata
            .get(META_TRACE_ID)
            .cloned()
            .unwrap_or_default();
        let span = tracing::info_span!(
            "deliver.put",
            trace_id = %trace_id,
            backend = B::NAME,
            key = %self.key,
            size = self.size,
            multipart = true,
        );
        async move {
            let sha = hex::encode(std::mem::take(&mut self.hasher).finalize());
            if let Some(expected) = self.expected_sha.take() {
                if !expected.eq_ignore_ascii_case(&sha) {
                    // Never commit an object whose metadata would lie about
                    // its digest.
                    let _ = self.inner.abort().await;
                    return Err(Error::ContentMismatch {
                        key: self.key.to_string(),
                        expected,
                        actual: sha,
                    });
                }
            }
            let result = self
                .inner
                .complete()
                .await
                .map_err(|e| Error::from_store(B::NAME, self.key.as_str(), e))?;
            self.backend
                .finish_multipart(&self.path, &self.metadata)
                .await?;
            Ok(PutAck {
                key: self.key.clone(),
                version: version_of_put(&result),
                content_sha256: sha,
                size: self.size,
                created: true,
            })
        }
        .instrument(span)
        .await
    }

    async fn abort(mut self: Box<Self>) -> Result<()> {
        self.inner
            .abort()
            .await
            .map_err(|e| Error::from_store(B::NAME, self.key.as_str(), e))
    }
}

fn version_of_put(result: &PutResult) -> ObjectVersion {
    ObjectVersion {
        e_tag: result.e_tag.clone(),
        version: result.version.clone(),
    }
}

pub(crate) fn object_meta(
    key: ObjectKey,
    meta: &object_store::ObjectMeta,
    metadata: Metadata,
) -> ObjectMeta {
    ObjectMeta {
        key,
        size: meta.size,
        last_modified: meta.last_modified,
        version: ObjectVersion {
            e_tag: meta.e_tag.clone(),
            version: meta.version.clone(),
        },
        metadata,
    }
}
