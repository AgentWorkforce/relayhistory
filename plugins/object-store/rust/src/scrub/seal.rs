//! The sanctioned path from a scrubbed chunk to object storage.
//!
//! [`put_raw_chunk`] accepts only a [`SealedChunk`], which only
//! [`SealedChunk::seal`] builds, and only from a [`ScrubbedChunk`], which only
//! [`super::scrub_chunk`] builds. A raw chunk uploaded through this path has
//! therefore always been scrubbed. The generic [`ObjectStore::put`] stays
//! available for manifests and other objects; raw chunks must not use it.

use bytes::Bytes;

use super::{ScrubbedChunk, RULESET, RULESET_VERSION};
use crate::{
    codec, Error, Metadata, ObjectKey, ObjectStore, PutAck, PutOpts, Result, CONTENT_TYPE_ZSTD,
};

/// Metadata key: the rule set the stored chunk went through (`raw.scrub.ruleset`).
pub const META_SCRUB_RULESET: &str = "scrub-ruleset";
/// Metadata key: the rule set version (`raw.scrub.version`).
pub const META_SCRUB_VERSION: &str = "scrub-version";
/// Metadata key: SHA-256 of the decompressed stored chunk (`chunk.raw_sha256`).
pub const META_RAW_SHA256: &str = "raw-sha256";
/// Metadata key: length of the decompressed stored chunk (`chunk.stored_raw_bytes`).
pub const META_RAW_BYTES: &str = "raw-bytes";

/// A scrubbed chunk, compressed and hashed: the stored body and, for its
/// manifest chunk entry, both the **source** fields (the unscrubbed range,
/// which drive offsets, epochs and rewrite detection) and the **stored**
/// fields (what was uploaded). Never swap them: `bytes`/`sha256` are always
/// the source values. Opaque; built only by [`SealedChunk::seal`].
///
/// | Accessor | Manifest field |
/// | --- | --- |
/// | [`source_bytes`](Self::source_bytes) | `chunk.bytes` (source) |
/// | [`source_sha256`](Self::source_sha256) | `chunk.sha256` (source; `sha256_8` derives from it) |
/// | [`stored_bytes`](Self::stored_bytes) | `chunk.stored_bytes` |
/// | [`stored_sha256`](Self::stored_sha256) | `chunk.stored_sha256` |
/// | [`stored_raw_bytes`](Self::stored_raw_bytes) | `chunk.stored_raw_bytes` |
/// | [`raw_sha256`](Self::raw_sha256) | `chunk.raw_sha256` |
/// | [`records`](Self::records) | `chunk.records` |
/// | [`scrub`](Self::scrub) | `raw.scrub` |
///
/// `chunk.start_offset` is the caller's: where the source range starts.
#[derive(Clone, PartialEq, Eq)]
pub struct SealedChunk {
    stored: Bytes,
    source_bytes: u64,
    source_sha256: String,
    stored_sha256: String,
    stored_raw_bytes: u64,
    raw_sha256: String,
    records: u64,
}

/// Lengths and counts only, like [`ScrubbedChunk`]'s: no body or digests.
impl std::fmt::Debug for SealedChunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SealedChunk")
            .field("stored_bytes", &self.stored.len())
            .field("stored_raw_bytes", &self.stored_raw_bytes)
            .field("source_bytes", &self.source_bytes)
            .field("records", &self.records)
            .finish_non_exhaustive()
    }
}

impl SealedChunk {
    /// Compress a scrubbed chunk and compute its digests. Fails on an empty
    /// chunk, which a manifest cannot list.
    pub fn seal(chunk: &ScrubbedChunk) -> Result<Self> {
        let raw = chunk.bytes();
        if raw.is_empty() {
            return Err(Error::InvalidArgument("cannot seal an empty chunk".into()));
        }
        let stored = codec::compress_zstd(raw)
            .map_err(|e| Error::InvalidArgument(format!("compressing the chunk failed: {e}")))?;
        Ok(Self {
            source_bytes: chunk.source_bytes(),
            source_sha256: chunk.source_sha256().to_string(),
            stored_sha256: codec::sha256_hex(&stored),
            stored_raw_bytes: raw.len() as u64,
            raw_sha256: codec::sha256_hex(raw),
            records: chunk.records(),
            stored,
        })
    }

    /// `chunk.bytes`: length of the unscrubbed source range.
    pub fn source_bytes(&self) -> u64 {
        self.source_bytes
    }

    /// `chunk.sha256`: SHA-256 of the unscrubbed source range.
    pub fn source_sha256(&self) -> &str {
        &self.source_sha256
    }

    /// The compressed body to upload.
    pub fn stored(&self) -> &Bytes {
        &self.stored
    }

    /// `chunk.stored_bytes`: length of the compressed body.
    pub fn stored_bytes(&self) -> u64 {
        self.stored.len() as u64
    }

    /// `chunk.stored_sha256`: SHA-256 of the compressed body.
    pub fn stored_sha256(&self) -> &str {
        &self.stored_sha256
    }

    /// `chunk.stored_raw_bytes`: length of the scrubbed records.
    pub fn stored_raw_bytes(&self) -> u64 {
        self.stored_raw_bytes
    }

    /// `chunk.raw_sha256`: SHA-256 of the scrubbed records.
    pub fn raw_sha256(&self) -> &str {
        &self.raw_sha256
    }

    /// `chunk.records`.
    pub fn records(&self) -> u64 {
        self.records
    }

    /// `(raw.scrub.ruleset, raw.scrub.version)` for the manifest.
    pub fn scrub(&self) -> (&'static str, u32) {
        (RULESET, RULESET_VERSION)
    }
}

/// Upload a sealed raw chunk, create-only. A retry that finds identical
/// content succeeds with `created: false`; different content under the key
/// is [`Error::AlreadyExists`]. In that case keep the stored object, as the
/// manifest spec requires: read its `raw-sha256` and `scrub-version`
/// metadata and record those, never this chunk's.
///
/// `metadata` (for example [`crate::META_TRACE_ID`]) is stored too; the scrub
/// keys above are set here and cannot be overridden.
pub async fn put_raw_chunk(
    store: &dyn ObjectStore,
    key: &ObjectKey,
    chunk: &SealedChunk,
    mut metadata: Metadata,
) -> Result<PutAck> {
    metadata.insert(META_SCRUB_RULESET.into(), RULESET.into());
    metadata.insert(META_SCRUB_VERSION.into(), RULESET_VERSION.to_string());
    metadata.insert(META_RAW_SHA256.into(), chunk.raw_sha256.clone());
    metadata.insert(META_RAW_BYTES.into(), chunk.stored_raw_bytes.to_string());
    store
        .put(
            key,
            chunk.stored.clone(),
            PutOpts {
                if_none_match: true,
                content_sha256: Some(chunk.stored_sha256.clone()),
                metadata,
                content_type: Some(CONTENT_TYPE_ZSTD.into()),
                ..Default::default()
            },
        )
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FsConfig, FsStore};

    #[tokio::test]
    async fn a_sealed_chunk_is_stored_scrubbed_with_its_digests() {
        let raw = b"{\"m\":\"key sk-abcdefghijklmnopqrstuvwxyz0123\"}\n";
        let sealed = SealedChunk::seal(&super::super::scrub_chunk(raw)).unwrap();
        // Source fields describe the unscrubbed input; stored fields what was uploaded.
        assert_eq!(sealed.source_bytes(), raw.len() as u64);
        assert_eq!(sealed.source_sha256(), codec::sha256_hex(raw));
        assert_ne!(
            sealed.source_sha256(),
            sealed.raw_sha256(),
            "scrubbing changed the bytes"
        );
        // Debug output carries no content and no digests.
        let dbg = format!("{sealed:?} {:?}", super::super::scrub_chunk(raw));
        assert!(
            !dbg.contains(sealed.source_sha256()) && !dbg.contains("REDACTED"),
            "{dbg}"
        );
        let dir = tempfile::tempdir().unwrap();
        let store = FsStore::new(FsConfig::new(dir.path())).unwrap();
        let key = ObjectKey::new("s/raw/0000000000-0-aaaaaaaa.jsonl.zst").unwrap();
        let ack = put_raw_chunk(&store, &key, &sealed, Metadata::new())
            .await
            .unwrap();
        assert!(ack.created);
        assert_eq!(ack.content_sha256, sealed.stored_sha256());

        let meta = store.head(&key).await.unwrap().unwrap().metadata;
        assert_eq!(meta[META_RAW_SHA256], sealed.raw_sha256());
        assert_eq!(meta[META_SCRUB_VERSION], RULESET_VERSION.to_string());
        let body = codec::decompress_zstd(sealed.stored()).unwrap();
        assert_eq!(codec::sha256_hex(&body), sealed.raw_sha256());
        assert_eq!(body.len() as u64, sealed.stored_raw_bytes());
        let text = String::from_utf8(body).unwrap();
        assert!(
            text.contains("[REDACTED]") && !text.contains("sk-abc"),
            "{text}"
        );

        // An identical retry is idempotent; the caller cannot override the scrub keys.
        let mut forged = Metadata::new();
        forged.insert(META_SCRUB_VERSION.into(), "999".into());
        let again = put_raw_chunk(&store, &key, &sealed, forged).await.unwrap();
        assert!(!again.created);
        let meta = store.head(&key).await.unwrap().unwrap().metadata;
        assert_eq!(meta[META_SCRUB_VERSION], RULESET_VERSION.to_string());
    }

    #[test]
    fn an_empty_chunk_cannot_be_sealed() {
        assert!(SealedChunk::seal(&super::super::scrub_chunk(b"")).is_err());
    }
}
