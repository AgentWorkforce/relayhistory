//! Multipart upload wrapper: digest tracking, failure poisoning and the
//! commit guard, shared by every backend.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use object_store::path::Path;
use object_store::PutPayload;
use sha2::{Digest, Sha256};
use tracing::Instrument;

use crate::core::{version_of_put, Backend};
use crate::{Error, Metadata, MultipartUpload, ObjectKey, PutAck, Result, META_TRACE_ID};

pub(crate) struct CoreUpload<B> {
    backend: Arc<B>,
    inner: Box<dyn object_store::MultipartUpload>,
    key: ObjectKey,
    path: Path,
    metadata: Metadata,
    expected_sha: Option<String>,
    hasher: Sha256,
    size: u64,
    /// Set when a part failed. `object_store` numbers a part when it is
    /// submitted, so a failed part leaves a hole that a retry cannot fill:
    /// the upload must be aborted and restarted.
    failed: bool,
}

impl<B: Backend> CoreUpload<B> {
    pub(crate) fn new(
        backend: Arc<B>,
        inner: Box<dyn object_store::MultipartUpload>,
        key: ObjectKey,
        path: Path,
        metadata: Metadata,
        expected_sha: Option<String>,
    ) -> Self {
        Self {
            backend,
            inner,
            key,
            path,
            metadata,
            expected_sha,
            hasher: Sha256::new(),
            size: 0,
            failed: false,
        }
    }

    fn check_usable(&self) -> Result<()> {
        if self.failed {
            return Err(Error::InvalidArgument(format!(
                "multipart upload of {} had a failed part; abort it and start a new upload",
                self.key
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl<B: Backend> MultipartUpload for CoreUpload<B> {
    async fn put_part(&mut self, part: Bytes) -> Result<()> {
        self.check_usable()?;
        if part.is_empty() {
            return Err(Error::InvalidArgument("empty multipart part".into()));
        }
        let len = part.len() as u64;
        match self
            .inner
            .put_part(PutPayload::from_bytes(part.clone()))
            .await
        {
            Ok(()) => {
                // Only parts the backend accepted count toward the digest.
                self.hasher.update(&part);
                self.size += len;
                Ok(())
            }
            Err(e) => {
                self.failed = true;
                Err(Error::from_store(B::NAME, self.key.as_str(), e))
            }
        }
    }

    async fn complete(mut self: Box<Self>) -> Result<PutAck> {
        self.check_usable()?;
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
            let _guard = self.backend.commit_guard().await?;
            let result = self
                .inner
                .complete()
                .await
                .map_err(|e| Error::from_store(B::NAME, self.key.as_str(), e))?;
            self.backend
                .finish_multipart(&self.path, &result, &self.metadata)
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
