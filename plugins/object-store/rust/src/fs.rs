//! Local-directory store for tests and air-gapped hosts.
//!
//! Objects live at `{root}/{key}` exactly as they would in a bucket, so a
//! directory written here can be synced to S3/GCS/Azure unchanged (ignoring
//! `{root}/.rh/`). `object_store`'s `LocalFileSystem` supplies atomic,
//! fsync'd create/overwrite; this module adds what it lacks:
//!
//! - user metadata, in a sidecar at `{root}/.rh/meta/{key}.json`;
//! - `if_match`, by holding an exclusive `flock` on `{root}/.rh/lock` across
//!   the compare and the atomic rename (works across processes on one host;
//!   not across hosts on a network filesystem);
//! - lexicographic listing (directory walks are unordered).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    ObjectStore as _, ObjectStoreExt as _, PutMode, PutOptions, PutPayload, PutResult,
};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::core::{Backend, Core};
use crate::delegate::delegate_object_store;
use crate::{Error, Metadata, Result};

const RESERVED_DIR: &str = ".rh";

#[derive(Debug, Clone)]
pub struct FsConfig {
    pub root: PathBuf,
    /// fsync files and directories before acknowledging a write. On by
    /// default: an acknowledgment lets the coordinator advance its checkpoint.
    pub fsync: bool,
}

impl FsConfig {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            fsync: true,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Sidecar {
    content_type: String,
    metadata: Metadata,
}

#[derive(Debug)]
pub(crate) struct FsBackend {
    store: Arc<LocalFileSystem>,
    root: PathBuf,
}

impl FsBackend {
    fn sidecar_path(path: &Path) -> Path {
        Path::from(format!("{RESERVED_DIR}/meta/{path}.json"))
    }

    async fn write_sidecar(
        &self,
        path: &Path,
        metadata: &Metadata,
        content_type: &str,
    ) -> object_store::Result<()> {
        let sidecar = Sidecar {
            content_type: content_type.to_string(),
            metadata: metadata.clone(),
        };
        let body = serde_json::to_vec(&sidecar).map_err(|e| generic(e.into()))?;
        self.store
            .put_opts(
                &Self::sidecar_path(path),
                PutPayload::from(body),
                PutOptions::default(),
            )
            .await
            .map(|_| ())
    }

    async fn read_metadata(&self, path: &Path) -> object_store::Result<Metadata> {
        let result = self
            .store
            .get_opts(&Self::sidecar_path(path), Default::default())
            .await;
        match result {
            Ok(result) => {
                let body = result.bytes().await?;
                // A torn or foreign sidecar degrades to "no metadata"; the
                // idempotency check then falls back to hashing the body.
                Ok(serde_json::from_slice::<Sidecar>(&body)
                    .map(|s| s.metadata)
                    .unwrap_or_default())
            }
            Err(object_store::Error::NotFound { .. }) => Ok(Metadata::new()),
            Err(err) => Err(err),
        }
    }

    /// Exclusive advisory lock serializing compare-and-swap writes.
    async fn lock(&self) -> object_store::Result<std::fs::File> {
        let dir = self.root.join(RESERVED_DIR);
        tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&dir)?;
            let file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(dir.join("lock"))?;
            file.lock()?;
            Ok::<_, std::io::Error>(file)
        })
        .await
        .map_err(|e| generic(e.into()))?
        .map_err(|e| generic(e.into()))
    }
}

#[async_trait]
impl Backend for FsBackend {
    const NAME: &'static str = "fs";

    fn store(&self) -> &dyn object_store::ObjectStore {
        self.store.as_ref()
    }

    async fn put_object(
        &self,
        path: &Path,
        body: Bytes,
        mode: PutMode,
        metadata: &Metadata,
        content_type: &str,
    ) -> object_store::Result<PutResult> {
        let payload = PutPayload::from_bytes(body);
        let result = match mode {
            PutMode::Update(expected) => {
                let _guard = self.lock().await?;
                let current = self.store.head(path).await.map_err(|err| match err {
                    // A CAS against a missing object fails its precondition.
                    object_store::Error::NotFound { path, source } => {
                        object_store::Error::Precondition { path, source }
                    }
                    other => other,
                })?;
                if current.e_tag != expected.e_tag {
                    return Err(object_store::Error::Precondition {
                        path: path.to_string(),
                        source: "e_tag changed".into(),
                    });
                }
                let result = self
                    .store
                    .put_opts(path, payload, PutOptions::default())
                    .await?;
                self.write_sidecar(path, metadata, content_type).await?;
                return Ok(result);
            }
            mode => {
                let opts = PutOptions {
                    mode,
                    ..PutOptions::default()
                };
                self.store.put_opts(path, payload, opts).await?
            }
        };
        self.write_sidecar(path, metadata, content_type).await?;
        Ok(result)
    }

    async fn head_object(
        &self,
        path: &Path,
    ) -> object_store::Result<(object_store::ObjectMeta, Metadata)> {
        let meta = self.store.head(path).await?;
        Ok((meta, self.read_metadata(path).await?))
    }

    fn list_objects(
        &self,
        prefix: &Path,
        offset: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        let listing = self.store.list(Some(prefix));
        let offset = offset.cloned();
        futures::stream::once(async move {
            let mut all: Vec<_> = listing
                .try_filter(|m| {
                    futures::future::ready(
                        offset.as_ref().is_none_or(|offset| m.location > *offset),
                    )
                })
                .try_collect()
                .await?;
            all.sort_by(|a, b| a.location.cmp(&b.location));
            Ok::<_, object_store::Error>(futures::stream::iter(all.into_iter().map(Ok)))
        })
        .try_flatten()
        .boxed()
    }

    /// A `file://` URL. The TTL cannot be enforced on a local path.
    async fn presign(&self, path: &Path, _ttl: Duration) -> Result<Url> {
        let file = self
            .store
            .path_to_filesystem(path)
            .map_err(|e| Error::from_store(Self::NAME, path.as_ref(), e))?;
        Url::from_file_path(&file).map_err(|()| {
            Error::InvalidArgument(format!("{} is not an absolute path", file.display()))
        })
    }

    async fn start_multipart(
        &self,
        path: &Path,
        _metadata: &Metadata,
        _content_type: &str,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.store
            .put_multipart_opts(path, Default::default())
            .await
    }

    async fn finish_multipart(&self, path: &Path, metadata: &Metadata) -> Result<()> {
        let content_type = crate::codec::infer_content_type(path.as_ref());
        self.write_sidecar(path, metadata, content_type)
            .await
            .map_err(|e| Error::from_store(Self::NAME, path.as_ref(), e))
    }
}

fn generic(source: Box<dyn std::error::Error + Send + Sync>) -> object_store::Error {
    object_store::Error::Generic {
        store: "fs",
        source,
    }
}

/// [`crate::ObjectStore`] over a local directory.
#[derive(Debug)]
pub struct FsStore {
    core: Core<FsBackend>,
}

delegate_object_store!(FsStore);

impl FsStore {
    pub fn new(config: FsConfig) -> Result<Self> {
        std::fs::create_dir_all(&config.root).map_err(|e| Error::Backend {
            backend: "fs",
            source: Box::new(e),
        })?;
        let root = std::fs::canonicalize(&config.root).map_err(|e| Error::Backend {
            backend: "fs",
            source: Box::new(e),
        })?;
        let store = LocalFileSystem::new_with_prefix(&root)
            .map_err(|e| Error::from_store("fs", "", e))?
            .with_fsync(config.fsync);
        Ok(Self {
            core: Core::new(FsBackend {
                store: Arc::new(store),
                root,
            }),
        })
    }

    pub fn with_page_size(mut self, page_size: usize) -> Self {
        self.core.set_page_size(page_size);
        self
    }

    pub fn root(&self) -> &std::path::Path {
        &self.core.backend().root
    }
}
