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
    /// ETag of the object version this sidecar describes. A sidecar whose
    /// `e_tag` does not match the current object (a crash between the object
    /// write and the sidecar write) is ignored, never trusted.
    #[serde(default)]
    e_tag: Option<String>,
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
        e_tag: Option<&str>,
        metadata: &Metadata,
        content_type: &str,
    ) -> object_store::Result<()> {
        let sidecar = Sidecar {
            content_type: content_type.to_string(),
            metadata: metadata.clone(),
            e_tag: e_tag.map(str::to_string),
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

    /// Metadata of the object whose current ETag is `e_tag`; empty when the
    /// sidecar is missing, torn, or describes a different version.
    async fn read_metadata(
        &self,
        path: &Path,
        e_tag: Option<&str>,
    ) -> object_store::Result<Metadata> {
        let result = self
            .store
            .get_opts(&Self::sidecar_path(path), Default::default())
            .await;
        match result {
            Ok(result) => {
                let body = result.bytes().await?;
                // A torn, foreign or stale sidecar degrades to "no metadata";
                // the idempotency check then hashes the body instead.
                Ok(serde_json::from_slice::<Sidecar>(&body)
                    .ok()
                    .filter(|s| s.e_tag.is_some() && s.e_tag.as_deref() == e_tag)
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
                self.write_sidecar(path, result.e_tag.as_deref(), metadata, content_type)
                    .await?;
                return Ok(result);
            }
            PutMode::Overwrite => {
                // Replacements take the same lock as compare-and-swap, so an
                // unconditional write cannot land between a CAS's compare and
                // its rename.
                let _guard = self.lock().await?;
                let result = self
                    .store
                    .put_opts(path, payload, PutOptions::default())
                    .await?;
                self.write_sidecar(path, result.e_tag.as_deref(), metadata, content_type)
                    .await?;
                return Ok(result);
            }
            PutMode::Create => {
                // A hard link is atomic and never replaces anything.
                let opts = PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                };
                self.store.put_opts(path, payload, opts).await?
            }
        };
        self.write_sidecar(path, result.e_tag.as_deref(), metadata, content_type)
            .await?;
        Ok(result)
    }

    /// A symbolic link anywhere under the root could point a valid key at
    /// any file on the host, so the store never traverses one: every component
    /// of the object path and of its sidecar path is `lstat`ed first.
    ///
    /// Threat model: this rejects links that exist when an operation starts
    /// (or, for multipart, again at commit). It cannot stop a concurrent
    /// actor who can write inside the root from swapping a checked directory
    /// for a link between the check and the open, because the underlying
    /// `LocalFileSystem` opens by path. Closing that race needs
    /// directory-relative `openat(O_NOFOLLOW)` access throughout; until then,
    /// treat the root as trusted (same-user, not shared with untrusted
    /// writers).
    async fn check_key(&self, key: &crate::ObjectKey) -> Result<()> {
        let root = self.root.clone();
        let segments: Vec<String> = key.as_str().split('/').map(str::to_string).collect();
        let key = key.to_string();
        tokio::task::spawn_blocking(move || {
            let mut sidecar = vec![RESERVED_DIR.to_string(), "meta".to_string()];
            sidecar.extend(segments.iter().cloned());
            if let Some(last) = sidecar.last_mut() {
                last.push_str(".json");
            }
            for path in [segments, sidecar] {
                no_symlink_under(&root, &path, &key)?;
            }
            Ok(())
        })
        .await
        .map_err(|e| Error::Backend {
            backend: "fs",
            source: Box::new(e),
        })?
    }

    async fn metadata_of_get(
        &self,
        path: &Path,
        meta: &object_store::ObjectMeta,
        _attributes: &object_store::Attributes,
    ) -> object_store::Result<Metadata> {
        self.read_metadata(path, meta.e_tag.as_deref()).await
    }

    async fn commit_guard(&self) -> Result<Option<Box<dyn Send>>> {
        let file = self
            .lock()
            .await
            .map_err(|e| Error::from_store(Self::NAME, "", e))?;
        Ok(Some(Box::new(file)))
    }

    async fn head_object(
        &self,
        path: &Path,
    ) -> object_store::Result<(object_store::ObjectMeta, Metadata)> {
        let meta = self.store.head(path).await?;
        let metadata = self.read_metadata(path, meta.e_tag.as_deref()).await?;
        Ok((meta, metadata))
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

    async fn finish_multipart(
        &self,
        path: &Path,
        result: &PutResult,
        metadata: &Metadata,
    ) -> Result<()> {
        // Re-check right before the late sidecar write: the entry check ran
        // when the upload started, possibly long ago.
        self.check_key(&crate::ObjectKey::from_path(path)?).await?;
        let content_type = crate::codec::infer_content_type(path.as_ref());
        self.write_sidecar(path, result.e_tag.as_deref(), metadata, content_type)
            .await
            .map_err(|e| Error::from_store(Self::NAME, path.as_ref(), e))
    }

    /// Rewrite a sidecar that was lost or went stale (see `Backend`). Runs
    /// under the write lock and only if the object is still the version that
    /// was verified, so it can never stamp an old ETag over a newer write's
    /// sidecar.
    async fn repair_metadata(
        &self,
        path: &Path,
        e_tag: Option<&str>,
        metadata: &Metadata,
        content_type: &str,
    ) -> Result<()> {
        let map = |e| Error::from_store(Self::NAME, path.as_ref(), e);
        let _guard = self.lock().await.map_err(map)?;
        let current = match self.store.head(path).await {
            Ok(meta) => meta,
            Err(object_store::Error::NotFound { .. }) => return Ok(()),
            Err(e) => return Err(map(e)),
        };
        if current.e_tag.as_deref() != e_tag {
            tracing::debug!(%path, "object changed since it was verified; skipping sidecar repair");
            return Ok(());
        }
        self.write_sidecar(path, e_tag, metadata, content_type)
            .await
            .map_err(map)
    }
}

fn no_symlink_under(root: &std::path::Path, segments: &[String], key: &str) -> Result<()> {
    let mut current = root.to_path_buf();
    for segment in segments {
        current.push(segment);
        match std::fs::symlink_metadata(&current) {
            Ok(meta) if meta.file_type().is_symlink() => {
                return Err(Error::InvalidKey(format!(
                    "{key}: {} is a symbolic link; FsStore does not follow links",
                    current.display()
                )))
            }
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => {
                return Err(Error::Backend {
                    backend: "fs",
                    source: Box::new(e),
                })
            }
        }
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ObjectKey, ObjectStore as _, PutOpts};

    fn store() -> (tempfile::TempDir, FsStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = FsStore::new(FsConfig::new(dir.path())).unwrap();
        (dir, store)
    }

    #[tokio::test]
    async fn lost_sidecar_is_repaired_by_an_idempotent_retry() {
        let (dir, store) = store();
        let key = ObjectKey::new("s/raw/0000000000-0-aaaaaaaa.jsonl.zst").unwrap();
        let body = Bytes::from_static(b"chunk");
        store
            .put(&key, body.clone(), PutOpts::create().with_trace_id("t-1"))
            .await
            .unwrap();
        // Crash between object write and sidecar write.
        std::fs::remove_file(
            dir.path()
                .join(".rh/meta/s/raw/0000000000-0-aaaaaaaa.jsonl.zst.json"),
        )
        .unwrap();
        assert!(store.head(&key).await.unwrap().unwrap().metadata.is_empty());

        let ack = store
            .put(&key, body, PutOpts::create().with_trace_id("t-1"))
            .await
            .unwrap();
        assert!(!ack.created);
        let meta = store.head(&key).await.unwrap().unwrap();
        assert_eq!(meta.trace_id(), Some("t-1"));
        assert_eq!(meta.content_sha256(), Some(ack.content_sha256.as_str()));
    }

    #[tokio::test]
    async fn repair_never_overwrites_a_newer_version() {
        let (_dir, store) = store();
        let key = ObjectKey::new("s/manifest.json").unwrap();
        let v1 = store
            .put(&key, Bytes::from_static(b"{\"v\":1}"), PutOpts::create())
            .await
            .unwrap();
        let v2 = store
            .put(
                &key,
                Bytes::from_static(b"{\"v\":2}"),
                PutOpts::replace(v1.version.clone()).with_trace_id("t-2"),
            )
            .await
            .unwrap();
        // A repair verified against v1 arrives after v2 committed.
        store
            .core
            .backend()
            .repair_metadata(
                &key.to_path(),
                v1.version.e_tag.as_deref(),
                &Metadata::new(),
                "application/json",
            )
            .await
            .unwrap();
        let meta = store.head(&key).await.unwrap().unwrap();
        assert_eq!(meta.trace_id(), Some("t-2"));
        assert_eq!(meta.content_sha256(), Some(v2.content_sha256.as_str()));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn refuses_to_follow_a_symlink_out_of_the_root() {
        let (dir, store) = store();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("secret"), b"host file").unwrap();
        std::os::unix::fs::symlink(outside.path(), dir.path().join("escape")).unwrap();
        let key = ObjectKey::new("escape/secret").unwrap();
        assert!(matches!(
            store.get(&key, None).await,
            Err(Error::InvalidKey(_))
        ));
        assert!(matches!(store.head(&key).await, Err(Error::InvalidKey(_))));
        let new = ObjectKey::new("escape/new").unwrap();
        assert!(matches!(
            store
                .put(&new, Bytes::from_static(b"x"), PutOpts::default())
                .await,
            Err(Error::InvalidKey(_))
        ));
        assert_eq!(
            std::fs::read_dir(outside.path()).unwrap().count(),
            1,
            "nothing written outside"
        );
        // A link in the sidecar tree is refused too.
        let meta = dir.path().join(".rh/meta");
        std::fs::create_dir_all(&meta).unwrap();
        std::os::unix::fs::symlink(outside.path(), meta.join("side")).unwrap();
        assert!(matches!(
            store
                .put(
                    &ObjectKey::new("side/x").unwrap(),
                    Bytes::from_static(b"x"),
                    PutOpts::default()
                )
                .await,
            Err(Error::InvalidKey(_))
        ));
    }

    #[tokio::test]
    async fn presign_rejects_sub_second_ttls() {
        let (_dir, store) = store();
        let key = ObjectKey::new("a/b").unwrap();
        for ttl in [Duration::ZERO, Duration::from_millis(500)] {
            assert!(matches!(
                store.presign_get(&key, ttl).await,
                Err(Error::InvalidArgument(_))
            ));
        }
        assert!(store
            .presign_get(&key, Duration::from_secs(1))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn stale_sidecar_is_never_trusted() {
        let (dir, store) = store();
        let key = ObjectKey::new("s/manifest.json").unwrap();
        store
            .put(&key, Bytes::from_static(b"old!"), PutOpts::create())
            .await
            .unwrap();
        // Crash after replacing the body but before the sidecar: same length,
        // different bytes, old sidecar left behind.
        std::fs::write(dir.path().join("s/manifest.json"), b"new!").unwrap();
        assert!(
            store.head(&key).await.unwrap().unwrap().metadata.is_empty(),
            "sidecar for another version must be ignored"
        );
        // A create retry of the *old* bytes must not be acknowledged.
        let err = store
            .put(&key, Bytes::from_static(b"old!"), PutOpts::create())
            .await
            .unwrap_err();
        assert!(matches!(err, Error::AlreadyExists { .. }), "{err:?}");
    }
}
