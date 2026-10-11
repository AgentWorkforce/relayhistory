//! Read-only, coherent provider acquisition helpers.
use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags, TransactionBehavior};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

/// Provider-owned roots, required before opening any primary or sidecar data.
pub(crate) struct AcquisitionContext<'a> {
    source: &'a str,
    pub roots: &'a crate::ProviderRoots,
}
impl<'a> AcquisitionContext<'a> {
    pub fn new(source: &'a str, roots: &'a crate::ProviderRoots) -> Self {
        Self { source, roots }
    }
    pub fn validate(&self, path: &Path) -> Result<()> {
        crate::hydrate::validate_provider_path(self.source, path, self.roots)
    }
    pub fn read(&self, path: &Path) -> Result<Vec<u8>> {
        self.validate(path)?;
        Ok(std::fs::read(path)?)
    }
}

/// Immutable provider evidence captured together with its content stamp.
/// The selected parser consumes this payload, never reopens a newer source.
#[derive(Debug)]
pub(crate) struct AcquiredSession {
    pub stamp: String,
    pub payload: Value,
    pub bytes: i64,
    pub records: i64,
    pub read_paths: Vec<PathBuf>,
}
impl AcquiredSession {
    pub fn new(payload: Value, bytes: i64, records: i64, read_paths: Vec<PathBuf>) -> Self {
        let encoded = serde_json::to_vec(&payload).expect("JSON payload serializes");
        let stamp = format!("sha256:{:x}", Sha256::digest(&encoded));
        Self {
            stamp,
            payload,
            bytes,
            records,
            read_paths,
        }
    }
}

/// Execute all session queries under one read-only SQLite transaction. WAL
/// rows are visible; never copy the main file, open immutable=1, migrate, or
/// install journal-mode pragmas on a provider-owned database.
pub(crate) fn sqlite_snapshot<T>(
    context: &AcquisitionContext<'_>,
    path: &Path,
    read: impl FnOnce(&Connection) -> Result<T>,
) -> Result<T> {
    context.validate(path)?;
    let mut conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("open provider SQLite snapshot {}", path.display()))?;
    conn.busy_timeout(std::time::Duration::from_secs(2))?;
    let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
    // Establish the snapshot before calling provider code.
    tx.query_row("SELECT count(*) FROM sqlite_schema", [], |row| {
        row.get::<_, i64>(0)
    })?;
    let result = read(&tx)?;
    tx.commit()?;
    Ok(result)
}

/// Capture a provider-owned explicit file set in stable order. The provider
/// enumerates only evidence files; this helper never recursively scans a home.
/// Byte strings are preserved, so a parser can defer an incomplete final line.
pub(crate) fn file_snapshot(
    context: &AcquisitionContext<'_>,
    mut paths: Vec<PathBuf>,
) -> Result<AcquiredSession> {
    paths.sort();
    paths.dedup();
    let mut bytes = 0i64;
    let mut files = Vec::new();
    for path in &paths {
        let data = context
            .read(path)
            .with_context(|| format!("read provider file {}", path.display()))?;
        bytes = bytes.saturating_add(i64::try_from(data.len()).unwrap_or(i64::MAX));
        files.push(serde_json::json!({"path":path, "bytes":data}));
    }
    Ok(AcquiredSession::new(Value::Array(files), bytes, 0, paths))
}

pub(crate) fn validate_provider_path(
    source: &str,
    path: &Path,
    provider_roots: &crate::ProviderRoots,
) -> Result<()> {
    let roots = crate::sources::catalog::transcript_roots(source, provider_roots);
    let canonical = std::fs::canonicalize(path)?;
    let valid = roots
        .iter()
        .filter_map(|root| std::fs::canonicalize(root).ok())
        .any(|root| canonical.starts_with(root));
    if !valid {
        anyhow::bail!("SESSION_SOURCE_MISMATCH: catalog locator does not belong to the {source} provider root");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sqlite_snapshot_includes_wal_and_keeps_all_queries_on_one_generation() {
        let dir = tempfile::tempdir().unwrap();
        let roots =
            crate::ProviderRoots::from_home(dir.path().into(), dir.path().join("opencode.db"));
        let directory = &roots.for_provider("test-fixture")[0];
        std::fs::create_dir_all(directory).unwrap();
        let path = directory.join("history.db");
        let context = AcquisitionContext::new("test-fixture", &roots);
        let writer = Connection::open(&path).unwrap();
        writer.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE messages (text TEXT); INSERT INTO messages VALUES ('before')").unwrap();
        let count = |conn: &Connection| {
            conn.query_row("SELECT count(*) FROM messages", [], |r| r.get::<_, i64>(0))
                .unwrap()
        };
        sqlite_snapshot(&context, &path, |reader| {
            assert_eq!(count(reader), 1);
            writer
                .execute("INSERT INTO messages VALUES ('after')", [])
                .unwrap();
            assert_eq!(
                count(reader),
                1,
                "a second query must not see the newer WAL generation"
            );
            assert!(reader.execute("DELETE FROM messages", []).is_err());
            Ok(())
        })
        .unwrap();
        assert_eq!(
            sqlite_snapshot(&context, &path, |reader| Ok(count(reader))).unwrap(),
            2
        );
        assert!(!dir.path().join("missing.db").exists());
        assert!(sqlite_snapshot(&context, &dir.path().join("missing.db"), |_| Ok(())).is_err());
        assert!(!dir.path().join("missing.db").exists());
    }
}
