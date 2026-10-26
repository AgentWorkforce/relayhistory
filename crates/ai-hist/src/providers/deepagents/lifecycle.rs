use super::*;
use crate::discover::WatchEntries;
use crate::ingest::check_capture_cancelled;

pub(super) fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut out = path.as_os_str().to_os_string();
    out.push(suffix);
    PathBuf::from(out)
}
pub(super) fn sweep_databases(
    roots: &ProviderRoots,
    scope: Option<&crate::ingest::path_scope::PathScope>,
) -> Result<(Vec<PathBuf>, u64)> {
    roots.ensure_provider_roots(ID)?;
    let context = AcquisitionContext::new(ID, roots);
    let mut paths = Vec::new();
    let mut unread = 0;
    for path in roots.for_provider(ID) {
        check_capture_cancelled()?;
        if !super::lifecycle::in_scope(path, scope) {
            continue;
        }
        match std::fs::metadata(path) {
            Ok(meta) if meta.is_file() && context.validate(path).is_ok() => {
                paths.push(path.clone())
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            _ => unread += 1,
        }
    }
    paths.sort();
    paths.dedup();
    Ok((paths, unread))
}
pub(super) fn watch_roots(roots: &ProviderRoots) -> Vec<WatchRoot> {
    roots
        .for_provider(ID)
        .iter()
        .filter_map(|path| {
            let parent = path.parent()?;
            let name = path.file_name()?.to_string_lossy();
            Some(WatchRoot::directory_of(
                parent.to_path_buf(),
                WatchEntries::OpencodeStores {
                    primary: name.into_owned().into(),
                    channels: false,
                },
            ))
        })
        .collect()
}
pub(super) fn in_scope(path: &Path, scope: Option<&crate::ingest::path_scope::PathScope>) -> bool {
    scope.is_none_or(|scope| {
        scope.paths().iter().any(|changed| {
            path.starts_with(changed)
                || ["", "-wal", "-shm", "-journal"]
                    .iter()
                    .any(|suffix| *changed == sidecar(path, suffix))
        })
    })
}

/// A missing database is authoritative only beneath an existing readable
/// configured parent, with no SQLite sidecar left behind.
pub(super) fn retire_missing_databases(ctx: &SweepContext<'_>) -> Result<u64> {
    let mut stmt = ctx.conn.prepare("SELECT raw_locator FROM session_presences WHERE source=?1 AND location='local' AND raw_locator IS NOT NULL UNION SELECT raw_locator FROM session_observations WHERE source=?1 AND location='local' AND raw_locator IS NOT NULL UNION SELECT raw_path FROM sessions WHERE source=?1 AND raw_path IS NOT NULL")?;
    let paths = stmt
        .query_map([ID], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut unread = 0;
    for raw in paths {
        check_capture_cancelled()?;
        let path = PathBuf::from(raw);
        if !in_scope(&path, ctx.paths) || !ctx.roots.for_provider(ID).contains(&path) {
            continue;
        }
        if retire_one(ctx, &path).is_err() {
            check_capture_cancelled()?;
            unread += 1;
        }
    }
    Ok(unread)
}
fn retire_one(ctx: &SweepContext<'_>, path: &Path) -> Result<()> {
    if path.try_exists()? {
        return Ok(());
    }
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    anyhow::ensure!(
        std::fs::metadata(parent)?.is_dir(),
        "DeepAgents database parent is not a readable directory"
    );
    // Enumerating the parent proves access. A surviving sidecar means the
    // provider's database lifecycle is still in flight, not deleted.
    for entry in std::fs::read_dir(parent)? {
        entry?;
    }
    for suffix in ["-wal", "-shm", "-journal"] {
        if sidecar(path, suffix).try_exists()? {
            return Ok(());
        }
    }
    if path.try_exists()? {
        return Ok(());
    }
    let tx = ctx.conn.unchecked_transaction()?;
    super::normalize::retire_deleted(&tx, path, &[])?;
    tx.commit()?;
    Ok(())
}
