//! A writable connection retained only by a sequential reconciliation round.
//! The caller must hold SyncRunLock before entering with_connection. External
//! data changes invalidate validation as well as DDL: migration readiness also
//! depends on data-only markers. No connection or transaction crosses callers.
use super::*;
use crate::read_pool::{file_identity, FileIdentity};
use std::cell::RefCell;

#[derive(Default)]
pub(crate) struct Cache(Option<Entry>);
impl std::fmt::Debug for Cache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncConnectionCache")
            .field("retained", &self.0.is_some())
            .finish()
    }
}

struct Entry {
    conn: Connection,
    path: PathBuf,
    resolved: PathBuf,
    cwd: Option<PathBuf>,
    identity: Option<FileIdentity>,
    validated: Versions,
}

#[derive(PartialEq, Eq)]
struct Versions {
    schema: i64,
    data: i64,
}
impl Versions {
    fn read(conn: &Connection) -> Result<Self> {
        Ok(Self {
            schema: conn.query_row("PRAGMA schema_version", [], |row| row.get(0))?,
            data: conn.query_row("PRAGMA data_version", [], |row| row.get(0))?,
        })
    }
}

thread_local! { static CURRENT: RefCell<Option<Cache>> = const { RefCell::new(None) }; }
#[cfg(test)]
thread_local! { static VALIDATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

pub(crate) fn with_cache<T>(cache: &mut Cache, run: impl FnOnce() -> T) -> T {
    struct Restore<'a>(&'a mut Cache, Option<Cache>);
    impl Drop for Restore<'_> {
        fn drop(&mut self) {
            *self.0 = CURRENT
                .with(|slot| slot.replace(self.1.take()))
                .unwrap_or_default();
        }
    }
    let previous = CURRENT.with(|slot| slot.replace(Some(std::mem::take(cache))));
    let _restore = Restore(cache, previous);
    run()
}

/// The slot is empty while user/provider code executes, so nested acquisition
/// cannot borrow the outer connection or panic on a live RefCell borrow.
pub(crate) fn with_connection<T>(
    path: &Path,
    run: impl FnOnce(&Connection) -> Result<T>,
) -> Result<T> {
    let scoped = CURRENT.with(|slot| slot.borrow().is_some());
    // SQLite owns URI resolution, and an in-memory/unknown-identity filename
    // cannot vouch for reuse. Preserve ordinary open behavior for these paths.
    if !scoped || path.to_str().is_some_and(|path| path.starts_with("file:")) {
        return run(&open_db(path)?);
    }
    let entry = CURRENT.with(|slot| slot.borrow_mut().as_mut().and_then(|cache| cache.0.take()));
    let mut entry = match entry.filter(|entry| entry.matches(path)) {
        Some(entry) => entry,
        None => Entry::open(path)?,
    };
    if Versions::read(&entry.conn)? != entry.validated {
        entry.validated = validate(&entry.conn)?;
    }
    anyhow::ensure!(
        entry.identity.is_none() || entry.matches(path),
        "database replaced while checking writable connection"
    );
    struct Return(Option<Entry>);
    impl Drop for Return {
        fn drop(&mut self) {
            if let Some(entry) = self
                .0
                .take()
                .filter(|entry| entry.conn.is_autocommit() && entry.identity.is_some())
            {
                CURRENT.with(|slot| {
                    if let Some(cache) = slot.borrow_mut().as_mut() {
                        cache.0 = Some(entry);
                    }
                });
            }
        }
    }
    let guard = Return(Some(entry));
    run(&guard
        .0
        .as_ref()
        .context("writable cache entry missing")?
        .conn)
}

impl Entry {
    fn matches(&self, path: &Path) -> bool {
        self.conn.is_autocommit()
            && self.path == path
            && (path.is_absolute()
                || (self.cwd.is_some() && self.cwd == std::env::current_dir().ok()))
            && self.identity.is_some()
            && file_identity(path) == self.identity
            && file_identity(&self.resolved) == self.identity
    }

    fn open(path: &Path) -> Result<Self> {
        let before = file_identity(path);
        let conn = open_uninitialized(path)?;
        let resolved = PathBuf::from(conn.path().context("writable connection has no filename")?);
        let identity = file_identity(&resolved);
        anyhow::ensure!(
            before.is_none() || before == identity,
            "database replaced while opening writable connection"
        );
        let validated = validate(&conn)?;
        anyhow::ensure!(
            identity.is_none()
                || (file_identity(&resolved) == identity && file_identity(path) == identity),
            "database replaced during writable connection validation"
        );
        Ok(Self {
            conn,
            path: path.into(),
            resolved,
            cwd: std::env::current_dir().ok(),
            identity,
            validated,
        })
    }
}

/// Same opening policy used by open_db; validation is separate so the cache
/// can observe versions on both sides of the complete existing init_db path.
pub(super) fn open_uninitialized(path: &Path) -> Result<Connection> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let conn = Connection::open(path)?;
    // Keep the same statement budget and busy policy as ordinary writers.
    conn.set_prepared_statement_cache_capacity(128);
    configure_busy_retry(&conn)?;
    Ok(conn)
}

fn validate(conn: &Connection) -> Result<Versions> {
    for _ in 0..3 {
        let before = Versions::read(conn)?;
        init_db(conn)?;
        let after = Versions::read(conn)?;
        if before == after {
            return Ok(after);
        }
    }
    anyhow::bail!("database kept changing during writable connection validation")
}

#[cfg(test)]
pub(crate) fn validations() -> usize {
    VALIDATIONS.with(|count| count.get())
}

#[cfg(test)]
pub(super) fn count_validation() {
    VALIDATIONS.with(|count| count.set(count.get() + 1));
}

#[cfg(test)]
mod tests;
