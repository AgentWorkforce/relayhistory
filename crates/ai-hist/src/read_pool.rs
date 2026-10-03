//! Read connections a [`crate::SessionStore`] handle reuses across calls.
//!
//! Opening a connection is cheap; its first statement is not. SQLite parses
//! the whole schema on a connection's first use, which on this store's schema
//! is most of a millisecond to several, and every read entry point used to
//! pay it again on a connection of its own (#366). A handle now keeps a few
//! idle read-only connections and hands one out per call.
//!
//! Reuse is only ever of a connection to the *same file*. Each idle
//! connection remembers the file identity around open; a checkout compares
//! it with the path's identity now
//! and discards every connection that disagrees, so a database replaced or
//! recreated under the handle gets a fresh connection. A schema change made
//! in place by another process needs no such check: SQLite notices the
//! changed `schema_version` on the next statement and re-parses, and the
//! schema gates are rechecked whenever that version changes.
//!
//! Only read paths use this. Writers keep opening their own connection under
//! the `SyncRunLock` and hydration locks, exactly as before.
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use rusqlite::Connection;

use crate::session_store::Error;
use crate::store::open_db_readonly;

/// Idle connections kept per handle. Enough for the few reads an embedder
/// overlaps (a drain open while it looks up sessions); more would only hold
/// file descriptors.
const MAX_IDLE: usize = 4;

/// What a path named when a connection was opened. `None` where the
/// platform gives no stable identity; such connections are never reused.
#[cfg(unix)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

#[cfg(unix)]
fn file_identity(path: &Path) -> Option<FileIdentity> {
    use std::os::unix::fs::MetadataExt;
    let metadata = std::fs::metadata(path).ok()?;
    Some(FileIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

#[cfg(windows)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct FileIdentity(Arc<same_file::Handle>);

#[cfg(windows)]
fn file_identity(path: &Path) -> Option<FileIdentity> {
    // Keep the handle alive: Windows may recycle file indexes after close.
    same_file::Handle::from_path(path)
        .ok()
        .map(|handle| FileIdentity(Arc::new(handle)))
}

#[cfg(not(any(unix, windows)))]
#[derive(Debug, Clone, PartialEq, Eq)]
struct FileIdentity;

#[cfg(not(any(unix, windows)))]
fn file_identity(_path: &Path) -> Option<FileIdentity> {
    None
}

struct Idle {
    conn: Connection,
    identity: FileIdentity,
    gates: Gates,
}

/// Schema gates a connection has already passed, valid for one
/// `PRAGMA schema_version`. Only passes are remembered: a gate that failed
/// can start passing after a migration that also writes data, while a gate
/// that passed stays passed until the schema changes (or the file is
/// replaced, which a new connection answers).
#[derive(Default)]
struct Gates {
    schema_version: i64,
    passed: Vec<&'static str>,
}

/// The idle read connections of one handle, shared by its clones.
#[derive(Default)]
pub(crate) struct ReadPool {
    idle: Mutex<Vec<Idle>>,
    /// SQLite resolves URI syntax (including percent escaping and query
    /// parameters) for us at store open. Empty/in-memory filenames stay absent.
    identity_path: Option<PathBuf>,
}

impl fmt::Debug for ReadPool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let idle = self.idle.lock().map_or(0, |idle| idle.len());
        formatter
            .debug_struct("ReadPool")
            .field("idle", &idle)
            .finish()
    }
}

impl ReadPool {
    pub(crate) fn new(conn: &Connection) -> Self {
        Self {
            idle: Mutex::default(),
            identity_path: conn
                .path()
                .filter(|path| !path.is_empty())
                .map(PathBuf::from),
        }
    }

    /// A read-only connection to `path`: an idle one to the same file if
    /// there is one, otherwise a new one.
    pub(crate) fn get(self: &Arc<Self>, path: &Path) -> Result<PooledConnection, Error> {
        self.get_checked(path, || {})
    }

    fn get_checked(
        self: &Arc<Self>,
        path: &Path,
        after_identity: impl FnOnce(),
    ) -> Result<PooledConnection, Error> {
        let identity_path = if path.to_str().is_some_and(|path| path.starts_with("file:")) {
            self.identity_path.as_deref().unwrap_or(path)
        } else {
            path
        };
        // Check within the checkout lock, then again after selecting an idle
        // connection. A replacement while waiting for that lock or between
        // the first stat and selection must discard the old connection.
        let mut idle = self
            .idle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let identity = file_identity(identity_path);
        after_identity();
        idle.retain(|entry| Some(&entry.identity) == identity.as_ref());
        if let Some(entry) = idle.pop() {
            if file_identity(identity_path).as_ref() == Some(&entry.identity) {
                return Ok(PooledConnection {
                    conn: Some(entry.conn),
                    identity: Some(entry.identity),
                    gates: entry.gates,
                    pool: Some(Arc::clone(self)),
                });
            }
            idle.clear();
        }
        drop(idle);
        // Both snapshots surround open. If they differ, this connection may
        // target the old file and must never be tagged with the new identity.
        for _ in 0..3 {
            let identity = file_identity(identity_path);
            let conn = open_db_readonly(path)
                .map_err(|error| Error::DatabaseOpen(format!("{error:#}")))?;
            if identity != file_identity(identity_path) {
                continue;
            }
            let pool = identity.as_ref().map(|_| Arc::clone(self));
            return Ok(PooledConnection {
                conn: Some(conn),
                identity,
                gates: Gates::default(),
                pool,
            });
        }
        Err(Error::DatabaseOpen(format!(
            "{} was repeatedly replaced while opening a read connection",
            path.display()
        )))
    }

    #[cfg(test)]
    pub(crate) fn idle(&self) -> usize {
        self.idle.lock().unwrap().len()
    }
}

/// A read connection checked out of a [`ReadPool`]. Returned on drop when it
/// is back in autocommit; one still inside a transaction is closed instead,
/// so no snapshot ever outlives the call that took it.
pub(crate) struct PooledConnection {
    conn: Option<Connection>,
    identity: Option<FileIdentity>,
    gates: Gates,
    pool: Option<Arc<ReadPool>>,
}

impl PooledConnection {
    /// A connection that belongs to no pool and is closed on drop.
    #[cfg(test)]
    pub(crate) fn detached(conn: Connection) -> Self {
        Self {
            conn: Some(conn),
            identity: None,
            gates: Gates::default(),
            pool: None,
        }
    }

    /// Whether `check`, a schema gate named `name`, passes on this
    /// connection, remembered across reuse until the schema changes. Each
    /// gate reads `sqlite_master` and `pragma_table_info` for many objects;
    /// `PRAGMA schema_version` is one header read.
    pub(crate) fn gate(
        &mut self,
        name: &'static str,
        check: impl FnOnce(&Connection) -> anyhow::Result<bool>,
    ) -> anyhow::Result<bool> {
        let conn = self
            .conn
            .as_ref()
            .expect("a pooled connection is present until drop");
        let version: i64 = conn.query_row("PRAGMA schema_version", [], |row| row.get(0))?;
        if version != self.gates.schema_version {
            self.gates = Gates {
                schema_version: version,
                passed: Vec::new(),
            };
        }
        if self.gates.passed.contains(&name) {
            return Ok(true);
        }
        let passed = check(conn)?;
        if passed {
            self.gates.passed.push(name);
        }
        Ok(passed)
    }
}

impl Deref for PooledConnection {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        self.conn
            .as_ref()
            .expect("a pooled connection is present until drop")
    }
}

impl DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Connection {
        self.conn
            .as_mut()
            .expect("a pooled connection is present until drop")
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        let (Some(conn), Some(identity), Some(pool)) =
            (self.conn.take(), self.identity.take(), self.pool.take())
        else {
            return;
        };
        if !conn.is_autocommit() {
            return;
        }
        let mut idle = pool
            .idle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if idle.len() < MAX_IDLE {
            idle.push(Idle {
                conn,
                identity,
                gates: std::mem::take(&mut self.gates),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::open_db;

    fn cache_size(conn: &Connection) -> i64 {
        conn.query_row("PRAGMA cache_size", [], |row| row.get(0))
            .unwrap()
    }

    fn count(conn: &Connection, table: &str) -> i64 {
        conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .unwrap()
    }

    fn fresh_db(dir: &Path) -> std::path::PathBuf {
        let db = dir.join("history.db");
        drop(open_db(&db).unwrap());
        db
    }

    /// A connection handed back is the one the next call gets: per-connection
    /// state set on the first is still there on the second.
    #[test]
    fn a_returned_connection_is_reused() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let pool = Arc::new(ReadPool::default());
        {
            let conn = pool.get(&db).unwrap();
            conn.pragma_update(None, "cache_size", -1234).unwrap();
        }
        assert_eq!(pool.idle(), 1);
        assert_eq!(cache_size(&pool.get(&db).unwrap()), -1234);
        // Two at once are two connections, and both go back.
        let (first, second) = (pool.get(&db).unwrap(), pool.get(&db).unwrap());
        assert_eq!(pool.idle(), 0);
        drop((first, second));
        assert_eq!(pool.idle(), 2);
    }

    /// A database replaced under the handle -- a new file at the same path --
    /// is read through a new connection, never the one to the old file.
    #[cfg(unix)]
    #[test]
    fn a_replaced_database_is_reopened() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let pool = Arc::new(ReadPool::default());
        assert_eq!(count(&pool.get(&db).unwrap(), "session_events"), 0);
        assert_eq!(pool.idle(), 1);

        let replacement = dir.path().join("replacement.db");
        let writer = open_db(&replacement).unwrap();
        writer
            .execute(
                "INSERT INTO session_events (source, session_id, ts_ms, role, kind, event_uid) \
                 VALUES ('claude', 's', 1, 'user', 'text', 'e1')",
                [],
            )
            .unwrap();
        writer
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
            .unwrap();
        drop(writer);
        std::fs::rename(&replacement, &db).unwrap();
        for sidecar in ["-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{sidecar}", db.display()));
        }

        assert_eq!(count(&pool.get(&db).unwrap(), "session_events"), 1);
        assert_eq!(
            pool.idle(),
            1,
            "the connection to the old file was discarded"
        );
    }

    #[test]
    fn sqlite_uri_reads_reuse_the_resolved_filename() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("history with space.db");
        drop(open_db(&db).unwrap());
        let uri = PathBuf::from(format!(
            "file:{}?mode=ro",
            db.to_string_lossy().replace('\\', "/").replace(' ', "%20")
        ));
        let seed = open_db_readonly(&uri).unwrap();
        let pool = Arc::new(ReadPool::new(&seed));
        drop(seed);
        {
            let conn = pool.get(&uri).unwrap();
            conn.pragma_update(None, "cache_size", -4321).unwrap();
        }
        assert_eq!(pool.idle(), 1);
        assert_eq!(cache_size(&pool.get(&uri).unwrap()), -4321);
    }

    #[cfg(unix)]
    #[test]
    fn replacement_between_identity_and_checkout_discards_the_idle_connection() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let pool = Arc::new(ReadPool::default());
        {
            let conn = pool.get(&db).unwrap();
            conn.pragma_update(None, "cache_size", -9876).unwrap();
        }
        let replacement = dir.path().join("replacement.db");
        drop(open_db(&replacement).unwrap());
        let conn = pool
            .get_checked(&db, || {
                std::fs::rename(&replacement, &db).unwrap();
                for sidecar in ["-wal", "-shm"] {
                    let _ = std::fs::remove_file(format!("{}{sidecar}", db.display()));
                }
            })
            .unwrap();
        assert_ne!(
            cache_size(&conn),
            -9876,
            "reused the old database connection"
        );
        assert_eq!(count(&conn, "session_events"), 0);
    }

    #[test]
    fn passed_gates_are_rechecked_after_a_schema_change() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let writer = open_db(&db).unwrap();
        let pool = Arc::new(ReadPool::default());
        {
            let mut conn = pool.get(&db).unwrap();
            assert!(conn.gate("test", |_| Ok(true)).unwrap());
        }
        let mut conn = pool.get(&db).unwrap();
        assert!(conn
            .gate("test", |_| panic!("cached pass was not reused"))
            .unwrap());
        writer
            .execute_batch("CREATE TABLE gate_changed(x)")
            .unwrap();
        assert!(!conn.gate("test", |_| Ok(false)).unwrap());
        assert!(
            conn.gate("test", |_| Ok(true)).unwrap(),
            "failed gates must not be cached"
        );
    }

    /// A schema changed in place by another connection is seen by a reused
    /// connection: SQLite re-reads a schema whose version moved.
    #[test]
    fn a_schema_changed_in_place_is_seen_by_a_reused_connection() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("history.db");
        let writer = open_db(&db).unwrap();
        let pool = Arc::new(ReadPool::default());
        assert_eq!(count(&pool.get(&db).unwrap(), "sessions"), 0);
        writer
            .execute_batch("CREATE TABLE added_later(x); INSERT INTO added_later VALUES (1);")
            .unwrap();
        assert_eq!(pool.idle(), 1);
        assert_eq!(count(&pool.get(&db).unwrap(), "added_later"), 1);
    }

    /// A connection still inside a transaction is closed, not pooled, so a
    /// read snapshot can never outlive the call that opened it.
    #[test]
    fn a_connection_left_in_a_transaction_is_not_pooled() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let pool = Arc::new(ReadPool::default());
        {
            let conn = pool.get(&db).unwrap();
            conn.execute_batch("BEGIN DEFERRED; SELECT COUNT(*) FROM sessions;")
                .unwrap();
        }
        assert_eq!(pool.idle(), 0);
    }

    /// Never more than `MAX_IDLE` connections are kept.
    #[test]
    fn the_idle_set_is_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let db = fresh_db(dir.path());
        let pool = Arc::new(ReadPool::default());
        let held: Vec<_> = (0..MAX_IDLE + 3).map(|_| pool.get(&db).unwrap()).collect();
        drop(held);
        assert_eq!(pool.idle(), MAX_IDLE);
    }
}
