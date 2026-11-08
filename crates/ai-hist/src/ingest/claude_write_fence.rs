//! Source-wide Claude mutation fence, independent of SQLite's writer lock.
//! Capture holds this across chunk commits. Other sources and unrelated SQLite
//! writes remain available; competing Claude writers receive SQLITE_BUSY.
use anyhow::Result;
use rusqlite::Connection;
use std::cell::RefCell;
use std::fs::{File, OpenOptions};
use std::path::PathBuf;
use std::rc::{Rc, Weak};

struct Lease {
    conn: usize,
    db: PathBuf,
    file: File,
}
impl Drop for Lease {
    fn drop(&mut self) {
        let _ = crate::file_lock::unlock(&self.file);
    }
}
thread_local! {
    static HELD: RefCell<Vec<Weak<Lease>>> = const { RefCell::new(Vec::new()) };
}
pub(crate) struct Guard(Option<Rc<Lease>>);
impl Guard {
    pub(super) fn fenced(&self) -> bool {
        self.0.is_some()
    }
}

/// Acquire before a write transaction. Same-connection nesting is reentrant;
/// another connection, even on this thread, cannot inherit the owner's lease.
/// Never wait while a caller might already hold SQLite's writer lock.
pub(crate) fn acquire(conn: &Connection, source: &str) -> Result<Guard> {
    if source != "claude" {
        return Ok(Guard(None));
    }
    let Some(path) = conn.path().filter(|path| !path.is_empty()) else {
        // Memory/shared-memory stores have no OS identity. They must retain
        // whole-read staging; callers may not enable chunking on this guard.
        return Ok(Guard(None));
    };
    let db = std::fs::canonicalize(path)?;
    let owner = std::ptr::from_ref(conn) as usize;
    if let Some(lease) = HELD.with(|held| {
        let mut held = held.borrow_mut();
        held.retain(|weak| weak.strong_count() > 0);
        held.iter()
            .filter_map(Weak::upgrade)
            .find(|lease| lease.conn == owner && lease.db == db)
    }) {
        return Ok(Guard(Some(lease)));
    }
    let mut lock = db.as_os_str().to_owned();
    lock.push(".claude-write.lock");
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(PathBuf::from(lock))?;
    if let Err(error) = crate::file_lock::try_lock_exclusive(&file) {
        if lock_contended(&error) {
            return Err(rusqlite::Error::SqliteFailure(rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),Some("Claude source is being captured by another connection; retry after the source write completes".into())).into());
        }
        return Err(error.into());
    }
    let lease = Rc::new(Lease {
        conn: owner,
        db,
        file,
    });
    HELD.with(|held| held.borrow_mut().push(Rc::downgrade(&lease)));
    Ok(Guard(Some(lease)))
}

/// Entry points may use the normal bounded SQLite busy budget before opening a
/// transaction. A different connection on this thread must never wait on us:
/// it may be executing from a progress callback while our lease is held.
pub(crate) fn acquire_wait(conn: &Connection, source: &str) -> Result<Guard> {
    if source != "claude" || !conn.is_autocommit() || held_by_other_connection(conn)? {
        return acquire(conn, source);
    }
    let mut attempts = 0;
    loop {
        match acquire(conn, source) {
            Ok(guard) => return Ok(guard),
            Err(error) => {
                let busy = matches!(
                    error.downcast_ref::<rusqlite::Error>(),
                    Some(rusqlite::Error::SqliteFailure(code, _))
                        if code.code == rusqlite::ErrorCode::DatabaseBusy
                );
                if !busy || !crate::store::busy_retry_handler(attempts) {
                    return Err(error);
                }
                attempts += 1;
            }
        }
    }
}

fn held_by_other_connection(conn: &Connection) -> Result<bool> {
    let Some(path) = conn.path().filter(|path| !path.is_empty()) else {
        return Ok(false);
    };
    let db = std::fs::canonicalize(path)?;
    let owner = std::ptr::from_ref(conn) as usize;
    Ok(HELD.with(|held| {
        held.borrow()
            .iter()
            .filter_map(Weak::upgrade)
            .any(|lease| lease.conn != owner && lease.db == db)
    }))
}

fn lock_contended(error: &std::io::Error) -> bool {
    #[cfg(windows)]
    if error.raw_os_error() == fs2::lock_contended_error().raw_os_error() {
        return true;
    }
    error.kind() == std::io::ErrorKind::WouldBlock
}

pub(super) fn begin_sweep(conn: &Connection) -> Result<(Guard, super::SweepWrite<'_>)> {
    let guard = acquire_wait(conn, "claude")?;
    let write = super::SweepWrite::begin(conn)?;
    Ok((guard, write))
}

/// Only dense, known, local buckets are free of prior ordering constraints.
/// Unknown identity, memory stores, remote anchors and sparse/extreme legacy
/// ranks retain whole-read staging. Hold `guard` until the pass is settled.
pub(super) fn stage_complete(
    conn: &Connection,
    session: Option<&str>,
    guard: &Guard,
) -> Result<bool> {
    if !guard.fenced() {
        return Ok(true);
    }
    // Without explicit attribution, records may name multiple sessions. A
    // head/session metadata hint is not proof that every later row belongs to
    // it, so check all sessions captured by that source-wide collector.
    let constrained=conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM session_events WHERE source='claude' AND (?1 IS NULL OR session_id=?1) GROUP BY session_id,ts_ms HAVING COUNT(source_order)!=COUNT(*) OR SUM(location!='local')>0 OR MIN(source_order)!=0 OR MAX(source_order)>=COUNT(*) OR COUNT(DISTINCT source_order)!=COUNT(*))",
        [session],|row|row.get(0),
    )?;
    Ok(constrained)
}

pub(crate) fn begin_transaction<'a>(
    conn: &'a mut Connection,
    source: &str,
) -> Result<(Guard, rusqlite::Transaction<'a>)> {
    let guard = acquire_wait(conn, source)?;
    let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    Ok((guard, tx))
}

pub(super) fn boundaries<'a>(
    conn: &Connection,
    session: Option<&str>,
    complete: bool,
    guard: &Guard,
    staged: &'a mut dyn FnMut() -> Result<()>,
    bounded: &'a mut dyn FnMut() -> Result<()>,
) -> Result<&'a mut dyn FnMut() -> Result<()>> {
    if complete && stage_complete(conn, session, guard)? {
        Ok(staged)
    } else {
        Ok(bounded)
    }
}
