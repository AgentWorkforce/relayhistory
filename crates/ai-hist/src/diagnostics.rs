//! Database health inspection and ingestion failure diagnostics.
use crate::SourceDatabaseError;
use rusqlite::{Connection, ErrorCode};
use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};
/// Sidecar paths SQLite keeps beside the database in WAL mode.
pub(crate) fn wal_path(db_path: &Path) -> PathBuf {
    PathBuf::from(format!("{}-wal", db_path.display()))
}

/// Free bytes on the filesystem holding `path`.
///
/// Every sweep asks this before it writes, and the sweep is a watch tick:
/// it is one filesystem call (`statfs`/`statvfs` on Unix, `GetDiskFreeSpaceExW`
/// on Windows), never a spawned `df`, which cost a forced tick several
/// milliseconds of `posix_spawn` and pipe reads. The figure is the space this
/// user may write -- `f_bavail` on Unix, as `df -P` reports it.
pub(crate) fn free_bytes(path: &Path) -> Option<u64> {
    free_bytes_at(&measured_dir(path)?)
}

/// The directory whose filesystem holds `path`: the database may not exist
/// yet, and `GetDiskFreeSpaceExW` takes only a directory. A symlinked
/// database lives on its target's filesystem, so links are followed first --
/// with `read_link`, which also answers for a target not created yet. A bare
/// filename's parent is the empty path, which names no directory, so it is `.`.
fn measured_dir(path: &Path) -> Option<PathBuf> {
    let mut path = path.to_path_buf();
    // A cycle stops at the kernel's own link limit and measures where it is.
    for _ in 0..40 {
        let Ok(target) = fs::read_link(&path) else {
            break;
        };
        // A relative target is relative to the link's directory; joining an
        // absolute one replaces the base.
        path = match path.parent() {
            Some(dir) => dir.join(target),
            None => target,
        };
    }
    if path.is_dir() {
        return Some(path);
    }
    match path.parent()? {
        parent if parent.as_os_str().is_empty() => Some(PathBuf::from(".")),
        parent => Some(parent.to_path_buf()),
    }
}

/// Apple's `statvfs` reports block counts as 32-bit `fsblkcnt_t`, so a volume
/// with more than 2^32 free blocks (16 TiB at 4 KiB) wraps to a small number
/// and would trip the free-space floor. Its `statfs` carries 64-bit counts.
#[cfg(any(target_os = "macos", target_os = "ios"))]
fn free_bytes_at(target: &Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(target.as_os_str().as_bytes()).ok()?;
    let mut stats = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: `path` is a valid NUL-terminated string and `stats` points to
    // writable memory of the right size; `statfs` initializes it on success.
    let rc = unsafe { libc::statfs(path.as_ptr(), stats.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    // SAFETY: `statfs` returned 0, so the struct is initialized.
    let stats = unsafe { stats.assume_init() };
    stats.f_bavail.checked_mul(u64::from(stats.f_bsize))
}

/// `statvfs`, whose block counts are 64-bit on Linux and the other Unixes
/// this builds for.
#[cfg(all(unix, not(any(target_os = "macos", target_os = "ios"))))]
fn free_bytes_at(target: &Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(target.as_os_str().as_bytes()).ok()?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `path` is a valid NUL-terminated string and `stats` points to
    // writable memory of the right size; `statvfs` initializes it on success.
    let rc = unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    // SAFETY: `statvfs` returned 0, so the struct is initialized.
    let stats = unsafe { stats.assume_init() };
    #[allow(clippy::unnecessary_cast)] // the field widths differ by platform
    let (available, fragment) = (stats.f_bavail as u64, stats.f_frsize as u64);
    available.checked_mul(fragment)
}

/// `GetDiskFreeSpaceExW`: a 64-bit byte count (the cluster counts of
/// `GetDiskFreeSpaceW` are 32-bit) that honors per-user quotas, like
/// `f_bavail`.
#[cfg(windows)]
fn free_bytes_at(dir: &Path) -> Option<u64> {
    use std::os::windows::ffi::OsStrExt;
    // A UNC directory -- including the `\\?\UNC\` form `canonicalize`
    // returns -- must end in a backslash; a local one may.
    let mut wide: Vec<u16> = dir.as_os_str().encode_wide().collect();
    if !matches!(wide.last(), Some(&c) if c == u16::from(b'\\') || c == u16::from(b'/')) {
        wide.push(u16::from(b'\\'));
    }
    wide.push(0);
    let mut available = 0u64;
    // SAFETY: `wide` is a NUL-terminated UTF-16 path; the out-pointer is a
    // valid `u64` and the unused outputs may be null.
    let ok = unsafe {
        windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW(
            wide.as_ptr(),
            &mut available,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    (ok != 0).then_some(available)
}

/// A process holding the database file open, and whether it can still release it.
pub struct DbHolder {
    pub pid: String,
    pub state: String,
    pub command: String,
}

impl DbHolder {
    /// Stopped (`T`) and zombie (`Z`) processes never run again on their own, so
    /// a write transaction they hold is held forever -- no busy timeout escapes
    /// it. This is the condition that wedged sync for days.
    pub fn is_wedged(&self) -> bool {
        self.state.starts_with('T') || self.state.starts_with('Z')
    }
}

/// Processes with the database open, via `lsof`, annotated with `ps` state.
pub(crate) fn db_holders(db_path: &Path) -> Vec<DbHolder> {
    // macOS keeps lsof in /usr/sbin, which is commonly absent from the PATH of
    // launchd jobs and embedded hosts. Try PATH first, then stable system
    // locations so automatic diagnostics do not silently lose their evidence.
    let out = ["lsof", "/usr/sbin/lsof", "/usr/bin/lsof"]
        .iter()
        .find_map(|program| {
            std::process::Command::new(program)
                .arg("-t")
                .arg(db_path)
                .output()
                .ok()
                .filter(|out| out.status.success())
        });
    let Some(out) = out else {
        return Vec::new();
    };
    let own_pid = std::process::id().to_string();
    String::from_utf8_lossy(&out.stdout)
        .split_whitespace()
        // Our own read-only handle is not a finding.
        .filter(|pid| *pid != own_pid)
        .filter_map(|pid| {
            let (state, command) = process_status(pid)?;
            Some(DbHolder {
                pid: pid.to_string(),
                state,
                command,
            })
        })
        .collect()
}

/// Process state with stable-path fallbacks for reduced-PATH launchd and embedded hosts.
pub(crate) fn process_status(pid: &str) -> Option<(String, String)> {
    process_status_with_programs(pid, &["ps", "/bin/ps", "/usr/bin/ps"])
}

pub(crate) fn process_status_with_programs(
    pid: &str,
    programs: &[&str],
) -> Option<(String, String)> {
    programs.iter().find_map(|program| {
        let output = std::process::Command::new(program)
            .args(["-o", "stat=,command=", "-p", pid])
            .output()
            .ok()
            .filter(|output| output.status.success())?;
        let line = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let (state, command) = line.split_once(char::is_whitespace)?;
        Some((state.to_string(), command.trim().to_string()))
    })
}

/// Can a writer actually start right now?
///
/// Uses a short timeout on purpose: `doctor` should report a wedged database
/// promptly rather than inherit the production retry sequence.
pub(crate) fn probe_write_lock(db_path: &Path) -> std::result::Result<(), String> {
    let conn = Connection::open(db_path).map_err(|err| err.to_string())?;
    let _ = conn.busy_timeout(Duration::from_millis(1500));
    conn.execute_batch("BEGIN IMMEDIATE; ROLLBACK;")
        .map_err(|err| err.to_string())
}

pub(crate) fn is_sqlite_contention(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<rusqlite::Error>(),
            Some(rusqlite::Error::SqliteFailure(code, _))
                if matches!(code.code, ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
        )
    })
}

/// Explain a write-contention failure using a fresh capability probe.
///
/// `lsof` proves only that a process has the file open. The `BEGIN IMMEDIATE`
/// probe below establishes whether a writer can actually start *now*; holder
/// output is deliberately phrased as causal only while that probe is blocked.
pub(crate) fn write_contention_diagnostic(db_path: &Path) -> String {
    let lock = probe_write_lock(db_path);
    let holders = db_holders(db_path);
    let wal_bytes = fs::metadata(wal_path(db_path))
        .map(|m| m.len())
        .unwrap_or(0);
    let mut lines = vec![match &lock {
        Ok(()) => "write capability probe now succeeds; the contention was transient".to_string(),
        Err(err) => format!("write capability probe is still blocked: {err}"),
    }];

    if lock.is_err() {
        let wedged: Vec<_> = holders.iter().filter(|holder| holder.is_wedged()).collect();
        if wedged.is_empty() {
            if holders.is_empty() {
                lines.push(
                    "no file-open holder was detected; SQLite does not expose lock ownership"
                        .to_string(),
                );
            } else {
                lines.push(format!(
                    "{} process(es) have the database open, but none is stopped or zombie; file-open status does not prove lock ownership",
                    holders.len()
                ));
            }
        } else {
            for holder in wedged {
                lines.push(format!(
                    "pid {} is {} with the database open; if it owns the transaction it cannot release it until resumed (kill -CONT {})",
                    holder.pid, holder.state, holder.pid
                ));
            }
        }
    }
    if let Some(wal_line) = wal_contention_line(wal_bytes, lock.is_err()) {
        lines.push(wal_line);
    }

    format!("ai-hist contention diagnostic: {}", lines.join("; "))
}

pub(crate) fn wal_contention_line(wal_bytes: u64, write_blocked: bool) -> Option<String> {
    if wal_bytes <= WAL_WARN_BYTES {
        return None;
    }
    Some(if write_blocked {
        format!(
            "WAL is {} while the write path is failing; checkpoint progress is starved",
            human_bytes(wal_bytes)
        )
    } else {
        format!(
            "WAL is {} after write capability recovered; a long-lived reader may still be delaying checkpoints",
            human_bytes(wal_bytes)
        )
    })
}

pub(crate) fn enrich_sync_error(db_path: &Path, error: anyhow::Error) -> anyhow::Error {
    if is_sqlite_contention(&error) {
        let diagnostic_path = source_database_path(&error)
            .map(Path::to_path_buf)
            .unwrap_or_else(|| db_path.to_path_buf());
        error.context(write_contention_diagnostic(&diagnostic_path))
    } else {
        error
    }
}

pub(crate) fn source_database_path(error: &anyhow::Error) -> Option<&Path> {
    error.chain().find_map(|cause| {
        cause
            .downcast_ref::<SourceDatabaseError>()
            .map(SourceDatabaseError::path)
    })
}

pub fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["B", "KB", "MB", "GB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

/// WAL beyond this points at checkpoint starvation: a long-lived reader is
/// pinning an old snapshot so SQLite cannot reclaim frames.
pub(crate) const WAL_WARN_BYTES: u64 = 64 * 1024 * 1024;

/// Below this, a write can fail partway and leave torn state behind, which is
/// how the `.sync-state.json` corruption started.
pub(crate) const FREE_SPACE_FLOOR_BYTES: u64 = 512 * 1024 * 1024;

/// Everything `doctor` measured about one database, before it is rendered as
/// text or JSON. Split out from the printing so the report can be asserted on.
pub struct DoctorReport {
    pub db_bytes: u64,
    pub wal_bytes: u64,
    /// Bytes of the database file sitting on SQLite's freelist: space a
    /// `compact` would hand back to the volume. `None` when the database could
    /// not be opened to ask.
    pub reclaimable: Option<u64>,
    pub free: Option<u64>,
    pub lock: std::result::Result<(), String>,
    pub holders: Vec<DbHolder>,
    pub problems: Vec<String>,
}

pub fn doctor_report(db_path: &Path) -> DoctorReport {
    let db_bytes = fs::metadata(db_path).map(|m| m.len()).unwrap_or(0);
    let wal_bytes = fs::metadata(wal_path(db_path))
        .map(|m| m.len())
        .unwrap_or(0);
    let free = free_bytes(db_path);
    let reclaimable = page_usage(db_path).ok().map(|usage| usage.free_bytes());
    let lock = probe_write_lock(db_path);
    let holders = db_holders(db_path);

    let mut problems: Vec<String> = Vec::new();
    if let Err(err) = &lock {
        problems.push(format!("write lock unavailable: {err}"));
    }
    // Having the file open is not the same as owning a write transaction, and
    // SQLite will not say who holds the lock. So only assert causation when a
    // writer is actually blocked; otherwise report the stopped process as a
    // risk, which is true without overclaiming.
    for holder in holders.iter().filter(|h| h.is_wedged()) {
        if lock.is_err() {
            problems.push(format!(
                "pid {} is {} and holds the database open; if it is mid-transaction it can never release the write lock (resume it: kill -CONT {})",
                holder.pid, holder.state, holder.pid
            ));
        } else {
            problems.push(format!(
                "pid {} is {} and holds the database open; writes work now, but it will wedge them if it stops mid-transaction (resume it: kill -CONT {})",
                holder.pid, holder.state, holder.pid
            ));
        }
    }
    if wal_bytes > WAL_WARN_BYTES {
        problems.push(format!(
            "WAL is {} -- checkpointing is starved, usually by a long-lived reader",
            human_bytes(wal_bytes)
        ));
    }
    if free.is_some_and(|free| free < FREE_SPACE_FLOOR_BYTES) {
        problems.push(format!(
            "only {} free -- writes can fail partway and leave torn state",
            human_bytes(free.unwrap_or(0))
        ));
    }

    if let Some(reclaimable) = reclaimable.filter(|bytes| worth_compacting(*bytes, db_bytes)) {
        problems.push(format!(
            "{} of the database file is free pages -- `ai-hist compact` returns it to the volume",
            human_bytes(reclaimable)
        ));
    }

    DoctorReport {
        db_bytes,
        wal_bytes,
        reclaimable,
        free,
        lock,
        holders,
        problems,
    }
}

/// Free pages below this are not worth a full rewrite of the database.
const RECLAIM_WARN_BYTES: u64 = 64 * 1024 * 1024;

/// Whether `doctor` should point at `compact`: enough free pages to matter in
/// absolute terms, and a real share of the file rather than normal churn.
fn worth_compacting(reclaimable: u64, db_bytes: u64) -> bool {
    reclaimable >= RECLAIM_WARN_BYTES && reclaimable.saturating_mul(4) >= db_bytes
}

/// How the database file's pages are spent, from SQLite's own accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageUsage {
    pub page_size: u64,
    pub page_count: u64,
    pub freelist_count: u64,
}

impl PageUsage {
    /// Bytes on the freelist: allocated to the file, holding nothing.
    pub fn free_bytes(&self) -> u64 {
        self.freelist_count.saturating_mul(self.page_size)
    }

    /// Bytes of live pages: roughly what a `VACUUM` writes out again.
    pub fn live_bytes(&self) -> u64 {
        self.page_count
            .saturating_sub(self.freelist_count)
            .saturating_mul(self.page_size)
    }
}

pub(crate) fn read_page_usage(conn: &Connection) -> rusqlite::Result<PageUsage> {
    let pragma = |name: &str| -> rusqlite::Result<u64> {
        conn.query_row(&format!("PRAGMA {name}"), [], |row| row.get::<_, i64>(0))
            .map(|value| value.max(0) as u64)
    };
    Ok(PageUsage {
        page_size: pragma("page_size")?,
        page_count: pragma("page_count")?,
        freelist_count: pragma("freelist_count")?,
    })
}

/// Page accounting through a read-only handle, so asking never takes a lock a
/// writer is waiting on and never creates a database that is not there.
pub(crate) fn page_usage(db_path: &Path) -> rusqlite::Result<PageUsage> {
    let conn = Connection::open_with_flags(
        db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    let _ = conn.busy_timeout(Duration::from_millis(1500));
    read_page_usage(&conn)
}

/// The full-text indexes `compact` merges. Each is an external-content FTS5
/// table, so `optimize` rewrites only its own segments into one b-tree.
const COMPACTED_FTS_TABLES: &[&str] = &["history_fts", "session_events_fts"];

/// What one [`compact_database`] run found and did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactReport {
    pub db_bytes_before: u64,
    pub wal_bytes_before: u64,
    pub db_bytes_after: u64,
    pub wal_bytes_after: u64,
    /// Freelist bytes before the rewrite, which the `VACUUM` gives back.
    pub reclaimable_before: u64,
    /// Full-text indexes whose segments were merged.
    pub fts_optimized: Vec<&'static str>,
    /// Whether the final `wal_checkpoint(TRUNCATE)` completed. `false` when a
    /// reader holding an older snapshot kept SQLite from resetting the WAL:
    /// the rewrite still happened, but the WAL keeps its size until a later
    /// checkpoint runs with no reader in the way.
    pub wal_truncated: bool,
}

impl CompactReport {
    /// Bytes the database and its WAL together occupy less than before.
    /// Negative when the rewrite grew them.
    pub fn saved_bytes(&self) -> i128 {
        (self.db_bytes_before as i128 + self.wal_bytes_before as i128)
            - (self.db_bytes_after as i128 + self.wal_bytes_after as i128)
    }
}

fn file_len(path: &Path) -> u64 {
    fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

/// Why [`compact_database`] refused before writing anything.
#[derive(Debug)]
pub enum CompactRefused {
    /// Another process is syncing this database; compacting under it would
    /// stall its writes for the whole rewrite.
    SyncRunning,
    /// The rewrite needs room for a second copy of the live pages (the
    /// `VACUUM` temp file) and for the WAL it writes them through, plus the
    /// floor below which any write risks torn state. Running out of space
    /// midway is how #44 began, so this is checked up front.
    InsufficientSpace { needed: u64, free: u64 },
    /// The volume's free space could not be measured (`df` missing or
    /// unreadable). The space guard fails closed rather than starting a
    /// rewrite it cannot vouch for.
    SpaceUnknown { needed: u64 },
}

impl std::fmt::Display for CompactRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SyncRunning => write!(
                f,
                "a sync is running against this database; compact again once it finishes"
            ),
            Self::InsufficientSpace { needed, free } => write!(
                f,
                "compacting needs about {} free on the database's volume and only {} is free",
                human_bytes(*needed),
                human_bytes(*free)
            ),
            Self::SpaceUnknown { needed } => write!(
                f,
                "compacting needs about {} free on the database's volume and the free space could not be measured",
                human_bytes(*needed)
            ),
        }
    }
}

impl std::error::Error for CompactRefused {}

/// Reclaim the space a history database holds but no longer uses.
///
/// Deletes nothing: every row survives. It merges each full-text index's
/// segments, rewrites the file with `VACUUM` so freelist pages go back to the
/// volume, and truncates the WAL. Both indexes are keyed on an explicit
/// `INTEGER PRIMARY KEY`, so the rowids `VACUUM` preserves still line up.
///
/// Holds the sync run lock for the duration, so a concurrent `sync` or `watch`
/// tick skips rather than blocking on the rewrite, and refuses outright when
/// the volume cannot hold the rewrite.
///
/// Other crate writers that do not take the sync lock (targeted hydration,
/// discovery) wait behind the rewrite through their busy handler like behind
/// any long write.
pub fn compact_database(db_path: &Path) -> anyhow::Result<CompactReport> {
    compact_database_measured(db_path, free_bytes)
}

/// Refuse unless the volume can hold a rewrite of `usage`'s live pages.
fn ensure_room_to_compact(
    db_path: &Path,
    usage: PageUsage,
    measure_free: &impl Fn(&Path) -> Option<u64>,
) -> anyhow::Result<()> {
    let needed = usage
        .live_bytes()
        .saturating_mul(2)
        .saturating_add(FREE_SPACE_FLOOR_BYTES);
    let Some(free) = measure_free(db_path) else {
        return Err(CompactRefused::SpaceUnknown { needed }.into());
    };
    if free < needed {
        return Err(CompactRefused::InsufficientSpace { needed, free }.into());
    }
    Ok(())
}

fn compact_database_measured(
    db_path: &Path,
    measure_free: impl Fn(&Path) -> Option<u64>,
) -> anyhow::Result<CompactReport> {
    anyhow::ensure!(db_path.exists(), "no database at {}", db_path.display());
    let Some(_sync) = crate::ingest::try_acquire_sync_lock(db_path)? else {
        return Err(CompactRefused::SyncRunning.into());
    };
    let db_bytes_before = file_len(db_path);
    let wal_bytes_before = file_len(&wal_path(db_path));
    // Measured through a read-only handle before anything opens the database
    // writable, so a refusal comes before any write.
    let before = page_usage(db_path)?;
    ensure_room_to_compact(db_path, before, &measure_free)?;
    // Not `open_db`: compacting rewrites the pages as they are, and a schema
    // migration on open would be a write -- one that can grow the live pages
    // -- after the space was vouched for.
    let conn = Connection::open(db_path)?;
    crate::store::configure_busy_retry(&conn)?;
    // The sync lock keeps sweeps out, but a hydration or discovery takes no
    // such lock and can commit between the measurement above and the first
    // write here. So the space is vouched for again on this handle, holding
    // SQLite's write lock, and the index merges run under that same lock.
    conn.execute_batch("BEGIN IMMEDIATE;")?;
    let merged = (|| -> anyhow::Result<Vec<&'static str>> {
        ensure_room_to_compact(db_path, read_page_usage(&conn)?, &measure_free)?;
        let mut merged = Vec::new();
        for table in COMPACTED_FTS_TABLES {
            let exists: bool = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?)",
                [table],
                |row| row.get(0),
            )?;
            if exists {
                conn.execute_batch(&format!(
                    "INSERT INTO {table}({table}) VALUES('optimize');"
                ))?;
                merged.push(*table);
            }
        }
        Ok(merged)
    })();
    let fts_optimized = match merged {
        Ok(merged) => {
            conn.execute_batch("COMMIT;")?;
            merged
        }
        Err(error) => {
            let _ = conn.execute_batch("ROLLBACK;");
            return Err(error);
        }
    };
    // Best effort: a busy result here only means VACUUM writes through a
    // larger WAL, and the final checkpoint below is the one reported.
    truncate_wal(&conn)?;
    conn.execute_batch("VACUUM;")?;
    let wal_truncated = truncate_wal(&conn)?;
    drop(conn);
    Ok(CompactReport {
        db_bytes_before,
        wal_bytes_before,
        db_bytes_after: file_len(db_path),
        wal_bytes_after: file_len(&wal_path(db_path)),
        reclaimable_before: before.free_bytes(),
        fts_optimized,
        wal_truncated,
    })
}

/// Run `wal_checkpoint(TRUNCATE)` and say whether it completed. SQLite reports
/// a reader that blocked the reset as `busy = 1` in the result row, not as an
/// error, so the row has to be read.
fn truncate_wal(conn: &Connection) -> rusqlite::Result<bool> {
    conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
        row.get::<_, i64>(0)
    })
    .map(|busy| busy == 0)
}

#[cfg(test)]
mod compact_tests {
    use super::*;

    fn seeded(dir: &Path) -> PathBuf {
        let db_path = dir.join("ai-history.db");
        let conn = crate::open_db(&db_path).unwrap();
        let text = "x".repeat(2_000);
        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..4_000 {
            tx.execute(
                "INSERT INTO history (source, session_id, prompt, timestamp_ms) \
                 VALUES ('claude', ?1, ?2, ?3)",
                rusqlite::params![format!("s{}", i % 40), format!("{text} n{i}"), i],
            )
            .unwrap();
        }
        tx.execute(
            "INSERT INTO trajectories \
             (id, decisions_json, retrospective_json, search_text, updated_ms, timestamp_ms) \
             VALUES ('t-gone', '[]', '{}', 'obsolete', 1, 1), \
                    ('t-kept', '[]', '{}', 'needle trajectory', 2, 2)",
            [],
        )
        .unwrap();
        tx.commit().unwrap();
        // Free pages: the rows a retention pass or a rewrite would drop.
        conn.execute("DELETE FROM history WHERE timestamp_ms % 4 <> 0", [])
            .unwrap();
        conn.execute("DELETE FROM trajectories WHERE id = 't-gone'", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
        db_path
    }

    /// `statfs`/`statvfs` report the figure `df -P` did. Free space moves under a
    /// running test suite, so the two are compared loosely.
    #[cfg(unix)]
    #[test]
    fn free_bytes_agrees_with_df() {
        let dir = tempfile::tempdir().unwrap();
        let measured = free_bytes(&dir.path().join("not-yet.db")).expect("statfs/statvfs answers");
        let out = std::process::Command::new("df")
            .arg("-Pk")
            .arg(dir.path())
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "df -Pk failed ({}): {}",
            out.status,
            String::from_utf8_lossy(&out.stderr)
        );
        let text = String::from_utf8_lossy(&out.stdout);
        let df: u64 = text
            .lines()
            .nth(1)
            .and_then(|line| line.split_whitespace().nth(3))
            .and_then(|kb| kb.parse::<u64>().ok())
            .expect("df -Pk prints available kilobytes")
            * 1024;
        let tolerance = (df / 20).max(2 << 30);
        assert!(
            measured.abs_diff(df) <= tolerance,
            "free_bytes {measured} vs df {df}"
        );
        assert!(free_bytes(Path::new("/definitely/not/a/dir/db")).is_none());
    }

    /// A bare filename such as `AI_HIST_DB=ai-history.db` lives in the current
    /// directory; its empty parent must not turn into an unknown measurement.
    #[test]
    fn free_bytes_measures_a_bare_filename_in_the_current_directory() {
        assert_eq!(
            measured_dir(Path::new("ai-history.db")),
            Some(PathBuf::from("."))
        );
        assert!(free_bytes(Path::new("ai-history.db")).is_some());
    }

    /// A database symlinked onto another volume is measured where its bytes
    /// are written, not where the link sits -- including on a first run, when
    /// the link's target does not exist yet.
    #[cfg(unix)]
    #[test]
    fn free_bytes_measures_a_symlinked_database_at_its_target() {
        let link_dir = tempfile::tempdir().unwrap();
        let data_dir = tempfile::tempdir().unwrap();
        let db = data_dir.path().join("history.db");
        let link = link_dir.path().join("history.db");
        std::os::unix::fs::symlink(&db, &link).unwrap();
        assert_eq!(measured_dir(&link), Some(data_dir.path().to_path_buf()));
        fs::write(&db, b"").unwrap();
        assert_eq!(measured_dir(&link), Some(data_dir.path().to_path_buf()));

        let relative = link_dir.path().join("relative.db");
        std::os::unix::fs::symlink("nested/history.db", &relative).unwrap();
        assert_eq!(measured_dir(&relative), Some(link_dir.path().join("nested")));
    }

    #[test]
    fn compact_returns_free_pages_and_keeps_every_row_searchable() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = seeded(dir.path());
        let before = page_usage(&db_path).unwrap();
        assert!(before.free_bytes() > 0, "the fixture must leave free pages");
        let report = doctor_report(&db_path);
        assert_eq!(report.reclaimable, Some(before.free_bytes()));

        let compacted = compact_database(&db_path).unwrap();
        assert_eq!(compacted.reclaimable_before, before.free_bytes());
        assert!(
            compacted.db_bytes_after < compacted.db_bytes_before,
            "{compacted:?}"
        );
        assert!(compacted.saved_bytes() > 0);
        assert_eq!(compacted.wal_bytes_after, 0);
        assert!(compacted.wal_truncated);
        assert_eq!(
            compacted.fts_optimized,
            vec!["history_fts", "session_events_fts"]
        );
        assert_eq!(page_usage(&db_path).unwrap().freelist_count, 0);

        let conn = crate::open_db(&db_path).unwrap();
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM history", [], |row| row.get(0))
            .unwrap();
        assert_eq!(rows, 1_000);
        // The full-text indexes still answer, and still point at live rows.
        let hits: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM history_fts f JOIN history h ON f.rowid = h.id \
                 WHERE history_fts MATCH 'n3996'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(hits, 1);
        for table in COMPACTED_FTS_TABLES {
            conn.execute_batch(&format!(
                "INSERT INTO {table}({table}, rank) VALUES('integrity-check', 1);"
            ))
            .unwrap_or_else(|error| panic!("{table} is out of step after compact: {error}"));
        }
    }

    #[test]
    fn compact_refuses_while_a_sync_holds_the_database() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = seeded(dir.path());
        let before = file_len(&db_path);
        let _sync = crate::ingest::try_acquire_sync_lock(&db_path)
            .unwrap()
            .expect("the lock is free");
        let error = compact_database(&db_path).unwrap_err();
        assert!(
            matches!(
                error.downcast_ref::<CompactRefused>(),
                Some(CompactRefused::SyncRunning)
            ),
            "{error:#}"
        );
        assert_eq!(file_len(&db_path), before, "a refusal writes nothing");
    }

    #[test]
    fn compact_measures_again_under_the_write_lock_before_writing() {
        // A hydration that commits between the first measurement and the
        // writable open can spend the room that measurement saw. The second
        // answer, taken holding SQLite's write lock, is the one that counts.
        let dir = tempfile::tempdir().unwrap();
        let db_path = seeded(dir.path());
        let before = file_len(&db_path);
        let calls = std::cell::Cell::new(0);
        let error = compact_database_measured(&db_path, |_| {
            calls.set(calls.get() + 1);
            Some(if calls.get() == 1 { u64::MAX } else { 0 })
        })
        .unwrap_err();
        assert!(
            matches!(
                error.downcast_ref::<CompactRefused>(),
                Some(CompactRefused::InsufficientSpace { free: 0, .. })
            ),
            "{error:#}"
        );
        assert_eq!(calls.get(), 2);
        assert_eq!(file_len(&db_path), before, "a refusal writes nothing");
        assert!(page_usage(&db_path).unwrap().freelist_count > 0);
    }

    #[test]
    fn compact_refuses_when_free_space_cannot_be_measured() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = seeded(dir.path());
        let before = file_len(&db_path);
        let error = compact_database_measured(&db_path, |_| None).unwrap_err();
        assert!(
            matches!(
                error.downcast_ref::<CompactRefused>(),
                Some(CompactRefused::SpaceUnknown { .. })
            ),
            "{error:#}"
        );
        assert_eq!(file_len(&db_path), before, "a refusal writes nothing");
        assert!(page_usage(&db_path).unwrap().freelist_count > 0);
    }

    #[test]
    fn compact_refuses_for_space_before_migrating_an_older_database() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = seeded(dir.path());
        // An index the current schema requires: opening writable would
        // migrate it back into place.
        Connection::open(&db_path)
            .unwrap()
            .execute_batch("DROP INDEX idx_sessions_recency;")
            .unwrap();
        let error = compact_database_measured(&db_path, |_| Some(0)).unwrap_err();
        assert!(
            matches!(
                error.downcast_ref::<CompactRefused>(),
                Some(CompactRefused::InsufficientSpace { free: 0, .. })
            ),
            "{error:#}"
        );
        let migrated: bool = Connection::open(&db_path)
            .unwrap()
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name = 'idx_sessions_recency')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!migrated, "the refusal must come before any migration");
    }

    #[test]
    fn a_wal_checkpoint_blocked_by_a_reader_is_reported_not_claimed() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = seeded(dir.path());
        let writer = Connection::open(&db_path).unwrap();
        writer.busy_timeout(Duration::ZERO).unwrap();
        writer
            .execute("DELETE FROM history WHERE timestamp_ms < 100", [])
            .unwrap();
        let reader = Connection::open(&db_path).unwrap();
        reader.execute_batch("BEGIN;").unwrap();
        let _: i64 = reader
            .query_row("SELECT COUNT(*) FROM history", [], |row| row.get(0))
            .unwrap();
        // No busy handler: the pinned snapshot shows up as busy at once.
        assert!(!truncate_wal(&writer).unwrap());
        assert!(file_len(&wal_path(&db_path)) > 0);
        reader.execute_batch("COMMIT;").unwrap();
        assert!(truncate_wal(&writer).unwrap());
        assert_eq!(file_len(&wal_path(&db_path)), 0);
    }

    #[test]
    fn doctor_points_at_compact_only_when_it_would_matter() {
        assert!(!worth_compacting(RECLAIM_WARN_BYTES - 1, RECLAIM_WARN_BYTES));
        assert!(!worth_compacting(RECLAIM_WARN_BYTES, 8 * RECLAIM_WARN_BYTES));
        assert!(worth_compacting(RECLAIM_WARN_BYTES, 4 * RECLAIM_WARN_BYTES));
    }

    #[test]
    fn compact_refuses_a_database_that_is_not_there() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("absent.db");
        assert!(compact_database(&db_path).is_err());
        assert!(!db_path.exists(), "compact must not create a database");
    }
}
