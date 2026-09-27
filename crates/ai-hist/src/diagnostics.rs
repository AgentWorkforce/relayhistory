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
/// Shells out to `df` rather than taking a libc dependency for one number;
/// this crate already shells out to `git` for the same reason.
pub(crate) fn free_bytes(path: &Path) -> Option<u64> {
    // df needs an existing path: fall back to the parent for a database that
    // has not been created yet.
    let target = if path.exists() {
        path.to_path_buf()
    } else {
        path.parent()?.to_path_buf()
    };
    let out = std::process::Command::new("df")
        .arg("-Pk")
        .arg(&target)
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&out.stdout);
    // Filesystem  1024-blocks  Used  Available  Capacity  Mounted-on
    let available_kb: u64 = text
        .lines()
        .nth(1)?
        .split_whitespace()
        .nth(3)?
        .parse()
        .ok()?;
    Some(available_kb * 1024)
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

    DoctorReport {
        db_bytes,
        wal_bytes,
        free,
        lock,
        holders,
        problems,
    }
}
