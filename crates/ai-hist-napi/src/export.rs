//! Local export RPC. Open snapshots live in this process: each holds one
//! read transaction over its store until it is closed or expires.
use ai_hist::export as core;
use napi_derive::napi;
use serde::Deserialize;
use std::sync::{Condvar, Mutex, Once};
use std::time::Duration;

#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Request {
    CreateExport {
        selection: core::ExportSelection,
        limits: core::ExportLimits,
        ttl_ms: i64,
        now_ms: i64,
    },
    ExportPage {
        cursor: String,
        now_ms: i64,
    },
    CloseExport {
        snapshot_id: String,
    },
    ExpireExports {
        now_ms: i64,
        limit: usize,
    },
}
// Core caps the decoded selection at 64 KiB. The wire envelope also carries
// metadata and may encode each ASCII character as a six-byte Unicode escape.
const MAX_EXPORT_REQUEST_BYTES: usize = 6 * 65_536 + 4_096;
/// Most snapshots open at once across every store.
const MAX_OPEN_EXPORTS: usize = 32;
static OPEN: Mutex<Vec<core::ExportSnapshot>> = Mutex::new(Vec::new());
/// Signalled whenever a snapshot is added, so the reaper recomputes when the
/// earliest one expires.
static REAPER_WAKE: Condvar = Condvar::new();
static REAPER: Once = Once::new();
/// Longest the reaper sleeps while a snapshot is open; see `start_reaper`.
const REAPER_MAX_WAIT_MS: u64 = 1_000;

fn open_snapshots() -> std::sync::MutexGuard<'static, Vec<core::ExportSnapshot>> {
    OPEN.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// The wall clock in Unix milliseconds: the clock the SDK stamps `now_ms`
/// with (`Date.now()`), so a snapshot's `expires_at_ms` means the same
/// instant here.
fn wall_clock_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
        })
}

/// Start, once per process, the thread that releases snapshots when their
/// TTL elapses.
///
/// Without it an abandoned snapshot -- a paging handle the caller dropped
/// without closing -- kept its read transaction until some later export call
/// happened to sweep, and in a long-lived host that might be never: WAL
/// checkpoints could not pass its view and the WAL grew without bound
/// (#306). The thread sleeps until the earliest expiry (or until a new
/// snapshot arrives) and holds no libuv handle, so it never keeps an
/// otherwise finished Node process alive. Explicit close, cursor replay and
/// the RPC-driven sweeps are unchanged; this only adds a deadline nobody has
/// to call.
fn start_reaper() {
    REAPER.call_once(|| {
        // If the thread cannot be spawned, the RPC-driven sweeps still
        // release expired snapshots, exactly as before.
        let _ = std::thread::Builder::new()
            .name("ai-hist-export-reaper".into())
            .spawn(|| {
                let mut open = open_snapshots();
                loop {
                    let now = wall_clock_ms();
                    expire(&mut open, now, usize::MAX);
                    let next = open
                        .iter()
                        .map(|snapshot| snapshot.handle().expires_at_ms)
                        .min();
                    open = match next {
                        None => REAPER_WAKE
                            .wait(open)
                            .unwrap_or_else(|poisoned| poisoned.into_inner()),
                        Some(at) => {
                            // Expiry is a wall-clock instant but the wait is
                            // a monotonic duration, so a clock that jumps
                            // forward would otherwise leave an expired
                            // snapshot open for up to its whole TTL. Waking
                            // at least once a second while anything is open
                            // re-reads the wall clock; it costs nothing when
                            // no snapshot is open.
                            let wait =
                                at.saturating_sub(now).clamp(1, REAPER_MAX_WAIT_MS as i64) as u64;
                            REAPER_WAKE
                                .wait_timeout(open, Duration::from_millis(wait))
                                .map_or_else(|poisoned| poisoned.into_inner().0, |(guard, _)| guard)
                        }
                    };
                }
            });
    });
}

/// Close up to `limit` snapshots expired at `now_ms`, returning how many.
fn expire(open: &mut Vec<core::ExportSnapshot>, now_ms: i64, limit: usize) -> usize {
    let mut closed = 0;
    open.retain(|snapshot| {
        let expire = closed < limit && snapshot.expired(now_ms);
        closed += usize::from(expire);
        !expire
    });
    closed
}

#[napi]
pub async fn history_export(request_json: String, db_path: Option<String>) -> napi::Result<String> {
    if request_json.len() > MAX_EXPORT_REQUEST_BYTES {
        return Err(crate::native_error(
            "INVALID_ARGUMENT",
            "export request exceeds bounded envelope limit",
        ));
    }
    let request: Request = serde_json::from_str(&request_json)
        .map_err(|_| crate::native_error("INVALID_ARGUMENT", "invalid export request"))?;
    let path = crate::db_path(db_path);
    napi::tokio::task::spawn_blocking(move || {
        // Only a new snapshot opens the store; every other operation acts on
        // snapshots this process already holds.
        let conn = match request {
            Request::CreateExport { .. } => {
                Some(ai_hist::open_db(&path).map_err(|e| crate::database_error(&path, e))?)
            }
            _ => None,
        };
        let value = (move || -> anyhow::Result<serde_json::Value> {
            Ok(match request {
                Request::CreateExport {
                    selection,
                    limits,
                    ttl_ms,
                    now_ms,
                } => {
                    let conn = conn.expect("a create request opened the store");
                    let snapshot =
                        core::ExportSnapshot::open(conn, &selection, &limits, ttl_ms, now_ms)?;
                    let handle = snapshot.handle();
                    let mut open = open_snapshots();
                    expire(&mut open, now_ms, usize::MAX);
                    anyhow::ensure!(
                        open.len() < MAX_OPEN_EXPORTS,
                        "maximum open exports reached; close or expire old snapshots"
                    );
                    open.push(snapshot);
                    drop(open);
                    start_reaper();
                    REAPER_WAKE.notify_all();
                    serde_json::to_value(handle)?
                }
                Request::ExportPage { cursor, now_ms } => {
                    let mut open = open_snapshots();
                    let index = open
                        .iter()
                        .position(|snapshot| snapshot.owns_cursor(&cursor))
                        .ok_or_else(|| anyhow::anyhow!("export cursor not found"))?;
                    // An expired snapshot is released as soon as its cursor
                    // comes back, so its read transaction never outlives it
                    // and holds back WAL checkpoints.
                    if open[index].expired(now_ms) {
                        open.remove(index);
                        anyhow::bail!("export snapshot expired");
                    }
                    let page = open[index].page(&cursor, now_ms)?;
                    expire(&mut open, now_ms, MAX_OPEN_EXPORTS);
                    serde_json::to_value(page)?
                }
                Request::CloseExport { snapshot_id } => {
                    open_snapshots().retain(|snapshot| snapshot.snapshot_id() != snapshot_id);
                    serde_json::Value::Null
                }
                Request::ExpireExports { now_ms, limit } => {
                    anyhow::ensure!(
                        (1..=MAX_OPEN_EXPORTS).contains(&limit),
                        "invalid export cleanup limit"
                    );
                    serde_json::to_value(expire(&mut open_snapshots(), now_ms, limit))?
                }
            })
        })()
        .map_err(|e| crate::native_error("HISTORY_EXPORT_FAILED", e))?;
        serde_json::to_string(&value).map_err(|e| crate::native_error("HISTORY_EXPORT_FAILED", e))
    })
    .await
    .map_err(crate::worker_error)?
}
