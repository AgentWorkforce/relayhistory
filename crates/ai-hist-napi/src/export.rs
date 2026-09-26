//! Local export RPC. Open snapshots live in this process: each holds one
//! read transaction over its store until it is closed or expires.
use ai_hist::export as core;
use napi_derive::napi;
use serde::Deserialize;
use std::sync::Mutex;

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

fn open_snapshots() -> std::sync::MutexGuard<'static, Vec<core::ExportSnapshot>> {
    OPEN.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
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
                    serde_json::to_value(handle)?
                }
                Request::ExportPage { cursor, now_ms } => {
                    let mut open = open_snapshots();
                    let snapshot = open
                        .iter_mut()
                        .find(|snapshot| snapshot.owns_cursor(&cursor))
                        .ok_or_else(|| anyhow::anyhow!("export cursor not found"))?;
                    serde_json::to_value(snapshot.page(&cursor, now_ms)?)?
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
