//! Storage-only export RPC. Upload entry points report their migration explicitly.
use ai_hist::export as core;
use napi_derive::napi;
use serde::Deserialize;

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
    RetainedBytes,
    SetRetentionLimit {
        max_bytes: i64,
    },
    CompactJournal {
        limit: usize,
    },
}
fn moved() -> napi::Error {
    crate::native_error("HISTORY_DELIVERY_MOVED", "Upload jobs are owned by agent-relay-probe / @relayhistory/capture. Use that package to manage existing jobs; local history and export remain available here.")
}
// Core caps the decoded selection at 64 KiB. The wire envelope also carries
// metadata and may encode each ASCII character as a six-byte Unicode escape.
const MAX_EXPORT_REQUEST_BYTES: usize = 6 * 65_536 + 4_096;
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
        if !path.exists() && matches!(request, Request::RetainedBytes) {
            return Ok(format!("[0,{}]", core::DEFAULT_RETENTION_LIMIT_BYTES));
        }
        let conn = ai_hist::open_db(&path).map_err(|e| crate::database_error(&path, e))?;
        let value = (|| -> anyhow::Result<serde_json::Value> {
            Ok(match request {
                Request::CreateExport {
                    selection,
                    limits,
                    ttl_ms,
                    now_ms,
                } => serde_json::to_value(core::create_export(
                    &conn, &selection, &limits, ttl_ms, now_ms,
                )?)?,
                Request::ExportPage { cursor, now_ms } => {
                    serde_json::to_value(core::export_page(&conn, &cursor, now_ms)?)?
                }
                Request::CloseExport { snapshot_id } => {
                    core::close_export(&conn, &snapshot_id)?;
                    serde_json::Value::Null
                }
                Request::ExpireExports { now_ms, limit } => {
                    serde_json::to_value(core::expire_exports(&conn, now_ms, limit)?)?
                }
                Request::RetainedBytes => serde_json::to_value(core::retained_bytes(&conn)?)?,
                Request::SetRetentionLimit { max_bytes } => {
                    core::set_retention_limit(&conn, max_bytes)?;
                    serde_json::Value::Null
                }
                Request::CompactJournal { limit } => {
                    serde_json::to_value(core::compact_journal(&conn, limit)?)?
                }
            })
        })()
        .map_err(|e| {
            crate::native_error(
                if core::is_retention_limit(&e) {
                    "EXPORT_RETENTION_LIMIT"
                } else {
                    "HISTORY_EXPORT_FAILED"
                },
                e,
            )
        })?;
        serde_json::to_string(&value).map_err(|e| crate::native_error("HISTORY_EXPORT_FAILED", e))
    })
    .await
    .map_err(crate::worker_error)?
}
/// Compatibility error only: this call cannot create a store or run an upload.
#[napi]
pub async fn history_delivery(
    _request_json: String,
    _db_path: Option<String>,
) -> napi::Result<String> {
    Err(moved())
}
/// Compatibility error only. The local addon no longer accepts receivers.
#[napi]
pub async fn history_delivery_drain(
    _options_json: String,
    _db_path: Option<String>,
) -> napi::Result<String> {
    Err(moved())
}
