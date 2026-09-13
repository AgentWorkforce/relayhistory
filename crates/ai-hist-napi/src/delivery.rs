//! Typed service-independent delivery/export boundary. No SQL or transport.
use ai_hist_core::{delivery as core, open_db};
use napi_derive::napi;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Request {
    CreateJob {
        config: core::DeliveryJobConfig,
        now_ms: i64,
    },
    Status {
        job_id: String,
    },
    ListJobs,
    PrepareBatch {
        job_id: String,
        now_ms: i64,
    },
    ClaimBatch {
        job_id: String,
        worker_id: String,
        lease_ms: i64,
        now_ms: i64,
    },
    RenewLease {
        lease: core::DeliveryLease,
        lease_ms: i64,
        now_ms: i64,
    },
    StorePreparedPayload {
        lease: core::DeliveryLease,
        mapping_version: String,
        content_type: String,
        body: String,
        now_ms: i64,
    },
    ValidateDispatch {
        lease: core::DeliveryLease,
        now_ms: i64,
    },
    Acknowledge {
        lease: core::DeliveryLease,
        acknowledgment: core::DeliveryAcknowledgment,
        now_ms: i64,
    },
    RecordFailure {
        lease: core::DeliveryLease,
        failure: core::DeliveryFailure,
        retry_after_ms: Option<i64>,
        now_ms: i64,
    },
    PauseJob {
        job_id: String,
    },
    ResumeJob {
        job_id: String,
    },
    RetryJob {
        job_id: String,
    },
    CancelJob {
        job_id: String,
    },
    SetSessionExcluded {
        session: core::SessionIdentity,
        excluded: bool,
    },
    SetRetentionLimit {
        max_bytes: i64,
    },
    RetainedBytes,
    CompactJournal {
        limit: usize,
    },
    CompactReceipts {
        limit: usize,
    },
    CreateExport {
        selection: core::ExportSelection,
        limits: core::DeliveryLimits,
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

#[napi]
pub async fn history_delivery(
    request_json: String,
    db_path: Option<String>,
) -> napi::Result<String> {
    // Native JSON is bounded even when called without the SDK. Payload bytes
    // have stricter per-job limits enforced by core before persistence.
    // JSON escapes a decoded byte as up to six ASCII bytes. Allow the core
    // 32 MiB prepared-body maximum plus envelope metadata, then let typed core
    // limits validate the decoded body.
    if request_json.len() > 200 * 1_048_576 {
        return Err(crate::native_error(
            "INVALID_ARGUMENT",
            "delivery request too large",
        ));
    }
    let request: Request = serde_json::from_str(&request_json)
        .map_err(|_| crate::native_error("INVALID_ARGUMENT", "invalid typed delivery request"))?;
    let path = crate::db_path(db_path);
    napi::tokio::task::spawn_blocking(move || {
        if !path.exists() && matches!(request, Request::ListJobs) {
            return Ok("[]".to_owned());
        }
        let conn = open_db(&path).map_err(|error| crate::database_error(&path, error))?;
        let result: anyhow::Result<serde_json::Value> = (|| {
            Ok(match request {
                Request::CreateJob { config, now_ms } => {
                    serde_json::to_value(core::create_job(&conn, &config, now_ms)?)?
                }
                Request::Status { job_id } => serde_json::to_value(core::status(&conn, &job_id)?)?,
                Request::ListJobs => serde_json::to_value(core::list_jobs(&conn)?)?,
                Request::PrepareBatch { job_id, now_ms } => {
                    serde_json::to_value(core::prepare_batch(&conn, &job_id, now_ms)?)?
                }
                Request::ClaimBatch {
                    job_id,
                    worker_id,
                    lease_ms,
                    now_ms,
                } => serde_json::to_value(core::claim_batch(
                    &conn, &job_id, &worker_id, lease_ms, now_ms,
                )?)?,
                Request::RenewLease {
                    lease,
                    lease_ms,
                    now_ms,
                } => serde_json::to_value(core::renew_lease(&conn, &lease, lease_ms, now_ms)?)?,
                Request::StorePreparedPayload {
                    lease,
                    mapping_version,
                    content_type,
                    body,
                    now_ms,
                } => serde_json::to_value(core::store_prepared_payload(
                    &conn,
                    &lease,
                    &mapping_version,
                    &content_type,
                    &body,
                    now_ms,
                )?)?,
                Request::ValidateDispatch { lease, now_ms } => {
                    serde_json::to_value(core::validate_dispatch(&conn, &lease, now_ms)?)?
                }
                Request::Acknowledge {
                    lease,
                    acknowledgment,
                    now_ms,
                } => serde_json::to_value(core::acknowledge(
                    &conn,
                    &lease,
                    &acknowledgment,
                    now_ms,
                )?)?,
                Request::RecordFailure {
                    lease,
                    failure,
                    retry_after_ms,
                    now_ms,
                } => serde_json::to_value(core::record_failure(
                    &conn,
                    &lease,
                    failure,
                    retry_after_ms,
                    now_ms,
                )?)?,
                Request::PauseJob { job_id } => {
                    serde_json::to_value(core::pause_job(&conn, &job_id)?)?
                }
                Request::ResumeJob { job_id } => {
                    serde_json::to_value(core::resume_job(&conn, &job_id)?)?
                }
                Request::RetryJob { job_id } => {
                    serde_json::to_value(core::retry_job(&conn, &job_id)?)?
                }
                Request::CancelJob { job_id } => {
                    serde_json::to_value(core::cancel_job(&conn, &job_id)?)?
                }
                Request::SetSessionExcluded { session, excluded } => {
                    core::set_session_excluded(&conn, &session, excluded)?;
                    serde_json::Value::Null
                }
                Request::SetRetentionLimit { max_bytes } => {
                    core::set_retention_limit(&conn, max_bytes)?;
                    serde_json::Value::Null
                }
                Request::RetainedBytes => serde_json::to_value(core::retained_bytes(&conn)?)?,
                Request::CompactJournal { limit } => {
                    serde_json::to_value(core::compact_journal(&conn, limit)?)?
                }
                Request::CompactReceipts { limit } => {
                    serde_json::to_value(core::compact_receipts(&conn, limit)?)?
                }
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
            })
        })();
        let value = result.map_err(|error| {
            let code = if core::is_retention_limit(&error) {
                "DELIVERY_RETENTION_LIMIT"
            } else {
                "HISTORY_DELIVERY_FAILED"
            };
            crate::native_error(code, error)
        })?;
        serde_json::to_string(&value)
            .map_err(|error| crate::native_error("HISTORY_DELIVERY_FAILED", error))
    })
    .await
    .map_err(crate::worker_error)?
}
