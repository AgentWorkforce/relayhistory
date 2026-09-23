//! Probe-owned helper RPC; leases use the same clock rules as the old addon.
use crate::delivery as core;
use anyhow::{Context, Result};
use serde::Deserialize;
use std::time::Instant;
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
    CompactJournalPass {
        page_size: usize,
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

pub fn request(path: &std::path::Path, value: serde_json::Value) -> Result<serde_json::Value> {
    let request: Request =
        serde_json::from_value(value).context("INVALID_ARGUMENT: invalid delivery request")?;
    let received = Instant::now();
    let conn = core::open_db(path)?;
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
                &conn,
                &job_id,
                &worker_id,
                lease_ms,
                &request_clock(now_ms, received)?,
            )?)?,
            Request::RenewLease {
                lease,
                lease_ms,
                now_ms,
            } => serde_json::to_value(core::renew_lease(
                &conn,
                &lease,
                lease_ms,
                &request_clock(now_ms, received)?,
            )?)?,
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
                &request_clock(now_ms, received)?,
            )?)?,
            Request::ValidateDispatch { lease, now_ms } => serde_json::to_value(
                core::validate_dispatch(&conn, &lease, &request_clock(now_ms, received)?)?,
            )?,
            Request::Acknowledge {
                lease,
                acknowledgment,
                now_ms,
            } => serde_json::to_value(core::acknowledge(
                &conn,
                &lease,
                &acknowledgment,
                &request_clock(now_ms, received)?,
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
                &request_clock(now_ms, received)?,
            )?)?,
            Request::PauseJob { job_id } => serde_json::to_value(core::pause_job(&conn, &job_id)?)?,
            Request::ResumeJob { job_id } => {
                serde_json::to_value(core::resume_job(&conn, &job_id)?)?
            }
            Request::RetryJob { job_id } => serde_json::to_value(core::retry_job(&conn, &job_id)?)?,
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
            Request::CompactJournalPass { page_size } => {
                serde_json::to_value(core::compact_journal_pass(&conn, page_size)?)?
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
    result
}
fn request_clock(now_ms: i64, received: Instant) -> anyhow::Result<impl Fn() -> i64> {
    anyhow::ensure!(now_ms >= 0, "invalid delivery clock");
    Ok(move || {
        let waited = i64::try_from(received.elapsed().as_millis()).unwrap_or(i64::MAX);
        now_ms.saturating_add(waited)
    })
}

#[cfg(test)]
mod tests {
    use super::{request, request_clock};
    use serde_json::json;
    use std::thread::sleep;
    use std::time::{Duration, Instant};

    /// A host reclaims every consumed journal row through one request, in
    /// transactions no larger than the page it names.
    #[test]
    fn a_compaction_pass_is_one_bounded_request() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("delivery.db");
        let conn = crate::delivery::open_db(&path).unwrap();
        conn.execute(
            "INSERT INTO history_subscriptions(id,journal_cursor) VALUES ('reader',0)",
            [],
        )
        .unwrap();
        for n in 0..30 {
            conn.execute("INSERT INTO session_events(source,session_id,event_uid,ts_ms,role,kind,text) VALUES ('claude','one',?,1,'user','text','consumed')", [n.to_string()]).unwrap();
        }
        conn.execute(
            "UPDATE history_subscriptions SET journal_cursor=(SELECT MAX(seq) FROM delivery_journal)",
            [],
        )
        .unwrap();
        let pass = |page_size: u64| {
            request(
                &path,
                json!({"operation":"compact_journal_pass","page_size":page_size}),
            )
        };
        assert!(pass(0).is_err());
        assert!(pass(crate::delivery::MAX_COMPACTION_PAGE as u64 + 1).is_err());
        assert_eq!(pass(7).unwrap(), json!(30));
        assert_eq!(pass(7).unwrap(), json!(0));
        let remaining: i64 = conn
            .query_row("SELECT COUNT(*) FROM delivery_journal", [], |r| r.get(0))
            .unwrap();
        assert_eq!(remaining, 0);
    }

    #[test]
    fn a_renewal_dated_after_a_wait_is_not_dated_from_before_it() {
        let received = Instant::now();
        let clock = request_clock(1_000, received).unwrap();
        // Read immediately: the caller's timestamp is the base, not replaced.
        assert!((1_000..1_000 + 5_000).contains(&clock()));
        sleep(Duration::from_millis(120));
        // Read after waiting: the time waited is on the clock. Passing the
        // request's `now_ms` straight through returned exactly 1_000 here.
        assert!(clock() >= 1_120, "clock did not advance past the wait");
    }

    #[test]
    fn a_negative_request_clock_is_refused_before_the_wait_can_advance_it() {
        // Read after a wait: had the check run on the advanced value, a small
        // negative timestamp would have crossed zero and been accepted.
        let received = Instant::now();
        sleep(Duration::from_millis(5));
        assert!(request_clock(-1, received).is_err());
        assert!(request_clock(0, received).is_ok());
    }

    #[test]
    fn a_renewal_clock_cannot_overflow() {
        let clock = request_clock(i64::MAX, Instant::now()).unwrap();
        assert_eq!(clock(), i64::MAX);
    }
}

/// One bounded probe-owned drain; receiver credentials and guards stay in Rust.
#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DrainRequest {
    pub job_ids: Option<Vec<String>>,
    pub worker_id: Option<String>,
    pub max_batches: Option<usize>,
    pub max_prepare_steps: Option<usize>,
    pub lease_ms: Option<i64>,
    pub request_timeout_ms: Option<i64>,
}
impl DrainRequest {
    pub fn options(self) -> core::worker::DrainOptions {
        let mut options = core::worker::DrainOptions::new(
            self.worker_id
                .unwrap_or_else(|| format!("probe-helper-{}", std::process::id())),
        );
        options.job_ids = self.job_ids;
        if let Some(value) = self.max_batches {
            options.max_batches = value;
        }
        if let Some(value) = self.max_prepare_steps {
            options.max_prepare_steps = value;
        }
        if let Some(value) = self.lease_ms {
            options.lease_ms = value;
        }
        if let Some(value) = self.request_timeout_ms {
            options.request_timeout_ms = value;
        }
        options
    }
}
