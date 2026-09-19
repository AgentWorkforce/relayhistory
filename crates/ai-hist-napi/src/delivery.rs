//! Typed service-independent delivery/export boundary. No SQL or transport.
use ai_hist::delivery::worker::{
    self, DrainOptions, PreparedBody, Receiver, ReceiverContext, ReceiverFailure, Receivers,
};
use ai_hist::{delivery as core, open_db};
use napi::bindgen_prelude::Promise;
use napi::threadsafe_function::{ErrorStrategy, ThreadsafeFunction};
use napi::tokio::runtime::Handle;
use napi_derive::napi;
use serde::Deserialize;
use std::sync::Mutex;
use std::time::{Duration, Instant};

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
        if !path.exists() {
            match request {
                Request::ListJobs => return Ok("[]".to_owned()),
                Request::RetainedBytes => {
                    return Ok(format!("[0,{}]", core::DEFAULT_RETENTION_LIMIT_BYTES))
                }
                Request::Status { .. } => {
                    return Err(crate::native_error(
                        "HISTORY_DELIVERY_FAILED",
                        "unknown delivery job",
                    ))
                }
                _ => {}
            }
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
            } else if error
                .to_string()
                .starts_with("DELIVERY_GENERATION_REQUIRED:")
            {
                "DELIVERY_GENERATION_REQUIRED"
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

// ---------------------------------------------------------------------------
// Generic drain adapter: JavaScript destinations as core delivery receivers.
//
// The bounded drain loop itself lives once, in `ai_hist::delivery::worker`.
// This adapter only bridges that worker's `Receiver` trait to a pair of
// JavaScript callbacks, so a Node host never reimplements leases, retries,
// acknowledgment checking or round-robin scheduling.
// ---------------------------------------------------------------------------

/// A registered JavaScript destination, described once per drain.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DestinationDescriptor {
    destination_id: String,
    instance_id: String,
    mapping_version: String,
    supported_kinds: Vec<String>,
    supports_tombstones: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DrainRequest {
    #[serde(default)]
    job_ids: Option<Vec<String>>,
    worker_id: String,
    #[serde(default)]
    max_batches: Option<usize>,
    #[serde(default)]
    max_prepare_steps: Option<usize>,
    #[serde(default)]
    lease_ms: Option<i64>,
    #[serde(default)]
    request_timeout_ms: Option<i64>,
    destinations: Vec<DestinationDescriptor>,
}

/// The adapter's reply envelope. A destination callback never rejects: it
/// classifies its own failure so arbitrary plugin text is never persisted.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Reply {
    ok: bool,
    #[serde(default)]
    value: Option<serde_json::Value>,
    #[serde(default)]
    failure: Option<core::DeliveryFailure>,
    #[serde(default)]
    retry_after_ms: Option<i64>,
}

#[derive(Deserialize)]
struct PreparedReply {
    content_type: String,
    body: String,
}

fn transient() -> ReceiverFailure {
    ReceiverFailure::from(core::DeliveryFailure::Transient)
}

#[derive(Default)]
struct StopPoll {
    stopped: bool,
    checked: Option<Instant>,
}

/// Owns the JavaScript callbacks and the runtime handle the blocking drain
/// thread uses to await them. Never touches the delivery database.
struct Bridge {
    handle: Handle,
    prepare: ThreadsafeFunction<String, ErrorStrategy::Fatal>,
    send: ThreadsafeFunction<String, ErrorStrategy::Fatal>,
    cancelled: ThreadsafeFunction<(), ErrorStrategy::Fatal>,
    stop: Mutex<StopPoll>,
}

impl Bridge {
    fn call<T: serde::de::DeserializeOwned>(
        &self,
        function: &ThreadsafeFunction<String, ErrorStrategy::Fatal>,
        request: serde_json::Value,
        timeout_ms: i64,
    ) -> Result<T, ReceiverFailure> {
        let payload = serde_json::to_string(&request).map_err(|_| transient())?;
        // The JavaScript adapter owns the receiver deadline, because only it can
        // abort the AbortSignal a destination observes. This budget is a
        // backstop for a callback that never settles at all, so it allows the
        // adapter a moment to report its own classified timeout first.
        let budget =
            Duration::from_millis(timeout_ms.clamp(1, 3_600_000).saturating_add(1_000) as u64);
        let replied = self.handle.block_on(async {
            napi::tokio::time::timeout(budget, async {
                function.call_async::<Promise<String>>(payload).await?.await
            })
            .await
        });
        // A timeout, a rejected promise or an unreadable envelope is transport
        // noise: it stays retryable and is never reported as the batch's fault.
        let Ok(Ok(replied)) = replied else {
            return Err(transient());
        };
        let reply: Reply = serde_json::from_str(&replied).map_err(|_| transient())?;
        if !reply.ok {
            return Err(ReceiverFailure {
                failure: reply.failure.unwrap_or(core::DeliveryFailure::Transient),
                retry_after_ms: reply.retry_after_ms,
            });
        }
        // A readable reply carrying an unreadable destination result is the
        // destination's own contract violation, not a retryable transport fault.
        serde_json::from_value(reply.value.unwrap_or(serde_json::Value::Null))
            .map_err(|_| ReceiverFailure::from(core::DeliveryFailure::InvalidPayload))
    }

    /// Host stop requests are sticky and polled at most every 25ms: each poll is
    /// a full event-loop round trip, and the worker asks between steps.
    fn stopped(&self) -> bool {
        let mut poll = self.stop.lock().unwrap_or_else(|error| error.into_inner());
        if poll.stopped {
            return true;
        }
        if poll
            .checked
            .is_some_and(|at| at.elapsed() < Duration::from_millis(25))
        {
            return false;
        }
        poll.checked = Some(Instant::now());
        let answer = self.handle.block_on(async {
            napi::tokio::time::timeout(Duration::from_secs(5), async {
                self.cancelled.call_async::<Promise<bool>>(()).await?.await
            })
            .await
        });
        // An unanswerable host keeps the drain running: only an explicit `true`
        // discards in-flight work, and a lost lease is detected independently.
        poll.stopped = matches!(answer, Ok(Ok(true)));
        poll.stopped
    }
}

struct JsReceiver<'a> {
    destination_id: &'a str,
    instance_id: &'a str,
    mapping_version: &'a str,
    supported_kinds: Vec<&'a str>,
    supports_tombstones: bool,
    bridge: &'a Bridge,
}

impl Receiver for JsReceiver<'_> {
    fn mapping_version(&self) -> &str {
        self.mapping_version
    }
    fn supported_kinds(&self) -> &[&str] {
        &self.supported_kinds
    }
    fn supports_tombstones(&self) -> bool {
        self.supports_tombstones
    }
    fn prepare(
        &self,
        batch: &core::HistoryExportBatch,
        ctx: &ReceiverContext<'_>,
    ) -> Result<PreparedBody, ReceiverFailure> {
        let request = serde_json::json!({
            "destinationId": self.destination_id,
            "instanceId": self.instance_id,
            "batch": batch,
        });
        let reply: PreparedReply =
            self.bridge
                .call(&self.bridge.prepare, request, ctx.timeout_ms)?;
        Ok(PreparedBody {
            content_type: reply.content_type,
            body: reply.body,
        })
    }
    fn send(
        &self,
        payload: &core::PreparedPayload,
        batch: &core::HistoryExportBatch,
        ctx: &ReceiverContext<'_>,
    ) -> Result<core::DeliveryAcknowledgment, ReceiverFailure> {
        let request = serde_json::json!({
            "destinationId": self.destination_id,
            "instanceId": self.instance_id,
            "payload": payload,
            "batch": batch,
            "idempotencyKey": ctx.idempotency_key,
        });
        self.bridge.call(&self.bridge.send, request, ctx.timeout_ms)
    }
}

struct JsReceivers<'a>(Vec<JsReceiver<'a>>);
impl Receivers for JsReceivers<'_> {
    fn receiver(&self, destination_id: &str, instance_id: &str) -> Option<&dyn Receiver> {
        self.0
            .iter()
            .find(|entry| {
                entry.destination_id == destination_id && entry.instance_id == instance_id
            })
            .map(|entry| entry as &dyn Receiver)
    }
}

/// One bounded drain of every selected delivery job, run by the single generic
/// Rust worker. `prepare` and `send` are the registered JavaScript destinations;
/// `cancelled` reports a host stop request. None of them ever rejects.
#[napi]
pub async fn history_delivery_drain(
    options_json: String,
    db_path: Option<String>,
    prepare: ThreadsafeFunction<String, ErrorStrategy::Fatal>,
    send: ThreadsafeFunction<String, ErrorStrategy::Fatal>,
    cancelled: ThreadsafeFunction<(), ErrorStrategy::Fatal>,
) -> napi::Result<String> {
    let request: DrainRequest = serde_json::from_str(&options_json)
        .map_err(|_| crate::native_error("INVALID_ARGUMENT", "invalid delivery drain options"))?;
    let path = crate::db_path(db_path);
    let handle = Handle::current();
    napi::tokio::task::spawn_blocking(move || {
        let DrainRequest {
            job_ids,
            worker_id,
            max_batches,
            max_prepare_steps,
            lease_ms,
            request_timeout_ms,
            destinations,
        } = request;
        let mut options = DrainOptions::new(worker_id);
        options.job_ids = job_ids;
        if let Some(value) = max_batches {
            options.max_batches = value;
        }
        if let Some(value) = max_prepare_steps {
            options.max_prepare_steps = value;
        }
        if let Some(value) = lease_ms {
            options.lease_ms = value;
        }
        if let Some(value) = request_timeout_ms {
            options.request_timeout_ms = value;
        }
        let bridge = Bridge {
            handle,
            prepare,
            send,
            cancelled,
            stop: Mutex::default(),
        };
        let receivers = JsReceivers(
            destinations
                .iter()
                .map(|entry| JsReceiver {
                    destination_id: &entry.destination_id,
                    instance_id: &entry.instance_id,
                    mapping_version: &entry.mapping_version,
                    supported_kinds: entry.supported_kinds.iter().map(String::as_str).collect(),
                    supports_tombstones: entry.supports_tombstones,
                    bridge: &bridge,
                })
                .collect(),
        );
        let result = worker::drain(
            &path,
            &receivers,
            &options,
            &|| worker::system_clock(),
            &|| bridge.stopped(),
        )
        .map_err(|error| {
            let message = error.to_string();
            if core::is_retention_limit(&error) {
                crate::native_error("DELIVERY_RETENTION_LIMIT", message)
            } else if let Some(detail) = message.strip_prefix("INVALID_ARGUMENT:") {
                crate::native_error("INVALID_ARGUMENT", detail.trim())
            } else {
                crate::native_error("HISTORY_DELIVERY_FAILED", message)
            }
        })?;
        serde_json::to_string(&result)
            .map_err(|error| crate::native_error("HISTORY_DELIVERY_FAILED", error))
    })
    .await
    .map_err(crate::worker_error)?
}
