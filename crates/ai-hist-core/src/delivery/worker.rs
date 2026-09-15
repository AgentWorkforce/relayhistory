//! The single generic delivery worker: one bounded drain loop, implemented once.
//!
//! Every host (Node through NAPI, the CLI, a plugin, the probe) supplies only a
//! [`Receiver`] — a destination mapping and transport — and this module owns the
//! durable protocol: round-robin over jobs, one claimed batch at a time, payload
//! persistence before transport, an eligibility recheck immediately before the
//! send, lease keepalive while a receiver runs, and acknowledgment or a
//! classified failure afterwards. No network, credential or environment access
//! happens here; receivers never see this module's database connections.
//!
//! A drain is bounded: it attempts at most `max_batches` batches, never waits
//! for a retry deadline and never enables, pauses or resumes a job. Delivery
//! stays at least once, so receivers must remain idempotent per revision.
//!
//! The core cannot forcibly interrupt a receiver: Rust has no way to cancel a
//! synchronous call. [`ReceiverContext::timeout_ms`] is the deadline a receiver
//! is required to honour itself (socket/read timeouts), and
//! [`ReceiverContext::cancelled`] reports a host stop request or a lost lease so
//! a long-running receiver can abandon its work early. A receiver that ignores
//! both merely delays this drain; its result is still discarded when the lease
//! was lost, and nothing is acknowledged.

use super::{
    acknowledge, claim_batch, compact_journal, compact_receipts, expire_exports,
    is_retention_limit, list_jobs, prepare_batch, record_failure, renew_lease, retained_bytes,
    status, store_prepared_payload, validate_dispatch, ClaimedBatch, DeliveryAcknowledgment,
    DeliveryFailure, DeliveryJobConfig, DeliveryStatus, HistoryExportBatch, PreparedPayload,
};
use anyhow::{anyhow, bail, Result};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Receiver-classified failure. `retry_after_ms` is an absolute epoch-ms retry
/// time, exactly as `record_failure` interprets its own `retry_after_ms`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiverFailure {
    pub failure: DeliveryFailure,
    pub retry_after_ms: Option<i64>,
}
impl From<DeliveryFailure> for ReceiverFailure {
    fn from(failure: DeliveryFailure) -> Self {
        Self {
            failure,
            retry_after_ms: None,
        }
    }
}

/// The destination body a receiver maps a batch into, without credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedBody {
    pub content_type: String,
    pub body: String,
}

/// Passed to receiver calls. `cancelled()` becomes true when the host asked the
/// drain to stop or the lease was lost; `timeout_ms` is the per-call deadline
/// the receiver is expected to honour itself.
pub struct ReceiverContext<'a> {
    pub cancelled: &'a dyn Fn() -> bool,
    pub timeout_ms: i64,
    /// The claimed batch id: stable across redelivery of the same bytes.
    pub idempotency_key: &'a str,
}

/// One destination implementation. Trusted application code, not a sandbox.
pub trait Receiver {
    fn mapping_version(&self) -> &str;
    fn supported_kinds(&self) -> &[&str];
    fn supports_tombstones(&self) -> bool;
    /// Pure payload mapping; never include credentials in the body.
    fn prepare(
        &self,
        batch: &HistoryExportBatch,
        ctx: &ReceiverContext<'_>,
    ) -> Result<PreparedBody, ReceiverFailure>;
    /// Send exactly the stored bytes. Must verify the remote account matches
    /// `batch.account_id`. Never launch an interactive login: report
    /// `AuthenticationRequired` instead.
    fn send(
        &self,
        payload: &PreparedPayload,
        batch: &HistoryExportBatch,
        ctx: &ReceiverContext<'_>,
    ) -> Result<DeliveryAcknowledgment, ReceiverFailure>;
}

/// Lookup of receivers by the job configuration's destination and instance ids.
pub trait Receivers {
    fn receiver(&self, destination_id: &str, instance_id: &str) -> Option<&dyn Receiver>;
}

impl Receivers for HashMap<(String, String), Box<dyn Receiver>> {
    fn receiver(&self, destination_id: &str, instance_id: &str) -> Option<&dyn Receiver> {
        self.get(&(destination_id.to_owned(), instance_id.to_owned()))
            .map(|receiver| receiver.as_ref())
    }
}

/// A single registered receiver, for hosts that deliver to one destination.
pub struct SingleReceiver<'a> {
    pub destination_id: String,
    pub instance_id: String,
    pub receiver: &'a dyn Receiver,
}
impl<'a> SingleReceiver<'a> {
    pub fn new(
        destination_id: impl Into<String>,
        instance_id: impl Into<String>,
        receiver: &'a dyn Receiver,
    ) -> Self {
        Self {
            destination_id: destination_id.into(),
            instance_id: instance_id.into(),
            receiver,
        }
    }
}
impl Receivers for SingleReceiver<'_> {
    fn receiver(&self, destination_id: &str, instance_id: &str) -> Option<&dyn Receiver> {
        (self.destination_id == destination_id && self.instance_id == instance_id)
            .then_some(self.receiver)
    }
}

#[derive(Debug, Clone)]
pub struct DrainOptions {
    /// `None` drains every job; an unknown id is an invalid argument.
    pub job_ids: Option<Vec<String>>,
    pub worker_id: String,
    pub max_batches: usize,
    pub max_prepare_steps: usize,
    pub lease_ms: i64,
    pub request_timeout_ms: i64,
}
impl DrainOptions {
    pub fn new(worker_id: impl Into<String>) -> Self {
        Self {
            job_ids: None,
            worker_id: worker_id.into(),
            max_batches: 100,
            max_prepare_steps: 100,
            lease_ms: 30_000,
            request_timeout_ms: 30_000,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DrainIssueCode {
    DestinationNotRegistered,
    DeliveryStateFailed,
    DeliveryRetentionLimit,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DrainIssue {
    pub job_id: String,
    pub code: DrainIssueCode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DrainRetention {
    pub used_bytes: i64,
    pub limit_bytes: i64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DrainResult {
    pub attempts: usize,
    pub statuses: Vec<DeliveryStatus>,
    pub issues: Vec<DrainIssue>,
    pub retention: DrainRetention,
}

pub fn system_clock() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| i64::try_from(value.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn range(value: i64, name: &str, minimum: i64, maximum: i64) -> Result<()> {
    if value < minimum || value > maximum {
        bail!("INVALID_ARGUMENT: {name} must be between {minimum} and {maximum}");
    }
    Ok(())
}
fn compact(conn: &Connection, now_ms: i64) -> Result<()> {
    expire_exports(conn, now_ms, 32)?;
    compact_journal(conn, 1_000)?;
    compact_receipts(conn, 1_000)?;
    Ok(())
}
fn chosen(selection: Option<&[String]>, job_id: &str) -> bool {
    match selection {
        None => true,
        Some(ids) => ids.iter().any(|id| id == job_id),
    }
}

/// One bounded drain. It never waits for a retry deadline or enables a job.
///
/// The database path, rather than a connection, is required because the lease
/// keepalive renews from its own connection on its own thread while a receiver
/// holds the calling thread. `clock` returns epoch milliseconds; pass
/// [`system_clock`] unless a test needs a deterministic clock. `cancelled`
/// reports a host stop request and is polled between and during attempts.
pub fn drain(
    db_path: &Path,
    receivers: &dyn Receivers,
    options: &DrainOptions,
    clock: &(dyn Fn() -> i64 + Sync),
    cancelled: &(dyn Fn() -> bool + Sync),
) -> Result<DrainResult> {
    range(options.max_batches as i64, "max_batches", 1, 10_000)?;
    range(
        options.max_prepare_steps as i64,
        "max_prepare_steps",
        1,
        10_000,
    )?;
    range(options.lease_ms, "lease_ms", 30, 86_400_000)?;
    range(
        options.request_timeout_ms,
        "request_timeout_ms",
        1,
        3_600_000,
    )?;
    let mut worker = Worker {
        conn: crate::open_db(db_path)?,
        keepalive: Mutex::new(crate::open_db(db_path)?),
        receivers,
        options,
        clock,
        cancelled,
        attempts: 0,
        prepare_steps: 0,
        issues: Vec::new(),
    };
    compact(&worker.conn, clock())?;
    let listed = list_jobs(&worker.conn)?;
    let selection = options.job_ids.as_deref();
    if let Some(ids) = selection {
        if ids
            .iter()
            .any(|id| !listed.iter().any(|job| &job.job_id == id))
        {
            bail!("INVALID_ARGUMENT: unknown delivery job selection");
        }
    }
    let jobs: Vec<String> = listed
        .into_iter()
        .filter(|job| chosen(selection, &job.job_id))
        .map(|job| job.job_id)
        .collect();
    // Round-robin jobs: one failed destination never consumes another's cursor.
    let mut progressed = true;
    while progressed && worker.budget() && !cancelled() {
        progressed = false;
        for job_id in &jobs {
            if !worker.budget() || cancelled() {
                break;
            }
            match worker.step(job_id) {
                Ok(Step::Progressed) => progressed = true,
                Ok(Step::Idle) => {}
                Ok(Step::Unregistered) => {
                    worker.issue(job_id, DrainIssueCode::DestinationNotRegistered, None)
                }
                // A receiver verdict never reaches here: it is classified and
                // recorded under the lease. Only local state errors escape.
                Err(error) => {
                    let retention = is_retention_limit(&error);
                    let code = if retention {
                        DrainIssueCode::DeliveryRetentionLimit
                    } else {
                        DrainIssueCode::DeliveryStateFailed
                    };
                    let detail = if retention {
                        error.to_string()
                    } else {
                        "delivery state operation failed".into()
                    };
                    worker.issue(job_id, code, Some(detail));
                }
            }
        }
    }
    let statuses = list_jobs(&worker.conn)?
        .into_iter()
        .filter(|job| chosen(selection, &job.job_id))
        .collect();
    compact(&worker.conn, clock())?;
    let (used_bytes, limit_bytes) = retained_bytes(&worker.conn)?;
    Ok(DrainResult {
        attempts: worker.attempts,
        statuses,
        issues: worker.issues,
        retention: DrainRetention {
            used_bytes,
            limit_bytes,
        },
    })
}

enum Step {
    Idle,
    Progressed,
    Unregistered,
}

struct Worker<'a> {
    conn: Connection,
    keepalive: Mutex<Connection>,
    receivers: &'a dyn Receivers,
    options: &'a DrainOptions,
    clock: &'a (dyn Fn() -> i64 + Sync),
    cancelled: &'a (dyn Fn() -> bool + Sync),
    attempts: usize,
    prepare_steps: usize,
    issues: Vec<DrainIssue>,
}

impl Worker<'_> {
    fn budget(&self) -> bool {
        self.attempts < self.options.max_batches
            && self.prepare_steps < self.options.max_prepare_steps
    }
    /// At most one issue per job, whatever its cause, exactly as the host SDK.
    fn issue(&mut self, job_id: &str, code: DrainIssueCode, detail: Option<String>) {
        if !self.issues.iter().any(|issue| issue.job_id == job_id) {
            self.issues.push(DrainIssue {
                job_id: job_id.to_owned(),
                code,
                detail,
            });
        }
    }

    /// One job iteration inside the drain's per-job error boundary.
    fn step(&mut self, job_id: &str) -> Result<Step> {
        let job = status(&self.conn, job_id)?;
        if job.state != "active" || job.next_attempt_ms > (self.clock)() {
            return Ok(Step::Idle);
        }
        let Some(receiver) = self
            .receivers
            .receiver(&job.config.destination_id, &job.config.instance_id)
        else {
            return Ok(Step::Unregistered);
        };
        let prepared = prepare_batch(&self.conn, job_id, (self.clock)())?;
        self.prepare_steps += 1;
        if prepared.batch_id.is_none() {
            // Scanning only excluded or unselected rows still moves a cursor.
            return Ok(
                if !prepared.bootstrap_complete || prepared.scanned_records > 0 {
                    Step::Progressed
                } else {
                    Step::Idle
                },
            );
        }
        let Some(claim) = claim_batch(
            &self.conn,
            job_id,
            &self.options.worker_id,
            self.options.lease_ms,
            (self.clock)(),
        )?
        else {
            return Ok(Step::Idle);
        };
        self.attempts += 1;
        self.attempt(receiver, &claim, &job.config)?;
        Ok(Step::Progressed)
    }

    /// One attempt under the claimed lease. Returns `Ok` once the outcome is
    /// durably recorded, whether that is an acknowledgment or a failure; only
    /// a fenced or failing state write escapes to the job error boundary.
    fn attempt(
        &self,
        receiver: &dyn Receiver,
        claim: &ClaimedBatch,
        config: &DeliveryJobConfig,
    ) -> Result<()> {
        let lost = AtomicBool::new(false);
        let stop = AtomicBool::new(false);
        let host = self.cancelled;
        let stopping = || host() || lost.load(Ordering::SeqCst);
        let outcome = {
            // Bind every value the keepalive thread touches: the worker itself
            // owns a connection and is deliberately not shareable.
            let keepalive = &self.keepalive;
            let clock = self.clock;
            let lease = &claim.lease;
            let lease_ms = self.options.lease_ms;
            let interval = Duration::from_millis(1.max(lease_ms / 3) as u64);
            std::thread::scope(|scope| {
                let renewals = scope.spawn(|| {
                    while sleep_until(&stop, interval) {
                        let renewed = keepalive
                            .lock()
                            .map_err(|_| anyhow!("delivery keepalive connection is poisoned"))
                            .and_then(|conn| renew_lease(&conn, lease, lease_ms, clock()));
                        if renewed.is_err() {
                            lost.store(true, Ordering::SeqCst);
                            break;
                        }
                    }
                });
                let result = self.dispatch(receiver, claim, config, &stopping);
                stop.store(true, Ordering::SeqCst);
                // A panicking keepalive thread must not take the drain with it.
                let _ = renewals.join();
                result
            })
        };
        let verdict = match outcome {
            // Never acknowledge work whose lease was lost or whose host asked
            // to stop: the outcome is uncertain and stays retryable.
            Ok(_) if stopping() => Err(DeliveryFailure::Transient.into()),
            Ok(ack) => acknowledge(&self.conn, &claim.lease, &ack, (self.clock)())
                .map(|_| ())
                .map_err(|_| ReceiverFailure::from(DeliveryFailure::Transient)),
            Err(failure) => Err(failure),
        };
        if let Err(failure) = verdict {
            // The core fences this write. If another worker owns the job, even
            // failure recording must fail rather than move that worker's state.
            record_failure(
                &self.conn,
                &claim.lease,
                failure.failure,
                failure.retry_after_ms,
                (self.clock)(),
            )?;
        }
        Ok(())
    }

    /// Mapping, payload persistence, the eligibility recheck and transport.
    /// Every local error here is a retryable transient: only the receiver
    /// classifies the batch itself.
    fn dispatch(
        &self,
        receiver: &dyn Receiver,
        claim: &ClaimedBatch,
        config: &DeliveryJobConfig,
        stopping: &dyn Fn() -> bool,
    ) -> Result<DeliveryAcknowledgment, ReceiverFailure> {
        let transient = || ReceiverFailure::from(DeliveryFailure::Transient);
        let context = ReceiverContext {
            cancelled: stopping,
            timeout_ms: self.options.request_timeout_ms,
            idempotency_key: &claim.batch.batch_id,
        };
        if stopping() {
            return Err(transient());
        }
        if receiver.mapping_version() != claim.batch.mapping_version {
            return Err(DeliveryFailure::MappingVersionMismatch.into());
        }
        let kinds = receiver.supported_kinds();
        if claim.batch.records.iter().any(|record| {
            !kinds.contains(&record.kind.as_str())
                || (record.operation == "delete" && !receiver.supports_tombstones())
        }) {
            return Err(DeliveryFailure::UnsupportedEvidence.into());
        }
        if claim.prepared.is_none() {
            let prepared = receiver.prepare(&claim.batch, &context)?;
            if stopping() {
                return Err(transient());
            }
            if prepared.content_type.is_empty()
                || prepared.content_type.len() > 200
                || prepared.content_type.contains(['\r', '\n'])
                || prepared.body.len() > config.limits.max_prepared_bytes
            {
                return Err(DeliveryFailure::InvalidPayload.into());
            }
            store_prepared_payload(
                &self.conn,
                &claim.lease,
                receiver.mapping_version(),
                &prepared.content_type,
                &prepared.body,
                (self.clock)(),
            )
            .map_err(|_| transient())?;
        }
        if stopping() {
            return Err(transient());
        }
        // Eligibility can change while a receiver maps its payload. Recheck it
        // in core immediately before transport and send the persisted bytes.
        let payload =
            validate_dispatch(&self.conn, &claim.lease, (self.clock)()).map_err(|_| transient())?;
        if stopping() {
            return Err(transient());
        }
        let ack = receiver.send(&payload, &claim.batch, &context)?;
        check_acknowledgment(&ack, &claim.batch)?;
        Ok(ack)
    }
}

/// Every acknowledged or unsupported id must be a distinct revision of this
/// exact batch. Core rejects a partial acceptance as a retryable hole.
fn check_acknowledgment(
    ack: &DeliveryAcknowledgment,
    batch: &HistoryExportBatch,
) -> Result<(), ReceiverFailure> {
    let expected: HashSet<&str> = batch
        .records
        .iter()
        .map(|record| record.revision_id.as_str())
        .collect();
    if ack.batch_id != batch.batch_id {
        return Err(DeliveryFailure::InvalidPayload.into());
    }
    let mut seen: HashSet<&str> = HashSet::new();
    for id in ack
        .accepted_revision_ids
        .iter()
        .chain(ack.unsupported_revision_ids.iter())
    {
        if !expected.contains(id.as_str()) || !seen.insert(id.as_str()) {
            return Err(DeliveryFailure::InvalidPayload.into());
        }
    }
    Ok(())
}

/// Sleep in slices so a finished attempt joins the keepalive thread promptly.
/// Returns false once the attempt asked it to stop.
fn sleep_until(stop: &AtomicBool, interval: Duration) -> bool {
    let slice = Duration::from_millis(5);
    let mut waited = Duration::ZERO;
    while waited < interval {
        if stop.load(Ordering::SeqCst) {
            return false;
        }
        let step = slice.min(interval - waited);
        std::thread::sleep(step);
        waited += step;
    }
    !stop.load(Ordering::SeqCst)
}
