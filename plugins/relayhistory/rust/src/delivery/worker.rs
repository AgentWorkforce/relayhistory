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
//! A drain is bounded: it attempts at most `max_batches` batches, starts no
//! further step once `max_elapsed` has passed, never waits for a retry deadline
//! and never enables, pauses or resumes a job. Delivery stays at least once, so
//! receivers must remain idempotent per revision.
//!
//! The core cannot forcibly interrupt a receiver: Rust has no way to cancel a
//! synchronous call. [`ReceiverContext::timeout_ms`] is the deadline a receiver
//! is required to honour itself (socket/read timeouts), and
//! [`ReceiverContext::cancelled`] reports a host stop request or a lost lease so
//! a long-running receiver can abandon its work early. A receiver that ignores
//! both merely delays this drain; its result is still discarded when the lease
//! was lost, and nothing is acknowledged.

use super::{
    acknowledge, claim_batch, compact_journal, compact_journal_pass, compact_receipts,
    compact_to_low_water, expire_exports, is_retention_limit, list_jobs, prepare_batch,
    record_failure, release_stopped_claim, renew_lease, retained_bytes, status,
    store_prepared_payload, validate_dispatch, ClaimedBatch, DeliveryAcknowledgment,
    DeliveryFailure, DeliveryJobConfig, DeliveryStatus, HistoryExportBatch, PreparedPayload,
    MAX_COMPACTION_PAGE,
};
use anyhow::{anyhow, bail, Result};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
    /// Wall-clock budget for the drain's delivery work, measured from the end
    /// of its own maintenance. Once it passes no further prepare step or
    /// attempt starts, and an attempt already under way runs to its receiver's
    /// own timeout. The budget bounds how long a drain keeps going, not whether
    /// it goes at all: its first step, and the first attempt that step
    /// prepares, always run, so however short the budget the queue still moves.
    /// `None` bounds the drain by counts alone.
    pub max_elapsed: Option<Duration>,
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
            max_elapsed: None,
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

/// Shortest gap between two lease-renewal attempts.
///
/// Once the keepalive is inside its renewal margin the computed wait is zero,
/// so without a floor a contended renewal would retry in a tight loop against
/// the writer holding the lock.
const RENEWAL_RETRY_FLOOR_MS: u64 = 5;

/// How long one renewal waits for the write lock before giving the decision
/// back to the renewal loop.
///
/// A sixth of the lease, so a blocked renewal returns with most of its margin
/// intact and several attempts still available before the deadline. Floored so
/// a very short lease still makes progress, and capped so a very long one does
/// not inherit the thirty-second wait this exists to avoid.
fn keepalive_busy_timeout_ms(lease_ms: i64) -> u64 {
    (lease_ms / 6).clamp(5, 2_000) as u64
}

/// Whether a failed write is SQLite refusing it because another connection
/// holds the lock, rather than the write itself being rejected.
///
/// "I could not write just now" and "this write is not allowed" are the same
/// `Err` at the call site, and the delivery keepalive read the first as the
/// second: a renewal that merely lost a race for the database marked the claim
/// lost, discarding a send that had already succeeded.
fn lost_a_race_for_the_database(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<rusqlite::Error>()
        .is_some_and(|error| {
            matches!(
                error,
                rusqlite::Error::SqliteFailure(failure, _)
                    if matches!(
                        failure.code,
                        rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
                    )
            )
        })
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
/// Maintenance proportional to what is reclaimable: expire abandoned
/// snapshots, release every settled receipt and reclaim every consumed
/// journal row. At the start of a drain that is all, so the wall-clock budget
/// goes to delivery; at the end, complete passes keep running while retained
/// bytes exceed three quarters of the cap, once the drain's acknowledgments
/// have released their bodies.
fn compact(conn: &Connection, now_ms: i64, recover: bool) -> Result<()> {
    expire_exports(conn, now_ms, 32)?;
    while compact_receipts(conn, MAX_COMPACTION_PAGE)? == MAX_COMPACTION_PAGE {}
    compact_journal(conn, MAX_COMPACTION_PAGE)?;
    if recover {
        compact_to_low_water(conn, MAX_COMPACTION_PAGE)?;
    }
    Ok(())
}
/// Run one local state write; when the retention cap refuses it, run one
/// complete compaction pass, recover to the low-water mark and retry the
/// write once. The refusal escapes only when nothing was reclaimable or the
/// retry is refused again. Batch writes have their own reserve above the
/// cap, so this fires only if that reserve is exhausted.
fn with_retention_recovery<T>(
    conn: &Connection,
    mut operation: impl FnMut() -> Result<T>,
) -> Result<T> {
    match operation() {
        Err(error) if is_retention_limit(&error) => {
            let reclaimed = compact_journal_pass(conn, MAX_COMPACTION_PAGE)?
                + compact_to_low_water(conn, MAX_COMPACTION_PAGE)?;
            if reclaimed == 0 {
                return Err(error);
            }
            operation()
        }
        result => result,
    }
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
    if let Some(limit) = options.max_elapsed {
        range(
            i64::try_from(limit.as_millis()).unwrap_or(i64::MAX),
            "max_elapsed_ms",
            1,
            86_400_000,
        )?;
    }
    let keepalive = super::open_db(db_path)?;
    // The shared busy policy retries for about thirty seconds, which is right
    // for a sync that must not give up and exactly wrong for a keepalive: a
    // renewal that waits thirty seconds to protect a lease shorter than that
    // has already lost the thing it was protecting, and it made the decision
    // inside a busy handler that has never heard of the deadline. Bound it so
    // contention comes back to the renewal loop, which does know.
    keepalive.busy_timeout(Duration::from_millis(keepalive_busy_timeout_ms(
        options.lease_ms,
    )))?;
    let conn = super::open_db(db_path)?;
    compact(&conn, clock(), false)?;
    let listed = list_jobs(&conn)?;
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
    // The budget covers delivery, so the clock starts once the drain's own
    // maintenance is done: on a loaded host, setup alone can outlast a short
    // budget and leave the drain no time to deliver anything.
    let mut worker = Worker {
        conn,
        keepalive: Mutex::new(keepalive),
        receivers,
        options,
        clock,
        cancelled,
        started: Instant::now(),
        attempts: 0,
        prepare_steps: 0,
        issues: Vec::new(),
    };
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
    compact(&worker.conn, clock(), true)?;
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
    started: Instant,
    attempts: usize,
    prepare_steps: usize,
    issues: Vec<DrainIssue>,
}

impl Worker<'_> {
    /// Whether the drain may start another step. The count ceilings are
    /// absolute; the elapsed budget bounds every step after the first, so a
    /// drain always does one unit of work however little budget is left, and
    /// scanning that produces no batch cannot run on past the deadline either.
    fn budget(&self) -> bool {
        self.attempts < self.options.max_batches
            && self.prepare_steps < self.options.max_prepare_steps
            && (self.prepare_steps == 0 || self.within_budget())
    }
    /// Whether an attempt may start. The first attempt of a drain always runs,
    /// so a batch is never left undelivered by a budget that expired while that
    /// same step was preparing it.
    fn may_attempt(&self) -> bool {
        self.attempts == 0 || self.within_budget()
    }
    fn within_budget(&self) -> bool {
        self.options
            .max_elapsed
            .is_none_or(|limit| self.started.elapsed() < limit)
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
        let prepared = with_retention_recovery(&self.conn, || {
            prepare_batch(&self.conn, job_id, (self.clock)())
        })?;
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
        // Preparing a batch can wait on the busy handler for longer than the
        // budget that remains. The batch stays pending for the next drain
        // rather than starting a network attempt past the deadline the host's
        // stop request and heartbeat depend on.
        if !self.may_attempt() {
            return Ok(Step::Progressed);
        }
        let Some(claim) = with_retention_recovery(&self.conn, || {
            claim_batch(
                &self.conn,
                job_id,
                &self.options.worker_id,
                self.options.lease_ms,
                self.clock,
            )
        })?
        else {
            // A privacy recheck can suppress the obsolete pending batch. If
            // re-inclusion has a fresh baseline ready, continue this bounded
            // drain instead of waiting for another host scheduling interval.
            let current = status(&self.conn, job_id)?;
            return Ok(
                if current.state == "active"
                    && current.pending_records == 0
                    && (!current.bootstrap_complete || current.unqueued_changes > 0)
                {
                    Step::Progressed
                } else {
                    Step::Idle
                },
            );
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
        // A cap refusal that recovery could not clear is recorded as a
        // transient failure like any other local error, and then reported at
        // the job boundary so the host sees the cap rather than a retry.
        let mut retention_refusal = None;
        let host = self.cancelled;
        let stopping = || host() || lost.load(Ordering::SeqCst);
        let outcome = {
            // Bind every value the keepalive thread touches: the worker itself
            // owns a connection and is deliberately not shareable.
            let keepalive = &self.keepalive;
            let clock = self.clock;
            let lease = &claim.lease;
            let lease_ms = self.options.lease_ms;
            // Renew with two thirds of the lease still to run, so two
            // consecutive renewals can be missed entirely before the claim is
            // at risk. Same cadence as a fixed `lease_ms / 3` interval in the
            // quiet case; the difference is that each wait is computed from
            // the lease's own deadline, so a renewal that took 300 ms is
            // followed by a correspondingly shorter wait instead of a full
            // interval on top of it. Anchoring on the deadline is what stops
            // the cadence drifting out under sustained load, which is how a
            // live claim silently lapsed and let a second worker dispatch the
            // same batch.
            let margin_ms = lease_ms - 1.max(lease_ms / 3);
            std::thread::scope(|scope| {
                let renewals = scope.spawn(|| {
                    let mut expires_at_ms = lease.expires_at_ms;
                    loop {
                        let wait_ms = (expires_at_ms - clock() - margin_ms).max(0) as u64;
                        // Inside the renewal margin the computed wait is zero,
                        // and a renewal that lost a race for the write lock
                        // must back off a little rather than contend in a
                        // tight loop with the writer it is waiting for.
                        let wait = Duration::from_millis(wait_ms.max(RENEWAL_RETRY_FLOOR_MS));
                        if !sleep_until(&stop, wait) {
                            break;
                        }
                        // Deliberately no "the deadline passed, so the claim is
                        // gone" check here. Expiry is a signal to *other*
                        // workers that an abandoned batch may be stolen, and
                        // `claim_batch` enforces it; a lease that lapsed while
                        // nobody took it is still ours, and `check_lease` says
                        // so by fence. Giving up on the clock alone would
                        // discard a send that had already succeeded, for a
                        // batch no one else ever touched.
                        let renewed = keepalive
                            .lock()
                            .map_err(|_| anyhow!("delivery keepalive connection is poisoned"))
                            .and_then(|conn| renew_lease(&conn, lease, lease_ms, &clock));
                        match renewed {
                            Ok(lease) => expires_at_ms = lease.expires_at_ms,
                            // Losing a race for the database is not losing the
                            // lease: the keepalive's connection gives up on a
                            // contended write quickly (see
                            // `keepalive_busy_timeout_ms`) precisely so the
                            // decision comes back here, where the deadline is
                            // known, rather than being made by a busy handler
                            // that has never heard of it. Retry.
                            //
                            // Anything else -- notably `check_lease` refusing
                            // because another worker now owns the batch -- is a
                            // verdict, and is reported at once.
                            Err(error) if lost_a_race_for_the_database(&error) => {}
                            Err(_) => {
                                lost.store(true, Ordering::SeqCst);
                                break;
                            }
                        }
                    }
                });
                // The keepalive must stop even if the receiver panics: the
                // scope joins every thread before unwinding, and a renewal loop
                // that never sees `stop` would hold the lease and hang the
                // drain. A receiver panic is contained as a transient failure
                // so one misbehaving destination cannot take the host down.
                let guard = StopOnDrop(&stop);
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    self.dispatch(receiver, claim, config, &stopping, &mut retention_refusal)
                }))
                .unwrap_or_else(|_| Err(DeliveryFailure::Transient.into()));
                drop(guard);
                // A panicking keepalive thread must not take the drain with it.
                let _ = renewals.join();
                result
            })
        };
        // A host stop is an orderly interruption, not a failed upload. The
        // transient result also covers a receiver honoring ctx.cancelled().
        // Keep explicit receiver refusals (including Retry-After) authoritative.
        // Even a completed send stays unacknowledged on stop: replay the same
        // persisted bytes/id after restart. The release is fenced in core.
        if host()
            && matches!(
                &outcome,
                Ok(_)
                    | Err(ReceiverFailure {
                        failure: DeliveryFailure::Transient,
                        retry_after_ms: None
                    })
            )
        {
            return release_stopped_claim(&self.conn, &claim.lease, self.clock);
        }
        let verdict = match outcome {
            // Lease loss still fences acknowledgments. If a stop arrives after
            // the outcome decision above, a completed owned send can commit;
            // do not turn that late stop into a synthetic receiver failure.
            Ok(_) if lost.load(Ordering::SeqCst) => Err(DeliveryFailure::Transient.into()),
            Ok(ack) => acknowledge(&self.conn, &claim.lease, &ack, self.clock)
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
                self.clock,
            )?;
        }
        match retention_refusal {
            Some(refusal) => Err(refusal),
            None => Ok(()),
        }
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
        retention_refusal: &mut Option<anyhow::Error>,
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
            with_retention_recovery(&self.conn, || {
                store_prepared_payload(
                    &self.conn,
                    &claim.lease,
                    receiver.mapping_version(),
                    &prepared.content_type,
                    &prepared.body,
                    self.clock,
                )
            })
            .map_err(|error| {
                if is_retention_limit(&error) {
                    *retention_refusal = Some(error);
                }
                transient()
            })?;
        }
        if stopping() {
            return Err(transient());
        }
        // Eligibility can change while a receiver maps its payload. Recheck it
        // in core immediately before transport and send the persisted bytes.
        let payload =
            validate_dispatch(&self.conn, &claim.lease, self.clock).map_err(|_| transient())?;
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

/// Raises the keepalive stop flag when dropped, including during unwinding.
struct StopOnDrop<'a>(&'a AtomicBool);
impl Drop for StopOnDrop<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// Sleep in slices so a finished attempt joins the keepalive thread promptly.
/// Returns false once the attempt asked it to stop.
///
/// The wait is measured against a real deadline, not by accumulating the
/// durations it asked for. `thread::sleep` guarantees only a lower bound, so a
/// loaded machine routinely returns from a 5 ms slice after 30 ms or more;
/// counting the requested 5 ms instead of the elapsed time made this function
/// silently overshoot by exactly the factor the machine was overloaded by.
/// That is the wrong way round for a keepalive: the renewal cadence stretched
/// precisely when scheduling pressure made a timely renewal matter most.
fn sleep_until(stop: &AtomicBool, interval: Duration) -> bool {
    let slice = Duration::from_millis(5);
    let deadline = Instant::now() + interval;
    loop {
        if stop.load(Ordering::SeqCst) {
            return false;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return !stop.load(Ordering::SeqCst);
        }
        std::thread::sleep(slice.min(remaining));
    }
}
