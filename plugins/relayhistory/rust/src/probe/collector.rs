use super::{lock, read_config, save_json, user_error, Config};
use anyhow::{ensure, Context, Result};
use relayhistory_plugin::delivery::{self, retention_limit_usage, worker, ExportSelection};
use relayhistory_plugin::destination;
use rusqlite::Connection;
use serde_json::json;
use std::{
    fs,
    io::Write,
    path::Path,
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc,
    },
    time::{Duration, Instant},
};

/// The destination and generation this probe delivers as. Both are part of the
/// saved job configuration, so they cannot change for an existing install.
const DESTINATION: &str = "relayhistory";
const INSTANCE: &str = "teams-probe";
/// Wall-clock budget of one delivery pass. A pass runs as many batches as the
/// destination accepts within it, so throughput follows the backlog, while a
/// stop request and the heartbeat still get the thread back promptly.
const DRAIN_TIME_BUDGET: Duration = Duration::from_secs(15);
/// Safety ceiling on attempts and prepare steps within one pass, well above
/// what the time budget admits against a live destination.
const DRAIN_STEP_CEILING: usize = 1_000;
/// Gap between passes while the job still has queued, unqueued or unscanned work.
const BACKLOG_PASS_INTERVAL: Duration = Duration::from_secs(2);
/// Gap between passes once the job is caught up.
const IDLE_PASS_INTERVAL: Duration = Duration::from_secs(20);
/// The retention sentence without a usage reading; the cycle report adds one.
const RETENTION_LIMIT_MESSAGE: &str =
    "Upload journal full. Compacting consumed records; queued sessions are preserved.";
/// The upload journal is at its retention cap, as reported by the delivery
/// drain rather than by a capture write. Typed so the cycle report classifies
/// it as the retention condition instead of a generic delivery fault.
#[derive(Debug)]
struct RetentionLimitReached;
impl std::fmt::Display for RetentionLimitReached {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(RETENTION_LIMIT_MESSAGE)
    }
}
impl std::error::Error for RetentionLimitReached {}
static STOP: AtomicBool = AtomicBool::new(false);
/// Poll the generation-scoped stop file independently of a long capture. The
/// hot record loops read an atomic flag rather than opening a file per record.
struct StopWatch {
    cancelled: Arc<AtomicBool>,
    finish: mpsc::Sender<()>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl StopWatch {
    fn start(directory: &Path, startup_id: &str) -> Self {
        let cancelled = Arc::new(AtomicBool::new(stop_requested(directory, startup_id)));
        let flag = cancelled.clone();
        let directory = directory.to_owned();
        let startup_id = startup_id.to_owned();
        let (finish, receive) = mpsc::channel();
        let thread = std::thread::spawn(move || loop {
            if stop_requested(&directory, &startup_id) {
                flag.store(true, Ordering::Relaxed);
                break;
            }
            match receive.recv_timeout(Duration::from_millis(100)) {
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                _ => break,
            }
        });
        Self {
            cancelled,
            finish,
            thread: Some(thread),
        }
    }
}
impl Drop for StopWatch {
    fn drop(&mut self) {
        let _ = self.finish.send(());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
fn stopping(cancelled: &AtomicBool) -> bool {
    cancelled.load(Ordering::Relaxed) || STOP.load(Ordering::Relaxed)
}

pub fn now() -> i64 {
    chrono::Utc::now().timestamp_millis()
}
/// The stored selection, independent of how much history exists locally. The
/// new-only baseline is deliberately absent: a delivery configuration is capped
/// at 64 KiB, which a few hundred session identities already exceed, and the
/// same setup must remain reproducible so an interrupted install can recover
/// its generation instead of orphaning it.
pub fn selection(include_existing: bool) -> ExportSelection {
    ExportSelection {
        all_sources: true,
        sources: vec![],
        sessions: vec![],
        // Prompt-only rows may have no session identity. New-only mode omits
        // them because no exclusion can prove when their session began.
        kinds: if include_existing {
            vec!["history".into(), "session_event".into(), "session".into()]
        } else {
            vec!["session_event".into(), "session".into()]
        },
        excluded_sessions: vec![],
    }
}
/// Converge the durable exclusion baseline on the requested sharing choice and
/// report how many sessions it now withholds. Delivery rechecks exclusions when
/// a batch is prepared, claimed and dispatched, so a new-only baseline has to be
/// complete before the job exists: written afterwards it would race an already
/// prepared batch, whereas written without a job it only withholds more history.
///
/// That ordering is also why the choice to share existing sessions must withdraw
/// the baseline rather than ignore it. Exclusions outlive the generation they
/// were recorded for, so a setup abandoned between the snapshot and its job
/// would otherwise keep suppressing exactly the history the user has now opted
/// in to send. Withdrawal is refused while a live job still selects a session,
/// which this path cannot reach: it runs only when no generation was adopted.
pub fn record_baseline(conn: &Connection, include_existing: bool) -> Result<usize> {
    let snapshot = conn.unchecked_transaction()?;
    let mut identities = Vec::new();
    loop {
        let page = ai_hist::storage::session_identities_after(&snapshot, identities.last(), 1000)?;
        if page.is_empty() {
            break;
        }
        identities.extend(page);
    }
    // Exclusions take their own short write transactions, so the consistent
    // read has to end before any of them is recorded or withdrawn.
    snapshot.commit()?;
    for identity in &identities {
        delivery::set_session_excluded(conn, identity, !include_existing)?;
    }
    Ok(if include_existing {
        0
    } else {
        identities.len()
    })
}
/// One bounded delivery pass through the shared core delivery worker — the
/// same loop the SDK drains with — carrying the RelayHistory receiver. The
/// worker owns leases and their keepalive, prepared-payload persistence, the
/// eligibility recheck before dispatch, retry classification and compaction;
/// the receiver owns only the legacy-scheduler/account/instance guards and the
/// transport. Returns the job status the drain left behind.
fn deliver(
    db_path: &Path,
    config: &Config,
    cancelled: &AtomicBool,
) -> Result<delivery::DeliveryStatus> {
    let receiver = destination::RelayHistoryReceiver {
        base_url: Some(config.history_url.clone()),
        expected_account: Some(config.delivery_account.clone()),
        instance_id: Some(INSTANCE.into()),
        acknowledge_uninspected_schedules: config.acknowledge_uninspected_schedules,
    };
    deliver_with_receiver(db_path, config, &receiver, &|| stopping(cancelled))
}
fn deliver_with_receiver(
    db_path: &Path,
    config: &Config,
    receiver: &dyn worker::Receiver,
    cancelled: &(dyn Fn() -> bool + Sync),
) -> Result<delivery::DeliveryStatus> {
    let options = worker::DrainOptions {
        job_ids: Some(vec![config.job_id.clone()]),
        // A pass is bounded by wall time so a stop request and the heartbeat
        // are never starved by an unbounded backlog; the next pass continues
        // it. The count bounds are ceilings the time budget rarely reaches.
        max_elapsed: Some(DRAIN_TIME_BUDGET),
        max_batches: DRAIN_STEP_CEILING,
        max_prepare_steps: DRAIN_STEP_CEILING,
        ..worker::DrainOptions::new(format!("probe-{}", std::process::id()))
    };
    let result = worker::drain(
        db_path,
        &worker::SingleReceiver::new(DESTINATION, INSTANCE, receiver),
        &options,
        &worker::system_clock,
        cancelled,
    )?;
    let status = result
        .statuses
        .into_iter()
        .find(|job| job.job_id == config.job_id)
        .context("delivery job missing")?;
    // Pause can arrive while a batch is in flight. It is a successful user
    // control action, even if it invalidated that batch's lease.
    if status.state == "paused" {
        return Ok(status);
    }
    if status.state != "active" {
        return Err(user_error(
            "Delivery needs attention. Reconnect with --force-login if credentials have expired.",
        ));
    }
    // A recorded transport failure is not an issue: the job stays active and
    // the worker owns its backoff. Only an unusable local state is reported,
    // and waiting for a retry deadline is normal operation, not a fault.
    //
    // A full journal is the one delivery fault with a user action behind it,
    // so it keeps its own condition: the report then names the usage and the
    // desktop offers compaction instead of showing a generic queued sentence.
    if result
        .issues
        .iter()
        .any(|issue| matches!(issue.code, worker::DrainIssueCode::DeliveryRetentionLimit))
    {
        return Err(RetentionLimitReached.into());
    }
    if !result.issues.is_empty() {
        return Err(user_error(
            "Session delivery is queued or needs attention. No unacknowledged data was discarded.",
        ));
    }
    Ok(status)
}
pub fn capture(directory: &Path, history_url: &str) -> Result<()> {
    capture_with_stop(directory, history_url, Arc::new(AtomicBool::new(false)))
}
fn capture_with_stop(
    directory: &Path,
    history_url: &str,
    cancelled: Arc<AtomicBool>,
) -> Result<()> {
    let started = Instant::now();
    let progress = super::progress::Monitor::start(directory, history_url, None, false);
    let latest = Arc::new(std::sync::Mutex::new(ai_hist::CaptureProgress::default()));
    let seen = latest.clone();
    let observer = progress.observer();
    let result = ai_hist::sync_local_at_cancellable(
        &directory.join("history.db"),
        move |value| {
            if let Ok(mut last) = seen.lock() {
                *last = value.clone();
            }
            observer(value);
        },
        move || stopping(&cancelled),
    );
    progress.finish(matches!(result, Ok(true)));
    capture_diagnostic(
        directory,
        "broad_capture",
        started,
        0,
        usize::from(!matches!(result, Ok(true))),
        result.as_ref().err(),
    )?;
    if let Err(error) = &result {
        let last = latest.lock().ok();
        let source = last
            .as_ref()
            .map(|v| v.source.as_str())
            .filter(|s| ai_hist::SOURCE_CHOICES.contains(s))
            .unwrap_or("unknown");
        let retention = retention_for(directory, error);
        let value = json!({"operation":"capture","stage":"broad_capture","source":source,"processed_files":last.as_ref().map(|v|v.processed_files).unwrap_or(0),"elapsed_ms":started.elapsed().as_millis(),"error_class":capture_error_class(error),"used_bytes":retention.map(|(used, _)| used),"limit_bytes":retention.map(|(_, limit)| limit)});
        save_json(&directory.join("capture-diagnostic.json"), &value)?;
        eprintln!("{value}");
    }
    if matches!(result, Ok(false)) {
        save_json(
            &directory.join("capture-diagnostic.json"),
            &json!({"operation":"capture","stage":"broad_capture","elapsed_ms":started.elapsed().as_millis(),"outcome":"lock_contended","error_class":"capture_not_started"}),
        )?;
    }
    ensure!(result?, "capture not complete");
    Ok(())
}

#[cfg(test)]
pub fn cycle(directory: &Path, config: &Config) -> Result<()> {
    cycle_with_stop(directory, config, Arc::new(AtomicBool::new(false)), false)?.result()
}

/// What one pass established. Capture and delivery fail independently and are
/// observed at different cadences, so a pass reports them apart: a pass that
/// only delivered leaves `capture` unset and the last capture verdict stands.
struct PassOutcome {
    capture: Option<Result<()>>,
    delivery: Result<()>,
}
impl PassOutcome {
    fn delivery_only(delivery: Result<()>) -> Self {
        Self {
            capture: None,
            delivery,
        }
    }
    /// One result for callers that report a single outcome. A capture fault
    /// names a local condition to act on, so it outranks a delivery fault.
    fn result(self) -> Result<()> {
        match self.capture {
            Some(Err(error)) => Err(error),
            _ => self.delivery,
        }
    }
}

fn cycle_with_stop(
    directory: &Path,
    config: &Config,
    cancelled: Arc<AtomicBool>,
    before_exit: bool,
) -> Result<PassOutcome> {
    super::bridge::enforce_selection(directory, config)?;
    let conn = relayhistory_plugin::delivery::open_db(&directory.join("history.db"))?;
    let status = delivery::status(&conn, &config.job_id)?;
    let selected = super::bridge::mode(config) == super::bridge::SharingMode::Selected;
    if status.state == "paused" {
        // A paused job delivers nothing. Broad capture still runs, so its
        // outcome is this pass's capture verdict; a selected pass observes
        // nothing about capture and leaves the last verdict standing.
        if selected {
            return Ok(PassOutcome::delivery_only(Ok(())));
        }
        let stop = cancelled.clone();
        let captured = ai_hist::sync_local_at_cancellable(
            &directory.join("history.db"),
            |_| {},
            move || stopping(&stop),
        );
        return Ok(PassOutcome {
            capture: Some(captured.map(|_| ())),
            delivery: Ok(()),
        });
    }
    Ok(run_capture_cycle(
        selected,
        || deliver_captured_with_stop(directory, config, before_exit, &cancelled),
        || ready_to_capture(&conn, &config.job_id),
        || capture_selected(directory, config, &cancelled),
        || capture_with_stop(directory, &config.history_url, cancelled.clone()),
        || stopping(&cancelled),
    ))
}

fn ready_to_capture(conn: &Connection, job_id: &str) -> Result<bool> {
    let status = delivery::status(conn, job_id)?;
    // A retry deadline means delivery cannot progress yet. Keep capturing
    // locally while offline rather than waiting for the remote queue to clear.
    Ok(status.next_attempt_ms > now()
        || (status.bootstrap_complete
            && status.pending_records == 0
            && status.unqueued_changes == 0))
}

// Tests inject a failing/blocked unrelated provider. Selected cycles never
// enter it, and backlog gets priority over any further selected hydration.
fn run_capture_cycle(
    selected: bool,
    deliver: impl Fn() -> Result<()>,
    capture_ready: impl Fn() -> Result<bool>,
    targeted: impl Fn() -> Result<()>,
    broad: impl Fn() -> Result<()>,
    stopped: impl Fn() -> bool,
) -> PassOutcome {
    let delivered = deliver();
    if stopped() {
        return PassOutcome::delivery_only(delivered);
    }
    if delivered.is_ok() && selected {
        match capture_ready() {
            // Readiness reads delivery state, so a failed read is delivery's
            // own outcome; capture simply does not run this pass.
            Err(error) => return PassOutcome::delivery_only(Err(error)),
            Ok(false) => return PassOutcome::delivery_only(delivered),
            Ok(true) => {}
        }
    }
    // Delivery errors must remain visible, but must not prevent acquiring
    // local evidence while credentials, transport or receiver state recover.
    let capture = if selected { targeted() } else { broad() };
    if stopped() {
        return PassOutcome {
            capture: Some(capture),
            delivery: delivered,
        };
    }
    let drained = deliver();
    PassOutcome {
        capture: Some(capture),
        delivery: delivered.and(drained),
    }
}

/// Hydrate only authorized members using core observation locks and cancellation.
fn capture_selected(directory: &Path, config: &Config, cancelled: &Arc<AtomicBool>) -> Result<()> {
    let conn = relayhistory_plugin::delivery::open_db(&directory.join("history.db"))?;
    let members = delivery::job_sessions(&conn, &config.job_id)?;
    capture_members(
        directory,
        members,
        || stopping(cancelled),
        |member| {
            // Core hydration owns observation locks, parsers and checkpoints.
            // It never enumerates an unrelated provider.
            let stop = cancelled.clone();
            ai_hist::hydrate_session_at_cancellable(
                &directory.join("history.db"),
                &ai_hist::HydrateSessionOptions {
                    source: member.source,
                    session_id: member.session_id,
                    scope: ai_hist::SessionScope::Local,
                    include_related: false,
                },
                move || stopping(&stop),
            )
            .map(|_| ())
        },
    )
}

/// Continue across member failures while retaining the first typed cause for
/// safe reporting. A member that stopped at the retention cap ends the pass:
/// every remaining member would meet the same budget, and the members captured
/// before it stay committed.
fn capture_members(
    directory: &Path,
    members: Vec<delivery::SessionIdentity>,
    stopped: impl Fn() -> bool,
    hydrate: impl Fn(delivery::SessionIdentity) -> Result<()>,
) -> Result<()> {
    let started = Instant::now();
    let mut captured = 0;
    let mut failed = 0;
    let mut first_error = None;
    for member in members {
        if stopped() {
            break;
        }
        match hydrate(member) {
            Ok(()) => captured += 1,
            Err(error) => {
                failed += 1;
                capture_diagnostic(
                    directory,
                    "targeted_hydration",
                    started,
                    captured,
                    failed,
                    Some(&error),
                )?;
                // The retention stop is why the pass ended and is the only
                // cause carrying the budget, so it replaces an earlier
                // member's failure; ordinary failures keep the first.
                if delivery::is_retention_limit(&error) {
                    first_error = Some(error);
                    break;
                }
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
    }
    if let Some(error) = first_error {
        // Keep the typed cause for the safe cycle/desktop classifier. Replacing
        // it with a generic aggregate turns a full journal into "offline" again.
        return Err(error.context("selected session capture incomplete"));
    }
    capture_diagnostic(directory, "targeted_hydration", started, captured, 0, None)?;
    Ok(())
}

/// Classify typed causes into stable public labels without exposing raw error details.
fn capture_error_class(error: &anyhow::Error) -> &'static str {
    if error.downcast_ref::<ai_hist::CaptureCancelled>().is_some() {
        return "cancelled";
    }
    if error.downcast_ref::<RetentionLimitReached>().is_some()
        || delivery::is_retention_limit(error)
    {
        return "retention_limit";
    }
    for cause in error.chain() {
        if let Some(sql) = cause.downcast_ref::<rusqlite::Error>() {
            return match sql.sqlite_error_code() {
                Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked) => {
                    "database_busy"
                }
                Some(rusqlite::ErrorCode::DiskFull) => "disk_full",
                Some(rusqlite::ErrorCode::ReadOnly | rusqlite::ErrorCode::PermissionDenied) => {
                    "permission_denied"
                }
                Some(rusqlite::ErrorCode::CannotOpen) => "database_unavailable",
                Some(rusqlite::ErrorCode::DatabaseCorrupt) => "database_corrupt",
                _ => "database_error",
            };
        }
        if let Some(io) = cause.downcast_ref::<std::io::Error>() {
            return match io.kind() {
                std::io::ErrorKind::NotFound => "source_missing",
                std::io::ErrorKind::PermissionDenied => "permission_denied",
                std::io::ErrorKind::StorageFull => "disk_full",
                _ => "io_error",
            };
        }
        if cause.downcast_ref::<serde_json::Error>().is_some() {
            return "invalid_source_json";
        }
    }
    "capture_error"
}

/// Return actionable, allowlisted storage guidance suitable for desktop status and stderr.
pub(super) fn local_failure_message(error: &anyhow::Error) -> Option<&'static str> {
    class_failure_message(capture_error_class(error))
}

/// The guidance for a class, so a verdict read back from a report renders the
/// same sentence as the error it came from.
fn class_failure_message(class: &str) -> Option<&'static str> {
    match class {
        "retention_limit" => Some(RETENTION_LIMIT_MESSAGE),
        "database_corrupt" => Some("The local history database needs repair. Preserve the database before recovery; reconnecting will not repair it."),
        "disk_full" => Some("The local disk is full. Free disk space to resume session uploads."),
        "database_busy" => Some("Local history is busy. Agent Relay will retry when the other operation finishes."),
        "database_unavailable" => Some("Agent Relay cannot open local history. Check that its directory exists and local file permissions allow access."),
        "permission_denied" => Some("Agent Relay cannot access local history. Check local file permissions."),
        _ => None,
    }
}

/// The retention sentence with the measured usage, so the desktop can show the
/// number and offer compaction. A journal the shown figures put at its cap is
/// "full"; one capture stopped at over the high-water mark is "nearly full".
/// Without a reading it names the condition alone.
fn retention_limit_message(retention: Option<(i64, i64)>) -> String {
    let megabytes = |bytes: i64| (bytes + 524_288) / 1_048_576;
    match retention {
        Some((used, limit)) if megabytes(used) >= megabytes(limit) => format!(
            "Upload journal full ({} MB of {} MB). Compacting consumed records; queued sessions are preserved.",
            megabytes(used),
            megabytes(limit)
        ),
        Some((used, limit)) => format!(
            "Upload journal nearly full ({} MB of {} MB); capture is waiting for room. Compacting consumed records; queued sessions are preserved.",
            megabytes(used),
            megabytes(limit)
        ),
        None => RETENTION_LIMIT_MESSAGE.to_owned(),
    }
}

/// Current journal usage against its cap, for reports; `None` when the
/// database cannot be read, which the report tolerates.
fn read_retention(directory: &Path) -> Option<(i64, i64)> {
    let conn = ai_hist::open_db_readonly(&directory.join("history.db")).ok()?;
    delivery::retained_bytes(&conn).ok()
}

/// The last verdict on each side of a pass: the stable class of that outcome,
/// `None` where it succeeded. Only the class is carried, so a verdict renders
/// its sentence from the current reading rather than a stale byte count.
#[derive(Default, Clone)]
struct Verdicts {
    capture: Option<String>,
    delivery: Option<String>,
}
impl Verdicts {
    #[cfg(test)]
    fn capture(result: &Result<()>) -> Self {
        Self {
            capture: verdict(result),
            delivery: None,
        }
    }
    /// The one class the desktop acts on: a capture fault first, because it
    /// names a local condition, then a delivery fault.
    fn effective(&self) -> Option<&str> {
        self.capture.as_deref().or(self.delivery.as_deref())
    }
    fn retention_limited(&self) -> bool {
        [&self.capture, &self.delivery]
            .iter()
            .any(|class| class.as_deref() == Some("retention_limit"))
    }
}

/// Classify one side's result into its public label.
fn verdict(result: &Result<()>) -> Option<String> {
    result
        .as_ref()
        .err()
        .map(|error| capture_error_class(error).to_owned())
}

/// The sentence for a class; the retention one carries the measured usage.
fn class_message(class: Option<&str>, retention: Option<(i64, i64)>) -> Option<String> {
    match class {
        None => None,
        Some("retention_limit") => Some(retention_limit_message(retention)),
        Some(class) => Some(
            class_failure_message(class)
                .unwrap_or("Sync paused or offline. Retrying; local data remains queued.")
                .to_owned(),
        ),
    }
}

/// The `(used_bytes, limit_bytes)` a failure is reported with: the budget the
/// pass stopped at when the failure carries it, otherwise the current budget
/// when the cap caused the failure. `None` for every other failure.
fn retention_for(directory: &Path, error: &anyhow::Error) -> Option<(i64, i64)> {
    if capture_error_class(error) != "retention_limit" {
        return None;
    }
    retention_limit_usage(error)
        .map(|usage| (usage.used_bytes, usage.limit_bytes))
        .or_else(|| read_retention(directory))
}

/// Build the advisory cycle status from typed classes without including
/// provider data. `capture` and `delivery` carry each side; the top-level
/// fields are the effective verdict, which is what the desktop shows.
fn cycle_report(verdicts: &Verdicts, retention: Option<(i64, i64)>) -> serde_json::Value {
    let side = |class: Option<&str>| json!({"ok": class.is_none(), "error_class": class, "message": class_message(class, retention)});
    let effective = verdicts.effective();
    json!({
        "at_ms": now(), "ok": effective.is_none(),
        "error_class": effective,
        "message": class_message(effective, retention),
        "capture": side(verdicts.capture.as_deref()),
        "delivery": side(verdicts.delivery.as_deref()),
        "used_bytes": retention.map(|(used, _)| used),
        "limit_bytes": retention.map(|(_, limit)| limit),
    })
}

/// The persisted report: when it was written, and the verdict it holds on each
/// side. A report that cannot be read carries nothing, and the next pass of
/// each kind re-establishes its side.
#[derive(Default)]
struct SavedReport {
    at_ms: i64,
    verdicts: Verdicts,
}
fn saved_report(directory: &Path) -> SavedReport {
    let side = |report: &serde_json::Value, name: &str| {
        report[name]["error_class"].as_str().map(str::to_owned)
    };
    fs::read(directory.join("cycle.json"))
        .ok()
        .and_then(|data| serde_json::from_slice::<serde_json::Value>(&data).ok())
        .map(|report| SavedReport {
            at_ms: report["at_ms"].as_i64().unwrap_or_default(),
            verdicts: Verdicts {
                capture: side(&report, "capture"),
                delivery: side(&report, "delivery"),
            },
        })
        .unwrap_or_default()
}

/// Serialize one report's read-modify-write against the other writer's. The
/// collector reports every pass and a desktop compaction re-measures the cap
/// between them, so without this the later write of two overlapping sequences
/// silently replaces the other's answer. The guard is advisory: a report that
/// cannot take it is still written, because status must never hold up a retry.
fn cycle_guard(directory: &Path) -> Option<fs::File> {
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(directory.join("cycle.lock"))
        .ok()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o600))
            .ok()?;
    }
    fs2::FileExt::lock_exclusive(&file).ok()?;
    Some(file)
}

/// Persist advisory status without interrupting retries when either status or logs cannot be written.
///
/// A retention verdict renders with the current reading of the journal; the
/// usage a failure of this pass `observed` stands in only when the journal
/// cannot be read.
fn write_cycle_report(
    directory: &Path,
    verdicts: &Verdicts,
    observed: Option<(i64, i64)>,
    mut diagnostics: impl Write,
) {
    let retention = verdicts
        .retention_limited()
        .then(|| read_retention(directory).or(observed))
        .flatten();
    let report = cycle_report(verdicts, retention);
    if let Err(error) = save_json(&directory.join("cycle.json"), &report) {
        let message = local_failure_message(&error)
            .unwrap_or("Sync status could not be saved. Retrying; local data remains queued.");
        // collector.log may be on the same full disk. Logging must not panic
        // or propagate another I/O error out of the retry loop either.
        let _ = writeln!(diagnostics, "{message}");
    }
    if let Some(message) = report["message"].as_str() {
        let _ = writeln!(diagnostics, "{message}");
    }
}

/// Report one pass, which began at `started_ms`. A pass that only delivered
/// observed nothing about capture, so the last capture verdict stands: a full
/// journal stays visible, with its numbers, between the capture cycles that
/// measure it.
///
/// A compaction that reported while this pass ran has re-measured the cap the
/// pass observed, and its answer is the current one. The older reading is
/// dropped rather than putting a resolved condition back on screen until
/// another pass measures it again.
fn persist_cycle_report(
    directory: &Path,
    outcome: &PassOutcome,
    started_ms: i64,
    diagnostics: impl Write,
) {
    let _guard = cycle_guard(directory);
    let saved = saved_report(directory);
    let recompacted = saved.at_ms > started_ms;
    let current = |observed: Option<String>, saved: &Option<String>| match (&observed, saved) {
        (Some(class), None) if recompacted && class == "retention_limit" => None,
        _ => observed,
    };
    let verdicts = Verdicts {
        capture: match &outcome.capture {
            Some(result) => current(verdict(result), &saved.verdicts.capture),
            None => saved.verdicts.capture.clone(),
        },
        delivery: current(verdict(&outcome.delivery), &saved.verdicts.delivery),
    };
    write_cycle_report(directory, &verdicts, observed_usage(outcome), diagnostics);
}

/// The usage a retention failure of this pass carries, capture side first.
fn observed_usage(outcome: &PassOutcome) -> Option<(i64, i64)> {
    outcome
        .capture
        .iter()
        .chain(std::iter::once(&outcome.delivery))
        .filter_map(|result| result.as_ref().err())
        .find_map(retention_limit_usage)
        .map(|usage| (usage.used_bytes, usage.limit_bytes))
}

/// Re-measure a retention verdict after a compaction pass that reclaimed
/// `reclaimed_bytes`.
///
/// The cap rejects the write that would exceed it without recording it, so a
/// journal that is full for the next record still reads below its cap: usage
/// alone never shows the condition, and only reclaiming space changes it. A
/// pass that freed space and left the journal under its cap therefore clears
/// the verdict, and one that freed nothing — a cap held by un-uploaded backlog
/// — leaves it standing with the current reading. Either way the next capture
/// or delivery pass measures the condition itself.
pub(super) fn refresh_retention_verdict(
    directory: &Path,
    reclaimed_bytes: i64,
    diagnostics: impl Write,
) {
    let _guard = cycle_guard(directory);
    let mut verdicts = saved_report(directory).verdicts;
    if !verdicts.retention_limited() {
        return;
    }
    let resolved =
        reclaimed_bytes > 0 && read_retention(directory).is_some_and(|(used, limit)| used < limit);
    if resolved {
        for class in [&mut verdicts.capture, &mut verdicts.delivery] {
            if class.as_deref() == Some("retention_limit") {
                *class = None;
            }
        }
    }
    write_cycle_report(directory, &verdicts, None, diagnostics);
}

fn capture_diagnostic(
    directory: &Path,
    stage: &str,
    started: Instant,
    captured: usize,
    failed: usize,
    error: Option<&anyhow::Error>,
) -> Result<()> {
    let retention = error.and_then(|error| retention_for(directory, error));
    let diagnostic = json!({"operation":"capture","stage":stage,"elapsed_ms":started.elapsed().as_millis(),"sessions_captured":captured,"failures":failed,"error_class":error.map(capture_error_class),"used_bytes":retention.map(|(used, _)| used),"limit_bytes":retention.map(|(_, limit)| limit)});
    let filename = if stage == "shallow_inventory" {
        "inventory-diagnostic.json"
    } else {
        "capture-diagnostic.json"
    };
    save_json(&directory.join(filename), &diagnostic)?;
    if error.is_some() {
        eprintln!("{diagnostic}");
    }
    Ok(())
}

/// All/new setup has already captured before creating its baseline. Selected
/// setup stays shallow unless --once makes this the only capture opportunity.
pub fn finish_setup(directory: &Path, config: &Config, once: bool) -> Result<()> {
    if once && super::bridge::mode(config) == super::bridge::SharingMode::Selected {
        cycle_with_stop(directory, config, Arc::new(AtomicBool::new(false)), true)?.result()
    } else {
        deliver_captured(directory, config, true)
    }
}

pub fn deliver_captured(directory: &Path, config: &Config, before_exit: bool) -> Result<()> {
    deliver_captured_with_stop(directory, config, before_exit, &AtomicBool::new(false))
}
fn deliver_captured_with_stop(
    directory: &Path,
    config: &Config,
    before_exit: bool,
    cancelled: &AtomicBool,
) -> Result<()> {
    if stopping(cancelled) {
        return Ok(());
    }
    // Every drain must apply selected-mode exclusions, including retries after
    // a failed capture. Never reach the delivery worker with an unvetted row.
    super::bridge::enforce_selection(directory, config)?;
    {
        let conn = relayhistory_plugin::delivery::open_db(&directory.join("history.db"))?;
        if delivery::status(&conn, &config.job_id)?.state == "paused" {
            return Ok(());
        }
    }
    ensure!(
        destination::selected_account(Some(&config.history_url))? == config.delivery_account,
        "wrong destination"
    );
    let db_path = directory.join("history.db");
    // The receiver rejects a batch whose account or mapping does not match, but
    // only once one exists. An idle generation pointed at another destination
    // must not look healthy, so the saved configuration is checked outright.
    {
        let conn = relayhistory_plugin::delivery::open_db(&db_path)?;
        let job = delivery::status(&conn, &config.job_id)?;
        if job.state == "paused" {
            return Ok(());
        }
        ensure!(
            job.config.account_id == config.delivery_account
                && job.config.mapping_version == destination::MAPPING_VERSION,
            "delivery config mismatch"
        );
    }
    // The receiver blocks the job when an older managed uploader appears, but
    // its verdict is a generic permission refusal. Name the cause here first so
    // the user sees which uploader to stop instead of a reconnect suggestion.
    super::check_legacy_schedules(config.acknowledge_uninspected_schedules)?;
    let progress = super::progress::Monitor::start(
        directory,
        &config.history_url,
        Some(&config.job_id),
        before_exit,
    );
    let result = deliver(&db_path, config, cancelled);
    if before_exit {
        let connected =
            progress.finish_before_exit(result.is_ok(), super::progress::COMPLETION_TIMEOUT);
        if result.is_ok() {
            ensure!(connected, user_error("Cloud connection could not be confirmed. Check the endpoint and retry; local data remains queued."));
        }
    } else {
        progress.finish(result.is_ok());
    }
    let status = result?;
    humanln!(
        "Probe connected: {} records received, {} queued.",
        status.acknowledged_records,
        status.pending_records
    );
    Ok(())
}
pub(super) fn running(directory: &Path) -> Result<bool> {
    if !directory.join("collector.lock").exists() {
        return Ok(false);
    }
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(directory.join("collector.lock"))?;
    match fs2::FileExt::try_lock_exclusive(&file) {
        Ok(()) => Ok(false),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(true),
        Err(error) => Err(error.into()),
    }
}
pub fn print_status(directory: &Path) -> Result<()> {
    humanln!(
        "{}",
        if running(directory)? {
            "Probe is running."
        } else {
            "Probe is stopped."
        }
    );
    Ok(())
}
pub fn stop(directory: &Path) -> Result<()> {
    stop_with_timeout(directory, Some(Duration::from_secs(45)))
}
pub fn stop_for_change(directory: &Path) -> Result<()> {
    // The durable stop request may outlive the normal CLI deadline. A sharing
    // change must wait for the lock to be released before it can safely apply
    // its plan and guarantee a replacement collector is started.
    stop_with_timeout(directory, None)
}
fn stop_with_timeout(directory: &Path, timeout: Option<Duration>) -> Result<()> {
    if !running(directory)? {
        humanln!("Probe is stopped.");
        return Ok(());
    }
    let runtime: serde_json::Value =
        serde_json::from_slice(&fs::read(directory.join("runtime.json"))?)?;
    let startup_id = runtime["startup_id"]
        .as_str()
        .context("missing run identity")?;
    save_json(
        &directory.join("stop.json"),
        &json!({"startup_id":startup_id}),
    )?;
    let deadline = timeout.map(|duration| Instant::now() + duration);
    loop {
        if !running(directory)? {
            humanln!("Probe stopped.");
            return Ok(());
        }
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(user_error(
                "Stop was requested. The probe is finishing its current capture operation.",
            ));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}
pub(super) fn stop_requested(directory: &Path, startup_id: &str) -> bool {
    let matched = fs::read(directory.join("stop.json"))
        .ok()
        .and_then(|data| serde_json::from_slice::<serde_json::Value>(&data).ok())
        .is_some_and(|value| value["startup_id"] == startup_id);
    STOP.load(Ordering::Relaxed) || matched
}
#[cfg(unix)]
extern "C" fn stop_signal(_: libc::c_int) {
    STOP.store(true, Ordering::Relaxed);
}
// Provider enumeration runs independently of selected delivery. The channel
// wakes its idle wait on shutdown; an in-flight provider read never owns the
// collector's control lock or blocks a delivery cycle waiting for a join.
struct InventoryWorker(mpsc::Sender<()>);
impl Drop for InventoryWorker {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}
fn inventory_options() -> ai_hist::DiscoverOptions {
    ai_hist::DiscoverOptions {
        scope: ai_hist::SessionScope::Local,
        sources: vec![],
        // This independent worker builds the complete catalog. A recency cap
        // would rediscover the same newest rows forever on selected installs.
        limit: None,
    }
}

fn start_inventory(directory: &Path, cancelled: Arc<AtomicBool>) -> InventoryWorker {
    let directory = directory.to_path_buf();
    let (finish, done) = mpsc::channel();
    std::thread::spawn(move || {
        // Give the first delivery pass priority over inventory initialization.
        if done.recv_timeout(Duration::from_secs(1)).is_ok() {
            return;
        }
        loop {
            if stopping(&cancelled) {
                break;
            }
            let started = Instant::now();
            let result = (|| -> Result<usize> {
                let conn = relayhistory_plugin::delivery::open_db(&directory.join("history.db"))?;
                let mut count = 0;
                let stop = cancelled.clone();
                ai_hist::discover_sessions_cancellable(
                    &conn,
                    &inventory_options(),
                    |_| count += 1,
                    move || stopping(&stop),
                )?;
                Ok(count)
            })();
            let _ = capture_diagnostic(
                &directory,
                "shallow_inventory",
                started,
                result.as_ref().copied().unwrap_or(0),
                usize::from(result.is_err()),
                result.as_ref().err(),
            );
            if done.recv_timeout(Duration::from_secs(60)).is_ok() {
                break;
            }
        }
    });
    InventoryWorker(finish)
}

/// Own the collector lock and retry capture/delivery until stopped; status writes are advisory.
pub fn run_background(directory: &Path, startup_id: &str) -> Result<()> {
    let _lock = lock(directory)?;
    ensure!(
        !directory.join("sharing-change.json").exists(),
        user_error("A sharing update is incomplete. Run start to recover it before retrying.")
    );
    let config = read_config(directory)?;
    std::env::set_var("RELAYHISTORY_HOME", directory);
    #[cfg(unix)]
    unsafe {
        // Handlers only store an atomic flag. Network/capture cleanup happens in
        // the normal loop, outside the signal handler.
        libc::signal(
            libc::SIGTERM,
            stop_signal as *const () as libc::sighandler_t,
        );
        libc::signal(libc::SIGINT, stop_signal as *const () as libc::sighandler_t);
    }
    // Readiness means this run owns the lock and its retry loop is live, not
    // that a cycle has finished. Capture and drain are unbounded in the size of
    // local history, so announcing readiness afterwards lets a supervisor's
    // startup wait expire and kill a collector that is working normally. Setup
    // already completed one cycle before spawning this process.
    save_json(
        &directory.join("runtime.json"),
        &json!({"startup_id":startup_id,"pid":std::process::id(),"ready":true}),
    )?;
    let stop = StopWatch::start(directory, startup_id);
    let _inventory = if super::bridge::mode(&config) == super::bridge::SharingMode::Selected {
        Some(start_inventory(directory, stop.cancelled.clone()))
    } else {
        None
    };
    let mut capture_due = Instant::now();
    while !stopping(&stop.cancelled) {
        let started_ms = now();
        let outcome = if Instant::now() >= capture_due {
            capture_due = Instant::now() + Duration::from_secs(60);
            // A pass that cannot start reports one local fault on the delivery
            // side, where its causes live; the last capture verdict stands.
            cycle_with_stop(directory, &config, stop.cancelled.clone(), false)
                .unwrap_or_else(|error| PassOutcome::delivery_only(Err(error)))
        } else {
            PassOutcome::delivery_only(deliver_captured_with_stop(
                directory,
                &config,
                false,
                &stop.cancelled,
            ))
        };
        // A user stop is not a failed/offline cycle and must not start delivery.
        if stopping(&stop.cancelled) {
            break;
        }
        persist_cycle_report(directory, &outcome, started_ms, std::io::stderr());
        let deadline = Instant::now() + pass_interval(backlog_pending(directory, &config));
        while !stopping(&stop.cancelled) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    save_json(
        &directory.join("runtime.json"),
        &json!({"startup_id":startup_id,"pid":std::process::id(),"ready":false}),
    )?;
    Ok(())
}
/// Whether the active job still has work a further pass can move: batches
/// queued for delivery, journal changes not yet queued, or an unfinished
/// historical snapshot. A paused job has nothing to move, whatever it holds,
/// and neither has one waiting out a retry deadline: the next pass before that
/// deadline does the same round of local work and reaches the same idle drain.
fn backlog_pending(directory: &Path, config: &Config) -> bool {
    let status = ai_hist::open_db_readonly(&directory.join("history.db"))
        .and_then(|conn| delivery::status(&conn, &config.job_id));
    status.is_ok_and(|status| {
        status.state == "active"
            && status.next_attempt_ms <= now()
            && (status.pending_records > 0
                || status.unqueued_changes > 0
                || !status.bootstrap_complete)
    })
}
/// The gap between two delivery passes: short while there is backlog, so
/// throughput follows capture, and long once caught up.
fn pass_interval(backlog: bool) -> Duration {
    if backlog {
        BACKLOG_PASS_INTERVAL
    } else {
        IDLE_PASS_INTERVAL
    }
}
pub fn start_background(directory: &Path) -> Result<()> {
    let executable = std::env::current_exe()?;
    let startup_id = format!("{}-{}", std::process::id(), now());
    let log_path = directory.join("collector.log");
    let mut log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        log.set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    writeln!(log, "Starting Agent Relay Probe")?;
    let mut command = Command::new(executable);
    command
        .arg("run")
        .arg("--directory")
        .arg(directory)
        .arg("--startup-id")
        .arg(&startup_id)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log));
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        unsafe {
            command.pre_exec(|| {
                // Only the async-signal-safe setsid syscall runs between fork/exec.
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }
    let mut child = command.spawn()?;
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if child.try_wait()?.is_some() {
            return Err(user_error(
                "Probe stopped during startup. Run setup in --foreground to retry.",
            ));
        }
        let ready = fs::read(directory.join("runtime.json"))
            .ok()
            .and_then(|data| serde_json::from_slice::<serde_json::Value>(&data).ok())
            .is_some_and(|value| value["startup_id"] == startup_id && value["ready"] == true);
        if ready {
            humanln!("Probe is running in the background. You can close this terminal.");
            humanln!("Run setup again after restarting the computer.");
            let config = read_config(directory)?;
            humanln!(
                "Stop: agent-relay-probe stop --site-url {} --workspace {} --account {}",
                config.site_url,
                config.workspace_id,
                config.account_id
            );
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    if child.try_wait()?.is_none() {
        child.kill()?;
        child.wait()?;
    }
    Err(user_error("Probe startup timed out. Run setup again."))
}
#[cfg(test)]
mod tests {
    use super::*;
    use fs2::FileExt;
    #[test]
    fn selected_once_setup_hydrates_only_members_even_when_delivery_is_unavailable() {
        // Hydration validates source paths against configured provider roots.
        // Run alone in a subprocess so fixture roots cannot race other tests.
        const CHILD: &str = "RELAYHISTORY_TEST_SELECTED_ONCE_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let output = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "collector::tests::selected_once_setup_hydrates_only_members_even_when_delivery_is_unavailable", "--nocapture"])
                .env(CHILD, "1")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }
        let home = tempfile::tempdir().unwrap();
        std::env::set_var("HOME", home.path());
        std::env::set_var("USERPROFILE", home.path());
        std::env::set_var("CLAUDE_CONFIG_DIR", home.path().join(".claude"));
        let project = home.path().join(".claude/projects/synthetic");
        fs::create_dir_all(&project).unwrap();
        for session in ["selected", "private"] {
            let record = json!({"sessionId":session,"uuid":format!("u-{session}"),"type":"user","message":{"role":"user","content":"synthetic"},"timestamp":"2026-09-01T01:00:00Z"});
            fs::write(
                project.join(format!("{session}.jsonl")),
                format!("{record}\n"),
            )
            .unwrap();
        }
        let conn = relayhistory_plugin::delivery::open_db(&home.path().join("history.db")).unwrap();
        let env = ai_hist::DiscoveryEnv::with_roots(
            &conn,
            home.path().to_path_buf(),
            home.path().join("opencode.db"),
        );
        ai_hist::discover_sessions_with_env(&env, &inventory_options(), |_| {}).unwrap();
        let job = delivery::create_session_job(&conn, &job_config(false), now()).unwrap();
        delivery::set_job_session(
            &conn,
            &job.job_id,
            &delivery::SessionIdentity {
                source: "claude".into(),
                session_id: "selected".into(),
            },
            true,
        )
        .unwrap();
        let config = Config {
            version: 1,
            site_url: "https://synthetic.invalid".into(),
            account_id: "account".into(),
            account_email: None,
            account_name: None,
            account_avatar_url: None,
            org_id: "org".into(),
            workspace_id: "workspace".into(),
            // Invalid URL fails before any credentials or transport can be used.
            history_url: "invalid synthetic URL".into(),
            delivery_account: job.config.account_id,
            job_id: job.job_id,
            include_existing: false,
            sharing_mode: Some(super::super::bridge::SharingMode::Selected),
            acknowledge_uninspected_schedules: false,
        };
        save_json(&home.path().join("config.json"), &config).unwrap();
        let events = || {
            conn.query_row("SELECT COUNT(*) FROM session_events", [], |r| {
                r.get::<_, usize>(0)
            })
            .unwrap()
        };
        assert_eq!(events(), 0);
        // Background setup stays shallow; the collector will hydrate later.
        assert!(finish_setup(home.path(), &config, false).is_err());
        assert_eq!(events(), 0);
        delivery::pause_job(&conn, &config.job_id).unwrap();
        finish_setup(home.path(), &config, true).unwrap();
        assert_eq!(events(), 0, "paused selected setup must not hydrate");
        delivery::resume_job(&conn, &config.job_id).unwrap();
        assert!(finish_setup(home.path(), &config, true).is_err());
        assert_eq!(
            events(),
            1,
            "selected --once must capture before returning the delivery error"
        );
        let session: String = conn
            .query_row("SELECT session_id FROM session_events", [], |r| r.get(0))
            .unwrap();
        assert_eq!(session, "selected");
        let diagnostic: serde_json::Value =
            serde_json::from_slice(&fs::read(home.path().join("capture-diagnostic.json")).unwrap())
                .unwrap();
        assert_eq!(diagnostic["sessions_captured"], 1);
        assert_eq!(diagnostic["failures"], 0);
    }

    #[test]
    fn selected_inventory_discovers_sessions_older_than_the_first_thousand() {
        let home = tempfile::tempdir().unwrap();
        let project = home.path().join(".claude/projects/synthetic");
        fs::create_dir_all(&project).unwrap();
        for n in 0..1001 {
            let path = project.join(format!("session-{n:04}.jsonl"));
            let record = json!({"sessionId":format!("session-{n:04}"),"uuid":format!("u-{n}"),"type":"user","message":{"role":"user","content":"synthetic"},"timestamp":"2026-09-01T01:00:00Z"});
            fs::write(&path, format!("{record}\n")).unwrap();
            fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .unwrap()
                .set_times(
                    fs::FileTimes::new().set_modified(
                        std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(n + 1),
                    ),
                )
                .unwrap();
        }
        let conn = relayhistory_plugin::delivery::open_db(&home.path().join("history.db")).unwrap();
        let job = delivery::create_session_job(&conn, &job_config(true), now()).unwrap();
        let env = ai_hist::DiscoveryEnv::with_roots(
            &conn,
            home.path().to_path_buf(),
            home.path().join("opencode.db"),
        );
        for _ in 0..2 {
            let mut found = std::collections::HashSet::new();
            ai_hist::discover_sessions_with_env(&env, &inventory_options(), |row| {
                found.insert(row.session_id.clone());
            })
            .unwrap();
            assert_eq!(
                found.len(),
                1001,
                "periodic inventory must not repeatedly cap the same newest sessions"
            );
            assert!(found.contains("session-0000"));
        }
        let oldest_known: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM sessions WHERE source='claude' AND session_id='session-0000')", [], |r| r.get(0)).unwrap();
        assert!(oldest_known);
        assert!(delivery::job_sessions(&conn, &job.job_id)
            .unwrap()
            .is_empty());
        assert!(delivery::prepare_batch(&conn, &job.job_id, now())
            .unwrap()
            .batch_id
            .is_none());
    }

    #[test]
    fn fake_receiver_drains_selected_backlog_despite_failing_capture_and_private_discovery() {
        struct Fake(std::cell::RefCell<Vec<String>>);
        impl worker::Receiver for Fake {
            fn mapping_version(&self) -> &str {
                destination::MAPPING_VERSION
            }
            fn supported_kinds(&self) -> &[&str] {
                &["history", "session_event", "session"]
            }
            fn supports_tombstones(&self) -> bool {
                true
            }
            fn prepare(
                &self,
                batch: &delivery::HistoryExportBatch,
                _: &worker::ReceiverContext<'_>,
            ) -> std::result::Result<worker::PreparedBody, worker::ReceiverFailure> {
                Ok(worker::PreparedBody {
                    content_type: "application/json".into(),
                    body: serde_json::to_string(batch).unwrap(),
                })
            }
            fn send(
                &self,
                _: &delivery::PreparedPayload,
                batch: &delivery::HistoryExportBatch,
                _: &worker::ReceiverContext<'_>,
            ) -> std::result::Result<delivery::DeliveryAcknowledgment, worker::ReceiverFailure>
            {
                self.0
                    .borrow_mut()
                    .extend(batch.records.iter().map(|r| r.session_id.clone().unwrap()));
                Ok(delivery::DeliveryAcknowledgment {
                    batch_id: batch.batch_id.clone(),
                    accepted_revision_ids: batch
                        .records
                        .iter()
                        .map(|r| r.revision_id.clone())
                        .collect(),
                    unsupported_revision_ids: vec![],
                    acceptance_level: delivery::AcceptanceLevel::Durable,
                })
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.db");
        let conn = relayhistory_plugin::delivery::open_db(&path).unwrap();
        conn.execute(
            "INSERT INTO sessions(source,session_id) VALUES ('claude','selected')",
            [],
        )
        .unwrap();
        for n in 0..20 {
            conn.execute("INSERT INTO session_events(source,session_id,event_uid,ts_ms,role,kind,text) VALUES('claude','selected',?,1,'user','text','synthetic')",[n.to_string()]).unwrap();
        }
        let mut jc = job_config(true);
        jc.limits.max_batch_records = 1;
        let job = delivery::create_session_job(&conn, &jc, now()).unwrap();
        delivery::set_job_session(
            &conn,
            &job.job_id,
            &delivery::SessionIdentity {
                source: "claude".into(),
                session_id: "selected".into(),
            },
            true,
        )
        .unwrap();
        // A newly discovered catalog row is visible, but never authorized.
        conn.execute(
            "INSERT INTO sessions(source,session_id) VALUES ('codex','new-private')",
            [],
        )
        .unwrap();
        let config = Config {
            version: 1,
            site_url: "https://example.invalid".into(),
            account_id: "account".into(),
            account_email: None,
            account_name: None,
            account_avatar_url: None,
            org_id: "org".into(),
            workspace_id: "workspace".into(),
            history_url: "https://example.invalid".into(),
            delivery_account: jc.account_id,
            job_id: job.job_id,
            include_existing: false,
            sharing_mode: Some(super::super::bridge::SharingMode::Selected),
            acknowledge_uninspected_schedules: false,
        };
        let fake = Fake(std::cell::RefCell::new(vec![]));
        let capture_calls = std::cell::Cell::new(0);
        for _ in 0..4 {
            let _ = run_capture_cycle(
                true,
                || deliver_with_receiver(&path, &config, &fake, &|| false).map(|_| ()),
                || {
                    let s = delivery::status(&conn, &config.job_id)?;
                    Ok(s.bootstrap_complete && s.pending_records == 0 && s.unqueued_changes == 0)
                },
                || {
                    capture_calls.set(capture_calls.get() + 1);
                    Err(anyhow::anyhow!("synthetic missing selected source"))
                },
                || panic!("unrelated slow/failing provider called"),
                || false,
            );
        }
        assert_eq!(fake.0.borrow().len(), 21);
        assert!(fake.0.borrow().iter().all(|id| id == "selected"));
        assert!(capture_calls.get() > 0);
        assert_eq!(
            delivery::status(&conn, &config.job_id)
                .unwrap()
                .acknowledged_records,
            21
        );
    }
    #[test]
    fn delivery_errors_do_not_prevent_selected_or_broad_capture() {
        for selected in [true, false] {
            let calls = std::cell::RefCell::new(Vec::new());
            let error = run_capture_cycle(
                selected,
                || {
                    calls.borrow_mut().push("deliver");
                    Err(anyhow::anyhow!("synthetic credential failure"))
                },
                || panic!("delivery failure must not suppress capture behind backlog"),
                || {
                    calls.borrow_mut().push("targeted");
                    Ok(())
                },
                || {
                    calls.borrow_mut().push("broad");
                    Ok(())
                },
                || false,
            )
            .result()
            .unwrap_err();
            assert_eq!(
                calls.into_inner(),
                vec![
                    "deliver",
                    if selected { "targeted" } else { "broad" },
                    "deliver"
                ]
            );
            assert_eq!(error.to_string(), "synthetic credential failure");
        }
    }

    #[test]
    fn selected_retry_backoff_does_not_starve_local_capture() {
        let conn = Connection::open_in_memory().unwrap();
        relayhistory_plugin::delivery::init_db(&conn).unwrap();
        conn.execute(
            "INSERT INTO sessions(source,session_id) VALUES ('claude','selected')",
            [],
        )
        .unwrap();
        let job = delivery::create_session_job(&conn, &job_config(true), now()).unwrap();
        delivery::set_job_session(
            &conn,
            &job.job_id,
            &delivery::SessionIdentity {
                source: "claude".into(),
                session_id: "selected".into(),
            },
            true,
        )
        .unwrap();
        delivery::prepare_batch(&conn, &job.job_id, now()).unwrap();
        assert!(
            delivery::status(&conn, &job.job_id)
                .unwrap()
                .pending_records
                > 0
        );
        let captures = std::cell::Cell::new(0);
        for waiting in [false, true, false] {
            conn.execute(
                "UPDATE delivery_jobs SET next_attempt_ms=? WHERE id=?",
                rusqlite::params![if waiting { now() + 60_000 } else { 0 }, job.job_id],
            )
            .unwrap();
            run_capture_cycle(
                true,
                || Ok(()),
                || ready_to_capture(&conn, &job.job_id),
                || {
                    captures.set(captures.get() + 1);
                    Ok(())
                },
                || panic!("selected mode must not use broad capture"),
                || false,
            )
            .result()
            .unwrap();
        }
        assert_eq!(
            captures.get(),
            1,
            "capture runs during backoff, but an eligible backlog retains priority"
        );
    }

    #[test]
    fn selected_backlog_does_not_enter_unrelated_capture() {
        use std::cell::Cell;
        let delivered = Cell::new(0);
        for _ in 0..3 {
            run_capture_cycle(
                true,
                || {
                    delivered.set(delivered.get() + 1);
                    Ok(())
                },
                || Ok(false),
                || panic!("backlog must drain first"),
                || panic!("blocked unrelated provider entered"),
                || false,
            )
            .result()
            .unwrap();
        }
        assert_eq!(delivered.get(), 3);
        run_capture_cycle(
            true,
            || {
                delivered.set(delivered.get() + 1);
                Ok(())
            },
            || Ok(true),
            || Err(anyhow::anyhow!("synthetic selected capture failure")),
            || panic!("failing unrelated provider entered"),
            || false,
        )
        .result()
        .unwrap_err();
        assert_eq!(delivered.get(), 5);
    }

    #[test]
    fn capture_diagnostics_never_include_raw_errors_or_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let error = anyhow::Error::new(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "secret prompt /private/source https://token:secret@example.invalid",
        ));
        capture_diagnostic(
            dir.path(),
            "targeted_hydration",
            Instant::now(),
            2,
            1,
            Some(&error),
        )
        .unwrap();
        let value = std::fs::read_to_string(dir.path().join("capture-diagnostic.json")).unwrap();
        assert!(value.contains("permission_denied"));
        for forbidden in ["secret", "prompt", "private", "https", "example"] {
            assert!(!value.contains(forbidden));
        }
    }

    fn members(ids: &[&str]) -> Vec<delivery::SessionIdentity> {
        ids.iter()
            .map(|id| delivery::SessionIdentity {
                source: "codex".into(),
                session_id: (*id).into(),
            })
            .collect()
    }

    /// Verify failed members retain their typed cause while healthy members are captured.
    #[test]
    fn targeted_storage_failure_keeps_its_class_and_other_members_still_capture() {
        let dir = tempfile::tempdir().unwrap();
        let captured = std::cell::Cell::new(0);
        let result = capture_members(
            dir.path(),
            members(&["corrupt", "healthy"]),
            || false,
            |member| {
                if member.session_id == "corrupt" {
                    Err(anyhow::Error::new(rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CORRUPT),
                        Some("secret transcript".into()),
                    )))
                } else {
                    captured.set(captured.get() + 1);
                    Ok(())
                }
            },
        );
        assert_eq!(captured.get(), 1);
        let report = cycle_report(&Verdicts::capture(&result), None);
        assert_eq!(report["error_class"], "database_corrupt");
        assert!(!report.to_string().contains("secret"));
        assert!(!report["message"].as_str().unwrap().contains("offline"));
    }

    /// A retention stop after an ordinary member failure is still what the
    /// cycle reports: it ended the pass and carries the budget.
    #[test]
    fn a_retention_stop_outranks_an_earlier_members_failure() {
        let dir = tempfile::tempdir().unwrap();
        let attempted = std::cell::Cell::new(0);
        let result = capture_members(
            dir.path(),
            members(&["corrupt", "full", "never"]),
            || false,
            |member| {
                attempted.set(attempted.get() + 1);
                if member.session_id == "corrupt" {
                    return Err(anyhow::Error::new(rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CORRUPT),
                        Some("secret transcript".into()),
                    )));
                }
                Err(anyhow::Error::new(delivery::RetentionLimitReached {
                    used_bytes: 9_000,
                    limit_bytes: 10_000,
                }))
            },
        );
        assert_eq!(attempted.get(), 2, "the member after the stop is not tried");
        let outcome = PassOutcome {
            capture: Some(result),
            delivery: Ok(()),
        };
        persist_cycle_report(dir.path(), &outcome, now(), Vec::new());
        let report: serde_json::Value =
            serde_json::from_slice(&fs::read(dir.path().join("cycle.json")).unwrap()).unwrap();
        assert_eq!(report["error_class"], "retention_limit");
        assert_eq!(report["used_bytes"], 9_000);
        assert_eq!(report["limit_bytes"], 10_000);
        assert!(!report.to_string().contains("secret"));
    }

    /// A member that stops at the retention cap ends the pass after at most
    /// one attempt; the report carries the budget the pass stopped at.
    #[test]
    fn targeted_retention_stop_ends_the_pass_and_reports_the_budget() {
        let dir = tempfile::tempdir().unwrap();
        let attempted = std::cell::Cell::new(0);
        let result = capture_members(
            dir.path(),
            members(&["full", "next", "another"]),
            || false,
            |_| {
                attempted.set(attempted.get() + 1);
                Err(anyhow::Error::new(delivery::RetentionLimitReached {
                    used_bytes: 9_000,
                    limit_bytes: 10_000,
                })
                .context("secret transcript"))
            },
        );
        assert_eq!(attempted.get(), 1);
        let outcome = PassOutcome {
            capture: Some(result),
            delivery: Ok(()),
        };
        persist_cycle_report(dir.path(), &outcome, now(), Vec::new());
        let report: serde_json::Value =
            serde_json::from_slice(&fs::read(dir.path().join("cycle.json")).unwrap()).unwrap();
        assert_eq!(report["error_class"], "retention_limit");
        assert_eq!(report["used_bytes"], 9_000);
        assert_eq!(report["limit_bytes"], 10_000);
        assert!(!report.to_string().contains("secret"));
        assert!(!report["message"].as_str().unwrap().contains("offline"));
        let diagnostic: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(dir.path().join("capture-diagnostic.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(diagnostic["error_class"], "retention_limit");
        assert_eq!(diagnostic["used_bytes"], 9_000);
        assert_eq!(diagnostic["limit_bytes"], 10_000);
        assert_eq!(diagnostic["sessions_captured"], 0);
    }

    /// A retention report names the budget the pass stopped at when the
    /// failure carries it, and the current budget for a capture trigger abort
    /// that reached the collector without one. Other failures carry nothing.
    #[test]
    fn retention_usage_comes_from_the_failure_before_the_database() {
        let dir = tempfile::tempdir().unwrap();
        let conn = relayhistory_plugin::delivery::open_db(&dir.path().join("history.db")).unwrap();
        delivery::set_retention_limit(&conn, 10 * 1_048_576).unwrap();
        let current = delivery::retained_bytes(&conn).unwrap();
        drop(conn);
        let trigger_abort = anyhow::Error::new(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CONSTRAINT_TRIGGER),
            Some("delivery retention limit exceeded; secret transcript".into()),
        ));
        assert_eq!(retention_for(dir.path(), &trigger_abort), Some(current));
        let stopped = anyhow::Error::new(delivery::RetentionLimitReached {
            used_bytes: 9_000,
            limit_bytes: 10_000,
        })
        .context("codex history delivery capture stopped at the retention cap");
        assert_eq!(retention_for(dir.path(), &stopped), Some((9_000, 10_000)));
        let corrupt = anyhow::Error::new(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CORRUPT),
            None,
        ));
        assert_eq!(retention_for(dir.path(), &corrupt), None);
    }

    /// Verify SQLite failure guidance and cycle reports never reveal raw sensitive details.
    #[test]
    fn cycle_errors_classify_local_storage_without_exposing_raw_details() {
        for (code, class) in [
            (rusqlite::ffi::SQLITE_CORRUPT, "database_corrupt"),
            (rusqlite::ffi::SQLITE_FULL, "disk_full"),
            (rusqlite::ffi::SQLITE_BUSY, "database_busy"),
            (rusqlite::ffi::SQLITE_READONLY, "permission_denied"),
            (
                rusqlite::ffi::SQLITE_READONLY_DIRECTORY,
                "permission_denied",
            ),
            (rusqlite::ffi::SQLITE_PERM, "permission_denied"),
            (rusqlite::ffi::SQLITE_CANTOPEN, "database_unavailable"),
        ] {
            let error = anyhow::Error::new(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(code),
                Some("secret prompt https://token:secret@example.invalid".into()),
            ));
            let report = cycle_report(&Verdicts::capture(&Err(error)), None);
            assert_eq!(report["error_class"], class);
            assert_eq!(report["ok"], false);
            assert!(!report["message"].as_str().unwrap().contains("offline"));
            for forbidden in ["secret", "prompt", "https", "example"] {
                assert!(!report.to_string().contains(forbidden));
            }
        }
        let error = anyhow::Error::new(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CONSTRAINT_TRIGGER),
            Some("delivery retention limit exceeded; secret".into()),
        ));
        assert_eq!(
            cycle_report(&Verdicts::capture(&Err(error)), None)["error_class"],
            "retention_limit"
        );
        let healthy = cycle_report(&Verdicts::default(), None);
        assert_eq!(healthy["ok"], true);
        assert!(healthy["message"].is_null());
        assert!(healthy["error_class"].is_null());
    }

    /// Verify contextual StorageFull and native ENOSPC errors retain safe disk guidance.
    #[test]
    fn filesystem_disk_full_uses_safe_local_guidance_through_context() {
        let errors = vec![std::io::Error::new(
            std::io::ErrorKind::StorageFull,
            "secret prompt /private/token",
        )];
        #[cfg(unix)]
        let errors = {
            let mut errors = errors;
            errors.push(std::io::Error::from_raw_os_error(libc::ENOSPC));
            errors
        };
        for error in errors {
            let error = anyhow::Error::new(error).context("writing secret runtime.json");
            assert!(local_failure_message(&error)
                .unwrap()
                .contains("Free disk space"));
            let report = cycle_report(&Verdicts::capture(&Err(error)), None);
            assert_eq!(report["error_class"], "disk_full");
            assert_eq!(report["ok"], false);
            assert!(!report.to_string().contains("secret"));
            assert!(!report.to_string().contains("offline"));
        }
    }

    /// A full log disk cannot make advisory cycle reporting terminate the collector.
    #[test]
    fn cycle_reporting_tolerates_failed_status_and_log_writes() {
        struct FullDisk;
        impl Write for FullDisk {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::ErrorKind::StorageFull.into())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cycle.json");
        fs::create_dir(&path).unwrap();
        let result = Err(anyhow::Error::new(std::io::Error::new(
            std::io::ErrorKind::StorageFull,
            "secret runtime path",
        )));
        write_cycle_report(
            directory.path(),
            &Verdicts::capture(&result),
            None,
            FullDisk,
        );
        let mut output = Vec::new();
        write_cycle_report(
            directory.path(),
            &Verdicts::capture(&result),
            None,
            &mut output,
        );
        let output = String::from_utf8(output).unwrap();
        assert!(output.contains("Sync status could not be saved"));
        assert!(output.contains("Free disk space"));
        assert!(!output.contains("secret"));
        fs::remove_dir(&path).unwrap();
        write_cycle_report(directory.path(), &Verdicts::default(), None, FullDisk);
        let saved: serde_json::Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        assert_eq!(saved["ok"], true);
    }

    /// A pass stopped over the high-water mark reports the journal as nearly
    /// full with the figures it stopped at; one at the cap reports it full.
    #[test]
    fn a_high_water_stop_reports_the_journal_as_nearly_full() {
        let stopped = anyhow::Error::new(delivery::RetentionLimitReached {
            used_bytes: 231 * 1_048_576,
            limit_bytes: 256 * 1_048_576,
        });
        let report = cycle_report(
            &Verdicts::capture(&Err(stopped)),
            Some((231 * 1_048_576, 256 * 1_048_576)),
        );
        assert_eq!(report["error_class"], "retention_limit");
        assert_eq!(
            report["message"],
            "Upload journal nearly full (231 MB of 256 MB); capture is waiting for room. Compacting consumed records; queued sessions are preserved."
        );
        assert_eq!(report["used_bytes"], 231 * 1_048_576);
        assert_eq!(report["limit_bytes"], 256 * 1_048_576);
    }

    /// The retention sentence carries the measured usage when the database is
    /// readable and still names the condition when it is not.
    #[test]
    fn retention_limit_report_carries_usage_in_megabytes() {
        let error = anyhow::Error::new(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CONSTRAINT_TRIGGER),
            Some("delivery retention limit exceeded; secret transcript".into()),
        ));
        let report = cycle_report(
            &Verdicts::capture(&Err(error)),
            Some((268_433_716, 268_435_456)),
        );
        assert_eq!(report["error_class"], "retention_limit");
        assert_eq!(
            report["message"],
            "Upload journal full (256 MB of 256 MB). Compacting consumed records; queued sessions are preserved."
        );
        assert!(!report.to_string().contains("secret"));

        let directory = tempfile::tempdir().unwrap();
        let conn =
            relayhistory_plugin::delivery::open_db(&directory.path().join("history.db")).unwrap();
        delivery::set_retention_limit(&conn, 10 * 1_048_576).unwrap();
        drop(conn);
        let result = Err(anyhow::Error::new(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CONSTRAINT_TRIGGER),
            Some("delivery retention limit exceeded; secret".into()),
        )));
        let mut output = Vec::new();
        write_cycle_report(
            directory.path(),
            &Verdicts::capture(&result),
            None,
            &mut output,
        );
        let saved: serde_json::Value =
            serde_json::from_slice(&fs::read(directory.path().join("cycle.json")).unwrap())
                .unwrap();
        assert_eq!(
            saved["message"],
            "Upload journal nearly full (0 MB of 10 MB); capture is waiting for room. Compacting consumed records; queued sessions are preserved."
        );
        assert!(String::from_utf8(output).unwrap().contains("0 MB of 10 MB"));

        let unreadable = tempfile::tempdir().unwrap();
        fs::create_dir(unreadable.path().join("history.db")).unwrap();
        write_cycle_report(
            unreadable.path(),
            &Verdicts::capture(&result),
            None,
            Vec::new(),
        );
        let saved: serde_json::Value =
            serde_json::from_slice(&fs::read(unreadable.path().join("cycle.json")).unwrap())
                .unwrap();
        assert_eq!(saved["message"], RETENTION_LIMIT_MESSAGE);
    }

    /// Passes follow the backlog: two seconds apart while the active job has
    /// queued or unqueued work, twenty once caught up or paused.
    #[test]
    fn pass_interval_follows_the_active_jobs_backlog() {
        let dir = tempfile::tempdir().unwrap();
        let conn = relayhistory_plugin::delivery::open_db(&dir.path().join("history.db")).unwrap();
        let job = delivery::create_job(&conn, &job_config(true), now()).unwrap();
        let config = Config {
            version: 1,
            site_url: "https://agentrelay.com".into(),
            account_id: "account".into(),
            account_email: None,
            account_name: None,
            account_avatar_url: None,
            org_id: "org".into(),
            workspace_id: "workspace".into(),
            history_url: "https://history.agentrelay.com".into(),
            delivery_account: job.config.account_id,
            job_id: job.job_id,
            include_existing: true,
            sharing_mode: None,
            acknowledge_uninspected_schedules: false,
        };
        // An empty database still owes one bootstrap scan before it is caught up.
        assert!(backlog_pending(dir.path(), &config));
        let prepared = delivery::prepare_batch(&conn, &config.job_id, now()).unwrap();
        assert!(prepared.bootstrap_complete && prepared.batch_id.is_none());
        assert!(!backlog_pending(dir.path(), &config));
        assert_eq!(pass_interval(false), Duration::from_secs(20));
        conn.execute(
            "INSERT INTO sessions(source,session_id) VALUES ('claude','queued')",
            [],
        )
        .unwrap();
        assert!(backlog_pending(dir.path(), &config));
        assert_eq!(pass_interval(true), Duration::from_secs(2));
        // A job in receiver backoff cannot attempt anything yet: passes two
        // seconds apart would repeat the same local work for an idle drain.
        conn.execute(
            "UPDATE delivery_jobs SET next_attempt_ms=? WHERE id=?",
            rusqlite::params![now() + 60_000, config.job_id],
        )
        .unwrap();
        assert!(!backlog_pending(dir.path(), &config));
        conn.execute(
            "UPDATE delivery_jobs SET next_attempt_ms=0 WHERE id=?",
            [&config.job_id],
        )
        .unwrap();
        assert!(backlog_pending(dir.path(), &config));
        delivery::pause_job(&conn, &config.job_id).unwrap();
        assert!(!backlog_pending(dir.path(), &config));
        assert!(!backlog_pending(
            tempfile::tempdir().unwrap().path(),
            &config
        ));
    }

    fn retention_error() -> anyhow::Error {
        anyhow::Error::new(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CONSTRAINT_TRIGGER),
            Some("delivery retention limit exceeded; secret".into()),
        ))
    }
    fn saved_cycle(directory: &Path) -> serde_json::Value {
        serde_json::from_slice(&fs::read(directory.join("cycle.json")).unwrap()).unwrap()
    }
    fn capped_directory(limit_bytes: i64) -> tempfile::TempDir {
        let directory = tempfile::tempdir().unwrap();
        let conn =
            relayhistory_plugin::delivery::open_db(&directory.path().join("history.db")).unwrap();
        delivery::set_retention_limit(&conn, limit_bytes).unwrap();
        directory
    }

    /// A retention report carries the journal's current reading, not the
    /// usage the stopped pass measured before compacting.
    #[test]
    fn a_retention_report_carries_the_current_reading_over_the_stopped_usage() {
        let directory = capped_directory(10 * 1_048_576);
        let current = read_retention(directory.path()).unwrap();
        let stopped = PassOutcome {
            capture: Some(Err(anyhow::Error::new(delivery::RetentionLimitReached {
                used_bytes: 9_500_000,
                limit_bytes: 10 * 1_048_576,
            }))),
            delivery: Ok(()),
        };
        persist_cycle_report(directory.path(), &stopped, now(), Vec::new());
        let report = saved_cycle(directory.path());
        assert_eq!(report["error_class"], "retention_limit");
        assert_eq!(report["used_bytes"], current.0);
        assert_eq!(report["limit_bytes"], current.1);
        assert!(report["message"]
            .as_str()
            .unwrap()
            .contains("(0 MB of 10 MB)"));
    }

    /// Capture and delivery are observed at different cadences: a pass that
    /// only delivered leaves the capture verdict, and the desktop banner it
    /// drives, exactly where the capture cycle put it.
    #[test]
    fn a_delivery_only_pass_keeps_the_last_capture_verdict() {
        let directory = capped_directory(10 * 1_048_576);
        let full = PassOutcome {
            capture: Some(Err(retention_error())),
            delivery: Ok(()),
        };
        persist_cycle_report(directory.path(), &full, now(), Vec::new());
        assert_eq!(
            saved_cycle(directory.path())["error_class"],
            "retention_limit"
        );

        for _ in 0..20 {
            persist_cycle_report(
                directory.path(),
                &PassOutcome::delivery_only(Ok(())),
                now(),
                Vec::new(),
            );
        }
        let report = saved_cycle(directory.path());
        assert_eq!(report["ok"], false);
        assert_eq!(report["error_class"], "retention_limit");
        assert_eq!(
            report["message"],
            "Upload journal nearly full (0 MB of 10 MB); capture is waiting for room. Compacting consumed records; queued sessions are preserved."
        );
        assert_eq!(report["capture"]["error_class"], "retention_limit");
        assert_eq!(report["delivery"]["ok"], true);
        assert!(report["delivery"]["message"].is_null());

        // A capture pass is what rewrites the capture verdict.
        persist_cycle_report(
            directory.path(),
            &PassOutcome {
                capture: Some(Ok(())),
                delivery: Ok(()),
            },
            now(),
            Vec::new(),
        );
        let report = saved_cycle(directory.path());
        assert_eq!(report["ok"], true);
        assert!(report["error_class"].is_null());
        assert!(report["message"].is_null());
    }

    /// A delivery fault of its own is reported while the capture verdict is
    /// healthy, and never outranks a capture fault the user can act on.
    #[test]
    fn a_delivery_fault_reports_under_a_healthy_capture_verdict() {
        let directory = capped_directory(10 * 1_048_576);
        persist_cycle_report(
            directory.path(),
            &PassOutcome {
                capture: Some(Ok(())),
                delivery: Err(anyhow::anyhow!("synthetic transport failure")),
            },
            now(),
            Vec::new(),
        );
        let report = saved_cycle(directory.path());
        assert_eq!(report["ok"], false);
        assert_eq!(report["error_class"], "capture_error");
        assert_eq!(
            report["message"],
            "Sync paused or offline. Retrying; local data remains queued."
        );
        assert!(!report.to_string().contains("synthetic"));

        persist_cycle_report(
            directory.path(),
            &PassOutcome {
                capture: Some(Err(retention_error())),
                delivery: Err(anyhow::anyhow!("synthetic transport failure")),
            },
            now(),
            Vec::new(),
        );
        let report = saved_cycle(directory.path());
        assert_eq!(report["error_class"], "retention_limit");
        assert_eq!(report["delivery"]["error_class"], "capture_error");
    }

    /// Compaction re-measures the verdict it just changed, so a resolved cap
    /// stops being reported before the next capture cycle measures it again.
    #[test]
    fn compaction_refreshes_the_retention_verdict_it_resolved() {
        let directory = capped_directory(10 * 1_048_576);
        // Without a persisted verdict there is nothing to refresh.
        refresh_retention_verdict(directory.path(), 4_096, Vec::new());
        assert!(!directory.path().join("cycle.json").exists());

        persist_cycle_report(
            directory.path(),
            &PassOutcome {
                capture: Some(Err(retention_error())),
                delivery: Err(retention_error()),
            },
            now(),
            Vec::new(),
        );
        // A pass that reclaimed nothing leaves the verdict standing: the cap
        // is held by records compaction cannot touch.
        refresh_retention_verdict(directory.path(), 0, Vec::new());
        assert_eq!(
            saved_cycle(directory.path())["error_class"],
            "retention_limit"
        );
        refresh_retention_verdict(directory.path(), 4_096, Vec::new());
        let report = saved_cycle(directory.path());
        assert_eq!(report["ok"], true);
        assert!(report["capture"]["error_class"].is_null());
        assert!(report["delivery"]["error_class"].is_null());

        // A journal still at its cap keeps the verdict and reports the reading.
        let directory = tempfile::tempdir().unwrap();
        let conn =
            relayhistory_plugin::delivery::open_db(&directory.path().join("history.db")).unwrap();
        delivery::create_job(&conn, &job_config(true), now()).unwrap();
        delivery::set_retention_limit(&conn, delivery::retained_bytes(&conn).unwrap().0.max(1))
            .unwrap();
        persist_cycle_report(
            directory.path(),
            &PassOutcome {
                capture: Some(Err(retention_error())),
                delivery: Ok(()),
            },
            now(),
            Vec::new(),
        );
        refresh_retention_verdict(directory.path(), 4_096, Vec::new());
        assert_eq!(
            saved_cycle(directory.path())["error_class"],
            "retention_limit"
        );
    }

    /// Two writers update one report, so each one's read-modify-write waits
    /// for the other: neither replaces an answer it never read.
    #[test]
    fn a_cycle_report_waits_for_the_other_writer() {
        let directory = capped_directory(10 * 1_048_576);
        let guard = cycle_guard(directory.path()).expect("the guard is available");
        let path = directory.path().to_owned();
        let writing = std::thread::spawn(move || {
            persist_cycle_report(
                &path,
                &PassOutcome::delivery_only(Ok(())),
                now(),
                Vec::new(),
            );
        });
        std::thread::sleep(Duration::from_millis(50));
        assert!(
            !directory.path().join("cycle.json").exists(),
            "a report was written while another writer held the guard"
        );
        drop(guard);
        writing.join().unwrap();
        assert_eq!(saved_cycle(directory.path())["ok"], true);
    }

    /// A pass reports what it observed, and a compaction that reported while
    /// it ran has since re-measured the same cap: the banner the user cleared
    /// does not come back for a pass interval.
    #[test]
    fn a_compaction_during_a_pass_outranks_the_reading_it_invalidated() {
        let directory = capped_directory(10 * 1_048_576);
        persist_cycle_report(
            directory.path(),
            &PassOutcome {
                capture: Some(Err(retention_error())),
                delivery: Ok(()),
            },
            now(),
            Vec::new(),
        );
        // A pass starts, observes the cap, and a compaction resolves it while
        // that pass is still running.
        let pass_started = now();
        std::thread::sleep(Duration::from_millis(2));
        refresh_retention_verdict(directory.path(), 4_096, Vec::new());
        assert_eq!(saved_cycle(directory.path())["ok"], true);

        persist_cycle_report(
            directory.path(),
            &PassOutcome {
                capture: Some(Err(retention_error())),
                delivery: Err(retention_error()),
            },
            pass_started,
            Vec::new(),
        );
        let report = saved_cycle(directory.path());
        assert_eq!(report["ok"], true);
        assert!(report["capture"]["error_class"].is_null());
        assert!(report["delivery"]["error_class"].is_null());

        // A pass that starts after the compaction reports what it measured.
        persist_cycle_report(
            directory.path(),
            &PassOutcome {
                capture: Some(Err(retention_error())),
                delivery: Ok(()),
            },
            now(),
            Vec::new(),
        );
        assert_eq!(
            saved_cycle(directory.path())["error_class"],
            "retention_limit"
        );
    }

    /// A drain that cannot materialize a batch because the journal is at its
    /// cap reports the retention condition: the desktop names the usage and
    /// offers compaction instead of showing a generic queued sentence.
    #[test]
    fn a_full_journal_during_delivery_reports_the_retention_condition() {
        struct Unreached;
        impl worker::Receiver for Unreached {
            fn mapping_version(&self) -> &str {
                destination::MAPPING_VERSION
            }
            fn supported_kinds(&self) -> &[&str] {
                &["history", "session_event", "session"]
            }
            fn supports_tombstones(&self) -> bool {
                true
            }
            fn prepare(
                &self,
                _: &delivery::HistoryExportBatch,
                _: &worker::ReceiverContext<'_>,
            ) -> std::result::Result<worker::PreparedBody, worker::ReceiverFailure> {
                panic!("a batch cannot be prepared while the journal is full")
            }
            fn send(
                &self,
                _: &delivery::PreparedPayload,
                _: &delivery::HistoryExportBatch,
                _: &worker::ReceiverContext<'_>,
            ) -> std::result::Result<delivery::DeliveryAcknowledgment, worker::ReceiverFailure>
            {
                panic!("nothing is dispatched while the journal is full")
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.db");
        let conn = relayhistory_plugin::delivery::open_db(&path).unwrap();
        conn.execute(
            "INSERT INTO sessions(source,session_id) VALUES ('claude','queued')",
            [],
        )
        .unwrap();
        conn.execute("INSERT INTO session_events(source,session_id,event_uid,ts_ms,role,kind,text) VALUES('claude','queued','one',1,'user','text','synthetic')",[]).unwrap();
        let job = delivery::create_job(&conn, &job_config(true), now()).unwrap();
        delivery::set_retention_limit(&conn, delivery::retained_bytes(&conn).unwrap().0.max(1))
            .unwrap();
        // Batch materialization has its own reserve above the cap, so a full
        // journal alone no longer refuses a batch: the drain is meant to
        // deliver its way out of one. This stands in for the exhausted reserve,
        // which is the only state that still refuses the write.
        conn.execute_batch(
            "CREATE TRIGGER synthetic_reserve_exhausted BEFORE INSERT ON delivery_batches
             BEGIN SELECT RAISE(ABORT,'delivery retention limit exceeded; synthetic reserve exhausted'); END;",
        )
        .unwrap();
        let config = Config {
            version: 1,
            site_url: "https://agentrelay.com".into(),
            account_id: "account".into(),
            account_email: None,
            account_name: None,
            account_avatar_url: None,
            org_id: "org".into(),
            workspace_id: "workspace".into(),
            history_url: "https://history.agentrelay.com".into(),
            delivery_account: job.config.account_id,
            job_id: job.job_id,
            include_existing: true,
            sharing_mode: None,
            acknowledge_uninspected_schedules: false,
        };
        let error = deliver_with_receiver(&path, &config, &Unreached, &|| false).unwrap_err();
        assert_eq!(capture_error_class(&error), "retention_limit");
        assert_eq!(local_failure_message(&error), Some(RETENTION_LIMIT_MESSAGE));
        assert!(!error.to_string().contains("queued or needs attention"));
    }

    fn job_config(include_existing: bool) -> delivery::DeliveryJobConfig {
        delivery::DeliveryJobConfig {
            destination_id: DESTINATION.into(),
            instance_id: INSTANCE.into(),
            account_id: destination::account_id("org", Some("workspace")),
            mapping_version: destination::MAPPING_VERSION.into(),
            selection: selection(include_existing),
            limits: Default::default(),
        }
    }
    #[test]
    fn pause_during_drain_is_successful_without_acknowledging_the_batch() {
        struct PausingReceiver<'a> {
            conn: &'a Connection,
            job_id: &'a str,
            sent: std::cell::Cell<bool>,
        }
        impl worker::Receiver for PausingReceiver<'_> {
            fn mapping_version(&self) -> &str {
                destination::MAPPING_VERSION
            }
            fn supported_kinds(&self) -> &[&str] {
                &["history", "session_event", "session"]
            }
            fn supports_tombstones(&self) -> bool {
                true
            }
            fn prepare(
                &self,
                _: &delivery::HistoryExportBatch,
                _: &worker::ReceiverContext<'_>,
            ) -> std::result::Result<worker::PreparedBody, worker::ReceiverFailure> {
                Ok(worker::PreparedBody {
                    content_type: "application/json".into(),
                    body: "{}".into(),
                })
            }
            fn send(
                &self,
                _: &delivery::PreparedPayload,
                _: &delivery::HistoryExportBatch,
                _: &worker::ReceiverContext<'_>,
            ) -> std::result::Result<delivery::DeliveryAcknowledgment, worker::ReceiverFailure>
            {
                self.sent.set(true);
                delivery::pause_job(self.conn, self.job_id).unwrap();
                Err(delivery::DeliveryFailure::Transient.into())
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("history.db");
        let conn = relayhistory_plugin::delivery::open_db(&db_path).unwrap();
        conn.execute(
            "INSERT INTO sessions(source,session_id) VALUES ('claude','in-flight')",
            [],
        )
        .unwrap();
        let job = delivery::create_job(&conn, &job_config(true), now()).unwrap();
        let config = Config {
            version: 1,
            site_url: "https://agentrelay.com".into(),
            account_id: "account".into(),
            account_email: None,
            account_name: None,
            account_avatar_url: None,
            org_id: "org".into(),
            workspace_id: "workspace".into(),
            history_url: "https://history.agentrelay.com".into(),
            delivery_account: job.config.account_id,
            job_id: job.job_id,
            include_existing: true,
            sharing_mode: None,
            acknowledge_uninspected_schedules: false,
        };
        let receiver = PausingReceiver {
            conn: &conn,
            job_id: &config.job_id,
            sent: std::cell::Cell::new(false),
        };
        let status = deliver_with_receiver(&db_path, &config, &receiver, &|| false).unwrap();
        assert!(receiver.sent.get());
        assert_eq!(status.state, "paused");
        assert_eq!(status.acknowledged_records, 0);
        assert!(status.pending_records > 0);
        delivery::cancel_job(&conn, &config.job_id).unwrap();
        assert!(deliver_with_receiver(&db_path, &config, &receiver, &|| false).is_err());
    }

    #[test]
    fn stop_watch_ignores_other_generations_and_cancels_capture() {
        let dir = tempfile::tempdir().unwrap();
        save_json(&dir.path().join("stop.json"), &json!({"startup_id":"old"})).unwrap();
        let watch = StopWatch::start(dir.path(), "current");
        std::thread::sleep(Duration::from_millis(150));
        assert!(!stopping(&watch.cancelled));
        let started = Instant::now();
        save_json(
            &dir.path().join("stop.json"),
            &json!({"startup_id":"current"}),
        )
        .unwrap();
        while !stopping(&watch.cancelled) && started.elapsed() < Duration::from_secs(2) {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(stopping(&watch.cancelled));
        // The same token used by capture is also passed to the delivery drain.
        let stop = watch.cancelled.clone();
        let error = ai_hist::sync_local_at_cancellable(
            &dir.path().join("history.db"),
            |_| {},
            move || stopping(&stop),
        )
        .unwrap_err();
        assert!(error.is::<ai_hist::CaptureCancelled>());
        assert!(!dir.path().join("history.db").exists());
    }

    #[test]
    fn stop_file_during_batch_preparation_prevents_send_and_keeps_pending_records() {
        struct StoppingReceiver<'a>(&'a Path);
        impl worker::Receiver for StoppingReceiver<'_> {
            fn mapping_version(&self) -> &str {
                destination::MAPPING_VERSION
            }
            fn supported_kinds(&self) -> &[&str] {
                &["history", "session_event", "session"]
            }
            fn supports_tombstones(&self) -> bool {
                true
            }
            fn prepare(
                &self,
                _: &delivery::HistoryExportBatch,
                ctx: &worker::ReceiverContext<'_>,
            ) -> std::result::Result<worker::PreparedBody, worker::ReceiverFailure> {
                save_json(&self.0.join("stop.json"), &json!({"startup_id":"drain"})).unwrap();
                let deadline = Instant::now() + Duration::from_secs(2);
                while !(ctx.cancelled)() && Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(10));
                }
                assert!(
                    (ctx.cancelled)(),
                    "normal stop must reach the receiver context"
                );
                Ok(worker::PreparedBody {
                    content_type: "application/json".into(),
                    body: "{}".into(),
                })
            }
            fn send(
                &self,
                _: &delivery::PreparedPayload,
                _: &delivery::HistoryExportBatch,
                _: &worker::ReceiverContext<'_>,
            ) -> std::result::Result<delivery::DeliveryAcknowledgment, worker::ReceiverFailure>
            {
                panic!("must not send after stop");
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("history.db");
        let conn = relayhistory_plugin::delivery::open_db(&db).unwrap();
        conn.execute(
            "INSERT INTO sessions(source,session_id) VALUES ('claude','queued')",
            [],
        )
        .unwrap();
        let job = delivery::create_job(&conn, &job_config(true), now()).unwrap();
        let config = Config {
            version: 1,
            site_url: "https://agentrelay.com".into(),
            account_id: "account".into(),
            account_email: None,
            account_name: None,
            account_avatar_url: None,
            org_id: "org".into(),
            workspace_id: "workspace".into(),
            history_url: "https://history.agentrelay.com".into(),
            delivery_account: job.config.account_id,
            job_id: job.job_id,
            include_existing: true,
            sharing_mode: None,
            acknowledge_uninspected_schedules: false,
        };
        let stop = StopWatch::start(dir.path(), "drain");
        let status = deliver_with_receiver(&db, &config, &StoppingReceiver(dir.path()), &|| {
            stopping(&stop.cancelled)
        })
        .unwrap();
        assert!(stopping(&stop.cancelled));
        assert_eq!(status.state, "active");
        assert_eq!(status.acknowledged_records, 0);
        assert!(status.pending_records > 0);
        assert_eq!(status.failure, None);
        assert_eq!(status.next_attempt_ms, 0);
        let attempts: i64 = conn
            .query_row(
                "SELECT attempts FROM delivery_jobs WHERE id=?",
                [&config.job_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(attempts, 0);
        assert!(
            delivery::claim_batch(&conn, &config.job_id, "restarted", 60_000, &now)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn sharing_stop_waits_for_collector_lock_release() {
        let dir = tempfile::tempdir().unwrap();
        let lock = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.path().join("collector.lock"))
            .unwrap();
        lock.lock_exclusive().unwrap();
        save_json(
            &dir.path().join("runtime.json"),
            &json!({"startup_id":"test-run"}),
        )
        .unwrap();
        let release = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            FileExt::unlock(&lock).unwrap();
        });

        stop_for_change(dir.path()).unwrap();

        release.join().unwrap();
        assert!(stop_requested(dir.path(), "test-run"));
        assert!(!running(dir.path()).unwrap());
    }
    /// Claim batches until one survives exclusion, and report its sessions.
    fn claim_sessions(conn: &Connection, job_id: &str) -> Vec<String> {
        for _ in 0..500 {
            let prepared = delivery::prepare_batch(conn, job_id, now()).unwrap();
            if prepared.batch_id.is_none() {
                if prepared.bootstrap_complete && prepared.scanned_records == 0 {
                    break;
                }
                continue;
            }
            if let Some(claim) = delivery::claim_batch(conn, job_id, "test", 60_000, &now).unwrap()
            {
                return claim
                    .batch
                    .records
                    .iter()
                    .filter_map(|record| record.session_id.clone())
                    .collect();
            }
        }
        vec![]
    }
    #[test]
    fn new_only_baseline_is_durable_and_independent_of_history_size() {
        let conn = Connection::open_in_memory().unwrap();
        relayhistory_plugin::delivery::init_db(&conn).unwrap();
        for index in 0..1200 {
            conn.execute(
                "INSERT INTO sessions(source, session_id) VALUES ('claude', ?1)",
                [format!("11111111-2222-3333-4444-{index:012}")],
            )
            .unwrap();
        }
        let baseline = ai_hist::storage::session_identities_after(&conn, None, 10_000).unwrap();
        // The same identities carried inline would exceed the 64 KiB cap that
        // create_job enforces on a delivery configuration.
        assert!(serde_json::to_vec(&baseline).unwrap().len() > 65_536);
        assert_eq!(record_baseline(&conn, false).unwrap(), baseline.len());
        let config = job_config(false);
        assert!(!config.selection.kinds.iter().any(|kind| kind == "history"));
        assert!(config.selection.excluded_sessions.is_empty());
        let job = delivery::create_job(&conn, &config, now()).unwrap();
        assert!(claim_sessions(&conn, &job.job_id).is_empty());
        assert!(
            delivery::status(&conn, &job.job_id)
                .unwrap()
                .suppressed_records
                > 0
        );
        conn.execute(
            "INSERT INTO sessions(source, session_id) VALUES ('claude','after')",
            [],
        )
        .unwrap();
        assert_eq!(
            claim_sessions(&conn, &job.job_id),
            vec!["after".to_string()]
        );
    }
    #[test]
    fn include_existing_records_no_baseline() {
        let conn = Connection::open_in_memory().unwrap();
        relayhistory_plugin::delivery::init_db(&conn).unwrap();
        conn.execute(
            "INSERT INTO sessions(source, session_id) VALUES ('claude','before')",
            [],
        )
        .unwrap();
        assert_eq!(record_baseline(&conn, true).unwrap(), 0);
        let job = delivery::create_job(&conn, &job_config(true), now()).unwrap();
        assert_eq!(
            claim_sessions(&conn, &job.job_id),
            vec!["before".to_string()]
        );
    }
    #[test]
    fn an_abandoned_baseline_does_not_outlive_a_later_include_existing_setup() {
        let conn = Connection::open_in_memory().unwrap();
        relayhistory_plugin::delivery::init_db(&conn).unwrap();
        conn.execute(
            "INSERT INTO sessions(source, session_id) VALUES ('claude','before')",
            [],
        )
        .unwrap();
        // Setup is interrupted after the snapshot, before any generation exists.
        assert_eq!(record_baseline(&conn, false).unwrap(), 1);
        // The next attempt chooses to share existing sessions instead.
        assert_eq!(record_baseline(&conn, true).unwrap(), 0);
        let job = delivery::create_job(&conn, &job_config(true), now()).unwrap();
        assert_eq!(
            claim_sessions(&conn, &job.job_id),
            vec!["before".to_string()]
        );
    }
    #[test]
    fn os_lock_releases_after_owner_exit_without_pid_reuse() {
        let directory = tempfile::tempdir().unwrap();
        let guard = lock(directory.path()).unwrap();
        assert!(running(directory.path()).unwrap());
        assert!(lock(directory.path()).is_err());
        drop(guard);
        assert!(!running(directory.path()).unwrap());
    }
}
