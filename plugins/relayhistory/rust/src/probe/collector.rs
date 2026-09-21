use super::{lock, read_config, save_json, user_error, Config};
use anyhow::{ensure, Context, Result};
use relayhistory_plugin::delivery::{self, worker, ExportSelection};
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
        // A cycle stays bounded so a stop request and the heartbeat are never
        // starved by an unbounded backlog; the next cycle continues it.
        max_batches: 8,
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
        let value = json!({"operation":"capture","stage":"broad_capture","source":source,"processed_files":last.as_ref().map(|v|v.processed_files).unwrap_or(0),"elapsed_ms":started.elapsed().as_millis(),"error_class":capture_error_class(error)});
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
    cycle_with_stop(directory, config, Arc::new(AtomicBool::new(false)), false)
}

fn cycle_with_stop(
    directory: &Path,
    config: &Config,
    cancelled: Arc<AtomicBool>,
    before_exit: bool,
) -> Result<()> {
    super::bridge::enforce_selection(directory, config)?;
    let conn = relayhistory_plugin::delivery::open_db(&directory.join("history.db"))?;
    let status = delivery::status(&conn, &config.job_id)?;
    let selected = super::bridge::mode(config) == super::bridge::SharingMode::Selected;
    if status.state == "paused" {
        if !selected {
            let stop = cancelled.clone();
            ai_hist::sync_local_at_cancellable(
                &directory.join("history.db"),
                |_| {},
                move || stopping(&stop),
            )?;
        }
        return Ok(());
    }
    run_capture_cycle(
        selected,
        || deliver_captured_with_stop(directory, config, before_exit, &cancelled),
        || ready_to_capture(&conn, &config.job_id),
        || capture_selected(directory, config, &cancelled),
        || capture_with_stop(directory, &config.history_url, cancelled.clone()),
        || stopping(&cancelled),
    )
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
) -> Result<()> {
    let delivered = deliver();
    if stopped() || (delivered.is_ok() && selected && !capture_ready()?) {
        return delivered;
    }
    // Delivery errors must remain visible, but must not prevent acquiring
    // local evidence while credentials, transport or receiver state recover.
    let capture = if selected { targeted() } else { broad() };
    if stopped() {
        return delivered.and(capture);
    }
    let drained = deliver();
    delivered.and(capture).and(drained)
}

fn capture_selected(directory: &Path, config: &Config, cancelled: &Arc<AtomicBool>) -> Result<()> {
    let conn = relayhistory_plugin::delivery::open_db(&directory.join("history.db"))?;
    let members = delivery::job_sessions(&conn, &config.job_id)?;
    let started = Instant::now();
    let mut captured = 0;
    let mut failed = 0;
    for member in members {
        if stopping(cancelled) {
            break;
        }
        // Core hydration owns observation locks, provider resolution, parser,
        // evidence and checkpoints. It never enumerates an unrelated provider.
        let stop = cancelled.clone();
        let result = ai_hist::hydrate_session_at_cancellable(
            &directory.join("history.db"),
            &ai_hist::HydrateSessionOptions {
                source: member.source,
                session_id: member.session_id,
                scope: ai_hist::SessionScope::Local,
                include_related: false,
            },
            move || stopping(&stop),
        );
        match result {
            Ok(_) => captured += 1,
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
            }
        }
    }
    if failed == 0 {
        capture_diagnostic(directory, "targeted_hydration", started, captured, 0, None)?;
    }
    ensure!(
        failed == 0,
        "selected session capture incomplete; see redacted capture-diagnostic.json"
    );
    Ok(())
}

fn capture_error_class(error: &anyhow::Error) -> &'static str {
    if error.downcast_ref::<ai_hist::CaptureCancelled>().is_some() {
        return "cancelled";
    }
    if delivery::is_retention_limit(error) {
        return "retention_limit";
    }
    for cause in error.chain() {
        if let Some(sql) = cause.downcast_ref::<rusqlite::Error>() {
            return match sql.sqlite_error_code() {
                Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked) => {
                    "database_busy"
                }
                Some(rusqlite::ErrorCode::DiskFull) => "disk_full",
                Some(rusqlite::ErrorCode::DatabaseCorrupt) => "database_corrupt",
                _ => "database_error",
            };
        }
        if let Some(io) = cause.downcast_ref::<std::io::Error>() {
            return match io.kind() {
                std::io::ErrorKind::NotFound => "source_missing",
                std::io::ErrorKind::PermissionDenied => "permission_denied",
                _ => "io_error",
            };
        }
        if cause.downcast_ref::<serde_json::Error>().is_some() {
            return "invalid_source_json";
        }
    }
    "capture_error"
}

pub(super) fn local_failure_message(error: &anyhow::Error) -> Option<&'static str> {
    match capture_error_class(error) {
        "retention_limit" => Some("The local upload journal is full. Uploads will retry after consumed records can be compacted; queued sessions are preserved."),
        "database_corrupt" => Some("The local history database needs repair. Preserve the database before recovery; reconnecting will not repair it."),
        "disk_full" => Some("The local disk is full. Free disk space to resume session uploads."),
        "database_busy" => Some("Local history is busy. Agent Relay will retry when the other operation finishes."),
        "permission_denied" => Some("Agent Relay cannot access local history. Check local file permissions."),
        _ => None,
    }
}

fn cycle_report(result: &Result<()>) -> serde_json::Value {
    let error = result.as_ref().err();
    json!({
        "at_ms": now(), "ok": result.is_ok(),
        "error_class": error.map(capture_error_class),
        "message": error.map(|error| local_failure_message(error).unwrap_or(
            "Sync paused or offline. Retrying; local data remains queued."
        )),
    })
}

fn capture_diagnostic(
    directory: &Path,
    stage: &str,
    started: Instant,
    captured: usize,
    failed: usize,
    error: Option<&anyhow::Error>,
) -> Result<()> {
    let diagnostic = json!({"operation":"capture","stage":stage,"elapsed_ms":started.elapsed().as_millis(),"sessions_captured":captured,"failures":failed,"error_class":error.map(capture_error_class)});
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
        cycle_with_stop(directory, config, Arc::new(AtomicBool::new(false)), true)
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
        let result = if Instant::now() >= capture_due {
            capture_due = Instant::now() + Duration::from_secs(60);
            cycle_with_stop(directory, &config, stop.cancelled.clone(), false)
        } else {
            deliver_captured_with_stop(directory, &config, false, &stop.cancelled)
        };
        // A user stop is not a failed/offline cycle and must not start delivery.
        if stopping(&stop.cancelled) {
            break;
        }
        let report = cycle_report(&result);
        save_json(&directory.join("cycle.json"), &report)?;
        if let Some(message) = report["message"].as_str() {
            eprintln!("{message}");
        }
        for _ in 0..200 {
            if stopping(&stop.cancelled) {
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    save_json(
        &directory.join("runtime.json"),
        &json!({"startup_id":startup_id,"pid":std::process::id(),"ready":false}),
    )?;
    Ok(())
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

    #[test]
    fn cycle_errors_classify_local_storage_without_exposing_raw_details() {
        for (code, class) in [
            (rusqlite::ffi::SQLITE_CORRUPT, "database_corrupt"),
            (rusqlite::ffi::SQLITE_FULL, "disk_full"),
            (rusqlite::ffi::SQLITE_BUSY, "database_busy"),
        ] {
            let error = anyhow::Error::new(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(code),
                Some("secret prompt https://token:secret@example.invalid".into()),
            ));
            let report = cycle_report(&Err(error));
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
        assert_eq!(cycle_report(&Err(error))["error_class"], "retention_limit");
        let healthy = cycle_report(&Ok(()));
        assert_eq!(healthy["ok"], true);
        assert!(healthy["message"].is_null());
        assert!(healthy["error_class"].is_null());
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
