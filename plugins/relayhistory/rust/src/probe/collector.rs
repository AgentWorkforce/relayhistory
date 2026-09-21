use super::{lock, read_config, save_json, user_error, Config};
use ai_hist::delivery::{self, worker, ExportSelection};
use anyhow::{ensure, Context, Result};
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
    let progress = super::progress::Monitor::start(directory, history_url, None);
    let result = ai_hist::sync_local_at_cancellable(
        &directory.join("history.db"),
        progress.observer(),
        move || stopping(&cancelled),
    );
    progress.finish(matches!(result, Ok(true)));
    ensure!(result?, "capture not complete");
    Ok(())
}

#[cfg(test)]
pub fn cycle(directory: &Path, config: &Config) -> Result<()> {
    cycle_with_stop(directory, config, Arc::new(AtomicBool::new(false)))
}

fn cycle_with_stop(directory: &Path, config: &Config, cancelled: Arc<AtomicBool>) -> Result<()> {
    let paused = {
        let conn = ai_hist::open_db(&directory.join("history.db"))?;
        delivery::status(&conn, &config.job_id)?.state == "paused"
    };
    if paused {
        // Another sync owns capture when this returns false. The paused
        // collector can retry next cycle without reporting an offline error.
        let stop = cancelled.clone();
        ai_hist::sync_local_at_cancellable(
            &directory.join("history.db"),
            |_| {},
            move || stopping(&stop),
        )?;
    } else {
        ensure!(
            destination::selected_account(Some(&config.history_url))? == config.delivery_account,
            "wrong destination"
        );
        capture_with_stop(directory, &config.history_url, cancelled.clone())?;
    }
    if stopping(&cancelled) {
        return Ok(());
    }
    deliver_captured_with_stop(directory, config, false, &cancelled)
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
        let conn = ai_hist::open_db(&directory.join("history.db"))?;
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
        let conn = ai_hist::open_db(&db_path)?;
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
    let progress =
        super::progress::Monitor::start(directory, &config.history_url, Some(&config.job_id));
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
    let mut capture_due = Instant::now();
    while !stopping(&stop.cancelled) {
        let result = if Instant::now() >= capture_due {
            capture_due = Instant::now() + Duration::from_secs(60);
            cycle_with_stop(directory, &config, stop.cancelled.clone())
        } else {
            deliver_captured_with_stop(directory, &config, false, &stop.cancelled)
        };
        // A user stop is not a failed/offline cycle and must not start delivery.
        if stopping(&stop.cancelled) {
            break;
        }
        save_json(
            &directory.join("cycle.json"),
            &json!({
                "at_ms":now(), "ok":result.is_ok(),
                "message":if result.is_err() { Some("Sync paused or offline. Retrying; local data remains queued.") } else { None }
            }),
        )?;
        if result.is_err() {
            eprintln!("Sync paused or offline. Retrying; local data remains queued.");
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
        let conn = ai_hist::open_db(&db_path).unwrap();
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
        let conn = ai_hist::open_db(&db).unwrap();
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
        ai_hist::init_db(&conn).unwrap();
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
        ai_hist::init_db(&conn).unwrap();
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
        ai_hist::init_db(&conn).unwrap();
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
