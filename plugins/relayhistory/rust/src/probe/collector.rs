use super::{lock, read_config, save_json, user_error, Config};
use ai_hist_core::delivery::{self, DeliveryFailure, ExportSelection};
use anyhow::{ensure, Context, Result};
use relayhistory_plugin::{cloud, destination};
use rusqlite::Connection;
use serde_json::json;
use std::{
    fs,
    io::Write,
    path::Path,
    process::{Command, Stdio},
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

static STOP: AtomicBool = AtomicBool::new(false);
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
/// Record the new-only baseline as durable exclusions, which delivery rechecks
/// when a batch is prepared, claimed and dispatched. This must complete before
/// the job exists: an exclusion added afterwards would race an already prepared
/// batch, while a baseline written without a job only withholds more history.
pub fn record_baseline(conn: &Connection, include_existing: bool) -> Result<usize> {
    if include_existing {
        return Ok(0);
    }
    let snapshot = conn.unchecked_transaction()?;
    let mut identities = Vec::new();
    loop {
        let page =
            ai_hist_core::storage::session_identities_after(&snapshot, identities.last(), 1000)?;
        if page.is_empty() {
            break;
        }
        identities.extend(page);
    }
    // Exclusions take their own short write transactions, so the consistent
    // read has to end before any of them is recorded.
    snapshot.commit()?;
    for identity in &identities {
        delivery::set_session_excluded(conn, identity, true)?;
    }
    Ok(identities.len())
}
/// Only the transport classifies the batch itself; `destination::prepare` and
/// `send` report a mapping or payload verdict as a `TransportFailure`. Every
/// other error on this path is a local condition that can clear — a lost lease,
/// a storage error, or the consent recheck failing to inspect schedules — so it
/// stays retryable. A nonretryable verdict blocks the job until an explicit
/// retry, which no amount of waiting would resolve.
fn failure_for(error: &anyhow::Error) -> DeliveryFailure {
    error
        .downcast_ref::<destination::TransportFailure>()
        .map(|failure| failure.0)
        .unwrap_or(DeliveryFailure::Transient)
}
fn drain(conn: &Connection, config: &Config) -> Result<()> {
    super::check_legacy_schedules(config.acknowledge_uninspected_schedules)?;
    let worker = format!("probe-{}", std::process::id());
    let mut attempts = 0;
    for _ in 0..100 {
        if STOP.load(Ordering::Relaxed) {
            return Ok(());
        }
        let status = delivery::status(conn, &config.job_id)?;
        ensure!(
            status.config.account_id == config.delivery_account
                && status.config.mapping_version == destination::MAPPING_VERSION,
            "delivery config mismatch"
        );
        if status.state != "active" {
            return Err(user_error("Delivery needs attention. Reconnect with --force-login if credentials have expired."));
        }
        if status.next_attempt_ms > now() {
            return Err(user_error("Delivery is queued for retry."));
        }
        if attempts >= 8 {
            break;
        }
        let prepared = delivery::prepare_batch(conn, &config.job_id, now())?;
        if prepared.batch_id.is_none() {
            if prepared.bootstrap_complete && prepared.scanned_records == 0 {
                break;
            }
            continue;
        }
        let Some(claim) = delivery::claim_batch(conn, &config.job_id, &worker, 300_000, now())?
        else {
            break;
        };
        attempts += 1;
        let result = (|| -> Result<()> {
            if claim.prepared.is_none() {
                let payload = destination::prepare(claim.batch.clone())?;
                delivery::store_prepared_payload(
                    conn,
                    &claim.lease,
                    &payload.mapping_version,
                    &payload.content_type,
                    &payload.body,
                    now(),
                )?;
            }
            // Recheck consent and the lease immediately before the network call.
            super::check_legacy_schedules(config.acknowledge_uninspected_schedules)?;
            let payload = delivery::validate_dispatch(conn, &claim.lease, now())?;
            let receipt = destination::send(Some(&config.history_url), &payload)?;
            delivery::acknowledge(conn, &claim.lease, &receipt, now())?;
            Ok(())
        })();
        if let Err(error) = result {
            delivery::record_failure(conn, &claim.lease, failure_for(&error), None, now())?;
            return Err(user_error("Session delivery is queued or needs attention. No unacknowledged data was discarded."));
        }
    }
    delivery::expire_exports(conn, now(), 32)?;
    delivery::compact_journal(conn, 1_000)?;
    delivery::compact_receipts(conn, 1_000)?;
    Ok(())
}
pub fn cycle(directory: &Path, config: &Config) -> Result<()> {
    ensure!(
        destination::selected_account(Some(&config.history_url))? == config.delivery_account,
        "wrong destination"
    );
    let db_path = directory.join("history.db");
    ensure!(
        ai_hist_engine::sync_local_at(&db_path)?,
        "capture not complete"
    );
    let conn = ai_hist_core::open_db(&db_path)?;
    drain(&conn, config)?;
    let token = cloud::access_token(Some(&config.history_url))?;
    let agent = ureq::AgentBuilder::new()
        .redirects(0)
        .timeout(Duration::from_secs(15))
        .build();
    agent
        .post(&format!("{}/v1/onboarding/heartbeat", config.history_url))
        .set("Authorization", &format!("Bearer {token}"))
        .call()
        .map_err(|_| user_error("Could not confirm the connection with Cloud."))?;
    let status = delivery::status(&conn, &config.job_id)?;
    println!(
        "Probe connected: {} records received, {} queued.",
        status.acknowledged_records, status.pending_records
    );
    Ok(())
}
fn running(directory: &Path) -> Result<bool> {
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
    println!(
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
    if !running(directory)? {
        println!("Probe is stopped.");
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
    for _ in 0..45 {
        if !running(directory)? {
            println!("Probe stopped.");
            return Ok(());
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    Err(user_error(
        "Stop was requested. The probe is finishing its current capture operation.",
    ))
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
    let config = read_config(directory)?;
    let _lock = lock(directory)?;
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
    while !stop_requested(directory, startup_id) {
        if cycle(directory, &config).is_err() {
            eprintln!("Sync paused or offline. Retrying; local data remains queued.");
        }
        for _ in 0..20 {
            if stop_requested(directory, startup_id) {
                break;
            }
            std::thread::sleep(Duration::from_secs(1));
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
            println!("Probe is running in the background. You can close this terminal.");
            println!("Run setup again after restarting the computer.");
            let config = read_config(directory)?;
            println!(
                "Stop: agent-relay-probe stop --site-url {} --workspace {} --account {}",
                config.site_url, config.workspace_id, config.account_id
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
    fn job_config(include_existing: bool) -> delivery::DeliveryJobConfig {
        delivery::DeliveryJobConfig {
            destination_id: "relayhistory".into(),
            instance_id: "teams-probe".into(),
            account_id: destination::account_id("org", Some("workspace")),
            mapping_version: destination::MAPPING_VERSION.into(),
            selection: selection(include_existing),
            limits: Default::default(),
        }
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
            if let Some(claim) = delivery::claim_batch(conn, job_id, "test", 60_000, now()).unwrap()
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
        ai_hist_core::init_db(&conn).unwrap();
        for index in 0..1200 {
            conn.execute(
                "INSERT INTO sessions(source, session_id) VALUES ('claude', ?1)",
                [format!("11111111-2222-3333-4444-{index:012}")],
            )
            .unwrap();
        }
        let baseline =
            ai_hist_core::storage::session_identities_after(&conn, None, 10_000).unwrap();
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
        ai_hist_core::init_db(&conn).unwrap();
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
    fn only_a_transport_verdict_can_block_the_job() {
        // A lease, storage or consent-recheck error must not be recorded as a
        // nonretryable payload verdict: that blocks delivery until an explicit
        // retry even after the condition clears.
        assert_eq!(
            failure_for(&user_error("Could not inspect older upload schedules.")),
            DeliveryFailure::Transient
        );
        assert_eq!(
            failure_for(&anyhow::anyhow!("delivery batch missing")),
            DeliveryFailure::Transient
        );
        assert_eq!(
            failure_for(
                &anyhow::Error::from(destination::TransportFailure(
                    DeliveryFailure::InvalidPayload
                ))
                .context("preparing batch")
            ),
            DeliveryFailure::InvalidPayload
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
