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
pub fn selection(conn: &Connection, include_existing: bool) -> Result<ExportSelection> {
    let excluded_sessions = if include_existing {
        vec![]
    } else {
        let snapshot = conn.unchecked_transaction()?;
        let mut identities = Vec::new();
        loop {
            let page = ai_hist_core::storage::session_identities_after(
                &snapshot,
                identities.last(),
                1000,
            )?;
            if page.is_empty() {
                break;
            }
            identities.extend(page);
        }
        snapshot.commit()?;
        identities
    };
    Ok(ExportSelection {
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
        excluded_sessions,
    })
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
            let failure = error
                .downcast_ref::<destination::TransportFailure>()
                .map(|v| v.0)
                .unwrap_or(DeliveryFailure::InvalidPayload);
            delivery::record_failure(conn, &claim.lease, failure, None, now())?;
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
    cycle(directory, &config)?;
    save_json(
        &directory.join("runtime.json"),
        &json!({"startup_id":startup_id,"pid":std::process::id(),"ready":true}),
    )?;
    while !stop_requested(directory, startup_id) {
        for _ in 0..20 {
            if stop_requested(directory, startup_id) {
                break;
            }
            std::thread::sleep(Duration::from_secs(1));
        }
        if stop_requested(directory, startup_id) {
            break;
        }
        if cycle(directory, &config).is_err() {
            eprintln!("Sync paused or offline. Retrying; local data remains queued.");
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
    use ai_hist_core::delivery::SessionIdentity;
    #[test]
    fn new_only_excludes_each_existing_session() {
        let conn = Connection::open_in_memory().unwrap();
        ai_hist_core::init_db(&conn).unwrap();
        conn.execute(
            "INSERT INTO sessions(source, session_id) VALUES ('claude','before')",
            [],
        )
        .unwrap();
        let selection = selection(&conn, false).unwrap();
        assert!(!selection.kinds.iter().any(|kind| kind == "history"));
        assert_eq!(
            selection.excluded_sessions,
            vec![SessionIdentity {
                source: "claude".into(),
                session_id: "before".into()
            }]
        );
        assert!(super::selection(&conn, true)
            .unwrap()
            .excluded_sessions
            .is_empty());
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
