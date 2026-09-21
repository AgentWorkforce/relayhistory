//! Exercise the real stop command while the collector is writing a large source.
use ai_hist::delivery::{self, DeliveryJobConfig, ExportSelection};
use fs2::FileExt;
use relayhistory_plugin::destination;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::Write,
    path::Path,
    process::{Child, Command, ExitStatus, Stdio},
    time::{Duration, Instant},
};

struct Running(Child);
impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn wait(child: &mut Running, timeout: Duration) -> ExitStatus {
    let start = Instant::now();
    loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            return status;
        }
        assert!(
            start.elapsed() < timeout,
            "probe did not stop within {timeout:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}
fn command(home: &Path) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_agent-relay-probe"));
    cmd.env("HOME", home)
        .env("CLAUDE_CONFIG_DIR", home.join(".claude"))
        .env("CODEX_HOME", home.join(".codex"))
        .env("GROK_HOME", home.join(".grok"))
        .env("OPENCODE_DB", home.join("opencode.db"))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    cmd
}

#[test]
fn stop_command_interrupts_active_capture_and_preserves_committed_history() {
    let home = tempfile::tempdir().unwrap();
    let site = "https://agentrelay.com";
    let key = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&(site, "account", "workspace")).unwrap())
    );
    let directory = home.path().join(".agentworkforce/probe").join(key);
    fs::create_dir_all(&directory).unwrap();
    let db = directory.join("history.db");
    let conn = ai_hist::open_db(&db).unwrap();
    let job = delivery::create_job(
        &conn,
        &DeliveryJobConfig {
            destination_id: "relayhistory".into(),
            instance_id: "teams-probe".into(),
            account_id: destination::account_id("org", Some("workspace")),
            mapping_version: destination::MAPPING_VERSION.into(),
            selection: ExportSelection {
                all_sources: true,
                kinds: vec!["history".into(), "session_event".into(), "session".into()],
                ..Default::default()
            },
            limits: Default::default(),
        },
        0,
    )
    .unwrap();
    // Pausing uploads leaves capture active, with no network or credentials.
    delivery::pause_job(&conn, &job.job_id).unwrap();
    fs::write(
        directory.join("config.json"),
        serde_json::to_vec(&json!({
            "version":1,"site_url":site,"account_id":"account","org_id":"org",
            "workspace_id":"workspace","history_url":"https://history.agentrelay.com",
            "delivery_account":job.config.account_id,"job_id":job.job_id,"include_existing":true,
            "sharing_mode":null,"acknowledge_uninspected_schedules":false
        }))
        .unwrap(),
    )
    .unwrap();
    fs::create_dir_all(home.path().join(".claude")).unwrap();
    let mut source = std::io::BufWriter::new(
        fs::File::create(home.path().join(".claude/history.jsonl")).unwrap(),
    );
    const RECORDS: usize = 200_000;
    for i in 0..RECORDS {
        writeln!(source, "{}", json!({"display":format!("prompt {i}"),"timestamp":1700000000000_i64+i as i64,"project":"/tmp/project","sessionId":"session"})).unwrap();
    }
    source.flush().unwrap();
    let mut collector = Running(
        command(home.path())
            .args(["run", "--directory"])
            .arg(&directory)
            .args(["--startup-id", "stop-test"])
            .spawn()
            .unwrap(),
    );
    let count = || {
        conn.query_row("SELECT COUNT(*) FROM history", [], |row| {
            row.get::<_, usize>(0)
        })
        .unwrap()
    };
    let start = Instant::now();
    while count() == 0 {
        assert!(
            collector.0.try_wait().unwrap().is_none(),
            "collector exited before capturing"
        );
        assert!(
            start.elapsed() < Duration::from_secs(15),
            "capture never started"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(count() < RECORDS, "fixture must still be capturing");
    let stopping = Instant::now();
    let mut stop = Running(
        command(home.path())
            .args([
                "stop",
                "--site-url",
                site,
                "--account",
                "account",
                "--workspace",
                "workspace",
            ])
            .spawn()
            .unwrap(),
    );
    assert!(wait(&mut stop, Duration::from_secs(3)).success());
    assert!(wait(&mut collector, Duration::from_secs(1)).success());
    eprintln!("busy collector stopped in {:?}", stopping.elapsed());
    assert!(
        (1..RECORDS).contains(&count()),
        "capture should stop partway through the source"
    );
    let runtime: Value =
        serde_json::from_slice(&fs::read(directory.join("runtime.json")).unwrap()).unwrap();
    assert_eq!(runtime["ready"], false);
    assert!(
        !directory.join("cycle.json").exists(),
        "cancellation is not an offline cycle"
    );
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(directory.join("collector.lock"))
        .unwrap();
    lock.try_lock_exclusive().unwrap();
    let integrity: String = conn
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .unwrap();
    assert_eq!(integrity, "ok");
    let checkpoint: Value =
        serde_json::from_slice(&fs::read(directory.join(".sync-state.json")).unwrap()).unwrap();
    assert!(checkpoint["claude"]["offset"].as_u64().unwrap() > 0);
}
