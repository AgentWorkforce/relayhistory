//! End-to-end check that a real Devin CLI store flows through the probe:
//! `sessions preview --refresh` must report `source: "devin"`, and an isolated
//! install target must list the session and queue its journal records for
//! delivery. Presence-gated: skipped on machines without a Devin store.
//!
//! Everything the test writes lands under a synthetic `HOME`; the only real
//! path it reads is the provider's own `sessions.db`, opened read-only. The
//! test asserts on identities and counts only — it never inspects or prints
//! session content.

use relayhistory_plugin::delivery::{self, DeliveryJobConfig, SessionIdentity};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::process::Command;

fn devin_cli_dir() -> Option<PathBuf> {
    let data = std::env::var_os("XDG_DATA_HOME")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/share")))?;
    let dir = data.join("devin").join("cli");
    dir.join("sessions.db").is_file().then_some(dir)
}

/// A Devin session id present in the local store, picked without reading
/// message content — the catalog id column only.
fn some_devin_session(cli_dir: &Path) -> Option<String> {
    let conn = rusqlite::Connection::open_with_flags(
        cli_dir.join("sessions.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .ok()?;
    conn.query_row(
        "SELECT id FROM sessions WHERE COALESCE(hidden, 0) = 0 ORDER BY last_activity_at DESC LIMIT 1",
        [],
        |row| row.get(0),
    )
    .ok()
}

fn probe(home: &Path, args: &[&str]) -> Value {
    let output = Command::new(env!("CARGO_BIN_EXE_agent-relay-probe"))
        .arg("--json")
        .args(args)
        .env("HOME", home)
        .env("USERPROFILE", home)
        .output()
        .expect("run agent-relay-probe");
    assert!(
        output.status.success(),
        "probe {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("probe output is JSON")
}

#[test]
fn devin_sessions_reach_preview_and_an_isolated_install() {
    let Some(cli_dir) = devin_cli_dir() else {
        eprintln!("no Devin CLI store on this machine; skipping");
        return;
    };
    let Some(session_id) = some_devin_session(&cli_dir) else {
        eprintln!("Devin store has no visible sessions; skipping");
        return;
    };

    // Isolate HOME so preview metadata and the fake install land in a tempdir.
    // Pin XDG_DATA_HOME to the data root the Devin dir came from first: when
    // it was unset, `devin_cli_dir` derived the root from the real HOME, which
    // is about to change for this process and the probe subprocess.
    let data_root = cli_dir
        .parent()
        .and_then(Path::parent)
        .expect("devin/cli has a data root")
        .to_path_buf();
    let home = tempfile::tempdir().expect("temp home");
    std::env::set_var("XDG_DATA_HOME", &data_root);
    std::env::set_var("HOME", home.path());
    std::env::set_var("USERPROFILE", home.path());

    // 1. Local-only preview refresh reports the store's sessions.
    let preview = probe(home.path(), &["sessions", "preview", "--refresh"]);
    let found = preview["sessions"]
        .as_array()
        .expect("sessions array")
        .iter()
        .any(|s| s["source"] == "devin" && s["session_id"] == session_id);
    assert!(found, "preview must report devin session {session_id}");

    // 2. An isolated install target: a probe directory keyed to a synthetic
    //    (site, account, workspace), populated locally and never registered.
    let site = "http://127.0.0.1:9";
    let account = "devin-e2e";
    let workspace = "devin-e2e";
    let key = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&(site, account, workspace)).unwrap())
    );
    let directory = home.path().join(".agentworkforce/probe").join(key);
    std::fs::create_dir_all(&directory).unwrap();
    let db = directory.join("history.db");

    // Delivery schema and journal triggers exist before capture writes, and
    // the subscription exists before sync so writes land in delivery_journal.
    let conn = delivery::open_db(&db).expect("delivery db");
    let job = delivery::create_session_job(
        &conn,
        &DeliveryJobConfig {
            destination_id: "relayhistory".into(),
            instance_id: "e2e".into(),
            account_id: relayhistory_plugin::destination::account_id("org", Some("workspace")),
            mapping_version: relayhistory_plugin::destination::MAPPING_VERSION.into(),
            selection: ai_hist::export::ExportSelection {
                all_sources: true,
                sources: vec![],
                sessions: vec![],
                kinds: vec!["history".into(), "session_event".into(), "session".into()],
                excluded_sessions: vec![],
            },
            limits: Default::default(),
        },
        1_700_000_000_000,
    )
    .expect("session job");
    assert!(delivery::set_job_session(
        &conn,
        &job.job_id,
        &SessionIdentity {
            source: "devin".into(),
            session_id: session_id.clone(),
        },
        true,
    )
    .expect("include devin session"));

    ai_hist::sync_local_at(&db).expect("local sync");
    let journaled: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM delivery_journal WHERE source='devin' AND session_id=?",
            [&session_id],
            |row| row.get(0),
        )
        .expect("journal query");
    assert!(journaled > 0, "capture must journal devin records");

    let prepared =
        delivery::prepare_batch(&conn, &job.job_id, 1_700_000_000_001).expect("prepare batch");
    assert!(
        prepared.batch_id.is_some() && prepared.scanned_records > 0,
        "delivery must queue records for the devin session"
    );

    // 3. `sessions list` against the isolated target reports it as shareable.
    let config = serde_json::json!({
        "version": 1,
        "site_url": site,
        "account_id": "account",
        "org_id": "org",
        "workspace_id": "workspace",
        "history_url": site,
        "delivery_account": job.config.account_id,
        "job_id": job.job_id,
        "include_existing": true,
        "sharing_mode": "all",
    });
    std::fs::write(
        directory.join("config.json"),
        serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();
    let list = probe(
        home.path(),
        &[
            "sessions",
            "list",
            "--site-url",
            site,
            "--account",
            account,
            "--workspace",
            workspace,
        ],
    );
    let listed = list["sessions"]
        .as_array()
        .expect("sessions array")
        .iter()
        .find(|s| s["source"] == "devin" && s["session_id"] == session_id);
    let listed = listed.expect("sessions list must include the devin session");
    assert_eq!(listed["included"], true, "session must be deliverable");
}
