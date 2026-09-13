use ai_hist_core::open_db;
use std::fs;
use std::process::Command;

fn isolated_sync(temp: &tempfile::TempDir, db_path: &std::path::Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ai-hist"));
    command
        .arg("--db")
        .arg(db_path)
        .arg("sync")
        .env("HOME", temp.path())
        .env("USERPROFILE", temp.path())
        .env("XDG_DATA_HOME", temp.path().join("xdg"))
        .env_remove("AI_HIST_DB")
        .env_remove("OPENCODE_DB")
        .env_remove("RELAYCAST_API_KEY")
        .env_remove("RELAYCAST_WORKSPACE_ID");
    command
}

#[test]
fn one_bad_source_does_not_abort_the_sync_run() {
    let temp = tempfile::tempdir().unwrap();
    let claude_history = temp.path().join(".claude/history.jsonl");
    fs::create_dir_all(&claude_history).unwrap(); // A directory cannot be read as JSONL.
    let codex_history = temp.path().join(".codex/history.jsonl");
    fs::create_dir_all(codex_history.parent().unwrap()).unwrap();
    fs::write(
        &codex_history,
        r#"{"text":"later source survived","ts":2,"session_id":"healthy"}
"#,
    )
    .unwrap();
    let db_path = temp.path().join("history.db");

    let output = isolated_sync(&temp, &db_path).output().unwrap();
    assert!(
        output.status.success(),
        "a later source should still complete: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("[claude]"), "missing source name: {stderr}");
    assert!(
        stderr.contains("history source(s) failed") && stderr.contains("source(s) completed"),
        "missing partial-success summary: {stderr}"
    );
    let conn = open_db(&db_path).unwrap();
    let imported: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM history WHERE source = 'codex' AND prompt = 'later source survived'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        imported, 1,
        "the source after the failure must still be committed"
    );
}

#[test]
fn a_busy_source_failure_prints_a_fresh_write_capability_probe() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("history.db");
    let holder = open_db(&db_path).unwrap();
    holder.execute_batch("BEGIN IMMEDIATE").unwrap();
    let claude_history = temp.path().join(".claude/history.jsonl");
    fs::create_dir_all(claude_history.parent().unwrap()).unwrap();
    fs::write(
        &claude_history,
        r#"{"display":"contended prompt","timestamp":1,"sessionId":"busy"}
"#,
    )
    .unwrap();

    let output = isolated_sync(&temp, &db_path).output().unwrap();
    // Other absent/up-to-date sources still complete, so #48 deliberately
    // keeps the aggregate run successful while reporting this source failure.
    assert!(
        output.status.success(),
        "sync failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("ai-hist contention diagnostic")
            && stderr.contains("write capability probe is still blocked"),
        "missing automatic capability diagnostic: {stderr}"
    );

    holder.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn unsupported_remote_sync_fails_before_watch_or_service_setup() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("history.db");
    for args in [vec!["--remote"], vec!["--remote", "--install-service"]] {
        let mut command = isolated_sync(&temp, &db_path);
        command.args(args);
        let output = command.output().unwrap();
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr)
            .contains("no remote provider connectors are configured"));
    }

    let output = Command::new(env!("CARGO_BIN_EXE_ai-hist"))
        .arg("--db")
        .arg(&db_path)
        .args(["watch", "--remote", "--interval", "1"])
        .env("HOME", temp.path())
        .env("USERPROFILE", temp.path())
        .env("XDG_DATA_HOME", temp.path().join("xdg"))
        .output()
        .unwrap();
    assert!(!output.status.success(), "watch must fail rather than loop");
    assert!(String::from_utf8_lossy(&output.stderr)
        .contains("no remote provider connectors are configured"));
    assert!(
        !db_path.exists(),
        "unsupported sync must not create a ledger"
    );
}
