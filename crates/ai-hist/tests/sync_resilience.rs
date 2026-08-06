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
    assert!(db_path.exists());
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
    assert!(output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("ai-hist contention diagnostic")
            && stderr.contains("write capability probe is still blocked"),
        "missing automatic capability diagnostic: {stderr}"
    );

    holder.execute_batch("ROLLBACK").unwrap();
}
