//! A native host can invoke this public operation before any other engine API.
//! Run it in a fresh process so prior tests cannot hide leaked sync output.
use ai_hist_engine::{remote::SourceConnectorSelection, SessionScope};
use std::{fs, process::Command};

#[test]
fn first_embedded_sync_does_not_write_progress_to_stdout() {
    let dir = tempfile::tempdir().unwrap();
    let transcript = dir.path().join(".claude/projects/app/first.jsonl");
    fs::create_dir_all(transcript.parent().unwrap()).unwrap();
    fs::write(&transcript, "{\"sessionId\":\"first\",\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"first native call\"},\"timestamp\":\"2026-09-13T00:00:00Z\"}\n").unwrap();
    let db = dir.path().join("history.db");
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "embedded_sync_child", "--nocapture"])
        .env("RH_SYNC_SILENCE_DB", &db)
        .env("HOME", dir.path())
        .env("USERPROFILE", dir.path())
        .env("XDG_DATA_HOME", dir.path().join("xdg"))
        .env("OPENCODE_DB", dir.path().join("absent-opencode.db"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(
        !stdout.contains("[claude]")
            && !stdout.contains("[opencode]")
            && !stdout.contains("Total:"),
        "embedded sync polluted stdout: {stdout}"
    );
    let conn = ai_hist_core::open_db(&db).unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM history WHERE prompt = 'first native call'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        count, 1,
        "the silent operation must actually ingest the provider fixture"
    );
    let observations = ai_hist_core::observations::list(&conn, "claude", "first").unwrap();
    assert_eq!(
        observations.len(),
        1,
        "ordinary local sync must establish executing connector provenance"
    );
    assert_eq!(observations[0].key.connector_id, "claude");
    assert_eq!(observations[0].raw_locator.as_deref(), transcript.to_str());
}

#[test]
fn embedded_sync_child() {
    let Some(path) = std::env::var_os("RH_SYNC_SILENCE_DB") else {
        return;
    };
    ai_hist_engine::sync_scoped_at_with_connectors(
        std::path::Path::new(&path),
        SessionScope::Local,
        &SourceConnectorSelection::default(),
    )
    .unwrap();
}
