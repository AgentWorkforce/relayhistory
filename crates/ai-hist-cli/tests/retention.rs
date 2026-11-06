use ai_hist::open_db;
use serde_json::{json, Value};
use std::process::Command;

fn run(path: &std::path::Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_ai-hist"))
        .arg("--db")
        .arg(path)
        .arg("retention")
        .args(args)
        .arg("--json")
        .output()
        .unwrap()
}
#[test]
fn preview_is_read_only_and_apply_is_one_protected_batch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("history.db");
    let preview = run(&path, &["--days", "1"]);
    assert!(preview.status.success());
    assert!(!path.exists());
    let result: Value = serde_json::from_slice(&preview.stdout).unwrap();
    assert_eq!(result["dry_run"], true);
    assert_eq!(result["preview"], "policy_only");
    let conn = open_db(&path).unwrap();
    conn.execute(
        "INSERT INTO sessions(source,session_id,last_activity_ms) VALUES('claude','old',1)",
        [],
    )
    .unwrap();
    for i in 0..5 {
        conn.execute("INSERT INTO session_events(source,session_id,event_uid,ts_ms,role,kind,text) VALUES('claude','old',?1,1,'assistant','text','retained')",[i.to_string()]).unwrap();
    }
    let preview = run(&path, &["--days", "1"]);
    assert!(preview.status.success());
    assert_eq!(
        ai_hist::retention::read_retention_policy(&path).unwrap(),
        ai_hist::retention::RetentionPolicy::default()
    );
    let guarded = run(&path, &["--apply", "--days", "1", "--max-rows", "2"]);
    assert!(guarded.status.success());
    let result: Value = serde_json::from_slice(&guarded.stdout).unwrap();
    assert_eq!(result["report"]["awaiting_upload_ack"], true);
    let applied = run(&path, &["--apply", "--allow-unuploaded", "--max-rows", "2"]);
    assert!(
        applied.status.success(),
        "{}",
        String::from_utf8_lossy(&applied.stderr)
    );
    let result: Value = serde_json::from_slice(&applied.stdout).unwrap();
    assert_eq!(result["report"]["rows"], json!(2));
    assert_eq!(
        conn.query_row("SELECT count(*) FROM session_events", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        3
    );
}
#[test]
fn invalid_options_do_not_create_a_database() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("history.db");
    for args in [
        &["--days", "-1"][..],
        &["--max-rows", "0"],
        &["--max-rows", "1025"],
        &["--allow-unuploaded"],
    ] {
        assert!(!run(&path, args).status.success());
        assert!(!path.exists());
    }
}
