#![cfg(all(feature = "export", feature = "unstable-internal"))]
//! Capture backpressure at the delivery retention cap.
//!
//! Fake provider homes and a temporary database only: no harness runs. A
//! whole-store subscription makes every evidence write journal a revision, so
//! the retention budget fills the way it does under a live delivery job.

use ai_hist::export::{self, capture, is_retention_limit, retention_limit_usage};
use ai_hist::{HydrateSessionOptions, SessionScope, SyncOutput};
use std::path::{Path, PathBuf};

/// One finished Codex rollout carrying a prompt, an answer and a tool call.
fn write_codex_rollout(home: &Path, session_id: &str) -> PathBuf {
    let day = home.join(".codex/sessions/2026/09/19");
    std::fs::create_dir_all(&day).expect("codex session dir");
    let path = day.join(format!("rollout-2026-09-19T10-00-00-{session_id}.jsonl"));
    let body = format!(
        "{}\n{}\n{}\n{}\n",
        format_args!(
            "{{\"timestamp\":\"2026-09-19T10:00:00.000Z\",\"type\":\"session_meta\",\
             \"payload\":{{\"id\":\"{session_id}\",\"cwd\":\"/tmp/{session_id}\"}}}}"
        ),
        "{\"timestamp\":\"2026-09-19T10:00:01.000Z\",\"type\":\"response_item\",\
         \"payload\":{\"type\":\"message\",\"role\":\"user\",\
         \"content\":[{\"type\":\"input_text\",\"text\":\"capture me\"}]}}",
        "{\"timestamp\":\"2026-09-19T10:00:02.000Z\",\"type\":\"event_msg\",\
         \"payload\":{\"type\":\"agent_message\",\"message\":\"Done.\"}}",
        "{\"timestamp\":\"2026-09-19T10:00:03.000Z\",\"type\":\"response_item\",\
         \"payload\":{\"type\":\"function_call\",\"id\":\"fc_1\",\
         \"name\":\"exec_command\",\"arguments\":\"{\\\"cmd\\\":\\\"git status\\\"}\",\
         \"call_id\":\"call_1\"}}",
    );
    std::fs::write(&path, body).expect("write rollout");
    path
}

fn append_codex_answer(path: &Path, text: &str) {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(path)
        .expect("open rollout");
    writeln!(
        file,
        "{{\"timestamp\":\"2026-09-19T10:00:09.000Z\",\"type\":\"event_msg\",\
         \"payload\":{{\"type\":\"agent_message\",\"message\":\"{text}\"}}}}"
    )
    .expect("append answer");
}

/// A whole-store reader whose journal cursor decides what compaction may
/// reclaim: nothing at zero, everything at the latest revision.
fn subscribe(db: &Path, cursor: i64) {
    let conn = ai_hist::open_db(db).expect("open db");
    let tx = conn.unchecked_transaction().expect("transaction");
    capture::save_subscription(
        &tx,
        &capture::Subscription {
            id: "reader",
            session: None,
            cursor,
            kind: 0,
            rowid: 0,
            complete: true,
        },
    )
    .expect("save subscription");
    tx.commit().expect("commit");
}

fn consume_everything(db: &Path) {
    let conn = ai_hist::open_db(db).expect("open db");
    let latest = capture::latest_revision(&conn).expect("latest revision");
    subscribe(db, latest);
}

fn usage(db: &Path) -> (i64, i64) {
    let conn = ai_hist::open_db(db).expect("open db");
    export::retained_bytes(&conn).expect("retained bytes")
}

fn cap_at(db: &Path, max_bytes: i64) {
    let conn = ai_hist::open_db(db).expect("open db");
    export::set_retention_limit(&conn, max_bytes).expect("set retention limit");
}

fn codex_sessions(db: &Path) -> Vec<String> {
    let conn = ai_hist::open_db(db).expect("open db");
    let mut statement = conn
        .prepare("SELECT session_id FROM sessions WHERE source = 'codex' ORDER BY session_id")
        .expect("prepare");
    statement
        .query_map([], |row| row.get(0))
        .expect("query")
        .collect::<Result<Vec<String>, _>>()
        .expect("rows")
}

fn codex_event_count(db: &Path, session_id: &str) -> i64 {
    let conn = ai_hist::open_db(db).expect("open db");
    conn.query_row(
        "SELECT COUNT(*) FROM session_events WHERE source = 'codex' AND session_id = ?",
        [session_id],
        |row| row.get(0),
    )
    .expect("count events")
}

fn sync(db: &Path, home: &Path) -> anyhow::Result<ai_hist::SyncTick> {
    ai_hist::sync_tick_at_with_home(db, home, SyncOutput::Silent, true)
}

fn hydrate(
    db: &Path,
    home: &Path,
    session_id: &str,
) -> anyhow::Result<ai_hist::HydrateSessionResult> {
    ai_hist::hydrate_session_at_with_home(
        db,
        &HydrateSessionOptions {
            source: "codex".into(),
            session_id: session_id.into(),
            scope: SessionScope::Local,
            include_related: false,
        },
        home,
    )
}

/// A seeded store with `sessions` captured under a reader at cursor zero.
fn seeded(sessions: usize) -> (tempfile::TempDir, PathBuf) {
    let home = tempfile::tempdir().expect("home");
    let db = home.path().join("history.db");
    subscribe(&db, 0);
    for index in 0..sessions {
        write_codex_rollout(home.path(), &format!("seed-{index:02}"));
    }
    assert!(sync(&db, home.path()).expect("seed sync").swept);
    assert!(usage(&db).0 > 0, "the seed must journal revisions");
    (home, db)
}

#[test]
fn a_full_budget_with_nothing_reclaimable_stops_the_pass_before_any_session() {
    let (home, db) = seeded(1);
    let (used, _) = usage(&db);
    cap_at(&db, used);
    for index in 0..5 {
        write_codex_rollout(home.path(), &format!("new-{index}"));
    }
    let error = sync(&db, home.path()).expect_err("the pass must stop at the cap");
    assert!(is_retention_limit(&error), "{error:#}");
    let reached = retention_limit_usage(&error).expect("typed usage");
    assert_eq!((reached.used_bytes, reached.limit_bytes), (used, used));
    assert_eq!(
        codex_sessions(&db),
        vec!["seed-00".to_string()],
        "no new session may be attempted at a full, unreclaimable budget"
    );
    assert_eq!(usage(&db), (used, used));
}

#[test]
fn a_full_budget_that_is_fully_consumed_is_compacted_and_the_pass_completes() {
    let (home, db) = seeded(10);
    let (used, _) = usage(&db);
    cap_at(&db, used);
    consume_everything(&db);
    for index in 0..2 {
        write_codex_rollout(home.path(), &format!("new-{index}"));
    }
    assert!(
        sync(&db, home.path())
            .expect("compaction restores headroom")
            .swept
    );
    assert_eq!(codex_sessions(&db).len(), 12);
    let (after, limit) = usage(&db);
    assert_eq!(limit, used);
    assert!(
        after < used,
        "the consumed journal must have been reclaimed"
    );
}

#[test]
fn sessions_committed_before_a_mid_pass_stop_remain_persisted() {
    let (home, db) = seeded(1);
    let (one_session, _) = usage(&db);
    // Room for a few more sessions below the high-water mark, so the stop is
    // a capture trigger abort inside a session transaction rather than the
    // pre-session check.
    cap_at(&db, one_session * 9 / 2);
    for index in 0..8 {
        write_codex_rollout(home.path(), &format!("new-{index}"));
    }
    let error = sync(&db, home.path()).expect_err("the pass must stop at the cap");
    assert!(is_retention_limit(&error), "{error:#}");
    let reached = retention_limit_usage(&error).expect("typed usage");
    assert!(reached.used_bytes <= reached.limit_bytes);
    let sessions = codex_sessions(&db);
    assert!(
        sessions.len() > 1 && sessions.len() < 9,
        "some but not every session lands before the stop: {sessions:?}"
    );
    for session in &sessions {
        assert!(
            codex_event_count(&db, session) > 0,
            "{session} committed before the stop keeps its evidence"
        );
    }
}

#[test]
fn hydration_stops_at_a_full_unreclaimable_budget_and_resumes_once_compacted() {
    let (home, db) = seeded(3);
    let (used, _) = usage(&db);
    cap_at(&db, used);
    let rollout = write_codex_rollout(home.path(), "seed-01");
    append_codex_answer(&rollout, "one more turn");
    let error = hydrate(&db, home.path(), "seed-01").expect_err("hydration must stop at the cap");
    assert!(is_retention_limit(&error), "{error:#}");
    let reached = retention_limit_usage(&error).expect("typed usage");
    assert_eq!((reached.used_bytes, reached.limit_bytes), (used, used));

    consume_everything(&db);
    let result = hydrate(&db, home.path(), "seed-01").expect("compaction restores headroom");
    assert_ne!(result.status, "unchanged");
    assert!(usage(&db).0 < used);
}
