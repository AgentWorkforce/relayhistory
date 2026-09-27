#![cfg(all(feature = "export", feature = "unstable-internal"))]
//! Capture backpressure at the delivery retention cap.
//!
//! Fake provider homes and a temporary database only: no harness runs. A
//! whole-store subscription makes every evidence write journal a revision, so
//! the retention budget fills the way it does under a live delivery job.
//!
//! One `#[test]`, because a broad sweep resolves its provider roots from the
//! process environment and Rust runs a binary's tests concurrently: each
//! scenario points those roots at its own home, which siblings running beside
//! it would overwrite. Every scenario is a named function, so a failure names
//! itself.

use ai_hist::export::{self, capture, is_retention_limit, retention_limit_usage};
use ai_hist::{HydrateSessionOptions, SessionScope, SyncOutput};
use rusqlite::Connection;
use std::path::{Path, PathBuf};

/// The body every fake session carries, so one session's journal cost is
/// large next to a store's fixed first-sync overhead and a cap set from a
/// measured usage lands where a test means it to: below the high-water mark
/// with less than one session of room, or with room for exactly one more.
const SESSION_BODY_BYTES: usize = 32 * 1024;

fn session_body() -> String {
    "x".repeat(SESSION_BODY_BYTES)
}

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
        format_args!(
            "{{\"timestamp\":\"2026-09-19T10:00:02.000Z\",\"type\":\"event_msg\",\
             \"payload\":{{\"type\":\"agent_message\",\"message\":\"{}\"}}}}",
            session_body()
        ),
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

/// A fake OpenCode SQLite store where OpenCode keeps its own, with `part`
/// seekable by session so sync takes the per-session plan the live log
/// showed attempting every session. Every session carries the same prompt.
fn opencode_store(home: &Path) -> PathBuf {
    let path = home.join(".local/share/opencode/opencode.db");
    std::fs::create_dir_all(path.parent().expect("store dir")).expect("opencode dir");
    let src = Connection::open(&path).expect("open opencode store");
    src.execute_batch(
        "CREATE TABLE IF NOT EXISTS session (id TEXT PRIMARY KEY, directory TEXT, time_created INTEGER, time_updated INTEGER);
         CREATE TABLE IF NOT EXISTS message (id TEXT PRIMARY KEY, session_id TEXT, time_created INTEGER, data TEXT);
         CREATE TABLE IF NOT EXISTS part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT, time_created INTEGER, data TEXT);
         CREATE INDEX IF NOT EXISTS part_session ON part(session_id);",
    )
    .expect("opencode schema");
    path
}

fn write_opencode_session(home: &Path, session_id: &str) {
    let src = Connection::open(opencode_store(home)).expect("open opencode store");
    src.execute(
        "INSERT INTO session VALUES (?1, '/tmp/project', 1, 2)",
        [session_id],
    )
    .expect("session row");
    src.execute(
        "INSERT INTO message VALUES (?1, ?2, 1, '{\"role\":\"user\",\"modelID\":\"test-model\"}')",
        [&format!("{session_id}-m1"), session_id],
    )
    .expect("message row");
    src.execute(
        "INSERT INTO part VALUES (?1, ?2, ?3, 1, ?4)",
        [
            &format!("{session_id}-p1"),
            &format!("{session_id}-m1"),
            session_id,
            &format!("{{\"type\":\"text\",\"text\":\"{}\"}}", session_body()),
        ],
    )
    .expect("part row");
}

fn sync_opencode(db: &Path, home: &Path) -> anyhow::Result<bool> {
    ai_hist::sync_opencode_at(db, &opencode_store(home), SyncOutput::Silent)
}

/// Whether a capture trigger refused a write inside the pass, as opposed to
/// the high-water check stopping it before one.
fn refused_by_trigger(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<rusqlite::Error>(),
            Some(rusqlite::Error::SqliteFailure(_, Some(message)))
                if message.starts_with("delivery retention limit exceeded;")
        )
    })
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

fn sessions_of(db: &Path, source: &str) -> Vec<String> {
    let conn = ai_hist::open_db(db).expect("open db");
    let mut statement = conn
        .prepare("SELECT session_id FROM sessions WHERE source = ? ORDER BY session_id")
        .expect("prepare");
    statement
        .query_map([source], |row| row.get(0))
        .expect("query")
        .collect::<Result<Vec<String>, _>>()
        .expect("rows")
}

fn codex_sessions(db: &Path) -> Vec<String> {
    sessions_of(db, "codex")
}

/// Every Codex session id with evidence rows, whether or not it has a
/// session row: a rollout refused mid-file keeps the rows it committed.
fn codex_sessions_with_events(db: &Path) -> Vec<String> {
    let conn = ai_hist::open_db(db).expect("open db");
    let mut statement = conn
        .prepare(
            "SELECT DISTINCT session_id FROM session_events WHERE source = 'codex' ORDER BY session_id",
        )
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

/// Point the process's provider roots at one fake home. A sweep reads
/// `CODEX_HOME` and friends ahead of the home it is handed, so a host that has
/// them set would otherwise pull its real transcripts into these stores.
fn use_home(home: &Path) {
    std::env::set_var("HOME", home);
    std::env::set_var("USERPROFILE", home);
    std::env::set_var("XDG_DATA_HOME", home.join(".local/share"));
    std::env::set_var("CLAUDE_CONFIG_DIR", home.join(".claude"));
    std::env::set_var("CODEX_HOME", home.join(".codex"));
    std::env::set_var("GROK_HOME", home.join(".grok"));
    std::env::set_var(
        "OPENCODE_DB",
        home.join(".local/share/opencode/opencode.db"),
    );
    std::env::set_var(
        "OPENCODE_STORAGE_DIR",
        home.join(".local/share/opencode/storage"),
    );
    std::env::set_var("TRAJECTORY_ROOT", home.join("no-such-trajectories"));
    std::env::remove_var("AI_HIST_DB");
}

/// A fake home whose provider roots this process now resolves to.
fn isolated_home() -> tempfile::TempDir {
    let home = tempfile::tempdir().expect("home");
    use_home(home.path());
    home
}

/// A seeded store with `sessions` captured under a reader at cursor zero.
fn seeded(sessions: usize) -> (tempfile::TempDir, PathBuf) {
    let home = isolated_home();
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
fn capture_applies_backpressure_at_the_retention_cap() {
    a_full_budget_with_nothing_reclaimable_stops_the_pass_before_any_session();
    a_full_budget_that_is_fully_consumed_is_compacted_and_the_pass_completes();
    sessions_committed_before_a_mid_pass_stop_remain_persisted();
    opencode_sessions_are_not_attempted_at_a_full_unreclaimable_budget();
    an_opencode_session_refused_by_the_trigger_ends_the_pass();
    hydration_stops_at_a_full_unreclaimable_budget_and_resumes_once_compacted();
}

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

fn sessions_committed_before_a_mid_pass_stop_remain_persisted() {
    let (home, db) = seeded(1);
    let (one_session, _) = usage(&db);
    // Room for one more session and half of another: the pass is below the
    // high-water mark when it checks before the second, which the capture
    // trigger then refuses.
    cap_at(&db, one_session + (SESSION_BODY_BYTES as i64) * 3 / 2);
    for index in 0..8 {
        write_codex_rollout(home.path(), &format!("new-{index}"));
    }
    let error = sync(&db, home.path()).expect_err("the pass must stop at the cap");
    assert!(is_retention_limit(&error), "{error:#}");
    assert!(
        refused_by_trigger(&error),
        "the stop is the trigger's refusal, carried through the pass: {error:#}"
    );
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
    // Codex rollouts commit statement by statement, so the refused rollout
    // may keep the rows it wrote before the refusal; every rollout after it
    // was never attempted and has none.
    let orphaned: Vec<_> = codex_sessions_with_events(&db)
        .into_iter()
        .filter(|session| !sessions.contains(session))
        .collect();
    assert!(
        orphaned.len() <= 1,
        "only the refused rollout may hold rows without a session: {orphaned:?}"
    );
}

/// The live-log regression: a store of many OpenCode sessions at a full,
/// unreclaimable budget. The pass stops before the first session instead of
/// attempting each one and aggregating their refusals.
fn opencode_sessions_are_not_attempted_at_a_full_unreclaimable_budget() {
    let home = isolated_home();
    let db = home.path().join("history.db");
    subscribe(&db, 0);
    write_opencode_session(home.path(), "seed");
    assert!(sync_opencode(&db, home.path()).expect("seed sync"));
    let (used, _) = usage(&db);
    assert!(used > 0, "the seed must journal revisions");
    cap_at(&db, used);
    for index in 0..20 {
        write_opencode_session(home.path(), &format!("new-{index:02}"));
    }
    let error = sync_opencode(&db, home.path()).expect_err("the pass must stop at the cap");
    assert!(is_retention_limit(&error), "{error:#}");
    assert!(
        !refused_by_trigger(&error),
        "no session was attempted: {error:#}"
    );
    let reached = retention_limit_usage(&error).expect("typed usage");
    assert_eq!((reached.used_bytes, reached.limit_bytes), (used, used));
    let rendered = format!("{error:#}");
    assert!(
        !rendered.contains("could not be read"),
        "no per-session failure aggregate: {rendered}"
    );
    assert_eq!(sessions_of(&db, "opencode"), vec!["seed".to_string()]);
    assert_eq!(usage(&db), (used, used));
}

/// An OpenCode session the capture trigger refuses ends the pass with that
/// refusal: the session rolls back, the sessions after it are not attempted,
/// and nothing is aggregated.
fn an_opencode_session_refused_by_the_trigger_ends_the_pass() {
    let home = isolated_home();
    let db = home.path().join("history.db");
    subscribe(&db, 0);
    write_opencode_session(home.path(), "seed");
    assert!(sync_opencode(&db, home.path()).expect("seed sync"));
    let (one_session, _) = usage(&db);
    // Below the high-water mark with half a session of room.
    let limit = one_session + (SESSION_BODY_BYTES as i64) / 2;
    cap_at(&db, limit);
    for index in 0..20 {
        write_opencode_session(home.path(), &format!("new-{index:02}"));
    }
    let error = sync_opencode(&db, home.path()).expect_err("the pass must stop at the cap");
    assert!(is_retention_limit(&error), "{error:#}");
    assert!(refused_by_trigger(&error), "{error:#}");
    let reached = retention_limit_usage(&error).expect("typed usage");
    assert_eq!(reached.limit_bytes, limit);
    assert!(reached.used_bytes <= reached.limit_bytes);
    let rendered = format!("{error:#}");
    assert!(
        !rendered.contains("could not be read"),
        "no per-session failure aggregate: {rendered}"
    );
    assert_eq!(
        sessions_of(&db, "opencode"),
        vec!["seed".to_string()],
        "the refused session rolled back and no later session was attempted"
    );
    assert_eq!(usage(&db).0, one_session);
}

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
