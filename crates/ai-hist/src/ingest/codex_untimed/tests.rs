//! A Codex rollout line without a usable time is never stored at 0.

use super::*;
use crate::init_db;
use rusqlite::Connection;
use std::fs;
use std::io::Write as _;

const SESSION: &str = "019da900-0000-7000-8000-0000000000aa";
const T0: &str = "2026-11-10T09:00:00.000Z";
const T1: &str = "2026-11-10T09:00:05.000Z";
const T2: &str = "2026-11-10T09:01:00.000Z";

fn iso(raw: &str) -> i64 {
    parse_iso_ms(raw).unwrap()
}

/// A rollout line; `ts` of `None` writes no `timestamp` at all.
fn line(ts: Option<&str>, kind: &str, payload: Value) -> String {
    let mut value = json!({"type": kind, "payload": payload});
    if let Some(ts) = ts {
        value["timestamp"] = json!(ts);
    }
    format!("{value}\n")
}

fn session_meta(ts: Option<&str>) -> String {
    line(
        ts,
        "session_meta",
        json!({"id": SESSION, "cwd": "/tmp/project", "timestamp": T0}),
    )
}

fn user(ts: Option<&str>, text: &str) -> String {
    line(
        ts,
        "event_msg",
        json!({"type": "user_message", "message": text}),
    )
}

fn agent(ts: Option<&str>, text: &str) -> String {
    line(
        ts,
        "event_msg",
        json!({"type": "agent_message", "message": text}),
    )
}

fn call(ts: Option<&str>, call_id: &str) -> String {
    line(
        ts,
        "response_item",
        json!({"type": "function_call", "name": "shell", "call_id": call_id,
               "arguments": "{\"command\":[\"ls\"]}"}),
    )
}

fn complete(ts: Option<&str>, turn: &str) -> String {
    line(
        ts,
        "event_msg",
        json!({"type": "task_complete", "turn_id": turn}),
    )
}

/// Every stored time of the session, by table.
fn stored_times(conn: &Connection, session: &str) -> Vec<(String, Option<i64>)> {
    let mut times = Vec::new();
    for (table, column) in [
        ("session_events", "ts_ms"),
        ("history", "timestamp_ms"),
        ("tool_calls", "ts_ms"),
        ("session_markers", "ts_ms"),
    ] {
        let mut statement = conn
            .prepare(&format!(
                "SELECT {column} FROM {table} WHERE source = 'codex' AND session_id = ? \
                 ORDER BY {column}"
            ))
            .unwrap();
        let rows = statement
            .query_map([session], |row| row.get::<_, Option<i64>>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        times.extend(rows.into_iter().map(|ts| (table.to_string(), ts)));
    }
    times
}

fn assert_no_epoch_rows(conn: &Connection, session: &str) {
    let times = stored_times(conn, session);
    assert!(
        times.iter().all(|(_, ts)| *ts != Some(0)),
        "a line without a timestamp was stored at 0: {times:?}"
    );
}

fn event_ts(conn: &Connection, text: &str) -> i64 {
    conn.query_row(
        "SELECT ts_ms FROM session_events WHERE source = 'codex' AND text = ?",
        [text],
        |row| row.get(0),
    )
    .unwrap()
}

fn open(path: &Path) -> (Connection, CodexSessionMeta) {
    let conn = Connection::open_in_memory().unwrap();
    init_db(&conn).unwrap();
    let meta = read_codex_session_meta(path).unwrap().unwrap();
    (conn, meta)
}

fn append(path: &Path, text: &str) {
    let mut file = fs::OpenOptions::new().append(true).open(path).unwrap();
    file.write_all(text.as_bytes()).unwrap();
}

/// A line with no `timestamp`, or one that does not parse, takes the time
/// of the line before it, in every table; timed lines keep their own.
#[test]
fn an_untimed_codex_line_inherits_the_preceding_time() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rollout-untimed.jsonl");
    let body = [
        session_meta(Some(T0)),
        user(Some(T1), "first prompt"),
        user(None, "untimed prompt"),
        agent(Some("not a time"), "unparseable answer"),
        call(None, "call_untimed"),
        user(Some(T2), "second prompt"),
        complete(None, "t1"),
    ]
    .concat();
    fs::write(&path, body).unwrap();
    let (conn, meta) = open(&path);

    let (outcome, diagnostics) =
        provider_diagnostics::collect(|| ingest_codex_rollout(&conn, &path, &meta));
    outcome.unwrap();

    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    assert_no_epoch_rows(&conn, SESSION);
    assert_eq!(event_ts(&conn, "first prompt"), iso(T1));
    assert_eq!(event_ts(&conn, "untimed prompt"), iso(T1));
    assert_eq!(event_ts(&conn, "unparseable answer"), iso(T1));
    assert_eq!(event_ts(&conn, "second prompt"), iso(T2));
    let tool_ts: i64 = conn
        .query_row(
            "SELECT ts_ms FROM tool_calls WHERE tool_use_id = 'call_untimed'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(tool_ts, iso(T1));
    let history_ts: i64 = conn
        .query_row(
            "SELECT timestamp_ms FROM history WHERE prompt = 'untimed prompt'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(history_ts, iso(T1));
}

/// The inherited time survives a pass boundary: a pass resuming after the
/// timed line stores what a read from zero stores.
#[test]
fn an_untimed_codex_line_read_on_a_later_pass_inherits_through_the_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rollout-resume.jsonl");
    let head = [
        session_meta(Some(T0)),
        user(Some(T1), "first prompt"),
        complete(Some(T1), "t1"),
    ]
    .concat();
    fs::write(&path, head).unwrap();
    let (conn, meta) = open(&path);
    let mut cursor = transcript_cursor::TranscriptCursorState::default();
    ingest_codex_rollout_incremental(&conn, &path, &meta, &mut cursor).unwrap();
    assert!(cursor.file.as_ref().is_some_and(|file| file.offset > 0));

    append(&path, &user(None, "appended prompt"));
    ingest_codex_rollout_incremental(&conn, &path, &meta, &mut cursor).unwrap();

    assert_no_epoch_rows(&conn, SESSION);
    assert_eq!(event_ts(&conn, "appended prompt"), iso(T1));
    let (fresh, _) = open(&path);
    ingest_codex_rollout(&fresh, &path, &meta).unwrap();
    assert_eq!(stored_times(&conn, SESSION), stored_times(&fresh, SESSION));
}

/// A cursor an earlier build saved has no line time, yet resumes past the
/// timed lines: the bytes it already read are folded once.
#[test]
fn an_untimed_codex_line_after_a_cursor_from_an_earlier_build_inherits_the_prefix_time() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rollout-legacy.jsonl");
    let head = [
        session_meta(Some(T0)),
        user(Some(T1), "first prompt"),
        complete(Some(T2), "t1"),
    ]
    .concat();
    fs::write(&path, head).unwrap();
    let (conn, meta) = open(&path);
    let mut cursor = transcript_cursor::TranscriptCursorState::default();
    ingest_codex_rollout_incremental(&conn, &path, &meta, &mut cursor).unwrap();
    // What an earlier build saved: neither field exists in its cursor.
    let mut saved = serde_json::to_value(&cursor).unwrap();
    let codex = saved["codex"].as_object_mut().unwrap();
    assert!(codex.remove("record_ts").is_some());
    assert!(codex.remove("record_ts_from_start").is_some());
    let mut cursor: transcript_cursor::TranscriptCursorState =
        serde_json::from_value(saved).unwrap();

    append(&path, &user(None, "appended prompt"));
    let (outcome, diagnostics) = provider_diagnostics::collect(|| {
        ingest_codex_rollout_incremental(&conn, &path, &meta, &mut cursor)
    });
    outcome.unwrap();

    assert!(diagnostics.is_empty(), "{diagnostics:?}");
    assert_no_epoch_rows(&conn, SESSION);
    // The `task_complete` stores only a marker, and its time is the latest.
    assert_eq!(event_ts(&conn, "appended prompt"), iso(T2));
    let (fresh, _) = open(&path);
    ingest_codex_rollout(&fresh, &path, &meta).unwrap();
    assert_eq!(stored_times(&conn, SESSION), stored_times(&fresh, SESSION));
}

/// With nothing to inherit, a line keeps a marker with no time, writes no
/// row at 0, retires what an earlier parser stored at 0 for it, and is
/// reported without its content.
#[test]
fn an_untimed_codex_line_with_nothing_to_inherit_is_a_counted_marker() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rollout-leading.jsonl");
    let body = [
        session_meta(None),
        user(None, "secret leading prompt"),
        user(Some(T1), "timed prompt"),
    ]
    .concat();
    fs::write(&path, body).unwrap();
    let (conn, meta) = open(&path);
    // What an earlier parser left for the leading prompt (line 1), and a row
    // of the same line already stored at a real time, which stays.
    conn.execute(
        "INSERT INTO history (source, session_id, project, prompt, timestamp_ms) \
         VALUES ('codex', ?1, '/tmp/project', 'secret leading prompt', 0)",
        [SESSION],
    )
    .unwrap();
    for (uid, ts) in [("1:user_message", 0), ("1:kept", 5)] {
        conn.execute(
            "INSERT INTO session_events (source, session_id, message_id, ts_ms, role, kind, \
               text, event_uid) VALUES ('codex', ?1, ?2, ?3, 'user', 'text', 'old', ?2)",
            params![SESSION, uid, ts],
        )
        .unwrap();
    }

    let (outcome, diagnostics) =
        provider_diagnostics::collect(|| ingest_codex_rollout(&conn, &path, &meta));
    outcome.unwrap();

    assert_no_epoch_rows(&conn, SESSION);
    let marker: (Option<i64>, String) = conn
        .query_row(
            "SELECT ts_ms, subkind FROM session_markers \
             WHERE source = 'codex' AND session_id = ? AND marker_uid = '1:marker'",
            [SESSION],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(marker, (None, "user_message".to_string()));
    let kept: i64 = conn
        .query_row(
            "SELECT ts_ms FROM session_events WHERE event_uid = '1:kept'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(kept, 5, "a row at a real time was retired");
    assert_eq!(event_ts(&conn, "timed prompt"), iso(T1));
    assert_eq!(diagnostics.len(), 1, "{diagnostics:?}");
    let reported = format!("{diagnostics:?}");
    assert!(reported.contains("EVIDENCE_PARTIAL"), "{reported}");
    assert!(reported.contains("user_message"), "{reported}");
    assert!(!reported.contains("secret"), "{reported}");
}

/// A fork's replayed prompt with no time is the one an earlier parser stored
/// at 0, and is retired at 0; the replay's marker inherits the child's time.
#[test]
fn an_untimed_replayed_fork_prompt_retires_the_row_stored_at_zero() {
    const PARENT: &str = "019da82f-d400-7000-8000-00000000000a";
    const CHILD: &str = "019da830-be60-7000-8000-00000000000c";
    // UUIDv7 turn ids: the gate places a turn before or after the fork by
    // its id's time.
    const PARENT_TURN: &str = "019da82f-d7e8-7000-8000-000000000001";
    const CHILD_TURN: &str = "019da830-d1e8-7000-8000-0000000000c1";
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rollout-fork.jsonl");
    let at = Some("2026-04-20T00:01:00.000Z");
    let own = Some("2026-04-20T00:01:05.000Z");
    let body = [
        line(
            at,
            "session_meta",
            json!({"id": CHILD, "forked_from_id": PARENT, "cwd": "/tmp/project",
                   "timestamp": "2026-04-20T00:01:00.000Z", "thread_source": "user"}),
        ),
        // The replayed parent history, written without times.
        line(
            None,
            "session_meta",
            json!({"id": PARENT, "cwd": "/tmp/project", "thread_source": "user"}),
        ),
        line(
            None,
            "event_msg",
            json!({"type": "task_started", "turn_id": PARENT_TURN}),
        ),
        user(None, "parent prompt"),
        complete(None, PARENT_TURN),
        line(
            own,
            "event_msg",
            json!({"type": "task_started", "turn_id": CHILD_TURN}),
        ),
        user(own, "child prompt"),
        complete(own, CHILD_TURN),
    ]
    .concat();
    fs::write(&path, body).unwrap();
    let (conn, meta) = open(&path);
    assert_eq!(meta.fork_parent_id.as_deref(), Some(PARENT));
    conn.execute(
        "INSERT INTO history (source, session_id, project, prompt, timestamp_ms) \
         VALUES ('codex', ?1, '/tmp/project', 'parent prompt', 0)",
        [CHILD],
    )
    .unwrap();

    ingest_codex_rollout(&conn, &path, &meta).unwrap();

    assert_no_epoch_rows(&conn, CHILD);
    let prompts: Vec<String> = conn
        .prepare("SELECT prompt FROM history WHERE source = 'codex' AND session_id = ?")
        .unwrap()
        .query_map([CHILD], |row| row.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(prompts, vec!["child prompt"]);
    let marker_ts: Option<i64> = conn
        .query_row(
            "SELECT ts_ms FROM session_markers WHERE source = 'codex' AND session_id = ? \
               AND kind = 'fork_replay_boundary'",
            [CHILD],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(marker_ts, Some(iso("2026-04-20T00:01:00.000Z")));
}

fn usage(total: u64) -> Value {
    json!({"type": "token_count", "info": {"total_token_usage": {
        "input_tokens": total, "cached_input_tokens": 0, "output_tokens": 0,
        "reasoning_output_tokens": 0, "total_tokens": total}}})
}

/// An untimed line that carries walk state still carries it: an opening
/// cumulative snapshot with no time is the baseline the next delta is
/// measured from, so the reply is charged its own request, not the total.
#[test]
fn an_untimed_opening_snapshot_still_seeds_the_usage_baseline() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rollout-baseline.jsonl");
    let body = [
        session_meta(None),
        line(
            None,
            "turn_context",
            json!({"turn_id": "t1", "cwd": "/tmp/project", "model": "gpt-5.4"}),
        ),
        line(None, "event_msg", usage(100)),
        agent(Some(T1), "timed answer"),
        line(Some(T2), "event_msg", usage(110)),
    ]
    .concat();
    fs::write(&path, body).unwrap();
    let (conn, meta) = open(&path);

    ingest_codex_rollout(&conn, &path, &meta).unwrap();

    assert_no_epoch_rows(&conn, SESSION);
    let total: i64 = conn
        .query_row(
            "SELECT json_extract(token_json, '$.total_tokens') FROM session_events \
             WHERE source = 'codex' AND text = 'timed answer'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(total, 10, "the reply is charged its own request");
    let (turn, model): (Option<String>, Option<String>) = conn
        .query_row(
            "SELECT turn_id, model FROM session_events \
             WHERE source = 'codex' AND text = 'timed answer'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        (turn.as_deref(), model.as_deref()),
        (Some("t1"), Some("gpt-5.4")),
        "the untimed turn_context still opens the turn"
    );
}

/// The prefix fold for a cursor from an earlier build takes a line's time on
/// the walk's terms: a line without an object payload is not observed by
/// either.
#[test]
fn the_legacy_prefix_fold_observes_times_as_the_walk_does() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rollout-legacy-payload.jsonl");
    let head = [
        session_meta(Some(T0)),
        user(Some(T1), "first prompt"),
        format!(
            "{}\n",
            json!({"timestamp": T2, "type": "event_msg", "payload": null})
        ),
        complete(None, "t1"),
    ]
    .concat();
    fs::write(&path, head).unwrap();
    let (conn, meta) = open(&path);
    let mut cursor = transcript_cursor::TranscriptCursorState::default();
    ingest_codex_rollout_incremental(&conn, &path, &meta, &mut cursor).unwrap();
    let mut saved = serde_json::to_value(&cursor).unwrap();
    let codex = saved["codex"].as_object_mut().unwrap();
    codex.remove("record_ts");
    codex.remove("record_ts_from_start");
    let mut cursor: transcript_cursor::TranscriptCursorState =
        serde_json::from_value(saved).unwrap();
    assert!(cursor.file.as_ref().is_some_and(|file| file.offset > 0));

    append(&path, &user(None, "appended prompt"));
    ingest_codex_rollout_incremental(&conn, &path, &meta, &mut cursor).unwrap();

    assert_eq!(event_ts(&conn, "appended prompt"), iso(T1));
    let (fresh, _) = open(&path);
    ingest_codex_rollout(&fresh, &path, &meta).unwrap();
    assert_eq!(stored_times(&conn, SESSION), stored_times(&fresh, SESSION));
}
