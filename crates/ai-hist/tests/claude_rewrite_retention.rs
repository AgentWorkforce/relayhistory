//! Provider files are not the source of truth for what was already observed.
//!
//! Claude Code rewrites a transcript in place when a session is resumed or
//! compacted: assistant turns the old file contained can be absent from the
//! new file. A re-parse therefore upserts what the file still contains and
//! never deletes what it no longer contains. Only a targeted heal that names
//! exact rows (sidechain re-attribution) may remove evidence.
//!
//! The only test in this binary: it sets `HOME` for the process, which is
//! safe exactly because nothing else here runs beside it.

use ai_hist::{
    discover_sessions_scoped_at, hydrate_session_at, open_db, sync_scoped_at, DiscoverOptions,
    HydrateSessionOptions, SessionScope,
};
use rusqlite::{Connection, OptionalExtension};
use std::fs;
use std::path::Path;

fn write(path: &Path, contents: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, contents).unwrap();
}

/// Overwrite a provider file without replacing it, then force its stamp
/// forward so the change is visible even when the new content has the same
/// byte length. `fs::write` truncates in place; the explicit mtime keeps the
/// test deterministic on filesystems with coarse timestamp granularity.
fn rewrite_in_place(path: &Path, contents: &str, mtime_bump_secs: u64) {
    fs::write(path, contents).unwrap();
    let mtime = std::time::SystemTime::now() + std::time::Duration::from_secs(mtime_bump_secs);
    fs::File::options()
        .write(true)
        .open(path)
        .unwrap()
        .set_modified(mtime)
        .unwrap();
}

fn count(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |row| row.get(0)).unwrap()
}

fn event_text(conn: &Connection, source: &str, session: &str, event_uid: &str) -> Option<String> {
    conn.query_row(
        "SELECT text FROM session_events WHERE source = ? AND session_id = ? AND event_uid = ?",
        [source, session, event_uid],
        |row| row.get(0),
    )
    .optional()
    .unwrap()
}

fn history_prompts(conn: &Connection, source: &str, session: &str) -> Vec<String> {
    let mut statement = conn
        .prepare("SELECT prompt FROM history WHERE source = ? AND session_id = ? ORDER BY rowid")
        .unwrap();
    statement
        .query_map([source, session], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

fn user_row(session: &str, uuid: &str, text: &str, ts: &str) -> String {
    format!(
        "{{\"sessionId\":\"{session}\",\"uuid\":\"{uuid}\",\"cwd\":\"/work/app\",\"type\":\"user\",\
         \"message\":{{\"role\":\"user\",\"content\":{text}}},\"timestamp\":\"{ts}\"}}\n",
        text = serde_json::Value::String(text.to_string())
    )
}

fn assistant_text_row(session: &str, uuid: &str, text: &str, usage: &str, ts: &str) -> String {
    format!(
        "{{\"sessionId\":\"{session}\",\"uuid\":\"{uuid}\",\"cwd\":\"/work/app\",\"type\":\"assistant\",\
         \"message\":{{\"role\":\"assistant\",\"model\":\"opus\",\"content\":[{{\
         \"type\":\"text\",\"text\":{text}}}],\"usage\":{usage}}},\"timestamp\":\"{ts}\"}}\n",
        text = serde_json::Value::String(text.to_string())
    )
}

fn assistant_tool_row(
    session: &str,
    uuid: &str,
    tool_use_id: &str,
    name: &str,
    input: &str,
    ts: &str,
) -> String {
    format!(
        "{{\"sessionId\":\"{session}\",\"uuid\":\"{uuid}\",\"cwd\":\"/work/app\",\"type\":\"assistant\",\
         \"message\":{{\"role\":\"assistant\",\"model\":\"opus\",\"content\":[{{\
         \"type\":\"tool_use\",\"id\":\"{tool_use_id}\",\"name\":\"{name}\",\"input\":{input}}}]}},\
         \"timestamp\":\"{ts}\"}}\n"
    )
}

fn tool_result_row(session: &str, uuid: &str, tool_use_id: &str, result: &str, ts: &str) -> String {
    format!(
        "{{\"sessionId\":\"{session}\",\"uuid\":\"{uuid}\",\"cwd\":\"/work/app\",\"type\":\"user\",\
         \"message\":{{\"role\":\"user\",\"content\":[{{\
         \"type\":\"tool_result\",\"tool_use_id\":\"{tool_use_id}\",\"content\":{result}}}]}},\
         \"timestamp\":\"{ts}\"}}\n",
        result = serde_json::Value::String(result.to_string())
    )
}

fn summary_row(session: &str, leaf_uuid: &str) -> String {
    format!(
        "{{\"type\":\"summary\",\"sessionId\":\"{session}\",\
         \"summary\":\"context compacted on resume\",\"leafUuid\":\"{leaf_uuid}\"}}\n"
    )
}

/// Full pre-compaction transcript: two user turns, two assistant answers (the
/// first carrying token usage), and one file edit with its result.
///
/// The prompts are per-scenario: `history` dedups on
/// `(source, timestamp_ms, prompt)` across sessions, so scenarios sharing one
/// HOME must not reuse the same prompt at the same timestamp.
fn full_transcript(session: &str, first_q: &str, second_q: &str, second_answer: &str) -> String {
    format!(
        "{}{}{}{}{}{}",
        user_row(session, "u1", first_q, "2026-09-18T10:00:00Z"),
        assistant_text_row(
            session,
            "a1",
            "first answer",
            r#"{"input_tokens":10,"output_tokens":20}"#,
            "2026-09-18T10:00:01Z"
        ),
        assistant_tool_row(
            session,
            "t1",
            "toolu_1",
            "Edit",
            r#"{"file_path":"/work/app/notes.txt"}"#,
            "2026-09-18T10:00:02Z"
        ),
        tool_result_row(
            session,
            "r1",
            "toolu_1",
            "edit applied",
            "2026-09-18T10:00:03Z"
        ),
        user_row(session, "u2", second_q, "2026-09-18T10:00:04Z"),
        assistant_text_row(
            session,
            "a2",
            second_answer,
            r#"{"input_tokens":5,"output_tokens":7}"#,
            "2026-09-18T10:00:05Z"
        ),
    )
}

/// What the provider file looks like after resume/compact: a summary marker
/// plus only the turns still present in the live file.
fn compacted_transcript(session: &str, second_q: &str, second_answer: &str) -> String {
    format!(
        "{}{}{}",
        summary_row(session, "u2"),
        user_row(session, "u2", second_q, "2026-09-18T10:00:04Z"),
        assistant_text_row(
            session,
            "a2",
            second_answer,
            r#"{"input_tokens":5,"output_tokens":7}"#,
            "2026-09-18T10:00:05Z"
        ),
    )
}

fn assert_full_claude_evidence(conn: &Connection, session: &str, first_q: &str, second_q: &str) {
    assert_eq!(
        history_prompts(conn, "claude", session),
        vec![first_q.to_string(), second_q.to_string()],
        "both human prompts are history rows"
    );
    assert_eq!(
        count(
            conn,
            &format!(
                "SELECT COUNT(*) FROM session_events WHERE source = 'claude' AND session_id = '{session}'"
            )
        ),
        6,
        "u1, a1, t1, r1, u2, a2 each produce one event"
    );
    assert_eq!(
        event_text(conn, "claude", session, "a1:0").as_deref(),
        Some("first answer")
    );
    let token_json: Option<String> = conn
        .query_row(
            "SELECT token_json FROM session_events \
             WHERE source = 'claude' AND session_id = ? AND event_uid = 'a1:0'",
            [session],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        token_json.is_some_and(|value| !value.is_empty()),
        "assistant usage survives ingestion"
    );
    assert_eq!(
        count(
            conn,
            &format!(
                "SELECT COUNT(*) FROM tool_calls WHERE source = 'claude' AND session_id = '{session}'"
            )
        ),
        1,
        "the file edit's tool call is recorded"
    );
    assert_eq!(
        count(
            conn,
            &format!(
                "SELECT COUNT(*) FROM file_edits WHERE source = 'claude' AND session_id = '{session}'"
            )
        ),
        1,
        "the file edit is recorded"
    );
    let last_activity: Option<i64> = conn
        .query_row(
            "SELECT last_activity_ms FROM sessions WHERE source = 'claude' AND session_id = ?",
            [session],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        last_activity.is_some_and(|value| value > 0),
        "the session carries activity"
    );
}

fn assert_compacted_claude_evidence(
    conn: &Connection,
    session: &str,
    first_q: &str,
    second_q: &str,
    second_answer: &str,
) {
    // The guard: a re-parse of the same raw path never shrinks the session.
    // Only a heal naming exact rows (sidechain re-attribution) may delete.
    assert_eq!(
        history_prompts(conn, "claude", session),
        vec![first_q.to_string(), second_q.to_string()],
        "the compacted-away first prompt is retained"
    );
    assert_eq!(
        count(
            conn,
            &format!(
                "SELECT COUNT(*) FROM session_events WHERE source = 'claude' AND session_id = '{session}'"
            )
        ),
        6,
        "no event is lost and none is duplicated"
    );
    assert_eq!(
        event_text(conn, "claude", session, "u1:0").as_deref(),
        Some(first_q),
        "the compacted-away user turn is retained"
    );
    assert_eq!(
        event_text(conn, "claude", session, "a1:0").as_deref(),
        Some("first answer"),
        "the compacted-away assistant turn is retained"
    );
    assert!(
        event_text(conn, "claude", session, "t1:0").is_some(),
        "the compacted-away tool use is retained"
    );
    assert_eq!(
        event_text(conn, "claude", session, "r1:0").as_deref(),
        Some("edit applied"),
        "the compacted-away tool result is retained"
    );
    assert_eq!(
        event_text(conn, "claude", session, "a2:0").as_deref(),
        Some(second_answer),
        "the surviving turn is updated in place, not duplicated"
    );
    assert_eq!(
        count(
            conn,
            &format!(
                "SELECT COUNT(*) FROM tool_calls WHERE source = 'claude' AND session_id = '{session}'"
            )
        ),
        1
    );
    assert_eq!(
        count(
            conn,
            &format!(
                "SELECT COUNT(*) FROM file_edits WHERE source = 'claude' AND session_id = '{session}'"
            )
        ),
        1
    );
}

fn sync(session_db: &Path) {
    sync_scoped_at(session_db, SessionScope::Local).unwrap();
}

fn discover(session_db: &Path) {
    discover_sessions_scoped_at(
        session_db,
        &DiscoverOptions {
            scope: SessionScope::Local,
            sources: Vec::new(),
            limit: None,
        },
    )
    .unwrap();
}

fn claude_sync_retention(home: &Path) {
    let folder = home.join("db-sync");
    fs::create_dir_all(&folder).unwrap();
    let db = folder.join("history.db");
    let transcript = home.join(".claude/projects/app/rewrite.jsonl");
    write(
        &transcript,
        &full_transcript(
            "rewrite",
            "first question sync",
            "second question sync",
            "second answer",
        ),
    );
    sync(&db);

    let conn = open_db(&db).unwrap();
    assert_full_claude_evidence(
        &conn,
        "rewrite",
        "first question sync",
        "second question sync",
    );
    drop(conn);

    // Compact in place: smaller file, newer mtime. Everything the live file
    // no longer contains must survive the re-parse.
    rewrite_in_place(
        &transcript,
        &compacted_transcript("rewrite", "second question sync", "second answer revised"),
        60,
    );
    sync(&db);

    let conn = open_db(&db).unwrap();
    assert_compacted_claude_evidence(
        &conn,
        "rewrite",
        "first question sync",
        "second question sync",
        "second answer revised",
    );
    // A third sync over the unchanged compacted file is a no-op.
    drop(conn);
    sync(&db);
    let conn = open_db(&db).unwrap();
    assert_compacted_claude_evidence(
        &conn,
        "rewrite",
        "first question sync",
        "second question sync",
        "second answer revised",
    );
}

fn claude_hydrate_retention(home: &Path) {
    let folder = home.join("db-hydrate");
    fs::create_dir_all(&folder).unwrap();
    let db = folder.join("history.db");
    let transcript = home.join(".claude/projects/app/rewrite-hydrate.jsonl");
    write(
        &transcript,
        &full_transcript(
            "rewrite-hydrate",
            "first question hydrate",
            "second question hydrate",
            "second answer",
        ),
    );
    sync(&db);
    discover(&db);

    let options = HydrateSessionOptions {
        source: "claude".into(),
        session_id: "rewrite-hydrate".into(),
        scope: SessionScope::Local,
        include_related: false,
    };
    let first = hydrate_session_at(&db, &options).unwrap();
    assert_ne!(first.status, "unchanged");
    let conn = open_db(&db).unwrap();
    assert_full_claude_evidence(
        &conn,
        "rewrite-hydrate",
        "first question hydrate",
        "second question hydrate",
    );
    drop(conn);

    rewrite_in_place(
        &transcript,
        &compacted_transcript(
            "rewrite-hydrate",
            "second question hydrate",
            "second answer revised",
        ),
        120,
    );
    let second = hydrate_session_at(&db, &options).unwrap();
    assert_eq!(second.status, "updated");
    let conn = open_db(&db).unwrap();
    assert_compacted_claude_evidence(
        &conn,
        "rewrite-hydrate",
        "first question hydrate",
        "second question hydrate",
        "second answer revised",
    );
}

fn claude_mtime_only_rewrite_retention(home: &Path) {
    let folder = home.join("db-mtime");
    fs::create_dir_all(&folder).unwrap();
    let db = folder.join("history.db");
    let transcript = home.join(".claude/projects/app/rewrite-mtime.jsonl");
    let original = full_transcript(
        "rewrite-mtime",
        "first question mtime",
        "second question mtime",
        "second answer",
    );
    write(&transcript, &original);
    sync(&db);
    let conn = open_db(&db).unwrap();
    assert_full_claude_evidence(
        &conn,
        "rewrite-mtime",
        "first question mtime",
        "second question mtime",
    );
    drop(conn);

    // Same byte length, newer mtime only: the stamp still catches the change.
    let mut compacted = compacted_transcript(
        "rewrite-mtime",
        "second question mtime",
        "second answer revised",
    );
    let padding = original.len().saturating_sub(compacted.len());
    assert!(
        padding > 0,
        "the fixture needs room for size-equalizing padding"
    );
    compacted.push_str(&" ".repeat(padding - 1));
    compacted.push('\n');
    assert_eq!(compacted.len(), original.len());
    rewrite_in_place(&transcript, &compacted, 180);
    assert_eq!(
        fs::metadata(&transcript).unwrap().len() as usize,
        original.len()
    );
    sync(&db);

    let conn = open_db(&db).unwrap();
    assert_compacted_claude_evidence(
        &conn,
        "rewrite-mtime",
        "first question mtime",
        "second question mtime",
        "second answer revised",
    );
}

fn codex_rollout(session: &str, second_answer: &str) -> String {
    format!(
        "{{\"timestamp\":\"2026-09-18T10:00:00Z\",\"type\":\"session_meta\",\
         \"payload\":{{\"id\":\"{session}\",\"cwd\":\"/work/app\"}}}}\n\
         {{\"timestamp\":\"2026-09-18T10:00:01Z\",\"type\":\"event_msg\",\
         \"payload\":{{\"type\":\"user_message\",\"message\":\"first request\"}}}}\n\
         {{\"timestamp\":\"2026-09-18T10:00:02Z\",\"type\":\"event_msg\",\
         \"payload\":{{\"type\":\"agent_message\",\"message\":\"first answer\"}}}}\n\
         {{\"timestamp\":\"2026-09-18T10:00:03Z\",\"type\":\"event_msg\",\
         \"payload\":{{\"type\":\"user_message\",\"message\":\"second request\"}}}}\n\
         {{\"timestamp\":\"2026-09-18T10:00:04Z\",\"type\":\"event_msg\",\
         \"payload\":{{\"type\":\"agent_message\",\"message\":\"{second_answer}\"}}}}\n"
    )
}

/// The relocated rollout: every observed line is still present, plus a
/// compaction marker the parser ignores today (mirroring the Claude summary
/// row). Codex rollout event identity is the line position, so only the
/// relocation shape is pinned here: the move changes the stamp-map key, the
/// file is re-ingested under the same session id, and the upsert must neither
/// lose nor duplicate what was already observed.
fn archived_codex_rollout(session: &str) -> String {
    format!(
        "{}{{\"timestamp\":\"2026-09-18T10:00:05Z\",\"type\":\"compacted\",\
         \"payload\":{{\"id\":\"{session}\"}}}}\n",
        codex_rollout(session, "second answer"),
    )
}

fn codex_event_count(conn: &Connection, session: &str) -> i64 {
    count(
        conn,
        &format!(
            "SELECT COUNT(*) FROM session_events WHERE source = 'codex' AND session_id = '{session}'"
        ),
    )
}

fn codex_has_text(conn: &Connection, session: &str, needle: &str) -> bool {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM session_events \
         WHERE source = 'codex' AND session_id = ? AND text LIKE '%' || ? || '%')",
        [session, needle],
        |row| row.get(0),
    )
    .unwrap()
}

fn assert_codex_retention(conn: &Connection, before: i64) {
    let after = codex_event_count(conn, "compact");
    assert_eq!(
        after, before,
        "relocating a rollout re-ingests without loss or duplication"
    );
    assert!(
        codex_has_text(conn, "compact", "first answer"),
        "the relocated codex turn is retained"
    );
    assert!(
        codex_has_text(conn, "compact", "second answer"),
        "the relocated codex turn is retained"
    );
    let prompts = history_prompts(conn, "codex", "compact");
    assert!(
        prompts.contains(&"first request".to_string()),
        "the relocated codex prompt is retained"
    );
    assert!(
        prompts.contains(&"second request".to_string()),
        "the relocated codex prompt is retained"
    );
}

fn codex_rewrite_retention(home: &Path) {
    let folder = home.join("db-codex");
    fs::create_dir_all(&folder).unwrap();
    let db = folder.join("history.db");
    let rollout = home.join(".codex/sessions/2026/09/20/rollout-compact.jsonl");
    write(&rollout, &codex_rollout("compact", "second answer"));
    sync(&db);

    let conn = open_db(&db).unwrap();
    assert!(codex_has_text(&conn, "compact", "first answer"));
    assert!(codex_has_text(&conn, "compact", "second answer"));
    let before = codex_event_count(&conn, "compact");
    assert!(before >= 4);
    drop(conn);

    // Move the file into the archive the way a compaction event relocates
    // it: the new path is a new stamp-map key, so the file is re-ingested
    // under the same session id. The re-parse must neither drop nor duplicate
    // what was already observed.
    let archived = home.join(".codex/archived_sessions/2026/09/20/rollout-compact.jsonl");
    fs::create_dir_all(archived.parent().unwrap()).unwrap();
    fs::write(&archived, archived_codex_rollout("compact")).unwrap();
    fs::remove_file(&rollout).unwrap();
    sync(&db);
    let conn = open_db(&db).unwrap();
    assert_codex_retention(&conn, before);
}

#[test]
fn in_place_rewrites_retain_already_ingested_evidence() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path();
    std::env::set_var("HOME", home);
    std::env::set_var("USERPROFILE", home);
    std::env::set_var("XDG_DATA_HOME", home.join("xdg"));
    std::env::set_var("OPENCODE_DB", home.join("missing-opencode.db"));
    std::env::set_var("TRAJECTORY_ROOT", home.join("missing-trajectories"));
    std::env::remove_var("AI_HIST_DB");
    claude_sync_retention(home);
    claude_hydrate_retention(home);
    claude_mtime_only_rewrite_retention(home);
    codex_rewrite_retention(home);
}
