use std::path::Path;

use rusqlite::Connection;
use serde_json::{json, Map, Value};

use super::test_support::*;
use super::*;

const START: i64 = 1_795_042_000_000;

fn records(composer: &Composer) -> Vec<Value> {
    composer
        .transcript
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn one_conversation(home: &Path, id: &str) -> PathBuf {
    let (path, conn) = open_state_db(home).unwrap();
    write_composer(
        &conn,
        id,
        "Fix the note",
        "/work/app",
        &tool_conversation("read note.txt", START),
        START - 5_000,
    )
    .unwrap();
    close_idle(&path, conn).unwrap();
    path
}

#[test]
fn a_locator_round_trips_and_names_no_file() {
    let db = Path::new("/home/me/.config/Cursor/User/globalStorage/state.vscdb");
    let locator = locator(db, "c-1");
    assert_eq!(parse_locator(&locator), Some((db.to_path_buf(), "c-1")));
    assert!(is_locator(Path::new(&locator)));
    assert_eq!(parse_locator("/tmp/state.vscdb"), None);
    assert_eq!(parse_locator("cursor-editor:/tmp/state.vscdb#"), None);
}

#[test]
fn a_composer_renders_as_the_transcript_cursor_would_write() {
    let dir = tempfile::tempdir().unwrap();
    let db = one_conversation(dir.path(), "c-1");
    let composer = read_composer(&db, "c-1").unwrap().unwrap();
    assert_eq!(composer.session_id, "c-1");
    assert_eq!(composer.title.as_deref(), Some("Fix the note"));
    assert_eq!(composer.cwd.as_deref(), Some("/work/app"));
    assert_eq!(composer.created_ms, Some(START - 5_000));
    assert_eq!(composer.updated_ms, Some(START + 4_000));
    assert_eq!(composer.skipped_bubbles, 0);
    let records = records(&composer);
    // prompt, thinking, read call + result, edit call + result, reply
    assert_eq!(records.len(), 7, "{records:#?}");
    assert_eq!(records[0]["role"], "user");
    assert_eq!(records[0]["timestamp"], START);
    assert_eq!(records[0]["message"]["content"][0]["text"], "read note.txt");
    assert_eq!(records[0]["message"]["id"], "cursor-bubble:b-user");
    assert_eq!(records[1]["message"]["content"][0]["type"], "thinking");
    assert_eq!(records[2]["message"]["content"][0]["type"], "tool_use");
    assert_eq!(records[2]["message"]["content"][0]["id"], "call-1");
    assert_eq!(records[2]["message"]["content"][0]["name"], "read_file_v2");
    assert_eq!(
        records[2]["message"]["content"][0]["input"]["targetFile"],
        "/work/app/note.txt"
    );
    assert_eq!(records[3]["role"], "user");
    assert_eq!(records[3]["message"]["content"][0]["type"], "tool_result");
    assert_eq!(records[3]["message"]["content"][0]["tool_use_id"], "call-1");
    assert_eq!(records[3]["timestamp"], START + 2_500);
    assert_eq!(
        records[6]["message"]["content"][0]["text"],
        "note.txt says hello"
    );
}

#[test]
fn an_idle_database_is_read_without_writing_beside_it() {
    let dir = tempfile::tempdir().unwrap();
    let db = one_conversation(dir.path(), "c-1");
    read_composer(&db, "c-1").unwrap().unwrap();
    composer_heads(&db).unwrap();
    let mut left: Vec<String> = std::fs::read_dir(db.parent().unwrap())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    left.sort();
    assert_eq!(left, vec!["state.vscdb".to_string()]);
}

#[test]
fn a_live_database_is_read_through_its_wal() {
    let dir = tempfile::tempdir().unwrap();
    let (db, conn) = open_state_db(dir.path()).unwrap();
    write_composer(
        &conn,
        "c-1",
        "Live",
        "/w",
        &tool_conversation("hi", START),
        START,
    )
    .unwrap();
    // The writer stays open, as Cursor does while it runs.
    assert!(crate::ingest::cursor_store::sidecar(&db, "-wal").exists());
    let composer = read_composer(&db, "c-1").unwrap().unwrap();
    assert_eq!(composer.records, 7);
    drop(conn);
}

#[test]
fn composers_without_a_conversation_are_not_offered() {
    let dir = tempfile::tempdir().unwrap();
    let (db, conn) = open_state_db(dir.path()).unwrap();
    write_composer(&conn, "empty", "", "/w", &[], START).unwrap();
    write_composer(
        &conn,
        "full",
        "",
        "/w",
        &tool_conversation("hi", START),
        START,
    )
    .unwrap();
    put(
        &conn,
        "composerData:empty-state-draft",
        &json!({"_v": 18, "isDraft": true}),
    )
    .unwrap();
    close_idle(&db, conn).unwrap();
    let heads = composers(dir.path(), None).unwrap();
    assert_eq!(
        heads
            .iter()
            .map(|h| h.session_id.as_str())
            .collect::<Vec<_>>(),
        vec!["full"]
    );
    assert_eq!(heads[0].recency_ms, Some(START + 4_000));
    assert!(read_composer(&db, "empty").unwrap().is_none());
    assert!(read_composer(&db, "missing").unwrap().is_none());
}

#[test]
fn a_composer_with_a_transcript_or_a_chat_store_is_left_to_it() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let (db, conn) = open_state_db(home).unwrap();
    for id in ["with-transcript", "with-store", "empty-dir", "editor-only"] {
        write_composer(&conn, id, "", "/w", &tool_conversation("hi", START), START).unwrap();
    }
    close_idle(&db, conn).unwrap();
    let transcript =
        home.join(".cursor/projects/w/agent-transcripts/with-transcript/with-transcript.jsonl");
    std::fs::create_dir_all(transcript.parent().unwrap()).unwrap();
    std::fs::write(&transcript, "{}\n").unwrap();
    // The folder Cursor creates and leaves empty is not a transcript.
    std::fs::create_dir_all(home.join(".cursor/projects/w/agent-transcripts/empty-dir")).unwrap();
    crate::ingest::cursor_store::test_support::write_store(
        home,
        "ws",
        "with-store",
        "/w",
        &crate::ingest::cursor_store::test_support::tool_chat("a"),
        5,
    )
    .unwrap();
    let mut ids: Vec<String> = composers(home, None)
        .unwrap()
        .into_iter()
        .map(|h| h.session_id)
        .collect();
    ids.sort();
    assert_eq!(
        ids,
        vec!["editor-only".to_string(), "empty-dir".to_string()]
    );
    assert!(superseded(home, "with-store").unwrap());
    assert!(!superseded(home, "empty-dir").unwrap());
}

#[test]
fn the_header_chunks_decide_the_order_and_unlisted_bubbles_are_left_out() {
    let dir = tempfile::tempdir().unwrap();
    let (db, conn) = open_state_db(dir.path()).unwrap();
    let bubbles = [
        user_bubble("u1", "first", START + 10_000),
        reply_bubble("a1", "answer", START + 5_000),
        user_bubble("u2", "second", START + 20_000),
    ];
    write_composer(&conn, "c", "", "/w", &bubbles, START).unwrap();
    // A bubble from an abandoned branch: stored, not in the conversation.
    put(
        &conn,
        "bubbleId:c:stray",
        &reply_bubble("stray", "abandoned", START),
    )
    .unwrap();
    close_idle(&db, conn).unwrap();
    let composer = read_composer(&db, "c").unwrap().unwrap();
    assert_eq!(texts(&composer), vec!["first", "answer", "second"]);
    assert!(!composer.incomplete);
}

fn texts(composer: &Composer) -> Vec<String> {
    records(composer)
        .iter()
        .map(|r| {
            r["message"]["content"][0]["text"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect()
}

/// Two chunks declared, three present: the third is an older generation's,
/// holding a valid bubble the conversation no longer has. It is never read.
#[test]
fn a_chunk_beyond_the_declared_header_log_is_not_read() {
    let dir = tempfile::tempdir().unwrap();
    let (db, conn) = open_state_db(dir.path()).unwrap();
    let bubbles = [
        user_bubble("u1", "first", START),
        reply_bubble("a1", "answer", START + 1),
    ];
    write_composer(&conn, "c", "", "/w", &bubbles, START).unwrap();
    put(
        &conn,
        "bubbleId:c:u2",
        &user_bubble("u2", "second", START + 2),
    )
    .unwrap();
    put(
        &conn,
        "composerConvHeaders:c:000001",
        &json!([{"bubbleId": "u2", "type": 1}]),
    )
    .unwrap();
    put(
        &conn,
        "bubbleId:c:old",
        &user_bubble("old", "abandoned turn", START + 3),
    )
    .unwrap();
    put(
        &conn,
        "composerConvHeaders:c:000002",
        &json!([{"bubbleId": "old", "type": 1}]),
    )
    .unwrap();
    set_header_log(&conn, "c", 2, 3);
    close_idle(&db, conn).unwrap();
    let composer = read_composer(&db, "c").unwrap().unwrap();
    assert_eq!(texts(&composer), vec!["first", "answer", "second"]);
    assert!(!composer.incomplete);
    // The declared count bounds the headers too: a longer chunk is cut to it.
    let (_, conn) = open_state_db(dir.path()).unwrap();
    set_header_log(&conn, "c", 2, 2);
    close_idle(&db, conn).unwrap();
    assert_eq!(
        texts(&read_composer(&db, "c").unwrap().unwrap()),
        vec!["first", "answer"]
    );
}

/// Rewrites the composer's `headerLog` to declare `chunks` chunks and `count`
/// headers.
fn set_header_log(conn: &Connection, id: &str, chunks: u64, count: u64) {
    let key = format!("composerData:{id}");
    let value: String = conn
        .query_row(
            "SELECT value FROM cursorDiskKV WHERE key = ?1",
            [&key],
            |row| row.get(0),
        )
        .unwrap();
    let mut data: Value = serde_json::from_str(&value).unwrap();
    data["headerLog"] = json!({"chunkSize": 256, "chunkCount": chunks, "count": count});
    put(conn, &key, &data).unwrap();
}

#[test]
fn a_missing_or_malformed_declared_chunk_makes_the_read_incomplete() {
    for damage in ["missing", "malformed", "short"] {
        let dir = tempfile::tempdir().unwrap();
        let (db, conn) = open_state_db(dir.path()).unwrap();
        let bubbles = [
            user_bubble("u1", "first", START),
            reply_bubble("a1", "answer", START + 1),
        ];
        write_composer(&conn, "c", "", "/w", &bubbles, START).unwrap();
        match damage {
            // Two chunks declared, the second never written.
            "missing" => set_header_log(&conn, "c", 2, 3),
            "malformed" => {
                put(
                    &conn,
                    "composerConvHeaders:c:000001",
                    &json!({"not": "headers"}),
                )
                .unwrap();
                set_header_log(&conn, "c", 2, 3);
            }
            // One chunk, but fewer headers in it than the log counts.
            _ => set_header_log(&conn, "c", 1, 5),
        }
        close_idle(&db, conn).unwrap();
        let composer = read_composer(&db, "c").unwrap().unwrap();
        assert!(composer.incomplete, "{damage}");
        assert_eq!(texts(&composer), vec!["first", "answer"], "{damage}");
    }
}

#[test]
fn a_listed_bubble_that_is_gone_makes_the_read_incomplete() {
    let dir = tempfile::tempdir().unwrap();
    let (db, conn) = open_state_db(dir.path()).unwrap();
    write_composer(
        &conn,
        "c",
        "",
        "/w",
        &[user_bubble("u1", "first", START)],
        START,
    )
    .unwrap();
    put(
        &conn,
        "composerConvHeaders:c:000000",
        &json!([{"bubbleId": "u1"}, {"bubbleId": "gone"}]),
    )
    .unwrap();
    set_header_log(&conn, "c", 1, 2);
    close_idle(&db, conn).unwrap();
    let composer = read_composer(&db, "c").unwrap().unwrap();
    assert_eq!(composer.skipped_bubbles, 1);
    assert!(composer.incomplete);
}

/// An incomplete read is indexed as far as it goes, but is not recorded as
/// read: no stamp is saved, the sweep is told, and the next pass reads it
/// again. A complete one beside it is unaffected.
#[test]
fn sync_does_not_mark_an_incomplete_conversation_read() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let (db, conn) = open_state_db(home).unwrap();
    write_composer(
        &conn,
        "whole",
        "",
        "/w",
        &tool_conversation("a", START),
        START,
    )
    .unwrap();
    write_composer(
        &conn,
        "short",
        "",
        "/w",
        &tool_conversation("b", START),
        START,
    )
    .unwrap();
    set_header_log(&conn, "short", 2, 10);
    close_idle(&db, conn).unwrap();
    let history = crate::open_db(&home.join("history.db")).unwrap();
    let mut state = Map::new();
    let mut coverage = crate::ingest::SweepCoverage::default();
    sync_editor_composers_in(&history, &mut state, home, &mut coverage, None).unwrap();
    assert!(!coverage.complete());
    assert!(state[EDITOR_STATE_KEY].get(locator(&db, "whole")).is_some());
    assert!(state[EDITOR_STATE_KEY].get(locator(&db, "short")).is_none());
    assert_eq!(
        count(&history, "history", "short"),
        1,
        "what was readable is indexed"
    );
    // Read again on the next pass, not skipped on a stamp.
    let mut again = crate::ingest::SweepCoverage::default();
    assert_eq!(
        sync_editor_composers_in(&history, &mut state, home, &mut again, None).unwrap(),
        1
    );
    assert!(!again.complete());
}

/// A pass interrupted after one composer keeps that one and leaves the rest
/// unread, with no stamp; the retry reads only what is left.
#[test]
fn an_interrupted_sync_keeps_what_it_committed_and_retries_the_rest() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let (db, conn) = open_state_db(home).unwrap();
    for id in ["a", "b", "c"] {
        write_composer(&conn, id, "", "/w", &tool_conversation(id, START), START).unwrap();
    }
    close_idle(&db, conn).unwrap();
    let path = home.join("history.db");
    let history = crate::open_db(&path).unwrap();
    let mut state = Map::new();
    // Stop as soon as one conversation is committed.
    let probe = path.clone();
    let stop = move || {
        let reader = Connection::open(&probe).unwrap();
        reader
            .query_row(
                "SELECT count(DISTINCT session_id) FROM sessions WHERE source = 'cursor'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
            >= 1
    };
    let mut coverage = crate::ingest::SweepCoverage::default();
    let error = crate::ingest::with_capture_stop(stop, || {
        sync_editor_composers_in(&history, &mut state, home, &mut coverage, None)
    })
    .unwrap_err();
    assert!(error.is::<crate::ingest::CaptureCancelled>(), "{error:#}");
    let saved = |state: &Map<String, Value>, id: &str| {
        state
            .get(EDITOR_STATE_KEY)
            .and_then(|seen| seen.get(locator(&db, id)))
            .is_some()
    };
    assert!(saved(&state, "a"));
    assert!(!saved(&state, "b") && !saved(&state, "c"));
    assert_eq!(count(&history, "history", "b"), 0);
    // The retry reads only the two left.
    let mut coverage = crate::ingest::SweepCoverage::default();
    assert_eq!(
        sync_editor_composers_in(&history, &mut state, home, &mut coverage, None).unwrap(),
        2
    );
    assert!(coverage.complete());
    for id in ["a", "b", "c"] {
        assert!(saved(&state, id));
        assert_eq!(count(&history, "history", id), 1);
    }
}

/// A headerless composer (the oldest layout) is ordered by SQLite: a full
/// read brings each payload into memory once, as it renders it, and a
/// preview brings in none.
#[test]
fn a_headerless_composer_is_ordered_without_materializing_its_payloads() {
    let dir = tempfile::tempdir().unwrap();
    let (db, conn) = open_state_db(dir.path()).unwrap();
    // Inserted newest first, so key and rowid order both disagree with time.
    for i in (0..500).rev() {
        let id = format!("b{i:03}");
        let bubble = if i % 2 == 0 {
            user_bubble(&id, &format!("turn {i}"), START + i * 1_000)
        } else {
            // Older builds wrote a number.
            json!({"type": 2, "bubbleId": id, "text": format!("turn {i}"), "createdAt": START + i * 1_000})
        };
        put(&conn, &format!("bubbleId:c:{id}"), &bubble).unwrap();
    }
    put(
        &conn,
        "bubbleId:c:undated",
        &json!({"type": 1, "bubbleId": "undated", "text": "undated"}),
    )
    .unwrap();
    put(&conn, "bubbleId:c:junk", &json!("not a bubble")).unwrap();
    put(
        &conn,
        "composerData:c",
        &json!({"_v": 14, "createdAt": START}),
    )
    .unwrap();
    close_idle(&db, conn).unwrap();

    let materialized = || read::MATERIALIZED.with(std::cell::Cell::get);
    let before = materialized();
    let preview = read_composer_preview(&db, "c").unwrap().unwrap();
    assert_eq!(
        materialized() - before,
        0,
        "a preview defers the headerless layout"
    );
    assert_eq!(preview.records, 0);
    assert!(!preview.incomplete);

    let before = materialized();
    let full = read_composer(&db, "c").unwrap().unwrap();
    assert_eq!(
        materialized() - before,
        502,
        "each payload once, while rendering"
    );
    assert_eq!(full.skipped_bubbles, 1, "the row that is not a bubble");
    let texts = texts(&full);
    assert_eq!(texts.len(), 501);
    assert_eq!(texts[0], "turn 0");
    assert_eq!(texts[499], "turn 499");
    assert_eq!(texts[500], "undated");
}

#[test]
fn without_headers_the_bubbles_are_read_in_creation_order() {
    let dir = tempfile::tempdir().unwrap();
    let (db, conn) = open_state_db(dir.path()).unwrap();
    put(
        &conn,
        "bubbleId:c:z",
        &reply_bubble("z", "answer", START + 5_000),
    )
    .unwrap();
    put(&conn, "bubbleId:c:a", &user_bubble("a", "question", START)).unwrap();
    put(
        &conn,
        "composerData:c",
        &json!({"_v": 14, "name": "Old", "createdAt": START, "fullConversationHeadersOnly": []}),
    )
    .unwrap();
    // An older build: whole bubbles inline.
    put(
        &conn,
        "composerData:inline",
        &json!({"_v": 3, "createdAt": START, "conversation": [user_bubble("i1", "inline prompt", START)]}),
    )
    .unwrap();
    close_idle(&db, conn).unwrap();
    let texts: Vec<String> = records(&read_composer(&db, "c").unwrap().unwrap())
        .iter()
        .map(|r| {
            r["message"]["content"][0]["text"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    assert_eq!(texts, vec!["question", "answer"]);
    let inline = read_composer(&db, "inline").unwrap().unwrap();
    assert_eq!(inline.records, 1);
    let mut ids: Vec<String> = composer_heads(&db)
        .unwrap()
        .into_iter()
        .map(|h| h.session_id)
        .collect();
    ids.sort();
    assert_eq!(ids, vec!["c".to_string(), "inline".to_string()]);
}

#[test]
fn the_workspace_folder_comes_from_workspace_storage_when_the_composer_names_none() {
    let dir = tempfile::tempdir().unwrap();
    let (db, conn) = open_state_db(dir.path()).unwrap();
    let mut data = json!({"_v": 18, "createdAt": START, "workspaceIdentifier": {"id": "abc123"}});
    put(&conn, "bubbleId:c:u", &user_bubble("u", "hi", START)).unwrap();
    put(&conn, "composerData:c", &data).unwrap();
    data["workspaceIdentifier"]["id"] = json!("../escape");
    put(&conn, "composerData:d", &data).unwrap();
    put(&conn, "bubbleId:d:u", &user_bubble("u", "hi", START)).unwrap();
    close_idle(&db, conn).unwrap();
    let storage = user_dir(dir.path()).join("workspaceStorage/abc123");
    std::fs::create_dir_all(&storage).unwrap();
    std::fs::write(
        storage.join("workspace.json"),
        r#"{"folder": "file:///work/my%20app"}"#,
    )
    .unwrap();
    assert_eq!(
        read_composer(&db, "c").unwrap().unwrap().cwd.as_deref(),
        Some("/work/my app")
    );
    assert_eq!(read_composer(&db, "d").unwrap().unwrap().cwd, None);
}

#[test]
fn a_stamp_moves_when_a_bubble_is_rewritten_and_only_for_its_composer() {
    let dir = tempfile::tempdir().unwrap();
    let (db, conn) = open_state_db(dir.path()).unwrap();
    write_composer(&conn, "a", "", "/w", &tool_conversation("a", START), START).unwrap();
    write_composer(&conn, "b", "", "/w", &tool_conversation("b", START), START).unwrap();
    let before = composer_heads(&db).unwrap();
    // A streamed reply is rewritten in place under the same key.
    put(
        &conn,
        "bubbleId:a:b-user/s:2",
        &reply_bubble("b-user/s:2", "longer reply", START + 4_000),
    )
    .unwrap();
    let after = composer_heads(&db).unwrap();
    assert_ne!(before[0].stamp, after[0].stamp);
    assert_eq!(before[1].stamp, after[1].stamp);
    assert_eq!(
        composer_stamp(&db, "a").unwrap().as_deref(),
        Some(after[0].stamp.as_str())
    );
    assert_eq!(composer_stamp(&db, "gone").unwrap(), None);
    drop(conn);
}

#[test]
fn a_path_scoped_tick_elsewhere_skips_the_database() {
    let dir = tempfile::tempdir().unwrap();
    one_conversation(dir.path(), "c-1");
    let elsewhere = crate::ingest::path_scope::PathScope::from_paths([dir
        .path()
        .join(".cursor/projects/x/y.jsonl")]);
    assert!(composers(dir.path(), elsewhere.as_ref())
        .unwrap()
        .is_empty());
    let here =
        crate::ingest::path_scope::PathScope::from_paths([crate::ingest::cursor_store::sidecar(
            &state_db_path(dir.path()),
            "-wal",
        )]);
    assert_eq!(composers(dir.path(), here.as_ref()).unwrap().len(), 1);
}

fn sync(conn: &Connection, state: &mut Map<String, Value>, home: &Path) -> usize {
    let mut coverage = crate::ingest::SweepCoverage::default();
    sync_editor_composers_in(conn, state, home, &mut coverage, None).unwrap()
}

fn count(conn: &Connection, table: &str, id: &str) -> i64 {
    conn.query_row(
        &format!("SELECT count(*) FROM {table} WHERE source = 'cursor' AND session_id = ?1"),
        [id],
        |row| row.get(0),
    )
    .unwrap()
}

#[test]
fn sync_indexes_a_composer_once_and_rebuilds_it_when_it_changes() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let db = one_conversation(home, "c-1");
    let conn = crate::open_db(&home.join("history.db")).unwrap();
    let mut state = Map::new();
    assert_eq!(sync(&conn, &mut state, home), 1);
    let (title, cwd, raw, first, last): (Option<String>, Option<String>, String, i64, i64) = conn
        .query_row(
            "SELECT title, cwd, raw_path, first_activity_ms, last_activity_ms FROM sessions \
             WHERE source = 'cursor' AND session_id = 'c-1'",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(title.as_deref(), Some("Fix the note"));
    assert_eq!(cwd.as_deref(), Some("/work/app"));
    assert_eq!(raw, locator(&db, "c-1"));
    assert_eq!(first, START - 5_000);
    assert_eq!(last, START + 4_000);
    assert_eq!(count(&conn, "tool_calls", "c-1"), 2);
    assert_eq!(count(&conn, "file_edits", "c-1"), 1);
    assert_eq!(count(&conn, "history", "c-1"), 1);
    let events = count(&conn, "session_events", "c-1");

    // Unchanged: nothing read, nothing duplicated.
    assert_eq!(sync(&conn, &mut state, home), 0);
    assert_eq!(count(&conn, "session_events", "c-1"), events);

    // A new turn: rebuilt whole, still one copy of each.
    let (_, writer) = open_state_db(home).unwrap();
    let mut bubbles = tool_conversation("read note.txt", START);
    bubbles.push(user_bubble("u2", "thanks", START + 60_000));
    write_composer(
        &writer,
        "c-1",
        "Fix the note",
        "/work/app",
        &bubbles,
        START - 5_000,
    )
    .unwrap();
    close_idle(&db, writer).unwrap();
    assert_eq!(sync(&conn, &mut state, home), 2);
    assert_eq!(count(&conn, "history", "c-1"), 2);
    assert_eq!(count(&conn, "tool_calls", "c-1"), 2);
}

#[test]
fn sync_leaves_a_composer_that_has_a_transcript_alone_and_forgets_its_stamp() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let db = one_conversation(home, "c-1");
    let conn = crate::open_db(&home.join("history.db")).unwrap();
    let mut state = Map::new();
    sync(&conn, &mut state, home);
    let key = locator(&db, "c-1");
    assert!(state[EDITOR_STATE_KEY].get(&key).is_some());
    let transcript = home.join(".cursor/projects/w/agent-transcripts/c-1/c-1.jsonl");
    std::fs::create_dir_all(transcript.parent().unwrap()).unwrap();
    std::fs::write(&transcript, "{}\n").unwrap();
    assert_eq!(sync(&conn, &mut state, home), 0);
    assert!(state[EDITOR_STATE_KEY].get(&key).is_none());
}

#[test]
fn sync_refills_a_rebuilt_destination() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    one_conversation(home, "c-1");
    let conn = crate::open_db(&home.join("history.db")).unwrap();
    let mut state = Map::new();
    sync(&conn, &mut state, home);
    drop(conn);
    std::fs::remove_file(home.join("history.db")).unwrap();
    let conn = crate::open_db(&home.join("history2.db")).unwrap();
    assert_eq!(
        sync(&conn, &mut state, home),
        1,
        "the stamp alone does not skip it"
    );
}

fn prompts(conn: &Connection, session: &str) -> Vec<String> {
    conn.prepare("SELECT prompt FROM history WHERE source = 'cursor' AND session_id = ?1 ORDER BY timestamp_ms")
        .unwrap()
        .query_map([session], |row| row.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

#[test]
fn a_transcript_that_appears_replaces_the_editor_evidence_in_a_full_sweep() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let db = one_conversation(home, "c-1");
    let conn = crate::open_db(&home.join("history.db")).unwrap();
    let mut state = Map::new();
    let mut coverage = crate::ingest::SweepCoverage::default();
    let projects = home.join(".cursor/projects");
    sync_editor_composers_in(&conn, &mut state, home, &mut coverage, None).unwrap();
    assert_eq!(prompts(&conn, "c-1"), vec!["read note.txt".to_string()]);
    let transcript = projects.join("w/agent-transcripts/c-1");
    std::fs::create_dir_all(&transcript).unwrap();
    std::fs::write(
        transcript.join("c-1.jsonl"),
        "{\"role\":\"user\",\"message\":{\"content\":\"transcript prompt\"}}\n",
    )
    .unwrap();
    // The sweep's order: transcripts, chat stores, then the editor database.
    crate::ingest::sync_cursor_in(&conn, &mut state, &projects, &mut coverage, None).unwrap();
    crate::ingest::cursor_store::sync_chat_stores_in(&conn, &mut state, home, &mut coverage, None)
        .unwrap();
    sync_editor_composers_in(&conn, &mut state, home, &mut coverage, None).unwrap();
    assert_eq!(prompts(&conn, "c-1"), vec!["transcript prompt".to_string()]);
    assert!(crate::session_tool_calls(&conn, "c-1", Some("cursor"))
        .unwrap()
        .is_empty());

    // The transcript's row is never taken back by the editor locator.
    let transcript = transcript.join("c-1.jsonl").to_string_lossy().into_owned();
    conn.execute(
        "UPDATE sessions SET raw_path = ?1 WHERE source = 'cursor' AND session_id = 'c-1'",
        [&transcript],
    )
    .unwrap();
    assert!(crate::catalog_owner::incoming_must_not_replace(
        &conn,
        "cursor",
        "c-1",
        Some(&locator(&db, "c-1"))
    )
    .unwrap());
    // And the editor locator gives its row up to a transcript.
    conn.execute(
        "UPDATE sessions SET raw_path = ?1 WHERE source = 'cursor' AND session_id = 'c-1'",
        [locator(&db, "c-1")],
    )
    .unwrap();
    assert!(!crate::catalog_owner::incoming_must_not_replace(
        &conn,
        "cursor",
        "c-1",
        Some(&transcript)
    )
    .unwrap());
}

#[test]
fn a_database_without_cursors_table_holds_no_composers() {
    let dir = tempfile::tempdir().unwrap();
    let db = state_db_path(dir.path());
    std::fs::create_dir_all(db.parent().unwrap()).unwrap();
    Connection::open(&db)
        .unwrap()
        .execute_batch("CREATE TABLE ItemTable (key TEXT UNIQUE ON CONFLICT REPLACE, value BLOB);")
        .unwrap();
    assert!(composers(dir.path(), None).unwrap().is_empty());
    assert!(read_composer(&db, "c").unwrap().is_none());
}

#[test]
fn a_preview_renders_only_both_ends_of_a_long_conversation() {
    let dir = tempfile::tempdir().unwrap();
    let (db, conn) = open_state_db(dir.path()).unwrap();
    let bubbles: Vec<Value> = (0..100)
        .map(|i| user_bubble(&format!("u{i}"), &format!("turn {i}"), START + i * 1_000))
        .collect();
    write_composer(&conn, "long", "", "/w", &bubbles, START).unwrap();
    close_idle(&db, conn).unwrap();
    let preview = read_composer_preview(&db, "long").unwrap().unwrap();
    assert_eq!(preview.records, 2 * read::PREVIEW_BUBBLES);
    let texts: Vec<String> = records(&preview)
        .iter()
        .map(|r| {
            r["message"]["content"][0]["text"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    assert_eq!(texts.first().map(String::as_str), Some("turn 0"));
    assert_eq!(texts.last().map(String::as_str), Some("turn 99"));
}
