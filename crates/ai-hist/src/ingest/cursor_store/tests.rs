use std::path::Path;

use rusqlite::Connection;
use serde_json::{json, Map, Value};

use super::read::{file_uri_path, RootFields};
use super::test_support::*;
use super::*;

#[test]
fn a_store_renders_as_the_transcript_cursor_would_write() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_store(
        dir.path(),
        "ws",
        "chat-1",
        "/work/app",
        &tool_chat("read note.txt"),
        1_795_042_344_825,
    )
    .unwrap();
    let store = read_chat_store(&path).unwrap().unwrap();
    assert_eq!(store.session_id, "chat-1");
    assert_eq!(store.cwd.as_deref(), Some("/work/app"));
    assert_eq!(store.origin.as_deref(), Some("cli"));
    assert_eq!(store.updated_ms, Some(1_795_042_344_825));
    assert_eq!(store.skipped_blobs, 0);
    let records: Vec<Value> = store
        .transcript
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    // System prompt and preamble are left out; the four turns remain.
    assert_eq!(records.len(), 4);
    assert_eq!(records[0]["role"], "user");
    assert!(records[0]["message"]["content"][0]["text"]
        .as_str()
        .unwrap()
        .contains("<user_query>"));
    assert_eq!(records[1]["message"]["content"][0]["type"], "tool_use");
    assert_eq!(records[1]["message"]["content"][0]["id"], "call-1");
    assert_eq!(records[1]["message"]["content"][0]["name"], "Read");
    assert_eq!(records[2]["role"], "user");
    assert_eq!(records[2]["message"]["content"][0]["type"], "tool_result");
    assert_eq!(records[2]["message"]["content"][0]["tool_use_id"], "call-1");
    assert_eq!(
        records[3]["message"]["content"][0]["text"],
        "note.txt says hello"
    );
    assert!(records[3]["message"]["id"]
        .as_str()
        .unwrap()
        .starts_with("cursor-blob:"));
}

#[test]
fn an_idle_store_with_no_wal_is_read_without_writing_beside_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_store(dir.path(), "ws", "chat-1", "/w", &tool_chat("hi"), 5).unwrap();
    assert!(!sidecar(&path, "-shm").exists());
    read_chat_store(&path).unwrap().unwrap();
    let mut left: Vec<String> = std::fs::read_dir(path.parent().unwrap())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    left.sort();
    assert_eq!(left, vec!["store.db".to_string()]);
}

#[test]
fn meta_json_without_a_conversation_is_no_chat() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_store(dir.path(), "ws", "chat-1", "/w", &tool_chat("hi"), 5).unwrap();
    std::fs::write(
        path.parent().unwrap().join("meta.json"),
        r#"{"schemaVersion":1,"hasConversation":false,"cwd":"/w"}"#,
    )
    .unwrap();
    assert!(read_chat_store(&path).unwrap().is_none());
}

#[test]
fn meta_json_supplies_cwd_and_times() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_store(dir.path(), "ws", "chat-1", "/root/cwd", &tool_chat("hi"), 5).unwrap();
    std::fs::write(
        path.parent().unwrap().join("meta.json"),
        r#"{"schemaVersion":1,"createdAtMs":10,"updatedAtMs":20,"hasConversation":true,"cwd":"/meta/cwd"}"#,
    )
    .unwrap();
    let store = read_chat_store(&path).unwrap().unwrap();
    assert_eq!(store.cwd.as_deref(), Some("/meta/cwd"));
    assert_eq!(store.created_ms, Some(10));
    assert_eq!(store.updated_ms, Some(20));
}

#[test]
fn an_unreadable_blob_is_skipped_and_counted() {
    let dir = tempfile::tempdir().unwrap();
    let mut messages = tool_chat("hi");
    messages.push(json!("placeholder"));
    let path = write_store(dir.path(), "ws", "chat-1", "/w", &messages, 5).unwrap();
    // Replace the last message blob with bytes that are not JSON, the
    // way an encrypted blob would read.
    let conn = Connection::open(&path).unwrap();
    let id: String = conn
        .query_row(
            "SELECT id FROM blobs WHERE data = ?1",
            [json!("placeholder").to_string().as_bytes()],
            |row| row.get(0),
        )
        .unwrap();
    conn.execute(
        "UPDATE blobs SET data = ?1 WHERE id = ?2",
        rusqlite::params![vec![0x9fu8, 0x00, 0x13], id],
    )
    .unwrap();
    conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    drop(conn);
    let _ = std::fs::remove_file(sidecar(&path, "-wal"));
    let _ = std::fs::remove_file(sidecar(&path, "-shm"));
    let store = read_chat_store(&path).unwrap().unwrap();
    assert_eq!(store.skipped_blobs, 1);
    assert_eq!(store.records, 4);
}

#[test]
fn a_chat_with_a_transcript_is_not_offered_from_its_store() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    write_store(home, "ws", "has-transcript", "/w", &tool_chat("a"), 5).unwrap();
    let only_store = write_store(home, "ws", "store-only", "/w", &tool_chat("b"), 5).unwrap();
    let transcript = home.join(".cursor/projects/w/agent-transcripts/has-transcript");
    std::fs::create_dir_all(&transcript).unwrap();
    std::fs::write(transcript.join("has-transcript.jsonl"), "{}\n").unwrap();
    let stores = chat_stores(home, None).unwrap();
    assert_eq!(stores, vec![(only_store, "store-only".to_string())]);
}

#[test]
fn a_wal_append_changes_the_stamp() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_store(dir.path(), "ws", "chat-1", "/w", &tool_chat("hi"), 5).unwrap();
    let (before, _) = chat_store_stamp_and_modified(&path).unwrap();
    std::fs::write(sidecar(&path, "-wal"), b"frames").unwrap();
    let (after, _) = chat_store_stamp_and_modified(&path).unwrap();
    assert_ne!(before, after);
}

fn prompts(conn: &Connection, session: &str) -> Vec<String> {
    conn.prepare("SELECT prompt FROM history WHERE source = 'cursor' AND session_id = ?1 ORDER BY timestamp_ms, prompt")
        .unwrap()
        .query_map([session], |row| row.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

#[test]
fn sync_indexes_a_store_only_chat_once_and_rebuilds_it_when_it_changes() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let conn = crate::open_db(&home.join("history.db")).unwrap();
    let mut state = Map::new();
    let mut coverage = crate::ingest::SweepCoverage::default();
    write_store(home, "ws", "chat-1", "/w", &tool_chat("first prompt"), 5).unwrap();
    assert_eq!(
        sync_chat_stores_in(&conn, &mut state, home, &mut coverage, None).unwrap(),
        1
    );
    assert_eq!(prompts(&conn, "chat-1"), vec!["first prompt".to_string()]);
    // Unchanged: skipped on its stamp.
    assert_eq!(
        sync_chat_stores_in(&conn, &mut state, home, &mut coverage, None).unwrap(),
        0
    );
    // Cursor appends a turn: the chat is rebuilt, not appended to twice.
    let mut longer = tool_chat("first prompt");
    longer.push(json!({"role": "user", "content": [{"type": "text", "text":
        "<timestamp>Thursday, Nov 19, 2026, 12:10 AM (UTC+1)</timestamp>\n<user_query>\nsecond prompt\n</user_query>"}]}));
    longer.push(json!({"role": "assistant", "content": [{"type": "text", "text": "done"}]}));
    std::thread::sleep(std::time::Duration::from_millis(20));
    write_store(home, "ws", "chat-1", "/w", &longer, 6).unwrap();
    sync_chat_stores_in(&conn, &mut state, home, &mut coverage, None).unwrap();
    assert_eq!(
        prompts(&conn, "chat-1"),
        vec!["first prompt".to_string(), "second prompt".to_string()]
    );
    let calls = crate::session_tool_calls(&conn, "chat-1", Some("cursor")).unwrap();
    assert_eq!(calls.len(), 1);
    let (raw, last): (String, String) = conn
        .query_row(
            "SELECT raw_path, last_assistant_text FROM sessions WHERE source='cursor' AND session_id='chat-1'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert!(raw.ends_with("chat-1/store.db"));
    assert_eq!(last, "done");
}

#[test]
fn sync_leaves_a_chat_with_a_transcript_to_the_transcript() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let conn = crate::open_db(&home.join("history.db")).unwrap();
    write_store(home, "ws", "chat-1", "/w", &tool_chat("from the store"), 5).unwrap();
    let transcript = home.join(".cursor/projects/w/agent-transcripts/chat-1");
    std::fs::create_dir_all(&transcript).unwrap();
    std::fs::write(
        transcript.join("chat-1.jsonl"),
        "{\"role\":\"user\",\"message\":{\"content\":\"from the transcript\"}}\n",
    )
    .unwrap();
    let mut state = Map::new();
    let mut coverage = crate::ingest::SweepCoverage::default();
    assert_eq!(
        sync_chat_stores_in(&conn, &mut state, home, &mut coverage, None).unwrap(),
        0
    );
    assert!(prompts(&conn, "chat-1").is_empty());
}

#[test]
fn a_store_that_gains_a_transcript_drops_its_stamp() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let conn = crate::open_db(&home.join("history.db")).unwrap();
    let store = write_store(home, "ws", "chat-1", "/w", &tool_chat("a"), 5).unwrap();
    let mut state = Map::new();
    let mut coverage = crate::ingest::SweepCoverage::default();
    sync_chat_stores_in(&conn, &mut state, home, &mut coverage, None).unwrap();
    let key = store.to_string_lossy().into_owned();
    assert!(state[CHAT_STORE_STATE_KEY].get(&key).is_some());
    let transcript = home.join(".cursor/projects/w/agent-transcripts/chat-1");
    std::fs::create_dir_all(&transcript).unwrap();
    std::fs::write(transcript.join("chat-1.jsonl"), "{}\n").unwrap();
    sync_chat_stores_in(&conn, &mut state, home, &mut coverage, None).unwrap();
    assert!(state[CHAT_STORE_STATE_KEY].get(&key).is_none());
    assert!(state["forgotten_paths"][CHAT_STORE_STATE_KEY]
        .as_array()
        .unwrap()
        .contains(&json!(key)));
}

#[test]
fn a_path_scoped_tick_reads_only_the_store_that_changed() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let changed = write_store(home, "ws", "chat-1", "/w", &tool_chat("a"), 5).unwrap();
    write_store(home, "ws", "chat-2", "/w", &tool_chat("b"), 5).unwrap();
    let scope = crate::ingest::path_scope::PathScope::from_paths([changed.clone()]).unwrap();
    let stores = chat_stores(home, Some(&scope)).unwrap();
    assert_eq!(stores, vec![(changed, "chat-1".to_string())]);
    let elsewhere =
        crate::ingest::path_scope::PathScope::from_paths([home.join(".claude/projects/x.jsonl")])
            .unwrap();
    assert!(chat_stores(home, Some(&elsewhere)).unwrap().is_empty());
}

#[test]
fn only_a_store_under_cursor_chats_is_a_chat_store() {
    assert!(is_chat_store(Path::new("/h/.cursor/chats/ws/id/store.db")));
    assert!(!is_chat_store(Path::new(
        "/h/.cursor/projects/ws/id/store.db"
    )));
    assert!(!is_chat_store(Path::new("/h/.cursor/chats/ws/id/other.db")));
}

#[test]
fn a_truncated_root_keeps_what_decoded() {
    let mut root = Vec::new();
    root.extend_from_slice(&[0x0a, 0x20]);
    root.extend_from_slice(&[7u8; 32]);
    root.extend_from_slice(&[0x0a, 0x20, 1, 2]);
    let fields = RootFields::decode(&root);
    assert_eq!(fields.messages.len(), 1);
}

#[test]
fn a_store_under_a_non_ascii_home_opens() {
    let dir = tempfile::tempdir().unwrap();
    let name = if cfg!(windows) {
        "Jos\u{e9} 中 %#"
    } else {
        "Jos\u{e9} 中 %#?"
    };
    let home = dir.path().join(name);
    let path = write_store(&home, "ws", "chat-1", "/w", &tool_chat("hi"), 5).unwrap();
    assert!(!sidecar(&path, "-wal").exists());
    let store = read_chat_store(&path).unwrap().unwrap();
    assert_eq!(store.records, 4);
}

#[test]
fn a_workspace_uri_decodes_without_slicing_through_a_character() {
    assert_eq!(
        file_uri_path("file:///work/%中").as_deref(),
        Some("/work/%中")
    );
    assert_eq!(
        file_uri_path("file:///Jos%C3%A9/app").as_deref(),
        Some("/José/app")
    );
    assert_eq!(file_uri_path("file:///w/%4").as_deref(), Some("/w/%4"));
}

#[test]
fn a_blob_listed_twice_is_two_messages() {
    let dir = tempfile::tempdir().unwrap();
    let mut messages = tool_chat("hi");
    let again = json!({"role": "assistant", "content": [{"type": "text", "text": "same"}]});
    messages.push(again.clone());
    messages.push(json!({"role": "user", "content": [{"type": "text", "text": "<user_query>\nmore\n</user_query>"}]}));
    messages.push(again);
    let path = write_store(dir.path(), "ws", "chat-1", "/w", &messages, 5).unwrap();
    let store = read_chat_store(&path).unwrap().unwrap();
    let ids: Vec<String> = store
        .transcript
        .lines()
        .map(|line| {
            serde_json::from_str::<Value>(line).unwrap()["message"]["id"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    let mut unique = ids.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), ids.len(), "{ids:?}");
}

#[test]
fn a_preview_renders_only_both_ends_of_a_long_chat() {
    let dir = tempfile::tempdir().unwrap();
    let mut messages = tool_chat("first");
    for n in 0..40 {
        messages.push(json!({"role": "assistant", "content": [{"type": "text", "text": format!("reply {n}")}]}));
    }
    let path = write_store(dir.path(), "ws", "chat-1", "/w", &messages, 5).unwrap();
    let preview = read_chat_store_preview(&path).unwrap().unwrap();
    let full = read_chat_store(&path).unwrap().unwrap();
    assert!(preview.records < full.records);
    assert!(preview.transcript.contains("<user_query>\\nfirst"));
    assert!(preview.transcript.contains("reply 39"));
    assert!(!preview.transcript.contains("reply 10\""));
}

#[test]
fn an_unchanged_store_is_indexed_again_into_a_rebuilt_database() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    write_store(home, "ws", "chat-1", "/w", &tool_chat("first prompt"), 5).unwrap();
    let mut state = Map::new();
    let mut coverage = crate::ingest::SweepCoverage::default();
    let first = crate::open_db(&home.join("first.db")).unwrap();
    sync_chat_stores_in(&first, &mut state, home, &mut coverage, None).unwrap();
    // Same sync state, fresh database: the stamp alone must not skip it.
    let rebuilt = crate::open_db(&home.join("rebuilt.db")).unwrap();
    sync_chat_stores_in(&rebuilt, &mut state, home, &mut coverage, None).unwrap();
    assert_eq!(
        prompts(&rebuilt, "chat-1"),
        vec!["first prompt".to_string()]
    );
}

#[test]
fn a_chat_with_a_subagent_transcript_is_not_offered_from_its_store() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    write_store(home, "ws", "child-1", "/w", &tool_chat("a"), 5).unwrap();
    let subagents = home.join(".cursor/projects/w/agent-transcripts/parent-1/subagents");
    std::fs::create_dir_all(&subagents).unwrap();
    std::fs::write(subagents.join("child-1.jsonl"), "{}\n").unwrap();
    assert!(chat_stores(home, None).unwrap().is_empty());
}

#[test]
fn a_transcript_takes_the_catalog_row_over_from_its_store() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let store = write_store(home, "ws", "chat-1", "/w", &tool_chat("a"), 5).unwrap();
    let transcript = home.join(".cursor/projects/w/agent-transcripts/chat-1/chat-1.jsonl");
    std::fs::create_dir_all(transcript.parent().unwrap()).unwrap();
    std::fs::write(&transcript, "{}\n").unwrap();
    let conn = crate::open_db(&home.join("history.db")).unwrap();
    conn.execute(
        "INSERT INTO sessions (source, session_id, raw_path, discovery_state) \
         VALUES ('cursor', 'chat-1', ?1, 'shallow')",
        [store.to_string_lossy()],
    )
    .unwrap();
    let transcript = transcript.to_string_lossy();
    assert!(!crate::catalog_owner::incoming_must_not_replace(
        &conn,
        "cursor",
        "chat-1",
        Some(&transcript)
    )
    .unwrap());
    // The reverse never happens: a store does not take a transcript's row.
    conn.execute(
        "UPDATE sessions SET raw_path = ?1 WHERE session_id = 'chat-1'",
        [&*transcript],
    )
    .unwrap();
    assert!(crate::catalog_owner::incoming_must_not_replace(
        &conn,
        "cursor",
        "chat-1",
        Some(&store.to_string_lossy())
    )
    .unwrap());
    assert!(superseded(&store).unwrap());
}

#[test]
fn an_untimed_chat_keeps_its_creation_time_as_its_start() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let messages = vec![
        json!({"role": "user", "content": "no timestamp here"}),
        json!({"role": "assistant", "content": "ok"}),
    ];
    write_store(home, "ws", "chat-1", "/w", &messages, 50_000).unwrap();
    let conn = crate::open_db(&home.join("history.db")).unwrap();
    let mut state = Map::new();
    let mut coverage = crate::ingest::SweepCoverage::default();
    sync_chat_stores_in(&conn, &mut state, home, &mut coverage, None).unwrap();
    let (first, last): (i64, i64) = conn
        .query_row(
            "SELECT first_activity_ms, last_activity_ms FROM sessions WHERE session_id = 'chat-1'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!((first, last), (49_000, 50_000));
}

#[test]
fn a_chat_with_no_evidence_is_not_rebuilt_on_every_pass() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let only_system = vec![json!({"role": "system", "content": "You are an AI coding assistant."})];
    write_store(home, "ws", "chat-1", "/w", &only_system, 5).unwrap();
    let conn = crate::open_db(&home.join("history.db")).unwrap();
    let mut state = Map::new();
    let mut coverage = crate::ingest::SweepCoverage::default();
    sync_chat_stores_in(&conn, &mut state, home, &mut coverage, None).unwrap();
    let saved = state[CHAT_STORE_STATE_KEY].as_object().unwrap().clone();
    let (stores, _) = (chat_stores(home, None).unwrap(), ());
    let (path, session_id) = &stores[0];
    let (stamp, _) = chat_store_stamp_and_modified(path).unwrap();
    assert!(super::sync::is_unchanged_for_test(
        &conn, &saved, path, &stamp, session_id
    ));
}

#[test]
fn a_dated_first_turn_keeps_its_start_and_the_store_update_bounds_the_end() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let mut messages = tool_chat("dated first");
    messages.push(json!({"role": "user", "content": "undated follow-up"}));
    write_store(home, "ws", "chat-1", "/w", &messages, 1_795_042_344_825).unwrap();
    let conn = crate::open_db(&home.join("history.db")).unwrap();
    let mut state = Map::new();
    let mut coverage = crate::ingest::SweepCoverage::default();
    sync_chat_stores_in(&conn, &mut state, home, &mut coverage, None).unwrap();
    let (first, last): (i64, i64) = conn
        .query_row(
            "SELECT first_activity_ms, last_activity_ms FROM sessions WHERE session_id = 'chat-1'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    // 2026-11-18 23:52 UTC+1, from the first turn's <timestamp>.
    assert_eq!(first, 1_795_042_320_000);
    assert_eq!(last, 1_795_042_344_825);
}

#[test]
fn a_transcript_that_appears_replaces_the_store_evidence_in_a_full_sweep() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    write_store(home, "ws", "chat-1", "/w", &tool_chat("store prompt"), 5).unwrap();
    let conn = crate::open_db(&home.join("history.db")).unwrap();
    let mut state = Map::new();
    let mut coverage = crate::ingest::SweepCoverage::default();
    let projects = home.join(".cursor/projects");
    sync_chat_stores_in(&conn, &mut state, home, &mut coverage, None).unwrap();
    assert_eq!(prompts(&conn, "chat-1"), vec!["store prompt".to_string()]);
    let transcript = projects.join("w/agent-transcripts/chat-1");
    std::fs::create_dir_all(&transcript).unwrap();
    std::fs::write(
        transcript.join("chat-1.jsonl"),
        "{\"role\":\"user\",\"message\":{\"content\":\"transcript prompt\"}}\n",
    )
    .unwrap();
    // The sweep's order: transcripts, then stores.
    crate::ingest::sync_cursor_in(&conn, &mut state, &projects, &mut coverage, None).unwrap();
    sync_chat_stores_in(&conn, &mut state, home, &mut coverage, None).unwrap();
    assert_eq!(
        prompts(&conn, "chat-1"),
        vec!["transcript prompt".to_string()]
    );
    assert!(crate::session_tool_calls(&conn, "chat-1", Some("cursor"))
        .unwrap()
        .is_empty());
}

#[test]
fn a_chat_filed_under_two_workspaces_is_read_from_one_store() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    write_store(home, "ws-a", "chat-1", "/w", &tool_chat("old"), 5).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(20));
    let newer = write_store(home, "ws-b", "chat-1", "/w", &tool_chat("new"), 6).unwrap();
    assert_eq!(
        chat_stores(home, None).unwrap(),
        vec![(newer, "chat-1".to_string())]
    );
}

#[test]
fn a_scoped_tick_on_an_older_copy_never_rebuilds_from_it() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let older = write_store(home, "ws-a", "chat-1", "/w", &tool_chat("old"), 5).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(20));
    let mut newer_chat = tool_chat("old");
    newer_chat.push(json!({"role": "user", "content": [{"type": "text", "text":
        "<timestamp>Thursday, Nov 19, 2026, 12:10 AM (UTC+1)</timestamp>\n<user_query>\nnewer prompt\n</user_query>"}]}));
    write_store(home, "ws-b", "chat-1", "/w", &newer_chat, 6).unwrap();
    let conn = crate::open_db(&home.join("history.db")).unwrap();
    let mut state = Map::new();
    let mut coverage = crate::ingest::SweepCoverage::default();
    sync_chat_stores_in(&conn, &mut state, home, &mut coverage, None).unwrap();
    let scope =
        crate::ingest::path_scope::PathScope::from_paths([older.parent().unwrap().to_path_buf()])
            .unwrap();
    assert!(chat_stores(home, Some(&scope)).unwrap().is_empty());
    sync_chat_stores_in(&conn, &mut state, home, &mut coverage, Some(&scope)).unwrap();
    sync_chat_stores_in(&conn, &mut state, home, &mut coverage, None).unwrap();
    assert!(prompts(&conn, "chat-1").contains(&"newer prompt".to_string()));
    assert!(superseded(&older).unwrap());
}

#[test]
fn the_owning_store_takes_the_catalog_row_over_from_an_older_copy() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let older = write_store(home, "ws-a", "chat-1", "/w", &tool_chat("a"), 5).unwrap();
    let newer = write_store(home, "ws-b", "chat-1", "/w", &tool_chat("b"), 6).unwrap();
    let conn = crate::open_db(&home.join("history.db")).unwrap();
    conn.execute(
        "INSERT INTO sessions (source, session_id, raw_path, discovery_state) \
         VALUES ('cursor', 'chat-1', ?1, 'shallow')",
        [older.to_string_lossy()],
    )
    .unwrap();
    assert!(!crate::catalog_owner::incoming_must_not_replace(
        &conn,
        "cursor",
        "chat-1",
        Some(&newer.to_string_lossy())
    )
    .unwrap());
}

#[test]
fn a_tick_scoped_to_a_wal_or_meta_change_reads_its_store() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let store = write_store(home, "ws", "chat-1", "/w", &tool_chat("a"), 5).unwrap();
    for sibling in ["store.db-wal", "meta.json"] {
        let changed = store.parent().unwrap().join(sibling);
        let scope = crate::ingest::path_scope::PathScope::from_paths([changed]).unwrap();
        assert_eq!(
            chat_stores(home, Some(&scope)).unwrap(),
            vec![(store.clone(), "chat-1".to_string())],
            "{sibling}"
        );
    }
}
