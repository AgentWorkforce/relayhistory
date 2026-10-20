//! Native step order must survive sparse compact captures and timestamp ties.
use ai_hist::{
    HydrateOptions, ProviderRoots, Role, SessionQuery, SessionRef, SessionStore, Source,
    StoreOptions, SyncOptions,
};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
const FIXTURE:&str=include_str!("fixtures/antigravity/ordinary/.gemini/antigravity/brain/00000000000000000000000000000001/.system_generated/logs/transcript_full.jsonl");
fn reference() -> SessionRef {
    SessionRef::id(
        Source::Antigravity,
        format!(
            "root:{:x}:native",
            Sha256::digest(b".gemini/antigravity/brain")
        ),
    )
}
fn open(home: &Path) -> SessionStore {
    SessionStore::open(StoreOptions::default().db_path(home.join("db")).roots(
        ProviderRoots::from_home(home.into(), home.join("opencode.db")),
    ))
    .unwrap()
}
fn stage(home: &Path, name: &str, text: &str) -> PathBuf {
    let path = home
        .join(".gemini/antigravity/brain/native/.system_generated/logs")
        .join(name);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, text).unwrap();
    path
}
fn read(store: &SessionStore) -> ai_hist::SessionEvidence {
    store
        .session(&reference(), SessionQuery::default())
        .unwrap()
        .unwrap()
}
#[test]
fn compact_truncated_user_then_full_preserves_native_order_on_timestamp_tie() {
    let home = tempfile::tempdir().unwrap();
    let mut rows = FIXTURE
        .lines()
        .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(rows[0]["created_at"], rows[1]["created_at"]);
    // Different ISO spellings/submillisecond values still share stored ts_ms.
    rows[1]["created_at"] = "2026-10-11T12:00:00.000999Z".into();
    rows[0]["content"] = "Truncated request".into();
    rows[0]["truncated_fields"] = serde_json::json!(["content"]);
    let compact = rows
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    stage(home.path(), "transcript.jsonl", &compact);
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    stage(home.path(), "transcript_full.jsonl", FIXTURE);
    store
        .hydrate(&reference(), HydrateOptions::default())
        .unwrap();
    let complete = read(&store);
    #[cfg(feature = "unstable-internal")]
    assert_pages(&store, &complete);
    assert_eq!(
        complete.messages.iter().map(|m| m.role).collect::<Vec<_>>(),
        vec![Role::User, Role::Assistant],
        "native step order must beat arrival order for equal timestamps"
    );
    assert_eq!(
        complete.messages[0].blocks[0].text.as_deref(),
        Some("Remember the synthetic fixture color blue.")
    );
    let watermark = store.head_revision().unwrap();
    store
        .hydrate(&reference(), HydrateOptions::default())
        .unwrap();
    assert_eq!(store.head_revision().unwrap(), watermark);
    drop(store);
    let store = open(home.path());
    assert_eq!(read(&store).messages, complete.messages);
    std::fs::remove_file(
        home.path()
            .join(".gemini/antigravity/brain/native/.system_generated/logs/transcript_full.jsonl"),
    )
    .unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert_eq!(
        read(&store).messages,
        complete.messages,
        "later compact gap must retain previously complete evidence"
    );
    let watermark = store.head_revision().unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert_eq!(store.head_revision().unwrap(), watermark);
}
#[test]
fn reversed_physical_records_follow_native_step_order_on_timestamp_tie() {
    let home = tempfile::tempdir().unwrap();
    let reversed = FIXTURE.lines().rev().collect::<Vec<_>>().join("\n") + "\n";
    stage(home.path(), "transcript_full.jsonl", &reversed);
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert_eq!(
        read(&store)
            .messages
            .iter()
            .map(|m| m.role)
            .collect::<Vec<_>>(),
        vec![Role::User, Role::Assistant]
    );
}

#[cfg(feature = "unstable-internal")]
fn assert_pages(store: &SessionStore, complete: &ai_hist::SessionEvidence) {
    let conn = ai_hist::open_db(store.db_path()).unwrap();
    let sid = &complete.session.session_id;
    let mut cursor = None;
    let mut roles = Vec::new();
    loop {
        let page =
            ai_hist::session_events_page(&conn, sid, Some("antigravity"), 1, cursor.as_ref())
                .unwrap();
        for event in page.events {
            assert_eq!(
                event.ts_ms, 1791720000000,
                "recorded timestamps stay unchanged"
            );
            roles.push(event.role);
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(roles, vec!["user", "assistant"]);
    let turns = ai_hist::session_user_turns_page(&conn, "antigravity", sid, 1, None).unwrap();
    assert_eq!(turns.user_turns.len(), 1);
    assert_eq!(turns.user_turns[0].message_id.as_deref(), Some("step:0"));
    assert_eq!(
        turns.user_turns[0].following_message_id.as_deref(),
        Some("step:1")
    );
    assert!(turns.next_cursor.is_none());
}

#[test]
fn a_gap_does_not_defer_complete_messages_at_different_recorded_times() {
    let home = tempfile::tempdir().unwrap();
    let mut rows = FIXTURE
        .lines()
        .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap())
        .collect::<Vec<_>>();
    rows[1]["created_at"] = "2026-10-11T12:00:01Z".into();
    let full = rows
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    rows[0]["truncated_fields"] = serde_json::json!(["content"]);
    let compact = rows
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    stage(home.path(), "transcript.jsonl", &compact);
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert_eq!(
        read(&store)
            .messages
            .iter()
            .map(|m| m.role)
            .collect::<Vec<_>>(),
        vec![Role::Assistant]
    );
    stage(home.path(), "transcript_full.jsonl", &full);
    store
        .hydrate(&reference(), HydrateOptions::default())
        .unwrap();
    assert_eq!(
        read(&store)
            .messages
            .iter()
            .map(|m| m.role)
            .collect::<Vec<_>>(),
        vec![Role::User, Role::Assistant]
    );
}

#[test]
fn nonterminal_message_gap_defers_new_equal_time_suffix_until_complete() {
    let home = tempfile::tempdir().unwrap();
    let mut rows = FIXTURE
        .lines()
        .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap())
        .collect::<Vec<_>>();
    rows[0]["status"] = "ACTIVE".into();
    let partial = rows
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    let full = stage(home.path(), "transcript_full.jsonl", &partial);
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert!(read(&store).messages.is_empty());
    std::fs::write(full, FIXTURE).unwrap();
    store
        .hydrate(&reference(), HydrateOptions::default())
        .unwrap();
    assert_eq!(
        read(&store)
            .messages
            .iter()
            .map(|m| m.role)
            .collect::<Vec<_>>(),
        vec![Role::User, Role::Assistant]
    );
}
