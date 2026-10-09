//! Claude records that open a streamed response, and the records that name an
//! explicit continuity target, are attributed through the public facade.

use ai_hist::{ProviderRoots, SessionQuery, SessionRef, SessionStore, Source, StoreOptions};
use std::fs;
use std::path::Path;

const MULTI_BLOCK: &str = "22222222-2222-2222-2222-222222222222";
const EXPLICIT: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";

fn stage(home: &Path, name: &str) {
    let from = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/claude")
        .join(name);
    let project = home.join(".claude/projects/-tmp-project");
    fs::create_dir_all(&project).unwrap();
    fs::copy(from, project.join(name)).unwrap();
}

#[allow(clippy::field_reassign_with_default)]
fn synced(home: &Path) -> SessionStore {
    let mut options = StoreOptions::default();
    options.db_path = Some(home.join("ai-history.db"));
    options.roots = Some(ProviderRoots::from_home(
        home.to_path_buf(),
        home.join(".local/share/opencode/opencode.db"),
    ));
    let store = SessionStore::open(options).expect("open");
    store.sync(Default::default()).expect("sync");
    store
}

/// `(message_ids, first_ts_ms, has_thinking)` of the multi-block request.
fn opening_request(store: &SessionStore) -> (Vec<String>, i64, bool) {
    let evidence = store
        .session(
            &SessionRef::id(Source::Claude, MULTI_BLOCK),
            SessionQuery::default(),
        )
        .unwrap()
        .unwrap();
    assert_eq!(evidence.requests.len(), 1);
    let request = &evidence.requests[0];
    let mut ids = request.message_ids.clone();
    ids.sort();
    (ids, request.first_ts_ms, request.has_thinking)
}

/// `(message_id, ts_ms, payload)` of the opening record's signature marker.
fn opening_marker(store: &SessionStore) -> Option<(String, i64, serde_json::Value)> {
    let evidence = store
        .session(
            &SessionRef::id(Source::Claude, MULTI_BLOCK),
            SessionQuery::default(),
        )
        .unwrap()
        .unwrap();
    evidence
        .markers
        .iter()
        .find(|marker| {
            marker.subkind.as_deref() == Some("thinking_signature")
                && marker.message_id.as_deref() == Some("u-asst-1a")
        })
        .map(|marker| {
            (
                marker.message_id.clone().unwrap(),
                marker.ts_ms.unwrap(),
                marker.payload.clone().unwrap(),
            )
        })
}

/// `spawned_at_ms` of the edge whose parent is `parent`.
fn spawned_at(store: &SessionStore, parent: &str) -> Option<i64> {
    let evidence = store
        .session(
            &SessionRef::id(Source::Claude, EXPLICIT),
            SessionQuery::default(),
        )
        .unwrap()
        .unwrap();
    evidence
        .relationships
        .iter()
        .find(|edge| edge.parent_session_id == parent)
        .expect("edge")
        .spawned_at_ms
}

/// The records that store events; the opening record stores only its marker.
const WITH_EVENTS: [&str; 3] = ["u-asst-1b", "u-asst-1c", "u-asst-1d"];
/// 2026-04-20T00:00:01.000Z, the signature-only record that opens the response.
const RESPONSE_START: i64 = 1_776_643_201_000;
/// 2026-04-20T00:00:01.500Z, the response's first record that stores an event.
const FIRST_EVENT: i64 = 1_776_643_201_500;
/// 2026-04-24T02:00:00.000Z, the user line carrying `continuedFromSessionId`.
const CONTINUED_AT: i64 = 1_776_996_000_000;
/// 2026-04-24T02:00:01.000Z, the assistant line carrying `forkSessionId`.
const FORKED_AT: i64 = 1_776_996_001_000;

#[test]
fn a_fresh_store_attributes_the_opening_record_and_the_naming_records() {
    let dir = tempfile::tempdir().unwrap();
    stage(dir.path(), "multi-block-turn.jsonl");
    stage(dir.path(), "explicit-line-relationships.jsonl");
    let store = synced(dir.path());
    assert_eq!(
        opening_request(&store),
        (WITH_EVENTS.map(String::from).to_vec(), FIRST_EVENT, false),
        "the request is grouped from events; the opening record has none"
    );
    assert_eq!(
        opening_marker(&store),
        Some((
            "u-asst-1a".to_string(),
            RESPONSE_START,
            serde_json::json!({
                "bytes": 3,
                "has_signature": true,
                "request_id": "req_1",
                "provider_message_id": "msg_multi_1",
            })
        )),
        "the opening record's marker carries when the request started and names it"
    );
    assert_eq!(spawned_at(&store, "original-session"), Some(CONTINUED_AT));
    assert_eq!(spawned_at(&store, "fork-source-session"), Some(FORKED_AT));
}
