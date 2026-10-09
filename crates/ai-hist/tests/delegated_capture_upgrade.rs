//! A store written before Claude delegation was recorded as the provider
//! wrote it heals on the next pass over unchanged transcripts — by plain
//! `sync` and by targeted hydration alike.
//!
//! The fixture is `claude/nested-sidecars`. Each test captures it, rewrites
//! the rows into the shape the previous parser left (the nested edge on the
//! root, no child model, no spawn-result `agent_id`) together with the state
//! that parser recorded, and runs one more pass with every file untouched.

use ai_hist::{
    open_db, DiscoveryOptions, HydrateOptions, ProviderRoots, RelationshipSide, SessionQuery,
    SessionRef, SessionStore, Source, StoreOptions, SyncOptions,
};
use std::fs;
use std::path::Path;

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/claude/nested-sidecars"
);
const MAIN: &str = "sidecar-session";

fn copy_tree(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for entry in fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), &target).unwrap();
        }
    }
}

fn open(home: &Path) -> SessionStore {
    let mut options = StoreOptions::default();
    options.db_path = Some(home.join("ai-history.db"));
    options.roots = Some(ProviderRoots::from_home(
        home.to_path_buf(),
        home.join(".local/share/opencode/opencode.db"),
    ));
    SessionStore::open(options).expect("open")
}

fn hydrate(store: &SessionStore) {
    let mut options = HydrateOptions::default();
    options.include_related = true;
    store
        .hydrate(&SessionRef::id(Source::Claude, MAIN), options)
        .expect("hydrate");
}

/// The rows the previous parser wrote for this fixture.
fn degrade_to_previous_parser(home: &Path) {
    let conn = open_db(&home.join("ai-history.db")).unwrap();
    conn.execute(
        "UPDATE session_events SET agent_id = NULL WHERE source = 'claude' AND kind = 'tool_result'",
        [],
    )
    .unwrap();
    conn.execute(
        "UPDATE session_relationships SET child_model = NULL WHERE source = 'claude'",
        [],
    )
    .unwrap();
    conn.execute(
        "UPDATE session_relationships SET parent_session_id = ?1 \
         WHERE source = 'claude' AND child_session_id = 'a2'",
        [MAIN],
    )
    .unwrap();
}

/// What the store reads back: the spawn result's child, and each edge as
/// `(parent, child, child_model)`.
type Observed = (Option<String>, Vec<(String, String, Option<String>)>);

fn observed(store: &SessionStore) -> Observed {
    let read = |id: &str| {
        store
            .session(&SessionRef::id(Source::Claude, id), SessionQuery::default())
            .unwrap()
            .unwrap_or_else(|| panic!("{id} is readable"))
    };
    let main = read(MAIN);
    let spawn = main
        .tool_results
        .iter()
        .find(|result| result.tool_use_id.as_deref() == Some("toolu_explore"))
        .and_then(|result| result.agent_id.clone());
    let mut edges = Vec::new();
    for id in [MAIN, "a1", "a2"] {
        for edge in read(id).relationships {
            if edge.side == RelationshipSide::Parent && edge.relationship == "delegated" {
                edges.push((
                    edge.parent_session_id,
                    edge.child_session_id.unwrap(),
                    edge.child_model,
                ));
            }
        }
    }
    edges.sort();
    (spawn, edges)
}

fn expected() -> Observed {
    (
        Some("a1".to_string()),
        vec![
            (
                "a1".to_string(),
                "a2".to_string(),
                Some("claude-sonnet-4-6".to_string()),
            ),
            (
                MAIN.to_string(),
                "a1".to_string(),
                Some("claude-haiku-4-5".to_string()),
            ),
        ],
    )
}

#[test]
fn plain_sync_heals_delegation_an_earlier_parser_recorded() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path();
    copy_tree(Path::new(FIXTURE), home);
    let store = open(home);
    store.sync(SyncOptions::default()).unwrap();
    assert_eq!(observed(&store), expected());

    degrade_to_previous_parser(home);
    // The previous build never wrote the delegation pass's key, and its sweep
    // fingerprint was taken under a generation without it.
    let state_path = home.join(".sync-state.json");
    let mut state: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(&fs::read_to_string(&state_path).unwrap()).unwrap();
    assert!(state.remove("claude_delegation_capture_v1").is_some());
    let fingerprint = state
        .get("source_fingerprint")
        .and_then(|value| value.as_str())
        .expect("a sweep fingerprint")
        .replacen('g', "g0", 1);
    state.insert("source_fingerprint".into(), fingerprint.into());
    fs::write(&state_path, serde_json::to_string(&state).unwrap()).unwrap();
    assert_ne!(observed(&store), expected());

    store.sync(SyncOptions::default()).unwrap();
    assert_eq!(observed(&store), expected());
}

#[test]
fn hydration_heals_delegation_an_earlier_parser_recorded() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path();
    copy_tree(Path::new(FIXTURE), home);
    let store = open(home);
    store.discover(DiscoveryOptions::default()).unwrap();
    hydrate(&store);
    assert_eq!(observed(&store), expected());

    degrade_to_previous_parser(home);
    open_db(&home.join("ai-history.db"))
        .unwrap()
        .execute(
            "UPDATE observation_hydration_checkpoints SET parser_version = 15 \
             WHERE source = 'claude' AND session_id = ?1",
            [MAIN],
        )
        .unwrap();
    assert_ne!(observed(&store), expected());

    hydrate(&store);
    assert_eq!(observed(&store), expected());
}

#[test]
fn plain_sync_heals_a_nested_edge_that_kept_its_model() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path();
    copy_tree(Path::new(FIXTURE), home);
    let store = open(home);
    store.sync(SyncOptions::default()).unwrap();

    // Only the parent is wrong: the model and the tool use are recorded, the
    // depth is not.
    let conn = open_db(&home.join("ai-history.db")).unwrap();
    conn.execute(
        "UPDATE session_relationships SET parent_session_id = ?1, spawn_depth = NULL \
         WHERE source = 'claude' AND child_session_id = 'a2'",
        [MAIN],
    )
    .unwrap();
    drop(conn);
    let state_path = home.join(".sync-state.json");
    let mut state: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(&fs::read_to_string(&state_path).unwrap()).unwrap();
    state.remove("claude_delegation_capture_v1");
    state.remove("source_fingerprint");
    fs::write(&state_path, serde_json::to_string(&state).unwrap()).unwrap();
    assert_ne!(observed(&store), expected());

    store.sync(SyncOptions::default()).unwrap();
    assert_eq!(observed(&store), expected());
}

#[test]
fn a_child_whose_only_rows_are_its_own_delegations_is_readable() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path();
    copy_tree(Path::new(FIXTURE), home);
    let store = open(home);
    store.sync(SyncOptions::default()).unwrap();

    let conn = open_db(&home.join("ai-history.db")).unwrap();
    for table in [
        "session_events",
        "tool_calls",
        "session_markers",
        "file_edits",
        "history",
    ] {
        conn.execute(
            &format!("DELETE FROM {table} WHERE source = 'claude' AND session_id = 'a1'"),
            [],
        )
        .unwrap();
    }
    drop(conn);

    let a1 = store
        .session(
            &SessionRef::id(Source::Claude, "a1"),
            SessionQuery::default(),
        )
        .unwrap()
        .expect("a1 still holds its delegation to a2");
    assert!(a1.messages.is_empty());
    assert!(a1
        .relationships
        .iter()
        .any(|edge| edge.side == RelationshipSide::Parent
            && edge.child_session_id.as_deref() == Some("a2")));
}
