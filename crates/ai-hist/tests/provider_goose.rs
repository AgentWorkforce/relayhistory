mod provider_support;
use ai_hist::{
    DiscoveryOptions, HydrateOptions, ProviderRoots, Role, SessionLocation, SessionQuery,
    SessionRef, SessionStore, ShallowSession, Source, SourceObservation, SourceObservationBatch,
    StoreOptions, SyncOptions,
};
use provider_support::{assert_history_conformance, HistoryCase};

const SID: &str = "default:32303236313032325f31";

fn open(home: &std::path::Path) -> SessionStore {
    SessionStore::open(
        StoreOptions::default()
            .db_path(home.join("ledger.db"))
            .roots(ProviderRoots::from_home(
                home.into(),
                home.join("opencode.db"),
            )),
    )
    .unwrap()
}

fn stage(home: &std::path::Path, sql: &str) {
    let dir = home.join(".local/share/goose/sessions");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("sessions.db");
    if path.exists() {
        std::fs::remove_file(&path).unwrap();
    }
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.execute_batch(sql).unwrap();
    assert_eq!(
        conn.query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| r
            .get::<_, i64>(
            0
        ))
        .unwrap(),
        0
    );
}

fn hydrate(store: &SessionStore) -> ai_hist::SessionEvidence {
    store
        .discover(DiscoveryOptions::default().sources([Source::Goose]))
        .unwrap();
    let reference = SessionRef::id(Source::Goose, SID);
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap()
}

#[test]
fn goose_native_schema17_public_history_conformance() {
    let home = tempfile::tempdir().unwrap();
    let parent = home.path().join(".local/share/goose/sessions");
    std::fs::create_dir_all(&parent).unwrap();
    let native = rusqlite::Connection::open(parent.join("sessions.db")).unwrap();
    native
        .execute_batch(include_str!("fixtures/goose/schema17.sql"))
        .unwrap();
    let violations: i64 = native
        .query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(
        violations, 0,
        "native fixture must have no broken foreign keys"
    );
    drop(native);
    let evidence = assert_history_conformance(
        home.path(),
        HistoryCase {
            source: Source::Goose,
            session_id: SID,
            messages: &[
                (Role::User, "Find the Goose fixture needle"),
                (Role::Assistant, "Found the Goose fixture needle."),
                (Role::User, "Continue after restart."),
                (Role::Assistant, "The historical answer is still available."),
            ],
        },
    );
    assert_eq!(evidence.tool_calls.len(), 1);
    assert_eq!(evidence.tool_calls[0].tool_use_id, "call-native-1");
    assert_eq!(evidence.tool_results.len(), 1);
    assert_eq!(
        evidence.tool_results[0].tool_use_id.as_deref(),
        Some("call-native-1")
    );
    assert!(evidence
        .requests
        .iter()
        .all(|request| request.usage.is_none()));
    let oracle: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/goose/schema17-official-export.json")).unwrap();
    assert_eq!(oracle["conversation"].as_array().unwrap().len(), 6);
    assert_eq!(oracle["id"], "20261022_1");
}

#[test]
fn goose_native_replace_reorders_stable_ids_and_retires_removed_result() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), include_str!("fixtures/goose/schema17.sql"));
    let store = open(home.path());
    let before = hydrate(&store);
    assert_eq!(before.tool_results.len(), 1);
    // Native replace_conversation retains message IDs but deletes and reinserts
    // SQLite rows. Align its recorded seconds with the original writer to
    // exercise same-millisecond order repair independently of clock drift.
    stage(home.path(), include_str!("fixtures/goose/rewritten.sql"));
    let native =
        rusqlite::Connection::open(home.path().join(".local/share/goose/sessions/sessions.db"))
            .unwrap();
    native.execute("UPDATE messages SET created_timestamp=1792642821 WHERE message_id IN ('user-native-1','assistant-native-1','assistant-native-2')", []).unwrap();
    native.execute("UPDATE messages SET created_timestamp=1792642824 WHERE message_id IN ('user-native-2','assistant-native-3')", []).unwrap();
    drop(native);
    let after = hydrate(&store);
    assert_eq!(after.tool_calls.len(), 1);
    assert!(after.tool_results.is_empty());
    let ids: Vec<_> = after
        .messages
        .iter()
        .map(|message| message.message_id.as_deref().unwrap())
        .collect();
    assert_eq!(
        ids,
        [
            "user-native-1",
            "assistant-native-2",
            "assistant-native-1",
            "user-native-2",
            "assistant-native-3"
        ]
    );
    let oracle: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/goose/rewritten-official-export.json"))
            .unwrap();
    let official_ids: Vec<_> = oracle["conversation"]
        .as_array()
        .unwrap()
        .iter()
        .map(|message| message["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, official_ids);
    assert!(after
        .messages
        .iter()
        .flat_map(|message| &message.blocks)
        .any(|block| block.text.as_deref() == Some("The revised historical answer is available.")));
    drop(store);
    let reopened = open(home.path());
    let again = reopened
        .session(&SessionRef::id(Source::Goose, SID), SessionQuery::default())
        .unwrap()
        .unwrap();
    assert_eq!(after.messages, again.messages);
    assert_eq!(after.tool_calls, again.tool_calls);
    assert_eq!(after.tool_results, again.tool_results);
}

#[test]
fn goose_rewrite_cannot_replace_remote_owned_evidence() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), include_str!("fixtures/goose/schema17.sql"));
    let store = open(home.path());
    let prior = hydrate(&store);
    let ledger = rusqlite::Connection::open(home.path().join("ledger.db")).unwrap();
    ledger
        .execute(
            "UPDATE session_events SET location='remote' WHERE source='goose' AND session_id=?1 AND message_id='assistant-native-2'",
            [SID],
        )
        .unwrap();
    drop(ledger);
    stage(home.path(), include_str!("fixtures/goose/rewritten.sql"));
    store
        .discover(DiscoveryOptions::default().sources([Source::Goose]))
        .unwrap();
    let reference = SessionRef::id(Source::Goose, SID);
    let error = store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap_err();
    assert!(error.to_string().contains("SHARED"), "{error}");
    let after = store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    assert_eq!(after.messages, prior.messages);
    assert_eq!(after.tool_calls, prior.tool_calls);
    assert_eq!(after.tool_results, prior.tool_results);
}

#[test]
fn goose_native_deletion_retires_local_observation_and_keeps_remote_catalog() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), include_str!("fixtures/goose/schema17.sql"));
    let store = open(home.path());
    hydrate(&store);
    store
        .apply_source_observations(SourceObservationBatch::new(
            "goose-remote",
            "fixture",
            SessionLocation::Remote,
            vec![SourceObservation {
                session: ShallowSession {
                    source: "goose".into(),
                    session_id: SID.into(),
                    title: Some("Remote Goose title".into()),
                    first_prompt: Some("Remote Goose prompt".into()),
                    source_stamp: Some("remote-stamp".into()),
                    ..Default::default()
                },
                raw_locator: Some("remote-goose-locator".into()),
            }],
        ))
        .unwrap();
    std::fs::remove_file(home.path().join(".local/share/goose/sessions/sessions.db")).unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    let conn = rusqlite::Connection::open(home.path().join("ledger.db")).unwrap();
    let local: i64 = conn
        .query_row("SELECT count(*) FROM session_observations WHERE source='goose' AND session_id=?1 AND location='local'", [SID], |r| r.get(0))
        .unwrap();
    assert_eq!(local, 0);
    let remote: i64 = conn
        .query_row("SELECT count(*) FROM session_observations WHERE source='goose' AND session_id=?1 AND location='remote'", [SID], |r| r.get(0))
        .unwrap();
    assert_eq!(remote, 1);
    let path: Option<String> = conn
        .query_row(
            "SELECT raw_path FROM sessions WHERE source='goose' AND session_id=?1",
            [SID],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(path.as_deref(), Some("remote-goose-locator"));
    let remaining = store
        .session(&SessionRef::id(Source::Goose, SID), SessionQuery::default())
        .unwrap()
        .unwrap();
    assert!(remaining.messages.is_empty());
    assert!(remaining.tool_calls.is_empty());
    assert!(remaining.tool_results.is_empty());
}
