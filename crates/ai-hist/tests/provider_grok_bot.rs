//! Public Grok Bot acquisition acceptance, using only invented local blobs.
#[path = "provider_support/grok_bot_fixture.rs"]
mod grok_bot_fixture;
mod provider_support;

use ai_hist::{
    CatalogQuery, DiscoveryOptions, HydrateOptions, ProviderRoots, Role, SessionQuery, SessionRef,
    SessionStore, Source, StoreOptions, SyncOptions,
};
use std::path::Path;

const SID: &str = "fixture-bot-complete";

fn open(home: &Path) -> SessionStore {
    SessionStore::open(
        StoreOptions::default()
            .db_path(home.join("conformance.db"))
            .roots(ProviderRoots::from_home(
                home.into(),
                home.join("opencode.db"),
            )),
    )
    .unwrap()
}

#[test]
fn grok_bot_discover_hydrate_read_reopen_changes_export_fullsync() {
    let home = tempfile::tempdir().unwrap();
    grok_bot_fixture::stage(home.path());
    let evidence = provider_support::assert_history_conformance(
        home.path(),
        provider_support::HistoryCase {
            source: Source::GrokBot,
            session_id: SID,
            messages: &[
                (Role::User, "Plan a fixture picnic."),
                (Role::Assistant, "Pack lunch and a map."),
                (Role::User, "North Park"),
                (Role::Assistant, "The basket is ready."),
            ],
        },
    );
    assert_eq!(
        evidence.session.title.as_deref(),
        Some("Fixture Research Bot")
    );
    assert!(evidence
        .markers
        .iter()
        .any(|marker| marker.kind == "voice-call"));
    assert!(evidence
        .markers
        .iter()
        .any(|marker| marker.kind == "widget"));
    assert!(evidence
        .relationships
        .iter()
        .any(|edge| edge.child_session_id.as_deref() == Some("bc-fixture-child")));
    assert!(!evidence
        .user_turns
        .iter()
        .any(|turn| turn.message_id.as_deref() == Some("t0s5")));
    let capabilities = Source::GrokBot.capabilities().relationships;
    assert_eq!(capabilities.stable_child_identity, "always");
    assert!(capabilities.records_agent_type);
    assert!(capabilities.records_spawn_time);
    assert!(capabilities.records_evidence_locator);
    assert!(evidence
        .messages
        .iter()
        .all(|message| message.usage.is_none()));
}

#[test]
fn grok_bot_sliding_window_accumulates_and_ignores_unrelated_files() {
    let home = tempfile::tempdir().unwrap();
    let path = grok_bot_fixture::stage(home.path());
    let user_data = grok_bot_fixture::persistence(home.path())
        .parent()
        .unwrap()
        .to_path_buf();
    std::fs::write(user_data.join("sand-secrets.json"), b"not JSON credentials").unwrap();
    std::fs::write(user_data.join("Cookies"), b"not a transcript").unwrap();
    std::fs::write(
        grok_bot_fixture::persistence(home.path()).join("unrelated.blob"),
        b"not JSON",
    )
    .unwrap();
    let store = open(home.path());
    let discovered = store
        .discover(DiscoveryOptions::default().sources([Source::GrokBot]))
        .unwrap();
    assert_eq!(discovered.discovered, 3);
    store
        .hydrate(
            &SessionRef::id(Source::GrokBot, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    let mut replica: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    replica["value"]["entries"] = serde_json::json!([
        {"kind":"message","id":"t9u","role":"user","content":"A later fixture window.","timestampMs":1700000030000_i64,"seq":99},
        {"kind":"message","id":"t9agent","role":"user","content":"Later route update.","fromAgent":{"id":"bc-fixture-child","name":"Route helper"},"timestampMs":1700000030500_i64,"seq":100},
        {"kind":"send-message","id":"t9s1","message":{"type":"text","content":"Later answer."},"timestampMs":1700000031000_i64,"seq":101}
    ]);
    replica["value"]["precedingTimestampMs"] = 1700000029000_i64.into();
    replica["value"]["persistedAt"] = 1700000032000_i64.into();
    std::fs::write(&path, serde_json::to_vec(&replica).unwrap()).unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    let evidence = store
        .session(
            &SessionRef::id(Source::GrokBot, SID),
            SessionQuery::default(),
        )
        .unwrap()
        .unwrap();
    let texts: Vec<_> = evidence
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .filter_map(|b| b.text.as_deref())
        .collect();
    assert!(texts.contains(&"Plan a fixture picnic."));
    assert!(texts.contains(&"A later fixture window."));
    assert!(evidence
        .markers
        .iter()
        .any(|marker| marker.kind == "truncated_tail"));
    let child = evidence
        .relationships
        .iter()
        .find(|edge| edge.child_session_id.as_deref() == Some("bc-fixture-child"))
        .unwrap();
    assert_eq!(child.spawned_at_ms, Some(1700000003000));
    assert_eq!(child.evidence_ref.as_deref(), Some("t0s3"));
    assert!(!evidence
        .user_turns
        .iter()
        .any(|turn| turn.message_id.as_deref() == Some("t9agent")));
    std::fs::remove_file(&path).unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert!(
        store
            .session(
                &SessionRef::id(Source::GrokBot, SID),
                SessionQuery::default()
            )
            .unwrap()
            .is_some(),
        "expired client replica must not erase accumulated history"
    );
}

#[test]
fn grok_bot_voice_turns_bound_shallow_and_full_activity() {
    let home = tempfile::tempdir().unwrap();
    let path = grok_bot_fixture::stage(home.path());
    let mut replica: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    replica["value"]["entries"][6]["conversation"]["turns"]
        .as_array_mut()
        .unwrap()
        .push(
            serde_json::json!({"speaker":"system","text":"Call metadata","atMs":1700000006300_i64}),
        );
    replica["value"]["entries"]
        .as_array_mut()
        .unwrap()
        .truncate(7);
    std::fs::write(path, serde_json::to_vec(&replica).unwrap()).unwrap();
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::GrokBot]))
        .unwrap();
    let catalog = || {
        store
            .sessions(CatalogQuery::default())
            .map(Result::unwrap)
            .find(|session| session.session_id == SID)
            .unwrap()
    };
    let shallow = catalog();
    assert_eq!(shallow.first_activity_ms, Some(1700000000000));
    assert_eq!(shallow.last_activity_ms, Some(1700000006200));
    store
        .hydrate(
            &SessionRef::id(Source::GrokBot, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    let full = catalog();
    assert_eq!(full.first_activity_ms, Some(1700000000000));
    assert_eq!(full.last_activity_ms, Some(1700000006200));
}

#[test]
fn grok_bot_explicitly_cleared_roster_title_is_removed() {
    let home = tempfile::tempdir().unwrap();
    grok_bot_fixture::stage(home.path());
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::GrokBot]))
        .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::GrokBot, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    let roster_path = grok_bot_fixture::persistence(home.path()).join(format!(
        "{}.blob",
        grok_bot_fixture::encode_key("sand.client.slice.account.fixture%7Cp.roster.last-roster")
    ));
    let mut roster: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&roster_path).unwrap()).unwrap();
    let row = roster["value"]["rows"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|row| row["id"] == SID)
        .unwrap();
    row["name"] = "".into();
    row["title"] = "".into();
    std::fs::write(&roster_path, serde_json::to_vec(&roster).unwrap()).unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    let catalog = store
        .sessions(CatalogQuery::default())
        .map(Result::unwrap)
        .find(|session| session.session_id == SID)
        .unwrap();
    assert_eq!(catalog.title, None);
}

#[test]
fn grok_bot_unmapped_entry_shapes_and_newer_roster_still_hydrate() {
    let home = tempfile::tempdir().unwrap();
    let path = grok_bot_fixture::stage(home.path());
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/grok-bot/replicas");
    std::fs::copy(fixtures.join("drift-replica.json"), &path).unwrap();
    std::fs::copy(
        fixtures.join("drift-roster.json"),
        grok_bot_fixture::persistence(home.path()).join(format!(
            "{}.blob",
            grok_bot_fixture::encode_key(
                "sand.client.slice.account.fixture%7Cp.roster.last-roster"
            )
        )),
    )
    .unwrap();
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::GrokBot]))
        .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::GrokBot, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    let evidence = store
        .session(
            &SessionRef::id(Source::GrokBot, SID),
            SessionQuery::default(),
        )
        .unwrap()
        .unwrap();
    let texts: Vec<_> = evidence
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .filter_map(|b| b.text.as_deref())
        .collect();
    assert_eq!(
        texts,
        [
            "Draft the fixture plan.",
            "Handing the fixture plan to a peer.",
            "The fixture plan is drafted."
        ]
    );
    assert_eq!(evidence.session.title.as_deref(), Some("Fixture Drift Bot"));
    for (kind, id) in [("message", "t0n1"), ("send-message", "t0s1")] {
        assert!(
            evidence
                .markers
                .iter()
                .any(|marker| marker.kind == kind && marker.message_id.as_deref() == Some(id)),
            "{id} is kept as a {kind} marker"
        );
    }
    assert!(evidence.relationships.is_empty());
}

#[test]
fn grok_bot_unusable_roster_keeps_transcript_and_last_title() {
    let home = tempfile::tempdir().unwrap();
    grok_bot_fixture::stage(home.path());
    let roster = grok_bot_fixture::persistence(home.path()).join(format!(
        "{}.blob",
        grok_bot_fixture::encode_key("sand.client.slice.account.fixture%7Cp.roster.last-roster")
    ));
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::GrokBot]))
        .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::GrokBot, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    let title = || {
        store
            .sessions(CatalogQuery::default())
            .map(Result::unwrap)
            .find(|session| session.session_id == SID)
            .unwrap()
            .title
    };
    assert_eq!(title().as_deref(), Some("Fixture Research Bot"));
    for contents in [
        &br#"{"schemaVersion":6,"value":{"rows":[{"id":"fixture-bot-complete","displayName":"Renamed"}]}}"#[..],
        b"not JSON",
    ] {
        std::fs::write(&roster, contents).unwrap();
        store.sync(SyncOptions::default().force(true)).unwrap();
        assert_eq!(title().as_deref(), Some("Fixture Research Bot"));
    }
    std::fs::remove_file(&roster).unwrap();
    std::fs::create_dir(&roster).unwrap();
    let fresh = SessionStore::open(
        StoreOptions::default()
            .db_path(home.path().join("fresh.db"))
            .roots(ProviderRoots::from_home(
                home.path().into(),
                home.path().join("opencode.db"),
            )),
    )
    .unwrap();
    let discovered = fresh
        .discover(DiscoveryOptions::default().sources([Source::GrokBot]))
        .unwrap();
    assert_eq!(discovered.discovered, 3);
    fresh
        .hydrate(
            &SessionRef::id(Source::GrokBot, SID),
            HydrateOptions::default(),
        )
        .unwrap();
}

#[test]
fn grok_bot_skipped_opening_entry_is_not_a_truncated_window() {
    let home = tempfile::tempdir().unwrap();
    let path = grok_bot_fixture::stage(home.path());
    let mut replica: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    replica["value"]["entries"][0]["timestampMs"] = "not a time".into();
    std::fs::write(&path, serde_json::to_vec(&replica).unwrap()).unwrap();
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::GrokBot]))
        .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::GrokBot, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    let evidence = store
        .session(
            &SessionRef::id(Source::GrokBot, SID),
            SessionQuery::default(),
        )
        .unwrap()
        .unwrap();
    assert!(!evidence
        .markers
        .iter()
        .any(|marker| marker.kind == "truncated_tail"));
}
