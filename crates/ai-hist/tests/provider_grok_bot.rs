//! Public Grok Bot acquisition acceptance, using only invented local blobs.
#[path = "provider_support/grok_bot_fixture.rs"]
mod grok_bot_fixture;
mod provider_support;

use ai_hist::{
    DiscoveryOptions, HydrateOptions, ProviderRoots, Role, SessionQuery, SessionRef, SessionStore,
    Source, StoreOptions, SyncOptions,
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
        {"kind":"send-message","id":"t9s1","message":{"type":"text","content":"Later answer."},"timestampMs":1700000031000_i64,"seq":100}
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
