//! Profile metadata stays consistent across shallow and full acquisition.
#[path = "provider_support/grok_bot_fixture.rs"]
mod grok_bot_fixture;

use ai_hist::{
    CatalogQuery, DiscoveryOptions, HydrateOptions, ProviderRoots, SessionQuery, SessionRef,
    SessionStore, Source, StoreOptions, SyncOptions,
};
use serde_json::{json, Value};

#[test]
fn normalized_names_survive_discover_sync_and_hydrate() {
    let home = tempfile::tempdir().unwrap();
    grok_bot_fixture::stage(home.path());
    let roster = grok_bot_fixture::persistence(home.path()).join(format!(
        "{}.blob",
        grok_bot_fixture::encode_key("sand.client.slice.account.fixture%7Cp.roster.last-roster")
    ));
    let store = SessionStore::open(
        StoreOptions::default()
            .db_path(home.path().join("profiles.db"))
            .roots(ProviderRoots::from_home(
                home.path().into(),
                home.path().join("opencode.db"),
            )),
    )
    .unwrap();
    let reference = SessionRef::id(Source::GrokBot, "fixture-bot-complete");
    let long = "界".repeat(130);
    let capped = "界".repeat(120);
    let cases = [
        (json!({"name":"  Saved name  "}), Some("Saved name"), true),
        (json!({"name":null}), Some("Saved name"), false),
        (json!({"name":42}), Some("Saved name"), false),
        (json!({"name":"bad\u{0000}name"}), Some("Saved name"), false),
        (json!({}), Some("Saved name"), false),
        (json!({"name":long}), Some(capped.as_str()), true),
        (
            json!({"name":null,"title":"  Fallback  "}),
            Some("Fallback"),
            true,
        ),
        (json!({"name":" \t\n "}), None, false),
    ];
    for (mut row, expected, has_profile) in cases {
        row["id"] = Value::String("fixture-bot-complete".into());
        std::fs::write(
            &roster,
            json!({"schemaVersion":4,"value":{"rows":[row]}}).to_string(),
        )
        .unwrap();
        store
            .discover(DiscoveryOptions::default().sources([Source::GrokBot]))
            .unwrap();
        assert_title(&store, expected);
        store
            .sync(
                SyncOptions::default()
                    .sources([Source::GrokBot])
                    .force(true),
            )
            .unwrap();
        assert_title(&store, expected);
        store
            .hydrate(&reference, HydrateOptions::default())
            .unwrap();
        assert_title(&store, expected);
        let profile = store.session_profile(&reference).unwrap();
        assert_eq!(profile.is_some(), has_profile);
        if let Some(profile) = profile {
            assert_eq!(profile.name.as_deref(), expected);
        }
        let captured = store
            .session(&reference, SessionQuery::default())
            .unwrap()
            .unwrap();
        assert_eq!(captured.session.title.as_deref(), expected);
    }
}

fn assert_title(store: &SessionStore, expected: Option<&str>) {
    let row = store
        .sessions(CatalogQuery::default())
        .map(Result::unwrap)
        .find(|row| row.session_id == "fixture-bot-complete")
        .unwrap();
    assert_eq!(row.title.as_deref(), expected);
}
