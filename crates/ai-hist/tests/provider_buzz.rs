//! Public native Buzz ACP capture acceptance, using a synthetic Agent Relay home.
mod provider_support;
use ai_hist::{
    DiscoveryOptions, HydrateOptions, ProviderRoots, Role, SessionQuery, SessionRef, SessionStore,
    Source, StoreOptions,
};
use std::path::Path;

const SID: &str = "ses_0123456789abcdef";
const FIXTURE: &str = include_str!(
    "fixtures/buzz/native-acp/Library/Application Support/com.agentrelay.desktop/spawned-agents/buzz/ses_0123456789abcdef.events.jsonl"
);

fn stage(home: &Path) -> std::path::PathBuf {
    let path = home.join("Library/Application Support/com.agentrelay.desktop/spawned-agents/buzz/ses_0123456789abcdef.events.jsonl");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, FIXTURE).unwrap();
    path
}

#[test]
fn buzz_discover_hydrate_read_reopen_changes_export_fullsync() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path());
    let evidence = provider_support::assert_history_conformance(
        home.path(),
        provider_support::HistoryCase {
            source: Source::Buzz,
            session_id: SID,
            messages: &[
                (Role::User, "Inspect this project."),
                (Role::Assistant, "The README says Hello fixture."),
            ],
        },
    );
    assert_eq!(evidence.tool_calls.len(), 2);
    assert_eq!(evidence.tool_results.len(), 2);
    assert!(evidence.tool_calls.iter().any(|c| c.is_error == Some(true)));
    assert!(evidence
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .any(|b| b.text.as_deref() == Some("The read confirms the greeting.")));
    assert!(evidence.markers.iter().any(|m| m.kind == "usage_snapshot"));
    assert!(evidence.markers.iter().any(|m| m.kind == "acp_control"));
    assert!(evidence.markers.iter().any(|m| m.kind == "acp_update"));
}

#[test]
fn buzz_partial_append_and_rewrite_preserve_then_retire_evidence() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let store = SessionStore::open(
        StoreOptions::default()
            .db_path(home.path().join("history.db"))
            .roots(ProviderRoots::from_home(
                home.path().into(),
                home.path().join("opencode.db"),
            )),
    )
    .unwrap();
    let selected = SessionRef::id(Source::Buzz, SID);
    store
        .discover(DiscoveryOptions::default().sources([Source::Buzz]))
        .unwrap();
    let hydrate = || store.hydrate(&selected, HydrateOptions::default()).unwrap();
    let read = || {
        store
            .session(&selected, SessionQuery::default())
            .unwrap()
            .unwrap()
    };
    hydrate();
    assert_eq!(read().tool_calls.len(), 2);
    let row = format!("{{\"version\":1,\"session_id\":\"{SID}\",\"timestamp_ms\":1700000006000,\"kind\":\"prompt\",\"frame\":{{\"delivery_id\":\"2\",\"text\":\"Second task\"}}}}\n");
    std::fs::write(&path, format!("{FIXTURE}{}", &row[..30])).unwrap();
    hydrate();
    assert_eq!(read().tool_calls.len(), 2);
    assert_eq!(read().prompts.len(), 1);
    std::fs::write(&path, format!("{FIXTURE}{row}")).unwrap();
    hydrate();
    assert_eq!(read().prompts.len(), 2);
    std::fs::write(&path, FIXTURE).unwrap();
    hydrate();
    assert_eq!(read().prompts.len(), 1);
}
