mod provider_support;
use ai_hist::{Role, Source};
use provider_support::{assert_history_conformance, HistoryCase};
#[test]
fn hermes_public_history_conformance() {
    let home = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(home.path().join(".hermes")).unwrap();
    let db = rusqlite::Connection::open(home.path().join(".hermes/state.db")).unwrap();
    db.execute_batch(include_str!("fixtures/hermes/schema31.sql"))
        .unwrap();
    drop(db);
    let evidence = assert_history_conformance(
        home.path(),
        HistoryCase {
            source: Source::Hermes,
            session_id: "default:636861742f6e6f742d612d757569643aceb1",
            messages: &[
                (Role::User, "Find the Hermes fixture needle"),
                (Role::Assistant, "Found the Hermes fixture needle."),
                (Role::User, "Continue after restart."),
                (Role::Assistant, "The historical answer is still available."),
            ],
        },
    );
    assert_eq!(evidence.tool_calls.len(), 1);
    assert_eq!(evidence.tool_results.len(), 1);
    let id = &evidence.tool_calls[0].tool_use_id;
    assert_eq!(id, "call/native:1");
    let blocks: Vec<_> = evidence
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .filter(|b| {
            matches!(
                b.kind,
                ai_hist::BlockKind::ToolUse | ai_hist::BlockKind::ToolResult
            )
        })
        .collect();
    assert_eq!(blocks.len(), 2);
    assert!(blocks.iter().all(|b| b.tool_use_id.as_ref() == Some(id)));
    assert!(evidence.requests.iter().all(|r| r.usage.is_none()));
}

#[cfg(all(feature = "unstable-internal", feature = "export"))]
#[path = "provider_hermes/transport.rs"]
mod transport;
