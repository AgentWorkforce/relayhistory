//! Public OpenClaw acquisition acceptance, using only synthetic fixture homes
//! written by the released 2026.10.4 writer.
mod provider_support;
use ai_hist::{BlockKind, Role, Source};
use provider_support::{assert_history_conformance, HistoryCase};
fn stage(home: &std::path::Path, agent: &str, sql: &str) {
    let path = home.join(format!(
        ".openclaw/agents/{agent}/agent/openclaw-agent.sqlite"
    ));
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let db = rusqlite::Connection::open(path).unwrap();
    db.execute_batch(sql).unwrap();
}
#[test]
fn openclaw_public_history_conformance() {
    let home = tempfile::tempdir().unwrap();
    stage(
        home.path(),
        "main",
        include_str!("fixtures/openclaw/main-schema24.sql"),
    );
    stage(
        home.path(),
        "ops",
        include_str!("fixtures/openclaw/ops-schema24.sql"),
    );
    let evidence = assert_history_conformance(
        home.path(),
        HistoryCase {
            source: Source::OpenClaw,
            session_id: "main:fixture/main:α",
            messages: &[
                (Role::User, "Find the OpenClaw fixture needle"),
                (Role::Assistant, "I will read the notes."),
                (Role::Assistant, "Found the OpenClaw fixture needle."),
            ],
        },
    );
    assert_eq!(evidence.tool_calls.len(), 1);
    assert_eq!(evidence.tool_results.len(), 1);
    let id = &evidence.tool_calls[0].tool_use_id;
    assert_eq!(id, "call_fixture|read:1");
    let blocks: Vec<_> = evidence
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .filter(|b| matches!(b.kind, BlockKind::ToolUse | BlockKind::ToolResult))
        .collect();
    assert_eq!(blocks.len(), 2);
    assert!(blocks.iter().all(|b| b.tool_use_id.as_ref() == Some(id)));
    // Two measured requests; the delivery mirror repeats the reply unmetered.
    let measured: Vec<_> = evidence
        .requests
        .iter()
        .filter_map(|r| r.usage.as_ref())
        .collect();
    assert_eq!(measured.len(), 2);
    assert_eq!(measured.iter().map(|u| u.input_tokens).sum::<u64>(), 230);

    let other = tempfile::tempdir().unwrap();
    stage(
        other.path(),
        "ops",
        include_str!("fixtures/openclaw/ops-schema24.sql"),
    );
    let relay = assert_history_conformance(
        other.path(),
        HistoryCase {
            source: Source::OpenClaw,
            session_id: "ops:fixture-relay-0001",
            messages: &[
                (Role::User, "Relay question for the ops agent"),
                (Role::Assistant, "Relay answer from the ops agent."),
            ],
        },
    );
    assert!(relay.tool_calls.is_empty());
}
