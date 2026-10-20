//! Public OpenClaw acquisition acceptance, using only synthetic fixture homes
//! written by released writers: 2026.10.4 (agent schema 24), 2026.8.1
//! (schema 19) and 2026.7.35 (the JSONL store).
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

fn copy_tree(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

#[test]
fn openclaw_older_layouts_public_history_conformance() {
    let schema19 = tempfile::tempdir().unwrap();
    stage(
        schema19.path(),
        "main",
        include_str!("fixtures/openclaw/main-schema19.sql"),
    );
    let first = assert_history_conformance(
        schema19.path(),
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
    assert_eq!(first.tool_calls.len(), 1);

    let legacy = tempfile::tempdir().unwrap();
    copy_tree(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/openclaw/legacy-jsonl"),
        legacy.path(),
    );
    let evidence = assert_history_conformance(
        legacy.path(),
        HistoryCase {
            source: Source::OpenClaw,
            session_id: "main:fixture-main-0001",
            messages: &[
                (Role::User, "Find the OpenClaw fixture needle"),
                (Role::Assistant, "I will read the notes."),
                (Role::Assistant, "Found the OpenClaw fixture needle."),
            ],
        },
    );
    assert_eq!(evidence.tool_calls.len(), 1);
    assert_eq!(evidence.tool_results.len(), 1);
    // The same requests as the database layout; the mirror stays unmetered.
    let measured: Vec<_> = evidence
        .requests
        .iter()
        .filter_map(|r| r.usage.as_ref())
        .collect();
    assert_eq!(measured.len(), 2);
    assert_eq!(measured.iter().map(|u| u.input_tokens).sum::<u64>(), 230);
}
