//! Public Copilot CLI acquisition acceptance, using only synthetic fixture homes.
mod provider_support;
use ai_hist::{
    DiscoveryOptions, HydrateOptions, ProviderRoots, Role, SessionEvidence, SessionQuery,
    SessionRef, SessionStore, Source, StoreOptions, SyncOptions,
};
use std::path::{Path, PathBuf};

const IMPORTED: &str = "b3576354-cf9a-4912-8bd8-e969c9b94323";
const IMPORTED_JOURNAL: &str = include_str!(
    "fixtures/copilot/official-importer/.copilot/session-state/b3576354-cf9a-4912-8bd8-e969c9b94323/events.jsonl"
);
const USAGE: &str = "5f0c2a8e-1d4b-4c6e-9a7f-3b2d1e0c9a81";
const USAGE_JOURNAL: &str = include_str!(
    "fixtures/copilot/cli-usage/.copilot/session-state/5f0c2a8e-1d4b-4c6e-9a7f-3b2d1e0c9a81/events.jsonl"
);

fn stage(home: &Path, sid: &str, journal: &str) -> PathBuf {
    let path = home.join(format!(".copilot/session-state/{sid}/events.jsonl"));
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, journal).unwrap();
    path
}

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

fn discover(store: &SessionStore) -> ai_hist::DiscoveryReport {
    store
        .discover(DiscoveryOptions::default().sources([Source::Copilot]))
        .unwrap()
}

fn hydrate(store: &SessionStore, sid: &str) {
    store
        .hydrate(
            &SessionRef::id(Source::Copilot, sid),
            HydrateOptions::default(),
        )
        .unwrap();
}

fn read(store: &SessionStore, sid: &str) -> SessionEvidence {
    store
        .session(
            &SessionRef::id(Source::Copilot, sid),
            SessionQuery::default(),
        )
        .unwrap()
        .unwrap()
}

fn has(e: &SessionEvidence, text: &str) -> bool {
    e.messages
        .iter()
        .flat_map(|m| &m.blocks)
        .any(|b| b.text.as_deref() == Some(text))
}

#[test]
fn copilot_imported_journal_discover_hydrate_read_reopen_changes_export_fullsync() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), IMPORTED, IMPORTED_JOURNAL);
    let evidence = provider_support::assert_history_conformance(
        home.path(),
        provider_support::HistoryCase {
            source: Source::Copilot,
            session_id: IMPORTED,
            messages: &[
                (Role::User, "Inspect the synthetic greeting."),
                (Role::Assistant, "I will read the synthetic greeting."),
                (
                    Role::Assistant,
                    "The greeting says Hello synthetic fixture.",
                ),
            ],
        },
    );
    assert_eq!(evidence.tool_calls.len(), 1);
    assert_eq!(evidence.tool_results.len(), 1);
    assert_eq!(
        evidence.tool_calls[0].tool_use_id,
        "2bd062f7-b79a-4158-a23f-7b9a076aa23c"
    );
    assert_eq!(evidence.session.cwd.as_deref(), Some("/fixture/copilot"));
    assert!(evidence.markers.iter().any(|m| m.kind == "session.start"));
    assert!(evidence.file_edits.is_empty());
    assert!(evidence.relationships.is_empty());
}

#[test]
fn copilot_messages_match_the_official_reader_oracle() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), IMPORTED, IMPORTED_JOURNAL);
    let store = open(home.path());
    discover(&store);
    hydrate(&store, IMPORTED);
    let evidence = read(&store, IMPORTED);
    let oracle: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/copilot/official-reader.json")).unwrap();
    let records = oracle.as_array().unwrap();
    // The reader's last record is an in-memory resume the journal never persisted.
    assert_eq!(records.last().unwrap()["type"], "session.resume");
    let persisted = &records[..records.len() - 1];
    let messages: Vec<_> = persisted
        .iter()
        .filter(|r| r["type"] == "user.message" || r["type"] == "assistant.message")
        .collect();
    assert_eq!(
        evidence
            .messages
            .iter()
            .filter(|m| matches!(m.role, Role::User | Role::Assistant))
            .count(),
        messages.len()
    );
    for record in messages {
        let message = evidence
            .messages
            .iter()
            .find(|m| m.message_id.as_deref() == record["id"].as_str())
            .unwrap();
        let role = if record["type"] == "user.message" {
            Role::User
        } else {
            Role::Assistant
        };
        assert_eq!(message.role, role);
        assert_eq!(message.parent_id.as_deref(), record["parentId"].as_str());
        assert!(has(&evidence, record["data"]["content"].as_str().unwrap()));
    }
    let result = persisted
        .iter()
        .find(|r| r["type"] == "tool.execution_complete")
        .unwrap();
    assert_eq!(
        evidence.tool_results[0].tool_use_id.as_deref(),
        result["data"]["toolCallId"].as_str()
    );
}

#[test]
fn copilot_usage_stays_verbatim_per_message_and_session_totals_stay_markers() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), USAGE, USAGE_JOURNAL);
    let store = open(home.path());
    discover(&store);
    hydrate(&store, USAGE);
    let evidence = read(&store, USAGE);
    let raw: Vec<_> = evidence
        .messages
        .iter()
        .filter_map(|m| m.raw_usage())
        .collect();
    assert_eq!(
        raw,
        [
            r#"{"outputTokens":42}"#,
            r#"{"outputTokens":17}"#,
            r#"{"outputTokens":23}"#,
            r#"{"outputTokens":11}"#,
            r#"{"outputTokens":31}"#,
            r#"{"outputTokens":5}"#,
        ]
    );
    let usage: Vec<_> = evidence
        .messages
        .iter()
        .filter_map(|m| m.usage.as_ref())
        .collect();
    assert_eq!(usage.iter().map(|u| u.output_tokens).sum::<u64>(), 129);
    assert!(usage
        .iter()
        .all(|u| !u.coverage.has_input_tokens && u.coverage.has_output_tokens));
    let shutdowns: Vec<serde_json::Value> = evidence
        .markers
        .iter()
        .filter(|m| m.kind == "session.shutdown")
        .map(|m| serde_json::from_str(m.raw_payload().unwrap()).unwrap())
        .collect();
    assert_eq!(shutdowns.len(), 2);
    let last = &shutdowns[1]["data"]["modelMetrics"]["gpt-5.4"]["usage"];
    assert_eq!(last["inputTokens"], 25000);
    assert_eq!(last["cacheReadTokens"], 14000);
    assert!(evidence
        .markers
        .iter()
        .any(|m| m.kind == "session.usage_checkpoint"));
    let models: std::collections::BTreeSet<_> = evidence
        .messages
        .iter()
        .filter_map(|m| m.model.as_deref())
        .collect();
    assert_eq!(
        models.into_iter().collect::<Vec<_>>(),
        ["claude-sonnet-4.6", "gpt-5.4", "gpt-5.4-mini"]
    );
    let failed = evidence
        .tool_results
        .iter()
        .find(|r| r.tool_use_id.as_deref() == Some("bash_1"))
        .unwrap();
    assert_eq!(failed.result_status.as_deref(), Some("errored"));
    // The subagent numbers its calls independently: its `bash_1` is scoped
    // by the delegating call, not merged into the main agent's.
    let delegated = evidence
        .tool_results
        .iter()
        .find(|r| r.tool_use_id.as_deref() == Some("call_fixture_task/bash_1"))
        .unwrap();
    assert_eq!(delegated.result_status.as_deref(), Some("completed"));
    assert_eq!(evidence.tool_calls.len(), 4);
    assert_eq!(evidence.prompts.len(), 3);
    assert_eq!(evidence.session.git_branch.as_deref(), Some("main"));
}

#[test]
fn copilot_append_partial_tail_rewrite_and_rotation() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), IMPORTED, IMPORTED_JOURNAL);
    let store = open(home.path());
    discover(&store);
    hydrate(&store, IMPORTED);
    let row = "{\"type\":\"user.message\",\"data\":{\"content\":\"A resumed prompt\"},\"id\":\"appended-user\",\"timestamp\":\"2026-10-11T19:20:00.000Z\",\"parentId\":\"4bfa2dcb-14d6-4dda-aa35-1d77fa486947\"}\n";
    std::fs::write(&path, format!("{IMPORTED_JOURNAL}{}", &row[..40])).unwrap();
    hydrate(&store, IMPORTED);
    assert!(!has(&read(&store, IMPORTED), "A resumed prompt"));
    std::fs::write(&path, format!("{IMPORTED_JOURNAL}{row}")).unwrap();
    hydrate(&store, IMPORTED);
    assert!(has(&read(&store, IMPORTED), "A resumed prompt"));
    assert_eq!(read(&store, IMPORTED).prompts.len(), 2);
    std::fs::write(&path, IMPORTED_JOURNAL).unwrap();
    hydrate(&store, IMPORTED);
    assert!(!has(&read(&store, IMPORTED), "A resumed prompt"));
    assert_eq!(read(&store, IMPORTED).prompts.len(), 1);
    let moved = home
        .path()
        .join(".copilot/session-state/moved-directory/events.jsonl");
    std::fs::create_dir_all(moved.parent().unwrap()).unwrap();
    std::fs::rename(&path, &moved).unwrap();
    discover(&store);
    hydrate(&store, IMPORTED);
    assert!(has(
        &read(&store, IMPORTED),
        "The greeting says Hello synthetic fixture."
    ));
    assert_eq!(
        read(&store, IMPORTED).session.raw_path.as_deref(),
        Some(moved.as_path())
    );
}

#[test]
fn copilot_missing_empty_unsupported_malformed_and_collision_are_truthful() {
    let home = tempfile::tempdir().unwrap();
    let store = open(home.path());
    assert_eq!(
        discover(&store).discovered,
        0,
        "missing root has no sessions"
    );
    std::fs::create_dir_all(home.path().join(".copilot/session-state/empty-dir")).unwrap();
    assert_eq!(discover(&store).discovered, 0, "empty root has no sessions");
    let path = stage(home.path(), IMPORTED, IMPORTED_JOURNAL);
    std::fs::write(
        &path,
        IMPORTED_JOURNAL.replacen("\"version\":1", "\"version\":2", 1),
    )
    .unwrap();
    let result = discover(&store);
    assert_eq!(result.discovered, 0);
    assert!(result
        .diagnostics
        .iter()
        .any(|d| d.message.contains("COPILOT_UNSUPPORTED_SCHEMA")));
    std::fs::write(&path, IMPORTED_JOURNAL).unwrap();
    discover(&store);
    hydrate(&store, IMPORTED);
    let duplicate = stage(
        home.path(),
        "duplicate-directory",
        &IMPORTED_JOURNAL.replace("Hello synthetic fixture", "WRONG PROFILE"),
    );
    let result = discover(&store);
    assert!(
        result
            .diagnostics
            .iter()
            .any(|d| d.message.contains("COPILOT_ID_COLLISION")),
        "duplicate native ID must be diagnosed per journal"
    );
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert!(has(
        &read(&store, IMPORTED),
        "The greeting says Hello synthetic fixture."
    ));
    assert!(!has(
        &read(&store, IMPORTED),
        "The greeting says WRONG PROFILE."
    ));
    std::fs::remove_file(duplicate).unwrap();
    std::fs::write(&path, format!("{IMPORTED_JOURNAL}{{broken}}\n")).unwrap();
    assert!(store
        .hydrate(
            &SessionRef::id(Source::Copilot, IMPORTED),
            HydrateOptions::default()
        )
        .is_err());
    assert!(has(
        &read(&store, IMPORTED),
        "The greeting says Hello synthetic fixture."
    ));
}

#[test]
#[cfg(unix)]
fn copilot_symlink_escape_is_never_read() {
    let home = tempfile::tempdir().unwrap();
    let external = tempfile::tempdir().unwrap();
    let path = stage(home.path(), IMPORTED, IMPORTED_JOURNAL);
    let secret = external.path().join("events.jsonl");
    std::fs::write(&secret, IMPORTED_JOURNAL).unwrap();
    std::fs::remove_file(&path).unwrap();
    std::os::unix::fs::symlink(&secret, &path).unwrap();
    let store = open(home.path());
    let result = discover(&store);
    assert_eq!(result.discovered, 0);
    assert!(result
        .diagnostics
        .iter()
        .any(|d| d.message.contains("SESSION_SOURCE_MISMATCH")));
}
