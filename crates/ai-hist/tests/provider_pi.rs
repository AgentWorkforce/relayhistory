//! Public Pi v3 acquisition acceptance, using only synthetic fixture homes.
mod provider_support;
use ai_hist::{
    DiscoveryOptions, HydrateOptions, ProviderRoots, Role, SessionEvidence, SessionQuery,
    SessionRef, SessionStore, Source, StoreOptions, SyncOptions,
};
use std::path::{Path, PathBuf};
const SID: &str = "custom-history-session";
const FIXTURE: &str =
    include_str!("fixtures/pi/writer-v3/.pi/agent/sessions/--fixture--/session.jsonl");
fn stage(home: &Path) -> PathBuf {
    let path = home.join(".pi/agent/sessions/--fixture--/session.jsonl");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, FIXTURE).unwrap();
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
fn hydrate(store: &SessionStore) {
    store
        .hydrate(&SessionRef::id(Source::Pi, SID), HydrateOptions::default())
        .unwrap();
}
fn read(store: &SessionStore) -> SessionEvidence {
    store
        .session(&SessionRef::id(Source::Pi, SID), SessionQuery::default())
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
fn pi_discover_hydrate_read_reopen_changes_export_fullsync() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path());
    let evidence = provider_support::assert_history_conformance(
        home.path(),
        provider_support::HistoryCase {
            source: Source::Pi,
            session_id: SID,
            messages: &[
                (Role::User, "Inspect the fixture greeting."),
                (Role::Assistant, "The greeting says Hello fixture."),
                (Role::Assistant, "This answer is on the alternate branch."),
            ],
        },
    );
    assert_eq!(evidence.tool_calls.len(), 1);
    assert_eq!(evidence.tool_results.len(), 1);
    assert_eq!(
        evidence
            .messages
            .iter()
            .find(|m| m.message_id.as_deref() == Some("entry-6"))
            .unwrap()
            .parent_id
            .as_deref(),
        Some("entry-1")
    );
    assert!(evidence.markers.iter().any(|m| m.kind == "compaction"));
    assert!(evidence.markers.iter().any(|m| m.kind == "usage"));
    let usage: Vec<_> = evidence
        .messages
        .iter()
        .filter_map(|m| m.usage.as_ref())
        .collect();
    assert_eq!(usage.len(), 3);
    assert_eq!(usage.iter().map(|u| u.input_tokens).sum::<u64>(), 42);
    assert!(evidence.file_edits.is_empty());
    assert!(evidence.relationships.is_empty());
}
#[test]
fn pi_append_partial_tail_rewrite_and_metadata_changes() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Pi]))
        .unwrap();
    hydrate(&store);
    let row = "{\"type\":\"message\",\"id\":\"append\",\"parentId\":\"entry-9\",\"timestamp\":\"2023-11-14T22:14:00Z\",\"message\":{\"role\":\"user\",\"content\":\"A resumed prompt\",\"timestamp\":1700000040000}}\n";
    std::fs::write(&path, format!("{FIXTURE}{}", &row[..35])).unwrap();
    hydrate(&store);
    assert!(!has(&read(&store), "A resumed prompt"));
    assert_eq!(read(&store).messages.len(), 6);
    std::fs::write(&path, format!("{FIXTURE}{row}")).unwrap();
    hydrate(&store);
    assert!(has(&read(&store), "A resumed prompt"));
    std::fs::write(&path, FIXTURE).unwrap();
    hydrate(&store);
    assert!(!has(&read(&store), "A resumed prompt"));
    assert_eq!(read(&store).prompts.len(), 2);
    let renamed = FIXTURE.replace("Synthetic Pi history", "Renamed fixture");
    std::fs::write(&path, renamed).unwrap();
    hydrate(&store);
    assert_eq!(
        read(&store).session.title.as_deref(),
        Some("Renamed fixture")
    );
    assert!(read(&store).markers.iter().any(|m| m
        .raw_payload()
        .is_some_and(|p| p.contains("Renamed fixture"))));
    let rotated = path.with_file_name("rotated.jsonl");
    std::fs::rename(&path, &rotated).unwrap();
    store
        .discover(DiscoveryOptions::default().sources([Source::Pi]))
        .unwrap();
    hydrate(&store);
    assert_eq!(read(&store).messages.len(), 6);
    assert!(has(&read(&store), "The greeting says Hello fixture."));
}
#[test]
fn pi_missing_empty_unsupported_malformed_and_collision_are_truthful() {
    let home = tempfile::tempdir().unwrap();
    let store = open(home.path());
    assert_eq!(
        store
            .discover(DiscoveryOptions::default().sources([Source::Pi]))
            .unwrap()
            .discovered,
        0
    );
    std::fs::create_dir_all(home.path().join(".pi/agent/sessions")).unwrap();
    assert_eq!(
        store
            .discover(DiscoveryOptions::default().sources([Source::Pi]))
            .unwrap()
            .discovered,
        0,
        "empty root has no sessions"
    );
    let path = stage(home.path());
    std::fs::write(
        &path,
        FIXTURE.replacen("\"version\":3", "\"version\":99", 1),
    )
    .unwrap();
    let result = store
        .discover(DiscoveryOptions::default().sources([Source::Pi]))
        .unwrap();
    assert_eq!(result.discovered, 0);
    assert!(result
        .diagnostics
        .iter()
        .any(|d| d.message.contains("PI_UNSUPPORTED_SCHEMA")));
    std::fs::write(&path, FIXTURE).unwrap();
    store
        .discover(DiscoveryOptions::default().sources([Source::Pi]))
        .unwrap();
    hydrate(&store);
    let duplicate = path.with_file_name("duplicate.jsonl");
    std::fs::write(
        &duplicate,
        FIXTURE.replace("Hello fixture", "WRONG PROFILE"),
    )
    .unwrap();
    let result = store
        .discover(DiscoveryOptions::default().sources([Source::Pi]))
        .unwrap();
    assert!(
        result
            .diagnostics
            .iter()
            .any(|d| d.message.contains("PI_ID_COLLISION")),
        "duplicate native ID must be diagnosed per file"
    );
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert!(has(&read(&store), "The greeting says Hello fixture."));
    assert!(!has(&read(&store), "The greeting says WRONG PROFILE."));
    std::fs::write(&path, format!("{FIXTURE}{{broken}}\n")).unwrap();
    assert!(store
        .hydrate(&SessionRef::id(Source::Pi, SID), HydrateOptions::default())
        .is_err());
    assert!(has(&read(&store), "The greeting says Hello fixture."));
}
#[test]
#[cfg(unix)]
fn pi_symlink_escape_is_never_read() {
    let home = tempfile::tempdir().unwrap();
    let external = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let secret = external.path().join("private.jsonl");
    std::fs::write(&secret, FIXTURE).unwrap();
    std::fs::remove_file(&path).unwrap();
    std::os::unix::fs::symlink(&secret, &path).unwrap();
    let store = open(home.path());
    let result = store
        .discover(DiscoveryOptions::default().sources([Source::Pi]))
        .unwrap();
    assert_eq!(result.discovered, 0);
    assert!(result
        .diagnostics
        .iter()
        .any(|d| d.message.contains("SESSION_SOURCE_MISMATCH")));
}

#[test]
fn pi_messages_match_independent_upstream_reader_oracle() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path());
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Pi]))
        .unwrap();
    hydrate(&store);
    let evidence = read(&store);
    let oracle: serde_json::Value =
        serde_json::from_str(include_str!("../src/providers/pi/oracle.json")).unwrap();
    let expected = oracle["messages"].as_array().unwrap();
    assert_eq!(evidence.messages.len(), expected.len());
    assert_eq!(
        oracle["activeContextMessages"], 1,
        "upstream active context has intentionally fewer messages than raw history"
    );
    for original in expected {
        let message = evidence
            .messages
            .iter()
            .find(|m| m.message_id.as_deref() == original["id"].as_str())
            .unwrap();
        let role = match original["role"].as_str().unwrap() {
            "user" => Role::User,
            "assistant" => Role::Assistant,
            "toolResult" => Role::ToolResult,
            _ => panic!("unexpected oracle role"),
        };
        assert_eq!(message.role, role);
        if let Some(text) = original["content"].as_str() {
            assert!(has(&evidence, text));
        }
        if let Some(blocks) = original["content"].as_array() {
            for block in blocks {
                if let Some(text) = block["text"].as_str() {
                    assert!(has(&evidence, text));
                }
            }
        }
    }
}

#[test]
fn pi_fresh_native_identity_collision_is_a_per_file_diagnostic() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    std::fs::write(
        path.with_file_name("duplicate.jsonl"),
        FIXTURE.replace("Hello fixture", "OTHER FILE"),
    )
    .unwrap();
    let store = open(home.path());
    let result = store
        .discover(DiscoveryOptions::default().sources([Source::Pi]))
        .unwrap();
    assert_eq!(result.discovered, 1);
    assert_eq!(result.diagnostics.len(), 1);
    assert!(result.diagnostics[0].message.contains("PI_ID_COLLISION"));
    let original_path = read(&store).session.raw_path;
    let unchanged = store
        .discover(DiscoveryOptions::default().sources([Source::Pi]))
        .unwrap();
    assert_eq!(unchanged.diagnostics.len(), 1);
    assert_eq!(
        read(&store).session.raw_path,
        original_path,
        "collision must not overwrite chosen catalog locator"
    );
}
