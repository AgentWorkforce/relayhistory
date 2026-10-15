//! Public acquisition acceptance for pinned, synthetic Gemini recorder fixtures.
mod provider_support;
use ai_hist::{
    DiscoveryOptions, HydrateOptions, ProviderRoots, Role, SessionEvidence, SessionQuery,
    SessionRef, SessionStore, Source, StoreOptions, SyncOptions,
};
use std::path::{Path, PathBuf};
const SID: &str = "11111111-1111-4111-8111-111111111111";
const FIXTURE: &str = include_str!("fixtures/gemini/ordinary/.gemini/tmp/gemini-project/chats/session-2026-10-11T12-00-11111111.jsonl");
fn stage(home: &Path) -> PathBuf {
    let path = home.join(".gemini/tmp/gemini-project/chats/session.jsonl");
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
fn discover(store: &SessionStore) {
    store
        .discover(DiscoveryOptions::default().sources([Source::Gemini]))
        .unwrap();
}
fn hydrate(store: &SessionStore) {
    store
        .hydrate(
            &SessionRef::id(Source::Gemini, SID),
            HydrateOptions::default(),
        )
        .unwrap();
}
fn read(store: &SessionStore) -> SessionEvidence {
    store
        .session(
            &SessionRef::id(Source::Gemini, SID),
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
fn gemini_discover_hydrate_read_reopen_changes_export_fullsync() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path());
    let e = provider_support::assert_history_conformance(
        home.path(),
        provider_support::HistoryCase {
            source: Source::Gemini,
            session_id: SID,
            messages: &[
                (Role::User, "Find the synthetic Gemini needle"),
                (Role::Assistant, "Found the synthetic Gemini needle."),
            ],
        },
    );
    assert_eq!(e.tool_calls.len(), 1);
    assert_eq!(e.tool_results.len(), 1);
    assert_eq!(
        e.tool_results[0].tool_use_id.as_deref(),
        Some("call-real-id")
    );
    assert_eq!(
        e.prompts.len(),
        1,
        "function responses and status records cannot become prompts"
    );
    assert!(e.file_edits.is_empty());
    assert!(e.relationships.is_empty());
    let usage: Vec<_> = e.messages.iter().filter_map(|m| m.usage.as_ref()).collect();
    assert_eq!(usage.len(), 1);
    assert_eq!(usage[0].input_tokens, 90);
    assert_eq!(usage[0].cache_read_tokens, 30);
    assert_eq!(usage[0].reasoning_tokens, Some(4));
    assert!(e.markers.iter().any(|m| m.kind == "thought_summary"));
    assert!(e.markers.iter().any(|m| m.kind == "journal_record"));
}
#[test]
fn gemini_append_partial_tail_rewrite_rotation_and_metadata() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let store = open(home.path());
    discover(&store);
    hydrate(&store);
    let row="{\"id\":\"appended\",\"timestamp\":\"2026-10-11T12:01:00Z\",\"type\":\"user\",\"content\":\"A resumed Gemini prompt\"}\n";
    std::fs::write(&path, format!("{FIXTURE}{}", &row[..40])).unwrap();
    hydrate(&store);
    assert!(!has(&read(&store), "A resumed Gemini prompt"));
    std::fs::write(&path, format!("{FIXTURE}{row}")).unwrap();
    hydrate(&store);
    assert!(has(&read(&store), "A resumed Gemini prompt"));
    std::fs::write(
        &path,
        format!("{FIXTURE}{{\"$set\":{{\"summary\":\"Renamed Gemini fixture\"}}}}\n"),
    )
    .unwrap();
    hydrate(&store);
    assert!(!has(&read(&store), "A resumed Gemini prompt"));
    assert_eq!(
        read(&store).session.title.as_deref(),
        Some("Renamed Gemini fixture")
    );
    std::fs::rename(&path, path.with_file_name("rotated.jsonl")).unwrap();
    discover(&store);
    hydrate(&store);
    assert!(has(&read(&store), "Found the synthetic Gemini needle."));
    let before = read(&store);
    store.sync(SyncOptions::default().force(true)).unwrap();
    let after = read(&store);
    assert_eq!(before.messages.len(), after.messages.len());
    assert_eq!(before.markers.len(), after.markers.len());
}
#[test]
fn gemini_destructive_reorder_preserves_recorded_user_roles_without_guessing_synthetic() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let store = open(home.path());
    let append = "{\"id\":\"human\",\"timestamp\":\"2026-10-11T12:01:00Z\",\"type\":\"user\",\"content\":\"<state_snapshot>This is genuine user text</state_snapshot>\"}\n{\"$patch\":{\"removeIds\":[\"info-one\"],\"orderIds\":[\"human\",\"user-one\"]}}\n";
    std::fs::write(&path, format!("{FIXTURE}{append}")).unwrap();
    discover(&store);
    hydrate(&store);
    let e = read(&store);
    assert_eq!(e.prompts.len(), 2);
    assert!(has(
        &e,
        "<state_snapshot>This is genuine user text</state_snapshot>"
    ));
    assert!(has(&e, "Find the synthetic Gemini needle"));
    assert!(e
        .markers
        .iter()
        .any(|m| m.raw_payload().is_some_and(|p| p.contains("removeIds"))));
}
#[test]
fn gemini_missing_empty_malformed_unsupported_and_collision() {
    let home = tempfile::tempdir().unwrap();
    let store = open(home.path());
    assert_eq!(
        store
            .discover(DiscoveryOptions::default().sources([Source::Gemini]))
            .unwrap()
            .discovered,
        0
    );
    std::fs::create_dir_all(home.path().join(".gemini/tmp")).unwrap();
    discover(&store);
    let path = stage(home.path());
    discover(&store);
    hydrate(&store);
    std::fs::write(&path, format!("{FIXTURE}{{broken}}\n")).unwrap();
    assert!(store
        .hydrate(
            &SessionRef::id(Source::Gemini, SID),
            HydrateOptions::default()
        )
        .is_err());
    assert!(has(&read(&store), "Found the synthetic Gemini needle."));
    std::fs::write(&path, format!("{FIXTURE}{{\"$futureOpcode\":true}}\n")).unwrap();
    assert!(store
        .hydrate(
            &SessionRef::id(Source::Gemini, SID),
            HydrateOptions::default()
        )
        .is_err());
    std::fs::write(&path, FIXTURE).unwrap();
    std::fs::write(
        path.with_file_name("duplicate.jsonl"),
        FIXTURE.replace("needle", "WRONG"),
    )
    .unwrap();
    let result = store
        .discover(DiscoveryOptions::default().sources([Source::Gemini]))
        .unwrap();
    assert!(result
        .diagnostics
        .iter()
        .any(|d| d.message.contains("GEMINI_ID_COLLISION")));
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert!(!has(&read(&store), "Found the synthetic Gemini WRONG."));
}
#[test]
fn gemini_legacy_migration_sibling_deduplicates_native_identity() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    std::fs::remove_file(&path).unwrap();
    let legacy = include_str!(
        "fixtures/gemini/legacy/.gemini/tmp/gemini-project/chats/session-legacy-22222222.json"
    );
    let migrated=include_str!("fixtures/gemini/legacy-migrated/.gemini/tmp/gemini-project/chats/session-legacy-22222222.jsonl");
    let legacy_path = path.with_extension("json");
    std::fs::write(&legacy_path, legacy).unwrap();
    let store = open(home.path());
    discover(&store);
    let reference = SessionRef::id(Source::Gemini, "22222222-2222-4222-8222-222222222222");
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    std::fs::write(&path, migrated).unwrap();
    let result = store
        .discover(DiscoveryOptions::default().sources([Source::Gemini]))
        .unwrap();
    assert!(result.diagnostics.is_empty(), "{:?}", result.diagnostics);
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    let evidence = store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    assert_eq!(
        evidence.session.session_id,
        "22222222-2222-4222-8222-222222222222"
    );
    assert!(evidence
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .any(|b| b.text.as_deref() == Some("Continue the synthetic legacy session.")));
    let append="{\"id\":\"migrated-append\",\"timestamp\":\"2026-10-11T12:01:00Z\",\"type\":\"user\",\"content\":\"After migration append\"}\n";
    std::fs::write(&path, format!("{migrated}{append}")).unwrap();
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    assert!(has(
        &store
            .session(&reference, SessionQuery::default())
            .unwrap()
            .unwrap(),
        "After migration append"
    ));
    std::fs::remove_file(&legacy_path).unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert!(has(
        &store
            .session(&reference, SessionQuery::default())
            .unwrap()
            .unwrap(),
        "After migration append"
    ));
}
#[test]
#[cfg(unix)]
fn gemini_symlink_escape_is_not_read() {
    let home = tempfile::tempdir().unwrap();
    let external = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let secret = external.path().join("secret.jsonl");
    std::fs::write(&secret, FIXTURE).unwrap();
    std::fs::remove_file(&path).unwrap();
    std::os::unix::fs::symlink(&secret, &path).unwrap();
    let result = open(home.path())
        .discover(DiscoveryOptions::default().sources([Source::Gemini]))
        .unwrap();
    assert_eq!(result.discovered, 0);
    assert!(result
        .diagnostics
        .iter()
        .any(|d| d.message.contains("SESSION_SOURCE_MISMATCH")));
}
