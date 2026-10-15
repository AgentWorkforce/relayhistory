//! Public Omp v3 acquisition acceptance, using only synthetic fixture homes.
mod provider_support;
use ai_hist::{
    DiscoveryOptions, HydrateOptions, ProviderRoots, Role, SessionEvidence, SessionQuery,
    SessionRef, SessionStore, Source, StoreOptions, SyncOptions,
};
use std::path::{Path, PathBuf};
const SID: &str = "019b76da-a800-7000-8000-000000000001";
const FIXTURE: &str =
    include_str!("fixtures/omp/title-v3/.omp/agent/sessions/--fixture--/session.jsonl");
fn stage(home: &Path) -> PathBuf {
    let path = home.join(".omp/agent/sessions/--fixture--/session.jsonl");
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
        .hydrate(&SessionRef::id(Source::Omp, SID), HydrateOptions::default())
        .unwrap();
}
fn read(store: &SessionStore) -> SessionEvidence {
    store
        .session(&SessionRef::id(Source::Omp, SID), SessionQuery::default())
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
fn omp_discover_hydrate_read_reopen_changes_export_fullsync() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path());
    let evidence = provider_support::assert_history_conformance(
        home.path(),
        provider_support::HistoryCase {
            source: Source::Omp,
            session_id: SID,
            messages: &[
                (Role::User, "Inspect this synthetic image"),
                (Role::Assistant, "Reading a fixture"),
            ],
        },
    );
    assert_eq!(evidence.tool_calls.len(), 1);
    assert_eq!(evidence.tool_results.len(), 1);
    assert_eq!(
        evidence
            .messages
            .iter()
            .find(|m| m.message_id.as_deref() == Some("00000003"))
            .unwrap()
            .parent_id
            .as_deref(),
        Some("00000002")
    );
    let usage: Vec<_> = evidence
        .messages
        .iter()
        .filter_map(|m| m.usage.as_ref())
        .collect();
    assert_eq!(usage.len(), 1);
    assert_eq!(usage[0].input_tokens, 12);
    assert!(!usage[0].coverage.has_output_tokens);
    assert!(evidence.file_edits.is_empty());
    assert!(evidence.relationships.is_empty());
}
#[test]
fn omp_append_partial_tail_rewrite_and_metadata_changes() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Omp]))
        .unwrap();
    hydrate(&store);
    let row = "{\"type\":\"message\",\"id\":\"append\",\"parentId\":\"entry-9\",\"timestamp\":\"2023-11-14T22:14:00Z\",\"message\":{\"role\":\"user\",\"content\":\"A resumed prompt\",\"timestamp\":1700000040000}}\n";
    std::fs::write(&path, format!("{FIXTURE}{}", &row[..35])).unwrap();
    hydrate(&store);
    assert!(!has(&read(&store), "A resumed prompt"));
    assert_eq!(read(&store).messages.len(), 3);
    std::fs::write(&path, format!("{FIXTURE}{row}")).unwrap();
    hydrate(&store);
    assert!(has(&read(&store), "A resumed prompt"));
    std::fs::write(&path, FIXTURE).unwrap();
    hydrate(&store);
    assert!(!has(&read(&store), "A resumed prompt"));
    assert_eq!(read(&store).prompts.len(), 1);
    let renamed = FIXTURE.replace("Synthetic initial title", "Synthetic changed title");
    std::fs::write(&path, renamed).unwrap();
    hydrate(&store);
    assert_eq!(
        read(&store).session.title.as_deref(),
        Some("Synthetic changed title")
    );
    assert!(read(&store).markers.iter().any(|m| m
        .raw_payload()
        .is_some_and(|p| p.contains("Synthetic changed title"))));
    let rotated = path.with_file_name("rotated.jsonl");
    std::fs::rename(&path, &rotated).unwrap();
    store
        .discover(DiscoveryOptions::default().sources([Source::Omp]))
        .unwrap();
    hydrate(&store);
    assert_eq!(read(&store).messages.len(), 3);
    assert!(has(&read(&store), "Reading a fixture"));
}
#[test]
fn omp_missing_empty_unsupported_malformed_and_collision_are_truthful() {
    let home = tempfile::tempdir().unwrap();
    let store = open(home.path());
    assert_eq!(
        store
            .discover(DiscoveryOptions::default().sources([Source::Omp]))
            .unwrap()
            .discovered,
        0
    );
    std::fs::create_dir_all(home.path().join(".omp/agent/sessions")).unwrap();
    assert_eq!(
        store
            .discover(DiscoveryOptions::default().sources([Source::Omp]))
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
        .discover(DiscoveryOptions::default().sources([Source::Omp]))
        .unwrap();
    assert_eq!(result.discovered, 0);
    assert!(result
        .diagnostics
        .iter()
        .any(|d| d.message.contains("OMP_UNSUPPORTED_SCHEMA")));
    std::fs::write(&path, FIXTURE).unwrap();
    store
        .discover(DiscoveryOptions::default().sources([Source::Omp]))
        .unwrap();
    hydrate(&store);
    let duplicate = path.with_file_name("duplicate.jsonl");
    std::fs::write(
        &duplicate,
        FIXTURE.replace("Reading a fixture", "WRONG PROFILE"),
    )
    .unwrap();
    let result = store
        .discover(DiscoveryOptions::default().sources([Source::Omp]))
        .unwrap();
    assert!(
        result
            .diagnostics
            .iter()
            .any(|d| d.message.contains("OMP_ID_COLLISION")),
        "duplicate native ID must be diagnosed per file"
    );
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert!(has(&read(&store), "Reading a fixture"));
    assert!(!has(&read(&store), "WRONG PROFILE"));
    std::fs::write(&path, format!("{FIXTURE}{{broken}}\n")).unwrap();
    assert!(store
        .hydrate(&SessionRef::id(Source::Omp, SID), HydrateOptions::default())
        .is_err());
    assert!(has(&read(&store), "Reading a fixture"));
}
#[test]
#[cfg(unix)]
fn omp_symlink_escape_is_never_read() {
    let home = tempfile::tempdir().unwrap();
    let external = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let secret = external.path().join("private.jsonl");
    std::fs::write(&secret, FIXTURE).unwrap();
    std::fs::remove_file(&path).unwrap();
    std::os::unix::fs::symlink(&secret, &path).unwrap();
    let store = open(home.path());
    let result = store
        .discover(DiscoveryOptions::default().sources([Source::Omp]))
        .unwrap();
    assert_eq!(result.discovered, 0);
    assert!(result
        .diagnostics
        .iter()
        .any(|d| d.message.contains("SESSION_SOURCE_MISMATCH")));
}

#[test]
fn omp_and_pi_shared_override_admit_each_file_once() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let mut roots = serde_json::to_value(ProviderRoots::from_home(
        home.path().into(),
        home.path().join("opencode.db"),
    ))
    .unwrap();
    roots["provider_roots"]["pi"] = roots["provider_roots"]["omp"].clone();
    // Direct-header legacy OMP cannot be distinguished from Pi. OMP must refuse it.
    let direct = FIXTURE
        .split_once('\n')
        .unwrap()
        .1
        .replace(SID, "pi-direct");
    std::fs::write(path.with_file_name("direct.jsonl"), direct).unwrap();
    let store = SessionStore::open(
        StoreOptions::default()
            .db_path(home.path().join("shared.db"))
            .roots(serde_json::from_value(roots).unwrap()),
    )
    .unwrap();
    let report = store
        .discover(DiscoveryOptions::default().sources([Source::Pi, Source::Omp]))
        .unwrap();
    assert_eq!(report.discovered, 2);
    assert!(store
        .session(&SessionRef::id(Source::Omp, SID), SessionQuery::default())
        .unwrap()
        .is_some());
    assert!(store
        .session(&SessionRef::id(Source::Pi, SID), SessionQuery::default())
        .unwrap()
        .is_none());
    assert!(store
        .session(
            &SessionRef::id(Source::Omp, "pi-direct"),
            SessionQuery::default()
        )
        .unwrap()
        .is_none());
    assert!(store
        .session(
            &SessionRef::id(Source::Pi, "pi-direct"),
            SessionQuery::default()
        )
        .unwrap()
        .is_some());
    assert!(report
        .diagnostics
        .iter()
        .any(|d| d.message.contains("OMP_AMBIGUOUS_LEGACY")));
}

#[test]
fn omp_real_writer_reopen_and_independent_full_reader_oracle() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    std::fs::write(
        path,
        include_str!("fixtures/omp/writer-v3/.omp/agent/sessions/--fixture--/session.jsonl"),
    )
    .unwrap();
    let evidence = provider_support::assert_history_conformance(
        home.path(),
        provider_support::HistoryCase {
            source: Source::Omp,
            session_id: "019b76da-a800-7000-8000-000000000002",
            messages: &[
                (Role::User, "Inspect the writer fixture."),
                (Role::Assistant, "Reading the writer fixture."),
                (
                    Role::Assistant,
                    "The fixture remains readable after reopen.",
                ),
            ],
        },
    );
    let oracle: serde_json::Value = serde_json::from_str(include_str!(
        "../../../plans/history-providers/fixtures/omp/writer-oracle.json"
    ))
    .unwrap();
    assert_eq!(
        evidence.messages.len(),
        oracle["messages"].as_array().unwrap().len()
    );
    assert_eq!(
        evidence.messages.len(),
        oracle["renderedMessages"].as_array().unwrap().len()
    );
    for original in oracle["messages"].as_array().unwrap() {
        let message = evidence
            .messages
            .iter()
            .find(|m| m.message_id.as_deref() == original["id"].as_str())
            .unwrap();
        assert_eq!(message.parent_id.as_deref(), original["parentId"].as_str());
        for block in original["content"].as_array().into_iter().flatten() {
            if let Some(text) = block["text"].as_str() {
                assert!(has(&evidence, text));
            }
        }
    }
    assert_eq!(evidence.tool_calls.len(), 1);
    assert_eq!(evidence.tool_results.len(), 1);
    assert_eq!(
        evidence.session.title.as_deref(),
        Some("Synthetic writer title")
    );
}

#[test]
fn omp_invalid_frozen_root_configuration_is_not_successfully_empty() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path());
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Omp]))
        .unwrap();
    let mut frozen = serde_json::to_value(store.roots()).unwrap();
    frozen["provider_root_errors"] =
        serde_json::json!({"omp":"OMP_INVALID_PROFILE: rejected ../escape"});
    frozen["provider_roots"]["omp"] = serde_json::json!([]);
    let db = store.db_path().to_owned();
    drop(store);
    let roots: ProviderRoots = serde_json::from_value(frozen).unwrap();
    let roots: ProviderRoots =
        serde_json::from_str(&serde_json::to_string(&roots).unwrap()).unwrap();
    let store = SessionStore::open(StoreOptions::default().db_path(db).roots(roots)).unwrap();
    assert!(store
        .discover(DiscoveryOptions::default().sources([Source::Omp]))
        .is_err());
    let pi = home.path().join(".pi/agent/sessions/direct.jsonl");
    std::fs::create_dir_all(pi.parent().unwrap()).unwrap();
    std::fs::write(
        pi,
        FIXTURE
            .split_once('\n')
            .unwrap()
            .1
            .replace(SID, "pi-independent"),
    )
    .unwrap();
    let result = store
        .discover(DiscoveryOptions::default().sources([Source::Omp, Source::Pi]))
        .unwrap();
    assert!(result
        .diagnostics
        .iter()
        .any(|d| d.message.contains("OMP_INVALID_PROFILE")));
    assert!(store
        .hydrate(&SessionRef::id(Source::Omp, SID), HydrateOptions::default())
        .is_err());
    // Sibling providers still sync; the blocked OMP sweep cannot retire cached evidence.
    let report = store.sync(SyncOptions::default().force(true)).unwrap();
    assert!(!report.changed.iter().any(|reference| match reference {
        SessionRef::Id { source, .. } => *source == Source::Omp,
        _ => false,
    }));
    assert_eq!(read(&store).session.session_id, SID);
}

#[test]
fn omp_shared_root_pi_file_does_not_block_missing_omp_retirement() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let mut roots = serde_json::to_value(ProviderRoots::from_home(
        home.path().into(),
        home.path().join("opencode.db"),
    ))
    .unwrap();
    roots["provider_roots"]["pi"] = roots["provider_roots"]["omp"].clone();
    std::fs::write(
        path.with_file_name("pi.jsonl"),
        FIXTURE.split_once('\n').unwrap().1.replace(SID, "pi-kept"),
    )
    .unwrap();
    let store = SessionStore::open(
        StoreOptions::default()
            .db_path(home.path().join("coinstall.db"))
            .roots(serde_json::from_value(roots).unwrap()),
    )
    .unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert!(has(&read(&store), "Reading a fixture"));
    std::fs::remove_file(path).unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert!(store
        .session(&SessionRef::id(Source::Omp, SID), SessionQuery::default())
        .unwrap()
        .is_none());
    let pi = store
        .session(
            &SessionRef::id(Source::Pi, "pi-kept"),
            SessionQuery::default(),
        )
        .unwrap()
        .unwrap();
    assert!(has(&pi, "Reading a fixture"));
}

#[test]
fn omp_same_locator_identity_replacement_retires_old_only_after_complete_capture() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Omp]))
        .unwrap();
    hydrate(&store);
    let replacement = "replacement-native";
    let rewritten = FIXTURE.replace(SID, replacement);
    std::fs::write(&path, format!("{rewritten}{{partial")).unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert!(
        store
            .session(&SessionRef::id(Source::Omp, SID), SessionQuery::default())
            .unwrap()
            .is_some(),
        "partial replacement must retain prior identity evidence"
    );
    std::fs::write(&path, rewritten).unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert!(
        store
            .session(&SessionRef::id(Source::Omp, SID), SessionQuery::default())
            .unwrap()
            .is_none(),
        "one complete locator cannot continue backing its previous native identity"
    );
    let replaced = store
        .session(
            &SessionRef::id(Source::Omp, replacement),
            SessionQuery::default(),
        )
        .unwrap()
        .unwrap();
    assert!(has(&replaced, "Reading a fixture"));
    assert_eq!(replaced.prompts.len(), 1);
}
