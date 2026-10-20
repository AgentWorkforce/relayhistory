//! Public acceptance for official SDK-native Antigravity transcript fixtures.
mod provider_support;
use ai_hist::{
    DiscoveryOptions, HydrateOptions, ProviderRoots, Role, SessionEvidence, SessionQuery,
    SessionRef, SessionStore, Source, StoreOptions, SyncOptions,
};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
const NATIVE: &str = "00000000000000000000000000000001";
fn sid(native: &str, variant: &str) -> String {
    format!(
        "root:{:x}:{native}",
        Sha256::digest(format!(".gemini/{variant}/brain").as_bytes())
    )
}
fn stage(home: &Path, case: &str) -> PathBuf {
    fn copy(from: &Path, to: &Path) {
        std::fs::create_dir_all(to).unwrap();
        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            let dest = to.join(entry.file_name());
            if entry.path().is_dir() {
                copy(&entry.path(), &dest)
            } else {
                std::fs::copy(entry.path(), dest).unwrap();
            }
        }
    }
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/antigravity")
        .join(case);
    copy(&fixture, home);
    let brain = home.join(".gemini/antigravity/brain");
    std::fs::read_dir(brain)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path()
        .join(".system_generated/logs/transcript_full.jsonl")
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
fn reference() -> SessionRef {
    SessionRef::id(Source::Antigravity, sid(NATIVE, "antigravity"))
}
fn hydrate(store: &SessionStore) {
    store
        .hydrate(&reference(), HydrateOptions::default())
        .unwrap();
}
fn read(store: &SessionStore) -> SessionEvidence {
    store
        .session(&reference(), SessionQuery::default())
        .unwrap()
        .unwrap()
}
#[test]
fn antigravity_discover_hydrate_reopen_export_and_sync() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), "ordinary");
    let evidence = provider_support::assert_history_conformance(
        home.path(),
        provider_support::HistoryCase {
            source: Source::Antigravity,
            session_id: &sid(NATIVE, "antigravity"),
            messages: &[
                (Role::User, "Remember the synthetic fixture color blue."),
                (Role::Assistant, "The synthetic fixture answer is blue."),
            ],
        },
    );
    assert!(evidence.tool_calls.is_empty());
    assert_eq!(evidence.markers.len(), 2);
    assert_eq!(Source::Antigravity.capabilities().usage_accounting, None);
}
#[test]
fn antigravity_full_projection_wins_without_duplicate_sessions() {
    let home = tempfile::tempdir().unwrap();
    let full = stage(home.path(), "ordinary");
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Antigravity]))
        .unwrap();
    hydrate(&store);
    assert_eq!(read(&store).session.raw_path, Some(full));
    assert_eq!(read(&store).messages.len(), 2);
    assert!(Source::parse("gemini").is_none());
    assert!(Source::parse("gemini-cli").is_none());
}
#[test]
fn antigravity_append_partial_rewrite_and_repeat_preserve_correct_evidence() {
    let home = tempfile::tempdir().unwrap();
    let full = stage(home.path(), "ordinary");
    let original = std::fs::read_to_string(&full).unwrap();
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let initial = read(&store);
    std::fs::write(&full, format!("{original}{{\"step_index\":")).unwrap();
    hydrate(&store);
    assert_eq!(read(&store).messages, initial.messages);
    std::fs::write(
        &full,
        original.replace(
            "The synthetic fixture answer is blue.",
            "Rewritten synthetic answer.",
        ),
    )
    .unwrap();
    hydrate(&store);
    let rewritten = read(&store);
    assert!(rewritten
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .any(|b| b.text.as_deref() == Some("Rewritten synthetic answer.")));
    assert!(!rewritten
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .any(|b| b.text.as_deref() == Some("The synthetic fixture answer is blue.")));
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert_eq!(read(&store).messages, rewritten.messages);
    std::fs::write(&full, "{broken}\n").unwrap();
    assert!(store
        .hydrate(&reference(), HydrateOptions::default())
        .is_err());
    assert_eq!(read(&store).messages, rewritten.messages);
}
#[test]
fn antigravity_compact_to_full_then_compact_fallback_keeps_identity() {
    let home = tempfile::tempdir().unwrap();
    let full = stage(home.path(), "ordinary");
    let original = std::fs::read(&full).unwrap();
    let compact = full.with_file_name("transcript.jsonl");
    std::fs::remove_file(&full).unwrap();
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert_eq!(read(&store).session.raw_path, Some(compact.clone()));
    std::fs::write(&full, &original).unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    let before = read(&store);
    assert_eq!(before.session.raw_path, Some(full.clone()));
    std::fs::remove_file(&full).unwrap();
    // The common hydration gate validates the catalog locator before capture.
    // A sweep repairs a disappeared full projection to its surviving compact.
    assert!(store
        .hydrate(&reference(), HydrateOptions::default())
        .is_err());
    for _ in 0..2 {
        store.sync(SyncOptions::default().force(true)).unwrap();
        let after = read(&store);
        assert_eq!(after.session.raw_path, Some(compact.clone()));
        assert_eq!(after.messages, before.messages);
    }
}
#[test]
fn antigravity_variant_roots_keep_reused_native_ids_separate() {
    let home = tempfile::tempdir().unwrap();
    let full = stage(home.path(), "ordinary");
    let other = home
        .path()
        .join(".gemini/antigravity-cli/brain")
        .join(NATIVE)
        .join(".system_generated/logs/transcript_full.jsonl");
    std::fs::create_dir_all(other.parent().unwrap()).unwrap();
    std::fs::copy(&full, &other).unwrap();
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    for variant in ["antigravity", "antigravity-cli"] {
        assert!(store
            .session(
                &SessionRef::id(Source::Antigravity, sid(NATIVE, variant)),
                SessionQuery::default()
            )
            .unwrap()
            .is_some());
    }
}
#[test]
#[cfg(unix)]
fn antigravity_symlink_outside_roots_is_rejected() {
    use std::os::unix::fs::symlink;
    let home = tempfile::tempdir().unwrap();
    let full = stage(home.path(), "ordinary");
    let outside = tempfile::tempdir().unwrap();
    let file = outside.path().join("private.jsonl");
    std::fs::rename(&full, &file).unwrap();
    symlink(&file, &full).unwrap();
    let store = open(home.path());
    assert!(store
        .discover(DiscoveryOptions::default().sources([Source::Antigravity]))
        .is_err());
    assert!(store
        .session(&reference(), SessionQuery::default())
        .unwrap()
        .is_none());
}

#[test]
fn antigravity_truncated_compact_is_observable_without_destroying_full_text() {
    let home = tempfile::tempdir().unwrap();
    let full = stage(home.path(), "long");
    let store = open(home.path());
    let reference = SessionRef::id(
        Source::Antigravity,
        sid("00000000000000000000000000000005", "antigravity"),
    );
    store.sync(SyncOptions::default().force(true)).unwrap();
    let before = store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    assert!(before
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .any(|b| b.text.as_ref().is_some_and(|t| t.len() > 20_000)));
    std::fs::remove_file(full).unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    let after = store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    assert_eq!(after.messages, before.messages);
    let report = store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    assert!(report
        .diagnostics
        .iter()
        .any(|d| d.code == "ANTIGRAVITY_INCOMPLETE_TRANSCRIPT"));
    let fresh = tempfile::tempdir().unwrap();
    let path = stage(fresh.path(), "long");
    std::fs::remove_file(path).unwrap();
    let fresh = open(fresh.path());
    fresh.sync(SyncOptions::default().force(true)).unwrap();
    let compact = fresh
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    assert_eq!(compact.messages.len(), 1);
    assert!(compact.markers.iter().any(|m| m
        .raw_payload()
        .is_some_and(|p| p.contains("truncated_fields"))));
}

#[test]
fn antigravity_long_first_prompt_uses_bounded_recorded_tail_for_discovery() {
    let home = tempfile::tempdir().unwrap();
    let full = stage(home.path(), "ordinary");
    let mut rows = std::fs::read_to_string(&full)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap())
        .collect::<Vec<_>>();
    rows[0]["content"] = "Long native-shaped regression prompt ".repeat(4000).into();
    let bytes = rows
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(&full, bytes).unwrap();
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Antigravity]))
        .unwrap();
    hydrate(&store);
    assert_eq!(read(&store).messages.len(), 2);
}

#[test]
fn antigravity_deleted_conversation_retires_after_complete_accessible_sweep() {
    let home = tempfile::tempdir().unwrap();
    let full = stage(home.path(), "ordinary");
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert!(!read(&store).messages.is_empty());
    std::fs::remove_file(full.with_file_name("transcript.jsonl")).unwrap();
    std::fs::remove_file(full).unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert!(store
        .session(&reference(), SessionQuery::default())
        .unwrap()
        .is_none());
}
