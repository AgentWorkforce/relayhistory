//! Catalog previews from complete user steps in otherwise incomplete captures.
use ai_hist::{
    CatalogQuery, DiscoveryOptions, HydrateOptions, ProviderRoots, SessionRef, SessionStore,
    Source, StoreOptions, SyncOptions,
};
use std::path::{Path, PathBuf};

fn stage(home: &Path, case: &str) -> PathBuf {
    fn copy(from: &Path, to: &Path) {
        std::fs::create_dir_all(to).unwrap();
        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            let target = to.join(entry.file_name());
            if entry.path().is_dir() {
                copy(&entry.path(), &target);
            } else {
                std::fs::copy(entry.path(), target).unwrap();
            }
        }
    }
    copy(
        &Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/antigravity")
            .join(case),
        home,
    );
    let session = std::fs::read_dir(home.join(".gemini/antigravity/brain"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    session.join(".system_generated/logs")
}
fn open(home: &Path) -> SessionStore {
    SessionStore::open(
        StoreOptions::default()
            .db_path(home.join("history.db"))
            .roots(ProviderRoots::from_home(
                home.into(),
                home.join("opencode.db"),
            )),
    )
    .unwrap()
}
fn preview(store: &SessionStore) -> Option<String> {
    store
        .sessions(CatalogQuery::default())
        .next()
        .unwrap()
        .unwrap()
        .first_prompt
}

#[test]
fn compact_only_capture_populates_preview_from_complete_user_step() {
    let home = tempfile::tempdir().unwrap();
    let logs = stage(home.path(), "long");
    std::fs::remove_file(logs.join("transcript_full.jsonl")).unwrap();
    let compact = logs.join("transcript.jsonl");
    let original = std::fs::read_to_string(&compact).unwrap();
    let mut rows: Vec<serde_json::Value> = original
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    rows[0]["source"] = "SYSTEM".into();
    std::fs::write(
        &compact,
        rows.iter()
            .map(|row| serde_json::to_string(row).unwrap() + "\n")
            .collect::<String>(),
    )
    .unwrap();
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Antigravity]))
        .unwrap();
    assert_eq!(preview(&store), None);
    let session = store
        .sessions(CatalogQuery::default())
        .next()
        .unwrap()
        .unwrap();
    std::fs::write(compact, original).unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Antigravity, session.session_id),
            HydrateOptions::default(),
        )
        .unwrap();
    assert_eq!(
        preview(&store).as_deref(),
        Some("Return the long synthetic fixture response.")
    );
}

#[test]
fn discovery_does_not_advertise_a_truncated_user_prompt() {
    let home = tempfile::tempdir().unwrap();
    let logs = stage(home.path(), "ordinary");
    let full = logs.join("transcript_full.jsonl");
    let mut rows: Vec<serde_json::Value> = std::fs::read_to_string(&full)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    rows[0]["is_truncated"] = true.into();
    std::fs::write(
        logs.join("transcript.jsonl"),
        rows.iter()
            .map(|row| serde_json::to_string(row).unwrap() + "\n")
            .collect::<String>(),
    )
    .unwrap();
    std::fs::remove_file(full).unwrap();
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Antigravity]))
        .unwrap();
    assert_eq!(preview(&store), None);
}

#[test]
fn truncated_user_step_cannot_replace_existing_complete_preview() {
    let home = tempfile::tempdir().unwrap();
    let logs = stage(home.path(), "ordinary");
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let original = preview(&store);
    assert_eq!(
        original.as_deref(),
        Some("Remember the synthetic fixture color blue.")
    );
    let full = logs.join("transcript_full.jsonl");
    let mut rows: Vec<serde_json::Value> = std::fs::read_to_string(&full)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    rows[0]["content"] = "shortened user prompt".into();
    rows[0]["truncated_fields"] = serde_json::json!(["content"]);
    let compact = rows
        .iter()
        .map(|row| serde_json::to_string(row).unwrap() + "\n")
        .collect::<String>();
    std::fs::write(logs.join("transcript.jsonl"), compact).unwrap();
    std::fs::remove_file(full).unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert_eq!(preview(&store), original);
}
