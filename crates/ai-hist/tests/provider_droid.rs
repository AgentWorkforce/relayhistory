//! Public Factory Droid acquisition acceptance in isolated fixture homes.
mod provider_support;
use ai_hist::{
    DiscoveryOptions, HydrateOptions, ProviderRoots, Role, SessionEvidence, SessionQuery,
    SessionRef, SessionStore, Source, StoreOptions, SyncOptions,
};
use std::path::{Path, PathBuf};
const SID: &str = "factory-fixture";
const FIXTURE: &str =
    include_str!("fixtures/droid/v2-synthetic/.factory/sessions/-fixture/factory-fixture.jsonl");
fn stage(home: &Path) -> PathBuf {
    let path = home.join(".factory/sessions/-fixture/factory-fixture.jsonl");
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
        .hydrate(
            &SessionRef::id(Source::Droid, SID),
            HydrateOptions::default(),
        )
        .unwrap();
}
fn read(store: &SessionStore) -> SessionEvidence {
    store
        .session(&SessionRef::id(Source::Droid, SID), SessionQuery::default())
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
fn droid_discover_hydrate_read_reopen_changes_export_fullsync() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path());
    std::fs::write(
        home.path()
            .join(".factory/sessions/-fixture/factory-fixture.settings.json"),
        include_str!(
            "fixtures/droid/v2-synthetic/.factory/sessions/-fixture/factory-fixture.settings.json"
        ),
    )
    .unwrap();
    let e = provider_support::assert_history_conformance(
        home.path(),
        provider_support::HistoryCase {
            source: Source::Droid,
            session_id: SID,
            messages: &[
                (Role::User, "Inspect the Factory fixture."),
                (Role::Assistant, "The greeting says Hello Factory fixture."),
            ],
        },
    );
    assert!(e
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .any(|b| b.role == Role::User && b.text.as_deref() == Some("Try the next fixture.")));
    assert!(e
        .prompts
        .iter()
        .any(|p| p.prompt.as_deref() == Some("Try the next fixture.")));
    assert_eq!(e.tool_calls.len(), 1);
    assert_eq!(e.tool_results.len(), 2);
    let failure = e
        .tool_results
        .iter()
        .find(|r| r.tool_use_id.as_deref() == Some("missing1"))
        .unwrap();
    assert_eq!(failure.result_status.as_deref(), Some("errored"));
    assert_eq!(
        failure.error_signal.as_deref(),
        Some("tool_result.is_error")
    );
    assert_eq!(failure.event_source.as_deref(), Some("tool_result"));
    assert!(e.messages.iter().all(|m| m.usage.is_none()));
    assert!(e.markers.iter().any(|m| m.kind == "session_settings"));
    assert!(e.file_edits.is_empty());
    assert!(e.relationships.is_empty());
    assert!(!has(&e, "Do not promote unknown blocks."));
}
#[test]
fn droid_partial_append_rewrite_and_corruption_preserve_history() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Droid]))
        .unwrap();
    hydrate(&store);
    let row=r#"{"type":"message","id":"resumed","timestamp":"2026-11-01T00:01:00Z","message":{"role":"user","content":[{"type":"text","text":"Resumed Factory prompt"}]}}"#.to_owned()+"\n";
    std::fs::write(&path, format!("{FIXTURE}{}", &row[..25])).unwrap();
    hydrate(&store);
    assert!(!has(&read(&store), "Resumed Factory prompt"));
    std::fs::write(&path, format!("{FIXTURE}{row}")).unwrap();
    hydrate(&store);
    assert!(has(&read(&store), "Resumed Factory prompt"));
    let n = read(&store).messages.len();
    hydrate(&store);
    assert_eq!(read(&store).messages.len(), n);
    std::fs::write(&path, format!("{FIXTURE}not json\n")).unwrap();
    assert!(store
        .hydrate(
            &SessionRef::id(Source::Droid, SID),
            HydrateOptions::default()
        )
        .is_err());
    assert_eq!(read(&store).messages.len(), n);
    std::fs::write(
        &path,
        FIXTURE.replace("Hello Factory fixture.", "Rewritten Factory fixture."),
    )
    .unwrap();
    hydrate(&store);
    assert!(!has(&read(&store), "Resumed Factory prompt"));
    assert!(has(
        &read(&store),
        "The greeting says Rewritten Factory fixture."
    ));
}
#[test]
fn droid_settings_only_changes_and_invalid_sidecar_are_not_silently_skipped() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let settings = path.with_extension("settings.json");
    let store = open(home.path());
    store
        .sync(SyncOptions::default().sources([Source::Droid]))
        .unwrap();
    std::fs::write(
        &settings,
        r#"{"model":"new-model","tokenUsage":{"inputTokens":90}}"#,
    )
    .unwrap();
    store
        .sync(SyncOptions::default().sources([Source::Droid]))
        .unwrap();
    assert!(read(&store)
        .markers
        .iter()
        .any(|m| m.kind == "session_settings"));
    std::fs::write(&settings, "{").unwrap();
    assert!(store
        .hydrate(
            &SessionRef::id(Source::Droid, SID),
            HydrateOptions::default()
        )
        .is_err());
    assert!(has(&read(&store), "Inspect the Factory fixture."));
}
#[test]
fn droid_unsupported_version_duplicate_id_and_legacy_root() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let store = open(home.path());
    std::fs::write(&path, FIXTURE.replace("\"version\":2", "\"version\":999")).unwrap();
    assert!(
        store
            .discover(DiscoveryOptions::default().sources([Source::Droid]))
            .unwrap()
            .discovered
            == 0
    );
    std::fs::write(&path, FIXTURE).unwrap();
    store
        .discover(DiscoveryOptions::default().sources([Source::Droid]))
        .unwrap();
    hydrate(&store);
    let legacy = home.path().join(".factory/sessions/legacy.jsonl");
    std::fs::write(
        &legacy,
        FIXTURE.replace("factory-fixture", "legacy-session"),
    )
    .unwrap();
    store
        .sync(SyncOptions::default().sources([Source::Droid]))
        .unwrap();
    assert!(store
        .session(
            &SessionRef::id(Source::Droid, "legacy-session"),
            SessionQuery::default()
        )
        .unwrap()
        .is_some());
    std::fs::write(&legacy, FIXTURE).unwrap();
    assert!(store
        .hydrate(
            &SessionRef::id(Source::Droid, SID),
            HydrateOptions::default()
        )
        .is_ok());
    // Another live locator may not claim the already indexed native identity.
    let report = store
        .discover(DiscoveryOptions::default().sources([Source::Droid]))
        .unwrap();
    assert!(report
        .diagnostics
        .iter()
        .any(|d| d.message.contains("ID_COLLISION")));
}

#[test]
fn droid_native_startup_is_metadata_only_not_a_successful_agent_turn() {
    let home = tempfile::tempdir().unwrap();
    let path = home
        .path()
        .join(".factory/sessions/-fixture/native-startup.jsonl");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(
        &path,
        include_str!(
            "fixtures/droid/v2-native-startup/.factory/sessions/-fixture/native-startup.jsonl"
        ),
    )
    .unwrap();
    std::fs::write(path.with_extension("settings.json"), include_str!("fixtures/droid/v2-native-startup/.factory/sessions/-fixture/native-startup.settings.json")).unwrap();
    let store = open(home.path());
    store
        .sync(SyncOptions::default().sources([Source::Droid]))
        .unwrap();
    let evidence = store
        .session(
            &SessionRef::id(Source::Droid, "native-startup"),
            SessionQuery::default(),
        )
        .unwrap()
        .unwrap();
    assert_eq!(Source::parse("factory"), Some(Source::Droid));
    assert_eq!(
        evidence.session.title.as_deref(),
        Some("Native startup fixture")
    );
    assert!(evidence.messages.is_empty());
    assert!(evidence.prompts.is_empty());
    assert!(evidence.markers.iter().any(|m| m.kind == "session_start"));
    assert!(evidence
        .markers
        .iter()
        .any(|m| m.kind == "session_settings"));
}

#[test]
fn droid_bad_block_rolls_back_and_unknown_role_stays_a_marker() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Droid]))
        .unwrap();
    hydrate(&store);
    let before = serde_json::to_value(read(&store)).unwrap();
    std::fs::write(
        &path,
        FIXTURE.replace("\"is_error\":false", "\"is_error\":\"bad\""),
    )
    .unwrap();
    assert!(store
        .hydrate(
            &SessionRef::id(Source::Droid, SID),
            HydrateOptions::default()
        )
        .is_err());
    assert_eq!(serde_json::to_value(read(&store)).unwrap(), before);
    let control = "{\"type\":\"message\",\"id\":\"control\",\"timestamp\":\"2026-11-01T00:01:00Z\",\"message\":{\"role\":\"system\",\"content\":[{\"type\":\"text\",\"text\":\"Not a human prompt\"}]}}\n";
    std::fs::write(&path, format!("{FIXTURE}{control}")).unwrap();
    hydrate(&store);
    let evidence = read(&store);
    assert!(!has(&evidence, "Not a human prompt"));
    assert!(evidence
        .markers
        .iter()
        .any(|m| m.message_id.as_deref() == Some("control")));
}

#[test]
fn droid_watch_automatically_captures_append_and_settings_changes() {
    use std::time::{Duration, Instant};
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let store = open(home.path());
    let mut options = ai_hist::WatchOptions::default();
    options.use_fs_events = false;
    options.poll_interval_ms = 50;
    let mut watch = store.watch(options).unwrap();
    assert!(
        watch
            .next_timeout(Duration::from_secs(15))
            .unwrap()
            .unwrap()
            .swept
    );
    let row = "{\"type\":\"message\",\"id\":\"watch-append\",\"timestamp\":\"2026-11-01T00:01:00Z\",\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"Automatically captured\"}]}}\n";
    std::fs::write(&path, format!("{FIXTURE}{row}")).unwrap();
    std::fs::write(
        path.with_extension("settings.json"),
        "{\"model\":\"watch-model\"}",
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut captured = false;
    while Instant::now() < deadline {
        if let Some(tick) = watch.next_timeout(Duration::from_secs(1)) {
            tick.unwrap();
            let evidence = read(&store);
            captured = has(&evidence, "Automatically captured")
                && evidence
                    .markers
                    .iter()
                    .any(|m| m.raw_payload().is_some_and(|p| p.contains("watch-model")));
            if captured {
                break;
            }
        }
    }
    watch.stopper().stop();
    assert!(captured, "watch must capture without an explicit sync call");
}

#[test]
fn authenticated_native_writer_keeps_hook_and_model_context_out_of_human_history() {
    let home = tempfile::tempdir().unwrap();
    let path = home
        .path()
        .join(".factory/sessions/-fixture/native-writer.jsonl");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(
        &path,
        include_str!(
            "fixtures/droid/v2-native-writer/.factory/sessions/-fixture/native-writer.jsonl"
        ),
    )
    .unwrap();
    std::fs::write(path.with_extension("settings.json"), include_str!("fixtures/droid/v2-native-writer/.factory/sessions/-fixture/native-writer.settings.json")).unwrap();
    let evidence = provider_support::assert_history_conformance(
        home.path(),
        provider_support::HistoryCase {
            source: Source::Droid,
            session_id: "native-writer",
            messages: &[
                (
                    Role::User,
                    "Reply exactly FACTORY_NATIVE_WRITER_OK. Do not call any tools.",
                ),
                (Role::Assistant, "FACTORY_NATIVE_WRITER_OK"),
            ],
        },
    );
    assert_eq!(evidence.messages.len(), 2);
    assert_eq!(evidence.prompts.len(), 1);
    assert_eq!(
        evidence.session.first_prompt.as_deref(),
        Some("Reply exactly FACTORY_NATIVE_WRITER_OK. Do not call any tools.")
    );
    assert!(!evidence.prompts.iter().any(|p| p
        .prompt
        .as_deref()
        .is_some_and(|text| text.contains("system-reminder"))));
    assert!(evidence
        .markers
        .iter()
        .any(|m| m.kind == "agent_turn_outcome"));
    assert!(
        evidence
            .markers
            .iter()
            .filter(|m| m.kind == "message")
            .count()
            >= 5
    );
}

#[test]
fn rewriting_a_user_row_as_explicit_runtime_context_retires_its_old_prompt() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let store = open(home.path());
    let row = serde_json::json!({"type":"message","id":"runtime-context","timestamp":"2026-11-01T00:02:00Z","message":{"role":"user","content":[{"type":"text","text":"Runtime metadata"}],"visibility":"both"}});
    std::fs::write(&path, format!("{FIXTURE}{row}\n")).unwrap();
    store
        .discover(DiscoveryOptions::default().sources([Source::Droid]))
        .unwrap();
    hydrate(&store);
    assert!(read(&store)
        .prompts
        .iter()
        .any(|p| p.prompt.as_deref() == Some("Runtime metadata")));
    let mut control = row;
    control["message"]["visibility"] = "llm_only".into();
    std::fs::write(&path, format!("{FIXTURE}{control}\n")).unwrap();
    hydrate(&store);
    let evidence = read(&store);
    assert!(!evidence
        .prompts
        .iter()
        .any(|p| p.prompt.as_deref() == Some("Runtime metadata")));
    assert!(!has(&evidence, "Runtime metadata"));
    assert!(evidence
        .markers
        .iter()
        .any(|m| m.message_id.as_deref() == Some("runtime-context")));
}
