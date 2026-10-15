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
#[test]
fn gemini_complete_identity_rewrite_retires_old_local_session_and_guards_selected_identity() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let store = open(home.path());
    discover(&store);
    hydrate(&store);
    let next = "44444444-4444-4444-8444-444444444444";
    std::fs::write(&path, FIXTURE.replacen(SID, next, 1)).unwrap();
    assert!(
        store
            .hydrate(
                &SessionRef::id(Source::Gemini, SID),
                HydrateOptions::default()
            )
            .is_err(),
        "selected old identity must not import a different native session"
    );
    assert!(has(&read(&store), "Found the synthetic Gemini needle."));
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert!(
        store
            .session(
                &SessionRef::id(Source::Gemini, SID),
                SessionQuery::default()
            )
            .unwrap()
            .is_none(),
        "old local session must retire when its existing locator acquires a new native identity"
    );
    let e = store
        .session(
            &SessionRef::id(Source::Gemini, next),
            SessionQuery::default(),
        )
        .unwrap()
        .unwrap();
    assert!(has(&e, "Found the synthetic Gemini needle."));
}
#[test]
fn gemini_unsupported_nested_patches_preserve_indexed_evidence_and_remain_retryable() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let store = open(home.path());
    discover(&store);
    hydrate(&store);
    let patch="{\"$patch\":{\"updates\":[{\"id\":\"assistant-one\",\"toolCalls\":[{\"id\":\"call-real-id\",\"futureField\":true}]}]}}\n";
    std::fs::write(&path, format!("{FIXTURE}{patch}")).unwrap();
    for _ in 0..2 {
        let error = store
            .hydrate(
                &SessionRef::id(Source::Gemini, SID),
                HydrateOptions::default(),
            )
            .unwrap_err();
        assert!(error.to_string().contains("GEMINI_UNSUPPORTED_SCHEMA"));
        let evidence = read(&store);
        assert!(has(&evidence, "Found the synthetic Gemini needle."));
        assert_eq!(evidence.tool_results.len(), 1);
    }
    std::fs::write(&path, FIXTURE).unwrap();
    hydrate(&store);
    assert!(has(&read(&store), "Found the synthetic Gemini needle."));
}
#[test]
fn gemini_shallow_tail_observes_resumed_activity_and_complete_rewrites_clear_title() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let store = open(home.path());
    let padding = serde_json::json!({"$set":{"memoryScratchpad":"p".repeat(80_000)}}).to_string();
    let updated = "2026-10-12T12:30:00Z";
    std::fs::write(&path,format!("{FIXTURE}{padding}\n{{\"$set\":{{\"lastUpdated\":\"{updated}\",\"summary\":\"A temporary title\"}}}}\n{{\"$set\":" )).unwrap();
    discover(&store);
    let e = read(&store);
    assert_eq!(e.session.last_activity_ms, Some(1791808200000));
    hydrate(&store);
    // Partial captures preserve a prior title. A complete capture assigns it.
    std::fs::write(
        &path,
        format!("{FIXTURE}{{\"$set\":{{\"summary\":\"A temporary title\"}}}}\n"),
    )
    .unwrap();
    hydrate(&store);
    assert_eq!(
        read(&store).session.title.as_deref(),
        Some("A temporary title")
    );
    std::fs::write(&path, FIXTURE).unwrap();
    hydrate(&store);
    assert_eq!(read(&store).session.title, None);
}
#[test]
fn gemini_sync_only_migration_survives_legacy_deletion_and_cached_journal() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    std::fs::remove_file(&path).unwrap();
    let legacy_path = path.with_extension("json");
    std::fs::write(
        &legacy_path,
        include_str!(
            "fixtures/gemini/legacy/.gemini/tmp/gemini-project/chats/session-legacy-22222222.json"
        ),
    )
    .unwrap();
    let store = open(home.path());
    let reference = SessionRef::id(Source::Gemini, "22222222-2222-4222-8222-222222222222");
    store.sync(SyncOptions::default().force(true)).unwrap();
    std::fs::write(&path,include_str!("fixtures/gemini/legacy-migrated/.gemini/tmp/gemini-project/chats/session-legacy-22222222.jsonl")).unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    let before = store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    assert_eq!(
        before
            .session
            .raw_path
            .as_deref()
            .unwrap()
            .extension()
            .unwrap(),
        "jsonl"
    );
    std::fs::remove_file(&legacy_path).unwrap();
    for _ in 0..2 {
        store.sync(SyncOptions::default().force(true)).unwrap();
        let after = store
            .session(&reference, SessionQuery::default())
            .unwrap()
            .unwrap();
        assert_eq!(before.messages, after.messages);
        assert!(has(&after, "Continue the synthetic legacy session."));
    }
}
#[test]
fn gemini_tool_and_opaque_usage_stays_measured_without_inferred_prompt_ownership() {
    for (opaque, tools) in [(false, true), (true, false), (true, true)] {
        let home = tempfile::tempdir().unwrap();
        let path = stage(home.path());
        let records=FIXTURE.lines().map(|line| {let mut row:serde_json::Value=serde_json::from_str(line).unwrap();if row["id"]=="assistant-one" {
            if opaque {row["content"]=serde_json::json!([{"inlineData":{"mimeType":"image/png","data":"AA=="}}]);}
            else {row["content"]=serde_json::json!([]);}
            if !tools {row["toolCalls"]=serde_json::json!([]);}
        }row.to_string()}).collect::<Vec<_>>().join("\n")+"\n";
        std::fs::write(&path, records).unwrap();
        let store = open(home.path());
        discover(&store);
        hydrate(&store);
        let evidence = read(&store);
        let usage = evidence.usage.as_ref().unwrap().usage.as_ref().unwrap();
        assert_eq!(
            (
                usage.input_tokens,
                usage.cache_read_tokens,
                usage.output_tokens
            ),
            (90, 30, 20)
        );
        // Raw event grouping uses the explicitly opt-in internal test surface.
        #[cfg(feature = "unstable-internal")]
        {
            let conn = ai_hist::open_db(store.db_path()).unwrap();
            let events = ai_hist::session_events(&conn, SID, Some("gemini")).unwrap();
            let assistant: Vec<_> = events
                .iter()
                .filter(|e| e.message_id.as_deref() == Some("assistant-one"))
                .collect();
            assert!(assistant.iter().any(|e| e.token_json.is_some()));
            assert!(assistant
                .iter()
                .all(|e| e.role == "assistant" && e.ts_ms == 1791720006000));
            assert!(
                ai_hist::attribute_usage_to_prompts(&events, "gemini").is_empty(),
                "upstream records no ownership links; attribution must remain unavailable"
            );
        }
        if tools {
            assert_eq!(evidence.tool_calls[0].ts_ms, Some(1791720008000));
        }
    }
}
#[test]
fn gemini_bad_migration_pair_does_not_block_healthy_sessions() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path());
    let directory = home.path().join(".gemini/tmp/other-project/chats");
    std::fs::create_dir_all(&directory).unwrap();
    let legacy = include_str!(
        "fixtures/gemini/legacy/.gemini/tmp/gemini-project/chats/session-legacy-22222222.json"
    );
    let journal=include_str!("fixtures/gemini/legacy-migrated/.gemini/tmp/gemini-project/chats/session-legacy-22222222.jsonl");
    std::fs::write(directory.join("bad.json"), legacy).unwrap();
    std::fs::write(
        directory.join("bad.jsonl"),
        journal.replace(
            "52e30737efc78fb781017102c6074d53ab432a8cc4edc0a26b2b9756700cd3a0",
            "mismatched-project",
        ),
    )
    .unwrap();
    let store = open(home.path());
    let report = store
        .discover(DiscoveryOptions::default().sources([Source::Gemini]))
        .unwrap();
    assert!(!report.diagnostics.is_empty());
    hydrate(&store);
    assert!(has(&read(&store), "Found the synthetic Gemini needle."));
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert!(has(&read(&store), "Found the synthetic Gemini needle."));
}

#[test]
fn gemini_fresh_migration_pair_discovers_and_hydrates_journal() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    std::fs::write(&path, include_str!("fixtures/gemini/legacy-migrated/.gemini/tmp/gemini-project/chats/session-legacy-22222222.jsonl")).unwrap();
    std::fs::write(
        path.with_extension("json"),
        include_str!(
            "fixtures/gemini/legacy/.gemini/tmp/gemini-project/chats/session-legacy-22222222.json"
        ),
    )
    .unwrap();
    let store = open(home.path());
    discover(&store);
    let reference = SessionRef::id(Source::Gemini, "22222222-2222-4222-8222-222222222222");
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    let evidence = store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    assert!(has(&evidence, "Continue the synthetic legacy session."));
    assert_eq!(
        evidence.session.raw_path.unwrap().extension().unwrap(),
        "jsonl"
    );
}

#[test]
fn gemini_complete_unterminated_final_activity_is_discovered() {
    for padding in [
        String::new(),
        serde_json::json!({"$set":{"memoryScratchpad":"p".repeat(80_000)}}).to_string() + "\n",
    ] {
        let home = tempfile::tempdir().unwrap();
        let path = stage(home.path());
        std::fs::write(
            &path,
            format!("{FIXTURE}{padding}{{\"$set\":{{\"lastUpdated\":\"2026-10-12T12:30:00Z\"}}}}"),
        )
        .unwrap();
        let store = open(home.path());
        discover(&store);
        assert_eq!(read(&store).session.last_activity_ms, Some(1791808200000));
        hydrate(&store);
        assert!(has(&read(&store), "Found the synthetic Gemini needle."));
    }
}
