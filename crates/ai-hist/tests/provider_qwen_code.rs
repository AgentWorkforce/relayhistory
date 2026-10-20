//! Public acquisition acceptance against pinned native Qwen Code recorder output.
mod provider_support;
use ai_hist::{
    DiscoveryOptions, HydrateOptions, ProviderRoots, Role, SessionEvidence, SessionQuery,
    SessionRef, SessionStore, Source, StoreOptions, SyncOptions,
};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
fn native(n: u8) -> String {
    format!("11111111-1111-4111-8111-{n:012}")
}
fn sid(n: u8) -> String {
    format!(
        "root:{:x}:{}",
        Sha256::digest(b".qwen/projects/-synthetic-qwen-project"),
        native(n)
    )
}
fn stage(home: &Path, case: &str, n: u8) -> PathBuf {
    let relative = PathBuf::from(".qwen/projects/-synthetic-qwen-project/chats")
        .join(format!("{}.jsonl", native(n)));
    let from = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/qwen-code")
        .join(case)
        .join(&relative);
    let to = home.join(relative);
    std::fs::create_dir_all(to.parent().unwrap()).unwrap();
    std::fs::copy(from, &to).unwrap();
    to
}
fn open(home: &Path) -> SessionStore {
    SessionStore::open(StoreOptions::default().db_path(home.join("qwen.db")).roots(
        ProviderRoots::from_home(home.into(), home.join("opencode.db")),
    ))
    .unwrap()
}
fn reference(n: u8) -> SessionRef {
    SessionRef::id(Source::QwenCode, sid(n))
}
fn read(store: &SessionStore, n: u8) -> SessionEvidence {
    store
        .session(&reference(n), SessionQuery::default())
        .unwrap()
        .unwrap()
}
fn hydrate(store: &SessionStore, n: u8) {
    store
        .hydrate(&reference(n), HydrateOptions::default())
        .unwrap();
}
fn texts(e: &SessionEvidence) -> Vec<String> {
    e.messages
        .iter()
        .flat_map(|m| &m.blocks)
        .filter(|b| b.kind == ai_hist::BlockKind::Text)
        .filter_map(|b| b.text.clone())
        .collect()
}
#[test]
fn qwen_native_discover_hydrate_read_search_reopen_export_and_repeat() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), "ordinary", 1);
    let e = provider_support::assert_history_conformance(
        home.path(),
        provider_support::HistoryCase {
            source: Source::QwenCode,
            session_id: &sid(1),
            messages: &[
                (Role::User, "Find the synthetic Qwen needle."),
                (Role::Assistant, "Found the synthetic Qwen needle."),
            ],
        },
    );
    assert_eq!(e.markers.len(), 2);
    let usage = e.usage.as_ref().unwrap().usage.as_ref().unwrap();
    assert_eq!(usage.input_tokens, 90);
    assert_eq!(usage.cache_read_tokens, 10);
    assert_eq!(usage.output_tokens, 20);
    assert_eq!(usage.provider_total_tokens, Some(120));
    assert_eq!(usage.reasoning_tokens, None);
    assert_eq!(e.messages.len(), 2);
    assert!(e.tool_calls.is_empty());
    assert_eq!(
        Source::QwenCode.capabilities().usage_accounting,
        Some(ai_hist::UsageAccounting::PerMessage)
    );
}
#[test]
fn qwen_rewind_follows_native_parent_chain_and_preserves_raw_discarded_branch() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), "rewind", 4);
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let e = read(&store, 4);
    assert_eq!(
        texts(&e),
        [
            "Replacement synthetic Qwen request.",
            "Replacement synthetic answer."
        ]
    );
    assert_eq!(e.markers.len(), 5);
    hydrate(&store, 4);
    assert_eq!(read(&store, 4).messages, e.messages);
    let before = store.head_revision().unwrap();
    hydrate(&store, 4);
    assert_eq!(store.head_revision().unwrap(), before);
}
#[test]
fn qwen_native_tools_reasoning_remain_markers_without_invented_joins() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), "tool", 2);
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let e = read(&store, 2);
    assert_eq!(
        texts(&e),
        [
            "Find the synthetic Qwen needle.",
            "Found the synthetic Qwen needle."
        ]
    );
    assert_eq!(e.markers.len(), 4);
    assert_eq!(e.tool_calls.len(), 1);
}
#[test]
fn qwen_native_restart_restores_same_timestamp_parent_order() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), "resumed", 3);
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let e = read(&store, 3);
    assert_eq!(
        texts(&e),
        [
            "Find the synthetic Qwen needle.",
            "Found the synthetic Qwen needle.",
            "Continue the synthetic investigation.",
            "The resumed investigation is complete."
        ]
    );
    assert!(e.messages.windows(2).all(|w| w[0].ts_ms == w[1].ts_ms));
    drop(store);
    assert_eq!(read(&open(home.path()), 3).messages, e.messages);
}
#[test]
fn qwen_partial_tail_preserves_complete_evidence_then_rewrite_reconciles() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "ordinary", 1);
    let original = std::fs::read_to_string(&path).unwrap();
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let initial = read(&store, 1);
    std::fs::write(&path, format!("{original}{{\"uuid\":")).unwrap();
    assert!(store
        .hydrate(&reference(1), HydrateOptions::default())
        .unwrap_err()
        .to_string()
        .contains("PARTIAL_REPLACEMENT_DEFERRED"));
    assert_eq!(read(&store, 1).messages, initial.messages);
    std::fs::write(
        &path,
        original.replace("Found the synthetic Qwen needle.", "Rewritten answer."),
    )
    .unwrap();
    hydrate(&store, 1);
    assert_eq!(texts(&read(&store, 1))[1], "Rewritten answer.");
    let revision = store.head_revision().unwrap();
    hydrate(&store, 1);
    assert_eq!(store.head_revision().unwrap(), revision);
    std::fs::write(&path, "{broken}\n").unwrap();
    assert!(store
        .hydrate(&reference(1), HydrateOptions::default())
        .is_err());
    assert_eq!(texts(&read(&store, 1))[1], "Rewritten answer.");
}
#[test]
fn qwen_containment_refuses_symlinked_history() {
    #[cfg(unix)]
    {
        let home = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let native = stage(outside.path(), "ordinary", 1);
        let dir = home.path().join(".qwen/projects/project/chats");
        std::fs::create_dir_all(&dir).unwrap();
        std::os::unix::fs::symlink(native, dir.join("stolen.jsonl")).unwrap();
        let store = open(home.path());
        assert!(store
            .discover(DiscoveryOptions::default().sources([Source::QwenCode]))
            .is_err());
        assert!(store
            .session(&reference(1), SessionQuery::default())
            .unwrap()
            .is_none());
    }
}
#[test]
fn qwen_missing_parent_does_not_insert_same_time_answer_ahead_of_request() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "ordinary", 1);
    let original = std::fs::read_to_string(&path).unwrap();
    std::fs::write(&path, original.lines().nth(1).unwrap()).unwrap();
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert!(store
        .session(&reference(1), SessionQuery::default())
        .unwrap()
        .is_none_or(|e| e.messages.is_empty()));
    std::fs::write(path, original).unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert_eq!(
        texts(&read(&store, 1)),
        [
            "Find the synthetic Qwen needle.",
            "Found the synthetic Qwen needle."
        ]
    );
}
#[test]
fn qwen_fourteen_same_time_native_messages_preserve_full_and_single_event_page_order() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), "same-time", 6);
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let evidence = read(&store, 6);
    assert_eq!(evidence.messages.len(), 14);
    assert!(evidence
        .messages
        .windows(2)
        .all(|w| w[0].ts_ms == w[1].ts_ms));
    let expected = texts(&evidence);
    assert_eq!(expected[2], "Synthetic request 1.");
    assert_eq!(expected[13], "Synthetic answer 6.");
    #[cfg(feature = "unstable-internal")]
    {
        let conn = ai_hist::open_db(store.db_path()).unwrap();
        let mut cursor = None;
        let mut actual = Vec::new();
        loop {
            let page =
                ai_hist::session_events_page(&conn, &sid(6), Some("qwen-code"), 1, cursor.as_ref())
                    .unwrap();
            for event in page.events {
                if let Some(text) = event.text {
                    actual.push(text);
                }
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(actual, expected);
    }
    hydrate(&store, 6);
    let revision = store.head_revision().unwrap();
    hydrate(&store, 6);
    assert_eq!(store.head_revision().unwrap(), revision);
    drop(store);
    assert_eq!(texts(&read(&open(home.path()), 6)), expected);
}
#[test]
fn qwen_bounded_discovery_has_no_invented_preview_and_hydrates_large_first_record() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "ordinary", 1);
    let original = std::fs::read_to_string(&path).unwrap();
    let mut rows = original
        .lines()
        .map(|s| serde_json::from_str::<serde_json::Value>(s).unwrap())
        .collect::<Vec<_>>();
    let prompt = "Synthetic large Qwen prompt ".repeat(5000);
    rows[0]["message"]["parts"][0]["text"] = prompt.clone().into();
    std::fs::write(
        &path,
        rows.iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n"),
    )
    .unwrap();
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::QwenCode]))
        .unwrap();
    let shallow = read(&store, 1);
    assert!(shallow.session.first_prompt.is_none());
    assert!(shallow.messages.is_empty());
    hydrate(&store, 1);
    assert_eq!(texts(&read(&store, 1))[0], prompt);
}
#[test]
fn qwen_complete_rewrite_retires_removed_messages_and_title() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "metadata", 5);
    let original = std::fs::read_to_string(&path).unwrap();
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let initial = read(&store, 5);
    assert_eq!(
        initial.session.title.as_deref(),
        Some("Synthetic Qwen title.")
    );
    std::fs::write(path, original.lines().next().unwrap()).unwrap();
    hydrate(&store, 5);
    let after = read(&store, 5);
    assert_eq!(after.messages.len(), 1);
    assert_eq!(after.markers.len(), 1);
    assert_eq!(after.session.title, None);
    hydrate(&store, 5);
    let revision = store.head_revision().unwrap();
    hydrate(&store, 5);
    assert_eq!(store.head_revision().unwrap(), revision);
}
#[test]
fn qwen_mixed_assistant_parts_keep_native_block_order_and_recorded_usage() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "tool", 2);
    let mut rows = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|s| serde_json::from_str::<serde_json::Value>(s).unwrap())
        .collect::<Vec<_>>();
    let call = rows[1]["message"]["parts"][1].clone();
    rows[1]["message"]["parts"] =
        serde_json::json!([{"text":"Before native call."},call,{"text":"After native call."}]);
    std::fs::write(
        path,
        rows.iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n"),
    )
    .unwrap();
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let e = read(&store, 2);
    let blocks = &e.messages[1].blocks;
    assert_eq!(
        blocks.iter().map(|b| b.kind).collect::<Vec<_>>(),
        [
            ai_hist::BlockKind::Text,
            ai_hist::BlockKind::ToolUse,
            ai_hist::BlockKind::Text
        ]
    );
    assert_eq!(blocks[0].text.as_deref(), Some("Before native call."));
    assert_eq!(blocks[2].text.as_deref(), Some("After native call."));
    assert_eq!(
        e.usage
            .as_ref()
            .unwrap()
            .usage
            .as_ref()
            .unwrap()
            .input_tokens,
        90
    );
}
#[test]
fn qwen_partial_replacement_keeps_prior_messages_tools_markers_usage_and_catalog() {
    assert_partial_replacement(false);
}
#[test]
fn qwen_bounded_partial_replacement_keeps_prior_complete_snapshot() {
    assert_partial_replacement(true);
}
fn assert_partial_replacement(large: bool) {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "tool", 2);
    let mut rows = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|s| serde_json::from_str::<serde_json::Value>(s).unwrap())
        .collect::<Vec<_>>();
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    hydrate(&store, 2);
    let initial = read(&store, 2);
    let request = if large {
        "Replacement synthetic request. ".repeat(5000)
    } else {
        "Replacement synthetic request.".into()
    };
    rows[0]["message"]["parts"][0]["text"] = request.clone().into();
    rows[0]["cwd"] = "/synthetic/replacement".into();
    rows[1]["message"]["parts"][1]["functionCall"]["args"]["value"] = "red".into();
    rows[2]["message"]["parts"][0]["functionResponse"]["response"]["result"] = "red".into();
    rows[3]["message"]["parts"][0]["text"] = "Replacement complete answer.".into();
    rows[3]["usageMetadata"] = serde_json::json!({"promptTokenCount":200,"cachedContentTokenCount":10,"candidatesTokenCount":30,"totalTokenCount":230});
    let partial = rows[..3]
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("\n")
        + "\n{\"uuid\":";
    std::fs::write(&path, partial).unwrap();
    let hydration = store.hydrate(&reference(2), HydrateOptions::default());
    assert_eq!(
        read(&store, 2).messages,
        initial.messages,
        "partial replacement must not mix a changed prefix with prior complete messages"
    );
    let error = hydration.unwrap_err();
    assert!(error.to_string().contains("PARTIAL_REPLACEMENT_DEFERRED"));
    for action in 0..3 {
        if action == 1 {
            store
                .discover(DiscoveryOptions::default().sources([Source::QwenCode]))
                .unwrap();
        }
        if action == 2 {
            store.sync(SyncOptions::default().force(true)).unwrap();
        }
        let kept = read(&store, 2);
        assert_eq!(kept.messages, initial.messages);
        assert_eq!(kept.tool_calls, initial.tool_calls);
        assert_eq!(kept.tool_results, initial.tool_results);
        assert_eq!(kept.markers, initial.markers);
        assert_eq!(kept.prompts, initial.prompts);
        assert_eq!(kept.usage, initial.usage);
        let mut expected = initial.session.clone();
        // Shared discovery reports the current file-observation fingerprint;
        // evidence-backed metadata remains the previous complete snapshot.
        if action > 0 {
            expected.source_stamp = kept.session.source_stamp.clone();
        }
        assert_eq!(kept.session, expected);
    }
    std::fs::write(
        &path,
        rows.iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("\n"),
    )
    .unwrap();
    hydrate(&store, 2);
    let complete = read(&store, 2);
    assert_eq!(texts(&complete)[0], request);
    assert!(texts(&complete).contains(&"Replacement complete answer.".into()));
    assert_ne!(complete.tool_calls, initial.tool_calls);
    assert_eq!(
        complete
            .usage
            .as_ref()
            .unwrap()
            .usage
            .as_ref()
            .unwrap()
            .input_tokens,
        190
    );
    let revision = store.head_revision().unwrap();
    hydrate(&store, 2);
    assert_eq!(store.head_revision().unwrap(), revision);
}

#[test]
fn qwen_first_partial_prefix_is_observable_then_complete_tail_hydrates() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "ordinary", 1);
    let full = std::fs::read_to_string(&path).unwrap();
    std::fs::write(
        &path,
        full.lines().next().unwrap().to_owned() + "\n{\"uuid\":",
    )
    .unwrap();
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::QwenCode]))
        .unwrap();
    let report = store
        .hydrate(&reference(1), HydrateOptions::default())
        .unwrap();
    assert!(report
        .diagnostics
        .iter()
        .any(|d| d.code == "QWEN_CODE_INCOMPLETE_TRANSCRIPT"));
    let prefix = read(&store, 1);
    assert_eq!(texts(&prefix), ["Find the synthetic Qwen needle."]);
    assert_eq!(prefix.markers.len(), 1);
    hydrate(&store, 1);
    assert_eq!(read(&store, 1).messages, prefix.messages);
    std::fs::write(path, full).unwrap();
    hydrate(&store, 1);
    assert_eq!(
        texts(&read(&store, 1)),
        [
            "Find the synthetic Qwen needle.",
            "Found the synthetic Qwen needle."
        ]
    );
}
