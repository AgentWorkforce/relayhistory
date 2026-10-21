//! Public acquisition of pinned official CLI and native recorder captures.
mod provider_support;
use ai_hist::{
    BlockKind, DiscoveryOptions, HydrateOptions, ProviderRoots, Role, SessionEvidence,
    SessionQuery, SessionRef, SessionStore, Source, StoreOptions, SyncOptions,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
fn sid(case: &str) -> String {
    let key = serde_json::to_vec(&(".codebuddy/projects", "fixture-workspace")).unwrap();
    format!("project:{:x}:fixture-{case}", Sha256::digest(key))
}
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
fn stage(home: &Path, case: &str) -> PathBuf {
    copy(
        &Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/codebuddy")
            .join(case),
        home,
    );
    home.join(format!(
        ".codebuddy/projects/fixture-workspace/fixture-{case}.jsonl"
    ))
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
fn reference(case: &str) -> SessionRef {
    SessionRef::id(Source::Codebuddy, sid(case))
}
fn read(store: &SessionStore, case: &str) -> SessionEvidence {
    store
        .session(&reference(case), SessionQuery::default())
        .unwrap()
        .unwrap()
}
fn sync(store: &SessionStore) {
    store.sync(SyncOptions::default().force(true)).unwrap();
}
fn texts(e: &SessionEvidence) -> Vec<&str> {
    e.messages
        .iter()
        .flat_map(|m| &m.blocks)
        .filter(|b| b.kind == BlockKind::Text)
        .filter_map(|b| b.text.as_deref())
        .collect()
}
#[test]
fn codebuddy_native_writer_public_conformance() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), "ordinary");
    let e = provider_support::assert_history_conformance(
        home.path(),
        provider_support::HistoryCase {
            source: Source::Codebuddy,
            session_id: &sid("ordinary"),
            messages: &[
                (Role::User, "Find the synthetic CodeBuddy needle."),
                (Role::Assistant, "Found the synthetic CodeBuddy needle."),
            ],
        },
    );
    assert_eq!(e.messages.len(), 2);
    assert_eq!(e.markers.len(), 3);
    assert!(e
        .messages
        .iter()
        .all(|m| m.message_id.as_ref().is_some_and(|id| !id.is_empty())));
}
#[test]
fn codebuddy_native_reader_oracles_match_active_messages() {
    let oracle: Value =
        serde_json::from_str(include_str!("fixtures/codebuddy/official-reader.json")).unwrap();
    let humans: Value =
        serde_json::from_str(include_str!("fixtures/codebuddy/official-human.json")).unwrap();
    for case in [
        "ordinary",
        "resumed",
        "tool",
        "spill",
        "error",
        "fork",
        "pending-rewind",
        "compact",
        "reasoning",
        "fragments",
        "metadata",
        "string-content",
    ] {
        let home = tempfile::tempdir().unwrap();
        stage(home.path(), case);
        let store = open(home.path());
        sync(&store);
        let evidence = read(&store, case);
        let expected = oracle[case]
            .as_array()
            .unwrap()
            .iter()
            .filter(|r| {
                r["type"] == "message"
                    && r["providerData"]["agent"] != "compact"
                    && r["providerData"]["skipRun"] != true
                    && (r["role"] != "user" || humans[case].as_array().unwrap().contains(&r["id"]))
            })
            .filter(|r| matches!(r["role"].as_str(), Some("user" | "assistant")))
            .map(|r| {
                r["content"].as_str().map(str::to_owned).unwrap_or_else(|| {
                    r["content"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .filter_map(|b| b["text"].as_str())
                        .collect::<Vec<_>>()
                        .join("\n")
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(
            texts(&evidence),
            expected.iter().map(String::as_str).collect::<Vec<_>>(),
            "{case}"
        );
        let before = store.head_revision().unwrap();
        sync(&store);
        assert_eq!(
            store.head_revision().unwrap(),
            before,
            "{case} repeated sweep"
        );
        if case == "compact" {
            assert_eq!(
                evidence.session.first_prompt.as_deref(),
                Some("Synthetic original prompt.")
            );
        }
        if case == "pending-rewind" {
            assert_eq!(evidence.messages.len(), 1);
        }
    }
}
#[test]
fn codebuddy_spills_use_full_inline_result_and_never_follow_pointer() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "spill");
    let mut rows: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|x| serde_json::from_str(x).unwrap())
        .collect();
    let result = rows
        .iter_mut()
        .find(|r| r["type"] == "function_call_result")
        .unwrap();
    result["output"]["text"] = Value::String(
        "<persisted-output>\nFull output saved to: /outside/secret.txt\n</persisted-output>".into(),
    );
    write(&path, &rows);
    std::fs::remove_dir_all(path.with_extension("")).unwrap();
    let store = open(home.path());
    sync(&store);
    let e = read(&store, "spill");
    assert_eq!(e.tool_calls.len(), 1);
    let result = e
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .find(|b| b.kind == BlockKind::ToolResult)
        .unwrap();
    assert!(result.text.as_ref().unwrap().len() > 6000);
    assert!(result
        .text
        .as_ref()
        .unwrap()
        .contains("Synthetic spilled output blue"));
    assert!(!result.text.as_ref().unwrap().contains("/outside/secret"));
}
fn write(path: &Path, rows: &[Value]) {
    std::fs::write(
        path,
        rows.iter()
            .map(|r| serde_json::to_string(r).unwrap() + "\n")
            .collect::<String>(),
    )
    .unwrap();
}
#[test]
fn codebuddy_partial_rewrite_retains_then_complete_retires() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "ordinary");
    let store = open(home.path());
    sync(&store);
    let old = read(&store, "ordinary");
    let bytes = std::fs::read(&path).unwrap();
    let cut = bytes.iter().rposition(|b| *b == b'{').unwrap();
    std::fs::write(&path, &bytes[..cut + 1]).unwrap();
    sync(&store);
    assert_eq!(read(&store, "ordinary").messages, old.messages);
    std::fs::write(&path, "bad\n").unwrap();
    assert!(store
        .hydrate(&reference("ordinary"), HydrateOptions::default())
        .is_err());
    assert_eq!(read(&store, "ordinary").messages, old.messages);
    let mut rows: Vec<Value> = String::from_utf8(bytes)
        .unwrap()
        .lines()
        .map(|x| serde_json::from_str(x).unwrap())
        .collect();
    rows.last_mut().unwrap()["content"][0]["text"] =
        Value::String("Synthetic rewritten answer.".into());
    write(&path, &rows);
    sync(&store);
    assert!(texts(&read(&store, "ordinary")).contains(&"Synthetic rewritten answer."));
    assert!(!texts(&read(&store, "ordinary")).contains(&"Found the synthetic CodeBuddy needle."));
    std::fs::remove_file(path).unwrap();
    sync(&store);
    assert!(store
        .session(&reference("ordinary"), SessionQuery::default())
        .unwrap()
        .is_none());
}
#[test]
fn codebuddy_global_index_and_runtime_files_are_not_sessions() {
    let home = tempfile::tempdir().unwrap();
    let root = home.path().join(".codebuddy");
    std::fs::create_dir_all(root.join("sessions")).unwrap();
    std::fs::write(
        root.join("history.jsonl"),
        "{\"display\":\"not a transcript\"}\n",
    )
    .unwrap();
    std::fs::write(root.join("sessions/active.json"), "{}").unwrap();
    let store = open(home.path());
    let r = store
        .discover(DiscoveryOptions::default().sources([Source::Codebuddy]))
        .unwrap();
    assert_eq!(r.discovered, 0);
}
#[test]
fn codebuddy_native_same_millisecond_order_is_stable() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "ordinary");
    let mut rows: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|x| serde_json::from_str(x).unwrap())
        .collect();
    for r in &mut rows {
        r["timestamp"] = Value::from(1791720000000i64);
    }
    write(&path, &rows);
    let e = provider_support::assert_history_conformance(
        home.path(),
        provider_support::HistoryCase {
            source: Source::Codebuddy,
            session_id: &sid("ordinary"),
            messages: &[
                (Role::User, "Find the synthetic CodeBuddy needle."),
                (Role::Assistant, "Found the synthetic CodeBuddy needle."),
            ],
        },
    );
    assert_eq!(
        e.messages.iter().map(|m| m.role).collect::<Vec<_>>(),
        vec![Role::User, Role::Assistant]
    );
}
#[test]
fn codebuddy_partial_success_does_not_block_unrelated_deletion() {
    let home = tempfile::tempdir().unwrap();
    let a = stage(home.path(), "ordinary");
    let b = stage(home.path(), "tool");
    let store = open(home.path());
    sync(&store);
    let mut bytes = std::fs::read(&a).unwrap();
    bytes.extend_from_slice(b"{");
    std::fs::write(a, bytes).unwrap();
    std::fs::remove_file(b).unwrap();
    sync(&store);
    assert_eq!(read(&store, "ordinary").messages.len(), 2);
    assert!(store
        .session(&reference("tool"), SessionQuery::default())
        .unwrap()
        .is_none());
}
#[test]
fn codebuddy_project_namespaces_prevent_same_native_id_collision() {
    let home = tempfile::tempdir().unwrap();
    let a = stage(home.path(), "ordinary");
    let b = a
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("other-project")
        .join(a.file_name().unwrap());
    std::fs::create_dir_all(b.parent().unwrap()).unwrap();
    std::fs::copy(&a, &b).unwrap();
    let store = open(home.path());
    let report = store
        .discover(DiscoveryOptions::default().sources([Source::Codebuddy]))
        .unwrap();
    assert_eq!(report.discovered, 2);
    sync(&store);
    let key = serde_json::to_vec(&(".codebuddy/projects", "other-project")).unwrap();
    let other = format!("project:{:x}:fixture-ordinary", Sha256::digest(key));
    assert!(store
        .session(
            &SessionRef::id(Source::Codebuddy, other),
            SessionQuery::default()
        )
        .unwrap()
        .is_some());
    assert_eq!(read(&store, "ordinary").messages.len(), 2);
}
#[test]
fn codebuddy_unknown_control_and_sidecar_only_fail_without_destroying_history() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "tool");
    let store = open(home.path());
    sync(&store);
    let old = read(&store, "tool");
    let original = std::fs::read_to_string(&path).unwrap();
    std::fs::write(
        &path,
        original.clone() + "{\"type\":\"unknown-lane-move\"}\n",
    )
    .unwrap();
    assert!(store
        .hydrate(&reference("tool"), HydrateOptions::default())
        .is_err());
    assert_eq!(read(&store, "tool").messages, old.messages);
    let mut rows: Vec<Value> = original
        .lines()
        .map(|x| serde_json::from_str(x).unwrap())
        .collect();
    let result = rows
        .iter_mut()
        .find(|r| r["type"] == "function_call_result")
        .unwrap();
    result["providerData"]["toolResult"]["content"] = Value::Null;
    result["output"]["text"] = Value::String(
        "<persisted-output>\nFull output saved to: /outside/secret.txt\n</persisted-output>".into(),
    );
    write(&path, &rows);
    assert!(store
        .hydrate(&reference("tool"), HydrateOptions::default())
        .is_err());
    assert_eq!(read(&store, "tool").messages, old.messages);
}
#[cfg(unix)]
#[test]
fn codebuddy_symlink_cannot_escape_projects_root() {
    let home = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "ordinary");
    let moved = outside.path().join("outside.jsonl");
    std::fs::rename(&path, &moved).unwrap();
    std::os::unix::fs::symlink(&moved, &path).unwrap();
    let store = open(home.path());
    assert!(store
        .hydrate(
            &SessionRef::path(Source::Codebuddy, path),
            HydrateOptions::default()
        )
        .is_err());
}
#[test]
fn codebuddy_title_overlay_updates_without_message_churn() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "metadata");
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Codebuddy]))
        .unwrap();
    assert_eq!(
        read(&store, "metadata").session.title.as_deref(),
        Some("Synthetic chosen title.")
    );
    store
        .hydrate(&reference("metadata"), HydrateOptions::default())
        .unwrap();
    let original = read(&store, "metadata");
    assert_eq!(
        original.session.title.as_deref(),
        Some("Synthetic chosen title.")
    );
    let mut rows: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|x| serde_json::from_str(x).unwrap())
        .collect();
    let meta = rows
        .iter_mut()
        .find(|r| r["type"] == "session-meta")
        .unwrap();
    meta["meta"]["codebuddy.ai/sessionTitle"] = serde_json::json!([{"type":"custom-title","id":"title-new","timestamp":1791720000040i64,"customTitle":"Synthetic updated custom title."}]);
    write(&path, &rows);
    store
        .discover(DiscoveryOptions::default().sources([Source::Codebuddy]))
        .unwrap();
    assert_eq!(
        read(&store, "metadata").session.title,
        original.session.title
    );
    store
        .hydrate(&reference("metadata"), HydrateOptions::default())
        .unwrap();
    let changed = read(&store, "metadata");
    assert_eq!(
        changed.session.title.as_deref(),
        Some("Synthetic updated custom title.")
    );
    assert_eq!(changed.messages, original.messages);
    rows.retain(|r| {
        !matches!(
            r["type"].as_str(),
            Some("custom-title" | "ai-title" | "topic")
        )
    });
    for row in &mut rows {
        if row["type"] == "session-meta" {
            row["meta"]["codebuddy.ai/sessionTitle"] = serde_json::json!([]);
        }
    }
    write(&path, &rows);
    store
        .discover(DiscoveryOptions::default().sources([Source::Codebuddy]))
        .unwrap();
    assert_eq!(
        read(&store, "metadata").session.title,
        changed.session.title
    );
    store
        .hydrate(&reference("metadata"), HydrateOptions::default())
        .unwrap();
    assert_eq!(read(&store, "metadata").session.title, None);
    assert_eq!(read(&store, "metadata").messages, original.messages);
    let revision = store.head_revision().unwrap();
    sync(&store);
    let changes = store
        .changes_since(
            revision,
            ai_hist::ChangeQuery::default().kinds([ai_hist::ChangeKind::SessionEvent]),
        )
        .unwrap()
        .collect::<Vec<_>>();
    assert!(changes.is_empty());
    assert_eq!(read(&store, "metadata").messages, original.messages);
    let settled = store.head_revision().unwrap();
    sync(&store);
    assert_eq!(store.head_revision().unwrap(), settled);
}
#[test]
fn codebuddy_invalid_parent_is_diagnosed_without_retiring_prior_messages() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "ordinary");
    let store = open(home.path());
    sync(&store);
    let old = read(&store, "ordinary");
    let mut rows: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|x| serde_json::from_str(x).unwrap())
        .collect();
    rows.last_mut().unwrap()["parentId"] = Value::from(42);
    write(&path, &rows);
    assert!(store
        .hydrate(&reference("ordinary"), HydrateOptions::default())
        .is_err());
    assert_eq!(read(&store, "ordinary").messages, old.messages);
}
#[test]
fn codebuddy_native_tool_error_and_response_usage_are_preserved() {
    for (case, error) in [("tool", false), ("error", true)] {
        let home = tempfile::tempdir().unwrap();
        stage(home.path(), case);
        let store = open(home.path());
        sync(&store);
        let e = read(&store, case);
        assert_eq!(e.tool_calls.len(), 1);
        assert_eq!(e.tool_calls[0].is_error, Some(error));
        let usages = e
            .messages
            .iter()
            .filter_map(|m| m.usage.as_ref())
            .collect::<Vec<_>>();
        assert_eq!(usages.len(), 2);
        assert_eq!(usages.iter().map(|u| u.input_tokens).sum::<u64>(), 46);
        assert_eq!(usages.iter().map(|u| u.output_tokens).sum::<u64>(), 14);
        assert!(usages.iter().all(|u| !u.coverage.has_cache_read_tokens));
    }
}
#[cfg(feature = "unstable-internal")]
#[test]
fn codebuddy_same_time_public_pages_keep_native_sequence() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "ordinary");
    let mut rows: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|x| serde_json::from_str(x).unwrap())
        .collect();
    for row in &mut rows {
        row["timestamp"] = Value::from(1791720000000i64);
    }
    write(&path, &rows);
    let store = open(home.path());
    sync(&store);
    let conn = ai_hist::open_db(store.db_path()).unwrap();
    let session = sid("ordinary");
    let mut cursor = None;
    let mut roles = Vec::new();
    loop {
        let page =
            ai_hist::session_events_page(&conn, &session, Some("codebuddy"), 1, cursor.as_ref())
                .unwrap();
        for event in page.events {
            assert_eq!(event.ts_ms, 1791720000000);
            roles.push(event.role);
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(roles, vec!["user", "assistant"]);
    let turns = ai_hist::session_user_turns_page(&conn, "codebuddy", &session, 1, None).unwrap();
    assert_eq!(turns.user_turns.len(), 1);
    assert!(turns.user_turns[0].following_message_id.is_some());
    assert!(turns.next_cursor.is_none());
}
#[test]
fn codebuddy_complete_same_time_rewrite_keeps_native_order() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "ordinary");
    let mut rows: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    for row in &mut rows {
        row["timestamp"] = Value::from(1791720000000i64);
    }
    let store = open(home.path());
    write(&path, &rows[1..]);
    sync(&store);
    write(&path, &rows);
    sync(&store);
    assert_eq!(
        read(&store, "ordinary")
            .messages
            .iter()
            .map(|m| m.role)
            .collect::<Vec<_>>(),
        vec![Role::User, Role::Assistant]
    );
    let before = read(&store, "ordinary");
    sync(&store);
    assert_eq!(read(&store, "ordinary"), before);
    let revision = store.head_revision().unwrap();
    sync(&store);
    assert_eq!(store.head_revision().unwrap(), revision);
    #[cfg(feature = "unstable-internal")]
    {
        let conn = ai_hist::open_db(store.db_path()).unwrap();
        let first =
            ai_hist::session_events_page(&conn, &sid("ordinary"), Some("codebuddy"), 1, None)
                .unwrap();
        assert_eq!(first.events[0].role, "user");
        let second = ai_hist::session_events_page(
            &conn,
            &sid("ordinary"),
            Some("codebuddy"),
            1,
            first.next_cursor.as_ref(),
        )
        .unwrap();
        assert_eq!(second.events[0].role, "assistant");
        assert!(second.next_cursor.is_none());
    }
    rows.reverse();
    write(&path, &rows);
    sync(&store);
    assert_eq!(
        read(&store, "ordinary")
            .messages
            .iter()
            .map(|m| m.role)
            .collect::<Vec<_>>(),
        vec![Role::Assistant, Role::User]
    );
}
#[test]
fn codebuddy_review_upstream_response_identity_groups_native_records() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "tool");
    let mut rows: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    for row in &mut rows {
        if row["role"] == "assistant" || row["type"] == "function_call" {
            row["providerData"]["messageId"] = Value::from("native-response-shared");
        }
    }
    write(&path, &rows);
    let store = open(home.path());
    sync(&store);
    let evidence = read(&store, "tool");
    assert_eq!(evidence.requests.len(), 1);
    assert_eq!(
        evidence.requests[0].request_key,
        "provider-message-id:native-response-shared"
    );
    assert!(evidence
        .messages
        .iter()
        .filter(|m| m.role == Role::Assistant)
        .all(|m| m.provider_message_id.as_deref() == Some("native-response-shared")));
    assert_eq!(
        evidence.requests[0].usage.as_ref().unwrap().input_tokens,
        23
    );
}
#[test]
fn codebuddy_filtered_metadata_does_not_consume_later_tool_usage() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "tool");
    let mut rows: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let call_index = rows
        .iter()
        .position(|row| row["type"] == "function_call")
        .unwrap();
    let call = rows[call_index].clone();
    let mut metadata = rows
        .iter()
        .find(|row| row["role"] == "assistant")
        .unwrap()
        .clone();
    metadata["id"] = Value::from("fixture-filtered-meta");
    metadata["timestamp"] = Value::from(call["timestamp"].as_i64().unwrap() - 1);
    metadata["providerData"]["messageId"] = call["providerData"]["messageId"].clone();
    metadata["providerData"]["rawUsage"] = call["providerData"]["rawUsage"].clone();
    metadata["providerData"]["isMeta"] = Value::Bool(true);
    metadata["content"] = Value::from("Private native metadata, not an assistant answer.");
    rows.insert(call_index, metadata);
    write(&path, &rows);

    let store = open(home.path());
    sync(&store);
    let evidence = read(&store, "tool");
    let request_key = format!(
        "provider-message-id:{}",
        call["providerData"]["messageId"].as_str().unwrap()
    );
    let request = evidence
        .requests
        .iter()
        .find(|request| request.request_key == request_key)
        .unwrap();
    assert_eq!(request.usage.as_ref().unwrap().input_tokens, 23);
    assert_eq!(request.usage.as_ref().unwrap().output_tokens, 7);
    assert_eq!(evidence.markers.len(), rows.len());
    assert!(evidence
        .markers
        .iter()
        .filter_map(|m| m.raw_payload())
        .any(|raw| {
            raw.contains("fixture-filtered-meta") && raw.contains("Private native metadata")
        }));
    assert!(!evidence
        .messages
        .iter()
        .any(|message| { message.message_id.as_deref() == Some("fixture-filtered-meta") }));
    assert_eq!(evidence.tool_calls[0].tool_use_id, "fixture-call-001");
    sync(&store);
    let revision = store.head_revision().unwrap();
    sync(&store);
    assert_eq!(store.head_revision().unwrap(), revision);
    assert_eq!(read(&store, "tool"), evidence);
}
#[test]
fn codebuddy_review_complete_unknown_outcome_clears_prior_native_result() {
    for case in ["tool", "error"] {
        let home = tempfile::tempdir().unwrap();
        let path = stage(home.path(), case);
        let store = open(home.path());
        sync(&store);
        let original = read(&store, case);
        assert!(original.tool_calls[0].is_error.is_some());
        let mut rows: Vec<Value> = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        for row in &mut rows {
            if row["type"] == "function_call_result" {
                row["status"] = Value::from("incomplete");
                row["providerData"]["toolResult"]["error"] = Value::Null;
                row["providerData"]["toolResult"]["rawResponse"]["is_error"] = Value::Null;
            }
        }
        write(&path, &rows);
        sync(&store);
        let evidence = read(&store, case);
        assert_eq!(evidence.tool_calls[0].is_error, None);
        assert_eq!(evidence.tool_results[0].result_status, None);
        assert_eq!(evidence.tool_results[0].error_signal, None);
        assert_eq!(evidence.tool_results[0].text, original.tool_results[0].text);
        assert_eq!(
            evidence.tool_results[0].event_uid,
            original.tool_results[0].event_uid
        );
        assert_eq!(evidence.markers.len(), rows.len());
        assert!(evidence
            .markers
            .iter()
            .filter_map(|m| m.raw_payload())
            .any(|raw| raw.contains("incomplete")));
        sync(&store);
        let revision = store.head_revision().unwrap();
        sync(&store);
        assert_eq!(store.head_revision().unwrap(), revision);
        assert_eq!(read(&store, case), evidence);
    }
}
#[test]
fn codebuddy_authoritative_inline_result_preserves_literal_persisted_output_tag() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "tool");
    let mut rows: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|x| serde_json::from_str(x).unwrap())
        .collect();
    let text =
        "<persisted-output>\nA complete synthetic literal tool response.\n</persisted-output>";
    let result = rows
        .iter_mut()
        .find(|r| r["type"] == "function_call_result")
        .unwrap();
    result["providerData"]["toolResult"]["content"] = Value::String(text.into());
    write(&path, &rows);
    let store = open(home.path());
    sync(&store);
    assert!(read(&store, "tool")
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .any(|b| b.kind == BlockKind::ToolResult && b.text.as_deref() == Some(text)));
}
#[test]
fn codebuddy_native_explicit_user_override_wins_over_meta_and_literal_summary_tag() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "string-content");
    let rows: Vec<Value> = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|x| serde_json::from_str(x).unwrap())
        .collect();
    let explicit = rows.iter().find(|r| r["id"] == "u6").unwrap();
    assert_eq!(explicit["providerData"]["startsNewUserRequest"], true);
    assert_eq!(explicit["providerData"]["isMeta"], true);
    let native: Value =
        serde_json::from_str(include_str!("fixtures/codebuddy/official-human.json")).unwrap();
    assert!(native["string-content"]
        .as_array()
        .unwrap()
        .contains(&Value::String("u6".into())));
    let store = open(home.path());
    sync(&store);
    let e = read(&store, "string-content");
    assert!(texts(&e).contains(&"Explicit native real user <cb_summary> literal."));
    assert!(e
        .prompts
        .iter()
        .any(|p| p.prompt.as_deref() == Some("Explicit native real user <cb_summary> literal.")));
    assert!(!texts(&e).contains(&"<cb_summary>Actual synthetic control summary.</cb_summary>"));
}
#[test]
fn codebuddy_incomplete_replacement_keeps_events_tools_prompts_and_markers_atomic() {
    for case in ["tool", "fragments"] {
        let home = tempfile::tempdir().unwrap();
        let path = stage(home.path(), case);
        let store = open(home.path());
        sync(&store);
        let old = read(&store, case);
        let mut rows: Vec<Value> = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(|x| serde_json::from_str(x).unwrap())
            .collect();
        rows[0]["content"] = Value::String("Synthetic replacement prompt.".into());
        if case == "tool" {
            rows.iter_mut()
                .find(|r| r["type"] == "function_call")
                .unwrap()["arguments"] =
                Value::String("{\"file_path\":\"/fixture/replacement.txt\"}".into());
            rows.iter_mut()
                .find(|r| r["type"] == "function_call_result")
                .unwrap()["providerData"]["toolResult"]["content"] =
                Value::String("Synthetic replacement tool result.".into());
        } else {
            let fragment = rows.iter_mut().find(|r| r["id"] == "a3").unwrap();
            fragment["content"] = Value::String("Synthetic replacement first fragment.".into());
        }
        let last = rows.pop().unwrap();
        let mut partial = rows.iter().map(|r| format!("{r}\n")).collect::<String>();
        partial.push_str("{\"type\":\"message\"");
        std::fs::write(&path, partial).unwrap();
        store
            .hydrate(&reference(case), HydrateOptions::default())
            .unwrap();
        let deferred = read(&store, case);
        assert_eq!(deferred.messages, old.messages, "{case} events");
        assert_eq!(deferred.prompts, old.prompts, "{case} prompts");
        assert_eq!(deferred.markers, old.markers, "{case} markers");
        assert_eq!(deferred.tool_calls, old.tool_calls, "{case} tools");
        assert_eq!(deferred.tool_results, old.tool_results, "{case} results");
        rows.push(last);
        write(&path, &rows);
        store
            .hydrate(&reference(case), HydrateOptions::default())
            .unwrap();
        let complete = read(&store, case);
        assert!(texts(&complete).contains(&"Synthetic replacement prompt."));
        assert_ne!(complete.messages, old.messages);
        if case == "tool" {
            assert!(complete
                .messages
                .iter()
                .flat_map(|m| &m.blocks)
                .any(|b| b.text.as_deref() == Some("Synthetic replacement tool result.")));
        }
    }
}
#[test]
fn codebuddy_incomplete_replacement_discovery_preserves_unknown_catalog_fields() {
    for oversized_tail in [false, true] {
        let home = tempfile::tempdir().unwrap();
        let path = stage(home.path(), "ordinary");
        let store = open(home.path());
        sync(&store);
        let old = read(&store, "ordinary");
        assert!(old.session.title.is_none());
        let mut rows: Vec<Value> = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(|x| serde_json::from_str(x).unwrap())
            .collect();
        rows.insert(0,serde_json::json!({"type":"custom-title","id":"replacement-title","timestamp":1791720099999i64,"customTitle":"Synthetic replacement title."}));
        rows[1]["content"] = Value::String("Synthetic replacement prompt.".into());
        let complete = rows.iter().map(|r| format!("{r}\n")).collect::<String>();
        let mut partial = rows[..rows.len() - 1]
            .iter()
            .map(|r| format!("{r}\n"))
            .collect::<String>();
        if oversized_tail {
            partial.push_str("{\"type\":\"message\",\"content\":\"");
            partial.push_str(&"Synthetic oversized pending tail. ".repeat(5000));
        } else {
            partial.push('{');
        }
        std::fs::write(&path, partial).unwrap();
        store
            .discover(DiscoveryOptions::default().sources([Source::Codebuddy]))
            .unwrap();
        let shallow = read(&store, "ordinary");
        assert_eq!(shallow.session.title, old.session.title);
        assert_eq!(shallow.session.first_prompt, old.session.first_prompt);
        assert_eq!(
            shallow.session.last_activity_ms,
            old.session.last_activity_ms
        );
        assert_eq!(shallow.markers, old.markers);
        std::fs::write(&path, complete).unwrap();
        store
            .discover(DiscoveryOptions::default().sources([Source::Codebuddy]))
            .unwrap();
        store
            .hydrate(&reference("ordinary"), HydrateOptions::default())
            .unwrap();
        let updated = read(&store, "ordinary");
        assert_eq!(
            updated.session.title.as_deref(),
            Some("Synthetic replacement title.")
        );
        assert!(texts(&updated).contains(&"Synthetic replacement prompt."));
    }
}

#[cfg(all(feature = "unstable-internal", feature = "export"))]
#[test]
fn codebuddy_actual_tool_export_remote_intake_and_local_rehydrate() {
    use ai_hist::export::{ExportLimits, ExportSelection, ExportSnapshot};
    use ai_hist::{
        EvidenceKind, EvidenceRecord, NormalizedSourceEvidence, ObservationKey, SessionLocation,
        ShallowSession, SourceEvidenceSubmission, SourceObservation, SourceObservationBatch,
    };
    for (case, error_only) in [
        ("tool", false),
        ("error", false),
        ("error", true),
        ("spill", false),
    ] {
        let origin = tempfile::tempdir().unwrap();
        let path = stage(origin.path(), case);
        if error_only {
            let mut rows: Vec<Value> = std::fs::read_to_string(&path)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            for row in &mut rows {
                if row["type"] == "function_call_result" {
                    row["providerData"]["toolResult"]["rawResponse"]["is_error"] = Value::Null;
                }
            }
            write(&path, &rows);
        }
        let captured = std::fs::read(&path).unwrap();
        let source = open(origin.path());
        sync(&source);
        if case == "tool" {
            let native = read(&source, case);
            assert_eq!(native.tool_calls.len(), 1);
            assert!(native.messages.iter().flat_map(|m| &m.blocks).any(
                |b| b.text.as_deref() == Some("   1→Synthetic fixture file color blue.\n   2→")
            ));
        }
        let expected = read(&source, case);
        assert_eq!(
            expected.tool_results[0].error_signal.as_deref(),
            if case == "error" {
                Some(if error_only {
                    "tool_status"
                } else {
                    "tool_result.is_error"
                })
            } else {
                None
            }
        );
        assert_eq!(expected.tool_calls.len(), 1);
        assert_eq!(expected.tool_calls[0].tool_use_id, "fixture-call-001");
        assert_eq!(
            expected.tool_results[0].tool_use_id.as_deref(),
            Some("fixture-call-001")
        );
        assert!(expected
            .messages
            .iter()
            .flat_map(|m| &m.blocks)
            .any(|b| b.kind == BlockKind::ToolResult
                && b.tool_use_id.as_deref() == Some("fixture-call-001")));
        assert!(expected
            .messages
            .iter()
            .flat_map(|m| &m.blocks)
            .any(|b| b.kind == BlockKind::ToolUse
                && b.tool_use_id.as_deref() == Some("fixture-call-001")));
        let kinds = vec![
            EvidenceKind::SessionEvent,
            EvidenceKind::SessionMarker,
            EvidenceKind::ToolCall,
        ];
        let mut snapshot = ExportSnapshot::open(
            rusqlite::Connection::open(origin.path().join("conformance.db")).unwrap(),
            &ExportSelection {
                sources: vec!["codebuddy".into()],
                kinds: kinds.iter().map(|k| k.as_str().into()).collect(),
                ..Default::default()
            },
            &ExportLimits {
                max_batch_records: 1,
                ..Default::default()
            },
            60_000,
            1_000,
        )
        .unwrap();
        let mut cursor = Some(snapshot.handle().cursor);
        let mut records = vec![];
        while let Some(value) = cursor {
            let page = snapshot.page(&value, 1_000).unwrap();
            cursor = page.next_cursor;
            for record in page.records {
                let mut payload = record.payload.as_object().unwrap().clone();
                assert!(!payload.contains_key("raw_facts_version"));
                payload.remove("id");
                payload.remove("location");
                records.push(EvidenceRecord {
                    kind: serde_json::from_value(serde_json::json!(record.kind)).unwrap(),
                    payload,
                    record_id: Some(record.record_id),
                    revision_id: Some(record.revision_id),
                });
            }
        }
        for derived_cache in [false, true] {
            let home = tempfile::tempdir().unwrap();
            let store = open(home.path());
            let key = ObservationKey {
                source: "codebuddy".into(),
                session_id: sid(case),
                location: SessionLocation::Remote,
                connector_id: "fixture-remote".into(),
                connector_instance: "default".into(),
            };
            store
                .apply_source_observations(SourceObservationBatch::new(
                    "fixture-remote",
                    "default",
                    SessionLocation::Remote,
                    vec![SourceObservation {
                        session: ShallowSession {
                            source: "codebuddy".into(),
                            session_id: sid(case),
                            source_stamp: Some("remote-fixture".into()),
                            ..Default::default()
                        },
                        raw_locator: None,
                    }],
                ))
                .unwrap();
            let revision = store.source_observation(&key).unwrap().revision.unwrap();
            store
                .apply_source_evidence(
                    SourceEvidenceSubmission::new(
                        key,
                        revision,
                        NormalizedSourceEvidence {
                            source_stamp: "remote-fixture".into(),
                            source_bytes: 1,
                            covered_kinds: kinds.clone(),
                            records: records.clone(),
                        },
                    )
                    .include_related(false),
                )
                .unwrap();
            let conn = ai_hist::open_db(&home.path().join("conformance.db")).unwrap();
            let unstamped: i64 = conn
                .query_row(
                    "SELECT count(*) FROM session_events WHERE raw_facts_version IS NULL",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert!(
                unstamped > 0,
                "receiver has only real remote intake before hydration"
            );
            if derived_cache {
                conn.execute("UPDATE session_events SET project_key='receiver-derived-cache',project_key_method='path'", []).unwrap();
            }
            let path = stage(home.path(), case);
            std::fs::write(path, &captured).unwrap();
            store
                .discover(DiscoveryOptions::default().sources([Source::Codebuddy]))
                .unwrap();
            store
                .hydrate(
                    &SessionRef::id(Source::Codebuddy, sid(case)),
                    HydrateOptions::default(),
                )
                .unwrap();
            let stamped: i64 = conn
                .query_row(
                    "SELECT count(*) FROM session_events WHERE raw_facts_version IS NOT NULL",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(stamped, unstamped);
            let before = read(&store, case);
            store
                .hydrate(&reference(case), HydrateOptions::default())
                .unwrap();
            assert_eq!(read(&store, case).messages, before.messages);
            let revision = store.head_revision().unwrap();
            store
                .hydrate(&reference(case), HydrateOptions::default())
                .unwrap();
            assert_eq!(store.head_revision().unwrap(), revision);
            assert_eq!(read(&store, case).messages, read(&source, case).messages);
            assert_eq!(
                read(&store, case).tool_calls,
                read(&source, case).tool_calls
            );
            assert_eq!(read(&store, case).tool_results, expected.tool_results);
            assert_eq!(read(&store, case).markers, expected.markers);
        }
    }
}
