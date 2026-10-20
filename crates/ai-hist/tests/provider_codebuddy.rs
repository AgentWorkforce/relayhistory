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
    store
        .hydrate(&reference("metadata"), HydrateOptions::default())
        .unwrap();
    let changed = read(&store, "metadata");
    assert_eq!(
        changed.session.title.as_deref(),
        Some("Synthetic updated custom title.")
    );
    assert_eq!(changed.messages, original.messages);
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
