//! Current Kimi Code2.2.0 native writer and independent official restart oracle.
mod provider_support;
use ai_hist::{
    CatalogQuery, DiscoveryOptions, HydrateOptions, ProviderRoots, Role, SessionQuery, SessionRef,
    SessionStore, Source, StoreOptions, SyncOptions,
};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
fn stage(home: &Path, case: &str) -> PathBuf {
    fn copy(from: &Path, to: &Path) {
        std::fs::create_dir_all(to).unwrap();
        for e in std::fs::read_dir(from).unwrap() {
            let e = e.unwrap();
            let to = to.join(e.file_name());
            if e.path().is_dir() {
                copy(&e.path(), &to)
            } else {
                std::fs::copy(e.path(), to).unwrap();
            }
        }
    }
    copy(
        &Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/kimi")
            .join(case),
        home,
    );
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/kimi")
        .join(case);
    let bucket = std::fs::read_dir(fixture.join(".kimi-code/sessions"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let session = std::fs::read_dir(bucket)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    home.join(session.strip_prefix(&fixture).unwrap())
        .join("agents/main/wire.jsonl")
}
fn sid(n: usize, agent: &str) -> String {
    format!(
        "root:{:x}:wd_fixture_{n:012}:session_00000000-0000-4000-8000-{n:012}:{agent}",
        Sha256::digest(b".kimi-code/sessions")
    )
}
fn open(home: &Path) -> SessionStore {
    SessionStore::open(StoreOptions::default().db_path(home.join("db")).roots(
        ProviderRoots::from_home(home.into(), home.join("opencode.db")),
    ))
    .unwrap()
}
fn evidence(store: &SessionStore, n: usize, agent: &str) -> ai_hist::SessionEvidence {
    store
        .session(
            &SessionRef::id(Source::Kimi, sid(n, agent)),
            SessionQuery::default(),
        )
        .unwrap()
        .unwrap()
}
#[test]
fn kimi_discover_hydrate_search_read_reopen_export_and_filter() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), "ordinary");
    provider_support::assert_history_conformance(
        home.path(),
        provider_support::HistoryCase {
            source: Source::Kimi,
            session_id: &sid(1, "main"),
            messages: &[
                (Role::User, "Remember the synthetic ordinary color blue."),
                (Role::Assistant, "Synthetic Kimi ordinary answer 1: blue."),
            ],
        },
    );
}
#[test]
fn native_tools_join_without_streaming_context_duplicates() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), "tool");
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let e = evidence(&store, 2, "main");
    assert_eq!(e.tool_calls.len(), 1);
    assert!(e
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .any(|b| b.text.as_deref() == Some("1\tSynthetic fixture file: blue.")));
    let texts = e
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .filter_map(|b| b.text.as_deref())
        .collect::<Vec<_>>();
    assert_eq!(
        texts
            .iter()
            .filter(|s| **s == "Remember the synthetic tool color blue.")
            .count(),
        1
    );
    assert!(e.markers.len() > e.messages.len());
}
#[test]
fn empty_native_tool_results_keep_their_call_identity() {
    for content in [
        serde_json::json!([]),
        serde_json::json!([{"type":"text","text":""}]),
    ] {
        let home = tempfile::tempdir().unwrap();
        let wire = stage(home.path(), "tool");
        let rewritten = std::fs::read_to_string(&wire)
            .unwrap()
            .lines()
            .map(|line| {
                let mut row: serde_json::Value = serde_json::from_str(line).unwrap();
                if row["type"] == "agent.message.appended"
                    && row["message"]["message"]["role"] == "tool"
                {
                    row["message"]["message"]["content"] = content.clone();
                }
                row.to_string()
            })
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(wire, format!("{rewritten}\n")).unwrap();
        let store = open(home.path());
        store.sync(SyncOptions::default().force(true)).unwrap();
        let native = evidence(&store, 2, "main");
        assert_eq!(native.tool_calls.len(), 1);
        let call_id = &native.tool_calls[0].tool_use_id;
        let results = native
            .messages
            .iter()
            .flat_map(|m| &m.blocks)
            .filter(|block| {
                block.kind == ai_hist::BlockKind::ToolResult
                    && block.tool_use_id.as_deref() == Some(call_id.as_str())
            })
            .collect::<Vec<_>>();
        assert_eq!(
            results.len(),
            1,
            "empty native result must join its tool call"
        );
        assert_eq!(results[0].text.as_deref(), Some(""));
    }
}
#[test]
fn known_id_resume_and_subagent_wires_remain_separate() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), "resumed");
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert_eq!(evidence(&store, 3, "main").messages.len(), 4);
    let child = tempfile::tempdir().unwrap();
    stage(child.path(), "subagent");
    let store = open(child.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert!(evidence(&store, 4, "agent-0")
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .any(|b| b.text.as_deref() == Some("Synthetic Kimi subagent answer 2: blue.")));
    assert_eq!(store.sessions(CatalogQuery::default()).count(), 2);
}
#[test]
fn partial_tail_rewrite_and_metadata_changes_are_truthful() {
    let home = tempfile::tempdir().unwrap();
    let wire = stage(home.path(), "ordinary");
    let original = std::fs::read_to_string(&wire).unwrap();
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let before = evidence(&store, 1, "main").messages;
    std::fs::write(&wire, format!("{original}{{\"type\":")).unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Kimi, sid(1, "main")),
            HydrateOptions::default(),
        )
        .unwrap();
    assert_eq!(evidence(&store, 1, "main").messages, before);
    std::fs::write(
        &wire,
        original.replace(
            "Synthetic Kimi ordinary answer 1: blue.",
            "Rewritten Kimi answer.",
        ),
    )
    .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Kimi, sid(1, "main")),
            HydrateOptions::default(),
        )
        .unwrap();
    assert!(evidence(&store, 1, "main")
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .any(|b| b.text.as_deref() == Some("Rewritten Kimi answer.")));
    let state = wire
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("state.json");
    let mut value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&state).unwrap()).unwrap();
    value["title"] = "Native renamed title".into();
    std::fs::write(state, serde_json::to_vec(&value).unwrap()).unwrap();
    store
        .discover(DiscoveryOptions::default().sources([Source::Kimi]))
        .unwrap();
    // This native wire exceeds the bounded discovery window. Hydration
    // applies the complete authoritative state after discovery observes it.
    store
        .hydrate(
            &SessionRef::id(Source::Kimi, sid(1, "main")),
            HydrateOptions::default(),
        )
        .unwrap();
    assert_eq!(
        store
            .sessions(CatalogQuery::default())
            .next()
            .unwrap()
            .unwrap()
            .title
            .as_deref(),
        Some("Native renamed title")
    );
}
#[test]
fn unsupported_protocol_cannot_destroy_previous_evidence() {
    let home = tempfile::tempdir().unwrap();
    let wire = stage(home.path(), "ordinary");
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let before = evidence(&store, 1, "main").messages;
    let text = std::fs::read_to_string(&wire).unwrap().replace(
        "\"protocol_version\":\"1.5\"",
        "\"protocol_version\":\"99\"",
    );
    std::fs::write(wire, text).unwrap();
    assert!(store
        .hydrate(
            &SessionRef::id(Source::Kimi, sid(1, "main")),
            HydrateOptions::default()
        )
        .is_err());
    assert_eq!(evidence(&store, 1, "main").messages, before);
}
#[test]
fn root_and_agent_namespaces_prevent_identity_collisions() {
    let home = tempfile::tempdir().unwrap();
    let wire = stage(home.path(), "ordinary");
    let custom = home.path().join("other/sessions");
    let relative = wire
        .strip_prefix(home.path().join(".kimi-code/sessions"))
        .unwrap();
    let other = custom.join(relative);
    std::fs::create_dir_all(other.parent().unwrap()).unwrap();
    std::fs::copy(&wire, &other).unwrap();
    let state = wire
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("state.json");
    let target = other
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("state.json");
    std::fs::copy(state, target).unwrap();
    let mut roots = ProviderRoots::from_home(home.path().into(), home.path().join("opencode.db"));
    let mut value = serde_json::to_value(&roots).unwrap();
    value["provider_roots"]["kimi"] =
        serde_json::json!([home.path().join(".kimi-code/sessions"), custom]);
    roots = serde_json::from_value(value).unwrap();
    let store = SessionStore::open(
        StoreOptions::default()
            .db_path(home.path().join("db"))
            .roots(roots),
    )
    .unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert_eq!(store.sessions(CatalogQuery::default()).count(), 2);
}
#[cfg(unix)]
#[test]
fn symlink_escape_never_reads_outside_roots() {
    use std::os::unix::fs::symlink;
    let home = tempfile::tempdir().unwrap();
    let wire = stage(home.path(), "ordinary");
    let outside = tempfile::tempdir().unwrap();
    let secret = outside.path().join("wire.jsonl");
    std::fs::rename(&wire, &secret).unwrap();
    symlink(secret, wire).unwrap();
    let store = open(home.path());
    assert!(store
        .discover(DiscoveryOptions::default().sources([Source::Kimi]))
        .is_err());
    assert_eq!(store.sessions(CatalogQuery::default()).count(), 0);
}

fn replace_rows(wire: &Path, rows: &[serde_json::Value]) {
    std::fs::write(
        wire,
        rows.iter()
            .map(|r| serde_json::to_string(r).unwrap() + "\n")
            .collect::<String>(),
    )
    .unwrap();
}
fn rows(wire: &Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(wire)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}
#[test]
fn metadata_only_native_session_is_discoverable_without_invented_history() {
    let home = tempfile::tempdir().unwrap();
    let wire = stage(home.path(), "ordinary");
    replace_rows(&wire, &rows(&wire)[..1]);
    let store = open(home.path());
    let report = store
        .discover(DiscoveryOptions::default().sources([Source::Kimi]))
        .unwrap();
    assert!(report.diagnostics.is_empty());
    store
        .hydrate(
            &SessionRef::id(Source::Kimi, sid(1, "main")),
            HydrateOptions::default(),
        )
        .unwrap();
    assert!(evidence(&store, 1, "main").messages.is_empty());
}
#[test]
fn mismatched_state_identity_is_rejected_during_discovery() {
    let home = tempfile::tempdir().unwrap();
    let wire = stage(home.path(), "ordinary");
    let state = wire
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("state.json");
    let mut value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&state).unwrap()).unwrap();
    value["id"] = "wrong-session".into();
    std::fs::write(state, serde_json::to_vec(&value).unwrap()).unwrap();
    let store = open(home.path());
    let report = store
        .discover(DiscoveryOptions::default().sources([Source::Kimi]))
        .unwrap();
    assert!(!report.diagnostics.is_empty());
    assert_eq!(store.sessions(CatalogQuery::default()).count(), 0);
}
#[test]
fn oversized_first_message_remains_discoverable_with_unknown_preview() {
    let home = tempfile::tempdir().unwrap();
    let wire = stage(home.path(), "ordinary");
    let mut records = rows(&wire);
    let mut user = records
        .iter()
        .find(|r| {
            r["type"] == "agent.message.appended" && r["message"]["message"]["role"] == "user"
        })
        .unwrap()
        .clone();
    user["message"]["message"]["content"][0]["text"] = "large fixture ".repeat(20000).into();
    records = vec![records[0].clone(), user];
    replace_rows(&wire, &records);
    let store = open(home.path());
    let report = store
        .discover(DiscoveryOptions::default().sources([Source::Kimi]))
        .unwrap();
    assert!(report.diagnostics.is_empty(), "{:#?}", report.diagnostics);
    let catalog = store
        .sessions(CatalogQuery::default())
        .next()
        .unwrap()
        .unwrap();
    assert!(catalog.first_prompt.is_none());
}
#[test]
fn harness_origin_rewrite_retires_false_human_prompt() {
    let home = tempfile::tempdir().unwrap();
    let wire = stage(home.path(), "ordinary");
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let mut records = rows(&wire);
    for row in &mut records {
        if row["type"] == "agent.message.appended" && row["message"]["message"]["role"] == "user" {
            row["message"]["meta"]["origin"] =
                serde_json::json!({"kind":"skill_activation","trigger":"model-tool"});
        }
    }
    replace_rows(&wire, &records);
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert!(!evidence(&store, 1, "main")
        .messages
        .iter()
        .any(|m| m.role == Role::User));
    assert!(store
        .sessions(CatalogQuery::default())
        .next()
        .unwrap()
        .unwrap()
        .first_prompt
        .is_none());
}
#[test]
fn official_rewind_constructor_removes_retired_messages() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), "rewound");
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let e = evidence(&store, 5, "main");
    assert!(e.messages.is_empty());
    assert!(e.markers.iter().any(|m| m
        .raw_payload()
        .is_some_and(|s| s.contains("agent.switched"))));
}
#[test]
fn native_order_survives_more_than_ten_same_millisecond_messages_and_branch() {
    let home = tempfile::tempdir().unwrap();
    let wire = stage(home.path(), "ordinary");
    let original = rows(&wire);
    let user = original
        .iter()
        .find(|r| {
            r["type"] == "agent.message.appended" && r["message"]["message"]["role"] == "user"
        })
        .unwrap();
    let mut records = vec![original[0].clone()];
    for n in 0..14 {
        let mut record = user.clone();
        record["time"] = 1791720000000i64.into();
        record["message"]["meta"]["userMessageId"] = format!("tie-{n}").into();
        record["message"]["message"]["content"][0]["text"] = format!("Tie message {n}").into();
        records.push(record);
    }
    replace_rows(&wire, &records);
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let expected = (0..14)
        .map(|n| format!("Tie message {n}"))
        .collect::<Vec<_>>();
    let actual = evidence(&store, 1, "main");
    assert_eq!(
        actual
            .messages
            .iter()
            .flat_map(|m| &m.blocks)
            .filter_map(|b| b.text.clone())
            .collect::<Vec<_>>(),
        expected
    );
    #[cfg(feature = "unstable-internal")]
    {
        let conn = ai_hist::open_db(&home.path().join("db")).unwrap();
        let mut cursor = None;
        let mut texts = Vec::new();
        loop {
            let page = ai_hist::session_events_page(
                &conn,
                &sid(1, "main"),
                Some("kimi"),
                1,
                cursor.as_ref(),
            )
            .unwrap();
            texts.extend(page.events.into_iter().filter_map(|e| e.text));
            let Some(next) = page.next_cursor else { break };
            cursor = Some(next);
        }
        assert_eq!(texts, expected);
    }
    records.push(serde_json::json!({"type":"agent.switched","branch":"rewritten","base":{"branch":"main","line":5},"time":1791720000001i64}));
    replace_rows(&wire, &records);
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert_eq!(evidence(&store, 1, "main").messages.len(), 4);
    let watermark = store.head_revision().unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert_eq!(store.head_revision().unwrap(), watermark);
}

#[test]
fn partial_replacement_defers_all_positional_evidence_until_complete() {
    let home = tempfile::tempdir().unwrap();
    let wire = stage(home.path(), "ordinary");
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let before = evidence(&store, 1, "main");
    let replacement = std::fs::read_to_string(&wire)
        .unwrap()
        .replace("blue", "replacement green");
    std::fs::write(&wire, format!("{replacement}{{\"type\":")).unwrap();
    store
        .discover(DiscoveryOptions::default().sources([Source::Kimi]))
        .unwrap();
    assert_eq!(
        store
            .sessions(CatalogQuery::default())
            .next()
            .unwrap()
            .unwrap()
            .first_prompt
            .as_deref(),
        Some("Remember the synthetic ordinary color blue.")
    );
    store
        .hydrate(
            &SessionRef::id(Source::Kimi, sid(1, "main")),
            HydrateOptions::default(),
        )
        .unwrap();
    let partial = evidence(&store, 1, "main");
    assert_eq!(partial.messages, before.messages);
    assert_eq!(partial.markers, before.markers);
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert_eq!(evidence(&store, 1, "main").messages, before.messages);
    std::fs::write(&wire, replacement).unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Kimi, sid(1, "main")),
            HydrateOptions::default(),
        )
        .unwrap();
    assert!(evidence(&store, 1, "main")
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .any(|b| b
            .text
            .as_deref()
            .is_some_and(|s| s.contains("replacement green"))));
}

#[test]
fn orphan_state_does_not_block_other_candidates_or_retire_prior_evidence() {
    let home = tempfile::tempdir().unwrap();
    let wire = stage(home.path(), "ordinary");
    let orphan = stage(home.path(), "tool");
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let previous = evidence(&store, 2, "main").messages;
    std::fs::remove_file(
        orphan
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("state.json"),
    )
    .unwrap();
    let original = std::fs::read_to_string(&wire).unwrap();
    std::fs::write(
        &wire,
        original.replace(
            "Synthetic Kimi ordinary answer 1: blue.",
            "Still imported beside orphan.",
        ),
    )
    .unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert_eq!(evidence(&store, 2, "main").messages, previous);
    assert!(evidence(&store, 1, "main")
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .any(|b| b.text.as_deref() == Some("Still imported beside orphan.")));
    let fresh = SessionStore::open(
        StoreOptions::default()
            .db_path(home.path().join("fresh.db"))
            .roots(ProviderRoots::from_home(
                home.path().into(),
                home.path().join("opencode.db"),
            )),
    )
    .unwrap();
    let report = fresh
        .discover(DiscoveryOptions::default().sources([Source::Kimi]))
        .unwrap();
    assert!(!report.diagnostics.is_empty());
    assert_eq!(fresh.sessions(CatalogQuery::default()).count(), 1);
}

#[test]
fn catalog_preview_is_bounded_without_truncating_message_evidence() {
    let home = tempfile::tempdir().unwrap();
    let wire = stage(home.path(), "ordinary");
    let original = rows(&wire);
    let mut user = original
        .iter()
        .find(|r| {
            r["type"] == "agent.message.appended" && r["message"]["message"]["role"] == "user"
        })
        .unwrap()
        .clone();
    let text = "界".repeat(5000);
    user["message"]["message"]["content"][0]["text"] = text.clone().into();
    replace_rows(&wire, &[original[0].clone(), user]);
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Kimi]))
        .unwrap();
    assert_eq!(
        store
            .sessions(CatalogQuery::default())
            .next()
            .unwrap()
            .unwrap()
            .first_prompt
            .unwrap()
            .chars()
            .count(),
        4096
    );
    store
        .hydrate(
            &SessionRef::id(Source::Kimi, sid(1, "main")),
            HydrateOptions::default(),
        )
        .unwrap();
    assert_eq!(
        store
            .sessions(CatalogQuery::default())
            .next()
            .unwrap()
            .unwrap()
            .first_prompt
            .unwrap()
            .chars()
            .count(),
        4096
    );
    assert!(evidence(&store, 1, "main")
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .any(|b| b.text.as_deref() == Some(&text)));
}

#[test]
fn discovery_refreshes_stamp_but_full_metadata_waits_for_hydration() {
    let home = tempfile::tempdir().unwrap();
    let wire = stage(home.path(), "ordinary");
    let state = wire
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("state.json");
    let mut value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&state).unwrap()).unwrap();
    value["title"] = "Draft".into();
    std::fs::write(&state, serde_json::to_vec(&value).unwrap()).unwrap();
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let catalog = || {
        store
            .sessions(CatalogQuery::default())
            .next()
            .unwrap()
            .unwrap()
    };
    let before = catalog();
    assert_eq!(before.title.as_deref(), Some("Draft"));
    value["title"] = "Final renamed title".into();
    std::fs::write(&state, serde_json::to_vec(&value).unwrap()).unwrap();
    store
        .discover(DiscoveryOptions::default().sources([Source::Kimi]))
        .unwrap();
    assert_ne!(catalog().source_stamp, before.source_stamp);
    assert_eq!(catalog().title.as_deref(), Some("Draft"));
    store
        .hydrate(
            &SessionRef::id(Source::Kimi, sid(1, "main")),
            HydrateOptions::default(),
        )
        .unwrap();
    assert_eq!(catalog().title.as_deref(), Some("Final renamed title"));
    let before = catalog();
    assert!(before.first_prompt.is_some());
    let fixture = tempfile::tempdir().unwrap();
    let rewind = rows(&stage(fixture.path(), "rewound"));
    let mut rewritten = rows(&wire);
    rewritten.extend_from_slice(&rewind[rewind.len() - 3..]);
    replace_rows(&wire, &rewritten);
    store
        .discover(DiscoveryOptions::default().sources([Source::Kimi]))
        .unwrap();
    assert_ne!(catalog().source_stamp, before.source_stamp);
    assert_eq!(catalog().first_prompt, before.first_prompt);
    store
        .hydrate(
            &SessionRef::id(Source::Kimi, sid(1, "main")),
            HydrateOptions::default(),
        )
        .unwrap();
    assert!(catalog().first_prompt.is_none());
    assert!(evidence(&store, 1, "main").messages.is_empty());
    value["title"] = serde_json::Value::Null;
    std::fs::write(&state, serde_json::to_vec(&value).unwrap()).unwrap();
    store
        .discover(DiscoveryOptions::default().sources([Source::Kimi]))
        .unwrap();
    assert_eq!(catalog().title.as_deref(), Some("Final renamed title"));
    store
        .hydrate(
            &SessionRef::id(Source::Kimi, sid(1, "main")),
            HydrateOptions::default(),
        )
        .unwrap();
    assert!(catalog().title.is_none());
}

#[test]
fn first_partial_capture_reports_deferral_and_complete_retry_indexes_evidence() {
    let home = tempfile::tempdir().unwrap();
    let wire = stage(home.path(), "ordinary");
    let complete = std::fs::read_to_string(&wire).unwrap();
    std::fs::write(&wire, format!("{complete}{{\"type\":")).unwrap();
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Kimi]))
        .unwrap();
    let reference = SessionRef::id(Source::Kimi, sid(1, "main"));
    for _ in 0..2 {
        let report = store
            .hydrate(&reference, HydrateOptions::default())
            .unwrap();
        assert!(report
            .diagnostics
            .iter()
            .any(|d| d.code == "KIMI_INCOMPLETE_TRANSCRIPT"));
        assert!(evidence(&store, 1, "main").messages.is_empty());
    }
    std::fs::write(&wire, complete).unwrap();
    let report = store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    assert!(!report
        .diagnostics
        .iter()
        .any(|d| d.code == "KIMI_INCOMPLETE_TRANSCRIPT"));
    assert_eq!(evidence(&store, 1, "main").messages.len(), 2);
}

#[test]
fn bounded_partial_discovery_preserves_evidence_backed_catalog_metadata() {
    let home = tempfile::tempdir().unwrap();
    let wire = stage(home.path(), "ordinary");
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let previous = store
        .sessions(CatalogQuery::default())
        .next()
        .unwrap()
        .unwrap();
    assert!(previous.title.is_none());
    let state = wire
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("state.json");
    let mut value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&state).unwrap()).unwrap();
    value["title"] = "Pending replacement title".into();
    std::fs::write(&state, serde_json::to_vec(&value).unwrap()).unwrap();
    let mut records = rows(&wire);
    records.push(serde_json::json!({"type":"fixture.padding", "time":1791720099999i64,"value":"x".repeat(200000)}));
    records.push(serde_json::json!({"type":"fixture.pending", "time":1791720199999i64}));
    replace_rows(&wire, &records);
    use std::io::Write;
    std::fs::OpenOptions::new()
        .append(true)
        .open(&wire)
        .unwrap()
        .write_all(b"{\"type\":")
        .unwrap();
    store
        .discover(DiscoveryOptions::default().sources([Source::Kimi]))
        .unwrap();
    let observed = store
        .sessions(CatalogQuery::default())
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(observed.title, previous.title);
    assert_eq!(observed.first_prompt, previous.first_prompt);
    assert_eq!(observed.first_activity_ms, previous.first_activity_ms);
    assert_eq!(observed.last_activity_ms, previous.last_activity_ms);
}

fn tied_tool_rewrite(wire: &Path, rewritten: bool) -> Vec<serde_json::Value> {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/kimi/tool");
    let source = std::fs::read_dir(fixture.join(".kimi-code/sessions"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let source = std::fs::read_dir(source)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path()
        .join("agents/main/wire.jsonl");
    let original = rows(&source);
    let mut call = original
        .iter()
        .find(|r| {
            r["type"] == "agent.message.appended"
                && r["message"]["message"]["toolCalls"]
                    .as_array()
                    .is_some_and(|v| !v.is_empty())
        })
        .unwrap()
        .clone();
    let mut hidden = original
        .iter()
        .find(|r| {
            r["type"] == "agent.message.appended" && r["message"]["message"]["role"] == "user"
        })
        .unwrap()
        .clone();
    hidden["message"]["message"]["content"][0]["text"] = "Newly visible middle user.".into();
    hidden["message"]["meta"]["origin"] =
        serde_json::json!({"kind": if rewritten {"user"} else {"system_trigger"}});
    if rewritten {
        call["message"]["message"]["content"] =
            serde_json::json!([{"type":"text","text":"New leading text before retained call."}]);
    }
    let mut later = original
        .iter()
        .rfind(|r| {
            r["type"] == "agent.message.appended" && r["message"]["message"]["role"] == "assistant"
        })
        .unwrap()
        .clone();
    later["message"]["message"]["content"] =
        serde_json::json!([{"type":"text","text":"Later retained same-clock answer."}]);
    for row in [&mut call, &mut hidden, &mut later] {
        row["time"] = 1791720000185i64.into();
    }
    let result = vec![original[0].clone(), call, hidden, later];
    replace_rows(wire, &result);
    result
}

#[test]
fn complete_rewrite_preserves_same_clock_native_order_like_fresh_import() {
    let home = tempfile::tempdir().unwrap();
    let wire = stage(home.path(), "tool");
    tied_tool_rewrite(&wire, false);
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let rewritten = tied_tool_rewrite(&wire, true);
    store
        .hydrate(
            &SessionRef::id(Source::Kimi, sid(2, "main")),
            HydrateOptions::default(),
        )
        .unwrap();
    let fresh_home = tempfile::tempdir().unwrap();
    let fresh_wire = stage(fresh_home.path(), "tool");
    replace_rows(&fresh_wire, &rewritten);
    let fresh = open(fresh_home.path());
    fresh.sync(SyncOptions::default().force(true)).unwrap();
    assert_eq!(
        evidence(&store, 2, "main").messages,
        evidence(&fresh, 2, "main").messages
    );
    #[cfg(feature = "unstable-internal")]
    {
        fn events(home: &Path, limit: i64) -> Vec<String> {
            let conn = ai_hist::open_db(&home.join("db")).unwrap();
            let mut cursor = None;
            let mut result = Vec::new();
            loop {
                let page = ai_hist::session_events_page(
                    &conn,
                    &sid(2, "main"),
                    Some("kimi"),
                    limit,
                    cursor.as_ref(),
                )
                .unwrap();
                result.extend(
                    page.events
                        .into_iter()
                        .map(|e| format!("{}:{}", e.kind, e.text.unwrap_or_default())),
                );
                let Some(next) = page.next_cursor else { break };
                cursor = Some(next);
            }
            result
        }
        let expected = events(fresh_home.path(), 100);
        assert_eq!(events(home.path(), 100), expected);
        assert_eq!(events(home.path(), 1), expected);
    }
    #[cfg(feature = "unstable-internal")]
    let event_ids = || {
        let conn = ai_hist::open_db(&home.path().join("db")).unwrap();
        let mut statement = conn.prepare("SELECT id,event_uid FROM session_events WHERE source='kimi' AND session_id=?1 ORDER BY id").unwrap();
        let result = statement
            .query_map([sid(2, "main")], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        result
    };
    #[cfg(feature = "unstable-internal")]
    let before_transition = event_ids();
    // Sweep metadata uses its own parser/stamp; event IDs must remain stable.
    store.sync(SyncOptions::default().force(true)).unwrap();
    #[cfg(feature = "unstable-internal")]
    assert_eq!(event_ids(), before_transition);

    let revision = store.head_revision().unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert_eq!(store.head_revision().unwrap(), revision);
}

#[cfg(feature = "unstable-internal")]
#[test]
fn shared_same_clock_ordering_conflict_rolls_back_entire_replacement() {
    let home = tempfile::tempdir().unwrap();
    let wire = stage(home.path(), "tool");
    tied_tool_rewrite(&wire, false);
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Kimi, sid(2, "main")),
            HydrateOptions::default(),
        )
        .unwrap();
    let conn = ai_hist::open_db(&home.path().join("db")).unwrap();
    conn.execute(
        "UPDATE session_events SET location='both' WHERE source='kimi' AND session_id=?1",
        [sid(2, "main")],
    )
    .unwrap();
    let before = evidence(&store, 2, "main");
    let checkpoint: String=conn.query_row("SELECT source_stamp FROM session_hydration_checkpoints WHERE source='kimi' AND session_id=?1",[sid(2,"main")],|r|r.get(0)).unwrap();
    tied_tool_rewrite(&wire, true);
    assert!(store
        .hydrate(
            &SessionRef::id(Source::Kimi, sid(2, "main")),
            HydrateOptions::default()
        )
        .is_err());
    let after = evidence(&store, 2, "main");
    assert_eq!(after.messages, before.messages);
    assert_eq!(after.markers, before.markers);
    assert_eq!(after.tool_calls, before.tool_calls);
    assert_eq!(after.prompts, before.prompts);
    let retained: String=conn.query_row("SELECT source_stamp FROM session_hydration_checkpoints WHERE source='kimi' AND session_id=?1",[sid(2,"main")],|r|r.get(0)).unwrap();
    assert_eq!(retained, checkpoint);
}

#[cfg(feature = "unstable-internal")]
#[test]
fn remote_backed_stable_position_rewrites_roll_back_every_evidence_kind() {
    for ownership in ["remote", "both"] {
        for change in [
            "text",
            "timestamp",
            "native-id",
            "nullable",
            "marker",
            "tool-only",
        ] {
            let home = tempfile::tempdir().unwrap();
            let wire = stage(home.path(), "tool");
            let store = open(home.path());
            let reference = SessionRef::id(Source::Kimi, sid(2, "main"));
            store
                .discover(DiscoveryOptions::default().sources([Source::Kimi]))
                .unwrap();
            store
                .hydrate(&reference, HydrateOptions::default())
                .unwrap();
            let conn = ai_hist::open_db(&home.path().join("db")).unwrap();
            let (table, key) = match change {
                "marker" => ("session_markers", "marker_uid='wire:0'"),
                "tool-only" => ("tool_calls", "tool_use_id='fixture-tool-001'"),
                _ => ("session_events", "event_uid='message:0:text'"),
            };
            conn.execute(&format!("UPDATE {table} SET location=?1 WHERE source='kimi' AND session_id=?2 AND {key}"),rusqlite::params![ownership,sid(2,"main")]).unwrap();
            if change == "nullable" {
                conn.execute("UPDATE session_events SET model='Remote recorded model' WHERE source='kimi' AND session_id=?1 AND event_uid='message:0:text'",[sid(2,"main")]).unwrap();
            }
            let before = evidence(&store, 2, "main");
            let revision = store.head_revision().unwrap();
            let checkpoint:String=conn.query_row("SELECT source_stamp FROM session_hydration_checkpoints WHERE source='kimi' AND session_id=?1",[sid(2,"main")],|r|r.get(0)).unwrap();
            let mut records = rows(&wire);
            let user = records
                .iter_mut()
                .find(|r| {
                    r["type"] == "agent.message.appended"
                        && r["message"]["message"]["role"] == "user"
                })
                .unwrap();
            match change {
                "text" => {
                    user["message"]["message"]["content"][0]["text"] =
                        "Reassigned complete user text.".into()
                }
                "timestamp" => user["time"] = (user["time"].as_i64().unwrap() + 1).into(),
                "native-id" => {
                    user["message"]["meta"]["userMessageId"] = "different-native-user-id".into()
                }
                "nullable" => {}
                "marker" => {
                    records[0]["fixtureChangedMetadata"] = "different marker payload".into()
                }
                "tool-only" => {
                    let call = records
                        .iter_mut()
                        .find(|r| {
                            r["type"] == "agent.message.appended"
                                && r["message"]["message"]["toolCalls"]
                                    .as_array()
                                    .is_some_and(|v| !v.is_empty())
                        })
                        .unwrap();
                    call["message"]["message"]["toolCalls"][0]["arguments"] =
                        "{\"path\":\"different-fixture.txt\"}".into();
                }
                _ => unreachable!(),
            }
            replace_rows(&wire, &records);
            // A raw-byte-only change also exercises nullable-field preservation.
            use std::io::Write;
            std::fs::OpenOptions::new()
                .append(true)
                .open(&wire)
                .unwrap()
                .write_all(b"\n")
                .unwrap();
            let error = store
                .hydrate(&reference, HydrateOptions::default())
                .unwrap_err();
            assert!(
                error.to_string().contains("KIMI_SHARED_EVIDENCE_CONFLICT"),
                "{ownership} {change}: {error}"
            );
            let after = evidence(&store, 2, "main");
            assert_eq!(
                after.session, before.session,
                "{ownership} {change} catalog"
            );
            assert_eq!(after.usage, before.usage, "{ownership} {change} usage");
            assert_eq!(
                store.head_revision().unwrap(),
                revision,
                "{ownership} {change} revision"
            );
            assert_eq!(
                after.messages, before.messages,
                "{ownership} {change} events"
            );
            assert_eq!(
                after.markers, before.markers,
                "{ownership} {change} markers"
            );
            assert_eq!(
                after.tool_calls, before.tool_calls,
                "{ownership} {change} tools"
            );
            assert_eq!(
                after.tool_results, before.tool_results,
                "{ownership} {change} results"
            );
            assert_eq!(
                after.prompts, before.prompts,
                "{ownership} {change} prompts"
            );
            let retained:String=conn.query_row("SELECT source_stamp FROM session_hydration_checkpoints WHERE source='kimi' AND session_id=?1",[sid(2,"main")],|r|r.get(0)).unwrap();
            assert_eq!(retained, checkpoint);
            let retained:String=conn.query_row(&format!("SELECT location FROM {table} WHERE source='kimi' AND session_id=?1 AND {key}"),[sid(2,"main")],|r|r.get(0)).unwrap();
            assert_eq!(retained, ownership);
        }
    }
}

#[cfg(feature = "unstable-internal")]
#[test]
fn identical_remote_backed_imports_can_add_local_shares() {
    for ownership in ["remote", "both"] {
        let home = tempfile::tempdir().unwrap();
        let wire = stage(home.path(), "tool");
        let store = open(home.path());
        let reference = SessionRef::id(Source::Kimi, sid(2, "main"));
        store
            .discover(DiscoveryOptions::default().sources([Source::Kimi]))
            .unwrap();
        store
            .hydrate(&reference, HydrateOptions::default())
            .unwrap();
        let conn = ai_hist::open_db(&home.path().join("db")).unwrap();
        for table in ["session_events", "session_markers", "tool_calls"] {
            conn.execute(
                &format!("UPDATE {table} SET location=?1 WHERE source='kimi' AND session_id=?2"),
                rusqlite::params![ownership, sid(2, "main")],
            )
            .unwrap();
        }
        let before = evidence(&store, 2, "main");
        use std::io::Write;
        std::fs::OpenOptions::new()
            .append(true)
            .open(&wire)
            .unwrap()
            .write_all(b"\n")
            .unwrap();
        store
            .hydrate(&reference, HydrateOptions::default())
            .unwrap();
        let after = evidence(&store, 2, "main");
        assert_eq!(after.messages, before.messages);
        assert_eq!(after.markers, before.markers);
        assert_eq!(after.tool_calls, before.tool_calls);
        for table in ["session_events", "session_markers", "tool_calls"] {
            let count:i64=conn.query_row(&format!("SELECT count(*) FROM {table} WHERE source='kimi' AND session_id=?1 AND location!='both'"),[sid(2,"main")],|r|r.get(0)).unwrap();
            assert_eq!(count, 0, "{ownership} {table}");
        }
    }
}

#[cfg(feature = "unstable-internal")]
fn shallow_state_discovery(
    home: &Path,
    limit: Option<usize>,
) -> (Vec<ai_hist::ShallowSession>, ai_hist::DiscoverySummary) {
    let conn = ai_hist::open_db(&home.join("db")).unwrap();
    let env =
        ai_hist::DiscoveryEnv::with_roots(&conn, home.to_owned(), home.join("missing-opencode.db"));
    let mut rows = Vec::new();
    let summary = ai_hist::discover_sessions_with_env(
        &env,
        &ai_hist::DiscoverOptions::new(ai_hist::SessionScope::Local, vec!["kimi".into()], limit),
        |row| rows.push(row.clone()),
    )
    .unwrap();
    (rows, summary)
}
#[cfg(feature = "unstable-internal")]
fn state_path(wire: &Path) -> PathBuf {
    wire.parent()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("state.json")
}
#[cfg(feature = "unstable-internal")]
fn compact_wire(wire: &Path) {
    let records = rows(wire)
        .into_iter()
        .filter(|r| r["type"] == "metadata" || r["type"] == "agent.message.appended")
        .collect::<Vec<_>>();
    replace_rows(wire, &records);
}
#[cfg(feature = "unstable-internal")]
#[test]
fn shallow_state_counts_both_files_and_exact_state_boundary_bytes() {
    for padded in [false, true] {
        let home = tempfile::tempdir().unwrap();
        let wire = stage(home.path(), "ordinary");
        compact_wire(&wire);
        let state = state_path(&wire);
        if padded {
            let mut bytes = std::fs::read(&state).unwrap();
            bytes.resize(64 * 1024, b' ');
            std::fs::write(&state, bytes).unwrap();
        }
        let expected =
            std::fs::metadata(&state).unwrap().len() + std::fs::metadata(&wire).unwrap().len();
        let (catalog, summary) = shallow_state_discovery(home.path(), Some(1));
        assert_eq!(catalog.len(), 1);
        assert!(summary.diagnostics.is_empty());
        assert_eq!(summary.counters.files_opened, 2);
        assert_eq!(summary.counters.bytes_read, expected);
    }
}
#[cfg(feature = "unstable-internal")]
#[test]
fn shallow_state_oversize_is_bounded_and_does_not_block_healthy_neighbor_or_full_capture() {
    let home = tempfile::tempdir().unwrap();
    let bad = stage(home.path(), "ordinary");
    let good = stage(home.path(), "tool");
    compact_wire(&bad);
    compact_wire(&good);
    let state = state_path(&bad);
    let mut value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&state).unwrap()).unwrap();
    value["fixtureOversizedState"] = "x".repeat(512 * 1024).into();
    std::fs::write(&state, serde_json::to_vec(&value).unwrap()).unwrap();
    // Read the bad candidate first so a global row limit must advance past it.
    std::fs::File::open(&bad)
        .unwrap()
        .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(2))
        .unwrap();
    std::fs::File::open(&good)
        .unwrap()
        .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(1))
        .unwrap();
    let (catalog, summary) = shallow_state_discovery(home.path(), Some(1));
    assert_eq!(
        catalog
            .iter()
            .map(|s| s.session_id.as_str())
            .collect::<Vec<_>>(),
        [sid(2, "main")]
    );
    assert!(summary
        .diagnostics
        .iter()
        .any(|d| d.error.contains("KIMI_STATE_TOO_LARGE")));
    assert_eq!(summary.counters.files_opened, 3);
    let healthy_bytes = std::fs::metadata(state_path(&good)).unwrap().len()
        + std::fs::metadata(&good).unwrap().len();
    assert_eq!(summary.counters.bytes_read, 64 * 1024 + 1 + healthy_bytes);
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert_eq!(evidence(&store, 1, "main").messages.len(), 2);
    assert_eq!(store.sessions(CatalogQuery::default()).count(), 2);
}
#[cfg(feature = "unstable-internal")]
#[test]
fn shallow_state_parse_failure_still_accounts_for_open_and_bytes() {
    let home = tempfile::tempdir().unwrap();
    let wire = stage(home.path(), "ordinary");
    std::fs::write(state_path(&wire), b"{").unwrap();
    let (catalog, summary) = shallow_state_discovery(home.path(), Some(1));
    assert!(catalog.is_empty());
    assert!(!summary.diagnostics.is_empty());
    assert_eq!(summary.counters.files_opened, 1);
    assert_eq!(summary.counters.bytes_read, 1);
}

#[cfg(all(unix, feature = "unstable-internal"))]
#[test]
fn shallow_state_symlink_is_rejected_before_content_open_with_healthy_neighbor() {
    let home = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let bad = stage(home.path(), "ordinary");
    let good = stage(home.path(), "tool");
    compact_wire(&good);
    let state = state_path(&bad);
    let external = outside.path().join("state.json");
    std::fs::rename(&state, &external).unwrap();
    std::os::unix::fs::symlink(&external, &state).unwrap();
    std::fs::File::open(&bad)
        .unwrap()
        .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(2))
        .unwrap();
    std::fs::File::open(&good)
        .unwrap()
        .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(1))
        .unwrap();
    let (rows, summary) = shallow_state_discovery(home.path(), Some(1));
    assert_eq!(
        rows.iter()
            .map(|s| s.session_id.as_str())
            .collect::<Vec<_>>(),
        [sid(2, "main")]
    );
    assert!(!summary.diagnostics.is_empty());
    assert_eq!(summary.counters.files_opened, 2);
    assert_eq!(
        summary.counters.bytes_read,
        std::fs::metadata(state_path(&good)).unwrap().len()
            + std::fs::metadata(&good).unwrap().len()
    );
}

#[cfg(all(feature = "unstable-internal", feature = "export"))]
#[test]
fn actual_remote_export_roundtrip_allows_local_parser_and_project_enrichment() {
    use ai_hist::export::{ExportLimits, ExportSelection, ExportSnapshot};
    use ai_hist::{
        EvidenceKind, EvidenceRecord, NormalizedSourceEvidence, ObservationKey, SessionLocation,
        ShallowSession, SourceEvidenceSubmission, SourceObservation, SourceObservationBatch,
    };
    for (case, n) in [("ordinary", 1), ("tool", 2)] {
        let origin = tempfile::tempdir().unwrap();
        stage(origin.path(), case);
        let source = open(origin.path());
        source.sync(SyncOptions::default().force(true)).unwrap();
        if case == "tool" {
            let native = evidence(&source, n, "main");
            assert_eq!(native.tool_calls.len(), 1);
            assert_eq!(native.tool_calls[0].tool_use_id, "fixture-tool-001");
            for kind in [ai_hist::BlockKind::ToolUse, ai_hist::BlockKind::ToolResult] {
                let joined = native
                    .messages
                    .iter()
                    .flat_map(|m| &m.blocks)
                    .filter(|block| {
                        block.kind == kind
                            && block.tool_use_id.as_deref()
                                == Some(native.tool_calls[0].tool_use_id.as_str())
                    })
                    .count();
                assert_eq!(joined, 1, "{kind:?} joins the exact native tool call");
            }
            assert!(native
                .messages
                .iter()
                .flat_map(|m| &m.blocks)
                .any(|b| b.text.as_deref() == Some("1\tSynthetic fixture file: blue.")));
        }
        let kinds = vec![
            EvidenceKind::SessionEvent,
            EvidenceKind::SessionMarker,
            EvidenceKind::ToolCall,
        ];
        let mut snapshot = ExportSnapshot::open(
            rusqlite::Connection::open(origin.path().join("db")).unwrap(),
            &ExportSelection {
                sources: vec!["kimi".into()],
                kinds: kinds.iter().map(|k| k.as_str().into()).collect(),
                ..Default::default()
            },
            &ExportLimits::default(),
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
                source: "kimi".into(),
                session_id: sid(n, "main"),
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
                            source: "kimi".into(),
                            session_id: sid(n, "main"),
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
            let conn = ai_hist::open_db(&home.path().join("db")).unwrap();
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
            stage(home.path(), case);
            store
                .discover(DiscoveryOptions::default().sources([Source::Kimi]))
                .unwrap();
            store
                .hydrate(
                    &SessionRef::id(Source::Kimi, sid(n, "main")),
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
            assert_eq!(
                evidence(&store, n, "main").messages,
                evidence(&source, n, "main").messages
            );
            assert_eq!(
                evidence(&store, n, "main").tool_calls,
                evidence(&source, n, "main").tool_calls
            );
        }
    }
}
