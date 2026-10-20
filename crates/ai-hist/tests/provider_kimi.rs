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
