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
    let bucket = std::fs::read_dir(home.join(".kimi-code/sessions"))
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
    session.join("agents/main/wire.jsonl")
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
