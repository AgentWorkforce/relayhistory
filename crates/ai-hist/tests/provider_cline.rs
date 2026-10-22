//! Public Cline core v1 history acceptance from native writer artifacts.
mod provider_support;
#[cfg(feature = "export")]
use ai_hist::export::{ExportLimits, ExportSelection, ExportSnapshot, SessionIdentity};
use ai_hist::{
    DiscoveryOptions, HydrateOptions, ProviderRoots, Role, SessionQuery, SessionRef, SessionStore,
    Source, StoreOptions, SyncOptions,
};
#[cfg(feature = "unstable-internal")]
use ai_hist::{
    EvidenceKind, EvidenceRecord, NormalizedSourceEvidence, ObservationKey, SessionLocation,
    ShallowSession, SourceEvidenceSubmission, SourceObservation, SourceObservationBatch,
};
use std::path::{Path, PathBuf};
const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/cline/native/.cline/data/sessions"
);
fn stage(home: &Path, name: &str) -> PathBuf {
    let source = Path::new(FIXTURE)
        .join(name)
        .join(format!("{name}.messages.json"));
    let dest = home
        .join(".cline/data/sessions")
        .join(name)
        .join(format!("{name}.messages.json"));
    std::fs::create_dir_all(dest.parent().unwrap()).unwrap();
    std::fs::copy(source, &dest).unwrap();
    dest
}
fn open(home: &Path) -> SessionStore {
    SessionStore::open(
        StoreOptions::default()
            .db_path(home.join("cline.db"))
            .roots(ProviderRoots::from_home(
                home.into(),
                home.join("opencode.db"),
            )),
    )
    .unwrap()
}
fn read(store: &SessionStore, sid: &str) -> ai_hist::SessionEvidence {
    store
        .session(&SessionRef::id(Source::Cline, sid), SessionQuery::default())
        .unwrap()
        .unwrap()
}
#[test]
fn native_tool_history_reaches_public_facade_and_reader_oracle() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), "fixture-success-01");
    let evidence = provider_support::assert_history_conformance(
        home.path(),
        provider_support::HistoryCase {
            source: Source::Cline,
            session_id: "fixture-success-01",
            messages: &[
                (Role::User, "Inspect the README and summarize it."),
                (
                    Role::Assistant,
                    "The README describes a small test fixture project.",
                ),
            ],
        },
    );
    let ids: Vec<_> = evidence
        .messages
        .iter()
        .filter_map(|m| m.message_id.as_deref())
        .collect();
    assert_eq!(
        ids,
        [
            "msg_user_1",
            "msg_assistant_1",
            "msg_user_2",
            "msg_assistant_2"
        ],
        "native message order"
    );
    assert_eq!(evidence.tool_calls.len(), 1);
    assert_eq!(evidence.tool_results.len(), 1);
    assert_eq!(evidence.tool_calls[0].tool_use_id, "tool-call-1");
    assert_eq!(
        evidence.tool_results[0].tool_use_id.as_deref(),
        Some("tool-call-1")
    );
    assert!(evidence
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .any(|b| b.kind == ai_hist::BlockKind::Thinking));
    assert!(
        evidence.messages.iter().all(|m| m.usage.is_none()),
        "aggregate fallback makes normalized usage unavailable"
    );
    let oracle: serde_json::Value = serde_json::from_str(include_str!(
        "../../../plans/history-providers/fixtures/cline/restart-reader.json"
    ))
    .unwrap();
    let native: Vec<_> = oracle["synthetic-ordinary.messages.json"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["id"].as_str().unwrap())
        .collect();
    assert_eq!(
        native,
        ["ordinary-u1", "ordinary-a1"],
        "independent restart reader preserved writer IDs"
    );
}
#[test]
fn native_resume_snapshot_rewrite_and_partial_last_good() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "synthetic-resume");
    let store = open(home.path());
    let report = store
        .discover(DiscoveryOptions::default().sources([Source::Cline]))
        .unwrap();
    assert_eq!(report.discovered, 1);
    let reference = SessionRef::id(Source::Cline, "synthetic-resume");
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    assert_eq!(read(&store, "synthetic-resume").prompts.len(), 2);
    let original = std::fs::read(&path).unwrap();
    std::fs::write(&path, &original[..original.len() / 2]).unwrap();
    assert!(store
        .hydrate(&reference, HydrateOptions::default())
        .is_err());
    assert_eq!(
        read(&store, "synthetic-resume").prompts.len(),
        2,
        "partial writer snapshot retains prior good evidence"
    );
    std::fs::write(&path, &original).unwrap();
    let mut snapshot: serde_json::Value = serde_json::from_slice(&original).unwrap();
    snapshot["messages"].as_array_mut().unwrap().truncate(2);
    std::fs::write(&path, serde_json::to_vec(&snapshot).unwrap()).unwrap();
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    assert_eq!(
        read(&store, "synthetic-resume").prompts.len(),
        1,
        "complete rewrite retires removed turn"
    );
    drop(store);
    let reopened = open(home.path());
    assert_eq!(read(&reopened, "synthetic-resume").prompts.len(), 1);
}

#[test]
fn empty_envelope_and_partial_metrics_do_not_hide_sibling_history() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "synthetic-resume");
    let mut snapshot: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    snapshot["messages"].as_array_mut().unwrap().insert(
        1,
        serde_json::json!({
            "id":"empty-native-envelope", "role":"user", "content":[]
        }),
    );
    snapshot["messages"][2]["metrics"] = serde_json::json!({"outputTokens":2});
    std::fs::write(&path, serde_json::to_vec(&snapshot).unwrap()).unwrap();
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let evidence = read(&store, "synthetic-resume");
    assert_eq!(evidence.prompts.len(), 2);
    assert_eq!(evidence.messages.len(), 4);
    assert!(evidence
        .messages
        .iter()
        .all(|message| message.usage.is_none()));
}
#[test]
fn malformed_sibling_does_not_hide_healthy_session() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), "synthetic-ordinary");
    let bad = home
        .path()
        .join(".cline/data/sessions/bad/bad.messages.json");
    std::fs::create_dir_all(bad.parent().unwrap()).unwrap();
    std::fs::write(&bad, b"{broken").unwrap();
    let store = open(home.path());
    let report = store
        .discover(DiscoveryOptions::default().sources([Source::Cline]))
        .unwrap();
    assert_eq!(report.discovered, 1);
    assert!(report
        .diagnostics
        .iter()
        .any(|d| d.message.contains("CLINE_INCOMPLETE_SNAPSHOT")));
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert_eq!(read(&store, "synthetic-ordinary").prompts.len(), 1);
}

#[test]
fn discovery_caps_native_snapshot_read_at_32_mib() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), "synthetic-ordinary");
    let oversized = home
        .path()
        .join(".cline/data/sessions/oversized/oversized.messages.json");
    std::fs::create_dir_all(oversized.parent().unwrap()).unwrap();
    std::fs::File::create(&oversized)
        .unwrap()
        .set_len(32 * 1024 * 1024 + 1)
        .unwrap();
    let store = open(home.path());
    let report = store
        .discover(DiscoveryOptions::default().sources([Source::Cline]))
        .unwrap();
    assert_eq!(report.discovered, 1);
    assert!(report
        .diagnostics
        .iter()
        .any(|d| d.message.contains("CLINE_SNAPSHOT_TOO_LARGE")));
    store
        .hydrate(
            &SessionRef::id(Source::Cline, "synthetic-ordinary"),
            HydrateOptions::default(),
        )
        .unwrap();
    assert_eq!(read(&store, "synthetic-ordinary").prompts.len(), 1);
}
#[test]
fn same_timestamp_reorder_replays_events_without_regressing_tool_status() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "fixture-success-01");
    let mut snapshot: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    snapshot["messages"][2]["content"][0]["is_error"] = true.into();
    std::fs::write(&path, serde_json::to_vec(&snapshot).unwrap()).unwrap();
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Cline]))
        .unwrap();
    let reference = SessionRef::id(Source::Cline, "fixture-success-01");
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    assert_eq!(
        read(&store, "fixture-success-01").tool_calls[0].is_error,
        Some(true)
    );
    let inserted = serde_json::json!({"id":"inserted-u","role":"user","content":[{"type":"text","text":"Check the error."}]});
    snapshot["messages"]
        .as_array_mut()
        .unwrap()
        .insert(2, inserted);
    std::fs::write(&path, serde_json::to_vec(&snapshot).unwrap()).unwrap();
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    let evidence = read(&store, "fixture-success-01");
    assert_eq!(
        evidence.tool_calls[0].is_error,
        Some(true),
        "event reorder must not reset settled call status"
    );
    assert_eq!(
        evidence.tool_results[0].result_status.as_deref(),
        Some("errored")
    );
    let ids: Vec<_> = evidence
        .messages
        .iter()
        .filter_map(|m| m.message_id.as_deref())
        .collect();
    assert_eq!(
        ids,
        [
            "msg_user_1",
            "msg_assistant_1",
            "inserted-u",
            "msg_user_2",
            "msg_assistant_2"
        ]
    );
}

#[test]
fn rewrite_without_optional_result_status_clears_stale_local_call_status() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "fixture-success-01");
    let mut snapshot: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    snapshot["messages"][2]["content"][0]["is_error"] = true.into();
    std::fs::write(&path, serde_json::to_vec(&snapshot).unwrap()).unwrap();
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert_eq!(
        read(&store, "fixture-success-01").tool_calls[0].is_error,
        Some(true)
    );
    snapshot["messages"][2]["content"][0]
        .as_object_mut()
        .unwrap()
        .remove("is_error");
    std::fs::write(&path, serde_json::to_vec(&snapshot).unwrap()).unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    let rewritten = read(&store, "fixture-success-01");
    assert_eq!(rewritten.tool_results[0].result_status, None);
    assert_eq!(rewritten.tool_calls[0].is_error, None);
}

#[test]
fn malformed_sibling_does_not_block_confirmed_deleted_session_retirement() {
    let home = tempfile::tempdir().unwrap();
    let removed = stage(home.path(), "synthetic-ordinary");
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert!(read(&store, "synthetic-ordinary").prompts.len() == 1);
    std::fs::remove_file(removed).unwrap();
    let bad = home
        .path()
        .join(".cline/data/sessions/bad/bad.messages.json");
    std::fs::create_dir_all(bad.parent().unwrap()).unwrap();
    std::fs::write(&bad, b"{broken").unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert!(store
        .session(
            &SessionRef::id(Source::Cline, "synthetic-ordinary"),
            SessionQuery::default()
        )
        .unwrap()
        .is_none());
}

#[test]
fn small_mixed_tool_result_preserves_native_nontext_blocks() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "fixture-success-01");
    let mut snapshot: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let content = serde_json::json!([
        {"type":"text","text":"Decoded result line one\nline two"},
        {"type":"resource","uri":"synthetic://cline/readme"},
        {"type":"image","source":{"type":"base64","media_type":"image/png","data":"synthetic"}}
    ]);
    snapshot["messages"][2]["content"][0]["content"] = content.clone();
    std::fs::write(&path, serde_json::to_vec(&snapshot).unwrap()).unwrap();
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let evidence = read(&store, "fixture-success-01");
    let text = evidence.tool_results[0].text.as_deref().unwrap();
    assert!(text.starts_with("Decoded result line one\nline two"));
    let projection: serde_json::Value = serde_json::from_str(
        text.rsplit_once("\n\n[Native Cline result projection]\n")
            .unwrap()
            .1,
    )
    .unwrap();
    assert_eq!(projection["truncated"], false);
    assert_eq!(projection["content"], content);
}
#[test]
fn duplicate_native_id_across_explicit_roots_is_diagnostic() {
    let home = tempfile::tempdir().unwrap();
    let first = home.path().join("first");
    let second = home.path().join("second");
    let fixture = Path::new(FIXTURE).join("synthetic-ordinary/synthetic-ordinary.messages.json");
    for root in [&first, &second] {
        let target = root.join("synthetic-ordinary/synthetic-ordinary.messages.json");
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::copy(&fixture, target).unwrap();
    }
    let base = ProviderRoots::from_home(home.path().into(), home.path().join("opencode.db"));
    let mut json = serde_json::to_value(&base).unwrap();
    json["provider_roots"]["cline"] = serde_json::json!([first, second]);
    let roots: ProviderRoots = serde_json::from_value(json).unwrap();
    let store = SessionStore::open(
        StoreOptions::default()
            .db_path(home.path().join("collision.db"))
            .roots(roots),
    )
    .unwrap();
    let report = store
        .discover(DiscoveryOptions::default().sources([Source::Cline]))
        .unwrap();
    assert_eq!(report.discovered, 1);
    assert!(report
        .diagnostics
        .iter()
        .any(|d| d.message.contains("CLINE_ID_COLLISION")));
    let reference = SessionRef::id(Source::Cline, "synthetic-ordinary");
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    assert_eq!(read(&store, "synthetic-ordinary").prompts.len(), 1);
}

#[test]
fn authoritative_missing_file_retires_local_history() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "synthetic-ordinary");
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert!(store
        .session(
            &SessionRef::id(Source::Cline, "synthetic-ordinary"),
            SessionQuery::default()
        )
        .unwrap()
        .is_some());
    std::fs::remove_file(path).unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert!(store
        .session(
            &SessionRef::id(Source::Cline, "synthetic-ordinary"),
            SessionQuery::default()
        )
        .unwrap()
        .is_none());
}

#[test]
#[cfg(all(feature = "export", feature = "unstable-internal"))]
fn escaped_large_result_searches_locally_and_after_remote_intake_with_bounded_export() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "fixture-success-01");
    let mut snapshot: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let visible = format!("alphaunique\nneedleunique {}", "\"".repeat(190_000));
    snapshot["messages"][2]["content"][0]["content"] = serde_json::json!([
        {"type":"text","text":visible},
        {"type":"resource","uri":"synthetic://cline/report"}
    ]);
    std::fs::write(&path, serde_json::to_vec(&snapshot).unwrap()).unwrap();
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let original = read(&store, "fixture-success-01");
    let result = &original.tool_results[0];
    assert!(result
        .text
        .as_deref()
        .unwrap()
        .contains("alphaunique\nneedleunique"));
    assert_eq!(search_result_count(store.db_path(), "needleunique"), 1);
    assert_eq!(
        search_result_count(store.db_path(), "\"alphaunique needleunique\""),
        1
    );
    let mut export = ExportSnapshot::open(
        rusqlite::Connection::open(store.db_path()).unwrap(),
        &ExportSelection {
            sessions: vec![SessionIdentity {
                source: "cline".into(),
                session_id: "fixture-success-01".into(),
            }],
            kinds: vec!["session_event".into()],
            ..Default::default()
        },
        &ExportLimits::default(),
        60_000,
        1_000,
    )
    .unwrap();
    let mut cursor = Some(export.handle().cursor);
    let mut record = None;
    while let Some(value) = cursor {
        let page = export.page(&value, 1_000).unwrap();
        cursor = page.next_cursor;
        for item in page.records {
            if item.payload["kind"] == "tool_result" {
                record = Some(item);
            }
        }
    }
    let record = record.unwrap();
    assert!(record.payload.to_string().len() <= 512 * 1024);
    assert!(record.payload["text"]
        .as_str()
        .unwrap()
        .contains("\"truncated\":true"));
    let receiver_home = tempfile::tempdir().unwrap();
    let receiver = open(receiver_home.path());
    receiver
        .apply_source_observations(SourceObservationBatch::new(
            "cline-transport",
            "fixture",
            SessionLocation::Remote,
            vec![SourceObservation {
                session: ShallowSession {
                    source: "cline".into(),
                    session_id: "fixture-success-01".into(),
                    source_stamp: Some("remote-1".into()),
                    ..Default::default()
                },
                raw_locator: None,
            }],
        ))
        .unwrap();
    let key = ObservationKey {
        source: "cline".into(),
        session_id: "fixture-success-01".into(),
        location: SessionLocation::Remote,
        connector_id: "cline-transport".into(),
        connector_instance: "fixture".into(),
    };
    let revision = receiver.source_observation(&key).unwrap().revision.unwrap();
    let mut payload = record.payload.as_object().unwrap().clone();
    payload.remove("id");
    payload.remove("location");
    receiver
        .apply_source_evidence(
            SourceEvidenceSubmission::new(
                key,
                revision,
                NormalizedSourceEvidence {
                    source_stamp: "remote-1".into(),
                    source_bytes: 0,
                    covered_kinds: vec![EvidenceKind::SessionEvent],
                    records: vec![EvidenceRecord {
                        kind: EvidenceKind::SessionEvent,
                        payload,
                        record_id: Some(record.record_id),
                        revision_id: Some(record.revision_id),
                    }],
                },
            )
            .include_related(false),
        )
        .unwrap();
    assert_eq!(search_result_count(receiver.db_path(), "needleunique"), 1);
    assert_eq!(
        search_result_count(receiver.db_path(), "\"alphaunique needleunique\""),
        1
    );
    let remote = read(&receiver, "fixture-success-01");
    assert!(remote.tool_results[0]
        .text
        .as_deref()
        .unwrap()
        .contains("alphaunique\nneedleunique"));
}

#[test]
#[cfg(feature = "export")]
fn near_limit_result_reserves_export_record_envelope_space() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "fixture-success-01");
    let mut snapshot: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    snapshot["messages"][2]["content"][0]["content"] = serde_json::json!([
        {"type":"text","text":"x".repeat(512 * 1024 - 128)}
    ]);
    std::fs::write(&path, serde_json::to_vec(&snapshot).unwrap()).unwrap();
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let mut export = ExportSnapshot::open(
        rusqlite::Connection::open(store.db_path()).unwrap(),
        &ExportSelection {
            sessions: vec![SessionIdentity {
                source: "cline".into(),
                session_id: "fixture-success-01".into(),
            }],
            kinds: vec!["session_event".into()],
            ..Default::default()
        },
        &ExportLimits::default(),
        60_000,
        1_000,
    )
    .unwrap();
    let mut cursor = Some(export.handle().cursor);
    let mut found = false;
    while let Some(value) = cursor {
        let page = export.page(&value, 1_000).unwrap();
        cursor = page.next_cursor;
        for record in page.records {
            if record.payload["kind"] == "tool_result" {
                assert!(record.payload.to_string().len() <= 512 * 1024);
                let text = record.payload["text"].as_str().unwrap();
                assert!(text.contains("\"truncated\":true"));
                found = true;
            }
        }
    }
    assert!(found);
}

#[test]
#[cfg(all(feature = "export", feature = "unstable-internal"))]
fn oversized_tool_use_exports_and_imports_complete_bounded_evidence() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "fixture-success-01");
    let mut snapshot: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let input = serde_json::json!({"file_path":"README.md","content":"\"".repeat(2_000_000)});
    snapshot["messages"][1]["content"][1]["input"] = input.clone();
    std::fs::write(&path, serde_json::to_vec(&snapshot).unwrap()).unwrap();
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let original = read(&store, "fixture-success-01");
    assert_eq!(original.tool_calls[0].tool_use_id, "tool-call-1");
    let mut export = ExportSnapshot::open(
        rusqlite::Connection::open(store.db_path()).unwrap(),
        &ExportSelection {
            sessions: vec![SessionIdentity {
                source: "cline".into(),
                session_id: "fixture-success-01".into(),
            }],
            kinds: vec![
                "history".into(),
                "session_event".into(),
                "tool_call".into(),
                "session_marker".into(),
            ],
            ..Default::default()
        },
        &ExportLimits::default(),
        60_000,
        1_000,
    )
    .unwrap();
    let mut records = Vec::new();
    let mut cursor = Some(export.handle().cursor);
    while let Some(value) = cursor {
        let page = export.page(&value, 1_000).unwrap();
        cursor = page.next_cursor;
        records.extend(page.records);
    }
    assert!(records.len() >= 4);
    assert!(records
        .iter()
        .all(|record| record.payload.to_string().len() <= 512 * 1024));
    let call = records
        .iter()
        .find(|record| record.kind == "tool_call")
        .unwrap();
    assert_eq!(call.payload["tool_use_id"], "tool-call-1");
    assert_eq!(call.payload["name"], "read_files");
    let args: serde_json::Value =
        serde_json::from_str(call.payload["args_json"].as_str().unwrap()).unwrap();
    assert_eq!(args["projection"], "cline-tool-input-v1");
    assert_eq!(args["truncated"], true);
    let event = records
        .iter()
        .find(|record| record.kind == "session_event" && record.payload["kind"] == "tool_use")
        .unwrap();
    let projected: serde_json::Value =
        serde_json::from_str(event.payload["text"].as_str().unwrap()).unwrap();
    assert_eq!(projected["id"], "tool-call-1");
    assert_eq!(projected["name"], "read_files");
    assert_eq!(projected["native_input_hash"], args["native_input_hash"]);
    assert_eq!(projected["native_input_bytes"], args["native_input_bytes"]);

    let receiver_home = tempfile::tempdir().unwrap();
    let receiver = open(receiver_home.path());
    let key = ObservationKey {
        source: "cline".into(),
        session_id: "fixture-success-01".into(),
        location: SessionLocation::Remote,
        connector_id: "cline-transport".into(),
        connector_instance: "tool-use".into(),
    };
    receiver
        .apply_source_observations(SourceObservationBatch::new(
            "cline-transport",
            "tool-use",
            SessionLocation::Remote,
            vec![SourceObservation {
                session: ShallowSession {
                    source: "cline".into(),
                    session_id: "fixture-success-01".into(),
                    source_stamp: Some("remote-1".into()),
                    ..Default::default()
                },
                raw_locator: None,
            }],
        ))
        .unwrap();
    let revision = receiver.source_observation(&key).unwrap().revision.unwrap();
    let covered = [
        EvidenceKind::History,
        EvidenceKind::SessionEvent,
        EvidenceKind::ToolCall,
        EvidenceKind::SessionMarker,
    ];
    let evidence_records = records
        .into_iter()
        .map(|record| {
            let kind = match record.kind.as_str() {
                "history" => EvidenceKind::History,
                "session_event" => EvidenceKind::SessionEvent,
                "tool_call" => EvidenceKind::ToolCall,
                "session_marker" => EvidenceKind::SessionMarker,
                other => panic!("unexpected export kind: {other}"),
            };
            let mut payload = record.payload.as_object().unwrap().clone();
            payload.remove("id");
            payload.remove("location");
            EvidenceRecord {
                kind,
                payload,
                record_id: Some(record.record_id),
                revision_id: Some(record.revision_id),
            }
        })
        .collect();
    receiver
        .apply_source_evidence(
            SourceEvidenceSubmission::new(
                key,
                revision,
                NormalizedSourceEvidence {
                    source_stamp: "remote-1".into(),
                    source_bytes: 0,
                    covered_kinds: covered.to_vec(),
                    records: evidence_records,
                },
            )
            .include_related(false),
        )
        .unwrap();
    let remote = read(&receiver, "fixture-success-01");
    assert_eq!(remote.tool_calls[0].tool_use_id, "tool-call-1");
    assert_eq!(remote.tool_calls[0].name, "read_files");
}

#[test]
fn mixed_user_text_and_tool_result_preserves_only_prompt_text() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "synthetic-ordinary");
    let mut snapshot: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    snapshot["messages"][0]["content"] = serde_json::json!([
        {"type":"tool_result","tool_use_id":"synthetic-call","content":"SECRETRESULT"},
        {"type":"text","text":"Please summarize"},
        {"type":"text","text":"the README"}
    ]);
    std::fs::write(&path, serde_json::to_vec(&snapshot).unwrap()).unwrap();
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Cline]))
        .unwrap();
    let conn = rusqlite::Connection::open(store.db_path()).unwrap();
    let first: String = conn.query_row("SELECT first_prompt FROM sessions WHERE source='cline' AND session_id='synthetic-ordinary'", [], |row| row.get(0)).unwrap();
    assert_eq!(first, "Please summarize\nthe README");
    store
        .hydrate(
            &SessionRef::id(Source::Cline, "synthetic-ordinary"),
            HydrateOptions::default(),
        )
        .unwrap();
    let evidence = read(&store, "synthetic-ordinary");
    assert_eq!(evidence.prompts.len(), 1);
    assert_eq!(
        evidence.prompts[0].prompt.as_deref(),
        Some("Please summarize\nthe README")
    );
    assert!(!evidence.prompts[0]
        .prompt
        .as_deref()
        .unwrap()
        .contains("SECRETRESULT"));
}

#[cfg(feature = "unstable-internal")]
fn search_result_count(path: &Path, query: &str) -> i64 {
    let conn = rusqlite::Connection::open(path).unwrap();
    conn.query_row("SELECT count(*) FROM session_events_fts f JOIN session_events e ON e.id=f.rowid WHERE session_events_fts MATCH ?1 AND e.source='cline' AND e.kind='tool_result'", [query], |row| row.get(0)).unwrap()
}

#[test]
#[cfg(feature = "unstable-internal")]
fn missing_local_artifact_preserves_remote_observation() {
    let home = tempfile::tempdir().unwrap();
    let store = open(home.path());
    store
        .apply_source_observations(SourceObservationBatch::new(
            "remote-cline",
            "fixture",
            SessionLocation::Remote,
            vec![SourceObservation {
                session: ShallowSession {
                    source: "cline".into(),
                    session_id: "synthetic-ordinary".into(),
                    title: Some("Remote title".into()),
                    source_stamp: Some("remote-1".into()),
                    ..Default::default()
                },
                raw_locator: Some("remote-cline-locator".into()),
            }],
        ))
        .unwrap();
    let key = ObservationKey {
        source: "cline".into(),
        session_id: "synthetic-ordinary".into(),
        location: SessionLocation::Remote,
        connector_id: "remote-cline".into(),
        connector_instance: "fixture".into(),
    };
    let revision = store.source_observation(&key).unwrap().revision.unwrap();
    let remote_event = serde_json::json!({
        "source":"cline", "session_id":"synthetic-ordinary",
        "event_uid":"connector:remote-note", "role":"assistant", "kind":"text",
        "ts_ms":1745343732000_i64, "text":"Remote Cline evidence survives deletion"
    });
    store
        .apply_source_evidence(
            SourceEvidenceSubmission::new(
                key,
                revision,
                NormalizedSourceEvidence {
                    source_stamp: "remote-1".into(),
                    source_bytes: 0,
                    covered_kinds: vec![EvidenceKind::SessionEvent],
                    records: vec![EvidenceRecord {
                        kind: EvidenceKind::SessionEvent,
                        payload: remote_event.as_object().unwrap().clone(),
                        record_id: Some("remote:note".into()),
                        revision_id: Some("remote:1".into()),
                    }],
                },
            )
            .include_related(false),
        )
        .unwrap();
    let path = stage(home.path(), "synthetic-ordinary");
    store.sync(SyncOptions::default().force(true)).unwrap();
    std::fs::remove_file(path).unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    let conn = rusqlite::Connection::open(store.db_path()).unwrap();
    let local: i64 = conn.query_row("SELECT count(*) FROM session_observations WHERE source='cline' AND session_id='synthetic-ordinary' AND location='local'", [], |r|r.get(0)).unwrap();
    let remote: i64 = conn.query_row("SELECT count(*) FROM session_observations WHERE source='cline' AND session_id='synthetic-ordinary' AND location='remote'", [], |r|r.get(0)).unwrap();
    assert_eq!((local, remote), (0, 1));
    let retained = read(&store, "synthetic-ordinary");
    assert!(retained
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .any(|b| b.text.as_deref() == Some("Remote Cline evidence survives deletion")));
    let locator: String = conn.query_row("SELECT raw_path FROM sessions WHERE source='cline' AND session_id='synthetic-ordinary'", [], |r|r.get(0)).unwrap();
    assert_eq!(locator, "remote-cline-locator");
}
