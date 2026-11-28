//! Public Qoder global CLI history acceptance with a synthetic native writer fixture.
mod provider_support;
use ai_hist::{
    DiscoveryOptions, HydrateOptions, ProviderRoots, Role, SessionQuery, SessionRef, SessionStore,
    Source, StoreOptions,
};
use std::path::{Path, PathBuf};

const TOOL: &str = include_str!(
    "fixtures/qoder/global-cli/.qoder/projects/-synthetic-qoder-project/synthetic-tool.jsonl"
);
const RESUME: &str = include_str!(
    "fixtures/qoder/global-cli/.qoder/projects/-synthetic-qoder-project/synthetic-resume.jsonl"
);

fn stage(home: &Path, id: &str, text: &str) -> PathBuf {
    let path = home.join(format!(
        ".qoder/projects/-synthetic-qoder-project/{id}.jsonl"
    ));
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, text).unwrap();
    path
}

fn open(home: &Path) -> SessionStore {
    SessionStore::open(
        StoreOptions::default()
            .db_path(home.join("qoder.db"))
            .roots(ProviderRoots::from_home(
                home.into(),
                home.join("opencode.db"),
            )),
    )
    .unwrap()
}

#[test]
fn qoder_native_writer_tool_history_reaches_public_facade() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), "synthetic-tool", TOOL);
    let evidence = provider_support::assert_history_conformance(
        home.path(),
        provider_support::HistoryCase {
            source: Source::Qoder,
            session_id: "synthetic-tool",
            messages: &[
                (Role::User, "Read note.txt."),
                (Role::Assistant, "Reading it."),
                (Role::Assistant, "The note is synthetic."),
            ],
        },
    );
    assert_eq!(evidence.tool_calls.len(), 1);
    assert_eq!(evidence.tool_results.len(), 1);
    assert_eq!(evidence.tool_calls[0].tool_use_id, "call-1");
    assert_eq!(
        evidence.tool_results[0].tool_use_id.as_deref(),
        Some("call-1")
    );
    assert_eq!(
        evidence.tool_results[0].text.as_deref(),
        Some("Synthetic note contents.")
    );
    assert_eq!(
        evidence
            .messages
            .iter()
            .find(|m| m.message_id.as_deref() == Some("tool-a1"))
            .unwrap()
            .parent_id
            .as_deref(),
        Some("tool-u1")
    );
    assert!(evidence.messages.iter().all(|m| m.usage.is_none()));
    assert_eq!(
        evidence.prompts.len(),
        1,
        "tool result is not a human prompt"
    );
    let ids: Vec<_> = evidence
        .messages
        .iter()
        .filter_map(|m| m.message_id.as_deref())
        .collect();
    assert_eq!(
        ids,
        ["tool-u1", "tool-a1", "tool-r1", "tool-a2"],
        "native writer order"
    );
    let oracle: serde_json::Value = serde_json::from_str(include_str!(
        "../../../plans/history-providers/fixtures/qoder/official-reader.json"
    ))
    .unwrap();
    let reader_ids: Vec<_> = oracle["synthetic-tool"]
        .as_array()
        .unwrap()
        .iter()
        .map(|message| message["uuid"].as_str().unwrap())
        .collect();
    assert_eq!(
        ids, reader_ids,
        "published CLI reader agrees with local order"
    );
    assert_eq!(
        oracle["synthetic-tool"][1]["message"]["content"][1]["id"], "call-1",
        "the independent reader recovered the tool identity"
    );
    assert!(evidence.markers.iter().any(|m| m.kind == "active-leaf"));
    assert_eq!(
        evidence
            .markers
            .iter()
            .find(|m| m.marker_uid == "1:active-leaf")
            .unwrap()
            .ts_ms,
        Some(1_792_606_312_007),
        "native integer-millisecond control timestamp reaches public markers"
    );
}

#[test]
fn qoder_resume_append_partial_tail_and_rewrite() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "synthetic-resume", RESUME);
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Qoder]))
        .unwrap();
    let reference = SessionRef::id(Source::Qoder, "synthetic-resume");
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    let read = || {
        store
            .session(&reference, SessionQuery::default())
            .unwrap()
            .unwrap()
    };
    assert_eq!(read().prompts.len(), 2);
    let extra = r#"{"type":"user","uuid":"resume-u3","message":{"role":"user","content":"Third turn."},"parentUuid":"resume-a2","isSidechain":false,"cwd":"/synthetic/qoder/project","sessionId":"synthetic-resume","entrypoint":"cli","version":"1.1.75","timestamp":"2026-10-21T18:04:00Z"}"#;
    std::fs::write(&path, format!("{RESUME}{}", &extra[..30])).unwrap();
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    assert_eq!(read().prompts.len(), 2, "incomplete tail is deferred");
    std::fs::write(&path, format!("{RESUME}{extra}\n")).unwrap();
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    assert_eq!(read().prompts.len(), 3);
    std::fs::write(&path, RESUME).unwrap();
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    assert_eq!(
        read().prompts.len(),
        2,
        "complete rewrite retires removed prompt"
    );
}

#[test]
fn qoder_unsupported_schema_is_diagnostic() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "synthetic-tool", TOOL);
    std::fs::write(
        &path,
        TOOL.replacen("\"version\":\"1.1.75\"", "\"version\":\"0.1\"", 1),
    )
    .unwrap();
    let result = open(home.path())
        .discover(DiscoveryOptions::default().sources([Source::Qoder]))
        .unwrap();
    assert_eq!(result.discovered, 0);
    assert!(result
        .diagnostics
        .iter()
        .any(|d| d.message.contains("QODER_UNSUPPORTED_SCHEMA")));
    std::fs::write(&path, format!("{TOOL}{{malformed}}\n")).unwrap();
    let malformed = open(home.path())
        .discover(DiscoveryOptions::default().sources([Source::Qoder]))
        .unwrap();
    assert_eq!(malformed.discovered, 0);
    assert!(malformed
        .diagnostics
        .iter()
        .any(|d| d.message.contains("QODER_INVALID_RECORD")));
}

#[test]
fn qoder_metadata_only_rewrite_keeps_messages_and_updates_marker() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "synthetic-tool", TOOL);
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Qoder]))
        .unwrap();
    let reference = SessionRef::id(Source::Qoder, "synthetic-tool");
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    let before = store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    std::fs::write(
        &path,
        TOOL.replacen(
            "\"lastPrompt\":\"Read note.txt.\"",
            "\"lastPrompt\":\"Renamed prompt label.\"",
            1,
        ),
    )
    .unwrap();
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    let after = store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    assert_eq!(after.messages, before.messages);
    assert_eq!(after.tool_calls, before.tool_calls);
    assert_ne!(after.markers, before.markers);
}

#[test]
fn qoder_rewrite_without_result_error_clears_old_local_failure() {
    use ai_hist::SyncOptions;
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "synthetic-tool", TOOL);
    let store = open(home.path());
    let mut lines = TOOL.lines().map(str::to_owned).collect::<Vec<_>>();
    let mut result: serde_json::Value = serde_json::from_str(&lines[6]).unwrap();
    result["message"]["content"][0]["is_error"] = true.into();
    lines[6] = result.to_string();
    std::fs::write(&path, format!("{}\n", lines.join("\n"))).unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    let reference = SessionRef::id(Source::Qoder, "synthetic-tool");
    assert_eq!(
        store
            .session(&reference, SessionQuery::default())
            .unwrap()
            .unwrap()
            .tool_calls[0]
            .is_error,
        Some(true)
    );
    std::fs::write(&path, TOOL).unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert_eq!(
        store
            .session(&reference, SessionQuery::default())
            .unwrap()
            .unwrap()
            .tool_calls[0]
            .is_error,
        None,
        "the complete local rewrite no longer reports an obsolete failure"
    );
}

#[test]
fn qoder_replaced_transcript_at_same_path_retires_old_local_session() {
    use ai_hist::SyncOptions;
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "synthetic-tool", TOOL);
    let store = open(home.path());
    store.sync(SyncOptions::default().force(true)).unwrap();
    let old = SessionRef::id(Source::Qoder, "synthetic-tool");
    assert!(store
        .session(&old, SessionQuery::default())
        .unwrap()
        .is_some());
    std::fs::write(&path, TOOL.replace("synthetic-tool", "replacement-tool")).unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert!(store
        .session(&old, SessionQuery::default())
        .unwrap()
        .is_none());
    let new = SessionRef::id(Source::Qoder, "replacement-tool");
    assert!(store
        .session(&new, SessionQuery::default())
        .unwrap()
        .is_some());
}

#[test]
fn qoder_empty_root_collision_and_explicit_root_are_truthful() {
    let home = tempfile::tempdir().unwrap();
    let store = open(home.path());
    assert_eq!(
        store
            .discover(DiscoveryOptions::default().sources([Source::Qoder]))
            .unwrap()
            .discovered,
        0
    );
    let native = stage(home.path(), "synthetic-tool", TOOL);
    store
        .discover(DiscoveryOptions::default().sources([Source::Qoder]))
        .unwrap();
    let second = home
        .path()
        .join(".qoder/projects/second-project/synthetic-tool.jsonl");
    std::fs::create_dir_all(second.parent().unwrap()).unwrap();
    std::fs::copy(&native, &second).unwrap();
    let scan = store
        .discover(DiscoveryOptions::default().sources([Source::Qoder]))
        .unwrap();
    assert!(scan
        .diagnostics
        .iter()
        .any(|d| d.message.contains("QODER_ID_COLLISION")));

    let separate = tempfile::tempdir().unwrap();
    let custom = separate.path().join("custom-qoder-projects");
    let target = custom.join("one/synthetic-tool.jsonl");
    std::fs::create_dir_all(target.parent().unwrap()).unwrap();
    std::fs::write(&target, TOOL).unwrap();
    let mut roots = serde_json::to_value(ProviderRoots::from_home(
        separate.path().into(),
        separate.path().join("opencode.db"),
    ))
    .unwrap();
    roots["provider_roots"]["qoder"] = serde_json::json!([custom]);
    roots["provider_default_roots"]
        .as_object_mut()
        .unwrap()
        .remove("qoder");
    let roots: ProviderRoots = serde_json::from_value(roots).unwrap();
    let custom_store = SessionStore::open(
        StoreOptions::default()
            .db_path(separate.path().join("custom.db"))
            .roots(roots),
    )
    .unwrap();
    assert_eq!(
        custom_store
            .discover(DiscoveryOptions::default().sources([Source::Qoder]))
            .unwrap()
            .discovered,
        1
    );
    custom_store
        .hydrate(
            &SessionRef::id(Source::Qoder, "synthetic-tool"),
            HydrateOptions::default(),
        )
        .unwrap();
    assert!(!custom_store
        .session(
            &SessionRef::id(Source::Qoder, "synthetic-tool"),
            SessionQuery::default()
        )
        .unwrap()
        .unwrap()
        .messages
        .is_empty());
}

#[test]
fn qoder_oversized_opening_prompt_remains_discoverable() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path(), "synthetic-tool", TOOL);
    let mut lines = TOOL.lines().map(str::to_owned).collect::<Vec<_>>();
    let mut opening: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
    opening["message"]["content"] = "X".repeat(70_000).into();
    lines[0] = opening.to_string();
    std::fs::write(path, format!("{}\n", lines.join("\n"))).unwrap();
    let store = open(home.path());
    let report = store
        .discover(DiscoveryOptions::default().sources([Source::Qoder]))
        .unwrap();
    assert_eq!(report.discovered, 1);
    let conn = rusqlite::Connection::open(home.path().join("qoder.db")).unwrap();
    let prompt: String = conn
        .query_row(
            "SELECT first_prompt FROM sessions WHERE source='qoder' AND session_id='synthetic-tool'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(prompt, "X".repeat(4096));
}

#[test]
fn qoder_over_cap_opening_is_diagnostic_without_hiding_a_valid_sibling() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), "synthetic-tool", TOOL);
    let mut opening: serde_json::Value =
        serde_json::from_str(TOOL.lines().next().unwrap()).unwrap();
    opening["sessionId"] = "oversized".into();
    opening["message"]["content"] = "X".repeat(8 * 1024 * 1024).into();
    stage(home.path(), "oversized", &format!("{opening}\n"));
    let store = open(home.path());
    let report = store
        .discover(DiscoveryOptions::default().sources([Source::Qoder]))
        .unwrap();
    assert_eq!(report.discovered, 1);
    assert!(report.diagnostics.iter().any(|diagnostic| diagnostic
        .message
        .contains("QODER_OVERSIZED_OPENING_RECORD")));
}

#[test]
fn qoder_shallow_tail_advances_activity_after_many_appends() {
    for count in [300, 1_000] {
        let home = tempfile::tempdir().unwrap();
        let path = stage(home.path(), "synthetic-tool", TOOL);
        let mut bytes = TOOL.to_owned();
        for index in 0..count {
            bytes.push_str(&format!(
                "{}\n",
                serde_json::json!({"type":"active-leaf","sessionId":"synthetic-tool","leafUuid":"tool-a2","timestamp":1_900_000_000_000_i64 + index})
            ));
        }
        std::fs::write(path, bytes).unwrap();
        let store = open(home.path());
        let report = store
            .discover(DiscoveryOptions::default().sources([Source::Qoder]))
            .unwrap();
        assert_eq!(report.discovered, 1);
        let conn = rusqlite::Connection::open(home.path().join("qoder.db")).unwrap();
        let last: i64 = conn
            .query_row(
                "SELECT last_activity_ms FROM sessions WHERE source='qoder' AND session_id='synthetic-tool'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(last, 1_900_000_000_000_i64 + count - 1);
    }
}

#[cfg(unix)]
#[test]
fn qoder_enumeration_skips_nested_trees_and_directory_symlinks() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path(), "synthetic-tool", TOOL);
    let root = home.path().join(".qoder/projects");
    let nested = root.join("unrelated/deep/tree");
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::write(nested.join("invalid.jsonl"), b"not a transcript\n").unwrap();
    std::os::unix::fs::symlink(&root, root.join("loop")).unwrap();
    let store = open(home.path());
    let report = store
        .discover(DiscoveryOptions::default().sources([Source::Qoder]))
        .unwrap();
    assert_eq!(report.discovered, 1);
    assert!(report.diagnostics.is_empty());
}

#[cfg(all(feature = "unstable-internal", feature = "export"))]
fn remote_case(
    fixture: &str,
    id: &str,
) -> (tempfile::TempDir, SessionStore, ai_hist::SessionEvidence) {
    use ai_hist::export::{ExportLimits, ExportSelection, ExportSnapshot};
    use ai_hist::{
        EvidenceKind, EvidenceRecord, NormalizedSourceEvidence, ObservationKey, SessionLocation,
        ShallowSession, SourceEvidenceSubmission, SourceObservation, SourceObservationBatch,
        SyncOptions,
    };
    let origin = tempfile::tempdir().unwrap();
    stage(origin.path(), id, fixture);
    let source = open(origin.path());
    source.sync(SyncOptions::default().force(true)).unwrap();
    let reference = SessionRef::id(Source::Qoder, id);
    let original = source
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    let kinds = vec![
        EvidenceKind::SessionEvent,
        EvidenceKind::SessionMarker,
        EvidenceKind::ToolCall,
    ];
    let mut export = ExportSnapshot::open(
        rusqlite::Connection::open(origin.path().join("qoder.db")).unwrap(),
        &ExportSelection {
            sources: vec!["qoder".into()],
            kinds: kinds.iter().map(|k| k.as_str().into()).collect(),
            ..Default::default()
        },
        &ExportLimits::default(),
        60_000,
        1_000,
    )
    .unwrap();
    let mut cursor = Some(export.handle().cursor);
    let mut records = vec![];
    while let Some(value) = cursor {
        let page = export.page(&value, 1_000).unwrap();
        cursor = page.next_cursor;
        for record in page.records {
            let mut payload = record.payload.as_object().unwrap().clone();
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
    assert!(records.iter().any(|r| r
        .payload
        .get("tool_use_id")
        .and_then(|v| v.as_str())
        .is_some()));
    let destination = tempfile::tempdir().unwrap();
    let store = open(destination.path());
    let key = ObservationKey {
        source: "qoder".into(),
        session_id: id.into(),
        location: SessionLocation::Remote,
        connector_id: "fixture-export".into(),
        connector_instance: "default".into(),
    };
    store
        .apply_source_observations(SourceObservationBatch::new(
            "fixture-export",
            "default",
            SessionLocation::Remote,
            vec![SourceObservation {
                session: ShallowSession {
                    source: "qoder".into(),
                    session_id: id.into(),
                    source_stamp: Some("export-1".into()),
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
                    source_stamp: "export-1".into(),
                    source_bytes: 1,
                    covered_kinds: kinds,
                    records,
                },
            )
            .include_related(false),
        )
        .unwrap();
    (destination, store, original)
}

#[cfg(all(feature = "unstable-internal", feature = "export"))]
#[test]
fn qoder_actual_export_public_intake_then_local_hydrate_preserves_remote_share() {
    let (destination, store, original) = remote_case(TOOL, "synthetic-tool");
    let reference = SessionRef::id(Source::Qoder, "synthetic-tool");
    let before = store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    assert_eq!(before.tool_calls[0].tool_use_id, "call-1");
    let local = stage(destination.path(), "synthetic-tool", TOOL);
    store
        .discover(DiscoveryOptions::default().sources([Source::Qoder]))
        .unwrap();
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    let combined = store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    assert_eq!(combined.messages, original.messages);
    assert_eq!(combined.tool_calls, original.tool_calls);
    std::fs::write(
        &local,
        TOOL.lines().take(3).collect::<Vec<_>>().join("\n") + "\n",
    )
    .unwrap();
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    let after = store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    assert!(
        after
            .messages
            .iter()
            .flat_map(|m| &m.blocks)
            .any(|b| b.text.as_deref() == Some("The note is synthetic.")),
        "remote share survives local rewrite"
    );
    let before_conflict = after.messages.clone();
    std::fs::write(&local, TOOL.replace("Reading it.", "Rewritten locally.")).unwrap();
    let failure = store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap_err();
    assert!(format!("{failure:#}").contains("QODER_SHARED_EVIDENCE_CONFLICT"));
    assert_eq!(
        store
            .session(&reference, SessionQuery::default())
            .unwrap()
            .unwrap()
            .messages,
        before_conflict,
        "conflicting local payload rewrite leaves remote snapshot intact"
    );
}

#[cfg(all(feature = "unstable-internal", feature = "export"))]
#[test]
fn qoder_replaced_locator_keeps_imported_old_session_remote_only() {
    use ai_hist::SyncOptions;
    let (destination, store, remote) = remote_case(TOOL, "synthetic-tool");
    let path = stage(destination.path(), "synthetic-tool", TOOL);
    store.sync(SyncOptions::default().force(true)).unwrap();
    let old = SessionRef::id(Source::Qoder, "synthetic-tool");
    assert_eq!(
        store
            .session(&old, SessionQuery::default())
            .unwrap()
            .unwrap()
            .messages,
        remote.messages
    );
    std::fs::write(&path, TOOL.replace("synthetic-tool", "replacement-tool")).unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert_eq!(
        store
            .session(&old, SessionQuery::default())
            .unwrap()
            .unwrap()
            .messages,
        remote.messages,
        "the imported old session remains searchable"
    );
    let conn = rusqlite::Connection::open(destination.path().join("qoder.db")).unwrap();
    let local_presence: i64 = conn.query_row(
        "SELECT count(*) FROM session_presences WHERE source='qoder' AND session_id='synthetic-tool' AND location='local'",
        [],
        |row| row.get(0),
    ).unwrap();
    assert_eq!(
        local_presence, 0,
        "the replaced locator no longer backs the old session"
    );
    assert!(store
        .session(
            &SessionRef::id(Source::Qoder, "replacement-tool"),
            SessionQuery::default()
        )
        .unwrap()
        .is_some());
}

#[cfg(all(feature = "unstable-internal", feature = "export"))]
#[test]
fn qoder_same_millisecond_export_preserves_order_and_rejects_native_conflicts() {
    const SAME: &str = include_str!("fixtures/qoder/global-cli/.qoder/projects/-synthetic-qoder-project/synthetic-same-ms.jsonl");
    let (destination, store, original) = remote_case(SAME, "synthetic-same-ms");
    assert_eq!(
        original
            .messages
            .iter()
            .filter_map(|m| m.message_id.as_deref())
            .collect::<Vec<_>>(),
        ["same-u1", "same-a1", "same-r1", "same-a2"],
        "local parser keeps native writer order"
    );
    let reference = SessionRef::id(Source::Qoder, "synthetic-same-ms");
    let prior = store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    assert_eq!(
        prior.messages, original.messages,
        "remote export preserves recorded order at one millisecond"
    );
    stage(destination.path(), "synthetic-same-ms", SAME);
    store
        .discover(DiscoveryOptions::default().sources([Source::Qoder]))
        .unwrap();
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    assert_eq!(
        store
            .session(&reference, SessionQuery::default())
            .unwrap()
            .unwrap()
            .messages,
        original.messages,
        "matching local evidence agrees with the transported native order"
    );
    let mut changed = SAME.lines().collect::<Vec<_>>();
    changed.swap(1, 3);
    stage(
        destination.path(),
        "synthetic-same-ms",
        &(changed.join("\n") + "\n\n"),
    );
    store
        .discover(DiscoveryOptions::default().sources([Source::Qoder]))
        .unwrap();
    let failure = store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap_err();
    assert!(format!("{failure:#}").contains("FILE_HISTORY_SHARED_ORDER_CONFLICT"));
    assert_eq!(
        store
            .session(&reference, SessionQuery::default())
            .unwrap()
            .unwrap()
            .messages,
        prior.messages,
        "failed local replay leaves remote snapshot intact"
    );
}
