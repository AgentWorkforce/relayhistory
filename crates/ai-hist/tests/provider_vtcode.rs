//! Public VT Code 0.180.2 canonical-journal acquisition acceptance.
mod provider_support;
use ai_hist::{
    CatalogQuery, DiscoveryOptions, HydrateOptions, ProviderRoots, Role, SessionQuery, SessionRef,
    SessionStore, Source, StoreOptions, SyncOptions,
};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

const NATIVE: &str = "vt-synthetic-session";
const FIXTURE: &str =
    include_str!("fixtures/vtcode/basic/.vtcode/sessions/vt-synthetic-session/events.jsonl");

fn stage(home: &Path) -> PathBuf {
    let path = home
        .join(".vtcode/sessions")
        .join(NATIVE)
        .join("events.jsonl");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, FIXTURE).unwrap();
    path
}
fn sid(home: &Path) -> String {
    format!(
        "root:{:x}:{NATIVE}",
        Sha256::digest(home.join(".vtcode/sessions").to_string_lossy().as_bytes())
    )
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
fn evidence(store: &SessionStore, id: &str) -> ai_hist::SessionEvidence {
    store
        .session(&SessionRef::id(Source::Vtcode, id), SessionQuery::default())
        .unwrap()
        .unwrap()
}
fn contains(evidence: &ai_hist::SessionEvidence, text: &str) -> bool {
    evidence
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .any(|block| block.text.as_deref() == Some(text))
}

#[test]
fn discover_hydrate_search_read_reopen_change_feed_export_and_filter() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path());
    let id = sid(home.path());
    let e = provider_support::assert_history_conformance(
        home.path(),
        provider_support::HistoryCase {
            source: Source::Vtcode,
            session_id: &id,
            messages: &[
                (Role::User, "Inspect the synthetic fixture"),
                (Role::Assistant, "I found one safe fixture file."),
                (Role::User, "Run the synthetic verification"),
                (Role::Assistant, "Verification returned fixture."),
            ],
        },
    );
    let oracle: serde_json::Value = serde_json::from_str(include_str!(
        "../../../plans/history-providers/fixtures/vtcode/native-reader.json"
    ))
    .unwrap();
    let native_rows = oracle["events"].as_array().unwrap();
    let native_ids = native_rows
        .iter()
        .filter_map(|row| {
            let event = &row["event"];
            if event["type"] == "turn.started" && event["context"]["origin"] == "user" {
                event["context"]["turn_id"].as_str()
            } else if event["type"] == "item.completed" {
                event["item"]["id"].as_str()
            } else {
                None
            }
        })
        .map(|id| format!("{id}:0"))
        .collect::<std::collections::BTreeSet<_>>();
    let public_ids = e
        .messages
        .iter()
        .flat_map(|message| &message.blocks)
        .map(|block| block.event_uid.clone())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(public_ids, native_ids, "native reader and public event IDs");
    let native_usage = native_rows
        .iter()
        .filter(|row| row["event"]["type"] == "turn.completed")
        .map(|row| &row["event"]["usage"])
        .collect::<Vec<_>>();
    let public_usage = e
        .messages
        .iter()
        .filter_map(|message| message.usage.as_ref())
        .collect::<Vec<_>>();
    assert_eq!(public_usage.len(), native_usage.len());
    for (actual, native) in public_usage.into_iter().zip(native_usage) {
        assert_eq!(
            actual.input_tokens + actual.cache_read_tokens + actual.cache_write_tokens,
            native["input_tokens"].as_u64().unwrap()
        );
        assert_eq!(
            actual.output_tokens,
            native["output_tokens"].as_u64().unwrap()
        );
    }
    assert_eq!(e.tool_calls.len(), 1);
    assert_eq!(e.tool_calls[0].tool_use_id, "call-b");
    assert!(e
        .tool_results
        .iter()
        .any(|r| r.tool_use_id.as_deref() == Some("call-b")));
    assert_eq!(
        e.messages.iter().filter_map(|m| m.usage.as_ref()).count(),
        2,
        "turn.completed usage is not counted again from thread.completed"
    );
    assert!(e
        .markers
        .iter()
        .any(|m| m.kind == "thread.compact_boundary"));
    assert!(e.file_edits.is_empty());
    assert!(e.relationships.is_empty());
    let store = open(home.path());
    let mut query = CatalogQuery::default();
    query.sources = Some(vec![Source::Vtcode]);
    assert_eq!(store.sessions(query).count(), 1);
}

#[test]
fn partial_tail_append_rewrite_and_empty_roots_do_not_claim_missing_history() {
    let home = tempfile::tempdir().unwrap();
    let store = open(home.path());
    assert_eq!(
        store
            .discover(DiscoveryOptions::default().sources([Source::Vtcode]))
            .unwrap()
            .discovered,
        0
    );
    let path = stage(home.path());
    let id = sid(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Vtcode]))
        .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Vtcode, &id),
            HydrateOptions::default(),
        )
        .unwrap();
    let original = evidence(&store, &id).messages;
    std::fs::write(&path, format!("{FIXTURE}{{\"schema_version\":")).unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Vtcode, &id),
            HydrateOptions::default(),
        )
        .unwrap();
    assert_eq!(evidence(&store, &id).messages, original);
    let rewritten = FIXTURE.replace("I found one safe fixture file.", "Changed fixture answer.");
    std::fs::write(&path, rewritten).unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Vtcode, &id),
            HydrateOptions::default(),
        )
        .unwrap();
    assert!(contains(&evidence(&store, &id), "Changed fixture answer."));
    assert!(!contains(
        &evidence(&store, &id),
        "I found one safe fixture file."
    ));
    std::fs::write(&path, FIXTURE).unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert!(contains(
        &evidence(&store, &id),
        "I found one safe fixture file."
    ));
    let mut turn: serde_json::Value =
        serde_json::from_str(FIXTURE.lines().nth(1).unwrap()).unwrap();
    turn["event"]["context"]["turn_id"] = "turn-c".into();
    turn["event"]["context"]["task_id"] = "task-c".into();
    turn["event"]["context"]["timestamp"] = "2026-10-20T12:02:00Z".into();
    turn["event"]["context"]["goal"] = "Appended synthetic turn".into();
    let mut answer: serde_json::Value =
        serde_json::from_str(FIXTURE.lines().nth(2).unwrap()).unwrap();
    answer["event"]["item"]["id"] = "assistant-c".into();
    answer["event"]["item"]["context"]["turn_id"] = "turn-c".into();
    answer["event"]["item"]["context"]["task_id"] = "task-c".into();
    answer["event"]["item"]["context"]["timestamp"] = "2026-10-20T12:02:01Z".into();
    answer["event"]["item"]["text"] = "Appended synthetic answer".into();
    let appended = format!("{FIXTURE}{turn}\n{answer}\n");
    std::fs::write(&path, appended).unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Vtcode, &id),
            HydrateOptions::default(),
        )
        .unwrap();
    assert!(contains(&evidence(&store, &id), "Appended synthetic turn"));
    assert!(contains(
        &evidence(&store, &id),
        "Appended synthetic answer"
    ));
    std::fs::write(&path, FIXTURE).unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Vtcode, &id),
            HydrateOptions::default(),
        )
        .unwrap();
    assert!(!contains(&evidence(&store, &id), "Appended synthetic turn"));
}

#[test]
fn unsupported_and_malformed_canonical_rows_are_diagnostic() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let store = open(home.path());
    std::fs::write(&path, FIXTURE.replacen("0.18.0", "9.0.0", 1)).unwrap();
    let report = store
        .discover(DiscoveryOptions::default().sources([Source::Vtcode]))
        .unwrap();
    assert_eq!(report.discovered, 0);
    assert!(report
        .diagnostics
        .iter()
        .any(|d| d.message.contains("VTCODE_UNSUPPORTED_SCHEMA")));
    std::fs::write(&path, format!("{FIXTURE}{{invalid}}\n")).unwrap();
    let report = store
        .discover(DiscoveryOptions::default().sources([Source::Vtcode]))
        .unwrap();
    assert_eq!(report.discovered, 0);
    assert!(report
        .diagnostics
        .iter()
        .any(|d| d.message.contains("VTCODE_INVALID_RECORD")));
}

#[test]
fn metadata_only_journal_change_updates_marker_without_rewriting_messages() {
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let id = sid(home.path());
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::Vtcode]))
        .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Vtcode, &id),
            HydrateOptions::default(),
        )
        .unwrap();
    let before = evidence(&store, &id);
    let mut lines = FIXTURE.lines().map(str::to_owned).collect::<Vec<_>>();
    let mut meta: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
    meta["event"]["fixture_metadata"] = "changed".into();
    lines[0] = meta.to_string();
    std::fs::write(&path, format!("{}\n", lines.join("\n"))).unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::Vtcode, &id),
            HydrateOptions::default(),
        )
        .unwrap();
    let after = evidence(&store, &id);
    assert_eq!(after.messages, before.messages);
    assert_ne!(after.markers, before.markers);
}

#[test]
fn missing_canonical_journal_is_diagnostic() {
    let home = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(home.path().join(".vtcode/sessions/missing")).unwrap();
    let store = open(home.path());
    let error = store
        .discover(DiscoveryOptions::default().sources([Source::Vtcode]))
        .unwrap_err();
    assert!(
        error.to_string().contains("VTCODE_MISSING_JOURNAL"),
        "{error}"
    );
}

#[cfg(unix)]
#[test]
fn symlink_to_outside_evidence_is_rejected() {
    let home = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let secret = outside.path().join("events.jsonl");
    std::fs::write(&secret, FIXTURE).unwrap();
    std::fs::remove_file(&path).unwrap();
    std::os::unix::fs::symlink(secret, &path).unwrap();
    let store = open(home.path());
    assert!(store
        .discover(DiscoveryOptions::default().sources([Source::Vtcode]))
        .is_err());
}

#[cfg(feature = "unstable-internal")]
#[test]
fn remote_backed_native_rewrite_rolls_back_catalog_and_evidence() {
    for ownership in ["remote", "both"] {
        let home = tempfile::tempdir().unwrap();
        let path = stage(home.path());
        let id = sid(home.path());
        let store = open(home.path());
        store
            .discover(DiscoveryOptions::default().sources([Source::Vtcode]))
            .unwrap();
        let reference = SessionRef::id(Source::Vtcode, &id);
        store
            .hydrate(&reference, HydrateOptions::default())
            .unwrap();
        let conn = ai_hist::open_db(&home.path().join("conformance.db")).unwrap();
        conn.execute("UPDATE session_events SET location=?1 WHERE source='vtcode' AND session_id=?2 AND event_uid='assistant-a:0'", rusqlite::params![ownership,id]).unwrap();
        let before = evidence(&store, &id);
        let revision = store.head_revision().unwrap();
        std::fs::write(
            &path,
            FIXTURE.replace("I found one safe fixture file.", "Remote backed rewrite."),
        )
        .unwrap();
        let error = store
            .hydrate(&reference, HydrateOptions::default())
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("VTCODE_SHARED_EVIDENCE_CONFLICT"),
            "{error}"
        );
        let after = evidence(&store, &id);
        assert_eq!(after.session, before.session);
        assert_eq!(after.messages, before.messages);
        assert_eq!(after.tool_calls, before.tool_calls);
        assert_eq!(after.markers, before.markers);
        assert_eq!(store.head_revision().unwrap(), revision);
    }
}

#[cfg(all(feature = "unstable-internal", feature = "export"))]
#[test]
fn native_export_remote_intake_then_local_hydrate_preserves_identities() {
    use ai_hist::export::{ExportLimits, ExportSelection, ExportSnapshot};
    use ai_hist::{
        EvidenceKind, EvidenceRecord, NormalizedSourceEvidence, ObservationKey, SessionLocation,
        ShallowSession, SourceEvidenceSubmission, SourceObservation, SourceObservationBatch,
    };
    let home = tempfile::tempdir().unwrap();
    let path = stage(home.path());
    let id = sid(home.path());
    let source = open(home.path());
    source.sync(SyncOptions::default().force(true)).unwrap();
    let native = evidence(&source, &id);
    let kinds = vec![
        EvidenceKind::SessionEvent,
        EvidenceKind::SessionMarker,
        EvidenceKind::ToolCall,
    ];
    let mut snapshot = ExportSnapshot::open(
        rusqlite::Connection::open(home.path().join("conformance.db")).unwrap(),
        &ExportSelection {
            sources: vec!["vtcode".into()],
            kinds: kinds.iter().map(|kind| kind.as_str().into()).collect(),
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
    let mut records = Vec::new();
    while let Some(value) = cursor {
        let page = snapshot.page(&value, 1_000).unwrap();
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
    assert!(records.len() >= native.messages.len());
    let receiver = SessionStore::open(
        StoreOptions::default()
            .db_path(home.path().join("remote.db"))
            .roots(ProviderRoots::from_home(
                home.path().into(),
                home.path().join("opencode.db"),
            )),
    )
    .unwrap();
    let key = ObservationKey {
        source: "vtcode".into(),
        session_id: id.clone(),
        location: SessionLocation::Remote,
        connector_id: "synthetic-remote".into(),
        connector_instance: "default".into(),
    };
    receiver
        .apply_source_observations(SourceObservationBatch::new(
            "synthetic-remote",
            "default",
            SessionLocation::Remote,
            vec![SourceObservation {
                session: ShallowSession {
                    source: "vtcode".into(),
                    session_id: id.clone(),
                    source_stamp: Some("synthetic-remote-v1".into()),
                    ..Default::default()
                },
                raw_locator: None,
            }],
        ))
        .unwrap();
    let revision = receiver.source_observation(&key).unwrap().revision.unwrap();
    receiver
        .apply_source_evidence(
            SourceEvidenceSubmission::new(
                key,
                revision,
                NormalizedSourceEvidence {
                    source_stamp: "synthetic-remote-v1".into(),
                    source_bytes: 1,
                    covered_kinds: kinds,
                    records,
                },
            )
            .include_related(false),
        )
        .unwrap();
    let before = evidence(&receiver, &id);
    assert_eq!(before.messages, native.messages);
    assert_eq!(before.tool_calls, native.tool_calls);
    receiver
        .discover(DiscoveryOptions::default().sources([Source::Vtcode]))
        .unwrap();
    receiver
        .hydrate(
            &SessionRef::id(Source::Vtcode, &id),
            HydrateOptions::default(),
        )
        .unwrap();
    let after = evidence(&receiver, &id);
    assert_eq!(after.messages, native.messages);
    assert_eq!(after.tool_calls, native.tool_calls);
    assert_eq!(after.markers, native.markers);
    let revision = receiver.head_revision().unwrap();
    std::fs::write(
        &path,
        FIXTURE.replace(
            "I found one safe fixture file.",
            "Conflicting native answer.",
        ),
    )
    .unwrap();
    let error = receiver
        .hydrate(
            &SessionRef::id(Source::Vtcode, &id),
            HydrateOptions::default(),
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("VTCODE_SHARED_EVIDENCE_CONFLICT"),
        "{error}"
    );
    let retained = evidence(&receiver, &id);
    assert_eq!(retained.messages, native.messages);
    assert_eq!(retained.tool_calls, native.tool_calls);
    assert_eq!(retained.markers, native.markers);
    assert_eq!(receiver.head_revision().unwrap(), revision);
}
