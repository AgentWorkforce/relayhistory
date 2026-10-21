//! Real schema-31 capture and pre-intake-fix ledger compatibility through public APIs.
use ai_hist::export::{
    ExportLimits, ExportSelection, ExportSnapshot, HistoryExportRecord, SessionIdentity,
};
use ai_hist::{
    BlockKind, DiscoveryOptions, EvidenceKind, EvidenceRecord, HydrateOptions,
    NormalizedSourceEvidence, ObservationKey, ProviderRoots, SessionEvidence, SessionLocation,
    SessionQuery, SessionRef, SessionStore, ShallowSession, Source, SourceEvidenceSubmission,
    SourceObservation, SourceObservationBatch, StoreOptions,
};
use serde_json::json;
use std::path::Path;
const SID: &str = "default:636861742f6e6f742d612d757569643aceb1";
const CALL: &str = "call/native:1";
fn kinds() -> Vec<EvidenceKind> {
    vec![
        EvidenceKind::SessionEvent,
        EvidenceKind::ToolCall,
        EvidenceKind::SessionMarker,
    ]
}
fn reference() -> SessionRef {
    SessionRef::id(Source::Hermes, SID)
}
fn open(home: &Path) -> SessionStore {
    SessionStore::open(
        StoreOptions::default()
            .db_path(home.join("ledger.db"))
            .roots(ProviderRoots::from_home(
                home.into(),
                home.join("opencode.db"),
            )),
    )
    .unwrap()
}
fn stage(home: &Path) {
    std::fs::create_dir_all(home.join(".hermes")).unwrap();
    rusqlite::Connection::open(home.join(".hermes/state.db"))
        .unwrap()
        .execute_batch(include_str!("../fixtures/hermes/schema31.sql"))
        .unwrap();
}
fn hydrate(store: &SessionStore) {
    store
        .discover(DiscoveryOptions::default().sources([Source::Hermes]))
        .unwrap();
    store
        .hydrate(&reference(), HydrateOptions::default())
        .unwrap();
}
fn read(store: &SessionStore) -> SessionEvidence {
    store
        .session(&reference(), SessionQuery::default())
        .unwrap()
        .unwrap()
}
fn assert_native(e: &SessionEvidence) {
    assert_eq!(e.tool_calls.len(), 1);
    assert_eq!(e.tool_calls[0].tool_use_id, CALL);
    assert_eq!(e.tool_results.len(), 1);
    assert_eq!(e.tool_results[0].tool_use_id.as_deref(), Some(CALL));
    let blocks: Vec<_> = e
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .filter(|b| matches!(b.kind, BlockKind::ToolUse | BlockKind::ToolResult))
        .collect();
    assert_eq!(blocks.len(), 2);
    assert!(blocks
        .iter()
        .all(|b| b.tool_use_id.as_deref() == Some(CALL)));
    assert!(blocks.iter().any(|b| b.kind == BlockKind::ToolResult
        && b.text.as_deref() == Some("The Hermes fixture needle is here.")));
    assert!(e
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .any(|b| b.text.as_deref() == Some("The historical answer is still available.")));
    assert!(!e.markers.is_empty());
    assert!(e.requests.iter().all(|r| r.usage.is_none()));
}
fn export(home: &Path) -> Vec<HistoryExportRecord> {
    let mut snapshot = ExportSnapshot::open(
        rusqlite::Connection::open(home.join("ledger.db")).unwrap(),
        &ExportSelection {
            sessions: vec![SessionIdentity {
                source: "hermes".into(),
                session_id: SID.into(),
            }],
            kinds: kinds().iter().map(|k| k.as_str().into()).collect(),
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
    while let Some(c) = cursor {
        let page = snapshot.page(&c, 1_000).unwrap();
        cursor = page.next_cursor;
        records.extend(page.records);
    }
    assert_eq!(records.len(), 18);
    records
}
fn records(archive: &[HistoryExportRecord]) -> Vec<EvidenceRecord> {
    archive
        .iter()
        .map(|r| {
            let mut payload = r.payload.as_object().unwrap().clone();
            payload.remove("id");
            payload.remove("location");
            EvidenceRecord {
                kind: serde_json::from_value(json!(r.kind)).unwrap(),
                payload,
                record_id: Some(r.record_id.clone()),
                revision_id: Some(r.revision_id.clone()),
            }
        })
        .collect()
}
fn key() -> ObservationKey {
    ObservationKey {
        source: "hermes".into(),
        session_id: SID.into(),
        location: SessionLocation::Remote,
        connector_id: "hermes-transport".into(),
        connector_instance: "fixture".into(),
    }
}
fn observe(store: &SessionStore) {
    store
        .apply_source_observations(SourceObservationBatch::new(
            "hermes-transport",
            "fixture",
            SessionLocation::Remote,
            vec![SourceObservation {
                session: ShallowSession {
                    source: "hermes".into(),
                    session_id: SID.into(),
                    source_stamp: Some("native-archive".into()),
                    ..Default::default()
                },
                raw_locator: None,
            }],
        ))
        .unwrap();
}
fn apply(
    store: &SessionStore,
    records: Vec<EvidenceRecord>,
) -> Result<ai_hist::HydrateSessionResult, ai_hist::Error> {
    let revision = store.source_observation(&key()).unwrap().revision.unwrap();
    store.apply_source_evidence(
        SourceEvidenceSubmission::new(
            key(),
            revision,
            NormalizedSourceEvidence {
                source_stamp: "native-archive".into(),
                source_bytes: 0,
                covered_kinds: kinds(),
                records,
            },
        )
        .include_related(false),
    )
}
fn assert_same(actual: &SessionEvidence, expected: &SessionEvidence) {
    assert_eq!(actual.messages, expected.messages);
    assert_eq!(actual.tool_calls, expected.tool_calls);
    assert_eq!(actual.tool_results, expected.tool_results);
    assert_eq!(actual.markers, expected.markers);
}
fn assert_location(home: &Path, location: &str) {
    let conn = rusqlite::Connection::open(home.join("ledger.db")).unwrap();
    for table in ["session_events", "tool_calls", "session_markers"] {
        let locations: Vec<String> = conn
            .prepare(&format!(
                "SELECT DISTINCT location FROM {table} WHERE source='hermes' AND session_id=?1"
            ))
            .unwrap()
            .query_map([SID], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(locations, vec![location.to_string()], "{table}");
    }
}
fn roundtrip(archive: &[HistoryExportRecord], expected: &SessionEvidence, local_first: bool) {
    let home = tempfile::tempdir().unwrap();
    let store = open(home.path());
    if local_first {
        stage(home.path());
        hydrate(&store);
        assert_location(home.path(), "local");
    }
    observe(&store);
    apply(&store, records(archive)).unwrap();
    assert_native(&read(&store));
    assert_same(&read(&store), expected);
    assert_location(home.path(), if local_first { "local" } else { "remote" });
    if !local_first {
        stage(home.path());
        hydrate(&store);
    }
    assert_location(home.path(), if local_first { "local" } else { "both" });
    assert_same(&read(&store), expected);
    let before = store.head_revision().unwrap();
    let report = apply(&store, records(archive)).unwrap();
    assert_eq!(report.status, "unchanged");
    store
        .hydrate(&reference(), HydrateOptions::default())
        .unwrap();
    assert_same(&read(&store), expected);
    assert_no_evidence_changes(&store, before);
    // The connector fence refuses an old acquisition before any replacement.
    let before = store.head_revision().unwrap();
    let error = store
        .apply_source_evidence(SourceEvidenceSubmission::new(
            key(),
            "stale-revision",
            NormalizedSourceEvidence {
                source_stamp: "native-archive".into(),
                source_bytes: 0,
                covered_kinds: kinds(),
                records: records(archive),
            },
        ))
        .unwrap_err();
    assert_eq!(error.code(), "SOURCE_REVISION_CONFLICT");
    assert_same(&read(&store), expected);
    assert_eq!(store.head_revision().unwrap(), before);
    assert_location(home.path(), if local_first { "local" } else { "both" });
}
#[test]
fn hermes_native_export_preserves_tool_identity_in_both_acquisition_orders() {
    let origin = tempfile::tempdir().unwrap();
    stage(origin.path());
    let store = open(origin.path());
    hydrate(&store);
    let expected = read(&store);
    assert_native(&expected);
    let archive = export(origin.path());
    for local_first in [false, true] {
        roundtrip(&archive, &expected, local_first);
    }
}
#[test]
fn hermes_pre_intake_index_and_archive_work_without_reindexing() {
    let home = tempfile::tempdir().unwrap();
    let conn = rusqlite::Connection::open(home.path().join("ledger.db")).unwrap();
    conn.execute_batch(include_str!("fixtures/pre-intake-index.sql"))
        .unwrap();
    drop(conn);
    // No native home is staged: opening and exporting cannot silently reacquire it.
    let store = open(home.path());
    let before = store.head_revision().unwrap();
    let expected = read(&store);
    assert_native(&expected);
    let conn = rusqlite::Connection::open(home.path().join("ledger.db")).unwrap();
    let stamp:String=conn.query_row("SELECT source_stamp FROM session_hydration_checkpoints WHERE source='hermes' AND session_id=?1 AND location='local'",[SID],|r|r.get(0)).unwrap();
    assert!(stamp.starts_with("hermes_schema31_v4:"));
    let current = export(home.path());
    let old: Vec<HistoryExportRecord> =
        serde_json::from_str(include_str!("fixtures/pre-intake-export.json")).unwrap();
    assert_eq!(
        records(&current)
            .iter()
            .map(|r| &r.payload)
            .collect::<Vec<_>>(),
        records(&old).iter().map(|r| &r.payload).collect::<Vec<_>>()
    );
    assert_eq!(
        store.head_revision().unwrap(),
        before,
        "old index remains unchanged without force-sync"
    );
    for archive in [&old, &current] {
        roundtrip(archive, &expected, false);
    }
}
#[test]
fn hermes_export_rejects_malformed_call_ids_and_result_fields_on_non_results() {
    let archive: Vec<HistoryExportRecord> =
        serde_json::from_str(include_str!("fixtures/pre-intake-export.json")).unwrap();
    let native = records(&archive);
    let call = native
        .iter()
        .find(|r| r.payload["kind"] == "tool_use")
        .unwrap();
    let result = native
        .iter()
        .find(|r| r.payload["kind"] == "tool_result")
        .unwrap();
    let home = tempfile::tempdir().unwrap();
    let store = open(home.path());
    observe(&store);
    let before = store.head_revision().unwrap();
    for kind in ["text", "thinking"] {
        let mut invalid = call.clone();
        invalid.payload.insert("kind".into(), json!(kind));
        assert!(
            apply(&store, vec![invalid]).is_err(),
            "{kind} must not carry a tool ID"
        );
    }
    for field in [
        "payload_bytes",
        "payload_truncated",
        "payload_hash",
        "call_index",
        "event_index",
        "result_status",
        "event_source",
        "error_signal",
        "subagent_session_id",
        "agent_id",
    ] {
        let mut invalid = call.clone();
        let value = result
            .payload
            .get(field)
            .filter(|v| !v.is_null())
            .cloned()
            .unwrap_or_else(|| {
                if field == "error_signal" {
                    json!("tool_result.is_error")
                } else {
                    json!("fixture-id")
                }
            });
        invalid.payload.insert(field.into(), value);
        let error = apply(&store, vec![invalid]).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("is only valid on a tool_result event"),
            "{field} must fail its result-only contract: {error}"
        );
    }
    for invalid_id in [
        json!(true),
        json!(12),
        json!([]),
        json!({}),
        json!(""),
        json!(" \t"),
    ] {
        for event in [call, result] {
            let mut invalid = event.clone();
            invalid
                .payload
                .insert("tool_use_id".into(), invalid_id.clone());
            assert!(
                apply(&store, vec![invalid]).is_err(),
                "malformed identity {invalid_id}"
            );
        }
    }
    assert_eq!(
        store.head_revision().unwrap(),
        before,
        "invalid snapshots never mutate evidence"
    );
    apply(&store, native).unwrap();
    assert_native(&read(&store));
}

fn assert_no_evidence_changes(store: &SessionStore, before: ai_hist::Watermark) {
    // Observation checkpoints are bookkeeping; canonical message/tool/marker
    // changes are what would cause duplicated or rewritten exported history.
    let query = ai_hist::ChangeQuery::default().kinds([
        ai_hist::ChangeKind::SessionEvent,
        ai_hist::ChangeKind::ToolCall,
        ai_hist::ChangeKind::SessionMarker,
    ]);
    assert_eq!(store.changes_since(before, query).unwrap().count(), 0);
}
#[test]
fn hermes_conflicting_native_rewrite_rolls_back_remote_backed_evidence() {
    let archive: Vec<HistoryExportRecord> =
        serde_json::from_str(include_str!("fixtures/pre-intake-export.json")).unwrap();
    let home = tempfile::tempdir().unwrap();
    let store = open(home.path());
    observe(&store);
    apply(&store, records(&archive)).unwrap();
    let expected = read(&store);
    stage(home.path());
    let native = rusqlite::Connection::open(home.path().join(".hermes/state.db")).unwrap();
    native
        .execute(
            "UPDATE messages SET content='Conflicting native rewrite' WHERE id=1",
            [],
        )
        .unwrap();
    drop(native);
    store
        .discover(DiscoveryOptions::default().sources([Source::Hermes]))
        .unwrap();
    let before = store.head_revision().unwrap();
    let error = store
        .hydrate(&reference(), HydrateOptions::default())
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("HERMES_SHARED_EVIDENCE_CONFLICT"),
        "{error}"
    );
    assert_same(&read(&store), &expected);
    assert_location(home.path(), "remote");
    assert_no_evidence_changes(&store, before);
}
