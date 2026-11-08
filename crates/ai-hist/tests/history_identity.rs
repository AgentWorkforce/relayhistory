//! Transport identity must not collapse equal-time prompts or claim legacy rows.
use ai_hist::export::{ExportLimits, ExportSelection, ExportSnapshot, HistoryExportRecord};
use ai_hist::{
    ChangeKind, ChangeQuery, EvidenceKind, EvidenceRecord, NormalizedSourceEvidence,
    ObservationKey, SessionLocation, SessionStore, ShallowSession, SourceEvidenceSubmission,
    SourceObservation, SourceObservationBatch, StoreOptions, Watermark,
};
use rusqlite::Connection;
use serde_json::{json, Value};
use std::path::Path;

fn open(path: &Path) -> SessionStore {
    SessionStore::open(StoreOptions::default().db_path(path)).unwrap()
}
fn key(instance: &str) -> ObservationKey {
    ObservationKey {
        source: "deepagents".into(),
        session_id: "s".into(),
        location: SessionLocation::Remote,
        connector_id: "transport".into(),
        connector_instance: instance.into(),
    }
}
fn observe(store: &SessionStore, instance: &str) {
    store
        .apply_source_observations(SourceObservationBatch::new(
            "transport",
            instance,
            SessionLocation::Remote,
            vec![SourceObservation {
                session: ShallowSession {
                    source: "deepagents".into(),
                    session_id: "s".into(),
                    source_stamp: Some("native".into()),
                    ..Default::default()
                },
                raw_locator: None,
            }],
        ))
        .unwrap();
}
fn record(uid: Option<&str>, text: &str) -> EvidenceRecord {
    EvidenceRecord{kind:EvidenceKind::History,payload:json!({"source":"deepagents","session_id":"s","timestamp_ms":123,"prompt":text,"event_uid":uid}).as_object().unwrap().clone(),record_id:None,revision_id:None}
}
fn submit(
    store: &SessionStore,
    instance: &str,
    records: Vec<EvidenceRecord>,
) -> Result<(), ai_hist::Error> {
    let k = key(instance);
    let revision = store.source_observation(&k).unwrap().revision.unwrap();
    store
        .apply_source_evidence(
            SourceEvidenceSubmission::new(
                k,
                revision,
                NormalizedSourceEvidence {
                    source_stamp: "native".into(),
                    source_bytes: 0,
                    covered_kinds: vec![EvidenceKind::History],
                    records,
                },
            )
            .include_related(false),
        )
        .map(|_| ())
}
fn export(path: &Path) -> Vec<HistoryExportRecord> {
    let selection = ExportSelection {
        all_sources: true,
        kinds: vec!["history".into()],
        ..Default::default()
    };
    let limits = ExportLimits {
        max_batch_records: 1,
        ..Default::default()
    };
    let mut snapshot = ExportSnapshot::open(
        Connection::open(path).unwrap(),
        &selection,
        &limits,
        60_000,
        1_000,
    )
    .unwrap();
    let mut cursor = Some(snapshot.handle().cursor);
    let mut records = vec![];
    while let Some(next) = cursor {
        let page = snapshot.page(&next, 1_000).unwrap();
        records.extend(page.records);
        cursor = page.next_cursor;
    }
    records
}
fn replay(records: &[HistoryExportRecord]) -> Vec<EvidenceRecord> {
    records
        .iter()
        .map(|r| {
            let mut payload = r.payload.as_object().unwrap().clone();
            payload.remove("id");
            EvidenceRecord {
                kind: EvidenceKind::History,
                payload,
                record_id: Some(r.record_id.clone()),
                revision_id: Some(r.revision_id.clone()),
            }
        })
        .collect()
}
fn keys(store: &SessionStore) -> Vec<Vec<Value>> {
    store
        .changes_since(
            Watermark::START,
            ChangeQuery::default().kinds([ChangeKind::History]),
        )
        .unwrap()
        .map(|c| c.unwrap().key)
        .collect()
}

#[test]
fn equal_time_transport_preserves_native_keys_replay_rewrite_withdrawal_and_reopen() {
    let home = tempfile::tempdir().unwrap();
    let path = home.path().join("sender.db");
    let store = open(&path);
    observe(&store, "one");
    submit(
        &store,
        "one",
        vec![record(Some("a"), "retry"), record(Some("b"), "retry")],
    )
    .unwrap();
    let original = export(&path);
    assert_eq!(original.len(), 2);
    assert_ne!(original[0].record_id, original[1].record_id);
    let identities = keys(&store);
    assert_eq!(identities.len(), 2);
    let receiver_path = home.path().join("receiver.db");
    let receiver = open(&receiver_path);
    observe(&receiver, "one");
    submit(&receiver, "one", replay(&original)).unwrap();
    let remote = export(&receiver_path);
    assert_eq!(
        remote.iter().map(|r| &r.record_id).collect::<Vec<_>>(),
        original.iter().map(|r| &r.record_id).collect::<Vec<_>>()
    );
    let before = export(&receiver_path);
    submit(&receiver, "one", replay(&original)).unwrap();
    assert_eq!(export(&receiver_path), before);
    drop(receiver);
    let receiver = open(&receiver_path);
    assert_eq!(keys(&receiver), identities);
    submit(
        &receiver,
        "one",
        vec![record(Some("a"), "edited"), record(Some("b"), "retry")],
    )
    .unwrap();
    let edited = export(&receiver_path);
    assert_eq!(
        edited.iter().map(|r| &r.record_id).collect::<Vec<_>>(),
        original.iter().map(|r| &r.record_id).collect::<Vec<_>>()
    );
    observe(&receiver, "two");
    submit(&receiver, "two", replay(&edited)).unwrap();
    submit(&receiver, "one", vec![record(Some("b"), "retry")]).unwrap();
    assert_eq!(export(&receiver_path).len(), 2);
    submit(&receiver, "two", vec![]).unwrap();
    let remaining = export(&receiver_path);
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].payload["event_uid"], "b");
    submit(&receiver, "one", vec![]).unwrap();
    assert!(export(&receiver_path).is_empty());
}

#[test]
fn anonymous_identity_stays_distinct_and_rejects_duplicate_native_snapshot_transactionally() {
    let home = tempfile::tempdir().unwrap();
    let path = home.path().join("ledger.db");
    let store = open(&path);
    observe(&store, "one");
    submit(
        &store,
        "one",
        vec![
            record(None, "retry"),
            record(Some("a"), "retry"),
            record(Some("b"), "retry"),
        ],
    )
    .unwrap();
    let exported = export(&path);
    assert_eq!(exported.len(), 3);
    let legacy = record(None, "retry");
    assert_eq!(
        legacy.identity(),
        json!(["history", ["deepagents", 123, "retry"]]).to_string()
    );
    let head = store.head_revision().unwrap();
    assert!(submit(
        &store,
        "one",
        vec![record(Some("a"), "first"), record(Some("a"), "second")]
    )
    .is_err());
    assert!(submit(&store, "one", vec![record(Some(""), "bad")]).is_err());
    assert_eq!(store.head_revision().unwrap(), head);
    assert_eq!(export(&path), exported);
    submit(
        &store,
        "one",
        vec![record(Some("a"), "retry"), record(Some("b"), "retry")],
    )
    .unwrap();
    assert_eq!(
        export(&path).len(),
        2,
        "legacy withdrawal must not erase identified native prompts"
    );
}
