mod provider_support;
#[cfg(feature = "export")]
use ai_hist::export::{ExportLimits, ExportSelection, ExportSnapshot, SessionIdentity};
use ai_hist::{
    DiscoveryOptions, HydrateOptions, ProviderRoots, Role, SessionRef, SessionStore, Source,
    StoreOptions, SyncOptions,
};
#[cfg(feature = "export")]
use ai_hist::{
    EvidenceKind, EvidenceRecord, NormalizedSourceEvidence, ObservationKey, SessionLocation,
    SessionQuery, ShallowSession, SourceEvidenceSubmission, SourceObservation,
    SourceObservationBatch,
};
use provider_support::{assert_history_conformance, HistoryCase};

const SID: &str =
    "default:30313961303030302d303030302d373030302d383030302d303030303030303030343433";

fn stage(home: &std::path::Path) -> std::path::PathBuf {
    let path = home.join(".deepagents/.state/sessions.db");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch(include_str!("fixtures/deepagents/sessions.sql"))
        .unwrap();
    path
}

fn open(home: &std::path::Path) -> SessionStore {
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

#[test]
fn deepagents_public_history_conformance() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path());
    let evidence = assert_history_conformance(
        home.path(),
        HistoryCase {
            source: Source::DeepAgents,
            session_id: SID,
            messages: &[
                (Role::User, "Find the DeepAgents\nfixture needle"),
                (Role::Assistant, "Looking now"),
                (Role::User, "Continue after restart"),
                (Role::Assistant, "History survived restart."),
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
    assert!(evidence.requests.iter().all(|r| r.usage.is_none()));
}

#[cfg(feature = "unstable-internal")]
#[test]
fn decoded_prompt_is_searchable_by_term_and_phrase() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path());
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::DeepAgents]))
        .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::DeepAgents, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    let conn = rusqlite::Connection::open(store.db_path()).unwrap();
    let filter = ai_hist::QueryFilter {
        source: Some("deepagents".into()),
        ..Default::default()
    };
    for (terms, raw) in [
        (vec!["needle".into()], false),
        (vec!["\"DeepAgents fixture\"".into()], true),
    ] {
        let hits = ai_hist::search(&conn, &terms, raw, &filter).unwrap();
        assert!(
            hits.iter()
                .any(|hit| hit.prompt == "Find the DeepAgents\nfixture needle"),
            "decoded native prompt missing for {terms:?}"
        );
    }
}

#[test]
fn cloned_native_thread_ids_remain_distinct_across_explicit_roots() {
    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let first_path = stage(first.path());
    let second_path = stage(second.path());
    let mut roots = serde_json::to_value(ProviderRoots::from_home(
        first.path().into(),
        first.path().join("opencode.db"),
    ))
    .unwrap();
    roots["provider_roots"]["deepagents"] = serde_json::json!([first_path, second_path]);
    let roots: ProviderRoots = serde_json::from_value(roots).unwrap();
    let store = SessionStore::open(
        StoreOptions::default()
            .db_path(first.path().join("ledger.db"))
            .roots(roots),
    )
    .unwrap();
    store
        .discover(DiscoveryOptions::default().sources([Source::DeepAgents]))
        .unwrap();
    let mut query = ai_hist::CatalogQuery::default();
    query.sources = Some(vec![Source::DeepAgents]);
    let ids = store
        .sessions(query)
        .map(|row| row.unwrap().session_id)
        .filter(|id| id.ends_with("343433"))
        .collect::<Vec<_>>();
    assert_eq!(ids.len(), 2);
    assert_ne!(ids[0], ids[1]);
    assert!(ids.iter().any(|id| id.starts_with("default:")));
    assert!(ids.iter().any(|id| id.starts_with("root:")));
}

#[cfg(feature = "export")]
#[test]
fn bounded_native_export_remote_intake_and_local_deletion_preserve_evidence() {
    let origin = tempfile::tempdir().unwrap();
    stage(origin.path());
    let sender = open(origin.path());
    sender
        .discover(DiscoveryOptions::default().sources([Source::DeepAgents]))
        .unwrap();
    sender
        .hydrate(
            &SessionRef::id(Source::DeepAgents, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    let original = sender
        .session(
            &SessionRef::id(Source::DeepAgents, SID),
            SessionQuery::default(),
        )
        .unwrap()
        .unwrap();
    let kinds = vec![
        EvidenceKind::History,
        EvidenceKind::SessionEvent,
        EvidenceKind::ToolCall,
        EvidenceKind::SessionMarker,
    ];
    let selection = ExportSelection {
        sessions: vec![SessionIdentity {
            source: "deepagents".into(),
            session_id: SID.into(),
        }],
        kinds: kinds.iter().map(|kind| kind.as_str().into()).collect(),
        ..Default::default()
    };
    let mut snapshot = ExportSnapshot::open(
        rusqlite::Connection::open(sender.db_path()).unwrap(),
        &selection,
        &ExportLimits {
            max_batch_records: 1,
            max_batch_bytes: 2048,
            ..Default::default()
        },
        60_000,
        1_000,
    )
    .unwrap();
    let mut cursor = Some(snapshot.handle().cursor);
    let mut rows = Vec::new();
    while let Some(value) = cursor {
        let page = snapshot.page(&value, 1_000).unwrap();
        cursor = page.next_cursor;
        assert!(page
            .records
            .iter()
            .all(|record| serde_json::to_vec(record).unwrap().len() <= 2048));
        rows.extend(page.records);
    }
    assert!(rows.iter().any(|row| row.kind == "tool_call"));
    assert!(rows.iter().any(|row| row.kind == "session_marker"));
    fn bounded(value: &serde_json::Value) {
        match value {
            serde_json::Value::String(text) => assert!(text.chars().count() <= 128),
            serde_json::Value::Array(items) => {
                assert!(items.len() <= 32);
                for item in items {
                    bounded(item);
                }
            }
            serde_json::Value::Object(fields) => {
                assert!(fields.len() <= 32);
                for (key, value) in fields {
                    assert!(key.chars().count() <= 128);
                    bounded(value);
                }
            }
            _ => {}
        }
    }
    for row in rows.iter().filter(|row| row.kind == "session_marker") {
        if let Some(payload) = row.payload["payload_json"].as_str() {
            bounded(&serde_json::from_str(payload).unwrap());
        }
    }
    let records = rows
        .into_iter()
        .map(|row| {
            let mut payload = row.payload.as_object().unwrap().clone();
            payload.remove("id");
            payload.remove("location");
            EvidenceRecord {
                kind: serde_json::from_value(serde_json::json!(row.kind)).unwrap(),
                payload,
                record_id: Some(row.record_id),
                revision_id: Some(row.revision_id),
            }
        })
        .collect();
    let remote_home = tempfile::tempdir().unwrap();
    let receiver = open(remote_home.path());
    receiver
        .apply_source_observations(SourceObservationBatch::new(
            "deepagents-transport",
            "fixture",
            SessionLocation::Remote,
            vec![SourceObservation {
                session: ShallowSession {
                    source: "deepagents".into(),
                    session_id: SID.into(),
                    source_stamp: Some("deepagents-native-archive".into()),
                    ..Default::default()
                },
                raw_locator: None,
            }],
        ))
        .unwrap();
    let key = ObservationKey {
        source: "deepagents".into(),
        session_id: SID.into(),
        location: SessionLocation::Remote,
        connector_id: "deepagents-transport".into(),
        connector_instance: "fixture".into(),
    };
    let revision = receiver.source_observation(&key).unwrap().revision.unwrap();
    receiver
        .apply_source_evidence(
            SourceEvidenceSubmission::new(
                key,
                revision,
                NormalizedSourceEvidence {
                    source_stamp: "deepagents-native-archive".into(),
                    source_bytes: 0,
                    covered_kinds: kinds,
                    records,
                },
            )
            .include_related(false),
        )
        .unwrap();
    let reference = SessionRef::id(Source::DeepAgents, SID);
    let imported = receiver
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    assert_eq!(imported.messages.len(), original.messages.len());
    assert_eq!(imported.tool_calls, original.tool_calls);
    assert_eq!(imported.tool_results, original.tool_results);
    #[cfg(feature = "unstable-internal")]
    {
        let conn = rusqlite::Connection::open(receiver.db_path()).unwrap();
        let hits = ai_hist::search(
            &conn,
            &["\"DeepAgents fixture\"".into()],
            true,
            &ai_hist::QueryFilter {
                source: Some("deepagents".into()),
                scope: ai_hist::SessionScope::All,
                ..Default::default()
            },
        )
        .unwrap();
        assert!(hits
            .iter()
            .any(|hit| hit.prompt == "Find the DeepAgents\nfixture needle"));
    }
    let native = stage(remote_home.path());
    receiver
        .discover(DiscoveryOptions::default().sources([Source::DeepAgents]))
        .unwrap();
    // Shared order has no durable native ordinal yet (#532); a local replay
    // may reject it, but must leave the remote snapshot intact.
    if let Err(error) = receiver.hydrate(&reference, HydrateOptions::default()) {
        assert!(format!("{error:#}").contains("FILE_HISTORY_SHARED_ORDER_CONFLICT"));
    }
    let db = rusqlite::Connection::open(&native).unwrap();
    db.execute("INSERT INTO writes (thread_id,checkpoint_ns,checkpoint_id,task_id,task_path,idx,channel,type,value) SELECT thread_id,checkpoint_ns,checkpoint_id,'zz-remote-append','',idx+10,channel,type,(SELECT value FROM writes WHERE thread_id LIKE '%446' AND channel='messages' ORDER BY checkpoint_id LIMIT 1) FROM writes WHERE thread_id LIKE '%443' LIMIT 1", []).unwrap();
    drop(db);
    if let Err(error) = receiver.hydrate(&reference, HydrateOptions::default()) {
        assert!(format!("{error:#}").contains("FILE_HISTORY_SHARED_ORDER_CONFLICT"));
    }
    let before_delete = receiver
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    assert!(before_delete.messages.len() >= imported.messages.len());
    std::fs::remove_file(&native).unwrap();
    receiver.sync(SyncOptions::default().force(true)).unwrap();
    let retained = receiver
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    assert_eq!(retained.messages.len(), before_delete.messages.len());
    assert_eq!(retained.tool_calls, imported.tool_calls);
    assert_eq!(retained.tool_results, imported.tool_results);
}
