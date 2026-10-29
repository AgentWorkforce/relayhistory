//! Order is captured evidence, independent of identity and array/storage order.
use ai_hist::{
    CatalogQuery, EvidenceKind, EvidenceRecord, NormalizedSourceEvidence, ObservationKey,
    ProviderRoots, SessionEventCursor, SessionLocation, SessionQuery, SessionRef, SessionStore,
    ShallowSession, Source, SourceEvidenceSubmission, SourceObservation, SourceObservationBatch,
    StoreOptions,
};
use serde_json::{json, Value};

const SID: &str = "order-test";
fn open(home: &std::path::Path) -> SessionStore {
    let mut options = StoreOptions::default();
    options.home = Some(home.to_path_buf());
    options.roots = Some(ProviderRoots::from_home(
        home.to_path_buf(),
        home.join("empty.db"),
    ));
    SessionStore::open(options).unwrap()
}
fn key(connector: &str) -> ObservationKey {
    ObservationKey {
        source: "claude".into(),
        session_id: SID.into(),
        location: SessionLocation::Remote,
        connector_id: connector.into(),
        connector_instance: "default".into(),
    }
}
fn record(uid: &str, role: &str, rank: Option<i64>, named: bool) -> EvidenceRecord {
    EvidenceRecord {
        kind: EvidenceKind::SessionEvent,
        record_id: None,
        revision_id: None,
        payload: json!({"source":"claude","session_id":SID,"event_uid":uid,
            "message_id":named.then_some(uid),"ts_ms":100,"role":role,"kind":"text",
            "text":uid,"source_order":rank})
        .as_object()
        .unwrap()
        .clone(),
    }
}
fn native() -> Vec<EvidenceRecord> {
    vec![
        record("z-user", "user", Some(0), true),
        record("a-reply", "assistant", Some(1), true),
        record("y-user", "user", Some(2), false),
        record("b-reply", "assistant", Some(3), true),
    ]
}
fn observe(store: &SessionStore, connector: &str) -> String {
    store
        .apply_source_observations(SourceObservationBatch::new(
            connector,
            "default",
            SessionLocation::Remote,
            vec![SourceObservation {
                session: ShallowSession {
                    source: "claude".into(),
                    session_id: SID.into(),
                    source_stamp: Some("fixture".into()),
                    ..Default::default()
                },
                raw_locator: Some("fixture:order".into()),
            }],
        ))
        .unwrap();
    store
        .source_observation(&key(connector))
        .unwrap()
        .revision
        .unwrap()
}
fn submit(
    store: &SessionStore,
    connector: &str,
    records: Vec<EvidenceRecord>,
) -> Result<ai_hist::HydrateSessionResult, ai_hist::Error> {
    let revision = observe(store, connector);
    store.apply_source_evidence(SourceEvidenceSubmission::new(
        key(connector),
        revision,
        NormalizedSourceEvidence {
            source_stamp: "fixture".into(),
            source_bytes: 0,
            covered_kinds: vec![EvidenceKind::SessionEvent],
            records,
        },
    ))
}
fn evidence(store: &SessionStore) -> ai_hist::SessionEvidence {
    let mut query = SessionQuery::default();
    query.include_text = true;
    store
        .session(&SessionRef::id(Source::Claude, SID), query)
        .unwrap()
        .unwrap()
}
fn ids(store: &SessionStore) -> Vec<(String, i64)> {
    let conn = rusqlite::Connection::open(store.db_path()).unwrap();
    let mut stmt = conn
        .prepare("SELECT event_uid,id FROM session_events WHERE session_id=?1 ORDER BY event_uid")
        .unwrap();
    stmt.query_map([SID], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}
fn uids(store: &SessionStore) -> Vec<String> {
    evidence(store)
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .map(|b| b.event_uid.clone())
        .collect()
}
#[test]
fn ordered_snapshot_replays_and_rewrites_in_place_with_user_turn_neighbors_and_pagination() {
    let home = tempfile::tempdir().unwrap();
    let store = open(home.path());
    let mut incoming = native();
    incoming.reverse();
    submit(&store, "first", incoming.clone()).unwrap();
    assert_eq!(uids(&store), ["z-user", "a-reply", "y-user", "b-reply"]);
    let original_ids = ids(&store);
    let original = evidence(&store);
    assert_eq!(original.user_turns.len(), 2);
    assert_eq!(
        original.user_turns[0].following_message_id.as_deref(),
        Some("a-reply")
    );
    assert_eq!(
        original.user_turns[1].preceding_message_id.as_deref(),
        Some("a-reply")
    );
    assert_eq!(
        original.user_turns[1].following_message_id.as_deref(),
        Some("b-reply")
    );
    let conn = rusqlite::Connection::open(store.db_path()).unwrap();
    let mut after = None;
    let mut paged = vec![];
    loop {
        let page =
            ai_hist::session_events_page(&conn, SID, Some("claude"), 1, after.as_ref()).unwrap();
        paged.extend(page.events.iter().map(|e| e.event_uid.clone()));
        after = page.next_cursor;
        if after.is_none() {
            break;
        }
    }
    assert_eq!(paged, uids(&store));
    let first = ai_hist::session_user_turns_page(&conn, "claude", SID, 1, None).unwrap();
    let second =
        ai_hist::session_user_turns_page(&conn, "claude", SID, 1, first.next_cursor.as_ref())
            .unwrap();
    assert_eq!(
        [first.user_turns, second.user_turns].concat(),
        original.user_turns
    );
    submit(&store, "first", incoming).unwrap();
    assert_eq!(ids(&store), original_ids);
    // Swap replies across user boundaries without changing native IDs or times.
    let mut rewrite = native();
    rewrite[1].payload.insert("source_order".into(), json!(3));
    rewrite[3].payload.insert("source_order".into(), json!(1));
    submit(&store, "first", rewrite.clone()).unwrap();
    assert_eq!(uids(&store), ["z-user", "b-reply", "y-user", "a-reply"]);
    assert_eq!(ids(&store), original_ids);
    assert_eq!(
        evidence(&store).user_turns[0]
            .following_message_id
            .as_deref(),
        Some("b-reply")
    );
    // Removing a record does not renumber identities or force a dense rank set.
    rewrite.retain(|r| r.payload["event_uid"] != "a-reply");
    submit(&store, "first", rewrite.clone()).unwrap();
    assert_eq!(uids(&store), ["z-user", "b-reply", "y-user"]);
    let remaining = ids(&store);
    assert!(remaining.iter().all(|row| original_ids.contains(row)));
    submit(&store, "first", rewrite).unwrap();
    assert_eq!(ids(&store), remaining);
    drop(store);
    let store = open(home.path());
    assert_eq!(uids(&store), ["z-user", "b-reply", "y-user"]);
}
#[test]
fn conflicting_observation_order_rolls_back_snapshot_revision_and_canonical_changes() {
    let home = tempfile::tempdir().unwrap();
    let store = open(home.path());
    submit(&store, "first", native()).unwrap();
    submit(&store, "second", native()).unwrap();
    let before = evidence(&store);
    let canonical_ids = ids(&store);
    let revision = store.source_observation(&key("second")).unwrap().revision;
    let mut conflicting = native();
    conflicting[0]
        .payload
        .insert("source_order".into(), json!(1));
    conflicting[1]
        .payload
        .insert("source_order".into(), json!(0));
    let error = submit(&store, "second", conflicting).unwrap_err();
    assert!(error.to_string().contains("SOURCE_ORDER_CONFLICT"));
    assert_eq!(evidence(&store).messages, before.messages);
    assert_eq!(ids(&store), canonical_ids);
    assert_eq!(
        store.source_observation(&key("second")).unwrap().revision,
        revision
    );
    // Failed replacement did not discard the second connector's old assertion.
    submit(&store, "first", native()).unwrap();
}
#[test]
fn legacy_unknown_order_is_not_inferred_from_snapshot_array_position_and_mixed_order_is_explicit() {
    let home = tempfile::tempdir().unwrap();
    let store = open(home.path());
    let mut legacy = native();
    for row in &mut legacy {
        row.payload.remove("source_order");
    }
    submit(&store, "legacy", legacy.clone()).unwrap();
    assert!(evidence(&store)
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .all(|b| b.source_order.is_none()));
    let unknown = uids(&store);
    legacy.reverse();
    submit(&store, "legacy", legacy).unwrap();
    assert_eq!(uids(&store), unknown);
    // Independent known evidence can add knowledge without inventing values for
    // the unknown rows. Known order is never cleared by a legacy snapshot.
    let mut mixed = native();
    mixed[2].payload.insert("source_order".into(), Value::Null);
    submit(&store, "legacy", mixed).unwrap();
    let blocks: Vec<_> = evidence(&store)
        .messages
        .into_iter()
        .flat_map(|m| m.blocks)
        .collect();
    assert!(blocks
        .iter()
        .any(|b| b.event_uid == "y-user" && b.source_order.is_none()));
    let conn = rusqlite::Connection::open(store.db_path()).unwrap();
    let cursor = SessionEventCursor {
        ts_ms: 100,
        id: 99999,
    };
    let error =
        ai_hist::session_events_page(&conn, SID, Some("claude"), 1, Some(&cursor)).unwrap_err();
    assert!(error.to_string().contains("STALE_EVENT_CURSOR"));
    let mut wrong = CatalogQuery::default();
    wrong.sources = Some(vec![Source::Codex]);
    assert!(store.sessions(wrong).next().is_none());
}

#[test]
fn partial_kind_submissions_retain_event_order_and_protected_canonical_order_conflicts() {
    let home = tempfile::tempdir().unwrap();
    let store = open(home.path());
    submit(&store, "first", native()).unwrap();
    let revision = store
        .source_observation(&key("first"))
        .unwrap()
        .revision
        .unwrap();
    store
        .apply_source_evidence(SourceEvidenceSubmission::new(
            key("first"),
            revision,
            NormalizedSourceEvidence {
                source_stamp: "partial".into(),
                source_bytes: 0,
                covered_kinds: vec![EvidenceKind::SessionMarker],
                records: vec![],
            },
        ))
        .unwrap();
    assert_eq!(uids(&store), ["z-user", "a-reply", "y-user", "b-reply"]);
    // An external canonical mutation revokes ownership; order disagreement
    // cannot be silently ignored or overwritten during the next intake.
    let conn = rusqlite::Connection::open(store.db_path()).unwrap();
    conn.execute(
        "UPDATE session_events SET source_order=90 WHERE event_uid='z-user'",
        [],
    )
    .unwrap();
    let canonical_ids = ids(&store);
    let error = submit(&store, "first", native()).unwrap_err();
    assert!(error.to_string().contains("SOURCE_ORDER_CONFLICT"));
    assert_eq!(ids(&store), canonical_ids);
    let rank: i64 = conn
        .query_row(
            "SELECT source_order FROM session_events WHERE event_uid='z-user'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(rank, 90);
}

#[cfg(feature = "export")]
fn exported(store: &SessionStore) -> Vec<EvidenceRecord> {
    use ai_hist::export::{ExportLimits, ExportSelection, ExportSnapshot};
    let mut snapshot = ExportSnapshot::open(
        rusqlite::Connection::open(store.db_path()).unwrap(),
        &ExportSelection {
            sources: vec!["claude".into()],
            kinds: vec!["session_event".into()],
            ..Default::default()
        },
        &ExportLimits::default(),
        60_000,
        1_000,
    )
    .unwrap();
    let mut cursor = Some(snapshot.handle().cursor);
    let mut records = vec![];
    while let Some(token) = cursor {
        let page = snapshot.page(&token, 1).unwrap();
        cursor = page.next_cursor;
        for record in page.records {
            let mut payload = record.payload.as_object().unwrap().clone();
            payload.remove("id");
            payload.remove("location");
            records.push(EvidenceRecord {
                kind: EvidenceKind::SessionEvent,
                payload,
                record_id: Some(record.record_id),
                revision_id: Some(record.revision_id),
            });
        }
    }
    records
}
#[cfg(feature = "export")]
#[test]
fn one_record_paged_export_intake_reopen_reexport_retains_durable_order() {
    let source_home = tempfile::tempdir().unwrap();
    let source = open(source_home.path());
    submit(&source, "fixture", native()).unwrap();
    let original = exported(&source);
    let receiver_home = tempfile::tempdir().unwrap();
    let receiver = open(receiver_home.path());
    let mut shuffled = original.clone();
    shuffled.reverse();
    submit(&receiver, "transport", shuffled).unwrap();
    drop(receiver);
    let receiver = open(receiver_home.path());
    assert_eq!(uids(&receiver), ["z-user", "a-reply", "y-user", "b-reply"]);
    let ordered_values = |records: Vec<EvidenceRecord>| {
        let mut values: Vec<_> = records
            .iter()
            .map(|r| {
                (
                    r.payload["event_uid"].as_str().unwrap().to_owned(),
                    r.payload["source_order"].as_i64(),
                )
            })
            .collect();
        values.sort();
        values
    };
    assert_eq!(
        ordered_values(exported(&receiver)),
        ordered_values(original)
    );
    let replay = exported(&receiver);
    let before = ids(&receiver);
    submit(&receiver, "transport", replay).unwrap();
    assert_eq!(ids(&receiver), before);
}
