//! Retiring a local source cannot discard its still-supported recorded order.
use super::*;
use serde_json::json;

fn events(known: bool) -> Vec<EvidenceRecord> {
    let mut rows = ["z", "a", "m"].into_iter().enumerate().map(|(rank,uid)| {
        let mut payload=json!({"source":"claude","session_id":"retained","event_uid":uid,"message_id":uid,"role":"assistant","kind":"text","ts_ms":1,"text":uid}).as_object().unwrap().clone();
        if known {payload.insert("source_order".into(),json!(rank));}
        EvidenceRecord{kind:EvidenceKind::SessionEvent,payload,record_id:None,revision_id:None}
    }).collect::<Vec<_>>();
    source_evidence::validate_records(
        &key(SessionLocation::Remote, "remote"),
        &[EvidenceKind::SessionEvent],
        &mut rows,
    )
    .unwrap();
    rows
}
fn ordered(conn: &Connection) -> Vec<(String, Option<i64>)> {
    conn.prepare("SELECT event_uid,source_order FROM session_events WHERE source='claude' AND session_id='retained' ORDER BY ts_ms,COALESCE(source_order,id),id").unwrap().query_map([],|row|Ok((row.get(0)?,row.get(1)?))).unwrap().collect::<rusqlite::Result<_>>().unwrap()
}
fn expected() -> Vec<(String, Option<i64>)> {
    vec![
        ("z".into(), Some(0)),
        ("a".into(), Some(1)),
        ("m".into(), Some(2)),
    ]
}

#[test]
fn legacy_retirement_retains_native_order_through_reopen_replay_and_withdrawal() {
    let (dir, mut conn) = setup();
    local(&conn, &events(true));
    let remote = key(SessionLocation::Remote, "remote");
    observe(&conn, &remote);
    submit(&mut conn, &remote, events(false));
    // Emulate a retained snapshot written before source_order existed at all.
    conn.execute("UPDATE observation_evidence SET payload_json=json_remove(payload_json,'$.item.record.payload.source_order')",[]).unwrap();
    let snapshot = observations::evidence(&conn, &remote).unwrap();
    let revision = observations::revision(&conn, &remote).unwrap();
    retire(&conn).unwrap();
    assert_eq!(ordered(&conn), expected());
    assert_eq!(observations::evidence(&conn, &remote).unwrap(), snapshot);
    assert_eq!(observations::revision(&conn, &remote).unwrap(), revision);
    drop(conn);
    let mut conn = crate::open_db(&dir.path().join("history.db")).unwrap();
    // An unrelated partial-kind update still rewrites the union; absent ranks
    // cannot erase the canonical overlay restored from the local source.
    let revision = observations::revision(&conn, &remote).unwrap().unwrap();
    apply_normalized(
        &mut conn,
        &remote,
        &revision,
        NormalizedSourceEvidence {
            source_stamp: "partial".into(),
            source_bytes: 0,
            covered_kinds: vec![EvidenceKind::ToolCall],
            records: vec![],
        },
        false,
        Instant::now(),
    )
    .unwrap();
    assert_eq!(ordered(&conn), expected());
    submit(&mut conn, &remote, events(false));
    assert_eq!(ordered(&conn), expected());
    submit(&mut conn, &remote, vec![]);
    assert!(ordered(&conn).is_empty());
    submit(&mut conn, &remote, events(false));
    assert!(
        ordered(&conn).iter().all(|(_, rank)| rank.is_none()),
        "withdrawn support cannot lend an old rank to reacquired unknown evidence"
    );
}

#[test]
fn a_legacy_timestamp_rewrite_does_not_inherit_retired_order() {
    let (_dir, mut conn) = setup();
    local(&conn, &events(true));
    let remote = key(SessionLocation::Remote, "remote");
    observe(&conn, &remote);
    submit(&mut conn, &remote, events(false));
    retire(&conn).unwrap();
    let mut changed = events(false);
    for row in &mut changed {
        row.payload.insert("ts_ms".into(), json!(2));
    }
    submit(&mut conn, &remote, changed);
    assert!(ordered(&conn).iter().all(|(_, rank)| rank.is_none()));
}

#[test]
fn conflicting_snapshots_never_receive_canonical_order() {
    for conflict in ["text", "ts_ms", "source_order"] {
        let (_dir, mut conn) = setup();
        local(&conn, &events(true));
        let matching = key(SessionLocation::Remote, "a-legacy");
        observe(&conn, &matching);
        submit(&mut conn, &matching, events(false));
        let conflicting = key(SessionLocation::Remote, "z-conflict");
        observe(&conn, &conflicting);
        let mut changed = events(false);
        for row in &mut changed {
            row.payload.insert(
                conflict.into(),
                match conflict {
                    "text" => json!("conflicting"),
                    "ts_ms" => json!(2),
                    _ => json!(9),
                },
            );
        }
        if conflict == "source_order" {
            // Keep the remote snapshot internally valid while contradicting
            // the local event ranks rather than colliding within itself.
            for (n, row) in changed.iter_mut().enumerate() {
                row.payload
                    .insert("source_order".into(), json!(n as i64 + 10));
            }
            let revision = observations::revision(&conn, &conflicting)
                .unwrap()
                .unwrap();
            let err = apply_normalized(
                &mut conn,
                &conflicting,
                &revision,
                NormalizedSourceEvidence {
                    source_stamp: "bad-order".into(),
                    source_bytes: 0,
                    covered_kinds: vec![EvidenceKind::SessionEvent],
                    records: changed.clone(),
                },
                false,
                Instant::now(),
            )
            .unwrap_err();
            assert!(format!("{err:#}").contains("SOURCE_ORDER_CONFLICT"));
            // A previously retained known assertion can become stale after
            // native local order changes. Capture must prefer the matching
            // legacy proof without borrowing the conflicting assertion's rank.
            let stale = RecordSnapshot {
                format: "records".into(),
                covered_kinds: vec![EvidenceKind::SessionEvent],
                acquired_kinds: None,
                records: changed
                    .iter()
                    .cloned()
                    .map(|record| ManagedRecord {
                        record,
                        managed: false,
                    })
                    .collect(),
            };
            observations::save_evidence(&conn, &conflicting, &serde_json::to_value(stale).unwrap())
                .unwrap();
        } else {
            submit(&mut conn, &conflicting, changed.clone());
        }
        retire(&conn).unwrap();
        assert_eq!(ordered(&conn), expected(), "{conflict}");
        if conflict == "source_order" {
            // A retained contradictory known assertion must reject a union
            // change atomically rather than inherit or replace the overlay.
            let revision = observations::revision(&conn, &matching).unwrap().unwrap();
            let result = apply_normalized(
                &mut conn,
                &matching,
                &revision,
                NormalizedSourceEvidence {
                    source_stamp: "withdraw".into(),
                    source_bytes: 0,
                    covered_kinds: vec![EvidenceKind::SessionEvent],
                    records: vec![],
                },
                false,
                Instant::now(),
            );
            assert!(format!("{:#}", result.unwrap_err()).contains("SOURCE_ORDER_CONFLICT"));
            assert_eq!(ordered(&conn), expected());
            submit(&mut conn, &conflicting, vec![]);
        }
        submit(&mut conn, &matching, vec![]);
        assert!(ordered(&conn).is_empty());
        if conflict != "source_order" {
            submit(&mut conn, &conflicting, changed);
            assert!(ordered(&conn).is_empty());
        }
    }
}
