use super::*;
use crate::observations::SessionObservation;

fn key(location: SessionLocation, connector: &str) -> ObservationKey {
    ObservationKey { source: "claude".into(), session_id: "retained".into(), location, connector_id: connector.into(), connector_instance: "default".into() }
}
fn observe(conn: &Connection, key: &ObservationKey) {
    observations::upsert(conn, &SessionObservation { key: key.clone(), raw_locator: Some("synthetic".into()), source_stamp: Some("one".into()), discovery_state: "full".into(), access_state: "available".into(), updated_ms: 1, first_prompt: None, last_assistant_text: None }).unwrap();
}
fn records(text: &str) -> Vec<EvidenceRecord> {
    let mut records = [(EvidenceKind::SessionEvent, serde_json::json!({"source":"claude","session_id":"retained","event_uid":"user:0","message_id":"user","role":"user","kind":"text","ts_ms":1,"text":text})),
     (EvidenceKind::History, serde_json::json!({"source":"claude","session_id":"retained","timestamp_ms":1,"prompt":text})),
     (EvidenceKind::ToolCall, serde_json::json!({"source":"claude","session_id":"retained","tool_use_id":"tool","name":"Read","args_json":text}))]
    .into_iter().map(|(kind,payload)| EvidenceRecord { kind, payload: payload.as_object().unwrap().clone(), record_id: None, revision_id: None }).collect::<Vec<_>>();
    source_evidence::validate_records(&key(SessionLocation::Remote,"remote"), &[EvidenceKind::SessionEvent,EvidenceKind::History,EvidenceKind::ToolCall], &mut records).unwrap();
    records
}
fn submit(conn: &mut Connection, key: &ObservationKey, records: Vec<EvidenceRecord>) {
    let revision = observations::revision(conn,key).unwrap().unwrap();
    apply_normalized(conn,key,&revision,NormalizedSourceEvidence { source_stamp:"one".into(),source_bytes:1,covered_kinds:vec![EvidenceKind::SessionEvent,EvidenceKind::History,EvidenceKind::ToolCall],records },false,Instant::now()).unwrap();
}
fn setup() -> (tempfile::TempDir, Connection) {
    let dir = tempfile::tempdir().unwrap();
    let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
    conn.execute("INSERT INTO sessions(source,session_id) VALUES('claude','retained')",[]).unwrap();
    (dir,conn)
}
fn local(conn: &Connection, records: &[EvidenceRecord]) {
    observe(conn,&key(SessionLocation::Local,"claude"));
    for record in records { record.write(conn,SessionLocation::Local).unwrap(); }
}
fn retire(conn: &Connection) -> Result<()> {
    let tx = conn.unchecked_transaction()?;
    crate::providers::file_history::retire_local_session(&tx,"claude","retained")?;
    tx.commit()?;
    Ok(())
}
#[test]
fn local_first_retirement_keeps_matching_remote_evidence() {
    let (dir,mut conn)=setup();
    let records=records("original");
    local(&conn,&records);
    let remote=key(SessionLocation::Remote,"remote");
    observe(&conn,&remote);
    submit(&mut conn,&remote,records.clone());
    retire(&conn).unwrap();
    drop(conn);
    let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
    for record in &records { assert!(record.matches_canonical(&conn).unwrap(),"missing {:?}",record.kind); }
}

#[test]
fn arrival_orders_conflicts_and_remote_withdrawal() {
    for remote_first in [false,true] {
        for conflict in [false,true] {
            let (_dir,mut conn)=setup();
            let original=records("original");
            let remote=key(SessionLocation::Remote,"remote");
            observe(&conn,&remote);
            if !remote_first { local(&conn,&original); }
            let acquired=if conflict { records("conflicting") } else { original.clone() };
            submit(&mut conn,&remote,acquired.clone());
            if remote_first { local(&conn,&original); }
            // The local value remains authoritative before retirement.
            assert!(original[0].matches_canonical(&conn).unwrap());
            // History has a different unique key when prompt text changes;
            // both independently recorded prompts are legitimate evidence.
            let fence=observations::revision(&conn,&remote).unwrap();
            let snapshot=observations::evidence(&conn,&remote).unwrap();
            retire(&conn).unwrap();
            assert_eq!(observations::revision(&conn,&remote).unwrap(),fence);
            assert_eq!(observations::evidence(&conn,&remote).unwrap(),snapshot);
            for record in &original {
                if !conflict { assert!(record.matches_canonical(&conn).unwrap()); }
            }
            if conflict {
                assert!(!acquired[0].exists(&conn).unwrap());
                assert!(!acquired[2].exists(&conn).unwrap());
                // Replaying a conflict cannot resurrect it after local retirement.
                submit(&mut conn,&remote,acquired);
                assert!(!original[0].exists(&conn).unwrap());
                assert!(!original[2].exists(&conn).unwrap());
            }
            submit(&mut conn,&remote,vec![]);
            for table in ["session_events","history","tool_calls"] {
                let count:i64=conn.query_row(&format!("SELECT count(*) FROM {table} WHERE session_id='retained'"),[],|r|r.get(0)).unwrap();
                assert_eq!(count,0,"{table}: remote_first={remote_first}, conflict={conflict}");
            }
        }
    }
}

#[test]
fn matching_connector_survives_conflicting_sibling_and_independent_withdrawal() {
    let (_dir,mut conn)=setup();
    let original=records("original");
    local(&conn,&original);
    // Earlier lexical owner conflicts; it must never win over proven support.
    for (id,records) in [("a",records("conflict")),("b",original.clone()),("c",original.clone())] {
        let key=key(SessionLocation::Remote,id);
        observe(&conn,&key);
        submit(&mut conn,&key,records);
    }
    let sibling=key(SessionLocation::Remote,"c");
    let revision=observations::revision(&conn,&sibling).unwrap();
    let snapshot=observations::evidence(&conn,&sibling).unwrap();
    retire(&conn).unwrap();
    submit(&mut conn,&key(SessionLocation::Remote,"b"),vec![]);
    for record in &original { assert!(record.matches_canonical(&conn).unwrap()); }
    assert_eq!(observations::revision(&conn,&sibling).unwrap(),revision);
    assert_eq!(observations::evidence(&conn,&sibling).unwrap(),snapshot);
    submit(&mut conn,&sibling,vec![]);
    assert!(!original[0].exists(&conn).unwrap());
    assert!(!original[2].exists(&conn).unwrap());
}

#[test]
fn retirement_rollback_restores_canonical_rows_and_ownership_metadata() {
    let (_dir,mut conn)=setup();
    let original=records("original");
    local(&conn,&original);
    let remote=key(SessionLocation::Remote,"remote");
    observe(&conn,&remote);
    submit(&mut conn,&remote,original.clone());
    let revision=observations::revision(&conn,&remote).unwrap();
    let snapshot=observations::evidence(&conn,&remote).unwrap();
    conn.execute_batch("CREATE TRIGGER reject_retirement_support BEFORE INSERT ON local_retirement_support BEGIN SELECT RAISE(ABORT,'rollback retirement'); END;").unwrap();
    assert!(retire(&conn).is_err());
    for record in &original { assert!(record.matches_canonical(&conn).unwrap()); }
    assert!(observations::get(&conn,&key(SessionLocation::Local,"claude")).unwrap().is_some());
    assert_eq!(observations::revision(&conn,&remote).unwrap(),revision);
    assert_eq!(observations::evidence(&conn,&remote).unwrap(),snapshot);
    let count:i64=conn.query_row("SELECT count(*) FROM local_retirement_support",[],|r|r.get(0)).unwrap();
    assert_eq!(count,0);
    conn.execute_batch("DROP TRIGGER reject_retirement_support;").unwrap();
    retire(&conn).unwrap();
    // The fence captured before retirement remains valid for the connector.
    apply_normalized(&mut conn,&remote,&revision.unwrap(),NormalizedSourceEvidence { source_stamp:"two".into(),source_bytes:0,covered_kinds:vec![EvidenceKind::SessionEvent,EvidenceKind::History,EvidenceKind::ToolCall],records:vec![] },false,Instant::now()).unwrap();
}

#[test]
fn withdrawn_support_cannot_claim_a_reused_identity() {
    let (_dir,mut conn)=setup();
    let original=records("original");
    local(&conn,&original);
    let remote=key(SessionLocation::Remote,"remote");
    observe(&conn,&remote);
    submit(&mut conn,&remote,original.clone());
    retire(&conn).unwrap();
    submit(&mut conn,&remote,vec![]);
    let count:i64=conn.query_row("SELECT count(*) FROM local_retirement_support",[],|r|r.get(0)).unwrap();
    assert_eq!(count,0);
    // An unknown canonical writer reuses the same key after withdrawal.
    for record in &original { record.write(&conn,SessionLocation::Local).unwrap(); }
    submit(&mut conn,&remote,original.clone());
    submit(&mut conn,&remote,vec![]);
    for record in &original { assert!(record.matches_canonical(&conn).unwrap()); }
}

#[test]
fn local_retirement_schema_upgrades_without_changing_snapshot_revisions() {
    let (dir,conn)=setup();
    let remote=key(SessionLocation::Remote,"remote");
    observe(&conn,&remote);
    let revision=observations::revision(&conn,&remote).unwrap();
    conn.execute_batch("DROP TRIGGER delete_local_retirement_support; DROP TABLE local_retirement_support;").unwrap();
    drop(conn);
    let conn=crate::open_db(&dir.path().join("history.db")).unwrap();
    assert_eq!(observations::revision(&conn,&remote).unwrap(),revision);
    assert_eq!(conn.query_row("SELECT count(*) FROM local_retirement_support",[],|r|r.get::<_,i64>(0)).unwrap(),0);
}

#[test]
fn identified_equal_time_prompts_survive_retirement_and_withdraw_independently() {
    for remote_first in [false, true] {
        let (_dir, mut conn) = setup();
        let remote = key(SessionLocation::Remote, "remote");
        observe(&conn, &remote);
        let mut records = records("repeat");
        records[1].payload.insert("event_uid".into(), serde_json::json!("prompt:1"));
        let mut repeated = records[1].clone();
        repeated.payload.insert("event_uid".into(), serde_json::json!("prompt:2"));
        records.push(repeated);
        source_evidence::validate_records(&remote,
            &[EvidenceKind::SessionEvent, EvidenceKind::History, EvidenceKind::ToolCall],
            &mut records).unwrap();
        if !remote_first { local(&conn, &records); }
        submit(&mut conn, &remote, records.clone());
        if remote_first { local(&conn, &records); }
        retire(&conn).unwrap();
        for record in &records { assert!(record.matches_canonical(&conn).unwrap()); }
        let ids: Vec<String> = conn.prepare("SELECT event_uid FROM history WHERE session_id='retained' ORDER BY event_uid").unwrap()
            .query_map([], |row| row.get(0)).unwrap().collect::<rusqlite::Result<_>>().unwrap();
        assert_eq!(ids, ["prompt:1", "prompt:2"]);
        records.pop();
        submit(&mut conn, &remote, records.clone());
        assert_eq!(conn.query_row("SELECT count(*) FROM history WHERE session_id='retained'", [], |row| row.get::<_, i64>(0)).unwrap(), 1);
        assert!(records[1].matches_canonical(&conn).unwrap());
        submit(&mut conn, &remote, vec![]);
        assert_eq!(conn.query_row("SELECT count(*) FROM history WHERE session_id='retained'", [], |row| row.get::<_, i64>(0)).unwrap(), 0);
    }
}

#[path = "order_tests.rs"]
mod order_tests;
