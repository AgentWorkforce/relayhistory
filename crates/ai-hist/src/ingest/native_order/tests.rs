use super::*;

#[test]
fn caught_nested_failure_keeps_partial_payload_but_does_not_certify_order() {
    let dir = tempfile::tempdir().unwrap();
    let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
    conn.execute("INSERT INTO session_events(source,session_id,event_uid,ts_ms,role,kind,text) VALUES('codex','nested','legacy',1,'assistant','text','legacy')",[]).unwrap();
    let outer = begin(&conn, "codex", Some("nested")).unwrap();
    let failure = (|| -> Result<()> {
        let _child = begin(&conn, "codex", Some("nested"))?;
        crate::ingest::insert_session_event(
            &conn,
            &EventRow {
                source: "codex",
                session_id: "nested",
                event_uid: "partial",
                message_id: "partial",
                ts_ms: 1,
                role: "assistant",
                kind: "text",
                text: Some("partial"),
                ..Default::default()
            },
        )?;
        anyhow::bail!("caught native parser failure")
    })();
    assert!(failure.is_err());
    outer.finish().unwrap();
    let (rows, known): (i64, i64) = conn
        .query_row(
            "SELECT COUNT(*),COUNT(source_order) FROM session_events WHERE session_id='nested'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(rows, 2, "valid partial payload remains available");
    assert_eq!(
        known, 0,
        "an outer reader cannot certify its failed child's prefix"
    );
}

#[test]
fn successful_nested_partial_read_discards_the_outer_order_proof() {
    let dir = tempfile::tempdir().unwrap();
    let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
    conn.execute("INSERT INTO session_events(source,session_id,event_uid,ts_ms,role,kind,text) VALUES('codex','nested','legacy',1,'assistant','text','legacy')",[]).unwrap();
    let outer = begin(&conn, "codex", None).unwrap();
    let child = begin(&conn, "codex", Some("nested")).unwrap();
    crate::ingest::insert_session_event(
        &conn,
        &EventRow {
            source: "codex",
            session_id: "nested",
            event_uid: "partial",
            message_id: "partial",
            ts_ms: 1,
            role: "assistant",
            kind: "text",
            text: Some("partial"),
            ..Default::default()
        },
    )
    .unwrap();
    child.discard().unwrap();
    outer.finish().unwrap();
    assert_eq!(
        conn.query_row(
            "SELECT COUNT(source_order) FROM session_events WHERE session_id='nested'",
            [],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
}

#[test]
fn caught_nested_finish_failure_does_not_certify_the_outer_order() {
    let dir = tempfile::tempdir().unwrap();
    let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
    conn.execute("INSERT INTO session_events(source,session_id,event_uid,ts_ms,role,kind,text) VALUES('codex','nested','legacy',1,'assistant','text','legacy')",[]).unwrap();
    conn.execute("INSERT INTO session_events(source,session_id,event_uid,message_id,ts_ms,role,kind,text,source_order,location) VALUES('codex','nested','anchor','anchor',1,'assistant','text','anchor',0,'remote')",[]).unwrap();
    let outer = begin(&conn, "codex", Some("nested")).unwrap();
    let child = begin(&conn, "codex", Some("nested")).unwrap();
    for uid in ["partial", "anchor"] {
        crate::ingest::insert_session_event(
            &conn,
            &EventRow {
                source: "codex",
                session_id: "nested",
                event_uid: uid,
                message_id: uid,
                ts_ms: 1,
                role: "assistant",
                kind: "text",
                text: Some(uid),
                ..Default::default()
            },
        )
        .unwrap();
    }
    assert!(child
        .finish()
        .unwrap_err()
        .to_string()
        .contains("SHARED_ORDER_CONFLICT"));
    outer.finish().unwrap();
    let rank: Option<i64> = conn
        .query_row(
            "SELECT source_order FROM session_events WHERE event_uid='partial'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        rank, None,
        "a caught reconciliation error must invalidate the outer proof"
    );
}

#[test]
fn caught_nested_unwind_invalidates_the_outer_order_proof() {
    let dir = tempfile::tempdir().unwrap();
    let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
    conn.execute("INSERT INTO session_events(source,session_id,event_uid,ts_ms,role,kind,text) VALUES('codex','nested','legacy',1,'assistant','text','legacy')",[]).unwrap();
    let outer = begin(&conn, "codex", Some("nested")).unwrap();
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _child = begin(&conn, "codex", Some("nested")).unwrap();
        crate::ingest::insert_session_event(
            &conn,
            &EventRow {
                source: "codex",
                session_id: "nested",
                event_uid: "partial",
                message_id: "partial",
                ts_ms: 1,
                role: "assistant",
                kind: "text",
                text: Some("partial"),
                ..Default::default()
            },
        )
        .unwrap();
        panic!("caught native reader unwind");
    }));
    assert!(failure.is_err());
    outer.finish().unwrap();
    assert_eq!(
        conn.query_row(
            "SELECT COUNT(source_order) FROM session_events WHERE session_id='nested'",
            [],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
}
