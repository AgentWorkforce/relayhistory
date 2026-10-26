use super::*;
use rusqlite::StatementStatus;

fn seed_large(conn: &Connection) {
    session(conn, "large", 40, 0);
    conn.execute_batch(
        "BEGIN;
        WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<30000)
        INSERT INTO session_events(source,session_id,event_uid,role,kind,text,ts_ms)
        SELECT 'codex','large','e'||i,'assistant','text','',1 FROM n;
        WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<30000)
        INSERT INTO tool_calls(source,session_id,tool_use_id,name)
        SELECT 'codex','large','t'||i,'tool' FROM n;
        WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<30000)
        INSERT INTO history(source,session_id,prompt,timestamp_ms)
        SELECT 'codex','large','',i FROM n;
        COMMIT;",
    )
    .unwrap();
}

#[test]
fn acknowledged_payload_validation_and_activity_use_bounded_seeks() {
    let (_dir, store, conn) = fixture();
    seed_large(&conn);
    let floor = store.head_revision().unwrap().revision as i64;
    let key = ("codex".into(), "large".into());
    assert!(targets::uploaded(&conn, &key, floor).unwrap());
    for sql in schema::upload_queries() {
        let mut stmt = conn.prepare(&sql).unwrap();
        assert!(!stmt.exists(params![key.0, key.1, floor]).unwrap());
        let steps = stmt.get_status(StatementStatus::VmStep);
        assert!(steps < 100, "{steps} VM steps: {sql}");
    }
    for sql in schema::activity_queries() {
        let mut stmt = conn.prepare(&sql).unwrap();
        let _: Option<i64> = stmt
            .query_row(params![key.0, key.1], |r| r.get(0))
            .optional()
            .unwrap();
        let steps = stmt.get_status(StatementStatus::VmStep);
        assert!(steps < 100, "{steps} VM steps: {sql}");
    }
    // A later row in another session does not protect this session.
    session(&conn, "other", 40, 1);
    assert!(targets::uploaded(&conn, &key, floor).unwrap());
    conn.execute(
        "UPDATE session_events SET text='not acknowledged' WHERE event_uid='e30000'",
        [],
    )
    .unwrap();
    assert!(!targets::uploaded(&conn, &key, floor).unwrap());
}

#[test]
fn tombstone_suppression_seeks_past_old_deletes() {
    let (_dir, _store, conn) = fixture();
    conn.execute_batch(
        "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<30000)
        INSERT INTO evidence_tombstones(kind,source,session_id,record_key,revision)
        SELECT 'session_event','codex','old',CAST(i AS TEXT),i FROM n;",
    )
    .unwrap();
    let mut stmt = conn.prepare(maintenance::SUPPRESS_TOMBSTONES).unwrap();
    assert_eq!(stmt.execute(params!["session_event", 30000]).unwrap(), 0);
    assert!(stmt.get_status(StatementStatus::VmStep) < 100);
    conn.execute(
        "INSERT INTO evidence_tombstones VALUES('session_event','codex','old','new',30001)",
        [],
    )
    .unwrap();
    maintenance::suppress_tombstones(&conn, 30000).unwrap();
    assert_eq!(count(&conn, "evidence_tombstones"), 30000);
}

#[test]
fn anonymous_ack_filter_is_applied_after_bounded_selection() {
    let (_dir, store, conn) = fixture();
    acknowledge(&store, "cloud");
    conn.execute_batch(
        "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<30000)
        INSERT INTO history(source,prompt,timestamp_ms) SELECT 'codex','',i FROM n;",
    )
    .unwrap();
    let mut stmt = conn.prepare(maintenance::ANONYMOUS_CANDIDATES).unwrap();
    let rows = stmt
        .query_map(params![NOW, 32], |_| Ok(()))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(rows.len(), 32);
    assert!(stmt.get_status(StatementStatus::VmStep) < 500);
    let report = run(&store, &RetentionOptions::default(), NOW).unwrap();
    assert!(report.awaiting_upload_ack);
    assert_eq!(report.rows, 0);
    assert_eq!(count(&conn, "history"), 30000);
    acknowledge(&store, "cloud");
    assert_eq!(
        run(&store, &RetentionOptions::default(), NOW).unwrap().rows,
        32
    );
}

#[test]
fn writable_reopen_adds_indexes_without_requiring_readonly_migration() {
    let (_dir, store, conn) = fixture();
    conn.execute_batch("DROP INDEX idx_retention_session_events_session_id_revision;")
        .unwrap();
    assert!(!schema::indexes_current(&conn).unwrap());
    let readonly = SessionStore::open(
        StoreOptions::default()
            .db_path(store.db_path())
            .read_only(true),
    )
    .unwrap();
    assert_eq!(
        readonly.retention_policy().unwrap(),
        RetentionPolicy::default()
    );
    assert!(!schema::indexes_current(&conn).unwrap());
    let _writable = SessionStore::open(StoreOptions::default().db_path(store.db_path())).unwrap();
    assert!(schema::indexes_current(&conn).unwrap());
}

#[test]
fn relationship_batches_reopen_survivors_before_deleting_both_edge_directions() {
    let (_dir, store, conn) = fixture();
    session(&conn, "expired", 40, 0);
    session(&conn, "survivor", 1, 0);
    for i in 0..7 {
        let locator = format!("transcript-{i}");
        conn.execute("INSERT INTO session_continuity_evidence(source,locator,session_id,updated_ms) VALUES('codex',?1,'survivor',1)", [&locator]).unwrap();
        let (parent, child) = if i % 2 == 0 {
            ("expired", "survivor")
        } else {
            ("survivor", "expired")
        };
        let kind = ["continuation", "fork", "resume"][i % 3];
        conn.execute("INSERT INTO session_relationships(source,parent_session_id,child_session_id,relationship_uid,relationship,identity_status,evidence_kind,evidence_locator,created_ms,updated_ms) VALUES('codex',?1,?2,?3,?4,'observed','test',?3,1,1)", params![parent,child,locator,kind]).unwrap();
    }
    acknowledge(&store, "cloud");
    let opts = RetentionOptions {
        max_rows: 1,
        ..Default::default()
    };
    for _ in 0..30 {
        let report = run(&store, &opts, NOW).unwrap();
        assert!(report.rows <= 1);
        // Every removed edge's evidence was reopened in the same transaction.
        let lost: i64 = conn.query_row("SELECT count(*) FROM session_continuity_evidence e WHERE pending_reason IS NULL AND NOT EXISTS(SELECT 1 FROM session_relationships r WHERE r.source=e.source AND r.evidence_locator=e.locator)", [], |r|r.get(0)).unwrap();
        assert_eq!(lost, 0);
        if report.cycle_complete {
            break;
        }
    }
    assert_eq!(count(&conn, "session_relationships"), 0);
    assert_eq!(count(&conn, "session_continuity_evidence"), 7);
    assert_eq!(count(&conn, "sessions"), 1);
}
