use super::super::{acquire, stage_complete};
use rusqlite::Connection;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
fn insert(conn: &Connection, session: &str, uid: &str, rank: Option<i64>) {
    conn.execute("INSERT INTO session_events(source,session_id,event_uid,role,kind,text,ts_ms,source_order) VALUES('claude',?1,?2,'user','text',?2,1,?3)",rusqlite::params![session,uid,rank]).unwrap();
}
#[test]
fn repeated_file_checks_inspect_only_revision_touched_sessions() {
    let dir = tempfile::tempdir().unwrap();
    let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
    conn.execute_batch("BEGIN; WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<2000) INSERT INTO session_events(source,session_id,event_uid,role,kind,text,ts_ms,source_order) SELECT 'claude','archive-'||i,'one','user','text','text',1,0 FROM n; COMMIT;").unwrap();
    let guard = acquire(&conn, "claude").unwrap();
    let instructions = Arc::new(AtomicUsize::new(0));
    let count = instructions.clone();
    conn.progress_handler(
        1,
        Some(move || {
            count.fetch_add(1, Ordering::Relaxed);
            false
        }),
    );
    assert!(!stage_complete(&conn, None, &guard).unwrap());
    let archive_scan = instructions.swap(0, Ordering::Relaxed);
    insert(&conn, "new-file", "one", Some(0));
    instructions.store(0, Ordering::Relaxed);
    assert!(!stage_complete(&conn, None, &guard).unwrap());
    let refresh = instructions.swap(0, Ordering::Relaxed);
    conn.progress_handler(0, None::<fn() -> bool>);
    assert!(archive_scan > 1000);
    assert!(
        refresh < archive_scan / 10,
        "archive scan={archive_scan}, one-file refresh={refresh}"
    );
    insert(&conn, "new-file", "two", Some(1));
    assert!(!stage_complete(&conn, None, &guard).unwrap());
    conn.execute(
        "DELETE FROM session_events WHERE session_id='new-file' AND event_uid='one'",
        [],
    )
    .unwrap();
    assert!(
        stage_complete(&conn, None, &guard).unwrap(),
        "deleted native rank leaves a sparse bucket"
    );
    conn.execute(
        "UPDATE session_events SET source_order=0 WHERE session_id='new-file'",
        [],
    )
    .unwrap();
    assert!(!stage_complete(&conn, None, &guard).unwrap());
    conn.execute(
        "UPDATE session_events SET source_order=NULL WHERE session_id='new-file'",
        [],
    )
    .unwrap();
    assert!(stage_complete(&conn, None, &guard).unwrap());
    conn.execute("DELETE FROM session_events WHERE session_id='new-file'", [])
        .unwrap();
    assert!(!stage_complete(&conn, None, &guard).unwrap());
    conn.execute(
        "UPDATE session_events SET location='both' WHERE session_id='archive-1999'",
        [],
    )
    .unwrap();
    assert!(
        stage_complete(&conn, None, &guard).unwrap(),
        "mixed-ID constraints remain global"
    );
}
#[test]
fn rolled_back_write_units_cannot_reuse_obsolete_proofs() {
    let dir = tempfile::tempdir().unwrap();
    let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
    insert(&conn, "constrained", "one", None);
    insert(&conn, "other", "one", Some(0));
    let guard = acquire(&conn, "claude").unwrap();
    let write = crate::ingest::SweepWrite::begin(&conn).unwrap();
    assert!(stage_complete(&conn, None, &guard).unwrap());
    conn.execute(
        "UPDATE session_events SET source_order=0 WHERE session_id='constrained'",
        [],
    )
    .unwrap();
    assert!(!stage_complete(&conn, None, &guard).unwrap());
    drop(write);
    let write = crate::ingest::SweepWrite::begin(&conn).unwrap();
    conn.execute(
        "UPDATE session_events SET text='rewritten' WHERE session_id='other'",
        [],
    )
    .unwrap();
    assert!(stage_complete(&conn, None, &guard).unwrap());
    write.commit().unwrap();
}

#[test]
fn a_real_multi_file_claude_sweep_loads_archive_eligibility_once() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("claude/projects/app");
    std::fs::create_dir_all(&root).unwrap();
    for n in 0..8 {
        let session = format!("native-{n}");
        let record = serde_json::json!({"type":"user","uuid":"one","sessionId":session,"timestamp":"2026-10-22T12:00:00Z","message":{"role":"user","content":"prompt"}});
        std::fs::write(root.join(format!("{session}.jsonl")), format!("{record}\n")).unwrap();
    }
    let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
    super::LOADS.with(|loads| loads.set(0));
    let mut state = serde_json::Map::new();
    crate::ingest::sync_claude_session_metadata_with_repairs_and_coverage(
        &conn,
        &mut state,
        &root,
        &crate::ingest::SweepRepairs::default(),
        &mut crate::ingest::SweepCoverage::default(),
        None,
    )
    .unwrap();
    assert_eq!(
        conn.query_row(
            "SELECT COUNT(DISTINCT session_id) FROM session_events WHERE source='claude'",
            [],
            |row| row.get::<_, usize>(0)
        )
        .unwrap(),
        8
    );
    assert_eq!(
        super::LOADS.with(std::cell::Cell::get),
        1,
        "per-file leases must share the outer provider-sweep proof"
    );
}
