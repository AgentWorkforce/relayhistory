use super::super::*;
use std::io::Write;

const SID: &str = "legacy-order";
const TIME: &str = "2026-04-20T05:00:01.000Z";

fn line(kind: &str, payload: Value) -> String {
    json!({"timestamp":TIME,"type":kind,"payload":payload}).to_string() + "\n"
}
fn turn(prompt: &str, answer: &str, id: &str) -> String {
    line("event_msg", json!({"type":"user_message","message":prompt}))
        + &line(
            "event_msg",
            json!({"type":"agent_message","message":answer}),
        )
        + &line("event_msg", json!({"type":"task_complete","turn_id":id}))
}

#[test]
fn resumed_codex_append_keeps_legacy_equal_time_bucket_unranked_and_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rollout-legacy.jsonl");
    let db = dir.path().join("history.db");
    fs::write(
        &path,
        line("session_meta", json!({"id":SID,"cwd":"/tmp/project"}))
            + &turn("first", "first answer", "turn-1"),
    )
    .unwrap();
    let meta = read_codex_session_meta(&path).unwrap().unwrap();
    let conn = open_db(&db).unwrap();
    let first = ingest_codex_rollout_at_locator(&conn, &path, &meta, true).unwrap();
    assert!(first.from_start);
    let locator = path.to_string_lossy();
    let key = transcript_cursor::CursorKey::Locator {
        source: "codex",
        locator: &locator,
    };
    let cursor = transcript_cursor::load_cursor(&conn, &key).unwrap();
    assert!(
        cursor.file.as_ref().is_some_and(|file| file.offset > 0),
        "closed turn must persist a resumable cursor"
    );
    // Model the upgraded schema without guessing ranks for retained legacy
    // events. The pre-upgrade cursor remains valid and must resume.
    conn.execute(
        "UPDATE session_events SET source_order=NULL WHERE source='codex' AND session_id=?1",
        [SID],
    )
    .unwrap();
    drop(conn);
    let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
    file.write_all(turn("second", "second answer", "turn-2").as_bytes())
        .unwrap();
    drop(file);
    let conn = open_db(&db).unwrap();
    let resumed = ingest_codex_rollout_at_locator(&conn, &path, &meta, true).unwrap();
    assert!(
        !resumed.from_start,
        "regression must exercise an incremental cursor resume"
    );
    let events = crate::session_events(&conn, SID, Some("codex")).unwrap();
    let text: Vec<_> = events
        .iter()
        .filter_map(|event| event.text.as_deref())
        .collect();
    assert_eq!(text, ["first", "first answer", "second", "second answer"]);
    assert!(
        events.iter().all(|event| event.source_order.is_none()),
        "a partial source read cannot rank a legacy bucket"
    );
    let ids: Vec<_> = events.iter().map(|event| event.id).collect();
    let full = ingest_codex_rollout_at_locator(&conn, &path, &meta, false).unwrap();
    assert!(full.from_start);
    let learned = crate::session_events(&conn, SID, Some("codex")).unwrap();
    assert_eq!(
        learned.iter().map(|event| event.id).collect::<Vec<_>>(),
        ids
    );
    assert_eq!(
        learned
            .iter()
            .map(|event| event.source_order)
            .collect::<Vec<_>>(),
        vec![Some(0), Some(1), Some(2), Some(3)]
    );
    let revisions = || {
        conn.prepare("SELECT id,revision FROM session_events WHERE source='codex' AND session_id=?1 ORDER BY id").unwrap()
        .query_map([SID], |row| Ok((row.get::<_,i64>(0)?,row.get::<_,i64>(1)?))).unwrap()
        .collect::<rusqlite::Result<Vec<_>>>().unwrap()
    };
    let before = revisions();
    ingest_codex_rollout_at_locator(&conn, &path, &meta, false).unwrap();
    assert_eq!(
        revisions(),
        before,
        "repeating a complete source order is a no-op"
    );
    // A brand-new timestamp bucket still has complete observed append order.
    insert_session_event(
        &conn,
        &EventRow {
            source: "codex",
            session_id: SID,
            message_id: "later",
            event_uid: "later",
            ts_ms: events[0].ts_ms + 1,
            role: "assistant",
            kind: "text",
            text: Some("later"),
            ..Default::default()
        },
    )
    .unwrap();
    let later: Option<i64> = conn
        .query_row(
            "SELECT source_order FROM session_events WHERE event_uid='later'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(later, Some(0));
}

/// One equal-time bucket (DeepAgents stamps every message with the session
/// start) must not be recounted per insert: the next-rank lookup stays a
/// couple of index seeks however large the bucket grows.
#[test]
fn next_source_order_seeks_instead_of_recounting_the_bucket() {
    use rusqlite::StatementStatus;
    let dir = tempfile::tempdir().unwrap();
    let conn = open_db(&dir.path().join("history.db")).unwrap();
    let tx = conn.unchecked_transaction().unwrap();
    for rank in 0..3000_i64 {
        tx.execute(
            "INSERT INTO session_events(source,session_id,event_uid,ts_ms,role,kind,text,source_order) \
             VALUES('deepagents','bucket',?1,100,'user','text','x',?2)",
            rusqlite::params![format!("e{rank}"), rank],
        )
        .unwrap();
    }
    tx.commit().unwrap();
    let next = |conn: &Connection| -> (Option<i64>, i32) {
        let mut stmt = conn
            .prepare(&format!("SELECT {}", super::NEXT_SOURCE_ORDER))
            .unwrap();
        stmt.raw_bind_parameter(1, "deepagents").unwrap();
        stmt.raw_bind_parameter(2, "bucket").unwrap();
        stmt.raw_bind_parameter(8, 100_i64).unwrap();
        let value = {
            let mut rows = stmt.raw_query();
            rows.next().unwrap().unwrap().get(0).unwrap()
        };
        (value, stmt.get_status(StatementStatus::VmStep))
    };
    let (rank, steps) = next(&conn);
    assert_eq!(rank, Some(3000));
    assert!(
        steps < 200,
        "next rank recounted the bucket: {steps} VM steps"
    );

    conn.execute(
        "INSERT INTO session_events(source,session_id,event_uid,ts_ms,role,kind,text) \
         VALUES('deepagents','bucket','legacy',100,'user','text','x')",
        [],
    )
    .unwrap();
    let (rank, steps) = next(&conn);
    assert_eq!(rank, None, "a bucket with an unranked row stays unranked");
    assert!(
        steps < 200,
        "unranked check scanned the bucket: {steps} VM steps"
    );
}
