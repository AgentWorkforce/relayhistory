//! Exercise real SweepWrite boundaries and an independent writer in their gap.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

fn line(session: &str, uid: &str) -> String {
    serde_json::json!({"type":"user","uuid":uid,"sessionId":session,"timestamp":"2026-10-22T12:00:00Z","message":{"role":"user","content":[{"type":"text","text":uid}]}}).to_string()+"\n"
}
fn remote(conn: &Connection) -> (crate::ObservationKey, String) {
    let key = crate::ObservationKey {
        source: "claude".into(),
        session_id: "native-claude".into(),
        location: crate::SessionLocation::Remote,
        connector_id: "boundary".into(),
        connector_instance: "one".into(),
    };
    crate::observations::upsert(
        conn,
        &crate::observations::SessionObservation {
            key: key.clone(),
            raw_locator: None,
            source_stamp: Some("remote".into()),
            discovery_state: "shallow".into(),
            access_state: "available".into(),
            updated_ms: 1,
            first_prompt: None,
            last_assistant_text: None,
        },
    )
    .unwrap();
    let revision = crate::observations::revision(conn, &key).unwrap().unwrap();
    (key, revision)
}

#[test]
fn fresh_and_ordinary_claude_reads_rotate_while_fencing_same_source_intake() {
    for reread in [false, true] {
        let home = tempfile::tempdir().unwrap();
        let db = home.path().join("ledger.db");
        let path = home.path().join("native-claude.jsonl");
        let text = (0..JSONL_CHUNK_LINES + 10)
            .map(|n| line("native-claude", &format!("record-{n}")))
            .collect::<String>();
        fs::write(&path, text).unwrap();
        let conn = crate::open_db(&db).unwrap();
        conn.execute("CREATE TABLE unrelated_writes(value INTEGER)", [])
            .unwrap();
        if reread {
            ingest_claude_transcript_as(&conn, &path, None).unwrap();
        }
        let (key, revision) = remote(&conn);
        let mut peer = Some(rusqlite::Connection::open(&db).unwrap());
        peer.as_ref()
            .unwrap()
            .busy_timeout(std::time::Duration::ZERO)
            .unwrap();
        let wrote = Arc::new(AtomicBool::new(false));
        let blocked = Arc::new(Mutex::new(None::<String>));
        let fence = super::super::claude_write_fence::acquire(&conn, "claude").unwrap();
        let mut write = SweepWrite::begin(&conn).unwrap();
        let mut cursor = transcript_cursor::TranscriptCursorState::default();
        let mut callbacks = 0usize;
        let result = incremental::ingest_claude_transcript_incremental_batched(
            &conn,
            &path,
            None,
            &mut cursor,
            &mut || {
                callbacks += 1;
                if callbacks != JSONL_CHUNK_LINES {
                    return write.record();
                }
                let mut other = peer.take().unwrap();
                let wrote = wrote.clone();
                let blocked = blocked.clone();
                let key = key.clone();
                let revision = revision.clone();
                // The callback is armed only around the real 2000-record rotate.
                // It succeeds exactly after COMMIT and before BEGIN IMMEDIATE.
                conn.progress_handler(1,Some(move || {
                if !wrote.load(Ordering::SeqCst) && other.prepare_cached("INSERT INTO unrelated_writes VALUES(1)").unwrap().execute([]).is_ok() {
                    wrote.store(true,Ordering::SeqCst);
                    let record=crate::EvidenceRecord{kind:crate::EvidenceKind::SessionEvent,payload:serde_json::json!({"source":"claude","session_id":"native-claude","event_uid":"injected-anchor","message_id":"injected-anchor","ts_ms":1792670400000_i64,"source_order":0,"role":"assistant","kind":"text","text":"anchor"}).as_object().unwrap().clone(),record_id:None,revision_id:None};
                    let result=crate::source_intake::apply_normalized(&mut other,&key,&revision,crate::NormalizedSourceEvidence{source_stamp:"injected".into(),source_bytes:0,covered_kinds:vec![crate::EvidenceKind::SessionEvent],records:vec![record]},false,std::time::Instant::now());
                    *blocked.lock().unwrap()=Some(match result {Ok(_)=>"unexpected successful intake".into(),Err(error)=>format!("{error:#}")});
                }
                false
            }));
                let result = write.record();
                conn.progress_handler(0, None::<fn() -> bool>);
                result
            },
        );
        conn.progress_handler(0, None::<fn() -> bool>);
        result.unwrap();
        write.commit().unwrap();
        drop(fence);
        assert!(
            callbacks > JSONL_CHUNK_LINES,
            "complete local read never reached its chunk boundary"
        );
        assert!(
            wrote.load(Ordering::SeqCst),
            "independent writer never acquired the real chunk gap: reread={reread}"
        );
        assert!(
            blocked
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|error| error.contains("Claude source is being captured")),
            "same-source intake was not fenced: {:?}",
            blocked.lock().unwrap()
        );
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM session_events WHERE source='claude'",
                [],
                |row| row.get::<_, usize>(0)
            )
            .unwrap(),
            JSONL_CHUNK_LINES + 10
        );
        assert!(cursor.file.is_some());
        let other = rusqlite::Connection::open(&db).unwrap();
        assert!(super::super::claude_write_fence::acquire(&other, "claude").is_ok());
    }
}

#[test]
fn a_mixed_identity_full_read_stages_constrained_sessions_beyond_the_head() {
    let home = tempfile::tempdir().unwrap();
    let db = home.path().join("ledger.db");
    let path = home.path().join("mixed.jsonl");
    fs::write(&path, line("constrained", "a") + &line("constrained", "z")).unwrap();
    let conn = crate::open_db(&db).unwrap();
    ingest_claude_transcript_as(&conn, &path, None).unwrap();
    conn.execute("UPDATE session_events SET location='both'", [])
        .unwrap();
    let prefix = (0..JSONL_CHUNK_LINES + 10)
        .map(|n| line("new-head", &format!("filler-{n}")))
        .collect::<String>();
    fs::write(
        &path,
        prefix + &line("constrained", "z") + &line("constrained", "a"),
    )
    .unwrap();
    let (_fence, mut write) = super::super::claude_write_fence::begin_sweep(&conn).unwrap();
    let mut cursor = transcript_cursor::TranscriptCursorState::default();
    let mut callbacks = 0;
    let result = incremental::ingest_claude_transcript_incremental_batched(
        &conn,
        &path,
        None,
        &mut cursor,
        &mut || {
            callbacks += 1;
            write.record()
        },
    );
    assert!(format!("{:#}", result.unwrap_err()).contains("FILE_HISTORY_SHARED_ORDER_CONFLICT"));
    drop(write);
    assert_eq!(
        callbacks, 0,
        "a constrained later session was missed by head-only classification"
    );
    assert_eq!(
        conn.query_row(
            "SELECT COUNT(*) FROM session_events WHERE session_id='new-head'",
            [],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
    assert!(cursor.file.is_none());
}
