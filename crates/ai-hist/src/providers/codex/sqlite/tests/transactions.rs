use super::*;

#[test]
fn failed_projection_replacement_keeps_evidence_digest_and_sweep_stamp() {
    let dir = tempfile::tempdir().unwrap();
    let codex = dir.path().join(".codex");
    install_fixture(&codex, "pruned-all-items");
    let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
    let mut state = serde_json::Map::new();
    let repairs = crate::ingest::SweepRepairs::default();
    sync(&conn, &mut state, &codex, &repairs).unwrap();
    let stamps = state["codex_sqlite_v2"].clone();
    let digest = || -> String {
        conn.query_row(
            "SELECT json_extract(payload_json,'$.content_hash') FROM session_markers \
             WHERE session_id='sqlite-pruned-all-items' AND marker_uid='sqlite:thread'",
            [],
            |row| row.get(0),
        )
        .unwrap()
    };
    let answer = || -> String {
        conn.query_row(
            "SELECT text FROM session_events WHERE session_id='sqlite-pruned-all-items' \
             AND event_uid='sqlite-sqlite-pruned-all-items-1'",
            [],
            |row| row.get(0),
        )
        .unwrap()
    };
    let old_digest = digest();
    let old_answer = answer();
    Connection::open(codex.join("thread_history_1.sqlite")).unwrap().execute(
        "UPDATE thread_items SET item_json=?1 WHERE item_id='sqlite-sqlite-pruned-all-items-1'",
        [json!({"id":"sqlite-sqlite-pruned-all-items-1","type":"agentMessage","text":"Rewritten answer"}).to_string()],
    ).unwrap();
    Connection::open(codex.join("state_5.sqlite"))
        .unwrap()
        .execute(
            "UPDATE threads SET updated_at_ms=updated_at_ms+1 WHERE id='sqlite-pruned-all-items'",
            [],
        )
        .unwrap();
    conn.execute_batch(
        "CREATE TRIGGER fail_projection BEFORE INSERT ON session_events \
         BEGIN SELECT RAISE(ABORT,'injected write failure'); END;",
    )
    .unwrap();

    let failed = sync(&conn, &mut state, &codex, &repairs).unwrap();
    assert!(failed.incomplete);
    assert_eq!(answer(), old_answer);
    assert_eq!(digest(), old_digest);
    assert_eq!(state["codex_sqlite_v2"], stamps);

    conn.execute_batch("DROP TRIGGER fail_projection").unwrap();
    sync(&conn, &mut state, &codex, &repairs).unwrap();
    assert_eq!(answer(), "Rewritten answer");
    assert_ne!(digest(), old_digest);
    assert_ne!(state["codex_sqlite_v2"], stamps);
}
