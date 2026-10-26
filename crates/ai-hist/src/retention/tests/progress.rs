use super::*;

fn blocked_prompts(conn: &Connection) {
    conn.execute_batch(
        "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<32)
        INSERT INTO history(source,prompt,timestamp_ms) SELECT 'codex','blocked',i FROM n;",
    )
    .unwrap();
}

fn reach_blocked_prompts(store: &SessionStore) {
    let batch = run(store, &RetentionOptions::default(), NOW).unwrap();
    assert_eq!(batch.sessions, 0);
    let batch = run(store, &RetentionOptions::default(), NOW).unwrap();
    assert!(batch.awaiting_upload_ack);
    assert!(!batch.cycle_complete);
    assert_eq!(batch.rows, 0);
}

#[test]
fn blocked_anonymous_prompts_do_not_starve_sessions_that_age() {
    let (_dir, store, conn) = fixture();
    session(&conn, "earlier", 29, 1);
    acknowledge(&store, "cloud");
    blocked_prompts(&conn);
    reach_blocked_prompts(&store);

    // The previously retained session has aged past 30 days. Its acknowledged
    // evidence must expire even though the anonymous prompts remain undelivered.
    let batch = run(&store, &RetentionOptions::default(), NOW + 2 * DAY).unwrap();
    assert_eq!(batch.sessions, 1);
    assert_eq!(batch.rows, 2);
    assert_eq!(count(&conn, "history"), 32);
    assert_eq!(count(&conn, "evidence_tombstones"), 0);
}

#[test]
fn blocked_anonymous_prompts_do_not_starve_newly_acknowledged_sessions() {
    let (_dir, store, conn) = fixture();
    session(&conn, "earlier", 40, 1);
    let session_head = store.head_revision().unwrap();
    store.register_retention_consumer("cloud").unwrap();
    store
        .acknowledge_retention(
            "cloud",
            Watermark {
                revision: 0,
                ..session_head
            },
        )
        .unwrap();
    blocked_prompts(&conn);
    reach_blocked_prompts(&store);

    // Publish only through the session's evidence: later anonymous prompts
    // still cannot be removed, but they must not pin the session sweep cursor.
    store.acknowledge_retention("cloud", session_head).unwrap();
    let batch = run(&store, &RetentionOptions::default(), NOW).unwrap();
    assert_eq!(batch.sessions, 1);
    assert_eq!(batch.rows, 2);
    assert_eq!(count(&conn, "history"), 32);
    assert_eq!(count(&conn, "evidence_tombstones"), 0);
}
