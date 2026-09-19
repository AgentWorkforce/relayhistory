//! The `session_requests` view is only bounded if its session filter reaches
//! the underlying table. A grouped view that SQLite cannot push a constraint
//! into would aggregate every session in the store on every read — and would
//! still return the right answer, so nothing but a plan check catches it.
use ai_hist::{init_db, session_requests_page, session_usage_summary};
use rusqlite::Connection;

fn seeded() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    init_db(&conn).unwrap();
    for session in ["s1", "s2", "s3"] {
        for index in 0..4 {
            conn.execute(
                "INSERT INTO session_events \
                 (source, session_id, message_id, ts_ms, role, kind, text, model, token_json, event_uid) \
                 VALUES ('claude', ?1, ?2, ?3, 'assistant', 'text', 'x', 'm', \
                         '{\"input_tokens\":1,\"output_tokens\":2}', ?4)",
                rusqlite::params![
                    session,
                    format!("{session}-m{index}"),
                    1_000 + index,
                    format!("{session}-m{index}:0")
                ],
            )
            .unwrap();
        }
    }
    conn
}

fn plan(conn: &Connection, sql: &str) -> String {
    conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
        .unwrap()
        .query_map([], |row| row.get::<_, String>(3))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
        .join("\n")
}

#[test]
fn a_session_scoped_request_read_uses_the_event_page_index() {
    let conn = seeded();
    for sql in [
        "SELECT * FROM session_requests WHERE source = 'claude' AND session_id = 's2' \
         ORDER BY first_ts_ms ASC, id ASC LIMIT 10",
        "SELECT * FROM session_requests WHERE source = 'claude' AND session_id = 's2'",
    ] {
        let plan = plan(&conn, sql);
        assert!(
            plan.contains("USING INDEX idx_session_events_source_page"),
            "the session filter must reach the table:\n{plan}"
        );
        assert!(
            !plan.contains("SCAN session_events") || plan.contains("USING INDEX"),
            "no unindexed scan of every session:\n{plan}"
        );
    }
}

#[test]
fn the_reads_still_answer_only_the_session_they_were_asked_for() {
    let conn = seeded();
    let page = session_requests_page(&conn, "claude", "s2", 50, None).unwrap();
    assert_eq!(page.requests.len(), 4);
    assert!(page
        .requests
        .iter()
        .all(|request| request.session_id == "s2"));
    let summary = session_usage_summary(&conn, "claude", "s2")
        .unwrap()
        .unwrap();
    assert_eq!(summary.request_count, 4);
    assert_eq!(summary.output_tokens, 8);
}
