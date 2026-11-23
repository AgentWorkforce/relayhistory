//! A session's activity window is never stored at 0.

use super::*;
use crate::init_db;
use rusqlite::Connection;

fn row(first_ts: i64, last_ts: i64) -> SessionCatalogRow<'static> {
    SessionCatalogRow {
        session_id: "s",
        source: "claude",
        cwd: None,
        git_branch: None,
        first_ts,
        last_ts,
        last_assistant_text: None,
        raw_path: None,
    }
}

fn window(conn: &Connection) -> (Option<i64>, Option<i64>) {
    conn.query_row(
        "SELECT first_activity_ms, last_activity_ms FROM sessions WHERE session_id = 's'",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .unwrap()
}

/// A pass that knows no time stores none, a later real time replaces a 0 an
/// earlier rule stored, and a pass with no time keeps the real one.
#[test]
fn an_unknown_activity_time_is_stored_as_null_and_a_stored_zero_heals() {
    let conn = Connection::open_in_memory().unwrap();
    init_db(&conn).unwrap();

    upsert_session(&conn, &row(0, 0)).unwrap();
    assert_eq!(window(&conn), (None, None));

    // What an earlier rule left behind.
    conn.execute(
        "UPDATE sessions SET first_activity_ms = 0, last_activity_ms = 0",
        [],
    )
    .unwrap();
    upsert_session(&conn, &row(1_000, 2_000)).unwrap();
    assert_eq!(window(&conn), (Some(1_000), Some(2_000)));

    upsert_session(&conn, &row(0, 0)).unwrap();
    assert_eq!(window(&conn), (Some(1_000), Some(2_000)));

    upsert_session_rebuilt(&conn, &row(0, 0)).unwrap();
    assert_eq!(
        window(&conn),
        (None, None),
        "a rebuild with no time knows none"
    );
}
