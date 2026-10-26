//! Covering seeks used by maintenance's writer-locked validation.
use rusqlite::Connection;
use std::collections::HashSet;

pub(super) const REVISION_SEEKS: &[(&str, &str)] = &[
    ("session_events", "session_id"),
    ("tool_calls", "session_id"),
    ("file_edits", "session_id"),
    ("session_markers", "session_id"),
    ("history", "session_id"),
    ("observation_evidence", "session_id"),
    ("session_observations", "session_id"),
    ("session_commit_links", "session_id"),
    ("session_relationships", "parent_session_id"),
    ("session_relationships", "child_session_id"),
];

pub(super) fn revision_index(table: &str, session: &str) -> String {
    format!("idx_retention_{table}_{session}_revision")
}

/// Run after change-feed migration, which supplies the revision columns.
pub(super) fn init_indexes(conn: &Connection) -> anyhow::Result<()> {
    for (table, session) in REVISION_SEEKS {
        conn.execute_batch(&format!(
            "CREATE INDEX IF NOT EXISTS {} ON {table}(source,{session},revision);",
            revision_index(table, session)
        ))?;
    }
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_retention_history_activity ON history(source,session_id,timestamp_ms);
         CREATE INDEX IF NOT EXISTS idx_retention_anonymous_activity ON history(timestamp_ms,id) WHERE session_id IS NULL;",
    )?;
    Ok(())
}

pub(super) fn indexes_current(conn: &Connection) -> anyhow::Result<bool> {
    let found = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='index'")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<HashSet<_>>>()?;
    let expected = REVISION_SEEKS
        .iter()
        .map(|(table, session)| revision_index(table, session))
        .chain([
            "idx_retention_history_activity".into(),
            "idx_retention_anonymous_activity".into(),
            "idx_sessions_identity".into(),
            "idx_session_presences_location".into(),
        ]);
    Ok(expected.into_iter().all(|index| found.contains(&index)))
}

pub(super) fn upload_queries() -> Vec<String> {
    let mut queries = vec![
        "SELECT 1 FROM sessions INDEXED BY idx_sessions_identity WHERE source=?1 AND session_id=?2 AND revision>?3 LIMIT 1".into(),
        // The existing location index makes this at most two point lookups.
        "SELECT 1 FROM session_presences INDEXED BY idx_session_presences_location WHERE location IN ('local','remote') AND source=?1 AND session_id=?2 AND revision>?3 LIMIT 1".into(),
    ];
    queries.extend(REVISION_SEEKS.iter().map(|(table, session)| format!(
        "SELECT 1 FROM {table} INDEXED BY {} WHERE source=?1 AND {session}=?2 AND revision>?3 LIMIT 1", revision_index(table, session))));
    queries
}

/// Constrain the nullable-page indexes' expression key before seeking the
/// last timestamp. MAX(ts_ms) without that constraint scans a whole session.
pub(super) fn activity_queries() -> Vec<String> {
    [
        ("session_events", "ts_ms", "idx_session_events_source_page", "ts_ms IS NOT NULL"),
        ("tool_calls", "ts_ms", "idx_tool_calls_page_v2", "(ts_ms IS NULL)=0"),
        ("file_edits", "ts_ms", "idx_file_edits_page_v2", "(ts_ms IS NULL)=0"),
        ("session_markers", "ts_ms", "idx_session_markers_page", "(ts_ms IS NULL)=0"),
        ("history", "timestamp_ms", "idx_retention_history_activity", "timestamp_ms IS NOT NULL"),
    ].into_iter().map(|(table, ts, index, nonnull)| format!(
        "SELECT {ts} FROM {table} INDEXED BY {index} WHERE source=?1 AND session_id=?2 AND {nonnull} ORDER BY {ts} DESC LIMIT 1")).collect()
}
