use rusqlite::{params, Connection, OptionalExtension};
use std::collections::BTreeSet;

pub(super) type Key = (String, String);
pub(super) const SCAN_LIMIT: usize = 32;
/// These tables have session-scoped indexes already; retention adds none to
/// the capture path. Other fed metadata is checked before final eviction.
pub(super) const PAYLOADS: &[(&str, &str)] = &[
    ("session_events", "ts_ms"),
    ("tool_calls", "ts_ms"),
    ("file_edits", "ts_ms"),
    ("session_markers", "ts_ms"),
    ("history", "timestamp_ms"),
    ("observation_evidence", ""),
];

/// Plan on a reader before acquiring SQLite's writer lock. Each query uses
/// existing session indexes; no payload text is read. Revalidate on the writer.
pub(super) fn candidates(conn: &Connection, after: &Key) -> anyhow::Result<Vec<Key>> {
    let mut keys = BTreeSet::new();
    for table in ["sessions", "retention_pending"]
        .into_iter()
        .chain(PAYLOADS.iter().map(|(table, _)| *table))
    {
        let sql = format!(
            "SELECT DISTINCT source,session_id FROM {table}
            WHERE (source,session_id) > (?1,?2) AND session_id IS NOT NULL
            ORDER BY source,session_id LIMIT {SCAN_LIMIT}"
        );
        keys.extend(
            conn.prepare(&sql)?
                .query_map(params![after.0, after.1], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })?
                .collect::<rusqlite::Result<Vec<Key>>>()?,
        );
    }
    Ok(keys.into_iter().take(SCAN_LIMIT).collect())
}

pub(super) fn cursor(conn: &Connection) -> anyhow::Result<Key> {
    Ok(conn
        .query_row(
            "SELECT source,session_id FROM retention_sweep WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?
        .unwrap_or_default())
}
pub(super) fn advance(conn: &Connection, key: &Key) -> anyhow::Result<()> {
    conn.execute("INSERT INTO retention_sweep VALUES(1,?1,?2) ON CONFLICT(singleton) DO UPDATE SET source=excluded.source,session_id=excluded.session_id", params![key.0,key.1])?;
    Ok(())
}

/// A catalog timestamp is not necessarily current for a delegated session or
/// after source intake. Consider all recorded activity before dropping logs.
pub(super) fn expired(conn: &Connection, key: &Key, cutoff: i64) -> anyhow::Result<bool> {
    Ok(activity(conn, key)?.is_some_and(|last| last < cutoff))
}

pub(super) fn activity(conn: &Connection, key: &Key) -> anyhow::Result<Option<i64>> {
    let mut last = None;
    for table in ["sessions", "retention_pending"] {
        let ts: Option<i64> = conn
            .prepare_cached(&format!(
                "SELECT last_activity_ms FROM {table} WHERE source=?1 AND session_id=?2"
            ))?
            .query_row(params![key.0, key.1], |r| r.get(0))
            .optional()?
            .flatten();
        last = last.max(ts);
    }
    for (table, timestamp) in PAYLOADS.iter().filter(|(_, ts)| !ts.is_empty()) {
        let value: Option<i64> = conn
            .prepare_cached(&format!(
                "SELECT MAX({timestamp}) FROM {table} WHERE source=?1 AND session_id=?2"
            ))?
            .query_row(params![key.0, key.1], |r| r.get(0))?;
        last = last.max(value);
    }
    Ok(last)
}

/// Protect the entire session if *any* of its feed rows (or an edge naming
/// it) has not reached every required destination. Rechecked under IMMEDIATE.
pub(super) fn uploaded(conn: &Connection, key: &Key, floor: i64) -> anyhow::Result<bool> {
    for table in [
        "sessions",
        "session_presences",
        "session_observations",
        "session_commit_links",
    ]
    .into_iter()
    .chain(PAYLOADS.iter().map(|(table, _)| *table))
    {
        if conn
            .prepare_cached(&format!(
                "SELECT 1 FROM {table} WHERE source=?1 AND session_id=?2 AND revision>?3 LIMIT 1"
            ))?
            .exists(params![key.0, key.1, floor])?
        {
            return Ok(false);
        }
    }
    for endpoint in ["parent_session_id", "child_session_id"] {
        if conn.prepare_cached(&format!("SELECT 1 FROM session_relationships WHERE source=?1 AND {endpoint}=?2 AND revision>?3 LIMIT 1"))?
            .exists(params![key.0,key.1,floor])? { return Ok(false); }
    }
    Ok(true)
}
