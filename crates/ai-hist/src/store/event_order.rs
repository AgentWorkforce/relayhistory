//! Recorded-order read positions and user-turn projections.
use super::*;

/// Resolve old `{ts_ms,id}` cursors through the stable canonical row. Once a
/// bucket has source ordering, a deleted cursor cannot safely guess its rank.
pub(super) fn event_cursor_order(
    conn: &Connection,
    session_id: &str,
    source: Option<&str>,
    cursor: &SessionEventCursor,
) -> Result<i64> {
    use rusqlite::OptionalExtension;
    let position = conn.query_row(
        "SELECT COALESCE(source_order,id) FROM session_events WHERE id=?1 AND session_id=?2 AND ts_ms=?3 AND (?4 IS NULL OR source=?4)",
        params![cursor.id,session_id,cursor.ts_ms,source], |row| row.get(0),
    ).optional()?;
    if let Some(position) = position {
        return Ok(position);
    }
    let ordered: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM session_events WHERE session_id=?1 AND ts_ms=?2 AND source_order IS NOT NULL AND (?3 IS NULL OR source=?3))",
        params![session_id,cursor.ts_ms,source], |row| row.get(0),
    )?;
    let exists: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM session_events WHERE id=?1)",
        [cursor.id],
        |row| row.get(0),
    )?;
    let legacy: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM session_events WHERE id=?1 AND session_id=?2 AND source_order IS NULL AND (?3 IS NULL OR source=?3))",
        params![cursor.id,session_id,source], |row| row.get(0),
    )?;
    // Old user-turn anchors combined MIN(ts_ms) and MIN(id), which can be
    // different rows. Preserve that positional cursor only in unknown-order
    // legacy buckets and the same source/session, never for ranked evidence.
    anyhow::ensure!(
        !exists || (legacy && !ordered),
        "INVALID_EVENT_CURSOR: row belongs to a different source, session or ordered timestamp"
    );
    anyhow::ensure!(
        !ordered,
        "STALE_EVENT_CURSOR: cursor row no longer exists in an ordered timestamp bucket"
    );
    Ok(cursor.id)
}

/// Every event of one session from a position onwards, in `(ts_ms, id)`
/// order, with whether it is a user-turn block. One range seek on
/// `idx_session_events_source_page`; the byte length is only computed for
/// block rows at or before the page's last block timestamp (the fifth
/// parameter), so a large tool result's text is never read just to be passed
/// over, whether it sits inside the page's span or in the rows read past it
/// while locating the following named message.
pub(super) fn user_turn_stream_sql() -> String {
    format!(
        "SELECT id, ts_ms, NULLIF(message_id, ''), \
                CASE WHEN {USER_TURN_ROW_FILTER} THEN 1 ELSE 0 END, \
                role, kind, tool_use_id, result_status, \
                CASE WHEN ts_ms <= ? AND {USER_TURN_ROW_FILTER} THEN {USER_TURN_BYTE_LEN} END, COALESCE(source_order,id) \
         FROM session_events \
         WHERE source = ? AND session_id = ? AND (ts_ms, COALESCE(source_order,id), id) >= (?, ?, ?) \
         ORDER BY ts_ms ASC, COALESCE(source_order,id) ASC, id ASC"
    )
}

/// Every turn of one session, grouped once.
///
/// The unpaged read uses this. A page does not: `HAVING` and `LIMIT` still
/// make SQLite aggregate every matching row first, so walking pages would
/// regroup the turns already returned. Per-key lookups for the whole
/// session would be one lookup per turn instead of this single pass.
pub(super) fn user_turn_header_sql() -> String {
    format!(
        "SELECT {USER_TURN_KEY} AS turn_key, MIN(ts_ms) AS turn_ts, MIN(id) AS turn_id, \
         MIN(NULLIF(message_id, '')) AS turn_message_id, MAX(ts_ms) AS turn_last_ts, MIN(id) AS event_order \
         FROM session_events \
         WHERE source = ? AND session_id = ? AND {USER_TURN_ROW_FILTER} \
         GROUP BY turn_key \
         ORDER BY turn_ts ASC, turn_id ASC"
    )
}

pub(super) fn ranked_turn_header_sql() -> String {
    format!(
        "WITH native AS (
            SELECT *, {USER_TURN_KEY} AS turn_key,
                ROW_NUMBER() OVER(PARTITION BY {USER_TURN_KEY} ORDER BY ts_ms,COALESCE(source_order,id),id) AS nth
            FROM session_events WHERE source=? AND session_id=? AND {USER_TURN_ROW_FILTER}
         ), grouped AS (
            SELECT turn_key,MIN(ts_ms) AS turn_ts,
                CASE WHEN COUNT(source_order)=0 THEN MIN(id) ELSE MAX(CASE WHEN nth=1 THEN id END) END AS anchor,
                MIN(NULLIF(message_id,'')) AS named,MAX(ts_ms) AS last_ts
            FROM native GROUP BY turn_key
         )
         SELECT turn_key,turn_ts,anchor,named,last_ts,
            (SELECT COALESCE(source_order,id) FROM native WHERE native.id=grouped.anchor) AS event_order
         FROM grouped ORDER BY turn_ts,event_order,anchor"
    )
}

pub(super) fn ensure_indexes(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_session_events_order ON session_events(session_id,ts_ms,COALESCE(source_order,id),id);
         CREATE INDEX IF NOT EXISTS idx_session_events_source_order ON session_events(source,session_id,ts_ms,COALESCE(source_order,id),id);
         CREATE INDEX IF NOT EXISTS idx_session_events_order_sequence ON session_events(source,session_id,ts_ms,source_order);"
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_session_events_source_page ON session_events(source, session_id, ts_ms, id)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_session_events_page ON session_events(session_id, ts_ms, id)",
        [],
    )?;
    Ok(())
}

pub(super) fn stream_position(row: &rusqlite::Row<'_>) -> rusqlite::Result<(i64, i64, i64)> {
    Ok((row.get(1)?, row.get(9)?, row.get(0)?))
}
