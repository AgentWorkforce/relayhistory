//! Retirement of the anonymous prompt projections written by older parsers.
use super::*;

/// Retire the history row the previous parser wrote for a record this
/// session now stores differently, without erasing another session's prompt.
///
/// `history` is unique on `(source, timestamp_ms, prompt)` with no session in
/// the key, so a task notification in one session and a human prompt with the
/// same text at the same millisecond in another are one row, owned by
/// whichever transcript was read first. Deleting that row outright when the
/// owner turns out to be control would erase the other session's prompt --
/// its own re-read had already been ignored by the unique key -- so before
/// the delete, the row is handed to a session that has a substantive user
/// text event at that instant with that text. Only when no such session
/// exists is it deleted. The lookup runs only when this session actually owns
/// such a row, which outside the one-time re-read is never.
pub(super) fn retire_claude_history_row(
    conn: &Connection,
    session_id: &str,
    ts_ms: i64,
    raw: &str,
) -> Result<()> {
    let owned: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM history \
         WHERE source = 'claude' AND session_id = ?1 AND timestamp_ms = ?2 AND prompt = ?3 AND event_uid IS NULL)",
        params![session_id, ts_ms, raw],
        |row| row.get(0),
    )?;
    if !owned {
        return Ok(());
    }
    let handed_over = conn.execute(
        "UPDATE history SET session_id = ( \
             SELECT e.session_id FROM session_events e \
             WHERE e.source = 'claude' AND e.session_id <> ?1 AND e.ts_ms = ?2 \
               AND e.role = 'user' AND e.kind = 'text' AND e.control_kind IS NULL \
               AND TRIM(e.text) = ?3 \
             ORDER BY e.session_id LIMIT 1), \
           project = COALESCE(( \
             SELECT e.project FROM session_events e \
             WHERE e.source = 'claude' AND e.session_id <> ?1 AND e.ts_ms = ?2 \
               AND e.role = 'user' AND e.kind = 'text' AND e.control_kind IS NULL \
               AND TRIM(e.text) = ?3 \
             ORDER BY e.session_id LIMIT 1), project) \
         WHERE source = 'claude' AND session_id = ?1 AND timestamp_ms = ?2 AND prompt = ?3 AND event_uid IS NULL \
           AND EXISTS( \
             SELECT 1 FROM session_events e \
             WHERE e.source = 'claude' AND e.session_id <> ?1 AND e.ts_ms = ?2 \
               AND e.role = 'user' AND e.kind = 'text' AND e.control_kind IS NULL \
               AND TRIM(e.text) = ?3)",
        params![session_id, ts_ms, raw],
    )?;
    if handed_over == 0 {
        conn.execute(
            "DELETE FROM history \
             WHERE source = 'claude' AND session_id = ?1 AND timestamp_ms = ?2 AND prompt = ?3 AND event_uid IS NULL",
            params![session_id, ts_ms, raw],
        )?;
    }
    Ok(())
}
