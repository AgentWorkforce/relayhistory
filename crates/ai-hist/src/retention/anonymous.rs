//! A bounded, restartable walk independent of the named-session sweep.
use rusqlite::{params, Connection, OptionalExtension};
const CURSOR: &str = "retention.anonymous_cursor";
const READ_CURSOR: &str = "SELECT value FROM store_settings WHERE key='retention.anonymous_cursor'";
const SAVE_CURSOR: &str = "INSERT INTO store_settings(key,value) VALUES('retention.anonymous_cursor',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value";
pub(super) const CANDIDATES: &str = "SELECT id,revision,timestamp_ms FROM history INDEXED BY idx_retention_anonymous_activity WHERE session_id IS NULL AND timestamp_ms<?1 ORDER BY timestamp_ms,id LIMIT ?2";
pub(super) const AFTER_CURSOR: &str = "SELECT id,revision,timestamp_ms FROM history INDEXED BY idx_retention_anonymous_activity WHERE session_id IS NULL AND timestamp_ms<?1 AND timestamp_ms=?3 AND id>?4 ORDER BY timestamp_ms,id LIMIT ?2";
const AFTER_TIMESTAMP: &str = "SELECT id,revision,timestamp_ms FROM history INDEXED BY idx_retention_anonymous_activity WHERE session_id IS NULL AND timestamp_ms<?1 AND timestamp_ms>?3 ORDER BY timestamp_ms,id LIMIT ?2";
const DELETE: &str = "DELETE FROM history WHERE id=?1 AND revision<=?2";

pub(super) fn reset(conn: &Connection) -> anyhow::Result<()> {
    conn.execute("DELETE FROM store_settings WHERE key=?1", [CURSOR])?;
    Ok(())
}
pub(super) fn warm(conn: &Connection) -> anyhow::Result<()> {
    for sql in [
        READ_CURSOR,
        SAVE_CURSOR,
        CANDIDATES,
        AFTER_CURSOR,
        AFTER_TIMESTAMP,
        DELETE,
    ] {
        drop(conn.prepare_cached(sql)?);
    }
    Ok(())
}
pub(super) fn prune(
    conn: &Connection,
    cutoff: i64,
    floor: i64,
    limit: u32,
) -> anyhow::Result<(u64, u64)> {
    let rows = candidates(conn, cutoff, limit)?;
    let mut removed = 0;
    let mut stmt = conn.prepare_cached(DELETE)?;
    for (id, revision, _) in &rows {
        if *revision <= floor {
            removed += stmt.execute(params![id, floor])? as u64;
        }
    }
    if rows.len() < limit as usize {
        reset(conn)?;
    } else if let Some((id, _, timestamp)) = rows.last() {
        conn.prepare_cached(SAVE_CURSOR)?
            .execute([serde_json::to_string(&(*timestamp, *id))?])?;
    }
    Ok((removed, rows.len() as u64))
}

type Prompt = (i64, i64, i64);
fn page(
    conn: &Connection,
    sql: &str,
    params: impl rusqlite::Params,
) -> anyhow::Result<Vec<Prompt>> {
    Ok(conn
        .prepare_cached(sql)?
        .query_map(params, |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
        .collect::<rusqlite::Result<_>>()?)
}
fn candidates(conn: &Connection, cutoff: i64, limit: u32) -> anyhow::Result<Vec<Prompt>> {
    let cursor: Option<String> = conn
        .prepare_cached(READ_CURSOR)?
        .query_row([], |r| r.get(0))
        .optional()?;
    let cursor: Option<(i64, i64)> = cursor.as_deref().map(serde_json::from_str).transpose()?;
    let Some((timestamp, id)) = cursor else {
        return page(conn, CANDIDATES, params![cutoff, limit]);
    };
    // SQLite does not bound the rowid suffix of a tuple range when many
    // timestamps tie. Seek equal timestamps by id, then the later range.
    let mut rows = page(conn, AFTER_CURSOR, params![cutoff, limit, timestamp, id])?;
    if rows.len() < limit as usize {
        rows.extend(page(
            conn,
            AFTER_TIMESTAMP,
            params![cutoff, limit as usize - rows.len(), timestamp],
        )?);
    }
    Ok(rows)
}
