//! Reopen surviving continuity evidence before retiring the edge that names it.
use super::targets::Key;
use rusqlite::{params, Connection};

const REOPEN: &str = "UPDATE session_continuity_evidence SET pending_reason='unreconciled' WHERE source=?1 AND locator=?2";
const DELETE: &str = "DELETE FROM session_relationships WHERE rowid=?1";
fn select(column: &str) -> String {
    format!("SELECT rowid,evidence_locator,relationship FROM session_relationships WHERE source=?1 AND {column}=?2 LIMIT ?3")
}

pub(super) fn warm(conn: &Connection) -> anyhow::Result<()> {
    for column in ["parent_session_id", "child_session_id"] {
        drop(conn.prepare_cached(&select(column))?);
    }
    drop(conn.prepare_cached(REOPEN)?);
    drop(conn.prepare_cached(DELETE)?);
    Ok(())
}

pub(super) fn delete(
    conn: &Connection,
    column: &str,
    key: &Key,
    limit: u64,
) -> anyhow::Result<u64> {
    let rows = conn
        .prepare_cached(&select(column))?
        .query_map(params![key.0, key.1, limit], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut reopen = conn.prepare_cached(REOPEN)?;
    let mut delete = conn.prepare_cached(DELETE)?;
    for (rowid, locator, kind) in &rows {
        if matches!(kind.as_str(), "continuation" | "fork" | "resume") {
            // (source, locator) is the evidence primary key: at most one update
            // per removed edge, even if the survivor has many transcript files.
            reopen.execute(params![key.0, locator])?;
        }
        delete.execute([rowid])?;
    }
    Ok(rows.len() as u64)
}
