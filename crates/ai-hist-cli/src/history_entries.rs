//! Preserve native prompt identity in history reads and SQLite imports.
use super::*;

pub(super) fn get_entry(conn: &Connection, id: i64) -> Result<HistoryEntry> {
    conn.query_row(
        "SELECT id, source, session_id, project, prompt, timestamp_ms, event_uid FROM history WHERE id = ?",
        [id],
        row_to_entry,
    )
    .with_context(|| format!("No entry with id {id}"))
}

pub(super) fn row_to_entry(row: &rusqlite::Row<'_>) -> rusqlite::Result<HistoryEntry> {
    Ok(HistoryEntry {
        event_uid: row.get(6)?,
        id: row.get(0)?,
        source: row.get(1)?,
        session_id: row.get(2)?,
        project: row.get(3)?,
        prompt: row.get(4)?,
        prompt_hash: None,
        timestamp_ms: row.get(5)?,
    })
}

pub(super) fn query_entries(
    conn: &Connection,
    sql: &str,
    params_: &[&String],
) -> Result<Vec<HistoryEntry>> {
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt
        .query_map(rusqlite::params_from_iter(params_.iter()), row_to_entry)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

pub(super) fn load_sqlite_entries(path: &Path) -> Result<Vec<HistoryEntry>> {
    let src = Connection::open(path)?;
    let cols = src
        .prepare("PRAGMA table_info(history)")?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let has_hash = cols.iter().any(|col| col == "prompt_hash");
    let hash = if has_hash { "prompt_hash" } else { "NULL" };
    let uid = if cols.iter().any(|c| c == "event_uid") {
        "event_uid"
    } else {
        "NULL"
    };
    let sql = format!(
        "SELECT id, source, session_id, project, prompt, {hash}, timestamp_ms, {uid} FROM history"
    );
    let mut stmt = src.prepare(&sql)?;
    let entries = stmt
        .query_map([], |row| {
            let prompt: String = row.get(4)?;
            Ok(HistoryEntry {
                event_uid: row.get(7)?,
                id: row.get(0)?,
                source: row.get(1)?,
                session_id: row.get(2)?,
                project: row.get(3)?,
                prompt_hash: row
                    .get::<_, Option<String>>(5)?
                    .or_else(|| Some(prompt_hash(&prompt))),
                prompt,
                timestamp_ms: row.get(6)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(entries)
}
