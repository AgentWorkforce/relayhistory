//! Dual prompt identity without changing any pre-existing record key.
use super::{record_history_presence, HistoryEntry, SessionLocation};
use anyhow::{ensure, Result};
use rusqlite::{params, Connection};

pub(super) fn ensure_schema(conn: &Connection) -> Result<()> {
    super::ensure_text_columns(conn, "history", super::REQUIRED_HISTORY_COLUMNS)?;
    migrate(conn)
}

pub(super) fn migrate(conn: &Connection) -> Result<()> {
    if super::migration_applied(conn, "history_event_identity_v1")? {
        return Ok(());
    }
    let sql: String = conn.query_row(
        "SELECT sql FROM sqlite_master WHERE name='history' COLLATE NOCASE",
        [],
        |r| r.get(0),
    )?;
    // The original inline UNIQUE creates an undroppable autoindex. Rebuild
    // only that legacy shape; every extra column, row id and revision survives.
    if let Some(range) = legacy_constraint(&sql) {
        rebuild(conn, &sql, range)?;
    }
    conn.execute_batch("CREATE UNIQUE INDEX IF NOT EXISTS history_legacy_identity ON history(source,timestamp_ms,prompt) WHERE event_uid IS NULL;
        CREATE UNIQUE INDEX IF NOT EXISTS history_event_identity ON history(source,session_id,event_uid) WHERE event_uid IS NOT NULL;
        INSERT INTO schema_migrations(name) VALUES('history_event_identity_v1');")?;
    Ok(())
}

// Published Python/Rust schemas differ only in SQL whitespace/quoting.
fn legacy_constraint(sql: &str) -> Option<std::ops::Range<usize>> {
    let positions = sql
        .char_indices()
        .filter(|(_, c)| !c.is_ascii_whitespace() && !matches!(c, '"' | '`' | '[' | ']'))
        .collect::<Vec<_>>();
    let normalized = positions
        .iter()
        .map(|(_, c)| c.to_ascii_lowercase())
        .collect::<String>();
    let target = "unique(source,timestamp_ms,prompt)";
    let byte_start = normalized.find(target)?;
    let start = normalized[..byte_start].chars().count();
    // The constraint is ASCII; preceding SQL may contain multibyte text.
    Some(positions[start].0..positions[start + target.len() - 1].0 + 1)
}

fn rebuild(conn: &Connection, sql: &str, range: std::ops::Range<usize>) -> Result<()> {
    let objects: Vec<String> = conn.prepare("SELECT sql FROM sqlite_master WHERE tbl_name='history' COLLATE NOCASE AND sql IS NOT NULL AND type IN ('trigger','index') AND name NOT LIKE 'change_feed_%' ORDER BY type")?
        .query_map([], |r| r.get(0))?.collect::<rusqlite::Result<_>>()?;
    let columns: Vec<String> = conn
        .prepare("PRAGMA table_info(history)")?
        .query_map([], |r| r.get(1))?
        .collect::<rusqlite::Result<_>>()?;
    let columns = columns
        .iter()
        .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(",");
    let mut ddl = sql.to_owned();
    ddl.replace_range(range,"CHECK(event_uid IS NULL OR (length(event_uid)>0 AND session_id IS NOT NULL AND length(session_id)>0))");
    let body = ddl
        .find('(')
        .ok_or_else(|| anyhow::anyhow!("history table has no column definition"))?;
    let ddl = format!("CREATE TABLE history_identity_rebuild {}", &ddl[body..]);
    let sequence: i64 = conn.query_row(
        "SELECT coalesce((SELECT seq FROM sqlite_sequence WHERE name='history' COLLATE NOCASE),0)",
        [],
        |r| r.get(0),
    )?;
    conn.execute_batch(&ddl)?;
    conn.execute_batch(&format!("INSERT INTO history_identity_rebuild({columns}) SELECT {columns} FROM history; DROP TABLE history;"))?;
    // External triggers/views can refer to history during the brief rename.
    let legacy: bool = conn.pragma_query_value(None, "legacy_alter_table", |r| r.get(0))?;
    conn.pragma_update(None, "legacy_alter_table", true)?;
    let renamed = conn.execute_batch("ALTER TABLE history_identity_rebuild RENAME TO history;");
    conn.pragma_update(None, "legacy_alter_table", legacy)?;
    renamed?;
    conn.execute(
        "UPDATE sqlite_sequence SET seq=max(seq,?1) WHERE name='history' COLLATE NOCASE",
        [sequence],
    )?;
    for object in objects {
        conn.execute_batch(&object)?;
    }
    Ok(())
}

pub(super) fn insert(
    conn: &Connection,
    entry: &HistoryEntry,
    location: SessionLocation,
) -> Result<usize> {
    if location == SessionLocation::Remote || entry.event_uid.is_some() {
        ensure!(
            entry.session_id.as_deref().is_some_and(|s| !s.is_empty()),
            "identified or remote history requires a non-empty session id"
        );
    }
    ensure!(
        entry.event_uid.as_deref().is_none_or(|s| !s.is_empty()),
        "history event_uid must not be empty"
    );
    record_history_presence(conn, entry, location)?;
    let hash = if entry.event_uid.is_some() {
        Some(super::prompt_hash(&entry.prompt))
    } else {
        entry.prompt_hash.clone()
    };
    let target = if entry.event_uid.is_some() {
        "(source,session_id,event_uid) WHERE event_uid IS NOT NULL DO UPDATE SET project=excluded.project,prompt=excluded.prompt,prompt_hash=excluded.prompt_hash,timestamp_ms=excluded.timestamp_ms WHERE history.project IS NOT excluded.project OR history.prompt IS NOT excluded.prompt OR history.prompt_hash IS NOT excluded.prompt_hash OR history.timestamp_ms IS NOT excluded.timestamp_ms"
    } else {
        "(source,timestamp_ms,prompt) WHERE event_uid IS NULL DO NOTHING"
    };
    Ok(conn.prepare_cached(&format!("INSERT INTO history(source,session_id,project,prompt,prompt_hash,timestamp_ms,event_uid) VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT{target}"))?
        .execute(params![entry.source,entry.session_id,entry.project,entry.prompt,hash,entry.timestamp_ms,entry.event_uid])?)
}

#[cfg(test)]
mod tests;
