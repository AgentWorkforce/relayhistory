//! Whether a database already has the schema [`super::init_db`] would apply.

use super::{
    EVIDENCE_LOCATION_TABLES, REQUIRED_HISTORY_COLUMNS, REQUIRED_HYDRATION_CHECKPOINT_COLUMNS,
    REQUIRED_HYDRATION_CURSOR_COLUMNS, REQUIRED_SCHEMA_MIGRATIONS, REQUIRED_SESSIONS_COLUMNS,
    REQUIRED_SESSION_EVENT_COLUMNS, REQUIRED_SESSION_MARKER_COLUMNS,
    REQUIRED_SESSION_PRESENCE_COLUMNS, REQUIRED_SESSION_RELATIONSHIP_COLUMNS, REQUIRED_TABLES,
    REQUIRED_TRIGGERS,
};
use anyhow::Result;
use rusqlite::Connection;
use std::collections::HashSet;

/// Whether the schema has every table, trigger, migration and column the
/// current version adds, plus `required_indexes`.
pub(super) fn schema_has_required_indexes(
    conn: &Connection,
    required_indexes: &[&str],
) -> Result<bool> {
    let mut table = conn.prepare("SELECT 1 FROM sqlite_master WHERE name = ? LIMIT 1")?;
    if !all_exist(&mut table, REQUIRED_TABLES)? || !all_exist(&mut table, REQUIRED_TRIGGERS)? {
        return Ok(false);
    }
    // A view is a query shape, not a row set: it can be present and still be
    // derived from a column set the table no longer has, which is why this
    // asks whether it is *current* rather than whether it exists.
    if !crate::session_usage::session_requests_view_is_current(conn)? {
        return Ok(false);
    }
    let mut migration = conn.prepare("SELECT 1 FROM schema_migrations WHERE name = ? LIMIT 1")?;
    if !all_exist(&mut migration, REQUIRED_SCHEMA_MIGRATIONS)? || !required_columns_exist(conn)? {
        return Ok(false);
    }
    let mut index =
        conn.prepare("SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ? LIMIT 1")?;
    if !all_exist(&mut index, required_indexes)? {
        return Ok(false);
    }
    // Rows or tables a retired source left behind are a pending migration
    // too: a read served before it would list sessions under a source this
    // build cannot name.
    Ok(!super::retired_sources::retired_sources_present(conn)?)
}

/// Whether `lookup`, a one-parameter existence query, finds every name.
fn all_exist(lookup: &mut rusqlite::Statement<'_>, names: &[&str]) -> Result<bool> {
    for name in names {
        if !lookup.exists([*name])? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Whether the column set `columns_sql` reads contains every `needed` name.
fn has_columns<'a>(
    conn: &Connection,
    columns_sql: &str,
    mut needed: impl Iterator<Item = &'a str>,
) -> Result<bool> {
    let columns: HashSet<String> = conn
        .prepare(columns_sql)?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<_>>()?;
    Ok(needed.all(|name| columns.contains(name)))
}

/// Whether every table carries the columns the current schema adds to it.
fn required_columns_exist(conn: &Connection) -> Result<bool> {
    for (columns_sql, needed) in [
        (
            "SELECT name FROM pragma_table_info('history')",
            REQUIRED_HISTORY_COLUMNS,
        ),
        (
            "SELECT name FROM pragma_table_info('sessions')",
            REQUIRED_SESSIONS_COLUMNS,
        ),
        (
            "SELECT name FROM pragma_table_info('session_hydration_checkpoints')",
            REQUIRED_HYDRATION_CHECKPOINT_COLUMNS,
        ),
        (
            "SELECT name FROM pragma_table_info('session_presences')",
            REQUIRED_SESSION_PRESENCE_COLUMNS,
        ),
    ] {
        if !has_columns(conn, columns_sql, needed.iter().copied())? {
            return Ok(false);
        }
    }
    for (columns_sql, declared) in [
        (
            "SELECT name FROM pragma_table_info('session_hydration_checkpoints')",
            REQUIRED_HYDRATION_CURSOR_COLUMNS,
        ),
        (
            "SELECT name FROM pragma_table_info('session_markers')",
            REQUIRED_SESSION_MARKER_COLUMNS,
        ),
        (
            "SELECT name FROM pragma_table_info('session_events')",
            REQUIRED_SESSION_EVENT_COLUMNS,
        ),
    ] {
        if !has_columns(conn, columns_sql, declared.iter().map(|(name, _)| *name))? {
            return Ok(false);
        }
    }
    Ok(evidence_tables_have_location(conn)?
        && has_columns(
            conn,
            "SELECT name FROM pragma_table_info('session_relationships')",
            REQUIRED_SESSION_RELATIONSHIP_COLUMNS.iter().copied(),
        )?)
}

fn evidence_tables_have_location(conn: &Connection) -> Result<bool> {
    for table in EVIDENCE_LOCATION_TABLES {
        let has_location: bool = conn
            .prepare("SELECT 1 FROM pragma_table_info(?) WHERE name = 'location'")?
            .exists([table])?;
        if !has_location {
            return Ok(false);
        }
    }
    Ok(true)
}
