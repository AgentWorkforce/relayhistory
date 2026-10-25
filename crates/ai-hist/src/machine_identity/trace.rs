//! Per-session trace ids: a UUIDv7 on every `sessions` row, minted by the
//! change feed's insert stamp.
use anyhow::Result;
use rusqlite::Connection;

/// The `sessions` columns this module adds.
pub(crate) const SESSION_COLUMNS: &[&str] = &["trace_id"];

/// The marker for the one-time trace id backfill of rows that predate it.
pub(crate) const TRACE_ID_MIGRATION: &str = "session_trace_id_v1";

/// The standalone trigger an earlier revision of this change minted trace
/// ids with; the change feed's insert stamp does it now.
const RETIRED_TRACE_ID_TRIGGER: &str = "sessions_trace_id_insert";

/// A fresh UUIDv7 as one SQL expression: 48 bits of Unix milliseconds, the
/// version and variant nibbles, 74 random bits.
///
/// Inline rather than a subquery, so an `UPDATE` evaluates it per row: SQLite
/// runs an uncorrelated subquery once per statement. `'now'` is fixed for one
/// statement, so both timestamp halves agree; each random group draws its own
/// `randomblob`. Only core functions -- `julianday` rather than
/// `unixepoch('subsec')`, which older SQLite builds lack -- so any client that
/// writes a session row can run the trigger. `random() & 3` rather than
/// `abs(random()) % 4`: `abs` overflows on the smallest integer.
pub(crate) const UUID_V7_SQL: &str = "lower(\
     substr(printf('%012x', CAST((julianday('now') - 2440587.5) * 86400000.0 AS INTEGER)), 1, 8) \
     || '-' || substr(printf('%012x', CAST((julianday('now') - 2440587.5) * 86400000.0 AS INTEGER)), 9, 4) \
     || '-7' || substr(hex(randomblob(2)), 2, 3) \
     || '-' || substr('89ab', (random() & 3) + 1, 1) || substr(hex(randomblob(2)), 2, 3) \
     || '-' || hex(randomblob(6)))";

/// What the change feed's insert stamp on `table` also sets, as a `, col =
/// expr` suffix of its `UPDATE ... SET`: a new session's trace id, unless the
/// insert gave one. Filling it in the stamp's own statement means a new
/// session takes one revision, not one for the insert and one for the fill.
/// Empty for every other table, and for `sessions` before the column exists.
pub(crate) fn insert_fill_sql(conn: &Connection, table: &str) -> Result<String> {
    if table != "sessions" {
        return Ok(String::new());
    }
    let has_column: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('sessions') WHERE name = 'trace_id')",
        [],
        |row| row.get(0),
    )?;
    Ok(if has_column {
        format!(", trace_id = COALESCE(trace_id, {UUID_V7_SQL})")
    } else {
        String::new()
    })
}

/// Once, give every existing session row a trace id. Runs inside the
/// caller's migration transaction, after the `trace_id` column exists and
/// before the change feed reconciles its export schema, which then restamps
/// the backfilled rows once.
pub(super) fn init_trace_ids(conn: &Connection) -> Result<()> {
    conn.execute_batch(&format!(
        "DROP TRIGGER IF EXISTS {RETIRED_TRACE_ID_TRIGGER};"
    ))?;
    if !crate::store::migration_applied(conn, TRACE_ID_MIGRATION)? {
        conn.execute_batch(&format!(
            "UPDATE sessions SET trace_id = {UUID_V7_SQL} WHERE trace_id IS NULL; \
             INSERT OR IGNORE INTO schema_migrations (name) VALUES ('{TRACE_ID_MIGRATION}');"
        ))?;
    }
    Ok(())
}
