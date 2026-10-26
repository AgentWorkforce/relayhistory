//! Local cache invalidation; never exposed as an upload deletion.
use rusqlite::{Connection, OptionalExtension};

const KEY: &str = "retention.catalog_generation";

pub(super) fn read(conn: &Connection) -> anyhow::Result<u64> {
    if !conn
        .prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='store_settings'")?
        .exists([])?
    {
        return Ok(0);
    }
    let value: Option<String> = conn
        .query_row(
            "SELECT value FROM store_settings WHERE key=?1",
            [KEY],
            |row| row.get(0),
        )
        .optional()?;
    value.map_or(Ok(0), |value| Ok(value.parse()?))
}

/// Caller holds the prune IMMEDIATE transaction. Overflow or corrupt state
/// aborts that transaction rather than publishing an ambiguous cache token.
pub(super) fn advance_if_evicted(
    conn: &Connection,
    rows: u64,
    catalog_changed: bool,
) -> anyhow::Result<()> {
    if rows == 0 && !catalog_changed {
        return Ok(());
    }
    let next = read(conn)?
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("retention catalog generation exhausted"))?;
    conn.execute("INSERT INTO store_settings(key,value) VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value", [KEY, &next.to_string()])?;
    Ok(())
}
