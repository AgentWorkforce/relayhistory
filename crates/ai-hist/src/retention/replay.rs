//! A small replay obligation, not retained provider payload.
use rusqlite::Connection;
const MARK: &str = "INSERT OR IGNORE INTO store_settings(key,value) VALUES(?1,'1')";
const NEEDS: &str = "SELECT EXISTS(SELECT 1 FROM store_settings WHERE key=?1)";
const CLEAR: &str = "DELETE FROM store_settings WHERE key=?1";
fn key(session: &str) -> String {
    format!("retention.grok_usage_replay:{session}")
}
pub(super) fn mark(conn: &Connection, session: &str) -> anyhow::Result<()> {
    conn.prepare_cached(MARK)?.execute([key(session)])?;
    Ok(())
}
pub(super) fn needed(conn: &Connection, session: &str) -> anyhow::Result<bool> {
    Ok(conn
        .prepare_cached(NEEDS)?
        .query_row([key(session)], |r| r.get(0))?)
}
pub(super) fn clear(conn: &Connection, session: &str) -> anyhow::Result<()> {
    conn.prepare_cached(CLEAR)?.execute([key(session)])?;
    Ok(())
}
pub(super) fn warm(conn: &Connection) -> anyhow::Result<()> {
    for sql in [MARK, NEEDS, CLEAR] {
        drop(conn.prepare_cached(sql)?);
    }
    Ok(())
}
