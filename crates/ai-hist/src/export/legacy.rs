//! One-time storage-subscription import from databases predating the split.
//! This never creates upload jobs or changes their queue/lease state.
use super::*;
fn exists(conn: &Connection, name: &str) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?)",
        [name],
        |r| r.get(0),
    )?)
}
pub(super) fn ensure_subscriptions(conn: &Connection) -> Result<()> {
    if !exists(conn, "delivery_jobs")? {
        return Ok(());
    }
    conn.execute_batch("CREATE TABLE IF NOT EXISTS history_subscriptions (id TEXT PRIMARY KEY, source TEXT, session_id TEXT, journal_cursor INTEGER NOT NULL, bootstrap_kind INTEGER NOT NULL DEFAULT 0, bootstrap_rowid INTEGER NOT NULL DEFAULT 0, bootstrap_done INTEGER NOT NULL DEFAULT 0);")?;
    adopt_subscriptions(conn)
}
pub(super) fn adopt_subscriptions(conn: &Connection) -> Result<()> {
    if !exists(conn, "delivery_jobs")? {
        return Ok(());
    }
    if conn.query_row("SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE name='probe_subscriptions_imported_v1')", [], |r| r.get::<_,bool>(0))? { return Ok(()); }
    conn.execute("INSERT OR IGNORE INTO history_subscriptions(id,journal_cursor,bootstrap_kind,bootstrap_rowid,bootstrap_done) SELECT id,journal_cursor,bootstrap_kind,bootstrap_rowid,bootstrap_done FROM delivery_jobs WHERE state <> 'cancelled'", [])?;
    if exists(conn, "delivery_session_members")? {
        conn.execute("INSERT OR IGNORE INTO history_subscriptions(id,source,session_id,journal_cursor,bootstrap_kind,bootstrap_rowid,bootstrap_done) SELECT m.id,m.source,m.session_id,m.cursor,m.bootstrap_kind,m.bootstrap_rowid,m.bootstrap_done FROM delivery_session_members m JOIN delivery_jobs j ON j.id=m.job_id WHERE j.state <> 'cancelled'", [])?;
    }
    conn.execute(
        "INSERT OR IGNORE INTO schema_migrations(name) VALUES ('probe_subscriptions_imported_v1')",
        [],
    )?;
    Ok(())
}
