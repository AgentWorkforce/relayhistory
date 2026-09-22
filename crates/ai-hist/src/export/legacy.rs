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
    retire_session_job_roots(conn)?;
    if conn.query_row("SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE name='probe_subscriptions_imported_v1')", [], |r| r.get::<_,bool>(0))? { return Ok(()); }
    // A session job subscribes through its members alone: a root row would
    // read as interest in every session.
    let roots = if exists(conn, "delivery_session_jobs")? {
        "SELECT id,journal_cursor,bootstrap_kind,bootstrap_rowid,bootstrap_done FROM delivery_jobs WHERE state <> 'cancelled' AND id NOT IN (SELECT job_id FROM delivery_session_jobs)"
    } else {
        "SELECT id,journal_cursor,bootstrap_kind,bootstrap_rowid,bootstrap_done FROM delivery_jobs WHERE state <> 'cancelled'"
    };
    conn.execute(&format!("INSERT OR IGNORE INTO history_subscriptions(id,journal_cursor,bootstrap_kind,bootstrap_rowid,bootstrap_done) {roots}"), [])?;
    if exists(conn, "delivery_session_members")? {
        conn.execute("INSERT OR IGNORE INTO history_subscriptions(id,source,session_id,journal_cursor,bootstrap_kind,bootstrap_rowid,bootstrap_done) SELECT m.id,m.source,m.session_id,m.cursor,m.bootstrap_kind,m.bootstrap_rowid,m.bootstrap_done FROM delivery_session_members m JOIN delivery_jobs j ON j.id=m.job_id WHERE j.state <> 'cancelled'", [])?;
    }
    conn.execute(
        "INSERT OR IGNORE INTO schema_migrations(name) VALUES ('probe_subscriptions_imported_v1')",
        [],
    )?;
    Ok(())
}
/// A session job's own subscription row and snapshot bounds serve nothing its
/// members do not: the members carry the snapshots and cursors, and a root row
/// would gate capture open for every session. Every lookup is indexed.
fn retire_session_job_roots(conn: &Connection) -> Result<()> {
    if !exists(conn, "delivery_session_jobs")? {
        return Ok(());
    }
    conn.execute(
        "DELETE FROM history_subscriptions WHERE source IS NULL AND id IN (SELECT job_id FROM delivery_session_jobs)",
        [],
    )?;
    if exists(conn, "delivery_bootstrap_bounds")? {
        conn.execute(
            "DELETE FROM delivery_bootstrap_bounds WHERE job_id IN (SELECT job_id FROM delivery_session_jobs)",
            [],
        )?;
    }
    Ok(())
}
