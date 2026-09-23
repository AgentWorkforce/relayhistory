//! Probe-owned upload tables. Evidence schema and snapshot capture stay in ai-hist.
use anyhow::Result;
use rusqlite::Connection;
pub(super) fn is_current(conn: &Connection) -> Result<bool> {
    if !conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('delivery_jobs') WHERE name='retry_build')",
        [],
        |r| r.get::<_, bool>(0),
    )? {
        return Ok(false);
    }
    if !conn.query_row("SELECT EXISTS(SELECT 1 FROM pragma_table_info('delivery_session_jobs') WHERE name='last_member')", [], |r| r.get::<_,bool>(0))? { return Ok(false); }
    Ok(conn.query_row("SELECT COUNT(*)=10 FROM sqlite_master WHERE (type='table' AND name IN ('delivery_jobs','delivery_batches','delivery_session_jobs','delivery_session_members')) OR (type='trigger' AND name IN ('delivery_session_ready','delivery_batches_cap_insert','delivery_batches_count_insert','delivery_batches_cap_update','delivery_batches_count_update','delivery_batches_count_delete'))", [], |r| r.get(0))?)
}
pub(super) fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(r#"CREATE TABLE IF NOT EXISTS delivery_jobs (
    id TEXT PRIMARY KEY, destination_id TEXT NOT NULL, instance_id TEXT NOT NULL, account_id TEXT NOT NULL,
    generation INTEGER NOT NULL, config_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('active','paused','blocked','cancelled')),
    created_ms INTEGER NOT NULL, cutoff INTEGER NOT NULL, journal_cursor INTEGER NOT NULL,
    bootstrap_kind INTEGER NOT NULL DEFAULT 0, bootstrap_rowid INTEGER NOT NULL DEFAULT 0,
    bootstrap_done INTEGER NOT NULL DEFAULT 0, acknowledged_cursor INTEGER NOT NULL DEFAULT 0,
    fence INTEGER NOT NULL DEFAULT 0, worker_id TEXT, lease_until_ms INTEGER,
    next_attempt_ms INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,
    last_attempt_ms INTEGER, last_acknowledged_ms INTEGER, acceptance_level TEXT, failure TEXT, retry_build TEXT,
    suppressed_records INTEGER NOT NULL DEFAULT 0, acknowledged_records INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS delivery_active_instance ON delivery_jobs(destination_id,instance_id,account_id) WHERE state <> 'cancelled';
CREATE TABLE IF NOT EXISTS delivery_batches (
    seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE, job_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('pending','leased','retry_wait','blocked','acknowledged','suppressed','cancelled')),
    payload TEXT, prepared TEXT, records INTEGER NOT NULL, bytes INTEGER NOT NULL,
    journal_end INTEGER NOT NULL, created_ms INTEGER NOT NULL, accepted_records INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS delivery_one_pending_batch ON delivery_batches(job_id) WHERE state IN ('pending','leased','retry_wait','blocked');
CREATE INDEX IF NOT EXISTS delivery_batch_job ON delivery_batches(job_id, seq);
CREATE TABLE IF NOT EXISTS delivery_session_jobs (job_id TEXT PRIMARY KEY, last_member TEXT NOT NULL DEFAULT '');
CREATE TABLE IF NOT EXISTS delivery_session_members (
 id TEXT NOT NULL UNIQUE, job_id TEXT NOT NULL, source TEXT NOT NULL, session_id TEXT NOT NULL,
 cutoff INTEGER NOT NULL, cursor INTEGER NOT NULL, bootstrap_kind INTEGER NOT NULL DEFAULT 0,
 bootstrap_rowid INTEGER NOT NULL DEFAULT 0, bootstrap_done INTEGER NOT NULL DEFAULT 0,
 ready INTEGER NOT NULL DEFAULT 1, PRIMARY KEY(job_id,source,session_id)
);
CREATE INDEX IF NOT EXISTS delivery_member_ready ON delivery_session_members(job_id,ready,id);
CREATE INDEX IF NOT EXISTS delivery_member_snapshot ON delivery_session_members(job_id,bootstrap_done);
CREATE INDEX IF NOT EXISTS delivery_member_cursor ON delivery_session_members(job_id,cursor);
CREATE INDEX IF NOT EXISTS delivery_member_identity ON delivery_session_members(source,session_id);
CREATE TRIGGER IF NOT EXISTS delivery_session_ready AFTER INSERT ON delivery_journal BEGIN
 UPDATE delivery_session_members SET ready=1 WHERE source=NEW.source AND session_id=NEW.session_id AND ready=0;
END;
"#)?;
    if !conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('delivery_jobs') WHERE name='retry_build')",
        [],
        |r| r.get::<_, bool>(0),
    )? {
        conn.execute_batch("ALTER TABLE delivery_jobs ADD COLUMN retry_build TEXT;")?;
    }
    if !conn.query_row("SELECT EXISTS(SELECT 1 FROM pragma_table_info('delivery_session_jobs') WHERE name='last_member')", [], |r| r.get::<_,bool>(0))? {
        conn.execute_batch("ALTER TABLE delivery_session_jobs ADD COLUMN last_member TEXT NOT NULL DEFAULT '';")?;
    }
    let table = "delivery_batches";
    let size =
        "coalesce(length(CAST(payload AS BLOB)),0)+coalesce(length(CAST(prepared AS BLOB)),0)+512";
    let qualified = |row: &str| {
        size.replace("payload", &format!("{row}.payload"))
            .replace("record_key", &format!("{row}.record_key"))
            .replace("prepared", &format!("{row}.prepared"))
    };
    let new = qualified("NEW");
    let old = qualified("OLD");
    conn.execute_batch(&format!(r#"
CREATE TRIGGER IF NOT EXISTS {table}_cap_insert BEFORE INSERT ON {table} BEGIN
 SELECT CASE WHEN (SELECT retained_bytes+({new})>max_retained_bytes FROM delivery_state WHERE singleton=1) THEN RAISE(ABORT,'delivery retention limit exceeded; compact consumed data or raise the retention cap') END;
END;
CREATE TRIGGER IF NOT EXISTS {table}_count_insert AFTER INSERT ON {table} BEGIN
 UPDATE delivery_state SET retained_bytes=retained_bytes+({new}) WHERE singleton=1;
END;
CREATE TRIGGER IF NOT EXISTS {table}_cap_update BEFORE UPDATE ON {table} BEGIN
 SELECT CASE WHEN (SELECT retained_bytes+({new})-({old})>max_retained_bytes FROM delivery_state WHERE singleton=1) THEN RAISE(ABORT,'delivery retention limit exceeded; compact consumed data or raise the retention cap') END;
END;
CREATE TRIGGER IF NOT EXISTS {table}_count_update AFTER UPDATE ON {table} BEGIN
 UPDATE delivery_state SET retained_bytes=retained_bytes+({new})-({old}) WHERE singleton=1;
END;
CREATE TRIGGER IF NOT EXISTS {table}_count_delete AFTER DELETE ON {table} BEGIN
 UPDATE delivery_state SET retained_bytes=retained_bytes-({old}) WHERE singleton=1;
END;
"#))?;
    Ok(())
}
