//! Probe-owned upload tables. Evidence schema and snapshot capture stay in ai-hist.
use anyhow::Result;
use rusqlite::Connection;
/// The retained bytes one `delivery_batches` row accounts for: its bodies plus
/// a fixed allowance for the row itself.
pub(super) const BATCH_ROW_BYTES: &str =
    "coalesce(length(CAST(payload AS BLOB)),0)+coalesce(length(CAST(prepared AS BLOB)),0)+512";
pub(super) fn is_current(conn: &Connection) -> Result<bool> {
    if !conn.query_row("SELECT EXISTS(SELECT 1 FROM pragma_table_info('delivery_session_jobs') WHERE name='last_member')", [], |r| r.get::<_,bool>(0))? { return Ok(false); }
    // A build that predates the batch reserve recreates its own plain-cap
    // triggers beside the reserve ones, and they refuse a batch write the
    // reserve allows. Their presence alone makes the schema stale.
    if conn.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='trigger' AND name IN ('delivery_batches_cap_insert','delivery_batches_cap_update'))", [], |r| r.get::<_,bool>(0))? { return Ok(false); }
    Ok(conn.query_row("SELECT COUNT(*)=10 FROM sqlite_master WHERE (type='table' AND name IN ('delivery_jobs','delivery_batches','delivery_session_jobs','delivery_session_members')) OR (type='trigger' AND name IN ('delivery_session_ready','delivery_batches_reserve_insert','delivery_batches_count_insert','delivery_batches_reserve_update','delivery_batches_count_update','delivery_batches_count_delete'))", [], |r| r.get(0))?)
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
    last_attempt_ms INTEGER, last_acknowledged_ms INTEGER, acceptance_level TEXT, failure TEXT,
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
    if !conn.query_row("SELECT EXISTS(SELECT 1 FROM pragma_table_info('delivery_session_jobs') WHERE name='last_member')", [], |r| r.get::<_,bool>(0))? {
        conn.execute_batch("ALTER TABLE delivery_session_jobs ADD COLUMN last_member TEXT NOT NULL DEFAULT '';")?;
    }
    let table = "delivery_batches";
    let qualified = |row: &str| {
        BATCH_ROW_BYTES
            .replace("payload", &format!("{row}.payload"))
            .replace("prepared", &format!("{row}.prepared"))
    };
    let new = qualified("NEW");
    let old = qualified("OLD");
    // Batch materialization always has room: a batch is the deliverable form
    // of journal rows the cap already holds, and the only way a journal full
    // of unconsumed backlog ever drains. Batch writes are checked against the
    // cap plus a reserve that is bounded by design: every non-cancelled job
    // holds at most one unresolved batch of at most its configured payload
    // and prepared bytes plus the row's own accounting, and settled receipts
    // keep their accounting until compaction releases them. Every other
    // retained table is checked against the plain cap.
    let settled = "state IN ('acknowledged','suppressed','cancelled')";
    let reserve = format!("(SELECT COALESCE(SUM(json_extract(config_json,'$.limits.max_batch_bytes')+json_extract(config_json,'$.limits.max_prepared_bytes')+512),0) FROM delivery_jobs WHERE state<>'cancelled')+(SELECT COUNT(*)*512 FROM delivery_batches WHERE {settled})");
    // An update that settles a row earns that row's receipt allowance in the
    // same statement. The count above still sees the row unresolved, and
    // cancelling a job drops the share that covered it in the transaction
    // that settles its batch.
    let settling =
        format!("{reserve}+CASE WHEN NEW.{settled} AND NOT OLD.{settled} THEN 512 ELSE 0 END");
    conn.execute_batch(&format!(r#"
DROP TRIGGER IF EXISTS {table}_cap_insert;
DROP TRIGGER IF EXISTS {table}_cap_update;
CREATE TRIGGER IF NOT EXISTS {table}_reserve_insert BEFORE INSERT ON {table} BEGIN
 SELECT CASE WHEN (SELECT retained_bytes+({new})>max_retained_bytes+{reserve} FROM delivery_state WHERE singleton=1) THEN RAISE(ABORT,'delivery retention limit exceeded; compact consumed data or raise the retention cap') END;
END;
CREATE TRIGGER IF NOT EXISTS {table}_count_insert AFTER INSERT ON {table} BEGIN
 UPDATE delivery_state SET retained_bytes=retained_bytes+({new}) WHERE singleton=1;
END;
CREATE TRIGGER IF NOT EXISTS {table}_reserve_update BEFORE UPDATE ON {table} BEGIN
 SELECT CASE WHEN (SELECT retained_bytes+({new})-({old})>max_retained_bytes+{settling} FROM delivery_state WHERE singleton=1) THEN RAISE(ABORT,'delivery retention limit exceeded; compact consumed data or raise the retention cap') END;
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
