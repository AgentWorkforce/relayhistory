//! Service-independent, opt-in durable delivery. No transport or credential I/O.
//!
//! Jobs bootstrap a historical snapshot in bounded pages, retaining preimages
//! for rows changed before their page is read. SQLite triggers capture later
//! revisions atomically with ingestion. A job has at most one unresolved batch;
//! its immutable generic records and optional destination body survive crashes.
//! Acknowledgment requires every revision and a durable remote acceptance level.
//! Delivery is at least once; receiver idempotency and stale-revision rejection
//! are necessary to prevent duplicate effects after uncertain outcomes.
//!
//! The retention cap bounds logical retained bytes, not physical database pages.
//! Capture fails the affected SQLite statement visibly when full. Callers that
//! transact ingestion and provider checkpoints must roll back that transaction
//! on error. Materializing a batch needs headroom in this same cap; if retained
//! journal data fills it, compact already-consumed journal/receipts or explicitly
//! raise the cap with `set_retention_limit` before draining. No checkpoint moves
//! on a capacity failure. Pausing preserves capture; cancellation is an explicit discard.
//! Deletes are exported as tombstones, but remote deletion requires a destination
//! that supports them. Presence is its own revisioned provenance evidence kind.

mod schema;
mod snapshot;
pub use snapshot::{
    close_export, create_export, expire_exports, export_page, ExportHandle, HistoryExportPage,
};

use anyhow::{ensure, Context, Result};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;

pub(crate) use schema::{init_schema, schema_is_current};
pub const EXPORT_SCHEMA_VERSION: u32 = 1;
pub const SUPPORTED_KINDS: &[&str] = &[
    "history",
    "session_event",
    "tool_call",
    "file_edit",
    "session",
    "presence",
    "relationship",
    "commit_link",
    "trajectory",
];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct SessionIdentity {
    pub source: String,
    pub session_id: String,
}

/// A nonempty explicit selection. Sources and sessions form a union. Empty
/// lists select nothing unless all_sources is true; kinds must be explicit.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExportSelection {
    pub all_sources: bool,
    pub sources: Vec<String>,
    pub sessions: Vec<SessionIdentity>,
    pub kinds: Vec<String>,
    pub excluded_sessions: Vec<SessionIdentity>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeliveryLimits {
    pub max_batch_records: usize,
    pub max_batch_bytes: usize,
    /// Bounds both bootstrap and journal scans, including excluded rows.
    pub max_scan_records: usize,
    pub max_prepared_bytes: usize,
}
impl Default for DeliveryLimits {
    fn default() -> Self {
        Self {
            max_batch_records: 100,
            max_batch_bytes: 1_048_576,
            max_scan_records: 400,
            max_prepared_bytes: 2_097_152,
        }
    }
}

/// All identifiers are non-secret application labels. Never put authentication
/// material in configuration. A new selection/mapping/account requires a new
/// generation after the old job is explicitly cancelled, not cursor reuse.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeliveryJobConfig {
    pub destination_id: String,
    pub instance_id: String,
    pub account_id: String,
    pub mapping_version: String,
    pub selection: ExportSelection,
    pub limits: DeliveryLimits,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HistoryExportRecord {
    pub schema_version: u32,
    pub origin_id: String,
    pub record_id: String,
    pub revision_id: String,
    pub revision: i64,
    pub kind: String,
    pub source: String,
    pub session_id: Option<String>,
    pub operation: String,
    /// Original stored field names, timestamps and raw JSON strings are kept.
    /// A tombstone has a null payload and retains its logical record identity.
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HistoryExportBatch {
    pub schema_version: u32,
    pub origin_id: String,
    pub batch_id: String,
    pub job_id: String,
    pub generation: i64,
    pub destination_id: String,
    pub instance_id: String,
    pub account_id: String,
    pub mapping_version: String,
    pub records: Vec<HistoryExportRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeliveryLease {
    pub job_id: String,
    pub batch_id: String,
    pub worker_id: String,
    pub fence: i64,
    pub expires_at_ms: i64,
}

/// Destination body only, without authorization headers or credentials.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PreparedPayload {
    pub mapping_version: String,
    pub content_type: String,
    pub body: String,
    pub sha256: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ClaimedBatch {
    pub lease: DeliveryLease,
    pub batch: HistoryExportBatch,
    pub prepared: Option<PreparedPayload>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AcceptanceLevel {
    Durable,
    Indexed,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeliveryAcknowledgment {
    pub batch_id: String,
    /// Exact revision_ids, not bare record IDs or a remote high-water mark.
    pub accepted_revision_ids: Vec<String>,
    /// Unsupported evidence blocks the job; it is never counted as delivered.
    pub unsupported_revision_ids: Vec<String>,
    pub acceptance_level: AcceptanceLevel,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryFailure {
    Transient,
    RateLimited,
    AuthenticationRequired,
    PermissionDenied,
    InvalidPayload,
    UnsupportedEvidence,
    MappingVersionMismatch,
}
impl DeliveryFailure {
    fn retryable(self) -> bool {
        matches!(self, Self::Transient | Self::RateLimited)
    }
    fn code(self) -> &'static str {
        match self {
            Self::Transient => "transient",
            Self::RateLimited => "rate_limited",
            Self::AuthenticationRequired => "authentication_required",
            Self::PermissionDenied => "permission_denied",
            Self::InvalidPayload => "invalid_payload",
            Self::UnsupportedEvidence => "unsupported_evidence",
            Self::MappingVersionMismatch => "mapping_version_mismatch",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeliveryStatus {
    pub job_id: String,
    pub config: DeliveryJobConfig,
    pub generation: i64,
    pub state: String,
    pub bootstrap_complete: bool,
    pub journal_cursor: i64,
    pub acknowledged_cursor: i64,
    pub pending_records: i64,
    pub pending_bytes: i64,
    pub oldest_pending_ms: Option<i64>,
    pub unqueued_changes: i64,
    pub next_attempt_ms: i64,
    pub last_attempt_ms: Option<i64>,
    pub last_acknowledged_ms: Option<i64>,
    pub acceptance_level: Option<String>,
    pub failure: Option<String>,
    pub suppressed_records: i64,
    pub acknowledged_records: i64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrepareResult {
    pub batch_id: Option<String>,
    pub scanned_records: usize,
    pub bootstrap_complete: bool,
}

#[derive(Debug)]
struct Job {
    id: String,
    config: DeliveryJobConfig,
    generation: i64,
    state: String,
    cutoff: i64,
    cursor: i64,
    bootstrap_kind: usize,
    bootstrap_rowid: i64,
    bootstrap_done: bool,
}
fn job(conn: &Connection, id: &str) -> Result<Job> {
    let value = conn.query_row(
        "SELECT config_json,generation,state,cutoff,journal_cursor,bootstrap_kind,bootstrap_rowid,bootstrap_done FROM delivery_jobs WHERE id=?",
        [id], |row| Ok((row.get::<_, String>(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get::<_, i64>(5)?,row.get(6)?,row.get::<_, bool>(7)?)),
    ).optional()?.context("delivery job not found")?;
    Ok(Job {
        id: id.into(),
        config: serde_json::from_str(&value.0)?,
        generation: value.1,
        state: value.2,
        cutoff: value.3,
        cursor: value.4,
        bootstrap_kind: value.5 as usize,
        bootstrap_rowid: value.6,
        bootstrap_done: value.7,
    })
}
fn write_transaction(conn: &Connection) -> Result<Transaction<'_>> {
    ensure!(
        conn.is_autocommit(),
        "delivery operations require their own short transaction"
    );
    Ok(Transaction::new_unchecked(
        conn,
        TransactionBehavior::Immediate,
    )?)
}
fn hash(value: impl AsRef<[u8]>) -> String {
    format!("{:x}", Sha256::digest(value.as_ref()))
}
fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 200
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.:/@".contains(&b))
}
fn validate(config: &DeliveryJobConfig) -> Result<()> {
    for value in [
        &config.destination_id,
        &config.instance_id,
        &config.account_id,
        &config.mapping_version,
    ] {
        ensure!(
            identifier(value),
            "delivery identifiers must be nonempty non-secret labels (maximum 200 characters)"
        );
    }
    let selection = &config.selection;
    ensure!(
        selection.all_sources || !selection.sources.is_empty() || !selection.sessions.is_empty(),
        "explicit export source/session selection required"
    );
    ensure!(
        !selection.kinds.is_empty()
            && selection
                .kinds
                .iter()
                .all(|kind| SUPPORTED_KINDS.contains(&kind.as_str())),
        "unsupported or empty export evidence selection"
    );
    ensure!(
        serde_json::to_vec(config)?.len() <= 65_536,
        "delivery configuration too large"
    );
    let limits = &config.limits;
    ensure!(
        (1..=10_000).contains(&limits.max_batch_records),
        "invalid batch record limit"
    );
    ensure!(
        (512..=16_777_216).contains(&limits.max_batch_bytes),
        "invalid batch byte limit"
    );
    ensure!(
        (1..=10_000).contains(&limits.max_scan_records),
        "invalid scan record limit"
    );
    ensure!(
        (1..=33_554_432).contains(&limits.max_prepared_bytes),
        "invalid prepared payload limit"
    );
    Ok(())
}

/// Explicitly enables capture. Repeating an identical configuration is
/// idempotent. Existing credentials or plugin installation never create jobs.
pub fn create_job(
    conn: &Connection,
    config: &DeliveryJobConfig,
    now_ms: i64,
) -> Result<DeliveryStatus> {
    validate(config)?;
    ensure!(now_ms >= 0, "invalid clock");
    let tx = write_transaction(conn)?;
    let existing: Option<String> = tx.query_row("SELECT id FROM delivery_jobs WHERE destination_id=? AND instance_id=? AND account_id=? AND state <> 'cancelled'", params![config.destination_id,config.instance_id,config.account_id], |row| row.get(0)).optional()?;
    if let Some(id) = existing {
        ensure!(job(&tx,&id)?.config == *config, "delivery configuration changed; explicitly cancel the old generation before creating a new one");
        tx.commit()?;
        return status(conn, &id);
    }
    let count: i64 = tx.query_row(
        "SELECT COUNT(*) FROM delivery_jobs WHERE state <> 'cancelled'",
        [],
        |row| row.get(0),
    )?;
    ensure!(count < 32, "maximum active delivery jobs reached");
    let id: String = tx.query_row("SELECT lower(hex(randomblob(16)))", [], |row| row.get(0))?;
    let generation: i64 = tx.query_row("SELECT COALESCE(MAX(generation),0)+1 FROM delivery_jobs WHERE destination_id=? AND instance_id=? AND account_id=?", params![config.destination_id,config.instance_id,config.account_id], |row| row.get(0))?;
    // Reserve a fresh revision for this snapshot, even after a period with
    // capture disabled. The same revision ID must never name changed payloads.
    tx.execute("INSERT INTO delivery_journal(kind,source,record_key,operation,payload) VALUES ('__cutoff','','','checkpoint','null')", [])?;
    let cutoff = tx.last_insert_rowid();
    tx.execute("INSERT INTO delivery_jobs(id,destination_id,instance_id,account_id,generation,config_json,state,created_ms,cutoff,journal_cursor) VALUES (?,?,?,?,?,?,'active',?,?,?)", params![id,config.destination_id,config.instance_id,config.account_id,generation,serde_json::to_string(config)?,now_ms,cutoff,cutoff])?;
    for table in schema::TABLES {
        tx.execute(&format!("INSERT INTO delivery_bootstrap_bounds(job_id,kind,max_rowid) SELECT ?,?,COALESCE(MAX(rowid),0) FROM {}",table.name), params![id,table.kind])?;
    }
    tx.commit()?;
    status(conn, &id)
}

pub fn status(conn: &Connection, job_id: &str) -> Result<DeliveryStatus> {
    let job = job(conn, job_id)?;
    let (pending_records,pending_bytes,oldest_pending_ms): (i64,i64,Option<i64>) = conn.query_row("SELECT COALESCE(SUM(records),0),COALESCE(SUM(bytes+COALESCE(length(CAST(prepared AS BLOB)),0)),0),MIN(created_ms) FROM delivery_batches WHERE job_id=? AND state IN ('pending','leased','retry_wait','blocked')", [job_id], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)))?;
    let unqueued_changes: i64 = conn.query_row(
        "SELECT COUNT(*) FROM delivery_journal WHERE seq>?",
        [job.cursor],
        |row| row.get(0),
    )?;
    conn.query_row("SELECT acknowledged_cursor,next_attempt_ms,last_attempt_ms,last_acknowledged_ms,acceptance_level,failure,suppressed_records,acknowledged_records FROM delivery_jobs WHERE id=?", [job_id], |row| Ok(DeliveryStatus {
        job_id:job.id.clone(), config:job.config.clone(), generation:job.generation, state:job.state.clone(), bootstrap_complete:job.bootstrap_done,
        journal_cursor:job.cursor, acknowledged_cursor:row.get(0)?, pending_records,pending_bytes,oldest_pending_ms,unqueued_changes,
        next_attempt_ms:row.get(1)?,last_attempt_ms:row.get(2)?,last_acknowledged_ms:row.get(3)?,acceptance_level:row.get(4)?,failure:row.get(5)?,suppressed_records:row.get(6)?,acknowledged_records:row.get(7)?,
    })).map_err(Into::into)
}

pub fn list_jobs(conn: &Connection) -> Result<Vec<DeliveryStatus>> {
    let ids = conn
        .prepare("SELECT id FROM delivery_jobs ORDER BY created_ms,id")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    ids.iter().map(|id| status(conn, id)).collect()
}

fn excluded(
    conn: &Connection,
    selection: &ExportSelection,
    source: &str,
    session: Option<&str>,
) -> Result<bool> {
    let Some(session_id) = session else {
        return Ok(false);
    };
    if selection
        .excluded_sessions
        .iter()
        .any(|id| id.source == source && id.session_id == session_id)
    {
        return Ok(true);
    }
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM delivery_exclusions WHERE source=? AND session_id=?)",
        params![source, session_id],
        |row| row.get(0),
    )?)
}
fn selected(selection: &ExportSelection, kind: &str, source: &str, session: Option<&str>) -> bool {
    selection.kinds.iter().any(|value| value == kind)
        && (selection.all_sources
            || selection.sources.iter().any(|value| value == source)
            || selection
                .sessions
                .iter()
                .any(|id| id.source == source && Some(id.session_id.as_str()) == session))
}

/// Exclusions are persistent and source-scoped. They are rechecked when a batch
/// is claimed/prepared. This cannot revoke a request already sent to a server.
pub fn set_session_excluded(
    conn: &Connection,
    session: &SessionIdentity,
    value: bool,
) -> Result<()> {
    let tx = write_transaction(conn)?;
    if value {
        tx.execute(
            "INSERT OR IGNORE INTO delivery_exclusions(source,session_id) VALUES (?,?)",
            params![session.source, session.session_id],
        )?;
    } else {
        tx.execute(
            "DELETE FROM delivery_exclusions WHERE source=? AND session_id=?",
            params![session.source, session.session_id],
        )?;
    }
    // Fence all active claims. A worker must re-claim and recheck before dispatch.
    tx.execute("UPDATE delivery_jobs SET fence=fence+1,worker_id=NULL,lease_until_ms=NULL WHERE state <> 'cancelled'", [])?;
    tx.execute(
        "UPDATE delivery_batches SET state='pending' WHERE state='leased'",
        [],
    )?;
    tx.commit()?;
    Ok(())
}

#[derive(Debug)]
struct RawRecord {
    position: i64,
    kind: String,
    source: String,
    session: Option<String>,
    key: String,
    operation: String,
    payload: String,
}
fn make_record(origin: &str, revision: i64, raw: &RawRecord) -> Result<HistoryExportRecord> {
    let record_id = hash(&raw.key);
    Ok(HistoryExportRecord {
        schema_version: EXPORT_SCHEMA_VERSION,
        origin_id: origin.into(),
        revision_id: hash(format!("{origin}:{record_id}:{revision}")),
        record_id,
        revision,
        kind: raw.kind.clone(),
        source: raw.source.clone(),
        session_id: raw.session.clone(),
        operation: raw.operation.clone(),
        payload: serde_json::from_str(&raw.payload)?,
    })
}
fn batch_template(conn: &Connection, job: &Job) -> Result<HistoryExportBatch> {
    let (origin_id, batch_id) = conn.query_row(
        "SELECT origin_id,lower(hex(randomblob(16))) FROM delivery_state WHERE singleton=1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    Ok(HistoryExportBatch {
        schema_version: EXPORT_SCHEMA_VERSION,
        origin_id,
        batch_id,
        job_id: job.id.clone(),
        generation: job.generation,
        destination_id: job.config.destination_id.clone(),
        instance_id: job.config.instance_id.clone(),
        account_id: job.config.account_id.clone(),
        mapping_version: job.config.mapping_version.clone(),
        records: Vec::new(),
    })
}
fn insert_batch(
    conn: &Connection,
    batch: &HistoryExportBatch,
    journal_end: i64,
    now: i64,
) -> Result<()> {
    let payload = serde_json::to_string(batch)?;
    conn.execute("INSERT INTO delivery_batches(id,job_id,state,payload,records,bytes,journal_end,created_ms) VALUES (?,?,'pending',?,?,?,?,?)",params![batch.batch_id,batch.job_id,payload,batch.records.len() as i64,payload.len() as i64,journal_end,now])?;
    Ok(())
}

/// Prepare at most one bounded batch; no network operation occurs. Calls that
/// scan only excluded/empty rows can return no batch while making progress.
/// The host repeats this call until caught up, respecting its own run budget.
pub fn prepare_batch(conn: &Connection, job_id: &str, now_ms: i64) -> Result<PrepareResult> {
    let tx = write_transaction(conn)?;
    let mut job = job(&tx, job_id)?;
    ensure!(job.state == "active", "delivery job is not active");
    let existing: Option<String> = tx.query_row("SELECT id FROM delivery_batches WHERE job_id=? AND state IN ('pending','leased','retry_wait','blocked')",[job_id],|row| row.get(0)).optional()?;
    if existing.is_some() {
        return Ok(PrepareResult {
            batch_id: existing,
            scanned_records: 0,
            bootstrap_complete: job.bootstrap_done,
        });
    }
    let mut batch = batch_template(&tx, &job)?;
    let mut scanned = 0;
    let mut suppressed = 0;
    while scanned < job.config.limits.max_scan_records
        && batch.records.len() < job.config.limits.max_batch_records
    {
        let raw = if !job.bootstrap_done {
            if job.bootstrap_kind >= schema::TABLES.len() {
                job.bootstrap_done = true;
                continue;
            }
            let Some(raw) = snapshot_record(&tx, job_id, job.bootstrap_kind, job.bootstrap_rowid)?
            else {
                job.bootstrap_kind += 1;
                job.bootstrap_rowid = 0;
                continue;
            };
            raw
        } else {
            let value = tx.query_row("SELECT seq,kind,source,session_id,record_key,operation,payload FROM delivery_journal WHERE seq>? ORDER BY seq LIMIT 1",[job.cursor],|row| Ok(RawRecord {position:row.get(0)?,kind:row.get(1)?,source:row.get(2)?,session:row.get(3)?,key:row.get(4)?,operation:row.get(5)?,payload:row.get(6)?})).optional()?;
            let Some(raw) = value else { break };
            raw
        };
        let eligible = raw.operation != "absent"
            && selected(
                &job.config.selection,
                &raw.kind,
                &raw.source,
                raw.session.as_deref(),
            );
        let private = eligible && raw_excluded(&tx, &job.config.selection, &raw)?;
        if eligible && !private {
            let revision = if job.bootstrap_done {
                raw.position
            } else {
                job.cutoff
            };
            batch
                .records
                .push(make_record(&batch.origin_id, revision, &raw)?);
            if serde_json::to_vec(&batch)?.len() > job.config.limits.max_batch_bytes {
                batch.records.pop();
                ensure!(
                    !batch.records.is_empty(),
                    "delivery record exceeds configured batch byte limit"
                );
                break;
            }
        }
        if private {
            suppressed += 1;
        }
        scanned += 1;
        if job.bootstrap_done {
            job.cursor = raw.position;
        } else {
            job.bootstrap_rowid = raw.position;
            tx.execute(
                "DELETE FROM delivery_shadow WHERE job_id=? AND kind=? AND row_id<=?",
                params![job_id, raw.kind, raw.position],
            )?;
        }
    }
    tx.execute("UPDATE delivery_jobs SET bootstrap_kind=?,bootstrap_rowid=?,bootstrap_done=?,journal_cursor=?,suppressed_records=suppressed_records+? WHERE id=?",params![job.bootstrap_kind as i64,job.bootstrap_rowid,job.bootstrap_done,job.cursor,suppressed,job_id])?;
    let batch_id = if batch.records.is_empty() {
        None
    } else {
        insert_batch(&tx, &batch, job.cursor, now_ms)?;
        Some(batch.batch_id)
    };
    tx.commit()?;
    Ok(PrepareResult {
        batch_id,
        scanned_records: scanned,
        bootstrap_complete: job.bootstrap_done,
    })
}

fn check_lease(conn: &Connection, lease: &DeliveryLease, now: i64) -> Result<()> {
    let valid: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM delivery_jobs j JOIN delivery_batches b ON b.job_id=j.id WHERE j.id=? AND j.state='active' AND j.worker_id=? AND j.fence=? AND j.lease_until_ms>? AND b.id=? AND b.state='leased')",params![lease.job_id,lease.worker_id,lease.fence,now,lease.batch_id],|row| row.get(0))?;
    ensure!(valid, "delivery lease expired or was fenced");
    Ok(())
}
fn pending_batch(
    conn: &Connection,
    id: &str,
) -> Result<Option<(HistoryExportBatch, Option<PreparedPayload>, i64)>> {
    let value = conn.query_row("SELECT payload,prepared,journal_end FROM delivery_batches WHERE job_id=? AND state IN ('pending','leased','retry_wait','blocked')",[id],|row| Ok((row.get::<_,String>(0)?,row.get::<_,Option<String>>(1)?,row.get(2)?))).optional()?;
    value
        .map(|(payload, prepared, end)| {
            Ok((
                serde_json::from_str(&payload)?,
                prepared
                    .map(|value| serde_json::from_str(&value))
                    .transpose()?,
                end,
            ))
        })
        .transpose()
}

/// Claims an already persisted batch. Lease expiry permits redelivery with the
/// same batch ID and body. No database transaction remains open during I/O.
pub fn claim_batch(
    conn: &Connection,
    job_id: &str,
    worker_id: &str,
    lease_ms: i64,
    now_ms: i64,
) -> Result<Option<ClaimedBatch>> {
    ensure!(identifier(worker_id), "invalid delivery worker id");
    ensure!(
        (1..=86_400_000).contains(&lease_ms) && now_ms >= 0,
        "invalid delivery lease duration/clock"
    );
    let expires_at_ms = now_ms
        .checked_add(lease_ms)
        .context("delivery clock overflow")?;
    let tx = write_transaction(conn)?;
    let job = job(&tx, job_id)?;
    if job.state != "active" {
        return Ok(None);
    }
    let unavailable: bool = tx.query_row(
        "SELECT next_attempt_ms>? OR COALESCE(lease_until_ms>?,0) FROM delivery_jobs WHERE id=?",
        params![now_ms, now_ms, job_id],
        |row| row.get(0),
    )?;
    if unavailable {
        return Ok(None);
    }
    let Some((mut batch, mut prepared, end)) = pending_batch(&tx, job_id)? else {
        return Ok(None);
    };
    let mut allowed = Vec::new();
    for record in &batch.records {
        if !record_excluded(&tx, &job.config.selection, record)? {
            allowed.push(record.clone());
        }
    }
    let removed = batch.records.len() - allowed.len();
    if removed > 0 {
        // Never change a payload beneath a previously used idempotency key.
        tx.execute(
            "UPDATE delivery_batches SET state='suppressed',payload=NULL,prepared=NULL WHERE id=?",
            [&batch.batch_id],
        )?;
        tx.execute("UPDATE delivery_jobs SET suppressed_records=suppressed_records+?,fence=fence+1,worker_id=NULL,lease_until_ms=NULL WHERE id=?",params![removed as i64,job_id])?;
        if allowed.is_empty() {
            tx.commit()?;
            return Ok(None);
        }
        batch.batch_id = tx.query_row("SELECT lower(hex(randomblob(16)))", [], |row| row.get(0))?;
        batch.records = allowed;
        prepared = None;
        insert_batch(&tx, &batch, end, now_ms)?;
    }
    tx.execute("UPDATE delivery_jobs SET fence=fence+1,worker_id=?,lease_until_ms=?,last_attempt_ms=?,attempts=attempts+1 WHERE id=?",params![worker_id,expires_at_ms,now_ms,job_id])?;
    let fence = tx.query_row(
        "SELECT fence FROM delivery_jobs WHERE id=?",
        [job_id],
        |row| row.get(0),
    )?;
    tx.execute(
        "UPDATE delivery_batches SET state='leased' WHERE id=?",
        [&batch.batch_id],
    )?;
    tx.commit()?;
    Ok(Some(ClaimedBatch {
        lease: DeliveryLease {
            job_id: job_id.into(),
            batch_id: batch.batch_id.clone(),
            worker_id: worker_id.into(),
            fence,
            expires_at_ms,
        },
        batch,
        prepared,
    }))
}

pub fn renew_lease(
    conn: &Connection,
    lease: &DeliveryLease,
    lease_ms: i64,
    now_ms: i64,
) -> Result<DeliveryLease> {
    ensure!(
        (1..=86_400_000).contains(&lease_ms),
        "invalid lease duration"
    );
    let expires_at_ms = now_ms
        .checked_add(lease_ms)
        .context("delivery clock overflow")?;
    let tx = write_transaction(conn)?;
    check_lease(&tx, lease, now_ms)?;
    tx.execute(
        "UPDATE delivery_jobs SET lease_until_ms=? WHERE id=?",
        params![expires_at_ms, lease.job_id],
    )?;
    tx.commit()?;
    Ok(DeliveryLease {
        expires_at_ms,
        ..lease.clone()
    })
}

/// Persist exact destination wire bytes before sending. An adapter upgrade must
/// handle this stored mapping_version/body, or block the job. Calling again with
/// different bytes is rejected; rotating auth headers are deliberately separate.
pub fn store_prepared_payload(
    conn: &Connection,
    lease: &DeliveryLease,
    mapping_version: &str,
    content_type: &str,
    body: &str,
    now_ms: i64,
) -> Result<PreparedPayload> {
    ensure!(
        !content_type.is_empty()
            && content_type.len() <= 200
            && !content_type.contains(['\r', '\n']),
        "invalid content type"
    );
    let tx = write_transaction(conn)?;
    check_lease(&tx, lease, now_ms)?;
    let job = job(&tx, &lease.job_id)?;
    ensure!(
        mapping_version == job.config.mapping_version,
        "destination mapping version mismatch"
    );
    ensure!(
        body.len() <= job.config.limits.max_prepared_bytes,
        "prepared delivery payload too large"
    );
    let (batch, stored, _) =
        pending_batch(&tx, &lease.job_id)?.context("delivery batch missing")?;
    for record in &batch.records {
        ensure!(
            !record_excluded(&tx, &job.config.selection, record)?,
            "delivery batch is now excluded"
        );
    }
    let payload = PreparedPayload {
        mapping_version: mapping_version.into(),
        content_type: content_type.into(),
        body: body.into(),
        sha256: hash(body),
    };
    if let Some(stored) = stored {
        ensure!(
            stored == payload,
            "cannot replace persisted destination payload"
        );
        return Ok(stored);
    }
    tx.execute(
        "UPDATE delivery_batches SET prepared=? WHERE id=?",
        params![serde_json::to_string(&payload)?, lease.batch_id],
    )?;
    tx.commit()?;
    Ok(payload)
}

/// Validate an acknowledgment against every exact revision in this batch.
/// Partial acceptance keeps the entire immutable batch retryable; the receiver
/// must deduplicate accepted revisions. Unsupported records block the job.
/// Async receipt-only acceptance must be polled by the adapter until durable;
/// it is intentionally not representable as a successful acknowledgment here.
pub fn acknowledge(
    conn: &Connection,
    lease: &DeliveryLease,
    ack: &DeliveryAcknowledgment,
    now_ms: i64,
) -> Result<DeliveryStatus> {
    let tx = write_transaction(conn)?;
    check_lease(&tx, lease, now_ms)?;
    ensure!(
        ack.batch_id == lease.batch_id,
        "acknowledgment batch mismatch"
    );
    let (batch, prepared, end) =
        pending_batch(&tx, &lease.job_id)?.context("delivery batch missing")?;
    ensure!(
        prepared.is_some(),
        "destination payload must be persisted before acknowledgment"
    );
    let expected: HashSet<&str> = batch
        .records
        .iter()
        .map(|r| r.revision_id.as_str())
        .collect();
    let accepted: HashSet<&str> = ack
        .accepted_revision_ids
        .iter()
        .map(String::as_str)
        .collect();
    let unsupported: HashSet<&str> = ack
        .unsupported_revision_ids
        .iter()
        .map(String::as_str)
        .collect();
    ensure!(
        accepted.len() == ack.accepted_revision_ids.len()
            && unsupported.len() == ack.unsupported_revision_ids.len(),
        "duplicate acknowledgment revision"
    );
    ensure!(
        accepted.is_subset(&expected)
            && unsupported.is_subset(&expected)
            && accepted.is_disjoint(&unsupported),
        "invalid acknowledgment revision"
    );
    if !unsupported.is_empty() {
        apply_failure(
            &tx,
            lease,
            DeliveryFailure::UnsupportedEvidence,
            None,
            now_ms,
        )?;
    } else if accepted != expected {
        apply_failure(&tx, lease, DeliveryFailure::Transient, None, now_ms)?;
    } else {
        tx.execute("UPDATE delivery_batches SET state='acknowledged',accepted_records=records,payload=NULL,prepared=NULL WHERE id=?",[&lease.batch_id])?;
        tx.execute("UPDATE delivery_jobs SET acknowledged_cursor=MAX(acknowledged_cursor,?),last_acknowledged_ms=?,acceptance_level=?,acknowledged_records=acknowledged_records+?,worker_id=NULL,lease_until_ms=NULL,fence=fence+1,attempts=0,next_attempt_ms=0,failure=NULL WHERE id=?",params![end,now_ms,match ack.acceptance_level {AcceptanceLevel::Durable=>"durable",AcceptanceLevel::Indexed=>"indexed"},batch.records.len() as i64,lease.job_id])?;
    }
    if accepted != expected {
        tx.execute(
            "UPDATE delivery_batches SET accepted_records=MAX(accepted_records,?) WHERE id=?",
            params![accepted.len() as i64, lease.batch_id],
        )?;
    }
    tx.commit()?;
    status(conn, &lease.job_id)
}

fn apply_failure(
    conn: &Connection,
    lease: &DeliveryLease,
    failure: DeliveryFailure,
    retry_after_ms: Option<i64>,
    now_ms: i64,
) -> Result<()> {
    let attempts: i64 = conn.query_row(
        "SELECT attempts FROM delivery_jobs WHERE id=?",
        [&lease.job_id],
        |row| row.get(0),
    )?;
    let backoff = 1_000_i64
        .saturating_mul(1_i64 << attempts.clamp(0, 12))
        .min(3_600_000);
    let digest = Sha256::digest(format!("{}:{attempts}", lease.batch_id));
    let jitter = (u16::from_be_bytes([digest[0], digest[1]]) as i64) * backoff / 327_675;
    let next = now_ms
        .saturating_add(backoff + jitter)
        .max(retry_after_ms.unwrap_or(0));
    let retryable = failure.retryable();
    conn.execute("UPDATE delivery_jobs SET state=?,failure=?,next_attempt_ms=?,fence=fence+1,worker_id=NULL,lease_until_ms=NULL WHERE id=?",params![if retryable{"active"}else{"blocked"},failure.code(),next,lease.job_id])?;
    conn.execute(
        "UPDATE delivery_batches SET state=? WHERE id=?",
        params![
            if retryable { "retry_wait" } else { "blocked" },
            lease.batch_id
        ],
    )?;
    Ok(())
}

pub fn record_failure(
    conn: &Connection,
    lease: &DeliveryLease,
    failure: DeliveryFailure,
    retry_after_ms: Option<i64>,
    now_ms: i64,
) -> Result<DeliveryStatus> {
    let tx = write_transaction(conn)?;
    check_lease(&tx, lease, now_ms)?;
    apply_failure(&tx, lease, failure, retry_after_ms, now_ms)?;
    tx.commit()?;
    status(conn, &lease.job_id)
}
fn change_state(conn: &Connection, job_id: &str, state: &str) -> Result<DeliveryStatus> {
    let tx = write_transaction(conn)?;
    let existing = job(&tx, job_id)?;
    ensure!(existing.state != "cancelled", "delivery job is cancelled");
    tx.execute("UPDATE delivery_jobs SET state=?,fence=fence+1,worker_id=NULL,lease_until_ms=NULL WHERE id=?",params![state,job_id])?;
    tx.execute(
        "UPDATE delivery_batches SET state='pending' WHERE job_id=? AND state='leased'",
        [job_id],
    )?;
    tx.commit()?;
    status(conn, job_id)
}
pub fn pause_job(conn: &Connection, job_id: &str) -> Result<DeliveryStatus> {
    change_state(conn, job_id, "paused")
}
pub fn resume_job(conn: &Connection, job_id: &str) -> Result<DeliveryStatus> {
    ensure!(
        job(conn, job_id)?.state == "paused",
        "only a paused job can resume; retry blocked jobs explicitly"
    );
    change_state(conn, job_id, "active")
}
pub fn retry_job(conn: &Connection, job_id: &str) -> Result<DeliveryStatus> {
    let tx = write_transaction(conn)?;
    ensure!(
        job(&tx, job_id)?.state != "cancelled",
        "delivery job is cancelled"
    );
    tx.execute("UPDATE delivery_jobs SET state='active',failure=NULL,next_attempt_ms=0,fence=fence+1,worker_id=NULL,lease_until_ms=NULL WHERE id=?",[job_id])?;
    tx.execute("UPDATE delivery_batches SET state='pending' WHERE job_id=? AND state IN ('leased','blocked','retry_wait')",[job_id])?;
    tx.commit()?;
    status(conn, job_id)
}
/// Explicit discard, separate from pause. Already accepted remote data is not
/// deleted. A later create_job starts a new generation and historical backfill.
pub fn cancel_job(conn: &Connection, job_id: &str) -> Result<DeliveryStatus> {
    let tx = write_transaction(conn)?;
    job(&tx, job_id)?;
    tx.execute("UPDATE delivery_jobs SET state='cancelled',fence=fence+1,worker_id=NULL,lease_until_ms=NULL WHERE id=?",[job_id])?;
    tx.execute("UPDATE delivery_batches SET state='cancelled',payload=NULL,prepared=NULL WHERE job_id=? AND state IN ('pending','leased','retry_wait','blocked')",[job_id])?;
    tx.execute("DELETE FROM delivery_shadow WHERE job_id=?", [job_id])?;
    tx.execute(
        "DELETE FROM delivery_bootstrap_bounds WHERE job_id=?",
        [job_id],
    )?;
    tx.commit()?;
    status(conn, job_id)
}

/// Explicit retained-byte cap. Exceeding it aborts capture rather than silently
/// dropping revisions or advancing ingestion. Raising it can unblock ingestion.
pub fn set_retention_limit(conn: &Connection, max_bytes: i64) -> Result<()> {
    let tx = write_transaction(conn)?;
    let used: i64 = tx.query_row(
        "SELECT retained_bytes FROM delivery_state WHERE singleton=1",
        [],
        |row| row.get(0),
    )?;
    ensure!(
        max_bytes >= used && max_bytes > 0,
        "retention cap is below current usage"
    );
    tx.execute(
        "UPDATE delivery_state SET max_retained_bytes=? WHERE singleton=1",
        [max_bytes],
    )?;
    tx.commit()?;
    Ok(())
}
pub fn retained_bytes(conn: &Connection) -> Result<(i64, i64)> {
    Ok(conn.query_row(
        "SELECT retained_bytes,max_retained_bytes FROM delivery_state WHERE singleton=1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?)
}
/// Remove a bounded number of journal rows already copied into every active
/// job's immutable queue. Bootstrap preimages live separately until scanned.
/// Acknowledged batch bodies are released at acknowledgment; small receipts
/// and job generations remain for audit and safe generation numbering.
pub fn compact_journal(conn: &Connection, limit: usize) -> Result<usize> {
    ensure!((1..=10_000).contains(&limit), "invalid compaction limit");
    let tx = write_transaction(conn)?;
    let floor:i64=tx.query_row("SELECT COALESCE((SELECT MIN(journal_cursor) FROM delivery_jobs WHERE state <> 'cancelled'),(SELECT COALESCE(MAX(seq),0) FROM delivery_journal))",[],|row|row.get(0))?;
    let removed=tx.execute("DELETE FROM delivery_journal WHERE seq IN (SELECT seq FROM delivery_journal WHERE seq<=? ORDER BY seq LIMIT ?)",params![floor,limit as i64])?;
    tx.commit()?;
    Ok(removed)
}

fn snapshot_record(
    conn: &Connection,
    id: &str,
    kind_index: usize,
    after: i64,
) -> Result<Option<RawRecord>> {
    let table = &schema::TABLES[kind_index];
    let maximum: i64 = conn.query_row(
        "SELECT max_rowid FROM delivery_bootstrap_bounds WHERE job_id=? AND kind=?",
        params![id, table.kind],
        |row| row.get(0),
    )?;
    let sql = format!(
        r#"
WITH ids AS (
 SELECT rowid AS row_id FROM {table} WHERE rowid>?1 AND rowid<=?2
 UNION SELECT row_id FROM delivery_shadow WHERE job_id=?3 AND kind=?4 AND row_id>?1 AND row_id<=?2
), candidate AS (SELECT row_id FROM ids ORDER BY row_id LIMIT 1)
SELECT c.row_id, CASE WHEN s.row_id IS NOT NULL THEN s.source ELSE {source} END,
 CASE WHEN s.row_id IS NOT NULL THEN s.session_id ELSE r.{session} END,
 CASE WHEN s.row_id IS NOT NULL THEN s.record_key ELSE {key} END,
 CASE WHEN s.row_id IS NOT NULL THEN s.payload ELSE {payload} END
FROM candidate c LEFT JOIN {table} r ON r.rowid=c.row_id
LEFT JOIN delivery_shadow s ON s.job_id=?3 AND s.kind=?4 AND s.row_id=c.row_id
"#,
        table = table.name,
        source = table.source("r"),
        session = table.session,
        key = table.key("r"),
        payload = table.payload(conn, "r")?
    );
    Ok(conn
        .query_row(&sql, params![after, maximum, id, table.kind], |row| {
            let payload: Option<String> = row.get(4)?;
            Ok(RawRecord {
                position: row.get(0)?,
                kind: table.kind.into(),
                source: row.get(1)?,
                session: row.get(2)?,
                key: row.get(3)?,
                operation: if payload.is_none() {
                    "absent"
                } else {
                    "upsert"
                }
                .into(),
                payload: payload.unwrap_or_else(|| "null".into()),
            })
        })
        .optional()?)
}

/// Recheck immediately before dispatch, after async mapping or lease renewal.
/// Returns only the exact persisted body. Local fencing cannot retract a socket
/// request already sent; destinations still need idempotency/revision ordering.
pub fn validate_dispatch(
    conn: &Connection,
    lease: &DeliveryLease,
    now_ms: i64,
) -> Result<PreparedPayload> {
    let tx = write_transaction(conn)?;
    check_lease(&tx, lease, now_ms)?;
    let job = job(&tx, &lease.job_id)?;
    let (batch, prepared, _) =
        pending_batch(&tx, &lease.job_id)?.context("delivery batch missing")?;
    for record in &batch.records {
        ensure!(
            !record_excluded(&tx, &job.config.selection, record)?,
            "delivery batch is now excluded"
        );
    }
    prepared.context("destination payload must be persisted before dispatch")
}

fn raw_excluded(conn: &Connection, selection: &ExportSelection, raw: &RawRecord) -> Result<bool> {
    if excluded(conn, selection, &raw.source, raw.session.as_deref())? {
        return Ok(true);
    }
    if raw.kind == "relationship" && raw.operation != "delete" {
        let value: serde_json::Value = serde_json::from_str(&raw.payload)?;
        if let Some(child) = value.get("child_session_id").and_then(|v| v.as_str()) {
            return excluded(conn, selection, &raw.source, Some(child));
        }
    }
    Ok(false)
}
fn record_excluded(
    conn: &Connection,
    selection: &ExportSelection,
    record: &HistoryExportRecord,
) -> Result<bool> {
    if excluded(
        conn,
        selection,
        &record.source,
        record.session_id.as_deref(),
    )? {
        return Ok(true);
    }
    if record.kind == "relationship" {
        if let Some(child) = record
            .payload
            .get("child_session_id")
            .and_then(|v| v.as_str())
        {
            return excluded(conn, selection, &record.source, Some(child));
        }
    }
    Ok(false)
}

/// Bound retained audit receipts separately from live queue/journal compaction.
/// Aggregate job counters and generation identities remain intact.
pub fn compact_receipts(conn: &Connection, limit: usize) -> Result<usize> {
    ensure!(
        (1..=10_000).contains(&limit),
        "invalid receipt compaction limit"
    );
    let tx = write_transaction(conn)?;
    let removed=tx.execute("DELETE FROM delivery_batches WHERE seq IN (SELECT seq FROM delivery_batches WHERE state IN ('acknowledged','suppressed','cancelled') ORDER BY seq LIMIT ?)",[limit as i64])?;
    tx.commit()?;
    Ok(removed)
}

/// Recognize the coordinator's trigger-originated capacity error without
/// forwarding arbitrary SQLite/provider error text to a host or plugin.
/// Hosts should expose a stable DELIVERY_RETENTION_LIMIT code and offer
/// compact_journal/compact_receipts or an explicit set_retention_limit action.
pub fn is_retention_limit(error: &anyhow::Error) -> bool {
    error.chain().any(|cause|matches!(cause.downcast_ref::<rusqlite::Error>(),Some(rusqlite::Error::SqliteFailure(_,Some(message))) if message.starts_with("delivery retention limit exceeded;")))
}
