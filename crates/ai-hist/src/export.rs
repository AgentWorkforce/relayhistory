//! Storage-owned evidence export types. Upload lifecycle belongs to the probe.
use serde::{Deserialize, Serialize};
pub const EXPORT_SCHEMA_VERSION: u32 = 1;
pub const DEFAULT_RETENTION_LIMIT_BYTES: i64 = 256 * 1_048_576;
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
    "source_observation",
    "observation_evidence",
    "session_marker",
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
pub struct ExportLimits {
    pub max_batch_records: usize,
    pub max_batch_bytes: usize,
    /// Bounds both bootstrap and journal scans, including excluded rows.
    pub max_scan_records: usize,
}
impl Default for ExportLimits {
    fn default() -> Self {
        Self {
            max_batch_records: 100,
            max_batch_bytes: 1_048_576,
            max_scan_records: 400,
        }
    }
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

mod schema;
pub(crate) use schema::{init_schema, journal_migrated_rows, schema_is_current, shadow_preimages};
pub mod capture;
mod legacy;
mod snapshot;
use anyhow::{ensure, Context, Result};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use sha2::{Digest, Sha256};
pub use snapshot::{
    close_export, create_export, expire_exports, export_page, ExportHandle, HistoryExportPage,
};
fn write_transaction(conn: &Connection) -> Result<Transaction<'_>> {
    ensure!(
        conn.is_autocommit(),
        "export operations require their own short transaction"
    );
    Ok(Transaction::new_unchecked(
        conn,
        TransactionBehavior::Immediate,
    )?)
}
fn hash(value: impl AsRef<[u8]>) -> String {
    format!("{:x}", Sha256::digest(value.as_ref()))
}
fn validate_export(selection: &ExportSelection, limits: &ExportLimits) -> Result<()> {
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
        serde_json::to_vec(selection)?.len() <= 65_536,
        "export selection too large"
    );
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
    Ok(())
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
    Ok(!capture::is_shareable(conn, source, session_id)?)
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

#[derive(Debug)]
pub struct RawRecord {
    pub position: i64,
    pub kind: String,
    pub source: String,
    pub session: Option<String>,
    pub key: String,
    pub operation: String,
    pub payload: String,
}
pub fn make_record(origin: &str, revision: i64, raw: &RawRecord) -> Result<HistoryExportRecord> {
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
/// Examine a bounded number of changes and remove those already consumed by every interested
/// subscription. An idle session subscription must not retain other sessions'
/// changes. A persistent scan cursor lets cleanup pass retained rows without
/// scanning the whole journal in one upload cycle. Bootstrap preimages are retained independently of this journal.
pub fn compact_journal(conn: &Connection, limit: usize) -> Result<usize> {
    ensure!((1..=10_000).contains(&limit), "invalid compaction limit");
    let tx = write_transaction(conn)?;
    let mut after: i64 = tx.query_row(
        "SELECT cursor FROM history_compaction WHERE singleton=1",
        [],
        |r| r.get(0),
    )?;
    let boundary = |cursor: i64| -> Result<Option<i64>> {
        Ok(tx.query_row("SELECT MAX(seq) FROM (SELECT seq FROM delivery_journal WHERE seq>? ORDER BY seq LIMIT ?)", params![cursor, limit as i64], |r| r.get(0))?)
    };
    let mut end = boundary(after)?;
    if end.is_none() && after != 0 {
        after = 0;
        end = boundary(after)?;
    }
    let removed = if let Some(end) = end {
        compact_journal_range(&tx, after, end)?
    } else {
        0
    };
    tx.commit()?;
    Ok(removed)
}

/// Reclaim consumed changes across one complete journal pass for retention recovery.
/// Each transaction scans at most `page_size` rows, including retained rows.
/// Freeze the upper sequence bound so concurrent appends cannot extend this pass.
/// Unlike the background compactor, this starts at zero regardless of its saved cursor.
pub fn compact_journal_pass(conn: &Connection, page_size: usize) -> Result<usize> {
    ensure!(
        (1..=10_000).contains(&page_size),
        "invalid compaction limit"
    );
    let through: i64 = conn.query_row(
        "SELECT COALESCE(MAX(seq),0) FROM delivery_journal",
        [],
        |r| r.get(0),
    )?;
    let mut after = 0;
    let mut removed = 0;
    while after < through {
        let tx = write_transaction(conn)?;
        let end: Option<i64> = tx.query_row(
            "SELECT MAX(seq) FROM (SELECT seq FROM delivery_journal WHERE seq>? AND seq<=? ORDER BY seq LIMIT ?)",
            params![after, through, page_size as i64],
            |r| r.get(0),
        )?;
        let Some(end) = end else { break };
        removed += compact_journal_range(&tx, after, end)?;
        tx.commit()?;
        after = end;
    }
    Ok(removed)
}

/// Delete only revisions consumed by every interested subscription and advance the scan cursor.
fn compact_journal_range(tx: &Transaction<'_>, after: i64, end: i64) -> Result<usize> {
    let removed = tx.execute(
        "DELETE FROM delivery_journal AS j WHERE seq>? AND seq<=? AND NOT EXISTS (
            SELECT 1 FROM history_subscriptions s
            WHERE s.journal_cursor < j.seq
              AND (s.source IS NULL OR (s.source=j.source AND s.session_id=j.session_id))
        )",
        params![after, end],
    )?;
    tx.execute(
        "UPDATE history_compaction SET cursor=? WHERE singleton=1",
        [end],
    )?;
    Ok(removed)
}

pub fn snapshot_record(
    conn: &Connection,
    id: &str,
    kind_index: usize,
    after: i64,
) -> Result<Option<RawRecord>> {
    let table = schema::TABLES
        .get(kind_index)
        .context("unknown evidence kind index")?;
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

pub fn is_retention_limit(error: &anyhow::Error) -> bool {
    error.chain().any(|cause|matches!(cause.downcast_ref::<rusqlite::Error>(),Some(rusqlite::Error::SqliteFailure(_,Some(message))) if message.starts_with("delivery retention limit exceeded;")))
}
