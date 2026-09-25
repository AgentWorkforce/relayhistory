//! Local evidence export: bounded, resumable snapshots a user writes to a
//! file or a pipe, one NDJSON record at a time.
//!
//! A snapshot covers the rows every selected table holds when it is created:
//! [`create_export`] records each table's largest rowid, and [`export_page`]
//! reads the live rows at or below it in rowid order, a bounded page per
//! call. A row written after creation is outside the snapshot; a row
//! rewritten before its page is read is exported as it then stands, at its
//! new revision; a row deleted before its page is read is not exported.
//!
//! A record carries the same identity and revision the change feed reports
//! for the row ([`crate::Change::key`], [`crate::Change::revision`]), so an
//! embedder can merge an export with the feed.
use anyhow::{ensure, Context, Result};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::change_feed::{self, ChangeKind};

/// The record format version. `2`: `origin_id` is the store's change-feed
/// epoch and `revision` is the row's change-feed revision.
pub const EXPORT_SCHEMA_VERSION: u32 = 2;

/// The kinds a selection may name, in the order a snapshot reads them.
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

/// Most snapshots a store keeps open at once.
const MAX_OPEN_EXPORTS: i64 = 32;

/// One session in a selection: the stored source name and session id.
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
    /// Rows one page examines, including rows the selection leaves out.
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
    /// The store's identity: its change-feed epoch, as 16 hex digits.
    pub origin_id: String,
    /// SHA-256 of the record's key, [`crate::Change::key`] serialized as
    /// compact JSON.
    pub record_id: String,
    pub revision_id: String,
    /// The row's change-feed revision.
    pub revision: i64,
    pub kind: String,
    pub source: String,
    pub session_id: Option<String>,
    pub operation: String,
    /// The row as stored: every column but `revision`, each value as SQLite
    /// holds it ([`crate::StoredRow`]).
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExportHandle {
    pub snapshot_id: String,
    pub cursor: String,
    pub expires_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HistoryExportPage {
    pub schema_version: u32,
    pub origin_id: String,
    pub records: Vec<HistoryExportRecord>,
    pub next_cursor: Option<String>,
}

/// Tables [`init_schema`] creates, for the store's schema check.
pub(crate) const REQUIRED_TABLES: &[&str] = &[
    "history_exports",
    "history_export_pages",
    "history_export_bounds",
];

/// Create the snapshot tables inside the caller's schema transaction.
///
/// `history_exports` and `history_export_pages` keep the shape an earlier
/// release created them with. A snapshot that release opened has no
/// `history_export_bounds` rows, so [`export_page`] refuses it and
/// [`expire_exports`] or [`close_export`] releases it.
pub(crate) fn init_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
CREATE TABLE IF NOT EXISTS history_exports (
 id TEXT PRIMARY KEY, selection_json TEXT NOT NULL, limits_json TEXT NOT NULL,
 cutoff INTEGER NOT NULL, bootstrap_kind INTEGER NOT NULL DEFAULT 0,
 bootstrap_rowid INTEGER NOT NULL DEFAULT 0, bootstrap_done INTEGER NOT NULL DEFAULT 0,
 expires_at_ms INTEGER NOT NULL, cursor TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS history_export_pages (
 cursor TEXT PRIMARY KEY, export_id TEXT NOT NULL, payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS history_export_page_owner ON history_export_pages(export_id);
CREATE TABLE IF NOT EXISTS history_export_bounds (
 export_id TEXT NOT NULL, kind TEXT NOT NULL, max_rowid INTEGER NOT NULL,
 PRIMARY KEY (export_id, kind)
);
"#,
    )?;
    Ok(())
}

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

/// The change-feed kind behind a supported kind name.
fn change_kind(name: &str) -> Result<ChangeKind> {
    ChangeKind::ALL
        .iter()
        .copied()
        .find(|kind| kind.as_str() == name)
        .with_context(|| format!("unknown evidence kind {name}"))
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

fn excluded(selection: &ExportSelection, source: &str, session: Option<&str>) -> bool {
    let Some(session_id) = session else {
        return false;
    };
    selection
        .excluded_sessions
        .iter()
        .any(|id| id.source == source && id.session_id == session_id)
}

fn selected(selection: &ExportSelection, source: &str, session: Option<&str>) -> bool {
    selection.all_sources
        || selection.sources.iter().any(|value| value == source)
        || selection
            .sessions
            .iter()
            .any(|id| id.source == source && Some(id.session_id.as_str()) == session)
}

/// Whether a row leaves the snapshot. A relationship names two sessions, so
/// it is left out when either endpoint is excluded.
fn row_excluded(selection: &ExportSelection, row: &change_feed::LiveRow) -> bool {
    if excluded(selection, &row.source, row.session.as_deref()) {
        return true;
    }
    row.columns
        .get("child_session_id")
        .and_then(|child| child.as_str())
        .is_some_and(|child| excluded(selection, &row.source, Some(child)))
}

fn make_record(
    origin: &str,
    kind: ChangeKind,
    row: change_feed::LiveRow,
) -> Result<HistoryExportRecord> {
    let record_id = hash(serde_json::to_string(&row.key)?);
    let revision = i64::try_from(row.revision).context("revision out of range")?;
    Ok(HistoryExportRecord {
        schema_version: EXPORT_SCHEMA_VERSION,
        origin_id: origin.into(),
        revision_id: hash(format!("{origin}:{record_id}:{revision}")),
        record_id,
        revision,
        kind: kind.as_str().into(),
        source: row.source,
        session_id: row.session,
        operation: "upsert".into(),
        payload: serde_json::to_value(&row.columns)?,
    })
}

/// Open a snapshot. TTL is bounded to one day; an expired snapshot can no
/// longer be read and [`expire_exports`] releases it. `now_ms` is a real Unix
/// millisecond clock.
pub fn create_export(
    conn: &Connection,
    selection: &ExportSelection,
    limits: &ExportLimits,
    ttl_ms: i64,
    now_ms: i64,
) -> Result<ExportHandle> {
    validate_export(selection, limits)?;
    ensure!(
        (1..=86_400_000).contains(&ttl_ms) && now_ms >= 0,
        "invalid export TTL/clock"
    );
    let expires_at_ms = now_ms
        .checked_add(ttl_ms)
        .context("export clock overflow")?;
    let tx = write_transaction(conn)?;
    let count: i64 = tx.query_row("SELECT COUNT(*) FROM history_exports", [], |row| row.get(0))?;
    ensure!(
        count < MAX_OPEN_EXPORTS,
        "maximum retained exports reached; close or expire old snapshots"
    );
    let (snapshot_id, cursor): (String, String) = tx.query_row(
        "SELECT lower(hex(randomblob(16))),lower(hex(randomblob(16)))",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let head = change_feed::read_head(&tx)?;
    tx.execute(
        "INSERT INTO history_exports(id,selection_json,limits_json,cutoff,expires_at_ms,cursor) \
         VALUES (?,?,?,?,?,?)",
        params![
            snapshot_id,
            serde_json::to_string(selection)?,
            serde_json::to_string(limits)?,
            i64::try_from(head.revision).context("revision out of range")?,
            expires_at_ms,
            cursor
        ],
    )?;
    for name in SUPPORTED_KINDS {
        let max_rowid = change_feed::max_rowid(&tx, change_kind(name)?)?;
        tx.execute(
            "INSERT INTO history_export_bounds(export_id,kind,max_rowid) VALUES (?,?,?)",
            params![snapshot_id, name, max_rowid],
        )?;
    }
    tx.commit()?;
    Ok(ExportHandle {
        snapshot_id,
        cursor,
        expires_at_ms,
    })
}

/// The next page of a snapshot, from an opaque cursor. Retrying a cursor
/// returns the page it returned before, so a client advances only after it
/// has written the page.
pub fn export_page(conn: &Connection, cursor: &str, now_ms: i64) -> Result<HistoryExportPage> {
    let tx = write_transaction(conn)?;
    if let Some(payload) = tx
        .query_row(
            "SELECT payload FROM history_export_pages WHERE cursor=?",
            [cursor],
            |r| r.get::<_, String>(0),
        )
        .optional()?
    {
        return Ok(serde_json::from_str(&payload)?);
    }
    let (id, selection_json, limits_json, kind_index, after, expires) = tx
        .query_row(
            "SELECT id,selection_json,limits_json,bootstrap_kind,bootstrap_rowid,expires_at_ms \
             FROM history_exports WHERE cursor=?",
            [cursor],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, i64>(4)?,
                    r.get::<_, i64>(5)?,
                ))
            },
        )
        .optional()?
        .context("export cursor not found")?;
    ensure!(expires > now_ms, "export snapshot expired");
    let selection: ExportSelection = serde_json::from_str(&selection_json)?;
    let limits: ExportLimits = serde_json::from_str(&limits_json)?;
    let origin_id = format!("{:016x}", change_feed::read_head(&tx)?.epoch);
    let next: String = tx.query_row("SELECT lower(hex(randomblob(16)))", [], |r| r.get(0))?;
    let mut page = HistoryExportPage {
        schema_version: EXPORT_SCHEMA_VERSION,
        origin_id,
        records: Vec::new(),
        next_cursor: Some(next.clone()),
    };
    // The page's serialized size, kept as records are added: the empty page,
    // plus each record and the comma before every record but the first.
    let mut bytes = serde_json::to_vec(&page)?.len();
    let mut kind_index = usize::try_from(kind_index).context("invalid export position")?;
    let mut after = after;
    let mut scanned = 0;
    'kinds: while kind_index < SUPPORTED_KINDS.len()
        && scanned < limits.max_scan_records
        && page.records.len() < limits.max_batch_records
    {
        let name = SUPPORTED_KINDS[kind_index];
        let rows = if selection.kinds.iter().any(|kind| kind == name) {
            let through: i64 = tx
                .query_row(
                    "SELECT max_rowid FROM history_export_bounds WHERE export_id=? AND kind=?",
                    params![id, name],
                    |r| r.get(0),
                )
                .optional()?
                .context(
                    "export snapshot was opened by an earlier release and cannot be read; \
                     close it and start a new one",
                )?;
            change_feed::rows_by_rowid(
                &tx,
                change_kind(name)?,
                after,
                through,
                limits.max_scan_records - scanned,
            )?
        } else {
            Vec::new()
        };
        if rows.is_empty() {
            kind_index += 1;
            after = 0;
            continue;
        }
        for row in rows {
            let rowid = row.rowid;
            if selected(&selection, &row.source, row.session.as_deref())
                && !row_excluded(&selection, &row)
            {
                let record = make_record(&page.origin_id, change_kind(name)?, row)?;
                let size =
                    serde_json::to_vec(&record)?.len() + usize::from(!page.records.is_empty());
                if bytes + size > limits.max_batch_bytes {
                    ensure!(
                        !page.records.is_empty(),
                        "export record exceeds configured page byte limit"
                    );
                    break 'kinds;
                }
                bytes += size;
                page.records.push(record);
            }
            scanned += 1;
            after = rowid;
            if page.records.len() >= limits.max_batch_records {
                break 'kinds;
            }
        }
    }
    let complete = kind_index >= SUPPORTED_KINDS.len();
    if complete {
        page.next_cursor = None;
    }
    tx.execute(
        "UPDATE history_exports SET bootstrap_kind=?,bootstrap_rowid=?,bootstrap_done=?,cursor=? \
         WHERE id=?",
        params![kind_index as i64, after, complete, next, id],
    )?;
    tx.execute(
        "INSERT INTO history_export_pages(cursor,export_id,payload) VALUES (?,?,?)",
        params![cursor, id, serde_json::to_string(&page)?],
    )?;
    tx.commit()?;
    Ok(page)
}

fn close(conn: &Connection, id: &str) -> Result<()> {
    conn.execute("DELETE FROM history_export_bounds WHERE export_id=?", [id])?;
    conn.execute("DELETE FROM history_export_pages WHERE export_id=?", [id])?;
    conn.execute("DELETE FROM history_exports WHERE id=?", [id])?;
    Ok(())
}

/// Release a snapshot and every page it served.
pub fn close_export(conn: &Connection, snapshot_id: &str) -> Result<()> {
    let tx = write_transaction(conn)?;
    close(&tx, snapshot_id)?;
    tx.commit()?;
    Ok(())
}

/// Release up to `limit` snapshots that expired at or before `now_ms`, the
/// oldest first. Returns how many were released.
pub fn expire_exports(conn: &Connection, now_ms: i64, limit: usize) -> Result<usize> {
    ensure!(
        (1..=MAX_OPEN_EXPORTS as usize).contains(&limit),
        "invalid export cleanup limit"
    );
    let tx = write_transaction(conn)?;
    let ids = tx
        .prepare(
            "SELECT id FROM history_exports WHERE expires_at_ms<=? ORDER BY expires_at_ms LIMIT ?",
        )?
        .query_map(params![now_ms, limit as i64], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for id in &ids {
        close(&tx, id)?;
    }
    tx.commit()?;
    Ok(ids.len())
}
