//! Local evidence export: a consistent snapshot of the store a user writes to
//! a file or a pipe, one NDJSON record at a time, in bounded pages.
//!
//! An [`ExportSnapshot`] owns a connection holding one read transaction for
//! its whole life. Every page it serves reads that transaction's view, so the
//! export is the store exactly as it stood when the snapshot opened: a row
//! written, rewritten or deleted afterwards -- including a new row that
//! reuses a deleted row's rowid -- does not change what the export contains.
//! The store is in WAL mode, so the open transaction never blocks a writer;
//! it holds back checkpointing past its view until the snapshot is dropped,
//! which ends the transaction.
//!
//! A record carries the same identity and revision the change feed reports
//! for the row ([`crate::Change::key`], [`crate::Change::revision`]), so an
//! embedder can merge an export with the feed.
use anyhow::{ensure, Context, Result};
use rusqlite::Connection;
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

/// Longest a snapshot may stay open: one day.
pub const MAX_EXPORT_TTL_MS: i64 = 86_400_000;

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
    /// Most bytes one page serializes to, envelope included.
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

fn token(conn: &Connection) -> Result<String> {
    Ok(conn.query_row("SELECT lower(hex(randomblob(16)))", [], |r| r.get(0))?)
}

/// One open export: a read transaction over the store, and the position of
/// the next page within it.
///
/// Pages are named by opaque cursors. Each page names the cursor of the next,
/// and the cursor of the page just served serves that page again, so a client
/// that failed to write a page retries it and advances only once it has.
/// Dropping the snapshot ends its transaction.
pub struct ExportSnapshot {
    conn: Connection,
    selection: ExportSelection,
    limits: ExportLimits,
    snapshot_id: String,
    origin_id: String,
    expires_at_ms: i64,
    /// The cursor of the next page, or `None` once the last page is served.
    cursor: Option<String>,
    /// The page served last, under the cursor that asked for it.
    served: Option<(String, HistoryExportPage)>,
    kind_index: usize,
    after: i64,
}

impl std::fmt::Debug for ExportSnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExportSnapshot")
            .field("snapshot_id", &self.snapshot_id)
            .field("expires_at_ms", &self.expires_at_ms)
            .finish_non_exhaustive()
    }
}

impl ExportSnapshot {
    /// Open a snapshot over `conn`, which it owns from here on and holds in
    /// one read transaction until dropped. `conn` must be a store connection
    /// with no transaction open. `ttl_ms` is at most [`MAX_EXPORT_TTL_MS`];
    /// `now_ms` is a real Unix millisecond clock.
    pub fn open(
        conn: Connection,
        selection: &ExportSelection,
        limits: &ExportLimits,
        ttl_ms: i64,
        now_ms: i64,
    ) -> Result<Self> {
        validate_export(selection, limits)?;
        ensure!(
            (1..=MAX_EXPORT_TTL_MS).contains(&ttl_ms) && now_ms >= 0,
            "invalid export TTL/clock"
        );
        let expires_at_ms = now_ms
            .checked_add(ttl_ms)
            .context("export clock overflow")?;
        ensure!(
            conn.is_autocommit(),
            "an export snapshot needs a connection with no open transaction"
        );
        let (snapshot_id, cursor) = (token(&conn)?, token(&conn)?);
        // A deferred transaction takes its snapshot at its first read, which
        // is the head read below: from here every page sees this view.
        conn.execute_batch("BEGIN DEFERRED")?;
        let head = change_feed::read_head(&conn)?;
        Ok(Self {
            conn,
            selection: selection.clone(),
            limits: limits.clone(),
            snapshot_id,
            origin_id: format!("{:016x}", head.epoch),
            expires_at_ms,
            cursor: Some(cursor),
            served: None,
            kind_index: 0,
            after: 0,
        })
    }

    /// The snapshot's id, its first cursor and its expiry.
    pub fn handle(&self) -> ExportHandle {
        ExportHandle {
            snapshot_id: self.snapshot_id.clone(),
            cursor: self
                .served
                .as_ref()
                .map(|(cursor, _)| cursor.clone())
                .or_else(|| self.cursor.clone())
                .unwrap_or_default(),
            expires_at_ms: self.expires_at_ms,
        }
    }

    pub fn snapshot_id(&self) -> &str {
        &self.snapshot_id
    }

    /// Whether the snapshot has expired at `now_ms`.
    pub fn expired(&self, now_ms: i64) -> bool {
        self.expires_at_ms <= now_ms
    }

    /// Whether `cursor` names a page of this snapshot: the next one, or the
    /// one served last.
    pub fn owns_cursor(&self, cursor: &str) -> bool {
        self.cursor.as_deref() == Some(cursor)
            || self
                .served
                .as_ref()
                .is_some_and(|(served, _)| served == cursor)
    }

    /// The page `cursor` names. An expired snapshot serves nothing.
    pub fn page(&mut self, cursor: &str, now_ms: i64) -> Result<HistoryExportPage> {
        ensure!(!self.expired(now_ms), "export snapshot expired");
        if let Some((served, page)) = &self.served {
            if served == cursor {
                return Ok(page.clone());
            }
        }
        ensure!(
            self.cursor.as_deref() == Some(cursor),
            "export cursor not found"
        );
        let next = token(&self.conn)?;
        let mut page = HistoryExportPage {
            schema_version: EXPORT_SCHEMA_VERSION,
            origin_id: self.origin_id.clone(),
            records: Vec::new(),
            next_cursor: Some(next.clone()),
        };
        // The page's serialized size, kept exactly as records are added: the
        // empty page with a cursor (the longest its envelope can be), plus
        // each record and the comma before every record but the first.
        let mut bytes = serde_json::to_vec(&page)?.len();
        ensure!(
            bytes < self.limits.max_batch_bytes,
            "export page envelope exceeds configured page byte limit"
        );
        // The position advances on copies and is kept only once the page is
        // whole, so a failed read leaves the cursor where it was and a retry
        // serves every record.
        let mut kind_index = self.kind_index;
        let mut after = self.after;
        let mut scanned = 0;
        'kinds: while kind_index < SUPPORTED_KINDS.len()
            && scanned < self.limits.max_scan_records
            && page.records.len() < self.limits.max_batch_records
        {
            let name = SUPPORTED_KINDS[kind_index];
            let rows = if self.selection.kinds.iter().any(|kind| kind == name) {
                change_feed::rows_by_rowid(
                    &self.conn,
                    change_kind(name)?,
                    after,
                    self.limits.max_scan_records - scanned,
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
                if selected(&self.selection, &row.source, row.session.as_deref())
                    && !row_excluded(&self.selection, &row)
                {
                    let record = make_record(&self.origin_id, change_kind(name)?, row)?;
                    let size =
                        serde_json::to_vec(&record)?.len() + usize::from(!page.records.is_empty());
                    if bytes + size > self.limits.max_batch_bytes {
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
                if page.records.len() >= self.limits.max_batch_records {
                    break 'kinds;
                }
            }
        }
        if kind_index >= SUPPORTED_KINDS.len() {
            page.next_cursor = None;
        }
        self.kind_index = kind_index;
        self.after = after;
        self.cursor = page.next_cursor.clone();
        self.served = Some((cursor.to_string(), page.clone()));
        Ok(page)
    }
}
