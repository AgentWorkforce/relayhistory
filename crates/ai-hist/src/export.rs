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
/// Largest number of journal rows one compaction transaction deletes or
/// examines.
pub const MAX_COMPACTION_PAGE: usize = 10_000;

/// The share of the retention cap above which capture applies backpressure:
/// it reclaims consumed changes first and stops the pass if that is not enough.
pub const RETENTION_HIGH_WATER_PERCENT: i64 = 90;

/// Capture stopped because the delivery retention budget is exhausted.
///
/// Carried in the error chain of every capture failure caused by the cap,
/// whether the pass stopped at the high-water check or a trigger aborted a
/// session transaction, so a host can report the usage without reading the
/// database again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetentionLimitReached {
    pub used_bytes: i64,
    pub limit_bytes: i64,
}
impl std::fmt::Display for RetentionLimitReached {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "delivery retention limit exceeded; {} of {} bytes retained; compact consumed data or raise the retention cap",
            self.used_bytes, self.limit_bytes
        )
    }
}
impl std::error::Error for RetentionLimitReached {}

/// Whether `used_bytes` is over the high-water mark of `limit_bytes`: the
/// usage at which capture stops rather than attempting another session.
pub fn above_high_water(used_bytes: i64, limit_bytes: i64) -> bool {
    i128::from(used_bytes) * 100
        > i128::from(limit_bytes) * i128::from(RETENTION_HIGH_WATER_PERCENT)
}

/// Backpressure before a source pass or a session transaction. Above the
/// high-water mark this reclaims consumed changes down to the low-water mark
/// with [`compact_to_low_water`]; if the budget is still above the high-water
/// mark the caller stops its pass with a [`RetentionLimitReached`] error
/// instead of attempting the remaining sessions. Requires autocommit mode:
/// compaction takes its own transactions.
pub fn ensure_capture_headroom(conn: &Connection) -> Result<()> {
    let (used_bytes, limit_bytes) = retained_bytes(conn)?;
    if !above_high_water(used_bytes, limit_bytes) {
        return Ok(());
    }
    compact_to_low_water(conn, MAX_COMPACTION_PAGE)?;
    let (used_bytes, limit_bytes) = retained_bytes(conn)?;
    if above_high_water(used_bytes, limit_bytes) {
        return Err(RetentionLimitReached {
            used_bytes,
            limit_bytes,
        }
        .into());
    }
    Ok(())
}

/// The retention usage a capture failure carries, if the cap caused it. The
/// usage is attached as context, which `anyhow` resolves through the
/// top-level downcast rather than the source chain.
pub fn retention_limit_usage(error: &anyhow::Error) -> Option<RetentionLimitReached> {
    error.downcast_ref::<RetentionLimitReached>().copied()
}

/// Attach the current retention usage to a trigger-aborted capture failure so
/// it reports like a high-water stop. Any other error is returned unchanged.
/// Read after the failed transaction rolled back; a usage read that fails
/// leaves the error as it was.
pub fn annotate_retention_limit(conn: &Connection, error: anyhow::Error) -> anyhow::Error {
    if !is_retention_limit(&error) || retention_limit_usage(&error).is_some() {
        return error;
    }
    match retained_bytes(conn) {
        Ok((used_bytes, limit_bytes)) => error.context(RetentionLimitReached {
            used_bytes,
            limit_bytes,
        }),
        Err(_) => error,
    }
}

fn validate_compaction_page(limit: usize) -> Result<()> {
    ensure!(
        (1..=MAX_COMPACTION_PAGE).contains(&limit),
        "invalid compaction limit"
    );
    Ok(())
}

/// The sequence every subscription has consumed through: every journal row
/// at or below it is reclaimable outright. A subscription reading every
/// session pins everything past its cursor; one reading a single session pins
/// only that session's rows past its cursor, so it counts only while such a
/// row exists. With nothing pinning, the whole journal is consumed.
fn consumed_floor(conn: &Connection) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT COALESCE((SELECT MIN(s.journal_cursor) FROM history_subscriptions s
            WHERE s.source IS NULL OR EXISTS (
                SELECT 1 FROM delivery_journal j
                WHERE j.source=s.source AND j.session_id=s.session_id AND j.seq>s.journal_cursor)),
            (SELECT COALESCE(MAX(seq),0) FROM delivery_journal))",
        [],
        |r| r.get(0),
    )?)
}

/// The last sequence a sweep can reclaim: the lowest cursor of a subscription
/// reading every session, or the tail without one. Every row past it is
/// retained by that subscription, so no sweep examines it.
fn sweep_ceiling(conn: &Connection) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT MIN(COALESCE((SELECT MIN(journal_cursor) FROM history_subscriptions WHERE source IS NULL),t.tail),t.tail)
         FROM (SELECT COALESCE(MAX(seq),0) AS tail FROM delivery_journal) t",
        [],
        |r| r.get(0),
    )?)
}

/// Delete up to `limit` rows at or below `floor`: one indexed range on the
/// primary key, no per-row predicate, so the cost is the rows reclaimed.
fn reclaim_consumed(tx: &Transaction<'_>, floor: i64, limit: usize) -> Result<usize> {
    Ok(tx.execute(
        "DELETE FROM delivery_journal WHERE seq<=(SELECT MAX(seq) FROM (SELECT seq FROM delivery_journal WHERE seq<=? ORDER BY seq LIMIT ?))",
        params![floor, limit as i64],
    )?)
}

/// The next sweep page: at most `limit` rows after `after` and no later than
/// `through`, as its row count and last sequence.
fn sweep_page(
    tx: &Transaction<'_>,
    after: i64,
    through: i64,
    limit: usize,
) -> Result<Option<(usize, i64)>> {
    let (count, end): (i64, Option<i64>) = tx.query_row(
        "SELECT COUNT(*),MAX(seq) FROM (SELECT seq FROM delivery_journal WHERE seq>? AND seq<=? ORDER BY seq LIMIT ?)",
        params![after, through, limit as i64],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    Ok(end.map(|end| (count as usize, end)))
}

/// Delete the rows in `(after, end]` no interested subscription still needs
/// and leave the sweep cursor at `cursor`.
fn sweep_range(tx: &Transaction<'_>, after: i64, end: i64, cursor: i64) -> Result<usize> {
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
        [cursor],
    )?;
    Ok(removed)
}

/// Reclaim every row at or below `floor` in transactions of at most `limit`
/// rows. Returns the number of rows removed and the floor the last
/// transaction observed.
fn reclaim_below(
    conn: &Connection,
    floor: impl Fn(&Connection) -> Result<i64>,
    limit: usize,
    more: &dyn Fn() -> bool,
) -> Result<(usize, i64)> {
    let mut removed = 0;
    loop {
        let tx = write_transaction(conn)?;
        let floor = floor(&tx)?;
        let reclaimed = reclaim_consumed(&tx, floor, limit)?;
        tx.commit()?;
        removed += reclaimed;
        if reclaimed < limit || !more() {
            return Ok((removed, floor));
        }
    }
}
/// A caller with no deadline of its own.
fn always() -> &'static dyn Fn() -> bool {
    &|| true
}

/// The steady-state compaction step: reclaim everything every subscription has
/// consumed, then examine one page above the floor.
///
/// Rows at or below the consumed floor go by indexed range in transactions of
/// at most `limit` rows, so the work is proportional to what is reclaimable,
/// never to the journal's length, and no consumed row waits on a cursor. Above
/// the floor, where a lagging session subscription pins its own rows among
/// other sessions' reclaimable ones, one page of `limit` rows is examined with
/// the exact per-session predicate from the persistent sweep cursor. The sweep
/// reaches only as far as the lowest cursor of a subscription reading every
/// session, since everything past it is retained; the cursor never sits below
/// the floor and wraps back to it whenever a page is short. Bootstrap
/// preimages are retained independently of this journal.
pub fn compact_journal(conn: &Connection, limit: usize) -> Result<usize> {
    compact_journal_while(conn, limit, always())
}

/// [`compact_journal`] for a caller that owns a deadline or a stop signal:
/// `more` is consulted between transactions, and a false answer ends the
/// reclaim where it stands. Each transaction is bounded either way.
pub fn compact_journal_while(
    conn: &Connection,
    limit: usize,
    more: &dyn Fn() -> bool,
) -> Result<usize> {
    validate_compaction_page(limit)?;
    let (mut removed, _) = reclaim_below(conn, consumed_floor, limit, more)?;
    // A stop that arrives during reclamation ends the call there: the sweep is
    // its own transaction, and one more of those is exactly what the stop asked
    // the caller not to wait for.
    if !more() {
        return Ok(removed);
    }
    let tx = write_transaction(conn)?;
    let floor = consumed_floor(&tx)?;
    let ceiling = sweep_ceiling(&tx)?;
    let saved: i64 = tx.query_row(
        "SELECT cursor FROM history_compaction WHERE singleton=1",
        [],
        |r| r.get(0),
    )?;
    let mut after = saved.max(floor);
    let mut page = sweep_page(&tx, after, ceiling, limit)?;
    if page.is_none() && after != floor {
        after = floor;
        page = sweep_page(&tx, after, ceiling, limit)?;
    }
    if let Some((count, end)) = page {
        let cursor = if count < limit { floor } else { end };
        removed += sweep_range(&tx, after, end, cursor)?;
    }
    tx.commit()?;
    Ok(removed)
}

/// One complete pass for retention recovery: every row at or below the
/// consumed floor, then a sweep with the exact predicate from the floor to the
/// sweep ceiling as it stood on entry, so concurrent appends cannot extend the
/// pass. Each transaction deletes or examines at most `page_size` rows.
/// Returns the number of rows removed; zero means nothing in the journal is
/// reclaimable.
pub fn compact_journal_pass(conn: &Connection, page_size: usize) -> Result<usize> {
    validate_compaction_page(page_size)?;
    journal_pass(conn, page_size, always())
}

fn journal_pass(conn: &Connection, page_size: usize, more: &dyn Fn() -> bool) -> Result<usize> {
    let through = sweep_ceiling(conn)?;
    let floor = |conn: &Connection| Ok(consumed_floor(conn)?.min(through));
    let (mut removed, floor) = reclaim_below(conn, floor, page_size, more)?;
    let mut after = floor;
    while after < through && more() {
        let tx = write_transaction(conn)?;
        let Some((count, end)) = sweep_page(&tx, after, through, page_size)? else {
            break;
        };
        let cursor = if count < page_size { floor } else { end };
        removed += sweep_range(&tx, after, end, cursor)?;
        tx.commit()?;
        after = end;
    }
    Ok(removed)
}

/// Whether retained bytes are under the low-water mark: three quarters of the
/// retention cap.
fn below_low_water(conn: &Connection) -> Result<bool> {
    let (used, limit) = retained_bytes(conn)?;
    Ok(i128::from(used) * 4 < i128::from(limit) * 3)
}

/// Budget-driven recovery: run complete passes while retained bytes are at or
/// above three quarters of the retention cap and the last pass reclaimed a
/// full page, so a pass that reclaimed less has caught up with whatever other
/// consumers freed meanwhile and ends the loop. Below the low-water mark this
/// does no work. Returns the number of rows removed.
pub fn compact_to_low_water(conn: &Connection, page_size: usize) -> Result<usize> {
    compact_to_low_water_while(conn, page_size, always())
}

/// [`compact_to_low_water`] for a caller that owns a deadline or a stop
/// signal: `more` is consulted between transactions and between passes, and a
/// false answer ends the recovery where it stands.
pub fn compact_to_low_water_while(
    conn: &Connection,
    page_size: usize,
    more: &dyn Fn() -> bool,
) -> Result<usize> {
    validate_compaction_page(page_size)?;
    let mut removed = 0;
    while !below_low_water(conn)? && more() {
        let reclaimed = journal_pass(conn, page_size, more)?;
        removed += reclaimed;
        if reclaimed < page_size {
            break;
        }
    }
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

/// Whether a failure was caused by the retention cap: a high-water stop or a
/// capture trigger abort inside a session transaction.
pub fn is_retention_limit(error: &anyhow::Error) -> bool {
    retention_limit_usage(error).is_some()
        || error.chain().any(|cause| {
            matches!(cause.downcast_ref::<rusqlite::Error>(),Some(rusqlite::Error::SqliteFailure(_,Some(message))) if message.starts_with("delivery retention limit exceeded;"))
        })
}
