//! Local retention policy and explicit, bounded maintenance. No network access.
//!
//! Settings do not prune synchronously. A host runs [`SessionStore::prune_expired`]
//! on its maintenance worker, publishes durable upload acknowledgements, and
//! yields between batches. Local eviction never retracts remote copies.
mod maintenance;
mod relationships;
mod schema;
mod targets;
#[cfg(test)]
mod tests;

use crate::{Error, SessionStore, StoreOptions, Watermark};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use std::num::NonZeroU32;
use std::path::Path;

/// Shared SQLite setting. `0` means forever; an absent key means 30 days.
pub const RETENTION_CONFIG_KEY: &str = "history.retention_days";
pub const DEFAULT_RETENTION_DAYS: u32 = 30;

/// How long inactive sessions' local logs are kept. Metadata and evidence of
/// an expired session are evicted together over maintenance batches. Provider
/// originals and cloud copies are never deleted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionPolicy {
    /// `None` means forever, not the default. [`Default`] is 30 days.
    pub days: Option<NonZeroU32>,
}
impl Default for RetentionPolicy {
    fn default() -> Self {
        Self::from_days(DEFAULT_RETENTION_DAYS)
    }
}
impl RetentionPolicy {
    /// `0` keeps logs forever; positive values are days since last activity.
    pub const fn from_days(days: u32) -> Self {
        Self {
            days: NonZeroU32::new(days),
        }
    }
}

/// Read only the local setting. Older stores without settings use the default.
pub fn read_retention_policy(path: &Path) -> Result<RetentionPolicy, Error> {
    let conn = crate::store::open_db_readonly(path).map_err(Error::query)?;
    policy(&conn).map_err(Error::query)
}
/// Persist the setting, migrating this local store if needed. Run off the UI
/// thread. This does not start maintenance or delete anything.
pub fn write_retention_policy(path: &Path, policy: RetentionPolicy) -> Result<(), Error> {
    SessionStore::open(StoreOptions::default().db_path(path))?.set_retention_policy(policy)
}

/// One maintenance invocation. A host yields between calls; no read, write,
/// discovery or upload API invokes this implicitly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionOptions {
    /// Maximum payload/auxiliary rows removed per transaction (default 128,
    /// max 1024), plus final catalog and at most two presence rows.
    pub max_rows: u32,
    /// Minimum delay between committed maintenance batches (default 100 ms).
    /// This protects SQLite writers from a back-to-back maintenance loop.
    pub min_interval_ms: u32,
    /// Explicit permission to lose logs before upload. Default false. With
    /// false, no registered upload consumer means no eviction.
    pub allow_unuploaded: bool,
}
impl Default for RetentionOptions {
    fn default() -> Self {
        Self {
            max_rows: 128,
            min_interval_ms: 100,
            allow_unuploaded: false,
        }
    }
}

/// Progress of one bounded maintenance batch. Repeat after yielding until
/// `cycle_complete`, then schedule another cycle (also after uploads/settings).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionReport {
    pub rows: u64,
    pub sessions: u64,
    pub protected_sessions: u64,
    /// No usable acknowledgement floor, or an anonymous prompt batch awaits upload.
    pub awaiting_upload_ack: bool,
    /// Another capture or writer owns the store. Retry later without waiting.
    pub busy: bool,
    /// The maintenance cooldown has not elapsed. Schedule another invocation;
    /// do not spin or sleep on a session read/write thread.
    pub deferred: bool,
    pub cycle_complete: bool,
}

impl SessionStore {
    pub fn retention_policy(&self) -> Result<RetentionPolicy, Error> {
        let conn = self.read_conn()?;
        policy(&conn).map_err(Error::query)
    }
    pub fn set_retention_policy(&self, value: RetentionPolicy) -> Result<(), Error> {
        let mut conn = writable(self, "set_retention_policy")?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(Error::sql)?;
        tx.execute("INSERT INTO store_settings(key,value) VALUES (?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            params![RETENTION_CONFIG_KEY, value.days.map_or(0, NonZeroU32::get).to_string()]).map_err(Error::sql)?;
        tx.execute("DELETE FROM retention_sweep", [])
            .map_err(Error::sql)?;
        tx.commit().map_err(Error::sql)
    }
    /// Register every required upload destination *before* capture/delivery.
    /// Registration starts at zero and blocks eviction until acknowledged.
    /// Re-registering the same name preserves its progress.
    pub fn register_retention_consumer(&self, name: &str) -> Result<(), Error> {
        validate_name(name)?;
        writable(self, "register_retention_consumer")?
            .execute(
                "INSERT OR IGNORE INTO retention_consumers(name,epoch,revision) VALUES (?1,0,0)",
                [name],
            )
            .map_err(Error::sql)?;
        Ok(())
    }
    /// Publish an all-kinds, unfiltered feed position only after every record
    /// through it has been durably uploaded (or explicitly excluded). This is
    /// separate from a read cursor: reading alone is not upload acceptance.
    /// Positions must belong to this store and cannot exceed its current head.
    pub fn acknowledge_retention(&self, name: &str, position: Watermark) -> Result<(), Error> {
        validate_name(name)?;
        let mut conn = writable(self, "acknowledge_retention")?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(Error::sql)?;
        let head = crate::change_feed::read_head(&tx).map_err(Error::query)?;
        if position.epoch != head.epoch || position.revision > head.revision {
            return Err(Error::WatermarkAheadOfStore(
                "retention acknowledgement must belong to this store and not exceed its head"
                    .into(),
            ));
        }
        let changed = tx.execute("UPDATE retention_consumers SET epoch=?2,revision=CASE WHEN epoch=?2 THEN MAX(revision,?3) ELSE ?3 END WHERE name=?1",
            params![name, position.epoch as i64, position.revision as i64]).map_err(Error::sql)?;
        if changed == 0 {
            return Err(Error::InvalidArgument(
                "register retention consumer before acknowledging".into(),
            ));
        }
        tx.commit().map_err(Error::sql)
    }
    /// Remove a permanently retired destination's protection. Explicitly doing
    /// so permits eviction of its undelivered logs once other consumers ack.
    pub fn remove_retention_consumer(&self, name: &str) -> Result<(), Error> {
        validate_name(name)?;
        writable(self, "remove_retention_consumer")?
            .execute("DELETE FROM retention_consumers WHERE name=?1", [name])
            .map_err(Error::sql)?;
        Ok(())
    }
    /// Evict one batch of expired local logs. Call on a separate maintenance
    /// worker, never in the session write-to-list path. Uses WAL transactions,
    /// yields to capture, and leaves no eviction tombstones for uploaders.
    pub fn prune_expired(&self, opts: RetentionOptions) -> Result<RetentionReport, Error> {
        maintenance::run(self, &opts, crate::now_ms())
    }
}

pub(crate) fn init_indexes(conn: &Connection) -> anyhow::Result<()> {
    schema::init_indexes(conn)
}
pub(crate) fn indexes_current(conn: &Connection) -> anyhow::Result<bool> {
    schema::indexes_current(conn)
}
pub(crate) fn init_schema(conn: &Connection) -> anyhow::Result<()> {
    conn.execute_batch("CREATE TABLE IF NOT EXISTS store_settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS retention_consumers(name TEXT PRIMARY KEY,epoch INTEGER NOT NULL,revision INTEGER NOT NULL CHECK(revision>=0));
        CREATE TABLE IF NOT EXISTS retention_pending(source TEXT NOT NULL,session_id TEXT NOT NULL,last_activity_ms INTEGER NOT NULL,PRIMARY KEY(source,session_id));
        CREATE TABLE IF NOT EXISTS retention_sweep(singleton INTEGER PRIMARY KEY CHECK(singleton=1),source TEXT NOT NULL,session_id TEXT NOT NULL);")?;
    Ok(())
}
fn writable(store: &SessionStore, operation: &str) -> Result<Connection, Error> {
    if store.read_only() {
        return Err(Error::read_only(operation));
    }
    crate::store::open_db(store.db_path()).map_err(Error::query)
}
fn validate_name(name: &str) -> Result<(), Error> {
    if name.trim().is_empty() || name.len() > 256 {
        return Err(Error::InvalidArgument(
            "retention consumer name must contain 1..=256 bytes".into(),
        ));
    }
    Ok(())
}
fn policy(conn: &Connection) -> anyhow::Result<RetentionPolicy> {
    if !conn
        .prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='store_settings'")?
        .exists([])?
    {
        return Ok(RetentionPolicy::default());
    }
    let days: Option<String> = conn
        .query_row(
            "SELECT value FROM store_settings WHERE key=?1",
            [RETENTION_CONFIG_KEY],
            |row| row.get(0),
        )
        .optional()?;
    Ok(match days {
        Some(days) => RetentionPolicy::from_days(days.parse()?),
        None => RetentionPolicy::default(),
    })
}
