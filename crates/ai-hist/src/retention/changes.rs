//! Bounded local invalidation history, committed with each eviction generation.
use crate::{Error, SessionIdentity, SessionStore};
use rusqlite::{params, Connection};
use std::collections::BTreeSet;

const KEPT_GENERATIONS: u64 = 1_024;
const MAX_IDENTITIES: usize = 4_096;
pub(super) type Affected = BTreeSet<(String, String)>;

/// Local retention changes since a caller's generation, on one read snapshot.
/// Pair the cursor with the store epoch. Ordinary evidence changes must also
/// be followed through the change feed.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RetentionChanges {
    /// Generation reached by this snapshot, including when coverage was lost.
    pub generation: u64,
    /// Identities whose existence or delegation may have changed. `None` means
    /// coverage is unavailable and the caller must rebuild its local view.
    /// An empty list is complete coverage with no named identities affected.
    pub sessions: Option<Vec<SessionIdentity>>,
}

impl SessionStore {
    /// Read bounded retention invalidations after `generation`. Missing batches
    /// from an older writer, an expired cursor, or an oversized result return
    /// `sessions: None`. Read-only handles also work with pre-journal stores.
    /// Persist the returned generation only after refreshing the named sessions.
    pub fn retention_changes_since(&self, generation: u64) -> Result<RetentionChanges, Error> {
        let conn = self.read_conn()?;
        let tx = conn.unchecked_transaction().map_err(Error::sql)?;
        let head = super::generation::read(&tx).map_err(Error::query)?;
        let sessions = read(&tx, generation, head).map_err(Error::query)?;
        tx.commit().map_err(Error::sql)?;
        Ok(RetentionChanges {
            generation: head,
            sessions: sessions.map(|sessions| {
                sessions
                    .into_iter()
                    .map(|(source, id)| SessionIdentity::new(source, id))
                    .collect()
            }),
        })
    }
}

pub(super) fn present(conn: &Connection) -> anyhow::Result<bool> {
    Ok(conn
        .prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='retention_changes'")?
        .exists([])?)
}

pub(super) fn init(conn: &Connection) -> anyhow::Result<()> {
    conn.execute_batch("CREATE TABLE IF NOT EXISTS retention_changes(generation TEXT PRIMARY KEY, identities TEXT NOT NULL) WITHOUT ROWID;")?;
    Ok(())
}

/// Fixed-width decimal preserves the full u64 token and lexicographic ordering.
fn key(generation: u64) -> String {
    format!("{generation:020}")
}

pub(super) fn record(
    conn: &Connection,
    generation: u64,
    identities: &Affected,
) -> anyhow::Result<()> {
    conn.execute(
        "INSERT INTO retention_changes(generation,identities) VALUES (?1,?2)",
        params![key(generation), serde_json::to_string(identities)?],
    )?;
    conn.execute(
        "DELETE FROM retention_changes WHERE generation<=?1",
        [key(generation.saturating_sub(KEPT_GENERATIONS))],
    )?;
    Ok(())
}

fn read(conn: &Connection, after: u64, head: u64) -> anyhow::Result<Option<Affected>> {
    if after == head {
        return Ok(Some(Affected::new()));
    }
    if after > head || head - after > KEPT_GENERATIONS || !present(conn)? {
        return Ok(None);
    }
    let mut statement = conn.prepare("SELECT generation,identities FROM retention_changes WHERE generation>?1 AND generation<=?2 ORDER BY generation")?;
    let mut rows = statement.query(params![key(after), key(head)])?;
    let mut reached = after;
    let mut identities = Affected::new();
    while let Some(row) = rows.next()? {
        let generation: String = row.get(0)?;
        if generation != key(reached + 1) {
            return Ok(None);
        }
        let encoded: String = row.get(1)?;
        let batch: Affected = serde_json::from_str(&encoded)?;
        identities.extend(batch);
        if identities.len() > MAX_IDENTITIES {
            return Ok(None);
        }
        reached += 1;
    }
    Ok((reached == head).then_some(identities))
}
