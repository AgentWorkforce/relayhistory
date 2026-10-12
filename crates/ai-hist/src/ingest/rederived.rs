//! Sessions a full sweep re-derived from byte zero of their own source, and
//! which of them the destination shortfall may take at their word.

use std::collections::HashSet;

use anyhow::Result;
use rusqlite::{params, Connection};

/// The sessions of `rederived` that hold no remote-backed evidence. A local
/// re-read restores only local rows, so a session with rows a remote side
/// backs keeps the ordinary shortfall check; one that cannot be read keeps it
/// too. A remote presence with no evidence behind it has nothing to restore.
pub(super) fn local_only(
    conn: &Connection,
    rederived: &HashSet<(String, String)>,
) -> HashSet<(String, String)> {
    rederived
        .iter()
        .filter(|(source, session_id)| {
            matches!(holds_remote_evidence(conn, source, session_id), Ok(false))
        })
        .cloned()
        .collect()
}

fn holds_remote_evidence(conn: &Connection, source: &str, session_id: &str) -> Result<bool> {
    for table in crate::store::EVIDENCE_LOCATION_TABLES {
        let remote: bool = conn
            .prepare_cached(&format!(
                "SELECT EXISTS(SELECT 1 FROM {table} WHERE source = ?1 AND session_id = ?2 \
                 AND location IN ('remote', 'both'))"
            ))?
            .query_row(params![source, session_id], |row| row.get(0))?;
        if remote {
            return Ok(true);
        }
    }
    Ok(false)
}
