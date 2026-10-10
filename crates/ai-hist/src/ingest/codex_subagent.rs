//! The rollout walk's fast path for a Codex rollout an earlier sync recorded
//! as a delegated child thread.

use super::{
    cleanup_codex_subagent_history, cleanup_codex_subagent_registration, codex_delegation_recorded,
    read_codex_session_meta, record_codex_delegation, Result,
};
use rusqlite::Connection;
use std::collections::HashMap;
use std::path::Path;

/// Repair the store for an unchanged rollout recorded as a subagent, and say
/// whether it is a session after all.
///
/// A recorded subagent with no delegation edge is re-read by its
/// `session_meta` line. One that names a parent gets the edge backfilled (a
/// database synced before delegation was recorded has no topology, and its
/// stamps never change again). One that names none is a standalone thread an
/// earlier build hid as a child: this returns `true`, leaving it to the
/// walk's full path to catalog it, and forgets the discovery skip that
/// remembered it as a non-session at this same stamp.
pub(super) fn repair_recorded_codex_subagent(
    conn: &Connection,
    rollout: &Path,
    session_id: &str,
    cwds: &mut HashMap<String, String>,
    branches: &mut HashMap<String, String>,
) -> Result<bool> {
    let unlinked_meta = if codex_delegation_recorded(conn, session_id)? {
        None
    } else {
        read_codex_session_meta(rollout)?
    };
    if unlinked_meta.as_ref().is_some_and(|meta| !meta.is_subagent) {
        conn.execute(
            "DELETE FROM observation_discovery_skips WHERE source = 'codex' AND locator = ?",
            [rollout.to_string_lossy()],
        )?;
        return Ok(true);
    }
    // Presence backfill can recreate a local catalog registration from
    // retained subagent events without changing the rollout stamp.
    cwds.remove(session_id);
    branches.remove(session_id);
    cleanup_codex_subagent_history(conn, session_id)?;
    cleanup_codex_subagent_registration(conn, session_id)?;
    if let Some(meta) = &unlinked_meta {
        if let Some(parent) = meta.parent_session_id.as_deref() {
            record_codex_delegation(conn, parent, meta, rollout)?;
        }
    }
    Ok(false)
}
