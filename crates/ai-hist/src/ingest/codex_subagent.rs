//! The rollout walk's fast path for a Codex rollout an earlier sync recorded
//! as a delegated child thread.

use super::{
    cached_query_row, cleanup_codex_subagent_history, cleanup_codex_subagent_registration,
    read_codex_session_meta, record_codex_delegation, Result,
};
use rusqlite::Connection;
use std::collections::HashMap;
use std::path::Path;

#[cfg(test)]
thread_local! {
    static REPAIRS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// How many recorded subagents this thread repaired since the last call.
#[cfg(test)]
pub(super) fn take_repairs() -> u64 {
    REPAIRS.with(|repairs| repairs.replace(0))
}

/// Whether the store holds a recorded subagent's delegation edge, and whether
/// the repair's cleanups would delete nothing.
///
/// The cleanups delete prompt history and a local registration only when no
/// remote presence makes them shared evidence, and the session row only when
/// no presence is left. So with a remote presence they are no-ops, and
/// without one they are no-ops exactly when no history, local presence or
/// session row exists. Every probe is a seek on an index this crate creates.
const CODEX_SUBAGENT_STATE_SQL: &str = "SELECT \
       EXISTS(SELECT 1 FROM session_relationships \
              WHERE source = 'codex' AND child_session_id = ?1 \
                AND relationship = 'delegated'), \
       EXISTS(SELECT 1 FROM session_presences \
              WHERE source = 'codex' AND session_id = ?1 AND location = 'remote') \
       OR NOT (\
         EXISTS(SELECT 1 FROM history WHERE source = 'codex' AND session_id = ?1) \
         OR EXISTS(SELECT 1 FROM session_presences \
                   WHERE source = 'codex' AND session_id = ?1 AND location = 'local') \
         OR EXISTS(SELECT 1 FROM sessions WHERE source = 'codex' AND session_id = ?1)\
       )";

/// Repair the store for an unchanged rollout recorded as a subagent, and say
/// whether it is a session after all.
///
/// A recorded subagent with no delegation edge is re-read by its
/// `session_meta` line. One that names a parent gets the edge backfilled (a
/// database synced before delegation was recorded has no topology, and its
/// stamps never change again). One that names none is a standalone thread an
/// earlier build hid as a child: this returns `true`, leaving it to the
/// walk's full path to catalog it.
///
/// The repair is a one-time one. A subagent whose edge is recorded and which
/// has nothing left to clean up is already repaired, and one indexed probe
/// says so; a steady sweep asks it of every recorded subagent rather than
/// re-running the cleanup's deletes and their row triggers.
pub(super) fn repair_recorded_codex_subagent(
    conn: &Connection,
    rollout: &Path,
    session_id: &str,
    cwds: &mut HashMap<String, String>,
    branches: &mut HashMap<String, String>,
) -> Result<bool> {
    let (delegated, clean): (bool, bool) =
        cached_query_row(conn, CODEX_SUBAGENT_STATE_SQL, [session_id], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?;
    let unlinked_meta = if delegated {
        None
    } else {
        read_codex_session_meta(rollout)?
    };
    if unlinked_meta.as_ref().is_some_and(|meta| !meta.is_subagent) {
        return Ok(true);
    }
    // Presence backfill can recreate a local catalog registration from
    // retained subagent events without changing the rollout stamp.
    cwds.remove(session_id);
    branches.remove(session_id);
    if delegated && clean {
        return Ok(false);
    }
    #[cfg(test)]
    REPAIRS.with(|repairs| repairs.set(repairs.get() + 1));
    cleanup_codex_subagent_history(conn, session_id)?;
    cleanup_codex_subagent_registration(conn, session_id)?;
    if let Some(meta) = &unlinked_meta {
        if let Some(parent) = meta.parent_session_id.as_deref() {
            record_codex_delegation(conn, parent, meta, rollout)?;
        }
    }
    Ok(false)
}
