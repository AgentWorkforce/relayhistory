//! Undoing a delegated thread's registration as a session: its local catalog
//! row and presence, and the prompt history an older sync gave it, unless a
//! remote observation makes them shared evidence.

use super::{cached_query_row, evidence_probe, Result};
use rusqlite::{params, Connection};

/// Reconcile the catalog registration for a locally observed subagent.
///
/// A local-only subagent stays out of the catalog. If the same canonical
/// session was separately discovered remotely, its retained local events are
/// also local evidence, so both presences and the canonical row must survive.
///
/// Deleting a `sessions` row fires `delete_session_hydration_state`, which
/// cascades away every relationship the row takes part in — as parent as well
/// as child. Undoing a subagent's registration is not that session going away,
/// so the observed delegation evidence, including the thread's own outgoing
/// edges, is carried across the delete. The trigger itself cannot be narrowed
/// instead: `CREATE TRIGGER IF NOT EXISTS` never replaces the one an existing
/// database already has.
pub(super) fn cleanup_subagent_registration(
    conn: &Connection,
    source: &str,
    session_id: &str,
) -> Result<()> {
    // Cached: a sweep runs this once per changed delegated thread, and each
    // statement fires row triggers that a fresh prepare would compile again.
    conn.prepare_cached(
        "DELETE FROM session_presences \
         WHERE source = ? AND session_id = ? AND location = 'local' \
           AND NOT EXISTS (\
             SELECT 1 FROM session_presences \
             WHERE source = ? AND session_id = ? AND location = 'remote'\
           )",
    )?
    .execute(params![source, session_id, source, session_id])?;
    conn.prepare_cached(
        "CREATE TEMP TABLE IF NOT EXISTS retained_relationships \
           AS SELECT * FROM session_relationships WHERE 0",
    )?
    .execute([])?;
    conn.prepare_cached("DELETE FROM retained_relationships")?
        .execute([])?;
    // One seek per end of the edge; see `session_continuity_edges`.
    conn.prepare_cached(
        "INSERT INTO retained_relationships \
         SELECT * FROM session_relationships WHERE source = ?1 AND parent_session_id = ?2 \
         UNION ALL \
         SELECT * FROM session_relationships \
         WHERE source = ?1 AND child_session_id = ?2 AND parent_session_id <> ?2",
    )?
    .execute(params![source, session_id])?;
    let sessions = conn
        .prepare_cached(
            "DELETE FROM sessions \
             WHERE source = ? AND session_id = ? \
               AND NOT EXISTS (\
                 SELECT 1 FROM session_presences \
                 WHERE source = ? AND session_id = ?\
               )",
        )?
        .execute(params![source, session_id, source, session_id])?;
    conn.prepare_cached(
        "INSERT OR IGNORE INTO session_relationships SELECT * FROM retained_relationships",
    )?
    .execute([])?;
    conn.prepare_cached("DELETE FROM retained_relationships")?
        .execute([])?;
    // Only the session delete drops evidence the probes join; a presence is
    // not. Releasing on every changed delegated-thread rollout re-read every
    // evidence key once per thread.
    evidence_probe::note_deleted(conn, "sessions", sessions);
    Ok(())
}

pub(super) fn cleanup_codex_subagent_registration(
    conn: &Connection,
    session_id: &str,
) -> Result<()> {
    cleanup_subagent_registration(conn, "codex", session_id)
}

pub(super) fn codex_session_has_remote_presence(
    conn: &Connection,
    session_id: &str,
) -> Result<bool> {
    session_has_remote_presence(conn, "codex", session_id)
}

/// Whether a session is also known remotely. Local and remote evidence share
/// one `(source, session_id)` identity, so a local re-read must not delete
/// rows a remote observation put there.
pub(super) fn session_has_remote_presence(
    conn: &Connection,
    source: &str,
    session_id: &str,
) -> Result<bool> {
    cached_query_row(
        conn,
        "SELECT EXISTS(\
           SELECT 1 FROM session_presences \
           WHERE source = ? AND session_id = ? AND location = 'remote'\
         )",
        params![source, session_id],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

/// Remove prompt history that an older local sync incorrectly attributed to a
/// subagent. A remote presence makes the history canonical shared evidence, so
/// it must not be destroyed by a later local scan.
pub(super) fn cleanup_codex_subagent_history(conn: &Connection, session_id: &str) -> Result<()> {
    if !codex_session_has_remote_presence(conn, session_id)? {
        conn.prepare_cached("DELETE FROM history WHERE source = 'codex' AND session_id = ?")?
            .execute([session_id])?;
    }
    Ok(())
}
