//! What an OpenCode session's normalized evidence holds, as the sweep's skip
//! guard checks it.
//!
//! A read writes the session's catalog row, its events and markers when it
//! has messages, and its parent edge when OpenCode names a parent. The flags
//! recorded with a stamp say which of the optional ones the read produced;
//! the catalog row is always required. A session missing any of them is read
//! again, as an unread one would be.

use super::super::{session_events_exist, session_markers_exist};
use anyhow::Result;
use rusqlite::{Connection, OptionalExtension};
use std::collections::HashSet;

/// Events or markers.
const EVIDENCE: char = 'e';
/// The `opencode_parent_id` edge naming this session as a child.
const PARENT_EDGE: char = 'r';

/// The flags for what `session_id` holds now, just after its read.
pub(super) fn held(conn: &Connection, session_id: &str) -> Result<String> {
    let mut flags = String::new();
    if evidence_exists(conn, session_id)? {
        flags.push(EVIDENCE);
    }
    if parent_edge_exists(conn, session_id)? {
        flags.push(PARENT_EDGE);
    }
    Ok(flags)
}

/// What every OpenCode session holds, read once per sweep in one index scan
/// per table: on a store of tens of thousands of sessions that is a fraction
/// of the random probes [`holds`] would make, one session at a time.
///
/// Read before the sweep writes anything, and asked only about sessions the
/// sweep has not written yet.
#[derive(Debug, Default)]
pub(super) struct Holdings {
    catalog: HashSet<String>,
    evidence: HashSet<String>,
    parent_edges: HashSet<String>,
}

impl Holdings {
    pub(super) fn read(conn: &Connection) -> Result<Self> {
        Ok(Self {
            catalog: ids(
                conn,
                "SELECT session_id FROM sessions WHERE source = 'opencode'",
            )?,
            evidence: ids(
                conn,
                "SELECT session_id FROM session_events WHERE source = 'opencode' \
                 UNION SELECT session_id FROM session_markers WHERE source = 'opencode'",
            )?,
            parent_edges: ids(
                conn,
                "SELECT child_session_id FROM session_relationships \
                 WHERE source = 'opencode' AND evidence_kind = 'opencode_parent_id'",
            )?,
        })
    }

    /// [`holds`], from what was read.
    pub(super) fn hold(&self, session_id: &str, flags: &str) -> bool {
        self.catalog.contains(session_id)
            && (!flags.contains(EVIDENCE) || self.evidence.contains(session_id))
            && (!flags.contains(PARENT_EDGE) || self.parent_edges.contains(session_id))
    }
}

fn ids(conn: &Connection, sql: &str) -> Result<HashSet<String>> {
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map([], |row| row.get::<_, Option<String>>(0))?;
    let mut ids = HashSet::new();
    for id in rows {
        ids.extend(id?);
    }
    Ok(ids)
}

/// Whether `session_id` still holds its catalog row and everything `flags`
/// recorded.
pub(super) fn holds(conn: &Connection, session_id: &str, flags: &str) -> Result<bool> {
    Ok(catalog_exists(conn, session_id)?
        && (!flags.contains(EVIDENCE) || evidence_exists(conn, session_id)?)
        && (!flags.contains(PARENT_EDGE) || parent_edge_exists(conn, session_id)?))
}

fn evidence_exists(conn: &Connection, session_id: &str) -> Result<bool> {
    Ok(session_events_exist(conn, "opencode", session_id)?
        || session_markers_exist(conn, "opencode", session_id)?)
}

fn catalog_exists(conn: &Connection, session_id: &str) -> Result<bool> {
    exists(
        conn,
        "SELECT 1 FROM sessions WHERE session_id = ?1 AND source = 'opencode'",
        session_id,
    )
}

fn parent_edge_exists(conn: &Connection, session_id: &str) -> Result<bool> {
    exists(
        conn,
        "SELECT 1 FROM session_relationships \
         WHERE source = 'opencode' AND child_session_id = ?1 \
           AND evidence_kind = 'opencode_parent_id'",
        session_id,
    )
}

fn exists(conn: &Connection, sql: &str, session_id: &str) -> Result<bool> {
    Ok(conn
        .prepare_cached(sql)?
        .query_row([session_id], |_| Ok(()))
        .optional()?
        .is_some())
}
