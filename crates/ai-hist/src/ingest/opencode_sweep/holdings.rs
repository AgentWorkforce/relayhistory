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
