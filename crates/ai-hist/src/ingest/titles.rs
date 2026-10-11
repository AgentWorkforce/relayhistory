//! The name each harness gives a session, and writing it to the catalog.
//!
//! Claude's title records and Codex's thread-name index need their own
//! readers ([`super::claude_title`], [`super::codex_thread_names`]); the
//! other harnesses keep the name on a row or file their reader already has
//! open, and only need it cleaned.

use super::{upsert_session_inner, ActivityWindow, SessionCatalogRow};
use anyhow::Result;
use rusqlite::{params, Connection};
use serde_json::Value;

/// Sets a catalog row's `title`, returning whether it changed.
///
/// `None` leaves the stored title alone: no harness clears a session's name,
/// so a read that found none has learned nothing (and a Claude fold resumed
/// from a cursor saved before titles were read has not seen the earlier
/// records). `IS NOT` keeps an unchanged title from rewriting, and so
/// restamping, the row.
pub(crate) fn set_session_title(
    conn: &Connection,
    source: &str,
    session_id: &str,
    title: Option<&str>,
) -> Result<bool> {
    let Some(title) = title else {
        return Ok(false);
    };
    let changed = conn
        .prepare_cached(
            "UPDATE sessions SET title = ?1 \
             WHERE source = ?2 AND session_id = ?3 AND title IS NOT ?1",
        )?
        .execute(params![title, source, session_id])?;
    Ok(changed > 0)
}

/// A full ingest's catalog write together with the title it read.
pub(crate) fn upsert_titled(
    conn: &Connection,
    row: &SessionCatalogRow<'_>,
    window: ActivityWindow,
    title: Option<&str>,
) -> Result<()> {
    upsert_session_inner(conn, row, window)?;
    set_session_title(conn, row.source, row.session_id, title)?;
    Ok(())
}

/// A harness's session name, trimmed and bounded like every catalog excerpt,
/// or `None` when it is blank.
pub(crate) fn non_blank_title(raw: Option<&str>) -> Option<String> {
    Some(crate::discover::excerpt(raw?)).filter(|title| !title.is_empty())
}

/// OpenCode's title for a session, or `None` while it is still the
/// placeholder OpenCode writes before its title model has named the session
/// (`New session - <ISO 8601>`, `Child session - <ISO 8601>`).
pub(crate) fn opencode_title(raw: Option<&str>) -> Option<String> {
    let title = non_blank_title(raw)?;
    let placeholder = ["New session - ", "Child session - "].iter().any(|prefix| {
        title.strip_prefix(prefix).is_some_and(|rest| {
            rest.len() >= 5
                && rest.as_bytes()[..4].iter().all(u8::is_ascii_digit)
                && rest.as_bytes()[4] == b'-'
        })
    });
    (!placeholder).then_some(title)
}

/// Grok's name for the session, from `summary.json`'s top-level `title`.
pub(crate) fn grok_title(summary: Option<&Value>) -> Option<String> {
    non_blank_title(summary?.get("title").and_then(Value::as_str))
}

#[cfg(test)]
mod tests {
    use super::opencode_title;

    #[test]
    fn an_opencode_placeholder_title_is_no_title() {
        assert_eq!(
            opencode_title(Some("New session - 2026-10-10T12:00:00.000Z")),
            None
        );
        assert_eq!(
            opencode_title(Some("Child session - 2026-10-10T12:00:00.000Z")),
            None
        );
        assert_eq!(opencode_title(Some("   ")), None);
        assert_eq!(opencode_title(None), None);
    }

    #[test]
    fn a_named_opencode_session_keeps_its_title() {
        assert_eq!(
            opencode_title(Some("  PR 54 merge and deploy  ")).as_deref(),
            Some("PR 54 merge and deploy")
        );
        // Only the placeholder's exact shape is dropped.
        assert_eq!(
            opencode_title(Some("New session - planning")).as_deref(),
            Some("New session - planning")
        );
    }
}
