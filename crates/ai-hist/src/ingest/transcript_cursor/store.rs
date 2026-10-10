use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension};

use super::{now_ms, TranscriptCursorState};

/// Which cursor row to read or write.
#[derive(Clone, Debug)]
pub(crate) enum CursorKey<'a> {
    /// The session's own primary transcript.
    Session {
        source: &'a str,
        session_id: &'a str,
        location: &'a str,
    },
    /// Any other transcript, addressed by the path it was read from.
    Locator { source: &'a str, locator: &'a str },
}

/// Read one transcript's resume state. A row that is missing, unparseable or
/// written by a different document version reads as "start from zero", which
/// is always safe: every insert on the ingest path is an idempotent upsert.
pub(crate) fn load_cursor(conn: &Connection, key: &CursorKey<'_>) -> Result<TranscriptCursorState> {
    Ok(load_cursor_with_document(conn, key)?.0)
}

/// [`load_cursor`], and the stored document it decoded, for a writer that
/// must only replace exactly what it read.
pub(super) fn load_cursor_with_document(
    conn: &Connection,
    key: &CursorKey<'_>,
) -> Result<(TranscriptCursorState, Option<String>)> {
    let raw: Option<Option<String>> = match key {
        CursorKey::Session {
            source,
            session_id,
            location,
        } => conn
            .prepare_cached(
                "SELECT parser_state_json FROM session_hydration_checkpoints \
                 WHERE source = ? AND session_id = ? AND location = ?",
            )?
            .query_row(params![source, session_id, location], |row| row.get(0))
            .optional()?,
        CursorKey::Locator { source, locator } => conn
            .prepare_cached(
                "SELECT parser_state_json FROM transcript_cursors \
                 WHERE source = ? AND locator = ?",
            )?
            .query_row(params![source, locator], |row| row.get(0))
            .optional()?,
    };
    let raw = raw.flatten();
    Ok((TranscriptCursorState::decode(raw.as_deref()), raw))
}

/// Write one transcript's resume state, with the three projections.
///
/// The session key updates a checkpoint row that hydration writes in full
/// elsewhere in the same transaction, so this only touches the cursor columns
/// and leaves an absent row absent — a cursor without a checkpoint would be a
/// resume position for evidence nothing recorded.
pub(crate) fn store_cursor(
    conn: &Connection,
    key: &CursorKey<'_>,
    state: &TranscriptCursorState,
) -> Result<()> {
    let encoded = state.encode();
    let offset = state.committed_offset();
    let prefix_hash = state.prefix_hash();
    let dev_ino = state.dev_ino();
    match key {
        CursorKey::Session {
            source,
            session_id,
            location,
        } => {
            conn.execute(
                "UPDATE session_hydration_checkpoints \
                 SET committed_offset = ?, prefix_hash = ?, dev_ino = ?, parser_state_json = ? \
                 WHERE source = ? AND session_id = ? AND location = ?",
                params![
                    offset,
                    prefix_hash,
                    dev_ino,
                    encoded,
                    source,
                    session_id,
                    location
                ],
            )?;
        }
        CursorKey::Locator { source, locator } => {
            conn.execute(
                "INSERT INTO transcript_cursors \
                 (source, locator, committed_offset, prefix_hash, dev_ino, parser_state_json, updated_ms) \
                 VALUES (?, ?, ?, ?, ?, ?, ?) \
                 ON CONFLICT(source, locator) DO UPDATE SET \
                   committed_offset = excluded.committed_offset, \
                   prefix_hash = excluded.prefix_hash, \
                   dev_ino = excluded.dev_ino, \
                   parser_state_json = excluded.parser_state_json, \
                   updated_ms = excluded.updated_ms",
                params![
                    source,
                    locator,
                    offset,
                    prefix_hash,
                    dev_ino,
                    encoded,
                    now_ms()
                ],
            )?;
        }
    }
    Ok(())
}
