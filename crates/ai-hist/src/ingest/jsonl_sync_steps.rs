//! Steps of an incremental JSONL sync: settling an unchanged cursor, indexing
//! one history line, and choosing the cursor a Cursor transcript scan
//! checkpoints.

use super::*;

/// Whether a JSONL file opened at its end still matches its saved cursor.
/// When it does, a cursor proven settled replaces the saved one.
pub(super) fn settle_unchanged_jsonl(
    state: &mut Map<String, Value>,
    name: &str,
    source: &CompleteJsonlReader,
) -> Result<bool> {
    let opened_cursor = source.cursor.to_value()?;
    if !same_opened_cursor(state.get(name), &opened_cursor) {
        return Ok(false);
    }
    if let Some(settled) = source.settled_cursor() {
        state.insert(name.to_string(), settled.to_value()?);
    }
    Ok(true)
}

/// Index one history line: the rows it inserted, and `1` in the second slot
/// when it did not parse. Blank lines count as neither.
pub(super) fn insert_history_line(
    conn: &Connection,
    parser: fn(&str) -> Result<Option<HistoryEntry>>,
    line: &str,
) -> Result<(usize, usize)> {
    if line.trim().is_empty() {
        return Ok((0, 0));
    }
    match parser(line) {
        Ok(Some(entry)) => Ok((insert_history(conn, &entry)?, 0)),
        Ok(None) => Ok((0, 0)),
        Err(_) => Ok((0, 1)),
    }
}

/// The cursor a scan checkpoints: the one it committed or, when nothing
/// advanced, the opened cursor once it is unchanged — proven by the
/// whole-prefix hash `open` just took — and old enough to settle on, so the
/// next sweep can stop at a stat.
pub(super) fn cursor_scan_checkpoint(
    source: &CompleteJsonlReader,
    committed: Option<FileCursor>,
    advanced: bool,
) -> Result<Option<Value>> {
    match committed {
        Some(cursor) => Ok(Some(cursor.to_value()?)),
        None if !advanced => source
            .settled_cursor()
            .map(|cursor| cursor.to_value())
            .transpose(),
        None => Ok(None),
    }
}
