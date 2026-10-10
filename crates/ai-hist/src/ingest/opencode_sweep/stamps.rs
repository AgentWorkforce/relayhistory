//! The stamps an OpenCode sweep compares: a store's files, and each SQLite
//! session's rows.

use super::super::{opencode, SyncStateStamp};
use super::AMBIGUITY_MS;
use anyhow::Result;
use rusqlite::Connection;
use std::collections::HashMap;
use std::path::Path;

pub(super) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

/// The store stamp, or `None` when the files cannot vouch for their content:
/// unreadable metadata (or no inode to read, off Unix), or an mtime within
/// [`AMBIGUITY_MS`] of now.
pub(super) fn store_file_stamp(path: &Path, generation: &str) -> Option<String> {
    let db = SyncStateStamp::at(path)?;
    let mut wal_path = path.as_os_str().to_os_string();
    wal_path.push("-wal");
    let wal = SyncStateStamp::at(Path::new(&wal_path));
    let newest_ns = db.mtime_ns.max(wal.map_or(0, |wal| wal.mtime_ns));
    let newest_ms = i64::try_from(newest_ns / 1_000_000).unwrap_or(i64::MAX);
    if now_ms().saturating_sub(newest_ms) <= AMBIGUITY_MS {
        return None;
    }
    Some(format!("{generation}:{db:?}:{wal:?}"))
}

/// One SQLite session's stamp inputs: its row's fields and its `message` and
/// `part` aggregates, plus the newest `time_updated` among them.
pub(super) struct SessionStampRow {
    pub(super) session_id: String,
    pub(super) parts: String,
    pub(super) newest_ms: i64,
}

/// `(count, newest time_updated, sum of row hashes)` per session.
type TableAggregate = HashMap<String, (i64, Option<i64>, i64)>;

/// Every session's stamp inputs, in one pass over each table.
///
/// Each row's hash covers its id and [`row_version`], so a rewritten row
/// moves the sum even when the count and the newest timestamp stay where
/// they were. The columns read are the ones the loaders read; a schema
/// without one stamps without it.
pub(super) fn sqlite_session_stamps(src: &Connection) -> Result<Vec<SessionStampRow>> {
    let session_columns = opencode::table_columns(src, "session")?;
    if !session_columns.contains("id") {
        return Ok(Vec::new());
    }
    let column = |name| opencode::optional_column(&session_columns, name);
    let messages = message_aggregate(src)?;
    let parts = part_aggregate(src)?;
    let sql = format!(
        "SELECT id, {}, {}, {}, {} FROM session WHERE id IS NOT NULL AND id <> ''",
        column("parent_id"),
        column("directory"),
        column("time_created"),
        column("time_updated"),
    );
    let mut stmt = src.prepare(&sql)?;
    let rows = stmt.query_map([], |row| {
        let fields: [rusqlite::types::Value; 4] =
            [row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?];
        Ok((row.get::<_, String>(0)?, fields))
    })?;
    let mut stamps = Vec::new();
    for row in rows {
        let (session_id, fields) = row?;
        let message = messages.get(&session_id).copied().unwrap_or_default();
        let part = parts.get(&session_id).copied().unwrap_or_default();
        let row_ms = [&fields[2], &fields[3]]
            .into_iter()
            .filter_map(|value| match value {
                rusqlite::types::Value::Integer(ms) => Some(*ms),
                _ => None,
            })
            .max();
        let newest_ms = [row_ms, message.1, part.1]
            .into_iter()
            .flatten()
            .max()
            .unwrap_or_default();
        stamps.push(SessionStampRow {
            parts: format!("{fields:?}|m{message:?}|p{part:?}"),
            session_id,
            newest_ms,
        });
    }
    Ok(stamps)
}

fn message_aggregate(src: &Connection) -> Result<TableAggregate> {
    let columns = opencode::table_columns(src, "message")?;
    if !["id", "data", "session_id"]
        .iter()
        .all(|name| columns.contains(*name))
    {
        return Ok(TableAggregate::new());
    }
    let updated = opencode::optional_column(&columns, "time_updated");
    let version = row_version(&columns, "");
    aggregate(
        src,
        &format!(
            "SELECT session_id, COUNT(*), MAX({updated}), \
             SUM(ai_hist_fnv(id || '|' || COALESCE(CAST({updated} AS TEXT), '') \
                 || '|' || {version})) \
             FROM message GROUP BY session_id"
        ),
    )
}

/// Grouped by the part's own `session_id` when it has one, otherwise by its
/// message's session -- the same parts either loader reads.
fn part_aggregate(src: &Connection) -> Result<TableAggregate> {
    let columns = opencode::table_columns(src, "part")?;
    if !["id", "data", "message_id"]
        .iter()
        .all(|name| columns.contains(*name))
    {
        return Ok(TableAggregate::new());
    }
    let version = row_version(&columns, "p.");
    let updated = match columns.contains("time_updated") {
        true => "p.time_updated",
        false => "NULL",
    };
    let (session, from) = if columns.contains("session_id") {
        ("p.session_id", "part p")
    } else {
        let message = opencode::table_columns(src, "message")?;
        if !message.contains("id") || !message.contains("session_id") {
            return Ok(TableAggregate::new());
        }
        (
            "m.session_id",
            "part p JOIN message m ON m.id = p.message_id",
        )
    };
    aggregate(
        src,
        &format!(
            "SELECT {session}, COUNT(*), MAX({updated}), \
             SUM(ai_hist_fnv(p.id || '|' || p.message_id || '|' \
                 || COALESCE(CAST({updated} AS TEXT), '') \
                 || '|' || {version})) \
             FROM {from} GROUP BY {session}"
        ),
    )
}

/// What identifies one version of a row's payload, as SQL over the row
/// (columns prefixed by `alias`).
///
/// OpenCode stamps `time_updated` on every write of a row, so with the
/// column a rewrite moves `time_updated` or, within the same millisecond,
/// lands within [`AMBIGUITY_MS`] of the read that saw the first version,
/// which leaves that session unstamped. Its payload length, read from the
/// record header, stands in for the rest. A schema without the column has
/// only the payload itself to show a rewrite, so the payload is hashed --
/// on a store of a gigabyte of payloads, seconds of reading on every sweep
/// of a store that moved, which a schema with the column does not pay.
fn row_version(columns: &std::collections::BTreeSet<String>, alias: &str) -> String {
    if columns.contains("time_updated") {
        format!("COALESCE(octet_length({alias}data), -1)")
    } else {
        format!("COALESCE({alias}data, '')")
    }
}

fn aggregate(src: &Connection, sql: &str) -> Result<TableAggregate> {
    let mut stmt = src.prepare(sql)?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, Option<String>>(0)?,
            (row.get(1)?, row.get(2)?, row.get(3)?),
        ))
    })?;
    let mut out = TableAggregate::new();
    for row in rows {
        let (session_id, aggregate) = row?;
        if let Some(session_id) = session_id {
            out.insert(session_id, aggregate);
        }
    }
    Ok(out)
}
