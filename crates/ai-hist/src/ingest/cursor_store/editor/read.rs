//! Reading composers out of the editor database without writing to it.

use std::path::Path;

use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::render::bubble_records;
use super::{file_stamp, Composer, ComposerHead};

/// How many bubbles a shallow read renders from each end of a conversation.
pub(crate) const PREVIEW_BUBBLES: usize = 16;

/// The half-open key range `[prefix, prefix with its last `:` bumped to `;`)`:
/// every key that starts with `prefix`, through the key index.
fn key_range(prefix: &str) -> (String, String) {
    let upper = match prefix.strip_suffix(':') {
        Some(stem) => format!("{stem};"),
        None => format!("{prefix}\u{10FFFF}"),
    };
    (prefix.to_string(), upper)
}

/// A read of the database, consistent for its whole length.
///
/// While Cursor runs, the database has a `-wal` and a read-only connection
/// reads through it inside one transaction, which is a consistent snapshot.
/// With Cursor closed it is opened immutable, so nothing is created beside it;
/// the file stamp is then re-checked after the read, and a read that raced a
/// writer that started meanwhile is refused.
fn with_snapshot<T>(db: &Path, read: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
    let immutable = !crate::ingest::cursor_store::sidecar(db, "-wal").exists();
    let before = file_stamp(db)?;
    let mut conn = crate::ingest::cursor_store::open_store(db)?;
    conn.busy_timeout(std::time::Duration::from_secs(2))?;
    let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
    let out = read(&tx)?;
    tx.commit()?;
    drop(conn);
    if immutable {
        anyhow::ensure!(
            file_stamp(db)? == before,
            "Cursor editor database {} changed while it was read",
            db.display()
        );
    }
    Ok(out)
}

/// Rows, the highest rowid and the rowid total under one key prefix, from
/// the key index alone. A rewritten row is a replace with a new rowid, so
/// any change to the rows moves at least one of the three.
fn rows_stamp(conn: &Connection, prefix: &str) -> Result<(i64, i64, i64)> {
    let (low, high) = key_range(prefix);
    Ok(conn
        .prepare_cached(
            "SELECT count(*), COALESCE(max(rowid), 0), CAST(total(rowid) AS INTEGER) \
         FROM cursorDiskKV WHERE key >= ?1 AND key < ?2",
        )?
        .query_row([low, high], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?)
}

/// A composer's change stamp, and whether it has any conversation at all.
fn stamp_of(conn: &Connection, id: &str, rowid: i64, value: &[u8]) -> Result<(String, bool)> {
    let digest = Sha256::digest(value);
    let hash: String = digest[..8].iter().map(|b| format!("{b:02x}")).collect();
    let bubbles = rows_stamp(conn, &format!("bubbleId:{id}:"))?;
    let headers = rows_stamp(conn, &format!("composerConvHeaders:{id}:"))?;
    let inline = serde_json::from_slice::<Value>(value)
        .ok()
        .and_then(|data| data.get("conversation")?.as_array().map(Vec::len))
        .unwrap_or(0);
    let stamp = format!(
        "cursor-editor:{rowid}:{hash}:b{}.{}.{}:h{}.{}.{}",
        bubbles.0, bubbles.1, bubbles.2, headers.0, headers.1, headers.2
    );
    Ok((stamp, bubbles.0 > 0 || inline > 0))
}

fn millis(value: Option<&Value>) -> Option<i64> {
    let value = value?;
    value
        .as_i64()
        .or_else(|| value.as_f64().map(|ms| ms as i64))
        .or_else(|| value.as_str().and_then(crate::parse_iso_ms))
        .filter(|ms| *ms > 0)
}

/// Whether the database has Cursor's key-value table; builds before it kept
/// no conversations here.
fn has_store(conn: &Connection) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'cursorDiskKV')",
        [],
        |row| row.get(0),
    )?)
}

/// Every composer that has a conversation, with its stamp and recency.
pub(crate) fn composer_heads(db: &Path) -> Result<Vec<ComposerHead>> {
    with_snapshot(db, |conn| {
        if !has_store(conn)? {
            return Ok(Vec::new());
        }
        let (low, high) = key_range("composerData:");
        let mut stmt = conn.prepare(
            "SELECT rowid, key, CAST(value AS BLOB) FROM cursorDiskKV \
             WHERE key >= ?1 AND key < ?2 ORDER BY key",
        )?;
        let rows = stmt
            .query_map([low, high], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<Vec<u8>>>(2)?.unwrap_or_default(),
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut heads = Vec::new();
        for (rowid, key, value) in rows {
            crate::ingest::check_capture_cancelled()?;
            let id = &key["composerData:".len()..];
            if id.is_empty() {
                continue;
            }
            let (stamp, has_conversation) = stamp_of(conn, id, rowid, &value)?;
            if !has_conversation {
                continue;
            }
            let data = serde_json::from_slice::<Value>(&value).unwrap_or(Value::Null);
            let recency_ms = millis(data.get("lastUpdatedAt")).or(millis(data.get("createdAt")));
            heads.push(ComposerHead {
                session_id: id.to_string(),
                stamp,
                recency_ms,
            });
        }
        Ok(heads)
    })
}

/// One composer's stamp, or `None` when the database no longer holds it or
/// it has no conversation.
pub(crate) fn composer_stamp(db: &Path, id: &str) -> Result<Option<String>> {
    with_snapshot(db, |conn| {
        let Some((rowid, value)) = composer_row(conn, id)? else {
            return Ok(None);
        };
        let (stamp, has_conversation) = stamp_of(conn, id, rowid, &value)?;
        Ok(has_conversation.then_some(stamp))
    })
}

fn composer_row(conn: &Connection, id: &str) -> Result<Option<(i64, Vec<u8>)>> {
    if !has_store(conn)? {
        return Ok(None);
    }
    Ok(conn
        .prepare_cached("SELECT rowid, CAST(value AS BLOB) FROM cursorDiskKV WHERE key = ?1")?
        .query_row([format!("composerData:{id}")], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<Vec<u8>>>(1)?.unwrap_or_default(),
            ))
        })
        .optional()?)
}

/// Read one composer whole. `Ok(None)` when it is gone or has no conversation.
pub(crate) fn read_composer(db: &Path, id: &str) -> Result<Option<Composer>> {
    read_bounded(db, id, None)
}

/// Read only the first and last [`PREVIEW_BUBBLES`] bubbles, for a shallow
/// catalog read.
pub(crate) fn read_composer_preview(db: &Path, id: &str) -> Result<Option<Composer>> {
    read_bounded(db, id, Some(PREVIEW_BUBBLES))
}

fn read_bounded(db: &Path, id: &str, ends: Option<usize>) -> Result<Option<Composer>> {
    let composer = with_snapshot(db, |conn| {
        let Some((_, value)) = composer_row(conn, id)? else {
            return Ok(None);
        };
        let data = serde_json::from_slice::<Value>(&value).unwrap_or(Value::Null);
        let mut composer = Composer {
            session_id: id.to_string(),
            title: crate::ingest::titles::non_blank_title(data.get("name").and_then(Value::as_str)),
            created_ms: millis(data.get("createdAt")),
            ..Composer::default()
        };
        composer.updated_ms = millis(data.get("lastUpdatedAt")).or(composer.created_ms);
        let conversation = conversation(conn, id, &data)?;
        if conversation.is_empty() {
            return Ok(None);
        }
        for position in selected(conversation.len(), ends) {
            let bubble = match &conversation[position] {
                Bubble::Inline(bubble) => Some(bubble.clone()),
                Bubble::Row(bubble_id) => bubble_row(conn, id, bubble_id)?,
            };
            let Some(bubble) = bubble.filter(Value::is_object) else {
                composer.skipped_bubbles += 1;
                continue;
            };
            for record in bubble_records(&bubble) {
                composer.transcript.push_str(&record.to_string());
                composer.transcript.push('\n');
                composer.records += 1;
            }
        }
        composer.cwd = workspace_folder(db, &data);
        Ok(Some(composer))
    })?;
    Ok(composer)
}

/// One message of the conversation: a bubble row by id, or a whole bubble
/// the oldest builds keep inline in `composerData.conversation`.
enum Bubble {
    Row(String),
    Inline(Value),
}

/// The conversation in order. The header chunks are the order Cursor shows;
/// without them the inline header list, then inline bubbles, and last every
/// bubble row the composer has, by creation time.
fn conversation(conn: &Connection, id: &str, data: &Value) -> Result<Vec<Bubble>> {
    let headers = header_chunks(conn, id)?;
    if !headers.is_empty() {
        return Ok(headers.into_iter().map(Bubble::Row).collect());
    }
    let inline_headers = header_ids(data.get("fullConversationHeadersOnly"));
    if !inline_headers.is_empty() {
        return Ok(inline_headers.into_iter().map(Bubble::Row).collect());
    }
    if let Some(inline) = data.get("conversation").and_then(Value::as_array) {
        if !inline.is_empty() {
            return Ok(inline.iter().cloned().map(Bubble::Inline).collect());
        }
    }
    all_bubbles(conn, id)
}

fn header_ids(headers: Option<&Value>) -> Vec<String> {
    headers
        .and_then(Value::as_array)
        .map(|headers| {
            headers
                .iter()
                .filter_map(|header| header.get("bubbleId")?.as_str())
                .filter(|id| !id.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// The bubble ids in `composerConvHeaders:<id>:<chunk>`, chunks in key order.
fn header_chunks(conn: &Connection, id: &str) -> Result<Vec<String>> {
    let (low, high) = key_range(&format!("composerConvHeaders:{id}:"));
    let mut stmt = conn.prepare_cached(
        "SELECT CAST(value AS BLOB) FROM cursorDiskKV WHERE key >= ?1 AND key < ?2 ORDER BY key",
    )?;
    let chunks = stmt
        .query_map([low, high], |row| row.get::<_, Option<Vec<u8>>>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(chunks
        .into_iter()
        .flatten()
        .filter_map(|chunk| serde_json::from_slice::<Value>(&chunk).ok())
        .flat_map(|chunk| header_ids(Some(&chunk)))
        .collect())
}

/// Every bubble row of the composer, oldest first; rowid breaks a tie.
fn all_bubbles(conn: &Connection, id: &str) -> Result<Vec<Bubble>> {
    let (low, high) = key_range(&format!("bubbleId:{id}:"));
    let mut stmt = conn.prepare_cached(
        "SELECT rowid, CAST(value AS BLOB) FROM cursorDiskKV WHERE key >= ?1 AND key < ?2",
    )?;
    let mut rows = stmt
        .query_map([low, high], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<Vec<u8>>>(1)?.unwrap_or_default(),
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .map(|(rowid, value)| {
            let bubble = serde_json::from_slice::<Value>(&value).unwrap_or(Value::Null);
            let at = millis(bubble.get("createdAt")).unwrap_or(i64::MAX);
            (at, rowid, bubble)
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|(at, rowid, _)| (*at, *rowid));
    Ok(rows
        .into_iter()
        .map(|(_, _, bubble)| Bubble::Inline(bubble))
        .collect())
}

fn bubble_row(conn: &Connection, id: &str, bubble_id: &str) -> Result<Option<Value>> {
    let value = conn
        .prepare_cached("SELECT CAST(value AS BLOB) FROM cursorDiskKV WHERE key = ?1")?
        .query_row([format!("bubbleId:{id}:{bubble_id}")], |row| {
            row.get::<_, Option<Vec<u8>>>(0)
        })
        .optional()?
        .flatten();
    Ok(value.and_then(|value| serde_json::from_slice::<Value>(&value).ok()))
}

/// The positions to render: all of them, or each end of the conversation.
fn selected(len: usize, ends: Option<usize>) -> Vec<usize> {
    match ends {
        Some(ends) if len > ends * 2 => (0..ends).chain(len - ends..len).collect(),
        _ => (0..len).collect(),
    }
}

/// The composer's workspace folder: the one it records, else the folder the
/// workspace it names was opened on. A multi-root workspace names no folder.
fn workspace_folder(db: &Path, data: &Value) -> Option<String> {
    let identifier = data.get("workspaceIdentifier")?;
    let recorded = identifier
        .get("uri")
        .and_then(|uri| uri.get("fsPath"))
        .and_then(Value::as_str)
        .filter(|path| !path.is_empty())
        .map(str::to_string);
    if recorded.is_some() {
        return recorded;
    }
    let workspace = identifier.get("id")?.as_str()?;
    if workspace.is_empty() || workspace.contains(['/', '\\', '.']) {
        return None;
    }
    let file = db
        .parent()?
        .parent()?
        .join("workspaceStorage")
        .join(workspace)
        .join("workspace.json");
    let json = serde_json::from_slice::<Value>(&std::fs::read(file).ok()?).ok()?;
    crate::ingest::cursor_store::file_uri_path(json.get("folder")?.as_str()?)
}
