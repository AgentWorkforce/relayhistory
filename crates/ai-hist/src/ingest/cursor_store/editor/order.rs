//! Which bubbles make up a composer's conversation, and in what order.

use anyhow::Result;
use rusqlite::{Connection, OptionalExtension};
use serde_json::Value;

use super::read::key_range;

/// The most header chunks a declared log is followed to. Cursor writes 256
/// headers a chunk, so this is far past any real conversation; a larger
/// declaration is damaged and read as incomplete.
const MAX_DECLARED_CHUNKS: u64 = 65_536;

/// One message of the conversation: a bubble row by id, a bubble row by the
/// rowid the headerless ordering chose, or a whole bubble the oldest builds
/// keep inline in `composerData.conversation`. `Deferred` stands for a
/// headerless conversation a preview leaves to the full read.
pub(super) enum Bubble {
    Row(String),
    Rowid(i64),
    Inline(Value),
    Deferred,
}

/// The conversation in order, and whether it could be read to the extent the
/// composer declares.
#[derive(Default)]
pub(super) struct Conversation {
    pub(super) bubbles: Vec<Bubble>,
    pub(super) incomplete: bool,
}

impl Conversation {
    fn rows(ids: Vec<String>, incomplete: bool) -> Self {
        Self {
            bubbles: ids.into_iter().map(Bubble::Row).collect(),
            incomplete,
        }
    }
}

/// The conversation in order. A composer whose `headerLog` declares its
/// chunks is read from exactly those chunks; one without a log from every
/// chunk it has; without chunks the inline header list, then inline bubbles,
/// and last every bubble row the composer has, by creation time. A preview
/// defers that last, headerless layout to the full read rather than order
/// every payload to pick its two ends.
pub(super) fn conversation(
    conn: &Connection,
    id: &str,
    data: &Value,
    preview: bool,
) -> Result<Conversation> {
    match declared_headers(conn, id, data.get("headerLog"))? {
        Some(declared) if !declared.bubbles.is_empty() || declared.incomplete => {
            return Ok(declared)
        }
        Some(_) => {}
        None => {
            let legacy = legacy_header_chunks(conn, id)?;
            if !legacy.bubbles.is_empty() || legacy.incomplete {
                return Ok(legacy);
            }
        }
    }
    let inline_headers = header_ids(data.get("fullConversationHeadersOnly"));
    if !inline_headers.is_empty() {
        return Ok(Conversation::rows(inline_headers, false));
    }
    if let Some(inline) = data.get("conversation").and_then(Value::as_array) {
        if !inline.is_empty() {
            return Ok(Conversation {
                bubbles: inline.iter().cloned().map(Bubble::Inline).collect(),
                incomplete: false,
            });
        }
    }
    if preview {
        return Ok(Conversation {
            bubbles: deferred(conn, id)?,
            incomplete: false,
        });
    }
    headerless(conn, id)
}

/// The header ids in one header array, and whether every entry named one.
fn header_entries(headers: &Value) -> Option<(Vec<String>, bool)> {
    let headers = headers.as_array()?;
    let ids: Vec<String> = headers
        .iter()
        .filter_map(|header| header.get("bubbleId")?.as_str())
        .filter(|id| !id.is_empty())
        .map(str::to_string)
        .collect();
    let whole = ids.len() == headers.len();
    Some((ids, whole))
}

fn header_ids(headers: Option<&Value>) -> Vec<String> {
    headers
        .and_then(header_entries)
        .map(|(ids, _)| ids)
        .unwrap_or_default()
}

/// The conversation a recognized `headerLog` (`chunkCount` and `count`)
/// declares: chunks `000000` up to `chunkCount`, no others, and the first
/// `count` headers in them. A chunk beyond the log is an older generation and
/// is never read. A declared chunk that is missing or not a header array, or
/// fewer headers than `count`, makes the read incomplete. `None` when the
/// composer has no log this reader recognizes.
fn declared_headers(
    conn: &Connection,
    id: &str,
    log: Option<&Value>,
) -> Result<Option<Conversation>> {
    let Some(log) = log else { return Ok(None) };
    let (Some(chunks), Some(count)) = (
        log.get("chunkCount").and_then(Value::as_u64),
        log.get("count").and_then(Value::as_u64),
    ) else {
        return Ok(None);
    };
    let mut ids = Vec::new();
    let mut incomplete = chunks > MAX_DECLARED_CHUNKS;
    for chunk in 0..chunks.min(MAX_DECLARED_CHUNKS) {
        let parsed = chunk_value(conn, &format!("composerConvHeaders:{id}:{chunk:06}"))?
            .as_ref()
            .and_then(header_entries);
        match parsed {
            Some((chunk_ids, whole)) => {
                incomplete |= !whole;
                ids.extend(chunk_ids);
            }
            None => incomplete = true,
        }
    }
    let declared = usize::try_from(count).unwrap_or(usize::MAX);
    incomplete |= ids.len() < declared;
    ids.truncate(declared);
    Ok(Some(Conversation::rows(ids, incomplete)))
}

fn chunk_value(conn: &Connection, key: &str) -> Result<Option<Value>> {
    let value = conn
        .prepare_cached("SELECT CAST(value AS BLOB) FROM cursorDiskKV WHERE key = ?1")?
        .query_row([key], |row| row.get::<_, Option<Vec<u8>>>(0))
        .optional()?
        .flatten();
    Ok(value.and_then(|value| serde_json::from_slice::<Value>(&value).ok()))
}

/// Every `composerConvHeaders:<id>:<chunk>` in key order, for a composer
/// with no header log; a chunk that is not a header array is incomplete.
fn legacy_header_chunks(conn: &Connection, id: &str) -> Result<Conversation> {
    let (low, high) = key_range(&format!("composerConvHeaders:{id}:"));
    let mut stmt = conn.prepare_cached(
        "SELECT CAST(value AS BLOB) FROM cursorDiskKV WHERE key >= ?1 AND key < ?2 ORDER BY key",
    )?;
    let chunks = stmt
        .query_map([low, high], |row| row.get::<_, Option<Vec<u8>>>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut ids = Vec::new();
    let mut incomplete = false;
    for chunk in chunks {
        let parsed = chunk
            .and_then(|chunk| serde_json::from_slice::<Value>(&chunk).ok())
            .as_ref()
            .and_then(header_entries);
        match parsed {
            Some((chunk_ids, whole)) => {
                incomplete |= !whole;
                ids.extend(chunk_ids);
            }
            None => incomplete = true,
        }
    }
    Ok(Conversation::rows(ids, incomplete))
}

/// A bubble's creation time in epoch milliseconds, computed by SQLite from
/// the payload so no payload is brought into memory to order the rows: ISO
/// 8601 text as Cursor writes it now, or a number as older builds did.
const CREATED_MS: &str = "CASE WHEN json_valid(CAST(value AS TEXT)) THEN \
     CASE json_type(CAST(value AS TEXT), '$.createdAt') \
       WHEN 'text' THEN CAST(round((julianday(json_extract(CAST(value AS TEXT), '$.createdAt')) \
         - 2440587.5) * 86400000.0) AS INTEGER) \
       WHEN 'integer' THEN json_extract(CAST(value AS TEXT), '$.createdAt') \
       WHEN 'real' THEN CAST(json_extract(CAST(value AS TEXT), '$.createdAt') AS INTEGER) \
     END END";

/// Every bubble row of the composer, oldest first, rowid breaking a tie and
/// an undated bubble last. Only rowids and times leave SQLite; the payloads
/// are read one at a time as they are rendered.
fn headerless(conn: &Connection, id: &str) -> Result<Conversation> {
    let (low, high) = key_range(&format!("bubbleId:{id}:"));
    let mut stmt = conn.prepare_cached(&format!(
        "SELECT rowid FROM (SELECT rowid, {CREATED_MS} AS at FROM cursorDiskKV \
           WHERE key >= ?1 AND key < ?2) \
         ORDER BY at IS NULL, at, rowid"
    ))?;
    let rowids = stmt
        .query_map([low, high], |row| row.get::<_, i64>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(Conversation {
        bubbles: rowids.into_iter().map(Bubble::Rowid).collect(),
        incomplete: false,
    })
}

/// A preview of a headerless composer: whether it has any bubble at all,
/// from the key index, and nothing to render. The full read orders it.
fn deferred(conn: &Connection, id: &str) -> Result<Vec<Bubble>> {
    let (low, high) = key_range(&format!("bubbleId:{id}:"));
    let any: bool = conn
        .prepare_cached("SELECT EXISTS(SELECT 1 FROM cursorDiskKV WHERE key >= ?1 AND key < ?2)")?
        .query_row([low, high], |row| row.get(0))?;
    Ok(if any {
        vec![Bubble::Deferred]
    } else {
        Vec::new()
    })
}
