//! Opening a chat store safely and decoding its meta, root and blobs.

use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags, TransactionBehavior};
use serde_json::Value;

use super::render::transcript_record;
use super::{chat_store_stamp_and_modified, meta_json_path, sidecar, store_session_id, ChatStore};

/// Open a provider-owned store for reading without writing to it.
///
/// While Cursor has a chat open its store has a `-wal` and `-shm`, and a
/// read-only connection reads through them like any other provider database.
/// A chat Cursor has closed has neither: every frame is in the main file. A
/// read-only open would create both beside it, writing into Cursor's chat
/// directory, so that store is read as immutable instead, which touches
/// nothing and sees exactly what a normal reader would. The caller re-stamps
/// after the read and drops it if a writer appeared meanwhile.
fn open_store(path: &Path) -> Result<Connection> {
    let conn = if sidecar(path, "-wal").exists() {
        Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
    } else {
        let uri = format!("file:{}?mode=ro&immutable=1", uri_path(path));
        Connection::open_with_flags(
            uri,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
        )
    };
    conn.with_context(|| format!("open Cursor chat store {}", path.display()))
}

/// `path` as a SQLite `file:` URI path: every byte outside the unreserved
/// set is percent-encoded, so a non-ASCII path names the same file.
fn uri_path(path: &Path) -> String {
    let text = path.to_string_lossy();
    // SQLite expects forward slashes and a leading `/` before a drive letter.
    let text = if cfg!(windows) {
        let slashed = text.replace('\\', "/");
        if slashed.starts_with('/') {
            slashed
        } else {
            format!("/{slashed}")
        }
    } else {
        text.into_owned()
    };
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || b"-._~/:".contains(&byte) {
            out.push(char::from(byte));
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// How many messages a shallow read renders from each end of a chat.
pub(crate) const PREVIEW_MESSAGES: usize = 16;

/// Read one chat whole. `Ok(None)` when the store holds no conversation yet.
pub(crate) fn read_chat_store(path: &Path) -> Result<Option<ChatStore>> {
    read_chat_store_bounded(path, None)
}

/// Read only the first and last [`PREVIEW_MESSAGES`] messages, for a shallow
/// catalog read: its head and tail are all a preview looks at.
pub(crate) fn read_chat_store_preview(path: &Path) -> Result<Option<ChatStore>> {
    read_chat_store_bounded(path, Some(PREVIEW_MESSAGES))
}

fn read_sidecar_meta(path: &Path) -> Option<Value> {
    meta_json_path(path)
        .and_then(|meta| std::fs::read(meta).ok())
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
}

fn read_chat_store_bounded(path: &Path, ends: Option<usize>) -> Result<Option<ChatStore>> {
    let session_id = store_session_id(path)
        .with_context(|| format!("Cursor chat store {} has no chat id", path.display()))?
        .to_string();
    // Stamped before anything is read, `meta.json` included, so a sidecar
    // replaced during the read fails the closing comparison.
    let (before, _) = chat_store_stamp_and_modified(path)?;
    let sidecar_meta = read_sidecar_meta(path);
    let no_conversation = sidecar_meta
        .as_ref()
        .and_then(|meta| meta.get("hasConversation"))
        .and_then(Value::as_bool)
        == Some(false);
    if no_conversation {
        return Ok(None);
    }
    let mut conn = open_store(path)?;
    conn.busy_timeout(std::time::Duration::from_secs(2))?;
    let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
    let Some((meta, root_fields)) = read_root(&tx, path, &session_id)? else {
        return Ok(None);
    };
    let mut store = ChatStore {
        session_id,
        origin: root_fields.origin.clone(),
        ..ChatStore::default()
    };
    let positions = selected(root_fields.messages.len(), ends);
    render_messages(&tx, &root_fields.messages, &positions, &mut store)?;
    tx.commit()?;
    drop(conn);
    // A writer that started while the store was read can have moved it on;
    // a mixture of two versions is not a chat. The next pass reads it again.
    let (after, _) = chat_store_stamp_and_modified(path)?;
    anyhow::ensure!(
        before == after,
        "Cursor chat store {} changed while it was read",
        path.display()
    );
    apply_context(&mut store, sidecar_meta.as_ref(), &meta, root_fields);
    Ok(Some(store))
}

/// The meta row and the decoded root blob it names, checked against the
/// directory the store is filed under.
fn read_root(
    conn: &Connection,
    path: &Path,
    session_id: &str,
) -> Result<Option<(Value, RootFields)>> {
    let Some(meta) = read_meta(conn)? else {
        return Ok(None);
    };
    if let Some(agent) = meta.get("agentId").and_then(Value::as_str) {
        anyhow::ensure!(
            agent == session_id,
            "Cursor chat store {} names chat {agent}, not its directory {session_id}",
            path.display()
        );
    }
    let Some(root_id) = meta.get("latestRootBlobId").and_then(Value::as_str) else {
        return Ok(None);
    };
    let Some(root) = read_blob(conn, root_id)? else {
        return Ok(None);
    };
    let fields = RootFields::decode(&root);
    Ok(Some((meta, fields)))
}

/// The message positions to render: all of them, or each end of the chat.
fn selected(len: usize, ends: Option<usize>) -> Vec<usize> {
    match ends {
        Some(ends) if len > ends * 2 => (0..ends).chain(len - ends..len).collect(),
        _ => (0..len).collect(),
    }
}

/// The id each message carries: its blob id, with the occurrence number when
/// the root lists the same blob more than once, so repeated identical
/// messages stay separate messages. One pass over the whole list, so a
/// preview numbers occurrences exactly as a full read does.
fn occurrence_ids(messages: &[String]) -> Vec<String> {
    let mut seen = std::collections::HashMap::<&str, usize>::new();
    messages
        .iter()
        .map(|id| {
            let count = seen.entry(id.as_str()).or_insert(0);
            *count += 1;
            if *count == 1 {
                id.clone()
            } else {
                format!("{id}#{count}")
            }
        })
        .collect()
}

fn render_messages(
    conn: &Connection,
    messages: &[String],
    positions: &[usize],
    store: &mut ChatStore,
) -> Result<()> {
    let mut preamble_open = true;
    let ids = occurrence_ids(messages);
    for &position in positions {
        let Some(data) = read_blob(conn, &messages[position])? else {
            store.skipped_blobs += 1;
            continue;
        };
        let Ok(message) = serde_json::from_slice::<Value>(&data) else {
            store.skipped_blobs += 1;
            continue;
        };
        let Some(record) = transcript_record(&ids[position], &message, &mut preamble_open) else {
            continue;
        };
        store.transcript.push_str(&record.to_string());
        store.transcript.push('\n');
        store.records += 1;
    }
    Ok(())
}

/// The chat's cwd and times: `meta.json` first, then the root and the meta row.
fn apply_context(
    store: &mut ChatStore,
    sidecar_meta: Option<&Value>,
    meta: &Value,
    root: RootFields,
) {
    let sidecar = |key: &str| sidecar_meta.and_then(|meta| meta.get(key));
    store.cwd = sidecar("cwd")
        .and_then(Value::as_str)
        .filter(|cwd| !cwd.is_empty())
        .map(str::to_string)
        .or(root.cwd)
        .or_else(|| root.workspace_uri.as_deref().and_then(file_uri_path));
    store.created_ms = sidecar("createdAtMs")
        .and_then(Value::as_i64)
        .or_else(|| meta.get("createdAt").and_then(Value::as_i64));
    store.updated_ms = sidecar("updatedAtMs")
        .and_then(Value::as_i64)
        .or(root.updated_ms)
        .or(store.created_ms);
}

fn read_meta(conn: &Connection) -> Result<Option<Value>> {
    let hex: Option<String> =
        match conn.query_row("SELECT value FROM meta WHERE key = '0'", [], |row| {
            row.get(0)
        }) {
            Ok(value) => Some(value),
            Err(rusqlite::Error::QueryReturnedNoRows) => None,
            Err(error) => return Err(error.into()),
        };
    let Some(hex) = hex else { return Ok(None) };
    let bytes = decode_hex(hex.trim()).or_else(|| Some(hex.clone().into_bytes()));
    Ok(bytes.and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok()))
}

fn read_blob(conn: &Connection, id: &str) -> Result<Option<Vec<u8>>> {
    match conn.query_row("SELECT data FROM blobs WHERE id = ?1", [id], |row| {
        row.get::<_, Vec<u8>>(0)
    }) {
        Ok(data) => Ok(Some(data)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn decode_hex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) || !text.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&text[at..at + 2], 16).ok())
        .collect()
}

pub(super) fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn hex_digit(byte: u8) -> Option<u8> {
    char::from(byte)
        .to_digit(16)
        .and_then(|digit| u8::try_from(digit).ok())
}

/// The path a `file://` URI names, percent escapes decoded; a malformed
/// escape is kept as written.
pub(super) fn file_uri_path(uri: &str) -> Option<String> {
    let bytes = uri.strip_prefix("file://")?.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut at = 0;
    while at < bytes.len() {
        let escaped = (bytes[at] == b'%')
            .then(|| Some(hex_digit(*bytes.get(at + 1)?)? << 4 | hex_digit(*bytes.get(at + 2)?)?))
            .flatten();
        match escaped {
            Some(byte) => {
                out.push(byte);
                at += 3;
            }
            None => {
                out.push(bytes[at]);
                at += 1;
            }
        }
    }
    String::from_utf8(out).ok().filter(|path| !path.is_empty())
}

/// The fields of a root blob this reader uses.
#[derive(Debug, Default)]
pub(super) struct RootFields {
    pub(super) messages: Vec<String>,
    workspace_uri: Option<String>,
    cwd: Option<String>,
    origin: Option<String>,
    updated_ms: Option<i64>,
}

impl RootFields {
    pub(super) fn decode(bytes: &[u8]) -> Self {
        let mut fields = Self::default();
        for (field, value) in protobuf_fields(bytes) {
            match (field, value) {
                (1, Field::Bytes(id)) if id.len() == 32 => fields.messages.push(encode_hex(id)),
                (9, Field::Bytes(uri)) => {
                    fields.workspace_uri = std::str::from_utf8(uri).ok().map(str::to_string)
                }
                (21, Field::Bytes(nested)) => {
                    fields.cwd = protobuf_fields(nested)
                        .into_iter()
                        .find_map(|(field, value)| match (field, value) {
                            (1, Field::Bytes(cwd)) => std::str::from_utf8(cwd)
                                .ok()
                                .filter(|cwd| !cwd.is_empty())
                                .map(str::to_string),
                            _ => None,
                        })
                }
                (22, Field::Bytes(origin)) => {
                    fields.origin = std::str::from_utf8(origin).ok().map(str::to_string)
                }
                (26, Field::Varint(ms)) => fields.updated_ms = i64::try_from(ms).ok(),
                _ => {}
            }
        }
        fields
    }
}

enum Field<'a> {
    Varint(u64),
    Bytes(&'a [u8]),
    Fixed,
}

fn varint(bytes: &[u8], at: &mut usize) -> Option<u64> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *bytes.get(*at)?;
        *at += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some(value);
        }
    }
    None
}

/// The top-level fields of a protobuf message, stopping at the first byte
/// that does not decode: what was read before it is kept.
fn protobuf_fields(bytes: &[u8]) -> Vec<(u64, Field<'_>)> {
    let mut out = Vec::new();
    let mut at = 0;
    while at < bytes.len() {
        let Some(key) = varint(bytes, &mut at) else {
            break;
        };
        let field = key >> 3;
        let value = match key & 7 {
            0 => match varint(bytes, &mut at) {
                Some(value) => Field::Varint(value),
                None => break,
            },
            1 => {
                at += 8;
                Field::Fixed
            }
            2 => {
                let Some(len) = varint(bytes, &mut at).and_then(|len| usize::try_from(len).ok())
                else {
                    break;
                };
                let Some(slice) = at.checked_add(len).and_then(|end| bytes.get(at..end)) else {
                    break;
                };
                at += len;
                Field::Bytes(slice)
            }
            5 => {
                at += 4;
                Field::Fixed
            }
            _ => break,
        };
        if at > bytes.len() {
            break;
        }
        out.push((field, value));
    }
    out
}
