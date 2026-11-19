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

/// `path` percent-encoded for a SQLite `file:` URI.
fn uri_path(path: &Path) -> String {
    let mut out = String::new();
    for byte in path.to_string_lossy().bytes() {
        match byte {
            b'%' | b'?' | b'#' | b' ' => out.push_str(&format!("%{byte:02X}")),
            _ => out.push(byte as char),
        }
    }
    out
}

/// Read one chat. `Ok(None)` when the store holds no conversation yet.
pub(crate) fn read_chat_store(path: &Path) -> Result<Option<ChatStore>> {
    let session_id = store_session_id(path)
        .with_context(|| format!("Cursor chat store {} has no chat id", path.display()))?
        .to_string();
    let sidecar_meta = meta_json_path(path)
        .and_then(|meta| std::fs::read(meta).ok())
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
    if sidecar_meta
        .as_ref()
        .and_then(|meta| meta.get("hasConversation"))
        .and_then(Value::as_bool)
        == Some(false)
    {
        return Ok(None);
    }
    let (before, _) = chat_store_stamp_and_modified(path)?;
    let mut conn = open_store(path)?;
    conn.busy_timeout(std::time::Duration::from_secs(2))?;
    let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
    let meta = read_meta(&tx)?;
    let Some(meta) = meta else {
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
    let Some(root) = read_blob(&tx, root_id)? else {
        return Ok(None);
    };
    let root_fields = RootFields::decode(&root);
    let mut store = ChatStore {
        session_id,
        origin: root_fields.origin.clone(),
        ..ChatStore::default()
    };
    let mut preamble_open = true;
    for id in &root_fields.messages {
        let Some(data) = read_blob(&tx, id)? else {
            store.skipped_blobs += 1;
            continue;
        };
        let Ok(message) = serde_json::from_slice::<Value>(&data) else {
            store.skipped_blobs += 1;
            continue;
        };
        let Some(record) = transcript_record(id, &message, &mut preamble_open) else {
            continue;
        };
        store.transcript.push_str(&record.to_string());
        store.transcript.push('\n');
        store.records += 1;
    }
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
    store.cwd = sidecar_meta
        .as_ref()
        .and_then(|meta| meta.get("cwd"))
        .and_then(Value::as_str)
        .filter(|cwd| !cwd.is_empty())
        .map(str::to_string)
        .or(root_fields.cwd)
        .or_else(|| root_fields.workspace_uri.as_deref().and_then(file_uri_path));
    store.created_ms = sidecar_meta
        .as_ref()
        .and_then(|meta| meta.get("createdAtMs"))
        .and_then(Value::as_i64)
        .or_else(|| meta.get("createdAt").and_then(Value::as_i64));
    store.updated_ms = sidecar_meta
        .as_ref()
        .and_then(|meta| meta.get("updatedAtMs"))
        .and_then(Value::as_i64)
        .or(root_fields.updated_ms)
        .or(store.created_ms);
    Ok(Some(store))
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

fn file_uri_path(uri: &str) -> Option<String> {
    let rest = uri.strip_prefix("file://")?;
    let bytes = rest.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut at = 0;
    while at < bytes.len() {
        if bytes[at] == b'%' && at + 2 < bytes.len() {
            if let Some(byte) = decode_hex(&rest[at + 1..at + 3]).and_then(|b| b.first().copied()) {
                out.push(byte);
                at += 3;
                continue;
            }
        }
        out.push(bytes[at]);
        at += 1;
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
