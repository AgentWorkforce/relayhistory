//! Cursor's own chat store:
//! `~/.cursor/chats/<workspace-hash>/<agentId>/store.db`.
//!
//! Every Cursor agent chat is kept here, whether or not the build also writes
//! an `agent-transcripts/<agentId>/<agentId>.jsonl` beside the project. A chat
//! with no transcript was invisible to RelayHistory: its only record was this
//! store. The transcript stays the primary source: a chat that has one is read
//! from it, exactly as before, and this store is read only for the chats that
//! have none.
//!
//! ## What the store holds
//!
//! Characterized from `cursor-agent` 2026.11.11 and 2026.11.17; Cursor
//! publishes no schema.
//!
//! * SQLite, `meta(key TEXT PRIMARY KEY, value TEXT)` and
//!   `blobs(id TEXT PRIMARY KEY, data BLOB)`, in WAL mode.
//! * `meta` key `0` is hex-encoded JSON:
//!   `{agentId, latestRootBlobId, name, mode, createdAt, …}`.
//! * Blobs are content-addressed (`id` is the SHA-256 of `data`). The root
//!   blob (`latestRootBlobId`) is a protobuf message whose repeated field 1
//!   lists the conversation's message blob ids in order; field 9 is the
//!   workspace `file://` URI, field 21 a message whose field 1 is the cwd,
//!   field 22 the client that wrote it (`cli` so far), field 26 the update
//!   time in epoch milliseconds.
//! * A message blob is JSON in the AI SDK shape,
//!   `{"role": "system"|"user"|"assistant"|"tool", "content": …}`, with
//!   `text`, `tool-call`, `tool-result` and `reasoning` blocks.
//! * Newer builds write `meta.json` beside the store:
//!   `{schemaVersion, createdAtMs, updatedAtMs, hasConversation, cwd}`.
//!
//! Each message is rendered as the record the transcript writes for it, and
//! the rendered records go through the transcript parser, so a chat reads the
//! same whichever file it came from. The system prompt and the `<user_info>`
//! environment preamble are omitted, as the transcript omits them. Unlike the
//! transcript, the store keeps tool results and tool-call ids, so those are
//! kept. Every rendered record carries its blob id as `message.id`.
//!
//! The meta also names a `blobEncryptionKey`. No store seen so far encrypted
//! its blobs; a blob that is not readable JSON is skipped and counted, never a
//! failure of the chat.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags, TransactionBehavior};
use serde_json::{json, Map, Value};

/// Where Cursor keeps its chat stores, relative to the home directory.
pub(crate) const CHATS_DIR: &str = ".cursor/chats";
/// The file name of one chat's store.
pub(crate) const STORE_FILE: &str = "store.db";
/// Where plain `sync` remembers which version of each chat store it indexed.
pub(crate) const CHAT_STORE_STATE_KEY: &str = "cursor_chat_stores_v1";

/// One chat, read from its store and rendered as transcript records.
#[derive(Debug, Default)]
pub(crate) struct ChatStore {
    pub session_id: String,
    pub cwd: Option<String>,
    pub created_ms: Option<i64>,
    pub updated_ms: Option<i64>,
    /// The client that wrote the chat (`cli` so far), when the root says.
    /// Read for tests and diagnostics; the catalog has no column for it yet.
    #[cfg_attr(not(test), allow(dead_code))]
    pub origin: Option<String>,
    /// Transcript-shaped JSON records, one per line, each ending in `\n`.
    pub transcript: String,
    pub records: usize,
    /// Message blobs that were missing or not readable JSON.
    pub skipped_blobs: usize,
}

/// Whether `path` is a chat store under some `.cursor/chats`.
pub(crate) fn is_chat_store(path: &Path) -> bool {
    if path.file_name().and_then(|name| name.to_str()) != Some(STORE_FILE) {
        return false;
    }
    let Some(workspace) = path.parent().and_then(Path::parent) else {
        return false;
    };
    let Some(chats) = workspace.parent() else {
        return false;
    };
    chats.file_name().and_then(|name| name.to_str()) == Some("chats")
        && chats
            .parent()
            .and_then(Path::file_name)
            .and_then(|name| name.to_str())
            == Some(".cursor")
}

/// The chat id a store is filed under: its directory name.
pub(crate) fn store_session_id(path: &Path) -> Option<&str> {
    path.parent()?.file_name()?.to_str()
}

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

fn meta_json_path(path: &Path) -> Option<PathBuf> {
    Some(path.parent()?.join("meta.json"))
}

/// A change stamp over everything a chat's content can be written to: the
/// store, its write-ahead log and `meta.json`. A WAL append leaves the store
/// file itself untouched until a checkpoint, so the store alone is not enough.
pub(crate) fn chat_store_stamp_and_modified(path: &Path) -> Result<(String, Option<i64>)> {
    let (store, mut modified) = crate::file_stamp_and_modified(path)?;
    let mut stamp = format!("cursor-store:{store}");
    let wal = sidecar(path, "-wal");
    let meta = meta_json_path(path);
    for (label, extra) in [("wal", Some(wal)), ("meta", meta)] {
        let Some(extra) = extra else { continue };
        if let Ok((extra_stamp, extra_modified)) = crate::file_stamp_and_modified(&extra) {
            stamp.push_str(&format!("|{label}:{extra_stamp}"));
            modified = modified.max(extra_modified);
        }
    }
    Ok((stamp, modified))
}

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

fn encode_hex(bytes: &[u8]) -> String {
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
struct RootFields {
    messages: Vec<String>,
    workspace_uri: Option<String>,
    cwd: Option<String>,
    origin: Option<String>,
    updated_ms: Option<i64>,
}

impl RootFields {
    fn decode(bytes: &[u8]) -> Self {
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

fn blocks_of(content: &Value) -> Vec<Value> {
    match content {
        Value::String(text) => vec![json!({"type": "text", "text": text})],
        Value::Array(blocks) => blocks.clone(),
        _ => Vec::new(),
    }
}

/// The transcript record Cursor writes for one stored message, or `None` for
/// a message the transcript leaves out.
fn transcript_record(blob_id: &str, message: &Value, preamble_open: &mut bool) -> Option<Value> {
    let role = message.get("role").and_then(Value::as_str)?;
    let content = blocks_of(message.get("content").unwrap_or(&Value::Null));
    let (role, blocks): (&str, Vec<Value>) = match role {
        "system" => return None,
        "user" => {
            // The environment preamble Cursor sends ahead of the first
            // prompt: not something the person typed, and not in the
            // transcript.
            let is_preamble = content.iter().all(|block| {
                block
                    .get("text")
                    .and_then(Value::as_str)
                    .is_some_and(|text| text.trim_start().starts_with("<user_info>"))
            });
            if *preamble_open && is_preamble && !content.is_empty() {
                return None;
            }
            *preamble_open = false;
            let blocks = content
                .iter()
                .filter_map(|block| match block.get("type").and_then(Value::as_str) {
                    Some("text") => Some(json!({"type": "text", "text": block.get("text")?})),
                    Some("image") | Some("file") => None,
                    _ => None,
                })
                .collect();
            ("user", blocks)
        }
        "assistant" => {
            *preamble_open = false;
            let blocks = content
                .iter()
                .filter_map(|block| match block.get("type").and_then(Value::as_str) {
                    Some("text") => Some(json!({"type": "text", "text": block.get("text")?})),
                    Some("reasoning") => Some(json!({
                        "type": "thinking",
                        "thinking": block.get("text").or_else(|| block.get("reasoning"))?,
                    })),
                    Some("tool-call") => {
                        let mut tool = Map::new();
                        tool.insert("type".into(), json!("tool_use"));
                        if let Some(id) = block.get("toolCallId") {
                            tool.insert("id".into(), id.clone());
                        }
                        tool.insert("name".into(), block.get("toolName")?.clone());
                        tool.insert(
                            "input".into(),
                            block
                                .get("args")
                                .or_else(|| block.get("input"))
                                .cloned()
                                .unwrap_or(Value::Null),
                        );
                        Some(Value::Object(tool))
                    }
                    _ => None,
                })
                .collect();
            ("assistant", blocks)
        }
        // Cursor writes a tool result back as its own `tool` message; the
        // transcript shape puts it in a user-role record, as tool results are.
        "tool" => {
            let blocks = content
                .iter()
                .filter_map(|block| match block.get("type").and_then(Value::as_str) {
                    Some("tool-result") => {
                        let mut result = Map::new();
                        result.insert("type".into(), json!("tool_result"));
                        result.insert("tool_use_id".into(), block.get("toolCallId")?.clone());
                        let output = block
                            .get("result")
                            .or_else(|| block.get("output"))
                            .cloned()
                            .unwrap_or(Value::Null);
                        result.insert("content".into(), output);
                        if let Some(is_error) = block.get("isError").and_then(Value::as_bool) {
                            result.insert("is_error".into(), json!(is_error));
                        }
                        Some(Value::Object(result))
                    }
                    _ => None,
                })
                .collect();
            ("user", blocks)
        }
        _ => return None,
    };
    if blocks.is_empty() {
        return None;
    }
    Some(json!({
        "role": role,
        "message": {"id": format!("cursor-blob:{blob_id}"), "content": blocks},
    }))
}

/// The chat ids that have an `agent-transcripts` transcript under
/// `projects`. Those chats are read from the transcript, never the store.
pub(crate) fn transcript_session_ids(projects: &Path) -> Result<HashSet<String>> {
    let mut ids = HashSet::new();
    for project in crate::sorted_dirs(projects)? {
        let transcripts = project.join("agent-transcripts");
        if !transcripts.is_dir() {
            continue;
        }
        for session in crate::sorted_dirs(&transcripts)? {
            let Some(id) = session.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if session.join(format!("{id}.jsonl")).is_file() {
                ids.insert(id.to_string());
            }
        }
    }
    Ok(ids)
}

/// Every chat store under `chats` (or under the changed paths in `scope`)
/// whose chat has no transcript, with its chat id.
pub(crate) fn chat_stores(
    home: &Path,
    scope: Option<&crate::ingest::path_scope::PathScope>,
) -> Result<Vec<(PathBuf, String)>> {
    let chats = home.join(CHATS_DIR);
    if !chats.is_dir() {
        return Ok(Vec::new());
    }
    let walk = crate::ingest::path_scope::PathScope::walk_of(scope, &chats);
    let partial = match walk {
        crate::ingest::path_scope::RootWalk::Skip => return Ok(Vec::new()),
        crate::ingest::path_scope::RootWalk::Partial(scope) => Some(scope),
        crate::ingest::path_scope::RootWalk::Full => None,
    };
    let transcripts = transcript_session_ids(&home.join(".cursor/projects"))?;
    let mut out = Vec::new();
    for workspace in crate::sorted_dirs(&chats)? {
        if partial.is_some_and(|scope| !scope.mentions(&workspace)) {
            continue;
        }
        for chat in crate::sorted_dirs(&workspace)? {
            if partial.is_some_and(|scope| !scope.mentions(&chat)) {
                continue;
            }
            let Some(id) = chat.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if transcripts.contains(id) {
                continue;
            }
            let store = chat.join(STORE_FILE);
            if store.is_file() {
                out.push((store, id.to_string()));
            }
        }
    }
    Ok(out)
}

/// Index every chat that exists only in Cursor's chat store. A store is
/// re-read whole when its stamp changes, and its session rebuilt from it.
pub(super) fn sync_chat_stores_in(
    conn: &Connection,
    state: &mut Map<String, Value>,
    home: &Path,
    coverage: &mut super::SweepCoverage,
    paths: Option<&crate::ingest::path_scope::PathScope>,
) -> Result<usize> {
    // `sync_note!` names the parent's quiet flag.
    use super::{AtomicOrdering, SYNC_QUIET};
    let stores = chat_stores(home, paths)?;
    let mut seen = state
        .get(CHAT_STORE_STATE_KEY)
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    // A full walk saw every store there is: a stamp for one it did not see
    // (deleted, or its chat now has a transcript) is dropped, so a store that
    // comes back is read again rather than skipped on a stale stamp.
    if crate::ingest::path_scope::PathScope::walk_is_full(paths, &home.join(CHATS_DIR)) {
        let present: HashSet<String> = stores
            .iter()
            .map(|(path, _)| path.to_string_lossy().into_owned())
            .collect();
        let missing: Vec<String> = seen
            .keys()
            .filter(|key| !present.contains(*key))
            .cloned()
            .collect();
        if !missing.is_empty() {
            for key in &missing {
                seen.remove(key);
            }
            super::forget_unobserved_paths(state, &mut Map::new(), CHAT_STORE_STATE_KEY, missing);
            state.insert(
                CHAT_STORE_STATE_KEY.to_string(),
                Value::Object(seen.clone()),
            );
        }
    }
    if stores.is_empty() {
        return Ok(0);
    }
    // Provider I/O before the destination writer lock, as the transcript
    // pass does.
    let mut prepared = Vec::new();
    let mut errors = 0usize;
    for (path, _) in super::capture_files("cursor", stores) {
        super::check_capture_cancelled()?;
        let key = path.to_string_lossy().into_owned();
        let stamp = match chat_store_stamp_and_modified(&path) {
            Ok((stamp, _)) => stamp,
            Err(_) => continue,
        };
        if seen.get(&key).and_then(Value::as_str) == Some(stamp.as_str()) {
            continue;
        }
        match read_chat_store(&path) {
            Ok(Some(store)) => prepared.push((path, key, stamp, store)),
            Ok(None) => {}
            Err(error) => {
                if error.is::<super::CaptureCancelled>() {
                    return Err(error);
                }
                sync_note!(
                    "  [cursor] skipping chat store {}: {error:#}",
                    path.display()
                );
                errors += 1;
                coverage.note_unread();
            }
        }
    }
    if prepared.is_empty() {
        return Ok(0);
    }
    let tx = conn.unchecked_transaction()?;
    let mut inserted = 0;
    for (path, key, stamp, store) in &prepared {
        super::check_capture_cancelled()?;
        let fallback_ms = store.updated_ms.or(store.created_ms).unwrap_or(0);
        super::clear_cursor_session_evidence(&tx, &store.session_id)?;
        let outcome = super::ingest_cursor_transcript_text(
            &tx,
            &store.transcript,
            &store.session_id,
            store.cwd.as_deref(),
            fallback_ms,
            0,
            u64::MAX,
        )
        .with_context(|| format!("index Cursor chat store {}", path.display()))?;
        inserted += outcome.prompts_inserted;
        super::upsert_session_rebuilt(
            &tx,
            &super::SessionCatalogRow {
                session_id: &store.session_id,
                source: "cursor",
                cwd: store.cwd.as_deref(),
                git_branch: None,
                first_ts: outcome.first_ts_ms.unwrap_or(fallback_ms),
                last_ts: outcome.last_ts_ms.unwrap_or(fallback_ms),
                last_assistant_text: outcome.last_assistant_text.as_deref(),
                raw_path: Some(&path.to_string_lossy()),
            },
        )?;
        seen.insert(key.clone(), Value::String(stamp.clone()));
    }
    tx.commit().context("commit Cursor chat store sync")?;
    state.insert(CHAT_STORE_STATE_KEY.to_string(), Value::Object(seen));
    let suffix = if errors > 0 {
        format!(" ({errors} errors)")
    } else {
        String::new()
    };
    sync_note!(
        "  [cursor] {} chat store(s), {inserted} prompts{suffix}",
        prepared.len()
    );
    Ok(inserted)
}

#[cfg(test)]
pub(crate) mod test_support {
    //! Synthetic chat stores in Cursor's layout, for tests.
    use super::*;
    use sha2::{Digest, Sha256};

    fn put_varint(out: &mut Vec<u8>, mut value: u64) {
        loop {
            let byte = (value & 0x7f) as u8;
            value >>= 7;
            if value == 0 {
                out.push(byte);
                return;
            }
            out.push(byte | 0x80);
        }
    }

    fn put_bytes(out: &mut Vec<u8>, field: u64, bytes: &[u8]) {
        put_varint(out, field << 3 | 2);
        put_varint(out, bytes.len() as u64);
        out.extend_from_slice(bytes);
    }

    /// Writes `<home>/.cursor/chats/<workspace>/<id>/store.db` holding
    /// `messages` in order, and returns its path.
    pub(crate) fn write_store(
        home: &Path,
        workspace: &str,
        id: &str,
        cwd: &str,
        messages: &[Value],
        updated_ms: u64,
    ) -> PathBuf {
        let dir = home.join(CHATS_DIR).join(workspace).join(id);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(STORE_FILE);
        let _ = std::fs::remove_file(&path);
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE blobs (id TEXT PRIMARY KEY, data BLOB);
             CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);",
        )
        .unwrap();
        let mut root = Vec::new();
        let put = |data: &[u8]| -> [u8; 32] {
            let digest: [u8; 32] = Sha256::digest(data).into();
            conn.execute(
                "INSERT OR IGNORE INTO blobs (id, data) VALUES (?1, ?2)",
                rusqlite::params![encode_hex(&digest), data],
            )
            .unwrap();
            digest
        };
        for message in messages {
            let digest = put(message.to_string().as_bytes());
            put_bytes(&mut root, 1, &digest);
        }
        put_bytes(&mut root, 9, format!("file://{cwd}").as_bytes());
        let mut nested = Vec::new();
        put_bytes(&mut nested, 1, cwd.as_bytes());
        put_bytes(&mut root, 21, &nested);
        put_bytes(&mut root, 22, b"cli");
        put_varint(&mut root, 26 << 3);
        put_varint(&mut root, updated_ms);
        let root_id = encode_hex(&put(&root));
        let meta = json!({
            "agentId": id,
            "latestRootBlobId": root_id,
            "name": "New Agent",
            "mode": "default",
            "createdAt": updated_ms.saturating_sub(1_000),
        });
        conn.execute(
            "INSERT INTO meta (key, value) VALUES ('0', ?1)",
            [encode_hex(meta.to_string().as_bytes())],
        )
        .unwrap();
        // Close cleanly, as Cursor does when a chat goes idle: no -wal or
        // -shm is left behind.
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        drop(conn);
        let _ = std::fs::remove_file(sidecar(&path, "-wal"));
        let _ = std::fs::remove_file(sidecar(&path, "-shm"));
        path
    }

    /// A short chat in the shape Cursor stores: system prompt, environment
    /// preamble, a framed prompt, a tool call, its result and a reply.
    pub(crate) fn tool_chat(prompt: &str) -> Vec<Value> {
        vec![
            json!({"role": "system", "content": "You are an AI coding assistant."}),
            json!({"role": "user", "content": "<user_info>\nOS Version: test\n</user_info>"}),
            json!({"role": "user", "content": [{"type": "text", "text": format!(
                "<timestamp>Wednesday, Nov 18, 2026, 11:52 PM (UTC+1)</timestamp>\n<user_query>\n{prompt}\n</user_query>"
            )}], "providerOptions": {"cursor": {"requestId": "r-1"}}}),
            json!({"role": "assistant", "content": [{"type": "tool-call", "toolCallId": "call-1", "toolName": "Read", "args": {"path": "/work/app/note.txt"}}]}),
            json!({"role": "tool", "content": [{"type": "tool-result", "toolCallId": "call-1", "toolName": "Read", "result": "hello\n"}]}),
            json!({"role": "assistant", "content": [{"type": "text", "text": "note.txt says hello"}], "id": "1"}),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;

    #[test]
    fn a_store_renders_as_the_transcript_cursor_would_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_store(
            dir.path(),
            "ws",
            "chat-1",
            "/work/app",
            &tool_chat("read note.txt"),
            1_795_042_344_825,
        );
        let store = read_chat_store(&path).unwrap().unwrap();
        assert_eq!(store.session_id, "chat-1");
        assert_eq!(store.cwd.as_deref(), Some("/work/app"));
        assert_eq!(store.origin.as_deref(), Some("cli"));
        assert_eq!(store.updated_ms, Some(1_795_042_344_825));
        assert_eq!(store.skipped_blobs, 0);
        let records: Vec<Value> = store
            .transcript
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        // System prompt and preamble are left out; the four turns remain.
        assert_eq!(records.len(), 4);
        assert_eq!(records[0]["role"], "user");
        assert!(records[0]["message"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("<user_query>"));
        assert_eq!(records[1]["message"]["content"][0]["type"], "tool_use");
        assert_eq!(records[1]["message"]["content"][0]["id"], "call-1");
        assert_eq!(records[1]["message"]["content"][0]["name"], "Read");
        assert_eq!(records[2]["role"], "user");
        assert_eq!(records[2]["message"]["content"][0]["type"], "tool_result");
        assert_eq!(records[2]["message"]["content"][0]["tool_use_id"], "call-1");
        assert_eq!(
            records[3]["message"]["content"][0]["text"],
            "note.txt says hello"
        );
        assert!(records[3]["message"]["id"]
            .as_str()
            .unwrap()
            .starts_with("cursor-blob:"));
    }

    #[test]
    fn an_idle_store_with_no_wal_is_read_without_writing_beside_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_store(dir.path(), "ws", "chat-1", "/w", &tool_chat("hi"), 5);
        assert!(!sidecar(&path, "-shm").exists());
        read_chat_store(&path).unwrap().unwrap();
        let mut left: Vec<String> = std::fs::read_dir(path.parent().unwrap())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        left.sort();
        assert_eq!(left, vec!["store.db".to_string()]);
    }

    #[test]
    fn meta_json_without_a_conversation_is_no_chat() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_store(dir.path(), "ws", "chat-1", "/w", &tool_chat("hi"), 5);
        std::fs::write(
            path.parent().unwrap().join("meta.json"),
            r#"{"schemaVersion":1,"hasConversation":false,"cwd":"/w"}"#,
        )
        .unwrap();
        assert!(read_chat_store(&path).unwrap().is_none());
    }

    #[test]
    fn meta_json_supplies_cwd_and_times() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_store(dir.path(), "ws", "chat-1", "/root/cwd", &tool_chat("hi"), 5);
        std::fs::write(
            path.parent().unwrap().join("meta.json"),
            r#"{"schemaVersion":1,"createdAtMs":10,"updatedAtMs":20,"hasConversation":true,"cwd":"/meta/cwd"}"#,
        )
        .unwrap();
        let store = read_chat_store(&path).unwrap().unwrap();
        assert_eq!(store.cwd.as_deref(), Some("/meta/cwd"));
        assert_eq!(store.created_ms, Some(10));
        assert_eq!(store.updated_ms, Some(20));
    }

    #[test]
    fn an_unreadable_blob_is_skipped_and_counted() {
        let dir = tempfile::tempdir().unwrap();
        let mut messages = tool_chat("hi");
        messages.push(json!("placeholder"));
        let path = write_store(dir.path(), "ws", "chat-1", "/w", &messages, 5);
        // Replace the last message blob with bytes that are not JSON, the
        // way an encrypted blob would read.
        let conn = Connection::open(&path).unwrap();
        let id: String = conn
            .query_row(
                "SELECT id FROM blobs WHERE data = ?1",
                [json!("placeholder").to_string().as_bytes()],
                |row| row.get(0),
            )
            .unwrap();
        conn.execute(
            "UPDATE blobs SET data = ?1 WHERE id = ?2",
            rusqlite::params![vec![0x9fu8, 0x00, 0x13], id],
        )
        .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        drop(conn);
        let _ = std::fs::remove_file(sidecar(&path, "-wal"));
        let _ = std::fs::remove_file(sidecar(&path, "-shm"));
        let store = read_chat_store(&path).unwrap().unwrap();
        assert_eq!(store.skipped_blobs, 1);
        assert_eq!(store.records, 4);
    }

    #[test]
    fn a_chat_with_a_transcript_is_not_offered_from_its_store() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        write_store(home, "ws", "has-transcript", "/w", &tool_chat("a"), 5);
        let only_store = write_store(home, "ws", "store-only", "/w", &tool_chat("b"), 5);
        let transcript = home.join(".cursor/projects/w/agent-transcripts/has-transcript");
        std::fs::create_dir_all(&transcript).unwrap();
        std::fs::write(transcript.join("has-transcript.jsonl"), "{}\n").unwrap();
        let stores = chat_stores(home, None).unwrap();
        assert_eq!(stores, vec![(only_store, "store-only".to_string())]);
    }

    #[test]
    fn a_wal_append_changes_the_stamp() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_store(dir.path(), "ws", "chat-1", "/w", &tool_chat("hi"), 5);
        let (before, _) = chat_store_stamp_and_modified(&path).unwrap();
        std::fs::write(sidecar(&path, "-wal"), b"frames").unwrap();
        let (after, _) = chat_store_stamp_and_modified(&path).unwrap();
        assert_ne!(before, after);
    }

    fn prompts(conn: &Connection, session: &str) -> Vec<String> {
        conn.prepare("SELECT prompt FROM history WHERE source = 'cursor' AND session_id = ?1 ORDER BY timestamp_ms, prompt")
            .unwrap()
            .query_map([session], |row| row.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    #[test]
    fn sync_indexes_a_store_only_chat_once_and_rebuilds_it_when_it_changes() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let conn = crate::open_db(&home.join("history.db")).unwrap();
        let mut state = Map::new();
        let mut coverage = super::super::SweepCoverage::default();
        write_store(home, "ws", "chat-1", "/w", &tool_chat("first prompt"), 5);
        assert_eq!(
            sync_chat_stores_in(&conn, &mut state, home, &mut coverage, None).unwrap(),
            1
        );
        assert_eq!(prompts(&conn, "chat-1"), vec!["first prompt".to_string()]);
        // Unchanged: skipped on its stamp.
        assert_eq!(
            sync_chat_stores_in(&conn, &mut state, home, &mut coverage, None).unwrap(),
            0
        );
        // Cursor appends a turn: the chat is rebuilt, not appended to twice.
        let mut longer = tool_chat("first prompt");
        longer.push(json!({"role": "user", "content": [{"type": "text", "text":
            "<timestamp>Thursday, Nov 19, 2026, 12:10 AM (UTC+1)</timestamp>\n<user_query>\nsecond prompt\n</user_query>"}]}));
        longer.push(json!({"role": "assistant", "content": [{"type": "text", "text": "done"}]}));
        std::thread::sleep(std::time::Duration::from_millis(20));
        write_store(home, "ws", "chat-1", "/w", &longer, 6);
        sync_chat_stores_in(&conn, &mut state, home, &mut coverage, None).unwrap();
        assert_eq!(
            prompts(&conn, "chat-1"),
            vec!["first prompt".to_string(), "second prompt".to_string()]
        );
        let calls = crate::session_tool_calls(&conn, "chat-1", Some("cursor")).unwrap();
        assert_eq!(calls.len(), 1);
        let (raw, last): (String, String) = conn
            .query_row(
                "SELECT raw_path, last_assistant_text FROM sessions WHERE source='cursor' AND session_id='chat-1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert!(raw.ends_with("chat-1/store.db"));
        assert_eq!(last, "done");
    }

    #[test]
    fn sync_leaves_a_chat_with_a_transcript_to_the_transcript() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let conn = crate::open_db(&home.join("history.db")).unwrap();
        write_store(home, "ws", "chat-1", "/w", &tool_chat("from the store"), 5);
        let transcript = home.join(".cursor/projects/w/agent-transcripts/chat-1");
        std::fs::create_dir_all(&transcript).unwrap();
        std::fs::write(
            transcript.join("chat-1.jsonl"),
            "{\"role\":\"user\",\"message\":{\"content\":\"from the transcript\"}}\n",
        )
        .unwrap();
        let mut state = Map::new();
        let mut coverage = super::super::SweepCoverage::default();
        assert_eq!(
            sync_chat_stores_in(&conn, &mut state, home, &mut coverage, None).unwrap(),
            0
        );
        assert!(prompts(&conn, "chat-1").is_empty());
    }

    #[test]
    fn a_store_that_gains_a_transcript_drops_its_stamp() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let conn = crate::open_db(&home.join("history.db")).unwrap();
        let store = write_store(home, "ws", "chat-1", "/w", &tool_chat("a"), 5);
        let mut state = Map::new();
        let mut coverage = super::super::SweepCoverage::default();
        sync_chat_stores_in(&conn, &mut state, home, &mut coverage, None).unwrap();
        let key = store.to_string_lossy().into_owned();
        assert!(state[CHAT_STORE_STATE_KEY].get(&key).is_some());
        let transcript = home.join(".cursor/projects/w/agent-transcripts/chat-1");
        std::fs::create_dir_all(&transcript).unwrap();
        std::fs::write(transcript.join("chat-1.jsonl"), "{}\n").unwrap();
        sync_chat_stores_in(&conn, &mut state, home, &mut coverage, None).unwrap();
        assert!(state[CHAT_STORE_STATE_KEY].get(&key).is_none());
        assert!(state["forgotten_paths"][CHAT_STORE_STATE_KEY]
            .as_array()
            .unwrap()
            .contains(&json!(key)));
    }

    #[test]
    fn a_path_scoped_tick_reads_only_the_store_that_changed() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let changed = write_store(home, "ws", "chat-1", "/w", &tool_chat("a"), 5);
        write_store(home, "ws", "chat-2", "/w", &tool_chat("b"), 5);
        let scope = crate::ingest::path_scope::PathScope::from_paths([changed.clone()]).unwrap();
        let stores = chat_stores(home, Some(&scope)).unwrap();
        assert_eq!(stores, vec![(changed, "chat-1".to_string())]);
        let elsewhere = crate::ingest::path_scope::PathScope::from_paths([
            home.join(".claude/projects/x.jsonl")
        ])
        .unwrap();
        assert!(chat_stores(home, Some(&elsewhere)).unwrap().is_empty());
    }

    #[test]
    fn only_a_store_under_cursor_chats_is_a_chat_store() {
        assert!(is_chat_store(Path::new("/h/.cursor/chats/ws/id/store.db")));
        assert!(!is_chat_store(Path::new(
            "/h/.cursor/projects/ws/id/store.db"
        )));
        assert!(!is_chat_store(Path::new("/h/.cursor/chats/ws/id/other.db")));
    }

    #[test]
    fn a_truncated_root_keeps_what_decoded() {
        let mut root = Vec::new();
        root.extend_from_slice(&[0x0a, 0x20]);
        root.extend_from_slice(&[7u8; 32]);
        root.extend_from_slice(&[0x0a, 0x20, 1, 2]);
        let fields = RootFields::decode(&root);
        assert_eq!(fields.messages.len(), 1);
    }
}
