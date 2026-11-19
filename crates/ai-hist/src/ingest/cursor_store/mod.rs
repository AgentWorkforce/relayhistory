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

mod read;
mod render;
mod sync;
#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;

pub(crate) use read::read_chat_store;
pub(in crate::ingest) use sync::sync_chat_stores_in;

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

pub(super) fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

pub(super) fn meta_json_path(path: &Path) -> Option<PathBuf> {
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

/// The text of a Cursor record file for the transcript parser: a
/// transcript's own text, or the transcript records a chat store renders to.
/// An unreadable source is an error, never an empty one.
pub(crate) fn read_records(path: &Path) -> Result<String> {
    if !is_chat_store(path) {
        return super::cold_read::read_string(path)
            .with_context(|| format!("read Cursor transcript {}", path.display()));
    }
    Ok(read_chat_store(path)?
        .map(|store| store.transcript)
        .unwrap_or_default())
}
