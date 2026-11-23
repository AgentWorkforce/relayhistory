//! The Cursor editor's own conversation store:
//! `<Cursor user dir>/globalStorage/state.vscdb`.
//!
//! The Cursor app (the editor, as opposed to the `cursor-agent` CLI) keeps
//! every Agent conversation ("composer") in this one SQLite database. It
//! writes an `agent-transcripts/<id>/<id>.jsonl` for some of them, but often
//! leaves the transcript directory empty, and those conversations had no other
//! record RelayHistory read. The transcript stays the primary source, and a
//! `cursor-agent` chat store comes next: a composer that has either is read
//! from it, exactly as before, and this database is read only for the
//! composers that have neither.
//!
//! ## What the database holds
//!
//! Characterized from Cursor 2.x editor builds (composer `_v` 14 and 18,
//! bubble `_v` 3); Cursor publishes no schema.
//!
//! * SQLite in WAL mode, `cursorDiskKV(key TEXT UNIQUE ON CONFLICT REPLACE,
//!   value BLOB)` beside VS Code's own `ItemTable`. A rewrite of a key is a
//!   replace, so the row gets a new rowid.
//! * `composerData:<composerId>` is JSON: `name` (the title), `createdAt` and
//!   `lastUpdatedAt` in epoch milliseconds, `workspaceIdentifier`
//!   (`{id, uri: {fsPath}}`, the workspace folder), and the conversation's
//!   order: `headerLog` names the chunked `composerConvHeaders:<id>:<chunk>`
//!   rows (newer builds), `fullConversationHeadersOnly` holds the headers
//!   inline (older builds, often empty), and the oldest builds keep whole
//!   bubbles in `conversation`.
//! * `bubbleId:<composerId>:<bubbleId>` is one message: `type` 1 is the
//!   person, 2 the assistant; `text`, `createdAt` (ISO 8601), `thinking.text`,
//!   and `toolFormerData` (`name`, `toolCallId`, `params` and `result` as JSON
//!   strings, `status`) for a tool call.
//! * `workspaceStorage/<workspaceIdentifier.id>/workspace.json` names the
//!   folder when the composer itself does not.
//!
//! Each bubble is rendered as the transcript record Cursor writes for it and
//! goes through the transcript parser, so a conversation reads the same from
//! either source. Bubbles carry their own creation time, which the parser
//! takes as the record `timestamp`. Every record carries its bubble id as
//! `message.id`.
//!
//! The database is shared by every composer and rewritten constantly while
//! Cursor runs, so a composer's change stamp is read from the rows that make
//! it up — the `composerData` row and the rowids of its bubble and header
//! rows — through the key index alone, and only a composer whose stamp moved
//! is read again.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use anyhow::Result;

mod read;
mod render;
mod sync;
#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
mod tests;

pub(crate) use read::{composer_heads, composer_stamp, read_composer, read_composer_preview};
pub(in crate::ingest) use sync::{rebuild, sync_editor_composers_in};

/// The prefix of a composer's locator: `cursor-editor:<db path>#<composer id>`.
const LOCATOR_PREFIX: &str = "cursor-editor:";
/// Where plain `sync` remembers which version of each composer it indexed.
pub(crate) const EDITOR_STATE_KEY: &str = "cursor_editor_composers_v1";

/// The Cursor editor's user directory under `home`, where Electron keeps it
/// on each platform.
pub(crate) fn user_dir(home: &Path) -> PathBuf {
    if cfg!(target_os = "macos") {
        home.join("Library/Application Support/Cursor/User")
    } else if cfg!(windows) {
        home.join("AppData/Roaming/Cursor/User")
    } else {
        home.join(".config/Cursor/User")
    }
}

/// The editor's global state database under `home`.
pub(crate) fn state_db_path(home: &Path) -> PathBuf {
    user_dir(home).join("globalStorage/state.vscdb")
}

/// One composer's catalog locator. Unique per composer, so the discovery
/// engine's per-locator stamps never collide in the shared database.
pub(crate) fn locator(db: &Path, composer_id: &str) -> String {
    format!("{LOCATOR_PREFIX}{}#{composer_id}", db.display())
}

/// The database and composer id a locator names.
pub(crate) fn parse_locator(locator: &str) -> Option<(PathBuf, &str)> {
    let (path, id) = locator.strip_prefix(LOCATOR_PREFIX)?.rsplit_once('#')?;
    (!id.is_empty() && !path.is_empty()).then(|| (PathBuf::from(path), id))
}

/// Whether `path` is a composer locator rather than a file.
pub(crate) fn is_locator(path: &Path) -> bool {
    path.to_str().and_then(parse_locator).is_some()
}

/// One composer, read from the database and rendered as transcript records.
#[derive(Debug, Default)]
pub(crate) struct Composer {
    pub session_id: String,
    pub title: Option<String>,
    pub cwd: Option<String>,
    pub created_ms: Option<i64>,
    pub updated_ms: Option<i64>,
    /// Transcript-shaped JSON records, one per line, each ending in `\n`.
    pub transcript: String,
    pub records: usize,
    /// Bubbles the conversation names that were missing or not readable JSON.
    pub skipped_bubbles: usize,
}

/// A composer found by enumeration: its id, change stamp and recency.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ComposerHead {
    pub session_id: String,
    pub stamp: String,
    pub recency_ms: Option<i64>,
}

/// The composer ids another Cursor source already owns: a transcript under
/// `.cursor/projects` (a subagent's included), or a `cursor-agent` chat store.
pub(crate) fn owned_elsewhere(home: &Path) -> Result<HashSet<String>> {
    let mut ids =
        crate::ingest::cursor_store::transcript_session_ids(&home.join(".cursor/projects"))?;
    ids.extend(crate::ingest::cursor_store::chat_store_ids(home)?);
    Ok(ids)
}

/// Whether a composer is now read from another Cursor source.
pub(crate) fn superseded(home: &Path, composer_id: &str) -> Result<bool> {
    Ok(owned_elsewhere(home)?.contains(composer_id))
}

/// Every composer in the editor database under `home` that has a
/// conversation and no other source, narrowed to a path-scoped tick that
/// touched the database. The database is read whole either way: one indexed
/// query per composer.
pub(crate) fn composers(
    home: &Path,
    scope: Option<&crate::ingest::path_scope::PathScope>,
) -> Result<Vec<ComposerHead>> {
    let db = state_db_path(home);
    if !db.is_file() {
        return Ok(Vec::new());
    }
    if let (Some(scope), Some(dir)) = (scope, db.parent()) {
        if !scope.mentions(dir) {
            return Ok(Vec::new());
        }
    }
    let owned = owned_elsewhere(home)?;
    Ok(composer_heads(&db)?
        .into_iter()
        .filter(|head| !owned.contains(&head.session_id))
        .collect())
}

/// A change stamp over the database file and its write-ahead log.
pub(super) fn file_stamp(db: &Path) -> Result<String> {
    let (main, _) = crate::file_stamp_and_modified(db)?;
    let wal = crate::ingest::cursor_store::sidecar(db, "-wal");
    Ok(match crate::file_stamp_and_modified(&wal) {
        Ok((wal, _)) => format!("{main}|wal:{wal}"),
        Err(_) => main,
    })
}
