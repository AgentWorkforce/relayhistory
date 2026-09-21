//! One OpenCode session, however it was stored, and the evidence it produces.
//!
//! OpenCode ships two on-disk layouts and both are in the field:
//!
//! * **SQLite** — `$OPENCODE_DB`, default `~/.local/share/opencode/opencode.db`,
//!   tables `session`, `message`, `part`, each row carrying the provider's own
//!   JSON payload in a `data` column.
//! * **Legacy JSON tree** — `$OPENCODE_STORAGE_DIR`, default
//!   `~/.local/share/opencode/storage`, laid out as
//!   `session/<scope>/<sessionId>.json`, `message/<sessionId>/<messageId>.json`
//!   and `part/<messageId>/<partId>.json`.
//!
//! The payloads are the *same* JSON either way: only the envelope differs. So
//! the two loaders below do nothing but produce an [`OpencodeSession`], and
//! every parsing decision — which parts count, what a target is, when a tool
//! failed — is written once in [`normalize`] and the helpers around it. Two
//! loaders, one parser: a fixture stored both ways yields byte-identical
//! evidence, which `opencode_parity` asserts.
//!
//! The field list and the part handling are ported from burn's read-only
//! reference reader (`crates/relayburn-sdk/src/reader/opencode.rs`), which is
//! the behaviour this store has to match.

use super::{
    insert_session_event_with_provenance, insert_tool_call, upsert_file_edit_from_call,
    upsert_session, RawMessageFacts, OPENCODE_MARKER_COMPACTION_BOUNDARY,
};
use crate::relationship_capture::{record_relationship, ObservedRelationship};
use crate::{insert_history, prompt_hash, HistoryEntry};
use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// The session record itself: identity, delegation, working directory.
#[derive(Debug, Clone, Default)]
pub(crate) struct OpencodeSessionInfo {
    pub id: String,
    /// `session.parentID`. OpenCode is the one harness that names a subagent's
    /// parent session outright, which is what makes its child identity stable.
    pub parent_id: Option<String>,
    pub directory: Option<String>,
    pub created_ms: Option<i64>,
    pub updated_ms: Option<i64>,
}

/// One message envelope. `raw` is kept so nothing has to be re-read from the
/// store to answer a question this struct did not anticipate.
#[derive(Debug, Clone)]
pub(crate) struct OpencodeMessage {
    pub id: String,
    pub role: String,
    pub time_created: i64,
    pub parent_id: Option<String>,
    pub provider_id: Option<String>,
    pub model_id: Option<String>,
    pub path_cwd: Option<String>,
    /// The provider's `tokens` object verbatim, stored as written:
    /// `{input, output, reasoning, cache:{read, write}}`.
    pub tokens: Option<Value>,
}

#[derive(Debug, Clone)]
pub(crate) struct OpencodePart {
    pub id: String,
    pub kind: String,
    pub raw: Map<String, Value>,
}

impl OpencodePart {
    fn get(&self, key: &str) -> Option<&Value> {
        self.raw.get(key)
    }
}

/// What both loaders produce and the single normalizer consumes.
#[derive(Debug, Clone, Default)]
pub(crate) struct OpencodeSession {
    pub session: OpencodeSessionInfo,
    pub messages: Vec<OpencodeMessage>,
    pub parts_by_message: BTreeMap<String, Vec<OpencodePart>>,
}

impl OpencodeSession {
    fn parts(&self, message_id: &str) -> &[OpencodePart] {
        self.parts_by_message
            .get(message_id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    /// The first substantive human turn, for the catalog excerpt. Synthetic
    /// text is skipped, as it is everywhere else in this parser.
    pub(crate) fn first_user_text(&self) -> Option<&str> {
        self.messages
            .iter()
            .filter(|message| message.role == "user")
            .flat_map(|message| self.parts(&message.id))
            .find_map(|part| part_text(part).filter(|text| !text.trim().is_empty()))
    }

    /// The first `"<providerID>/<modelID>"` an assistant message named.
    pub(crate) fn first_model(&self) -> Option<String> {
        self.messages.iter().find_map(|message| {
            (message.role == "assistant")
                .then(|| build_model(message.provider_id.as_deref(), message.model_id.as_deref()))
                .flatten()
        })
    }
}

// ---------------------------------------------------------------------------
// Layout detection
// ---------------------------------------------------------------------------

/// Which OpenCode store a host actually has. `opencode.db` wins when both are
/// present: newer releases write SQLite and leave the old tree behind, so
/// preferring the tree would silently serve stale history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OpencodeLayout {
    Sqlite(PathBuf),
    JsonTree(PathBuf),
}

impl OpencodeLayout {
    pub fn detect(db_path: &Path, storage_dir: &Path) -> Option<Self> {
        if db_path.is_file() {
            return Some(Self::Sqlite(db_path.to_path_buf()));
        }
        if storage_dir.join("session").is_dir() {
            return Some(Self::JsonTree(storage_dir.to_path_buf()));
        }
        None
    }
}

// ---------------------------------------------------------------------------
// Loader: SQLite (`opencode.db`)
// ---------------------------------------------------------------------------

/// Read one session out of an already-open, read-only provider connection.
///
/// Every query is keyed on the session id, so this never enumerates or copies
/// the whole provider store however large it is.
pub(crate) fn load_from_sqlite(
    src: &Connection,
    session_id: &str,
) -> Result<Option<OpencodeSession>> {
    let session_columns = table_columns(src, "session")?;
    if !session_columns.contains("id") {
        return Ok(None);
    }
    let parent = optional_column(&session_columns, "parent_id");
    let directory = optional_column(&session_columns, "directory");
    let created = optional_column(&session_columns, "time_created");
    let updated = optional_column(&session_columns, "time_updated");
    let sql =
        format!("SELECT id, {parent}, {directory}, {created}, {updated} FROM session WHERE id = ?");
    let info = src
        .query_row(&sql, [session_id], |row| {
            Ok(OpencodeSessionInfo {
                id: row.get::<_, String>(0)?,
                parent_id: row.get::<_, Option<String>>(1)?,
                directory: row.get::<_, Option<String>>(2)?,
                created_ms: row.get::<_, Option<i64>>(3)?,
                updated_ms: row.get::<_, Option<i64>>(4)?,
            })
        })
        // `optional()`, not `.ok()`. A failed query is not an absent session:
        // a lock held past the busy timeout, a provider value of a type this
        // row mapping cannot take, a corrupted page — every one of them became
        // `None` here, which the callers read as "this session is not in the
        // store". Hydration then commits its checkpoint over zero events and
        // stamps that as the current state of a session that is really still
        // there, so the retry the failure called for never happens.
        .optional()
        .with_context(|| format!("reading OpenCode session {session_id}"))?;
    let Some(mut info) = info else {
        return Ok(None);
    };
    info.parent_id = info.parent_id.filter(|value| !value.is_empty());

    let message_columns = table_columns(src, "message")?;
    let mut messages = Vec::new();
    if message_columns.contains("id")
        && message_columns.contains("data")
        && message_columns.contains("session_id")
    {
        let fallback = optional_column(&message_columns, "time_created");
        let sql = format!(
            "SELECT id, data, {fallback} FROM message WHERE session_id = ? AND json_valid(data)"
        );
        let mut stmt = src.prepare(&sql)?;
        let rows = stmt
            .query_map([session_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        for (id, data, fallback_ts) in rows {
            let Ok(Value::Object(object)) = serde_json::from_str::<Value>(&data) else {
                continue;
            };
            if let Some(message) = parse_message(&id, &object, fallback_ts) {
                messages.push(message);
            }
        }
    }

    let part_columns = table_columns(src, "part")?;
    let mut parts_by_message: BTreeMap<String, Vec<OpencodePart>> = BTreeMap::new();
    if part_columns.contains("id")
        && part_columns.contains("data")
        && part_columns.contains("message_id")
    {
        // Seek by session when the provider indexes it, otherwise by the
        // message ids this session's own messages already named. Both stay
        // session-keyed; neither scans the provider's whole `part` table.
        let mut push = |id: String, message_id: String, data: String| {
            let Ok(Value::Object(object)) = serde_json::from_str::<Value>(&data) else {
                return;
            };
            let kind = object
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            parts_by_message
                .entry(message_id)
                .or_default()
                .push(OpencodePart {
                    id,
                    kind,
                    raw: object,
                });
        };
        // Seek by whichever column the provider actually indexes. Choosing on
        // column *presence* alone picks a predicate SQLite can only answer by
        // scanning `part`, which for one session is merely slow but for the
        // global sweep is one full scan per session.
        let seek_part_by_session = part_columns.contains("session_id")
            && (has_leading_index(src, "part", "session_id")?
                || !has_leading_index(src, "part", "message_id")?);
        if seek_part_by_session {
            let mut stmt = src.prepare(
                "SELECT id, message_id, data FROM part WHERE session_id = ? AND json_valid(data)",
            )?;
            let rows = stmt
                .query_map([session_id], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            for (id, message_id, data) in rows {
                push(id, message_id, data);
            }
        } else {
            let mut stmt = src.prepare(
                "SELECT id, message_id, data FROM part WHERE message_id = ? AND json_valid(data)",
            )?;
            for message in &messages {
                let rows = stmt
                    .query_map([&message.id], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                for (id, message_id, data) in rows {
                    push(id, message_id, data);
                }
            }
        }
    }

    Ok(Some(finish(info, messages, parts_by_message)))
}

/// How global sync should read a provider store.
///
/// Per-session queries are the right shape when the provider indexes the
/// column they seek on. When it does not, each one costs a full scan of
/// `part` — and global sync runs one per session, so a store with 10,000
/// sessions and a million parts pays 10,000 scans. The removed
/// whole-database backup used to hide this by copying the store and building
/// `part(session_id)` on the copy; nothing may build an index on the
/// provider's own database, so the unindexed store gets a different plan
/// instead of a slower version of the same one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpencodeSyncPlan {
    /// One bounded, index-seeking query per session.
    PerSession,
    /// One scan of `message` and one of `part` for the whole store, grouped
    /// in memory. Reads every row once rather than once per session.
    SinglePass,
}

/// Whether `table` has an index whose *leading* column is `column`, which is
/// what makes an equality predicate on it a seek rather than a scan. SQLite's
/// primary-key autoindexes are included by the pragma.
///
/// Discovery already treats these provider indexes as optional; ingestion has
/// to as well, for the same reason: RelayHistory never issues provider DDL,
/// so an index that is not there cannot be created.
pub(crate) fn has_leading_index(conn: &Connection, table: &str, column: &str) -> Result<bool> {
    Ok(conn
        .prepare(&format!(
            "SELECT 1 FROM pragma_index_list('{table}') indexes \
             JOIN pragma_index_info(indexes.name) columns \
             WHERE columns.seqno = 0 AND columns.name = ? LIMIT 1"
        ))?
        .query_row([column], |_| Ok(()))
        .optional()?
        .is_some())
}

/// Which plan this store supports. Session-keyed reads need `part` seekable
/// by session, or messages seekable by session *and* parts by message.
pub(crate) fn sync_plan(src: &Connection) -> Result<OpencodeSyncPlan> {
    let part_by_session = has_leading_index(src, "part", "session_id")?;
    let message_by_session = has_leading_index(src, "message", "session_id")?;
    let part_by_message = has_leading_index(src, "part", "message_id")?;
    if part_by_session || (message_by_session && part_by_message) {
        Ok(OpencodeSyncPlan::PerSession)
    } else {
        Ok(OpencodeSyncPlan::SinglePass)
    }
}

/// What one whole-store pass could read, and which sessions it could not.
///
/// Returned together for the same reason the legacy tree's listing is: one
/// session whose row this mapping rejects must not cost the rest of the store,
/// and it must not be silently absent either.
#[derive(Debug, Default)]
pub(crate) struct OpencodeStoreLoad {
    pub sessions: Vec<OpencodeSession>,
    pub failures: Vec<OpencodeSessionFailure>,
}

/// One session the store holds and this pass could not read.
#[derive(Debug)]
pub(crate) struct OpencodeSessionFailure {
    pub session_id: String,
    pub error: String,
}

/// Read the whole store in one pass per table and group it in memory.
///
/// The fallback for a store whose provider indexes are missing. It trades
/// memory for scans, which is the right way round here: the alternative this
/// replaced copied the entire database to a temporary file first, so holding
/// the rows costs no more than that did and reads each one exactly once.
pub(crate) fn load_all_from_sqlite(src: &Connection) -> Result<OpencodeStoreLoad> {
    let mut load = OpencodeStoreLoad::default();
    // Sessions whose rows this pass could not map. Dropped from the output at
    // the end rather than as they are found, because a message can fail after
    // some of its session's rows have already been collected, and a session
    // read in part is not a session read.
    let mut failed: BTreeMap<String, String> = BTreeMap::new();
    let session_columns = table_columns(src, "session")?;
    if !session_columns.contains("id") {
        return Ok(load);
    }
    let parent = optional_column(&session_columns, "parent_id");
    let directory = optional_column(&session_columns, "directory");
    let created = optional_column(&session_columns, "time_created");
    let updated = optional_column(&session_columns, "time_updated");
    let sql = format!(
        "SELECT id, {parent}, {directory}, {created}, {updated} FROM session \
         WHERE id IS NOT NULL AND id <> ''"
    );
    // Mapped one row at a time. A value the mapping cannot take is one
    // session's problem; failing to *step* the statement is the store's, and
    // that still propagates.
    let mut infos: BTreeMap<String, OpencodeSessionInfo> = BTreeMap::new();
    {
        let mut stmt = src.prepare(&sql)?;
        let rows = stmt.query_map([], |row| {
            let id = row.get::<_, String>(0);
            let mapped = (|| -> rusqlite::Result<OpencodeSessionInfo> {
                Ok(OpencodeSessionInfo {
                    id: row.get::<_, String>(0)?,
                    parent_id: row.get::<_, Option<String>>(1)?,
                    directory: row.get::<_, Option<String>>(2)?,
                    created_ms: row.get::<_, Option<i64>>(3)?,
                    updated_ms: row.get::<_, Option<i64>>(4)?,
                })
            })();
            Ok((id, mapped))
        })?;
        for row in rows {
            let (id, mapped) = row?;
            match mapped {
                Ok(mut info) => {
                    info.parent_id = info.parent_id.filter(|value| !value.is_empty());
                    infos.insert(info.id.clone(), info);
                }
                Err(error) => {
                    let id = id.unwrap_or_else(|_| "<unnamed session row>".into());
                    failed.insert(id, error.to_string());
                }
            }
        }
    }

    // One scan of `message`, grouped by session, remembering which session
    // each message belongs to so the parts can be placed without a second
    // lookup per row.
    let message_columns = table_columns(src, "message")?;
    let mut messages_by_session: BTreeMap<String, Vec<OpencodeMessage>> = BTreeMap::new();
    let mut session_of_message: BTreeMap<String, String> = BTreeMap::new();
    if message_columns.contains("id")
        && message_columns.contains("data")
        && message_columns.contains("session_id")
    {
        let fallback = optional_column(&message_columns, "time_created");
        let sql =
            format!("SELECT id, session_id, data, {fallback} FROM message WHERE json_valid(data)");
        let mut stmt = src.prepare(&sql)?;
        let mapped = stmt.query_map([], |row| {
            // `session_id` first and on its own, because a row that fails is
            // only attributable to one session if it can still say which.
            let session_id = row.get::<_, String>(1);
            let rest = (|| -> rusqlite::Result<(String, String, Option<i64>)> {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                ))
            })();
            Ok((session_id, rest))
        })?;
        let mut rows = Vec::new();
        for row in mapped {
            let (session_id, rest) = row?;
            match (session_id, rest) {
                (Ok(session_id), Ok((id, data, fallback_ts))) => {
                    rows.push((id, session_id, data, fallback_ts))
                }
                (Ok(session_id), Err(error)) => {
                    failed
                        .entry(session_id)
                        .or_insert_with(|| error.to_string());
                }
                // A row that cannot even name its session could belong to any
                // of them, so there is no session to exclude and no honest way
                // to call the rest of this pass complete.
                (Err(error), _) => return Err(error).context("reading an OpenCode `message` row"),
            }
        }
        for (id, session_id, data, fallback_ts) in rows {
            if !infos.contains_key(&session_id) {
                continue;
            }
            let Ok(Value::Object(object)) = serde_json::from_str::<Value>(&data) else {
                continue;
            };
            if let Some(message) = parse_message(&id, &object, fallback_ts) {
                session_of_message.insert(message.id.clone(), session_id.clone());
                messages_by_session
                    .entry(session_id)
                    .or_default()
                    .push(message);
            }
        }
    }

    // One scan of `part`, placed by the message map above.
    let part_columns = table_columns(src, "part")?;
    let mut parts_by_session: BTreeMap<String, BTreeMap<String, Vec<OpencodePart>>> =
        BTreeMap::new();
    if part_columns.contains("id")
        && part_columns.contains("data")
        && part_columns.contains("message_id")
    {
        let mut stmt =
            src.prepare("SELECT id, message_id, data FROM part WHERE json_valid(data)")?;
        let mapped = stmt.query_map([], |row| {
            let message_id = row.get::<_, String>(1);
            let rest = (|| -> rusqlite::Result<(String, String)> {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(2)?))
            })();
            Ok((message_id, rest))
        })?;
        let mut rows = Vec::new();
        for row in mapped {
            let (message_id, rest) = row?;
            match (message_id, rest) {
                (Ok(message_id), Ok((id, data))) => rows.push((id, message_id, data)),
                (Ok(message_id), Err(error)) => {
                    if let Some(session_id) = session_of_message.get(&message_id) {
                        failed
                            .entry(session_id.clone())
                            .or_insert_with(|| error.to_string());
                    }
                }
                (Err(error), _) => return Err(error).context("reading an OpenCode `part` row"),
            }
        }
        for (id, message_id, data) in rows {
            let Some(session_id) = session_of_message.get(&message_id) else {
                continue;
            };
            let Ok(Value::Object(object)) = serde_json::from_str::<Value>(&data) else {
                continue;
            };
            let kind = object
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            parts_by_session
                .entry(session_id.clone())
                .or_default()
                .entry(message_id)
                .or_default()
                .push(OpencodePart {
                    id,
                    kind,
                    raw: object,
                });
        }
    }

    let ids: Vec<String> = infos.keys().cloned().collect();
    load.sessions.reserve(ids.len());
    for id in ids {
        if failed.contains_key(&id) {
            continue;
        }
        let info = infos.remove(&id).expect("id came from this map");
        load.sessions.push(finish(
            info,
            messages_by_session.remove(&id).unwrap_or_default(),
            parts_by_session.remove(&id).unwrap_or_default(),
        ));
    }
    load.failures = failed
        .into_iter()
        .map(|(session_id, error)| OpencodeSessionFailure { session_id, error })
        .collect();
    Ok(load)
}

/// Every session id the provider store names. Used by global sync, which is
/// the only caller allowed to enumerate.
pub(crate) fn list_sqlite_session_ids(src: &Connection) -> Result<Vec<String>> {
    Ok(src
        .prepare("SELECT id FROM session WHERE id IS NOT NULL AND id <> ''")?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}

fn table_columns(conn: &Connection, table: &str) -> Result<BTreeSet<String>> {
    Ok(conn
        .prepare(&format!("SELECT name FROM pragma_table_info('{table}')"))?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<BTreeSet<String>>>()?)
}

fn optional_column<'a>(columns: &BTreeSet<String>, name: &'a str) -> &'a str {
    if columns.contains(name) {
        name
    } else {
        "NULL"
    }
}

// ---------------------------------------------------------------------------
// Loader: legacy JSON tree (`storage/`)
// ---------------------------------------------------------------------------

/// Every session file under the tree, sorted, so enumeration is deterministic.
pub(crate) fn list_json_tree_session_files(storage_root: &Path) -> OpencodeTreeListing {
    let mut listing = OpencodeTreeListing::default();
    collect_session_files(&storage_root.join("session"), &mut listing);
    listing.sessions.sort();
    listing.unreadable.sort_by(|a, b| a.path.cmp(&b.path));
    listing
}

/// What a walk of the legacy tree found, and what it could not look at.
///
/// The two are returned together on purpose. Propagating the first listing
/// error would take every session in the tree down with one unreadable scope
/// directory, which is the failure the previous round fixed; dropping it — as
/// this did — hands the caller a complete-looking list with a whole subtree
/// missing from it, and nothing says so. Sessions *and* the paths that could
/// not be walked, so a caller can index what it has and still report what it
/// could not see.
#[derive(Debug, Default)]
pub(crate) struct OpencodeTreeListing {
    pub sessions: Vec<PathBuf>,
    pub unreadable: Vec<OpencodeUnreadableDir>,
}

/// A directory under `session/` that exists and could not be walked.
#[derive(Debug)]
pub(crate) struct OpencodeUnreadableDir {
    pub path: PathBuf,
    pub error: String,
}

fn collect_session_files(dir: &Path, listing: &mut OpencodeTreeListing) {
    let note = |path: &Path, error: std::io::Error, listing: &mut OpencodeTreeListing| {
        listing.unreadable.push(OpencodeUnreadableDir {
            path: path.to_path_buf(),
            error: error.to_string(),
        });
    };
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        // A directory that is not there holds nothing, which is ordinary: the
        // tree may have no `session/` at all, and a scope can be removed while
        // the walk is in it.
        Err(error) if error.kind() == io::ErrorKind::NotFound => return,
        Err(error) => return note(dir, error, listing),
    };
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                note(dir, error, listing);
                continue;
            }
        };
        let path = entry.path();
        let kind = match entry.file_type() {
            Ok(kind) => kind,
            Err(error) => {
                note(&path, error, listing);
                continue;
            }
        };
        if kind.is_dir() {
            collect_session_files(&path, listing);
        } else if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("ses_") && name.ends_with(".json"))
        {
            listing.sessions.push(path);
        }
    }
}

/// Read one session out of the JSON tree, given its session file.
pub(crate) fn load_from_json_tree(session_file: &Path) -> Result<Option<OpencodeSession>> {
    // A read that fails is not a verdict. `Ok(None)` means "this candidate is
    // not a session", and discovery records that against the current source
    // stamp; hydration commits its checkpoint over whatever came back. An
    // unreadable file changes neither its size nor its mtime, so a skip or a
    // checkpoint written while it was unreadable stays valid after access
    // recovers -- the omission outlives the outage that caused it.
    //
    // So the two are separated: a *malformed* provider record is skippable and
    // still returns `Ok(None)`, while an I/O failure propagates with the path
    // that failed. `NotFound` counts as malformed-not-present: discovery has
    // already filtered to files that exist, so a file that vanished between
    // the stat and the read is a race, not an outage, and the next run sees
    // the tree as it now is.
    let raw = match fs::read_to_string(session_file) {
        Ok(raw) => raw,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| {
                format!("reading OpenCode session file {}", session_file.display())
            })
        }
    };
    let Ok(Value::Object(object)) = serde_json::from_str::<Value>(&raw) else {
        return Ok(None);
    };
    let Some(id) = object.get("id").and_then(Value::as_str) else {
        return Ok(None);
    };
    let time = object.get("time").and_then(Value::as_object);
    let info = OpencodeSessionInfo {
        id: id.to_string(),
        parent_id: object
            .get("parentID")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_string),
        directory: object
            .get("directory")
            .and_then(Value::as_str)
            .map(str::to_string),
        created_ms: time
            .and_then(|time| time.get("created"))
            .and_then(Value::as_i64),
        updated_ms: time
            .and_then(|time| time.get("updated"))
            .and_then(Value::as_i64),
    };
    // `session/<scope>/<id>.json` → the tree root is two levels up; a session
    // file sitting directly under `session/` is one.
    let storage_root = derive_storage_root(session_file);

    let mut messages = Vec::new();
    for (message_id, object) in read_json_dir(&storage_root.join("message").join(&info.id))? {
        if let Some(message) = parse_message(&message_id, &object, None) {
            messages.push(message);
        }
    }

    let mut parts_by_message: BTreeMap<String, Vec<OpencodePart>> = BTreeMap::new();
    for message in &messages {
        for (part_id, object) in read_json_dir(&storage_root.join("part").join(&message.id))? {
            let kind = object
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            parts_by_message
                .entry(message.id.clone())
                .or_default()
                .push(OpencodePart {
                    id: part_id,
                    kind,
                    raw: object,
                });
        }
    }

    Ok(Some(finish(info, messages, parts_by_message)))
}

/// The tree root, from a session file inside it. OpenCode writes
/// `session/<scope>/<id>.json`, but has also written `session/<id>.json`, so
/// the directory holding the file decides how far up the root is.
///
/// Shared, and deliberately the only implementation: hydration once had its
/// own two-`parent()` version of this, which resolved the scoped layout to
/// `storage/session` instead of `storage`. Every message and part lookup then
/// probed a directory that does not exist, found nothing, and produced a
/// stamp over the session file alone — so a session that grew reported
/// `unchanged` forever. One caller of one function cannot drift from itself.
pub(crate) fn derive_storage_root(session_file: &Path) -> PathBuf {
    let mut dir = session_file.to_path_buf();
    dir.pop();
    if dir.file_name().and_then(|name| name.to_str()) == Some("session") {
        dir.pop();
        return dir;
    }
    dir.pop(); // out of the <scope> directory
    dir.pop(); // out of `session/`
    dir
}

/// What a legacy-tree session is composed of, as a change signal.
///
/// OpenCode appends a turn by writing *new files* under `message/` and
/// `part/`; it does not rewrite the session JSON. So anything that stamps the
/// session file alone reports a growing session as unchanged, and both the
/// discovery cache and the hydration checkpoint then skip it forever.
///
/// `files` is the load-bearing field. `newest_ms` alone would be unreliable —
/// filesystem timestamp granularity is coarse enough that a turn appended
/// within the same tick of a previous read can share its mtime — but adding a
/// turn always adds a file, so the count moves whether or not the clock does.
/// `bytes` catches an in-place edit of an existing part.
///
/// Those three are *aggregates*, though, and aggregates collide. A part
/// rewritten to a different payload of the same length leaves `bytes` and
/// `files` exactly where they were, and if the rewrite lands in the same
/// filesystem timestamp tick as the read before it, `newest_ns` too — so the
/// session reports `unchanged` and keeps the superseded event. `digest`
/// closes that: it is a per-file fold rather than a sum, so a file that
/// changes length while another changes the opposite way no longer cancels
/// out, and for the files where metadata *cannot* settle the question it folds
/// in the content itself. See [`stamp_json_tree_session`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct OpencodeTreeStamp {
    pub bytes: u64,
    pub files: u64,
    /// Newest modification time across those files, in nanoseconds.
    pub newest_ns: u128,
    /// Per-file fold of name, length and mtime, plus the contents of any file
    /// recent enough that its next write could share this mtime.
    pub digest: u64,
}

impl OpencodeTreeStamp {
    /// The opaque token stored as a source stamp.
    pub fn token(&self) -> String {
        format!(
            "{}:{}:{}:{:016x}",
            self.bytes, self.files, self.newest_ns, self.digest
        )
    }

    /// Newest activity as epoch milliseconds, for a recency hint.
    pub fn newest_ms(&self) -> Option<i64> {
        i64::try_from(self.newest_ns / 1_000_000).ok()
    }
}

/// How close to the moment of stamping a file's mtime has to be before
/// metadata stops being able to prove it unchanged.
///
/// A same-length rewrite is invisible to `bytes` and `files`, so the only
/// metadata left is mtime — and mtime can only distinguish two writes that
/// land in different ticks. Suppose a file is written at T, stamped at S, and
/// rewritten at U. If the rewrite is invisible then `mtime(T) == mtime(U)`,
/// which means T and U share a tick; S lies between them, so S is in that tick
/// too. In other words the collision is only possible for a file whose mtime
/// is *within one tick of the stamp*, and hashing those is enough. Two seconds
/// covers the coarsest granularity still in the field (FAT's two-second mtime).
///
/// This is what keeps the content hash off the hot path: in a tree that is not
/// being written to right now, no file qualifies and the stamp is still pure
/// metadata. A file that settles from ambiguous to old changes the digest once
/// and costs one extra read of one session — the safe direction, since the
/// error it cannot make is reporting `unchanged` over a change.
const STAMP_AMBIGUITY_NS: u128 = 2_000_000_000;

/// Stamp every file that composes one legacy-tree session: the session JSON,
/// its messages, and those messages' parts.
///
/// Metadata for everything, plus the contents of any file recent enough that
/// metadata cannot settle it (see [`STAMP_AMBIGUITY_NS`]), so discovery can
/// still run this over a whole tree.
pub(crate) fn stamp_json_tree_session(
    session_file: &Path,
    session_id: &str,
) -> Result<OpencodeTreeStamp> {
    let root = derive_storage_root(session_file);
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or_default();
    let mut stamp = OpencodeTreeStamp {
        digest: FNV_OFFSET,
        ..OpencodeTreeStamp::default()
    };
    let mut add = |path: &Path, stamp: &mut OpencodeTreeStamp| -> Result<()> {
        let meta = match fs::metadata(path) {
            Ok(meta) => meta,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error).with_context(|| format!("stat {}", path.display())),
        };
        stamp.bytes += meta.len();
        stamp.files += 1;
        let changed = meta
            .modified()
            .or_else(|_| meta.created())
            .ok()
            .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or_default();
        stamp.newest_ns = stamp.newest_ns.max(changed);

        // Fold this file in by name, length and mtime rather than adding it to
        // a total, so two changes cannot cancel each other out.
        stamp.digest = fnv(
            stamp.digest,
            path.file_name().unwrap_or_default().as_encoded_bytes(),
        );
        stamp.digest = fnv(stamp.digest, &meta.len().to_le_bytes());
        stamp.digest = fnv(stamp.digest, &changed.to_le_bytes());
        if now_ns.saturating_sub(changed) <= STAMP_AMBIGUITY_NS {
            let body = match fs::read(path) {
                Ok(body) => body,
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("reading OpenCode record {}", path.display()))
                }
            };
            stamp.digest = fnv(stamp.digest, b"\x01");
            stamp.digest = fnv(stamp.digest, &body);
        }
        Ok(())
    };
    add(session_file, &mut stamp)?;
    let mut message_ids =
        json_file_stems(&root.join("message").join(session_id), &mut stamp, &mut add)?;
    message_ids.sort();
    for message_id in &message_ids {
        json_file_stems(&root.join("part").join(message_id), &mut stamp, &mut add)?;
    }
    Ok(stamp)
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;

/// FNV-1a. Not a security primitive and not asked to be one: this only has to
/// change when the bytes change, over files one process wrote and this process
/// reads back.
fn fnv(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// Visit every `*.json` in one directory, returning their stems. Sorted, so
/// the fold is order-independent of the filesystem's own listing order; a
/// missing directory is simply empty, an unlistable one is an error.
fn json_file_stems(
    dir: &Path,
    stamp: &mut OpencodeTreeStamp,
    visit: &mut impl FnMut(&Path, &mut OpencodeTreeStamp) -> Result<()>,
) -> Result<Vec<String>> {
    let mut paths = json_paths_in(dir)?;
    paths.sort();
    let mut stems = Vec::new();
    for path in &paths {
        visit(path, stamp)?;
        if let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) {
            stems.push(stem.to_string());
        }
    }
    Ok(stems)
}

/// `*.json` in one directory, keyed by the payload's own `id` when it has one
/// and by the file stem otherwise. A file that is not a JSON object is
/// skipped, exactly as the reference reader skips it; a file that cannot be
/// *read* is an error, for the reason given on `load_from_json_tree`.
fn read_json_dir(dir: &Path) -> Result<Vec<(String, Map<String, Value>)>> {
    let mut paths = json_paths_in(dir)?;
    paths.sort();
    let mut out = Vec::new();
    for path in paths {
        let raw = match fs::read_to_string(&path) {
            Ok(raw) => raw,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("reading OpenCode record {}", path.display()))
            }
        };
        let Ok(Value::Object(object)) = serde_json::from_str::<Value>(&raw) else {
            continue;
        };
        let id = object
            .get("id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| {
                path.file_stem()
                    .and_then(|stem| stem.to_str())
                    .map(str::to_string)
            });
        let Some(id) = id else { continue };
        out.push((id, object));
    }
    Ok(out)
}

/// Every `*.json` directly in `dir`, unsorted. A directory that is not there
/// is empty -- a session with no messages is ordinary. A directory that is
/// there and cannot be listed is an error, because "it enumerated as nothing"
/// and "it holds nothing" are the two readings this whole change is about
/// keeping apart.
fn json_paths_in(dir: &Path) -> Result<Vec<PathBuf>> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error).with_context(|| format!("listing {}", dir.display())),
    };
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry.with_context(|| format!("listing {}", dir.display()))?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("json") {
            paths.push(path);
        }
    }
    Ok(paths)
}

// ---------------------------------------------------------------------------
// Shared parsing
// ---------------------------------------------------------------------------

fn parse_message(
    id: &str,
    object: &Map<String, Value>,
    fallback_ts: Option<i64>,
) -> Option<OpencodeMessage> {
    let role = object.get("role").and_then(Value::as_str)?.to_string();
    let id = object
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or(id)
        .to_string();
    if id.is_empty() {
        return None;
    }
    let time_created = object
        .get("time")
        .and_then(Value::as_object)
        .and_then(|time| time.get("created"))
        .and_then(Value::as_i64)
        .or(fallback_ts)?;
    Some(OpencodeMessage {
        id,
        role,
        time_created,
        parent_id: object
            .get("parentID")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_string),
        provider_id: object
            .get("providerID")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_string),
        model_id: object
            .get("modelID")
            .and_then(Value::as_str)
            .or_else(|| {
                object
                    .get("model")
                    .and_then(Value::as_object)
                    .and_then(|model| model.get("modelID"))
                    .and_then(Value::as_str)
            })
            .filter(|value| !value.is_empty())
            .map(str::to_string),
        path_cwd: object
            .get("path")
            .and_then(Value::as_object)
            .and_then(|path| path.get("cwd"))
            .and_then(Value::as_str)
            .map(str::to_string),
        tokens: object.get("tokens").cloned(),
    })
}

/// Order everything the way the reference reader does: messages by creation
/// time, parts by part id. Both loaders end here, so the two layouts cannot
/// disagree about ordering even when the filesystem or SQLite hands rows back
/// in a different sequence.
fn finish(
    info: OpencodeSessionInfo,
    mut messages: Vec<OpencodeMessage>,
    mut parts_by_message: BTreeMap<String, Vec<OpencodePart>>,
) -> OpencodeSession {
    messages.sort_by(|a, b| {
        a.time_created
            .cmp(&b.time_created)
            .then_with(|| a.id.cmp(&b.id))
    });
    for parts in parts_by_message.values_mut() {
        parts.sort_by(|a, b| a.id.cmp(&b.id));
    }
    OpencodeSession {
        session: info,
        messages,
        parts_by_message,
    }
}

/// `"<providerID>/<modelID>"`, degrading to whichever half the provider
/// recorded. Ported from burn's `build_model`.
fn build_model(provider_id: Option<&str>, model_id: Option<&str>) -> Option<String> {
    match (provider_id, model_id) {
        (Some(provider), Some(model)) if !provider.is_empty() && !model.is_empty() => {
            Some(format!("{provider}/{model}"))
        }
        (_, Some(model)) if !model.is_empty() => Some(model.to_string()),
        (Some(provider), _) if !provider.is_empty() => Some(provider.to_string()),
        _ => None,
    }
}

struct ToolPart<'a> {
    call_id: &'a str,
    tool: &'a str,
    state: Option<&'a Map<String, Value>>,
}

fn as_tool_part(part: &OpencodePart) -> Option<ToolPart<'_>> {
    if part.kind != "tool" {
        return None;
    }
    let call_id = part.get("callID")?.as_str()?;
    if call_id.is_empty() {
        return None;
    }
    let tool = part.get("tool")?.as_str()?;
    Some(ToolPart {
        call_id,
        tool,
        state: part.get("state").and_then(Value::as_object),
    })
}

/// Which input field names the thing a tool acted on. Ported verbatim from
/// burn's `pick_target`.
fn pick_target(name: &str, input: &Value) -> Option<String> {
    let object = input.as_object()?;
    let field = |key: &str| -> Option<String> {
        object.get(key).and_then(Value::as_str).map(str::to_string)
    };
    match name {
        "read" | "write" | "edit" => field("filePath")
            .or_else(|| field("file_path"))
            .or_else(|| field("path")),
        "bash" => field("command"),
        "grep" | "glob" => field("pattern"),
        "webfetch" => field("url"),
        "task" => field("subagent_type")
            .or_else(|| field("description"))
            .or_else(|| field("prompt")),
        _ => field("filePath")
            .or_else(|| field("file_path"))
            .or_else(|| field("path"))
            .or_else(|| field("url"))
            .or_else(|| field("command")),
    }
}

/// Tools whose target is a file this session edited. `read` is a file tool in
/// burn's `files_touched` sense but does not produce a `file_edits` row here:
/// that table records edits, and the issue names `write`/`edit`/`patch`.
fn is_file_edit_tool(name: &str) -> bool {
    matches!(name, "write" | "edit" | "patch")
}

/// A tool failed when the provider says so, either through `state.status` or
/// through a non-zero process exit in `state.metadata.exit`. Ported verbatim
/// from burn's `is_failed_tool`.
fn is_failed_tool(state: Option<&Map<String, Value>>) -> bool {
    let Some(state) = state else {
        return false;
    };
    if state.get("status").and_then(Value::as_str) == Some("error") {
        return true;
    }
    let exit = state
        .get("metadata")
        .and_then(Value::as_object)
        .and_then(|metadata| metadata.get("exit"))
        .and_then(Value::as_i64);
    matches!(exit, Some(code) if code != 0)
}

/// The reason on the message's *last* `step-finish` part. A turn can take
/// several steps; only the last one says why the turn ended.
fn last_step_finish_reason(parts: &[OpencodePart]) -> Option<String> {
    parts.iter().rev().find_map(|part| {
        (part.kind == "step-finish")
            .then(|| part.get("reason").and_then(Value::as_str))
            .flatten()
            .map(str::to_string)
    })
}

/// Text a part carries, unless the provider marked it `synthetic` — synthetic
/// text is the harness talking to itself, not the transcript.
fn part_text(part: &OpencodePart) -> Option<&str> {
    if part.kind != "text" {
        return None;
    }
    if part.get("synthetic").and_then(Value::as_bool) == Some(true) {
        return None;
    }
    part.get("text")
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
}

/// A tool result payload as a string: provider outputs are usually strings but
/// are not required to be.
fn tool_output_text(output: &Value) -> Option<String> {
    match output {
        Value::Null => None,
        Value::String(text) => Some(text.clone()),
        other => serde_json::to_string(other).ok(),
    }
}

// ---------------------------------------------------------------------------
// Normalizer: one session's evidence
// ---------------------------------------------------------------------------

/// What one `normalize` pass wrote, for the callers that report row counts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct OpencodeIngestCounts {
    pub prompts: usize,
    pub events: usize,
    pub tool_calls: usize,
    pub file_edits: usize,
    pub markers: usize,
}

/// Write one OpenCode session's evidence into the catalog.
///
/// `raw_path` is the provenance recorded on the session row: the store path
/// for SQLite, the session file for the JSON tree. Everything else comes from
/// the loaded session, so the two layouts produce identical rows apart from
/// that one field.
pub(crate) fn normalize(
    conn: &Connection,
    loaded: &OpencodeSession,
    raw_path: &str,
) -> Result<OpencodeIngestCounts> {
    // One session, one atomic replacement. Reading a session is a read of its
    // *current* files, so it has to end with the database agreeing — and the
    // half-state between "the retired rows are gone" and "the new ones are
    // written" must never be visible to a reader.
    //
    // A savepoint rather than a transaction, because the callers differ:
    // hydration already holds one (a nested `BEGIN` would fail), global sync
    // does not. A savepoint nests either way.
    conn.execute_batch("SAVEPOINT ai_hist_opencode_session")?;
    let result = normalize_session(conn, loaded, raw_path);
    if result.is_ok() {
        conn.execute_batch("RELEASE ai_hist_opencode_session")?;
    } else {
        let _ = conn.execute_batch(
            "ROLLBACK TO ai_hist_opencode_session; RELEASE ai_hist_opencode_session;",
        );
    }
    result
}

/// The provider-owned rows this read produced, by each table's own stable key.
///
/// What is *not* in here is what the session's files no longer say, and that
/// is the point: see [`retire_absent_rows`].
#[derive(Debug, Default)]
struct OpencodeSnapshotKeys {
    events: BTreeSet<String>,
    tool_calls: BTreeSet<String>,
    file_edits: BTreeSet<String>,
    markers: BTreeSet<String>,
    /// `timestamp_ms:prompt_hash` — `history`'s identity within one session.
    prompts: BTreeSet<String>,
}

fn normalize_session(
    conn: &Connection,
    loaded: &OpencodeSession,
    raw_path: &str,
) -> Result<OpencodeIngestCounts> {
    let mut counts = OpencodeIngestCounts::default();
    let mut keys = OpencodeSnapshotKeys::default();
    let session_id = loaded.session.id.as_str();
    let project = loaded
        .messages
        .iter()
        .find_map(|message| message.path_cwd.clone())
        .or_else(|| loaded.session.directory.clone());

    let first_ts = loaded
        .messages
        .first()
        .map(|message| message.time_created)
        .or(loaded.session.created_ms)
        .unwrap_or(0);
    // The later of the two, which is what discovery's shallow read already
    // computes for the same session. Taking the newest message whenever one
    // exists is right for the usual case -- OpenCode appends a turn without
    // rewriting the session JSON, so `updated` lags it -- but it is not
    // symmetric: a session whose JSON is *ahead* of its messages then has its
    // catalog recency moved backwards by a successful hydration, because
    // hydration writes through here and nothing else. Two paths deciding the
    // same field differently is the drift; neither value supersedes the
    // other, so the later one stands and either alone stands when the other
    // is absent.
    let last_ts = match (
        loaded.messages.last().map(|message| message.time_created),
        loaded.session.updated_ms,
    ) {
        (Some(newest), Some(updated)) => newest.max(updated),
        (Some(newest), None) => newest,
        (None, Some(updated)) => updated,
        (None, None) => first_ts,
    };

    let mut last_assistant_text: Option<String> = None;

    for message in &loaded.messages {
        let parts = loaded.parts(&message.id);
        let model = build_model(message.provider_id.as_deref(), message.model_id.as_deref());
        let token_json = message
            .tokens
            .as_ref()
            .and_then(|tokens| serde_json::to_string(tokens).ok());
        let stop_reason = last_step_finish_reason(parts);
        let message_project = message.path_cwd.clone().or_else(|| project.clone());

        if message.role == "user" {
            for part in parts {
                if part.kind == "compaction" {
                    // A compaction part sits on the user message OpenCode
                    // inserts at the boundary; the marker is the boundary
                    // itself, not a turn.
                    let marker_uid = format!("part:{}", part.id);
                    counts.markers += insert_session_marker(
                        conn,
                        session_id,
                        OPENCODE_MARKER_COMPACTION_BOUNDARY,
                        Some(&message.id),
                        message.time_created,
                        part.raw.get("auto").and_then(Value::as_bool),
                        &marker_uid,
                    )?;
                    keys.markers.insert(marker_uid);
                    continue;
                }
                let Some(text) = part_text(part) else {
                    continue;
                };
                let event_uid = format!("text:{}", part.id);
                insert_session_event_with_provenance(
                    conn,
                    "opencode",
                    session_id,
                    message_project.as_deref(),
                    message_project.as_deref(),
                    None,
                    &message.id,
                    message.parent_id.as_deref(),
                    message.time_created,
                    "user",
                    "text",
                    Some(text),
                    None,
                    None,
                    None,
                    None,
                    &event_uid,
                    None,
                    RawMessageFacts::default(),
                )?;
                keys.events.insert(event_uid);
                counts.events += 1;
                // The prompt ledger predates session events and other tools
                // read it; keep writing it from the same parse.
                let trimmed = text.trim();
                if !trimmed.is_empty() {
                    let hash = prompt_hash(trimmed);
                    keys.prompts
                        .insert(format!("{}:{hash}", message.time_created));
                    counts.prompts += insert_history(
                        conn,
                        &HistoryEntry {
                            id: 0,
                            source: "opencode".into(),
                            session_id: Some(session_id.to_string()),
                            project: message_project.clone(),
                            prompt: trimmed.to_string(),
                            prompt_hash: Some(hash),
                            timestamp_ms: message.time_created,
                        },
                    )?;
                }
            }
            continue;
        }

        if message.role != "assistant" {
            continue;
        }

        // OpenCode persists a tool call several times as it progresses, once
        // per state, each as its own part sharing the `callID`. Only the last
        // of them is the finished call: it carries the output, the final
        // arguments and the failure status. Taking the first would store a
        // call that looks successful and has no result. `finish` has already
        // ordered the parts, so "last" here is the provider's own order.
        let final_part_for_call = last_part_index_per_call(parts);
        for (index, part) in parts.iter().enumerate() {
            if let Some(text) = part_text(part) {
                let event_uid = format!("text:{}", part.id);
                insert_session_event_with_provenance(
                    conn,
                    "opencode",
                    session_id,
                    message_project.as_deref(),
                    message_project.as_deref(),
                    None,
                    &message.id,
                    message.parent_id.as_deref(),
                    message.time_created,
                    "assistant",
                    "text",
                    Some(text),
                    model.as_deref(),
                    token_json.as_deref(),
                    message.provider_id.as_deref(),
                    stop_reason.as_deref(),
                    &event_uid,
                    None,
                    RawMessageFacts::default(),
                )?;
                keys.events.insert(event_uid);
                counts.events += 1;
                // `sessions.last_assistant_text` is a catalog *excerpt*, not
                // the turn: it is read to show a session in a list, and every
                // other full-ingest parser caps it here. The event above keeps
                // the whole text, which is where a reader that wants the turn
                // goes.
                last_assistant_text = Some(
                    text.chars()
                        .take(crate::discover::EXCERPT_MAX_CHARS)
                        .collect(),
                );
                continue;
            }
            let Some(tool) = as_tool_part(part) else {
                continue;
            };
            if final_part_for_call.get(tool.call_id) != Some(&index) {
                continue;
            }
            let input = tool
                .state
                .and_then(|state| state.get("input"))
                .cloned()
                .filter(|value| value.is_object())
                .unwrap_or_else(|| Value::Object(Map::new()));
            let target = pick_target(tool.tool, &input);
            let event_text = super::format_tool_event_text(tool.tool, target.as_deref(), &input);
            let args_json = serde_json::to_string(&input).unwrap_or_else(|_| "{}".into());
            let is_error = is_failed_tool(tool.state);

            insert_session_event_with_provenance(
                conn,
                "opencode",
                session_id,
                message_project.as_deref(),
                message_project.as_deref(),
                None,
                &message.id,
                message.parent_id.as_deref(),
                message.time_created,
                "assistant",
                "tool_use",
                Some(&event_text),
                model.as_deref(),
                token_json.as_deref(),
                message.provider_id.as_deref(),
                stop_reason.as_deref(),
                &format!("tool_use:{}", tool.call_id),
                None,
                RawMessageFacts::default(),
            )?;
            keys.events.insert(format!("tool_use:{}", tool.call_id));
            keys.tool_calls.insert(tool.call_id.to_string());
            counts.events += 1;
            insert_tool_call(
                conn,
                "opencode",
                session_id,
                &message.id,
                tool.call_id,
                tool.tool,
                target.as_deref(),
                &args_json,
                Some(is_error),
                message.time_created,
            )?;
            counts.tool_calls += 1;

            if is_file_edit_tool(tool.tool) {
                if let Some(file_path) = target.as_deref() {
                    upsert_file_edit_from_call(
                        conn,
                        "opencode",
                        session_id,
                        &message.id,
                        tool.call_id,
                        file_path,
                        tool.tool,
                        message.time_created,
                        None,
                        message_project.as_deref(),
                    )?;
                    keys.file_edits.insert(tool.call_id.to_string());
                    counts.file_edits += 1;
                }
            }

            // A tool part with an `output` key is terminal: the call finished
            // and its result is what the model saw next.
            if let Some(output) = tool.state.and_then(|state| state.get("output")) {
                if let Some(text) = tool_output_text(output) {
                    insert_session_event_with_provenance(
                        conn,
                        "opencode",
                        session_id,
                        message_project.as_deref(),
                        message_project.as_deref(),
                        None,
                        &message.id,
                        message.parent_id.as_deref(),
                        message.time_created,
                        "tool_result",
                        "tool_result",
                        Some(&text),
                        model.as_deref(),
                        token_json.as_deref(),
                        message.provider_id.as_deref(),
                        stop_reason.as_deref(),
                        &format!("tool_result:{}", tool.call_id),
                        None,
                        RawMessageFacts::default(),
                    )?;
                    keys.events.insert(format!("tool_result:{}", tool.call_id));
                    counts.events += 1;
                }
            }
        }
    }

    upsert_session(
        conn,
        session_id,
        "opencode",
        project.as_deref(),
        None,
        first_ts,
        last_ts,
        last_assistant_text.as_deref(),
        Some(raw_path),
    )?;

    // `session.parentID` names the parent outright, so the child identity is
    // observed rather than inferred — the one provider where that is true.
    if let Some(parent) = loaded.session.parent_id.as_deref() {
        let child_model = loaded
            .messages
            .iter()
            .find(|message| message.role == "assistant")
            .and_then(|message| {
                build_model(message.provider_id.as_deref(), message.model_id.as_deref())
            });
        record_relationship(
            conn,
            &ObservedRelationship {
                source: "opencode",
                parent_session_id: parent,
                child_session_id: Some(session_id),
                relationship: "delegated",
                child_agent_type: None,
                child_agent_name: None,
                child_model: child_model.as_deref(),
                spawn_depth: None,
                evidence_kind: "opencode_parent_id",
                evidence_locator: Some(raw_path),
                evidence_ref: Some(session_id),
                child_has_events: counts.events > 0,
                spawned_at_ms: loaded.session.created_ms.or(Some(first_ts)),
                ..ObservedRelationship::default()
            },
        )?;
    }

    retire_absent_rows(conn, session_id, &keys, loaded.session.parent_id.as_deref())?;
    Ok(counts)
}

/// Remove the provider-owned rows this session's files no longer produce.
///
/// A read of an OpenCode session is a read of the session as it is *now*.
/// OpenCode rewrites a part in place — a tool part loses its `state.output`, a
/// text part is edited, a `parentID` is dropped — and it removes files. An
/// upsert-only ingest has no way to express any of that: the replacement row
/// is simply never emitted, the superseded row keeps its uniqueness key, and a
/// reader cannot tell it from a live one. `get_session_events` would go on
/// returning a tool result whose output the provider deleted, which is the
/// worst shape of this bug: not stale metadata, but content that was taken
/// away and is still being served.
///
/// Deleted by absence from the new snapshot rather than by clearing the
/// session first, which matters beyond elegance: every unchanged row keeps its
/// rowid, so a resync of an unchanged session writes nothing, and the delivery
/// capture triggers emit nothing. OpenCode's global sync re-reads every session
/// every run, so clear-and-rewrite would churn the outbox for the whole store
/// on every sync.
///
/// Scope is narrow. Only `source = 'opencode'` rows belonging to this session,
/// by each table's own stable key. The relationship is keyed on the **child**,
/// because the row a session's file owns is the one naming *its* parent —
/// keying on the parent would delete the links this session's own children
/// recorded about themselves.
fn retire_absent_rows(
    conn: &Connection,
    session_id: &str,
    keys: &OpencodeSnapshotKeys,
    parent_id: Option<&str>,
) -> Result<usize> {
    let mut retired = 0;
    for (sql, present) in [
        (
            "DELETE FROM session_events WHERE source = 'opencode' AND session_id = ?1 \
             AND event_uid NOT IN (SELECT value FROM json_each(?2))",
            &keys.events,
        ),
        (
            "DELETE FROM tool_calls WHERE source = 'opencode' AND session_id = ?1 \
             AND tool_use_id NOT IN (SELECT value FROM json_each(?2))",
            &keys.tool_calls,
        ),
        (
            "DELETE FROM file_edits WHERE source = 'opencode' AND session_id = ?1 \
             AND tool_use_id NOT IN (SELECT value FROM json_each(?2))",
            &keys.file_edits,
        ),
        (
            "DELETE FROM session_markers WHERE source = 'opencode' AND session_id = ?1 \
             AND marker_uid NOT IN (SELECT value FROM json_each(?2))",
            &keys.markers,
        ),
        (
            "DELETE FROM history WHERE source = 'opencode' AND session_id = ?1 \
             AND (timestamp_ms || ':' || coalesce(prompt_hash, '')) \
             NOT IN (SELECT value FROM json_each(?2))",
            &keys.prompts,
        ),
    ] {
        let present = serde_json::to_string(present).unwrap_or_else(|_| "[]".into());
        retired += conn.execute(sql, params![session_id, present])?;
    }
    retired += match parent_id {
        // The link is still asserted, but possibly to a different parent than
        // the one recorded before.
        Some(parent) => conn.execute(
            "DELETE FROM session_relationships WHERE source = 'opencode' \
             AND child_session_id = ?1 AND evidence_kind = 'opencode_parent_id' \
             AND parent_session_id <> ?2",
            params![session_id, parent],
        )?,
        None => conn.execute(
            "DELETE FROM session_relationships WHERE source = 'opencode' \
             AND child_session_id = ?1 AND evidence_kind = 'opencode_parent_id'",
            params![session_id],
        )?,
    };
    Ok(retired)
}

/// Where each `callID`'s last part sits in this message's ordered parts.
///
/// One entry per call, so a call persisted five times still produces one
/// `tool_calls` row, one `tool_use` event and at most one `tool_result` — from
/// its final state.
fn last_part_index_per_call(parts: &[OpencodePart]) -> BTreeMap<&str, usize> {
    let mut last = BTreeMap::new();
    for (index, part) in parts.iter().enumerate() {
        if let Some(tool) = as_tool_part(part) {
            last.insert(tool.call_id, index);
        }
    }
    last
}

/// Record one session-level marker. Returns how many rows the write added, so
/// re-ingesting the same session does not inflate the count.
fn insert_session_marker(
    conn: &Connection,
    session_id: &str,
    kind: &str,
    message_id: Option<&str>,
    ts_ms: i64,
    auto: Option<bool>,
    marker_uid: &str,
) -> Result<usize> {
    crate::mark_session_presence(conn, "opencode", session_id, crate::SessionLocation::Local)?;
    let detail_json = auto.map(|auto| format!("{{\"auto\":{auto}}}"));
    Ok(conn.execute(
        "INSERT INTO session_markers \
         (source, session_id, kind, message_id, ts_ms, detail_json, marker_uid) \
         VALUES ('opencode', ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(source, session_id, marker_uid) DO UPDATE SET \
         kind=excluded.kind, message_id=excluded.message_id, ts_ms=excluded.ts_ms, \
         detail_json=COALESCE(excluded.detail_json, session_markers.detail_json)",
        params![session_id, kind, message_id, ts_ms, detail_json, marker_uid],
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_model_degrades_to_whichever_half_the_provider_recorded() {
        assert_eq!(
            build_model(Some("anthropic"), Some("claude-opus-4-5")).as_deref(),
            Some("anthropic/claude-opus-4-5")
        );
        assert_eq!(build_model(None, Some("gpt-5")).as_deref(), Some("gpt-5"));
        assert_eq!(build_model(Some("openai"), None).as_deref(), Some("openai"));
        assert_eq!(build_model(None, None), None);
    }

    #[test]
    fn a_nonzero_process_exit_marks_a_completed_tool_as_failed() {
        let ok: Map<String, Value> =
            serde_json::from_str(r#"{"status":"completed","metadata":{"exit":0}}"#).unwrap();
        let failed: Map<String, Value> =
            serde_json::from_str(r#"{"status":"completed","metadata":{"exit":1}}"#).unwrap();
        let errored: Map<String, Value> = serde_json::from_str(r#"{"status":"error"}"#).unwrap();
        assert!(!is_failed_tool(Some(&ok)));
        assert!(is_failed_tool(Some(&failed)));
        assert!(is_failed_tool(Some(&errored)));
        assert!(!is_failed_tool(None));
    }

    #[test]
    fn the_last_step_finish_reason_wins_over_earlier_steps() {
        let parts = vec![
            part(r#"{"id":"p1","type":"step-finish","reason":"tool-calls"}"#),
            part(r#"{"id":"p2","type":"step-finish","reason":"end_turn"}"#),
        ];
        assert_eq!(last_step_finish_reason(&parts).as_deref(), Some("end_turn"));
    }

    #[test]
    fn synthetic_text_is_not_transcript() {
        assert_eq!(
            part_text(&part(r#"{"id":"p","type":"text","text":"real"}"#)),
            Some("real")
        );
        assert_eq!(
            part_text(&part(
                r#"{"id":"p","type":"text","text":"generated","synthetic":true}"#
            )),
            None
        );
    }

    /// A provider store with the fixture schema, and optionally the indexes
    /// a real OpenCode install ships.
    fn store(indexed: bool) -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE session (id TEXT PRIMARY KEY, parent_id TEXT, directory TEXT,
                                   time_created INTEGER, time_updated INTEGER);
             CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT,
                                   time_created INTEGER, data TEXT);
             CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT,
                                time_created INTEGER, data TEXT);",
        )
        .unwrap();
        if indexed {
            conn.execute_batch(
                "CREATE INDEX part_session_idx ON part (session_id);
                 CREATE INDEX message_session_idx ON message (session_id, time_created, id);",
            )
            .unwrap();
        }
        conn
    }

    /// What SQLite says it will do for the per-session part predicate. The
    /// plan choice is only worth making if it tracks this.
    fn part_by_session_plan(conn: &Connection) -> String {
        conn.query_row(
            "EXPLAIN QUERY PLAN SELECT id, message_id, data FROM part WHERE session_id = ?",
            ["x"],
            |row| row.get::<_, String>(3),
        )
        .unwrap()
    }

    #[test]
    fn an_unindexed_store_is_swept_in_one_pass_not_once_per_session() {
        // The provider's own index makes the per-session predicate a seek...
        let indexed = store(true);
        assert!(
            part_by_session_plan(&indexed).contains("SEARCH"),
            "with the index the predicate must be a seek: {}",
            part_by_session_plan(&indexed)
        );
        assert_eq!(sync_plan(&indexed).unwrap(), OpencodeSyncPlan::PerSession);

        // ...and without it, a scan -- which global sync would pay once per
        // session. That is the whole reason the plan differs.
        let bare = store(false);
        assert!(
            part_by_session_plan(&bare).contains("SCAN"),
            "without the index the predicate must be a scan: {}",
            part_by_session_plan(&bare)
        );
        assert_eq!(sync_plan(&bare).unwrap(), OpencodeSyncPlan::SinglePass);
    }

    #[test]
    fn messages_by_session_and_parts_by_message_are_also_seekable() {
        // The other indexed shape: no `part(session_id)`, but the pair that
        // lets a session be reached through its messages.
        let conn = store(false);
        conn.execute_batch(
            "CREATE INDEX message_session_idx ON message (session_id);
             CREATE INDEX part_message_idx ON part (message_id);",
        )
        .unwrap();
        assert_eq!(sync_plan(&conn).unwrap(), OpencodeSyncPlan::PerSession);
    }

    #[test]
    fn a_leading_index_is_what_counts_not_merely_being_mentioned() {
        let conn = store(false);
        // `session_id` is in this index, but not first, so an equality
        // predicate on it alone still cannot seek.
        conn.execute_batch("CREATE INDEX part_trailing ON part (time_created, session_id);")
            .unwrap();
        assert!(!has_leading_index(&conn, "part", "session_id").unwrap());
        assert_eq!(sync_plan(&conn).unwrap(), OpencodeSyncPlan::SinglePass);
    }

    /// A minimal legacy tree: one session, one assistant message, one text
    /// part. Returns the tree root and the part's path.
    fn tree(dir: &Path) -> (PathBuf, PathBuf) {
        let root = dir.join("storage");
        let write = |path: PathBuf, body: &str| {
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, body).unwrap();
            path
        };
        write(
            root.join("session/global/ses_io.json"),
            r#"{"id":"ses_io","directory":"/tmp/p","time":{"created":1,"updated":2}}"#,
        );
        write(
            root.join("message/ses_io/msg_io.json"),
            r#"{"id":"msg_io","sessionID":"ses_io","role":"assistant","time":{"created":2}}"#,
        );
        let part = write(
            root.join("part/msg_io/prt_io.json"),
            r#"{"id":"prt_io","sessionID":"ses_io","messageID":"msg_io","type":"text","text":"aaa"}"#,
        );
        (root, part)
    }

    /// Replace `path` with something that still enumerates and still stats but
    /// cannot be read.
    ///
    /// A unix socket, and not the obvious alternatives: `chmod 000` does
    /// nothing when the suite runs as root, which it does here, and a
    /// directory in place of the file is not a `.json` file to the scan at
    /// all. `open(2)` on a socket fails with `ENXIO` whatever the uid while
    /// `metadata()` still succeeds — which is exactly the hazard's shape, a
    /// path that looks present and unchanged to every check made before the
    /// read. (The technique is #190's; its rationale is quoted because it is
    /// the reason the obvious fixtures are false greens.)
    #[cfg(unix)]
    fn make_unreadable(path: &Path) {
        fs::remove_file(path).unwrap();
        // Leaked deliberately: dropping the listener would not remove the
        // socket file, and the file is what the test needs.
        std::mem::forget(std::os::unix::net::UnixListener::bind(path).unwrap());
        assert!(
            fs::read_to_string(path).is_err(),
            "the fixture must actually be unreadable or this test proves nothing"
        );
        assert!(
            fs::metadata(path).is_ok(),
            "the fixture must still stat, or the loader would never reach the read"
        );
    }

    #[cfg(unix)]
    #[test]
    fn an_unreadable_part_is_an_error_not_a_session_without_it() {
        let dir = tempfile::tempdir().unwrap();
        let (root, part) = tree(dir.path());
        let session_file = root.join("session/global/ses_io.json");

        // Control: while everything is readable the part is there.
        let loaded = load_from_json_tree(&session_file).unwrap().unwrap();
        assert_eq!(loaded.parts_by_message["msg_io"].len(), 1);

        make_unreadable(&part);
        let error = format!(
            "{:#}",
            load_from_json_tree(&session_file).expect_err(
                "an unreadable part must be an error, not a session recorded without it"
            )
        );
        assert!(
            error.contains("prt_io.json"),
            "the error must name the file that failed, got {error}"
        );

        // And it recovers: nothing about the failure is remembered.
        fs::remove_file(&part).unwrap();
        fs::write(
            &part,
            r#"{"id":"prt_io","sessionID":"ses_io","messageID":"msg_io","type":"text","text":"aaa"}"#,
        )
        .unwrap();
        let loaded = load_from_json_tree(&session_file).unwrap().unwrap();
        assert_eq!(loaded.parts_by_message["msg_io"].len(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn an_unreadable_session_file_is_an_error_not_a_missing_session() {
        let dir = tempfile::tempdir().unwrap();
        let (root, _) = tree(dir.path());
        let session_file = root.join("session/global/ses_io.json");
        make_unreadable(&session_file);
        let error = format!(
            "{:#}",
            load_from_json_tree(&session_file)
                .expect_err("an unreadable session file must not read as 'not a session'")
        );
        assert!(
            error.contains("ses_io.json"),
            "the error must name the file that failed, got {error}"
        );
    }

    /// The other half of the same rule: what is *skippable* still skips. A
    /// malformed record and an absent directory are ordinary, and turning
    /// those into errors would trade a silent omission for a loud one.
    #[test]
    fn malformed_and_absent_records_are_still_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let (root, part) = tree(dir.path());
        let session_file = root.join("session/global/ses_io.json");

        fs::write(&part, "{ not json").unwrap();
        let loaded = load_from_json_tree(&session_file).unwrap().unwrap();
        assert!(
            loaded.parts_by_message.is_empty(),
            "a malformed part is skipped, not fatal"
        );

        fs::remove_dir_all(root.join("part/msg_io")).unwrap();
        fs::remove_dir_all(root.join("message/ses_io")).unwrap();
        let loaded = load_from_json_tree(&session_file).unwrap().unwrap();
        assert!(loaded.messages.is_empty(), "a session may have no messages");

        fs::write(&session_file, "{ not json").unwrap();
        assert!(
            load_from_json_tree(&session_file).unwrap().is_none(),
            "a malformed session file is not a session"
        );

        fs::remove_file(&session_file).unwrap();
        assert!(
            load_from_json_tree(&session_file).unwrap().is_none(),
            "a file that vanished between the stat and the read is a race, not an outage"
        );
    }

    /// Pin `path`'s modification time, leaving its contents alone.
    fn pin_mtime(path: &Path, when: std::time::SystemTime) {
        let file = fs::OpenOptions::new().write(true).open(path).unwrap();
        file.set_times(fs::FileTimes::new().set_modified(when))
            .unwrap();
        assert_eq!(fs::metadata(path).unwrap().modified().unwrap(), when);
    }

    #[test]
    fn a_same_length_rewrite_in_one_mtime_tick_still_moves_the_stamp() {
        let dir = tempfile::tempdir().unwrap();
        let (root, part) = tree(dir.path());
        let session_file = root.join("session/global/ses_io.json");

        let before = stamp_json_tree_session(&session_file, "ses_io").unwrap();
        let pinned = fs::metadata(&part).unwrap().modified().unwrap();
        // Premise: this file is recent enough that its next write could share
        // this mtime. That is the only case the content fold exists for, and a
        // slow machine must fail here rather than pass for another reason.
        assert!(
            std::time::SystemTime::now()
                .duration_since(pinned)
                .unwrap_or_default()
                < std::time::Duration::from_secs(1),
            "the fixture must still be inside the ambiguity window"
        );

        // Control: stamping again without touching anything is stable, or an
        // assertion that the stamp *moved* would mean nothing.
        assert_eq!(
            before.token(),
            stamp_json_tree_session(&session_file, "ses_io")
                .unwrap()
                .token(),
            "an untouched tree must stamp the same twice"
        );

        let original = fs::read_to_string(&part).unwrap();
        let rewritten = original.replace("\"aaa\"", "\"bbb\"");
        assert_eq!(rewritten.len(), original.len());
        assert_ne!(rewritten, original);
        fs::write(&part, &rewritten).unwrap();
        pin_mtime(&part, pinned);

        let after = stamp_json_tree_session(&session_file, "ses_io").unwrap();
        assert_eq!(
            (before.bytes, before.files, before.newest_ns),
            (after.bytes, after.files, after.newest_ns),
            "precondition: every aggregate must be unchanged, or the content \
             fold is not what caught this"
        );
        assert_ne!(
            before.token(),
            after.token(),
            "a same-length rewrite inside one mtime tick must still move the stamp"
        );
    }

    /// The content fold has to stay off the hot path: discovery stamps every
    /// session in the tree, and reading all of them on every run would be a
    /// different bug. A settled file is stat-only; a recent one is read.
    /// Measured, because "it should be cheap" is not a bound.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_settled_tree_is_stamped_without_reading_it() {
        /// Bytes the *calling thread* has read.
        ///
        /// `/proc/self/io` counts the whole process, and `cargo test` runs a
        /// crate's unit tests as parallel threads in one process — so a sample
        /// taken from it includes whatever every other test happened to read
        /// in between. That is not a flake to widen the bound for: it made
        /// this test red in CI at `73998 bytes for a 262225-byte part`, and
        /// none of those bytes were this test's. It passed on earlier heads
        /// only because the interleaving happened to be quiet, which is the
        /// worse half of the problem.
        ///
        /// `/proc/thread-self/io` is the same counters for this thread alone
        /// (Linux 3.17+), so the sample measures the work under test and
        /// nothing else.
        fn thread_bytes_read() -> u64 {
            rchar("/proc/thread-self/io")
        }
        /// The process-wide counter, kept only as the control below.
        fn process_bytes_read() -> u64 {
            rchar("/proc/self/io")
        }
        fn rchar(path: &str) -> u64 {
            fs::read_to_string(path)
                .unwrap()
                .lines()
                .find_map(|line| line.strip_prefix("rchar:"))
                .and_then(|value| value.trim().parse().ok())
                .unwrap()
        }

        let dir = tempfile::tempdir().unwrap();
        let (root, part) = tree(dir.path());
        let session_file = root.join("session/global/ses_io.json");
        let body = format!(
            "{{\"id\":\"prt_io\",\"sessionID\":\"ses_io\",\"messageID\":\"msg_io\",\
             \"type\":\"text\",\"text\":\"{}\"}}",
            "x".repeat(256 * 1024)
        );
        fs::write(&part, &body).unwrap();
        let size = body.len() as u64;

        // Control for the probe itself: the stamp's own reads must be visible
        // to it, or "it read nothing" would be worth nothing.
        let recent = std::time::SystemTime::now();
        pin_mtime(&part, recent);
        let before = thread_bytes_read();
        stamp_json_tree_session(&session_file, "ses_io").unwrap();
        let while_recent = thread_bytes_read() - before;

        // The measurement under test, taken with a neighbour thread reading
        // between the two samples on purpose -- that interleaving is exactly
        // what a process-wide counter cannot tell apart from the stamp's own
        // reads.
        let noise = dir.path().join("noise.bin");
        let noise_bytes = 4 * 1024 * 1024u64;
        fs::write(&noise, vec![0u8; noise_bytes as usize]).unwrap();

        pin_mtime(&part, recent - std::time::Duration::from_secs(600));
        let before_thread = thread_bytes_read();
        let before_process = process_bytes_read();
        std::thread::spawn(move || fs::read(&noise).unwrap())
            .join()
            .unwrap();
        stamp_json_tree_session(&session_file, "ses_io").unwrap();
        let once_settled = thread_bytes_read() - before_thread;
        let neighbour = process_bytes_read() - before_process;

        assert!(
            while_recent >= size,
            "a file that could still be rewritten in this tick must be read: \
             {while_recent} bytes for a {size}-byte part"
        );
        assert!(
            once_settled < size / 4,
            "a settled file must be stat-only: {once_settled} bytes for a \
             {size}-byte part"
        );
        // Positive control for the isolation: the process-wide counter did
        // move, by the neighbour's whole file. Had the sample above been taken
        // from it, this test would have measured that instead.
        assert!(
            neighbour >= noise_bytes,
            "the neighbour must actually have read, or the isolation is \
             untested: {neighbour} bytes"
        );
    }

    #[test]
    fn a_failed_session_query_is_an_error_not_a_session_that_is_not_there() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE session (id TEXT PRIMARY KEY, parent_id, directory TEXT,
                                   time_created INTEGER, time_updated INTEGER);
             CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT,
                                   time_created INTEGER, data TEXT);
             CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT, session_id TEXT,
                                time_created INTEGER, data TEXT);",
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session (id, parent_id, directory, time_created, time_updated) \
             VALUES ('ses_ok', NULL, '/tmp/p', 1, 2)",
            [],
        )
        .unwrap();
        // SQLite columns are dynamically typed, so a provider can put a value
        // in `parent_id` that this row mapping cannot take. That is a query
        // failure, not a missing session.
        conn.execute(
            "INSERT INTO session (id, parent_id, directory, time_created, time_updated) \
             VALUES ('ses_bad', x'ff', '/tmp/p', 1, 2)",
            [],
        )
        .unwrap();

        // Control: a session that is genuinely absent is still `None`, and a
        // well-formed one still loads — or "it errored" would mean nothing.
        assert!(load_from_sqlite(&conn, "ses_missing").unwrap().is_none());
        assert!(load_from_sqlite(&conn, "ses_ok").unwrap().is_some());

        let error = format!(
            "{:#}",
            load_from_sqlite(&conn, "ses_bad")
                .expect_err("a failed query must not read as 'this session is not in the store'")
        );
        assert!(
            error.contains("ses_bad"),
            "the error must name the session that failed, got {error}"
        );
    }

    #[test]
    fn a_session_directory_that_cannot_be_listed_is_not_an_empty_one() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("storage");
        fs::create_dir_all(root.join("session/global")).unwrap();
        fs::write(
            root.join("session/global/ses_here.json"),
            r#"{"id":"ses_here"}"#,
        )
        .unwrap();

        // Control: a tree that lists cleanly reports no failures, and a
        // `session/` that is simply not there is empty rather than broken.
        let listing = list_json_tree_session_files(&root);
        assert_eq!(listing.sessions.len(), 1);
        assert!(listing.unreadable.is_empty());
        let absent = list_json_tree_session_files(&dir.path().join("no-such-storage"));
        assert!(absent.sessions.is_empty() && absent.unreadable.is_empty());

        // A `session` path that is not a directory: `read_dir` fails with
        // `ENOTDIR` for any uid. (`chmod 000` does not, because this suite
        // runs as root and keeps `CAP_DAC_READ_SEARCH`.)
        let other = dir.path().join("flat");
        fs::create_dir_all(&other).unwrap();
        fs::write(other.join("session"), "not a directory").unwrap();
        let listing = list_json_tree_session_files(&other);
        assert!(
            listing.sessions.is_empty(),
            "nothing is found, which is the whole difficulty"
        );
        assert_eq!(
            listing.unreadable.len(),
            1,
            "and the path that could not be walked must be reported, not dropped"
        );
        assert_eq!(listing.unreadable[0].path, other.join("session"));
    }

    fn part(raw: &str) -> OpencodePart {
        let Value::Object(object) = serde_json::from_str::<Value>(raw).unwrap() else {
            unreachable!("fixture is an object")
        };
        OpencodePart {
            id: object
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            kind: object
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            raw: object,
        }
    }
}
