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
    upsert_session, OPENCODE_MARKER_COMPACTION_BOUNDARY,
};
use crate::relationship_capture::{record_relationship, ObservedRelationship};
use crate::{insert_history, prompt_hash, HistoryEntry};
use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
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
        .ok();
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

/// Read the whole store in one pass per table and group it in memory.
///
/// The fallback for a store whose provider indexes are missing. It trades
/// memory for scans, which is the right way round here: the alternative this
/// replaced copied the entire database to a temporary file first, so holding
/// the rows costs no more than that did and reads each one exactly once.
pub(crate) fn load_all_from_sqlite(src: &Connection) -> Result<Vec<OpencodeSession>> {
    let session_columns = table_columns(src, "session")?;
    if !session_columns.contains("id") {
        return Ok(Vec::new());
    }
    let parent = optional_column(&session_columns, "parent_id");
    let directory = optional_column(&session_columns, "directory");
    let created = optional_column(&session_columns, "time_created");
    let updated = optional_column(&session_columns, "time_updated");
    let sql = format!(
        "SELECT id, {parent}, {directory}, {created}, {updated} FROM session \
         WHERE id IS NOT NULL AND id <> ''"
    );
    let mut infos: BTreeMap<String, OpencodeSessionInfo> = src
        .prepare(&sql)?
        .query_map([], |row| {
            Ok(OpencodeSessionInfo {
                id: row.get::<_, String>(0)?,
                parent_id: row.get::<_, Option<String>>(1)?,
                directory: row.get::<_, Option<String>>(2)?,
                created_ms: row.get::<_, Option<i64>>(3)?,
                updated_ms: row.get::<_, Option<i64>>(4)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .map(|mut info| {
            info.parent_id = info.parent_id.filter(|value| !value.is_empty());
            (info.id.clone(), info)
        })
        .collect();

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
        let rows = src
            .prepare(&sql)?
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
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
        let rows = src
            .prepare("SELECT id, message_id, data FROM part WHERE json_valid(data)")?
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
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

    let mut out = Vec::with_capacity(infos.len());
    let ids: Vec<String> = infos.keys().cloned().collect();
    for id in ids {
        let info = infos.remove(&id).expect("id came from this map");
        out.push(finish(
            info,
            messages_by_session.remove(&id).unwrap_or_default(),
            parts_by_session.remove(&id).unwrap_or_default(),
        ));
    }
    Ok(out)
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
pub(crate) fn list_json_tree_session_files(storage_root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    collect_session_files(&storage_root.join("session"), &mut out);
    out.sort();
    out
}

fn collect_session_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        if kind.is_dir() {
            collect_session_files(&path, out);
        } else if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("ses_") && name.ends_with(".json"))
        {
            out.push(path);
        }
    }
}

/// Read one session out of the JSON tree, given its session file.
pub(crate) fn load_from_json_tree(session_file: &Path) -> Result<Option<OpencodeSession>> {
    let Ok(raw) = fs::read_to_string(session_file) else {
        return Ok(None);
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
    for (message_id, object) in read_json_dir(&storage_root.join("message").join(&info.id)) {
        if let Some(message) = parse_message(&message_id, &object, None) {
            messages.push(message);
        }
    }

    let mut parts_by_message: BTreeMap<String, Vec<OpencodePart>> = BTreeMap::new();
    for message in &messages {
        for (part_id, object) in read_json_dir(&storage_root.join("part").join(&message.id)) {
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct OpencodeTreeStamp {
    pub bytes: u64,
    pub files: u64,
    /// Newest modification time across those files, in nanoseconds.
    pub newest_ns: u128,
}

impl OpencodeTreeStamp {
    /// The opaque token stored as a source stamp.
    pub fn token(&self) -> String {
        format!("{}:{}:{}", self.bytes, self.files, self.newest_ns)
    }

    /// Newest activity as epoch milliseconds, for a recency hint.
    pub fn newest_ms(&self) -> Option<i64> {
        i64::try_from(self.newest_ns / 1_000_000).ok()
    }
}

/// Stamp every file that composes one legacy-tree session: the session JSON,
/// its messages, and those messages' parts. Metadata only — nothing is read
/// or parsed, so this stays cheap enough for discovery to run over a tree.
pub(crate) fn stamp_json_tree_session(session_file: &Path, session_id: &str) -> OpencodeTreeStamp {
    let root = derive_storage_root(session_file);
    let mut stamp = OpencodeTreeStamp::default();
    let mut add = |path: &Path| {
        let Ok(meta) = fs::metadata(path) else { return };
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
    };
    add(session_file);
    let mut message_ids = json_file_stems(&root.join("message").join(session_id), &mut add);
    message_ids.sort();
    for message_id in &message_ids {
        json_file_stems(&root.join("part").join(message_id), &mut add);
    }
    stamp
}

/// Visit every `*.json` in one directory, returning their stems. Sorted by the
/// caller where order matters; a missing directory is simply empty.
fn json_file_stems(dir: &Path, visit: &mut impl FnMut(&Path)) -> Vec<String> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut paths: Vec<PathBuf> = entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("json"))
        .collect();
    paths.sort();
    let mut stems = Vec::new();
    for path in &paths {
        visit(path);
        if let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) {
            stems.push(stem.to_string());
        }
    }
    stems
}

/// `*.json` in one directory, keyed by the payload's own `id` when it has one
/// and by the file stem otherwise. Unreadable or non-object files are skipped,
/// exactly as the reference reader skips them.
fn read_json_dir(dir: &Path) -> Vec<(String, Map<String, Value>)> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut paths: Vec<PathBuf> = entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("json"))
        .collect();
    paths.sort();
    let mut out = Vec::new();
    for path in paths {
        let Ok(raw) = fs::read_to_string(&path) else {
            continue;
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
    out
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
    let mut counts = OpencodeIngestCounts::default();
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
    let last_ts = loaded
        .messages
        .last()
        .map(|message| message.time_created)
        .or(loaded.session.updated_ms)
        .unwrap_or(first_ts);

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
                    counts.markers += insert_session_marker(
                        conn,
                        session_id,
                        OPENCODE_MARKER_COMPACTION_BOUNDARY,
                        Some(&message.id),
                        message.time_created,
                        part.raw.get("auto").and_then(Value::as_bool),
                        &format!("part:{}", part.id),
                    )?;
                    continue;
                }
                let Some(text) = part_text(part) else {
                    continue;
                };
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
                    &format!("text:{}", part.id),
                )?;
                counts.events += 1;
                // The prompt ledger predates session events and other tools
                // read it; keep writing it from the same parse.
                let trimmed = text.trim();
                if !trimmed.is_empty() {
                    counts.prompts += insert_history(
                        conn,
                        &HistoryEntry {
                            id: 0,
                            source: "opencode".into(),
                            session_id: Some(session_id.to_string()),
                            project: message_project.clone(),
                            prompt: trimmed.to_string(),
                            prompt_hash: Some(prompt_hash(trimmed)),
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
                    &format!("text:{}", part.id),
                )?;
                counts.events += 1;
                last_assistant_text = Some(text.to_string());
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
                target.as_deref(),
                model.as_deref(),
                token_json.as_deref(),
                message.provider_id.as_deref(),
                stop_reason.as_deref(),
                &format!("tool_use:{}", tool.call_id),
            )?;
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
                    )?;
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
            },
        )?;
    }

    Ok(counts)
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
