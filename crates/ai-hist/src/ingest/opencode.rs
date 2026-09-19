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
use rusqlite::{params, Connection};
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
        if part_columns.contains("session_id") {
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
fn derive_storage_root(session_file: &Path) -> PathBuf {
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

        let mut seen_calls: BTreeSet<String> = BTreeSet::new();
        for part in parts {
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
            // OpenCode can write the same call id more than once as a call
            // progresses; the last write wins in the store and the first one
            // here would otherwise double-count.
            if !seen_calls.insert(tool.call_id.to_string()) {
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
