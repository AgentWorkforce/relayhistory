//! One Devin CLI session as its `sessions.db` records it.
//!
//! The Devin CLI keeps its session store at
//! `$XDG_DATA_HOME/devin/cli/sessions.db` (default
//! `~/.local/share/devin/cli/sessions.db`) with a transcript export per
//! session at `transcripts/<session_id>.json`. The database is the
//! authoritative record: `sessions` carries identity and metadata,
//! `message_nodes` carries one `chat_message` JSON document per node
//! (`role` of `system`/`user`/`assistant`/`tool`), and `tool_call_state`
//! carries the ACP-shaped tool call and its latest update
//! (`kind`, `title`, `rawInput`, `locations`, `status`).
//!
//! All provider timestamps are **epoch seconds**; every timestamp written to
//! the canonical tables is converted to milliseconds here. The transcripts
//! are read only for their `agent` envelope (`name`, `version`,
//! `model_name`) and numeric `final_metrics` — their `steps` duplicate what
//! `message_nodes` already stores in richer form.
//!
//! Sessions the CLI marks `hidden` are not indexed. Malformed `chat_message`
//! or tool-call JSON is skipped per record and counted in
//! [`DevinIngestCounts`]; one bad row never fails the session or the sweep.

use anyhow::{bail, Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use crate::discover;
use crate::ingest::tool_result_facts::{
    ToolResultFacts, ToolResultIndexer, EVENT_SOURCE_FUNCTION_CALL_OUTPUT, STATUS_CANCELLED,
    STATUS_COMPLETED, STATUS_ERRORED, STATUS_RUNNING, STATUS_UNKNOWN,
};
use crate::store::{open_db_readonly, HistoryEntry, NewSessionMarker};
use crate::{insert_history, insert_session_marker, prompt_hash};

/// The `source` value stamped on every row this module writes.
pub(crate) const SOURCE: &str = "devin";

/// Sync-state key for the per-session stamp records of the incremental sweep.
const SYNC_STATE_KEY: &str = "devin_sessions_v1";

/// `tool_call_state.status` values the CLI has been observed to write, mapped
/// onto the canonical result-status vocabulary.
const ERROR_SIGNAL_TOOL_STATUS: &str = "tool_call_state.status";

/// One row of the provider's `sessions` table.
pub(crate) struct DevinSessionInfo {
    pub id: String,
    pub title: Option<String>,
    pub working_directory: Option<String>,
    pub workspace_dirs: Vec<String>,
    pub backend_type: Option<String>,
    pub model: Option<String>,
    pub agent_mode: Option<String>,
    /// `created_at` converted to milliseconds.
    pub created_ms: Option<i64>,
    /// `last_activity_at` converted to milliseconds.
    pub last_activity_ms: Option<i64>,
    /// Numeric fields of the `metadata` JSON (`total_*_cost` and friends).
    pub metadata: Option<Map<String, Value>>,
}

/// One `message_nodes` row with its JSON documents already parsed.
pub(crate) struct DevinNode {
    pub node_id: i64,
    pub parent_node_id: Option<i64>,
    /// `created_at` converted to milliseconds.
    pub created_ms: Option<i64>,
    /// Parsed `chat_message`; `None` rows are malformed and skipped.
    pub message: Option<Value>,
    /// Parsed `metadata` column of the node row itself.
    pub node_metadata: Option<Value>,
}

/// One `tool_call_state` row: the original call plus its latest update.
#[derive(Default)]
pub(crate) struct DevinToolState {
    pub call: Option<Value>,
    pub update: Option<Value>,
}

/// Everything [`normalize`] needs, loaded inside one read snapshot.
pub(crate) struct DevinSession {
    pub info: DevinSessionInfo,
    pub nodes: Vec<DevinNode>,
    /// `tool_call_id` → call/update pair.
    pub tools: BTreeMap<String, DevinToolState>,
    pub transcript: Option<DevinTranscriptMeta>,
}

/// The fields of `transcripts/<id>.json` this store indexes.
pub(crate) struct DevinTranscriptMeta {
    pub agent_name: Option<String>,
    pub agent_version: Option<String>,
    pub model_name: Option<String>,
    pub schema_version: Option<Value>,
    /// `final_metrics` with only its numeric entries kept.
    pub final_metrics: Option<Map<String, Value>>,
}

/// What one normalized session produced, for sync reporting and tests.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DevinIngestCounts {
    pub prompts: usize,
    pub events: usize,
    pub tool_calls: usize,
    pub file_edits: usize,
    pub markers: usize,
    /// `chat_message` JSON that did not parse — skipped, not fatal.
    pub malformed_nodes: usize,
    /// `tool_call_json`/`tool_call_update_json` that did not parse.
    pub malformed_tool_state: usize,
}

/// `sessions.db` inside a Devin CLI data directory.
pub(crate) fn sessions_db_path(cli_dir: &Path) -> PathBuf {
    cli_dir.join("sessions.db")
}

/// `transcripts/` inside a Devin CLI data directory.
pub(crate) fn transcripts_dir(cli_dir: &Path) -> PathBuf {
    cli_dir.join("transcripts")
}

fn seconds_to_ms(seconds: Option<i64>) -> Option<i64> {
    seconds.map(|s| s.saturating_mul(1000))
}

/// The column names of `table` in `src`, for schema-drift guards.
pub(crate) fn table_columns(src: &Connection, table: &str) -> Result<BTreeSet<String>> {
    let mut stmt = src.prepare(&format!("SELECT name FROM pragma_table_info('{table}')"))?;
    let mut columns = BTreeSet::new();
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        columns.insert(row.get::<_, String>(0)?);
    }
    Ok(columns)
}

/// Whether `sessions.db` has the layout this adapter parses.
///
/// Returns `false` rather than erroring on a missing file or absent tables so
/// discovery and sync can treat "no Devin install" and "not a Devin store"
/// the same way: nothing to read.
pub(crate) fn is_devin_store(src: &Connection) -> bool {
    for table in ["sessions", "message_nodes", "tool_call_state"] {
        let exists: Option<i64> = src
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",
                params![table],
                |row| row.get(0),
            )
            .optional()
            .unwrap_or(None);
        if exists.is_none() {
            return false;
        }
    }
    true
}

fn parse_json_column(raw: Option<String>) -> Option<Value> {
    raw.and_then(|text| serde_json::from_str(&text).ok())
}

/// Load one session's rows inside the caller's read transaction.
///
/// `None` means the session is absent or marked `hidden`. `chat_message` and
/// tool-state JSON that fails to parse is carried as `None` so one malformed
/// row cannot void the rest of the session.
pub(crate) fn load_from_sqlite(
    src: &Connection,
    session_id: &str,
    cli_dir: &Path,
) -> Result<Option<DevinSession>> {
    let has_hidden = table_columns(src, "sessions")?.contains("hidden");
    let hidden_pred = if has_hidden {
        "COALESCE(hidden, 0) = 0"
    } else {
        "1=1"
    };
    let info = src
        .query_row(
            &format!(
                "SELECT id, title, working_directory, backend_type, model, agent_mode, \
                 created_at, last_activity_at, workspace_dirs, metadata \
                 FROM sessions WHERE id = ?1 AND {hidden_pred}"
            ),
            params![session_id],
            |row| {
                Ok(DevinSessionInfo {
                    id: row.get(0)?,
                    title: row.get(1)?,
                    working_directory: row.get(2)?,
                    backend_type: row.get(3)?,
                    model: row.get(4)?,
                    agent_mode: row.get(5)?,
                    created_ms: seconds_to_ms(row.get(6)?),
                    last_activity_ms: seconds_to_ms(row.get(7)?),
                    workspace_dirs: parse_json_column(row.get(8)?)
                        .and_then(|v| v.as_array().cloned())
                        .unwrap_or_default()
                        .iter()
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect(),
                    metadata: parse_json_column(row.get(9)?)
                        .and_then(|v| v.as_object().cloned())
                        .map(numeric_fields),
                })
            },
        )
        .optional()?;
    let Some(info) = info else {
        return Ok(None);
    };

    let mut nodes = Vec::new();
    let mut stmt = src.prepare(
        "SELECT node_id, parent_node_id, created_at, chat_message, metadata \
         FROM message_nodes WHERE session_id = ?1 ORDER BY node_id ASC, row_id ASC",
    )?;
    let mut rows = stmt.query(params![session_id])?;
    while let Some(row) = rows.next()? {
        nodes.push(DevinNode {
            node_id: row.get(0)?,
            parent_node_id: row.get(1)?,
            created_ms: seconds_to_ms(row.get(2)?),
            message: parse_json_column(row.get::<_, Option<String>>(3)?),
            node_metadata: parse_json_column(row.get::<_, Option<String>>(4)?),
        });
    }
    drop(rows);
    drop(stmt);

    let mut tools = BTreeMap::new();
    let mut stmt = src.prepare(
        "SELECT tool_call_id, tool_call_json, tool_call_update_json \
         FROM tool_call_state WHERE session_id = ?1 ORDER BY rowid ASC",
    )?;
    let mut rows = stmt.query(params![session_id])?;
    while let Some(row) = rows.next()? {
        tools.insert(
            row.get::<_, String>(0)?,
            DevinToolState {
                call: parse_json_column(row.get::<_, Option<String>>(1)?),
                update: parse_json_column(row.get::<_, Option<String>>(2)?),
            },
        );
    }
    drop(rows);
    drop(stmt);

    let transcript =
        load_transcript_meta(&transcripts_dir(cli_dir).join(format!("{session_id}.json")));
    Ok(Some(DevinSession {
        info,
        nodes,
        tools,
        transcript,
    }))
}

/// Keep only the numeric fields of a provider metadata object — enough for
/// provenance without copying free-form provider text into the index.
fn numeric_fields(map: Map<String, Value>) -> Map<String, Value> {
    map.into_iter()
        .filter(|(_, v)| v.is_number())
        .collect::<Map<_, _>>()
}

/// The top-level transcript envelope. `steps` — the full duplicated session —
/// and every other unlisted key are walked by the parser but never
/// materialized: serde skips them through `IgnoredAny`, so reading a large
/// transcript costs the envelope, not the steps tree.
#[derive(serde::Deserialize)]
struct DevinTranscriptEnvelope {
    agent: Option<DevinAgentMeta>,
    schema_version: Option<Value>,
    final_metrics: Option<Value>,
}

#[derive(serde::Deserialize)]
struct DevinAgentMeta {
    name: Option<String>,
    version: Option<String>,
    model_name: Option<String>,
}

/// Read the `agent` envelope and `final_metrics` of `transcripts/<id>.json`.
///
/// A missing or malformed transcript is not an error: the database is
/// authoritative and `None` simply means no envelope was recorded.
fn load_transcript_meta(path: &Path) -> Option<DevinTranscriptMeta> {
    let file = std::fs::File::open(path).ok()?;
    let envelope: DevinTranscriptEnvelope =
        serde_json::from_reader(std::io::BufReader::new(file)).ok()?;
    let agent = envelope.agent;
    Some(DevinTranscriptMeta {
        agent_name: agent.as_ref().and_then(|a| a.name.clone()),
        agent_version: agent.as_ref().and_then(|a| a.version.clone()),
        model_name: agent.as_ref().and_then(|a| a.model_name.clone()),
        schema_version: envelope.schema_version,
        final_metrics: envelope
            .final_metrics
            .as_ref()
            .and_then(Value::as_object)
            .cloned()
            .map(numeric_fields),
    })
}

/// The session ids the sweep should consider — everything not `hidden`.
pub(crate) fn list_session_ids(src: &Connection) -> Result<Vec<String>> {
    let hidden_pred = if table_columns(src, "sessions")?.contains("hidden") {
        "COALESCE(hidden, 0) = 0"
    } else {
        "1=1"
    };
    let mut stmt = src.prepare(&format!(
        "SELECT id FROM sessions WHERE id <> '' AND {hidden_pred} \
         ORDER BY created_at ASC, id ASC"
    ))?;
    let mut ids = Vec::new();
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        ids.push(row.get(0)?);
    }
    Ok(ids)
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;

/// FNV-1a, the same non-cryptographic fold the opencode stamp uses: it only
/// has to change when the bytes change.
fn fnv(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// Register `ai_hist_fnv` on a read-only Devin connection.
///
/// [`session_stamp`] skips loading message content, but SQLite exposes no
/// hash aggregate, and a provider rewrite that keeps row ids, counts and
/// `last_activity_at` unchanged — an in-place compaction, or a rewrite that
/// preserves byte length — would go unseen by a metadata-only stamp. The UDF
/// is connection-local: the provider database itself is never modified.
pub(crate) fn register_stamp_fn(src: &Connection) -> Result<()> {
    src.create_scalar_function(
        "ai_hist_fnv",
        1,
        rusqlite::functions::FunctionFlags::SQLITE_UTF8
            | rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let hash = match ctx.get::<Option<String>>(0)? {
                Some(text) => fnv(FNV_OFFSET, text.as_bytes()),
                None => FNV_OFFSET,
            };
            // SUM overflows i64 on enough rows; masking to 31 bits keeps the
            // fold deterministic and the aggregate far below the limit.
            Ok((hash & 0x7FFF_FFFF) as i64)
        },
    )?;
    Ok(())
}

/// A change stamp covering everything the ingest reads for one session.
///
/// `None` means the session is absent or hidden. The stamp folds in the
/// activity timestamp, a checksum of the session row's own fields (title,
/// cwd, workspace, model, mode, metadata), the message-node count, max and
/// sum of row ids, a content checksum of every `chat_message`/`metadata`,
/// the tool-state count and a checksum of its payloads, and the transcript
/// file's own stamp — discovery, sync and hydration all derive "did this
/// change" from the same tuple so no surface can skip evidence the others
/// would re-read.
///
/// Requires [`register_stamp_fn`] on `src`.
pub(crate) fn session_stamp(
    src: &Connection,
    session_id: &str,
    transcripts_dir: &Path,
) -> Result<Option<String>> {
    let hidden_pred = if table_columns(src, "sessions")?.contains("hidden") {
        "COALESCE(hidden, 0) = 0"
    } else {
        "1=1"
    };
    let head: Option<(i64, i64)> = src
        .query_row(
            &format!(
                "SELECT COALESCE(last_activity_at, 0), ai_hist_fnv(\
                 COALESCE(title, '') || '|' || COALESCE(working_directory, '') || '|' || \
                 COALESCE(workspace_dirs, '') || '|' || COALESCE(model, '') || '|' || \
                 COALESCE(agent_mode, '') || '|' || COALESCE(backend_type, '') || '|' || \
                 COALESCE(metadata, '') || '|' || COALESCE(created_at, -1)) \
                 FROM sessions WHERE id = ?1 AND {hidden_pred}"
            ),
            params![session_id],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()?;
    let Some((last_activity, head_digest)) = head else {
        return Ok(None);
    };
    let (node_count, node_max, node_rows, node_digest): (i64, i64, i64, i64) = src.query_row(
        "SELECT COUNT(*), COALESCE(MAX(row_id), 0), COALESCE(SUM(row_id), 0), \
         COALESCE(SUM(ai_hist_fnv(chat_message) + ai_hist_fnv(metadata) + \
         ai_hist_fnv(CAST(created_at AS TEXT))), 0) \
         FROM message_nodes WHERE session_id = ?1",
        params![session_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )?;
    let (tool_count, tool_rows, tool_digest): (i64, i64, i64) = src
        .query_row(
            "SELECT COUNT(*), COALESCE(SUM(rowid), 0), \
             COALESCE(SUM(ai_hist_fnv(tool_call_json) + \
             ai_hist_fnv(tool_call_update_json)), 0) \
             FROM tool_call_state WHERE session_id = ?1",
            params![session_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap_or((0, 0, 0));
    let transcript_stamp =
        super::file_stamp_and_modified(&transcripts_dir.join(format!("{session_id}.json")))
            .map(|(stamp, _)| stamp)
            .unwrap_or_else(|_| "absent".to_string());
    Ok(Some(format!(
        "{last_activity}:{head_digest}:{node_count}:{node_max}:{node_rows}:{node_digest}:{tool_count}:{tool_rows}:{tool_digest}|{transcript_stamp}"
    )))
}

fn message_id_of(node_id: i64, message: Option<&Value>) -> String {
    message
        .and_then(|m| m.get("message_id"))
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| format!("n{node_id}"))
}

fn str_field<'a>(value: Option<&'a Value>, key: &str) -> Option<&'a str> {
    value
        .and_then(|v| v.get(key))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}

/// The text of a `content` field that is a plain string.
fn content_text(message: Option<&Value>) -> Option<&str> {
    message
        .and_then(|m| m.get("content"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

/// Map one `tool_call_state` update status onto the canonical vocabulary.
fn tool_result_status(state: Option<&DevinToolState>) -> &'static str {
    let status = state
        .and_then(|s| s.update.as_ref().or(s.call.as_ref()))
        .and_then(|v| v.get("status"))
        .and_then(Value::as_str)
        .unwrap_or("");
    match status {
        "completed" => STATUS_COMPLETED,
        "failed" | "error" | "errored" => STATUS_ERRORED,
        "cancelled" | "canceled" => STATUS_CANCELLED,
        "in_progress" | "pending" | "running" => STATUS_RUNNING,
        _ => STATUS_UNKNOWN,
    }
}

/// Whether the provider says this call failed.
fn tool_is_error(state: Option<&DevinToolState>) -> Option<bool> {
    match tool_result_status(state) {
        STATUS_ERRORED => Some(true),
        STATUS_COMPLETED => Some(false),
        _ => None,
    }
}

/// The ACP `kind` of a recorded tool call (`execute`, `edit`, `read`, …).
fn tool_kind(state: Option<&DevinToolState>) -> Option<&str> {
    state
        .and_then(|s| s.call.as_ref())
        .and_then(|v| v.get("kind"))
        .and_then(Value::as_str)
}

/// A file path the provider itself recorded for a tool call: `locations[0]`,
/// then `content[].path`, then the call's `rawInput`/`arguments` path fields.
fn tool_file_path<'a>(
    state: Option<&'a DevinToolState>,
    arguments: Option<&'a Value>,
) -> Option<&'a str> {
    let call = state.and_then(|s| s.call.as_ref());
    if let Some(path) = call
        .and_then(|c| c.get("locations"))
        .and_then(Value::as_array)
        .and_then(|locs| locs.first())
        .and_then(|loc| loc.get("path"))
        .and_then(Value::as_str)
        .filter(|p| !p.is_empty())
    {
        return Some(path);
    }
    if let Some(path) = call
        .and_then(|c| c.get("content"))
        .and_then(Value::as_array)
        .and_then(|items| {
            items.iter().find_map(|item| {
                item.get("path")
                    .and_then(Value::as_str)
                    .filter(|p| !p.is_empty())
            })
        })
    {
        return Some(path);
    }
    for source in [arguments, call.and_then(|c| c.get("rawInput"))]
        .into_iter()
        .flatten()
    {
        for key in ["file_path", "path", "filePath", "filename"] {
            if let Some(path) = source
                .get(key)
                .and_then(Value::as_str)
                .filter(|p| !p.is_empty())
            {
                return Some(path);
            }
        }
    }
    None
}

/// A display target for a tool call: its file path, then command-ish fields.
fn pick_target<'a>(
    state: Option<&'a DevinToolState>,
    arguments: Option<&'a Value>,
) -> Option<&'a str> {
    if let Some(path) = tool_file_path(state, arguments) {
        return Some(path);
    }
    for source in [
        arguments,
        state
            .and_then(|s| s.call.as_ref())
            .and_then(|c| c.get("rawInput")),
    ]
    .into_iter()
    .flatten()
    {
        for key in ["command", "cmd", "query", "pattern", "url"] {
            if let Some(v) = source
                .get(key)
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
            {
                return Some(v);
            }
        }
    }
    None
}

/// `state` when the provider says the call edits files (`kind: "edit"` or a
/// recorded `locations` path), i.e. file edits only where source data
/// exposes them.
fn edit_file_path<'a>(
    state: Option<&'a DevinToolState>,
    name: &str,
    arguments: Option<&'a Value>,
) -> Option<&'a str> {
    let kind = tool_kind(state);
    let exposes_path = tool_file_path(state, arguments);
    match (kind, exposes_path) {
        (Some("edit"), Some(path)) => Some(path),
        (Some("edit"), None) => None,
        (_, Some(_)) if matches!(kind, Some("edit")) => unreachable!(),
        // No recorded kind: trust an edit-shaped tool name only when the
        // provider also recorded a path.
        (None, Some(path)) if name.contains("edit") || name.contains("write") => Some(path),
        _ => None,
    }
}

/// Write one fully-loaded session into the canonical tables.
///
/// Every row is an idempotent upsert keyed on provider-stable ids
/// (`node_id`/`tool_call_id`-derived uids), and [`retire_absent_rows`]
/// removes rows whose source records disappeared. A pass that read the
/// session's entire source is authoritative for both ends of its activity
/// window, so this uses `upsert_session_rebuilt`.
///
/// A savepoint rather than a transaction, because the callers differ:
/// hydration already holds one (a nested `BEGIN` would fail), global sync
/// does not. A savepoint nests either way.
pub(crate) fn normalize(
    conn: &Connection,
    loaded: &DevinSession,
    raw_path: &str,
) -> Result<DevinIngestCounts> {
    conn.execute_batch("SAVEPOINT ai_hist_devin_session")?;
    let result = normalize_inner(conn, loaded, raw_path);
    if result.is_ok() {
        conn.execute_batch("RELEASE ai_hist_devin_session")?;
    } else {
        let _ =
            conn.execute_batch("ROLLBACK TO ai_hist_devin_session; RELEASE ai_hist_devin_session;");
    }
    result
}

fn normalize_inner(
    conn: &Connection,
    loaded: &DevinSession,
    raw_path: &str,
) -> Result<DevinIngestCounts> {
    let mut counts = DevinIngestCounts::default();
    let session_id = loaded.info.id.as_str();
    let cwd = loaded.info.working_directory.as_deref();
    let project = cwd;
    let base_ts = loaded
        .info
        .created_ms
        .or(loaded.info.last_activity_ms)
        .unwrap_or(0);

    // node_id → stored message_id, so parent links point at event rows.
    let mut node_message_ids: BTreeMap<i64, String> = BTreeMap::new();
    for node in &loaded.nodes {
        node_message_ids.insert(
            node.node_id,
            message_id_of(node.node_id, node.message.as_ref()),
        );
    }

    let mut present_events: BTreeSet<String> = BTreeSet::new();
    let mut present_tool_calls: BTreeSet<String> = BTreeSet::new();
    let mut present_file_edits: BTreeSet<String> = BTreeSet::new();
    let mut present_markers: BTreeSet<String> = BTreeSet::new();
    let mut present_prompts: BTreeSet<String> = BTreeSet::new();
    let mut referenced_tool_calls: BTreeSet<String> = BTreeSet::new();
    let mut indexer = ToolResultIndexer::default();
    let mut last_assistant_text: Option<String> = None;
    let mut first_ts: Option<i64> = None;
    let mut last_ts: Option<i64> = None;
    let mut note_ts = |ts: i64| {
        first_ts = Some(first_ts.map_or(ts, |t| t.min(ts)));
        last_ts = Some(last_ts.map_or(ts, |t| t.max(ts)));
    };

    for node in &loaded.nodes {
        let ts = node.created_ms.unwrap_or(base_ts);
        note_ts(ts);
        let message_id = node_message_ids
            .get(&node.node_id)
            .cloned()
            .unwrap_or_else(|| format!("n{}", node.node_id));
        let parent_id = node
            .parent_node_id
            .and_then(|parent| node_message_ids.get(&parent))
            .cloned();
        let Some(message) = node.message.as_ref() else {
            counts.malformed_nodes += 1;
            // Record the gap under the node's own message id: children that
            // name this node as their parent then anchor to an addressable
            // row instead of dangling.
            let uid = format!("n{}:malformed", node.node_id);
            present_markers.insert(uid.clone());
            counts.markers += insert_session_marker(
                conn,
                SOURCE,
                session_id,
                &NewSessionMarker {
                    marker_uid: &uid,
                    ts_ms: Some(ts),
                    message_id: Some(&message_id),
                    parent_id: parent_id.as_deref(),
                    turn_id: None,
                    kind: "malformed_node",
                    subkind: None,
                    text: node
                        .node_metadata
                        .as_ref()
                        .and_then(|m| serde_json::to_string(m).ok())
                        .map(|s| super::truncate_marker_text(&s))
                        .as_deref(),
                    payload_json: None,
                },
            )?;
            continue;
        };
        let meta = message.get("metadata").and_then(Value::as_object);
        let role = str_field(message.into(), "role").unwrap_or("");
        let message_model = meta
            .and_then(|m| m.get("generation_model"))
            .and_then(Value::as_str)
            .or(loaded.info.model.as_deref());
        let token_json = meta
            .and_then(|m| m.get("num_tokens"))
            .filter(|v| v.is_number())
            .map(|n| format!("{{\"num_tokens\":{n}}}"));
        let identity = super::RequestIdentity {
            request_id: meta
                .and_then(|m| m.get("request_id"))
                .and_then(Value::as_str),
            provider_message_id: message.get("message_id").and_then(Value::as_str),
        };
        let raw_facts = super::RawMessageFacts {
            request_id: identity.request_id,
            stop_reason: meta
                .and_then(|m| m.get("finish_reason"))
                .and_then(Value::as_str),
            agent_version: loaded
                .transcript
                .as_ref()
                .and_then(|t| t.agent_version.as_deref()),
            is_sidechain: None,
            is_meta: None,
            turn_id: None,
            request_span: None,
        };

        match role {
            "user" => {
                let is_user_input = meta
                    .and_then(|m| m.get("is_user_input"))
                    .and_then(Value::as_bool)
                    .unwrap_or(true);
                if !is_user_input {
                    let uid = format!("n{}:synthetic", node.node_id);
                    present_markers.insert(uid.clone());
                    counts.markers += insert_session_marker(
                        conn,
                        SOURCE,
                        session_id,
                        &NewSessionMarker {
                            marker_uid: &uid,
                            ts_ms: Some(ts),
                            message_id: Some(&message_id),
                            parent_id: parent_id.as_deref(),
                            turn_id: None,
                            kind: "synthetic_turn",
                            subkind: Some("user"),
                            text: content_text(Some(message))
                                .map(super::truncate_marker_text)
                                .as_deref(),
                            payload_json: None,
                        },
                    )?;
                    continue;
                }
                if let Some(text) = content_text(Some(message)) {
                    let uid = format!("n{}:text", node.node_id);
                    present_events.insert(uid.clone());
                    super::insert_session_event(
                        conn,
                        SOURCE,
                        session_id,
                        project,
                        cwd,
                        None,
                        &message_id,
                        parent_id.as_deref(),
                        ts,
                        "user",
                        "text",
                        Some(text),
                        None,
                        None,
                        identity,
                        &uid,
                        None,
                        raw_facts,
                    )?;
                    counts.events += 1;
                    let hash = prompt_hash(text);
                    present_prompts.insert(format!("{ts}:{hash}"));
                    counts.prompts += insert_history(
                        conn,
                        &HistoryEntry {
                            id: 0,
                            source: SOURCE.to_string(),
                            session_id: Some(session_id.to_string()),
                            project: project.map(str::to_string),
                            prompt: text.to_string(),
                            prompt_hash: Some(hash),
                            timestamp_ms: ts,
                        },
                    )?;
                }
            }
            "assistant" | "final_answer" => {
                if let Some(thinking) = message
                    .get("thinking")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                {
                    let uid = format!("n{}:thinking", node.node_id);
                    present_events.insert(uid.clone());
                    super::insert_session_event(
                        conn,
                        SOURCE,
                        session_id,
                        project,
                        cwd,
                        None,
                        &message_id,
                        parent_id.as_deref(),
                        ts,
                        "assistant",
                        "thinking",
                        Some(thinking),
                        message_model,
                        token_json.as_deref(),
                        identity,
                        &uid,
                        None,
                        raw_facts,
                    )?;
                    counts.events += 1;
                }
                if let Some(text) = content_text(Some(message)) {
                    let uid = format!("n{}:text", node.node_id);
                    present_events.insert(uid.clone());
                    super::insert_session_event(
                        conn,
                        SOURCE,
                        session_id,
                        project,
                        cwd,
                        None,
                        &message_id,
                        parent_id.as_deref(),
                        ts,
                        "assistant",
                        "text",
                        Some(text),
                        message_model,
                        token_json.as_deref(),
                        identity,
                        &uid,
                        None,
                        raw_facts,
                    )?;
                    counts.events += 1;
                    last_assistant_text = Some(discover::excerpt(text));
                }
                if let Some(calls) = message.get("tool_calls").and_then(Value::as_array) {
                    for (index, call) in calls.iter().enumerate() {
                        let call_id = str_field(call.into(), "id")
                            .map(str::to_string)
                            .unwrap_or_else(|| format!("n{}:{}", node.node_id, index));
                        referenced_tool_calls.insert(call_id.clone());
                        let state = loaded.tools.get(&call_id);
                        let name = str_field(call.into(), "name")
                            .or_else(|| {
                                state
                                    .and_then(|s| s.call.as_ref())
                                    .and_then(|c| c.get("title"))
                                    .and_then(Value::as_str)
                            })
                            .unwrap_or("unknown");
                        let arguments = call.get("arguments").or_else(|| {
                            state
                                .and_then(|s| s.call.as_ref())
                                .and_then(|c| c.get("rawInput"))
                        });
                        let args_json = arguments
                            .map(|a| serde_json::to_string(a).unwrap_or_else(|_| "{}".into()))
                            .unwrap_or_else(|| "{}".to_string());
                        let target = pick_target(state, arguments);
                        let is_error = tool_is_error(state);
                        let uid = format!("n{}:tool:{call_id}", node.node_id);
                        present_events.insert(uid.clone());
                        super::insert_session_event(
                            conn,
                            SOURCE,
                            session_id,
                            project,
                            cwd,
                            None,
                            &message_id,
                            parent_id.as_deref(),
                            ts,
                            "assistant",
                            "tool_use",
                            Some(&super::format_tool_event_text(
                                name,
                                target,
                                arguments.unwrap_or(&Value::Null),
                            )),
                            message_model,
                            token_json.as_deref(),
                            identity,
                            &uid,
                            None,
                            raw_facts,
                        )?;
                        counts.events += 1;
                        present_tool_calls.insert(call_id.clone());
                        super::insert_tool_call(
                            conn,
                            SOURCE,
                            session_id,
                            &message_id,
                            &call_id,
                            name,
                            target,
                            &args_json,
                            is_error,
                            ts,
                        )?;
                        counts.tool_calls += 1;
                        if let Some(path) = edit_file_path(state, name, arguments) {
                            present_file_edits.insert(call_id.clone());
                            super::upsert_file_edit_from_call(
                                conn,
                                SOURCE,
                                session_id,
                                &message_id,
                                &call_id,
                                path,
                                name,
                                ts,
                                None,
                                cwd,
                            )?;
                            counts.file_edits += 1;
                        }
                    }
                }
            }
            "tool" => {
                let tool_use_id = str_field(message.into(), "tool_call_id")
                    .map(str::to_string)
                    .unwrap_or_default();
                if !tool_use_id.is_empty() {
                    referenced_tool_calls.insert(tool_use_id.clone());
                }
                let state = loaded.tools.get(&tool_use_id);
                let status = tool_result_status(state);
                let payload = message.get("content").cloned().unwrap_or(Value::Null);
                let (call_index, event_index) = indexer.next(&tool_use_id);
                let mut facts =
                    ToolResultFacts::from_payload(&payload).with_ordering(call_index, event_index);
                facts.result_status = Some(status.to_string());
                facts.event_source = Some(EVENT_SOURCE_FUNCTION_CALL_OUTPUT.to_string());
                facts.tool_use_id = Some(tool_use_id.clone());
                if status == STATUS_ERRORED {
                    facts.error_signal = Some(ERROR_SIGNAL_TOOL_STATUS.to_string());
                }
                let text = content_text(Some(message));
                let uid = format!("n{}:result:{tool_use_id}", node.node_id);
                present_events.insert(uid.clone());
                super::insert_session_event(
                    conn,
                    SOURCE,
                    session_id,
                    project,
                    cwd,
                    None,
                    &message_id,
                    parent_id.as_deref(),
                    ts,
                    "tool_result",
                    "tool_result",
                    text,
                    None,
                    None,
                    identity,
                    &uid,
                    Some(&facts),
                    raw_facts,
                )?;
                counts.events += 1;
                if !tool_use_id.is_empty() && status == STATUS_ERRORED {
                    super::set_tool_call_error(conn, SOURCE, session_id, &tool_use_id, true)?;
                }
            }
            "system" => {
                let uid = format!("n{}:system", node.node_id);
                present_markers.insert(uid.clone());
                counts.markers += insert_session_marker(
                    conn,
                    SOURCE,
                    session_id,
                    &NewSessionMarker {
                        marker_uid: &uid,
                        ts_ms: Some(ts),
                        message_id: Some(&message_id),
                        parent_id: parent_id.as_deref(),
                        turn_id: None,
                        kind: "system",
                        subkind: None,
                        text: content_text(Some(message))
                            .map(super::truncate_marker_text)
                            .as_deref(),
                        payload_json: None,
                    },
                )?;
            }
            other => {
                let uid = format!("n{}:other", node.node_id);
                present_markers.insert(uid.clone());
                counts.markers += insert_session_marker(
                    conn,
                    SOURCE,
                    session_id,
                    &NewSessionMarker {
                        marker_uid: &uid,
                        ts_ms: Some(ts),
                        message_id: Some(&message_id),
                        parent_id: parent_id.as_deref(),
                        turn_id: None,
                        kind: "unknown",
                        subkind: if other.is_empty() { None } else { Some(other) },
                        text: content_text(Some(message))
                            .map(super::truncate_marker_text)
                            .as_deref(),
                        payload_json: None,
                    },
                )?;
            }
        }

        // Node-level metadata that is not part of the message document.
        if let Some(summarized) = node
            .node_metadata
            .as_ref()
            .and_then(|m| m.get("summarized_from"))
            .filter(|v| !v.is_null())
        {
            let uid = format!("n{}:compaction", node.node_id);
            present_markers.insert(uid.clone());
            let payload = super::marker_payload(vec![("summarized_from", summarized.clone())]);
            counts.markers += insert_session_marker(
                conn,
                SOURCE,
                session_id,
                &NewSessionMarker {
                    marker_uid: &uid,
                    ts_ms: Some(ts),
                    message_id: Some(&message_id),
                    parent_id: parent_id.as_deref(),
                    turn_id: None,
                    kind: "compaction_boundary",
                    subkind: None,
                    text: None,
                    payload_json: payload.as_deref(),
                },
            )?;
        }
    }

    // Tool calls the provider persisted without a referencing message node —
    // they are still evidence, keyed by the call's own id.
    let orphan_ts = loaded.info.last_activity_ms.unwrap_or(base_ts);
    for (call_id, state) in &loaded.tools {
        if referenced_tool_calls.contains(call_id) {
            continue;
        }
        if state.call.is_none() && state.update.is_none() {
            counts.malformed_tool_state += 1;
            continue;
        }
        let call = state.call.as_ref();
        let name = call
            .and_then(|c| c.get("title"))
            .and_then(Value::as_str)
            .or_else(|| call.and_then(|c| c.get("name")).and_then(Value::as_str))
            .unwrap_or("unknown");
        let arguments = call.and_then(|c| c.get("rawInput"));
        let args_json = arguments
            .map(|a| serde_json::to_string(a).unwrap_or_else(|_| "{}".into()))
            .unwrap_or_else(|| "{}".to_string());
        let target = pick_target(Some(state), arguments);
        let message_id = format!("tcs:{call_id}");
        let uid = format!("tcs:{call_id}:tool");
        present_events.insert(uid.clone());
        super::insert_session_event(
            conn,
            SOURCE,
            session_id,
            project,
            cwd,
            None,
            &message_id,
            None,
            orphan_ts,
            "assistant",
            "tool_use",
            Some(&super::format_tool_event_text(
                name,
                target,
                arguments.unwrap_or(&Value::Null),
            )),
            loaded.info.model.as_deref(),
            None,
            super::RequestIdentity::none(),
            &uid,
            None,
            super::RawMessageFacts::default(),
        )?;
        counts.events += 1;
        present_tool_calls.insert(call_id.clone());
        super::insert_tool_call(
            conn,
            SOURCE,
            session_id,
            &message_id,
            call_id,
            name,
            target,
            &args_json,
            tool_is_error(Some(state)),
            orphan_ts,
        )?;
        counts.tool_calls += 1;
        if let Some(path) = edit_file_path(Some(state), name, arguments) {
            present_file_edits.insert(call_id.clone());
            super::upsert_file_edit_from_call(
                conn,
                SOURCE,
                session_id,
                &message_id,
                call_id,
                path,
                name,
                orphan_ts,
                None,
                cwd,
            )?;
            counts.file_edits += 1;
        }
    }

    // Session-level metadata that has no canonical column: the CLI's own
    // title, its mode/backend, and the transcript's agent envelope ride along
    // as bounded markers rather than being dropped.
    if let Some(title) = loaded
        .info
        .title
        .as_deref()
        .map(str::trim)
        .filter(|t| !t.is_empty())
    {
        let uid = "meta:title".to_string();
        present_markers.insert(uid.clone());
        counts.markers += insert_session_marker(
            conn,
            SOURCE,
            session_id,
            &NewSessionMarker {
                marker_uid: &uid,
                ts_ms: loaded.info.created_ms,
                message_id: None,
                parent_id: None,
                turn_id: None,
                kind: "session_title",
                subkind: None,
                text: Some(&super::truncate_marker_text(title)),
                payload_json: None,
            },
        )?;
    }
    {
        let payload = super::marker_payload(vec![
            (
                "backend_type",
                loaded
                    .info
                    .backend_type
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            ),
            (
                "agent_mode",
                loaded
                    .info
                    .agent_mode
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            ),
            (
                "model",
                loaded
                    .info
                    .model
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            ),
            (
                "workspace_dirs",
                if loaded.info.workspace_dirs.is_empty() {
                    Value::Null
                } else {
                    Value::Array(
                        loaded
                            .info
                            .workspace_dirs
                            .iter()
                            .cloned()
                            .map(Value::String)
                            .collect(),
                    )
                },
            ),
            (
                "metadata",
                loaded
                    .info
                    .metadata
                    .clone()
                    .map(Value::Object)
                    .unwrap_or(Value::Null),
            ),
        ]);
        if let Some(payload) = payload {
            let uid = "meta:session".to_string();
            present_markers.insert(uid.clone());
            counts.markers += insert_session_marker(
                conn,
                SOURCE,
                session_id,
                &NewSessionMarker {
                    marker_uid: &uid,
                    ts_ms: loaded.info.created_ms,
                    message_id: None,
                    parent_id: None,
                    turn_id: None,
                    kind: "session_meta",
                    subkind: None,
                    text: None,
                    payload_json: Some(&payload),
                },
            )?;
        }
    }
    if let Some(transcript) = &loaded.transcript {
        let payload = super::marker_payload(vec![
            (
                "agent_name",
                transcript
                    .agent_name
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            ),
            (
                "agent_version",
                transcript
                    .agent_version
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            ),
            (
                "model_name",
                transcript
                    .model_name
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            ),
            (
                "schema_version",
                transcript.schema_version.clone().unwrap_or(Value::Null),
            ),
            (
                "final_metrics",
                transcript
                    .final_metrics
                    .clone()
                    .map(Value::Object)
                    .unwrap_or(Value::Null),
            ),
        ]);
        if let Some(payload) = payload {
            let uid = "meta:agent".to_string();
            present_markers.insert(uid.clone());
            counts.markers += insert_session_marker(
                conn,
                SOURCE,
                session_id,
                &NewSessionMarker {
                    marker_uid: &uid,
                    ts_ms: loaded.info.last_activity_ms.or(loaded.info.created_ms),
                    message_id: None,
                    parent_id: None,
                    turn_id: None,
                    kind: "agent_manifest",
                    subkind: None,
                    text: None,
                    payload_json: Some(&payload),
                },
            )?;
        }
    }

    // The session's own `created_at` can precede every node timestamp;
    // earliest activity is the minimum of the two, not the first node.
    let first_ts = [first_ts, loaded.info.created_ms]
        .into_iter()
        .flatten()
        .min()
        .unwrap_or(0);
    let last_ts = last_ts.or(loaded.info.last_activity_ms).unwrap_or(first_ts);
    let last_ts = last_ts.max(loaded.info.last_activity_ms.unwrap_or(last_ts));
    super::upsert_session_rebuilt(
        conn,
        session_id,
        SOURCE,
        cwd,
        None,
        first_ts,
        last_ts,
        last_assistant_text.as_deref(),
        Some(raw_path),
    )?;
    retire_absent_rows(
        conn,
        session_id,
        &AbsentKeys {
            events: present_events,
            tool_calls: present_tool_calls,
            file_edits: present_file_edits,
            markers: present_markers,
            prompts: present_prompts,
        },
    )?;
    Ok(counts)
}

/// The provider ids of rows this pass produced, per canonical table.
struct AbsentKeys {
    events: BTreeSet<String>,
    tool_calls: BTreeSet<String>,
    file_edits: BTreeSet<String>,
    markers: BTreeSet<String>,
    prompts: BTreeSet<String>,
}

/// Remove rows a previous pass wrote whose source records are gone.
///
/// Devin rewrites `message_nodes` on compaction and drops tool state when a
/// session is retried, so stale rows are expected; identity is by the
/// provider uid each table keys on.
fn retire_absent_rows(conn: &Connection, session_id: &str, keys: &AbsentKeys) -> Result<usize> {
    let mut retired = 0;
    let present_prompts = serde_json::to_string(&keys.prompts).unwrap_or_else(|_| "[]".into());
    // A prompt another session still references is reassigned, not deleted.
    retired += reassign_shared_prompts(conn, session_id, &present_prompts)?;
    for (sql, present) in [
        (
            "DELETE FROM session_events WHERE source = 'devin' AND session_id = ?1 \
             AND event_uid NOT IN (SELECT value FROM json_each(?2))",
            serde_json::to_string(&keys.events).unwrap_or_else(|_| "[]".into()),
        ),
        (
            "DELETE FROM tool_calls WHERE source = 'devin' AND session_id = ?1 \
             AND tool_use_id NOT IN (SELECT value FROM json_each(?2))",
            serde_json::to_string(&keys.tool_calls).unwrap_or_else(|_| "[]".into()),
        ),
        (
            "DELETE FROM file_edits WHERE source = 'devin' AND session_id = ?1 \
             AND tool_use_id NOT IN (SELECT value FROM json_each(?2))",
            serde_json::to_string(&keys.file_edits).unwrap_or_else(|_| "[]".into()),
        ),
        (
            "DELETE FROM session_markers WHERE source = 'devin' AND session_id = ?1 \
             AND marker_uid NOT IN (SELECT value FROM json_each(?2))",
            serde_json::to_string(&keys.markers).unwrap_or_else(|_| "[]".into()),
        ),
        (
            "DELETE FROM history WHERE source = 'devin' AND session_id = ?1 \
             AND (timestamp_ms || ':' || coalesce(prompt_hash, '')) \
             NOT IN (SELECT value FROM json_each(?2))",
            present_prompts,
        ),
    ] {
        retired += conn.execute(sql, params![session_id, present])?;
    }
    Ok(retired)
}

/// The per-table evidence counts a pass leaves behind, so a stamp match is
/// trusted only while the destination still holds exactly the rows a
/// previous pass recorded. A partial loss — one event or edit row after an
/// incomplete restore — fails the comparison and repairs rather than being
/// masked by whatever evidence survived.
fn evidence_holdings(conn: &Connection, session_id: &str) -> Result<Value> {
    let mut holdings = Map::new();
    for (key, table) in [
        ("events", "session_events"),
        ("tool_calls", "tool_calls"),
        ("file_edits", "file_edits"),
        ("markers", "session_markers"),
        ("prompts", "history"),
        ("catalog", "sessions"),
    ] {
        let count: i64 = conn.query_row(
            &format!("SELECT COUNT(*) FROM {table} WHERE source = 'devin' AND session_id = ?1"),
            [session_id],
            |row| row.get(0),
        )?;
        holdings.insert(key.to_string(), Value::from(count));
    }
    Ok(Value::Object(holdings))
}

/// `history` is keyed `(source, timestamp_ms, prompt)`, so two Devin
/// sessions can share one row. Before this session's rows are deleted,
/// hand every prompt another Devin session still references to that
/// session instead of dropping shared evidence.
fn reassign_shared_prompts(
    conn: &Connection,
    session_id: &str,
    present_json: &str,
) -> Result<usize> {
    conn.execute(
        "UPDATE history SET session_id = ( \
            SELECT e.session_id FROM session_events e \
            WHERE e.source = 'devin' AND e.session_id <> ?1 \
              AND e.role = 'user' AND e.kind = 'text' \
              AND e.ts_ms = history.timestamp_ms AND e.text = history.prompt \
            LIMIT 1) \
         WHERE source = 'devin' AND session_id = ?1 \
           AND (timestamp_ms || ':' || COALESCE(prompt_hash, '')) \
               NOT IN (SELECT value FROM json_each(?2)) \
           AND EXISTS ( \
             SELECT 1 FROM session_events e \
             WHERE e.source = 'devin' AND e.session_id <> ?1 \
               AND e.role = 'user' AND e.kind = 'text' \
               AND e.ts_ms = history.timestamp_ms AND e.text = history.prompt)",
        params![session_id, present_json],
    )
    .map_err(Into::into)
}

/// Remove the local footprint of a previously indexed session: the provider
/// says it is gone or hidden, so the evidence the local pass wrote must go
/// too — otherwise a hidden local transcript stays searchable forever.
///
/// The `(source, session_id)` catalog identity is shared between local and
/// remote acquisition, so when a remote presence or remote observation
/// survives, the catalog row and the remote rows are kept and the session
/// stays visible as remote-only. Remote connector evidence lives in
/// `observation_evidence`, keyed per observation — it never touches the
/// canonical tables, so the canonical rows removed here are always the local
/// pass's own output.
fn retire_session(conn: &Connection, session_id: &str) -> Result<()> {
    let has_remote: bool = conn.query_row(
        "SELECT EXISTS(\
           SELECT 1 FROM session_presences \
           WHERE source = 'devin' AND session_id = ?1 AND location = 'remote'\
         ) OR EXISTS(\
           SELECT 1 FROM session_observations \
           WHERE source = 'devin' AND session_id = ?1 AND location = 'remote'\
         )",
        [session_id],
        |row| row.get(0),
    )?;
    reassign_shared_prompts(conn, session_id, "[]")?;
    conn.execute(
        "DELETE FROM history WHERE source = 'devin' AND session_id = ?1",
        [session_id],
    )?;
    conn.execute(
        "DELETE FROM session_events WHERE source = 'devin' AND session_id = ?1",
        [session_id],
    )?;
    conn.execute(
        "DELETE FROM tool_calls WHERE source = 'devin' AND session_id = ?1",
        [session_id],
    )?;
    conn.execute(
        "DELETE FROM file_edits WHERE source = 'devin' AND session_id = ?1",
        [session_id],
    )?;
    conn.execute(
        "DELETE FROM session_markers WHERE source = 'devin' AND session_id = ?1",
        [session_id],
    )?;
    // Only the local footprint goes: remote presences, remote observations
    // and remote hydration state belong to a session that still exists.
    conn.execute(
        "DELETE FROM session_presences \
         WHERE source = 'devin' AND session_id = ?1 AND location = 'local'",
        [session_id],
    )?;
    conn.execute(
        "DELETE FROM session_observations \
         WHERE source = 'devin' AND session_id = ?1 AND location = 'local'",
        [session_id],
    )?;
    conn.execute(
        "DELETE FROM session_hydration_checkpoints \
         WHERE source = 'devin' AND session_id = ?1 AND location = 'local'",
        [session_id],
    )?;
    if !has_remote {
        // No surviving remote view of the session: the catalog row goes, and
        // its delete triggers cascade whatever presence, observation,
        // relationship and hydration rows remain.
        conn.execute(
            "DELETE FROM sessions WHERE source = 'devin' AND session_id = ?1",
            [session_id],
        )?;
    }
    Ok(())
}

/// Read one Devin store and normalize every changed session.
///
/// `cli_dir` is the Devin CLI data directory (`sessions.db` + `transcripts/`).
/// The source is opened read-only and wrapped in one deferred read
/// transaction, so a live writer in WAL mode sees no lock contention and this
/// pass sees one coherent snapshot. Per-session stamps under
/// [`SYNC_STATE_KEY`] skip unchanged sessions; a session whose stamp matches
/// but whose evidence rows vanished is re-indexed rather than trusted.
pub(super) fn sync_devin_db(
    conn: &Connection,
    state: &mut Map<String, Value>,
    cli_dir: &Path,
    repairs: &super::SweepRepairs,
    coverage: &mut super::SweepCoverage,
) -> Result<usize> {
    super::check_capture_cancelled()?;
    let db_path = sessions_db_path(cli_dir);
    if !db_path.is_file() {
        return Ok(0);
    }
    let src = open_db_readonly(&db_path)
        .with_context(|| format!("open devin store {}", db_path.display()))?;
    register_stamp_fn(&src)?;
    src.execute_batch("PRAGMA query_only = ON; BEGIN DEFERRED")?;
    let result = sync_devin_from_source(conn, state, &src, cli_dir, coverage, repairs);
    let _ = src.execute_batch("ROLLBACK");
    result
}

fn sync_devin_from_source(
    conn: &Connection,
    state: &mut Map<String, Value>,
    src: &Connection,
    cli_dir: &Path,
    coverage: &mut super::SweepCoverage,
    repairs: &super::SweepRepairs,
) -> Result<usize> {
    if !is_devin_store(src) {
        return Ok(0);
    }
    let raw_path = sessions_db_path(cli_dir).to_string_lossy().to_string();
    let transcripts = transcripts_dir(cli_dir);
    let mut devin_state = state
        .get(SYNC_STATE_KEY)
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let ids = list_session_ids(src)?;
    let visible: BTreeSet<&str> = ids.iter().map(String::as_str).collect();
    // A session that became hidden or was deleted since the last pass keeps
    // no catalog row, no evidence and no stamp state — and reappears cleanly
    // if it is unhidden again.
    let gone: Vec<String> = devin_state
        .keys()
        .filter(|id| !visible.contains(id.as_str()))
        .cloned()
        .collect();
    for session_id in gone {
        retire_session(conn, &session_id)?;
        devin_state.remove(&session_id);
    }
    let mut inserted = 0usize;
    let mut failures: Vec<String> = Vec::new();
    for session_id in ids {
        super::check_capture_cancelled()?;
        let stamp = match session_stamp(src, &session_id, &transcripts) {
            Ok(Some(stamp)) => stamp,
            Ok(None) => continue,
            Err(err) => {
                coverage.note_unread();
                failures.push(format!("{session_id}: {err:#}"));
                continue;
            }
        };
        let recorded = devin_state.get(&session_id);
        // A session the destination marker names as short is re-read even
        // when its stamp matches — that is the repair the marker exists for.
        let needs_repair = repairs.contains(SOURCE, &session_id);
        if !needs_repair
            && recorded
                .and_then(|e| e.get("stamp"))
                .and_then(Value::as_str)
                == Some(stamp.as_str())
        {
            // Unchanged per the stamp — but only trustworthy while the
            // destination still holds exactly the rows the previous pass
            // recorded. Entries recorded before holdings existed
            // re-normalize once and pick the new shape up.
            let intact = recorded
                .and_then(|e| e.get("holdings"))
                .is_some_and(|held| {
                    evidence_holdings(conn, &session_id)
                        .map(|current| current == *held)
                        .unwrap_or(false)
                });
            if intact {
                continue;
            }
        }
        match load_from_sqlite(src, &session_id, cli_dir) {
            Ok(Some(loaded)) => {
                let outcome = normalize(conn, &loaded, &raw_path);
                match outcome {
                    Ok(counts) => {
                        inserted += counts.prompts;
                        let holdings = evidence_holdings(conn, &session_id)?;
                        devin_state.insert(
                            session_id,
                            serde_json::json!({
                                "stamp": stamp,
                                "session": loaded.info.id,
                                "holdings": holdings,
                            }),
                        );
                    }
                    Err(err) => {
                        coverage.note_unread();
                        failures.push(format!("{session_id}: {err:#}"));
                    }
                }
            }
            Ok(None) => {
                // Hidden between enumeration and load: record the stamp so
                // the session is not re-queried until it changes again. Its
                // holdings are whatever the last visible pass left — the
                // next sweep retires it once it drops out of `visible`.
                let holdings = evidence_holdings(conn, &session_id)?;
                devin_state.insert(
                    session_id,
                    serde_json::json!({ "stamp": stamp, "holdings": holdings }),
                );
            }
            Err(err) => {
                coverage.note_unread();
                failures.push(format!("{session_id}: {err:#}"));
            }
        }
    }
    state.insert(SYNC_STATE_KEY.to_string(), Value::Object(devin_state));
    if !failures.is_empty() {
        bail!("devin sessions unreadable: {}", failures.join(", "));
    }
    Ok(inserted)
}

/// Re-index one session for targeted hydration. Returns the prompt count.
pub(crate) fn sync_devin_session(
    conn: &Connection,
    cli_dir: &Path,
    session_id: &str,
) -> Result<usize> {
    let db_path = sessions_db_path(cli_dir);
    let src = open_db_readonly(&db_path)
        .with_context(|| format!("open devin store {}", db_path.display()))?;
    src.execute_batch("PRAGMA query_only = ON; BEGIN DEFERRED")?;
    let result = (|| -> Result<usize> {
        let Some(loaded) = load_from_sqlite(&src, session_id, cli_dir)? else {
            bail!("devin session {session_id} not found");
        };
        let raw_path = db_path.to_string_lossy().to_string();
        let counts = normalize(conn, &loaded, &raw_path)?;
        Ok(counts.prompts)
    })();
    let _ = src.execute_batch("ROLLBACK");
    result
}
