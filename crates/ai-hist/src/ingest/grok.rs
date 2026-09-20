//! Grok CLI ("Grok Build") session record parsing.
//!
//! One place for the interpretation of
//! `~/.grok/sessions/<encoded-cwd>/<session-id>/`, so shallow discovery, full
//! sync and targeted hydration cannot drift apart.
//!
//! ## What Grok actually writes
//!
//! xAI publishes no schema for these files. The shapes handled here were
//! characterized from Grok Build's own bundled user guide and from public
//! write-ups of real session corpora; the provenance of every field — whether
//! it is corroborated by a source that read a real session, inferred from a
//! description, or unverified — is recorded in `docs/session-catalog.md`
//! ("How each adapter works → grok"), which also carries the `jq` checklist a
//! maintainer with Grok installed runs to confirm or correct it. The short
//! version:
//!
//! * `chat_history.jsonl` is the model-facing conversation: one
//!   `ConversationItem` per line, `{"type": …, "content": …}`, with the
//!   variants `system`, `user`, `assistant`, `tool_result`,
//!   `backend_tool_call` and `reasoning`. It carries **no timestamps**, the
//!   typed prompt is wrapped in `<user_query>`, and synthetic context turns
//!   are marked with `synthetic_reason`.
//! * An `assistant` record carries `model_id` and, when the turn called tools,
//!   `tool_calls: [{id, name, arguments}]` — not an OpenAI `function` wrapper.
//!   A `tool_result` is `{type, tool_call_id, content}`.
//! * A `reasoning` record's readable text is its `summary`; the trace itself
//!   is `encrypted_content`, which is opaque and is never stored as thinking.
//! * `updates.jsonl` is the ACP session-update stream and Grok's own guide
//!   calls it "the authoritative conversation log that drives `/resume`". Each
//!   line is `{"timestamp": <epoch s>, "method": "session/update" |
//!   "_x.ai/session/update", "params": {"sessionId", "update":
//!   {"sessionUpdate": <kind>, …}, "_meta": {"eventId", "agentTimestampMs",
//!   …}}}`, with the ACP kinds `user_message_chunk`, `agent_message_chunk`,
//!   `agent_thought_chunk`, `tool_call`, `tool_call_update` and `plan`, plus
//!   the x.ai extensions `turn_completed`, `hook_execution` and `retry_state`.
//! * `turn_completed` carries a `totalTokens` context snapshot, which **can
//!   decrease** when the context is compacted. It is not billing usage, and
//!   this parser records no per-turn input/output token counts.
//!
//! ## The join
//!
//! `chat_history.jsonl` has the content; `updates.jsonl` has the time. Neither
//! file cross-references the other, so the join is:
//!
//! 1. A record that carries its own `timestamp` uses it. (The documented Grok
//!    Build shape has none; an older layout does.)
//! 2. Tool calls and tool results join **by id**: `tool_calls[].id` and
//!    `tool_result.tool_call_id` against the `toolCallId` on a `tool_call` /
//!    `tool_call_update` row. This is exact.
//! 3. Prose joins **by ordinal**: the n-th non-synthetic `user` record takes
//!    the time of the n-th `user_message_chunk` group, and likewise for
//!    `assistant` prose against `agent_message_chunk` and for `reasoning`
//!    summaries against `agent_thought_chunk`. Consecutive rows of one kind
//!    are one group, because Grok streams a message in chunks.
//! 4. Anything left over takes its turn's `turnStartMs`, then the time of the
//!    nearest preceding record that had one, then the session's `created_at`.
//!    Steps 3 and 4 are counted and reported as hydration diagnostics.
//!
//! What is *not* done: no timestamp is ever derived from a record's position
//! in the file. The previous parser stamped prompt *n* with
//! `created_at + n` milliseconds, which is a fabricated fact.

use serde_json::{Map, Value};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// chat_history.jsonl
// ---------------------------------------------------------------------------

/// One tool invocation an assistant record asked for.
#[derive(Debug, Clone)]
pub(crate) struct GrokToolCall {
    /// `tool_calls[].id`. Absent only in layouts that do not write one, and
    /// then the call is keyed on its position instead.
    pub id: Option<String>,
    pub name: String,
    pub arguments: Value,
}

/// One `chat_history.jsonl` line, interpreted.
#[derive(Debug, Clone)]
pub(crate) enum GrokRecord {
    System {
        text: Option<String>,
    },
    User {
        text: Option<String>,
        /// Grok's own injected turn (`synthetic_reason`), not something a
        /// person typed. Kept out of `history` exactly as before.
        synthetic: bool,
    },
    Reasoning {
        summary: Option<String>,
        /// The trace was written as an opaque `encrypted_content` blob.
        encrypted: bool,
    },
    Assistant {
        text: Option<String>,
        model: Option<String>,
        calls: Vec<GrokToolCall>,
    },
    ToolResult {
        call_id: Option<String>,
        text: Option<String>,
        is_error: Option<bool>,
    },
    /// A record type this parser does not interpret. Counted, never guessed at.
    Other,
}

/// A parsed record plus the facts the line itself carried about it.
#[derive(Debug, Clone)]
pub(crate) struct GrokChatLine {
    pub record: GrokRecord,
    /// The record's own timestamp, when the layout writes one.
    pub ts_ms: Option<i64>,
}

/// Interpret one `chat_history.jsonl` line.
pub(crate) fn parse_chat_record(value: &Value) -> GrokChatLine {
    let kind = value
        .get("type")
        .or_else(|| value.get("role"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let content = value.get("content");
    let ts_ms = record_timestamp(value);
    let record = match kind.as_str() {
        "system" => GrokRecord::System {
            text: content.and_then(content_text),
        },
        "user" => GrokRecord::User {
            text: content
                .and_then(content_text)
                .map(|text| unwrap_user_query(&text))
                .filter(|text| !text.is_empty()),
            synthetic: value.get("synthetic_reason").is_some(),
        },
        "reasoning" => GrokRecord::Reasoning {
            summary: value
                .get("summary")
                .and_then(content_text)
                .or_else(|| content.and_then(content_text)),
            encrypted: value.get("encrypted_content").is_some(),
        },
        "assistant" => {
            let mut calls = tool_calls_of(value);
            let text = content.and_then(|content| {
                // A Claude-shaped layout puts tool calls in the content array
                // instead of a top-level `tool_calls`; take those too rather
                // than dropping the evidence.
                calls.extend(content_tool_uses(content));
                content_text(content)
            });
            GrokRecord::Assistant {
                text,
                model: string_field(value, &["model_id", "model"]),
                calls,
            }
        }
        // The guide's `BackendToolCall` variant: a call with no prose.
        "backend_tool_call" => GrokRecord::Assistant {
            text: None,
            model: string_field(value, &["model_id", "model"]),
            calls: tool_calls_of(value),
        },
        "tool_result" => GrokRecord::ToolResult {
            call_id: string_field(value, &["tool_call_id", "tool_use_id", "toolCallId", "id"]),
            text: content.and_then(content_text),
            is_error: value
                .get("is_error")
                .or_else(|| value.get("isError"))
                .and_then(Value::as_bool),
        },
        _ => GrokRecord::Other,
    };
    GrokChatLine { record, ts_ms }
}

/// The tool calls a record declared at the top level.
fn tool_calls_of(value: &Value) -> Vec<GrokToolCall> {
    value
        .get("tool_calls")
        .or_else(|| value.get("toolCalls"))
        .and_then(Value::as_array)
        .map(|calls| calls.iter().filter_map(parse_tool_call).collect())
        .unwrap_or_default()
}

fn parse_tool_call(value: &Value) -> Option<GrokToolCall> {
    let name = string_field(value, &["name", "tool", "tool_name"])?;
    let arguments = value
        .get("arguments")
        .or_else(|| value.get("args"))
        .or_else(|| value.get("input"))
        .cloned()
        .unwrap_or(Value::Null);
    // Some layouts nest the arguments as a JSON *string*.
    let arguments = match arguments.as_str() {
        Some(raw) => serde_json::from_str(raw).unwrap_or(Value::String(raw.to_string())),
        None => arguments,
    };
    Some(GrokToolCall {
        id: string_field(value, &["id", "tool_call_id", "toolCallId"]),
        name,
        arguments,
    })
}

/// `tool_use` blocks inside a content array, for the layout that writes them.
fn content_tool_uses(content: &Value) -> Vec<GrokToolCall> {
    let Some(items) = content.as_array() else {
        return Vec::new();
    };
    items
        .iter()
        .filter(|item| item.get("type").and_then(Value::as_str) == Some("tool_use"))
        .filter_map(parse_tool_call)
        .collect()
}

/// The readable text of a `content` value, whatever shape it takes.
pub(crate) fn content_text(content: &Value) -> Option<String> {
    let mut parts: Vec<String> = Vec::new();
    match content {
        Value::String(text) => parts.push(text.clone()),
        Value::Array(items) => {
            for item in items {
                if let Some(text) = item.as_str() {
                    parts.push(text.to_string());
                    continue;
                }
                let kind = item.get("type").and_then(Value::as_str);
                if matches!(kind, None | Some("text") | Some("output_text")) {
                    if let Some(text) = item.get("text").and_then(Value::as_str) {
                        parts.push(text.to_string());
                    }
                }
            }
        }
        Value::Object(_) => {
            if let Some(text) = content.get("text").and_then(Value::as_str) {
                parts.push(text.to_string());
            }
        }
        _ => {}
    }
    let text = parts
        .iter()
        .map(|part| part.trim())
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    (!text.is_empty()).then_some(text)
}

/// Strip the `<user_query>` envelope Grok wraps a typed prompt in, so the
/// stored prompt is what the person actually typed.
pub(crate) fn unwrap_user_query(text: &str) -> String {
    let Some(start) = text.find("<user_query>") else {
        return text.trim().to_string();
    };
    let rest = &text[start + "<user_query>".len()..];
    match rest.find("</user_query>") {
        Some(end) => rest[..end].trim().to_string(),
        None => rest.trim().to_string(),
    }
}

fn string_field(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_str))
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_string)
}

/// A timestamp a record carried itself, as epoch milliseconds.
fn record_timestamp(value: &Value) -> Option<i64> {
    for key in ["timestamp_ms", "timestampMs"] {
        if let Some(found) = value.get(key).and_then(millisecond_value) {
            return Some(found);
        }
    }
    for key in ["timestamp", "ts", "created_at"] {
        if let Some(found) = value.get(key).and_then(timestamp_value_ms) {
            return Some(found);
        }
    }
    None
}

/// A field whose **name** says milliseconds (`agentTimestampMs`,
/// `turnStartMs`, …), read as milliseconds without guessing at the unit.
///
/// The inference in [`timestamp_value_ms`] exists for `timestamp`, which Grok
/// writes in seconds; applying it to a field already named `…Ms` would
/// multiply a small but real millisecond value by a thousand.
pub(crate) fn millisecond_value(value: &Value) -> Option<i64> {
    if let Some(text) = value.as_str() {
        return crate::parse_iso_ms(text);
    }
    let number = value.as_f64()?;
    (number.is_finite() && number > 0.0).then_some(number as i64)
}

/// Epoch milliseconds from a timestamp field written as an RFC 3339 string, as
/// epoch seconds, or as epoch milliseconds.
///
/// Grok's `updates.jsonl` envelope timestamp is in **seconds** while
/// `agentTimestampMs` is in milliseconds, so the unit has to be inferred:
/// anything at or above `10^12` is already milliseconds (that threshold is
/// September 2001 in milliseconds and the year 33658 in seconds).
pub(crate) fn timestamp_value_ms(value: &Value) -> Option<i64> {
    const MILLISECOND_FLOOR: f64 = 1e12;
    if let Some(text) = value.as_str() {
        return crate::parse_iso_ms(text);
    }
    let number = value.as_f64()?;
    if !number.is_finite() || number <= 0.0 {
        return None;
    }
    Some(if number >= MILLISECOND_FLOOR {
        number as i64
    } else {
        (number * 1000.0).round() as i64
    })
}

// ---------------------------------------------------------------------------
// tools
// ---------------------------------------------------------------------------

/// Lower-case the tool name and drop any provider namespace prefix, so
/// `StrReplace`, `str_replace` and `functions.StrReplace` classify alike.
fn normalize_tool_name(name: &str) -> String {
    name.rsplit('.')
        .next()
        .unwrap_or(name)
        .replace(['_', '-'], "")
        .to_ascii_lowercase()
}

/// Tools whose call writes to a file, and therefore produces a `file_edits`
/// row. The names are the ones burn #489 observed Grok emit, plus the
/// snake_case spellings of the same tools.
pub(crate) fn is_file_edit_tool(name: &str) -> bool {
    matches!(
        normalize_tool_name(name).as_str(),
        "write"
            | "strreplace"
            | "edit"
            | "editfile"
            | "writefile"
            | "createfile"
            | "applypatch"
            | "multiedit"
            | "strreplaceeditor"
            | "editnotebook"
    )
}

/// Tools whose call delegates work to a Grok subagent.
pub(crate) fn is_subagent_tool(name: &str) -> bool {
    matches!(
        normalize_tool_name(name).as_str(),
        "task" | "subagent" | "spawnagent"
    )
}

/// The file a Grok tool call touched, or the command it ran.
pub(crate) fn pick_tool_target(name: &str, arguments: &Value) -> Option<String> {
    if let Some(patch) = arguments.as_str() {
        return patch_target(patch).or_else(|| Some(first_line(patch)));
    }
    let object = arguments.as_object()?;
    let get = |key: &str| {
        object
            .get(key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    };
    if let Some(target) = object
        .get("patch")
        .or_else(|| object.get("diff"))
        .and_then(Value::as_str)
        .and_then(patch_target)
    {
        return Some(target);
    }
    match normalize_tool_name(name).as_str() {
        "shell" | "bash" | "run" | "runterminalcmd" => get("command").or_else(|| get("cmd")),
        "grep" | "glob" | "search" => get("pattern")
            .or_else(|| get("query"))
            .or_else(|| get("path")),
        "websearch" => get("query"),
        "webfetch" => get("url"),
        "callmcptool" | "usetool" | "searchtool" => get("tool")
            .or_else(|| get("name"))
            .or_else(|| get("server")),
        "task" | "subagent" | "spawnagent" => get("description")
            .or_else(|| get("prompt"))
            .or_else(|| get("agent_type")),
        _ => get("path")
            .or_else(|| get("file_path"))
            .or_else(|| get("filePath"))
            .or_else(|| get("target_file"))
            .or_else(|| get("notebook_path"))
            .or_else(|| get("url"))
            .or_else(|| get("query"))
            .or_else(|| get("command")),
    }
}

/// The file named by a unified diff or an `apply_patch` envelope.
fn patch_target(patch: &str) -> Option<String> {
    for line in patch.lines() {
        let line = line.trim();
        for marker in [
            "*** Update File: ",
            "*** Add File: ",
            "*** Delete File: ",
            "*** Move to: ",
        ] {
            if let Some(rest) = line.strip_prefix(marker) {
                let rest = rest.trim();
                if !rest.is_empty() {
                    return Some(rest.to_string());
                }
            }
        }
        if let Some(rest) = line.strip_prefix("+++ ") {
            let rest = rest.trim();
            let rest = rest.strip_prefix("b/").unwrap_or(rest);
            if !rest.is_empty() && rest != "/dev/null" {
                return Some(rest.to_string());
            }
        }
    }
    None
}

fn first_line(text: &str) -> String {
    text.lines().next().unwrap_or_default().trim().to_string()
}

// ---------------------------------------------------------------------------
// updates.jsonl
// ---------------------------------------------------------------------------

/// The ACP update kinds this parser interprets. Everything else Grok streams
/// (`plan`, `hook_execution`, `retry_state`, …) is counted and skipped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UpdateKind {
    UserMessage,
    AgentMessage,
    AgentThought,
    ToolCall,
    ToolCallUpdate,
    TurnCompleted,
    Other,
}

fn update_kind(raw: &str) -> UpdateKind {
    match raw {
        "user_message_chunk" => UpdateKind::UserMessage,
        "agent_message_chunk" => UpdateKind::AgentMessage,
        "agent_thought_chunk" => UpdateKind::AgentThought,
        "tool_call" => UpdateKind::ToolCall,
        "tool_call_update" => UpdateKind::ToolCallUpdate,
        "turn_completed" => UpdateKind::TurnCompleted,
        _ => UpdateKind::Other,
    }
}

/// One coalesced run of consecutive updates of the same kind: Grok streams a
/// message as many chunks, and one chunk is not one message.
#[derive(Debug, Clone)]
pub(crate) struct ChunkGroup {
    /// The first chunk's time — when the message started, not when it ended.
    pub ts_ms: Option<i64>,
    /// The first chunk's ACP `eventId`, which is a provider-stable identity
    /// for the message; `chat_history.jsonl` has none of its own.
    pub event_id: Option<String>,
    pub turn: usize,
}

/// When a tool call started and when its result came back.
#[derive(Debug, Clone, Default)]
pub(crate) struct ToolTiming {
    pub started_ms: Option<i64>,
    pub finished_ms: Option<i64>,
    pub status: Option<String>,
    pub turn: Option<usize>,
}

/// One turn, as `updates.jsonl` recorded it.
#[derive(Debug, Clone, Default)]
pub(crate) struct TurnTiming {
    pub start_ms: Option<i64>,
    pub end_ms: Option<i64>,
    /// The `turn_completed` context-window snapshot. A proxy, not billing
    /// usage, and it can decrease after a compaction.
    pub total_tokens: Option<i64>,
}

/// Everything `updates.jsonl` establishes about a session's timing.
#[derive(Debug, Clone, Default)]
pub(crate) struct GrokUpdates {
    pub user_messages: Vec<ChunkGroup>,
    pub agent_messages: Vec<ChunkGroup>,
    pub agent_thoughts: Vec<ChunkGroup>,
    pub tools: HashMap<String, ToolTiming>,
    pub turns: Vec<TurnTiming>,
    pub first_ms: Option<i64>,
    pub last_ms: Option<i64>,
    /// Rows that parsed as JSON but carried no `sessionUpdate` this parser
    /// interprets. Reported, never guessed at.
    pub unread_rows: usize,
}

impl GrokUpdates {
    pub(crate) fn is_empty(&self) -> bool {
        self.user_messages.is_empty()
            && self.agent_messages.is_empty()
            && self.agent_thoughts.is_empty()
            && self.tools.is_empty()
            && self.turns.is_empty()
    }
}

/// Read `updates.jsonl` into the timing facts the join needs.
pub(crate) fn parse_updates(contents: &str) -> GrokUpdates {
    let mut updates = GrokUpdates::default();
    let mut previous_kind: Option<UpdateKind> = None;
    let mut turn;
    for line in contents.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let params = value.get("params").unwrap_or(&Value::Null);
        let update = params.get("update").unwrap_or(&Value::Null);
        let meta = params.get("_meta").unwrap_or(&Value::Null);
        let Some(raw_kind) = update
            .get("sessionUpdate")
            .or_else(|| update.get("session_update"))
            .and_then(Value::as_str)
        else {
            updates.unread_rows += 1;
            continue;
        };
        let kind = update_kind(raw_kind);
        let ts_ms = envelope_timestamp_ms(&value, params, update, meta);
        if kind == UpdateKind::Other {
            // A kind this parser does not interpret is not a break in the
            // conversation. Grok interleaves `plan`, `hook_execution` and
            // `retry_state` rows into a streaming message, and treating one as
            // a boundary would split that message into two chunk groups --
            // which shifts every later ordinal join by one and hands the
            // following user, assistant and thinking events somebody else's
            // timestamp. So it updates the session's time bounds, which are a
            // real fact about it, and touches nothing else: not the turn,
            // not `previous_kind`.
            updates.unread_rows += 1;
            if let Some(ts) = ts_ms {
                updates.first_ms = Some(updates.first_ms.map_or(ts, |first| first.min(ts)));
                updates.last_ms = Some(updates.last_ms.map_or(ts, |last| last.max(ts)));
            }
            continue;
        }
        let turn_start_ms = first_number(&[
            update.get("turnStartMs"),
            meta.get("turnStartMs"),
            value.get("turnStartMs"),
        ]);
        // A turn boundary is a change of `turnStartMs`; for a stream that
        // writes none, it is the start of a user message.
        let boundary = match turn_start_ms {
            Some(start) => updates
                .turns
                .last()
                .is_none_or(|last| last.start_ms != Some(start)),
            None => {
                updates.turns.is_empty()
                    || (kind == UpdateKind::UserMessage
                        && previous_kind != Some(UpdateKind::UserMessage))
            }
        };
        if boundary {
            updates.turns.push(TurnTiming {
                start_ms: turn_start_ms,
                ..Default::default()
            });
        }
        turn = updates.turns.len() - 1;
        if let Some(ts) = ts_ms {
            updates.first_ms = Some(updates.first_ms.map_or(ts, |first| first.min(ts)));
            updates.last_ms = Some(updates.last_ms.map_or(ts, |last| last.max(ts)));
            if let Some(current) = updates.turns.get_mut(turn) {
                current.end_ms = Some(current.end_ms.map_or(ts, |end| end.max(ts)));
                if current.start_ms.is_none() {
                    current.start_ms = Some(ts);
                }
            }
        }
        let event_id = string_field(meta, &["eventId", "event_id"]);
        // A turn boundary ends the message, even when the next row is the same
        // kind: two `agent_message_chunk`s either side of a new `turnStartMs`
        // are two messages, and merging them leaves the ordinal join with
        // fewer groups than the transcript has records — so every record after
        // the merge takes the previous turn's time.
        let continues = !boundary && previous_kind == Some(kind);
        match kind {
            UpdateKind::UserMessage | UpdateKind::AgentMessage | UpdateKind::AgentThought => {
                let bucket = match kind {
                    UpdateKind::UserMessage => &mut updates.user_messages,
                    UpdateKind::AgentMessage => &mut updates.agent_messages,
                    _ => &mut updates.agent_thoughts,
                };
                if continues {
                    if let Some(group) = bucket.last_mut() {
                        if group.ts_ms.is_none() {
                            group.ts_ms = ts_ms;
                        }
                        if group.event_id.is_none() {
                            group.event_id = event_id;
                        }
                    }
                } else {
                    bucket.push(ChunkGroup {
                        ts_ms,
                        event_id,
                        turn,
                    });
                }
            }
            UpdateKind::ToolCall | UpdateKind::ToolCallUpdate => {
                let Some(id) = string_field(update, &["toolCallId", "tool_call_id", "id"]) else {
                    updates.unread_rows += 1;
                    previous_kind = Some(kind);
                    continue;
                };
                let timing = updates.tools.entry(id).or_default();
                timing.turn = Some(turn);
                if kind == UpdateKind::ToolCall {
                    if timing.started_ms.is_none() {
                        timing.started_ms = ts_ms;
                    }
                } else {
                    timing.finished_ms = ts_ms.or(timing.finished_ms);
                }
                if let Some(status) = string_field(update, &["status"]) {
                    timing.status = Some(status);
                }
            }
            UpdateKind::TurnCompleted => {
                let total = first_number(&[
                    update.get("totalTokens"),
                    update.get("total_tokens"),
                    update.pointer("/usage/totalTokens"),
                    update.pointer("/usage/total_tokens"),
                    meta.get("totalTokens"),
                ]);
                if let Some(current) = updates.turns.get_mut(turn) {
                    current.total_tokens = total;
                    current.end_ms = ts_ms.or(current.end_ms);
                }
            }
            // Handled above, before any turn or coalescing state was touched.
            UpdateKind::Other => {}
        }
        previous_kind = Some(kind);
    }
    updates
}

/// The time one whole `updates.jsonl` line recorded, for a caller that has the
/// line but not the pieces — shallow discovery reads the stream's first and
/// last record this way.
pub(crate) fn line_timestamp_ms(value: &Value) -> Option<i64> {
    let params = value.get("params").unwrap_or(&Value::Null);
    let update = params.get("update").unwrap_or(&Value::Null);
    let meta = params.get("_meta").unwrap_or(&Value::Null);
    envelope_timestamp_ms(value, params, update, meta)
}

/// The time an ACP envelope recorded, preferring the agent's own millisecond
/// clock over the envelope's epoch-second `timestamp`.
fn envelope_timestamp_ms(
    value: &Value,
    params: &Value,
    update: &Value,
    meta: &Value,
) -> Option<i64> {
    for candidate in [meta.get("agentTimestampMs"), meta.get("timestampMs")] {
        if let Some(found) = candidate.and_then(millisecond_value) {
            return Some(found);
        }
    }
    for candidate in [
        meta.get("timestamp"),
        update.get("timestamp"),
        params.get("timestamp"),
        value.get("timestamp"),
    ] {
        if let Some(found) = candidate.and_then(timestamp_value_ms) {
            return Some(found);
        }
    }
    None
}

fn first_number(candidates: &[Option<&Value>]) -> Option<i64> {
    candidates.iter().flatten().find_map(|value| {
        value
            .as_i64()
            .or_else(|| value.as_f64().map(|number| number as i64))
    })
}

// ---------------------------------------------------------------------------
// signals.json, subagents/, compaction_checkpoints/
// ---------------------------------------------------------------------------

/// The session aggregates `signals.json` records, as far as they are named in
/// public descriptions of the file. Unknown keys are preserved verbatim in the
/// marker payload rather than being dropped or renamed.
#[derive(Debug, Clone, Default)]
pub(crate) struct GrokSignals {
    pub turn_count: Option<i64>,
    pub compaction_count: Option<i64>,
    pub context_tokens_used: Option<i64>,
    pub raw: Map<String, Value>,
}

pub(crate) fn parse_signals(value: &Value) -> GrokSignals {
    let object = value.as_object().cloned().unwrap_or_default();
    let number =
        |keys: &[&str]| first_number(&keys.iter().map(|key| object.get(*key)).collect::<Vec<_>>());
    GrokSignals {
        turn_count: number(&["turnCount", "turn_count", "turns"]),
        compaction_count: number(&["compactionCount", "compaction_count", "compactions"]),
        context_tokens_used: number(&["contextTokensUsed", "context_tokens_used", "contextTokens"]),
        raw: object,
    }
}

/// What one file in `subagents/` says about a delegated child.
///
/// Grok's guide describes the directory as "per-subagent metadata; child
/// sessions live in the normal sessions tree", so a file that names a child
/// session id is a linked delegation and one that does not is unlinked
/// evidence. The child id is **never** taken from the file name.
#[derive(Debug, Clone, Default)]
pub(crate) struct GrokSubagent {
    pub child_session_id: Option<String>,
    pub agent_type: Option<String>,
    pub agent_name: Option<String>,
    pub model: Option<String>,
    pub spawned_at_ms: Option<i64>,
}

pub(crate) fn parse_subagent(value: &Value) -> GrokSubagent {
    GrokSubagent {
        child_session_id: string_field(
            value,
            &["session_id", "sessionId", "child_session_id", "id"],
        ),
        agent_type: string_field(value, &["agent_type", "agentType", "type", "kind"]),
        agent_name: string_field(value, &["name", "title", "description", "label"]),
        model: string_field(value, &["model", "model_id", "modelId"]),
        spawned_at_ms: [
            "created_at",
            "createdAt",
            "started_at",
            "spawned_at",
            "timestamp",
        ]
        .iter()
        .find_map(|key| value.get(*key).and_then(timestamp_value_ms))
        .or_else(|| {
            ["createdAtMs", "spawnedAtMs", "startedAtMs"]
                .iter()
                .find_map(|key| value.get(*key).and_then(millisecond_value))
        }),
    }
}

/// When one `compaction_checkpoints/` entry was taken, if it says.
pub(crate) fn compaction_timestamp_ms(value: &Value) -> Option<i64> {
    ["turnStartMs", "timestampMs"]
        .iter()
        .find_map(|key| value.get(*key).and_then(millisecond_value))
        .or_else(|| {
            [
                "created_at",
                "createdAt",
                "timestamp",
                "ts",
                "compacted_at",
                "compactedAt",
            ]
            .iter()
            .find_map(|key| value.get(*key).and_then(timestamp_value_ms))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_user_record_keeps_only_what_the_person_typed() {
        let line = parse_chat_record(&json!({
            "type": "user",
            "content": "<user_query>ship it</user_query>",
        }));
        match line.record {
            GrokRecord::User { text, synthetic } => {
                assert_eq!(text.as_deref(), Some("ship it"));
                assert!(!synthetic);
            }
            other => panic!("expected a user record, got {other:?}"),
        }
    }

    #[test]
    fn a_synthetic_turn_is_marked_rather_than_dropped_here() {
        let line = parse_chat_record(&json!({
            "type": "user",
            "content": "reminder",
            "synthetic_reason": "compaction",
        }));
        assert!(matches!(
            line.record,
            GrokRecord::User {
                synthetic: true,
                ..
            }
        ));
    }

    #[test]
    fn an_assistant_record_carries_its_model_and_its_calls() {
        let line = parse_chat_record(&json!({
            "type": "assistant",
            "content": "patching",
            "model_id": "grok-4-build",
            "tool_calls": [{"id": "call_1", "name": "Shell", "arguments": {"command": "ls"}}],
        }));
        match line.record {
            GrokRecord::Assistant { text, model, calls } => {
                assert_eq!(text.as_deref(), Some("patching"));
                assert_eq!(model.as_deref(), Some("grok-4-build"));
                assert_eq!(calls.len(), 1);
                assert_eq!(calls[0].id.as_deref(), Some("call_1"));
                assert_eq!(
                    pick_tool_target(&calls[0].name, &calls[0].arguments).as_deref(),
                    Some("ls")
                );
            }
            other => panic!("expected an assistant record, got {other:?}"),
        }
    }

    #[test]
    fn a_content_block_tool_use_is_read_as_a_call_too() {
        let line = parse_chat_record(&json!({
            "type": "assistant",
            "content": [
                {"type": "text", "text": "editing"},
                {"type": "tool_use", "id": "t1", "name": "str_replace_editor",
                 "input": {"path": "/tmp/a.ts", "command": "str_replace"}},
            ],
        }));
        match line.record {
            GrokRecord::Assistant { calls, .. } => {
                assert_eq!(calls.len(), 1);
                assert!(is_file_edit_tool("StrReplace"));
                assert_eq!(
                    pick_tool_target(&calls[0].name, &calls[0].arguments).as_deref(),
                    Some("/tmp/a.ts")
                );
            }
            other => panic!("expected an assistant record, got {other:?}"),
        }
    }

    #[test]
    fn reasoning_keeps_the_summary_and_flags_the_encrypted_trace() {
        let line = parse_chat_record(&json!({
            "type": "reasoning",
            "summary": "look at the client",
            "encrypted_content": "b64",
        }));
        match line.record {
            GrokRecord::Reasoning { summary, encrypted } => {
                assert_eq!(summary.as_deref(), Some("look at the client"));
                assert!(encrypted);
            }
            other => panic!("expected a reasoning record, got {other:?}"),
        }
    }

    #[test]
    fn a_field_named_in_milliseconds_is_never_rescaled() {
        // A field named `…Ms` is milliseconds even when it is small; running
        // it through the seconds/milliseconds inference would multiply a real
        // value by a thousand.
        let stream = r#"{"method":"session/update","params":{"update":{"sessionUpdate":"agent_message_chunk"},"_meta":{"agentTimestampMs":1000}}}"#;
        let updates = parse_updates(stream);
        assert_eq!(updates.agent_messages[0].ts_ms, Some(1000));
    }

    #[test]
    fn an_envelope_timestamp_in_seconds_becomes_milliseconds() {
        assert_eq!(
            timestamp_value_ms(&json!(1_789_560_000)),
            Some(1_789_560_000_000)
        );
        assert_eq!(
            timestamp_value_ms(&json!(1_789_560_000_000i64)),
            Some(1_789_560_000_000)
        );
        assert_eq!(
            timestamp_value_ms(&json!("2026-09-16T12:00:00.000Z")),
            Some(1_789_560_000_000)
        );
    }

    #[test]
    fn consecutive_chunks_of_one_message_coalesce_into_one_group() {
        let stream = [
            r#"{"timestamp":1789560000,"method":"session/update","params":{"update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"one "}},"_meta":{"eventId":"a","agentTimestampMs":1789560000000,"turnStartMs":1789560000000}}}"#,
            r#"{"timestamp":1789560001,"method":"session/update","params":{"update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"two"}},"_meta":{"eventId":"b","agentTimestampMs":1789560001000,"turnStartMs":1789560000000}}}"#,
        ]
        .join("\n");
        let updates = parse_updates(&stream);
        assert_eq!(updates.agent_messages.len(), 1);
        assert_eq!(updates.agent_messages[0].ts_ms, Some(1_789_560_000_000));
        assert_eq!(updates.agent_messages[0].event_id.as_deref(), Some("a"));
    }

    /// A turn boundary ends a message. Two `agent_message_chunk` rows either
    /// side of a new `turnStartMs` are two messages, and merging them leaves
    /// the ordinal join with fewer groups than the transcript has records —
    /// after which every later record takes the previous turn's time.
    #[test]
    fn a_turn_boundary_ends_a_message_even_between_two_rows_of_one_kind() {
        let stream = [
            r#"{"method":"session/update","params":{"update":{"sessionUpdate":"agent_message_chunk"},"_meta":{"eventId":"a","agentTimestampMs":1000,"turnStartMs":1000}}}"#,
            r#"{"method":"session/update","params":{"update":{"sessionUpdate":"agent_message_chunk"},"_meta":{"eventId":"b","agentTimestampMs":5000,"turnStartMs":5000}}}"#,
        ]
        .join("\n");
        let updates = parse_updates(&stream);
        assert_eq!(updates.agent_messages.len(), 2, "two turns, two messages");
        assert_eq!(updates.agent_messages[0].ts_ms, Some(1000));
        assert_eq!(updates.agent_messages[0].turn, 0);
        assert_eq!(updates.agent_messages[1].ts_ms, Some(5000));
        assert_eq!(updates.agent_messages[1].turn, 1);
        assert_eq!(updates.turns.len(), 2);
    }

    /// The positive control for the rule above: inside one turn, a streamed
    /// message is still one message however many chunks it arrives in.
    #[test]
    fn chunks_inside_one_turn_still_coalesce() {
        let stream = [
            r#"{"method":"session/update","params":{"update":{"sessionUpdate":"agent_message_chunk"},"_meta":{"eventId":"a","agentTimestampMs":1000,"turnStartMs":1000}}}"#,
            r#"{"method":"session/update","params":{"update":{"sessionUpdate":"agent_message_chunk"},"_meta":{"eventId":"b","agentTimestampMs":1100,"turnStartMs":1000}}}"#,
            r#"{"method":"session/update","params":{"update":{"sessionUpdate":"agent_message_chunk"},"_meta":{"eventId":"c","agentTimestampMs":1200,"turnStartMs":1000}}}"#,
        ]
        .join("\n");
        let updates = parse_updates(&stream);
        assert_eq!(updates.agent_messages.len(), 1);
        assert_eq!(updates.agent_messages[0].ts_ms, Some(1000));
        assert_eq!(updates.turns.len(), 1);
    }

    #[test]
    fn a_tool_call_and_its_update_share_one_timing_row() {
        let stream = [
            r#"{"method":"session/update","params":{"update":{"sessionUpdate":"tool_call","toolCallId":"c1"},"_meta":{"agentTimestampMs":1000,"turnStartMs":500}}}"#,
            r#"{"method":"session/update","params":{"update":{"sessionUpdate":"tool_call_update","toolCallId":"c1","status":"failed"},"_meta":{"agentTimestampMs":2000,"turnStartMs":500}}}"#,
        ]
        .join("\n");
        let updates = parse_updates(&stream);
        let timing = updates.tools.get("c1").expect("timing for c1");
        assert_eq!(timing.started_ms, Some(1000));
        assert_eq!(timing.finished_ms, Some(2000));
        assert_eq!(timing.status.as_deref(), Some("failed"));
    }

    #[test]
    fn turn_completed_records_the_context_proxy_for_its_turn() {
        let stream = [
            r#"{"method":"session/update","params":{"update":{"sessionUpdate":"user_message_chunk"},"_meta":{"agentTimestampMs":1000,"turnStartMs":1000}}}"#,
            r#"{"method":"_x.ai/session/update","params":{"update":{"sessionUpdate":"turn_completed","totalTokens":4242},"_meta":{"agentTimestampMs":2000,"turnStartMs":1000}}}"#,
            r#"{"method":"session/update","params":{"update":{"sessionUpdate":"user_message_chunk"},"_meta":{"agentTimestampMs":3000,"turnStartMs":3000}}}"#,
            r#"{"method":"_x.ai/session/update","params":{"update":{"sessionUpdate":"turn_completed","usage":{"totalTokens":99}},"_meta":{"agentTimestampMs":4000,"turnStartMs":3000}}}"#,
        ]
        .join("\n");
        let updates = parse_updates(&stream);
        assert_eq!(updates.turns.len(), 2);
        assert_eq!(updates.turns[0].total_tokens, Some(4242));
        // The proxy can fall between turns; nothing here treats that as an error.
        assert_eq!(updates.turns[1].total_tokens, Some(99));
        assert_eq!(updates.user_messages.len(), 2);
        assert_eq!(updates.user_messages[1].turn, 1);
    }

    /// Grok interleaves `plan`, `hook_execution` and `retry_state` rows into
    /// a streaming message. A kind this parser does not interpret is not a
    /// break in the conversation: treating one as a boundary splits a message
    /// into two groups, and every later ordinal join then reads one group too
    /// far and hands the next message somebody else's timestamp.
    #[test]
    fn a_skipped_update_kind_does_not_split_a_streamed_message() {
        let stream = [
            r#"{"method":"session/update","params":{"update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"one "}},"_meta":{"eventId":"a","agentTimestampMs":1000,"turnStartMs":1000}}}"#,
            r#"{"method":"session/update","params":{"update":{"sessionUpdate":"plan","entries":[]},"_meta":{"eventId":"p","agentTimestampMs":1500,"turnStartMs":1000}}}"#,
            r#"{"method":"session/update","params":{"update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"two"}},"_meta":{"eventId":"b","agentTimestampMs":2000,"turnStartMs":1000}}}"#,
            // A tool call is a real boundary, so what follows it is a second
            // message. The `plan` row above is not.
            r#"{"method":"session/update","params":{"update":{"sessionUpdate":"tool_call","toolCallId":"c1"},"_meta":{"agentTimestampMs":3000,"turnStartMs":1000}}}"#,
            r#"{"method":"session/update","params":{"update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"next"}},"_meta":{"eventId":"c","agentTimestampMs":5000,"turnStartMs":1000}}}"#,
        ]
        .join("\n");
        let updates = parse_updates(&stream);
        // Two messages: the streamed one, and the one after it. Not three.
        assert_eq!(updates.agent_messages.len(), 2);
        assert_eq!(updates.agent_messages[0].ts_ms, Some(1000));
        assert_eq!(updates.agent_messages[0].event_id.as_deref(), Some("a"));
        // The second assistant record must get 5000. With the plan row
        // counted as a boundary it would get 2000 -- the middle of the first
        // message -- and everything after it would be wrong too.
        assert_eq!(updates.agent_messages[1].ts_ms, Some(5000));
        assert_eq!(updates.unread_rows, 1);
        // The skipped row is still real activity, so it still bounds the
        // session; it just does not divide it.
        assert_eq!(updates.first_ms, Some(1000));
        assert_eq!(updates.last_ms, Some(5000));
        assert_eq!(updates.turns.len(), 1);
    }

    /// A skipped row carrying a `turnStartMs` must not open a turn either.
    #[test]
    fn a_skipped_update_kind_does_not_open_a_turn() {
        let stream = [
            r#"{"method":"session/update","params":{"update":{"sessionUpdate":"hook_execution"},"_meta":{"agentTimestampMs":10,"turnStartMs":10}}}"#,
            r#"{"method":"session/update","params":{"update":{"sessionUpdate":"user_message_chunk"},"_meta":{"agentTimestampMs":1000,"turnStartMs":1000}}}"#,
            r#"{"method":"_x.ai/session/update","params":{"update":{"sessionUpdate":"turn_completed","totalTokens":77},"_meta":{"agentTimestampMs":2000,"turnStartMs":1000}}}"#,
        ]
        .join("\n");
        let updates = parse_updates(&stream);
        assert_eq!(updates.turns.len(), 1);
        assert_eq!(updates.turns[0].start_ms, Some(1000));
        assert_eq!(updates.turns[0].total_tokens, Some(77));
        assert_eq!(updates.user_messages[0].turn, 0);
    }

    #[test]
    fn an_uninterpreted_update_kind_is_counted_not_guessed_at() {
        let stream =
            r#"{"method":"session/update","params":{"update":{"sessionUpdate":"hook_execution"}}}"#;
        let updates = parse_updates(stream);
        assert_eq!(updates.unread_rows, 1);
        assert!(updates.agent_messages.is_empty());
    }
}
