use std::collections::HashMap;
use std::path::Path;

use anyhow::Result;
use serde_json::Value;

use super::super::jsonl;
use super::{millisecond_value, string_field, timestamp_value_ms};

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
    /// The first chunk's ACP `eventId`. Provider-stable, but **not unique**:
    /// Grok reuses it across records, so it is never an identity on its own.
    pub event_id: Option<String>,
    /// The identity this message's event is stored under: the `eventId`
    /// itself for its first occurrence in the stream, and `<eventId>#<n>` for
    /// the *n*-th repeat after it (`#1`, `#2`, …). Keying two messages on one
    /// repeated id would upsert the second over the first and silently drop a
    /// turn; suffixing only the repeats means an append that reuses an id
    /// never renames a message already stored.
    pub event_key: Option<String>,
    pub turn: usize,
    /// Position among every message group of the stream, for the
    /// disambiguation pass.
    seq: usize,
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
    /// The `turn_completed.usage` object, verbatim, when it carries a per-turn
    /// breakdown ([`usage_breakdown`]). Billing evidence for the turn, kept
    /// apart from `total_tokens`.
    pub usage: Option<Value>,
    /// The model the turn ran on: `params.update._meta.modelId` on any of its
    /// rows (the last one wins), else the single key of
    /// `turn_completed.usage.modelUsage` when that map names exactly one.
    pub model: Option<String>,
}

/// The counters a `turn_completed.usage` object carries when it is a per-turn
/// breakdown rather than only a total, in every spelling seen in the field.
/// The same lists `crate::usage` normalizes from.
pub(crate) const GROK_USAGE_INPUT_KEYS: &[&str] = &["inputTokens", "input_tokens", "promptTokens"];
pub(crate) const GROK_USAGE_OUTPUT_KEYS: &[&str] =
    &["outputTokens", "output_tokens", "completionTokens"];
pub(crate) const GROK_USAGE_CACHE_READ_KEYS: &[&str] = &[
    "cachedReadTokens",
    "cacheReadTokens",
    "cache_read_input_tokens",
];
pub(crate) const GROK_USAGE_CACHE_WRITE_KEYS: &[&str] = &[
    "cachedWriteTokens",
    "cacheWriteTokens",
    "cacheCreationTokens",
    "cache_creation_input_tokens",
];
pub(crate) const GROK_USAGE_REASONING_KEYS: &[&str] =
    &["reasoningTokens", "thoughtTokens", "thinkingTokens"];

/// `usage` when it is a per-turn breakdown: an object naming at least one
/// input, output, cache or reasoning counter. A `usage` holding only
/// `totalTokens` is the context snapshot under another name and stays that.
pub(super) fn usage_breakdown(usage: Option<&Value>) -> Option<&Value> {
    let object = usage?.as_object()?;
    [
        GROK_USAGE_INPUT_KEYS,
        GROK_USAGE_OUTPUT_KEYS,
        GROK_USAGE_CACHE_READ_KEYS,
        GROK_USAGE_CACHE_WRITE_KEYS,
        GROK_USAGE_REASONING_KEYS,
    ]
    .iter()
    .flat_map(|keys| keys.iter())
    .any(|key| object.get(*key).is_some_and(|value| !value.is_null()))
    .then_some(usage?)
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
pub(crate) fn parse_updates(contents: &str, path: &Path) -> Result<GrokUpdates> {
    let mut updates = GrokUpdates::default();
    let mut previous_kind: Option<UpdateKind> = None;
    let mut group_seq = 0usize;
    for (number, row) in jsonl::rows(contents).enumerate() {
        let Some(value) = jsonl::parse_row(row, path, number + 1)? else {
            continue;
        };
        apply_update_line(&mut updates, &mut previous_kind, &mut group_seq, &value);
    }
    assign_event_keys(&mut updates);
    Ok(updates)
}

fn apply_update_line(
    updates: &mut GrokUpdates,
    previous_kind: &mut Option<UpdateKind>,
    group_seq: &mut usize,
    value: &Value,
) {
    let params = value.get("params").unwrap_or(&Value::Null);
    let update = params.get("update").unwrap_or(&Value::Null);
    let meta = params.get("_meta").unwrap_or(&Value::Null);
    let Some(raw_kind) = update
        .get("sessionUpdate")
        .or_else(|| update.get("session_update"))
        .and_then(Value::as_str)
    else {
        updates.unread_rows += 1;
        return;
    };
    let kind = update_kind(raw_kind);
    let ts_ms = envelope_timestamp_ms(value, params, update, meta);
    if kind == UpdateKind::Other {
        record_uninterpreted(updates, ts_ms);
        return;
    }
    let turn_start_ms = first_number(&[
        update.get("turnStartMs"),
        meta.get("turnStartMs"),
        value.get("turnStartMs"),
    ]);
    let boundary = turn_boundary(updates, turn_start_ms, kind, *previous_kind);
    if boundary {
        updates.turns.push(TurnTiming {
            start_ms: turn_start_ms,
            ..Default::default()
        });
    }
    let turn = updates.turns.len() - 1;
    note_row_time(updates, turn, ts_ms);
    note_row_model(updates, turn, update);
    let event_id = string_field(meta, &["eventId", "event_id"]);
    // A turn boundary ends the message, even when the next row is the same
    // kind: two `agent_message_chunk`s either side of a new `turnStartMs`
    // are two messages, and merging them leaves the ordinal join with
    // fewer groups than the transcript has records — so every record after
    // the merge takes the previous turn's time.
    let continues = !boundary && *previous_kind == Some(kind);
    match kind {
        UpdateKind::UserMessage | UpdateKind::AgentMessage | UpdateKind::AgentThought => {
            apply_message_chunk(updates, kind, continues, ts_ms, event_id, turn, group_seq);
        }
        UpdateKind::ToolCall | UpdateKind::ToolCallUpdate => {
            apply_tool_update(updates, kind, ts_ms, turn, update);
        }
        UpdateKind::TurnCompleted => {
            apply_turn_completed(updates, ts_ms, turn, update, meta);
        }
        // Handled above, before any turn or coalescing state was touched.
        UpdateKind::Other => {}
    }
    *previous_kind = Some(kind);
}

fn record_uninterpreted(updates: &mut GrokUpdates, ts_ms: Option<i64>) {
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
}

fn turn_boundary(
    updates: &GrokUpdates,
    turn_start_ms: Option<i64>,
    kind: UpdateKind,
    previous_kind: Option<UpdateKind>,
) -> bool {
    // A turn boundary is a change of `turnStartMs`; for a stream that
    // writes none, it is the start of a user message.
    match turn_start_ms {
        Some(start) => updates
            .turns
            .last()
            .is_none_or(|last| last.start_ms != Some(start)),
        None => {
            updates.turns.is_empty()
                || (kind == UpdateKind::UserMessage
                    && previous_kind != Some(UpdateKind::UserMessage))
        }
    }
}

fn note_row_time(updates: &mut GrokUpdates, turn: usize, ts_ms: Option<i64>) {
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
}

fn note_row_model(updates: &mut GrokUpdates, turn: usize, update: &Value) {
    if let Some(model) = update
        .get("_meta")
        .and_then(|update_meta| string_field(update_meta, &["modelId"]))
    {
        if let Some(current) = updates.turns.get_mut(turn) {
            current.model = Some(model);
        }
    }
}

fn apply_message_chunk(
    updates: &mut GrokUpdates,
    kind: UpdateKind,
    continues: bool,
    ts_ms: Option<i64>,
    event_id: Option<String>,
    turn: usize,
    group_seq: &mut usize,
) {
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
            event_key: None,
            turn,
            seq: *group_seq,
        });
        *group_seq += 1;
    }
}

fn apply_tool_update(
    updates: &mut GrokUpdates,
    kind: UpdateKind,
    ts_ms: Option<i64>,
    turn: usize,
    update: &Value,
) {
    let Some(id) = string_field(update, &["toolCallId", "tool_call_id", "id"]) else {
        updates.unread_rows += 1;
        return;
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

fn apply_turn_completed(
    updates: &mut GrokUpdates,
    ts_ms: Option<i64>,
    turn: usize,
    update: &Value,
    meta: &Value,
) {
    let breakdown = usage_breakdown(update.get("usage"));
    // Beside a breakdown, `usage.totalTokens` is that turn's
    // input + output, not the context window, so it is only the
    // proxy when the object carries nothing else.
    let usage_total = if breakdown.is_some() {
        [None, None]
    } else {
        [
            update.pointer("/usage/totalTokens"),
            update.pointer("/usage/total_tokens"),
        ]
    };
    let total = first_number(&[
        update.get("totalTokens"),
        update.get("total_tokens"),
        usage_total[0],
        usage_total[1],
        meta.get("totalTokens"),
    ]);
    if let Some(current) = updates.turns.get_mut(turn) {
        current.total_tokens = total;
        current.usage = breakdown.cloned();
        current.end_ms = ts_ms.or(current.end_ms);
        if current.model.is_none() {
            current.model = breakdown.and_then(single_model_usage_key);
        }
    }
}

/// Give every message group the identity its event is stored under.
///
/// Grok reuses `eventId` across records. The first group in stream order to
/// carry an id keeps it plain — what earlier builds stored — and each later
/// group carrying it is suffixed with its repeat number (`#1`, `#2`, …).
/// Only what came *before* a group decides its key, so appending to the
/// stream never renames a message that is already stored.
fn assign_event_keys(updates: &mut GrokUpdates) {
    let mut groups: Vec<&mut ChunkGroup> = updates
        .user_messages
        .iter_mut()
        .chain(updates.agent_messages.iter_mut())
        .chain(updates.agent_thoughts.iter_mut())
        .collect();
    groups.sort_by_key(|group| group.seq);
    let mut seen: HashMap<String, usize> = HashMap::new();
    for group in groups {
        let Some(id) = group.event_id.clone() else {
            continue;
        };
        let repeats = seen.entry(id.clone()).or_default();
        group.event_key = Some(match *repeats {
            0 => id,
            n => format!("{id}#{n}"),
        });
        *repeats += 1;
    }
}

/// The model a `modelUsage` map names, when it names exactly one: a turn that
/// ran on one model says so there even when no row carries `modelId`.
pub(super) fn single_model_usage_key(usage: &Value) -> Option<String> {
    let map = usage.get("modelUsage")?.as_object()?;
    if map.len() != 1 {
        return None;
    }
    map.keys()
        .next()
        .map(|key| key.trim().to_string())
        .filter(|key| !key.is_empty())
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

pub(super) fn first_number(candidates: &[Option<&Value>]) -> Option<i64> {
    candidates.iter().flatten().find_map(|value| {
        value
            .as_i64()
            .or_else(|| value.as_f64().map(|number| number as i64))
    })
}
