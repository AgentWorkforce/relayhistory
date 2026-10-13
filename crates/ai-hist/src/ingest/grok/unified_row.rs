use std::collections::HashMap;

use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use super::updates::{
    first_number, single_model_usage_key, usage_breakdown, GROK_USAGE_CACHE_READ_KEYS,
    GROK_USAGE_CACHE_WRITE_KEYS, GROK_USAGE_INPUT_KEYS, GROK_USAGE_OUTPUT_KEYS,
    GROK_USAGE_REASONING_KEYS,
};
use super::{string_field, timestamp_value_ms};

// ---------------------------------------------------------------------------
// logs/unified.jsonl
// ---------------------------------------------------------------------------

/// One per-inference usage record from `logs/unified.jsonl`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GrokUnifiedUsage {
    pub session_id: String,
    pub pid: Option<i64>,
    /// The row's own model, else the one its process last named.
    pub model: Option<String>,
    pub ts_ms: Option<i64>,
    /// The counters: the row's `usage` object verbatim, or — for a row that
    /// writes them at its top level — just the counter keys projected out of
    /// it, so a whole log row is never stored as "usage".
    pub usage: Value,
    /// `event_id`/`eventId`, kept as a fact about the row. Not its identity:
    /// Grok reuses it across usage records.
    pub event_id: Option<String>,
}

/// What one `logs/unified.jsonl` row is.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum GrokUnifiedRow {
    /// A per-inference token breakdown naming its session.
    Usage(GrokUnifiedUsage),
    /// A breakdown that names no session. Counted; there is nothing to attach
    /// it to, and guessing one from the pid would be invention.
    Unattributed,
    /// A row naming a process's model without a breakdown — a process start
    /// or a model change. Later usage rows from that pid with no model of
    /// their own take it.
    ModelChange { pid: i64, model: String },
    /// Anything else the process logged.
    Other,
}

/// The counter keys a unified row's top level is projected down to when it
/// carries no `usage` object of its own.
const UNIFIED_PROJECTED_KEYS: &[&str] =
    &["totalTokens", "total_tokens", "costUsdTicks", "modelUsage"];

/// Session id spellings. Grok Build writes top-level `sid` (the session
/// directory name). The others are still accepted.
const SESSION_ID_KEYS: &[&str] = &["sid", "session_id", "sessionId"];

/// `ctx` counters on a Grok Build inference row, and the one spelling
/// `crate::usage` reads for each. Only the canonical name is stored: a
/// normalizer that summed every matching key would count the inference twice.
const CTX_COUNTERS: &[(&str, &str)] = &[
    ("prompt_tokens", "inputTokens"),
    ("cached_prompt_tokens", "cachedReadTokens"),
    ("completion_tokens", "outputTokens"),
    ("reasoning_tokens", "reasoningTokens"),
];

/// Interpret one `logs/unified.jsonl` row.
///
/// A Grok Build usage row is `msg` `shell.turn.inference_done`: session id
/// `sid`, time `ts`, process `pid`, and the four counters under `ctx`.
/// `ctx.model_elapsed_ms` is a duration, not a model id. A `usage` object or
/// top-level counters in the `turn_completed.usage` spellings are still
/// accepted. Auth, startup, and tool rows carry none of those counters.
pub(crate) fn parse_unified_row(
    value: &Value,
    pid_models: &HashMap<i64, String>,
) -> GrokUnifiedRow {
    let ctx = value.get("ctx").unwrap_or(&Value::Null);
    let pid = first_number(&[value.get("pid"), ctx.get("pid")]);
    let model = row_string(value, ctx, &["model_id", "modelId", "model"]);
    let Some(usage) = row_usage(value, ctx) else {
        return model_change_or_other(pid, model);
    };
    let Some(session_id) = row_string(value, ctx, SESSION_ID_KEYS) else {
        return GrokUnifiedRow::Unattributed;
    };
    let model = model
        .or_else(|| single_model_usage_key(&usage))
        .or_else(|| pid.and_then(|pid| pid_models.get(&pid).cloned()));
    GrokUnifiedRow::Usage(GrokUnifiedUsage {
        session_id,
        pid,
        model,
        ts_ms: row_timestamp(value, ctx),
        usage,
        event_id: row_string(value, ctx, &["event_id", "eventId"]),
    })
}

fn row_string(value: &Value, ctx: &Value, keys: &[&str]) -> Option<String> {
    string_field(value, keys).or_else(|| string_field(ctx, keys))
}

fn row_timestamp(value: &Value, ctx: &Value) -> Option<i64> {
    ["ts", "timestamp"]
        .iter()
        .find_map(|key| value.get(*key).or_else(|| ctx.get(*key)))
        .and_then(timestamp_value_ms)
}

/// A pid that named a model and carried no counters. `model_elapsed_ms` is
/// not in that key list, so a duration cannot set the process model.
fn model_change_or_other(pid: Option<i64>, model: Option<String>) -> GrokUnifiedRow {
    match (pid, model) {
        (Some(pid), Some(model)) => GrokUnifiedRow::ModelChange { pid, model },
        _ => GrokUnifiedRow::Other,
    }
}

/// The counters worth storing, from one source only: a `usage` object, else
/// the top-level counter keys, else `ctx`'s snake_case counters projected
/// onto the canonical names. Two of those sources are never merged.
fn row_usage(value: &Value, ctx: &Value) -> Option<Value> {
    if let Some(usage) = usage_breakdown(value.get("usage")) {
        return Some(usage.clone());
    }
    if let Some(row) = usage_breakdown(Some(value)).and_then(|row| row.as_object()) {
        let projected = project_kept_keys(row);
        if projected
            .as_object()
            .is_some_and(|object| !object.is_empty())
        {
            return Some(projected);
        }
    }
    project_ctx_counters(ctx)
}

fn project_kept_keys(object: &Map<String, Value>) -> Value {
    let keep: Vec<&str> = [
        GROK_USAGE_INPUT_KEYS,
        GROK_USAGE_OUTPUT_KEYS,
        GROK_USAGE_CACHE_READ_KEYS,
        GROK_USAGE_CACHE_WRITE_KEYS,
        GROK_USAGE_REASONING_KEYS,
        UNIFIED_PROJECTED_KEYS,
    ]
    .concat();
    Value::Object(
        object
            .iter()
            .filter(|(key, _)| keep.contains(&key.as_str()))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    )
}

fn project_ctx_counters(ctx: &Value) -> Option<Value> {
    let object = ctx.as_object()?;
    let mut projected = Map::new();
    for (from, to) in CTX_COUNTERS {
        if let Some(value) = object.get(*from).filter(|value| !value.is_null()) {
            projected.insert((*to).to_string(), value.clone());
        }
    }
    (!projected.is_empty()).then_some(Value::Object(projected))
}

/// The identity a `logs/unified.jsonl` row is stored under: a digest of the
/// whole row, normalized through `serde_json` so whitespace does not matter.
///
/// tokscale keys these rows on `event_id`/`eventId`/`id` first — but it also
/// records that Grok reuses `eventId` across usage records, and a key that is
/// not unique collapses distinct inferences into one. The whole row is what
/// is actually unique: a row read twice (a rotation, a second pass over the
/// same bytes) is the same row and lands on the same key, and two inferences
/// that share an id but not their counters or time do not.
pub(crate) fn unified_row_key(value: &Value) -> String {
    let normalized = serde_json::to_string(value).unwrap_or_default();
    let digest = Sha256::digest(normalized.as_bytes());
    format!("{:.32}", format!("{digest:x}"))
}
