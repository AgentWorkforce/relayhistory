use std::collections::HashMap;

use serde_json::Value;
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

/// Interpret one `logs/unified.jsonl` row.
///
/// The row shape is **inferred**, not observed: tokscale reads a session id, a
/// pid, a model and per-inference input/output/cache counters from it, but no
/// public sample shows the field spellings. So every field is looked for in
/// the spellings Grok uses elsewhere, at the top level and then under `ctx`
/// (where tokscale reads `ctx.event_id`), and the counters are the same key
/// lists `turn_completed.usage` is read with.
pub(crate) fn parse_unified_row(
    value: &Value,
    pid_models: &HashMap<i64, String>,
) -> GrokUnifiedRow {
    let ctx = value.get("ctx").unwrap_or(&Value::Null);
    let field = |keys: &[&str]| string_field(value, keys).or_else(|| string_field(ctx, keys));
    let pid = first_number(&[value.get("pid"), ctx.get("pid")]);
    let model = field(&["model_id", "modelId", "model"]);
    let usage = match usage_breakdown(value.get("usage")) {
        Some(usage) => Some(usage.clone()),
        None => usage_breakdown(Some(value)).map(|row| {
            let object = row.as_object().cloned().unwrap_or_default();
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
                    .into_iter()
                    .filter(|(key, _)| keep.contains(&key.as_str()))
                    .collect(),
            )
        }),
    };
    let Some(usage) = usage else {
        return match (pid, model) {
            (Some(pid), Some(model)) => GrokUnifiedRow::ModelChange { pid, model },
            _ => GrokUnifiedRow::Other,
        };
    };
    let Some(session_id) = field(&["session_id", "sessionId"]) else {
        return GrokUnifiedRow::Unattributed;
    };
    let model = model
        .or_else(|| single_model_usage_key(&usage))
        .or_else(|| pid.and_then(|pid| pid_models.get(&pid).cloned()));
    let ts_ms = ["ts", "timestamp"]
        .iter()
        .find_map(|key| value.get(*key).or_else(|| ctx.get(*key)))
        .and_then(timestamp_value_ms);
    GrokUnifiedRow::Usage(GrokUnifiedUsage {
        session_id,
        pid,
        model,
        ts_ms,
        usage,
        event_id: field(&["event_id", "eventId"]),
    })
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
