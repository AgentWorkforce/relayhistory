//! OpenClaw entry fields: validation, text, controls and usage rules shared
//! by discovery and normalization.
use super::*;
use anyhow::{ensure, Context};

// `src/auto-reply/heartbeat.ts` and `heartbeat-filter.ts` at the reviewed
// upstream revision: the transcript markers internal wakes persist instead
// of their prompt, the current and legacy default prompts, and the periodic
// task prompt.
const WAKE_MARKERS: &[&str] = &[
    "[OpenClaw heartbeat poll]",
    "[OpenClaw cron wake]",
    "[OpenClaw session event]",
    "[OpenClaw exec completion]",
];
const HEARTBEAT_PROMPTS: &[&str] = &[
    "Follow the heartbeat monitor scratch context when provided. Recurring tasks are automations; create or change their schedules with the automations tool, not heartbeat scratch. Do not infer or repeat old tasks from prior chats. If nothing needs attention, reply NO_REPLY.",
    "Read HEARTBEAT.md if it exists (workspace context). Follow it strictly. Do not infer or repeat old tasks from prior chats. If nothing needs attention, reply HEARTBEAT_OK.",
];
const TASK_PROMPT: &str =
    "Run the following periodic tasks (only those due based on their intervals):";
const TASK_COMPLETIONS: &[&str] = &[
    "After completing all due tasks, reply HEARTBEAT_OK.",
    "After completing all due tasks, reply NO_REPLY.",
    "Use heartbeat_respond to report the wake outcome.",
];
/// Upstream's `isHeartbeatUserMessage` for an unconfigured prompt: an internal
/// wake marker, a default heartbeat prompt (alone or followed by a newline),
/// or the periodic-task prompt.
pub(super) fn is_heartbeat_prompt(text: &str) -> bool {
    let text = text.trim();
    WAKE_MARKERS.contains(&text)
        || text.starts_with("[OpenClaw exec completion]\n")
        || HEARTBEAT_PROMPTS.iter().any(|p| {
            text == *p
                || text
                    .strip_prefix(p)
                    .is_some_and(|rest| rest.starts_with('\n'))
        })
        || (text.starts_with(TASK_PROMPT) && TASK_COMPLETIONS.iter().any(|c| text.contains(c)))
}
/// A quiet heartbeat reply (`stripHeartbeatToken` in heartbeat mode): the
/// silent token alone, or the heartbeat token at either edge of at most 300
/// further characters.
pub(super) fn is_heartbeat_ack(text: &str) -> bool {
    let trimmed = text
        .trim()
        .trim_matches(|c| matches!(c, '*' | '`' | '~' | '_'));
    if trimmed == "NO_REPLY" {
        return true;
    }
    let rest = trimmed
        .strip_prefix("HEARTBEAT_OK")
        .or_else(|| trimmed.strip_suffix("HEARTBEAT_OK"));
    rest.is_some_and(|rest| rest.trim().chars().count() <= 300)
}
/// Text blocks concatenated as upstream's heartbeat filter reads them.
fn joined_text(content: &Value) -> String {
    match content {
        Value::String(text) => text.clone(),
        Value::Array(blocks) => blocks
            .iter()
            .filter(|b| b["type"] == "text")
            .filter_map(|b| b["text"].as_str())
            .collect(),
        _ => String::new(),
    }
}

/// A user-role record OpenClaw wrote itself rather than a human prompt.
pub(super) fn control_kind(msg: &Value) -> Option<&'static str> {
    // `external_user` provenance is a person on another channel, and Codex
    // mirror fields annotate the admitted human prompt in place: both stay
    // prompts. Inter-session and system input, context-excluded notes and
    // context-free commands are OpenClaw's own records.
    let synthetic = msg["role"] == "user"
        && (is_heartbeat_prompt(&joined_text(&msg["content"]))
            || msg["excludeFromContext"] == true
            || msg["__openclaw"]["contextFreeCommand"] == true
            || matches!(
                msg["provenance"]["kind"].as_str(),
                Some("inter_session" | "internal_system")
            ));
    synthetic.then_some("synthetic")
}

/// Whether this assistant message's usage is a request OpenClaw itself made.
/// Transcript-only copies carry zero or repeated usage, and CLI-harness rows
/// repeat usage the harness's own native log (Claude Code, ...) records.
pub(super) fn counts_usage(msg: &Value) -> bool {
    let mirror_origin = msg["__openclaw"]["mirrorOrigin"].as_str().unwrap_or("");
    !(is_delivery_mirror(msg)
        || msg["provider"] == "openclaw"
        || msg["api"] == "cli"
        || matches!(
            msg["model"].as_str(),
            Some("delivery-mirror" | "gateway-injected" | "native-history")
        )
        || mirror_origin.ends_with("-catalog-import")
        || zero_usage(&msg["usage"]))
}
/// Harness projections stamp commentary and tool-call rows with an all-zero
/// snapshot; that is an unmeasured row, not a measured free request.
/// A malformed counter is not zero: it stays for the usage normalizer to
/// report.
fn zero_usage(usage: &Value) -> bool {
    ["input", "output", "cacheRead", "cacheWrite", "totalTokens"]
        .iter()
        .all(|key| usage.get(key).is_none_or(|n| n.as_u64() == Some(0)))
}

/// Assistant text OpenClaw copies into the transcript after channel delivery.
pub(super) fn is_delivery_mirror(msg: &Value) -> bool {
    msg["openclawDeliveryMirror"].is_object()
        || (msg["provider"] == "openclaw"
            && matches!(
                msg["model"].as_str(),
                Some("delivery-mirror" | "gateway-injected")
            ))
}

pub(super) fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value[field]
        .as_str()
        .filter(|s| !s.is_empty() && !s.chars().any(char::is_control))
        .with_context(|| format!("OPENCLAW_INVALID_RECORD: missing/invalid {field}"))
}

pub(super) fn entry_time(entry: &Value) -> Result<i64> {
    let text = string(entry, "timestamp")?;
    let ms = chrono::DateTime::parse_from_rfc3339(text)
        .context("OPENCLAW_INVALID_RECORD: entry timestamp")?
        .timestamp_millis();
    ensure!(
        ms >= 0,
        "OPENCLAW_INVALID_RECORD: entry timestamp before epoch"
    );
    Ok(ms)
}

pub(super) fn text(content: &Value) -> String {
    if let Some(text) = content.as_str() {
        return text.into();
    }
    content
        .as_array()
        .map(|a| {
            a.iter()
                .filter(|b| b["type"] == "text")
                .filter_map(|b| b["text"].as_str())
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default()
}

/// The first human prompt, using the same controls as full normalization.
pub(super) fn first_prompt<'a>(entries: impl IntoIterator<Item = &'a Value>) -> Option<String> {
    entries
        .into_iter()
        .filter(|e| e["type"] == "message" && e["message"]["role"] == "user")
        .filter(|e| control_kind(&e["message"]).is_none())
        .map(|e| text(&e["message"]["content"]))
        .find(|value| !value.is_empty())
        .map(|value| value.chars().take(4096).collect())
}

/// Validate one captured entry before any of the session is written.
pub(super) fn validate(entry: &Value, first: bool) -> Result<()> {
    if first {
        ensure!(
            entry["type"] == "session",
            "OPENCLAW_INVALID_RECORD: first transcript event must be the session header"
        );
        let version = entry["version"].as_i64().unwrap_or(0);
        ensure!(
            (3..=4).contains(&version),
            "OPENCLAW_UNSUPPORTED_SCHEMA: transcript header version {version}; supported: 3-4"
        );
        string(entry, "id")?;
        entry_time(entry)?;
        return Ok(());
    }
    let kind = string(entry, "type")?;
    ensure!(
        kind != "session",
        "OPENCLAW_INVALID_RECORD: duplicate session header"
    );
    string(entry, "id")?;
    ensure!(
        entry
            .get("parentId")
            .is_none_or(|p| p.is_null() || p.as_str().is_some()),
        "OPENCLAW_INVALID_RECORD: parentId"
    );
    entry_time(entry)?;
    if kind == "message" {
        string(&entry["message"], "role")?;
        ensure!(
            entry["message"]["content"].is_string() || entry["message"]["content"].is_array(),
            "OPENCLAW_INVALID_RECORD: message content"
        );
    }
    Ok(())
}
