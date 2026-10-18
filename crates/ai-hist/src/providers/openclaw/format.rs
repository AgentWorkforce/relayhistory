//! OpenClaw entry fields: validation, text, controls and usage rules shared
//! by discovery and normalization.
use super::*;
use anyhow::{ensure, Context};

/// A user-role record OpenClaw wrote itself rather than a human prompt.
pub(super) fn control_kind(msg: &Value) -> Option<&'static str> {
    // `external_user` provenance is a person on another channel, and Codex
    // mirror fields annotate the admitted human prompt in place: both stay
    // prompts. Inter-session and system input, context-excluded notes and
    // context-free commands are OpenClaw's own records.
    let synthetic = msg["role"] == "user"
        && (msg["excludeFromContext"] == true
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
