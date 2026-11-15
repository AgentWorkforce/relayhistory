//! Claude's and Codex's `history.jsonl` lines, one prompt each.
//!
//! A line with no usable time is left out rather than stored at 0, which every
//! reader takes for 1970-01-01. `history.timestamp_ms` is `NOT NULL`, and a
//! history line stands alone, so there is no earlier record of its own to
//! inherit a time from. The same prompt is still indexed, with its time, from
//! the session transcript.

use anyhow::Result;
use serde_json::Value;

use super::{prompt_hash, HistoryEntry};

pub fn parse_claude(line: &str) -> Result<Option<HistoryEntry>> {
    let obj: serde_json::Value = serde_json::from_str(line)?;
    let display = obj
        .get("display")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim();
    if display.is_empty() {
        return Ok(None);
    }
    let Some(timestamp_ms) = claude_ms(&obj) else {
        return Ok(None);
    };
    Ok(Some(HistoryEntry {
        id: 0,
        source: "claude".into(),
        session_id: obj
            .get("sessionId")
            .and_then(|v| v.as_str())
            .map(str::to_string),
        project: obj
            .get("project")
            .and_then(|v| v.as_str())
            .map(str::to_string),
        prompt: display.to_string(),
        prompt_hash: Some(prompt_hash(display)),
        timestamp_ms,
    }))
}

pub fn parse_codex(line: &str) -> Result<Option<HistoryEntry>> {
    let obj: serde_json::Value = serde_json::from_str(line)?;
    let text = obj
        .get("text")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim();
    if text.is_empty() {
        return Ok(None);
    }
    let Some(timestamp_ms) = codex_ms(&obj) else {
        return Ok(None);
    };
    Ok(Some(HistoryEntry {
        id: 0,
        source: "codex".into(),
        session_id: obj
            .get("session_id")
            .or_else(|| obj.get("sessionId"))
            .and_then(|v| v.as_str())
            .map(str::to_string),
        project: None,
        prompt: text.to_string(),
        prompt_hash: Some(prompt_hash(text)),
        timestamp_ms,
    }))
}

/// Claude's `timestamp`, in milliseconds.
fn claude_ms(obj: &Value) -> Option<i64> {
    obj.get("timestamp")
        .and_then(Value::as_i64)
        .filter(|ts| *ts > 0)
}

/// Codex's `ts`, in seconds, as milliseconds.
fn codex_ms(obj: &Value) -> Option<i64> {
    obj.get("ts")
        .and_then(Value::as_f64)
        .map(|ts| (ts * 1000.0) as i64)
        .filter(|ts| *ts > 0)
}
