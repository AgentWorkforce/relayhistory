//! The Copilot CLI session journal: `session-state/<id>/events.jsonl`, one
//! event envelope per line (`type`, `data`, `id`, `timestamp`, `parentId`),
//! opened by a `session.start` header at journal version 1.
use super::*;
use std::collections::HashSet;

pub(super) fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value[field]
        .as_str()
        .filter(|s| !s.is_empty() && !s.chars().any(char::is_control))
        .with_context(|| format!("COPILOT_INVALID_RECORD: missing/invalid {field}"))
}

pub(super) fn time(value: &Value) -> Result<i64> {
    Ok(
        chrono::DateTime::parse_from_rfc3339(string(value, "timestamp")?)
            .context("COPILOT_INVALID_RECORD: timestamp")?
            .timestamp_millis(),
    )
}

/// The native session ID the `session.start` header declares.
pub(super) fn session_id(header: &Value) -> Result<&str> {
    string(&header["data"], "sessionId")
}

/// Every complete record in `bytes`. A trailing fragment without its newline
/// is left for the caller; `shallow` stops after a bounded prefix.
pub(super) fn parse(bytes: &[u8], shallow: bool) -> Result<Vec<Value>> {
    let end = bytes
        .iter()
        .rposition(|b| *b == b'\n')
        .context("COPILOT_INCOMPLETE_HEADER: no terminated header within read limit")?;
    let mut entries = Vec::new();
    let mut ids = HashSet::new();
    for line in bytes[..end]
        .split(|b| *b == b'\n')
        .filter(|s| !s.is_empty())
    {
        let entry: Value = serde_json::from_slice(line)
            .context("COPILOT_INVALID_RECORD: malformed complete JSONL record")?;
        if entries.is_empty() {
            validate_header(&entry)?;
        } else {
            anyhow::ensure!(
                entry["type"] != "session.start",
                "COPILOT_INVALID_RECORD: duplicate session.start"
            );
        }
        validate_envelope(&entry, &mut ids)?;
        validate_data(&entry)?;
        entries.push(entry);
        if shallow && entries.len() >= 128 {
            break;
        }
    }
    anyhow::ensure!(!entries.is_empty(), "COPILOT_INCOMPLETE_HEADER");
    Ok(entries)
}

fn validate_header(entry: &Value) -> Result<()> {
    anyhow::ensure!(
        entry["type"] == "session.start" && entry["data"]["version"] == 1,
        "COPILOT_UNSUPPORTED_SCHEMA: expected a session.start header at journal version 1"
    );
    session_id(entry)?;
    Ok(())
}

fn validate_envelope(entry: &Value, ids: &mut HashSet<String>) -> Result<()> {
    string(entry, "type")?;
    let id = string(entry, "id")?;
    anyhow::ensure!(
        ids.insert(id.into()),
        "COPILOT_INVALID_RECORD: duplicate event id"
    );
    anyhow::ensure!(
        entry
            .get("parentId")
            .is_some_and(|p| p.is_null() || p.as_str().is_some()),
        "COPILOT_INVALID_RECORD: parentId"
    );
    anyhow::ensure!(
        entry["data"].is_object(),
        "COPILOT_INVALID_RECORD: data must be an object"
    );
    time(entry)?;
    Ok(())
}

/// The fields normalization reads as identities or content.
fn validate_data(entry: &Value) -> Result<()> {
    let data = &entry["data"];
    match entry["type"].as_str() {
        Some("user.message") => anyhow::ensure!(
            data["content"].is_string(),
            "COPILOT_INVALID_RECORD: user.message content"
        ),
        Some("assistant.message") => {
            anyhow::ensure!(
                data["content"].is_string() || data["content"].is_null(),
                "COPILOT_INVALID_RECORD: assistant.message content"
            );
            for request in tool_requests(data)? {
                string(request, "toolCallId")?;
                string(request, "name")?;
            }
        }
        Some("tool.execution_start") => {
            string(data, "toolCallId")?;
            string(data, "toolName")?;
        }
        Some("tool.execution_complete") => {
            string(data, "toolCallId")?;
            anyhow::ensure!(
                data["success"].is_boolean(),
                "COPILOT_INVALID_RECORD: tool.execution_complete success"
            );
        }
        _ => {}
    }
    Ok(())
}

pub(super) fn tool_requests(data: &Value) -> Result<&[Value]> {
    match &data["toolRequests"] {
        Value::Null => Ok(&[]),
        Value::Array(requests) => Ok(requests),
        _ => anyhow::bail!("COPILOT_INVALID_RECORD: toolRequests must be an array"),
    }
}

/// A tool call's identity within the session. Subagents number their calls
/// independently of the main agent (`bash_14` can name one call of each), so
/// a call made under a delegating call is scoped by that call's ID.
pub(super) fn call_id(data: &Value, raw: &str) -> String {
    match data["parentToolCallId"].as_str() {
        Some(parent) => format!("{parent}/{raw}"),
        None => raw.to_owned(),
    }
}

/// A user message a person typed, as opposed to one another agent delivered.
pub(super) fn is_human_prompt(entry: &Value) -> bool {
    entry["type"] == "user.message" && entry["data"]["source"].is_null()
}

pub(super) fn first_prompt(entries: &[Value]) -> Option<String> {
    entries
        .iter()
        .filter(|e| is_human_prompt(e))
        .filter_map(|e| e["data"]["content"].as_str())
        .find(|value| !value.is_empty())
        .map(|value| value.chars().take(4096).collect())
}

/// Every model the records name, in first-seen order: the header's
/// selection, model changes and the models assistant messages report.
pub(super) fn models(entries: &[Value]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for entry in entries {
        let data = &entry["data"];
        let named = match entry["type"].as_str() {
            Some("session.start") | Some("session.resume") => data["selectedModel"].as_str(),
            Some("session.model_change") => data["newModel"].as_str(),
            Some("assistant.message") => data["model"].as_str(),
            _ => None,
        };
        if let Some(model) = named.filter(|m| !m.is_empty()) {
            if !out.iter().any(|seen| seen == model) {
                out.push(model.to_owned());
            }
        }
    }
    out
}

pub(super) fn title(entries: &[Value]) -> Option<&str> {
    entries
        .iter()
        .rev()
        .find(|e| e["type"] == "session.title_changed")
        .and_then(|e| e["data"]["title"].as_str())
}

/// The timestamp of the last complete record in a journal's tail window.
pub(super) fn last_time(tail: &[u8]) -> Option<i64> {
    let end = tail.iter().rposition(|b| *b == b'\n')?;
    tail[..end]
        .split(|b| *b == b'\n')
        .rev()
        .filter(|s| !s.is_empty())
        .find_map(|line| {
            serde_json::from_slice::<Value>(line)
                .ok()
                .and_then(|v| time(&v).ok())
        })
}
