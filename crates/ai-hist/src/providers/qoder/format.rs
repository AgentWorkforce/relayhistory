use super::*;
use std::collections::HashSet;

pub(super) fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value[field]
        .as_str()
        .filter(|s| !s.is_empty() && !s.chars().any(char::is_control))
        .with_context(|| format!("QODER_INVALID_RECORD: missing/invalid {field}"))
}

pub(super) fn time(value: &Value) -> Result<Option<i64>> {
    let Some(timestamp) = value["timestamp"].as_str() else {
        return Ok(None);
    };
    Ok(Some(
        chrono::DateTime::parse_from_rfc3339(timestamp)
            .context("QODER_INVALID_RECORD: timestamp")?
            .timestamp_millis(),
    ))
}

pub(super) fn parse(bytes: &[u8], shallow: bool) -> Result<Vec<Value>> {
    let end = bytes
        .iter()
        .rposition(|b| *b == b'\n')
        .context("QODER_INCOMPLETE_HEADER: no terminated record within read limit")?;
    let mut entries = Vec::new();
    let mut ids = HashSet::new();
    let mut sid = None;
    for line in bytes[..end]
        .split(|b| *b == b'\n')
        .filter(|s| !s.is_empty())
    {
        let entry: Value = serde_json::from_slice(line)
            .context("QODER_INVALID_RECORD: malformed complete JSONL record")?;
        let kind = string(&entry, "type")?;
        let entry_sid = string(&entry, "sessionId")?;
        if let Some(sid) = sid.as_deref() {
            anyhow::ensure!(sid == entry_sid, "QODER_INVALID_RECORD: mixed session IDs");
        } else {
            sid = Some(entry_sid.to_owned());
            anyhow::ensure!(
                matches!(kind, "user" | "assistant"),
                "QODER_UNSUPPORTED_SCHEMA: expected global CLI opening message"
            );
            anyhow::ensure!(
                entry["entrypoint"] == "cli"
                    && entry["version"]
                        .as_str()
                        .is_some_and(|v| v.starts_with("1.1.")),
                "QODER_UNSUPPORTED_SCHEMA: expected global CLI 1.1.x transcript"
            );
        }
        if matches!(kind, "user" | "assistant") {
            let id = string(&entry, "uuid")?;
            anyhow::ensure!(
                ids.insert(id.to_owned()),
                "QODER_INVALID_RECORD: duplicate message UUID"
            );
            anyhow::ensure!(
                entry["message"]["role"] == kind,
                "QODER_INVALID_RECORD: message role mismatch"
            );
            anyhow::ensure!(
                entry["message"]["content"].is_string() || entry["message"]["content"].is_array(),
                "QODER_INVALID_RECORD: message content"
            );
            anyhow::ensure!(
                entry["parentUuid"].is_null() || entry["parentUuid"].is_string(),
                "QODER_INVALID_RECORD: parentUuid"
            );
            anyhow::ensure!(
                time(&entry)?.is_some(),
                "QODER_INVALID_RECORD: missing message timestamp"
            );
        }
        entries.push(entry);
        if shallow && entries.len() >= 128 {
            break;
        }
    }
    anyhow::ensure!(!entries.is_empty(), "QODER_INCOMPLETE_HEADER");
    Ok(entries)
}

pub(super) fn text(content: &Value) -> String {
    if let Some(text) = content.as_str() {
        return text.into();
    }
    content
        .as_array()
        .map(|blocks| {
            blocks
                .iter()
                .filter(|b| b["type"] == "text")
                .filter_map(|b| b["text"].as_str())
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default()
}

pub(super) fn first_prompt(entries: &[Value]) -> Option<String> {
    entries
        .iter()
        .filter(|e| e["type"] == "user")
        .filter(|e| {
            !e["message"]["content"]
                .as_array()
                .is_some_and(|a| a.iter().any(|b| b["type"] == "tool_result"))
        })
        .map(|e| text(&e["message"]["content"]))
        .find(|value| !value.is_empty())
        .map(|value| value.chars().take(4096).collect())
}
