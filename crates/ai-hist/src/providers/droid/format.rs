use super::*;
use std::collections::HashSet;
pub(super) fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value[field]
        .as_str()
        .filter(|s| !s.is_empty() && !s.chars().any(char::is_control))
        .with_context(|| format!("DROID_INVALID_RECORD: missing/invalid {field}"))
}
pub(super) fn time(value: &Value) -> Result<Option<i64>> {
    value
        .get("timestamp")
        .map(|_| {
            chrono::DateTime::parse_from_rfc3339(string(value, "timestamp")?)
                .map(|t| t.timestamp_millis())
                .context("DROID_INVALID_RECORD: timestamp")
        })
        .transpose()
}
pub(super) fn parse(bytes: &[u8], shallow: bool) -> Result<Vec<Value>> {
    let end = bytes
        .iter()
        .rposition(|b| *b == b'\n')
        .context("DROID_INCOMPLETE_HEADER: no terminated header within read limit")?;
    let mut entries = Vec::new();
    let mut ids = HashSet::new();
    for line in bytes[..end]
        .split(|b| *b == b'\n')
        .filter(|s| !s.is_empty())
    {
        crate::ingest::check_capture_cancelled()?;
        let entry: Value = serde_json::from_slice(line)
            .context("DROID_INVALID_RECORD: malformed complete JSONL record")?;
        if entries.is_empty() {
            anyhow::ensure!(
                entry["type"] == "session_start" && entry["version"] == 2,
                "DROID_UNSUPPORTED_SCHEMA: expected Droid session version 2"
            );
            string(&entry, "id")?;
        } else {
            let kind = string(&entry, "type")?;
            anyhow::ensure!(
                kind != "session_start",
                "DROID_INVALID_RECORD: duplicate header"
            );
            let timestamp = time(&entry)?;
            if kind == "message" {
                let id = string(&entry, "id")?;
                anyhow::ensure!(
                    ids.insert(id.to_owned()),
                    "DROID_INVALID_RECORD: duplicate message id"
                );
                anyhow::ensure!(
                    timestamp.is_some(),
                    "DROID_INVALID_RECORD: message timestamp missing"
                );
                string(&entry["message"], "role")?;
                anyhow::ensure!(
                    entry["message"]["content"].is_array(),
                    "DROID_INVALID_RECORD: content must be an array"
                );
            }
        }
        entries.push(entry);
        if shallow && entries.len() >= 128 {
            break;
        }
    }
    anyhow::ensure!(!entries.is_empty(), "DROID_INCOMPLETE_HEADER");
    Ok(entries)
}
pub(super) fn last_activity(bytes: &[u8]) -> Result<(Option<i64>, usize)> {
    let end = bytes.iter().rposition(|b| *b == b'\n').unwrap_or(0);
    let mut latest = None;
    let mut records = 0;
    for line in bytes[..end]
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
    {
        crate::ingest::check_capture_cancelled()?;
        let row: Value = serde_json::from_slice(line)
            .context("DROID_INVALID_RECORD: malformed complete tail record")?;
        anyhow::ensure!(
            row.is_object(),
            "DROID_INVALID_RECORD: tail record must be an object"
        );
        latest = latest.max(time(&row)?);
        records += 1;
    }
    Ok((latest, records))
}
pub(super) fn text(content: &Value) -> String {
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
pub(super) fn first_prompt(entries: &[Value]) -> Option<String> {
    entries
        .iter()
        .filter(|e| {
            e["type"] == "message" && e["message"]["role"] == "user" && !is_control(&e["message"])
        })
        .map(|e| text(&e["message"]["content"]))
        .find(|v| !v.is_empty())
        .map(|v| v.chars().take(4096).collect())
}
pub(super) fn is_control(message: &Value) -> bool {
    message["role"] == "user"
        && (matches!(
            message["visibility"].as_str(),
            Some("llm_only" | "user_only")
        ) || message["hookEventName"]
            .as_str()
            .is_some_and(|event| !event.is_empty()))
}
