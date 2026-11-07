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
            time(&entry)?;
            if kind == "message" {
                let id = string(&entry, "id")?;
                anyhow::ensure!(
                    ids.insert(id.to_owned()),
                    "DROID_INVALID_RECORD: duplicate message id"
                );
                anyhow::ensure!(
                    time(&entry)?.is_some(),
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
        .filter(|e| e["type"] == "message" && e["message"]["role"] == "user")
        .map(|e| text(&e["message"]["content"]))
        .find(|v| !v.is_empty())
        .map(|v| v.chars().take(4096).collect())
}
