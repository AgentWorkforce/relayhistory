use super::*;
use std::collections::HashSet;
pub(super) fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value[field]
        .as_str()
        .filter(|s| !s.is_empty() && !s.chars().any(char::is_control))
        .with_context(|| format!("PI_INVALID_RECORD: missing/invalid {field}"))
}
pub(super) fn time(value: &Value) -> Result<i64> {
    Ok(
        chrono::DateTime::parse_from_rfc3339(string(value, "timestamp")?)
            .context("PI_INVALID_RECORD: timestamp")?
            .timestamp_millis(),
    )
}
pub(super) fn parse(bytes: &[u8], shallow: bool) -> Result<Vec<Value>> {
    let end = bytes
        .iter()
        .rposition(|b| *b == b'\n')
        .context("PI_INCOMPLETE_HEADER: no terminated header within read limit")?;
    let mut entries = Vec::new();
    let mut ids = HashSet::new();
    for line in bytes[..end]
        .split(|b| *b == b'\n')
        .filter(|s| !s.is_empty())
    {
        let entry: Value = serde_json::from_slice(line)
            .context("PI_INVALID_RECORD: malformed complete JSONL record")?;
        if entries.is_empty() {
            anyhow::ensure!(
                entry["type"] == "session" && entry["version"] == 3,
                "PI_UNSUPPORTED_SCHEMA: expected Pi session version 3"
            );
            string(&entry, "id")?;
            string(&entry, "cwd")?;
            time(&entry)?;
        } else {
            validate_entry(&entry, &mut ids)?;
        }
        entries.push(entry);
        if shallow && entries.len() >= 128 {
            break;
        }
    }
    anyhow::ensure!(!entries.is_empty(), "PI_INCOMPLETE_HEADER");
    Ok(entries)
}
fn validate_entry(entry: &Value, ids: &mut HashSet<String>) -> Result<()> {
    let kind = string(entry, "type")?;
    anyhow::ensure!(kind != "session", "PI_INVALID_RECORD: duplicate header");
    let id = string(entry, "id")?;
    anyhow::ensure!(
        ids.insert(id.into()),
        "PI_INVALID_RECORD: duplicate entry id"
    );
    anyhow::ensure!(
        entry
            .get("parentId")
            .is_some_and(|p| p.is_null() || p.as_str().is_some()),
        "PI_INVALID_RECORD: parentId"
    );
    time(entry)?;
    if kind == "message" {
        string(&entry["message"], "role")?;
        anyhow::ensure!(
            entry["message"]["content"].is_string() || entry["message"]["content"].is_array(),
            "PI_INVALID_RECORD: message content"
        );
    }
    Ok(())
}
pub(super) fn text(content: &Value) -> String {
    if let Some(text) = content.as_str() {
        return text.into();
    }
    content
        .as_array()
        .map(|a| {
            a.iter()
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
        .find(|value| !value.is_empty())
        .map(|value| value.chars().take(4096).collect())
}
