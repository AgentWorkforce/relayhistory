use super::*;
use std::collections::HashSet;
pub(super) fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value[field]
        .as_str()
        .filter(|s| !s.is_empty() && !s.chars().any(char::is_control))
        .with_context(|| format!("OMP_INVALID_RECORD: missing/invalid {field}"))
}
pub(super) fn time(value: &Value) -> Result<i64> {
    Ok(
        chrono::DateTime::parse_from_rfc3339(string(value, "timestamp")?)
            .context("OMP_INVALID_RECORD: timestamp")?
            .timestamp_millis(),
    )
}
pub(super) fn parse(bytes: &[u8], shallow: bool) -> Result<Vec<Value>> {
    let end = bytes
        .iter()
        .rposition(|b| *b == b'\n')
        .context("OMP_INCOMPLETE_HEADER: no terminated header within read limit")?;
    let first_end = bytes
        .iter()
        .position(|b| *b == b'\n')
        .context("OMP_INCOMPLETE_HEADER")?;
    let slot: Value =
        serde_json::from_slice(&bytes[..first_end]).context("OMP_INVALID_RECORD: first line")?;
    if slot["type"] == "session" && slot["version"] == 3 {
        string(&slot, "id")?;
        string(&slot, "cwd")?;
        time(&slot)?;
        anyhow::bail!("OMP_AMBIGUOUS_LEGACY: title slot required to distinguish OMP from Pi");
    }
    anyhow::ensure!(
        first_end == 255,
        "OMP_INVALID_TITLE_SLOT: expected 256-byte title slot"
    );
    anyhow::ensure!(
        slot["type"] == "title" && slot["v"] == 1,
        "OMP_INVALID_TITLE_SLOT"
    );
    anyhow::ensure!(end > first_end, "OMP_INCOMPLETE_HEADER");
    anyhow::ensure!(
        slot["title"].is_string() && slot["updatedAt"].is_string() && slot["pad"].is_string(),
        "OMP_INVALID_TITLE_SLOT"
    );
    anyhow::ensure!(
        slot.get("source")
            .is_none_or(|s| s == "auto" || s == "user"),
        "OMP_INVALID_TITLE_SLOT: source"
    );
    let mut entries = Vec::new();
    let mut ids = HashSet::new();
    for line in bytes[first_end + 1..end]
        .split(|b| *b == b'\n')
        .filter(|s| !s.is_empty())
    {
        let mut entry: Value = serde_json::from_slice(line)
            .context("OMP_INVALID_RECORD: malformed complete JSONL record")?;
        if entries.is_empty() {
            anyhow::ensure!(
                entry["type"] == "session" && entry["version"] == 3,
                "OMP_UNSUPPORTED_SCHEMA: expected OMP session version 3"
            );
            overlay_title(&mut entry, &slot);
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
    anyhow::ensure!(!entries.is_empty(), "OMP_INCOMPLETE_HEADER");
    Ok(entries)
}
fn validate_entry(entry: &Value, ids: &mut HashSet<String>) -> Result<()> {
    let kind = string(entry, "type")?;
    anyhow::ensure!(kind != "session", "OMP_INVALID_RECORD: duplicate header");
    let id = string(entry, "id")?;
    anyhow::ensure!(
        ids.insert(id.into()),
        "OMP_INVALID_RECORD: duplicate entry id"
    );
    anyhow::ensure!(
        entry
            .get("parentId")
            .is_some_and(|p| p.is_null() || p.as_str().is_some()),
        "OMP_INVALID_RECORD: parentId"
    );
    time(entry)?;
    if kind == "message" {
        let role = string(&entry["message"], "role")?;
        anyhow::ensure!(
            !["user", "assistant", "toolResult"].contains(&role)
                || entry["message"]["content"].is_string()
                || entry["message"]["content"].is_array(),
            "OMP_INVALID_RECORD: message content"
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

fn overlay_title(header: &mut Value, slot: &Value) {
    let header = header.as_object_mut().expect("validated session header");
    if slot["title"].as_str().is_some_and(|s| !s.is_empty()) {
        header.insert("title".into(), slot["title"].clone());
    } else {
        header.remove("title");
    }
    if let Some(source) = slot.get("source") {
        header.insert("titleSource".into(), source.clone());
    } else {
        header.remove("titleSource");
    }
}
