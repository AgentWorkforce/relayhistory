use super::*;
use std::collections::{BTreeMap, BTreeSet};
pub(super) fn string<'a>(row: &'a Value, key: &str) -> Result<&'a str> {
    row[key]
        .as_str()
        .filter(|s| !s.is_empty())
        .with_context(|| format!("QWEN_CODE_INVALID_RECORD: {key} missing or invalid"))
}
pub(super) fn time(row: &Value) -> Result<i64> {
    Ok(
        chrono::DateTime::parse_from_rfc3339(string(row, "timestamp")?)
            .context("QWEN_CODE_INVALID_RECORD: invalid timestamp")?
            .timestamp_millis(),
    )
}
pub(super) fn parse(bytes: &[u8], bounded: bool) -> Result<(Vec<Value>, bool)> {
    let mut rows = Vec::new();
    let mut partial = false;
    for line in bytes.split_inclusive(|b| *b == b'\n') {
        crate::ingest::check_capture_cancelled()?;
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        if bounded && !line.ends_with(b"\n") {
            break;
        }
        match serde_json::from_slice::<Value>(line) {
            Ok(row) => {
                anyhow::ensure!(!row["subtype"].as_str().is_some_and(|s|s.starts_with("managed_session_")), "QWEN_CODE_UNSUPPORTED_MANAGED: transactional managed-session journals require their native projector");
                for key in ["uuid", "sessionId", "type", "cwd", "version"] {
                    string(&row, key)?;
                }
                anyhow::ensure!(
                    row.get("parentUuid")
                        .is_some_and(|v| v.is_null() || v.is_string()),
                    "QWEN_CODE_INVALID_RECORD: invalid parentUuid"
                );
                anyhow::ensure!(
                    matches!(
                        row["type"].as_str(),
                        Some("user" | "assistant" | "tool_result" | "system")
                    ),
                    "QWEN_CODE_INVALID_RECORD: unknown record type"
                );
                time(&row)?;
                anyhow::ensure!(!matches!(row["type"].as_str(),Some("user"|"assistant")) || row["provenance"].is_string(), "QWEN_CODE_UNSUPPORTED_PROVENANCE: user/assistant record lacks native provenance");
                if let Some(message) = row.get("message") {
                    anyhow::ensure!(
                        message["parts"].is_array(),
                        "QWEN_CODE_INVALID_RECORD: message.parts must be array"
                    );
                }
                rows.push(row);
            }
            Err(e) if e.is_eof() && !line.ends_with(b"\n") => partial = true,
            Err(e) => return Err(e).context("QWEN_CODE_INVALID_RECORD: malformed JSONL"),
        }
    }
    Ok((rows, partial))
}
// Mirror the pinned independent reader: aggregate repeated UUID fragments,
// choose the last conversation record, then walk its parent chain root-first.
pub(super) fn project(rows: &[Value]) -> Result<Vec<Value>> {
    let mut by_id = BTreeMap::<String, Value>::new();
    let mut leaf = None;
    let mut session = None;
    for row in rows {
        let sid = string(row, "sessionId")?;
        anyhow::ensure!(
            session.is_none_or(|s| s == sid),
            "QWEN_CODE_INVALID_RECORD: mixed session IDs"
        );
        session = Some(sid);
        if row["type"] == "system"
            && matches!(
                row["subtype"].as_str(),
                Some(
                    "session_artifact_event"
                        | "session_artifact_snapshot"
                        | "session_sources_snapshot"
                )
            )
        {
            continue;
        }
        let id = string(row, "uuid")?.to_owned();
        leaf = Some(id.clone());
        if let Some(old) = by_id.get_mut(&id) {
            merge_fragment(old, row)?;
        } else {
            by_id.insert(id, row.clone());
        }
    }
    let mut out = Vec::new();
    let mut seen = BTreeSet::new();
    while let Some(id) = leaf {
        anyhow::ensure!(
            seen.insert(id.clone()),
            "QWEN_CODE_INVALID_RECORD: parent cycle"
        );
        let row = by_id
            .get(&id)
            .context("QWEN_CODE_INCOMPLETE_CHAIN: parent record missing")?;
        out.push(row.clone());
        leaf = row["parentUuid"].as_str().map(str::to_owned);
    }
    out.reverse();
    Ok(out)
}
pub(super) fn role(row: &Value) -> Option<&'static str> {
    match (row["type"].as_str()?, row["provenance"].as_str()) {
        ("user", Some("real_user")) if row.get("subtype").is_none() => Some("user"),
        ("assistant", Some("assistant_output")) => Some("assistant"),
        _ => None,
    }
}
pub(super) fn text(row: &Value) -> String {
    let parts = row["message"]["parts"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default();
    if role(row) == Some("user") {
        let payload = row.get("systemPayload");
        let final_hook = parts.len() > 1
            && parts
                .last()
                .and_then(|p| p["text"].as_str())
                .is_some_and(hook_context);
        if let Some(text) = payload
            .filter(|p| p["hookContext"].is_string() || final_hook)
            .and_then(|p| p["displayText"].as_str())
        {
            return text.to_owned();
        }
        if payload.is_none() && final_hook {
            return visible_text(&parts[..parts.len() - 1]);
        }
    }
    visible_text(parts)
}
fn visible_text(parts: &[Value]) -> String {
    parts
        .iter()
        .filter(|p| p["thought"] != true)
        .filter_map(|p| p["text"].as_str())
        .collect::<Vec<_>>()
        .join("\n")
}
fn hook_context(text: &str) -> bool {
    const OPEN: &str = "<qwen:user-prompt-submit-context>";
    const CLOSE: &str = "</qwen:user-prompt-submit-context>";
    text.trim()
        .strip_prefix(&format!("{OPEN}\n"))
        .and_then(|s| s.strip_suffix(&format!("\n{CLOSE}")))
        .is_some_and(|body| !body.contains(OPEN) && !body.contains(CLOSE))
}

fn merge_fragment(old: &mut Value, row: &Value) -> Result<()> {
    anyhow::ensure!(
        old["parentUuid"] == row["parentUuid"] && old["type"] == row["type"],
        "QWEN_CODE_INVALID_RECORD: conflicting UUID fragments"
    );
    if let Some(parts) = row["message"]["parts"].as_array() {
        if old["message"]["parts"].is_null() {
            old["message"] = row["message"].clone();
        } else {
            old["message"]["parts"]
                .as_array_mut()
                .context("invalid fragment parts")?
                .extend(parts.iter().cloned());
        }
    }
    if time(row)? > time(old)? {
        old["timestamp"] = row["timestamp"].clone();
    }
    if let Some(v) = row.get("usageMetadata") {
        old["usageMetadata"] = v.clone();
    }
    for key in ["toolCallResult", "model"] {
        if let Some(v) = row.get(key).filter(|_| old.get(key).is_none()) {
            old[key] = v.clone();
        }
    }
    Ok(())
}
