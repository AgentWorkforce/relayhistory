use super::*;

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
                validate(&row)?;
                rows.push(row);
            }
            Err(error) if error.is_eof() && !line.ends_with(b"\n") => {
                partial = true;
            }
            Err(error) => return Err(error).context("ANTIGRAVITY_INVALID_RECORD: malformed JSONL"),
        }
    }
    let mut seen = std::collections::BTreeSet::new();
    for row in &rows {
        anyhow::ensure!(
            seen.insert(index(row)?),
            "ANTIGRAVITY_INVALID_RECORD: duplicate step_index"
        );
    }
    Ok((rows, partial))
}
fn validate(row: &Value) -> Result<()> {
    index(row)?;
    time(row)?;
    for field in ["source", "type", "status"] {
        string(row, field)?;
    }
    for field in ["content", "thinking"] {
        anyhow::ensure!(
            row.get(field).is_none_or(Value::is_string),
            "ANTIGRAVITY_INVALID_RECORD: {field} must be text"
        );
    }
    for field in ["tool_calls", "media", "truncated_fields"] {
        anyhow::ensure!(
            row.get(field).is_none_or(Value::is_array),
            "ANTIGRAVITY_INVALID_RECORD: {field} must be array"
        );
    }
    anyhow::ensure!(
        row.get("is_truncated").is_none_or(Value::is_boolean),
        "ANTIGRAVITY_INVALID_RECORD: is_truncated must be boolean"
    );
    Ok(())
}
pub(super) fn string<'a>(row: &'a Value, field: &str) -> Result<&'a str> {
    row[field]
        .as_str()
        .filter(|s| !s.is_empty())
        .with_context(|| format!("ANTIGRAVITY_INVALID_RECORD: {field} missing or invalid"))
}
pub(super) fn index(row: &Value) -> Result<u64> {
    row["step_index"]
        .as_u64()
        .context("ANTIGRAVITY_INVALID_RECORD: step_index must be nonnegative integer")
}
pub(super) fn time(row: &Value) -> Result<i64> {
    Ok(
        chrono::DateTime::parse_from_rfc3339(string(row, "created_at")?)
            .context("ANTIGRAVITY_INVALID_RECORD: invalid created_at")?
            .timestamp_millis(),
    )
}
pub(super) fn role(row: &Value) -> Option<&'static str> {
    match (row["source"].as_str()?, row["type"].as_str()?) {
        ("USER_EXPLICIT", "USER_INPUT") => Some("user"),
        ("MODEL", "PLANNER_RESPONSE") if row["status"] == "DONE" => Some("assistant"),
        _ => None,
    }
}
pub(super) fn text(row: &Value) -> Option<&str> {
    let text = row["content"].as_str()?;
    if role(row) != Some("user") {
        return Some(text);
    }
    let Some(wrapped) = text.strip_prefix("<USER_REQUEST>\n") else {
        return Some(text);
    };
    let Some((request, metadata)) =
        wrapped.rsplit_once("\n</USER_REQUEST>\n<ADDITIONAL_METADATA>\n")
    else {
        return Some(text);
    };
    let Some(timestamp) = metadata
        .strip_prefix("The current local time is: ")
        .and_then(|s| s.strip_suffix(".\n</ADDITIONAL_METADATA>"))
    else {
        return Some(text);
    };
    if chrono::DateTime::parse_from_rfc3339(timestamp).is_ok() {
        Some(request)
    } else {
        Some(text)
    }
}
