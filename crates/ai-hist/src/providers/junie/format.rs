use super::*;
use serde_json::Value;

pub(super) fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value[field]
        .as_str()
        .with_context(|| format!("JUNIE_INVALID_RECORD: missing {field}"))
}

pub(super) fn time(row: &Value) -> Result<i64> {
    let ts = row["timestampMs"]
        .as_i64()
        .context("JUNIE_INVALID_RECORD: missing timestampMs")?;
    anyhow::ensure!(ts > 0, "JUNIE_INVALID_RECORD: nonpositive timestampMs");
    Ok(ts)
}

pub(super) fn human_text(row: &Value) -> Option<&str> {
    if row["kind"] != "UserPromptEvent"
        || !row["delivery"].is_null() && row["delivery"] != "Sent"
        || !row["channel"].is_null() && row["channel"] != "Cli"
    {
        return None;
    }
    row["prompt"].as_str().filter(|prompt| !prompt.is_empty())
}

pub(super) fn parse(bytes: &[u8]) -> Result<(Vec<Value>, bool)> {
    let mut rows = Vec::new();
    let mut partial = false;
    for (index, line) in bytes.split_inclusive(|b| *b == b'\n').enumerate() {
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        if !line.ends_with(b"\n") {
            partial = true;
            break;
        }
        match serde_json::from_slice::<Value>(line) {
            Ok(row) => {
                validate(&row).with_context(|| format!("Junie event line {}", index + 1))?;
                rows.push(row);
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("JUNIE_INVALID_RECORD: line {}", index + 1))
            }
        }
    }
    Ok((rows, partial))
}

pub(super) fn validate(row: &Value) -> Result<()> {
    let kind = string(row, "kind")?;
    time(row)?;
    match kind {
        "UserPromptEvent" => {
            string(row, "prompt")?;
            if let Some(request_id) = row.get("requestId") {
                anyhow::ensure!(
                    request_id.is_null() || request_id.as_str().is_some_and(|id| !id.is_empty()),
                    "JUNIE_INVALID_RECORD: requestId"
                );
            }
        }
        "SessionA2uxEvent" => {
            let event = row
                .get("event")
                .context("JUNIE_INVALID_RECORD: missing event")?;
            string(event, "state")?;
            let agent = event
                .get("agentEvent")
                .context("JUNIE_INVALID_RECORD: missing agentEvent")?;
            let subkind = string(agent, "kind")?;
            if matches!(
                subkind,
                "MarkdownBlockUpdatedEvent" | "ResultBlockUpdatedEvent" | "ToolBlockUpdatedEvent"
            ) {
                let key = if subkind == "ResultBlockUpdatedEvent" {
                    "result"
                } else {
                    "text"
                };
                string(agent, key)?;
            }
        }
        _ => {}
    }
    Ok(())
}
