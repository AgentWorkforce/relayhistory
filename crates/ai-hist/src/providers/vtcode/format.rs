use super::*;

pub(super) fn parse(bytes: &[u8]) -> Result<(Vec<Value>, bool)> {
    let mut rows = Vec::new();
    let mut partial = false;
    for line in bytes.split_inclusive(|b| *b == b'\n') {
        crate::ingest::check_capture_cancelled()?;
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        if !line.ends_with(b"\n") {
            partial = true;
            continue;
        }
        let row: Value = match serde_json::from_slice(line) {
            Ok(value) => value,
            Err(error) => {
                return Err(error).context("VTCODE_INVALID_RECORD: malformed canonical JSONL")
            }
        };
        anyhow::ensure!(
            row["schema_version"] == "0.18.0",
            "VTCODE_UNSUPPORTED_SCHEMA: requires event schema 0.18.0"
        );
        string(&row["event"], "type")?;
        rows.push(row["event"].clone());
    }
    anyhow::ensure!(
        !rows.is_empty(),
        "VTCODE_INVALID_RECORD: empty canonical journal"
    );
    Ok((rows, partial))
}

pub(super) fn string<'a>(v: &'a Value, field: &str) -> Result<&'a str> {
    v[field]
        .as_str()
        .filter(|s| !s.is_empty())
        .with_context(|| format!("VTCODE_INVALID_RECORD: {field} missing"))
}

pub(super) fn text<'a>(v: &'a Value, field: &str) -> Result<&'a str> {
    v[field]
        .as_str()
        .with_context(|| format!("VTCODE_INVALID_RECORD: {field} missing"))
}

pub(super) fn time(text: &str) -> Result<i64> {
    Ok(chrono::DateTime::parse_from_rfc3339(text)
        .context("VTCODE_INVALID_RECORD: invalid timestamp")?
        .timestamp_millis())
}

pub(super) fn event_time(event: &Value) -> Result<Option<i64>> {
    let text = event["context"]["timestamp"]
        .as_str()
        .or_else(|| event["item"]["context"]["timestamp"].as_str())
        .or_else(|| event["completed_at"].as_str());
    text.map(time).transpose()
}

pub(super) fn first_prompt(rows: &[Value], native: &str) -> Option<(String, i64, String)> {
    rows.iter().find_map(|row| {
        if row["type"] != "turn.started"
            || row["context"]["origin"] != "user"
            || row["context"]["actor_id"] != native
        {
            return None;
        }
        let goal = row["context"]["goal"]
            .as_str()?
            .chars()
            .take(4096)
            .collect();
        let id = row["context"]["turn_id"].as_str()?.to_owned();
        let ts = time(row["context"]["timestamp"].as_str()?).ok()?;
        Some((goal, ts, id))
    })
}
