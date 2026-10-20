use super::*;
pub(super) fn string<'a>(row: &'a Value, key: &str) -> Result<&'a str> {
    row[key]
        .as_str()
        .filter(|s| !s.is_empty())
        .with_context(|| format!("CODEBUDDY_INVALID_RECORD: missing {key}"))
}
pub(super) fn time(row: &Value) -> Result<i64> {
    row["timestamp"]
        .as_i64()
        .context("CODEBUDDY_INVALID_RECORD: timestamp must be integer milliseconds")
}
pub(super) fn parse(bytes: &[u8], bounded: bool) -> Result<(Vec<Value>, bool)> {
    let mut rows = Vec::new();
    let mut partial = false;
    let mut ids = std::collections::BTreeSet::new();
    let mut previous: Option<Value> = None;
    for line in bytes.split_inclusive(|b| *b == b'\n') {
        crate::ingest::check_capture_cancelled()?;
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        if bounded && !line.ends_with(b"\n") {
            break;
        }
        let row: Value = match serde_json::from_slice(line) {
            Ok(v) => v,
            Err(e) if e.is_eof() && !line.ends_with(b"\n") => {
                partial = true;
                continue;
            }
            Err(e) => return Err(e).context("CODEBUDDY_INVALID_RECORD: malformed JSONL"),
        };
        string(&row, "type")?;
        // Session metadata facts can omit clocks; they never invent event time.
        if row.get("timestamp").is_some() {
            time(&row)?;
        }
        if matches!(
            row["type"].as_str(),
            Some("message" | "function_call" | "function_call_result" | "reasoning")
        ) {
            time(&row)?;
            let id = string(&row, "id")?;
            let fresh = ids.insert(id.to_owned());
            anyhow::ensure!(
                fresh
                    || previous.as_ref().is_some_and(|p| p["id"] == id
                        && p["type"] == row["type"]
                        && p["parentId"] == row["parentId"]),
                "CODEBUDDY_INVALID_RECORD: conflicting native ID redelivery"
            );
        }
        validate_shapes(&row)?;
        previous = Some(row.clone());
        rows.push(row);
    }
    Ok((rows, partial))
}
pub(super) fn text(row: &Value) -> String {
    if let Some(text) = row["content"].as_str() {
        return text.into();
    }
    row["content"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|b| matches!(b["type"].as_str(), Some("input_text" | "output_text")))
        .filter_map(|b| b["text"].as_str())
        .collect::<Vec<_>>()
        .join("\n")
}
pub(super) fn first_prompt(rows: &[Value]) -> Option<String> {
    rows.iter()
        .find(|r| human_message(r) && r["role"] == "user")
        .map(text)
        .filter(|t| !t.is_empty())
}

pub(super) fn fact_type(kind: &str) -> bool {
    matches!(
        kind,
        "custom-title"
            | "ai-title"
            | "file-history-snapshot"
            | "summary"
            | "topic"
            | "goal-result"
            | "goal-progress"
            | "turn-metrics"
            | "resend-fork-notice"
            | "acp-terminal-state"
            | "request-recovery"
            | "model-usage"
            | "credit-usage"
            | "session-meta"
    )
}
pub(super) fn known_type(kind: &str) -> bool {
    fact_type(kind)
        || matches!(
            kind,
            "message" | "function_call" | "function_call_result" | "reasoning"
        )
}
pub(super) fn human_message(row: &Value) -> bool {
    if row["type"] != "message" {
        return false;
    }
    if row["role"] == "user" {
        return external_user(row);
    }
    row["role"] == "assistant"
        && row["providerData"]["agent"] != "compact"
        && row["providerData"]["isCompactInternal"] != true
        && row["providerData"]["isMeta"] != true
        && !truthy(&row["providerData"]["skipRun"])
}
fn external_user(row: &Value) -> bool {
    let data = &row["providerData"];
    if truthy(&data["skipRun"])
        || data["isCompactInternal"] == true
        || data["agent"] == "compact"
        || data["teammateMessage"]["from"].is_string()
        || truthy(&data["isAdditionalData"])
    {
        return false;
    }
    if let Some(explicit) = data.get("startsNewUserRequest") {
        return explicit == true
            && data["isCompacted"] != true
            && data["isSummary"] != true
            && !data["compactType"].is_string();
    }
    let text = text(row).to_lowercase();
    data["isMeta"] != true && !word_end(&text, "<teammate-message") && !compact_boundary(row, &text)
}
fn compact_boundary(row: &Value, text: &str) -> bool {
    let data = &row["providerData"];
    if data["isPtlRecovery"] == true || data["isMediaBodyRecovery"] == true {
        return false;
    }
    if data["isCompacted"] == true || data["isSummary"] == true {
        return true;
    }
    let boundary = word_end(text, "<cb_summary")
        || word_end(text, "<conversation_history_summary")
        || compact_attribute(text);
    let continuation =
        word_end(text, "please continue") && text.contains("summarized context above") && !boundary;
    boundary || (data["compactType"].is_string() && !continuation)
}

pub(super) fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(v) => *v,
        Value::Number(n) => n.as_f64() != Some(0.0),
        Value::String(s) => !s.is_empty(),
        _ => true,
    }
}

fn validate_shapes(row: &Value) -> Result<()> {
    if row["type"] == "message" {
        anyhow::ensure!(
            row["content"].is_string() || row["content"].is_array(),
            "CODEBUDDY_UNSUPPORTED_CONTENT: message content must be string or blocks"
        );
        for block in row["content"].as_array().into_iter().flatten() {
            if matches!(block["type"].as_str(), Some("input_text" | "output_text")) {
                anyhow::ensure!(
                    block["text"].is_string(),
                    "CODEBUDDY_INVALID_RECORD: text block has no text"
                );
            }
        }
    }
    for field in ["parentId", "logicalParentId"] {
        anyhow::ensure!(
            row.get(field)
                .is_none_or(|v| v.is_null() || v.as_str().is_some_and(|s| !s.is_empty())),
            "CODEBUDDY_INVALID_RECORD: invalid native parent"
        );
    }
    Ok(())
}

fn word_end(text: &str, needle: &str) -> bool {
    text.match_indices(needle).any(|(i, _)| {
        text.as_bytes()
            .get(i + needle.len())
            .is_none_or(|b| !b.is_ascii_alphanumeric() && *b != b'_')
    })
}
fn compact_attribute(text: &str) -> bool {
    text.match_indices("data-role=").any(|(i, _)| {
        let rest = &text[i + 10..];
        matches!(rest.as_bytes().first(), Some(b'\'' | b'"'))
            && rest
                .get(1..)
                .and_then(|s| s.strip_prefix("compact-summary"))
                .is_some_and(|s| matches!(s.as_bytes().first(), Some(b'\'' | b'"')))
    })
}
