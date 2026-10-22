use super::*;
use std::collections::HashMap;

pub(super) fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value[field]
        .as_str()
        .filter(|s| !s.is_empty() && !s.chars().any(char::is_control))
        .with_context(|| format!("CORTEX_INVALID_RECORD: missing/invalid {field}"))
}
pub(super) fn time(value: &Value) -> Result<Option<i64>> {
    match &value["timestamp"] {
        Value::Null => Ok(None),
        Value::Number(n) => Ok(n.as_i64()),
        Value::String(s) => Ok(Some(
            chrono::DateTime::parse_from_rfc3339(s)
                .context("CORTEX_INVALID_RECORD: timestamp")?
                .timestamp_millis(),
        )),
        _ => anyhow::bail!("CORTEX_INVALID_RECORD: timestamp type"),
    }
}
pub(super) fn created_at(meta: &Value) -> Result<i64> {
    let created = string(meta, "created_at")?;
    Ok(chrono::DateTime::parse_from_rfc3339(created)
        .context("CORTEX_INVALID_METADATA: created_at")?
        .timestamp_millis())
}
pub(super) fn metadata(bytes: &[u8], filename_id: &str) -> Result<Value> {
    anyhow::ensure!(bytes.len() <= 8 * 1024 * 1024, "CORTEX_OVERSIZED_METADATA");
    let meta: Value = serde_json::from_slice(bytes).context("CORTEX_INVALID_METADATA")?;
    anyhow::ensure!(meta.is_object(), "CORTEX_INVALID_METADATA: object expected");
    anyhow::ensure!(
        string(&meta, "session_id")? == filename_id,
        "CORTEX_INVALID_METADATA: filename/native session ID mismatch"
    );
    anyhow::ensure!(
        meta["history"].is_null() || meta["history"].is_array(),
        "CORTEX_UNSUPPORTED_SCHEMA: history must be array"
    );
    Ok(meta)
}
fn normalize_block(block: &Value) -> Value {
    let kind = block["type"].as_str().unwrap_or("unknown");
    match kind {
        "text" => {
            if block["internalOnly"] == true || block["is_user_prompt"] == false {
                serde_json::json!({"type":"internal_text","payload":block})
            } else {
                serde_json::json!({"type":"text","text":block["displayText"].as_str().or_else(||block["text"].as_str()).unwrap_or_default()})
            }
        }
        "thinking" => {
            serde_json::json!({"type":"thinking","thinking":block["thinking"]["text"].as_str().or_else(||block["thinking"].as_str()).unwrap_or_default()})
        }
        "tool_use" => {
            serde_json::json!({"type":"tool_use","id":block["tool_use"]["tool_use_id"],"name":block["tool_use"]["name"],"input":block["tool_use"]["input"]})
        }
        "tool_result" => {
            let status = block["tool_result"]["status"].as_str();
            let error = match status {
                Some("success" | "completed") => Some(false),
                Some("error" | "failed") => Some(true),
                _ => None,
            };
            serde_json::json!({"type":"tool_result","tool_use_id":block["tool_result"]["tool_use_id"],"content":block["tool_result"]["content"],"is_error":error,"native_status":status})
        }
        _ => block.clone(),
    }
}
fn record_to_entry(meta: &Value, record: &Value) -> Result<Value> {
    let role = string(record, "role")?;
    let id = string(record, "id")?;
    let timestamp = if role == "assistant" {
        record["assistant_sent_time"]
            .as_str()
            .or_else(|| record["user_sent_time"].as_str())
    } else {
        record["user_sent_time"].as_str()
    }
    .or_else(|| record["created_on"].as_str());
    let content = record["content"]
        .as_array()
        .context("CORTEX_INVALID_RECORD: content array required")?;
    let blocks: Vec<Value> = content.iter().map(normalize_block).collect();
    let kind = if matches!(role, "user" | "assistant") {
        role
    } else {
        "control"
    };
    Ok(serde_json::json!({
        "type":kind,"sessionId":meta["session_id"],"uuid":id,"timestamp":timestamp,
        "cwd":meta["working_directory"],"message":{"role":role,"content":blocks,"model":record["model"]},
        "native_role":role,"title":meta["title"]
    }))
}
fn parse_line(line: &[u8]) -> Result<Value> {
    let value: Value =
        serde_json::from_slice(line).context("CORTEX_INVALID_RECORD: complete JSONL line")?;
    anyhow::ensure!(value.is_object(), "CORTEX_INVALID_RECORD: object expected");
    Ok(value)
}
pub(super) fn history(meta: &Value, bytes: Option<&[u8]>) -> Result<(Vec<Value>, bool)> {
    let (rows, partial) = if let Some(bytes) = bytes {
        let end = bytes.iter().rposition(|b| *b == b'\n');
        let complete = end.map_or(&[][..], |i| &bytes[..=i]);
        let partial = !bytes.is_empty() && end != Some(bytes.len() - 1);
        let mut rows = Vec::new();
        for line in complete
            .split(|b| *b == b'\n')
            .filter(|line| !line.is_empty())
        {
            rows.push(parse_line(line)?);
        }
        (rows, partial)
    } else {
        let rows = meta["history"]
            .as_array()
            .or_else(|| meta["messages"].as_array())
            .context("CORTEX_MISSING_HISTORY: neither sidecar nor legacy array")?
            .clone();
        (rows, false)
    };
    anyhow::ensure!(
        !rows.is_empty() || partial || bytes.is_some_and(|bytes| bytes.is_empty()),
        "CORTEX_EMPTY_HISTORY"
    );
    let flat = rows
        .first()
        .is_some_and(|r| r["message_id"].is_string() || r["transcript_format"] == "flat");
    anyhow::ensure!(
        rows.iter()
            .all(|r| (r["message_id"].is_string() || r["transcript_format"] == "flat") == flat),
        "CORTEX_UNSUPPORTED_SCHEMA: mixed history variants"
    );
    let messages = if flat { merge_flat(rows)? } else { rows };
    let mut last = HashMap::new();
    for (index, message) in messages.iter().enumerate() {
        last.insert(string(message, "id")?.to_owned(), index);
    }
    let mut entries = messages
        .iter()
        .enumerate()
        .filter(|(i, m)| last.get(m["id"].as_str().unwrap_or_default()) == Some(i))
        .map(|(_, m)| record_to_entry(meta, m))
        .collect::<Result<Vec<_>>>()?;
    let mut previous = None;
    for entry in &mut entries {
        if let Some(recorded) = time(entry)? {
            previous = Some(recorded);
            entry["timestamp_origin"] = Value::String("native".into());
        } else {
            let (inferred, origin) = if let Some(previous) = previous {
                (previous, "previous_message")
            } else {
                (created_at(meta)?, "metadata.created_at")
            };
            entry["timestamp"] = Value::from(inferred);
            entry["timestamp_origin"] = Value::String(origin.into());
            previous = Some(inferred);
        }
    }
    Ok((entries, partial))
}
fn merge_flat(rows: Vec<Value>) -> Result<Vec<Value>> {
    let mut out: Vec<Value> = Vec::new();
    let mut previous_index: Option<i64> = None;
    for row in rows {
        let id = string(&row, "message_id")?;
        let role = row["source_role"]
            .as_str()
            .or_else(|| row["role"].as_str())
            .context("CORTEX_INVALID_RECORD: flat role")?;
        let index = row["content_block_index"].as_i64();
        let continuation = out
            .last()
            .is_some_and(|last| last["id"] == id && last["role"] == role)
            && matches!((previous_index,index), (Some(a),Some(b)) if b > a);
        if continuation {
            if let Some(blocks) = row["content"].as_array() {
                out.last_mut().context("flat message")?["content"]
                    .as_array_mut()
                    .context("flat content")?
                    .extend(blocks.clone());
            }
        } else {
            let mut message = row.clone();
            message["id"] = Value::String(id.to_owned());
            message["role"] = Value::String(role.to_owned());
            out.push(message);
        }
        previous_index = index;
    }
    Ok(out)
}
pub(super) fn text(content: &Value) -> String {
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
        .find(|s| !s.is_empty())
        .map(|s| s.chars().take(4096).collect())
}
