//! A stored bubble rendered as the transcript records Cursor writes for it.

use serde_json::{json, Map, Value};

/// The bubble's creation time in epoch milliseconds: `createdAt` (ISO 8601 on
/// current builds, a number on older ones), else when its work started.
fn bubble_time(bubble: &Value) -> Option<i64> {
    let created = bubble.get("createdAt");
    created
        .and_then(Value::as_str)
        .and_then(crate::parse_iso_ms)
        .or_else(|| created.and_then(Value::as_i64))
        .or_else(|| bubble.get("startedAtMs").and_then(Value::as_i64))
        .filter(|ms| *ms > 0)
}

/// A JSON string field parsed as JSON, else kept as the string it is.
fn embedded_json(value: Option<&Value>) -> Option<Value> {
    let value = value?;
    match value {
        Value::String(text) if text.trim().is_empty() => None,
        Value::String(text) => Some(serde_json::from_str(text).unwrap_or_else(|_| value.clone())),
        Value::Null => None,
        other => Some(other.clone()),
    }
}

fn record(role: &str, id: String, at: Option<i64>, blocks: Vec<Value>) -> Value {
    let mut record = Map::new();
    record.insert("role".into(), json!(role));
    if let Some(at) = at {
        record.insert("timestamp".into(), json!(at));
    }
    record.insert("message".into(), json!({"id": id, "content": blocks}));
    Value::Object(record)
}

fn non_empty_text(value: Option<&Value>) -> Option<&str> {
    value
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
}

/// The `tool_use` block for a bubble's tool call, and the `tool_result` block
/// that answers it when the bubble holds a result.
fn tool_blocks(tool: &Value) -> Option<(Value, Option<Value>)> {
    let name = non_empty_text(tool.get("name"))?;
    let id = non_empty_text(tool.get("toolCallId"));
    let input = embedded_json(tool.get("params"))
        .or_else(|| embedded_json(tool.get("rawArgs")))
        .unwrap_or(Value::Null);
    let mut call = Map::new();
    call.insert("type".into(), json!("tool_use"));
    if let Some(id) = id {
        call.insert("id".into(), json!(id));
    }
    call.insert("name".into(), json!(name));
    call.insert("input".into(), input);
    let status = tool.get("status").and_then(Value::as_str);
    let output = embedded_json(tool.get("result"));
    let result = match (id, output) {
        (Some(id), Some(output)) => Some(json!({
            "type": "tool_result",
            "tool_use_id": id,
            "content": output,
            "is_error": status == Some("error"),
        })),
        (Some(id), None) if status == Some("error") => Some(json!({
            "type": "tool_result",
            "tool_use_id": id,
            "content": "",
            "is_error": true,
        })),
        _ => None,
    };
    Some((Value::Object(call), result))
}

/// The records for one bubble: none for a bubble with nothing the transcript
/// would show, one for a message, and two for a tool call that has its result
/// (the result is a user-role record, as the transcript writes it).
pub(super) fn bubble_records(bubble: &Value) -> Vec<Value> {
    let bubble_id = bubble
        .get("bubbleId")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let id = format!("cursor-bubble:{bubble_id}");
    let at = bubble_time(bubble);
    match bubble.get("type").and_then(Value::as_i64) {
        Some(1) => non_empty_text(bubble.get("text"))
            .map(|text| {
                vec![record(
                    "user",
                    id,
                    at,
                    vec![json!({"type": "text", "text": text})],
                )]
            })
            .unwrap_or_default(),
        Some(2) => assistant_records(bubble, id, at),
        _ => Vec::new(),
    }
}

fn assistant_records(bubble: &Value, id: String, at: Option<i64>) -> Vec<Value> {
    let mut blocks = Vec::new();
    if let Some(thinking) = non_empty_text(bubble.get("thinking").and_then(|t| t.get("text"))) {
        blocks.push(json!({"type": "thinking", "thinking": thinking}));
    }
    if let Some(text) = non_empty_text(bubble.get("text")) {
        blocks.push(json!({"type": "text", "text": text}));
    }
    let mut result = None;
    if let Some((call, answer)) = bubble.get("toolFormerData").and_then(tool_blocks) {
        blocks.push(call);
        result = answer;
    }
    if blocks.is_empty() {
        return Vec::new();
    }
    let mut out = vec![record("assistant", id.clone(), at, blocks)];
    if let Some(result) = result {
        let done = bubble
            .get("completedAtMs")
            .and_then(Value::as_i64)
            .filter(|ms| *ms > 0)
            .or(at);
        out.push(record("user", format!("{id}#result"), done, vec![result]));
    }
    out
}
