//! A stored message rendered as the transcript record Cursor writes for it.

use serde_json::{json, Map, Value};

fn blocks_of(content: &Value) -> Vec<Value> {
    match content {
        Value::String(text) => vec![json!({"type": "text", "text": text})],
        Value::Array(blocks) => blocks.clone(),
        _ => Vec::new(),
    }
}

/// The transcript record Cursor writes for one stored message, or `None` for
/// a message the transcript leaves out.
pub(super) fn transcript_record(
    blob_id: &str,
    message: &Value,
    preamble_open: &mut bool,
) -> Option<Value> {
    let role = message.get("role").and_then(Value::as_str)?;
    let content = blocks_of(message.get("content").unwrap_or(&Value::Null));
    let (role, blocks): (&str, Vec<Value>) = match role {
        "system" => return None,
        "user" => {
            // The environment preamble Cursor sends ahead of the first
            // prompt: not something the person typed, and not in the
            // transcript.
            let is_preamble = content.iter().all(|block| {
                block
                    .get("text")
                    .and_then(Value::as_str)
                    .is_some_and(|text| text.trim_start().starts_with("<user_info>"))
            });
            if *preamble_open && is_preamble && !content.is_empty() {
                return None;
            }
            *preamble_open = false;
            let blocks = content
                .iter()
                .filter_map(|block| match block.get("type").and_then(Value::as_str) {
                    Some("text") => Some(json!({"type": "text", "text": block.get("text")?})),
                    Some("image") | Some("file") => None,
                    _ => None,
                })
                .collect();
            ("user", blocks)
        }
        "assistant" => {
            *preamble_open = false;
            let blocks = content
                .iter()
                .filter_map(|block| match block.get("type").and_then(Value::as_str) {
                    Some("text") => Some(json!({"type": "text", "text": block.get("text")?})),
                    Some("reasoning") => Some(json!({
                        "type": "thinking",
                        "thinking": block.get("text").or_else(|| block.get("reasoning"))?,
                    })),
                    Some("tool-call") => {
                        let mut tool = Map::new();
                        tool.insert("type".into(), json!("tool_use"));
                        if let Some(id) = block.get("toolCallId") {
                            tool.insert("id".into(), id.clone());
                        }
                        tool.insert("name".into(), block.get("toolName")?.clone());
                        tool.insert(
                            "input".into(),
                            block
                                .get("args")
                                .or_else(|| block.get("input"))
                                .cloned()
                                .unwrap_or(Value::Null),
                        );
                        Some(Value::Object(tool))
                    }
                    _ => None,
                })
                .collect();
            ("assistant", blocks)
        }
        // Cursor writes a tool result back as its own `tool` message; the
        // transcript shape puts it in a user-role record, as tool results are.
        "tool" => {
            let blocks = content
                .iter()
                .filter_map(|block| match block.get("type").and_then(Value::as_str) {
                    Some("tool-result") => {
                        let mut result = Map::new();
                        result.insert("type".into(), json!("tool_result"));
                        result.insert("tool_use_id".into(), block.get("toolCallId")?.clone());
                        let output = block
                            .get("result")
                            .or_else(|| block.get("output"))
                            .cloned()
                            .unwrap_or(Value::Null);
                        result.insert("content".into(), output);
                        if let Some(is_error) = block.get("isError").and_then(Value::as_bool) {
                            result.insert("is_error".into(), json!(is_error));
                        }
                        Some(Value::Object(result))
                    }
                    _ => None,
                })
                .collect();
            ("user", blocks)
        }
        _ => return None,
    };
    if blocks.is_empty() {
        return None;
    }
    Some(json!({
        "role": role,
        "message": {"id": format!("cursor-blob:{blob_id}"), "content": blocks},
    }))
}
