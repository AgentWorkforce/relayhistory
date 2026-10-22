use anyhow::{Context, Result};
use serde_json::Value;
use std::collections::HashSet;

pub(super) fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value[field]
        .as_str()
        .filter(|s| !s.is_empty())
        .with_context(|| format!("CLINE_INVALID_RECORD: missing or empty {field}"))
}

pub(super) fn time(value: &Value) -> Result<i64> {
    Ok(
        chrono::DateTime::parse_from_rfc3339(string(value, "updated_at")?)
            .context("CLINE_INVALID_RECORD: updated_at must be RFC3339")?
            .timestamp_millis(),
    )
}

pub(super) fn message_times(messages: &[Value], fallback: i64) -> Vec<(i64, &'static str)> {
    // The native writer timestamps assistant output, not user input. A reverse
    // pass assigns each undated envelope the next native timestamp, or the
    // snapshot timestamp after the last native one. Reuse this map everywhere
    // so a long run of undated envelopes remains linear in the snapshot size.
    let mut next = None;
    let mut times = Vec::with_capacity(messages.len());
    for message in messages.iter().rev() {
        if let Some(ts) = message["ts"].as_i64() {
            next = Some(ts);
            times.push((ts, "native_ts"));
        } else if let Some(ts) = next {
            times.push((ts, "next_assistant_ts"));
        } else {
            times.push((fallback, "snapshot_updated_at"));
        }
    }
    times.reverse();
    times
}

pub(super) fn result_text(value: &Value, facts: &crate::ingest::ToolResultFacts) -> String {
    let full = if let Some(s) = value.as_str() {
        s.to_owned()
    } else if let Some(blocks) = value.as_array() {
        blocks
            .iter()
            .map(|block| {
                if block["type"] == "text" {
                    block["text"].as_str().unwrap_or("").to_owned()
                } else {
                    format!(
                        "[non-text result content: {}]",
                        block["type"].as_str().unwrap_or("unknown")
                    )
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
    } else {
        "[non-text result content]".into()
    };
    const BOUNDARY: &str = "\n\n[Native Cline result projection]\n";
    // Leave space for the exported event envelope, native IDs and timestamps.
    const MAX_ENCODED: usize = 128 * 1024;
    let has_nontext = value
        .as_array()
        .is_some_and(|blocks| blocks.iter().any(|block| block["type"] != "text"));
    let exact = if has_nontext {
        format!(
            "{full}{BOUNDARY}{}",
            serde_json::json!({"projection":"cline-tool-result-v1","truncated":false,"content":value})
        )
    } else {
        full.clone()
    };
    if Value::String(exact.clone()).to_string().len() <= MAX_ENCODED {
        return exact;
    }
    let projection = serde_json::json!({
        "projection":"cline-tool-result-v1",
        "truncated":true,
        "native_payload_bytes":facts.payload_bytes,
        "native_payload_hash":facts.payload_hash,
    });
    let prefix = full.chars().take(32 * 1024).collect::<String>();
    let bounded = format!("{prefix}{BOUNDARY}{projection}");
    if Value::String(bounded.clone()).to_string().len() <= MAX_ENCODED {
        bounded
    } else {
        format!("{BOUNDARY}{projection}")
    }
}

pub(super) fn parse(bytes: &[u8], path_id: &str) -> Result<Value> {
    let payload: Value = serde_json::from_slice(bytes)
        .context("CLINE_INCOMPLETE_SNAPSHOT: invalid or partially written JSON")?;
    anyhow::ensure!(
        payload["version"] == 1,
        "CLINE_UNSUPPORTED_SCHEMA: expected messages version 1"
    );
    anyhow::ensure!(
        string(&payload, "sessionId")? == path_id,
        "CLINE_ID_MISMATCH: file and artifact session IDs differ"
    );
    anyhow::ensure!(
        matches!(
            payload["agent"].as_str(),
            Some("lead" | "subagent" | "teammate")
        ),
        "CLINE_UNSUPPORTED_SCHEMA: agent kind"
    );
    time(&payload)?;
    let messages = payload["messages"]
        .as_array()
        .context("CLINE_INVALID_RECORD: messages must be an array")?;
    let mut ids = HashSet::new();
    for message in messages {
        let id = string(message, "id")?;
        anyhow::ensure!(ids.insert(id), "CLINE_INVALID_RECORD: duplicate message id");
        anyhow::ensure!(
            matches!(message["role"].as_str(), Some("user" | "assistant")),
            "CLINE_INVALID_RECORD: message role"
        );
        let blocks = message["content"]
            .as_array()
            .context("CLINE_INVALID_RECORD: content must be an array")?;
        for block in blocks {
            let kind = string(block, "type")?;
            match kind {
                "text" => {
                    anyhow::ensure!(
                        block["text"].is_string(),
                        "CLINE_INVALID_RECORD: text block"
                    );
                }
                "thinking" => {
                    anyhow::ensure!(
                        block["thinking"].is_string(),
                        "CLINE_INVALID_RECORD: thinking block"
                    );
                }
                "tool_use" => {
                    anyhow::ensure!(
                        string(block, "id")?.len() <= 1024 && string(block, "name")?.len() <= 256,
                        "CLINE_INVALID_RECORD: tool identity exceeds export bounds"
                    );
                    anyhow::ensure!(
                        block.get("input").is_some(),
                        "CLINE_INVALID_RECORD: tool input"
                    );
                }
                "tool_result" => {
                    string(block, "tool_use_id")?;
                    anyhow::ensure!(
                        block.get("content").is_some(),
                        "CLINE_INVALID_RECORD: tool result content"
                    );
                }
                _ => {}
            }
        }
        anyhow::ensure!(
            message.get("ts").is_none() || message["ts"].as_i64().is_some(),
            "CLINE_INVALID_RECORD: ts must be milliseconds"
        );
        if let Some(metrics) = message.get("metrics") {
            anyhow::ensure!(metrics.is_object(), "CLINE_INVALID_RECORD: metrics");
        }
    }
    Ok(payload)
}

pub(super) fn text(value: &Value) -> String {
    if let Some(s) = value.as_str() {
        return s.into();
    }
    value
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

pub(super) fn bounded_visible_text(full: &str) -> String {
    const MAX_ENCODED: usize = 128 * 1024;
    if Value::String(full.to_owned()).to_string().len() <= MAX_ENCODED {
        return full.to_owned();
    }
    let facts = crate::ingest::ToolResultFacts::from_payload(&Value::String(full.to_owned()));
    let projection = serde_json::json!({
        "projection":"cline-visible-text-v1", "truncated":true,
        "native_text_bytes":facts.payload_bytes, "native_text_hash":facts.payload_hash,
    });
    let boundary = "\n\n[Native Cline text projection]\n";
    let prefix = full.chars().take(32 * 1024).collect::<String>();
    let bounded = format!("{prefix}{boundary}{projection}");
    if Value::String(bounded.clone()).to_string().len() <= MAX_ENCODED {
        bounded
    } else {
        format!("{boundary}{projection}")
    }
}

pub(super) fn first_prompt(messages: &[Value]) -> Option<String> {
    messages
        .iter()
        .filter(|m| m["role"] == "user")
        .map(|m| text(&m["content"]))
        .find(|s| !s.trim().is_empty())
        .map(|s| s.chars().take(4096).collect())
}

// Keep each encoded field well below the export page's single-record budget.
// Both projections describe the exact native input using the same measurement.
const MAX_TOOL_FIELD_ENCODED: usize = 128 * 1024;

pub(super) fn tool_input_args(block: &Value) -> Result<String> {
    let exact = serde_json::to_string(&block["input"])?;
    if Value::String(exact.clone()).to_string().len() <= MAX_TOOL_FIELD_ENCODED {
        return Ok(exact);
    }
    let facts = crate::ingest::ToolResultFacts::from_payload(&block["input"]);
    let projection = serde_json::json!({
        "projection":"cline-tool-input-v1", "truncated":true,
        "native_input_bytes":facts.payload_bytes, "native_input_hash":facts.payload_hash,
    });
    Ok(projection.to_string())
}

pub(super) fn tool_use_text(block: &Value) -> Result<String> {
    let exact = serde_json::to_string(block)?;
    if Value::String(exact.clone()).to_string().len() <= MAX_TOOL_FIELD_ENCODED {
        return Ok(exact);
    }
    let facts = crate::ingest::ToolResultFacts::from_payload(&block["input"]);
    let projection = serde_json::json!({
        "projection":"cline-tool-use-v1", "truncated":true,
        "id":string(block, "id")?, "name":string(block, "name")?,
        "native_input_bytes":facts.payload_bytes, "native_input_hash":facts.payload_hash,
    });
    Ok(projection.to_string())
}
