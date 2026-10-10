use super::{
    MuseEvent, MuseMetadata, MuseSubagentLink, MuseToolCall, MuseToolResult, METADATA_PAYLOAD_TYPE,
};
use serde_json::Value;

/// `recorded_at` microseconds as epoch milliseconds.
pub(crate) fn recorded_ms(value: &Value) -> Option<i64> {
    value
        .get("recorded_at")
        .and_then(Value::as_i64)
        .filter(|micros| *micros > 0)
        .map(|micros| micros / 1000)
}

pub(super) fn text_field(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .map(str::to_string)
}

pub(super) fn stream_id(value: &Value) -> Option<&str> {
    value.pointer("/stream/id").and_then(Value::as_str)
}

/// The session a metadata record names, or `None` when the record is not one
/// (or names no session). Identity comes only from `stream.id` on a
/// `session`-kind stream, never from the directory name.
pub(crate) fn parse_metadata(value: &Value) -> Option<MuseMetadata> {
    if value.get("payload_type").and_then(Value::as_str) != Some(METADATA_PAYLOAD_TYPE) {
        return None;
    }
    if value.pointer("/stream/kind").and_then(Value::as_str) != Some("session") {
        return None;
    }
    let session_id = stream_id(value).filter(|id| !id.is_empty())?;
    let record = value.pointer("/payload/record").unwrap_or(&Value::Null);
    Some(MuseMetadata {
        session_id: session_id.to_string(),
        workspace_root: text_field(record, "workspace_root"),
        model_id: text_field(record, "model_id"),
        provider_id: text_field(record, "provider_id"),
        cli_version: record
            .pointer("/build/semver")
            .and_then(Value::as_str)
            .filter(|version| !version.is_empty())
            .map(str::to_string),
        recorded_ms: recorded_ms(value),
    })
}

/// Interpret one record that is already known to be on the session's stream.
/// `None` for the bookkeeping and diagnostics this reader does not keep.
pub(crate) fn parse_event(value: &Value) -> Option<MuseEvent> {
    let payload = value.get("payload")?;
    match value.get("payload_type").and_then(Value::as_str)? {
        METADATA_PAYLOAD_TYPE => parse_metadata(value).map(MuseEvent::Metadata),
        "runtime.session" => {
            if payload.get("kind").and_then(Value::as_str) != Some("run") {
                return None;
            }
            parse_run_event(payload.get("event")?)
        }
        "tool_batch.effect.terminal" => {
            let record = payload.get("record")?;
            let call_id = text_field(record, "call_id")?;
            let outcome = record.get("outcome")?;
            Some(MuseEvent::ToolOutcome {
                call_id,
                status: text_field(outcome, "kind")?,
                reason: text_field(outcome, "reason"),
            })
        }
        "session.opened.observed" => Some(MuseEvent::SessionOpened {
            resume: payload
                .pointer("/record/resume")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        }),
        "session.resumed" => Some(MuseEvent::SessionResumed),
        "session.end" => Some(MuseEvent::SessionEnd {
            exit_reason: payload
                .get("record")
                .and_then(|record| text_field(record, "exit_reason")),
        }),
        _ => None,
    }
}

fn parse_run_event(event: &Value) -> Option<MuseEvent> {
    match event.get("kind").and_then(Value::as_str)? {
        "started" => text_field(event, "prompt").map(|text| MuseEvent::Prompt { text }),
        "reasoning_committed" => Some(MuseEvent::Reasoning {
            message_id: text_field(event, "message_id"),
            text: text_field(event, "text"),
            encrypted: event
                .get("encrypted_content")
                .is_some_and(|content| !content.is_null() && content != ""),
        }),
        "assistant_message_committed" => Some(MuseEvent::AssistantText {
            message_id: text_field(event, "message_id"),
            response_id: text_field(event, "response_id"),
            text: text_field(event, "text")?,
        }),
        "assistant_tool_calls_committed" => {
            let calls: Vec<MuseToolCall> = event
                .get("tool_calls")
                .and_then(Value::as_array)
                .map(|calls| calls.iter().filter_map(parse_tool_call).collect())
                .unwrap_or_default();
            (!calls.is_empty()).then(|| MuseEvent::ToolCalls {
                message_id: text_field(event, "message_id"),
                response_id: text_field(event, "response_id"),
                calls,
            })
        }
        "tool_result_batch_committed" => {
            let results: Vec<MuseToolResult> = event
                .get("results")
                .and_then(Value::as_array)
                .map(|results| {
                    results
                        .iter()
                        .filter_map(|result| {
                            Some(MuseToolResult {
                                call_id: text_field(result, "tool_call_id")?,
                                text: result
                                    .get("text")
                                    .and_then(Value::as_str)
                                    .map(str::to_string),
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();
            (!results.is_empty()).then_some(MuseEvent::ToolResults { results })
        }
        "model_completed" => Some(MuseEvent::ModelCompleted {
            model: text_field(event, "model"),
            usage: event
                .get("usage")
                .filter(|usage| usage.is_object())
                .cloned(),
            finish_reason: text_field(event, "finish_reason"),
            duration_ms: event.get("duration_ms").and_then(Value::as_i64),
        }),
        "task_stream_linked" => {
            let display = event.get("display").unwrap_or(&Value::Null);
            Some(MuseEvent::SubagentLinked(MuseSubagentLink {
                path: text_field(display, "path"),
                role: text_field(display, "role"),
                label: text_field(display, "label"),
                // `same-as-main` is a policy, not a model name.
                model: text_field(display, "model").filter(|model| model != "same-as-main"),
                task_id: text_field(event, "task_id"),
                child_session_id: None,
            }))
        }
        "memory_reminder_child_session_linked" => {
            Some(MuseEvent::SubagentLinked(MuseSubagentLink {
                path: text_field(event, "child_session_log_path"),
                role: Some("reminder".to_string()),
                label: text_field(event, "reminder_agent_id"),
                model: None,
                task_id: text_field(event, "task_id"),
                child_session_id: text_field(event, "child_session_id"),
            }))
        }
        "terminal" => Some(MuseEvent::RunTerminal {
            status: text_field(event, "terminal"),
            reason: text_field(event, "reason"),
        }),
        _ => None,
    }
}

pub(super) fn parse_tool_call(call: &Value) -> Option<MuseToolCall> {
    // `call_id` is what results and outcomes name; `id` is the provider's
    // response item id and only the fallback.
    let call_id = text_field(call, "call_id").or_else(|| text_field(call, "id"))?;
    let name = text_field(call, "name")?;
    let arguments = match call.get("args") {
        Some(Value::String(raw)) => {
            serde_json::from_str(raw).unwrap_or_else(|_| Value::String(raw.clone()))
        }
        Some(other) => other.clone(),
        None => Value::Null,
    };
    Some(MuseToolCall {
        call_id,
        name,
        arguments,
    })
}
