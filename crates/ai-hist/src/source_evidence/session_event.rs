//! Validation of recorded message roles, tool identities and result-only fields.
use super::{
    ERROR_SIGNALS, EVENT_SOURCES, NON_NEGATIVE_FIELDS, RESULT_STATUSES, TOOL_RESULT_FIELDS,
};
use anyhow::{ensure, Result};
use serde_json::{Map, Value};

pub(super) fn validate_session_event(payload: &Map<String, Value>) -> Result<()> {
    ensure!(
        ["user", "assistant", "tool_result"]
            .contains(&payload["role"].as_str().unwrap_or_default())
            && ["text", "thinking", "tool_use", "tool_result"]
                .contains(&payload["kind"].as_str().unwrap_or_default()),
        "INVALID_ARGUMENT: invalid event role or kind"
    );
    if let Some(value) = payload.get("source_order").filter(|value| !value.is_null()) {
        ensure!(
            value.as_i64().is_some_and(|order| order >= 0),
            "INVALID_ARGUMENT: source_order must be a nonnegative integer"
        );
    }
    // `control_kind` is a closed vocabulary, and every reader -- the
    // user-turn page, prompt attribution, the plugin's turn export --
    // treats any non-null value as authoritative without rechecking
    // it. A spelling nobody classifies, or a kind on a row the
    // contract says can never carry one, would silently hide real
    // evidence, so it is refused here rather than stored.
    if let Some(value) = payload.get("control_kind").filter(|v| !v.is_null()) {
        ensure!(
            value
                .as_str()
                .is_some_and(|value| crate::ingest::control::ControlKind::parse(value).is_some()),
            "INVALID_ARGUMENT: unsupported control_kind value"
        );
        ensure!(
            payload["role"].as_str() == Some("user") && payload["kind"].as_str() == Some("text"),
            "INVALID_ARGUMENT: control_kind is only valid on a user text event"
        );
    }
    let tool_result = payload["kind"].as_str() == Some("tool_result");
    for field in TOOL_RESULT_FIELDS {
        let Some(value) = payload.get(*field).filter(|v| !v.is_null()) else {
            continue;
        };
        // A native call ID joins both tool-use and tool-result blocks;
        // every other fidelity field describes a result only.
        ensure!(
            tool_result || (*field == "tool_use_id" && payload["kind"] == "tool_use"),
            "INVALID_ARGUMENT: {field} is only valid on a tool_result event (tool_use_id also accepts tool_use)"
        );
        validate_tool_result_field(field, value)?;
    }
    Ok(())
}

fn validate_tool_result_field(field: &str, value: &Value) -> Result<()> {
    if field == "tool_use_id" {
        ensure!(
            value.as_str().is_some_and(|id| !id.trim().is_empty()),
            "INVALID_ARGUMENT: tool_use_id must not be empty"
        );
    }
    if NON_NEGATIVE_FIELDS.contains(&field) {
        ensure!(
            value.as_i64().is_some_and(|n| n >= 0),
            "INVALID_ARGUMENT: {field} must not be negative"
        );
    }
    let vocabulary = match field {
        "result_status" => Some(RESULT_STATUSES),
        "event_source" => Some(EVENT_SOURCES),
        "error_signal" => Some(ERROR_SIGNALS),
        _ => None,
    };
    if let Some(allowed) = vocabulary {
        ensure!(
            value.as_str().is_some_and(|v| allowed.contains(&v)),
            "INVALID_ARGUMENT: unsupported {field} value"
        );
    }
    Ok(())
}
