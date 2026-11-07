use super::*;
use crate::ingest::{
    insert_session_event, insert_tool_call, set_tool_call_error, EventRow, ToolCallRef,
    ToolResultFacts,
};
use crate::providers::file_history::Keys;
pub(super) fn write(
    conn: &Connection,
    base: &EventRow<'_>,
    block: &Value,
    uid: &str,
    keys: &mut Keys,
) -> Result<bool> {
    let raw_kind = format::string(block, "type")?;
    let mut facts = ToolResultFacts::default();
    let (kind, text) = match raw_kind {
        "text" => (
            "text",
            Some(
                block["text"]
                    .as_str()
                    .context("DROID_INVALID_RECORD: text must be a string")?
                    .to_owned(),
            ),
        ),
        "thinking" => (
            "thinking",
            Some(
                block["thinking"]
                    .as_str()
                    .context("DROID_INVALID_RECORD: thinking must be a string")?
                    .to_owned(),
            ),
        ),
        "tool_use" => {
            let id = format::string(block, "id")?;
            let name = format::string(block, "name")?;
            let args = serde_json::to_string(&block["input"])?;
            insert_tool_call(
                conn,
                &ToolCallRef {
                    source: ID,
                    session_id: base.session_id,
                    message_id: base.message_id,
                    tool_use_id: id,
                    ts_ms: base.ts_ms,
                    cwd: base.cwd,
                    ..Default::default()
                },
                name,
                block["input"]["file_path"].as_str(),
                &args,
                None,
            )?;
            keys.tools.insert(id.into());
            facts.tool_use_id = Some(id.into());
            ("tool_use", Some(serde_json::to_string(block)?))
        }
        "tool_result" => {
            let id = format::string(block, "tool_use_id")?;
            facts = ToolResultFacts::from_payload(&block["content"]);
            facts.tool_use_id = Some(id.into());
            facts.event_source = Some("tool_result".into());
            if let Some(error) = block.get("is_error") {
                let error = error
                    .as_bool()
                    .context("DROID_INVALID_RECORD: tool is_error must be boolean")?;
                facts.result_status = Some(if error { "errored" } else { "completed" }.into());
                facts.error_signal = error.then(|| "tool_result.is_error".into());
                set_tool_call_error(conn, ID, base.session_id, id, error)?;
                keys.results.insert(id.into());
            }
            let text = block["content"]
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| format::text(&block["content"]));
            ("tool_result", Some(text))
        }
        _ => {
            normalize::marker(
                conn,
                base.session_id,
                &serde_json::json!({"type":"opaque_block","block":block,"messageId":base.message_id,"timestamp":chrono::DateTime::from_timestamp_millis(base.ts_ms).context("Droid invalid time")?.to_rfc3339()}),
                &format!("block:{uid}"),
                keys,
            )?;
            return Ok(false);
        }
    };
    insert_session_event(
        conn,
        &EventRow {
            event_uid: uid,
            kind,
            text: text.as_deref(),
            role: if kind == "tool_result" {
                "tool_result"
            } else {
                base.role
            },
            tool_result_facts: Some(&facts),
            ..*base
        },
    )?;
    keys.events.insert(uid.into());
    Ok(true)
}
