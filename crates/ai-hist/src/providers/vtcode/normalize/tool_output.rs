use super::*;
use rusqlite::OptionalExtension;

pub(super) fn result(
    conn: &Connection,
    sid: &str,
    cwd: Option<&str>,
    item: &Value,
    keys: &mut Keys,
) -> Result<()> {
    let id = format::string(item, "id")?;
    let call_id = format::string(item, "call_id")?;
    let invocation_tool_id: Option<String> = conn
        .query_row(
            "SELECT tool_use_id FROM tool_calls WHERE source=?1 AND session_id=?2 AND message_id=?3",
            rusqlite::params![ID, sid, call_id],
            |row| row.get(0),
        )
        .optional()?;
    let tool_id = item["tool_call_id"]
        .as_str()
        .or(invocation_tool_id.as_deref())
        .unwrap_or(call_id);
    let ts = format::time(format::string(&item["context"], "timestamp")?)?;
    let text = item["output"].as_str().unwrap_or("");
    let error = item["status"] == "failed";
    let mut facts = ToolResultFacts::from_payload(&Value::String(text.into()));
    facts.tool_use_id = Some(tool_id.into());
    facts.result_status = Some(if error { "errored" } else { "completed" }.into());
    facts.error_signal = error.then(|| "status_failed".into());
    let uid = format!("{id}:0");
    insert_session_event(
        conn,
        &EventRow {
            source: ID,
            session_id: sid,
            message_id: id,
            event_uid: &uid,
            role: "tool_result",
            kind: "tool_result",
            text: Some(text),
            ts_ms: ts,
            cwd,
            project: cwd,
            tool_result_facts: Some(&facts),
            raw_kind: Some("tool_output"),
            ..Default::default()
        },
    )?;
    set_tool_call_error(conn, ID, sid, tool_id, error)?;
    keys.results.insert(tool_id.into());
    keys.events.insert(uid);
    Ok(())
}
