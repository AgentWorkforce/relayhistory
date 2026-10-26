use super::*;

pub(super) fn tool_id<'a>(item: &'a Value, calls: &'a BTreeMap<String, String>) -> Result<&'a str> {
    let call_id = format::string(item, "call_id")?;
    Ok(item["tool_call_id"]
        .as_str()
        .or_else(|| calls.get(call_id).map(String::as_str))
        .unwrap_or(call_id))
}

pub(super) fn result(
    conn: &Connection,
    sid: &str,
    cwd: Option<&str>,
    item: &Value,
    calls: &BTreeMap<String, String>,
    keys: &mut Keys,
) -> Result<()> {
    let tool_id = result_event(conn, sid, cwd, item, calls, keys)?;
    set_tool_call_error(conn, ID, sid, &tool_id, item["status"] == "failed")?;
    keys.results.insert(tool_id);
    Ok(())
}

pub(super) fn result_event(
    conn: &Connection,
    sid: &str,
    cwd: Option<&str>,
    item: &Value,
    calls: &BTreeMap<String, String>,
    keys: &mut Keys,
) -> Result<String> {
    let id = format::string(item, "id")?;
    let tool_id = tool_id(item, calls)?;
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
    keys.events.insert(uid);
    Ok(tool_id.into())
}
