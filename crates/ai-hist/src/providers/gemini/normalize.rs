use super::*;
use crate::ingest::{
    insert_session_event, insert_tool_call, set_tool_call_error, upsert_session_rebuilt, EventRow,
    SessionCatalogRow, ToolCallRef, ToolResultFacts,
};
use crate::providers::file_history::{self, Keys};
use crate::{insert_history, insert_session_marker, HistoryEntry, NewSessionMarker};
pub(super) fn check_identity(conn: &Connection, sid: &str, path: &Path) -> Result<()> {
    // Only a retained legacy sibling may hand ownership to its migrated journal.
    use rusqlite::OptionalExtension;
    let old: Option<String> = conn
        .query_row(
            "SELECT raw_path FROM sessions WHERE source=?1 AND session_id=?2",
            [ID, sid],
            |r| r.get(0),
        )
        .optional()?
        .flatten();
    if old.as_ref().is_some_and(|old| {
        path_is_legacy(Path::new(old)) && Path::new(old).with_extension("jsonl") == path
    }) {
        return Ok(());
    }
    file_history::check_identity(conn, ID, "GEMINI", sid, path)
}
pub(super) fn write(conn: &Connection, payload: &Value, path: &Path) -> Result<usize> {
    let conversation = &payload["conversation"];
    let sid = format::string(payload, "ledgerId")?;
    check_identity(conn, sid, path)?;
    let messages = conversation["messages"]
        .as_array()
        .context("Gemini messages missing")?;
    let first = format::date(conversation, "startTime")?;
    let mut last = format::date(conversation, "lastUpdated")?;
    let mut keys = Keys::default();
    let mut count = 0;
    // Preserve native identity, workspace directories and every original revision,
    // including superseded/rewound content; markers never become human turns.
    for (index, record) in payload["records"].as_array().unwrap().iter().enumerate() {
        marker(
            conn,
            sid,
            record,
            &format!("journal:{index}"),
            "journal_record",
            &mut keys,
        )?;
    }
    let mut assistant = None;
    for msg in messages {
        crate::ingest::check_capture_cancelled()?;
        last = last.max(format::date(msg, "timestamp")?);
        if !matches!(msg["type"].as_str(), Some("user" | "gemini")) {
            marker(
                conn,
                sid,
                msg,
                &format!("status:{}", format::string(msg, "id")?),
                "status",
                &mut keys,
            )?;
            continue;
        }
        count += message(conn, sid, msg, &mut keys)?;
        if msg["type"] == "gemini" {
            let text = format::text(&msg["content"]);
            if !text.is_empty() {
                assistant = Some(text);
            }
        }
    }
    let complete = payload["partial"] == false;
    if complete {
        file_history::retire(conn, ID, sid, &keys)?;
        // The new events must exist before ownership reconciliation so a
        // canonical prompt row can move without changing its public ID.
        file_history::retire_locator_sessions(conn, ID, path, Some(sid))?;
    }
    let writer = if complete {
        upsert_session_rebuilt
    } else {
        crate::ingest::upsert_session
    };
    writer(
        conn,
        &SessionCatalogRow {
            source: ID,
            session_id: sid,
            cwd: None,
            git_branch: None,
            first_ts: first,
            last_ts: last,
            last_assistant_text: complete.then_some(assistant.as_deref()).flatten(),
            raw_path: Some(&path.to_string_lossy()),
        },
    )?;
    finalize(conn, sid, conversation, messages, complete)?;
    Ok(count)
}
fn marker(
    conn: &Connection,
    sid: &str,
    value: &Value,
    uid: &str,
    kind: &str,
    keys: &mut Keys,
) -> Result<()> {
    let payload = serde_json::to_string(&crate::ingest::bound_marker_value(value.clone()))?;
    insert_session_marker(
        conn,
        ID,
        sid,
        &NewSessionMarker {
            marker_uid: uid,
            kind,
            ts_ms: value
                .get("timestamp")
                .and_then(|_| format::date(value, "timestamp").ok()),
            message_id: value["id"].as_str(),
            payload_json: Some(&payload),
            ..Default::default()
        },
    )?;
    keys.markers.insert(uid.into());
    Ok(())
}
fn message(conn: &Connection, sid: &str, msg: &Value, keys: &mut Keys) -> Result<usize> {
    let mid = format::string(msg, "id")?;
    let role = if msg["type"] == "gemini" {
        "assistant"
    } else {
        "user"
    };
    let tokens = msg
        .get("tokens")
        .filter(|v| !v.is_null())
        .map(serde_json::to_string)
        .transpose()?;
    let base = EventRow {
        source: ID,
        session_id: sid,
        message_id: mid,
        ts_ms: format::date(msg, "timestamp")?,
        role,
        model: msg["model"].as_str(),
        token_json: tokens.as_deref(),
        raw_kind: Some("recorded_message"),
        ..Default::default()
    };
    let parts = msg["content"]
        .as_array()
        .cloned()
        .unwrap_or_else(|| vec![serde_json::json!({"text":msg["content"]})]);
    for (index, part) in parts.iter().enumerate() {
        let uid = format!("{mid}:{index}");
        if let Some(text) = part["text"].as_str() {
            insert_session_event(
                conn,
                &EventRow {
                    event_uid: &uid,
                    kind: "text",
                    text: Some(text),
                    ..base
                },
            )?;
            keys.events.insert(uid);
        } else {
            // Function responses are tool evidence, never user prompts. The
            // assistant's patched call is the authoritative joined result.
            marker(conn, sid, part, &uid, "content_part", keys)?;
        }
    }
    if parts.is_empty() && role == "assistant" {
        let uid = format!("{mid}:0");
        insert_session_event(
            conn,
            &EventRow {
                event_uid: &uid,
                kind: "text",
                ..base
            },
        )?;
        keys.events.insert(uid);
    }
    for (index, thought) in msg["thoughts"].as_array().into_iter().flatten().enumerate() {
        marker(
            conn,
            sid,
            thought,
            &format!("{mid}:thought:{index}"),
            "thought_summary",
            keys,
        )?;
    }
    for (index, call) in msg["toolCalls"]
        .as_array()
        .into_iter()
        .flatten()
        .enumerate()
    {
        tool(conn, &base, call, index, keys)?;
    }
    if role != "user" {
        return Ok(0);
    }
    let text = format::text(&msg["content"]);
    if text.is_empty() {
        return Ok(0);
    }
    // Upstream provides a recorded user role but no synthetic/human bit.
    // Do not guess from summary markup or remove/order mutations.
    let hash = crate::prompt_hash(&text);
    keys.prompts.insert(format!("{}:{hash}", base.ts_ms));
    insert_history(
        conn,
        &HistoryEntry {
            id: 0,
            source: ID.into(),
            session_id: Some(sid.into()),
            project: None,
            prompt: text,
            prompt_hash: Some(hash),
            timestamp_ms: base.ts_ms,
        },
    )
}
fn tool(
    conn: &Connection,
    base: &EventRow<'_>,
    call: &Value,
    index: usize,
    keys: &mut Keys,
) -> Result<()> {
    let id = format::string(call, "id")?;
    let args = serde_json::to_string(&call["args"])?;
    let ts = format::date(call, "timestamp")?;
    insert_tool_call(
        conn,
        &ToolCallRef {
            source: ID,
            session_id: base.session_id,
            message_id: base.message_id,
            tool_use_id: id,
            ts_ms: ts,
            ..Default::default()
        },
        format::string(call, "name")?,
        None,
        &args,
        None,
    )?;
    keys.tools.insert(id.into());
    let text = serde_json::to_string(call)?;
    let uid = format!("{}:call:{index}", base.message_id);
    let facts = ToolResultFacts {
        tool_use_id: Some(id.into()),
        ..Default::default()
    };
    insert_session_event(
        conn,
        &EventRow {
            event_uid: &uid,
            ts_ms: ts,
            kind: "tool_use",
            text: Some(&text),
            tool_result_facts: Some(&facts),
            ..*base
        },
    )?;
    keys.events.insert(uid);
    let Some(result) = call.get("result") else {
        return Ok(());
    };
    if result.is_null() {
        return Ok(());
    }
    let text = serde_json::to_string(result)?;
    let mut facts = ToolResultFacts::from_payload(result);
    facts.tool_use_id = Some(id.into());
    // Only the captured scheduler transition is admitted. Unknown spellings
    // remain unknown, with their raw status preserved in the tool-use payload.
    if call["status"] == "success" {
        facts.result_status = Some("completed".into());
        set_tool_call_error(conn, ID, base.session_id, id, false)?;
        keys.results.insert(id.into());
    }
    let uid = format!("{}:result:{index}", base.message_id);
    insert_session_event(
        conn,
        &EventRow {
            event_uid: &uid,
            ts_ms: ts,
            kind: "tool_result",
            role: "tool_result",
            text: Some(&text),
            token_json: None,
            tool_result_facts: Some(&facts),
            ..*base
        },
    )?;
    keys.events.insert(uid);
    Ok(())
}

fn finalize(
    conn: &Connection,
    sid: &str,
    conversation: &Value,
    messages: &[Value],
    complete: bool,
) -> Result<()> {
    if complete {
        crate::ingest::set_session_title(conn, ID, sid, conversation["summary"].as_str())?;
        let prompt = messages
            .iter()
            .find(|m| m["type"] == "user" && !format::text(&m["content"]).is_empty())
            .map(|m| format::text(&m["content"]));
        conn.execute("UPDATE sessions SET first_prompt=?2 WHERE source='gemini' AND session_id=?1 AND first_prompt IS NOT ?2", rusqlite::params![sid,prompt])?;
    }
    Ok(())
}
