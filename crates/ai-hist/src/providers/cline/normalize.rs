use super::*;
use crate::ingest::{
    insert_session_event, insert_tool_call, set_tool_call_error, upsert_session_rebuilt, EventRow,
    SessionCatalogRow, ToolCallRef, ToolResultFacts,
};
use crate::providers::file_history::{self, Keys};
use crate::providers::shared_evidence::Snapshot;
use crate::{insert_history, insert_session_marker, HistoryEntry, NewSessionMarker};

pub(super) fn check_identity(conn: &Connection, sid: &str, path: &Path) -> Result<()> {
    file_history::check_identity(conn, ID, "CLINE", sid, path)
}

pub(super) fn write(conn: &Connection, payload: &Value, path: &Path) -> Result<usize> {
    let sid = format::string(payload, "sessionId")?;
    let messages = payload["messages"]
        .as_array()
        .context("CLINE_INVALID_RECORD: messages")?;
    let updated = format::time(payload)?;
    let times = format::message_times(messages, updated);
    check_identity(conn, sid, path)?;
    let shared = Snapshot::capture(conn, ID, sid)?;
    let (inserted, first, last, last_assistant, mut keys) =
        write_rows(conn, sid, messages, &times, updated)?;
    file_history::retire_with_prompts(conn, ID, sid, &keys, prompts::reconcile)?;
    let order = ordered_events(messages, &times)?;
    let replay = file_history::reconcile_event_order(conn, ID, sid, &order)?;
    if !replay.is_empty() {
        for (index, message) in messages.iter().enumerate() {
            let ts = times[index].0;
            if replay.contains(&ts) {
                write_message(conn, sid, message, ts, &mut keys, true)?;
            }
        }
    }
    upsert_session_rebuilt(
        conn,
        &SessionCatalogRow {
            source: ID,
            session_id: sid,
            cwd: None,
            git_branch: None,
            first_ts: first,
            last_ts: last,
            last_assistant_text: last_assistant.as_deref(),
            raw_path: Some(&path.to_string_lossy()),
        },
    )?;
    let prompt = format::first_prompt(messages);
    conn.execute("UPDATE sessions SET first_prompt=?2 WHERE source='cline' AND session_id=?1 AND first_prompt IS NOT ?2", rusqlite::params![sid,prompt])?;
    shared.ensure_unchanged(conn, sid)?;
    file_history::retire_locator_sessions(conn, ID, path, Some(sid))?;
    Ok(inserted)
}

fn write_rows(
    conn: &Connection,
    sid: &str,
    messages: &[Value],
    times: &[(i64, &'static str)],
    updated: i64,
) -> Result<(usize, i64, i64, Option<String>, Keys)> {
    let mut keys = Keys::default();
    let mut inserted = 0;
    let mut last_assistant = None;
    let mut first = updated;
    let mut last = updated;
    for (index, message) in messages.iter().enumerate() {
        crate::ingest::check_capture_cancelled()?;
        let (ts, basis) = times[index];
        if index == 0 {
            first = ts;
        }
        last = last.max(ts);
        inserted += write_message(conn, sid, message, ts, &mut keys, false)?;
        if basis != "native_ts" {
            provenance(conn, sid, message, ts, basis, &mut keys)?;
        }
        if message["role"] == "assistant" {
            let text = format::text(&message["content"]);
            if !text.trim().is_empty() {
                last_assistant = Some(format::bounded_visible_text(&text));
            }
        }
    }
    Ok((inserted, first, last, last_assistant, keys))
}

fn ordered_events(messages: &[Value], times: &[(i64, &'static str)]) -> Result<Vec<(i64, String)>> {
    let mut out = Vec::new();
    for (index, message) in messages.iter().enumerate() {
        let ts = times[index].0;
        let id = format::string(message, "id")?;
        for (block_index, block) in message["content"]
            .as_array()
            .into_iter()
            .flatten()
            .enumerate()
        {
            if matches!(
                block["type"].as_str(),
                Some("text" | "thinking" | "tool_use" | "tool_result")
            ) {
                out.push((ts, format!("{id}:{block_index}")));
            }
        }
    }
    Ok(out)
}
fn write_message(
    conn: &Connection,
    sid: &str,
    message: &Value,
    ts: i64,
    keys: &mut Keys,
    replay: bool,
) -> Result<usize> {
    let mid = format::string(message, "id")?;
    let role = format::string(message, "role")?;

    let blocks = message["content"]
        .as_array()
        .context("CLINE_INVALID_RECORD: content")?;
    let base = EventRow {
        source: ID,
        session_id: sid,
        message_id: mid,
        ts_ms: ts,
        role,
        model: message["modelInfo"]["id"].as_str(),
        provider: message["modelInfo"]["provider"].as_str(),
        token_json: None,
        raw_kind: Some(role),
        ..Default::default()
    };
    write_blocks(conn, &base, message, blocks, keys, replay)?;
    if replay || role != "user" {
        return Ok(0);
    }
    let text = format::text(&message["content"]);
    if text.trim().is_empty() {
        return Ok(0);
    }
    let text = format::bounded_visible_text(&text);
    let hash = crate::prompt_hash(&text);
    keys.prompts.insert(format!("{ts}:{hash}"));
    insert_history(
        conn,
        &HistoryEntry {
            id: 0,
            source: ID.into(),
            session_id: Some(sid.into()),
            project: None,
            prompt: text,
            prompt_hash: Some(hash),
            timestamp_ms: ts,
        },
    )
}
fn write_blocks(
    conn: &Connection,
    base: &EventRow<'_>,
    message: &Value,
    blocks: &[Value],
    keys: &mut Keys,
    replay: bool,
) -> Result<()> {
    let mid = base.message_id;
    let sid = base.session_id;
    let ts = base.ts_ms;
    if let Some(metrics) = message.get("metrics").filter(|_| !replay) {
        let payload = serde_json::to_string(&crate::ingest::bound_marker_value(metrics.clone()))?;
        let uid = format!("{mid}:metrics");
        insert_session_marker(
            conn,
            ID,
            sid,
            &NewSessionMarker {
                marker_uid: &uid,
                kind: "usage_snapshot",
                subkind: Some("terminal_metrics"),
                ts_ms: Some(ts),
                message_id: Some(mid),
                payload_json: Some(&payload),
                ..Default::default()
            },
        )?;
        keys.markers.insert(uid);
    }
    for (index, block) in blocks.iter().enumerate() {
        let uid = format!("{mid}:{index}");
        let kind = format::string(block, "type")?;
        match kind {
            "text" | "thinking" | "tool_use" => {
                let text = match kind {
                    "text" => block["text"].as_str().map(format::bounded_visible_text),
                    "thinking" => block["thinking"].as_str().map(format::bounded_visible_text),
                    _ => {
                        if !replay {
                            call(conn, base, block, keys)?;
                        }
                        Some(format::tool_use_text(block)?)
                    }
                };
                let facts = ToolResultFacts {
                    tool_use_id: if kind == "tool_use" {
                        Some(format::string(block, "id")?.into())
                    } else {
                        None
                    },
                    ..Default::default()
                };
                insert_session_event(
                    conn,
                    &EventRow {
                        event_uid: &uid,
                        kind,
                        text: text.as_deref(),
                        tool_result_facts: Some(&facts),
                        ..*base
                    },
                )?;
                keys.events.insert(uid);
            }
            "tool_result" => result(conn, base, block, &uid, keys, replay)?,
            _ if !replay => marker(conn, base, block, &uid, keys)?,
            _ => {}
        }
    }
    Ok(())
}
fn marker(
    conn: &Connection,
    base: &EventRow<'_>,
    block: &Value,
    uid: &str,
    keys: &mut Keys,
) -> Result<()> {
    let payload = serde_json::to_string(&crate::ingest::bound_marker_value(block.clone()))?;
    insert_session_marker(
        conn,
        ID,
        base.session_id,
        &NewSessionMarker {
            marker_uid: uid,
            kind: "unknown",
            subkind: block["type"].as_str(),
            ts_ms: Some(base.ts_ms),
            message_id: Some(base.message_id),
            payload_json: Some(&payload),
            ..Default::default()
        },
    )?;
    keys.markers.insert(uid.into());
    Ok(())
}
fn call(conn: &Connection, base: &EventRow<'_>, block: &Value, keys: &mut Keys) -> Result<()> {
    let id = format::string(block, "id")?;
    let name = format::string(block, "name")?;
    let args = format::tool_input_args(block)?;
    let target = block["input"]["file_path"]
        .as_str()
        .filter(|path| path.len() <= 4096);
    insert_tool_call(
        conn,
        &ToolCallRef {
            source: ID,
            session_id: base.session_id,
            message_id: base.message_id,
            tool_use_id: id,
            ts_ms: base.ts_ms,
            ..Default::default()
        },
        name,
        target,
        &args,
        None,
    )?;
    keys.tools.insert(id.into());
    Ok(())
}
fn result(
    conn: &Connection,
    base: &EventRow<'_>,
    block: &Value,
    uid: &str,
    keys: &mut Keys,
    replay: bool,
) -> Result<()> {
    let id = format::string(block, "tool_use_id")?;
    let mut facts = ToolResultFacts::from_payload(&block["content"]);
    let text = format::result_text(&block["content"], &facts);
    facts.tool_use_id = Some(id.into());
    let error = block["is_error"].as_bool();
    facts.result_status = error.map(|e| if e { "errored" } else { "completed" }.into());
    facts.error_signal = error.filter(|e| *e).map(|_| "is_error".into());
    insert_session_event(
        conn,
        &EventRow {
            event_uid: uid,
            kind: "tool_result",
            role: "tool_result",
            text: Some(&text),
            tool_result_facts: Some(&facts),
            ..*base
        },
    )?;
    if !replay {
        if let Some(error) = error {
            set_tool_call_error(conn, ID, base.session_id, id, error)?;
        } else {
            // Native is_error is optional. Clear only a local call's prior
            // outcome; remote-backed rows remain under Snapshot's guard.
            conn.execute(
                "UPDATE tool_calls SET is_error=NULL WHERE source=?1 AND session_id=?2 AND tool_use_id=?3 AND location='local' AND is_error IS NOT NULL",
                rusqlite::params![ID, base.session_id, id],
            )?;
        }
    }
    keys.results.insert(id.into());
    keys.events.insert(uid.into());
    Ok(())
}

fn provenance(
    conn: &Connection,
    sid: &str,
    message: &Value,
    ts: i64,
    basis: &str,
    keys: &mut Keys,
) -> Result<()> {
    let mid = format::string(message, "id")?;
    let uid = format!("{mid}:timestamp");
    let payload = serde_json::to_string(&serde_json::json!({"basis":basis}))?;
    insert_session_marker(
        conn,
        ID,
        sid,
        &NewSessionMarker {
            marker_uid: &uid,
            kind: "timestamp_provenance",
            ts_ms: Some(ts),
            message_id: Some(mid),
            payload_json: Some(&payload),
            ..Default::default()
        },
    )?;
    keys.markers.insert(uid);
    Ok(())
}
