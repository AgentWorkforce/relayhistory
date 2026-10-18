use super::*;
use crate::ingest::{
    insert_session_event, insert_tool_call, set_tool_call_error, upsert_session_rebuilt, EventRow,
    SessionCatalogRow, ToolCallRef, ToolResultFacts,
};
use crate::providers::file_history::{self, Keys};
use crate::{HistoryEntry, NewSessionMarker};

pub(super) fn write(conn: &Connection, payload: &Value, path: &Path) -> Result<usize> {
    let entries = payload["entries"]
        .as_array()
        .context("Buzz entries missing")?;
    let (sid, first, cwd) = header(entries)?;
    file_history::check_identity(conn, ID, "BUZZ", sid, path)?;
    let mut keys = Keys::default();
    let mut inserted = 0;
    let mut last = first;
    let mut assistant = None;
    for (index, entry) in entries.iter().enumerate().skip(1) {
        crate::ingest::check_capture_cancelled()?;
        last = last.max(
            entry["timestamp_ms"]
                .as_i64()
                .context("BUZZ_INVALID_TIMESTAMP")?,
        );
        let (count, text) = record(conn, sid, cwd, index, entry, &mut keys)?;
        inserted += count;
        assistant = text.or(assistant);
    }
    let complete = payload["tail"].as_array().is_some_and(Vec::is_empty);
    if complete {
        file_history::retire(conn, ID, sid, &keys)?;
    }
    let upsert = if complete {
        upsert_session_rebuilt
    } else {
        crate::ingest::upsert_session
    };
    upsert(
        conn,
        &SessionCatalogRow {
            source: ID,
            session_id: sid,
            cwd,
            git_branch: None,
            first_ts: first,
            last_ts: last,
            last_assistant_text: assistant.as_deref(),
            raw_path: Some(&path.to_string_lossy()),
        },
    )?;
    if complete {
        let prompt = first_prompt(entries);
        conn.execute("UPDATE sessions SET first_prompt=?2 WHERE source='buzz' AND session_id=?1 AND first_prompt IS NOT ?2", rusqlite::params![sid,prompt])?;
    }
    Ok(inserted)
}

struct Row<'a> {
    conn: &'a Connection,
    sid: &'a str,
    cwd: Option<&'a str>,
    ts: i64,
    mid: &'a str,
    keys: &'a mut Keys,
}

fn record(
    conn: &Connection,
    sid: &str,
    cwd: Option<&str>,
    index: usize,
    entry: &Value,
    keys: &mut Keys,
) -> Result<(usize, Option<String>)> {
    let ts = entry["timestamp_ms"]
        .as_i64()
        .context("BUZZ_INVALID_TIMESTAMP")?;
    let frame = &entry["frame"];
    let mid = format!("frame-{index}");
    let mut row = Row {
        conn,
        sid,
        cwd,
        ts,
        mid: &mid,
        keys,
    };
    if let Some(text) = prompt(entry) {
        let hash = crate::prompt_hash(&text);
        row.keys.prompts.insert(format!("{ts}:{hash}"));
        let count = crate::insert_history(
            conn,
            &HistoryEntry {
                id: 0,
                source: ID.into(),
                session_id: Some(sid.into()),
                project: cwd.map(str::to_owned),
                prompt: text.clone(),
                prompt_hash: Some(hash),
                timestamp_ms: ts,
            },
        )?;
        event(&mut row, "user", "text", Some(&text))?;
        return Ok((count, None));
    }
    if frame["method"] == "_goose/unstable/session/update"
        && frame["params"]["update"]["sessionUpdate"] == "usage_update"
    {
        marker(&mut row, "usage_snapshot", None, &frame["params"]["update"])?;
        return Ok((0, None));
    }
    if frame["method"] != "session/update" {
        marker(&mut row, "acp_control", frame["method"].as_str(), frame)?;
        return Ok((0, None));
    }
    Ok((0, update(&mut row, &frame["params"]["update"])?))
}

fn update(row: &mut Row<'_>, update: &Value) -> Result<Option<String>> {
    match update["sessionUpdate"].as_str() {
        Some("agent_message_chunk") | Some("agent_thought_chunk") => {
            let text = update["content"]["text"]
                .as_str()
                .context("BUZZ_INVALID_MESSAGE_CHUNK")?;
            let thought = update["sessionUpdate"] == "agent_thought_chunk";
            let message = update["messageId"].as_str().unwrap_or(row.mid);
            let uid = format!("{}:{}", row.mid, if thought { "thinking" } else { "text" });
            event_with_uid(
                row,
                message,
                &uid,
                "assistant",
                if thought { "thinking" } else { "text" },
                Some(text),
            )?;
            Ok((!thought).then(|| text.to_owned()))
        }
        Some("tool_call") => {
            tool_call(row, update)?;
            Ok(None)
        }
        Some("tool_call_update")
            if update["status"] == "completed" || update["status"] == "failed" =>
        {
            tool_result(row, update)?;
            Ok(None)
        }
        _ => {
            marker(row, "acp_update", update["sessionUpdate"].as_str(), update)?;
            Ok(None)
        }
    }
}

fn tool_call(row: &mut Row<'_>, update: &Value) -> Result<()> {
    let id = update["toolCallId"]
        .as_str()
        .context("BUZZ_INVALID_TOOL_CALL")?;
    let name = update["title"].as_str().context("BUZZ_INVALID_TOOL_CALL")?;
    let args = serde_json::to_string(&update["rawInput"])?;
    insert_tool_call(
        row.conn,
        &ToolCallRef {
            source: ID,
            session_id: row.sid,
            message_id: row.mid,
            tool_use_id: id,
            ts_ms: row.ts,
            cwd: row.cwd,
            ..Default::default()
        },
        name,
        update["rawInput"]["path"].as_str(),
        &args,
        None,
    )?;
    row.keys.tools.insert(id.into());
    event(row, "assistant", "tool_use", Some(&args))
}

fn tool_result(row: &mut Row<'_>, update: &Value) -> Result<()> {
    let id = update["toolCallId"]
        .as_str()
        .context("BUZZ_INVALID_TOOL_RESULT")?;
    let failed = update["status"] == "failed" || update["rawOutput"]["isError"] == true;
    set_tool_call_error(row.conn, ID, row.sid, id, failed)?;
    row.keys.results.insert(id.into());
    let text = update["content"]
        .as_array()
        .and_then(|v| v.first())
        .and_then(|v| v["content"]["text"].as_str())
        .or_else(|| update["rawOutput"]["error"].as_str());
    let facts = ToolResultFacts {
        tool_use_id: Some(id.into()),
        result_status: Some(if failed { "errored" } else { "completed" }.into()),
        error_signal: failed.then(|| "is_error".into()),
        ..Default::default()
    };
    insert_session_event(
        row.conn,
        &EventRow {
            source: ID,
            session_id: row.sid,
            message_id: row.mid,
            event_uid: &format!("{}:0", row.mid),
            ts_ms: row.ts,
            cwd: row.cwd,
            project: row.cwd,
            role: "tool_result",
            kind: "tool_result",
            text,
            tool_result_facts: Some(&facts),
            ..Default::default()
        },
    )?;
    row.keys.events.insert(format!("{}:0", row.mid));
    Ok(())
}

fn marker(row: &mut Row<'_>, kind: &str, subkind: Option<&str>, payload: &Value) -> Result<()> {
    let raw = serde_json::to_string(&crate::ingest::bound_marker_value(payload.clone()))?;
    crate::insert_session_marker(
        row.conn,
        ID,
        row.sid,
        &NewSessionMarker {
            marker_uid: row.mid,
            kind,
            subkind,
            ts_ms: Some(row.ts),
            payload_json: Some(&raw),
            ..Default::default()
        },
    )?;
    row.keys.markers.insert(row.mid.into());
    Ok(())
}

fn event(row: &mut Row<'_>, role: &str, kind: &str, text: Option<&str>) -> Result<()> {
    let uid = format!("{}:0", row.mid);
    event_with_uid(row, row.mid, &uid, role, kind, text)
}
fn event_with_uid(
    row: &mut Row<'_>,
    mid: &str,
    uid: &str,
    role: &str,
    kind: &str,
    text: Option<&str>,
) -> Result<()> {
    insert_session_event(
        row.conn,
        &EventRow {
            source: ID,
            session_id: row.sid,
            message_id: mid,
            event_uid: uid,
            ts_ms: row.ts,
            cwd: row.cwd,
            project: row.cwd,
            role,
            kind,
            text,
            ..Default::default()
        },
    )?;
    row.keys.events.insert(uid.into());
    Ok(())
}
