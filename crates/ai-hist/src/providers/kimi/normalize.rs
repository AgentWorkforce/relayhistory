use super::*;
use crate::ingest::{
    insert_session_event, insert_tool_call, upsert_session_rebuilt, EventRow, SessionCatalogRow,
    ToolCallRef, ToolResultFacts,
};
use crate::providers::file_history::{self, Keys};
use crate::providers::shared_evidence::Snapshot;
use crate::{insert_history, insert_session_marker, HistoryEntry, NewSessionMarker};
pub(super) fn check_identity(conn: &Connection, sid: &str, path: &Path) -> Result<()> {
    file_history::check_identity(conn, ID, "KIMI", sid, path)
}
pub(super) fn write(conn: &Connection, payload: &Value) -> Result<usize> {
    let sid = format::string(payload, "sid")?;
    let path = Path::new(format::string(payload, "path")?);
    check_identity(conn, sid, path)?;
    let rows = payload["rows"].as_array().context("Kimi rows missing")?;
    let complete = payload["partial"] == false;
    // Native rewrites and branch switches renumber positional evidence keys.
    // Defer the entire replacement until its final record is complete.
    if !complete {
        return Ok(0);
    }
    let shared = Snapshot::capture(conn, ID, sid)?;
    let mut keys = Keys::default();
    let mut outcome = MessageOutcome::default();
    let mut first = i64::MAX;
    let mut last = i64::MIN;
    let cwd = payload["state"]["cwd"].as_str();
    let live = format::active_messages(rows)?;
    let mut message_index = 0;
    for (index, row) in rows.iter().enumerate() {
        crate::ingest::check_capture_cancelled()?;
        let ts = format::time(row)?;
        first = first.min(ts);
        last = last.max(ts);
        let uid = format!("wire:{index}");
        let raw = serde_json::to_string(&crate::ingest::bound_marker_value(row.clone()))?;
        insert_session_marker(
            conn,
            ID,
            sid,
            &NewSessionMarker {
                marker_uid: &uid,
                kind: format::string(row, "type")?,
                ts_ms: Some(ts),
                payload_json: Some(&raw),
                ..Default::default()
            },
        )?;
        keys.markers.insert(uid);
        if row["type"] != "agent.message.appended" {
            continue;
        }
        if !live.contains(&index) {
            continue;
        }
        let result = message(conn, sid, cwd, row, message_index, &mut keys)?;
        message_index += 1;
        outcome.absorb(result);
    }
    if complete {
        file_history::retire(conn, ID, sid, &keys)?;
        file_history::retire_locator_sessions(conn, ID, path, Some(sid))?;
        let reordered = file_history::reconcile_event_order(conn, ID, sid, &outcome.events)?;
        replay_buckets(conn, sid, cwd, rows, &live, &reordered, &mut keys)?;
    }
    finish(conn, sid, payload, first, last, outcome, shared)
}

fn finish(
    conn: &Connection,
    sid: &str,
    payload: &Value,
    first: i64,
    last: i64,
    outcome: MessageOutcome,
    shared: Snapshot,
) -> Result<usize> {
    catalog(
        conn,
        payload,
        first,
        last,
        outcome.assistant.as_deref(),
        outcome.preview,
    )?;
    shared.ensure_unchanged(conn, sid)?;
    Ok(outcome.inserted)
}

fn replay_buckets(
    conn: &Connection,
    sid: &str,
    cwd: Option<&str>,
    rows: &[Value],
    live: &std::collections::BTreeSet<usize>,
    reordered: &std::collections::BTreeSet<i64>,
    keys: &mut Keys,
) -> Result<()> {
    if reordered.is_empty() {
        return Ok(());
    }
    let mut message_index = 0;
    for (index, row) in rows.iter().enumerate() {
        if row["type"] != "agent.message.appended" || !live.contains(&index) {
            continue;
        }
        if reordered.contains(&format::time(row)?) {
            message(conn, sid, cwd, row, message_index, keys)?;
        }
        message_index += 1;
    }
    Ok(())
}

#[derive(Default)]
struct MessageOutcome {
    inserted: usize,
    events: Vec<(i64, String)>,
    preview: Option<String>,
    assistant: Option<String>,
}
impl MessageOutcome {
    fn absorb(&mut self, next: Self) {
        self.inserted += next.inserted;
        self.events.extend(next.events);
        if self.preview.is_none() {
            self.preview = next.preview;
        }
        if next.assistant.is_some() {
            self.assistant = next.assistant;
        }
    }
}

fn message(
    conn: &Connection,
    sid: &str,
    cwd: Option<&str>,
    row: &Value,
    index: usize,
    keys: &mut Keys,
) -> Result<MessageOutcome> {
    let mut result = MessageOutcome::default();
    let ts = format::time(row)?;
    let event = &row["message"];
    let msg = &event["message"];
    let meta = &event["meta"];
    let role = format::string(msg, "role")?;
    if role == "user" && !format::human_origin(&meta["origin"]) {
        return Ok(result);
    }
    let generated = format!("message:{index}");
    let mid = meta["userMessageId"]
        .as_str()
        .or_else(|| meta["messageId"].as_str())
        .unwrap_or(&generated);
    let base = EventRow {
        source: ID,
        session_id: sid,
        message_id: mid,
        ts_ms: ts,
        role: if role == "tool" { "tool_result" } else { role },
        project: cwd,
        cwd,
        raw_kind: Some("agent.message.appended"),
        ..Default::default()
    };
    let text = format::text(&msg["content"]);
    if role == "tool" || !text.is_empty() {
        let eid = format!("{generated}:text");
        let mut facts = ToolResultFacts::from_payload(&msg["content"]);
        facts.tool_use_id = msg["toolCallId"].as_str().map(str::to_owned);
        insert_session_event(
            conn,
            &EventRow {
                event_uid: &eid,
                kind: if role == "tool" {
                    "tool_result"
                } else {
                    "text"
                },
                text: Some(&text),
                tool_result_facts: (role == "tool").then_some(&facts),
                ..base
            },
        )?;
        keys.events.insert(eid.clone());
        result.events.push((ts, eid));
    }
    for (n, call) in msg["toolCalls"]
        .as_array()
        .into_iter()
        .flatten()
        .enumerate()
    {
        let id = format::string(call, "id")?;
        let name = format::string(call, "name")?;
        let args = call["arguments"]
            .as_str()
            .context("KIMI_INVALID_RECORD: tool arguments")?;
        insert_tool_call(
            conn,
            &ToolCallRef {
                source: ID,
                session_id: sid,
                message_id: mid,
                tool_use_id: id,
                ts_ms: ts,
                cwd,
                ..Default::default()
            },
            name,
            None,
            args,
            None,
        )?;
        keys.tools.insert(id.into());
        let eid = format!("{generated}:tool:{n}");
        let facts = ToolResultFacts {
            tool_use_id: Some(id.into()),
            ..Default::default()
        };
        insert_session_event(
            conn,
            &EventRow {
                event_uid: &eid,
                kind: "tool_use",
                text: Some(args),
                tool_result_facts: Some(&facts),
                ..base
            },
        )?;
        keys.events.insert(eid.clone());
        result.events.push((ts, eid));
    }
    if let Some(prompt) = format::human_text(row) {
        result.preview = Some(prompt.chars().take(4096).collect());
        let hash = crate::prompt_hash(&prompt);
        keys.prompts.insert(format!("{ts}:{hash}"));
        result.inserted += insert_history(
            conn,
            &HistoryEntry {
                event_uid: None,
                id: 0,
                source: ID.into(),
                session_id: Some(sid.into()),
                project: cwd.map(str::to_owned),
                prompt,
                prompt_hash: Some(hash),
                timestamp_ms: ts,
            },
        )?;
    }
    if role == "assistant" && !text.is_empty() {
        result.assistant = Some(text);
    }
    Ok(result)
}

fn catalog(
    conn: &Connection,
    payload: &Value,
    first: i64,
    last: i64,
    assistant: Option<&str>,
    preview: Option<String>,
) -> Result<()> {
    let sid = format::string(payload, "sid")?;
    let path = Path::new(format::string(payload, "path")?);
    let cwd = payload["state"]["cwd"].as_str();
    let complete = payload["partial"] == false;
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
            cwd,
            git_branch: None,
            first_ts: first,
            last_ts: last,
            last_assistant_text: assistant,
            raw_path: Some(&path.to_string_lossy()),
        },
    )?;
    if complete {
        conn.execute("UPDATE sessions SET first_prompt=?3,title=?4 WHERE source=?1 AND session_id=?2 AND (first_prompt IS NOT ?3 OR title IS NOT ?4)",rusqlite::params![ID,sid,preview,payload["state"]["title"].as_str()])?;
    } else if let Some(preview) = preview {
        conn.execute("UPDATE sessions SET first_prompt=?3 WHERE source=?1 AND session_id=?2 AND first_prompt IS NULL",rusqlite::params![ID,sid,preview])?;
    }

    Ok(())
}
