use super::*;
use crate::ingest::{
    insert_session_event, insert_tool_call, upsert_session_rebuilt, EventRow, SessionCatalogRow,
    ToolCallRef, ToolResultFacts,
};
use crate::providers::file_history::{self, Keys};
use crate::{insert_history, insert_session_marker, HistoryEntry, NewSessionMarker};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn check_identity(conn: &Connection, sid: &str, path: &Path) -> Result<()> {
    file_history::check_identity(conn, ID, "JUNIE", sid, path)
}

fn journal_rows(payload: &Value) -> Result<&[Value]> {
    let rows = payload["rows"]
        .as_array()
        .context("JUNIE_INVALID_JOURNAL: rows")?;
    anyhow::ensure!(!rows.is_empty(), "JUNIE_EMPTY_JOURNAL: no event rows");
    Ok(rows)
}

fn captured_identity(payload: &Value) -> Result<(&str, &Path)> {
    let sid = format::string(payload, "sid")?;
    let path = Path::new(format::string(payload, "path")?);
    Ok((sid, path))
}

pub(super) fn write(conn: &Connection, payload: &Value) -> Result<usize> {
    let (sid, path) = captured_identity(payload)?;
    check_identity(conn, sid, path)?;
    if payload["partial"] == true {
        return Ok(0);
    }
    let rows = journal_rows(payload)?;
    let summary = &payload["summary"];
    let shared = crate::providers::shared_evidence::Snapshot::capture(conn, ID, sid)?;
    let cwd_value = project::project_for_capture(conn, sid, path, summary)?;
    let cwd = cwd_value.as_deref();
    let live = last_block_updates(rows);
    let mut keys = Keys::default();
    let mut desired = Vec::new();
    let mut prompts = 0;
    let mut preview = None;
    let mut assistant = None;
    let mut first = i64::MAX;
    let mut last = i64::MIN;
    for (index, row) in rows.iter().enumerate() {
        crate::ingest::check_capture_cancelled()?;
        let ts = format::time(row)?;
        first = first.min(ts);
        last = last.max(ts);
        marker(conn, sid, row, index, ts, &mut keys)?;
        if !live.contains(&index) {
            continue;
        }
        let emitted = emit(conn, sid, cwd, row, index, &mut keys, &mut desired)?;
        prompts += emitted.prompts;
        if preview.is_none() {
            preview = emitted.preview;
        }
        if emitted.assistant.is_some() {
            assistant = emitted.assistant;
        }
    }
    file_history::retire(conn, ID, sid, &keys)?;
    file_history::retire_locator_sessions(conn, ID, path, Some(sid))?;
    replay_reordered(conn, sid, cwd, rows, &live, &desired)?;
    upsert_session_rebuilt(
        conn,
        &SessionCatalogRow {
            source: ID,
            session_id: sid,
            cwd,
            git_branch: None,
            first_ts: summary["createdAt"].as_i64().unwrap_or(first),
            last_ts: summary["updatedAt"].as_i64().unwrap_or(last),
            last_assistant_text: assistant.as_deref(),
            raw_path: Some(&path.to_string_lossy()),
        },
    )?;
    conn.execute("UPDATE sessions SET first_prompt=?3 WHERE source=?1 AND session_id=?2 AND first_prompt IS NOT ?3",
        rusqlite::params![ID,sid,preview])?;
    // The optional index may be absent or too large to read. Only a native
    // nonblank taskName is authority to replace a cached title.
    let title = crate::ingest::titles::non_blank_title(summary["taskName"].as_str());
    crate::ingest::titles::set_session_title(conn, ID, sid, title.as_deref())?;
    shared.ensure_unchanged(conn, sid)?;
    Ok(prompts)
}

fn marker(
    conn: &Connection,
    sid: &str,
    row: &Value,
    index: usize,
    ts: i64,
    keys: &mut Keys,
) -> Result<()> {
    let uid = format!("journal:{index:020}");
    let subkind = row["event"]["agentEvent"]["kind"].as_str();
    let raw = serde_json::to_string(&crate::ingest::bound_marker_value(row.clone()))?;
    insert_session_marker(
        conn,
        ID,
        sid,
        &NewSessionMarker {
            marker_uid: &uid,
            kind: if row["kind"] == "SessionA2uxEvent" {
                "a2ux"
            } else {
                "provider_event"
            },
            subkind: subkind.or_else(|| row["kind"].as_str()),
            ts_ms: Some(ts),
            payload_json: Some(&raw),
            ..Default::default()
        },
    )?;
    keys.markers.insert(uid);
    Ok(())
}

fn replay_reordered(
    conn: &Connection,
    sid: &str,
    cwd: Option<&str>,
    rows: &[Value],
    live: &BTreeSet<usize>,
    desired: &[(i64, String)],
) -> Result<()> {
    let reorder = file_history::reconcile_event_order(conn, ID, sid, desired)?;
    let mut replay_keys = Keys::default();
    let mut replay_order = Vec::new();
    for (index, row) in rows.iter().enumerate() {
        if live.contains(&index) && reorder.contains(&format::time(row)?) {
            emit(
                conn,
                sid,
                cwd,
                row,
                index,
                &mut replay_keys,
                &mut replay_order,
            )?;
        }
    }
    Ok(())
}

fn last_block_updates(rows: &[Value]) -> BTreeSet<usize> {
    let mut latest = BTreeMap::<(String, String, String), usize>::new();
    for (index, row) in rows.iter().enumerate() {
        if row["kind"] != "SessionA2uxEvent" {
            continue;
        }
        let agent = &row["event"]["agentEvent"];
        let Some(step) = agent["stepId"].as_str().filter(|id| !id.is_empty()) else {
            continue;
        };
        let Some(kind) = agent["kind"].as_str() else {
            continue;
        };
        if matches!(
            kind,
            "MarkdownBlockUpdatedEvent" | "ResultBlockUpdatedEvent" | "ToolBlockUpdatedEvent"
        ) {
            latest.insert(
                (
                    row["taskId"].as_str().unwrap_or("").into(),
                    step.into(),
                    kind.into(),
                ),
                index,
            );
        }
    }
    let selected: BTreeSet<_> = latest.into_values().collect();
    (0..rows.len())
        .filter(|index| {
            let row = &rows[*index];
            let agent = &row["event"]["agentEvent"];
            agent["stepId"].as_str().is_none_or(str::is_empty)
                || !matches!(
                    agent["kind"].as_str(),
                    Some(
                        "MarkdownBlockUpdatedEvent"
                            | "ResultBlockUpdatedEvent"
                            | "ToolBlockUpdatedEvent"
                    )
                )
                || selected.contains(index)
        })
        .collect()
}

#[derive(Default)]
struct Emitted {
    prompts: usize,
    preview: Option<String>,
    assistant: Option<String>,
}

struct EmitWrites<'a> {
    keys: &'a mut Keys,
    desired: &'a mut Vec<(i64, String)>,
}

fn emit(
    conn: &Connection,
    sid: &str,
    cwd: Option<&str>,
    row: &Value,
    index: usize,
    keys: &mut Keys,
    desired: &mut Vec<(i64, String)>,
) -> Result<Emitted> {
    let mut result = Emitted::default();
    let ts = format::time(row)?;
    if let Some(prompt) = format::human_text(row) {
        return emit_user(
            conn,
            sid,
            cwd,
            row,
            index,
            prompt,
            EmitWrites { keys, desired },
        );
    }
    if row["kind"] != "SessionA2uxEvent" {
        return Ok(result);
    }
    let agent = &row["event"]["agentEvent"];
    let kind = format::string(agent, "kind")?;
    let fallback = format!("journal:{index:020}");
    let mid = agent["stepId"]
        .as_str()
        .filter(|id| !id.is_empty())
        .unwrap_or(&fallback);
    match kind {
        "MarkdownBlockUpdatedEvent" | "ResultBlockUpdatedEvent" => {
            let text = if kind == "ResultBlockUpdatedEvent" {
                format::string(agent, "result")?
            } else {
                format::string(agent, "text")?
            };
            if !text.is_empty() {
                let uid = format!("journal:{index:020}:text");
                insert_session_event(
                    conn,
                    &EventRow {
                        source: ID,
                        session_id: sid,
                        message_id: mid,
                        ts_ms: ts,
                        role: "assistant",
                        kind: "text",
                        text: Some(text),
                        event_uid: &uid,
                        project: cwd,
                        cwd,
                        raw_kind: Some(kind),
                        ..Default::default()
                    },
                )?;
                keys.events.insert(uid.clone());
                desired.push((ts, uid));
                result.assistant = Some(text.into());
            }
        }
        "ToolBlockUpdatedEvent" => {
            emit_tool(
                conn,
                sid,
                cwd,
                agent,
                index,
                ts,
                EmitWrites { keys, desired },
            )?;
        }
        _ => {}
    }
    Ok(result)
}

fn emit_user(
    conn: &Connection,
    sid: &str,
    cwd: Option<&str>,
    row: &Value,
    index: usize,
    prompt: &str,
    writes: EmitWrites<'_>,
) -> Result<Emitted> {
    let ts = format::time(row)?;
    let fallback = format!("journal:{index:020}");
    let mid = row["requestId"]
        .as_str()
        .filter(|id| !id.is_empty())
        .unwrap_or(&fallback);
    let uid = format!("journal:{index:020}:text");
    insert_session_event(
        conn,
        &EventRow {
            source: ID,
            session_id: sid,
            message_id: mid,
            ts_ms: ts,
            role: "user",
            kind: "text",
            text: Some(prompt),
            event_uid: &uid,
            project: cwd,
            cwd,
            raw_kind: Some("UserPromptEvent"),
            ..Default::default()
        },
    )?;
    writes.keys.events.insert(uid.clone());
    writes.desired.push((ts, uid));
    let hash = crate::prompt_hash(prompt);
    writes.keys.prompts.insert(format!("{ts}:{hash}"));
    let prompts = insert_history(
        conn,
        &HistoryEntry {
            event_uid: None,
            id: 0,
            source: ID.into(),
            session_id: Some(sid.into()),
            project: cwd.map(str::to_owned),
            prompt: prompt.into(),
            prompt_hash: Some(hash),
            timestamp_ms: ts,
        },
    )?;
    Ok(Emitted {
        prompts,
        preview: Some(prompt.chars().take(4096).collect()),
        assistant: None,
    })
}

fn emit_tool(
    conn: &Connection,
    sid: &str,
    cwd: Option<&str>,
    agent: &Value,
    index: usize,
    ts: i64,
    writes: EmitWrites<'_>,
) -> Result<()> {
    let Some(native_id) = agent["stepId"].as_str().filter(|id| !id.is_empty()) else {
        return Ok(());
    };
    let text = format::string(agent, "text")?;
    let name = agent["toolType"].as_str().unwrap_or("Junie tool");
    let status = agent["status"].as_str();
    let is_error = match status {
        Some("COMPLETED") => Some(false),
        Some("FAILED" | "REJECTED" | "CANCELED") => Some(true),
        _ => None,
    };
    insert_tool_call(
        conn,
        &ToolCallRef {
            source: ID,
            session_id: sid,
            message_id: native_id,
            tool_use_id: native_id,
            ts_ms: ts,
            cwd,
            ..Default::default()
        },
        name,
        None,
        "{}",
        is_error,
    )?;
    writes.keys.tools.insert(native_id.into());
    if is_error.is_some() {
        writes.keys.results.insert(native_id.into());
    }
    let facts = ToolResultFacts {
        tool_use_id: Some(native_id.into()),
        ..Default::default()
    };
    let uid = format!("journal:{index:020}:0-tool");
    insert_session_event(
        conn,
        &EventRow {
            source: ID,
            session_id: sid,
            message_id: native_id,
            ts_ms: ts,
            role: "assistant",
            kind: "tool_use",
            text: Some(text),
            event_uid: &uid,
            tool_result_facts: Some(&facts),
            project: cwd,
            cwd,
            raw_kind: Some("ToolBlockUpdatedEvent"),
            ..Default::default()
        },
    )?;
    writes.keys.events.insert(uid.clone());
    writes.desired.push((ts, uid));
    if let Some(output) = agent["output"].as_str() {
        let mut facts = ToolResultFacts::from_payload(&agent["output"]);
        facts.tool_use_id = Some(native_id.into());
        facts.event_source = Some("function_call_output".into());
        facts.result_status = Some(
            match status {
                Some("COMPLETED") => "completed",
                Some("FAILED" | "REJECTED") => "errored",
                Some("CANCELED") => "cancelled",
                Some("IN_PROGRESS") => "running",
                _ => "unknown",
            }
            .into(),
        );
        if is_error == Some(true) {
            facts.error_signal = Some("tool_status".into());
        }
        let uid = format!("journal:{index:020}:1-output");
        insert_session_event(
            conn,
            &EventRow {
                source: ID,
                session_id: sid,
                message_id: native_id,
                ts_ms: ts,
                role: "tool_result",
                kind: "tool_result",
                text: Some(output),
                event_uid: &uid,
                tool_result_facts: Some(&facts),
                project: cwd,
                cwd,
                raw_kind: Some("ToolBlockUpdatedEvent"),
                ..Default::default()
            },
        )?;
        writes.keys.events.insert(uid.clone());
        writes.desired.push((ts, uid));
    }
    Ok(())
}
