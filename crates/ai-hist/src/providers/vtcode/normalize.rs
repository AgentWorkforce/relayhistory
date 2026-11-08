use super::*;
use crate::ingest::{
    insert_session_event, insert_tool_call, set_tool_call_error, upsert_session_rebuilt, EventRow,
    SessionCatalogRow, ToolCallRef, ToolResultFacts,
};
use crate::providers::file_history::{self, Keys};
use crate::providers::shared_evidence::Snapshot;
use crate::{insert_history, insert_session_marker, HistoryEntry, NewSessionMarker};
use std::collections::BTreeMap;

mod catalog;
mod tool_output;

fn current_calls(rows: &[Value], native: &str) -> Result<BTreeMap<String, String>> {
    let mut calls = BTreeMap::new();
    for row in rows {
        if row["type"] == "item.completed"
            && row["item"]["type"] == "tool_invocation"
            && row["item"]["context"]["actor_id"] == native
        {
            let item = &row["item"];
            let id = format::string(item, "id")?;
            let tool_id = item["tool_call_id"].as_str().unwrap_or(id);
            anyhow::ensure!(
                calls.insert(id.to_owned(), tool_id.to_owned()).is_none(),
                "VTCODE_INVALID_RECORD: duplicate tool invocation item id"
            );
        }
    }
    Ok(calls)
}

pub(super) fn validate_native(rows: &[Value], native: &str) -> Result<()> {
    for row in rows {
        if row["type"] == "thread.started"
            || row["type"] == "thread.completed"
            || row["type"] == "thread.compact_boundary"
        {
            anyhow::ensure!(
                row["thread_id"].as_str() == Some(native),
                "VTCODE_ID_MISMATCH: journal thread differs from session directory"
            );
        }
        if row["type"] == "thread.completed" {
            anyhow::ensure!(
                row["session_id"].as_str() == Some(native),
                "VTCODE_ID_MISMATCH: terminal session differs from directory"
            );
        }
    }
    Ok(())
}

pub(super) fn check_identity(conn: &Connection, sid: &str, path: &Path) -> Result<()> {
    file_history::check_identity(conn, ID, "VTCODE", sid, path)
}

pub(super) fn write(conn: &Connection, payload: &Value, path: &Path) -> Result<usize> {
    let sid = format::string(payload, "sid")?;
    let native = format::string(payload, "native")?;
    check_identity(conn, sid, path)?;
    let rows = payload["rows"].as_array().context("VT Code rows missing")?;
    let complete = payload["partial"] == false;
    if !complete && has_evidence(conn, sid)? {
        return Ok(0);
    }
    let shared = Snapshot::capture(conn, ID, sid)?;
    let prior_activity = if complete {
        catalog::prior_activity(conn, sid)?
    } else {
        None
    };
    let cwd = path
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .and_then(Path::parent)
        .map(|p| p.to_string_lossy().into_owned());
    let written = write_rows(conn, sid, native, cwd.as_deref(), rows)?;
    if complete {
        file_history::retire(conn, ID, sid, &written.keys)?;
        file_history::retire_locator_sessions(conn, ID, path, Some(sid))?;
        repair_order(conn, sid, native, cwd.as_deref(), rows)?;
    }
    if let Some(first) = written.first {
        let summary = catalog::Summary {
            cwd: cwd.as_deref(),
            first,
            last: written.last.unwrap_or(first),
            last_assistant: written.last_assistant,
            prior: prior_activity.as_ref(),
        };
        catalog::timed(conn, sid, native, path, rows, complete, &summary)?;
    } else {
        catalog::untimed(
            conn,
            sid,
            cwd.as_deref(),
            path,
            complete,
            prior_activity.as_ref(),
        )?;
    }
    shared.ensure_unchanged(conn, sid)?;
    Ok(written.inserted)
}

fn repair_order(
    conn: &Connection,
    sid: &str,
    native: &str,
    cwd: Option<&str>,
    rows: &[Value],
) -> Result<()> {
    let desired = desired_events(rows, native)?;
    let reordered = file_history::reconcile_event_order(conn, ID, sid, &desired)?;
    if reordered.is_empty() {
        return Ok(());
    }
    let usage = usage::turn_usage(rows, native)?;
    let calls = current_calls(rows, native)?;
    let replay = ReplayFacts {
        reordered: &reordered,
        usage: &usage,
        calls: &calls,
    };
    let mut replay_keys = Keys::default();
    for row in rows {
        replay_row(conn, sid, native, cwd, row, &replay, &mut replay_keys)?;
    }
    Ok(())
}

struct ReplayFacts<'a> {
    reordered: &'a std::collections::BTreeSet<i64>,
    usage: &'a BTreeMap<String, String>,
    calls: &'a BTreeMap<String, String>,
}

fn replay_row(
    conn: &Connection,
    sid: &str,
    native: &str,
    cwd: Option<&str>,
    row: &Value,
    replay: &ReplayFacts<'_>,
    keys: &mut Keys,
) -> Result<()> {
    if row["type"] == "turn.started"
        && row["context"]["origin"] == "user"
        && row["context"]["actor_id"] == native
    {
        let ts = format::time(format::string(&row["context"], "timestamp")?)?;
        if replay.reordered.contains(&ts) {
            prompt(conn, sid, cwd, row, keys)?;
        }
    } else if row["type"] == "item.completed" && row["item"]["context"]["actor_id"] == native {
        let item = &row["item"];
        let ts = format::time(format::string(&item["context"], "timestamp")?)?;
        if !replay.reordered.contains(&ts) {
            return Ok(());
        }
        match item["type"].as_str() {
            Some("agent_message") => assistant(
                conn,
                sid,
                cwd,
                item,
                replay.usage.get(format::string(item, "id")?),
                keys,
            )?,
            Some("tool_invocation") => call_event(
                conn,
                sid,
                cwd,
                item,
                replay.usage.get(format::string(item, "id")?),
                keys,
            )?,
            Some("tool_output") => {
                tool_output::result_event(conn, sid, cwd, item, replay.calls, keys)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn desired_events(rows: &[Value], native: &str) -> Result<Vec<(i64, String)>> {
    let mut desired = Vec::new();
    for row in rows {
        if row["type"] == "turn.started"
            && row["context"]["origin"] == "user"
            && row["context"]["actor_id"] == native
        {
            desired.push((
                format::time(format::string(&row["context"], "timestamp")?)?,
                format!("{}:0", format::string(&row["context"], "turn_id")?),
            ));
        } else if row["type"] == "item.completed"
            && row["item"]["context"]["actor_id"] == native
            && matches!(
                row["item"]["type"].as_str(),
                Some("agent_message" | "tool_invocation" | "tool_output")
            )
        {
            desired.push((
                format::time(format::string(&row["item"]["context"], "timestamp")?)?,
                format!("{}:0", format::string(&row["item"], "id")?),
            ));
        }
    }
    Ok(desired)
}

struct WrittenRows<'a> {
    keys: Keys,
    inserted: usize,
    first: Option<i64>,
    last: Option<i64>,
    last_assistant: Option<&'a str>,
}

struct CaptureFacts<'a> {
    usage: &'a BTreeMap<String, String>,
    calls: &'a BTreeMap<String, String>,
}

fn write_item<'a>(
    conn: &Connection,
    sid: &str,
    cwd: Option<&str>,
    row: &'a Value,
    index: usize,
    facts: &CaptureFacts<'_>,
    keys: &mut Keys,
) -> Result<Option<&'a str>> {
    let item = &row["item"];
    match item["type"].as_str() {
        Some("agent_message") => {
            let text = format::text(item, "text")?;
            assistant(
                conn,
                sid,
                cwd,
                item,
                facts.usage.get(format::string(item, "id")?),
                keys,
            )?;
            Ok(Some(text))
        }
        Some("tool_invocation") => {
            call(
                conn,
                sid,
                cwd,
                item,
                facts.usage.get(format::string(item, "id")?),
                keys,
            )?;
            Ok(None)
        }
        Some("tool_output") => {
            tool_output::result(conn, sid, cwd, item, facts.calls, keys)?;
            Ok(None)
        }
        _ => {
            marker(conn, sid, row, index, keys)?;
            Ok(None)
        }
    }
}

fn write_rows<'a>(
    conn: &Connection,
    sid: &str,
    native: &str,
    cwd: Option<&str>,
    rows: &'a [Value],
) -> Result<WrittenRows<'a>> {
    let mut keys = Keys::default();
    let mut inserted = 0;
    let mut last_assistant = None;
    let usage = usage::turn_usage(rows, native)?;
    let calls = current_calls(rows, native)?;
    let facts = CaptureFacts {
        usage: &usage,
        calls: &calls,
    };
    let mut first = None;
    let mut last = None;
    for (index, row) in rows.iter().enumerate() {
        crate::ingest::check_capture_cancelled()?;
        if let Some(ts) = format::event_time(row)? {
            first = Some(first.map_or(ts, |value: i64| value.min(ts)));
            last = Some(last.map_or(ts, |value: i64| value.max(ts)));
        }
        if row["type"] == "turn.started"
            && row["context"]["origin"] == "user"
            && row["context"]["actor_id"] == native
        {
            inserted += prompt(conn, sid, cwd, row, &mut keys)?;
        } else if row["type"] == "item.completed" && row["item"]["context"]["actor_id"] == native {
            if let Some(text) = write_item(conn, sid, cwd, row, index, &facts, &mut keys)? {
                last_assistant = Some(text);
            }
        } else {
            marker(conn, sid, row, index, &mut keys)?;
        }
    }
    Ok(WrittenRows {
        keys,
        inserted,
        first,
        last,
        last_assistant,
    })
}

fn has_evidence(conn: &Connection, sid: &str) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM session_events WHERE source='vtcode' AND session_id=?1)",
        [sid],
        |row| row.get::<_, bool>(0),
    )?)
}

fn prompt(
    conn: &Connection,
    sid: &str,
    cwd: Option<&str>,
    row: &Value,
    keys: &mut Keys,
) -> Result<usize> {
    let context = &row["context"];
    let id = format::string(context, "turn_id")?;
    let text = format::text(context, "goal")?;
    let ts = format::time(format::string(context, "timestamp")?)?;
    let uid = format!("{id}:0");
    insert_session_event(
        conn,
        &EventRow {
            source: ID,
            session_id: sid,
            message_id: id,
            event_uid: &uid,
            role: "user",
            kind: "text",
            text: Some(text),
            ts_ms: ts,
            cwd,
            project: cwd,
            raw_kind: Some("turn.started"),
            ..Default::default()
        },
    )?;
    keys.events.insert(uid);
    let hash = crate::prompt_hash(text);
    keys.prompts.insert(format!("{ts}:{hash}"));
    insert_history(
        conn,
        &HistoryEntry {
            event_uid: None,
            id: 0,
            source: ID.into(),
            session_id: Some(sid.into()),
            project: cwd.map(str::to_owned),
            prompt: text.into(),
            prompt_hash: Some(hash),
            timestamp_ms: ts,
        },
    )
}

fn assistant(
    conn: &Connection,
    sid: &str,
    cwd: Option<&str>,
    item: &Value,
    usage: Option<&String>,
    keys: &mut Keys,
) -> Result<()> {
    let id = format::string(item, "id")?;
    let ts = format::time(format::string(&item["context"], "timestamp")?)?;
    let uid = format!("{id}:0");
    insert_session_event(
        conn,
        &EventRow {
            source: ID,
            session_id: sid,
            message_id: id,
            event_uid: &uid,
            role: "assistant",
            kind: "text",
            text: Some(format::text(item, "text")?),
            ts_ms: ts,
            cwd,
            project: cwd,
            token_json: usage.map(String::as_str),
            raw_kind: Some("agent_message"),
            ..Default::default()
        },
    )?;
    keys.events.insert(uid);
    Ok(())
}

fn call(
    conn: &Connection,
    sid: &str,
    cwd: Option<&str>,
    item: &Value,
    usage: Option<&String>,
    keys: &mut Keys,
) -> Result<()> {
    let id = format::string(item, "id")?;
    let tool_id = item["tool_call_id"].as_str().unwrap_or(id);
    let ts = format::time(format::string(&item["context"], "timestamp")?)?;
    let args = serde_json::to_string(&item["arguments"])?;
    let status = match item["status"].as_str() {
        Some("failed") => Some(true),
        Some("completed") => Some(false),
        _ => None,
    };
    insert_tool_call(
        conn,
        &ToolCallRef {
            source: ID,
            session_id: sid,
            message_id: id,
            tool_use_id: tool_id,
            ts_ms: ts,
            cwd,
            ..Default::default()
        },
        format::string(item, "tool_name")?,
        None,
        &args,
        status,
    )?;
    keys.tools.insert(tool_id.into());
    if status.is_some() {
        keys.results.insert(tool_id.into());
    }
    call_event(conn, sid, cwd, item, usage, keys)
}

// Order repair only replaces session events. The full pass has already
// established the final tool-call status from every native output.
fn call_event(
    conn: &Connection,
    sid: &str,
    cwd: Option<&str>,
    item: &Value,
    usage: Option<&String>,
    keys: &mut Keys,
) -> Result<()> {
    let id = format::string(item, "id")?;
    let tool_id = item["tool_call_id"].as_str().unwrap_or(id);
    let ts = format::time(format::string(&item["context"], "timestamp")?)?;
    let args = serde_json::to_string(&item["arguments"])?;
    let uid = format!("{id}:0");
    let facts = ToolResultFacts {
        tool_use_id: Some(tool_id.into()),
        ..Default::default()
    };
    insert_session_event(
        conn,
        &EventRow {
            source: ID,
            session_id: sid,
            message_id: id,
            event_uid: &uid,
            role: "assistant",
            kind: "tool_use",
            text: Some(&args),
            token_json: usage.map(String::as_str),
            ts_ms: ts,
            cwd,
            project: cwd,
            tool_result_facts: Some(&facts),
            raw_kind: Some("tool_invocation"),
            ..Default::default()
        },
    )?;
    keys.events.insert(uid);
    Ok(())
}

fn marker(conn: &Connection, sid: &str, row: &Value, index: usize, keys: &mut Keys) -> Result<()> {
    let kind = format::string(row, "type")?;
    let id = format!("line:{index}");
    let payload = serde_json::to_string(&crate::ingest::bound_marker_value(row.clone()))?;
    insert_session_marker(
        conn,
        ID,
        sid,
        &NewSessionMarker {
            marker_uid: &id,
            kind: if kind.starts_with("thread.") || kind.starts_with("turn.") {
                kind
            } else {
                "unknown"
            },
            subkind: (!kind.starts_with("thread.") && !kind.starts_with("turn.")).then_some(kind),
            ts_ms: format::event_time(row)?,
            message_id: row["context"]["turn_id"]
                .as_str()
                .or_else(|| row["item"]["id"].as_str()),
            payload_json: Some(&payload),
            ..Default::default()
        },
    )?;
    keys.markers.insert(id);
    Ok(())
}
