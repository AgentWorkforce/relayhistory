use super::projections;
use super::*;
use crate::ingest::tool_result_facts::{ToolResultFacts, ToolResultIndexer};
use crate::ingest::{
    insert_session_event, insert_tool_call, set_tool_call_error, upsert_session_rebuilt, EventRow,
    RawMessageFacts, SessionCatalogRow, ToolCallRef,
};
use crate::providers::file_history::{self, Keys};
use crate::providers::shared_evidence::Snapshot;
use crate::{insert_history, insert_session_marker, HistoryEntry, NewSessionMarker};
use anyhow::{ensure, Context};
use std::collections::BTreeSet;

pub(super) fn session(conn: &Connection, session: &Session, path: &Path) -> Result<usize> {
    let transaction = conn
        .is_autocommit()
        .then(|| conn.unchecked_transaction())
        .transpose()?;
    file_history::check_identity(conn, ID, "KILO", &session.id, path)?;
    let shared = Snapshot::capture(conn, ID, &session.id)?;
    let mut keys = Keys::default();
    let mut order = vec![];
    let mut indexer = ToolResultIndexer::default();
    let mut inserted = 0;
    marker(
        conn,
        session,
        MarkerSpec {
            uid: "session",
            mid: None,
            ts: session.started,
            kind: "session_metadata",
            payload: &serde_json::json!({"native_session_id":session.native_id,"namespace":session.namespace,"parent_session_id":session.parent,"title":session.title,"writer":"@kilocode/cli 7.8.15"}),
        },
        &mut keys,
    )?;
    for message in &session.messages {
        crate::ingest::check_capture_cancelled()?;
        inserted += write_message(
            conn,
            session,
            message,
            &mut keys,
            &mut order,
            &mut indexer,
            None,
        )?;
    }
    let first_prompt = session
        .messages
        .iter()
        .filter(|m| m.role == "user")
        .filter_map(prompt)
        .next();
    let assistant = session
        .messages
        .iter()
        .rev()
        .filter(|m| m.role == "assistant")
        .filter_map(joined_text)
        .next();
    upsert_session_rebuilt(
        conn,
        &SessionCatalogRow {
            source: ID,
            session_id: &session.id,
            cwd: Some(&session.cwd),
            git_branch: None,
            first_ts: session.started,
            last_ts: session.latest.max(session.started),
            last_assistant_text: assistant.as_deref(),
            raw_path: Some(&path.to_string_lossy()),
        },
    )?;
    crate::discover::upsert_shallow_session(
        conn,
        &ShallowSession {
            source: ID.into(),
            session_id: session.id.clone(),
            title: Some(session.title.clone()),
            cwd: Some(session.cwd.clone()),
            discovery_state: "full".into(),
            ..Default::default()
        },
    )?;
    conn.execute("UPDATE sessions SET title=?3,first_prompt=?4,cwd=?5 WHERE source=?1 AND session_id=?2 AND (title IS NOT ?3 OR first_prompt IS NOT ?4 OR cwd IS NOT ?5)",rusqlite::params![ID,session.id,session.title,first_prompt.as_deref().map(|p| p.chars().take(512).collect::<String>()),session.cwd])?;
    retire_evidence(conn, &session.id, &keys)?;
    let reordered = file_history::reconcile_event_order(conn, ID, &session.id, &order)?;
    if !reordered.is_empty() {
        let mut replay_keys = Keys::default();
        let mut replay_order = vec![];
        let mut replay_indexer = ToolResultIndexer::default();
        for message in &session.messages {
            write_message(
                conn,
                session,
                message,
                &mut replay_keys,
                &mut replay_order,
                &mut replay_indexer,
                Some(&reordered),
            )?;
        }
    }
    prompts::reconcile_history(conn, &session.id)?;
    shared.ensure_unchanged(conn, &session.id)?;
    if let Some(tx) = transaction {
        tx.commit()?;
    }
    Ok(inserted)
}
struct MarkerSpec<'a> {
    uid: &'a str,
    mid: Option<&'a str>,
    ts: i64,
    kind: &'a str,
    payload: &'a Value,
}
fn marker(
    conn: &Connection,
    session: &Session,
    spec: MarkerSpec<'_>,
    keys: &mut Keys,
) -> Result<()> {
    let MarkerSpec {
        uid,
        mid,
        ts,
        kind,
        payload,
    } = spec;
    let bounded = crate::ingest::bound_marker_json(&payload.to_string());
    insert_session_marker(
        conn,
        ID,
        &session.id,
        &NewSessionMarker {
            marker_uid: uid,
            message_id: mid,
            ts_ms: Some(ts),
            kind,
            subkind: Some("kilo"),
            payload_json: bounded.as_deref(),
            ..Default::default()
        },
    )?;
    keys.markers.insert(uid.into());
    Ok(())
}
fn joined_text(message: &Message) -> Option<String> {
    let parts: Vec<_> = message
        .parts
        .iter()
        .filter(|p| {
            p.data["type"] == "text" && p.data["synthetic"] != true && p.data["ignored"] != true
        })
        .filter_map(|p| p.data["text"].as_str())
        .map(projections::bounded_text)
        .collect();
    (!parts.is_empty()).then(|| projections::bounded_joined_text(&parts.join("\n")))
}
fn prompt(message: &Message) -> Option<String> {
    if message.data["summary"] == true {
        return None;
    }
    joined_text(message)
}
fn write_message(
    conn: &Connection,
    session: &Session,
    message: &Message,
    keys: &mut Keys,
    order: &mut Vec<(i64, String)>,
    indexer: &mut ToolResultIndexer,
    replay: Option<&BTreeSet<i64>>,
) -> Result<usize> {
    ensure!(!message.id.is_empty(), "Kilo message ID empty");
    let mut inserted = 0;
    if replay.is_none() {
        marker(
            conn,
            session,
            MarkerSpec {
                uid: &format!("message:{}", message.id),
                mid: Some(&message.id),
                ts: message.timestamp,
                kind: "message_metadata",
                payload: &message.data,
            },
            keys,
        )?;
        if message.role == "user" {
            if let Some(text) = prompt(message) {
                let hash = crate::prompt_hash(&text);
                keys.prompts.insert(format!("{}:{hash}", message.timestamp));
                inserted += insert_history(
                    conn,
                    &HistoryEntry {
                        id: 0,
                        source: ID.into(),
                        session_id: Some(session.id.clone()),
                        project: Some(session.cwd.clone()),
                        prompt: text,
                        prompt_hash: Some(hash),
                        timestamp_ms: message.timestamp,
                    },
                )?;
            }
        }
    }
    for part in &message.parts {
        crate::ingest::check_capture_cancelled()?;
        let kind = part.data["type"]
            .as_str()
            .context("Kilo part type missing")?;
        let uid = format!("{}:{}", message.id, part.id);
        let enabled = replay.is_none_or(|times| times.contains(&message.timestamp));
        match kind {
            "text" | "reasoning" => {
                let text = part.data["text"]
                    .as_str()
                    .context("Kilo text part missing text")?;
                let text = projections::bounded_text(text);
                let control = part.data["synthetic"] == true
                    || part.data["ignored"] == true
                    || message.data["summary"] == true;
                let event_kind = if kind == "reasoning" {
                    "thinking"
                } else {
                    "text"
                };
                if enabled {
                    insert_session_event(
                        conn,
                        &EventRow {
                            source: ID,
                            session_id: &session.id,
                            message_id: &message.id,
                            event_uid: &uid,
                            ts_ms: message.timestamp,
                            role: &message.role,
                            kind: event_kind,
                            text: Some(&text),
                            project: Some(&session.cwd),
                            cwd: Some(&session.cwd),
                            raw_kind: Some("part.data"),
                            raw_facts: RawMessageFacts {
                                control_kind: (message.role == "user" && control)
                                    .then_some("synthetic"),
                                ..Default::default()
                            },
                            ..Default::default()
                        },
                    )?;
                    keys.events.insert(uid.clone());
                }
                order.push((message.timestamp, uid));
            }
            "tool" => tool(
                ToolContext {
                    conn,
                    session,
                    message,
                    enabled,
                    keys,
                    order,
                    indexer,
                    write_call: replay.is_none(),
                },
                part,
                &uid,
            )?,
            _ => {
                if replay.is_none() {
                    marker(
                        conn,
                        session,
                        MarkerSpec {
                            uid: &format!("part:{}", part.id),
                            mid: Some(&message.id),
                            ts: part.timestamp,
                            kind,
                            payload: &part.data,
                        },
                        keys,
                    )?;
                }
            }
        }
    }
    Ok(inserted)
}
struct ToolContext<'a> {
    conn: &'a Connection,
    session: &'a Session,
    message: &'a Message,
    enabled: bool,
    keys: &'a mut Keys,
    order: &'a mut Vec<(i64, String)>,
    indexer: &'a mut ToolResultIndexer,
    write_call: bool,
}
fn tool(ctx: ToolContext<'_>, part: &Part, uid: &str) -> Result<()> {
    let ToolContext {
        conn,
        session,
        message,
        enabled,
        keys,
        order,
        indexer,
        write_call,
    } = ctx;
    let call_id = part.data["callID"]
        .as_str()
        .filter(|s| !s.is_empty())
        .context("Kilo tool callID missing")?;
    let name = part.data["tool"]
        .as_str()
        .filter(|s| !s.is_empty())
        .context("Kilo tool name missing")?;
    ensure!(call_id.len() <= 1024, "Kilo tool callID exceeds bound");
    ensure!(name.len() <= 256, "Kilo tool name exceeds bound");
    let status = part.data["state"]["status"]
        .as_str()
        .context("Kilo tool status missing")?;
    ensure!(
        ["pending", "running", "completed", "error"].contains(&status),
        "unsupported Kilo tool status {status}"
    );
    let args = projections::bounded_arguments(&part.data["state"]["input"]);
    if write_call {
        insert_tool_call(
            conn,
            &ToolCallRef {
                source: ID,
                session_id: &session.id,
                message_id: &message.id,
                tool_use_id: call_id,
                ts_ms: message.timestamp,
                cwd: Some(&session.cwd),
                ..Default::default()
            },
            name,
            None,
            &args,
            None,
        )?;
        keys.tools.insert(call_id.into());
        if status == "completed" || status == "error" {
            set_tool_call_error(conn, ID, &session.id, call_id, status == "error")?;
            keys.results.insert(call_id.into());
        } else {
            conn.execute(
                "UPDATE tool_calls SET is_error=NULL WHERE source=?1 AND session_id=?2 AND tool_use_id=?3 AND location='local'",
                rusqlite::params![ID, session.id, call_id],
            )?;
        }
    }
    let call_uid = format!("{uid}:call");
    if enabled {
        let facts = ToolResultFacts {
            tool_use_id: Some(call_id.into()),
            ..Default::default()
        };
        insert_session_event(
            conn,
            &EventRow {
                source: ID,
                session_id: &session.id,
                message_id: &message.id,
                event_uid: &call_uid,
                ts_ms: message.timestamp,
                role: &message.role,
                kind: "tool_use",
                text: Some(&args),
                project: Some(&session.cwd),
                cwd: Some(&session.cwd),
                raw_kind: Some("part.data"),
                tool_result_facts: Some(&facts),
                ..Default::default()
            },
        )?;
        keys.events.insert(call_uid.clone());
    }
    order.push((message.timestamp, call_uid));
    if status != "completed" && status != "error" {
        return Ok(());
    }
    let payload = if status == "error" {
        &part.data["state"]["error"]
    } else {
        &part.data["state"]["output"]
    };
    let (call_index, event_index) = indexer.next(call_id);
    let mut facts = ToolResultFacts::from_payload(payload).with_ordering(call_index, event_index);
    facts.tool_use_id = Some(call_id.into());
    facts.event_source = Some("function_call_output".into());
    facts.result_status = Some(
        if status == "error" {
            "errored"
        } else {
            "completed"
        }
        .into(),
    );
    if status == "error" {
        facts.error_signal = Some("tool_status".into());
    }
    let result_uid = format!("{uid}:result");
    let text = bounded_result(payload, &facts);
    if enabled {
        insert_session_event(
            conn,
            &EventRow {
                source: ID,
                session_id: &session.id,
                message_id: &message.id,
                event_uid: &result_uid,
                ts_ms: message.timestamp,
                role: "tool_result",
                kind: "tool_result",
                text: Some(&text),
                project: Some(&session.cwd),
                cwd: Some(&session.cwd),
                raw_kind: Some("part.data"),
                tool_result_facts: Some(&facts),
                ..Default::default()
            },
        )?;
        keys.events.insert(result_uid.clone());
    }
    order.push((message.timestamp, result_uid));
    Ok(())
}
pub(super) fn bounded_result(payload: &Value, facts: &ToolResultFacts) -> String {
    let full = match payload {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    if Value::String(full.clone()).to_string().len() <= 512 * 1024 {
        return full;
    }
    const BOUNDARY: &str = "\n\n[Native Kilo result projection]\n";
    let projection = serde_json::json!({"projection":"kilo-tool-result-v1","truncated":true,"native_payload_bytes":facts.payload_bytes,"native_payload_hash":facts.payload_hash});
    // Keep the searchable prefix decoded. JSON-encoding it as a projection
    // field would turn a newline phrase into escaped bytes in FTS.
    let prefix = full.chars().take(32 * 1024).collect::<String>();
    let display = format!("{prefix}{BOUNDARY}{projection}");
    if Value::String(display.clone()).to_string().len() <= 512 * 1024 {
        display
    } else {
        format!("{BOUNDARY}{projection}")
    }
}

pub(super) fn retire_deleted(conn: &Connection, path: &Path, keep: &[String]) -> Result<()> {
    let keep = serde_json::to_string(keep)?;
    let mut stmt = conn.prepare("SELECT session_id FROM (SELECT session_id FROM sessions WHERE source=?1 AND raw_path=?2 UNION SELECT session_id FROM session_observations WHERE source=?1 AND location='local' AND raw_locator=?2 UNION SELECT session_id FROM session_presences WHERE source=?1 AND location='local' AND raw_locator=?2) WHERE session_id NOT IN (SELECT value FROM json_each(?3))")?;
    let absent = stmt
        .query_map(rusqlite::params![ID, path.to_string_lossy(), keep], |r| {
            r.get::<_, String>(0)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for id in absent {
        crate::ingest::check_capture_cancelled()?;
        prompts::retire_local_session(conn, &id)?;
    }
    Ok(())
}

fn retire_evidence(conn: &Connection, sid: &str, keys: &Keys) -> Result<()> {
    let results = serde_json::to_string(&keys.results)?;
    conn.execute("UPDATE tool_calls SET is_error=NULL WHERE source=?1 AND session_id=?2 AND location='local' AND is_error IS NOT NULL AND tool_use_id NOT IN (SELECT value FROM json_each(?3))",rusqlite::params![ID,sid,results])?;
    for (table, column, keep) in [
        ("session_events", "event_uid", &keys.events),
        ("tool_calls", "tool_use_id", &keys.tools),
        ("session_markers", "marker_uid", &keys.markers),
    ] {
        let condition = format!(
            "source=?1 AND session_id=?2 AND {column} NOT IN (SELECT value FROM json_each(?3))"
        );
        crate::store::retire_evidence_share(
            conn,
            table,
            &condition,
            rusqlite::params![ID, sid, serde_json::to_string(keep)?],
            crate::SessionLocation::Local,
        )?;
    }
    Ok(())
}
