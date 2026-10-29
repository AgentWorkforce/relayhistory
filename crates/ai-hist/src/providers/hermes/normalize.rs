use super::*;
use crate::ingest::tool_result_facts::{ToolResultFacts, ToolResultIndexer};
use crate::ingest::{
    insert_session_event, insert_tool_call, upsert_session_rebuilt, EventRow, RawMessageFacts,
    SessionCatalogRow, ToolCallRef,
};
use crate::store::NewSessionMarker;
use crate::HistoryEntry;
use anyhow::ensure;
use std::collections::BTreeSet;
#[derive(Default)]
struct Keys {
    events: BTreeSet<String>,
    tools: BTreeSet<String>,
    markers: BTreeSet<String>,
}
/// One Hermes marker row, before the session and key bookkeeping it is
/// written with.
struct HermesMarker<'a> {
    uid: &'a str,
    message: Option<&'a str>,
    ts: i64,
    kind: &'a str,
    payload: &'a Value,
}
fn marker(conn: &Connection, s: &Session, m: HermesMarker<'_>, keys: &mut Keys) -> Result<()> {
    let HermesMarker {
        uid,
        message,
        ts,
        kind,
        payload,
    } = m;
    crate::insert_session_marker(
        conn,
        ID,
        &s.id,
        &NewSessionMarker {
            marker_uid: uid,
            ts_ms: Some(ts),
            message_id: message,
            parent_id: None,
            turn_id: None,
            kind,
            subkind: Some("hermes"),
            text: None,
            payload_json: Some(&payload.to_string()),
        },
    )?;
    keys.markers.insert(uid.into());
    Ok(())
}
pub(super) fn session(conn: &Connection, s: &Session, path: &Path) -> Result<usize> {
    // Callers normally own a database snapshot transaction; direct internal
    // callers receive the same rollback guarantee without nesting transactions.
    let transaction = conn
        .is_autocommit()
        .then(|| conn.unchecked_transaction())
        .transpose()?;
    let shared = crate::providers::shared_evidence::Snapshot::capture(conn, ID, &s.id)?;
    let inserted = write_session(conn, s, path)?;
    shared.ensure_unchanged(conn, &s.id)?;
    if let Some(transaction) = transaction {
        transaction.commit()?;
    }
    Ok(inserted)
}
fn write_session(conn: &Connection, s: &Session, path: &Path) -> Result<usize> {
    crate::ingest::check_capture_cancelled()?;
    let mut keys = Keys::default();
    let mut inserted = 0;
    let mut indexer = ToolResultIndexer::default();
    marker(
        conn,
        s,
        HermesMarker {
            uid: "session",
            message: None,
            ts: s.started,
            kind: "session_metadata",
            payload: &serde_json::json!({"native_session_id":s.native_id,"profile":s.profile,"parent_session_id":s.parent,"end_reason":s.end_reason,"title":s.title,"model":s.model,"schema_version":31}),
        },
        &mut keys,
    )?;
    for m in &s.messages {
        crate::ingest::check_capture_cancelled()?;
        inserted += message(conn, s, m, &mut keys, &mut indexer)?;
    }
    let last = s.latest.max(s.started);
    let assistant = s
        .messages
        .iter()
        .rev()
        .find(|m| m.role == "assistant" && m.content.is_some())
        .and_then(|m| m.content.as_deref());
    upsert_session_rebuilt(
        conn,
        &SessionCatalogRow {
            source: ID,
            session_id: &s.id,
            cwd: s.cwd.as_deref(),
            git_branch: s.git_branch.as_deref(),
            first_ts: s.started,
            last_ts: last,
            last_assistant_text: assistant,
            raw_path: Some(&path.to_string_lossy()),
        },
    )?;
    crate::discover::upsert_shallow_session(
        conn,
        &ShallowSession {
            source: ID.into(),
            session_id: s.id.clone(),
            title: s.title.clone(),
            models: s.model.iter().cloned().collect(),
            discovery_state: "full".into(),
            ..Default::default()
        },
    )?;
    // Missing cwd withdraws only the local path projection; stronger recorded
    // remote/inherited identities remain authoritative.
    if s.cwd.as_deref().is_none_or(|cwd| cwd.trim().is_empty()) {
        for table in ["sessions", "session_events"] {
            let scope = if table == "session_events" {
                " AND location IN ('local','both')"
            } else {
                ""
            };
            conn.execute(&format!("UPDATE {table} SET project_key=NULL,project_key_method=NULL WHERE source=?1 AND session_id=?2 AND project_key_method='path'{scope}"), rusqlite::params![ID,s.id])?;
        }
    }
    // A complete snapshot is authoritative even when metadata was cleared.
    let models = serde_json::to_string(&s.model.iter().collect::<Vec<_>>())?;
    let first_prompt = s
        .messages
        .iter()
        .find(|m| m.role == "user" && !is_control(m) && m.content.is_some())
        .and_then(|m| m.content.as_ref())
        .map(|text| text.chars().take(512).collect::<String>());
    conn.execute("UPDATE sessions SET title=?3,models_json=?4,first_prompt=?5,cwd=?6,git_branch=?7 WHERE source=?1 AND session_id=?2 AND (title IS NOT ?3 OR models_json IS NOT ?4 OR first_prompt IS NOT ?5 OR cwd IS NOT ?6 OR git_branch IS NOT ?7)",rusqlite::params![ID,s.id,s.title,models,first_prompt,s.cwd,s.git_branch])?;
    for (table, column, keep) in [
        ("session_events", "event_uid", &keys.events),
        ("tool_calls", "tool_use_id", &keys.tools),
        ("session_markers", "marker_uid", &keys.markers),
    ] {
        crate::store::retire_evidence_share(
            conn,
            table,
            &format!(
                "source=?1 AND session_id=?2 AND {column} NOT IN (SELECT value FROM json_each(?3))"
            ),
            rusqlite::params![ID, s.id, serde_json::to_string(keep)?],
            crate::SessionLocation::Local,
        )?;
    }
    reconcile_history(conn, &s.id)?;
    Ok(inserted)
}
fn message(
    conn: &Connection,
    s: &Session,
    m: &Message,
    keys: &mut Keys,
    indexer: &mut ToolResultIndexer,
) -> Result<usize> {
    ensure!(m.id > 0, "invalid Hermes message row ID");
    ensure!(
        ["user", "assistant", "tool", "system"].contains(&m.role.as_str()),
        "unsupported Hermes message role {}",
        m.role
    );
    let mid = m.id.to_string();
    let control = is_control(m);
    marker(
        conn,
        s,
        HermesMarker {
            uid: &format!("message:{mid}"),
            message: Some(&mid),
            ts: m.timestamp,
            kind: "message_metadata",
            payload: &serde_json::json!({"active":m.active,"compacted":m.compacted,"compressed_summary":m.compressed,"observed":m.observed,"display_kind":m.display_kind,"tool_name":m.tool_name}),
        },
        keys,
    )?;
    if m.role == "system" {
        marker(
            conn,
            s,
            HermesMarker {
                uid: &format!("system:{mid}"),
                message: Some(&mid),
                ts: m.timestamp,
                kind: "system",
                payload: &serde_json::json!({"content":m.content}),
            },
            keys,
        )?;
        return Ok(0);
    }
    let mut inserted = 0;
    if m.role == "user" && !control {
        if let Some(text) = &m.content {
            inserted += crate::insert_history(
                conn,
                &HistoryEntry {
                    event_uid: None,
                    id: 0,
                    source: ID.into(),
                    session_id: Some(s.id.clone()),
                    project: s.cwd.clone(),
                    prompt: text.clone(),
                    prompt_hash: None,
                    timestamp_ms: m.timestamp,
                },
            )?;
        }
    }
    for (kind, text) in [
        (
            if m.role == "tool" {
                "tool_result"
            } else {
                "text"
            },
            m.content.as_deref(),
        ),
        (
            "thinking",
            m.reasoning_content.as_deref().or(m.reasoning.as_deref()),
        ),
    ] {
        let Some(text) = text else {
            continue;
        };
        let uid = format!("{mid}:{kind}");
        let facts = (kind == "tool_result").then(|| {
            let mut facts = ToolResultFacts::from_payload(&Value::String(text.into()));
            facts.tool_use_id = m.tool_call_id.clone();
            facts.result_status = Some("unknown".into());
            facts.event_source = Some("function_call_output".into());
            if let Some(call) = &m.tool_call_id {
                let (i, j) = indexer.next(call);
                facts = facts.with_ordering(i, j);
            }
            facts
        });
        insert_session_event(
            conn,
            &EventRow {
                source: ID,
                session_id: &s.id,
                message_id: &mid,
                event_uid: &uid,
                ts_ms: m.timestamp,
                role: if m.role == "tool" {
                    "tool_result"
                } else {
                    &m.role
                },
                kind,
                text: Some(text),
                model: None,
                cwd: s.cwd.as_deref(),
                git_branch: s.git_branch.as_deref(),
                raw_kind: Some("messages"),
                raw_facts: RawMessageFacts {
                    stop_reason: m.finish_reason.as_deref(),
                    control_kind: if m.role == "user" && kind == "text" && control {
                        Some("synthetic")
                    } else {
                        None
                    },
                    ..Default::default()
                },
                tool_result_facts: facts.as_ref(),
                ..Default::default()
            },
        )?;
        keys.events.insert(uid);
    }
    if let Some(raw) = &m.tool_calls {
        let calls: Value = serde_json::from_str(raw).context("malformed Hermes tool_calls")?;
        let calls = calls
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("Hermes tool_calls must be an array"))?;
        for (index, call) in calls.iter().enumerate() {
            tool(conn, s, m, &mid, index, call, keys)?;
        }
    }
    Ok(inserted)
}
use anyhow::Context;
fn tool(
    conn: &Connection,
    s: &Session,
    m: &Message,
    mid: &str,
    index: usize,
    call: &Value,
    keys: &mut Keys,
) -> Result<()> {
    let id = call["id"]
        .as_str()
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("Hermes tool call missing id"))?;
    let name = call["function"]["name"]
        .as_str()
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("Hermes tool call missing name"))?;
    let args = call["function"]["arguments"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("Hermes tool arguments must be a string"))?;
    // Preserve malformed argument strings as recorded; never treat paths as reads.
    insert_tool_call(
        conn,
        &ToolCallRef {
            source: ID,
            session_id: &s.id,
            message_id: mid,
            tool_use_id: id,
            ts_ms: m.timestamp,
            ..Default::default()
        },
        name,
        None,
        args,
        None,
    )?;
    keys.tools.insert(id.into());
    let uid = format!("{mid}:tool:{index}");
    let facts = ToolResultFacts {
        tool_use_id: Some(id.into()),
        ..Default::default()
    };
    insert_session_event(
        conn,
        &EventRow {
            source: ID,
            session_id: &s.id,
            message_id: mid,
            event_uid: &uid,
            ts_ms: m.timestamp,
            role: &m.role,
            kind: "tool_use",
            text: Some(&call.to_string()),
            model: None,
            raw_kind: Some("tool_calls"),
            tool_result_facts: Some(&facts),
            ..Default::default()
        },
    )?;
    keys.events.insert(uid);
    Ok(())
}

fn is_control(m: &Message) -> bool {
    m.compressed
        || m.observed
        || (!m.active && !m.compacted)
        || matches!(
            m.display_kind.as_deref(),
            Some("hidden" | "async_delegation_complete")
        )
}

// History has a source/timestamp/text identity shared by sessions, not an event
// identity or location column. Reconcile against the finalized evidence rows.
fn reconcile_history(conn: &Connection, session: &str) -> Result<()> {
    const MATCH: &str = "e.source=history.source AND e.role='user' AND e.kind='text' AND e.control_kind IS NULL AND e.ts_ms=history.timestamp_ms AND e.text=history.prompt";
    conn.execute(&format!("UPDATE history SET session_id=(SELECT e.session_id FROM session_events e WHERE {MATCH} ORDER BY e.session_id LIMIT 1), project=(SELECT e.cwd FROM session_events e WHERE {MATCH} ORDER BY e.session_id LIMIT 1) WHERE source=?1 AND session_id=?2 AND NOT EXISTS(SELECT 1 FROM session_events e WHERE {MATCH} AND e.session_id=?2) AND EXISTS(SELECT 1 FROM session_events e WHERE {MATCH})"),rusqlite::params![ID,session])?;
    conn.execute(&format!("DELETE FROM history WHERE source=?1 AND session_id=?2 AND NOT EXISTS(SELECT 1 FROM session_events e WHERE {MATCH})"),rusqlite::params![ID,session])?;
    conn.execute("UPDATE history SET project=(SELECT e.cwd FROM session_events e WHERE e.source=history.source AND e.session_id=history.session_id AND e.role='user' AND e.kind='text' AND e.control_kind IS NULL AND e.ts_ms=history.timestamp_ms AND e.text=history.prompt LIMIT 1) WHERE source=?1 AND session_id=?2 AND project IS NOT (SELECT e.cwd FROM session_events e WHERE e.source=history.source AND e.session_id=history.session_id AND e.role='user' AND e.kind='text' AND e.control_kind IS NULL AND e.ts_ms=history.timestamp_ms AND e.text=history.prompt LIMIT 1)", rusqlite::params![ID,session])?;
    Ok(())
}

pub(super) fn retire_deleted(conn: &Connection, path: &Path, sessions: &[Session]) -> Result<()> {
    let keep = serde_json::to_string(&sessions.iter().map(|s| &s.id).collect::<Vec<_>>())?;
    let mut stmt = conn.prepare("SELECT session_id FROM sessions WHERE source=?1 AND raw_path=?2 AND session_id NOT IN (SELECT value FROM json_each(?3))")?;
    let absent = stmt
        .query_map(rusqlite::params![ID, path.to_string_lossy(), keep], |r| {
            r.get::<_, String>(0)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for id in absent {
        crate::ingest::check_capture_cancelled()?;
        for table in ["session_events", "tool_calls", "session_markers"] {
            crate::store::retire_evidence_share(
                conn,
                table,
                "source=?1 AND session_id=?2",
                rusqlite::params![ID, id],
                crate::SessionLocation::Local,
            )?;
        }
        reconcile_history(conn, &id)?;
        conn.execute("DELETE FROM session_hydration_checkpoints WHERE source=?1 AND session_id=?2 AND location='local'", rusqlite::params![ID,id])?;
        conn.execute(
            "DELETE FROM session_presences WHERE source=?1 AND session_id=?2 AND location='local'",
            rusqlite::params![ID, id],
        )?;
        conn.execute("DELETE FROM sessions WHERE source=?1 AND session_id=?2 AND NOT EXISTS(SELECT 1 FROM session_presences WHERE source=?1 AND session_id=?2 AND location='remote') AND NOT EXISTS(SELECT 1 FROM session_events WHERE source=?1 AND session_id=?2) AND NOT EXISTS(SELECT 1 FROM tool_calls WHERE source=?1 AND session_id=?2) AND NOT EXISTS(SELECT 1 FROM session_markers WHERE source=?1 AND session_id=?2)", rusqlite::params![ID,id])?;
    }
    Ok(())
}
