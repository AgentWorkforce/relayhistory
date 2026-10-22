use super::*;
use crate::ingest::tool_result_facts::{ToolResultFacts, ToolResultIndexer};
use crate::ingest::{
    insert_session_event, insert_tool_call, set_tool_call_error, upsert_session_rebuilt, EventRow,
    RawMessageFacts, SessionCatalogRow, ToolCallRef,
};
use crate::store::NewSessionMarker;
use crate::HistoryEntry;
use anyhow::{ensure, Context};
use std::collections::BTreeSet;
#[derive(Default)]
struct Keys {
    events: BTreeSet<String>,
    event_order: Vec<(i64, String)>,
    tools: BTreeSet<String>,
    results: BTreeSet<String>,
    markers: BTreeSet<String>,
}
/// One Goose marker row, before the session and key bookkeeping it is
/// written with.
struct GooseMarker<'a> {
    uid: &'a str,
    message: Option<&'a str>,
    ts: i64,
    kind: &'a str,
    payload: &'a Value,
}
fn marker(conn: &Connection, s: &Session, m: GooseMarker<'_>, keys: &mut Keys) -> Result<()> {
    let GooseMarker {
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
            subkind: Some("goose"),
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
fn text_blocks(m: &Message) -> Vec<&str> {
    m.content
        .as_array()
        .into_iter()
        .flatten()
        .filter(|b| b["type"] == "text")
        .filter_map(|b| b["text"].as_str())
        .collect()
}
fn joined_text(m: &Message) -> Option<String> {
    let parts = text_blocks(m);
    (!parts.is_empty()).then(|| parts.join("\n"))
}
fn message_id(m: &Message) -> String {
    m.native_id
        .clone()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("row:{}", m.row_id))
}
fn write_session(conn: &Connection, s: &Session, path: &Path) -> Result<usize> {
    crate::ingest::check_capture_cancelled()?;
    let mut keys = Keys::default();
    let mut inserted = 0;
    let mut indexer = ToolResultIndexer::default();
    marker(
        conn,
        s,
        GooseMarker {
            uid: "session",
            message: None,
            ts: s.started,
            kind: "session_metadata",
            payload: &serde_json::json!({"native_session_id":s.native_id,"namespace":s.namespace,"parent_session_id":s.parent,"title":s.title,"model":s.model,"schema_version":17}),
        },
        &mut keys,
    )?;
    for m in &s.messages {
        crate::ingest::check_capture_cancelled()?;
        inserted += blocks::message(conn, s, m, &mut keys, &mut indexer, None)?;
    }
    let assistant = s
        .messages
        .iter()
        .rev()
        .filter(|m| m.role == "assistant")
        .find_map(joined_text);
    upsert_session_rebuilt(
        conn,
        &SessionCatalogRow {
            source: ID,
            session_id: &s.id,
            cwd: s.cwd.as_deref(),
            git_branch: None,
            first_ts: s.started,
            last_ts: s.latest.max(s.started),
            last_assistant_text: assistant.as_deref(),
            raw_path: Some(&path.to_string_lossy()),
        },
    )?;
    crate::discover::upsert_shallow_session(
        conn,
        &ShallowSession {
            source: ID.into(),
            session_id: s.id.clone(),
            title: Some(s.title.clone()),
            models: s.model.iter().cloned().collect(),
            discovery_state: "full".into(),
            ..Default::default()
        },
    )?;
    let models = serde_json::to_string(&s.model.iter().collect::<Vec<_>>())?;
    let first_prompt = s
        .messages
        .iter()
        .filter(|m| m.role == "user" && !is_control(m))
        .find_map(joined_text)
        .map(|t| t.chars().take(512).collect::<String>());
    conn.execute("UPDATE sessions SET title=?3,models_json=?4,first_prompt=?5,cwd=?6,git_branch=NULL WHERE source=?1 AND session_id=?2 AND (title IS NOT ?3 OR models_json IS NOT ?4 OR first_prompt IS NOT ?5 OR cwd IS NOT ?6 OR git_branch IS NOT NULL)",
        rusqlite::params![ID,s.id,s.title,models,first_prompt,s.cwd])?;
    let results = serde_json::to_string(&keys.results)?;
    conn.execute("UPDATE tool_calls SET is_error=NULL WHERE source=?1 AND session_id=?2 AND location='local' AND is_error IS NOT NULL AND tool_use_id NOT IN (SELECT value FROM json_each(?3))",rusqlite::params![ID,s.id,results])?;
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
    let reordered =
        crate::providers::file_history::reconcile_event_order(conn, ID, &s.id, &keys.event_order)?;
    if !reordered.is_empty() {
        let mut replay_keys = Keys::default();
        let mut replay_indexer = ToolResultIndexer::default();
        for message in &s.messages {
            blocks::message(
                conn,
                s,
                message,
                &mut replay_keys,
                &mut replay_indexer,
                Some(&reordered),
            )?;
        }
    }
    reconcile_history(conn, &s.id)?;
    Ok(inserted)
}
mod blocks;

fn is_control(m: &Message) -> bool {
    m.metadata
        .as_ref()
        .is_some_and(|v| v["turnContext"] == true || v["userVisible"] == false)
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
    let mut stmt = conn.prepare("SELECT session_id FROM (SELECT session_id FROM sessions WHERE source=?1 AND raw_path=?2 UNION SELECT session_id FROM session_observations WHERE source=?1 AND location='local' AND raw_locator=?2 UNION SELECT session_id FROM session_presences WHERE source=?1 AND location='local' AND raw_locator=?2) WHERE session_id NOT IN (SELECT value FROM json_each(?3))")?;
    let absent = stmt
        .query_map(rusqlite::params![ID, path.to_string_lossy(), keep], |r| {
            r.get::<_, String>(0)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for id in absent {
        crate::ingest::check_capture_cancelled()?;
        crate::providers::file_history::retire_local_session(conn, ID, &id)?;
    }
    Ok(())
}
