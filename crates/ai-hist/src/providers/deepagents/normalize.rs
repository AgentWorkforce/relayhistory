use super::*;
use crate::ingest::tool_result_facts::{ToolResultFacts, ToolResultIndexer};
use crate::ingest::{
    insert_session_event, insert_tool_call, upsert_session_rebuilt, EventRow, RawMessageFacts,
    SessionCatalogRow, ToolCallRef,
};
use crate::store::NewSessionMarker;
use crate::HistoryEntry;
use anyhow::{ensure, Context};
use sha2::{Digest, Sha256};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Default)]
struct Keys {
    events: BTreeSet<String>,
    tools: BTreeSet<String>,
    markers: BTreeSet<String>,
    event_order: Vec<(i64, String)>,
}
struct MarkerSpec<'a> {
    uid: &'a str,
    message_id: Option<&'a str>,
    ts_ms: i64,
    kind: &'a str,
}
fn marker(
    conn: &Connection,
    s: &Session,
    spec: MarkerSpec<'_>,
    payload: &serde_json::Value,
    keys: &mut Keys,
) -> Result<()> {
    let text = crate::ingest::bound_marker_json(&payload.to_string());
    crate::insert_session_marker(
        conn,
        ID,
        &s.id,
        &NewSessionMarker {
            marker_uid: spec.uid,
            ts_ms: Some(spec.ts_ms),
            message_id: spec.message_id,
            parent_id: None,
            turn_id: None,
            kind: spec.kind,
            subkind: Some("dcode"),
            text: None,
            payload_json: text.as_deref(),
        },
    )?;
    keys.markers.insert(spec.uid.into());
    Ok(())
}
pub(super) fn session(conn: &Connection, s: &Session, path: &Path) -> Result<usize> {
    let transaction = conn
        .is_autocommit()
        .then(|| conn.unchecked_transaction())
        .transpose()?;
    let shared = crate::providers::shared_evidence::Snapshot::capture(conn, ID, &s.id)?;
    let inserted = write(conn, s, path)?;
    shared.ensure_unchanged(conn, &s.id)?;
    if let Some(transaction) = transaction {
        transaction.commit()?;
    }
    Ok(inserted)
}
fn write(conn: &Connection, s: &Session, path: &Path) -> Result<usize> {
    crate::ingest::check_capture_cancelled()?;
    let mut keys = Keys::default();
    let mut indexer = ToolResultIndexer::default();
    marker(
        conn,
        s,
        MarkerSpec {
            uid: &format!("checkpoint:{}", hex(&s.checkpoint_id)),
            message_id: None,
            ts_ms: s.latest,
            kind: "session_metadata",
        },
        &serde_json::json!({
            "native_thread_id":s.native_id,"checkpoint_id":s.checkpoint_id,"timestamp_provenance":"uuid7_creation_ms_or_first_checkpoint_for_undated_messages","format":"dcode-msgpack-v2-or-v4"
        }),
        &mut keys,
    )?;
    let mut inserted = 0;
    for m in &s.messages {
        crate::ingest::check_capture_cancelled()?;
        inserted += message(conn, s, m, &mut keys, &mut indexer, false)?;
    }
    reconcile_tool_status(conn, s, &keys.tools)?;
    let assistant = s
        .messages
        .iter()
        .rev()
        .find(|m| m.role == "assistant" && !m.content.is_empty())
        .map(|m| bounded_content(&m.content));
    upsert_session_rebuilt(
        conn,
        &SessionCatalogRow {
            source: ID,
            session_id: &s.id,
            cwd: s.cwd.as_deref(),
            git_branch: s.git_branch.as_deref(),
            first_ts: s.started,
            last_ts: s.latest,
            last_assistant_text: assistant.as_deref(),
            raw_path: Some(&path.to_string_lossy()),
        },
    )?;
    crate::discover::upsert_shallow_session(
        conn,
        &ShallowSession {
            source: ID.into(),
            session_id: s.id.clone(),
            title: s.title.clone(),
            discovery_state: "full".into(),
            ..Default::default()
        },
    )?;
    let first_prompt = s
        .messages
        .iter()
        .find(|m| m.role == "user" && !control(m))
        .map(|m| m.content.chars().take(512).collect::<String>());
    conn.execute("UPDATE sessions SET title=?3,first_prompt=?4,cwd=?5,git_branch=?6 WHERE source=?1 AND session_id=?2 AND (title IS NOT ?3 OR first_prompt IS NOT ?4 OR cwd IS NOT ?5 OR git_branch IS NOT ?6)", rusqlite::params![ID,s.id,s.title,first_prompt,s.cwd,s.git_branch])?;
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
    let changed =
        crate::providers::file_history::reconcile_event_order(conn, ID, &s.id, &keys.event_order)?;
    if !changed.is_empty() {
        let mut replay = Keys::default();
        let mut replay_indexer = ToolResultIndexer::default();
        for m in &s.messages {
            message(conn, s, m, &mut replay, &mut replay_indexer, true)?;
        }
    }
    reconcile_history(conn, &s.id)?;
    Ok(inserted)
}
fn reconcile_tool_status(conn: &Connection, s: &Session, tools: &BTreeSet<String>) -> Result<()> {
    let mut outcomes = BTreeMap::new();
    for message in &s.messages {
        if message.role == "tool" {
            if let Some(id) = &message.tool_call_id {
                let status = match message.status.as_deref() {
                    Some("error") => Some(true),
                    Some("success") => Some(false),
                    _ => None,
                };
                outcomes.insert(id.as_str(), status);
            }
        }
    }
    for id in tools {
        let status = outcomes.get(id.as_str()).copied().flatten();
        // A conclusive native outcome must conflict with a different remote
        // outcome. Snapshot::ensure_unchanged rolls the transaction back.
        // Unknown/removal may clear only our own provisional result.
        conn.execute(
            "UPDATE tool_calls SET is_error=?4 WHERE source=?1 AND session_id=?2 AND tool_use_id=?3 AND (?4 IS NOT NULL OR location='local') AND is_error IS NOT ?4",
            rusqlite::params![ID, s.id, id, status.map(i64::from)],
        )?;
    }
    Ok(())
}
fn control(_m: &Message) -> bool {
    false
}
fn hex(value: &str) -> String {
    value
        .as_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
fn bounded_content(content: &str) -> Cow<'_, str> {
    const MAX_PREFIX_BYTES: usize = 32 * 1024;
    if content.len() <= MAX_PREFIX_BYTES {
        return Cow::Borrowed(content);
    }
    let mut end = MAX_PREFIX_BYTES;
    while !content.is_char_boundary(end) {
        end -= 1;
    }
    Cow::Owned(format!(
        "{}\n[DeepAgents native text truncated: {} bytes; sha256={:x}]",
        &content[..end],
        content.len(),
        Sha256::digest(content.as_bytes())
    ))
}
fn message(
    conn: &Connection,
    s: &Session,
    m: &Message,
    keys: &mut Keys,
    indexer: &mut ToolResultIndexer,
    replay: bool,
) -> Result<usize> {
    ensure!(
        ["user", "assistant", "tool", "system"].contains(&m.role.as_str()),
        "unsupported DeepAgents role"
    );
    let content = bounded_content(&m.content);
    let prefix = format!("msg:{}", hex(&m.id));
    if m.role == "system" {
        if !replay {
            marker(
                conn,
                s,
                MarkerSpec {
                    uid: &format!("{prefix}:system"),
                    message_id: Some(&m.id),
                    ts_ms: s.started,
                    kind: "system",
                },
                &serde_json::json!({"native_message_id":m.id,"content":content}),
                keys,
            )?;
        }
        return Ok(0);
    }
    let mut inserted = 0;
    if !replay && m.role == "user" && !m.content.is_empty() {
        inserted += crate::insert_history(
            conn,
            &HistoryEntry {
                id: 0,
                source: ID.into(),
                session_id: Some(s.id.clone()),
                project: s.cwd.clone(),
                prompt: content.to_string(),
                prompt_hash: None,
                timestamp_ms: s.started,
            },
        )?;
    }
    if !m.content.is_empty() || m.role == "tool" {
        let kind = if m.role == "tool" {
            "tool_result"
        } else {
            "text"
        };
        let uid = format!("{prefix}:{kind}");
        let facts = (m.role == "tool").then(|| {
            let mut facts =
                ToolResultFacts::from_payload(&serde_json::Value::String(m.content.clone()));
            facts.tool_use_id = m.tool_call_id.clone();
            facts.result_status = Some(
                match m.status.as_deref() {
                    Some("success") => "completed",
                    Some("error") => "errored",
                    _ => "unknown",
                }
                .into(),
            );
            facts.event_source =
                Some(crate::ingest::tool_result_facts::EVENT_SOURCE_FUNCTION_CALL_OUTPUT.into());
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
                message_id: &m.id,
                event_uid: &uid,
                ts_ms: s.started,
                role: if m.role == "tool" {
                    "tool_result"
                } else {
                    &m.role
                },
                kind,
                text: Some(&content),
                model: None,
                project: s.cwd.as_deref(),
                cwd: s.cwd.as_deref(),
                git_branch: s.git_branch.as_deref(),
                raw_kind: Some("messages"),
                raw_facts: RawMessageFacts::default(),
                tool_result_facts: facts.as_ref(),
                ..Default::default()
            },
        )?;
        keys.event_order.push((s.started, uid.clone()));
        keys.events.insert(uid);
    }
    for call in &m.tool_calls {
        tool(conn, s, m, &prefix, call, keys, replay)?;
    }
    Ok(inserted)
}
fn tool(
    conn: &Connection,
    s: &Session,
    m: &Message,
    prefix: &str,
    call: &serde_json::Value,
    keys: &mut Keys,
    replay: bool,
) -> Result<()> {
    let id = call
        .get("id")
        .and_then(serde_json::Value::as_str)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| anyhow::anyhow!("DeepAgents tool call lacks ID"))?;
    let name = call
        .get("name")
        .and_then(serde_json::Value::as_str)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| anyhow::anyhow!("DeepAgents tool call lacks name"))?;
    ensure!(
        id.len() <= 1024 && name.len() <= 256,
        "DeepAgents tool identity exceeds limit"
    );
    let args = bounded_tool_json(
        call.get("args")
            .context("DeepAgents tool call lacks arguments")?,
        "arguments",
        id,
        name,
    )?;
    let event_text = bounded_tool_json(call, "tool_call", id, name)?;
    if !replay {
        insert_tool_call(
            conn,
            &ToolCallRef {
                source: ID,
                session_id: &s.id,
                message_id: &m.id,
                tool_use_id: id,
                ts_ms: s.started,
                ..Default::default()
            },
            name,
            None,
            &args,
            None,
        )?;
        keys.tools.insert(id.into());
    }
    let uid = format!("{prefix}:tool:{}", hex(id));
    let facts = ToolResultFacts {
        tool_use_id: Some(id.into()),
        ..Default::default()
    };
    insert_session_event(
        conn,
        &EventRow {
            source: ID,
            session_id: &s.id,
            message_id: &m.id,
            event_uid: &uid,
            ts_ms: s.started,
            role: "assistant",
            kind: "tool_use",
            text: Some(&event_text),
            model: None,
            project: s.cwd.as_deref(),
            cwd: s.cwd.as_deref(),
            git_branch: s.git_branch.as_deref(),
            raw_kind: Some("tool_calls"),
            tool_result_facts: Some(&facts),
            ..Default::default()
        },
    )?;
    keys.event_order.push((s.started, uid.clone()));
    keys.events.insert(uid);
    Ok(())
}
fn bounded_tool_json(
    value: &serde_json::Value,
    kind: &str,
    id: &str,
    name: &str,
) -> Result<String> {
    const MAX_INLINE_BYTES: usize = 64 * 1024;
    let encoded = serde_json::to_string(value)?;
    if encoded.len() <= MAX_INLINE_BYTES {
        return Ok(encoded);
    }
    Ok(serde_json::json!({
        "projection": "deepagents-native-tool-v1",
        "kind": kind,
        "native_id": id,
        "name": name,
        "truncated": true,
        "native_encoded_bytes": encoded.len(),
        "native_sha256": format!("{:x}", Sha256::digest(encoded.as_bytes())),
    })
    .to_string())
}
fn reconcile_history(conn: &Connection, session: &str) -> Result<()> {
    const MATCH: &str = "e.source=history.source AND e.role='user' AND e.kind='text' AND e.control_kind IS NULL AND e.ts_ms=history.timestamp_ms AND e.text=history.prompt";
    conn.execute(&format!("UPDATE history SET session_id=(SELECT e.session_id FROM session_events e WHERE {MATCH} ORDER BY e.session_id LIMIT 1), project=(SELECT e.cwd FROM session_events e WHERE {MATCH} ORDER BY e.session_id LIMIT 1) WHERE source=?1 AND session_id=?2 AND NOT EXISTS(SELECT 1 FROM session_events e WHERE {MATCH} AND e.session_id=?2) AND EXISTS(SELECT 1 FROM session_events e WHERE {MATCH})"),rusqlite::params![ID,session])?;
    conn.execute(&format!("DELETE FROM history WHERE source=?1 AND session_id=?2 AND NOT EXISTS(SELECT 1 FROM session_events e WHERE {MATCH})"),rusqlite::params![ID,session])?;
    Ok(())
}
pub(super) fn retire_deleted(conn: &Connection, path: &Path, ids: &[String]) -> Result<()> {
    let keep = serde_json::to_string(ids)?;
    let mut stmt = conn.prepare("SELECT session_id FROM session_presences WHERE source=?1 AND location='local' AND raw_locator=?2 UNION SELECT session_id FROM session_observations WHERE source=?1 AND location='local' AND raw_locator=?2 UNION SELECT s.session_id FROM sessions s WHERE s.source=?1 AND s.raw_path=?2 AND EXISTS(SELECT 1 FROM session_events e WHERE e.source=s.source AND e.session_id=s.session_id AND e.location IN ('local','both')) EXCEPT SELECT value FROM json_each(?3)")?;
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
