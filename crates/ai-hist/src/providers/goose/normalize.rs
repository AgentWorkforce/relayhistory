use super::*;
use crate::ingest::tool_result_facts::{ToolResultFacts, ToolResultIndexer};
use crate::ingest::{
    insert_session_event, insert_tool_call, set_tool_call_error, upsert_session_rebuilt, EventRow,
    RawMessageFacts, SessionCatalogRow, ToolCallRef,
};
use crate::store::NewSessionMarker;
use crate::HistoryEntry;
use anyhow::{ensure, Context};
use std::collections::{BTreeMap, BTreeSet};
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
    // Markers are bounded projections by contract. Full tool result bodies
    // belong in the event text column, not in marker payload_json.
    let bounded_payload = crate::ingest::bound_marker_json(&payload.to_string());
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
            payload_json: bounded_payload.as_deref(),
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

// History is keyed by source/time/joined prompt, while public events retain
// individual native text blocks. The positional event key distinguishes even
// repeated native message IDs within one timestamp bucket.
fn text_message_key(uid: &str) -> Option<(&str, usize)> {
    let without_tag = uid.strip_suffix(":text")?;
    let (key, position) = without_tag.rsplit_once(':')?;
    if !(key.starts_with("native:") || key.starts_with("row:")) {
        return None;
    }
    position
        .parse::<usize>()
        .ok()
        .map(|position| (key, position))
}

struct PromptSupport {
    session_id: String,
    timestamp_ms: i64,
    prompt: String,
    project: Option<String>,
}

struct PromptPart {
    position: usize,
    text: String,
    project: Option<String>,
}

fn supported_prompts(conn: &Connection, times: &BTreeSet<i64>) -> Result<Vec<PromptSupport>> {
    let mut groups: BTreeMap<(String, i64, String), Vec<PromptPart>> = BTreeMap::new();
    let timestamps = serde_json::to_string(times)?;
    let mut stmt = conn.prepare("SELECT session_id,ts_ms,event_uid,text,cwd FROM session_events WHERE source=?1 AND role='user' AND kind='text' AND text IS NOT NULL AND control_kind IS NULL AND ts_ms IN (SELECT value FROM json_each(?2)) ORDER BY ts_ms,id")?;
    let rows = stmt.query_map(rusqlite::params![ID, timestamps], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, Option<String>>(4)?,
        ))
    })?;
    for row in rows {
        let (sid, ts, uid, text, project) = row?;
        // Remote intake accepts opaque event IDs. Let an unknown format
        // support its own single-event prompt without grouping it with native
        // blocks or aborting an unrelated local session.
        let (key, position) = text_message_key(&uid)
            .map(|(key, position)| (key.to_owned(), position))
            .unwrap_or_else(|| (format!("opaque:{uid}"), 0));
        groups.entry((sid, ts, key)).or_default().push(PromptPart {
            position,
            text,
            project,
        });
    }
    Ok(groups
        .into_iter()
        .map(|((session_id, timestamp_ms, _), mut parts)| {
            parts.sort_by_key(|part| part.position);
            let project = parts.first().and_then(|part| part.project.clone());
            PromptSupport {
                session_id,
                timestamp_ms,
                prompt: parts
                    .into_iter()
                    .map(|part| part.text)
                    .collect::<Vec<_>>()
                    .join("\n"),
                project,
            }
        })
        .collect())
}

fn reconcile_history(conn: &Connection, session: &str) -> Result<()> {
    let mut stmt = conn
        .prepare("SELECT id,timestamp_ms,prompt FROM history WHERE source=?1 AND session_id=?2")?;
    let rows = stmt
        .query_map([ID, session], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let times = rows.iter().map(|(_, ts, _)| *ts).collect();
    let supported = supported_prompts(conn, &times)?;
    for (id, ts, prompt) in rows {
        let owner = supported
            .iter()
            .filter(|p| p.timestamp_ms == ts && p.prompt == prompt)
            .min_by_key(|p| (p.session_id != session, &p.session_id));
        if let Some(owner) = owner {
            conn.execute("UPDATE history SET session_id=?2,project=?3 WHERE id=?1 AND (session_id IS NOT ?2 OR project IS NOT ?3)",rusqlite::params![id,owner.session_id,owner.project])?;
        } else {
            conn.execute("DELETE FROM history WHERE id=?1", [id])?;
        }
    }
    Ok(())
}

pub(super) fn retire_deleted(conn: &Connection, path: &Path, session_ids: &[String]) -> Result<()> {
    let keep = serde_json::to_string(session_ids)?;
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
