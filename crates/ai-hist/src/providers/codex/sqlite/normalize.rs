//! Normalize a captured Codex SQLite thread into the existing ledger.
use super::*;
use crate::ingest::{ToolResultFacts, ToolResultIndexer};
use sha2::{Digest, Sha256};
use std::io::{BufWriter, Write};

// Legacy rollouts lack native message IDs and may record the event a few seconds
// apart from the SQLite projection. Keep the compatibility window bounded to
// five seconds; exact text, role and one-to-one overlap markers also must match.
const IDLESS_ROLLOUT_TIMESTAMP_TOLERANCE_MS: i64 = 5_000;

fn content_hash(capture: &Capture) -> Result<String> {
    let t = &capture.thread;
    let content = json!({
        "items": capture.items, "turns": capture.turns, "edges": capture.edges,
        "source": t.source, "thread_source": t.thread_source, "cwd": t.cwd,
        "branch": t.branch, "model": t.model, "tokens_used": t.tokens_used,
    });
    let mut digest = Sha256::new();
    {
        let mut writer = BufWriter::new(&mut digest);
        serde_json::to_writer(&mut writer, &content)?;
        writer.flush()?;
    }
    Ok(format!("{:x}", digest.finalize()))
}

pub(crate) fn reconcile(
    conn: &Connection,
    capture: &Capture,
    state: &Path,
    include_related: bool,
) -> Result<usize> {
    let hash = content_hash(capture)?;
    retire_if_changed(conn, capture, include_related, &hash)?;
    normalize(conn, capture, state, include_related, &hash)
}

/// The selected stamp includes descendants. Keep this thread's row IDs when
/// only a child changed, while still replacing a rewritten projection.
fn retire_if_changed(
    conn: &Connection,
    capture: &Capture,
    include_related: bool,
    hash: &str,
) -> Result<()> {
    let prior: Option<String> = conn
        .query_row(
            "SELECT json_extract(payload_json, '$.content_hash') FROM session_markers \
             WHERE source='codex' AND session_id=?1 AND marker_uid='sqlite:thread'",
            [&capture.thread.id],
            |row| row.get(0),
        )
        .optional()?
        .flatten();
    if prior.as_deref() != Some(hash) {
        retire(conn, &capture.thread.id, include_related)?;
    } else if include_related {
        retire_relationships(conn, &capture.thread.id)?;
    }
    Ok(())
}

fn prior_history_ids(conn: &Connection, session_id: &str) -> Result<Vec<i64>> {
    let raw: Option<String> = conn
        .query_row(
            "SELECT json_extract(payload_json, '$.history_ids') FROM session_markers \
             WHERE source='codex' AND session_id=?1 AND marker_uid='sqlite:thread'",
            [session_id],
            |row| row.get(0),
        )
        .optional()?
        .flatten();
    raw.map(|raw| serde_json::from_str(&raw).map_err(Into::into))
        .transpose()
        .map(Option::unwrap_or_default)
}

/// A duplicate in history.jsonl claims the existing SQLite prompt row. The
/// log cursor will not replay it, so later projection retirement must keep it.
pub(crate) fn claim_history_log_prompt(conn: &Connection, entry: &HistoryEntry) -> Result<()> {
    let Some(session_id) = entry.session_id.as_deref() else {
        return Ok(());
    };
    let id: Option<i64> = conn
        .query_row(
            "SELECT id FROM history WHERE source='codex' AND session_id=?1 AND timestamp_ms=?2 AND prompt=?3",
            params![session_id, entry.timestamp_ms, entry.prompt],
            |row| row.get(0),
        )
        .optional()?;
    let Some(id) = id else {
        return Ok(());
    };
    let raw: Option<String> = conn.query_row(
        "SELECT payload_json FROM session_markers WHERE source='codex' AND session_id=?1 AND marker_uid='sqlite:thread'",
        [session_id],
        |row| row.get(0),
    ).optional()?;
    let Some(raw) = raw else {
        return Ok(());
    };
    let mut payload: Value = serde_json::from_str(&raw)?;
    let Some(ids) = payload.get_mut("history_ids").and_then(Value::as_array_mut) else {
        return Ok(());
    };
    let before = ids.len();
    ids.retain(|value| value.as_i64() != Some(id));
    if ids.len() != before {
        conn.execute(
            "UPDATE session_markers SET payload_json=?1 WHERE source='codex' AND session_id=?2 AND marker_uid='sqlite:thread'",
            params![payload.to_string(), session_id],
        )?;
    }
    Ok(())
}

fn thread_payload(capture: &Capture, history_ids: Vec<i64>, hash: &str) -> Value {
    let t = &capture.thread;
    let mut payload = json!({"archived":t.archived,"source":t.source,"name":t.name,"agent_nickname":t.agent_nickname,"agent_role":t.agent_role,"next_rollout_byte_offset":t.next_offset,"content_hash":hash});
    if !history_ids.is_empty() {
        payload["history_ids"] = json!(history_ids);
    }
    payload
}

fn marker(
    conn: &Connection,
    t: &Thread,
    uid: &str,
    ts: i64,
    kind: &str,
    payload: &Value,
    turn: Option<&str>,
) -> Result<()> {
    crate::insert_session_marker(
        conn,
        "codex",
        &t.id,
        &NewSessionMarker {
            marker_uid: uid,
            ts_ms: Some(ts),
            message_id: None,
            parent_id: None,
            turn_id: turn,
            kind,
            subkind: Some("sqlite"),
            text: None,
            payload_json: Some(&payload.to_string()),
        },
    )?;
    Ok(())
}

mod events;
use events::{event, text_event, Kind, Overlaps};

/// A changed projection replaces only rows that came from the previous
/// SQLite capture. The rollout and history.jsonl keep their own evidence.
pub(crate) fn retire(conn: &Connection, session_id: &str, relationships: bool) -> Result<()> {
    conn.execute(
        "DELETE FROM history WHERE source='codex' AND session_id=?1 \
         AND id IN (SELECT json_each.value \
         FROM session_markers, json_each(session_markers.payload_json, '$.history_ids') \
         WHERE source='codex' AND session_id=?1 \
         AND marker_uid='sqlite:thread' AND subkind='sqlite')",
        [session_id],
    )?;
    const NATIVE_EVENTS: &str = "source='codex' AND session_id=?1 \
        AND ((raw_kind IN ('userMessage','agentMessage','reasoning','commandExecution', \
        'mcpToolCall','collabAgentToolCall','webSearch','fileChange') \
        AND (event_uid=message_id OR event_uid=message_id||':result')) \
        OR (raw_kind='input_image' AND event_uid LIKE message_id||':input_image:%'))";
    for table in ["tool_calls", "file_edits"] {
        conn.execute(
            &format!(
                "DELETE FROM {table} WHERE source='codex' AND session_id=?1 \
                 AND message_id IN (SELECT message_id FROM session_events WHERE {NATIVE_EVENTS})"
            ),
            [session_id],
        )?;
    }
    conn.execute(
        &format!("DELETE FROM session_events WHERE {NATIVE_EVENTS}"),
        [session_id],
    )?;
    conn.execute(
        "DELETE FROM session_markers WHERE source='codex' AND session_id=?1 AND subkind='sqlite'",
        [session_id],
    )?;
    if relationships {
        retire_relationships(conn, session_id)?;
    }
    Ok(())
}

fn retire_relationships(conn: &Connection, session_id: &str) -> Result<()> {
    conn.execute(
        "DELETE FROM session_relationships WHERE source='codex' AND \
         ((parent_session_id=?1 AND evidence_kind IN ('codex_sqlite_item','codex_thread_spawn_edge')) \
         OR (child_session_id=?1 AND evidence_kind IN ('codex_thread_spawn_edge','codex_sqlite_thread_source')))",
        [session_id],
    )?;
    Ok(())
}

fn text_blocks(value: &Value) -> String {
    value
        .as_array()
        .map(|blocks| {
            blocks
                .iter()
                .filter_map(|b| b.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default()
}

fn child_edge(conn: &Connection, t: &Thread, child: &str, item: &Item) -> Result<()> {
    crate::ingest::record_relationship(
        conn,
        &crate::ingest::ObservedRelationship {
            source: "codex",
            parent_session_id: &t.id,
            child_session_id: Some(child),
            relationship: "delegated",
            evidence_kind: "codex_sqlite_item",
            evidence_ref: Some(&item.id),
            child_has_events: child_has_events(conn, child)?,
            spawned_at_ms: Some(item.ts),
            ..Default::default()
        },
    )
}

fn child_has_events(conn: &Connection, child: &str) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM session_events WHERE source='codex' AND session_id=?1)",
        [child],
        |row| row.get(0),
    )?)
}

fn normalize_relationships(
    conn: &Connection,
    capture: &Capture,
    state: &Path,
    session_locator: &str,
) -> Result<()> {
    let t = &capture.thread;
    for edge in &capture.edges {
        let parent_locator = locator(state, &edge.parent);
        crate::ingest::record_relationship(
            conn,
            &crate::ingest::ObservedRelationship {
                source: "codex",
                parent_session_id: &edge.parent,
                child_session_id: Some(&edge.child),
                relationship: "delegated",
                evidence_kind: "codex_thread_spawn_edge",
                evidence_locator: Some(&parent_locator),
                evidence_ref: edge.status.as_deref(),
                child_has_events: child_has_events(conn, &edge.child)?,
                spawned_at_ms: (edge.child == t.id).then_some(t.created),
                ..Default::default()
            },
        )?;
    }
    if let Some(source) = t
        .source
        .as_deref()
        .and_then(|s| serde_json::from_str::<Value>(s).ok())
    {
        if let Some(parent) = source
            .pointer("/subagent/thread_spawn/parent_thread_id")
            .and_then(Value::as_str)
        {
            crate::ingest::record_relationship(
                conn,
                &crate::ingest::ObservedRelationship {
                    source: "codex",
                    parent_session_id: parent,
                    child_session_id: Some(&t.id),
                    relationship: "delegated",
                    evidence_kind: "codex_sqlite_thread_source",
                    evidence_locator: Some(session_locator),
                    child_has_events: child_has_events(conn, &t.id)?,
                    spawned_at_ms: Some(t.created),
                    ..Default::default()
                },
            )?;
        }
    }
    Ok(())
}

mod items;
use items::normalize_item;

fn normalize(
    conn: &Connection,
    capture: &Capture,
    state: &Path,
    include_related: bool,
    hash: &str,
) -> Result<usize> {
    let t = &capture.thread;
    let session_locator = locator(state, &t.id);
    if !t.is_child() {
        let last_answer = capture
            .items
            .iter()
            .rev()
            .find(|item| item.kind == "agentMessage")
            .and_then(|item| item.value["text"].as_str());
        crate::ingest::upsert_session(
            conn,
            &SessionCatalogRow {
                source: "codex",
                session_id: &t.id,
                cwd: t.cwd.as_deref(),
                git_branch: t.branch.as_deref(),
                first_ts: t.created,
                last_ts: t.updated,
                last_assistant_text: last_answer,
                raw_path: Some(&session_locator),
            },
        )?;
        crate::discover::upsert_shallow_session(
            conn,
            &ShallowSession {
                source: "codex".into(),
                session_id: t.id.clone(),
                title: t.name.clone().or(t.title.clone()),
                models: t.model.iter().cloned().collect(),
                originator: t.originator.clone(),
                agent_version: t.version.clone(),
                repo_url: t.repo.clone(),
                discovery_state: "full".into(),
                ..Default::default()
            },
        )?;
    }
    let mut prompts = 0;
    let mut history_ids = prior_history_ids(conn, &t.id)?;
    let mut result_indexer = ToolResultIndexer::default();
    let mut overlaps = Overlaps::load(conn, &t.id)?;
    for item in &capture.items {
        prompts += normalize_item(
            conn,
            t,
            item,
            &mut result_indexer,
            include_related,
            &mut history_ids,
            &mut overlaps,
        )?;
    }
    for turn in &capture.turns {
        if let Some(error) = &turn.error {
            marker(
                conn,
                t,
                &format!("turn:{}:error", turn.id),
                turn.ts.unwrap_or(t.updated),
                "turn_error",
                &serde_json::from_str::<Value>(error).unwrap_or_else(|_| json!({"raw":error})),
                Some(&turn.id),
            )?;
        }
    }
    if let Some(tokens) = t.tokens_used {
        marker(
            conn,
            t,
            "sqlite:tokens_used",
            t.updated,
            "usage_snapshot",
            &json!({"tokens_used":tokens,"accounting":"cumulative"}),
            None,
        )?;
    }
    let thread_payload = thread_payload(capture, history_ids, hash);
    marker(
        conn,
        t,
        "sqlite:thread",
        t.created,
        "session_metadata",
        &thread_payload,
        None,
    )?;
    if include_related {
        normalize_relationships(conn, capture, state, &session_locator)?;
    }
    if t.is_child() {
        crate::ingest::cleanup_codex_subagent_registration(conn, &t.id)?;
    }
    Ok(prompts)
}

#[cfg(test)]
mod tests;
