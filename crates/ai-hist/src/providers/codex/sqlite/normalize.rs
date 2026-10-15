//! Normalize a captured Codex SQLite thread into the existing ledger.
use super::*;
use crate::ingest::{ToolResultFacts, ToolResultIndexer};

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

fn event(
    conn: &Connection,
    t: &Thread,
    item: &Item,
    uid: &str,
    role: &str,
    kind: &str,
    text: Option<&str>,
    result_facts: Option<&ToolResultFacts>,
) -> Result<bool> {
    let rollout_has_item = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM session_events WHERE source='codex' AND session_id=?1 AND event_uid<>?2 AND role=?3 AND kind=?4 AND (message_id=?5 OR provider_message_id=?5))",
        params![t.id, uid, role, kind, item.id],
        |r| r.get::<_, bool>(0),
    )?;
    if rollout_has_item {
        return Ok(false);
    }
    crate::ingest::insert_session_event(
        conn,
        &EventRow {
            source: "codex",
            session_id: &t.id,
            project: t.cwd.as_deref(),
            cwd: t.cwd.as_deref(),
            git_branch: t.branch.as_deref(),
            message_id: &item.id,
            event_uid: uid,
            ts_ms: item.ts,
            role,
            kind,
            text,
            model: t.model.as_deref(),
            raw_kind: Some(&item.kind),
            tool_result_facts: result_facts,
            raw_facts: RawMessageFacts {
                turn_id: item.turn_id.as_deref(),
                ..Default::default()
            },
            ..Default::default()
        },
    )?;
    Ok(true)
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

mod items;
use items::normalize_item;

pub(crate) fn normalize(conn: &Connection, capture: &Capture, state: &Path) -> Result<usize> {
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
    let mut result_indexer = ToolResultIndexer::default();
    for item in &capture.items {
        prompts += normalize_item(conn, t, item, &mut result_indexer)?;
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
    marker(
        conn,
        t,
        "sqlite:thread",
        t.created,
        "session_metadata",
        &json!({"archived":t.archived,"source":t.source,"name":t.name,"agent_nickname":t.agent_nickname,"agent_role":t.agent_role}),
        None,
    )?;
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
                    evidence_locator: Some(&session_locator),
                    child_has_events: child_has_events(conn, &t.id)?,
                    spawned_at_ms: Some(t.created),
                    ..Default::default()
                },
            )?;
        }
    }
    if t.is_child() {
        crate::ingest::cleanup_codex_subagent_registration(conn, &t.id)?;
    }
    Ok(prompts)
}
