use super::*;
use crate::ingest::{
    insert_session_event, insert_tool_call, upsert_session_rebuilt, EventRow, SessionCatalogRow,
    ToolCallRef, ToolResultFacts,
};
use crate::providers::file_history::{self, Keys};
use crate::providers::shared_evidence::Snapshot;
use crate::{insert_history, insert_session_marker, HistoryEntry, NewSessionMarker};

pub(super) fn check_identity(conn: &Connection, sid: &str, path: &Path) -> Result<()> {
    file_history::check_identity(conn, ID, "VIBE", sid, path)
}

fn timestamp(entry: &Value) -> Result<i64> {
    entry["createdAt"]
        .as_i64()
        .context("VIBE_INVALID_ENTRY: missing createdAt")
}

fn text(entry: &Value) -> String {
    entry["content"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|block| block["type"] == "text")
        .filter_map(|block| block["text"].as_str())
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn event(
    conn: &Connection,
    sid: &str,
    cwd: Option<&str>,
    entry: &Value,
    keys: &mut Keys,
    events_only: bool,
) -> Result<Option<(i64, String)>> {
    let role = entry["role"].as_str().context("VIBE_INVALID_ENTRY: role")?;
    anyhow::ensure!(
        matches!(role, "user" | "assistant" | "system"),
        "VIBE_INVALID_ENTRY: role"
    );
    if role == "system" {
        return Ok(None);
    }
    let mid = entry["id"]
        .as_str()
        .filter(|id| !id.is_empty())
        .context("VIBE_INVALID_ENTRY: id")?;
    let ts = timestamp(entry)?;
    let content = text(entry);
    let uid = format!("{mid}:0");
    insert_session_event(
        conn,
        &EventRow {
            source: ID,
            session_id: sid,
            event_uid: &uid,
            message_id: mid,
            parent_id: entry["inputEntryId"].as_str(),
            cwd,
            project: cwd,
            ts_ms: ts,
            role,
            kind: "text",
            text: Some(&content),
            raw_kind: Some("message"),
            ..Default::default()
        },
    )?;
    keys.events.insert(uid.clone());
    if !events_only && role == "user" && !content.trim().is_empty() {
        let hash = crate::prompt_hash(&content);
        keys.prompts.insert(format!("{ts}:{hash}"));
        insert_history(
            conn,
            &HistoryEntry {
                id: 0,
                source: ID.into(),
                session_id: Some(sid.into()),
                project: cwd.map(str::to_owned),
                prompt: content,
                prompt_hash: Some(hash),
                timestamp_ms: ts,
            },
        )?;
    }
    Ok(Some((ts, uid)))
}

fn marker(conn: &Connection, sid: &str, entry: &Value, keys: &mut Keys) -> Result<()> {
    let id = entry["id"]
        .as_str()
        .filter(|id| !id.is_empty())
        .context("VIBE_INVALID_MARKER_ID")?;
    let uid = format!("marker:{id}");
    let payload = serde_json::to_string(&crate::ingest::bound_marker_value(entry.clone()))?;
    insert_session_marker(
        conn,
        ID,
        sid,
        &NewSessionMarker {
            marker_uid: &uid,
            kind: entry["type"].as_str().unwrap_or("unknown"),
            ts_ms: entry["createdAt"].as_i64(),
            message_id: entry["id"].as_str(),
            parent_id: entry["relatedEntryId"].as_str(),
            text: entry["text"].as_str().or_else(|| entry["message"].as_str()),
            payload_json: Some(&payload),
            ..Default::default()
        },
    )?;
    keys.markers.insert(uid);
    Ok(())
}

fn effect(
    conn: &Connection,
    sid: &str,
    cwd: Option<&str>,
    entry: &Value,
    keys: &mut Keys,
    order: &mut Vec<(i64, String)>,
    replay: Option<&std::collections::BTreeSet<i64>>,
) -> Result<()> {
    // The public projector gives a tool effect one stable ID, then replaces
    // that same entry as it runs and settles. It is the exact call/result join.
    let id = entry["id"]
        .as_str()
        .filter(|id| id.starts_with("effect-"))
        .context("VIBE_INVALID_EFFECT_ID")?;
    let name = entry["detail"]["toolName"]
        .as_str()
        .or_else(|| entry["title"].as_str())
        .context("VIBE_INVALID_EFFECT_NAME")?;
    let ts = timestamp(entry)?;
    let status = entry["state"]["status"]
        .as_str()
        .context("VIBE_INVALID_EFFECT_STATUS")?;
    let error = match status {
        "completed" => Some(false),
        "failed" => Some(true),
        "running" | "skipped" | "cancelled" => None,
        _ => anyhow::bail!("VIBE_UNKNOWN_EFFECT_STATUS"),
    };
    if replay.is_none() {
        let args = serde_json::to_string(&entry["detail"]["input"])?;
        insert_tool_call(
            conn,
            &ToolCallRef {
                source: ID,
                session_id: sid,
                message_id: id,
                tool_use_id: id,
                ts_ms: ts,
                cwd,
                ..Default::default()
            },
            name,
            None,
            &args,
            error,
        )?;
        keys.tools.insert(id.into());
    }
    let facts = ToolResultFacts {
        tool_use_id: Some(id.into()),
        ..Default::default()
    };
    let uid = format!("{id}:call");
    if replay.is_none_or(|buckets| buckets.contains(&ts)) {
        insert_session_event(
            conn,
            &EventRow {
                source: ID,
                session_id: sid,
                event_uid: &uid,
                message_id: id,
                parent_id: entry["inputEntryId"].as_str(),
                cwd,
                project: cwd,
                ts_ms: ts,
                role: "assistant",
                kind: "tool_use",
                text: Some(name),
                raw_kind: Some("effect"),
                tool_result_facts: Some(&facts),
                ..Default::default()
            },
        )?;
        keys.events.insert(uid.clone());
        order.push((ts, uid));
    }
    if let Some(error) = error {
        keys.results.insert(id.into());
        let output = entry["state"]["outputText"]
            .as_str()
            .or_else(|| entry["state"]["error"]["message"].as_str())
            .or_else(|| entry["state"]["reason"].as_str())
            .unwrap_or("");
        let mut facts = ToolResultFacts::from_payload(&entry["state"]["output"]);
        facts.tool_use_id = Some(id.into());
        facts.result_status = Some(if error { "errored" } else { "completed" }.into());
        if error {
            facts.error_signal = Some("tool_status".into());
        }
        let uid = format!("{id}:result");
        let result_ts = entry["updatedAt"].as_i64().unwrap_or(ts);
        if replay.is_none_or(|buckets| buckets.contains(&result_ts)) {
            insert_session_event(
                conn,
                &EventRow {
                    source: ID,
                    session_id: sid,
                    event_uid: &uid,
                    message_id: id,
                    parent_id: entry["inputEntryId"].as_str(),
                    cwd,
                    project: cwd,
                    ts_ms: result_ts,
                    role: "tool_result",
                    kind: "tool_result",
                    text: Some(output),
                    raw_kind: Some("effect"),
                    tool_result_facts: Some(&facts),
                    ..Default::default()
                },
            )?;
            keys.events.insert(uid.clone());
            order.push((result_ts, uid));
        }
    }
    Ok(())
}

pub(super) fn write(conn: &Connection, payload: &Value) -> Result<usize> {
    let sid = payload["sid"]
        .as_str()
        .context("VIBE_INVALID_CAPTURE: sid")?;
    let path = Path::new(
        payload["path"]
            .as_str()
            .context("VIBE_INVALID_CAPTURE: path")?,
    );
    check_identity(conn, sid, path)?;
    if payload["partial"] == true {
        return Ok(0);
    }
    let state = &payload["state"];
    let entries = state["history"]["entries"]
        .as_array()
        .context("VIBE_INVALID_HISTORY")?;
    let cwd = payload["runtime"]["session_metadata"]["cwd"].as_str();
    let title = state["session"]["title"].as_str();
    let shared = Snapshot::capture(conn, ID, sid)?;
    let mut keys = Keys::default();
    let mut order = Vec::new();
    let mut first_prompt = None;
    let mut last_assistant = None;
    let mut first = state["session"]["createdAt"].as_i64().unwrap_or(i64::MAX);
    let mut last = state["session"]["updatedAt"].as_i64().unwrap_or(i64::MIN);
    let mut prompts = 0;
    let mut native_ids = std::collections::HashSet::new();
    for entry in entries {
        let id = entry["id"]
            .as_str()
            .filter(|id| !id.is_empty())
            .context("VIBE_INVALID_ENTRY_ID")?;
        anyhow::ensure!(native_ids.insert(id), "VIBE_DUPLICATE_ENTRY_ID");
        crate::ingest::check_capture_cancelled()?;
        if let Some(ts) = entry["createdAt"].as_i64() {
            first = first.min(ts);
            last = last.max(ts);
        }
        if let Some(ts) = entry["updatedAt"].as_i64() {
            last = last.max(ts);
        }
        if entry["type"] == "message" {
            if let Some(desired) = event(conn, sid, cwd, entry, &mut keys, false)? {
                order.push(desired);
            }
            let content = text(entry);
            if entry["role"] == "user" && !content.trim().is_empty() {
                prompts += 1;
                if first_prompt.is_none() {
                    first_prompt = Some(content.chars().take(4096).collect::<String>());
                }
            } else if entry["role"] == "assistant" && !content.trim().is_empty() {
                last_assistant = Some(content);
            }
        } else if entry["type"] == "effect" && entry["detail"]["kind"] == "tool" {
            effect(conn, sid, cwd, entry, &mut keys, &mut order, None)?;
            if matches!(
                entry["state"]["status"].as_str(),
                Some("skipped" | "cancelled")
            ) {
                marker(conn, sid, entry, &mut keys)?;
            }
        } else {
            marker(conn, sid, entry, &mut keys)?;
        }
    }
    file_history::retire(conn, ID, sid, &keys)?;
    file_history::retire_locator_sessions(conn, ID, path, Some(sid))?;
    let reordered = file_history::reconcile_event_order(conn, ID, sid, &order)?;
    for entry in entries {
        if entry["type"] == "message"
            && entry["createdAt"]
                .as_i64()
                .is_some_and(|ts| reordered.contains(&ts))
        {
            event(conn, sid, cwd, entry, &mut keys, true)?;
        } else if entry["type"] == "effect"
            && entry["detail"]["kind"] == "tool"
            && (entry["createdAt"]
                .as_i64()
                .is_some_and(|ts| reordered.contains(&ts))
                || entry["updatedAt"]
                    .as_i64()
                    .is_some_and(|ts| reordered.contains(&ts)))
        {
            effect(
                conn,
                sid,
                cwd,
                entry,
                &mut keys,
                &mut order,
                Some(&reordered),
            )?;
        }
    }
    upsert_session_rebuilt(
        conn,
        &SessionCatalogRow {
            source: ID,
            session_id: sid,
            cwd,
            git_branch: None,
            first_ts: (first != i64::MAX).then_some(first).unwrap_or(0),
            last_ts: (last != i64::MIN).then_some(last).unwrap_or(0),
            last_assistant_text: last_assistant.as_deref(),
            raw_path: Some(&path.to_string_lossy()),
        },
    )?;
    conn.execute("UPDATE sessions SET first_prompt=?3 WHERE source=?1 AND session_id=?2 AND first_prompt IS NOT ?3",
        rusqlite::params![ID, sid, first_prompt])?;
    crate::ingest::titles::set_session_title(conn, ID, sid, title)?;
    shared.ensure_unchanged(conn, sid)?;
    Ok(prompts)
}
