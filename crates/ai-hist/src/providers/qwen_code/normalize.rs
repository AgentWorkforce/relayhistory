use super::*;
use crate::ingest::{
    insert_session_event, insert_tool_call, set_tool_call_error, upsert_session_rebuilt, EventRow,
    SessionCatalogRow, ToolCallRef, ToolResultFacts,
};
use crate::providers::file_history::{self, Keys};
use crate::{insert_history, insert_session_marker, HistoryEntry, NewSessionMarker};
pub(super) fn check_identity(conn: &Connection, sid: &str, path: &Path) -> Result<()> {
    file_history::check_identity(conn, ID, "QWEN_CODE", sid, path)
}
pub(super) fn write(conn: &Connection, payload: &Value) -> Result<usize> {
    let sid = format::string(payload, "sid")?;
    let path = Path::new(format::string(payload, "path")?);
    check_identity(conn, sid, path)?;
    let complete = payload["partial"] == false;
    let mut keys = Keys::default();
    markers(conn, sid, payload, &mut keys)?;
    let mut inserted = 0;
    let mut first = i64::MAX;
    let mut last = i64::MIN;
    let mut preview = None;
    let mut last_assistant = None;
    let mut cwd = None;
    let mut branch = None;
    for row in payload["active"]
        .as_array()
        .context("Qwen active chain missing")?
    {
        crate::ingest::check_capture_cancelled()?;
        let ts = format::time(row)?;
        first = first.min(ts);
        last = last.max(ts);
        cwd = row["cwd"].as_str();
        branch = row["gitBranch"].as_str();
        let (count, message) = message(conn, sid, row, &mut keys)?;
        inserted += count;
        if let Some((role, text)) = message {
            if role == "user" {
                preview.get_or_insert(text);
            } else {
                last_assistant = Some(text);
            }
        }
    }
    if complete {
        file_history::retire(conn, ID, sid, &keys)?;
        file_history::retire_locator_sessions(conn, ID, path, Some(sid))?;
    }
    let writer = if complete {
        upsert_session_rebuilt
    } else {
        crate::ingest::upsert_session
    };
    writer(
        conn,
        &SessionCatalogRow {
            source: ID,
            session_id: sid,
            cwd,
            git_branch: branch,
            first_ts: first,
            last_ts: last,
            last_assistant_text: complete.then_some(last_assistant.as_deref()).flatten(),
            raw_path: Some(&path.to_string_lossy()),
        },
    )?;
    if complete {
        // Native session metadata reads the latest raw title independently of branch selection.
        let title = payload["rows"]
            .as_array()
            .into_iter()
            .flatten()
            .rev()
            .find(|r| r["subtype"] == "custom_title")
            .and_then(|r| r["systemPayload"]["customTitle"].as_str())
            .filter(|s| !s.is_empty());
        conn.execute("UPDATE sessions SET first_prompt=?3,title=?4 WHERE source=?1 AND session_id=?2 AND (first_prompt IS NOT ?3 OR title IS NOT ?4)",rusqlite::params![ID,sid,preview,title])?;
    }
    Ok(inserted)
}

fn content(conn: &Connection, row: &Value, base: &EventRow<'_>, keys: &mut Keys) -> Result<bool> {
    let mut emitted = false;
    for (index, part) in row["message"]["parts"]
        .as_array()
        .into_iter()
        .flatten()
        .enumerate()
    {
        let uid = format!("{}:{index}", base.message_id);
        if let Some(text) = part["text"]
            .as_str()
            .filter(|_| base.role == "assistant" && part["thought"] != true)
        {
            insert_session_event(
                conn,
                &EventRow {
                    kind: "text",
                    text: Some(text),
                    event_uid: &uid,
                    ..*base
                },
            )?;
            keys.events.insert(uid);
            emitted = true;
            continue;
        }
        if let Some(call) = part
            .get("functionCall")
            .filter(|_| base.role == "assistant")
        {
            let (Some(id), Some(name)) = (
                call["id"].as_str().filter(|s| !s.is_empty()),
                call["name"].as_str().filter(|s| !s.is_empty()),
            ) else {
                continue;
            };
            let args = serde_json::to_string(&call["args"])?;
            insert_tool_call(
                conn,
                &ToolCallRef {
                    source: ID,
                    session_id: base.session_id,
                    message_id: base.message_id,
                    tool_use_id: id,
                    ts_ms: base.ts_ms,
                    cwd: base.cwd,
                    git_branch: base.git_branch,
                },
                name,
                None,
                &args,
                None,
            )?;
            keys.tools.insert(id.into());
            let facts = ToolResultFacts {
                tool_use_id: Some(id.into()),
                ..Default::default()
            };
            let raw = serde_json::to_string(call)?;
            insert_session_event(
                conn,
                &EventRow {
                    kind: "tool_use",
                    text: Some(&raw),
                    event_uid: &uid,
                    tool_result_facts: Some(&facts),
                    ..*base
                },
            )?;
            keys.events.insert(uid);
            emitted = true;
        } else if let Some(result) = part
            .get("functionResponse")
            .filter(|_| base.role == "tool_result")
        {
            let Some(id) = result["id"].as_str().filter(|s| !s.is_empty()) else {
                continue;
            };
            let raw = serde_json::to_string(&result["response"])?;
            let mut facts = ToolResultFacts::from_payload(&result["response"]);
            facts.tool_use_id = Some(id.into());
            let status = row["toolCallResult"]["status"].as_str();
            facts.result_status = match status {
                Some("success") => Some("completed".into()),
                Some("error") => Some("errored".into()),
                _ => None,
            };
            insert_session_event(
                conn,
                &EventRow {
                    kind: "tool_result",
                    text: Some(&raw),
                    event_uid: &uid,
                    tool_result_facts: Some(&facts),
                    ..*base
                },
            )?;
            if matches!(status, Some("success" | "error")) {
                set_tool_call_error(conn, ID, base.session_id, id, status == Some("error"))?;
                keys.results.insert(id.into());
            }
            keys.events.insert(uid);
            emitted = true;
        }
    }
    Ok(emitted)
}

fn message(
    conn: &Connection,
    sid: &str,
    row: &Value,
    keys: &mut Keys,
) -> Result<(usize, Option<(&'static str, String)>)> {
    let ts = format::time(row)?;
    let cwd = row["cwd"].as_str();
    let branch = row["gitBranch"].as_str();
    let role = match format::role(row) {
        Some(role) => role,
        None if row["type"] == "tool_result" && row["provenance"] == "tool_result" => "tool_result",
        _ => return Ok((0, None)),
    };
    let text = format::text(row);
    let id = format::string(row, "uuid")?;
    let usage = (role == "assistant")
        .then(|| row.get("usageMetadata"))
        .flatten()
        .map(serde_json::to_string)
        .transpose()?;
    let uid = format!("{id}:0");
    let base = EventRow {
        source: ID,
        session_id: sid,
        message_id: id,
        parent_id: row["parentUuid"].as_str(),
        ts_ms: ts,
        role,
        model: row["model"].as_str(),
        cwd,
        project: cwd,
        git_branch: branch,
        raw_kind: row["type"].as_str(),
        token_json: usage.as_deref(),
        ..Default::default()
    };
    let emitted = content(conn, row, &base, keys)?;
    if role == "assistant" && emitted {
        return Ok((0, (!text.is_empty()).then_some((role, text))));
    }
    if role == "tool_result" {
        return Ok((0, None));
    }
    // A usage-only assistant still owns its recorded counters; opaque parts
    // remain raw markers, never fabricated searchable prose.
    if text.is_empty() && usage.is_none() {
        return Ok((0, None));
    }
    insert_session_event(
        conn,
        &EventRow {
            source: ID,
            session_id: sid,
            message_id: id,
            parent_id: row["parentUuid"].as_str(),
            ts_ms: ts,
            role,
            kind: "text",
            text: (!text.is_empty()).then_some(text.as_str()),
            model: row["model"].as_str(),
            cwd,
            project: cwd,
            git_branch: branch,
            token_json: usage.as_deref(),
            event_uid: &uid,
            raw_kind: row["type"].as_str(),
            ..Default::default()
        },
    )?;
    keys.events.insert(uid);
    let inserted = if role == "user" && !text.is_empty() {
        let hash = crate::prompt_hash(&text);
        keys.prompts.insert(format!("{ts}:{hash}"));
        insert_history(
            conn,
            &HistoryEntry {
                id: 0,
                source: ID.into(),
                session_id: Some(sid.into()),
                project: cwd.map(str::to_owned),
                prompt: text.clone(),
                prompt_hash: Some(hash),
                timestamp_ms: ts,
            },
        )?
    } else {
        0
    };
    Ok((inserted, (!text.is_empty()).then_some((role, text))))
}

fn markers(conn: &Connection, sid: &str, payload: &Value, keys: &mut Keys) -> Result<()> {
    let mut counts = std::collections::BTreeMap::<&str, usize>::new();
    let rows = payload["rows"].as_array().context("Qwen records missing")?;
    for row in rows {
        let id = format::string(row, "uuid")?;
        let count = counts.entry(id).or_default();
        let uid = format!("raw:{id}:{count}");
        *count += 1;
        let raw = serde_json::to_string(&crate::ingest::bound_marker_value(row.clone()))?;
        insert_session_marker(
            conn,
            ID,
            sid,
            &NewSessionMarker {
                marker_uid: &uid,
                kind: row["subtype"]
                    .as_str()
                    .unwrap_or(format::string(row, "type")?),
                ts_ms: Some(format::time(row)?),
                payload_json: Some(&raw),
                ..Default::default()
            },
        )?;
        keys.markers.insert(uid);
    }
    Ok(())
}
