use super::*;
use crate::ingest::{
    insert_session_event, insert_tool_call, set_tool_call_error, upsert_session_rebuilt, EventRow,
    RawMessageFacts, SessionCatalogRow, ToolCallRef, ToolResultFacts,
};
use crate::{insert_history, insert_session_marker, HistoryEntry, NewSessionMarker};
use rusqlite::OptionalExtension;
use std::collections::BTreeSet;
#[derive(Default)]
struct Keys {
    events: BTreeSet<String>,
    tools: BTreeSet<String>,
    results: BTreeSet<String>,
    markers: BTreeSet<String>,
    prompts: BTreeSet<String>,
}
pub(super) fn check_identity(conn: &Connection, sid: &str, path: &Path) -> Result<()> {
    let old: Option<String> = conn
        .query_row(
            "SELECT raw_path FROM sessions WHERE source = 'pi' AND session_id = ?1",
            [sid],
            |r| r.get(0),
        )
        .optional()?
        .flatten();
    if let Some(old) = old {
        let same = Path::new(&old) == path
            || std::fs::canonicalize(&old)
                .ok()
                .zip(std::fs::canonicalize(path).ok())
                .is_some_and(|(a, b)| a == b);
        anyhow::ensure!(
            same || !Path::new(&old).exists(),
            "PI_ID_COLLISION: native session {sid:?} at {} conflicts with {old}",
            path.display()
        );
    }
    Ok(())
}
pub(super) fn write(conn: &Connection, payload: &Value, path: &Path) -> Result<usize> {
    let entries = payload["entries"]
        .as_array()
        .context("Pi entries missing")?;
    let header = &entries[0];
    let sid = format::string(header, "id")?;
    check_identity(conn, sid, path)?;
    let cwd = header["cwd"].as_str();
    let first = format::time(header)?;
    let mut last = first;
    let mut assistant = None;
    let mut keys = Keys::default();
    let mut inserted = 0;
    marker(conn, sid, header, "header", &mut keys)?;
    for entry in &entries[1..] {
        crate::ingest::check_capture_cancelled()?;
        last = last.max(format::time(entry)?);
        if entry["type"] == "message" {
            inserted += message(conn, sid, cwd, entry, &mut keys)?;
            if entry["message"]["role"] == "assistant" {
                let text = format::text(&entry["message"]["content"]);
                if !text.trim().is_empty() {
                    assistant = Some(text);
                }
            }
        } else {
            marker(conn, sid, entry, format::string(entry, "id")?, &mut keys)?;
        }
    }
    let complete = payload["tail"].as_array().is_some_and(Vec::is_empty);
    if complete {
        retire(conn, sid, &keys)?;
    }
    let catalog_writer = if complete {
        upsert_session_rebuilt
    } else {
        crate::ingest::upsert_session
    };
    catalog_writer(
        conn,
        &SessionCatalogRow {
            source: ID,
            session_id: sid,
            cwd,
            git_branch: None,
            first_ts: first,
            last_ts: last,
            last_assistant_text: complete.then_some(assistant.as_deref()).flatten(),
            raw_path: Some(&path.to_string_lossy()),
        },
    )?;
    let title = entries
        .iter()
        .rev()
        .find(|e| e["type"] == "session_info")
        .and_then(|e| e["name"].as_str());
    if complete {
        crate::ingest::set_session_title(conn, ID, sid, title)?;
    }
    Ok(inserted)
}
fn marker(conn: &Connection, sid: &str, entry: &Value, id: &str, keys: &mut Keys) -> Result<()> {
    let kind = format::string(entry, "type")?;
    let payload = serde_json::to_string(&crate::ingest::bound_marker_value(entry.clone()))?;
    insert_session_marker(
        conn,
        ID,
        sid,
        &NewSessionMarker {
            marker_uid: id,
            kind,
            ts_ms: Some(format::time(entry)?),
            parent_id: entry["parentId"].as_str(),
            message_id: Some(id),
            text: entry["summary"].as_str(),
            payload_json: Some(&payload),
            ..Default::default()
        },
    )?;
    keys.markers.insert(id.into());
    Ok(())
}
fn message(
    conn: &Connection,
    sid: &str,
    cwd: Option<&str>,
    entry: &Value,
    keys: &mut Keys,
) -> Result<usize> {
    let msg = &entry["message"];
    let role = format::string(msg, "role")?;
    if !["user", "assistant", "toolResult"].contains(&role) {
        marker(conn, sid, entry, format::string(entry, "id")?, keys)?;
        return Ok(0);
    }
    let mid = format::string(entry, "id")?;
    let ts = match msg.get("timestamp") {
        Some(value) => value
            .as_i64()
            .context("PI_INVALID_RECORD: message timestamp must be integer milliseconds")?,
        None => format::time(entry)?,
    };
    let token = (role == "assistant")
        .then(|| msg.get("usage"))
        .flatten()
        .map(serde_json::to_string)
        .transpose()?;
    let base = EventRow {
        source: ID,
        session_id: sid,
        message_id: mid,
        parent_id: entry["parentId"].as_str(),
        cwd,
        project: cwd,
        ts_ms: ts,
        role: if role == "toolResult" {
            "tool_result"
        } else {
            role
        },
        model: msg["model"].as_str(),
        provider: msg["provider"].as_str(),
        token_json: token.as_deref(),
        raw_kind: Some("message"),
        raw_facts: RawMessageFacts {
            stop_reason: msg["stopReason"].as_str(),
            ..Default::default()
        },
        ..Default::default()
    };
    if role == "toolResult" {
        result(conn, &base, msg, keys)?;
    } else {
        content(conn, &base, &msg["content"], keys)?;
    }
    if role != "user" {
        return Ok(0);
    }
    let text = format::text(&msg["content"]);
    if text.is_empty() {
        return Ok(0);
    }
    let hash = crate::prompt_hash(&text);
    keys.prompts.insert(format!("{ts}:{hash}"));
    insert_history(
        conn,
        &HistoryEntry {
            id: 0,
            source: ID.into(),
            session_id: Some(sid.into()),
            project: cwd.map(str::to_owned),
            prompt: text,
            prompt_hash: Some(hash),
            timestamp_ms: ts,
        },
    )
}
fn content(conn: &Connection, base: &EventRow<'_>, value: &Value, keys: &mut Keys) -> Result<()> {
    let blocks = value
        .as_array()
        .cloned()
        .unwrap_or_else(|| vec![serde_json::json!({"type":"text", "text":value})]);
    for (index, block) in blocks.iter().enumerate() {
        let uid = format!("{}:{index}", base.message_id);
        let kind = block["type"].as_str().unwrap_or("unknown");
        let (kind, text) = match kind {
            "text" => ("text", block["text"].as_str().map(str::to_owned)),
            "thinking" => ("thinking", block["thinking"].as_str().map(str::to_owned)),
            "toolCall" => {
                call(conn, base, block, keys)?;
                ("tool_use", Some(serde_json::to_string(block)?))
            }
            _ => {
                opaque_block(conn, base, block, &uid, keys)?;
                continue;
            }
        };
        let facts = ToolResultFacts {
            tool_use_id: (kind == "tool_use").then(|| block["id"].as_str().unwrap().to_owned()),
            ..Default::default()
        };
        insert_session_event(
            conn,
            &EventRow {
                event_uid: &uid,
                kind,
                tool_result_facts: Some(&facts),
                text: text.as_deref(),
                ..*base
            },
        )?;
        keys.events.insert(uid);
    }
    // Empty assistant responses can still contain measured usage.
    if blocks.is_empty() {
        let uid = format!("{}:0", base.message_id);
        insert_session_event(
            conn,
            &EventRow {
                event_uid: &uid,
                kind: "text",
                ..*base
            },
        )?;
        keys.events.insert(uid);
    }
    Ok(())
}
fn opaque_block(
    conn: &Connection,
    base: &EventRow<'_>,
    block: &Value,
    uid: &str,
    keys: &mut Keys,
) -> Result<()> {
    let payload = serde_json::to_string(&crate::ingest::bound_marker_value(block.clone()))?;
    insert_session_marker(
        conn,
        ID,
        base.session_id,
        &NewSessionMarker {
            marker_uid: uid,
            kind: "unknown",
            subkind: block["type"].as_str(),
            ts_ms: Some(base.ts_ms),
            message_id: Some(base.message_id),
            parent_id: base.parent_id,
            payload_json: Some(&payload),
            ..Default::default()
        },
    )?;
    keys.markers.insert(uid.into());
    Ok(())
}
fn call(conn: &Connection, base: &EventRow<'_>, block: &Value, keys: &mut Keys) -> Result<()> {
    let id = format::string(block, "id")?;
    let name = format::string(block, "name")?;
    let args = serde_json::to_string(&block["arguments"])?;
    insert_tool_call(
        conn,
        &ToolCallRef {
            source: ID,
            session_id: base.session_id,
            message_id: base.message_id,
            tool_use_id: id,
            ts_ms: base.ts_ms,
            cwd: base.cwd,
            ..Default::default()
        },
        name,
        block["arguments"]["path"].as_str(),
        &args,
        None,
    )?;
    keys.tools.insert(id.into());
    Ok(())
}
fn result(conn: &Connection, base: &EventRow<'_>, msg: &Value, keys: &mut Keys) -> Result<()> {
    let id = format::string(msg, "toolCallId")?;
    let error = msg["isError"]
        .as_bool()
        .context("PI_INVALID_RECORD: tool isError")?;
    let text = format::text(&msg["content"]);
    let mut facts = ToolResultFacts::from_payload(&msg["content"]);
    facts.tool_use_id = Some(id.into());
    facts.result_status = Some(if error { "errored" } else { "completed" }.into());
    facts.error_signal = error.then(|| "is_error".into());
    let uid = format!("{}:0", base.message_id);
    insert_session_event(
        conn,
        &EventRow {
            event_uid: &uid,
            kind: "tool_result",
            text: Some(&text),
            tool_result_facts: Some(&facts),
            ..*base
        },
    )?;
    set_tool_call_error(conn, ID, base.session_id, id, error)?;
    keys.results.insert(id.into());
    keys.events.insert(uid);
    Ok(())
}
fn retire(conn: &Connection, sid: &str, keys: &Keys) -> Result<()> {
    let prompts = serde_json::to_string(&keys.prompts)?;
    let results = serde_json::to_string(&keys.results)?;
    conn.execute("UPDATE tool_calls SET is_error = NULL WHERE source = 'pi' AND session_id = ?1 AND location = 'local' AND is_error IS NOT NULL AND tool_use_id NOT IN (SELECT value FROM json_each(?2))", rusqlite::params![sid, results])?;

    for (table, key, present) in [
        ("session_events", "event_uid", &keys.events),
        ("tool_calls", "tool_use_id", &keys.tools),
        ("session_markers", "marker_uid", &keys.markers),
    ] {
        let condition = format!(
            "source = 'pi' AND session_id = ?1 AND {key} NOT IN (SELECT value FROM json_each(?2))"
        );
        let keep = serde_json::to_string(present)?;
        crate::store::retire_evidence_share(
            conn,
            table,
            &condition,
            &[&sid, &keep],
            crate::SessionLocation::Local,
        )?;
    }
    reconcile_prompts(conn, sid, &prompts)?;
    Ok(())
}

// Reconcile after local shares retire: surviving remote events in this session
// and other sessions still support prompt rows. Compare complete ordered user
// text projections, and prefer the existing owner when its evidence survives.
fn reconcile_prompts(conn: &Connection, sid: &str, keep: &str) -> Result<()> {
    let reference = "SELECT e.session_id FROM session_events e \
        WHERE e.source = 'pi' AND e.role = 'user' \
          AND e.kind = 'text' AND e.ts_ms = history.timestamp_ms \
        GROUP BY e.session_id, e.message_id \
        HAVING (SELECT group_concat(text, char(10)) FROM \
          (SELECT text FROM session_events b WHERE b.source = 'pi' \
           AND b.session_id = e.session_id AND b.message_id = e.message_id \
           AND b.role = 'user' AND b.kind = 'text' \
           ORDER BY CAST(substr(b.event_uid, length(b.message_id) + 2) AS INTEGER))) = history.prompt \
        ORDER BY (e.session_id = ?1) DESC, e.session_id LIMIT 1";
    let project = reference.replacen("SELECT e.session_id", "SELECT e.project", 1);
    conn.execute(&format!("UPDATE history SET project = ({project}), session_id = ({reference}) \
        WHERE source = 'pi' AND session_id = ?1 \
        AND (timestamp_ms || ':' || coalesce(prompt_hash, '')) NOT IN (SELECT value FROM json_each(?2)) \
        AND EXISTS ({reference}) AND session_id IS NOT ({reference})"), rusqlite::params![sid, keep])?;
    conn.execute(&format!("DELETE FROM history WHERE source = 'pi' AND session_id = ?1 \
        AND (timestamp_ms || ':' || coalesce(prompt_hash, '')) NOT IN (SELECT value FROM json_each(?2)) \
        AND NOT EXISTS ({reference})"), rusqlite::params![sid, keep])?;
    Ok(())
}
