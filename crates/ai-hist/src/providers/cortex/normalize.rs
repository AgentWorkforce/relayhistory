use super::*;
use crate::ingest::{
    insert_session_event, insert_tool_call, set_tool_call_error, upsert_session_rebuilt, EventRow,
    RawMessageFacts, SessionCatalogRow, ToolCallRef, ToolResultFacts,
};
use crate::providers::file_history::{self, Keys};
use crate::providers::shared_evidence::Snapshot;
use crate::{insert_history, insert_session_marker, HistoryEntry, NewSessionMarker};
pub(super) fn check_identity(conn: &Connection, sid: &str, path: &Path) -> Result<()> {
    file_history::check_identity(conn, ID, "CORTEX", sid, path)
}
pub(super) fn write(conn: &Connection, payload: &Value, path: &Path) -> Result<usize> {
    anyhow::ensure!(
        payload["tail"].as_array().is_some_and(Vec::is_empty),
        "CORTEX_PARTIAL_TAIL: incomplete final record; prior evidence retained"
    );
    let entries = payload["entries"]
        .as_array()
        .context("Cortex entries missing")?;
    let meta = &payload["metadata"];
    let sid = format::string(meta, "session_id")?;
    check_identity(conn, sid, path)?;
    let shared = Snapshot::capture(conn, ID, sid)?;
    let cwd = meta["working_directory"].as_str();
    let mut keys = Keys::default();
    let written = if entries.is_empty() {
        WrittenRows {
            inserted: 0,
            last: format::created_at(meta)?,
            assistant: None,
        }
    } else {
        write_rows(conn, sid, cwd, entries, &mut keys)?
    };
    file_history::retire(conn, ID, sid, &keys)?;
    reconcile_complete(conn, sid, cwd, entries)?;
    write_catalog(conn, sid, cwd, path, entries, meta, &written)?;
    shared.ensure_unchanged(conn, sid)?;
    file_history::retire_locator_sessions(conn, ID, path, Some(sid))?;
    Ok(written.inserted)
}

struct WrittenRows {
    inserted: usize,
    last: i64,
    assistant: Option<String>,
}

fn write_rows(
    conn: &Connection,
    sid: &str,
    cwd: Option<&str>,
    entries: &[Value],
    keys: &mut Keys,
) -> Result<WrittenRows> {
    let first = format::time(&entries[0])?.context("CORTEX_INVALID_RECORD: first timestamp")?;
    let mut last = first;
    let mut assistant = None;
    let mut inserted = 0;
    for entry in entries {
        crate::ingest::check_capture_cancelled()?;
        if let Some(ts) = format::time(entry)? {
            last = last.max(ts);
        }
        if matches!(entry["type"].as_str(), Some("user" | "assistant")) {
            inserted += message(conn, sid, cwd, entry, keys)?;
            if entry["type"] == "assistant" {
                let text = format::text(&entry["message"]["content"]);
                if !text.trim().is_empty() {
                    assistant = Some(text);
                }
            }
        } else {
            marker(conn, sid, entry, format::string(entry, "uuid")?, keys)?;
        }
    }
    Ok(WrittenRows {
        inserted,
        last,
        assistant,
    })
}

fn reconcile_complete(
    conn: &Connection,
    sid: &str,
    cwd: Option<&str>,
    entries: &[Value],
) -> Result<()> {
    let replay = file_history::reconcile_event_order(conn, ID, sid, &ordered_events(entries)?)?;
    for entry in entries {
        if matches!(entry["type"].as_str(), Some("user" | "assistant"))
            && format::time(entry)?.is_some_and(|ts| replay.contains(&ts))
        {
            replay_message_events(conn, sid, cwd, entry)?;
        }
    }
    Ok(())
}
fn replay_message_events(
    conn: &Connection,
    sid: &str,
    cwd: Option<&str>,
    entry: &Value,
) -> Result<()> {
    let role = format::string(&entry["message"], "role")?;
    let mid = format::string(entry, "uuid")?;
    let ts = format::time(entry)?.context("CORTEX_INVALID_RECORD: message timestamp")?;
    let base = EventRow {
        source: ID,
        session_id: sid,
        message_id: mid,
        cwd,
        project: cwd,
        ts_ms: ts,
        role,
        model: entry["message"]["model"].as_str(),
        raw_kind: Some(role),
        ..Default::default()
    };
    let blocks = entry["message"]["content"]
        .as_array()
        .context("CORTEX_INVALID_RECORD: content")?;
    for (index, block) in blocks.iter().enumerate() {
        let uid = format!("{mid}:{index}");
        let kind = block["type"].as_str().unwrap_or("unknown");
        let (text, facts) = match kind {
            "text" => (block["text"].as_str().map(str::to_owned), None),
            "thinking" => (block["thinking"].as_str().map(str::to_owned), None),
            "tool_use" => (
                Some(serde_json::to_string(block)?),
                Some(ToolResultFacts {
                    tool_use_id: block["id"].as_str().map(str::to_owned),
                    ..Default::default()
                }),
            ),
            "tool_result" => (
                Some(format::text(&block["content"])),
                Some(result_facts(block)),
            ),
            _ => continue,
        };
        insert_session_event(
            conn,
            &EventRow {
                event_uid: &uid,
                kind,
                role: if kind == "tool_result" {
                    "tool_result"
                } else {
                    role
                },
                text: text.as_deref(),
                tool_result_facts: facts.as_ref(),
                ..base
            },
        )?;
    }
    if blocks.is_empty() {
        let uid = format!("{mid}:0");
        insert_session_event(
            conn,
            &EventRow {
                event_uid: &uid,
                kind: "text",
                ..base
            },
        )?;
    }
    Ok(())
}
fn result_facts(block: &Value) -> ToolResultFacts {
    let mut facts = ToolResultFacts::from_payload(&block["content"]);
    facts.tool_use_id = block["tool_use_id"].as_str().map(str::to_owned);
    facts.result_status = Some(
        match block["native_status"].as_str() {
            Some("success" | "completed") => "completed",
            Some("error" | "failed") => "errored",
            Some("cancelled" | "canceled") => "cancelled",
            Some("running") => "running",
            _ => "unknown",
        }
        .into(),
    );
    facts.error_signal = block["is_error"]
        .as_bool()
        .filter(|b| *b)
        .map(|_| "native_status".into());
    facts
}

fn write_catalog(
    conn: &Connection,
    sid: &str,
    cwd: Option<&str>,
    path: &Path,
    entries: &[Value],
    meta: &Value,
    written: &WrittenRows,
) -> Result<()> {
    let first = if let Some(entry) = entries.first() {
        format::time(entry)?.context("CORTEX_INVALID_RECORD: first timestamp")?
    } else {
        format::created_at(meta)?
    };
    upsert_session_rebuilt(
        conn,
        &SessionCatalogRow {
            source: ID,
            session_id: sid,
            cwd,
            git_branch: None,
            first_ts: first,
            last_ts: written.last,
            last_assistant_text: written.assistant.as_deref(),
            raw_path: Some(&path.to_string_lossy()),
        },
    )?;
    let title = meta["title"].as_str();
    crate::ingest::set_session_title(conn, ID, sid, title)?;
    let first_prompt = format::first_prompt(entries);
    conn.execute("UPDATE sessions SET first_prompt=?2 WHERE source='cortex' AND session_id=?1 AND first_prompt IS NOT ?2", rusqlite::params![sid,first_prompt])?;
    Ok(())
}

fn ordered_events(entries: &[Value]) -> Result<Vec<(i64, String)>> {
    let mut events = Vec::new();
    for entry in entries {
        if !matches!(entry["type"].as_str(), Some("user" | "assistant")) {
            continue;
        }
        let timestamp = format::time(entry)?.context("CORTEX_INVALID_RECORD: message timestamp")?;
        let id = format::string(entry, "uuid")?;
        let content = &entry["message"]["content"];
        let blocks = content.as_array();
        if let Some(blocks) = blocks {
            if blocks.is_empty() {
                events.push((timestamp, format!("{id}:0")));
            }
            for (index, block) in blocks.iter().enumerate() {
                if matches!(
                    block["type"].as_str(),
                    Some("text" | "thinking" | "tool_use" | "tool_result")
                ) {
                    events.push((timestamp, format!("{id}:{index}")));
                }
            }
        } else {
            events.push((timestamp, format!("{id}:0")));
        }
    }
    Ok(events)
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
            ts_ms: format::time(entry)?,
            parent_id: entry["parentUuid"].as_str(),
            message_id: entry["uuid"].as_str(),
            text: entry["summary"]
                .as_str()
                .or_else(|| entry["customTitle"].as_str()),
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
    if !["user", "assistant"].contains(&role) {
        return Ok(0);
    }
    let mid = format::string(entry, "uuid")?;
    let ts = format::time(entry)?.context("CORTEX_INVALID_RECORD: message timestamp")?;
    let base = EventRow {
        source: ID,
        session_id: sid,
        message_id: mid,
        parent_id: entry["parentUuid"].as_str(),
        cwd,
        project: cwd,
        ts_ms: ts,
        role,
        model: msg["model"].as_str(),
        provider: msg["provider"].as_str(),
        token_json: None,
        raw_kind: Some(role),
        raw_facts: RawMessageFacts {
            stop_reason: msg["stopReason"].as_str(),
            ..Default::default()
        },
        ..Default::default()
    };
    content(conn, &base, &msg["content"], keys)?;
    if role != "user"
        || msg["content"]
            .as_array()
            .is_some_and(|a| a.iter().any(|b| b["type"] == "tool_result"))
    {
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
            "tool_use" => {
                call(conn, base, block, keys)?;
                ("tool_use", Some(serde_json::to_string(block)?))
            }
            "tool_result" => {
                result(conn, base, block, &uid, keys)?;
                continue;
            }
            _ => {
                opaque_block(conn, base, block, &uid, keys)?;
                continue;
            }
        };
        let facts = ToolResultFacts {
            tool_use_id: if kind == "tool_use" {
                Some(format::string(block, "id")?.to_owned())
            } else {
                None
            },
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
    // Preserve an empty assistant response as a message without invented usage.
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
    let args = serde_json::to_string(&block["input"])?;
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
        block["input"]["file_path"].as_str(),
        &args,
        None,
    )?;
    keys.tools.insert(id.into());
    Ok(())
}
fn result(
    conn: &Connection,
    base: &EventRow<'_>,
    block: &Value,
    uid: &str,
    keys: &mut Keys,
) -> Result<()> {
    let id = format::string(block, "tool_use_id")?;
    let text = format::text(&block["content"]);
    let facts = result_facts(block);
    let error = block["is_error"].as_bool();
    insert_session_event(
        conn,
        &EventRow {
            event_uid: uid,
            kind: "tool_result",
            role: "tool_result",
            text: Some(&text),
            tool_result_facts: Some(&facts),
            ..*base
        },
    )?;
    if let Some(error) = error {
        set_tool_call_error(conn, ID, base.session_id, id, error)?;
    } else {
        conn.execute(
            "UPDATE tool_calls SET is_error=NULL WHERE source='cortex' AND session_id=?1 AND tool_use_id=?2 AND location='local' AND is_error IS NOT NULL",
            rusqlite::params![base.session_id, id],
        )?;
    }
    keys.results.insert(id.into());
    keys.events.insert(uid.into());
    Ok(())
}
