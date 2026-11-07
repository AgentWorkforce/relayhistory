use super::*;
use crate::ingest::{
    insert_session_event, upsert_session_rebuilt, EventRow, RawMessageFacts, SessionCatalogRow,
};
use crate::providers::file_history::{self, Keys};
use crate::providers::shared_evidence::Snapshot;
use crate::{insert_history, insert_session_marker, HistoryEntry, NewSessionMarker};
pub(super) fn check_identity(conn: &Connection, sid: &str, path: &Path) -> Result<()> {
    file_history::check_identity(conn, ID, "DROID", sid, path)
}
pub(super) fn write(conn: &Connection, payload: &Value, path: &Path) -> Result<usize> {
    let entries = payload["entries"]
        .as_array()
        .context("Droid entries missing")?;
    let header = &entries[0];
    let sid = format::string(header, "id")?;
    check_identity(conn, sid, path)?;
    let shared = Snapshot::capture(conn, ID, sid)?;
    let cwd = header["cwd"].as_str();
    let mut keys = Keys::default();
    let records = records(conn, sid, cwd, entries, &mut keys)?;
    if payload["settings"].is_object() {
        marker(
            conn,
            sid,
            &serde_json::json!({"type":"session_settings","settings":payload["settings"]}),
            "settings",
            &mut keys,
        )?;
    }
    let complete = payload["tail"].as_array().is_some_and(Vec::is_empty);
    if complete {
        file_history::retire(conn, ID, sid, &keys)?;
        reconcile_order(conn, sid, cwd, entries, &mut keys, &records.order)?;
    }
    let fallback = payload["modified"].as_i64().unwrap_or(0);
    let times: Vec<_> = entries
        .iter()
        .map(format::time)
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect();
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
            git_branch: None,
            first_ts: times.iter().min().copied().unwrap_or(fallback),
            last_ts: times.iter().max().copied().unwrap_or(fallback),
            last_assistant_text: complete.then_some(records.assistant.as_deref()).flatten(),
            raw_path: Some(&path.to_string_lossy()),
        },
    )?;
    if complete {
        let title = header["sessionTitle"]
            .as_str()
            .or_else(|| header["title"].as_str());
        crate::ingest::set_session_title(conn, ID, sid, title)?;
        conn.execute("UPDATE sessions SET first_prompt=?2 WHERE source='droid' AND session_id=?1 AND first_prompt IS NOT ?2",rusqlite::params![sid,format::first_prompt(entries)])?;
        file_history::retire_locator_sessions(conn, ID, path, Some(sid))?;
    }
    shared.ensure_unchanged(conn, sid)?;
    Ok(records.inserted)
}
struct Records {
    inserted: usize,
    assistant: Option<String>,
    order: Vec<(i64, String)>,
}
fn reconcile_order(
    conn: &Connection,
    sid: &str,
    cwd: Option<&str>,
    entries: &[Value],
    keys: &mut Keys,
    order: &[(i64, String)],
) -> Result<()> {
    let replay = file_history::reconcile_event_order(conn, ID, sid, order)?;
    for entry in entries.iter().filter(|e| e["type"] == "message") {
        if format::time(entry)?.is_some_and(|t| replay.contains(&t)) {
            message(conn, sid, cwd, entry, keys, &mut Vec::new())?;
        }
    }
    Ok(())
}
fn records(
    conn: &Connection,
    sid: &str,
    cwd: Option<&str>,
    entries: &[Value],
    keys: &mut Keys,
) -> Result<Records> {
    let mut inserted = 0;
    let mut assistant = None;
    let mut order = Vec::new();
    for (index, entry) in entries.iter().enumerate() {
        crate::ingest::check_capture_cancelled()?;
        if entry["type"] == "message" {
            inserted += message(conn, sid, cwd, entry, keys, &mut order)?;
            if entry["message"]["role"] == "assistant" {
                let text = format::text(&entry["message"]["content"]);
                if !text.trim().is_empty() {
                    assistant = Some(text);
                }
            }
        } else {
            marker(conn, sid, entry, &format!("record:{index}"), keys)?;
        }
    }
    Ok(Records {
        inserted,
        assistant,
        order,
    })
}
pub(super) fn marker(
    conn: &Connection,
    sid: &str,
    entry: &Value,
    id: &str,
    keys: &mut Keys,
) -> Result<()> {
    let payload = serde_json::to_string(&crate::ingest::bound_marker_value(entry.clone()))?;
    insert_session_marker(
        conn,
        ID,
        sid,
        &NewSessionMarker {
            marker_uid: id,
            kind: format::string(entry, "type")?,
            ts_ms: format::time(entry)?,
            message_id: entry["id"].as_str(),
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
    order: &mut Vec<(i64, String)>,
) -> Result<usize> {
    let msg = &entry["message"];
    let role = format::string(msg, "role")?;
    let mid = format::string(entry, "id")?;
    if !["user", "assistant"].contains(&role) || format::is_control(msg) {
        marker(conn, sid, entry, &format!("message:{mid}"), keys)?;
        return Ok(0);
    }
    let base = EventRow {
        source: ID,
        session_id: sid,
        message_id: mid,
        cwd,
        project: cwd,
        parent_id: entry["parentId"].as_str(),
        ts_ms: format::time(entry)?.context("Droid timestamp missing")?,
        role,
        model: msg["modelId"].as_str(),
        provider: msg["apiProvider"].as_str(),
        raw_kind: Some("message"),
        raw_facts: RawMessageFacts::default(),
        ..Default::default()
    };
    let blocks = msg["content"].as_array().context("Droid content missing")?;
    for (index, block) in blocks.iter().enumerate() {
        let uid = format!("{mid}:{index}");
        if blocks::write(conn, &base, block, &uid, keys)? {
            order.push((base.ts_ms, uid));
        }
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
        keys.events.insert(uid.clone());
        order.push((base.ts_ms, uid));
    }
    if role != "user" {
        return Ok(0);
    }
    let text = format::text(&msg["content"]);
    if text.is_empty() {
        return Ok(0);
    }
    let hash = crate::prompt_hash(&text);
    keys.prompts.insert(format!("{}:{hash}", base.ts_ms));
    insert_history(
        conn,
        &HistoryEntry {
            id: 0,
            source: ID.into(),
            session_id: Some(sid.into()),
            project: cwd.map(str::to_owned),
            prompt: text,
            prompt_hash: Some(hash),
            timestamp_ms: base.ts_ms,
        },
    )
}
