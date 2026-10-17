use super::*;
use crate::ingest::control::ControlKind;
use crate::ingest::{
    insert_session_event, upsert_session, EventRow, RawMessageFacts, SessionCatalogRow,
};
use crate::providers::file_history;
use crate::relationship_capture::{record_relationship, ObservedRelationship};
use crate::{insert_history, insert_session_marker, HistoryEntry, NewSessionMarker};
use rusqlite::OptionalExtension;
use std::collections::HashSet;

pub(super) fn check_identity(conn: &Connection, sid: &str, path: &Path) -> Result<()> {
    file_history::check_identity(conn, ID, "GROK_BOT", sid, path)
}

pub(super) fn write(conn: &Connection, payload: &Value, path: &Path) -> Result<usize> {
    let sid = format::string(payload, "session_id")?;
    check_identity(conn, sid, path)?;
    let replica = &payload["replica"];
    let entries = format::valid_replica(replica)?;
    let roster = payload["roster"]["value"]["rows"]
        .as_array()
        .and_then(|rows| rows.iter().find(|row| row["id"].as_str() == Some(sid)));
    let first = entries
        .iter()
        .copied()
        .filter_map(|e| e["timestampMs"].as_i64())
        .min()
        .or_else(|| roster.and_then(|row| row["createdAt"].as_i64()))
        .or_else(|| replica["value"]["persistedAt"].as_i64())
        .context("GROK_BOT_INVALID_REPLICA: no timestamp")?;
    let last = entries
        .iter()
        .copied()
        .filter_map(format::last_activity)
        .max()
        .unwrap_or(first);
    let mut inserted = 0;
    let mut linked_children = HashSet::new();
    for &entry in &entries {
        crate::ingest::check_capture_cancelled()?;
        inserted += write_entry(conn, sid, path, entry, &mut linked_children)?;
    }
    // Truncation is judged from the native first entry, so a malformed `tbs0`
    // that was skipped above does not read as a trimmed window.
    if let Some(head) = entries.first() {
        let native_head = &replica["value"]["entries"][0]["id"];
        if native_head != "tbs0" || replica["value"]["precedingTimestampMs"].is_number() {
            let uid = format!("window:{}", format::string(head, "id")?);
            let details = serde_json::json!({
                "firstEntryId": head["id"],
                "precedingTimestampMs": replica["value"]["precedingTimestampMs"]
            });
            marker(
                conn,
                sid,
                &uid,
                "truncated_tail",
                Some(first),
                None,
                &details,
            )?;
        }
    }
    let assistant: Option<String> = conn.query_row(
        "SELECT text FROM session_events WHERE source='grok-bot' AND session_id=?1 AND role='assistant' AND kind='text' AND text IS NOT NULL ORDER BY ts_ms DESC,id DESC LIMIT 1",
        [sid], |row| row.get(0),
    ).optional()?;
    upsert_session(
        conn,
        &SessionCatalogRow {
            source: ID,
            session_id: sid,
            cwd: None,
            git_branch: None,
            first_ts: first,
            last_ts: last,
            last_assistant_text: assistant.as_deref(),
            raw_path: Some(&path.to_string_lossy()),
        },
    )?;
    // An explicit blank title clears it. A missing roster row, or one whose
    // newer schema omits both fields, keeps the last known name.
    if let Some(row) = roster.filter(|row| row.get("name").is_some() || row.get("title").is_some()) {
        if let Some(title) = row["name"]
            .as_str()
            .filter(|name| !name.trim().is_empty())
            .or_else(|| {
                row["title"]
                    .as_str()
                    .filter(|title| !title.trim().is_empty())
            })
        {
            crate::ingest::set_session_title(conn, ID, sid, Some(title))?;
        } else {
            conn.execute(
                "UPDATE sessions SET title=NULL WHERE source=?1 AND session_id=?2 AND title IS NOT NULL",
                rusqlite::params![ID, sid],
            )?;
        }
    }
    let prompt: Option<String> = conn.query_row(
        "SELECT prompt FROM history WHERE source='grok-bot' AND session_id=?1 ORDER BY timestamp_ms,id LIMIT 1",
        [sid], |row| row.get(0),
    ).optional()?;
    conn.execute("UPDATE sessions SET first_prompt=?2 WHERE source='grok-bot' AND session_id=?1 AND first_prompt IS NOT ?2", rusqlite::params![sid,prompt])?;
    Ok(inserted)
}

fn write_entry(
    conn: &Connection,
    sid: &str,
    path: &Path,
    entry: &Value,
    linked_children: &mut HashSet<String>,
) -> Result<usize> {
    let kind = format::string(entry, "kind")?;
    let id = format::string(entry, "id")?;
    let ts = format::timestamp(entry)?;
    match kind {
        "message" => message(conn, sid, path, entry, id, ts, linked_children),
        "send-message" => sent(conn, sid, path, entry, id, ts, linked_children),
        "voice-call" => voice(conn, sid, entry, id, ts),
        "event" | "user-attachment" => {
            let subtype = entry["event"]["type"].as_str().unwrap_or(kind);
            marker(conn, sid, id, subtype, Some(ts), None, entry)?;
            Ok(0)
        }
        _ => {
            marker(conn, sid, id, kind, Some(ts), None, entry)?;
            Ok(0)
        }
    }
}

struct EventMeta<'a> {
    parent: Option<&'a str>,
    raw_kind: &'a str,
    control_kind: Option<&'a str>,
}

fn event(
    conn: &Connection,
    sid: &str,
    uid: &str,
    ts: i64,
    role: &str,
    text: Option<&str>,
    meta: EventMeta<'_>,
) -> Result<()> {
    insert_session_event(
        conn,
        &EventRow {
            source: ID,
            session_id: sid,
            event_uid: uid,
            message_id: uid,
            parent_id: meta.parent,
            ts_ms: ts,
            role,
            kind: "text",
            text,
            raw_kind: Some(meta.raw_kind),
            raw_facts: RawMessageFacts {
                control_kind: meta.control_kind,
                ..Default::default()
            },
            ..Default::default()
        },
    )
}

fn history(conn: &Connection, sid: &str, ts: i64, text: &str) -> Result<usize> {
    if text.trim().is_empty() {
        return Ok(0);
    }
    insert_history(
        conn,
        &HistoryEntry {
            id: 0,
            source: ID.into(),
            session_id: Some(sid.into()),
            project: None,
            prompt: text.into(),
            prompt_hash: Some(crate::prompt_hash(text)),
            timestamp_ms: ts,
        },
    )
}

fn message(
    conn: &Connection,
    sid: &str,
    path: &Path,
    entry: &Value,
    id: &str,
    ts: i64,
    linked_children: &mut HashSet<String>,
) -> Result<usize> {
    let role = match entry["role"].as_str() {
        Some(role @ ("user" | "assistant")) => role,
        // A role this adapter does not map stays visible as a marker.
        _ => {
            marker(conn, sid, id, "message", Some(ts), None, entry)?;
            return Ok(0);
        }
    };
    let text = entry["content"].as_str().unwrap_or_default();
    let from_agent = !entry["fromAgent"].is_null();
    let peer = entry["toAgent"]
        .as_object()
        .or_else(|| entry["fromAgent"].as_object());
    if let Some(peer) = peer {
        relationship(conn, sid, path, id, ts, peer, linked_children)?;
    }
    event(
        conn,
        sid,
        id,
        ts,
        role,
        Some(text),
        EventMeta {
            parent: None,
            raw_kind: "message",
            control_kind: (role == "user" && from_agent).then_some(ControlKind::Synthetic.as_str()),
        },
    )?;
    if role == "user" && !from_agent {
        history(conn, sid, ts, text)
    } else {
        Ok(0)
    }
}

fn sent(
    conn: &Connection,
    sid: &str,
    path: &Path,
    entry: &Value,
    id: &str,
    ts: i64,
    linked_children: &mut HashSet<String>,
) -> Result<usize> {
    let message = &entry["message"];
    match format::string(message, "type").unwrap_or("send-message") {
        "text" => {
            event(
                conn,
                sid,
                id,
                ts,
                "assistant",
                message["content"].as_str(),
                EventMeta {
                    parent: None,
                    raw_kind: "send-message/text",
                    control_kind: None,
                },
            )?;
            Ok(0)
        }
        "widget" => {
            let widget = &message["widget"];
            let prompt = widget["prompt"].as_str().unwrap_or_default();
            event(
                conn,
                sid,
                id,
                ts,
                "assistant",
                Some(prompt),
                EventMeta {
                    parent: None,
                    raw_kind: "send-message/widget",
                    control_kind: None,
                },
            )?;
            marker(
                conn,
                sid,
                &format!("{id}:widget"),
                "widget",
                Some(ts),
                Some(id),
                widget,
            )?;
            if let Some(response) = entry.get("respondedValue").filter(|v| !v.is_null()) {
                let response = response
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| response.to_string());
                let response_id = format!("{id}:response");
                event(
                    conn,
                    sid,
                    &response_id,
                    ts,
                    "user",
                    Some(&response),
                    EventMeta {
                        parent: Some(id),
                        raw_kind: "widget-response",
                        control_kind: None,
                    },
                )?;
                return history(conn, sid, ts, &response);
            }
            if entry["widgetSkipped"] == true {
                marker(
                    conn,
                    sid,
                    &format!("{id}:skipped"),
                    "widget_skipped",
                    Some(ts),
                    Some(id),
                    entry,
                )?;
            }
            Ok(0)
        }
        "cursor-agent" => {
            let peer = serde_json::Map::from_iter([
                ("id".to_owned(), message["bcId"].clone()),
                ("name".to_owned(), message["title"].clone()),
            ]);
            relationship(conn, sid, path, id, ts, &peer, linked_children)?;
            marker(conn, sid, id, "cursor-agent", Some(ts), None, entry)?;
            Ok(0)
        }
        other => {
            marker(conn, sid, id, other, Some(ts), None, entry)?;
            Ok(0)
        }
    }
}

fn voice(conn: &Connection, sid: &str, entry: &Value, id: &str, ts: i64) -> Result<usize> {
    marker(conn, sid, id, "voice-call", Some(ts), None, entry)?;
    let mut inserted = 0;
    if let Some(turns) = entry["conversation"]["turns"].as_array() {
        for (index, turn) in turns.iter().enumerate() {
            let role = match turn["speaker"].as_str() {
                Some("user") => "user",
                Some("assistant") => "assistant",
                _ => continue,
            };
            let uid = turn["id"]
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| format!("{id}:turn:{index}"));
            let at = turn["atMs"].as_i64().unwrap_or(ts);
            let text = turn["text"].as_str().unwrap_or_default();
            event(
                conn,
                sid,
                &uid,
                at,
                role,
                Some(text),
                EventMeta {
                    parent: Some(id),
                    raw_kind: "voice-call/turn",
                    control_kind: None,
                },
            )?;
            if role == "user" {
                inserted += history(conn, sid, at, text)?;
            }
        }
    }
    Ok(inserted)
}

fn marker(
    conn: &Connection,
    sid: &str,
    uid: &str,
    kind: &str,
    ts: Option<i64>,
    parent: Option<&str>,
    payload: &Value,
) -> Result<()> {
    let json = serde_json::to_string(&crate::ingest::bound_marker_value(payload.clone()))?;
    insert_session_marker(
        conn,
        ID,
        sid,
        &NewSessionMarker {
            marker_uid: uid,
            kind,
            ts_ms: ts,
            parent_id: parent,
            message_id: Some(uid),
            payload_json: Some(&json),
            ..Default::default()
        },
    )?;
    Ok(())
}

fn relationship(
    conn: &Connection,
    sid: &str,
    path: &Path,
    id: &str,
    ts: i64,
    peer: &serde_json::Map<String, Value>,
    linked_children: &mut HashSet<String>,
) -> Result<()> {
    // Only a Cursor cloud agent (`bc-…`) is a delegated child. Any other peer
    // keeps its message evidence without inventing a relationship edge.
    let Some(child) = peer
        .get("id")
        .and_then(Value::as_str)
        .filter(|child| child.starts_with("bc-") && !child.chars().any(char::is_control))
    else {
        return Ok(());
    };
    if !linked_children.insert(child.to_owned()) {
        return Ok(());
    }
    // The client keeps only a sliding tail. A later window may retain a
    // message to the child after the original spawn entry has disappeared.
    let prior: Option<(Option<i64>, Option<String>)> = conn
        .query_row(
            "SELECT spawned_at_ms, evidence_ref FROM session_relationships \
             WHERE source=?1 AND parent_session_id=?2 AND relationship_uid=?3",
            rusqlite::params![ID, sid, format!("child:{child}")],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let (spawned_at, evidence_ref) = match prior {
        Some((Some(previous), previous_id)) if previous <= ts => {
            (previous, previous_id.unwrap_or_else(|| id.to_owned()))
        }
        _ => (ts, id.to_owned()),
    };
    record_relationship(
        conn,
        &ObservedRelationship {
            source: ID,
            parent_session_id: sid,
            child_session_id: Some(child),
            relationship: "delegated",
            child_agent_type: Some("cursor-cloud-agent"),
            child_agent_name: peer.get("name").and_then(Value::as_str),
            evidence_kind: "transcript-entry",
            evidence_locator: Some(&path.to_string_lossy()),
            evidence_ref: Some(&evidence_ref),
            child_has_events: false,
            spawned_at_ms: Some(spawned_at),
            ..Default::default()
        },
    )
}
