use super::*;
use crate::ingest::{insert_session_event, upsert_session_rebuilt, EventRow, SessionCatalogRow};
use crate::providers::file_history::{self, Keys};
use crate::{insert_history, insert_session_marker, HistoryEntry, NewSessionMarker};

pub(super) fn check_identity(conn: &Connection, sid: &str, path: &Path) -> Result<()> {
    use rusqlite::OptionalExtension;
    let old: Option<String> = conn
        .query_row(
            "SELECT raw_path FROM sessions WHERE source=?1 AND session_id=?2",
            [ID, sid],
            |r| r.get(0),
        )
        .optional()?
        .flatten();
    if let Some(old) = old {
        let old = Path::new(&old);
        if old.parent() == path.parent()
            && ["transcript.jsonl", "transcript_full.jsonl"]
                .iter()
                .any(|n| old.file_name().is_some_and(|v| v == *n))
        {
            return Ok(());
        }
    }
    file_history::check_identity(conn, ID, "ANTIGRAVITY", sid, path)
}
pub(super) fn write(conn: &Connection, payload: &Value) -> Result<usize> {
    let sid = format::string(payload, "sid")?;
    let path = Path::new(format::string(payload, "path")?);
    check_identity(conn, sid, path)?;
    let rows = payload["rows"]
        .as_array()
        .context("Antigravity steps missing")?;
    let complete = payload["partial"] == false;
    let mut keys = Keys::default();
    let mut inserted = 0;
    let mut last_assistant = None;
    let mut first = i64::MAX;
    let mut last = i64::MIN;
    let mut preview = None;
    for row in rows {
        crate::ingest::check_capture_cancelled()?;
        let ts = format::time(row)?;
        first = first.min(ts);
        last = last.max(ts);
        let Some((role, text)) = step(conn, sid, row, &mut keys)? else {
            continue;
        };
        if role == "user" {
            if row["status"] == "DONE" {
                preview.get_or_insert(text);
            }
            let hash = crate::prompt_hash(text);
            keys.prompts.insert(format!("{ts}:{hash}"));
            inserted += insert_history(
                conn,
                &HistoryEntry {
                    id: 0,
                    source: ID.into(),
                    session_id: Some(sid.into()),
                    project: None,
                    prompt: text.into(),
                    prompt_hash: Some(hash),
                    timestamp_ms: ts,
                },
            )?;
        } else {
            last_assistant = Some(text);
        }
    }
    if complete {
        file_history::retire(conn, ID, sid, &keys)?;
        file_history::retire_locator_sessions(conn, ID, path, Some(sid))?;
    }
    // Only the two projections of the same conversation may transfer a live
    // locator; ordinary duplicate identities still fail above.
    conn.execute(
        "UPDATE sessions SET raw_path=?3 WHERE source=?1 AND session_id=?2 AND raw_path IS NOT ?3",
        rusqlite::params![ID, sid, path.to_string_lossy()],
    )?;
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
            cwd: None,
            git_branch: None,
            first_ts: first,
            last_ts: last,
            last_assistant_text: complete.then_some(last_assistant).flatten(),
            raw_path: Some(&path.to_string_lossy()),
        },
    )?;
    if complete {
        conn.execute("UPDATE sessions SET first_prompt=?3 WHERE source=?1 AND session_id=?2 AND first_prompt IS NOT ?3",rusqlite::params![ID,sid,preview])?;
    } else if let Some(preview) = preview {
        // A complete user step remains useful when another step is partial.
        // Such a capture cannot replace a preview from complete evidence.
        conn.execute(
            "UPDATE sessions SET first_prompt=?3 WHERE source=?1 AND session_id=?2 AND first_prompt IS NULL",
            rusqlite::params![ID, sid, preview],
        )?;
    }
    Ok(inserted)
}

fn step<'a>(
    conn: &Connection,
    sid: &str,
    row: &'a Value,
    keys: &mut Keys,
) -> Result<Option<(&'static str, &'a str)>> {
    let index = format::index(row)?;
    let ts = format::time(row)?;
    let mid = format!("step:{index}");
    let raw = serde_json::to_string(&crate::ingest::bound_marker_value(row.clone()))?;
    insert_session_marker(
        conn,
        ID,
        sid,
        &NewSessionMarker {
            marker_uid: &mid,
            kind: format::string(row, "type")?,
            ts_ms: Some(ts),
            payload_json: Some(&raw),
            ..Default::default()
        },
    )?;
    keys.markers.insert(mid.clone());
    // Compact truncation is explicitly lossy. Preserve its raw step, but
    // never overwrite a previously complete message with shortened text.
    if row["is_truncated"] == true
        || row["truncated_fields"]
            .as_array()
            .is_some_and(|a| !a.is_empty())
    {
        return Ok(None);
    }
    let Some(role) = format::role(row) else {
        return Ok(None);
    };
    let Some(text) = format::text(row).filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    let uid = format!("{mid}:0");
    insert_session_event(
        conn,
        &EventRow {
            source: ID,
            session_id: sid,
            message_id: &mid,
            event_uid: &uid,
            ts_ms: ts,
            role,
            kind: "text",
            text: Some(text),
            raw_kind: row["type"].as_str(),
            ..Default::default()
        },
    )?;
    keys.events.insert(uid);
    Ok(Some((role, text)))
}
