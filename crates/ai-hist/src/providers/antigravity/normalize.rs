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
    let OrderedSteps {
        rows: ordered,
        gaps,
    } = ordered_steps(rows)?;
    let mut keys = Keys::default();
    let mut inserted = 0;
    let mut last_assistant = None;
    let mut first = i64::MAX;
    let mut last = i64::MIN;
    let mut preview = None;
    for (index, row) in ordered {
        crate::ingest::check_capture_cancelled()?;
        let ts = format::time(row)?;
        first = first.min(ts);
        last = last.max(ts);
        let defer_new = gaps.get(&ts).is_some_and(|gap| index >= *gap);
        let Some((role, text)) = step(conn, sid, row, &mut keys, defer_new)? else {
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
                    event_uid: None,
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
    crate::ingest::set_session_title(conn, ID, sid, payload["title"].as_str())?;
    write_preview(conn, sid, preview, complete)?;
    write_subagents(conn, sid, payload, rows)?;
    Ok(inserted)
}

/// Record each subagent the conversation spawned as a delegated child, so a
/// root listing shows one conversation rather than the parent and every
/// child beside it. Either side can be indexed first, so the child also
/// refreshes the `child_has_events` of the edge that points at it.
const EVIDENCE_KIND: &str = "antigravity_subagent_file";

fn write_subagents(conn: &Connection, sid: &str, payload: &Value, rows: &[Value]) -> Result<()> {
    use crate::relationship_capture::{now_ms, record_relationship, ObservedRelationship};
    let has_events = |child: &str| -> Result<bool> {
        Ok(conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM session_events WHERE source=?1 AND session_id=?2)",
            [ID, child],
            |r| r.get(0),
        )?)
    };
    // The sidecar directory is read whole on every capture, so an edge this
    // parent recorded earlier that the current snapshot no longer names is
    // gone: its file was removed or now names another child.
    let current: std::collections::BTreeSet<String> = payload["subagents"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|s| s["childSid"].as_str())
        .filter(|child| *child != sid)
        .map(|child| format!("child:{child}"))
        .collect();
    // Only an established absence retires a link: the directory was listed,
    // and the sidecar behind the edge is gone or now names another child. One
    // that exists but could not be read keeps its link until it can be.
    let unreadable: std::collections::BTreeSet<&str> = payload["subagentsUnreadable"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect();
    let stale: Vec<String> = if payload["subagentsListed"] == true {
        conn.prepare_cached(
            "SELECT relationship_uid, evidence_locator FROM session_relationships \
             WHERE source=?1 AND parent_session_id=?2 AND evidence_kind=?3",
        )?
        .query_map([ID, sid, EVIDENCE_KIND], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .filter(|(uid, locator)| {
            !current.contains(uid) && !locator.as_deref().is_some_and(|l| unreadable.contains(l))
        })
        .map(|(uid, _)| uid)
        .collect()
    } else {
        Vec::new()
    };
    for uid in stale {
        conn.execute(
            "DELETE FROM session_relationships \
             WHERE source=?1 AND parent_session_id=?2 AND relationship_uid=?3",
            [ID, sid, uid.as_str()],
        )?;
    }
    for subagent in payload["subagents"].as_array().into_iter().flatten() {
        let Some(child) = subagent["childSid"].as_str() else {
            continue;
        };
        if child == sid {
            continue;
        }
        let spawned_at_ms = subagent["spawnStepIndex"].as_u64().and_then(|step| {
            rows.iter()
                .find(|r| format::index(r).ok() == Some(step))
                .and_then(|r| format::time(r).ok())
        });
        record_relationship(
            conn,
            &ObservedRelationship {
                source: ID,
                parent_session_id: sid,
                child_session_id: Some(child),
                child_agent_type: subagent["agentType"].as_str(),
                child_agent_name: subagent["agentName"].as_str(),
                spawn_depth: Some(1),
                evidence_kind: EVIDENCE_KIND,
                evidence_locator: subagent["locator"].as_str(),
                child_has_events: has_events(child)?,
                spawned_at_ms,
                ..Default::default()
            },
        )?;
    }
    let mine = has_events(sid)?;
    conn.execute(
        "UPDATE session_relationships SET child_has_events=?3, updated_ms=?4 \
         WHERE source=?1 AND child_session_id=?2 AND child_has_events<>?3",
        rusqlite::params![ID, sid, mine, now_ms()],
    )?;
    Ok(())
}

fn step<'a>(
    conn: &Connection,
    sid: &str,
    row: &'a Value,
    keys: &mut Keys,
    defer_new: bool,
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
    // Shared reads/pages break equal timestamps by database insertion order.
    // Do not insert a new suffix ahead of an omitted native step in that same
    // timestamp group. Markers and previously complete messages stay readable.
    let uid = format!("{mid}:0");
    if defer_new && !conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM session_events WHERE source=?1 AND session_id=?2 AND event_uid=?3)",
        [ID, sid, uid.as_str()], |r| r.get::<_, bool>(0),
    )? { return Ok(None); }
    if row["status"] != "DONE" && row["status"] != "ERROR" {
        return Ok(None);
    }
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

fn omitted_message(row: &Value) -> bool {
    let message = matches!(
        (row["source"].as_str(), row["type"].as_str()),
        (Some("USER_EXPLICIT"), Some("USER_INPUT")) | (Some("MODEL"), Some("PLANNER_RESPONSE"))
    );
    message
        && (row["is_truncated"] == true
            || row["truncated_fields"]
                .as_array()
                .is_some_and(|fields| !fields.is_empty())
            || (row["status"] != "DONE" && row["status"] != "ERROR"))
}

struct OrderedSteps<'a> {
    rows: Vec<(u64, &'a Value)>,
    gaps: std::collections::BTreeMap<i64, u64>,
}

fn ordered_steps(rows: &[Value]) -> Result<OrderedSteps<'_>> {
    let mut ordered = rows
        .iter()
        .map(|row| Ok((format::index(row)?, row)))
        .collect::<Result<Vec<_>>>()?;
    ordered.sort_by_key(|(index, _)| *index);
    let mut gaps = std::collections::BTreeMap::<i64, u64>::new();
    for (index, row) in &ordered {
        if omitted_message(row) {
            gaps.entry(format::time(row)?)
                .and_modify(|first| *first = (*first).min(*index))
                .or_insert(*index);
        }
    }
    Ok(OrderedSteps {
        rows: ordered,
        gaps,
    })
}

fn write_preview(
    conn: &Connection,
    sid: &str,
    preview: Option<&str>,
    complete: bool,
) -> Result<()> {
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
    Ok(())
}
