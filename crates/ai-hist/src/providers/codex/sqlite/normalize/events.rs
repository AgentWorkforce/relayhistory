use super::*;

mod overlap;
pub(super) use overlap::{Kind, Overlaps};

const NATIVE_ITEM_SQL: &str = "SELECT EXISTS(SELECT 1 FROM session_events \
         WHERE source='codex' AND session_id=?1 AND event_uid<>?2 AND role=?3 AND kind=?4 AND message_id=?5 \
         UNION ALL SELECT 1 FROM session_events \
         WHERE source='codex' AND session_id=?1 AND event_uid<>?2 AND role=?3 AND kind=?4 AND provider_message_id=?5)";

const IDLESS_TEXT_SQL: &str =
    "SELECT event_uid FROM session_events WHERE source='codex' AND session_id=?1 \
                 AND role=?2 AND kind='text' AND raw_kind IS NULL AND text=?3 \
                 AND ABS(ts_ms - ?4) <= ?5 \
                 AND ts_ms BETWEEN ?6 AND ?7 \
                 AND (provider_message_id IS NULL OR provider_message_id='') \
                 AND message_id=event_uid \
                 ORDER BY ABS(ts_ms - ?4), id";

pub(super) fn event(
    conn: &Connection,
    t: &Thread,
    item: &Item,
    uid: &str,
    role_and_kind: (&str, &str),
    text: Option<&str>,
    result_facts: Option<&ToolResultFacts>,
) -> Result<bool> {
    if native_item(conn, t, item, uid, role_and_kind)? {
        return Ok(false);
    }
    insert_event(conn, t, item, uid, role_and_kind, text, result_facts)
}

fn native_item(
    conn: &Connection,
    t: &Thread,
    item: &Item,
    uid: &str,
    (role, kind): (&str, &str),
) -> Result<bool> {
    conn.query_row(
        NATIVE_ITEM_SQL,
        params![t.id, uid, role, kind, item.id],
        |r| r.get::<_, bool>(0),
    )
    .map_err(Into::into)
}

pub(super) fn text_event(
    conn: &Connection,
    t: &Thread,
    item: &Item,
    role: &str,
    text: Option<&str>,
    overlaps: &mut Overlaps,
) -> Result<bool> {
    let overlap_uid = format!("sqlite:overlap:{}", item.id);
    let kind = if role == "user" {
        Kind::User
    } else {
        Kind::Assistant
    };
    if let Some(text) = text {
        if overlaps.mapped(conn, t, item, &overlap_uid, kind, text)? {
            return Ok(false);
        }
    }
    if native_item(conn, t, item, &item.id, (role, "text"))? {
        return Ok(false);
    }
    if let Some(text) = text {
        let mut stmt = conn.prepare(IDLESS_TEXT_SQL)?;
        let mut rows = stmt.query(params![
            t.id,
            role,
            text,
            item.ts,
            IDLESS_ROLLOUT_TIMESTAMP_TOLERANCE_MS,
            item.ts
                .saturating_sub(IDLESS_ROLLOUT_TIMESTAMP_TOLERANCE_MS),
            item.ts
                .saturating_add(IDLESS_ROLLOUT_TIMESTAMP_TOLERANCE_MS)
        ])?;
        while let Some(row) = rows.next()? {
            let uid: String = row.get(0)?;
            if !overlaps.claimed(&uid) {
                overlaps.claim(conn, t, item, &overlap_uid, kind, uid)?;
                return Ok(false);
            }
        }
    }
    insert_event(conn, t, item, &item.id, (role, "text"), text, None)
}

fn insert_event(
    conn: &Connection,
    t: &Thread,
    item: &Item,
    uid: &str,
    role_and_kind: (&str, &str),
    text: Option<&str>,
    result_facts: Option<&ToolResultFacts>,
) -> Result<bool> {
    let (role, kind) = role_and_kind;
    let control_kind = if (role, kind) == ("user", "text") {
        text.and_then(crate::ingest::control::codex_text_control_kind)
            .map(crate::ingest::control::ControlKind::as_str)
    } else {
        None
    };
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
                control_kind,
                ..Default::default()
            },
            ..Default::default()
        },
    )?;
    Ok(true)
}

#[cfg(test)]
mod tests;
