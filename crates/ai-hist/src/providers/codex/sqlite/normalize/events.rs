use super::*;

const NATIVE_ITEM_SQL: &str = "SELECT EXISTS(SELECT 1 FROM session_events \
         WHERE source='codex' AND session_id=?1 AND event_uid<>?2 AND role=?3 AND kind=?4 AND message_id=?5 \
         UNION ALL SELECT 1 FROM session_events \
         WHERE source='codex' AND session_id=?1 AND event_uid<>?2 AND role=?3 AND kind=?4 AND provider_message_id=?5)";

const IDLESS_TEXT_SQL: &str = "SELECT event_uid FROM session_events WHERE source='codex' AND session_id=?1 \
                 AND role=?2 AND kind='text' AND raw_kind IS NULL AND text=?3 \
                 AND ABS(ts_ms - ?4) <= ?5 \
                 AND ts_ms BETWEEN ?6 AND ?7 \
                 AND NOT EXISTS (SELECT 1 FROM session_markers AS marker \
                   WHERE marker.source='codex' AND marker.session_id=?1 \
                   AND marker.marker_uid LIKE 'sqlite:overlap:%' \
                   AND json_extract(marker.payload_json, '$.rollout_event_uid')=session_events.event_uid) \
                 ORDER BY ABS(ts_ms - ?4), id LIMIT 1";

pub(super) fn event(
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
    let overlap_uid = format!("sqlite:overlap:{}", item.id);
    if matches!((role, kind), ("user" | "assistant", "text")) {
        let mapped: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM session_markers WHERE source='codex' AND session_id=?1 AND marker_uid=?2)",
            params![t.id, overlap_uid],
            |row| row.get(0),
        )?;
        if mapped {
            return Ok(false);
        }
    }
    let rollout_has_item = conn.query_row(
        NATIVE_ITEM_SQL,
        params![t.id, uid, role, kind, item.id],
        |r| r.get::<_, bool>(0),
    )?;
    if rollout_has_item {
        return Ok(false);
    }
    if let Some(text) = text.filter(|_| matches!((role, kind), ("user" | "assistant", "text"))) {
        let rollout_uid: Option<String> = conn
            .query_row(
                IDLESS_TEXT_SQL,
                params![
                    t.id,
                    role,
                    text,
                    item.ts,
                    IDLESS_ROLLOUT_TIMESTAMP_TOLERANCE_MS,
                    item.ts
                        .saturating_sub(IDLESS_ROLLOUT_TIMESTAMP_TOLERANCE_MS),
                    item.ts
                        .saturating_add(IDLESS_ROLLOUT_TIMESTAMP_TOLERANCE_MS)
                ],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(rollout_uid) = rollout_uid {
            marker(
                conn,
                t,
                &overlap_uid,
                item.ts,
                "rollout_overlap",
                &json!({"rollout_event_uid":rollout_uid}),
                item.turn_id.as_deref(),
            )?;
            return Ok(false);
        }
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
