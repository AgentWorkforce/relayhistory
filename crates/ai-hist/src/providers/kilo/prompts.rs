use super::projections;
use super::*;
use crate::providers::file_history;
use std::collections::{BTreeMap, BTreeSet};

struct PromptSupport {
    session_id: String,
    timestamp: i64,
    prompt: String,
    project: Option<String>,
}
type PromptParts = BTreeMap<(String, i64, String), Vec<(String, Option<String>)>>;
fn supported_prompts(conn: &Connection, times: &BTreeSet<i64>) -> Result<Vec<PromptSupport>> {
    let times = serde_json::to_string(times)?;
    let mut stmt = conn.prepare("SELECT session_id,ts_ms,message_id,event_uid,text,COALESCE(project,cwd) FROM session_events WHERE source=?1 AND role='user' AND kind='text' AND text IS NOT NULL AND control_kind IS NULL AND ts_ms IN (SELECT value FROM json_each(?2)) ORDER BY ts_ms,id")?;
    let mut groups = PromptParts::new();
    let rows = stmt.query_map(rusqlite::params![ID, times], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, Option<String>>(5)?,
        ))
    })?;
    for row in rows {
        let (sid, ts, mid, uid, text, project) = row?;
        // Kilo native text parts share a message ID. Opaque remote records
        // without that identity are independent prompts, not guessed turns.
        let group = mid.filter(|mid| !mid.is_empty()).unwrap_or(uid);
        groups
            .entry((sid, ts, group))
            .or_default()
            .push((text, project));
    }
    Ok(groups
        .into_iter()
        .map(|((session_id, timestamp, _), parts)| PromptSupport {
            session_id,
            timestamp,
            project: parts.first().and_then(|(_, p)| p.clone()),
            prompt: projections::bounded_joined_text(
                &parts
                    .into_iter()
                    .map(|(text, _)| text)
                    .collect::<Vec<_>>()
                    .join("\n"),
            ),
        })
        .collect())
}
pub(super) fn reconcile_history(conn: &Connection, sid: &str) -> Result<()> {
    let mut stmt = conn
        .prepare("SELECT id,timestamp_ms,prompt FROM history WHERE source=?1 AND session_id=?2")?;
    let rows = stmt
        .query_map([ID, sid], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, String>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let times = rows.iter().map(|(_, ts, _)| *ts).collect();
    let support = supported_prompts(conn, &times)?;
    for (id, ts, prompt) in rows {
        let owner = support
            .iter()
            .filter(|p| p.timestamp == ts && p.prompt == prompt)
            .min_by_key(|p| (p.session_id != sid, &p.session_id));
        if let Some(owner) = owner {
            conn.execute("UPDATE history SET session_id=?2,project=?3 WHERE id=?1 AND (session_id IS NOT ?2 OR project IS NOT ?3)",rusqlite::params![id,owner.session_id,owner.project])?;
        } else {
            conn.execute("DELETE FROM history WHERE id=?1", [id])?;
        }
    }
    Ok(())
}
pub(super) fn retire_local_session(conn: &Connection, sid: &str) -> Result<()> {
    file_history::retire_local_session_with_prompts(conn, ID, sid, reconcile_history_for_source)
}
fn reconcile_history_for_source(conn: &Connection, _: &str, sid: &str) -> Result<()> {
    reconcile_history(conn, sid)
}
