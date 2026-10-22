use super::*;
use crate::providers::file_history::{self, Keys};
use crate::{insert_session_marker, NewSessionMarker};
use rusqlite::OptionalExtension;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

fn full_hash(text: &str) -> String {
    format!("{:x}", Sha256::digest(text.as_bytes()))
}

pub(super) fn support(
    conn: &Connection,
    sid: &str,
    mid: &str,
    ts: i64,
    prompt: &str,
    keys: &mut Keys,
) -> Result<()> {
    let uid = format!("{mid}:prompt_support");
    let payload = serde_json::json!({"text_sha256":full_hash(prompt)}).to_string();
    insert_session_marker(
        conn,
        ID,
        sid,
        &NewSessionMarker {
            marker_uid: &uid,
            kind: "prompt_support",
            subkind: Some("projected_text_sha256"),
            ts_ms: Some(ts),
            message_id: Some(mid),
            payload_json: Some(&payload),
            ..Default::default()
        },
    )?;
    keys.markers.insert(uid);
    Ok(())
}

// A bounded prompt may be a projection of the original joined native text,
// while each exported event projects one content block. Joining event text is
// not an equality proof for that prompt. Remote intake records the exact
// history item it owns, so retain that item by its managed evidence identity.
pub(super) fn reconcile(conn: &Connection, source: &str, sid: &str, keep: &str) -> Result<()> {
    let mut supported: BTreeSet<String> = serde_json::from_str(keep)?;
    retain_remote_history(conn, source, sid, &mut supported)?;
    reattribute_local_support(conn, source, sid, &mut supported)?;
    file_history::reconcile_prompts(conn, source, sid, &serde_json::to_string(&supported)?)
}

fn retain_remote_history(
    conn: &Connection,
    source: &str,
    sid: &str,
    supported: &mut BTreeSet<String>,
) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT h.timestamp_ms,h.prompt_hash FROM history h WHERE h.source=?1 AND h.session_id=?2 \
         AND EXISTS(SELECT 1 FROM observation_evidence oe WHERE oe.source=h.source \
         AND oe.session_id=h.session_id AND oe.location='remote' \
         AND json_extract(oe.payload_json,'$.item.managed')=1 \
         AND json_extract(oe.payload_json,'$.item.record.kind')='history' \
         AND json_extract(oe.payload_json,'$.item.record.payload.timestamp_ms')=h.timestamp_ms \
         AND json_extract(oe.payload_json,'$.item.record.payload.prompt')=h.prompt)",
    )?;
    let rows = stmt.query_map([source, sid], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, Option<String>>(1)?))
    })?;
    for row in rows {
        let (timestamp, hash) = row?;
        supported.insert(format!("{timestamp}:{}", hash.unwrap_or_default()));
    }
    Ok(())
}

fn reattribute_local_support(
    conn: &Connection,
    source: &str,
    sid: &str,
    supported: &mut BTreeSet<String>,
) -> Result<()> {
    let mut history = conn.prepare(
        "SELECT id,timestamp_ms,prompt,prompt_hash FROM history WHERE source=?1 AND session_id=?2",
    )?;
    let rows = history
        .query_map([source, sid], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut owner = conn.prepare(
        "SELECT session_id FROM session_markers WHERE source=?1 AND kind='prompt_support' \
         AND ts_ms=?2 AND json_extract(payload_json,'$.text_sha256')=?3 \
         ORDER BY session_id=?4 DESC,session_id LIMIT 1",
    )?;
    for (id, ts, prompt, hash) in &rows {
        let key = format!("{ts}:{}", hash.as_deref().unwrap_or_default());
        if supported.contains(&key) {
            continue;
        }
        let projected_hash = full_hash(prompt);
        let query = rusqlite::params![source, ts, projected_hash, sid];
        let candidate: Option<String> = owner.query_row(query, |row| row.get(0)).optional()?;
        if let Some(candidate) = candidate {
            if candidate == sid {
                supported.insert(key);
            } else {
                conn.execute(
                    "UPDATE history SET session_id=?2,project=NULL WHERE id=?1",
                    rusqlite::params![id, candidate],
                )?;
            }
        }
    }
    Ok(())
}

pub(super) fn reconcile_retired(conn: &Connection, source: &str, sid: &str) -> Result<()> {
    reconcile(conn, source, sid, "[]")
}
