use super::*;
use crate::providers::file_history::{self, Keys};
use crate::{insert_session_marker, NewSessionMarker};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

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
    let needed: BTreeSet<(i64, String)> = rows
        .iter()
        .filter_map(|(_, ts, prompt, hash)| {
            let key = format!("{ts}:{}", hash.as_deref().unwrap_or_default());
            (!supported.contains(&key)).then(|| (*ts, full_hash(prompt)))
        })
        .collect();
    let owners = marker_owners(conn, source, sid, &needed)?;
    for (id, ts, prompt, hash) in &rows {
        let key = format!("{ts}:{}", hash.as_deref().unwrap_or_default());
        if supported.contains(&key) {
            continue;
        }
        if let Some(candidate) = owners.get(&(*ts, full_hash(prompt))) {
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

fn marker_owners(
    conn: &Connection,
    source: &str,
    sid: &str,
    needed: &BTreeSet<(i64, String)>,
) -> Result<BTreeMap<(i64, String), String>> {
    let mut owners = BTreeMap::new();
    if !needed.is_empty() {
        let times: BTreeSet<i64> = needed.iter().map(|(ts, _)| *ts).collect();
        let times = serde_json::to_string(&times)?;
        let mut markers = conn.prepare(
            "SELECT session_id,ts_ms,CASE WHEN json_valid(payload_json) THEN \
             CASE WHEN json_type(payload_json,'$.text_sha256')='text' THEN \
             json_extract(payload_json,'$.text_sha256') END END FROM session_markers \
             WHERE source=?1 AND kind='prompt_support' AND message_id IS NOT NULL \
             AND length(CAST(payload_json AS BLOB))<=128 \
             AND ts_ms IN (SELECT value FROM json_each(?2)) ORDER BY session_id",
        )?;
        let found = markers.query_map(rusqlite::params![source, times], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })?;
        for found in found {
            crate::ingest::check_capture_cancelled()?;
            let (candidate, ts, Some(hash)) = found? else {
                continue;
            };
            if hash.len() != 64
                || !hash.bytes().all(|byte| byte.is_ascii_hexdigit())
                || !needed.contains(&(ts, hash.clone()))
            {
                continue;
            }
            let key = (ts, hash);
            let owner = owners.entry(key).or_insert_with(|| candidate.clone());
            if candidate == sid {
                *owner = candidate;
            }
        }
    }
    Ok(owners)
}

pub(super) fn reconcile_retired(conn: &Connection, source: &str, sid: &str) -> Result<()> {
    reconcile(conn, source, sid, "[]")
}
