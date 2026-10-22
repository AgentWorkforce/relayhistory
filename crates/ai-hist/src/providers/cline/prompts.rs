use super::*;
use crate::providers::file_history;
use std::collections::BTreeSet;

// A bounded prompt may be a projection of the original joined native text,
// while each exported event projects one content block. Joining event text is
// not an equality proof for that prompt. Remote intake records the exact
// history item it owns, so retain that item by its managed evidence identity.
pub(super) fn reconcile(conn: &Connection, source: &str, sid: &str, keep: &str) -> Result<()> {
    let mut supported: BTreeSet<String> = serde_json::from_str(keep)?;
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
    file_history::reconcile_prompts(conn, source, sid, &serde_json::to_string(&supported)?)
}

pub(super) fn reconcile_retired(conn: &Connection, source: &str, sid: &str) -> Result<()> {
    reconcile(conn, source, sid, "[]")
}
