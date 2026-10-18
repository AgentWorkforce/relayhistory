//! Transcript rows: hot `transcript_events` (JSON or zstd), or a verified
//! cold archive, decoded into validated entries.
use super::*;
use crate::ingest::check_capture_cancelled;
use anyhow::{ensure, Context};
const MAX_EVENTS: usize = 1_000_000;
/// Upstream bounds each stored event, compressed or not, to 4 MiB.
const MAX_EVENT_BYTES: usize = 4 * 1024 * 1024;

/// Every stored event in sequence order, active branch flagged: hot rows, or
/// the rows of a verified cold archive. Compressed rows are decoded with
/// upstream's recorded length bound.
pub(super) fn events(
    context: &AcquisitionContext<'_>,
    conn: &Connection,
    path: &Path,
    s: &Session,
) -> Result<Vec<Event>> {
    let native = s.native_id.as_str();
    if let Some(cold) = &s.cold {
        return cold::events(context, conn, path, native, cold);
    }
    let mut stmt = conn.prepare(
        "SELECT e.seq, e.event_json, e.event_zstd, e.event_utf8_bytes, e.created_at, \
         a.event_seq IS NOT NULL FROM transcript_events e \
         LEFT JOIN session_transcript_active_events a ON a.session_id=e.session_id AND a.event_seq=e.seq \
         WHERE e.session_id=?1 ORDER BY e.seq LIMIT ?2",
    )?;
    let mut rows = stmt.query(rusqlite::params![native, (MAX_EVENTS + 1) as i64])?;
    let mut result = vec![];
    while let Some(r) = rows.next()? {
        check_capture_cancelled()?;
        let seq: i64 = r.get(0)?;
        let text = match r.get::<_, Option<String>>(1)? {
            Some(json) => json,
            None => decompress(r.get(2)?, r.get(3)?)
                .with_context(|| format!("OpenClaw event {seq} of {native:?}"))?,
        };
        result.push(event(
            native,
            seq,
            &text,
            r.get(4)?,
            r.get(5)?,
            result.is_empty(),
        )?);
        ensure!(
            result.len() <= MAX_EVENTS,
            "OpenClaw event capture exceeds safety limit"
        );
    }
    Ok(result)
}
/// The header and every hot event from `from_seq` on: the part of a grown
/// transcript an append-only pass re-reads.
pub(super) fn tail(conn: &Connection, native: &str, from_seq: i64) -> Result<Vec<Event>> {
    let mut stmt = conn.prepare(
        "SELECT e.seq, e.event_json, e.event_zstd, e.event_utf8_bytes, e.created_at, \
         a.event_seq IS NOT NULL FROM transcript_events e \
         LEFT JOIN session_transcript_active_events a ON a.session_id=e.session_id AND a.event_seq=e.seq \
         WHERE e.session_id=?1 AND (e.seq=(SELECT min(seq) FROM transcript_events WHERE session_id=?1) OR e.seq>=?2) \
         ORDER BY e.seq LIMIT ?3",
    )?;
    let mut rows = stmt.query(rusqlite::params![native, from_seq, (MAX_EVENTS + 1) as i64])?;
    let mut result = vec![];
    while let Some(r) = rows.next()? {
        check_capture_cancelled()?;
        let seq: i64 = r.get(0)?;
        let text = match r.get::<_, Option<String>>(1)? {
            Some(json) => json,
            None => decompress(r.get(2)?, r.get(3)?)
                .with_context(|| format!("OpenClaw event {seq} of {native:?}"))?,
        };
        result.push(event(
            native,
            seq,
            &text,
            r.get(4)?,
            r.get(5)?,
            result.is_empty(),
        )?);
        ensure!(
            result.len() <= MAX_EVENTS,
            "OpenClaw event capture exceeds safety limit"
        );
    }
    Ok(result)
}
/// `(seq, entry id)` of the last `count` events, from the identity index
/// alone: proof that an earlier tail was not replaced.
pub(super) fn tail_ids(
    conn: &Connection,
    native: &str,
    count: usize,
) -> Result<Vec<(i64, String)>> {
    let mut stmt = conn.prepare(
        "SELECT seq, event_id FROM transcript_event_identities WHERE session_id=?1 ORDER BY seq DESC LIMIT ?2",
    )?;
    let mut ids = stmt
        .query_map(rusqlite::params![native, count as i64], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    ids.reverse();
    Ok(ids)
}
pub(super) fn ids_at(
    conn: &Connection,
    native: &str,
    from_seq: i64,
    to_seq: i64,
) -> Result<Vec<(i64, String)>> {
    let mut stmt = conn.prepare(
        "SELECT seq, event_id FROM transcript_event_identities WHERE session_id=?1 AND seq BETWEEN ?2 AND ?3 ORDER BY seq",
    )?;
    let ids = stmt
        .query_map(rusqlite::params![native, from_seq, to_seq], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(ids)
}
pub(super) fn event(
    native: &str,
    seq: i64,
    text: &str,
    created_at: i64,
    active: bool,
    first: bool,
) -> Result<Event> {
    let entry: Value = serde_json::from_str(text).with_context(|| {
        format!("OPENCLAW_INVALID_RECORD: event {seq} of {native:?} is not JSON")
    })?;
    format::validate(&entry, first)
        .with_context(|| format!("OpenClaw event {seq} of {native:?}"))?;
    Ok(Event {
        seq,
        created_at: storage::timestamp(created_at)?,
        active,
        entry,
    })
}
pub(super) fn decompress(blob: Option<Vec<u8>>, bytes: Option<i64>) -> Result<String> {
    let blob = blob.context("OPENCLAW_INVALID_RECORD: event has neither JSON nor zstd payload")?;
    let bytes = usize::try_from(bytes.unwrap_or(-1))
        .ok()
        .filter(|n| (1..=MAX_EVENT_BYTES).contains(n))
        .context("OPENCLAW_INVALID_RECORD: compressed event length out of bounds")?;
    let decoded = zstd::bulk::decompress(&blob, bytes).context("OPENCLAW_INVALID_RECORD: zstd")?;
    ensure!(
        decoded.len() == bytes,
        "OPENCLAW_INVALID_RECORD: compressed event length mismatch"
    );
    String::from_utf8(decoded).context("OPENCLAW_INVALID_RECORD: event is not UTF-8")
}
