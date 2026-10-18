//! Cold transcript archives (`session-cold-storage-codec.ts`). Cold storage
//! deletes a session's `transcript_events` rows after writing one verified
//! zstd JSONL envelope; until upstream restores it, the archive is the
//! session's only transcript.
use super::*;
use anyhow::{ensure, Context};
use sha2::{Digest, Sha256};
/// Upstream's own bound on a decoded archive.
const MAX_ARCHIVE_BYTES: usize = 64 * 1024 * 1024;

/// `<agents>/<id>/sessions/cold/<sha256>.jsonl.zst` beside an `agent/`
/// database directory, else `cold/` beside the database.
pub(super) fn archive_path(database: &Path, name: &str) -> Result<PathBuf> {
    ensure!(
        name.len() == 74
            && name.ends_with(".jsonl.zst")
            && name[..64]
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "OPENCLAW_INVALID_RECORD: invalid cold archive name"
    );
    let dir = database
        .parent()
        .context("OpenClaw database has no parent")?;
    let sessions = if dir.file_name().is_some_and(|n| n == "agent") {
        dir.parent()
            .context("OpenClaw agent directory has no parent")?
            .join("sessions")
    } else {
        dir.to_path_buf()
    };
    Ok(sessions.join("cold").join(name))
}

pub(super) fn events(
    context: &AcquisitionContext<'_>,
    conn: &Connection,
    database: &Path,
    native: &str,
    archive: &ColdArchive,
) -> Result<Vec<Event>> {
    let bytes = match archive.storage.as_str() {
        "sqlite" => conn
            .query_row(
                "SELECT archive_blob FROM session_transcript_cold_archives WHERE session_id=?1",
                [native],
                |r| r.get::<_, Option<Vec<u8>>>(0),
            )?
            .context("OPENCLAW_INVALID_RECORD: cold archive blob missing")?,
        "file" => context
            .read(&archive_path(database, &archive.name)?)
            .context("OpenClaw cold transcript archive is missing or unreadable")?,
        other => anyhow::bail!("OPENCLAW_INVALID_RECORD: cold archive storage {other:?}"),
    };
    let digest: String = Sha256::digest(&bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    ensure!(
        bytes.len() as i64 == archive.archive_bytes && digest == archive.sha256,
        "OpenClaw cold transcript archive {} failed verification",
        archive.name
    );
    decode(native, archive, &bytes)
}

fn decode(native: &str, archive: &ColdArchive, bytes: &[u8]) -> Result<Vec<Event>> {
    let mut decoded = Vec::new();
    let mut decoder = zstd::stream::read::Decoder::new(bytes)?;
    std::io::Read::read_to_end(
        &mut std::io::Read::take(&mut decoder, MAX_ARCHIVE_BYTES as u64 + 1),
        &mut decoded,
    )
    .context("OPENCLAW_INVALID_RECORD: cold archive zstd")?;
    ensure!(
        decoded.len() <= MAX_ARCHIVE_BYTES,
        "OpenClaw cold archive exceeds {MAX_ARCHIVE_BYTES} bytes"
    );
    let text = std::str::from_utf8(&decoded)
        .context("OPENCLAW_INVALID_RECORD: cold archive is not UTF-8")?;
    let mut lines = text.trim_end().split('\n');
    let header: Value = serde_json::from_str(lines.next().unwrap_or_default())
        .context("OPENCLAW_INVALID_RECORD: cold archive header")?;
    ensure!(
        header["kind"] == "header"
            && header["version"] == 1
            && header["sessionId"] == native
            && header["generation"] == archive.generation.as_str(),
        "OpenClaw cold archive header does not match its session"
    );
    let mut rows = vec![];
    let mut active = std::collections::BTreeSet::new();
    for line in lines {
        crate::ingest::check_capture_cancelled()?;
        let record: Value =
            serde_json::from_str(line).context("OPENCLAW_INVALID_RECORD: cold archive record")?;
        let row = &record["row"];
        match record["kind"].as_str() {
            Some("event") => rows.push((
                row["seq"].as_i64().context("cold event seq")?,
                row["event_json"]
                    .as_str()
                    .context("cold event_json")?
                    .to_owned(),
                row["created_at"]
                    .as_i64()
                    .context("cold event created_at")?,
            )),
            Some("active") => {
                active.insert(row["event_seq"].as_i64().context("cold active event_seq")?);
            }
            Some("header") => anyhow::bail!("OpenClaw cold archive has a second header"),
            // Identity, index and FTS records are derived projections.
            _ => {}
        }
    }
    let raw: usize = rows.iter().map(|(_, json, _)| json.len()).sum::<usize>() + rows.len();
    ensure!(
        rows.len() as i64 == archive.event_count
            && rows.last().map(|r| r.0) == Some(archive.last_seq)
            && raw as i64 - 1 == archive.raw_bytes,
        "OpenClaw cold archive metadata does not match its contents"
    );
    rows.sort_by_key(|r| r.0);
    rows.iter()
        .enumerate()
        .map(|(i, (seq, json, created))| {
            events::event(native, *seq, json, *created, active.contains(seq), i == 0)
        })
        .collect()
}

/// Encode an archive exactly as upstream's cold-storage worker lays it out.
#[cfg(test)]
pub(super) fn encode(native: &str, generation: &str, rows: &[(i64, String, i64)]) -> Vec<u8> {
    let mut lines = vec![
        serde_json::json!({"kind":"header","version":1,"sessionId":native,"generation":generation})
            .to_string(),
    ];
    for (seq, json, created) in rows {
        lines.push(serde_json::json!({"kind":"event","row":{"seq":seq,"event_json":json,"created_at":created}}).to_string());
    }
    for (position, (seq, _, _)) in rows.iter().enumerate().skip(1) {
        lines.push(serde_json::json!({"kind":"active","row":{"active_position":position - 1,"event_seq":seq,"message_position":null,"context_eligible":1}}).to_string());
    }
    zstd::bulk::compress((lines.join("\n") + "\n").as_bytes(), 3).unwrap()
}
