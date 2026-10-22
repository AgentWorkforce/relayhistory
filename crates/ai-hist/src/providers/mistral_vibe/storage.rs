use super::*;
use sha2::{Digest, Sha256};
use std::io::Read;

const MAX_FILE: u64 = 64 * 1024 * 1024;
const MAX_CHUNKS: usize = 4096;

fn hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn read(
    context: &AcquisitionContext<'_>,
    path: &Path,
    paths: &mut Vec<(PathBuf, i64)>,
) -> Result<Vec<u8>> {
    let bytes = bounded_read(context, path, MAX_FILE)?;
    paths.push((path.to_owned(), bytes.len() as i64));
    Ok(bytes)
}

pub(super) fn bounded_read(
    context: &AcquisitionContext<'_>,
    path: &Path,
    limit: u64,
) -> Result<Vec<u8>> {
    context.validate(path)?;
    anyhow::ensure!(
        std::fs::metadata(path)?.len() <= limit,
        "VIBE_OVERSIZED_FILE: {}",
        path.display()
    );
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(limit + 1)
        .read_to_end(&mut bytes)?;
    anyhow::ensure!(bytes.len() as u64 <= limit, "VIBE_OVERSIZED_FILE");
    Ok(bytes)
}

fn document(bytes: &[u8], digest: Option<&str>) -> Result<Value> {
    anyhow::ensure!(
        bytes.ends_with(b"\n"),
        "VIBE_INVALID_DOCUMENT: no final newline"
    );
    let body = &bytes[..bytes.len() - 1];
    if let Some(expected) = digest {
        anyhow::ensure!(hex(body) == expected, "VIBE_DIGEST_MISMATCH");
    }
    Ok(serde_json::from_slice(body)?)
}

fn required<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value[key]
        .as_str()
        .filter(|s| !s.is_empty())
        .with_context(|| format!("VIBE_INVALID_DOCUMENT: missing {key}"))
}

fn generation(value: &Value) -> Result<&str> {
    let generation = required(value, "generation")?;
    anyhow::ensure!(
        generation.len() == 16 && generation.bytes().all(|b| b.is_ascii_digit()),
        "VIBE_INVALID_GENERATION"
    );
    Ok(generation)
}

fn digest(value: &Value) -> Result<&str> {
    let digest = value
        .as_str()
        .context("VIBE_INVALID_DIGEST: missing digest")?;
    anyhow::ensure!(
        digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit()),
        "VIBE_INVALID_DIGEST"
    );
    Ok(digest)
}

fn descriptor(
    context: &AcquisitionContext<'_>,
    base: &Path,
    value: &Value,
    expected: &str,
    paths: &mut Vec<(PathBuf, i64)>,
) -> Result<(Value, i64)> {
    anyhow::ensure!(
        required(value, "path")? == expected,
        "VIBE_INVALID_FILE_PATH"
    );
    let bytes = read(context, &base.join(expected), paths)?;
    let result = document(&bytes, Some(digest(&value["sha256"])?))?;
    Ok((result, bytes.len() as i64))
}

fn chunks(
    context: &AcquisitionContext<'_>,
    base: &Path,
    value: &Value,
    paths: &mut Vec<(PathBuf, i64)>,
) -> Result<(Vec<Value>, i64)> {
    let names = value
        .as_array()
        .context("VIBE_INVALID_CHUNKS: missing chunk list")?;
    anyhow::ensure!(names.len() <= MAX_CHUNKS, "VIBE_TOO_MANY_CHUNKS");
    let mut rows = Vec::new();
    let mut total = 0i64;
    for name in names {
        let name = digest(name)?;
        let bytes = read(
            context,
            &base.join("chunks").join(format!("{name}.json")),
            paths,
        )?;
        total += bytes.len() as i64;
        let value = document(&bytes, Some(name))?;
        rows.extend(
            value
                .as_array()
                .context("VIBE_INVALID_CHUNK: expected array")?
                .iter()
                .cloned(),
        );
    }
    Ok((rows, total))
}

fn journal(
    bytes: &[u8],
    first: u64,
    state: &mut Value,
    runtime: &mut Value,
    mut watermark: u64,
) -> Result<(usize, bool)> {
    let complete = bytes.last().is_none_or(|byte| *byte == b'\n');
    let final_index = bytes.iter().filter(|byte| **byte == b'\n').count();
    let mut expected = first;
    let mut previous: Option<String> = None;
    let mut count = 0;
    for (index, raw) in bytes.split(|b| *b == b'\n').enumerate() {
        let at_end = index == final_index;
        if at_end && raw.is_empty() {
            break;
        }
        anyhow::ensure!(
            !raw.is_empty(),
            "VIBE_INVALID_JOURNAL: empty interior record"
        );
        // The native reader ignores one unterminated final record. Do not
        // publish its prefix as complete evidence or advance the local cursor.
        if !complete && at_end {
            break;
        }
        let record: Value = serde_json::from_slice(raw)?;
        let marker = b",\"record_sha256\":\"";
        let start = raw
            .windows(marker.len())
            .rposition(|window| window == marker)
            .context("VIBE_INVALID_JOURNAL_DIGEST")?;
        let from = start + marker.len();
        anyhow::ensure!(
            raw.len() >= from + 65 && raw[from + 64] == b'"',
            "VIBE_INVALID_JOURNAL_DIGEST"
        );
        let mut unsigned = Vec::with_capacity(raw.len() - marker.len() - 65);
        unsigned.extend_from_slice(&raw[..start]);
        unsigned.extend_from_slice(&raw[from + 65..]);
        let actual = hex(&unsigned);
        anyhow::ensure!(
            record["record_sha256"] == actual,
            "VIBE_JOURNAL_DIGEST_MISMATCH"
        );
        anyhow::ensure!(
            record["sequence"].as_u64() == Some(expected),
            "VIBE_JOURNAL_SEQUENCE_GAP"
        );
        anyhow::ensure!(
            record["previous_record_sha256"].as_str() == previous.as_deref()
                || (previous.is_none() && record["previous_record_sha256"].is_null()),
            "VIBE_JOURNAL_CHAIN_MISMATCH"
        );
        anyhow::ensure!(
            matches!(
                record["type"].as_str(),
                Some(
                    "command_reserved"
                        | "core_input"
                        | "action_intent"
                        | "action_result"
                        | "process_operation_dispatched"
                        | "process_state_changed"
                        | "process_notification_submitted"
                        | "callback_registered"
                        | "callback_resolved"
                        | "receipt_succeeded"
                        | "receipt_failed"
                        | "projection_advanced"
                        | "projection_delta"
                        | "session_relocated"
                )
            ),
            "VIBE_UNKNOWN_JOURNAL_RECORD"
        );
        if matches!(
            record["type"].as_str(),
            Some("projection_advanced" | "projection_delta" | "session_relocated")
        ) {
            let next = record["payload"]["watermark"]
                .as_u64()
                .context("VIBE_INVALID_WATERMARK")?;
            anyhow::ensure!(next >= watermark, "VIBE_WATERMARK_REWIND");
            watermark = next;
            match record["type"].as_str() {
                Some("projection_advanced") => *state = record["payload"]["snapshot"].clone(),
                Some("session_relocated") => {
                    runtime["session_metadata"] = record["payload"]["metadata"].clone();
                    apply_delta(state, &record["payload"]["delta"])?;
                }
                _ => apply_delta(state, &record["payload"]["delta"])?,
            }
        }
        previous = Some(actual);
        expected += 1;
        count += 1;
    }
    Ok((count, !complete))
}

fn apply_delta(state: &mut Value, delta: &Value) -> Result<()> {
    let ops = delta
        .as_array()
        .context("VIBE_INVALID_DELTA: expected operations")?;
    for op in ops {
        if op["op"] == "set_envelope" {
            let retained = state["history"]["entries"]
                .as_array()
                .context("VIBE_INVALID_PROJECTION: missing history")?
                .clone();
            *state = op["state"].clone();
            state["history"]["entries"] = Value::Array(retained);
            continue;
        }
        let entries = state["history"]["entries"]
            .as_array_mut()
            .context("VIBE_INVALID_PROJECTION: missing history")?;
        match required(op, "op")? {
            "append_entry" => entries.push(op["entry"].clone()),
            "replace_entry" => {
                let id = required(op, "id")?;
                let old = entries
                    .iter_mut()
                    .find(|entry| entry["id"] == id)
                    .context("VIBE_INVALID_DELTA: absent replaced entry")?;
                *old = op["entry"].clone();
            }
            "remove_entry" => {
                let id = required(op, "id")?;
                let before = entries.len();
                entries.retain(|entry| entry["id"] != id);
                anyhow::ensure!(
                    entries.len() < before,
                    "VIBE_INVALID_DELTA: absent removed entry"
                );
            }
            "set_history_entries" => {
                *entries = op["entries"]
                    .as_array()
                    .context("VIBE_INVALID_DELTA: entries")?
                    .clone();
            }
            _ => anyhow::bail!("VIBE_UNSUPPORTED_DELTA"),
        }
    }
    Ok(())
}

pub(super) fn capture(path: &Path, context: &AcquisitionContext<'_>) -> Result<AcquiredSession> {
    let mut last = None;
    for _ in 0..2 {
        let pointer_before = bounded_read(context, path, 16 * 1024)?;
        match capture_once(path, context) {
            Ok(capture) => return Ok(capture),
            Err(error)
                if error.to_string().contains("VIBE_CONCURRENT_WRITE")
                    || bounded_read(context, path, 16 * 1024).ok().as_deref()
                        != Some(pointer_before.as_slice()) =>
            {
                last = Some(error);
            }
            Err(error) => return Err(error),
        }
    }
    Err(last.unwrap_or_else(|| anyhow::anyhow!("VIBE_CONCURRENT_WRITE")))
}

fn capture_once(path: &Path, context: &AcquisitionContext<'_>) -> Result<AcquiredSession> {
    let (sid, native) = roots::identity(context.roots, path)?;
    let session = path.parent().context("VIBE_INVALID_PATH")?;
    let mut paths = Vec::new();
    let current_bytes = read(context, path, &mut paths)?;
    let current = document(&current_bytes, None)?;
    anyhow::ensure!(
        current["store_format"] == "mistral.vibe.unified-session-store/v1",
        "VIBE_UNSUPPORTED_FORMAT"
    );
    anyhow::ensure!(
        current["store_format_minor"].as_u64() == Some(9),
        "VIBE_UNSUPPORTED_FORMAT_MINOR"
    );
    anyhow::ensure!(current["session_id"] == native, "VIBE_ID_MISMATCH");
    let generation = generation(&current)?;
    let base = session.join("generations").join(generation);
    let manifest_path = base.join("manifest.json");
    let manifest_bytes = read(context, &manifest_path, &mut paths)?;
    let manifest = document(&manifest_bytes, Some(digest(&current["manifest_sha256"])?))?;
    anyhow::ensure!(
        manifest["session_id"] == native && manifest["generation"] == generation,
        "VIBE_MANIFEST_MISMATCH"
    );
    anyhow::ensure!(
        manifest["snapshot_sequence"] == current["snapshot_sequence"],
        "VIBE_MANIFEST_SEQUENCE_MISMATCH"
    );
    // The native loader validates every referenced document and transcript,
    // including checkpoint and pooled runtime chunks, before publishing a
    // generation. Mirror that boundary even though only the projection is
    // normalized into RelayHistory evidence.
    let (checkpoint, _) = descriptor(
        context,
        &base,
        &manifest["checkpoint"],
        "checkpoint.json",
        &mut paths,
    )?;
    anyhow::ensure!(
        checkpoint["checkpoint_version"] == manifest["checkpoint"]["checkpoint_version"],
        "VIBE_CHECKPOINT_VERSION_MISMATCH"
    );
    if !manifest["checkpoint"]["chunks"].is_null() {
        chunks(
            context,
            session,
            &manifest["checkpoint"]["chunks"],
            &mut paths,
        )?;
    }
    let (projection, _) = descriptor(
        context,
        &base,
        &manifest["projection_state"],
        "projection-state.json",
        &mut paths,
    )?;
    let (mut runtime, _) = descriptor(
        context,
        &base,
        &manifest["runtime_state"],
        "runtime-state.json",
        &mut paths,
    )?;
    if !manifest["runtime_state"]["chunks"].is_null() {
        chunks(
            context,
            session,
            &manifest["runtime_state"]["chunks"],
            &mut paths,
        )?;
    }
    anyhow::ensure!(
        projection["session_id"] == native && runtime["session_id"] == native,
        "VIBE_STATE_ID_MISMATCH"
    );
    let mut state = projection["snapshot"].clone();
    if !manifest["projection_state"]["chunks"].is_null() {
        let (entries, _) = chunks(
            context,
            session,
            &manifest["projection_state"]["chunks"],
            &mut paths,
        )?;
        anyhow::ensure!(
            state["history"]["entries"]
                .as_array()
                .is_some_and(Vec::is_empty),
            "VIBE_INVALID_PROJECTION_ENVELOPE"
        );
        state["history"]["entries"] = Value::Array(entries);
    }
    let journal_name = required(&manifest["recovery_journal_segment"], "path")?;
    let first_journal_sequence = current["snapshot_sequence"]
        .as_u64()
        .and_then(|sequence| sequence.checked_add(1))
        .context("VIBE_INVALID_SEQUENCE")?;
    let expected_journal = format!("journal/{first_journal_sequence:016}.jsonl");
    anyhow::ensure!(
        journal_name == expected_journal,
        "VIBE_INVALID_JOURNAL_PATH"
    );
    anyhow::ensure!(
        manifest["recovery_journal_segment"]["first_sequence"].as_u64()
            == Some(first_journal_sequence),
        "VIBE_INVALID_JOURNAL_FIRST_SEQUENCE"
    );
    let journal_path = session.join(journal_name);
    let journal_bytes = read(context, &journal_path, &mut paths)?;
    let (journal_count, partial) = journal(
        &journal_bytes,
        first_journal_sequence,
        &mut state,
        &mut runtime,
        projection["watermark"]
            .as_u64()
            .context("VIBE_INVALID_WATERMARK")?,
    )?;
    anyhow::ensure!(!partial, "VIBE_PARTIAL_JOURNAL: incomplete final record");
    let after = bounded_read(context, path, 16 * 1024)?;
    let journal_after = bounded_read(context, &journal_path, MAX_FILE)?;
    anyhow::ensure!(
        after == current_bytes && journal_after == journal_bytes,
        "VIBE_CONCURRENT_WRITE"
    );
    anyhow::ensure!(
        state["session"]["id"] == native,
        "VIBE_PROJECTION_ID_MISMATCH"
    );
    let entries = state["history"]["entries"]
        .as_array()
        .context("VIBE_INVALID_HISTORY")?;
    anyhow::ensure!(
        entries.iter().all(|entry| entry["sessionId"] == native),
        "VIBE_ENTRY_ID_MISMATCH"
    );
    let bytes = paths.iter().map(|(_, size)| *size).sum::<i64>();
    let paths = paths.into_iter().map(|(path, _)| path).collect();
    Ok(AcquiredSession::new(
        serde_json::json!({"sid":sid,"native":native,"path":path,"state":state,"runtime":runtime,"partial":partial,"journal_count":journal_count}),
        bytes,
        entries.len() as i64 + journal_count as i64,
        paths,
    ))
}
