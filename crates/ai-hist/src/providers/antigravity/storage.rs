use super::*;

pub(super) fn selected(context: &AcquisitionContext<'_>, path: &Path) -> Result<PathBuf> {
    context.validate(path)?;
    anyhow::ensure!(
        matches!(
            path.file_name().and_then(|s| s.to_str()),
            Some("transcript.jsonl" | "transcript_full.jsonl")
        ),
        "ANTIGRAVITY_INVALID_PATH: not a transcript"
    );
    let full = path.with_file_name("transcript_full.jsonl");
    if full.try_exists()? {
        context.validate(&full)?;
        Ok(full)
    } else {
        Ok(path.to_owned())
    }
}
pub(super) fn enumerate(env: &DiscoveryEnv<'_>) -> Result<Vec<Candidate>> {
    env.provider_roots.ensure_provider_roots(ID)?;
    let context = AcquisitionContext::new(ID, &env.provider_roots);
    let mut out = Vec::new();
    for root in env.provider_roots.for_provider(ID) {
        let summary_touched = env
            .path_scope()
            .is_some_and(|s| summary::scope_mentions(s, root));
        if !summary_touched && env.path_scope().is_some_and(|s| !s.mentions(root)) {
            continue;
        }
        match std::fs::metadata(root) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            result => {
                result?;
                context.validate(root)?;
            }
        }
        let titles = summary::read_titles_for_brain(root, &context);
        for entry in std::fs::read_dir(root)? {
            crate::ingest::check_capture_cancelled()?;
            let dir = entry?.path();
            context.validate(&dir)?;
            if !dir.is_dir() {
                continue;
            }
            let logs = dir.join(".system_generated/logs");
            let full = logs.join("transcript_full.jsonl");
            let compact = logs.join("transcript.jsonl");
            // A scoped compact-file notification must also revisit its full
            // sibling: SetStep and Branch can rewrite the selected projection.
            if !summary_touched && env.path_scope().is_some_and(|s| !s.mentions(&dir)) {
                continue;
            }
            let path = if full.try_exists()? {
                full
            } else if compact.try_exists()? {
                compact
            } else {
                continue;
            };
            context.validate(&path)?;
            roots::identity(&env.provider_roots, &path)?;
            let (stamp, recency_hint_ms) = crate::file_stamp_and_modified(&path)?;
            let title = dir
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|id| titles.get(id))
                .map(String::as_str);
            out.push(Candidate {
                source: ID,
                locator: path.to_string_lossy().into_owned(),
                session_id: None,
                recency_hint_ms,
                stamp: summary::stamp_with_title(stamp, title),
            });
        }
    }
    out.sort_by(|a, b| a.locator.cmp(&b.locator));
    out.dedup_by(|a, b| a.locator == b.locator);
    Ok(out)
}
pub(super) fn capture(path: &Path, context: &AcquisitionContext<'_>) -> Result<AcquiredSession> {
    let path = selected(context, path)?;
    let bytes = context.read(&path)?;
    let name = path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow::anyhow!("ANTIGRAVITY_INVALID_PATH: not a transcript"))?;
    let (rows, partial) = format::parse(&bytes, false)?;
    anyhow::ensure!(
        !rows.is_empty(),
        "ANTIGRAVITY_INVALID_RECORD: no complete steps"
    );
    let truncated = rows.iter().any(|r| {
        r["is_truncated"] == true
            || r["truncated_fields"]
                .as_array()
                .is_some_and(|a| !a.is_empty())
    });
    anyhow::ensure!(
        name != "transcript_full.jsonl" || !truncated,
        "ANTIGRAVITY_INVALID_RECORD: full transcript declares truncation"
    );
    let sid = roots::identity(context.roots, &path)?;
    let incomplete = partial
        || truncated
        || rows
            .iter()
            .any(|r| r["status"] != "DONE" && r["status"] != "ERROR");
    let bytes_hash = format!("{:x}", Sha256::digest(&bytes));
    let Subagents {
        entries: subagents,
        paths: subagent_paths,
        hashes: subagent_hashes,
        unreadable,
        listed,
    } = subagents(&path, context)?;
    let (title, summary_path) = summary::read_title_for_transcript(&path, context);
    // The absolute locator is needed by normalization, but a temporary HOME
    // relocation is not an evidence revision. Identity, projection selection
    // and exact source bytes still distinguish every meaningful capture.
    // Subagent entries and summary titles join the revision only when present,
    // so a conversation without them keeps its stamp, and one with them is
    // captured again when they appear or change.
    let revision = match (subagent_hashes.is_empty(), title.as_deref()) {
        (true, None) => serde_json::to_vec(&(&sid, name, &bytes_hash))?,
        (false, None) => serde_json::to_vec(&(&sid, name, &bytes_hash, &subagent_hashes))?,
        (true, Some(title)) => serde_json::to_vec(&(&sid, name, &bytes_hash, title))?,
        (false, Some(title)) => {
            serde_json::to_vec(&(&sid, name, &bytes_hash, &subagent_hashes, title))?
        }
    };
    let stamp = format!("sha256:{:x}", Sha256::digest(revision));
    let records = rows.len() as i64;
    let payload = serde_json::json!({"sid":sid,"path":path,"rows":rows,"partial":incomplete,"bytesSha256":bytes_hash,"title":title,"subagents":subagents,"subagentsListed":listed,"subagentsUnreadable":unreadable});
    let mut read_paths = vec![path];
    if let Some(summary_path) = summary_path {
        read_paths.push(summary_path);
    }
    read_paths.extend(subagent_paths);
    Ok(AcquiredSession {
        stamp,
        payload,
        bytes: bytes.len() as i64,
        records,
        read_paths,
    })
}

/// The delegations a conversation recorded: one
/// `<conversation>/.system_generated/subagents/<child>.json` per spawned
/// subagent, naming the child's own conversation (a sibling under the same
/// brain root). The child id is taken from `conversationId`, never from the
/// file name; an entry without a usable one is not evidence of a link.
///
/// Sidecars are secondary evidence: one that cannot be validated, read or
/// parsed never fails the parent's capture. It is reported as unreadable
/// instead, so normalization keeps whatever link it evidenced before, and a
/// directory that cannot be listed leaves every earlier link in place.
#[derive(Default)]
struct Subagents {
    entries: Vec<Value>,
    paths: Vec<PathBuf>,
    hashes: Vec<(String, String)>,
    unreadable: Vec<String>,
    /// Whether the directory was listed, so an absent sidecar is established.
    listed: bool,
}
fn subagents(path: &Path, context: &AcquisitionContext<'_>) -> Result<Subagents> {
    let mut out = Subagents::default();
    // path = <root>/<conversation>/.system_generated/logs/<transcript>
    let Some(conversation) = path.ancestors().nth(3) else {
        return Ok(out);
    };
    let Some(root) = conversation.parent() else {
        return Ok(out);
    };
    let dir = conversation.join(".system_generated/subagents");
    let files = match std::fs::read_dir(&dir) {
        Ok(entries) => entries
            .map(|e| e.map(|e| e.path()))
            .collect::<std::io::Result<Vec<_>>>(),
        Err(e) => Err(e),
    };
    let mut files = match files {
        Ok(files) => files,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            out.listed = true;
            return Ok(out);
        }
        Err(_) => return Ok(out),
    };
    out.listed = true;
    files.retain(|p| p.extension().is_some_and(|e| e == "json"));
    files.sort();
    for file in files {
        crate::ingest::check_capture_cancelled()?;
        let locator = file.to_string_lossy().into_owned();
        let bytes = match context.validate(&file).and_then(|_| context.read(&file)) {
            Ok(bytes) => bytes,
            // Removed between the listing and the read: established absence.
            Err(_) if !file.try_exists().unwrap_or(true) => continue,
            Err(_) => {
                out.unreadable.push(locator);
                continue;
            }
        };
        // Hashed by name, not locator: HOME relocation is not a revision.
        let name = file
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let hash = format!("{:x}", Sha256::digest(&bytes));
        let Ok(entry) = serde_json::from_slice::<Value>(&bytes) else {
            // A sidecar caught mid-rewrite: keep its old link for now, and
            // let its hash move the stamp so the next capture retries it.
            out.hashes.push((name, hash));
            out.unreadable.push(locator);
            continue;
        };
        out.hashes.push((name, hash));
        let Some(child) = entry["conversationId"]
            .as_str()
            .filter(|c| !c.is_empty() && Path::new(c).components().count() == 1)
            .filter(|c| !matches!(*c, "." | ".."))
        else {
            continue;
        };
        let child_path = root
            .join(child)
            .join(".system_generated/logs/transcript.jsonl");
        let Ok(child_sid) = roots::identity(context.roots, &child_path) else {
            continue;
        };
        out.entries.push(serde_json::json!({
            "childSid": child_sid,
            "agentType": entry["subagentDescriptor"]["typeName"].as_str(),
            "agentName": entry["subagentDescriptor"]["role"].as_str(),
            "spawnStepIndex": entry["spawnStepIndex"].as_u64(),
            "locator": locator,
        }));
        out.paths.push(file);
    }
    Ok(out)
}

pub(super) fn shallow_rows(scan: &ScanEnv<'_>, path: &Path) -> Result<(Vec<Value>, Option<i64>)> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = std::fs::File::open(path)?;
    let length = file.metadata()?.len();
    let mut prefix = Vec::new();
    (&mut file).take(HEAD_LIMIT).read_to_end(&mut prefix)?;
    scan.note_open();
    scan.note_bytes(prefix.len() as u64);
    let (rows, _) = format::parse(&prefix, length > HEAD_LIMIT)?;
    scan.note_records(rows.len() as u64);
    let mut latest = rows
        .iter()
        .map(format::time)
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .max();
    if length > HEAD_LIMIT {
        let offset = length.saturating_sub(HEAD_LIMIT);
        file.seek(SeekFrom::Start(offset.saturating_sub(1)))?;
        let mut tail = Vec::new();
        file.read_to_end(&mut tail)?;
        scan.note_bytes(tail.len() as u64);
        // Include complete newline-free records only at physical EOF; discard
        // the boundary fragment even if it happens to be parseable JSON.
        let start = if tail.first() == Some(&b'\n') {
            1
        } else {
            tail.iter()
                .position(|b| *b == b'\n')
                .map_or(tail.len(), |n| n + 1)
        };
        let (last, _) = format::parse(&tail[start..], false)?;
        scan.note_records(last.len() as u64);
        for row in &last {
            let time = format::time(row)?;
            latest = Some(latest.map_or(time, |t| t.max(time)));
        }
    }
    let oversized_record = length > HEAD_LIMIT
        && prefix.iter().copied().find(|b| !b.is_ascii_whitespace()) == Some(b'{');
    anyhow::ensure!(
        latest.is_some() || oversized_record,
        "ANTIGRAVITY_INVALID_RECORD: no complete timed steps in bounded transcript"
    );
    // A single step can span both bounded windows. Keep its path-derived
    // conversation identity discoverable without inventing activity or text;
    // explicit hydration validates and reads the complete record.
    Ok((rows, latest))
}

/// Each enumerated conversation's subagent sidecars, stamped like the
/// transcripts so a sync fingerprint sees one appear, change or vanish.
pub(super) fn subagent_candidates(transcripts: &[Candidate]) -> Result<Vec<Candidate>> {
    let mut out = Vec::new();
    for transcript in transcripts {
        let Some(conversation) = Path::new(&transcript.locator).ancestors().nth(3) else {
            continue;
        };
        let dir = conversation.join(".system_generated/subagents");
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        let mut files = entries
            .map(|e| e.map(|e| e.path()))
            .collect::<std::io::Result<Vec<_>>>()?;
        files.retain(|p| p.extension().is_some_and(|e| e == "json"));
        files.sort();
        for file in files {
            // A sidecar that vanished between the listing and the stat is
            // simply absent from this fingerprint.
            let Ok((stamp, recency_hint_ms)) = crate::file_stamp_and_modified(&file) else {
                continue;
            };
            out.push(Candidate {
                source: ID,
                locator: file.to_string_lossy().into_owned(),
                session_id: None,
                recency_hint_ms,
                stamp,
            });
        }
    }
    Ok(out)
}
