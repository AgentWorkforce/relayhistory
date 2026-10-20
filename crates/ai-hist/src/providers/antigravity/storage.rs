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
        if env.path_scope().is_some_and(|s| !s.mentions(root)) {
            continue;
        }
        match std::fs::metadata(root) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            result => {
                result?;
                context.validate(root)?;
            }
        }
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
            if env.path_scope().is_some_and(|s| !s.mentions(&dir)) {
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
            out.push(Candidate {
                source: ID,
                locator: path.to_string_lossy().into_owned(),
                session_id: None,
                recency_hint_ms,
                stamp,
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
        path.file_name().unwrap() != "transcript_full.jsonl" || !truncated,
        "ANTIGRAVITY_INVALID_RECORD: full transcript declares truncation"
    );
    let sid = roots::identity(context.roots, &path)?;
    let incomplete = partial
        || truncated
        || rows
            .iter()
            .any(|r| r["status"] != "DONE" && r["status"] != "ERROR");
    let bytes_hash = format!("{:x}", Sha256::digest(&bytes));
    // The absolute locator is needed by normalization, but a temporary HOME
    // relocation is not an evidence revision. Identity, projection selection
    // and exact source bytes still distinguish every meaningful capture.
    let revision = serde_json::to_vec(&(
        &sid,
        path.file_name().unwrap().to_str().unwrap(),
        &bytes_hash,
    ))?;
    let stamp = format!("sha256:{:x}", Sha256::digest(revision));
    let records = rows.len() as i64;
    let payload = serde_json::json!({"sid":sid,"path":path,"rows":rows,"partial":incomplete,"bytesSha256":bytes_hash});
    Ok(AcquiredSession {
        stamp,
        payload,
        bytes: bytes.len() as i64,
        records,
        read_paths: vec![path],
    })
}

pub(super) fn shallow_rows(scan: &ScanEnv<'_>, path: &Path) -> Result<(Vec<Value>, i64)> {
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
    Ok((
        rows,
        latest.context(
            "ANTIGRAVITY_INVALID_RECORD: bounded windows contain no complete timed steps",
        )?,
    ))
}
