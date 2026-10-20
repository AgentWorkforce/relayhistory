use super::*;
pub(super) struct ShallowRead {
    pub rows: Vec<Value>,
    pub partial: bool,
    pub bounded: bool,
}
pub(super) fn shallow_rows(scan: &ScanEnv<'_>, path: &Path) -> Result<ShallowRead> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = std::fs::File::open(path)?;
    let length = file.metadata()?.len();
    let mut prefix = Vec::new();
    (&mut file).take(HEAD_LIMIT).read_to_end(&mut prefix)?;
    scan.note_open();
    scan.note_bytes(prefix.len() as u64);
    let (rows, mut partial) = format::parse(&prefix, length > HEAD_LIMIT)?;
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
        let (last, tail_partial) = format::parse(&tail[start..], false)?;
        partial |= tail_partial;
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
        "QWEN_CODE_INVALID_RECORD: no complete timed steps in bounded transcript"
    );
    // A single step can span both bounded windows. Keep its path-derived
    // conversation identity discoverable without inventing activity or text;
    // explicit hydration validates and reads the complete record.
    Ok(ShallowRead {
        rows,
        partial,
        bounded: length > HEAD_LIMIT,
    })
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
            r => {
                r?;
                context.validate(root)?;
            }
        }
        for project in std::fs::read_dir(root)? {
            let project = project?.path();
            context.validate(&project)?;
            if !project.is_dir() {
                continue;
            }
            let chats = project.join("chats");
            if !chats.try_exists()? {
                continue;
            }
            context.validate(&chats)?;
            for entry in std::fs::read_dir(chats)? {
                let path = entry?.path();
                if !admitted(env, &path) {
                    continue;
                }
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
    }
    out.sort_by(|a, b| a.locator.cmp(&b.locator));
    out.dedup_by(|a, b| a.locator == b.locator);
    Ok(out)
}
pub(super) fn capture(path: &Path, context: &AcquisitionContext<'_>) -> Result<AcquiredSession> {
    context.validate(path)?;
    let sid = roots::identity(context.roots, path)?;
    let bytes = context.read(path)?;
    let (rows, partial) = format::parse(&bytes, false)?;
    anyhow::ensure!(
        !rows.is_empty(),
        "QWEN_CODE_INVALID_RECORD: no complete records"
    );
    let native = path
        .file_stem()
        .and_then(|v| v.to_str())
        .context("invalid session filename")?;
    anyhow::ensure!(
        rows.iter().all(|r| r["sessionId"] == native),
        "QWEN_CODE_ID_MISMATCH: filename and native session disagree"
    );
    let active = format::project(&rows)?;
    anyhow::ensure!(
        !active.is_empty(),
        "QWEN_CODE_NO_CONVERSATION: only non-conversation artifacts recorded"
    );
    let hash = format!("{:x}", Sha256::digest(&bytes));
    let stamp = format!(
        "sha256:{:x}",
        Sha256::digest(serde_json::to_vec(&(&sid, &hash))?)
    );
    Ok(AcquiredSession {
        stamp,
        bytes: bytes.len() as i64,
        records: rows.len() as i64,
        read_paths: vec![path.to_owned()],
        payload: serde_json::json!({"sid":sid,"path":path,"rows":rows,"active":active,"partial":partial,"bytesSha256":hash}),
    })
}

fn admitted(env: &DiscoveryEnv<'_>, path: &Path) -> bool {
    path.extension().is_some_and(|v| v == "jsonl")
        && env.path_scope().is_none_or(|s| s.mentions(path))
}
