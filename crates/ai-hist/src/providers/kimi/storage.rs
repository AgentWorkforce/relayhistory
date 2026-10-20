use super::*;
pub(super) fn selected(context: &AcquisitionContext<'_>, path: &Path) -> Result<PathBuf> {
    context.validate(path)?;
    roots::identity(context.roots, path)?;
    Ok(path.to_owned())
}
fn directories(context: &AcquisitionContext<'_>, root: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    if !root.try_exists()? {
        return Ok(out);
    }
    context.validate(root)?;
    for entry in std::fs::read_dir(root)? {
        crate::ingest::check_capture_cancelled()?;
        let path = entry?.path();
        context.validate(&path)?;
        if path.is_dir() {
            out.push(path)
        }
    }
    Ok(out)
}
pub(super) fn enumerate(env: &DiscoveryEnv<'_>) -> Result<Vec<Candidate>> {
    env.provider_roots.ensure_provider_roots(ID)?;
    let context = AcquisitionContext::new(ID, &env.provider_roots);
    let mut out = Vec::new();
    for root in env.provider_roots.for_provider(ID) {
        if env.path_scope().is_some_and(|s| !s.mentions(root)) {
            continue;
        }
        for bucket in directories(&context, root)? {
            for session in directories(&context, &bucket)? {
                session_candidates(env, &context, &session, &mut out)?
            }
        }
    }
    out.sort_by(|a, b| a.locator.cmp(&b.locator));
    out.dedup_by(|a, b| a.locator == b.locator);
    Ok(out)
}
fn session_candidates(
    env: &DiscoveryEnv<'_>,
    context: &AcquisitionContext<'_>,
    session: &Path,
    out: &mut Vec<Candidate>,
) -> Result<()> {
    for agent in directories(context, &session.join("agents"))? {
        let path = agent.join("wire.jsonl");
        if !path.try_exists()?
            || env
                .path_scope()
                .is_some_and(|s| !s.mentions(&path) && !s.mentions(&session.join("state.json")))
        {
            continue;
        }
        context.validate(&path)?;
        let (wire_stamp, recency_hint_ms) = crate::file_stamp_and_modified(&path)?;
        let state = session.join("state.json");
        let (state_stamp, _) = (|| {
            context.validate(&state)?;
            crate::file_stamp_and_modified(&state)
        })()
        .unwrap_or_else(|_| ("unreadable-state".into(), None));
        let stamp = format!("{wire_stamp}:{state_stamp}");
        out.push(Candidate {
            source: ID,
            locator: path.to_string_lossy().into_owned(),
            session_id: None,
            recency_hint_ms,
            stamp,
        });
    }
    Ok(())
}
pub(super) fn capture(path: &Path, context: &AcquisitionContext<'_>) -> Result<AcquiredSession> {
    let path = selected(context, path)?;
    let state_path = path
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .context("Kimi session parent")?
        .join("state.json");
    let state_bytes = context.read(&state_path)?;
    let state: Value = serde_json::from_slice(&state_bytes).context("KIMI_INVALID_STATE")?;
    anyhow::ensure!(
        state["version"] == 2,
        "KIMI_UNSUPPORTED_STATE: requires version2"
    );
    validate_state(&path, &state)?;
    let bytes = context.read(&path)?;
    let (rows, partial) = format::parse(&bytes)?;
    let sid = roots::identity(context.roots, &path)?;
    let records = rows.len() as i64;
    let stamp = format!(
        "sha256:{:x}",
        Sha256::digest([bytes.as_slice(), state_bytes.as_slice()].concat())
    );
    Ok(AcquiredSession {
        stamp,
        payload: serde_json::json!({"sid":sid,"path":path,"rows":rows,"state":state,"partial":partial}),
        bytes: (bytes.len() + state_bytes.len()) as i64,
        records,
        read_paths: vec![path, state_path],
    })
}

pub(super) fn shallow(
    scan: &ScanEnv<'_>,
    context: &AcquisitionContext<'_>,
    path: &Path,
) -> Result<AcquiredSession> {
    use std::io::{Read, Seek, SeekFrom};
    let path = selected(context, path)?;
    let state_path = path
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .context("Kimi parent")?
        .join("state.json");
    let state_bytes = context.read(&state_path)?;
    let state: Value = serde_json::from_slice(&state_bytes)?;
    anyhow::ensure!(
        state["version"] == 2,
        "KIMI_UNSUPPORTED_STATE: requires version2"
    );
    validate_state(&path, &state)?;
    let mut file = std::fs::File::open(&path)?;
    let length = file.metadata()?.len();
    let mut head = Vec::new();
    (&mut file).take(64 * 1024).read_to_end(&mut head)?;
    scan.note_open();
    scan.note_bytes(head.len() as u64);
    if length > head.len() as u64 {
        if let Some(end) = head.iter().rposition(|b| *b == b'\n') {
            head.truncate(end + 1)
        } else {
            anyhow::bail!("KIMI_INVALID_RECORD: oversized metadata")
        }
    }
    let (mut rows, partial) = format::parse(&head)?;
    if length > 64 * 1024 {
        let offset = length.saturating_sub(64 * 1024).max(head.len() as u64);
        file.seek(SeekFrom::Start(offset.saturating_sub(1)))?;
        let mut tail = Vec::new();
        file.read_to_end(&mut tail)?;
        scan.note_bytes(tail.len() as u64);
        let start = tail
            .iter()
            .position(|b| *b == b'\n')
            .map_or(tail.len(), |n| n + 1);
        for line in tail[start..].split_inclusive(|b| *b == b'\n') {
            match serde_json::from_slice::<Value>(line) {
                Ok(row) => {
                    format::time(&row)?;
                    rows.push(row)
                }
                Err(e) if e.is_eof() && !line.ends_with(b"\n") => {}
                Err(e) => return Err(e).context("KIMI_INVALID_RECORD: malformed tail"),
            }
        }
    }
    scan.note_records(rows.len() as u64);
    let records = rows.len() as i64;
    let sid = roots::identity(context.roots, &path)?;
    Ok(AcquiredSession {
        stamp: String::new(),
        payload: serde_json::json!({"sid":sid,"path":path,"rows":rows,"state":state,"bounded":length>64*1024,"partial":partial}),
        bytes: 0,
        records,
        read_paths: vec![path, state_path],
    })
}

fn validate_state(path: &Path, state: &Value) -> Result<()> {
    let native = path
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .and_then(Path::file_name)
        .context("Kimi native id")?;
    anyhow::ensure!(
        state["id"].as_str() == native.to_str(),
        "KIMI_ID_MISMATCH: state id differs from directory"
    );

    Ok(())
}
