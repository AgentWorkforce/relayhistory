use super::*;
pub(super) fn shallow_rows(
    scan: &ScanEnv<'_>,
    path: &Path,
) -> Result<(Vec<Value>, Option<i64>, bool)> {
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
        .filter_map(|r| r.get("timestamp").and_then(Value::as_i64))
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
        // A record spanning both windows cannot be validated by discovery.
        // Treat an unparsed unterminated tail conservatively until hydration.
        partial |= tail_partial || (!tail.ends_with(b"\n") && last.is_empty());
        scan.note_records(last.len() as u64);
        for row in &last {
            if let Some(time) = row["timestamp"].as_i64() {
                latest = Some(latest.map_or(time, |t| t.max(time)));
            }
        }
    }
    let oversized_record = length > HEAD_LIMIT
        && prefix.iter().copied().find(|b| !b.is_ascii_whitespace()) == Some(b'{');
    anyhow::ensure!(
        latest.is_some() || oversized_record,
        "CODEBUDDY_INVALID_RECORD: no complete timed steps in bounded transcript"
    );
    // A single step can span both bounded windows. Keep its path-derived
    // conversation identity discoverable without inventing activity or text;
    // explicit hydration validates and reads the complete record.
    Ok((rows, latest, partial))
}
pub(super) fn selected(context: &AcquisitionContext<'_>, path: &Path) -> Result<PathBuf> {
    context.validate(path)?;
    roots::identity(context.roots, path)?;
    Ok(path.to_owned())
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
        for project in std::fs::read_dir(root)? {
            let project = project?.path();
            context.validate(&project)?;
            if !project.is_dir() || env.path_scope().is_some_and(|s| !s.mentions(&project)) {
                continue;
            }
            for entry in std::fs::read_dir(&project)? {
                crate::ingest::check_capture_cancelled()?;
                let path = entry?.path();
                if path.extension().is_none_or(|s| s != "jsonl")
                    || env.path_scope().is_some_and(|s| !s.mentions(&path))
                {
                    continue;
                }
                selected(&context, &path)?;
                let (stamp, recency_hint_ms) = crate::file_stamp_and_modified(&path)?;
                out.push(Candidate {
                    source: ID,
                    locator: path.to_string_lossy().into_owned(),
                    session_id: None,
                    stamp,
                    recency_hint_ms,
                });
            }
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
        rows.iter().any(|r| r["type"] == "message"),
        "CODEBUDDY_INVALID_RECORD: no complete messages"
    );
    let native = path.file_stem().and_then(|s| s.to_str()).unwrap();
    for row in &rows {
        anyhow::ensure!(
            row.get("sessionId").is_none_or(|s| s == native),
            "CODEBUDDY_ID_MISMATCH: record belongs to another session"
        );
        anyhow::ensure!(
            format::known_type(format::string(row, "type")?),
            "CODEBUDDY_UNSUPPORTED_REPLAY: unrecognized native control fact"
        );
    }
    let active = active_rows(&rows)?;
    let sid = roots::identity(context.roots, &path)?;
    let hash = format!("{:x}", Sha256::digest(&bytes));
    let stamp = format!(
        "sha256:{:x}",
        Sha256::digest(serde_json::to_vec(&(&sid, &hash))?)
    );
    let records = rows.len() as i64;
    Ok(AcquiredSession {
        stamp,
        bytes: bytes.len() as i64,
        records,
        read_paths: vec![path.clone()],
        payload: serde_json::json!({"sid":sid,"path":path,"rows":rows,"active":active,"partial":partial,"bytesSha256":hash}),
    })
}
pub(super) fn active_rows(rows: &[Value]) -> Result<Vec<Value>> {
    let pending = rows
        .iter()
        .rev()
        .find(|r| {
            !format::fact_type(r["type"].as_str().unwrap()) || r["type"] == "resend-fork-notice"
        })
        .filter(|r| r["type"] == "resend-fork-notice");
    if let Some(pending) = pending {
        return walk_history(rows, pending["parentId"].as_str());
    }
    let parents = rows.iter().any(|r| r["parentId"].is_string());
    let fork = rows.iter().any(|r| {
        r["type"] == "resend-fork-notice"
            || format::truthy(&r["providerData"]["contentFilterNotice"])
    });
    if !parents || !fork {
        return Ok(rows
            .iter()
            .filter(|r| !format::fact_type(r["type"].as_str().unwrap()))
            .cloned()
            .collect());
    }
    let tip = rows
        .iter()
        .rev()
        .find(|r| !format::fact_type(r["type"].as_str().unwrap()))
        .and_then(|r| r["id"].as_str());
    walk_history(rows, tip)
}
fn walk_history<'a>(rows: &'a [Value], mut tip: Option<&'a str>) -> Result<Vec<Value>> {
    let compacted = rows
        .iter()
        .any(|r| r["logicalParentId"].is_string() && !r["parentId"].is_string());
    let mut seen = std::collections::BTreeSet::new();
    let mut selected = std::collections::BTreeSet::new();
    let mut end = rows.len();
    while let Some(id) = tip {
        anyhow::ensure!(
            seen.insert(id),
            "CODEBUDDY_INVALID_REPLAY: cyclic native parent chain"
        );
        let mut index = (0..end)
            .rev()
            .find(|i| rows[*i]["id"] == id)
            .context("CODEBUDDY_INVALID_REPLAY: unresolved native parent")?;
        selected.insert(index);
        while index > 0 && rows[index - 1]["id"] == id {
            index -= 1;
            selected.insert(index);
        }
        let row = &rows[index];
        tip = row["parentId"].as_str().or_else(|| {
            (compacted || index == rows.len() - 1 || row["providerData"]["agent"] == "compact")
                .then(|| row["logicalParentId"].as_str())
                .flatten()
        });
        end = index;
    }
    Ok(selected
        .into_iter()
        .filter(|i| !format::fact_type(rows[*i]["type"].as_str().unwrap()))
        .map(|i| rows[i].clone())
        .collect())
}
