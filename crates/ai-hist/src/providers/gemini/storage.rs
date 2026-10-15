use super::*;
use sha2::{Digest, Sha256};
pub(super) fn identity(roots: &ProviderRoots, path: &Path, native: &str) -> Result<String> {
    let root = roots
        .for_provider(ID)
        .iter()
        .filter(|r| path.starts_with(r))
        .max_by_key(|r| r.components().count())
        .context("GEMINI_ROOT: path outside roots")?;
    if roots.default_for_provider(ID) == Some(root.as_path()) {
        return Ok(native.into());
    }
    Ok(format!(
        "root:{:x}:{native}",
        Sha256::digest(root.as_os_str().as_encoded_bytes())
    ))
}
pub(super) fn enumerate(env: &DiscoveryEnv<'_>) -> Result<Vec<Candidate>> {
    let mut out = Vec::new();
    let context = AcquisitionContext::new(ID, &env.provider_roots);
    for root in env.provider_roots.for_provider(ID) {
        if env.path_scope().is_some_and(|s| !s.mentions(root)) {
            continue;
        }
        match std::fs::metadata(root) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            result => {
                result.with_context(|| format!("Gemini root {}", root.display()))?;
            }
        }
        context.validate(root)?;
        for extension in ["jsonl", "json"] {
            for path in
                crate::ingest::collect_matching_files_scoped(root, "", extension, env.path_scope())?
            {
                let relative = path.strip_prefix(root)?;
                let parts: Vec<_> = relative.components().collect();
                // Only project/chats/session and project/chats/parent/child evidence trees.
                if !(parts.len() == 3 || parts.len() == 4) || parts[1].as_os_str() != "chats" {
                    continue;
                }
                crate::ingest::check_capture_cancelled()?;
                // A bad pair must remain a candidate so shallow discovery reports
                // its per-file diagnostic and sweep counts it unread, without
                // preventing unrelated healthy sessions from being acquired.
                if skip_legacy(&context, &path).unwrap_or(false) {
                    continue;
                }
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
    // JSONL wins only for the matching sibling validated by superseded().
    out.sort_by(|a, b| b.locator.cmp(&a.locator));
    out.dedup_by(|a, b| a.locator == b.locator);
    Ok(out)
}
pub(super) fn superseded(
    context: &AcquisitionContext<'_>,
    path: &Path,
    native: &str,
    project: &str,
) -> Result<bool> {
    let sibling = path.with_extension(if path_is_legacy(path) {
        "jsonl"
    } else {
        "json"
    });
    if !sibling.try_exists()? {
        return Ok(false);
    }
    let bytes = prefix(context, &sibling)?;
    let header = format::header(&bytes, path_is_legacy(&sibling))?;
    anyhow::ensure!(
        format::string(&header, "sessionId")? == native
            && format::string(&header, "projectHash")? == project,
        "GEMINI_ID_COLLISION: legacy migration sibling identity differs"
    );
    Ok(path_is_legacy(path))
}

fn skip_legacy(context: &AcquisitionContext<'_>, path: &Path) -> Result<bool> {
    if !path_is_legacy(path) || !path.with_extension("jsonl").try_exists()? {
        return Ok(false);
    }
    let bytes = prefix(context, path)?;
    let header = format::header(&bytes, true)?;
    superseded(
        context,
        path,
        format::string(&header, "sessionId")?,
        format::string(&header, "projectHash")?,
    )
}

pub(super) fn retire_missing_legacy(conn: &Connection, roots: &ProviderRoots) -> Result<()> {
    let mut statement = conn.prepare(
        "SELECT DISTINCT raw_path FROM sessions WHERE source='gemini' AND raw_path IS NOT NULL",
    )?;
    let paths = statement
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for path in paths {
        let path = PathBuf::from(path);
        if path_is_legacy(&path) && legacy_missing(roots, &path)? {
            let tx = conn.unchecked_transaction()?;
            crate::providers::file_history::retire_missing_locator(&tx, ID, &path)?;
            tx.commit()?;
        }
    }
    Ok(())
}
fn legacy_missing(roots: &ProviderRoots, path: &Path) -> Result<bool> {
    let context = AcquisitionContext::new(ID, roots);
    let Some(root) = roots.for_provider(ID).iter().find(|r| path.starts_with(r)) else {
        return Ok(false);
    };
    if !root.is_dir() {
        return Ok(false);
    }
    match std::fs::symlink_metadata(path) {
        Ok(_) => return Ok(false),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    let mut parent = path.parent();
    while let Some(dir) = parent {
        if dir.try_exists()? {
            context.validate(dir)?;
            for entry in std::fs::read_dir(dir)? {
                entry?;
            }
        }
        if dir == root {
            return Ok(!path.try_exists()?);
        }
        parent = dir.parent();
    }
    Ok(false)
}

pub(super) fn recent_time(
    scan: &ScanEnv<'_>,
    path: &Path,
    head: &[u8],
    header: &Value,
) -> Result<i64> {
    use std::io::Seek;
    if path_is_legacy(path) {
        return format::date(header, "lastUpdated");
    }
    let (head_latest, records) = format::recent_time(head, false, header)?;
    scan.note_records(records);
    if head.len() < HEAD_LIMIT as usize {
        return Ok(head_latest);
    }
    let context = AcquisitionContext::new(ID, scan.provider_roots);
    context.validate(path)?;
    let mut file = std::fs::File::open(path)?;
    scan.note_open();
    let length = file.metadata()?.len();
    let (tail, offset) = if length <= head.len() as u64 {
        (head.to_vec(), 0)
    } else {
        let offset = length.saturating_sub(HEAD_LIMIT);
        file.seek(std::io::SeekFrom::Start(offset))?;
        let mut tail = Vec::new();
        file.take(HEAD_LIMIT).read_to_end(&mut tail)?;
        scan.note_bytes(tail.len() as u64);
        (tail, offset)
    };
    let (latest, records) = format::recent_time(&tail, offset > 0, header)?;
    scan.note_records(records);
    Ok(latest.max(head_latest))
}
