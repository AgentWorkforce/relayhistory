//! Related Codex SQLite capture and rollout restoration.
use super::*;
use sha2::{Digest, Sha256};
use std::io::Read as _;

const MAX_RELATED_THREADS: usize = 1_024;

/// A catalog observation can still name SQLite after the rollout has caught
/// up. Resolve that transition before selected hydration takes its stamp.
pub(crate) fn restored_rollout(
    roots: &crate::ProviderRoots,
    state: &Path,
    id: &str,
) -> Result<Option<PathBuf>> {
    let Some((current_state, history)) = databases(&roots.codex)? else {
        return Ok(None);
    };
    ensure!(
        std::fs::canonicalize(state)? == std::fs::canonicalize(current_state)?,
        "SESSION_SOURCE_MISMATCH: stale Codex SQLite locator"
    );
    let context = AcquisitionContext::new("codex", roots);
    let rollout_path: Option<String> = sqlite_snapshot(&context, state, |db| {
        db.query_row(
            "SELECT rollout_path FROM threads WHERE id=?1",
            [id],
            |row| row.get(0),
        )
        .optional()
        .map_err(Into::into)
    })?
    .flatten();
    let Some(path) = rollout_path.map(PathBuf::from) else {
        return Ok(None);
    };
    let offset: Option<i64> = sqlite_snapshot(&context, &history, |db| {
        db.query_row(
            "SELECT next_rollout_byte_offset FROM thread_history_projection_state WHERE thread_id=?1",
            [id],
            |row| row.get(0),
        )
        .optional()
        .map_err(Into::into)
    })?;
    let Ok(metadata) = crate::ingest::transcript_file::metadata(&path) else {
        return Ok(None);
    };
    context.validate(&path)?;
    if !metadata.is_file()
        || offset.is_some_and(|offset| offset < 0 || metadata.len() < offset as u64)
    {
        return Ok(None);
    }
    Ok(Some(path))
}

fn spawn_children(
    context: &AcquisitionContext<'_>,
    state: &Path,
    parent: &str,
) -> Result<Vec<String>> {
    sqlite_snapshot(context, state, |db| {
        let mut stmt = db.prepare("SELECT child_thread_id FROM thread_spawn_edges WHERE parent_thread_id=?1 ORDER BY child_thread_id LIMIT 100001")?;
        let children = stmt
            .query_map([parent], |row| row.get(0))?
            .collect::<rusqlite::Result<Vec<String>>>()?;
        ensure!(
            children.len() <= MAX_THREADS,
            "Codex edges exceed safety limit"
        );
        Ok(children)
    })
}

fn child_ids(capture: &Capture) -> Vec<String> {
    let mut children = capture
        .edges
        .iter()
        .filter(|edge| edge.parent == capture.thread.id)
        .map(|edge| edge.child.clone())
        .collect::<Vec<_>>();
    for item in &capture.items {
        match item.kind.as_str() {
            "subAgentActivity" => {
                if let Some(id) = item.value["agentThreadId"].as_str() {
                    children.push(id.to_owned());
                }
            }
            "collabAgentToolCall" => {
                if let Some(ids) = item.value["receiverThreadIds"].as_array() {
                    children.extend(ids.iter().filter_map(Value::as_str).map(str::to_owned));
                }
            }
            _ => {}
        }
    }
    children
}

fn rollout_content_stamp(path: &Path) -> Result<String> {
    let mut file = crate::ingest::transcript_file::open(path)?;
    let mut digest = Sha256::new();
    let mut chunk = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut chunk)?;
        if read == 0 {
            break;
        }
        digest.update(&chunk[..read]);
    }
    Ok(format!("sha256:{:x}", digest.finalize()))
}

fn related_rollout(
    context: &AcquisitionContext<'_>,
    state: &Path,
    id: &str,
    path: PathBuf,
) -> Result<(RelatedRollout, Vec<String>)> {
    let meta = crate::ingest::read_codex_session_meta(&path)?
        .ok_or_else(|| anyhow::anyhow!("related Codex rollout has no session metadata"))?;
    ensure!(
        meta.session_id == id,
        "related Codex rollout session id mismatch"
    );
    let children = spawn_children(context, state, id)?;
    let stamp = rollout_content_stamp(&path)?;
    Ok((
        RelatedRollout {
            id: id.to_owned(),
            path,
            stamp,
        },
        children,
    ))
}

/// Selected hydration captures every SQLite descendant named by spawn edges.
/// Rollout descendants contribute their file stamp and are ingested by the
/// rollout parser in the same destination transaction as the SQLite parent.
pub(crate) fn capture_related(
    roots: &crate::ProviderRoots,
    state: &Path,
    id: &str,
) -> Result<AcquiredSession> {
    let mut root_acquired = capture(roots, state, id)?;
    let mut root: Capture = serde_json::from_value(root_acquired.payload)?;
    let mut seen = HashSet::from([id.to_string()]);
    let mut pending = child_ids(&root);
    let context = AcquisitionContext::new("codex", roots);
    while let Some(child) = pending.pop() {
        if !seen.insert(child.clone()) {
            continue;
        }
        ensure!(
            seen.len() <= MAX_RELATED_THREADS,
            "Codex related thread capture exceeds safety limit"
        );
        let rollout = match restored_rollout(roots, state, &child) {
            Ok(rollout) => rollout,
            Err(error) if error.is::<crate::ingest::CaptureCancelled>() => return Err(error),
            Err(error) => {
                crate::ingest::scoped_files::provider_diagnostic(format_args!(
                    "[codex] skipping related thread {child}: {error:#}"
                ));
                root.related_incomplete = true;
                continue;
            }
        };
        if let Some(path) = rollout {
            match related_rollout(&context, state, &child, path) {
                Ok((rollout, children)) => {
                    pending.extend(children);
                    root_acquired.read_paths.push(rollout.path.clone());
                    root.related_rollouts.push(rollout);
                }
                Err(error) if error.is::<crate::ingest::CaptureCancelled>() => return Err(error),
                Err(error) => {
                    crate::ingest::scoped_files::provider_diagnostic(format_args!(
                        "[codex] skipping related rollout {child}: {error:#}"
                    ));
                    root.related_incomplete = true;
                }
            }
            continue;
        }
        let acquired = match capture(roots, state, &child) {
            Ok(acquired) => acquired,
            Err(error) if error.is::<crate::ingest::CaptureCancelled>() => return Err(error),
            Err(error) => {
                crate::ingest::scoped_files::provider_diagnostic(format_args!(
                    "[codex] skipping related thread {child}: {error:#}"
                ));
                root.related_incomplete = true;
                continue;
            }
        };
        let capture: Capture = serde_json::from_value(acquired.payload)?;
        pending.extend(child_ids(&capture));
        root_acquired.records += acquired.records;
        ensure!(
            root_acquired.records <= MAX_ITEMS as i64,
            "Codex related items exceed safety limit"
        );
        root_acquired.read_paths.extend(acquired.read_paths);
        root.related.push(capture);
    }
    root.related.sort_by(|a, b| a.thread.id.cmp(&b.thread.id));
    root.related_rollouts.sort_by(|a, b| a.id.cmp(&b.id));
    root_acquired.read_paths.sort();
    root_acquired.read_paths.dedup();
    Ok(AcquiredSession::new(
        serde_json::to_value(root)?,
        root_acquired.bytes,
        root_acquired.records,
        root_acquired.read_paths,
    ))
}
