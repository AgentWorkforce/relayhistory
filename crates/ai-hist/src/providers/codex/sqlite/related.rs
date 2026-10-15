//! Related Codex SQLite capture and rollout restoration.
use super::*;

const MAX_RELATED_THREADS: usize = 1_024;

/// A catalog observation can still name SQLite after the rollout has caught
/// up. Resolve that transition before selected hydration takes its stamp.
pub(crate) fn restored_rollout(
    roots: &crate::ProviderRoots,
    state: &Path,
    id: &str,
) -> Result<Option<PathBuf>> {
    let (current_state, history) =
        databases(&roots.codex)?.context("Codex SQLite history unavailable")?;
    ensure!(
        std::fs::canonicalize(state)? == std::fs::canonicalize(current_state)?,
        "SESSION_SOURCE_MISMATCH: stale Codex SQLite locator"
    );
    let context = AcquisitionContext::new("codex", roots);
    let rollout_path: Option<String> = sqlite_snapshot(&context, state, |db| {
        db.query_row(
            "SELECT rollout_path FROM threads WHERE id=?1 AND history_mode='paginated'",
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
    if !offset.is_some_and(|offset| {
        offset >= 0
            && path
                .metadata()
                .is_ok_and(|meta| meta.len() >= offset as u64)
    }) {
        return Ok(None);
    }
    context.validate(&path)?;
    Ok(Some(path))
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
    let mut pending = root
        .edges
        .iter()
        .filter(|edge| edge.parent == id)
        .map(|edge| edge.child.clone())
        .collect::<Vec<_>>();
    while let Some(child) = pending.pop() {
        if !seen.insert(child.clone()) {
            continue;
        }
        ensure!(
            seen.len() <= MAX_RELATED_THREADS,
            "Codex related thread capture exceeds safety limit"
        );
        if let Some(path) = restored_rollout(roots, state, &child)? {
            let context = AcquisitionContext::new("codex", roots);
            let (_, edges) = thread_and_edges(&context, state, &child)?;
            pending.extend(
                edges
                    .iter()
                    .filter(|edge| edge.parent == child)
                    .map(|edge| edge.child.clone()),
            );
            let (stamp, _) = crate::file_stamp_and_modified(&path)?;
            root.related_rollouts.push(RelatedRollout {
                id: child,
                path: path.clone(),
                stamp,
            });
            root_acquired.read_paths.push(path);
            continue;
        }
        let acquired = capture(roots, state, &child)?;
        let capture: Capture = serde_json::from_value(acquired.payload)?;
        pending.extend(
            capture
                .edges
                .iter()
                .filter(|edge| edge.parent == child)
                .map(|edge| edge.child.clone()),
        );
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
