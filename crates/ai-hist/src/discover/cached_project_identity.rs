use super::*;
use crate::project_identity::ProjectKeyMethod;

/// Re-resolve the project identity of a row served straight from the catalog.
///
/// The stamp shortcut is right about everything the transcript says and wrong
/// about the one field derived from the filesystem beside it. A session first
/// seen before its checkout had an `origin` would otherwise keep its path key
/// on every later pass, because the transcript never changes and so the row is
/// never reconsidered.
///
/// This runs where the row is read rather than as a sweep at the end of the
/// pass, because the row is *streamed*: `on_row` hands it to the caller as
/// soon as the window is decided, so a later correction would fix the catalog
/// and still have emitted the stale value — the JSONL a consumer parses would
/// disagree with the database it came from.
///
/// For the same reason, the answer it streams must be the answer the
/// end-of-pass refresh will store, so a cached child that is about to inherit
/// its parent's repository is given that key here rather than the path its own
/// directory resolves to.
///
/// Only a key that is absent, a path, or inherited can change, and an
/// inherited one only for the child's own `remote`. A settled remote can
/// backfill its URL or drop a legacy URL suffix. The write is skipped
/// entirely when the answer is the one already stored, which is the usual
/// case: these are reconsidered because they *might* be upgradable, and almost
/// never are.
pub(super) fn upgrade_cached_project_identity(conn: &Connection, row: &mut ShallowSession) -> Result<()> {
    #[cfg(test)]
    crate::store::project_identity_resolver::note_observation();
    let stored = row.project_key_method.as_deref();
    let original_url = row.repo_url.clone();
    if stored == Some(ProjectKeyMethod::Remote.as_str()) {
        let (safe_key, safe_url) = crate::project_identity::settled_catalog_identity(
            &row.source,
            row.cwd.as_deref(),
            original_url.as_deref(),
            row.project_key.as_deref(),
        );
        if safe_url != original_url || safe_key != row.project_key {
            let changed = conn.execute(
                "UPDATE sessions SET repo_url = ?1, project_key = ?2 WHERE source = ?3 AND session_id = ?4
                 AND repo_url IS ?5 AND project_key IS ?6 AND cwd IS ?7 AND project_key_method = 'remote'",
                params![safe_url, safe_key, row.source, row.session_id, original_url, row.project_key, row.cwd],
            )?;
            if changed == 0 {
                reload_cached_project_identity(conn, row)?;
            } else {
                row.project_key = safe_key;
                row.repo_url = safe_url;
            }
        }
        return Ok(());
    }
    let (resolved, recorded) = crate::project_identity::catalog_identity_observation(
        row.cwd.as_deref(),
        original_url.as_deref(),
        row.source == "claude",
    );
    row.repo_url = resolved.as_ref().and_then(|(_, _, url)| url.clone());
    let resolved = resolved.map(|(key, method, _)| (key, method));
    let candidate = match resolved {
        // The session's own repository beats anything borrowed.
        Some((key, ProjectKeyMethod::Remote)) => Some((key, ProjectKeyMethod::Remote)),
        // Anything weaker has to be weighed against what the end-of-pass
        // refresh is about to do to this row. Deciding that here, and not only
        // in the pass, is what keeps the row this function streams equal to
        // the row the pass will store: a consumer reading
        // `sessions discover --json` beside the catalog must not be told two
        // different projects.
        //
        // `inheritable_parent_project_key` is asked even when this row already
        // holds a borrowed key, because the key it borrowed can go stale — the
        // same refresh may promote its parent to a `remote` of its own, and
        // pass 2 then lends the new one down.
        weaker => {
            match crate::store::inheritable_parent_project_key(
                conn,
                &row.source,
                &row.session_id,
            )? {
                Some(parent) => Some((
                    crate::project_identity::safe_catalog_project_key(&parent).to_owned(),
                    ProjectKeyMethod::Inherited,
                )),
                // An inherited key is borrowed, so it outranks a path: a child
                // whose directory still resolves to nothing canonical keeps
                // the parent's repository, which is the point of inheriting it.
                None if stored == Some(ProjectKeyMethod::Inherited.as_str()) => None,
                None => weaker,
            }
        }
    };
    let Some((key, method)) = candidate else {
        if stored == Some("inherited") {
            row.project_key = row
                .project_key
                .as_deref()
                .map(crate::project_identity::safe_catalog_project_key)
                .map(str::to_owned);
        }
        return Ok(());
    };
    if row.project_key.as_deref() == Some(key.as_str())
        && stored == Some(method.as_str())
        && row.repo_url == original_url
    {
        return Ok(());
    }
    let origin = crate::project_identity::codex_project_key_origin(
        &row.source, Some(method.as_str()), recorded, true,
    );
    let changed = conn.execute(
        "UPDATE sessions SET project_key = ?1, project_key_method = ?2, repo_url = ?5, project_key_origin = ?8
         WHERE source = ?3 AND session_id = ?4 AND repo_url IS ?6 AND cwd IS ?7
           AND (project_key IS NULL OR project_key_method = 'path'
                OR (project_key_method = 'inherited' AND ?2 IN ('remote', 'inherited')))",
        params![key, method.as_str(), row.source, row.session_id, row.repo_url, original_url, row.cwd, origin],
    )?;
    if changed == 0 {
        reload_cached_project_identity(conn, row)?;
        return Ok(());
    }
    row.project_key = Some(key);
    row.project_key_method = Some(method.as_str().to_string());
    Ok(())
}

fn reload_cached_project_identity(conn: &Connection, row: &mut ShallowSession) -> Result<()> {
    if let Some(stored) = catalog_row(conn, &row.source, &row.session_id, false)? {
        row.project_key = stored.project_key;
        row.project_key_method = stored.project_key_method;
        row.repo_url = stored.repo_url;
    }
    Ok(())
}
