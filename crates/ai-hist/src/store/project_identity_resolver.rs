//! Catalog-wide filesystem identity repair, reserved for full maintenance.
use super::*;
use std::collections::BTreeMap;

/// Pass 1: stamp sessions with no key, and upgrade ones stuck on a borrowed
/// or provisional key.
///
/// A path key is not a settled answer, it is the absence of one. Only filling
/// NULLs would freeze it: `upsert_session` resolves from the working directory
/// alone, so a session whose checkout has since been deleted or moved gets a
/// path key, and nothing would ever revisit it — not this pass, which would
/// see a non-NULL key, and not shallow discovery, which skips a source whose
/// bytes have not changed. The repository's own `repo_url`, which the Codex
/// reader records, then sits in the same row unused while every worktree of
/// that repository files under a different key.
///
/// So a `path` row is reconsidered on every pass and replaced only by
/// something strictly better. An `inherited` row is reconsidered too, for the
/// mirror-image reason: the parent's repository is a stand-in, and a child
/// that later gains its own `origin` would otherwise wear the parent's key for
/// as long as its transcript stayed unchanged — which is forever.
///
/// Nothing here can downgrade a key. `remote` replaces `path` or `inherited`;
/// `path` replaces only `path` or NULL, never an inherited key, because a
/// borrowed repository says more about a session than the directory it
/// happened to run in.
pub(super) fn resolve_missing_project_keys(conn: &Connection) -> Result<usize> {
    // Every reconsidered row, with the key it holds, in one read. The UPDATE
    // below is keyed on `(cwd, repo_url)`, which no index serves, so asking
    // "does any row of this pair need it?" in SQL was one scan of the catalog
    // per distinct directory — O(directories x sessions) on every sync and
    // every discovery pass, to conclude, almost always, that nothing moves.
    // The same predicate is evaluated here over the rows already read.
    #[cfg(test)]
    WORK.with(|work| work.borrow_mut().queries += 1);
    type Held = (Option<String>, Option<String>);
    let mut pending: BTreeMap<(Option<String>, Option<String>), Vec<Held>> = BTreeMap::new();
    {
        let mut statement = conn.prepare(
            "SELECT cwd, repo_url, project_key, project_key_method FROM sessions \
             WHERE (project_key IS NULL \
                    OR project_key_method IN ('path', 'inherited')) \
               AND (cwd IS NOT NULL OR repo_url IS NOT NULL)",
        )?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            #[cfg(test)]
            WORK.with(|work| work.borrow_mut().rows += 1);
            pending
                .entry((row.get(0)?, row.get(1)?))
                .or_default()
                .push((row.get(2)?, row.get(3)?));
        }
    }
    if pending.is_empty() {
        return Ok(0);
    }
    // The predicate the update would apply, asked as a question first.
    //
    // In the steady state every one of these rows resolves to the key it
    // already has: a `path` key is reconsidered on every pass precisely
    // because it might be upgradable, and almost always is not. Issuing the
    // UPDATE anyway takes the write lock to change nothing, on every sync, for
    // every path-keyed session -- and a `SQLITE_BUSY` on that no-op would fail
    // the whole refresh, taking inheritance and denormalization down with it.
    // The other two passes probe for the same reason.
    //
    // An `inherited` key is reconsidered too, but displaced only by a
    // `remote`: it is a borrowed key, so it outranks a path and loses to the
    // child's own repository. A child that later gains its own `origin` would
    // otherwise keep the parent's repository for as long as its transcript
    // stayed unchanged, which is forever.
    const UPGRADABLE: &str = "(project_key IS NULL \
             OR (project_key_method = 'path' \
                 AND (?2 = 'remote' OR project_key <> ?1)) \
             OR (project_key_method = 'inherited' AND ?2 = 'remote')) \
           AND cwd IS ?3 AND repo_url IS ?4";
    let mut update = conn.prepare(&format!(
        "UPDATE sessions SET project_key = ?1, project_key_method = ?2 WHERE {UPGRADABLE}"
    ))?;
    let mut written = 0;
    for ((cwd, repo_url), held) in pending {
        let Some((key, method)) =
            crate::project_identity::identity_for(cwd.as_deref(), repo_url.as_deref())
        else {
            continue;
        };
        // `UPGRADABLE`, row by row, over what the read above returned.
        let remote = method == crate::project_identity::ProjectKeyMethod::Remote;
        let needed =
            held.iter().any(
                |(held_key, held_method)| match (held_key, held_method.as_deref()) {
                    (None, _) => true,
                    (Some(held_key), Some("path")) => remote || held_key != &key,
                    (Some(_), Some("inherited")) => remote,
                    _ => false,
                },
            );
        if !needed {
            continue;
        }
        written += update.execute(params![key, method.as_str(), cwd, repo_url])?;
    }
    Ok(written)
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Work {
    pub queries: usize,
    pub rows: usize,
    pub observations: usize,
}
#[cfg(test)]
thread_local! {
    static WORK: std::cell::RefCell<Work> = const { std::cell::RefCell::new(Work { queries: 0, rows: 0, observations: 0 }) };
}
#[cfg(test)]
pub(crate) fn take_work() -> Work {
    WORK.with(|work| std::mem::take(&mut *work.borrow_mut()))
}

#[cfg(test)]
pub(crate) fn note_observation() {
    WORK.with(|work| work.borrow_mut().observations += 1);
}
