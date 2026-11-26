//! Catalog-wide filesystem identity repair, reserved for full maintenance.
use super::*;
use std::collections::{BTreeMap, BTreeSet};

/// Resolve provisional catalog identities and backfill safe URLs for settled
/// remotes. A session's remote outranks an inherited key, which outranks a
/// path. Settled remote keys retain their repository when the checkout moves.
pub(super) fn resolve_missing_project_keys(conn: &Connection) -> Result<usize> {
    // Every reconsidered row, with the key it holds, in one read. The UPDATE
    // below is keyed on `(cwd, repo_url)`, which no index serves, so asking
    // "does any row of this pair need it?" in SQL was one scan of the catalog
    // per distinct directory — O(directories x sessions) on every sync and
    // every discovery pass, to conclude, almost always, that nothing moves.
    // The same predicate is evaluated here over the rows already read.
    #[cfg(test)]
    WORK.with(|work| work.borrow_mut().queries += 1);
    type Observation = (String, Option<String>, Option<String>);
    let mut pending: BTreeMap<Observation, BTreeSet<Held>> = BTreeMap::new();
    {
        let mut statement = conn.prepare(
            "SELECT cwd, repo_url, project_key, project_key_method, source FROM sessions \
             WHERE (project_key IS NULL \
                    OR project_key_method IN ('path', 'inherited')
                    OR (source = 'codex' AND repo_url IS NOT NULL)
                    OR (repo_url IS NOT NULL AND (repo_url GLOB '*://*@*'
                        OR instr(repo_url, '?') > 0 OR instr(repo_url, '#') > 0))
                    OR (project_key_method = 'remote' AND (instr(project_key, '?') > 0
                        OR instr(project_key, '#') > 0))
                    OR (source = 'claude' AND project_key_method = 'remote' AND repo_url IS NULL)) \
               AND (cwd IS NOT NULL OR repo_url IS NOT NULL OR project_key_method = 'remote')",
        )?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            #[cfg(test)]
            WORK.with(|work| work.borrow_mut().rows += 1);
            pending
                .entry((row.get(4)?, row.get(0)?, row.get(1)?))
                .or_default()
                .insert((row.get(2)?, row.get(3)?));
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
                 AND (?2 = 'remote' OR project_key <> ?1 OR repo_url IS NOT ?5)) \
             OR (project_key_method = 'inherited' AND ?2 = 'remote')) \
           AND cwd IS ?3 AND repo_url IS ?4 AND source = ?6";
    let mut update = conn.prepare(&format!(
        "UPDATE sessions SET project_key = ?1, project_key_method = ?2, repo_url = ?5 WHERE {UPGRADABLE}"
    ))?;
    let mut settled_update = conn.prepare(
        "UPDATE sessions SET repo_url = ?1, project_key = ?6 WHERE cwd IS ?2 AND repo_url IS ?3
         AND source = ?4 AND project_key IS ?5 AND project_key_method = 'remote'",
    )?;
    let mut written = 0;
    for ((source, cwd, repo_url), held) in pending {
        written += repair_settled_remotes(
            &mut settled_update,
            &source,
            cwd.as_deref(),
            repo_url.as_deref(),
            &held,
        )?;
        written += resolve_provisional_identity(
            &mut update,
            &source,
            cwd.as_deref(),
            repo_url.as_deref(),
            &held,
        )?;
    }
    Ok(written)
}

type Held = (Option<String>, Option<String>);

fn repair_settled_remotes(
    update: &mut rusqlite::Statement<'_>,
    source: &str,
    cwd: Option<&str>,
    repo_url: Option<&str>,
    held: &BTreeSet<Held>,
) -> Result<usize> {
    let mut written = 0;
    for (key, method) in held {
        if method.as_deref() != Some("remote") {
            continue;
        }
        let (safe_key, safe_url) = crate::project_identity::settled_catalog_identity(
            source,
            cwd,
            repo_url,
            key.as_deref(),
        );
        if safe_url.as_deref() != repo_url || &safe_key != key {
            written += update.execute(params![safe_url, cwd, repo_url, source, key, safe_key])?;
        }
    }
    Ok(written)
}

fn resolve_provisional_identity(
    update: &mut rusqlite::Statement<'_>,
    source: &str,
    cwd: Option<&str>,
    repo_url: Option<&str>,
    held: &BTreeSet<Held>,
) -> Result<usize> {
    if held
        .iter()
        .all(|(key, method)| key.is_some() && method.as_deref() == Some("remote"))
    {
        return Ok(0);
    }
    let Some((key, method, safe)) =
        crate::project_identity::catalog_identity(cwd, repo_url, source == "claude")
    else {
        return Ok(0);
    };
    // `UPGRADABLE`, row by row, over what the read above returned.
    let remote = method == crate::project_identity::ProjectKeyMethod::Remote;
    let needed =
        held.iter().any(
            |(held_key, held_method)| match (held_key, held_method.as_deref()) {
                (None, _) => true,
                (Some(held_key), Some("path")) => {
                    remote || held_key != &key || safe.as_deref() != repo_url
                }
                (Some(_), Some("inherited")) => remote,
                _ => false,
            },
        );
    if !needed {
        return Ok(0);
    }
    Ok(update.execute(params![key, method.as_str(), cwd, repo_url, safe, source])?)
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
