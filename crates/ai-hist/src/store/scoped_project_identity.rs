use super::*;

/// Reconcile one hydrated session and its delegation descendants with indexed
/// identity lookups. Ancestor resolution uses the same cycle-safe ranking walk
/// as the global maintenance pass; unrelated events are never examined.
pub(crate) fn refresh_session_project_identity(
    conn: &Connection,
    source: &str,
    session_id: &str,
) -> Result<usize> {
    let mut pending = std::collections::VecDeque::from([(session_id.to_string(), 0usize)]);
    let mut seen = HashSet::new();
    let mut written = 0;
    while let Some((id, depth)) = pending.pop_front() {
        crate::ingest::check_capture_cancelled()?;
        if !seen.insert(id.clone()) {
            continue;
        }
        let row = conn
            .query_row(
                "SELECT cwd, repo_url, project_key, project_key_method, project_key_origin
             FROM sessions WHERE source = ? AND session_id = ?",
                params![source, id],
                |row| {
                    Ok(StoredProject {
                        cwd: row.get(0)?,
                        repo_url: row.get(1)?,
                        key: row.get(2)?,
                        method: row.get(3)?,
                        origin: row.get(4)?,
                    })
                },
            )
            .optional()?;
        let (key, method) = if let Some(row) = row {
            let StoredProject {
                key,
                method,
                repo_url,
                origin,
                ..
            } = reconsidered_session_identity(conn, source, &id, row)?;
            written += conn.execute(
                "UPDATE sessions SET project_key = ?3, project_key_method = ?4, repo_url = ?5, project_key_origin = ?6
                 WHERE source = ?1 AND session_id = ?2
                   AND (project_key IS NOT ?3 OR project_key_method IS NOT ?4 OR repo_url IS NOT ?5 OR project_key_origin IS NOT ?6)",
                params![source, id, key, method, repo_url, origin],
            )?;
            (key, method)
        } else {
            (
                lendable_ancestor_key_for(conn, source, &id)?,
                Some("inherited".into()),
            )
        };
        if let Some(key) = key {
            let rank = project_key_method_rank(method.as_deref());
            let stored = event_key_rank_sql("project_key", "project_key_method");
            written+=conn.execute(&format!("UPDATE session_events SET project_key=?3,project_key_method=?4 WHERE source=?1 AND session_id=?2 AND (({stored})<?5 OR (({stored})=?5 AND project_key IS NOT ?3))"),params![source,id,key,method,rank])?;
        }
        if depth < PROJECT_KEY_INHERITANCE_PASSES {
            let children=conn.prepare("SELECT DISTINCT child_session_id FROM session_relationships WHERE source=? AND parent_session_id=? AND child_session_id IS NOT NULL")?.query_map(params![source,id],|r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
            pending.extend(children.into_iter().map(|child| (child, depth + 1)));
        }
    }
    Ok(written)
}

/// Whether a session's key is still open to reconsideration: none yet, or one
/// that is provisional (`path`) or borrowed (`inherited`).
fn project_key_is_unsettled(key: Option<&str>, method: Option<&str>) -> bool {
    key.is_none() || matches!(method, Some("path" | "inherited"))
}

struct StoredProject {
    cwd: Option<String>,
    repo_url: Option<String>,
    key: Option<String>,
    method: Option<String>,
    origin: Option<String>,
}

/// Reconsider a session's own repository before borrowing an ancestor's key.
fn reconsidered_session_identity(
    conn: &Connection,
    source: &str,
    id: &str,
    mut row: StoredProject,
) -> Result<StoredProject> {
    use crate::project_identity::{
        catalog_identity_observation, codex_project_key_origin, safe_catalog_project_key,
        settled_catalog_identity,
    };
    if row.method.as_deref() == Some("remote") && row.key.is_some() {
        (row.key, row.repo_url) = settled_catalog_identity(
            source,
            row.cwd.as_deref(),
            row.repo_url.as_deref(),
            row.key.as_deref(),
        );
        return Ok(row);
    }
    if !project_key_is_unsettled(row.key.as_deref(), row.method.as_deref()) {
        return Ok(row);
    }
    let (own, recorded) = catalog_identity_observation(
        row.cwd.as_deref(),
        row.repo_url.as_deref(),
        source == "claude",
    );
    row.repo_url = None;
    if let Some((key, how, url)) = own {
        if row.key.is_none() || how.as_str() == "remote" || row.method.as_deref() == Some("path") {
            row.key = Some(key);
            row.method = Some(how.as_str().to_string());
            row.repo_url = url;
            row.origin = codex_project_key_origin(source, row.method.as_deref(), recorded, true)
                .map(str::to_owned);
        }
    }
    if project_key_is_unsettled(row.key.as_deref(), row.method.as_deref()) {
        if let Some(parent) = lendable_ancestor_key_for(conn, source, id)? {
            row.key = Some(safe_catalog_project_key(&parent).to_owned());
            row.method = Some("inherited".into());
            row.origin = None;
        }
    }
    Ok(row)
}

fn project_key_method_rank(method: Option<&str>) -> i32 {
    match method {
        Some("remote") => 3,
        Some("inherited") => 2,
        Some("path") => 1,
        _ => 0,
    }
}

/// Strength of a stored project identity, as a SQL expression over `alias`.
///
/// Upserts merge on this ranking so a later, weaker observation can never
/// overwrite a stronger one: a shallow rescan that cannot see the repository
/// any more must not replace a resolved remote key with the directory it was
/// run from. `0` is "nothing stored", which every real value outranks.
fn project_key_rank_sql(alias: &str) -> String {
    format!(
        "CASE WHEN {alias}.project_key IS NULL THEN 0 \
              WHEN {alias}.project_key_method = 'remote' THEN 3 \
              WHEN {alias}.project_key_method = 'inherited' THEN 2 \
              ELSE 1 END"
    )
}

/// The `ON CONFLICT ... DO UPDATE SET` fragment that merges project identity
/// into the `sessions` table. Shared by shallow discovery and full ingest so
/// the two cannot disagree about precedence. A recorded remote can replace
/// both the key and the URL, even when only its key is suitable for storage.
pub(crate) fn project_key_merge_sql(recorded: &str) -> String {
    let incoming = project_key_rank_sql("excluded");
    let stored = project_key_rank_sql("sessions");
    let incoming_wins = format!(
        "({incoming}) >= ({stored}) AND NOT (
            sessions.source = 'codex' AND sessions.project_key_method IS 'remote'
            AND sessions.project_key IS NOT NULL AND NOT ({recorded})
            AND (sessions.repo_url IS NOT NULL OR sessions.project_key_origin IS NOT 'checkout')
        )"
    );
    format!(
        "project_key = CASE WHEN {incoming_wins} \
            THEN excluded.project_key ELSE sessions.project_key END, \
         project_key_method = CASE WHEN {incoming_wins} \
            THEN excluded.project_key_method ELSE sessions.project_key_method END, \
         project_key_origin = CASE WHEN {incoming_wins} \
            THEN excluded.project_key_origin ELSE sessions.project_key_origin END, \
         repo_url = CASE WHEN ({recorded}) THEN excluded.repo_url \
            ELSE COALESCE(excluded.repo_url, sessions.repo_url) END"
    )
}
