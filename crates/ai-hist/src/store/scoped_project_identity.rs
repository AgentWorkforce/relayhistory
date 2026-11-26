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
        let row=conn.query_row("SELECT cwd,repo_url,project_key,project_key_method FROM sessions WHERE source=? AND session_id=?",params![source,id],|r|Ok((r.get::<_,Option<String>>(0)?,r.get::<_,Option<String>>(1)?,r.get::<_,Option<String>>(2)?,r.get::<_,Option<String>>(3)?))).optional()?;
        let (key, method) = if let Some((cwd, repo_url, key, method)) = row {
            let (key, method, repo_url) =
                reconsidered_session_identity(conn, source, &id, cwd, repo_url, key, method)?;
            written += conn.execute(
                "UPDATE sessions SET project_key = ?3, project_key_method = ?4, repo_url = ?5
                 WHERE source = ?1 AND session_id = ?2
                   AND (project_key IS NOT ?3 OR project_key_method IS NOT ?4 OR repo_url IS NOT ?5)",
                params![source, id, key, method, repo_url],
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

type SessionProjectIdentity = (Option<String>, Option<String>, Option<String>);

/// Reconsider a session's own repository before borrowing an ancestor's key.
fn reconsidered_session_identity(
    conn: &Connection,
    source: &str,
    id: &str,
    cwd: Option<String>,
    repo_url: Option<String>,
    mut key: Option<String>,
    mut method: Option<String>,
) -> Result<SessionProjectIdentity> {
    use crate::project_identity::{
        catalog_identity, safe_catalog_project_key, settled_catalog_identity,
    };
    let capture_origin = source == "claude";
    if method.as_deref() == Some("remote") && key.is_some() {
        let (safe_key, safe_url) =
            settled_catalog_identity(source, cwd.as_deref(), repo_url.as_deref(), key.as_deref());
        return Ok((safe_key, method, safe_url));
    }
    if !project_key_is_unsettled(key.as_deref(), method.as_deref()) {
        return Ok((key, method, repo_url));
    }
    let mut safe_url = None;
    if let Some((own, how, url)) =
        catalog_identity(cwd.as_deref(), repo_url.as_deref(), capture_origin)
    {
        if key.is_none() || how.as_str() == "remote" || method.as_deref() == Some("path") {
            key = Some(own);
            method = Some(how.as_str().to_string());
            safe_url = url;
        }
    }
    if project_key_is_unsettled(key.as_deref(), method.as_deref()) {
        if let Some(parent) = lendable_ancestor_key_for(conn, source, id)? {
            key = Some(safe_catalog_project_key(&parent).to_owned());
            method = Some("inherited".into());
        }
    }
    Ok((key, method, safe_url))
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
pub(crate) fn project_key_merge_sql(recorded: &str, preserve_remote: &str) -> String {
    let incoming = project_key_rank_sql("excluded");
    let stored = project_key_rank_sql("sessions");
    let incoming_wins = format!(
        "({incoming}) >= ({stored}) AND NOT (
            sessions.source = 'codex' AND sessions.project_key_method IS 'remote'
            AND sessions.project_key IS NOT NULL AND NOT ({recorded}) AND (sessions.repo_url IS NOT NULL OR ({preserve_remote}))
        )"
    );
    format!(
        "project_key = CASE WHEN {incoming_wins} \
            THEN excluded.project_key ELSE sessions.project_key END, \
         project_key_method = CASE WHEN {incoming_wins} \
            THEN excluded.project_key_method ELSE sessions.project_key_method END, \
         repo_url = CASE WHEN ({recorded}) THEN excluded.repo_url \
            ELSE COALESCE(excluded.repo_url, sessions.repo_url) END"
    )
}
