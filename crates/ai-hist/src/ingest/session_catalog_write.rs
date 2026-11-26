use super::*;

/// One `sessions` catalog row as an ingest pass writes it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SessionCatalogRow<'a> {
    pub(crate) session_id: &'a str,
    pub(crate) source: &'a str,
    pub(crate) cwd: Option<&'a str>,
    pub(crate) git_branch: Option<&'a str>,
    /// The activity window. A time that is not positive is not known and is
    /// stored as `NULL`, never as 0.
    pub(crate) first_ts: i64,
    pub(crate) last_ts: i64,
    pub(crate) last_assistant_text: Option<&'a str>,
    pub(crate) raw_path: Option<&'a str>,
}

/// Register a session whose full evidence (`session_events`, `tool_calls`, …)
/// has just been ingested, so the row is marked `discovery_state = 'full'`.
/// Shallow discovery writes through [`discover::upsert_shallow_session`]
/// instead and never downgrades a `'full'` row.
pub(crate) fn upsert_session(conn: &Connection, row: &SessionCatalogRow<'_>) -> Result<()> {
    upsert_session_inner(conn, row, ActivityWindow::Expand, None)
}

/// How an upsert reconciles the activity window it carries with the one the
/// row already has.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ActivityWindow {
    /// Widen the stored window — the default, and right whenever this pass saw
    /// only part of the session.
    Expand,
    /// Replace both endpoints. Only for a caller that just re-read the
    /// session's *entire* source and is therefore authoritative about both
    /// ends. A rebuild that expanded instead would keep an endpoint an earlier
    /// parser derived from the file mtime forever: MAX() can never retract it,
    /// because a real recorded timestamp is almost always smaller.
    Replace,
}

pub(crate) fn upsert_session_rebuilt(conn: &Connection, row: &SessionCatalogRow<'_>) -> Result<()> {
    upsert_session_inner(conn, row, ActivityWindow::Replace, None)
}

pub(super) fn upsert_session_inner(
    conn: &Connection,
    row: &SessionCatalogRow<'_>,
    window: ActivityWindow,
    recorded_repo_url: Option<&str>,
) -> Result<()> {
    let SessionCatalogRow {
        session_id,
        source,
        cwd,
        git_branch,
        first_ts,
        last_ts,
        last_assistant_text,
        raw_path,
    } = *row;
    // `last_assistant_text` follows the same rule as the window. A pass that
    // saw only part of the session must not erase prose it did not read, but a
    // rebuild has just re-read the whole source: if there is no assistant
    // prose in it any more, keeping the old value would leave the catalog
    // quoting a reply the transcript no longer contains.
    let (first_activity, last_activity, assistant_text) = match window {
        // A time that is not positive is no time: a reader takes 0 for
        // 1970-01-01, so it is stored as NULL, and a 0 an earlier rule stored
        // gives way to the first real time instead of pinning MIN() forever.
        ActivityWindow::Expand => (
            "first_activity_ms = MIN(COALESCE(NULLIF(sessions.first_activity_ms, 0), excluded.first_activity_ms), COALESCE(excluded.first_activity_ms, NULLIF(sessions.first_activity_ms, 0)))",
            "last_activity_ms = MAX(COALESCE(NULLIF(sessions.last_activity_ms, 0), excluded.last_activity_ms), COALESCE(excluded.last_activity_ms, NULLIF(sessions.last_activity_ms, 0)))",
            "last_assistant_text = COALESCE(excluded.last_assistant_text, sessions.last_assistant_text)",
        ),
        ActivityWindow::Replace => (
            "first_activity_ms = excluded.first_activity_ms",
            "last_activity_ms = excluded.last_activity_ms",
            "last_assistant_text = excluded.last_assistant_text",
        ),
    };
    // A different transcript does not replace the recorded file, or the
    // excerpt taken from it, while that file exists. Activity still moves:
    // events from both transcripts belong to the session. The same path is a
    // normal update, including a directory a provider rewrites in place.
    let keep_recorded =
        crate::catalog_owner::incoming_must_not_replace(conn, source, session_id, raw_path)?;
    let (assistant_text, raw_path_sql, presence_path) =
        crate::catalog_owner::session_assigns(keep_recorded, assistant_text, raw_path);
    // Canonical project identity, derived once per session from the cwd the
    // provider recorded. Resolution is cached per directory, so the repeated
    // upserts a growing transcript produces cost one filesystem walk in total.
    let (project_key, project_key_method, repo_url) =
        match crate::project_identity::catalog_identity(cwd, recorded_repo_url, source == "claude")
        {
            Some((key, method, url)) => (Some(key), Some(method.as_str()), url),
            None => (None, None, None),
        };
    let project_key_merge = crate::store::project_key_merge_sql();
    conn.prepare_cached(
        &format!("INSERT INTO sessions \
         (session_id, source, cwd, git_branch, first_activity_ms, last_activity_ms, last_assistant_text, raw_path, parser_version, project_key, project_key_method, repo_url, discovery_state) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, 'full') \
         ON CONFLICT(session_id, source) DO UPDATE SET \
         {project_key_merge}, \
         cwd = COALESCE(excluded.cwd, sessions.cwd), \
         repo_url = COALESCE(excluded.repo_url, sessions.repo_url), \
         git_branch = COALESCE(excluded.git_branch, sessions.git_branch), \
         {first_activity}, \
         {last_activity}, \
         {assistant_text}, \
         {raw_path_sql}, \
         parser_version = excluded.parser_version, \
         discovery_state = 'full'"
        ),
    )?
    .execute(
        params![
            session_id,
            source,
            cwd,
            git_branch,
            (first_ts > 0).then_some(first_ts),
            (last_ts > 0).then_some(last_ts),
            last_assistant_text,
            raw_path,
            project_key,
            project_key_method,
            repo_url,
        ],
    )?;
    // Events earlier in this unit may have cached a miss or a weaker key.
    sweep_session::invalidate(conn, source, session_id);
    crate::upsert_session_presence(
        conn,
        source,
        session_id,
        SessionLocation::Local,
        presence_path,
        None,
        Some("full"),
    )?;
    Ok(())
}
