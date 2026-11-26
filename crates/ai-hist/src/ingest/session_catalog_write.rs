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
    write_session(conn, row, window, recorded_repo_url, false)
}

/// History backfill may create a catalog row or add activity and metadata.
/// Its checkout is weaker evidence than a remote already settled by a rollout.
pub(super) fn upsert_codex_history_backfill(
    conn: &Connection,
    row: &SessionCatalogRow<'_>,
) -> Result<()> {
    write_session(conn, row, ActivityWindow::Expand, None, true)
}

fn write_session(
    conn: &Connection,
    row: &SessionCatalogRow<'_>,
    window: ActivityWindow,
    recorded_repo_url: Option<&str>,
    preserve_remote: bool,
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
    let (identity, recorded) = crate::project_identity::catalog_identity_observation(
        cwd,
        recorded_repo_url,
        source == "claude",
    );
    let (project_key, project_key_method, repo_url) = match identity {
        Some((key, method, url)) => (Some(key), Some(method.as_str()), url),
        None => (None, None, None),
    };
    let project_key_merge = crate::store::project_key_merge_sql("?12", "?13");
    conn.prepare_cached(
        &format!("INSERT INTO sessions \
         (session_id, source, cwd, git_branch, first_activity_ms, last_activity_ms, last_assistant_text, raw_path, parser_version, project_key, project_key_method, repo_url, discovery_state) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, 'full') \
         ON CONFLICT(session_id, source) DO UPDATE SET \
         {project_key_merge}, \
         cwd = COALESCE(excluded.cwd, sessions.cwd), \
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
            recorded,
            preserve_remote,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codex_history_backfill_preserves_recorded_remote_keys_with_no_url() {
        let conn = Connection::open_in_memory().unwrap();
        crate::init_db(&conn).unwrap();
        let checkout = tempfile::tempdir().unwrap();
        let git = checkout.path().join(".git");
        fs::create_dir(&git).unwrap();
        fs::write(
            git.join("config"),
            "[remote \"origin\"]\nurl = https://checkout.test/Org/Other.git\n",
        )
        .unwrap();
        let cwd = checkout.path().to_str().unwrap();
        assert_eq!(
            crate::project_identity::identity_for(Some(cwd), None)
                .unwrap()
                .0,
            "checkout.test/Org/Other"
        );

        let row = SessionCatalogRow {
            session_id: "recorded",
            source: "codex",
            cwd: Some(cwd),
            git_branch: None,
            first_ts: 1,
            last_ts: 1,
            last_assistant_text: None,
            raw_path: None,
        };
        let observe =
            |url| upsert_session_inner(&conn, &row, ActivityWindow::Expand, Some(url)).unwrap();
        let stored = || {
            conn.query_row(
                "SELECT project_key, project_key_method, repo_url, last_activity_ms
             FROM sessions WHERE source = 'codex' AND session_id = 'recorded'",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .unwrap()
        };
        observe("ssh://git@recorded.test:/Org/First.git");
        assert_eq!(
            stored(),
            ("recorded.test/Org/First".into(), "remote".into(), None, 1)
        );
        conn.execute(
            "INSERT INTO history (source, session_id, prompt, timestamp_ms)
             VALUES ('codex', 'recorded', 'first prompt', 2)",
            [],
        )
        .unwrap();
        let backfill = || {
            backfill_codex_metadata(
                &conn,
                &HashMap::from([("recorded".into(), cwd.into())]),
                &HashMap::new(),
            )
            .unwrap()
        };
        backfill();
        assert_eq!(
            stored(),
            ("recorded.test/Org/First".into(), "remote".into(), None, 2)
        );

        observe("https://recorded.test/Org/Second.git");
        assert_eq!(
            stored(),
            (
                "recorded.test/Org/Second".into(),
                "remote".into(),
                Some("https://recorded.test/Org/Second.git".into()),
                2,
            )
        );
        crate::discover::upsert_shallow_session(
            &conn,
            &crate::discover::ShallowSession {
                session_id: "recorded".into(),
                source: "codex".into(),
                cwd: Some(cwd.into()),
                repo_url: Some("ssh://git@recorded.test:/Org/Third.git".into()),
                ..Default::default()
            },
        )
        .unwrap();
        backfill();
        assert_eq!(
            stored(),
            ("recorded.test/Org/Third".into(), "remote".into(), None, 2)
        );
    }
}
