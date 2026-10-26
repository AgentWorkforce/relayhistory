use super::*;
use rusqlite::OptionalExtension;

pub(super) struct Summary<'a> {
    pub(super) cwd: Option<&'a str>,
    pub(super) first: i64,
    pub(super) last: i64,
    pub(super) last_assistant: Option<&'a str>,
    pub(super) prior: Option<&'a PriorActivity>,
}

pub(super) struct PriorActivity {
    catalog_first: Option<i64>,
    catalog_last: Option<i64>,
    local_first: Option<i64>,
    local_last: Option<i64>,
}

fn has_remote(conn: &Connection, sid: &str) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM session_presences WHERE source='vtcode' AND session_id=?1 AND location='remote') OR EXISTS(SELECT 1 FROM session_observations WHERE source='vtcode' AND session_id=?1 AND location='remote')",
        [sid],
        |row| row.get(0),
    )?)
}

pub(super) fn prior_activity(conn: &Connection, sid: &str) -> Result<Option<PriorActivity>> {
    if !has_remote(conn, sid)? {
        return Ok(None);
    }
    let (catalog_first, catalog_last): (Option<i64>, Option<i64>) = conn.query_row(
        "SELECT first_activity_ms,last_activity_ms FROM sessions WHERE source='vtcode' AND session_id=?1",
        [sid],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let (local_first, local_last): (Option<i64>, Option<i64>) = conn.query_row(
        "SELECT MIN(ts),MAX(ts) FROM (SELECT ts_ms AS ts FROM session_events WHERE source='vtcode' AND session_id=?1 AND location='local' UNION ALL SELECT ts_ms FROM session_markers WHERE source='vtcode' AND session_id=?1 AND location='local' UNION ALL SELECT ts_ms FROM tool_calls WHERE source='vtcode' AND session_id=?1 AND location='local')",
        [sid],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    Ok(Some(PriorActivity {
        catalog_first,
        catalog_last,
        local_first,
        local_last,
    }))
}

pub(super) fn untimed(
    conn: &Connection,
    sid: &str,
    cwd: Option<&str>,
    path: &Path,
    complete: bool,
    prior: Option<&PriorActivity>,
) -> Result<()> {
    crate::discover::upsert_shallow_session(
        conn,
        &ShallowSession {
            source: ID.into(),
            session_id: sid.into(),
            cwd: cwd.map(str::to_owned),
            raw_path: Some(path.to_string_lossy().into_owned()),
            discovery_state: if complete { "full" } else { "shallow" }.into(),
            ..Default::default()
        },
    )?;
    if complete {
        conn.execute(
            "UPDATE sessions SET discovery_state='full' WHERE source='vtcode' AND session_id=?1",
            [sid],
        )?;
        if let Some(prior) = prior {
            refresh_shared(conn, sid, prior)?;
        } else {
            conn.execute(
                "UPDATE sessions SET first_activity_ms=NULL,last_activity_ms=NULL,first_prompt=NULL,last_assistant_text=NULL WHERE source='vtcode' AND session_id=?1",
                [sid],
            )?;
        }
    }
    Ok(())
}

pub(super) fn timed(
    conn: &Connection,
    sid: &str,
    native: &str,
    path: &Path,
    rows: &[Value],
    complete: bool,
    summary: &Summary<'_>,
) -> Result<()> {
    let remote = complete && summary.prior.is_some();
    let writer = if complete {
        upsert_session_rebuilt
    } else {
        crate::ingest::upsert_session
    };
    writer(
        conn,
        &SessionCatalogRow {
            source: ID,
            session_id: sid,
            cwd: summary.cwd,
            git_branch: None,
            first_ts: summary.first,
            last_ts: summary.last,
            last_assistant_text: summary.last_assistant,
            raw_path: Some(&path.to_string_lossy()),
        },
    )?;
    if let Some(prior) = summary.prior.filter(|_| remote) {
        refresh_shared(conn, sid, prior)?;
    } else {
        let first_prompt = format::first_prompt(rows, native).map(|(text, _, _)| text);
        if complete || first_prompt.is_some() {
            conn.execute("UPDATE sessions SET first_prompt=?2 WHERE source='vtcode' AND session_id=?1 AND first_prompt IS NOT ?2",
                rusqlite::params![sid, first_prompt])?;
        }
    }
    Ok(())
}

// A local complete rewrite is authoritative only for the local share. Derive
// previews from surviving shared events, then remote observation metadata if
// that connector has not supplied full events yet. Never reuse a retired local
// preview merely because a remote presence keeps the session row alive.
fn refresh_shared(conn: &Connection, sid: &str, prior: &PriorActivity) -> Result<()> {
    let first_prompt: Option<String> = conn
        .query_row(
            "SELECT text FROM session_events WHERE source='vtcode' AND session_id=?1 AND role='user' AND kind='text' AND text IS NOT NULL ORDER BY ts_ms,id LIMIT 1",
            [sid],
            |row| row.get(0),
        )
        .optional()?
        .or(conn.query_row(
            "SELECT first_prompt FROM session_observations WHERE source='vtcode' AND session_id=?1 AND location='remote' AND first_prompt IS NOT NULL ORDER BY connector_id='legacy-unknown',access_state!='available',connector_id,connector_instance LIMIT 1",
            [sid],
            |row| row.get(0),
        ).optional()?);
    let first_prompt = first_prompt.map(|text| text.chars().take(4096).collect::<String>());
    let last_assistant: Option<String> = conn
        .query_row(
            "SELECT text FROM session_events WHERE source='vtcode' AND session_id=?1 AND role='assistant' AND kind='text' AND text IS NOT NULL ORDER BY ts_ms DESC,id DESC LIMIT 1",
            [sid],
            |row| row.get(0),
        )
        .optional()?
        .or(conn.query_row(
            "SELECT last_assistant_text FROM session_observations WHERE source='vtcode' AND session_id=?1 AND location='remote' AND last_assistant_text IS NOT NULL ORDER BY connector_id='legacy-unknown',access_state!='available',connector_id,connector_instance LIMIT 1",
            [sid],
            |row| row.get(0),
        ).optional()?);
    conn.execute(
        "UPDATE sessions SET first_prompt=?2,last_assistant_text=?3 WHERE source='vtcode' AND session_id=?1 AND (first_prompt IS NOT ?2 OR last_assistant_text IS NOT ?3)",
        rusqlite::params![sid, first_prompt, last_assistant],
    )?;
    let (observed_first, observed_last): (Option<i64>, Option<i64>) = conn.query_row(
        "SELECT MIN(ts),MAX(ts) FROM (SELECT ts_ms AS ts FROM session_events WHERE source='vtcode' AND session_id=?1 UNION ALL SELECT ts_ms FROM session_markers WHERE source='vtcode' AND session_id=?1 UNION ALL SELECT ts_ms FROM tool_calls WHERE source='vtcode' AND session_id=?1 UNION ALL SELECT timestamp_ms FROM history WHERE source='vtcode' AND session_id=?1)",
        [sid],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    // The shared catalog has no per-observation time columns. An endpoint
    // outside the old local evidence range is known to come from the remote
    // observation; an endpoint equal to retired local evidence is ambiguous
    // and must not be carried forward as if it were still observed.
    let remote_first = prior
        .catalog_first
        .filter(|first| prior.local_first.is_none_or(|local| *first < local));
    let remote_last = prior
        .catalog_last
        .filter(|last| prior.local_last.is_none_or(|local| *last > local));
    let first = observed_first.into_iter().chain(remote_first).min();
    let last = observed_last.into_iter().chain(remote_last).max();
    conn.execute(
        "UPDATE sessions SET first_activity_ms=?2,last_activity_ms=?3 WHERE source='vtcode' AND session_id=?1 AND (first_activity_ms IS NOT ?2 OR last_activity_ms IS NOT ?3)",
        rusqlite::params![sid, first, last],
    )?;
    Ok(())
}
