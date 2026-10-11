//! Conversion between cached catalog rows and the public shallow session.

use super::ShallowSession;

fn json_string_list(raw: Option<String>) -> Vec<String> {
    raw.and_then(|raw| serde_json::from_str::<Vec<String>>(&raw).ok())
        .unwrap_or_default()
}

pub(crate) fn row_to_session(row: &rusqlite::Row<'_>) -> rusqlite::Result<ShallowSession> {
    let (session_kind, locations) = kind_and_locations(row)?;
    Ok(ShallowSession {
        source: row.get(0)?,
        session_id: row.get(1)?,
        cwd: row.get(2)?,
        git_branch: row.get(3)?,
        first_activity_ms: row.get(4)?,
        last_activity_ms: row.get(5)?,
        first_prompt: row.get(6)?,
        last_assistant_text: row.get(7)?,
        models: json_string_list(row.get(8)?),
        originator: row.get(9)?,
        agent_version: row.get(10)?,
        repo_url: row.get(11)?,
        initial_commit: row.get(12)?,
        workspace_roots: json_string_list(row.get(13)?),
        raw_path: row.get(14)?,
        source_stamp: row.get(15)?,
        discovery_state: row
            .get::<_, Option<String>>(16)?
            .unwrap_or_else(|| "full".to_string()),
        project_key: row.get(17)?,
        project_key_method: row.get(18)?,
        title: row.get(19)?,
        session_kind,
        locations,
        from_cache: true,
    })
}

fn kind_and_locations(row: &rusqlite::Row<'_>) -> rusqlite::Result<(Option<String>, Vec<String>)> {
    Ok((row.get(20)?, json_string_list(row.get(21)?)))
}

