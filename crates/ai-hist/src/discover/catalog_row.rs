//! Conversion between cached catalog rows and the public shallow session.

use super::ShallowSession;

fn json_string_list(raw: Option<String>) -> Vec<String> {
    raw.and_then(|raw| serde_json::from_str::<Vec<String>>(&raw).ok())
        .unwrap_or_default()
}

pub(crate) fn row_to_session(row: &rusqlite::Row<'_>) -> rusqlite::Result<ShallowSession> {
    let (session_kind, agent_name, locations) = kind_agent_and_locations(row)?;
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
        agent_name,
        locations,
        from_cache: true,
    })
}

/// Exact capture consumes identity and the hydration locator/state, never
/// previews, model/root lists or presence. Borrow-check the remaining scalar
/// values so malformed catalog data still takes the original cache-miss path,
/// including invalid UTF-8; checking SQL `typeof` alone would miss that case.
pub(super) fn row_to_capture_session(row: &rusqlite::Row<'_>) -> rusqlite::Result<ShallowSession> {
    for index in [3, 6, 7, 8, 9, 10, 12, 13, 15, 19, 20, 21] {
        let value = row.get_ref(index)?;
        value.as_str_or_null().map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(index, value.data_type(), Box::new(error))
        })?;
    }
    // The general decoder rejects non-integral timestamps, even though
    // capture does not consume them. These conversions do not allocate.
    row.get::<_, Option<i64>>(4)?;
    row.get::<_, Option<i64>>(5)?;
    Ok(ShallowSession {
        source: row.get(0)?,
        session_id: row.get(1)?,
        cwd: row.get(2)?,
        repo_url: row.get(11)?,
        raw_path: row.get(14)?,
        discovery_state: row
            .get::<_, Option<String>>(16)?
            .unwrap_or_else(|| "full".to_string()),
        project_key: row.get(17)?,
        project_key_method: row.get(18)?,
        from_cache: true,
        ..ShallowSession::default()
    })
}

type KindAgentLocations = (Option<String>, Option<String>, Vec<String>);

fn kind_agent_and_locations(row: &rusqlite::Row<'_>) -> rusqlite::Result<KindAgentLocations> {
    Ok((row.get(20)?, row.get(21)?, json_string_list(row.get(22)?)))
}
