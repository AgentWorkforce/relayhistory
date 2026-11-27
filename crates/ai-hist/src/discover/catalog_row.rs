//! Conversion between cached catalog rows and the public shallow session.

use super::ShallowSession;

fn json_string_list(raw: Option<String>) -> Vec<String> {
    raw.and_then(|raw| serde_json::from_str::<Vec<String>>(&raw).ok())
        .unwrap_or_default()
}

pub(crate) fn row_to_session(row: &rusqlite::Row<'_>) -> rusqlite::Result<ShallowSession> {
    let mut session = row_to_unsanitized_session(row)?;
    (session.project_key, session.repo_url) = catalog_identity_projection(
        &session.source,
        session.project_key_method.as_deref(),
        session.repo_url.as_deref(),
        session.project_key.as_deref(),
    );
    Ok(session)
}

fn catalog_identity_projection(
    source: &str,
    method: Option<&str>,
    repo_url: Option<&str>,
    project_key: Option<&str>,
) -> (Option<String>, Option<String>) {
    use crate::project_identity::{
        safe_catalog_project_key, safe_catalog_repo_url, settled_catalog_identity,
    };
    if method == Some("remote") {
        return settled_catalog_identity(source, None, repo_url, project_key);
    }
    let project_key = if method == Some("inherited") {
        project_key.map(safe_catalog_project_key)
    } else {
        project_key
    };
    (project_key.map(str::to_owned), repo_url.and_then(safe_catalog_repo_url))
}

/// Evaluate the same cache-only key before SQL applies its page limit.
pub(crate) fn register_catalog_project_key(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    use rusqlite::functions::FunctionFlags;
    // A cache-only listing can share a connection with an active reader.
    // SQLite refuses replacement of an existing function in that case.
    if conn.prepare_cached("SELECT catalog_project_key(NULL, NULL, NULL, NULL)").is_ok() {
        return Ok(());
    }

    conn.create_scalar_function(
        "catalog_project_key",
        4,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            // A malformed scalar is a catalog cache miss. Keep projection
            // out of the index without rejecting the underlying SQLite write.
            let (Ok(source), Ok(method), Ok(repo_url), Ok(key)) = (
                ctx.get::<String>(0),
                ctx.get::<Option<String>>(1),
                ctx.get::<Option<String>>(2),
                ctx.get::<Option<String>>(3),
            ) else {
                return Ok(None);
            };
            Ok(catalog_identity_projection(
                &source, method.as_deref(), repo_url.as_deref(), key.as_deref(),
            ).0)
        },
    )
}

pub(super) fn row_to_unsanitized_session(row: &rusqlite::Row<'_>) -> rusqlite::Result<ShallowSession> {
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
