//! Conversion from the cached catalog row to the typed store facade.

use super::{delegated_child_session, path_names_one_session, SessionRef};
use super::{CatalogSession, DiscoveryState, Error, ShallowSession, Source};
use crate::discover;
use rusqlite::Connection;
use std::path::PathBuf;

impl CatalogSession {
    /// A delegated child's catalog-shaped description from one row of
    /// `DELEGATED_CHILD_SQL` (see `delegated_child_session`). `models` is left
    /// empty for the caller to decode.
    pub(super) fn delegated(
        source: Source,
        session_id: &str,
        row: &rusqlite::Row<'_>,
    ) -> rusqlite::Result<Self> {
        Ok(Self {
            source,
            session_id: session_id.to_string(),
            cwd: row.get(2)?,
            git_branch: row.get(3)?,
            first_activity_ms: row.get(0)?,
            last_activity_ms: row.get(1)?,
            first_prompt: None,
            last_assistant_text: None,
            title: None,
            session_kind: None,
            agent_name: None,
            models: Vec::new(),
            originator: None,
            agent_version: row.get(4)?,
            repo_url: None,
            initial_commit: None,
            workspace_roots: Vec::new(),
            project_key: row.get(5)?,
            project_key_method: row.get(6)?,
            raw_path: row.get::<_, Option<String>>(8)?.map(PathBuf::from),
            source_stamp: None,
            discovery_state: DiscoveryState::Delegated,
            locations: Vec::new(),
        })
    }

    pub(super) fn from_row(row: ShallowSession) -> Result<Self, Error> {
        let source = Source::parse(&row.source).ok_or_else(|| {
            Error::Query(format!(
                "catalog row {}/{} names a source this build does not know",
                row.source, row.session_id
            ))
        })?;
        Ok(Self {
            source,
            session_id: row.session_id,
            cwd: row.cwd,
            git_branch: row.git_branch,
            first_activity_ms: row.first_activity_ms,
            last_activity_ms: row.last_activity_ms,
            first_prompt: row.first_prompt,
            last_assistant_text: row.last_assistant_text,
            title: row.title,
            session_kind: row.session_kind,
            agent_name: row.agent_name,
            models: row.models,
            originator: row.originator,
            agent_version: row.agent_version,
            repo_url: row.repo_url,
            initial_commit: row.initial_commit,
            workspace_roots: row.workspace_roots,
            project_key: row.project_key,
            project_key_method: row.project_key_method,
            raw_path: row.raw_path.map(PathBuf::from),
            source_stamp: row.source_stamp,
            discovery_state: if row.discovery_state == "shallow" {
                DiscoveryState::Shallow
            } else {
                DiscoveryState::Full
            },
            locations: row.locations,
        })
    }
}

/// The catalog row `r` names, or the delegated child its id names, or
/// `None` when the store has neither.
pub(super) fn catalog_session(
    conn: &Connection,
    r: &SessionRef,
    include_text: bool,
) -> Result<Option<CatalogSession>, Error> {
    let source = r.source();
    let name = source.as_str();
    let row = match r {
        SessionRef::Id { session_id, .. } => {
            discover::catalog_row(conn, name, session_id, include_text)
        }
        SessionRef::Path { path, .. } => {
            // A path names one session only where the provider keeps one
            // session per file. OpenCode's rows all carry the provider
            // database as their locator, so a lookup by it would answer
            // with whichever session sorts first — well-formed and wrong.
            path_names_one_session(source)?;
            discover::catalog_row_by_path(conn, name, &path.to_string_lossy(), include_text)
        }
    }
    .map_err(Error::query)?;
    match (row, r) {
        (Some(row), _) => CatalogSession::from_row(row).map(Some),
        // A delegated child — a Claude subagent sidecar, a Codex child
        // thread — is kept out of the catalog because it is part of its
        // parent's work, but its evidence is stored under its own id.
        (None, SessionRef::Id { session_id, .. }) => {
            delegated_child_session(conn, source, session_id)
        }
        (None, SessionRef::Path { .. }) => Ok(None),
    }
}
