//! Conversion from the cached catalog row to the typed store facade.

use super::{CatalogSession, DiscoveryState, Error, ShallowSession, Source};
use std::path::PathBuf;

impl CatalogSession {
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
