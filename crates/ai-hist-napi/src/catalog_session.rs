//! Native catalog row exposed to JavaScript.

use napi_derive::napi;

#[napi(object)]
pub struct CatalogSession {
    pub source: String,
    pub session_id: String,
    pub cwd: Option<String>,
    pub git_branch: Option<String>,
    pub first_activity_ms: Option<i64>,
    pub last_activity_ms: Option<i64>,
    pub first_prompt: Option<String>,
    pub last_assistant_text: Option<String>,
    /// The name the harness gave the session (Claude's title, Codex's thread
    /// name).
    pub title: Option<String>,
    /// Provider-native kind, such as Grok's `subagent`.
    pub session_kind: Option<String>,
    /// The name other sessions address this one by (Claude's `agent-name`,
    /// e.g. an Agent Relay worker's name).
    pub agent_name: Option<String>,
    pub models: Vec<String>,
    pub originator: Option<String>,
    pub agent_version: Option<String>,
    pub repo_url: Option<String>,
    pub initial_commit: Option<String>,
    pub workspace_roots: Vec<String>,
    pub raw_path: Option<String>,
    pub source_stamp: Option<String>,
    pub discovery_state: String,
    /// Canonical project identity: `host/owner/repo`, or the working
    /// directory when no git remote resolves.
    pub project_key: Option<String>,
    /// `remote`, `path`, or `inherited`.
    pub project_key_method: Option<String>,
    pub locations: Vec<String>,
    pub from_cache: bool,
}

impl From<ai_hist::ShallowSession> for CatalogSession {
    fn from(session: ai_hist::ShallowSession) -> Self {
        Self {
            source: session.source,
            session_id: session.session_id,
            cwd: session.cwd,
            git_branch: session.git_branch,
            first_activity_ms: session.first_activity_ms,
            last_activity_ms: session.last_activity_ms,
            first_prompt: session.first_prompt,
            last_assistant_text: session.last_assistant_text,
            title: session.title,
            session_kind: session.session_kind,
            agent_name: session.agent_name,
            models: session.models,
            originator: session.originator,
            agent_version: session.agent_version,
            repo_url: session.repo_url,
            initial_commit: session.initial_commit,
            workspace_roots: session.workspace_roots,
            raw_path: session.raw_path,
            source_stamp: session.source_stamp,
            discovery_state: session.discovery_state,
            project_key: session.project_key,
            project_key_method: session.project_key_method,
            locations: session.locations,
            from_cache: session.from_cache,
        }
    }
}
