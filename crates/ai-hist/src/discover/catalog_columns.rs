//! Shared scalar catalog columns, the public locations projection, and the
//! shallow catalog upsert.

use std::sync::LazyLock;

macro_rules! session_scalar_columns {
    () => {
        "source, session_id, cwd, git_branch, first_activity_ms, \
     last_activity_ms, first_prompt, last_assistant_text, models_json, originator, \
     agent_version, repo_url, initial_commit, workspace_roots_json, raw_path, source_stamp, \
     discovery_state, project_key, project_key_method, title, session_kind, agent_name"
    };
}

pub(super) const SESSION_SCALAR_COLUMNS: &str = session_scalar_columns!();

pub(crate) const SESSION_COLUMNS: &str = concat!(session_scalar_columns!(), ", \
     CASE \
       WHEN EXISTS (SELECT 1 FROM session_presences p WHERE p.source = sessions.source AND p.session_id = sessions.session_id AND p.location = 'local') \
        AND EXISTS (SELECT 1 FROM session_presences p WHERE p.source = sessions.source AND p.session_id = sessions.session_id AND p.location = 'remote') \
       THEN '[\"local\",\"remote\"]' \
       WHEN EXISTS (SELECT 1 FROM session_presences p WHERE p.source = sessions.source AND p.session_id = sessions.session_id AND p.location = 'remote') \
       THEN '[\"remote\"]' \
       WHEN EXISTS (SELECT 1 FROM session_presences p WHERE p.source = sessions.source AND p.session_id = sessions.session_id AND p.location = 'local') \
       THEN '[\"local\"]' \
       ELSE '[]' \
     END");

/// The shallow catalog upsert; see `upsert_shallow_session_in_transaction`.
pub(super) static UPSERT_SESSION_SQL: LazyLock<String> = LazyLock::new(|| {
    let project_key_merge = crate::store::project_key_merge_sql();
    format!(
        "INSERT INTO sessions \
         (session_id, source, cwd, git_branch, first_activity_ms, last_activity_ms, \
          last_assistant_text, raw_path, parser_version, first_prompt, models_json, originator, \
          agent_version, repo_url, initial_commit, workspace_roots_json, source_stamp, \
          project_key, project_key_method, title, session_kind, agent_name, \
          discovery_state) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?18, ?19, CASE WHEN ?2 = 'grok-bot' THEN NULLIF(?20, '') ELSE ?20 END, ?21, ?22, 'shallow') \
         ON CONFLICT(session_id, source) DO UPDATE SET \
         {project_key_merge}, \
         cwd = COALESCE(excluded.cwd, sessions.cwd), \
         git_branch = COALESCE(excluded.git_branch, sessions.git_branch), \
         first_activity_ms = CASE \
             WHEN excluded.source = 'grok' THEN COALESCE(excluded.first_activity_ms, sessions.first_activity_ms) \
             WHEN excluded.first_activity_ms IS NULL THEN sessions.first_activity_ms \
             WHEN sessions.first_activity_ms IS NULL THEN excluded.first_activity_ms \
             ELSE MIN(sessions.first_activity_ms, excluded.first_activity_ms) END, \
         last_activity_ms = CASE \
             WHEN excluded.source = 'grok' THEN COALESCE(excluded.last_activity_ms, sessions.last_activity_ms) \
             WHEN excluded.last_activity_ms IS NULL THEN sessions.last_activity_ms \
             WHEN sessions.last_activity_ms IS NULL THEN excluded.last_activity_ms \
             ELSE MAX(sessions.last_activity_ms, excluded.last_activity_ms) END, \
         last_assistant_text = COALESCE(excluded.last_assistant_text, sessions.last_assistant_text), \
         raw_path = CASE WHEN ?17 = 'remote' AND EXISTS ( \
             SELECT 1 FROM session_presences p WHERE p.source = sessions.source \
             AND p.session_id = sessions.session_id AND p.location = 'local') \
             THEN COALESCE(sessions.raw_path, excluded.raw_path) \
             ELSE COALESCE(excluded.raw_path, sessions.raw_path) END, \
         first_prompt = COALESCE(excluded.first_prompt, sessions.first_prompt), \
         title = CASE \
             WHEN excluded.source = 'grok-bot' AND ?17 = 'local' AND ?20 IS NOT NULL THEN NULLIF(?20, '') \
             WHEN sessions.discovery_state IS NULL OR sessions.discovery_state = 'full' \
             THEN COALESCE(sessions.title, excluded.title) \
             ELSE COALESCE(excluded.title, sessions.title) END, \
         session_kind = CASE \
             WHEN excluded.source = 'grok' AND ?17 = 'local' THEN excluded.session_kind \
             WHEN excluded.source = 'grok' AND EXISTS ( \
                 SELECT 1 FROM session_presences p WHERE p.source = sessions.source \
                 AND p.session_id = sessions.session_id AND p.location = 'local') THEN sessions.session_kind \
             WHEN excluded.source = 'grok' THEN excluded.session_kind \
             ELSE COALESCE(excluded.session_kind, sessions.session_kind) END, \
         agent_name = CASE WHEN COALESCE(sessions.discovery_state, 'full') = 'full' THEN COALESCE(sessions.agent_name, excluded.agent_name) ELSE COALESCE(excluded.agent_name, sessions.agent_name) END, \
         models_json = COALESCE(excluded.models_json, sessions.models_json), \
         originator = COALESCE(excluded.originator, sessions.originator), \
         agent_version = COALESCE(excluded.agent_version, sessions.agent_version), \
         repo_url = COALESCE(excluded.repo_url, sessions.repo_url), \
         initial_commit = COALESCE(excluded.initial_commit, sessions.initial_commit), \
         workspace_roots_json = COALESCE(excluded.workspace_roots_json, sessions.workspace_roots_json), \
         source_stamp = COALESCE(excluded.source_stamp, sessions.source_stamp), \
         discovery_state = CASE \
             WHEN sessions.discovery_state IS NULL OR sessions.discovery_state = 'full' \
             THEN 'full' ELSE 'shallow' END \
         RETURNING {SESSION_COLUMNS}"
    )
});
