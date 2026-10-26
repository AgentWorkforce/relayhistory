//! Shared scalar catalog columns and the public locations projection.

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
