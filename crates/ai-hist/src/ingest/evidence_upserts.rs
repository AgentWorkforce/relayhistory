//! Upserts that skip a rewrite when every assigned value is already stored.
//!
//! The `SET` expression and the `WHERE` comparison are the same text, so an
//! unchanged conflict does not rewrite the row. A Grok event whose incoming
//! `token_json` is null keeps the turn fact a later update stored.

use std::sync::LazyLock;

/// `token_json` for a Grok row is filled after the insert, by the turn tail.
/// A conflict that wrote the incoming null would wipe that fact and stamp a
/// revision, then the tail update would stamp another. A null incoming value
/// keeps the stored Grok fact; every other source still clears it.
const TOKEN_JSON_ASSIGNED: &str = "CASE \
     WHEN excluded.token_json IS NOT NULL THEN excluded.token_json \
     WHEN excluded.source = 'grok' THEN session_events.token_json \
     ELSE excluded.token_json END";

const PROJECT_KEY_ASSIGNED: &str = "COALESCE((SELECT s.project_key FROM sessions s \
     WHERE s.source = ?1 AND s.session_id = ?2), session_events.project_key, ?16)";

const PROJECT_KEY_METHOD_ASSIGNED: &str = "CASE \
     WHEN (SELECT s.project_key FROM sessions s WHERE s.source = ?1 AND s.session_id = ?2) \
          IS NOT NULL \
       THEN (SELECT s.project_key_method FROM sessions s \
             WHERE s.source = ?1 AND s.session_id = ?2) \
     WHEN session_events.project_key IS NOT NULL THEN session_events.project_key_method \
     ELSE ?17 END";

const LOCATION_ASSIGNED: &str = "CASE WHEN session_events.location = excluded.location \
     THEN session_events.location ELSE 'both' END";

pub(crate) static SESSION_EVENT_UPSERT: LazyLock<String> = LazyLock::new(|| {
    format!(
        "{} WHERE {}",
        *SESSION_EVENT_REWRITE, *SESSION_EVENT_UNCHANGED
    )
});

pub(crate) static SESSION_EVENT_REWRITE: LazyLock<String> = LazyLock::new(|| {
    format!(
        "INSERT INTO session_events \
         (source, session_id, project, project_key, project_key_method, cwd, git_branch, \
          message_id, parent_id, ts_ms, role, kind, text, model, token_json, provider, event_uid, \
          tool_use_id, payload_bytes, payload_truncated, payload_hash, call_index, event_index, \
          result_status, event_source, error_signal, subagent_session_id, agent_id, \
          request_id, provider_message_id, stop_reason, agent_version, is_sidechain, is_meta, \
          turn_id, request_span, raw_facts_version, raw_kind, control_kind, record_token_json) \
         VALUES (?1, ?2, ?3, \
           COALESCE((SELECT s.project_key FROM sessions s WHERE s.source = ?1 AND s.session_id = ?2), ?16), \
           CASE WHEN (SELECT s.project_key FROM sessions s WHERE s.source = ?1 AND s.session_id = ?2) IS NOT NULL \
                THEN (SELECT s.project_key_method FROM sessions s WHERE s.source = ?1 AND s.session_id = ?2) \
                ELSE ?17 END, \
           ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, \
           ?14, ?15, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, \
           ?29, ?30, ?31, ?32, ?33, ?34, ?35, ?36, ?37, ?38, ?39, ?40) \
         ON CONFLICT(source, session_id, event_uid) DO UPDATE SET \
         project=excluded.project, \
         project_key={PROJECT_KEY_ASSIGNED}, \
         project_key_method={PROJECT_KEY_METHOD_ASSIGNED}, \
         cwd=excluded.cwd, git_branch=excluded.git_branch, message_id=excluded.message_id, \
         parent_id=excluded.parent_id, ts_ms=excluded.ts_ms, role=excluded.role, kind=excluded.kind, \
         text=excluded.text, model=excluded.model, token_json={TOKEN_JSON_ASSIGNED}, \
         provider=excluded.provider, \
         tool_use_id=excluded.tool_use_id, payload_bytes=excluded.payload_bytes, \
         payload_truncated=excluded.payload_truncated, payload_hash=excluded.payload_hash, \
         call_index=excluded.call_index, event_index=excluded.event_index, \
         result_status=excluded.result_status, event_source=excluded.event_source, \
         error_signal=excluded.error_signal, subagent_session_id=excluded.subagent_session_id, \
         agent_id=excluded.agent_id, request_id=excluded.request_id, \
         provider_message_id=excluded.provider_message_id, \
         stop_reason=excluded.stop_reason, agent_version=excluded.agent_version, \
         is_sidechain=excluded.is_sidechain, is_meta=excluded.is_meta, turn_id=excluded.turn_id, \
         request_span=excluded.request_span, \
         raw_facts_version=excluded.raw_facts_version, raw_kind=excluded.raw_kind, \
         control_kind=excluded.control_kind, record_token_json=excluded.record_token_json, \
         location={LOCATION_ASSIGNED}"
    )
});

static SESSION_EVENT_UNCHANGED: LazyLock<String> = LazyLock::new(|| {
    format!(
        "excluded.project IS NOT session_events.project \
         OR ({PROJECT_KEY_ASSIGNED}) IS NOT session_events.project_key \
         OR ({PROJECT_KEY_METHOD_ASSIGNED}) IS NOT session_events.project_key_method \
         OR excluded.cwd IS NOT session_events.cwd \
         OR excluded.git_branch IS NOT session_events.git_branch \
         OR excluded.message_id IS NOT session_events.message_id \
         OR excluded.parent_id IS NOT session_events.parent_id \
         OR excluded.ts_ms IS NOT session_events.ts_ms \
         OR excluded.role IS NOT session_events.role \
         OR excluded.kind IS NOT session_events.kind \
         OR excluded.text IS NOT session_events.text \
         OR excluded.model IS NOT session_events.model \
         OR ({TOKEN_JSON_ASSIGNED}) IS NOT session_events.token_json \
         OR excluded.provider IS NOT session_events.provider \
         OR excluded.tool_use_id IS NOT session_events.tool_use_id \
         OR excluded.payload_bytes IS NOT session_events.payload_bytes \
         OR excluded.payload_truncated IS NOT session_events.payload_truncated \
         OR excluded.payload_hash IS NOT session_events.payload_hash \
         OR excluded.call_index IS NOT session_events.call_index \
         OR excluded.event_index IS NOT session_events.event_index \
         OR excluded.result_status IS NOT session_events.result_status \
         OR excluded.event_source IS NOT session_events.event_source \
         OR excluded.error_signal IS NOT session_events.error_signal \
         OR excluded.subagent_session_id IS NOT session_events.subagent_session_id \
         OR excluded.agent_id IS NOT session_events.agent_id \
         OR excluded.request_id IS NOT session_events.request_id \
         OR excluded.provider_message_id IS NOT session_events.provider_message_id \
         OR excluded.stop_reason IS NOT session_events.stop_reason \
         OR excluded.agent_version IS NOT session_events.agent_version \
         OR excluded.is_sidechain IS NOT session_events.is_sidechain \
         OR excluded.is_meta IS NOT session_events.is_meta \
         OR excluded.turn_id IS NOT session_events.turn_id \
         OR excluded.request_span IS NOT session_events.request_span \
         OR excluded.raw_facts_version IS NOT session_events.raw_facts_version \
         OR excluded.raw_kind IS NOT session_events.raw_kind \
         OR excluded.control_kind IS NOT session_events.control_kind \
         OR excluded.record_token_json IS NOT session_events.record_token_json \
         OR ({LOCATION_ASSIGNED}) IS NOT session_events.location"
    )
});

const TOOL_LOCATION_ASSIGNED: &str = "CASE WHEN tool_calls.location = excluded.location \
     THEN tool_calls.location ELSE 'both' END";

pub(crate) static TOOL_CALL_UPSERT: LazyLock<String> =
    LazyLock::new(|| format!("{} WHERE {}", *TOOL_CALL_REWRITE, *TOOL_CALL_UNCHANGED));

pub(crate) static TOOL_CALL_REWRITE: LazyLock<String> = LazyLock::new(|| {
    format!(
        "INSERT INTO tool_calls \
         (source, session_id, message_id, tool_use_id, name, target, args_json, is_error, ts_ms) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(source, session_id, tool_use_id) DO UPDATE SET \
         message_id=excluded.message_id, name=excluded.name, target=excluded.target, \
         args_json=excluded.args_json, \
         is_error=COALESCE(excluded.is_error, tool_calls.is_error), ts_ms=excluded.ts_ms, \
         location={TOOL_LOCATION_ASSIGNED}"
    )
});

static TOOL_CALL_UNCHANGED: LazyLock<String> = LazyLock::new(|| {
    format!(
        "excluded.message_id IS NOT tool_calls.message_id \
         OR excluded.name IS NOT tool_calls.name \
         OR excluded.target IS NOT tool_calls.target \
         OR excluded.args_json IS NOT tool_calls.args_json \
         OR COALESCE(excluded.is_error, tool_calls.is_error) IS NOT tool_calls.is_error \
         OR excluded.ts_ms IS NOT tool_calls.ts_ms \
         OR ({TOOL_LOCATION_ASSIGNED}) IS NOT tool_calls.location"
    )
});

const EDIT_LOCATION_ASSIGNED: &str =
    "CASE WHEN file_edits.location = excluded.location THEN file_edits.location ELSE 'both' END";

const FILE_EDIT_INSERT: &str = "INSERT INTO file_edits \
     (source, session_id, message_id, tool_use_id, file_path, tool_name, ts_ms, git_branch, cwd) \
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) \
     ON CONFLICT(source, session_id, tool_use_id) DO UPDATE SET \
     message_id=excluded.message_id, file_path=excluded.file_path, tool_name=excluded.tool_name, \
     ts_ms=excluded.ts_ms, ";

/// A local row takes the snapshot's branch and cwd, including NULL. A row
/// both sides back keeps the other side's value when this snapshot has none,
/// which is what demoting it and inserting again used to do.
const FILE_EDIT_BRANCH_SNAPSHOT: &str = "git_branch=CASE WHEN file_edits.location = 'local' \
     THEN excluded.git_branch ELSE COALESCE(excluded.git_branch, file_edits.git_branch) END, \
     cwd=CASE WHEN file_edits.location = 'local' THEN excluded.cwd \
     ELSE COALESCE(excluded.cwd, file_edits.cwd) END";

const FILE_EDIT_BRANCH_KEPT: &str =
    "git_branch=COALESCE(excluded.git_branch, file_edits.git_branch), \
     cwd=COALESCE(excluded.cwd, file_edits.cwd)";

static FILE_EDIT_UNCHANGED: LazyLock<String> = LazyLock::new(|| {
    format!(
        "excluded.message_id IS NOT file_edits.message_id \
         OR excluded.file_path IS NOT file_edits.file_path \
         OR excluded.tool_name IS NOT file_edits.tool_name \
         OR excluded.ts_ms IS NOT file_edits.ts_ms \
         OR (CASE WHEN file_edits.location = 'local' THEN excluded.git_branch \
             ELSE COALESCE(excluded.git_branch, file_edits.git_branch) END) \
            IS NOT file_edits.git_branch \
         OR (CASE WHEN file_edits.location = 'local' THEN excluded.cwd \
             ELSE COALESCE(excluded.cwd, file_edits.cwd) END) \
            IS NOT file_edits.cwd \
         OR ({EDIT_LOCATION_ASSIGNED}) IS NOT file_edits.location"
    )
});

pub(crate) static FILE_EDIT_UPSERT: LazyLock<String> = LazyLock::new(|| {
    format!(
        "{FILE_EDIT_INSERT}{FILE_EDIT_BRANCH_SNAPSHOT}, location={EDIT_LOCATION_ASSIGNED} WHERE {}",
        *FILE_EDIT_UNCHANGED
    )
});

pub(crate) static FILE_EDIT_REWRITE: LazyLock<String> = LazyLock::new(|| {
    format!("{FILE_EDIT_INSERT}{FILE_EDIT_BRANCH_KEPT}, location={EDIT_LOCATION_ASSIGNED}")
});
