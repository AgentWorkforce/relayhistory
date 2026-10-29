//! Upserts that skip a rewrite when every assigned value is already stored.
//!
//! The `SET` expression and the `WHERE` comparison are the same text, so an
//! unchanged conflict does not rewrite the row. A Grok or Codex event whose
//! incoming `token_json` is null keeps the usage a later write in the same
//! pass stored.

use std::sync::LazyLock;

/// `token_json` for a Grok row is filled after the insert, by the turn tail.
/// A Codex row is filled by a later `token_count` in the same pass. A conflict
/// that wrote the incoming null would wipe that fact and stamp a revision,
/// then the later write would stamp another. A null incoming value keeps the
/// stored fact for those two sources; every other source still clears it.
const TOKEN_JSON_ASSIGNED: &str = "CASE \
     WHEN excluded.token_json IS NOT NULL THEN excluded.token_json \
     WHEN excluded.source = 'grok' THEN session_events.token_json \
     WHEN excluded.source = 'codex' THEN session_events.token_json \
     ELSE excluded.token_json END";

/// The catalog lookup a one-off insert still runs. A sweep unit binds the same
/// value as `?41` / `?42` after reading it once.
const SESSION_KEY_LOOKUP: &str = "(SELECT s.project_key FROM sessions s \
     WHERE s.source = ?1 AND s.session_id = ?2)";

const SESSION_METHOD_LOOKUP: &str = "(SELECT s.project_key_method FROM sessions s \
     WHERE s.source = ?1 AND s.session_id = ?2)";

const LOCATION_ASSIGNED: &str = "CASE WHEN session_events.location = excluded.location \
     THEN session_events.location ELSE 'both' END";

pub(crate) static SESSION_EVENT_UPSERT: LazyLock<String> =
    LazyLock::new(|| session_event_sql(SESSION_KEY_LOOKUP, SESSION_METHOD_LOOKUP).1);

pub(crate) static SESSION_EVENT_REWRITE: LazyLock<String> =
    LazyLock::new(|| session_event_sql(SESSION_KEY_LOOKUP, SESSION_METHOD_LOOKUP).0);

/// Same assignment as [`SESSION_EVENT_UPSERT`], with the catalog key already
/// bound. `?41` is null when the session row has no key.
pub(crate) static SESSION_EVENT_UPSERT_CACHED: LazyLock<String> =
    LazyLock::new(|| session_event_sql("?41", "?42").1);

pub(crate) static SESSION_EVENT_REWRITE_CACHED: LazyLock<String> =
    LazyLock::new(|| session_event_sql("?41", "?42").0);

/// `(rewrite, upsert)`. The upsert adds the unchanged-row guard. The key
/// expression prefers the catalog row, then the stored event key, then the
/// cwd fallback (`?16`), and the method comes from the same arm.
fn session_event_sql(session_key: &str, session_method: &str) -> (String, String) {
    let insert_key = format!("COALESCE({session_key}, ?16)");
    let insert_method =
        format!("CASE WHEN {session_key} IS NOT NULL THEN {session_method} ELSE ?17 END");
    let assigned_key = format!("COALESCE({session_key}, session_events.project_key, ?16)");
    let assigned_method = format!(
        "CASE WHEN {session_key} IS NOT NULL THEN {session_method} \
         WHEN session_events.project_key IS NOT NULL THEN session_events.project_key_method \
         ELSE ?17 END"
    );
    // An incremental cursor does not re-observe a legacy bucket's prefix.
    // Allocate a rank only when every retained row has known source order;
    // otherwise append as unknown and preserve the bucket's row-id fallback.
    // Existing known ranks are retained by the event-identity lookup first.
    let rewrite = format!(
        "INSERT INTO session_events \
         (source, session_id, project, project_key, project_key_method, cwd, git_branch, \
          message_id, parent_id, ts_ms, role, kind, text, model, token_json, provider, event_uid, \
          tool_use_id, payload_bytes, payload_truncated, payload_hash, call_index, event_index, \
          result_status, event_source, error_signal, subagent_session_id, agent_id, \
          request_id, provider_message_id, stop_reason, agent_version, is_sidechain, is_meta, \
          turn_id, request_span, raw_facts_version, raw_kind, control_kind, record_token_json, source_order) \
         VALUES (?1, ?2, ?3, \
           {insert_key}, \
           {insert_method}, \
           ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, \
           ?14, ?15, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, \
           ?29, ?30, ?31, ?32, ?33, ?34, ?35, ?36, ?37, ?38, ?39, ?40, \
           COALESCE((SELECT source_order FROM session_events WHERE source=?1 AND session_id=?2 AND event_uid=?15 AND ts_ms=?8), \
             (SELECT CASE WHEN COUNT(source_order)=COUNT(*) THEN COALESCE(MAX(source_order),-1)+1 END \
              FROM session_events WHERE source=?1 AND session_id=?2 AND ts_ms=?8))) \
         ON CONFLICT(source, session_id, event_uid) DO UPDATE SET \
         project=excluded.project, \
         project_key={assigned_key}, \
         project_key_method={assigned_method}, \
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
         source_order=CASE WHEN session_events.ts_ms=excluded.ts_ms \
           THEN COALESCE(session_events.source_order,excluded.source_order) ELSE excluded.source_order END, \
         location={LOCATION_ASSIGNED}"
    );
    let unchanged = format!(
        "excluded.source_order IS NOT session_events.source_order \
         OR excluded.project IS NOT session_events.project \
         OR ({assigned_key}) IS NOT session_events.project_key \
         OR ({assigned_method}) IS NOT session_events.project_key_method \
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
    );
    let upsert = format!("{rewrite} WHERE {unchanged}");
    (rewrite, upsert)
}

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

#[cfg(test)]
mod tests;
