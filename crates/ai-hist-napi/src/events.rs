//! Event evidence crossing the Node boundary without losing native identity.
use ai_hist::SessionEvent as CoreSessionEvent;
use napi_derive::napi;

#[napi(object)]
pub struct NativeSessionEvent {
    pub id: i64,
    pub source: String,
    pub session_id: String,
    pub project: Option<String>,
    /// Canonical project identity, denormalized from the owning session.
    pub project_key: Option<String>,
    pub cwd: Option<String>,
    pub git_branch: Option<String>,
    pub message_id: Option<String>,
    pub parent_id: Option<String>,
    pub ts_ms: i64,
    pub role: String,
    pub kind: String,
    pub text: Option<String>,
    pub model: Option<String>,
    pub token_json: Option<String>,
    /// The upstream inference provider the harness named, when it names one
    /// (OpenCode's `providerID`). Null elsewhere rather than inferred.
    pub provider: Option<String>,
    pub event_uid: String,
    /// Observed order within a timestamp bucket; null when unknown.
    pub source_order: Option<i64>,
    /// Provider-native record or block type, when recorded.
    pub raw_kind: Option<String>,
    /// Upstream API message identity, distinct from the transcript record ID.
    pub provider_message_id: Option<String>,
    /// Recorded request grouping when the provider does not name requests.
    pub request_span: Option<String>,
    /// Per-tool-result fidelity. Null on every row that is not a tool result,
    /// and on a tool-result row whose provider does not record the fact.
    pub tool_use_id: Option<String>,
    /// Raw UTF-8 byte length of the provider's result payload.
    pub payload_bytes: Option<i64>,
    /// True when the harness had already truncated the payload.
    pub payload_truncated: Option<bool>,
    /// First 16 hex characters of the payload's sha256.
    pub payload_hash: Option<String>,
    /// n-th result recorded for this `toolUseId`, from zero.
    pub call_index: Option<i64>,
    /// Position of this result in the transcript's tool-result order.
    pub event_index: Option<i64>,
    /// `running` / `completed` / `errored` / `cancelled` / `unknown`.
    pub result_status: Option<String>,
    /// `tool_result` / `subagent_notification` / `function_call_output`.
    pub event_source: Option<String>,
    /// Which provider signal set the error.
    pub error_signal: Option<String>,
    pub subagent_session_id: Option<String>,
    pub agent_id: Option<String>,
    pub request_id: Option<String>,
    /// Why the turn ended, as the harness reported it.
    pub stop_reason: Option<String>,
    pub agent_version: Option<String>,
    pub is_sidechain: Option<bool>,
    pub is_meta: Option<bool>,
    pub turn_id: Option<String>,
    /// Why a user-role row is not a human prompt; null for a genuine prompt
    /// and for every model-output row.
    pub control_kind: Option<String>,
    /// Claude: the usage blob this one record carried, where it differs from
    /// the settled request blob in `token_json` (JSON `null` for a copy that
    /// carried none). Null wherever `token_json` is the record's own.
    pub record_token_json: Option<String>,
}

impl From<CoreSessionEvent> for NativeSessionEvent {
    fn from(event: CoreSessionEvent) -> Self {
        Self {
            id: event.id,
            source: event.source,
            session_id: event.session_id,
            project: event.project,
            project_key: event.project_key,
            cwd: event.cwd,
            git_branch: event.git_branch,
            message_id: event.message_id,
            parent_id: event.parent_id,
            ts_ms: event.ts_ms,
            role: event.role,
            kind: event.kind,
            text: event.text,
            model: event.model,
            token_json: event.token_json,
            provider: event.provider,
            event_uid: event.event_uid,
            source_order: event.source_order,
            raw_kind: event.raw_kind,
            provider_message_id: event.provider_message_id,
            request_span: event.request_span,
            tool_use_id: event.tool_use_id,
            payload_bytes: event.payload_bytes,
            payload_truncated: event.payload_truncated.map(|value| value != 0),
            payload_hash: event.payload_hash,
            call_index: event.call_index,
            event_index: event.event_index,
            result_status: event.result_status,
            event_source: event.event_source,
            error_signal: event.error_signal,
            subagent_session_id: event.subagent_session_id,
            agent_id: event.agent_id,
            request_id: event.request_id,
            stop_reason: event.stop_reason,
            agent_version: event.agent_version,
            is_sidechain: event.is_sidechain.map(|value| value != 0),
            is_meta: event.is_meta.map(|value| value != 0),
            turn_id: event.turn_id,
            control_kind: event.control_kind,
            record_token_json: event.record_token_json,
        }
    }
}
