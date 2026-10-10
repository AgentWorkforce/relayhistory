//! Row mapper for [`super::SessionEvent`].
//!
//! Column order is the `SELECT` list, not struct-field order. Index 36 is
//! `raw_kind`, 37 is `control_kind`, and 38 is `record_token_json`.

use super::SessionEvent;

pub(crate) fn row_to_session_event(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionEvent> {
    let identity = session_event_identity(row)?;
    let payload = session_event_payload(row)?;
    let envelope = session_event_envelope(row)?;
    Ok(SessionEvent {
        id: identity.id,
        source: identity.source,
        session_id: identity.session_id,
        project: identity.project,
        project_key: identity.project_key,
        cwd: identity.cwd,
        git_branch: identity.git_branch,
        message_id: identity.message_id,
        parent_id: identity.parent_id,
        ts_ms: identity.ts_ms,
        role: identity.role,
        kind: identity.kind,
        text: identity.text,
        model: payload.model,
        token_json: payload.token_json,
        provider: payload.provider,
        event_uid: payload.event_uid,
        tool_use_id: payload.tool_use_id,
        payload_bytes: payload.payload_bytes,
        payload_truncated: payload.payload_truncated,
        payload_hash: payload.payload_hash,
        call_index: payload.call_index,
        event_index: payload.event_index,
        result_status: payload.result_status,
        event_source: payload.event_source,
        error_signal: payload.error_signal,
        subagent_session_id: envelope.subagent_session_id,
        agent_id: envelope.agent_id,
        request_id: envelope.request_id,
        provider_message_id: envelope.provider_message_id,
        stop_reason: envelope.stop_reason,
        agent_version: envelope.agent_version,
        is_sidechain: envelope.is_sidechain,
        is_meta: envelope.is_meta,
        turn_id: envelope.turn_id,
        request_span: envelope.request_span,
        raw_kind: envelope.raw_kind,
        control_kind: envelope.control_kind,
        record_token_json: envelope.record_token_json,
    })
}

struct SessionEventIdentity {
    id: i64,
    source: String,
    session_id: String,
    project: Option<String>,
    project_key: Option<String>,
    cwd: Option<String>,
    git_branch: Option<String>,
    message_id: Option<String>,
    parent_id: Option<String>,
    ts_ms: i64,
    role: String,
    kind: String,
    text: Option<String>,
}

fn session_event_identity(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionEventIdentity> {
    Ok(SessionEventIdentity {
        id: row.get(0)?,
        source: row.get(1)?,
        session_id: row.get(2)?,
        project: row.get(3)?,
        project_key: row.get(4)?,
        cwd: row.get(5)?,
        git_branch: row.get(6)?,
        message_id: row.get(7)?,
        parent_id: row.get(8)?,
        ts_ms: row.get(9)?,
        role: row.get(10)?,
        kind: row.get(11)?,
        text: row.get(12)?,
    })
}

struct SessionEventPayload {
    model: Option<String>,
    token_json: Option<String>,
    provider: Option<String>,
    event_uid: String,
    tool_use_id: Option<String>,
    payload_bytes: Option<i64>,
    payload_truncated: Option<i64>,
    payload_hash: Option<String>,
    call_index: Option<i64>,
    event_index: Option<i64>,
    result_status: Option<String>,
    event_source: Option<String>,
    error_signal: Option<String>,
}

fn session_event_payload(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionEventPayload> {
    Ok(SessionEventPayload {
        model: row.get(13)?,
        token_json: row.get(14)?,
        provider: row.get(15)?,
        event_uid: row.get(16)?,
        tool_use_id: row.get(17)?,
        payload_bytes: row.get(18)?,
        payload_truncated: row.get(19)?,
        payload_hash: row.get(20)?,
        call_index: row.get(21)?,
        event_index: row.get(22)?,
        result_status: row.get(23)?,
        event_source: row.get(24)?,
        error_signal: row.get(25)?,
    })
}

struct SessionEventEnvelope {
    subagent_session_id: Option<String>,
    agent_id: Option<String>,
    request_id: Option<String>,
    provider_message_id: Option<String>,
    stop_reason: Option<String>,
    agent_version: Option<String>,
    is_sidechain: Option<i64>,
    is_meta: Option<i64>,
    turn_id: Option<String>,
    request_span: Option<String>,
    raw_kind: Option<String>,
    control_kind: Option<String>,
    record_token_json: Option<String>,
}

fn session_event_envelope(row: &rusqlite::Row<'_>) -> rusqlite::Result<SessionEventEnvelope> {
    Ok(SessionEventEnvelope {
        subagent_session_id: row.get(26)?,
        agent_id: row.get(27)?,
        request_id: row.get(28)?,
        provider_message_id: row.get(29)?,
        stop_reason: row.get(30)?,
        agent_version: row.get(31)?,
        is_sidechain: row.get(32)?,
        is_meta: row.get(33)?,
        turn_id: row.get(34)?,
        request_span: row.get(35)?,
        raw_kind: row.get(36)?,
        control_kind: row.get(37)?,
        record_token_json: row.get(38)?,
    })
}
