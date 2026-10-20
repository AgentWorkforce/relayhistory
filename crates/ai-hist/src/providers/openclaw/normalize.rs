//! OpenClaw transcript entries are Pi session entries (header, message,
//! model_change, compaction, custom, ...) stored as SQLite rows. Messages carry
//! OpenClaw's own delivery and run metadata beside the Pi fields.
use super::conversation::Conversation;
use super::format::{
    control_kind, counts_usage, entry_time, first_prompt, is_delivery_mirror, is_heartbeat_ack,
    string, text,
};
use super::*;
use crate::ingest::{
    insert_session_event, insert_tool_call, set_tool_call_error, upsert_session_rebuilt, EventRow,
    RawMessageFacts, SessionCatalogRow, ToolCallRef, ToolResultFacts,
};
use crate::providers::file_history::{self, Keys};
use crate::{insert_history, insert_session_marker, HistoryEntry, NewSessionMarker};
use anyhow::Context;

/// Message fields OpenClaw adds to Pi messages. Retained verbatim in a marker.
const OPENCLAW_FIELDS: &[&str] = &[
    "__openclaw",
    "idempotencyKey",
    "openclawDelivery",
    "openclawDeliveryMirror",
    "openclawDisplayContent",
    "openclawStreamFallback",
    "provenance",
    "excludeFromContext",
    "diagnostics",
    "errorMessage",
    "responseModel",
    "supersedesEntryId",
];

/// What one pass over a conversation's windows wrote.
struct Written {
    keys: Keys,
    inserted: usize,
    first: i64,
    last: i64,
    assistant: Option<String>,
}
impl Written {
    fn new(conv: &Conversation) -> Self {
        Self {
            keys: Keys::default(),
            inserted: 0,
            first: conv.started(),
            last: conv.last_activity().max(conv.started()),
            assistant: None,
        }
    }
}

fn header(s: &Session) -> Result<&Event> {
    s.events
        .first()
        .filter(|e| e.entry["type"] == "session")
        .context("OPENCLAW_INVALID_RECORD: transcript has no session header")
}
fn cwd(header: &Event) -> Option<&str> {
    header.entry["cwd"]
        .as_str()
        .filter(|c| !c.trim().is_empty())
}
/// Write one window's header marker and every captured event after it.
fn write(conn: &Connection, s: &Session, out: &mut Written) -> Result<()> {
    crate::ingest::check_capture_cancelled()?;
    let header = header(s)?;
    let cwd = cwd(header);
    session_marker(conn, s, header, &mut out.keys)?;
    for event in &s.events[1..] {
        crate::ingest::check_capture_cancelled()?;
        let entry = &event.entry;
        let id = string(entry, "id")?;
        let ts = entry_time(entry)?;
        out.first = out.first.min(ts);
        out.last = out.last.max(ts);
        if entry["type"] == "message" {
            out.inserted += message(conn, s, cwd, event, &mut out.keys)?;
            let msg = &entry["message"];
            if msg["role"] == "assistant" && !is_delivery_mirror(msg) {
                let text = text(&msg["content"]);
                // A quiet heartbeat acknowledgement is not the session's reply.
                if !text.trim().is_empty() && !is_heartbeat_ack(&text) {
                    out.assistant = Some(text);
                }
            }
        } else {
            marker(
                conn,
                s,
                entry,
                id,
                string(entry, "type")?,
                None,
                &mut out.keys,
            )?;
        }
    }
    Ok(())
}

/// Normalize a complete conversation: authoritative for every row and
/// catalog field, so evidence its windows no longer hold is retired.
pub(super) fn session(conn: &Connection, conv: &Conversation, path: &Path) -> Result<usize> {
    let mut written = Written::new(conv);
    for window in &conv.windows {
        write(conn, window, &mut written)?;
    }
    super::legacy::claim(conn, conv, path)?;
    file_history::retire(conn, ID, &conv.id, &written.keys)?;
    let cwd = conv.cwd();
    upsert_session_rebuilt(
        conn,
        &SessionCatalogRow {
            source: ID,
            session_id: &conv.id,
            cwd,
            git_branch: None,
            first_ts: written.first,
            last_ts: written.last,
            last_assistant_text: written.assistant.as_deref(),
            raw_path: Some(&path.to_string_lossy()),
        },
    )?;
    crate::ingest::set_session_title(conn, ID, &conv.id, conv.title())?;
    let models = serde_json::to_string(&conv.models())?;
    let first_prompt = first_prompt(conv.entries());
    // A complete snapshot is authoritative even when metadata was cleared.
    conn.execute(
        "UPDATE sessions SET models_json=?3, first_prompt=?4, cwd=?5 WHERE source=?1 AND session_id=?2 AND (models_json IS NOT ?3 OR first_prompt IS NOT ?4 OR cwd IS NOT ?5)",
        rusqlite::params![ID, conv.id, models, first_prompt, cwd],
    )?;
    Ok(written.inserted)
}

/// Normalize the windows of a conversation that only grew since its last
/// complete read: each captured window's header plus its re-read tail, or a
/// new window whole. Windows with no captured events were not re-read.
/// Nothing is retired and catalog fields only widen.
pub(super) fn append(conn: &Connection, conv: &Conversation, path: &Path) -> Result<usize> {
    let mut written = Written::new(conv);
    for window in conv.windows.iter().filter(|w| !w.events.is_empty()) {
        write(conn, window, &mut written)?;
    }
    super::legacy::claim(conn, conv, path)?;
    crate::ingest::upsert_session(
        conn,
        &SessionCatalogRow {
            source: ID,
            session_id: &conv.id,
            cwd: conv.cwd(),
            git_branch: None,
            first_ts: written.first,
            last_ts: written.last,
            last_assistant_text: written.assistant.as_deref(),
            raw_path: Some(&path.to_string_lossy()),
        },
    )?;
    if let Some(title) = conv.title() {
        crate::ingest::set_session_title(conn, ID, &conv.id, Some(title))?;
    }
    let models = serde_json::to_string(&conv.models())?;
    conn.execute(
        "UPDATE sessions SET models_json=(SELECT json_group_array(value) FROM (SELECT value FROM json_each(coalesce(sessions.models_json,'[]')) UNION SELECT value FROM json_each(?3) ORDER BY value)), \
         first_prompt=coalesce(first_prompt, ?4) WHERE source=?1 AND session_id=?2",
        rusqlite::params![ID, conv.id, models, first_prompt(conv.entries())],
    )?;
    Ok(written.inserted)
}

/// One window's session header plus the OpenClaw catalog row it belongs to.
fn session_marker(conn: &Connection, s: &Session, header: &Event, keys: &mut Keys) -> Result<()> {
    let mut payload = serde_json::json!({
        "header": header.entry,
        "native_session_id": s.native_id,
        "agent_id": s.agent_id,
        "session_key": s.session_key,
        "channel": s.channel,
        "window_channel": s.window_channel,
        "account_id": s.account_id,
        "chat_type": s.chat_type,
        "session_scope": s.scope,
        "window_reason": s.reason,
        "previous_session_id": s.previous_session_id,
        "parent_session_key": s.parent_session_key,
        "spawned_by": s.spawned_by,
        "created_via": s.created_via,
        "agent_harness_id": s.harness,
        "model_provider": s.model_provider,
        "model": s.model,
        "status": s.status,
        "ended_at_ms": s.ended,
        "schema_version": s.schema_version,
        "cold_archive": s.cold.as_ref().map(|c| serde_json::json!({"name": c.name, "storage": c.storage, "archived_at_ms": c.archived_at, "event_count": c.event_count})),
    });
    super::legacy::mark(&mut payload, s);
    let payload = serde_json::to_string(&crate::ingest::bound_marker_value(payload))?;
    // One header marker per window; the session ID is the window's own.
    let uid = format!("session:{}", s.native_id);
    insert_session_marker(
        conn,
        ID,
        &s.id,
        &NewSessionMarker {
            marker_uid: &uid,
            kind: "session_metadata",
            subkind: Some("openclaw"),
            ts_ms: Some(entry_time(&header.entry)?),
            payload_json: Some(&payload),
            ..Default::default()
        },
    )?;
    keys.markers.insert(uid);
    Ok(())
}

fn marker(
    conn: &Connection,
    s: &Session,
    entry: &Value,
    uid: &str,
    kind: &str,
    payload: Option<Value>,
    keys: &mut Keys,
) -> Result<()> {
    let payload = payload.unwrap_or_else(|| entry.clone());
    let payload = serde_json::to_string(&crate::ingest::bound_marker_value(payload))?;
    insert_session_marker(
        conn,
        ID,
        &s.id,
        &NewSessionMarker {
            marker_uid: uid,
            kind,
            ts_ms: Some(entry_time(entry)?),
            parent_id: entry["parentId"].as_str(),
            message_id: entry["id"].as_str(),
            text: entry["summary"].as_str(),
            payload_json: Some(&payload),
            ..Default::default()
        },
    )?;
    keys.markers.insert(uid.into());
    Ok(())
}

fn message(
    conn: &Connection,
    s: &Session,
    cwd: Option<&str>,
    event: &Event,
    keys: &mut Keys,
) -> Result<usize> {
    let entry = &event.entry;
    let msg = &entry["message"];
    let role = string(msg, "role")?;
    let mid = string(entry, "id")?;
    let control = control_kind(msg);
    let mut metadata = openclaw_metadata(msg, event.active);
    if role == "assistant" && !counts_usage(msg) {
        if let Some(usage) = msg.get("usage") {
            // Retained as reported, outside request accounting.
            let fields = metadata.get_or_insert_with(|| Value::Object(Default::default()));
            fields["uncounted_usage"] = usage.clone();
        }
    }
    if let Some(metadata) = metadata {
        let uid = format!("message:{mid}");
        marker(
            conn,
            s,
            entry,
            &uid,
            "message_metadata",
            Some(metadata),
            keys,
        )?;
    }
    if !["user", "assistant", "toolResult"].contains(&role) {
        // Custom and other extension roles never become human or model turns.
        marker(conn, s, entry, mid, role, None, keys)?;
        return Ok(0);
    }
    let ts = match msg.get("timestamp") {
        Some(value) => value
            .as_i64()
            .context("OPENCLAW_INVALID_RECORD: message timestamp must be integer milliseconds")?,
        None => entry_time(entry)?,
    };
    let token = (role == "assistant" && counts_usage(msg))
        .then(|| msg.get("usage"))
        .flatten()
        .map(serde_json::to_string)
        .transpose()?;
    let base = EventRow {
        source: ID,
        session_id: &s.id,
        message_id: mid,
        parent_id: entry["parentId"].as_str(),
        cwd,
        project: cwd,
        ts_ms: ts,
        role: if role == "toolResult" {
            "tool_result"
        } else {
            role
        },
        model: msg["model"].as_str(),
        provider: msg["provider"].as_str(),
        token_json: token.as_deref(),
        // Inactive branches keep copies of the same response; its provider
        // response ID makes every copy one request, counted once.
        identity: crate::ingest::RequestIdentity {
            request_id: None,
            provider_message_id: (role == "assistant")
                .then(|| msg["responseId"].as_str())
                .flatten()
                .filter(|id| !id.trim().is_empty()),
        },
        raw_kind: Some("message"),
        raw_facts: RawMessageFacts {
            stop_reason: msg["stopReason"].as_str(),
            control_kind: control,
            ..Default::default()
        },
        ..Default::default()
    };
    if role == "toolResult" {
        result(conn, &base, msg, keys)?;
    } else {
        content(conn, s, &base, &msg["content"], keys)?;
    }
    if role != "user" || control.is_some() {
        return Ok(0);
    }
    let text = text(&msg["content"]);
    if text.is_empty() {
        return Ok(0);
    }
    let hash = crate::prompt_hash(&text);
    keys.prompts.insert(format!("{ts}:{hash}"));
    insert_history(
        conn,
        &HistoryEntry {
            id: 0,
            source: ID.into(),
            session_id: Some(s.id.clone()),
            project: cwd.map(str::to_owned),
            prompt: text,
            prompt_hash: Some(hash),
            timestamp_ms: ts,
        },
    )
}

/// OpenClaw-owned message fields, or None when the message is plain Pi.
fn openclaw_metadata(msg: &Value, active: bool) -> Option<Value> {
    let mut fields = serde_json::Map::new();
    for key in OPENCLAW_FIELDS {
        if let Some(value) = msg.get(*key) {
            fields.insert((*key).into(), value.clone());
        }
    }
    if !active {
        fields.insert("active_branch".into(), Value::Bool(false));
    }
    (!fields.is_empty()).then_some(Value::Object(fields))
}

fn content(
    conn: &Connection,
    s: &Session,
    base: &EventRow<'_>,
    value: &Value,
    keys: &mut Keys,
) -> Result<()> {
    let blocks = value
        .as_array()
        .cloned()
        .unwrap_or_else(|| vec![serde_json::json!({"type":"text", "text":value})]);
    for (index, block) in blocks.iter().enumerate() {
        let uid = format!("{}:{index}", base.message_id);
        let (kind, text) = match block["type"].as_str().unwrap_or("unknown") {
            "text" => ("text", block["text"].as_str().map(str::to_owned)),
            "thinking" => ("thinking", block["thinking"].as_str().map(str::to_owned)),
            "toolCall" => {
                call(conn, base, block, keys)?;
                ("tool_use", Some(serde_json::to_string(block)?))
            }
            _ => {
                opaque_block(conn, s, base, block, &uid, keys)?;
                continue;
            }
        };
        let facts = ToolResultFacts {
            tool_use_id: (kind == "tool_use").then(|| block["id"].as_str().unwrap().to_owned()),
            ..Default::default()
        };
        // A control kind only ever marks a user text row.
        let raw_facts = RawMessageFacts {
            control_kind: base.raw_facts.control_kind.filter(|_| kind == "text"),
            ..base.raw_facts
        };
        insert_session_event(
            conn,
            &EventRow {
                event_uid: &uid,
                kind,
                tool_result_facts: Some(&facts),
                text: text.as_deref(),
                raw_facts,
                ..*base
            },
        )?;
        keys.events.insert(uid);
    }
    // Empty assistant responses can still carry measured usage.
    if blocks.is_empty() {
        let uid = format!("{}:0", base.message_id);
        insert_session_event(
            conn,
            &EventRow {
                event_uid: &uid,
                kind: "text",
                ..*base
            },
        )?;
        keys.events.insert(uid);
    }
    Ok(())
}

fn opaque_block(
    conn: &Connection,
    s: &Session,
    base: &EventRow<'_>,
    block: &Value,
    uid: &str,
    keys: &mut Keys,
) -> Result<()> {
    let payload = serde_json::to_string(&crate::ingest::bound_marker_value(block.clone()))?;
    insert_session_marker(
        conn,
        ID,
        &s.id,
        &NewSessionMarker {
            marker_uid: uid,
            kind: "unknown",
            subkind: block["type"].as_str(),
            ts_ms: Some(base.ts_ms),
            message_id: Some(base.message_id),
            parent_id: base.parent_id,
            payload_json: Some(&payload),
            ..Default::default()
        },
    )?;
    keys.markers.insert(uid.into());
    Ok(())
}

fn call(conn: &Connection, base: &EventRow<'_>, block: &Value, keys: &mut Keys) -> Result<()> {
    let id = string(block, "id")?;
    let name = string(block, "name")?;
    let args = serde_json::to_string(&block["arguments"])?;
    // Paths in arguments are recorded as text; they are never opened.
    insert_tool_call(
        conn,
        &ToolCallRef {
            source: ID,
            session_id: base.session_id,
            message_id: base.message_id,
            tool_use_id: id,
            ts_ms: base.ts_ms,
            cwd: base.cwd,
            ..Default::default()
        },
        name,
        block["arguments"]["path"].as_str(),
        &args,
        None,
    )?;
    keys.tools.insert(id.into());
    Ok(())
}

fn result(conn: &Connection, base: &EventRow<'_>, msg: &Value, keys: &mut Keys) -> Result<()> {
    let id = string(msg, "toolCallId")?;
    let error = match msg.get("isError") {
        None | Some(Value::Null) => None,
        Some(value) => Some(
            value
                .as_bool()
                .context("OPENCLAW_INVALID_RECORD: tool isError must be boolean")?,
        ),
    };
    let text = text(&msg["content"]);
    let mut facts = ToolResultFacts::from_payload(&msg["content"]);
    facts.tool_use_id = Some(id.into());
    facts.result_status = Some(
        match error {
            Some(true) => "errored",
            Some(false) => "completed",
            None => "unknown",
        }
        .into(),
    );
    facts.error_signal = (error == Some(true)).then(|| "is_error".into());
    let uid = format!("{}:0", base.message_id);
    insert_session_event(
        conn,
        &EventRow {
            event_uid: &uid,
            kind: "tool_result",
            text: Some(&text),
            tool_result_facts: Some(&facts),
            ..*base
        },
    )?;
    if let Some(error) = error {
        set_tool_call_error(conn, ID, base.session_id, id, error)?;
        keys.results.insert(id.into());
    }
    keys.events.insert(uid);
    Ok(())
}
