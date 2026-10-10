//! Role branches of a Devin session normalization.

use super::super::{
    content_text, content_type, edit_file_path, pick_target, str_field, tool_is_error,
    tool_result_status, DevinSession, ERROR_SIGNAL_TOOL_STATUS, SOURCE,
};
use super::{DevinNormalize, NodeCtx};
use crate::discover;
use crate::ingest::tool_result_facts::{
    ToolResultFacts, EVENT_SOURCE_FUNCTION_CALL_OUTPUT, STATUS_ERRORED,
};
use crate::store::NewSessionMarker;
use crate::{insert_history, insert_session_marker, prompt_hash, HistoryEntry};
use anyhow::Result;
use serde_json::Value;

impl DevinNormalize<'_> {
    pub(super) fn index_user(&mut self, ctx: &NodeCtx<'_>) -> Result<()> {
        let conn = self.conn;
        let session_id = self.session_id;
        let project = self.project;
        let cwd = self.cwd;
        let counts = &mut *self.counts;
        let present_events = &mut *self.present_events;
        let present_markers = &mut *self.present_markers;
        let present_prompts = &mut *self.present_prompts;
        let node = ctx.node;
        let message = ctx.message;
        let ts = ctx.ts;
        let message_id = ctx.message_id;
        let parent_id = ctx.parent_id;
        let meta = ctx.meta;
        let identity = ctx.identity;
        let raw_facts = ctx.raw_facts;

        let is_user_input = meta
            .and_then(|m| m.get("is_user_input"))
            .and_then(Value::as_bool)
            .unwrap_or(true);
        if !is_user_input {
            let uid = format!("n{}:synthetic", node.node_id);
            present_markers.insert(uid.clone());
            counts.markers += insert_session_marker(
                conn,
                SOURCE,
                session_id,
                &NewSessionMarker {
                    marker_uid: &uid,
                    ts_ms: Some(ts),
                    message_id: Some(message_id),
                    parent_id: parent_id.as_deref(),
                    turn_id: None,
                    kind: "synthetic_turn",
                    subkind: Some("user"),
                    text: content_text(Some(message))
                        .as_deref()
                        .map(super::super::super::truncate_marker_text)
                        .as_deref(),
                    payload_json: None,
                },
            )?;
            return Ok(());
        }
        if let Some(text) = content_text(Some(message)).as_deref() {
            let uid = format!("n{}:text", node.node_id);
            present_events.insert(uid.clone());
            super::super::super::insert_session_event(
                conn,
                &super::super::super::EventRow {
                    source: SOURCE,
                    session_id,
                    project,
                    cwd,
                    message_id,
                    parent_id: parent_id.as_deref(),
                    ts_ms: ts,
                    role: "user",
                    kind: "text",
                    text: Some(text),
                    identity,
                    event_uid: &uid,
                    raw_facts,
                    ..super::super::super::EventRow::default()
                },
            )?;
            counts.events += 1;
            let hash = prompt_hash(text);
            present_prompts.insert(format!("{ts}:{hash}"));
            counts.prompts += insert_history(
                conn,
                &HistoryEntry {
                    id: 0,
                    source: SOURCE.to_string(),
                    session_id: Some(session_id.to_string()),
                    project: project.map(str::to_string),
                    prompt: text.to_string(),
                    prompt_hash: Some(hash),
                    timestamp_ms: ts,
                },
            )?;
        } else if let Some(content_type) = content_type(message) {
            // A turn the human sent whose content holds no text this
            // parser can read. It is not a prompt, but it happened:
            // record that it did, and in what shape, without copying
            // the payload itself.
            let uid = format!("n{}:unsupported", node.node_id);
            present_markers.insert(uid.clone());
            counts.markers += insert_session_marker(
                conn,
                SOURCE,
                session_id,
                &NewSessionMarker {
                    marker_uid: &uid,
                    ts_ms: Some(ts),
                    message_id: Some(message_id),
                    parent_id: parent_id.as_deref(),
                    turn_id: None,
                    kind: "unsupported_block",
                    subkind: Some("user_content"),
                    text: None,
                    payload_json: Some(
                        &serde_json::json!({ "content_type": content_type }).to_string(),
                    ),
                },
            )?;
        }
        Ok(())
    }

    pub(super) fn index_assistant(
        &mut self,
        loaded: &DevinSession,
        ctx: &NodeCtx<'_>,
    ) -> Result<()> {
        let conn = self.conn;
        let session_id = self.session_id;
        let project = self.project;
        let cwd = self.cwd;
        let counts = &mut *self.counts;
        let present_events = &mut *self.present_events;
        let present_tool_calls = &mut *self.present_tool_calls;
        let present_file_edits = &mut *self.present_file_edits;
        let referenced_tool_calls = &mut *self.referenced_tool_calls;
        let last_assistant_text = &mut *self.last_assistant_text;
        let node = ctx.node;
        let message = ctx.message;
        let ts = ctx.ts;
        let message_id = ctx.message_id;
        let parent_id = ctx.parent_id;
        let message_model = ctx.message_model;
        let token_json = ctx.token_json;
        let identity = ctx.identity;
        let raw_facts = ctx.raw_facts;

        if let Some(thinking) = message
            .get("thinking")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            let uid = format!("n{}:thinking", node.node_id);
            present_events.insert(uid.clone());
            super::super::super::insert_session_event(
                conn,
                &super::super::super::EventRow {
                    source: SOURCE,
                    session_id,
                    project,
                    cwd,
                    message_id,
                    parent_id: parent_id.as_deref(),
                    ts_ms: ts,
                    role: "assistant",
                    kind: "thinking",
                    text: Some(thinking),
                    model: message_model,
                    token_json: token_json.as_deref(),
                    identity,
                    event_uid: &uid,
                    raw_facts,
                    ..super::super::super::EventRow::default()
                },
            )?;
            counts.events += 1;
        }
        if let Some(text) = content_text(Some(message)).as_deref() {
            let uid = format!("n{}:text", node.node_id);
            present_events.insert(uid.clone());
            super::super::super::insert_session_event(
                conn,
                &super::super::super::EventRow {
                    source: SOURCE,
                    session_id,
                    project,
                    cwd,
                    message_id,
                    parent_id: parent_id.as_deref(),
                    ts_ms: ts,
                    role: "assistant",
                    kind: "text",
                    text: Some(text),
                    model: message_model,
                    token_json: token_json.as_deref(),
                    identity,
                    event_uid: &uid,
                    raw_facts,
                    ..super::super::super::EventRow::default()
                },
            )?;
            counts.events += 1;
            *last_assistant_text = Some(discover::excerpt(text));
        }
        if let Some(calls) = message.get("tool_calls").and_then(Value::as_array) {
            for (index, call) in calls.iter().enumerate() {
                let call_id = str_field(call.into(), "id")
                    .map(str::to_string)
                    .unwrap_or_else(|| format!("n{}:{}", node.node_id, index));
                referenced_tool_calls.insert(call_id.clone());
                let state = loaded.tools.get(&call_id);
                let name = str_field(call.into(), "name")
                    .or_else(|| {
                        state
                            .and_then(|s| s.call.as_ref())
                            .and_then(|c| c.get("title"))
                            .and_then(Value::as_str)
                    })
                    .unwrap_or("unknown");
                let arguments = call.get("arguments").or_else(|| {
                    state
                        .and_then(|s| s.call.as_ref())
                        .and_then(|c| c.get("rawInput"))
                });
                let args_json = arguments
                    .map(|a| serde_json::to_string(a).unwrap_or_else(|_| "{}".into()))
                    .unwrap_or_else(|| "{}".to_string());
                let target = pick_target(state, arguments);
                let is_error = tool_is_error(state);
                let uid = format!("n{}:tool:{call_id}", node.node_id);
                present_events.insert(uid.clone());
                super::super::super::insert_session_event(
                    conn,
                    &super::super::super::EventRow {
                        source: SOURCE,
                        session_id,
                        project,
                        cwd,
                        message_id,
                        parent_id: parent_id.as_deref(),
                        ts_ms: ts,
                        role: "assistant",
                        kind: "tool_use",
                        text: Some(&super::super::super::format_tool_event_text(
                            name,
                            target,
                            arguments.unwrap_or(&Value::Null),
                        )),
                        model: message_model,
                        token_json: token_json.as_deref(),
                        identity,
                        event_uid: &uid,
                        raw_facts,
                        ..super::super::super::EventRow::default()
                    },
                )?;
                counts.events += 1;
                present_tool_calls.insert(call_id.clone());
                let call = super::super::super::ToolCallRef {
                    source: SOURCE,
                    session_id,
                    message_id,
                    tool_use_id: &call_id,
                    ts_ms: ts,
                    git_branch: None,
                    cwd,
                };
                super::super::super::insert_tool_call(
                    conn, &call, name, target, &args_json, is_error,
                )?;
                counts.tool_calls += 1;
                if let Some(path) = edit_file_path(state, name, arguments) {
                    present_file_edits.insert(call_id.clone());
                    super::super::super::upsert_file_edit_from_call(conn, &call, path, name)?;
                    counts.file_edits += 1;
                }
            }
        }
        Ok(())
    }

    pub(super) fn index_tool(&mut self, loaded: &DevinSession, ctx: &NodeCtx<'_>) -> Result<()> {
        let conn = self.conn;
        let session_id = self.session_id;
        let project = self.project;
        let cwd = self.cwd;
        let counts = &mut *self.counts;
        let present_events = &mut *self.present_events;
        let referenced_tool_calls = &mut *self.referenced_tool_calls;
        let indexer = &mut *self.indexer;
        let node = ctx.node;
        let message = ctx.message;
        let ts = ctx.ts;
        let message_id = ctx.message_id;
        let parent_id = ctx.parent_id;
        let identity = ctx.identity;
        let raw_facts = ctx.raw_facts;

        let tool_use_id = str_field(message.into(), "tool_call_id")
            .map(str::to_string)
            .unwrap_or_default();
        if !tool_use_id.is_empty() {
            referenced_tool_calls.insert(tool_use_id.clone());
        }
        let state = loaded.tools.get(&tool_use_id);
        let status = tool_result_status(state);
        let payload = message.get("content").cloned().unwrap_or(Value::Null);
        let (call_index, event_index) = indexer.next(&tool_use_id);
        let mut facts =
            ToolResultFacts::from_payload(&payload).with_ordering(call_index, event_index);
        facts.result_status = Some(status.to_string());
        facts.event_source = Some(EVENT_SOURCE_FUNCTION_CALL_OUTPUT.to_string());
        facts.tool_use_id = Some(tool_use_id.clone());
        if status == STATUS_ERRORED {
            facts.error_signal = Some(ERROR_SIGNAL_TOOL_STATUS.to_string());
        }
        let text = content_text(Some(message));
        let text = text.as_deref();
        let uid = format!("n{}:result:{tool_use_id}", node.node_id);
        present_events.insert(uid.clone());
        super::super::super::insert_session_event(
            conn,
            &super::super::super::EventRow {
                source: SOURCE,
                session_id,
                project,
                cwd,
                message_id,
                parent_id: parent_id.as_deref(),
                ts_ms: ts,
                role: "tool_result",
                kind: "tool_result",
                text,
                identity,
                event_uid: &uid,
                tool_result_facts: Some(&facts),
                raw_facts,
                ..super::super::super::EventRow::default()
            },
        )?;
        counts.events += 1;
        if !tool_use_id.is_empty() && status == STATUS_ERRORED {
            super::super::super::set_tool_call_error(conn, SOURCE, session_id, &tool_use_id, true)?;
        }
        Ok(())
    }

    pub(super) fn index_system(&mut self, ctx: &NodeCtx<'_>) -> Result<()> {
        let conn = self.conn;
        let session_id = self.session_id;
        let counts = &mut *self.counts;
        let present_markers = &mut *self.present_markers;
        let node = ctx.node;
        let message = ctx.message;
        let ts = ctx.ts;
        let message_id = ctx.message_id;
        let parent_id = ctx.parent_id;

        let uid = format!("n{}:system", node.node_id);
        present_markers.insert(uid.clone());
        counts.markers += insert_session_marker(
            conn,
            SOURCE,
            session_id,
            &NewSessionMarker {
                marker_uid: &uid,
                ts_ms: Some(ts),
                message_id: Some(message_id),
                parent_id: parent_id.as_deref(),
                turn_id: None,
                kind: "system",
                subkind: None,
                text: content_text(Some(message))
                    .as_deref()
                    .map(super::super::super::truncate_marker_text)
                    .as_deref(),
                payload_json: None,
            },
        )?;
        Ok(())
    }

    pub(super) fn index_other(&mut self, ctx: &NodeCtx<'_>, other: &str) -> Result<()> {
        let conn = self.conn;
        let session_id = self.session_id;
        let counts = &mut *self.counts;
        let present_markers = &mut *self.present_markers;
        let node = ctx.node;
        let message = ctx.message;
        let ts = ctx.ts;
        let message_id = ctx.message_id;
        let parent_id = ctx.parent_id;

        let uid = format!("n{}:other", node.node_id);
        present_markers.insert(uid.clone());
        counts.markers += insert_session_marker(
            conn,
            SOURCE,
            session_id,
            &NewSessionMarker {
                marker_uid: &uid,
                ts_ms: Some(ts),
                message_id: Some(message_id),
                parent_id: parent_id.as_deref(),
                turn_id: None,
                kind: "unknown",
                subkind: if other.is_empty() { None } else { Some(other) },
                text: content_text(Some(message))
                    .as_deref()
                    .map(super::super::super::truncate_marker_text)
                    .as_deref(),
                payload_json: None,
            },
        )?;
        Ok(())
    }
}
