//! Per-role normalization of one Devin session.

use super::{
    edit_file_path, pick_target, str_field, tool_is_error, DevinIngestCounts, DevinNode,
    DevinSession, SOURCE,
};
use crate::ingest::tool_result_facts::ToolResultIndexer;
use crate::insert_session_marker;
use crate::store::NewSessionMarker;
use anyhow::Result;
use rusqlite::Connection;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

pub(super) struct DevinNormalize<'a> {
    pub(super) conn: &'a Connection,
    pub(super) session_id: &'a str,
    pub(super) project: Option<&'a str>,
    pub(super) cwd: Option<&'a str>,
    pub(super) counts: &'a mut DevinIngestCounts,
    pub(super) present_events: &'a mut BTreeSet<String>,
    pub(super) present_tool_calls: &'a mut BTreeSet<String>,
    pub(super) present_file_edits: &'a mut BTreeSet<String>,
    pub(super) present_markers: &'a mut BTreeSet<String>,
    pub(super) present_prompts: &'a mut BTreeSet<String>,
    pub(super) referenced_tool_calls: &'a mut BTreeSet<String>,
    pub(super) indexer: &'a mut ToolResultIndexer,
    pub(super) last_assistant_text: &'a mut Option<String>,
    pub(super) first_ts: &'a mut Option<i64>,
    pub(super) last_ts: &'a mut Option<i64>,
}

struct NodeCtx<'a> {
    node: &'a DevinNode,
    message: &'a Value,
    ts: i64,
    message_id: &'a String,
    parent_id: &'a Option<String>,
    meta: Option<&'a serde_json::Map<String, Value>>,
    message_model: Option<&'a str>,
    token_json: &'a Option<String>,
    identity: super::super::RequestIdentity<'a>,
    raw_facts: super::super::RawMessageFacts<'a>,
}

mod roles;

impl DevinNormalize<'_> {
    fn note_ts(&mut self, ts: i64) {
        let first = *self.first_ts;
        let last = *self.last_ts;
        *self.first_ts = Some(first.map_or(ts, |t| t.min(ts)));
        *self.last_ts = Some(last.map_or(ts, |t| t.max(ts)));
    }

    pub(super) fn index_nodes(
        &mut self,
        loaded: &DevinSession,
        node_message_ids: &BTreeMap<i64, String>,
        base_ts: i64,
    ) -> Result<()> {
        let conn = self.conn;
        let session_id = self.session_id;

        for node in &loaded.nodes {
            let ts = node.created_ms.unwrap_or(base_ts);
            self.note_ts(ts);
            let message_id = node_message_ids
                .get(&node.node_id)
                .cloned()
                .unwrap_or_else(|| format!("n{}", node.node_id));
            let parent_id = node
                .parent_node_id
                .and_then(|parent| node_message_ids.get(&parent))
                .cloned();
            // Node metadata survives even when the message document is malformed
            // or the user turn is synthetic. Record the boundary before either
            // branch can continue past the role-specific normalization below.
            if let Some(summarized) = node
                .node_metadata
                .as_ref()
                .and_then(|m| m.get("summarized_from"))
                .filter(|v| !v.is_null())
            {
                let uid = format!("n{}:compaction", node.node_id);
                self.present_markers.insert(uid.clone());
                let payload =
                    super::super::marker_payload(vec![("summarized_from", summarized.clone())]);
                self.counts.markers += insert_session_marker(
                    conn,
                    SOURCE,
                    session_id,
                    &NewSessionMarker {
                        marker_uid: &uid,
                        ts_ms: Some(ts),
                        message_id: Some(&message_id),
                        parent_id: parent_id.as_deref(),
                        turn_id: None,
                        kind: "compaction_boundary",
                        subkind: None,
                        text: None,
                        payload_json: payload.as_deref(),
                    },
                )?;
            }
            let Some(message) = node.message.as_ref() else {
                self.counts.malformed_nodes += 1;
                // Record the gap under the node's own message id: children that
                // name this node as their parent then anchor to an addressable
                // row instead of dangling.
                let uid = format!("n{}:malformed", node.node_id);
                self.present_markers.insert(uid.clone());
                self.counts.markers += insert_session_marker(
                    conn,
                    SOURCE,
                    session_id,
                    &NewSessionMarker {
                        marker_uid: &uid,
                        ts_ms: Some(ts),
                        message_id: Some(&message_id),
                        parent_id: parent_id.as_deref(),
                        turn_id: None,
                        kind: "malformed_node",
                        subkind: None,
                        text: node
                            .node_metadata
                            .as_ref()
                            .and_then(|m| serde_json::to_string(m).ok())
                            .map(|s| super::super::truncate_marker_text(&s))
                            .as_deref(),
                        payload_json: None,
                    },
                )?;
                continue;
            };
            let meta = message.get("metadata").and_then(Value::as_object);
            let role = str_field(message.into(), "role").unwrap_or("");
            let message_model = meta
                .and_then(|m| m.get("generation_model"))
                .and_then(Value::as_str)
                .or(loaded.info.model.as_deref());
            let token_json = meta
                .and_then(|m| m.get("num_tokens"))
                .filter(|v| v.is_number())
                .map(|n| format!("{{\"num_tokens\":{n}}}"));
            let identity = super::super::RequestIdentity {
                request_id: meta
                    .and_then(|m| m.get("request_id"))
                    .and_then(Value::as_str),
                provider_message_id: message.get("message_id").and_then(Value::as_str),
            };
            let raw_facts = super::super::RawMessageFacts {
                request_id: identity.request_id,
                stop_reason: meta
                    .and_then(|m| m.get("finish_reason"))
                    .and_then(Value::as_str),
                agent_version: loaded
                    .transcript
                    .as_ref()
                    .and_then(|t| t.agent_version.as_deref()),
                is_sidechain: None,
                is_meta: None,
                turn_id: None,
                request_span: None,
                // The control vocabulary covers Claude and Codex wrappers; Devin
                // marks its synthetic user turns with `is_user_input` instead.
                control_kind: None,
                // Devin's usage is stored once per message; `token_json` is it.
                record_token_json: None,
            };

            let ctx = NodeCtx {
                node,
                message,
                ts,
                message_id: &message_id,
                parent_id: &parent_id,
                meta,
                message_model,
                token_json: &token_json,
                identity,
                raw_facts,
            };
            match role {
                "user" => self.index_user(&ctx)?,
                "assistant" | "final_answer" => self.index_assistant(loaded, &ctx)?,
                "tool" => self.index_tool(loaded, &ctx)?,
                "system" => self.index_system(&ctx)?,
                other => self.index_other(&ctx, other)?,
            }
        }
        Ok(())
    }

    pub(super) fn index_orphans(&mut self, loaded: &DevinSession, base_ts: i64) -> Result<()> {
        let conn = self.conn;
        let session_id = self.session_id;
        let project = self.project;
        let cwd = self.cwd;
        let counts = &mut *self.counts;
        let present_events = &mut *self.present_events;
        let present_tool_calls = &mut *self.present_tool_calls;
        let present_file_edits = &mut *self.present_file_edits;
        let referenced_tool_calls = &mut *self.referenced_tool_calls;

        // Tool calls the provider persisted without a referencing message node —
        // they are still evidence, keyed by the call's own id.
        let orphan_ts = loaded.info.last_activity_ms.unwrap_or(base_ts);
        for (call_id, state) in &loaded.tools {
            if referenced_tool_calls.contains(call_id) {
                continue;
            }
            if state.call.is_none() && state.update.is_none() {
                counts.malformed_tool_state += 1;
                continue;
            }
            let call = state.call.as_ref();
            let name = call
                .and_then(|c| c.get("title"))
                .and_then(Value::as_str)
                .or_else(|| call.and_then(|c| c.get("name")).and_then(Value::as_str))
                .unwrap_or("unknown");
            let arguments = call.and_then(|c| c.get("rawInput"));
            let args_json = arguments
                .map(|a| serde_json::to_string(a).unwrap_or_else(|_| "{}".into()))
                .unwrap_or_else(|| "{}".to_string());
            let target = pick_target(Some(state), arguments);
            let message_id = format!("tcs:{call_id}");
            let uid = format!("tcs:{call_id}:tool");
            present_events.insert(uid.clone());
            super::super::insert_session_event(
                conn,
                &super::super::EventRow {
                    source: SOURCE,
                    session_id,
                    project,
                    cwd,
                    message_id: &message_id,
                    ts_ms: orphan_ts,
                    role: "assistant",
                    kind: "tool_use",
                    text: Some(&super::super::format_tool_event_text(
                        name,
                        target,
                        arguments.unwrap_or(&Value::Null),
                    )),
                    model: loaded.info.model.as_deref(),
                    event_uid: &uid,
                    ..super::super::EventRow::default()
                },
            )?;
            counts.events += 1;
            present_tool_calls.insert(call_id.clone());
            let call = super::super::ToolCallRef {
                source: SOURCE,
                session_id,
                message_id: &message_id,
                tool_use_id: call_id,
                ts_ms: orphan_ts,
                git_branch: None,
                cwd,
            };
            super::super::insert_tool_call(
                conn,
                &call,
                name,
                target,
                &args_json,
                tool_is_error(Some(state)),
            )?;
            counts.tool_calls += 1;
            if let Some(path) = edit_file_path(Some(state), name, arguments) {
                present_file_edits.insert(call_id.clone());
                super::super::upsert_file_edit_from_call(conn, &call, path, name)?;
                counts.file_edits += 1;
            }
        }
        Ok(())
    }

    pub(super) fn index_meta(&mut self, loaded: &DevinSession) -> Result<()> {
        let conn = self.conn;
        let session_id = self.session_id;
        let counts = &mut *self.counts;
        let present_markers = &mut *self.present_markers;

        // Session-level metadata that has no canonical column: the CLI's own
        // title, its mode/backend, and the transcript's agent envelope ride along
        // as bounded markers rather than being dropped.
        if let Some(title) = loaded
            .info
            .title
            .as_deref()
            .map(str::trim)
            .filter(|t| !t.is_empty())
        {
            let uid = "meta:title".to_string();
            present_markers.insert(uid.clone());
            counts.markers += insert_session_marker(
                conn,
                SOURCE,
                session_id,
                &NewSessionMarker {
                    marker_uid: &uid,
                    ts_ms: loaded.info.created_ms,
                    message_id: None,
                    parent_id: None,
                    turn_id: None,
                    kind: "session_title",
                    subkind: None,
                    text: Some(&super::super::truncate_marker_text(title)),
                    payload_json: None,
                },
            )?;
        }
        {
            let payload = super::super::marker_payload(vec![
                (
                    "backend_type",
                    loaded
                        .info
                        .backend_type
                        .clone()
                        .map(Value::String)
                        .unwrap_or(Value::Null),
                ),
                (
                    "agent_mode",
                    loaded
                        .info
                        .agent_mode
                        .clone()
                        .map(Value::String)
                        .unwrap_or(Value::Null),
                ),
                (
                    "model",
                    loaded
                        .info
                        .model
                        .clone()
                        .map(Value::String)
                        .unwrap_or(Value::Null),
                ),
                (
                    "workspace_dirs",
                    if loaded.info.workspace_dirs.is_empty() {
                        Value::Null
                    } else {
                        Value::Array(
                            loaded
                                .info
                                .workspace_dirs
                                .iter()
                                .cloned()
                                .map(Value::String)
                                .collect(),
                        )
                    },
                ),
                (
                    "metadata",
                    loaded
                        .info
                        .metadata
                        .clone()
                        .map(Value::Object)
                        .unwrap_or(Value::Null),
                ),
            ]);
            if let Some(payload) = payload {
                let uid = "meta:session".to_string();
                present_markers.insert(uid.clone());
                counts.markers += insert_session_marker(
                    conn,
                    SOURCE,
                    session_id,
                    &NewSessionMarker {
                        marker_uid: &uid,
                        ts_ms: loaded.info.created_ms,
                        message_id: None,
                        parent_id: None,
                        turn_id: None,
                        kind: "session_meta",
                        subkind: None,
                        text: None,
                        payload_json: Some(&payload),
                    },
                )?;
            }
        }
        if let Some(transcript) = &loaded.transcript {
            let payload = super::super::marker_payload(vec![
                (
                    "agent_name",
                    transcript
                        .agent_name
                        .clone()
                        .map(Value::String)
                        .unwrap_or(Value::Null),
                ),
                (
                    "agent_version",
                    transcript
                        .agent_version
                        .clone()
                        .map(Value::String)
                        .unwrap_or(Value::Null),
                ),
                (
                    "model_name",
                    transcript
                        .model_name
                        .clone()
                        .map(Value::String)
                        .unwrap_or(Value::Null),
                ),
                (
                    "schema_version",
                    transcript.schema_version.clone().unwrap_or(Value::Null),
                ),
                (
                    "final_metrics",
                    transcript
                        .final_metrics
                        .clone()
                        .map(Value::Object)
                        .unwrap_or(Value::Null),
                ),
            ]);
            if let Some(payload) = payload {
                let uid = "meta:agent".to_string();
                present_markers.insert(uid.clone());
                counts.markers += insert_session_marker(
                    conn,
                    SOURCE,
                    session_id,
                    &NewSessionMarker {
                        marker_uid: &uid,
                        ts_ms: loaded.info.last_activity_ms.or(loaded.info.created_ms),
                        message_id: None,
                        parent_id: None,
                        turn_id: None,
                        kind: "agent_manifest",
                        subkind: None,
                        text: None,
                        payload_json: Some(&payload),
                    },
                )?;
            }
        }
        Ok(())
    }
}
