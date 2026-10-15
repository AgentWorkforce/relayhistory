//! One captured journal into the store. Messages, tool requests and results
//! become events and tool calls; every other event is a raw marker, so
//! session lifecycle, model changes, subagents and the cumulative
//! `session.shutdown` usage totals stay readable exactly as written.
use super::*;
use crate::ingest::{
    insert_session_event, insert_tool_call, set_tool_call_error, upsert_session_rebuilt, EventRow,
    RawMessageFacts, RequestIdentity, SessionCatalogRow, ToolCallRef, ToolResultFacts,
};
use crate::providers::file_history::{self, Keys};
use crate::{insert_history, insert_session_marker, HistoryEntry, NewSessionMarker};
use std::collections::{HashMap, HashSet};

pub(super) fn check_identity(conn: &Connection, sid: &str, path: &Path) -> Result<()> {
    file_history::check_identity(conn, ID, "COPILOT", sid, path)
}

pub(super) fn write(conn: &Connection, payload: &Value, path: &Path) -> Result<usize> {
    let entries = payload["entries"]
        .as_array()
        .context("Copilot entries missing")?;
    let header = &entries[0];
    let sid = format::session_id(header)?;
    check_identity(conn, sid, path)?;
    let context = &header["data"]["context"];
    let mut writer = Writer {
        conn,
        sid,
        cwd: context["cwd"].as_str(),
        branch: context["branch"].as_str(),
        version: header["data"]["copilotVersion"].as_str(),
        keys: Keys::default(),
        requested: HashSet::new(),
        markers: HashMap::new(),
        assistant: None,
        inserted: 0,
    };
    let first = format::time(header)?;
    let mut last = first;
    for entry in entries {
        crate::ingest::check_capture_cancelled()?;
        last = last.max(format::time(entry)?);
        writer.entry(entry)?;
    }
    let complete = payload["tail"].as_array().is_some_and(Vec::is_empty);
    if complete {
        file_history::retire(conn, ID, sid, &writer.keys)?;
        // A journal now declaring another identity no longer backs the old one.
        file_history::retire_locator_sessions(conn, ID, path, Some(sid))?;
    }
    let catalog_writer = if complete {
        upsert_session_rebuilt
    } else {
        crate::ingest::upsert_session
    };
    catalog_writer(
        conn,
        &SessionCatalogRow {
            source: ID,
            session_id: sid,
            cwd: writer.cwd,
            git_branch: writer.branch,
            first_ts: first,
            last_ts: last,
            last_assistant_text: complete.then_some(writer.assistant.as_deref()).flatten(),
            raw_path: Some(&path.to_string_lossy()),
        },
    )?;
    if complete {
        crate::ingest::set_session_title(conn, ID, sid, format::title(entries))?;
        let first_prompt = format::first_prompt(entries);
        let models = serde_json::to_string(&format::models(entries))?;
        conn.execute("UPDATE sessions SET first_prompt=?2, models_json=?3 WHERE source='copilot' AND session_id=?1 AND (first_prompt IS NOT ?2 OR models_json IS NOT ?3)", rusqlite::params![sid, first_prompt, models])?;
    }
    Ok(writer.inserted)
}

struct Writer<'a> {
    conn: &'a Connection,
    sid: &'a str,
    cwd: Option<&'a str>,
    branch: Option<&'a str>,
    version: Option<&'a str>,
    keys: Keys,
    /// Tool call IDs an assistant message already requested.
    requested: HashSet<String>,
    /// Marker record ID → its `parentId`, so an event's parent can skip the
    /// turn boundaries and other markers between it and the record before.
    markers: HashMap<&'a str, Option<&'a str>>,
    assistant: Option<String>,
    inserted: usize,
}

impl<'a> Writer<'a> {
    fn entry(&mut self, entry: &'a Value) -> Result<()> {
        match entry["type"].as_str() {
            Some("user.message") => self.user(entry),
            Some("assistant.message") => self.assistant(entry),
            Some("tool.execution_start") => self.tool_start(entry),
            Some("tool.execution_complete") => self.tool_result(entry),
            _ => self.marker(entry),
        }
    }

    fn base<'b>(&self, entry: &'b Value, role: &'b str) -> Result<EventRow<'b>>
    where
        'a: 'b,
    {
        let data = &entry["data"];
        Ok(EventRow {
            source: ID,
            session_id: self.sid,
            message_id: format::string(entry, "id")?,
            parent_id: self.parent(entry),
            cwd: self.cwd,
            project: self.cwd,
            git_branch: self.branch,
            ts_ms: format::time(entry)?,
            role,
            model: data["model"].as_str(),
            raw_kind: entry["type"].as_str(),
            raw_facts: RawMessageFacts {
                agent_version: self.version,
                turn_id: data["turnId"].as_str(),
                is_sidechain: data["parentToolCallId"].is_string().then_some(true),
                control_kind: (entry["type"] == "user.message" && !format::is_human_prompt(entry))
                    .then_some(crate::ingest::control::ControlKind::Synthetic.as_str()),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    /// The nearest record up the `parentId` chain that is stored as an event.
    /// Copilot chains every record to the one before it, so a message's
    /// `parentId` often names a turn boundary or another marker.
    fn parent(&self, entry: &'a Value) -> Option<&'a str> {
        let mut parent = entry["parentId"].as_str();
        let mut hops = 0;
        while let Some(grandparent) = parent.and_then(|id| self.markers.get(id)) {
            parent = *grandparent;
            hops += 1;
            if hops > self.markers.len() {
                return None;
            }
        }
        parent
    }

    fn event(
        &mut self,
        row: EventRow<'_>,
        index: usize,
        kind: &str,
        text: Option<&str>,
    ) -> Result<()> {
        let uid = format!("{}:{index}", row.message_id);
        insert_session_event(
            self.conn,
            &EventRow {
                event_uid: &uid,
                kind,
                text,
                ..row
            },
        )?;
        self.keys.events.insert(uid);
        Ok(())
    }

    fn user(&mut self, entry: &'a Value) -> Result<()> {
        let base = self.base(entry, "user")?;
        let text = entry["data"]["content"].as_str().unwrap_or_default();
        self.event(base, 0, "text", Some(text))?;
        if !format::is_human_prompt(entry) || text.is_empty() {
            return Ok(());
        }
        let hash = crate::prompt_hash(text);
        self.keys.prompts.insert(format!("{}:{hash}", base.ts_ms));
        self.inserted += insert_history(
            self.conn,
            &HistoryEntry {
                id: 0,
                source: ID.into(),
                session_id: Some(self.sid.into()),
                project: self.cwd.map(str::to_owned),
                prompt: text.into(),
                prompt_hash: Some(hash),
                timestamp_ms: base.ts_ms,
            },
        )?;
        Ok(())
    }

    fn assistant(&mut self, entry: &'a Value) -> Result<()> {
        let data = &entry["data"];
        // The one usage counter Copilot persists per message, kept verbatim.
        let token = data
            .get("outputTokens")
            .map(|n| serde_json::to_string(&serde_json::json!({ "outputTokens": n })))
            .transpose()?;
        let base = EventRow {
            token_json: token.as_deref(),
            identity: RequestIdentity {
                request_id: data["requestId"].as_str().filter(|s| !s.trim().is_empty()),
                provider_message_id: data["messageId"].as_str().filter(|s| !s.trim().is_empty()),
            },
            ..self.base(entry, "assistant")?
        };
        let mut index = 0;
        if let Some(reasoning) = data["reasoningText"].as_str().filter(|s| !s.is_empty()) {
            self.event(base, index, "thinking", Some(reasoning))?;
            index += 1;
        }
        let text = data["content"].as_str().unwrap_or_default();
        if !text.is_empty() {
            self.event(base, index, "text", Some(text))?;
            index += 1;
            // A subagent's reply is not the conversation's latest answer.
            if base.raw_facts.is_sidechain.is_none() {
                self.assistant = Some(text.into());
            }
        }
        for request in format::tool_requests(data)? {
            self.tool_use(&base, index, data, request)?;
            index += 1;
        }
        // An assistant message with no content still carries its usage.
        if index == 0 {
            self.event(base, 0, "text", None)?;
        }
        Ok(())
    }

    fn tool_use(
        &mut self,
        base: &EventRow<'_>,
        index: usize,
        message: &Value,
        request: &Value,
    ) -> Result<()> {
        let id = format::call_id(message, format::string(request, "toolCallId")?);
        let name = format::string(request, "name")?;
        self.call(base, &id, name, &request["arguments"])?;
        let facts = ToolResultFacts {
            tool_use_id: Some(id),
            ..Default::default()
        };
        let block = serde_json::to_string(request)?;
        self.event(
            EventRow {
                tool_result_facts: Some(&facts),
                ..*base
            },
            index,
            "tool_use",
            Some(&block),
        )
    }

    fn call(&mut self, base: &EventRow<'_>, id: &str, name: &str, args: &Value) -> Result<()> {
        insert_tool_call(
            self.conn,
            &ToolCallRef {
                source: ID,
                session_id: self.sid,
                message_id: base.message_id,
                tool_use_id: id,
                ts_ms: base.ts_ms,
                cwd: self.cwd,
                git_branch: self.branch,
            },
            name,
            args["path"].as_str(),
            &serde_json::to_string(args)?,
            None,
        )?;
        self.requested.insert(id.into());
        self.keys.tools.insert(id.into());
        Ok(())
    }

    /// A start is a marker; it is also the call itself when no assistant
    /// message requested it (a user-requested tool).
    fn tool_start(&mut self, entry: &'a Value) -> Result<()> {
        let data = &entry["data"];
        let id = format::call_id(data, format::string(data, "toolCallId")?);
        if !self.requested.contains(&id) {
            let base = self.base(entry, "assistant")?;
            self.call(
                &base,
                &id,
                format::string(data, "toolName")?,
                &data["arguments"],
            )?;
        }
        self.marker(entry)
    }

    fn tool_result(&mut self, entry: &'a Value) -> Result<()> {
        let data = &entry["data"];
        let id = format::call_id(data, format::string(data, "toolCallId")?);
        let error = !data["success"].as_bool().unwrap_or_default();
        let payload = if error {
            &data["error"]["message"]
        } else {
            &data["result"]["content"]
        };
        let text = payload.as_str();
        let mut facts = ToolResultFacts::from_payload(payload);
        facts.tool_use_id = Some(id.clone());
        facts.result_status = Some(if error { "errored" } else { "completed" }.into());
        facts.error_signal = error.then(|| "tool.execution_complete.success".into());
        let base = self.base(entry, "tool_result")?;
        self.event(
            EventRow {
                tool_result_facts: Some(&facts),
                ..base
            },
            0,
            "tool_result",
            text,
        )?;
        set_tool_call_error(self.conn, ID, self.sid, &id, error)?;
        self.keys.results.insert(id);
        Ok(())
    }

    fn marker(&mut self, entry: &'a Value) -> Result<()> {
        let id = format::string(entry, "id")?;
        self.markers.insert(id, entry["parentId"].as_str());
        let data = &entry["data"];
        let payload = serde_json::to_string(&crate::ingest::bound_marker_value(entry.clone()))?;
        insert_session_marker(
            self.conn,
            ID,
            self.sid,
            &NewSessionMarker {
                marker_uid: id,
                kind: format::string(entry, "type")?,
                ts_ms: Some(format::time(entry)?),
                parent_id: entry["parentId"].as_str(),
                message_id: Some(id),
                turn_id: data["turnId"].as_str(),
                text: ["title", "message", "summary"]
                    .iter()
                    .find_map(|key| data[key].as_str()),
                payload_json: Some(&payload),
                ..Default::default()
            },
        )?;
        self.keys.markers.insert(id.into());
        Ok(())
    }
}
