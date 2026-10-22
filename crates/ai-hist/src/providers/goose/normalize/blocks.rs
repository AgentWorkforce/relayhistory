use super::*;

struct Sink<'a> {
    conn: &'a Connection,
    session: &'a Session,
    message: &'a Message,
    mid: &'a str,
    keys: &'a mut Keys,
    indexer: &'a mut ToolResultIndexer,
    replay: Option<&'a BTreeSet<i64>>,
}

pub(super) fn message(
    conn: &Connection,
    session: &Session,
    message: &Message,
    keys: &mut Keys,
    indexer: &mut ToolResultIndexer,
    replay: Option<&BTreeSet<i64>>,
) -> Result<usize> {
    ensure!(message.row_id > 0, "invalid Goose message row ID");
    ensure!(
        ["user", "assistant"].contains(&message.role.as_str()),
        "unsupported Goose message role {}",
        message.role
    );
    let blocks = message
        .content
        .as_array()
        .context("Goose content_json must be an array")?;
    let mid = message_id(message);
    let mut sink = Sink {
        conn,
        session,
        message,
        mid: &mid,
        keys,
        indexer,
        replay,
    };
    let inserted = if replay.is_none() {
        sink.metadata()?;
        sink.history(blocks)?
    } else {
        0
    };
    for (position, block) in blocks.iter().enumerate() {
        crate::ingest::check_capture_cancelled()?;
        sink.block(position, block)?;
    }
    Ok(inserted)
}

impl Sink<'_> {
    fn enabled(&self) -> bool {
        self.replay
            .is_none_or(|times| times.contains(&self.message.timestamp))
    }

    fn note_event(&mut self, uid: &str) {
        self.keys.events.insert(uid.into());
        self.keys
            .event_order
            .push((self.message.timestamp, uid.into()));
    }

    fn metadata(&mut self) -> Result<()> {
        marker(
            self.conn,
            self.session,
            GooseMarker {
                uid: &format!("message:{}", self.message.key),
                message: Some(self.mid),
                ts: self.message.timestamp,
                kind: "message_metadata",
                payload: &serde_json::json!({"native_message_id":self.message.native_id,"metadata":self.message.metadata}),
            },
            self.keys,
        )
    }

    fn history(&mut self, blocks: &[Value]) -> Result<usize> {
        let parts = blocks
            .iter()
            .filter(|b| b["type"] == "text")
            .map(|b| b["text"].as_str().context("Goose text block missing text"))
            .collect::<Result<Vec<_>>>()?;
        if parts.is_empty() {
            return Ok(0);
        }
        let text = parts.join("\n");
        let control = is_control(self.message);
        if self.message.role == "user" && !control {
            return crate::insert_history(
                self.conn,
                &HistoryEntry {
                    id: 0,
                    source: ID.into(),
                    session_id: Some(self.session.id.clone()),
                    project: self.session.cwd.clone(),
                    prompt: text.clone(),
                    prompt_hash: None,
                    timestamp_ms: self.message.timestamp,
                },
            );
        }
        Ok(0)
    }

    fn text(&mut self, uid: &str, block: &Value) -> Result<()> {
        let text = block["text"]
            .as_str()
            .context("Goose text block missing text")?;
        if !self.enabled() {
            return Ok(());
        }
        let control = is_control(self.message);
        insert_session_event(
            self.conn,
            &EventRow {
                source: ID,
                session_id: &self.session.id,
                message_id: self.mid,
                event_uid: uid,
                ts_ms: self.message.timestamp,
                role: &self.message.role,
                kind: "text",
                text: Some(text),
                cwd: self.session.cwd.as_deref(),
                raw_kind: Some("messages.content_json"),
                raw_facts: RawMessageFacts {
                    control_kind: if self.message.role == "user" && control {
                        Some("synthetic")
                    } else {
                        None
                    },
                    ..Default::default()
                },
                ..Default::default()
            },
        )?;
        self.note_event(uid);
        Ok(())
    }

    fn block(&mut self, position: usize, block: &Value) -> Result<()> {
        let tag = block["type"]
            .as_str()
            .context("Goose content block missing type")?;
        let uid = format!("{}:{position}:{tag}", self.message.key);
        match tag {
            "text" => self.text(&uid, block),
            "thinking" => self.thinking(&uid, block),
            "toolRequest" => self.call(&uid, block),
            "toolResponse" => self.result(&uid, block),
            "image"
            | "document"
            | "redactedThinking"
            | "toolConfirmationRequest"
            | "actionRequired"
            | "systemNotification"
            | "error" => self.other(&uid, block),
            _ => anyhow::bail!("unsupported Goose content type {tag}"),
        }
    }

    fn thinking(&mut self, uid: &str, block: &Value) -> Result<()> {
        let text = block["thinking"]
            .as_str()
            .context("Goose thinking missing text")?;
        if !self.enabled() {
            return Ok(());
        }
        insert_session_event(
            self.conn,
            &EventRow {
                source: ID,
                session_id: &self.session.id,
                message_id: self.mid,
                event_uid: uid,
                ts_ms: self.message.timestamp,
                role: &self.message.role,
                kind: "thinking",
                text: Some(text),
                raw_kind: Some("messages.content_json"),
                ..Default::default()
            },
        )?;
        self.note_event(uid);
        Ok(())
    }

    fn call(&mut self, uid: &str, block: &Value) -> Result<()> {
        let id = block["id"]
            .as_str()
            .filter(|s| !s.is_empty())
            .context("Goose tool request missing id")?;
        let status = block["toolCall"]["status"]
            .as_str()
            .context("Goose tool call missing status")?;
        ensure!(
            ["success", "error"].contains(&status),
            "unsupported Goose tool call status {status}"
        );
        if status == "success" && self.replay.is_none() {
            self.write_call(id, &block["toolCall"]["value"])?;
        }
        if !self.enabled() {
            return Ok(());
        }
        let facts = ToolResultFacts {
            tool_use_id: Some(id.into()),
            ..Default::default()
        };
        let serialized = block.to_string();
        insert_session_event(
            self.conn,
            &EventRow {
                source: ID,
                session_id: &self.session.id,
                message_id: self.mid,
                event_uid: uid,
                ts_ms: self.message.timestamp,
                role: &self.message.role,
                kind: "tool_use",
                text: Some(&serialized),
                raw_kind: Some("messages.content_json"),
                tool_result_facts: Some(&facts),
                ..Default::default()
            },
        )?;
        self.note_event(uid);
        Ok(())
    }

    fn write_call(&mut self, id: &str, value: &Value) -> Result<()> {
        let name = value["name"]
            .as_str()
            .filter(|s| !s.is_empty())
            .context("Goose tool call missing name")?;
        let args = value
            .get("arguments")
            .cloned()
            .unwrap_or(Value::Null)
            .to_string();
        ensure!(
            self.keys.tools.insert(id.into()),
            "duplicate Goose tool request id {id}"
        );
        insert_tool_call(
            self.conn,
            &ToolCallRef {
                source: ID,
                session_id: &self.session.id,
                message_id: self.mid,
                tool_use_id: id,
                ts_ms: self.message.timestamp,
                ..Default::default()
            },
            name,
            None,
            &args,
            None,
        )
    }

    fn result(&mut self, uid: &str, block: &Value) -> Result<()> {
        let id = block["id"]
            .as_str()
            .filter(|s| !s.is_empty())
            .context("Goose tool response missing id")?;
        let status = block["toolResult"]["status"]
            .as_str()
            .context("Goose tool result missing status")?;
        ensure!(
            ["success", "error"].contains(&status),
            "unsupported Goose tool result status {status}"
        );
        let text = result_text(block, status)?;
        let errored = status == "error" || block["toolResult"]["value"]["isError"] == true;
        let native_content = &block["toolResult"]["value"]["content"];
        let mixed_content = native_content
            .as_array()
            .is_some_and(|content| content.iter().any(|item| item["type"] != "text"));
        let (call_index, event_index) = self.indexer.next(id);
        if !self.enabled() {
            return Ok(());
        }
        let measured = if mixed_content {
            native_content.clone()
        } else {
            Value::String(text.clone())
        };
        let mut facts =
            ToolResultFacts::from_payload(&measured).with_ordering(call_index, event_index);
        facts.tool_use_id = Some(id.into());
        facts.event_source = Some("tool_result".into());
        facts.result_status = Some(if errored { "errored" } else { "completed" }.into());
        if errored {
            facts.error_signal = Some("tool_status".into());
        }
        if self.replay.is_none() {
            set_tool_call_error(self.conn, ID, &self.session.id, id, errored)?;
            self.keys.results.insert(id.into());
            if mixed_content {
                // Search text is only a projection of mixed native content.
                // Keep the complete block for public reads and exports.
                marker(
                    self.conn,
                    self.session,
                    GooseMarker {
                        uid,
                        message: Some(self.mid),
                        ts: self.message.timestamp,
                        kind: "tool_result_block",
                        payload: block,
                    },
                    self.keys,
                )?;
            }
        }
        insert_session_event(
            self.conn,
            &EventRow {
                source: ID,
                session_id: &self.session.id,
                message_id: self.mid,
                event_uid: uid,
                ts_ms: self.message.timestamp,
                role: "tool_result",
                kind: "tool_result",
                text: Some(&text),
                raw_kind: Some("messages.content_json"),
                tool_result_facts: Some(&facts),
                ..Default::default()
            },
        )?;
        self.note_event(uid);
        Ok(())
    }

    fn other(&mut self, uid: &str, block: &Value) -> Result<()> {
        if self.replay.is_some() {
            return Ok(());
        }
        marker(
            self.conn,
            self.session,
            GooseMarker {
                uid,
                message: Some(self.mid),
                ts: self.message.timestamp,
                kind: "content_block",
                payload: block,
            },
            self.keys,
        )
    }
}

fn result_text(block: &Value, status: &str) -> Result<String> {
    if status == "error" {
        return Ok(block["toolResult"]["error"]
            .as_str()
            .context("Goose tool error missing error")?
            .into());
    }
    let content = block["toolResult"]["value"]["content"]
        .as_array()
        .context("Goose tool result missing content")?;
    let visible = content
        .iter()
        .filter_map(|item| item["text"].as_str())
        .collect::<Vec<_>>()
        .join("\n");
    Ok(if visible.is_empty() {
        Value::Array(content.clone()).to_string()
    } else {
        visible
    })
}
