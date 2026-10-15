//! Native paginated item mapping.
use super::*;

pub(super) fn normalize_item(
    conn: &Connection,
    t: &Thread,
    item: &Item,
    result_indexer: &mut ToolResultIndexer,
    include_related: bool,
    history_ids: &mut Vec<i64>,
) -> Result<usize> {
    match item.kind.as_str() {
        "userMessage" => user(conn, t, item, history_ids),
        "agentMessage" => agent(conn, t, item),
        "reasoning" => reasoning(conn, t, item),
        "commandExecution" | "mcpToolCall" | "collabAgentToolCall" | "webSearch" => {
            tool(conn, t, item, result_indexer, include_related)
        }
        "fileChange" => file_change(conn, t, item),
        "contextCompaction" => mark(conn, t, item, "compaction"),
        "subAgentActivity" => subagent(conn, t, item, include_related),
        "imageView" | "sleep" | "hookPrompt" => mark(conn, t, item, &item.kind),
        _ => mark(conn, t, item, "unknown"),
    }
}

fn user(conn: &Connection, t: &Thread, item: &Item, history_ids: &mut Vec<i64>) -> Result<usize> {
    let text = text_blocks(&item.value["content"]);
    let control = crate::ingest::control::codex_text_control_kind(&text);
    let recorded = if text.is_empty() {
        false
    } else {
        event(conn, t, item, &item.id, ("user", "text"), Some(&text), None)?
    };
    user_images(conn, t, item)?;
    if t.is_child() || !recorded || control.is_some() {
        return Ok(0);
    }
    let hash = crate::prompt_hash(&text);
    let inserted = crate::insert_history(
        conn,
        &HistoryEntry {
            id: 0,
            source: "codex".into(),
            session_id: Some(t.id.clone()),
            project: t.cwd.clone(),
            prompt: text,
            prompt_hash: Some(hash),
            timestamp_ms: item.ts,
        },
    )?;
    if inserted > 0 {
        history_ids.push(conn.last_insert_rowid());
    }
    Ok(inserted)
}

fn user_images(conn: &Connection, t: &Thread, item: &Item) -> Result<()> {
    let Some(content) = item.value["content"].as_array() else {
        return Ok(());
    };
    for (index, block) in content.iter().enumerate() {
        if block["type"] != "input_image" {
            continue;
        }
        let markdown = block["image_url"]
            .as_str()
            .and_then(crate::ingest::image_markdown)
            .unwrap_or_else(|| {
                "[Image unavailable: the recording has no supported embedded image under 1 MiB.]"
                    .into()
            });
        let rollout_has_image = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM session_events WHERE source='codex' AND session_id=?1 \
             AND message_id=?2 AND raw_kind='input_image' AND text=?3)",
            params![t.id, item.id, markdown],
            |row| row.get::<_, bool>(0),
        )?;
        if rollout_has_image {
            continue;
        }
        let overlap_uid = format!("sqlite:overlap:{}:input_image:{index}", item.id);
        let mapped: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM session_markers WHERE source='codex' AND session_id=?1 AND marker_uid=?2)",
            params![t.id, overlap_uid],
            |row| row.get(0),
        )?;
        if mapped {
            continue;
        }
        let rollout_uid: Option<String> = conn.query_row(
            "SELECT event_uid FROM session_events WHERE source='codex' AND session_id=?1 \
             AND role='user' AND kind='text' AND raw_kind='input_image' AND text=?2 \
             AND ABS(ts_ms - ?3) <= 5000 \
             AND NOT EXISTS (SELECT 1 FROM session_markers AS marker \
               WHERE marker.source='codex' AND marker.session_id=?1 \
               AND marker.marker_uid LIKE 'sqlite:overlap:%' \
               AND json_extract(marker.payload_json, '$.rollout_event_uid')=session_events.event_uid) \
             ORDER BY ABS(ts_ms - ?3), id LIMIT 1",
            params![t.id, markdown, item.ts],
            |row| row.get(0),
        ).optional()?;
        if let Some(rollout_uid) = rollout_uid {
            marker(
                conn,
                t,
                &overlap_uid,
                item.ts,
                "rollout_overlap",
                &json!({"rollout_event_uid":rollout_uid}),
                item.turn_id.as_deref(),
            )?;
            continue;
        }
        crate::ingest::insert_session_event(
            conn,
            &EventRow {
                source: "codex",
                session_id: &t.id,
                project: t.cwd.as_deref(),
                cwd: t.cwd.as_deref(),
                git_branch: t.branch.as_deref(),
                message_id: &item.id,
                event_uid: &format!("{}:input_image:{index}", item.id),
                ts_ms: item.ts,
                role: "user",
                kind: "text",
                text: Some(&markdown),
                raw_kind: Some("input_image"),
                raw_facts: RawMessageFacts {
                    turn_id: item.turn_id.as_deref(),
                    ..Default::default()
                },
                ..Default::default()
            },
        )?;
    }
    Ok(())
}

fn agent(conn: &Connection, t: &Thread, item: &Item) -> Result<usize> {
    let value = &item.value;
    event(
        conn,
        t,
        item,
        &item.id,
        ("assistant", "text"),
        value["text"].as_str(),
        None,
    )?;
    if let Some(phase) = value["phase"].as_str() {
        marker(
            conn,
            t,
            &format!("{}:phase", item.id),
            item.ts,
            "message_phase",
            &json!({"phase":phase}),
            item.turn_id.as_deref(),
        )?;
    }
    Ok(0)
}

fn reasoning(conn: &Connection, t: &Thread, item: &Item) -> Result<usize> {
    let summary = text_blocks(&item.value["summary"]);
    let text = if summary.is_empty() {
        text_blocks(&item.value["content"])
    } else {
        summary
    };
    if text.is_empty() {
        marker(
            conn,
            t,
            &item.id,
            item.ts,
            "reasoning",
            &item.value,
            item.turn_id.as_deref(),
        )?;
    } else {
        event(
            conn,
            t,
            item,
            &item.id,
            ("assistant", "thinking"),
            Some(&text),
            None,
        )?;
    }
    Ok(0)
}

fn tool_name(item: &Item) -> String {
    let v = &item.value;
    match item.kind.as_str() {
        "commandExecution" => "shell".into(),
        "mcpToolCall" => format!(
            "mcp:{}.{}",
            v["server"].as_str().unwrap_or("unknown"),
            v["tool"].as_str().unwrap_or("unknown")
        ),
        "collabAgentToolCall" => v["tool"].as_str().unwrap_or("collab_agent").into(),
        _ => "web_search".into(),
    }
}

fn tool(
    conn: &Connection,
    t: &Thread,
    item: &Item,
    result_indexer: &mut ToolResultIndexer,
    include_related: bool,
) -> Result<usize> {
    let v = &item.value;
    let target = v["command"].as_str().or_else(|| v["query"].as_str());
    let args = v.get("arguments").unwrap_or(v);
    let error = v["error"]
        .as_bool()
        .or_else(|| v["exitCode"].as_i64().map(|code| code != 0))
        .or_else(|| match v["status"].as_str() {
            Some("failed") => Some(true),
            Some("completed") => Some(false),
            _ => None,
        });
    if event(
        conn,
        t,
        item,
        &item.id,
        ("assistant", "tool_use"),
        target,
        None,
    )? {
        crate::ingest::insert_tool_call(
            conn,
            &ToolCallRef {
                source: "codex",
                session_id: &t.id,
                message_id: &item.id,
                tool_use_id: &item.id,
                ts_ms: item.ts,
                ..Default::default()
            },
            &tool_name(item),
            target,
            &args.to_string(),
            error,
        )?;
    }
    if let Some(result) = v.get("aggregatedOutput").or_else(|| v.get("result")) {
        let (call_index, event_index) = result_indexer.next(&item.id);
        let mut facts =
            ToolResultFacts::from_payload(result).with_ordering(call_index, event_index);
        facts.tool_use_id = Some(item.id.clone());
        facts.event_source = Some(crate::ingest::EVENT_SOURCE_FUNCTION_CALL_OUTPUT.into());
        facts.result_status = Some(
            if error == Some(true) {
                crate::ingest::STATUS_ERRORED
            } else if error == Some(false) || v["status"].as_str() == Some("completed") {
                crate::ingest::STATUS_COMPLETED
            } else {
                crate::ingest::STATUS_UNKNOWN
            }
            .into(),
        );
        if v["exitCode"].as_i64().is_some_and(|code| code != 0) {
            facts.error_signal = Some(crate::ingest::ERROR_SIGNAL_EXIT_CODE.into());
        } else if v["error"].as_bool() == Some(true) {
            facts.error_signal = Some(crate::ingest::ERROR_SIGNAL_MCP_ERR.into());
        } else if v["status"].as_str() == Some("failed") {
            facts.error_signal = Some(crate::ingest::ERROR_SIGNAL_TOOL_STATUS.into());
        }
        let text = result
            .as_str()
            .map(str::to_string)
            .unwrap_or_else(|| result.to_string());
        event(
            conn,
            t,
            item,
            &format!("{}:result", item.id),
            ("tool_result", "tool_result"),
            Some(&text),
            Some(&facts),
        )?;
    }
    if include_related {
        collab_children(conn, t, item)?;
    }
    Ok(0)
}

fn collab_children(conn: &Connection, t: &Thread, item: &Item) -> Result<()> {
    if item.kind != "collabAgentToolCall" {
        return Ok(());
    }
    if let Some(receivers) = item.value["receiverThreadIds"].as_array() {
        for child in receivers.iter().filter_map(Value::as_str) {
            child_edge(conn, t, child, item)?;
        }
    }
    Ok(())
}

fn file_change(conn: &Connection, t: &Thread, item: &Item) -> Result<usize> {
    if !event(
        conn,
        t,
        item,
        &item.id,
        ("assistant", "tool_use"),
        Some("file changes"),
        None,
    )? {
        return Ok(0);
    }
    if let Some(changes) = item.value["changes"].as_array() {
        for (index, change) in changes.iter().enumerate() {
            let Some(path) = change["path"].as_str() else {
                continue;
            };
            let edit_id = format!("{}:{index}", item.id);
            crate::ingest::upsert_file_edit_from_call(
                conn,
                &ToolCallRef {
                    source: "codex",
                    session_id: &t.id,
                    message_id: &item.id,
                    tool_use_id: &edit_id,
                    ts_ms: item.ts,
                    git_branch: t.branch.as_deref(),
                    cwd: t.cwd.as_deref(),
                },
                path,
                "fileChange",
            )?;
            conn.execute("UPDATE file_edits SET structured_patch_json=?1 WHERE source='codex' AND session_id=?2 AND tool_use_id=?3", params![change.to_string(),t.id,edit_id])?;
        }
    }
    Ok(0)
}

fn mark(conn: &Connection, t: &Thread, item: &Item, kind: &str) -> Result<usize> {
    marker(
        conn,
        t,
        &item.id,
        item.ts,
        kind,
        &item.value,
        item.turn_id.as_deref(),
    )?;
    Ok(0)
}

fn subagent(conn: &Connection, t: &Thread, item: &Item, include_related: bool) -> Result<usize> {
    mark(conn, t, item, "subagent_activity")?;
    if let Some(child) = item.value["agentThreadId"]
        .as_str()
        .filter(|_| include_related)
    {
        child_edge(conn, t, child, item)?;
    }
    Ok(0)
}
