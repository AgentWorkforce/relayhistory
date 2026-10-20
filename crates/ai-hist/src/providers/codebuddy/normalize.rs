use super::*;
use crate::ingest::{
    insert_session_event, insert_tool_call, set_tool_call_error, upsert_session_rebuilt, EventRow,
    RequestIdentity, SessionCatalogRow, ToolCallRef, ToolResultFacts,
};
use crate::providers::file_history::{self, Keys};
use crate::{insert_history, insert_session_marker, HistoryEntry, NewSessionMarker};
pub(super) fn check_identity(conn: &Connection, sid: &str, path: &Path) -> Result<()> {
    file_history::check_identity(conn, ID, "CODEBUDDY", sid, path)
}
pub(super) fn write(conn: &Connection, payload: &Value) -> Result<usize> {
    let sid = format::string(payload, "sid")?;
    let path = Path::new(format::string(payload, "path")?);
    check_identity(conn, sid, path)?;
    let rows = payload["rows"]
        .as_array()
        .context("Codebuddy rows missing")?;
    let active = payload["active"]
        .as_array()
        .context("Codebuddy active lane missing")?;
    let complete = payload["partial"] == false;
    // A rewritten prefix plus an incomplete tail must not replace selected
    // old rows while leaving the old suffix live. Publish the replacement
    // atomically only when complete; a first capture may expose its prefix.
    if !complete && has_evidence(conn, sid)? {
        return Ok(0);
    }
    let mut keys = Keys::default();
    let mut inserted = 0;
    let mut seen_usage = std::collections::BTreeSet::new();
    for (index, row) in rows.iter().enumerate() {
        marker(conn, sid, row, index, &mut keys)?;
    }
    let mut occurrences = std::collections::BTreeMap::new();
    for row in active {
        inserted += event(
            conn,
            sid,
            row,
            &mut occurrences,
            &mut seen_usage,
            &mut keys,
            None,
        )?;
    }
    if complete {
        file_history::retire(conn, ID, sid, &keys)?;
        reconcile_order(conn, sid, active, &mut keys)?;
        file_history::retire_locator_sessions(conn, ID, path, Some(sid))?;
    }
    catalog(conn, sid, path, active, rows, complete)?;
    if complete {
        title::write(conn, sid, rows)?;
    }
    Ok(inserted)
}
fn reconcile_order(conn: &Connection, sid: &str, active: &[Value], keys: &mut Keys) -> Result<()> {
    let replay = file_history::reconcile_event_order(conn, ID, sid, &ordered_events(active)?)?;
    if !replay.is_empty() {
        let mut occurrences = std::collections::BTreeMap::new();
        let mut seen_usage = std::collections::BTreeSet::new();
        for row in active {
            event(
                conn,
                sid,
                row,
                &mut occurrences,
                &mut seen_usage,
                keys,
                Some(&replay),
            )?;
        }
    }
    Ok(())
}
fn catalog(
    conn: &Connection,
    sid: &str,
    path: &Path,
    rows: &[Value],
    raw: &[Value],
    complete: bool,
) -> Result<()> {
    let times = raw
        .iter()
        .filter_map(|r| r["timestamp"].as_i64())
        .collect::<Vec<_>>();
    let last = rows
        .iter()
        .rev()
        .find(|r| format::human_message(r) && r["role"] == "assistant")
        .map(format::text);
    let writer = if complete {
        upsert_session_rebuilt
    } else {
        crate::ingest::upsert_session
    };
    writer(
        conn,
        &SessionCatalogRow {
            source: ID,
            session_id: sid,
            cwd: rows.iter().find_map(|r| r["cwd"].as_str()),
            git_branch: None,
            first_ts: *times
                .iter()
                .min()
                .context("Codebuddy has no recorded time")?,
            last_ts: *times
                .iter()
                .max()
                .context("Codebuddy has no recorded time")?,
            last_assistant_text: complete.then_some(last.as_deref()).flatten(),
            raw_path: Some(&path.to_string_lossy()),
        },
    )?;
    let preview = format::first_prompt(rows);
    if complete {
        conn.execute("UPDATE sessions SET first_prompt=?3 WHERE source=?1 AND session_id=?2 AND first_prompt IS NOT ?3",rusqlite::params![ID,sid,preview])?;
    } else if preview.is_some() {
        conn.execute("UPDATE sessions SET first_prompt=?3 WHERE source=?1 AND session_id=?2 AND first_prompt IS NULL",rusqlite::params![ID,sid,preview])?;
    }
    Ok(())
}
fn marker(conn: &Connection, sid: &str, row: &Value, index: usize, keys: &mut Keys) -> Result<()> {
    let mid = format!("record:{index}");
    let raw = serde_json::to_string(&crate::ingest::bound_marker_value(row.clone()))?;
    insert_session_marker(
        conn,
        ID,
        sid,
        &NewSessionMarker {
            marker_uid: &mid,
            kind: format::string(row, "type")?,
            ts_ms: row["timestamp"].as_i64(),
            parent_id: row["parentId"].as_str(),
            message_id: row["id"].as_str(),
            payload_json: Some(&raw),
            ..Default::default()
        },
    )?;
    keys.markers.insert(mid);
    Ok(())
}
fn call(conn: &Connection, base: &EventRow<'_>, row: &Value, keys: &mut Keys) -> Result<()> {
    let id = format::string(row, "callId")?;
    let name = format::string(row, "name")?;
    let args = format::string(row, "arguments")?;
    let parsed: Value =
        serde_json::from_str(args).context("CODEBUDDY_INVALID_RECORD: invalid tool arguments")?;
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
        parsed["file_path"].as_str(),
        args,
        None,
    )?;
    let facts = ToolResultFacts {
        tool_use_id: Some(id.into()),
        ..Default::default()
    };
    insert_session_event(
        conn,
        &EventRow {
            kind: "tool_use",
            text: Some(args),
            tool_result_facts: Some(&facts),
            ..*base
        },
    )?;
    keys.tools.insert(id.into());
    keys.events.insert(base.event_uid.into());
    Ok(())
}
fn result(conn: &Connection, base: &EventRow<'_>, row: &Value, keys: &mut Keys) -> Result<()> {
    let id = format::string(row, "callId")?;
    let inline = row["providerData"]["toolResult"]["content"].as_str();
    let text = inline
        .or_else(|| row["output"]["text"].as_str())
        .context("CODEBUDDY_UNSUPPORTED_RESULT: no native text result")?;
    // Inline content is authoritative, including literal persisted-output tags.
    // Only the fallback preview is a sidecar reference; never follow its path.
    anyhow::ensure!(
        inline.is_some() || !text.starts_with("<persisted-output>"),
        "CODEBUDDY_UNSUPPORTED_RESULT: sidecar-only output"
    );
    let native_error = row["providerData"]["toolResult"]["rawResponse"]["is_error"] == true;
    let error = native_error || format::truthy(&row["providerData"]["toolResult"]["error"]);
    let mut facts = ToolResultFacts::from_payload(&Value::String(text.into()));
    facts.tool_use_id = Some(id.into());
    facts.result_status = if error {
        Some("errored".into())
    } else {
        (row["status"] == "completed").then(|| "completed".into())
    };
    facts.error_signal = if native_error {
        Some(crate::ingest::ERROR_SIGNAL_TOOL_RESULT.into())
    } else {
        error.then(|| crate::ingest::ERROR_SIGNAL_TOOL_STATUS.into())
    };
    insert_session_event(
        conn,
        &EventRow {
            kind: "tool_result",
            text: Some(text),
            tool_result_facts: Some(&facts),
            ..*base
        },
    )?;
    if error || row["status"] == "completed" {
        set_tool_call_error(conn, ID, base.session_id, id, error)?;
        keys.results.insert(id.into());
    }
    keys.events.insert(base.event_uid.into());
    Ok(())
}

fn event(
    conn: &Connection,
    sid: &str,
    row: &Value,
    occurrences: &mut std::collections::BTreeMap<String, usize>,
    seen_usage: &mut std::collections::BTreeSet<String>,
    keys: &mut Keys,
    only: Option<&std::collections::BTreeSet<i64>>,
) -> Result<usize> {
    let mut inserted = 0;

    crate::ingest::check_capture_cancelled()?;
    let kind = format::string(row, "type")?;
    if !matches!(kind, "message" | "function_call" | "function_call_result") {
        return Ok(0);
    }
    let mid = format::string(row, "id")?;
    let ts = format::time(row)?;
    let occurrence = occurrences.entry(mid.to_owned()).or_default();
    let uid = format!("{mid}:{}", *occurrence);
    *occurrence += 1;
    let role = match kind {
        "function_call" => "assistant",
        "function_call_result" => "tool_result",
        _ => format::string(row, "role")?,
    };
    let usage = &row["providerData"]["rawUsage"];
    let provider_message_id = row["providerData"]["messageId"]
        .as_str()
        .filter(|id| !id.trim().is_empty());
    let request = provider_message_id.unwrap_or(mid);
    let token = (!usage.is_null() && seen_usage.insert(request.to_owned()))
        .then(|| serde_json::to_string(usage))
        .transpose()?;
    if only.is_some_and(|times| !times.contains(&ts)) {
        return Ok(0);
    }
    let base = EventRow {
        source: ID,
        session_id: sid,
        message_id: mid,
        event_uid: &uid,
        ts_ms: ts,
        role,
        parent_id: row["parentId"].as_str(),
        cwd: row["cwd"].as_str(),
        project: row["cwd"].as_str(),
        model: row["providerData"]["model"].as_str(),
        token_json: token.as_deref(),
        identity: RequestIdentity {
            provider_message_id,
            ..Default::default()
        },
        raw_kind: Some(kind),
        ..Default::default()
    };
    match kind {
        "message" => {
            let text = format::text(row);
            if !format::human_message(row) {
                return Ok(0);
            }
            insert_session_event(
                conn,
                &EventRow {
                    kind: "text",
                    text: (!text.is_empty()).then_some(text.as_str()),
                    ..base
                },
            )?;
            keys.events.insert(uid);
            if role == "user" && !text.is_empty() {
                let hash = crate::prompt_hash(&text);
                keys.prompts.insert(format!("{ts}:{hash}"));
                inserted += insert_history(
                    conn,
                    &HistoryEntry {
                        id: 0,
                        source: ID.into(),
                        session_id: Some(sid.into()),
                        project: row["cwd"].as_str().map(str::to_owned),
                        prompt: text,
                        prompt_hash: Some(hash),
                        timestamp_ms: ts,
                    },
                )?;
            }
        }
        "function_call" => call(conn, &base, row, keys)?,
        "function_call_result" => result(conn, &base, row, keys)?,
        _ => unreachable!(),
    }
    Ok(inserted)
}

pub(super) fn has_evidence(conn: &Connection, sid: &str) -> Result<bool> {
    Ok(conn.query_row("SELECT EXISTS(SELECT 1 FROM session_events WHERE source=?1 AND session_id=?2) OR EXISTS(SELECT 1 FROM session_markers WHERE source=?1 AND session_id=?2)",[ID,sid],|r|r.get(0))?)
}

fn ordered_events(rows: &[Value]) -> Result<Vec<(i64, String)>> {
    let mut occurrences = std::collections::BTreeMap::new();
    let mut desired = Vec::new();
    for row in rows {
        let kind = format::string(row, "type")?;
        if !matches!(kind, "message" | "function_call" | "function_call_result") {
            continue;
        }
        let mid = format::string(row, "id")?;
        let occurrence = occurrences.entry(mid).or_insert(0usize);
        let uid = format!("{mid}:{occurrence}");
        *occurrence += 1;
        if kind != "message" || format::human_message(row) {
            desired.push((format::time(row)?, uid));
        }
    }
    Ok(desired)
}
