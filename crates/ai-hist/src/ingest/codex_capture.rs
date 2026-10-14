//! Completed shell commands nested inside Codex's JavaScript orchestration tool.
//! The outer `functions.exec` call has JavaScript arguments, not the shell
//! command that ran. History PR evidence needs the native command and its own
//! successful output, joined by the native execution id.

use super::*;

pub(super) fn pick_tool_target(name: &str, args: &Value) -> Option<String> {
    let obj = args.as_object()?;
    let get = |keys: &[&str]| {
        keys.iter()
            .find_map(|k| obj.get(*k).and_then(Value::as_str))
            .map(str::to_string)
    };
    match name {
        "exec_command" | "shell" | "exec" => get(&["cmd", "command"]),
        "read_file" | "write_file" => get(&["path", "file_path"]),
        _ => get(&["path", "file_path", "cmd", "command", "url", "query"]),
    }
}

pub(super) fn capture_rollout(
    conn: &Connection,
    meta: &CodexSessionMeta,
    value: &Value,
    position: (usize, i64, Option<&str>, u64),
    indexer: &mut ToolResultIndexer,
) -> Result<usize> {
    let images = images::capture_rollout(conn, meta, value, (position.0, position.1, position.2))?;
    let commands = capture_command(conn, meta, value, position, indexer)?;
    Ok(images + commands)
}

pub(super) fn is_command_execution(value: &Value) -> bool {
    value.get("type").and_then(Value::as_str) == Some("event_msg")
        && value
            .get("payload")
            .and_then(|p| p.get("type"))
            .and_then(Value::as_str)
            == Some("item_completed")
        && value
            .get("payload")
            .and_then(|p| p.get("item"))
            .and_then(|i| i.get("type"))
            .and_then(Value::as_str)
            == Some("CommandExecution")
}

/// Native argv is usually `[shell, "-lc", script]`; retain the script itself.
fn command(item: &Map<String, Value>) -> Option<String> {
    let argv = item.get("command")?.as_array()?;
    let words: Vec<&str> = argv.iter().map(Value::as_str).collect::<Option<_>>()?;
    let shell = words.first()?.rsplit('/').next()?;
    let command = if words.len() >= 3
        && matches!(shell, "sh" | "bash" | "zsh" | "dash" | "ksh" | "fish")
        && matches!(words[words.len() - 2], "-c" | "-lc" | "-ic")
    {
        words[words.len() - 1].to_string()
    } else {
        words
            .iter()
            .map(|word| quote_word(word))
            .collect::<Vec<_>>()
            .join(" ")
    };
    (!command.trim().is_empty()).then_some(command)
}

fn quote_word(word: &str) -> String {
    if !word.is_empty()
        && word
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_./:@+=,-".contains(&byte))
    {
        word.to_string()
    } else {
        format!("'{}'", word.replace('\'', "'\"'\"'"))
    }
}

fn capture_command(
    conn: &Connection,
    meta: &CodexSessionMeta,
    value: &Value,
    (index, ts_ms, turn_id, request_span): (usize, i64, Option<&str>, u64),
    indexer: &mut ToolResultIndexer,
) -> Result<usize> {
    let session_id = meta.session_id.as_str();
    let cwd = Some(meta.cwd.as_str());
    let branch = meta.git_branch.as_deref();
    if !is_command_execution(value) {
        return Ok(0);
    }
    let Some(item) = value
        .get("payload")
        .and_then(|p| p.get("item"))
        .and_then(Value::as_object)
    else {
        return Ok(0);
    };
    let Some(call_id) = item
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
    else {
        return Ok(0);
    };
    let Some(command) = command(item) else {
        return Ok(0);
    };
    let Some(output) = item.get("aggregated_output").and_then(Value::as_str) else {
        return Ok(0);
    };
    // A completed item without an exit code cannot establish that GitHub
    // accepted the command, so it cannot become PR evidence.
    let Some(exit_code) = item.get("exit_code").and_then(Value::as_i64) else {
        return Ok(0);
    };
    let call_uid = format!("{index}:native_command_call");
    let result_uid = format!("{index}:native_command_result");
    let args = json!({ "cmd": command });
    let event_text = format_tool_event_text("exec_command", Some(&command), &args);
    insert_session_event(
        conn,
        &EventRow {
            source: "codex",
            session_id,
            project: cwd,
            cwd,
            git_branch: branch,
            message_id: &call_uid,
            ts_ms,
            role: "assistant",
            kind: "tool_use",
            text: Some(&event_text),
            event_uid: &call_uid,
            raw_facts: RawMessageFacts {
                turn_id,
                request_span: Some(request_span.to_string().as_str()),
                ..RawMessageFacts::default()
            },
            ..EventRow::default()
        },
    )?;
    insert_tool_call(
        conn,
        &ToolCallRef {
            source: "codex",
            session_id,
            message_id: &call_uid,
            tool_use_id: call_id,
            ts_ms,
            ..Default::default()
        },
        "exec_command",
        Some(&command),
        &args.to_string(),
        Some(exit_code != 0),
    )?;
    let (call_index, event_index) = indexer.next(call_id);
    let mut facts = ToolResultFacts::from_payload(&Value::String(output.to_string()))
        .with_ordering(call_index, event_index);
    facts.tool_use_id = Some(call_id.to_string());
    facts.event_source = Some(EVENT_SOURCE_FUNCTION_CALL_OUTPUT.to_string());
    facts.result_status = Some(
        if exit_code == 0 {
            STATUS_COMPLETED
        } else {
            STATUS_ERRORED
        }
        .to_string(),
    );
    if exit_code != 0 {
        facts.error_signal = Some(ERROR_SIGNAL_EXIT_CODE.to_string());
    }
    insert_session_event(
        conn,
        &EventRow {
            source: "codex",
            session_id,
            project: cwd,
            cwd,
            git_branch: branch,
            message_id: &result_uid,
            ts_ms,
            role: "tool_result",
            kind: "tool_result",
            text: Some(output),
            event_uid: &result_uid,
            tool_result_facts: Some(&facts),
            raw_facts: RawMessageFacts {
                turn_id,
                ..RawMessageFacts::default()
            },
            ..EventRow::default()
        },
    )?;
    crate::store::retire_evidence_share(
        conn,
        "session_markers",
        "source = 'codex' AND session_id = ?1 AND marker_uid = ?2 AND kind = 'unknown'",
        params![session_id, format!("{index}:marker")],
        SessionLocation::Local,
    )?;
    Ok(2)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::init_db;

    #[test]
    fn native_commands_capture_pr_evidence_and_exit_status() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join("rollout-2026-10-13T22-37-02-native-pr.jsonl");
        let rows = [
            json!({"timestamp":"2026-10-13T22:37:02Z","type":"session_meta","payload":{"id":"native-pr","cwd":"/tmp/skills","timestamp":"2026-10-13T22:37:02Z"}}),
            json!({"timestamp":"2026-10-13T22:38:00Z","type":"event_msg","payload":{"type":"item_completed","item":{"type":"CommandExecution","id":"exec-push","command":["/bin/zsh","-lc","git commit -m update && git push origin feature"],"aggregated_output":"To github.com:AgentWorkforce/skills.git\n   19e3636..7dccd1d  feature -> feature\n","exit_code":0}}}),
            json!({"timestamp":"2026-10-13T22:39:00Z","type":"event_msg","payload":{"type":"item_completed","item":{"type":"CommandExecution","id":"exec-edit","command":["/bin/zsh","-lc","gh pr edit 167 --add-label mergeable"],"aggregated_output":"https://github.com/AgentWorkforce/skills/pull/167\n","exit_code":0}}}),
            json!({"timestamp":"2026-10-13T22:40:00Z","type":"event_msg","payload":{"type":"item_completed","item":{"type":"CommandExecution","id":"exec-failed","command":["/bin/zsh","-lc","gh pr create --title failed"],"aggregated_output":"https://github.com/AgentWorkforce/skills/pull/168\n","exit_code":1}}}),
            json!({"timestamp":"2026-10-13T22:41:00Z","type":"event_msg","payload":{"type":"task_complete"}}),
        ];
        fs::write(
            &path,
            rows.iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n")
                + "\n",
        )
        .unwrap();
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let meta = read_codex_session_meta(&path).unwrap().unwrap();
        ingest_codex_rollout(&conn, &path, &meta).unwrap();
        let calls = conn.prepare("SELECT tool_use_id, target, is_error FROM tool_calls WHERE source='codex' AND session_id='native-pr' ORDER BY ts_ms").unwrap()
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, i64>(2)?))).unwrap()
            .collect::<rusqlite::Result<Vec<_>>>().unwrap();
        assert_eq!(
            calls,
            vec![
                (
                    "exec-push".into(),
                    "git commit -m update && git push origin feature".into(),
                    0
                ),
                (
                    "exec-edit".into(),
                    "gh pr edit 167 --add-label mergeable".into(),
                    0
                ),
                (
                    "exec-failed".into(),
                    "gh pr create --title failed".into(),
                    1
                ),
            ]
        );
        let results = conn.prepare("SELECT tool_use_id, result_status, error_signal FROM session_events WHERE source='codex' AND session_id='native-pr' AND kind='tool_result' ORDER BY ts_ms").unwrap()
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, Option<String>>(2)?))).unwrap()
            .collect::<rusqlite::Result<Vec<_>>>().unwrap();
        assert_eq!(
            results,
            vec![
                ("exec-push".into(), "completed".into(), None),
                ("exec-edit".into(), "completed".into(), None),
                (
                    "exec-failed".into(),
                    "errored".into(),
                    Some("exit_code".into())
                ),
            ]
        );
        let (requests, _) =
            crate::session_usage::session_requests_all(&conn, "codex", "native-pr", None).unwrap();
        let ids: Vec<&str> = requests
            .iter()
            .flat_map(|request| request.tool_use_ids.iter().map(String::as_str))
            .collect();
        assert!(
            ids.contains(&"exec-edit"),
            "the native PR action belongs to its Codex request"
        );
    }

    #[test]
    fn native_command_keeps_non_shell_interpreters() {
        let item = json!({"command":["python","-c","print('hello')"]});
        assert_eq!(
            command(item.as_object().unwrap()).as_deref(),
            Some("python -c 'print('\"'\"'hello'\"'\"')'")
        );
        let shell = json!({"command":["/bin/zsh","-lc","gh pr edit 167"]});
        assert_eq!(
            command(shell.as_object().unwrap()).as_deref(),
            Some("gh pr edit 167")
        );
    }
}
