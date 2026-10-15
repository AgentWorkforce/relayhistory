//! Session event insertion and Codex SQLite-to-rollout reconciliation.
use super::*;

pub(super) fn insert_session_event(conn: &Connection, row: &EventRow<'_>) -> Result<()> {
    sweep_session::write_event(conn, row)
}

/// Once the rollout has reached the SQLite projection's byte offset, retire
/// the provisional SQLite copy before reading the rollout from byte zero.
/// This runs once per rollout pass; the marker lookup is an indexed miss for
/// ordinary Codex sessions. The native item kinds distinguish SQLite rows
/// from rollout rows even when a rollout message has no provider id.
pub(super) fn reconcile_codex_sqlite(
    conn: &Connection,
    session_id: &str,
    rollout: &Path,
    cursor: &mut transcript_cursor::TranscriptCursorState,
) -> Result<()> {
    let offset: Option<i64> = conn
        .query_row(
            "SELECT json_extract(payload_json, '$.next_rollout_byte_offset') \
             FROM session_markers WHERE source='codex' AND session_id=?1 \
             AND marker_uid='sqlite:thread'",
            [session_id],
            |row| row.get(0),
        )
        .optional()?
        .flatten();
    let Some(offset) = offset.filter(|value| *value >= 0) else {
        return Ok(());
    };
    if !rollout
        .metadata()
        .is_ok_and(|metadata| metadata.len() >= offset as u64)
    {
        return Ok(());
    }
    const NATIVE_EVENTS: &str = "source='codex' AND session_id=?1 \
        AND raw_kind IN ('userMessage','agentMessage','reasoning','commandExecution', \
        'mcpToolCall','collabAgentToolCall','webSearch','fileChange') \
        AND (event_uid=message_id OR event_uid=message_id||':result')";
    for table in ["tool_calls", "file_edits"] {
        conn.execute(
            &format!(
                "DELETE FROM {table} WHERE source='codex' AND session_id=?1 \
                 AND message_id IN (SELECT message_id FROM session_events WHERE {NATIVE_EVENTS})"
            ),
            [session_id],
        )?;
    }
    conn.execute(
        &format!("DELETE FROM session_events WHERE {NATIVE_EVENTS}"),
        [session_id],
    )?;
    conn.execute(
        "DELETE FROM session_markers WHERE source='codex' AND session_id=?1 \
         AND subkind='sqlite'",
        [session_id],
    )?;
    *cursor = transcript_cursor::TranscriptCursorState::default();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_finished_rollout_replaces_sqlite_tools_edits_and_events_without_repeating_an_edge() {
        let dir = tempfile::tempdir().unwrap();
        let codex = dir.path().join(".codex");
        std::fs::create_dir_all(&codex).unwrap();
        let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/codex/sqlite");
        for (script, database) in [
            ("pruned-all-items-state.sql", "state_5.sqlite"),
            ("pruned-all-items-history.sql", "thread_history_1.sqlite"),
        ] {
            let sql = std::fs::read_to_string(fixtures.join(script))
                .unwrap()
                .replace("{{CODEX_HOME}}", &codex.to_string_lossy());
            Connection::open(codex.join(database))
                .unwrap()
                .execute_batch(&sql)
                .unwrap();
        }
        let rollout = codex.join("rollout.jsonl");
        let lines = [
            json!({"timestamp":"2026-04-20T00:00:00Z","type":"session_meta","payload":{"id":"sqlite-pruned-all-items","cwd":"/work/app"}}),
            json!({"timestamp":"2026-04-20T00:00:01Z","type":"response_item","payload":{"type":"function_call","name":"exec_command","call_id":"sqlite-sqlite-pruned-all-items-2","arguments":"{\"cmd\":\"printf hello\"}"}}),
            json!({"timestamp":"2026-04-20T00:00:02Z","type":"response_item","payload":{"type":"custom_tool_call","name":"apply_patch","call_id":"rollout-patch","input":"*** Begin Patch\n*** Update File: src/main.rs\n@@\n+let ready = true;\n*** End Patch\n"}}),
            json!({"timestamp":"2026-04-20T00:00:03Z","type":"event_msg","payload":{"type":"patch_apply_end","call_id":"rollout-patch","success":true,"changes":{"src/main.rs":{"unified_diff":"+let ready = true;"}}}}),
            json!({"timestamp":"2026-04-20T00:00:04Z","type":"event_msg","payload":{"type":"task_complete"}}),
        ];
        let full = lines
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        Connection::open(codex.join("state_5.sqlite"))
            .unwrap()
            .execute(
                "UPDATE threads SET rollout_path=?1 WHERE id='sqlite-pruned-all-items'",
                [rollout.to_string_lossy().as_ref()],
            )
            .unwrap();
        Connection::open(codex.join("thread_history_1.sqlite"))
            .unwrap()
            .execute(
                "UPDATE thread_history_projection_state SET next_rollout_byte_offset=?1",
                [full.len() as i64],
            )
            .unwrap();

        let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
        let mut state = serde_json::Map::new();
        crate::providers::codex::sqlite::sync(&conn, &mut state, &codex, &SweepRepairs::default())
            .unwrap();
        let count = |table: &str| -> i64 {
            conn.query_row(
                &format!("SELECT COUNT(*) FROM {table} WHERE source='codex' AND session_id='sqlite-pruned-all-items'"),
                [],
                |row| row.get(0),
            )
            .unwrap()
        };
        assert!(count("tool_calls") >= 4);
        assert_eq!(count("file_edits"), 1);

        std::fs::write(&rollout, format!("{}\n", lines[0])).unwrap();
        let meta = read_codex_session_meta(&rollout).unwrap().unwrap();
        ingest_codex_rollout(&conn, &rollout, &meta).unwrap();
        assert!(
            count("tool_calls") >= 4,
            "the short rollout keeps SQLite's tail"
        );

        std::fs::write(&rollout, full).unwrap();
        ingest_codex_rollout(&conn, &rollout, &meta).unwrap();
        assert_eq!(count("tool_calls"), 2);
        assert_eq!(count("file_edits"), 1);
        let native_events: i64 = conn.query_row(
            "SELECT COUNT(*) FROM session_events WHERE source='codex' AND session_id='sqlite-pruned-all-items' AND raw_kind IN ('userMessage','agentMessage','commandExecution','fileChange')",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(native_events, 0);
        let sqlite_markers: i64 = conn.query_row(
            "SELECT COUNT(*) FROM session_markers WHERE source='codex' AND session_id='sqlite-pruned-all-items' AND subkind='sqlite'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(sqlite_markers, 0);

        let child = codex.join("child.jsonl");
        std::fs::write(&child, r#"{"timestamp":"2026-04-20T00:00:05Z","type":"session_meta","payload":{"id":"sqlite-child","cwd":"/work/app","source":{"subagent":{"thread_spawn":{"parent_thread_id":"sqlite-pruned-all-items"}}}}}"#).unwrap();
        let child_meta = read_codex_session_meta(&child).unwrap().unwrap();
        record_codex_delegation(&conn, "sqlite-pruned-all-items", &child_meta, &child).unwrap();
        let relationships: Vec<String> = conn.prepare(
            "SELECT relationship_uid FROM session_relationships WHERE source='codex' AND parent_session_id='sqlite-pruned-all-items'"
        ).unwrap().query_map([], |row| row.get(0)).unwrap().collect::<rusqlite::Result<_>>().unwrap();
        assert_eq!(relationships, ["child:sqlite-child"]);
    }
}
