use super::*;

fn install_fixture(codex: &Path, stem: &str) {
    std::fs::create_dir_all(codex).unwrap();
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/codex/sqlite");
    for (script, database) in [
        (format!("{stem}-state.sql"), "state_5.sqlite"),
        (format!("{stem}-history.sql"), "thread_history_1.sqlite"),
    ] {
        let sql = std::fs::read_to_string(fixtures.join(script))
            .unwrap()
            .replace("{{CODEX_HOME}}", &codex.to_string_lossy());
        Connection::open(codex.join(database))
            .unwrap()
            .execute_batch(&sql)
            .unwrap();
    }
}

#[test]
fn guardian_requires_a_parent_before_it_is_hidden_from_the_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let codex = dir.path().join(".codex");
    install_fixture(&codex, "subagent-edge");
    let state = Connection::open(codex.join("state_5.sqlite")).unwrap();
    state
        .execute(
            "UPDATE threads SET source=?1, thread_source='guardian_review' WHERE id='sqlite-child'",
            [r#"{"subagent":{"other":"guardian"}}"#],
        )
        .unwrap();
    let mut roots =
        crate::ProviderRoots::from_home(dir.path().to_path_buf(), dir.path().join("opencode.db"));
    roots.codex = codex.clone();
    let (candidates, _) = enumerate(&codex, &roots).unwrap();
    assert!(candidates
        .iter()
        .any(|c| c.session_id.as_deref() == Some("sqlite-child")));

    state
        .execute(
            "UPDATE threads SET source=?1 WHERE id='sqlite-child'",
            [r#"{"subagent":{"thread_spawn":{"parent_thread_id":"sqlite-parent"}}}"#],
        )
        .unwrap();
    let (candidates, _) = enumerate(&codex, &roots).unwrap();
    assert!(!candidates
        .iter()
        .any(|c| c.session_id.as_deref() == Some("sqlite-child")));
}

#[test]
fn related_capture_keeps_supported_children_when_other_edges_are_unavailable() {
    let dir = tempfile::tempdir().unwrap();
    let codex = dir.path().join(".codex");
    install_fixture(&codex, "subagent-edge");
    let state = Connection::open(codex.join("state_5.sqlite")).unwrap();
    let history = Connection::open(codex.join("thread_history_1.sqlite")).unwrap();
    state
        .execute_batch(
            "INSERT INTO thread_spawn_edges VALUES ('sqlite-parent','missing-child','running');
         INSERT INTO threads (id,created_at_ms,updated_at_ms,source,archived,history_mode)
         VALUES ('item-child',1776643200000,1776643300000,'cli',0,'paginated');",
        )
        .unwrap();
    history
        .execute_batch("INSERT INTO thread_history_projection_state VALUES ('item-child',1,0);")
        .unwrap();
    history.execute(
        "INSERT INTO thread_items VALUES ('sqlite-parent','turn-1','item-link',50,1776643250000,'subAgentActivity',?1)",
        [json!({"type":"subAgentActivity","id":"item-link","agentThreadId":"item-child"}).to_string()],
    ).unwrap();
    history.execute(
        "INSERT INTO thread_items VALUES ('item-child','turn-1','item-answer',0,1776643251000,'agentMessage',?1)",
        [json!({"type":"agentMessage","id":"item-answer","text":"Item-linked answer"}).to_string()],
    ).unwrap();
    let rollout = codex.join("sessions/2026/04/20/rollout-legacy-child.jsonl");
    std::fs::create_dir_all(rollout.parent().unwrap()).unwrap();
    let first = json!({"timestamp":"2026-04-20T00:00:00Z","type":"session_meta","payload":{"id":"legacy-child","cwd":"/work/app","source":{"subagent":{"thread_spawn":{"parent_thread_id":"sqlite-parent"}}}}}).to_string()
        + "\n" + &json!({"timestamp":"2026-04-20T00:00:01Z","type":"event_msg","payload":{"type":"agent_message","message":"alpha"}}).to_string() + "\n";
    std::fs::write(&rollout, &first).unwrap();
    state.execute(
        "INSERT INTO threads (id,rollout_path,created_at_ms,updated_at_ms,source,archived,history_mode) VALUES ('legacy-child',?1,1776643200000,1776643300000,'cli',0,'legacy')",
        [rollout.to_string_lossy().as_ref()],
    ).unwrap();
    state
        .execute(
            "INSERT INTO thread_spawn_edges VALUES ('sqlite-parent','legacy-child','completed')",
            [],
        )
        .unwrap();
    let guardian = codex.join("sessions/2026/04/20/rollout-guardian.jsonl");
    std::fs::write(&guardian, json!({"timestamp":"2026-04-20T00:00:00Z","type":"session_meta","payload":{"id":"guardian","cwd":"/work/app","thread_source":"guardian_review"}}).to_string() + "\n").unwrap();
    state.execute(
        "INSERT INTO threads (id,rollout_path,created_at_ms,updated_at_ms,source,archived,history_mode) VALUES ('guardian',?1,1776643200000,1776643300000,'cli',0,'legacy')",
        [guardian.to_string_lossy().as_ref()],
    ).unwrap();
    state
        .execute(
            "INSERT INTO thread_spawn_edges VALUES ('sqlite-parent','guardian','completed')",
            [],
        )
        .unwrap();
    let mut roots =
        crate::ProviderRoots::from_home(dir.path().to_path_buf(), dir.path().join("opencode.db"));
    roots.codex = codex.clone();
    let state_path = codex.join("state_5.sqlite");
    let first_capture = capture_related(&roots, &state_path, "sqlite-parent").unwrap();
    let captured: Capture = serde_json::from_value(first_capture.payload).unwrap();
    assert!(
        captured.related_incomplete,
        "the missing edge remains incomplete"
    );
    assert!(captured
        .related
        .iter()
        .any(|child| child.thread.id == "item-child" && !child.items.is_empty()));
    assert!(captured
        .related_rollouts
        .iter()
        .any(|child| child.id == "legacy-child"));
    assert!(captured
        .related_rollouts
        .iter()
        .any(|child| child.id == "guardian"));

    state
        .execute(
            "DELETE FROM thread_spawn_edges WHERE child_thread_id='missing-child'",
            [],
        )
        .unwrap();
    let guardian_only: Capture = serde_json::from_value(
        capture_related(&roots, &state_path, "sqlite-parent")
            .unwrap()
            .payload,
    )
    .unwrap();
    assert!(
        !guardian_only.related_incomplete,
        "a valid standalone guardian rollout completes the explicit edge"
    );

    let modified = rollout.metadata().unwrap().modified().unwrap();
    let rewritten = first.replace("alpha", "bravo");
    assert_eq!(rewritten.len(), first.len());
    std::fs::write(&rollout, rewritten).unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&rollout)
        .unwrap()
        .set_times(std::fs::FileTimes::new().set_modified(modified))
        .unwrap();
    let second_capture = capture_related(&roots, &state_path, "sqlite-parent").unwrap();
    let second: Capture = serde_json::from_value(second_capture.payload).unwrap();
    let stamp = |capture: &Capture| {
        capture
            .related_rollouts
            .iter()
            .find(|child| child.id == "legacy-child")
            .unwrap()
            .stamp
            .clone()
    };
    assert_ne!(
        stamp(&captured),
        stamp(&second),
        "a same-size rewrite with restored mtime must change the related stamp"
    );
}

#[test]
fn image_only_sqlite_user_item_keeps_its_embedded_image() {
    let dir = tempfile::tempdir().unwrap();
    let codex = dir.path().join(".codex");
    install_fixture(&codex, "pruned-all-items");
    let image = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=";
    let value = json!({"type":"userMessage","id":"image-only","content":[{"type":"input_image","image_url":image}]});
    Connection::open(codex.join("thread_history_1.sqlite"))
        .unwrap()
        .execute(
            "INSERT INTO thread_items VALUES ('sqlite-pruned-all-items','turn-1','image-only',21,1776643221000,'userMessage',?1)",
            [value.to_string()],
        )
        .unwrap();
    let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
    let mut state = serde_json::Map::new();
    sync(
        &conn,
        &mut state,
        &codex,
        &crate::ingest::SweepRepairs::default(),
    )
    .unwrap();
    let image_text: String = conn.query_row(
        "SELECT text FROM session_events WHERE source='codex' AND session_id='sqlite-pruned-all-items' AND raw_kind='input_image'",
        [],
        |row| row.get(0),
    ).unwrap();
    assert_eq!(image_text, format!("![Attached image]({image})"));
    let empty_texts: i64 = conn.query_row(
        "SELECT count(*) FROM session_events WHERE session_id='sqlite-pruned-all-items' AND role='user' AND text=''",
        [],
        |row| row.get(0),
    ).unwrap();
    assert_eq!(empty_texts, 0);
}

#[test]
fn idless_rollout_prefix_and_rewritten_projection_do_not_leave_duplicate_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let codex = dir.path().join(".codex");
    install_fixture(&codex, "pruned-all-items");
    let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
    let history = Connection::open(codex.join("thread_history_1.sqlite")).unwrap();
    history.execute(
        "INSERT INTO thread_items VALUES ('sqlite-pruned-all-items','turn-1','repeated-answer',22,1776643201000,'agentMessage',?1)",
        [json!({"type":"agentMessage","id":"repeated-answer","text":"I inspected the project and fixed it."}).to_string()],
    ).unwrap();
    crate::ingest::insert_session_event(
        &conn,
        &EventRow {
            source: "codex",
            session_id: "sqlite-pruned-all-items",
            message_id: "1:agent_message",
            event_uid: "1:agent_message",
            ts_ms: 1776643201000,
            role: "assistant",
            kind: "text",
            text: Some("I inspected the project and fixed it."),
            ..Default::default()
        },
    )
    .unwrap();
    let mut state = serde_json::Map::new();
    let repairs = crate::ingest::SweepRepairs::default();
    sync(&conn, &mut state, &codex, &repairs).unwrap();
    let duplicate: i64 = conn.query_row(
        "SELECT count(*) FROM session_events WHERE source='codex' AND session_id='sqlite-pruned-all-items' AND role='assistant' AND kind='text' AND text='I inspected the project and fixed it.'",
        [], |row| row.get(0),
    ).unwrap();
    assert_eq!(
        duplicate, 2,
        "one rollout row and one separate repeated reply"
    );

    history.execute("DELETE FROM thread_items WHERE thread_id='sqlite-pruned-all-items' AND item_id IN ('sqlite-sqlite-pruned-all-items-0','sqlite-sqlite-pruned-all-items-2','sqlite-sqlite-pruned-all-items-4')", []).unwrap();
    history
        .execute(
            "DELETE FROM thread_items WHERE item_id='repeated-answer'",
            [],
        )
        .unwrap();
    Connection::open(codex.join("state_5.sqlite"))
        .unwrap()
        .execute(
            "UPDATE threads SET updated_at_ms=updated_at_ms+1 WHERE id='sqlite-pruned-all-items'",
            [],
        )
        .unwrap();
    sync(&conn, &mut state, &codex, &repairs).unwrap();
    let count = |table: &str, predicate: &str| -> i64 {
        conn.query_row(
            &format!("SELECT count(*) FROM {table} WHERE source='codex' AND session_id='sqlite-pruned-all-items' AND {predicate}"),
            [], |row| row.get(0),
        ).unwrap()
    };
    assert_eq!(count("history", "prompt='Please inspect the project'"), 0);
    assert_eq!(
        count(
            "tool_calls",
            "message_id='sqlite-sqlite-pruned-all-items-2'"
        ),
        0
    );
    assert_eq!(
        count(
            "file_edits",
            "message_id='sqlite-sqlite-pruned-all-items-4'"
        ),
        0
    );
    assert_eq!(
        count(
            "session_events",
            "role='assistant' AND kind='text' AND text='I inspected the project and fixed it.'"
        ),
        1
    );
}

#[test]
fn an_incomplete_sqlite_pair_is_skipped() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("state_5.sqlite"), b"half pair").unwrap();
    let selection = database_selection(dir.path()).unwrap();
    assert!(selection.pair.is_none());
    assert!(selection.diagnostic.unwrap().contains("incomplete"));
}

#[test]
fn a_pruned_rollout_is_read_again_after_it_was_restored() {
    let dir = tempfile::tempdir().unwrap();
    let codex = dir.path().join(".codex");
    std::fs::create_dir_all(&codex).unwrap();
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/codex/sqlite");
    for (script, database) in [
        ("rollout-primary-state.sql", "state_5.sqlite"),
        ("rollout-primary-history.sql", "thread_history_1.sqlite"),
    ] {
        let sql = std::fs::read_to_string(fixtures.join(script))
            .unwrap()
            .replace("{{CODEX_HOME}}", &codex.to_string_lossy());
        Connection::open(codex.join(database))
            .unwrap()
            .execute_batch(&sql)
            .unwrap();
    }
    let rollout = codex.join("sessions/2026/04/20/rollout-rollout-primary.jsonl");
    std::fs::create_dir_all(rollout.parent().unwrap()).unwrap();
    std::fs::write(&rollout, "complete rollout").unwrap();
    let home = dir.path().to_path_buf();
    let mut roots = crate::ProviderRoots::from_home(home, dir.path().join("opencode.db"));
    roots.codex = codex.clone();
    assert!(enumerate(&codex, &roots).unwrap().0.is_empty());

    let conn = crate::open_db(&dir.path().join("ai-history.db")).unwrap();
    let mut state = serde_json::Map::new();
    let repairs = crate::ingest::SweepRepairs::default();
    assert!(sync(&conn, &mut state, &codex, &repairs)
        .unwrap()
        .1
        .is_empty());

    std::fs::remove_file(&rollout).unwrap();
    assert_eq!(
        sync(&conn, &mut state, &codex, &repairs).unwrap().1,
        vec!["sqlite-rollout-primary".to_string()]
    );
    std::fs::write(&rollout, "complete rollout").unwrap();
    assert!(sync(&conn, &mut state, &codex, &repairs)
        .unwrap()
        .1
        .is_empty());

    std::fs::remove_file(&rollout).unwrap();
    assert_eq!(
        sync(&conn, &mut state, &codex, &repairs).unwrap().1,
        vec!["sqlite-rollout-primary".to_string()]
    );
}
