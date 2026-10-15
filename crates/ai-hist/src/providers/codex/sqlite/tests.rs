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
