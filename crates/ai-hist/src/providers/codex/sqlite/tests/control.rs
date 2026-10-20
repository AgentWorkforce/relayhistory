use super::*;

#[test]
fn sqlite_control_wrappers_are_events_but_never_prompts() {
    let dir = tempfile::tempdir().unwrap();
    let codex = dir.path().join(".codex");
    install_fixture(&codex, "pruned-all-items");
    let wrapper = "<environment_context>\n<cwd>/work/app</cwd>\n</environment_context>";
    let state_db = Connection::open(codex.join("state_5.sqlite")).unwrap();
    state_db
        .execute(
            "UPDATE threads SET first_user_message=?1 WHERE id='sqlite-pruned-all-items'",
            [wrapper],
        )
        .unwrap();
    let history_db = Connection::open(codex.join("thread_history_1.sqlite")).unwrap();
    history_db
        .execute(
            "UPDATE thread_items SET item_json=?1 WHERE item_id='sqlite-sqlite-pruned-all-items-0'",
            [json!({"type":"userMessage","id":"sqlite-sqlite-pruned-all-items-0","content":[{"type":"text","text":wrapper}]}).to_string()],
        )
        .unwrap();
    let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
    let mut roots =
        crate::ProviderRoots::from_home(dir.path().to_path_buf(), dir.path().join("opencode.db"));
    roots.codex = codex.clone();
    let (candidates, _) = enumerate(&codex, &roots).unwrap();
    let candidate = candidates
        .iter()
        .find(|candidate| candidate.session_id.as_deref() == Some("sqlite-pruned-all-items"))
        .unwrap();
    let env = crate::discover::DiscoveryEnv::with_provider_roots(&conn, roots);
    assert!(shallow(&env.scan(), candidate)
        .unwrap()
        .unwrap()
        .first_prompt
        .is_none());

    let mut stamps = serde_json::Map::new();
    sync(
        &conn,
        &mut stamps,
        &codex,
        &crate::ingest::SweepRepairs::default(),
    )
    .unwrap();
    let control: Option<String> = conn
        .query_row(
            "SELECT control_kind FROM session_events WHERE source='codex' AND session_id='sqlite-pruned-all-items' AND message_id='sqlite-sqlite-pruned-all-items-0'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(control.as_deref(), Some("codex_context_wrapper"));
    let prompts: i64 = conn
        .query_row(
            "SELECT count(*) FROM history WHERE source='codex' AND session_id='sqlite-pruned-all-items'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(prompts, 0);

    history_db.execute(
        "INSERT INTO thread_items VALUES ('sqlite-pruned-all-items','turn-1','real-prompt',13,1776643213000,'userMessage',?1)",
        [json!({"type":"userMessage","id":"real-prompt","content":[{"type":"text","text":"Please inspect the project"}]}).to_string()],
    ).unwrap();
    state_db
        .execute(
            "UPDATE threads SET updated_at_ms=1776643310000 WHERE id='sqlite-pruned-all-items'",
            [],
        )
        .unwrap();
    sync(
        &conn,
        &mut stamps,
        &codex,
        &crate::ingest::SweepRepairs::default(),
    )
    .unwrap();
    let prompts: Vec<String> = conn
        .prepare("SELECT prompt FROM history WHERE source='codex' AND session_id='sqlite-pruned-all-items'")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(|row| row.unwrap())
        .collect();
    assert_eq!(prompts, ["Please inspect the project"]);
}

#[test]
fn incomplete_rollout_prompt_survives_wrapper_only_sqlite_items() {
    let dir = tempfile::tempdir().unwrap();
    let codex = dir.path().join(".codex");
    install_fixture(&codex, "pruned-all-items");
    let wrapper = "<environment_context>injected</environment_context>";
    let rollout = codex.join("sessions/short.jsonl");
    std::fs::create_dir_all(rollout.parent().unwrap()).unwrap();
    std::fs::write(&rollout, "short").unwrap();
    let state_db = Connection::open(codex.join("state_5.sqlite")).unwrap();
    state_db.execute(
        "UPDATE threads SET first_user_message=?1,rollout_path=?2 WHERE id='sqlite-pruned-all-items'",
        params![wrapper, rollout.to_string_lossy()],
    ).unwrap();
    let history_db = Connection::open(codex.join("thread_history_1.sqlite")).unwrap();
    history_db.execute(
        "UPDATE thread_history_projection_state SET next_rollout_byte_offset=100 WHERE thread_id='sqlite-pruned-all-items'",
        [],
    ).unwrap();
    history_db.execute(
        "UPDATE thread_items SET item_json=?1 WHERE item_id='sqlite-sqlite-pruned-all-items-0'",
        [json!({"type":"userMessage","id":"sqlite-sqlite-pruned-all-items-0","content":[{"type":"text","text":wrapper}]}).to_string()],
    ).unwrap();
    let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
    conn.execute(
        "INSERT INTO sessions (source,session_id,raw_path,first_prompt) VALUES ('codex','sqlite-pruned-all-items',?1,'Earlier rollout prompt')",
        [rollout.to_string_lossy().as_ref()],
    ).unwrap();
    sync(
        &conn,
        &mut serde_json::Map::new(),
        &codex,
        &crate::ingest::SweepRepairs::default(),
    )
    .unwrap();
    let (path, prompt): (String, Option<String>) = conn.query_row(
        "SELECT raw_path,first_prompt FROM sessions WHERE source='codex' AND session_id='sqlite-pruned-all-items'",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    ).unwrap();
    assert!(path.starts_with("sqlite:"));
    assert_eq!(prompt.as_deref(), Some("Earlier rollout prompt"));
}
