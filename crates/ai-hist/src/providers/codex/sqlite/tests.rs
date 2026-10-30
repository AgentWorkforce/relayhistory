use super::*;

mod control;

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

fn install_thread_set(codex: &Path, updated: &[i64]) {
    std::fs::create_dir_all(codex).unwrap();
    let state = Connection::open(codex.join("state_5.sqlite")).unwrap();
    state
        .execute_batch(
            "CREATE TABLE threads(id TEXT PRIMARY KEY, rollout_path TEXT, created_at_ms INTEGER, \
             updated_at_ms INTEGER, source TEXT, cwd TEXT, git_branch TEXT, git_origin_url TEXT, \
             title TEXT, name TEXT, first_user_message TEXT, originator TEXT, cli_version TEXT, \
             model TEXT, tokens_used INTEGER, archived INTEGER, agent_nickname TEXT, \
             agent_role TEXT, history_mode TEXT, thread_source TEXT); \
             CREATE TABLE thread_spawn_edges(parent_thread_id TEXT,child_thread_id TEXT,status TEXT);",
        )
        .unwrap();
    let history = Connection::open(codex.join("thread_history_1.sqlite")).unwrap();
    history
        .execute_batch(
            "CREATE TABLE thread_history_projection_state(thread_id TEXT PRIMARY KEY, \
             next_rollout_ordinal INTEGER,next_rollout_byte_offset INTEGER); \
             CREATE TABLE thread_items(thread_id TEXT,turn_id TEXT,item_id TEXT, \
             rollout_ordinal INTEGER,created_at_ms INTEGER,item_type TEXT,item_json TEXT); \
             CREATE TABLE thread_turns(thread_id TEXT,turn_id TEXT,rollout_ordinal INTEGER, \
             started_at INTEGER,error_json TEXT);",
        )
        .unwrap();
    for (index, updated) in updated.iter().enumerate() {
        let id = format!("thread-{index:03}");
        state
            .execute(
                "INSERT INTO threads (id,created_at_ms,updated_at_ms,source,archived,history_mode) \
                 VALUES (?1,?2,?2,'cli',0,'paginated')",
                params![id, updated],
            )
            .unwrap();
        history
            .execute(
                "INSERT INTO thread_history_projection_state VALUES (?1,0,0)",
                [id],
            )
            .unwrap();
    }
}

fn insert_thread(codex: &Path, id: &str, updated: i64) {
    Connection::open(codex.join("state_5.sqlite"))
        .unwrap()
        .execute(
            "INSERT INTO threads (id,created_at_ms,updated_at_ms,source,archived,history_mode) \
             VALUES (?1,?2,?2,'cli',0,'paginated')",
            params![id, updated],
        )
        .unwrap();
    Connection::open(codex.join("thread_history_1.sqlite"))
        .unwrap()
        .execute(
            "INSERT INTO thread_history_projection_state VALUES (?1,0,0)",
            [id],
        )
        .unwrap();
}

#[test]
fn sqlite_backlog_is_capped_newest_first_and_resumes_from_its_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let codex = dir.path().join(".codex");
    install_thread_set(&codex, &(0..60).map(|i| 2_000_000 + i).collect::<Vec<_>>());
    let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
    let mut state = serde_json::Map::new();
    let policy = SweepPolicy::unbounded();
    let repairs = crate::ingest::SweepRepairs::default();

    let first = sync_with_policy(&conn, &mut state, &codex, &repairs, policy).unwrap();
    let first = first.read;
    assert_eq!(first.len(), DEFAULT_SWEEP_THREADS);
    assert_eq!(first.first().map(String::as_str), Some("thread-059"));
    assert_eq!(first.last().map(String::as_str), Some("thread-010"));
    assert!(state.get("codex_sqlite_sweep_cursor_v1").is_some());

    let second = sync_with_policy(&conn, &mut state, &codex, &repairs, policy).unwrap();
    let second = second.read;
    assert_eq!(second.len(), 10);
    assert_eq!(second.first().map(String::as_str), Some("thread-009"));
    assert_eq!(second.last().map(String::as_str), Some("thread-000"));
    assert!(state["codex_sqlite_sweep_cursor_v1"].is_null());
    assert_eq!(
        conn.query_row(
            "SELECT COUNT(*) FROM sessions WHERE source='codex'",
            [],
            |row| { row.get::<_, usize>(0) }
        )
        .unwrap(),
        60
    );
}

#[test]
fn full_sync_persists_multiple_cursor_pages_and_restarts_for_a_new_arrival() {
    let dir = tempfile::tempdir().unwrap();
    let codex = dir.path().join(".codex");
    install_thread_set(
        &codex,
        &[
            2_000_000, 2_000_001, 2_000_002, 2_000_003, 2_000_004, 2_000_005, 2_000_006,
        ],
    );
    let db = dir.path().join("history.db");
    let conn = crate::open_db(&db).unwrap();
    let mut roots =
        crate::ProviderRoots::from_home(dir.path().to_path_buf(), dir.path().join("opencode.db"));
    roots.codex = codex;
    let scope = crate::ingest::SweepScope::everything();
    let count = || {
        conn.query_row(
            "SELECT COUNT(*) FROM sessions WHERE source='codex'",
            [],
            |row| row.get::<_, usize>(0),
        )
        .unwrap()
    };

    let policy = SweepPolicy {
        max_threads: 2,
        min_updated_ms: None,
        max_age_days: None,
    };
    let sync = || {
        with_test_policy(policy, || {
            crate::ingest::sync_basic(&conn, &db, &roots, false, &scope)
        })
        .unwrap()
    };
    assert!(sync());
    assert_eq!(count(), 2);
    insert_thread(&roots.codex, "thread-new", 3_000_000);
    let mut settled = false;
    for _ in 0..12 {
        if !sync() {
            settled = true;
            break;
        }
    }
    assert!(settled, "a stable traversal eventually arms the fast path");
    assert_eq!(count(), 8);
    let persisted_bytes = std::fs::read(dir.path().join(".sync-state.json")).unwrap();
    let persisted: Value = serde_json::from_slice(&persisted_bytes).unwrap();
    assert!(persisted["codex_sqlite_sweep_cursor_v1"].is_null());
}

#[test]
fn sqlite_children_are_recovered_but_not_discovered_as_top_level_sessions() {
    let dir = tempfile::tempdir().unwrap();
    let codex = dir.path().join(".codex");
    install_thread_set(&codex, &[2_000_000]);
    Connection::open(codex.join("state_5.sqlite"))
        .unwrap()
        .execute(
            "UPDATE threads SET thread_source='subagent', \
             source='{\"subagent\":{\"thread_spawn\":{\"parent_thread_id\":\"parent\"}}}' \
             WHERE id='thread-000'",
            [],
        )
        .unwrap();
    let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
    let mut state = serde_json::Map::new();
    let outcome = sync(
        &conn,
        &mut state,
        &codex,
        &crate::ingest::SweepRepairs::default(),
    )
    .unwrap();
    assert_eq!(outcome.read, ["thread-000"]);

    let mut roots =
        crate::ProviderRoots::from_home(dir.path().to_path_buf(), dir.path().join("opencode.db"));
    roots.codex = codex.clone();
    assert!(enumerate(&codex, &roots).unwrap().0.is_empty());
}

#[test]
fn sqlite_cutoff_excludes_old_threads() {
    let dir = tempfile::tempdir().unwrap();
    let codex = dir.path().join(".codex");
    install_thread_set(&codex, &[1_000, 2_000]);
    let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
    let mut state = serde_json::Map::new();
    let policy = SweepPolicy {
        max_threads: DEFAULT_SWEEP_THREADS,
        min_updated_ms: Some(1_500),
        max_age_days: None,
    };
    let read = sync_with_policy(
        &conn,
        &mut state,
        &codex,
        &crate::ingest::SweepRepairs::default(),
        policy,
    )
    .unwrap()
    .read;
    assert_eq!(read, ["thread-001"]);
}

#[test]
fn sqlite_cutoff_change_moves_the_source_fingerprint() {
    let fingerprint = |max_age_days| {
        with_test_policy(
            SweepPolicy {
                max_threads: DEFAULT_SWEEP_THREADS,
                min_updated_ms: None,
                max_age_days,
            },
            || Ok(fingerprint_component()),
        )
        .unwrap()
    };
    assert_ne!(fingerprint(Some(30)), fingerprint(None));
}

#[test]
fn sqlite_threads_report_bounded_capture_progress() {
    let dir = tempfile::tempdir().unwrap();
    let codex = dir.path().join(".codex");
    install_thread_set(&codex, &[1_000, 2_000, 3_000]);
    let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
    let mut state = serde_json::Map::new();
    let updates = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let observed = updates.clone();
    crate::ingest::with_capture_observer(
        move |progress| observed.borrow_mut().push(progress),
        || {
            sync_with_policy(
                &conn,
                &mut state,
                &codex,
                &crate::ingest::SweepRepairs::default(),
                SweepPolicy::unbounded(),
            )
            .map(|_| ())
        },
    )
    .unwrap();
    let updates = updates.borrow();
    assert_eq!(updates.first().unwrap().source, "codex-sqlite");
    assert_eq!(updates.first().unwrap().processed_files, 0);
    assert_eq!(updates.last().unwrap().processed_files, 3);
    assert_eq!(updates.last().unwrap().total_files, Some(3));
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
fn locator_refresh_preserves_a_live_catalog_owned_sibling() {
    let dir = tempfile::tempdir().unwrap();
    let codex = dir.path().join(".codex");
    install_fixture(&codex, "subagent-edge");
    let primary = codex.join("sessions/primary.jsonl");
    let sibling = codex.join("sessions/sibling.jsonl");
    std::fs::create_dir_all(primary.parent().unwrap()).unwrap();
    std::fs::write(&primary, "primary\n").unwrap();
    std::fs::write(&sibling, "sibling\n").unwrap();
    Connection::open(codex.join("state_5.sqlite"))
        .unwrap()
        .execute(
            "UPDATE threads SET rollout_path=?1 WHERE id='sqlite-parent'",
            [primary.to_string_lossy().as_ref()],
        )
        .unwrap();
    Connection::open(codex.join("thread_history_1.sqlite"))
        .unwrap()
        .execute(
            "UPDATE thread_history_projection_state SET next_rollout_byte_offset=8 WHERE thread_id='sqlite-parent'",
            [],
        )
        .unwrap();
    let roots =
        crate::ProviderRoots::from_home(dir.path().to_path_buf(), dir.path().join("opencode.db"));
    assert_eq!(
        current_locator(&roots, "sqlite-parent", &sibling.to_string_lossy()).unwrap(),
        None,
        "a live catalog-owned sibling remains selected"
    );
    std::fs::remove_file(&sibling).unwrap();
    assert_eq!(
        current_locator(&roots, "sqlite-parent", &sibling.to_string_lossy()).unwrap(),
        Some(primary.to_string_lossy().into_owned()),
        "a missing observation may follow the current complete rollout"
    );
    std::fs::remove_file(&primary).unwrap();
    assert_eq!(
        current_locator(&roots, "sqlite-parent", &sibling.to_string_lossy()).unwrap(),
        Some(locator(&codex.join("state_5.sqlite"), "sqlite-parent")),
        "a pruned rollout recovers its SQLite projection"
    );
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

    // An unchanged item ID can carry rewritten output: ID-set retirement alone
    // would retain the old conflict-ignored event.
    history
        .execute(
            "UPDATE thread_items SET item_json=?1 WHERE item_id='repeated-answer'",
            [
                json!({"type":"agentMessage","id":"repeated-answer","text":"Rewritten answer"})
                    .to_string(),
            ],
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
    let rewritten: String = conn.query_row(
        "SELECT text FROM session_events WHERE session_id='sqlite-pruned-all-items' AND event_uid='repeated-answer'",
        [], |row| row.get(0),
    ).unwrap();
    assert_eq!(rewritten, "Rewritten answer");

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
        .read
        .is_empty());

    std::fs::remove_file(&rollout).unwrap();
    let pruned = sync(&conn, &mut state, &codex, &repairs).unwrap().read;
    assert_eq!(pruned, vec!["sqlite-rollout-primary".to_string()]);
    std::fs::write(&rollout, "complete rollout").unwrap();
    assert!(sync(&conn, &mut state, &codex, &repairs)
        .unwrap()
        .read
        .is_empty());

    std::fs::remove_file(&rollout).unwrap();
    let repruned = sync(&conn, &mut state, &codex, &repairs).unwrap().read;
    assert_eq!(repruned, vec!["sqlite-rollout-primary".to_string()]);
}

#[test]
fn restored_rollouts_remove_recovery_stamps_from_persisted_sync_state() {
    let dir = tempfile::tempdir().unwrap();
    let codex = dir.path().join(".codex");
    install_fixture(&codex, "rollout-primary");
    let rollout = codex.join("sessions/2026/04/20/rollout-rollout-primary.jsonl");
    let db = dir.path().join("ai-history.db");
    let conn = crate::open_db(&db).unwrap();
    let mut roots =
        crate::ProviderRoots::from_home(dir.path().to_path_buf(), dir.path().join("opencode.db"));
    roots.codex = codex;
    let scope = crate::ingest::SweepScope::everything();
    let sweep = || {
        with_test_policy(SweepPolicy::unbounded(), || {
            crate::ingest::sync_basic(&conn, &db, &roots, true, &scope)
        })
        .unwrap()
    };
    let stamps = || {
        let state: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(".sync-state.json")).unwrap())
                .unwrap();
        state["codex_sqlite_v2"].as_object().unwrap().clone()
    };

    assert!(sweep());
    assert!(stamps().contains_key("sqlite-rollout-primary"));
    std::fs::create_dir_all(rollout.parent().unwrap()).unwrap();
    std::fs::write(&rollout, "complete rollout").unwrap();
    assert!(sweep());
    assert!(!stamps().contains_key("sqlite-rollout-primary"));
    std::fs::remove_file(&rollout).unwrap();
    assert!(sweep());
    assert!(stamps().contains_key("sqlite-rollout-primary"));
}

#[test]
fn compressed_rollout_completeness_uses_decoded_projection_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let codex = dir.path().join(".codex");
    install_fixture(&codex, "rollout-primary");
    let rollout = codex.join("sessions/2026/04/20/rollout-rollout-primary.jsonl");
    std::fs::create_dir_all(rollout.parent().unwrap()).unwrap();
    let decoded = format!("{}\n", "complete rollout content".repeat(128));
    let compressed = zstd::bulk::compress(decoded.as_bytes(), 3).unwrap();
    assert!(compressed.len() < decoded.len());
    let compressed_path = crate::ingest::transcript_file::compressed_path(&rollout);
    std::fs::write(&compressed_path, compressed).unwrap();
    Connection::open(codex.join("thread_history_1.sqlite"))
        .unwrap()
        .execute(
            "UPDATE thread_history_projection_state SET next_rollout_byte_offset=?1 WHERE thread_id='sqlite-rollout-primary'",
            [decoded.len() as i64],
        )
        .unwrap();
    let mut roots =
        crate::ProviderRoots::from_home(dir.path().to_path_buf(), dir.path().join("opencode.db"));
    roots.codex = codex.clone();
    let state = codex.join("state_5.sqlite");
    assert!(enumerate(&codex, &roots).unwrap().0.is_empty());
    assert_eq!(
        restored_rollout(&roots, &state, "sqlite-rollout-primary").unwrap(),
        Some(rollout.clone())
    );
    assert_eq!(
        current_locator(&roots, "sqlite-rollout-primary", &rollout.to_string_lossy()).unwrap(),
        Some(rollout.to_string_lossy().into_owned())
    );

    std::fs::write(
        &compressed_path,
        zstd::bulk::compress(b"short\n", 3).unwrap(),
    )
    .unwrap();
    assert_eq!(
        enumerate(&codex, &roots).unwrap().0.len(),
        1,
        "a decoded rollout shorter than the projection uses SQLite"
    );
    assert_eq!(
        restored_rollout(&roots, &state, "sqlite-rollout-primary").unwrap(),
        None
    );
    assert_eq!(
        current_locator(&roots, "sqlite-rollout-primary", &rollout.to_string_lossy()).unwrap(),
        Some(locator(&state, "sqlite-rollout-primary"))
    );
}

#[test]
fn a_malformed_thread_does_not_block_other_sqlite_threads() {
    let dir = tempfile::tempdir().unwrap();
    let codex = dir.path().join(".codex");
    install_fixture(&codex, "pruned-all-items");
    let state_db = Connection::open(codex.join("state_5.sqlite")).unwrap();
    let history_db = Connection::open(codex.join("thread_history_1.sqlite")).unwrap();
    state_db.execute_batch(
        "INSERT INTO threads SELECT 'sqlite-zz-good',rollout_path,created_at_ms,updated_at_ms,source,cwd,git_branch,git_origin_url,title,name,first_user_message,originator,cli_version,model,tokens_used,archived,agent_nickname,agent_role,history_mode,thread_source FROM threads WHERE id='sqlite-pruned-all-items';"
    ).unwrap();
    history_db.execute_batch(
        "INSERT INTO thread_history_projection_state SELECT 'sqlite-zz-good',next_rollout_ordinal,next_rollout_byte_offset FROM thread_history_projection_state WHERE thread_id='sqlite-pruned-all-items';
         INSERT INTO thread_items SELECT 'sqlite-zz-good',turn_id,item_id,rollout_ordinal,created_at_ms,item_type,item_json FROM thread_items WHERE thread_id='sqlite-pruned-all-items';
         INSERT INTO thread_turns SELECT 'sqlite-zz-good',turn_id,rollout_ordinal,started_at,error_json FROM thread_turns WHERE thread_id='sqlite-pruned-all-items';
         UPDATE thread_items SET item_json='not-json' WHERE thread_id='sqlite-pruned-all-items' AND rollout_ordinal=0;"
    ).unwrap();
    let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
    let mut state = serde_json::Map::new();
    let outcome = sync(
        &conn,
        &mut state,
        &codex,
        &crate::ingest::SweepRepairs::default(),
    )
    .unwrap();
    assert!(outcome.inserted > 0);
    assert_eq!(outcome.read, ["sqlite-zz-good"]);
    assert!(outcome.incomplete);
    let stamps = state["codex_sqlite_v2"].as_object().unwrap();
    assert!(stamps.contains_key("sqlite-zz-good"));
    assert!(!stamps.contains_key("sqlite-pruned-all-items"));
    let completion = sync(
        &conn,
        &mut state,
        &codex,
        &crate::ingest::SweepRepairs::default(),
    )
    .unwrap();
    assert!(completion.read.is_empty());
    assert!(
        completion.incomplete,
        "a failure on an earlier cursor page survives through completion"
    );
}

#[test]
fn failed_thread_retries_after_the_backlog_drains() {
    let dir = tempfile::tempdir().unwrap();
    let codex = dir.path().join(".codex");
    install_fixture(&codex, "pruned-all-items");
    let state_db = Connection::open(codex.join("state_5.sqlite")).unwrap();
    let history_db = Connection::open(codex.join("thread_history_1.sqlite")).unwrap();
    state_db.execute_batch(
        "INSERT INTO threads SELECT 'sqlite-zz-good',rollout_path,created_at_ms,updated_at_ms,source,cwd,git_branch,git_origin_url,title,name,first_user_message,originator,cli_version,model,tokens_used,archived,agent_nickname,agent_role,history_mode,thread_source FROM threads WHERE id='sqlite-pruned-all-items';"
    ).unwrap();
    history_db.execute_batch(
        "INSERT INTO thread_history_projection_state SELECT 'sqlite-zz-good',next_rollout_ordinal,next_rollout_byte_offset FROM thread_history_projection_state WHERE thread_id='sqlite-pruned-all-items';
         INSERT INTO thread_items SELECT 'sqlite-zz-good',turn_id,item_id,rollout_ordinal,created_at_ms,item_type,item_json FROM thread_items WHERE thread_id='sqlite-pruned-all-items';
         INSERT INTO thread_turns SELECT 'sqlite-zz-good',turn_id,rollout_ordinal,started_at,error_json FROM thread_turns WHERE thread_id='sqlite-pruned-all-items';
         UPDATE thread_items SET item_json='not-json' WHERE thread_id='sqlite-pruned-all-items' AND rollout_ordinal=0;"
    ).unwrap();
    let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
    let mut state = serde_json::Map::new();
    let repairs = crate::ingest::SweepRepairs::default();
    let original = SweepPolicy {
        max_threads: 1,
        min_updated_ms: Some(0),
        max_age_days: None,
    };

    assert_eq!(
        sync_with_policy(&conn, &mut state, &codex, &repairs, original)
            .unwrap()
            .read,
        ["sqlite-zz-good"]
    );
    let failed = sync_with_policy(&conn, &mut state, &codex, &repairs, original).unwrap();
    assert!(failed.incomplete);
    let drained = sync_with_policy(&conn, &mut state, &codex, &repairs, original).unwrap();
    assert!(drained.incomplete);
    assert!(state["codex_sqlite_sweep_cursor_v1"]
        .as_str()
        .unwrap()
        .contains("\"restart\":true"));

    history_db
        .execute(
            "UPDATE thread_items SET item_json=(SELECT item_json FROM thread_items WHERE thread_id='sqlite-zz-good' AND rollout_ordinal=0) WHERE thread_id='sqlite-pruned-all-items' AND rollout_ordinal=0",
            [],
        )
        .unwrap();
    let retry = SweepPolicy {
        max_threads: 1,
        min_updated_ms: Some(0),
        max_age_days: None,
    };
    let _ = sync_with_policy(&conn, &mut state, &codex, &repairs, retry).unwrap();
    let retried = sync_with_policy(&conn, &mut state, &codex, &repairs, retry).unwrap();
    assert_eq!(retried.read, ["sqlite-pruned-all-items"]);
    let _ = sync_with_policy(&conn, &mut state, &codex, &repairs, retry).unwrap();
    assert!(state["codex_sqlite_sweep_cursor_v1"].is_null());
}

#[test]
fn retry_restart_uses_the_current_cutoff() {
    let dir = tempfile::tempdir().unwrap();
    let codex = dir.path().join(".codex");
    install_thread_set(&codex, &[100]);
    let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
    let mut state = serde_json::Map::new();
    state.insert(
        "codex_sqlite_sweep_cursor_v1".into(),
        Value::String(
            json!({
                "updated_at_ms": 100,
                "thread_id": "thread-000",
                "retry_required": false,
                "restart": true,
                "min_updated_ms": 0
            })
            .to_string(),
        ),
    );
    let expired = SweepPolicy {
        max_threads: 1,
        min_updated_ms: Some(101),
        max_age_days: None,
    };

    let outcome = sync_with_policy(
        &conn,
        &mut state,
        &codex,
        &crate::ingest::SweepRepairs::default(),
        expired,
    )
    .unwrap();
    assert!(!outcome.incomplete);
    assert!(outcome.read.is_empty());
    assert!(state["codex_sqlite_sweep_cursor_v1"].is_null());
}

#[test]
fn exact_rollout_sync_does_not_enumerate_or_hydrate_sqlite_siblings() {
    let temp = tempfile::tempdir().unwrap();
    let codex = temp.path().join(".codex");
    install_thread_set(&codex, &vec![crate::now_ms(); 1000]);
    let path = codex.join("sessions/2026/10/23/rollout-2026-10-23T10-00-00-12345678-1234-1234-1234-123456789abc.jsonl");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let record = |uuid| {
        serde_json::json!({"timestamp":"2026-10-23T10:00:00Z","type":"event_msg","payload":{"type":"user_message","message":uuid}}).to_string()+"\n"
    };
    std::fs::write(&path,serde_json::json!({"type":"session_meta","payload":{"id":"12345678-1234-1234-1234-123456789abc","cwd":"/tmp/p"}}).to_string()+"\n"+&record("first")).unwrap();
    let mut roots =
        crate::ProviderRoots::from_home(temp.path().to_owned(), temp.path().join("opencode.db"));
    roots.codex = codex;
    let store = crate::SessionStore::open(
        crate::StoreOptions::default()
            .db_path(temp.path().join("history.db"))
            .roots(roots),
    )
    .unwrap();
    let options = crate::SyncOptions::default()
        .sources([crate::Source::Codex])
        .paths([path.clone()]);
    store.sync(options.clone()).unwrap();
    use std::io::Write;
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(record("delta").as_bytes())
        .unwrap();
    crate::ingest::state_work::take();
    let report = store.sync(options).unwrap();
    assert_eq!(crate::ingest::state_work::take(), (0, 0));
    assert!(report.changed.len() <= 1);
    let conn = crate::open_db(store.db_path()).unwrap();
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM sessions", [], |row| row
            .get::<_, i64>(0))
            .unwrap(),
        1,
        "SQLite-only siblings must stay untouched"
    );
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM session_events", [], |row| row
            .get::<_, i64>(0))
            .unwrap(),
        2
    );
}

#[test]
fn selected_sqlite_locator_reports_unavailable_after_pair_disappears() {
    let temp = tempfile::tempdir().unwrap();
    let codex = temp.path().join(".codex");
    install_thread_set(&codex, &[2_000_000]);
    let roots =
        crate::ProviderRoots::from_home(temp.path().to_owned(), temp.path().join("opencode.db"));
    let db = temp.path().join("history.db");
    let conn = crate::open_db(&db).unwrap();
    let locator = locator(&codex.join("state_5.sqlite"), "thread-000");
    conn.execute("INSERT INTO sessions (source,session_id,raw_path,discovery_state) VALUES ('codex','thread-000',?1,'shallow')", [&locator]).unwrap();
    conn.execute("INSERT INTO session_presences (source,session_id,location,raw_locator,source_stamp,discovery_state) VALUES ('codex','thread-000','local',?1,'v2:test','shallow')", [&locator]).unwrap();
    drop(conn);
    std::fs::remove_file(codex.join("state_5.sqlite")).unwrap();
    std::fs::remove_file(codex.join("thread_history_1.sqlite")).unwrap();
    let store =
        crate::SessionStore::open(crate::StoreOptions::default().db_path(db).roots(roots)).unwrap();
    let error = store
        .hydrate(
            &crate::SessionRef::id(crate::Source::Codex, "thread-000"),
            crate::HydrateOptions::default(),
        )
        .unwrap_err();
    assert_eq!(error.code(), "SESSION_SOURCE_UNAVAILABLE");
}

#[test]
fn history_log_collision_does_not_claim_another_sessions_sqlite_prompt() {
    let temp = tempfile::tempdir().unwrap();
    let conn = crate::open_db(&temp.path().join("history.db")).unwrap();
    let entry = HistoryEntry {
        id: 0,
        source: "codex".into(),
        session_id: Some("owner".into()),
        project: None,
        prompt: "same prompt".into(),
        prompt_hash: None,
        timestamp_ms: 1000,
    };
    crate::insert_history(&conn, &entry).unwrap();
    let id = conn.last_insert_rowid();
    conn.execute("INSERT INTO session_markers (source,session_id,marker_uid,kind,subkind,payload_json) VALUES ('codex','owner','sqlite:thread','session_metadata','sqlite',?1)", [json!({"history_ids":[id]}).to_string()]).unwrap();
    let mut collision = entry.clone();
    collision.session_id = Some("other".into());
    assert_eq!(crate::insert_history(&conn, &collision).unwrap(), 0);
    claim_history_log_prompt(&conn, &collision).unwrap();
    let ids: String = conn.query_row("SELECT json_extract(payload_json,'$.history_ids') FROM session_markers WHERE session_id='owner'", [], |row| row.get(0)).unwrap();
    assert_eq!(ids, json!([id]).to_string());
    claim_history_log_prompt(&conn, &entry).unwrap();
    let ids: String = conn.query_row("SELECT json_extract(payload_json,'$.history_ids') FROM session_markers WHERE session_id='owner'", [], |row| row.get(0)).unwrap();
    assert_eq!(ids, "[]");
}

/// The facade's sync is quiet, so a Codex store pair it cannot use is not
/// on stderr; it is on the report, as the diagnostic the host asked for by
/// embedding instead of running the CLI.
#[test]
fn a_quiet_sync_reports_a_missing_sqlite_store_pair_on_the_report() {
    let temp = tempfile::tempdir().unwrap();
    let codex = temp.path().join(".codex");
    install_thread_set(&codex, &[2_000_000]);
    std::fs::remove_file(codex.join("thread_history_1.sqlite")).unwrap();
    let roots =
        crate::ProviderRoots::from_home(temp.path().to_owned(), temp.path().join("opencode.db"));
    let store = crate::SessionStore::open(
        crate::StoreOptions::default()
            .db_path(temp.path().join("history.db"))
            .roots(roots),
    )
    .unwrap();
    let report = store.sync(crate::SyncOptions::default()).unwrap();
    assert!(report.swept);
    let codex_skips: Vec<_> = report
        .diagnostics
        .iter()
        .filter(|diagnostic| diagnostic.source == crate::Source::Codex)
        .collect();
    assert_eq!(codex_skips.len(), 1, "{:?}", report.diagnostics);
    assert_eq!(codex_skips[0].code, "SESSION_SOURCE_UNAVAILABLE");
    assert!(
        codex_skips[0]
            .message
            .starts_with("skipping SQLite history: incomplete Codex SQLite history pair"),
        "{}",
        codex_skips[0].message
    );
    assert_eq!(codex_skips[0].subject, None);
    // The report round-trips with the diagnostics on it.
    let json = serde_json::to_string(&report).unwrap();
    assert_eq!(
        serde_json::from_str::<crate::SyncReport>(&json).unwrap(),
        report
    );
}

/// A store pair the process may not read is a skip of the same kind.
#[cfg(unix)]
#[test]
fn a_quiet_sync_reports_a_withheld_sqlite_store_pair_on_the_report() {
    use std::os::unix::fs::PermissionsExt;
    let temp = tempfile::tempdir().unwrap();
    let codex = temp.path().join(".codex");
    install_thread_set(&codex, &[2_000_000]);
    let state = codex.join("state_5.sqlite");
    std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o000)).unwrap();
    if std::fs::File::open(&state).is_ok() {
        // Root reads through the mode bits; nothing is withheld here.
        return;
    }
    let roots =
        crate::ProviderRoots::from_home(temp.path().to_owned(), temp.path().join("opencode.db"));
    let store = crate::SessionStore::open(
        crate::StoreOptions::default()
            .db_path(temp.path().join("history.db"))
            .roots(roots),
    )
    .unwrap();
    let report = store.sync(crate::SyncOptions::default()).unwrap();
    let codex_skips: Vec<_> = report
        .diagnostics
        .iter()
        .filter(|diagnostic| diagnostic.source == crate::Source::Codex)
        .collect();
    assert!(!codex_skips.is_empty(), "{:?}", report.diagnostics);
    assert!(codex_skips
        .iter()
        .all(|diagnostic| diagnostic.code == "SESSION_SOURCE_UNAVAILABLE"));
    assert!(
        codex_skips[0]
            .message
            .starts_with("skipping SQLite history: "),
        "{}",
        codex_skips[0].message
    );
    std::fs::set_permissions(&state, std::fs::Permissions::from_mode(0o644)).unwrap();
}

/// A watch tick is the same quiet sweep: its report carries the skip too.
#[test]
fn a_watch_tick_reports_a_missing_sqlite_store_pair_on_its_report() {
    let temp = tempfile::tempdir().unwrap();
    let codex = temp.path().join(".codex");
    install_thread_set(&codex, &[2_000_000]);
    std::fs::remove_file(codex.join("thread_history_1.sqlite")).unwrap();
    let roots =
        crate::ProviderRoots::from_home(temp.path().to_owned(), temp.path().join("opencode.db"));
    let store = crate::SessionStore::open(
        crate::StoreOptions::default()
            .db_path(temp.path().join("history.db"))
            .roots(roots),
    )
    .unwrap();
    let mut watch = store
        .watch(crate::WatchOptions {
            use_fs_events: false,
            poll_interval_ms: 100,
            immediate: true,
            ..crate::WatchOptions::default()
        })
        .unwrap();
    let first = watch
        .next_timeout(std::time::Duration::from_secs(30))
        .expect("startup tick")
        .unwrap();
    assert!(first.swept);
    let codex_skips: Vec<_> = first
        .diagnostics
        .iter()
        .filter(|diagnostic| diagnostic.source == crate::Source::Codex)
        .collect();
    assert_eq!(codex_skips.len(), 1, "{:?}", first.diagnostics);
    assert_eq!(codex_skips[0].code, "SESSION_SOURCE_UNAVAILABLE");
    watch.stop();
}

/// A hydration whose locator refresh finds a store pair the selection rules
/// reject reads the saved rollout and says why SQLite was not consulted.
#[test]
fn a_hydration_reports_an_unsupported_sqlite_store_pair_on_its_report() {
    let temp = tempfile::tempdir().unwrap();
    let codex = temp.path().join(".codex");
    let rollout = codex.join("sessions/2026/04/20/rollout-root-thread.jsonl");
    std::fs::create_dir_all(rollout.parent().unwrap()).unwrap();
    let lines = [
        json!({"timestamp":"2026-04-20T00:00:00Z","type":"session_meta","payload":{"id":"root-thread","cwd":"/work/app"}}),
        json!({"timestamp":"2026-04-20T00:00:01Z","type":"event_msg","payload":{"type":"agent_message","message":"Done"}}),
    ];
    std::fs::write(
        &rollout,
        lines.iter().map(Value::to_string).collect::<Vec<_>>().join("\n") + "\n",
    )
    .unwrap();
    std::fs::write(codex.join("state_6.sqlite"), b"").unwrap();
    std::fs::write(codex.join("thread_history_1.sqlite"), b"").unwrap();
    let roots =
        crate::ProviderRoots::from_home(temp.path().to_owned(), temp.path().join("opencode.db"));
    let store = crate::SessionStore::open(
        crate::StoreOptions::default()
            .db_path(temp.path().join("history.db"))
            .roots(roots),
    )
    .unwrap();
    store.sync(crate::SyncOptions::default()).unwrap();
    let report = store
        .hydrate(
            &crate::SessionRef::id(crate::Source::Codex, "root-thread"),
            crate::HydrateOptions::default(),
        )
        .unwrap();
    let skips: Vec<_> = report
        .diagnostics
        .iter()
        .filter(|diagnostic| diagnostic.code == "SESSION_SOURCE_UNAVAILABLE")
        .collect();
    assert_eq!(skips.len(), 1, "{:?}", report.diagnostics);
    assert_eq!(
        skips[0].message,
        "skipping SQLite locator refresh: unsupported Codex state_6.sqlite; supported version: 5"
    );
    assert_eq!(skips[0].subject.as_deref(), Some("root-thread"));
}
