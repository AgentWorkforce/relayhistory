use super::*;

const SID: &str = "5f0c2a8e-1d4b-4c6e-9a7f-3b2d1e0c9a81";
const FIXTURE: &str = include_str!(
    "../../../tests/fixtures/copilot/cli-usage/.copilot/session-state/5f0c2a8e-1d4b-4c6e-9a7f-3b2d1e0c9a81/events.jsonl"
);

fn fixture() -> (tempfile::TempDir, crate::SessionStore, PathBuf) {
    let temp = tempfile::tempdir().unwrap();
    let roots = ProviderRoots::from_home(temp.path().into(), temp.path().join("opencode.db"));
    let path = roots.for_provider(ID)[0].join(SID).join(JOURNAL);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, FIXTURE).unwrap();
    let store = crate::SessionStore::open(
        crate::StoreOptions::default()
            .db_path(temp.path().join("store.db"))
            .roots(roots),
    )
    .unwrap();
    (temp, store, path)
}

fn sibling(path: &Path, sid: &str) -> PathBuf {
    let journal = path.parent().unwrap().with_file_name(sid).join(JOURNAL);
    std::fs::create_dir_all(journal.parent().unwrap()).unwrap();
    std::fs::write(&journal, FIXTURE.replace(SID, sid)).unwrap();
    journal
}

fn run(
    conn: &Connection,
    store: &crate::SessionStore,
    state: &mut Map<String, Value>,
    paths: Option<&PathScope>,
) -> SweepResult {
    sweep(SweepContext {
        conn,
        state,
        roots: store.roots(),
        paths,
        repairs: &Default::default(),
    })
    .unwrap()
}

fn count(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |r| r.get(0)).unwrap()
}

fn payload(store: &crate::SessionStore, path: &Path) -> Value {
    snapshot(path, &AcquisitionContext::new(ID, store.roots()))
        .unwrap()
        .payload
}

#[test]
fn strict_header_version_and_tail() {
    let err = |bytes: &[u8]| format::parse(bytes, false).unwrap_err().to_string();
    assert!(err(b"{\"type\":\"user.message\"}\n").contains("UNSUPPORTED_SCHEMA"));
    assert!(
        err(b"{\"type\":\"session.start\",\"data\":{\"version\":2,\"sessionId\":\"s\"}}\n")
            .contains("UNSUPPORTED_SCHEMA")
    );
    assert!(format::parse(b"{", true)
        .unwrap_err()
        .to_string()
        .contains("INCOMPLETE_HEADER"));
    let header = FIXTURE.lines().next().unwrap();
    let again = format!("{header}\n{header}\n");
    assert!(err(again.as_bytes()).contains("duplicate"));
    let bad_tool = format!(
        "{header}\n{}\n",
        r#"{"type":"tool.execution_complete","data":{"toolCallId":"t"},"id":"x","timestamp":"2026-01-01T00:00:00Z","parentId":null}"#
    );
    assert!(err(bad_tool.as_bytes()).contains("success"));
}

#[test]
fn usage_is_output_only_and_rejects_non_integers() {
    let u = usage::normalize(&serde_json::json!({"outputTokens": 42}))
        .unwrap()
        .unwrap();
    assert_eq!(u.output_tokens, 42);
    assert!(u.coverage.has_output_tokens && !u.coverage.has_input_tokens);
    assert!(usage::normalize(&serde_json::json!({"outputTokens": -1})).is_err());
    assert!(usage::normalize(&serde_json::json!([])).is_err());
    assert!(usage::normalize(&serde_json::json!({})).unwrap().is_none());
}

#[test]
fn copilot_home_replaces_the_whole_root() {
    let home = Path::new("/isolated");
    assert_eq!(
        resolve_roots(home, false),
        home.join(".copilot/session-state")
    );
}

#[test]
fn journals_skip_session_sidecars_and_non_session_entries() {
    let (_temp, store, path) = fixture();
    let session = path.parent().unwrap();
    for sidecar in ["checkpoints", "files", "rewind-file-snapshots/x"] {
        let dir = session.join(sidecar);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("events.jsonl"), FIXTURE).unwrap();
    }
    std::fs::write(session.parent().unwrap().join("stray.jsonl"), FIXTURE).unwrap();
    std::fs::create_dir_all(session.with_file_name("no-journal")).unwrap();
    let conn = crate::open_db(store.db_path()).unwrap();
    let env = DiscoveryEnv::with_provider_roots(&conn, store.roots().clone());
    let found = Provider.enumerate(&env, None).unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].locator, path.to_string_lossy());
    assert_eq!(env.counters().files_opened, 0);
}

#[test]
fn shallow_read_takes_last_activity_from_the_tail_window() {
    let (_temp, store, path) = fixture();
    let filler = format!(
        "{}\n",
        serde_json::json!({"type":"system.message","data":{"content":"x".repeat(80 * 1024)},
            "id":"filler","timestamp":"2026-09-18T21:30:01.500Z","parentId":null})
    );
    let mut lines: Vec<&str> = FIXTURE.lines().collect();
    let header = lines.remove(0);
    std::fs::write(&path, format!("{header}\n{filler}{}\n", lines.join("\n"))).unwrap();
    let conn = crate::open_db(store.db_path()).unwrap();
    let env = DiscoveryEnv::with_provider_roots(&conn, store.roots().clone());
    let candidate = Provider.enumerate(&env, None).unwrap().remove(0);
    let shallow = Provider
        .read_shallow(&env.scan(), Some(&conn), &candidate)
        .unwrap()
        .unwrap();
    let last = format::time(&serde_json::from_str(lines.last().unwrap()).unwrap()).unwrap();
    assert_eq!(shallow.last_activity_ms, Some(last));
    assert_eq!(shallow.agent_version.as_deref(), Some("1.0.86"));
    assert_eq!(shallow.git_branch.as_deref(), Some("main"));
    assert!(env.counters().bytes_read <= HEAD_LIMIT + TAIL_LIMIT);
}

#[test]
fn selected_snapshot_is_immutable() {
    let (_temp, store, path) = fixture();
    let captured = snapshot(&path, &AcquisitionContext::new(ID, store.roots())).unwrap();
    std::fs::write(&path, FIXTURE.replace("You're welcome.", "Changed on disk")).unwrap();
    let conn = crate::open_db(store.db_path()).unwrap();
    normalize::write(&conn, &captured.payload, &path).unwrap();
    assert_eq!(
        count(
            &conn,
            "SELECT count(*) FROM session_events WHERE source='copilot' AND text='You''re welcome.'"
        ),
        1
    );
}

#[test]
fn scoped_tick_skips_siblings_and_full_backstop_reads_them() {
    let (_temp, store, path) = fixture();
    let other = sibling(&path, "other-session");
    let conn = crate::open_db(store.db_path()).unwrap();
    let mut state = Map::new();
    let scope = PathScope::from_paths([path.clone()]);
    run(&conn, &store, &mut state, scope.as_ref());
    let sessions = "SELECT count(*) FROM sessions WHERE source='copilot'";
    assert_eq!(count(&conn, sessions), 1);
    run(&conn, &store, &mut state, None);
    assert_eq!(count(&conn, sessions), 2);
    // A change under a session's sidecar directory re-checks only that journal.
    std::fs::write(&other, "not a journal").unwrap();
    let sidecar = PathScope::from_paths([path.parent().unwrap().join("checkpoints/index.md")]);
    assert_eq!(run(&conn, &store, &mut state, sidecar.as_ref()).unread, 0);
}

#[test]
fn bad_siblings_do_not_poison_discovery_or_sweep() {
    let (_temp, store, path) = fixture();
    for (name, bytes) in [
        ("invalid", "{bad}\n"),
        (
            "unsupported",
            "{\"type\":\"session.start\",\"data\":{\"version\":9}}\n",
        ),
        ("partial", "{"),
    ] {
        let journal = path.parent().unwrap().with_file_name(name).join(JOURNAL);
        std::fs::create_dir_all(journal.parent().unwrap()).unwrap();
        std::fs::write(journal, bytes).unwrap();
    }
    let discovered = store
        .discover(crate::DiscoveryOptions::default().sources([crate::Source::Copilot]))
        .unwrap();
    assert_eq!(discovered.discovered, 1);
    assert_eq!(discovered.diagnostics.len(), 3);
    let conn = crate::open_db(store.db_path()).unwrap();
    let mut state = Map::new();
    assert_eq!(run(&conn, &store, &mut state, None).unread, 3);
    assert_eq!(
        count(
            &conn,
            "SELECT count(*) FROM sessions WHERE source='copilot'"
        ),
        1
    );
}

#[test]
fn file_cache_skips_unchanged_and_refreshes_generation() {
    let (_temp, store, path) = fixture();
    let conn = crate::open_db(store.db_path()).unwrap();
    let mut state = Map::new();
    run(&conn, &store, &mut state, None);
    conn.execute(
        "UPDATE sessions SET last_assistant_text='sentinel' WHERE source='copilot'",
        [],
    )
    .unwrap();
    run(&conn, &store, &mut state, None);
    let preview = || {
        conn.query_row(
            "SELECT last_assistant_text FROM sessions WHERE source='copilot'",
            [],
            |r| r.get::<_, String>(0),
        )
        .unwrap()
    };
    assert_eq!(
        preview(),
        "sentinel",
        "unchanged sweep did not normalize again"
    );
    state["copilot_file_stamps"][path.to_string_lossy().as_ref()]["generation"] = "old".into();
    run(&conn, &store, &mut state, None);
    assert_eq!(preview(), "You're welcome.");
}

#[test]
fn partial_tail_preserves_prior_evidence_and_stays_retryable() {
    let (_temp, store, path) = fixture();
    let conn = crate::open_db(store.db_path()).unwrap();
    let mut state = Map::new();
    run(&conn, &store, &mut state, None);
    let events = "SELECT count(*) FROM session_events WHERE source='copilot'";
    let before = count(&conn, events);
    let truncated: String = FIXTURE
        .lines()
        .take(5)
        .map(|l| format!("{l}\n"))
        .collect::<String>()
        + "{partial";
    std::fs::write(&path, truncated).unwrap();
    let scope = PathScope::from_paths([path.clone()]);
    assert_eq!(run(&conn, &store, &mut state, scope.as_ref()).unread, 1);
    assert_eq!(
        count(&conn, events),
        before,
        "partial rewrite retires nothing"
    );
    assert!(state["copilot_file_stamps"]
        .get(path.to_string_lossy().as_ref())
        .is_none());
    assert_eq!(run(&conn, &store, &mut state, scope.as_ref()).unread, 1);
}

#[test]
fn rewrite_retires_dropped_tools_results_and_markers() {
    let (_temp, store, path) = fixture();
    let conn = crate::open_db(store.db_path()).unwrap();
    let mut payload = payload(&store, &path);
    normalize::write(&conn, &payload, &path).unwrap();
    let bash_error = "SELECT is_error FROM tool_calls WHERE source='copilot' AND tool_use_id='toolu_fixture_bash'";
    assert_eq!(count(&conn, bash_error), 1);
    payload["entries"].as_array_mut().unwrap().retain(|e| {
        !(e["type"] == "tool.execution_complete" && e["data"]["toolCallId"] == "toolu_fixture_bash")
            && e["type"] != "session.shutdown"
    });
    normalize::write(&conn, &payload, &path).unwrap();
    let error: Option<bool> = conn.query_row(bash_error, [], |r| r.get(0)).unwrap();
    assert_eq!(error, None, "removed result clears its outcome");
    assert_eq!(
        count(
            &conn,
            "SELECT count(*) FROM session_markers WHERE source='copilot' AND kind='session.shutdown'"
        ),
        0
    );
    payload["entries"]
        .as_array_mut()
        .unwrap()
        .retain(|e| e["data"]["toolCallId"] != "toolu_fixture_bash");
    for entry in payload["entries"].as_array_mut().unwrap() {
        if entry["type"] == "assistant.message" {
            entry["data"]["toolRequests"] = serde_json::json!([]);
        }
    }
    normalize::write(&conn, &payload, &path).unwrap();
    assert_eq!(
        count(
            &conn,
            "SELECT count(*) FROM tool_calls WHERE source='copilot' AND tool_use_id='toolu_fixture_bash'"
        ),
        0
    );
}

#[test]
fn unrequested_tool_start_is_its_own_call_and_agent_messages_are_not_prompts() {
    let (_temp, store, path) = fixture();
    let conn = crate::open_db(store.db_path()).unwrap();
    let mut payload = payload(&store, &path);
    let entries = payload["entries"].as_array_mut().unwrap();
    entries.push(serde_json::json!({"type":"tool.execution_start","data":{"toolCallId":"user-requested","toolName":"bash","arguments":{"command":"ls"}},"id":"start-only","timestamp":"2026-09-18T21:31:00Z","parentId":null}));
    entries.push(serde_json::json!({"type":"user.message","data":{"content":"Delivered by another agent","source":"agent-other"},"id":"agent-user","timestamp":"2026-09-18T21:31:01Z","parentId":"start-only"}));
    entries.push(serde_json::json!({"type":"session.future_event","data":{"anything":true},"id":"future","timestamp":"2026-09-18T21:31:02Z","parentId":"agent-user"}));
    normalize::write(&conn, &payload, &path).unwrap();
    for (sql, expected) in [
        ("SELECT count(*) FROM tool_calls WHERE source='copilot' AND tool_use_id='user-requested' AND name='bash'", 1),
        ("SELECT count(*) FROM history WHERE source='copilot' AND prompt='Delivered by another agent'", 0),
        ("SELECT count(*) FROM session_events WHERE source='copilot' AND role='user' AND text='Delivered by another agent'", 1),
        ("SELECT count(*) FROM session_markers WHERE source='copilot' AND kind='session.future_event'", 1),
    ] {
        assert_eq!(count(&conn, sql), expected, "{sql}");
    }
}

#[test]
fn subagent_messages_are_sidechain_and_identities_are_native() {
    let (_temp, store, path) = fixture();
    let conn = crate::open_db(store.db_path()).unwrap();
    normalize::write(&conn, &payload(&store, &path), &path).unwrap();
    for (sql, expected) in [
        ("SELECT count(DISTINCT message_id) FROM session_events WHERE source='copilot' AND is_sidechain=1", 2),
        ("SELECT count(*) FROM session_events WHERE source='copilot' AND role='assistant' AND (provider_message_id IS NULL OR agent_version IS NOT '1.0.86')", 0),
        ("SELECT count(*) FROM session_events WHERE source='copilot' AND request_id='req-fixture-1'", 3),
    ] {
        assert_eq!(count(&conn, sql), expected, "{sql}");
    }
}

#[test]
fn deleted_journal_retires_only_after_an_accessible_full_sweep() {
    let (temp, store, path) = fixture();
    let other = sibling(&path, "other-session");
    let conn = crate::open_db(store.db_path()).unwrap();
    let mut state = Map::new();
    run(&conn, &store, &mut state, None);
    let root = store.roots().for_provider(ID)[0].clone();
    let moved = temp.path().join("unavailable");
    std::fs::rename(&root, &moved).unwrap();
    run(&conn, &store, &mut state, None);
    assert!(crate::session_locations(&conn, ID, SID)
        .unwrap()
        .contains(&"local".into()));
    std::fs::rename(&moved, &root).unwrap();
    std::fs::remove_dir_all(path.parent().unwrap()).unwrap();
    let scope = PathScope::from_paths([path.clone()]).unwrap();
    run(&conn, &store, &mut state, Some(&scope));
    assert!(crate::session_locations(&conn, ID, SID)
        .unwrap()
        .contains(&"local".into()));
    run(&conn, &store, &mut state, None);
    assert!(crate::session_locations(&conn, ID, SID).unwrap().is_empty());
    assert_eq!(
        count(
            &conn,
            "SELECT count(*) FROM sessions WHERE source='copilot'"
        ),
        1
    );
    assert!(other.exists());
}
