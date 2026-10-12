use super::*;
#[test]
fn strict_versions_and_tail() {
    assert!(
        format::parse(b"{\"type\":\"session\",\"version\":4}\n", false)
            .unwrap_err()
            .to_string()
            .contains("UNSUPPORTED_SCHEMA")
    );
    assert!(format::parse(b"{", true)
        .unwrap_err()
        .to_string()
        .contains("INCOMPLETE_HEADER"));
}
#[test]
fn usage_is_measured_and_partial() {
    let u = usage::normalize(
        &serde_json::json!({"input":10,"output":2,"cacheRead":3,"cacheWrite":4,"totalTokens":19}),
    )
    .unwrap()
    .unwrap();
    assert_eq!(u.input_tokens, 10);
    assert_eq!(u.cache_read_tokens, 3);
    assert!(u.coverage.is_complete());
    assert!(usage::normalize(&serde_json::json!({"input":-1})).is_err());
    assert!(usage::normalize(&serde_json::json!({"tokensBefore":1000}))
        .unwrap()
        .is_none());
}

const FIXTURE: &str = include_str!(
    "../../../tests/fixtures/pi/writer-v3/.pi/agent/sessions/--fixture--/session.jsonl"
);
fn fixture() -> (tempfile::TempDir, crate::SessionStore, PathBuf) {
    let temp = tempfile::tempdir().unwrap();
    let roots = ProviderRoots::from_home(temp.path().into(), temp.path().join("opencode.db"));
    let path = roots.for_provider(ID)[0].join("session.jsonl");
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
#[test]
fn pi_selected_snapshot_is_immutable_and_prefix_bounded() {
    let (_temp, store, path) = fixture();
    let ctx = AcquisitionContext::new(ID, store.roots());
    let captured = snapshot(&path, &ctx).unwrap();
    std::fs::write(&path, FIXTURE.replace("Hello fixture", "Changed on disk")).unwrap();
    let conn = crate::open_db(store.db_path()).unwrap();
    normalize::write(&conn, &captured.payload, &path).unwrap();
    let e = store
        .session(
            &crate::SessionRef::id(crate::Source::Pi, "custom-history-session"),
            crate::SessionQuery::default(),
        )
        .unwrap()
        .unwrap();
    assert!(e
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .any(|b| b.text.as_deref() == Some("The greeting says Hello fixture.")));
    std::fs::write(&path, vec![b'a'; 128 * 1024]).unwrap();
    assert_eq!(prefix(&ctx, &path).unwrap().len(), HEAD_LIMIT as usize);
    assert!(format::parse(&prefix(&ctx, &path).unwrap(), true)
        .unwrap_err()
        .to_string()
        .contains("INCOMPLETE_HEADER"));
}
#[test]
fn pi_scoped_tick_skips_sibling_and_full_backstop_preserves_it() {
    let (_temp, store, path) = fixture();
    let sibling = path.with_file_name("sibling.jsonl");
    std::fs::write(
        &sibling,
        FIXTURE.replace("custom-history-session", "other-session"),
    )
    .unwrap();
    let conn = crate::open_db(store.db_path()).unwrap();
    let scope = crate::ingest::SweepScope::only([ID]).with_paths(
        crate::ingest::path_scope::PathScope::from_paths([path.clone()]),
    );
    crate::ingest::sync_basic(&conn, store.db_path(), store.roots(), true, &scope).unwrap();
    let read = |id| {
        store
            .session(
                &crate::SessionRef::id(crate::Source::Pi, id),
                crate::SessionQuery::default(),
            )
            .unwrap()
    };
    assert!(read("custom-history-session").is_some());
    assert!(read("other-session").is_none());
    crate::ingest::sync_basic(
        &conn,
        store.db_path(),
        store.roots(),
        true,
        &crate::ingest::SweepScope::only([ID]),
    )
    .unwrap();
    assert!(read("other-session").is_some());
    std::fs::write(sibling, "not readable Pi").unwrap();
    crate::ingest::sync_basic(&conn, store.db_path(), store.roots(), true, &scope).unwrap();
    assert!(read("other-session").is_some());
}
#[test]
fn pi_explicit_roots_and_legacy_serialized_defaults() {
    let (temp, store, _) = fixture();
    let custom = temp.path().join("explicit");
    std::fs::create_dir(&custom).unwrap();
    std::fs::write(custom.join("custom.jsonl"), FIXTURE).unwrap();
    let mut json = serde_json::to_value(store.roots()).unwrap();
    json["provider_roots"]["pi"] = serde_json::json!([custom]);
    let roots: ProviderRoots = serde_json::from_value(json.clone()).unwrap();
    assert_eq!(roots.for_provider(ID), &[custom]);
    let conn = crate::open_db(store.db_path()).unwrap();
    let env = DiscoveryEnv::with_provider_roots(&conn, roots);
    assert_eq!(Provider.enumerate(&env, None).unwrap().len(), 1);
    json.as_object_mut().unwrap().remove("provider_roots");
    let legacy: ProviderRoots = serde_json::from_value(json).unwrap();
    assert_eq!(
        legacy.for_provider(ID),
        &[temp.path().join(".pi/agent/sessions")]
    );
    assert_eq!(expand(temp.path(), "~/custom"), temp.path().join("custom"));
}
#[test]
#[cfg(unix)]
fn pi_denied_root_is_error_and_parent_or_spill_paths_are_not_followed() {
    use std::os::unix::fs::PermissionsExt;
    let (_temp, store, path) = fixture();
    let root = path.parent().unwrap();
    let conn = crate::open_db(store.db_path()).unwrap();
    let env = DiscoveryEnv::with_provider_roots(&conn, store.roots().clone());
    std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o000)).unwrap();
    let result = Provider.enumerate(&env, None);
    std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(result.is_err());
    let mut lines: Vec<Value> = FIXTURE
        .lines()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();
    lines[0]["parentSession"] = "/private/not-authorized.jsonl".into();
    lines[2]["message"]["content"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({"type":"image","path":"/private/spill.png"}));
    std::fs::write(
        &path,
        lines.iter().map(|v| format!("{v}\n")).collect::<String>(),
    )
    .unwrap();
    let acquired = snapshot(&path, &AcquisitionContext::new(ID, store.roots())).unwrap();
    assert_eq!(acquired.read_paths, vec![path.clone()]);
    normalize::write(&conn, &acquired.payload, &path).unwrap();
}

#[test]
fn pi_root_precedence_and_partial_tail_prevent_pruning() {
    let home = Path::new("/isolated");
    assert_eq!(
        roots_from_values(home, Some("~/sessions"), Some("~/agent")),
        vec![home.join("sessions")]
    );
    assert_eq!(
        roots_from_values(home, None, Some("~/agent")),
        vec![home.join("agent/sessions")]
    );
    let (_temp, store, path) = fixture();
    let conn = crate::open_db(store.db_path()).unwrap();
    let ctx = AcquisitionContext::new(ID, store.roots());
    let original = snapshot(&path, &ctx).unwrap();
    normalize::write(&conn, &original.payload, &path).unwrap();
    let before: (i64, Option<String>) = conn
        .query_row(
            "SELECT last_activity_ms,last_assistant_text FROM sessions WHERE source='pi'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    let partial = FIXTURE
        .lines()
        .take(3)
        .map(|l| format!("{l}\n"))
        .collect::<String>()
        + "{partial";
    std::fs::write(&path, partial).unwrap();
    let capture = snapshot(&path, &ctx).unwrap();
    normalize::write(&conn, &capture.payload, &path).unwrap();
    let after: (i64, Option<String>) = conn
        .query_row(
            "SELECT last_activity_ms,last_assistant_text FROM sessions WHERE source='pi'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(
        before, after,
        "incomplete rewrite preserves catalog end and preview"
    );
    let count: i64 = conn
        .query_row(
            "SELECT count(*) FROM session_events WHERE source='pi' AND message_id='entry-7'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        count, 1,
        "partial rewrite must not retire prior complete rows"
    );
}
#[test]
fn pi_rewrite_retires_stale_tools_and_clears_removed_result_status() {
    let (_temp, store, path) = fixture();
    let conn = crate::open_db(store.db_path()).unwrap();
    let ctx = AcquisitionContext::new(ID, store.roots());
    let capture = snapshot(&path, &ctx).unwrap();
    normalize::write(&conn, &capture.payload, &path).unwrap();
    let mut payload = capture.payload;
    payload["entries"]
        .as_array_mut()
        .unwrap()
        .retain(|e| e["id"] != "entry-3");
    normalize::write(&conn, &payload, &path).unwrap();
    let error: Option<bool> = conn
        .query_row(
            "SELECT is_error FROM tool_calls WHERE source='pi'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(error, None);
    let blocks = payload["entries"][2]["message"]["content"]
        .as_array_mut()
        .unwrap();
    blocks.retain(|b| b["type"] != "toolCall");
    normalize::write(&conn, &payload, &path).unwrap();
    let calls: i64 = conn
        .query_row(
            "SELECT count(*) FROM tool_calls WHERE source='pi'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(calls, 0);
}

#[test]
fn pi_fork_shared_multiblock_prompt_survives_original_rewrite() {
    let (_temp, store, path) = fixture();
    let conn = crate::open_db(store.db_path()).unwrap();
    let mut payload = snapshot(&path, &AcquisitionContext::new(ID, store.roots()))
        .unwrap()
        .payload;
    payload["entries"][1]["message"]["content"] =
        serde_json::json!([{"type":"text","text":"part one"},{"type":"text","text":"part two"}]);
    normalize::write(&conn, &payload, &path).unwrap();
    let mut fork = payload.clone();
    fork["entries"][0]["id"] = "another-session".into();
    fork["entries"][0]["cwd"] = "/fork/project".into();
    normalize::write(&conn, &fork, &path.with_file_name("fork.jsonl")).unwrap();
    payload["entries"]
        .as_array_mut()
        .unwrap()
        .retain(|e| e["id"] != "entry-1");
    normalize::write(&conn, &payload, &path).unwrap();
    let owner:String=conn.query_row("SELECT session_id FROM history WHERE source='pi' AND prompt = 'part one'||char(10)||'part two'",[],|r|r.get(0)).unwrap();
    assert_eq!(owner, "another-session");
    let project: Option<String> = conn
        .query_row(
            "SELECT project FROM history WHERE source='pi' AND session_id='another-session'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(project.as_deref(), Some("/fork/project"));
    let found: i64 = conn.query_row("SELECT count(*) FROM history_fts f JOIN history h ON h.id=f.rowid WHERE history_fts MATCH 'project' AND h.project='/fork/project' AND h.session_id='another-session'", [], |r| r.get(0)).unwrap();
    assert_eq!(
        found, 1,
        "project reassignment refreshes the searchable project column"
    );
}

#[test]
fn pi_rewrite_preserves_prompt_backed_by_same_session_remote_events() {
    let (_temp, store, path) = fixture();
    let conn = crate::open_db(store.db_path()).unwrap();
    let mut payload = snapshot(&path, &AcquisitionContext::new(ID, store.roots()))
        .unwrap()
        .payload;
    payload["entries"][1]["message"]["content"] = serde_json::json!([{"type":"text","text":"remote first"},{"type":"text","text":"remote second"}]);
    normalize::write(&conn, &payload, &path).unwrap();
    conn.execute("UPDATE session_events SET location = 'both' WHERE source = 'pi' AND message_id = 'entry-1'", []).unwrap();
    payload["entries"]
        .as_array_mut()
        .unwrap()
        .retain(|e| e["message"]["role"] != "user");
    normalize::write(&conn, &payload, &path).unwrap();
    let remote: i64 = conn.query_row("SELECT count(*) FROM session_events WHERE source = 'pi' AND message_id = 'entry-1' AND location = 'remote'", [], |r| r.get(0)).unwrap();
    assert_eq!(
        remote, 2,
        "local retirement must preserve both remote text blocks"
    );
    let prompts: Vec<(String, String)> = conn
        .prepare("SELECT session_id, prompt FROM history WHERE source = 'pi'")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(
        prompts,
        vec![(
            "custom-history-session".into(),
            "remote first\nremote second".into()
        )],
        "same-session remote prompt survives; local-only prompt is retired"
    );
    normalize::write(&conn, &payload, &path).unwrap();
    let remaining: i64 = conn
        .query_row(
            "SELECT count(*) FROM history WHERE source = 'pi'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        remaining, 1,
        "unchanged replay preserves remote prompt ownership"
    );
    conn.execute(
        "DELETE FROM session_events WHERE source = 'pi' AND message_id = 'entry-1'",
        [],
    )
    .unwrap();
    normalize::write(&conn, &payload, &path).unwrap();
    let remaining: i64 = conn
        .query_row(
            "SELECT count(*) FROM history WHERE source = 'pi'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        remaining, 0,
        "prompt retires once no finalized user projection supports it"
    );
}

#[test]
fn pi_bad_siblings_do_not_poison_discovery_or_sweep() {
    let (_temp, store, path) = fixture();
    for (name, bytes) in [
        ("invalid", "{bad}\n"),
        ("unsupported", "{\"type\":\"session\",\"version\":4}\n"),
        ("partial", "{"),
    ] {
        std::fs::write(path.with_file_name(format!("{name}.jsonl")), bytes).unwrap();
    }
    let conn = crate::open_db(store.db_path()).unwrap();
    let env = DiscoveryEnv::with_provider_roots(&conn, store.roots().clone());
    let candidates = Provider.enumerate(&env, None).unwrap();
    assert_eq!(candidates.len(), 4);
    assert_eq!(env.counters().files_opened, 0);
    let discovered = store
        .discover(crate::DiscoveryOptions::default().sources([crate::Source::Pi]))
        .unwrap();
    assert_eq!(discovered.discovered, 1);
    assert_eq!(discovered.diagnostics.len(), 3);
    assert!(discovered.diagnostics.iter().all(|d| d.subject.is_some()));
    let mut state = Map::new();
    let result = sweep(SweepContext {
        conn: &conn,
        state: &mut state,
        roots: store.roots(),
        paths: None,
        repairs: &Default::default(),
    })
    .unwrap();
    assert_eq!(result.unread, 3);
    let sessions: i64 = conn
        .query_row("SELECT count(*) FROM sessions WHERE source='pi'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(sessions, 1);
}

#[test]
fn pi_file_cache_skips_unchanged_and_refreshes_generation() {
    let (_temp, store, path) = fixture();
    let conn = crate::open_db(store.db_path()).unwrap();
    let mut state = Map::new();
    let run = |state: &mut Map<String, Value>| {
        sweep(SweepContext {
            conn: &conn,
            state,
            roots: store.roots(),
            paths: None,
            repairs: &Default::default(),
        })
        .unwrap()
    };
    run(&mut state);
    conn.execute(
        "UPDATE sessions SET last_assistant_text='sentinel' WHERE source='pi'",
        [],
    )
    .unwrap();
    run(&mut state);
    let preview = || {
        conn.query_row(
            "SELECT last_assistant_text FROM sessions WHERE source='pi'",
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
    state["pi_file_stamps"][path.to_string_lossy().as_ref()]["generation"] = "old".into();
    run(&mut state);
    assert_ne!(
        preview(),
        "sentinel",
        "generation drift forces unchanged bytes through normalizer"
    );
}

#[test]
fn pi_tool_identity_and_readable_preview_survive_empty_assistants() {
    let (_temp, store, path) = fixture();
    let conn = crate::open_db(store.db_path()).unwrap();
    let mut payload = snapshot(&path, &AcquisitionContext::new(ID, store.roots()))
        .unwrap()
        .payload;
    payload["entries"].as_array_mut().unwrap().push(serde_json::json!({"type":"message", "id":"empty", "parentId":null,"timestamp":"2023-11-14T22:13:50Z", "message":{"role":"assistant","content":[],"timestamp":1700000030000_i64}}));
    payload["entries"].as_array_mut().unwrap().push(serde_json::json!({"type":"message", "id":"tool-only", "parentId":"empty","timestamp":"2023-11-14T22:13:51Z", "message":{"role":"assistant","content":[{"type":"toolCall","id":"call-late","name":"read","arguments":{"path":"later.txt"}}],"timestamp":1700000031000_i64}}));
    normalize::write(&conn, &payload, &path).unwrap();
    let identity: String = conn
        .query_row(
            "SELECT tool_use_id FROM session_events WHERE source='pi' AND kind='tool_use'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(identity, "call-fixture");
    let evidence = store
        .session(
            &crate::SessionRef::id(crate::Source::Pi, "custom-history-session"),
            crate::SessionQuery::default(),
        )
        .unwrap()
        .unwrap();
    let ids: Vec<_> = evidence
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .filter(|block| block.kind == crate::BlockKind::ToolUse)
        .map(|block| block.tool_use_id.as_deref().unwrap())
        .collect();
    assert_eq!(ids, vec!["call-fixture", "call-late"]);
    let calls: Vec<String> = conn
        .prepare("SELECT tool_use_id FROM tool_calls WHERE source='pi' ORDER BY ts_ms")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(ids, calls.iter().map(String::as_str).collect::<Vec<_>>());
    let preview: Option<String> = conn
        .query_row(
            "SELECT last_assistant_text FROM sessions WHERE source='pi'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        preview.as_deref(),
        Some("This answer is on the alternate branch.")
    );
    for entry in payload["entries"].as_array_mut().unwrap() {
        if entry["message"]["role"] == "assistant" {
            entry["message"]["content"] = serde_json::json!([]);
        }
    }
    normalize::write(&conn, &payload, &path).unwrap();
    let preview: Option<String> = conn
        .query_row(
            "SELECT last_assistant_text FROM sessions WHERE source='pi'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        preview, None,
        "complete rewrite clears removed readable prose"
    );
}

#[test]
fn pi_cache_preserves_unvisited_files_and_partial_tails_retry() {
    let (_temp, store, path) = fixture();
    let sibling = path.with_file_name("sibling.jsonl");
    std::fs::write(
        &sibling,
        FIXTURE.replace("custom-history-session", "sibling-session"),
    )
    .unwrap();
    let conn = crate::open_db(store.db_path()).unwrap();
    let mut state = Map::new();
    let run = |state: &mut Map<String, Value>, paths| {
        sweep(SweepContext {
            conn: &conn,
            state,
            roots: store.roots(),
            paths,
            repairs: &Default::default(),
        })
        .unwrap()
    };
    run(&mut state, None);
    let previous = state["pi_file_stamps"][sibling.to_string_lossy().as_ref()].clone();
    std::fs::write(
        &path,
        format!(
            "{}{{",
            FIXTURE.replace("custom-history-session", "partial-sibling")
        ),
    )
    .unwrap();
    let scope = crate::ingest::path_scope::PathScope::from_paths([path.clone()]);
    assert_eq!(run(&mut state, scope.as_ref()).unread, 1);
    assert_eq!(
        state["pi_file_stamps"][sibling.to_string_lossy().as_ref()],
        previous
    );
    assert!(state["pi_file_stamps"]
        .get(path.to_string_lossy().as_ref())
        .is_none());
    assert_eq!(
        run(&mut state, scope.as_ref()).unread,
        1,
        "partial tail remains retryable even without file changes"
    );
}

#[test]
fn pi_unchanged_sync_repairs_lost_events_despite_file_cache() {
    let (_temp, store, _path) = fixture();
    let conn = crate::open_db(store.db_path()).unwrap();
    let scope = crate::ingest::SweepScope::everything();
    crate::ingest::sync_basic(&conn, store.db_path(), store.roots(), true, &scope).unwrap();
    let count = || {
        conn.query_row(
            "SELECT count(*) FROM session_events WHERE source='pi'",
            [],
            |r| r.get::<_, i64>(0),
        )
        .unwrap()
    };
    let original = count();
    conn.execute(
        "DELETE FROM session_events WHERE source='pi' AND role='assistant'",
        [],
    )
    .unwrap();
    assert!(count() < original);
    crate::ingest::sync_basic(&conn, store.db_path(), store.roots(), true, &scope).unwrap();
    assert_eq!(count(), original);
}

#[test]
fn pi_reassigned_prompt_without_cwd_clears_original_project() {
    let (_temp, store, path) = fixture();
    let conn = crate::open_db(store.db_path()).unwrap();
    let mut payload = snapshot(&path, &AcquisitionContext::new(ID, store.roots()))
        .unwrap()
        .payload;
    normalize::write(&conn, &payload, &path).unwrap();
    let mut fork = payload.clone();
    fork["entries"][0]["id"] = "no-cwd".into();
    fork["entries"][0].as_object_mut().unwrap().remove("cwd");
    let fork_path = path.with_file_name("no-cwd.jsonl");
    // Simulate surviving destination evidence without a known project.
    normalize::write(&conn, &fork, &fork_path).unwrap();
    payload["entries"]
        .as_array_mut()
        .unwrap()
        .retain(|entry| entry["message"]["role"] != "user");
    normalize::write(&conn, &payload, &path).unwrap();
    let rows: Vec<(String, Option<String>)> = conn
        .prepare("SELECT session_id, project FROM history WHERE source='pi'")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert!(!rows.is_empty());
    assert!(rows
        .iter()
        .all(|(sid, project)| sid == "no-cwd" && project.is_none()));
}

#[test]
fn pi_retained_prompt_project_tracks_cwd_and_shared_owner() {
    let (_temp, store, path) = fixture();
    let conn = crate::open_db(store.db_path()).unwrap();
    let mut payload = snapshot(&path, &AcquisitionContext::new(ID, store.roots()))
        .unwrap()
        .payload;
    normalize::write(&conn, &payload, &path).unwrap();
    let mut fork = payload.clone();
    fork["entries"][0]["id"] = "fork-owner".into();
    fork["entries"][0]["cwd"] = "/fork/retained".into();
    normalize::write(&conn, &fork, &path.with_file_name("fork.jsonl")).unwrap();
    payload["entries"][0]["cwd"] = "/new/retained".into();
    normalize::write(&conn, &payload, &path).unwrap();
    let stale: i64 = conn
        .query_row(
            "SELECT count(*) FROM history WHERE source='pi' AND project IS NOT '/new/retained'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(stale, 0);
    let found: i64 = conn.query_row("SELECT count(*) FROM history_fts f JOIN history h ON h.id=f.rowid WHERE history_fts MATCH 'retained' AND h.project='/new/retained'", [], |r| r.get(0)).unwrap();
    assert!(found > 0);
    normalize::write(&conn, &fork, &path.with_file_name("fork.jsonl")).unwrap();
    let stolen: i64 = conn
        .query_row(
            "SELECT count(*) FROM history WHERE source='pi' AND session_id='fork-owner'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(stolen, 0);
}

#[test]
fn pi_complete_rewrite_refreshes_and_clears_first_prompt_but_partial_preserves_it() {
    let (_temp, store, path) = fixture();
    store
        .discover(crate::DiscoveryOptions::default().sources([crate::Source::Pi]))
        .unwrap();
    let conn = crate::open_db(store.db_path()).unwrap();
    let mut payload = snapshot(&path, &AcquisitionContext::new(ID, store.roots()))
        .unwrap()
        .payload;
    normalize::write(&conn, &payload, &path).unwrap();
    let read = || {
        conn.query_row("SELECT first_prompt FROM sessions WHERE source='pi' AND session_id='custom-history-session'", [], |r| r.get::<_,Option<String>>(0)).unwrap()
    };
    assert!(read().is_some());
    payload["entries"][1]["message"]["content"] = "Replacement first prompt".into();
    normalize::write(&conn, &payload, &path).unwrap();
    assert_eq!(read().as_deref(), Some("Replacement first prompt"));
    payload["entries"]
        .as_array_mut()
        .unwrap()
        .retain(|e| e["message"]["role"] != "user");
    payload["tail"] = serde_json::json!([123]);
    normalize::write(&conn, &payload, &path).unwrap();
    assert_eq!(read().as_deref(), Some("Replacement first prompt"));
    payload["tail"] = serde_json::json!([]);
    normalize::write(&conn, &payload, &path).unwrap();
    assert_eq!(read(), None);
}

#[test]
fn pi_shallow_catalog_carries_observed_activity_without_full_ingest() {
    let (_temp, store, path) = fixture();
    let conn = crate::open_db(store.db_path()).unwrap();
    let env = DiscoveryEnv::with_provider_roots(&conn, store.roots().clone());
    let candidate = Provider.enumerate(&env, None).unwrap().remove(0);
    let shallow = Provider
        .read_shallow(&env.scan(), Some(&conn), &candidate)
        .unwrap()
        .unwrap();
    let entries = format::parse(FIXTURE.as_bytes(), true).unwrap();
    let latest = entries
        .iter()
        .map(|e| format::time(e).unwrap())
        .max()
        .unwrap();
    assert_eq!(shallow.last_activity_ms, Some(latest));
    store
        .discover(crate::DiscoveryOptions::default().sources([crate::Source::Pi]))
        .unwrap();
    let activity: Option<i64> = conn
        .query_row(
            "SELECT last_activity_ms FROM sessions WHERE source='pi'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(activity, Some(latest));
    assert!(path.exists());
}

#[test]
fn pi_deleted_locator_retires_only_after_accessible_full_sweep() {
    let (temp, store, path) = fixture();
    let sibling = path.with_file_name("sibling.jsonl");
    std::fs::write(
        &sibling,
        FIXTURE.replace("custom-history-session", "sibling-session"),
    )
    .unwrap();
    let conn = crate::open_db(store.db_path()).unwrap();
    let mut state = Map::new();
    let repairs = Default::default();
    let run = |state: &mut Map<String, Value>, paths| {
        sweep(SweepContext {
            conn: &conn,
            state,
            roots: store.roots(),
            paths,
            repairs: &repairs,
        })
    };
    run(&mut state, None).unwrap();
    conn.execute("UPDATE session_events SET location='both' WHERE source='pi' AND session_id='custom-history-session' AND role='user'", []).unwrap();
    crate::mark_session_presence(
        &conn,
        ID,
        "custom-history-session",
        crate::SessionLocation::Remote,
    )
    .unwrap();
    let remote_only_path = path.to_string_lossy().into_owned();
    conn.execute("INSERT INTO sessions(source,session_id,raw_path,first_prompt) VALUES ('pi','remote-only',?1,'Independent remote preview')", [&remote_only_path]).unwrap();
    crate::mark_session_presence(&conn, ID, "remote-only", crate::SessionLocation::Remote).unwrap();
    let root = store.roots().for_provider(ID)[0].clone();
    let moved = temp.path().join("unavailable");
    std::fs::rename(&root, &moved).unwrap();
    run(&mut state, None).unwrap();
    assert!(
        crate::session_locations(&conn, ID, "custom-history-session")
            .unwrap()
            .contains(&"local".into())
    );
    std::fs::rename(&moved, &root).unwrap();
    std::fs::rename(&root, &moved).unwrap();
    std::fs::write(&root, b"root temporarily replaced by a file").unwrap();
    run(&mut state, None).unwrap();
    assert!(crate::session_locations(&conn, ID, "sibling-session")
        .unwrap()
        .contains(&"local".into()));
    std::fs::remove_file(&root).unwrap();
    std::fs::rename(&moved, &root).unwrap();
    state.clear(); // Durable catalog lookup must work without file-stamp cache.
    std::fs::remove_file(&path).unwrap();
    std::fs::remove_file(&sibling).unwrap();
    let bad = path.with_file_name("bad.jsonl");
    for bytes in [
        "{bad}\n".to_owned(),
        format!(
            "{}{{",
            FIXTURE.replace("custom-history-session", "partial-sibling")
        ),
    ] {
        std::fs::write(&bad, bytes).unwrap();
        let result = run(&mut state, None).unwrap();
        assert!(result.unread > 0);
        assert!(crate::session_locations(&conn, ID, "sibling-session")
            .unwrap()
            .contains(&"local".into()));
    }
    std::fs::remove_file(&bad).unwrap();
    let scope = crate::ingest::path_scope::PathScope::from_paths([path.clone()]).unwrap();
    run(&mut state, Some(&scope)).unwrap();
    assert!(
        crate::session_locations(&conn, ID, "custom-history-session")
            .unwrap()
            .contains(&"local".into())
    );
    assert!(crate::session_locations(&conn, ID, "sibling-session")
        .unwrap()
        .contains(&"local".into()));
    run(&mut state, None).unwrap();
    assert_eq!(
        crate::session_locations(&conn, ID, "custom-history-session").unwrap(),
        ["remote"]
    );
    let rows: i64 = conn
        .query_row("SELECT count(*) FROM sessions WHERE source='pi'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(rows, 2);
    let remote: i64 = conn.query_row("SELECT count(*) FROM history WHERE source='pi' AND session_id='custom-history-session'", [], |r| r.get(0)).unwrap();
    assert!(remote > 0);
    let local: i64 = conn
        .query_row(
            "SELECT count(*) FROM session_events WHERE source='pi' AND location != 'remote'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(local, 0);
    let projection: (Option<String>, Option<String>) = conn
        .query_row(
            "SELECT raw_path,last_assistant_text FROM sessions WHERE source='pi' AND session_id='custom-history-session'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(projection, (None, None));
    let independent: (String, String) = conn.query_row("SELECT raw_path,first_prompt FROM sessions WHERE source='pi' AND session_id='remote-only'", [], |r| Ok((r.get(0)?,r.get(1)?))).unwrap();
    assert_eq!(
        independent,
        (remote_only_path, "Independent remote preview".into())
    );

    let stale: i64 = conn.query_row("SELECT count(*) FROM history_fts f JOIN history h ON h.id=f.rowid WHERE history_fts MATCH 'greeting' AND h.source='pi' AND h.session_id='sibling-session'", [], |r| r.get(0)).unwrap();
    assert_eq!(stale, 0);
}

#[test]
fn pi_moved_locator_and_new_fork_preserve_prompt_identity_before_deletion() {
    let (_temp, store, path) = fixture();
    let conn = crate::open_db(store.db_path()).unwrap();
    let mut state = Map::new();
    let repairs = Default::default();
    let run = |state: &mut Map<String, Value>| {
        sweep(SweepContext {
            conn: &conn,
            state,
            roots: store.roots(),
            paths: None,
            repairs: &repairs,
        })
        .unwrap()
    };
    run(&mut state);
    let first: i64 = conn
        .query_row("SELECT min(id) FROM history WHERE source='pi'", [], |r| {
            r.get(0)
        })
        .unwrap();
    let moved = path.with_file_name("moved.jsonl");
    std::fs::rename(&path, &moved).unwrap();
    run(&mut state);
    let raw: String = conn
        .query_row("SELECT raw_path FROM sessions WHERE source='pi'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(raw, moved.to_string_lossy());
    let fork = path.with_file_name("fork.jsonl");
    std::fs::write(
        &fork,
        FIXTURE
            .replace("custom-history-session", "surviving-fork")
            .replace("/fixture/project", "/fork/final"),
    )
    .unwrap();
    std::fs::remove_file(&moved).unwrap();
    run(&mut state);
    let rows: i64 = conn
        .query_row("SELECT count(*) FROM sessions WHERE source='pi'", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(rows, 1);
    let owner: (String, Option<String>) = conn
        .query_row(
            "SELECT session_id,project FROM history WHERE id=?1",
            [first],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(owner, ("surviving-fork".into(), Some("/fork/final".into())));
}
