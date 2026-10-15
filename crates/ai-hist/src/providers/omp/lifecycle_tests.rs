use super::*;
const FIXTURE: &str = include_str!(
    "../../../tests/fixtures/omp/title-v3/.omp/agent/sessions/--fixture--/session.jsonl"
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
fn omp_scoped_tick_skips_sibling_and_full_backstop_preserves_it() {
    let (_temp, store, path) = fixture();
    let sibling = path.with_file_name("sibling.jsonl");
    std::fs::write(
        &sibling,
        FIXTURE.replace("019b76da-a800-7000-8000-000000000001", "other-session"),
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
                &crate::SessionRef::id(crate::Source::Omp, id),
                crate::SessionQuery::default(),
            )
            .unwrap()
    };
    assert!(read("019b76da-a800-7000-8000-000000000001").is_some());
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
fn omp_rewrite_retires_stale_tools_and_clears_removed_result_status() {
    let (_temp, store, path) = fixture();
    let conn = crate::open_db(store.db_path()).unwrap();
    let ctx = AcquisitionContext::new(ID, store.roots());
    let capture = snapshot(&path, &ctx).unwrap();
    normalize::write(&conn, &capture.payload, &path).unwrap();
    let mut payload = capture.payload;
    payload["entries"]
        .as_array_mut()
        .unwrap()
        .retain(|e| e["id"] != "00000003");
    normalize::write(&conn, &payload, &path).unwrap();
    let error: Option<bool> = conn
        .query_row(
            "SELECT is_error FROM tool_calls WHERE source='omp'",
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
            "SELECT count(*) FROM tool_calls WHERE source='omp'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(calls, 0);
}

#[test]
fn omp_fork_shared_multiblock_prompt_survives_original_rewrite() {
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
        .retain(|e| e["id"] != "00000001");
    normalize::write(&conn, &payload, &path).unwrap();
    let owner:String=conn.query_row("SELECT session_id FROM history WHERE source='omp' AND prompt = 'part one'||char(10)||'part two'",[],|r|r.get(0)).unwrap();
    assert_eq!(owner, "another-session");
    let project: Option<String> = conn
        .query_row(
            "SELECT project FROM history WHERE source='omp' AND session_id='another-session'",
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
fn omp_rewrite_preserves_prompt_backed_by_same_session_remote_events() {
    let (_temp, store, path) = fixture();
    let conn = crate::open_db(store.db_path()).unwrap();
    let mut payload = snapshot(&path, &AcquisitionContext::new(ID, store.roots()))
        .unwrap()
        .payload;
    payload["entries"][1]["message"]["content"] = serde_json::json!([{"type":"text","text":"remote first"},{"type":"text","text":"remote second"}]);
    normalize::write(&conn, &payload, &path).unwrap();
    conn.execute("UPDATE session_events SET location = 'both' WHERE source = 'omp' AND message_id = '00000001'", []).unwrap();
    payload["entries"]
        .as_array_mut()
        .unwrap()
        .retain(|e| e["message"]["role"] != "user");
    normalize::write(&conn, &payload, &path).unwrap();
    let remote: i64 = conn.query_row("SELECT count(*) FROM session_events WHERE source = 'omp' AND message_id = '00000001' AND location = 'remote'", [], |r| r.get(0)).unwrap();
    assert_eq!(
        remote, 2,
        "local retirement must preserve both remote text blocks"
    );
    let prompts: Vec<(String, String)> = conn
        .prepare("SELECT session_id, prompt FROM history WHERE source = 'omp'")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(
        prompts,
        vec![(
            "019b76da-a800-7000-8000-000000000001".into(),
            "remote first\nremote second".into()
        )],
        "same-session remote prompt survives; local-only prompt is retired"
    );
    normalize::write(&conn, &payload, &path).unwrap();
    let remaining: i64 = conn
        .query_row(
            "SELECT count(*) FROM history WHERE source = 'omp'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        remaining, 1,
        "unchanged replay preserves remote prompt ownership"
    );
    conn.execute(
        "DELETE FROM session_events WHERE source = 'omp' AND message_id = '00000001'",
        [],
    )
    .unwrap();
    normalize::write(&conn, &payload, &path).unwrap();
    let remaining: i64 = conn
        .query_row(
            "SELECT count(*) FROM history WHERE source = 'omp'",
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
fn omp_file_cache_skips_unchanged_and_refreshes_generation() {
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
        "UPDATE sessions SET last_assistant_text='sentinel' WHERE source='omp'",
        [],
    )
    .unwrap();
    run(&mut state);
    let preview = || {
        conn.query_row(
            "SELECT last_assistant_text FROM sessions WHERE source='omp'",
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
    state["omp_file_stamps"][path.to_string_lossy().as_ref()]["generation"] = "old".into();
    run(&mut state);
    assert_ne!(
        preview(),
        "sentinel",
        "generation drift forces unchanged bytes through normalizer"
    );
}

#[test]
#[cfg(unix)]
fn omp_denied_root_is_reported_and_blob_references_are_not_opened() {
    use std::os::unix::fs::PermissionsExt;
    let (_home, store, path) = fixture();
    let context = AcquisitionContext::new(ID, store.roots());
    let capture = snapshot(&path, &context).unwrap();
    assert_eq!(capture.read_paths, vec![path.clone()]);
    assert!(capture.payload.to_string().contains("blob:sha256:"));
    let root = path.parent().unwrap();
    let permissions = std::fs::metadata(root).unwrap().permissions();
    std::fs::set_permissions(root, std::fs::Permissions::from_mode(0)).unwrap();
    let conn = crate::open_db(store.db_path()).unwrap();
    let env = DiscoveryEnv::with_provider_roots(&conn, store.roots().clone());
    let result = Provider.enumerate(&env, None);
    std::fs::set_permissions(root, permissions).unwrap();
    assert!(
        result.is_err(),
        "unreadable OMP root cannot be a successful empty scan"
    );
}
