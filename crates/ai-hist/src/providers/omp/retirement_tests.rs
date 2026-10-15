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
fn omp_deleted_locator_retires_only_after_accessible_full_sweep() {
    let (temp, store, path) = fixture();
    let sibling = path.with_file_name("sibling.jsonl");
    std::fs::write(
        &sibling,
        FIXTURE.replace("019b76da-a800-7000-8000-000000000001", "sibling-session"),
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
    conn.execute("UPDATE session_events SET location='both' WHERE source='omp' AND session_id='019b76da-a800-7000-8000-000000000001' AND role='user'", []).unwrap();
    crate::mark_session_presence(
        &conn,
        ID,
        "019b76da-a800-7000-8000-000000000001",
        crate::SessionLocation::Remote,
    )
    .unwrap();
    let remote_only_path = path.to_string_lossy().into_owned();
    conn.execute("INSERT INTO sessions(source,session_id,raw_path,first_prompt) VALUES ('omp','remote-only',?1,'Independent remote preview')", [&remote_only_path]).unwrap();
    crate::mark_session_presence(&conn, ID, "remote-only", crate::SessionLocation::Remote).unwrap();
    let root = store.roots().for_provider(ID)[0].clone();
    let moved = temp.path().join("unavailable");
    std::fs::rename(&root, &moved).unwrap();
    run(&mut state, None).unwrap();
    assert!(
        crate::session_locations(&conn, ID, "019b76da-a800-7000-8000-000000000001")
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
            FIXTURE.replace("019b76da-a800-7000-8000-000000000001", "partial-sibling")
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
        crate::session_locations(&conn, ID, "019b76da-a800-7000-8000-000000000001")
            .unwrap()
            .contains(&"local".into())
    );
    assert!(crate::session_locations(&conn, ID, "sibling-session")
        .unwrap()
        .contains(&"local".into()));
    run(&mut state, None).unwrap();
    assert_eq!(
        crate::session_locations(&conn, ID, "019b76da-a800-7000-8000-000000000001").unwrap(),
        ["remote"]
    );
    let rows: i64 = conn
        .query_row(
            "SELECT count(*) FROM sessions WHERE source='omp'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(rows, 2);
    let remote: i64 = conn.query_row("SELECT count(*) FROM history WHERE source='omp' AND session_id='019b76da-a800-7000-8000-000000000001'", [], |r| r.get(0)).unwrap();
    assert!(remote > 0);
    let local: i64 = conn
        .query_row(
            "SELECT count(*) FROM session_events WHERE source='omp' AND location != 'remote'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(local, 0);
    let projection: (Option<String>, Option<String>) = conn
        .query_row(
            "SELECT raw_path,last_assistant_text FROM sessions WHERE source='omp' AND session_id='019b76da-a800-7000-8000-000000000001'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(projection, (None, None));
    let independent: (String, String) = conn.query_row("SELECT raw_path,first_prompt FROM sessions WHERE source='omp' AND session_id='remote-only'", [], |r| Ok((r.get(0)?,r.get(1)?))).unwrap();
    assert_eq!(
        independent,
        (remote_only_path, "Independent remote preview".into())
    );

    let stale: i64 = conn.query_row("SELECT count(*) FROM history_fts f JOIN history h ON h.id=f.rowid WHERE history_fts MATCH 'synthetic' AND h.source='omp' AND h.session_id='sibling-session'", [], |r| r.get(0)).unwrap();
    assert_eq!(stale, 0);
}

#[test]
fn omp_moved_locator_and_new_fork_preserve_prompt_identity_before_deletion() {
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
        .query_row("SELECT min(id) FROM history WHERE source='omp'", [], |r| {
            r.get(0)
        })
        .unwrap();
    let moved = path.with_file_name("moved.jsonl");
    std::fs::rename(&path, &moved).unwrap();
    run(&mut state);
    let raw: String = conn
        .query_row(
            "SELECT raw_path FROM sessions WHERE source='omp'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(raw, moved.to_string_lossy());
    let fork = path.with_file_name("fork.jsonl");
    std::fs::write(
        &fork,
        FIXTURE
            .replace("019b76da-a800-7000-8000-000000000001", "surviving-fork")
            .replace("/synthetic/workspace", "/fork/final"),
    )
    .unwrap();
    std::fs::remove_file(&moved).unwrap();
    run(&mut state);
    let rows: i64 = conn
        .query_row(
            "SELECT count(*) FROM sessions WHERE source='omp'",
            [],
            |r| r.get(0),
        )
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

#[test]
fn omp_same_locator_identity_replacement_preserves_remote_share() {
    let (_home, store, path) = fixture();
    let conn = crate::open_db(store.db_path()).unwrap();
    let mut payload = snapshot(&path, &AcquisitionContext::new(ID, store.roots()))
        .unwrap()
        .payload;
    normalize::write(&conn, &payload, &path).unwrap();
    let old = payload["entries"][0]["id"].as_str().unwrap().to_owned();
    conn.execute("UPDATE session_events SET location='both' WHERE source='omp' AND session_id=?1 AND role='user'",[&old]).unwrap();
    crate::mark_session_presence(&conn, ID, &old, crate::SessionLocation::Remote).unwrap();
    payload["entries"][0]["id"] = "replacement-native".into();
    normalize::write(&conn, &payload, &path).unwrap();
    assert_eq!(
        crate::session_locations(&conn, ID, &old).unwrap(),
        ["remote"]
    );
    let local:i64=conn.query_row("SELECT count(*) FROM session_events WHERE source='omp' AND session_id=?1 AND location!='remote'",[&old],|r|r.get(0)).unwrap();
    assert_eq!(local, 0);
    let remote:i64=conn.query_row("SELECT count(*) FROM session_events WHERE source='omp' AND session_id=?1 AND location='remote'",[&old],|r|r.get(0)).unwrap();
    assert_eq!(remote, 1);
    let raw: Option<String> = conn
        .query_row(
            "SELECT raw_path FROM sessions WHERE source='omp' AND session_id=?1",
            [&old],
            |r| r.get(0),
        )
        .unwrap();
    assert!(raw.is_none());
    let new:i64=conn.query_row("SELECT count(*) FROM session_events WHERE source='omp' AND session_id='replacement-native'",[],|r|r.get(0)).unwrap();
    assert!(new > 0);
}
