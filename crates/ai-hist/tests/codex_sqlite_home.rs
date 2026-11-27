//! Codex may keep its rollout tree and thread databases in separate directories.
use ai_hist::{
    CatalogQuery, DiscoveryOptions, ProviderRoots, SessionRef, SessionStore, Source, StoreOptions,
    SyncOptions,
};
use rusqlite::{params, Connection};
use std::{collections::BTreeMap, fs, path::Path, process::Command};

const ROLLOUT: &str = "sessions/2026/04/20/rollout-rollout-primary.jsonl";
const OVERLAP: &str = "sqlite-rollout-primary";

fn add_thread(state: &Connection, history: &Connection, index: usize) {
    let id = format!("db-thread-{index:03}");
    let prompt = format!("Prompt for {id}");
    let item = serde_json::json!({
        "type":"userMessage","id":id,"content":[{"type":"text","text":prompt}]
    });
    state.execute(
        "INSERT INTO threads(id,first_user_message,created_at_ms,updated_at_ms,source,archived,history_mode)
         VALUES(?1,?2,1776643200000,1776643200000,'cli',0,'paginated')",
        params![id, prompt],
    ).unwrap();
    history
        .execute(
            "INSERT INTO thread_history_projection_state VALUES(?1,1,0)",
            [&id],
        )
        .unwrap();
    history
        .execute(
            "INSERT INTO thread_items VALUES(?1,'turn-1',?1,0,1776643200000,'userMessage',?2)",
            params![id, item.to_string()],
        )
        .unwrap();
}

fn install_fixture(rollouts: &Path, sqlite: &Path, count: usize) {
    fs::create_dir_all(sqlite).unwrap();
    let path = rollouts.join(ROLLOUT);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(
        &path,
        include_str!("fixtures/codex/sqlite/rollout-rollout-primary.jsonl"),
    )
    .unwrap();
    let state = Connection::open(sqlite.join("state_5.sqlite")).unwrap();
    let history = Connection::open(sqlite.join("thread_history_1.sqlite")).unwrap();
    state
        .execute_batch(include_str!(
            "fixtures/codex/sqlite/rollout-primary-state.sql"
        ))
        .unwrap();
    state
        .execute(
            "UPDATE threads SET rollout_path=?1",
            [path.to_str().unwrap()],
        )
        .unwrap();
    history
        .execute_batch(include_str!(
            "fixtures/codex/sqlite/rollout-primary-history.sql"
        ))
        .unwrap();
    for index in 0..count {
        add_thread(&state, &history, index);
    }
}

#[test]
fn resolve_codex_sqlite_home_in_an_isolated_process() {
    for mode in ["unset", "blank", "split", "unavailable", "absent"] {
        let dir = tempfile::tempdir().unwrap();
        let rollouts = dir.path().join("codex");
        let sqlite = dir.path().join("sqlite");
        let mut child = Command::new(std::env::current_exe().unwrap());
        child
            .args(["--exact", "check_resolved_roots", "--nocapture"])
            .env_clear()
            .env("HOME", dir.path())
            .env("USERPROFILE", dir.path())
            .env("AI_HIST_CODEX_SQLITE_MAX_AGE_DAYS", "0")
            .env("CODEX_HOME", &rollouts)
            .env("RH_TEST_ROOT_MODE", mode);
        // Windows requires SystemRoot for some OS services.
        if let Some(root) = std::env::var_os("SystemRoot") {
            child.env("SystemRoot", root);
        }
        match mode {
            "split" => {
                install_fixture(&rollouts, &sqlite, 60);
                child.env("CODEX_SQLITE_HOME", &sqlite);
            }
            "unavailable" | "absent" => {
                fs::create_dir(&rollouts).unwrap();
                if mode == "unavailable" {
                    fs::write(&sqlite, b"not a directory").unwrap();
                }
                child.env("CODEX_SQLITE_HOME", &sqlite);
            }
            _ => {
                install_fixture(&rollouts, &rollouts, 1);
                if mode == "blank" {
                    child.env("CODEX_SQLITE_HOME", "  ");
                }
            }
        }
        let output = child.output().unwrap();
        assert!(
            output.status.success(),
            "{mode}: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

fn catalog(store: &SessionStore) -> Vec<String> {
    let mut query = CatalogQuery::default();
    query.sources = Some(vec![Source::Codex]);
    query.page_size = 13;
    let mut ids: Vec<_> = store
        .sessions(query)
        .map(|row| row.unwrap().session_id)
        .collect();
    ids.sort();
    ids
}

#[test]
fn check_resolved_roots() {
    let Ok(mode) = std::env::var("RH_TEST_ROOT_MODE") else {
        return;
    };
    let home = std::path::PathBuf::from(std::env::var_os("HOME").unwrap());
    let rollouts = home.join("codex");
    let sqlite = if matches!(mode.as_str(), "split" | "absent" | "unavailable") {
        home.join("sqlite")
    } else {
        rollouts.clone()
    };
    let db = home.join("history.db");
    let store = SessionStore::open(StoreOptions::default().home(&home).db_path(&db)).unwrap();
    let options = DiscoveryOptions::default().sources([Source::Codex]);
    // The malformed override in the unavailable case is an observable probe:
    // a selected provider reports it, an excluded provider must not touch it.
    let excluded = store
        .discover(DiscoveryOptions::default().sources([Source::Claude]))
        .unwrap();
    assert!(excluded.diagnostics.is_empty(), "{excluded:?}");
    assert!(catalog(&store).is_empty());
    let discovery = store.discover(options).unwrap();
    if matches!(mode.as_str(), "absent" | "unavailable") {
        assert!(!discovery.diagnostics.is_empty(), "{discovery:?}");
        let ignored = store
            .sync(SyncOptions::default().sources([Source::Claude]))
            .unwrap();
        assert!(ignored.diagnostics.is_empty(), "{ignored:?}");
        return;
    }
    check_watch_roots(&store, &sqlite);
    if mode == "split" {
        check_fixed_roots(&home, &sqlite);
    }
    check_hydration(&store, &db, if mode == "split" { 60 } else { 1 });
    if mode == "split" {
        check_projection_handoff(&store, &db, &rollouts, &sqlite);
        check_wal_update(&store, &sqlite);
    }
}

fn check_watch_roots(store: &SessionStore, sqlite: &Path) {
    let watch = Source::Codex.capabilities().watch_roots(store.roots());
    for name in [
        "state_5.sqlite",
        "thread_history_1.sqlite",
        "state_5.sqlite-wal",
        "thread_history_1.sqlite-wal",
    ] {
        assert!(
            watch
                .iter()
                .any(|r| r.path == sqlite && r.admits(name.as_ref())),
            "missing {name}: {watch:?}"
        );
    }
}

fn check_fixed_roots(home: &Path, sqlite: &Path) {
    let isolated = ProviderRoots::from_home(home.join("unrelated"), home.join("empty.db"));
    let other = SessionStore::open(
        StoreOptions::default()
            .roots(isolated)
            .db_path(home.join("unrelated.db")),
    )
    .unwrap();
    other
        .discover(DiscoveryOptions::default().sources([Source::Codex]))
        .unwrap();
    assert!(catalog(&other).is_empty());
    assert!(Source::Codex
        .capabilities()
        .watch_roots(other.roots())
        .iter()
        .all(|r| !r.path.starts_with(sqlite)));
}

fn prompts(conn: &Connection) -> Vec<(String, String)> {
    let mut stmt = conn.prepare(
        "SELECT session_id,text FROM session_events WHERE source='codex' AND role='user' ORDER BY session_id",
    ).unwrap();
    stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

fn check_hydration(store: &SessionStore, db: &Path, count: usize) {
    let mut expected: BTreeMap<_, _> = (0..count)
        .map(|i| {
            let id = format!("db-thread-{i:03}");
            (id.clone(), format!("Prompt for {id}"))
        })
        .collect();
    expected.insert(OVERLAP.to_owned(), "Rollout primary prompt".into());
    let ids = catalog(store);
    assert_eq!(ids, expected.keys().cloned().collect::<Vec<_>>());
    for id in &ids {
        store
            .hydrate(&SessionRef::id(Source::Codex, id), Default::default())
            .unwrap();
    }
    let conn = Connection::open(db).unwrap();
    let expected_rows: Vec<_> = expected.into_iter().collect();
    assert_eq!(prompts(&conn), expected_rows);
    store
        .discover(DiscoveryOptions::default().sources([Source::Codex]))
        .unwrap();
    store
        .hydrate(&SessionRef::id(Source::Codex, OVERLAP), Default::default())
        .unwrap();
    assert_eq!(catalog(store), ids);
    assert_eq!(prompts(&conn), expected_rows);
}

fn check_projection_handoff(store: &SessionStore, db: &Path, rollouts: &Path, sqlite: &Path) {
    let conn = Connection::open(db).unwrap();
    let locator = || {
        conn.query_row(
            "SELECT raw_path FROM sessions WHERE source='codex' AND session_id=?1",
            [OVERLAP],
            |row| row.get::<_, String>(0),
        )
        .unwrap()
    };
    assert_eq!(Path::new(&locator()), rollouts.join(ROLLOUT));
    let history = Connection::open(sqlite.join("thread_history_1.sqlite")).unwrap();
    let set_offset = |offset: i64| {
        history.execute(
            "UPDATE thread_history_projection_state SET next_rollout_byte_offset=?1 WHERE thread_id=?2",
            params![offset, OVERLAP],
        ).unwrap();
    };
    let projected_locator = format!(
        "sqlite:{}#{OVERLAP}",
        sqlite.join("state_5.sqlite").display()
    );
    set_offset(999999);
    store
        .discover(DiscoveryOptions::default().sources([Source::Codex]))
        .unwrap();
    assert_eq!(locator(), projected_locator);
    let session = SessionRef::id(Source::Codex, OVERLAP);
    store.hydrate(&session, Default::default()).unwrap();
    let tail: i64 = conn.query_row(
        "SELECT count(*) FROM session_events WHERE source='codex' AND session_id=?1 AND text='SQLITE MUST NOT WIN'",
        [OVERLAP], |row| row.get(0),
    ).unwrap();
    assert_eq!(
        tail, 1,
        "additional projected evidence must now be available"
    );
    set_offset(10);
    store.hydrate(&session, Default::default()).unwrap();
    assert_eq!(Path::new(&locator()), rollouts.join(ROLLOUT));

    // Selected hydration must also refresh a cached rollout's owner when no
    // discovery has run since the SQLite projection advanced.
    set_offset(1000000);
    store.hydrate(&session, Default::default()).unwrap();
    assert_eq!(locator(), projected_locator);
    set_offset(10);
    store.hydrate(&session, Default::default()).unwrap();
    assert_eq!(Path::new(&locator()), rollouts.join(ROLLOUT));
}

fn check_wal_update(store: &SessionStore, sqlite: &Path) {
    let state = Connection::open(sqlite.join("state_5.sqlite")).unwrap();
    let history = Connection::open(sqlite.join("thread_history_1.sqlite")).unwrap();
    history.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
    add_thread(&state, &history, 60);
    let wal = sqlite.join("thread_history_1.sqlite-wal");
    assert!(wal.is_file());
    store.sync(SyncOptions::default().paths([wal])).unwrap();
    assert!(catalog(store).contains(&"db-thread-060".to_owned()));
}
