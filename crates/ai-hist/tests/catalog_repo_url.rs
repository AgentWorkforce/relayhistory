use std::fs;
use std::path::Path;

use ai_hist::{
    CatalogQuery, CatalogSession, DiscoveryOptions, ProviderRoots, SessionStore, Source,
    StoreOptions, SyncOptions,
};
use rusqlite::Connection;

fn claude(home: &Path, id: &str, cwd: &Path) {
    let projects = home.join(".claude/projects/synthetic");
    fs::create_dir_all(&projects).unwrap();
    let user = serde_json::json!({
        "type": "user", "sessionId": id, "uuid": format!("{id}-user"),
        "timestamp": "2026-10-12T12:00:00Z", "cwd": cwd,
        "message": { "role": "user", "content": "hello" }
    });
    fs::write(projects.join(format!("{id}.jsonl")), format!("{user}\n")).unwrap();
}

fn codex(home: &Path, id: &str, cwd: &Path) {
    codex_with_remote(home, id, cwd, "https://fixture-user:fixture-token@recorded.test/Org/App.git?token=fixture-query#fixture-fragment");
}

fn codex_with_remote(home: &Path, id: &str, cwd: &Path, remote: &str) {
    write_codex_rollout(home, id, cwd, Some(remote));
}

fn write_codex_rollout(home: &Path, id: &str, cwd: &Path, remote: Option<&str>) {
    let day = home.join(".codex/sessions/2026/10/12");
    fs::create_dir_all(&day).unwrap();
    let mut meta = serde_json::json!({
        "type": "session_meta", "timestamp": "2026-10-12T12:00:00Z",
        "payload": { "id": id, "cwd": cwd }
    });
    if let Some(remote) = remote {
        meta["payload"]["git"] = serde_json::json!({ "repository_url": remote });
    }
    let user = serde_json::json!({
        "type": "event_msg", "timestamp": "2026-10-12T12:00:01Z",
        "payload": { "type": "user_message", "message": "hello" }
    });
    fs::write(
        day.join(format!("rollout-{id}.jsonl")),
        format!("{meta}\n{user}\n"),
    )
    .unwrap();
}

fn open(home: &Path, db: &Path) -> SessionStore {
    SessionStore::open(
        StoreOptions::default()
            .db_path(db)
            .roots(ProviderRoots::from_home(
                home.to_path_buf(),
                home.join("missing-opencode.db"),
            )),
    )
    .unwrap()
}

fn row(store: &SessionStore, id: &str) -> CatalogSession {
    store
        .sessions(CatalogQuery::default())
        .map(Result::unwrap)
        .find(|row| row.session_id == id)
        .unwrap_or_else(|| panic!("missing catalog row {id}"))
}

fn assert_repo(store: &SessionStore, id: &str, url: &str, key: &str) {
    let row = row(store, id);
    assert_eq!(row.repo_url.as_deref(), Some(url));
    assert_eq!(row.project_key.as_deref(), Some(key));
    assert_eq!(row.project_key_method.as_deref(), Some("remote"));
}

#[test]
fn catalog_urls_agree_across_discovery_cache_sweep_and_store_reads() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path();
    let git = home.join("main/.git");
    let worktree = home.join("linked");
    let worktree_git = git.join("worktrees/linked");
    fs::create_dir_all(&worktree_git).unwrap();
    fs::create_dir_all(&worktree).unwrap();
    fs::write(
        worktree.join(".git"),
        format!("gitdir: {}\n", worktree_git.display()),
    )
    .unwrap();
    fs::write(worktree_git.join("commondir"), "../..\n").unwrap();
    fs::write(
        git.join("config"),
        "[remote \"origin\"]\nurl = git@origin.test:Org/Worktree.git\n",
    )
    .unwrap();

    claude(home, "claude-remote", &worktree);
    let missing = home.join("missing");
    claude(home, "claude-path", &missing);
    codex(home, "codex-remote", &missing);
    codex(home, "codex-precedence", &worktree);

    let sources = [Source::Claude, Source::Codex];
    let db = home.join("discovery.db");
    let store = open(home, &db);
    let discover = || {
        store
            .discover(DiscoveryOptions::default().sources(sources))
            .unwrap()
    };
    let check = |store: &SessionStore| {
        assert_repo(
            store,
            "claude-remote",
            "git@origin.test:Org/Worktree.git",
            "origin.test/Org/Worktree",
        );
        assert_repo(
            store,
            "codex-remote",
            "https://recorded.test/Org/App.git",
            "recorded.test/Org/App",
        );
        assert_repo(
            store,
            "codex-precedence",
            "https://recorded.test/Org/App.git",
            "recorded.test/Org/App",
        );
        let path = row(store, "claude-path");
        assert_eq!(path.repo_url, None);
        assert_eq!(path.project_key.as_deref(), missing.to_str());
        assert_eq!(path.project_key_method.as_deref(), Some("path"));
    };
    assert_eq!(discover().discovered, 4);
    check(&store);

    let raw = Connection::open(&db).unwrap();
    let age_catalog = || {
        raw.execute(
            "UPDATE sessions SET repo_url = NULL WHERE session_id = 'claude-remote'",
            [],
        )
        .unwrap();
        raw.execute(
            "UPDATE sessions SET project_key = 'origin.test/Org/Worktree' WHERE session_id = 'codex-precedence'",
            [],
        )
        .unwrap();
        raw.execute(
            "UPDATE sessions SET repo_url = ?1, project_key = ?2 WHERE session_id = 'codex-remote'",
            ["https://fixture-user:fixture-token@recorded.test/Org/App.git?token=fixture-query#fixture-fragment",
             "recorded.test/Org/App.git?token=fixture-query#fixture-fragment"],
        ).unwrap();
    };
    age_catalog();
    assert_eq!(
        row(&store, "codex-remote").repo_url.as_deref(),
        Some("https://recorded.test/Org/App.git")
    );
    // Filtering must apply the displayed key before the page limit and leave
    // the unrepaired catalog untouched.
    let mut query = CatalogQuery::default();
    query.project_key = Some("recorded.test/Org/App".into());
    query.page_size = 1;
    let mut matched: Vec<_> = store
        .sessions(query)
        .map(|row| row.unwrap().session_id)
        .collect();
    matched.sort();
    assert_eq!(matched, ["codex-precedence", "codex-remote"]);
    let mut stale = CatalogQuery::default();
    stale.project_key = Some("origin.test/Org/Worktree".into());
    stale.sources = Some(vec![Source::Codex]);
    assert!(store.sessions(stale).next().is_none());

    let stored_key: String = raw
        .query_row(
            "SELECT project_key FROM sessions WHERE session_id = 'codex-precedence'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stored_key, "origin.test/Org/Worktree");
    assert_eq!(discover().skipped_unchanged, 4);
    check(&store);

    let sync = |store: &SessionStore| store.sync(SyncOptions::default().sources(sources)).unwrap();
    let sweep_db = home.join("sweep.db");
    let swept = open(home, &sweep_db);
    sync(&swept);
    check(&swept);

    age_catalog();
    sync(&store);
    check(&store);
    let exact_db = home.join("exact.db");
    let exact = open(home, &exact_db);
    let admitted_paths = [
        home.join(".claude/projects/synthetic/claude-remote.jsonl"),
        home.join(".claude/projects/synthetic/claude-path.jsonl"),
        home.join(".codex/sessions/2026/10/12/rollout-codex-remote.jsonl"),
        home.join(".codex/sessions/2026/10/12/rollout-codex-precedence.jsonl"),
    ];
    exact
        .sync(
            SyncOptions::default()
                .sources(sources)
                .paths(admitted_paths),
        )
        .unwrap();
    check(&exact);

    let persisted: String = raw
        .query_row(
            "SELECT repo_url FROM sessions WHERE session_id = 'codex-remote'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(persisted, "https://recorded.test/Org/App.git");
    let recorded_key: String = raw
        .query_row(
            "SELECT project_key FROM sessions WHERE session_id = 'codex-precedence'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(recorded_key, "recorded.test/Org/App");
}

#[test]
fn literal_scp_paths_remain_distinct_after_cached_discovery() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path();
    let store = open(home, &home.join("catalog.db"));
    for (id, path) in [
        ("plain", "App"),
        ("literal", "App?one#two"),
        ("escaped", "App%3Fone%23two"),
    ] {
        let cwd = home.join(id);
        fs::create_dir_all(cwd.join(".git")).unwrap();
        fs::write(
            cwd.join(".git/config"),
            format!("[remote \"origin\"]\nurl = \"git@origin.test:Org/{path}.git\"\n"),
        )
        .unwrap();
        claude(home, id, &cwd);
    }
    let observe = || {
        store
            .discover(DiscoveryOptions::default().sources([Source::Claude]))
            .unwrap()
    };
    assert_eq!(observe().discovered, 3);
    assert_eq!(observe().skipped_unchanged, 3);
    for (id, url_path, key_path) in [
        ("plain", "App", "App"),
        ("literal", "App?one#two", "App%3Fone%23two"),
        ("escaped", "App%3Fone%23two", "App%253Fone%2523two"),
    ] {
        assert_repo(
            &store,
            id,
            &format!("git@origin.test:Org/{url_path}.git"),
            &format!("origin.test/Org/{key_path}"),
        );
    }
}

#[test]
fn a_recorded_remote_can_supply_a_safe_key_without_a_catalog_url() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path();
    let cwd = home.join("checkout");
    fs::create_dir_all(cwd.join(".git")).unwrap();
    fs::write(
        cwd.join(".git/config"),
        "[remote \"origin\"]\nurl = https://local.test/Org/Other.git\n",
    )
    .unwrap();
    let missing = home.join("missing");
    let cases = [
        (
            "unicode",
            cwd.as_path(),
            "https://münchen.test/Org/App.git",
            "münchen.test/Org/App",
        ),
        (
            "empty-port",
            cwd.as_path(),
            "ssh://git@origin.test:/Org/App.git",
            "origin.test/Org/App",
        ),
        (
            "query-backslash",
            missing.as_path(),
            r"https://origin.test/Org/App.git?token=test\secret",
            "origin.test/Org/App",
        ),
        (
            "userinfo-delimiter",
            missing.as_path(),
            "https://test-user:fixture#secret@origin.test/Org/App.git",
            "origin.test/Org/App",
        ),
    ];
    for (id, cwd, url, _) in cases {
        codex_with_remote(home, id, cwd, url);
    }

    let check = |store: &SessionStore| {
        for (id, _, _, key) in cases {
            let session = row(store, id);
            assert_eq!(session.project_key.as_deref(), Some(key), "{id}");
            assert_eq!(
                session.project_key_method.as_deref(),
                Some("remote"),
                "{id}"
            );
            assert_eq!(session.repo_url, None, "{id}");
        }
    };
    let db = home.join("discover.db");
    let store = open(home, &db);
    let observe = || {
        store
            .discover(DiscoveryOptions::default().sources([Source::Codex]))
            .unwrap()
    };
    assert_eq!(observe().discovered, cases.len());
    check(&store);
    let raw = Connection::open(db).unwrap();
    for (id, _, url, _) in cases {
        raw.execute(
            "UPDATE sessions SET repo_url = ?1, project_key = 'local.test/Org/Other' WHERE session_id = ?2",
            [url, id],
        ).unwrap();
    }
    check(&store);
    assert_eq!(observe().skipped_unchanged, cases.len());
    check(&store);

    let remaining: usize = raw
        .query_row(
            "SELECT count(*) FROM sessions WHERE repo_url IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(remaining, 0);

    let swept = open(home, &home.join("sweep.db"));
    swept
        .sync(SyncOptions::default().sources([Source::Codex]))
        .unwrap();
    check(&swept);
}

#[test]
fn reopened_history_only_codex_sync_refreshes_checkout_keys_and_preserves_recorded_keys() {
    use std::io::Write;

    let temp = tempfile::tempdir().unwrap();
    let home = temp.path();
    let checkout = home.join("checkout");
    fs::create_dir_all(checkout.join(".git")).unwrap();
    let origin = |name: &str| {
        fs::write(
            checkout.join(".git/config"),
            format!("[remote \"origin\"]\nurl = https://checkout.test/Org/{name}.git\n"),
        )
        .unwrap();
    };
    origin("Old");
    write_codex_rollout(home, "checkout", &checkout, None);
    write_codex_rollout(home, "legacy", &checkout, None);
    codex_with_remote(
        home,
        "recorded",
        &checkout,
        "ssh://git@recorded.test:/Org/Old.git",
    );

    let history = home.join(".codex/history.jsonl");
    fs::write(&history, "").unwrap();
    let db = home.join("catalog.db");
    let store = open(home, &db);
    store
        .sync(SyncOptions::default().sources([Source::Codex]))
        .unwrap();
    let list = |store: &SessionStore, key: &str| {
        let mut query = CatalogQuery::default();
        query.project_key = Some(key.into());
        query.page_size = 1;
        let mut ids: Vec<_> = store
            .sessions(query)
            .map(|row| row.unwrap().session_id)
            .collect();
        ids.sort_unstable();
        ids
    };
    assert_eq!(
        list(&store, "checkout.test/Org/Old"),
        ["checkout", "legacy"]
    );
    assert_eq!(list(&store, "recorded.test/Org/Old"), ["recorded"]);
    assert_eq!(row(&store, "recorded").repo_url, None);
    drop(store);

    let raw = Connection::open(&db).unwrap();
    raw.execute(
        "UPDATE sessions SET project_key_origin = NULL WHERE session_id = 'legacy'",
        [],
    )
    .unwrap();
    let origins = || {
        let mut stmt = raw
            .prepare("SELECT project_key_origin FROM sessions ORDER BY session_id")
            .unwrap();
        stmt.query_map([], |row| row.get::<_, Option<String>>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    };
    assert_eq!(
        origins(),
        [Some("checkout".into()), None, Some("recorded".into())]
    );
    origin("New");

    let mut log = fs::OpenOptions::new().append(true).open(&history).unwrap();
    for id in ["checkout", "legacy", "recorded"] {
        writeln!(
            log,
            "{}",
            serde_json::json!({
                "session_id": id, "ts": 1791810000, "text": format!("history only {id}")
            })
        )
        .unwrap();
    }
    drop(log);
    let reopened = open(home, &db);
    reopened
        .sync(
            SyncOptions::default()
                .sources([Source::Codex])
                .paths([history]),
        )
        .unwrap();
    for id in ["checkout", "legacy", "recorded"] {
        let captured = raw
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM history WHERE session_id = ?1 AND prompt = ?2)",
                [id, &format!("history only {id}")],
                |row| row.get::<_, bool>(0),
            )
            .unwrap();
        assert!(captured, "{id}");
    }
    assert_eq!(list(&reopened, "checkout.test/Org/New"), ["checkout"]);
    assert_eq!(list(&reopened, "checkout.test/Org/Old"), ["legacy"]);
    assert_eq!(list(&reopened, "recorded.test/Org/Old"), ["recorded"]);
    assert_eq!(row(&reopened, "recorded").repo_url, None);

    codex_with_remote(
        home,
        "recorded",
        &checkout,
        "https://recorded.test/Org/Newer.git",
    );
    reopened
        .sync(SyncOptions::default().sources([Source::Codex]))
        .unwrap();
    assert!(list(&reopened, "recorded.test/Org/Old").is_empty());
    assert_eq!(list(&reopened, "recorded.test/Org/Newer"), ["recorded"]);
    assert_repo(
        &reopened,
        "recorded",
        "https://recorded.test/Org/Newer.git",
        "recorded.test/Org/Newer",
    );
}

#[test]
fn history_only_sync_uses_the_checkout_seen_by_discovery_after_a_rollout_rewrite() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path();
    let old_checkout = home.join("checkout");
    let new_checkout = home.join("checkout-new");
    for (cwd, project) in [(&old_checkout, "Old"), (&new_checkout, "New")] {
        fs::create_dir_all(cwd.join(".git")).unwrap();
        fs::write(
            cwd.join(".git/config"),
            format!("[remote \"origin\"]\nurl = https://checkout.test/Org/{project}.git\n"),
        )
        .unwrap();
    }

    let id = "moved";
    write_codex_rollout(home, id, &old_checkout, None);
    let history = home.join(".codex/history.jsonl");
    fs::write(&history, "").unwrap();
    let db = home.join("catalog.db");
    let store = open(home, &db);
    store
        .sync(SyncOptions::default().sources([Source::Codex]))
        .unwrap();
    assert_eq!(
        row(&store, id).project_key.as_deref(),
        Some("checkout.test/Org/Old")
    );

    write_codex_rollout(home, id, &new_checkout, None);
    store
        .discover(DiscoveryOptions::default().sources([Source::Codex]))
        .unwrap();
    assert_eq!(row(&store, id).cwd.as_deref(), new_checkout.to_str());
    assert_eq!(
        row(&store, id).project_key.as_deref(),
        Some("checkout.test/Org/New")
    );
    drop(store);

    fs::write(
        &history,
        format!(
            "{}\n",
            serde_json::json!({
                "session_id": id, "ts": 1791810000, "text": "after checkout move"
            })
        ),
    )
    .unwrap();
    let store = open(home, &db);
    store
        .sync(
            SyncOptions::default()
                .sources([Source::Codex])
                .paths([history]),
        )
        .unwrap();
    let session = row(&store, id);
    assert_eq!(session.cwd.as_deref(), new_checkout.to_str());
    assert_eq!(
        session.project_key.as_deref(),
        Some("checkout.test/Org/New")
    );
    let prompt_cwd: String = Connection::open(&db)
        .unwrap()
        .query_row(
            "SELECT project FROM history WHERE session_id = ?1 AND prompt = 'after checkout move'",
            [id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(Some(prompt_cwd.as_str()), new_checkout.to_str());
    let list = |key: &str| {
        let mut query = CatalogQuery::default();
        query.project_key = Some(key.into());
        query.page_size = 1;
        store
            .sessions(query)
            .map(|row| row.unwrap().session_id)
            .collect::<Vec<_>>()
    };
    assert!(list("checkout.test/Org/Old").is_empty());
    assert_eq!(list("checkout.test/Org/New"), [id]);
}
