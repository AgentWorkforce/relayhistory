//! Windows-only: a guest filesystem mapped into the host is usually reached
//! through a UNC share (`\\wsl.localhost\<distro>\...`, `\\<vm>\c$\...`), a
//! verbatim path, or a forward-slash path handed over from JavaScript. The
//! explicit guest roots must work unchanged in each spelling, and the host's
//! own populated Codex stores must stay out of the guest store.
//!
//! The UNC cases reach the temp directory through the administrative drive
//! share (`\\localhost\C$`). They probe it first and skip with a message when
//! it can't be reached; `AI_HIST_TEST_REQUIRE_UNC=1` turns that skip into a
//! failure, and `AI_HIST_TEST_UNC_HOST` names another host than `localhost`.
#![cfg(windows)]
use ai_hist::{
    CatalogQuery, DiscoveryOptions, ProviderRoots, SessionRef, SessionStore, Source, StoreOptions,
    SyncOptions,
};
use rusqlite::{params, Connection};
use std::{
    fs,
    path::{Path, PathBuf},
};

const ROLLOUT: &str = "sessions/2026/04/20/rollout-rollout-primary.jsonl";
const OVERLAP: &str = "sqlite-rollout-primary";
const ROLLOUT_PROMPT: &str = "Rollout primary prompt";
/// The host store reuses the guest's thread IDs with this prefix on every
/// prompt, so a read from the host store can't produce the guest's text.
const HOST: &str = "Host ";

fn add_thread(state: &Connection, history: &Connection, index: usize, marker: &str) {
    let id = format!("db-thread-{index:03}");
    let prompt = format!("{marker}Prompt for {id}");
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

/// `recorded` replaces the rollout path stored in SQLite, as a guest writes its
/// own native path. `marker` prefixes every prompt, the overlapping rollout's too.
fn install_fixture(
    rollouts: &Path,
    sqlite: &Path,
    count: usize,
    recorded: Option<&str>,
    marker: &str,
) {
    fs::create_dir_all(sqlite).unwrap();
    let path = rollouts.join(ROLLOUT);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let rollout = include_str!("fixtures/codex/sqlite/rollout-rollout-primary.jsonl");
    assert!(rollout.contains(ROLLOUT_PROMPT));
    fs::write(
        &path,
        rollout.replace(ROLLOUT_PROMPT, &format!("{marker}{ROLLOUT_PROMPT}")),
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
            [recorded.unwrap_or(path.to_str().unwrap())],
        )
        .unwrap();
    history
        .execute_batch(include_str!(
            "fixtures/codex/sqlite/rollout-primary-history.sql"
        ))
        .unwrap();
    for index in 0..count {
        add_thread(&state, &history, index, marker);
    }
}

fn catalog(store: &SessionStore) -> Vec<String> {
    let mut query = CatalogQuery::default();
    query.sources = Some(vec![Source::Codex]);
    query.page_size = 50;
    let mut ids: Vec<_> = store
        .sessions(query)
        .map(|r| r.unwrap().session_id)
        .collect();
    ids.sort();
    ids
}

fn prompts(db: &Path) -> Vec<(String, String)> {
    let conn = Connection::open(db).unwrap();
    let mut stmt = conn
        .prepare("SELECT session_id,text FROM session_events WHERE source='codex' AND role='user' ORDER BY session_id")
        .unwrap();
    stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

/// Event text anywhere in the ledger that came from the host store.
fn host_events(db: &Path) -> i64 {
    Connection::open(db)
        .unwrap()
        .query_row(
            "SELECT count(*) FROM session_events WHERE source='codex' AND text LIKE ?1",
            [format!("{HOST}%")],
            |r| r.get(0),
        )
        .unwrap()
}

fn locator(db: &Path, id: &str) -> String {
    Connection::open(db)
        .unwrap()
        .query_row(
            "SELECT raw_path FROM sessions WHERE source='codex' AND session_id=?1",
            [id],
            |r| r.get(0),
        )
        .unwrap()
}

/// The file a locator points at: `sqlite:{state}#{id}` names the state database.
fn locator_file(locator: &str) -> &Path {
    Path::new(match locator.strip_prefix("sqlite:") {
        Some(rest) => rest.rsplit_once('#').expect("sqlite locator without #").0,
        None => locator,
    })
}

/// `dir` canonicalized, without the `\\?\` prefix the other spellings add back.
fn local(dir: &Path) -> PathBuf {
    let path = dir.canonicalize().unwrap();
    PathBuf::from(path.to_str().unwrap().trim_start_matches(r"\\?\"))
}

/// `C:\x` -> `\\localhost\C$\x` (or `AI_HIST_TEST_UNC_HOST` in place of `localhost`)
fn unc(path: &Path) -> PathBuf {
    let host = std::env::var("AI_HIST_TEST_UNC_HOST").unwrap_or_else(|_| "localhost".into());
    let text = path.to_str().unwrap();
    let (drive, rest) = text.split_once(":\\").unwrap();
    PathBuf::from(format!(r"\\{host}\{drive}$\{rest}"))
}

/// Whether the temp directory can be written through its administrative
/// share. Local accounts, disabled file sharing or a stopped Server service
/// block it, and the UNC cases then skip unless `AI_HIST_TEST_REQUIRE_UNC=1`.
fn unc_share_reachable(test: &str) -> bool {
    let dir = tempfile::tempdir().unwrap();
    let base = local(dir.path());
    let probe = unc(&base).join("probe");
    let outcome = fs::write(&probe, b"ok").and_then(|()| fs::read(base.join("probe")));
    if matches!(&outcome, Ok(bytes) if bytes == b"ok") {
        return true;
    }
    let why = format!(
        "{test}: the share at {} is not reachable: {outcome:?}",
        probe.display()
    );
    if std::env::var("AI_HIST_TEST_REQUIRE_UNC").as_deref() == Ok("1") {
        panic!("{why} (AI_HIST_TEST_REQUIRE_UNC=1)");
    }
    eprintln!("skipping {why}. Set AI_HIST_TEST_REQUIRE_UNC=1 to fail instead.");
    false
}

fn verbatim(path: &Path) -> PathBuf {
    PathBuf::from(format!(r"\\?\{}", path.display()))
}

fn forward(path: &Path) -> PathBuf {
    PathBuf::from(path.to_str().unwrap().replace('\\', "/"))
}

/// Runs discovery, selected hydration, watch routing and a WAL sync against
/// guest roots spelled with `spell`, while the host home holds its own store.
fn exercise(label: &str, spell: fn(&Path) -> PathBuf) {
    exercise_as(label, spell, None)
}

fn exercise_as(label: &str, spell: fn(&Path) -> PathBuf, recorded: Option<&str>) {
    // The fixture threads are dated 2026-04-20; admit them regardless of age,
    // as tests/codex_sqlite_home.rs does. Every test sets the same value.
    std::env::set_var("AI_HIST_CODEX_SQLITE_MAX_AGE_DAYS", "0");
    let dir = tempfile::tempdir().unwrap();
    let base = local(dir.path());
    // Host: a populated default Codex home with the same thread IDs as the
    // guest but host-marked prompts. None of it may reach the guest store.
    let host = base.join("host");
    install_fixture(&host.join(".codex"), &host.join(".codex"), 8, None, HOST);
    // Guest: split rollout and SQLite directories, reached through `spell`.
    let guest = spell(&base.join("guest"));
    let rollouts = guest.join("rollouts");
    let sqlite = guest.join("database");
    install_fixture(&rollouts, &sqlite, 2, recorded, "");

    let mut roots = ProviderRoots::from_home(host.clone(), host.join("unused.db"))
        .with_codex_sqlite_home(sqlite.clone());
    roots.codex = rollouts.clone();
    let db = base.join(format!("{label}.db"));
    let store = SessionStore::open(StoreOptions::default().roots(roots).db_path(&db)).unwrap();
    let found = store
        .discover(DiscoveryOptions::default().sources([Source::Codex]))
        .unwrap();
    assert!(found.diagnostics.is_empty(), "{label}: {found:?}");
    let expected = vec![
        "db-thread-000".to_owned(),
        "db-thread-001".to_owned(),
        OVERLAP.to_owned(),
    ];
    assert_eq!(catalog(&store), expected, "{label}: catalog");
    for id in &expected {
        store
            .hydrate(&SessionRef::id(Source::Codex, id), Default::default())
            .unwrap_or_else(|e| panic!("{label}: hydrate {id}: {e:?}"));
    }
    assert_eq!(
        prompts(&db),
        vec![
            ("db-thread-000".into(), "Prompt for db-thread-000".into()),
            ("db-thread-001".into(), "Prompt for db-thread-001".into()),
            (OVERLAP.into(), ROLLOUT_PROMPT.into()),
        ],
        "{label}: hydrated prompts"
    );
    assert_eq!(host_events(&db), 0, "{label}: host store text hydrated");

    let db_locator = locator(&db, "db-thread-000");
    eprintln!("{label}: db-only locator = {db_locator}");
    assert!(db_locator.starts_with("sqlite:"), "{label}: {db_locator}");
    assert!(
        db_locator.ends_with("#db-thread-000"),
        "{label}: {db_locator}"
    );
    assert_eq!(
        locator_file(&db_locator),
        sqlite.join("state_5.sqlite"),
        "{label}: db-only locator names another database"
    );
    for id in &expected {
        let found = locator(&db, id);
        eprintln!("{label}: {id} locator = {found}");
        assert!(
            locator_file(&found).starts_with(&guest),
            "{label}: {id} located outside the guest: {found}"
        );
    }

    let watch = Source::Codex.capabilities().watch_roots(store.roots());
    eprintln!(
        "{label}: watch roots = {:?}",
        watch.iter().map(|r| &r.path).collect::<Vec<_>>()
    );
    for name in ["state_5.sqlite", "thread_history_1.sqlite-wal"] {
        assert!(
            watch
                .iter()
                .any(|r| r.path == sqlite && r.admits(name.as_ref())),
            "{label}: missing {name}: {watch:?}"
        );
    }
    assert!(
        watch.iter().all(|r| !r.path.starts_with(&host)),
        "{label}: host root leaked into guest watch: {watch:?}"
    );

    // A thread written to the guest WAL is picked up by a path-scoped sync.
    // The host store already holds a host-marked db-thread-007.
    let state = Connection::open(sqlite.join("state_5.sqlite")).unwrap();
    let history = Connection::open(sqlite.join("thread_history_1.sqlite")).unwrap();
    history.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
    add_thread(&state, &history, 7, "");
    let wal = sqlite.join("thread_history_1.sqlite-wal");
    assert!(wal.is_file(), "{label}: no WAL at {}", wal.display());
    let synced = store.sync(SyncOptions::default().paths([wal])).unwrap();
    assert!(
        catalog(&store).contains(&"db-thread-007".to_owned()),
        "{label}: WAL sync missed db-thread-007: {synced:?}"
    );
    store
        .hydrate(
            &SessionRef::id(Source::Codex, "db-thread-007"),
            Default::default(),
        )
        .unwrap();
    assert!(
        prompts(&db).contains(&("db-thread-007".into(), "Prompt for db-thread-007".into())),
        "{label}: db-thread-007 prompts: {:?}",
        prompts(&db)
    );
    assert_eq!(host_events(&db), 0, "{label}: host store text after sync");

    // Reopening the same ledger (a new process) keeps the guest owner.
    drop(store);
    let mut roots = ProviderRoots::from_home(host.clone(), host.join("unused.db"))
        .with_codex_sqlite_home(sqlite.clone());
    roots.codex = rollouts;
    let store = SessionStore::open(StoreOptions::default().roots(roots).db_path(&db)).unwrap();
    store
        .discover(DiscoveryOptions::default().sources([Source::Codex]))
        .unwrap();
    assert_eq!(catalog(&store).len(), 4, "{label}: reopen catalog");
    assert_eq!(
        locator(&db, "db-thread-000"),
        db_locator,
        "{label}: locator churn on reopen"
    );
    drop(state);
    drop(history);
}

#[test]
fn guest_roots_as_unc_share() {
    if unc_share_reachable("guest_roots_as_unc_share") {
        exercise("unc", unc);
    }
}

#[test]
fn guest_roots_as_verbatim_path() {
    exercise("verbatim", verbatim);
}

#[test]
fn guest_roots_with_forward_slashes() {
    exercise("forward", forward);
}

#[test]
fn guest_roots_as_plain_drive_path() {
    exercise("plain", |p| p.to_path_buf());
}

/// The serialized roots (what an embedder hands across a process boundary)
/// carry the guest SQLite directory.
#[test]
fn guest_sqlite_home_survives_serialization() {
    let roots = ProviderRoots::from_home(r"C:\host".into(), r"C:\host\o.db".into())
        .with_codex_sqlite_home(r"\\wsl.localhost\Ubuntu\home\u\.codex".into());
    let json = serde_json::to_string(&roots).unwrap();
    assert!(
        json.contains(r#""codex_sqlite_home":"\\\\wsl.localhost\\Ubuntu\\home\\u\\.codex""#),
        "{json}"
    );
    let back: ProviderRoots = serde_json::from_str(&json).unwrap();
    assert_eq!(back, roots);
}

/// A WSL or VM guest records its own POSIX rollout path, which the host cannot
/// open. Database-only threads and the projected rollout must still resolve.
#[test]
fn guest_roots_as_unc_share_with_guest_native_rollout_path() {
    if unc_share_reachable("guest_roots_as_unc_share_with_guest_native_rollout_path") {
        exercise_as(
            "unc-posix",
            unc,
            Some("/home/u/.codex/sessions/2026/04/20/rollout-rollout-primary.jsonl"),
        );
    }
}
