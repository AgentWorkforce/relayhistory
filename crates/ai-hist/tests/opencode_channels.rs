//! OpenCode keeps one SQLite store per release channel: `opencode.db` for
//! `latest`/`beta`, and `opencode-<channel>.db` beside it for every other
//! channel. Every one of them is a store of sessions, and a user on
//! `nightly` keeps theirs only in `opencode-nightly.db`.
//!
//! One `#[test]`, because the provider roots come from process-wide
//! environment variables and Rust runs the tests in a binary concurrently.

use ai_hist::{
    discover_sessions_scoped_at, hydrate_session_at, open_db, sync_local_at, DiscoverOptions,
    HydrateSessionOptions, SessionScope,
};
use rusqlite::Connection;
use std::fs;
use std::path::{Path, PathBuf};

const ROOT: &str = "ses_sqlite_root";
const CHILD: &str = "ses_sqlite_child";

fn temp_root(tag: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "ai-hist-opencode-channels-{tag}-{}-{:?}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(&root).unwrap();
    root
}

/// The checked-in fixture store, holding only `keep` of its two sessions.
fn build_store(target: &Path, keep: &[&str]) {
    let sql = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/opencode/sqlite-store.sql"),
    )
    .unwrap();
    fs::create_dir_all(target.parent().unwrap()).unwrap();
    let db = Connection::open(target).unwrap();
    db.execute_batch(&sql).unwrap();
    for session in [ROOT, CHILD] {
        if keep.contains(&session) {
            continue;
        }
        db.execute("DELETE FROM part WHERE session_id = ?", [session])
            .unwrap();
        db.execute("DELETE FROM message WHERE session_id = ?", [session])
            .unwrap();
        db.execute("DELETE FROM session WHERE id = ?", [session])
            .unwrap();
    }
}

fn use_home(home: &Path, pinned_db: Option<&Path>) {
    std::env::set_var("HOME", home);
    std::env::set_var("USERPROFILE", home);
    std::env::remove_var("AI_HIST_DB");
    match pinned_db {
        Some(db) => std::env::set_var("OPENCODE_DB", db),
        None => std::env::remove_var("OPENCODE_DB"),
    }
    std::env::set_var("OPENCODE_STORAGE_DIR", home.join("no-such-storage"));
}

fn opencode_only() -> DiscoverOptions {
    DiscoverOptions {
        sources: vec!["opencode".into()],
        ..Default::default()
    }
}

fn catalog(db_path: &Path) -> Vec<(String, String)> {
    open_db(db_path)
        .unwrap()
        .prepare(
            "SELECT session_id, COALESCE(raw_path, '') FROM sessions \
             WHERE source = 'opencode' ORDER BY session_id",
        )
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
}

fn history_sessions(db_path: &Path) -> Vec<String> {
    open_db(db_path)
        .unwrap()
        .prepare(
            "SELECT DISTINCT session_id FROM history WHERE source = 'opencode' \
             ORDER BY session_id",
        )
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
}

fn event_count(db_path: &Path, session_id: &str) -> i64 {
    open_db(db_path)
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM session_events WHERE source = 'opencode' AND session_id = ?",
            [session_id],
            |row| row.get(0),
        )
        .unwrap()
}

#[test]
fn opencode_channel_databases_are_discovered_synced_and_hydrated() {
    every_channel_store_is_discovered_and_hydrates();
    a_plain_sync_indexes_every_channel_store();
    a_session_in_two_stores_is_owned_by_the_first();
    a_pinned_opencode_db_reads_only_that_file();
}

/// `opencode.db` holds one session and `opencode-nightly.db` the other; a
/// `-wal` sidecar beside the nightly store is not a store of its own.
fn channel_home(tag: &str) -> (PathBuf, PathBuf, PathBuf) {
    let root = temp_root(tag);
    let home = root.join("home");
    let data = home.join(".local/share/opencode");
    build_store(&data.join("opencode.db"), &[ROOT]);
    build_store(&data.join("opencode-nightly.db"), &[CHILD]);
    fs::write(data.join("opencode-nightly.db-wal"), b"not a database").unwrap();
    (root, home, data)
}

fn every_channel_store_is_discovered_and_hydrates() {
    let (root, home, data) = channel_home("discover");
    use_home(&home, None);
    let db_path = root.join("history.db");
    discover_sessions_scoped_at(&db_path, &opencode_only()).unwrap();
    assert_eq!(
        catalog(&db_path),
        vec![
            (
                CHILD.to_string(),
                data.join("opencode-nightly.db")
                    .to_string_lossy()
                    .into_owned()
            ),
            (
                ROOT.to_string(),
                data.join("opencode.db").to_string_lossy().into_owned()
            ),
        ],
        "each session is catalogued with the channel store that holds it"
    );

    for session in [CHILD, ROOT] {
        let hydrated = hydrate_session_at(
            &db_path,
            &HydrateSessionOptions {
                source: "opencode".into(),
                session_id: session.into(),
                scope: SessionScope::Local,
                include_related: false,
            },
        )
        .unwrap_or_else(|error| panic!("hydrating {session}: {error:#}"));
        assert!(
            hydrated.evidence.events > 0,
            "{session} must hydrate from its own store, got {:?}",
            hydrated.evidence
        );
    }
    fs::remove_dir_all(&root).ok();
}

fn a_plain_sync_indexes_every_channel_store() {
    let (root, home, _data) = channel_home("sync");
    use_home(&home, None);
    let db_path = root.join("history.db");
    sync_local_at(&db_path).unwrap();
    assert_eq!(
        history_sessions(&db_path),
        vec![CHILD.to_string(), ROOT.to_string()]
    );
    assert!(event_count(&db_path, CHILD) > 0);
    assert!(event_count(&db_path, ROOT) > 0);
    fs::remove_dir_all(&root).ok();
}

/// A session copied into a second channel store is one session, not two, and
/// every path agrees on which store it comes from: the configured
/// `opencode.db` first, then the channel stores in name order.
fn a_session_in_two_stores_is_owned_by_the_first() {
    let (root, home, data) = channel_home("duplicate");
    build_store(&data.join("opencode-stable.db"), &[ROOT, CHILD]);
    use_home(&home, None);
    let db_path = root.join("history.db");
    sync_local_at(&db_path).unwrap();
    discover_sessions_scoped_at(&db_path, &opencode_only()).unwrap();
    assert_eq!(
        catalog(&db_path),
        vec![
            (
                CHILD.to_string(),
                data.join("opencode-nightly.db")
                    .to_string_lossy()
                    .into_owned()
            ),
            (
                ROOT.to_string(),
                data.join("opencode.db").to_string_lossy().into_owned()
            ),
        ]
    );
    // The sync's own provenance, which discovery does not rewrite: the child's
    // delegation edge names the store the sync indexed it from.
    let locators: Vec<String> = open_db(&db_path)
        .unwrap()
        .prepare(
            "SELECT COALESCE(evidence_locator, '') FROM session_relationships \
             WHERE source = 'opencode' AND child_session_id = ?",
        )
        .unwrap()
        .query_map([CHILD], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        locators,
        vec![data
            .join("opencode-nightly.db")
            .to_string_lossy()
            .into_owned()],
        "a later store must not re-index a session an earlier one owns"
    );
    fs::remove_dir_all(&root).ok();
}

fn a_pinned_opencode_db_reads_only_that_file() {
    let (root, home, data) = channel_home("pinned");
    let nightly = data.join("opencode-nightly.db");
    use_home(&home, Some(&nightly));
    let db_path = root.join("history.db");
    sync_local_at(&db_path).unwrap();
    discover_sessions_scoped_at(&db_path, &opencode_only()).unwrap();
    assert_eq!(
        catalog(&db_path),
        vec![(CHILD.to_string(), nightly.to_string_lossy().into_owned())],
        "OPENCODE_DB names one store, and its channel siblings are not read"
    );
    assert_eq!(history_sessions(&db_path), vec![CHILD.to_string()]);
    fs::remove_dir_all(&root).ok();
}
