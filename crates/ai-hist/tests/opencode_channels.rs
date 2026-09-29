//! OpenCode keeps one SQLite store per release channel: `opencode.db` for
//! `latest`/`beta`, and `opencode-<channel>.db` beside it for every other
//! channel. Every one of them is a store of sessions, and a user on
//! `nightly` keeps theirs only in `opencode-nightly.db`.
//!
//! One `#[test]`, because the provider roots come from process-wide
//! environment variables and Rust runs the tests in a binary concurrently.

use ai_hist::{
    discover_sessions_scoped_at, hydrate_session_at, open_db, sync_local_at,
    sync_opencode_with_roots, DiscoverOptions, HydrateSessionOptions, ProviderRoots, SessionScope,
    SyncOutput,
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
    a_limited_page_is_not_shortened_by_duplicates();
    a_limited_page_keeps_first_store_ownership();
    one_broken_channel_store_does_not_hide_the_others();
    a_limited_page_still_reports_a_broken_channel_store();
    #[cfg(unix)]
    sync_opencode_fails_when_the_channel_directory_cannot_be_listed();
    hydration_refuses_a_copy_an_earlier_store_has_since_claimed();
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

/// With a limit, a session a later store shares with an earlier one must not
/// take one of the later store's slots: `opencode.db` holds an old copy of
/// the root, `opencode-nightly.db` a fresh copy of it plus the child. The
/// root belongs to `opencode.db` at its old time, so the newest session
/// overall is the nightly child — which a per-store page cut before
/// de-duplication never reached.
fn a_limited_page_is_not_shortened_by_duplicates() {
    let root = temp_root("limited");
    let home = root.join("home");
    let data = home.join(".local/share/opencode");
    build_store(&data.join("opencode.db"), &[ROOT]);
    Connection::open(data.join("opencode.db"))
        .unwrap()
        .execute(
            "UPDATE session SET time_updated = 1776643000000 WHERE id = ?",
            [ROOT],
        )
        .unwrap();
    build_store(&data.join("opencode-nightly.db"), &[ROOT, CHILD]);
    use_home(&home, None);
    let db_path = root.join("history.db");
    let (rows, _) = discover_sessions_scoped_at(
        &db_path,
        &DiscoverOptions {
            limit: Some(1),
            ..opencode_only()
        },
    )
    .unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| (
                row.session_id.clone(),
                row.raw_path.clone().unwrap_or_default()
            ))
            .collect::<Vec<_>>(),
        vec![(
            CHILD.to_string(),
            data.join("opencode-nightly.db")
                .to_string_lossy()
                .into_owned()
        )]
    );
    fs::remove_dir_all(&root).ok();
}

/// A channel database that cannot be opened is that store's failure: the
/// healthy store is still catalogued, and the broken one is named in a
/// diagnostic rather than failing the whole provider.
fn one_broken_channel_store_does_not_hide_the_others() {
    let root = temp_root("broken");
    let home = root.join("home");
    let data = home.join(".local/share/opencode");
    build_store(&data.join("opencode-stable.db"), &[ROOT]);
    fs::write(data.join("opencode-nightly.db"), b"not a database at all").unwrap();
    use_home(&home, None);
    let db_path = root.join("history.db");
    let (_, summary) = discover_sessions_scoped_at(&db_path, &opencode_only()).unwrap();
    assert_eq!(
        catalog(&db_path),
        vec![(
            ROOT.to_string(),
            data.join("opencode-stable.db")
                .to_string_lossy()
                .into_owned()
        )]
    );
    let broken = data
        .join("opencode-nightly.db")
        .to_string_lossy()
        .into_owned();
    assert!(
        summary
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.locator.as_deref() == Some(broken.as_str())),
        "the unreadable store is reported against its path: {:?}",
        summary.diagnostics
    );
    assert!(!summary.providers["opencode"].failed);
    fs::remove_dir_all(&root).ok();
}

/// A row catalogued from `opencode-nightly.db` names a superseded copy once
/// `opencode.db` gains the same session: sync now reads that one, so
/// hydration refuses rather than import evidence sync disagrees with.
fn hydration_refuses_a_copy_an_earlier_store_has_since_claimed() {
    let (root, home, data) = channel_home("superseded");
    use_home(&home, None);
    let db_path = root.join("history.db");
    discover_sessions_scoped_at(&db_path, &opencode_only()).unwrap();
    fs::remove_file(data.join("opencode.db")).unwrap();
    build_store(&data.join("opencode.db"), &[ROOT, CHILD]);
    let error = hydrate_session_at(
        &db_path,
        &HydrateSessionOptions {
            source: "opencode".into(),
            session_id: CHILD.into(),
            scope: SessionScope::Local,
            include_related: false,
        },
    )
    .expect_err("hydrating a superseded channel copy must be refused");
    assert!(
        format!("{error:#}").contains("SESSION_SOURCE_MISMATCH"),
        "{error:#}"
    );
    fs::remove_dir_all(&root).ok();
}

/// With a limit, ownership is still decided over everything a store holds,
/// not only over its limited page: `opencode.db` holds the child and an old
/// copy of the root (outside its one-row page), `opencode-nightly.db` a fresh
/// copy of the root. The root is `opencode.db`'s — sync reads it there — so a
/// limited page must not catalogue it from the nightly store.
fn a_limited_page_keeps_first_store_ownership() {
    let root = temp_root("limited-owner");
    let home = root.join("home");
    let data = home.join(".local/share/opencode");
    build_store(&data.join("opencode.db"), &[ROOT, CHILD]);
    Connection::open(data.join("opencode.db"))
        .unwrap()
        .execute(
            "UPDATE session SET time_updated = 1776643000000 WHERE id = ?",
            [ROOT],
        )
        .unwrap();
    build_store(&data.join("opencode-nightly.db"), &[ROOT]);
    use_home(&home, None);
    let db_path = root.join("history.db");
    let (rows, _) = discover_sessions_scoped_at(
        &db_path,
        &DiscoverOptions {
            limit: Some(1),
            ..opencode_only()
        },
    )
    .unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| (
                row.session_id.clone(),
                row.raw_path.clone().unwrap_or_default()
            ))
            .collect::<Vec<_>>(),
        vec![(
            CHILD.to_string(),
            data.join("opencode.db").to_string_lossy().into_owned()
        )],
        "the root belongs to opencode.db, whose copy is older than the child"
    );
    fs::remove_dir_all(&root).ok();
}

/// A full page of healthy sessions must not crowd out the report of a
/// channel store that could not be opened.
fn a_limited_page_still_reports_a_broken_channel_store() {
    let root = temp_root("broken-limited");
    let home = root.join("home");
    let data = home.join(".local/share/opencode");
    build_store(&data.join("opencode.db"), &[ROOT, CHILD]);
    fs::write(data.join("opencode-nightly.db"), b"not a database at all").unwrap();
    use_home(&home, None);
    let db_path = root.join("history.db");
    let (rows, summary) = discover_sessions_scoped_at(
        &db_path,
        &DiscoverOptions {
            limit: Some(1),
            ..opencode_only()
        },
    )
    .unwrap();
    assert_eq!(rows.len(), 1, "the broken store costs the page no session");
    let broken = data
        .join("opencode-nightly.db")
        .to_string_lossy()
        .into_owned();
    assert!(
        summary
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.locator.as_deref() == Some(broken.as_str())),
        "{:?}",
        summary.diagnostics
    );
    fs::remove_dir_all(&root).ok();
}

/// `sync-opencode` over a directory it can search but not list indexes the
/// configured store and then fails, rather than report a partial import of
/// the channel stores as a complete one.
#[cfg(unix)]
fn sync_opencode_fails_when_the_channel_directory_cannot_be_listed() {
    use std::os::unix::fs::PermissionsExt;
    let (root, home, data) = channel_home("unlisted");
    use_home(&home, None);
    let db_path = root.join("history.db");
    let roots = ProviderRoots::from_home(home.clone(), data.join("opencode.db"));
    fs::set_permissions(&data, fs::Permissions::from_mode(0o300)).unwrap();
    let listable = fs::read_dir(&data).is_ok();
    let result = sync_opencode_with_roots(&db_path, &roots, SyncOutput::Silent);
    fs::set_permissions(&data, fs::Permissions::from_mode(0o755)).unwrap();
    // As root the permissions do not bind and there is nothing to observe.
    if !listable {
        let error = result.expect_err("an unlistable channel directory must fail the sync");
        assert!(
            format!("{error:#}").contains("could not list OpenCode channel databases"),
            "{error:#}"
        );
        assert_eq!(history_sessions(&db_path), vec![ROOT.to_string()]);
    }
    fs::remove_dir_all(&root).ok();
}
