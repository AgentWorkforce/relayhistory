//! Markers must be readable by an embedder on the crate's *default* features.
//!
//! This file deliberately has no `[[test]]` entry in `Cargo.toml`, so it builds
//! with `default = []` — no `unstable-internal`, no `delivery`. That is the
//! surface an external consumer of this crate actually gets, and it is the only
//! way to show that a read path exists there: every other integration test in
//! this crate requires `unstable-internal`, which re-exports the whole `store`
//! module and so can reach anything whether or not it is public.
//!
//! The sourcing contract says marker evidence is readable through the Rust SDK.
//! A marker synced by an embedder with no supported way to read it back is a
//! write-only table for everyone outside this workspace.

use ai_hist::{SessionMarkerPage, SessionStore, Source, StoreOptions};

fn options(db_path: std::path::PathBuf, read_only: bool) -> StoreOptions {
    // Field by field rather than `..Default::default()`: `StoreOptions` is
    // `#[non_exhaustive]`, which is exactly the shape an outside crate sees.
    let mut options = StoreOptions::default();
    options.db_path = Some(db_path);
    options.read_only = read_only;
    options
}

#[test]
fn an_embedder_can_read_markers_back_on_the_default_feature_set() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = SessionStore::open(options(dir.path().join("history.db"), false))
        .expect("opening a fresh store");

    // Empty is a real answer, not an error: a session with no markers reads as
    // an empty page rather than failing, the same as every other page here.
    let page: SessionMarkerPage = store
        .session_markers_page(Source::Claude, "no-such-session", 10, None)
        .expect("reading markers must be a supported operation");
    assert!(page.markers.is_empty());
    assert!(page.next_cursor.is_none());
}

#[test]
fn a_marker_page_from_an_outdated_database_says_so_rather_than_guessing() {
    let dir = tempfile::tempdir().expect("tempdir");
    // Create the database, then take the marker page index away, which is what
    // a database written before this schema looks like to the read guard.
    let db_path = dir.path().join("history.db");
    {
        SessionStore::open(options(db_path.clone(), false)).expect("opening a fresh store");
        let conn = rusqlite::Connection::open(&db_path).expect("open");
        conn.execute_batch("DROP INDEX IF EXISTS idx_session_markers_page;")
            .expect("drop index");
    }
    let store = SessionStore::open(options(db_path, true))
        .expect("a read-only open of an existing database");
    let error = store
        .session_markers_page(Source::Claude, "s", 10, None)
        .expect_err("an outdated database must be reported, not read anyway");
    let message = error.to_string();
    assert!(
        message.contains("migrate") || message.contains("predates"),
        "the error must tell the caller what to do: {message}"
    );
}
