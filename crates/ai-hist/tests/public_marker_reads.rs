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

use ai_hist::{EvidenceKind, SessionQuery, SessionRef, SessionStore, Source, StoreOptions};

fn options(db_path: std::path::PathBuf, read_only: bool) -> StoreOptions {
    // Field by field rather than `..Default::default()`: `StoreOptions` is
    // `#[non_exhaustive]`, which is exactly the shape an outside crate sees.
    let mut options = StoreOptions::default();
    options.db_path = Some(db_path);
    options.read_only = read_only;
    options
}

fn markers_only() -> SessionQuery {
    let mut query = SessionQuery::default();
    query.kinds = Some(vec![EvidenceKind::SessionMarker]);
    query
}

#[test]
fn an_embedder_can_read_markers_back_on_the_default_feature_set() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("history.db");
    let store = SessionStore::open(options(db_path.clone(), false)).expect("opening a fresh store");
    // A catalogued session with one marker and nothing else. Written through
    // SQL because the default feature set exposes no writer but `sync`, and
    // a fixture home is what `tests/sourcing_api.rs` covers.
    let conn = rusqlite::Connection::open(&db_path).expect("open");
    conn.execute_batch(
        "INSERT INTO sessions (session_id, source, discovery_state) VALUES ('s', 'claude', 'full');
         INSERT INTO session_markers (source, session_id, marker_uid, ts_ms, kind, subkind, payload_json)
         VALUES ('claude', 's', 'mk1', 5, 'compaction', 'compact_boundary', '{\"trigger\":\"auto\"}');",
    )
    .expect("seed");

    // Empty is a real answer, not an error: a session with no markers reads as
    // an empty list rather than failing, the same as every other kind here.
    let missing = store
        .session(
            &SessionRef::id(Source::Claude, "no-such-session"),
            markers_only(),
        )
        .expect("reading markers must be a supported operation");
    assert!(missing.is_none());

    let evidence = store
        .session(&SessionRef::id(Source::Claude, "s"), markers_only())
        .expect("read")
        .expect("catalogued");
    assert_eq!(evidence.markers.len(), 1);
    let marker = &evidence.markers[0];
    assert_eq!(marker.kind, "compaction");
    assert_eq!(marker.payload, Some(serde_json::json!({"trigger": "auto"})));
    assert_eq!(marker.raw_payload(), Some("{\"trigger\":\"auto\"}"));
    assert_eq!(evidence.loaded, vec![EvidenceKind::SessionMarker]);
    assert!(evidence.messages.is_empty());
}

#[test]
fn a_read_only_open_of_an_outdated_database_says_so_rather_than_guessing() {
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
    let error = SessionStore::open(options(db_path, true))
        .expect_err("an outdated database must be reported at open, not read anyway");
    assert_eq!(error.code(), "DATABASE_OPEN_FAILED");
    let message = error.to_string();
    assert!(
        message.contains("migrate") || message.contains("predates"),
        "the error must tell the caller what to do: {message}"
    );
}
