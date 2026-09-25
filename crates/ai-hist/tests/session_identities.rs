//! `SessionStore::session_identities` through the public surface: every
//! session the store holds evidence for, catalogued or not, paged without
//! gaps or repeats.
//!
//! Public API only, so this runs in the `--no-default-features` job too. The
//! rows are written with a raw connection, standing in for the writers --
//! prompt logs, sidechains, connectors -- that store evidence the catalog
//! never names.

use ai_hist::{
    Change, ChangeQuery, IdentityQuery, SessionIdentity, SessionStore, Source, StoreOptions,
    Watermark,
};
use rusqlite::Connection;
use std::collections::BTreeSet;

struct Store {
    _dir: tempfile::TempDir,
    db: std::path::PathBuf,
    store: SessionStore,
}

fn store() -> Store {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("ai-history.db");
    let mut options = StoreOptions::default();
    options.db_path = Some(db.clone());
    options.home = Some(dir.path().to_path_buf());
    let store = SessionStore::open(options).unwrap();
    Store {
        _dir: dir,
        db,
        store,
    }
}

impl Store {
    fn write(&self, sql: &str) {
        Connection::open(&self.db)
            .unwrap()
            .execute_batch(sql)
            .unwrap();
    }

    fn all(&self, limit: usize) -> Vec<SessionIdentity> {
        let mut identities: Vec<SessionIdentity> = Vec::new();
        loop {
            let mut query = IdentityQuery::default().limit(limit);
            if let Some(last) = identities.last() {
                query = query.after(last.clone());
            }
            let page = self.store.session_identities(query).unwrap();
            if page.is_empty() {
                break;
            }
            // Zero asks for the default page.
            let bound = if limit == 0 { 1_000 } else { limit };
            assert!(page.len() <= bound, "a page is bounded by its limit");
            identities.extend(page);
        }
        identities
    }
}

fn pair(identity: &SessionIdentity) -> (&str, &str) {
    (identity.source_name.as_str(), identity.session_id.as_str())
}

/// Every table that can hold a session the catalog does not name.
const EVIDENCE: &str = r#"
INSERT INTO sessions (session_id, source) VALUES ('catalogued', 'claude');
INSERT INTO session_events (source, session_id, message_id, ts_ms, role, kind, text, event_uid)
    VALUES ('claude', 'sidechain', 'm', 1, 'assistant', 'text', 'x', 'e1'),
           ('claude', 'sidechain', 'm', 2, 'assistant', 'text', 'y', 'e2'),
           ('claude', 'catalogued', 'm', 1, 'user', 'text', 'z', 'e1');
INSERT INTO history (source, session_id, prompt, timestamp_ms)
    VALUES ('codex', 'history-only', 'a prompt', 1),
           ('codex', NULL, 'a prompt with no session', 2),
           ('claude', '', 'a prompt with an empty session', 3);
INSERT INTO session_events (source, session_id, message_id, ts_ms, role, kind, text, event_uid)
    VALUES ('grok', '', 'm', 1, 'user', 'text', 'no session', 'e1');
INSERT INTO trajectories (id, decisions_json, retrospective_json, search_text, updated_ms,
    timestamp_ms) VALUES ('', '[]', '{}', 'x', 1, 1);
INSERT INTO tool_calls (source, session_id, tool_use_id, name)
    VALUES ('claude', 'tool-only', 't1', 'Bash');
INSERT INTO file_edits (source, session_id, tool_use_id, file_path, tool_name)
    VALUES ('claude', 'edit-only', 't1', '/a.rs', 'Edit');
INSERT INTO session_markers (source, session_id, marker_uid, kind)
    VALUES ('grok', 'marker-only', 'mk1', 'compaction');
INSERT INTO session_relationships (source, parent_session_id, relationship_uid,
    child_session_id, relationship, identity_status, evidence_kind, created_ms, updated_ms)
    VALUES ('claude', 'parent-only', 'r1', 'child-named-by-an-edge', 'delegated', 'observed',
        'sidecar', 1, 1);
INSERT INTO session_presences (source, session_id, location)
    VALUES ('opencode', 'presence-only', 'remote');
INSERT INTO session_commit_links (source, session_id, repo, commit_sha, match_method,
    confidence, created_at_ms)
    VALUES ('codex', 'commit-only', 'repo', 'abc', 'trailer', 1.0, 1);
INSERT INTO session_observations (source, session_id, location, connector_id,
    connector_instance, updated_ms)
    VALUES ('some-new-agent', 'observed-only', 'remote', 'conn', 'default', 1);
INSERT INTO observation_evidence (source, session_id, location, connector_id,
    connector_instance, evidence_uid, payload_json)
    VALUES ('some-new-agent', 'observed-only', 'remote', 'conn', 'default', 'ev1', '{}');
INSERT INTO trajectories (id, decisions_json, retrospective_json, search_text, updated_ms,
    timestamp_ms) VALUES ('traj-1', '[]', '{}', 'x', 1, 1);
"#;

/// A session counts when any table stores a row under it: an events-only
/// sidechain and a history-only prompt log appear beside the catalog, each
/// once, in order, while a prompt with no session, rows under an empty
/// session id, and a child only an edge names do not. Every identity listed
/// is one `ChangeQuery::session` accepts.
#[test]
fn every_stored_session_is_an_identity_once() {
    let store = store();
    store.write(EVIDENCE);
    let identities = store.all(1_000);
    let pairs: Vec<(&str, &str)> = identities.iter().map(pair).collect();
    assert_eq!(
        pairs,
        vec![
            ("claude", "catalogued"),
            ("claude", "edit-only"),
            ("claude", "parent-only"),
            ("claude", "sidechain"),
            ("claude", "tool-only"),
            ("codex", "commit-only"),
            ("codex", "history-only"),
            ("grok", "marker-only"),
            ("opencode", "presence-only"),
            ("some-new-agent", "observed-only"),
            ("trajectory", "traj-1"),
        ]
    );
    for identity in &identities {
        assert!(store
            .store
            .changes_since(
                Watermark::START,
                ChangeQuery::default().session(&identity.source_name, &identity.session_id),
            )
            .is_ok());
    }
    assert_eq!(identities[0].source(), Some(Source::Claude));
    assert_eq!(identities[9].source(), None, "an unknown source is carried");
    assert_eq!(identities[10].source(), Some(Source::Trajectory));
}

/// Every page size walks the same identities, completely and without a
/// repeat, and a cursor that names no identity resumes after where it sorts.
#[test]
fn paging_is_complete_and_never_repeats() {
    let store = store();
    store.write(EVIDENCE);
    // Many sessions, each in several tables, so page edges fall inside runs
    // of duplicates across tables.
    let mut bulk = String::new();
    for index in 0..150 {
        bulk.push_str(&format!(
            "INSERT INTO sessions (session_id, source) VALUES ('s{index:03}', 'claude');
             INSERT INTO session_events (source, session_id, message_id, ts_ms, role, kind, \
                 text, event_uid) VALUES ('claude', 's{index:03}', 'm', 1, 'user', 'text', \
                 'x', 'e1'), ('claude', 's{index:03}', 'm', 2, 'user', 'text', 'y', 'e2');
             INSERT INTO history (source, session_id, prompt, timestamp_ms) \
                 VALUES ('claude', 's{index:03}', 'p{index}', {index});\n"
        ));
    }
    store.write(&bulk);
    let whole = store.all(10_000);
    assert_eq!(whole.len(), 11 + 150);
    let distinct: BTreeSet<&SessionIdentity> = whole.iter().collect();
    assert_eq!(distinct.len(), whole.len(), "no identity repeats");
    let mut sorted = whole.clone();
    sorted.sort();
    assert_eq!(sorted, whole, "in (source_name, session_id) order");
    for limit in [1, 2, 3, 7, 64, 0] {
        assert_eq!(store.all(limit), whole, "limit {limit}");
    }

    let resumed = store
        .store
        .session_identities(IdentityQuery::default().after(SessionIdentity::new("claude", "s149~")))
        .unwrap();
    assert_eq!(pair(&resumed[0]), ("claude", "sidechain"));
    let past_the_end = store
        .store
        .session_identities(IdentityQuery::default().after(SessionIdentity::new("zzz", "")))
        .unwrap();
    assert!(past_the_end.is_empty());
}

/// The identities are the sessions the change feed names, and each one's
/// session drain is its own rows.
#[test]
fn identities_are_the_sessions_the_feed_names() {
    let store = store();
    store.write(EVIDENCE);
    let feed: Vec<Change> = store
        .store
        .changes_since(Watermark::START, ChangeQuery::default())
        .unwrap()
        .map(|change| change.unwrap())
        .collect();
    let named: BTreeSet<(String, String)> = feed
        .iter()
        .filter(|change| !change.session_id.is_empty())
        .map(|change| (change.source_name.clone(), change.session_id.clone()))
        .collect();
    let identities: BTreeSet<(String, String)> = store
        .all(1_000)
        .into_iter()
        .map(|identity| (identity.source_name, identity.session_id))
        .collect();
    assert_eq!(identities, named);
    for (source, session) in &identities {
        let drained = store
            .store
            .changes_since(
                Watermark::START,
                ChangeQuery::default().session(source, session),
            )
            .unwrap()
            .count();
        assert!(drained > 0, "{source} {session}");
    }
}

/// `has_session` answers exactly as the listing does: every listed identity
/// exists, and an empty source or session id, or one nothing is stored
/// under, does not.
#[test]
fn has_session_is_true_exactly_for_listed_identities() {
    let store = store();
    store.write(EVIDENCE);
    store.write(
        "INSERT INTO tool_calls (source, session_id, tool_use_id, name) \
         VALUES ('', 'blank-source', 't1', 'Bash');",
    );
    let identities = store.all(1_000);
    assert_eq!(identities.len(), 11);
    for identity in &identities {
        assert!(store.store.has_session(identity).unwrap(), "{identity:?}");
    }
    for (source, session) in [
        ("", "blank-source"),
        ("claude", ""),
        ("grok", ""),
        ("codex", "missing"),
        ("trajectory", "missing"),
        ("claude", "child-named-by-an-edge"),
    ] {
        assert!(
            !store
                .store
                .has_session(&SessionIdentity::new(source, session))
                .unwrap(),
            "{source:?} {session:?}"
        );
    }
}
