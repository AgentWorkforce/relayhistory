//! Mistral Vibe 3.0.1 Unified-store writer and independent reader fixture.
mod provider_support;
use ai_hist::{
    CatalogQuery, DiscoveryOptions, HydrateOptions, ProviderRoots, Role, SessionLocation,
    SessionQuery, SessionRef, SessionStore, ShallowSession, Source, SourceObservation,
    SourceObservationBatch, StoreOptions, SyncOptions,
};
use provider_support::{assert_history_conformance, HistoryCase};
use std::path::{Path, PathBuf};

const SID: &str = "vibe-synthetic-001";

fn copy_tree(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let dest = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &dest);
        } else {
            std::fs::copy(entry.path(), dest).unwrap();
        }
    }
}

fn stage(home: &Path) -> PathBuf {
    let source =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mistral-vibe/fixture-home");
    copy_tree(&source, home);
    home.join(".vibe/logs/session/unified")
        .join(SID)
        .join("CURRENT")
}

fn open(home: &Path) -> SessionStore {
    SessionStore::open(StoreOptions::default().db_path(home.join("vibe.db")).roots(
        ProviderRoots::from_home(home.into(), home.join("missing-opencode.db")),
    ))
    .unwrap()
}

fn evidence(store: &SessionStore) -> ai_hist::SessionEvidence {
    store
        .session(
            &SessionRef::id(Source::MistralVibe, SID),
            SessionQuery::default(),
        )
        .unwrap()
        .unwrap()
}

#[test]
fn native_unified_history_reaches_public_api() {
    let home = tempfile::tempdir().unwrap();
    stage(home.path());
    let got = assert_history_conformance(
        home.path(),
        HistoryCase {
            source: Source::MistralVibe,
            session_id: SID,
            messages: &[
                (Role::User, "Explain the synthetic repository"),
                (Role::Assistant, "It contains one fixture."),
                (Role::User, "Check the journal"),
                (Role::Assistant, "Journal captured."),
            ],
        },
    );
    assert_eq!(got.messages.len(), 5);
    assert_eq!(got.tool_calls.len(), 1);
    assert_eq!(got.tool_calls[0].name, "read_file");
}

#[test]
fn incomplete_journal_and_bad_digest_keep_last_good_capture() {
    let home = tempfile::tempdir().unwrap();
    let current = stage(home.path());
    let journal = current
        .parent()
        .unwrap()
        .join("journal/0000000000000001.jsonl");
    let original = std::fs::read(&journal).unwrap();
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::MistralVibe]))
        .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::MistralVibe, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    let before = evidence(&store);
    std::fs::write(
        &journal,
        [original.as_slice(), b"{\"type\":\"projection_delta\""].concat(),
    )
    .unwrap();
    assert!(store
        .hydrate(
            &SessionRef::id(Source::MistralVibe, SID),
            HydrateOptions::default(),
        )
        .is_err());
    assert_eq!(evidence(&store).messages, before.messages);
    std::fs::write(&journal, original).unwrap();
    let chunk = std::fs::read_dir(current.parent().unwrap().join("chunks"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::write(&chunk, b"[]\n").unwrap();
    assert!(store
        .hydrate(
            &SessionRef::id(Source::MistralVibe, SID),
            HydrateOptions::default(),
        )
        .is_err());
    assert_eq!(evidence(&store).messages, before.messages);
}

#[test]
fn root_is_empty_until_a_real_unified_store_is_present() {
    let home = tempfile::tempdir().unwrap();
    let store = open(home.path());
    assert_eq!(
        store
            .discover(DiscoveryOptions::default().sources([Source::MistralVibe]))
            .unwrap()
            .discovered,
        0
    );
    stage(home.path());
    assert_eq!(
        store
            .discover(DiscoveryOptions::default().sources([Source::MistralVibe]))
            .unwrap()
            .discovered,
        1
    );
    let mut query = CatalogQuery::default();
    query.sources = Some(vec![Source::MistralVibe]);
    assert_eq!(store.sessions(query).count(), 1);
    assert!(
        store
            .sync(SyncOptions::default().force(true))
            .unwrap()
            .swept
    );
    assert_eq!(evidence(&store).messages.len(), 5);
}

#[test]
fn native_generation_rewrite_retires_old_local_evidence() {
    let home = tempfile::tempdir().unwrap();
    let current = stage(home.path());
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::MistralVibe]))
        .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::MistralVibe, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    assert_eq!(evidence(&store).tool_calls.len(), 1);
    let replacement = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/provider-fixtures/mistral-vibe/rewrite-home/.vibe/logs/session/unified")
        .join(SID);
    std::fs::remove_dir_all(current.parent().unwrap()).unwrap();
    copy_tree(&replacement, current.parent().unwrap());
    store
        .hydrate(
            &SessionRef::id(Source::MistralVibe, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    let after = evidence(&store);
    assert!(after.tool_calls.is_empty());
    assert_eq!(after.messages.len(), 2);
    assert!(after
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .any(|b| b.text.as_deref() == Some("It contains a revised fixture.")));
    assert!(!after
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .any(|b| b.text.as_deref() == Some("It contains one fixture.")));
}

#[test]
fn explicit_roots_namespace_equal_native_session_ids() {
    let home = tempfile::tempdir().unwrap();
    let baseline = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/mistral-vibe/fixture-home/.vibe/logs/session/unified")
        .join(SID);
    let root_a = home.path().join("profile-a/unified");
    let root_b = home.path().join("profile-b/unified");
    for root in [&root_a, &root_b] {
        copy_tree(&baseline, &root.join(SID));
    }
    let mut roots = serde_json::to_value(ProviderRoots::from_home(
        home.path().into(),
        home.path().join("opencode.db"),
    ))
    .unwrap();
    roots["provider_roots"]["mistral-vibe"] = serde_json::json!([root_a, root_b]);
    roots["provider_default_roots"]
        .as_object_mut()
        .unwrap()
        .remove("mistral-vibe");
    let roots: ProviderRoots = serde_json::from_value(roots).unwrap();
    let store = SessionStore::open(
        StoreOptions::default()
            .db_path(home.path().join("profiles.db"))
            .roots(roots),
    )
    .unwrap();
    assert_eq!(
        store
            .discover(DiscoveryOptions::default().sources([Source::MistralVibe]))
            .unwrap()
            .discovered,
        2
    );
    let mut query = CatalogQuery::default();
    query.sources = Some(vec![Source::MistralVibe]);
    let ids = store
        .sessions(query)
        .map(|row| row.unwrap().session_id)
        .collect::<Vec<_>>();
    assert_eq!(ids.len(), 2);
    assert_ne!(ids[0], ids[1]);
    assert!(ids
        .iter()
        .all(|id| id.starts_with("root:") && id.ends_with(SID)));
}

#[cfg(unix)]
#[test]
fn escaped_sibling_cannot_hide_native_store() {
    use std::os::unix::fs::symlink;
    let home = tempfile::tempdir().unwrap();
    stage(home.path());
    let outside = tempfile::tempdir().unwrap();
    let sibling = home.path().join(".vibe/logs/session/unified/escaped");
    symlink(outside.path(), sibling).unwrap();
    let store = open(home.path());
    assert_eq!(
        store
            .discover(DiscoveryOptions::default().sources([Source::MistralVibe]))
            .unwrap()
            .discovered,
        1
    );
    store
        .hydrate(
            &SessionRef::id(Source::MistralVibe, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    assert_eq!(evidence(&store).messages.len(), 5);
}

#[test]
fn only_native_session_directory_deletion_retires_evidence() {
    let home = tempfile::tempdir().unwrap();
    let current = stage(home.path());
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::MistralVibe]))
        .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::MistralVibe, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    std::fs::remove_file(&current).unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert_eq!(
        evidence(&store).messages.len(),
        5,
        "missing CURRENT within a store is not native deletion"
    );
    std::fs::remove_dir_all(current.parent().unwrap()).unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert!(store
        .session(
            &SessionRef::id(Source::MistralVibe, SID),
            SessionQuery::default()
        )
        .unwrap()
        .is_none());
}

#[test]
fn native_same_millisecond_reorder_keeps_entry_ids_and_tool_state() {
    let home = tempfile::tempdir().unwrap();
    let current = stage(home.path());
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::MistralVibe]))
        .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::MistralVibe, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    let bucket = || {
        let conn = rusqlite::Connection::open(home.path().join("vibe.db")).unwrap();
        let rows = conn.prepare("SELECT event_uid FROM session_events WHERE source='mistral-vibe' AND session_id=?1 AND ts_ms=1792609201000 ORDER BY id")
            .unwrap().query_map([SID], |row| row.get::<_, String>(0)).unwrap()
            .collect::<rusqlite::Result<Vec<_>>>().unwrap();
        rows
    };
    let result_row = || {
        let conn = rusqlite::Connection::open(home.path().join("vibe.db")).unwrap();
        conn.query_row("SELECT id FROM session_events WHERE source='mistral-vibe' AND session_id=?1 AND event_uid='effect-action-1:result'", [SID], |row| row.get::<_, i64>(0)).unwrap()
    };
    assert_eq!(bucket(), ["effect-action-1:call", "assistant-1:0"]);
    let untouched_result_id = result_row();
    let replacement = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/provider-fixtures/mistral-vibe/reorder-home/.vibe/logs/session/unified")
        .join(SID);
    std::fs::remove_dir_all(current.parent().unwrap()).unwrap();
    copy_tree(&replacement, current.parent().unwrap());
    store
        .hydrate(
            &SessionRef::id(Source::MistralVibe, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    assert_eq!(bucket(), ["assistant-1:0", "effect-action-1:call"]);
    assert_eq!(
        result_row(),
        untouched_result_id,
        "effect result at another timestamp was not replayed"
    );
    let after = evidence(&store);
    assert_eq!(after.tool_calls.len(), 1);
    assert_eq!(after.tool_calls[0].tool_use_id, "effect-action-1");
    assert_eq!(after.tool_calls[0].is_error, Some(false));
}

#[test]
fn native_journal_only_append_is_seen_by_unforced_sync() {
    let home = tempfile::tempdir().unwrap();
    let source = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/provider-fixtures/mistral-vibe/pre-append-home");
    copy_tree(&source, home.path());
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::MistralVibe]))
        .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::MistralVibe, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    assert_eq!(evidence(&store).messages.len(), 3);
    store.sync(SyncOptions::default().force(true)).unwrap();
    let journal = home
        .path()
        .join(".vibe/logs/session/unified")
        .join(SID)
        .join("journal/0000000000000001.jsonl");
    let appended = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/mistral-vibe/fixture-home/.vibe/logs/session/unified")
        .join(SID)
        .join("journal/0000000000000001.jsonl");
    std::fs::copy(appended, journal).unwrap();
    store.sync(SyncOptions::default()).unwrap();
    assert_eq!(evidence(&store).messages.len(), 5);
}

#[test]
fn native_deletion_preserves_remote_catalog_after_locator_changes() {
    let home = tempfile::tempdir().unwrap();
    let current = stage(home.path());
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::MistralVibe]))
        .unwrap();
    store
        .hydrate(
            &SessionRef::id(Source::MistralVibe, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    store
        .apply_source_observations(SourceObservationBatch::new(
            "vibe-remote",
            "fixture",
            SessionLocation::Remote,
            vec![SourceObservation {
                session: ShallowSession {
                    source: "mistral-vibe".into(),
                    session_id: SID.into(),
                    title: Some("Remote Vibe title".into()),
                    source_stamp: Some("remote-stamp".into()),
                    ..Default::default()
                },
                raw_locator: Some("remote-vibe-locator".into()),
            }],
        ))
        .unwrap();
    std::fs::remove_dir_all(current.parent().unwrap()).unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    let conn = rusqlite::Connection::open(home.path().join("vibe.db")).unwrap();
    let local: i64 = conn.query_row("SELECT count(*) FROM session_observations WHERE source='mistral-vibe' AND session_id=?1 AND location='local'", [SID], |row| row.get(0)).unwrap();
    let remote: i64 = conn.query_row("SELECT count(*) FROM session_observations WHERE source='mistral-vibe' AND session_id=?1 AND location='remote'", [SID], |row| row.get(0)).unwrap();
    assert_eq!(local, 0);
    assert_eq!(remote, 1);
    let remaining = evidence(&store);
    assert!(remaining.messages.is_empty());
    assert!(remaining.tool_calls.is_empty());
}
