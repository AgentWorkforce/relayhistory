use super::*;
use crate::{
    CatalogQuery, DiscoveryOptions, HydrateOptions, SessionQuery, SessionRef, SessionStore, Source,
    StoreOptions, SyncOptions,
};

fn fixture() -> (tempfile::TempDir, SessionStore, PathBuf) {
    let temp = tempfile::tempdir().unwrap();
    let roots = ProviderRoots::from_home(temp.path().into(), temp.path().join("opencode.db"));
    let dir = directory(&roots);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("session.json");
    std::fs::write(
        &path,
        include_str!("../../../tests/provider-fixtures/test-fixture/session.json"),
    )
    .unwrap();
    let store = SessionStore::open(
        StoreOptions::default()
            .db_path(temp.path().join("store.db"))
            .roots(roots),
    )
    .unwrap();
    (temp, store, path)
}

#[test]
fn generated_provider_discovery_hydration_read_reopen_and_full_sync() {
    let (temp, store, _) = fixture();
    assert_eq!(
        Source::parse("test-fixture-alias"),
        Some(Source::TestFixture)
    );
    store
        .discover(DiscoveryOptions::default().sources([Source::TestFixture]))
        .unwrap();
    let query = CatalogQuery {
        sources: Some(vec![Source::TestFixture]),
        ..Default::default()
    };
    assert_eq!(store.sessions(query).count(), 1);
    let reference = SessionRef::id(Source::TestFixture, "fixture-session");
    let initial = store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    assert_eq!(initial.status, crate::HydrateStatus::Hydrated);
    let before = store
        .session(&reference, SessionQuery::default())
        .unwrap()
        .unwrap();
    assert_eq!(before.messages.len(), 2);
    assert_eq!(
        store
            .hydrate(&reference, HydrateOptions::default())
            .unwrap()
            .status,
        crate::HydrateStatus::Unchanged
    );
    let watermark = store.head_revision().unwrap();
    store.sync(SyncOptions::default()).unwrap();
    assert_eq!(
        store
            .session(&reference, SessionQuery::default())
            .unwrap()
            .unwrap()
            .messages,
        before.messages
    );
    assert!(store.head_revision().unwrap() >= watermark);
    drop(store);
    let roots = ProviderRoots::from_home(temp.path().into(), temp.path().join("opencode.db"));
    let store = SessionStore::open(
        StoreOptions::default()
            .db_path(temp.path().join("store.db"))
            .roots(roots),
    )
    .unwrap();
    assert_eq!(
        store
            .session(&reference, SessionQuery::default())
            .unwrap()
            .unwrap()
            .messages,
        before.messages
    );
}

#[test]
fn generated_provider_scoped_watch_sweep_reads_changes_and_exposes_roots() {
    let (_temp, store, path) = fixture();
    let roots = Source::TestFixture
        .capabilities()
        .watch_roots(store.roots());
    assert!(roots.iter().any(|root| path.starts_with(&root.path)));
    let conn = crate::open_db(store.db_path()).unwrap();
    let scope = crate::ingest::SweepScope::only([ID]);
    assert!(!scope.reads_nothing());
    crate::ingest::sync_basic(&conn, store.db_path(), store.roots(), true, &scope).unwrap();
    let mut value: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    value["messages"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({"id":"u2","role":"user","text":"watch update","ts":3000}));
    std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
    crate::ingest::sync_basic(&conn, store.db_path(), store.roots(), true, &scope).unwrap();
    let evidence = store
        .session(
            &SessionRef::id(Source::TestFixture, "fixture-session"),
            SessionQuery::default(),
        )
        .unwrap()
        .unwrap();
    assert_eq!(evidence.messages.len(), 3);
}

#[test]
fn generated_provider_path_scoped_tick_preserves_unvisited_sibling() {
    let (_temp, store, path) = fixture();
    let sibling = path.parent().unwrap().join("sibling.json");
    let mut value: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    value["id"] = "sibling-session".into();
    std::fs::write(&sibling, serde_json::to_vec(&value).unwrap()).unwrap();
    let conn = crate::open_db(store.db_path()).unwrap();
    let scope = crate::ingest::SweepScope::only([ID]).with_paths(
        crate::ingest::path_scope::PathScope::from_paths([path.clone()]),
    );
    crate::ingest::sync_basic(&conn, store.db_path(), store.roots(), true, &scope).unwrap();
    let read = |id| {
        store
            .session(
                &SessionRef::id(Source::TestFixture, id),
                SessionQuery::default(),
            )
            .unwrap()
    };
    assert!(read("fixture-session").is_some());
    assert!(
        read("sibling-session").is_none(),
        "scoped discovery and ingestion skip siblings"
    );
    crate::ingest::sync_basic(
        &conn,
        store.db_path(),
        store.roots(),
        true,
        &crate::ingest::SweepScope::only([ID]),
    )
    .unwrap();
    assert!(
        read("sibling-session").is_some(),
        "full backstop still reads siblings"
    );
    std::fs::write(&sibling, "unreadable sibling payload").unwrap();
    crate::ingest::sync_basic(&conn, store.db_path(), store.roots(), true, &scope).unwrap();
    assert!(
        read("sibling-session").is_some(),
        "unvisited evidence is retained"
    );
}

#[test]
fn captured_file_payload_cannot_change_between_stamp_and_ingest() {
    let (_temp, store, path) = fixture();
    let options = HydrateSessionOptions {
        source: ID.into(),
        session_id: "fixture-session".into(),
        scope: crate::SessionScope::Local,
        include_related: true,
    };
    let acquired = acquire(&options, &path, &AcquisitionContext::new(ID, store.roots())).unwrap();
    std::fs::write(&path, "broken later file").unwrap();
    let conn = crate::open_db(store.db_path()).unwrap();
    normalize(&conn, &acquired.payload, &path).unwrap();
    let evidence = store
        .session(
            &SessionRef::id(Source::TestFixture, "fixture-session"),
            SessionQuery::default(),
        )
        .unwrap()
        .unwrap();
    assert_eq!(evidence.messages.len(), 2);
}

#[test]
fn acquisition_rejects_external_and_symlinked_sidecars_before_reading() {
    let (_temp, store, path) = fixture();
    let outside = tempfile::tempdir().unwrap();
    let external = outside.path().join("private.json");
    std::fs::write(&external, "must never be read").unwrap();
    let context = AcquisitionContext::new(ID, store.roots());
    assert!(file_snapshot(&context, vec![external.clone()]).is_err());
    assert!(sqlite_snapshot(&context, &external, |_| -> Result<()> {
        panic!("must not open external database")
    })
    .is_err());
    #[cfg(unix)]
    {
        let link = path.parent().unwrap().join("external.json");
        std::os::unix::fs::symlink(&external, &link).unwrap();
        assert!(file_snapshot(&context, vec![link]).is_err());
    }
}

#[test]
fn provider_roots_are_frozen_and_old_serialized_roots_gain_home_defaults() {
    let temp = tempfile::tempdir().unwrap();
    let mut roots = ProviderRoots::from_home(temp.path().into(), temp.path().join("opencode.db"));
    let mut resolver_value = temp.path().join("first");
    roots
        .provider_roots
        .insert(ID.into(), vec![super::super::resolve_root_value(temp.path(), Some(resolver_value.clone().into_os_string()), ".history-fixture")]);
    resolver_value = temp.path().join("second");
    assert_ne!(directory(&roots), resolver_value);
    assert_eq!(directory(&roots), temp.path().join("first"));
    assert_eq!(
        Source::TestFixture.capabilities().watch_roots(&roots)[0].path,
        temp.path().join("first")
    );
    let isolated = ProviderRoots::from_home(temp.path().into(), temp.path().join("opencode.db"));
    assert_eq!(directory(&isolated), temp.path().join(".history-fixture"));
    let mut serialized = serde_json::to_value(&roots).unwrap();
    serialized.as_object_mut().unwrap().remove("provider_roots");
    let old: ProviderRoots = serde_json::from_value(serialized).unwrap();
    assert_eq!(directory(&old), temp.path().join(".history-fixture"));
}

#[test]
fn provider_generation_upgrade_resets_old_cursor_without_global_parser_bump() {
    let (_temp, store, _) = fixture();
    store
        .discover(DiscoveryOptions::default().sources([Source::TestFixture]))
        .unwrap();
    let reference = SessionRef::id(Source::TestFixture, "fixture-session");
    store
        .hydrate(&reference, HydrateOptions::default())
        .unwrap();
    let conn = crate::open_db(store.db_path()).unwrap();
    for table in [
        "session_hydration_checkpoints",
        "observation_hydration_checkpoints",
    ] {
        conn.execute(&format!("UPDATE {table} SET source_stamp = 'test_fixture_v0:old', parser_state_json = '{{\"v\":1,\"must_reset\":true}}' WHERE source = ?"), [ID]).unwrap();
    }
    assert_eq!(
        store
            .hydrate(&reference, HydrateOptions::default())
            .unwrap()
            .status,
        crate::HydrateStatus::Updated
    );
    let raw: String = conn
        .query_row(
            "SELECT parser_state_json FROM session_hydration_checkpoints WHERE source=?",
            [ID],
            |r| r.get(0),
        )
        .unwrap();
    let cursor: Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(cursor["upgraded"], true);
    assert!(cursor.get("must_reset").is_none());
    assert_eq!(
        store
            .hydrate(&reference, HydrateOptions::default())
            .unwrap()
            .status,
        crate::HydrateStatus::Unchanged
    );
}

fn assert_unforced_loss_repair(table: &str) {
    let (temp, store, _) = fixture();
    store.sync(SyncOptions::default()).unwrap();
    let reference = SessionRef::id(Source::TestFixture, "fixture-session");
    let before = store.session(&reference, SessionQuery::default()).unwrap().unwrap();
    let conn = Connection::open(temp.path().join("store.db")).unwrap();
    let count = || conn.query_row(&format!("SELECT count(*) FROM {table} WHERE source = 'test-fixture'"), [], |r| r.get::<_, i64>(0)).unwrap();
    let expected = count();
    assert!(expected > 0);
    conn.execute(&format!("DELETE FROM {table} WHERE rowid IN (SELECT rowid FROM {table} WHERE source = 'test-fixture' LIMIT 1)"), []).unwrap();
    assert_eq!(count(), expected - 1);
    store.sync(SyncOptions::default()).unwrap();
    assert_eq!(count(), expected, "lost {table} row must be replayed on unchanged bytes");
    assert_eq!(store.session(&reference, SessionQuery::default()).unwrap().unwrap().messages, before.messages);
}
#[test]
fn unforced_sync_repairs_adapter_partial_event_loss() { assert_unforced_loss_repair("session_events"); }
#[test]
fn unforced_sync_repairs_adapter_history_loss() { assert_unforced_loss_repair("history"); }
#[test]
fn unforced_sync_repairs_adapter_marker_loss() { assert_unforced_loss_repair("session_markers"); }
#[test]
fn unforced_sync_repairs_adapter_catalog_loss() { assert_unforced_loss_repair("sessions"); }

#[test]
fn public_hydration_preserves_sidecar_source_mismatch_classification() {
    let (temp, store, path) = fixture();
    let external = temp.path().join("external.json");
    std::fs::write(&external, "{}").unwrap();
    let mut value: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    value["sidecar"] = serde_json::json!(external);
    std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
    store
        .discover(DiscoveryOptions::default().sources([Source::TestFixture]))
        .unwrap();
    let error = store
        .hydrate(
            &SessionRef::id(Source::TestFixture, "fixture-session"),
            HydrateOptions::default(),
        )
        .unwrap_err();
    assert_eq!(error.code(), "SESSION_SOURCE_MISMATCH");
}
