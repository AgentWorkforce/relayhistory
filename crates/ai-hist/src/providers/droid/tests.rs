use super::*;
#[test]
fn rejects_unknown_schema_and_complete_corruption_but_defers_tails() {
    assert!(
        format::parse(b"{\"type\":\"session_start\",\"version\":3}\n", false)
            .unwrap_err()
            .to_string()
            .contains("UNSUPPORTED_SCHEMA")
    );
    assert!(format::parse(b"{\n", false)
        .unwrap_err()
        .to_string()
        .contains("INVALID_RECORD"));
    let partial =
        b"{\"type\":\"session_start\",\"id\":\"partial\",\"version\":2}\n{\"type\":\"message\"";
    assert_eq!(format::parse(partial, false).unwrap().len(), 1);
    assert!(format::parse(b"{", true).is_err());
}
#[test]
fn prompts_only_include_typed_text() {
    let blocks = serde_json::json!([{"type":"tool_result","text":"not human"},{"type":"future","text":"not known"},{"type":"text","text":"Human text"}]);
    assert_eq!(format::text(&blocks), "Human text");
}

const FIXTURE: &str = include_str!(
    "../../../tests/fixtures/droid/v2-synthetic/.factory/sessions/-fixture/factory-fixture.jsonl"
);
fn fixture() -> (tempfile::TempDir, crate::SessionStore, PathBuf) {
    let temp = tempfile::tempdir().unwrap();
    let roots = ProviderRoots::from_home(temp.path().into(), temp.path().join("opencode.db"));
    let path = roots.for_provider(ID)[0].join("C-Projects-fixture/session.jsonl");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, FIXTURE).unwrap();
    let store = crate::SessionStore::open(
        crate::StoreOptions::default()
            .db_path(temp.path().join("store.db"))
            .roots(roots),
    )
    .unwrap();
    (temp, store, path)
}
#[test]
fn selected_snapshot_is_immutable_and_discovery_prefix_is_bounded() {
    let (_temp, store, path) = fixture();
    let ctx = AcquisitionContext::new(ID, store.roots());
    let captured = snapshot(&path, &ctx).unwrap();
    std::fs::write(&path, FIXTURE.replace("Hello Factory", "Changed on disk")).unwrap();
    let conn = crate::open_db(store.db_path()).unwrap();
    let tx = conn.unchecked_transaction().unwrap();
    normalize::write(&tx, &captured.payload, &path).unwrap();
    tx.commit().unwrap();
    let evidence = store
        .session(
            &crate::SessionRef::id(crate::Source::Droid, "factory-fixture"),
            crate::SessionQuery::default(),
        )
        .unwrap()
        .unwrap();
    assert!(evidence
        .messages
        .iter()
        .flat_map(|m| &m.blocks)
        .any(|b| b.text.as_deref() == Some("The greeting says Hello Factory fixture.")));
    std::fs::write(&path, vec![b'a'; 128 * 1024]).unwrap();
    assert_eq!(prefix(&ctx, &path).unwrap().len(), HEAD_LIMIT as usize);
    assert!(format::parse(&prefix(&ctx, &path).unwrap(), true).is_err());
}
#[test]
fn scoped_tick_skips_sibling_and_includes_settings_only_changes() {
    let (_temp, store, path) = fixture();
    let sibling = path.with_file_name("sibling.jsonl");
    std::fs::write(
        &sibling,
        FIXTURE.replace("factory-fixture", "other-session"),
    )
    .unwrap();
    let conn = crate::open_db(store.db_path()).unwrap();
    let scope = crate::ingest::SweepScope::only([ID]).with_paths(
        crate::ingest::path_scope::PathScope::from_paths([path.clone()]),
    );
    crate::ingest::sync_basic(&conn, store.db_path(), store.roots(), true, &scope).unwrap();
    let read = |id| {
        store
            .session(
                &crate::SessionRef::id(crate::Source::Droid, id),
                crate::SessionQuery::default(),
            )
            .unwrap()
    };
    assert!(read("factory-fixture").is_some());
    assert!(read("other-session").is_none());
    crate::ingest::sync_basic(
        &conn,
        store.db_path(),
        store.roots(),
        true,
        &crate::ingest::SweepScope::only([ID]),
    )
    .unwrap();
    assert!(read("other-session").is_some());
    std::fs::write(&sibling, "malformed sibling\n").unwrap();
    crate::ingest::sync_basic(&conn, store.db_path(), store.roots(), true, &scope).unwrap();
    assert!(read("other-session").is_some());
    let settings = path.with_extension("settings.json");
    std::fs::write(&settings, "{\"model\":\"settings-only\"}").unwrap();
    let scope = crate::ingest::SweepScope::only([ID])
        .with_paths(crate::ingest::path_scope::PathScope::from_paths([settings]));
    crate::ingest::sync_basic(&conn, store.db_path(), store.roots(), true, &scope).unwrap();
    assert!(read("factory-fixture")
        .unwrap()
        .markers
        .iter()
        .any(|m| m.raw_payload().is_some_and(|p| p.contains("settings-only"))));
}
#[cfg(unix)]
#[test]
fn escaped_primary_and_settings_symlinks_are_never_read() {
    use std::os::unix::fs::symlink;
    let (_temp, store, path) = fixture();
    let outside = tempfile::tempdir().unwrap();
    let private = outside.path().join("outside.jsonl");
    std::fs::write(&private, FIXTURE).unwrap();
    let ctx = AcquisitionContext::new(ID, store.roots());
    symlink(&private, path.with_extension("settings.json")).unwrap();
    assert!(snapshot(&path, &ctx).is_err());
    std::fs::remove_file(path.with_extension("settings.json")).unwrap();
    std::fs::remove_file(&path).unwrap();
    symlink(&private, &path).unwrap();
    assert!(snapshot(&path, &ctx).is_err());
}

#[cfg(unix)]
#[test]
fn one_failed_sidecar_does_not_block_discovery_or_capture_of_healthy_sessions() {
    use std::os::unix::fs::symlink;
    let (_temp, store, path) = fixture();
    let bad = path.with_file_name("bad.jsonl");
    std::fs::write(&bad, FIXTURE.replace("factory-fixture", "bad-session")).unwrap();
    let outside = tempfile::tempdir().unwrap();
    let inaccessible = outside.path().join("not-authorized.json");
    std::fs::write(&inaccessible, "{}").unwrap();
    symlink(inaccessible, bad.with_extension("settings.json")).unwrap();
    let report = store
        .discover(crate::DiscoveryOptions::default().sources([crate::Source::Droid]))
        .unwrap();
    assert_eq!(report.discovered, 1);
    assert_eq!(report.diagnostics.len(), 1);
    assert!(report.diagnostics[0]
        .subject
        .as_deref()
        .is_some_and(|p| p.ends_with("bad.jsonl")));
    let sync = store
        .sync(crate::SyncOptions::default().sources([crate::Source::Droid]))
        .unwrap();
    assert!(sync
        .diagnostics
        .iter()
        .any(|diagnostic| diagnostic.source == crate::Source::Droid
            && diagnostic
                .subject
                .as_deref()
                .is_some_and(|path| path.ends_with("bad.jsonl"))));
    let evidence = store
        .session(
            &crate::SessionRef::id(crate::Source::Droid, "factory-fixture"),
            crate::SessionQuery::default(),
        )
        .unwrap()
        .unwrap();
    assert!(!evidence.messages.is_empty());
    // A partial source pass must not retire prior evidence whose file disappeared.
    std::fs::remove_file(&path).unwrap();
    store
        .sync(crate::SyncOptions::default().sources([crate::Source::Droid]))
        .unwrap();
    let retained = store
        .session(
            &crate::SessionRef::id(crate::Source::Droid, "factory-fixture"),
            crate::SessionQuery::default(),
        )
        .unwrap()
        .unwrap();
    assert_eq!(retained.messages.len(), evidence.messages.len());
}

#[test]
fn snapshot_read_limits_and_recent_shallow_activity_are_bounded() {
    let (_temp, store, path) = fixture();
    let ctx = AcquisitionContext::new(ID, store.roots());
    assert!(storage::read_limited(&path, &ctx, 64, "DROID_TEST_LIMIT")
        .unwrap_err()
        .to_string()
        .contains("DROID_TEST_LIMIT"));
    let mut rows =
        vec![serde_json::json!({"type":"session_start","id":"factory-fixture","version":2})];
    for index in 0..200 {
        let timestamp = chrono::DateTime::from_timestamp(1793491200 + index, 0)
            .unwrap()
            .to_rfc3339();
        rows.push(serde_json::json!({"type":"message","id":format!("m{index}"),"timestamp":timestamp,"message":{"role":"user","content":[{"type":"text","text":"bounded shallow test"}]}}));
    }
    std::fs::write(
        &path,
        rows.iter()
            .map(|row| format!("{row}\n"))
            .collect::<String>(),
    )
    .unwrap();
    store
        .discover(crate::DiscoveryOptions::default().sources([crate::Source::Droid]))
        .unwrap();
    let evidence = store
        .session(
            &crate::SessionRef::id(crate::Source::Droid, "factory-fixture"),
            crate::SessionQuery::default(),
        )
        .unwrap()
        .unwrap();
    assert_eq!(evidence.session.last_activity_ms, Some(1793491399000));
    assert!(storage::tail(&path, &ctx).unwrap().len() <= HEAD_LIMIT as usize);
}
