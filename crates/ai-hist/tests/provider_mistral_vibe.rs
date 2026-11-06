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
fn absolute_save_dir_precedes_vibe_home_and_from_home_ignores_overrides() {
    const CHILD: &str = "RH_VIBE_ROOT_PROOF_CHILD";
    if let Some(home) = std::env::var_os(CHILD) {
        let home = PathBuf::from(home);
        let env_store = SessionStore::open(
            StoreOptions::default()
                .db_path(home.join("environment.db"))
                .roots(ProviderRoots::from_env(home.clone())),
        )
        .unwrap();
        assert_eq!(
            env_store
                .discover(DiscoveryOptions::default().sources([Source::MistralVibe]))
                .unwrap()
                .discovered,
            1,
            "absolute save_dir overrides VIBE_HOME"
        );
        let home_store = open(&home);
        assert_eq!(
            home_store
                .discover(DiscoveryOptions::default().sources([Source::MistralVibe]))
                .unwrap()
                .discovered,
            0,
            "from_home must ignore ambient Vibe overrides"
        );
        std::env::set_var("VIBE_SESSION_LOGGING__SAVE_DIR", "relative-session-logs");
        let relative = SessionStore::open(
            StoreOptions::default()
                .db_path(home.join("relative.db"))
                .roots(ProviderRoots::from_env(home.clone())),
        )
        .unwrap();
        assert!(relative
            .discover(DiscoveryOptions::default().sources([Source::MistralVibe]))
            .is_err());
        return;
    }
    let home = tempfile::tempdir().unwrap();
    let save_dir = home.path().join("configured-logs");
    let source = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/mistral-vibe/fixture-home/.vibe/logs/session/unified");
    copy_tree(&source, &save_dir.join("unified"));
    let result = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("absolute_save_dir_precedes_vibe_home_and_from_home_ignores_overrides")
        .arg("--nocapture")
        .env(CHILD, home.path())
        .env("VIBE_HOME", home.path().join("other-vibe-home"))
        .env("VIBE_SESSION_LOGGING__SAVE_DIR", save_dir)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
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
fn directory_pointer_cannot_hide_healthy_sibling() {
    let home = tempfile::tempdir().unwrap();
    let current = stage(home.path());
    let broken = home
        .path()
        .join(".vibe/logs/session/unified/broken/CURRENT");
    std::fs::create_dir_all(&broken).unwrap();
    let store = open(home.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::MistralVibe]))
        .unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
    assert_eq!(evidence(&store).messages.len(), 5);
    std::fs::remove_file(&current).unwrap();
    std::fs::create_dir(&current).unwrap();
    store.sync(SyncOptions::default().force(true)).unwrap();
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
        let rows = conn.prepare("SELECT event_uid FROM session_events WHERE source='mistral-vibe' AND session_id=?1 AND ts_ms=1792609201000 ORDER BY COALESCE(source_order,id),id")
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
fn native_journal_replace_updates_existing_message_and_tool() {
    let home = tempfile::tempdir().unwrap();
    let source = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/provider-fixtures/mistral-vibe/pre-replace-home");
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
    let before = evidence(&store);
    assert_eq!(before.messages.len(), 3);
    assert_eq!(before.tool_calls.len(), 1);
    assert_eq!(before.tool_calls[0].is_error, None);
    assert_eq!(before.tool_results.len(), 0);
    assert!(before.messages.iter().any(|message| message
        .blocks
        .iter()
        .any(|block| { block.text.as_deref() == Some("Pending synthetic answer") })));
    assert_eq!(
        before.tool_calls[0].args.as_ref().unwrap()["path"],
        "/synthetic/repository/README.md"
    );

    let journal = home
        .path()
        .join(".vibe/logs/session/unified")
        .join(SID)
        .join("journal/0000000000000001.jsonl");
    let replacement = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/provider-fixtures/mistral-vibe/replace-home/.vibe/logs/session/unified")
        .join(SID)
        .join("journal/0000000000000001.jsonl");
    std::fs::copy(replacement, journal).unwrap();
    store.sync(SyncOptions::default()).unwrap();
    let after = evidence(&store);
    assert_eq!(after.messages.len(), 3);
    assert_eq!(after.tool_calls.len(), 1);
    assert_eq!(after.tool_results.len(), 1);
    assert_eq!(after.tool_calls[0].tool_use_id, "effect-action-1");
    assert_eq!(after.tool_calls[0].is_error, Some(false));
    assert_eq!(
        after.tool_calls[0].args.as_ref().unwrap()["path"],
        "/synthetic/repository/UPDATED.md"
    );
    assert!(after.messages.iter().any(|message| message
        .blocks
        .iter()
        .any(|block| { block.text.as_deref() == Some("Final synthetic answer") })));
    assert!(!after.messages.iter().any(|message| message
        .blocks
        .iter()
        .any(|block| { block.text.as_deref() == Some("Pending synthetic answer") })));
    assert_eq!(
        after.tool_results[0].text.as_deref(),
        Some("A revised synthetic README")
    );
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

#[cfg(feature = "export")]
#[test]
fn native_export_remote_intake_preserves_native_order() {
    use ai_hist::export::{ExportLimits, ExportSelection, ExportSnapshot};
    use ai_hist::{
        EvidenceKind, EvidenceRecord, NormalizedSourceEvidence, ObservationKey,
        SourceEvidenceSubmission,
    };
    let origin = tempfile::tempdir().unwrap();
    stage(origin.path());
    let source = open(origin.path());
    source
        .discover(DiscoveryOptions::default().sources([Source::MistralVibe]))
        .unwrap();
    source
        .hydrate(
            &SessionRef::id(Source::MistralVibe, SID),
            HydrateOptions::default(),
        )
        .unwrap();
    let kinds = vec![
        EvidenceKind::SessionEvent,
        EvidenceKind::SessionMarker,
        EvidenceKind::ToolCall,
    ];
    let mut snapshot = ExportSnapshot::open(
        rusqlite::Connection::open(origin.path().join("vibe.db")).unwrap(),
        &ExportSelection {
            sources: vec!["mistral-vibe".into()],
            kinds: kinds.iter().map(|kind| kind.as_str().into()).collect(),
            ..Default::default()
        },
        &ExportLimits::default(),
        60_000,
        1_000,
    )
    .unwrap();
    let mut cursor = Some(snapshot.handle().cursor);
    let mut records = Vec::new();
    while let Some(value) = cursor {
        let page = snapshot.page(&value, 1).unwrap();
        cursor = page.next_cursor;
        for record in page.records {
            let mut payload = record.payload.as_object().unwrap().clone();
            payload.remove("id");
            payload.remove("location");
            records.push(EvidenceRecord {
                kind: serde_json::from_value(serde_json::json!(record.kind)).unwrap(),
                payload,
                record_id: Some(record.record_id),
                revision_id: Some(record.revision_id),
            });
        }
    }
    assert!(records
        .iter()
        .any(|record| record.kind == EvidenceKind::ToolCall));
    let receiver = tempfile::tempdir().unwrap();
    let store = open(receiver.path());
    let key = ObservationKey {
        source: "mistral-vibe".into(),
        session_id: SID.into(),
        location: SessionLocation::Remote,
        connector_id: "fixture-remote".into(),
        connector_instance: "default".into(),
    };
    store
        .apply_source_observations(SourceObservationBatch::new(
            "fixture-remote",
            "default",
            SessionLocation::Remote,
            vec![SourceObservation {
                session: ShallowSession {
                    source: "mistral-vibe".into(),
                    session_id: SID.into(),
                    source_stamp: Some("remote-vibe-fixture".into()),
                    ..Default::default()
                },
                raw_locator: None,
            }],
        ))
        .unwrap();
    let revision = store.source_observation(&key).unwrap().revision.unwrap();
    store
        .apply_source_evidence(
            SourceEvidenceSubmission::new(
                key,
                revision,
                NormalizedSourceEvidence {
                    source_stamp: "remote-vibe-fixture".into(),
                    source_bytes: 1,
                    covered_kinds: kinds,
                    records,
                },
            )
            .include_related(false),
        )
        .unwrap();
    let remote = evidence(&store);
    assert_eq!(remote.tool_calls[0].tool_use_id, "effect-action-1");
    let event_ids = |read: &ai_hist::SessionEvidence| {
        read.messages
            .iter()
            .flat_map(|message| &message.blocks)
            .map(|block| block.event_uid.clone())
            .collect::<std::collections::BTreeSet<_>>()
    };
    assert_eq!(event_ids(&remote), event_ids(&evidence(&source)));
    assert_eq!(remote.tool_calls, evidence(&source).tool_calls);
    stage(receiver.path());
    store
        .discover(DiscoveryOptions::default().sources([Source::MistralVibe]))
        .unwrap();
    assert_eq!(remote.messages, evidence(&source).messages);
    for _ in 0..2 {
        store
            .hydrate(
                &SessionRef::id(Source::MistralVibe, SID),
                HydrateOptions::default(),
            )
            .unwrap();
        let after = evidence(&store);
        assert_eq!(after.messages, remote.messages);
        assert_eq!(after.tool_calls, remote.tool_calls);
    }
}
