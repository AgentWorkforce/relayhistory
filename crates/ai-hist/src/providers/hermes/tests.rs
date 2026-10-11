use super::*;
use rusqlite::params;
const SQL: &str = include_str!("../../../tests/fixtures/hermes/schema31.sql");
const NATIVE: &str = "chat/not-a-uuid:α";
const SESSION: &str = "default:636861742f6e6f742d612d757569643aceb1";
fn fixture() -> (tempfile::TempDir, ProviderRoots, PathBuf, Connection) {
    let home = tempfile::tempdir().unwrap();
    let roots = ProviderRoots::from_home(home.path().into(), home.path().join("opencode.db"));
    let path = home.path().join(".hermes/state.db");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let db = Connection::open(&path).unwrap();
    db.execute_batch(SQL).unwrap();
    (home, roots, path, db)
}
#[test]
fn official_export_matches_captured_writer_messages() {
    let (_home, roots, path, _db) = fixture();
    let captured = storage::all(&AcquisitionContext::new(ID, &roots), &path).unwrap();
    let oracle: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/hermes/official-export.json"
    ))
    .unwrap();
    assert_eq!(oracle[0]["input_tokens"], 1500);
    assert_eq!(oracle[0]["output_tokens"], 200);
    for s in captured {
        let expected = oracle
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["id"] == s.native_id)
            .unwrap();
        let messages = expected["messages"].as_array().unwrap();
        assert_eq!(messages.len(), s.messages.len());
        for (actual, expected) in s.messages.iter().zip(messages) {
            assert_eq!(Some(actual.role.as_str()), expected["role"].as_str());
            assert_eq!(actual.content.as_deref(), expected["content"].as_str());
            assert_eq!(
                actual.tool_call_id.as_deref(),
                expected["tool_call_id"].as_str()
            );
        }
    }
}
#[test]
fn wal_only_commits_change_discovery_and_captured_generation() {
    let (_home, roots, path, db) = fixture();
    db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0")
        .unwrap();
    let before = storage::enumerate(&roots, None).unwrap();
    let capture =
        || storage::capture(&AcquisitionContext::new(ID, &roots), &path, SESSION).unwrap();
    let first = capture();
    db.execute(
        "UPDATE messages SET content='WAL-only replacement' WHERE id=1",
        [],
    )
    .unwrap();
    let after = storage::enumerate(&roots, None).unwrap();
    assert_ne!(before[0].stamp, after[0].stamp);
    let second = capture();
    assert_ne!(first.stamp, second.stamp);
    assert_eq!(
        first.payload["messages"][0]["content"],
        "Find the Hermes fixture needle"
    );
    assert_eq!(
        second.payload["messages"][0]["content"],
        "WAL-only replacement"
    );
    assert_eq!(second.bytes, 0, "SQLite bytes are unknown");
}
#[test]
fn concurrent_snapshot_never_mixes_session_and_message_generations() {
    let (_home, roots, path, db) = fixture();
    db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0")
        .unwrap();
    db.execute("UPDATE sessions SET model='0'", []).unwrap();
    db.execute("UPDATE messages SET content='0'", []).unwrap();
    let writer = std::thread::spawn(move || {
        for i in 1..30 {
            let tx = db.unchecked_transaction().unwrap();
            tx.execute("UPDATE sessions SET model=?", [i.to_string()])
                .unwrap();
            tx.execute("UPDATE messages SET content=?", [i.to_string()])
                .unwrap();
            tx.commit().unwrap();
        }
    });
    for _ in 0..30 {
        let v = storage::capture(&AcquisitionContext::new(ID, &roots), &path, SESSION)
            .unwrap()
            .payload;
        assert_eq!(v["model"], v["messages"][0]["content"]);
    }
    writer.join().unwrap();
}
#[test]
fn profiles_are_discovered_after_roots_freeze_and_native_ids_do_not_collide() {
    let (home, roots, path, _db) = fixture();
    let before = storage::enumerate(&roots, None).unwrap();
    assert_eq!(before.len(), 2);
    let profile = home.path().join(".hermes/profiles/coder/state.db");
    std::fs::create_dir_all(profile.parent().unwrap()).unwrap();
    Connection::open(&profile)
        .unwrap()
        .execute_batch(SQL)
        .unwrap();
    let after = storage::enumerate(&roots, None).unwrap();
    assert_eq!(after.len(), 4);
    let ids: std::collections::BTreeSet<_> = after.iter().map(|c| c.session_id.clone()).collect();
    assert_eq!(ids.len(), 4);
    let selected = storage::capture(
        &AcquisitionContext::new(ID, &roots),
        &profile,
        "profile:636f646572:636861742f6e6f742d612d757569643aceb1",
    )
    .unwrap();
    assert_eq!(selected.payload["native_id"], NATIVE);
    assert!(storage::capture(
        &AcquisitionContext::new(ID, &roots),
        &path,
        "profile:636f646572:636861742f6e6f742d612d757569643aceb1"
    )
    .is_err());
}
#[test]
fn missing_empty_unsupported_and_malformed_are_distinguished() {
    let home = tempfile::tempdir().unwrap();
    let roots = ProviderRoots::from_home(home.path().into(), home.path().join("opencode.db"));
    assert!(storage::enumerate(&roots, None).unwrap().is_empty());
    let (_home, roots, path, db) = fixture();
    db.execute_batch("DELETE FROM messages; DELETE FROM sessions")
        .unwrap();
    assert!(storage::enumerate(&roots, None).unwrap().is_empty());
    db.execute("UPDATE schema_version SET version=32", [])
        .unwrap();
    assert!(storage::enumerate(&roots, None)
        .unwrap_err()
        .to_string()
        .contains("unsupported Hermes schema 32"));
    drop(db);
    std::fs::write(&path, b"not SQLite").unwrap();
    assert!(storage::enumerate(&roots, None).is_err());
}
#[test]
fn malformed_evidence_rolls_back_without_pruning_previous_rows() {
    let (_home, roots, path, db) = fixture();
    let conn = crate::open_db(&_home.path().join("ledger.db")).unwrap();
    let mut session = storage::all(&AcquisitionContext::new(ID, &roots), &path)
        .unwrap()
        .into_iter()
        .find(|s| s.native_id == NATIVE)
        .unwrap();
    normalize::session(&conn, &session, &path).unwrap();
    let before: i64 = conn
        .query_row(
            "SELECT count(*) FROM session_events WHERE source='hermes'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    session.messages[1].tool_calls = Some("not JSON".into());
    let tx = conn.unchecked_transaction().unwrap();
    assert!(normalize::session(&tx, &session, &path).is_err());
    drop(tx);
    let after: i64 = conn
        .query_row(
            "SELECT count(*) FROM session_events WHERE source='hermes'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(before, after);
    db.execute("UPDATE sessions SET started_at=-1", []).unwrap();
    assert!(storage::enumerate(&roots, None).is_err());
}
#[cfg(unix)]
#[test]
fn escaped_database_sidecar_and_profile_are_rejected_before_open() {
    use std::os::unix::fs::symlink;
    let (home, roots, path, db) = fixture();
    drop(db);
    let outside = tempfile::tempdir().unwrap();
    let secret = outside.path().join("external");
    std::fs::write(&secret, "private").unwrap();
    let sidecar = path.with_file_name("state.db-wal");
    symlink(&secret, &sidecar).unwrap();
    assert!(
        storage::capture(&AcquisitionContext::new(ID, &roots), &path, SESSION)
            .unwrap_err()
            .to_string()
            .contains("SESSION_SOURCE_MISMATCH")
    );
    std::fs::remove_file(sidecar).unwrap();
    std::fs::create_dir_all(home.path().join(".hermes/profiles")).unwrap();
    symlink(outside.path(), home.path().join(".hermes/profiles/escape")).unwrap();
    assert!(storage::enumerate(&roots, None).is_err());
}
#[cfg(unix)]
#[test]
fn denied_database_reports_error_and_does_not_mutate_permissions() {
    use std::os::unix::fs::PermissionsExt;
    let (_home, roots, path, db) = fixture();
    drop(db);
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o0)).unwrap();
    assert!(storage::enumerate(&roots, None).is_err());
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0
    );
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
}
#[test]
fn custom_root_and_old_serialized_roots_do_not_read_host_environment() {
    let (home, mut roots, path, db) = fixture();
    drop(db);
    let custom = home.path().join("custom");
    std::fs::create_dir_all(&custom).unwrap();
    std::fs::rename(path, custom.join("state.db")).unwrap();
    roots.provider_roots.insert(ID.into(), vec![custom]);
    assert_eq!(storage::enumerate(&roots, None).unwrap().len(), 2);
    let mut json = serde_json::to_value(&roots).unwrap();
    json.as_object_mut().unwrap().remove("provider_roots");
    let old: ProviderRoots = serde_json::from_value(json).unwrap();
    assert_eq!(old.for_provider(ID), &[home.path().join(".hermes")]);
}
#[test]
fn sqlite_source_remains_read_only_and_spill_paths_are_only_text() {
    let (_home, roots, path, db) = fixture();
    let before: String = db
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    db.execute(
        "UPDATE messages SET content=? WHERE id=1",
        params!["file:///outside/private.txt"],
    )
    .unwrap();
    let captured = storage::capture(&AcquisitionContext::new(ID, &roots), &path, SESSION).unwrap();
    assert_eq!(
        captured.payload["messages"][0]["content"],
        "file:///outside/private.txt"
    );
    let after: String = db
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    assert_eq!(before, after);
}
#[test]
fn rewritten_metadata_and_retired_events_preserve_remote_shares() {
    let (home, roots, path, _db) = fixture();
    let conn = crate::open_db(&home.path().join("ledger.db")).unwrap();
    let mut session = storage::all(&AcquisitionContext::new(ID, &roots), &path)
        .unwrap()
        .into_iter()
        .find(|s| s.native_id == NATIVE)
        .unwrap();
    session.title = Some("Original title".into());
    normalize::session(&conn, &session, &path).unwrap();
    conn.execute(
        "UPDATE session_events SET location='both' WHERE source='hermes' AND event_uid='2:tool:0'",
        [],
    )
    .unwrap();
    session.messages.retain(|m| m.id != 2);
    session.title = None;
    session.model = None;
    session.cwd = None;
    session.git_branch = None;
    normalize::session(&conn, &session, &path).unwrap();
    let (title, models): (Option<String>, String) = conn
        .query_row(
            "SELECT title,models_json FROM sessions WHERE source='hermes' AND session_id=?",
            [SESSION],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(title, None);
    assert_eq!(models, "[]");
    let cwd: Option<String> = conn
        .query_row(
            "SELECT cwd FROM sessions WHERE source='hermes' AND session_id=?",
            [SESSION],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(cwd, None);
    let location: String = conn
        .query_row(
            "SELECT location FROM session_events WHERE source='hermes' AND event_uid='2:tool:0'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(location, "remote");
    let tools: i64 = conn
        .query_row(
            "SELECT count(*) FROM tool_calls WHERE source='hermes'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(tools, 0);
}
#[test]
fn compression_summary_is_not_a_human_turn_and_parent_is_not_delegation() {
    let (home, roots, path, _db) = fixture();
    let conn = crate::open_db(&home.path().join("ledger.db")).unwrap();
    for s in storage::all(&AcquisitionContext::new(ID, &roots), &path).unwrap() {
        normalize::session(&conn, &s, &path).unwrap();
    }
    let synthetic:i64=conn.query_row("SELECT count(*) FROM session_events WHERE source='hermes' AND control_kind='synthetic'",[],|r|r.get(0)).unwrap();
    assert_eq!(synthetic, 2);
    let history: i64 = conn
        .query_row(
            "SELECT count(*) FROM history WHERE source='hermes' AND prompt LIKE '%summary%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(history, 0);
    let parent:String=conn.query_row("SELECT payload_json FROM session_markers WHERE source='hermes' AND session_id='default:636f6d707265737365642d6368696c64' AND marker_uid='session'",[],|r|r.get(0)).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&parent).unwrap()["parent_session_id"],
        NATIVE
    );
    let count: i64 = conn
        .query_row(
            "SELECT count(*) FROM session_relationships WHERE source='hermes'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 0);
}
#[test]
fn prompt_rewrites_remove_stale_search_text_but_keep_shared_prompt_owners() {
    let (home, roots, path, _db) = fixture();
    let conn = crate::open_db(&home.path().join("ledger.db")).unwrap();
    let mut sessions = storage::all(&AcquisitionContext::new(ID, &roots), &path).unwrap();
    let index = sessions.iter().position(|s| s.native_id == NATIVE).unwrap();
    let mut parent = sessions.remove(index);
    let mut child = sessions.remove(0);
    child.messages =
        serde_json::from_value(serde_json::to_value(&parent.messages).unwrap()).unwrap();
    child.cwd = Some("/fork/project".into());
    normalize::session(&conn, &parent, &path).unwrap();
    normalize::session(&conn, &child, &path).unwrap();
    parent.messages[0].content = Some("Rewritten human request".into());
    normalize::session(&conn, &parent, &path).unwrap();
    let owner:String=conn.query_row("SELECT session_id FROM history WHERE source='hermes' AND prompt='Find the Hermes fixture needle'",[],|r|r.get(0)).unwrap();
    assert_eq!(owner, child.id);
    let project: Option<String> = conn
        .query_row(
            "SELECT project FROM history WHERE prompt='Find the Hermes fixture needle'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(project.as_deref(), Some("/fork/project"));
    child.cwd = None;
    normalize::session(&conn, &child, &path).unwrap();
    let project: Option<String> = conn
        .query_row(
            "SELECT project FROM history WHERE prompt='Find the Hermes fixture needle'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(project, None);
    child.messages.remove(0);
    normalize::session(&conn, &child, &path).unwrap();
    let old:i64=conn.query_row("SELECT count(*) FROM history WHERE source='hermes' AND prompt='Find the Hermes fixture needle'",[],|r|r.get(0)).unwrap();
    assert_eq!(old, 0);
    let new:i64=conn.query_row("SELECT count(*) FROM history WHERE source='hermes' AND prompt='Rewritten human request'",[],|r|r.get(0)).unwrap();
    assert_eq!(new, 1);
    let preview: String = conn
        .query_row(
            "SELECT first_prompt FROM sessions WHERE source='hermes' AND session_id=?",
            [SESSION],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(preview, "Rewritten human request");
}
#[test]
fn watches_filter_existing_profile_configuration_and_track_future_profile_parent() {
    let (_home, roots, _path, _db) = fixture();
    let watches = storage::watch_roots(&roots);
    assert!(watches
        .iter()
        .all(|w| w.depth != crate::discover::WatchDepth::Tree));
    assert!(watches.iter().any(|w| w.path.ends_with("profiles")));
    let main = watches
        .iter()
        .find(|w| w.path.ends_with(".hermes"))
        .unwrap();
    assert!(main.entries.admits(std::ffi::OsStr::new("state.db-wal")));
    assert!(!main.entries.admits(std::ffi::OsStr::new("config.yaml")));
    assert!(!main.entries.admits(std::ffi::OsStr::new(".env")));
}

#[test]
fn cancellation_interrupts_snapshot_collection_before_it_finishes() {
    let (_home, roots, path, _db) = fixture();
    let calls = std::rc::Rc::new(std::cell::Cell::new(0));
    let count = calls.clone();
    let result = crate::ingest::with_capture_stop(
        move || {
            let n = count.get() + 1;
            count.set(n);
            n >= 6
        },
        || {
            let result = storage::all(&AcquisitionContext::new(ID, &roots), &path);
            assert!(result.is_err(), "capture must stop inside its row loops");
            result
        },
    );
    assert!(result.unwrap_err().is::<crate::ingest::CaptureCancelled>());
}
#[test]
fn cancellation_inside_normalization_rolls_back_partial_evidence() {
    let (home, roots, path, _db) = fixture();
    let session = storage::all(&AcquisitionContext::new(ID, &roots), &path)
        .unwrap()
        .remove(0);
    let conn = crate::open_db(&home.path().join("ledger.db")).unwrap();
    let calls = std::rc::Rc::new(std::cell::Cell::new(0));
    let count = calls.clone();
    let result = crate::ingest::with_capture_stop(
        move || {
            let n = count.get() + 1;
            count.set(n);
            n >= 4
        },
        || {
            let tx = conn.unchecked_transaction()?;
            let result = normalize::session(&tx, &session, &path);
            assert!(
                result.is_err(),
                "normalization must stop before finishing all messages"
            );
            result
        },
    );
    assert!(result.unwrap_err().is::<crate::ingest::CaptureCancelled>());
    let rows: i64 = conn
        .query_row("SELECT count(*) FROM session_markers", [], |r| r.get(0))
        .unwrap();
    assert_eq!(rows, 0, "cancelled session writes must roll back");
}

#[test]
fn discovery_ranks_profiles_by_latest_message_and_fingerprint_never_opens_sqlite() {
    let (home, roots, path, db) = fixture();
    let profile = home.path().join(".hermes/profiles/new/state.db");
    std::fs::create_dir_all(profile.parent().unwrap()).unwrap();
    let other = Connection::open(&profile).unwrap();
    other.execute_batch(SQL).unwrap();
    other
        .execute("UPDATE messages SET timestamp=9999999999 WHERE id=1", [])
        .unwrap();
    let ranked = storage::enumerate(&roots, Some(1)).unwrap();
    assert_eq!(ranked.len(), 1);
    assert_eq!(ranked[0].locator, profile.to_string_lossy());
    assert_eq!(ranked[0].recency_hint_ms, Some(9999999999000));
    db.execute("UPDATE messages SET timestamp=99999999999 WHERE id=1", [])
        .unwrap();
    let latest = storage::enumerate(&roots, Some(1)).unwrap();
    assert_eq!(latest[0].locator, path.to_string_lossy());
    drop(db);
    std::fs::write(path, b"not sqlite").unwrap();
    let catalog = Connection::open_in_memory().unwrap();
    let env = DiscoveryEnv::with_provider_roots(&catalog, roots.clone());
    assert!(!Provider.fingerprint_inputs(&env).unwrap().is_empty());
    assert!(storage::enumerate(&roots, None).is_err());
}

#[test]
fn deleted_sessions_retire_only_after_complete_selected_profile_capture() {
    let (home, roots, path, db) = fixture();
    let conn = crate::open_db(&home.path().join("ledger.db")).unwrap();
    let mut state = serde_json::Map::new();
    let repairs = crate::ingest::SweepRepairs::default();
    let run = |state: &mut serde_json::Map<String, Value>, paths| {
        sweep(SweepContext {
            conn: &conn,
            state,
            roots: &roots,
            paths,
            repairs: &repairs,
        })
    };
    run(&mut state, None).unwrap();
    conn.execute(
        "UPDATE session_events SET location='both' WHERE session_id=?1 AND event_uid='1:text'",
        [SESSION],
    )
    .unwrap();
    crate::mark_session_presence(&conn, ID, SESSION, crate::SessionLocation::Remote).unwrap();
    db.execute("DELETE FROM messages WHERE session_id=?1", [NATIVE])
        .unwrap();
    db.execute(
        "UPDATE sessions SET parent_session_id=NULL WHERE parent_session_id=?1",
        [NATIVE],
    )
    .unwrap();
    db.execute("DELETE FROM sessions WHERE id=?1", [NATIVE])
        .unwrap();
    let unrelated =
        crate::ingest::path_scope::PathScope::from_paths([home.path().join("unrelated/state.db")])
            .unwrap();
    run(&mut state, Some(&unrelated)).unwrap();
    assert!(crate::session_locations(&conn, ID, SESSION)
        .unwrap()
        .contains(&"local".into()));
    let wal =
        crate::ingest::path_scope::PathScope::from_paths([path.with_file_name("state.db-wal")])
            .unwrap();
    run(&mut state, Some(&wal)).unwrap();
    assert_eq!(
        crate::session_locations(&conn, ID, SESSION).unwrap(),
        ["remote"]
    );
    let location: String = conn
        .query_row(
            "SELECT location FROM session_events WHERE session_id=?1 AND event_uid='1:text'",
            [SESSION],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(location, "remote");
    db.execute_batch("DELETE FROM messages; DELETE FROM sessions")
        .unwrap();
    db.execute("UPDATE schema_version SET version=32", [])
        .unwrap();
    run(&mut state, Some(&unrelated)).unwrap();
    assert!(run(&mut state, None).is_err());
    let retained: i64 = conn
        .query_row(
            "SELECT count(*) FROM sessions WHERE session_id != ?1",
            [SESSION],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(retained, 1);
    db.execute("UPDATE schema_version SET version=31", [])
        .unwrap();
    run(&mut state, None).unwrap();
    for table in [
        "sessions",
        "session_events",
        "tool_calls",
        "session_markers",
        "history",
    ] {
        let remaining: i64 = conn
            .query_row(
                &format!("SELECT count(*) FROM {table} WHERE source='hermes' AND session_id != ?1"),
                [SESSION],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            remaining, 0,
            "local-only deleted session remains in {table}"
        );
    }
}

#[test]
fn prompt_project_follows_final_owner_and_cwd_rewrites() {
    let (home, roots, path, _db) = fixture();
    let conn = crate::open_db(&home.path().join("ledger.db")).unwrap();
    let mut s = storage::all(&AcquisitionContext::new(ID, &roots), &path)
        .unwrap()
        .into_iter()
        .find(|s| s.native_id == NATIVE)
        .unwrap();
    normalize::session(&conn, &s, &path).unwrap();
    s.cwd = Some("/changed/project".into());
    normalize::session(&conn, &s, &path).unwrap();
    let project: String = conn
        .query_row(
            "SELECT project FROM history WHERE session_id=?1",
            [SESSION],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(project, "/changed/project");
}

#[test]
fn shallow_prompt_excludes_undone_async_and_observed_controls_like_full_capture() {
    let (home, roots, path, db) = fixture();
    let conn = crate::open_db(&home.path().join("ledger.db")).unwrap();
    for assignments in [
        "active=0,compacted=0",
        "display_kind='async_delegation_complete'",
        "observed=1",
        "_compressed_summary=1",
        "display_kind='hidden'",
    ] {
        db.execute("UPDATE messages SET active=1,compacted=0,display_kind=NULL,observed=0,_compressed_summary=0 WHERE id=1", []).unwrap();
        db.execute(&format!("UPDATE messages SET {assignments} WHERE id=1"), [])
            .unwrap();
        let s = storage::all(&AcquisitionContext::new(ID, &roots), &path)
            .unwrap()
            .into_iter()
            .find(|s| s.native_id == NATIVE)
            .unwrap();
        normalize::session(&conn, &s, &path).unwrap();
        let full: Option<String> = conn
            .query_row(
                "SELECT first_prompt FROM sessions WHERE session_id=?1",
                [SESSION],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(storage::first_prompt(&db, NATIVE).unwrap(), full);
        assert_eq!(full.as_deref(), Some("Continue after restart."));
    }
    db.execute("UPDATE messages SET active=0,compacted=1,display_kind=NULL,observed=0,_compressed_summary=0 WHERE id=1", []).unwrap();
    assert_eq!(
        storage::first_prompt(&db, NATIVE).unwrap().as_deref(),
        Some("Find the Hermes fixture needle")
    );
}

#[test]
fn replacement_snapshot_preserves_shared_prompt_identity_and_new_owner_project() {
    let (home, roots, _path, db) = fixture();
    let conn = crate::open_db(&home.path().join("ledger.db")).unwrap();
    let mut state = serde_json::Map::new();
    let repairs = crate::ingest::SweepRepairs::default();
    let run = |state: &mut serde_json::Map<String, Value>| {
        sweep(SweepContext {
            conn: &conn,
            state,
            roots: &roots,
            paths: None,
            repairs: &repairs,
        })
    };
    run(&mut state).unwrap();
    let before: i64 = conn
        .query_row(
            "SELECT id FROM history WHERE prompt='Find the Hermes fixture needle'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    db.execute("UPDATE sessions SET parent_session_id=NULL", [])
        .unwrap();
    db.execute_batch("BEGIN; PRAGMA defer_foreign_keys=ON")
        .unwrap();
    db.execute(
        "UPDATE sessions SET id='replacement',cwd='/replacement' WHERE id=?1",
        [NATIVE],
    )
    .unwrap();
    db.execute(
        "UPDATE messages SET session_id='replacement' WHERE session_id=?1",
        [NATIVE],
    )
    .unwrap();
    db.execute(
        "UPDATE session_model_usage SET session_id='replacement' WHERE session_id=?1",
        [NATIVE],
    )
    .unwrap();
    db.execute_batch("COMMIT").unwrap();
    run(&mut state).unwrap();
    let (id, owner, project): (i64, String, String) = conn.query_row("SELECT id,session_id,project FROM history WHERE prompt='Find the Hermes fixture needle'", [], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap();
    assert_eq!(id, before);
    assert_eq!(owner, "default:7265706c6163656d656e74");
    assert_eq!(project, "/replacement");
}

#[test]
fn native_suffix_and_windows_roots_keep_portable_ids_but_same_path_overrides_do_not() {
    let home = tempfile::tempdir().unwrap();
    for windows in [false, true] {
        let appdata = home.path().join("relocated-appdata");
        let resolve = |custom: Option<PathBuf>| {
            roots::resolve_with(home.path(), true, windows, |key| match key {
                "HERMES_HOME" => custom.as_ref().map(|p| p.as_os_str().to_os_string()),
                "HERMES_DATA_DIR_SUFFIX" => Some("-sandbox".into()),
                "LOCALAPPDATA" => Some(appdata.as_os_str().to_os_string()),
                _ => None,
            })
        };
        let native = resolve(None);
        let root = native.paths[0].clone();
        let path = root.join("state.db");
        let named = root.join("profiles/coder/state.db");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::create_dir_all(named.parent().unwrap()).unwrap();
        Connection::open(&path).unwrap().execute_batch(SQL).unwrap();
        Connection::open(&named)
            .unwrap()
            .execute_batch(SQL)
            .unwrap();
        let mut roots =
            ProviderRoots::from_home(home.path().into(), home.path().join("opencode.db"));
        roots.provider_roots.insert(ID.into(), native.paths);
        roots
            .provider_default_roots
            .insert(ID.into(), native.default_root.unwrap());
        let frozen: ProviderRoots =
            serde_json::from_value(serde_json::to_value(&roots).unwrap()).unwrap();
        assert_eq!(storage::profile(&frozen, &path), "default");
        assert_eq!(storage::profile(&frozen, &named), "profile:636f646572");
        assert!(storage::enumerate(&frozen, None)
            .unwrap()
            .iter()
            .any(|c| c.session_id.as_deref() == Some(SESSION)));
        let custom = resolve(Some(root));
        roots.provider_roots.insert(ID.into(), custom.paths);
        assert!(custom.default_root.is_none());
        roots.provider_default_roots.clear();
        let custom: ProviderRoots =
            serde_json::from_value(serde_json::to_value(&roots).unwrap()).unwrap();
        assert!(storage::profile(&custom, &path).starts_with("root:"));
        assert!(storage::profile(&custom, &named).starts_with("root:"));
        // A changed identity contract retires the old local catalog rather than
        // leaving path-hashed duplicates beside the new portable IDs.
        let conn = crate::open_db(&home.path().join(format!("ledger-{windows}.db"))).unwrap();
        let mut state = serde_json::Map::new();
        let repairs = crate::ingest::SweepRepairs::default();
        for selected in [&custom, &frozen] {
            sweep(SweepContext {
                conn: &conn,
                state: &mut state,
                roots: selected,
                paths: None,
                repairs: &repairs,
            })
            .unwrap();
        }
        let count: i64 = conn
            .query_row(
                "SELECT count(*) FROM sessions WHERE source='hermes'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let stale: i64 = conn
            .query_row(
                "SELECT count(*) FROM sessions WHERE source='hermes' AND session_id LIKE 'root:%'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 4);
        assert_eq!(stale, 0);
    }
}

#[test]
fn legacy_roots_infer_only_the_canonical_default_without_reading_environment() {
    let home = tempfile::tempdir().unwrap();
    let roots = ProviderRoots::from_home(home.path().into(), home.path().join("opencode.db"));
    let mut wire = serde_json::to_value(&roots).unwrap();
    wire.as_object_mut()
        .unwrap()
        .remove("provider_default_roots");
    let legacy: ProviderRoots = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(
        storage::profile(&legacy, &home.path().join(".hermes/state.db")),
        "default"
    );
    wire["provider_roots"][ID] = serde_json::json!([home.path().join(".hermes-suffix")]);
    let legacy: ProviderRoots = serde_json::from_value(wire).unwrap();
    assert!(
        storage::profile(&legacy, &home.path().join(".hermes-suffix/state.db"))
            .starts_with("root:")
    );
}

#[test]
fn deleted_database_retires_only_confirmed_scoped_local_evidence() {
    let (home, roots, path, db) = fixture();
    let conn = crate::open_db(&home.path().join("ledger.db")).unwrap();
    let mut state = serde_json::Map::new();
    let repairs = crate::ingest::SweepRepairs::default();
    let run = |state: &mut serde_json::Map<String, Value>, paths| {
        sweep(SweepContext {
            conn: &conn,
            state,
            roots: &roots,
            paths,
            repairs: &repairs,
        })
    };
    run(&mut state, None).unwrap();
    conn.execute(
        "UPDATE session_events SET location='both' WHERE session_id=?1 AND event_uid='1:text'",
        [SESSION],
    )
    .unwrap();
    crate::mark_session_presence(&conn, ID, SESSION, crate::SessionLocation::Remote).unwrap();
    drop(db);
    let root = path.parent().unwrap();
    let moved = home.path().join("unavailable");
    std::fs::rename(root, &moved).unwrap();
    run(&mut state, None).unwrap();
    assert!(crate::session_locations(&conn, ID, SESSION)
        .unwrap()
        .contains(&"local".into()));
    std::fs::rename(&moved, root).unwrap();
    std::fs::remove_file(&path).unwrap();
    let unrelated =
        crate::ingest::path_scope::PathScope::from_paths([home.path().join("unrelated")]).unwrap();
    run(&mut state, Some(&unrelated)).unwrap();
    assert!(crate::session_locations(&conn, ID, SESSION)
        .unwrap()
        .contains(&"local".into()));
    let selected = crate::ingest::path_scope::PathScope::from_paths([path.clone()]).unwrap();
    run(&mut state, Some(&selected)).unwrap();
    assert_eq!(
        crate::session_locations(&conn, ID, SESSION).unwrap(),
        ["remote"]
    );
    let retained: i64 = conn
        .query_row(
            "SELECT count(*) FROM sessions WHERE source='hermes'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(retained, 1);
    let location: String = conn
        .query_row(
            "SELECT location FROM session_events WHERE session_id=?1 AND event_uid='1:text'",
            [SESSION],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(location, "remote");
    let stale: i64 = conn
        .query_row(
            "SELECT count(*) FROM history WHERE source='hermes' AND session_id != ?1",
            [SESSION],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(stale, 0);
}

#[test]
fn full_snapshot_bounds_activity_and_clears_path_project_identity() {
    let (home, roots, path, _db) = fixture();
    let conn = crate::open_db(&home.path().join("ledger.db")).unwrap();
    let mut s = storage::all(&AcquisitionContext::new(ID, &roots), &path)
        .unwrap()
        .into_iter()
        .find(|s| s.native_id == NATIVE)
        .unwrap();
    normalize::session(&conn, &s, &path).unwrap();
    conn.execute("INSERT INTO session_events(source,session_id,event_uid,ts_ms,role,kind,text,cwd,project_key,project_key_method,location) VALUES ('hermes',?1,'remote-only',1,'user','text','remote fact','/remote/project','/remote/project','path','remote')", [SESSION]).unwrap();
    s.started = s.latest + 1000;
    s.latest = s.started;
    s.cwd = None;
    normalize::session(&conn, &s, &path).unwrap();
    let row: (i64, i64, Option<String>, Option<String>) = conn.query_row("SELECT first_activity_ms,last_activity_ms,cwd,project_key FROM sessions WHERE source='hermes' AND session_id=?1", [SESSION], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).unwrap();
    assert_eq!(row.0, row.1);
    assert_eq!(row.2, None);
    assert_eq!(row.3, None);
    let stale: i64 = conn.query_row("SELECT count(*) FROM session_events WHERE source='hermes' AND session_id=?1 AND location != 'remote' AND project_key IS NOT NULL", [SESSION], |r| r.get(0)).unwrap();
    assert_eq!(stale, 0);
    let remote: (String, String) = conn.query_row("SELECT cwd,project_key FROM session_events WHERE source='hermes' AND session_id=?1 AND event_uid='remote-only'", [SESSION], |r| Ok((r.get(0)?, r.get(1)?))).unwrap();
    assert_eq!(remote, ("/remote/project".into(), "/remote/project".into()));

    conn.execute("UPDATE sessions SET project_key='github.com/acme/remote',project_key_method='remote' WHERE source='hermes' AND session_id=?1", [SESSION]).unwrap();
    conn.execute("UPDATE session_events SET project_key='github.com/acme/remote',project_key_method='remote',location='both' WHERE source='hermes' AND session_id=?1", [SESSION]).unwrap();
    normalize::session(&conn, &s, &path).unwrap();
    let key: String = conn
        .query_row(
            "SELECT project_key FROM sessions WHERE source='hermes' AND session_id=?1",
            [SESSION],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(key, "github.com/acme/remote");
    let lost: i64 = conn.query_row("SELECT count(*) FROM session_events WHERE source='hermes' AND session_id=?1 AND project_key IS NOT 'github.com/acme/remote'", [SESSION], |r| r.get(0)).unwrap();
    assert_eq!(lost, 0);
}

#[test]
fn deleted_profile_database_preserves_unvisited_default_and_rejects_escaped_parent() {
    let (home, roots, path, _db) = fixture();
    let profile = path.parent().unwrap().join("profiles/work/state.db");
    std::fs::create_dir_all(profile.parent().unwrap()).unwrap();
    let db = Connection::open(&profile).unwrap();
    db.execute_batch(SQL).unwrap();
    drop(db);
    let conn = crate::open_db(&home.path().join("ledger.db")).unwrap();
    let mut state = serde_json::Map::new();
    let repairs = crate::ingest::SweepRepairs::default();
    let run = |state: &mut serde_json::Map<String, Value>, paths| {
        sweep(SweepContext {
            conn: &conn,
            state,
            roots: &roots,
            paths,
            repairs: &repairs,
        })
    };
    run(&mut state, None).unwrap();
    std::fs::remove_dir_all(profile.parent().unwrap()).unwrap();
    let selected = crate::ingest::path_scope::PathScope::from_paths([profile.clone()]).unwrap();
    run(&mut state, Some(&selected)).unwrap();
    let rows: Vec<String> = conn
        .prepare("SELECT DISTINCT raw_path FROM sessions WHERE source='hermes'")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(rows, [path.to_string_lossy().into_owned()]);
    #[cfg(unix)]
    {
        let outside = home.path().join("outside");
        std::fs::create_dir(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, profile.parent().unwrap()).unwrap();
        assert!(storage::confirmed_missing(&roots, &profile).is_err());
    }
}

#[test]
fn identical_database_path_namespace_change_invalidates_unforced_sync_fingerprint() {
    let (home, native, path, _db) = fixture();
    let mut custom = native.clone();
    custom.provider_default_roots.remove(ID);
    assert_eq!(native.for_provider(ID), custom.for_provider(ID));
    let catalog = Connection::open_in_memory().unwrap();
    let fingerprint = |roots: &ProviderRoots| {
        let env = DiscoveryEnv::with_provider_roots(&catalog, roots.clone());
        crate::discover::source_fingerprint(&env, &[&Provider]).unwrap()
    };
    let native_fp = fingerprint(&native);
    let custom_fp = fingerprint(&custom);
    assert_ne!(native_fp, custom_fp);
    assert_eq!(native_fp.split(':').nth(1), custom_fp.split(':').nth(1));
    assert_eq!(
        native_fp.split(':').nth(1).unwrap().parse::<u64>().unwrap(),
        std::fs::metadata(&path).unwrap().len()
    );
    let ledger = home.path().join("namespace-ledger.db");
    let open = |roots: ProviderRoots| {
        crate::SessionStore::open(crate::StoreOptions::default().db_path(&ledger).roots(roots))
            .unwrap()
    };
    let store = open(native);
    assert!(store.sync(crate::SyncOptions::default()).unwrap().swept);
    assert!(!store.sync(crate::SyncOptions::default()).unwrap().swept);
    drop(store);
    let store = open(custom);
    assert!(store.sync(crate::SyncOptions::default()).unwrap().swept);
    assert!(!store.sync(crate::SyncOptions::default()).unwrap().swept);
    drop(store);
    let conn = crate::open_db(&ledger).unwrap();
    let (all, custom): (i64, i64) = conn
        .query_row(
            "SELECT count(*),sum(session_id LIKE 'root:%') FROM sessions WHERE source='hermes'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!((all, custom), (2, 2));
}

#[test]
fn recency_query_aggregates_messages_once_and_bounds_selected_capture() {
    let (_home, roots, _path, db) = fixture();
    for selected in [false, true] {
        let query = storage::session_query(selected);
        let plan: Vec<String> = db
            .prepare(&format!("EXPLAIN QUERY PLAN {query}"))
            .unwrap()
            .query_map(params![selected.then_some(NATIVE), 1], |r| r.get(3))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert!(!plan.iter().any(|step| step.contains("CORRELATED")), "{plan:?}");
        assert_eq!(plan.iter().filter(|step| step.contains("SCAN messages")).count(), 1, "{plan:?}");
    }
    // Optional upstream indexes must still constrain selected captures to one
    // native session rather than aggregating the whole message index.
    db.execute("CREATE INDEX recency_test ON messages(session_id,timestamp)", []).unwrap();
    let selected = storage::session_query(true);
    let plan: Vec<String> = db.prepare(&format!("EXPLAIN QUERY PLAN {selected}"))
        .unwrap().query_map(params![NATIVE, 1], |r| r.get(3)).unwrap()
        .map(|r| r.unwrap()).collect();
    assert!(plan.iter().any(|step| step.contains("SEARCH messages") && step.contains("session_id=?")), "{plan:?}");
    db.execute("INSERT INTO sessions(id,source,started_at,ended_at) VALUES('empty','cli',2000000000,2000000001)", []).unwrap();
    db.execute("UPDATE messages SET timestamp=3000000000 WHERE session_id=?1", [NATIVE]).unwrap();
    let candidates = storage::enumerate(&roots, Some(1)).unwrap();
    assert_eq!(candidates[0].session_id.as_deref(), Some(SESSION));
    assert_eq!(candidates[0].recency_hint_ms, Some(3000000000000));
    let selected = storage::session_query(true);
    let latest: f64 = db.query_row(&selected, params!["empty", 1], |r| r.get(9)).unwrap();
    assert_eq!(latest, 2000000001.0);
}
