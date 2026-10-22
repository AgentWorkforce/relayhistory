use super::*;
use rusqlite::params;
const SQL: &str = include_str!("../../../tests/fixtures/deepagents/sessions.sql");
fn fixture() -> (tempfile::TempDir, ProviderRoots, PathBuf, Connection) {
    let home = tempfile::tempdir().unwrap();
    let roots = ProviderRoots::from_home(home.path().into(), home.path().join("opencode.db"));
    let path = home.path().join(".deepagents/.state/sessions.db");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let db = Connection::open(&path).unwrap();
    db.execute_batch(SQL).unwrap();
    (home, roots, path, db)
}
#[test]
fn native_writer_and_official_inspector_agree_on_all_four_threads() {
    let (_home, roots, path, _db) = fixture();
    let captured = storage::all(&AcquisitionContext::new(ID, &roots), &path).unwrap();
    assert_eq!(captured.unread, 0);
    assert_eq!(captured.sessions.len(), 4);
    for (thread, oracle) in [
        (
            "443",
            include_str!("../../../tests/fixtures/deepagents/official-443.json"),
        ),
        (
            "444",
            include_str!("../../../tests/fixtures/deepagents/official-444.json"),
        ),
        (
            "445",
            include_str!("../../../tests/fixtures/deepagents/official-445.json"),
        ),
        (
            "446",
            include_str!("../../../tests/fixtures/deepagents/official-446.json"),
        ),
    ] {
        let expected: serde_json::Value = serde_json::from_str(oracle).unwrap();
        let actual = captured
            .sessions
            .iter()
            .find(|s| s.native_id.ends_with(thread))
            .unwrap();
        let expected_messages: Vec<&serde_json::Value> = expected["preamble"]
            .as_array()
            .unwrap()
            .iter()
            .chain(
                expected["turns"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .flat_map(|turn| turn["messages"].as_array().unwrap().iter()),
            )
            .collect();
        assert_eq!(
            actual.messages.len(),
            expected_messages.len(),
            "thread {thread}"
        );
        for (message, expected) in actual.messages.iter().zip(expected_messages) {
            assert_eq!(message.id, expected["id"], "thread {thread}");
            assert_eq!(message.role, expected["role"], "thread {thread}");
            assert_eq!(message.content, expected["content"], "thread {thread}");
        }
    }
    let revised = captured
        .sessions
        .iter()
        .find(|s| s.native_id.ends_with("445"))
        .unwrap();
    assert_eq!(
        revised
            .messages
            .iter()
            .map(|m| m.id.as_str())
            .collect::<Vec<_>>(),
        ["keep", "revise"]
    );
    let compacted = captured
        .sessions
        .iter()
        .find(|s| s.native_id.ends_with("444"))
        .unwrap();
    assert_eq!(
        compacted
            .messages
            .iter()
            .map(|m| m.id.as_str())
            .collect::<Vec<_>>(),
        ["compact-human", "compact-ai"]
    );
    let native: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/deepagents/native-446-state.json"
    ))
    .unwrap();
    let graph = captured
        .sessions
        .iter()
        .find(|s| s.native_id.ends_with("446"))
        .unwrap();
    assert_eq!(
        graph.messages.len(),
        111,
        "crossed actual DeltaChannel snapshot boundary"
    );
    for (actual, expected) in graph
        .messages
        .iter()
        .zip(native["messages"].as_array().unwrap())
    {
        assert_eq!(actual.id, expected["id"]);
        assert_eq!(actual.content, expected["content"]);
        assert_eq!(
            actual.role,
            match expected["role"].as_str().unwrap() {
                "human" => "user",
                "ai" => "assistant",
                other => other,
            }
        );
    }
}
#[test]
fn current_delta_checkpoint_time_drives_discovery_and_shallow_recency() {
    let (home, roots, path, _db) = fixture();
    let full = storage::all(&AcquisitionContext::new(ID, &roots), &path)
        .unwrap()
        .sessions;
    let graph = full.iter().find(|s| s.native_id.ends_with("446")).unwrap();
    let candidates = storage::enumerate(&roots, None).unwrap();
    let graph_candidate = candidates
        .iter()
        .find(|candidate| candidate.session_id.as_deref() == Some(graph.id.as_str()))
        .unwrap();
    assert_eq!(graph_candidate.recency_hint_ms, Some(graph.latest));
    let store = crate::SessionStore::open(
        crate::StoreOptions::default()
            .db_path(home.path().join("catalog.db"))
            .roots(roots),
    )
    .unwrap();
    store
        .discover(crate::DiscoveryOptions::default().sources([crate::Source::DeepAgents]))
        .unwrap();
    let conn = Connection::open(store.db_path()).unwrap();
    let shallow: i64 = conn
        .query_row(
            "SELECT last_activity_ms FROM sessions WHERE source='deepagents' AND session_id=?1",
            [&graph.id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(shallow, graph.latest);
}
#[test]
fn discovery_isolates_corrupt_file_and_directory_roots() {
    let (home, roots, good, _db) = fixture();
    let corrupt = home.path().join("corrupt.db");
    std::fs::write(&corrupt, b"not a SQLite database").unwrap();
    let directory = home.path().join("directory.db");
    std::fs::create_dir(&directory).unwrap();
    let mut config = serde_json::to_value(roots).unwrap();
    config["provider_roots"]["deepagents"] = serde_json::json!([corrupt, directory, good]);
    let mixed: ProviderRoots = serde_json::from_value(config).unwrap();
    let found = storage::enumerate(&mixed, None).unwrap();
    assert_eq!(
        found.len(),
        4,
        "healthy conversations survive two bad siblings"
    );
    let fingerprints = storage::fingerprint_inputs(&mixed).unwrap();
    assert!(fingerprints
        .iter()
        .any(|candidate| candidate.locator == good.to_string_lossy()));
    assert!(fingerprints
        .iter()
        .all(|candidate| !candidate.locator.contains("corrupt.db")
            && !candidate.locator.contains("directory.db")));
    let mut only_bad = serde_json::to_value(mixed).unwrap();
    only_bad["provider_roots"]["deepagents"] = serde_json::json!([corrupt, directory]);
    let only_bad: ProviderRoots = serde_json::from_value(only_bad).unwrap();
    assert!(storage::enumerate(&only_bad, None).is_err());
    assert!(storage::fingerprint_inputs(&only_bad).is_err());
}
#[test]
fn wal_only_write_and_revision_change_selected_capture_without_source_write() {
    let (_home, roots, path, db) = fixture();
    db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0")
        .unwrap();
    let id = storage::enumerate(&roots, None)
        .unwrap()
        .into_iter()
        .find(|c| {
            c.session_id
                .as_ref()
                .is_some_and(|id| id.ends_with("343433"))
        })
        .unwrap()
        .session_id
        .unwrap();
    let first = storage::capture(&AcquisitionContext::new(ID, &roots), &path, &id).unwrap();
    let (kind, blob): (String, Vec<u8>) = db.query_row("SELECT type,value FROM writes WHERE thread_id LIKE '%446' AND channel='messages' ORDER BY checkpoint_id LIMIT 1", [], |r| Ok((r.get(0)?,r.get(1)?))).unwrap();
    db.execute("INSERT INTO writes (thread_id,checkpoint_ns,checkpoint_id,task_id,task_path,idx,channel,type,value) SELECT thread_id,checkpoint_ns,checkpoint_id,'zz-new','',idx+10,channel,?1,?2 FROM writes WHERE thread_id LIKE '%443' LIMIT 1", params![kind,blob]).unwrap();
    let second = storage::capture(&AcquisitionContext::new(ID, &roots), &path, &id).unwrap();
    assert_ne!(first.stamp, second.stamp);
    assert_eq!(
        second.payload["messages"].as_array().unwrap().len(),
        first.payload["messages"].as_array().unwrap().len() + 1
    );
    assert_eq!(second.bytes, 0, "SQLite byte accounting is unknown");
}
#[test]
fn malformed_sibling_isolated_and_no_absence_reconciliation() {
    let (home, roots, path, db) = fixture();
    let ledger = crate::open_db(&home.path().join("ledger.db")).unwrap();
    let before = storage::all(&AcquisitionContext::new(ID, &roots), &path).unwrap();
    for s in &before.sessions {
        normalize::session(&ledger, s, &path).unwrap();
    }
    let native = rmpv::Value::Array(vec![
        rmpv::Value::from("langchain_core.messages.tool"),
        rmpv::Value::from("ToolMessage"),
        rmpv::Value::Map(vec![
            (
                rmpv::Value::from("id"),
                rmpv::Value::from("bad-tool-result"),
            ),
            (rmpv::Value::from("content"), rmpv::Value::from("result")),
            (
                rmpv::Value::from("tool_call_id"),
                rmpv::Value::from("x".repeat(1025)),
            ),
        ]),
        rmpv::Value::from("model_validate_json"),
    ]);
    let mut inner = Vec::new();
    rmpv::encode::write_value(&mut inner, &native).unwrap();
    let mut encoded = Vec::new();
    rmpv::encode::write_value(&mut encoded, &rmpv::Value::Ext(5, inner)).unwrap();
    db.execute("UPDATE writes SET value=?1 WHERE rowid=(SELECT rowid FROM writes WHERE thread_id LIKE '%445' AND channel='messages' LIMIT 1)", params![encoded]).unwrap();
    let after = storage::all(&AcquisitionContext::new(ID, &roots), &path).unwrap();
    assert_eq!(after.unread, 1);
    assert_eq!(after.sessions.len(), 3);
    let unchanged: i64 = ledger.query_row("SELECT count(*) FROM session_events WHERE source='deepagents' AND session_id LIKE '%343435'", [], |r| r.get(0)).unwrap();
    assert_eq!(unchanged, 2, "last good sibling evidence remains");
    assert!(after.sessions.iter().any(|s| s.native_id.ends_with("443")));
}
#[cfg(feature = "export")]
#[test]
fn oversized_native_tool_arguments_keep_identity_and_export_all_evidence() {
    use crate::export::{ExportLimits, ExportSelection, ExportSnapshot, SessionIdentity};
    use sha2::{Digest, Sha256};

    let (home, roots, path, _db) = fixture();
    let mut session = storage::all(&AcquisitionContext::new(ID, &roots), &path)
        .unwrap()
        .sessions
        .into_iter()
        .find(|s| s.native_id.ends_with("443"))
        .unwrap();
    let mut large_text = Vec::new();
    for role in ["user", "assistant", "tool"] {
        let message = session
            .messages
            .iter_mut()
            .rev()
            .find(|m| m.role == role)
            .unwrap();
        message.content = format!("deepagents-{role}-needle {}", "\"".repeat(2 * 1024 * 1024));
        large_text.push((
            message.id.clone(),
            role,
            format!("{:x}", Sha256::digest(message.content.as_bytes())),
        ));
    }
    let call = session
        .messages
        .iter_mut()
        .flat_map(|m| m.tool_calls.iter_mut())
        .next()
        .unwrap();
    let id = call["id"].as_str().unwrap().to_owned();
    let name = call["name"].as_str().unwrap().to_owned();
    call["args"] = serde_json::json!({"payload":"x".repeat(2 * 1024 * 1024)});
    let original = call["args"].to_string();
    let expected_hash = format!("{:x}", Sha256::digest(original.as_bytes()));
    let expected_event_hash = format!("{:x}", Sha256::digest(call.to_string().as_bytes()));

    let ledger_path = home.path().join("ledger.db");
    let ledger = crate::open_db(&ledger_path).unwrap();
    normalize::session(&ledger, &session, &path).unwrap();
    for (message_id, role, hash) in &large_text {
        let kind = if *role == "tool" {
            "tool_result"
        } else {
            "text"
        };
        let text: String = ledger
            .query_row(
                "SELECT text FROM session_events WHERE source=?1 AND session_id=?2 AND message_id=?3 AND kind=?4",
                params![ID, session.id, message_id, kind],
                |r| r.get(0),
            )
            .unwrap();
        assert!(text.len() < 40 * 1024, "{role} event is bounded");
        assert!(text.starts_with(&format!("deepagents-{role}-needle")));
        assert!(text.contains(hash));
        assert!(text.contains("native text truncated"));
    }
    let prompt: String = ledger
        .query_row(
            "SELECT prompt FROM history WHERE source=?1 AND session_id=?2 ORDER BY id DESC LIMIT 1",
            params![ID, session.id],
            |r| r.get(0),
        )
        .unwrap();
    assert!(prompt.len() < 40 * 1024);
    assert!(prompt.contains(&large_text[0].2));
    let summary: String = ledger
        .query_row(
            "SELECT last_assistant_text FROM sessions WHERE source=?1 AND session_id=?2",
            params![ID, session.id],
            |r| r.get(0),
        )
        .unwrap();
    assert!(summary.len() < 40 * 1024);
    assert!(summary.contains(&large_text[1].2));
    let arguments: String = ledger
        .query_row(
            "SELECT args_json FROM tool_calls WHERE source=?1 AND session_id=?2 AND tool_use_id=?3",
            params![ID, session.id, id],
            |r| r.get(0),
        )
        .unwrap();
    let projected: serde_json::Value = serde_json::from_str(&arguments).unwrap();
    assert!(arguments.len() < 1024);
    assert_eq!(projected["native_id"], id);
    assert_eq!(projected["name"], name);
    assert_eq!(projected["native_sha256"], expected_hash);
    assert_eq!(projected["truncated"], true);
    let event: String = ledger
        .query_row(
            "SELECT text FROM session_events WHERE source=?1 AND session_id=?2 AND kind='tool_use'",
            params![ID, session.id],
            |r| r.get(0),
        )
        .unwrap();
    let projected_event: serde_json::Value = serde_json::from_str(&event).unwrap();
    assert!(event.len() < 1024);
    assert_eq!(projected_event["native_id"], id);
    assert_eq!(projected_event["name"], name);
    assert_eq!(projected_event["truncated"], true);
    assert_eq!(projected_event["native_sha256"], expected_event_hash);

    let selection = ExportSelection {
        sessions: vec![SessionIdentity {
            source: ID.into(),
            session_id: session.id,
        }],
        kinds: ["history", "session_event", "tool_call", "session_marker"]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        ..Default::default()
    };
    drop(ledger);
    let mut export = ExportSnapshot::open(
        Connection::open(ledger_path).unwrap(),
        &selection,
        &ExportLimits::default(),
        60_000,
        1_000,
    )
    .unwrap();
    let mut cursor = Some(export.handle().cursor);
    let mut kinds = std::collections::BTreeSet::new();
    while let Some(current) = cursor {
        let page = export.page(&current, 1_000).unwrap();
        for record in &page.records {
            assert!(serde_json::to_vec(record).unwrap().len() < 1_048_576);
            kinds.insert(record.kind.as_str().to_owned());
        }
        cursor = page.next_cursor;
    }
    assert_eq!(
        kinds,
        ["history", "session_event", "session_marker", "tool_call"]
            .into_iter()
            .map(str::to_owned)
            .collect()
    );
}
#[test]
fn native_tool_status_rewrites_and_removal_clear_only_local_call_state() {
    let (home, roots, path, _db) = fixture();
    let mut session = storage::all(&AcquisitionContext::new(ID, &roots), &path)
        .unwrap()
        .sessions
        .into_iter()
        .find(|s| s.native_id.ends_with("443"))
        .unwrap();
    let ledger = crate::open_db(&home.path().join("ledger.db")).unwrap();
    let status = |conn: &Connection| -> Option<i64> {
        conn.query_row(
            "SELECT is_error FROM tool_calls WHERE source='deepagents' AND tool_use_id='call-1'",
            [],
            |r| r.get(0),
        )
        .unwrap()
    };
    let result = session
        .messages
        .iter_mut()
        .find(|m| m.role == "tool")
        .unwrap();
    result.status = Some("error".into());
    normalize::session(&ledger, &session, &path).unwrap();
    assert_eq!(status(&ledger), Some(1));
    session
        .messages
        .iter_mut()
        .find(|m| m.role == "tool")
        .unwrap()
        .status = Some("success".into());
    normalize::session(&ledger, &session, &path).unwrap();
    assert_eq!(status(&ledger), Some(0));
    session
        .messages
        .iter_mut()
        .find(|m| m.role == "tool")
        .unwrap()
        .status = None;
    normalize::session(&ledger, &session, &path).unwrap();
    assert_eq!(status(&ledger), None);
    session.messages.retain(|m| m.role != "tool");
    normalize::session(&ledger, &session, &path).unwrap();
    assert_eq!(status(&ledger), None);
}
#[test]
fn conclusive_native_tool_status_conflicts_with_remote_backed_outcome() {
    let (home, roots, path, _db) = fixture();
    let mut session = storage::all(&AcquisitionContext::new(ID, &roots), &path)
        .unwrap()
        .sessions
        .into_iter()
        .find(|s| s.native_id.ends_with("443"))
        .unwrap();
    let ledger = crate::open_db(&home.path().join("ledger.db")).unwrap();
    normalize::session(&ledger, &session, &path).unwrap();
    ledger.execute(
        "UPDATE tool_calls SET location='both' WHERE source='deepagents' AND session_id=?1 AND tool_use_id='call-1'",
        [&session.id],
    ).unwrap();
    // Native success matches the remote-backed value and remains idempotent.
    normalize::session(&ledger, &session, &path).unwrap();
    session
        .messages
        .iter_mut()
        .find(|m| m.role == "tool")
        .unwrap()
        .status = Some("error".into());
    assert!(normalize::session(&ledger, &session, &path).is_err());
    let (status, location): (Option<i64>, String) = ledger.query_row(
        "SELECT is_error,location FROM tool_calls WHERE source='deepagents' AND session_id=?1 AND tool_use_id='call-1'",
        [&session.id], |r| Ok((r.get(0)?, r.get(1)?)),
    ).unwrap();
    assert_eq!((status, location.as_str()), (Some(0), "both"));
}
#[cfg(feature = "export")]
#[test]
fn equal_time_repeated_prompts_keep_distinct_native_events_and_export() {
    use crate::export::{ExportLimits, ExportSelection, ExportSnapshot, SessionIdentity};
    let (home, roots, path, _db) = fixture();
    let mut session = storage::all(&AcquisitionContext::new(ID, &roots), &path)
        .unwrap()
        .sessions
        .into_iter()
        .find(|s| s.native_id.ends_with("443"))
        .unwrap();
    for message in session.messages.iter_mut().filter(|m| m.role == "user") {
        message.content = "retry native prompt".into();
    }
    let ledger_path = home.path().join("ledger.db");
    let ledger = crate::open_db(&ledger_path).unwrap();
    normalize::session(&ledger, &session, &path).unwrap();
    let mut statement = ledger.prepare("SELECT message_id FROM session_events WHERE source='deepagents' AND session_id=?1 AND role='user' AND text='retry native prompt' ORDER BY id").unwrap();
    let native_ids: Vec<String> = statement
        .query_map([&session.id], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(native_ids, ["human-1", "human-2"]);
    let history_count: i64 = ledger.query_row("SELECT count(*) FROM history WHERE source='deepagents' AND session_id=?1 AND prompt='retry native prompt'", [&session.id], |r| r.get(0)).unwrap();
    assert_eq!(history_count, 1, "shared history uniqueness collapses equal-time identical prompts; event evidence retains both");
    drop(statement);
    drop(ledger);
    let store = crate::SessionStore::open(
        crate::StoreOptions::default()
            .db_path(&ledger_path)
            .roots(roots),
    )
    .unwrap();
    let conversation = store
        .session(
            &crate::SessionRef::id(crate::Source::DeepAgents, &session.id),
            crate::SessionQuery::default(),
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        conversation
            .messages
            .iter()
            .filter(|m| m.role == crate::Role::User
                && m.blocks
                    .iter()
                    .any(|b| b.text.as_deref() == Some("retry native prompt")))
            .count(),
        2
    );
    let selection = ExportSelection {
        sessions: vec![SessionIdentity {
            source: ID.into(),
            session_id: session.id.clone(),
        }],
        kinds: vec!["session_event".into()],
        ..Default::default()
    };
    let mut export = ExportSnapshot::open(
        Connection::open(&ledger_path).unwrap(),
        &selection,
        &ExportLimits::default(),
        60_000,
        1_000,
    )
    .unwrap();
    let mut cursor = Some(export.handle().cursor);
    let mut exported = 0;
    while let Some(value) = cursor {
        let page = export.page(&value, 1_000).unwrap();
        exported += page
            .records
            .iter()
            .filter(|row| {
                row.kind == "session_event" && row.payload["text"] == "retry native prompt"
            })
            .count();
        cursor = page.next_cursor;
    }
    assert_eq!(exported, 2);
}
#[test]
fn normalization_failure_rolls_back_only_that_sibling() {
    let (home, roots, _path, _db) = fixture();
    let ledger = crate::open_db(&home.path().join("ledger.db")).unwrap();
    ledger.execute_batch("CREATE TRIGGER reject_445 BEFORE INSERT ON session_events WHEN NEW.source='deepagents' AND NEW.session_id LIKE '%343435' BEGIN SELECT RAISE(ABORT, 'synthetic normalization failure'); END;").unwrap();
    let mut state = serde_json::Map::new();
    let repairs = crate::ingest::SweepRepairs::default();
    let result = sweep(SweepContext {
        conn: &ledger,
        state: &mut state,
        roots: &roots,
        paths: None,
        repairs: &repairs,
    })
    .unwrap();
    assert_eq!(result.unread, 1);
    let healthy: i64 = ledger.query_row("SELECT count(*) FROM session_events WHERE source='deepagents' AND session_id LIKE '%343433'", [], |r| r.get(0)).unwrap();
    let rejected: i64 = ledger.query_row("SELECT count(*) FROM session_events WHERE source='deepagents' AND session_id LIKE '%343435'", [], |r| r.get(0)).unwrap();
    assert!(
        healthy > 0,
        "healthy sibling survives a failed normalization"
    );
    assert_eq!(rejected, 0, "failed sibling transaction rolls back");
}
#[cfg(unix)]
#[test]
fn unreadable_missing_locator_does_not_block_later_authoritative_retirement() {
    use std::os::unix::fs::symlink;
    let (home, roots, good, db) = fixture();
    let ledger = crate::open_db(&home.path().join("ledger.db")).unwrap();
    let mut state = serde_json::Map::new();
    let repairs = crate::ingest::SweepRepairs::default();
    sweep(SweepContext {
        conn: &ledger,
        state: &mut state,
        roots: &roots,
        paths: None,
        repairs: &repairs,
    })
    .unwrap();
    let before: i64 = ledger.query_row("SELECT count(*) FROM session_events WHERE source='deepagents' AND session_id LIKE '%343433'", [], |r| r.get(0)).unwrap();
    assert!(before > 0);
    drop(db);
    std::fs::remove_file(&good).unwrap();
    let bad = good.with_file_name("aa-loop.db");
    symlink(&bad, &bad).unwrap();
    ledger.execute("INSERT INTO session_presences(source,session_id,location,raw_locator) VALUES ('deepagents','bad-loop','local',?1)", [bad.to_string_lossy().as_ref()]).unwrap();
    let mut config = serde_json::to_value(roots).unwrap();
    config["provider_roots"]["deepagents"] = serde_json::json!([bad, good]);
    let roots: ProviderRoots = serde_json::from_value(config).unwrap();
    let result = sweep(SweepContext {
        conn: &ledger,
        state: &mut state,
        roots: &roots,
        paths: None,
        repairs: &repairs,
    })
    .unwrap();
    assert!(result.unread >= 1);
    let after: i64 = ledger.query_row("SELECT count(*) FROM session_events WHERE source='deepagents' AND session_id LIKE '%343433' AND location IN ('local','both')", [], |r| r.get(0)).unwrap();
    assert_eq!(
        after, 0,
        "healthy missing locator retired despite earlier ELOOP sibling"
    );
    let bad_presence: i64 = ledger.query_row("SELECT count(*) FROM session_presences WHERE source='deepagents' AND session_id='bad-loop' AND location='local'", [], |r| r.get(0)).unwrap();
    assert_eq!(bad_presence, 1, "uncertain sibling remains last-good");
}
#[test]
fn unsupported_version_extension_and_trailing_bytes_are_rejected() {
    let (_home, roots, path, db) = fixture();
    db.execute(
        "UPDATE checkpoints SET type='pickle' WHERE thread_id LIKE '%443'",
        [],
    )
    .unwrap();
    let id = storage::identity(&roots, &path, "019a0000-0000-7000-8000-000000000443");
    assert!(storage::capture(&AcquisitionContext::new(ID, &roots), &path, &id).is_err());
    let (_home2, roots2, path2, db2) = fixture();
    db2.execute(
        "UPDATE checkpoints SET checkpoint=checkpoint||X'00' WHERE thread_id LIKE '%443'",
        [],
    )
    .unwrap();
    assert!(storage::capture(&AcquisitionContext::new(ID, &roots2), &path2, &id).is_err());
}
#[test]
fn pruning_an_early_checkpoint_keeps_native_ids_order_and_event_time() {
    let (_home, roots, path, db) = fixture();
    let id = storage::identity(&roots, &path, "019a0000-0000-7000-8000-000000000443");
    let before = storage::capture(&AcquisitionContext::new(ID, &roots), &path, &id).unwrap();
    db.execute(
        "DELETE FROM checkpoints WHERE thread_id LIKE '%443' AND checkpoint_id LIKE '%0001'",
        [],
    )
    .unwrap();
    let after = storage::capture(&AcquisitionContext::new(ID, &roots), &path, &id).unwrap();
    assert_eq!(before.payload["messages"], after.payload["messages"]);
    assert_eq!(
        before.payload["started"], after.payload["started"],
        "UUIDv7 creation time is independent of checkpoint pruning"
    );
}
#[test]
fn native_append_preserves_remote_backed_existing_evidence() {
    let (home, roots, path, db) = fixture();
    let ledger = crate::open_db(&home.path().join("ledger.db")).unwrap();
    let id = storage::identity(&roots, &path, "019a0000-0000-7000-8000-000000000443");
    let first = storage::all(&AcquisitionContext::new(ID, &roots), &path)
        .unwrap()
        .sessions
        .into_iter()
        .find(|s| s.id == id)
        .unwrap();
    normalize::session(&ledger, &first, &path).unwrap();
    let event_uid: String = ledger.query_row("SELECT event_uid FROM session_events WHERE source='deepagents' AND session_id=?1 ORDER BY id LIMIT 1", [&id], |r| r.get(0)).unwrap();
    let marker_uid: String = ledger.query_row("SELECT marker_uid FROM session_markers WHERE source='deepagents' AND session_id=?1 AND kind='session_metadata' LIMIT 1", [&id], |r| r.get(0)).unwrap();
    ledger.execute("UPDATE session_events SET location='both' WHERE source='deepagents' AND session_id=?1 AND event_uid=?2", params![id,event_uid]).unwrap();
    ledger.execute("UPDATE session_markers SET location='both' WHERE source='deepagents' AND session_id=?1 AND marker_uid=?2", params![id,marker_uid]).unwrap();
    let (kind, blob): (String, Vec<u8>) = db.query_row("SELECT type,value FROM writes WHERE thread_id LIKE '%446' AND channel='messages' ORDER BY checkpoint_id LIMIT 1", [], |r| Ok((r.get(0)?,r.get(1)?))).unwrap();
    db.execute("INSERT INTO writes (thread_id,checkpoint_ns,checkpoint_id,task_id,task_path,idx,channel,type,value) SELECT thread_id,checkpoint_ns,checkpoint_id,'zz-append','',idx+10,channel,?1,?2 FROM writes WHERE thread_id LIKE '%443' LIMIT 1", params![kind,blob]).unwrap();
    let next = storage::all(&AcquisitionContext::new(ID, &roots), &path)
        .unwrap()
        .sessions
        .into_iter()
        .find(|s| s.id == id)
        .unwrap();
    assert_eq!(next.messages.len(), first.messages.len() + 1);
    normalize::session(&ledger, &next, &path).unwrap();
    let old: String = ledger.query_row("SELECT location FROM session_events WHERE source='deepagents' AND session_id=?1 AND event_uid=?2", params![id,event_uid], |r| r.get(0)).unwrap();
    assert_eq!(old, "both");
    let old_marker: String = ledger.query_row("SELECT location FROM session_markers WHERE source='deepagents' AND session_id=?1 AND marker_uid=?2", params![id,marker_uid], |r| r.get(0)).unwrap();
    assert_eq!(
        old_marker, "both",
        "same native checkpoint marker retains its local and remote share after pending write"
    );
    let count: i64 = ledger
        .query_row(
            "SELECT count(*) FROM session_events WHERE source='deepagents' AND session_id=?1",
            [&id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 8);
}
