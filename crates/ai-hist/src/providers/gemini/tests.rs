use super::*;
#[test]
fn roots_match_upstream_precedence() {
    let home = Path::new("/isolated");
    assert_eq!(
        roots_from_values(home, None, None).paths,
        vec![home.join(".gemini/tmp")]
    );
    assert_eq!(
        roots_from_values(home, Some("/override"), Some("sandbox-exec")).paths,
        vec![PathBuf::from("/override/.cache/.gemini/tmp")]
    );
    assert_eq!(
        roots_from_values(home, None, Some("docker")).paths,
        vec![home.join(".gemini/tmp")]
    );
    assert!(roots_from_values(home, Some("/override"), None)
        .default_root
        .is_none());
}
#[test]
fn official_reader_projection_matches_all_captured_cases() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/gemini");
    // Byte-identical copies of the captured research manifest/reader oracle;
    // kept inside the crate so published-package tests remain self-contained.
    let manifest: Value = serde_json::from_str(include_str!("cases.json")).unwrap();
    let oracle: Value = serde_json::from_str(include_str!("oracle.json")).unwrap();
    for case in manifest["cases"].as_array().unwrap() {
        let path = root.join(
            case["path"]
                .as_str()
                .unwrap()
                .strip_prefix("cases/")
                .unwrap(),
        );
        let result = format::parse(&std::fs::read(&path).unwrap(), path_is_legacy(&path)).unwrap();
        assert_eq!(
            result["conversation"],
            {
                let mut expected = oracle[case["name"].as_str().unwrap()].clone();
                for field in [
                    "firstUserMessage",
                    "hasResumableContent",
                    "messageCount",
                    "userMessageCount",
                ] {
                    expected.as_object_mut().unwrap().remove(field);
                }
                expected
            },
            "{}",
            case["name"]
        );
    }
}
fn fixture() -> (tempfile::TempDir, ProviderRoots, PathBuf) {
    let home = tempfile::tempdir().unwrap();
    let roots = ProviderRoots::from_home(home.path().into(), home.path().join("opencode.db"));
    let path = home.path().join(".gemini/tmp/slug/chats/session.jsonl");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/gemini/ordinary/.gemini/tmp/gemini-project/chats/session-2026-10-11T12-00-11111111.jsonl");
    std::fs::copy(source, &path).unwrap();
    (home, roots, path)
}
#[test]
fn namespaces_follow_frozen_root_provenance() {
    let (home, mut roots, path) = fixture();
    assert_eq!(
        storage::identity(&roots, &path, "native").unwrap(),
        "native"
    );
    roots.provider_default_roots.remove(ID);
    let one = storage::identity(&roots, &path, "native").unwrap();
    assert!(one.starts_with("root:"));
    assert!(one.ends_with(":native"));
    let other = home.path().join("other");
    roots.provider_roots.insert(ID.into(), vec![other.clone()]);
    let two = storage::identity(&roots, &other.join("x"), "native").unwrap();
    assert_ne!(one, two);
    let serialized = serde_json::to_vec(&roots).unwrap();
    let restored: ProviderRoots = serde_json::from_slice(&serialized).unwrap();
    assert_eq!(
        two,
        storage::identity(&restored, &other.join("x"), "native").unwrap()
    );
}
#[test]
fn rewrite_retires_local_evidence_but_keeps_remote_shares() {
    let (home, roots, path) = fixture();
    let context = AcquisitionContext::new(ID, &roots);
    let conn = crate::open_db(&home.path().join("test.db")).unwrap();
    let payload = snapshot(&path, &context).unwrap().payload;
    normalize::write(&conn, &payload, &path).unwrap();
    conn.execute("UPDATE session_events SET location='both' WHERE source='gemini' AND message_id='assistant-one'",[]).unwrap();
    let mut rewritten = payload;
    rewritten["conversation"]["messages"]
        .as_array_mut()
        .unwrap()
        .retain(|m| m["id"] != "assistant-one");
    normalize::write(&conn, &rewritten, &path).unwrap();
    let count:i64=conn.query_row("SELECT count(*) FROM session_events WHERE source='gemini' AND message_id='assistant-one' AND location='remote'",[],|r|r.get(0)).unwrap();
    assert_eq!(
        count, 2,
        "assistant text + tool use remote shares survive local rewrite"
    );
    assert_eq!(
        conn.query_row(
            "SELECT count(*) FROM tool_calls WHERE source='gemini'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
}
#[test]
fn cancellation_during_replay_and_normalization_rolls_back() {
    let (home, roots, path) = fixture();
    let context = AcquisitionContext::new(ID, &roots);
    let bytes = context.read(&path).unwrap();
    let calls = std::rc::Rc::new(std::cell::Cell::new(0));
    let count = calls.clone();
    let result = crate::ingest::with_capture_stop(
        move || {
            let n = count.get() + 1;
            count.set(n);
            n >= 3
        },
        || format::parse(&bytes, false),
    );
    assert!(result.unwrap_err().is::<crate::ingest::CaptureCancelled>());
    let payload = snapshot(&path, &context).unwrap().payload;
    let conn = crate::open_db(&home.path().join("test.db")).unwrap();
    let calls = std::rc::Rc::new(std::cell::Cell::new(0));
    let count = calls.clone();
    let result = crate::ingest::with_capture_stop(
        move || {
            let n = count.get() + 1;
            count.set(n);
            n >= 2
        },
        || {
            let tx = conn.unchecked_transaction()?;
            normalize::write(&tx, &payload, &path)
        },
    );
    assert!(result.unwrap_err().is::<crate::ingest::CaptureCancelled>());
    assert_eq!(
        conn.query_row("SELECT count(*) FROM session_markers", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
}
#[test]
fn scoped_sweep_preserves_unvisited_sessions_and_repairs_lost_evidence() {
    let (home, roots, path) = fixture();
    let conn = crate::open_db(&home.path().join("test.db")).unwrap();
    let mut state = Map::new();
    let repairs = crate::ingest::SweepRepairs::default();
    let run = |state: &mut Map<String, Value>,
               paths: Option<&crate::ingest::path_scope::PathScope>| {
        sweep(SweepContext {
            conn: &conn,
            state,
            roots: &roots,
            paths,
            repairs: &repairs,
        })
    };
    run(&mut state, None).unwrap();
    let original: i64 = conn
        .query_row(
            "SELECT count(*) FROM session_events WHERE source='gemini'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let scope =
        crate::ingest::path_scope::PathScope::from_paths([home.path().join("unrelated")]).unwrap();
    run(&mut state, Some(&scope)).unwrap();
    assert_eq!(
        conn.query_row(
            "SELECT count(*) FROM session_events WHERE source='gemini'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        original
    );
    // New parser generations must replay unchanged bytes cached by v1.
    state["gemini_file_stamps"][path.to_string_lossy().as_ref()]["generation"] =
        "gemini_journal_v1".into();
    conn.execute("DELETE FROM session_events WHERE source='gemini'", [])
        .unwrap();
    run(&mut state, None).unwrap();
    assert_eq!(
        conn.query_row(
            "SELECT count(*) FROM session_events WHERE source='gemini'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        original
    );
}
#[test]
fn legacy_shallow_prefix_is_bounded_and_unsupported_order_is_diagnostic() {
    let mut value = serde_json::json!({"sessionId":"native","projectHash":"hash","startTime":"2026-10-11T12:00:00Z","lastUpdated":"2026-10-11T12:00:01Z","messages":[]});
    let bytes = serde_json::to_vec(&value).unwrap();
    assert!(
        format::header(&bytes, true).is_err(),
        "sorted JSON puts messages before sessionId; bounded discovery must not invent an identity"
    );
    value.as_object_mut().unwrap().remove("messages");
    let text = serde_json::to_string(&value).unwrap();
    let huge = format!(
        "{},\"messages\":[{}",
        &text[..text.len() - 1],
        " ".repeat(100_000)
    );
    let header = format::header(&huge.as_bytes()[..HEAD_LIMIT as usize], true).unwrap();
    assert_eq!(header["sessionId"], "native");
}
#[test]
fn partial_tail_and_malformed_unknown_mutations_are_distinct() {
    let (_home, roots, path) = fixture();
    let context = AcquisitionContext::new(ID, &roots);
    let bytes = context.read(&path).unwrap();
    let mut partial = bytes.clone();
    partial.extend_from_slice(b"{\"$patch\":");
    assert_eq!(format::parse(&partial, false).unwrap()["partial"], true);
    for tail in [
        "{broken}",
        "{\"$patch\":{\"futureField\":true}}\n",
        "{\"$set\":{},\"$unknown\":true}\n",
    ] {
        let mut bad = bytes.clone();
        bad.extend_from_slice(tail.as_bytes());
        assert!(format::parse(&bad, false).is_err(), "{tail}");
    }
}
#[test]
#[cfg(unix)]
fn denied_root_is_error_without_mutating_permissions() {
    use std::os::unix::fs::PermissionsExt;
    let (home, roots, path) = fixture();
    let root = path.parent().unwrap();
    std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o000)).unwrap();
    // Privileged processes can bypass mode bits, invalidating this premise.
    if std::fs::read_dir(root).is_ok() {
        std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700)).unwrap();
        return;
    }
    let conn = crate::open_db(&home.path().join("test.db")).unwrap();
    let result = storage::enumerate(&DiscoveryEnv::with_provider_roots(&conn, roots));
    let mode = std::fs::metadata(root).unwrap().permissions().mode() & 0o777;
    std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(mode, 0);
    assert!(result.is_err());
}
#[test]
fn deleted_legacy_locator_retires_only_local_share_when_root_is_accessible() {
    let (home, roots, path) = fixture();
    std::fs::remove_file(&path).unwrap();
    let path = path.with_extension("json");
    let input=Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/gemini/legacy/.gemini/tmp/gemini-project/chats/session-legacy-22222222.json");
    std::fs::copy(input, &path).unwrap();
    let conn = crate::open_db(&home.path().join("test.db")).unwrap();
    let payload = snapshot(&path, &AcquisitionContext::new(ID, &roots))
        .unwrap()
        .payload;
    normalize::write(&conn, &payload, &path).unwrap();
    conn.execute("UPDATE session_events SET location='both' WHERE source='gemini' AND message_id='assistant-one'",[]).unwrap();
    std::fs::remove_file(&path).unwrap();
    storage::retire_missing_legacy(&conn, &roots).unwrap();
    let local:i64=conn.query_row("SELECT count(*) FROM session_events WHERE source='gemini' AND location IN ('local','both')",[],|r|r.get(0)).unwrap();
    let remote: i64 = conn
        .query_row(
            "SELECT count(*) FROM session_events WHERE source='gemini' AND location='remote'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(local, 0);
    assert_eq!(remote, 2);
}
#[test]
fn native_inclusive_input_becomes_cache_exclusive_without_inventing_missing_input() {
    let measured = usage::normalize(&serde_json::json!({"input":100,"cached":20}))
        .unwrap()
        .unwrap();
    assert_eq!(
        (measured.input_tokens, measured.cache_read_tokens),
        (80, 20)
    );
    let cached_only = usage::normalize(&serde_json::json!({"cached":20}))
        .unwrap()
        .unwrap();
    assert_eq!(cached_only.input_tokens, 0);
    assert!(!cached_only.coverage.has_input_tokens);
    assert_eq!(cached_only.cache_read_tokens, 20);
    assert!(cached_only.coverage.has_cache_read_tokens);
    assert!(matches!(
        usage::normalize(&serde_json::json!({"input":10,"cached":20})),
        Err(UsageError::CounterRegressed { .. })
    ));
}
#[test]
fn migration_sibling_must_match_project_and_native_session() {
    let (_home, roots, path) = fixture();
    let legacy_path = path.with_extension("json");
    let mut input: Value = serde_json::from_slice(
        std::fs::read(&path)
            .unwrap()
            .split(|b| *b == b'\n')
            .next()
            .unwrap(),
    )
    .unwrap();
    input["messages"] = serde_json::json!([]);
    input["projectHash"] = "other-project".into();
    std::fs::write(&legacy_path, serde_json::to_vec(&input).unwrap()).unwrap();
    let context = AcquisitionContext::new(ID, &roots);
    assert!(snapshot(&legacy_path, &context)
        .unwrap_err()
        .to_string()
        .contains("GEMINI_ID_COLLISION"));
    assert!(storage::superseded(
        &context,
        &legacy_path,
        input["sessionId"].as_str().unwrap(),
        "other-project"
    )
    .is_err());
}
#[test]
fn native_identity_rewrite_retires_local_share_and_preserves_remote_share() {
    let (home, roots, path) = fixture();
    let context = AcquisitionContext::new(ID, &roots);
    let conn = crate::open_db(&home.path().join("test.db")).unwrap();
    let mut payload = snapshot(&path, &context).unwrap().payload;
    normalize::write(&conn, &payload, &path).unwrap();
    conn.execute("UPDATE session_events SET location='both' WHERE source='gemini' AND message_id='assistant-one'",[]).unwrap();
    let original_prompt_id: i64 = conn
        .query_row("SELECT id FROM history WHERE source='gemini'", [], |r| {
            r.get(0)
        })
        .unwrap();
    let old = payload["ledgerId"].as_str().unwrap().to_owned();
    let new = "55555555-5555-4555-8555-555555555555";
    payload["ledgerId"] = new.into();
    payload["conversation"]["sessionId"] = new.into();
    payload["records"][0]["sessionId"] = new.into();
    let tx = conn.unchecked_transaction().unwrap();
    normalize::write(&tx, &payload, &path).unwrap();
    tx.commit().unwrap();
    let local:i64=conn.query_row("SELECT count(*) FROM session_events WHERE source='gemini' AND session_id=?1 AND location IN ('local','both')",[&old],|r|r.get(0)).unwrap();
    let remote:i64=conn.query_row("SELECT count(*) FROM session_events WHERE source='gemini' AND session_id=?1 AND location='remote'",[&old],|r|r.get(0)).unwrap();
    let added:i64=conn.query_row("SELECT count(*) FROM session_events WHERE source='gemini' AND session_id=?1 AND location='local'",[new],|r|r.get(0)).unwrap();
    let retained_prompt_id: i64 = conn
        .query_row(
            "SELECT id FROM history WHERE source='gemini' AND session_id=?1",
            [new],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        retained_prompt_id, original_prompt_id,
        "identity rewrite transfers canonical prompt ownership without delete/reinsert"
    );
    assert_eq!(local, 0);
    assert_eq!(remote, 2);
    assert!(added > 0);
}
#[test]
fn nested_patch_extensions_fail_even_when_target_is_unknown() {
    let (_home, roots, path) = fixture();
    let bytes = AcquisitionContext::new(ID, &roots).read(&path).unwrap();
    for message_id in ["assistant-one", "unknown-message"] {
        for tool_id in ["call-real-id", "unknown-call"] {
            let patches = [
                serde_json::json!({"updates":[{"id":message_id,"futureField":true}]}),
                serde_json::json!({"updates":[{"id":message_id,"toolCalls":[{"id":tool_id,"futureField":true}]}]}),
                serde_json::json!({"id":message_id,"toolCalls":[{"id":tool_id,"futureField":true}]}),
            ];
            for patch in patches {
                let mut input = bytes.clone();
                input.extend(serde_json::to_vec(&serde_json::json!({"$patch":patch})).unwrap());
                input.push(b'\n');
                assert!(
                    format::parse(&input, false)
                        .unwrap_err()
                        .to_string()
                        .contains("GEMINI_UNSUPPORTED_SCHEMA"),
                    "{patch}"
                );
            }
        }
    }
    let mut known = bytes;
    known.extend_from_slice(b"{\"$patch\":{\"id\":\"assistant-two\",\"content\":\"direct patch\",\"removeIds\":[\"info-one\"],\"orderIds\":[\"assistant-two\"]}}\n");
    let projection = format::parse(&known, false).unwrap();
    assert_eq!(
        projection["conversation"]["messages"]
            .as_array()
            .unwrap()
            .last()
            .unwrap()["content"],
        "direct patch"
    );
}

#[test]
fn shallow_recency_distinguishes_real_eof_from_truncated_window() {
    let header = serde_json::json!({"lastUpdated":"2026-10-11T12:00:00Z", "sessionId":"s"});
    let bytes = b"{\"$set\":{\"lastUpdated\":\"2026-10-12T12:30:00Z\"}}";
    assert_eq!(
        format::recent_time(bytes, false, true, &header).unwrap().0,
        1791808200000
    );
    assert_eq!(
        format::recent_time(bytes, false, false, &header).unwrap().0,
        format::date(&header, "lastUpdated").unwrap()
    );
    assert_eq!(
        format::recent_time(b"{\"$set\":", false, true, &header)
            .unwrap()
            .0,
        format::date(&header, "lastUpdated").unwrap()
    );
    assert!(format::recent_time(b"garbage", false, true, &header).is_err());
}

#[test]
fn source_stamp_hashes_exact_bytes_without_expanding_them() {
    let (_home, roots, path) = fixture();
    let context = AcquisitionContext::new(ID, &roots);
    let first = snapshot(&path, &context).unwrap();
    assert!(first.payload.get("bytes").is_none());
    assert_eq!(first.payload["bytesSha256"].as_str().unwrap().len(), 64);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes.push(b' ');
    std::fs::write(&path, bytes).unwrap();
    let second = snapshot(&path, &context).unwrap();
    assert_ne!(first.stamp, second.stamp);
    assert_ne!(first.payload["bytesSha256"], second.payload["bytesSha256"]);
}

#[test]
fn inline_result_without_response_record_has_absent_message_identity() {
    let (home, roots, path) = fixture();
    let mut payload = snapshot(&path, &AcquisitionContext::new(ID, &roots))
        .unwrap()
        .payload;
    payload["conversation"]["messages"]
        .as_array_mut()
        .unwrap()
        .retain(|m| {
            !m["content"]
                .as_array()
                .is_some_and(|parts| parts.iter().any(|p| p.get("functionResponse").is_some()))
        });
    let conn = crate::open_db(&home.path().join("test.db")).unwrap();
    normalize::write(&conn, &payload, &path).unwrap();
    let identity: Option<String> = conn
        .query_row(
            "SELECT message_id FROM session_events WHERE source='gemini' AND kind='tool_result'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(identity, None);
}

#[test]
fn scoped_migration_then_journal_deletion_reclaims_legacy_cache() {
    let (home, roots, journal) = fixture();
    std::fs::remove_file(&journal).unwrap();
    let legacy = journal.with_extension("json");
    std::fs::write(&legacy, include_str!("../../../tests/fixtures/gemini/legacy/.gemini/tmp/gemini-project/chats/session-legacy-22222222.json")).unwrap();
    let db = home.path().join("test.db");
    let store = crate::SessionStore::open(
        crate::StoreOptions::default()
            .db_path(&db)
            .roots(roots.clone()),
    )
    .unwrap();
    let conn = crate::open_db(&db).unwrap();
    let mut state = Map::new();
    let repairs = crate::ingest::SweepRepairs::default();
    let run = |state: &mut Map<String, Value>,
               paths: Option<&crate::ingest::path_scope::PathScope>| {
        sweep(SweepContext {
            conn: &conn,
            state,
            roots: &roots,
            paths,
            repairs: &repairs,
        })
        .unwrap()
    };
    run(&mut state, None);
    let legacy_cache = state["gemini_file_stamps"][legacy.to_string_lossy().as_ref()].clone();
    std::fs::write(&journal, include_str!("../../../tests/fixtures/gemini/legacy-migrated/.gemini/tmp/gemini-project/chats/session-legacy-22222222.jsonl")).unwrap();
    let scope = crate::ingest::path_scope::PathScope::from_paths([journal.clone()]).unwrap();
    run(&mut state, Some(&scope));
    assert_eq!(
        state["gemini_file_stamps"][legacy.to_string_lossy().as_ref()],
        legacy_cache
    );
    let owner = || {
        conn.query_row("SELECT raw_path FROM sessions WHERE source='gemini' AND session_id='22222222-2222-4222-8222-222222222222'", [], |r| r.get::<_, String>(0)).unwrap()
    };
    assert_eq!(owner(), journal.to_string_lossy());
    std::fs::remove_file(&journal).unwrap();
    let mut previous = None;
    for _ in 0..2 {
        assert_eq!(run(&mut state, None).unread, 0);
        assert_eq!(
            owner(),
            legacy.to_string_lossy(),
            "surviving legacy must reclaim catalog ownership"
        );
        let sid = "22222222-2222-4222-8222-222222222222";
        let evidence = store
            .session(
                &crate::SessionRef::id(crate::Source::Gemini, sid),
                crate::SessionQuery::default(),
            )
            .unwrap()
            .expect("surviving legacy remains publicly readable");
        assert_eq!(evidence.session.raw_path.as_ref(), Some(&legacy));
        assert!(!evidence.messages.is_empty());
        assert!(crate::session_locations(&conn, ID, sid)
            .unwrap()
            .contains(&"local".into()));
        let hits = crate::search(
            &conn,
            &["needle".into()],
            false,
            &crate::QueryFilter {
                source: Some(ID.into()),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(
            hits.iter().any(|h| h.session_id.as_deref() == Some(sid)),
            "surviving legacy remains searchable"
        );
        let revision: i64 = conn
            .query_row(
                "SELECT max(revision) FROM session_events WHERE source='gemini' AND session_id=?1",
                [sid],
                |r| r.get(0),
            )
            .unwrap();
        if let Some((messages, previous_revision)) = previous.as_ref() {
            assert_eq!(&evidence.messages, messages);
            assert_eq!(
                revision, *previous_revision,
                "unchanged fallback cannot churn evidence"
            );
        }
        previous = Some((evidence.messages, revision));
    }
}
