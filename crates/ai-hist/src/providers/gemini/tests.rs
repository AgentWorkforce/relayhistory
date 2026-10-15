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
        count, 3,
        "text + tool use + result remote shares survive local rewrite"
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
    // The v2 retirement fix must replay unchanged bytes cached by v1.
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
    assert_eq!(remote, 3);
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
    assert_eq!(remote, 3);
    assert!(added > 0);
}
