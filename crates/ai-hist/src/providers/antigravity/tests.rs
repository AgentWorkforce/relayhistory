use super::*;

#[test]
fn roots_are_frozen_and_variant_namespaces_are_distinct() {
    let home = Path::new("/fixture-home");
    let resolved = roots::resolve(home, false);
    assert_eq!(resolved.paths.len(), 3);
    let mut roots = ProviderRoots::from_home(home.into(), home.join("opencode.db"));
    roots
        .provider_roots
        .insert(ID.into(), resolved.paths.clone());
    let ids = resolved
        .paths
        .iter()
        .map(|root| {
            roots::identity(
                &roots,
                &root.join("native-id/.system_generated/logs/transcript_full.jsonl"),
            )
            .unwrap()
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(ids.len(), 3);
    let restored: ProviderRoots =
        serde_json::from_value(serde_json::to_value(&roots).unwrap()).unwrap();
    assert_eq!(restored.for_provider(ID), roots.for_provider(ID));
}
#[test]
fn incomplete_and_malformed_records_are_distinct() {
    assert!(format::parse(b"{\"step_index\":", false).unwrap().1);
    assert!(format::parse(b"{broken}", false).is_err());
    assert!(format::parse(b"{\"step_index\":\n", false).is_err());
}

#[test]
fn native_transcript_projection_matches_independent_sdk_reader_all_cases() {
    let oracle: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/antigravity/official-reader.json"
    ))
    .unwrap();
    for case in ["ordinary", "tool", "resumed", "literal-tags", "long"] {
        let home = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/antigravity")
            .join(case);
        let roots = ProviderRoots::from_home(home.clone(), home.join("opencode.db"));
        let brain = home.join(".gemini/antigravity/brain");
        let path = std::fs::read_dir(brain)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path()
            .join(".system_generated/logs/transcript_full.jsonl");
        let captured = storage::capture(&path, &AcquisitionContext::new(ID, &roots)).unwrap();
        let temp = tempfile::tempdir().unwrap();
        let store = crate::SessionStore::open(
            crate::StoreOptions::default()
                .db_path(temp.path().join("db"))
                .roots(roots),
        )
        .unwrap();
        let conn = crate::open_db(store.db_path()).unwrap();
        normalize::write(&conn, &captured.payload).unwrap();
        let evidence = store
            .session(
                &crate::SessionRef::id(
                    crate::Source::Antigravity,
                    captured.payload["sid"].as_str().unwrap(),
                ),
                crate::SessionQuery::default(),
            )
            .unwrap()
            .unwrap();
        let actual = evidence
            .messages
            .iter()
            .flat_map(|m| {
                m.blocks.iter().filter_map(move |b| {
                    b.text.as_ref().map(|t| {
                        (
                            if m.role == crate::Role::User {
                                "user".to_owned()
                            } else {
                                "assistant".to_owned()
                            },
                            t.clone(),
                        )
                    })
                })
            })
            .collect::<Vec<_>>();
        let expected = oracle[case]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|r| {
                if r["type"] != "TEXT_RESPONSE" {
                    return None;
                }
                let text = r["content"].as_str().filter(|s| !s.is_empty())?;
                let role = match r["source"].as_str()? {
                    "USER" => "user",
                    "MODEL" => "assistant",
                    _ => return None,
                };
                Some((role.to_owned(), text.to_owned()))
            })
            .collect::<Vec<_>>();
        assert_eq!(
            actual, expected,
            "{case}: native SDK restart-reader text projection"
        );
        assert!(evidence.tool_calls.is_empty());
        assert!(evidence
            .markers
            .iter()
            .any(|m| m.raw_payload().is_some_and(|p| p.contains("USER_REQUEST"))));
    }
}

#[test]
fn exact_source_stamp_changes_for_whitespace_and_nonterminal_steps_defer_retirement() {
    let home = tempfile::tempdir().unwrap();
    let roots = ProviderRoots::from_home(home.path().into(), home.path().join("opencode.db"));
    let path = home
        .path()
        .join(".gemini/antigravity/brain/native/.system_generated/logs/transcript_full.jsonl");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let original=include_bytes!("../../../tests/fixtures/antigravity/ordinary/.gemini/antigravity/brain/00000000000000000000000000000001/.system_generated/logs/transcript_full.jsonl");
    std::fs::write(&path, original).unwrap();
    let context = AcquisitionContext::new(ID, &roots);
    let first = storage::capture(&path, &context).unwrap();
    let mut changed = original.to_vec();
    changed.push(b' ');
    std::fs::write(&path, changed).unwrap();
    let second = storage::capture(&path, &context).unwrap();
    assert_ne!(first.stamp, second.stamp);
    assert_eq!(first.payload["rows"], second.payload["rows"]);
    std::fs::write(
        &path,
        String::from_utf8_lossy(original).replace("\"status\":\"DONE\"", "\"status\":\"ACTIVE\""),
    )
    .unwrap();
    assert_eq!(
        storage::capture(&path, &context).unwrap().payload["partial"],
        true
    );
}

#[test]
fn source_stamp_is_home_independent_but_tracks_projection_and_identity() {
    let original=include_bytes!("../../../tests/fixtures/antigravity/ordinary/.gemini/antigravity/brain/00000000000000000000000000000001/.system_generated/logs/transcript_full.jsonl");
    let capture = |home: &Path, variant: &str, name: &str| {
        let roots = ProviderRoots::from_home(home.into(), home.join("opencode.db"));
        let path = home
            .join(".gemini")
            .join(variant)
            .join("brain/native/.system_generated/logs")
            .join(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, original).unwrap();
        storage::capture(&path, &AcquisitionContext::new(ID, &roots)).unwrap()
    };
    let first_home = tempfile::tempdir().unwrap();
    let second_home = tempfile::tempdir().unwrap();
    let first = capture(first_home.path(), "antigravity", "transcript_full.jsonl");
    let relocated = capture(second_home.path(), "antigravity", "transcript_full.jsonl");
    assert_ne!(first.payload["path"], relocated.payload["path"]);
    assert_eq!(first.payload["sid"], relocated.payload["sid"]);
    assert_eq!(
        first.stamp, relocated.stamp,
        "temporary HOME relocation is not an evidence revision"
    );
    std::fs::remove_file(first.payload["path"].as_str().unwrap()).unwrap();
    let compact = capture(first_home.path(), "antigravity", "transcript.jsonl");
    assert_eq!(first.payload["bytesSha256"], compact.payload["bytesSha256"]);
    assert_ne!(
        first.stamp, compact.stamp,
        "projection handoff must trigger hydration even for equal bytes"
    );
    let other = capture(
        second_home.path(),
        "antigravity-cli",
        "transcript_full.jsonl",
    );
    assert_ne!(first.stamp, other.stamp, "root namespaces remain separate");
}

#[test]
fn user_request_unwraps_with_trailing_settings_change_block() {
    let row = serde_json::json!({
        "source": "USER_EXPLICIT", "type": "USER_INPUT", "status": "DONE",
        "content": "<USER_REQUEST>\nPlease review the repo.\n</USER_REQUEST>\n<ADDITIONAL_METADATA>\nThe current local time is: 2026-11-19T21:00:34-08:00.\n</ADDITIONAL_METADATA>\n<USER_SETTINGS_CHANGE>\nThe user changed setting `Model Selection`.\n</USER_SETTINGS_CHANGE>"
    });
    assert_eq!(format::text(&row), Some("Please review the repo."));
    let unknown = serde_json::json!({
        "source": "USER_EXPLICIT", "type": "USER_INPUT", "status": "DONE",
        "content": "<USER_REQUEST>\nhi\n</USER_REQUEST>\n<ADDITIONAL_METADATA>\nThe current local time is: 2026-11-19T21:00:34-08:00.\n</ADDITIONAL_METADATA>\ntrailing prose"
    });
    assert!(format::text(&unknown)
        .unwrap()
        .starts_with("<USER_REQUEST>"));
}

#[test]
fn user_request_unwraps_with_multiline_additional_metadata() {
    let mentions = serde_json::json!({
        "source": "USER_EXPLICIT", "type": "USER_INPUT", "status": "DONE",
        "content": "<USER_REQUEST>\nFix PR merge conflicts @[relayhistory#580]\n</USER_REQUEST>\n<ADDITIONAL_METADATA>\nThe current local time is: 2026-11-20T06:37:44-08:00.\n\nThe user has mentioned some items in the form @[ITEM].\n@[relayhistory#580] is a [Link]:\nURL: https://github.com/AgentWorkforce/relayhistory/pull/580\n</ADDITIONAL_METADATA>\n<USER_SETTINGS_CHANGE>\nThe user changed setting `Model Selection`.\n</USER_SETTINGS_CHANGE>"
    });
    assert_eq!(
        format::text(&mentions),
        Some("Fix PR merge conflicts @[relayhistory#580]")
    );
    let uploads = serde_json::json!({
        "source": "USER_EXPLICIT", "type": "USER_INPUT", "status": "DONE",
        "content": "<USER_REQUEST>\nCheck these screenshots\n</USER_REQUEST>\n<ADDITIONAL_METADATA>\nThe current local time is: 2026-11-26T09:42:49-08:00.\n\nThe user has uploaded 2 image(s):\n- /tmp/a.png\n</ADDITIONAL_METADATA>"
    });
    assert_eq!(format::text(&uploads), Some("Check these screenshots"));
    let bad_timestamp = serde_json::json!({
        "source": "USER_EXPLICIT", "type": "USER_INPUT", "status": "DONE",
        "content": "<USER_REQUEST>\nhi\n</USER_REQUEST>\n<ADDITIONAL_METADATA>\nNot a timestamp.\nMore lines.\n</ADDITIONAL_METADATA>"
    });
    assert!(format::text(&bad_timestamp)
        .unwrap()
        .starts_with("<USER_REQUEST>"));
}

#[test]
fn titles_are_read_from_conversation_summaries_db_and_track_updates() {
    let home = tempfile::tempdir().unwrap();
    let roots = ProviderRoots::from_home(home.path().into(), home.path().join("opencode.db"));
    let variant = home.path().join(".gemini/antigravity");
    let logs = variant.join("brain/conv-1/.system_generated/logs");
    std::fs::create_dir_all(&logs).unwrap();
    let transcript = logs.join("transcript_full.jsonl");
    let body = format!(
        "{}\n{}\n",
        serde_json::json!({"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-11-20T06:37:44-08:00","content":"<USER_REQUEST>\nFix PR\n</USER_REQUEST>\n<ADDITIONAL_METADATA>\nThe current local time is: 2026-11-20T06:37:44-08:00.\n\nExtra context\n</ADDITIONAL_METADATA>"}),
        serde_json::json!({"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","created_at":"2026-11-20T06:37:50-08:00","content":"Done."}),
    );
    std::fs::write(&transcript, body).unwrap();
    let summary_db = variant.join("conversation_summaries.db");
    let summary_conn = Connection::open(&summary_db).unwrap();
    summary_conn
        .execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE conversation_summaries (conversation_id TEXT PRIMARY KEY, title TEXT NOT NULL DEFAULT '');
             INSERT INTO conversation_summaries (conversation_id, title) VALUES ('conv-1', '  Fix PR Merge Conflicts  '), ('blank-conv', '   ');",
        )
        .unwrap();

    let temp = tempfile::tempdir().unwrap();
    let store = crate::SessionStore::open(
        crate::StoreOptions::default()
            .db_path(temp.path().join("db"))
            .roots(roots.clone()),
    )
    .unwrap();
    let conn = crate::open_db(store.db_path()).unwrap();
    let provider = Provider::default();
    let env = crate::discover::DiscoveryEnv::with_provider_roots(&conn, roots.clone());
    let candidates =
        <Provider as ShallowSessionProvider>::enumerate(&provider, &env, None).unwrap();
    assert_eq!(candidates.len(), 1);
    let scan = env.scan();
    let shallow = <Provider as ShallowSessionProvider>::read_shallow(
        &provider,
        &scan,
        Some(&conn),
        &candidates[0],
    )
    .unwrap()
    .unwrap();
    assert_eq!(shallow.title.as_deref(), Some("Fix PR Merge Conflicts"));
    assert_eq!(shallow.first_prompt.as_deref(), Some("Fix PR"));

    // Full capture + normalization persists the title and stamps it.
    let context = AcquisitionContext::new(ID, &roots);
    let first = storage::capture(&transcript, &context).unwrap();
    assert_eq!(
        first.payload["title"].as_str(),
        Some("Fix PR Merge Conflicts")
    );
    assert!(first.read_paths.iter().any(|p| p == &summary_db));
    normalize::write(&conn, &first.payload).unwrap();
    let stored_title: Option<String> = conn
        .query_row(
            "SELECT title FROM sessions WHERE source='antigravity' AND session_id=?1",
            [first.payload["sid"].as_str().unwrap()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(stored_title.as_deref(), Some("Fix PR Merge Conflicts"));

    // Updating only conversation_summaries.db changes capture stamp, candidate stamp,
    // fingerprint inputs, and scoped enumeration.
    summary_conn
        .execute(
            "UPDATE conversation_summaries SET title = 'Renamed Title' WHERE conversation_id = 'conv-1'",
            [],
        )
        .unwrap();
    let second = storage::capture(&transcript, &context).unwrap();
    assert_ne!(first.stamp, second.stamp);
    assert_eq!(second.payload["title"].as_str(), Some("Renamed Title"));
    let scoped_env = crate::discover::DiscoveryEnv::with_provider_roots(&conn, roots.clone())
        .with_path_scope(crate::path_scope::PathScope::from_paths([
            summary_db.clone()
        ]));
    let scoped_candidates =
        <Provider as ShallowSessionProvider>::enumerate(&provider, &scoped_env, None).unwrap();
    assert_eq!(scoped_candidates.len(), 1);
    assert_ne!(candidates[0].stamp, scoped_candidates[0].stamp);
    let fp = <Provider as ShallowSessionProvider>::fingerprint_inputs(&provider, &env).unwrap();
    assert!(fp
        .iter()
        .any(|c| c.locator.ends_with("conversation_summaries.db")));

    // Corrupt summary DB never breaks discovery or capture.
    drop(summary_conn);
    std::fs::write(&summary_db, b"not a sqlite db").unwrap();
    let degraded = storage::capture(&transcript, &context).unwrap();
    assert!(degraded.payload["title"].is_null());
}

#[test]
fn shallow_discovery_updates_full_session_title_on_rename_and_retains_when_db_missing() {
    let home = tempfile::tempdir().unwrap();
    let roots = ProviderRoots::from_home(home.path().into(), home.path().join("opencode.db"));
    let variant = home.path().join(".gemini/antigravity");
    let logs = variant.join("brain/conv-1/.system_generated/logs");
    std::fs::create_dir_all(&logs).unwrap();
    let transcript = logs.join("transcript_full.jsonl");
    let body = format!(
        "{}\n{}\n",
        serde_json::json!({"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-11-20T06:37:44-08:00","content":"hello"}),
        serde_json::json!({"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","created_at":"2026-11-20T06:37:50-08:00","content":"hi"}),
    );
    std::fs::write(&transcript, body).unwrap();
    let summary_db = variant.join("conversation_summaries.db");
    let summary_conn = Connection::open(&summary_db).unwrap();
    summary_conn
        .execute_batch(
            "CREATE TABLE conversation_summaries (conversation_id TEXT PRIMARY KEY, title TEXT NOT NULL DEFAULT '');
             INSERT INTO conversation_summaries (conversation_id, title) VALUES ('conv-1', 'Draft');",
        )
        .unwrap();

    let temp = tempfile::tempdir().unwrap();
    let store = crate::SessionStore::open(
        crate::StoreOptions::default()
            .db_path(temp.path().join("db"))
            .roots(roots),
    )
    .unwrap();
    store
        .sync(crate::SyncOptions::default().sources([crate::Source::Antigravity]))
        .unwrap();

    summary_conn
        .execute(
            "UPDATE conversation_summaries SET title = 'Final' WHERE conversation_id = 'conv-1'",
            [],
        )
        .unwrap();
    for pass in 0..2 {
        let report = store
            .discover(crate::DiscoveryOptions::default().sources([crate::Source::Antigravity]))
            .unwrap();
        assert!(report.diagnostics.is_empty());
        if pass == 0 {
            assert_eq!(report.discovered, 1);
        } else {
            assert_eq!(report.skipped_unchanged, 1);
        }
        let sessions: Vec<_> = store
            .sessions(crate::CatalogQuery {
                sources: Some(vec![crate::Source::Antigravity]),
                ..Default::default()
            })
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(sessions.len(), 1);
        assert_eq!(
            sessions[0].title.as_deref(),
            Some("Final"),
            "pass {pass} must return the renamed title on a full row"
        );
    }

    drop(summary_conn);
    std::fs::remove_file(&summary_db).unwrap();
    store
        .discover(crate::DiscoveryOptions::default().sources([crate::Source::Antigravity]))
        .unwrap();
    let after_removal: Vec<_> = store
        .sessions(crate::CatalogQuery {
            sources: Some(vec![crate::Source::Antigravity]),
            ..Default::default()
        })
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(after_removal[0].title.as_deref(), Some("Final"));
}

#[test]
fn untitled_pre_upgrade_cached_shallow_prompt_is_refreshed_and_empty_uploads_skipped() {
    let home = tempfile::tempdir().unwrap();
    let roots = ProviderRoots::from_home(home.path().into(), home.path().join("opencode.db"));
    let logs = home
        .path()
        .join(".gemini/antigravity/brain/conv-untitled/.system_generated/logs");
    std::fs::create_dir_all(&logs).unwrap();
    let transcript = logs.join("transcript_full.jsonl");
    let image_only = "<USER_REQUEST>\n\n</USER_REQUEST>\n<ADDITIONAL_METADATA>\nThe current local time is: 2026-11-26T09:42:49-08:00.\n\nThe user has uploaded 1 image(s):\n- /tmp/a.png\n</ADDITIONAL_METADATA>";
    let real_prompt = "<USER_REQUEST>\nReal follow-up prompt\n</USER_REQUEST>\n<ADDITIONAL_METADATA>\nThe current local time is: 2026-11-26T09:43:00-08:00.\n\nExtra context\n</ADDITIONAL_METADATA>";
    let body = format!(
        "{}\n{}\n{}\n",
        serde_json::json!({"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-11-26T09:42:49-08:00","content":image_only}),
        serde_json::json!({"step_index":1,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-11-26T09:43:00-08:00","content":real_prompt}),
        serde_json::json!({"step_index":2,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","created_at":"2026-11-26T09:43:10-08:00","content":"ok"}),
    );
    std::fs::write(&transcript, body).unwrap();

    let temp = tempfile::tempdir().unwrap();
    let store = crate::SessionStore::open(
        crate::StoreOptions::default()
            .db_path(temp.path().join("db"))
            .roots(roots),
    )
    .unwrap();
    store
        .discover(crate::DiscoveryOptions::default().sources([crate::Source::Antigravity]))
        .unwrap();
    let initial: Vec<_> = store
        .sessions(crate::CatalogQuery {
            sources: Some(vec![crate::Source::Antigravity]),
            ..Default::default()
        })
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(initial.len(), 1);
    assert_eq!(
        initial[0].first_prompt.as_deref(),
        Some("Real follow-up prompt")
    );

    // Seed the pre-upgrade plain file stamp and leaked XML preview on an untitled session.
    let (plain_stamp, _) = crate::file_stamp_and_modified(&transcript).unwrap();
    let pre_upgrade_stamp = format!(
        "v{}:{plain_stamp}",
        crate::discover::SHALLOW_SCANNER_VERSION
    );
    let conn = crate::open_db(store.db_path()).unwrap();
    conn.execute(
        "UPDATE sessions SET first_prompt = ?1, source_stamp = ?2 WHERE source = 'antigravity'",
        rusqlite::params![image_only, &pre_upgrade_stamp],
    )
    .unwrap();
    conn.execute(
        "UPDATE session_observations SET source_stamp = ?1, first_prompt = ?2 WHERE source = 'antigravity'",
        rusqlite::params![&pre_upgrade_stamp, image_only],
    )
    .unwrap();

    let refresh_report = store
        .discover(crate::DiscoveryOptions::default().sources([crate::Source::Antigravity]))
        .unwrap();
    assert_eq!(refresh_report.discovered, 1);
    let refreshed: Vec<_> = store
        .sessions(crate::CatalogQuery {
            sources: Some(vec![crate::Source::Antigravity]),
            ..Default::default()
        })
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(refreshed.len(), 1);
    assert_eq!(
        refreshed[0].first_prompt.as_deref(),
        Some("Real follow-up prompt")
    );

    store
        .sync(crate::SyncOptions::default().sources([crate::Source::Antigravity]))
        .unwrap();
    let stored_prompt: Option<String> = conn
        .query_row(
            "SELECT first_prompt FROM sessions WHERE source = 'antigravity'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(stored_prompt.as_deref(), Some("Real follow-up prompt"));
}

#[test]
fn summary_db_only_reconciliation_updates_titles_without_replaying_transcripts() {
    let home = tempfile::tempdir().unwrap();
    let roots = ProviderRoots::from_home(home.path().into(), home.path().join("opencode.db"));
    let variant = home.path().join(".gemini/antigravity");
    let write_conv = |id: &str, prompt: &str| {
        let logs = variant
            .join("brain")
            .join(id)
            .join(".system_generated/logs");
        std::fs::create_dir_all(&logs).unwrap();
        let path = logs.join("transcript_full.jsonl");
        let body = format!(
            "{}\n{}\n",
            serde_json::json!({"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-11-20T06:37:44-08:00","content":prompt}),
            serde_json::json!({"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","created_at":"2026-11-20T06:37:50-08:00","content":"ok"}),
        );
        std::fs::write(&path, body).unwrap();
        path
    };
    let _conv_a = write_conv("conv-a", "first");
    let conv_b = write_conv("conv-b", "second");
    let summary_db = variant.join("conversation_summaries.db");
    let summary_conn = Connection::open(&summary_db).unwrap();
    summary_conn
        .execute_batch(
            "CREATE TABLE conversation_summaries (conversation_id TEXT PRIMARY KEY, title TEXT NOT NULL DEFAULT '');
             INSERT INTO conversation_summaries (conversation_id, title) VALUES ('conv-a', 'Alpha'), ('conv-b', 'Beta');",
        )
        .unwrap();

    let temp = tempfile::tempdir().unwrap();
    let store = crate::SessionStore::open(
        crate::StoreOptions::default()
            .db_path(temp.path().join("db"))
            .roots(roots.clone()),
    )
    .unwrap();
    store
        .sync(crate::SyncOptions::default().sources([crate::Source::Antigravity]))
        .unwrap();

    // Fingerprint inputs stay stat-only: transcript stamps omit `:title:` while
    // `conversation_summaries.db` is fingerprinted by its own file stat.
    let conn = crate::open_db(store.db_path()).unwrap();
    let env = crate::discover::DiscoveryEnv::with_provider_roots(&conn, roots);
    let fp = <Provider as ShallowSessionProvider>::fingerprint_inputs(&Provider::default(), &env)
        .unwrap();
    assert!(fp
        .iter()
        .any(|c| c.locator.ends_with("conversation_summaries.db")));
    assert!(fp
        .iter()
        .filter(|c| c.locator.ends_with("transcript_full.jsonl"))
        .all(|c| !c.stamp.contains(":title:")));

    // Break conv-b's transcript so any unintended full replay would fail, then
    // rename conv-a and run a summary-DB-only scoped sync (as reconciliation
    // issues for a 1-path batch on `conversation_summaries.db`).
    std::fs::write(&conv_b, b"{not valid json}\n").unwrap();
    summary_conn
        .execute(
            "UPDATE conversation_summaries SET title = 'Alpha Renamed' WHERE conversation_id = 'conv-a'",
            [],
        )
        .unwrap();

    let report = store
        .sync(
            crate::SyncOptions::default()
                .sources([crate::Source::Antigravity])
                .paths([summary_db]),
        )
        .unwrap();
    assert!(report.diagnostics.is_empty());
    assert_eq!(report.changed.len(), 1);
    let titles: std::collections::BTreeMap<String, Option<String>> = conn
        .prepare("SELECT session_id, title FROM sessions WHERE source = 'antigravity'")
        .unwrap()
        .query_map([], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?))
        })
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert!(titles
        .iter()
        .any(|(sid, title)| sid.ends_with(":conv-a") && title.as_deref() == Some("Alpha Renamed")));
    assert!(titles
        .iter()
        .any(|(sid, title)| sid.ends_with(":conv-b") && title.as_deref() == Some("Beta")));
}

#[test]
fn spawned_subagents_are_delegated_children_of_their_parent() {
    let home = tempfile::tempdir().unwrap();
    let roots = ProviderRoots::from_home(home.path().into(), home.path().join("opencode.db"));
    let brain = home.path().join(".gemini/antigravity/brain");
    let transcript = |conv: &str, first: &str| {
        let logs = brain.join(conv).join(".system_generated/logs");
        std::fs::create_dir_all(&logs).unwrap();
        let rows = [
            serde_json::json!({"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-11-20T05:00:34Z","content":first}),
            serde_json::json!({"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","created_at":"2026-11-20T05:00:40Z","content":"ok"}),
        ];
        let body = rows.iter().map(|r| format!("{r}\n")).collect::<String>();
        let path = logs.join("transcript.jsonl");
        std::fs::write(&path, body).unwrap();
        path
    };
    let parent = transcript("parent", "review it");
    let child = transcript("child", "subtask");
    let subagents = brain.join("parent/.system_generated/subagents");
    std::fs::create_dir_all(&subagents).unwrap();
    std::fs::write(
        subagents.join("child.json"),
        r#"{"conversationId":"child","subagentDescriptor":{"typeName":"research","role":"Reviewer"},"state":"SUBAGENT_STATE_ALIVE","spawnStepIndex":1}"#,
    )
    .unwrap();
    // Not evidence of a link: no usable id, or a path-shaped one.
    std::fs::write(subagents.join("bad.json"), r#"{"conversationId":"../x"}"#).unwrap();
    std::fs::write(subagents.join("broken.json"), "{").unwrap();

    let context = AcquisitionContext::new(ID, &roots);
    let temp = tempfile::tempdir().unwrap();
    let store = crate::SessionStore::open(
        crate::StoreOptions::default()
            .db_path(temp.path().join("db"))
            .roots(roots.clone()),
    )
    .unwrap();
    let conn = crate::open_db(store.db_path()).unwrap();
    // Parent first: the child is not indexed yet.
    let p = storage::capture(&parent, &context).unwrap();
    normalize::write(&conn, &p.payload).unwrap();
    let c = storage::capture(&child, &context).unwrap();
    normalize::write(&conn, &c.payload).unwrap();
    let edges: Vec<(String, String, String, String, bool, i64)> = conn
        .prepare("SELECT parent_session_id, child_session_id, relationship, child_agent_name, child_has_events, spawned_at_ms FROM session_relationships WHERE source='antigravity'")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(
        edges,
        vec![(
            p.payload["sid"].as_str().unwrap().to_owned(),
            c.payload["sid"].as_str().unwrap().to_owned(),
            "delegated".to_owned(),
            "Reviewer".to_owned(),
            true,
            chrono::DateTime::parse_from_rfc3339("2026-11-20T05:00:40Z")
                .unwrap()
                .timestamp_millis(),
        )]
    );
    assert!(p.read_paths.iter().any(|p| p.ends_with("child.json")));
    // Session reads expose the edge, since the provider declares it.
    let evidence = store
        .session(
            &crate::SessionRef::id(
                crate::Source::Antigravity,
                p.payload["sid"].as_str().unwrap(),
            ),
            crate::SessionQuery::default(),
        )
        .unwrap()
        .unwrap();
    assert_eq!(evidence.relationships.len(), 1);
    // A sync fingerprint sees the sidecars, not only the transcripts.
    let env = crate::discover::DiscoveryEnv::with_provider_roots(&conn, roots.clone());
    let inputs =
        <Provider as ShallowSessionProvider>::fingerprint_inputs(&Provider::default(), &env)
            .unwrap();
    assert!(inputs
        .iter()
        .any(|c| c.locator.ends_with("subagents/child.json")));
    let count = |conn: &Connection| -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM session_relationships WHERE source='antigravity'",
            [],
            |r| r.get(0),
        )
        .unwrap()
    };
    // A sidecar caught mid-rewrite is not an absence: the capture still
    // succeeds, and the link it evidenced stays.
    std::fs::write(subagents.join("child.json"), b"{\"conversationId\":").unwrap();
    let p = storage::capture(&parent, &context).unwrap();
    normalize::write(&conn, &p.payload).unwrap();
    assert_eq!(count(&conn), 1);
    // A recapture without the sidecar retires the edge it evidenced.
    std::fs::remove_file(subagents.join("child.json")).unwrap();
    let p = storage::capture(&parent, &context).unwrap();
    normalize::write(&conn, &p.payload).unwrap();
    let remaining: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM session_relationships WHERE source='antigravity'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(remaining, 0);
}
