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
    let inputs = <Provider as ShallowSessionProvider>::fingerprint_inputs(&Provider, &env).unwrap();
    assert!(inputs
        .iter()
        .any(|c| c.locator.ends_with("subagents/child.json")));
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
