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
