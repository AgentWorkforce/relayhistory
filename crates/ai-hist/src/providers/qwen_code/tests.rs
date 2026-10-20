use super::*;
#[test]
fn native_writer_matches_independent_reader() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/qwen-code");
    let oracle: Value =
        serde_json::from_slice(&std::fs::read(root.join("official-reader.json")).unwrap()).unwrap();
    for case in [
        "ordinary",
        "tool",
        "resumed",
        "rewind",
        "metadata",
        "same-time",
    ] {
        let home = root.join(case);
        let roots = ProviderRoots::from_home(home.clone(), home.join("opencode.db"));
        let chats = home.join(".qwen/projects/-synthetic-qwen-project/chats");
        let path = std::fs::read_dir(chats)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let captured = storage::capture(&path, &AcquisitionContext::new(ID, &roots)).unwrap();
        assert_eq!(
            captured.payload["active"], oracle[case]["records"],
            "{case}"
        );
    }
}
#[test]
fn malformed_partial_and_managed_are_distinct() {
    assert!(format::parse(b"{\"uuid\":", false).unwrap().1);
    assert!(format::parse(b"{broken}", false).is_err());
    assert!(format::parse(b"{\"uuid\":\n", false).is_err());
    let error = format::parse(br#"{"subtype":"managed_session_header_v1"}"#, false).unwrap_err();
    assert!(error.to_string().contains("UNSUPPORTED_MANAGED"));
}
#[test]
fn native_uuid_fragments_are_aggregated_and_parent_cycles_rejected() {
    let root=Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/qwen-code/ordinary/.qwen/projects/-synthetic-qwen-project/chats/11111111-1111-4111-8111-000000000001.jsonl");
    let (mut rows, _) = format::parse(&std::fs::read(root).unwrap(), false).unwrap();
    let mut fragment = rows[1].clone();
    fragment["message"]["parts"] = serde_json::json!([{"text":"More native fragment."}]);
    rows.push(fragment);
    let active = format::project(&rows).unwrap();
    assert_eq!(active.len(), 2);
    assert_eq!(
        format::text(&active[1]),
        "Found the synthetic Qwen needle.\nMore native fragment."
    );
    rows[0]["parentUuid"] = rows[1]["uuid"].clone();
    assert!(format::project(&rows).is_err());
}
#[test]
fn usage_preserves_unknown_and_zero() {
    assert!(usage::normalize(&serde_json::json!({})).unwrap().is_none());
    let u=usage::normalize(&serde_json::json!({"promptTokenCount":0,"candidatesTokenCount":2,"cachedContentTokenCount":0,"totalTokenCount":2})).unwrap().unwrap();
    assert_eq!(u.input_tokens, 0);
    assert!(u.coverage.has_input_tokens);
    assert!(!u.coverage.has_cache_write_tokens);
    assert_eq!(u.provider_total_tokens, Some(2));
    assert!(usage::normalize(&serde_json::json!({"promptTokenCount":-1})).is_err());
}
#[test]
fn artifact_only_and_missing_provenance_are_explicitly_unsupported() {
    let root=Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/qwen-code/metadata/.qwen/projects/-synthetic-qwen-project/chats/11111111-1111-4111-8111-000000000005.jsonl");
    let (rows, _) = format::parse(&std::fs::read(root).unwrap(), false).unwrap();
    let artifact = rows
        .iter()
        .find(|r| r["subtype"] == "session_artifact_snapshot")
        .unwrap();
    assert!(format::project(std::slice::from_ref(artifact))
        .unwrap()
        .is_empty());
    let home = tempfile::tempdir().unwrap();
    let path = home
        .path()
        .join(".qwen/projects/project/chats/11111111-1111-4111-8111-000000000005.jsonl");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, artifact.to_string()).unwrap();
    let roots = ProviderRoots::from_home(home.path().into(), home.path().join("opencode.db"));
    assert!(
        storage::capture(&path, &AcquisitionContext::new(ID, &roots))
            .unwrap_err()
            .to_string()
            .contains("NO_CONVERSATION")
    );
    let mut user = rows[0].clone();
    user.as_object_mut().unwrap().remove("provenance");
    assert!(format::parse(user.to_string().as_bytes(), false)
        .unwrap_err()
        .to_string()
        .contains("UNSUPPORTED_PROVENANCE"));
    user["provenance"] = "system".into();
    assert_eq!(format::role(&user), None);
}
#[test]
fn user_display_projection_requires_exact_native_hook_provenance() {
    let mut row = serde_json::json!({"type":"user","provenance":"real_user","message":{"parts":[{"text":"Literal <qwen:user-prompt-submit-context> stays."},{"text":"<qwen:user-prompt-submit-context>\nSynthetic hook instructions\n</qwen:user-prompt-submit-context>"}]}});
    assert_eq!(
        format::text(&row),
        "Literal <qwen:user-prompt-submit-context> stays."
    );
    row["systemPayload"] = serde_json::json!({"hookContext":"Synthetic hook instructions","displayText":"Native displayed request."});
    assert_eq!(format::text(&row), "Native displayed request.");
    row["message"]["parts"] = serde_json::json!([{"text":"<qwen:user-prompt-submit-context>\nLiteral sole user text\n</qwen:user-prompt-submit-context>"}]);
    row.as_object_mut().unwrap().remove("systemPayload");
    assert!(format::text(&row).contains("Literal sole user text"));
}
#[test]
fn usage_subtracts_inclusive_cache_and_rejects_underflow() {
    let u=usage::normalize(&serde_json::json!({"promptTokenCount":100,"candidatesTokenCount":20,"cachedContentTokenCount":10,"totalTokenCount":120})).unwrap().unwrap();
    assert_eq!(u.input_tokens, 90);
    assert_eq!(u.cache_read_tokens, 10);
    assert_eq!(u.reasoning_tokens, None);
    assert!(usage::normalize(
        &serde_json::json!({"promptTokenCount":2,"cachedContentTokenCount":3})
    )
    .is_err());
}
#[test]
fn root_namespaces_and_frozen_default_ignore_runtime_environment() {
    let home = Path::new("/synthetic/home");
    let roots = ProviderRoots::from_home(home.into(), home.join("opencode.db"));
    assert_eq!(roots.for_provider(ID), [home.join(".qwen/projects")]);
    let a = roots::identity(
        &roots,
        &home.join(".qwen/projects/project-a/chats/native.jsonl"),
    )
    .unwrap();
    let b = roots::identity(
        &roots,
        &home.join(".qwen/projects/project-b/chats/native.jsonl"),
    )
    .unwrap();
    assert_ne!(a, b);
    assert!(roots::identity(
        &roots,
        &home.join(".qwen/projects/project/chats/../native.jsonl")
    )
    .is_err());
    let restored: ProviderRoots =
        serde_json::from_value(serde_json::to_value(&roots).unwrap()).unwrap();
    assert_eq!(roots.for_provider(ID), restored.for_provider(ID));
}
#[test]
fn fragment_time_usage_and_trailing_artifact_match_independent_reader() {
    let oracle: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/qwen-code/derived-reader.json"
    ))
    .unwrap();
    let rows = oracle["rows"].as_array().unwrap();
    let active = format::project(rows).unwrap();
    assert_eq!(
        serde_json::to_value(active).unwrap(),
        oracle["reader"]["records"]
    );
}
