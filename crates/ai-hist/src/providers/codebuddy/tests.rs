use super::*;
#[test]
fn native_integer_usage_preserves_unknown_counters() {
    let usage = usage::normalize(
        &serde_json::json!({"prompt_tokens":23,"completion_tokens":7,"total_tokens":30}),
    )
    .unwrap()
    .unwrap();
    assert_eq!(usage.input_tokens, 23);
    assert!(!usage.coverage.has_cache_read_tokens);
    assert!(usage::normalize(&serde_json::json!({"prompt_tokens":1.5})).is_err());
    assert!(usage::normalize(&serde_json::json!({})).unwrap().is_none());
}
#[test]
fn incomplete_tail_is_not_a_complete_rewrite() {
    let bytes = b"{\"type\":\"message\",\"role\":\"user\",\"id\":\"u\",\"timestamp\":1,\"content\":\"fixture\"}\n{";
    let (rows, partial) = format::parse(bytes, false).unwrap();
    assert_eq!(rows.len(), 1);
    assert!(partial);
    assert!(format::parse(b"bad\n", false).is_err());
}

#[test]
fn root_override_freezes_relative_paths_without_expanding_tilde() {
    let isolated = tempfile::tempdir().unwrap();
    let home = isolated.path();
    let work = home.join("work");
    let cwd = work.as_path();
    assert_eq!(
        roots::from_value(home, Some(std::ffi::OsStr::new("relative")), cwd),
        cwd.join("relative")
    );
    assert_eq!(
        roots::from_value(home, Some(std::ffi::OsStr::new("~/literal")), cwd),
        cwd.join("~/literal")
    );
    assert_eq!(
        roots::from_value(home, Some(std::ffi::OsStr::new("  ")), cwd),
        home.join(".codebuddy")
    );
    assert_eq!(
        roots::from_value(home, Some(std::ffi::OsStr::new(" padded ")), cwd),
        cwd.join(" padded ")
    );
    assert_eq!(
        roots::resolve(home, false).paths,
        vec![home.join(".codebuddy/projects")]
    );
}
#[test]
fn usage_subtracts_only_reported_cache_and_rejects_underflow() {
    let v = serde_json::json!({"prompt_tokens":23,"completion_tokens":7,"total_tokens":30,"prompt_tokens_details":{"cached_tokens":5},"completion_tokens_details":{"reasoning_tokens":2}});
    let n = usage::normalize(&v).unwrap().unwrap();
    assert_eq!(n.input_tokens, 18);
    assert_eq!(n.cache_read_tokens, 5);
    assert_eq!(n.reasoning_tokens, Some(2));
    assert!(n.coverage.has_cache_read_tokens);
    assert!(usage::normalize(
        &serde_json::json!({"prompt_tokens":2,"prompt_tokens_details":{"cached_tokens":5}})
    )
    .is_err());
}
#[test]
fn native_parent_shape_and_adjacent_fragments_are_checked() {
    let row = serde_json::json!({"type":"message","id":"a","timestamp":1,"parentId":"u","role":"assistant","content":"first"});
    let mut second = row.clone();
    second["content"] = Value::String("second".into());
    let bytes = format!("{row}\n{second}\n");
    assert_eq!(format::parse(bytes.as_bytes(), false).unwrap().0.len(), 2);
    second["parentId"] = Value::from(42);
    assert!(format::parse(format!("{second}\n").as_bytes(), false).is_err());
}

#[test]
fn literal_control_lookalikes_remain_user_text() {
    for text in [
        "Discuss <cb_summary_extra> literally.",
        "Discuss <teammate-messageboard> literally.",
        "please continued summarized context above",
        "please continue: summarized context above",
    ] {
        let row = serde_json::json!({"type":"message","role":"user","content":text});
        assert!(format::human_message(&row), "{text}");
    }
    for text in [
        "<cb_summary>actual summary</cb_summary>",
        "<teammate-message>actual control</teammate-message>",
    ] {
        let row = serde_json::json!({"type":"message","role":"user","content":text});
        assert!(!format::human_message(&row), "{text}");
    }
}
#[test]
fn capture_stamp_is_portable_and_exact_bytes_are_frozen() {
    let fixture=include_bytes!("../../../tests/fixtures/codebuddy/ordinary/.codebuddy/projects/fixture-workspace/fixture-ordinary.jsonl");
    let one = tempfile::tempdir().unwrap();
    let two = tempfile::tempdir().unwrap();
    let stage = |home: &Path| {
        let path = home.join(".codebuddy/projects/fixture-workspace/fixture-ordinary.jsonl");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, fixture).unwrap();
        path
    };
    let p1 = stage(one.path());
    let p2 = stage(two.path());
    let r1 = ProviderRoots::from_home(one.path().into(), one.path().join("unused.db"));
    let r2 = ProviderRoots::from_home(two.path().into(), two.path().join("unused.db"));
    let first = storage::capture(&p1, &AcquisitionContext::new(ID, &r1)).unwrap();
    let second = storage::capture(&p2, &AcquisitionContext::new(ID, &r2)).unwrap();
    assert_eq!(first.stamp, second.stamp);
    assert_eq!(first.read_paths, vec![p1.clone()]);
    let mut bytes = fixture.to_vec();
    bytes.push(b'\n');
    std::fs::write(&p1, bytes).unwrap();
    let changed = storage::capture(&p1, &AcquisitionContext::new(ID, &r1)).unwrap();
    assert_ne!(first.stamp, changed.stamp);
    assert_eq!(first.payload["rows"], changed.payload["rows"]);
    std::fs::write(&p1, "bad\n").unwrap();
    assert_eq!(first.payload["rows"], second.payload["rows"]);
    let mut custom = r1.clone();
    custom
        .provider_roots
        .insert(ID.into(), vec![two.path().join(".codebuddy/projects")]);
    assert_ne!(
        roots::identity(&custom, &p2).unwrap(),
        roots::identity(&r2, &p2).unwrap()
    );
}
