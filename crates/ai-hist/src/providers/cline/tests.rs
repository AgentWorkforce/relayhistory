use super::*;
#[test]
fn root_precedence_follows_pinned_native_resolver() {
    let home = Path::new("/synthetic/home");
    assert_eq!(
        roots_from_values(home, Some("/sessions"), Some("/data"), Some("/config")).paths,
        vec![PathBuf::from("/sessions")]
    );
    assert_eq!(
        roots_from_values(home, None, Some("/data"), Some("/config")).paths,
        vec![PathBuf::from("/data/sessions")]
    );
    assert_eq!(
        roots_from_values(home, None, None, Some("/config")).paths,
        vec![PathBuf::from("/config/data/sessions")]
    );
    assert_eq!(
        roots_from_values(home, None, None, None).paths,
        vec![home.join(".cline/data/sessions")]
    );
}
#[test]
fn unsupported_or_partial_snapshot_cannot_be_admitted() {
    let fixture=include_bytes!("../../../tests/fixtures/cline/native/.cline/data/sessions/fixture-success-01/fixture-success-01.messages.json");
    let mut value: Value = serde_json::from_slice(fixture).unwrap();
    value["version"] = 2.into();
    assert!(
        format::parse(&serde_json::to_vec(&value).unwrap(), "fixture-success-01")
            .unwrap_err()
            .to_string()
            .contains("UNSUPPORTED_SCHEMA")
    );
    assert!(
        format::parse(&fixture[..fixture.len() / 2], "fixture-success-01")
            .unwrap_err()
            .to_string()
            .contains("INCOMPLETE_SNAPSHOT")
    );
}

#[test]
fn many_undated_envelopes_share_the_next_native_time_in_one_pass() {
    let mut messages = vec![serde_json::json!({"role":"user","content":[]}); 10_000];
    messages.push(serde_json::json!({"role":"assistant","ts":123,"content":[]}));
    messages.push(serde_json::json!({"role":"user","content":[]}));
    let times = format::message_times(&messages, 456);
    assert_eq!(times.len(), 10_002);
    assert!(times[..10_000]
        .iter()
        .all(|value| *value == (123, "next_assistant_ts")));
    assert_eq!(times[10_000], (123, "native_ts"));
    assert_eq!(times[10_001], (456, "snapshot_updated_at"));
}

#[test]
fn multibyte_and_escaped_result_text_keep_a_bounded_readable_prefix() {
    for tail in ["🦀".repeat(40_000), "\u{0000}".repeat(40_000)] {
        let full = format!("readable-start {tail}");
        let visible = format::bounded_visible_text(&full);
        assert!(visible.starts_with("readable-start "));
        assert!(visible.contains("\"truncated\":true"));
        assert!(serde_json::to_string(&visible).unwrap().len() <= 128 * 1024);

        let native = serde_json::Value::String(full);
        let facts = crate::ingest::ToolResultFacts::from_payload(&native);
        let result = format::result_text(&native, &facts);
        assert!(result.starts_with("readable-start "));
        assert!(result.contains("\"truncated\":true"));
        assert!(serde_json::to_string(&result).unwrap().len() <= 128 * 1024);
    }
}
