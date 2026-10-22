use super::*;
const META: &[u8] = include_bytes!(
    "../../../tests/fixtures/cortex/native/.snowflake/cortex/conversations/11111111-1111-4111-8111-111111111111.json"
);
const HISTORY: &[u8] = include_bytes!(
    "../../../tests/fixtures/cortex/native/.snowflake/cortex/conversations/11111111-1111-4111-8111-111111111111.history.jsonl"
);
const FLAT_META: &[u8] = include_bytes!(
    "../../../tests/fixtures/cortex/native/.snowflake/cortex/conversations/22222222-2222-4222-8222-222222222222.json"
);
const FLAT_HISTORY: &[u8] = include_bytes!(
    "../../../tests/fixtures/cortex/native/.snowflake/cortex/conversations/22222222-2222-4222-8222-222222222222.history.jsonl"
);
#[test]
fn native_writer_fixture_has_provider_ids_and_tools() {
    let meta = format::metadata(META, "11111111-1111-4111-8111-111111111111").unwrap();
    let (entries, partial) = format::history(&meta, Some(HISTORY)).unwrap();
    assert!(!partial);
    assert_eq!(entries.len(), 4);
    assert_eq!(
        format::first_prompt(&entries).as_deref(),
        Some("Create a demo table")
    );
    assert_eq!(entries[1]["uuid"], "assistant-1");
    assert_eq!(entries[1]["message"]["content"][1]["id"], "tool-1");
    assert_eq!(entries[2]["message"]["content"][0]["tool_use_id"], "tool-1");
}
#[test]
fn native_last_id_wins_at_last_position() {
    let meta = format::metadata(META, "11111111-1111-4111-8111-111111111111").unwrap();
    let mut bytes = HISTORY.to_vec();
    bytes.extend_from_slice(b"{\"role\":\"user\",\"id\":\"user-1\",\"content\":[{\"type\":\"text\",\"text\":\"Revised\"}],\"user_sent_time\":\"2026-10-21T10:00:04.000Z\"}\n");
    let (entries, _) = format::history(&meta, Some(&bytes)).unwrap();
    assert_eq!(entries.len(), 4);
    assert_eq!(entries.last().unwrap()["uuid"], "user-1");
    assert_eq!(format::first_prompt(&entries).as_deref(), Some("Revised"));
}
#[test]
fn native_flat_rows_match_restart_reader_groups() {
    let meta = format::metadata(FLAT_META, "22222222-2222-4222-8222-222222222222").unwrap();
    let (entries, partial) = format::history(&meta, Some(FLAT_HISTORY)).unwrap();
    assert!(!partial);
    let oracle: Value = serde_json::from_str(include_str!(
        "../../../../../plans/history-providers/fixtures/cortex/flat-reader-oracle.json"
    ))
    .unwrap();
    let ids: Vec<_> = entries
        .iter()
        .map(|entry| entry["uuid"].as_str().unwrap())
        .collect();
    let expected: Vec<_> = oracle
        .as_array()
        .unwrap()
        .iter()
        .map(|message| message["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, expected);
    assert_eq!(entries[1]["message"]["content"][1]["id"], "flat-tool-1");
}
#[test]
fn incomplete_tail_keeps_complete_history() {
    let meta = format::metadata(META, "11111111-1111-4111-8111-111111111111").unwrap();
    let mut bytes = HISTORY.to_vec();
    bytes.extend_from_slice(b"{\"role\":\"assistant\"");
    let (entries, partial) = format::history(&meta, Some(&bytes)).unwrap();
    assert!(partial);
    assert_eq!(entries.len(), 4);
}
#[test]
fn unknown_native_result_status_is_not_completed_or_error() {
    let meta = format::metadata(META, "11111111-1111-4111-8111-111111111111").unwrap();
    let row = serde_json::json!({
        "role":"user","id":"pending-result","user_sent_time":"2026-10-21T10:00:05.000Z",
        "content":[{"type":"tool_result","tool_result":{"tool_use_id":"tool-1","status":"pending","content":[]}}]
    });
    let line = serde_json::to_vec(&row).unwrap();
    let mut bytes = HISTORY.to_vec();
    bytes.extend(line);
    bytes.push(b'\n');
    let (entries, _) = format::history(&meta, Some(&bytes)).unwrap();
    let result = &entries.last().unwrap()["message"]["content"][0];
    assert_eq!(result["is_error"], Value::Null);
    assert_eq!(result["native_status"], "pending");
}
#[test]
fn internal_text_keeps_native_marker_payload_without_prompt_projection() {
    let meta = format::metadata(META, "11111111-1111-4111-8111-111111111111").unwrap();
    let row = serde_json::json!({
        "role":"user","id":"internal-user","user_sent_time":"2026-10-21T10:00:06.000Z",
        "content":[{"type":"text","text":"hidden command","is_user_prompt":false,"internalOnly":true}]
    });
    let mut bytes = serde_json::to_vec(&row).unwrap();
    bytes.push(b'\n');
    let (entries, _) = format::history(&meta, Some(&bytes)).unwrap();
    assert_eq!(format::first_prompt(&entries), None);
    assert_eq!(
        entries[0]["message"]["content"][0]["payload"]["text"],
        "hidden command"
    );
}
#[test]
fn root_default_is_native_conversations_tree() {
    let home = tempfile::tempdir().unwrap();
    let roots = resolve_roots(home.path(), false);
    assert_eq!(
        roots.paths[0],
        home.path().join(".snowflake/cortex/conversations")
    );
}
