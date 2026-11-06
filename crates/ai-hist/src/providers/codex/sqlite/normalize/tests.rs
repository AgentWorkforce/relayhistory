use super::*;

#[test]
fn projection_digest_keeps_the_existing_canonical_representation() {
    let capture: Capture = serde_json::from_value(json!({
        "thread": {
            "id":"thread", "created":1, "updated":2, "archived":false,
            "next_ordinal":1, "next_offset":100, "source":"cli",
            "cwd":"/tmp/雪", "branch":"main", "model":"codex", "tokens_used":123
        },
        "items": [{
            "id":"item", "turn_id":"turn", "ts":2, "kind":"agentMessage",
            "value": {
                "id":"item", "type":"agentMessage",
                "text":"A quote: \" and a slash: \\ and a newline:\n雪",
                "nested":{"z":[1,null,true],"a":"first"}
            }
        }],
        "turns":[{"id":"turn","ts":1,"error":"{\"message\":\"failed\"}"}],
        "edges":[{"parent":"p","child":"c","status":"running"}]
    }))
    .unwrap();
    let t = &capture.thread;
    let previous = json!({
        "items":capture.items, "turns":capture.turns, "edges":capture.edges,
        "source":t.source, "thread_source":t.thread_source, "cwd":t.cwd,
        "branch":t.branch, "model":t.model, "tokens_used":t.tokens_used,
    });
    let previous_hash = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&previous).unwrap())
    );
    assert_eq!(content_hash(&capture).unwrap(), previous_hash);
}
