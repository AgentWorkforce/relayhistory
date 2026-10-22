use serde_json::Value;
use sha2::{Digest, Sha256};

pub(super) fn bounded_arguments(input: &Value) -> String {
    let full = input.to_string();
    if full.len() <= 256 * 1024 {
        return full;
    }
    serde_json::json!({
        "projection": "kilo-tool-arguments-v1",
        "truncated": true,
        "native_payload_bytes": full.len(),
        "native_payload_hash": format!("{:x}", Sha256::digest(full.as_bytes())),
        "preview": full.chars().take(8 * 1024).collect::<String>(),
    })
    .to_string()
}

pub(super) fn bounded_text(text: &str) -> String {
    if Value::String(text.to_owned()).to_string().len() <= 256 * 1024 {
        return text.to_owned();
    }
    let projection = serde_json::json!({
        "projection": "kilo-text-v1",
        "truncated": true,
        "native_payload_bytes": text.len(),
        "native_payload_hash": format!("{:x}", Sha256::digest(text.as_bytes())),
    });
    format!(
        "{}\n\n[Native Kilo text projection]\n{projection}",
        text.chars().take(32 * 1024).collect::<String>()
    )
}
