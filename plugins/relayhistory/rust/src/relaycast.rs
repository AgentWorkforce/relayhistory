//! Legacy incremental Relaycast import, explicitly invoked by the optional plugin.
//! The caller owns durable state persistence. This is not a complete source snapshot:
//! channel-only keys can lack DM permission, and old cursors retain their original meaning.
use crate::parse_iso_ms;
use ai_hist_core::{prompt_hash, HistoryEntry, SessionLocation};
use anyhow::{Context, Result};
use rusqlite::Connection;
use serde_json::{json, Map, Value};
pub fn sync_relaycast(conn: &Connection, state: &mut Map<String, Value>) -> Result<usize> {
    let api_key = std::env::var("RELAYCAST_API_KEY").unwrap_or_default();
    let workspace = std::env::var("RELAYCAST_WORKSPACE_ID").unwrap_or_default();
    if api_key.is_empty() || workspace.is_empty() {
        return Ok(0);
    }
    let base =
        std::env::var("RELAYCAST_BASE_URL").unwrap_or_else(|_| "https://api.relaycast.dev".into());
    let mut relay_state = state
        .get("relay")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let mut inserted = 0;
    let channels = relay_get(&base, &api_key, "channels", &[])?
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    for channel in channels {
        let Some(name) = channel.get("name").and_then(Value::as_str) else {
            continue;
        };
        inserted += sync_relay_messages(
            conn,
            &mut relay_state,
            &base,
            &api_key,
            &format!("channels/{name}/messages"),
            &format!("ch:{name}"),
            &format!("#{name}"),
            &workspace,
        )?;
    }
    let conversations = relay_get(&base, &api_key, "dm/conversations/all", &[])
        .ok()
        .and_then(|v| v.get("data").and_then(Value::as_array).cloned())
        .unwrap_or_default();
    for conversation in conversations {
        let Some(id) = conversation.get("id").and_then(Value::as_str) else {
            continue;
        };
        inserted += sync_relay_messages(
            conn,
            &mut relay_state,
            &base,
            &api_key,
            &format!("dm/conversations/{id}/messages"),
            &format!("dm:{id}"),
            &format!("dm:{id}"),
            &workspace,
        )?;
    }
    state.insert("relay".to_string(), Value::Object(relay_state));
    Ok(inserted)
}

// Preserve the legacy importer call contract during package extraction.
#[allow(clippy::too_many_arguments)]
fn sync_relay_messages(
    conn: &Connection,
    relay_state: &mut Map<String, Value>,
    base: &str,
    api_key: &str,
    path: &str,
    state_key: &str,
    fallback_session: &str,
    workspace: &str,
) -> Result<usize> {
    let mut inserted = 0;
    let mut after = relay_state
        .get(state_key)
        .and_then(Value::as_str)
        .map(str::to_string);
    let mut max_id = after.clone();
    loop {
        let mut params = vec![("limit", "100")];
        if let Some(after) = after.as_deref() {
            params.push(("after", after));
        }
        let messages = relay_get(base, api_key, path, &params)?
            .get("data")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if messages.is_empty() {
            break;
        }
        for msg in &messages {
            let text = msg.get("text").and_then(Value::as_str).unwrap_or("");
            if text.is_empty() {
                continue;
            }
            let sender = msg
                .get("from_name")
                .or_else(|| msg.get("from_id"))
                .and_then(Value::as_str)
                .unwrap_or("");
            let prompt = if sender.is_empty() {
                text.to_string()
            } else {
                format!("[{sender}] {text}")
            };
            let session_id = msg
                .get("thread_id")
                .and_then(Value::as_str)
                .unwrap_or(fallback_session);
            let timestamp_ms = msg
                .get("created_at")
                .and_then(Value::as_str)
                .and_then(parse_iso_ms)
                .unwrap_or(0);
            inserted += ai_hist_core::insert_history_at_location(
                conn,
                &HistoryEntry {
                    id: 0,
                    source: "relay".into(),
                    session_id: Some(session_id.to_string()),
                    project: Some(workspace.to_string()),
                    prompt_hash: Some(prompt_hash(&prompt)),
                    prompt,
                    timestamp_ms,
                },
                SessionLocation::Remote,
            )?;
            if let Some(id) = msg.get("id").and_then(Value::as_str) {
                if max_id.as_deref().is_none_or(|current| id > current) {
                    max_id = Some(id.to_string());
                }
            }
        }
        if messages.len() < 100 {
            break;
        }
        after = messages
            .last()
            .and_then(|msg| msg.get("id"))
            .and_then(Value::as_str)
            .map(str::to_string);
        if after.is_none() {
            break;
        }
    }
    if let Some(max_id) = max_id {
        relay_state.insert(state_key.to_string(), json!(max_id));
    }
    Ok(inserted)
}

fn relay_get(base: &str, api_key: &str, path: &str, params_: &[(&str, &str)]) -> Result<Value> {
    let mut url = format!("{}/v1/{}", base.trim_end_matches('/'), path);
    if !params_.is_empty() {
        url.push('?');
        url.push_str(
            &params_
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join("&"),
        );
    }
    let output = std::process::Command::new("curl")
        .arg("-fsSL")
        .arg("-H")
        .arg(format!("Authorization: Bearer {api_key}"))
        .arg("-H")
        .arg("Accept: application/json")
        .arg(url)
        .output()
        .context("running curl for Relaycast API")?;
    anyhow::ensure!(
        output.status.success(),
        "Relaycast API request failed with status {}",
        output.status
    );
    Ok(serde_json::from_slice(&output.stdout)?)
}

#[cfg(test)]
mod tests;
