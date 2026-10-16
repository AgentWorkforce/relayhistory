use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

const KEY: &str = "codex_sqlite_sweep_cursor_v1";

#[derive(Clone, Deserialize, Serialize)]
pub(super) struct SweepCursor {
    pub(super) updated_at_ms: i64,
    pub(super) thread_id: String,
    #[serde(default)]
    pub(super) source_fingerprint: Option<String>,
    #[serde(default)]
    pub(super) retry_required: bool,
    #[serde(default)]
    pub(super) restart: bool,
    #[serde(default)]
    pub(super) min_updated_ms: Option<i64>,
}

pub(super) fn load(state: &Map<String, Value>) -> Option<SweepCursor> {
    state.get(KEY).and_then(|value| match value {
        Value::String(encoded) => serde_json::from_str(encoded).ok(),
        // Accept the short-lived prerelease encoding so a checkout that
        // already ran it can migrate without stranding its backlog.
        Value::Object(_) => serde_json::from_value(value.clone()).ok(),
        _ => None,
    })
}

pub(super) fn store(state: &mut Map<String, Value>, cursor: Option<SweepCursor>) {
    match cursor {
        Some(cursor) => {
            state.insert(KEY.into(), json!(serde_json::to_string(&cursor).expect("cursor")));
        }
        None => {
            // Sync-state checkpoints merge keys forward. An explicit scalar
            // reset both survives that merge and replaces the cursor atomically.
            state.insert(KEY.into(), Value::Null);
        }
    }
}
