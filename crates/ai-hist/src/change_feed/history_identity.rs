//! History's legacy keys are byte-for-byte stable. Native keys occupy a
//! disjoint tagged namespace and exclude mutable time/text from identity.
use super::StoredRow;
use serde_json::{json, Value};

pub(super) fn record_key_sql(row: &str) -> String {
    format!("CASE WHEN {row}.event_uid IS NULL THEN json_array({row}.timestamp_ms,{row}.prompt) ELSE json_array('event',{row}.session_id,{row}.event_uid) END")
}

pub(super) fn key_from_identity(source: &str, record: Vec<Value>) -> Vec<Value> {
    let mut key = vec![json!("history"), json!(source)];
    key.extend(record);
    key
}

pub(super) fn key_from_row(row: &StoredRow) -> Vec<Value> {
    if row.get("event_uid").is_some_and(|v| !v.is_null()) {
        vec![
            json!("history"),
            row.get("source").cloned().unwrap_or(Value::Null),
            json!("event"),
            row.get("session_id").cloned().unwrap_or(Value::Null),
            row.get("event_uid").cloned().unwrap_or(Value::Null),
        ]
    } else {
        vec![
            json!("history"),
            row.get("source").cloned().unwrap_or(Value::Null),
            row.get("timestamp_ms").cloned().unwrap_or(Value::Null),
            row.get("prompt").cloned().unwrap_or(Value::Null),
        ]
    }
}

use super::HistoryEntry;
/// The typed [`HistoryEntry`] columns, in the order [`row_to_history`] reads.
pub(super) const HISTORY_COLUMNS: &str =
    "id, source, session_id, project, prompt, prompt_hash, timestamp_ms, event_uid";

pub(super) fn row_to_history(row: &rusqlite::Row<'_>) -> rusqlite::Result<HistoryEntry> {
    Ok(HistoryEntry {
        event_uid: row.get(7)?,
        id: row.get(0)?,
        source: row.get(1)?,
        session_id: row.get(2)?,
        project: row.get(3)?,
        prompt: row.get(4)?,
        prompt_hash: row.get(5)?,
        timestamp_ms: row.get(6)?,
    })
}
