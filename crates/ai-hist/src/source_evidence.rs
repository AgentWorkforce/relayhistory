//! Validated canonical evidence records accepted from installed source adapters.
//! Foreign database row ids are retained in observations but never assigned locally.
use crate::observations::ObservationKey;
use anyhow::{ensure, Context, Result};
use rusqlite::{params_from_iter, Connection};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceKind {
    History,
    SessionEvent,
    ToolCall,
    FileEdit,
    Relationship,
    CommitLink,
}
pub const FULL_SESSION_KINDS: &[EvidenceKind] = &[
    EvidenceKind::History,
    EvidenceKind::SessionEvent,
    EvidenceKind::ToolCall,
    EvidenceKind::FileEdit,
    EvidenceKind::Relationship,
];
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvidenceRecord {
    pub kind: EvidenceKind,
    pub payload: Map<String, Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision_id: Option<String>,
}
struct Spec {
    table: &'static str,
    columns: &'static str,
    required: &'static str,
    key: &'static str,
}
impl EvidenceKind {
    fn spec(self) -> Spec {
        match self {
        Self::History=>Spec{table:"history",columns:"source,session_id,project,prompt,prompt_hash,timestamp_ms,git_branch",required:"source,session_id,prompt,timestamp_ms",key:"source,timestamp_ms,prompt"},
        Self::SessionEvent=>Spec{table:"session_events",columns:"source,session_id,project,cwd,git_branch,message_id,parent_id,ts_ms,role,kind,text,model,token_json,event_uid,tool_use_id,payload_bytes,payload_truncated,payload_hash,call_index,event_index,result_status,event_source,error_signal,subagent_session_id,agent_id",required:"source,session_id,ts_ms,role,kind,event_uid",key:"source,session_id,event_uid"},
        Self::ToolCall=>Spec{table:"tool_calls",columns:"source,session_id,message_id,tool_use_id,name,target,args_json,is_error,ts_ms",required:"source,session_id,tool_use_id,name",key:"source,session_id,tool_use_id"},
        Self::FileEdit=>Spec{table:"file_edits",columns:"source,session_id,message_id,tool_use_id,file_path,tool_name,lines_added,lines_removed,structured_patch_json,user_modified,ts_ms,git_branch,cwd",required:"source,session_id,tool_use_id,file_path,tool_name",key:"source,session_id,tool_use_id"},
        Self::Relationship=>Spec{table:"session_relationships",columns:"source,parent_session_id,relationship_uid,child_session_id,relationship,identity_status,child_agent_type,child_agent_name,child_model,spawn_depth,evidence_kind,evidence_locator,evidence_ref,child_has_events,spawned_at_ms,created_ms,updated_ms",required:"source,parent_session_id,relationship_uid,relationship,identity_status,evidence_kind,created_ms,updated_ms",key:"source,parent_session_id,relationship_uid"},
        Self::CommitLink=>Spec{table:"session_commit_links",columns:"source,session_id,repo,branch,commit_sha,note_ref,match_method,confidence,files_json,numstat_json,evidence_json,created_at_ms",required:"source,session_id,repo,commit_sha,match_method,confidence,created_at_ms",key:"source,session_id,commit_sha,match_method"},
    }
    }
}
fn numeric(field: &str) -> bool {
    matches!(
        field,
        "timestamp_ms"
            | "ts_ms"
            | "lines_added"
            | "lines_removed"
            | "spawn_depth"
            | "spawned_at_ms"
            | "created_ms"
            | "updated_ms"
            | "created_at_ms"
            | "confidence"
            | "payload_bytes"
            | "call_index"
            | "event_index"
    )
}
/// The `session_events` columns that only a tool-result row may carry, and
/// the closed vocabulary each string-valued one is drawn from.
///
/// A submitted value reaches `SessionEvent.resultStatus` in the TypeScript
/// SDK, which types it as a closed union. Accepting any string here would let
/// an adapter put `"successful"` where every consumer has been told to expect
/// `"completed"`, and the cast at the boundary would not notice -- a value
/// that is well-formed and wrong, served beside measured ones.
const TOOL_RESULT_FIELDS: &[&str] = &[
    "tool_use_id",
    "payload_bytes",
    "payload_truncated",
    "payload_hash",
    "call_index",
    "event_index",
    "result_status",
    "event_source",
    "error_signal",
    "subagent_session_id",
    "agent_id",
];
const RESULT_STATUSES: &[&str] = &["running", "completed", "errored", "cancelled", "unknown"];
const EVENT_SOURCES: &[&str] = &[
    "tool_result",
    "subagent_notification",
    "function_call_output",
];
const ERROR_SIGNALS: &[&str] = &[
    "tool_result.is_error",
    "exit_code",
    "patch_apply",
    "mcp_err",
    "subagent_status",
];

/// The fidelity counts that cannot be negative. A byte count of `-1` is not a
/// small payload; it is a bug upstream, and ranking by it puts the row first.
const NON_NEGATIVE_FIELDS: &[&str] = &["payload_bytes", "call_index", "event_index"];

fn boolean(field: &str) -> bool {
    matches!(
        field,
        "is_error" | "user_modified" | "child_has_events" | "payload_truncated"
    )
}

/// Validate the complete response before opening a database or mutating history.
pub fn validate_records(
    key: &ObservationKey,
    covered: &[EvidenceKind],
    records: &mut [EvidenceRecord],
) -> Result<()> {
    key.validate()?;
    let unique = covered.iter().collect::<std::collections::BTreeSet<_>>();
    ensure!(
        !covered.is_empty() && unique.len() == covered.len(),
        "INVALID_ARGUMENT: covered_kinds must be nonempty and unique"
    );
    let mut identities = std::collections::HashSet::new();
    for record in records {
        ensure!(
            covered.contains(&record.kind),
            "INVALID_ARGUMENT: record kind is not covered by this snapshot"
        );
        let spec = record.kind.spec();
        for (field, value) in &record.payload {
            ensure!(
                spec.columns.split(',').any(|column| column == field)
                    || matches!(field.as_str(), "id" | "rowid"),
                "INVALID_ARGUMENT: unsupported {} column {field}",
                spec.table
            );
            if matches!(field.as_str(), "id" | "rowid") {
                continue;
            }
            if !value.is_null() {
                ensure!(
                    if numeric(field) {
                        if field == "confidence" {
                            value.is_number()
                        } else {
                            value.as_i64().is_some()
                        }
                    } else if boolean(field) {
                        value.is_boolean() || matches!(value.as_i64(), Some(0 | 1))
                    } else {
                        value.is_string()
                    },
                    "INVALID_ARGUMENT: invalid type for {field}"
                );
            }
        }
        for required in spec.required.split(',') {
            ensure!(
                record
                    .payload
                    .get(required)
                    .is_some_and(|value| !value.is_null()),
                "INVALID_ARGUMENT: {required} is required"
            );
        }
        let session_field = if record.kind == EvidenceKind::Relationship {
            "parent_session_id"
        } else {
            "session_id"
        };
        ensure!(
            record.payload.get("source").and_then(Value::as_str) == Some(&key.source)
                && record.payload.get(session_field).and_then(Value::as_str)
                    == Some(&key.session_id),
            "INVALID_ARGUMENT: evidence belongs to another source or session"
        );
        for field in spec.key.split(',').filter(|field| !numeric(field)) {
            ensure!(
                record
                    .payload
                    .get(field)
                    .and_then(Value::as_str)
                    .is_some_and(|value| !value.is_empty()),
                "INVALID_ARGUMENT: empty evidence identity {field}"
            );
        }
        if record.kind == EvidenceKind::SessionEvent {
            ensure!(
                ["user", "assistant", "tool_result"]
                    .contains(&record.payload["role"].as_str().unwrap_or_default())
                    && ["text", "thinking", "tool_use", "tool_result"]
                        .contains(&record.payload["kind"].as_str().unwrap_or_default()),
                "INVALID_ARGUMENT: invalid event role or kind"
            );
            let tool_result = record.payload["kind"].as_str() == Some("tool_result");
            for field in TOOL_RESULT_FIELDS {
                let Some(value) = record.payload.get(*field).filter(|v| !v.is_null()) else {
                    continue;
                };
                // The contract says these are null on anything that is not a
                // tool result. An adapter that fills them anyway is describing
                // a row it does not understand.
                ensure!(
                    tool_result,
                    "INVALID_ARGUMENT: {field} is only valid on a tool_result event"
                );
                if NON_NEGATIVE_FIELDS.contains(field) {
                    ensure!(
                        value.as_i64().is_some_and(|n| n >= 0),
                        "INVALID_ARGUMENT: {field} must not be negative"
                    );
                }
                let vocabulary = match *field {
                    "result_status" => Some(RESULT_STATUSES),
                    "event_source" => Some(EVENT_SOURCES),
                    "error_signal" => Some(ERROR_SIGNALS),
                    _ => None,
                };
                if let Some(allowed) = vocabulary {
                    ensure!(
                        value
                            .as_str()
                            .is_some_and(|value| allowed.contains(&value)),
                        "INVALID_ARGUMENT: unsupported {field} value"
                    );
                }
            }
        }
        if record.kind == EvidenceKind::Relationship {
            let status = record.payload["identity_status"]
                .as_str()
                .unwrap_or_default();
            let child = record
                .payload
                .get("child_session_id")
                .and_then(Value::as_str);
            ensure!(
                (status == "observed" && child.is_some_and(|id| !id.is_empty()))
                    || (status == "unlinked" && child.is_none()),
                "INVALID_ARGUMENT: inconsistent relationship child identity"
            );
        }
        if record.kind == EvidenceKind::History {
            record.payload.insert(
                "prompt_hash".into(),
                Value::String(crate::prompt_hash(
                    record.payload["prompt"].as_str().unwrap_or_default(),
                )),
            );
        }
        for column in spec.columns.split(',') {
            record.payload.entry(column.to_string()).or_insert_with(|| {
                if column == "child_has_events" {
                    json!(0)
                } else {
                    Value::Null
                }
            });
        }
        ensure!(
            identities.insert(record.identity()),
            "INVALID_ARGUMENT: duplicate canonical record in source snapshot"
        );
    }
    Ok(())
}
fn sql_value(value: &Value) -> rusqlite::types::Value {
    match value {
        Value::Null => rusqlite::types::Value::Null,
        Value::Bool(value) => rusqlite::types::Value::Integer(i64::from(*value)),
        Value::Number(value) => value
            .as_i64()
            .map(rusqlite::types::Value::Integer)
            .unwrap_or_else(|| rusqlite::types::Value::Real(value.as_f64().unwrap())),
        Value::String(value) => rusqlite::types::Value::Text(value.clone()),
        _ => unreachable!("validated evidence contains only SQLite scalars"),
    }
}
impl EvidenceRecord {
    pub fn identity(&self) -> String {
        serde_json::to_string(&json!([
            self.kind,
            self.kind
                .spec()
                .key
                .split(',')
                .map(|field| self.payload.get(field).cloned().unwrap_or(Value::Null))
                .collect::<Vec<_>>()
        ]))
        .expect("JSON evidence identity")
    }
    fn key_sql(&self) -> (String, Vec<rusqlite::types::Value>) {
        let columns = self.kind.spec().key.split(',').collect::<Vec<_>>();
        (
            columns
                .iter()
                .map(|column| format!("{column}=?"))
                .collect::<Vec<_>>()
                .join(" AND "),
            columns
                .iter()
                .map(|column| sql_value(&self.payload[*column]))
                .collect(),
        )
    }
    /// Whether the canonical row still equals this adapter-owned projection.
    /// Local ingestion can write directly; a changed value revokes remote ownership.
    pub fn matches_canonical(&self, conn: &Connection) -> Result<bool> {
        let spec = self.kind.spec();
        let mut clauses = vec![];
        let mut values = vec![];
        for column in spec.columns.split(',') {
            clauses.push(format!("{column} IS ?"));
            values.push(sql_value(self.payload.get(column).unwrap_or(&Value::Null)));
        }
        Ok(conn.query_row(
            &format!(
                "SELECT EXISTS(SELECT 1 FROM {} WHERE {})",
                spec.table,
                clauses.join(" AND ")
            ),
            params_from_iter(values),
            |row| row.get(0),
        )?)
    }
    pub fn exists(&self, conn: &Connection) -> Result<bool> {
        let (condition, values) = self.key_sql();
        Ok(conn.query_row(
            &format!(
                "SELECT EXISTS(SELECT 1 FROM {} WHERE {condition})",
                self.kind.spec().table
            ),
            params_from_iter(values),
            |row| row.get(0),
        )?)
    }
    pub fn remove(&self, conn: &Connection) -> Result<()> {
        let (condition, values) = self.key_sql();
        conn.execute(
            &format!("DELETE FROM {} WHERE {condition}", self.kind.spec().table),
            params_from_iter(values),
        )?;
        Ok(())
    }
    pub fn write(&self, conn: &Connection) -> Result<()> {
        let spec = self.kind.spec();
        let columns = spec.columns.split(',').collect::<Vec<_>>();
        let values = columns
            .iter()
            .map(|column| sql_value(self.payload.get(*column).unwrap_or(&Value::Null)))
            .collect::<Vec<_>>();
        let updates = columns
            .iter()
            .filter(|column| !spec.key.split(',').any(|key| key == **column))
            .map(|column| format!("{column}=excluded.{column}"))
            .collect::<Vec<_>>()
            .join(",");
        conn.execute(
            &format!(
                "INSERT INTO {}({}) VALUES({}) ON CONFLICT({}) DO UPDATE SET {updates}",
                spec.table,
                spec.columns,
                vec!["?"; columns.len()].join(","),
                spec.key
            ),
            params_from_iter(values),
        )
        .context("persisting normalized source evidence")?;
        Ok(())
    }
}

/// Snapshot parser output through the same transport-neutral row contract.
pub fn read_session(
    conn: &Connection,
    source: &str,
    session: &str,
    kinds: &[EvidenceKind],
) -> Result<Vec<EvidenceRecord>> {
    let mut records = vec![];
    for kind in kinds {
        let spec = kind.spec();
        let columns = spec.columns.split(',').collect::<Vec<_>>();
        let session_field = if *kind == EvidenceKind::Relationship {
            "parent_session_id"
        } else {
            "session_id"
        };
        let mut query = conn.prepare(&format!(
            "SELECT {} FROM {} WHERE source=? AND {session_field}=? ORDER BY {}",
            spec.columns, spec.table, spec.key
        ))?;
        let rows = query.query_map([source, session], |row| {
            let mut payload = Map::new();
            for (index, column) in columns.iter().enumerate() {
                let value = match row.get_ref(index)? {
                    rusqlite::types::ValueRef::Null => Value::Null,
                    rusqlite::types::ValueRef::Integer(value) => json!(value),
                    rusqlite::types::ValueRef::Real(value) => json!(value),
                    rusqlite::types::ValueRef::Text(value) => {
                        Value::String(String::from_utf8_lossy(value).into_owned())
                    }
                    rusqlite::types::ValueRef::Blob(_) => {
                        return Err(rusqlite::Error::InvalidColumnType(
                            index,
                            column.to_string(),
                            rusqlite::types::Type::Blob,
                        ))
                    }
                };
                payload.insert(column.to_string(), value);
            }
            Ok(EvidenceRecord {
                kind: *kind,
                payload,
                record_id: None,
                revision_id: None,
            })
        })?;
        records.extend(rows.collect::<rusqlite::Result<Vec<_>>>()?);
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SessionLocation;

    fn key() -> ObservationKey {
        ObservationKey {
            source: "claude".into(),
            session_id: "s1".into(),
            location: SessionLocation::Remote,
            connector_id: "c".into(),
            connector_instance: "i".into(),
        }
    }

    /// A tool-result event with the fidelity fields a plugin may legitimately
    /// submit, so each rejection test below changes exactly one thing.
    fn tool_result_event() -> EvidenceRecord {
        EvidenceRecord {
            kind: EvidenceKind::SessionEvent,
            payload: json!({
                "source": "claude",
                "session_id": "s1",
                "ts_ms": 1,
                "role": "tool_result",
                "kind": "tool_result",
                "event_uid": "e1",
                "tool_use_id": "toolu_1",
                "payload_bytes": 12,
                "payload_truncated": false,
                "payload_hash": "0123456789abcdef",
                "call_index": 0,
                "event_index": 0,
                "result_status": "completed",
                "event_source": "tool_result",
            })
            .as_object()
            .unwrap()
            .clone(),
        record_id: None,
            revision_id: None,
        }
    }

    fn validate(record: EvidenceRecord) -> Result<()> {
        validate_records(&key(), &[EvidenceKind::SessionEvent], &mut [record])
    }

    fn with(field: &str, value: Value) -> EvidenceRecord {
        let mut record = tool_result_event();
        record.payload.insert(field.to_string(), value);
        record
    }

    #[test]
    fn a_well_formed_tool_result_event_is_accepted() {
        validate(tool_result_event()).expect("the documented shape must pass");
    }

    #[test]
    fn negative_fidelity_counts_are_rejected() {
        // A byte count of -1 is not a small payload. Accepted here it would
        // sort first under `--rank-by bytes`, which is the one place the value
        // is load-bearing.
        for field in ["payload_bytes", "call_index", "event_index"] {
            let error = validate(with(field, json!(-1)))
                .expect_err("a negative {field} must be rejected");
            assert!(
                error.to_string().contains("must not be negative"),
                "{field}: {error}"
            );
        }
    }

    #[test]
    fn values_outside_the_documented_vocabularies_are_rejected() {
        // The SDK types these as closed unions and casts without checking, so
        // a plausible synonym would reach consumers looking exactly like a
        // value they were told to expect.
        for (field, value) in [
            ("result_status", "successful"),
            ("event_source", "tool_output"),
            ("error_signal", "nonzero_exit"),
        ] {
            let error = validate(with(field, json!(value)))
                .expect_err("an undocumented {field} must be rejected");
            assert!(
                error.to_string().contains("unsupported"),
                "{field}: {error}"
            );
        }
    }

    #[test]
    fn fidelity_fields_are_rejected_on_a_row_that_is_not_a_tool_result() {
        let mut record = tool_result_event();
        record.payload.insert("role".into(), json!("assistant"));
        record.payload.insert("kind".into(), json!("text"));
        let error = validate(record).expect_err("fidelity on a text row must be rejected");
        assert!(
            error.to_string().contains("only valid on a tool_result event"),
            "{error}"
        );
    }

    #[test]
    fn an_explicit_null_fidelity_field_stays_legal_everywhere() {
        // Absence is how a provider says it does not record the fact, so a
        // null must never be the thing that fails a snapshot.
        let mut record = tool_result_event();
        record.payload.insert("role".into(), json!("assistant"));
        record.payload.insert("kind".into(), json!("text"));
        for field in TOOL_RESULT_FIELDS {
            record.payload.insert((*field).to_string(), Value::Null);
        }
        validate(record).expect("null fidelity fields are legal on any row");
    }
}
