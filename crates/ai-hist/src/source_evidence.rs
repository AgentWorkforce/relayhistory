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
    SessionMarker,
}
/// What a remote connector must supply for its snapshot to count as complete.
///
/// Deliberately does NOT include [`EvidenceKind::SessionMarker`]. A marker is
/// derived by this crate's parser, not something a source plugin can produce,
/// so requiring one here would silently demote every third-party connector
/// from `full` to partial. The round trip through our own parser uses
/// [`PARSED_SESSION_KINDS`] instead.
pub const FULL_SESSION_KINDS: &[EvidenceKind] = &[
    EvidenceKind::History,
    EvidenceKind::SessionEvent,
    EvidenceKind::ToolCall,
    EvidenceKind::FileEdit,
    EvidenceKind::Relationship,
];

/// Everything this crate's own parser writes for one session.
///
/// A remote `ClaudeFull` snapshot is parsed into a temporary database and then
/// projected back out through a kind list; whatever that list omits is written
/// during normalization and thrown away before anything durable sees it. That
/// is exactly how remote hydration came to drop every marker. The projection
/// list is therefore kept separate from the connector-capability list above,
/// and it is the one that has to name every table the parser touches.
pub const PARSED_SESSION_KINDS: &[EvidenceKind] = &[
    EvidenceKind::History,
    EvidenceKind::SessionEvent,
    EvidenceKind::ToolCall,
    EvidenceKind::FileEdit,
    EvidenceKind::Relationship,
    EvidenceKind::SessionMarker,
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
        // `raw_kind` is part of the projection, not just the table: the row
        // contract is what `read_session` reads back and what a snapshot is
        // compared against, and a column missing here is rejected outright as
        // an unsupported column when it appears in a payload. It stays out of
        // `required` because it is optional for every source.
        Self::SessionEvent=>Spec{table:"session_events",columns:"source,session_id,project,cwd,git_branch,message_id,parent_id,ts_ms,role,kind,text,model,token_json,event_uid,raw_kind",required:"source,session_id,ts_ms,role,kind,event_uid",key:"source,session_id,event_uid"},
        Self::ToolCall=>Spec{table:"tool_calls",columns:"source,session_id,message_id,tool_use_id,name,target,args_json,is_error,ts_ms",required:"source,session_id,tool_use_id,name",key:"source,session_id,tool_use_id"},
        Self::FileEdit=>Spec{table:"file_edits",columns:"source,session_id,message_id,tool_use_id,file_path,tool_name,lines_added,lines_removed,structured_patch_json,user_modified,ts_ms,git_branch,cwd",required:"source,session_id,tool_use_id,file_path,tool_name",key:"source,session_id,tool_use_id"},
        Self::Relationship=>Spec{table:"session_relationships",columns:"source,parent_session_id,relationship_uid,child_session_id,relationship,identity_status,child_agent_type,child_agent_name,child_model,spawn_depth,evidence_kind,evidence_locator,evidence_ref,child_has_events,spawned_at_ms,created_ms,updated_ms",required:"source,parent_session_id,relationship_uid,relationship,identity_status,evidence_kind,created_ms,updated_ms",key:"source,parent_session_id,relationship_uid"},
        Self::CommitLink=>Spec{table:"session_commit_links",columns:"source,session_id,repo,branch,commit_sha,note_ref,match_method,confidence,files_json,numstat_json,evidence_json,created_at_ms",required:"source,session_id,repo,commit_sha,match_method,confidence,created_at_ms",key:"source,session_id,commit_sha,match_method"},
        // The `kind` column here is the marker's own classification, not the
        // evidence kind. It is required because a marker without one is the
        // unclassified row this table exists to keep.
        Self::SessionMarker=>Spec{table:"session_markers",columns:"source,session_id,marker_uid,ts_ms,message_id,parent_id,turn_id,kind,subkind,payload_json",required:"source,session_id,marker_uid,kind",key:"source,session_id,marker_uid"},
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
    )
}
fn boolean(field: &str) -> bool {
    matches!(field, "is_error" | "user_modified" | "child_has_events")
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
    use rusqlite::Connection;
    use std::collections::HashSet;

    /// Every column a canonical table has must appear in its evidence spec.
    ///
    /// `validate_records` rejects any payload field the spec does not list, and
    /// `read_session` builds payloads from the live table. So a column added to
    /// one of these tables but not to its spec turns every normalized snapshot
    /// of that table into `INVALID_ARGUMENT` -- and it surfaces in remote
    /// intake, a long way from the migration that caused it. Asserting the two
    /// agree here puts the failure next to the change that causes it.
    #[test]
    fn every_canonical_column_is_part_of_its_evidence_spec() {
        let conn = Connection::open_in_memory().unwrap();
        crate::init_db(&conn).unwrap();
        for kind in [
            EvidenceKind::History,
            EvidenceKind::SessionEvent,
            EvidenceKind::ToolCall,
            EvidenceKind::FileEdit,
            EvidenceKind::Relationship,
            EvidenceKind::CommitLink,
            EvidenceKind::SessionMarker,
        ] {
            let spec = kind.spec();
            let declared: HashSet<&str> = spec.columns.split(',').collect();
            let actual: Vec<String> = conn
                .prepare(&format!(
                    "SELECT name FROM pragma_table_info('{}')",
                    spec.table
                ))
                .unwrap()
                .query_map([], |row| row.get(0))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
            assert!(
                !actual.is_empty(),
                "{} has no columns -- the probe read nothing, so it proves nothing",
                spec.table
            );
            let missing: Vec<&String> = actual
                .iter()
                // `id` / `rowid` are surrogate keys `validate_records` accepts
                // and ignores; they are deliberately not part of a projection.
                .filter(|name| !matches!(name.as_str(), "id" | "rowid"))
                .filter(|name| !declared.contains(name.as_str()))
                .collect();
            assert!(
                missing.is_empty(),
                "{} columns are missing from the {kind:?} evidence spec: {missing:?}",
                spec.table
            );
        }
    }
}
