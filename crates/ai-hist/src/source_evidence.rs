//! Validated canonical evidence records accepted from installed source adapters.
//! Foreign database row ids are retained in observations but never assigned locally.
#[cfg(any(test, feature = "unstable-internal"))]
use crate::observations::ObservationKey;
#[cfg(feature = "unstable-internal")]
use anyhow::Context;
#[cfg(any(test, feature = "unstable-internal"))]
use anyhow::{ensure, Result};
#[cfg(feature = "unstable-internal")]
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
impl EvidenceKind {
    /// The wire name, identical to this enum's serde representation. Callers
    /// that name a kind in a diagnostic or a JSON contract use this rather
    /// than `Debug`, which prints the Rust variant.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::History => "history",
            Self::SessionEvent => "session_event",
            Self::ToolCall => "tool_call",
            Self::FileEdit => "file_edit",
            Self::Relationship => "relationship",
            Self::CommitLink => "commit_link",
            Self::SessionMarker => "session_marker",
        }
    }
}

/// Render a kind list the way diagnostics and docs name it.
pub fn join_kinds(kinds: &[EvidenceKind]) -> String {
    kinds
        .iter()
        .map(|kind| kind.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(any(test, feature = "unstable-internal"))]
struct Spec {
    table: &'static str,
    columns: &'static str,
    required: &'static str,
}
impl EvidenceKind {
    /// The comma-separated columns that identify one record of this kind.
    fn key(self) -> &'static str {
        match self {
            Self::History => "source,timestamp_ms,prompt",
            Self::SessionEvent => "source,session_id,event_uid",
            Self::ToolCall => "source,session_id,tool_use_id",
            Self::FileEdit => "source,session_id,tool_use_id",
            Self::Relationship => "source,parent_session_id,relationship_uid",
            Self::CommitLink => "source,session_id,commit_sha,match_method",
            Self::SessionMarker => "source,session_id,marker_uid",
        }
    }

    /// Columns that travel with the record but are not the adapter's to
    /// vouch for: canonical state this database derives for itself.
    ///
    /// They are written back into the row *inside* the same transaction that
    /// stores the adapter's snapshot, so comparing them against the snapshot
    /// asks whether this database edited its own derived field — to which the
    /// answer is always yes, and the consequence is that the connector is
    /// protected against its own record. Ownership is about the fields the
    /// adapter reports, so only those are compared.
    #[cfg(feature = "unstable-internal")]
    fn derived(self) -> &'static str {
        match self {
            Self::SessionEvent => "project_key,project_key_method",
            Self::History
            | Self::ToolCall
            | Self::FileEdit
            | Self::Relationship
            | Self::CommitLink
            | Self::SessionMarker => "",
        }
    }

    #[cfg(any(test, feature = "unstable-internal"))]
    fn spec(self) -> Spec {
        match self {
        Self::History=>Spec{table:"history",columns:"source,session_id,project,prompt,prompt_hash,timestamp_ms,git_branch",required:"source,session_id,prompt,timestamp_ms"},
        // `project_key` travels with the event so a snapshot round-trips the
        // canonical identity the emitting side resolved, and `project_key_method`
        // with it so the receiving side can tell a key the emitter resolved for
        // itself from one it was lent. A key that arrives without a method
        // ranks below every stated one, so an older adapter's events are
        // improved by the first pass that knows better rather than defended as
        // if the emitter had vouched for them. Neither is trusted as final:
        // `refresh_project_identity`'s denormalization pass brings every event
        // back in line with its own session's key.
        //
        // The per-message raw facts travel too, and are the adapter's to vouch
        // for: they are what the provider wrote on the envelope, so a
        // connector that read the transcript can report them. `raw_facts_version`
        // is deliberately absent -- it records which local parser generation
        // wrote a row, which is this database's bookkeeping and not something a
        // remote emitter can speak to.
        // `raw_kind` is part of the projection, not just the table: the row
        // contract is what `read_session` reads back and what a snapshot is
        // compared against, and a column missing here is rejected outright as
        // an unsupported column when it appears in a payload. It stays out of
        // `required` because it is optional for every source. `control_kind`
        // travels for the same reason: a connector that classified a row is
        // reporting a fact about it, and a snapshot without the column would
        // read every control row back as a prompt.
        Self::SessionEvent=>Spec{table:"session_events",columns:"source,session_id,project,project_key,project_key_method,cwd,git_branch,message_id,parent_id,ts_ms,role,kind,text,model,token_json,provider,event_uid,tool_use_id,payload_bytes,payload_truncated,payload_hash,call_index,event_index,result_status,event_source,error_signal,subagent_session_id,agent_id,request_id,provider_message_id,stop_reason,agent_version,is_sidechain,is_meta,turn_id,request_span,raw_kind,control_kind",required:"source,session_id,ts_ms,role,kind,event_uid"},
        Self::ToolCall=>Spec{table:"tool_calls",columns:"source,session_id,message_id,tool_use_id,name,target,args_json,is_error,ts_ms",required:"source,session_id,tool_use_id,name"},
        Self::FileEdit=>Spec{table:"file_edits",columns:"source,session_id,message_id,tool_use_id,file_path,tool_name,lines_added,lines_removed,structured_patch_json,user_modified,ts_ms,git_branch,cwd",required:"source,session_id,tool_use_id,file_path,tool_name"},
        Self::Relationship=>Spec{table:"session_relationships",columns:"source,parent_session_id,relationship_uid,child_session_id,relationship,identity_status,child_agent_type,child_agent_name,child_model,spawn_depth,evidence_kind,evidence_locator,evidence_ref,child_has_events,spawned_at_ms,created_ms,updated_ms,origin_session_id",required:"source,parent_session_id,relationship_uid,relationship,identity_status,evidence_kind,created_ms,updated_ms"},
        Self::CommitLink=>Spec{table:"session_commit_links",columns:"source,session_id,repo,branch,commit_sha,note_ref,match_method,confidence,files_json,numstat_json,evidence_json,created_at_ms",required:"source,session_id,repo,commit_sha,match_method,confidence,created_at_ms"},
        // The `kind` column here is the marker's own classification, not the
        // evidence kind. It is required because a marker without one is the
        // unclassified row this table exists to keep.
        Self::SessionMarker=>Spec{table:"session_markers",columns:"source,session_id,marker_uid,ts_ms,message_id,parent_id,turn_id,kind,subkind,text,payload_json",required:"source,session_id,marker_uid,kind"},
    }
    }
}
#[cfg(any(test, feature = "unstable-internal"))]
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
#[cfg(any(test, feature = "unstable-internal"))]
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
#[cfg(any(test, feature = "unstable-internal"))]
const RESULT_STATUSES: &[&str] = &["running", "completed", "errored", "cancelled", "unknown"];
#[cfg(any(test, feature = "unstable-internal"))]
const EVENT_SOURCES: &[&str] = &[
    "tool_result",
    "subagent_notification",
    "function_call_output",
];
#[cfg(any(test, feature = "unstable-internal"))]
const ERROR_SIGNALS: &[&str] = &[
    "tool_result.is_error",
    "exit_code",
    "patch_apply",
    "mcp_err",
    "subagent_status",
];

/// The fidelity counts that cannot be negative. A byte count of `-1` is not a
/// small payload; it is a bug upstream, and ranking by it puts the row first.
#[cfg(any(test, feature = "unstable-internal"))]
const NON_NEGATIVE_FIELDS: &[&str] = &["payload_bytes", "call_index", "event_index"];

#[cfg(any(test, feature = "unstable-internal"))]
fn boolean(field: &str) -> bool {
    matches!(
        field,
        "is_error"
            | "user_modified"
            | "child_has_events"
            | "payload_truncated"
            | "is_sidechain"
            | "is_meta"
    )
}

/// Validate the complete response before opening a database or mutating history.
#[cfg(any(test, feature = "unstable-internal"))]
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
        for field in record.kind.key().split(',').filter(|field| !numeric(field)) {
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
            // `control_kind` is a closed vocabulary, and every reader -- the
            // user-turn page, prompt attribution, the plugin's turn export --
            // treats any non-null value as authoritative without rechecking
            // it. A spelling nobody classifies, or a kind on a row the
            // contract says can never carry one, would silently hide real
            // evidence, so it is refused here rather than stored.
            if let Some(value) = record.payload.get("control_kind").filter(|v| !v.is_null()) {
                ensure!(
                    value
                        .as_str()
                        .is_some_and(|value| crate::ingest::control::ControlKind::parse(value).is_some()),
                    "INVALID_ARGUMENT: unsupported control_kind value"
                );
                ensure!(
                    record.payload["role"].as_str() == Some("user")
                        && record.payload["kind"].as_str() == Some("text"),
                    "INVALID_ARGUMENT: control_kind is only valid on a user text event"
                );
            }
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
        if record.kind == EvidenceKind::SessionMarker {
            // `required` only asks whether the field is present and non-null,
            // so `""` passed it and the NOT NULL column stored it happily. A
            // marker whose classification is the empty string is
            // indistinguishable from one whose classifier failed, which is the
            // state this table exists to make impossible. `unknown` is the
            // answer for a record nothing recognises.
            ensure!(
                record
                    .payload
                    .get("kind")
                    .and_then(Value::as_str)
                    .is_some_and(|kind| !kind.trim().is_empty()),
                "INVALID_ARGUMENT: marker kind must not be empty"
            );
            if let Some(payload_json) = record
                .payload
                .get("payload_json")
                .and_then(Value::as_str)
            {
                // `payload_json` is a bounded projection -- every string at 128
                // characters, every container at 32 entries, recursively. The
                // parsers apply that bound; putting the column on this contract
                // handed a connector a way around it, in the one place the
                // bound exists to defend.
                //
                // Refused rather than silently bounded, because that is how
                // this boundary treats every other out-of-contract value. It
                // derives `prompt_hash` and fills absent columns with null, but
                // it never rewrites a value a connector supplied: doing so here
                // would store something the submitter did not send and cannot
                // reconcile its own copy against.
                let parsed: Value = serde_json::from_str(payload_json).map_err(|err| {
                    anyhow::anyhow!("INVALID_ARGUMENT: payload_json must be JSON: {err}")
                })?;
                ensure!(
                    crate::ingest::marker_payload_is_bounded(&parsed),
                    "INVALID_ARGUMENT: payload_json exceeds the marker payload bound"
                );
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
#[cfg(feature = "unstable-internal")]
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
                .key()
                .split(',')
                .map(|field| self.payload.get(field).cloned().unwrap_or(Value::Null))
                .collect::<Vec<_>>()
        ]))
        .expect("JSON evidence identity")
    }
    #[cfg(feature = "unstable-internal")]
    fn key_sql(&self) -> (String, Vec<rusqlite::types::Value>) {
        let columns = self.kind.key().split(',').collect::<Vec<_>>();
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
    ///
    /// Crate-private, like the three writes below: each takes a raw
    /// `rusqlite::Connection`, and an embedder on the default features never
    /// holds one. Exporting them put `rusqlite` in the crate's public API,
    /// which `crates/ai-hist/public-api.txt` now forbids.
    ///
    /// [`Spec::derived`] columns are left out of the comparison. They are
    /// rewritten by `refresh_project_identity` in the same transaction that
    /// stores the snapshot they are compared against, so including them makes
    /// every enriched record look externally edited — and an adapter whose own
    /// event was enriched then loses it: its updates stop landing and the
    /// records it drops are never deleted, while it goes on reporting into a
    /// row it no longer owns. The fields still travel in the record, because a
    /// snapshot should round-trip what the emitting side knew; they are simply
    /// not evidence about who owns the row.
    #[cfg(feature = "unstable-internal")]
    pub(crate) fn matches_canonical(&self, conn: &Connection) -> Result<bool> {
        let spec = self.kind.spec();
        let derived: Vec<&str> = self.kind.derived().split(',').filter(|c| !c.is_empty()).collect();
        let mut clauses = vec![];
        let mut values = vec![];
        for column in spec.columns.split(',').filter(|c| !derived.contains(c)) {
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
    #[cfg(feature = "unstable-internal")]
    pub(crate) fn exists(&self, conn: &Connection) -> Result<bool> {
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
    #[cfg(feature = "unstable-internal")]
    pub(crate) fn remove(&self, conn: &Connection) -> Result<()> {
        let (condition, values) = self.key_sql();
        conn.execute(
            &format!("DELETE FROM {} WHERE {condition}", self.kind.spec().table),
            params_from_iter(values),
        )?;
        Ok(())
    }
    #[cfg(feature = "unstable-internal")]
    pub(crate) fn write(&self, conn: &Connection) -> Result<()> {
        let spec = self.kind.spec();
        let columns = spec.columns.split(',').collect::<Vec<_>>();
        let values = columns
            .iter()
            .map(|column| sql_value(self.payload.get(*column).unwrap_or(&Value::Null)))
            .collect::<Vec<_>>();
        let updates = columns
            .iter()
            .filter(|column| !self.kind.key().split(',').any(|key| key == **column))
            .map(|column| format!("{column}=excluded.{column}"))
            .collect::<Vec<_>>()
            .join(",");
        conn.execute(
            &format!(
                "INSERT INTO {}({}) VALUES({}) ON CONFLICT({}) DO UPDATE SET {updates}",
                spec.table,
                spec.columns,
                vec!["?"; columns.len()].join(","),
                self.kind.key()
            ),
            params_from_iter(values),
        )
        .context("persisting normalized source evidence")?;
        Ok(())
    }
}

/// Snapshot parser output through the same transport-neutral row contract.
#[cfg(feature = "unstable-internal")]
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
            spec.columns, spec.table, kind.key()
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

    /// A user text event as a connector contributes it, with the one field
    /// each control-kind test below varies.
    fn user_text_event(control_kind: Value) -> EvidenceRecord {
        EvidenceRecord {
            kind: EvidenceKind::SessionEvent,
            payload: json!({
                "source": "claude",
                "session_id": "s1",
                "ts_ms": 1,
                "role": "user",
                "kind": "text",
                "text": "<task-notification>done</task-notification>",
                "event_uid": "e1",
                "control_kind": control_kind,
            })
            .as_object()
            .unwrap()
            .clone(),
            record_id: None,
            revision_id: None,
        }
    }

    #[test]
    fn a_control_kind_from_the_vocabulary_is_accepted_on_a_user_text_event() {
        validate(user_text_event(json!("task_notification")))
            .expect("a documented control kind on a user text row is the contract");
        validate(user_text_event(Value::Null)).expect("null is a genuine prompt");
    }

    #[test]
    fn an_unknown_control_kind_spelling_is_rejected() {
        for value in [json!("meta_row"), json!("Meta"), json!("")] {
            let error = validate(user_text_event(value.clone()))
                .expect_err("a spelling no classifier produces must be refused");
            assert!(
                error.to_string().contains("unsupported control_kind value"),
                "{value}: {error}"
            );
        }
        // A non-string is refused by the column's type check before the
        // vocabulary is consulted; either way it never reaches the table.
        assert!(validate(user_text_event(json!(7))).is_err());
    }

    /// The example from review: an assistant text event with
    /// `control_kind: "meta"` passed validation and was then dropped from
    /// turn publishing and attribution as if it were the harness's.
    #[test]
    fn a_control_kind_on_an_assistant_or_tool_result_event_is_rejected() {
        let mut assistant = user_text_event(json!("meta"));
        assistant.payload.insert("role".into(), json!("assistant"));
        let error = validate(assistant).expect_err("assistant rows never carry a control kind");
        assert!(
            error.to_string().contains("only valid on a user text event"),
            "{error}"
        );

        let error = validate(with("control_kind", json!("codex_context_wrapper")))
            .expect_err("tool results never carry a control kind");
        assert!(
            error.to_string().contains("only valid on a user text event"),
            "{error}"
        );

        // A user row that is not text either: the classification is about
        // what the human's turn is, not about a tool result on it.
        let mut user_tool_result = user_text_event(json!("meta"));
        user_tool_result.payload.insert("kind".into(), json!("tool_result"));
        assert!(validate(user_tool_result).is_err());
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

    /// A diagnostic that names a kind and a JSON contract that serializes one
    /// must agree, or a consumer matching on the wire name silently misses it.
    #[test]
    fn kind_names_match_their_serde_representation() {
        for kind in [
            EvidenceKind::History,
            EvidenceKind::SessionEvent,
            EvidenceKind::ToolCall,
            EvidenceKind::FileEdit,
            EvidenceKind::Relationship,
            EvidenceKind::CommitLink,
            EvidenceKind::SessionMarker,
        ] {
            assert_eq!(serde_json::to_value(kind).unwrap(), json!(kind.as_str()));
        }
        assert_eq!(
            join_kinds(FULL_SESSION_KINDS),
            "history, session_event, tool_call, file_edit, relationship"
        );
    }

    /// Every column a canonical table has must appear in its evidence spec.
    ///
    /// `validate_records` rejects any payload field the spec does not list, and
    /// `read_session` builds payloads from the live table. So a column added to
    /// one of these tables but not to its spec turns every normalized snapshot
    /// of that table into `INVALID_ARGUMENT` -- and it surfaces in remote
    /// intake, a long way from the migration that caused it. Asserting the two
    /// agree here puts the failure next to the change that causes it.
    ///
    /// The one exemption is named, not a pattern: a column left out of a
    /// projection has to say why, because the failure it otherwise causes is
    /// invisible until a remote snapshot hits it.
    #[test]
    fn every_canonical_column_is_part_of_its_evidence_spec() {
        // Columns deliberately outside a projection, each with its reason.
        //
        // `raw_facts_version` is the local parser's own generation stamp, not
        // a provider fact. Putting it in the projection would let a snapshot
        // an installed adapter contributed claim a parser generation that
        // never ran over it, and the backfill probes that read the column
        // would then skip exactly the rows they exist to repair.
        //
        // `revision` is the change feed's stamp: the position of a row's
        // last write in this database's own clock. It is set by trigger on
        // every write and means nothing outside the database it was written
        // in, so an adapter can neither supply it nor be held to it.
        const LOCAL_ONLY: &[(&str, &str)] = &[
            ("session_events", "raw_facts_version"),
            ("session_events", "revision"),
            ("tool_calls", "revision"),
            ("file_edits", "revision"),
            ("session_relationships", "revision"),
            ("session_markers", "revision"),
        ];
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        crate::init_db(&conn).unwrap();
        // An exemption for a column that is in fact projected would sit here
        // forever hiding the next real drift, so each one is checked to still
        // be needed.
        for (table, column) in LOCAL_ONLY {
            let projected = [
                EvidenceKind::History,
                EvidenceKind::SessionEvent,
                EvidenceKind::ToolCall,
                EvidenceKind::FileEdit,
                EvidenceKind::Relationship,
                EvidenceKind::CommitLink,
                EvidenceKind::SessionMarker,
            ]
            .into_iter()
            .map(EvidenceKind::spec)
            .filter(|spec| spec.table == *table)
            .any(|spec| spec.columns.split(',').any(|name| name == *column));
            assert!(
                !projected,
                "{table}.{column} is projected after all -- drop its exemption"
            );
        }
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
            let declared: std::collections::HashSet<&str> = spec.columns.split(',').collect();
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
                .filter(|name| !LOCAL_ONLY.contains(&(spec.table, name.as_str())))
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
