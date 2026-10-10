use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};

use super::parse::{string_array, ts_map};
use super::ContinuityEvidence;
use crate::relationship_capture::{record_relationship, ObservedRelationship};

// ---------------------------------------------------------------------------
// Storage helpers
// ---------------------------------------------------------------------------

/// One lineage edge a resolution step derived from a transcript's evidence.
pub(super) struct ContinuityEdge<'a> {
    pub(super) relationship: &'a str,
    pub(super) parent_session_id: &'a str,
    pub(super) child_session_id: Option<&'a str>,
    pub(super) evidence_ref: &'a str,
    pub(super) origin_session_id: Option<&'a str>,
    pub(super) spawned_at_ms: Option<i64>,
}

pub(super) fn write_edge(
    conn: &Connection,
    evidence: &ContinuityEvidence,
    edge: &ContinuityEdge<'_>,
) -> Result<(String, String)> {
    let &ContinuityEdge {
        relationship,
        parent_session_id,
        child_session_id,
        evidence_ref,
        origin_session_id,
        spawned_at_ms,
    } = edge;
    // Unlinked branches of one origin must not collapse into a single row, so
    // their uid carries the transcript that distinguishes them.
    let uid = match child_session_id {
        Some(child) => format!("{relationship}:{child}"),
        None => format!("{relationship}:{}", evidence.branch_label()),
    };
    let child_has_events = match child_session_id {
        Some(child) => session_has_events(conn, &evidence.source, child)?,
        None => false,
    };
    record_relationship(
        conn,
        &ObservedRelationship {
            source: &evidence.source,
            parent_session_id,
            child_session_id,
            relationship,
            evidence_kind: evidence_kind(&evidence.source),
            evidence_locator: Some(&evidence.locator),
            evidence_ref: Some(evidence_ref),
            child_has_events,
            spawned_at_ms,
            origin_session_id,
            relationship_uid: Some(&uid),
            ..ObservedRelationship::default()
        },
    )?;
    Ok((parent_session_id.to_string(), uid))
}

fn evidence_kind(source: &str) -> &'static str {
    match source {
        "codex" => "codex_session_meta_continuity",
        _ => "claude_transcript_continuity",
    }
}

pub(super) fn load_pending(conn: &Connection, source: &str) -> Result<Vec<ContinuityEvidence>> {
    Ok(conn
        .prepare(
            "SELECT source, locator, session_id, file_session_id, first_parent_uuid, \
                    first_ts_ms, in_log_session_ids_json, has_resume_marker, resume_target, \
                    explicit_targets_json, source_version \
             FROM session_continuity_evidence \
             WHERE source = ? AND pending_reason IS NOT NULL \
             ORDER BY locator ASC",
        )?
        .query_map(params![source], map_evidence)?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}

/// Every transcript claiming one origin, pending or already resolved.
pub(super) fn fork_group(
    conn: &Connection,
    source: &str,
    origin: &str,
) -> Result<Vec<ContinuityEvidence>> {
    Ok(conn
        .prepare(
            "SELECT source, locator, session_id, file_session_id, first_parent_uuid, \
                    first_ts_ms, in_log_session_ids_json, has_resume_marker, resume_target, \
                    explicit_targets_json, source_version \
             FROM session_continuity_evidence \
             WHERE source = ? AND origin_session_id = ? \
             ORDER BY locator ASC",
        )?
        .query_map(params![source, origin], map_evidence)?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}

pub(super) fn map_evidence(row: &rusqlite::Row<'_>) -> rusqlite::Result<ContinuityEvidence> {
    let in_log: String = row.get(6)?;
    let targets: String = row.get(9)?;
    let targets: Value = serde_json::from_str(&targets).unwrap_or_else(|_| json!({}));
    Ok(ContinuityEvidence {
        source: row.get(0)?,
        locator: row.get(1)?,
        session_id: row.get(2)?,
        file_session_id: row.get(3)?,
        first_parent_uuid: row.get(4)?,
        first_ts_ms: row.get(5)?,
        in_log_session_ids: serde_json::from_str(&in_log).unwrap_or_default(),
        has_resume_marker: row.get(7)?,
        resume_target: row.get(8)?,
        explicit_continuation_targets: string_array(&targets, "continuation"),
        explicit_fork_targets: string_array(&targets, "fork"),
        explicit_fork_refs: targets
            .get("fork_refs")
            .and_then(Value::as_object)
            .map(|refs| {
                refs.iter()
                    .filter_map(|(target, field)| {
                        Some((target.clone(), field.as_str()?.to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default(),
        explicit_continuation_ts_ms: ts_map(&targets, "continuation_ts_ms"),
        explicit_fork_ts_ms: ts_map(&targets, "fork_ts_ms"),
        explicit_source_session_id: targets
            .get("source")
            .and_then(Value::as_str)
            .map(str::to_string),
        source_version: row.get(10)?,
    })
}

/// The session holding the record with this provider uuid.
///
/// `message_id` is the record's own uuid; `event_uid` appends a block index to
/// it. Both are checked so a record whose uuid only ever reached the uid still
/// resolves, and the answer is the lowest session id either names.
///
/// Asked as two keyed searches rather than one `message_id = ? OR event_uid =
/// ?` predicate. The OR fits neither index, so SQLite answered it by walking
/// every event of the source — once per pending transcript, on every sweep,
/// for as long as the transcript stays pending, which for a parent uuid
/// nothing has indexed is for ever (#215). The uid half only has to find what
/// the message half cannot: a row whose uid is `<uuid>:0` but whose
/// `message_id` is not that uuid. That is what
/// `idx_session_events_claude_uid_unmatched` holds, so on a store where every
/// row's uid extends its own message id the index is empty and costs no write.
pub(super) fn session_holding_record(
    conn: &Connection,
    source: &str,
    record_uuid: &str,
) -> Result<Option<String>> {
    let lowest = |sql: &str| -> Result<Option<String>> {
        Ok(conn
            .prepare_cached(sql)?
            .query_row(params![source, record_uuid], |row| row.get::<_, String>(0))
            .optional()?)
    };
    let by_message = lowest(SESSION_HOLDING_MESSAGE_SQL)?;
    // Only Claude records a parent uuid today, and the partial index is
    // Claude's; another source keeps the same answer through the unindexed
    // form rather than a different one.
    let by_uid = if source == "claude" {
        lowest(&session_holding_claude_uid_sql())?
    } else {
        lowest(SESSION_HOLDING_UID_SQL)?
    };
    // `min` over `String` is a byte-wise comparison, which is the `BINARY`
    // collation the single query ordered by.
    Ok(match (by_message, by_uid) {
        (Some(message), Some(uid)) => Some(message.min(uid)),
        (message, uid) => message.or(uid),
    })
}

/// The message half of [`session_holding_record`], a search on
/// `idx_session_events_message`. `+session_id` keeps the planner from
/// walking `idx_session_events_session` in session order to satisfy the
/// `ORDER BY`, which without statistics it prefers and which visits every
/// event of the source until one matches.
pub(crate) const SESSION_HOLDING_MESSAGE_SQL: &str = "SELECT session_id FROM session_events \
     WHERE source = ?1 AND message_id = ?2 ORDER BY +session_id ASC LIMIT 1";

/// The rows `idx_session_events_claude_uid_unmatched` covers: a Claude
/// block-0 uid that does not extend the row's own message id, which is the
/// only shape the message half of [`session_holding_record`] cannot see.
/// This one spelling builds both the index's `WHERE` (in `store::init_db`)
/// and the lookup below, because SQLite proves a partial index applies by
/// matching the query's terms against the index's, and a drift between the
/// two would silently turn the lookup back into a scan of every Claude
/// event per transcript per sweep.
pub(crate) const CLAUDE_UID_UNMATCHED_PREDICATE: &str = "source = 'claude' \
     AND substr(event_uid, -2) = ':0' \
     AND (message_id IS NULL OR event_uid <> message_id || ':0')";

/// The uid half for Claude: only rows the message half cannot see, spelled
/// with exactly the terms of `idx_session_events_claude_uid_unmatched`'s
/// `WHERE` ([`CLAUDE_UID_UNMATCHED_PREDICATE`]) so SQLite can prove the
/// partial index applies. `?1` is left unreferenced so both halves bind the
/// same parameters.
pub(crate) fn session_holding_claude_uid_sql() -> String {
    format!(
        "SELECT session_id FROM session_events \
         WHERE {CLAUDE_UID_UNMATCHED_PREDICATE} AND event_uid = ?2 || ':0' \
         ORDER BY +session_id ASC LIMIT 1"
    )
}

/// The uid half for any other source. No source but Claude records a parent
/// uuid, so this is not reached today; it keeps the lookup's meaning if one
/// ever does.
const SESSION_HOLDING_UID_SQL: &str = "SELECT session_id FROM session_events \
     WHERE source = ?1 AND event_uid = ?2 || ':0' ORDER BY +session_id ASC LIMIT 1";

fn session_has_events(conn: &Connection, source: &str, session_id: &str) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM session_events WHERE source = ? AND session_id = ? LIMIT 1)",
        params![source, session_id],
        |row| row.get(0),
    )?)
}

pub(super) fn table_exists(conn: &Connection, name: &str) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?)",
        [name],
        |row| row.get(0),
    )?)
}
