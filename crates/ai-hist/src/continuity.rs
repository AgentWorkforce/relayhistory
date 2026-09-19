//! Fork, resume and continuation relationships, reconciled across transcripts.
//!
//! Delegation is observable inside a single transcript: the record that spawns
//! a child names it. Continuity is not. A resumed, forked or continued session
//! is a *different file*, and the only thing tying it to its origin is a uuid
//! it references but does not contain, a session id it shares with a sibling,
//! or a `/resume` the human typed. Reconstructing that needs evidence from
//! more than one file at once.
//!
//! burn does this by holding every parsed file in memory and reconciling at
//! the end of a full scan. relayhistory hydrates one session at a time, so the
//! same answer has to survive between runs: each transcript's evidence is
//! written to `session_continuity_evidence` as it is read, and reconciliation
//! runs over the stored rows. A file whose parent uuid is not indexed yet
//! stays pending with a reason attached; hydrating the file that holds the
//! record resolves it, without re-reading the first file.
//!
//! Nothing here infers a link from similarity. Every edge comes from an
//! explicit provider field, a marker the human typed, or a uuid that two
//! transcripts genuinely share.

use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::path::Path;

use crate::relationship_capture::{record_relationship, ObservedRelationship};
use crate::relationships::{
    RelationshipDiagnostic, RELATIONSHIP_CONTINUATION, RELATIONSHIP_FORK, RELATIONSHIP_RESUME,
};

/// A transcript carries evidence that is still waiting on something.
pub const CONTINUITY_UNRESOLVED: &str = "RELATIONSHIP_CONTINUITY_UNRESOLVED";

/// Field names recorded in `evidence_ref`, so a consumer reading a row knows
/// which signal produced it rather than having to guess.
const REF_CONTINUED_FROM: &str = "continuedFromSessionId";
const REF_FORK_SESSION: &str = "forkSessionId";
const REF_RESUME_MARKER: &str = "resume-marker";
const REF_SHARED_SESSION_ID: &str = "sharedSessionId";

/// What one transcript says about where its conversation came from.
///
/// Mirrors burn's `ClaudeRelationshipEvidence`, with `locator` and
/// `session_id` added: relayhistory keys sessions by the in-log `sessionId`,
/// and the file that carried it is the thing this row is about.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContinuityEvidence {
    pub source: String,
    /// The transcript this evidence was read from. The row's identity.
    pub locator: String,
    /// The in-log session id, which is the identity relayhistory stores
    /// events under.
    pub session_id: String,
    /// The provider's explicit `fileSessionId`, else the file basename. Used
    /// only to tell two transcripts apart — never as a session identity.
    pub file_session_id: Option<String>,
    /// `parentUuid` of the first non-sidechain user record. A conversation
    /// that begins by answering a record it does not contain came from
    /// somewhere else.
    pub first_parent_uuid: Option<String>,
    pub first_ts_ms: Option<i64>,
    /// Every distinct in-log session id, in first-seen order.
    pub in_log_session_ids: Vec<String>,
    pub has_resume_marker: bool,
    pub resume_target: Option<String>,
    pub explicit_continuation_targets: Vec<String>,
    pub explicit_fork_targets: Vec<String>,
    /// An explicit `sourceSessionId`, which names the origin directly.
    pub explicit_source_session_id: Option<String>,
    pub source_version: Option<String>,
}

impl ContinuityEvidence {
    /// The conversation two or more transcripts would be branches of.
    ///
    /// An explicit `sourceSessionId` names it. Otherwise it is the in-log
    /// session id, and only when the transcript's own file identity differs
    /// from it: a file named after the session it contains is an ordinary
    /// session, not a branch of anything.
    pub fn origin_session_id(&self) -> Option<String> {
        if let Some(explicit) = self
            .explicit_source_session_id
            .as_deref()
            .filter(|id| !id.is_empty() && *id != self.session_id)
        {
            return Some(explicit.to_string());
        }
        let file_session_id = self.file_session_id.as_deref()?;
        (file_session_id != self.session_id).then(|| self.session_id.clone())
    }

    /// How this transcript is told apart from a sibling sharing its origin.
    fn branch_label(&self) -> &str {
        self.file_session_id.as_deref().unwrap_or(&self.locator)
    }

    fn explicit_targets_json(&self) -> String {
        json!({
            "continuation": self.explicit_continuation_targets,
            "fork": self.explicit_fork_targets,
            "source": self.explicit_source_session_id,
        })
        .to_string()
    }
}

/// Read one Claude transcript's continuity evidence in a single pass.
///
/// Returns `None` for a transcript with no in-log session id: relayhistory has
/// no identity to attach the evidence to, and inventing one from the file name
/// is exactly what the delegation model already refuses to do.
pub fn scan_claude_transcript(path: &Path) -> Result<Option<ContinuityEvidence>> {
    let text = std::fs::read_to_string(path).unwrap_or_default();
    let mut evidence = ContinuityEvidence {
        source: "claude".to_string(),
        locator: path.to_string_lossy().to_string(),
        file_session_id: file_session_id_from_path(path),
        ..ContinuityEvidence::default()
    };
    let mut first_user_seen = false;
    let mut any = false;
    for line in text.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let Some(object) = value.as_object() else {
            continue;
        };
        any = true;
        if let Some(explicit) = string_field(object, &["fileSessionId", "file_session_id"]) {
            evidence.file_session_id = Some(explicit);
        }
        if let Some(session_id) = string_field(object, &["sessionId", "session_id"]) {
            if !evidence.in_log_session_ids.contains(&session_id) {
                evidence.in_log_session_ids.push(session_id);
            }
            if evidence.first_ts_ms.is_none() {
                evidence.first_ts_ms = record_ts_ms(object);
            }
        }
        if evidence.source_version.is_none() {
            evidence.source_version = string_field(object, &["version", "source_version"]);
        }
        if let Some(target) =
            string_field(object, &["continuedFromSessionId", "continued_from_session_id"])
        {
            push_unique(&mut evidence.explicit_continuation_targets, target);
        }
        if let Some(target) = string_field(object, &["forkSessionId", "fork_session_id"]) {
            push_unique(&mut evidence.explicit_fork_targets, target);
        }
        if evidence.explicit_source_session_id.is_none() {
            evidence.explicit_source_session_id =
                string_field(object, &["sourceSessionId", "source_session_id"]);
        }
        let sidechain = object
            .get("isSidechain")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let is_user = object.get("type").and_then(Value::as_str) == Some("user");
        if is_user && !sidechain {
            if !first_user_seen {
                first_user_seen = true;
                evidence.first_parent_uuid =
                    string_field(object, &["parentUuid", "parent_uuid"]);
            }
            record_resume_marker(&mut evidence, object);
        }
    }
    if !any {
        return Ok(None);
    }
    let Some(session_id) = evidence.in_log_session_ids.first().cloned() else {
        return Ok(None);
    };
    evidence.session_id = session_id;
    Ok(Some(evidence))
}

/// Codex records continuity on the `session_meta` line that opens a rollout,
/// or not at all.
///
/// `codex resume` leaves no identity behind in a local rollout: the resumed
/// thread opens with a fresh `payload.id`, and the only trace of the prior
/// conversation is a carried-over token baseline, which is a number, not a
/// session. So this reads the explicit fields when a producer writes them and
/// records nothing when it does not — see
/// `codex_resume_without_explicit_fields_records_no_continuity`.
pub fn scan_codex_rollout(path: &Path) -> Result<Option<ContinuityEvidence>> {
    let first = std::fs::read_to_string(path)
        .ok()
        .and_then(|text| text.lines().next().map(str::to_string))
        .unwrap_or_default();
    if first.trim().is_empty() {
        return Ok(None);
    }
    let Ok(value) = serde_json::from_str::<Value>(&first) else {
        return Ok(None);
    };
    if value.get("type").and_then(Value::as_str) != Some("session_meta") {
        return Ok(None);
    }
    let Some(payload) = value.get("payload").and_then(Value::as_object) else {
        return Ok(None);
    };
    let Some(session_id) = string_field(payload, &["id"]) else {
        return Ok(None);
    };
    let mut evidence = ContinuityEvidence {
        source: "codex".to_string(),
        locator: path.to_string_lossy().to_string(),
        session_id: session_id.clone(),
        // A rollout's file name is not an identity signal for Codex: the
        // thread id is always in the payload.
        file_session_id: Some(session_id.clone()),
        in_log_session_ids: vec![session_id],
        source_version: string_field(payload, &["cli_version", "version"]),
        explicit_source_session_id: string_field(
            payload,
            &["sourceSessionId", "source_session_id"],
        ),
        ..ContinuityEvidence::default()
    };
    evidence.first_ts_ms = record_ts_ms(
        value
            .as_object()
            .expect("session_meta line is an object here"),
    );
    if let Some(target) =
        string_field(payload, &["continuedFromSessionId", "continued_from_session_id"])
    {
        push_unique(&mut evidence.explicit_continuation_targets, target);
    }
    if let Some(target) = string_field(payload, &["forkSessionId", "fork_session_id"]) {
        push_unique(&mut evidence.explicit_fork_targets, target);
    }
    if evidence.explicit_continuation_targets.is_empty()
        && evidence.explicit_fork_targets.is_empty()
        && evidence.explicit_source_session_id.is_none()
    {
        return Ok(None);
    }
    Ok(Some(evidence))
}

/// Persist one transcript's evidence, replacing whatever the last read of the
/// same file recorded.
///
/// Stored as pending: reconciliation is what clears `pending_reason`, and a
/// re-read of a changed file has to be reconsidered even when the previous
/// read had resolved.
pub fn record_evidence(conn: &Connection, evidence: &ContinuityEvidence) -> Result<()> {
    conn.execute(
        "INSERT INTO session_continuity_evidence \
         (source, locator, session_id, file_session_id, first_parent_uuid, first_ts_ms, \
          in_log_session_ids_json, has_resume_marker, resume_target, explicit_targets_json, \
          source_version, origin_session_id, pending_reason, updated_ms) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'unreconciled', ?) \
         ON CONFLICT(source, locator) DO UPDATE SET \
           session_id = excluded.session_id, \
           file_session_id = excluded.file_session_id, \
           first_parent_uuid = excluded.first_parent_uuid, \
           first_ts_ms = excluded.first_ts_ms, \
           in_log_session_ids_json = excluded.in_log_session_ids_json, \
           has_resume_marker = excluded.has_resume_marker, \
           resume_target = excluded.resume_target, \
           explicit_targets_json = excluded.explicit_targets_json, \
           source_version = excluded.source_version, \
           origin_session_id = excluded.origin_session_id, \
           pending_reason = 'unreconciled', \
           updated_ms = excluded.updated_ms",
        params![
            evidence.source,
            evidence.locator,
            evidence.session_id,
            evidence.file_session_id,
            evidence.first_parent_uuid,
            evidence.first_ts_ms,
            serde_json::to_string(&evidence.in_log_session_ids)?,
            evidence.has_resume_marker,
            evidence.resume_target,
            evidence.explicit_targets_json(),
            evidence.source_version,
            evidence.origin_session_id(),
            crate::now_ms(),
        ],
    )?;
    Ok(())
}

/// Read one transcript's evidence and store it, in one call.
pub fn capture_claude_transcript(conn: &Connection, path: &Path) -> Result<()> {
    if let Some(evidence) = scan_claude_transcript(path)? {
        record_evidence(conn, &evidence)?;
    }
    Ok(())
}

/// Read one rollout's evidence and store it, when it carries any.
pub fn capture_codex_rollout(conn: &Connection, path: &Path) -> Result<()> {
    if let Some(evidence) = scan_codex_rollout(path)? {
        record_evidence(conn, &evidence)?;
    }
    Ok(())
}

/// What one reconciliation pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContinuityReconciliation {
    pub considered: usize,
    pub edges_written: usize,
    pub resolved: usize,
    pub pending: usize,
}

/// Resolve every transcript still waiting, and write the edges it establishes.
///
/// Only pending rows are considered, so a steady-state sync over an already
/// reconciled database reads nothing. A fork group is the exception that makes
/// this correct rather than merely cheap: when a pending row's origin turns
/// out to have siblings, every member of that group is re-emitted, because the
/// sibling that arrived first resolved with a group of one and has no fork
/// edge yet. `record_relationship` is an idempotent upsert, so re-emitting an
/// edge that already exists changes nothing but `updated_ms`.
pub fn reconcile(conn: &Connection, source: &str) -> Result<ContinuityReconciliation> {
    let pending = load_pending(conn, source)?;
    let mut report = ContinuityReconciliation {
        considered: pending.len(),
        ..ContinuityReconciliation::default()
    };
    for evidence in &pending {
        let mut reasons: Vec<String> = Vec::new();
        let mut lineage = resolve_explicit(conn, evidence)?;
        lineage += resolve_resume(conn, evidence, &mut reasons)?;
        lineage += resolve_cross_file_parent(conn, evidence, &mut reasons)?;
        report.edges_written += lineage;
        report.edges_written += resolve_fork_group(conn, evidence, lineage > 0, &mut reasons)?;
        let reason = (!reasons.is_empty()).then(|| reasons.join("; "));
        if reason.is_none() {
            report.resolved += 1;
        } else {
            report.pending += 1;
        }
        conn.execute(
            "UPDATE session_continuity_evidence SET pending_reason = ?, updated_ms = ? \
             WHERE source = ? AND locator = ?",
            params![reason, crate::now_ms(), evidence.source, evidence.locator],
        )?;
    }
    Ok(report)
}

/// Diagnostics for evidence about `session_id` that is still waiting.
///
/// Read from the stored rows rather than recomputed, so the caller sees the
/// same unresolved state whether it asks during the hydration that produced it
/// or a week later.
pub fn pending_diagnostics(
    conn: &Connection,
    source: &str,
    session_id: &str,
) -> Result<Vec<RelationshipDiagnostic>> {
    // A database that predates continuity has no table to read.
    if !table_exists(conn, "session_continuity_evidence")? {
        return Ok(Vec::new());
    }
    Ok(conn
        .prepare(
            "SELECT locator, pending_reason FROM session_continuity_evidence \
             WHERE source = ? AND session_id = ? AND pending_reason IS NOT NULL \
             ORDER BY locator ASC",
        )?
        .query_map(params![source, session_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .map(|(locator, reason)| RelationshipDiagnostic {
            code: CONTINUITY_UNRESOLVED.to_string(),
            message: format!(
                "{source} continuity evidence at {locator} is unresolved: {}",
                reason.as_deref().unwrap_or("unreconciled")
            ),
            relationship_uid: None,
        })
        .collect())
}

/// The same pending evidence, as `(locator, reason)` pairs.
pub fn pending_reasons(
    conn: &Connection,
    source: &str,
    session_id: &str,
) -> Result<Vec<(String, String)>> {
    if !table_exists(conn, "session_continuity_evidence")? {
        return Ok(Vec::new());
    }
    Ok(conn
        .prepare(
            "SELECT locator, pending_reason FROM session_continuity_evidence \
             WHERE source = ? AND session_id = ? AND pending_reason IS NOT NULL \
             ORDER BY locator ASC",
        )?
        .query_map(params![source, session_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?
                    .unwrap_or_else(|| "unreconciled".to_string()),
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}

// ---------------------------------------------------------------------------
// Resolution steps
// ---------------------------------------------------------------------------

/// Explicit provider fields. These name the origin outright, so they never
/// wait on anything and never produce a pending reason.
fn resolve_explicit(conn: &Connection, evidence: &ContinuityEvidence) -> Result<usize> {
    let mut written = 0;
    for target in &evidence.explicit_continuation_targets {
        if target.is_empty() || *target == evidence.session_id {
            continue;
        }
        write_edge(
            conn,
            evidence,
            RELATIONSHIP_CONTINUATION,
            target,
            Some(evidence.session_id.as_str()),
            REF_CONTINUED_FROM,
            evidence.explicit_source_session_id.as_deref(),
        )?;
        written += 1;
    }
    for target in &evidence.explicit_fork_targets {
        if target.is_empty() || *target == evidence.session_id {
            continue;
        }
        let origin = evidence
            .explicit_source_session_id
            .as_deref()
            .unwrap_or(target.as_str());
        write_edge(
            conn,
            evidence,
            RELATIONSHIP_FORK,
            target,
            Some(evidence.session_id.as_str()),
            REF_FORK_SESSION,
            Some(origin),
        )?;
        written += 1;
    }
    Ok(written)
}

/// A `/resume <id>` or `/continue <id>` the human typed.
///
/// A marker naming no session is recorded as unresolved rather than guessed
/// at: the parent of a relationship row cannot be null, and the nearest
/// session in time is not evidence.
fn resolve_resume(
    conn: &Connection,
    evidence: &ContinuityEvidence,
    reasons: &mut Vec<String>,
) -> Result<usize> {
    if !evidence.has_resume_marker {
        return Ok(0);
    }
    let Some(target) = evidence
        .resume_target
        .as_deref()
        .filter(|target| !target.is_empty())
    else {
        reasons.push("resume marker names no prior session".to_string());
        return Ok(0);
    };
    if target == evidence.session_id {
        return Ok(0);
    }
    write_edge(
        conn,
        evidence,
        RELATIONSHIP_RESUME,
        target,
        Some(evidence.session_id.as_str()),
        REF_RESUME_MARKER,
        Some(target),
    )?;
    Ok(1)
}

/// The transcript answers a record it does not contain.
///
/// The uuid is resolved against the events already indexed, so the answer is
/// whichever session actually holds that record — never a file name, and never
/// a guess. A uuid nothing has indexed yet is left pending with the uuid in
/// the reason, which is what makes hydrating the origin afterwards enough.
fn resolve_cross_file_parent(
    conn: &Connection,
    evidence: &ContinuityEvidence,
    reasons: &mut Vec<String>,
) -> Result<usize> {
    let Some(parent_uuid) = evidence
        .first_parent_uuid
        .as_deref()
        .filter(|uuid| !uuid.is_empty())
    else {
        return Ok(0);
    };
    let Some(parent_session_id) = session_holding_record(conn, &evidence.source, parent_uuid)?
    else {
        reasons.push(format!(
            "parent record {parent_uuid} is not indexed in any session yet"
        ));
        return Ok(0);
    };
    if parent_session_id == evidence.session_id {
        return Ok(0);
    }
    // An explicit field already said this, and said it better.
    if evidence
        .explicit_continuation_targets
        .contains(&parent_session_id)
        || evidence.resume_target.as_deref() == Some(parent_session_id.as_str())
    {
        return Ok(0);
    }
    write_edge(
        conn,
        evidence,
        RELATIONSHIP_CONTINUATION,
        &parent_session_id,
        Some(evidence.session_id.as_str()),
        parent_uuid,
        evidence.explicit_source_session_id.as_deref(),
    )?;
    Ok(1)
}

/// Two or more transcripts claiming one origin conversation are its branches.
///
/// A group of one is not a fork — it is a single session whose file happens to
/// be named something else — so it stays pending until a sibling shows up. The
/// branch's child identity is its in-log session id only when that differs
/// from the origin; when both transcripts carry the same in-log id, the branch
/// has no provider-recorded identity of its own and the row is unlinked
/// evidence keyed on the transcript, exactly as a nameless subagent sidecar is.
/// Deriving a child id from the file name is what this codebase refuses to do
/// everywhere else, and a fork is not the place to start.
fn resolve_fork_group(
    conn: &Connection,
    evidence: &ContinuityEvidence,
    has_lineage: bool,
    reasons: &mut Vec<String>,
) -> Result<usize> {
    let Some(origin) = evidence.origin_session_id() else {
        return Ok(0);
    };
    // An explicit fork field already established the branch.
    if !evidence.explicit_fork_targets.is_empty() {
        return Ok(0);
    }
    let group = fork_group(conn, &evidence.source, &origin)?;
    let labels: BTreeSet<&str> = group
        .iter()
        .map(|member| member.branch_label())
        .collect::<BTreeSet<_>>();
    if labels.len() < 2 {
        // A transcript that already knows where it came from is not waiting on
        // a sibling: the fork inference is the fallback for a file with no
        // lineage of its own, so reporting it as pending here would put a
        // permanent diagnostic on every fully explained transcript.
        if !has_lineage {
            reasons.push(format!(
                "only one transcript claims origin {origin}; a fork needs a sibling"
            ));
        }
        return Ok(0);
    }
    let mut written = 0;
    for member in &group {
        let child = (member.session_id != origin).then_some(member.session_id.as_str());
        write_edge(
            conn,
            member,
            RELATIONSHIP_FORK,
            &origin,
            child,
            REF_SHARED_SESSION_ID,
            Some(origin.as_str()),
        )?;
        written += 1;
    }
    Ok(written)
}

// ---------------------------------------------------------------------------
// Storage helpers
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn write_edge(
    conn: &Connection,
    evidence: &ContinuityEvidence,
    relationship: &str,
    parent_session_id: &str,
    child_session_id: Option<&str>,
    evidence_ref: &str,
    origin_session_id: Option<&str>,
) -> Result<()> {
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
            spawned_at_ms: evidence.first_ts_ms,
            origin_session_id,
            relationship_uid: Some(&uid),
            ..ObservedRelationship::default()
        },
    )
}

fn evidence_kind(source: &str) -> &'static str {
    match source {
        "codex" => "codex_session_meta_continuity",
        _ => "claude_transcript_continuity",
    }
}

fn load_pending(conn: &Connection, source: &str) -> Result<Vec<ContinuityEvidence>> {
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
fn fork_group(conn: &Connection, source: &str, origin: &str) -> Result<Vec<ContinuityEvidence>> {
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

fn map_evidence(row: &rusqlite::Row<'_>) -> rusqlite::Result<ContinuityEvidence> {
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
/// resolves.
fn session_holding_record(
    conn: &Connection,
    source: &str,
    record_uuid: &str,
) -> Result<Option<String>> {
    Ok(conn
        .query_row(
            "SELECT session_id FROM session_events \
             WHERE source = ? AND (message_id = ?2 OR event_uid = ?2 || ':0') \
             ORDER BY session_id ASC LIMIT 1",
            params![source, record_uuid],
            |row| row.get::<_, String>(0),
        )
        .optional()?)
}

fn session_has_events(conn: &Connection, source: &str, session_id: &str) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM session_events WHERE source = ? AND session_id = ? LIMIT 1)",
        params![source, session_id],
        |row| row.get(0),
    )?)
}

fn table_exists(conn: &Connection, name: &str) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?)",
        [name],
        |row| row.get(0),
    )?)
}

// ---------------------------------------------------------------------------
// Parsing helpers
// ---------------------------------------------------------------------------

/// The transcript's own file identity, used only to tell two files apart.
///
/// burn derives this from an explicit `fileSessionId` and then the transcript
/// path's basename, deliberately never from the path the parser happened to
/// open. relayhistory only ever has the real on-disk path, so the basename is
/// it — and because this value is never written as a session id, a file named
/// after nothing in particular costs nothing.
fn file_session_id_from_path(path: &Path) -> Option<String> {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .filter(|stem| !stem.is_empty())
        .map(str::to_string)
}

fn record_resume_marker(
    evidence: &mut ContinuityEvidence,
    object: &serde_json::Map<String, Value>,
) {
    let Some(text) = plain_user_text(object) else {
        return;
    };
    let trimmed = text.trim();
    let Some(after_slash) = trimmed.strip_prefix('/') else {
        return;
    };
    let command_end = after_slash
        .find(char::is_whitespace)
        .unwrap_or(after_slash.len());
    let command = after_slash[..command_end].to_lowercase();
    if command != "resume" && command != "continue" {
        return;
    }
    evidence.has_resume_marker = true;
    if evidence.resume_target.is_some() {
        return;
    }
    let rest = after_slash[command_end..].trim_start();
    let token_end = rest.find(char::is_whitespace).unwrap_or(rest.len());
    let token = &rest[..token_end];
    if !token.is_empty() {
        evidence.resume_target = Some(token.to_string());
    }
}

/// The user's own typed text, from either content shape. Tool results and
/// structured blocks are not something a human typed a slash command into.
fn plain_user_text(object: &serde_json::Map<String, Value>) -> Option<String> {
    let content = object.get("message").and_then(|m| m.get("content"))?;
    if let Some(text) = content.as_str() {
        return Some(text.to_string());
    }
    let blocks = content.as_array()?;
    let parts: Vec<&str> = blocks
        .iter()
        .filter(|block| block.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|block| block.get("text").and_then(Value::as_str))
        .collect();
    (!parts.is_empty()).then(|| parts.join("\n"))
}

fn record_ts_ms(object: &serde_json::Map<String, Value>) -> Option<i64> {
    object.get("timestamp").and_then(|value| {
        value
            .as_str()
            .and_then(crate::parse_iso_ms)
            .or_else(|| value.as_i64())
    })
}

fn string_field(object: &serde_json::Map<String, Value>, keys: &[&str]) -> Option<String> {
    keys.iter()
        .filter_map(|key| object.get(*key))
        .filter_map(Value::as_str)
        .map(str::trim)
        .find(|value| !value.is_empty())
        .map(str::to_string)
}

fn string_array(value: &Value, key: &str) -> Vec<String> {
    value
        .get(key)
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn push_unique(values: &mut Vec<String>, value: String) {
    if !values.contains(&value) {
        values.push(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::relationships::{RelationshipKinds, IDENTITY_OBSERVED, IDENTITY_UNLINKED};
    use crate::ingest::ingest_claude_transcript;
    use crate::{open_db, session_children, session_relationships};
    use rusqlite::Connection;
    use std::path::PathBuf;

    /// burn's corpus, copied verbatim, so the two implementations are read
    /// against the same bytes.
    fn fixture(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/claude")
            .join(name)
    }

    fn database() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let conn = open_db(&dir.path().join("history.db")).unwrap();
        (dir, conn)
    }

    /// Exactly what hydration does for one Claude transcript: index its
    /// records, bank its continuity evidence, then reconcile.
    fn ingest(conn: &Connection, name: &str) {
        let path = fixture(name);
        ingest_claude_transcript(conn, &path).unwrap();
        capture_claude_transcript(conn, &path).unwrap();
        reconcile(conn, "claude").unwrap();
    }

    fn edges(conn: &Connection, parent: &str) -> Vec<(String, Option<String>, String, String)> {
        session_children(conn, "claude", parent, &RelationshipKinds::continuity())
            .unwrap()
            .into_iter()
            .map(|edge| {
                (
                    edge.relationship,
                    edge.child_session_id,
                    edge.relationship_uid,
                    edge.evidence_ref.unwrap_or_default(),
                )
            })
            .collect()
    }

    const ORIGINAL: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    const CROSS: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
    const SHARED_FORK: &str = "00000000-0000-0000-0000-000000000fff";
    const RESUMED: &str = "99999999-9999-9999-9999-999999999999";
    const RESUME_TARGET: &str = "11111111-1111-1111-1111-111111111111";

    #[test]
    fn resume_marker_records_a_resume_edge_naming_the_prior_session() {
        let (_dir, conn) = database();
        ingest(&conn, "resume-marker.jsonl");
        assert_eq!(
            edges(&conn, RESUME_TARGET),
            vec![(
                RELATIONSHIP_RESUME.to_string(),
                Some(RESUMED.to_string()),
                format!("resume:{RESUMED}"),
                REF_RESUME_MARKER.to_string(),
            )]
        );
        // The marker is lineage, so the file is not also left waiting on a
        // fork sibling it has no reason to expect.
        assert!(pending_reasons(&conn, "claude", RESUMED).unwrap().is_empty());
    }

    #[test]
    fn explicit_fields_record_their_own_field_name_as_the_evidence() {
        let (_dir, conn) = database();
        ingest(&conn, "explicit-line-relationships.jsonl");
        assert_eq!(
            edges(&conn, "original-session"),
            vec![(
                RELATIONSHIP_CONTINUATION.to_string(),
                Some(CROSS.to_string()),
                format!("continuation:{CROSS}"),
                REF_CONTINUED_FROM.to_string(),
            )]
        );
        assert_eq!(
            edges(&conn, "fork-source-session"),
            vec![(
                RELATIONSHIP_FORK.to_string(),
                Some(CROSS.to_string()),
                format!("fork:{CROSS}"),
                REF_FORK_SESSION.to_string(),
            )]
        );
    }

    #[test]
    fn a_cross_file_parent_uuid_resolves_to_the_session_that_holds_the_record() {
        let (_dir, conn) = database();
        ingest(&conn, "original-session.jsonl");
        ingest(&conn, "cross-file-parent.jsonl");
        assert_eq!(
            edges(&conn, ORIGINAL),
            vec![(
                RELATIONSHIP_CONTINUATION.to_string(),
                Some(CROSS.to_string()),
                format!("continuation:{CROSS}"),
                // The uuid that linked the two files, not a file name.
                "u-original-asst".to_string(),
            )]
        );
        let edge = session_children(&conn, "claude", ORIGINAL, &RelationshipKinds::continuity())
            .unwrap()
            .remove(0);
        assert_eq!(edge.identity_status, IDENTITY_OBSERVED);
        assert!(edge.child_has_events);
    }

    #[test]
    fn an_unindexed_parent_uuid_is_reported_and_resolved_by_a_later_ingest() {
        let (_dir, conn) = database();
        // The branch arrives first: nothing holds `u-original-asst` yet.
        ingest(&conn, "cross-file-parent.jsonl");
        assert!(edges(&conn, ORIGINAL).is_empty());
        let pending = pending_reasons(&conn, "claude", CROSS).unwrap();
        assert_eq!(pending.len(), 1);
        assert!(
            pending[0].1.contains("u-original-asst"),
            "reason names the uuid that is missing: {}",
            pending[0].1
        );
        let diagnostics = session_relationships(&conn, "claude", CROSS)
            .unwrap()
            .diagnostics;
        assert!(diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == CONTINUITY_UNRESOLVED));

        // The origin arrives. `cross-file-parent.jsonl` is never read again:
        // its evidence was banked, and reconciliation runs off the database.
        let origin = fixture("original-session.jsonl");
        ingest_claude_transcript(&conn, &origin).unwrap();
        capture_claude_transcript(&conn, &origin).unwrap();
        reconcile(&conn, "claude").unwrap();

        assert_eq!(
            edges(&conn, ORIGINAL),
            vec![(
                RELATIONSHIP_CONTINUATION.to_string(),
                Some(CROSS.to_string()),
                format!("continuation:{CROSS}"),
                "u-original-asst".to_string(),
            )]
        );
        assert!(pending_reasons(&conn, "claude", CROSS).unwrap().is_empty());
    }

    #[test]
    fn two_transcripts_sharing_one_session_id_are_forks_of_it() {
        let (_dir, conn) = database();
        ingest(&conn, "fork-branch-a.jsonl");
        // One branch is not a fork: there is no sibling yet, and the
        // transcript is honest about waiting rather than inventing an origin.
        assert!(edges(&conn, SHARED_FORK).is_empty());
        let pending = pending_reasons(&conn, "claude", SHARED_FORK).unwrap();
        assert_eq!(pending.len(), 1);
        assert!(pending[0].1.contains("a fork needs a sibling"));

        ingest(&conn, "fork-branch-b.jsonl");
        let rows = edges(&conn, SHARED_FORK);
        assert_eq!(
            rows.iter()
                .map(|row| (row.0.as_str(), row.2.as_str()))
                .collect::<Vec<_>>(),
            vec![
                (RELATIONSHIP_FORK, "fork:fork-branch-a"),
                (RELATIONSHIP_FORK, "fork:fork-branch-b"),
            ]
        );
        // Both branches carry the shared in-log id, so neither has a provider
        // identity of its own. A file name is not one, here or anywhere else.
        for edge in
            session_children(&conn, "claude", SHARED_FORK, &RelationshipKinds::continuity())
                .unwrap()
        {
            assert_eq!(edge.identity_status, IDENTITY_UNLINKED);
            assert_eq!(edge.child_session_id, None);
            assert_eq!(edge.origin_session_id.as_deref(), Some(SHARED_FORK));
            assert_eq!(edge.evidence_ref.as_deref(), Some(REF_SHARED_SESSION_ID));
        }
        assert!(pending_reasons(&conn, "claude", SHARED_FORK)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn re_reading_a_transcript_neither_duplicates_nor_re_opens_its_edges() {
        let (_dir, conn) = database();
        ingest(&conn, "original-session.jsonl");
        ingest(&conn, "cross-file-parent.jsonl");
        let before = edges(&conn, ORIGINAL);
        ingest(&conn, "cross-file-parent.jsonl");
        ingest(&conn, "original-session.jsonl");
        assert_eq!(edges(&conn, ORIGINAL), before);
        let rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_continuity_evidence",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rows, 2);
    }

    #[test]
    fn a_fully_explained_corpus_reads_no_evidence_on_the_next_pass() {
        // Reconciliation only ever loads rows that are still waiting, so a
        // steady-state sync over an explained corpus does no work at all.
        let (_dir, conn) = database();
        ingest(&conn, "fork-branch-a.jsonl");
        ingest(&conn, "fork-branch-b.jsonl");
        let report = reconcile(&conn, "claude").unwrap();
        assert_eq!(report.considered, 0);
        assert_eq!(report.edges_written, 0);
    }

    #[test]
    fn a_transcript_still_waiting_is_reconsidered_on_every_pass() {
        // The other half of the same rule: a row that cannot resolve yet is
        // the one thing a later pass must pick up again, because what
        // resolves it is a *different* file arriving.
        let (_dir, conn) = database();
        ingest(&conn, "cross-file-parent.jsonl");
        let again = reconcile(&conn, "claude").unwrap();
        assert_eq!(again.considered, 1);
        assert_eq!(again.pending, 1);
        assert_eq!(again.edges_written, 0);
    }

    #[test]
    fn a_transcript_named_after_its_own_session_claims_no_origin() {
        // The real-world shape: Claude names a transcript after the session it
        // contains, so there is no foreign identity to be a branch of and the
        // fork inference never fires.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("session-1.jsonl");
        std::fs::write(
            &path,
            "{\"sessionId\":\"session-1\",\"uuid\":\"u1\",\"type\":\"user\",\
             \"message\":{\"role\":\"user\",\"content\":\"hello\"},\
             \"timestamp\":\"2026-08-31T10:00:00Z\"}\n",
        )
        .unwrap();
        let evidence = scan_claude_transcript(&path).unwrap().unwrap();
        assert_eq!(evidence.session_id, "session-1");
        assert_eq!(evidence.origin_session_id(), None);
        assert_eq!(evidence.first_parent_uuid, None);
        assert!(!evidence.has_resume_marker);
    }

    #[test]
    fn similar_prompts_alone_never_produce_an_edge() {
        // The forbidden inference, stated as a test: two unrelated sessions
        // with the same text and no shared uuid, marker or explicit field.
        let (_dir, conn) = database();
        let dir = tempfile::tempdir().unwrap();
        for id in ["twin-a", "twin-b"] {
            let path = dir.path().join(format!("{id}.jsonl"));
            std::fs::write(
                &path,
                format!(
                    "{{\"sessionId\":\"{id}\",\"uuid\":\"u-{id}\",\"type\":\"user\",\
                     \"message\":{{\"role\":\"user\",\"content\":\"refactor the auth module\"}},\
                     \"timestamp\":\"2026-08-31T10:00:00Z\"}}\n"
                ),
            )
            .unwrap();
            ingest_claude_transcript(&conn, &path).unwrap();
            capture_claude_transcript(&conn, &path).unwrap();
        }
        reconcile(&conn, "claude").unwrap();
        let total: i64 = conn
            .query_row("SELECT COUNT(*) FROM session_relationships", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(total, 0);
    }

    #[test]
    fn codex_session_meta_records_continuity_only_when_it_names_a_prior_thread() {
        let dir = tempfile::tempdir().unwrap();
        let explicit = dir.path().join("rollout-explicit.jsonl");
        std::fs::write(
            &explicit,
            "{\"timestamp\":\"2026-04-24T00:00:00.000Z\",\"type\":\"session_meta\",\
             \"payload\":{\"id\":\"sess_meta_child\",\"cwd\":\"/tmp/project\",\
             \"cli_version\":\"0.130.0\",\"sourceSessionId\":\"sess_original\",\
             \"forkSessionId\":\"sess_fork_base\",\
             \"continuedFromSessionId\":\"sess_previous\"}}\n",
        )
        .unwrap();
        let evidence = scan_codex_rollout(&explicit).unwrap().unwrap();
        assert_eq!(evidence.session_id, "sess_meta_child");
        assert_eq!(
            evidence.explicit_continuation_targets,
            vec!["sess_previous".to_string()]
        );
        assert_eq!(
            evidence.explicit_fork_targets,
            vec!["sess_fork_base".to_string()]
        );
        assert_eq!(
            evidence.explicit_source_session_id.as_deref(),
            Some("sess_original")
        );

        let conn = open_db(&dir.path().join("history.db")).unwrap();
        capture_codex_rollout(&conn, &explicit).unwrap();
        reconcile(&conn, "codex").unwrap();
        let rows: Vec<(String, String, Option<String>)> = conn
            .prepare(
                "SELECT relationship, parent_session_id, origin_session_id \
                 FROM session_relationships WHERE source = 'codex' \
                 ORDER BY relationship ASC",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            rows,
            vec![
                (
                    RELATIONSHIP_CONTINUATION.to_string(),
                    "sess_previous".to_string(),
                    Some("sess_original".to_string())
                ),
                (
                    RELATIONSHIP_FORK.to_string(),
                    "sess_fork_base".to_string(),
                    Some("sess_original".to_string())
                ),
            ]
        );
    }

    #[test]
    fn codex_resume_without_explicit_fields_records_no_continuity() {
        // Characterization, with the positive control above: a rollout that a
        // `codex resume` produced opens with its own fresh thread id and
        // carries a token baseline, which is a number and not an identity.
        // There is nothing here to record, so nothing is recorded — rather
        // than a `resume` row pointed at a guess.
        let dir = tempfile::tempdir().unwrap();
        let resumed = dir.path().join("rollout-resumed.jsonl");
        std::fs::write(
            &resumed,
            concat!(
                "{\"timestamp\":\"2026-08-02T09:00:00.000Z\",\"type\":\"session_meta\",",
                "\"payload\":{\"id\":\"sess-resumed\",\"cwd\":\"/tmp/proj\",",
                "\"originator\":\"codex_cli_rs\",\"thread_source\":\"user\",",
                "\"cli_version\":\"0.148.0\"}}\n",
                "{\"timestamp\":\"2026-08-02T09:00:01.000Z\",\"type\":\"event_msg\",",
                "\"payload\":{\"type\":\"token_count\",\"info\":{\"total_token_usage\":",
                "{\"input_tokens\":9000,\"output_tokens\":120}}}}\n",
            ),
        )
        .unwrap();
        assert_eq!(scan_codex_rollout(&resumed).unwrap(), None);
        let conn = open_db(&dir.path().join("history.db")).unwrap();
        capture_codex_rollout(&conn, &resumed).unwrap();
        reconcile(&conn, "codex").unwrap();
        let stored: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_continuity_evidence",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored, 0);
        let edges: i64 = conn
            .query_row("SELECT COUNT(*) FROM session_relationships", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(edges, 0);
    }
}
