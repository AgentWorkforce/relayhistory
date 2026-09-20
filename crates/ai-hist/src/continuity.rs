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

use anyhow::{Context, Result};
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
const REF_SOURCE_SESSION: &str = "sourceSessionId";

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

/// Read one newline-delimited record into `raw`; `false` at end of file.
///
/// Bounded by [`crate::ingest::cursor::MAX_RECORD_BYTES`]: a record that runs
/// past the ceiling is walked to its newline in fixed-size chunks and `raw` is
/// left empty, so one pathological line cannot cost this walk the file's size
/// in memory. `raw` is reused across calls, so the buffer is grown once.
fn next_record(reader: &mut impl std::io::BufRead, raw: &mut Vec<u8>) -> std::io::Result<bool> {
    use std::io::{BufRead, Read};
    const CEILING: u64 = crate::ingest::cursor::MAX_RECORD_BYTES;
    raw.clear();
    // The cap is on the reader rather than a check around it: `read_until`
    // extends `raw` until it finds a newline, so a budget consulted afterwards
    // can only observe an allocation that already happened.
    let read = reader.take(CEILING).read_until(b'\n', raw)? as u64;
    if read == 0 {
        return Ok(false);
    }
    if raw.last() != Some(&b'\n') {
        if read == CEILING {
            // Over the ceiling. Drop what was read and walk to the newline
            // through the buffer, consuming only up to it: anything after it
            // is the next record and must still be there to read.
            raw.clear();
            loop {
                let available = match reader.fill_buf() {
                    Ok(bytes) => bytes,
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(error) => return Err(error),
                };
                if available.is_empty() {
                    break;
                }
                match available.iter().position(|byte| *byte == b'\n') {
                    Some(at) => {
                        reader.consume(at + 1);
                        break;
                    }
                    None => {
                        let all = available.len();
                        reader.consume(all);
                    }
                }
            }
            return Ok(true);
        }
        // A genuine tail: the file ends here, under the ceiling.
        return Ok(true);
    }
    raw.pop();
    if raw.last() == Some(&b'\r') {
        raw.pop();
    }
    Ok(true)
}

/// Read one Claude transcript's continuity evidence in a single pass.
///
/// Returns `None` for a transcript with no in-log session id: relayhistory has
/// no identity to attach the evidence to, and inventing one from the file name
/// is exactly what the delegation model already refuses to do.
pub fn scan_claude_transcript(path: &Path) -> Result<Option<ContinuityEvidence>> {
    // Propagated, never collapsed into empty content. `Ok(None)` is what the
    // caller retracts on, so a transient read failure returning it would have
    // deleted real topology and reported a clean sync — the file still says
    // what it said, and we simply failed to look.
    // Streamed under the same record ceiling the ingest readers use.
    // `read_to_string` here made hydration of a live transcript unbounded in
    // memory and fatal on one stray byte. `TranscriptReader` is deliberately
    // not reused: it hashes a prefix window on open so a commit can detect a
    // rewrite, and this walk keeps no position, so that window would be 128
    // KiB of provider reads charged to a hydration for a cursor nobody writes.
    let file = std::fs::File::open(path)
        .with_context(|| format!("reading Claude transcript {}", path.display()))?;
    let mut reader = std::io::BufReader::new(file);
    let mut raw = Vec::new();
    let mut evidence = ContinuityEvidence {
        source: "claude".to_string(),
        locator: path.to_string_lossy().to_string(),
        file_session_id: file_session_id_from_path(path),
        ..ContinuityEvidence::default()
    };
    let mut first_user_seen = false;
    let mut any = false;
    while next_record(&mut reader, &mut raw)
        .with_context(|| format!("reading Claude transcript {}", path.display()))?
    {
        // An oversized record is not buffered and an undecodable one is not
        // repaired; both leave `raw` empty or unparseable and take the same
        // skip any other malformed record takes.
        let Ok(line) = std::str::from_utf8(&raw) else {
            continue;
        };
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
    // As above: a read failure is an error, not an empty rollout.
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading Codex rollout {}", path.display()))?;
    let first = text.lines().next().unwrap_or_default();
    if first.trim().is_empty() {
        return Ok(None);
    }
    let Ok(value) = serde_json::from_str::<Value>(first) else {
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
    // A rollout that names nothing still gets a row. The row's existence is
    // what tells a later sync this locator has already been read, and without
    // it every rollout that carries no continuity — which is nearly all of
    // them — would be re-read on every sync forever.
    Ok(Some(evidence))
}

/// Persist one transcript's evidence, replacing whatever the last read of the
/// same file recorded.
///
/// Stored as pending: reconciliation is what clears `pending_reason`, and a
/// re-read of a changed file has to be reconsidered even when the previous
/// read had resolved.
pub fn record_evidence(conn: &Connection, evidence: &ContinuityEvidence) -> Result<()> {
    // Read before write: a locator's *previous* identity is what its former
    // dependents were resolved against, and after the upsert it is gone.
    let previous = stored_identity(conn, &evidence.source, &evidence.locator)?;
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
    reopen_dependents(
        conn,
        &evidence.source,
        &[
            previous,
            Some((evidence.session_id.clone(), evidence.origin_session_id())),
        ],
    )?;
    Ok(())
}

/// The session and origin a stored evidence row currently claims.
fn stored_identity(
    conn: &Connection,
    source: &str,
    locator: &str,
) -> Result<Option<(String, Option<String>)>> {
    Ok(conn
        .query_row(
            "SELECT session_id, origin_session_id FROM session_continuity_evidence \
             WHERE source = ? AND locator = ?",
            params![source, locator],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?)
}

/// Mark every row whose resolution could have depended on these identities.
///
/// A resolved row is not re-read on later passes, which is what makes
/// reconciliation cheap — and what made it stale. Two rows can depend on a
/// third: a continuation resolved against the session that holds its parent
/// record, and a fork branch that is only a fork because a sibling claims the
/// same origin. When that third transcript is rewritten or removed, the
/// dependents have to be reconsidered or they keep an edge the evidence no
/// longer supports — and a fork group that shrinks below two has to lose its
/// fork edges, which only happens if the survivors are reconciled again.
///
/// Both directions are covered exactly rather than by re-reading everything:
/// fork siblings by the origin they share, and continuation dependents by the
/// edges that actually point at the session, read back from
/// `session_relationships`. Reopening is idempotent — a row that resolves the
/// same way rewrites the same edge.
fn reopen_dependents(
    conn: &Connection,
    source: &str,
    identities: &[Option<(String, Option<String>)>],
) -> Result<usize> {
    let mut sessions: BTreeSet<String> = BTreeSet::new();
    let mut origins: BTreeSet<String> = BTreeSet::new();
    for (session_id, origin) in identities.iter().flatten() {
        sessions.insert(session_id.clone());
        if let Some(origin) = origin {
            origins.insert(origin.clone());
        }
    }
    if sessions.is_empty() && origins.is_empty() {
        return Ok(0);
    }
    let session_holes = vec!["?"; sessions.len()].join(", ");
    let origin_holes = vec!["?"; origins.len()].join(", ");
    let sql = format!(
        "UPDATE session_continuity_evidence SET pending_reason = 'unreconciled' \
         WHERE source = ?1 AND pending_reason IS NULL \
           AND (origin_session_id IN ({origin_holes}) \
                OR locator IN ( \
                  SELECT evidence_locator FROM session_relationships \
                  WHERE source = ?1 \
                    AND relationship IN ('continuation', 'fork', 'resume') \
                    AND evidence_locator IS NOT NULL \
                    AND parent_session_id IN ({session_holes}) \
                ))"
    );
    let mut values: Vec<rusqlite::types::Value> = vec![source.to_string().into()];
    values.extend(origins.into_iter().map(Into::into));
    values.extend(sessions.into_iter().map(Into::into));
    Ok(conn.execute(&sql, rusqlite::params_from_iter(values))?)
}

/// Read one transcript's evidence and store it, in one call.
pub fn capture_claude_transcript(conn: &Connection, path: &Path) -> Result<()> {
    capture(conn, "claude", path, scan_claude_transcript(path)?)
}

/// Read one rollout's evidence and store it, in one call.
pub fn capture_codex_rollout(conn: &Connection, path: &Path) -> Result<()> {
    capture(conn, "codex", path, scan_codex_rollout(path)?)
}

/// Store what a file says now, or retract what it used to say.
///
/// A transcript that stops yielding evidence — rewritten, truncated, replaced
/// by a file that is not a session at all — has to retract the edges it
/// established, not keep them queryable forever. Leaving the old row in place
/// was the quieter failure: the file no longer says a thing, and the graph
/// went on reporting it.
fn capture(
    conn: &Connection,
    source: &str,
    path: &Path,
    evidence: Option<ContinuityEvidence>,
) -> Result<()> {
    match evidence {
        Some(evidence) => record_evidence(conn, &evidence),
        None => clear_evidence(conn, source, &path.to_string_lossy()),
    }
}

/// Drop one locator's evidence and every continuity edge it established.
pub fn clear_evidence(conn: &Connection, source: &str, locator: &str) -> Result<()> {
    let previous = stored_identity(conn, source, locator)?;
    retract_edges(conn, source, locator, &[])?;
    conn.execute(
        "DELETE FROM session_continuity_evidence WHERE source = ? AND locator = ?",
        params![source, locator],
    )?;
    // A removed transcript is exactly the case a survivor must be reconsidered
    // for: a fork group of two becomes a group of one, and the remaining
    // branch is no longer a branch of anything.
    reopen_dependents(conn, source, &[previous])?;
    Ok(())
}

/// Remove the continuity edges this locator established that it no longer does.
///
/// Retraction is keyed on `evidence_locator`, so it reaches only the rows this
/// file is responsible for — a sibling branch's fork row carries the sibling's
/// locator and is untouched. Surviving rows are left alone rather than deleted
/// and rewritten, which keeps their `created_ms` at first observation.
fn retract_edges(
    conn: &Connection,
    source: &str,
    locator: &str,
    keep: &[(String, String)],
) -> Result<usize> {
    // Keyed on the whole row identity, not the uid alone: a `/resume` retyped
    // against a different session keeps its `resume:<child>` uid and changes
    // only the parent, so a uid-only keep-set would have let the edge to the
    // old target survive beside the new one.
    let kept = keep
        .iter()
        .map(|_| " AND NOT (parent_session_id = ? AND relationship_uid = ?)")
        .collect::<String>();
    let sql = format!(
        "DELETE FROM session_relationships \
         WHERE source = ? AND evidence_locator = ? \
           AND relationship IN ('continuation', 'fork', 'resume'){kept}"
    );
    let mut values: Vec<rusqlite::types::Value> =
        vec![source.to_string().into(), locator.to_string().into()];
    for (parent, uid) in keep {
        values.push(parent.clone().into());
        values.push(uid.clone().into());
    }
    Ok(conn.execute(&sql, rusqlite::params_from_iter(values))?)
}

/// What one reconciliation pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContinuityReconciliation {
    pub considered: usize,
    pub edges_written: usize,
    /// Edges a re-read transcript no longer establishes, removed rather than
    /// left queryable.
    pub retracted: usize,
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
        // Every uid this locator establishes on this pass. What it established
        // on an earlier pass and no longer does is retracted below, so a
        // rewritten transcript cannot leave a stale edge queryable.
        let mut written: Vec<(String, String)> = Vec::new();
        resolve_explicit(conn, evidence, &mut written)?;
        resolve_resume(conn, evidence, &mut written)?;
        resolve_cross_file_parent(conn, evidence, &mut reasons, &mut written)?;
        // Last of the explicit signals: only when none of the above applied.
        resolve_explicit_source(conn, evidence, &mut written)?;
        let lineage = written.len();
        resolve_fork_group(conn, evidence, lineage > 0, &mut reasons, &mut written)?;
        report.edges_written += written.len();
        report.retracted += retract_edges(conn, &evidence.source, &evidence.locator, &written)?;
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
fn resolve_explicit(
    conn: &Connection,
    evidence: &ContinuityEvidence,
    written: &mut Vec<(String, String)>,
) -> Result<()> {
    for target in &evidence.explicit_continuation_targets {
        if target.is_empty() || *target == evidence.session_id {
            continue;
        }
        written.push(write_edge(
            conn,
            evidence,
            RELATIONSHIP_CONTINUATION,
            target,
            Some(evidence.session_id.as_str()),
            REF_CONTINUED_FROM,
            evidence.explicit_source_session_id.as_deref(),
        )?);
    }
    for target in &evidence.explicit_fork_targets {
        if target.is_empty() || *target == evidence.session_id {
            continue;
        }
        let origin = evidence
            .explicit_source_session_id
            .as_deref()
            .unwrap_or(target.as_str());
        written.push(write_edge(
            conn,
            evidence,
            RELATIONSHIP_FORK,
            target,
            Some(evidence.session_id.as_str()),
            REF_FORK_SESSION,
            Some(origin),
        )?);
    }
    Ok(())
}

/// An explicit `sourceSessionId`, when nothing else has said where this
/// transcript came from.
///
/// The field names the origin outright, which is lineage on its own — leaving
/// it to the fork-group fallback made a transcript that *says* where it came
/// from wait for a sibling to prove it. But it is the weakest of the explicit
/// signals, and on its own it does not say *how* the conversation carried on.
/// A file that also carries `continuedFromSessionId`, a resume marker or a
/// resolvable `parentUuid` has already been explained by a signal that does,
/// so emitting this as well would give one transcript two parents — or a
/// `fork` and a `continuation` to the same session. Those signals run first
/// and this runs only if they wrote nothing.
///
/// `sourceSessionId` is still recorded as `origin_session_id` on whichever
/// edge they did write, so the origin is never lost by being skipped here.
fn resolve_explicit_source(
    conn: &Connection,
    evidence: &ContinuityEvidence,
    written: &mut Vec<(String, String)>,
) -> Result<()> {
    if !written.is_empty() {
        return Ok(());
    }
    let Some(origin) = evidence
        .explicit_source_session_id
        .as_deref()
        .filter(|id| !id.is_empty() && *id != evidence.session_id)
    else {
        return Ok(());
    };
    written.push(write_edge(
        conn,
        evidence,
        RELATIONSHIP_FORK,
        origin,
        Some(evidence.session_id.as_str()),
        REF_SOURCE_SESSION,
        Some(origin),
    )?);
    Ok(())
}

/// A `/resume <id>` or `/continue <id>` the human typed.
///
/// A marker naming no session does not establish lineage. Another transcript
/// cannot supply its missing target, so it is neither an edge nor pending work.
fn resolve_resume(
    conn: &Connection,
    evidence: &ContinuityEvidence,
    written: &mut Vec<(String, String)>,
) -> Result<()> {
    if !evidence.has_resume_marker {
        return Ok(());
    }
    let Some(target) = evidence
        .resume_target
        .as_deref()
        .filter(|target| !target.is_empty())
    else {
        return Ok(());
    };
    if target == evidence.session_id {
        return Ok(());
    }
    written.push(write_edge(
        conn,
        evidence,
        RELATIONSHIP_RESUME,
        target,
        Some(evidence.session_id.as_str()),
        REF_RESUME_MARKER,
        // The explicit origin when the file names one, so skipping the
        // source-only fork never loses it.
        evidence
            .explicit_source_session_id
            .as_deref()
            .filter(|id| !id.is_empty() && *id != evidence.session_id)
            .or(Some(target)),
    )?);
    Ok(())
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
    written: &mut Vec<(String, String)>,
) -> Result<()> {
    // An explicit field has already said where this conversation came from,
    // and this is inference over the same question. Running it anyway left the
    // row pending on a uuid the answer did not depend on — and once that uuid
    // was indexed under some other session, added a second parent beside the
    // one the provider named. Callers reach this before `sourceSessionId` is
    // considered, so source-only evidence still gets the lookup and a
    // resolvable parent still outranks it.
    if !written.is_empty() {
        return Ok(());
    }
    let Some(parent_uuid) = evidence
        .first_parent_uuid
        .as_deref()
        .filter(|uuid| !uuid.is_empty())
    else {
        return Ok(());
    };
    let Some(parent_session_id) = session_holding_record(conn, &evidence.source, parent_uuid)?
    else {
        reasons.push(format!(
            "parent record {parent_uuid} is not indexed in any session yet"
        ));
        return Ok(());
    };
    if parent_session_id == evidence.session_id {
        return Ok(());
    }
    written.push(write_edge(
        conn,
        evidence,
        RELATIONSHIP_CONTINUATION,
        &parent_session_id,
        Some(evidence.session_id.as_str()),
        parent_uuid,
        evidence.explicit_source_session_id.as_deref(),
    )?);
    Ok(())
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
    written: &mut Vec<(String, String)>,
) -> Result<()> {
    let Some(origin) = evidence.origin_session_id() else {
        return Ok(());
    };
    // An explicit field already established the branch — `forkSessionId`, or
    // a `sourceSessionId` naming an origin of its own. The group inference is
    // only ever the fallback for a transcript with no explicit lineage.
    if !evidence.explicit_fork_targets.is_empty()
        || evidence
            .explicit_source_session_id
            .as_deref()
            .is_some_and(|id| !id.is_empty() && id != evidence.session_id)
    {
        return Ok(());
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
        return Ok(());
    }
    for member in &group {
        // Group membership alone is weaker than a provider-named target or a
        // parent record already indexed in another session. This check must
        // cover siblings too: they may have reconciled before the group grew,
        // and emitting their fork here would add a second lineage parent.
        if (member.locator == evidence.locator && has_lineage)
            || has_stronger_lineage(conn, member)?
        {
            continue;
        }
        let child = (member.session_id != origin).then_some(member.session_id.as_str());
        let uid = write_edge(
            conn,
            member,
            RELATIONSHIP_FORK,
            &origin,
            child,
            REF_SHARED_SESSION_ID,
            Some(origin.as_str()),
        )?;
        // Only this locator's own row is part of its keep-set; a sibling's row
        // is retracted by the sibling's own pass, never by this one.
        if member.locator == evidence.locator {
            written.push(uid);
        }
    }
    Ok(())
}

fn has_stronger_lineage(conn: &Connection, evidence: &ContinuityEvidence) -> Result<bool> {
    let names_other_session = |target: &str| !target.is_empty() && target != evidence.session_id;
    if evidence
        .explicit_continuation_targets
        .iter()
        .chain(&evidence.explicit_fork_targets)
        .any(|target| names_other_session(target))
        || evidence
            .explicit_source_session_id
            .as_deref()
            .is_some_and(names_other_session)
        || evidence.has_resume_marker
            && evidence
                .resume_target
                .as_deref()
                .is_some_and(names_other_session)
    {
        return Ok(true);
    }
    let Some(parent_uuid) = evidence.first_parent_uuid.as_deref() else {
        return Ok(false);
    };
    Ok(session_holding_record(conn, &evidence.source, parent_uuid)?
        .is_some_and(|session| session != evidence.session_id))
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
) -> Result<(String, String)> {
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
    )?;
    Ok((parent_session_id.to_string(), uid))
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

/// A `/resume` or `/continue` the human ran, in either form Claude writes.
///
/// Claude Code does not store a slash command as the text the human typed. It
/// stores a control wrapper — `<command-message>resume is running…`,
/// `<command-name>/resume</command-name>`, `<command-args>…</command-args>` —
/// which this crate already recognises as a control prompt and keeps out of
/// prompt history. Matching only bare `/resume` therefore matched the one form
/// a real session never contains, and every actual resume went unrecorded.
///
/// burn reads the same marker off plain user text only, so on the bare form
/// the two agree; the wrapped form is one burn does not detect either.
fn record_resume_marker(
    evidence: &mut ContinuityEvidence,
    object: &serde_json::Map<String, Value>,
) {
    let Some(text) = plain_user_text(object) else {
        return;
    };
    let trimmed = text.trim();
    let (command, rest) = if crate::discover::is_claude_control_prompt(trimmed) {
        match wrapped_command(trimmed) {
            Some(parsed) => parsed,
            None => return,
        }
    } else {
        match bare_command(trimmed) {
            Some(parsed) => parsed,
            None => return,
        }
    };
    if command != "resume" && command != "continue" {
        return;
    }
    evidence.has_resume_marker = true;
    if evidence.resume_target.is_some() {
        return;
    }
    let token_end = rest.find(char::is_whitespace).unwrap_or(rest.len());
    let token = &rest[..token_end];
    if !token.is_empty() {
        evidence.resume_target = Some(token.to_string());
    }
}

/// `/resume <target>` as typed: the form burn matches.
fn bare_command(text: &str) -> Option<(String, &str)> {
    let after_slash = text.strip_prefix('/')?;
    let command_end = after_slash
        .find(char::is_whitespace)
        .unwrap_or(after_slash.len());
    Some((
        after_slash[..command_end].to_lowercase(),
        after_slash[command_end..].trim_start(),
    ))
}

/// The command name and arguments Claude Code's control wrapper carries.
///
/// `<command-args>` is absent when the command took none, and the elements can
/// arrive in either order, so each is read independently rather than by
/// position.
fn wrapped_command(text: &str) -> Option<(String, &str)> {
    let name = tag_body(text, "command-name")?;
    let command = name.trim().trim_start_matches('/').to_lowercase();
    let args = tag_body(text, "command-args").unwrap_or("").trim_start();
    Some((command, args))
}

fn tag_body<'a>(text: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = text.find(&open)? + open.len();
    let end = text[start..].find(&close)? + start;
    Some(&text[start..end])
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
        let evidence = scan_codex_rollout(&resumed).unwrap().unwrap();
        assert_eq!(evidence.session_id, "sess-resumed");
        assert!(evidence.explicit_continuation_targets.is_empty());
        assert!(evidence.explicit_fork_targets.is_empty());
        assert_eq!(evidence.explicit_source_session_id, None);
        assert_eq!(evidence.origin_session_id(), None);

        let conn = open_db(&dir.path().join("history.db")).unwrap();
        capture_codex_rollout(&conn, &resumed).unwrap();
        reconcile(&conn, "codex").unwrap();
        // The rollout still gets an evidence row. That row is what tells the
        // next sync this locator has already been read: without it, every
        // rollout that carries no continuity — nearly all of them — would be
        // re-read on every sync forever.
        let stored: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_continuity_evidence",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored, 1);
        // What it does not get is an edge pointed at a guess.
        let edges: i64 = conn
            .query_row("SELECT COUNT(*) FROM session_relationships", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(edges, 0);
        // And it settles: a second pass has nothing left to consider.
        assert_eq!(reconcile(&conn, "codex").unwrap().considered, 0);
    }

    #[test]
    fn a_rewritten_transcript_retracts_the_edge_it_no_longer_establishes() {
        // The quiet failure this guards: the upsert replaces the evidence row,
        // reconciliation only ever adds, so the edge the file used to
        // establish stayed queryable after the file stopped saying it.
        let (_dir, conn) = database();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("resumer.jsonl");
        let write = |target: &str| {
            std::fs::write(
                &path,
                format!(
                    "{{\"sessionId\":\"resumer\",\"uuid\":\"u1\",\"parentUuid\":null,\
                     \"type\":\"user\",\"message\":{{\"role\":\"user\",\
                     \"content\":\"/resume {target}\"}},\
                     \"timestamp\":\"2026-08-31T10:00:00Z\"}}\n"
                ),
            )
            .unwrap();
        };
        let current = |conn: &Connection| -> Vec<(String, String)> {
            conn.prepare(
                "SELECT relationship, parent_session_id FROM session_relationships \
                 WHERE relationship IN ('continuation', 'fork', 'resume') \
                 ORDER BY parent_session_id",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
        };

        write("old-origin");
        capture_claude_transcript(&conn, &path).unwrap();
        reconcile(&conn, "claude").unwrap();
        assert_eq!(
            current(&conn),
            vec![(RELATIONSHIP_RESUME.to_string(), "old-origin".to_string())]
        );

        // The human re-ran the command against a different session.
        write("new-origin");
        capture_claude_transcript(&conn, &path).unwrap();
        let report = reconcile(&conn, "claude").unwrap();
        assert_eq!(report.retracted, 1);
        assert_eq!(
            current(&conn),
            vec![(RELATIONSHIP_RESUME.to_string(), "new-origin".to_string())],
            "exactly one current edge remains, naming the session the file now names"
        );
    }

    #[test]
    fn a_transcript_that_stops_yielding_evidence_retracts_it_entirely() {
        let (_dir, conn) = database();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("resumer.jsonl");
        std::fs::write(
            &path,
            "{\"sessionId\":\"resumer\",\"uuid\":\"u1\",\"type\":\"user\",\
             \"message\":{\"role\":\"user\",\"content\":\"/resume old-origin\"},\
             \"timestamp\":\"2026-08-31T10:00:00Z\"}\n",
        )
        .unwrap();
        capture_claude_transcript(&conn, &path).unwrap();
        reconcile(&conn, "claude").unwrap();
        assert_eq!(edges(&conn, "old-origin").len(), 1);

        // Truncated, replaced, or otherwise no longer a session at all.
        std::fs::write(&path, "").unwrap();
        capture_claude_transcript(&conn, &path).unwrap();
        assert!(edges(&conn, "old-origin").is_empty());
        let rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_continuity_evidence",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(rows, 0);
    }

    #[test]
    fn a_siblings_fork_row_survives_this_locators_retraction() {
        // Retraction is keyed on `evidence_locator`, so a pass over one branch
        // must not remove the row the other branch is responsible for.
        let (_dir, conn) = database();
        ingest(&conn, "fork-branch-a.jsonl");
        ingest(&conn, "fork-branch-b.jsonl");
        assert_eq!(edges(&conn, SHARED_FORK).len(), 2);
        // Re-capture one branch and reconcile: the sibling's row is untouched.
        capture_claude_transcript(&conn, &fixture("fork-branch-a.jsonl")).unwrap();
        let report = reconcile(&conn, "claude").unwrap();
        assert_eq!(report.retracted, 0);
        assert_eq!(edges(&conn, SHARED_FORK).len(), 2);
    }

    #[test]
    fn a_wrapped_slash_command_is_the_form_a_real_session_carries() {
        // Claude Code stores `/resume <id>` as a control wrapper, not as the
        // text the human typed. Matching only the bare form matched the one
        // shape a real transcript never contains.
        let (_dir, conn) = database();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wrapped.jsonl");
        std::fs::write(
            &path,
            concat!(
                "{\"sessionId\":\"wrapped\",\"uuid\":\"u1\",\"parentUuid\":null,",
                "\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":",
                "\"<command-message>resume is running…</command-message>\\n",
                "<command-name>/resume</command-name>\\n",
                "<command-args>prior-session extra</command-args>\"},",
                "\"timestamp\":\"2026-08-31T10:00:00Z\"}\n",
            ),
        )
        .unwrap();
        let evidence = scan_claude_transcript(&path).unwrap().unwrap();
        assert!(evidence.has_resume_marker);
        assert_eq!(evidence.resume_target.as_deref(), Some("prior-session"));

        capture_claude_transcript(&conn, &path).unwrap();
        reconcile(&conn, "claude").unwrap();
        assert_eq!(
            edges(&conn, "prior-session")
                .into_iter()
                .map(|edge| edge.0)
                .collect::<Vec<_>>(),
            vec![RELATIONSHIP_RESUME.to_string()]
        );
    }

    #[test]
    fn a_wrapped_command_without_args_is_a_marker_with_no_target() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bare-wrapped.jsonl");
        std::fs::write(
            &path,
            concat!(
                "{\"sessionId\":\"wrapped\",\"uuid\":\"u1\",\"type\":\"user\",",
                "\"message\":{\"role\":\"user\",\"content\":",
                "\"<command-message>continue is running…</command-message>\\n",
                "<command-name>/continue</command-name>\"},",
                "\"timestamp\":\"2026-08-31T10:00:00Z\"}\n",
            ),
        )
        .unwrap();
        let evidence = scan_claude_transcript(&path).unwrap().unwrap();
        assert!(evidence.has_resume_marker);
        assert_eq!(evidence.resume_target, None);
    }

    #[test]
    fn a_wrapped_command_that_is_not_a_resume_sets_no_marker() {
        // The positive control for the two above: the same wrapper shape, a
        // different command, and nothing is recorded.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("review.jsonl");
        std::fs::write(
            &path,
            concat!(
                "{\"sessionId\":\"wrapped\",\"uuid\":\"u1\",\"type\":\"user\",",
                "\"message\":{\"role\":\"user\",\"content\":",
                "\"<command-message>review is running…</command-message>\\n",
                "<command-name>/review</command-name>\"},",
                "\"timestamp\":\"2026-08-31T10:00:00Z\"}\n",
            ),
        )
        .unwrap();
        let evidence = scan_claude_transcript(&path).unwrap().unwrap();
        assert!(!evidence.has_resume_marker);
        assert_eq!(evidence.resume_target, None);
    }

    #[test]
    fn a_read_failure_never_retracts_what_the_file_still_says() {
        // A transient read failure used to collapse into empty content, which
        // `capture` reads as "this file says nothing any more" and retracts
        // on. The file still says what it said; we simply failed to look.
        let (_dir, conn) = database();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("resumer.jsonl");
        std::fs::write(
            &path,
            "{\"sessionId\":\"resumer\",\"uuid\":\"u1\",\"type\":\"user\",\
             \"message\":{\"role\":\"user\",\"content\":\"/resume prior\"},\
             \"timestamp\":\"2026-08-31T10:00:00Z\"}\n",
        )
        .unwrap();
        capture_claude_transcript(&conn, &path).unwrap();
        reconcile(&conn, "claude").unwrap();
        assert_eq!(edges(&conn, "prior").len(), 1);

        // The positive control for the two assertions below: a readable file
        // scans, an unreadable one errors rather than reporting emptiness.
        let missing = dir.path().join("not-there.jsonl");
        assert!(scan_claude_transcript(&path).unwrap().is_some());
        assert!(scan_claude_transcript(&missing).is_err());
        assert!(scan_codex_rollout(&missing).is_err());

        // And the error reaches the caller instead of retracting.
        assert!(capture_claude_transcript(&conn, &missing).is_err());
        assert_eq!(
            edges(&conn, "prior").len(),
            1,
            "a failed read left the edge the file still establishes"
        );
    }

    #[test]
    fn an_origin_that_the_file_stops_naming_is_dropped_not_merged() {
        // Optional relationship detail is merged with COALESCE so a thinner
        // later observation cannot erase it. `origin_session_id` is the
        // exception: reconciliation rebuilds the whole row on every recapture,
        // so a re-read that no longer finds `sourceSessionId` is saying the
        // origin is gone, and merging kept the stale one forever.
        let (_dir, conn) = database();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("branch.jsonl");
        let write = |source_field: &str| {
            std::fs::write(
                &path,
                format!(
                    "{{\"sessionId\":\"branch\",\"uuid\":\"u1\",\"type\":\"user\",\
                     {source_field}\"forkSessionId\":\"base\",\
                     \"message\":{{\"role\":\"user\",\"content\":\"hi\"}},\
                     \"timestamp\":\"2026-08-31T10:00:00Z\"}}\n"
                ),
            )
            .unwrap();
        };
        let origin = |conn: &Connection| -> Option<String> {
            conn.query_row(
                "SELECT origin_session_id FROM session_relationships \
                 WHERE relationship = 'fork'",
                [],
                |row| row.get(0),
            )
            .unwrap()
        };

        write("\"sourceSessionId\":\"original\",");
        capture_claude_transcript(&conn, &path).unwrap();
        reconcile(&conn, "claude").unwrap();
        assert_eq!(origin(&conn).as_deref(), Some("original"));

        // The field is removed; the fork itself still stands.
        write("");
        capture_claude_transcript(&conn, &path).unwrap();
        reconcile(&conn, "claude").unwrap();
        assert_eq!(
            origin(&conn).as_deref(),
            Some("base"),
            "the origin follows the evidence rather than surviving it"
        );
    }

    #[test]
    fn a_survivor_stops_being_a_fork_when_its_sibling_goes_away() {
        // A resolved row is never re-read, which is what keeps reconciliation
        // cheap — and what made a shrinking fork group stale. The survivor is
        // no longer a branch of anything, so its fork edge has to go.
        let (_dir, conn) = database();
        ingest(&conn, "fork-branch-a.jsonl");
        ingest(&conn, "fork-branch-b.jsonl");
        assert_eq!(edges(&conn, SHARED_FORK).len(), 2);

        // One branch is removed from the store.
        clear_evidence(
            &conn,
            "claude",
            &fixture("fork-branch-b.jsonl").to_string_lossy(),
        )
        .unwrap();
        let report = reconcile(&conn, "claude").unwrap();
        assert_eq!(
            report.considered, 1,
            "the survivor was reopened, not left resolved and stale"
        );
        assert!(
            edges(&conn, SHARED_FORK).is_empty(),
            "one transcript claiming an origin is not a fork"
        );
        // It is pending rather than silently empty: a sibling may come back.
        let pending = pending_reasons(&conn, "claude", SHARED_FORK).unwrap();
        assert_eq!(pending.len(), 1);
        assert!(pending[0].1.contains("a fork needs a sibling"));
    }

    /// Two transcripts named after the sessions they contain, the second
    /// opening by answering the first's last record — the real-world shape,
    /// with no basename mismatch to make either look like a fork branch, so
    /// the corpus settles completely and "reopened" is unambiguous.
    fn linked_pair(dir: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf) {
        let origin = dir.join("origin.jsonl");
        let follower = dir.join("follower.jsonl");
        std::fs::write(
            &origin,
            concat!(
                "{\"sessionId\":\"origin\",\"uuid\":\"origin-u\",\"parentUuid\":null,",
                "\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"start\"},",
                "\"timestamp\":\"2026-08-31T10:00:00Z\"}\n",
                "{\"sessionId\":\"origin\",\"uuid\":\"origin-a\",\"parentUuid\":\"origin-u\",",
                "\"type\":\"assistant\",\"message\":{\"role\":\"assistant\",\"content\":\"ok\"},",
                "\"timestamp\":\"2026-08-31T10:00:01Z\"}\n",
            ),
        )
        .unwrap();
        std::fs::write(
            &follower,
            concat!(
                "{\"sessionId\":\"follower\",\"uuid\":\"follow-u\",",
                "\"parentUuid\":\"origin-a\",\"type\":\"user\",",
                "\"message\":{\"role\":\"user\",\"content\":\"carry on\"},",
                "\"timestamp\":\"2026-08-31T11:00:00Z\"}\n",
            ),
        )
        .unwrap();
        (origin, follower)
    }

    fn ingest_file(conn: &Connection, path: &std::path::Path) {
        ingest_claude_transcript(conn, path).unwrap();
        capture_claude_transcript(conn, path).unwrap();
        reconcile(conn, "claude").unwrap();
    }

    #[test]
    fn a_rewritten_origin_reopens_the_continuation_that_pointed_at_it() {
        // The other dependency direction: a continuation resolved against the
        // session that holds its parent record. Rewriting that transcript has
        // to reconsider the dependent, which is resolved and would otherwise
        // never be read again.
        let (_dir, conn) = database();
        let dir = tempfile::tempdir().unwrap();
        let (origin, follower) = linked_pair(dir.path());
        ingest_file(&conn, &origin);
        ingest_file(&conn, &follower);
        assert_eq!(edges(&conn, "origin").len(), 1);
        assert_eq!(
            reconcile(&conn, "claude").unwrap().considered,
            0,
            "the corpus is fully explained before the rewrite"
        );

        capture_claude_transcript(&conn, &origin).unwrap();
        let report = reconcile(&conn, "claude").unwrap();
        assert_eq!(
            report.considered, 2,
            "the origin and the continuation that depends on it"
        );
        // The evidence is unchanged, so the edge is too: reopening is
        // idempotent, not destructive.
        assert_eq!(edges(&conn, "origin").len(), 1);
    }

    #[test]
    fn an_unrelated_transcript_is_not_reopened() {
        // The positive control for the test above: reopening is scoped to rows
        // that actually depend on the recaptured locator, not a re-read of
        // everything, which would give back the cost the pending marker buys.
        let (_dir, conn) = database();
        let dir = tempfile::tempdir().unwrap();
        let (origin, follower) = linked_pair(dir.path());
        ingest_file(&conn, &origin);
        ingest_file(&conn, &follower);
        let unrelated = dir.path().join("unrelated.jsonl");
        std::fs::write(
            &unrelated,
            concat!(
                "{\"sessionId\":\"unrelated\",\"uuid\":\"un-u\",\"parentUuid\":null,",
                "\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"alone\"},",
                "\"timestamp\":\"2026-08-31T12:00:00Z\"}\n",
            ),
        )
        .unwrap();
        ingest_file(&conn, &unrelated);
        assert_eq!(reconcile(&conn, "claude").unwrap().considered, 0);

        capture_claude_transcript(&conn, &unrelated).unwrap();
        let report = reconcile(&conn, "claude").unwrap();
        assert_eq!(
            report.considered, 1,
            "only the recaptured transcript, which nothing else depends on"
        );
    }

    #[test]
    fn a_claude_transcript_naming_only_a_source_session_is_a_branch_of_it() {
        // A provider that writes `sourceSessionId` and nothing else has said
        // where the conversation came from. Leaving that to the fork-group
        // fallback made it wait for a sibling to prove what it already
        // stated, so it stayed pending forever and produced no edge.
        let (_dir, conn) = database();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("branch.jsonl");
        std::fs::write(
            &path,
            concat!(
                "{\"sessionId\":\"branch\",\"uuid\":\"b-u\",\"parentUuid\":null,",
                "\"type\":\"user\",\"sourceSessionId\":\"origin\",",
                "\"message\":{\"role\":\"user\",\"content\":\"branched\"},",
                "\"timestamp\":\"2026-08-31T10:00:00Z\"}\n",
            ),
        )
        .unwrap();
        capture_claude_transcript(&conn, &path).unwrap();
        reconcile(&conn, "claude").unwrap();

        let rows = edges(&conn, "origin");
        assert_eq!(
            rows,
            vec![(
                RELATIONSHIP_FORK.to_string(),
                Some("branch".to_string()),
                "fork:branch".to_string(),
                REF_SOURCE_SESSION.to_string(),
            )]
        );
        assert!(
            pending_reasons(&conn, "claude", "branch").unwrap().is_empty(),
            "a transcript that names its origin is not waiting on a sibling"
        );
    }

    #[test]
    fn a_codex_rollout_naming_only_a_source_session_is_a_branch_of_it() {
        let dir = tempfile::tempdir().unwrap();
        let rollout = dir.path().join("rollout-branch.jsonl");
        std::fs::write(
            &rollout,
            concat!(
                "{\"timestamp\":\"2026-08-31T10:00:00Z\",\"type\":\"session_meta\",",
                "\"payload\":{\"id\":\"thread-b\",\"cwd\":\"/tmp/proj\",",
                "\"cli_version\":\"0.148.0\",\"sourceSessionId\":\"thread-a\"}}\n",
            ),
        )
        .unwrap();
        let conn = open_db(&dir.path().join("history.db")).unwrap();
        capture_codex_rollout(&conn, &rollout).unwrap();
        reconcile(&conn, "codex").unwrap();

        let row: (String, String, Option<String>, Option<String>) = conn
            .query_row(
                "SELECT relationship, parent_session_id, child_session_id, origin_session_id \
                 FROM session_relationships WHERE source = 'codex'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(
            row,
            (
                RELATIONSHIP_FORK.to_string(),
                "thread-a".to_string(),
                Some("thread-b".to_string()),
                Some("thread-a".to_string())
            )
        );
        assert!(pending_reasons(&conn, "codex", "thread-b")
            .unwrap()
            .is_empty());
    }

    #[test]
    fn a_transcript_with_no_explicit_lineage_still_waits_for_a_sibling() {
        // The positive control for the two above: the same shape *without*
        // the explicit field is exactly the case the fork-group fallback is
        // for, and it is still reported as pending rather than guessed at.
        let (_dir, conn) = database();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("not-the-session-id.jsonl");
        std::fs::write(
            &path,
            concat!(
                "{\"sessionId\":\"shared\",\"uuid\":\"s-u\",\"parentUuid\":null,",
                "\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"hi\"},",
                "\"timestamp\":\"2026-08-31T10:00:00Z\"}\n",
            ),
        )
        .unwrap();
        capture_claude_transcript(&conn, &path).unwrap();
        reconcile(&conn, "claude").unwrap();
        assert!(edges(&conn, "shared").is_empty());
        let pending = pending_reasons(&conn, "claude", "shared").unwrap();
        assert_eq!(pending.len(), 1);
        assert!(pending[0].1.contains("a fork needs a sibling"));
    }

    #[test]
    fn a_later_fork_sibling_does_not_add_a_parent_to_stronger_lineage() {
        let (dir, conn) = database();
        let stronger = dir.path().join("branch-a.jsonl");
        let sibling = dir.path().join("branch-b.jsonl");
        std::fs::write(
            &stronger,
            "{\"sessionId\":\"shared\",\"uuid\":\"a-u\",\"parentUuid\":null,\"type\":\"user\",\"continuedFromSessionId\":\"prior\",\"message\":{\"role\":\"user\",\"content\":\"carry on\"}}\n",
        )
        .unwrap();
        std::fs::write(
            &sibling,
            "{\"sessionId\":\"shared\",\"uuid\":\"b-u\",\"parentUuid\":null,\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"branch\"}}\n",
        )
        .unwrap();
        capture_claude_transcript(&conn, &stronger).unwrap();
        reconcile(&conn, "claude").unwrap();
        capture_claude_transcript(&conn, &sibling).unwrap();
        reconcile(&conn, "claude").unwrap();

        let rows: Vec<(String, String, String)> = conn
            .prepare(
                "SELECT relationship, parent_session_id, evidence_locator \
                 FROM session_relationships WHERE source = 'claude' \
                 ORDER BY relationship, evidence_locator",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            rows,
            vec![
                (
                    "continuation".into(),
                    "prior".into(),
                    stronger.to_string_lossy().into_owned(),
                ),
                (
                    "fork".into(),
                    "shared".into(),
                    sibling.to_string_lossy().into_owned(),
                ),
            ]
        );
    }

    #[test]
    fn nameless_continue_does_not_leave_permanent_pending_evidence() {
        let (dir, conn) = database();
        for (session, explicit) in [("ordinary", ""), ("continued", ",\"continuedFromSessionId\":\"prior\"")] {
            let path = dir.path().join(format!("{session}.jsonl"));
            std::fs::write(
                &path,
                format!(
                    "{{\"sessionId\":\"{session}\",\"uuid\":\"{session}-u\",\"parentUuid\":null,\"type\":\"user\"{explicit},\"message\":{{\"role\":\"user\",\"content\":\"/continue\"}}}}\n"
                ),
            )
            .unwrap();
            capture_claude_transcript(&conn, &path).unwrap();
        }
        reconcile(&conn, "claude").unwrap();
        assert!(pending_reasons(&conn, "claude", "ordinary")
            .unwrap()
            .is_empty());
        assert!(pending_reasons(&conn, "claude", "continued")
            .unwrap()
            .is_empty());
        assert!(edges(&conn, "ordinary").is_empty());
        assert_eq!(edges(&conn, "prior").len(), 1);
    }

    #[test]
    fn a_transcript_with_stronger_lineage_gets_one_parent_not_two() {
        // `sourceSessionId` names the origin but not how the conversation
        // carried on, so it is the weakest explicit signal. Emitting it beside
        // `continuedFromSessionId` gave one transcript two parents — and when
        // both fields name the same session, a `fork` and a `continuation` to
        // the same place.
        let (_dir, conn) = database();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("continued.jsonl");
        std::fs::write(
            &path,
            concat!(
                "{\"sessionId\":\"continued\",\"uuid\":\"c-u\",\"parentUuid\":null,",
                "\"type\":\"user\",\"continuedFromSessionId\":\"prior\",",
                "\"sourceSessionId\":\"origin\",",
                "\"message\":{\"role\":\"user\",\"content\":\"carry on\"},",
                "\"timestamp\":\"2026-08-31T10:00:00Z\"}\n",
            ),
        )
        .unwrap();
        capture_claude_transcript(&conn, &path).unwrap();
        reconcile(&conn, "claude").unwrap();

        let parents: Vec<(String, String, Option<String>)> = conn
            .prepare(
                "SELECT relationship, parent_session_id, origin_session_id \
                 FROM session_relationships WHERE child_session_id = 'continued' \
                 ORDER BY parent_session_id",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            parents,
            vec![(
                RELATIONSHIP_CONTINUATION.to_string(),
                "prior".to_string(),
                // The origin is kept on the edge that was written, so
                // skipping the source-only fork loses nothing.
                Some("origin".to_string())
            )],
            "exactly one parent, from the signal that says how it carried on"
        );
    }

    #[test]
    fn a_resolvable_parent_uuid_also_outranks_a_source_fork() {
        // The same rule for the inferred-but-resolvable signal: a transcript
        // whose first record answers an indexed record has been explained by
        // something that names the relationship, so the source is recorded as
        // the origin rather than as a second parent.
        let (_dir, conn) = database();
        let dir = tempfile::tempdir().unwrap();
        let (origin, _) = linked_pair(dir.path());
        ingest_file(&conn, &origin);
        let follower = dir.path().join("follower.jsonl");
        std::fs::write(
            &follower,
            concat!(
                "{\"sessionId\":\"follower\",\"uuid\":\"follow-u\",",
                "\"parentUuid\":\"origin-a\",\"type\":\"user\",",
                "\"sourceSessionId\":\"elsewhere\",",
                "\"message\":{\"role\":\"user\",\"content\":\"carry on\"},",
                "\"timestamp\":\"2026-08-31T11:00:00Z\"}\n",
            ),
        )
        .unwrap();
        ingest_file(&conn, &follower);

        let parents: Vec<(String, String, Option<String>)> = conn
            .prepare(
                "SELECT relationship, parent_session_id, origin_session_id \
                 FROM session_relationships WHERE child_session_id = 'follower' \
                 ORDER BY parent_session_id",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            parents,
            vec![(
                RELATIONSHIP_CONTINUATION.to_string(),
                "origin".to_string(),
                Some("elsewhere".to_string())
            )]
        );
    }

    #[test]
    fn a_resume_marker_also_outranks_a_source_fork() {
        let (_dir, conn) = database();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("resumer.jsonl");
        std::fs::write(
            &path,
            concat!(
                "{\"sessionId\":\"resumer\",\"uuid\":\"r-u\",\"parentUuid\":null,",
                "\"type\":\"user\",\"sourceSessionId\":\"origin\",",
                "\"message\":{\"role\":\"user\",\"content\":\"/resume prior\"},",
                "\"timestamp\":\"2026-08-31T10:00:00Z\"}\n",
            ),
        )
        .unwrap();
        capture_claude_transcript(&conn, &path).unwrap();
        reconcile(&conn, "claude").unwrap();

        let parents: Vec<(String, String, Option<String>)> = conn
            .prepare(
                "SELECT relationship, parent_session_id, origin_session_id \
                 FROM session_relationships WHERE child_session_id = 'resumer' \
                 ORDER BY parent_session_id",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            parents,
            vec![(
                RELATIONSHIP_RESUME.to_string(),
                "prior".to_string(),
                Some("origin".to_string())
            )]
        );
    }

    #[test]
    fn deleting_an_origin_leaves_its_continuation_recoverable() {
        // Deleting a session removes every relationship touching it, and the
        // dependent transcript's evidence row was left resolved — with the
        // edge that was the only way to rediscover that dependent now gone.
        // Rehydrating the origin would never rebuild the continuation.
        let (_dir, conn) = database();
        let dir = tempfile::tempdir().unwrap();
        let (origin, follower) = linked_pair(dir.path());
        ingest_file(&conn, &origin);
        ingest_file(&conn, &follower);
        assert_eq!(edges(&conn, "origin").len(), 1);
        conn.execute(
            "INSERT OR IGNORE INTO sessions (source, session_id) VALUES ('claude', 'origin')",
            [],
        )
        .unwrap();

        // The origin is deleted: its events and its edges go with it.
        conn.execute(
            "DELETE FROM sessions WHERE source = 'claude' AND session_id = 'origin'",
            [],
        )
        .unwrap();
        conn.execute(
            "DELETE FROM session_events WHERE source = 'claude' AND session_id = 'origin'",
            [],
        )
        .unwrap();
        assert!(edges(&conn, "origin").is_empty());

        // Rehydrating it brings the continuation back, because the dependent
        // was marked unreconciled before its edge was removed.
        ingest_file(&conn, &origin);
        assert_eq!(
            edges(&conn, "origin")
                .into_iter()
                .map(|edge| (edge.0, edge.1))
                .collect::<Vec<_>>(),
            vec![(
                RELATIONSHIP_CONTINUATION.to_string(),
                Some("follower".to_string())
            )],
            "the continuation is rebuilt from the dependent's banked evidence"
        );
    }

    #[test]
    fn deleting_an_unrelated_session_reopens_nothing() {
        // The positive control: the deletion trigger reopens the dependents of
        // the session that went away, not every row in the table.
        let (_dir, conn) = database();
        let dir = tempfile::tempdir().unwrap();
        let (origin, follower) = linked_pair(dir.path());
        ingest_file(&conn, &origin);
        ingest_file(&conn, &follower);
        assert_eq!(reconcile(&conn, "claude").unwrap().considered, 0);

        conn.execute(
            "INSERT OR IGNORE INTO sessions (source, session_id) VALUES ('claude', 'stranger')",
            [],
        )
        .unwrap();
        conn.execute(
            "DELETE FROM sessions WHERE source = 'claude' AND session_id = 'stranger'",
            [],
        )
        .unwrap();
        assert_eq!(
            reconcile(&conn, "claude").unwrap().considered,
            0,
            "nothing depended on the deleted session"
        );
        assert_eq!(edges(&conn, "origin").len(), 1);
    }

    #[test]
    fn explicit_lineage_is_not_joined_by_a_parent_uuid_that_resolves_later() {
        // A transcript with an explicit `continuedFromSessionId` *and* a
        // `parentUuid` nothing has indexed yet used to stay pending on that
        // uuid — and then gain a second parent the day some other session
        // turned out to hold it. The explicit field had already answered the
        // question the uuid was being asked.
        let (_dir, conn) = database();
        let dir = tempfile::tempdir().unwrap();
        let branch = dir.path().join("branch.jsonl");
        std::fs::write(
            &branch,
            concat!(
                "{\"sessionId\":\"branch\",\"uuid\":\"b-u\",\"parentUuid\":\"elsewhere-a\",",
                "\"type\":\"user\",\"continuedFromSessionId\":\"prior\",",
                "\"message\":{\"role\":\"user\",\"content\":\"carry on\"},",
                "\"timestamp\":\"2026-08-31T10:00:00Z\"}\n",
            ),
        )
        .unwrap();
        ingest_file(&conn, &branch);

        let parents = |conn: &Connection| -> Vec<(String, String)> {
            conn.prepare(
                "SELECT relationship, parent_session_id FROM session_relationships \
                 WHERE child_session_id = 'branch' ORDER BY parent_session_id",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
        };
        let one_parent = vec![(RELATIONSHIP_CONTINUATION.to_string(), "prior".to_string())];
        assert_eq!(parents(&conn), one_parent);
        assert!(
            pending_reasons(&conn, "claude", "branch").unwrap().is_empty(),
            "the uuid was never needed, so nothing is waiting on it"
        );

        // The uuid turns up later, held by an unrelated session.
        let elsewhere = dir.path().join("elsewhere.jsonl");
        std::fs::write(
            &elsewhere,
            concat!(
                "{\"sessionId\":\"elsewhere\",\"uuid\":\"elsewhere-a\",\"parentUuid\":null,",
                "\"type\":\"assistant\",\"message\":{\"role\":\"assistant\",\"content\":\"hi\"},",
                "\"timestamp\":\"2026-08-31T09:00:00Z\"}\n",
            ),
        )
        .unwrap();
        ingest_file(&conn, &elsewhere);
        assert_eq!(
            parents(&conn),
            one_parent,
            "still one parent: the explicit field settled it"
        );
    }

    #[test]
    fn a_parent_uuid_that_resolves_later_still_answers_source_only_evidence() {
        // The positive control for the skip above: evidence carrying only
        // `sourceSessionId` has named an origin but not how the conversation
        // carried on, so the uuid lookup still runs — and when it resolves,
        // the continuation replaces the weaker source fork.
        let (_dir, conn) = database();
        let dir = tempfile::tempdir().unwrap();
        let branch = dir.path().join("branch.jsonl");
        std::fs::write(
            &branch,
            concat!(
                "{\"sessionId\":\"branch\",\"uuid\":\"b-u\",\"parentUuid\":\"origin-a\",",
                "\"type\":\"user\",\"sourceSessionId\":\"origin\",",
                "\"message\":{\"role\":\"user\",\"content\":\"carry on\"},",
                "\"timestamp\":\"2026-08-31T10:00:00Z\"}\n",
            ),
        )
        .unwrap();
        ingest_file(&conn, &branch);
        // Nothing holds `origin-a` yet, so the source fork stands in.
        assert_eq!(
            edges(&conn, "origin")
                .into_iter()
                .map(|edge| edge.0)
                .collect::<Vec<_>>(),
            vec![RELATIONSHIP_FORK.to_string()]
        );

        let (origin, _) = linked_pair(dir.path());
        ingest_file(&conn, &origin);
        assert_eq!(
            edges(&conn, "origin")
                .into_iter()
                .map(|edge| (edge.0, edge.3))
                .collect::<Vec<_>>(),
            vec![(RELATIONSHIP_CONTINUATION.to_string(), "origin-a".to_string())],
            "the resolved uuid replaced the source fork rather than joining it"
        );
    }

    #[test]
    fn a_rewritten_origin_that_drops_a_record_leaves_the_row_indexed() {
        // Characterizing, not asserting a desired outcome. Re-reading a Claude
        // transcript upserts the records it now contains and removes nothing,
        // so a record deleted from the file keeps its `session_events` row.
        // This is the ingest replacement semantics on main, and it is what
        // `session_holding_record` — and `getSessionEventsPage`, and the tool
        // call and file edit pages, and the hydration evidence counts — all
        // read afterwards.
        let (_dir, conn) = database();
        let dir = tempfile::tempdir().unwrap();
        let (origin, follower) = linked_pair(dir.path());
        ingest_file(&conn, &origin);
        ingest_file(&conn, &follower);
        assert_eq!(edges(&conn, "origin").len(), 1);

        // The origin is rewritten without its assistant record.
        std::fs::write(
            &origin,
            concat!(
                "{\"sessionId\":\"origin\",\"uuid\":\"origin-u\",\"parentUuid\":null,",
                "\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"start\"},",
                "\"timestamp\":\"2026-08-31T10:00:00Z\"}\n",
            ),
        )
        .unwrap();
        ingest_file(&conn, &origin);

        let still_indexed: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_events \
                 WHERE source = 'claude' AND message_id = 'origin-a'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            still_indexed, 1,
            "ingest replaces what a transcript contains and retracts nothing"
        );
        assert_eq!(
            session_holding_record(&conn, "claude", "origin-a")
                .unwrap()
                .as_deref(),
            Some("origin"),
            "so the record still resolves, and the continuation is restored"
        );
        assert_eq!(edges(&conn, "origin").len(), 1);
    }

    #[test]
    fn one_session_id_is_held_by_more_than_one_transcript() {
        // The positive control for the test above: retracting "the rows of
        // this session that this file no longer contains" is not a deletion
        // anyone can scope by session. A forked conversation writes two files
        // under one `sessionId`, and re-reading either one would delete every
        // record the other contributed — the corpus this feature exists to
        // read. A retraction needs a per-row owner, which `session_events`
        // does not carry.
        let (_dir, conn) = database();
        ingest(&conn, "fork-branch-a.jsonl");
        ingest(&conn, "fork-branch-b.jsonl");
        let held = |conn: &Connection| -> Vec<String> {
            conn.prepare(
                "SELECT message_id FROM session_events \
                 WHERE source = 'claude' AND session_id = ?1 AND message_id IS NOT NULL \
                 ORDER BY message_id",
            )
            .unwrap()
            .query_map([SHARED_FORK], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
        };
        assert_eq!(
            held(&conn),
            vec![
                "u-fork-a-1".to_string(),
                "u-fork-a-asst".to_string(),
                "u-fork-b-1".to_string(),
                "u-fork-b-asst".to_string(),
            ]
        );

        // Re-reading branch A changes nothing, precisely because ingest only
        // upserts what the file contains.
        ingest(&conn, "fork-branch-a.jsonl");
        assert_eq!(held(&conn).len(), 4);
        assert_eq!(edges(&conn, SHARED_FORK).len(), 2);
    }
}
