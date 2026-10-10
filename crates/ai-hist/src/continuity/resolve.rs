use anyhow::Result;
use rusqlite::Connection;
use std::collections::BTreeSet;

use super::parse::named_at;
use super::storage::{fork_group, session_holding_record, write_edge, ContinuityEdge};
use super::{
    ContinuityEvidence, REF_CONTINUED_FROM, REF_FORK_SESSION, REF_RESUME_MARKER,
    REF_SHARED_SESSION_ID, REF_SOURCE_SESSION,
};
use crate::relationships::{RELATIONSHIP_CONTINUATION, RELATIONSHIP_FORK, RELATIONSHIP_RESUME};

// ---------------------------------------------------------------------------
// Resolution steps
// ---------------------------------------------------------------------------

/// Explicit provider fields. These name the origin outright, so they never
/// wait on anything and never produce a pending reason.
pub(super) fn resolve_explicit(
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
            &ContinuityEdge {
                relationship: RELATIONSHIP_CONTINUATION,
                parent_session_id: target,
                child_session_id: Some(evidence.session_id.as_str()),
                evidence_ref: REF_CONTINUED_FROM,
                origin_session_id: evidence.explicit_source_session_id.as_deref(),
                spawned_at_ms: named_at(&evidence.explicit_continuation_ts_ms, target, evidence),
            },
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
        let evidence_ref = evidence
            .explicit_fork_refs
            .get(target)
            .map_or(REF_FORK_SESSION, String::as_str);
        written.push(write_edge(
            conn,
            evidence,
            &ContinuityEdge {
                relationship: RELATIONSHIP_FORK,
                parent_session_id: target,
                child_session_id: Some(evidence.session_id.as_str()),
                evidence_ref,
                origin_session_id: Some(origin),
                spawned_at_ms: named_at(&evidence.explicit_fork_ts_ms, target, evidence),
            },
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
pub(super) fn resolve_explicit_source(
    conn: &Connection,
    evidence: &ContinuityEvidence,
    written: &mut Vec<(String, String)>,
) -> Result<()> {
    if !written.is_empty() {
        return Ok(());
    }
    let Some(origin) = evidence.explicit_origin() else {
        return Ok(());
    };
    written.push(write_edge(
        conn,
        evidence,
        &ContinuityEdge {
            relationship: RELATIONSHIP_FORK,
            parent_session_id: origin,
            child_session_id: Some(evidence.session_id.as_str()),
            evidence_ref: REF_SOURCE_SESSION,
            origin_session_id: Some(origin),
            spawned_at_ms: evidence.first_ts_ms,
        },
    )?);
    Ok(())
}

/// A `/resume <id>` or `/continue <id>` the human typed.
///
/// A marker naming no session does not establish lineage. Another transcript
/// cannot supply its missing target, so it is neither an edge nor pending work.
pub(super) fn resolve_resume(
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
        &ContinuityEdge {
            relationship: RELATIONSHIP_RESUME,
            parent_session_id: target,
            child_session_id: Some(evidence.session_id.as_str()),
            evidence_ref: REF_RESUME_MARKER,
            // The explicit origin when the file names one, so skipping the
            // source-only fork never loses it.
            origin_session_id: evidence.explicit_origin().or(Some(target)),
            spawned_at_ms: evidence.first_ts_ms,
        },
    )?);
    Ok(())
}

/// The transcript answers a record it does not contain.
///
/// The uuid is resolved against the events already indexed, so the answer is
/// whichever session actually holds that record — never a file name, and never
/// a guess. A uuid nothing has indexed yet is left pending with the uuid in
/// the reason, which is what makes hydrating the origin afterwards enough.
pub(super) fn resolve_cross_file_parent(
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
        &ContinuityEdge {
            relationship: RELATIONSHIP_CONTINUATION,
            parent_session_id: &parent_session_id,
            child_session_id: Some(evidence.session_id.as_str()),
            evidence_ref: parent_uuid,
            origin_session_id: evidence.explicit_source_session_id.as_deref(),
            spawned_at_ms: evidence.first_ts_ms,
        },
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
pub(super) fn resolve_fork_group(
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
    if !evidence.explicit_fork_targets.is_empty() || evidence.explicit_origin().is_some() {
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
            &ContinuityEdge {
                relationship: RELATIONSHIP_FORK,
                parent_session_id: &origin,
                child_session_id: child,
                evidence_ref: REF_SHARED_SESSION_ID,
                origin_session_id: Some(origin.as_str()),
                spawned_at_ms: member.first_ts_ms,
            },
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
