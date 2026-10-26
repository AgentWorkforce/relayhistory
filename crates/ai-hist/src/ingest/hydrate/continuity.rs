//! Primary continuity capture independent of family enumeration.
use super::*;

pub(super) fn capture_claude(
    conn: &Connection,
    options: &HydrateSessionOptions,
    path: &Path,
    meta: &ClaudeSessionMeta,
    exact: Option<crate::continuity::CaptureScope>,
    snapshot: Option<&ClaudeTranscriptSnapshot>,
    scan: (bool, Option<crate::continuity::ContinuityEvidence>),
) -> Result<()> {
    let (scan_superseded, scan_continuity) = scan;
    record_claude_remote_relationship(conn, meta, options.include_related)?;
    if options.include_related || exact.is_some() {
        capture_claude_continuity(conn, path, snapshot, scan_superseded, scan_continuity)?;
        if let Some(exact) = exact {
            crate::continuity::reconcile_captured(conn, "claude", path, exact)?;
        } else {
            crate::continuity::reconcile(conn, "claude")?;
        }
    }
    Ok(())
}

/// Record the transcript's continuity evidence: folded from the hook's text,
/// or the metadata walk's fold unless the walk was superseded.
fn capture_claude_continuity(
    conn: &Connection,
    path: &Path,
    snapshot: Option<&ClaudeTranscriptSnapshot>,
    scan_superseded: bool,
    scan_continuity: Option<crate::continuity::ContinuityEvidence>,
) -> Result<()> {
    match snapshot {
        Some(snapshot) => {
            crate::continuity::capture_claude_transcript_text(conn, path, &snapshot.text)
        }
        None if !scan_superseded => crate::continuity::capture_folded(conn, path, scan_continuity),
        None => Ok(()),
    }
}
