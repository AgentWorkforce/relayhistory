use super::*;

/// Read what the Grok home's `logs/unified.jsonl` gained, before a Grok
/// hydration decides whether its session changed.
///
/// The log lives outside the session directory, so the session's stamp never
/// moves when it grows: reading it only on the parse path meant an
/// `unchanged` hydration never attached an inference appended after the
/// session's last write. It runs in its own transaction, before the
/// hydration's, and whatever it attaches clears the touched sessions' cached
/// diagnostics so an `unchanged` result is rebuilt from the stored rows.
///
/// The log is every session's, so failing to read it — unreadable, replaced
/// mid-read — is not a reason to fail this session's hydration. It is
/// reported as `GROK_UNIFIED_LOG_UNREADABLE` and the hydration goes on with
/// whatever the log had already given; the cursor has not moved, so the next
/// pass tries again.
///
/// Any other provider's hydration has no such log and reads nothing.
pub(super) fn read_grok_unified_log_for_hydration(
    conn: &Connection,
    options: &HydrateSessionOptions,
    grok_home: &Path,
) -> Result<Vec<HydrationDiagnostic>> {
    if options.source != "grok" {
        return Ok(Vec::new());
    }
    let pass = if crate::retention::grok_usage_replay_needed(conn, &options.session_id)? {
        crate::ingest::grok_unified::replay_for_hydration(conn, grok_home, &options.session_id)
    } else {
        crate::ingest::sync_grok_unified_log(conn, grok_home)
    };
    match pass {
        Ok(_) => Ok(Vec::new()),
        Err(error)
            if error.is::<crate::ingest::CaptureCancelled>() || !error.is::<std::io::Error>() =>
        {
            Err(error)
        }
        Err(error) => Ok(vec![HydrationDiagnostic {
            code: "GROK_UNIFIED_LOG_UNREADABLE".to_string(),
            message: format!(
                "grok's logs/unified.jsonl could not be read, so any usage it gained since the \
                 last pass is not attached yet: {error:#}"
            ),
            duration_ms: None,
            source_bytes: None,
            records_parsed: None,
        }]),
    }
}
