//! Selected Codex hydration for rollout and SQLite layouts.
use super::*;

pub(crate) fn ingest_selected_codex(ctx: SelectedIngest<'_>) -> Result<SelectedIngestResult> {
    if let Some(acquired) = ctx.provider_session {
        let capture = crate::providers::codex::sqlite::decode(acquired)?;
        let path = ctx
            .path
            .ok_or_else(|| anyhow::anyhow!("Codex SQLite capture has no state path"))?;
        crate::providers::codex::sqlite::normalize(ctx.conn, &capture, path)?;
        return Ok((
            IngestOutcome {
                records: acquired.records,
                ..Default::default()
            },
            Vec::new(),
            None,
        ));
    }
    let path = ctx.file();
    ingest_codex(ctx.conn, ctx.options, path, ctx.cursor, ctx.parser_upgrade)
        .map(|outcome| (outcome, Vec::new(), None))
}

fn ingest_codex(
    conn: &Connection,
    options: &HydrateSessionOptions,
    path: &Path,
    cursor: &mut TranscriptCursorState,
    parser_upgrade: bool,
) -> Result<IngestOutcome> {
    let (meta, meta_bytes) = read_codex_session_meta_counted(path)?;
    let meta = meta.ok_or_else(|| {
        hydration_error(
            "SESSION_SOURCE_MISMATCH",
            "Codex rollout has no session metadata",
        )
    })?;
    if meta.session_id != options.session_id || meta.is_subagent {
        return Err(hydration_error(
            "SESSION_SOURCE_MISMATCH",
            "Codex rollout identity does not match the selected root session",
        ));
    }
    let mut indexed = IngestOutcome {
        bytes_read: meta_bytes as i64,
        ..Default::default()
    };
    let (outcome, pass) = ingest_codex_rollout_incremental(conn, path, &meta, cursor)?;
    indexed.absorb(pass);
    if let Some(first) = outcome.first_ts {
        upsert_session(
            conn,
            &SessionCatalogRow {
                session_id: &meta.session_id,
                source: "codex",
                cwd: Some(&meta.cwd),
                git_branch: meta.git_branch.as_deref(),
                first_ts: first,
                last_ts: outcome.last_ts.unwrap_or(first),
                last_assistant_text: outcome.last_assistant_text.as_deref(),
                raw_path: Some(&path.to_string_lossy()),
            },
        )?;
    }
    if options.include_related {
        // Codex records continuity only when a producer writes explicit
        // fields on `session_meta`; a plain `codex resume` leaves no signal.
        indexed.bytes_read += crate::continuity::capture_codex_rollout(conn, path)? as i64;
        // Each child's own continuity is captured with it (a spawned
        // subagent's `thread_spawn` parent is a fork edge that lives only in
        // the child's `session_meta`), so reconcile once they are all banked.
        indexed.absorb_outcome(ingest_codex_children(conn, options, path, parser_upgrade)?);
        crate::continuity::reconcile(conn, "codex")?;
    }
    Ok(indexed)
}
