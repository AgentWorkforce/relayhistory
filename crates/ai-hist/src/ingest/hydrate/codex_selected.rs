//! Selected Codex hydration for rollout and SQLite layouts.
use super::*;

/// A saved rollout observation can become stale after a projection catches up
/// and the rollout is pruned again. Refresh its locator before acquisition.
pub(super) fn refresh_catalog_codex_locator(
    options: &HydrateSessionOptions,
    roots: &crate::ProviderRoots,
    target: &mut CatalogTarget,
    observation: &mut Option<SessionObservation>,
) -> Result<()> {
    if target
        .locator
        .as_deref()
        .is_some_and(|locator| crate::providers::codex::sqlite::parse_locator(locator).is_none())
    {
        let current = crate::providers::codex::sqlite::current_locator(
            roots,
            &options.session_id,
            target.locator.as_deref().unwrap_or_default(),
        );
        let current = match current {
            Ok(current) => current,
            Err(error) if error.is::<crate::ingest::CaptureCancelled>() => return Err(error),
            Err(error) => {
                crate::ingest::provider_diagnostics::provider_diagnostic(
                    crate::ingest::provider_diagnostics::ProviderSkip {
                        source: crate::Source::Codex,
                        code: "SESSION_SOURCE_UNAVAILABLE",
                        subject: Some(&options.session_id),
                        what: format_args!("skipping SQLite locator refresh"),
                        detail: format_args!("{error:#}"),
                    },
                );
                None
            }
        };
        if let Some(current) = current {
            target.locator = Some(current);
            if let Some(observation) = observation.as_mut() {
                observation.raw_locator = target.locator.clone();
            }
        }
    }
    Ok(())
}

pub(crate) fn ingest_selected_codex(ctx: SelectedIngest<'_>) -> Result<SelectedIngestResult> {
    if let Some(acquired) = ctx.provider_session {
        let capture = crate::providers::codex::sqlite::decode(acquired)?;
        let path = ctx
            .path
            .ok_or_else(|| anyhow::anyhow!("Codex SQLite capture has no state path"))?;
        crate::providers::codex::sqlite::retire_if_changed(
            ctx.conn,
            &capture,
            ctx.options.include_related,
        )?;
        crate::providers::codex::sqlite::normalize_with_relationships(
            ctx.conn,
            &capture,
            path,
            ctx.options.include_related,
        )?;
        if ctx.options.include_related {
            for child in &capture.related {
                crate::providers::codex::sqlite::retire_if_changed(ctx.conn, child, true)?;
                crate::providers::codex::sqlite::normalize(ctx.conn, child, path)?;
            }
            for child in &capture.related_rollouts {
                ingest_related_codex_rollout(ctx.conn, child)?;
            }
            // Parent edges precede both SQLite and rollout child events.
            ctx.conn.execute(
                "UPDATE session_relationships SET child_has_events=1 \
                 WHERE source='codex' AND parent_session_id=?1 AND child_has_events=0 \
                 AND EXISTS (SELECT 1 FROM session_events \
                             WHERE source='codex' AND session_id=child_session_id)",
                [&capture.thread.id],
            )?;
        }
        return Ok((
            IngestOutcome {
                records: acquired.records,
                ..Default::default()
            },
            Vec::new(),
            None,
        ));
    }
    let path = ctx.file()?;
    ingest_codex(ctx.conn, ctx.options, path, ctx.cursor, ctx.parser_upgrade)
        .map(|outcome| (outcome, Vec::new(), None))
}

/// Index one related rollout a SQLite capture named, under the identity the
/// capture recorded for it.
fn ingest_related_codex_rollout(
    conn: &Connection,
    child: &crate::providers::codex::sqlite::RelatedRollout,
) -> Result<()> {
    let meta = read_codex_session_meta(&child.path)?
        .ok_or_else(|| anyhow::anyhow!("Codex related rollout has no metadata"))?;
    anyhow::ensure!(
        meta.session_id == child.id,
        "SESSION_SOURCE_MISMATCH: Codex related rollout identity changed"
    );
    let outcome = ingest_codex_rollout_at_locator(conn, &child.path, &meta, false)?;
    if meta.is_subagent {
        cleanup_codex_subagent_history(conn, &meta.session_id)?;
        cleanup_codex_subagent_registration(conn, &meta.session_id)?;
        if let Some(parent) = meta.parent_session_id.as_deref() {
            record_codex_delegation(conn, parent, &meta, &child.path)?;
        }
    } else if let Some(first) = outcome.first_ts {
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
                raw_path: Some(&child.path.to_string_lossy()),
            },
        )?;
    }
    Ok(())
}

fn ingest_codex(
    conn: &Connection,
    options: &HydrateSessionOptions,
    path: &Path,
    cursor: &mut TranscriptCursorState,
    parser_upgrade: bool,
) -> Result<IngestOutcome> {
    let exact = super::super::path_capture::capturing_exact_paths()
        .then(|| crate::continuity::CaptureScope::begin(conn, "codex", path))
        .transpose()?;

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
    if options.include_related || exact.is_some() {
        // Codex records continuity only when a producer writes explicit
        // fields on `session_meta`; a plain `codex resume` leaves no signal.
        indexed.bytes_read += crate::continuity::capture_codex_rollout(conn, path)? as i64;
        // Each child's own continuity is captured with it (a spawned
        // subagent's `thread_spawn` parent is a fork edge that lives only in
        // the child's `session_meta`), so reconcile once they are all banked.
        if options.include_related {
            indexed.absorb_outcome(ingest_codex_children(conn, options, path, parser_upgrade)?);
        }
        if let Some(exact) = exact {
            crate::continuity::reconcile_captured(conn, "codex", path, exact)?;
        } else {
            crate::continuity::reconcile(conn, "codex")?;
        }
    }
    Ok(indexed)
}
