//! One sync of Codex's local sources: the rollouts, the thread-name index,
//! and the prompts in `history.jsonl`.

use super::*;

pub(super) fn sync_codex_sources(
    conn: &Connection,
    state: &mut Map<String, Value>,
    root: &Path,
    repairs: &SweepRepairs,
    coverage: &mut SweepCoverage,
    touched: &mut HashSet<String>,
) -> Result<usize> {
    let (cwds, branches, mut inserted) = sync_codex_rollouts_with_repairs_and_coverage(
        conn, state, root, repairs, coverage, touched,
    )?;
    let named = super::codex_thread_names::apply_codex_thread_names(conn, state, root, touched)?;
    if named > 0 {
        sync_note!("  [codex] named {named} threads");
    }
    let path = root.join("history.jsonl");
    if !path.exists() {
        sync_note!("  [codex] not found: {} (skipped)", path.display());
        // The rows the backfill fills are already in the database; a missing
        // log only means no new ones. Sessions a rollout re-read or an earlier
        // sweep left owed still get their pass, or clearing them as pending
        // would lose them.
        backfill_codex_metadata_scoped(conn, state, &cwds, &branches, touched)?;
        return Ok(inserted);
    }
    let mut source = CompleteJsonlReader::open(&path, state.get("codex"))?;
    let offset = source.position;
    let size = source.reader.get_ref().metadata()?.len();
    let mut errors = 0;
    let mut consumed = offset;
    if offset < size {
        sync_note!("  [codex] syncing {} new bytes...", size - offset);
        let read = ingest_codex_history_lines(conn, &mut source, &cwds, touched)?;
        consumed = read.consumed;
        inserted += read.inserted;
        errors = read.errors;
    }
    // This also upgrades legacy numeric cursors at EOF. The actual committed
    // position may include complete lines appended after the initial stat.
    let opened_cursor = source.cursor.to_value();
    if consumed != offset || state.get("codex") != Some(&opened_cursor) {
        state.insert(
            "codex".to_string(),
            source.committed_cursor(consumed, true)?.to_value(),
        );
    }
    let backfilled = backfill_codex_metadata_scoped(conn, state, &cwds, &branches, touched)?;
    if consumed == offset && backfilled == 0 {
        sync_note!("  [codex] up to date");
    } else {
        let mut parts = Vec::new();
        if inserted > 0 || consumed > offset {
            parts.push(format!("+{inserted} rows"));
        }
        if backfilled > 0 {
            parts.push(format!("backfilled {backfilled} project/branch values"));
        }
        if errors > 0 {
            parts.push(format!("{errors} errors"));
        }
        sync_note!("  [codex] {}", parts.join(", "));
    }
    Ok(inserted)
}

/// What one pass over the new lines of Codex's `history.jsonl` did.
struct CodexHistoryRead {
    /// The position after the last line read.
    consumed: u64,
    inserted: usize,
    errors: usize,
}

/// Inserts every prompt in the unread tail of Codex's `history.jsonl`, naming
/// a prompt's project from its session's rollout when the log does not.
fn ingest_codex_history_lines(
    conn: &Connection,
    source: &mut CompleteJsonlReader,
    cwds: &HashMap<String, String>,
    touched: &mut HashSet<String>,
) -> Result<CodexHistoryRead> {
    let mut read = CodexHistoryRead {
        consumed: source.position,
        inserted: 0,
        errors: 0,
    };
    let mut line = String::new();
    while let Some(position) = source.next_line(&mut line)? {
        read.consumed = position;
        if line.trim().is_empty() {
            continue;
        }
        match parse_codex_line(&line) {
            Ok(Some(mut entry)) => {
                if let Some(session_id) = entry.session_id.as_deref() {
                    if entry.project.is_none() {
                        entry.project = cwds.get(session_id).cloned();
                    }
                    touched.insert(session_id.to_string());
                }
                read.inserted += insert_history(conn, &entry)?;
            }
            Ok(None) => {}
            Err(_) => read.errors += 1,
        }
    }
    Ok(read)
}
