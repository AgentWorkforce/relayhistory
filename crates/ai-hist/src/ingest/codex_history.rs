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
    paths: Option<&super::path_scope::PathScope>,
) -> Result<usize> {
    let inserted =
        sync_codex_rollouts_and_history(conn, state, root, repairs, coverage, touched, paths)?;
    // Last, after every phase that can create a Codex row (the history
    // backfill included): a name applied before its row exists would be
    // recorded as applied and never land.
    let named = super::codex_thread_names::apply_codex_thread_names(conn, state, root, touched)?;
    if named > 0 {
        sync_note!("  [codex] named {named} threads");
    }
    Ok(inserted)
}

fn sync_codex_rollouts_and_history(
    conn: &Connection,
    state: &mut Map<String, Value>,
    root: &Path,
    repairs: &SweepRepairs,
    coverage: &mut SweepCoverage,
    touched: &mut HashSet<String>,
    paths: Option<&super::path_scope::PathScope>,
) -> Result<usize> {
    // A full walk of either rollout root reads its evidence keys once. A
    // path-scoped tick keeps the point probe.
    let sessions = root.join("sessions");
    let archived = root.join("archived_sessions");
    let _guard = super::evidence_probe::Guard::arm_when_full(conn, paths, &[sessions, archived]);
    let (cwds, branches, mut inserted) = sync_codex_rollouts_with_repairs_and_coverage(
        conn, state, root, repairs, coverage, touched, paths,
    )?;
    match sync_codex_sqlite_scoped(conn, state, root, repairs, coverage, touched, paths) {
        Ok(sqlite_inserted) => inserted += sqlite_inserted,
        Err(error) if error.is::<CaptureCancelled>() => return Err(error),
        Err(error) => {
            coverage.note_unread();
            sync_note!("  [codex] SQLite history unavailable: {error:#}");
        }
    }
    let path = root.join("history.jsonl");
    // One incremental file. A tick whose paths are rollouts has nothing new
    // in it, and opening it on every append is a read the backstop still does.
    if !super::path_scope::file_in_scope(paths, &path) {
        backfill_codex_metadata_scoped(conn, state, &cwds, &branches, touched)?;
        return Ok(inserted);
    }
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

fn sync_codex_sqlite_scoped(
    conn: &Connection,
    state: &mut Map<String, Value>,
    root: &Path,
    repairs: &SweepRepairs,
    coverage: &mut SweepCoverage,
    touched: &mut HashSet<String>,
    paths: Option<&super::path_scope::PathScope>,
) -> Result<usize> {
    let selection = crate::providers::codex::sqlite::database_selection(root)?;
    if let Some(diagnostic) = selection.diagnostic {
        sync_note!("  [codex] skipping SQLite history: {diagnostic}");
    }
    let Some((state_db, history_db)) = selection.pair else {
        return Ok(0);
    };
    let changed = paths.is_none()
        || !repairs.is_empty()
        || paths.is_some_and(|scope| {
            scope.mentions(&root.join("sessions"))
                || scope.mentions(&root.join("archived_sessions"))
        })
        || [state_db, history_db].iter().any(|file| {
            ["", "-wal", "-shm", "-journal"].iter().any(|suffix| {
                super::path_scope::file_in_scope(
                    paths,
                    &PathBuf::from(format!("{}{suffix}", file.display())),
                )
            })
        });
    if changed {
        let (inserted, read) = crate::providers::codex::sqlite::sync(conn, state, root, repairs)?;
        for id in read {
            coverage.note_read("codex", &id, true);
            touched.insert(id);
        }
        Ok(inserted)
    } else {
        Ok(0)
    }
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
                let inserted = insert_history(conn, &entry)?;
                if inserted == 0 {
                    crate::providers::codex::sqlite::claim_history_log_prompt(conn, &entry)?;
                }
                read.inserted += inserted;
            }
            Ok(None) => {}
            Err(_) => read.errors += 1,
        }
    }
    Ok(read)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discover::{CodexProvider, DiscoveryEnv, ShallowSessionProvider};

    #[test]
    fn newer_sqlite_schema_skips_fallback_without_losing_rollouts() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join(".codex");
        std::fs::create_dir_all(&root).unwrap();
        for name in [
            "state_5.sqlite",
            "thread_history_1.sqlite",
            "state_6.sqlite",
        ] {
            std::fs::write(root.join(name), b"new schema").unwrap();
        }
        let rollout = root.join("sessions/2026/04/20/rollout-primary.jsonl");
        std::fs::create_dir_all(rollout.parent().unwrap()).unwrap();
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/codex/sqlite/rollout-rollout-primary.jsonl");
        std::fs::copy(fixture, &rollout).unwrap();

        let selection = crate::providers::codex::sqlite::database_selection(&root).unwrap();
        assert!(selection.pair.is_none());
        assert!(selection.diagnostic.unwrap().contains("state_6.sqlite"));

        let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
        let mut roots = crate::ProviderRoots::from_home(
            dir.path().to_path_buf(),
            dir.path().join("opencode.db"),
        );
        roots.codex = root.clone();
        let env = DiscoveryEnv::with_provider_roots(&conn, roots);
        let candidates = CodexProvider::default().enumerate(&env, None).unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].locator, rollout.to_string_lossy());

        let mut state = Map::new();
        sync_codex_sources(
            &conn,
            &mut state,
            &root,
            &SweepRepairs::default(),
            &mut SweepCoverage::default(),
            &mut HashSet::new(),
            None,
        )
        .unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sessions WHERE source='codex' AND session_id='sqlite-rollout-primary'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn unreadable_sqlite_tables_do_not_hide_rollouts_or_history_log() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join(".codex");
        std::fs::create_dir_all(&root).unwrap();
        Connection::open(root.join("state_5.sqlite")).unwrap();
        Connection::open(root.join("thread_history_1.sqlite")).unwrap();
        let rollout = root.join("sessions/2026/04/20/rollout-primary.jsonl");
        std::fs::create_dir_all(rollout.parent().unwrap()).unwrap();
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/codex/sqlite/rollout-rollout-primary.jsonl");
        std::fs::copy(fixture, &rollout).unwrap();
        std::fs::write(
            root.join("history.jsonl"),
            "{\"text\":\"history-only\",\"ts\":1776643300,\"sessionId\":\"sqlite-rollout-primary\"}\n",
        ).unwrap();

        let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
        let mut roots = crate::ProviderRoots::from_home(
            dir.path().to_path_buf(),
            dir.path().join("opencode.db"),
        );
        roots.codex = root.clone();
        let env = DiscoveryEnv::with_provider_roots(&conn, roots);
        let candidates = CodexProvider::default().enumerate(&env, None).unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].locator, rollout.to_string_lossy());

        let mut state = Map::new();
        let mut coverage = SweepCoverage::default();
        sync_codex_sources(
            &conn,
            &mut state,
            &root,
            &SweepRepairs::default(),
            &mut coverage,
            &mut HashSet::new(),
            None,
        )
        .unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT count(*) FROM history WHERE source='codex' AND prompt='history-only'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn a_history_log_duplicate_survives_sqlite_projection_pruning() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join(".codex");
        std::fs::create_dir_all(&root).unwrap();
        let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/codex/sqlite");
        for (script, database) in [
            ("pruned-all-items-state.sql", "state_5.sqlite"),
            ("pruned-all-items-history.sql", "thread_history_1.sqlite"),
        ] {
            let sql = std::fs::read_to_string(fixtures.join(script))
                .unwrap()
                .replace("{{CODEX_HOME}}", &root.to_string_lossy());
            Connection::open(root.join(database))
                .unwrap()
                .execute_batch(&sql)
                .unwrap();
        }
        std::fs::write(
            root.join("history.jsonl"),
            serde_json::json!({"session_id":"sqlite-pruned-all-items","ts":1776643200,"text":"Please inspect the project"}).to_string() + "\n",
        ).unwrap();
        let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
        let mut state = Map::new();
        let mut coverage = SweepCoverage::default();
        let mut touched = HashSet::new();
        let repairs = SweepRepairs::default();
        sync_codex_sources(
            &conn,
            &mut state,
            &root,
            &repairs,
            &mut coverage,
            &mut touched,
            None,
        )
        .unwrap();
        Connection::open(root.join("thread_history_1.sqlite"))
            .unwrap()
            .execute(
                "DELETE FROM thread_items WHERE item_id='sqlite-sqlite-pruned-all-items-0'",
                [],
            )
            .unwrap();
        Connection::open(root.join("state_5.sqlite")).unwrap().execute(
            "UPDATE threads SET updated_at_ms=updated_at_ms+1 WHERE id='sqlite-pruned-all-items'", [],
        ).unwrap();
        sync_codex_sources(
            &conn,
            &mut state,
            &root,
            &repairs,
            &mut coverage,
            &mut touched,
            None,
        )
        .unwrap();
        let count: i64 = conn.query_row(
            "SELECT count(*) FROM history WHERE source='codex' AND prompt='Please inspect the project'",
            [], |row| row.get(0),
        ).unwrap();
        assert_eq!(
            count, 1,
            "the consumed history.jsonl prompt remains searchable"
        );
    }
}
