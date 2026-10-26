//! Provider-owned exact transcript acquisition without global sync state.
use super::*;
pub(crate) mod cold_proof;
use crate::observations;

/// One local sweep for [`crate::SessionStore`]: silent, against the store's
/// resolved provider roots, forcing past the source fingerprint when asked,
/// with two reads bracketing the sweep **inside the locked section**.
///
/// `before` runs once the `SyncRunLock` is held and before anything is
/// written; `after` runs when the sweep has finished, on the same connection,
/// still under the lock. That is what lets the facade say what *this* sweep
/// changed: a baseline taken before the lock would also count whatever
/// another sync wrote while this call waited for it. `None` means the lock
/// was held elsewhere and nothing ran — the facade turns that into an error
/// rather than a silent no-op. `scope` names the sources the sweep reads;
/// see [`SweepScope`] for what a scoped sweep leaves alone.
pub(crate) fn sync_facade_tick<B, R>(
    db_path: &Path,
    roots: &crate::ProviderRoots,
    force: bool,
    scope: &SweepScope,
    before: impl FnOnce(&Connection) -> Result<B>,
    after: impl FnOnce(&Connection, B, SyncTick, bool) -> Result<R>,
) -> Result<Option<(SyncTick, R, SyncRunLock)>> {
    SYNC_QUIET.store(true, AtomicOrdering::Relaxed);
    check_capture_cancelled()?;
    let Some(sync_lock) = try_acquire_sync_lock(db_path)? else {
        return Ok(None);
    };
    let _reads = cold_read::begin_read_scope();
    crate::store::sync_connection::with_connection(db_path, |conn| {
        // Keep setup diagnostics separate from provider errors: legacy full
        // sync already enriches its own contention, and probing twice would
        // add waits without changing the failed acquisition.
        Ok((|| {
            let baseline = before(conn)?;
            let captured = try_capture_paths(conn, db_path, roots, scope, force).and_then(
                |scoped| match scoped {
                    Some(swept) => Ok(swept),
                    None => sync_basic(conn, db_path, roots, force, scope)
                        .map_err(|error| enrich_sync_error(db_path, error)),
                },
            );
            let tick = SyncTick {
                attempted: true,
                swept: captured.as_ref().copied().unwrap_or(false),
            };
            // A cancelled/failed acquisition can already have committed earlier
            // files. Capture that bracket before unlocking, retaining its delta for
            // the next successful report instead of swallowing it as an outside write.
            let outcome = after(conn, baseline, tick, captured.is_ok());
            captured?;
            let outcome = outcome?;
            Ok(Some((tick, outcome, sync_lock)))
        })())
    })
    .map_err(|error| enrich_sync_error(db_path, error))?
}

pub(crate) fn reconcile_pending(db: &Path, source: &str) -> Result<bool> {
    check_capture_cancelled()?;
    let Some(_lock) = try_acquire_sync_lock(db)? else {
        return Ok(false);
    };
    crate::store::sync_connection::with_connection(db, |conn| {
        crate::continuity::reconcile(conn, source)?;
        check_capture_cancelled()?;
        Ok(true)
    })
}

/// `None` retains the complete legacy source path for flat prompt logs,
/// subagent-only records, SQLite stores and unsupported metadata paths.
fn try_capture_paths(
    conn: &Connection,
    db: &Path,
    roots: &crate::ProviderRoots,
    scope: &SweepScope,
    force: bool,
) -> Result<Option<bool>> {
    match capture_paths(conn, db, roots, scope, force, false) {
        Err(error) if format!("{error:#}").starts_with("SESSION_SOURCE_MISMATCH:") => {
            // A same-stat replacement can retain a stale shallow identity.
            // Re-read only the admitted paths once; a genuinely inconsistent
            // writer still fails and remains pending for the next retry.
            capture_paths(conn, db, roots, scope, force, true)
        }
        outcome => outcome,
    }
}

fn capture_paths(
    conn: &Connection,
    db: &Path,
    roots: &crate::ProviderRoots,
    scope: &SweepScope,
    force: bool,
    fresh: bool,
) -> Result<Option<bool>> {
    let Some(paths) = scope.paths() else {
        return Ok(None);
    };
    if !paths
        .paths()
        .iter()
        .all(|path| transcript_source(roots, scope, path).is_some())
    {
        return Ok(None);
    }
    let sources: std::collections::BTreeSet<_> = paths
        .paths()
        .iter()
        .filter_map(|path| transcript_source(roots, scope, path))
        .collect();
    // `synchronous = NORMAL` for the whole capture -- discovery, hydration
    // and the capture marker -- rather than an fsync per commit. That is the
    // durability the sweep deliberately does *not* take (docs/benchmarks.md):
    // there a power loss can drop the last commits while `.sync-state.json`
    // stamps survive and vouch for them. Exact capture writes no sidecar
    // state -- it only reads `.sync-state.json` through the cold proof, and
    // the one out-of-database checkpoint, the Cursor transcript cursor, is for
    // a source `transcript_source` never admits here -- so every checkpoint it
    // writes is in the same database and a lost commit loses its checkpoint
    // with it; the next capture of the path, or the next reconciliation round,
    // reads it again. In WAL mode NORMAL still keeps the database consistent.
    // The guard restores the connection's level on drop, so a retained writer
    // goes back to full durability for whatever uses it next.
    let _relaxed = crate::discover::RelaxedSynchronous::new(conn)?;
    let mut env =
        DiscoveryEnv::with_provider_roots(conn, roots.clone()).with_path_scope(Some(paths.clone()));
    env.fresh_paths = fresh;
    let options = DiscoverOptions {
        sources: sources.iter().map(|source| (*source).into()).collect(),
        ..DiscoverOptions::default()
    };
    let mut sessions = std::collections::BTreeMap::new();
    let mut covered = HashSet::new();
    let report =
        crate::discover::identity_refresh::discover_sessions_for_capture(&env, &options, |row| {
            if let Some(path) = &row.raw_path {
                covered.insert(transcript_file::locator_of(Path::new(path)));
            }
            sessions.insert(
                (row.source.clone(), row.session_id.clone()),
                (row.raw_path.clone(), row.discovery_state == "full"),
            );
        })?;
    if !report.diagnostics.is_empty()
        || paths
            .paths()
            .iter()
            .any(|path| !covered.contains(&transcript_file::locator_of(path)))
    {
        return Ok(None);
    }
    for (index, ((source, session_id), (path, cached))) in sessions.into_iter().enumerate() {
        check_capture_cancelled()?;
        capture_progress(&source, index, Some(paths.paths().len()));
        let path = PathBuf::from(path.context("discovered transcript has no locator")?);
        if !force && cached && unchanged(conn, db, &source, &session_id, &path)? {
            continue;
        }
        let validate =
            force || hydrate::exact_capture_needs_validation(conn, &source, &session_id, &path)?;
        let observed_metadata = transcript_file::metadata(&path)?;
        let observed_change_time = super::transcript_cursor::change_time_ns(&observed_metadata);
        with_content_validation(validate, || {
            // On the capture's own connection: one validated writer for
            // the whole capture, see `hydrate_local_session_on`.
            hydrate::hydrate_local_session_on(
                conn,
                db,
                &hydrate::HydrateSessionOptions {
                    source: source.clone(),
                    session_id: session_id.clone(),
                    scope: SessionScope::Local,
                    include_related: false,
                },
                roots,
                None,
            )
        })?;
        hydrate::mark_exact_capture(conn, &source, &session_id, &path, observed_change_time)?;
        if fresh {
            retire_replaced_observations(conn, &source, &session_id, &path)?;
        }
    }
    Ok(Some(true))
}

fn unchanged(
    conn: &Connection,
    db: &Path,
    source: &str,
    session: &str,
    path: &Path,
) -> Result<bool> {
    Ok(
        hydrate::can_skip_exact_capture(conn, source, session, path)?
            || cold_proof::unchanged(conn, db, source, session, path)?,
    )
}

/// Retain old evidence/provenance, but stop resolving a replaced locator to
/// its former identity on every subsequent unchanged pass. Only a successful
/// fresh acquisition proves which local default observation owns this path.
fn retire_replaced_observations(
    conn: &Connection,
    source: &str,
    current: &str,
    path: &Path,
) -> Result<()> {
    let ids: Vec<String> = conn
        .prepare_cached(
            "SELECT session_id FROM session_observations WHERE source=?1 AND location='local' \
         AND connector_id=?1 AND connector_instance='default' AND raw_locator=?2 \
         AND access_state='available' AND session_id!=?3",
        )?
        .query_map(params![source, path.to_string_lossy(), current], |row| {
            row.get(0)
        })?
        .collect::<rusqlite::Result<_>>()?;
    for session_id in ids {
        observations::set_access(
            conn,
            &observations::ObservationKey {
                source: source.into(),
                session_id,
                location: SessionLocation::Local,
                connector_id: source.into(),
                connector_instance: "default".into(),
            },
            "unavailable",
        )?;
    }
    Ok(())
}

/// Admission stays beside the provider acquisition implementation. A path we
/// cannot prove names one primary transcript retains the normal sync path.
fn transcript_source(
    roots: &crate::ProviderRoots,
    scope: &SweepScope,
    path: &Path,
) -> Option<&'static str> {
    if !path.is_file() {
        return None;
    }
    let projects = roots.claude.join("projects");
    let claude = scope.includes("claude")
        && path.starts_with(&projects)
        && path.extension().is_some_and(|ext| ext == "jsonl")
        && !is_claude_workflow_journal(&projects, path)
        && !is_claude_sidecar_file(path);
    let codex = scope.includes("codex")
        && [
            roots.codex.join("sessions"),
            roots.codex.join("archived_sessions"),
        ]
        .iter()
        .any(|root| path.starts_with(root))
        && scoped_files::is_codex_rollout_locator(&transcript_file::locator_of(path));
    match (claude, codex) {
        (true, false) => Some("claude"),
        (false, true) => Some("codex"),
        _ => None,
    }
}

thread_local! {
    static VALIDATE_CONTENT: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
}

/// An exact dirty path is evidence of a write even when stat values were
/// restored. Preserve the durable cursor, but let its parser validate/resume
/// rather than skip before reaching uncommitted records.
pub(super) fn validating_content() -> bool {
    VALIDATE_CONTENT.with(|value| value.get().unwrap_or(false))
}

pub(super) fn capturing_exact_paths() -> bool {
    VALIDATE_CONTENT.with(|value| value.get().is_some())
}

fn with_content_validation<T>(force: bool, run: impl FnOnce() -> Result<T>) -> Result<T> {
    struct Restore(Option<bool>);
    impl Drop for Restore {
        fn drop(&mut self) {
            VALIDATE_CONTENT.with(|value| value.set(self.0));
        }
    }
    let _restore = Restore(VALIDATE_CONTENT.with(|value| value.replace(Some(force))));
    run()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validation_policy_is_thread_local_and_restored_after_error_and_panic() {
        assert!(!validating_content());
        let error: Result<()> = with_content_validation(true, || {
            assert!(validating_content());
            assert!(!std::thread::spawn(validating_content).join().unwrap());
            anyhow::bail!("synthetic failure")
        });
        assert!(error.is_err());
        assert!(!validating_content());
        let panic = std::panic::catch_unwind(|| {
            with_content_validation(true, || -> Result<()> { panic!("synthetic panic") })
        });
        assert!(panic.is_err());
        assert!(!validating_content());
    }
}

#[cfg(test)]
mod identity_tests;
