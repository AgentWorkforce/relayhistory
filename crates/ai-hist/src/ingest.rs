use crate::{
    default_db_path, insert_history, now_ms, open_db, open_db_readonly, parse_cursor_text,
    prompt_hash, schema_is_catalog_read_current, sync_opencode_db, sync_opencode_session,
    HistoryEntry, SessionLocation, SessionScope,
};
use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{self, BufRead, BufReader, Read, Seek};
use std::path::{Path, PathBuf};
use std::time::Duration;

pub(crate) mod codex;
pub(crate) mod hydrate;

use crate::diagnostics::*;
use crate::discover;
#[cfg(test)]
use crate::history_search::{search_all, SearchRole};
use crate::paths::home_dir;
use crate::remote;

pub use crate::discover::{
    discover_sessions, discover_sessions_collect, discover_sessions_with_env,
    discover_sessions_with_providers, list_session_catalog, list_session_catalog_page,
    shallow_providers, validate_discovery_scope, AllProvidersFailed, Candidate, CatalogCursor,
    CatalogListOptions, DiscoverOptions, DiscoveryCounters, DiscoveryDiagnostic, DiscoveryEnv,
    DiscoverySummary, ProviderSummary, ScanEnv, SessionCatalogPage, ShallowReadAccess,
    ShallowSession, ShallowSessionProvider, SourceExemption, DEFAULT_CATALOG_LIMIT,
    DISCOVERY_EXEMPTIONS, SESSION_CATALOG_CONTRACT_VERSION, SHALLOW_SCANNER_VERSION,
};
pub use crate::relationship_capture::{record_relationship, ObservedRelationship};
pub use hydrate::{
    hydrate_session, hydrate_session_at, hydrate_session_at_with_connectors, HydrateSessionOptions,
    HydrateSessionResult, HydrationDiagnostic, HydrationEvidence, HydrationIndexedThrough,
    SESSION_HYDRATION_CONTRACT_VERSION,
};

fn is_delivery_retention_limit(error: &anyhow::Error) -> bool {
    #[cfg(feature = "delivery")]
    {
        crate::delivery::is_retention_limit(error)
    }
    #[cfg(not(feature = "delivery"))]
    {
        let _ = error;
        false
    }
}

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};

/// When set, sync progress lines (`[claude] +N rows`, …) are suppressed. The
/// in-process local sync API sets this to keep the embedding host's stdout
/// available for its own output; the CLI leaves it false.
static SYNC_QUIET: AtomicBool = AtomicBool::new(false);

/// `println!` for sync progress that honors [`SYNC_QUIET`].
macro_rules! sync_note {
    ($($arg:tt)*) => {
        if !SYNC_QUIET.load(AtomicOrdering::Relaxed) {
            println!($($arg)*);
        }
    };
}

/// Refresh local agent history without performing any cloud operation.
/// Embedding applications should use this before opening the local catalog.
/// When another process owns the sync lock the refresh is skipped — the
/// concurrent scan is already producing the fresh data this caller wants.
pub fn sync_local() -> Result<()> {
    sync_local_at(&default_db_path()).map(|_| ())
}

/// Refresh local history into an explicitly selected database.
///
/// This is the reusable engine entry point used by the N-API boundary. The
/// command-line parser is intentionally not involved.
pub fn sync_local_at(db_path: &Path) -> Result<bool> {
    sync_local_at_with_home(db_path, &home_dir())
}

/// Content-free progress for hosts displaying a local capture operation.
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CaptureProgress {
    pub source: String,
    pub processed_files: usize,
    pub total_files: Option<usize>,
}
type CaptureObserver = std::rc::Rc<dyn Fn(CaptureProgress)>;
thread_local! {
    static CAPTURE_OBSERVER: std::cell::RefCell<Option<CaptureObserver>> = std::cell::RefCell::new(None);
}

/// Observes this thread's capture only; no paths or session contents are exposed.
pub fn sync_local_at_with_progress(
    db_path: &Path,
    observer: impl Fn(CaptureProgress) + 'static,
) -> Result<bool> {
    struct Restore(Option<CaptureObserver>);
    impl Drop for Restore {
        fn drop(&mut self) {
            CAPTURE_OBSERVER.with(|slot| *slot.borrow_mut() = self.0.take());
        }
    }
    let _restore =
        Restore(CAPTURE_OBSERVER.with(|slot| slot.replace(Some(std::rc::Rc::new(observer)))));
    capture_progress("initializing", 0, None);
    let result = sync_local_at(db_path);
    if result.is_ok() {
        capture_progress("complete", 0, None);
    }
    result
}

fn capture_progress(source: &str, processed_files: usize, total_files: Option<usize>) {
    CAPTURE_OBSERVER.with(|slot| {
        let observer = slot.borrow().clone();
        if let Some(observer) = observer {
            observer(CaptureProgress {
                source: source.into(),
                processed_files,
                total_files,
            });
        }
    });
}

// Report before taking each file, including the final None. This counts completed
// files even when the ingest loop continues early for an unchanged checkpoint.
fn capture_files(source: &'static str, files: Vec<PathBuf>) -> impl Iterator<Item = PathBuf> {
    let total = files.len();
    let mut files = files.into_iter();
    let mut processed = 0;
    std::iter::from_fn(move || {
        capture_progress(source, processed, Some(total));
        let next = files.next();
        if next.is_some() {
            processed += 1;
        }
        next
    })
}

/// Full local ingest using an explicit provider home instead of the process
/// `HOME`. Used by [`crate::SessionStore`] when the embedder overrides home.
pub(crate) fn sync_local_at_with_home(db_path: &Path, home: &Path) -> Result<bool> {
    SYNC_QUIET.store(true, AtomicOrdering::Relaxed);
    sync_exclusive_with_home(db_path, home)
}

/// Full ingestion for a selected scope into the default database.
pub fn sync_scoped(scope: SessionScope) -> Result<bool> {
    sync_scoped_at(&default_db_path(), scope)
}

/// Full ingestion for a selected session-presence scope.
///
/// The local distribution ingests local providers for `local` and `all`.
/// Explicit `remote` acquisition requires an installed source plugin composed
/// through [`sources::SourceRegistry`] or an SDK host. Credentials alone never
/// add a data source. Cached remote catalog reads remain available.
pub fn sync_scoped_at(db_path: &Path, scope: SessionScope) -> Result<bool> {
    SYNC_QUIET.store(true, AtomicOrdering::Relaxed);
    sync_scope_exclusive(db_path, scope)
}

fn sync_scope_exclusive(db_path: &Path, scope: SessionScope) -> Result<bool> {
    sync_scope_with_connectors(db_path, scope, &remote::SourceConnectorSelection::default())
}

/// Full ingestion with an explicit remote connector allowlist. Local scope
/// ignores all remote connectors and never probes their credentials.
pub fn sync_scoped_at_with_connectors(
    db_path: &Path,
    scope: SessionScope,
    connectors: &remote::SourceConnectorSelection,
) -> Result<bool> {
    SYNC_QUIET.store(true, AtomicOrdering::Relaxed);
    sync_scope_with_connectors(db_path, scope, connectors)
}

/// Controls optional ingestion progress for command-line applications.
#[derive(Debug, Clone, Copy)]
pub enum SyncOutput {
    Silent,
    Progress,
}

/// Scoped ingestion with an explicitly chosen progress destination. Embedded
/// callers should use `sync_scoped_at_with_connectors`, which is always silent.
pub fn sync_scoped_at_with_output(
    db_path: &Path,
    scope: SessionScope,
    connectors: &remote::SourceConnectorSelection,
    output: SyncOutput,
) -> Result<bool> {
    SYNC_QUIET.store(
        matches!(output, SyncOutput::Silent),
        AtomicOrdering::Relaxed,
    );
    sync_scope_with_connectors(db_path, scope, connectors)
}

/// Import the selected OpenCode store with the same exclusive ingestion lock.
pub fn sync_opencode_at(db_path: &Path, source_path: &Path, output: SyncOutput) -> Result<bool> {
    SYNC_QUIET.store(
        matches!(output, SyncOutput::Silent),
        AtomicOrdering::Relaxed,
    );
    sync_opencode_exclusive(db_path, source_path)
}

fn sync_scope_with_connectors(
    db_path: &Path,
    scope: SessionScope,
    connectors: &remote::SourceConnectorSelection,
) -> Result<bool> {
    if scope == SessionScope::Remote {
        remote::ensure_selected_remote_connectors_configured_for_at(
            "sync",
            &home_dir(),
            &[],
            connectors,
        )?;
    }
    let mut ran = false;
    if matches!(scope, SessionScope::Local | SessionScope::All) {
        ran |= sync_exclusive(db_path)?;
    }
    if matches!(scope, SessionScope::Remote | SessionScope::All) {
        ran |= sync_remote_connectors(db_path, scope, connectors)?;
    }
    Ok(ran)
}

/// Run every configured remote connector's acquisition into the ledger.
///
/// This is the discovery engine at remote scope with no row cap: stamp-guarded
/// catalog upserts plus `remote` presences, so it deliberately does not take
/// the sync advisory lock (the same concurrency argument as [`discover`]).
/// Under `all` scope a machine with no connector configured skips quietly —
/// that is the documented "runs whatever is available" contract; a remote-only
/// request was already rejected by [`sync_scoped_at_with_connectors`] before this point.
fn sync_remote_connectors(
    _db_path: &Path,
    scope: SessionScope,
    _connectors: &remote::SourceConnectorSelection,
) -> Result<bool> {
    if scope == SessionScope::All {
        return Ok(false);
    }
    anyhow::bail!("CONNECTOR_NOT_CONFIGURED: remote acquisition requires an explicitly registered source plugin")
}

/// Cache-only session catalog listing against the default database.
///
/// The in-process equivalent of `ai-hist sessions list`: one indexed query
/// over `sessions`, no provider I/O. A database that does not exist yet is an
/// empty catalog, not an error — the caller is expected to run discovery next.
pub fn list_sessions_local(options: &CatalogListOptions) -> Result<SessionCatalogPage> {
    list_sessions_local_at(&default_db_path(), options)
}

/// Cache-only catalog listing against an explicitly selected database.
pub fn list_sessions_local_at(
    db_path: &Path,
    options: &CatalogListOptions,
) -> Result<SessionCatalogPage> {
    anyhow::ensure!(
        options.scope == SessionScope::Local,
        "list_sessions_local_at only accepts local scope; use list_sessions_scoped_at for remote or all"
    );
    list_sessions_scoped_at(db_path, options)
}

/// Cache-only catalog listing for an explicit session-presence scope.
pub fn list_sessions_scoped(options: &CatalogListOptions) -> Result<SessionCatalogPage> {
    list_sessions_scoped_at(&default_db_path(), options)
}

/// Scoped cache-only catalog listing against an explicitly selected database.
pub fn list_sessions_scoped_at(
    db_path: &Path,
    options: &CatalogListOptions,
) -> Result<SessionCatalogPage> {
    if !db_path.exists() {
        return Ok(SessionCatalogPage {
            scope: options.scope,
            ..Default::default()
        });
    }
    let conn = match open_db_readonly(db_path) {
        Ok(conn) if schema_is_catalog_read_current(&conn).unwrap_or(false) => conn,
        // Missing migration (a read-only handle skips init_db) or an
        // unreadable handle: let the writable open sort it out.
        _ => open_db(db_path)?,
    };
    list_session_catalog_page(&conn, options)
}

/// Shallow discovery against the default database, with the rows collected.
///
/// The in-process equivalent of `ai-hist sessions discover`. Upsert-only and
/// stamp-guarded, so it does not take the sync lock and is safe to run beside
/// `sync_local`.
pub fn discover_sessions_local(
    options: &DiscoverOptions,
) -> Result<(Vec<ShallowSession>, DiscoverySummary)> {
    discover_sessions_local_at(&default_db_path(), options)
}

/// Shallow discovery into an explicitly selected database.
pub fn discover_sessions_local_at(
    db_path: &Path,
    options: &DiscoverOptions,
) -> Result<(Vec<ShallowSession>, DiscoverySummary)> {
    anyhow::ensure!(
        options.scope == SessionScope::Local,
        "discover_sessions_local_at only accepts local scope; use discover_sessions_scoped_at for remote or all"
    );
    let conn = open_db(db_path)?;
    discover_sessions_collect(&conn, options)
}

/// Scoped discovery into the default database.
pub fn discover_sessions_scoped(
    options: &DiscoverOptions,
) -> Result<(Vec<ShallowSession>, DiscoverySummary)> {
    discover_sessions_scoped_at(&default_db_path(), options)
}

/// Scoped discovery into an explicitly selected database.
pub fn discover_sessions_scoped_at(
    db_path: &Path,
    options: &DiscoverOptions,
) -> Result<(Vec<ShallowSession>, DiscoverySummary)> {
    discover_sessions_scoped_at_with_connectors(
        db_path,
        options,
        &remote::SourceConnectorSelection::default(),
    )
}

pub fn discover_sessions_scoped_at_with_connectors(
    db_path: &Path,
    options: &DiscoverOptions,
    connectors: &remote::SourceConnectorSelection,
) -> Result<(Vec<ShallowSession>, DiscoverySummary)> {
    if options.scope == SessionScope::Remote {
        remote::ensure_selected_remote_connectors_configured_for_at(
            "discovery",
            &home_dir(),
            &options.sources,
            connectors,
        )?;
    }
    let conn = open_db(db_path)?;
    let mut rows = Vec::new();
    let summary = discover::discover_sessions_with_connectors(
        &DiscoveryEnv::new(&conn),
        options,
        connectors,
        |row| rows.push(row.clone()),
    )?;
    Ok((rows, summary))
}

#[derive(Default)]
struct SyncSourceReport {
    succeeded: usize,
    failures: Vec<SyncSourceFailure>,
}

struct SyncSourceFailure {
    source: String,
    error: anyhow::Error,
    is_contention: bool,
    contention_path: Option<PathBuf>,
}

impl SyncSourceReport {
    fn capture<T>(&mut self, source: &str, result: Result<T>) -> Option<T> {
        match result {
            Ok(value) => {
                self.succeeded += 1;
                Some(value)
            }
            Err(error) => {
                self.failures.push(SyncSourceFailure {
                    source: source.to_string(),
                    is_contention: is_sqlite_contention(&error),
                    contention_path: source_database_path(&error).map(Path::to_path_buf),
                    error,
                });
                None
            }
        }
    }

    fn finish(mut self, db_path: &Path) -> Result<()> {
        if self.failures.is_empty() {
            return Ok(());
        }

        eprintln!(
            "ai-hist: {} history source(s) failed; {} source(s) completed:",
            self.failures.len(),
            self.succeeded
        );
        for failure in &self.failures {
            eprintln!("  [{}] {:#}", failure.source, failure.error);
        }
        let mut diagnosed = HashSet::new();
        for failure in self.failures.iter().filter(|failure| failure.is_contention) {
            let path = failure.contention_path.as_deref().unwrap_or(db_path);
            if diagnosed.insert(path.to_path_buf()) {
                eprintln!("{}", write_contention_diagnostic(path));
            }
        }
        // An enabled durable capture job cannot silently lose history. Missing
        // providers count as successful no-ops, so the ordinary partial-source
        // policy would otherwise turn a full capture store into sync success.
        // Preserve the cause so native/SDK callers can report its safe code.
        if let Some(index) = self
            .failures
            .iter()
            .position(|failure| is_delivery_retention_limit(&failure.error))
        {
            let failure = self.failures.remove(index);
            return Err(failure.error)
                .with_context(|| format!("{} history delivery capture failed", failure.source));
        }
        if self.succeeded == 0 {
            anyhow::bail!(
                "all {} history sources failed; no source made progress",
                self.failures.len()
            );
        }
        Ok(())
    }
}

fn sync_basic(conn: &Connection, db_path: &Path, home: &Path) -> Result<()> {
    // See `begin_acquisition_pass`: the resolver's cache is sound only within
    // one pass, and a long-lived host (watch, the Node addon, a desktop app)
    // runs many passes without restarting.
    crate::project_identity::begin_acquisition_pass();
    let mut total_inserted = 0;
    let mut report = SyncSourceReport::default();
    let state_path = db_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(".sync-state.json");
    if let Err(error) = cleanup_stale_sync_state_temps(&state_path) {
        eprintln!(
            "ai-hist: could not clean stale sync-state temp files beside {}: {error:#}",
            state_path.display()
        );
    }
    // Refuse to start rather than fail partway. A write that runs out of space
    // mid-flight is what truncated .sync-state.json and wedged sync for days;
    // stopping up front with an actionable message is strictly better than
    // discovering it through torn state.
    if let Some(free) = free_bytes(db_path) {
        if free < FREE_SPACE_FLOOR_BYTES {
            anyhow::bail!(
                "only {} free on the volume holding {} (need {}). \
                 Free space before syncing: a write that fails partway can leave torn state.",
                human_bytes(free),
                db_path.display(),
                human_bytes(FREE_SPACE_FLOOR_BYTES)
            );
        }
    }
    let mut state = load_sync_state(&state_path)?;
    // Checkpoint after every source that advances `state`, rather than once at
    // the end. A run can die partway through -- killed process, locked database,
    // full disk -- and state written only at the end discards every source that
    // already finished, sending the next run back over the same files. That
    // turns one interrupted run into a loop that re-scans from scratch forever
    // and never persists anything. Checkpointing makes each source's cursor
    // durable the moment that source completes.
    capture_progress("claude-history", 0, None);
    if let Some(inserted) = report.capture(
        "claude",
        sync_jsonl_incremental(
            conn,
            &mut state,
            "claude",
            &home.join(".claude/history.jsonl"),
            parse_claude_line,
            &mut |in_progress| checkpoint_sync_state(&state_path, in_progress),
        ),
    ) {
        total_inserted += inserted;
        checkpoint_sync_state(&state_path, &state);
    }
    capture_progress("claude", 0, None);
    if report
        .capture(
            "claude-metadata",
            sync_claude_session_metadata(conn, &mut state, &home.join(".claude/projects")),
        )
        .is_some()
    {
        checkpoint_sync_state(&state_path, &state);
    }
    capture_progress("codex", 0, None);
    if let Some(inserted) = report.capture("codex", sync_codex(conn, &mut state, home)) {
        total_inserted += inserted;
        checkpoint_sync_state(&state_path, &state);
    }
    capture_progress("cursor", 0, None);
    if let Some(inserted) = report.capture(
        "cursor",
        sync_cursor(conn, &mut state, &home.join(".cursor/projects")),
    ) {
        total_inserted += inserted;
        checkpoint_sync_state(&state_path, &state);
    }
    capture_progress("grok", 0, None);
    if let Some(inserted) = report.capture(
        "grok",
        sync_grok(conn, &mut state, &home.join(".grok/sessions")),
    ) {
        total_inserted += inserted;
        checkpoint_sync_state(&state_path, &state);
    }
    capture_progress("trajectory", 0, None);
    if let Some(inserted) = report.capture("trajectory", sync_trajectories(conn, &mut state, home))
    {
        total_inserted += inserted;
        checkpoint_sync_state(&state_path, &state);
    }
    capture_progress("opencode", 0, None);
    let opencode = std::env::var_os("OPENCODE_DB")
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".local/share/opencode/opencode.db"));
    if let Some(open_inserted) = report.capture("opencode", sync_opencode_db(conn, &opencode)) {
        if opencode.exists() {
            sync_note!("  [opencode] +{open_inserted} rows");
        } else {
            sync_note!("  [opencode] not found: {} (skipped)", opencode.display());
        }
        total_inserted += open_inserted;
    }
    report.finish(db_path)?;
    // Establish connector-owned locators from actual provider enumeration after
    // ingestion, including on a checkpoint-only retry. Never infer an adapter
    // from an old aggregate presence row.
    capture_progress("catalog", 0, None);
    let discovery_env = DiscoveryEnv::with_roots(conn, home.to_path_buf(), opencode);
    discover::discover_sessions_with_providers(
        &discovery_env,
        &DiscoverOptions::default(),
        &shallow_providers(),
        |_| {},
    )?;
    // After discovery, not before: shallow discovery is what fills in `cwd`
    // and `repo_url` for sessions a provider's history file mentions without
    // describing, and inheritance needs every relationship this run recorded
    // to already be in the ledger.
    refresh_project_identity_after_sync(conn);
    let total: i64 = conn.query_row("SELECT COUNT(*) FROM history", [], |row| row.get(0))?;
    // Fold the WAL back into the database now that the writes are done. Best
    // effort: a concurrent reader pinning an old snapshot blocks a full
    // checkpoint, and that is not a reason to fail a sync that did its work.
    // Left unchecked the WAL grows without bound (156MB observed in the wild).
    match conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
        ))
    }) {
        Ok((busy, log_frames, checkpointed_frames)) if busy != 0 => {
            sync_note!(
                "  [wal] checkpoint incomplete: {checkpointed_frames}/{log_frames} frames; another reader is active"
            );
        }
        Ok(_) => {}
        Err(err) => sync_note!("  [wal] checkpoint skipped: {err}"),
    }
    let wal_bytes = fs::metadata(wal_path(db_path))
        .map(|m| m.len())
        .unwrap_or(0);
    if wal_bytes > WAL_WARN_BYTES {
        eprintln!(
            "ai-hist: WAL is {} after checkpointing -- a long-lived reader is \
             pinning an old snapshot; run `ai-hist doctor`",
            human_bytes(wal_bytes)
        );
    }
    sync_note!("  [rust-sync] +{total_inserted} rows");
    sync_note!("  Total: {total} entries");
    Ok(())
}

/// Cross-process sync guard for one canonical database identity. Reflex, launchd, cron, and
/// manual invocations can otherwise all walk the same multi-gigabyte history at once.
struct SyncRunLock {
    _file: fs::File,
}

impl Drop for SyncRunLock {
    fn drop(&mut self) {
        // Do not rely solely on platform-specific close timing. Linux CI exposed a race where
        // a just-dropped guard was not immediately reacquirable through an alias path.
        let _ = crate::file_lock::unlock(&self._file);
    }
}

fn canonical_db_identity(db_path: &Path) -> Result<PathBuf> {
    if db_path.exists() {
        return fs::canonicalize(db_path)
            .with_context(|| format!("canonicalizing database path {}", db_path.display()));
    }
    let file_name = db_path
        .file_name()
        .context("database path has no file name")?;
    let parent = db_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    Ok(fs::canonicalize(parent)?.join(file_name))
}

fn sync_lock_path(db_path: &Path) -> Result<PathBuf> {
    let canonical = canonical_db_identity(db_path)?;
    let mut name = canonical
        .file_name()
        .context("canonical database path has no file name")?
        .to_os_string();
    name.push(".sync.lock");
    Ok(canonical.with_file_name(name))
}

fn try_acquire_sync_lock(db_path: &Path) -> Result<Option<SyncRunLock>> {
    let path = sync_lock_path(db_path)?;
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)?;
    match crate::file_lock::try_lock_exclusive(&file) {
        Ok(()) => Ok(Some(SyncRunLock { _file: file })),
        Err(err) if err.kind() == io::ErrorKind::WouldBlock => Ok(None),
        Err(err) => Err(err.into()),
    }
}

fn sync_exclusive(db_path: &Path) -> Result<bool> {
    sync_exclusive_with_home(db_path, &home_dir())
}

fn sync_exclusive_with_home(db_path: &Path, home: &Path) -> Result<bool> {
    let Some(_sync_lock) = try_acquire_sync_lock(db_path)? else {
        sync_note!("  [sync] another sync is already running; skipped");
        return Ok(false);
    };
    let conn = open_db(db_path).map_err(|error| enrich_sync_error(db_path, error))?;
    sync_basic(&conn, db_path, home).map_err(|error| enrich_sync_error(db_path, error))?;
    Ok(true)
}

fn sync_opencode_exclusive(db_path: &Path, opencode_path: &Path) -> Result<bool> {
    let Some(_sync_lock) = try_acquire_sync_lock(db_path)? else {
        sync_note!("  [sync-opencode] another sync is already running; skipped");
        return Ok(false);
    };
    let conn = open_db(db_path).map_err(|error| enrich_sync_error(db_path, error))?;
    let inserted = sync_opencode_db(&conn, opencode_path)
        .map_err(|error| enrich_sync_error(db_path, error))?;
    sync_note!("  [opencode] +{inserted} rows");
    let env = DiscoveryEnv::with_roots(&conn, home_dir(), opencode_path.to_path_buf());
    let options = DiscoverOptions {
        sources: vec!["opencode".into()],
        ..Default::default()
    };
    discover::discover_sessions_with_connectors(
        &env,
        &options,
        &remote::SourceConnectorSelection::new(Vec::new())?,
        |_| {},
    )?;
    refresh_project_identity_after_sync(&conn);
    Ok(true)
}

/// Recompute canonical project identity at the end of a sync, reporting rather
/// than failing.
///
/// The evidence rows are already committed by this point, and every key here
/// is *derived* from them: the next run recomputes whatever this one could not
/// write. Failing the whole sync over it would turn a run that did its real
/// work into a reported failure — the same argument [`checkpoint_sync_state`]
/// makes for its bookkeeping write, and the reason a contended database still
/// reports partial success rather than an error.
///
/// Deliberately not silent. A skip that printed nothing would leave project
/// keys stuck at NULL with no trace of why, and "nothing to do" and "could not
/// write" would look identical from the outside.
fn refresh_project_identity_after_sync(conn: &Connection) {
    if let Err(error) = crate::store::refresh_project_identity(conn) {
        eprintln!(
            "ai-hist: could not refresh canonical project identity: {error:#} \
             (project keys stay as they were; the next sync retries)"
        );
    }
}

/// Synchronize local providers, or read the current snapshot when another sync owns the lock.
/// The boolean reports that synchronization was skipped. No destination or credentials are read.
pub fn prepare_local_sync_snapshot(db_path: &Path) -> Result<(Connection, bool)> {
    SYNC_QUIET.store(true, AtomicOrdering::Relaxed);
    let Some(sync_lock) = try_acquire_sync_lock(db_path)? else {
        // Pushing already-indexed rows only reads SQLite and remains useful while another
        // process scans. A read-only connection avoids joining the writer contention.
        let conn = open_db_readonly(db_path).with_context(|| {
            format!(
                "another sync owns the lock and no readable database is available at {}",
                db_path.display()
            )
        })?;
        return Ok((conn, true));
    };
    let conn = open_db(db_path).map_err(|error| enrich_sync_error(db_path, error))?;
    sync_basic(&conn, db_path, &home_dir()).map_err(|error| enrich_sync_error(db_path, error))?;
    drop(sync_lock);
    Ok((conn, false))
}

/// Sync state is an optimization, not a source of truth: an unreadable file
/// costs a full re-scan (every insert path upserts) but must never wedge sync.
/// A disk-full write used to leave this file empty and abort every later run.
fn load_sync_state(path: &Path) -> Result<Map<String, Value>> {
    if !path.exists() {
        return Ok(Map::new());
    }
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(err) => {
            eprintln!(
                "ai-hist: could not read {} ({err}); starting from empty sync state",
                path.display()
            );
            return Ok(Map::new());
        }
    };
    match serde_json::from_str::<Value>(&text) {
        Ok(value) => Ok(value.as_object().cloned().unwrap_or_default()),
        Err(err) => {
            eprintln!(
                "ai-hist: {} is corrupt ({err}); starting from empty sync state",
                path.display()
            );
            Ok(Map::new())
        }
    }
}

/// Persist progress mid-run, between sources.
///
/// Deliberately non-fatal: the rows are already committed, so a run that
/// finished real work should not be reported as failed because its bookkeeping
/// write did not land. The next checkpoint retries, and [`save_sync_state`]
/// leaves the previous state intact when a write fails, so the worst case is a
/// re-scan rather than corruption.
fn checkpoint_sync_state(path: &Path, state: &Map<String, Value>) {
    let checkpoint = || -> Result<()> {
        let _lock = SyncStateLock::acquire(path)?;
        match merged_sync_state(path, state)? {
            // Disk is already current; skip the rewrite.
            None => Ok(()),
            Some(merged) => save_sync_state(path, &merged),
        }
    };
    if let Err(err) = checkpoint() {
        eprintln!("ai-hist: could not checkpoint sync state: {err:#}");
    }
}

/// Serializes the complete load/merge/rename operation. Atomic rename prevents
/// torn JSON, but without this lock two writers can both merge from the same
/// snapshot and the later rename can still discard the earlier update.
struct SyncStateLock {
    file: fs::File,
}

impl SyncStateLock {
    fn acquire(state_path: &Path) -> Result<Self> {
        Self::acquire_with_timeout(state_path, SYNC_STATE_LOCK_TIMEOUT)
    }

    fn acquire_with_timeout(state_path: &Path, timeout: Duration) -> Result<Self> {
        let parent = state_path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)?;
        let name = state_path
            .file_name()
            .context("sync-state path has no file name")?;
        let mut lock_name = name.to_os_string();
        lock_name.push(".lock");
        let file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(parent.join(lock_name))?;
        let started = std::time::Instant::now();
        loop {
            match crate::file_lock::try_lock_exclusive(&file) {
                Ok(()) => break,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    if started.elapsed() >= timeout {
                        anyhow::bail!("sync-state lock remained busy for {timeout:?}");
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(error) => return Err(error.into()),
            }
        }
        Ok(Self { file })
    }
}

const SYNC_STATE_LOCK_TIMEOUT: Duration = Duration::from_secs(2);

impl Drop for SyncStateLock {
    fn drop(&mut self) {
        let _ = crate::file_lock::unlock(&self.file);
    }
}

/// Fold this run's cursors into whatever is already on disk.
///
/// Sync runs are not serialized -- the CLI, the background service, and the
/// in-process napi entry point can all overlap -- and each holds its own copy
/// of the whole state map. Writing that copy wholesale lets a slow run replace
/// a fast run's newer cursors with its own stale ones, so the next run rescans
/// work that was already finished: exactly the loop checkpointing exists to
/// prevent. Merging per key keeps sources this run did not touch. File cursors
/// advance monotonically only within one generation; a newer generation must
/// replace the old cursor even when it starts at a smaller offset.
///
/// Returns `None` when disk already reflects everything here, so a steady-state
/// run that finds every source up to date does not rewrite the file per source.
/// State keys earlier versions wrote and no longer maintain, each paired with the
/// key that supersedes it.
///
/// [`merged_sync_state`] folds this run's keys into what is already on disk and
/// deliberately never walks the on-disk keys: a run legitimately omits every
/// source it did not touch, so absence cannot mean "delete". That makes an
/// in-memory `state.remove(...)` invisible to disk — the retired map is reloaded
/// and rewritten forever, and the cleanup those migrations intend never
/// completes. Retirements therefore have to be declared here, where the merge
/// can act on them, rather than inferred from a key's absence.
///
/// The pairing is an ordering rule, not decoration. `checkpoint_sync_state` runs
/// once per source against the whole state map, and `codex_rollouts_v4` is still
/// *read* by this version to seed the v5 migration. Sweeping unconditionally
/// would drop it during an earlier source's checkpoint, before
/// `sync_codex_rollouts` has written `codex_rollouts_v5`; a crash or an
/// overlapping sync in that window would find neither map and force a full
/// re-read of the archive. Requiring the successor in the same write closes that
/// gap: the old map only leaves disk once its replacement is on the way there.
/// Bump when a parser change needs transcripts an earlier release already
/// indexed to be re-read once.
///
/// The per-session "does this look unparsed?" probes below decide *which*
/// files a backfill pass re-reads. They cannot decide *whether* a pass is
/// still owed, because a row they can see may not be a row the local
/// transcript owns: `session_events` is keyed by `(source, session_id)`, and
/// local and remote observations of one session share that identity. An
/// installed source adapter contributes rows through the evidence path, which
/// does not carry `raw_facts_version`, and re-reading the local transcript
/// never repairs a row that came from somewhere else -- so "some row for this
/// session is unstamped" is a condition that can stay true forever, re-reading
/// an unchanged file on every sync while never repairing anything.
///
/// Recording the generation per provider makes the pass happen exactly once.
const RAW_MESSAGE_FACTS_GENERATION: i64 = 1;
const CLAUDE_RAW_MESSAGE_FACTS_KEY: &str = "claude_raw_message_facts";
const CODEX_RAW_MESSAGE_FACTS_KEY: &str = "codex_raw_message_facts";

/// Whether this provider still owes a one-time raw-facts backfill pass.
fn raw_facts_backfill_pending(state: &Map<String, Value>, key: &str) -> bool {
    state.get(key).and_then(Value::as_i64).unwrap_or(0) < RAW_MESSAGE_FACTS_GENERATION
}

/// Record that the pass finished. Only reached when the walk completed, so an
/// interrupted sync retires the backfill rather than skipping it.
///
/// `walked_every_known_root` is the other half of that: a walk that completed
/// without being able to open the files it was meant to repair has not done
/// the pass, and recording the generation there retires it for good. The facts
/// then stay null on every row for the life of the install while `sync` goes
/// on reporting success -- the same shape of failure the pass exists to undo,
/// one level up.
fn record_raw_facts_backfill(
    state: &mut Map<String, Value>,
    key: &str,
    walked_every_known_root: bool,
) {
    if !walked_every_known_root {
        return;
    }
    state.insert(key.to_string(), json!(RAW_MESSAGE_FACTS_GENERATION));
}

/// Whether this run can actually read `path`.
///
/// Both parsers reach for a transcript with
/// `fs::read_to_string(..).unwrap_or_default()`, which turns a read error into
/// an empty string -- and an empty string is indistinguishable from an empty
/// file. A permission change, a path swapped for something that is not a
/// regular file, or an I/O error between enumeration and read therefore all
/// read as "this transcript has nothing in it", and the walk stamps the file
/// as seen. Once the raw-facts generation is recorded, the rows behind that
/// path stay null for the life of the install.
///
/// A read that fails is not an observation. Asking here lets the walk treat an
/// unreadable file exactly as it treats one it never saw: no stamp, the
/// generation withheld, and the stale stamp dropped so a later run that *can*
/// read it does.
///
/// One byte is enough -- `EACCES`, `EISDIR` and `ENXIO` all surface on the
/// open or the first read -- and this only runs for files the walk is about to
/// parse anyway, never for the ones the stamp fast path skips.
fn transcript_is_readable(path: &Path) -> bool {
    match fs::File::open(path) {
        Ok(mut file) => file.read(&mut [0u8; 1]).is_ok(),
        Err(_) => false,
    }
}

/// Whether `key`, a sync-state path, names a file inside `root`.
fn path_key_is_under(key: &str, root: &Path) -> bool {
    let prefix = root.to_string_lossy();
    key.len() > prefix.len()
        && key.starts_with(prefix.as_ref())
        && key[prefix.len()..].starts_with(std::path::MAIN_SEPARATOR)
}

/// Whether the sync state already names transcripts under `root`.
///
/// This is what distinguishes an archive that is *unavailable* on this run --
/// an unmounted home, a profile directory that has not been created yet, an
/// external drive, a sync client that has not pulled the tree down -- from one
/// that simply does not exist for this install. The first makes the walk
/// complete over files whose rows still need repairing; the second has nothing
/// to repair. Only the first may hold the generation back, or an install that
/// never had an `archived_sessions` tree would keep re-probing forever.
fn state_names_files_under(known: &Map<String, Value>, root: &Path) -> bool {
    known.keys().any(|key| path_key_is_under(key, root))
}

/// The paths under `root` that the stamp map names and this walk did not see.
///
/// Availability is per file, not per root. A root can be readable while part
/// of it is not -- a partially mounted archive, a sync client that has pulled
/// down some of a tree -- and then "the walk returned at least one file" says
/// nothing about the ones it did not return. Their rows are still there and
/// still null.
fn unobserved_known_paths(
    known: &Map<String, Value>,
    root: &Path,
    observed: &[PathBuf],
) -> Vec<String> {
    let observed: HashSet<String> = observed
        .iter()
        .map(|path| path.to_string_lossy().to_string())
        .collect();
    known
        .keys()
        .filter(|key| path_key_is_under(key, root) && !observed.contains(*key))
        .cloned()
        .collect()
}

/// Sync-state key carrying the nested stamp entries this run deliberately
/// dropped.
///
/// It is an instruction for the checkpoint write rather than state:
/// [`merged_sync_state`] applies it and then removes it, so it never reaches
/// disk and no later run inherits it.
///
/// It has to exist because removing a key from the in-memory map does not
/// remove it from the file. The merge folds this run's keys into what is
/// already on disk, and `merge_object_values` starts from the on-disk object,
/// so an entry this run dropped is simply absent from the overlay and survives
/// untouched. Without this the stamp comes back on every write, the pass stays
/// withheld for the life of the install, and every later sync re-runs the
/// raw-fact probes over the whole archive.
const FORGOTTEN_PATHS_KEY: &str = "forgotten_paths";

/// Forget the stamps of files this run could not see, and report whether the
/// walk covered everything the state knows about.
///
/// Three things have to happen, and each is insufficient alone. The generation
/// is withheld, because a run that did not see a file has not backfilled it.
/// The file's stamp is dropped, because a stamp is a claim that the file is
/// unchanged since we last read it, and a file that vanished and came back is
/// not something this process watched -- skipping it on that stale stamp is
/// exactly how its rows would keep null facts after the archive returns. And
/// the drop is recorded for the checkpoint merge, or it lives only in this
/// run's copy of the map and the file on disk keeps the entry.
///
/// Dropping the stamp is also what keeps a genuinely deleted file cheap. It
/// holds the pass open for one more sync, then it is no longer a path the
/// state knows about and the generation is recorded normally, instead of the
/// pass staying pending for the life of the install.
fn forget_unobserved_paths(
    state: &mut Map<String, Value>,
    stamps: &mut Map<String, Value>,
    stamp_map_key: &str,
    missing: Vec<String>,
) -> bool {
    if missing.is_empty() {
        return true;
    }
    let forgotten = state
        .entry(FORGOTTEN_PATHS_KEY)
        .or_insert_with(|| json!({}));
    if let Some(per_map) = forgotten.as_object_mut() {
        let list = per_map
            .entry(stamp_map_key)
            .or_insert_with(|| Value::Array(Vec::new()));
        if let Some(list) = list.as_array_mut() {
            for key in &missing {
                let entry = json!(key);
                if !list.contains(&entry) {
                    list.push(entry);
                }
            }
        }
    }
    for key in missing {
        stamps.remove(&key);
    }
    false
}

const RETIRED_SYNC_STATE_KEYS: &[(&str, &str)] = &[
    ("codex_rollouts", "codex_rollouts_v5"),
    ("codex_rollout_user_messages_v2", "codex_rollouts_v5"),
    ("codex_rollouts_v3", "codex_rollouts_v5"),
    ("codex_rollouts_v4", "codex_rollouts_v5"),
];

fn merged_sync_state(path: &Path, ours: &Map<String, Value>) -> Result<Option<Map<String, Value>>> {
    let mut merged = load_sync_state(path)?;
    let mut changed = false;
    for (key, value) in ours {
        // Applied after the fold, and never persisted.
        if key == FORGOTTEN_PATHS_KEY {
            continue;
        }
        let next = match merged.get(key) {
            Some(existing) if key == "cursor" => merge_file_cursor_map(existing, value),
            Some(existing) => merge_sync_value(existing, value),
            None => value.clone(),
        };
        if merged.get(key) == Some(&next) {
            continue;
        }
        merged.insert(key.clone(), next);
        changed = true;
    }
    // Sweep after the fold, so a run concurrent with this one cannot resurrect a
    // retired key, and only once this run actually carries the successor.
    for (retired, superseded_by) in RETIRED_SYNC_STATE_KEYS {
        if !ours.contains_key(*superseded_by) {
            continue;
        }
        if merged.remove(*retired).is_some() {
            changed = true;
        }
    }
    // The same idea one level down: entries this run deliberately dropped from
    // a stamp map. The fold above cannot express a removal -- it only overlays
    // the keys this run carries -- so they are applied here instead. A run
    // concurrent with this one may have just re-stamped one of these paths
    // because it could see the file; dropping its stamp costs that file one
    // re-read and never loses a row, which is the safe direction.
    if let Some(forgotten) = ours.get(FORGOTTEN_PATHS_KEY).and_then(Value::as_object) {
        for (stamp_map_key, paths) in forgotten {
            let Some(stamps) = merged.get_mut(stamp_map_key).and_then(Value::as_object_mut) else {
                continue;
            };
            for path in paths.as_array().into_iter().flatten() {
                let Some(path) = path.as_str() else { continue };
                if stamps.remove(path).is_some() {
                    changed = true;
                }
            }
        }
    }
    // An instruction, not state: it must not survive into the file, and an
    // older build that wrote one is cleaned up here.
    if merged.remove(FORGOTTEN_PATHS_KEY).is_some() {
        changed = true;
    }
    Ok(if changed { Some(merged) } else { None })
}

fn merge_sync_value(on_disk: &Value, ours: &Value) -> Value {
    match (FileCursor::decode(on_disk), FileCursor::decode(ours)) {
        (Some(DecodedFileCursor::Typed(on_disk)), Some(DecodedFileCursor::Typed(ours))) => {
            FileCursor::merge(on_disk, ours).to_value()
        }
        // Once a cursor carries a generation, a concurrent legacy writer cannot
        // safely replace it: its numeric offset may belong to the previous file.
        (Some(DecodedFileCursor::Typed(on_disk)), Some(DecodedFileCursor::Legacy(_))) => {
            on_disk.to_value()
        }
        (Some(DecodedFileCursor::Legacy(_)), Some(DecodedFileCursor::Typed(ours))) => {
            ours.to_value()
        }
        (Some(DecodedFileCursor::Legacy(on_disk)), Some(DecodedFileCursor::Legacy(ours))) => {
            json!(on_disk.max(ours))
        }
        _ if on_disk.is_object() && ours.is_object() => merge_object_values(on_disk, ours),
        _ if on_disk == ours => on_disk.clone(),
        _ => ours.clone(),
    }
}

fn merge_object_values(on_disk: &Value, ours: &Value) -> Value {
    let on_disk = on_disk.as_object().expect("checked object");
    let ours = ours.as_object().expect("checked object");
    let mut merged = on_disk.clone();
    for (key, value) in ours {
        let next = merged
            .get(key)
            .map_or_else(|| value.clone(), |saved| merge_sync_value(saved, value));
        merged.insert(key.clone(), next);
    }
    Value::Object(merged)
}

fn merge_file_cursor_map(on_disk: &Value, ours: &Value) -> Value {
    if !on_disk.is_object() || !ours.is_object() {
        return merge_sync_value(on_disk, ours);
    }
    merge_object_values(on_disk, ours)
}

const STALE_SYNC_STATE_TMP_AGE: Duration = Duration::from_secs(24 * 60 * 60);

#[cfg(unix)]
fn process_is_alive(pid: u32) -> bool {
    let Ok(pid) = libc::pid_t::try_from(pid) else {
        return false;
    };
    // SAFETY: signal 0 does not deliver a signal; it performs only existence
    // and permission checks. EPERM therefore still means the process is live.
    let result = unsafe { libc::kill(pid, 0) };
    result == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(not(unix))]
fn process_is_alive(_pid: u32) -> bool {
    // The age bound below still provides portable cleanup without guessing at
    // platform-specific process APIs.
    true
}

/// Remove uniquely named state temp files whose writer is gone or whose write
/// has been abandoned for a full day.
fn cleanup_stale_sync_state_temps(path: &Path) -> Result<usize> {
    let Some(parent) = path.parent() else {
        return Ok(0);
    };
    if !parent.exists() {
        return Ok(0);
    }
    let Some(state_name) = path.file_name().and_then(|name| name.to_str()) else {
        return Ok(0);
    };
    let prefix = format!("{state_name}.tmp.");
    let own_pid = std::process::id();
    let mut removed = 0;

    for entry in fs::read_dir(parent)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(suffix) = name.strip_prefix(&prefix) else {
            continue;
        };
        let owner_pid = suffix
            .split_once('.')
            .and_then(|(pid, _)| pid.parse::<u32>().ok());
        let abandoned_by_age = entry
            .metadata()
            .and_then(|metadata| metadata.modified())
            .ok()
            .and_then(|modified| modified.elapsed().ok())
            .is_some_and(|age| age >= STALE_SYNC_STATE_TMP_AGE);
        let owner_is_gone = owner_pid.is_some_and(|pid| pid != own_pid && !process_is_alive(pid));
        if !owner_is_gone && !abandoned_by_age {
            continue;
        }
        match fs::remove_file(entry.path()) {
            Ok(()) => removed += 1,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("removing stale sync state temp {}", entry.path().display())
                })
            }
        }
    }
    Ok(removed)
}

/// Writes via a temp file + rename so an interrupted or out-of-space write
/// leaves the previous state intact rather than a truncated file.
///
/// The temp name is unique per writer, not just per destination: `sync_basic`
/// runs from the CLI, from `watch_loop`, and from the in-process napi binding,
/// so two saves can overlap. Sharing one temp path would let them interleave
/// writes and rename a torn blend into place, or leave the slower writer's
/// rename failing on a path the faster one already moved — reintroducing the
/// class of failure this function exists to prevent.
fn save_sync_state(path: &Path, state: &Map<String, Value>) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    static NEXT_TMP_ID: AtomicU64 = AtomicU64::new(0);
    let tmp_path = path.with_extension(format!(
        "json.tmp.{}.{}",
        std::process::id(),
        NEXT_TMP_ID.fetch_add(1, AtomicOrdering::Relaxed)
    ));
    let saved = fs::write(&tmp_path, serde_json::to_string_pretty(state)? + "\n")
        .with_context(|| format!("writing sync state to {}", tmp_path.display()))
        .and_then(|()| {
            fs::rename(&tmp_path, path)
                .with_context(|| format!("replacing sync state at {}", path.display()))
        });
    if saved.is_err() {
        // Best effort: don't leave a stray temp file behind on a failed save.
        let _ = fs::remove_file(&tmp_path);
    }
    saved
}

/// Lines per transaction when ingesting a JSONL source.
///
/// Two jobs. It takes the write lock once per chunk instead of once per row,
/// which matters because this database has several concurrent writers and every
/// auto-commit insert is a separate lock acquisition. And it bounds how much
/// work a failure can destroy: the byte offset is checkpointed on each commit,
/// so an interrupted run resumes from the last committed chunk. Ingesting a
/// large backlog in one transaction would instead hold the write lock for
/// minutes and starve everyone else.
const JSONL_CHUNK_LINES: usize = 2_000;

/// A byte cursor is valid only for the file generation that produced it.
/// `observed_at_ns` orders overlapping writers across rotations, while the
/// identity and start metadata let writers recognize the same generation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct FileGeneration {
    #[serde(skip_serializing_if = "Option::is_none")]
    device: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    inode: Option<u64>,
    started_mtime_ns: u64,
    started_size: u64,
    observed_at_ns: u64,
    /// Monotonic per-path generation for rewrites that retain the same inode.
    #[serde(default)]
    rewrite_epoch: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct FileCursor {
    /// Bytes through the last newline whose database work has committed.
    offset: u64,
    generation: FileGeneration,
    /// Latest mtime seen for this generation, used to catch backwards rewrites.
    observed_mtime_ns: u64,
    /// SHA-256 of every complete byte through `offset`. Rebuilding and checking
    /// it detects in-place rewrites that regrow past the cursor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    prefix_hash: Option<String>,
}

enum DecodedFileCursor {
    Legacy(u64),
    Typed(FileCursor),
}

impl FileCursor {
    fn decode(value: &Value) -> Option<DecodedFileCursor> {
        if let Some(offset) = value.as_u64() {
            return Some(DecodedFileCursor::Legacy(offset));
        }
        serde_json::from_value(value.clone())
            .ok()
            .map(DecodedFileCursor::Typed)
    }

    fn to_value(&self) -> Value {
        serde_json::to_value(self).expect("file cursor serialization cannot fail")
    }

    fn same_generation(&self, other: &Self) -> bool {
        let same_identity = self.same_known_identity(other).unwrap_or({
            self.generation.started_mtime_ns == other.generation.started_mtime_ns
                && self.generation.started_size == other.generation.started_size
        });
        same_identity && self.generation.rewrite_epoch == other.generation.rewrite_epoch
    }

    fn same_known_identity(&self, other: &Self) -> Option<bool> {
        match (
            self.generation.device,
            self.generation.inode,
            other.generation.device,
            other.generation.inode,
        ) {
            (Some(left_device), Some(left_inode), Some(right_device), Some(right_inode)) => {
                Some((left_device, left_inode) == (right_device, right_inode))
            }
            _ => None,
        }
    }

    fn generation_order(&self) -> (u64, u64, Option<u64>, Option<u64>, u64) {
        (
            self.generation.observed_at_ns,
            self.generation.started_mtime_ns,
            self.generation.device,
            self.generation.inode,
            self.generation.started_size,
        )
    }

    fn merge(on_disk: Self, ours: Self) -> Self {
        if on_disk.same_generation(&ours) {
            let (mut winner, other) = if on_disk.offset >= ours.offset {
                (on_disk, ours)
            } else {
                (ours, on_disk)
            };
            winner.observed_mtime_ns = winner.observed_mtime_ns.max(other.observed_mtime_ns);
            winner
        } else if on_disk.same_known_identity(&ours) == Some(true)
            && on_disk.generation.rewrite_epoch != ours.generation.rewrite_epoch
        {
            if ours.generation.rewrite_epoch > on_disk.generation.rewrite_epoch {
                ours
            } else {
                on_disk
            }
        } else if ours.generation_order() > on_disk.generation_order() {
            ours
        } else {
            on_disk
        }
    }
}

fn metadata_mtime_ns(metadata: &fs::Metadata) -> u64 {
    metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(unix)]
fn metadata_identity(metadata: &fs::Metadata) -> (Option<u64>, Option<u64>) {
    use std::os::unix::fs::MetadataExt;
    (Some(metadata.dev()), Some(metadata.ino()))
}

#[cfg(not(unix))]
fn metadata_identity(_metadata: &fs::Metadata) -> (Option<u64>, Option<u64>) {
    (None, None)
}

struct CompleteJsonlReader {
    path: PathBuf,
    reader: BufReader<fs::File>,
    cursor: FileCursor,
    position: u64,
    prefix_hasher: Sha256,
    start_offset: u64,
    start_prefix_hash: String,
    validated_size: u64,
    validated_mtime_ns: u64,
    reset_cursor: Option<FileCursor>,
}

impl CompleteJsonlReader {
    fn open(path: &Path, saved: Option<&Value>) -> Result<Self> {
        let file = fs::File::open(path)?;
        let metadata = file.metadata()?;
        let size = metadata.len();
        let mtime_ns = metadata_mtime_ns(&metadata);
        let (device, inode) = metadata_identity(&metadata);
        let decoded = saved.and_then(FileCursor::decode);

        let (offset, generation, prefix_hasher) = match decoded {
            Some(DecodedFileCursor::Typed(saved)) => {
                let identity_changed = saved.generation.device.is_some()
                    && device.is_some()
                    && (saved.generation.device, saved.generation.inode) != (device, inode);
                let verified_prefix = (size >= saved.offset)
                    .then(|| hash_file_prefix(path, saved.offset))
                    .transpose()
                    .ok()
                    .flatten();
                let prefix_changed = verified_prefix.as_ref().is_none_or(|(current, _)| {
                    saved.prefix_hash.as_deref() != Some(current.as_str())
                });
                let replaced = identity_changed
                    || size < saved.offset
                    || mtime_ns < saved.observed_mtime_ns
                    || prefix_changed;
                if replaced {
                    (
                        0,
                        FileGeneration {
                            device,
                            inode,
                            started_mtime_ns: mtime_ns,
                            started_size: size,
                            observed_at_ns: now_ns(),
                            rewrite_epoch: saved.generation.rewrite_epoch.saturating_add(1),
                        },
                        Sha256::new(),
                    )
                } else {
                    (
                        saved.offset,
                        saved.generation,
                        verified_prefix
                            .expect("validated typed cursor has a prefix hash")
                            .1,
                    )
                }
            }
            // A numeric cursor has no generation or prefix identity. Seeking
            // to it could permanently skip the prefix of a replacement file,
            // so upgrade safely by rescanning once; inserts are idempotent.
            Some(DecodedFileCursor::Legacy(_)) => (
                0,
                FileGeneration {
                    device,
                    inode,
                    started_mtime_ns: mtime_ns,
                    started_size: size,
                    observed_at_ns: now_ns(),
                    rewrite_epoch: 0,
                },
                Sha256::new(),
            ),
            _ => (
                0,
                FileGeneration {
                    device,
                    inode,
                    started_mtime_ns: mtime_ns,
                    started_size: size,
                    observed_at_ns: now_ns(),
                    rewrite_epoch: 0,
                },
                Sha256::new(),
            ),
        };

        let prefix_hash = finish_prefix_hash(&prefix_hasher);
        let mut reader = BufReader::new(file);
        reader.seek(std::io::SeekFrom::Start(offset))?;
        Ok(Self {
            path: path.to_path_buf(),
            reader,
            cursor: FileCursor {
                offset,
                generation,
                observed_mtime_ns: mtime_ns,
                prefix_hash: Some(prefix_hash.clone()),
            },
            position: offset,
            prefix_hasher,
            start_offset: offset,
            start_prefix_hash: prefix_hash,
            validated_size: size,
            validated_mtime_ns: mtime_ns,
            reset_cursor: None,
        })
    }

    /// Returns only newline-terminated records. A partial final buffer remains
    /// uncommitted and will be read again after the writer completes it.
    fn next_line(&mut self, line: &mut String) -> Result<Option<u64>> {
        line.clear();
        let mut raw = Vec::new();
        let read = self.reader.read_until(b'\n', &mut raw)?;
        if read == 0 || raw.last() != Some(&b'\n') {
            return Ok(None);
        }
        line.push_str(&String::from_utf8_lossy(&raw));
        self.position += read as u64;
        self.prefix_hasher.update(&raw);
        Ok(Some(self.position))
    }

    fn committed_cursor(&mut self, offset: u64, force_validation: bool) -> Result<FileCursor> {
        if let Some(cursor) = &self.reset_cursor {
            return Ok(cursor.clone());
        }
        let current = fs::File::open(&self.path)?;
        let metadata = current.metadata()?;
        let mtime_ns = metadata_mtime_ns(&metadata);
        let identity = metadata_identity(&metadata);
        let expected_identity = (self.cursor.generation.device, self.cursor.generation.inode);
        let identity_changed =
            expected_identity.0.is_some() && identity.0.is_some() && identity != expected_identity;
        let metadata_changed = metadata.len() != self.validated_size
            || mtime_ns != self.validated_mtime_ns
            || identity_changed;
        let prefix_is_valid = (!force_validation && !metadata_changed)
            || (!identity_changed
                && metadata.len() >= self.start_offset
                && hash_file_prefix(&self.path, self.start_offset)
                    .is_ok_and(|(hash, _)| hash == self.start_prefix_hash));
        if !prefix_is_valid {
            let reset = FileCursor {
                offset: 0,
                generation: FileGeneration {
                    device: identity.0,
                    inode: identity.1,
                    started_mtime_ns: mtime_ns,
                    started_size: metadata.len(),
                    observed_at_ns: now_ns(),
                    rewrite_epoch: self.cursor.generation.rewrite_epoch.saturating_add(1),
                },
                observed_mtime_ns: mtime_ns,
                prefix_hash: Some(empty_prefix_hash()),
            };
            self.reset_cursor = Some(reset.clone());
            return Ok(reset);
        }
        self.validated_size = metadata.len();
        self.validated_mtime_ns = mtime_ns;
        let mut cursor = self.cursor.clone();
        cursor.offset = offset;
        cursor.observed_mtime_ns = mtime_ns;
        cursor.prefix_hash = Some(finish_prefix_hash(&self.prefix_hasher));
        Ok(cursor)
    }
}

fn empty_prefix_hash() -> String {
    format!("{:x}", Sha256::digest([]))
}

fn finish_prefix_hash(hasher: &Sha256) -> String {
    format!("{:x}", hasher.clone().finalize())
}

fn hash_file_prefix(path: &Path, offset: u64) -> Result<(String, Sha256)> {
    let mut file = fs::File::open(path)?;
    let mut remaining = offset;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    let mut last = None;
    while remaining > 0 {
        let wanted = usize::try_from(remaining.min(buffer.len() as u64)).unwrap_or(buffer.len());
        let read = file.read(&mut buffer[..wanted])?;
        anyhow::ensure!(read > 0, "cursor offset extends past end of file");
        hasher.update(&buffer[..read]);
        last = Some(buffer[read - 1]);
        remaining -= read as u64;
    }
    anyhow::ensure!(
        offset == 0 || last == Some(b'\n'),
        "cursor offset is not a complete-line boundary"
    );
    Ok((finish_prefix_hash(&hasher), hasher))
}

fn sync_jsonl_incremental(
    conn: &Connection,
    state: &mut Map<String, Value>,
    name: &str,
    path: &Path,
    parser: fn(&str) -> Result<Option<HistoryEntry>>,
    checkpoint: &mut dyn FnMut(&Map<String, Value>),
) -> Result<usize> {
    if !path.exists() {
        sync_note!("  [{name}] not found: {} (skipped)", path.display());
        return Ok(0);
    }
    let mut source = CompleteJsonlReader::open(path, state.get(name))?;
    let offset = source.position;
    let size = source.reader.get_ref().metadata()?.len();
    let opened_cursor = source.cursor.to_value();
    if offset >= size && state.get(name) == Some(&opened_cursor) {
        sync_note!("  [{name}] up to date");
        return Ok(0);
    }
    sync_note!(
        "  [{name}] syncing {} new bytes...",
        size.saturating_sub(offset)
    );
    let mut inserted = 0;
    let mut errors = 0;
    // Byte position of the last line handed to the database, tracked as we read
    // so a checkpoint records exactly what is committed. Generation and prefix
    // validation determine whether this offset remains valid across runs.
    let mut consumed = offset;
    let ingest = {
        let mut run = || -> Result<()> {
            conn.execute_batch("BEGIN")?;
            let mut pending = 0usize;
            let mut line = String::new();
            loop {
                let Some(position) = source.next_line(&mut line)? else {
                    break;
                };
                consumed = position;
                if !line.trim().is_empty() {
                    match parser(&line) {
                        Ok(Some(entry)) => inserted += insert_history(conn, &entry)?,
                        Ok(None) => {}
                        Err(_) => errors += 1,
                    }
                }
                pending += 1;
                if pending >= JSONL_CHUNK_LINES {
                    conn.execute_batch("COMMIT")?;
                    state.insert(
                        name.to_string(),
                        source.committed_cursor(consumed, false)?.to_value(),
                    );
                    checkpoint(state);
                    conn.execute_batch("BEGIN")?;
                    pending = 0;
                }
            }
            conn.execute_batch("COMMIT")?;
            state.insert(
                name.to_string(),
                source.committed_cursor(consumed, true)?.to_value(),
            );
            Ok(())
        };
        run()
    };
    if let Err(err) = ingest {
        // Drop the open chunk, then persist the offset of the chunks that did
        // commit so the next run resumes there instead of starting over.
        let _ = conn.execute_batch("ROLLBACK");
        checkpoint(state);
        return Err(err);
    }
    let suffix = if errors > 0 {
        format!(" ({errors} errors)")
    } else {
        String::new()
    };
    sync_note!("  [{name}] +{inserted} rows{suffix}");
    Ok(inserted)
}

fn sync_codex(conn: &Connection, state: &mut Map<String, Value>, home: &Path) -> Result<usize> {
    let (cwds, branches, mut inserted) = sync_codex_rollouts(conn, state, home)?;
    let path = home.join(".codex/history.jsonl");
    if !path.exists() {
        sync_note!("  [codex] not found: {} (skipped)", path.display());
        return Ok(inserted);
    }
    let mut source = CompleteJsonlReader::open(&path, state.get("codex"))?;
    let offset = source.position;
    let size = source.reader.get_ref().metadata()?.len();
    let mut errors = 0;
    let mut consumed = offset;
    if offset < size {
        sync_note!("  [codex] syncing {} new bytes...", size - offset);
        let mut line = String::new();
        while let Some(position) = source.next_line(&mut line)? {
            consumed = position;
            if line.trim().is_empty() {
                continue;
            }
            match parse_codex_line(&line) {
                Ok(Some(mut entry)) => {
                    if let Some(session_id) = entry.session_id.as_deref() {
                        if entry.project.is_none() {
                            entry.project = cwds.get(session_id).cloned();
                        }
                    }
                    inserted += insert_history(conn, &entry)?;
                }
                Ok(None) => {}
                Err(_) => errors += 1,
            }
        }
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
    let backfilled = backfill_codex_metadata(conn, &cwds, &branches)?;
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

/// One pass over every Codex rollout file: session metadata (cwd/branch maps
/// plus `sessions` rows), user prompts into `history`, and the full
/// conversation into `session_events` / `tool_calls` / `file_edits`.
///
/// Replaces the earlier split walks (state keys `codex_rollouts` and
/// `codex_rollout_user_messages_v2`) with one stamp map. The current
/// `codex_rollouts_v5` generation repairs the user-message parser change
/// and reclassifies existing `source.subagent` markers by re-reading unchanged
/// files once. Its per-file record carries the session id and classification so
/// a wiped database or an older standalone-guardian classification forces the
/// necessary re-ingestion even when the file stamp is unchanged. (session cwds,
/// session branches, prompts inserted).
type CodexRolloutWalk = (HashMap<String, String>, HashMap<String, String>, usize);

/// Reconcile the catalog registration for a locally observed subagent.
///
/// A local-only subagent stays out of the catalog. If the same canonical
/// session was separately discovered remotely, its retained local events are
/// also local evidence, so both presences and the canonical row must survive.
///
/// Deleting a `sessions` row fires `delete_session_hydration_state`, which
/// cascades away every relationship the row takes part in — as parent as well
/// as child. Undoing a subagent's registration is not that session going away,
/// so the observed delegation evidence, including the thread's own outgoing
/// edges, is carried across the delete. The trigger itself cannot be narrowed
/// instead: `CREATE TRIGGER IF NOT EXISTS` never replaces the one an existing
/// database already has.
fn cleanup_subagent_registration(conn: &Connection, source: &str, session_id: &str) -> Result<()> {
    conn.execute(
        "DELETE FROM session_presences \
         WHERE source = ? AND session_id = ? AND location = 'local' \
           AND NOT EXISTS (\
             SELECT 1 FROM session_presences \
             WHERE source = ? AND session_id = ? AND location = 'remote'\
           )",
        params![source, session_id, source, session_id],
    )?;
    conn.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS retained_relationships \
           AS SELECT * FROM session_relationships WHERE 0; \
         DELETE FROM retained_relationships;",
    )?;
    conn.execute(
        "INSERT INTO retained_relationships SELECT * FROM session_relationships \
         WHERE source = ? AND (parent_session_id = ? OR child_session_id = ?)",
        params![source, session_id, session_id],
    )?;
    conn.execute(
        "DELETE FROM sessions \
         WHERE source = ? AND session_id = ? \
           AND NOT EXISTS (\
             SELECT 1 FROM session_presences \
             WHERE source = ? AND session_id = ?\
           )",
        params![source, session_id, source, session_id],
    )?;
    conn.execute_batch(
        "INSERT OR IGNORE INTO session_relationships SELECT * FROM retained_relationships; \
         DELETE FROM retained_relationships;",
    )?;
    Ok(())
}

fn cleanup_codex_subagent_registration(conn: &Connection, session_id: &str) -> Result<()> {
    cleanup_subagent_registration(conn, "codex", session_id)
}

fn codex_session_has_remote_presence(conn: &Connection, session_id: &str) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(\
           SELECT 1 FROM session_presences \
           WHERE source = 'codex' AND session_id = ? AND location = 'remote'\
         )",
        [session_id],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

/// Remove prompt history that an older local sync incorrectly attributed to a
/// subagent. A remote presence makes the history canonical shared evidence, so
/// it must not be destroyed by a later local scan.
fn cleanup_codex_subagent_history(conn: &Connection, session_id: &str) -> Result<()> {
    if !codex_session_has_remote_presence(conn, session_id)? {
        conn.execute(
            "DELETE FROM history WHERE source = 'codex' AND session_id = ?",
            [session_id],
        )?;
    }
    Ok(())
}

fn sync_codex_rollouts(
    conn: &Connection,
    state: &mut Map<String, Value>,
    home: &Path,
) -> Result<CodexRolloutWalk> {
    let mut cwds = load_state_string_map(state, "codex_session_cwds");
    let mut branches = load_state_string_map(state, "codex_session_branches");
    let has_v5 = state.contains_key("codex_rollouts_v5");
    let has_v4 = state.contains_key("codex_rollouts_v4");
    // v4 already repaired user-message parsing. Its only stale knowledge is
    // the source.subagent classification, so a v4->v5 upgrade must not
    // re-read the complete archive: invalidate only marked subagent entries.
    let repair_user_messages = !has_v5 && !has_v4;
    let mut seen = state
        .get("codex_rollouts_v5")
        .or_else(|| state.get("codex_rollouts_v4"))
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    if !has_v5 && has_v4 {
        seen.retain(|_, record| record.get("subagent").and_then(Value::as_bool) != Some(true));
    }
    // Superseded stamp maps from the split-walk era; keeping them would carry
    // three path->stamp maps over the same 2K-file tree in .sync-state.json.
    state.remove("codex_rollouts");
    state.remove("codex_rollout_user_messages_v2");
    state.remove("codex_rollouts_v4");
    let backfill_raw_facts = raw_facts_backfill_pending(state, CODEX_RAW_MESSAGE_FACTS_KEY);
    // A root the stamp map has entries for but whose rollouts this run cannot
    // see is an archive we could not read, not an archive that is gone.
    // Walking it vacuously and then recording the generation would retire the
    // one-time backfill over rollouts nothing ever looked at.
    let mut walked_every_known_root = true;
    // Rollouts this run enumerated and then could not read. Collected rather
    // than handled inline because they are only unobserved if the state knew
    // about them: one that was never indexed has no rows to backfill, and
    // holding the pass open for it would never end.
    let mut unreadable: Vec<String> = Vec::new();
    // Whether the stamp map names rollouts under each root, read before the
    // walk starts inserting into it.
    let known_roots: Vec<bool> = [
        home.join(".codex/sessions"),
        home.join(".codex/archived_sessions"),
    ]
    .iter()
    .map(|root| state_names_files_under(&seen, root))
    .collect();
    let mut inserted = 0;
    let mut scanned = 0;
    let mut events = 0usize;
    for (index, root) in [
        home.join(".codex/sessions"),
        home.join(".codex/archived_sessions"),
    ]
    .into_iter()
    .enumerate()
    {
        let known_here = known_roots[index];
        if !root.exists() {
            if known_here {
                walked_every_known_root = false;
            }
            continue;
        }
        let rollouts = collect_matching_files(&root, "rollout-", "jsonl")?;
        // A readable root is not a fully readable root. A partially mounted
        // archive, or a sync client part way through pulling a tree down,
        // returns some of the rollouts the stamp map names and not others, and
        // the ones it did not return are still there and still null. So the
        // question is asked per path, not per root.
        if known_here {
            let missing = unobserved_known_paths(&seen, &root, &rollouts);
            if !forget_unobserved_paths(state, &mut seen, "codex_rollouts_v5", missing) {
                walked_every_known_root = false;
            }
        }
        for rollout in capture_files("codex", rollouts) {
            let key = rollout.to_string_lossy().to_string();
            let stamp = file_stamp(&rollout)?;
            let record = seen.get(&key).and_then(Value::as_object);
            let stamp_unchanged = record
                .map(|r| r.get("stamp").and_then(Value::as_str) == Some(stamp.as_str()))
                .unwrap_or(false);
            if stamp_unchanged {
                let recorded_session = record
                    .and_then(|r| r.get("session"))
                    .and_then(Value::as_str);
                if record
                    .and_then(|r| r.get("subagent"))
                    .and_then(Value::as_bool)
                    == Some(true)
                {
                    if let Some(session_id) = recorded_session {
                        // The fast path must still repair state from an older
                        // sync/migration. In particular, presence backfill can
                        // recreate a local catalog registration from retained
                        // subagent events without changing the rollout stamp.
                        cwds.remove(session_id);
                        branches.remove(session_id);
                        cleanup_codex_subagent_history(conn, session_id)?;
                        cleanup_codex_subagent_registration(conn, session_id)?;
                        // A database synced before delegation was recorded has
                        // no topology at all, and its stamps never change
                        // again. Re-reading one meta line per subagent
                        // backfills the edge without re-ingesting the rollout.
                        if !codex_delegation_recorded(conn, session_id)? {
                            if let Some(meta) = read_codex_session_meta(&rollout)? {
                                if let Some(parent) = meta.parent_session_id.as_deref() {
                                    record_codex_delegation(conn, parent, &meta, &rollout)?;
                                }
                            }
                        }
                    }
                }
                match recorded_session {
                    // No session id was recorded because the file had no
                    // usable session_meta; there is nothing to re-ingest.
                    None => continue,
                    Some(id)
                        if codex_session_events_exist(conn, id)?
                            && !(backfill_raw_facts
                                && events_lack_raw_facts(conn, "codex", id)?) =>
                    {
                        continue
                    }
                    // Stamp matches but the events are gone (wiped or rebuilt
                    // database): fall through and re-ingest.
                    _ => {}
                }
            }
            // Asked before the parse, because `read_codex_session_meta`
            // swallows the read error and then cannot tell an unreadable
            // rollout from one with no `session_meta` line -- and the branch
            // below stamps the latter as seen.
            if !transcript_is_readable(&rollout) {
                unreadable.push(key);
                continue;
            }
            let Some(meta) = read_codex_session_meta(&rollout)? else {
                seen.insert(key, json!({ "stamp": stamp }));
                continue;
            };
            scanned += 1;
            if meta.is_subagent {
                // Earlier syncs (before subagent detection) registered these
                // threads: their map entries feed backfill_codex_metadata and
                // their history rows feed session discovery, either of which
                // would resurrect the session row this walk refuses to create.
                cwds.remove(&meta.session_id);
                branches.remove(&meta.session_id);
                cleanup_codex_subagent_history(conn, &meta.session_id)?;
            } else {
                cwds.insert(meta.session_id.clone(), meta.cwd.clone());
                if let Some(branch) = &meta.git_branch {
                    branches.insert(meta.session_id.clone(), branch.clone());
                }
            }
            let outcome = if repair_user_messages {
                repair_codex_rollout_user_messages(conn, &rollout, &meta)
            } else {
                ingest_codex_rollout(conn, &rollout, &meta)
            };
            let cleanup = meta
                .is_subagent
                .then(|| cleanup_codex_subagent_registration(conn, &meta.session_id));
            let outcome = match outcome {
                Ok(outcome) => {
                    if let Some(cleanup) = cleanup {
                        cleanup?;
                    }
                    outcome
                }
                Err(error) => {
                    // Cleanup was attempted above. Preserve the ingestion
                    // failure as the primary diagnostic if both operations
                    // fail, since it explains why this rollout made no
                    // progress and is what a retry must address.
                    return Err(error);
                }
            };
            inserted += outcome.prompts;
            events += outcome.events;
            // Topology is recorded by the full sync too, so delegation is
            // queryable after a plain `sync` and not only after targeted
            // hydration of the parent.
            if meta.is_subagent {
                if let Some(parent) = meta.parent_session_id.as_deref() {
                    record_codex_delegation(conn, parent, &meta, &rollout)?;
                }
            }
            // Subagent threads (guardian/reviewer spawns) keep their events
            // under their own thread id but stay out of the session list.
            if !meta.is_subagent {
                if let Some(first) = outcome.first_ts {
                    upsert_session(
                        conn,
                        &meta.session_id,
                        "codex",
                        Some(&meta.cwd),
                        meta.git_branch.as_deref(),
                        first,
                        outcome.last_ts.unwrap_or(first),
                        outcome.last_assistant_text.as_deref(),
                        Some(&rollout.to_string_lossy()),
                    )?;
                    if let Some(first_prompt) = outcome.first_prompt.as_deref() {
                        conn.execute(
                            "UPDATE sessions SET first_prompt = ? \
                             WHERE source = 'codex' AND session_id = ?",
                            params![first_prompt, meta.session_id],
                        )?;
                    }
                }
            }
            seen.insert(
                key,
                json!({ "stamp": stamp, "session": meta.session_id, "subagent": meta.is_subagent }),
            );
        }
    }
    state.insert(
        "codex_session_cwds".to_string(),
        Value::Object(
            cwds.iter()
                .map(|(k, v)| (k.clone(), json!(v)))
                .collect::<Map<_, _>>(),
        ),
    );
    state.insert(
        "codex_session_branches".to_string(),
        Value::Object(
            branches
                .iter()
                .map(|(k, v)| (k.clone(), json!(v)))
                .collect::<Map<_, _>>(),
        ),
    );
    state.remove("codex_rollouts_v3");
    // Only the unreadable rollouts the state already knew about count against
    // the pass; one it never indexed has nothing to repair.
    unreadable.retain(|key| seen.contains_key(key));
    if !forget_unobserved_paths(state, &mut seen, "codex_rollouts_v5", unreadable) {
        walked_every_known_root = false;
    }
    state.insert("codex_rollouts_v5".to_string(), Value::Object(seen));
    record_raw_facts_backfill(state, CODEX_RAW_MESSAGE_FACTS_KEY, walked_every_known_root);
    if scanned > 0 {
        sync_note!(
            "  [codex-rollouts] scanned {scanned} files; +{inserted} prompts, +{events} events"
        );
    }
    Ok((cwds, branches, inserted))
}

fn load_state_string_map(state: &Map<String, Value>, key: &str) -> HashMap<String, String> {
    state
        .get(key)
        .and_then(Value::as_object)
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect::<HashMap<_, _>>()
        })
        .unwrap_or_default()
}

/// Record one Codex subagent rollout as a delegation of its parent thread.
///
/// The rollout's own `session_meta` is the evidence: the file locates it, the
/// thread id is the child's real identity, and the meta line's timestamp is
/// when the provider recorded the thread starting.
fn record_codex_delegation(
    conn: &Connection,
    parent_session_id: &str,
    meta: &CodexSessionMeta,
    rollout: &Path,
) -> Result<()> {
    let locator = rollout.to_string_lossy();
    record_relationship(
        conn,
        &ObservedRelationship {
            source: "codex",
            parent_session_id,
            child_session_id: Some(&meta.session_id),
            relationship: "delegated",
            child_agent_type: meta.subagent_label.as_deref(),
            child_agent_name: None,
            child_model: None,
            spawn_depth: None,
            evidence_kind: "codex_session_meta",
            evidence_locator: Some(&locator),
            evidence_ref: meta.parent_thread_id.as_deref(),
            child_has_events: codex_session_events_exist(conn, &meta.session_id)?,
            spawned_at_ms: meta.meta_ts_ms,
        },
    )
}

fn codex_delegation_recorded(conn: &Connection, child_session_id: &str) -> Result<bool> {
    let exists: i64 = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM session_relationships \
         WHERE source = 'codex' AND child_session_id = ? LIMIT 1)",
        [child_session_id],
        |row| row.get(0),
    )?;
    Ok(exists != 0)
}

fn session_events_exist(conn: &Connection, source: &str, session_id: &str) -> Result<bool> {
    let exists: i64 = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM session_events WHERE source = ? AND session_id = ? LIMIT 1)",
        params![source, session_id],
        |row| row.get(0),
    )?;
    Ok(exists != 0)
}

fn codex_session_events_exist(conn: &Connection, session_id: &str) -> Result<bool> {
    session_events_exist(conn, "codex", session_id)
}

struct CodexSessionMeta {
    session_id: String,
    cwd: String,
    git_branch: Option<String>,
    is_subagent: bool,
    /// The parent this thread was delegated by, from whichever field the
    /// provider recorded it in.
    parent_session_id: Option<String>,
    /// The provider-native parent-thread reference, whether top-level or in a
    /// structured thread-spawn source, kept separately from legacy `session_id`.
    parent_thread_id: Option<String>,
    /// The label under `payload.source.subagent`, when the rollout carries one.
    subagent_label: Option<String>,
    /// The `session_meta` line's own timestamp: when the provider recorded
    /// this thread starting.
    meta_ts_ms: Option<i64>,
}

/// Resolve the parent identity from every Codex session-meta shape observed in
/// local rollouts. Newer producers use the explicit top-level field, spawned
/// agents can retain it inside their structured source, and older producers
/// used `session_id` for the parent conversation.
fn codex_parent_thread_id(
    payload: Option<&serde_json::Map<String, Value>>,
    session_id: &str,
) -> Option<String> {
    let valid_parent = |value: Option<&Value>| {
        value
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty() && *id != session_id)
            .map(str::to_string)
    };

    valid_parent(payload.and_then(|p| p.get("parent_thread_id"))).or_else(|| {
        valid_parent(
            payload
                .and_then(|p| p.get("source"))
                .and_then(Value::as_object)
                .and_then(|source| source.get("subagent"))
                .and_then(Value::as_object)
                .and_then(|subagent| subagent.get("thread_spawn"))
                .and_then(Value::as_object)
                .and_then(|spawn| spawn.get("parent_thread_id")),
        )
    })
}

fn codex_parent_session_id(
    payload: Option<&serde_json::Map<String, Value>>,
    session_id: &str,
) -> Option<String> {
    codex_parent_thread_id(payload, session_id).or_else(|| {
        payload
            .and_then(|p| p.get("session_id"))
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty() && *id != session_id)
            .map(str::to_string)
    })
}

/// Codex rollouts marked as subagents are hidden from the root session
/// catalog.  A `thread_source: subagent` rollout remains a child even when an
/// older producer omitted its parent fields.  The object form of
/// `source.subagent` is treated as a child only when an explicit parent is
/// present, so a standalone guardian remains discoverable under `payload.id`.
pub(crate) fn codex_is_subagent(payload: Option<&Value>, session_id: &str) -> bool {
    let thread_source_is_subagent = payload
        .and_then(|p| p.get("thread_source"))
        .and_then(Value::as_str)
        == Some("subagent");
    let source_marks_subagent = payload
        .and_then(|p| p.get("source"))
        .and_then(Value::as_object)
        .is_some_and(|source| source.contains_key("subagent"));

    // `payload.id` is always the rollout's own identity. A `source.subagent`
    // marker is not by itself evidence of a parent: standalone guardian rollouts
    // carry that marker while keeping their own identity, so they stay
    // discoverable under `payload.id`.
    thread_source_is_subagent
        || (source_marks_subagent
            && codex_parent_session_id(payload.and_then(Value::as_object), session_id).is_some())
}

/// Read the `session_meta` line that opens every rollout file.
///
/// Sessions key on `payload.id` — the per-thread id. Subagent rollouts can
/// carry their parent in `parent_thread_id`, a structured thread-spawn source,
/// or the legacy `session_id`; keying on any of those would collapse every
/// subagent into its parent. Subagent threads are detected instead
/// (`thread_source`, or the object form of `payload.source` *together with* an
/// explicit parent) and excluded from session registration. A standalone
/// guardian carries `source.subagent` without a parent and stays discoverable.
fn read_codex_session_meta(path: &Path) -> Result<Option<CodexSessionMeta>> {
    let first = fs::read_to_string(path)
        .ok()
        .and_then(|text| text.lines().next().map(str::to_string))
        .unwrap_or_default();
    if first.trim().is_empty() {
        return Ok(None);
    }
    let value: Value = match serde_json::from_str(&first) {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };
    if value.get("type").and_then(Value::as_str) != Some("session_meta") {
        return Ok(None);
    }
    let payload_value = value.get("payload");
    let payload = payload_value.and_then(Value::as_object);
    let Some(session_id) = payload
        .and_then(|p| p.get("id"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    else {
        return Ok(None);
    };
    let Some(cwd) = payload
        .and_then(|p| p.get("cwd"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    else {
        return Ok(None);
    };
    let git_branch = payload
        .and_then(|p| p.get("git"))
        .and_then(Value::as_object)
        .and_then(|g| g.get("branch"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let subagent = payload
        .and_then(|p| p.get("source"))
        .and_then(Value::as_object)
        .and_then(|s| s.get("subagent"));
    let is_subagent = codex_is_subagent(payload_value, session_id);
    let parent_thread_id = is_subagent
        .then(|| codex_parent_thread_id(payload, session_id))
        .flatten();
    let parent_session_id = is_subagent
        .then(|| codex_parent_session_id(payload, session_id))
        .flatten();
    // The label is an object in the observed rollouts (`{"other":"guardian"}`);
    // its value names the agent, and its key is the only thing left when the
    // value is not a string.
    let subagent_label = subagent.and_then(|value| match value {
        Value::String(label) => Some(label.clone()),
        Value::Object(map) => map
            .values()
            .find_map(Value::as_str)
            .map(str::to_string)
            .or_else(|| map.keys().next().cloned()),
        _ => None,
    });
    let meta_ts_ms = value
        .get("timestamp")
        .and_then(Value::as_str)
        .and_then(parse_iso_ms);
    Ok(Some(CodexSessionMeta {
        session_id: session_id.to_string(),
        cwd: cwd.to_string(),
        git_branch,
        is_subagent,
        parent_session_id,
        parent_thread_id,
        subagent_label,
        meta_ts_ms,
    }))
}

#[derive(Default)]
struct CodexIngestOutcome {
    prompts: usize,
    events: usize,
    first_ts: Option<i64>,
    last_ts: Option<i64>,
    first_prompt: Option<String>,
    last_assistant_text: Option<String>,
}

/// Cumulative token totals from a Codex `token_count` event
/// (`info.total_token_usage`). `input` is inclusive of `cached_input`.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
struct CodexTokenTotals {
    input: i64,
    cached_input: i64,
    cache_write: i64,
    output: i64,
    reasoning_output: i64,
    total: i64,
}

impl CodexTokenTotals {
    fn from_usage(value: &Value) -> Option<Self> {
        let obj = value.as_object()?;
        let get = |key: &str| obj.get(key).and_then(Value::as_i64).unwrap_or(0);
        Some(Self {
            input: get("input_tokens"),
            cached_input: get("cached_input_tokens"),
            cache_write: get("cache_write_input_tokens"),
            output: get("output_tokens"),
            reasoning_output: get("reasoning_output_tokens"),
            total: get("total_tokens"),
        })
    }

    fn fields(&self) -> [i64; 6] {
        [
            self.input,
            self.cached_input,
            self.cache_write,
            self.output,
            self.reasoning_output,
            self.total,
        ]
    }

    /// Strictly-advancing snapshots mark a completed model request; identical
    /// repeats (Codex re-emits them) and regressions are not deltas.
    fn advanced_from(&self, prev: &Self) -> bool {
        let (a, b) = (self.fields(), prev.fields());
        a.iter().zip(b.iter()).all(|(x, y)| x >= y) && a != b
    }

    fn regressed_from(&self, prev: &Self) -> bool {
        self.fields()
            .iter()
            .zip(prev.fields().iter())
            .any(|(x, y)| x < y)
    }

    fn minus(&self, prev: &Self) -> Self {
        Self {
            input: (self.input - prev.input).max(0),
            cached_input: (self.cached_input - prev.cached_input).max(0),
            cache_write: (self.cache_write - prev.cache_write).max(0),
            output: (self.output - prev.output).max(0),
            reasoning_output: (self.reasoning_output - prev.reasoning_output).max(0),
            total: (self.total - prev.total).max(0),
        }
    }

    fn plus(&self, other: &Self) -> Self {
        Self {
            input: self.input + other.input,
            cached_input: self.cached_input + other.cached_input,
            cache_write: self.cache_write + other.cache_write,
            output: self.output + other.output,
            reasoning_output: self.reasoning_output + other.reasoning_output,
            total: self.total + other.total,
        }
    }

    fn to_token_json(self) -> String {
        json!({
            "input_tokens": self.input,
            "cached_input_tokens": self.cached_input,
            "cache_write_input_tokens": self.cache_write,
            "output_tokens": self.output,
            "reasoning_output_tokens": self.reasoning_output,
            "total_tokens": self.total,
        })
        .to_string()
    }
}

/// Ingest one rollout file's conversation into `session_events`,
/// `tool_calls`, and `file_edits`, and its user prompts into `history`.
///
/// Format notes (verified against rollouts spanning cli 0.36 to 0.148):
/// user text can arrive as either `event_msg/user_message` or
/// `response_item/message`; adjacent mirrored encodings of one turn are
/// collapsed. `response_item/reasoning` carries encrypted content while the
/// readable stream is `event_msg/agent_reasoning`. Tool calls are
/// `response_item` rows correlated by `call_id`. Token usage arrives as cumulative
/// `token_count` snapshots; consecutive strictly-advancing snapshots are
/// diffed into per-request deltas and attached to the nearest assistant
/// event, so summing `token_json` over a session equals the session total.
fn repair_codex_rollout_user_messages(
    conn: &Connection,
    path: &Path,
    meta: &CodexSessionMeta,
) -> Result<CodexIngestOutcome> {
    // One bounded transaction per rollout makes an interrupted parser upgrade
    // leave either the old user rows or the fully rebuilt ones. Assistant,
    // tool, and file-edit evidence is retained and idempotently upserted.
    let tx = conn.unchecked_transaction()?;
    tx.execute(
        "DELETE FROM session_events \
         WHERE source = 'codex' AND session_id = ? AND role = 'user'",
        [meta.session_id.as_str()],
    )?;
    if meta.is_subagent {
        cleanup_codex_subagent_history(&tx, &meta.session_id)?;
    } else {
        tx.execute(
            "DELETE FROM history WHERE source = 'codex' AND session_id = ?",
            [meta.session_id.as_str()],
        )?;
    }
    let outcome = ingest_codex_rollout(&tx, path, meta)?;
    tx.commit()?;
    Ok(outcome)
}

fn ingest_codex_rollout(
    conn: &Connection,
    path: &Path,
    meta: &CodexSessionMeta,
) -> Result<CodexIngestOutcome> {
    let file = fs::File::open(path)?;
    let session_id = meta.session_id.as_str();
    let cwd = Some(meta.cwd.as_str());
    let branch = meta.git_branch.as_deref();
    let mut outcome = CodexIngestOutcome::default();
    let mut model: Option<String> = None;
    let mut prev_totals: Option<CodexTokenTotals> = None;
    let mut pending_delta: Option<CodexTokenTotals> = None;
    let mut untokened_assistant_uid: Option<String> = None;
    // The turn a record falls inside, stamped from the last `turn_context`
    // until the next one names a different turn. Codex writes it once per turn
    // rather than on every record, so carrying it forward is what makes turn
    // boundaries recoverable downstream.
    let mut turn_id: Option<String> = None;
    let mut saw_model_output = false;
    let mut human_messages = codex::HumanMessageDeduper::default();
    let mut reader = BufReader::new(file);
    let mut raw = Vec::new();
    let mut line_index = 0usize;
    loop {
        raw.clear();
        if reader.read_until(b'\n', &mut raw)? == 0 {
            break;
        }
        // A line without its newline is the half-written tail of a live
        // session; the next sync re-reads the whole file.
        if raw.last() != Some(&b'\n') {
            break;
        }
        let index = line_index;
        line_index += 1;
        let Ok(text) = std::str::from_utf8(&raw) else {
            continue;
        };
        let Ok(value) = serde_json::from_str::<Value>(text.trim()) else {
            continue;
        };
        let Some(payload) = value.get("payload").and_then(Value::as_object) else {
            continue;
        };
        let line_type = value.get("type").and_then(Value::as_str).unwrap_or("");
        let ts_ms = value
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(parse_iso_ms)
            .unwrap_or(0);
        if ts_ms > 0 {
            outcome.first_ts.get_or_insert(ts_ms);
            outcome.last_ts = Some(outcome.last_ts.map_or(ts_ms, |last| last.max(ts_ms)));
        }
        let payload_type = payload.get("type").and_then(Value::as_str).unwrap_or("");
        let payload_str = |key: &str| payload.get(key).and_then(Value::as_str);
        if let Some(message) = human_messages.observe(&value) {
            let suffix = match message.format {
                codex::HumanMessageFormat::EventMessage => "user_message",
                codex::HumanMessageFormat::ResponseItem => "response_item_user_message",
            };
            let uid = format!("{index}:{suffix}");
            let message_id = message.message_id.as_deref().unwrap_or(uid.as_str());
            insert_codex_event(
                conn,
                session_id,
                cwd,
                branch,
                ts_ms,
                "user",
                "text",
                &message.text,
                &uid,
                message_id,
                None,
                None,
                turn_id.as_deref(),
            )?;
            outcome.events += 1;
            // A subagent's "user" turns are the parent agent's task prompts;
            // only human threads feed prompt history and session discovery.
            if !meta.is_subagent {
                outcome
                    .first_prompt
                    .get_or_insert_with(|| message.text.chars().take(4096).collect());
                outcome.prompts += insert_history(
                    conn,
                    &HistoryEntry {
                        id: 0,
                        source: "codex".into(),
                        session_id: Some(meta.session_id.clone()),
                        project: Some(meta.cwd.clone()),
                        prompt_hash: Some(prompt_hash(&message.text)),
                        prompt: message.text,
                        timestamp_ms: ts_ms,
                    },
                )?;
            }
            continue;
        }
        match line_type {
            "turn_context" => {
                if let Some(m) = payload_str("model") {
                    model = Some(m.to_string());
                }
                // A turn_context without a turn_id closes the previous turn
                // rather than extending it: stamping the stale id onto the new
                // turn's records would fabricate a boundary that is not there.
                turn_id = payload_str("turn_id").map(str::to_string);
            }
            "event_msg" => match payload_type {
                "user_message" => {}
                "agent_message" => {
                    if let Some(message) = payload_str("message").filter(|m| !m.trim().is_empty()) {
                        let uid = format!("{index}:agent_message");
                        let token_json = pending_delta.take().map(CodexTokenTotals::to_token_json);
                        insert_codex_event(
                            conn,
                            session_id,
                            cwd,
                            branch,
                            ts_ms,
                            "assistant",
                            "text",
                            message.trim(),
                            &uid,
                            &uid,
                            model.as_deref(),
                            token_json.as_deref(),
                            turn_id.as_deref(),
                        )?;
                        outcome.events += 1;
                        untokened_assistant_uid = token_json.is_none().then(|| uid.clone());
                        outcome.last_assistant_text =
                            Some(message.trim().chars().take(4096).collect());
                        saw_model_output = true;
                    }
                }
                "agent_reasoning" => {
                    if let Some(reasoning) = payload_str("text").filter(|t| !t.trim().is_empty()) {
                        let uid = format!("{index}:agent_reasoning");
                        insert_codex_event(
                            conn,
                            session_id,
                            cwd,
                            branch,
                            ts_ms,
                            "assistant",
                            "thinking",
                            reasoning.trim(),
                            &uid,
                            &uid,
                            model.as_deref(),
                            None,
                            turn_id.as_deref(),
                        )?;
                        outcome.events += 1;
                        untokened_assistant_uid = Some(uid);
                        saw_model_output = true;
                    }
                }
                "token_count" => {
                    let Some(totals) = payload
                        .get("info")
                        .and_then(|info| info.get("total_token_usage"))
                        .and_then(CodexTokenTotals::from_usage)
                    else {
                        continue;
                    };
                    match prev_totals {
                        // The first snapshot before any model output is the
                        // carried-over baseline of a resumed session (a fresh
                        // session's opening snapshot has `info: null`).
                        None if !saw_model_output => prev_totals = Some(totals),
                        // A regressed snapshot is treated as a transient
                        // glitch: keeping the prior baseline means the next
                        // advancing snapshot's delta covers exactly the spend
                        // since that baseline, so per-event sums still
                        // reproduce the cumulative totals.
                        Some(prev) if totals.regressed_from(&prev) => {}
                        Some(prev) if !totals.advanced_from(&prev) => {}
                        _ => {
                            let baseline = prev_totals.unwrap_or_default();
                            let mut delta = totals.minus(&baseline);
                            prev_totals = Some(totals);
                            if let Some(pending) = pending_delta.take() {
                                delta = delta.plus(&pending);
                            }
                            if let Some(uid) = untokened_assistant_uid.take() {
                                conn.execute(
                                    "UPDATE session_events SET token_json = ? \
                                     WHERE source = 'codex' AND session_id = ? AND event_uid = ?",
                                    params![delta.to_token_json(), session_id, uid],
                                )?;
                            } else {
                                pending_delta = Some(delta);
                            }
                        }
                    }
                }
                "task_complete" => {
                    if let Some(message) =
                        payload_str("last_agent_message").filter(|m| !m.trim().is_empty())
                    {
                        outcome.last_assistant_text =
                            Some(message.trim().chars().take(4096).collect());
                    }
                }
                "thread_settings_applied" => {
                    if let Some(m) = payload
                        .get("thread_settings")
                        .and_then(|s| s.get("model"))
                        .and_then(Value::as_str)
                    {
                        model = Some(m.to_string());
                    }
                }
                "mcp_tool_call_end" => {
                    let Some(call_id) = payload_str("call_id").filter(|s| !s.is_empty()) else {
                        continue;
                    };
                    let invocation = payload.get("invocation");
                    let name = invocation
                        .map(|inv| {
                            format!(
                                "{}.{}",
                                inv.get("server").and_then(Value::as_str).unwrap_or("mcp"),
                                inv.get("tool").and_then(Value::as_str).unwrap_or("tool"),
                            )
                        })
                        .unwrap_or_else(|| "mcp.tool".to_string());
                    let args_json = invocation
                        .and_then(|inv| serde_json::to_string(inv).ok())
                        .unwrap_or_else(|| "null".to_string());
                    let is_error = payload
                        .get("result")
                        .and_then(Value::as_object)
                        .map(|r| r.contains_key("Err"));
                    insert_tool_call(
                        conn,
                        "codex",
                        session_id,
                        &format!("{index}:mcp_tool_call_end"),
                        call_id,
                        &name,
                        None,
                        &args_json,
                        is_error,
                        ts_ms,
                    )?;
                }
                "web_search_end" => {
                    let Some(call_id) = payload_str("call_id").filter(|s| !s.is_empty()) else {
                        continue;
                    };
                    insert_tool_call(
                        conn,
                        "codex",
                        session_id,
                        &format!("{index}:web_search_end"),
                        call_id,
                        "web_search",
                        payload_str("query"),
                        "null",
                        None,
                        ts_ms,
                    )?;
                }
                "patch_apply_end" => {
                    let Some(call_id) = payload_str("call_id").filter(|s| !s.is_empty()) else {
                        continue;
                    };
                    let success = payload.get("success").and_then(Value::as_bool);
                    if let Some(success) = success {
                        set_tool_call_error(conn, "codex", session_id, call_id, !success)?;
                    }
                    let Some(changes) = payload.get("changes").and_then(Value::as_object) else {
                        continue;
                    };
                    for (file_path, change) in changes {
                        // One patch can touch several files; file_edits keys
                        // on tool_use_id, so scope the id per path.
                        let edit_id = format!("{call_id}#{file_path}");
                        upsert_file_edit_from_call(
                            conn,
                            "codex",
                            session_id,
                            &format!("{index}:patch_apply_end"),
                            &edit_id,
                            file_path,
                            "apply_patch",
                            ts_ms,
                            branch,
                            cwd,
                        )?;
                        let diff = change
                            .get("unified_diff")
                            .and_then(Value::as_str)
                            .unwrap_or("");
                        let (added, removed) = count_unified_diff_lines(diff);
                        conn.execute(
                            "UPDATE file_edits SET lines_added = ?, lines_removed = ?, structured_patch_json = ? \
                             WHERE source = 'codex' AND session_id = ? AND tool_use_id = ?",
                            params![
                                added,
                                removed,
                                serde_json::to_string(change).ok(),
                                session_id,
                                edit_id,
                            ],
                        )?;
                    }
                }
                "exec_command_end" => {
                    if let Some(call_id) = payload_str("call_id").filter(|s| !s.is_empty()) {
                        if let Some(exit_code) = payload.get("exit_code").and_then(Value::as_i64) {
                            set_tool_call_error(
                                conn,
                                "codex",
                                session_id,
                                call_id,
                                exit_code != 0,
                            )?;
                        }
                    }
                }
                _ => {}
            },
            "response_item" => match payload_type {
                "function_call" | "custom_tool_call" => {
                    let name = payload_str("name").unwrap_or("");
                    let call_id = payload_str("call_id").unwrap_or("");
                    let args = if payload_type == "function_call" {
                        let raw_args = payload_str("arguments").unwrap_or("");
                        serde_json::from_str::<Value>(raw_args)
                            .unwrap_or_else(|_| json!({ "arguments": raw_args }))
                    } else {
                        json!({ "input": payload_str("input").unwrap_or("") })
                    };
                    let target = if name == "apply_patch" {
                        codex_apply_patch_target(payload_str("input").unwrap_or(""))
                    } else {
                        codex_pick_tool_target(name, &args)
                    };
                    let uid = format!("{index}:{payload_type}");
                    let message_id = payload_str("id").unwrap_or(uid.as_str()).to_string();
                    let event_text = format_tool_event_text(name, target.as_deref(), &args);
                    let token_json = pending_delta.take().map(CodexTokenTotals::to_token_json);
                    insert_codex_event(
                        conn,
                        session_id,
                        cwd,
                        branch,
                        ts_ms,
                        "assistant",
                        "tool_use",
                        &event_text,
                        &uid,
                        &message_id,
                        model.as_deref(),
                        token_json.as_deref(),
                        turn_id.as_deref(),
                    )?;
                    outcome.events += 1;
                    untokened_assistant_uid = token_json.is_none().then(|| uid.clone());
                    saw_model_output = true;
                    if !call_id.is_empty() && !name.is_empty() {
                        let args_json =
                            serde_json::to_string(&args).unwrap_or_else(|_| "null".to_string());
                        insert_tool_call(
                            conn,
                            "codex",
                            session_id,
                            &message_id,
                            call_id,
                            name,
                            target.as_deref(),
                            &args_json,
                            None,
                            ts_ms,
                        )?;
                    }
                }
                "function_call_output" | "custom_tool_call_output" => {
                    if let Some(output_text) =
                        materialize_codex_output_text(payload.get("output").unwrap_or(&Value::Null))
                    {
                        let uid = format!("{index}:{payload_type}");
                        let message_id = payload_str("id").unwrap_or(uid.as_str()).to_string();
                        insert_codex_event(
                            conn,
                            session_id,
                            cwd,
                            branch,
                            ts_ms,
                            "tool_result",
                            "tool_result",
                            &output_text,
                            &uid,
                            &message_id,
                            None,
                            None,
                            turn_id.as_deref(),
                        )?;
                        outcome.events += 1;
                    }
                }
                // Readable reasoning arrives as event_msg/agent_reasoning;
                // this row is encrypted, but it still marks model output.
                "reasoning" => saw_model_output = true,
                // Assistant `response_item` messages still duplicate the
                // readable event_msg stream, but mark model output for token
                // baselines. User messages were handled canonically above.
                "message" if payload_str("role") == Some("assistant") => saw_model_output = true,
                _ => {}
            },
            _ => {}
        }
    }
    Ok(outcome)
}

#[allow(clippy::too_many_arguments)]
fn insert_codex_event(
    conn: &Connection,
    session_id: &str,
    cwd: Option<&str>,
    branch: Option<&str>,
    ts_ms: i64,
    role: &str,
    kind: &str,
    text: &str,
    uid: &str,
    message_id: &str,
    model: Option<&str>,
    token_json: Option<&str>,
    turn_id: Option<&str>,
) -> Result<()> {
    insert_session_event(
        conn,
        "codex",
        session_id,
        cwd,
        cwd,
        branch,
        message_id,
        None,
        ts_ms,
        role,
        kind,
        Some(text),
        model,
        token_json,
        uid,
        RawMessageFacts {
            turn_id,
            ..RawMessageFacts::default()
        },
    )
}

fn codex_pick_tool_target(name: &str, args: &Value) -> Option<String> {
    let obj = args.as_object()?;
    let get = |keys: &[&str]| {
        keys.iter()
            .find_map(|k| obj.get(*k).and_then(Value::as_str))
            .map(str::to_string)
    };
    match name {
        "exec_command" | "shell" | "exec" => get(&["cmd", "command"]),
        "read_file" | "write_file" => get(&["path", "file_path"]),
        _ => get(&["path", "file_path", "cmd", "command", "url", "query"]),
    }
}

fn codex_apply_patch_target(input: &str) -> Option<String> {
    input.lines().find_map(|line| {
        let line = line.trim();
        ["*** Update File: ", "*** Add File: ", "*** Delete File: "]
            .iter()
            .find_map(|prefix| line.strip_prefix(prefix))
            .map(str::to_string)
    })
}

fn materialize_codex_output_text(output: &Value) -> Option<String> {
    match output {
        Value::String(s) => (!s.trim().is_empty()).then(|| s.clone()),
        Value::Array(items) => {
            let parts = items
                .iter()
                .filter_map(|item| item.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>();
            (!parts.is_empty()).then(|| parts.join("\n"))
        }
        Value::Null => None,
        other => serde_json::to_string(other).ok(),
    }
}

fn count_unified_diff_lines(diff: &str) -> (i64, i64) {
    let mut added = 0;
    let mut removed = 0;
    for line in diff.lines() {
        if line.starts_with("+++") || line.starts_with("---") {
            continue;
        }
        if line.starts_with('+') {
            added += 1;
        } else if line.starts_with('-') {
            removed += 1;
        }
    }
    (added, removed)
}

fn backfill_codex_metadata(
    conn: &Connection,
    cwds: &HashMap<String, String>,
    branches: &HashMap<String, String>,
) -> Result<usize> {
    let mut updated = 0;
    for (session_id, cwd) in cwds {
        let branch = branches.get(session_id);
        updated += conn.execute(
            "UPDATE history SET project = COALESCE(project, ?), git_branch = COALESCE(git_branch, ?) WHERE source = 'codex' AND session_id = ? AND (project IS NULL OR git_branch IS NULL)",
            params![cwd, branch, session_id],
        )?;
        let (first, last): (Option<i64>, Option<i64>) = conn.query_row(
            "SELECT MIN(timestamp_ms), MAX(timestamp_ms) FROM history WHERE source = 'codex' AND session_id = ?",
            [session_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if let Some(first) = first {
            upsert_session(
                conn,
                session_id,
                "codex",
                Some(cwd),
                branch.map(String::as_str),
                first,
                last.unwrap_or(first),
                None,
                None,
            )?;
        }
    }
    Ok(updated)
}

fn sync_claude_session_metadata(
    conn: &Connection,
    state: &mut Map<String, Value>,
    root: &Path,
) -> Result<()> {
    // Load-bearing for the raw-facts generation below: an absent root returns
    // before anything is recorded, so a run that could not see the archive
    // does not retire the one-time backfill pass over it.
    if !root.exists() {
        return Ok(());
    }
    // The v3 key forces one full re-scan on upgrade so exact remote/local ids
    // are cached for bounded post-discovery correlation. The earlier v2 pass
    // healed sidechains that had been attributed to their parent.
    let mut session_state = state
        .get("claude_sessions_v3")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    state.remove("claude_sessions");
    state.remove("claude_sessions_v2");
    let backfill_raw_facts = raw_facts_backfill_pending(state, CLAUDE_RAW_MESSAGE_FACTS_KEY);
    // Same question the codex walk asks of its roots, and asked the same way:
    // per path, because a project tree can be readable while part of it is
    // not. A transcript the stamp map names that this run did not see has rows
    // that are still there and still null, so it withholds the generation and
    // loses the stamp this run can no longer vouch for.
    let transcripts = collect_matching_files(root, "", "jsonl")?;
    let missing = unobserved_known_paths(&session_state, root, &transcripts);
    let mut walked_every_known_root =
        forget_unobserved_paths(state, &mut session_state, "claude_sessions_v3", missing);
    // Transcripts this run enumerated and then could not read. Only the ones
    // the state already knew about count against the pass; one that was never
    // indexed has no rows to backfill.
    let mut unreadable: Vec<String> = Vec::new();
    let mut scanned = 0;
    let mut upserted = 0;
    for path in capture_files("claude", transcripts) {
        let key = path.to_string_lossy().to_string();
        let stamp = claude_sync_stamp(&path)?;
        if session_state.get(&key).and_then(Value::as_str) == Some(stamp.as_str())
            && (claude_transcript_events_exist(conn, &path)?
                || claude_sidecar_evidence_exists(conn, &path)?)
            // During the one-time backfill pass, an unchanged transcript
            // whose rows predate the per-message raw facts is re-read to
            // populate them. Outside that pass the stamp alone decides, so a
            // row this transcript does not own -- an adapter's contribution to
            // the same session -- cannot pin the file off the fast path.
            && !(backfill_raw_facts && claude_transcript_lacks_raw_facts(conn, &path)?)
        {
            continue;
        }
        // Asked before the parse, because `scan_claude_session_file` reads
        // with `unwrap_or_default()` and an unreadable transcript is then
        // indistinguishable from an empty one -- which would be stamped as
        // seen on the next line.
        if !transcript_is_readable(&path) {
            unreadable.push(key);
            continue;
        }
        scanned += 1;
        session_state.insert(key, json!(stamp));
        if let Some(meta) = scan_claude_session_file(&path)? {
            // A subagent sidecar carries the parent's `sessionId` but is not
            // that session: registering it would overwrite the parent's
            // locator with the sidecar path, and ingesting it unattributed
            // would pull the child's output back onto the parent that
            // hydration just moved it off.
            if meta.subagent {
                // Topology is recorded by the full sync too, so delegation is
                // queryable after a plain `sync` and not only after targeted
                // hydration of the parent. The sidecar's records name that
                // parent, so this walk reaches the same edge from the child's
                // side, through the very code hydration uses: a named child
                // is an observed row, an unnamed one is unlinked evidence.
                let evidence = hydrate::claude_subagent_evidence(path.clone(), &meta);
                hydrate::ingest_claude_subagent(conn, &meta.session_id, &evidence)?;
                continue;
            }
            upsert_session(
                conn,
                &meta.session_id,
                "claude",
                meta.cwd.as_deref(),
                meta.git_branch.as_deref(),
                meta.first_ts,
                meta.last_ts,
                meta.last_assistant_text.as_deref(),
                Some(&path.to_string_lossy()),
            )?;
            ingest_claude_transcript(conn, &path)?;
            record_claude_remote_relationship(conn, &meta)?;
            upserted += 1;
        }
    }
    unreadable.retain(|key| session_state.contains_key(key));
    if !forget_unobserved_paths(state, &mut session_state, "claude_sessions_v3", unreadable) {
        walked_every_known_root = false;
    }
    state.insert(
        "claude_sessions_v3".to_string(),
        Value::Object(session_state),
    );
    record_raw_facts_backfill(state, CLAUDE_RAW_MESSAGE_FACTS_KEY, walked_every_known_root);
    if scanned > 0 {
        sync_note!("  [claude-sessions] scanned {scanned} files, {upserted} sessions updated");
    }
    Ok(())
}

/// What a Claude transcript is skipped on when nothing about it changed.
///
/// A subagent sidecar's `agent-<agentId>.meta.json` is the only place the
/// child's type, name, model and spawn depth are recorded, so metadata that
/// changes beside an untouched transcript is still new evidence and has to
/// reach `session_relationships`. Hydration stamps its source snapshot by the
/// same rule.
fn claude_sync_stamp(path: &Path) -> Result<String> {
    let mut stamp = file_stamp(path)?;
    let metadata = hydrate::claude_subagent_meta_path(path);
    if metadata.is_file() {
        stamp.push('|');
        stamp.push_str(&file_stamp(&metadata)?);
    }
    Ok(stamp)
}

/// Whether an unchanged subagent sidecar has already been ingested.
///
/// A sidecar is deliberately never registered as a session, so the catalog
/// join [`claude_transcript_events_exist`] makes can never confirm one and
/// every sidecar would otherwise be re-read and re-ingested on every sync,
/// however unchanged it is. Its recorded delegation is the equivalent proof,
/// paired with the events it produced — under the child it named, or under
/// the parent when it named none — so a wiped or rebuilt database still
/// re-reads the file.
fn claude_sidecar_evidence_exists(conn: &Connection, path: &Path) -> Result<bool> {
    let locator = path.to_string_lossy();
    let exists: i64 = conn.query_row(
        "SELECT EXISTS(
            SELECT 1
            FROM session_relationships r
            WHERE r.source = 'claude' AND r.evidence_locator = ?
              AND EXISTS(
                SELECT 1 FROM session_events e
                WHERE e.source = 'claude'
                  AND e.session_id = COALESCE(r.child_session_id, r.parent_session_id)
              )
            LIMIT 1
        )",
        [locator.as_ref()],
        |row| row.get(0),
    )?;
    Ok(exists != 0)
}

/// Whether a session still holds locally parsed rows indexed before the
/// per-message raw provider facts existed.
///
/// A schema migration adds the nullable columns and writes its marker; the
/// sync stamp maps are what decide whether a provider transcript is opened at
/// all. Without this probe a database that migrated cleanly skips every
/// unchanged transcript, and plain `sync` leaves `request_id`, `stop_reason`,
/// `agent_version`, `is_sidechain`, `is_meta` and `turn_id` null on every row
/// that was already indexed -- while reporting a perfectly successful sync.
/// Only explicit hydration, which `HYDRATION_PARSER_VERSION` covers, or an
/// unrelated edit to the file would ever repair them.
///
/// `raw_facts_version` is the field to test, and it is the reason that column
/// exists: the parser stamps it on every event it writes whatever the record
/// contained. None of the six facts can play that role -- a real record
/// legitimately has no `request_id`, no `stop_reason` and no `turn_id`, and
/// Codex sets none of the other three -- so a probe on any of them would
/// select transcripts that have nothing to gain.
///
/// This only selects which files a backfill pass re-reads; it is not what ends
/// the pass. `session_events` is keyed by `(source, session_id)` and local and
/// remote observations of one session share that identity, so a row this probe
/// sees may be an adapter's contribution that re-reading the local transcript
/// will never stamp. `RAW_MESSAGE_FACTS_GENERATION` is what guarantees the
/// work happens once.
///
/// Selecting files this way is narrower than bumping the stamp-map generation,
/// which would re-read every transcript in the archive, including the ones
/// with nothing to gain, and would discard the selective-repair state the
/// codex generations carry.
fn events_lack_raw_facts(conn: &Connection, source: &str, session_id: &str) -> Result<bool> {
    let lacking: i64 = conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM session_events e
            WHERE e.source = ? AND e.session_id = ?
              AND COALESCE(e.raw_facts_version, 0) < ?
            LIMIT 1
        )",
        params![source, session_id, RAW_MESSAGE_FACTS_VERSION],
        |row| row.get(0),
    )?;
    Ok(lacking != 0)
}

/// The same question for a Claude transcript, which the walk knows by path.
///
/// A file reaches its rows by one of two routes, matching the two ways the
/// walk already decides a transcript is indexed. A session transcript owns the
/// `sessions` row carrying its `raw_path`. A subagent sidecar never gets one:
/// the walk hands it to `ingest_claude_subagent` and skips the catalog upsert
/// entirely, so its rows are reachable only through the relationship whose
/// `evidence_locator` is the sidecar's own path. Asking through `raw_path`
/// alone left every sidecar out of the backfill -- unchanged on disk, never
/// re-read, its events keeping null facts for good.
fn claude_transcript_lacks_raw_facts(conn: &Connection, path: &Path) -> Result<bool> {
    let raw_path = path.to_string_lossy();
    let lacking: i64 = conn.query_row(
        "SELECT
            EXISTS(
                SELECT 1
                FROM sessions s
                JOIN session_events e ON e.source = s.source AND e.session_id = s.session_id
                WHERE s.source = 'claude' AND s.raw_path = ?1
                  AND COALESCE(e.raw_facts_version, 0) < ?2
                LIMIT 1
            )
            OR EXISTS(
                SELECT 1
                FROM session_relationships r
                JOIN session_events e
                  ON e.source = 'claude'
                 AND e.session_id = COALESCE(r.child_session_id, r.parent_session_id)
                WHERE r.source = 'claude' AND r.evidence_locator = ?1
                  AND COALESCE(e.raw_facts_version, 0) < ?2
                LIMIT 1
            )",
        params![raw_path.as_ref(), RAW_MESSAGE_FACTS_VERSION],
        |row| row.get(0),
    )?;
    Ok(lacking != 0)
}

fn claude_transcript_events_exist(conn: &Connection, path: &Path) -> Result<bool> {
    let raw_path = path.to_string_lossy();
    let exists: i64 = conn.query_row(
        "SELECT EXISTS(
            SELECT 1
            FROM sessions s
            JOIN session_events e ON e.source = s.source AND e.session_id = s.session_id
            WHERE s.source = 'claude' AND s.raw_path = ?
            LIMIT 1
        )",
        [raw_path.as_ref()],
        |row| row.get(0),
    )?;
    Ok(exists != 0)
}

pub(crate) struct ClaudeSessionMeta {
    session_id: String,
    remote_session_id: Option<String>,
    cwd: Option<String>,
    git_branch: Option<String>,
    first_ts: i64,
    last_ts: i64,
    last_assistant_text: Option<String>,
    /// Every identified record is a sidechain row, so this file is a delegated
    /// sidecar rather than a session of its own — the same rule discovery uses
    /// to keep sidecars out of the catalog.
    subagent: bool,
    /// The child identity the provider recorded, read from the records and
    /// never from the file name. Absent on versions that do not emit it.
    agent_id: Option<String>,
}

fn scan_claude_session_file(path: &Path) -> Result<Option<ClaudeSessionMeta>> {
    let text = fs::read_to_string(path).unwrap_or_default();
    let mut session_id = None;
    let mut remote_session_id = None;
    let mut cwd = None;
    let mut git_branch = None;
    let mut first_ts = None;
    let mut last_ts = None;
    let mut last_assistant_text = None;
    let mut identified_records = 0usize;
    let mut sidechain_records = 0usize;
    let mut agent_id: Option<String> = None;
    for line in text.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if value
            .get("sessionId")
            .and_then(Value::as_str)
            .is_some_and(|id| !id.is_empty())
        {
            identified_records += 1;
            if value.get("isSidechain").and_then(Value::as_bool) == Some(true) {
                sidechain_records += 1;
            }
        }
        if agent_id.is_none() {
            agent_id = value
                .get("agentId")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
                .map(str::to_string);
        }
        if session_id.is_none() {
            session_id = value
                .get("sessionId")
                .and_then(Value::as_str)
                .map(str::to_string);
        }
        if remote_session_id.is_none() {
            remote_session_id = [
                value.get("remoteSessionId"),
                value.get("remote_session_id"),
                value.pointer("/teleportedSessionInfo/sessionId"),
            ]
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::trim)
            .find(|id| !id.is_empty())
            .map(str::to_string);
        }
        if cwd.is_none() {
            cwd = value.get("cwd").and_then(Value::as_str).map(str::to_string);
        }
        if let Some(branch) = value.get("gitBranch").and_then(Value::as_str) {
            git_branch = Some(branch.to_string());
        }
        if let Some(ts) = value
            .get("timestamp")
            .and_then(|v| v.as_str().and_then(parse_iso_ms).or_else(|| v.as_i64()))
        {
            first_ts.get_or_insert(ts);
            last_ts = Some(ts);
        }
        if value.get("type").and_then(Value::as_str) == Some("assistant")
            && value.get("isSidechain").and_then(Value::as_bool) != Some(true)
        {
            if let Some(content) = value.pointer("/message/content") {
                if let Some(text) = content.as_str() {
                    last_assistant_text = Some(text.chars().take(4096).collect());
                } else if let Some(items) = content.as_array() {
                    let parts = items
                        .iter()
                        .filter(|item| item.get("type").and_then(Value::as_str) == Some("text"))
                        .filter_map(|item| item.get("text").and_then(Value::as_str))
                        .collect::<Vec<_>>();
                    if !parts.is_empty() {
                        last_assistant_text = Some(parts.join("\n").chars().take(4096).collect());
                    }
                }
            }
        }
    }
    let Some(session_id) = session_id else {
        return Ok(None);
    };
    let first = first_ts.unwrap_or(0);
    Ok(Some(ClaudeSessionMeta {
        session_id,
        remote_session_id,
        cwd,
        git_branch,
        first_ts: first,
        last_ts: last_ts.unwrap_or(first),
        last_assistant_text,
        subagent: identified_records > 0 && sidechain_records == identified_records,
        agent_id,
    }))
}

pub(crate) fn reconcile_claude_remote_relationships(conn: &Connection) -> Result<()> {
    let correlations = conn
        .prepare(
            "SELECT c.remote_session_id, c.local_session_id \
             FROM session_identity_correlations c \
             JOIN session_presences p \
               ON p.source = c.source AND p.session_id = c.remote_session_id \
              AND p.location = 'remote' \
             WHERE c.source = 'claude' AND c.relationship = 'materialized_local'",
        )?
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (remote_id, local_id) in correlations {
        record_claude_materialized_relationship(conn, &remote_id, &local_id)?;
    }
    Ok(())
}

fn record_claude_remote_relationship(conn: &Connection, meta: &ClaudeSessionMeta) -> Result<()> {
    let Some(remote_id) = meta.remote_session_id.as_deref() else {
        return Ok(());
    };
    if remote_id == meta.session_id {
        return Ok(());
    }
    conn.execute(
        "INSERT INTO session_identity_correlations \
         (source, local_session_id, remote_session_id, relationship, evidence_kind, updated_ms) \
         VALUES ('claude', ?, ?, 'materialized_local', 'claude_remote_session_id', ?) \
         ON CONFLICT(source, local_session_id, relationship) DO UPDATE SET \
           remote_session_id = excluded.remote_session_id, \
           evidence_kind = excluded.evidence_kind, updated_ms = excluded.updated_ms",
        params![meta.session_id, remote_id, (now_ns() / 1_000_000) as i64],
    )?;
    let remote_exists: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM session_presences \
         WHERE source = 'claude' AND session_id = ? AND location = 'remote')",
        [remote_id],
        |row| row.get(0),
    )?;
    if remote_exists {
        record_claude_materialized_relationship(conn, remote_id, &meta.session_id)?;
    }
    Ok(())
}

fn record_claude_materialized_relationship(
    conn: &Connection,
    remote_id: &str,
    local_id: &str,
) -> Result<()> {
    record_relationship(
        conn,
        &ObservedRelationship {
            source: "claude",
            parent_session_id: remote_id,
            child_session_id: Some(local_id),
            relationship: "materialized_local",
            child_agent_type: None,
            child_agent_name: None,
            child_model: None,
            spawn_depth: None,
            evidence_kind: "claude_remote_session_id",
            evidence_locator: None,
            evidence_ref: Some(remote_id),
            child_has_events: session_events_exist(conn, "claude", local_id)?,
            spawned_at_ms: None,
        },
    )
}

fn ingest_claude_transcript(conn: &Connection, path: &Path) -> Result<()> {
    ingest_claude_transcript_as(conn, path, None)
}

/// Remove everything a single transcript record produced under one session id.
///
/// `message_uuid` is the same identity insertion derives event uids from and
/// stamps on the rows it derives from a record's tool use, so this reaches the
/// record's events, its tool calls and its file edits together — including
/// records with no `uuid` of their own, which fall back to the message id or
/// the file position. Leaving the derived rows behind would keep a parent
/// exposing a delegated thread's actions as its own long after the events
/// moved to the child. The event prefix is compared with `substr` rather than
/// `LIKE` because a provider id may contain `_` or `%`.
fn delete_claude_record_rows(
    conn: &Connection,
    session_id: &str,
    message_uuid: &str,
) -> Result<()> {
    conn.execute(
        "DELETE FROM session_events WHERE source = 'claude' AND session_id = ? \
         AND substr(event_uid, 1, length(?) + 1) = ? || ':'",
        params![session_id, message_uuid, message_uuid],
    )?;
    conn.execute(
        "DELETE FROM tool_calls WHERE source = 'claude' AND session_id = ? AND message_id = ?",
        params![session_id, message_uuid],
    )?;
    conn.execute(
        "DELETE FROM file_edits WHERE source = 'claude' AND session_id = ? AND message_id = ?",
        params![session_id, message_uuid],
    )?;
    Ok(())
}

/// Message ids only a pre-upgrade parse could have stored: current parses
/// never emit positional `{stem}:{line}` identities. Scoped to one file stem
/// and session, filtered in Rust so a stem containing SQL wildcards cannot
/// widen the match.
fn legacy_positional_message_ids(
    conn: &Connection,
    session_id: &str,
    stem: &str,
) -> Result<Vec<String>> {
    let prefix = format!("{stem}:");
    let mut ids = Vec::new();
    for table in ["session_events", "tool_calls", "file_edits"] {
        let mut statement = conn.prepare(&format!(
            "SELECT DISTINCT message_id FROM {table} WHERE source = 'claude' AND session_id = ?"
        ))?;
        let mut rows = statement.query([session_id])?;
        while let Some(row) = rows.next()? {
            let id: Option<String> = row.get(0)?;
            if id.as_deref().is_some_and(|id| {
                id.starts_with(&prefix) && id[prefix.len()..].chars().all(|c| c.is_ascii_digit())
            }) {
                ids.push(id.unwrap());
            }
        }
    }
    ids.sort();
    ids.dedup();
    Ok(ids)
}

/// Stored facts of one transcript record that a legacy positional row can be
/// matched against: every event the record produces, with the role, kind,
/// model and token spend ingestion stores beside the text, plus every tool
/// use id it carries. Each mirrors the ingestion mapping below so the
/// comparison is exact for an unchanged record, including the null texts
/// ingestion still writes for empty tool results. Verbatim envelope facts
/// (`request_id` and friends) are deliberately excluded: pre-upgrade rows
/// predate those columns, so requiring them would block every legacy heal.
struct ClaudeRecordFacts {
    events: Vec<ClaudeFactEvent>,
    tool_use_ids: Vec<String>,
    model: Option<String>,
    token_json: Option<String>,
}

struct ClaudeFactEvent {
    text: Option<String>,
    role: String,
    kind: String,
}

fn claude_record_facts(
    message: Option<&Map<String, Value>>,
    message_role: &str,
    model: Option<&str>,
    token_json: Option<&str>,
) -> ClaudeRecordFacts {
    let mut facts = ClaudeRecordFacts {
        events: Vec::new(),
        tool_use_ids: Vec::new(),
        model: model.map(str::to_string),
        token_json: token_json.map(str::to_string),
    };
    let human_role = if message_role == "assistant" {
        "assistant"
    } else {
        "user"
    };
    let mut push = |text: Option<String>, role: &str, kind: &str| {
        facts.events.push(ClaudeFactEvent {
            text,
            role: role.to_string(),
            kind: kind.to_string(),
        });
    };
    let Some(content) = message.and_then(|m| m.get("content")) else {
        return facts;
    };
    if let Some(text) = content.as_str() {
        if !text.trim().is_empty() {
            push(Some(text.to_string()), human_role, "text");
        }
        return facts;
    }
    let Some(blocks) = content.as_array() else {
        return facts;
    };
    for block in blocks {
        let block_type = block.get("type").and_then(Value::as_str).unwrap_or("");
        match block_type {
            "text" => {
                let text = block.get("text").and_then(Value::as_str).unwrap_or("");
                if !text.trim().is_empty() {
                    push(Some(text.to_string()), human_role, "text");
                }
            }
            "thinking" => {
                let text = block
                    .get("thinking")
                    .or_else(|| block.get("text"))
                    .and_then(Value::as_str);
                if text.is_some_and(|s| !s.trim().is_empty()) {
                    push(Some(text.unwrap().to_string()), "assistant", "thinking");
                }
            }
            "tool_use" => {
                let name = block.get("name").and_then(Value::as_str).unwrap_or("");
                let args = block.get("input").unwrap_or(&Value::Null);
                push(
                    Some(format_tool_event_text(
                        name,
                        pick_tool_target(name, args).as_deref(),
                        args,
                    )),
                    "assistant",
                    "tool_use",
                );
                if let Some(id) = block.get("id").and_then(Value::as_str) {
                    if !name.is_empty() {
                        facts.tool_use_ids.push(id.to_string());
                    }
                }
            }
            // An empty tool result still writes a null-text event, so the
            // null is part of the record's identity for matching.
            "tool_result" => {
                push(
                    materialize_tool_result_text(block.get("content").unwrap_or(&Value::Null)),
                    "tool_result",
                    "tool_result",
                );
            }
            _ => {}
        }
    }
    facts
}

/// Heal one id-less record's pre-upgrade positional leftovers: candidates
/// first (one indexed pass per table, the common empty case stays cheap),
/// then the unique-or-preserved full-record match.
fn heal_legacy_positional_record(
    conn: &Connection,
    session_id: &str,
    stem: &str,
    ts_ms: i64,
    message: Option<&Map<String, Value>>,
    message_role: &str,
    model: Option<&str>,
    token_json: Option<&str>,
) -> Result<()> {
    let legacy = legacy_positional_message_ids(conn, session_id, stem)?;
    if legacy.is_empty() {
        return Ok(());
    }
    heal_legacy_positional_rows(
        conn,
        session_id,
        &legacy,
        ts_ms,
        &claude_record_facts(message, message_role, model, token_json),
    )
}

/// A legacy event's persisted identity for matching: every field ingestion
/// stores beside the bytes. Verbatim envelope facts are excluded on purpose
/// (see [`claude_record_facts`]): pre-upgrade rows predate those columns.
type LegacyEventKey = (
    Option<String>,
    Option<i64>,
    String,
    String,
    Option<String>,
    Option<String>,
);

/// Remove pre-upgrade positional leftovers that match one id-less record
/// unambiguously: a legacy message heals only on an exact full-record match
/// of its events — text, timestamp, role, kind, model and token spend — and
/// only when it is the single legacy message carrying that record. Tool
/// calls and file edits match per row by tool use id, which names its record
/// (unique per session by schema) even when the event texts do not.
/// Anything ambiguous stays preserved under the retention contract.
fn heal_legacy_positional_rows(
    conn: &Connection,
    session_id: &str,
    legacy_ids: &[String],
    ts_ms: i64,
    facts: &ClaudeRecordFacts,
) -> Result<()> {
    if legacy_ids.is_empty() || (facts.events.is_empty() && facts.tool_use_ids.is_empty()) {
        return Ok(());
    }
    let placeholders = legacy_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let sql = format!(
        "SELECT message_id, text, ts_ms, role, kind, model, token_json FROM session_events \
         WHERE source = 'claude' AND session_id = ? AND message_id IN ({placeholders})"
    );
    let mut statement = conn.prepare(&sql)?;
    let mut rows = statement.query(rusqlite::params_from_iter(
        [session_id.to_string()]
            .into_iter()
            .chain(legacy_ids.iter().cloned()),
    ))?;
    // Group legacy events by message: a legacy message heals only on an
    // exact full-record match. Matching one shared block would erase the
    // message's changed siblings, which the content-hash model treats as a
    // distinct retained predecessor.
    type MessageContentKey = Vec<LegacyEventKey>;
    let mut by_message: HashMap<String, MessageContentKey> = HashMap::new();
    while let Some(row) = rows.next()? {
        let message_id: String = row.get(0)?;
        by_message.entry(message_id).or_default().push((
            row.get(1)?,
            row.get(2)?,
            row.get(3)?,
            row.get(4)?,
            row.get(5)?,
            row.get(6)?,
        ));
    }
    drop(rows);
    drop(statement);
    let mut record_key: MessageContentKey = facts
        .events
        .iter()
        .map(|event| {
            (
                event.text.clone(),
                Some(ts_ms),
                event.role.clone(),
                event.kind.clone(),
                facts.model.clone(),
                facts.token_json.clone(),
            )
        })
        .collect();
    record_key.sort();
    let mut heal_messages: HashSet<String> = HashSet::new();
    if !record_key.is_empty() {
        let mut full_matches = Vec::new();
        for (message_id, mut key) in by_message {
            key.sort();
            if key == record_key {
                full_matches.push(message_id);
            }
        }
        // Unique or preserved: two legacy messages carrying the same record
        // stay put rather than risk healing the wrong one.
        if full_matches.len() == 1 {
            heal_messages.insert(full_matches.pop().unwrap());
        }
    }
    for message_id in &heal_messages {
        conn.execute(
            "DELETE FROM session_events WHERE source = 'claude' AND session_id = ? AND message_id = ?",
            params![session_id, message_id],
        )?;
    }
    // A tool use id names its own call and edit rows even when the record's
    // event texts match nothing uniquely — but only those rows. Sibling
    // events the rewritten record dropped or changed belong to a retained
    // predecessor, so they stay unless the full-record match above heals
    // their message.
    if !facts.tool_use_ids.is_empty() {
        let id_placeholders = facts
            .tool_use_ids
            .iter()
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(",");
        let msg_placeholders = legacy_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        for table in ["tool_calls", "file_edits"] {
            let sql = format!(
                "DELETE FROM {table} WHERE source = 'claude' AND session_id = ? \
                 AND message_id IN ({msg_placeholders}) AND tool_use_id IN ({id_placeholders})"
            );
            conn.execute(
                &sql,
                rusqlite::params_from_iter(
                    [session_id.to_string()]
                        .into_iter()
                        .chain(legacy_ids.iter().cloned())
                        .chain(facts.tool_use_ids.iter().cloned()),
                ),
            )?;
        }
    }
    Ok(())
}

/// `attributed_session_id` overrides the record's own `sessionId`. Claude
/// subagent transcripts carry the PARENT's sessionId plus a per-child
/// `agentId`; when the provider records that agentId we store the child's
/// events under it so the child is independently addressable.
fn ingest_claude_transcript_as(
    conn: &Connection,
    path: &Path,
    attributed_session_id: Option<&str>,
) -> Result<()> {
    let text = fs::read_to_string(path).unwrap_or_default();
    for line in text.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let Some(obj) = value.as_object() else {
            continue;
        };
        let record_session_id = match obj.get("sessionId").and_then(Value::as_str) {
            Some(s) if !s.is_empty() => s,
            _ => continue,
        };
        let session_id = attributed_session_id.unwrap_or(record_session_id);
        // Subagent sidecar transcripts share the parent's sessionId with
        // isSidechain rows. The subagent's assistant output is real session
        // activity (text and token spend), but its user-role rows are the
        // parent agent's own prompts and tool results — ingesting those
        // manufactures fake human turns.
        let is_sidechain = obj.get("isSidechain").and_then(Value::as_bool);
        let sidechain = is_sidechain.unwrap_or(false);
        let skipped_sidechain = sidechain
            && obj
                .get("message")
                .and_then(|m| m.get("role"))
                .and_then(Value::as_str)
                != Some("assistant");
        let uuid = obj.get("uuid").and_then(Value::as_str);
        let message = obj.get("message").and_then(Value::as_object);
        // Records without provider identity fall back to a content hash, never
        // the line index: compaction drops prefixes and inserts summary rows,
        // so indexes shift and surviving rows would land on earlier rows'
        // event uids, overwriting retained evidence through the conflict
        // upsert instead of retaining it. The hash keeps a surviving row on
        // its identity across a rewrite; a row whose bytes changed is a new
        // identity whose predecessor stays retained. Byte-identical id-less
        // rows share one identity. The `sha256:` namespace keeps hash
        // identities disjoint from the legacy positional `{stem}:{digits}`
        // namespace, so the legacy detector below can never select a row
        // the new parser wrote (an all-decimal hash would otherwise match).
        let digest = Sha256::digest(line.as_bytes());
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("session");
        let fallback_uid = format!(
            "{stem}:sha256:{}",
            digest
                .iter()
                .take(8)
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        );
        // A pre-upgrade parse stored id-less records under a positional
        // `{stem}:{line}` identity that no current parse emits. Heals remove
        // exactly the current identity and never guess the historical one:
        // after a prefix-dropping rewrite the live index no longer belongs
        // to the same record, so a positional delete could drop another
        // retained record while leaving the real predecessor behind. Stale
        // positional leftovers are preserved by design — retention-safe
        // duplication, bounded to id-less files — rather than healed by
        // position.
        let message_uuid = uuid
            .or_else(|| message.and_then(|m| m.get("id")).and_then(Value::as_str))
            .unwrap_or(&fallback_uid);
        let id_less = uuid.is_none()
            && message
                .and_then(|m| m.get("id"))
                .and_then(Value::as_str)
                .is_none();
        // ts_ms, role, model and token spend feed the legacy heal below;
        // they only read the record.
        let ts_ms = obj
            .get("timestamp")
            .and_then(|v| v.as_str().and_then(parse_iso_ms).or_else(|| v.as_i64()))
            .unwrap_or(0);
        let message_role = message
            .and_then(|m| m.get("role"))
            .and_then(Value::as_str)
            .or_else(|| obj.get("type").and_then(Value::as_str))
            .unwrap_or("");
        let model = message.and_then(|m| m.get("model")).and_then(Value::as_str);
        let token_json = message
            .and_then(|m| m.get("usage"))
            .and_then(|v| serde_json::to_string(v).ok());
        // Heal what an earlier parser version wrote for this record: it
        // attributed every sidechain row to the parent, and stored the rows
        // this guard now skips. Re-reading the file removes the stale rows
        // under the identity they were written with, so a re-parse moves them
        // onto the child instead of duplicating them across both.
        // Pre-upgrade positional leftovers match by full stored record,
        // unique or preserved: the live index cannot identify them after a
        // rewrite.
        if session_id != record_session_id {
            delete_claude_record_rows(conn, record_session_id, message_uuid)?;
            if id_less {
                heal_legacy_positional_record(
                    conn,
                    record_session_id,
                    stem,
                    ts_ms,
                    message,
                    message_role,
                    model,
                    token_json.as_deref(),
                )?;
            }
        }
        if skipped_sidechain {
            delete_claude_record_rows(conn, session_id, message_uuid)?;
            if id_less {
                heal_legacy_positional_record(
                    conn,
                    session_id,
                    stem,
                    ts_ms,
                    message,
                    message_role,
                    model,
                    token_json.as_deref(),
                )?;
            }
            continue;
        }
        let cwd = obj.get("cwd").and_then(Value::as_str);
        let project = cwd;
        let git_branch = obj.get("gitBranch").and_then(Value::as_str);
        let parent_id = obj.get("parentUuid").and_then(Value::as_str);
        let is_meta = obj.get("isMeta").and_then(Value::as_bool);
        // Verbatim envelope facts. `stop_reason` is deliberately not mapped to
        // an enum: a JSON null means the turn is still in flight, and that is
        // exactly the distinction a normalizing parser would erase.
        let facts = RawMessageFacts {
            request_id: obj
                .get("requestId")
                .or_else(|| obj.get("request_id"))
                .and_then(Value::as_str),
            stop_reason: message
                .and_then(|m| m.get("stop_reason"))
                .and_then(Value::as_str),
            agent_version: obj
                .get("version")
                .or_else(|| obj.get("sourceVersion"))
                .and_then(Value::as_str),
            is_sidechain,
            is_meta,
            turn_id: None,
        };
        let Some(content) = message.and_then(|m| m.get("content")) else {
            continue;
        };
        if !sidechain && message_role == "user" && is_meta != Some(true) {
            let prompt = if let Some(text) = content.as_str() {
                text.trim().to_string()
            } else {
                content
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|block| block.get("type").and_then(Value::as_str) == Some("text"))
                    .filter_map(|block| block.get("text").and_then(Value::as_str))
                    .map(str::trim)
                    .filter(|text| !text.is_empty())
                    .collect::<Vec<_>>()
                    .join("\n")
            };
            if !prompt.is_empty() && !discover::is_claude_control_prompt(&prompt) {
                insert_history(
                    conn,
                    &HistoryEntry {
                        id: 0,
                        source: "claude".into(),
                        session_id: Some(session_id.to_string()),
                        project: project.map(str::to_string),
                        prompt_hash: Some(prompt_hash(&prompt)),
                        prompt,
                        timestamp_ms: ts_ms,
                    },
                )?;
            }
        }
        if let Some(s) = content.as_str() {
            if !s.trim().is_empty() {
                let role = if message_role == "assistant" {
                    "assistant"
                } else {
                    "user"
                };
                insert_session_event(
                    conn,
                    "claude",
                    session_id,
                    project,
                    cwd,
                    git_branch,
                    message_uuid,
                    parent_id,
                    ts_ms,
                    role,
                    "text",
                    Some(s),
                    model,
                    token_json.as_deref(),
                    &format!("{message_uuid}:0"),
                    facts,
                )?;
            }
            continue;
        }
        let Some(blocks) = content.as_array() else {
            continue;
        };
        for (block_index, block) in blocks.iter().enumerate() {
            let block_type = block.get("type").and_then(Value::as_str).unwrap_or("");
            let event_uid = format!("{message_uuid}:{block_index}");
            match block_type {
                "text" => {
                    if let Some(text) = block.get("text").and_then(Value::as_str) {
                        if !text.trim().is_empty() {
                            let role = if message_role == "assistant" {
                                "assistant"
                            } else {
                                "user"
                            };
                            insert_session_event(
                                conn,
                                "claude",
                                session_id,
                                project,
                                cwd,
                                git_branch,
                                message_uuid,
                                parent_id,
                                ts_ms,
                                role,
                                "text",
                                Some(text),
                                model,
                                token_json.as_deref(),
                                &event_uid,
                                facts,
                            )?;
                        }
                    }
                }
                "thinking" => {
                    let text = block
                        .get("thinking")
                        .or_else(|| block.get("text"))
                        .and_then(Value::as_str);
                    if text.is_some_and(|s| !s.trim().is_empty()) {
                        insert_session_event(
                            conn,
                            "claude",
                            session_id,
                            project,
                            cwd,
                            git_branch,
                            message_uuid,
                            parent_id,
                            ts_ms,
                            "assistant",
                            "thinking",
                            text,
                            model,
                            token_json.as_deref(),
                            &event_uid,
                            facts,
                        )?;
                    }
                }
                "tool_use" => {
                    let tool_use_id = block.get("id").and_then(Value::as_str).unwrap_or("");
                    let name = block.get("name").and_then(Value::as_str).unwrap_or("");
                    let args = block.get("input").unwrap_or(&Value::Null);
                    let target = pick_tool_target(name, args);
                    let event_text = format_tool_event_text(name, target.as_deref(), args);
                    insert_session_event(
                        conn,
                        "claude",
                        session_id,
                        project,
                        cwd,
                        git_branch,
                        message_uuid,
                        parent_id,
                        ts_ms,
                        "assistant",
                        "tool_use",
                        Some(&event_text),
                        model,
                        token_json.as_deref(),
                        &event_uid,
                        facts,
                    )?;
                    if !tool_use_id.is_empty() && !name.is_empty() {
                        let args_json =
                            serde_json::to_string(args).unwrap_or_else(|_| "null".to_string());
                        insert_tool_call(
                            conn,
                            "claude",
                            session_id,
                            message_uuid,
                            tool_use_id,
                            name,
                            target.as_deref(),
                            &args_json,
                            None,
                            ts_ms,
                        )?;
                        if is_file_edit_tool(name) {
                            if let Some(file_path) = target.as_deref() {
                                upsert_file_edit_from_call(
                                    conn,
                                    "claude",
                                    session_id,
                                    message_uuid,
                                    tool_use_id,
                                    file_path,
                                    name,
                                    ts_ms,
                                    git_branch,
                                    cwd,
                                )?;
                            }
                        }
                    }
                }
                "tool_result" => {
                    let tool_use_id = block
                        .get("tool_use_id")
                        .or_else(|| block.get("toolUseId"))
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let content = block.get("content").unwrap_or(&Value::Null);
                    let text = materialize_tool_result_text(content);
                    insert_session_event(
                        conn,
                        "claude",
                        session_id,
                        project,
                        cwd,
                        git_branch,
                        message_uuid,
                        parent_id,
                        ts_ms,
                        "tool_result",
                        "tool_result",
                        text.as_deref(),
                        model,
                        token_json.as_deref(),
                        &event_uid,
                        facts,
                    )?;
                    let is_error = block.get("is_error").and_then(Value::as_bool);
                    if !tool_use_id.is_empty() {
                        if let Some(err) = is_error {
                            set_tool_call_error(conn, "claude", session_id, tool_use_id, err)?;
                        }
                        if let Some(result) = find_tool_use_result(block) {
                            update_file_edit_from_tool_result(
                                conn,
                                "claude",
                                session_id,
                                message_uuid,
                                tool_use_id,
                                result,
                                ts_ms,
                                git_branch,
                                cwd,
                            )?;
                        }
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}

/// The verbatim stop reason an OpenCode `step-finish` part carries, if this is
/// one.
///
/// OpenCode does not put a stop reason on the message: it writes a trailing
/// `step-finish` part whose `reason` is the wire string (`tool-calls`,
/// `stop`, `length`, …). Stored as written, exactly like Claude's
/// `message.stop_reason`; consumers map it to their own enum.
///
/// The OpenCode adapter currently ingests prompt history only, so nothing calls
/// this yet — the event-level parity work (#168) is what wires it into the
/// assistant rows. It is landed with the column so that work is a call site
/// rather than a schema change.
#[allow(dead_code)]
fn opencode_step_finish_stop_reason(part: &Value) -> Option<&str> {
    (part.get("type").and_then(Value::as_str) == Some("step-finish"))
        .then(|| part.get("reason").and_then(Value::as_str))
        .flatten()
}

/// The per-message facts a provider records on the envelope rather than in the
/// message body, carried verbatim from the parser to the row.
///
/// These are the facts consumers need and normalization would otherwise read
/// past: which API request a turn belongs to (`request_id`), why the model
/// stopped — and, by its absence, that it has not yet (`stop_reason`), which
/// harness build produced it (`agent_version`), whether the row is delegated or
/// injected rather than human (`is_sidechain` / `is_meta`), and which Codex
/// turn it falls inside (`turn_id`). Every field is optional and stored as the
/// provider wrote it: relayhistory preserves, consumers map.
/// The generation of raw-fact parsing the local parser stamps on every event
/// it writes.
///
/// Unlike the facts themselves this is not a provider value and is never null
/// on a row the current parser wrote: `request_id`, `stop_reason` and
/// `turn_id` are legitimately absent on real records, and `is_sidechain` /
/// `is_meta` are absent on any record whose envelope omits the flag, so none
/// of them can answer "was this row indexed before the facts existed?".
/// This can, which is what the full-sync backfill probes read. Bump it when a
/// later change adds facts that existing rows should be re-read for.
const RAW_MESSAGE_FACTS_VERSION: i64 = 1;

#[derive(Debug, Default, Clone, Copy)]
struct RawMessageFacts<'a> {
    request_id: Option<&'a str>,
    stop_reason: Option<&'a str>,
    agent_version: Option<&'a str>,
    is_sidechain: Option<bool>,
    is_meta: Option<bool>,
    turn_id: Option<&'a str>,
}

#[allow(clippy::too_many_arguments)]
fn insert_session_event(
    conn: &Connection,
    source: &str,
    session_id: &str,
    project: Option<&str>,
    cwd: Option<&str>,
    git_branch: Option<&str>,
    message_id: &str,
    parent_id: Option<&str>,
    ts_ms: i64,
    role: &str,
    kind: &str,
    text: Option<&str>,
    model: Option<&str>,
    token_json: Option<&str>,
    event_uid: &str,
    facts: RawMessageFacts<'_>,
) -> Result<()> {
    crate::mark_session_presence(conn, source, session_id, SessionLocation::Local)?;
    // Stamp `project_key` as the row is inserted rather than sweeping for it
    // afterwards. An UPDATE over `session_events` is a change every
    // durable-delivery subscriber has to be told about, so a sweep would
    // journal a second upsert for every event of every session on every sync.
    //
    // The owning session's key is preferred when the row already exists,
    // because it may be stronger than anything derivable here (a Codex
    // `repository_url`, or a key inherited from a delegating parent). But it
    // is not depended on: the Codex walk writes a rollout's events *before*
    // upserting its session, and a delegated thread never gets a catalog row
    // at all, so a lookup alone would leave those events null and hand them
    // straight back to the sweep this design exists to avoid. Resolving the
    // cwd is the fallback, and it agrees with what `upsert_session` will store
    // for the same directory, so the ordinary case converges with no rewrite.
    //
    // On conflict the order is the same but the stored value comes second, so
    // a re-ingest can only ever improve the key. The cwd fallback is last
    // precisely because it is the weakest: for a delegated child there is no
    // `sessions` row to consult, and its stored key is the parent's, inherited
    // by the denormalizing pass. Preferring the incoming value there -- which
    // is what a plain `COALESCE(excluded.project_key, ...)` does now that the
    // fallback makes it non-null -- would overwrite a canonical repository key
    // with a machine-local path on every re-ingest, and hand every one of
    // those events back to the sweep to fix, journalling a second delivery
    // upsert each time.
    //
    // The *method* is stamped beside the key, always from the same arm that
    // produced it. It is what tells a later pass whether this row worked its
    // key out from its own directory or was lent one, and a row that carries a
    // key with no method is indistinguishable from one that was never
    // resolved: the denormalizing pass would then replace a delegated
    // thread's own repository with its delegator's, on every sync, forever.
    let resolved = cwd.and_then(|cwd| crate::project_identity::identity_for(Some(cwd), None));
    let resolved_key = resolved.as_ref().map(|(key, _)| key.as_str());
    let resolved_method = resolved.as_ref().map(|(_, method)| method.as_str());
    conn.execute(
        "INSERT INTO session_events \
         (source, session_id, project, project_key, project_key_method, cwd, git_branch, message_id, parent_id, ts_ms, role, kind, text, model, token_json, event_uid, \
          request_id, stop_reason, agent_version, is_sidechain, is_meta, turn_id, raw_facts_version) \
         VALUES (?1, ?2, ?3, \
           COALESCE((SELECT s.project_key FROM sessions s WHERE s.source = ?1 AND s.session_id = ?2), ?15), \
           CASE WHEN (SELECT s.project_key FROM sessions s WHERE s.source = ?1 AND s.session_id = ?2) IS NOT NULL \
                THEN (SELECT s.project_key_method FROM sessions s WHERE s.source = ?1 AND s.session_id = ?2) \
                ELSE ?16 END, \
           ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, \
           ?17, ?18, ?19, ?20, ?21, ?22, ?23) \
         ON CONFLICT(source, session_id, event_uid) DO UPDATE SET \
         project=excluded.project, \
         project_key=COALESCE((SELECT s.project_key FROM sessions s WHERE s.source = ?1 AND s.session_id = ?2), session_events.project_key, ?15), \
         project_key_method=CASE \
           WHEN (SELECT s.project_key FROM sessions s WHERE s.source = ?1 AND s.session_id = ?2) IS NOT NULL \
             THEN (SELECT s.project_key_method FROM sessions s WHERE s.source = ?1 AND s.session_id = ?2) \
           WHEN session_events.project_key IS NOT NULL THEN session_events.project_key_method \
           ELSE ?16 END, \
         cwd=excluded.cwd, git_branch=excluded.git_branch, message_id=excluded.message_id, \
         parent_id=excluded.parent_id, ts_ms=excluded.ts_ms, role=excluded.role, kind=excluded.kind, text=excluded.text, \
         model=excluded.model, token_json=excluded.token_json, request_id=excluded.request_id, \
         stop_reason=excluded.stop_reason, agent_version=excluded.agent_version, \
         is_sidechain=excluded.is_sidechain, is_meta=excluded.is_meta, turn_id=excluded.turn_id, \
         raw_facts_version=excluded.raw_facts_version",
        params![
            source,
            session_id,
            project,
            cwd,
            git_branch,
            message_id,
            parent_id,
            ts_ms,
            role,
            kind,
            text,
            model,
            token_json,
            event_uid,
            resolved_key,
            resolved_method,
            facts.request_id,
            facts.stop_reason,
            facts.agent_version,
            facts.is_sidechain,
            facts.is_meta,
            facts.turn_id,
            RAW_MESSAGE_FACTS_VERSION,
        ],
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn insert_tool_call(
    conn: &Connection,
    source: &str,
    session_id: &str,
    message_id: &str,
    tool_use_id: &str,
    name: &str,
    target: Option<&str>,
    args_json: &str,
    is_error: Option<bool>,
    ts_ms: i64,
) -> Result<()> {
    crate::mark_session_presence(conn, source, session_id, SessionLocation::Local)?;
    conn.execute(
        "INSERT INTO tool_calls \
         (source, session_id, message_id, tool_use_id, name, target, args_json, is_error, ts_ms) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(source, session_id, tool_use_id) DO UPDATE SET \
         message_id=excluded.message_id, name=excluded.name, target=excluded.target, args_json=excluded.args_json, \
         is_error=COALESCE(excluded.is_error, tool_calls.is_error), ts_ms=excluded.ts_ms",
        params![
            source,
            session_id,
            message_id,
            tool_use_id,
            name,
            target,
            args_json,
            is_error.map(|v| if v { 1 } else { 0 }),
            ts_ms,
        ],
    )?;
    Ok(())
}

fn set_tool_call_error(
    conn: &Connection,
    source: &str,
    session_id: &str,
    tool_use_id: &str,
    is_error: bool,
) -> Result<()> {
    conn.execute(
        "UPDATE tool_calls SET is_error = ? WHERE source = ? AND session_id = ? AND tool_use_id = ?",
        params![if is_error { 1 } else { 0 }, source, session_id, tool_use_id],
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn upsert_file_edit_from_call(
    conn: &Connection,
    source: &str,
    session_id: &str,
    message_id: &str,
    tool_use_id: &str,
    file_path: &str,
    tool_name: &str,
    ts_ms: i64,
    git_branch: Option<&str>,
    cwd: Option<&str>,
) -> Result<()> {
    crate::mark_session_presence(conn, source, session_id, SessionLocation::Local)?;
    conn.execute(
        "INSERT INTO file_edits \
         (source, session_id, message_id, tool_use_id, file_path, tool_name, ts_ms, git_branch, cwd) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(source, session_id, tool_use_id) DO UPDATE SET \
         message_id=excluded.message_id, file_path=excluded.file_path, tool_name=excluded.tool_name, \
         ts_ms=excluded.ts_ms, git_branch=COALESCE(excluded.git_branch, file_edits.git_branch), cwd=COALESCE(excluded.cwd, file_edits.cwd)",
        params![
            source,
            session_id,
            message_id,
            tool_use_id,
            file_path,
            tool_name,
            ts_ms,
            git_branch,
            cwd,
        ],
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn update_file_edit_from_tool_result(
    conn: &Connection,
    source: &str,
    session_id: &str,
    message_id: &str,
    tool_use_id: &str,
    result: &Value,
    ts_ms: i64,
    git_branch: Option<&str>,
    cwd: Option<&str>,
) -> Result<()> {
    let structured_patch = result
        .get("structuredPatch")
        .or_else(|| result.get("structured_patch"));
    let patch_json = structured_patch.and_then(|v| serde_json::to_string(v).ok());
    let (lines_added, lines_removed) = structured_patch.map(count_patch_lines).unwrap_or((0, 0));
    let user_modified = result
        .get("userModified")
        .or_else(|| result.get("user_modified"))
        .and_then(Value::as_bool);
    let file_path = result
        .get("filePath")
        .or_else(|| result.get("file_path"))
        .or_else(|| result.get("path"))
        .and_then(Value::as_str);
    conn.execute(
        "UPDATE file_edits SET \
         message_id = COALESCE(message_id, ?), \
         file_path = COALESCE(?, file_path), \
         lines_added = ?, lines_removed = ?, structured_patch_json = COALESCE(?, structured_patch_json), \
         user_modified = COALESCE(?, user_modified), ts_ms = COALESCE(ts_ms, ?), \
         git_branch = COALESCE(?, git_branch), cwd = COALESCE(?, cwd) \
         WHERE source = ? AND session_id = ? AND tool_use_id = ?",
        params![
            message_id,
            file_path,
            lines_added,
            lines_removed,
            patch_json,
            user_modified.map(|v| if v { 1 } else { 0 }),
            ts_ms,
            git_branch,
            cwd,
            source,
            session_id,
            tool_use_id,
        ],
    )?;
    Ok(())
}

fn pick_tool_target(name: &str, input: &Value) -> Option<String> {
    let obj = input.as_object()?;
    let get = |k: &str| obj.get(k).and_then(Value::as_str).map(str::to_string);
    match name {
        "Read" | "Edit" | "Write" | "NotebookEdit" => get("file_path")
            .or_else(|| get("path"))
            .or_else(|| get("notebook_path")),
        "Bash" => get("command"),
        "Grep" | "Glob" => get("pattern"),
        _ => get("file_path")
            .or_else(|| get("path"))
            .or_else(|| get("url"))
            .or_else(|| get("command")),
    }
}

fn format_tool_event_text(name: &str, target: Option<&str>, args: &Value) -> String {
    match target {
        Some(target) if !target.is_empty() => format!("{name} {target}"),
        _ => format!("{name} {}", serde_json::to_string(args).unwrap_or_default()),
    }
}

fn is_file_edit_tool(name: &str) -> bool {
    matches!(name, "Edit" | "Write" | "NotebookEdit")
}

fn materialize_tool_result_text(content: &Value) -> Option<String> {
    if let Some(s) = content.as_str() {
        return (!s.trim().is_empty()).then(|| s.to_string());
    }
    if content.is_null() {
        return None;
    }
    serde_json::to_string(content).ok()
}

fn find_tool_use_result(block: &Value) -> Option<&Value> {
    block
        .get("toolUseResult")
        .or_else(|| block.get("tool_use_result"))
        .or_else(|| block.get("content").and_then(|c| c.get("toolUseResult")))
        .or_else(|| block.get("content").and_then(|c| c.get("tool_use_result")))
}

fn count_patch_lines(value: &Value) -> (i64, i64) {
    match value {
        Value::String(s) => count_patch_text(s),
        Value::Array(items) => items
            .iter()
            .map(count_patch_lines)
            .fold((0, 0), |acc, next| (acc.0 + next.0, acc.1 + next.1)),
        Value::Object(map) => {
            for key in ["patch", "diff", "text", "content", "structuredPatch"] {
                if let Some(v) = map.get(key) {
                    let count = count_patch_lines(v);
                    if count != (0, 0) {
                        return count;
                    }
                }
            }
            (0, 0)
        }
        _ => (0, 0),
    }
}

fn count_patch_text(text: &str) -> (i64, i64) {
    let mut added = 0;
    let mut removed = 0;
    for line in text.lines() {
        if line.starts_with("+++") || line.starts_with("---") {
            continue;
        }
        if line.starts_with('+') {
            added += 1;
        } else if line.starts_with('-') {
            removed += 1;
        }
    }
    (added, removed)
}

/// Register a session whose full evidence (`session_events`, `tool_calls`, …)
/// has just been ingested, so the row is marked `discovery_state = 'full'`.
/// Shallow discovery writes through [`discover::upsert_shallow_session`]
/// instead and never downgrades a `'full'` row.
#[allow(clippy::too_many_arguments)]
pub(crate) fn upsert_session(
    conn: &Connection,
    session_id: &str,
    source: &str,
    cwd: Option<&str>,
    git_branch: Option<&str>,
    first_ts: i64,
    last_ts: i64,
    last_assistant_text: Option<&str>,
    raw_path: Option<&str>,
) -> Result<()> {
    // Canonical project identity, derived once per session from the cwd the
    // provider recorded. Resolution is cached per directory, so the repeated
    // upserts a growing transcript produces cost one filesystem walk in total.
    let (project_key, project_key_method) = match crate::project_identity::identity_for(cwd, None) {
        Some((key, method)) => (Some(key), Some(method.as_str().to_string())),
        None => (None, None),
    };
    let project_key_merge = crate::store::project_key_merge_sql();
    conn.execute(
        &format!("INSERT INTO sessions \
         (session_id, source, cwd, git_branch, first_activity_ms, last_activity_ms, last_assistant_text, raw_path, parser_version, project_key, project_key_method, discovery_state) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, 'full') \
         ON CONFLICT(session_id, source) DO UPDATE SET \
         {project_key_merge}, \
         cwd = COALESCE(excluded.cwd, sessions.cwd), \
         git_branch = COALESCE(excluded.git_branch, sessions.git_branch), \
         first_activity_ms = MIN(COALESCE(sessions.first_activity_ms, excluded.first_activity_ms), excluded.first_activity_ms), \
         last_activity_ms = MAX(COALESCE(sessions.last_activity_ms, excluded.last_activity_ms), excluded.last_activity_ms), \
         last_assistant_text = COALESCE(excluded.last_assistant_text, sessions.last_assistant_text), \
         raw_path = COALESCE(excluded.raw_path, sessions.raw_path), \
         parser_version = excluded.parser_version, \
         discovery_state = 'full'"),
        params![
            session_id,
            source,
            cwd,
            git_branch,
            first_ts,
            last_ts,
            last_assistant_text,
            raw_path,
            project_key,
            project_key_method,
        ],
    )?;
    crate::upsert_session_presence(
        conn,
        source,
        session_id,
        SessionLocation::Local,
        raw_path,
        None,
        Some("full"),
    )?;
    Ok(())
}

pub(crate) fn collect_matching_files(root: &Path, prefix: &str, ext: &str) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    collect_matching_files_inner(root, prefix, ext, &mut out)?;
    out.sort();
    Ok(out)
}

fn collect_matching_files_inner(
    root: &Path,
    prefix: &str,
    ext: &str,
    out: &mut Vec<PathBuf>,
) -> Result<()> {
    if !root.is_dir() {
        return Ok(());
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        // The directory read already knows each entry's type; only a symlink
        // needs a stat to see what it points at.
        let file_type = entry.file_type()?;
        let path = entry.path();
        let is_dir = if file_type.is_symlink() {
            path.is_dir()
        } else {
            file_type.is_dir()
        };
        if is_dir {
            collect_matching_files_inner(&path, prefix, ext, out)?;
        } else if path
            .file_name()
            .and_then(|s| s.to_str())
            .is_some_and(|name| name.starts_with(prefix))
            && path.extension().and_then(|s| s.to_str()) == Some(ext)
        {
            out.push(path);
        }
    }
    Ok(())
}

fn stamp_of(metadata: &fs::Metadata) -> String {
    format!(
        "{}:{}",
        metadata
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_nanos())
            .unwrap_or(0),
        metadata.len()
    )
}

fn modified_ms_of(metadata: &fs::Metadata) -> Option<i64> {
    metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64)
}

fn file_stamp(path: &Path) -> Result<String> {
    Ok(stamp_of(&path.metadata()?))
}

/// The change stamp and recency hint discovery needs per enumerated file,
/// from one stat. Errors for anything that is not a regular file.
pub(crate) fn file_stamp_and_modified(path: &Path) -> Result<(String, Option<i64>)> {
    let metadata = path.metadata()?;
    anyhow::ensure!(
        metadata.is_file(),
        "{} is not a regular file",
        path.display()
    );
    Ok((stamp_of(&metadata), modified_ms_of(&metadata)))
}

fn sync_cursor(conn: &Connection, state: &mut Map<String, Value>, root: &Path) -> Result<usize> {
    sync_cursor_with_scan_hook(conn, state, root, &mut |_| {})
}

struct PreparedCursorTranscript {
    path: PathBuf,
    session_id: String,
    project: String,
    timestamp_ms: i64,
    prompts: Vec<String>,
}

struct PreparedCursorSync {
    transcripts: Vec<PreparedCursorTranscript>,
    cursor_state: Map<String, Value>,
    errors: usize,
    files_seen: usize,
}

fn sync_cursor_with_scan_hook(
    conn: &Connection,
    state: &mut Map<String, Value>,
    root: &Path,
    before_transcript: &mut dyn FnMut(&Path),
) -> Result<usize> {
    if !root.exists() {
        return Ok(0);
    }
    // Finish provider I/O and parsing before taking the destination writer lock.
    let prepared = prepare_cursor_sync(state, root, before_transcript)
        .with_context(|| format!("prepare Cursor transcripts from {}", root.display()))?;
    // Match rollback to the source-wide checkpoint boundary: replaying committed
    // prompts after a file's mtime changes can duplicate them.
    let tx = conn.unchecked_transaction()?;
    let mut inserted = 0;
    for transcript in prepared.transcripts {
        for prompt in transcript.prompts {
            inserted += insert_cursor_prompt(
                &tx,
                prompt,
                &transcript.session_id,
                &transcript.project,
                transcript.timestamp_ms,
            )
            .with_context(|| format!("insert Cursor prompt from {}", transcript.path.display()))?;
        }
    }
    tx.commit().context("commit Cursor sync")?;
    state.insert("cursor".to_string(), Value::Object(prepared.cursor_state));
    if prepared.files_seen > 0 {
        let suffix = if prepared.errors > 0 {
            format!(" ({} errors)", prepared.errors)
        } else {
            String::new()
        };
        sync_note!(
            "  [cursor] +{inserted} rows from {} files{suffix}",
            prepared.files_seen
        );
    }
    Ok(inserted)
}

/// One transcript's worth of prepared work. `checkpoint` is returned rather than
/// written in place so a transcript that fails midway leaves its saved checkpoint
/// exactly as it was.
struct ScannedCursorTranscript {
    prompts: Vec<String>,
    timestamp_ms: i64,
    parse_errors: usize,
    checkpoint: Option<Value>,
}

fn scan_cursor_transcript(jsonl: &Path, saved: Option<&Value>) -> Result<ScannedCursorTranscript> {
    let mut source = CompleteJsonlReader::open(jsonl, saved)
        .with_context(|| format!("open Cursor transcript {}", jsonl.display()))?;
    let offset = source.position;
    let size = source
        .reader
        .get_ref()
        .metadata()
        .with_context(|| format!("read Cursor transcript metadata {}", jsonl.display()))?
        .len();
    let timestamp_ms = jsonl
        .metadata()
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let mut consumed = offset;
    let mut prompts = Vec::new();
    let mut parse_errors = 0;
    if offset < size {
        let mut line = String::new();
        while let Some(position) = source
            .next_line(&mut line)
            .with_context(|| format!("read Cursor transcript {}", jsonl.display()))?
        {
            consumed = position;
            match parse_cursor_text(&line) {
                Ok(Some(prompt)) => prompts.push(prompt),
                Ok(None) => {}
                Err(_) => parse_errors += 1,
            }
        }
    }
    let opened_cursor = source.cursor.to_value();
    let checkpoint = if consumed != offset || saved != Some(&opened_cursor) {
        Some(
            source
                .committed_cursor(consumed, true)
                .with_context(|| format!("validate Cursor transcript {}", jsonl.display()))?
                .to_value(),
        )
    } else {
        None
    };
    Ok(ScannedCursorTranscript {
        prompts,
        timestamp_ms,
        parse_errors,
        checkpoint,
    })
}

fn prepare_cursor_sync(
    state: &Map<String, Value>,
    root: &Path,
    before_transcript: &mut dyn FnMut(&Path),
) -> Result<PreparedCursorSync> {
    let mut cursor_state = state
        .get("cursor")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let mut transcripts = Vec::new();
    let mut errors = 0;
    let mut files_seen = 0;
    for project_dir in sorted_dirs(root)? {
        let ts_root = project_dir.join("agent-transcripts");
        if !ts_root.is_dir() {
            continue;
        }
        let project_path = decode_cursor_project(
            project_dir
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or_default(),
        );
        for session_dir in sorted_dirs(&ts_root)? {
            let session_id = session_dir
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or_default()
                .to_string();
            let jsonl = session_dir.join(format!("{session_id}.jsonl"));
            if !jsonl.exists() {
                continue;
            }
            before_transcript(&jsonl);
            files_seen += 1;
            let key = jsonl.to_string_lossy().to_string();
            // Cursor rewrites and rotates transcripts underneath us, so a file can
            // vanish or be replaced between enumeration and open. That race is
            // per-file, not per-source: record it, leave this file's checkpoint
            // untouched so the next sync retries it from the same offset, and keep
            // preparing the remaining transcripts.
            let scan = match scan_cursor_transcript(&jsonl, cursor_state.get(&key)) {
                Ok(scan) => scan,
                Err(error) => {
                    sync_note!("  [cursor] skipping {}: {error:#}", jsonl.display());
                    errors += 1;
                    continue;
                }
            };
            errors += scan.parse_errors;
            if let Some(checkpoint) = scan.checkpoint {
                cursor_state.insert(key, checkpoint);
            }
            if !scan.prompts.is_empty() {
                transcripts.push(PreparedCursorTranscript {
                    path: jsonl,
                    session_id,
                    project: project_path.clone(),
                    timestamp_ms: scan.timestamp_ms,
                    prompts: scan.prompts,
                });
            }
        }
    }
    Ok(PreparedCursorSync {
        transcripts,
        cursor_state,
        errors,
        files_seen,
    })
}

fn ingest_cursor_line(
    conn: &Connection,
    line: &str,
    session_id: &str,
    project: &str,
    timestamp_ms: i64,
) -> Result<usize> {
    let Some(prompt) = parse_cursor_text(line)? else {
        return Ok(0);
    };
    insert_cursor_prompt(conn, prompt, session_id, project, timestamp_ms)
}

fn insert_cursor_prompt(
    conn: &Connection,
    prompt: String,
    session_id: &str,
    project: &str,
    timestamp_ms: i64,
) -> Result<usize> {
    insert_history(
        conn,
        &HistoryEntry {
            id: 0,
            source: "cursor".into(),
            session_id: Some(session_id.to_string()),
            project: Some(project.to_string()),
            prompt_hash: Some(prompt_hash(&prompt)),
            prompt,
            timestamp_ms,
        },
    )
}

pub(crate) fn sorted_dirs(root: &Path) -> Result<Vec<PathBuf>> {
    let mut dirs = Vec::new();
    if root.exists() {
        for entry in fs::read_dir(root)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            let path = entry.path();
            let is_dir = if file_type.is_symlink() {
                path.is_dir()
            } else {
                file_type.is_dir()
            };
            if is_dir {
                dirs.push(path);
            }
        }
    }
    dirs.sort();
    Ok(dirs)
}

pub(crate) fn decode_cursor_project(name: &str) -> String {
    format!("/{}", name.replace('-', "/"))
}

fn sync_grok(conn: &Connection, state: &mut Map<String, Value>, root: &Path) -> Result<usize> {
    if !root.exists() {
        sync_note!("  [grok] not found: {} (skipped)", root.display());
        return Ok(0);
    }
    let mut grok_state = state
        .get("grok_sessions")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let mut inserted = 0;
    let mut scanned = 0;
    let mut sessions = 0;
    let mut errors = 0;
    for chat in capture_files(
        "grok",
        collect_matching_files(root, "chat_history", "jsonl")?,
    ) {
        let key = chat.to_string_lossy().to_string();
        let stamp = grok_session_stamp(&chat)?;
        if grok_state.get(&key).and_then(Value::as_str) == Some(stamp.as_str()) {
            continue;
        }
        scanned += 1;
        match scan_grok_session_file(&chat) {
            Ok(Some(session)) => {
                let raw_path = chat.to_string_lossy().to_string();
                inserted += ingest_grok_session(conn, &session, &raw_path)?;
                sessions += 1;
                grok_state.insert(key, json!(stamp));
            }
            Ok(None) => {
                grok_state.insert(key, json!(stamp));
            }
            Err(_) => errors += 1,
        }
    }
    state.insert("grok_sessions".to_string(), Value::Object(grok_state));
    if scanned > 0 {
        let suffix = if errors > 0 {
            format!(" ({errors} errors)")
        } else {
            String::new()
        };
        sync_note!("  [grok] +{inserted} rows from {sessions} sessions{suffix}");
    }
    Ok(inserted)
}

fn ingest_grok_session(conn: &Connection, session: &GrokSession, raw_path: &str) -> Result<usize> {
    upsert_session(
        conn,
        &session.session_id,
        "grok",
        session.cwd.as_deref(),
        session.git_branch.as_deref(),
        session.first_ts,
        session.last_ts,
        session.last_assistant_text.as_deref(),
        Some(raw_path),
    )?;
    let mut inserted = 0;
    for (idx, prompt) in session.prompts.iter().enumerate() {
        inserted += insert_history(
            conn,
            &HistoryEntry {
                id: 0,
                source: "grok".into(),
                session_id: Some(session.session_id.clone()),
                project: session.cwd.clone(),
                prompt_hash: Some(prompt_hash(prompt)),
                prompt: prompt.clone(),
                timestamp_ms: session.first_ts + idx as i64,
            },
        )?;
    }
    Ok(inserted)
}

fn grok_session_stamp(chat: &Path) -> Result<String> {
    let mut stamp = file_stamp(chat)?;
    let summary = chat.with_file_name("summary.json");
    if summary.exists() {
        stamp.push('|');
        stamp.push_str(&file_stamp(&summary)?);
    }
    Ok(stamp)
}

/// [`grok_session_stamp`] plus the recency hint, with one stat per file.
pub(crate) fn grok_session_stamp_and_modified(chat: &Path) -> Result<(String, Option<i64>)> {
    let metadata = chat.metadata()?;
    anyhow::ensure!(
        metadata.is_file(),
        "{} is not a regular file",
        chat.display()
    );
    let mut stamp = stamp_of(&metadata);
    if let Ok(summary) = chat.with_file_name("summary.json").metadata() {
        stamp.push('|');
        stamp.push_str(&stamp_of(&summary));
    }
    Ok((stamp, modified_ms_of(&metadata)))
}

struct GrokSession {
    session_id: String,
    cwd: Option<String>,
    git_branch: Option<String>,
    first_ts: i64,
    last_ts: i64,
    last_assistant_text: Option<String>,
    prompts: Vec<String>,
}

fn scan_grok_session_file(chat: &Path) -> Result<Option<GrokSession>> {
    let summary = read_grok_summary(&chat.with_file_name("summary.json"));
    let fallback_session = chat
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|s| s.to_str())
        .unwrap_or("unknown");
    let session_id = summary
        .as_ref()
        .and_then(|s| s.pointer("/info/id"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .unwrap_or(fallback_session)
        .to_string();
    let cwd = summary
        .as_ref()
        .and_then(|s| s.pointer("/info/cwd").or_else(|| s.get("git_root_dir")))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| grok_project_from_path(chat));
    let git_branch = summary
        .as_ref()
        .and_then(|s| s.get("head_branch"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let created_ms = summary
        .as_ref()
        .and_then(|s| s.get("created_at").and_then(Value::as_str))
        .and_then(parse_iso_ms)
        .or_else(|| file_modified_ms(chat))
        .unwrap_or(0);
    let updated_ms = summary
        .as_ref()
        .and_then(|s| s.get("updated_at").and_then(Value::as_str))
        .and_then(parse_iso_ms)
        .unwrap_or(created_ms);

    let mut prompts = Vec::new();
    let mut last_assistant_text = None;
    let contents = fs::read_to_string(chat)
        .with_context(|| format!("read Grok chat history {}", chat.display()))?;
    for line in contents.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if let Some(text) = grok_chat_text(&value, "user") {
            prompts.push(text);
        }
        if let Some(text) = grok_chat_text(&value, "assistant") {
            last_assistant_text = Some(text.chars().take(4096).collect());
        }
    }
    if session_id.is_empty() {
        return Ok(None);
    }
    let last_ts = if prompts.is_empty() {
        updated_ms
    } else {
        created_ms + prompts.len() as i64 - 1
    };
    Ok(Some(GrokSession {
        session_id,
        cwd,
        git_branch,
        first_ts: created_ms,
        last_ts,
        last_assistant_text,
        prompts,
    }))
}

pub(crate) fn read_grok_summary(path: &Path) -> Option<Value> {
    serde_json::from_str(&fs::read_to_string(path).ok()?).ok()
}

pub(crate) fn grok_project_from_path(chat: &Path) -> Option<String> {
    let project_dir = chat.parent()?.parent()?.file_name()?.to_str()?;
    percent_decode_path(project_dir)
}

fn percent_decode_path(raw: &str) -> Option<String> {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Some(hex) = bytes
                .get(i + 1..i + 3)
                .and_then(|hex| std::str::from_utf8(hex).ok())
                .and_then(|hex| u8::from_str_radix(hex, 16).ok())
            {
                out.push(hex);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).ok().filter(|s| !s.is_empty())
}

pub(crate) fn file_modified_ms(path: &Path) -> Option<i64> {
    path.metadata().ok().as_ref().and_then(modified_ms_of)
}

pub(crate) fn grok_chat_text(value: &Value, role: &str) -> Option<String> {
    if value.get("type").and_then(Value::as_str) != Some(role) {
        return None;
    }
    if role == "user" && value.get("synthetic_reason").is_some() {
        return None;
    }
    let content = value.get("content")?;
    let mut parts = Vec::new();
    if let Some(text) = content.as_str() {
        parts.push(text);
    } else if let Some(items) = content.as_array() {
        for item in items {
            if item.get("type").and_then(Value::as_str) == Some("text") {
                if let Some(text) = item.get("text").and_then(Value::as_str) {
                    parts.push(text);
                }
            }
        }
    } else if let Some(text) = content.get("text").and_then(Value::as_str) {
        parts.push(text);
    }
    let text = parts
        .into_iter()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    (!text.is_empty()).then_some(text)
}

fn sync_trajectories(
    conn: &Connection,
    state: &mut Map<String, Value>,
    home: &Path,
) -> Result<usize> {
    let files = trajectory_files(home)?;
    if files.is_empty() {
        return Ok(0);
    }
    let mut trajectory_state = state
        .get("trajectory")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let mut inserted = 0;
    let mut updated = 0;
    let mut skipped = 0;
    let mut errors = 0;
    for path in capture_files("trajectory", files) {
        let metadata = match path.metadata() {
            Ok(metadata) => metadata,
            Err(_) => {
                errors += 1;
                continue;
            }
        };
        let stamp = format!(
            "{}:{}",
            metadata
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs())
                .unwrap_or(0),
            metadata.len()
        );
        let key = path.to_string_lossy().to_string();
        if trajectory_state.get(&key).and_then(Value::as_str) == Some(stamp.as_str()) {
            skipped += 1;
            continue;
        }
        let Some(row) = parse_trajectory_file(&path)? else {
            skipped += 1;
            continue;
        };
        let existed: Option<i64> = conn
            .query_row("SELECT 1 FROM trajectories WHERE id = ?", [&row.id], |r| {
                r.get(0)
            })
            .ok();
        if let Err(error) = upsert_trajectory(conn, &row) {
            if is_delivery_retention_limit(&error) {
                return Err(error);
            }
            errors += 1;
            continue;
        }
        trajectory_state.insert(key, json!(stamp));
        if existed.is_some() {
            updated += 1;
        } else {
            inserted += 1;
        }
    }
    state.insert("trajectory".to_string(), Value::Object(trajectory_state));
    let mut parts = vec![format!("+{inserted} rows")];
    if updated > 0 {
        parts.push(format!("{updated} updated"));
    }
    if skipped > 0 {
        parts.push(format!("{skipped} unchanged"));
    }
    if errors > 0 {
        parts.push(format!("{errors} errors"));
    }
    sync_note!("  [trajectory] {}", parts.join(", "));
    Ok(inserted + updated)
}

#[derive(Debug)]
struct TrajectoryRow {
    id: String,
    version: Option<i64>,
    persona_id: Option<String>,
    project_id: Option<String>,
    task_title: Option<String>,
    task_description: Option<String>,
    status: Option<String>,
    started_at: Option<String>,
    completed_at: Option<String>,
    decisions_json: String,
    retrospective_json: String,
    search_text: String,
    path: String,
    updated_ms: i64,
    timestamp_ms: i64,
}

fn trajectory_files(home: &Path) -> Result<Vec<PathBuf>> {
    let mut roots = Vec::new();
    if let Some(raw) = std::env::var_os("TRAJECTORY_ROOT") {
        for part in std::env::split_paths(&raw) {
            if !part.as_os_str().is_empty() {
                roots.push(part);
            }
        }
    } else {
        let projects = home.join("Projects");
        if projects.exists() {
            collect_named_dirs(&projects, ".trajectories", &mut roots)?;
        }
    }
    let mut files = Vec::new();
    for root in roots {
        if root.is_file() && root.extension().and_then(|s| s.to_str()) == Some("json") {
            files.push(root);
            continue;
        }
        if !root.exists() {
            continue;
        }
        // Recursively collect every trajectory JSON under the `.trajectories` root.
        // The parser decides whether each file is a per-run trajectory or compacted roll-up.
        collect_trajectory_json(&root, &mut files)?;
    }
    files.sort();
    files.dedup();
    Ok(files)
}

fn collect_named_dirs(root: &Path, name: &str, out: &mut Vec<PathBuf>) -> Result<()> {
    if !root.is_dir() {
        return Ok(());
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        // Never follow symlinks: dependency links can revisit the same tree or cycle.
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let path = entry.path();
        let file_name = entry.file_name();
        let child = file_name.to_str().unwrap_or("");
        if child == name {
            out.push(path);
        } else if !matches!(
            child,
            "node_modules"
                | ".git"
                | "target"
                | ".next"
                | ".venv"
                | "venv"
                | "__pycache__"
                | ".cache"
        ) {
            collect_named_dirs(&path, name, out)?;
        }
    }
    Ok(())
}

/// Recursively collect trajectory JSON under a `.trajectories` root: `completed/<month>/`
/// individual runs, `compacted/` roll-ups, `active/`. Skips index/state/trace sidecars;
/// `parse_trajectory_file` decides per-file what's mappable.
fn collect_trajectory_json(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let path = entry.path();
        if file_type.is_dir() {
            collect_trajectory_json(&path, out)?;
        } else if file_type.is_file() && path.extension().and_then(|s| s.to_str()) == Some("json") {
            let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
            if name != "index.json" && name != ".sync-state.json" && !name.ends_with(".trace.json")
            {
                out.push(path);
            }
        }
    }
    Ok(())
}

fn parse_trajectory_file(path: &Path) -> Result<Option<TrajectoryRow>> {
    let obj: Value = match serde_json::from_str(&fs::read_to_string(path)?) {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };
    let Some(map) = obj.as_object() else {
        return Ok(None);
    };
    let Some(id) = map
        .get("id")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    else {
        return Ok(None);
    };
    let is_compacted = map.get("type").and_then(Value::as_str) == Some("compacted")
        && map
            .get("sourceTrajectories")
            .and_then(Value::as_array)
            .is_some();
    let task = map.get("task").and_then(Value::as_object);
    let retrospective = map.get("retrospective").and_then(Value::as_object);
    let decisions = map
        .get("decisions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(Value::is_object)
        .collect::<Vec<_>>();
    let search_text = trajectory_search_text(map);
    let timestamp_ms = trajectory_timestamp_ms(map, path);
    let updated_ms = path
        .metadata()
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64)
        .unwrap_or(timestamp_ms);
    Ok(Some(TrajectoryRow {
        id: id.to_string(),
        version: map.get("version").and_then(Value::as_i64),
        persona_id: map
            .get("personaId")
            .and_then(Value::as_str)
            .map(str::to_string),
        project_id: map
            .get("projectId")
            .and_then(Value::as_str)
            .map(str::to_string),
        task_title: task
            .and_then(|m| m.get("title"))
            .and_then(Value::as_str)
            .map(str::to_string),
        task_description: task
            .and_then(|m| m.get("description"))
            .and_then(Value::as_str)
            .map(str::to_string),
        status: map
            .get("status")
            .and_then(Value::as_str)
            .map(str::to_string),
        started_at: map
            .get("startedAt")
            .and_then(Value::as_str)
            .map(str::to_string),
        completed_at: map
            .get("completedAt")
            .and_then(Value::as_str)
            .map(str::to_string),
        decisions_json: serde_json::to_string(&decisions)?,
        retrospective_json: if is_compacted {
            serde_json::to_string(map)?
        } else {
            serde_json::to_string(retrospective.unwrap_or(&Map::new()))?
        },
        search_text,
        path: path.to_string_lossy().to_string(),
        updated_ms,
        timestamp_ms,
    }))
}

fn trajectory_search_text(map: &Map<String, Value>) -> String {
    let mut parts = Vec::new();
    for key in ["id", "personaId", "projectId", "status"] {
        push_text(&mut parts, map.get(key));
    }
    if let Some(task) = map.get("task").and_then(Value::as_object) {
        push_text(&mut parts, task.get("title"));
        push_text(&mut parts, task.get("description"));
    }
    if let Some(decisions) = map.get("decisions").and_then(Value::as_array) {
        for decision in decisions {
            if let Some(decision) = decision.as_object() {
                for key in ["question", "chosen", "reasoning"] {
                    push_text(&mut parts, decision.get(key));
                }
                if let Some(items) = decision.get("alternatives").and_then(Value::as_array) {
                    for item in items {
                        push_text(&mut parts, Some(item));
                    }
                }
            }
        }
    }
    if let Some(retro) = map.get("retrospective").and_then(Value::as_object) {
        for key in ["summary", "approach"] {
            push_text(&mut parts, retro.get(key));
        }
        if let Some(confidence) = retro.get("confidence") {
            parts.push(confidence.to_string());
        }
        if let Some(items) = retro.get("learnings").and_then(Value::as_array) {
            for item in items {
                push_text(&mut parts, Some(item));
            }
        }
    }
    if map.get("type").and_then(Value::as_str) == Some("compacted") {
        push_text(&mut parts, map.get("narrative"));
        for key in ["keyFindings", "keyLearnings", "openQuestions"] {
            if let Some(items) = map.get(key).and_then(Value::as_array) {
                for item in items {
                    push_text(&mut parts, Some(item));
                }
            }
        }
        for key in ["lessons", "conventions"] {
            if let Some(items) = map.get(key).and_then(Value::as_array) {
                for item in items {
                    if let Some(item) = item.as_object() {
                        for value in item.values() {
                            push_text(&mut parts, Some(value));
                        }
                    }
                }
            }
        }
    }
    parts.join("\n")
}

fn push_text(parts: &mut Vec<String>, value: Option<&Value>) {
    if let Some(text) = value.and_then(Value::as_str).filter(|s| !s.is_empty()) {
        parts.push(text.to_string());
    }
}

fn trajectory_timestamp_ms(map: &Map<String, Value>, path: &Path) -> i64 {
    for key in ["completedAt", "startedAt", "compactedAt"] {
        if let Some(ms) = map
            .get(key)
            .and_then(Value::as_str)
            .and_then(parse_iso_ms)
            .filter(|ms| *ms > 0)
        {
            return ms;
        }
    }
    path.metadata()
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn upsert_trajectory(conn: &Connection, row: &TrajectoryRow) -> Result<()> {
    conn.execute(
        "INSERT INTO trajectories \
         (id, version, persona_id, project_id, task_title, task_description, status, started_at, completed_at, decisions_json, retrospective_json, search_text, path, updated_ms, timestamp_ms) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(id) DO UPDATE SET version=excluded.version, persona_id=excluded.persona_id, project_id=excluded.project_id, task_title=excluded.task_title, task_description=excluded.task_description, status=excluded.status, started_at=excluded.started_at, completed_at=excluded.completed_at, decisions_json=excluded.decisions_json, retrospective_json=excluded.retrospective_json, search_text=excluded.search_text, path=excluded.path, updated_ms=excluded.updated_ms, timestamp_ms=excluded.timestamp_ms",
        params![
            row.id,
            row.version,
            row.persona_id,
            row.project_id,
            row.task_title,
            row.task_description,
            row.status,
            row.started_at,
            row.completed_at,
            row.decisions_json,
            row.retrospective_json,
            row.search_text,
            row.path,
            row.updated_ms,
            row.timestamp_ms,
        ],
    )?;
    conn.execute(
        "DELETE FROM history WHERE source = 'trajectory' AND session_id = ?",
        [&row.id],
    )?;
    insert_history(
        conn,
        &HistoryEntry {
            id: 0,
            source: "trajectory".into(),
            session_id: Some(row.id.clone()),
            project: row.project_id.clone(),
            prompt_hash: Some(prompt_hash(&row.search_text)),
            prompt: row.search_text.clone(),
            timestamp_ms: row.timestamp_ms,
        },
    )?;
    Ok(())
}

pub(crate) fn parse_iso_ms(raw: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|dt| dt.timestamp_millis())
}

fn parse_claude_line(line: &str) -> Result<Option<HistoryEntry>> {
    crate::parse_claude(line)
}

fn parse_codex_line(line: &str) -> Result<Option<HistoryEntry>> {
    crate::parse_codex(line)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{init_db, open_db, HistoryEntry, QueryFilter, SourceDatabaseError};
    use rusqlite::Connection;
    use serde_json::{json, Map, Value};
    use std::{fs, io::Write as _, time::Duration};
    fn saved_cursor_offset(value: &Value) -> u64 {
        match super::FileCursor::decode(value).expect("valid file cursor") {
            super::DecodedFileCursor::Legacy(offset) => offset,
            super::DecodedFileCursor::Typed(cursor) => cursor.offset,
        }
    }

    fn test_file_cursor(offset: u64, inode: u64, generation: u64) -> Value {
        super::FileCursor {
            offset,
            generation: super::FileGeneration {
                device: Some(1),
                inode: Some(inode),
                started_mtime_ns: generation,
                started_size: 100,
                observed_at_ns: generation,
                rewrite_epoch: 0,
            },
            observed_mtime_ns: generation,
            prefix_hash: Some("test-prefix".to_string()),
        }
        .to_value()
    }

    fn decode_typed_cursor(value: Value) -> super::FileCursor {
        match super::FileCursor::decode(&value).expect("valid typed cursor") {
            super::DecodedFileCursor::Typed(cursor) => cursor,
            super::DecodedFileCursor::Legacy(_) => panic!("expected typed cursor"),
        }
    }

    #[test]
    fn sync_lock_canonicalizes_aliases_and_blocks_every_sync_entry_point_before_open() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("history.db");
        let alias = dir.path().join(".").join("history.db");
        let missing_opencode = dir.path().join("missing-opencode.db");

        let first = try_acquire_sync_lock(&db_path).unwrap().unwrap();
        assert!(try_acquire_sync_lock(&alias).unwrap().is_none());
        assert!(
            !db_path.exists(),
            "the sidecar lock must not initialize SQLite"
        );
        assert!(!sync_exclusive(&alias).unwrap());
        assert!(!sync_local_at(&alias).unwrap());
        assert!(!sync_opencode_exclusive(&alias, &missing_opencode).unwrap());
        assert!(
            !db_path.exists(),
            "contended sync paths must not create the DB"
        );

        drop(first);
        for _ in 0..16 {
            let reacquired = try_acquire_sync_lock(&alias)
                .unwrap()
                .expect("a dropped sync guard must release the lock immediately");
            drop(reacquired);
        }
    }

    #[test]
    fn reflex_uses_a_read_only_database_and_keeps_pushing_when_scan_lock_is_busy() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("history.db");
        drop(open_db(&db_path).unwrap());
        let _scan_owner = try_acquire_sync_lock(&db_path).unwrap().unwrap();

        let (conn, sync_skipped) = prepare_local_sync_snapshot(&db_path).unwrap();
        assert!(sync_skipped);
        assert!(conn
            .query_row("SELECT COUNT(*) FROM history", [], |row| row
                .get::<_, i64>(0))
            .is_ok());
        assert!(
            conn.execute("CREATE TABLE should_not_write(id INTEGER)", [])
                .is_err(),
            "the push-only fallback must not join SQLite writer contention"
        );
    }

    #[test]
    fn load_sync_state_recovers_from_empty_or_corrupt_file() {
        let dir = std::env::temp_dir().join(format!("ai-hist-state-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(".sync-state.json");

        // Missing file: nothing synced yet.
        assert!(load_sync_state(&path).unwrap().is_empty());

        // Empty file — what an ENOSPC-truncated write leaves behind. Used to
        // abort every sync with "EOF while parsing a value at line 1 column 0".
        fs::write(&path, "").unwrap();
        assert!(load_sync_state(&path).unwrap().is_empty());

        // Partially written / otherwise corrupt JSON.
        fs::write(&path, "{\"claude\": ").unwrap();
        assert!(load_sync_state(&path).unwrap().is_empty());

        // Valid state still round-trips.
        let mut state = Map::new();
        state.insert("claude".into(), json!({"offset": 42}));
        save_sync_state(&path, &state).unwrap();
        assert_eq!(load_sync_state(&path).unwrap(), state);

        // The temp file is renamed away, never left beside the real one.
        assert_eq!(leftover_tmp_files(&dir), Vec::<String>::new());

        fs::remove_dir_all(&dir).ok();
    }

    /// Temp files staged by `save_sync_state`, which should never outlive a save.
    fn leftover_tmp_files(dir: &std::path::Path) -> Vec<String> {
        let mut found: Vec<String> = fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with(".sync-state.json.tmp"))
            .collect();
        found.sort();
        found
    }

    #[test]
    fn stale_sync_state_temps_are_removed_without_touching_a_live_writer() {
        let dir =
            std::env::temp_dir().join(format!("ai-hist-state-cleanup-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(".sync-state.json");
        let stale = dir.join(".sync-state.json.tmp.999999999.0");
        let live = dir.join(format!(".sync-state.json.tmp.{}.0", std::process::id()));
        fs::write(&stale, "stale").unwrap();
        fs::write(&live, "live").unwrap();

        assert_eq!(cleanup_stale_sync_state_temps(&path).unwrap(), 1);
        assert!(!stale.exists());
        assert!(live.exists());

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_failed_source_does_not_prevent_later_sources_from_completing() {
        let mut report = SyncSourceReport::default();
        assert!(report
            .capture::<usize>("broken", Err(anyhow::anyhow!("bad source")))
            .is_none());
        assert_eq!(report.capture("healthy", Ok(7)), Some(7));
        assert_eq!(report.succeeded, 1);
        assert_eq!(report.failures.len(), 1);
        assert!(report.finish(std::path::Path::new("unused.db")).is_ok());

        let mut all_failed = SyncSourceReport::default();
        all_failed.capture::<usize>("only", Err(anyhow::anyhow!("still bad")));
        assert!(all_failed
            .finish(std::path::Path::new("unused.db"))
            .is_err());
    }

    #[test]
    fn contention_diagnostics_reprobe_write_capability() {
        let dir = std::env::temp_dir().join(format!("ai-hist-contention-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("contention.db");
        let holder = open_db(&path).unwrap();
        holder.execute_batch("BEGIN IMMEDIATE").unwrap();

        let contender = Connection::open(&path).unwrap();
        contender.busy_timeout(std::time::Duration::ZERO).unwrap();
        let busy = contender
            .execute_batch("BEGIN IMMEDIATE")
            .expect_err("the competing writer must be busy");
        assert!(is_sqlite_contention(&anyhow::Error::new(busy)));
        let blocked = write_contention_diagnostic(&path);
        assert!(blocked.contains("write capability probe is still blocked"));

        holder.execute_batch("ROLLBACK").unwrap();
        let recovered = write_contention_diagnostic(&path);
        assert!(recovered.contains("write capability probe now succeeds"));

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn contention_diagnostics_keep_source_paths_and_recovered_wal_wording_accurate() {
        let source = std::path::PathBuf::from("/tmp/opencode-source.db");
        let busy = rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
            None,
        );
        let error = anyhow::Error::new(SourceDatabaseError::new(&source, busy));
        assert!(is_sqlite_contention(&error));
        assert_eq!(source_database_path(&error), Some(source.as_path()));

        let recovered = wal_contention_line(WAL_WARN_BYTES + 1, false).unwrap();
        assert!(
            recovered.contains("write capability recovered"),
            "{recovered}"
        );
        assert!(!recovered.contains("write path is failing"), "{recovered}");
        let blocked = wal_contention_line(WAL_WARN_BYTES + 1, true).unwrap();
        assert!(blocked.contains("write path is failing"), "{blocked}");
    }

    #[test]
    fn process_status_uses_an_absolute_fallback_when_path_lookup_fails() {
        let pid = std::process::id().to_string();
        let (state, command) = process_status_with_programs(
            &pid,
            &["/definitely/missing/ps", "/bin/ps", "/usr/bin/ps"],
        )
        .expect("an absolute system ps should describe the current process");
        assert!(!state.is_empty());
        assert!(!command.is_empty());
    }

    #[test]
    fn a_slow_run_cannot_rewind_or_clobber_a_faster_runs_cursors() {
        let dir = std::env::temp_dir().join(format!("ai-hist-merge-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(".sync-state.json");

        // A fast run finished claude and codex and moved on.
        let mut fast = Map::new();
        fast.insert("claude".into(), json!(900));
        fast.insert("codex".into(), json!(500));
        checkpoint_sync_state(&path, &fast);

        // A slow overlapping run only just finished claude, at an older offset.
        // Writing its whole map wholesale would rewind claude and delete codex,
        // sending the next run back over work that was already done.
        let mut slow = Map::new();
        slow.insert("claude".into(), json!(400));
        checkpoint_sync_state(&path, &slow);

        let on_disk = load_sync_state(&path).unwrap();
        assert_eq!(on_disk.get("claude").and_then(Value::as_u64), Some(900));
        assert_eq!(on_disk.get("codex").and_then(Value::as_u64), Some(500));

        // A genuinely newer cursor still advances.
        let mut newer = Map::new();
        newer.insert("claude".into(), json!(1200));
        checkpoint_sync_state(&path, &newer);
        assert_eq!(
            load_sync_state(&path)
                .unwrap()
                .get("claude")
                .and_then(Value::as_u64),
            Some(1200)
        );

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn typed_cursor_merges_are_monotonic_within_a_generation_and_replace_stale_generations() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".sync-state.json");
        let transcript = "/cursor/session.jsonl";

        let mut newest = Map::new();
        newest.insert("claude".into(), test_file_cursor(25, 10, 200));
        newest.insert(
            "cursor".into(),
            json!({transcript: test_file_cursor(30, 10, 200)}),
        );
        checkpoint_sync_state(&path, &newest);

        // A stale writer from the prior inode cannot restore its larger offset.
        let mut stale_generation = Map::new();
        stale_generation.insert("claude".into(), test_file_cursor(900, 9, 100));
        stale_generation.insert(
            "cursor".into(),
            json!({transcript: test_file_cursor(800, 9, 100)}),
        );
        checkpoint_sync_state(&path, &stale_generation);

        // Nor can a slow writer rewind the active generation.
        let mut slow_same_generation = Map::new();
        let mut later_open = decode_typed_cursor(test_file_cursor(12, 10, 200));
        later_open.generation.started_mtime_ns += 10;
        later_open.generation.started_size += 50;
        later_open.generation.observed_at_ns += 10;
        slow_same_generation.insert("claude".into(), later_open.to_value());
        slow_same_generation.insert(
            "cursor".into(),
            json!({transcript: test_file_cursor(14, 10, 200)}),
        );
        checkpoint_sync_state(&path, &slow_same_generation);

        let saved = load_sync_state(&path).unwrap();
        assert_eq!(saved_cursor_offset(&saved["claude"]), 25);
        assert_eq!(saved_cursor_offset(&saved["cursor"][transcript]), 30);
    }

    #[test]
    fn a_same_inode_rewrite_epoch_supersedes_the_old_larger_offset() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".sync-state.json");
        let mut old = Map::new();
        old.insert("claude".into(), test_file_cursor(900, 10, 100));
        checkpoint_sync_state(&path, &old);

        let mut reset = decode_typed_cursor(test_file_cursor(0, 10, 200));
        reset.generation.rewrite_epoch = 1;
        let mut rewritten = Map::new();
        rewritten.insert("claude".into(), reset.to_value());
        checkpoint_sync_state(&path, &rewritten);

        let saved = load_sync_state(&path).unwrap();
        assert_eq!(saved_cursor_offset(&saved["claude"]), 0);
    }

    #[test]
    fn a_new_inode_wins_by_observation_time_even_with_a_lower_rewrite_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".sync-state.json");
        let mut old_cursor = decode_typed_cursor(test_file_cursor(900, 10, 100));
        old_cursor.generation.rewrite_epoch = 10;
        let mut old = Map::new();
        old.insert("claude".into(), old_cursor.to_value());
        checkpoint_sync_state(&path, &old);

        let mut replacement = decode_typed_cursor(test_file_cursor(0, 11, 200));
        replacement.generation.rewrite_epoch = 2;
        let mut new = Map::new();
        new.insert("claude".into(), replacement.to_value());
        checkpoint_sync_state(&path, &new);

        let saved = load_sync_state(&path).unwrap();
        assert_eq!(saved_cursor_offset(&saved["claude"]), 0);
        let saved = decode_typed_cursor(saved["claude"].clone());
        assert_eq!(saved.generation.inode, Some(11));
    }

    #[test]
    fn sync_state_merge_recurses_through_all_object_maps() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".sync-state.json");
        let mut on_disk = Map::new();
        on_disk.insert(
            "claude_sessions_v3".into(),
            json!({"first.jsonl": "old", "nested": {"left": 1}}),
        );
        save_sync_state(&path, &on_disk).unwrap();

        let mut ours = Map::new();
        ours.insert(
            "claude_sessions_v3".into(),
            json!({"second.jsonl": "new", "nested": {"right": 2}}),
        );
        checkpoint_sync_state(&path, &ours);

        assert_eq!(
            load_sync_state(&path).unwrap()["claude_sessions_v3"],
            json!({
                "first.jsonl": "old",
                "second.jsonl": "new",
                "nested": {"left": 1, "right": 2}
            })
        );
    }

    /// The v4->v5 migration drops `codex_rollouts_v4` from the in-memory map, but
    /// the merge only folds in the keys a run *has*, so on its own that deletion
    /// never reaches disk: the retired map is reloaded and rewritten forever.
    #[test]
    fn retired_state_keys_are_deleted_from_disk_not_just_from_memory() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".sync-state.json");
        let mut on_disk = Map::new();
        on_disk.insert(
            "codex_rollouts_v4".into(),
            json!({"a.jsonl": {"stamp": "1:1"}}),
        );
        on_disk.insert("codex_rollouts".into(), json!({"legacy.jsonl": "1:1"}));
        on_disk.insert(
            "codex_rollout_user_messages_v2".into(),
            json!({"u.jsonl": "1:1"}),
        );
        on_disk.insert("codex_rollouts_v3".into(), json!({"v3.jsonl": "1:1"}));
        on_disk.insert("claude".into(), json!({"keep.jsonl": 7}));
        save_sync_state(&path, &on_disk).unwrap();

        // What a post-migration run holds: v5 written, the retired keys removed.
        let mut ours = Map::new();
        ours.insert(
            "codex_rollouts_v5".into(),
            json!({"a.jsonl": {"stamp": "2:2"}}),
        );
        checkpoint_sync_state(&path, &ours);

        let saved = load_sync_state(&path).unwrap();
        for (retired, _) in super::RETIRED_SYNC_STATE_KEYS {
            assert!(
                !saved.contains_key(*retired),
                "{retired} must not survive the migration on disk"
            );
        }
        assert_eq!(
            saved["codex_rollouts_v5"],
            json!({"a.jsonl": {"stamp": "2:2"}})
        );
        // A source this run never touched must still be preserved -- absence from
        // `ours` is not a deletion, which is why retirement has to be declared.
        assert_eq!(saved["claude"], json!({"keep.jsonl": 7}));

        // Idempotent: with nothing retired left on disk, a steady-state run that
        // changes nothing must not rewrite the file.
        assert!(super::merged_sync_state(&path, &ours).unwrap().is_none());
    }

    /// `checkpoint_sync_state` runs once per source, and `codex_rollouts_v4` is
    /// still read to seed the v5 migration. An earlier source's checkpoint must
    /// therefore not drop it: if a crash landed between that checkpoint and
    /// `sync_codex_rollouts` writing v5, neither map would survive and the next
    /// run would re-read the whole archive.
    #[test]
    fn a_retired_key_survives_until_its_successor_is_written() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".sync-state.json");
        let mut on_disk = Map::new();
        on_disk.insert(
            "codex_rollouts_v4".into(),
            json!({"a.jsonl": {"stamp": "1:1"}}),
        );
        save_sync_state(&path, &on_disk).unwrap();

        // An earlier source checkpoints first; codex has not run yet, so nothing
        // in this write supersedes v4.
        let mut early = Map::new();
        early.insert("claude".into(), json!({"c.jsonl": 3}));
        checkpoint_sync_state(&path, &early);
        assert_eq!(
            load_sync_state(&path).unwrap()["codex_rollouts_v4"],
            json!({"a.jsonl": {"stamp": "1:1"}}),
            "v4 must still be readable until v5 replaces it"
        );

        // Codex then runs and writes v5 in the same state map.
        let mut after_codex = early.clone();
        after_codex.insert(
            "codex_rollouts_v5".into(),
            json!({"a.jsonl": {"stamp": "2:2"}}),
        );
        checkpoint_sync_state(&path, &after_codex);
        let saved = load_sync_state(&path).unwrap();
        assert!(!saved.contains_key("codex_rollouts_v4"));
        assert_eq!(
            saved["codex_rollouts_v5"],
            json!({"a.jsonl": {"stamp": "2:2"}})
        );
    }

    #[test]
    fn an_unchanged_source_does_not_rewrite_the_state_file() {
        let dir = std::env::temp_dir().join(format!("ai-hist-norewrite-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(".sync-state.json");

        let mut state = Map::new();
        state.insert("claude".into(), json!(42));
        state.insert("codex".into(), json!({"files": {"a.jsonl": 7}}));
        assert!(super::merged_sync_state(&path, &state).unwrap().is_some());
        checkpoint_sync_state(&path, &state);

        // Steady state: every source reports "up to date" and checkpoints the
        // same map after each one. Rewriting the full file seven times a minute
        // for no change is pure cost, so nothing should be written.
        assert!(super::merged_sync_state(&path, &state).unwrap().is_none());

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn local_named_catalog_wrappers_reject_nonlocal_scope_without_touching_disk() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("must-not-be-created.db");

        let list_error = super::list_sessions_local_at(
            &db,
            &super::CatalogListOptions {
                scope: super::SessionScope::Remote,
                ..Default::default()
            },
        )
        .expect_err("the local wrapper must not coerce a remote request");
        assert!(list_error
            .to_string()
            .contains("use list_sessions_scoped_at"));
        assert!(!db.exists());

        let discover_error = super::discover_sessions_local_at(
            &db,
            &super::DiscoverOptions {
                scope: super::SessionScope::All,
                ..Default::default()
            },
        )
        .expect_err("the local wrapper must not coerce an all-scope request");
        assert!(discover_error
            .to_string()
            .contains("use discover_sessions_scoped_at"));
        assert!(!db.exists());
    }

    #[test]
    fn normalized_local_evidence_records_local_presence() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();

        super::insert_session_event(
            &conn,
            "claude",
            "event-session",
            None,
            None,
            None,
            "message-1",
            None,
            1,
            "user",
            "text",
            Some("hello"),
            None,
            None,
            "event-1",
            super::RawMessageFacts::default(),
        )
        .unwrap();
        super::insert_tool_call(
            &conn,
            "claude",
            "tool-session",
            "message-2",
            "tool-1",
            "Read",
            Some("README.md"),
            "{}",
            None,
            2,
        )
        .unwrap();
        super::upsert_file_edit_from_call(
            &conn,
            "claude",
            "edit-session",
            "message-3",
            "tool-2",
            "src/lib.rs",
            "Edit",
            3,
            None,
            None,
        )
        .unwrap();

        for session_id in ["event-session", "tool-session", "edit-session"] {
            let count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM session_presences \
                     WHERE source = 'claude' AND session_id = ? AND location = 'local'",
                    [session_id],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count, 1, "missing local presence for {session_id}");
        }
    }

    #[test]
    fn codex_subagent_cleanup_preserves_remote_session_and_local_evidence() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        conn.execute(
            "INSERT INTO sessions (source, session_id) VALUES ('codex', 'subagent')",
            [],
        )
        .unwrap();
        crate::mark_session_presence(&conn, "codex", "subagent", super::SessionLocation::Local)
            .unwrap();
        crate::mark_session_presence(&conn, "codex", "subagent", super::SessionLocation::Remote)
            .unwrap();

        super::cleanup_codex_subagent_registration(&conn, "subagent").unwrap();

        let locations: Vec<String> = conn
            .prepare(
                "SELECT location FROM session_presences \
                 WHERE source = 'codex' AND session_id = 'subagent' ORDER BY location",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(locations, vec!["local", "remote"]);
        let session_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sessions \
                 WHERE source = 'codex' AND session_id = 'subagent'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(session_count, 1);

        let local_page = super::list_session_catalog_page(
            &conn,
            &super::CatalogListOptions {
                scope: super::SessionScope::Local,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(local_page.sessions.len(), 1);
        assert_eq!(local_page.sessions[0].session_id, "subagent");
    }

    #[test]
    fn a_stopped_or_zombie_holder_is_reported_as_wedged() {
        let wedged = |state: &str| {
            super::DbHolder {
                pid: "1".into(),
                state: state.into(),
                command: "agent-relay".into(),
            }
            .is_wedged()
        };

        // Stopped and zombie processes never run again on their own, so a lock
        // they hold is held forever -- this is the case worth surfacing.
        assert!(wedged("T"));
        assert!(wedged("Ts"));
        assert!(wedged("Z"));
        // Running or sleeping holders are normal and will release in time.
        assert!(!wedged("S"));
        assert!(!wedged("R"));
        assert!(!wedged("Ss"));
    }

    #[test]
    fn human_bytes_scales_units() {
        assert_eq!(super::human_bytes(512), "512 B");
        assert_eq!(super::human_bytes(1024), "1.0 KB");
        assert_eq!(super::human_bytes(156_205_712), "149.0 MB");
        assert_eq!(super::human_bytes(3_038_662_656), "2.8 GB");
    }

    #[test]
    fn jsonl_ingest_checkpoints_mid_source_and_resumes_from_there() {
        let dir = std::env::temp_dir().join(format!("ai-hist-jsonl-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("history.jsonl");

        // Span several chunks so checkpoints land inside the source, not just
        // at its end -- that is what lets an interrupted run make progress.
        let lines = super::JSONL_CHUNK_LINES * 2 + 500;
        let body: String = (0..lines)
            .map(|i| {
                format!(
                    r#"{{"display":"prompt {i}","timestamp":{},"project":"/p","sessionId":"s"}}"#,
                    i + 1
                ) + "\n"
            })
            .collect();
        fs::write(&path, &body).unwrap();

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        let mut checkpoints: Vec<Value> = Vec::new();
        let inserted = super::sync_jsonl_incremental(
            &conn,
            &mut state,
            "claude",
            &path,
            super::parse_claude_line,
            &mut |in_progress| {
                checkpoints.push(in_progress["claude"].clone());
            },
        )
        .unwrap();
        assert_eq!(inserted, lines);

        // Progress was published while the source was still running, and each
        // checkpoint is a real byte position inside the file.
        assert_eq!(checkpoints.len(), lines / super::JSONL_CHUNK_LINES);
        let checkpoint_offsets: Vec<u64> = checkpoints.iter().map(saved_cursor_offset).collect();
        assert!(checkpoint_offsets.windows(2).all(|w| w[0] < w[1]));
        assert!(checkpoint_offsets.iter().all(|&at| at < body.len() as u64));
        assert_eq!(saved_cursor_offset(&state["claude"]), body.len() as u64);

        // Resuming from a mid-file checkpoint ingests only the remainder, and
        // the offsets line up exactly -- nothing skipped, nothing double-counted.
        let resumed_conn = Connection::open_in_memory().unwrap();
        init_db(&resumed_conn).unwrap();
        let mut resumed = Map::new();
        resumed.insert("claude".into(), checkpoints[0].clone());
        let after = super::sync_jsonl_incremental(
            &resumed_conn,
            &mut resumed,
            "claude",
            &path,
            super::parse_claude_line,
            &mut |_| {},
        )
        .unwrap();
        assert_eq!(after, lines - super::JSONL_CHUNK_LINES);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn generic_jsonl_retries_partial_lines_then_imports_them_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.jsonl");
        fs::write(&path, r#"{"display":"half"#).unwrap();
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();

        assert_eq!(
            super::sync_jsonl_incremental(
                &conn,
                &mut state,
                "claude",
                &path,
                super::parse_claude_line,
                &mut |_| {},
            )
            .unwrap(),
            0
        );
        assert_eq!(saved_cursor_offset(&state["claude"]), 0);

        let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(br#" prompt","timestamp":1,"project":"/p","sessionId":"s"}"#)
            .unwrap();
        file.write_all(b"\n").unwrap();
        drop(file);
        assert_eq!(
            super::sync_jsonl_incremental(
                &conn,
                &mut state,
                "claude",
                &path,
                super::parse_claude_line,
                &mut |_| {},
            )
            .unwrap(),
            1
        );
        assert_eq!(
            super::sync_jsonl_incremental(
                &conn,
                &mut state,
                "claude",
                &path,
                super::parse_claude_line,
                &mut |_| {},
            )
            .unwrap(),
            0
        );
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM history", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
    }

    #[test]
    fn generic_jsonl_skips_complete_malformed_lines_permanently() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.jsonl");
        fs::write(&path, "{malformed}\n").unwrap();
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        let sync = |state: &mut Map<String, Value>| {
            super::sync_jsonl_incremental(
                &conn,
                state,
                "claude",
                &path,
                super::parse_claude_line,
                &mut |_| {},
            )
            .unwrap()
        };

        assert_eq!(sync(&mut state), 0);
        assert_eq!(saved_cursor_offset(&state["claude"]), 12);
        assert_eq!(sync(&mut state), 0);
        assert_eq!(saved_cursor_offset(&state["claude"]), 12);
    }

    #[test]
    fn generic_jsonl_skips_complete_non_utf8_lines_permanently() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.jsonl");
        fs::write(&path, [0xff, b'\n']).unwrap();
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();

        assert_eq!(
            super::sync_jsonl_incremental(
                &conn,
                &mut state,
                "claude",
                &path,
                super::parse_claude_line,
                &mut |_| {},
            )
            .unwrap(),
            0
        );
        assert_eq!(saved_cursor_offset(&state["claude"]), 2);

        let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
        writeln!(
            file,
            r#"{{"display":"valid","timestamp":1,"sessionId":"s"}}"#
        )
        .unwrap();
        drop(file);
        assert_eq!(
            super::sync_jsonl_incremental(
                &conn,
                &mut state,
                "claude",
                &path,
                super::parse_claude_line,
                &mut |_| {},
            )
            .unwrap(),
            1
        );
    }

    #[test]
    fn generic_jsonl_resets_after_truncation_and_atomic_replacement() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.jsonl");
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        let sync = |state: &mut Map<String, Value>| {
            super::sync_jsonl_incremental(
                &conn,
                state,
                "claude",
                &path,
                super::parse_claude_line,
                &mut |_| {},
            )
            .unwrap()
        };

        fs::write(
            &path,
            concat!(
                r#"{"display":"a deliberately long original prompt","timestamp":1,"sessionId":"old"}"#,
                "\n"
            ),
        )
        .unwrap();
        assert_eq!(sync(&mut state), 1);

        fs::write(
            &path,
            concat!(
                r#"{"display":"short","timestamp":2,"sessionId":"new"}"#,
                "\n"
            ),
        )
        .unwrap();
        assert_eq!(sync(&mut state), 1, "same-inode truncation must reset");
        assert_eq!(sync(&mut state), 0);

        fs::write(
            &path,
            concat!(
                r#"{"display":"same inode regrown beyond the previous cursor","timestamp":3,"sessionId":"regrown"}"#,
                "\n"
            ),
        )
        .unwrap();
        assert_eq!(
            sync(&mut state),
            1,
            "same-inode truncate-and-regrow must reset"
        );
        assert_eq!(sync(&mut state), 0);

        let replacement = dir.path().join("replacement.jsonl");
        fs::write(
            &replacement,
            concat!(
                r#"{"display":"replacement prompt longer than the prior cursor","timestamp":4,"sessionId":"replacement"}"#,
                "\n"
            ),
        )
        .unwrap();
        fs::rename(&replacement, &path).unwrap();
        assert_eq!(sync(&mut state), 1, "new inode must reset even when larger");
        assert_eq!(sync(&mut state), 0);
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM history", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            4
        );
    }

    #[test]
    fn generic_jsonl_detects_same_size_rewrites_with_an_unchanged_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.jsonl");
        let shared_tail = "x".repeat(256);
        let original =
            format!(r#"{{"display":"old-one-{shared_tail}","timestamp":1,"sessionId":"old"}}"#)
                + "\n";
        let replacement =
            format!(r#"{{"display":"new-one-{shared_tail}","timestamp":2,"sessionId":"new"}}"#)
                + "\n";
        assert_eq!(original.len(), replacement.len());
        fs::write(&path, original).unwrap();
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        let sync = |state: &mut Map<String, Value>| {
            super::sync_jsonl_incremental(
                &conn,
                state,
                "claude",
                &path,
                super::parse_claude_line,
                &mut |_| {},
            )
            .unwrap()
        };

        assert_eq!(sync(&mut state), 1);
        fs::write(&path, replacement).unwrap();
        assert_eq!(sync(&mut state), 1, "changed prefix must reset the cursor");
        assert_eq!(sync(&mut state), 0);
    }

    #[test]
    fn an_active_scan_revalidates_its_starting_prefix_before_checkpointing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.jsonl");
        let original = concat!(r#"{"display":"original","timestamp":1}"#, "\n");
        fs::write(&path, original).unwrap();
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        super::sync_jsonl_incremental(
            &conn,
            &mut state,
            "claude",
            &path,
            super::parse_claude_line,
            &mut |_| {},
        )
        .unwrap();

        let starting_offset = saved_cursor_offset(&state["claude"]);
        let mut reader = super::CompleteJsonlReader::open(&path, state.get("claude")).unwrap();
        let replacement = concat!(r#"{"display":"replaced","timestamp":2}"#, "\n");
        fs::write(&path, replacement).unwrap();
        let cursor = reader.committed_cursor(starting_offset, true).unwrap();
        assert_eq!(cursor.offset, 0);
        assert_ne!(cursor.generation, reader.cursor.generation);
    }

    #[test]
    fn generic_jsonl_consumes_complete_data_appended_during_a_scan() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.jsonl");
        let initial: String = (0..super::JSONL_CHUNK_LINES)
            .map(|i| format!(r#"{{"display":"prompt {i}","timestamp":{i}}}"#) + "\n")
            .collect();
        fs::write(&path, initial).unwrap();
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        let mut appended = false;
        let inserted = super::sync_jsonl_incremental(
            &conn,
            &mut state,
            "claude",
            &path,
            super::parse_claude_line,
            &mut |_| {
                if !appended {
                    let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
                    writeln!(file, r#"{{"display":"appended","timestamp":999999}}"#).unwrap();
                    appended = true;
                }
            },
        )
        .unwrap();
        assert_eq!(inserted, super::JSONL_CHUNK_LINES + 1);
        assert_eq!(
            saved_cursor_offset(&state["claude"]),
            fs::metadata(&path).unwrap().len()
        );
    }

    #[test]
    fn legacy_numeric_cursor_rescans_safely_before_upgrading() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.jsonl");
        let first = concat!(r#"{"display":"already imported","timestamp":1}"#, "\n");
        let second = concat!(r#"{"display":"new record","timestamp":2}"#, "\n");
        fs::write(&path, format!("{first}{second}")).unwrap();
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        state.insert("claude".into(), json!(first.len() as u64));

        assert_eq!(
            super::sync_jsonl_incremental(
                &conn,
                &mut state,
                "claude",
                &path,
                super::parse_claude_line,
                &mut |_| {},
            )
            .unwrap(),
            2
        );
        assert!(matches!(
            super::FileCursor::decode(&state["claude"]),
            Some(super::DecodedFileCursor::Typed(_))
        ));
        assert_eq!(
            saved_cursor_offset(&state["claude"]),
            fs::metadata(&path).unwrap().len()
        );
    }

    #[test]
    fn cursor_sync_allows_another_writer_while_preparing_later_transcripts() {
        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("P/agent-transcripts/s1/s1.jsonl");
        let second = dir.path().join("P/agent-transcripts/s2/s2.jsonl");
        for (path, prompt) in [(&first, "first pending prompt"), (&second, "second prompt")] {
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(
                path,
                format!("{}\n", json!({"role":"user","message":{"content":prompt}})),
            )
            .unwrap();
        }
        let db = dir.path().join("history.db");
        let conn = Connection::open(&db).unwrap();
        init_db(&conn).unwrap();
        conn.execute_batch("CREATE TABLE writer_probe (value INTEGER);")
            .unwrap();
        let competitor = Connection::open(&db).unwrap();
        competitor.busy_timeout(Duration::ZERO).unwrap();
        let mut visited = Vec::new();
        let mut competing_write = None;
        let mut state = Map::new();
        assert_eq!(
            super::sync_cursor_with_scan_hook(&conn, &mut state, dir.path(), &mut |path| {
                visited.push(path.to_path_buf());
                if path == second {
                    competing_write =
                        Some(competitor.execute("INSERT INTO writer_probe VALUES (1)", []));
                }
            })
            .unwrap(),
            2
        );
        assert_eq!(visited, vec![first.clone(), second.clone()]);
        assert_eq!(
            competing_write
                .expect("the later transcript must be reached")
                .unwrap(),
            1,
            "preparation must leave the destination available to another writer"
        );
        for path in [&first, &second] {
            assert_eq!(
                saved_cursor_offset(&state["cursor"][path.to_string_lossy().as_ref()]),
                fs::metadata(path).unwrap().len()
            );
        }
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM history", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            2
        );
    }

    #[test]
    fn cursor_sync_recovers_when_a_transcript_vanishes_during_preparation() {
        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("P/agent-transcripts/s1/s1.jsonl");
        let second = dir.path().join("P/agent-transcripts/s2/s2.jsonl");
        fs::create_dir_all(first.parent().unwrap()).unwrap();
        fs::write(
            &first,
            concat!(r#"{"role":"user","message":{"content":"seed"}}"#, "\n"),
        )
        .unwrap();
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        assert_eq!(
            super::sync_cursor(&conn, &mut state, dir.path()).unwrap(),
            1
        );
        let saved = state.clone();
        let seed_timestamp: i64 = conn
            .query_row("SELECT timestamp_ms FROM history", [], |row| row.get(0))
            .unwrap();

        let mut file = fs::OpenOptions::new().append(true).open(&first).unwrap();
        writeln!(
            file,
            r#"{{"role":"user","message":{{"content":"pending prompt"}}}}"#
        )
        .unwrap();
        drop(file);
        fs::create_dir_all(second.parent().unwrap()).unwrap();
        fs::write(
            &second,
            concat!(
                r#"{"role":"user","message":{"content":"later prompt"}}"#,
                "\n"
            ),
        )
        .unwrap();
        let mut visited = Vec::new();
        let inserted =
            super::sync_cursor_with_scan_hook(&conn, &mut state, dir.path(), &mut |path| {
                visited.push(path.to_path_buf());
                if path == second {
                    assert_eq!(
                        conn.query_row("SELECT COUNT(*) FROM history", [], |row| row
                            .get::<_, i64>(0))
                            .unwrap(),
                        1,
                        "the earlier transcript must still be pending during preparation"
                    );
                    // The hook runs after the existence check, forcing open to fail.
                    fs::remove_file(path).unwrap();
                }
            })
            .expect("one vanished transcript must not abort the whole Cursor source");
        assert_eq!(visited, vec![first.clone(), second.clone()]);
        assert_eq!(
            inserted, 1,
            "the surviving transcript must still be ingested"
        );
        let rows: Vec<(String, i64)> = conn
            .prepare("SELECT prompt, timestamp_ms FROM history ORDER BY id")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            rows.iter().map(|(p, _)| p.as_str()).collect::<Vec<_>>(),
            vec!["seed", "pending prompt"],
            "the readable transcript must be ingested despite its neighbour vanishing"
        );
        assert_eq!(rows[0].1, seed_timestamp);
        // The surviving file advances; the vanished file keeps whatever checkpoint it
        // had, so the next sync retries it from the same offset.
        assert_eq!(
            saved_cursor_offset(&state["cursor"][first.to_string_lossy().as_ref()]),
            fs::metadata(&first).unwrap().len(),
            "the readable transcript's checkpoint must advance"
        );
        assert_eq!(
            state["cursor"].get(second.to_string_lossy().as_ref()),
            saved["cursor"].get(second.to_string_lossy().as_ref()),
            "the vanished transcript's checkpoint must be left untouched"
        );
    }

    #[test]
    fn cursor_sync_preserves_checkpoint_after_failed_write_and_retries() {
        let dir = tempfile::tempdir().unwrap();
        let cursor = dir.path().join("P/agent-transcripts/s1/s1.jsonl");
        fs::create_dir_all(cursor.parent().unwrap()).unwrap();
        let seed = concat!(r#"{"role":"user","message":{"content":"seed"}}"#, "\n");
        fs::write(&cursor, seed).unwrap();
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        assert_eq!(
            super::sync_cursor(&conn, &mut state, dir.path()).unwrap(),
            1
        );
        assert_eq!(
            saved_cursor_offset(&state["cursor"][cursor.to_string_lossy().as_ref()]),
            seed.len() as u64
        );
        let saved = state.clone();

        let mut file = fs::OpenOptions::new().append(true).open(&cursor).unwrap();
        for prompt in ["before failure", "rejected", "after failure"] {
            writeln!(
                file,
                "{}",
                json!({"role":"user","message":{"content":prompt}})
            )
            .unwrap();
        }
        drop(file);
        conn.execute_batch(
            "CREATE TRIGGER reject_cursor_prompt BEFORE INSERT ON history \
             WHEN NEW.source = 'cursor' AND NEW.prompt = 'rejected' \
             BEGIN SELECT RAISE(FAIL, 'forced Cursor write failure'); END;",
        )
        .unwrap();

        let error = super::sync_cursor(&conn, &mut state, dir.path())
            .expect_err("a failed database write must fail Cursor sync");
        assert!(format!("{error:#}").contains("forced Cursor write failure"));
        assert_eq!(
            state, saved,
            "failed sync must preserve the entire checkpoint"
        );
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM history", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            1,
            "failed sync must roll back new prompts and preserve the seed"
        );

        let failed_metadata = fs::metadata(&cursor).unwrap();
        let mut file = fs::OpenOptions::new().append(true).open(&cursor).unwrap();
        writeln!(
            file,
            r#"{{"role":"user","message":{{"content":"appended before retry"}}}}"#
        )
        .unwrap();
        file.set_times(
            fs::FileTimes::new()
                .set_modified(failed_metadata.modified().unwrap() + Duration::from_secs(60)),
        )
        .unwrap();
        drop(file);
        assert_ne!(
            super::modified_ms_of(&failed_metadata).unwrap(),
            super::modified_ms_of(&fs::metadata(&cursor).unwrap()).unwrap(),
            "retry must use a different timestamp in the history uniqueness key"
        );

        conn.execute_batch("DROP TRIGGER reject_cursor_prompt;")
            .unwrap();
        assert_eq!(
            super::sync_cursor(&conn, &mut state, dir.path()).unwrap(),
            4
        );
        assert_eq!(
            saved_cursor_offset(&state["cursor"][cursor.to_string_lossy().as_ref()]),
            fs::metadata(&cursor).unwrap().len()
        );
        assert_eq!(
            super::sync_cursor(&conn, &mut state, dir.path()).unwrap(),
            0
        );
        let prompts: Vec<(String, i64)> = conn
            .prepare("SELECT prompt, COUNT(*) FROM history GROUP BY prompt ORDER BY prompt")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            prompts,
            vec![
                ("after failure".into(), 1),
                ("appended before retry".into(), 1),
                ("before failure".into(), 1),
                ("rejected".into(), 1),
                ("seed".into(), 1),
            ]
        );
    }

    #[test]
    fn cursor_sync_rolls_back_earlier_files_before_retrying_changed_transcripts() {
        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("P/agent-transcripts/s1/s1.jsonl");
        let second = dir.path().join("P/agent-transcripts/s2/s2.jsonl");
        for (path, prompts) in [
            (&first, vec!["first file"]),
            (
                &second,
                vec!["second before failure", "rejected", "second after failure"],
            ),
        ] {
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            let mut file = fs::File::create(path).unwrap();
            for prompt in prompts {
                writeln!(
                    file,
                    "{}",
                    json!({"role":"user","message":{"content":prompt}})
                )
                .unwrap();
            }
        }
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        conn.execute_batch(
            "CREATE TRIGGER reject_cursor_prompt BEFORE INSERT ON history \
             WHEN NEW.source = 'cursor' AND NEW.prompt = 'rejected' \
             BEGIN SELECT RAISE(FAIL, 'forced Cursor write failure'); END;",
        )
        .unwrap();
        let mut state = Map::new();
        let saved = state.clone();
        let error = super::sync_cursor(&conn, &mut state, dir.path())
            .expect_err("the later file must fail the entire source");
        assert!(format!("{error:#}").contains("forced Cursor write failure"));
        assert_eq!(state, saved, "first failed sync must not publish any state");
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM history", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            0,
            "writes from both the earlier file and the failing file must roll back"
        );
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM session_presences", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            0,
            "session presence writes must roll back with the evidence"
        );

        for (path, prompt) in [(&first, "first appended"), (&second, "second appended")] {
            let failed_metadata = fs::metadata(path).unwrap();
            let mut file = fs::OpenOptions::new().append(true).open(path).unwrap();
            writeln!(
                file,
                "{}",
                json!({"role":"user","message":{"content":prompt}})
            )
            .unwrap();
            file.set_times(
                fs::FileTimes::new()
                    .set_modified(failed_metadata.modified().unwrap() + Duration::from_secs(60)),
            )
            .unwrap();
            drop(file);
            assert_ne!(
                super::modified_ms_of(&failed_metadata).unwrap(),
                super::modified_ms_of(&fs::metadata(path).unwrap()).unwrap()
            );
        }
        conn.execute_batch("DROP TRIGGER reject_cursor_prompt;")
            .unwrap();
        assert_eq!(
            super::sync_cursor(&conn, &mut state, dir.path()).unwrap(),
            6
        );
        for path in [&first, &second] {
            assert_eq!(
                saved_cursor_offset(&state["cursor"][path.to_string_lossy().as_ref()]),
                fs::metadata(path).unwrap().len()
            );
        }
        assert_eq!(
            super::sync_cursor(&conn, &mut state, dir.path()).unwrap(),
            0
        );
        let prompts: Vec<(String, i64)> = conn
            .prepare("SELECT prompt, COUNT(*) FROM history GROUP BY prompt ORDER BY prompt")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            prompts,
            vec![
                ("first appended".into(), 1),
                ("first file".into(), 1),
                ("rejected".into(), 1),
                ("second after failure".into(), 1),
                ("second appended".into(), 1),
                ("second before failure".into(), 1),
            ]
        );
    }

    #[test]
    fn cursor_sync_consumes_malformed_and_non_user_rows_and_continues() {
        let dir = tempfile::tempdir().unwrap();
        let cursor = dir.path().join("P/agent-transcripts/s1/s1.jsonl");
        fs::create_dir_all(cursor.parent().unwrap()).unwrap();
        fs::write(
            &cursor,
            concat!(
                r#"{"role":"user","message":{"content":"first"}}"#,
                "\n",
                "{malformed JSON}\n",
                r#"{"role":"assistant","message":{"content":"assistant response"}}"#,
                "\n",
                r#"{"role":"system","message":{"content":"system message"}}"#,
                "\n",
                r#"{"role":"user","message":{"content":"second"}}"#,
                "\n",
            ),
        )
        .unwrap();
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        assert_eq!(
            super::sync_cursor(&conn, &mut state, dir.path()).unwrap(),
            2
        );
        assert_eq!(
            saved_cursor_offset(&state["cursor"][cursor.to_string_lossy().as_ref()]),
            fs::metadata(&cursor).unwrap().len()
        );
        assert_eq!(
            super::sync_cursor(&conn, &mut state, dir.path()).unwrap(),
            0
        );

        let mut file = fs::OpenOptions::new().append(true).open(&cursor).unwrap();
        writeln!(
            file,
            r#"{{"role":"user","message":{{"content":"continued"}}}}"#
        )
        .unwrap();
        drop(file);
        assert_eq!(
            super::sync_cursor(&conn, &mut state, dir.path()).unwrap(),
            1
        );
        assert_eq!(
            saved_cursor_offset(&state["cursor"][cursor.to_string_lossy().as_ref()]),
            fs::metadata(&cursor).unwrap().len()
        );
        assert_eq!(
            super::sync_cursor(&conn, &mut state, dir.path()).unwrap(),
            0
        );
        let prompts: Vec<String> = conn
            .prepare("SELECT prompt FROM history ORDER BY prompt")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(prompts, vec!["continued", "first", "second"]);
    }

    #[test]
    fn codex_and_cursor_sources_retry_unterminated_records() {
        let dir = tempfile::tempdir().unwrap();
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();

        let codex = dir.path().join(".codex/history.jsonl");
        fs::create_dir_all(codex.parent().unwrap()).unwrap();
        fs::write(&codex, r#"{"text":"codex"#).unwrap();
        assert_eq!(super::sync_codex(&conn, &mut state, dir.path()).unwrap(), 0);
        assert_eq!(saved_cursor_offset(&state["codex"]), 0);
        let mut file = fs::OpenOptions::new().append(true).open(&codex).unwrap();
        file.write_all(br#" prompt","ts":1,"session_id":"c1"}"#)
            .unwrap();
        file.write_all(b"\n").unwrap();
        drop(file);
        assert_eq!(super::sync_codex(&conn, &mut state, dir.path()).unwrap(), 1);

        let cursor_root = dir.path().join(".cursor/projects");
        let cursor = cursor_root.join("P/agent-transcripts/s1/s1.jsonl");
        fs::create_dir_all(cursor.parent().unwrap()).unwrap();
        fs::write(&cursor, r#"{"role":"user","message":{"content":"cursor"#).unwrap();
        assert_eq!(
            super::sync_cursor(&conn, &mut state, &cursor_root).unwrap(),
            0
        );
        assert_eq!(
            saved_cursor_offset(&state["cursor"][cursor.to_string_lossy().as_ref()]),
            0
        );
        let mut file = fs::OpenOptions::new().append(true).open(&cursor).unwrap();
        file.write_all(br#" prompt"}}"#).unwrap();
        file.write_all(b"\n").unwrap();
        drop(file);
        assert_eq!(
            super::sync_cursor(&conn, &mut state, &cursor_root).unwrap(),
            1
        );
        assert_eq!(
            super::sync_cursor(&conn, &mut state, &cursor_root).unwrap(),
            0
        );
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM history", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            2
        );
    }

    #[test]
    fn checkpoints_persist_between_sources_and_never_abort_a_run() {
        let dir = std::env::temp_dir().join(format!("ai-hist-checkpoint-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(".sync-state.json");

        // A cursor saved mid-run is visible to the next run. This is what stops
        // an interrupted sync from re-scanning what it already finished.
        let mut state = Map::new();
        state.insert("claude".into(), json!({"offset": 147_624_483u64}));
        checkpoint_sync_state(&path, &state);
        assert_eq!(load_sync_state(&path).unwrap(), state);

        // A later source advances state; its checkpoint supersedes the earlier one.
        state.insert("codex".into(), json!({"files": 3}));
        checkpoint_sync_state(&path, &state);
        assert_eq!(load_sync_state(&path).unwrap(), state);

        // An unwritable destination warns instead of unwinding -- the rows are
        // already committed, so a failed bookkeeping write must not fail the run.
        let blocker = dir.join("blocker");
        fs::write(&blocker, "not a directory").unwrap();
        checkpoint_sync_state(&blocker.join("nested").join(".sync-state.json"), &state);

        // ...and the last good checkpoint is left untouched by that failure.
        assert_eq!(load_sync_state(&path).unwrap(), state);
        assert_eq!(leftover_tmp_files(&dir), Vec::<String>::new());

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn concurrent_saves_never_publish_a_torn_state_file() {
        let dir =
            std::env::temp_dir().join(format!("ai-hist-state-concurrent-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(".sync-state.json");

        // Each writer stages a differently-sized payload, so a shared temp path
        // would interleave into something that either fails to parse or blends
        // two writers' bytes. Every save must publish exactly one writer's state.
        let threads: Vec<_> = (0..8)
            .map(|i| {
                let path = path.clone();
                std::thread::spawn(move || {
                    let mut state = Map::new();
                    state.insert("writer".into(), json!(i));
                    state.insert("padding".into(), json!("x".repeat(i * 4096)));
                    save_sync_state(&path, &state).unwrap();
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }

        // Whoever renamed last wins, but the winner must be intact and whole.
        let published = load_sync_state(&path).unwrap();
        let writer = published.get("writer").and_then(Value::as_u64).unwrap();
        let padding = published.get("padding").and_then(Value::as_str).unwrap();
        assert_eq!(padding.len(), writer as usize * 4096);
        assert_eq!(leftover_tmp_files(&dir), Vec::<String>::new());

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn concurrent_checkpoints_preserve_every_writers_keys() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".sync-state.json");
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(16));
        let threads: Vec<_> = (0..16)
            .map(|writer| {
                let path = path.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let mut state = Map::new();
                    state.insert(format!("writer-{writer}"), json!(writer));
                    barrier.wait();
                    checkpoint_sync_state(&path, &state);
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }

        let published = load_sync_state(&path).unwrap();
        for writer in 0..16 {
            assert_eq!(
                published.get(&format!("writer-{writer}")),
                Some(&json!(writer))
            );
        }
    }

    #[test]
    fn sync_state_lock_contention_is_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".sync-state.json");
        let _holder = super::SyncStateLock::acquire(&path).unwrap();
        assert!(
            super::SyncStateLock::acquire_with_timeout(&path, std::time::Duration::ZERO).is_err()
        );
    }

    #[test]
    fn parses_compacted_rollup_instead_of_skipping_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("compact_fixture.json");
        fs::write(
            &path,
            r#"{
                "id":"compact_fixture",
                "type":"compacted",
                "version":1,
                "sourceTrajectories":["traj_a"],
                "compactedAt":"2026-06-21T10:00:00.000Z",
                "decisions":[{"question":"Which DB?","chosen":"Neon","reasoning":"pgvector","impact":"rank Pair warnings"}],
                "lessons":[{"context":"Deploy","lesson":"Scrub snippets","recommendation":"Redact ghp_FAKE0000000000000000000000000000abcd"}],
                "keyFindings":["kind in PK"],
                "narrative":"Compacted roll-up captured durable guidance."
            }"#,
        )
        .unwrap();

        let row = parse_trajectory_file(&path).unwrap().unwrap();
        assert_eq!(row.id, "compact_fixture");
        assert_eq!(row.version, Some(1));
        assert!(row.retrospective_json.contains(r#""type":"compacted""#));
        assert!(row.search_text.contains("kind in PK"));
        assert!(row.search_text.contains("Redact ghp_FAKE"));
        assert_eq!(row.timestamp_ms, 1_782_036_000_000);
    }

    #[test]
    fn ingests_claude_transcript_events_tools_edits_and_searches_agent_text() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sess-rich.jsonl");
        write_rich_claude_transcript(&path);

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        ingest_claude_transcript(&conn, &path).unwrap();

        let event_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM session_events", [], |row| row.get(0))
            .unwrap();
        let tool_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM tool_calls", [], |row| row.get(0))
            .unwrap();
        let edit = conn
            .query_row(
                "SELECT file_path, lines_added, lines_removed, user_modified, git_branch, cwd FROM file_edits",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                    ))
                },
            )
            .unwrap();

        assert_eq!(event_count, 4);
        assert_eq!(tool_count, 1);
        assert_eq!(
            edit,
            (
                "/tmp/proj/auth.ts".to_string(),
                1,
                1,
                1,
                "feat/rich".to_string(),
                "/tmp/proj".to_string()
            )
        );

        let rows = search_all(
            &conn,
            &["update".to_string()],
            false,
            &QueryFilter {
                limit: 10,
                ..Default::default()
            },
            SearchRole::Assistant,
        )
        .unwrap();
        assert!(rows.iter().any(|row| {
            row.match_source == "session_event"
                && row.role == "assistant"
                && row.text.contains("I will update auth.ts")
        }));
    }

    #[test]
    fn malformed_raw_fts_query_has_a_friendly_error() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();

        let error = search_all(
            &conn,
            &["parity-check".to_string()],
            true,
            &QueryFilter {
                limit: 10,
                ..Default::default()
            },
            SearchRole::All,
        )
        .expect_err("malformed raw FTS query should fail");
        assert_friendly_fts_error(&error.to_string());

        // event-row branch: history lookup succeeds, the event search still parses raw
        let assistant_error = search_all(
            &conn,
            &["parity-check".to_string()],
            true,
            &QueryFilter {
                limit: 10,
                ..Default::default()
            },
            SearchRole::Assistant,
        )
        .expect_err("malformed raw FTS query should fail for assistant role");
        assert_friendly_fts_error(&assistant_error.to_string());

        // core search invoked directly with raw_fts enabled
        let core_error = crate::search(
            &conn,
            &["parity-check".to_string()],
            true,
            &QueryFilter {
                limit: 10,
                ..Default::default()
            },
        )
        .expect_err("malformed raw FTS query should fail in core search");
        assert_friendly_fts_error(&core_error.to_string());
    }

    fn assert_friendly_fts_error(message: &str) {
        assert!(
            message.contains("Invalid raw FTS5 MATCH expression"),
            "got: {message}"
        );
        assert!(message.contains("Quote literal terms"), "got: {message}");
        assert!(!message.contains("no such column"), "got: {message}");
        assert!(
            !message.contains("SQL error or missing database"),
            "got: {message}"
        );
    }

    #[test]
    fn sync_backfills_transcript_events_when_existing_stamp_has_no_events() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sess-rich.jsonl");
        write_rich_claude_transcript(&path);

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();

        let mut claude_sessions = Map::new();
        claude_sessions.insert(
            path.to_string_lossy().to_string(),
            json!(file_stamp(&path).unwrap()),
        );
        let mut state = Map::new();
        state.insert(
            "claude_sessions_v3".to_string(),
            Value::Object(claude_sessions),
        );

        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();

        let event_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM session_events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(event_count, 4);
    }

    /// One Claude project tree as the provider writes it: a parent transcript,
    /// a subagent sidecar the provider named with an in-record `agentId` (with
    /// its `meta.json` beside it), and a sidechain sidecar from a provider
    /// version that names no child at all. Returns the three transcripts.
    fn write_claude_parent_with_subagents(
        root: &std::path::Path,
    ) -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
        let subagents = root.join("app/claude-root/subagents");
        fs::create_dir_all(&subagents).unwrap();
        let parent = root.join("app/claude-root.jsonl");
        fs::write(
            &parent,
            concat!(
                r#"{"sessionId":"claude-root","uuid":"u1","cwd":"/work/app","type":"user","message":{"role":"user","content":"human prompt"},"timestamp":"2026-08-31T11:00:00Z"}"#, "\n",
                r#"{"sessionId":"claude-root","uuid":"a1","cwd":"/work/app","type":"assistant","message":{"role":"assistant","content":[{"type":"tool_use","id":"toolu_1","name":"Agent","input":{"prompt":"plan it"}}]},"timestamp":"2026-08-31T11:00:01Z"}"#, "\n",
            ),
        )
        .unwrap();
        let named = subagents.join("agent-abc.jsonl");
        fs::write(
            &named,
            concat!(
                r#"{"sessionId":"claude-root","agentId":"abc","isSidechain":true,"uuid":"side-u","cwd":"/work/app","type":"user","message":{"role":"user","content":"delegated instruction"},"timestamp":"2026-08-31T11:00:02Z"}"#, "\n",
                r#"{"sessionId":"claude-root","agentId":"abc","isSidechain":true,"uuid":"side-a","cwd":"/work/app","type":"assistant","message":{"role":"assistant","content":"child result"},"timestamp":"2026-08-31T11:00:03Z"}"#, "\n",
            ),
        )
        .unwrap();
        fs::write(
            subagents.join("agent-abc.meta.json"),
            r#"{"agentType":"Plan","description":"plan the work","toolUseId":"toolu_1","spawnDepth":1,"model":"test-model"}"#,
        )
        .unwrap();
        let nameless = subagents.join("agent-nameless.jsonl");
        fs::write(
            &nameless,
            concat!(
                r#"{"sessionId":"claude-root","isSidechain":true,"uuid":"other-u","cwd":"/work/app","type":"user","message":{"role":"user","content":"second instruction"},"timestamp":"2026-08-31T11:00:04Z"}"#, "\n",
                r#"{"sessionId":"claude-root","isSidechain":true,"uuid":"other-a","cwd":"/work/app","type":"assistant","message":{"role":"assistant","content":"unnamed result"},"timestamp":"2026-08-31T11:00:05Z"}"#, "\n",
            ),
        )
        .unwrap();
        (parent, named, nameless)
    }

    struct DelegationRow {
        parent: String,
        child: Option<String>,
        identity_status: String,
        agent_type: Option<String>,
        agent_name: Option<String>,
        model: Option<String>,
        spawn_depth: Option<i64>,
        evidence_kind: String,
        evidence_locator: Option<String>,
        evidence_ref: Option<String>,
        child_has_events: bool,
        spawned_at_ms: Option<i64>,
        created_ms: i64,
    }

    fn delegation_row(conn: &Connection, evidence_kind: &str) -> DelegationRow {
        conn.query_row(
            "SELECT parent_session_id, child_session_id, identity_status, child_agent_type, \
                    child_agent_name, child_model, spawn_depth, evidence_kind, evidence_locator, \
                    evidence_ref, child_has_events, spawned_at_ms, created_ms \
             FROM session_relationships WHERE source = 'claude' AND evidence_kind = ?",
            [evidence_kind],
            |row| {
                Ok(DelegationRow {
                    parent: row.get(0)?,
                    child: row.get(1)?,
                    identity_status: row.get(2)?,
                    agent_type: row.get(3)?,
                    agent_name: row.get(4)?,
                    model: row.get(5)?,
                    spawn_depth: row.get(6)?,
                    evidence_kind: row.get(7)?,
                    evidence_locator: row.get(8)?,
                    evidence_ref: row.get(9)?,
                    child_has_events: row.get(10)?,
                    spawned_at_ms: row.get(11)?,
                    created_ms: row.get(12)?,
                })
            },
        )
        .unwrap()
    }

    #[test]
    fn reattribution_heals_only_unambiguous_legacy_positional_rows() {
        let dir = tempfile::tempdir().unwrap();
        let transcript = dir.path().join("side.jsonl");
        // No uuid and no message.id anywhere: every record's identity is the
        // content hash. Timestamps are shared with the staged legacy rows so
        // the (text, timestamp) match is exact.
        fs::write(
            &transcript,
            concat!(
                "{\"sessionId\":\"parent\",\"type\":\"assistant\",\"message\":{\"role\":\"assistant\",\"model\":\"opus\",\"content\":\"delegated work\",\"usage\":{\"input_tokens\":3,\"output_tokens\":5}},\"timestamp\":\"2026-09-18T10:00:00Z\"}\n",
                "{\"sessionId\":\"parent\",\"type\":\"assistant\",\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"tool_use\",\"id\":\"toolu_9\",\"name\":\"Read\",\"input\":{\"file_path\":\"/work/app/notes.txt\"}}]},\"timestamp\":\"2026-09-18T10:00:01Z\"}\n",
                "{\"sessionId\":\"parent\",\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"same words\"},\"timestamp\":\"2026-09-18T10:00:02Z\"}\n",
                "{\"sessionId\":\"parent\",\"type\":\"assistant\",\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"analysis\"},{\"type\":\"text\",\"text\":\"new answer\"}]},\"timestamp\":\"2026-09-18T10:00:03Z\"}\n",
                "{\"sessionId\":\"parent\",\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"memo\"},{\"type\":\"tool_result\",\"tool_use_id\":\"toolu_7\",\"content\":\"\"}]},\"timestamp\":\"2026-09-18T10:00:04Z\"}\n",
            ),
        )
        .unwrap();
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let ts = |iso: &str| super::parse_iso_ms(iso).unwrap();
        let token_json =
            serde_json::to_string(&serde_json::json!({"input_tokens": 3, "output_tokens": 5}))
                .unwrap();
        // Unique full-record match, model and token spend included: healed.
        legacy_event(
            &conn,
            "parent",
            "side:0",
            "delegated work",
            ts("2026-09-18T10:00:00Z"),
            0,
            "assistant",
            "text",
            Some("opus"),
            Some(&token_json),
        );
        // Same text, timestamp, role and kind, different model: a changed
        // record whose predecessor stays retained.
        legacy_event(
            &conn,
            "parent",
            "side:12",
            "delegated work",
            ts("2026-09-18T10:00:00Z"),
            0,
            "assistant",
            "text",
            Some("opus-old"),
            Some(&token_json),
        );
        // Same text and timestamp, different role: a changed record whose
        // predecessor stays retained.
        legacy_event(
            &conn,
            "parent",
            "side:7",
            "delegated work",
            ts("2026-09-18T10:00:00Z"),
            0,
            "user",
            "text",
            None,
            None,
        );
        // Same text and role, different timestamp: not the same record.
        legacy_event(
            &conn,
            "parent",
            "side:4",
            "delegated work",
            1,
            0,
            "assistant",
            "text",
            None,
            None,
        );
        // Twice-stored duplicate: ambiguous, both preserved.
        legacy_event(
            &conn,
            "parent",
            "side:5",
            "same words",
            ts("2026-09-18T10:00:02Z"),
            0,
            "user",
            "text",
            None,
            None,
        );
        legacy_event(
            &conn,
            "parent",
            "side:6",
            "same words",
            ts("2026-09-18T10:00:02Z"),
            0,
            "user",
            "text",
            None,
            None,
        );
        // One shared block with a changed sibling: the record is a new
        // identity whose predecessor stays retained, so both stay.
        let partial_ts = ts("2026-09-18T10:00:03Z");
        legacy_event(
            &conn,
            "parent",
            "side:8",
            "analysis",
            partial_ts,
            0,
            "assistant",
            "text",
            None,
            None,
        );
        legacy_event(
            &conn,
            "parent",
            "side:8",
            "old answer",
            partial_ts,
            1,
            "assistant",
            "text",
            None,
            None,
        );
        // The tool use text is shared with another message, so the event
        // match stays ambiguous and the event is preserved — but the tool
        // use id names its own call row, which heals.
        let tool_ts = ts("2026-09-18T10:00:01Z");
        legacy_event(
            &conn,
            "parent",
            "side:1",
            "Read /work/app/notes.txt",
            tool_ts,
            0,
            "assistant",
            "tool_use",
            None,
            None,
        );
        legacy_event(
            &conn,
            "parent",
            "side:9",
            "Read /work/app/notes.txt",
            tool_ts,
            0,
            "assistant",
            "tool_use",
            None,
            None,
        );
        // An empty tool result writes a null-text event; the null is part
        // of the record key, so the unchanged record still heals exactly.
        let null_ts = ts("2026-09-18T10:00:04Z");
        legacy_event(
            &conn, "parent", "side:10", "memo", null_ts, 0, "user", "text", None, None,
        );
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, message_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 'parent', 'side:10', ?1, 'tool_result', 'tool_result', NULL, 'side:10:1')",
            [null_ts],
        )
        .unwrap();
        // Referenced tool call: healed by tool use id (unique per session).
        conn.execute(
            "INSERT INTO tool_calls (source, session_id, message_id, tool_use_id, name) \
             VALUES ('claude', 'parent', 'side:1', 'toolu_9', 'Read')",
            [],
        )
        .unwrap();
        // Unreferenced tool call: preserved.
        conn.execute(
            "INSERT INTO tool_calls (source, session_id, message_id, tool_use_id, name) \
             VALUES ('claude', 'parent', 'side:3', 'toolu_old', 'Read')",
            [],
        )
        .unwrap();

        ingest_claude_transcript_as(&conn, &transcript, Some("child")).unwrap();

        let remaining: Vec<(String, String)> = conn
            .prepare(
                "SELECT message_id, text FROM session_events \
                 WHERE source = 'claude' AND session_id = 'parent' ORDER BY message_id, text",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(
            remaining,
            [
                ("side:1".to_string(), "Read /work/app/notes.txt".to_string()),
                ("side:12".to_string(), "delegated work".to_string()),
                ("side:4".to_string(), "delegated work".to_string()),
                ("side:5".to_string(), "same words".to_string()),
                ("side:6".to_string(), "same words".to_string()),
                ("side:7".to_string(), "delegated work".to_string()),
                ("side:8".to_string(), "analysis".to_string()),
                ("side:8".to_string(), "old answer".to_string()),
                ("side:9".to_string(), "Read /work/app/notes.txt".to_string()),
            ],
            "only uniquely matched legacy messages heal"
        );
        let calls: Vec<String> = conn
            .prepare(
                "SELECT tool_use_id FROM tool_calls WHERE source = 'claude' AND session_id = 'parent'",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(calls, ["toolu_old".to_string()]);
        // The healed records land under the child with hash identities.
        let healed: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_events WHERE source = 'claude' AND session_id = 'child'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(healed, 7);
        let sample_uid: String = conn
            .query_row(
                "SELECT event_uid FROM session_events WHERE source = 'claude' AND session_id = 'child' \
                 AND text = 'delegated work'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            sample_uid.starts_with("side:sha256:"),
            "the healed row carries the namespaced hash identity: {sample_uid}"
        );
        let positional_under_child: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_events WHERE source = 'claude' AND session_id = 'child' \
                 AND (event_uid = 'side:0:0' OR event_uid LIKE 'side:1:%' OR event_uid LIKE 'side:2:%' \
                      OR event_uid LIKE 'side:3:%' OR event_uid LIKE 'side:4:%')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(positional_under_child, 0);
    }

    #[test]
    fn byte_identical_id_less_records_share_one_content_identity() {
        // Two provider records with identical bytes and no identity collapse
        // onto one event by design: an ordinal would be positional identity
        // by another name, and a dropped prefix would shift every survivor
        // onto an earlier row's identity. Documented in architecture.md.
        let dir = tempfile::tempdir().unwrap();
        let transcript = dir.path().join("dupes.jsonl");
        let line = "{\"sessionId\":\"dupes\",\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"same\"},\"timestamp\":\"2026-09-18T10:00:00Z\"}\n";
        fs::write(&transcript, format!("{line}{line}")).unwrap();
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();
        let events: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_events WHERE source = 'claude' AND session_id = 'dupes'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(events, 1);
    }

    fn legacy_event(
        conn: &Connection,
        session_id: &str,
        message_id: &str,
        text: &str,
        ts_ms: i64,
        block: i64,
        role: &str,
        kind: &str,
        model: Option<&str>,
        token_json: Option<&str>,
    ) {
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, message_id, ts_ms, role, kind, text, event_uid, model, token_json) \
             VALUES ('claude', ?1, ?2, ?3, ?6, ?7, ?4, ?2 || ':' || ?5, ?8, ?9)",
            rusqlite::params![
                session_id,
                message_id,
                ts_ms,
                text,
                block,
                role,
                kind,
                model,
                token_json
            ],
        )
        .unwrap();
    }

    #[test]
    fn local_first_sync_reconciles_a_later_remote_presence_by_exact_trimmed_id() {
        let dir = tempfile::tempdir().unwrap();
        let transcript = dir.path().join(".claude/projects/app/local.jsonl");
        fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        fs::write(
            &transcript,
            concat!(
                r#"{"sessionId":"local-materialized","remoteSessionId":7,"remote_session_id":"  session_01remote  ","uuid":"u1","type":"user","timestamp":1,"message":{"role":"user","content":"hello"}}"#,
                "\n"
            ),
        )
        .unwrap();
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();
        let cached: (String, String) = conn
            .query_row(
                "SELECT local_session_id, remote_session_id \
                 FROM session_identity_correlations WHERE source = 'claude'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            cached,
            ("local-materialized".into(), "session_01remote".into())
        );
        let before: i64 = conn
            .query_row("SELECT COUNT(*) FROM session_relationships", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(before, 0);
        conn.execute(
            "INSERT INTO sessions (source, session_id, discovery_state) \
             VALUES ('claude', 'session_01remote', 'shallow')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_presences \
             (source, session_id, location, raw_locator, discovery_state) \
             VALUES ('claude', 'session_01remote', 'remote', 'session_01remote', 'shallow')",
            [],
        )
        .unwrap();

        // Remote reconciliation must use the observed identity cache. The
        // transcript may have disappeared between local and remote syncs.
        fs::remove_file(&transcript).unwrap();

        reconcile_claude_remote_relationships(&conn).unwrap();
        let relationship: (String, String, String) = conn
            .query_row(
                "SELECT parent_session_id, child_session_id, relationship \
                 FROM session_relationships WHERE evidence_kind='claude_remote_session_id'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            relationship,
            (
                "session_01remote".into(),
                "local-materialized".into(),
                "materialized_local".into()
            )
        );
    }

    #[test]
    fn plain_claude_sync_records_a_named_subagent_as_an_observed_child() {
        let dir = tempfile::tempdir().unwrap();
        let (_, named, _) = write_claude_parent_with_subagents(dir.path());
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();

        // No hydration anywhere: a plain sync is the only thing that has ever
        // read these files.
        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();

        let row = delegation_row(&conn, "claude_subagent_meta");
        assert_eq!(row.parent, "claude-root");
        assert_eq!(row.child.as_deref(), Some("abc"));
        assert_eq!(row.identity_status, "observed");
        assert_eq!(row.agent_type.as_deref(), Some("Plan"));
        assert_eq!(row.agent_name.as_deref(), Some("plan the work"));
        assert_eq!(row.model.as_deref(), Some("test-model"));
        assert_eq!(row.spawn_depth, Some(1));
        assert_eq!(row.evidence_kind, "claude_subagent_meta");
        assert_eq!(
            row.evidence_locator.as_deref(),
            Some(named.to_string_lossy().as_ref())
        );
        assert_eq!(row.evidence_ref.as_deref(), Some("toolu_1"));
        assert!(row.child_has_events);
        assert_eq!(
            row.spawned_at_ms,
            super::parse_iso_ms("2026-08-31T11:00:02Z")
        );

        // The child's output is addressable under the child, its delegated
        // instruction is nobody's human prompt, and a delegated thread never
        // becomes a session of its own.
        let counts: (i64, i64, i64, i64) = conn
            .query_row(
                "SELECT \
                   (SELECT COUNT(*) FROM session_events WHERE source='claude' AND session_id='abc'), \
                   (SELECT COUNT(*) FROM history WHERE source='claude' AND session_id='abc'), \
                   (SELECT COUNT(*) FROM sessions WHERE source='claude' AND session_id='abc'), \
                   (SELECT COUNT(*) FROM session_events WHERE source='claude' AND session_id='claude-root' AND event_uid LIKE 'side-%')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(counts, (1, 0, 0, 0));
    }

    #[test]
    fn plain_claude_sync_records_a_nameless_sidechain_as_unlinked_evidence() {
        let dir = tempfile::tempdir().unwrap();
        let (_, _, nameless) = write_claude_parent_with_subagents(dir.path());
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();

        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();

        let row = delegation_row(&conn, "claude_sidechain_records");
        assert_eq!(row.parent, "claude-root");
        assert_eq!(row.child, None);
        assert_eq!(row.identity_status, "unlinked");
        assert!(!row.child_has_events);
        assert_eq!(
            row.evidence_locator.as_deref(),
            Some(nameless.to_string_lossy().as_ref())
        );
        assert_eq!(
            row.spawned_at_ms,
            super::parse_iso_ms("2026-08-31T11:00:04Z")
        );
        // Nothing was invented from the file name, and the unnamed child's
        // output stays where it can still be addressed: on the parent.
        let placement: (i64, i64) = conn
            .query_row(
                "SELECT \
                   (SELECT COUNT(*) FROM session_relationships WHERE child_session_id = 'nameless'), \
                   (SELECT COUNT(*) FROM session_events WHERE source='claude' AND session_id='claude-root' AND event_uid = 'other-a:0')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(placement, (0, 1));
    }

    #[test]
    fn repeated_claude_sync_neither_duplicates_nor_ages_delegation_rows() {
        let dir = tempfile::tempdir().unwrap();
        write_claude_parent_with_subagents(dir.path());
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();

        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();
        let first = (
            delegation_row(&conn, "claude_subagent_meta").created_ms,
            delegation_row(&conn, "claude_sidechain_records").created_ms,
        );

        // The second walk sees unchanged stamps for every file.
        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM session_relationships", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(rows, 2);
        assert_eq!(
            (
                delegation_row(&conn, "claude_subagent_meta").created_ms,
                delegation_row(&conn, "claude_sidechain_records").created_ms,
            ),
            first
        );
        let child_events: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_events WHERE source='claude' AND session_id='abc'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(child_events, 1);
    }

    /// Reproduce what a release without the per-message raw facts left behind:
    /// the rows and the sync stamps are there, the columns the migration added
    /// are null, and the sync state carries no backfill generation because that
    /// release never wrote one. This is exactly the state an upgraded install
    /// is in on its first `sync`.
    fn blank_raw_message_facts_state(state: &mut Map<String, Value>) {
        state.remove(super::CLAUDE_RAW_MESSAGE_FACTS_KEY);
        state.remove(super::CODEX_RAW_MESSAGE_FACTS_KEY);
    }

    fn blank_raw_message_facts(conn: &Connection, source: &str) {
        conn.execute(
            "UPDATE session_events SET request_id = NULL, stop_reason = NULL, \
             agent_version = NULL, is_sidechain = NULL, is_meta = NULL, turn_id = NULL, \
             raw_facts_version = NULL WHERE source = ?",
            [source],
        )
        .unwrap();
    }

    #[test]
    fn plain_claude_sync_backfills_raw_facts_for_transcripts_indexed_before_them() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sess-facts.jsonl");
        fs::write(
            &path,
            concat!(
                r#"{"type":"user","uuid":"u1","sessionId":"sess-facts","cwd":"/tmp/project","isSidechain":false,"version":"2.1.96","timestamp":"2026-04-20T00:00:00.000Z","message":{"role":"user","content":"run it"}}"#, "\n",
                r#"{"type":"assistant","uuid":"a1","parentUuid":"u1","sessionId":"sess-facts","cwd":"/tmp/project","isSidechain":false,"requestId":"req_1","version":"2.1.96","timestamp":"2026-04-20T00:00:01.000Z","message":{"role":"assistant","model":"claude-opus-5","stop_reason":"end_turn","content":[{"type":"text","text":"done"}]}}"#, "\n",
            ),
        )
        .unwrap();

        let facts =
            |conn: &Connection| -> (Option<String>, Option<String>, Option<String>, Option<i64>) {
                conn.query_row(
                    "SELECT request_id, stop_reason, agent_version, is_sidechain \
                 FROM session_events WHERE source='claude' AND event_uid='a1:0'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .unwrap()
            };

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();
        assert_eq!(
            facts(&conn),
            (
                Some("req_1".into()),
                Some("end_turn".into()),
                Some("2.1.96".into()),
                Some(0)
            )
        );

        blank_raw_message_facts(&conn, "claude");
        blank_raw_message_facts_state(&mut state);
        // The stamp is unchanged and the events exist, so every other
        // condition on the fast path says "skip". Without the raw-facts check
        // this sync is a no-op and the columns stay null indefinitely while
        // the run still reports success.
        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();
        assert_eq!(
            facts(&conn),
            (
                Some("req_1".into()),
                Some("end_turn".into()),
                Some("2.1.96".into()),
                Some(0)
            )
        );

        // Repaired once, back on the fast path: a sentinel a re-read would
        // overwrite has to survive, or the transcript is being re-read on
        // every sync forever.
        conn.execute(
            "UPDATE session_events SET text = 'sentinel' WHERE source='claude' AND event_uid='a1:0'",
            [],
        )
        .unwrap();
        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();
        let text: Option<String> = conn
            .query_row(
                "SELECT text FROM session_events WHERE source='claude' AND event_uid='a1:0'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(text.as_deref(), Some("sentinel"));
    }

    /// A subagent sidecar has no `sessions` row of its own: the walk hands it
    /// to `ingest_claude_subagent` before the catalog upsert. A backfill that
    /// asks only `sessions.raw_path` therefore never selects one, and every
    /// sidecar's events keep null facts however many times `sync` runs.
    #[test]
    fn plain_claude_sync_backfills_raw_facts_for_subagent_sidecars() {
        let dir = tempfile::tempdir().unwrap();
        write_claude_parent_with_subagents(dir.path());
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();

        // The sidecar's assistant output lives under the child id the provider
        // named, which no `sessions.raw_path` points at.
        let child = |conn: &Connection| -> (Option<i64>, Option<i64>, Option<String>) {
            conn.query_row(
                "SELECT is_sidechain, raw_facts_version, text FROM session_events \
                 WHERE source='claude' AND session_id='abc' AND event_uid='side-a:0'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap()
        };
        assert_eq!(
            child(&conn),
            (
                Some(1),
                Some(super::RAW_MESSAGE_FACTS_VERSION),
                Some("child result".into())
            )
        );

        // The state an upgraded install is in: rows and stamps intact, facts
        // null, no backfill generation recorded. Neither file changes on disk.
        blank_raw_message_facts(&conn, "claude");
        blank_raw_message_facts_state(&mut state);
        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();
        assert_eq!(
            child(&conn),
            (
                Some(1),
                Some(super::RAW_MESSAGE_FACTS_VERSION),
                Some("child result".into())
            ),
            "an unchanged sidecar must be re-read by the one-time backfill pass"
        );

        // Marker recorded: the pass is over and the sidecar is back on the
        // fast path, so a sentinel a re-read would overwrite survives.
        conn.execute(
            "UPDATE session_events SET text = 'sentinel' \
             WHERE source='claude' AND session_id='abc' AND event_uid='side-a:0'",
            [],
        )
        .unwrap();
        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();
        assert_eq!(child(&conn).2.as_deref(), Some("sentinel"));
    }

    #[test]
    fn a_contributed_row_without_raw_facts_does_not_re_read_the_local_transcript_forever() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sess-shared.jsonl");
        fs::write(
            &path,
            concat!(
                r#"{"type":"user","uuid":"u1","sessionId":"sess-shared","cwd":"/tmp/project","isSidechain":false,"version":"2.1.96","timestamp":"2026-04-20T00:00:00.000Z","message":{"role":"user","content":"run it"}}"#, "\n",
                r#"{"type":"assistant","uuid":"a1","parentUuid":"u1","sessionId":"sess-shared","cwd":"/tmp/project","isSidechain":false,"requestId":"req_1","version":"2.1.96","timestamp":"2026-04-20T00:00:01.000Z","message":{"role":"assistant","model":"claude-opus-5","stop_reason":"end_turn","content":[{"type":"text","text":"done"}]}}"#, "\n",
            ),
        )
        .unwrap();

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();

        // A remote observation of the same session, contributed through the
        // source-adapter boundary. `session_events` is keyed by
        // `(source, session_id)`, so it lands beside the local rows -- and the
        // evidence spec does not carry `raw_facts_version`, so it is
        // permanently unstamped. Re-reading the local transcript can never
        // stamp this row, because the local transcript does not contain it.
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 'sess-shared', 3, 'assistant', 'text', \
                     'remote output', 'remote-uid-1')",
            [],
        )
        .unwrap();

        // A sentinel a re-read would overwrite. If the unstamped remote row put
        // the file back in the backfill set, this sync re-reads it -- and so
        // would every sync after it, forever, while never stamping the remote
        // row.
        conn.execute(
            "UPDATE session_events SET text = 'sentinel' \
             WHERE source = 'claude' AND event_uid = 'a1:0'",
            [],
        )
        .unwrap();
        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();
        let local: String = conn
            .query_row(
                "SELECT text FROM session_events WHERE source='claude' AND event_uid='a1:0'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            local, "sentinel",
            "an unchanged transcript must stay on the fast path even when another \
             observation of the same session carries no raw facts"
        );
    }

    /// Restore `path` byte for byte *and* to its original mtime, so its sync
    /// stamp is the one the state already holds and it takes the fast path.
    ///
    /// Without the mtime the file would be re-read for having changed, and a
    /// test that means to prove the backfill ran would pass whether or not it
    /// did. The caller asserts the stamp matches to prove this worked.
    fn restore_unchanged(path: &std::path::Path, bytes: &str, modified: std::time::SystemTime) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, bytes).unwrap();
        fs::File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(modified)
            .unwrap();
    }

    /// A readable root is not a fully readable root. A partially mounted
    /// archive returns some of the rollouts the stamp map names and not
    /// others, and "the walk returned at least one file" says nothing about
    /// the ones it did not return: their rows are still there and still null.
    #[test]
    fn a_partially_visible_codex_archive_does_not_retire_the_backfill_pass() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let day = home.join(".codex/archived_sessions/2026/04/20");
        fs::create_dir_all(&day).unwrap();
        let rollout = |id: &str| day.join(format!("rollout-2026-04-20T05-00-00-{id}.jsonl"));
        let lines = |id: &str| {
            format!(
                concat!(
                    r#"{{"timestamp":"2026-04-20T05:00:00.000Z","type":"session_meta","payload":{{"id":"{0}","cwd":"/tmp/project"}}}}"#,
                    "\n",
                    r#"{{"timestamp":"2026-04-20T05:00:00.100Z","type":"turn_context","payload":{{"turn_id":"turn_{0}","cwd":"/tmp/project","model":"gpt-5.4"}}}}"#,
                    "\n",
                    r#"{{"timestamp":"2026-04-20T05:00:01.000Z","type":"event_msg","payload":{{"type":"user_message","message":"run it"}}}}"#,
                    "\n",
                    r#"{{"timestamp":"2026-04-20T05:00:02.000Z","type":"event_msg","payload":{{"type":"agent_message","message":"done"}}}}"#,
                    "\n",
                ),
                id
            )
        };
        fs::write(rollout("sess_a"), lines("sess_a")).unwrap();
        fs::write(rollout("sess_b"), lines("sess_b")).unwrap();

        let turn_id = |conn: &Connection, id: &str| -> Option<String> {
            conn.query_row(
                "SELECT turn_id FROM session_events \
                 WHERE source='codex' AND session_id=? AND event_uid='3:agent_message'",
                [id],
                |row| row.get(0),
            )
            .unwrap()
        };

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        super::sync_codex_rollouts(&conn, &mut state, home).unwrap();
        assert_eq!(turn_id(&conn, "sess_a"), Some("turn_sess_a".into()));
        assert_eq!(turn_id(&conn, "sess_b"), Some("turn_sess_b".into()));
        let b_modified = rollout("sess_b").metadata().unwrap().modified().unwrap();

        // Half the archive is visible. The root is readable and the walk
        // returns `sess_a`, so nothing about the root itself is suspicious --
        // but `sess_b` is exactly as unreadable as if the whole mount were
        // missing, and its rows are just as null.
        blank_raw_message_facts(&conn, "codex");
        blank_raw_message_facts_state(&mut state);
        fs::remove_file(rollout("sess_b")).unwrap();
        super::sync_codex_rollouts(&conn, &mut state, home).unwrap();
        assert_eq!(turn_id(&conn, "sess_a"), Some("turn_sess_a".into()));
        assert!(
            state.get(super::CODEX_RAW_MESSAGE_FACTS_KEY).is_none(),
            "a run that saw only part of a known archive has not done the pass"
        );

        // Back, unchanged. Its stamp was dropped when the walk could not see
        // it, so it is read afresh rather than skipped on a stamp nothing
        // watched, and its facts land.
        restore_unchanged(&rollout("sess_b"), &lines("sess_b"), b_modified);
        super::sync_codex_rollouts(&conn, &mut state, home).unwrap();
        assert_eq!(turn_id(&conn, "sess_b"), Some("turn_sess_b".into()));
        assert_eq!(
            state
                .get(super::CODEX_RAW_MESSAGE_FACTS_KEY)
                .and_then(Value::as_i64),
            Some(super::RAW_MESSAGE_FACTS_GENERATION)
        );
    }

    /// The Claude walk asks the same question of its project tree.
    #[test]
    fn a_partially_visible_claude_root_does_not_retire_the_backfill_pass() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("projects");
        fs::create_dir_all(&root).unwrap();
        let transcript = |id: &str| root.join(format!("{id}.jsonl"));
        let lines = |id: &str| {
            format!(
                concat!(
                    r#"{{"type":"user","uuid":"u1","sessionId":"{0}","cwd":"/tmp/project","isSidechain":false,"version":"2.1.96","timestamp":"2026-04-20T00:00:00.000Z","message":{{"role":"user","content":"run it"}}}}"#,
                    "\n",
                    r#"{{"type":"assistant","uuid":"a1","parentUuid":"u1","sessionId":"{0}","cwd":"/tmp/project","isSidechain":false,"requestId":"req_{0}","version":"2.1.96","timestamp":"2026-04-20T00:00:01.000Z","message":{{"role":"assistant","model":"claude-opus-5","stop_reason":"end_turn","content":[{{"type":"text","text":"done"}}]}}}}"#,
                    "\n",
                ),
                id
            )
        };
        fs::write(transcript("sess-a"), lines("sess-a")).unwrap();
        fs::write(transcript("sess-b"), lines("sess-b")).unwrap();

        let request_id = |conn: &Connection, id: &str| -> Option<String> {
            conn.query_row(
                "SELECT request_id FROM session_events \
                 WHERE source='claude' AND session_id=? AND event_uid='a1:0'",
                [id],
                |row| row.get(0),
            )
            .unwrap()
        };

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        sync_claude_session_metadata(&conn, &mut state, &root).unwrap();
        assert_eq!(request_id(&conn, "sess-a"), Some("req_sess-a".into()));
        assert_eq!(request_id(&conn, "sess-b"), Some("req_sess-b".into()));
        let b_modified = transcript("sess-b").metadata().unwrap().modified().unwrap();

        blank_raw_message_facts(&conn, "claude");
        blank_raw_message_facts_state(&mut state);
        fs::remove_file(transcript("sess-b")).unwrap();
        sync_claude_session_metadata(&conn, &mut state, &root).unwrap();
        assert_eq!(request_id(&conn, "sess-a"), Some("req_sess-a".into()));
        assert!(
            state.get(super::CLAUDE_RAW_MESSAGE_FACTS_KEY).is_none(),
            "a run that saw only part of a known project tree has not done the pass"
        );

        restore_unchanged(&transcript("sess-b"), &lines("sess-b"), b_modified);
        sync_claude_session_metadata(&conn, &mut state, &root).unwrap();
        assert_eq!(request_id(&conn, "sess-b"), Some("req_sess-b".into()));
        assert_eq!(
            state
                .get(super::CLAUDE_RAW_MESSAGE_FACTS_KEY)
                .and_then(Value::as_i64),
            Some(super::RAW_MESSAGE_FACTS_GENERATION)
        );
    }

    /// A file the user really deleted must not hold the pass open for the life
    /// of the install. Its stamp is dropped when the walk cannot see it, so it
    /// costs exactly one more sync and then stops being a path the state knows
    /// about.
    ///
    /// Every sync here goes through the real `.sync-state.json` write and is
    /// reloaded from that file, because the in-memory map is not where this
    /// claim can be tested. `merged_sync_state` folds a run's keys into what is
    /// already on disk, and `merge_object_values` starts from the on-disk
    /// object -- so a nested entry this run *removed* is simply absent from the
    /// overlay and survives. An earlier version of this test held one `Map`
    /// across all three syncs and passed while the stamp was being resurrected
    /// on every write.
    #[test]
    fn a_deleted_rollout_holds_the_backfill_pass_open_for_one_sync_only() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let state_path = home.join(".sync-state.json");
        let day = home.join(".codex/sessions/2026/04/20");
        fs::create_dir_all(&day).unwrap();
        let kept = day.join("rollout-2026-04-20T05-00-00-sess_kept.jsonl");
        let gone = day.join("rollout-2026-04-20T05-00-00-sess_gone.jsonl");
        for (path, id) in [(&kept, "sess_kept"), (&gone, "sess_gone")] {
            fs::write(
                path,
                format!(
                    concat!(
                        r#"{{"timestamp":"2026-04-20T05:00:00.000Z","type":"session_meta","payload":{{"id":"{0}","cwd":"/tmp/project"}}}}"#, "\n",
                        r#"{{"timestamp":"2026-04-20T05:00:01.000Z","type":"event_msg","payload":{{"type":"user_message","message":"run it"}}}}"#, "\n",
                    ),
                    id
                ),
            )
            .unwrap();
        }

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();

        // One sync, persisted and reloaded the way a real run does it.
        let sync = |conn: &Connection| {
            let mut state = super::load_sync_state(&state_path).unwrap();
            super::sync_codex_rollouts(conn, &mut state, home).unwrap();
            super::checkpoint_sync_state(&state_path, &state);
            super::load_sync_state(&state_path).unwrap()
        };
        let stamped_paths = |state: &Map<String, Value>| -> Vec<String> {
            state
                .get("codex_rollouts_v5")
                .and_then(Value::as_object)
                .map(|map| map.keys().cloned().collect())
                .unwrap_or_default()
        };

        let state = sync(&conn);
        assert_eq!(stamped_paths(&state).len(), 2);

        // Clear the generation on disk, the way an upgraded install has it.
        let mut state = state;
        blank_raw_message_facts_state(&mut state);
        super::save_sync_state(&state_path, &state).unwrap();

        fs::remove_file(&gone).unwrap();
        let state = sync(&conn);
        assert!(
            state.get(super::CODEX_RAW_MESSAGE_FACTS_KEY).is_none(),
            "the first run after the file vanished cannot know it is gone"
        );
        assert_eq!(
            stamped_paths(&state),
            vec![kept.to_string_lossy().to_string()],
            "the vanished rollout's stamp must not survive the checkpoint merge"
        );
        assert!(
            !state.contains_key(super::FORGOTTEN_PATHS_KEY),
            "the removal instruction is not state and must not reach the file"
        );

        // Nothing changed on disk, but the deleted rollout is no longer a path
        // the state knows about, so the pass finishes.
        let state = sync(&conn);
        assert_eq!(
            state
                .get(super::CODEX_RAW_MESSAGE_FACTS_KEY)
                .and_then(Value::as_i64),
            Some(super::RAW_MESSAGE_FACTS_GENERATION),
            "a deleted file must not hold the pass open for the life of the install"
        );
    }

    /// Replace `path` with something the walk still enumerates and can still
    /// stat, but cannot read.
    ///
    /// A unix socket, because the obvious choices do not work here: `chmod 000`
    /// does nothing when the suite runs as root, which it does in CI, and a
    /// directory in place of the file is recursed into rather than enumerated,
    /// so it would exercise the unobserved-path rule instead of this one.
    /// `open(2)` on a socket fails with `ENXIO` whatever the uid, while
    /// `metadata()` succeeds, which is exactly the shape of the hazard: a path
    /// that looks present and unchanged to every check the walk makes before
    /// it tries to read.
    #[cfg(unix)]
    fn make_unreadable(path: &std::path::Path) {
        fs::remove_file(path).unwrap();
        // Leaked deliberately: dropping the listener would not remove the
        // socket file, and the file is what the test needs.
        std::mem::forget(std::os::unix::net::UnixListener::bind(path).unwrap());
        assert!(
            fs::read_to_string(path).is_err(),
            "the fixture must actually be unreadable or this test proves nothing"
        );
        assert!(
            path.metadata().is_ok(),
            "the fixture must still stat, or the walk would not even enumerate it"
        );
    }

    /// A read that fails is not an observation. Both parsers reach for the
    /// file with `read_to_string(..).unwrap_or_default()`, which turns a
    /// permission change, a swapped-out file or an I/O error into an empty
    /// string -- indistinguishable from an empty transcript. The walk then
    /// stamps the file as seen and records the generation, and the legacy rows
    /// behind that path stay null for the life of the install.
    #[cfg(unix)]
    #[test]
    fn an_unreadable_claude_transcript_does_not_retire_the_backfill_pass() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("projects");
        fs::create_dir_all(&root).unwrap();
        let state_path = dir.path().join(".sync-state.json");
        let transcript = |id: &str| root.join(format!("{id}.jsonl"));
        let lines = |id: &str| {
            format!(
                concat!(
                    r#"{{"type":"user","uuid":"u1","sessionId":"{0}","cwd":"/tmp/project","isSidechain":false,"version":"2.1.96","timestamp":"2026-04-20T00:00:00.000Z","message":{{"role":"user","content":"run it"}}}}"#,
                    "\n",
                    r#"{{"type":"assistant","uuid":"a1","parentUuid":"u1","sessionId":"{0}","cwd":"/tmp/project","isSidechain":false,"requestId":"req_{0}","version":"2.1.96","timestamp":"2026-04-20T00:00:01.000Z","message":{{"role":"assistant","model":"claude-opus-5","stop_reason":"end_turn","content":[{{"type":"text","text":"done"}}]}}}}"#,
                    "\n",
                ),
                id
            )
        };
        fs::write(transcript("sess-a"), lines("sess-a")).unwrap();
        fs::write(transcript("sess-b"), lines("sess-b")).unwrap();

        let request_id = |conn: &Connection, id: &str| -> Option<String> {
            conn.query_row(
                "SELECT request_id FROM session_events \
                 WHERE source='claude' AND session_id=? AND event_uid='a1:0'",
                [id],
                |row| row.get(0),
            )
            .unwrap()
        };

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let sync = |conn: &Connection| {
            let mut state = super::load_sync_state(&state_path).unwrap();
            sync_claude_session_metadata(conn, &mut state, &root).unwrap();
            super::checkpoint_sync_state(&state_path, &state);
            super::load_sync_state(&state_path).unwrap()
        };
        let stamped = |state: &Map<String, Value>, path: &std::path::Path| -> bool {
            state
                .get("claude_sessions_v3")
                .and_then(Value::as_object)
                .is_some_and(|map| map.contains_key(path.to_string_lossy().as_ref()))
        };

        let state = sync(&conn);
        assert_eq!(request_id(&conn, "sess-b"), Some("req_sess-b".into()));
        let b_modified = transcript("sess-b").metadata().unwrap().modified().unwrap();

        let mut state = state;
        blank_raw_message_facts(&conn, "claude");
        blank_raw_message_facts_state(&mut state);
        super::save_sync_state(&state_path, &state).unwrap();

        make_unreadable(&transcript("sess-b"));
        let state = sync(&conn);
        assert_eq!(request_id(&conn, "sess-a"), Some("req_sess-a".into()));
        assert!(
            state.get(super::CLAUDE_RAW_MESSAGE_FACTS_KEY).is_none(),
            "a transcript this run could not read has not been backfilled"
        );
        assert!(
            !stamped(&state, &transcript("sess-b")),
            "a failed read must not be recorded as an observation"
        );

        // Readable again and unchanged: the stamp it could not be given is
        // what gets it re-read, and its facts land.
        fs::remove_file(transcript("sess-b")).unwrap();
        restore_unchanged(&transcript("sess-b"), &lines("sess-b"), b_modified);
        let state = sync(&conn);
        assert_eq!(request_id(&conn, "sess-b"), Some("req_sess-b".into()));
        assert_eq!(
            state
                .get(super::CLAUDE_RAW_MESSAGE_FACTS_KEY)
                .and_then(Value::as_i64),
            Some(super::RAW_MESSAGE_FACTS_GENERATION)
        );
    }

    /// The codex walk reaches its rollouts the same way, through
    /// `read_codex_session_meta`, which swallows the read error with `.ok()`
    /// and then cannot tell an unreadable rollout from one with no
    /// `session_meta` line.
    #[cfg(unix)]
    #[test]
    fn an_unreadable_codex_rollout_does_not_retire_the_backfill_pass() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let state_path = home.join(".sync-state.json");
        let day = home.join(".codex/sessions/2026/04/20");
        fs::create_dir_all(&day).unwrap();
        let rollout = |id: &str| day.join(format!("rollout-2026-04-20T05-00-00-{id}.jsonl"));
        let lines = |id: &str| {
            format!(
                concat!(
                    r#"{{"timestamp":"2026-04-20T05:00:00.000Z","type":"session_meta","payload":{{"id":"{0}","cwd":"/tmp/project"}}}}"#,
                    "\n",
                    r#"{{"timestamp":"2026-04-20T05:00:00.100Z","type":"turn_context","payload":{{"turn_id":"turn_{0}","cwd":"/tmp/project","model":"gpt-5.4"}}}}"#,
                    "\n",
                    r#"{{"timestamp":"2026-04-20T05:00:01.000Z","type":"event_msg","payload":{{"type":"user_message","message":"run it"}}}}"#,
                    "\n",
                    r#"{{"timestamp":"2026-04-20T05:00:02.000Z","type":"event_msg","payload":{{"type":"agent_message","message":"done"}}}}"#,
                    "\n",
                ),
                id
            )
        };
        fs::write(rollout("sess_a"), lines("sess_a")).unwrap();
        fs::write(rollout("sess_b"), lines("sess_b")).unwrap();

        let turn_id = |conn: &Connection, id: &str| -> Option<String> {
            conn.query_row(
                "SELECT turn_id FROM session_events \
                 WHERE source='codex' AND session_id=? AND event_uid='3:agent_message'",
                [id],
                |row| row.get(0),
            )
            .unwrap()
        };

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let sync = |conn: &Connection| {
            let mut state = super::load_sync_state(&state_path).unwrap();
            super::sync_codex_rollouts(conn, &mut state, home).unwrap();
            super::checkpoint_sync_state(&state_path, &state);
            super::load_sync_state(&state_path).unwrap()
        };
        let stamped = |state: &Map<String, Value>, path: &std::path::Path| -> bool {
            state
                .get("codex_rollouts_v5")
                .and_then(Value::as_object)
                .is_some_and(|map| map.contains_key(path.to_string_lossy().as_ref()))
        };

        let state = sync(&conn);
        assert_eq!(turn_id(&conn, "sess_b"), Some("turn_sess_b".into()));
        let b_modified = rollout("sess_b").metadata().unwrap().modified().unwrap();

        let mut state = state;
        blank_raw_message_facts(&conn, "codex");
        blank_raw_message_facts_state(&mut state);
        super::save_sync_state(&state_path, &state).unwrap();

        make_unreadable(&rollout("sess_b"));
        let state = sync(&conn);
        assert_eq!(turn_id(&conn, "sess_a"), Some("turn_sess_a".into()));
        assert!(
            state.get(super::CODEX_RAW_MESSAGE_FACTS_KEY).is_none(),
            "a rollout this run could not read has not been backfilled"
        );
        assert!(
            !stamped(&state, &rollout("sess_b")),
            "a failed read must not be recorded as an observation"
        );

        fs::remove_file(rollout("sess_b")).unwrap();
        restore_unchanged(&rollout("sess_b"), &lines("sess_b"), b_modified);
        let state = sync(&conn);
        assert_eq!(turn_id(&conn, "sess_b"), Some("turn_sess_b".into()));
        assert_eq!(
            state
                .get(super::CODEX_RAW_MESSAGE_FACTS_KEY)
                .and_then(Value::as_i64),
            Some(super::RAW_MESSAGE_FACTS_GENERATION)
        );
    }

    /// The same bound for Claude, through the same persisted path.
    #[test]
    fn a_deleted_transcript_holds_the_backfill_pass_open_for_one_sync_only() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("projects");
        fs::create_dir_all(&root).unwrap();
        let state_path = dir.path().join(".sync-state.json");
        let kept = root.join("sess-kept.jsonl");
        let gone = root.join("sess-gone.jsonl");
        for (path, id) in [(&kept, "sess-kept"), (&gone, "sess-gone")] {
            fs::write(
                path,
                format!(
                    r#"{{"type":"user","uuid":"u1","sessionId":"{0}","cwd":"/tmp/project","isSidechain":false,"version":"2.1.96","timestamp":"2026-04-20T00:00:00.000Z","message":{{"role":"user","content":"run it"}}}}"#,
                    id
                ) + "\n",
            )
            .unwrap();
        }

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();

        let sync = |conn: &Connection| {
            let mut state = super::load_sync_state(&state_path).unwrap();
            sync_claude_session_metadata(conn, &mut state, &root).unwrap();
            super::checkpoint_sync_state(&state_path, &state);
            super::load_sync_state(&state_path).unwrap()
        };
        let stamped_paths = |state: &Map<String, Value>| -> Vec<String> {
            state
                .get("claude_sessions_v3")
                .and_then(Value::as_object)
                .map(|map| map.keys().cloned().collect())
                .unwrap_or_default()
        };

        let state = sync(&conn);
        assert_eq!(stamped_paths(&state).len(), 2);

        let mut state = state;
        blank_raw_message_facts_state(&mut state);
        super::save_sync_state(&state_path, &state).unwrap();

        fs::remove_file(&gone).unwrap();
        let state = sync(&conn);
        assert!(
            state.get(super::CLAUDE_RAW_MESSAGE_FACTS_KEY).is_none(),
            "the first run after the transcript vanished cannot know it is gone"
        );
        assert_eq!(
            stamped_paths(&state),
            vec![kept.to_string_lossy().to_string()],
            "the vanished transcript's stamp must not survive the checkpoint merge"
        );

        let state = sync(&conn);
        assert_eq!(
            state
                .get(super::CLAUDE_RAW_MESSAGE_FACTS_KEY)
                .and_then(Value::as_i64),
            Some(super::RAW_MESSAGE_FACTS_GENERATION),
            "a deleted transcript must not hold the pass open for the life of the install"
        );
    }

    /// A mount point exists whether or not anything is mounted on it. An
    /// archive root that is present but shows none of the rollouts the stamp
    /// map names is one this run could not read, and `root.exists()` alone
    /// cannot tell it from a reachable archive: the walk completes over
    /// nothing, the generation is recorded, and when the files come back
    /// unchanged their stamps put them straight on the fast path with their
    /// facts still null.
    #[test]
    fn an_empty_but_present_codex_archive_does_not_retire_the_backfill_pass() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let archived = home.join(".codex/archived_sessions/2026/04/20");
        fs::create_dir_all(&archived).unwrap();
        let rollout = archived.join("rollout-2026-04-20T05-00-00-sess_mounted.jsonl");
        let bytes = concat!(
            r#"{"timestamp":"2026-04-20T05:00:00.000Z","type":"session_meta","payload":{"id":"sess_mounted","cwd":"/tmp/project"}}"#,
            "\n",
            r#"{"timestamp":"2026-04-20T05:00:00.100Z","type":"turn_context","payload":{"turn_id":"turn_1","cwd":"/tmp/project","model":"gpt-5.4"}}"#,
            "\n",
            r#"{"timestamp":"2026-04-20T05:00:01.000Z","type":"event_msg","payload":{"type":"user_message","message":"run it"}}"#,
            "\n",
            r#"{"timestamp":"2026-04-20T05:00:02.000Z","type":"event_msg","payload":{"type":"agent_message","message":"done"}}"#,
            "\n",
        );
        fs::write(&rollout, bytes).unwrap();

        let turn_id = |conn: &Connection| -> Option<String> {
            conn.query_row(
                "SELECT turn_id FROM session_events \
                 WHERE source='codex' AND event_uid='3:agent_message'",
                [],
                |row| row.get(0),
            )
            .unwrap()
        };

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        super::sync_codex_rollouts(&conn, &mut state, home).unwrap();
        assert_eq!(turn_id(&conn), Some("turn_1".into()));
        let stamp = file_stamp(&rollout).unwrap();
        let modified = rollout.metadata().unwrap().modified().unwrap();

        // The upgraded-install state, with the archive mounted but empty. The
        // root is still there; its contents are not.
        blank_raw_message_facts(&conn, "codex");
        blank_raw_message_facts_state(&mut state);
        fs::remove_file(&rollout).unwrap();
        assert!(home.join(".codex/archived_sessions").exists());
        super::sync_codex_rollouts(&conn, &mut state, home).unwrap();
        assert!(
            state.get(super::CODEX_RAW_MESSAGE_FACTS_KEY).is_none(),
            "a root that showed none of the rollouts it is known to hold was not walked"
        );

        // Back, byte for byte and at its original mtime: the stamp matches, so
        // nothing but a still-pending backfill can cause this file to be read.
        restore_unchanged(&rollout, bytes, modified);
        assert_eq!(
            file_stamp(&rollout).unwrap(),
            stamp,
            "the restored rollout must carry its original stamp or this proves nothing"
        );
        super::sync_codex_rollouts(&conn, &mut state, home).unwrap();
        assert_eq!(turn_id(&conn), Some("turn_1".into()));
        assert_eq!(
            state
                .get(super::CODEX_RAW_MESSAGE_FACTS_KEY)
                .and_then(Value::as_i64),
            Some(super::RAW_MESSAGE_FACTS_GENERATION)
        );
    }

    /// The Claude walk had the same gap, and only checked that its root
    /// existed.
    #[test]
    fn an_empty_but_present_claude_root_does_not_retire_the_backfill_pass() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("projects");
        fs::create_dir_all(&root).unwrap();
        let path = root.join("sess-mounted.jsonl");
        let bytes = concat!(
            r#"{"type":"user","uuid":"u1","sessionId":"sess-mounted","cwd":"/tmp/project","isSidechain":false,"version":"2.1.96","timestamp":"2026-04-20T00:00:00.000Z","message":{"role":"user","content":"run it"}}"#,
            "\n",
            r#"{"type":"assistant","uuid":"a1","parentUuid":"u1","sessionId":"sess-mounted","cwd":"/tmp/project","isSidechain":false,"requestId":"req_1","version":"2.1.96","timestamp":"2026-04-20T00:00:01.000Z","message":{"role":"assistant","model":"claude-opus-5","stop_reason":"end_turn","content":[{"type":"text","text":"done"}]}}"#,
            "\n",
        );
        fs::write(&path, bytes).unwrap();

        let request_id = |conn: &Connection| -> Option<String> {
            conn.query_row(
                "SELECT request_id FROM session_events \
                 WHERE source='claude' AND event_uid='a1:0'",
                [],
                |row| row.get(0),
            )
            .unwrap()
        };

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        sync_claude_session_metadata(&conn, &mut state, &root).unwrap();
        assert_eq!(request_id(&conn), Some("req_1".into()));
        let stamp = claude_sync_stamp(&path).unwrap();
        let modified = path.metadata().unwrap().modified().unwrap();

        blank_raw_message_facts(&conn, "claude");
        blank_raw_message_facts_state(&mut state);
        fs::remove_file(&path).unwrap();
        assert!(root.exists());
        sync_claude_session_metadata(&conn, &mut state, &root).unwrap();
        assert!(
            state.get(super::CLAUDE_RAW_MESSAGE_FACTS_KEY).is_none(),
            "a project tree that showed none of its known transcripts was not walked"
        );

        restore_unchanged(&path, bytes, modified);
        assert_eq!(
            claude_sync_stamp(&path).unwrap(),
            stamp,
            "the restored transcript must carry its original stamp or this proves nothing"
        );
        sync_claude_session_metadata(&conn, &mut state, &root).unwrap();
        assert_eq!(request_id(&conn), Some("req_1".into()));
        assert_eq!(
            state
                .get(super::CLAUDE_RAW_MESSAGE_FACTS_KEY)
                .and_then(Value::as_i64),
            Some(super::RAW_MESSAGE_FACTS_GENERATION)
        );
    }

    /// An archive root the stamp map knows about but that is not on disk this
    /// run is one the walk could not read, not one that is gone. Recording the
    /// generation there would retire the one-time pass over rollouts nothing
    /// ever opened, and the facts would stay null for the life of the install
    /// while `sync` went on reporting success.
    #[test]
    fn an_unavailable_codex_archive_does_not_retire_the_backfill_pass() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let archived = home.join(".codex/archived_sessions/2026/04/20");
        fs::create_dir_all(&archived).unwrap();
        let rollout = archived.join("rollout-2026-04-20T05-00-00-sess_archived.jsonl");
        fs::write(
            &rollout,
            concat!(
                r#"{"timestamp":"2026-04-20T05:00:00.000Z","type":"session_meta","payload":{"id":"sess_archived","cwd":"/tmp/project"}}"#, "\n",
                r#"{"timestamp":"2026-04-20T05:00:00.100Z","type":"turn_context","payload":{"turn_id":"turn_1","cwd":"/tmp/project","model":"gpt-5.4"}}"#, "\n",
                r#"{"timestamp":"2026-04-20T05:00:01.000Z","type":"event_msg","payload":{"type":"user_message","message":"run it"}}"#, "\n",
                r#"{"timestamp":"2026-04-20T05:00:02.000Z","type":"event_msg","payload":{"type":"agent_message","message":"done"}}"#, "\n",
            ),
        )
        .unwrap();

        let turn_id = |conn: &Connection| -> Option<String> {
            conn.query_row(
                "SELECT turn_id FROM session_events \
                 WHERE source='codex' AND event_uid='3:agent_message'",
                [],
                |row| row.get(0),
            )
            .unwrap()
        };

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        super::sync_codex_rollouts(&conn, &mut state, home).unwrap();
        assert_eq!(turn_id(&conn), Some("turn_1".into()));

        // The upgraded-install state, with the archive unavailable: an
        // unmounted home, an external drive, a profile not yet materialized.
        blank_raw_message_facts(&conn, "codex");
        blank_raw_message_facts_state(&mut state);
        fs::remove_dir_all(home.join(".codex/archived_sessions")).unwrap();
        super::sync_codex_rollouts(&conn, &mut state, home).unwrap();
        assert_eq!(
            turn_id(&conn),
            None,
            "nothing could be repaired while the archive was unavailable"
        );
        assert!(
            state.get(super::CODEX_RAW_MESSAGE_FACTS_KEY).is_none(),
            "a walk that could not open the archive has not done the pass"
        );

        // Back on disk, unchanged: the pass is still owed, so it runs now.
        fs::create_dir_all(&archived).unwrap();
        fs::write(
            &rollout,
            concat!(
                r#"{"timestamp":"2026-04-20T05:00:00.000Z","type":"session_meta","payload":{"id":"sess_archived","cwd":"/tmp/project"}}"#, "\n",
                r#"{"timestamp":"2026-04-20T05:00:00.100Z","type":"turn_context","payload":{"turn_id":"turn_1","cwd":"/tmp/project","model":"gpt-5.4"}}"#, "\n",
                r#"{"timestamp":"2026-04-20T05:00:01.000Z","type":"event_msg","payload":{"type":"user_message","message":"run it"}}"#, "\n",
                r#"{"timestamp":"2026-04-20T05:00:02.000Z","type":"event_msg","payload":{"type":"agent_message","message":"done"}}"#, "\n",
            ),
        )
        .unwrap();
        super::sync_codex_rollouts(&conn, &mut state, home).unwrap();
        assert_eq!(turn_id(&conn), Some("turn_1".into()));
        assert_eq!(
            state
                .get(super::CODEX_RAW_MESSAGE_FACTS_KEY)
                .and_then(Value::as_i64),
            Some(super::RAW_MESSAGE_FACTS_GENERATION)
        );
    }

    /// The other side of that guard: a root this install never had is not an
    /// archive we failed to read, and must not hold the generation back
    /// forever. Only `.codex/sessions` exists here and `archived_sessions`
    /// never did.
    #[test]
    fn a_root_the_state_never_knew_does_not_hold_the_backfill_pass_open() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let day = home.join(".codex/sessions/2026/04/20");
        fs::create_dir_all(&day).unwrap();
        fs::write(
            day.join("rollout-2026-04-20T05-00-00-sess_only.jsonl"),
            concat!(
                r#"{"timestamp":"2026-04-20T05:00:00.000Z","type":"session_meta","payload":{"id":"sess_only","cwd":"/tmp/project"}}"#, "\n",
                r#"{"timestamp":"2026-04-20T05:00:01.000Z","type":"event_msg","payload":{"type":"user_message","message":"run it"}}"#, "\n",
            ),
        )
        .unwrap();

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        super::sync_codex_rollouts(&conn, &mut state, home).unwrap();
        assert!(!home.join(".codex/archived_sessions").exists());
        assert_eq!(
            state
                .get(super::CODEX_RAW_MESSAGE_FACTS_KEY)
                .and_then(Value::as_i64),
            Some(super::RAW_MESSAGE_FACTS_GENERATION)
        );
    }

    #[test]
    fn plain_codex_sync_backfills_raw_facts_for_rollouts_indexed_before_them() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let day = home.join(".codex/sessions/2026/04/20");
        fs::create_dir_all(&day).unwrap();
        let rollout = day.join("rollout-2026-04-20T05-00-00-sess_backfill.jsonl");
        fs::write(
            &rollout,
            concat!(
                r#"{"timestamp":"2026-04-20T05:00:00.000Z","type":"session_meta","payload":{"id":"sess_backfill","cwd":"/tmp/project"}}"#, "\n",
                r#"{"timestamp":"2026-04-20T05:00:00.100Z","type":"turn_context","payload":{"turn_id":"turn_1","cwd":"/tmp/project","model":"gpt-5.4"}}"#, "\n",
                r#"{"timestamp":"2026-04-20T05:00:01.000Z","type":"event_msg","payload":{"type":"user_message","message":"run it"}}"#, "\n",
                r#"{"timestamp":"2026-04-20T05:00:02.000Z","type":"event_msg","payload":{"type":"agent_message","message":"done"}}"#, "\n",
            ),
        )
        .unwrap();

        let turn_ids = |conn: &Connection| -> Vec<(String, Option<String>)> {
            conn.prepare(
                "SELECT event_uid, turn_id FROM session_events \
                 WHERE source='codex' AND session_id='sess_backfill' ORDER BY id",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
        };

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        super::sync_codex_rollouts(&conn, &mut state, home).unwrap();
        assert_eq!(
            turn_ids(&conn),
            vec![
                ("2:user_message".into(), Some("turn_1".into())),
                ("3:agent_message".into(), Some("turn_1".into())),
            ]
        );

        blank_raw_message_facts(&conn, "codex");
        blank_raw_message_facts_state(&mut state);
        super::sync_codex_rollouts(&conn, &mut state, home).unwrap();
        assert_eq!(
            turn_ids(&conn),
            vec![
                ("2:user_message".into(), Some("turn_1".into())),
                ("3:agent_message".into(), Some("turn_1".into())),
            ]
        );

        conn.execute(
            "UPDATE session_events SET text = 'sentinel' \
             WHERE source='codex' AND event_uid='3:agent_message'",
            [],
        )
        .unwrap();
        super::sync_codex_rollouts(&conn, &mut state, home).unwrap();
        let text: Option<String> = conn
            .query_row(
                "SELECT text FROM session_events WHERE source='codex' AND event_uid='3:agent_message'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(text.as_deref(), Some("sentinel"));
    }

    #[test]
    fn an_unchanged_subagent_sidecar_is_not_re_read() {
        let dir = tempfile::tempdir().unwrap();
        let (_, named, _) = write_claude_parent_with_subagents(dir.path());
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();

        // Rewrite the sidecar and record the rewritten file as already seen: a
        // walk that re-reads every unchanged sidecar, because a sidecar never
        // has the catalog row the transcript check needs, would ingest this.
        fs::write(
            &named,
            concat!(
                r#"{"sessionId":"claude-root","agentId":"abc","isSidechain":true,"uuid":"side-b","cwd":"/work/app","type":"assistant","message":{"role":"assistant","content":"later result"},"timestamp":"2026-08-31T11:00:06Z"}"#,
                "\n",
            ),
        )
        .unwrap();
        let key = named.to_string_lossy().to_string();
        state
            .get_mut("claude_sessions_v3")
            .and_then(Value::as_object_mut)
            .unwrap()
            .insert(key, json!(claude_sync_stamp(&named).unwrap()));

        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();
        let rewritten = |conn: &Connection| -> i64 {
            conn.query_row(
                "SELECT COUNT(*) FROM session_events \
                 WHERE source='claude' AND session_id='abc' AND event_uid='side-b:0'",
                [],
                |row| row.get(0),
            )
            .unwrap()
        };
        assert_eq!(rewritten(&conn), 0);

        // Once the events it produced are gone the sidecar is evidence of
        // nothing indexed, so the same unchanged stamp reads it again.
        conn.execute(
            "DELETE FROM session_events WHERE source='claude' AND session_id='abc'",
            [],
        )
        .unwrap();
        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();
        assert_eq!(rewritten(&conn), 1);
    }

    #[test]
    fn a_changed_metadata_sidecar_refreshes_delegation_during_a_full_sync() {
        let dir = tempfile::tempdir().unwrap();
        let (_, named, _) = write_claude_parent_with_subagents(dir.path());
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();

        // The transcript never moves; only what describes the child changes.
        fs::write(
            named.with_extension("meta.json"),
            r#"{"agentType":"Explore","description":"explore the code","toolUseId":"toolu_1","spawnDepth":2,"model":"other-model"}"#,
        )
        .unwrap();
        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();

        let row = delegation_row(&conn, "claude_subagent_meta");
        assert_eq!(row.agent_type.as_deref(), Some("Explore"));
        assert_eq!(row.agent_name.as_deref(), Some("explore the code"));
        assert_eq!(row.model.as_deref(), Some("other-model"));
        assert_eq!(row.spawn_depth, Some(2));
    }

    #[test]
    fn unchanged_claude_stamps_backfill_missing_delegation_rows() {
        let dir = tempfile::tempdir().unwrap();
        let (parent, named, nameless) = write_claude_parent_with_subagents(dir.path());
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        // A database synced before delegation was recorded: the parent is
        // registered and indexed, so its stamp fast path skips it entirely,
        // and no topology exists at all. The row carries the current raw-facts
        // generation; what is missing here is topology, not the facts.
        super::upsert_session(
            &conn,
            "claude-root",
            "claude",
            Some("/work/app"),
            None,
            1,
            2,
            None,
            Some(&parent.to_string_lossy()),
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, ts_ms, role, kind, text, event_uid, raw_facts_version) \
             VALUES ('claude', 'claude-root', 2, 'assistant', 'text', 'kept event', 'a1:0', ?)",
            [super::RAW_MESSAGE_FACTS_VERSION],
        )
        .unwrap();
        let mut claude_sessions = Map::new();
        for path in [&parent, &named, &nameless] {
            claude_sessions.insert(
                path.to_string_lossy().to_string(),
                json!(claude_sync_stamp(path).unwrap()),
            );
        }
        let mut state = Map::new();
        state.insert(
            "claude_sessions_v3".to_string(),
            Value::Object(claude_sessions),
        );

        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();

        assert_eq!(
            delegation_row(&conn, "claude_subagent_meta")
                .child
                .as_deref(),
            Some("abc")
        );
        assert_eq!(
            delegation_row(&conn, "claude_sidechain_records").identity_status,
            "unlinked"
        );
        // The parent itself was never re-read: its stamp skip still holds, so
        // the walk that backfilled the topology did not re-ingest its prompt.
        let parent_prompts: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM history WHERE source='claude'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(parent_prompts, 0);
    }

    #[test]
    fn claude_control_wrappers_do_not_enter_prompt_history() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("control.jsonl");
        fs::write(
            &path,
            concat!(
                "{\"sessionId\":\"s-control\",\"uuid\":\"control\",\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"<command-message>generated wrapper\"}}\n",
                "{\"sessionId\":\"s-control\",\"uuid\":\"human\",\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"real prompt\"}}\n",
            ),
        )
        .unwrap();
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();

        ingest_claude_transcript(&conn, &path).unwrap();

        let prompts: Vec<String> = conn
            .prepare("SELECT prompt FROM history ORDER BY id")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(prompts, vec!["real prompt"]);
    }

    fn write_rich_claude_transcript(path: &std::path::Path) {
        fs::write(
            path,
            r#"{"type":"user","uuid":"u1","sessionId":"s-rich","cwd":"/tmp/proj","gitBranch":"feat/rich","timestamp":"2026-06-25T10:00:00.000Z","message":{"role":"user","content":"please update auth"}}
{"type":"assistant","uuid":"a1","parentUuid":"u1","sessionId":"s-rich","cwd":"/tmp/proj","gitBranch":"feat/rich","timestamp":"2026-06-25T10:00:01.000Z","message":{"role":"assistant","model":"claude-test","usage":{"input_tokens":11,"output_tokens":22},"content":[{"type":"text","text":"I will update auth.ts"},{"type":"tool_use","id":"toolu_1","name":"Edit","input":{"file_path":"/tmp/proj/auth.ts","old_string":"old","new_string":"new"}}]}}
{"type":"user","uuid":"r1","parentUuid":"a1","sessionId":"s-rich","cwd":"/tmp/proj","gitBranch":"feat/rich","timestamp":"2026-06-25T10:00:02.000Z","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"ok","toolUseResult":{"filePath":"/tmp/proj/auth.ts","structuredPatch":"--- a/auth.ts\n+++ b/auth.ts\n-old\n+new\n","userModified":true}}]}}"#,
        )
        .unwrap();
    }

    fn write_rich_codex_rollout(path: &std::path::Path) {
        fs::write(
            path,
            concat!(
                r#"{"timestamp":"2026-08-01T10:00:00.000Z","type":"session_meta","payload":{"id":"sess-top","cwd":"/tmp/proj","git":{"branch":"main"},"cli_version":"0.148.0"}}"#, "\n",
                r#"{"timestamp":"2026-08-01T10:00:00.100Z","type":"turn_context","payload":{"turn_id":"t1","cwd":"/tmp/proj","model":"gpt-5.4"}}"#, "\n",
                r#"{"timestamp":"2026-08-01T10:00:00.150Z","type":"event_msg","payload":{"type":"token_count","info":null,"rate_limits":{}}}"#, "\n",
                r#"{"timestamp":"2026-08-01T10:00:00.200Z","type":"event_msg","payload":{"type":"task_started","turn_id":"t1"}}"#, "\n",
                r#"{"timestamp":"2026-08-01T10:00:01.000Z","type":"event_msg","payload":{"type":"user_message","message":"fix the importer"}}"#, "\n",
                r#"{"timestamp":"2026-08-01T10:00:01.100Z","type":"event_msg","payload":{"type":"user_message","message":"<environment_context>injected</environment_context>"}}"#, "\n",
                r#"{"timestamp":"2026-08-01T10:00:02.000Z","type":"event_msg","payload":{"type":"agent_reasoning","text":"I should check git status."}}"#, "\n",
                r#"{"timestamp":"2026-08-01T10:00:02.500Z","type":"response_item","payload":{"type":"reasoning","id":"rs_1","summary":[],"encrypted_content":"opaque"}}"#, "\n",
                r#"{"timestamp":"2026-08-01T10:00:03.000Z","type":"response_item","payload":{"type":"function_call","id":"fc_1","name":"exec_command","arguments":"{\"cmd\":\"git status\"}","call_id":"call_1"}}"#, "\n",
                r#"{"timestamp":"2026-08-01T10:00:04.000Z","type":"response_item","payload":{"type":"function_call_output","id":"fco_1","call_id":"call_1","output":"clean tree"}}"#, "\n",
                r#"{"timestamp":"2026-08-01T10:00:04.500Z","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":1000,"cached_input_tokens":400,"cache_write_input_tokens":0,"output_tokens":120,"reasoning_output_tokens":30,"total_tokens":1120}}}}"#, "\n",
                r#"{"timestamp":"2026-08-01T10:00:04.600Z","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":1000,"cached_input_tokens":400,"cache_write_input_tokens":0,"output_tokens":120,"reasoning_output_tokens":30,"total_tokens":1120}}}}"#, "\n",
                r#"{"timestamp":"2026-08-01T10:00:05.000Z","type":"response_item","payload":{"type":"custom_tool_call","id":"ctc_1","status":"completed","call_id":"call_2","name":"apply_patch","input":"*** Begin Patch\n*** Update File: /tmp/proj/README.md\n@@\n+banner\n-old\n*** End Patch\n"}}"#, "\n",
                r#"{"timestamp":"2026-08-01T10:00:05.500Z","type":"event_msg","payload":{"type":"patch_apply_end","call_id":"call_2","turn_id":"t1","success":true,"changes":{"/tmp/proj/README.md":{"type":"update","unified_diff":"@@\n+banner\n-old"}}}}"#, "\n",
                r#"{"timestamp":"2026-08-01T10:00:06.000Z","type":"event_msg","payload":{"type":"agent_message","message":"Done. The importer is fixed.","phase":"final_answer"}}"#, "\n",
                r#"{"timestamp":"2026-08-01T10:00:06.200Z","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":1600,"cached_input_tokens":900,"cache_write_input_tokens":50,"output_tokens":180,"reasoning_output_tokens":40,"total_tokens":1780}}}}"#, "\n",
                r#"{"timestamp":"2026-08-01T10:00:06.300Z","type":"event_msg","payload":{"type":"task_complete","turn_id":"t1","last_agent_message":"Done. The importer is fixed.","duration_ms":5000}}"#, "\n",
            ),
        )
        .unwrap();
    }

    fn codex_meta(path: &std::path::Path) -> super::CodexSessionMeta {
        super::read_codex_session_meta(path).unwrap().unwrap()
    }

    #[test]
    fn codex_parent_resolution_supports_current_structured_and_legacy_metadata() {
        let cases = [
            (
                json!({
                    "id": "child",
                    "parent_thread_id": "top-level-parent",
                    "session_id": "legacy-parent",
                    "source": {"subagent": {"thread_spawn": {
                        "parent_thread_id": "nested-parent",
                        "depth": 1
                    }}}
                }),
                Some("top-level-parent"),
            ),
            (
                json!({
                    "id": "child",
                    "source": {"subagent": {"thread_spawn": {
                        "parent_thread_id": "nested-parent",
                        "depth": 1
                    }}}
                }),
                Some("nested-parent"),
            ),
            (
                json!({"id": "child", "session_id": "legacy-parent"}),
                Some("legacy-parent"),
            ),
            (
                json!({
                    "id": "child",
                    "parent_thread_id": "child",
                    "session_id": "",
                    "source": {"subagent": {"thread_spawn": {
                        "parent_thread_id": "child",
                        "depth": 1
                    }}}
                }),
                None,
            ),
        ];

        for (payload, expected) in cases {
            assert_eq!(
                super::codex_parent_session_id(payload.as_object(), "child").as_deref(),
                expected
            );
        }
    }

    /// A `source.subagent` marker with no parent metadata is a standalone
    /// guardian: it keeps its own identity and stays discoverable as a root.
    ///
    /// This inverts the `is_subagent` assertion that ac0b64a
    /// ("fix: resolve Codex subagent parent IDs") left here, which is the exact
    /// classification this PR exists to change. That commit's own concern — that
    /// a marker-only rollout resolves *no* parent — is asserted unchanged below,
    /// and the linked-child case it protected is covered by the sibling test.
    #[test]
    fn codex_marker_only_guardian_is_a_standalone_root() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rollout-guardian.jsonl");
        fs::write(
            &path,
            concat!(
                r#"{"timestamp":"2026-08-31T10:00:00.000Z","type":"session_meta","payload":{"id":"guardian","cwd":"/tmp/proj","source":{"subagent":{"other":"guardian"}}}}"#,
                "\n",
            ),
        )
        .unwrap();

        let meta = codex_meta(&path);
        assert!(
            !meta.is_subagent,
            "a marker-only guardian must stay discoverable under its own payload.id"
        );
        assert_eq!(meta.parent_session_id, None);
        assert_eq!(meta.parent_thread_id, None);
        assert_eq!(meta.subagent_label.as_deref(), Some("guardian"));
    }

    /// The same marker *with* an explicit parent stays a child, including when
    /// the parent is only reachable through the structured thread-spawn source
    /// that ac0b64a taught the resolver to read.
    #[test]
    fn codex_marker_guardian_with_a_parent_stays_a_subagent() {
        let dir = tempfile::tempdir().unwrap();
        for (name, payload) in [
            (
                "explicit",
                r#"{"id":"guardian","cwd":"/tmp/proj","parent_thread_id":"root","source":{"subagent":{"other":"guardian"}}}"#,
            ),
            (
                "thread-spawn",
                r#"{"id":"guardian","cwd":"/tmp/proj","source":{"subagent":{"other":"guardian","thread_spawn":{"parent_thread_id":"root"}}}}"#,
            ),
        ] {
            let path = dir.path().join(format!("rollout-{name}.jsonl"));
            fs::write(
                &path,
                format!(
                    "{{\"timestamp\":\"2026-08-31T10:00:00.000Z\",\"type\":\"session_meta\",\"payload\":{payload}}}\n"
                ),
            )
            .unwrap();
            let meta = codex_meta(&path);
            assert!(meta.is_subagent, "{name} guardian must remain a child");
            assert_eq!(meta.parent_session_id.as_deref(), Some("root"), "{name}");
        }
    }

    fn response_user(ts: &str, text: &str) -> String {
        serde_json::json!({
            "timestamp": ts,
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": text}]
            }
        })
        .to_string()
    }

    #[test]
    fn ingests_current_codex_desktop_user_messages_and_filters_context() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rollout-current.jsonl");
        fs::write(
            &path,
            format!(
                "{}\n{}\n{}\n{}\n{}\n{}\n",
                r#"{"timestamp":"2026-08-31T10:00:00.000Z","type":"session_meta","payload":{"id":"sess-current","cwd":"/tmp/proj","thread_source":"user","source":"vscode","originator":"Codex Desktop"}}"#,
                response_user(
                    "2026-08-31T10:00:01.000Z",
                    "<environment_context>injected</environment_context>"
                ),
                serde_json::json!({
                    "timestamp": "2026-08-31T10:00:02.000Z",
                    "type": "response_item",
                    "payload": {
                        "type": "message", "role": "user", "id": "msg_user_1",
                        "content": [
                            {"type": "input_text", "text": "fix"},
                            {"type": "input_text", "text": "the scanner"}
                        ]
                    }
                }),
                r#"{"timestamp":"2026-08-31T10:00:02.100Z","type":"event_msg","payload":{"type":"item_completed"}}"#,
                r#"{"timestamp":"2026-08-31T10:00:03.000Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"duplicate assistant stream"}]}}"#,
                r#"{"timestamp":"2026-08-31T10:00:03.100Z","type":"event_msg","payload":{"type":"agent_message","message":"Done."}}"#,
            ),
        )
        .unwrap();
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();

        let outcome = super::ingest_codex_rollout(&conn, &path, &codex_meta(&path)).unwrap();
        assert_eq!(outcome.prompts, 1);
        assert_eq!(outcome.events, 2);
        assert_eq!(outcome.first_prompt.as_deref(), Some("fix\nthe scanner"));
        let rows: Vec<(String, String, String)> = conn
            .prepare("SELECT role, text, message_id FROM session_events ORDER BY ts_ms")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            rows,
            vec![
                (
                    "user".into(),
                    "fix\nthe scanner".into(),
                    "msg_user_1".into()
                ),
                ("assistant".into(), "Done.".into(), "5:agent_message".into()),
            ]
        );
    }

    #[test]
    fn codex_user_mirrors_dedupe_but_repeated_turns_survive() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rollout-mirrors.jsonl");
        fs::write(
            &path,
            format!(
                "{}\n{}\n{}\n{}\n{}\n",
                r#"{"timestamp":"2026-08-31T10:00:00.000Z","type":"session_meta","payload":{"id":"sess-mirror","cwd":"/tmp/proj"}}"#,
                response_user("2026-08-31T10:00:01.000Z", "retry"),
                r#"{"timestamp":"2026-08-31T10:00:01.000Z","type":"event_msg","payload":{"type":"user_message","message":"retry"}}"#,
                r#"{"timestamp":"2026-08-31T10:00:02.000Z","type":"event_msg","payload":{"type":"agent_message","message":"Try one."}}"#,
                response_user("2026-08-31T10:00:03.000Z", "retry"),
            ),
        )
        .unwrap();
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();

        let outcome = super::ingest_codex_rollout(&conn, &path, &codex_meta(&path)).unwrap();
        assert_eq!(outcome.prompts, 2);
        let user_rows: Vec<(i64, String)> = conn
            .prepare("SELECT ts_ms, text FROM session_events WHERE role='user' ORDER BY ts_ms")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(user_rows.len(), 2);
        assert_eq!(user_rows[0].1, "retry");
        assert_eq!(user_rows[1].1, "retry");
    }

    #[test]
    fn ingests_codex_rollout_events_tools_edits_and_token_deltas() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rollout-rich.jsonl");
        write_rich_codex_rollout(&path);
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();

        let meta = codex_meta(&path);
        assert_eq!(meta.session_id, "sess-top");
        assert_eq!(meta.cwd, "/tmp/proj");
        assert_eq!(meta.git_branch.as_deref(), Some("main"));
        assert!(!meta.is_subagent);

        let outcome = super::ingest_codex_rollout(&conn, &path, &meta).unwrap();
        assert_eq!(outcome.prompts, 1);
        assert_eq!(outcome.events, 6);
        assert_eq!(
            outcome.last_assistant_text.as_deref(),
            Some("Done. The importer is fixed.")
        );
        assert_eq!(outcome.first_ts, Some(1_785_578_400_000));

        let kinds: Vec<(String, String, Option<String>)> = conn
            .prepare("SELECT role, kind, model FROM session_events ORDER BY id")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            kinds,
            vec![
                ("user".into(), "text".into(), None),
                (
                    "assistant".into(),
                    "thinking".into(),
                    Some("gpt-5.4".into())
                ),
                (
                    "assistant".into(),
                    "tool_use".into(),
                    Some("gpt-5.4".into())
                ),
                ("tool_result".into(), "tool_result".into(), None),
                (
                    "assistant".into(),
                    "tool_use".into(),
                    Some("gpt-5.4".into())
                ),
                ("assistant".into(), "text".into(), Some("gpt-5.4".into())),
            ]
        );

        // The boilerplate user message is filtered from both stores.
        let prompt_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM history WHERE source='codex'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(prompt_count, 1);

        // Request 1's cumulative snapshot lands on the tool_use event that
        // closed it; the duplicate snapshot adds nothing; request 2's delta
        // lands on the final assistant message.
        let first: String = conn
            .query_row(
                "SELECT token_json FROM session_events WHERE event_uid = '8:function_call'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let first: Value = serde_json::from_str(&first).unwrap();
        assert_eq!(first["input_tokens"], 1000);
        assert_eq!(first["cached_input_tokens"], 400);
        assert_eq!(first["output_tokens"], 120);
        assert_eq!(first["reasoning_output_tokens"], 30);
        let second: String = conn
            .query_row(
                "SELECT token_json FROM session_events WHERE event_uid = '14:agent_message'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let second: Value = serde_json::from_str(&second).unwrap();
        assert_eq!(second["input_tokens"], 600);
        assert_eq!(second["cached_input_tokens"], 500);
        assert_eq!(second["cache_write_input_tokens"], 50);
        assert_eq!(second["output_tokens"], 60);
        let token_rows: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_events WHERE token_json IS NOT NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(token_rows, 2);
        // Summing per-event deltas reproduces the session's final totals.
        let output_sum: i64 = conn
            .query_row(
                "SELECT SUM(json_extract(token_json, '$.output_tokens')) FROM session_events",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(output_sum, 180);

        let calls: Vec<(String, String, Option<String>, Option<i64>)> = conn
            .prepare("SELECT tool_use_id, name, target, is_error FROM tool_calls ORDER BY id")
            .unwrap()
            .query_map([], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            calls,
            vec![
                (
                    "call_1".into(),
                    "exec_command".into(),
                    Some("git status".into()),
                    None
                ),
                (
                    "call_2".into(),
                    "apply_patch".into(),
                    Some("/tmp/proj/README.md".into()),
                    Some(0),
                ),
            ]
        );

        let edit: (String, i64, i64, String) = conn
            .query_row(
                "SELECT file_path, lines_added, lines_removed, structured_patch_json FROM file_edits",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(edit.0, "/tmp/proj/README.md");
        assert_eq!((edit.1, edit.2), (1, 1));
        assert!(edit.3.contains("unified_diff"));
    }

    #[test]
    fn codex_rollout_reingest_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rollout-rich.jsonl");
        write_rich_codex_rollout(&path);
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let meta = codex_meta(&path);
        super::ingest_codex_rollout(&conn, &path, &meta).unwrap();
        super::ingest_codex_rollout(&conn, &path, &meta).unwrap();
        let counts: (i64, i64, i64, i64) = conn
            .query_row(
                "SELECT (SELECT COUNT(*) FROM session_events), (SELECT COUNT(*) FROM tool_calls), \
                 (SELECT COUNT(*) FROM file_edits), (SELECT COUNT(*) FROM history)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(counts, (6, 2, 1, 1));
    }

    #[test]
    fn codex_v5_repair_restores_users_without_duplicating_existing_evidence() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let day = home.join(".codex/sessions/2026/08/31");
        fs::create_dir_all(&day).unwrap();
        let path = day.join("rollout-2026-08-31T10-00-00-repair.jsonl");
        fs::write(
            &path,
            format!(
                "{}\n{}\n{}\n{}\n",
                r#"{"timestamp":"2026-08-31T10:00:00.000Z","type":"session_meta","payload":{"id":"sess-repair","cwd":"/tmp/proj"}}"#,
                response_user("2026-08-31T10:00:01.000Z", "restore my prompt"),
                r#"{"timestamp":"2026-08-31T10:00:02.000Z","type":"response_item","payload":{"type":"function_call","id":"fc_1","name":"exec_command","arguments":"{\"cmd\":\"git status\"}","call_id":"call_1"}}"#,
                r#"{"timestamp":"2026-08-31T10:00:03.000Z","type":"event_msg","payload":{"type":"agent_message","message":"Done."}}"#,
            ),
        )
        .unwrap();
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        conn.execute(
            "INSERT INTO sessions (session_id, source, cwd, first_activity_ms, last_activity_ms) \
             VALUES ('sess-repair', 'codex', '/tmp/proj', 1, 2)",
            [],
        )
        .unwrap();
        super::insert_session_event(
            &conn,
            "codex",
            "sess-repair",
            Some("/tmp/proj"),
            Some("/tmp/proj"),
            None,
            "3:agent_message",
            None,
            1_788_176_403_000,
            "assistant",
            "text",
            Some("Done."),
            None,
            None,
            "3:agent_message",
            super::RawMessageFacts::default(),
        )
        .unwrap();
        super::insert_tool_call(
            &conn,
            "codex",
            "sess-repair",
            "fc_1",
            "call_1",
            "exec_command",
            Some("git status"),
            r#"{"cmd":"git status"}"#,
            None,
            1_788_176_402_000,
        )
        .unwrap();

        let mut state = Map::new();
        state.insert(
            "codex_rollouts_v3".into(),
            json!({path.to_string_lossy().to_string(): {
                "stamp": super::file_stamp(&path).unwrap(),
                "session": "sess-repair"
            }}),
        );
        super::sync_codex_rollouts(&conn, &mut state, home).unwrap();
        let counts: (i64, i64, i64, Option<String>) = conn
            .query_row(
                "SELECT \
                 (SELECT COUNT(*) FROM history WHERE session_id='sess-repair'), \
                 (SELECT COUNT(*) FROM session_events WHERE session_id='sess-repair' AND role='user'), \
                 (SELECT COUNT(*) FROM session_events WHERE session_id='sess-repair' AND role='assistant'), \
                 (SELECT first_prompt FROM sessions WHERE session_id='sess-repair')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(counts, (1, 1, 2, Some("restore my prompt".into())));
        let tool_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM tool_calls WHERE session_id='sess-repair'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(tool_count, 1);
        assert!(state.get("codex_rollouts_v3").is_none());
        assert!(state.get("codex_rollouts_v5").is_some());

        super::sync_codex_rollouts(&conn, &mut state, home).unwrap();
        let second_counts: (i64, i64, i64, i64) = conn
            .query_row(
                "SELECT \
                 (SELECT COUNT(*) FROM history WHERE session_id='sess-repair'), \
                 (SELECT COUNT(*) FROM session_events WHERE session_id='sess-repair' AND role='user'), \
                 (SELECT COUNT(*) FROM session_events WHERE session_id='sess-repair' AND role='assistant'), \
                 (SELECT COUNT(*) FROM tool_calls WHERE session_id='sess-repair')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(second_counts, (1, 1, 2, 1));
    }

    #[test]
    fn resumed_codex_rollout_treats_first_snapshot_as_carried_baseline() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rollout-resumed.jsonl");
        fs::write(
            &path,
            concat!(
                r#"{"timestamp":"2026-08-02T09:00:00.000Z","type":"session_meta","payload":{"id":"sess-resumed","cwd":"/tmp/proj"}}"#, "\n",
                r#"{"timestamp":"2026-08-02T09:00:00.100Z","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":1000,"cached_input_tokens":0,"cache_write_input_tokens":0,"output_tokens":120,"reasoning_output_tokens":0,"total_tokens":1120}}}}"#, "\n",
                r#"{"timestamp":"2026-08-02T09:00:01.000Z","type":"event_msg","payload":{"type":"agent_message","message":"Picking the work back up."}}"#, "\n",
                r#"{"timestamp":"2026-08-02T09:00:01.500Z","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":40,"cached_input_tokens":0,"cache_write_input_tokens":0,"output_tokens":4,"reasoning_output_tokens":0,"total_tokens":44}}}}"#, "\n",
                r#"{"timestamp":"2026-08-02T09:00:02.000Z","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":1500,"cached_input_tokens":0,"cache_write_input_tokens":0,"output_tokens":150,"reasoning_output_tokens":0,"total_tokens":1650}}}}"#, "\n",
            ),
        )
        .unwrap();
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        super::ingest_codex_rollout(&conn, &path, &codex_meta(&path)).unwrap();
        let token_json: String = conn
            .query_row(
                "SELECT token_json FROM session_events WHERE token_json IS NOT NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let delta: Value = serde_json::from_str(&token_json).unwrap();
        assert_eq!(delta["input_tokens"], 500);
        assert_eq!(delta["output_tokens"], 30);
    }

    #[test]
    fn codex_rollout_ignores_a_half_written_trailing_line() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rollout-torn.jsonl");
        fs::write(
            &path,
            concat!(
                r#"{"timestamp":"2026-08-02T09:00:00.000Z","type":"session_meta","payload":{"id":"sess-torn","cwd":"/tmp/proj"}}"#, "\n",
                r#"{"timestamp":"2026-08-02T09:00:01.000Z","type":"event_msg","payload":{"type":"user_message","message":"first"}}"#, "\n",
                r#"{"timestamp":"2026-08-02T09:00:02.000Z","type":"event_msg","payload":{"type":"user_message","message":"tor"#,
            ),
        )
        .unwrap();
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let outcome = super::ingest_codex_rollout(&conn, &path, &codex_meta(&path)).unwrap();
        assert_eq!(outcome.events, 1);
        let text: String = conn
            .query_row("SELECT text FROM session_events", [], |r| r.get(0))
            .unwrap();
        assert_eq!(text, "first");
    }

    fn write_codex_subagent_rollout(path: &std::path::Path, session_id: &str) {
        fs::write(
            path,
            format!(
                concat!(
                    r#"{{"timestamp":"2026-08-01T10:01:00.000Z","type":"session_meta","payload":{{"id":"{}","session_id":"parent","parent_thread_id":"parent","thread_source":"subagent","source":{{"subagent":{{"other":"guardian"}}}},"cwd":"/tmp/proj"}}}}"#,
                    "\n",
                    r#"{{"timestamp":"2026-08-01T10:01:01.000Z","type":"event_msg","payload":{{"type":"agent_message","message":"Reviewing."}}}}"#,
                    "\n",
                ),
                session_id
            ),
        )
        .unwrap();
    }

    fn write_standalone_codex_guardian_rollout(path: &std::path::Path, session_id: &str) {
        let lines = [
            json!({
                "timestamp": "2026-08-01T10:02:00.000Z",
                "type": "session_meta",
                "payload": {
                    "id": session_id,
                    "cwd": "/tmp/proj",
                    "source": {"subagent": {"other": "guardian"}}
                }
            }),
            json!({
                "timestamp": "2026-08-01T10:02:01.000Z",
                "type": "event_msg",
                "payload": {"type": "user_message", "message": "standalone guardian prompt"}
            }),
            json!({
                "timestamp": "2026-08-01T10:02:02.000Z",
                "type": "event_msg",
                "payload": {"type": "agent_message", "message": "standalone guardian answer"}
            }),
        ];
        let content = lines
            .into_iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        fs::write(path, content).unwrap();
    }

    #[test]
    fn unchanged_subagent_state_still_repairs_migrated_local_catalog_rows() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let day = home.join(".codex/sessions/2026/08/01");
        fs::create_dir_all(&day).unwrap();
        let rollout = day.join("rollout-unchanged-sub.jsonl");
        write_codex_subagent_rollout(&rollout, "sess-unchanged-sub");

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        conn.execute(
            "INSERT INTO sessions (session_id, source, cwd) \
             VALUES ('sess-unchanged-sub', 'codex', '/tmp/proj')",
            [],
        )
        .unwrap();
        crate::mark_session_presence(
            &conn,
            "codex",
            "sess-unchanged-sub",
            super::SessionLocation::Local,
        )
        .unwrap();
        conn.execute(
            "INSERT INTO history (source, session_id, prompt, timestamp_ms) \
             VALUES ('codex', 'sess-unchanged-sub', 'stale task', 1)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, ts_ms, role, kind, text, event_uid, raw_facts_version) \
             VALUES ('codex', 'sess-unchanged-sub', 2, 'assistant', 'text', 'kept event', 'event-1', ?)",
            [super::RAW_MESSAGE_FACTS_VERSION],
        )
        .unwrap();

        let key = rollout.to_string_lossy().to_string();
        let mut state = Map::new();
        state.insert(
            "codex_rollouts_v5".into(),
            json!({
                (key): {
                    "stamp": file_stamp(&rollout).unwrap(),
                    "session": "sess-unchanged-sub",
                    "subagent": true
                }
            }),
        );
        state.insert(
            "codex_session_cwds".into(),
            json!({"sess-unchanged-sub": "/tmp/proj"}),
        );
        state.insert(
            "codex_session_branches".into(),
            json!({"sess-unchanged-sub": "main"}),
        );

        let (cwds, branches, inserted) =
            super::sync_codex_rollouts(&conn, &mut state, home).unwrap();
        assert_eq!(inserted, 0);
        assert!(!cwds.contains_key("sess-unchanged-sub"));
        assert!(!branches.contains_key("sess-unchanged-sub"));
        let stale_rows: i64 = conn
            .query_row(
                "SELECT \
                   (SELECT COUNT(*) FROM sessions WHERE source='codex' AND session_id='sess-unchanged-sub') + \
                   (SELECT COUNT(*) FROM session_presences WHERE source='codex' AND session_id='sess-unchanged-sub') + \
                   (SELECT COUNT(*) FROM history WHERE source='codex' AND session_id='sess-unchanged-sub')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stale_rows, 0);
        let events: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_events \
                 WHERE source='codex' AND session_id='sess-unchanged-sub'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(events, 1);
    }

    /// State that makes `sync_codex_rollouts` take its stamp-unchanged fast
    /// path for one already-ingested subagent rollout.
    fn unchanged_subagent_state(rollout: &std::path::Path, session_id: &str) -> Map<String, Value> {
        let mut state = Map::new();
        state.insert(
            "codex_rollouts_v4".into(),
            json!({
                rollout.to_string_lossy().to_string(): {
                    "stamp": file_stamp(rollout).unwrap(),
                    "session": session_id,
                    "subagent": true
                }
            }),
        );
        state
    }

    #[test]
    fn unchanged_subagent_state_backfills_missing_delegation_rows() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let day = home.join(".codex/sessions/2026/08/01");
        fs::create_dir_all(&day).unwrap();
        let rollout = day.join("rollout-unchanged-sub.jsonl");
        write_codex_subagent_rollout(&rollout, "sess-unchanged-sub");

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        // A database synced before delegation was recorded: the events are
        // already there, so the rollout is never re-read for its own sake.
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, ts_ms, role, kind, text, event_uid, raw_facts_version) \
             VALUES ('codex', 'sess-unchanged-sub', 2, 'assistant', 'text', 'kept event', 'event-1', ?)",
            [super::RAW_MESSAGE_FACTS_VERSION],
        )
        .unwrap();
        let mut state = unchanged_subagent_state(&rollout, "sess-unchanged-sub");

        super::sync_codex_rollouts(&conn, &mut state, home).unwrap();
        let edge: (String, String, String, i64) = conn
            .query_row(
                "SELECT parent_session_id, relationship_uid, evidence_kind, created_ms \
                 FROM session_relationships \
                 WHERE source='codex' AND child_session_id='sess-unchanged-sub'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(edge.0, "parent");
        assert_eq!(edge.1, "child:sess-unchanged-sub");
        assert_eq!(edge.2, "codex_session_meta");

        // A second pass over the same unchanged state neither duplicates the
        // row nor re-reads the rollout to rewrite it.
        super::sync_codex_rollouts(&conn, &mut state, home).unwrap();
        let (count, created): (i64, i64) = conn
            .query_row(
                "SELECT COUNT(*), MIN(created_ms) FROM session_relationships \
                 WHERE source='codex' AND child_session_id='sess-unchanged-sub'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(created, edge.3);
    }

    #[test]
    fn subagent_registration_cleanup_keeps_the_thread_s_own_delegations() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let day = home.join(".codex/sessions/2026/08/01");
        fs::create_dir_all(&day).unwrap();
        let rollout = day.join("rollout-unchanged-sub.jsonl");
        write_codex_subagent_rollout(&rollout, "sess-unchanged-sub");

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        // A stale catalog registration for the subagent, which the fast path
        // removes — firing the cascade that used to take the topology with it.
        conn.execute(
            "INSERT INTO sessions (session_id, source, cwd) \
             VALUES ('sess-unchanged-sub', 'codex', '/tmp/proj')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, ts_ms, role, kind, text, event_uid, raw_facts_version) \
             VALUES ('codex', 'sess-unchanged-sub', 2, 'assistant', 'text', 'kept event', 'event-1', ?)",
            [super::RAW_MESSAGE_FACTS_VERSION],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_relationships \
             (source, parent_session_id, relationship_uid, child_session_id, relationship, \
              identity_status, evidence_kind, child_has_events, created_ms, updated_ms) \
             VALUES ('codex', 'sess-unchanged-sub', 'child:grandchild', 'grandchild', 'delegated', \
                     'observed', 'codex_session_meta', 1, 1, 1)",
            [],
        )
        .unwrap();
        let mut state = unchanged_subagent_state(&rollout, "sess-unchanged-sub");

        super::sync_codex_rollouts(&conn, &mut state, home).unwrap();
        let registrations: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sessions \
                 WHERE source='codex' AND session_id='sess-unchanged-sub'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(registrations, 0);
        let edges: Vec<String> = conn
            .prepare(
                "SELECT relationship_uid FROM session_relationships \
                 WHERE source='codex' AND parent_session_id='sess-unchanged-sub'",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(edges, vec!["child:grandchild".to_string()]);
    }

    #[test]
    fn old_codex_cache_reclassifies_standalone_guardian_before_fast_path() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let day = home.join(".codex/sessions/2026/08/01");
        fs::create_dir_all(&day).unwrap();
        let root = day.join("rollout-root.jsonl");
        let standalone = day.join("rollout-standalone-guardian.jsonl");
        let linked = day.join("rollout-linked-guardian.jsonl");
        write_rich_codex_rollout(&root);
        write_standalone_codex_guardian_rollout(&standalone, "sess-standalone-guardian");
        write_codex_subagent_rollout(&linked, "sess-linked-guardian");

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        // Existing events make the old v4 entries eligible for the fast path.
        // They carry the current raw-facts generation so this test exercises the
        // guardian reclassification and not the raw-facts backfill.
        for session_id in [
            "sess-top",
            "sess-standalone-guardian",
            "sess-linked-guardian",
        ] {
            conn.execute(
                "INSERT INTO session_events \
                 (source, session_id, ts_ms, role, kind, text, event_uid, raw_facts_version) \
                 VALUES ('codex', ?, 2, 'assistant', 'text', 'retained event', ?, ?)",
                rusqlite::params![
                    session_id,
                    format!("retained-{session_id}"),
                    super::RAW_MESSAGE_FACTS_VERSION
                ],
            )
            .unwrap();
        }
        // The normal root has an existing catalog row and should stay on the
        // stamp fast path during this targeted migration.
        conn.execute(
            "INSERT INTO sessions (session_id, source, cwd) \
             VALUES ('sess-top', 'codex', '/tmp/proj')",
            [],
        )
        .unwrap();
        // The linked child also has stale catalog registration from the old
        // classifier; the upgrade must remove it rather than promote it.
        conn.execute(
            "INSERT INTO sessions (session_id, source, cwd) \
             VALUES ('sess-linked-guardian', 'codex', '/tmp/proj')",
            [],
        )
        .unwrap();
        crate::mark_session_presence(
            &conn,
            "codex",
            "sess-linked-guardian",
            super::SessionLocation::Local,
        )
        .unwrap();
        conn.execute(
            "INSERT INTO history (source, session_id, prompt, timestamp_ms) \
             VALUES ('codex', 'sess-linked-guardian', 'stale child prompt', 1)",
            [],
        )
        .unwrap();

        let mut state = Map::new();
        let standalone_key = standalone.to_string_lossy().to_string();
        let linked_key = linked.to_string_lossy().to_string();
        state.insert(
            "codex_rollouts_v4".into(),
            json!({
                (root.to_string_lossy().to_string()): {
                    "stamp": super::file_stamp(&root).unwrap(),
                    "session": "sess-top",
                    "subagent": false
                },
                (standalone_key.clone()): {
                    "stamp": super::file_stamp(&standalone).unwrap(),
                    "session": "sess-standalone-guardian",
                    "subagent": true
                },
                (linked_key.clone()): {
                    "stamp": super::file_stamp(&linked).unwrap(),
                    "session": "sess-linked-guardian",
                    "subagent": true
                }
            }),
        );
        state.insert(
            "codex_session_cwds".into(),
            json!({
                "sess-top": "/tmp/proj",
                "sess-standalone-guardian": "/tmp/proj",
                "sess-linked-guardian": "/tmp/proj"
            }),
        );

        let (_, _, inserted) = super::sync_codex_rollouts(&conn, &mut state, home).unwrap();
        assert_eq!(inserted, 1, "standalone guardian prompt is newly indexed");
        let sessions: Vec<String> = conn
            .prepare("SELECT session_id FROM sessions WHERE source='codex' ORDER BY session_id")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(sessions, vec!["sess-standalone-guardian", "sess-top"]);
        let standalone_presence: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_presences \
                 WHERE source='codex' AND session_id='sess-standalone-guardian' AND location='local'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(standalone_presence, 1);
        let linked_history: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM history \
                 WHERE source='codex' AND session_id='sess-linked-guardian'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(linked_history, 0);
        let root_history: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM history WHERE source='codex' AND session_id='sess-top'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            root_history, 0,
            "the unchanged root stayed on the fast path"
        );
        assert!(state.get("codex_rollouts_v4").is_none());
        let records = state
            .get("codex_rollouts_v5")
            .and_then(Value::as_object)
            .expect("upgraded rollout cache");
        assert_eq!(
            records
                .get(standalone_key.as_str())
                .and_then(|record| record.get("subagent")),
            Some(&Value::Bool(false))
        );
        assert_eq!(
            records
                .get(linked_key.as_str())
                .and_then(|record| record.get("subagent")),
            Some(&Value::Bool(true))
        );
    }

    #[test]
    fn local_subagent_sync_preserves_history_for_a_remote_canonical_session() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let day = home.join(".codex/sessions/2026/08/01");
        fs::create_dir_all(&day).unwrap();
        let rollout = day.join("rollout-remote-sub.jsonl");
        write_codex_subagent_rollout(&rollout, "sess-remote-sub");

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        conn.execute(
            "INSERT INTO sessions (session_id, source, cwd) \
             VALUES ('sess-remote-sub', 'codex', '/remote/project')",
            [],
        )
        .unwrap();
        crate::insert_history_at_location(
            &conn,
            &HistoryEntry {
                id: 0,
                source: "codex".into(),
                session_id: Some("sess-remote-sub".into()),
                project: Some("/remote/project".into()),
                prompt: "remote canonical prompt".into(),
                prompt_hash: None,
                timestamp_ms: 1,
            },
            super::SessionLocation::Remote,
        )
        .unwrap();

        super::sync_codex_rollouts(&conn, &mut Map::new(), home).unwrap();

        let prompts: Vec<String> = conn
            .prepare(
                "SELECT prompt FROM history \
                 WHERE source='codex' AND session_id='sess-remote-sub'",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(prompts, vec!["remote canonical prompt"]);
        let locations: Vec<String> = conn
            .prepare(
                "SELECT location FROM session_presences \
                 WHERE source='codex' AND session_id='sess-remote-sub' ORDER BY location",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(locations, vec!["local", "remote"]);
    }

    #[test]
    fn failed_subagent_ingest_still_cleans_local_catalog_registration() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let day = home.join(".codex/sessions/2026/08/01");
        fs::create_dir_all(&day).unwrap();
        let rollout = day.join("rollout-failing-sub.jsonl");
        write_codex_subagent_rollout(&rollout, "sess-failing-sub");

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        conn.execute(
            "INSERT INTO sessions (session_id, source) VALUES ('sess-failing-sub', 'codex')",
            [],
        )
        .unwrap();
        crate::mark_session_presence(
            &conn,
            "codex",
            "sess-failing-sub",
            super::SessionLocation::Local,
        )
        .unwrap();
        conn.execute_batch(
            "CREATE TRIGGER reject_subagent_event \
             BEFORE INSERT ON session_events \
             BEGIN SELECT RAISE(FAIL, 'forced subagent ingest failure'); END;",
        )
        .unwrap();

        let error = super::sync_codex_rollouts(&conn, &mut Map::new(), home)
            .expect_err("the trigger must fail ingestion");
        assert!(error.to_string().contains("forced subagent ingest failure"));
        let stale_rows: i64 = conn
            .query_row(
                "SELECT \
                   (SELECT COUNT(*) FROM sessions WHERE source='codex' AND session_id='sess-failing-sub') + \
                   (SELECT COUNT(*) FROM session_presences WHERE source='codex' AND session_id='sess-failing-sub')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stale_rows, 0);
    }

    #[test]
    fn subagent_rollouts_keep_events_but_stay_out_of_the_session_list() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let day = home.join(".codex/sessions/2026/08/01");
        fs::create_dir_all(&day).unwrap();
        fs::write(
            day.join("rollout-2026-08-01T10-00-00-top.jsonl"),
            concat!(
                r#"{"timestamp":"2026-08-01T10:00:00.000Z","type":"session_meta","payload":{"id":"sess-top","cwd":"/tmp/proj"}}"#, "\n",
                r#"{"timestamp":"2026-08-01T10:00:01.000Z","type":"event_msg","payload":{"type":"user_message","message":"do the thing"}}"#, "\n",
                r#"{"timestamp":"2026-08-01T10:00:02.000Z","type":"event_msg","payload":{"type":"agent_message","message":"Doing it."}}"#, "\n",
            ),
        )
        .unwrap();
        fs::write(
            day.join("rollout-2026-08-01T10-01-00-sub.jsonl"),
            concat!(
                r#"{"timestamp":"2026-08-01T10:01:00.000Z","type":"session_meta","payload":{"id":"sess-sub","session_id":"sess-top","parent_thread_id":"sess-top","thread_source":"subagent","source":{"subagent":{"other":"guardian"}},"cwd":"/tmp/proj"}}"#, "\n",
                r#"{"timestamp":"2026-08-01T10:01:00.500Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"Review the implementation."}]}}"#, "\n",
                r#"{"timestamp":"2026-08-01T10:01:01.000Z","type":"event_msg","payload":{"type":"agent_message","message":"Reviewing."}}"#, "\n",
            ),
        )
        .unwrap();

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        // Stale keys from the split-walk era are dropped from state.
        state.insert("codex_rollouts".into(), json!({"old": "1:1"}));
        state.insert(
            "codex_rollout_user_messages_v2".into(),
            json!({"old": "1:1"}),
        );
        // A sync predating subagent detection left the thread registered:
        // a cwd-map entry, a prompt row, and a session row.
        state.insert(
            "codex_session_cwds".into(),
            json!({"sess-sub": "/tmp/proj"}),
        );
        conn.execute(
            "INSERT INTO history (source, session_id, project, prompt, timestamp_ms) VALUES ('codex', 'sess-sub', '/tmp/proj', 'do a review', 1)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO sessions (session_id, source, cwd) VALUES ('sess-sub', 'codex', '/tmp/proj')",
            [],
        )
        .unwrap();
        let (cwds, _, inserted) = super::sync_codex_rollouts(&conn, &mut state, home).unwrap();
        assert_eq!(inserted, 1);
        // Subagent threads never reach the maps, prompt history, or session
        // registration — including rows left behind by earlier syncs.
        assert_eq!(cwds.get("sess-sub"), None);
        let sub_history: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM history WHERE session_id = 'sess-sub'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(sub_history, 0);
        assert!(state.get("codex_rollouts").is_none());
        assert!(state.get("codex_rollout_user_messages_v2").is_none());

        let sessions: Vec<String> = conn
            .prepare("SELECT session_id FROM sessions WHERE source='codex' ORDER BY session_id")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(sessions, vec!["sess-top".to_string()]);
        let event_sessions: Vec<String> = conn
            .prepare("SELECT DISTINCT session_id FROM session_events ORDER BY session_id")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            event_sessions,
            vec!["sess-sub".to_string(), "sess-top".to_string()]
        );
        let sub_user_events: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_events WHERE session_id='sess-sub' AND role='user'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(sub_user_events, 1);

        // A second walk with unchanged stamps ingests nothing new.
        let before: i64 = conn
            .query_row("SELECT COUNT(*) FROM session_events", [], |r| r.get(0))
            .unwrap();
        let (_, _, inserted_again) = super::sync_codex_rollouts(&conn, &mut state, home).unwrap();
        assert_eq!(inserted_again, 0);
        let after: i64 = conn
            .query_row("SELECT COUNT(*) FROM session_events", [], |r| r.get(0))
            .unwrap();
        assert_eq!(before, after);
    }

    #[test]
    fn sidechain_rows_keep_assistant_output_but_drop_fake_user_turns() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agent-sub.jsonl");
        fs::write(
            &path,
            concat!(
                r#"{"type":"user","uuid":"su1","sessionId":"s-parent","isSidechain":true,"cwd":"/tmp/proj","timestamp":"2026-06-25T10:00:00.000Z","message":{"role":"user","content":"Research the repo thoroughly."}}"#, "\n",
                r#"{"type":"assistant","uuid":"sa1","sessionId":"s-parent","isSidechain":true,"cwd":"/tmp/proj","timestamp":"2026-06-25T10:01:00.000Z","message":{"id":"msg_sub","role":"assistant","model":"claude-opus-5","usage":{"input_tokens":10,"output_tokens":20},"content":[{"type":"text","text":"Here is the report."}]}}"#, "\n",
            ),
        )
        .unwrap();
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        ingest_claude_transcript(&conn, &path).unwrap();
        let rows: Vec<(String, String)> = conn
            .prepare("SELECT role, text FROM session_events ORDER BY id")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            rows,
            vec![("assistant".into(), "Here is the report.".into())]
        );
    }

    /// Envelope facts burn needs per message: which API request a turn belongs
    /// to, which harness build wrote it, whether the row is delegated, and why
    /// the model stopped. Shapes follow a real Claude transcript: one request
    /// id spans several `uuid`s that share a `message.id`, and only the block
    /// that ends the turn carries a `stop_reason`.
    #[test]
    fn claude_records_carry_request_id_stop_reason_agent_version_and_flags() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sess-facts.jsonl");
        fs::write(
            &path,
            concat!(
                r#"{"parentUuid":null,"isSidechain":false,"type":"user","message":{"role":"user","content":"check the repo"},"uuid":"u-user-1","timestamp":"2026-04-20T00:00:00.000Z","cwd":"/tmp/project","sessionId":"s-facts","version":"2.1.96"}"#, "\n",
                r#"{"parentUuid":"u-user-1","isSidechain":false,"message":{"model":"claude-opus-4-7","id":"msg_multi_1","role":"assistant","content":[{"type":"text","text":"Let me check."}],"stop_reason":null,"usage":{"input_tokens":3,"output_tokens":43}},"requestId":"req_1","type":"assistant","uuid":"u-asst-1b","timestamp":"2026-04-20T00:00:01.500Z","cwd":"/tmp/project","sessionId":"s-facts","version":"2.1.96"}"#, "\n",
                r#"{"parentUuid":"u-asst-1b","isSidechain":false,"message":{"model":"claude-opus-4-7","id":"msg_multi_1","role":"assistant","content":[{"type":"tool_use","id":"toolu_bash_1","name":"Bash","input":{"command":"ls -la"}}],"stop_reason":"tool_use","usage":{"input_tokens":3,"output_tokens":43}},"requestId":"req_1","type":"assistant","uuid":"u-asst-1c","timestamp":"2026-04-20T00:00:02.000Z","cwd":"/tmp/project","sessionId":"s-facts","version":"2.1.96"}"#, "\n",
                r#"{"parentUuid":"u-asst-1c","isSidechain":false,"type":"user","isMeta":true,"message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_bash_1","content":"a.txt"}]},"requestId":"req_2","uuid":"u-result-1","timestamp":"2026-04-20T00:00:03.000Z","cwd":"/tmp/project","sessionId":"s-facts","version":"2.1.97"}"#, "\n",
            ),
        )
        .unwrap();
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        ingest_claude_transcript(&conn, &path).unwrap();

        type FactRow = (
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<i64>,
            Option<i64>,
        );
        let rows: Vec<FactRow> = conn
            .prepare(
                "SELECT event_uid, request_id, stop_reason, agent_version, is_sidechain, is_meta \
                 FROM session_events ORDER BY id",
            )
            .unwrap()
            .query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();

        assert_eq!(
            rows,
            vec![
                // The opening human turn predates the first API request.
                (
                    "u-user-1:0".into(),
                    None,
                    None,
                    Some("2.1.96".into()),
                    Some(0),
                    None
                ),
                // Both blocks of the same request share `req_1`; only the one
                // that ended the turn carries a stop reason. A JSON null must
                // stay null -- burn reads its absence as "still in flight".
                (
                    "u-asst-1b:0".into(),
                    Some("req_1".into()),
                    None,
                    Some("2.1.96".into()),
                    Some(0),
                    None
                ),
                (
                    "u-asst-1c:0".into(),
                    Some("req_1".into()),
                    Some("tool_use".into()),
                    Some("2.1.96".into()),
                    Some(0),
                    None
                ),
                (
                    "u-result-1:0".into(),
                    Some("req_2".into()),
                    None,
                    Some("2.1.97".into()),
                    Some(0),
                    Some(1)
                ),
            ]
        );
    }

    /// `sourceVersion` is the older spelling of the harness build, and a
    /// sidechain record's assistant output is the one row a delegated
    /// transcript contributes: it must be marked as delegated, not as the
    /// parent's own work.
    #[test]
    fn claude_sidechain_rows_are_flagged_and_source_version_is_accepted() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sess-side.jsonl");
        fs::write(
            &path,
            concat!(
                r#"{"type":"assistant","uuid":"sa1","sessionId":"s-side","isSidechain":true,"cwd":"/tmp/proj","timestamp":"2026-06-25T10:01:00.000Z","sourceVersion":"1.0.88","requestId":"req_side","message":{"id":"msg_sub","role":"assistant","model":"claude-opus-5","stop_reason":"end_turn","content":[{"type":"text","text":"Here is the report."}]}}"#, "\n",
            ),
        )
        .unwrap();
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        ingest_claude_transcript(&conn, &path).unwrap();

        let row: (Option<String>, Option<String>, Option<String>, Option<i64>) = conn
            .query_row(
                "SELECT request_id, stop_reason, agent_version, is_sidechain FROM session_events",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(
            row,
            (
                Some("req_side".into()),
                Some("end_turn".into()),
                Some("1.0.88".into()),
                Some(1)
            )
        );
    }

    /// `token_json` is the provider's `message.usage` object stored verbatim,
    /// so the nested ephemeral cache buckets pricing depends on survive a round
    /// trip. A usage shape flattened to the fields this crate happens to name
    /// would silently lose them.
    #[test]
    fn claude_usage_round_trips_nested_ephemeral_cache_creation_buckets() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sess-usage.jsonl");
        fs::write(
            &path,
            concat!(
                r#"{"parentUuid":null,"isSidechain":false,"message":{"model":"claude-opus-4-7","id":"msg_usage","role":"assistant","content":[{"type":"text","text":"Let me check."}],"stop_reason":"end_turn","usage":{"input_tokens":3,"cache_creation_input_tokens":4773,"cache_read_input_tokens":11496,"cache_creation":{"ephemeral_5m_input_tokens":771,"ephemeral_1h_input_tokens":4002},"output_tokens":43,"service_tier":"standard"}},"requestId":"req_1","type":"assistant","uuid":"u-asst-usage","timestamp":"2026-04-20T00:00:01.000Z","cwd":"/tmp/project","sessionId":"s-usage","version":"2.1.96"}"#, "\n",
            ),
        )
        .unwrap();
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        ingest_claude_transcript(&conn, &path).unwrap();

        let token_json: String = conn
            .query_row("SELECT token_json FROM session_events", [], |row| {
                row.get(0)
            })
            .unwrap();
        let usage: Value = serde_json::from_str(&token_json).unwrap();
        assert_eq!(usage["cache_creation"]["ephemeral_5m_input_tokens"], 771);
        assert_eq!(usage["cache_creation"]["ephemeral_1h_input_tokens"], 4002);
        assert_eq!(usage["cache_creation_input_tokens"], 4773);
        assert_eq!(usage["output_tokens"], 43);
        // The same values are reachable through SQL, which is how a consumer
        // that does not deserialize the whole object reads them.
        let ephemeral_1h: i64 = conn
            .query_row(
                "SELECT json_extract(token_json, '$.cache_creation.ephemeral_1h_input_tokens') \
                 FROM session_events",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(ephemeral_1h, 4002);
    }

    /// Codex names a turn once, in `turn_context`, and every later record
    /// belongs to it until the next one. Carrying it forward is what makes the
    /// turn boundary recoverable from the stored events.
    #[test]
    fn codex_turn_ids_are_stamped_until_the_next_turn_context() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rollout-turns.jsonl");
        fs::write(
            &path,
            concat!(
                r#"{"timestamp":"2026-04-20T02:00:00.000Z","type":"session_meta","payload":{"id":"sess-turns","cwd":"/tmp/proj"}}"#, "\n",
                r#"{"timestamp":"2026-04-20T02:00:00.100Z","type":"turn_context","payload":{"turn_id":"turn_multi_1","cwd":"/tmp/proj","model":"gpt-5.4"}}"#, "\n",
                r#"{"timestamp":"2026-04-20T02:00:01.000Z","type":"event_msg","payload":{"type":"user_message","message":"first task"}}"#, "\n",
                r#"{"timestamp":"2026-04-20T02:00:02.000Z","type":"event_msg","payload":{"type":"agent_message","message":"On it."}}"#, "\n",
                r#"{"timestamp":"2026-04-20T02:00:10.000Z","type":"turn_context","payload":{"turn_id":"turn_multi_2","cwd":"/tmp/proj","model":"gpt-5.3-codex"}}"#, "\n",
                r#"{"timestamp":"2026-04-20T02:00:11.000Z","type":"event_msg","payload":{"type":"agent_reasoning","text":"Considering."}}"#, "\n",
                r#"{"timestamp":"2026-04-20T02:00:12.000Z","type":"response_item","payload":{"type":"function_call","name":"shell","call_id":"call_1","arguments":"{\"command\":\"ls\"}"}}"#, "\n",
                r#"{"timestamp":"2026-04-20T02:00:13.000Z","type":"response_item","payload":{"type":"function_call_output","call_id":"call_1","output":"a.txt"}}"#, "\n",
            ),
        )
        .unwrap();
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        super::ingest_codex_rollout(&conn, &path, &codex_meta(&path)).unwrap();

        let rows: Vec<(String, Option<String>)> = conn
            .prepare("SELECT event_uid, turn_id FROM session_events ORDER BY id")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            rows,
            vec![
                ("2:user_message".into(), Some("turn_multi_1".into())),
                ("3:agent_message".into(), Some("turn_multi_1".into())),
                ("5:agent_reasoning".into(), Some("turn_multi_2".into())),
                ("6:function_call".into(), Some("turn_multi_2".into())),
                ("7:function_call_output".into(), Some("turn_multi_2".into())),
            ]
        );
    }

    /// Records before any `turn_context`, and after one that names no turn,
    /// have no turn to belong to. Stamping the last seen id onto them would
    /// invent a boundary the provider never recorded.
    #[test]
    fn codex_events_outside_a_named_turn_have_no_turn_id() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rollout-unnamed.jsonl");
        fs::write(
            &path,
            concat!(
                r#"{"timestamp":"2026-04-20T02:00:00.000Z","type":"session_meta","payload":{"id":"sess-unnamed","cwd":"/tmp/proj"}}"#, "\n",
                r#"{"timestamp":"2026-04-20T02:00:01.000Z","type":"event_msg","payload":{"type":"agent_message","message":"Before any turn."}}"#, "\n",
                r#"{"timestamp":"2026-04-20T02:00:02.000Z","type":"turn_context","payload":{"turn_id":"turn_1","cwd":"/tmp/proj","model":"gpt-5.4"}}"#, "\n",
                r#"{"timestamp":"2026-04-20T02:00:03.000Z","type":"event_msg","payload":{"type":"agent_message","message":"Inside turn one."}}"#, "\n",
                r#"{"timestamp":"2026-04-20T02:00:04.000Z","type":"turn_context","payload":{"cwd":"/tmp/proj","model":"gpt-5.4"}}"#, "\n",
                r#"{"timestamp":"2026-04-20T02:00:05.000Z","type":"event_msg","payload":{"type":"agent_message","message":"After an unnamed turn."}}"#, "\n",
            ),
        )
        .unwrap();
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        super::ingest_codex_rollout(&conn, &path, &codex_meta(&path)).unwrap();

        let rows: Vec<(String, Option<String>)> = conn
            .prepare("SELECT text, turn_id FROM session_events ORDER BY id")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            rows,
            vec![
                ("Before any turn.".into(), None),
                ("Inside turn one.".into(), Some("turn_1".into())),
                ("After an unnamed turn.".into(), None),
            ]
        );
    }

    /// OpenCode records the stop reason on a `step-finish` part rather than on
    /// the message, and writes it as its own wire string. The mapping hook is
    /// landed here; wiring it to real OpenCode events waits on the OpenCode
    /// parity work (#168), which is why the end-to-end assertion below is
    /// ignored rather than absent.
    #[test]
    fn opencode_step_finish_reason_is_read_verbatim() {
        assert_eq!(
            super::opencode_step_finish_stop_reason(
                &json!({"type": "step-finish", "reason": "tool-calls"})
            ),
            Some("tool-calls")
        );
        assert_eq!(
            super::opencode_step_finish_stop_reason(&json!({"type": "step-finish"})),
            None
        );
        // Another part type never contributes a stop reason, whatever it
        // happens to carry under that key.
        assert_eq!(
            super::opencode_step_finish_stop_reason(
                &json!({"type": "text", "reason": "not-a-stop-reason"})
            ),
            None
        );
    }

    /// The end-to-end half of the OpenCode stop-reason contract. OpenCode sync
    /// ingests prompt history only today, so no assistant event exists to carry
    /// a stop reason and this cannot pass yet; the OpenCode event parity work
    /// (#168) is what makes it green. It is written now so that work has a
    /// failing test to satisfy rather than a field to remember.
    #[test]
    #[ignore = "OpenCode assistant events land with the OpenCode parity work (#168)"]
    fn opencode_assistant_events_carry_step_finish_stop_reason() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("opencode.db");
        let src = Connection::open(&source_path).unwrap();
        src.execute_batch(
            "CREATE TABLE session (id TEXT PRIMARY KEY, directory TEXT, time_created INTEGER);
             CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT, data TEXT, time_created INTEGER);
             CREATE TABLE part (id TEXT PRIMARY KEY, session_id TEXT, message_id TEXT, data TEXT, time_created INTEGER);
             INSERT INTO session VALUES ('s-oc', '/tmp/proj', 1);
             INSERT INTO message VALUES ('m-user', 's-oc', '{\"role\":\"user\"}', 1);
             INSERT INTO part VALUES ('p-user', 's-oc', 'm-user', '{\"type\":\"text\",\"text\":\"do the thing\"}', 1);
             INSERT INTO message VALUES ('m-asst', 's-oc', '{\"role\":\"assistant\"}', 2);
             INSERT INTO part VALUES ('p-asst', 's-oc', 'm-asst', '{\"type\":\"text\",\"text\":\"doing it\"}', 2);
             INSERT INTO part VALUES ('p-step', 's-oc', 'm-asst', '{\"type\":\"step-finish\",\"reason\":\"tool-calls\"}', 3);",
        )
        .unwrap();
        drop(src);

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        crate::sync_opencode_session(&conn, &source_path, "s-oc").unwrap();

        let stop_reason: Option<String> = conn
            .query_row(
                "SELECT stop_reason FROM session_events \
                 WHERE source = 'opencode' AND role = 'assistant'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stop_reason.as_deref(), Some("tool-calls"));
    }
}

#[cfg(test)]
mod capture_progress_tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[test]
    fn file_progress_counts_completed_files_and_restores_observer() {
        let updates = Arc::new(Mutex::new(Vec::new()));
        let observed = updates.clone();
        CAPTURE_OBSERVER.with(|slot| {
            *slot.borrow_mut() = Some(std::rc::Rc::new(move |p| observed.lock().unwrap().push(p)))
        });
        let files: Vec<_> = capture_files("claude", vec!["a".into(), "b".into()]).collect();
        assert_eq!(files.len(), 2);
        let values = updates.lock().unwrap();
        assert_eq!(
            values.iter().map(|v| v.processed_files).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert!(values.iter().all(|v| v.total_files == Some(2)));
        drop(values);
        // Even an early database failure restores the caller's observer.
        let home = tempfile::tempdir().unwrap();
        let db = home.path().join("bad.db");
        fs::write(&db, "not a database").unwrap();
        assert!(sync_local_at_with_progress(&db, |_| {}).is_err());
        capture_progress("restored", 0, None);
        assert_eq!(updates.lock().unwrap().last().unwrap().source, "restored");
        CAPTURE_OBSERVER.with(|slot| slot.borrow_mut().take());
    }

    #[test]
    fn callbacks_can_reenter_capture_and_restore_the_outer_observer() {
        let home = tempfile::tempdir().unwrap();
        let db = home.path().join("bad.db");
        fs::write(&db, "not a database").unwrap();
        let inner_db = db.clone();
        let updates = Arc::new(Mutex::new(Vec::new()));
        let outer_updates = updates.clone();
        assert!(sync_local_at_with_progress(&db, move |progress| {
            outer_updates
                .lock()
                .unwrap()
                .push(format!("outer:{}", progress.source));
            let inner_updates = outer_updates.clone();
            assert!(sync_local_at_with_progress(&inner_db, move |inner| {
                inner_updates
                    .lock()
                    .unwrap()
                    .push(format!("inner:{}", inner.source));
            })
            .is_err());
        })
        .is_err());
        assert_eq!(
            *updates.lock().unwrap(),
            vec!["outer:initializing", "inner:initializing"]
        );
        CAPTURE_OBSERVER.with(|slot| assert!(slot.borrow().is_none()));
    }

    #[test]
    fn skipped_sync_still_emits_complete_progress() {
        let home = tempfile::tempdir().unwrap();
        let db = home.path().join("history.db");
        let _lock = try_acquire_sync_lock(&db).unwrap().unwrap();
        let updates = Arc::new(Mutex::new(Vec::new()));
        let observed = updates.clone();
        assert!(!sync_local_at_with_progress(&db, move |progress| {
            observed.lock().unwrap().push(progress.source);
        })
        .unwrap());
        assert_eq!(
            *updates.lock().unwrap(),
            vec!["initializing".to_string(), "complete".to_string()]
        );
    }

    #[test]
    fn trajectory_discovery_prunes_dependencies_and_build_output() {
        let home = tempfile::tempdir().unwrap();
        let wanted = home.path().join("repo/.trajectories");
        fs::create_dir_all(wanted.join("completed/month")).unwrap();
        for ignored in [
            "node_modules/pkg",
            ".git/objects",
            "target/debug",
            ".next/cache",
            ".venv/lib",
        ] {
            fs::create_dir_all(home.path().join(ignored).join(".trajectories")).unwrap();
        }
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(home.path(), home.path().join("repo/cycle")).unwrap();
            std::os::unix::fs::symlink(&wanted, wanted.join("completed/cycle")).unwrap();
        }
        let mut roots = vec![];
        collect_named_dirs(home.path(), ".trajectories", &mut roots).unwrap();
        assert_eq!(roots, vec![wanted.clone()]);
        fs::write(wanted.join("completed/month/run.json"), "{}").unwrap();
        let mut files = vec![];
        collect_trajectory_json(&wanted, &mut files).unwrap();
        assert_eq!(files, vec![wanted.join("completed/month/run.json")]);
    }
}
