use crate::{
    default_db_path, insert_history, insert_session_marker, now_ms, open_db, open_db_readonly,
    parse_cursor_text, prompt_hash, schema_is_catalog_read_current, sync_opencode_db,
    sync_opencode_session, HistoryEntry, SessionLocation, SessionMarker, SessionScope,
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
pub(crate) mod cursor;
pub(crate) mod grok;
pub(crate) mod hydrate;
pub(crate) mod jsonl;
pub(crate) mod tool_result_facts;

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
pub use tool_result_facts::{
    content_hash, stable_stringify, ToolResultFacts, ToolResultIndexer, ERROR_SIGNAL_EXIT_CODE,
    ERROR_SIGNAL_MCP_ERR, ERROR_SIGNAL_PATCH_APPLY, ERROR_SIGNAL_SUBAGENT_STATUS,
    ERROR_SIGNAL_TOOL_RESULT, EVENT_SOURCE_FUNCTION_CALL_OUTPUT,
    EVENT_SOURCE_SUBAGENT_NOTIFICATION, EVENT_SOURCE_TOOL_RESULT, STATUS_COMPLETED, STATUS_ERRORED,
    STATUS_UNKNOWN,
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
    SYNC_QUIET.store(true, AtomicOrdering::Relaxed);
    sync_exclusive(db_path)
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

fn sync_basic(conn: &Connection, db_path: &Path, roots: &crate::ProviderRoots) -> Result<()> {
    let home = &roots.home;
    // See `begin_acquisition_pass`: the resolver's cache is sound only within
    // one pass, and a long-lived host (watch, the Node addon, a desktop app)
    // runs many passes without restarting.
    crate::project_identity::begin_acquisition_pass();
    if roots.use_env_roots {
        for (var, root) in [
            ("CLAUDE_CONFIG_DIR", &roots.claude),
            ("CODEX_HOME", &roots.codex),
            ("GROK_HOME", &roots.grok),
        ] {
            if std::env::var_os(var).is_some_and(|value| !value.to_string_lossy().trim().is_empty())
                && !root.exists()
            {
                sync_note!(
                    "  [{var}] configured root not found: {} (skipped)",
                    root.display()
                );
            }
        }
    }
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
            &roots.claude.join("history.jsonl"),
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
            sync_claude_session_metadata(conn, &mut state, &roots.claude.join("projects")),
        )
        .is_some()
    {
        checkpoint_sync_state(&state_path, &state);
    }
    capture_progress("codex", 0, None);
    if let Some(inserted) = report.capture("codex", sync_codex(conn, &mut state, &roots.codex)) {
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
        sync_grok(conn, &mut state, &roots.grok.join("sessions")),
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
    let opencode = roots.opencode_db.clone();
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
    let discovery_env = DiscoveryEnv::with_provider_roots(conn, roots.clone());
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
    let roots = crate::ProviderRoots::from_env(home_dir());
    sync_exclusive_with_roots(db_path, &roots)
}

fn sync_exclusive_with_home(db_path: &Path, home: &Path) -> Result<bool> {
    let roots = crate::ProviderRoots::from_env(home.to_path_buf());
    sync_exclusive_with_roots(db_path, &roots)
}

fn sync_exclusive_with_roots(db_path: &Path, roots: &crate::ProviderRoots) -> Result<bool> {
    let Some(_sync_lock) = try_acquire_sync_lock(db_path)? else {
        sync_note!("  [sync] another sync is already running; skipped");
        return Ok(false);
    };
    let conn = open_db(db_path).map_err(|error| enrich_sync_error(db_path, error))?;
    sync_basic(&conn, db_path, roots).map_err(|error| enrich_sync_error(db_path, error))?;
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
    let home = home_dir();
    let env = DiscoveryEnv::with_provider_roots(
        &conn,
        crate::ProviderRoots::from_home(home, opencode_path.to_path_buf()),
    );
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
    let roots = crate::ProviderRoots::from_env(home_dir());
    sync_basic(&conn, db_path, &roots).map_err(|error| enrich_sync_error(db_path, error))?;
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
/// adapter is allowed to contribute a tool result with no fidelity at all, and
/// re-reading the local transcript never repairs a row that came from
/// somewhere else -- so "any canonical row is null" is a condition that can
/// stay true forever, re-reading an unchanged file on every sync.
///
/// Recording the generation per provider makes the pass happen exactly once.
const TOOL_RESULT_FIDELITY_GENERATION: i64 = 1;
const CLAUDE_FIDELITY_GENERATION_KEY: &str = "claude_tool_result_fidelity";
const CODEX_FIDELITY_GENERATION_KEY: &str = "codex_tool_result_fidelity";

/// Whether this provider still owes a one-time fidelity backfill pass.
fn fidelity_backfill_pending(state: &Map<String, Value>, key: &str) -> bool {
    state.get(key).and_then(Value::as_i64).unwrap_or(0) < TOOL_RESULT_FIDELITY_GENERATION
}

/// Record that the pass finished.
///
/// Callers reach this only after a walk that both completed and covered every
/// location it has indexed from: an interrupted sync, or one that could not
/// reach a root it has rows under, retries the backfill rather than retiring
/// it over evidence it never looked at.
fn record_fidelity_backfill(state: &mut Map<String, Value>, key: &str) {
    state.insert(key.to_string(), json!(TOOL_RESULT_FIDELITY_GENERATION));
}

/// The same bound is required for per-message raw facts. An
/// installed source adapter contributes rows through the evidence path, which
/// does not carry `raw_facts_version`, and re-reading the local transcript
/// never repairs a row that came from somewhere else -- so "some row for this
/// session is unstamped" is a condition that can stay true forever, re-reading
/// an unchanged file on every sync while never repairing anything.
///
/// Recording the generation per provider makes the pass happen exactly once.
///
/// **Moves with `RAW_MESSAGE_FACTS_VERSION`, always.** The version decides what
/// `events_lack_raw_facts` counts as stale; this decides whether that probe is
/// consulted at all, because sync only asks while the pass is pending. Raising
/// the version alone leaves an install sitting at the recorded generation
/// answering "not pending", skipping every unchanged transcript and never
/// repairing the rows the bump was for — a change that reads as done and does
/// nothing, for exactly the installs that needed it.
/// `the_raw_facts_version_and_generation_are_bumped_together` is the guard.
const RAW_MESSAGE_FACTS_GENERATION: i64 = 2;
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
    ("cursor", CURSOR_SYNC_STATE_KEY),
    ("cursor_events_v1", CURSOR_SYNC_STATE_KEY),
    ("grok_sessions", GROK_SYNC_STATE_KEY),
];

/// Where plain `sync` remembers how far it has read each Cursor transcript.
///
/// Renaming the key is how an unchanged transcript gets re-read after a parser
/// upgrade: the old map is retired, every file opens at offset 0 again, and the
/// records that only ever produced a prompt are re-indexed into
/// `session_events`, `tool_calls` and `file_edits`. Bumping
/// `HYDRATION_PARSER_VERSION` alone only repairs sessions somebody hydrates by
/// name; plain `sync` would keep skipping the rest forever.
const CURSOR_SYNC_STATE_KEY: &str = "cursor_events_v2";

/// Byte cursor covering the complete records hydration actually indexed.
///
/// Targeted hydration re-reads the whole file and does not otherwise touch
/// `.sync-state.json`. Without this, a later incremental sync resumes from
/// the offset it had before hydration and re-inserts untimed prompts under
/// the new mtime.
///
/// `consumed_through` is the offset `ingest_cursor_transcript` stopped at.
/// The live file is not scanned to EOF: a record appended after that read
/// must be left for the next sync, and opening without the saved cursor
/// would reset `rewrite_epoch` so a merge could throw this offset away.
pub(crate) fn record_cursor_hydrate_checkpoint(
    db_path: &Path,
    transcript: &Path,
    consumed_through: u64,
) -> Result<()> {
    let state_path = db_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(".sync-state.json");
    let existing = load_sync_state(&state_path)?;
    let key = transcript.to_string_lossy().into_owned();
    let saved = existing
        .get(CURSOR_SYNC_STATE_KEY)
        .and_then(Value::as_object)
        .and_then(|map| map.get(&key));
    let mut source = CompleteJsonlReader::open(transcript, saved)
        .with_context(|| format!("open Cursor transcript {}", transcript.display()))?;
    let mut line = String::new();
    let mut consumed = source.position;
    while consumed < consumed_through {
        let Some(position) = source
            .next_line(&mut line)
            .with_context(|| format!("read Cursor transcript {}", transcript.display()))?
        else {
            break;
        };
        consumed = position.min(consumed_through);
    }
    let cursor = source
        .committed_cursor(consumed, true)
        .with_context(|| format!("validate Cursor transcript {}", transcript.display()))?;
    let mut cursor_state = Map::new();
    cursor_state.insert(key, cursor.to_value());
    let mut ours = Map::new();
    ours.insert(
        CURSOR_SYNC_STATE_KEY.to_string(),
        Value::Object(cursor_state),
    );
    checkpoint_sync_state(&state_path, &ours);
    Ok(())
}

/// Where plain `sync` remembers the change stamp of each Grok session
/// directory it has already read.
///
/// Renaming the key is how a session that has not changed on disk gets re-read
/// after a parser upgrade: the old map is retired, every session directory
/// looks unseen again, and sessions that only ever produced prompts are
/// re-indexed into `session_events`, `tool_calls`, `file_edits`,
/// `session_markers` and `session_relationships` — with the timestamps
/// `updates.jsonl` recorded, in place of the synthesized ones the previous
/// parser wrote. Bumping `HYDRATION_PARSER_VERSION` alone only repairs
/// sessions somebody hydrates by name.
const GROK_SYNC_STATE_KEY: &str = "grok_events_v1";

fn merged_sync_state(path: &Path, ours: &Map<String, Value>) -> Result<Option<Map<String, Value>>> {
    let mut merged = load_sync_state(path)?;
    let mut changed = false;
    for (key, value) in ours {
        // Applied after the fold, and never persisted.
        if key == FORGOTTEN_PATHS_KEY {
            continue;
        }
        let next = match merged.get(key) {
            Some(existing) if key == CURSOR_SYNC_STATE_KEY => {
                merge_file_cursor_map(existing, value)
            }
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

fn sync_codex(conn: &Connection, state: &mut Map<String, Value>, root: &Path) -> Result<usize> {
    let (cwds, branches, mut inserted) = sync_codex_rollouts(conn, state, root)?;
    let path = root.join("history.jsonl");
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
    root: &Path,
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
    let backfill_fidelity = fidelity_backfill_pending(state, CODEX_FIDELITY_GENERATION_KEY);
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
    let roots = [root.join("sessions"), root.join("archived_sessions")];
    let known_roots: Vec<bool> = roots
        .iter()
        .map(|root| state_names_files_under(&seen, root))
        .collect();
    let mut inserted = 0;
    let mut scanned = 0;
    let mut events = 0usize;
    for (index, root) in roots.into_iter().enumerate() {
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
                // The same gap on the continuity side, repaired the same way.
                // A rollout's continuity signals all live on its `session_meta`
                // line, so a database that predates continuity is backfilled by
                // reading that one line rather than re-ingesting the rollout.
                // The capture always writes a row for a readable rollout, so
                // this clears after one pass and never re-reads again.
                if recorded_session.is_some() && !codex_continuity_evidence_exists(conn, &rollout)?
                {
                    crate::continuity::capture_codex_rollout(conn, &rollout)?;
                }
                match recorded_session {
                    // No session id was recorded because the file had no
                    // usable session_meta; there is nothing to re-ingest.
                    None => continue,
                    Some(id)
                        if codex_session_events_exist(conn, id)?
                            && !(backfill_fidelity
                                && tool_results_lack_fidelity(conn, "codex", id)?)
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
            // Only rollouts whose `session_meta` actually names a prior thread
            // bank anything here; `codex resume` on its own does not.
            crate::continuity::capture_codex_rollout(conn, &rollout)?;
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
    if walked_every_known_root {
        record_fidelity_backfill(state, CODEX_FIDELITY_GENERATION_KEY);
    }
    crate::continuity::reconcile(conn, "codex")?;
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
            ..ObservedRelationship::default()
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

/// The stamp a previous run saved for one Grok session file.
///
/// An earlier build of this walk stored the stamp as a bare string. The shape
/// now is an object that also carries the session id the file was indexed
/// under, and both are read so an upgrade does not re-read the whole store.
fn grok_state_stamp(entry: &Value) -> Option<&str> {
    match entry {
        Value::String(stamp) => Some(stamp.as_str()),
        _ => entry.get("stamp").and_then(Value::as_str),
    }
}

/// The session id a previous run indexed one Grok session file under, when it
/// recorded one. `None` for a state file from the older shape, and for a file
/// that held no session.
fn grok_state_session(entry: &Value) -> Option<&str> {
    entry
        .get("session")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
}

/// Whether the run that last indexed this file wrote any evidence for it.
///
/// `None` for a state entry written before this was recorded; the caller then
/// falls back to asking the database, which self-heals on the next re-read.
fn grok_state_had_evidence(entry: &Value) -> Option<bool> {
    entry.get("evidence").and_then(Value::as_bool)
}

/// Whether any Grok-owned evidence is stored for a session.
///
/// Every table this ingestion writes, because a Grok session need not produce
/// events: one made only of `system` lines, synthetic turns or encrypted
/// reasoning is stored entirely as markers, and one whose transcript is empty
/// but whose `subagents/` directory names a child has only a relationship.
/// Asking about a subset would call such a session unindexed on every run.
fn grok_evidence_exists(conn: &Connection, session_id: &str) -> Result<bool> {
    if session_events_exist(conn, "grok", session_id)? {
        return Ok(true);
    }
    for statement in [
        "SELECT EXISTS(SELECT 1 FROM session_markers \
         WHERE source = 'grok' AND session_id = ? LIMIT 1)",
        "SELECT EXISTS(SELECT 1 FROM session_relationships \
         WHERE source = 'grok' AND parent_session_id = ? \
           AND evidence_kind = 'grok_subagent_dir' LIMIT 1)",
    ] {
        let exists: i64 = conn.query_row(statement, params![session_id], |row| row.get(0))?;
        if exists != 0 {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Whether the catalog row this ingestion writes is still there.
///
/// The one thing every Grok ingestion produces, whatever the transcript held.
/// It is the right question only for a session recorded as having written no
/// evidence: for such a session the catalog row is the whole of what indexing
/// it produced, so its presence and "is it indexed" are the same question.
/// For a session that did write evidence it would be too weak, because
/// discovery writes catalog rows too and one could stand over missing rows.
fn grok_catalog_row_exists(conn: &Connection, session_id: &str) -> Result<bool> {
    let exists: i64 = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sessions \
         WHERE source = 'grok' AND session_id = ? LIMIT 1)",
        params![session_id],
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

pub(crate) struct CodexSessionMeta {
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
pub(crate) fn read_codex_session_meta(path: &Path) -> Result<Option<CodexSessionMeta>> {
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
pub(crate) struct CodexIngestOutcome {
    prompts: usize,
    events: usize,
    first_ts: Option<i64>,
    last_ts: Option<i64>,
    first_prompt: Option<String>,
    last_assistant_text: Option<String>,
}

/// Cumulative token totals from a Codex `token_count` event
/// (`info.total_token_usage`). `input` is inclusive of `cached_input`.
///
/// Counters are `u64` because that is what a token count is. They were `i64`
/// read with `unwrap_or(0)`, which turned every counter the provider wrote
/// badly — negative, fractional, out of range — into a zero, and the
/// differencing below then clamped the result back to a plausible
/// non-negative delta. The stored JSON was valid, so nothing downstream could
/// tell a corrupted snapshot from a reported zero.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
struct CodexTokenTotals {
    input: u64,
    cached_input: u64,
    cache_write: u64,
    output: u64,
    reasoning_output: u64,
    total: u64,
}

/// Usage measured but not yet attached to an assistant event.
enum PendingCodexUsage {
    /// A per-request delta between two strictly-advancing snapshots.
    Delta(CodexTokenTotals),
    /// The provider's own snapshot object, kept **verbatim** because it could
    /// not be differenced: a counter is not a non-negative integer, or the
    /// arithmetic would leave `u64`.
    ///
    /// Storing the raw object is what makes the refusal reach a caller:
    /// `normalize_usage` rejects it with a stable code, so the request reports
    /// `usage: null` with `unnormalizable-usage` instead of a delta of zeros
    /// that looks like a measurement. Nothing is fabricated — this is what the
    /// provider wrote.
    Unusable(String),
}

impl PendingCodexUsage {
    fn into_token_json(self) -> String {
        match self {
            Self::Delta(totals) => totals.to_token_json(),
            Self::Unusable(raw) => raw,
        }
    }

    /// Fold a newly measured delta into whatever is already pending.
    ///
    /// An unusable snapshot poisons the sum. A total that silently omits a
    /// segment it could not measure is worse than one that says so.
    fn merged(self, delta: CodexTokenTotals, raw: &Value) -> Self {
        match self {
            Self::Unusable(raw) => Self::Unusable(raw),
            Self::Delta(pending) => match pending.plus(&delta) {
                Some(sum) => Self::Delta(sum),
                None => Self::Unusable(raw.to_string()),
            },
        }
    }
}

impl CodexTokenTotals {
    /// Read one snapshot, or `None` when any counter the provider wrote is
    /// not a non-negative integer.
    ///
    /// An absent or null counter is "not reported" and reads as zero, which
    /// is the shape `total_token_usage` genuinely has. A *present* counter
    /// that is negative, fractional, or larger than `u64` is corruption, and
    /// the caller keeps the provider's object instead of inventing a number
    /// for it.
    fn from_usage(value: &Value) -> Option<Self> {
        let obj = value.as_object()?;
        let get = |key: &str| match obj.get(key) {
            None | Some(Value::Null) => Some(0),
            Some(value) => value.as_u64(),
        };
        Some(Self {
            input: get("input_tokens")?,
            cached_input: get("cached_input_tokens")?,
            cache_write: get("cache_write_input_tokens")?,
            output: get("output_tokens")?,
            reasoning_output: get("reasoning_output_tokens")?,
            total: get("total_tokens")?,
        })
    }

    fn fields(&self) -> [u64; 6] {
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

    /// Difference two snapshots, or `None` if any field would go backwards.
    ///
    /// Checked rather than clamped per field: the caller only calls this once
    /// [`Self::advanced_from`] holds, so an underflow here means an invariant
    /// broke, and `max(0)` would hide it behind a plausible zero.
    fn minus(&self, prev: &Self) -> Option<Self> {
        Some(Self {
            input: self.input.checked_sub(prev.input)?,
            cached_input: self.cached_input.checked_sub(prev.cached_input)?,
            cache_write: self.cache_write.checked_sub(prev.cache_write)?,
            output: self.output.checked_sub(prev.output)?,
            reasoning_output: self.reasoning_output.checked_sub(prev.reasoning_output)?,
            total: self.total.checked_sub(prev.total)?,
        })
    }

    fn plus(&self, other: &Self) -> Option<Self> {
        Some(Self {
            input: self.input.checked_add(other.input)?,
            cached_input: self.cached_input.checked_add(other.cached_input)?,
            cache_write: self.cache_write.checked_add(other.cache_write)?,
            output: self.output.checked_add(other.output)?,
            reasoning_output: self.reasoning_output.checked_add(other.reasoning_output)?,
            total: self.total.checked_add(other.total)?,
        })
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

/// One Codex snapshot that could not be differenced, recorded as it arrived.
///
/// The log is **append-only**: an entry is never edited, replaced or removed
/// while the rollout is being read. Nothing is decided during ingestion — the
/// refusals are computed once, at the end, by [`surviving_refusals`].
///
/// Four defects in a row came from deciding incrementally instead: attaching a
/// refusal immediately stole the turn a later snapshot was owed; holding a
/// single slot let one turn's refusal overwrite another's; clearing every held
/// refusal on a measured delta dropped ones the delta could not account for;
/// and clearing by generation still let a second refusal for one turn inherit
/// a newer generation than the evidence supported. Each fix was locally right
/// and produced the next defect, because the rule lived in three places and
/// nowhere in full.
struct UnreadableSnapshot {
    /// The assistant event waiting for a measurement when this arrived. That
    /// is the turn this snapshot failed to measure, named now rather than
    /// looked up later, because the waiting slot moves on as turns appear.
    uid: String,
    /// Which baseline `prev_totals` held at that moment — see
    /// `baseline_generation`.
    generation: u64,
    /// The provider's own object, verbatim, so the request can report what was
    /// rejected rather than a zero that reads like a measurement.
    raw: String,
}

/// **The invariant.** A turn keeps a refusal exactly when no measured delta
/// was differenced from the baseline generation that refusal was recorded
/// under.
///
/// Everything the ingest loop knows about refusals is in its two arguments,
/// and this is the only place that interprets them.
///
/// Why the generation is the whole test: an unreadable snapshot does not
/// advance the baseline, so a delta differenced from generation `g` spans
/// every refusal recorded under `g` — those turns' spend is reported inside
/// that delta's request and they are owed nothing. A baseline *reinstall*
/// advances it without measuring anything, absorbing every earlier span into
/// itself, so no later delta can ever account for a refusal recorded under an
/// older generation. Those survive.
///
/// A turn refused more than once keeps the **earliest** surviving refusal: it
/// is the first thing that went wrong for that turn, and a later one is a
/// consequence. Crucially a later refusal never erases an earlier one, which
/// is only true because the log is append-only.
fn surviving_refusals(
    log: &[UnreadableSnapshot],
    measured_generations: &HashSet<u64>,
) -> Vec<(String, String)> {
    let mut refused = Vec::new();
    let mut seen: HashSet<&str> = HashSet::new();
    for entry in log {
        if measured_generations.contains(&entry.generation) {
            continue;
        }
        if seen.insert(entry.uid.as_str()) {
            refused.push((entry.uid.clone(), entry.raw.clone()));
        }
    }
    refused
}

/// Attach measured usage to the assistant event that earned it, or hold it
/// until one appears.
fn attach_codex_usage(
    conn: &Connection,
    session_id: &str,
    pending: &mut Option<PendingCodexUsage>,
    untokened_assistant_uid: &mut Option<String>,
    usage: PendingCodexUsage,
) -> Result<()> {
    match untokened_assistant_uid.take() {
        Some(uid) => {
            conn.execute(
                "UPDATE session_events SET token_json = ? \
                 WHERE source = 'codex' AND session_id = ? AND event_uid = ?",
                params![usage.into_token_json(), session_id, uid],
            )?;
        }
        None => *pending = Some(usage),
    }
    Ok(())
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

pub(crate) fn ingest_codex_rollout(
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
    let mut pending_usage: Option<PendingCodexUsage> = None;
    // Every snapshot that could not be differenced, in arrival order and never
    // rewritten, plus the baselines a delta was actually measured from. These
    // two are facts about the rollout; what they *mean* is decided once, after
    // the loop, by `surviving_refusals` — which is the only place the rule
    // lives.
    let mut unreadable_snapshots: Vec<UnreadableSnapshot> = Vec::new();
    let mut measured_generations: HashSet<u64> = HashSet::new();
    // Which baseline `prev_totals` currently holds. Bumped every time it is
    // replaced, so each fact above can name the baseline it belongs to.
    let mut baseline_generation: u64 = 0;
    // Which API request the rows being written belong to.
    //
    // Codex names no request — no request id, no message id — but it *ends*
    // one with every `token_count`: the span between two snapshots is one API
    // call, and `agent_reasoning`, each `function_call` and the closing
    // `agent_message` of that call all fall inside it. Numbered here because
    // the boundary is only knowable while reading the rollout in order; a
    // reader given the stored rows would have to scan the session to find the
    // next row carrying a measurement.
    //
    // Deliberately not the turn: a turn runs a tool loop and holds as many
    // calls as it made round trips. Grouping by `turn_id` would merge calls
    // with different measurements into one request and report no usage for
    // either.
    let mut request_span: u64 = 0;
    // Set when a snapshot is unreadable while no baseline has been
    // established. A resumed rollout opens with the cumulative total it
    // carried over; if that opening snapshot cannot be read, there is no
    // baseline, and differencing the next good one against zero would charge
    // the session's entire carried-over history to one request.
    let mut baseline_unknown = false;
    let mut untokened_assistant_uid: Option<String> = None;
    // The turn a record falls inside, stamped from the last `turn_context`
    // until the next one names a different turn. Codex writes it once per turn
    // rather than on every record, so carrying it forward is what makes turn
    // boundaries recoverable downstream.
    let mut turn_id: Option<String> = None;
    let mut saw_model_output = false;
    let mut human_messages = codex::HumanMessageDeduper::default();
    // Codex reports how a call ended out of band — `exec_command_end`,
    // `patch_apply_end`, `mcp_tool_call_end` — and only guarantees they have
    // all arrived by `task_complete`. Results are recorded with an `unknown`
    // status and the turn's error signals are applied to them when it closes.
    let mut indexer = tool_result_facts::ToolResultIndexer::default();
    let mut turn_error_signals: HashMap<String, &'static str> = HashMap::new();
    let mut pending_results: Vec<(String, String)> = Vec::new();
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
                None,
                turn_id.as_deref(),
                // A user turn is not a request, and the view does not read it.
                None,
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
                        let token_json =
                            pending_usage.take().map(PendingCodexUsage::into_token_json);
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
                            None,
                            turn_id.as_deref(),
                            Some(request_span.to_string().as_str()),
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
                            None,
                            turn_id.as_deref(),
                            Some(request_span.to_string().as_str()),
                        )?;
                        outcome.events += 1;
                        untokened_assistant_uid = Some(uid);
                        saw_model_output = true;
                    }
                }
                "token_count" => {
                    let Some(usage) = payload
                        .get("info")
                        .and_then(|info| info.get("total_token_usage"))
                    else {
                        continue;
                    };
                    // The provider reported a call here, so the rows written
                    // since the last snapshot are that call and the rows after
                    // this one are the next. The boundary is the snapshot
                    // itself, not whether it could be differenced: two turns
                    // whose snapshots were both unreadable are two refused
                    // requests, and folding them into one span would merge
                    // their refusals into a single request holding two
                    // disagreeing blobs — reported as ambiguous rather than as
                    // two rejections.
                    //
                    // Measurement is a separate question, settled by
                    // `surviving_refusals`: a span whose snapshot could not be
                    // read has its spend reported inside whichever later delta
                    // covers it, and reads as a request with no usage.
                    request_span += 1;
                    let Some(totals) = CodexTokenTotals::from_usage(usage) else {
                        // A counter that is not a non-negative integer cannot
                        // be differenced — which is the same situation as a
                        // regressed snapshot below, and gets the same
                        // treatment: change nothing.
                        //
                        // Attaching the refusal here would consume the
                        // assistant event still waiting for its measurement,
                        // so the next *valid* snapshot would have nowhere to
                        // land and the turn the provider did report would be
                        // marked unreadable while its real delta went
                        // elsewhere. The baseline is untouched, so that next
                        // snapshot's delta already covers this whole span;
                        // the number is recoverable and the turn must get it.
                        //
                        // It is only recorded. Whether it ends up refusing
                        // anything is `surviving_refusals`' decision, taken
                        // once the whole rollout is known. With no turn
                        // waiting, it measured a span no turn is missing and
                        // there is nothing to record.
                        if let Some(uid) = &untokened_assistant_uid {
                            unreadable_snapshots.push(UnreadableSnapshot {
                                uid: uid.clone(),
                                generation: baseline_generation,
                                raw: usage.to_string(),
                            });
                        }
                        // With no baseline yet, this was the resume snapshot,
                        // and nothing now says where the session started.
                        if prev_totals.is_none() && !saw_model_output {
                            baseline_unknown = true;
                        }
                        continue;
                    };
                    match prev_totals {
                        // The first snapshot before any model output is the
                        // carried-over baseline of a resumed session (a fresh
                        // session's opening snapshot has `info: null`).
                        None if !saw_model_output => {
                            prev_totals = Some(totals);
                            baseline_generation += 1;
                        }
                        // The carried-over baseline was unreadable, so this
                        // snapshot establishes one and measures nothing. A
                        // delta from zero here would be this session's whole
                        // history reported as a single request's spend — a
                        // well-formed number that is wrong by however much the
                        // session had already used.
                        None if baseline_unknown => {
                            prev_totals = Some(totals);
                            baseline_unknown = false;
                            // This installs a baseline without measuring, so
                            // everything spent up to here — a refused turn
                            // included — is absorbed into it and can never
                            // appear in a later delta. The generation bump is
                            // what records that, and `surviving_refusals` is
                            // what acts on it.
                            baseline_generation += 1;
                        }
                        // A regressed snapshot is treated as a transient
                        // glitch: keeping the prior baseline means the next
                        // advancing snapshot's delta covers exactly the spend
                        // since that baseline, so per-event sums still
                        // reproduce the cumulative totals.
                        Some(prev) if totals.regressed_from(&prev) => {}
                        Some(prev) if !totals.advanced_from(&prev) => {}
                        _ => {
                            let baseline = prev_totals.unwrap_or_default();
                            let measured = totals.minus(&baseline);
                            prev_totals = Some(totals);
                            // Record which baseline was measured from, and
                            // only when something really was measured — the
                            // `None` arm below produces no delta, so it spans
                            // nothing and settles nothing.
                            if measured.is_some() {
                                measured_generations.insert(baseline_generation);
                            }
                            baseline_generation += 1;
                            let next = match measured {
                                Some(delta) => match pending_usage.take() {
                                    Some(pending) => pending.merged(delta, usage),
                                    None => PendingCodexUsage::Delta(delta),
                                },
                                // `advanced_from` held, so this cannot
                                // underflow; if it ever does, say so rather
                                // than publish a clamped zero.
                                None => PendingCodexUsage::Unusable(usage.to_string()),
                            };
                            attach_codex_usage(
                                conn,
                                session_id,
                                &mut pending_usage,
                                &mut untokened_assistant_uid,
                                next,
                            )?;
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
                    resolve_codex_tool_results(
                        conn,
                        session_id,
                        &mut pending_results,
                        &mut turn_error_signals,
                        Settle::TurnComplete,
                    )?;
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
                    if is_error == Some(true) {
                        turn_error_signals
                            .insert(call_id.to_string(), tool_result_facts::ERROR_SIGNAL_MCP_ERR);
                    }
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
                        if !success {
                            turn_error_signals.insert(
                                call_id.to_string(),
                                tool_result_facts::ERROR_SIGNAL_PATCH_APPLY,
                            );
                        }
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
                            if exit_code != 0 {
                                turn_error_signals.insert(
                                    call_id.to_string(),
                                    tool_result_facts::ERROR_SIGNAL_EXIT_CODE,
                                );
                            }
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
                    let token_json = pending_usage.take().map(PendingCodexUsage::into_token_json);
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
                        None,
                        turn_id.as_deref(),
                        Some(request_span.to_string().as_str()),
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
                    let output = payload.get("output").unwrap_or(&Value::Null);
                    // An output with nothing displayable in it -- a silent
                    // command's empty string, a structured array carrying no
                    // `text` member -- is still a result the tool returned.
                    // Dropping it because a transcript view would render
                    // nothing also drops the call's linkage, its measured size
                    // (an empty payload is zero bytes, not an unknown number)
                    // and its place in the ordering, which is exactly what a
                    // span tree needs when a tool answers with silence.
                    let output_text = materialize_codex_output_text(output).unwrap_or_default();
                    let uid = format!("{index}:{payload_type}");
                    let message_id = payload_str("id").unwrap_or(uid.as_str()).to_string();
                    let call_id = payload_str("call_id").unwrap_or("");
                    let (call_index, event_index) = indexer.next(call_id);
                    // Measured over the provider's raw `output`, not the
                    // flattened text below: a payload that arrives as
                    // structured blocks is bigger on the wire than the
                    // joined string this row stores.
                    let facts = tool_result_facts::codex_output_facts(output, call_id)
                        .with_ordering(call_index, event_index);
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
                        Some(&facts),
                        turn_id.as_deref(),
                        // Likewise: a tool's own output is not an API call.
                        None,
                    )?;
                    outcome.events += 1;
                    if !call_id.is_empty() {
                        pending_results.push((uid, call_id.to_string()));
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
    // The whole rollout is known, so the refusals can be worked out from what
    // was recorded. `token_json IS NULL` keeps one from overwriting a real
    // measurement the turn acquired by another route.
    for (uid, raw) in surviving_refusals(&unreadable_snapshots, &measured_generations) {
        conn.execute(
            "UPDATE session_events SET token_json = ? \
             WHERE source = 'codex' AND session_id = ? AND event_uid = ? \
               AND token_json IS NULL",
            params![
                PendingCodexUsage::Unusable(raw).into_token_json(),
                session_id,
                uid
            ],
        )?;
    }
    // End of file is not a turn boundary. A live rollout's last turn has no
    // `task_complete` yet, and the `exec_command_end` that fails one of its
    // calls can still be written after the bytes this pass read. Failures
    // already observed are recorded; a result with no signal yet stays
    // `unknown`, because "not known to have failed" is not "succeeded". The
    // next sync re-reads the file and settles it.
    resolve_codex_tool_results(
        conn,
        session_id,
        &mut pending_results,
        &mut turn_error_signals,
        Settle::EndOfFile,
    )?;
    Ok(outcome)
}

/// Whether the pass that is resolving buffered results knows the turn is over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Settle {
    /// `task_complete` was read: every out-of-band signal for the turn has
    /// arrived, so a result with none of them succeeded.
    TurnComplete,
    /// The readable transcript ended mid-turn: record the failures seen, and
    /// leave everything else undecided.
    EndOfFile,
}

/// Apply a Codex turn's out-of-band error signals to the tool-result rows it
/// buffered.
fn resolve_codex_tool_results(
    conn: &Connection,
    session_id: &str,
    pending: &mut Vec<(String, String)>,
    signals: &mut HashMap<String, &'static str>,
    settle: Settle,
) -> Result<()> {
    for (uid, call_id) in pending.drain(..) {
        let signal = signals.get(call_id.as_str()).copied();
        let status = match (signal, settle) {
            (Some(_), _) => tool_result_facts::STATUS_ERRORED,
            (None, Settle::TurnComplete) => tool_result_facts::STATUS_COMPLETED,
            // Left as inserted, so a later pass over a completed turn is the
            // only thing that can call it a success.
            (None, Settle::EndOfFile) => continue,
        };
        conn.execute(
            "UPDATE session_events SET result_status = ?, error_signal = ? \
             WHERE source = 'codex' AND session_id = ? AND event_uid = ?",
            params![status, signal, session_id, uid],
        )?;
    }
    signals.clear();
    Ok(())
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
    tool_result_facts: Option<&ToolResultFacts>,
    turn_id: Option<&str>,
    request_span: Option<&str>,
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
        RequestIdentity::none(),
        uid,
        tool_result_facts,
        RawMessageFacts {
            turn_id,
            request_span,
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
    let backfill_fidelity = fidelity_backfill_pending(state, CLAUDE_FIDELITY_GENERATION_KEY);
    // The first read failure, returned once the in-memory state has been
    // brought up to date. Continuing past it indexes the rest of the tree,
    // but the run did omit a transcript it discovered, and a caller told the
    // sync completed would treat an incomplete cache as current.
    let mut read_error: Option<anyhow::Error> = None;
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
        let transcript_events = claude_transcript_events_exist(conn, &path)?;
        if session_state.get(&key).and_then(Value::as_str) == Some(stamp.as_str())
            && (transcript_events || claude_sidecar_evidence_exists(conn, &path)?)
            // During the one-time backfill pass, an unchanged transcript
            // whose rows predate either additive evidence shape is re-read to
            // populate it. Outside those passes the stamp alone decides, so a
            // row this transcript does not own cannot pin the file off the
            // fast path.
            && !(backfill_fidelity
                && claude_transcript_lacks_tool_result_fidelity(conn, &path)?)
            && !(backfill_raw_facts && claude_transcript_lacks_raw_facts(conn, &path)?)
        {
            // A transcript registered as a session and indexed before
            // continuity existed still owes its evidence. Reading it here
            // rather than falling through keeps the skip's promise:
            // continuity is written to its own table and touches no indexed
            // row, so the file is read without being re-ingested — the same
            // shape as the subagent delegation backfill in the Codex walk.
            // The capture writes a row for every readable transcript, so this
            // clears after one pass and never reads again.
            //
            // Gated on `transcript_events` because a subagent sidecar reaches
            // this skip through its delegation evidence instead, and a sidecar
            // is not a session: it never reaches the capture on the ingest
            // path either, so it has no row to owe and must not be re-read.
            if transcript_events && claude_transcript_lacks_continuity_evidence(conn, &path)? {
                crate::continuity::capture_claude_transcript(conn, &path)?;
            }
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
        // Read before stamping. The stamp is this walk's claim to have
        // indexed the file; recording it first means a read that fails
        // afterwards still looks done, and since a restored file keeps its
        // length and mtime, the unchanged stamp would skip it forever.
        let scanned_meta = match scan_claude_session_file(&path) {
            Ok(meta) => meta,
            Err(error) => {
                // Only a file that would otherwise be skipped holds the pass
                // open. An unstamped file, or one whose stamp has moved, is
                // reopened by the next walk regardless -- and a pending
                // generation keeps the per-session fidelity probe live, which
                // drags any session carrying a contributed null row through a
                // re-read on every sync.
                if session_state.get(&key).and_then(Value::as_str) == Some(stamp.as_str()) {
                    walked_every_known_root = false;
                }
                sync_note!(
                    "  [claude-sessions] could not read {}: {error:#}",
                    path.display()
                );
                read_error.get_or_insert(error);
                continue;
            }
        };
        session_state.insert(key, json!(stamp));
        if let Some(meta) = scanned_meta {
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
            // Global sync is not scoped to one thread, so it indexes the
            // materialization edge like every other kind.
            record_claude_remote_relationship(conn, &meta, true)?;
            // Continuity is cross-file, so the evidence is banked here and
            // reconciled once the whole walk has indexed everything it can
            // reach; a branch read before its origin is resolved by the same
            // pass rather than needing a second sync.
            crate::continuity::capture_claude_transcript(conn, &path)?;
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
    if walked_every_known_root {
        record_fidelity_backfill(state, CLAUDE_FIDELITY_GENERATION_KEY);
    }
    crate::continuity::reconcile(conn, "claude")?;
    record_raw_facts_backfill(state, CLAUDE_RAW_MESSAGE_FACTS_KEY, walked_every_known_root);
    if scanned > 0 {
        sync_note!("  [claude-sessions] scanned {scanned} files, {upserted} sessions updated");
    }
    // Reported after the state above is current, so the work that did land is
    // kept and only the run's own status says a transcript was missed.
    match read_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
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

/// Whether a session's indexed tool results predate the fidelity columns.
///
/// A schema migration adds nullable columns and declares itself done; the sync
/// stamp maps are what decide whether a transcript is opened at all. Without
/// this, a database that migrated cleanly skips every unchanged transcript and
/// plain `sync` leaves their `payload_bytes`, `event_index` and the rest null
/// indefinitely -- while reporting a perfectly successful sync. Only explicit
/// hydration or an unrelated edit to the file would ever repair them.
///
/// `event_index` is the field to test: every tool-result row the current
/// parsers write has one, whatever the payload was. A session with no tool
/// results has nothing to backfill and stays on the fast path.
///
/// This only selects which files a backfill pass re-reads; it is not what
/// ends the pass. `session_events` is keyed by `(source, session_id)` and
/// local and remote observations of one session share that identity, so a row
/// this probe sees may be an adapter's contribution that re-reading the local
/// transcript will never touch. `TOOL_RESULT_FIDELITY_GENERATION` is what
/// guarantees the work happens once.
///
/// Selecting files this way is narrower than bumping the stamp-map
/// generation, which would re-read every transcript in the archive, including
/// the ones with nothing to gain, and would discard the selective-repair state
/// the codex generations carry.
fn tool_results_lack_fidelity(conn: &Connection, source: &str, session_id: &str) -> Result<bool> {
    let lacking: i64 = conn.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM session_events
            WHERE source = ? AND session_id = ? AND kind = 'tool_result'
              AND event_index IS NULL
            LIMIT 1
        )",
        params![source, session_id],
        |row| row.get(0),
    )?;
    Ok(lacking != 0)
}

/// Whether this rollout's continuity evidence has ever been banked.
///
/// Keyed on the locator, like the Claude probe below and for the same reason:
/// the stamp map would otherwise skip exactly the rollouts that an upgrade
/// into continuity needs to read.
fn codex_continuity_evidence_exists(conn: &Connection, path: &Path) -> Result<bool> {
    let locator = path.to_string_lossy();
    let exists: i64 = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM session_continuity_evidence \
         WHERE source = 'codex' AND locator = ? LIMIT 1)",
        [locator.as_ref()],
        |row| row.get(0),
    )?;
    Ok(exists != 0)
}

/// Whether an unchanged transcript still owes the continuity index one read.
///
/// The stamp map decides whether a transcript is opened at all, so an install
/// that upgrades into continuity has a stamp for every existing file and would
/// skip exactly the ones whose evidence has never been banked — reporting a
/// successful sync over a `session_continuity_evidence` table that stays empty
/// until some transcript happens to change. This is narrower than bumping the
/// `claude_sessions_v3` generation, which would re-read the whole archive and
/// discard the selective-repair state that map carries.
///
/// Keyed on the locator, which is what the evidence table is keyed on. The
/// caller has already established that this file is the transcript of a
/// registered session, so it has an in-log `sessionId`, so the capture writes
/// a row — the condition clears after one read and never fires again.
fn claude_transcript_lacks_continuity_evidence(conn: &Connection, path: &Path) -> Result<bool> {
    let locator = path.to_string_lossy();
    let exists: i64 = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM session_continuity_evidence \
         WHERE source = 'claude' AND locator = ? LIMIT 1)",
        [locator.as_ref()],
        |row| row.get(0),
    )?;
    Ok(exists == 0)
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

/// The fidelity question for a Claude transcript, which the walk knows by path.
fn claude_transcript_lacks_tool_result_fidelity(conn: &Connection, path: &Path) -> Result<bool> {
    let raw_path = path.to_string_lossy();
    let lacking: i64 = conn.query_row(
        "SELECT
            EXISTS(
                SELECT 1
                FROM sessions s
                JOIN session_events e ON e.source = s.source AND e.session_id = s.session_id
                WHERE s.source = 'claude' AND s.raw_path = ?1
                  AND e.kind = 'tool_result' AND e.event_index IS NULL
                LIMIT 1
            )
            OR EXISTS(
                SELECT 1
                FROM session_relationships r
                JOIN session_events e
                  ON e.source = 'claude'
                 AND e.session_id = COALESCE(r.child_session_id, r.parent_session_id)
                WHERE r.source = 'claude' AND r.evidence_locator = ?1
                  AND e.kind = 'tool_result' AND e.event_index IS NULL
                LIMIT 1
            )",
        [raw_path.as_ref()],
        |row| row.get(0),
    )?;
    Ok(lacking != 0)
}

/// The raw-facts question for a Claude transcript.
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
    // Propagated, not defaulted. An I/O or UTF-8 failure reduced to an empty
    // string is indistinguishable here from a file that genuinely holds no
    // session, and the caller would record it as successfully read.
    let text = fs::read_to_string(path)
        .with_context(|| format!("reading claude transcript {}", path.display()))?;
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

/// Correlate a local transcript with the remote session it materialized from.
///
/// `include_related` gates only the `session_relationships` write. The identity
/// correlation is a fact about the selected session itself -- which remote id
/// it carries -- and appears in no reported relationship field, so recording it
/// keeps `reconcile_claude_remote_relationships` able to link the pair later
/// without this acquisition claiming evidence it was told not to gather.
fn record_claude_remote_relationship(
    conn: &Connection,
    meta: &ClaudeSessionMeta,
    include_related: bool,
) -> Result<()> {
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
    // A `materialized_local` edge lands in `session_relationships`, the table
    // the `Relationship` kind is defined over, so writing one for a request
    // that declined related evidence leaves the result reporting no
    // relationship coverage over a database that has some.
    if remote_exists && include_related {
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
            ..ObservedRelationship::default()
        },
    )
}

pub(crate) fn ingest_claude_transcript(conn: &Connection, path: &Path) -> Result<()> {
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
    let text = fs::read_to_string(path)
        .with_context(|| format!("reading claude transcript {}", path.display()))?;
    // Ordering is assigned over the whole transcript, and this parser always
    // re-reads the file from the start, so a re-sync reproduces the same
    // indexes instead of advancing them.
    let mut indexer = tool_result_facts::ToolResultIndexer::default();
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
        // Read once per record: every row this record produces belongs to the
        // same provider request, whatever `message_uuid` the block gets.
        let identity = RequestIdentity::from_claude_record(obj, message);
        let cwd = obj.get("cwd").and_then(Value::as_str);
        let project = cwd;
        let git_branch = obj.get("gitBranch").and_then(Value::as_str);
        let parent_id = obj.get("parentUuid").and_then(Value::as_str);
        let is_meta = obj.get("isMeta").and_then(Value::as_bool);
        let raw_facts = RawMessageFacts {
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
            // Claude names its requests, so it groups on `request_id` and
            // needs no span.
            request_span: None,
        };
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
        // Claude reports a finished subagent as a `type: "system"` line with
        // no message body, so the block walk below never sees it. It is the
        // only record that ties a delegated child back to the Agent call that
        // spawned it, which makes it a tool result in everything but shape.
        //
        // This runs before the sidechain guard, and has to. A system line
        // carries no `message`, so `skipped_sidechain` is true for every one
        // of them that is marked `isSidechain` -- and a nested Agent call
        // writes its completion line inside the child's sidecar, where every
        // record is a sidechain. Skipping those would drop the only record of
        // the nested spawn while keeping the rows for the spawns that happen
        // to sit on the parent transcript. The guard below still owns every
        // other sidechain row; a system line that names no child falls
        // through to it.
        let system_record = obj.get("type").and_then(Value::as_str) == Some("system");
        if system_record {
            if let Some(tool_facts) = tool_result_facts::claude_subagent_notification_facts(obj) {
                let tool_use_id = tool_facts.tool_use_id.clone().unwrap_or_default();
                let (call_index, event_index) = indexer.next(&tool_use_id);
                let tool_facts = tool_facts.with_ordering(call_index, event_index);
                let notification_text = obj
                    .get("content")
                    .and_then(Value::as_str)
                    .filter(|value| !value.trim().is_empty());
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
                    notification_text,
                    None,
                    None,
                    identity,
                    &format!("{message_uuid}:subagent_notification"),
                    Some(&tool_facts),
                    raw_facts,
                )?;
                continue;
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
        // A system line that names no delegated child has nothing else this
        // parser stores -- it carries no `message` body to walk -- and is
        // dropped exactly as it was before the notification handler existed.
        if system_record {
            continue;
        }
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
                    identity,
                    &format!("{message_uuid}:0"),
                    None,
                    raw_facts,
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
                                identity,
                                &event_uid,
                                None,
                                raw_facts,
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
                            identity,
                            &event_uid,
                            None,
                            raw_facts,
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
                        identity,
                        &event_uid,
                        None,
                        raw_facts,
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
                    // Measured over the provider's raw content, before
                    // `materialize_tool_result_text` reshapes it: the whole
                    // point of `payload_bytes` is to say what the harness
                    // actually returned, which a post-processed string can no
                    // longer answer.
                    let (call_index, event_index) = indexer.next(tool_use_id);
                    let facts = tool_result_facts::claude_tool_result_facts(block)
                        .with_ordering(call_index, event_index);
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
                        identity,
                        &event_uid,
                        Some(&facts),
                        raw_facts,
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
/// 2 adds `request_span`: Codex rows indexed before it have none, so they
/// would keep grouping one API call into a request per row until re-read.
const RAW_MESSAGE_FACTS_VERSION: i64 = 2;

#[derive(Debug, Default, Clone, Copy)]
struct RawMessageFacts<'a> {
    request_id: Option<&'a str>,
    stop_reason: Option<&'a str>,
    agent_version: Option<&'a str>,
    is_sidechain: Option<bool>,
    is_meta: Option<bool>,
    turn_id: Option<&'a str>,
    /// Which API request this row belongs to, for a provider that delimits
    /// its requests with usage snapshots instead of naming them. Numbered per
    /// session by the parser; see `codex_request_span` at its call site.
    request_span: Option<&'a str>,
}

#[allow(clippy::too_many_arguments)]
/// The provider's own identities for the request a record belongs to, read
/// once per record and stored verbatim.
///
/// These are *not* the `message_id` column, which holds the record's own
/// `uuid`. One Claude request is written as several records with different
/// uuids and the same `requestId`, so only these establish which rows belong
/// to one API call.
#[derive(Clone, Copy, Default)]
pub(crate) struct RequestIdentity<'a> {
    /// Claude's `requestId` (older transcripts spell it `request_id`).
    pub request_id: Option<&'a str>,
    /// The provider's own message id — Claude's `message.id`.
    pub provider_message_id: Option<&'a str>,
}

impl<'a> RequestIdentity<'a> {
    /// What a source that records neither supplies. Its stored records are
    /// already one per request.
    fn none() -> Self {
        Self::default()
    }

    /// Read both from one Claude transcript record.
    ///
    /// The value is stored **verbatim**. Trimming would make `"req"` and
    /// `" req "` the same grouping key, merging two providers' requests into
    /// one row and summing usage that belongs to neither — the same reason
    /// the napi boundary rejects a padded session id rather than trimming it.
    /// A value that is empty once trimmed is not an identity at all and is
    /// stored as absent, so the key never becomes `""` for every record in a
    /// session.
    fn from_claude_record(
        object: &'a Map<String, Value>,
        message: Option<&'a Map<String, Value>>,
    ) -> Self {
        let text = |value: Option<&'a Value>| {
            value
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
        };
        Self {
            request_id: text(object.get("requestId")).or_else(|| text(object.get("request_id"))),
            provider_message_id: text(message.and_then(|message| message.get("id"))),
        }
    }
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
    identity: RequestIdentity<'_>,
    event_uid: &str,
    // Carried as one struct rather than ten more positional arguments: the
    // fidelity columns are only meaningful together, and a tenth `None` in a
    // call list is how a fact silently ends up in the wrong column.
    tool_result_facts: Option<&ToolResultFacts>,
    raw_facts: RawMessageFacts<'_>,
) -> Result<()> {
    crate::mark_session_presence(conn, source, session_id, SessionLocation::Local)?;
    let blank = ToolResultFacts::default();
    let tool_result_facts = tool_result_facts.unwrap_or(&blank);
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
          tool_use_id, payload_bytes, payload_truncated, payload_hash, call_index, event_index, result_status, event_source, \
          error_signal, subagent_session_id, agent_id, \
          request_id, provider_message_id, stop_reason, agent_version, is_sidechain, is_meta, turn_id, request_span, raw_facts_version) \
         VALUES (?1, ?2, ?3, \
           COALESCE((SELECT s.project_key FROM sessions s WHERE s.source = ?1 AND s.session_id = ?2), ?15), \
           CASE WHEN (SELECT s.project_key FROM sessions s WHERE s.source = ?1 AND s.session_id = ?2) IS NOT NULL \
                THEN (SELECT s.project_key_method FROM sessions s WHERE s.source = ?1 AND s.session_id = ?2) \
                ELSE ?16 END, \
           ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, \
           ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, \
           ?28, ?29, ?30, ?31, ?32, ?33, ?34, ?35, ?36) \
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
         model=excluded.model, token_json=excluded.token_json, \
         tool_use_id=excluded.tool_use_id, payload_bytes=excluded.payload_bytes, \
         payload_truncated=excluded.payload_truncated, payload_hash=excluded.payload_hash, \
         call_index=excluded.call_index, event_index=excluded.event_index, \
         result_status=excluded.result_status, event_source=excluded.event_source, \
         error_signal=excluded.error_signal, subagent_session_id=excluded.subagent_session_id, \
         agent_id=excluded.agent_id, request_id=excluded.request_id, \
         provider_message_id=excluded.provider_message_id, \
         stop_reason=excluded.stop_reason, agent_version=excluded.agent_version, \
         is_sidechain=excluded.is_sidechain, is_meta=excluded.is_meta, turn_id=excluded.turn_id, \
         request_span=excluded.request_span, \
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
            tool_result_facts.tool_use_id,
            tool_result_facts.payload_bytes,
            tool_result_facts.payload_truncated.map(i64::from),
            tool_result_facts.payload_hash,
            tool_result_facts.call_index,
            tool_result_facts.event_index,
            tool_result_facts.result_status,
            tool_result_facts.event_source,
            tool_result_facts.error_signal,
            tool_result_facts.subagent_session_id,
            tool_result_facts.agent_id,
            identity.request_id.or(raw_facts.request_id),
            identity.provider_message_id,
            raw_facts.stop_reason,
            raw_facts.agent_version,
            raw_facts.is_sidechain,
            raw_facts.is_meta,
            raw_facts.turn_id,
            raw_facts.request_span,
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
    upsert_session_inner(
        conn,
        session_id,
        source,
        cwd,
        git_branch,
        first_ts,
        last_ts,
        last_assistant_text,
        raw_path,
        ActivityWindow::Expand,
    )
}

/// How an upsert reconciles the activity window it carries with the one the
/// row already has.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ActivityWindow {
    /// Widen the stored window — the default, and right whenever this pass saw
    /// only part of the session.
    Expand,
    /// Replace both endpoints. Only for a caller that just re-read the
    /// session's *entire* source and is therefore authoritative about both
    /// ends. A rebuild that expanded instead would keep an endpoint an earlier
    /// parser derived from the file mtime forever: MAX() can never retract it,
    /// because a real recorded timestamp is almost always smaller.
    Replace,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn upsert_session_rebuilt(
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
    upsert_session_inner(
        conn,
        session_id,
        source,
        cwd,
        git_branch,
        first_ts,
        last_ts,
        last_assistant_text,
        raw_path,
        ActivityWindow::Replace,
    )
}

#[allow(clippy::too_many_arguments)]
fn upsert_session_inner(
    conn: &Connection,
    session_id: &str,
    source: &str,
    cwd: Option<&str>,
    git_branch: Option<&str>,
    first_ts: i64,
    last_ts: i64,
    last_assistant_text: Option<&str>,
    raw_path: Option<&str>,
    window: ActivityWindow,
) -> Result<()> {
    // `last_assistant_text` follows the same rule as the window. A pass that
    // saw only part of the session must not erase prose it did not read, but a
    // rebuild has just re-read the whole source: if there is no assistant
    // prose in it any more, keeping the old value would leave the catalog
    // quoting a reply the transcript no longer contains.
    let (first_activity, last_activity, assistant_text) = match window {
        ActivityWindow::Expand => (
            "first_activity_ms = MIN(COALESCE(sessions.first_activity_ms, excluded.first_activity_ms), excluded.first_activity_ms)",
            "last_activity_ms = MAX(COALESCE(sessions.last_activity_ms, excluded.last_activity_ms), excluded.last_activity_ms)",
            "last_assistant_text = COALESCE(excluded.last_assistant_text, sessions.last_assistant_text)",
        ),
        ActivityWindow::Replace => (
            "first_activity_ms = excluded.first_activity_ms",
            "last_activity_ms = excluded.last_activity_ms",
            "last_assistant_text = excluded.last_assistant_text",
        ),
    };
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
         {first_activity}, \
         {last_activity}, \
         {assistant_text}, \
         raw_path = COALESCE(excluded.raw_path, sessions.raw_path), \
         parser_version = excluded.parser_version, \
         discovery_state = 'full'"
        ),
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
    /// True when the byte cursor started this transcript from the beginning —
    /// a first sight of the file, a Cursor rewrite, or the one re-read a
    /// retired state key forces after a parser upgrade. Only then may the
    /// session's existing `history` rows be rebuilt, because only then can
    /// their timestamps have come from a previous parser.
    restarted: bool,
    /// The byte offset this sync resumed from. Records before it are already
    /// in `history`; see [`ingest_cursor_transcript`].
    history_from_offset: u64,
    /// How far the byte scan actually got, and therefore how far the
    /// checkpoint this run will commit reaches. Indexing must not go past it.
    scanned_through: u64,
    /// The generation the scan read. Re-checked before indexing.
    generation: Option<CursorGeneration>,
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
    sync_cursor_with_hooks(conn, state, root, before_transcript, &mut |_| {})
}

/// `after_read` runs between a transcript's byte scan and the cursor that scan
/// commits — the one window in which Cursor can replace a file the scan has
/// already read but not yet identified. Production passes a no-op.
fn sync_cursor_with_hooks(
    conn: &Connection,
    state: &mut Map<String, Value>,
    root: &Path,
    before_transcript: &mut dyn FnMut(&Path),
    after_read: &mut dyn FnMut(&Path),
) -> Result<usize> {
    if !root.exists() {
        return Ok(0);
    }
    // Finish provider I/O and parsing before taking the destination writer lock.
    let prepared = prepare_cursor_sync(state, root, before_transcript, after_read)
        .with_context(|| format!("prepare Cursor transcripts from {}", root.display()))?;
    // Match rollback to the source-wide checkpoint boundary: replaying committed
    // prompts after a file's mtime changes can duplicate them.
    let tx = conn.unchecked_transaction()?;
    let mut inserted = 0;
    let mut cursor_state = prepared.cursor_state;
    for mut transcript in prepared.transcripts {
        // Cursor can replace a transcript between the scan and this read. The
        // scan's `restarted`, `history_from_offset` and `scanned_through` then
        // describe a file that is gone, and using them commits a mixture of
        // two generations: evidence keyed on the old offsets is never cleared,
        // because `restarted` is false, and every prompt in the new file
        // before the old resume point is skipped. A replacement is a rewrite,
        // so re-scan the generation that is actually there and treat it as
        // one. The re-scan is provider I/O under the destination lock, which
        // the fast path deliberately avoids — it happens only on this rare
        // path, and the alternative is committing evidence that is wrong.
        // A file that is simply *gone* is not a replacement: there is nothing
        // to re-scan, and the read below reports it with the message the
        // rollback path is written against. An *append* is not a replacement
        // either — it changes length and mtime while leaving every scanned
        // byte alone, and `cursor_generation` is deliberately blind to that so
        // the common case stays on the resume path. Only a file whose scanned
        // prefix or identity has changed is a rewrite.
        if cursor_transcript_was_replaced(&transcript.path, transcript.generation.as_ref())? {
            let rescan = scan_cursor_transcript(&transcript.path, None).with_context(|| {
                format!(
                    "re-scan replaced Cursor transcript {}",
                    transcript.path.display()
                )
            })?;
            transcript.restarted = true;
            transcript.history_from_offset = 0;
            transcript.scanned_through = rescan.consumed_through;
            transcript.timestamp_ms = rescan.timestamp_ms;
            transcript.generation = rescan.generation;
            // The checkpoint the scan prepared describes the old generation,
            // so replace it with the one this re-scan validated.
            if let Some(checkpoint) = rescan.checkpoint {
                cursor_state.insert(transcript.path.to_string_lossy().to_string(), checkpoint);
            }
        }
        // A transcript read from its start is a rebuild: its prompts may carry
        // timestamps a previous parser took from the file mtime, and its
        // events, tool calls and file edits are keyed on byte offsets from a
        // generation of the file that no longer exists. Clear all of it and
        // rebuild from the file, the way the Codex rollout repair does.
        if transcript.restarted {
            clear_cursor_session_evidence(&tx, &transcript.session_id)?;
        }
        let outcome = ingest_cursor_transcript(
            &tx,
            &transcript.path,
            &transcript.session_id,
            Some(&transcript.project),
            transcript.timestamp_ms,
            transcript.history_from_offset,
            transcript.scanned_through,
        )
        .with_context(|| format!("index Cursor transcript {}", transcript.path.display()))?;
        // The check above is a moment before the read, so a replacement can
        // still land between the two and be read with the old scan's offsets.
        // Re-check afterwards and fail rather than commit a mixture of two
        // generations: the transaction and the checkpoint roll back together,
        // and the next sync sees the new file from zero. An append again does
        // not trip this, because the generation is identified by content.
        // A missing generation fails here for the same reason it forces a
        // rebuild above: the run could not identify what it read, which is not
        // a reason to trust it.
        anyhow::ensure!(
            transcript.generation.as_ref().is_some_and(|generation| {
                cursor_generation_intact(&transcript.path, generation).unwrap_or(false)
            }),
            "Cursor transcript {} was replaced while it was being indexed",
            transcript.path.display()
        );
        inserted += outcome.prompts_inserted;
        // A restarted read saw the whole file, so it owns both endpoints; an
        // incremental one saw only the tail and may only widen the window.
        let upsert = if transcript.restarted {
            upsert_session_rebuilt
        } else {
            upsert_session
        };
        upsert(
            &tx,
            &transcript.session_id,
            "cursor",
            Some(&transcript.project),
            None,
            outcome.first_ts_ms.unwrap_or(transcript.timestamp_ms),
            outcome.last_ts_ms.unwrap_or(transcript.timestamp_ms),
            outcome.last_assistant_text.as_deref(),
            Some(&transcript.path.to_string_lossy()),
        )?;
    }
    tx.commit().context("commit Cursor sync")?;
    state.insert(
        CURSOR_SYNC_STATE_KEY.to_string(),
        Value::Object(cursor_state),
    );
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
    timestamp_ms: i64,
    parse_errors: usize,
    checkpoint: Option<Value>,
    /// The byte cursor opened this file at its start.
    restarted: bool,
    /// The read found bytes this store has not seen.
    advanced: bool,
    /// Where the byte cursor resumed from.
    resumed_from: u64,
    /// The byte position the scan consumed through, which is what this run's
    /// checkpoint will record.
    consumed_through: u64,
    /// The generation this scan read, re-checked before its offsets are used
    /// to index. Identified by content, so an append does not look like a
    /// replacement.
    generation: Option<CursorGeneration>,
}

fn scan_cursor_transcript(jsonl: &Path, saved: Option<&Value>) -> Result<ScannedCursorTranscript> {
    scan_cursor_transcript_with(jsonl, saved, &mut |_| {})
}

/// `after_read` runs after the byte scan has read what it is going to read and
/// before the cursor it commits is validated. That is the window in which a
/// replacement is invisible to both ends of the scan, so it is the only place
/// a test can put one; production passes a no-op.
fn scan_cursor_transcript_with(
    jsonl: &Path,
    saved: Option<&Value>,
    after_read: &mut dyn FnMut(&Path),
) -> Result<ScannedCursorTranscript> {
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
    let mut parse_errors = 0;
    if offset < size {
        let mut line = String::new();
        while let Some(position) = source
            .next_line(&mut line)
            .with_context(|| format!("read Cursor transcript {}", jsonl.display()))?
        {
            consumed = position;
            // The scan advances and validates the byte cursor; indexing happens
            // over the whole file afterwards. A malformed record is still
            // counted here so the sync note reports it.
            if parse_cursor_text(&line).is_err() {
                parse_errors += 1;
            }
        }
    }
    after_read(jsonl);
    let saved_offset = saved
        .and_then(FileCursor::decode)
        .map(|decoded| match decoded {
            DecodedFileCursor::Typed(cursor) => cursor.offset,
            DecodedFileCursor::Legacy(offset) => offset,
        });
    let opened_cursor = source.cursor.to_value();
    // The generation comes from the cursor this scan commits, whose prefix
    // hash is the digest of the bytes the reader *actually read* — not a
    // re-read of the file afterwards. That distinction is the whole point: a
    // re-read would hash whatever is on disk at that later moment, so a
    // replacement landing between the read and the identification would be
    // recorded as the generation the scan saw and then compare equal at the
    // index check, laundering the new file in under the old scan's offsets.
    // Hashing what was read closes that window by construction.
    let committed = if consumed != offset || saved != Some(&opened_cursor) {
        Some(
            source
                .committed_cursor(consumed, true)
                .with_context(|| format!("validate Cursor transcript {}", jsonl.display()))?,
        )
    } else {
        None
    };
    // A cursor the reader *reset* is not this scan's generation. It describes
    // the file that replaced the one being read — offset zero, the
    // replacement's identity, the empty-prefix hash — while `restarted`,
    // `resumed_from` and `consumed_through` below still describe the file that
    // is gone. Stamping the scan with it would hand the write phase an
    // identity that matches the new file exactly, so the check would find
    // "no replacement" and index the new file from the old one's resume point
    // with none of the old evidence cleared. The generation and the offsets
    // have to move together, so a reset makes the generation *unidentifiable*
    // and the write phase re-scans: `restarted`, `history_from_offset` and
    // `scanned_through` then all come from the one read that saw the
    // replacement whole.
    let replaced_mid_scan = source.reset_cursor.is_some();
    let generation = if replaced_mid_scan {
        None
    } else {
        match &committed {
            Some(cursor) => cursor
                .prefix_hash
                .as_ref()
                .map(|prefix_hash| CursorGeneration {
                    device: cursor.generation.device,
                    inode: cursor.generation.inode,
                    prefix_hash: prefix_hash.clone(),
                    prefix_len: cursor.offset,
                }),
            // Nothing advanced and the saved cursor still describes the file,
            // so there is nothing to index and no offsets to protect.
            None => cursor_generation(jsonl, consumed)?,
        }
    };
    Ok(ScannedCursorTranscript {
        timestamp_ms,
        parse_errors,
        checkpoint: committed.map(|cursor| cursor.to_value()),
        restarted: offset == 0,
        // A replacement is work even when the read that found it consumed
        // nothing: the session's stored evidence belongs to a file that no
        // longer exists, and only a queued transcript gets re-scanned and
        // rebuilt. Leaving it unqueued would keep that evidence until some
        // later sync happened to find new bytes.
        //
        // `replaced_mid_scan` catches a rewrite the reader noticed *while*
        // reading. A rewrite to empty is noticed in `open` instead: the saved
        // cursor had a positive offset, the file is now shorter, and the
        // reader starts at zero with both offsets equal, so neither
        // `consumed != offset` nor `reset_cursor` fires. The checkpoint
        // still advances (the opened cursor is a new generation), and
        // without this the obsolete rows stay.
        advanced: consumed != offset
            || replaced_mid_scan
            || (offset == 0 && saved_offset.is_some_and(|previous| previous > 0)),
        resumed_from: offset,
        consumed_through: consumed,
        // Re-checked by the index phase, because a replacement can still land
        // after the scan has finished.
        generation,
    })
}

/// The identity of the generation a scan read, by **content** rather than by
/// "anything changed".
///
/// Cursor appends to a transcript constantly, and an append changes both the
/// length and the mtime while leaving every byte the scan read untouched. A
/// stamp that compares those fields calls that a replacement, and the caller
/// would then rebuild the whole session — clearing its evidence, restamping
/// untimed turns with the new mtime and indexing past the bound that exists
/// to leave the append for the next sync. So the identity is the file's
/// device and inode plus a hash of exactly the prefix the scan consumed: an
/// append leaves it intact, and a rewrite, a truncation or a new inode does
/// not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CursorGeneration {
    pub device: Option<u64>,
    pub inode: Option<u64>,
    /// SHA-256 of bytes `[0, prefix_len)`.
    prefix_hash: String,
    prefix_len: u64,
}

/// Identify the generation of `path`, hashing the first `through` bytes.
/// `Ok(None)` means one thing only: the file is shorter than the prefix the
/// scan read, which is the truncate half of a rewrite and therefore a
/// different generation. Every other failure — a stat that fails, a read that
/// fails — is an `Err`, because folding those into `None` would launder an
/// I/O or permission fault into "this was replaced" and hand back a confident
/// rebuild for a file nobody could read. A caller that cannot read a file
/// should say so, with the path and the reason.
pub(crate) fn cursor_generation(path: &Path, through: u64) -> Result<Option<CursorGeneration>> {
    let metadata = fs::metadata(path)
        .with_context(|| format!("identify Cursor transcript {}", path.display()))?;
    if !metadata.is_file() {
        // `Ok(None)` means a regular file shrank below the scanned prefix.
        // A directory's `len` is often smaller than that prefix (especially
        // on macOS), so the length check would call this a truncation and
        // rebuild. Open it so the I/O error names what it actually is.
        fs::File::open(path)
            .and_then(|mut file| file.read(&mut [0u8; 1]).map(|_| ()))
            .with_context(|| format!("identify Cursor transcript {}", path.display()))?;
        anyhow::bail!(
            "identify Cursor transcript {}: not a regular file",
            path.display()
        );
    }
    let (device, inode) = metadata_identity(&metadata);
    if metadata.len() < through {
        return Ok(None);
    }
    let Some(prefix_hash) = hash_prefix_bytes(path, through)
        .with_context(|| format!("identify Cursor transcript {}", path.display()))?
    else {
        return Ok(None);
    };
    Ok(Some(CursorGeneration {
        device,
        inode,
        prefix_hash,
        prefix_len: through,
    }))
}

/// SHA-256 of the first `through` bytes, or `None` if the file is shorter.
///
/// Deliberately *not* [`hash_file_prefix`], which also asserts the prefix ends
/// on a newline. That assertion belongs to cursor validation, not to identity:
/// a rewrite whose bytes happen not to align to the old line boundary is a
/// different generation, not a read failure, and surfacing it as an error
/// would turn an ordinary rewrite into a failed sync. Only a real I/O failure
/// is an error here.
fn hash_prefix_bytes(path: &Path, through: u64) -> Result<Option<String>> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    let mut remaining = through;
    while remaining > 0 {
        let wanted = usize::try_from(remaining.min(buffer.len() as u64)).unwrap_or(buffer.len());
        let read = file.read(&mut buffer[..wanted])?;
        if read == 0 {
            // Shorter than the prefix the scan read: a different generation.
            return Ok(None);
        }
        hasher.update(&buffer[..read]);
        remaining -= read as u64;
    }
    Ok(Some(finish_prefix_hash(&hasher)))
}

/// Is the file still the generation `generation` describes?
///
/// A file that cannot be stated, has shrunk below what the scan read, or whose
/// scanned prefix no longer hashes the same is a different generation. An
/// append is not.
pub(crate) fn cursor_generation_intact(path: &Path, generation: &CursorGeneration) -> Result<bool> {
    Ok(cursor_generation(path, generation.prefix_len)?
        .is_some_and(|current| &current == generation))
}

/// Must this transcript be rebuilt rather than resumed?
///
/// The `None` case is the subtle one. `cursor_generation` yields `None` when
/// the file is shorter than the prefix the scan read — the truncate half of a
/// rewrite — so a scan that ended while Cursor was rewriting queues the
/// transcript with no generation at all. Reading that as "nothing to compare,
/// carry on" would use the old `restarted` and `history_from_offset` against
/// the new file, keeping stale evidence and skipping the replacement's
/// prefix. An unidentifiable generation is by definition not the one the scan
/// saw, so it is a replacement.
///
/// A file that is simply *gone* is still not a replacement: there is nothing
/// to re-scan, and the read reports it with the message the rollback path is
/// written against.
pub(crate) fn cursor_transcript_was_replaced(
    path: &Path,
    generation: Option<&CursorGeneration>,
) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    match generation {
        Some(generation) => Ok(!cursor_generation_intact(path, generation)?),
        None => Ok(true),
    }
}

fn prepare_cursor_sync(
    state: &Map<String, Value>,
    root: &Path,
    before_transcript: &mut dyn FnMut(&Path),
    after_read: &mut dyn FnMut(&Path),
) -> Result<PreparedCursorSync> {
    let mut cursor_state = state
        .get(CURSOR_SYNC_STATE_KEY)
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
            let scan = match scan_cursor_transcript_with(&jsonl, cursor_state.get(&key), after_read)
            {
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
            // The byte cursor is the change detector; the parser then reads the
            // whole file. Cursor's records are id-less, so every event, tool
            // call and file edit is keyed on the record's byte offset — which a
            // full re-parse reproduces exactly, and a resumed partial read
            // could not, because a window's records have no absolute position
            // of their own.
            if scan.advanced {
                transcripts.push(PreparedCursorTranscript {
                    path: jsonl,
                    session_id,
                    project: project_path.clone(),
                    timestamp_ms: scan.timestamp_ms,
                    restarted: scan.restarted,
                    history_from_offset: scan.resumed_from,
                    scanned_through: scan.consumed_through,
                    generation: scan.generation,
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

/// Timestamps a previous pass stored, keyed by the record's byte offset.
///
/// Every event derived from one record shares that record's byte offset as the
/// prefix of its `event_uid` (`"{offset}:{block}"`), so the first event at
/// each offset answers the question. Loaded once per incremental pass because
/// a `substr(event_uid, …)` predicate cannot use the uid index.
fn stored_cursor_record_timestamps(
    conn: &Connection,
    session_id: &str,
) -> Result<HashMap<u64, i64>> {
    let mut stored = HashMap::new();
    let mut stmt = conn.prepare(
        "SELECT event_uid, ts_ms FROM session_events \
         WHERE source = 'cursor' AND session_id = ? ORDER BY id",
    )?;
    let mut rows = stmt.query([session_id])?;
    while let Some(row) = rows.next()? {
        let uid: String = row.get(0)?;
        let ts: i64 = row.get(1)?;
        if let Some(offset) = uid
            .split_once(':')
            .and_then(|(prefix, _)| prefix.parse::<u64>().ok())
        {
            stored.entry(offset).or_insert(ts);
        }
    }
    Ok(stored)
}

/// Drop every row a previous read of this Cursor transcript produced.
///
/// Cursor evidence is keyed on the record's byte offset, which is stable only
/// within one generation of the file. When Cursor rewrites a transcript — or
/// when a retired state key reopens it at offset 0 — the offsets it produced
/// before name records that no longer exist there. Upserting the new read on
/// top leaves those stale rows behind, so a session keeps tool calls and file
/// edits it never made. Clearing all four tables together, inside the caller's
/// transaction, is what makes a rebuild a rebuild.
pub(crate) fn clear_cursor_session_evidence(conn: &Connection, session_id: &str) -> Result<()> {
    for table in ["history", "session_events", "tool_calls", "file_edits"] {
        conn.execute(
            &format!("DELETE FROM {table} WHERE source = 'cursor' AND session_id = ?"),
            [session_id],
        )?;
    }
    Ok(())
}

/// What one pass over a Cursor transcript established.
///
/// `used_mtime_fallback` is the honest part: Cursor records carry no timestamp
/// field, so every event whose turn had no readable `<timestamp>` tag is
/// stamped with the file mtime, and the caller reports that as a
/// `CURSOR_TIMESTAMP_FROM_MTIME` diagnostic rather than letting a
/// filesystem-derived time pass for a provider-recorded one.
#[derive(Debug, Default)]
pub(crate) struct CursorTranscriptOutcome {
    pub prompts_inserted: usize,
    pub records: i64,
    pub first_ts_ms: Option<i64>,
    pub last_ts_ms: Option<i64>,
    pub last_assistant_text: Option<String>,
    pub models: Vec<String>,
    pub used_mtime_fallback: bool,
    pub subagent_calls: usize,
    /// Byte offset after the last complete record this pass indexed.
    pub consumed_through: u64,
}

/// Index one Cursor agent transcript into every evidence table.
///
/// Mirrors [`ingest_claude_transcript_as`]: `session_events` for text,
/// thinking, tool use and tool results; `tool_calls` for every call Cursor
/// names; `file_edits` for the calls that write a file; `history` for the
/// human turns.
///
/// Two things are structurally different from Claude and drive the design:
///
/// * Cursor writes **no record identity** — no uuid, and its `tool_use` blocks
///   carry no `id`. Event and tool identity therefore come from the record's
///   byte offset in the file, which is stable across an incremental read, a
///   whole-file re-parse and a re-hydration, and resets exactly when Cursor
///   rewrites the file.
/// * Cursor writes **no timestamp field**. The only time signal is the
///   `<timestamp>` tag its client injects into a user turn; the assistant
///   records that answer that turn inherit it, because they belong to it. A
///   record with no turn time at all takes the file mtime and sets
///   `used_mtime_fallback`.
///
/// `model` and `usage` are read from `message.model` / `message.usage` when a
/// build writes them. No observed Cursor build does; the parser does not
/// invent them, and the capability matrix says so.
/// `history_from_offset` is the one place the whole-file re-parse is *not*
/// idempotent. A prompt's `history` identity is `(source, timestamp_ms,
/// prompt)`, so a turn that carries a real `<timestamp>` re-inserts harmlessly
/// — but a turn with no readable time is stamped with the file mtime, which
/// moves every time Cursor appends to the transcript, and re-inserting it would
/// leave one copy per sync. Callers that resume mid-file therefore pass the
/// offset they had already committed, and only records at or after it become
/// history. Callers that rebuild the session's history first pass `0`.
pub(crate) fn ingest_cursor_transcript(
    conn: &Connection,
    path: &Path,
    session_id: &str,
    project: Option<&str>,
    mtime_ms: i64,
    history_from_offset: u64,
    index_through: u64,
) -> Result<CursorTranscriptOutcome> {
    // An unreadable transcript is a failure, not an empty one. Swallowing the
    // error here would index the session as having no records at all — after
    // the caller has already deleted the rows it is about to rebuild — and
    // then let the byte checkpoint advance over content nobody read. The
    // error propagates so the whole transaction, including the checkpoint
    // update, rolls back and the next sync retries the same offset.
    let text = fs::read_to_string(path)
        .with_context(|| format!("read Cursor transcript {}", path.display()))?;
    let mut outcome = CursorTranscriptOutcome::default();
    let mut offset: u64 = 0;
    // One query for the session, keyed by record offset. The per-record
    // `substr(event_uid, …)` lookup cannot use the uid index, so an advanced
    // sync of a long untimed transcript would otherwise scan events once per
    // old record.
    let stored_ts = if history_from_offset > 0 {
        stored_cursor_record_timestamps(conn, session_id)?
    } else {
        HashMap::new()
    };
    // The last turn time seen while walking forward, inherited by the records
    // that answer that turn.
    let mut turn_ts: Option<i64> = None;
    // Only complete records are indexed, matching `CompleteJsonlReader` and
    // `complete_jsonl_records`. A transcript Cursor is mid-write has a partial
    // final line; indexing it would publish a truncated prompt that the next
    // read replaces at a different byte offset, leaving both.
    for line in text
        .split_inclusive('\n')
        .filter(|line| line.ends_with('\n'))
    {
        let record_offset = offset;
        offset += line.len() as u64;
        // Cursor can append between the byte scan and this read. Those bytes
        // are past the checkpoint the scan committed, so indexing them would
        // publish evidence the next sync reads again from the checkpoint —
        // and an untimed prompt re-read after the mtime moved is a duplicate,
        // not an upsert. Stop at what was actually scanned; the append is
        // picked up by the next sync, from the offset that still points at it.
        if offset > index_through {
            break;
        }
        let line = line.trim_end_matches(['\n', '\r']);
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let Some(obj) = value.as_object() else {
            continue;
        };
        let Some(role) = cursor::record_role(obj) else {
            continue;
        };
        let blocks = cursor::record_blocks(obj);
        if blocks.is_empty() {
            continue;
        }
        outcome.records += 1;
        let message = obj.get("message").and_then(Value::as_object);
        let model = message.and_then(|m| m.get("model")).and_then(Value::as_str);
        if let Some(model) = model.filter(|model| !model.is_empty()) {
            if !outcome.models.iter().any(|seen| seen == model) {
                outcome.models.push(model.to_string());
            }
        }
        let token_json = message
            .and_then(|m| m.get("usage"))
            .filter(|usage| !usage.is_null())
            .and_then(|usage| serde_json::to_string(usage).ok());
        // A build that does write a record timestamp is believed over the
        // injected tag; none observed so far does.
        let record_ts = obj
            .get("timestamp")
            .and_then(|v| v.as_str().and_then(parse_iso_ms).or_else(|| v.as_i64()))
            // A record `timestamp` is a provider field and is believed
            // whatever the role; the injected tag is a clock only in a human
            // turn's own text. See `cursor::injected_turn_time`.
            .or_else(|| cursor::injected_turn_time(Some(role), &blocks));
        // A *human* turn opens a new one, so it replaces the inherited time —
        // including with `None`. Letting an untimed human turn keep the
        // previous turn's time would date a prompt to a conversation that had
        // already ended, and would hide the fact that it was undated: the
        // mtime fallback would never fire and `CURSOR_TIMESTAMP_FROM_MTIME`
        // would never be reported.
        //
        // The role alone does not say that. Cursor writes tool results back as
        // user-role records, and a record carrying only a `tool_result` — or
        // only a marker like `turn_ended` — is an answer to the turn that is
        // already open, not a new one. Clearing the turn time for those sent
        // them to the mtime, dating a tool result hours after the call it
        // answers and putting the two halves of one exchange in disagreement.
        // Assistant records, and user records that are not human turns, move
        // the time only when they carry one of their own.
        let opens_human_turn = role == "user"
            && blocks
                .iter()
                .any(|block| block.get("type").and_then(Value::as_str) == Some("text"));
        if opens_human_turn || record_ts.is_some() {
            turn_ts = record_ts;
        }
        // True when this record's stamp is the *current* mtime standing in for
        // a time that could not be recovered — see the window update below.
        let mut ts_is_guessed = false;
        let ts_ms = match turn_ts {
            Some(ts) => ts,
            None => {
                outcome.used_mtime_fallback = true;
                // The mtime moves on every append, so re-deriving it for an
                // *old* record would silently redate evidence that was stored
                // under a different one. Worse, the `history` row for that same
                // turn is not rewritten on an incremental read, so the prompt
                // and its event would drift apart. A record this pass is only
                // re-reading keeps the stamp it already has; only records at or
                // past the resumed offset take the current mtime.
                if record_offset < history_from_offset {
                    match stored_ts.get(&record_offset).copied() {
                        Some(stored) => stored,
                        None => {
                            ts_is_guessed = true;
                            mtime_ms
                        }
                    }
                } else {
                    mtime_ms
                }
            }
        };
        let message_id = message
            .and_then(|m| m.get("id"))
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| format!("cursor:{record_offset}"));
        // The window covers every record this pass *stamps with evidence*, not
        // only the ones that carried a recorded time. An undated turn is still
        // stamped — with the file mtime — and still produces events at that
        // time, so leaving it out made the catalog claim a recency older than
        // the session's own newest event. Where a time was recorded it is the
        // one used, so a fully dated session still reports its recorded times
        // rather than the mtime.
        //
        // Two records must stay out of it. One that emits nothing has no event
        // to be the recency *of*: a `turn_ended` marker is the whole record,
        // and on a re-read it also has no stored event to recover a time from,
        // so it fell back to the current mtime — which moves on every append —
        // and dragged the window to "now" on every sync. And a pre-resume
        // record whose original time cannot be recovered is stamped with a
        // guess, which is not evidence of when anything happened. So the
        // window is taken after the blocks, from records that actually stored
        // something, and never from a guessed stamp.
        let mut emitted_evidence = false;
        for (block_index, block) in blocks.iter().enumerate() {
            let block_type = block.get("type").and_then(Value::as_str).unwrap_or("");
            let event_uid = format!("{record_offset}:{block_index}");
            match block_type {
                "text" => {
                    let Some(raw) = block.get("text").and_then(Value::as_str) else {
                        continue;
                    };
                    let is_user = role == "user";
                    let text = if is_user {
                        cursor::unwrap_user_text(raw)
                    } else {
                        raw.trim().to_string()
                    };
                    if text.is_empty() {
                        continue;
                    }
                    if !is_user {
                        // Same 4096-character cap discovery writes. Hydrating a
                        // long reply used to store the full block and rewrite
                        // the catalog summary the agreement test exists to keep
                        // stable.
                        outcome.last_assistant_text = Some(crate::discover::excerpt(&text));
                    }
                    emitted_evidence = true;
                    insert_session_event(
                        conn,
                        "cursor",
                        session_id,
                        project,
                        project,
                        None,
                        &message_id,
                        None,
                        ts_ms,
                        if is_user { "user" } else { "assistant" },
                        "text",
                        Some(&text),
                        model,
                        token_json.as_deref(),
                        // Cursor records no request identity on this path.
                        RequestIdentity::none(),
                        &event_uid,
                        None,
                        RawMessageFacts::default(),
                    )?;
                }
                "thinking" | "reasoning" => {
                    let thinking = block
                        .get("thinking")
                        .or_else(|| block.get("text"))
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|text| !text.is_empty());
                    if let Some(thinking) = thinking {
                        emitted_evidence = true;
                        insert_session_event(
                            conn,
                            "cursor",
                            session_id,
                            project,
                            project,
                            None,
                            &message_id,
                            None,
                            ts_ms,
                            "assistant",
                            "thinking",
                            Some(thinking),
                            model,
                            token_json.as_deref(),
                            RequestIdentity::none(),
                            &event_uid,
                            None,
                            RawMessageFacts::default(),
                        )?;
                    }
                }
                "tool_use" | "tool_call" => {
                    let name = block
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    if name.is_empty() {
                        continue;
                    }
                    let args = block
                        .get("input")
                        .or_else(|| block.get("args"))
                        .unwrap_or(&Value::Null);
                    let target = cursor::pick_tool_target(&name, args);
                    // Cursor writes no `id` on a tool_use block, so the call is
                    // addressed by where it sits in the file. A build that does
                    // write one is preferred, because it also links a
                    // tool_result.
                    let tool_use_id = block
                        .get("id")
                        .or_else(|| block.get("tool_use_id"))
                        .and_then(Value::as_str)
                        .filter(|id| !id.is_empty())
                        .map(str::to_string)
                        .unwrap_or_else(|| format!("cursor:{record_offset}:{block_index}"));
                    if cursor::is_subagent_tool(&name) {
                        outcome.subagent_calls += 1;
                    }
                    emitted_evidence = true;
                    insert_session_event(
                        conn,
                        "cursor",
                        session_id,
                        project,
                        project,
                        None,
                        &message_id,
                        None,
                        ts_ms,
                        "assistant",
                        "tool_use",
                        Some(&format_tool_event_text(&name, target.as_deref(), args)),
                        model,
                        token_json.as_deref(),
                        // Cursor records no request identity on this path.
                        RequestIdentity::none(),
                        &event_uid,
                        None,
                        RawMessageFacts::default(),
                    )?;
                    insert_tool_call(
                        conn,
                        "cursor",
                        session_id,
                        &message_id,
                        &tool_use_id,
                        &name,
                        target.as_deref(),
                        &serde_json::to_string(args).unwrap_or_else(|_| "null".to_string()),
                        None,
                        ts_ms,
                    )?;
                    if cursor::is_file_edit_tool(&name) {
                        // One `ApplyPatch` call routinely rewrites several
                        // files. `file_edits` keys on `tool_use_id`, so each
                        // path gets its own scoped id and its own slice of the
                        // patch — the same shape the Codex `patch_apply_end`
                        // path uses. Taking only the first header, as this
                        // once did, dropped every later file in the patch.
                        let patched = cursor::patch_text(args)
                            .map(cursor::split_patch_files)
                            .unwrap_or_default();
                        if patched.is_empty() {
                            // A non-patch edit tool (Write, StrReplace) names
                            // exactly one file and carries no diff, so its
                            // edit keeps the unscoped call id and no line
                            // counts are invented for it.
                            if let Some(file_path) = target.as_deref() {
                                upsert_file_edit_from_call(
                                    conn,
                                    "cursor",
                                    session_id,
                                    &message_id,
                                    &tool_use_id,
                                    file_path,
                                    &name,
                                    ts_ms,
                                    None,
                                    project,
                                )?;
                            }
                        } else {
                            for file in &patched {
                                let edit_id = format!("{tool_use_id}#{}", file.path);
                                upsert_file_edit_from_call(
                                    conn,
                                    "cursor",
                                    session_id,
                                    &message_id,
                                    &edit_id,
                                    &file.path,
                                    &name,
                                    ts_ms,
                                    None,
                                    project,
                                )?;
                                // Line counts come from this file's own slice
                                // of the patch, through the same counter the
                                // Claude edits use.
                                update_file_edit_from_tool_result(
                                    conn,
                                    "cursor",
                                    session_id,
                                    &message_id,
                                    &edit_id,
                                    &json!({ "structuredPatch": file.patch }),
                                    ts_ms,
                                    None,
                                    project,
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
                        .unwrap_or_default()
                        .to_string();
                    let content = block.get("content").unwrap_or(&Value::Null);
                    emitted_evidence = true;
                    insert_session_event(
                        conn,
                        "cursor",
                        session_id,
                        project,
                        project,
                        None,
                        &message_id,
                        None,
                        ts_ms,
                        "tool_result",
                        "tool_result",
                        materialize_tool_result_text(content).as_deref(),
                        model,
                        token_json.as_deref(),
                        // Cursor records no request identity on this path.
                        RequestIdentity::none(),
                        &event_uid,
                        None,
                        RawMessageFacts::default(),
                    )?;
                    if !tool_use_id.is_empty() {
                        if let Some(is_error) = block.get("is_error").and_then(Value::as_bool) {
                            set_tool_call_error(
                                conn,
                                "cursor",
                                session_id,
                                &tool_use_id,
                                is_error,
                            )?;
                        }
                        if let Some(result) = find_tool_use_result(block) {
                            update_file_edit_from_tool_result(
                                conn,
                                "cursor",
                                session_id,
                                &message_id,
                                &tool_use_id,
                                result,
                                ts_ms,
                                None,
                                project,
                            )?;
                        }
                    }
                }
                // `turn_ended` closes a turn; it is a marker, not an event, and
                // there is no `session_events.kind` that it honestly is.
                _ => {}
            }
        }
        // One row for the turn, after the blocks, so the prompt carries
        // everything the person wrote in it. `session_events` keeps the blocks
        // apart because that is what the record says; `history` does not, for
        // two reasons. A person typed one message, and searching for it should
        // find one row. And `history`'s identity is
        // `(source, timestamp_ms, prompt)`, so two blocks that happen to carry
        // the same text in one turn would collide on insert and silently store
        // one row for two events -- the tables would then disagree about how
        // many times the person said it.
        //
        // `cursor::human_turn_prompt` is the same function the catalog's
        // `first_prompt` goes through, so the two cannot drift. Records before
        // the offset this read resumed from are already in `history` under a
        // timestamp this pass must not restate; see `history_from_offset`.
        let turn_prompt = (role == "user")
            .then(|| cursor::human_turn_prompt(&blocks))
            .flatten();
        if let Some(prompt) = turn_prompt.filter(|_| record_offset >= history_from_offset) {
            outcome.prompts_inserted += insert_history(
                conn,
                &HistoryEntry {
                    id: 0,
                    source: "cursor".into(),
                    session_id: Some(session_id.to_string()),
                    project: project.map(str::to_string),
                    prompt_hash: Some(prompt_hash(&prompt)),
                    prompt,
                    timestamp_ms: ts_ms,
                },
            )?;
        }
        if emitted_evidence && !ts_is_guessed {
            outcome.first_ts_ms = Some(outcome.first_ts_ms.map_or(ts_ms, |first| first.min(ts_ms)));
            outcome.last_ts_ms = Some(outcome.last_ts_ms.map_or(ts_ms, |last| last.max(ts_ms)));
        }
    }
    outcome.consumed_through = offset;
    Ok(outcome)
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
        .get(GROK_SYNC_STATE_KEY)
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let mut inserted = 0;
    let mut scanned = 0;
    let mut sessions = 0;
    let mut errors = 0;
    // Sessions this run could **account for**: indexed now, or confirmed
    // unchanged against a stamp it read successfully. Not the same as
    // `sessions`, which counts only what was indexed — a store that is fully
    // synced indexes nothing on every later run, and using that count to
    // decide whether the source failed would fail it forever over one
    // unreadable sibling.
    let mut accounted = 0;
    for chat in capture_files(
        "grok",
        collect_matching_files(root, "chat_history", "jsonl")?,
    ) {
        let key = chat.to_string_lossy().to_string();
        // One unreadable session directory does not stop the rest, and it does
        // not update its saved stamp either: the next run tries again. It is
        // counted as looked at, so the run says so — a stamp that fails
        // closed and is then reported as nothing at all is the silence the
        // strictness exists to prevent.
        let stamp = match grok_session_stamp(&chat) {
            Ok(stamp) => stamp,
            Err(error) => {
                scanned += 1;
                errors += 1;
                sync_note!("  [grok] unreadable session {}: {error:#}", chat.display());
                continue;
            }
        };
        let recorded = grok_state.get(&key);
        let recorded_stamp = recorded.and_then(grok_state_stamp).map(str::to_string);
        let recorded_session = recorded.and_then(grok_state_session).map(str::to_string);
        let recorded_evidence = recorded.and_then(grok_state_had_evidence);
        if recorded_stamp.as_deref() == Some(stamp.as_str()) {
            // The stamp says the file has not changed. That is only half the
            // question: `.sync-state.json` lives beside the database, so a
            // deleted or rebuilt `history.db` keeps this file, and a stamp
            // read on its own would answer "already indexed" for a session
            // whose rows are gone -- until some file in the directory changes,
            // which for a finished session is never. The Codex and Claude
            // walks check the database for the same reason.
            //
            // The other half of that is knowing what "its rows" means for
            // *this* session. Not every Grok session produces `session_events`
            // -- one made only of `system` lines, synthetic turns or encrypted
            // reasoning is stored entirely as markers, and one with an empty
            // transcript and a `subagents/` entry only a relationship -- so
            // asking only about events would find nothing and re-read such a
            // session on every run, forever. The answer is not to guess from
            // the tables but to record what the indexing run actually
            // produced, and then to ask about that.
            match (recorded_session.as_deref(), recorded_evidence) {
                // Written by an older build that saved the stamp alone, so
                // there is no session id to look the evidence up by. Trust
                // the stamp, as that build did.
                (None, _) => {
                    accounted += 1;
                    continue;
                }
                // The run that indexed it wrote no evidence rows at all. It
                // still wrote a catalog row -- every ingestion does -- so
                // that is what there is to check, and it is enough: a session
                // with no evidence *is* its catalog row. Skipping without
                // asking anything is what let an empty session disappear for
                // good when the database was rebuilt.
                (Some(id), Some(false)) if grok_catalog_row_exists(conn, id)? => {
                    accounted += 1;
                    continue;
                }
                (Some(id), Some(true) | None) if grok_evidence_exists(conn, id)? => {
                    accounted += 1;
                    continue;
                }
                // Unchanged, and what it did write is missing: re-index it.
                _ => {}
            }
        }
        scanned += 1;
        match scan_grok_session_file(&chat) {
            Ok(Some(session)) => {
                let raw_path = chat.to_string_lossy().to_string();
                let session_id = session.session_id.clone();
                // One session directory is one transaction: its evidence is
                // replaced, not merged, and a reader must never see the gap
                // between the two halves of that.
                let tx = conn.unchecked_transaction()?;
                let outcome = ingest_grok_session(&tx, &session, &raw_path)?;
                inserted += outcome.prompts;
                tx.commit()?;
                // Whether this session has any evidence to go looking for on
                // a later run. A session stored entirely as markers has no
                // events; one whose transcript is empty but whose
                // `subagents/` directory names a child has only a
                // relationship. Every writer this ingestion drives has to be
                // counted here, or a later run asks about a table this
                // session never filled and re-reads it for ever.
                let had_evidence =
                    outcome.events > 0 || outcome.markers > 0 || outcome.relationships > 0;
                sessions += 1;
                accounted += 1;
                // The session id travels with the stamp so the next run can
                // ask the database whether this evidence is still there, and
                // `evidence` says whether there was any to ask about.
                grok_state.insert(
                    key,
                    json!({ "stamp": stamp, "session": session_id, "evidence": had_evidence }),
                );
            }
            Ok(None) => {
                // Read fine, and there was no session in it -- so there is no
                // session id to check any future run against either.
                accounted += 1;
                grok_state.insert(key, json!({ "stamp": stamp }));
            }
            Err(error) => {
                errors += 1;
                sync_note!("  [grok] unreadable session {}: {error:#}", chat.display());
            }
        }
    }
    state.insert(GROK_SYNC_STATE_KEY.to_string(), Value::Object(grok_state));
    if scanned > 0 {
        let suffix = if errors > 0 {
            format!(" ({errors} errors)")
        } else {
            String::new()
        };
        sync_note!("  [grok] +{inserted} rows from {sessions} sessions{suffix}");
    }
    // A store where nothing could be read is a failed source, not an empty
    // one: the caller records it and the run says so. A store where something
    // failed and something else was accounted for stays a success, so the
    // sessions that did read keep their saved stamps -- otherwise one
    // unreadable directory would send every future run back over the whole
    // store, and a fully synced store would fail this source on every run
    // from then on.
    if errors > 0 && accounted == 0 {
        anyhow::bail!(
            "{errors} Grok session(s) could not be read and none were indexed; \
             the unreadable directories are named above"
        );
    }
    Ok(inserted)
}

/// What indexing one Grok session directory produced, and what its own records
/// could not establish.
///
/// The fallback counters are not diagnostics of *this* run: they are how many
/// facts Grok did not write down, which is what a caller has to be told before
/// it trusts a timestamp.
#[derive(Debug, Default, Clone)]
pub(crate) struct GrokIngestOutcome {
    pub prompts: usize,
    pub events: usize,
    pub tool_calls: usize,
    pub file_edits: usize,
    pub markers: usize,
    pub relationships: usize,
    /// Subagent metadata files that named no child session id.
    pub unlinked_subagents: usize,
    /// `Task`-style calls in the transcript. Grok names no child in the call
    /// itself, so these produce a tool call and never a relationship row.
    pub subagent_calls: usize,
    /// Records whose time came from their turn's `turnStartMs`.
    pub turn_start_fallbacks: usize,
    /// Records that inherited the previous record's time.
    pub inherited_fallbacks: usize,
    /// Records stamped with the session's `created_at`.
    pub session_start_fallbacks: usize,
    pub encrypted_reasoning: usize,
    pub unread_update_rows: usize,
    /// The session directory had no `updates.jsonl` at all.
    pub missing_updates: bool,
    /// The stream was there and established no timing at all — no turn, no
    /// message group, no tool. Distinct from `missing_updates`, because the
    /// two want different answers: a session Grok wrote no updates for is
    /// normal, and a stream this parser could not use is a gap in coverage
    /// worth knowing about.
    pub updates_yielded_no_timing: bool,
    /// The newest `turn_completed` context snapshot, for the caller's report.
    pub context_total_tokens: Option<i64>,
}

/// Index one Grok session directory: prompts, events, tools, edits, markers
/// and subagent delegations.
fn ingest_grok_session(
    conn: &Connection,
    session: &GrokSession,
    raw_path: &str,
) -> Result<GrokIngestOutcome> {
    const SOURCE: &str = "grok";
    let sid = session.session_id.as_str();
    let project = session.cwd.as_deref();
    let branch = session.git_branch.as_deref();
    replace_grok_session_evidence(conn, sid)?;
    let mut outcome = GrokIngestOutcome {
        missing_updates: !session.updates_present,
        updates_yielded_no_timing: session.updates_present && session.updates.is_empty(),
        unread_update_rows: session.updates.unread_rows,
        context_total_tokens: session
            .updates
            .turns
            .iter()
            .rev()
            .find_map(|turn| turn.total_tokens),
        ..Default::default()
    };

    let mut user_ordinal = 0usize;
    let mut message_ordinal = 0usize;
    let mut thought_ordinal = 0usize;
    let mut first_prompt: Option<String> = None;
    // Chat-side turn index: `None` until the first typed prompt, because the
    // system preamble belongs to no turn.
    let mut turn: Option<usize> = None;
    let mut inherited: Option<i64> = None;
    // Where each turn's context-token snapshot is recorded: the turn's last
    // assistant prose event, or — for a turn whose whole answer was tool calls
    // and no prose, which is an ordinary Grok turn — its last tool-use event.
    // Keeping the two separate means a tool call after a message does not
    // displace the message, and a turn with no message still has a tail.
    let mut turn_tail: HashMap<usize, String> = HashMap::new();
    let mut turn_tool_tail: HashMap<usize, String> = HashMap::new();

    for (idx, line) in session.lines.iter().enumerate() {
        // Looked up on use rather than cached: a record that establishes
        // which turn it is in (a prose chunk that joined to a group) changes
        // the answer for the rest of its own branch, and a cached value would
        // hand it the *previous* turn's start as its fallback.
        let turn_start = |turn: Option<usize>| {
            turn.and_then(|turn| session.updates.turns.get(turn))
                .and_then(|timing| timing.start_ms)
        };
        match &line.record {
            grok::GrokRecord::System { text } => {
                let ts = resolve_grok_ts(
                    line.ts_ms,
                    None,
                    turn_start(turn),
                    inherited,
                    session.created_ms,
                    &mut outcome,
                );
                inherited = Some(ts);
                outcome.markers += insert_session_marker(
                    conn,
                    &SessionMarker {
                        source: SOURCE.into(),
                        session_id: sid.to_string(),
                        marker_uid: format!("r{idx}"),
                        kind: "system".into(),
                        ts_ms: Some(ts),
                        text: text.as_deref().map(truncate_marker_text),
                        detail_json: None,
                    },
                )?;
            }
            grok::GrokRecord::User { text, synthetic } => {
                let Some(text) = text else { continue };
                if *synthetic {
                    // Grok injected this turn; nobody typed it. Recording it as
                    // a user message would put words in a person's mouth, so it
                    // is kept as a marker carrying the reason Grok gave.
                    let ts = resolve_grok_ts(
                        line.ts_ms,
                        None,
                        turn_start(turn),
                        inherited,
                        session.created_ms,
                        &mut outcome,
                    );
                    outcome.markers += insert_session_marker(
                        conn,
                        &SessionMarker {
                            source: SOURCE.into(),
                            session_id: sid.to_string(),
                            marker_uid: format!("r{idx}"),
                            kind: "synthetic_turn".into(),
                            ts_ms: Some(ts),
                            text: Some(truncate_marker_text(text)),
                            detail_json: session
                                .synthetic_reasons
                                .get(&idx)
                                .map(|reason| json!({ "synthetic_reason": reason }).to_string()),
                        },
                    )?;
                    // A marker is still a record with a place in the file, so
                    // the next record with no time of its own inherits *this*
                    // one's, not that of whatever came before it.
                    inherited = Some(ts);
                    continue;
                }
                let group = session.updates.user_messages.get(user_ordinal);
                user_ordinal += 1;
                // `updates.jsonl` numbers the turns; the chat-side counter is
                // only the fallback for a session whose stream is missing.
                turn =
                    Some(group.map_or_else(|| turn.map_or(0, |turn| turn + 1), |group| group.turn));
                let ts = resolve_grok_ts(
                    line.ts_ms,
                    group.and_then(|group| group.ts_ms),
                    turn_start(turn),
                    inherited,
                    session.created_ms,
                    &mut outcome,
                );
                inherited = Some(ts);
                if first_prompt.is_none() {
                    first_prompt = Some(crate::discover::excerpt(text));
                }
                let uid = grok_event_uid(group.and_then(|group| group.event_id.as_deref()), idx);
                insert_session_event(
                    conn,
                    SOURCE,
                    sid,
                    project,
                    project,
                    branch,
                    &uid,
                    None,
                    ts,
                    "user",
                    "text",
                    Some(text),
                    None,
                    None,
                    RequestIdentity::none(),
                    &uid,
                    None,
                    RawMessageFacts::default(),
                )?;
                outcome.events += 1;
                outcome.prompts += insert_history(
                    conn,
                    &HistoryEntry {
                        id: 0,
                        source: SOURCE.into(),
                        session_id: Some(sid.to_string()),
                        project: session.cwd.clone(),
                        prompt_hash: Some(prompt_hash(text)),
                        prompt: text.clone(),
                        timestamp_ms: ts,
                    },
                )?;
            }
            grok::GrokRecord::Reasoning { summary, encrypted } => {
                // Every reasoning record occupies an `agent_thoughts` ordinal,
                // including encrypted-only traces with no readable summary.
                // Skipping that consume handed the next thought the encrypted
                // record's chunk — its timestamp and event id.
                let group = session.updates.agent_thoughts.get(thought_ordinal);
                thought_ordinal += 1;
                if let Some(group) = group {
                    turn = Some(group.turn);
                }
                let ts = resolve_grok_ts(
                    line.ts_ms,
                    group.and_then(|group| group.ts_ms),
                    turn_start(turn),
                    inherited,
                    session.created_ms,
                    &mut outcome,
                );
                inherited = Some(ts);
                let Some(summary) = summary else {
                    if *encrypted {
                        // The trace exists but is opaque. Recording the fact
                        // that Grok thought here is honest; inventing readable
                        // thinking for it would not be.
                        outcome.encrypted_reasoning += 1;
                        outcome.markers += insert_session_marker(
                            conn,
                            &SessionMarker {
                                source: SOURCE.into(),
                                session_id: sid.to_string(),
                                marker_uid: format!("r{idx}"),
                                kind: "encrypted_reasoning".into(),
                                ts_ms: Some(ts),
                                text: None,
                                detail_json: None,
                            },
                        )?;
                    }
                    continue;
                };
                let uid = grok_event_uid(group.and_then(|group| group.event_id.as_deref()), idx);
                insert_session_event(
                    conn,
                    SOURCE,
                    sid,
                    project,
                    project,
                    branch,
                    &uid,
                    None,
                    ts,
                    "assistant",
                    "thinking",
                    Some(summary),
                    None,
                    None,
                    RequestIdentity::none(),
                    &uid,
                    None,
                    RawMessageFacts::default(),
                )?;
                outcome.events += 1;
            }
            grok::GrokRecord::Assistant { text, model, calls } => {
                if let Some(text) = text {
                    let group = session.updates.agent_messages.get(message_ordinal);
                    message_ordinal += 1;
                    if let Some(group) = group {
                        turn = Some(group.turn);
                    }
                    let ts = resolve_grok_ts(
                        line.ts_ms,
                        group.and_then(|group| group.ts_ms),
                        turn_start(turn),
                        inherited,
                        session.created_ms,
                        &mut outcome,
                    );
                    inherited = Some(ts);
                    let uid =
                        grok_event_uid(group.and_then(|group| group.event_id.as_deref()), idx);
                    insert_session_event(
                        conn,
                        SOURCE,
                        sid,
                        project,
                        project,
                        branch,
                        &uid,
                        None,
                        ts,
                        "assistant",
                        "text",
                        Some(text),
                        model.as_deref(),
                        None,
                        RequestIdentity::none(),
                        &uid,
                        None,
                        RawMessageFacts::default(),
                    )?;
                    outcome.events += 1;
                    if let Some(turn) = turn {
                        turn_tail.insert(turn, uid);
                    }
                }
                for (nth, call) in calls.iter().enumerate() {
                    let tool_use_id = call.id.clone().unwrap_or_else(|| format!("r{idx}:t{nth}"));
                    let timing = session.updates.tools.get(&tool_use_id);
                    // A tool call joins by id, so its turn is known exactly.
                    // The transcript-side cursor is only the fallback, and it
                    // must not displace a matched call's own turn.
                    let call_turn = timing.and_then(|timing| timing.turn).or(turn);
                    let ts = resolve_grok_ts(
                        line.ts_ms,
                        timing.and_then(|timing| timing.started_ms),
                        turn_start(call_turn),
                        inherited,
                        session.created_ms,
                        &mut outcome,
                    );
                    inherited = Some(ts);
                    let target = grok::pick_tool_target(&call.name, &call.arguments);
                    let uid = format!("tool:{tool_use_id}");
                    insert_session_event(
                        conn,
                        SOURCE,
                        sid,
                        project,
                        project,
                        branch,
                        &uid,
                        None,
                        ts,
                        "assistant",
                        "tool_use",
                        Some(&format_tool_event_text(
                            &call.name,
                            target.as_deref(),
                            &call.arguments,
                        )),
                        model.as_deref(),
                        None,
                        RequestIdentity::none(),
                        &uid,
                        None,
                        RawMessageFacts::default(),
                    )?;
                    outcome.events += 1;
                    // `updates.jsonl` knows which turn the call belongs to even
                    // when the transcript side does not, because the call is
                    // joined by id.
                    if let Some(call_turn) = call_turn {
                        turn_tool_tail.insert(call_turn, uid.clone());
                    }
                    insert_tool_call(
                        conn,
                        SOURCE,
                        sid,
                        &uid,
                        &tool_use_id,
                        &call.name,
                        target.as_deref(),
                        &serde_json::to_string(&call.arguments).unwrap_or_default(),
                        None,
                        ts,
                    )?;
                    outcome.tool_calls += 1;
                    if grok::is_subagent_tool(&call.name) {
                        outcome.subagent_calls += 1;
                    }
                    if grok::is_file_edit_tool(&call.name) {
                        if let Some(path) = target.as_deref() {
                            upsert_file_edit_from_call(
                                conn,
                                SOURCE,
                                sid,
                                &uid,
                                &tool_use_id,
                                path,
                                &call.name,
                                ts,
                                branch,
                                project,
                            )?;
                            outcome.file_edits += 1;
                        }
                    }
                }
            }
            grok::GrokRecord::ToolResult {
                call_id,
                text,
                is_error,
            } => {
                let tool_use_id = call_id.clone().unwrap_or_else(|| format!("r{idx}:result"));
                let timing = session.updates.tools.get(&tool_use_id);
                // Paired by id with its call, so the result belongs to the
                // call's turn whatever the transcript cursor says.
                let result_turn = timing.and_then(|timing| timing.turn).or(turn);
                let ts = resolve_grok_ts(
                    line.ts_ms,
                    timing.and_then(|timing| timing.finished_ms),
                    turn_start(result_turn),
                    inherited,
                    session.created_ms,
                    &mut outcome,
                );
                inherited = Some(ts);
                // Grok records failure in two places and neither is always
                // present: the result's own `is_error`, and the terminal
                // `status` on the tool call's last ACP update.
                let failed = is_error.unwrap_or(false)
                    || timing
                        .and_then(|timing| timing.status.as_deref())
                        .is_some_and(|status| {
                            matches!(status, "failed" | "error" | "cancelled" | "canceled")
                        });
                let uid = format!("result:{tool_use_id}");
                insert_session_event(
                    conn,
                    SOURCE,
                    sid,
                    project,
                    project,
                    branch,
                    &uid,
                    None,
                    ts,
                    "tool_result",
                    "tool_result",
                    text.as_deref(),
                    None,
                    None,
                    RequestIdentity::none(),
                    &uid,
                    None,
                    RawMessageFacts::default(),
                )?;
                outcome.events += 1;
                if failed || is_error.is_some() {
                    set_tool_call_error(conn, SOURCE, sid, &tool_use_id, failed)?;
                }
            }
            grok::GrokRecord::Other => {}
        }
    }

    // The context-token proxy, on the turn's final assistant message. It is
    // stored with its source named so a consumer can see that it is a context
    // snapshot and not billed usage: Grok logs no per-turn input/output token
    // counts, and none are estimated here.
    for (turn, timing) in session.updates.turns.iter().enumerate() {
        let tail = turn_tail.get(&turn).or_else(|| turn_tool_tail.get(&turn));
        let (Some(total), Some(uid)) = (timing.total_tokens, tail) else {
            continue;
        };
        let token_json = json!({
            "context_total_tokens": total,
            "source": "updates.jsonl",
        })
        .to_string();
        conn.execute(
            "UPDATE session_events SET token_json = ? \
             WHERE source = 'grok' AND session_id = ? AND event_uid = ?",
            params![token_json, sid, uid],
        )?;
    }

    ingest_grok_session_extras(conn, session, &mut outcome)?;
    refresh_grok_incoming_relationships(conn, sid)?;

    upsert_session(
        conn,
        sid,
        SOURCE,
        project,
        branch,
        session.first_ts,
        session.last_ts,
        session.last_assistant_text.as_deref(),
        Some(raw_path),
    )?;
    // `upsert_session` merges: it takes MIN/MAX of the activity bounds and
    // keeps the old `last_assistant_text` when the new read has none. That is
    // right for an append-only transcript and wrong for a snapshot — after a
    // compaction the directory says the session now ends earlier, or ran no
    // model at all, and the merge would keep reporting yesterday's end time
    // and yesterday's model forever. So the fields the snapshot owns are
    // assigned from it, empty values included, while every other provider
    // keeps the monotonic merge.
    conn.execute(
        "UPDATE sessions SET first_activity_ms = ?, last_activity_ms = ?, \
         last_assistant_text = ?, models_json = ?, first_prompt = ? \
         WHERE source = 'grok' AND session_id = ?",
        params![
            session.first_ts,
            session.last_ts,
            session.last_assistant_text.as_deref(),
            (!session.models.is_empty())
                .then(|| serde_json::to_string(&session.models).unwrap_or_default()),
            first_prompt.as_deref(),
            sid,
        ],
    )?;
    Ok(outcome)
}

/// Clear the evidence this session's files own, so a read is a replacement
/// rather than a merge.
///
/// A Grok read is always a whole-directory read, and Grok **rewrites**
/// `chat_history.jsonl` in place on a format upgrade or a compaction. So the
/// files are a snapshot, not an append-only log: a tool call that is no longer
/// in the transcript, a checkpoint whose file was removed, a `subagents/`
/// entry that is gone — none of them happened, as far as the current evidence
/// goes. Upserting alone would leave every one of them in the database
/// forever, and a reader cannot tell a stale row from a live one.
///
/// Scope is deliberately narrow. Rows are deleted only where `source =
/// 'grok'` and this session owns them; a relationship is deleted only where
/// this session is the **parent** and the evidence came from its own
/// `subagents/` directory, so another session's record of *this* session as a
/// child is untouched. The caller runs inside a transaction, so the window
/// where the evidence is missing is never observable.
fn replace_grok_session_evidence(conn: &Connection, session_id: &str) -> Result<()> {
    for statement in [
        "DELETE FROM session_events WHERE source = 'grok' AND session_id = ?",
        "DELETE FROM tool_calls WHERE source = 'grok' AND session_id = ?",
        "DELETE FROM file_edits WHERE source = 'grok' AND session_id = ?",
        "DELETE FROM session_markers WHERE source = 'grok' AND session_id = ?",
    ] {
        conn.execute(statement, params![session_id])?;
    }
    // `history` is keyed `(source, timestamp_ms, prompt)` and inserted with
    // `INSERT OR IGNORE`, so a prompt two sessions both contain is **one row**,
    // attributed to whichever session was indexed first. Deleting this
    // session's rows outright would therefore delete a prompt that another,
    // unchanged session still has -- it would vanish from search with nothing
    // to say it had ever been there, and nothing would ever put it back,
    // because that session's own files never change again.
    //
    // So a row this session no longer owns is **re-attributed** rather than
    // skipped. Skipping was the other option and is worse: it would leave the
    // row filed under a session that no longer contains the prompt, which is
    // a false statement about provenance, and would leave it undeletable --
    // the only session that could ever clean it up is the one that no longer
    // evidences it. Re-attribution moves the row to a session whose stored
    // events actually carry that prompt at that moment, so the row stays true
    // and stays owned.
    conn.execute(
        "UPDATE history AS h SET            session_id = (SELECT e.session_id FROM session_events e                          WHERE e.source = 'grok' AND e.role = 'user' AND e.kind = 'text'                            AND e.session_id <> h.session_id                            AND e.ts_ms = h.timestamp_ms AND e.text = h.prompt                          ORDER BY e.session_id LIMIT 1),            project = (SELECT e.project FROM session_events e                       WHERE e.source = 'grok' AND e.role = 'user' AND e.kind = 'text'                         AND e.session_id <> h.session_id                         AND e.ts_ms = h.timestamp_ms AND e.text = h.prompt                       ORDER BY e.session_id LIMIT 1)          WHERE h.source = 'grok' AND h.session_id = ?            AND EXISTS(SELECT 1 FROM session_events e                       WHERE e.source = 'grok' AND e.role = 'user' AND e.kind = 'text'                         AND e.session_id <> h.session_id                         AND e.ts_ms = h.timestamp_ms AND e.text = h.prompt)",
        params![session_id],
    )?;
    for statement in [
        // What is left is this session's alone. A prompt's identity is
        // `(source, timestamp_ms, prompt)`, and every Grok prompt written
        // before this parser carried a timestamp synthesized as
        // `created_at + index`; merging would keep the fabricated ones
        // forever.
        "DELETE FROM history WHERE source = 'grok' AND session_id = ?",
        "DELETE FROM session_relationships WHERE source = 'grok' \
         AND parent_session_id = ? AND evidence_kind = 'grok_subagent_dir'",
    ] {
        conn.execute(statement, params![session_id])?;
    }
    Ok(())
}

/// Tell this session's parents whether it is addressable yet.
///
/// A parent's `subagents/` directory can be read before its child has ever
/// been indexed — sync walks the store in whatever order the filesystem hands
/// it back, and the child may not even exist on disk yet. The edge is recorded
/// then with `child_has_events = 0`, and nothing would ever correct it,
/// because refreshing it requires re-reading the *parent*. So the child does
/// it when it is indexed, in both directions: a session whose events were
/// replaced by a read that produced none says so too.
fn refresh_grok_incoming_relationships(conn: &Connection, session_id: &str) -> Result<()> {
    let has_events = session_events_exist(conn, "grok", session_id)?;
    conn.execute(
        "UPDATE session_relationships SET child_has_events = ?, updated_ms = ? \
         WHERE source = 'grok' AND child_session_id = ? AND child_has_events <> ?",
        params![has_events, now_ms(), session_id, has_events],
    )?;
    Ok(())
}

/// The session-level files beside the transcript: `signals.json`,
/// `prompt_context.json`, `compaction_checkpoints/` and `subagents/`.
fn ingest_grok_session_extras(
    conn: &Connection,
    session: &GrokSession,
    outcome: &mut GrokIngestOutcome,
) -> Result<()> {
    const SOURCE: &str = "grok";
    let sid = session.session_id.as_str();
    if let Some(signals) = &session.signals {
        outcome.markers += insert_session_marker(
            conn,
            &SessionMarker {
                source: SOURCE.into(),
                session_id: sid.to_string(),
                marker_uid: "signals".into(),
                kind: "signals".into(),
                ts_ms: session.updates.last_ms.or(Some(session.last_ts)),
                text: Some(grok_signals_summary(signals)),
                detail_json: Some(Value::Object(signals.raw.clone()).to_string()),
            },
        )?;
    }
    // The AGENTS.md snapshot itself is not copied into the database: it is the
    // project's file, not session evidence. Its path and content hash are, so
    // a later consumer can tell whether two sessions ran with the same
    // instructions.
    if let Some(context) = &session.prompt_context {
        outcome.markers += insert_session_marker(
            conn,
            &SessionMarker {
                source: SOURCE.into(),
                session_id: sid.to_string(),
                marker_uid: "prompt_context".into(),
                kind: "prompt_context".into(),
                ts_ms: None,
                text: Some(context.path.clone()),
                detail_json: Some(
                    json!({
                        "path": context.path,
                        "sha256": context.sha256,
                        "bytes": context.bytes,
                    })
                    .to_string(),
                ),
            },
        )?;
    }
    for checkpoint in &session.compactions {
        outcome.markers += insert_session_marker(
            conn,
            &SessionMarker {
                source: SOURCE.into(),
                session_id: sid.to_string(),
                marker_uid: format!("compaction:{}", checkpoint.name),
                kind: "compaction_boundary".into(),
                ts_ms: checkpoint.ts_ms,
                text: Some(checkpoint.locator.clone()),
                detail_json: checkpoint.detail_json.clone(),
            },
        )?;
    }
    for subagent in &session.subagents {
        if subagent.metadata.child_session_id.is_none() {
            outcome.unlinked_subagents += 1;
        }
        record_relationship(
            conn,
            &ObservedRelationship {
                source: SOURCE,
                parent_session_id: sid,
                child_session_id: subagent.metadata.child_session_id.as_deref(),
                relationship: "delegated",
                child_agent_type: subagent.metadata.agent_type.as_deref(),
                child_agent_name: subagent.metadata.agent_name.as_deref(),
                child_model: subagent.metadata.model.as_deref(),
                spawn_depth: Some(1),
                evidence_kind: "grok_subagent_dir",
                evidence_locator: Some(&subagent.locator),
                evidence_ref: None,
                // Whether the child is addressable is a fact about the child,
                // not a default. `session_tree` reads this stored flag rather
                // than probing, so a hard-coded `false` renders an indexed
                // child as an empty node.
                child_has_events: match subagent.metadata.child_session_id.as_deref() {
                    Some(child) => session_events_exist(conn, SOURCE, child)?,
                    None => false,
                },
                spawned_at_ms: subagent.metadata.spawned_at_ms,
                origin_session_id: None,
                relationship_uid: None,
            },
        )?;
        outcome.relationships += 1;
    }
    Ok(())
}

/// The counters `signals.json` records, as one readable line. The file's own
/// object is kept verbatim in the marker's `detail_json`; this is the part a
/// person reads.
fn grok_signals_summary(signals: &grok::GrokSignals) -> String {
    let mut parts = Vec::new();
    if let Some(turns) = signals.turn_count {
        parts.push(format!("turns={turns}"));
    }
    if let Some(compactions) = signals.compaction_count {
        parts.push(format!("compactions={compactions}"));
    }
    if let Some(tokens) = signals.context_tokens_used {
        parts.push(format!("context_tokens_used={tokens}"));
    }
    parts.join(" ")
}

/// Where one record's timestamp came from, counting every step below an exact
/// match so the caller can report how much of the timeline Grok did not write.
fn resolve_grok_ts(
    own: Option<i64>,
    matched: Option<i64>,
    turn_start: Option<i64>,
    inherited: Option<i64>,
    session_start: i64,
    outcome: &mut GrokIngestOutcome,
) -> i64 {
    if let Some(ts) = own.or(matched) {
        return ts;
    }
    if let Some(ts) = turn_start {
        outcome.turn_start_fallbacks += 1;
        return ts;
    }
    if let Some(ts) = inherited {
        outcome.inherited_fallbacks += 1;
        return ts;
    }
    outcome.session_start_fallbacks += 1;
    session_start
}

/// A record's event identity: the ACP `eventId` of the update it joined to
/// when there is one, and its position in `chat_history.jsonl` otherwise.
///
/// `chat_history.jsonl` writes no record id at all, so without the join the
/// only identity available is positional. Grok rebuilds that file on a format
/// upgrade, which renumbers it; an `eventId` survives that, which is why it is
/// preferred.
fn grok_event_uid(event_id: Option<&str>, index: usize) -> String {
    match event_id {
        Some(id) => format!("ev:{id}"),
        None => format!("r{index}"),
    }
}

/// Markers carry an excerpt, not a transcript: the full text is already in the
/// file the marker's session points at.
fn truncate_marker_text(text: &str) -> String {
    text.chars().take(512).collect()
}

fn grok_session_stamp(chat: &Path) -> Result<String> {
    Ok(grok_source_inventory(chat)?.stamp)
}

/// The files beside `chat_history.jsonl` that get their own readable marker in
/// the stamp. `updates.jsonl` is here because it carries every timestamp: a
/// session whose transcript is unchanged but whose update stream grew still
/// has new evidence.
const GROK_STAMPED_SIBLINGS: &[&str] = &["summary.json", "updates.jsonl"];

/// The remaining files `ingest_grok_session` reads. They are folded into one
/// digest rather than appended, so a session with many checkpoints does not
/// grow an unbounded stamp.
const GROK_DIGESTED_SIBLINGS: &[&str] = &["signals.json", "prompt_context.json"];

/// The directories `ingest_grok_session` reads, entry by entry.
const GROK_DIGESTED_DIRECTORIES: &[&str] = &["compaction_checkpoints", "subagents"];

/// [`grok_session_stamp`] plus the recency hint, with one stat per file.
pub(crate) fn grok_session_stamp_and_modified(chat: &Path) -> Result<(String, Option<i64>)> {
    let inventory = grok_source_inventory(chat)?;
    Ok((inventory.stamp, inventory.modified_ms))
}

/// What one Grok session directory holds, from its metadata alone.
///
/// Every field here comes from a `stat`: no file's contents are read. That is
/// the difference between a scan that costs one syscall per file and one that
/// parses every update stream in the store — discovery stamps every candidate
/// on every run, and a session's stream is routinely megabytes. The record
/// count is deliberately *not* here; see [`grok_source_records`].
///
/// `bytes` still describes **everything the read consumes**, not just the
/// transcript: a 10 KB `chat_history.jsonl` beside a 2 MB `updates.jsonl` is a
/// 2 MB read, and a length is metadata.
pub(crate) struct GrokSourceInventory {
    pub stamp: String,
    pub modified_ms: Option<i64>,
    pub bytes: i64,
}

/// The change stamp of one Grok session directory, over **every** file its
/// ingestion reads.
///
/// Discovery, plain `sync` and targeted hydration all take their stamp from
/// here, so no one of them can decide a session is unchanged on evidence the
/// others would have re-read. A stamp that covered only the transcript, the
/// summary and the update stream let a new `compaction_checkpoints/` entry, an
/// edited `signals.json` or a new `subagents/` entry land after the last
/// update row and never be read at all.
///
/// The two directories are listed, not stat-ed: a directory's own mtime moves
/// when an entry is added or removed, but not when an entry's contents change.
///
/// The same walk produces the stamp and the byte count, and
/// [`grok_source_records`] walks the same list, so the numbers a hydration
/// reports can never describe a different set of files from the one its change
/// stamp covers.
pub(crate) fn grok_source_inventory(chat: &Path) -> Result<GrokSourceInventory> {
    let metadata = chat.metadata()?;
    anyhow::ensure!(
        metadata.is_file(),
        "{} is not a regular file",
        chat.display()
    );
    let mut stamp = stamp_of(&metadata);
    let mut modified = modified_ms_of(&metadata);
    let mut bytes = metadata.len() as i64;
    let freshen = |candidate: Option<i64>, modified: &mut Option<i64>| {
        if let Some(candidate) = candidate {
            *modified = Some(modified.map_or(candidate, |current| current.max(candidate)));
        }
    };
    for name in GROK_STAMPED_SIBLINGS {
        let path = chat.with_file_name(name);
        let Some(sibling) = grok_entry_metadata(&path)? else {
            continue;
        };
        stamp.push('|');
        stamp.push_str(&stamp_of(&sibling));
        // `updates.jsonl` is appended on every turn, so it — not the
        // transcript — is usually the freshest signal of activity.
        freshen(modified_ms_of(&sibling), &mut modified);
        bytes += sibling.len() as i64;
    }
    let mut digest = Sha256::new();
    for sibling in GROK_DIGESTED_SIBLINGS {
        let path = chat.with_file_name(sibling);
        let Some(found) = grok_entry_metadata(&path)? else {
            continue;
        };
        digest.update(format!("{sibling}={}\n", stamp_of(&found)));
        freshen(modified_ms_of(&found), &mut modified);
        bytes += found.len() as i64;
    }
    for directory in GROK_DIGESTED_DIRECTORIES {
        // Sorted, so the digest describes the directory's contents rather than
        // the order the filesystem happened to hand them back.
        let mut entries = read_dir_files(&chat.with_file_name(directory))?;
        entries.sort();
        for entry in entries {
            let found = entry
                .metadata()
                .with_context(|| format!("stat Grok entry {}", entry.display()))?;
            let name = entry
                .file_name()
                .map(|name| name.to_string_lossy().to_string())
                .unwrap_or_default();
            digest.update(format!("{directory}/{name}={}\n", stamp_of(&found)));
            freshen(modified_ms_of(&found), &mut modified);
            bytes += found.len() as i64;
        }
    }
    // Sixteen hex characters of SHA-256: enough that two different sets of
    // extra files colliding is not a failure mode worth designing against,
    // short enough that the stamp stays readable in a diagnostic.
    stamp.push_str(&format!("|x:{:.16}", format!("{:x}", digest.finalize())));
    Ok(GrokSourceInventory {
        stamp,
        modified_ms: modified,
        bytes,
    })
}

/// How many records a Grok session directory holds — the content pass.
///
/// Separate from [`grok_source_inventory`] because it is the expensive half:
/// it reads every byte of `chat_history.jsonl` and `updates.jsonl`. Only a
/// hydration that is actually going to parse the session calls it. Discovery,
/// plain `sync` and a hydration that decides nothing changed all stamp from
/// metadata and never open a file, which is what makes the unchanged
/// short-circuit worth taking.
///
/// One per complete JSONL record in the two streams, plus **one per whole-file
/// JSON sidecar** — `summary.json`, `signals.json`, `prompt_context.json`, and
/// each `compaction_checkpoints/` and `subagents/` entry. A sidecar is one
/// document: the read either parsed it or did not, and counting it as zero
/// would make a directory of fifty checkpoints look like no work at all. A
/// `.jsonl` entry inside those directories is counted by record, as the older
/// Grok layout writes subagent transcripts that way.
pub(crate) fn grok_source_records(chat: &Path) -> Result<i64> {
    let mut records = hydrate::complete_jsonl_records(chat)?;
    for name in GROK_STAMPED_SIBLINGS.iter().chain(GROK_DIGESTED_SIBLINGS) {
        let path = chat.with_file_name(name);
        if grok_entry_metadata(&path)?.is_some_and(|found| found.is_file()) {
            records += grok_record_count(&path)?;
        }
    }
    for directory in GROK_DIGESTED_DIRECTORIES {
        let mut entries = read_dir_files(&chat.with_file_name(directory))?;
        entries.sort();
        for entry in entries {
            records += grok_record_count(&entry)?;
        }
    }
    Ok(records)
}

/// How many records one file in a Grok session directory contributes: its
/// complete JSONL records, or one, for a whole-file JSON document.
fn grok_record_count(path: &Path) -> Result<i64> {
    if path.extension().and_then(|extension| extension.to_str()) == Some("jsonl") {
        hydrate::complete_jsonl_records(path)
    } else {
        Ok(1)
    }
}

/// One `compaction_checkpoints/` entry.
struct GrokCompaction {
    /// The entry's file name, which is its identity within the session.
    name: String,
    locator: String,
    ts_ms: Option<i64>,
    detail_json: Option<String>,
}

/// One `subagents/` entry.
struct GrokSubagentEvidence {
    locator: String,
    metadata: grok::GrokSubagent,
}

/// The `prompt_context.json` Grok rendered the system prompt from, identified
/// rather than copied.
struct GrokPromptContext {
    path: String,
    sha256: String,
    bytes: u64,
}

/// One Grok session directory, read.
struct GrokSession {
    session_id: String,
    cwd: Option<String>,
    git_branch: Option<String>,
    first_ts: i64,
    last_ts: i64,
    /// `summary.json`'s `created_at`, the last-resort timestamp.
    created_ms: i64,
    last_assistant_text: Option<String>,
    models: Vec<String>,
    lines: Vec<grok::GrokChatLine>,
    /// `synthetic_reason` by record index, kept out of the parsed record so
    /// the record enum stays about content.
    synthetic_reasons: HashMap<usize, String>,
    updates: grok::GrokUpdates,
    /// Whether `updates.jsonl` was there at all. Distinct from
    /// `updates.is_empty()`, which says only that nothing in the file parsed
    /// as a timing structure this parser understands: a stream of rows it does
    /// not interpret is a stream that exists, and reporting it as missing
    /// would tell a caller the wrong thing about the session.
    updates_present: bool,
    signals: Option<grok::GrokSignals>,
    prompt_context: Option<GrokPromptContext>,
    compactions: Vec<GrokCompaction>,
    subagents: Vec<GrokSubagentEvidence>,
}

fn scan_grok_session_file(chat: &Path) -> Result<Option<GrokSession>> {
    let summary = read_grok_summary(&chat.with_file_name("summary.json"))?;
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
    if session_id.is_empty() {
        return Ok(None);
    }
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

    // Three states, not two. A stream that is **not there** is a session Grok
    // wrote no ACP updates for, and its events fall back to what the
    // transcript says. A stream that is there but **cannot be read** is a
    // failure: taking it as absent would replace exact event times with
    // fallbacks, drop the token snapshots, and — because the change stamp was
    // computed from metadata that is still perfectly readable — checkpoint
    // that loss as the session's settled state until some file changes again.
    let updates_path = chat.with_file_name("updates.jsonl");
    let (updates, updates_present) = match fs::read_to_string(&updates_path) {
        Ok(contents) => (grok::parse_updates(&contents, &updates_path)?, true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            (grok::GrokUpdates::default(), false)
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("read Grok updates {}", updates_path.display()))
        }
    };

    let mut lines = Vec::new();
    let mut synthetic_reasons = HashMap::new();
    let mut last_assistant_text = None;
    let mut models: Vec<String> = Vec::new();
    let contents = fs::read_to_string(chat)
        .with_context(|| format!("read Grok chat history {}", chat.display()))?;
    for (number, row) in jsonl::rows(&contents).enumerate() {
        // A complete row that does not parse fails the read. The ingestion
        // this feeds replaces the session's evidence, so dropping the row
        // would commit a transcript that is missing a turn Grok did write --
        // and save the stamp that stops the next run from looking again.
        let Some(value) = jsonl::parse_row(row, chat, number + 1)? else {
            continue;
        };
        if let Some(reason) = value.get("synthetic_reason").and_then(Value::as_str) {
            synthetic_reasons.insert(lines.len(), reason.to_string());
        }
        let parsed = grok::parse_chat_record(&value);
        if let grok::GrokRecord::Assistant { text, model, .. } = &parsed.record {
            if let Some(text) = text {
                last_assistant_text = Some(text.chars().take(4096).collect::<String>());
            }
            if let Some(model) = model {
                if !models.iter().any(|seen| seen == model) {
                    models.push(model.clone());
                }
            }
        }
        lines.push(parsed);
    }
    if models.is_empty() {
        if let Some(model) = summary
            .as_ref()
            .and_then(|s| s.pointer("/info/model").or_else(|| s.get("model")))
            .and_then(Value::as_str)
            .filter(|model| !model.is_empty())
        {
            models.push(model.to_string());
        }
    }

    let directory = chat.parent().map(Path::to_path_buf);
    // A `signals.json` that exists but does not parse is still a fact about
    // the session, so the marker is written from its existence and the counters
    // are simply absent. Collapsing that into "no signals" would delete the
    // marker a previous read wrote and record nothing in its place.
    let signals = match directory.as_deref() {
        Some(dir) => match read_json_file(&dir.join("signals.json"))? {
            GrokSidecar::Absent => None,
            GrokSidecar::Unparsed => Some(grok::GrokSignals::default()),
            GrokSidecar::Read(value) => Some(grok::parse_signals(&value)),
        },
        None => None,
    };
    let prompt_context = match directory.as_deref() {
        Some(dir) => read_grok_prompt_context(&dir.join("prompt_context.json"))?,
        None => None,
    };
    let (compactions, subagents) = match directory.as_deref() {
        Some(dir) => (
            read_grok_compactions(&dir.join("compaction_checkpoints"))?,
            read_grok_subagents(&dir.join("subagents"))?,
        ),
        None => (Vec::new(), Vec::new()),
    };

    // Real recorded times first, in this order: the update stream, then any
    // timestamp the transcript's own records carried, then `summary.json`.
    // Nothing here is derived from a record's position in the file.
    let record_times: Vec<i64> = lines.iter().filter_map(|line| line.ts_ms).collect();
    let first_ts = updates
        .first_ms
        .or_else(|| record_times.iter().copied().min())
        .unwrap_or(created_ms);
    let last_ts = updates
        .last_ms
        .or_else(|| record_times.iter().copied().max())
        .unwrap_or(updated_ms)
        .max(first_ts);

    Ok(Some(GrokSession {
        session_id,
        cwd,
        git_branch,
        first_ts,
        last_ts,
        created_ms,
        last_assistant_text,
        models,
        lines,
        synthetic_reasons,
        updates,
        updates_present,
        signals,
        prompt_context,
        compactions,
        subagents,
    }))
}

/// What a read of one Grok sidecar found.
///
/// The three states are not interchangeable, and collapsing any two of them
/// loses evidence:
///
/// * **Absent** — Grok never wrote this file. There is nothing to record.
/// * **Unparsed** — the file is there and this parser cannot interpret it: a
///   half-written document, or a shape nobody has characterized yet. The
///   *existence* is evidence, so the caller still records the entry, with no
///   detail. Deleting the previous marker and writing nothing would report a
///   session that had never had one.
/// * **Read** — the document, parsed.
///
/// An I/O failure is none of these: it is an error, so a file that cannot be
/// read never masquerades as a file that is not there.
enum GrokSidecar {
    Absent,
    Unparsed,
    Read(Value),
}

impl GrokSidecar {
    /// The parsed document, if there is one.
    fn value(&self) -> Option<&Value> {
        match self {
            Self::Read(value) => Some(value),
            _ => None,
        }
    }
}

fn read_json_file(path: &Path) -> Result<GrokSidecar> {
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(GrokSidecar::Absent),
        Err(error) => {
            return Err(error).with_context(|| format!("read Grok sidecar {}", path.display()))
        }
    };
    Ok(match serde_json::from_str(&contents) {
        Ok(value) => GrokSidecar::Read(value),
        Err(_) => GrokSidecar::Unparsed,
    })
}

/// The metadata of a file a Grok read consumes: `None` when it is not there,
/// an error when it is there and cannot be stat-ed.
///
/// The `Ok(_) => continue` this replaces was the same silent-absence bug one
/// level down: a `summary.json` or `updates.jsonl` that could not be stat-ed
/// dropped out of the change stamp, making an unreadable file indistinguishable
/// from a missing one.
fn grok_entry_metadata(path: &Path) -> Result<Option<fs::Metadata>> {
    match path.metadata() {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("stat Grok file {}", path.display())),
    }
}

/// Identify `prompt_context.json` without copying it: the path, its SHA-256
/// and its size are enough to tell two sessions' instruction snapshots apart.
///
/// Absent is `None`; unreadable is an error. Taking a permission failure for
/// absence would delete the marker this session already had and then save the
/// stamp, so every later run would agree the directory was unchanged and the
/// snapshot would stay gone.
fn read_grok_prompt_context(path: &Path) -> Result<Option<GrokPromptContext>> {
    let contents = match fs::read(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("read Grok prompt context {}", path.display()))
        }
    };
    let mut hasher = Sha256::new();
    hasher.update(&contents);
    Ok(Some(GrokPromptContext {
        path: path.to_string_lossy().to_string(),
        sha256: format!("{:x}", hasher.finalize()),
        bytes: contents.len() as u64,
    }))
}

/// Every readable entry in `compaction_checkpoints/`, oldest name first.
///
/// An entry that parses as nothing is still a compaction: the directory entry
/// itself is the evidence, so the marker is written with no detail rather than
/// dropped.
fn read_grok_compactions(dir: &Path) -> Result<Vec<GrokCompaction>> {
    let mut checkpoints = Vec::new();
    for path in read_dir_files(dir)? {
        let parsed = read_json_file(&path)?;
        let name = path
            .file_name()
            .map(|name| name.to_string_lossy().to_string())
            .unwrap_or_default();
        checkpoints.push(GrokCompaction {
            ts_ms: parsed
                .value()
                .and_then(grok::compaction_timestamp_ms)
                .or_else(|| timestamp_from_name(&name)),
            detail_json: parsed.value().map(ToString::to_string),
            locator: path.to_string_lossy().to_string(),
            name,
        });
    }
    checkpoints.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(checkpoints)
}

/// Every readable entry in `subagents/`.
fn read_grok_subagents(dir: &Path) -> Result<Vec<GrokSubagentEvidence>> {
    let mut found = Vec::new();
    for path in read_dir_files(dir)? {
        found.push(GrokSubagentEvidence {
            metadata: read_json_file(&path)?
                .value()
                .map(grok::parse_subagent)
                .unwrap_or_default(),
            locator: path.to_string_lossy().to_string(),
        });
    }
    found.sort_by(|left, right| left.locator.cmp(&right.locator));
    Ok(found)
}

/// The regular files in one of a Grok session's sidecar directories.
///
/// A directory that is not there is empty — Grok writes `subagents/` only for
/// a session that spawned one. **Anything else is an error**, and has to be,
/// because this list is what the change stamp covers and what the replacement
/// snapshot is built from: a permission or I/O failure read as "empty" would
/// stamp the session as having no checkpoints and then delete the ones already
/// stored, reporting success. The read has to fail so the surrounding
/// transaction rolls back and the prior evidence survives.
///
/// A single entry that has vanished between the listing and the stat is
/// skipped rather than fatal: the next run will not see it either.
fn read_dir_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(error).with_context(|| format!("read Grok directory {}", dir.display()))
        }
    };
    let mut files = Vec::new();
    for entry in entries {
        let path = entry
            .with_context(|| format!("read Grok directory entry under {}", dir.display()))?
            .path();
        // `metadata` follows the link, so an entry that cannot be resolved --
        // a symlink loop, an unreadable mount -- is an error here rather than
        // a silently absent file.
        match path.metadata() {
            Ok(metadata) if metadata.is_file() => files.push(path),
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| format!("stat Grok entry {}", path.display()))
            }
        }
    }
    Ok(files)
}

/// A checkpoint file named after the moment it was taken, e.g.
/// `1789560090000.json`. Only used when the file itself records no time.
fn timestamp_from_name(name: &str) -> Option<i64> {
    let digits: String = name.chars().take_while(char::is_ascii_digit).collect();
    grok::timestamp_value_ms(&json!(digits.parse::<i64>().ok()?))
}

/// `summary.json`, parsed.
///
/// `None` covers two cases that share an answer: the file is not there, or it
/// is there and does not parse. Both leave identity to the path layout, which
/// is the documented degrade. An I/O failure does **not** share that answer —
/// a session whose summary cannot be read would otherwise be indexed under its
/// directory name with a `created_at` taken from an mtime, silently becoming a
/// different session from the one already in the catalog.
/// `summary.json`, with malformed treated as an error rather than as absence.
///
/// This sidecar is the one exception to "malformed but present is evidence",
/// and the reason is that it does not carry detail -- it carries **identity**.
/// `info.id` becomes the session id, which keys every evidence row and scopes
/// every delete a replacing read performs. When a malformed summary fell back
/// to the directory name, a session caught mid-write was stored under
/// `<encoded-cwd-folder>`; once the file was repaired the next read stored the
/// same session under its real id, and the first set of rows -- catalog row,
/// events, markers, history -- was left behind for ever, keyed to an id no
/// read would ever name again. Nothing would delete them, because deletes are
/// scoped by the id that produced them.
///
/// So a present-but-unparseable summary fails the session read: the previous
/// transaction stands, the stamp is left unsaved, and the next run tries again
/// once the file changes. Absence keeps the directory-name fallback, because
/// a session directory with no summary at all is a real shape and its folder
/// name is the only identity there is.
pub(crate) fn read_grok_summary(path: &Path) -> Result<Option<Value>> {
    match read_json_file(path)? {
        GrokSidecar::Absent => Ok(None),
        GrokSidecar::Read(value) => Ok(Some(value)),
        GrokSidecar::Unparsed => anyhow::bail!(
            "{}: summary.json is present but not valid JSON, and it carries the session identity; \
             refusing to index this session under its directory name",
            path.display()
        ),
    }
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
    // The same unwrapping the full read does. Discovery and hydration write
    // the *same* prompt to different columns -- `sessions.first_prompt` and
    // `history.prompt` -- so a wrapper stripped in one and kept in the other
    // shows a person the `<user_query>` envelope in the catalog and the typed
    // prompt in the transcript, for one session, with nothing to say which is
    // the prompt.
    let text = if role == "user" {
        grok::unwrap_user_query(&text)
    } else {
        text
    };
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
    fn refusal(uid: &str, generation: u64, raw: &str) -> UnreadableSnapshot {
        UnreadableSnapshot {
            uid: uid.to_string(),
            generation,
            raw: raw.to_string(),
        }
    }

    /// The whole refusal rule, read directly rather than through a rollout.
    ///
    /// Four rounds of review found four different ways to get this wrong while
    /// it was spread across the ingest loop. It is one function now, and this
    /// is the test that says what it means — a reviewer has one thing to
    /// check.
    #[test]
    fn a_refusal_survives_exactly_when_no_delta_was_measured_from_its_baseline() {
        let log = vec![
            refusal("a", 0, "{\"first\":true}"),
            refusal("b", 1, "{\"second\":true}"),
            refusal("c", 2, "{\"third\":true}"),
        ];

        // Nothing measured: every turn is owed its refusal.
        assert_eq!(
            surviving_refusals(&log, &HashSet::new())
                .iter()
                .map(|(uid, _)| uid.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b", "c"]
        );

        // A delta from generation 1 spans only what was recorded under 1.
        let measured = HashSet::from([1]);
        assert_eq!(
            surviving_refusals(&log, &measured)
                .iter()
                .map(|(uid, _)| uid.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "c"],
            "a generation that was measured settles its own refusals and no others"
        );

        // Measuring a generation nothing was recorded under changes nothing.
        assert_eq!(surviving_refusals(&log, &HashSet::from([7])).len(), 3);
    }

    /// One turn, refused on either side of a baseline reinstall. The later
    /// refusal must not stand in for the earlier one: the delta that follows
    /// covers the newer generation only, and the earlier span was absorbed
    /// into the reinstalled baseline where no delta can reach it.
    #[test]
    fn a_later_refusal_never_erases_an_earlier_one_for_the_same_turn() {
        let log = vec![
            refusal("a", 0, "{\"before\":true}"),
            refusal("a", 1, "{\"after\":true}"),
        ];
        let surviving = surviving_refusals(&log, &HashSet::from([1]));
        assert_eq!(
            surviving,
            vec![("a".to_string(), "{\"before\":true}".to_string())],
            "the pre-reinstall refusal survives, and is what the turn reports"
        );

        // And when both are settled, the turn is silent rather than refused:
        // its spend is inside the requests those deltas produced.
        assert!(surviving_refusals(&log, &HashSet::from([0, 1])).is_empty());

        // One entry per turn either way — a turn has one `token_json`.
        assert_eq!(surviving_refusals(&log, &HashSet::new()).len(), 1);
    }

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
            super::CURSOR_SYNC_STATE_KEY.into(),
            json!({transcript: test_file_cursor(30, 10, 200)}),
        );
        checkpoint_sync_state(&path, &newest);

        // A stale writer from the prior inode cannot restore its larger offset.
        let mut stale_generation = Map::new();
        stale_generation.insert("claude".into(), test_file_cursor(900, 9, 100));
        stale_generation.insert(
            super::CURSOR_SYNC_STATE_KEY.into(),
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
            super::CURSOR_SYNC_STATE_KEY.into(),
            json!({transcript: test_file_cursor(14, 10, 200)}),
        );
        checkpoint_sync_state(&path, &slow_same_generation);

        let saved = load_sync_state(&path).unwrap();
        assert_eq!(saved_cursor_offset(&saved["claude"]), 25);
        assert_eq!(
            saved_cursor_offset(&saved[super::CURSOR_SYNC_STATE_KEY][transcript]),
            30
        );
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
    /// A Grok session a previous release already consumed must be re-read
    /// once, and must come back with evidence instead of prompts alone.
    ///
    /// The change stamp of that session is unchanged on disk, so nothing but
    /// retiring the sync-state key can make `sync` look at it again — and the
    /// prompts it wrote with synthesized timestamps must be replaced, not
    /// joined by a second copy carrying the real ones.
    /// Build a Grok session directory from raw lines and index it.
    fn ingest_grok_lines(home: &Path, chat: &str, updates: &str) -> Connection {
        let dir = home.join(".grok/sessions/%2Ftmp%2Ffallback/grok-fb-0001");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("chat_history.jsonl"), chat).unwrap();
        fs::write(dir.join("updates.jsonl"), updates).unwrap();
        fs::write(
            dir.join("summary.json"),
            br#"{"info":{"id":"grok-fb-0001","cwd":"/tmp/fallback"},"created_at":"2026-01-01T00:00:00.000Z"}"#,
        )
        .unwrap();
        let conn = open_db(&home.join("history.db")).unwrap();
        let chat_path = dir.join("chat_history.jsonl");
        let session = super::scan_grok_session_file(&chat_path).unwrap().unwrap();
        super::ingest_grok_session(&conn, &session, &chat_path.to_string_lossy()).unwrap();
        conn
    }

    fn grok_event_time(conn: &Connection, text: &str) -> i64 {
        conn.query_row(
            "SELECT ts_ms FROM session_events WHERE source = 'grok' AND text = ?",
            params![text],
            |row| row.get(0),
        )
        .unwrap()
    }

    /// An assistant message with no recorded time of its own falls back to
    /// **its own** turn's start. Resolving against a turn index captured
    /// before the message told us which turn it was in hands it the previous
    /// turn's start — a time from before the person had even asked.
    #[test]
    fn an_untimed_assistant_message_falls_back_to_its_own_turns_start() {
        let home = tempfile::tempdir().unwrap();
        let conn = ingest_grok_lines(
            home.path(),
            &[
                r#"{"type":"user","content":"first"}"#,
                r#"{"type":"assistant","content":"answer one"}"#,
                // The second turn is the model continuing on its own: Grok
                // wrote no user record for it, so the chat side never learns
                // the turn advanced and only the matched group knows.
                r#"{"type":"assistant","content":"answer two"}"#,
                "",
            ]
            .join("\n"),
            &[
                r#"{"method":"session/update","params":{"update":{"sessionUpdate":"user_message_chunk"},"_meta":{"agentTimestampMs":1000,"turnStartMs":1000}}}"#,
                r#"{"method":"session/update","params":{"update":{"sessionUpdate":"agent_message_chunk"},"_meta":{"agentTimestampMs":2000,"turnStartMs":1000}}}"#,
                r#"{"method":"_x.ai/session/update","params":{"update":{"sessionUpdate":"turn_completed"},"_meta":{"agentTimestampMs":2500,"turnStartMs":1000}}}"#,
                // The second turn opens with the model continuing: the chunk
                // says which turn it belongs to, and records no time of its
                // own — which is the only reason the fallback is reached.
                r#"{"method":"session/update","params":{"update":{"sessionUpdate":"agent_message_chunk"},"_meta":{"eventId":"ev_late","turnStartMs":5000}}}"#,
                "",
            ]
            .join("\n"),
        );
        // The positive control: the first turn's events are exact, so the
        // second one's fallback is the only thing under test.
        assert_eq!(grok_event_time(&conn, "first"), 1000);
        assert_eq!(grok_event_time(&conn, "answer one"), 2000);

        assert_eq!(
            grok_event_time(&conn, "answer two"),
            5000,
            "an untimed reply belongs to the turn it answered, not the one before it"
        );
    }

    /// A marker is a record with a place in the file: the next record with no
    /// time of its own inherits the marker's, not that of whatever preceded
    /// it.
    #[test]
    fn a_marker_carries_its_time_forward_to_the_next_untimed_record() {
        let home = tempfile::tempdir().unwrap();
        let conn = ingest_grok_lines(
            home.path(),
            &[
                r#"{"type":"user","content":"first","timestamp_ms":1000}"#,
                r#"{"type":"user","content":"injected","synthetic_reason":"compaction","timestamp_ms":2000}"#,
                r#"{"type":"assistant","content":"after the marker"}"#,
                "",
            ]
            .join("\n"),
            "",
        );
        // The positive control: the marker really was stored at 2000, so the
        // event below inherits a time that exists rather than a default.
        let marker: i64 = conn
            .query_row(
                "SELECT ts_ms FROM session_markers WHERE source = 'grok' AND kind = 'synthetic_turn'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(marker, 2000);
        assert_eq!(
            grok_event_time(&conn, "after the marker"),
            2000,
            "the nearest preceding record is the marker, not the prompt before it"
        );
    }

    /// Reasoning and tools join their update the same way prose does, so they
    /// have to adopt the turn that match establishes. A model-initiated turn
    /// has no user record to advance the transcript-side cursor, so without
    /// that the fallback dates them from before the turn began.
    #[test]
    fn untimed_reasoning_and_tools_fall_back_to_their_own_turns_start() {
        let home = tempfile::tempdir().unwrap();
        let conn = ingest_grok_lines(
            home.path(),
            &[
                r#"{"type":"user","content":"first"}"#,
                r#"{"type":"reasoning","summary":"thinking in turn zero"}"#,
                r#"{"type":"assistant","content":"answer one"}"#,
                // The model continues on its own: no user record opens turn 1.
                r#"{"type":"reasoning","summary":"thinking in turn one"}"#,
                r#"{"type":"assistant","content":"","tool_calls":[{"id":"call_late","name":"Shell","arguments":{"command":"ls"}}]}"#,
                r#"{"type":"tool_result","tool_call_id":"call_late","content":"done"}"#,
                "",
            ]
            .join("\n"),
            &[
                r#"{"method":"session/update","params":{"update":{"sessionUpdate":"user_message_chunk"},"_meta":{"agentTimestampMs":1000,"turnStartMs":1000}}}"#,
                r#"{"method":"session/update","params":{"update":{"sessionUpdate":"agent_thought_chunk"},"_meta":{"agentTimestampMs":1100,"turnStartMs":1000}}}"#,
                r#"{"method":"session/update","params":{"update":{"sessionUpdate":"agent_message_chunk"},"_meta":{"agentTimestampMs":2000,"turnStartMs":1000}}}"#,
                r#"{"method":"_x.ai/session/update","params":{"update":{"sessionUpdate":"turn_completed"},"_meta":{"agentTimestampMs":2500,"turnStartMs":1000}}}"#,
                // Turn 1 opens on the model's own thinking, and none of its
                // rows records a time of its own.
                r#"{"method":"session/update","params":{"update":{"sessionUpdate":"agent_thought_chunk"},"_meta":{"eventId":"ev_think","turnStartMs":5000}}}"#,
                r#"{"method":"session/update","params":{"update":{"sessionUpdate":"tool_call","toolCallId":"call_late"},"_meta":{"turnStartMs":5000}}}"#,
                r#"{"method":"session/update","params":{"update":{"sessionUpdate":"tool_call_update","toolCallId":"call_late","status":"completed"},"_meta":{"turnStartMs":5000}}}"#,
                "",
            ]
            .join("\n"),
        );
        // The positive control: turn 0 is exact, so only the fallbacks below
        // are under test.
        assert_eq!(grok_event_time(&conn, "first"), 1000);
        assert_eq!(grok_event_time(&conn, "thinking in turn zero"), 1100);
        assert_eq!(grok_event_time(&conn, "answer one"), 2000);

        assert_eq!(
            grok_event_time(&conn, "thinking in turn one"),
            5000,
            "a thought belongs to the turn its group names, not the previous one"
        );
        let tool_times: Vec<i64> = conn
            .prepare(
                "SELECT ts_ms FROM session_events WHERE source = 'grok' \
                   AND event_uid IN ('tool:call_late', 'result:call_late') ORDER BY event_uid",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            tool_times,
            vec![5000, 5000],
            "a call and its result join by id, so their turn is known exactly"
        );
    }

    /// Encrypted-only reasoning still occupies an `agent_thoughts` ordinal.
    /// Leaving that cursor unmoved handed the next readable thought the
    /// encrypted record's timestamp and event id.
    #[test]
    fn encrypted_reasoning_consumes_its_thought_chunk_ordinal() {
        let home = tempfile::tempdir().unwrap();
        let conn = ingest_grok_lines(
            home.path(),
            &[
                r#"{"type":"user","content":"first"}"#,
                r#"{"type":"reasoning","encrypted_content":"opaque"}"#,
                r#"{"type":"reasoning","summary":"visible thought"}"#,
                r#"{"type":"assistant","content":"done"}"#,
                "",
            ]
            .join("\n"),
            &[
                r#"{"method":"session/update","params":{"update":{"sessionUpdate":"user_message_chunk"},"_meta":{"agentTimestampMs":1000,"turnStartMs":1000}}}"#,
                r#"{"method":"session/update","params":{"update":{"sessionUpdate":"agent_thought_chunk"},"_meta":{"agentTimestampMs":1100,"turnStartMs":1000}}}"#,
                // A new turnStartMs starts a second thought group. Consecutive
                // chunks in the same turn would merge into one group, which
                // would not exercise the ordinal join this test is about.
                r#"{"method":"session/update","params":{"update":{"sessionUpdate":"agent_thought_chunk"},"_meta":{"eventId":"ev_visible","agentTimestampMs":2000,"turnStartMs":2000}}}"#,
                r#"{"method":"session/update","params":{"update":{"sessionUpdate":"agent_message_chunk"},"_meta":{"agentTimestampMs":3000,"turnStartMs":2000}}}"#,
                "",
            ]
            .join("\n"),
        );
        let encrypted: i64 = conn
            .query_row(
                "SELECT ts_ms FROM session_markers WHERE source = 'grok' AND kind = 'encrypted_reasoning'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            encrypted, 1100,
            "the opaque trace uses its own chunk's time"
        );
        assert_eq!(
            grok_event_time(&conn, "visible thought"),
            2000,
            "the next thought must not inherit the encrypted chunk"
        );
        let uid: String = conn
            .query_row(
                "SELECT event_uid FROM session_events WHERE source = 'grok' AND text = 'visible thought'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(uid, "ev:ev_visible");
    }

    /// `sessions.first_prompt` is a bounded catalog excerpt. Hydration must
    /// not overwrite discovery's 4,096-character field with the full prompt.
    #[test]
    fn grok_first_prompt_is_the_bounded_catalog_excerpt() {
        let home = tempfile::tempdir().unwrap();
        let long = "x".repeat(crate::discover::EXCERPT_MAX_CHARS + 80);
        let conn = ingest_grok_lines(
            home.path(),
            &format!("{{\"type\":\"user\",\"content\":\"<user_query>{long}</user_query>\"}}\n"),
            concat!(
                r#"{"method":"session/update","params":{"update":{"sessionUpdate":"user_message_chunk"},"_meta":{"agentTimestampMs":1000,"turnStartMs":1000}}}"#,
                "\n",
            ),
        );
        let stored: String = conn
            .query_row(
                "SELECT first_prompt FROM sessions WHERE source = 'grok'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored.chars().count(), crate::discover::EXCERPT_MAX_CHARS);
        let history: String = conn
            .query_row(
                "SELECT prompt FROM history WHERE source = 'grok'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            history.chars().count(),
            crate::discover::EXCERPT_MAX_CHARS + 80,
            "the transcript keeps the full prompt; only the catalog excerpt is bounded"
        );
    }

    /// A directory that cannot be read is not an empty directory.
    ///
    /// The read is a replacement: an unreadable `compaction_checkpoints/`
    /// taken as empty would stamp the session as having no checkpoints and
    /// then delete the ones already stored, reporting success. It has to fail
    /// so the transaction rolls back.
    #[test]
    fn an_unreadable_sidecar_directory_fails_instead_of_erasing_the_evidence() {
        let home = tempfile::tempdir().unwrap();
        let dir = home
            .path()
            .join(".grok/sessions/%2Ftmp%2Fbroken/grok-brk-0001");
        fs::create_dir_all(dir.join("compaction_checkpoints")).unwrap();
        fs::write(
            dir.join("chat_history.jsonl"),
            b"{\"type\":\"user\",\"content\":\"hello\"}\n",
        )
        .unwrap();
        fs::write(
            dir.join("summary.json"),
            br#"{"info":{"id":"grok-brk-0001","cwd":"/tmp/broken"},"created_at":"2026-01-01T00:00:00.000Z"}"#,
        )
        .unwrap();
        fs::write(
            dir.join("compaction_checkpoints/1700000000000.json"),
            br#"{"created_at":"2026-01-01T00:10:00.000Z"}"#,
        )
        .unwrap();
        let chat = dir.join("chat_history.jsonl");

        let conn = open_db(&home.path().join("history.db")).unwrap();
        let session = super::scan_grok_session_file(&chat).unwrap().unwrap();
        super::ingest_grok_session(&conn, &session, &chat.to_string_lossy()).unwrap();
        let markers = || -> i64 {
            conn.query_row(
                "SELECT COUNT(*) FROM session_markers WHERE source = 'grok' AND kind = 'compaction_boundary'",
                [],
                |row| row.get(0),
            )
            .unwrap()
        };
        let prompts = || -> i64 {
            conn.query_row(
                "SELECT COUNT(*) FROM history WHERE source = 'grok'",
                [],
                |row| row.get(0),
            )
            .unwrap()
        };
        assert_eq!(markers(), 1);
        assert_eq!(prompts(), 1);

        // An entry that `read_dir` lists and `stat` cannot resolve: a symlink
        // to itself is ELOOP on every platform that has symlinks.
        let loop_entry = dir.join("compaction_checkpoints/loop.json");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&loop_entry, &loop_entry).unwrap();
        #[cfg(not(unix))]
        return;

        // The stamp fails rather than describing a directory it could not read.
        assert!(super::grok_session_stamp(&chat).is_err());

        // And so does the read, so nothing is replaced.
        let scanned = super::scan_grok_session_file(&chat);
        assert!(scanned.is_err(), "an unreadable entry must not scan clean");
        assert_eq!(markers(), 1, "the stored checkpoint marker must survive");
        assert_eq!(prompts(), 1);

        // Positive control: with the entry readable the same paths succeed and
        // the second checkpoint is indexed, so the failure above is the
        // unreadable entry and not the extra file.
        fs::remove_file(&loop_entry).unwrap();
        fs::write(&loop_entry, br#"{"created_at":"2026-01-01T00:20:00.000Z"}"#).unwrap();
        assert!(super::grok_session_stamp(&chat).is_ok());
        let session = super::scan_grok_session_file(&chat).unwrap().unwrap();
        super::ingest_grok_session(&conn, &session, &chat.to_string_lossy()).unwrap();
        assert_eq!(markers(), 2);
    }

    /// The stamp sync saved for one Grok session file.
    fn grok_saved_stamp<'a>(state: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
        state
            .get(super::GROK_SYNC_STATE_KEY)?
            .get(key)
            .and_then(super::grok_state_stamp)
    }

    /// Write a Grok session directory with no `updates.jsonl`, and answer with
    /// the transcript and the directory.
    fn grok_stream_fixture(home: &Path, id: &str) -> (PathBuf, PathBuf) {
        let dir = home.join(".grok/sessions/%2Ftmp%2Fstream").join(id);
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("chat_history.jsonl"),
            format!("{{\"type\":\"user\",\"content\":\"hello from {id}\"}}\n"),
        )
        .unwrap();
        fs::write(
            dir.join("summary.json"),
            format!(
                r#"{{"info":{{"id":"{id}","cwd":"/tmp/stream"}},"created_at":"2026-01-01T00:00:00.000Z"}}"#
            ),
        )
        .unwrap();
        (dir.join("chat_history.jsonl"), dir)
    }

    /// An `updates.jsonl` that exists and cannot be read is a failure, not an
    /// absent stream.
    ///
    /// Taking it as absent would replace every exact event time with a
    /// fallback and drop the token snapshots — and the change stamp, computed
    /// from metadata that is still perfectly readable, would then checkpoint
    /// that loss as the session's settled state until some file changes again.
    #[test]
    fn an_unreadable_update_stream_fails_instead_of_dating_events_from_fallbacks() {
        let home = tempfile::tempdir().unwrap();
        let (chat, dir) = grok_stream_fixture(home.path(), "grok-str-0001");

        // The positive control first: with no stream at all the session reads
        // clean and simply reports that there is none.
        let absent = super::scan_grok_session_file(&chat).unwrap().unwrap();
        assert!(!absent.updates_present);

        let stream = dir.join("updates.jsonl");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&stream, &stream).unwrap();
        #[cfg(not(unix))]
        return;

        let scanned = super::scan_grok_session_file(&chat);
        assert!(
            scanned.is_err(),
            "an unreadable update stream must not scan as an absent one"
        );

        // And once it is readable, the same path succeeds and the stream is
        // present — so the failure was the unreadable file, not its existence.
        fs::remove_file(&stream).unwrap();
        fs::write(
            &stream,
            br#"{"method":"session/update","params":{"update":{"sessionUpdate":"user_message_chunk"},"_meta":{"agentTimestampMs":1000,"turnStartMs":1000}}}
"#,
        )
        .unwrap();
        let read = super::scan_grok_session_file(&chat).unwrap().unwrap();
        assert!(read.updates_present);
        assert_eq!(read.first_ts, 1000);
    }

    /// "Nothing in it parsed" is not "it is not there". A stream of rows this
    /// parser does not interpret is a stream that exists, and a caller told it
    /// is missing would look for a file that is sitting right there.
    #[test]
    fn a_stream_of_rows_the_parser_cannot_use_is_present_not_missing() {
        let home = tempfile::tempdir().unwrap();
        let (chat, dir) = grok_stream_fixture(home.path(), "grok-str-0002");
        fs::write(
            dir.join("updates.jsonl"),
            b"{\"hello\":\"world\"}\n{\"another\":\"row\"}\n",
        )
        .unwrap();
        let conn = open_db(&home.path().join("history.db")).unwrap();
        let session = super::scan_grok_session_file(&chat).unwrap().unwrap();
        let outcome = super::ingest_grok_session(&conn, &session, &chat.to_string_lossy()).unwrap();
        assert!(
            !outcome.missing_updates,
            "the file exists; it just said nothing this parser understands"
        );
        assert!(outcome.updates_yielded_no_timing);
        assert_eq!(outcome.unread_update_rows, 2);

        // The positive control: with the file removed, it really is missing.
        fs::remove_file(dir.join("updates.jsonl")).unwrap();
        let session = super::scan_grok_session_file(&chat).unwrap().unwrap();
        let outcome = super::ingest_grok_session(&conn, &session, &chat.to_string_lossy()).unwrap();
        assert!(outcome.missing_updates);
        assert!(!outcome.updates_yielded_no_timing);
        assert_eq!(outcome.unread_update_rows, 0);
    }

    /// A stamp that fails closed has to be *reported*. Round four stopped one
    /// unreadable directory from failing the whole Grok pass; on its own that
    /// turned a store whose only session is unreadable into a silent success —
    /// the strictness produced an error and then swallowed it.
    #[test]
    fn a_store_whose_only_session_is_unreadable_reports_the_failure() {
        let home = tempfile::tempdir().unwrap();
        let (_chat, dir) = grok_stream_fixture(home.path(), "grok-sil-0001");
        let conn = open_db(&home.path().join("history.db")).unwrap();
        let root = home.path().join(".grok/sessions");

        // The positive control: a healthy store syncs and reports nothing.
        let mut state = Map::new();
        assert_eq!(super::sync_grok(&conn, &mut state, &root).unwrap(), 1);
        assert!(state.contains_key(super::GROK_SYNC_STATE_KEY));

        fs::create_dir_all(dir.join("compaction_checkpoints")).unwrap();
        let loop_entry = dir.join("compaction_checkpoints/loop.json");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&loop_entry, &loop_entry).unwrap();
        #[cfg(not(unix))]
        return;

        let mut state = Map::new();
        let error = super::sync_grok(&conn, &mut state, &root)
            .expect_err("a store that could read nothing is a failed source");
        assert!(
            format!("{error:#}").contains("could not be read"),
            "unexpected error: {error:#}"
        );

        // …and a store where something still worked keeps its progress rather
        // than failing wholesale, so one broken directory cannot send every
        // later run back over the whole store.
        let (_second, _) = grok_stream_fixture(home.path(), "grok-sil-0002");
        let mut state = Map::new();
        assert_eq!(super::sync_grok(&conn, &mut state, &root).unwrap(), 1);
        let saved = state[super::GROK_SYNC_STATE_KEY].as_object().unwrap();
        assert_eq!(saved.len(), 1, "only the readable session is checkpointed");
    }

    /// Every sidecar read obeys the same three-way rule: absent is nothing,
    /// malformed-but-present is evidence, unreadable is an error.
    ///
    /// The malformed case is the one that is easy to get wrong in the safe
    /// direction: collapsing it into "absent" deletes the marker a previous
    /// read wrote and records nothing in its place, so a session that *had* a
    /// `prompt_context` or `signals` file looks like one that never did.
    #[test]
    fn a_malformed_sidecar_is_evidence_and_an_unreadable_one_is_an_error() {
        let home = tempfile::tempdir().unwrap();
        let (chat, dir) = grok_stream_fixture(home.path(), "grok-side-0001");
        fs::write(
            dir.join("signals.json"),
            br#"{"contextTokensUsed":10,"turnCount":2,"compactionCount":1}"#,
        )
        .unwrap();
        fs::write(dir.join("prompt_context.json"), br#"{"files":[]}"#).unwrap();
        let conn = open_db(&home.path().join("history.db")).unwrap();
        let index = |conn: &Connection| {
            let session = super::scan_grok_session_file(&chat).unwrap().unwrap();
            super::ingest_grok_session(conn, &session, &chat.to_string_lossy()).unwrap()
        };
        let marker = |conn: &Connection, kind: &str| -> Option<(Option<String>, Option<String>)> {
            conn.query_row(
                "SELECT text, detail_json FROM session_markers \
                 WHERE source = 'grok' AND kind = ?",
                params![kind],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .ok()
        };

        // The positive control: parsed, both markers carry their detail.
        index(&conn);
        let signals = marker(&conn, "signals").expect("a signals marker");
        assert_eq!(
            signals.0.as_deref(),
            Some("turns=2 compactions=1 context_tokens_used=10")
        );
        assert!(signals.1.unwrap().contains("contextTokensUsed"));
        assert!(marker(&conn, "prompt_context").is_some());

        // Half-written: still a session that has a signals file, recorded with
        // no counters rather than deleted.
        fs::write(dir.join("signals.json"), b"{\"contextTokensUsed\":").unwrap();
        index(&conn);
        let signals = marker(&conn, "signals").expect("a malformed signals file is still evidence");
        assert_eq!(signals.0.as_deref(), Some(""));
        assert_eq!(signals.1.as_deref(), Some("{}"));

        // Absent: nothing to record, and the read is clean.
        fs::remove_file(dir.join("signals.json")).unwrap();
        index(&conn);
        assert!(marker(&conn, "signals").is_none());
        assert!(marker(&conn, "prompt_context").is_some());

        // Unreadable: an error, so the transaction rolls back and the
        // prompt_context marker this session already had survives.
        let context = dir.join("prompt_context.json");
        fs::remove_file(&context).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&context, &context).unwrap();
        #[cfg(not(unix))]
        return;
        assert!(super::scan_grok_session_file(&chat).is_err());
        assert!(
            marker(&conn, "prompt_context").is_some(),
            "a failed read must leave the previous evidence alone"
        );
        // …and an unreadable summary.json does not quietly re-identify the
        // session from its directory name either.
        fs::remove_file(&context).unwrap();
        let summary = dir.join("summary.json");
        fs::remove_file(&summary).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&summary, &summary).unwrap();
        assert!(super::scan_grok_session_file(&chat).is_err());
    }

    /// A fully synced store indexes nothing on every later run. Deciding
    /// whether the source failed from "sessions indexed" therefore fails it
    /// forever over one unreadable sibling; the count has to be "sessions this
    /// run could account for".
    #[test]
    fn an_unchanged_session_still_accounts_for_itself_when_a_sibling_is_unreadable() {
        let home = tempfile::tempdir().unwrap();
        let (_chat, healthy) = grok_stream_fixture(home.path(), "grok-acc-0001");
        let conn = open_db(&home.path().join("history.db")).unwrap();
        let root = home.path().join(".grok/sessions");

        let mut state = Map::new();
        assert_eq!(super::sync_grok(&conn, &mut state, &root).unwrap(), 1);

        // A second session that cannot be read, beside one that is now
        // unchanged. The store is still healthy enough to report.
        let (_broken, broken) = grok_stream_fixture(home.path(), "grok-acc-0002");
        fs::create_dir_all(broken.join("compaction_checkpoints")).unwrap();
        let loop_entry = broken.join("compaction_checkpoints/loop.json");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&loop_entry, &loop_entry).unwrap();
        #[cfg(not(unix))]
        return;

        let inserted = super::sync_grok(&conn, &mut state, &root)
            .expect("an unchanged session accounts for itself; the source has not failed");
        assert_eq!(inserted, 0, "nothing new was indexed, and that is fine");
        assert_eq!(
            state[super::GROK_SYNC_STATE_KEY].as_object().unwrap().len(),
            1,
            "the healthy session keeps its checkpoint and the broken one gets none"
        );

        // The positive control from round five still holds: with *only* the
        // unreadable session in the store, the source fails.
        fs::remove_dir_all(&healthy).unwrap();
        let mut fresh = Map::new();
        let error = super::sync_grok(&conn, &mut fresh, &root)
            .expect_err("a store that could account for nothing is a failed source");
        assert!(format!("{error:#}").contains("could not be read"));
    }

    /// What repeated prompts at one timestamp do to `history`, stated rather
    /// than left to be discovered.
    ///
    /// `history` is keyed `UNIQUE(source, timestamp_ms, prompt)` and inserted
    /// with `INSERT OR IGNORE`. Two turns with the same text at the same
    /// millisecond are therefore one history row -- and `session_id` is not in
    /// that key, so this holds across sessions and is a property of the shared
    /// table, not of this parser: the positive control below shows two
    /// different Claude sessions collapsing exactly the same way.
    ///
    /// Grok makes it visible rather than causing it. A session with no
    /// `updates.jsonl` and no per-record times has one real timestamp --
    /// `created_at` -- for every turn, so two `continue` turns collide where
    /// another provider's per-record clock would separate them. The honest
    /// answer is not to space them out by an artificial millisecond each:
    /// that is precisely the synthesized `first_ts + index` ladder this work
    /// deleted, and it would put a fabricated time in a ledger whose value is
    /// that its times are real.
    ///
    /// So the transcript, which is keyed per record, keeps both turns, and the
    /// prompt rollup keeps one. Widening the `history` key is a change to a
    /// contract every provider shares (#198 and #199 rest on it too) and is
    /// not this PR's to make; this test pins the behaviour so that whoever
    /// does change it sees what Grok expects.
    #[test]
    fn repeated_prompts_at_one_timestamp_are_one_history_row_in_every_source() {
        let home = tempfile::tempdir().unwrap();
        let dir = home
            .path()
            .join(".grok/sessions/%2Ftmp%2Fstream/grok-dup-0001");
        fs::create_dir_all(&dir).unwrap();
        // No `updates.jsonl`, and no record carries a time of its own, so
        // every turn falls back to `created_at`.
        fs::write(
            dir.join("chat_history.jsonl"),
            concat!(
                "{\"type\":\"user\",\"content\":\"continue\"}\n",
                "{\"type\":\"assistant\",\"content\":\"ok\"}\n",
                "{\"type\":\"user\",\"content\":\"continue\"}\n",
                "{\"type\":\"assistant\",\"content\":\"ok again\"}\n",
            ),
        )
        .unwrap();
        fs::write(
            dir.join("summary.json"),
            r#"{"info":{"id":"grok-dup-0001","cwd":"/tmp/stream"},"created_at":"2026-01-01T00:00:00.000Z"}"#,
        )
        .unwrap();
        let conn = open_db(&home.path().join("history.db")).unwrap();
        let mut state = Map::new();
        super::sync_grok(&conn, &mut state, &home.path().join(".grok/sessions")).unwrap();

        let count = |sql: &str| -> i64 { conn.query_row(sql, [], |row| row.get(0)).unwrap() };
        assert_eq!(
            count(
                "SELECT COUNT(*) FROM session_events \
                 WHERE session_id = 'grok-dup-0001' AND role = 'user'"
            ),
            2,
            "both turns are in the transcript, which is what replay reads"
        );
        let times: Vec<i64> = conn
            .prepare(
                "SELECT ts_ms FROM session_events \
                 WHERE session_id = 'grok-dup-0001' AND role = 'user' ORDER BY id",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            times[0], times[1],
            "the session records one real time, and both turns carry it unaltered"
        );
        assert_eq!(
            count("SELECT COUNT(*) FROM history WHERE source = 'grok'"),
            1,
            "the prompt rollup keys on (source, timestamp_ms, prompt), so they are one row"
        );

        // The positive control: the same collapse on a source with per-record
        // times, proving the key and not the Grok parser is what does this.
        let entry = |source: &str, session: &str| HistoryEntry {
            id: 0,
            source: source.into(),
            session_id: Some(session.to_string()),
            project: Some("/tmp/stream".into()),
            prompt_hash: Some(prompt_hash("continue")),
            prompt: "continue".into(),
            timestamp_ms: times[0],
        };
        assert_eq!(
            insert_history(&conn, &entry("claude", "claude-a")).unwrap(),
            1,
            "the first of the two is inserted, so the control can observe a difference"
        );
        assert_eq!(
            insert_history(&conn, &entry("claude", "claude-b")).unwrap(),
            0,
            "a different Claude session collapses the same way; session_id is not in the key"
        );
    }

    /// Replacing one session must not delete another session's prompt.
    ///
    /// This is the shared `history` key -- `UNIQUE(source, timestamp_ms,
    /// prompt)`, no `session_id` -- surfacing a third time, and the one place
    /// where it costs data rather than just collapsing a count. Two Grok
    /// sessions both containing `continue` at the same millisecond are **one**
    /// history row, attributed to whichever was indexed first. Grok ingestion
    /// replaces a session's evidence, and deleting that session's history rows
    /// outright took the shared row with it: the other session's prompt
    /// disappeared from search although nothing about it had changed, and
    /// nothing would ever put it back, because its own files never change
    /// again.
    ///
    /// The row is **re-attributed**, not skipped. Skipping would leave it
    /// filed under a session that no longer contains the prompt -- untrue, and
    /// undeletable, since the only session that could clean it up is the one
    /// that no longer evidences it. Re-attribution moves it to a session whose
    /// stored events carry that prompt now, so the row stays both true and
    /// owned.
    #[test]
    fn replacing_a_session_leaves_a_prompt_another_session_still_has() {
        let home = tempfile::tempdir().unwrap();
        let write = |id: &str, prompts: &[&str]| {
            let dir = home.path().join(".grok/sessions/%2Ftmp%2Fshared").join(id);
            fs::create_dir_all(&dir).unwrap();
            let transcript: String = prompts
                .iter()
                .map(|prompt| format!("{{\"type\":\"user\",\"content\":\"{prompt}\"}}\n"))
                .collect();
            fs::write(dir.join("chat_history.jsonl"), transcript).unwrap();
            // The same `created_at` for both, so their untimed prompts land on
            // the same millisecond -- which is exactly how this happens in the
            // field, for a session with no `updates.jsonl`.
            fs::write(
                dir.join("summary.json"),
                format!(
                    r#"{{"info":{{"id":"{id}","cwd":"/tmp/shared"}},"created_at":"2026-01-01T00:00:00.000Z"}}"#
                ),
            )
            .unwrap();
        };
        let root = home.path().join(".grok/sessions");
        let conn = open_db(&home.path().join("history.db")).unwrap();
        let prompts = |session: &str| -> Vec<String> {
            conn.prepare(
                "SELECT prompt FROM history WHERE source = 'grok' AND session_id = ? \
                 ORDER BY prompt",
            )
            .unwrap()
            .query_map(params![session], |row| row.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap()
        };

        // A is indexed first, so A wins the shared row; B's identical prompt
        // is ignored on insert. `only-a` is A's alone.
        write("grok-shared-a", &["continue", "only-a"]);
        write("grok-shared-b", &["continue", "only-b"]);
        let mut state = Map::new();
        super::sync_grok(&conn, &mut state, &root).unwrap();
        assert_eq!(
            prompts("grok-shared-a"),
            vec!["continue".to_string(), "only-a".to_string()],
            "A owns the shared row because it was indexed first"
        );
        assert_eq!(
            prompts("grok-shared-b"),
            vec!["only-b".to_string()],
            "B's identical prompt was ignored on insert; that is the setup"
        );

        // A is compacted: the shared prompt is gone from its transcript, and
        // so is `only-a`.
        write("grok-shared-a", &["after-compaction"]);
        super::sync_grok(&conn, &mut state, &root).unwrap();

        // B never changed, and B still contains `continue`. It has to still
        // be findable -- under B, since B is what evidences it now.
        assert_eq!(
            prompts("grok-shared-b"),
            vec!["continue".to_string(), "only-b".to_string()],
            "B's prompt must survive a replacement of A, re-attributed to B"
        );

        // The positive control: a prompt only A had is gone. Without this the
        // fix could simply be "never delete history".
        let all: Vec<String> = conn
            .prepare("SELECT prompt FROM history WHERE source = 'grok' ORDER BY prompt")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert!(
            !all.iter().any(|prompt| prompt == "only-a"),
            "a prompt only the replaced session had is gone: {all:?}"
        );
        assert!(
            all.iter().any(|prompt| prompt == "after-compaction"),
            "and the replacement's own prompt is there: {all:?}"
        );
        let orphaned: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM history WHERE source = 'grok' AND session_id IS NULL",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            orphaned, 0,
            "re-attribution must find a real owner, never null one out"
        );
    }

    /// A half-written `summary.json` must not invent a session id.
    ///
    /// `summary.json` is the one sidecar that carries **identity** rather than
    /// detail: `info.id` becomes the session id, which keys every evidence row
    /// and scopes every delete a replacing read performs. Falling back to the
    /// directory name when the file was present but unparseable -- a summary
    /// caught mid-write -- stored the whole transcript under the encoded-cwd
    /// folder name. Once the file was repaired the next read stored the same
    /// session under its real id, and the first set of rows was stranded: no
    /// read would ever name that id again, and deletes are scoped by the id
    /// that produced them, so nothing would ever remove them.
    ///
    /// So the three-way rule from round seven applies here with malformed on
    /// the failing side: **absent** keeps the directory-name fallback (a
    /// session directory with no summary is a real shape, and its folder name
    /// is the only identity there is), while **present-but-unparseable** fails
    /// the read, leaving the previous evidence and the stamp alone.
    #[test]
    fn a_malformed_summary_fails_rather_than_naming_the_session_after_its_folder() {
        let home = tempfile::tempdir().unwrap();
        let dir = home
            .path()
            .join(".grok/sessions/local-folder/grok-phantom-0001");
        fs::create_dir_all(&dir).unwrap();
        let chat = dir.join("chat_history.jsonl");
        fs::write(&chat, "{\"type\":\"user\",\"content\":\"hello\"}\n").unwrap();
        let summary = dir.join("summary.json");
        let valid = r#"{"info":{"id":"grok-123","cwd":"/tmp/phantom"},"created_at":"2026-01-01T00:00:00.000Z"}"#;
        fs::write(&summary, valid).unwrap();

        let conn = open_db(&home.path().join("history.db")).unwrap();
        let root = home.path().join(".grok/sessions");
        let ids = || -> Vec<String> {
            conn.prepare(
                "SELECT session_id FROM sessions WHERE source = 'grok' ORDER BY session_id",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap()
        };

        // Positive control one: a valid summary indexes under its own id.
        let mut state = Map::new();
        super::sync_grok(&conn, &mut state, &root).unwrap();
        assert_eq!(ids(), vec!["grok-123".to_string()]);
        let key = chat.to_string_lossy().to_string();
        let stamp = grok_saved_stamp(&state, &key).unwrap().to_string();

        // The summary is caught mid-write.
        fs::write(&summary, "{\"info\":{\"id\":").unwrap();
        let error = match super::scan_grok_session_file(&chat) {
            Err(error) => error,
            Ok(_) => panic!("a summary that carries identity and does not parse is a failed read"),
        };
        assert!(
            format!("{error:#}").contains("summary.json"),
            "unexpected error: {error:#}"
        );

        super::sync_grok(&conn, &mut state, &root)
            .expect_err("the only session in the store could not be read");
        assert_eq!(
            ids(),
            vec!["grok-123".to_string()],
            "no session is invented from the folder name, and the good rows stay"
        );
        assert_eq!(
            grok_saved_stamp(&state, &key),
            Some(stamp.as_str()),
            "the stamp does not advance over a failed read, so a repair is picked up"
        );

        // Repaired, and it is still the same session -- no second identity.
        fs::write(&summary, valid).unwrap();
        super::sync_grok(&conn, &mut state, &root).unwrap();
        assert_eq!(ids(), vec!["grok-123".to_string()]);

        // Positive control two: with no summary at all, the directory name is
        // still the identity. Without this the fix could be "always fail
        // without a summary", which would stop indexing a real Grok shape.
        fs::remove_file(&summary).unwrap();
        let mut fresh = Map::new();
        super::sync_grok(&conn, &mut fresh, &root).unwrap();
        assert!(
            ids().contains(&"grok-phantom-0001".to_string()),
            "an absent summary still falls back to the directory name: {:?}",
            ids()
        );
    }

    /// A session with no events and no markers is still an indexed session.
    ///
    /// Round eight recorded whether the indexing run wrote any evidence, so a
    /// session that wrote none would not be re-read for ever looking for rows
    /// it never had. But it then skipped such a session *without asking the
    /// database anything at all* -- and "wrote no evidence" was measured from
    /// events and markers only. Two things fall through that:
    ///
    /// - a Grok session whose transcript is empty but whose `subagents/`
    ///   directory names a child writes a **relationship**, and was recorded
    ///   as having written nothing;
    /// - every ingestion writes a **catalog row**, and nothing checked it.
    ///
    /// `.sync-state.json` survives a rebuilt `history.db`, so after a rebuild
    /// the stamp matched, the entry said "no evidence", the run skipped, and
    /// neither the catalog row nor the relationship ever came back -- for a
    /// finished session directory, permanently.
    #[test]
    fn an_empty_session_with_a_subagent_survives_a_rebuilt_database() {
        let home = tempfile::tempdir().unwrap();
        let dir = home
            .path()
            .join(".grok/sessions/%2Ftmp%2Fstream/grok-void-0001");
        fs::create_dir_all(dir.join("subagents")).unwrap();
        // A transcript with nothing in it that becomes an event or a marker.
        fs::write(dir.join("chat_history.jsonl"), "").unwrap();
        fs::write(
            dir.join("summary.json"),
            r#"{"info":{"id":"grok-void-0001","cwd":"/tmp/stream"},"created_at":"2026-01-01T00:00:00.000Z"}"#,
        )
        .unwrap();
        fs::write(
            dir.join("subagents/agent-review.json"),
            r#"{"session_id":"grok-void-child","agent_type":"review","spawned_at":"2026-01-01T00:01:00.000Z"}"#,
        )
        .unwrap();
        let root = home.path().join(".grok/sessions");
        let db_path = home.path().join("history.db");

        let counts = |conn: &Connection| -> (i64, i64, i64, i64) {
            let one = |sql: &str| -> i64 { conn.query_row(sql, [], |row| row.get(0)).unwrap() };
            (
                one("SELECT COUNT(*) FROM sessions WHERE source = 'grok'"),
                one("SELECT COUNT(*) FROM session_relationships WHERE source = 'grok'"),
                one("SELECT COUNT(*) FROM session_events WHERE source = 'grok'"),
                one("SELECT COUNT(*) FROM session_markers WHERE source = 'grok'"),
            )
        };

        let conn = open_db(&db_path).unwrap();
        let mut state = Map::new();
        super::sync_grok(&conn, &mut state, &root).unwrap();
        let (sessions, relationships, events, markers) = counts(&conn);
        assert_eq!(
            (events, markers),
            (0, 0),
            "the fixture must write neither, or it does not test the case"
        );
        assert_eq!(sessions, 1, "but indexing it did write a catalog row");
        assert_eq!(relationships, 1, "and the subagent relationship");

        // The positive control: with the database intact, the unchanged
        // session is skipped -- the round-eight behaviour this must not undo.
        let relationship_ids: Vec<i64> = conn
            .prepare("SELECT rowid FROM session_relationships WHERE source = 'grok' ORDER BY rowid")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        super::sync_grok(&conn, &mut state, &root).unwrap();
        let after: Vec<i64> = conn
            .prepare("SELECT rowid FROM session_relationships WHERE source = 'grok' ORDER BY rowid")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            after, relationship_ids,
            "an unchanged session whose rows are present is still skipped"
        );

        // The database is rebuilt. The state file, living beside it, survives
        // -- and the session directory will never change again.
        drop(conn);
        fs::remove_file(&db_path).unwrap();
        let conn = open_db(&db_path).unwrap();
        assert_eq!(counts(&conn), (0, 0, 0, 0), "the rebuild really is empty");

        super::sync_grok(&conn, &mut state, &root).unwrap();
        let (sessions, relationships, _, _) = counts(&conn);
        assert_eq!(sessions, 1, "the catalog row has to come back");
        assert_eq!(relationships, 1, "and so does the relationship");
    }

    /// A session whose only evidence is markers is an indexed session.
    ///
    /// Round seven stopped trusting an unchanged stamp on its own, by asking
    /// the database whether the session's rows were still there. Asking only
    /// about `session_events` was too narrow: Grok writes events for user,
    /// assistant, tool and readable-reasoning records, and a session made only
    /// of `system` lines, synthetic turns or encrypted reasoning lands in
    /// `session_markers` alone. Such a session answered "not indexed" on every
    /// later sync and was re-read in full every time -- a fix that made the
    /// hot path do the thing it was added to prevent.
    ///
    /// So the indexing run records whether it wrote any evidence, and the
    /// check asks about the tables Grok actually writes.
    #[test]
    fn a_session_stored_only_as_markers_is_not_re_read_every_sync() {
        let home = tempfile::tempdir().unwrap();
        let dir = home
            .path()
            .join(".grok/sessions/%2Ftmp%2Fstream/grok-mark-0001");
        fs::create_dir_all(&dir).unwrap();
        // `system` and encrypted reasoning: markers, and not one event.
        fs::write(
            dir.join("chat_history.jsonl"),
            concat!(
                "{\"type\":\"system\",\"content\":\"session opened\"}\n",
                "{\"type\":\"reasoning\",\"encrypted_content\":\"b64\"}\n",
            ),
        )
        .unwrap();
        fs::write(
            dir.join("summary.json"),
            r#"{"info":{"id":"grok-mark-0001","cwd":"/tmp/stream"},"created_at":"2026-01-01T00:00:00.000Z"}"#,
        )
        .unwrap();
        let conn = open_db(&home.path().join("history.db")).unwrap();
        let root = home.path().join(".grok/sessions");

        let count = |sql: &str| -> i64 { conn.query_row(sql, [], |row| row.get(0)).unwrap() };
        let mut state = Map::new();
        super::sync_grok(&conn, &mut state, &root).unwrap();
        assert_eq!(
            count("SELECT COUNT(*) FROM session_events WHERE source = 'grok'"),
            0,
            "the fixture must produce no events, or it does not test the case"
        );
        let markers = count("SELECT COUNT(*) FROM session_markers WHERE source = 'grok'");
        assert!(markers > 0, "but it does produce markers");

        // The second sync must skip it. `sync_grok` returns prompts, which are
        // zero either way here, so the re-read is detected by the markers
        // being deleted and rewritten rather than left alone.
        let marker_ids: Vec<i64> = conn
            .prepare("SELECT id FROM session_markers WHERE source = 'grok' ORDER BY id")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        super::sync_grok(&conn, &mut state, &root).unwrap();
        let after: Vec<i64> = conn
            .prepare("SELECT id FROM session_markers WHERE source = 'grok' ORDER BY id")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            after, marker_ids,
            "an unchanged marker-only session must be skipped, not deleted and rewritten"
        );

        // The positive control: round seven's case still holds. Wipe the
        // evidence and the same unchanged session is read again.
        conn.execute("DELETE FROM session_markers WHERE source = 'grok'", [])
            .unwrap();
        super::sync_grok(&conn, &mut state, &root).unwrap();
        assert!(
            count("SELECT COUNT(*) FROM session_markers WHERE source = 'grok'") > 0,
            "a session with no stored evidence at all is still re-read"
        );
    }

    /// An unchanged file whose rows are gone is not an indexed session.
    ///
    /// `.sync-state.json` sits beside `history.db`, so deleting or rebuilding
    /// the database leaves the state file behind. A stamp read on its own then
    /// answers "already indexed" for a session with no rows at all, and goes
    /// on answering it until some file in the directory changes -- which, for
    /// a session the user has finished with, is never. The stamp has to be
    /// checked against the database, the way the Codex and Claude walks
    /// already check theirs.
    #[test]
    fn an_unchanged_session_whose_evidence_was_wiped_is_indexed_again() {
        let home = tempfile::tempdir().unwrap();
        let (chat, _dir) = grok_stream_fixture(home.path(), "grok-wipe-0001");
        let conn = open_db(&home.path().join("history.db")).unwrap();
        let root = home.path().join(".grok/sessions");
        let key = chat.to_string_lossy().to_string();

        let mut state = Map::new();
        assert_eq!(super::sync_grok(&conn, &mut state, &root).unwrap(), 1);
        let stamp = grok_saved_stamp(&state, &key).unwrap().to_string();

        // The positive control: nothing was touched, the rows are there, and
        // the run correctly does no work.
        assert_eq!(super::sync_grok(&conn, &mut state, &root).unwrap(), 0);
        assert!(super::session_events_exist(&conn, "grok", "grok-wipe-0001").unwrap());

        // The database is rebuilt; the state file, living beside it, survives.
        conn.execute("DELETE FROM session_events WHERE source = 'grok'", [])
            .unwrap();
        assert!(!super::session_events_exist(&conn, "grok", "grok-wipe-0001").unwrap());
        assert_eq!(
            grok_saved_stamp(&state, &key),
            Some(stamp.as_str()),
            "the file has not changed, so the stamp still matches"
        );

        assert_eq!(
            super::sync_grok(&conn, &mut state, &root).unwrap(),
            1,
            "an unchanged file with no rows behind it has to be read again"
        );
        assert!(super::session_events_exist(&conn, "grok", "grok-wipe-0001").unwrap());
    }

    /// A finished row that does not parse is a damaged file, not an empty one.
    ///
    /// Grok ingestion **replaces** a session's evidence: it deletes the stored
    /// events and writes what it just read. Dropping an unparseable row would
    /// therefore commit a transcript that is missing a turn Grok did write,
    /// and then save the change stamp that stops the next run from ever
    /// looking at the file again -- the loss becomes the session's settled
    /// state. The read has to fail instead, so the previous transaction stands
    /// and the stamp stays behind for a retry.
    ///
    /// The line that has *not* been newline-terminated is the one exception,
    /// and the positive controls below hold it: a fragment Grok is still
    /// writing is ignorable, and a valid rewrite still replaces.
    #[test]
    fn a_malformed_finished_row_fails_the_read_instead_of_erasing_the_session() {
        let home = tempfile::tempdir().unwrap();
        let (chat, _dir) = grok_stream_fixture(home.path(), "grok-mal-0001");
        let (sibling, _) = grok_stream_fixture(home.path(), "grok-mal-0002");
        let conn = open_db(&home.path().join("history.db")).unwrap();
        let root = home.path().join(".grok/sessions");
        let key = chat.to_string_lossy().to_string();
        let events = |session: &str| -> i64 {
            conn.query_row(
                "SELECT COUNT(*) FROM session_events WHERE session_id = ?",
                [session],
                |row| row.get(0),
            )
            .unwrap()
        };

        let mut state = Map::new();
        assert_eq!(super::sync_grok(&conn, &mut state, &root).unwrap(), 2);
        let indexed = events("grok-mal-0001");
        assert!(indexed > 0, "the session was indexed before it was damaged");
        let first_stamp = grok_saved_stamp(&state, &key)
            .expect("a healthy session is checkpointed")
            .to_string();

        // The one complete row of the transcript, damaged but still finished.
        fs::write(&chat, "{\"type\":\"user\",\"content\":\n").unwrap();
        let damaged_stamp = grok_session_stamp(&chat).unwrap();
        assert_ne!(
            damaged_stamp, first_stamp,
            "the rewrite has to look like a change, or the run would skip it"
        );

        let inserted = super::sync_grok(&conn, &mut state, &root)
            .expect("the readable sibling accounts for itself; the source has not failed");
        assert_eq!(inserted, 0, "nothing was indexed from the damaged session");
        assert_eq!(
            events("grok-mal-0001"),
            indexed,
            "the evidence from the last good read survives the failed one"
        );
        assert_eq!(
            grok_saved_stamp(&state, &key),
            Some(first_stamp.as_str()),
            "the stamp does not advance over a read that failed, so the next run retries"
        );

        // Positive control one: a trailing fragment with no newline is a
        // record still being written. It scans clean, and the finished rows
        // before it are still indexed.
        fs::write(
            &sibling,
            "{\"type\":\"user\",\"content\":\"finished\"}\n{\"type\":\"user\",\"cont",
        )
        .unwrap();
        let scanned = super::scan_grok_session_file(&sibling)
            .expect("an unfinished tail is not a damaged file")
            .expect("the finished row is still a session");
        assert_eq!(scanned.lines.len(), 1, "the fragment is skipped, not read");

        // Positive control two: repair the transcript and the session is
        // replaced, stamp and all.
        fs::write(&chat, "{\"type\":\"user\",\"content\":\"rewritten\"}\n").unwrap();
        assert_eq!(
            super::sync_grok(&conn, &mut state, &root).unwrap(),
            2,
            "the repaired session and the sibling control one rewrote both re-index"
        );
        assert_ne!(
            grok_saved_stamp(&state, &key),
            Some(first_stamp.as_str()),
            "a read that worked does advance the stamp"
        );
        let text: String = conn
            .query_row(
                "SELECT text FROM session_events WHERE session_id = ? AND role = 'user'",
                ["grok-mal-0001"],
                |row| row.get(0),
            )
            .unwrap();
        assert!(text.contains("rewritten"), "unexpected content: {text}");
    }

    #[test]
    fn retiring_the_grok_state_key_re_reads_a_session_sync_already_consumed() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let session = home.join(".grok/sessions/%2Ftmp%2Fdemo/grok-evt-0001");
        fs::create_dir_all(&session).unwrap();
        let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/grok/events-session/.grok/sessions/%2Ftmp%2Fdemo/grok-evt-0001");
        for name in ["chat_history.jsonl", "updates.jsonl", "summary.json"] {
            fs::copy(fixture.join(name), session.join(name)).unwrap();
        }
        let db_path = home.join("history.db");
        let chat = session.join("chat_history.jsonl");

        // What the previous release left behind: the old state key, and a
        // prompt stamped `created_at + index`.
        let mut previous = Map::new();
        previous.insert(
            "grok_sessions".into(),
            json!({ chat.to_string_lossy(): grok_session_stamp(&chat).unwrap() }),
        );
        save_sync_state(&home.join(".sync-state.json"), &previous).unwrap();
        {
            let conn = open_db(&db_path).unwrap();
            insert_history(
                &conn,
                &HistoryEntry {
                    id: 0,
                    source: "grok".into(),
                    session_id: Some("grok-evt-0001".into()),
                    project: Some("/tmp/demo".into()),
                    prompt_hash: None,
                    prompt: "add a retry to the http client".into(),
                    timestamp_ms: 1_789_560_000_000,
                },
            )
            .unwrap();
            insert_history(
                &conn,
                &HistoryEntry {
                    id: 0,
                    source: "grok".into(),
                    session_id: Some("grok-evt-0001".into()),
                    project: Some("/tmp/demo".into()),
                    prompt_hash: None,
                    prompt: "now a test".into(),
                    // The fabricated one: one millisecond after the first.
                    timestamp_ms: 1_789_560_000_001,
                },
            )
            .unwrap();
        }

        sync_local_at_with_home(&db_path, home).unwrap();

        let conn = open_db(&db_path).unwrap();
        let prompts: Vec<(String, i64)> = conn
            .prepare("SELECT prompt, timestamp_ms FROM history WHERE source = 'grok' ORDER BY timestamp_ms")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            prompts,
            vec![
                (
                    "add a retry to the http client".to_string(),
                    1_789_560_000_000
                ),
                ("now a test".to_string(), 1_789_560_120_000),
            ],
            "the synthesized timestamp must be replaced, not duplicated"
        );
        let events: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_events WHERE source = 'grok'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(events > 0, "the re-read must produce events");

        let state = load_sync_state(&home.join(".sync-state.json")).unwrap();
        assert!(state.contains_key(GROK_SYNC_STATE_KEY));
        assert!(
            !state.contains_key("grok_sessions"),
            "the retired key must be gone from disk, not just from memory"
        );

        // Steady state: the second run reads nothing and changes nothing.
        sync_local_at_with_home(&db_path, home).unwrap();
        let after: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_events WHERE source = 'grok'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(after, events);
    }

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
            RequestIdentity::none(),
            "event-1",
            None,
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
                saved_cursor_offset(
                    &state[super::CURSOR_SYNC_STATE_KEY][path.to_string_lossy().as_ref()]
                ),
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
            saved_cursor_offset(
                &state[super::CURSOR_SYNC_STATE_KEY][first.to_string_lossy().as_ref()]
            ),
            fs::metadata(&first).unwrap().len(),
            "the readable transcript's checkpoint must advance"
        );
        assert_eq!(
            state[super::CURSOR_SYNC_STATE_KEY].get(second.to_string_lossy().as_ref()),
            saved[super::CURSOR_SYNC_STATE_KEY].get(second.to_string_lossy().as_ref()),
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
            saved_cursor_offset(
                &state[super::CURSOR_SYNC_STATE_KEY][cursor.to_string_lossy().as_ref()]
            ),
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
            saved_cursor_offset(
                &state[super::CURSOR_SYNC_STATE_KEY][cursor.to_string_lossy().as_ref()]
            ),
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
                saved_cursor_offset(
                    &state[super::CURSOR_SYNC_STATE_KEY][path.to_string_lossy().as_ref()]
                ),
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

    fn cursor_fixture(name: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/cursor")
            .join(name)
    }

    /// Stage a fixture where a Cursor install would put it, so the project
    /// decoding and the session id both come from the real path layout.
    fn stage_cursor_fixture(root: &Path, fixture: &str, session_id: &str) -> PathBuf {
        let transcript = root
            .join(".cursor/projects/home-dev-demo/agent-transcripts")
            .join(session_id)
            .join(format!("{session_id}.jsonl"));
        fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        fs::copy(cursor_fixture(fixture), &transcript).unwrap();
        transcript
    }

    fn event_kinds(conn: &Connection, session_id: &str) -> Vec<(String, String)> {
        crate::session_events(conn, session_id, Some("cursor"))
            .unwrap()
            .into_iter()
            .map(|event| (event.role, event.kind))
            .collect()
    }

    /// Write one Cursor transcript verbatim, so a test can control the exact
    /// bytes — a partial final line, a rewrite, a multi-file patch.
    fn write_cursor_transcript(root: &Path, session_id: &str, body: &str) -> PathBuf {
        let transcript = root
            .join(".cursor/projects/home-dev-demo/agent-transcripts")
            .join(session_id)
            .join(format!("{session_id}.jsonl"));
        fs::create_dir_all(transcript.parent().unwrap()).unwrap();
        fs::write(&transcript, body).unwrap();
        transcript
    }

    fn set_file_mtime_ms(path: &Path, ms: i64) {
        let file = fs::OpenOptions::new().write(true).open(path).unwrap();
        let when = std::time::SystemTime::UNIX_EPOCH + Duration::from_millis(ms as u64);
        file.set_times(fs::FileTimes::new().set_modified(when))
            .unwrap();
    }

    fn cursor_row_count(conn: &Connection, table: &str, session_id: &str) -> i64 {
        conn.query_row(
            &format!("SELECT COUNT(*) FROM {table} WHERE source = 'cursor' AND session_id = ?"),
            [session_id],
            |row| row.get(0),
        )
        .unwrap()
    }

    /// Group A. An unreadable transcript must abort the read, not index the
    /// session as empty after its rows have already been deleted.
    ///
    /// Positive control: with `read_to_string(...).unwrap_or_default()` this
    /// returned `Ok` with an empty outcome, and the assertion below failed
    /// with `expected an error, indexed 0 records instead`.
    #[test]
    fn an_unreadable_cursor_transcript_fails_the_read_instead_of_indexing_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();

        // A transcript that vanished between the scan and the read.
        let missing = dir.path().join("gone/gone.jsonl");
        let error =
            super::ingest_cursor_transcript(&conn, &missing, "s-gone", None, 1, 0, u64::MAX)
                .err()
                .unwrap_or_else(|| panic!("expected an error for a vanished transcript"));
        assert!(
            format!("{error:#}").contains("read Cursor transcript"),
            "the failure must name the transcript it could not read: {error:#}"
        );

        // A transcript that is not UTF-8 — Cursor writes UTF-8, so this is a
        // corrupt or truncated multi-byte write, not an empty session.
        let binary = dir.path().join("bin/bin.jsonl");
        fs::create_dir_all(binary.parent().unwrap()).unwrap();
        fs::write(&binary, [0x7b, 0xff, 0xfe, 0x0a]).unwrap();
        let error = super::ingest_cursor_transcript(&conn, &binary, "s-bin", None, 1, 0, u64::MAX)
            .err()
            .unwrap_or_else(|| panic!("expected an error for a non-UTF-8 transcript"));
        assert!(
            format!("{error:#}").contains("read Cursor transcript"),
            "{error:#}"
        );
    }

    /// Group A, at the sync boundary: the failure must roll the transaction
    /// back, so the deleted rows return and the byte checkpoint does not move
    /// past content nobody read.
    ///
    /// Positive control: before the fix this test failed at the first
    /// assertion with `left: 0, right: 1` — the session's history had been
    /// deleted, the empty read committed, and the checkpoint advanced.
    #[test]
    fn a_failed_cursor_read_rolls_back_the_delete_and_the_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let transcript = write_cursor_transcript(
            dir.path(),
            "s-fail",
            concat!(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:37 PM (UTC-4)</timestamp><user_query>seed</user_query>"}]}}"#,
                "\n"
            ),
        );
        let root = dir.path().join(".cursor/projects");
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        assert_eq!(super::sync_cursor(&conn, &mut state, &root).unwrap(), 1);
        let committed = state.clone();
        assert_eq!(cursor_row_count(&conn, "history", "s-fail"), 1);

        // Rewrite the file so the next sync restarts it, and add a second
        // transcript that sorts after it. The scan hook fires per transcript
        // *before* that transcript is opened, so deleting the first one while
        // the second is being prepared leaves the first already scanned and
        // checkpointed but unreadable when the write phase indexes it — the
        // exact window this defect lived in.
        let later = write_cursor_transcript(
            dir.path(),
            "s-later",
            concat!(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:45 PM (UTC-4)</timestamp><user_query>later</user_query>"}]}}"#,
                "\n"
            ),
        );
        fs::write(
            &transcript,
            concat!(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:41 PM (UTC-4)</timestamp><user_query>second</user_query>"}]}}"#,
                "\n"
            ),
        )
        .unwrap();
        let mut next = state.clone();
        let error = super::sync_cursor_with_scan_hook(&conn, &mut next, &root, &mut |path| {
            if path == later {
                fs::remove_file(&transcript).unwrap();
            }
        })
        .expect_err("an unreadable transcript must fail the Cursor sync");
        assert!(
            format!("{error:#}").contains("index Cursor transcript"),
            "{error:#}"
        );

        // The seeded prompt survives: the delete was inside the transaction
        // that rolled back.
        assert_eq!(
            cursor_row_count(&conn, "history", "s-fail"),
            1,
            "a failed read must not leave the session's history deleted"
        );
        // And the caller's state map is untouched, so the next sync retries.
        assert_eq!(state, committed);
    }

    /// Group B. A rewritten transcript reuses byte offsets, so the rows an
    /// earlier generation wrote past the new end of the file must go.
    ///
    /// Positive control: with only `history` cleared this failed with
    /// `stale tool calls survived the rewrite: left: 3, right: 1` — the two
    /// tool calls from the longer first generation were still attributed to
    /// the session.
    #[test]
    fn rewriting_a_cursor_transcript_clears_the_evidence_keyed_on_the_old_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let long = concat!(
            r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:37 PM (UTC-4)</timestamp><user_query>first</user_query>"}]}}"#,
            "\n",
            r#"{"role":"assistant","message":{"content":[{"type":"tool_use","name":"Write","input":{"path":"a.rs"}}]}}"#,
            "\n",
            r#"{"role":"assistant","message":{"content":[{"type":"tool_use","name":"Write","input":{"path":"b.rs"}}]}}"#,
            "\n"
        );
        let transcript = write_cursor_transcript(dir.path(), "s-rewrite", long);
        let root = dir.path().join(".cursor/projects");
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        super::sync_cursor(&conn, &mut state, &root).unwrap();
        assert_eq!(cursor_row_count(&conn, "tool_calls", "s-rewrite"), 2);
        assert_eq!(cursor_row_count(&conn, "file_edits", "s-rewrite"), 2);

        // Cursor rewrites the transcript shorter. The old offsets now name
        // records that are not in the file.
        fs::write(
            &transcript,
            concat!(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:41 PM (UTC-4)</timestamp><user_query>only</user_query>"}]}}"#,
                "\n",
                r#"{"role":"assistant","message":{"content":[{"type":"tool_use","name":"Write","input":{"path":"c.rs"}}]}}"#,
                "\n"
            ),
        )
        .unwrap();
        super::sync_cursor(&conn, &mut state, &root).unwrap();

        assert_eq!(
            cursor_row_count(&conn, "tool_calls", "s-rewrite"),
            1,
            "stale tool calls survived the rewrite"
        );
        let edits = crate::session_file_edits(&conn, "s-rewrite", Some("cursor")).unwrap();
        assert_eq!(
            edits
                .iter()
                .map(|e| e.file_path.as_str())
                .collect::<Vec<_>>(),
            vec!["c.rs"],
            "the session kept file edits it never made"
        );
        let events = crate::session_events(&conn, "s-rewrite", Some("cursor")).unwrap();
        assert_eq!(events.len(), 2, "stale events survived the rewrite");
    }

    /// An empty rewrite is still a rewrite. `open` notices the file is shorter
    /// than the saved cursor and starts at offset 0; both offsets are then
    /// zero, so `advanced` used to stay false, the checkpoint advanced, and
    /// the old tool calls stayed.
    ///
    /// Positive control: without queuing a restart from a previously advanced
    /// cursor this failed with `stale tool calls survived the empty rewrite:
    /// left: 1, right: 0`.
    #[test]
    fn rewriting_a_cursor_transcript_empty_clears_the_old_evidence() {
        let dir = tempfile::tempdir().unwrap();
        let transcript = write_cursor_transcript(
            dir.path(),
            "s-empty",
            concat!(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"<user_query>do it</user_query>"}]}}"#,
                "\n",
                r#"{"role":"assistant","message":{"content":[{"type":"tool_use","name":"Write","input":{"path":"a.rs"}}]}}"#,
                "\n"
            ),
        );
        let root = dir.path().join(".cursor/projects");
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        super::sync_cursor(&conn, &mut state, &root).unwrap();
        assert_eq!(cursor_row_count(&conn, "tool_calls", "s-empty"), 1);
        assert_eq!(cursor_row_count(&conn, "history", "s-empty"), 1);

        fs::write(&transcript, "").unwrap();
        super::sync_cursor(&conn, &mut state, &root).unwrap();

        assert_eq!(
            cursor_row_count(&conn, "tool_calls", "s-empty"),
            0,
            "stale tool calls survived the empty rewrite"
        );
        assert_eq!(
            cursor_row_count(&conn, "history", "s-empty"),
            0,
            "stale prompts survived the empty rewrite"
        );
        assert_eq!(cursor_row_count(&conn, "session_events", "s-empty"), 0);
        assert_eq!(cursor_row_count(&conn, "file_edits", "s-empty"), 0);
    }

    /// Hydration must checkpoint the bytes it indexed, not a later scan of
    /// the live file. An append between those two reads would otherwise be
    /// covered by the cursor and skipped by the next sync.
    #[test]
    fn a_cursor_hydrate_checkpoint_stops_at_the_bytes_hydration_indexed() {
        let dir = tempfile::tempdir().unwrap();
        let first = concat!(
            r#"{"role":"user","message":{"content":[{"type":"text","text":"<user_query>one</user_query>"}]}}"#,
            "\n"
        );
        let second = concat!(
            r#"{"role":"user","message":{"content":[{"type":"text","text":"<user_query>two</user_query>"}]}}"#,
            "\n"
        );
        let transcript =
            write_cursor_transcript(dir.path(), "s-bound", &format!("{first}{second}"));
        let db = dir.path().join("history.db");
        super::record_cursor_hydrate_checkpoint(&db, &transcript, first.len() as u64).unwrap();
        let state = super::load_sync_state(&dir.path().join(".sync-state.json")).unwrap();
        let cursor = &state[super::CURSOR_SYNC_STATE_KEY][transcript.to_string_lossy().as_ref()];
        assert_eq!(
            saved_cursor_offset(cursor),
            first.len() as u64,
            "an unindexed append must not be covered by the hydration checkpoint"
        );
    }

    /// Group B, targeted hydration: the same rebuild guarantee.
    ///
    /// Positive control: before the fix this failed with
    /// `left: 3, right: 1` on the tool_calls count.
    #[test]
    fn re_hydrating_a_rewritten_cursor_transcript_clears_the_old_evidence() {
        let dir = tempfile::tempdir().unwrap();
        let transcript = write_cursor_transcript(
            dir.path(),
            "s-rehydrate",
            concat!(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:37 PM (UTC-4)</timestamp><user_query>first</user_query>"}]}}"#,
                "\n",
                r#"{"role":"assistant","message":{"content":[{"type":"tool_use","name":"Write","input":{"path":"a.rs"}}]}}"#,
                "\n",
                r#"{"role":"assistant","message":{"content":[{"type":"tool_use","name":"Write","input":{"path":"b.rs"}}]}}"#,
                "\n"
            ),
        );
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        super::ingest_cursor_transcript(&conn, &transcript, "s-rehydrate", None, 1, 0, u64::MAX)
            .unwrap();
        assert_eq!(cursor_row_count(&conn, "tool_calls", "s-rehydrate"), 2);

        fs::write(
            &transcript,
            concat!(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:41 PM (UTC-4)</timestamp><user_query>only</user_query>"}]}}"#,
                "\n",
                r#"{"role":"assistant","message":{"content":[{"type":"tool_use","name":"Write","input":{"path":"c.rs"}}]}}"#,
                "\n"
            ),
        )
        .unwrap();
        // What targeted hydration does around the parser.
        super::clear_cursor_session_evidence(&conn, "s-rehydrate").unwrap();
        super::ingest_cursor_transcript(&conn, &transcript, "s-rehydrate", None, 1, 0, u64::MAX)
            .unwrap();

        assert_eq!(cursor_row_count(&conn, "tool_calls", "s-rehydrate"), 1);
        assert_eq!(cursor_row_count(&conn, "file_edits", "s-rehydrate"), 1);
        assert_eq!(cursor_row_count(&conn, "history", "s-rehydrate"), 1);
    }

    /// Group C. An untimed human turn opens a new turn with no time; it must
    /// not inherit the previous turn's.
    ///
    /// Positive control: with `if let Some(ts) = record_ts { turn_ts = ... }`
    /// this failed at `used_mtime_fallback` (`false`, expected `true`) and the
    /// second prompt was dated 1789587420000 — the first turn's time — so
    /// `CURSOR_TIMESTAMP_FROM_MTIME` was never reported for a transcript that
    /// plainly needed it.
    #[test]
    fn an_untimed_cursor_turn_does_not_inherit_the_previous_turns_time() {
        let dir = tempfile::tempdir().unwrap();
        let transcript = write_cursor_transcript(
            dir.path(),
            "s-untimed",
            concat!(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:37 PM (UTC-4)</timestamp><user_query>timed turn</user_query>"}]}}"#,
                "\n",
                r#"{"role":"assistant","message":{"content":[{"type":"text","text":"answering the timed turn"}]}}"#,
                "\n",
                r#"{"role":"user","message":{"content":[{"type":"text","text":"untimed turn"}]}}"#,
                "\n",
                r#"{"role":"assistant","message":{"content":[{"type":"text","text":"answering the untimed turn"}]}}"#,
                "\n"
            ),
        );
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let outcome = super::ingest_cursor_transcript(
            &conn,
            &transcript,
            "s-untimed",
            None,
            7_777,
            0,
            u64::MAX,
        )
        .unwrap();

        assert!(
            outcome.used_mtime_fallback,
            "an undated turn must set the mtime fallback so the diagnostic fires"
        );
        let events = crate::session_events(&conn, "s-untimed", Some("cursor")).unwrap();
        let by_text: Vec<(i64, &str)> = events
            .iter()
            .map(|event| (event.ts_ms, event.text.as_deref().unwrap_or("")))
            .collect();
        assert!(
            by_text.contains(&(7_777, "untimed turn")),
            "the undated prompt must take the mtime, not the earlier turn's time: {by_text:?}"
        );
        assert!(
            by_text.contains(&(7_777, "answering the untimed turn")),
            "the reply to an undated turn inherits the undated turn: {by_text:?}"
        );
        assert!(
            by_text.contains(&(1_789_587_420_000, "answering the timed turn")),
            "a reply still inherits a turn that does have a time: {by_text:?}"
        );
        // The window covers every event this pass stamped, including the
        // undated turn at the mtime — the catalog row must not claim a
        // recency its own events contradict. Here the fixture's mtime is
        // deliberately tiny, so it widens the *start* of the window.
        assert_eq!(outcome.first_ts_ms, Some(7_777));
        assert_eq!(outcome.last_ts_ms, Some(1_789_587_420_000));
    }

    /// Group G. An append moves the file mtime, and the mtime is the fallback
    /// stamp for records with no recorded time. Re-deriving it for records an
    /// earlier pass already stored silently redates them.
    ///
    /// The divergence is the tell: `history` rows before the resumed offset
    /// are deliberately not rewritten, so a redated event ends up disagreeing
    /// with the prompt of its own turn.
    ///
    /// Positive control: re-deriving the mtime for every untimed record, this
    /// failed with `an already-stored untimed event must keep its timestamp:
    /// left: 9999, right: 4242` — the first turn's event had been dragged
    /// forward to the new mtime while its history row stayed at the old one.
    #[test]
    fn appending_to_a_cursor_transcript_does_not_redate_events_already_stored() {
        let dir = tempfile::tempdir().unwrap();
        let first = concat!(
            r#"{"role":"user","message":{"content":[{"type":"text","text":"first untimed"}]}}"#,
            "\n"
        );
        let transcript = write_cursor_transcript(dir.path(), "s-redate", first);
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        super::ingest_cursor_transcript(&conn, &transcript, "s-redate", None, 4_242, 0, u64::MAX)
            .unwrap();
        let stored: i64 = conn
            .query_row(
                "SELECT ts_ms FROM session_events WHERE source = 'cursor' AND session_id = 's-redate'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored, 4_242);

        // Cursor appends a second untimed turn; the file mtime moves with it.
        let resumed = first.len() as u64;
        fs::write(
            &transcript,
            format!(
                "{first}{}",
                concat!(
                    r#"{"role":"user","message":{"content":[{"type":"text","text":"second untimed"}]}}"#,
                    "\n"
                )
            ),
        )
        .unwrap();
        super::ingest_cursor_transcript(
            &conn,
            &transcript,
            "s-redate",
            None,
            9_999,
            resumed,
            u64::MAX,
        )
        .unwrap();

        let by_text: Vec<(String, i64)> = conn
            .prepare(
                "SELECT text, ts_ms FROM session_events \
                 WHERE source = 'cursor' AND session_id = 's-redate' ORDER BY id",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            by_text[0],
            ("first untimed".to_string(), 4_242),
            "an already-stored untimed event must keep its timestamp"
        );
        assert_eq!(
            by_text[1],
            ("second untimed".to_string(), 9_999),
            "a newly read untimed record takes the current mtime"
        );
        // And the event agrees with the prompt of its own turn, which the
        // incremental read deliberately leaves alone.
        let prompt_ts: i64 = conn
            .query_row(
                "SELECT timestamp_ms FROM history WHERE source = 'cursor' AND prompt = 'first untimed'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(prompt_ts, by_text[0].1);
    }

    /// Group J. Cursor can replace a transcript *during* the byte scan. The
    /// reader detects that and resets the checkpoint to offset 0 of the new
    /// file, but `restarted` and `resumed_from` were computed from the opening
    /// position and still describe the file that is gone. The run then commits
    /// a mixture of two generations: evidence keyed on the old file's offsets
    /// is not cleared, because `restarted` is false, and every prompt in the
    /// new file before the old resume point is skipped, because
    /// `history_from_offset` still points into the old one.
    ///
    /// Positive control: before the fix this failed at the first assertion
    /// with `stale evidence from the replaced generation survived: left: 2,
    /// right: 1` — the replaced transcript's tool call was still attributed to
    /// the session, and `"replacement first turn"` was missing from history
    /// because it sits before the old offset.
    #[test]
    fn a_cursor_transcript_replaced_during_the_scan_is_treated_as_a_rewrite() {
        let dir = tempfile::tempdir().unwrap();
        // A first generation long enough that its resume offset lands well
        // inside the replacement, so a skipped prefix is observable.
        let original = concat!(
            r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:37 PM (UTC-4)</timestamp><user_query>original first</user_query>"}]}}"#,
            "\n",
            r#"{"role":"assistant","message":{"content":[{"type":"tool_use","name":"Write","input":{"path":"original.rs"}}]}}"#,
            "\n",
            r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:39 PM (UTC-4)</timestamp><user_query>original second</user_query>"}]}}"#,
            "\n"
        );
        let transcript = write_cursor_transcript(dir.path(), "s-replaced", original);
        let root = dir.path().join(".cursor/projects");
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        super::sync_cursor(&conn, &mut state, &root).unwrap();
        assert_eq!(cursor_row_count(&conn, "tool_calls", "s-replaced"), 1);
        assert_eq!(cursor_row_count(&conn, "history", "s-replaced"), 2);

        // Grow it so the next sync has something to resume for, then replace
        // the whole file while that scan is in flight. The hook fires per
        // transcript before that transcript is opened, so a second transcript
        // gives us a point after the first has been opened and identified.
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .unwrap();
        file.write_all(
            concat!(
                r#"{"role":"assistant","message":{"content":[{"type":"text","text":"more original"}]}}"#,
                "\n"
            )
            .as_bytes(),
        )
        .unwrap();
        drop(file);

        let replacement = concat!(
            r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 4:01 PM (UTC-4)</timestamp><user_query>replacement first turn</user_query>"}]}}"#,
            "\n",
            r#"{"role":"assistant","message":{"content":[{"type":"tool_use","name":"Write","input":{"path":"replacement.rs"}}]}}"#,
            "\n"
        );
        let later = write_cursor_transcript(
            dir.path(),
            "s-zlater",
            concat!(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 4:05 PM (UTC-4)</timestamp><user_query>unrelated</user_query>"}]}}"#,
                "\n"
            ),
        );
        super::sync_cursor_with_scan_hook(&conn, &mut state, &root, &mut |path| {
            if path == later {
                // Replace, not truncate-in-place: a fresh inode is what a
                // real Cursor rewrite produces.
                fs::remove_file(&transcript).unwrap();
                fs::write(&transcript, replacement).unwrap();
            }
        })
        .unwrap();

        // Nothing from the replaced generation may survive.
        let edits = crate::session_file_edits(&conn, "s-replaced", Some("cursor")).unwrap();
        assert!(
            !edits.iter().any(|edit| edit.file_path == "original.rs"),
            "stale evidence from the replaced generation survived: {:?}",
            edits
                .iter()
                .map(|e| e.file_path.as_str())
                .collect::<Vec<_>>()
        );
        let prompts: Vec<String> = conn
            .prepare(
                "SELECT prompt FROM history WHERE source = 'cursor' AND session_id = 's-replaced' \
                 ORDER BY id",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert!(
            prompts.contains(&"replacement first turn".to_string()),
            "a prompt before the old resume offset must not be skipped: {prompts:?}"
        );
        assert!(
            !prompts.iter().any(|p| p.starts_with("original")),
            "prompts from the replaced generation must not survive: {prompts:?}"
        );
    }

    /// Group N. A transcript that cannot be *read* is an error, not a rebuild.
    ///
    /// `cursor_generation` folded every failure into `None`, and `None` means
    /// "different generation" — so a permission or I/O failure was laundered
    /// into a silent full rebuild, repeated on every sync, with nothing ever
    /// reported. That is the same confident-default shape as the read failure
    /// the earlier `a_failed_cursor_read_rolls_back_the_delete_and_the_
    /// checkpoint` test exists to prevent, arriving by a different door.
    ///
    /// The fixture replaces the transcript with a *directory*: `metadata`
    /// still succeeds, so this reaches the read rather than the stat, and it
    /// needs no non-root permission trick.
    ///
    /// Positive control, and worth stating precisely because my first
    /// attempt at this test was worthless. With `cursor_generation` returning
    /// `Option`, the sync *did* already fail here — but by a different route:
    /// the unreadable file was classified as a replacement, the rebuild was
    /// attempted, and the rescan then failed. So "the sync returns Err" is
    /// true both before and after and proves nothing. What changed is the
    /// claim the failure makes. Unfixed, the chain read `re-scan replaced
    /// Cursor transcript …: read Cursor transcript …: Is a directory` —
    /// asserting a replacement it never established, after clearing evidence
    /// for it. Fixed, it reads `identify Cursor transcript …: Is a directory`
    /// and never reaches the rebuild. The third assertion below is the one
    /// that is red.
    #[test]
    fn an_unreadable_cursor_generation_fails_the_sync_instead_of_rebuilding() {
        let dir = tempfile::tempdir().unwrap();
        let body = concat!(
            r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:37 PM (UTC-4)</timestamp><user_query>seed</user_query>"}]}}"#,
            "\n"
        );
        let transcript = write_cursor_transcript(dir.path(), "s-unreadable", body);
        let root = dir.path().join(".cursor/projects");
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        super::sync_cursor(&conn, &mut state, &root).unwrap();
        let committed = state.clone();

        // Give the next sync something to resume for.
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .unwrap();
        file.write_all(
            concat!(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:41 PM (UTC-4)</timestamp><user_query>second</user_query>"}]}}"#,
                "\n"
            )
            .as_bytes(),
        )
        .unwrap();
        drop(file);

        let later = write_cursor_transcript(
            dir.path(),
            "s-zunread",
            concat!(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"unrelated"}]}}"#,
                "\n"
            ),
        );
        let mut next = state.clone();
        let error = super::sync_cursor_with_scan_hook(&conn, &mut next, &root, &mut |path| {
            if path == later {
                fs::remove_file(&transcript).unwrap();
                fs::create_dir(&transcript).unwrap();
            }
        })
        .expect_err("an unreadable transcript must fail the sync, not silently rebuild");
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("s-unreadable"),
            "the failure must name the transcript: {rendered}"
        );
        assert!(
            rendered.contains("Is a directory"),
            "the failure must name the reason: {rendered}"
        );
        assert!(
            !rendered.contains("replaced"),
            "an unreadable file must not be reported as a replacement it never \
             established: {rendered}"
        );

        // The transaction rolled back, so the committed evidence and the
        // caller's checkpoint are untouched and the next sync retries.
        assert_eq!(cursor_row_count(&conn, "history", "s-unreadable"), 1);
        assert_eq!(next, committed);
    }

    /// Group M, at the level the defect actually lives. The window Bugbot
    /// described — a rewrite in flight as the *scan itself* ends, so the scan
    /// cannot identify what it read — is not reachable through `sync_cursor`,
    /// because the scan hook fires before a transcript is opened, not between
    /// its read loop and its identification. So the decision is tested
    /// directly.
    ///
    /// Positive control: with the guard written as
    /// `generation.is_some_and(|g| … )` this failed at `an unidentifiable
    /// generation must be treated as a replacement` — `None` read as
    /// "unchanged" and the transcript indexed with the old file's offsets.
    #[test]
    fn an_unidentifiable_cursor_generation_counts_as_a_replacement() {
        let dir = tempfile::tempdir().unwrap();
        let body = concat!(
            r#"{"role":"user","message":{"content":[{"type":"text","text":"one"}]}}"#,
            "\n"
        );
        let path = dir.path().join("t.jsonl");
        fs::write(&path, body).unwrap();
        let generation = super::cursor_generation(&path, body.len() as u64)
            .unwrap()
            .unwrap();
        let replaced = |generation: Option<&super::CursorGeneration>| {
            super::cursor_transcript_was_replaced(&path, generation).unwrap()
        };

        // The scan identified what it read and nothing moved: resume.
        assert!(!replaced(Some(&generation)));
        // The scan could not identify what it read: rebuild.
        assert!(
            replaced(None),
            "an unidentifiable generation must be treated as a replacement"
        );
        // Truncated below the scanned prefix: rebuild.
        fs::write(&path, "{}\n").unwrap();
        assert!(replaced(Some(&generation)));
        // Gone is not a replacement — there is nothing to re-scan, and the
        // read path owns that failure.
        fs::remove_file(&path).unwrap();
        assert!(!replaced(Some(&generation)));
        assert!(!replaced(None));
    }

    /// Group M. A generation that cannot be identified is not "unchanged".
    ///
    /// `cursor_generation` yields `None` when the file is shorter than the
    /// prefix the scan read — the truncate half of the rewrite Cursor does.
    /// If that happens as the scan ends, the transcript is queued with no
    /// generation at all, and a check written as "compare when we have
    /// something to compare" skips it entirely: the old `restarted` and
    /// `history_from_offset` are used against the new file, keeping stale
    /// evidence and skipping the replacement's prefix. The previous
    /// length/mtime stamp always produced *a* value, so this gap arrived with
    /// the content-identity change.
    ///
    /// Positive control: with the checks written as `is_some_and` / `if let
    /// Some` this failed at `a truncation below the scanned prefix must force
    /// a rebuild: left: false, right: true` — the session kept the replaced
    /// generation's tool call.
    #[test]
    fn a_transcript_with_no_identifiable_generation_is_treated_as_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let original = concat!(
            r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:37 PM (UTC-4)</timestamp><user_query>original</user_query>"}]}}"#,
            "\n",
            r#"{"role":"assistant","message":{"content":[{"type":"tool_use","name":"Write","input":{"path":"original.rs"}}]}}"#,
            "\n"
        );
        let transcript = write_cursor_transcript(dir.path(), "s-truncated", original);
        let root = dir.path().join(".cursor/projects");
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        super::sync_cursor(&conn, &mut state, &root).unwrap();
        assert_eq!(cursor_row_count(&conn, "tool_calls", "s-truncated"), 1);

        // Grow it so the next sync resumes, then truncate it below what that
        // scan read, from the hook of a later transcript.
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .unwrap();
        file.write_all(
            concat!(
                r#"{"role":"assistant","message":{"content":[{"type":"text","text":"more"}]}}"#,
                "\n"
            )
            .as_bytes(),
        )
        .unwrap();
        drop(file);

        let shorter = concat!(
            r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 4:01 PM (UTC-4)</timestamp><user_query>rewritten</user_query>"}]}}"#,
            "\n"
        );
        let later = write_cursor_transcript(
            dir.path(),
            "s-zlast",
            concat!(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"unrelated"}]}}"#,
                "\n"
            ),
        );
        super::sync_cursor_with_scan_hook(&conn, &mut state, &root, &mut |path| {
            if path == later {
                fs::write(&transcript, shorter).unwrap();
            }
        })
        .unwrap();

        let edits = crate::session_file_edits(&conn, "s-truncated", Some("cursor")).unwrap();
        assert!(
            !edits.iter().any(|edit| edit.file_path == "original.rs"),
            "a truncation below the scanned prefix must force a rebuild: {:?}",
            edits
                .iter()
                .map(|e| e.file_path.as_str())
                .collect::<Vec<_>>()
        );
        let prompts: Vec<String> = conn
            .prepare(
                "SELECT prompt FROM history WHERE source = 'cursor' \
                 AND session_id = 's-truncated' ORDER BY id",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            prompts,
            vec!["rewritten".to_string()],
            "history_from_offset must be reset, so the replacement's prefix is indexed"
        );
    }

    /// Group L. A session's recency must cover the events it actually has.
    ///
    /// An untimed turn is still *stamped* — with the file mtime — and still
    /// produces events at that time. But the activity window was only widened
    /// for records that carried a recorded time, so a transcript whose last
    /// turn is undated left `sessions.last_activity_ms` at the earlier timed
    /// turn. The catalog row then claims a recency older than its own newest
    /// event, and the session sorts behind siblings that are genuinely older.
    ///
    /// Positive control: with the window guarded by `record_ts.is_some()` this
    /// failed at `the session's recency must cover its newest event:
    /// left: 1789587420000, right: 1789600000000`, and the ordering assertion
    /// below put the stale session behind its older sibling.
    #[test]
    fn an_untimed_final_turn_still_advances_the_session_recency() {
        let dir = tempfile::tempdir().unwrap();
        // A dated turn, then an undated one — the undated turn is stamped with
        // the file mtime, which is later.
        let transcript = write_cursor_transcript(
            dir.path(),
            "s-untimed-end",
            concat!(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:37 PM (UTC-4)</timestamp><user_query>dated turn</user_query>"}]}}"#,
                "\n",
                r#"{"role":"user","message":{"content":[{"type":"text","text":"undated final turn"}]}}"#,
                "\n"
            ),
        );
        set_file_mtime_ms(&transcript, 1_789_600_000_000);
        // A sibling whose last activity falls between the two.
        let sibling = write_cursor_transcript(
            dir.path(),
            "s-sibling",
            concat!(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 7:00 PM (UTC-4)</timestamp><user_query>sibling turn</user_query>"}]}}"#,
                "\n"
            ),
        );
        set_file_mtime_ms(&sibling, 1_789_599_000_000);

        let root = dir.path().join(".cursor/projects");
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        super::sync_cursor(&conn, &mut state, &root).unwrap();

        let newest_event: i64 = conn
            .query_row(
                "SELECT MAX(ts_ms) FROM session_events WHERE source = 'cursor' \
                 AND session_id = 's-untimed-end'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(newest_event, 1_789_600_000_000);
        let last_activity: i64 = conn
            .query_row(
                "SELECT last_activity_ms FROM sessions WHERE source = 'cursor' \
                 AND session_id = 's-untimed-end'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            last_activity, newest_event,
            "the session's recency must cover its newest event"
        );

        // And it therefore sorts ahead of the genuinely older sibling.
        let order: Vec<String> = conn
            .prepare(
                "SELECT session_id FROM sessions WHERE source = 'cursor' \
                 ORDER BY last_activity_ms DESC",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            order,
            vec!["s-untimed-end".to_string(), "s-sibling".to_string()],
            "a session must not sort behind one whose newest event is older"
        );
    }

    /// Group L's control: a session whose last turn *is* dated keeps reporting
    /// that recorded time, not the file mtime, so the fix must widen the
    /// window to cover stamped events without letting mtime win outright.
    #[test]
    fn a_session_ending_in_a_dated_turn_keeps_its_recorded_recency() {
        let dir = tempfile::tempdir().unwrap();
        let transcript = write_cursor_transcript(
            dir.path(),
            "s-dated-end",
            concat!(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:37 PM (UTC-4)</timestamp><user_query>first</user_query>"}]}}"#,
                "\n",
                r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:41 PM (UTC-4)</timestamp><user_query>last</user_query>"}]}}"#,
                "\n"
            ),
        );
        // A much later mtime that must NOT become the session's recency,
        // because every turn here carries a recorded time.
        set_file_mtime_ms(&transcript, 1_999_999_999_000);
        let root = dir.path().join(".cursor/projects");
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        super::sync_cursor(&conn, &mut state, &root).unwrap();

        let (first, last): (i64, i64) = conn
            .query_row(
                "SELECT first_activity_ms, last_activity_ms FROM sessions \
                 WHERE source = 'cursor' AND session_id = 's-dated-end'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(first, 1_789_587_420_000);
        assert_eq!(
            last, 1_789_587_660_000,
            "a fully dated session must report its recorded times, not the mtime"
        );
    }

    /// Group K. An append *between the scan and the index* is not a rewrite.
    ///
    /// This is the case group J's own control missed: that test appends
    /// between syncs, so the file is already settled by the time the next
    /// scan runs and the index-time check compares equal. An append that
    /// lands inside the window the check guards changes length and mtime
    /// without changing any byte the scan read, and a stamp that compares
    /// those fields calls it a replacement.
    ///
    /// Positive control: with the generation stamp comparing length and mtime
    /// this failed at `an append must not restamp an untimed event:
    /// left: 9999, right: 4242` — the append triggered a full rebuild, which
    /// cleared the session and re-indexed the whole file, restamping the
    /// untimed turn with the new mtime and indexing the appended record past
    /// the `index_through` bound that exists to leave it for the next sync.
    #[test]
    fn an_append_between_the_scan_and_the_index_is_not_a_rewrite() {
        let dir = tempfile::tempdir().unwrap();
        // Untimed, so its stored timestamp is the mtime and a rebuild is
        // visible as a restamp.
        let seed = concat!(
            r#"{"role":"user","message":{"content":[{"type":"text","text":"seed turn"}]}}"#,
            "\n"
        );
        let transcript = write_cursor_transcript(dir.path(), "s-lateappend", seed);
        set_file_mtime_ms(&transcript, 4_242);
        let root = dir.path().join(".cursor/projects");
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        super::sync_cursor(&conn, &mut state, &root).unwrap();
        assert_eq!(cursor_row_count(&conn, "history", "s-lateappend"), 1);

        // Give the next sync something to resume for, then append again from
        // the scan hook of a later transcript — i.e. after this file has been
        // scanned and checkpointed, before the write phase reads it.
        let second = concat!(
            r#"{"role":"user","message":{"content":[{"type":"text","text":"second turn"}]}}"#,
            "\n"
        );
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .unwrap();
        file.write_all(second.as_bytes()).unwrap();
        drop(file);

        let later = write_cursor_transcript(
            dir.path(),
            "s-zlate",
            concat!(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"unrelated"}]}}"#,
                "\n"
            ),
        );
        super::sync_cursor_with_scan_hook(&conn, &mut state, &root, &mut |path| {
            if path == later {
                let mut file = fs::OpenOptions::new()
                    .append(true)
                    .open(&transcript)
                    .unwrap();
                file.write_all(
                    concat!(
                        r#"{"role":"user","message":{"content":[{"type":"text","text":"raced append"}]}}"#,
                        "\n"
                    )
                    .as_bytes(),
                )
                .unwrap();
                drop(file);
                set_file_mtime_ms(&transcript, 9_999);
            }
        })
        .unwrap();

        // An append is not a rewrite, so the already-stored untimed event
        // keeps the stamp it was written with.
        let seed_ts: i64 = conn
            .query_row(
                "SELECT ts_ms FROM session_events WHERE source = 'cursor' \
                 AND session_id = 's-lateappend' AND text = 'seed turn'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            seed_ts, 4_242,
            "an append must not restamp an untimed event"
        );
        // And the raced append stays behind the checkpoint for the next sync,
        // exactly as the `index_through` bound intends.
        let prompts: Vec<String> = conn
            .prepare(
                "SELECT prompt FROM history WHERE source = 'cursor' \
                 AND session_id = 's-lateappend' ORDER BY id",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            prompts,
            vec!["seed turn".to_string(), "second turn".to_string()],
            "the raced append belongs to the next sync"
        );
    }

    /// The rule both halves of group K turn on, at the unit level: a
    /// generation survives an append and does not survive a rewrite.
    #[test]
    fn a_cursor_generation_survives_an_append_but_not_a_rewrite() {
        let dir = tempfile::tempdir().unwrap();
        let body = concat!(
            r#"{"role":"user","message":{"content":[{"type":"text","text":"one"}]}}"#,
            "\n"
        );
        let path = dir.path().join("t.jsonl");
        fs::write(&path, body).unwrap();
        let generation = super::cursor_generation(&path, body.len() as u64)
            .unwrap()
            .unwrap();
        let intact = |path: &Path| super::cursor_generation_intact(path, &generation).unwrap();
        assert!(intact(&path));

        // Appending leaves every scanned byte untouched.
        let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(
            concat!(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"two"}]}}"#,
                "\n"
            )
            .as_bytes(),
        )
        .unwrap();
        drop(file);
        assert!(intact(&path), "an append must leave the generation intact");

        // A rewrite that keeps the length but changes a scanned byte does not.
        let rewritten = body.replace("one", "ONE");
        assert_eq!(rewritten.len(), body.len());
        fs::write(&path, &rewritten).unwrap();
        assert!(
            !intact(&path),
            "an in-place rewrite must not pass as the same generation"
        );

        // Nor does a truncation below what the scan read.
        fs::write(&path, "{}\n").unwrap();
        assert!(!intact(&path));

        // Nor does an unlink-and-recreate carrying different content. The
        // inode may or may not be reused — that is the filesystem's choice,
        // and the check must not depend on it — but the scanned prefix
        // differs either way.
        fs::remove_file(&path).unwrap();
        fs::write(
            &path,
            concat!(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"different"}]}}"#,
                "\n"
            ),
        )
        .unwrap();
        assert!(!intact(&path));
    }

    /// Group J's other half: an ordinary append, with no replacement, must
    /// still resume from the checkpoint rather than rebuilding from zero.
    /// The fix must key on a detected replacement, not on "the file changed".
    #[test]
    fn a_cursor_append_without_a_replacement_still_resumes_from_the_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let first = concat!(
            r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:37 PM (UTC-4)</timestamp><user_query>first</user_query>"}]}}"#,
            "\n"
        );
        let transcript = write_cursor_transcript(dir.path(), "s-plainappend", first);
        let root = dir.path().join(".cursor/projects");
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        super::sync_cursor(&conn, &mut state, &root).unwrap();
        let resumed_from = saved_cursor_offset(
            &state[super::CURSOR_SYNC_STATE_KEY][transcript.to_string_lossy().as_ref()],
        );
        assert_eq!(resumed_from, first.len() as u64);

        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .unwrap();
        file.write_all(
            concat!(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:41 PM (UTC-4)</timestamp><user_query>second</user_query>"}]}}"#,
                "\n"
            )
            .as_bytes(),
        )
        .unwrap();
        drop(file);
        // One new prompt inserted, not a rebuild of both.
        assert_eq!(super::sync_cursor(&conn, &mut state, &root).unwrap(), 1);
        let prompts: Vec<String> = conn
            .prepare(
                "SELECT prompt FROM history WHERE source = 'cursor' AND session_id = 's-plainappend' \
                 ORDER BY id",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(prompts, vec!["first".to_string(), "second".to_string()]);
        assert!(
            saved_cursor_offset(
                &state[super::CURSOR_SYNC_STATE_KEY][transcript.to_string_lossy().as_ref()]
            ) > resumed_from,
            "an append must advance the checkpoint, not reset it"
        );
    }

    /// Group H. Cursor can append between the byte scan and the read. Those
    /// bytes are past the checkpoint this run commits, so indexing them
    /// publishes evidence the next sync reads again — and an untimed prompt
    /// re-read after the mtime moved duplicates rather than upserting.
    ///
    /// Positive control: without the `index_through` bound this failed with
    /// `the post-scan append must not be indexed twice: left: 3, right: 2` —
    /// the appended prompt was indexed once past the checkpoint and once more
    /// on the next sync, under a new mtime.
    #[test]
    fn a_cursor_append_after_the_scan_is_not_indexed_past_the_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let seed = concat!(
            r#"{"role":"user","message":{"content":[{"type":"text","text":"seed turn"}]}}"#,
            "\n"
        );
        let transcript = write_cursor_transcript(dir.path(), "s-append", seed);
        let root = dir.path().join(".cursor/projects");
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();

        let appended = concat!(
            r#"{"role":"user","message":{"content":[{"type":"text","text":"raced turn"}]}}"#,
            "\n"
        );
        // The hook fires per transcript *before* that transcript is scanned,
        // so appending to the first one while the second is being prepared
        // lands exactly in the window this guards: after the first file's scan
        // took its checkpoint, before the write phase reads the whole file.
        let later = write_cursor_transcript(
            dir.path(),
            "s-later",
            concat!(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"unrelated"}]}}"#,
                "\n"
            ),
        );
        super::sync_cursor_with_scan_hook(&conn, &mut state, &root, &mut |path| {
            if path == later {
                let mut file = fs::OpenOptions::new()
                    .append(true)
                    .open(&transcript)
                    .unwrap();
                file.write_all(appended.as_bytes()).unwrap();
            }
        })
        .unwrap();

        // Whatever the first pass indexed, the second must not double it. The
        // raced turn is untimed, so re-reading it after the mtime moved
        // inserts a second row rather than upserting the first.
        super::sync_cursor(&conn, &mut state, &root).unwrap();
        let prompts: Vec<String> = conn
            .prepare(
                "SELECT prompt FROM history WHERE source = 'cursor' AND session_id = 's-append' \
                 ORDER BY id",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            prompts.len(),
            2,
            "the post-scan append must not be indexed twice: {prompts:?}"
        );
        assert_eq!(
            prompts,
            vec!["seed turn".to_string(), "raced turn".to_string()]
        );
    }

    /// Group I. A rebuild has re-read the whole source, so it owns
    /// `last_assistant_text` too: if the reply is gone from the transcript,
    /// the catalog must stop quoting it.
    ///
    /// Positive control: with the `COALESCE(excluded, sessions)` merge this
    /// failed with `a rebuild must clear a reply the transcript no longer has:
    /// left: Some("the old reply"), right: None`.
    #[test]
    fn a_rebuild_clears_an_assistant_reply_the_transcript_no_longer_has() {
        let dir = tempfile::tempdir().unwrap();
        let with_reply = concat!(
            r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:37 PM (UTC-4)</timestamp><user_query>ask</user_query>"}]}}"#,
            "\n",
            r#"{"role":"assistant","message":{"content":[{"type":"text","text":"the old reply"}]}}"#,
            "\n"
        );
        let transcript = write_cursor_transcript(dir.path(), "s-reply", with_reply);
        let root = dir.path().join(".cursor/projects");
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        super::sync_cursor(&conn, &mut state, &root).unwrap();
        let quoted: Option<String> = conn
            .query_row(
                "SELECT last_assistant_text FROM sessions \
                 WHERE source = 'cursor' AND session_id = 's-reply'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(quoted.as_deref(), Some("the old reply"));

        // Cursor rewrites the transcript without the reply.
        fs::write(
            &transcript,
            concat!(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:41 PM (UTC-4)</timestamp><user_query>ask again</user_query>"}]}}"#,
                "\n"
            ),
        )
        .unwrap();
        super::sync_cursor(&conn, &mut state, &root).unwrap();

        let quoted: Option<String> = conn
            .query_row(
                "SELECT last_assistant_text FROM sessions \
                 WHERE source = 'cursor' AND session_id = 's-reply'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            quoted, None,
            "a rebuild must clear a reply the transcript no longer has"
        );
    }

    /// Group D. Only newline-terminated records may be indexed, matching
    /// `CompleteJsonlReader` and `complete_jsonl_records`.
    ///
    /// The interesting case is a final record that is *already valid JSON* but
    /// whose newline has not landed yet — a truncated one is skipped anyway by
    /// the parse guard, so it proves nothing. The byte checkpoint does not
    /// consider an unterminated record consumed, and `records_parsed` does not
    /// count it; a parser that indexes it publishes evidence at an offset the
    /// checkpoint has not covered, and the row then has to be reconciled
    /// against whatever Cursor actually appends after it.
    ///
    /// Positive control: without the `ends_with('\n')` filter this failed at
    /// the first assertion with `left: 2, right: 1` — the unterminated record
    /// was indexed as if it were complete.
    #[test]
    fn a_cursor_record_without_its_newline_is_not_indexed_until_it_is_complete() {
        let dir = tempfile::tempdir().unwrap();
        let complete = concat!(
            r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:37 PM (UTC-4)</timestamp><user_query>done</user_query>"}]}}"#,
            "\n"
        );
        // Valid JSON, no trailing newline: Cursor has flushed the object but
        // not yet terminated the line.
        let unterminated = r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:41 PM (UTC-4)</timestamp><user_query>still writing</user_query>"}]}}"#;
        let transcript = write_cursor_transcript(
            dir.path(),
            "s-partial",
            &format!("{complete}{unterminated}"),
        );
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let outcome =
            super::ingest_cursor_transcript(&conn, &transcript, "s-partial", None, 1, 0, u64::MAX)
                .unwrap();
        assert_eq!(
            cursor_row_count(&conn, "history", "s-partial"),
            1,
            "a record whose newline has not landed must not be published"
        );
        // The parser agrees with the reader that only one record exists.
        assert_eq!(outcome.records, 1);

        // Cursor terminates the line. Now it is indexed, exactly once, at the
        // byte offset the checkpoint covers.
        fs::write(&transcript, format!("{complete}{unterminated}\n")).unwrap();
        super::ingest_cursor_transcript(&conn, &transcript, "s-partial", None, 1, 0, u64::MAX)
            .unwrap();
        let prompts: Vec<String> = conn
            .prepare("SELECT prompt FROM history WHERE source = 'cursor' ORDER BY timestamp_ms")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            prompts,
            vec!["done".to_string(), "still writing".to_string()]
        );
    }

    /// Group E. A full rebuild owns both ends of the activity window, so an
    /// endpoint an earlier parser took from the file mtime must be replaced.
    ///
    /// Positive control: through `upsert_session`'s MAX() merge this failed
    /// with `left: 7000000000000, right: 1789587660000` — the mtime endpoint
    /// the prompt-only parser had written outlived the rebuild, because MAX()
    /// can never retract it.
    #[test]
    fn a_full_cursor_rebuild_replaces_a_stale_mtime_activity_endpoint() {
        let dir = tempfile::tempdir().unwrap();
        stage_cursor_fixture(dir.path(), "observed-3.13.25.jsonl", "s-window");
        let root = dir.path().join(".cursor/projects");
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        // What the prompt-only parser left: both endpoints at the file mtime.
        super::upsert_session(
            &conn,
            "s-window",
            "cursor",
            Some("/home/dev/demo"),
            None,
            7_000_000_000_000,
            7_000_000_000_000,
            None,
            None,
        )
        .unwrap();

        let mut state = Map::new();
        super::sync_cursor(&conn, &mut state, &root).unwrap();

        let (first, last): (i64, i64) = conn
            .query_row(
                "SELECT first_activity_ms, last_activity_ms FROM sessions \
                 WHERE source = 'cursor' AND session_id = 's-window'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(first, 1_789_587_420_000);
        assert_eq!(
            last, 1_789_587_660_000,
            "a rebuild must replace a stale mtime endpoint, not merge under it"
        );
    }

    /// Group E, the other half: an incremental read saw only the tail, so it
    /// may only widen the window.
    #[test]
    fn an_incremental_cursor_read_still_widens_rather_than_replaces_the_window() {
        let dir = tempfile::tempdir().unwrap();
        let first_turn = concat!(
            r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:37 PM (UTC-4)</timestamp><user_query>first</user_query>"}]}}"#,
            "\n"
        );
        let transcript = write_cursor_transcript(dir.path(), "s-grow", first_turn);
        let root = dir.path().join(".cursor/projects");
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        super::sync_cursor(&conn, &mut state, &root).unwrap();

        // Append a later turn; the sync resumes mid-file.
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .unwrap();
        file.write_all(
            concat!(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:41 PM (UTC-4)</timestamp><user_query>second</user_query>"}]}}"#,
                "\n"
            )
            .as_bytes(),
        )
        .unwrap();
        drop(file);
        super::sync_cursor(&conn, &mut state, &root).unwrap();

        let (first, last): (i64, i64) = conn
            .query_row(
                "SELECT first_activity_ms, last_activity_ms FROM sessions \
                 WHERE source = 'cursor' AND session_id = 's-grow'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            first, 1_789_587_420_000,
            "an incremental read must not drop the start it never re-read"
        );
        assert_eq!(last, 1_789_587_660_000);
    }

    /// Group F. One `ApplyPatch` call can rewrite several files; each needs
    /// its own `file_edits` identity and its own line counts, under one
    /// `tool_calls` row.
    ///
    /// Positive control: taking only the first `*** Update File:` header this
    /// failed with `left: ["src/a.rs"], right: ["src/a.rs", "src/b.rs",
    /// "src/c.rs"]` — two of the three files the patch wrote were lost.
    #[test]
    fn a_multi_file_apply_patch_records_one_edit_per_file() {
        let dir = tempfile::tempdir().unwrap();
        let patch = "*** Begin Patch\\n\
                     *** Update File: src/a.rs\\n\
                     @@\\n-one\\n+two\\n\
                     *** Add File: src/b.rs\\n\
                     @@\\n+alpha\\n+beta\\n+gamma\\n\
                     *** Delete File: src/c.rs\\n\
                     @@\\n-gone\\n\
                     *** End Patch";
        let transcript = write_cursor_transcript(
            dir.path(),
            "s-multi",
            &format!(
                concat!(
                    r#"{{"role":"user","message":{{"content":[{{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:37 PM (UTC-4)</timestamp><user_query>refactor</user_query>"}}]}}}}"#,
                    "\n",
                    r#"{{"role":"assistant","message":{{"content":[{{"type":"tool_use","name":"ApplyPatch","input":"{patch}"}}]}}}}"#,
                    "\n"
                ),
                patch = patch
            ),
        );
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        super::ingest_cursor_transcript(&conn, &transcript, "s-multi", None, 1, 0, u64::MAX)
            .unwrap();

        // One call, three edits.
        let calls = crate::session_tool_calls(&conn, "s-multi", Some("cursor")).unwrap();
        assert_eq!(calls.len(), 1, "a patch is one tool call");
        assert_eq!(calls[0].name, "ApplyPatch");

        let edits = crate::session_file_edits(&conn, "s-multi", Some("cursor")).unwrap();
        let mut touched: Vec<&str> = edits.iter().map(|e| e.file_path.as_str()).collect();
        touched.sort_unstable();
        assert_eq!(touched, vec!["src/a.rs", "src/b.rs", "src/c.rs"]);

        // Each file's counts come from its own slice of the patch, not from
        // the whole call.
        let counts = |path: &str| {
            let edit = edits.iter().find(|e| e.file_path == path).unwrap();
            (edit.lines_added, edit.lines_removed)
        };
        assert_eq!(counts("src/a.rs"), (Some(1), Some(1)));
        assert_eq!(counts("src/b.rs"), (Some(3), Some(0)));
        assert_eq!(counts("src/c.rs"), (Some(0), Some(1)));
        // And each stores only its own patch text.
        let b = edits.iter().find(|e| e.file_path == "src/b.rs").unwrap();
        let stored = b.structured_patch_json.as_deref().unwrap();
        assert!(stored.contains("src/b.rs"), "{stored}");
        assert!(!stored.contains("src/a.rs"), "{stored}");
    }

    #[test]
    fn an_observed_cursor_transcript_yields_events_tool_calls_and_file_edits() {
        let dir = tempfile::tempdir().unwrap();
        let path = stage_cursor_fixture(dir.path(), "observed-3.13.25.jsonl", "s-observed");
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();

        let outcome = super::ingest_cursor_transcript(
            &conn,
            &path,
            "s-observed",
            Some("/home/dev/demo"),
            7_000_000_000_000,
            0,
            u64::MAX,
        )
        .unwrap();

        // Both human turns land in history with the time the client injected,
        // never the file mtime.
        assert_eq!(outcome.prompts_inserted, 2);
        assert!(!outcome.used_mtime_fallback);
        assert_eq!(outcome.first_ts_ms, Some(1_789_587_420_000));
        assert_eq!(outcome.last_ts_ms, Some(1_789_587_660_000));
        assert_eq!(
            outcome.last_assistant_text.as_deref(),
            Some("Fixed the failing assertion.")
        );
        // Cursor 3.13.25 writes no model, so none is claimed.
        assert!(outcome.models.is_empty());

        let kinds = event_kinds(&conn, "s-observed");
        assert!(kinds.contains(&("user".into(), "text".into())));
        assert!(kinds.contains(&("assistant".into(), "text".into())));
        assert!(kinds.contains(&("assistant".into(), "tool_use".into())));
        // The observed shape carries no tool_result and no thinking; the
        // parser must not manufacture either.
        assert!(!kinds.iter().any(|(_, kind)| kind == "tool_result"));
        assert!(!kinds.iter().any(|(_, kind)| kind == "thinking"));

        let events = crate::session_events(&conn, "s-observed", Some("cursor")).unwrap();
        let stamps: HashSet<i64> = events.iter().map(|event| event.ts_ms).collect();
        assert!(
            stamps.len() > 1,
            "per-turn timestamps must differ across records: {stamps:?}"
        );
        assert!(
            !stamps.contains(&7_000_000_000_000),
            "no record with a readable turn time may take the file mtime"
        );
        // A human turn is stored as what the person typed, not as the markup
        // the client wrapped around it.
        assert!(events.iter().any(|event| event.role == "user"
            && event.text.as_deref() == Some("add a changelog entry for the cursor adapter")));

        let calls = crate::session_tool_calls(&conn, "s-observed", Some("cursor")).unwrap();
        let named: Vec<&str> = calls.iter().map(|call| call.name.as_str()).collect();
        assert_eq!(named, vec!["Read", "ApplyPatch", "Shell", "StrReplace"]);
        // Cursor writes no tool_use id, so identity comes from the record's
        // byte offset and every call still gets its own row.
        assert_eq!(
            calls
                .iter()
                .map(|call| call.tool_use_id.clone())
                .collect::<HashSet<_>>()
                .len(),
            4
        );
        assert_eq!(
            calls
                .iter()
                .find(|call| call.name == "Shell")
                .and_then(|call| call.target.clone()),
            Some("cargo test -p ai-hist".into())
        );

        let edits = crate::session_file_edits(&conn, "s-observed", Some("cursor")).unwrap();
        let touched: Vec<&str> = edits.iter().map(|edit| edit.file_path.as_str()).collect();
        assert_eq!(touched, vec!["CHANGELOG.md", "src/lib.rs"]);
        // ApplyPatch carries the diff, so its line counts are real.
        let patched = edits
            .iter()
            .find(|edit| edit.file_path == "CHANGELOG.md")
            .unwrap();
        assert_eq!(
            (patched.lines_added, patched.lines_removed),
            (Some(2), Some(0))
        );
        assert!(patched.structured_patch_json.is_some());
        // StrReplace carries no diff; counts stay at zero rather than guessed.
        let replaced = edits
            .iter()
            .find(|edit| edit.file_path == "src/lib.rs")
            .unwrap();
        assert_eq!((replaced.lines_added, replaced.lines_removed), (None, None));
        assert!(replaced.structured_patch_json.is_none());
    }

    #[test]
    fn a_cursor_build_that_writes_model_usage_and_tool_results_has_them_recorded() {
        let dir = tempfile::tempdir().unwrap();
        let path = stage_cursor_fixture(dir.path(), "extended-unverified.jsonl", "s-extended");
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();

        let outcome = super::ingest_cursor_transcript(
            &conn,
            &path,
            "s-extended",
            Some("/home/dev/demo"),
            1,
            0,
            u64::MAX,
        )
        .unwrap();
        assert_eq!(outcome.models, vec!["claude-4.5-sonnet".to_string()]);
        assert_eq!(outcome.subagent_calls, 1);

        let events = crate::session_events(&conn, "s-extended", Some("cursor")).unwrap();
        let kinds: HashSet<&str> = events.iter().map(|event| event.kind.as_str()).collect();
        assert_eq!(
            kinds,
            HashSet::from(["text", "thinking", "tool_use", "tool_result"])
        );
        let assistant = events
            .iter()
            .find(|event| event.kind == "thinking")
            .expect("thinking event");
        assert_eq!(assistant.model.as_deref(), Some("claude-4.5-sonnet"));
        assert_eq!(
            assistant.token_json.as_deref(),
            Some(r#"{"input_tokens":812,"output_tokens":77}"#)
        );

        // A build that writes tool ids links the result back to the call.
        let calls = crate::session_tool_calls(&conn, "s-extended", Some("cursor")).unwrap();
        assert!(calls.iter().any(|call| call.tool_use_id == "toolu_ext_1"));
        assert_eq!(
            calls
                .iter()
                .find(|call| call.tool_use_id == "toolu_ext_1")
                .and_then(|call| call.is_error),
            Some(0)
        );
        let edits = crate::session_file_edits(&conn, "s-extended", Some("cursor")).unwrap();
        assert_eq!(edits.len(), 1);
        assert_eq!(edits[0].file_path, "src/helper.rs");
        assert_eq!(
            (edits[0].lines_added, edits[0].lines_removed),
            (Some(1), Some(1))
        );
    }

    #[test]
    fn a_cursor_record_with_no_readable_turn_time_takes_the_mtime_and_says_so() {
        let dir = tempfile::tempdir().unwrap();
        let path = stage_cursor_fixture(dir.path(), "legacy-string-content.jsonl", "s-legacy");
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();

        let outcome = super::ingest_cursor_transcript(
            &conn,
            &path,
            "s-legacy",
            Some("/home/dev/demo"),
            4_242,
            0,
            u64::MAX,
        )
        .unwrap();
        assert!(outcome.used_mtime_fallback);
        // No turn here carries a recorded time, so the whole window is the
        // mtime the records were stamped with — the session still has events,
        // and its activity window has to cover them.
        assert_eq!(outcome.first_ts_ms, Some(4_242));
        assert_eq!(outcome.last_ts_ms, Some(4_242));
        let events = crate::session_events(&conn, "s-legacy", Some("cursor")).unwrap();
        assert_eq!(events.len(), 2);
        assert!(events.iter().all(|event| event.ts_ms == 4_242));
    }

    #[test]
    fn re_reading_a_cursor_transcript_upserts_in_place_instead_of_duplicating() {
        let dir = tempfile::tempdir().unwrap();
        let path = stage_cursor_fixture(dir.path(), "observed-3.13.25.jsonl", "s-repeat");
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        for _ in 0..2 {
            super::ingest_cursor_transcript(
                &conn,
                &path,
                "s-repeat",
                Some("/home/dev/demo"),
                1,
                0,
                u64::MAX,
            )
            .unwrap();
        }
        let counts = |table: &str| {
            conn.query_row(
                &format!("SELECT COUNT(*) FROM {table} WHERE source = 'cursor'"),
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
        };
        assert_eq!(counts("history"), 2);
        assert_eq!(counts("session_events"), 9);
        assert_eq!(counts("tool_calls"), 4);
        assert_eq!(counts("file_edits"), 2);
    }

    #[test]
    fn plain_sync_indexes_cursor_events_and_re_reads_after_the_state_key_retires() {
        let dir = tempfile::tempdir().unwrap();
        stage_cursor_fixture(dir.path(), "observed-3.13.25.jsonl", "s-sync");
        let root = dir.path().join(".cursor/projects");
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();

        let mut state = Map::new();
        assert_eq!(super::sync_cursor(&conn, &mut state, &root).unwrap(), 2);
        assert!(state.contains_key(super::CURSOR_SYNC_STATE_KEY));
        let events = crate::session_events(&conn, "s-sync", Some("cursor")).unwrap();
        assert!(
            events.iter().any(|event| event.kind == "tool_use"),
            "plain sync must index tool use, not just prompts"
        );
        assert_eq!(
            crate::session_file_edits(&conn, "s-sync", Some("cursor"))
                .unwrap()
                .len(),
            2
        );

        // An unchanged transcript is skipped while the key matches.
        assert_eq!(super::sync_cursor(&conn, &mut state, &root).unwrap(), 0);

        // A store written by the prompt-only parser carries the retired key and
        // no events. Retiring it is what makes plain `sync` re-read the file.
        let mut legacy = Map::new();
        legacy.insert("cursor".into(), state[super::CURSOR_SYNC_STATE_KEY].clone());
        let fresh = Connection::open_in_memory().unwrap();
        init_db(&fresh).unwrap();
        assert_eq!(super::sync_cursor(&fresh, &mut legacy, &root).unwrap(), 2);
        assert!(crate::session_events(&fresh, "s-sync", Some("cursor"))
            .unwrap()
            .iter()
            .any(|event| event.kind == "tool_use"));
    }

    #[test]
    fn a_restarted_cursor_transcript_rebuilds_prompts_stamped_by_the_old_parser() {
        let dir = tempfile::tempdir().unwrap();
        stage_cursor_fixture(dir.path(), "observed-3.13.25.jsonl", "s-restamp");
        let root = dir.path().join(".cursor/projects");
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        // What the prompt-only parser left behind: the right text at the file
        // mtime instead of the turn time.
        insert_history(
            &conn,
            &HistoryEntry {
                id: 0,
                source: "cursor".into(),
                session_id: Some("s-restamp".into()),
                project: Some("/home/dev/demo".into()),
                prompt: "add a changelog entry for the cursor adapter".into(),
                prompt_hash: None,
                timestamp_ms: 7_000_000_000_000,
            },
        )
        .unwrap();

        let mut state = Map::new();
        super::sync_cursor(&conn, &mut state, &root).unwrap();

        let rows: Vec<(String, i64)> = conn
            .prepare("SELECT prompt, timestamp_ms FROM history WHERE source = 'cursor' ORDER BY timestamp_ms")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            rows,
            vec![
                (
                    "add a changelog entry for the cursor adapter".to_string(),
                    1_789_587_420_000
                ),
                ("now run the tests".to_string(), 1_789_587_660_000),
            ],
            "the mtime-stamped row must be replaced, not kept alongside the real one"
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
            saved_cursor_offset(
                &state[super::CURSOR_SYNC_STATE_KEY][cursor.to_string_lossy().as_ref()]
            ),
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
            saved_cursor_offset(
                &state[super::CURSOR_SYNC_STATE_KEY][cursor.to_string_lossy().as_ref()]
            ),
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
        assert_eq!(
            super::sync_codex(&conn, &mut state, &dir.path().join(".codex")).unwrap(),
            0
        );
        assert_eq!(saved_cursor_offset(&state["codex"]), 0);
        let mut file = fs::OpenOptions::new().append(true).open(&codex).unwrap();
        file.write_all(br#" prompt","ts":1,"session_id":"c1"}"#)
            .unwrap();
        file.write_all(b"\n").unwrap();
        drop(file);
        assert_eq!(
            super::sync_codex(&conn, &mut state, &dir.path().join(".codex")).unwrap(),
            1
        );

        let cursor_root = dir.path().join(".cursor/projects");
        let cursor = cursor_root.join("P/agent-transcripts/s1/s1.jsonl");
        fs::create_dir_all(cursor.parent().unwrap()).unwrap();
        fs::write(&cursor, r#"{"role":"user","message":{"content":"cursor"#).unwrap();
        assert_eq!(
            super::sync_cursor(&conn, &mut state, &cursor_root).unwrap(),
            0
        );
        assert_eq!(
            saved_cursor_offset(
                &state[super::CURSOR_SYNC_STATE_KEY][cursor.to_string_lossy().as_ref()]
            ),
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

    /// Simulate a database that was synced by a release without the fidelity
    /// columns: the rows and the sync stamps are there, the columns the
    /// migration added are null, and the sync state carries no backfill
    /// generation because that release never wrote one. This is exactly the
    /// state an upgraded install is in on its first `sync`.
    fn blank_tool_result_fidelity_state(state: &mut Map<String, Value>) {
        state.remove(super::CLAUDE_FIDELITY_GENERATION_KEY);
        state.remove(super::CODEX_FIDELITY_GENERATION_KEY);
    }

    fn blank_tool_result_fidelity(conn: &Connection, source: &str) {
        conn.execute(
            "UPDATE session_events SET tool_use_id = NULL, payload_bytes = NULL, \
             payload_truncated = NULL, payload_hash = NULL, call_index = NULL, \
             event_index = NULL, result_status = NULL, event_source = NULL, \
             error_signal = NULL WHERE source = ?",
            [source],
        )
        .unwrap();
    }

    #[test]
    fn plain_claude_sync_backfills_fidelity_for_transcripts_indexed_before_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sess-fidelity.jsonl");
        let lines = [
            claude_line(json!({
                "type": "user", "uuid": "u1", "sessionId": "sess-fidelity",
                "cwd": "/tmp/project", "timestamp": "2026-04-20T00:00:00.000Z",
                "message": { "role": "user", "content": "run it" },
            })),
            claude_line(json!({
                "type": "assistant", "uuid": "a1", "parentUuid": "u1",
                "sessionId": "sess-fidelity", "cwd": "/tmp/project",
                "timestamp": "2026-04-20T00:00:01.000Z",
                "message": { "role": "assistant", "content": [
                    { "type": "tool_use", "id": "tu_1", "name": "Bash",
                      "input": { "command": "ls" } },
                ]},
            })),
            claude_line(json!({
                "type": "user", "uuid": "u2", "parentUuid": "a1",
                "sessionId": "sess-fidelity", "cwd": "/tmp/project",
                "timestamp": "2026-04-20T00:00:02.000Z",
                "message": { "role": "user", "content": [
                    { "type": "tool_result", "tool_use_id": "tu_1", "content": "a\nb\n" },
                ]},
            })),
        ];
        fs::write(&path, format!("{}\n", lines.join("\n"))).unwrap();

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();
        assert_eq!(
            tool_results(&conn, "claude", "sess-fidelity")[0].payload_bytes,
            Some(4)
        );

        blank_tool_result_fidelity(&conn, "claude");
        blank_tool_result_fidelity_state(&mut state);
        // The stamp is unchanged and the events exist, so every other
        // condition on the fast path says "skip". Without the fidelity check
        // this sync is a no-op and the columns stay null indefinitely while
        // the run still reports success.
        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();
        let rows = tool_results(&conn, "claude", "sess-fidelity");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].payload_bytes, Some(4));
        assert_eq!(rows[0].tool_use_id.as_deref(), Some("tu_1"));
        assert_eq!(rows[0].event_index, Some(0));
        assert_eq!(rows[0].event_source.as_deref(), Some("tool_result"));

        // Repaired once, back on the fast path: a sentinel a re-read would
        // overwrite has to survive, or the transcript is being re-read on
        // every sync forever.
        conn.execute(
            "UPDATE session_events SET text = 'sentinel' WHERE source = 'claude' AND kind = 'tool_result'",
            [],
        )
        .unwrap();
        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();
        assert_eq!(
            tool_results(&conn, "claude", "sess-fidelity")[0]
                .text
                .as_deref(),
            Some("sentinel"),
        );
    }

    #[test]
    fn plain_codex_sync_backfills_fidelity_for_rollouts_indexed_before_it() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let day = home.join(".codex/sessions/2026/04/20");
        fs::create_dir_all(&day).unwrap();
        let rollout = day.join("rollout-2026-04-20T05-00-00-sess_backfill.jsonl");
        let lines = [
            r#"{"timestamp":"2026-04-20T05:00:00.000Z","type":"session_meta","payload":{"id":"sess_backfill","cwd":"/tmp/project"}}"#,
            r#"{"timestamp":"2026-04-20T05:00:01.000Z","type":"event_msg","payload":{"type":"user_message","message":"run it"}}"#,
            r#"{"timestamp":"2026-04-20T05:00:02.000Z","type":"response_item","payload":{"type":"function_call","name":"shell","arguments":"{\"command\":\"ls\"}","call_id":"call_1"}}"#,
            r#"{"timestamp":"2026-04-20T05:00:03.000Z","type":"response_item","payload":{"type":"function_call_output","call_id":"call_1","output":"a\nb\n"}}"#,
            r#"{"timestamp":"2026-04-20T05:00:04.000Z","type":"event_msg","payload":{"type":"task_complete","turn_id":"t1"}}"#,
        ];
        fs::write(&rollout, format!("{}\n", lines.join("\n"))).unwrap();

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        super::sync_codex_rollouts(&conn, &mut state, &home.join(".codex")).unwrap();
        assert_eq!(
            tool_results(&conn, "codex", "sess_backfill")[0].payload_bytes,
            Some(4)
        );

        blank_tool_result_fidelity(&conn, "codex");
        blank_tool_result_fidelity_state(&mut state);
        super::sync_codex_rollouts(&conn, &mut state, &home.join(".codex")).unwrap();
        let rows = tool_results(&conn, "codex", "sess_backfill");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].payload_bytes, Some(4));
        assert_eq!(rows[0].tool_use_id.as_deref(), Some("call_1"));
        assert_eq!(rows[0].event_index, Some(0));
        assert_eq!(rows[0].result_status.as_deref(), Some("completed"));

        conn.execute(
            "UPDATE session_events SET text = 'sentinel' WHERE source = 'codex' AND kind = 'tool_result'",
            [],
        )
        .unwrap();
        super::sync_codex_rollouts(&conn, &mut state, &home.join(".codex")).unwrap();
        assert_eq!(
            tool_results(&conn, "codex", "sess_backfill")[0]
                .text
                .as_deref(),
            Some("sentinel"),
        );
    }

    /// Pin a file's mtime so a rewrite does not move its sync stamp. The
    /// reported failure needs the stamp to be identical before and after the
    /// unreadable window -- that is what makes the file invisible to the
    /// fast path once the generation has been retired.
    fn restore_mtime(path: &std::path::Path, times: std::fs::FileTimes) {
        fs::OpenOptions::new()
            .write(true)
            .open(path)
            .unwrap()
            .set_times(times)
            .unwrap();
    }

    #[test]
    fn an_unreadable_claude_transcript_keeps_the_backfill_pass_owed() {
        let dir = tempfile::tempdir().unwrap();
        let readable = dir.path().join("sess-readable.jsonl");
        let unreadable = dir.path().join("sess-unreadable.jsonl");
        let transcript = |session: &str| {
            [
                claude_line(json!({
                    "type": "assistant", "uuid": format!("{session}-a1"),
                    "sessionId": session, "cwd": "/tmp/project",
                    "timestamp": "2026-04-22T00:00:01.000Z",
                    "message": { "role": "assistant", "content": [
                        { "type": "tool_use", "id": "tu_1", "name": "Bash",
                          "input": { "command": "ls" } },
                    ]},
                })),
                claude_line(json!({
                    "type": "user", "uuid": format!("{session}-u2"),
                    "parentUuid": format!("{session}-a1"),
                    "sessionId": session, "cwd": "/tmp/project",
                    "timestamp": "2026-04-22T00:00:02.000Z",
                    "message": { "role": "user", "content": [
                        { "type": "tool_result", "tool_use_id": "tu_1", "content": "ok" },
                    ]},
                })),
            ]
            .join("\n")
                + "\n"
        };
        fs::write(&readable, transcript("sess-readable")).unwrap();
        let good = transcript("sess-unreadable");
        fs::write(&unreadable, &good).unwrap();

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();
        for session in ["sess-readable", "sess-unreadable"] {
            assert_eq!(
                tool_results(&conn, "claude", session)[0].payload_bytes,
                Some(2)
            );
        }
        // The stamp is mtime plus length, and both are held fixed from here
        // on, so every later sync sees the stamp it already stored.
        let pinned = {
            let meta = fs::metadata(&unreadable).unwrap();
            std::fs::FileTimes::new()
                .set_accessed(meta.accessed().unwrap())
                .set_modified(meta.modified().unwrap())
        };
        let stamp_before = claude_sync_stamp(&unreadable).unwrap();

        // The state an upgrade leaves behind.
        blank_tool_result_fidelity(&conn, "claude");
        blank_tool_result_fidelity_state(&mut state);

        // The file is momentarily unreadable. Invalid UTF-8 of exactly the
        // same length rather than a permission bit, because the suite runs as
        // root and root reads a chmod 000 file happily -- the test would pass
        // without proving anything.
        fs::write(&unreadable, vec![0xff_u8; good.len()]).unwrap();
        restore_mtime(&unreadable, pinned);
        assert_eq!(claude_sync_stamp(&unreadable).unwrap(), stamp_before);

        // The run reports the omission rather than claiming a clean sync.
        let error = sync_claude_session_metadata(&conn, &mut state, dir.path())
            .expect_err("a discovered transcript was not indexed");
        assert!(
            format!("{error:#}").contains("sess-unreadable.jsonl"),
            "the error names the file it could not read: {error:#}"
        );

        // Positive control: the readable file in the same run was repaired,
        // so the walk is not simply failing wholesale.
        assert_eq!(
            tool_results(&conn, "claude", "sess-readable")[0].payload_bytes,
            Some(2)
        );
        assert_eq!(
            tool_results(&conn, "claude", "sess-unreadable")[0].payload_bytes,
            None
        );
        assert!(
            state.get(super::CLAUDE_FIDELITY_GENERATION_KEY).is_none(),
            "a pass that could not read every file it discovered is not complete"
        );

        // The file reads again, with the stamp it has had all along. Nothing
        // about the file changed, so the missing-fidelity probe is the only
        // thing that can reopen it -- which is exactly what retiring the
        // generation would have taken away.
        fs::write(&unreadable, &good).unwrap();
        restore_mtime(&unreadable, pinned);
        assert_eq!(claude_sync_stamp(&unreadable).unwrap(), stamp_before);

        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();
        assert_eq!(
            tool_results(&conn, "claude", "sess-unreadable")[0].payload_bytes,
            Some(2)
        );
        assert_eq!(
            state
                .get(super::CLAUDE_FIDELITY_GENERATION_KEY)
                .and_then(Value::as_i64),
            Some(super::TOOL_RESULT_FIDELITY_GENERATION),
        );
    }

    #[test]
    fn an_all_readable_claude_walk_still_reports_success() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("sess-fine.jsonl"),
            claude_line(json!({
                "type": "user", "uuid": "u1", "sessionId": "sess-fine",
                "cwd": "/tmp/project", "timestamp": "2026-04-23T00:00:00.000Z",
                "message": { "role": "user", "content": "hello" },
            })) + "\n",
        )
        .unwrap();

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        sync_claude_session_metadata(&conn, &mut state, dir.path())
            .expect("a walk that read everything it found reports success");
        assert_eq!(
            state
                .get(super::CLAUDE_FIDELITY_GENERATION_KEY)
                .and_then(Value::as_i64),
            Some(super::TOOL_RESULT_FIDELITY_GENERATION),
        );
    }

    #[test]
    fn an_unreadable_new_claude_transcript_does_not_pin_the_backfill_open() {
        let dir = tempfile::tempdir().unwrap();
        let indexed = dir.path().join("sess-adapter.jsonl");
        fs::write(
            &indexed,
            [
                claude_line(json!({
                    "type": "assistant", "uuid": "a1", "sessionId": "sess-adapter",
                    "cwd": "/tmp/project", "timestamp": "2026-04-23T00:00:01.000Z",
                    "message": { "role": "assistant", "content": [
                        { "type": "tool_use", "id": "tu_1", "name": "Bash",
                          "input": { "command": "ls" } },
                    ]},
                })),
                claude_line(json!({
                    "type": "user", "uuid": "u2", "parentUuid": "a1",
                    "sessionId": "sess-adapter", "cwd": "/tmp/project",
                    "timestamp": "2026-04-23T00:00:02.000Z",
                    "message": { "role": "user", "content": [
                        { "type": "tool_result", "tool_use_id": "tu_1", "content": "ok" },
                    ]},
                })),
            ]
            .join("\n")
                + "\n",
        )
        .unwrap();

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();

        // A contributed row for the same session that carries no fidelity.
        // Re-reading the local transcript can never populate it, so the
        // per-session probe answers "still missing" on every sync for as long
        // as the probe is consulted at all.
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 'sess-adapter', 9, 'tool_result', 'tool_result', \
                     'remote output', 'remote-uid-1')",
            [],
        )
        .unwrap();

        // The state an upgrade leaves behind, plus a brand-new file that
        // cannot be read. It has never been stamped, so the walk reopens it
        // by itself next time -- holding the generation pending does nothing
        // for it, and a pending generation keeps the probe live.
        blank_tool_result_fidelity(&conn, "claude");
        blank_tool_result_fidelity_state(&mut state);
        fs::write(
            dir.path().join("sess-broken.jsonl"),
            b"\xff\xfe not utf-8\n",
        )
        .unwrap();

        sync_claude_session_metadata(&conn, &mut state, dir.path())
            .expect_err("the broken file is reported, not swallowed");
        assert_eq!(
            tool_result_for(&conn, "claude", "sess-adapter", "tu_1").payload_bytes,
            Some(2),
            "the readable transcript is still repaired"
        );
        assert_eq!(
            state
                .get(super::CLAUDE_FIDELITY_GENERATION_KEY)
                .and_then(Value::as_i64),
            Some(super::TOOL_RESULT_FIDELITY_GENERATION),
            "a never-stamped unreadable file is revisited on its own and must \
             not hold the pass open"
        );

        // With the pass retired, the probe is no longer consulted, so the
        // contributed null row cannot drag the unchanged transcript through a
        // re-read on every sync. A sentinel a re-read would overwrite proves
        // the file stayed on the fast path.
        conn.execute(
            "UPDATE session_events SET text = 'sentinel' \
             WHERE source = 'claude' AND event_uid = 'u2:0'",
            [],
        )
        .unwrap();
        sync_claude_session_metadata(&conn, &mut state, dir.path())
            .expect_err("the broken file is still there and still reported");
        let local = conn
            .query_row(
                "SELECT text FROM session_events WHERE source='claude' AND event_uid='u2:0'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap();
        assert_eq!(
            local, "sentinel",
            "an unchanged transcript must not be re-read on every sync"
        );
    }

    #[test]
    fn an_unreadable_new_claude_transcript_is_not_stamped_as_indexed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sess-new.jsonl");
        // A file seen for the first time and unreadable on that pass. The
        // stamp is this walk's claim to have indexed it, so recording it
        // before the read means the content is skipped once it arrives.
        fs::write(&path, b"\xff\xfe not utf-8\n").unwrap();

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        sync_claude_session_metadata(&conn, &mut state, dir.path())
            .expect_err("a discovered transcript was not indexed");
        let stamps = state
            .get("claude_sessions_v3")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        assert!(
            !stamps.contains_key(&path.to_string_lossy().to_string()),
            "an unreadable file must not be stamped as though it were read"
        );
    }

    #[test]
    fn an_unreachable_codex_archive_keeps_the_backfill_pass_owed() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let live_day = home.join(".codex/sessions/2026/04/20");
        let archive_day = home.join(".codex/archived_sessions/2026/01/02");
        fs::create_dir_all(&live_day).unwrap();
        fs::create_dir_all(&archive_day).unwrap();

        let rollout = |id: &str, ts: &str| {
            [
                format!(
                    r#"{{"timestamp":"{ts}","type":"session_meta","payload":{{"id":"{id}","cwd":"/tmp/project"}}}}"#
                ),
                format!(
                    r#"{{"timestamp":"{ts}","type":"response_item","payload":{{"type":"function_call","name":"shell","arguments":"{{\"command\":\"ls\"}}","call_id":"c_{id}"}}}}"#
                ),
                format!(
                    r#"{{"timestamp":"{ts}","type":"response_item","payload":{{"type":"function_call_output","call_id":"c_{id}","output":"out"}}}}"#
                ),
                format!(
                    r#"{{"timestamp":"{ts}","type":"event_msg","payload":{{"type":"task_complete","turn_id":"t1"}}}}"#
                ),
            ]
            .join("\n")
                + "\n"
        };
        let live = live_day.join("rollout-2026-04-20T00-00-00-sess_live.jsonl");
        let archived = archive_day.join("rollout-2026-01-02T00-00-00-sess_archived.jsonl");
        fs::write(&live, rollout("sess_live", "2026-04-20T00:00:00.000Z")).unwrap();
        fs::write(
            &archived,
            rollout("sess_archived", "2026-01-02T00:00:00.000Z"),
        )
        .unwrap();

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        super::sync_codex_rollouts(&conn, &mut state, &home.join(".codex")).unwrap();
        assert_eq!(
            tool_results(&conn, "codex", "sess_archived")[0].payload_bytes,
            Some(3)
        );

        // The state an upgrade leaves behind: rows and stamps for both roots,
        // fidelity null, no backfill generation recorded.
        blank_tool_result_fidelity(&conn, "codex");
        blank_tool_result_fidelity_state(&mut state);

        // The archive is not mounted on this run.
        let stowed = home.join("archived_sessions.away");
        fs::rename(home.join(".codex/archived_sessions"), &stowed).unwrap();
        super::sync_codex_rollouts(&conn, &mut state, &home.join(".codex")).unwrap();

        // The reachable root was backfilled, but the pass is still owed: the
        // archived rows are stamped, so retiring the generation here would
        // strand them null for good.
        assert_eq!(
            tool_results(&conn, "codex", "sess_live")[0].payload_bytes,
            Some(3)
        );
        assert_eq!(
            tool_results(&conn, "codex", "sess_archived")[0].payload_bytes,
            None
        );
        assert!(
            state.get(super::CODEX_FIDELITY_GENERATION_KEY).is_none(),
            "a walk that could not reach an indexed root must not retire the pass"
        );

        // The archive comes back.
        fs::rename(&stowed, home.join(".codex/archived_sessions")).unwrap();
        super::sync_codex_rollouts(&conn, &mut state, &home.join(".codex")).unwrap();
        assert_eq!(
            tool_results(&conn, "codex", "sess_archived")[0].payload_bytes,
            Some(3)
        );
        assert_eq!(
            state
                .get(super::CODEX_FIDELITY_GENERATION_KEY)
                .and_then(Value::as_i64),
            Some(super::TOOL_RESULT_FIDELITY_GENERATION),
        );
    }

    #[test]
    fn a_codex_root_this_database_never_indexed_does_not_hold_the_pass_open() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let day = home.join(".codex/sessions/2026/04/20");
        fs::create_dir_all(&day).unwrap();
        // Most installs have no archive directory at all. Waiting for one that
        // has never existed would leave the pass pending on every sync
        // forever, which is the same bug pointed the other way.
        let path = day.join("rollout-2026-04-20T00-00-00-sess_only.jsonl");
        fs::write(
            &path,
            "{\"timestamp\":\"2026-04-20T00:00:00.000Z\",\"type\":\"session_meta\",\"payload\":{\"id\":\"sess_only\",\"cwd\":\"/tmp/project\"}}\n",
        )
        .unwrap();

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        assert!(!home.join(".codex/archived_sessions").exists());
        super::sync_codex_rollouts(&conn, &mut state, &home.join(".codex")).unwrap();
        assert_eq!(
            state
                .get(super::CODEX_FIDELITY_GENERATION_KEY)
                .and_then(Value::as_i64),
            Some(super::TOOL_RESULT_FIDELITY_GENERATION),
        );
    }

    #[test]
    fn codex_function_outputs_are_not_user_turns() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join("rollout-2026-04-21T00-00-00-sess_turns.jsonl");
        // Codex records the human message and each function output as separate
        // response items, and an output's `message_id` is its own item id. It
        // is not a block on the user's message and never was, so a rollout
        // with one prompt and three outputs is one user turn, not four.
        let lines = [
            r#"{"timestamp":"2026-04-21T00:00:00.000Z","type":"session_meta","payload":{"id":"sess_turns","cwd":"/tmp/project"}}"#.to_string(),
            r#"{"timestamp":"2026-04-21T00:00:01.000Z","type":"event_msg","payload":{"type":"user_message","message":"fix the build"}}"#.to_string(),
            r#"{"timestamp":"2026-04-21T00:00:02.000Z","type":"response_item","payload":{"type":"function_call","id":"fc_1","name":"shell","arguments":"{\"command\":\"ls\"}","call_id":"c1"}}"#.to_string(),
            r#"{"timestamp":"2026-04-21T00:00:03.000Z","type":"response_item","payload":{"type":"function_call_output","id":"fo_1","call_id":"c1","output":"one"}}"#.to_string(),
            r#"{"timestamp":"2026-04-21T00:00:04.000Z","type":"response_item","payload":{"type":"function_call_output","id":"fo_2","call_id":"c2","output":"two"}}"#.to_string(),
            r#"{"timestamp":"2026-04-21T00:00:05.000Z","type":"response_item","payload":{"type":"function_call_output","id":"fo_3","call_id":"c3","output":"three"}}"#.to_string(),
            r#"{"timestamp":"2026-04-21T00:00:06.000Z","type":"event_msg","payload":{"type":"task_complete","turn_id":"t1"}}"#.to_string(),
        ];
        fs::write(&path, format!("{}\n", lines.join("\n"))).unwrap();

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        super::ingest_codex_rollout(&conn, &path, &codex_meta(&path)).unwrap();

        // The outputs are still indexed as tool results with their facts; they
        // are simply not user turns.
        assert_eq!(tool_results(&conn, "codex", "sess_turns").len(), 3);

        let page = crate::session_user_turns_page(&conn, "codex", "sess_turns", 100, None).unwrap();
        assert_eq!(
            page.user_turns.len(),
            1,
            "one prompt is one user turn: {:?}",
            page.user_turns,
        );
        let turn = &page.user_turns[0];
        assert_eq!(turn.blocks.len(), 1);
        assert_eq!(turn.blocks[0].kind, "text");
        assert_eq!(turn.blocks[0].byte_len, "fix the build".len() as i64);
    }

    #[test]
    fn a_contributed_row_without_fidelity_does_not_re_read_the_local_transcript_forever() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sess-shared.jsonl");
        let lines = [
            claude_line(json!({
                "type": "assistant", "uuid": "a1", "sessionId": "sess-shared",
                "cwd": "/tmp/project", "timestamp": "2026-04-20T00:00:01.000Z",
                "message": { "role": "assistant", "content": [
                    { "type": "tool_use", "id": "tu_1", "name": "Bash",
                      "input": { "command": "ls" } },
                ]},
            })),
            claude_line(json!({
                "type": "user", "uuid": "u2", "parentUuid": "a1",
                "sessionId": "sess-shared", "cwd": "/tmp/project",
                "timestamp": "2026-04-20T00:00:02.000Z",
                "message": { "role": "user", "content": [
                    { "type": "tool_result", "tool_use_id": "tu_1", "content": "ok" },
                ]},
            })),
        ];
        fs::write(&path, format!("{}\n", lines.join("\n"))).unwrap();

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();
        assert_eq!(
            tool_results(&conn, "claude", "sess-shared")[0].payload_bytes,
            Some(2)
        );

        // A remote observation of the same session, contributed through the
        // source-adapter boundary. `session_events` is keyed by
        // `(source, session_id)`, so it lands beside the local rows -- and the
        // adapter contract lets it record no fidelity at all. Re-reading the
        // local transcript can never populate this row, because the local
        // transcript does not contain it.
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 'sess-shared', 3, 'tool_result', 'tool_result', \
                     'remote output', 'remote-uid-1')",
            [],
        )
        .unwrap();

        // A sentinel a re-read would overwrite. If the null remote row put the
        // file back in the backfill set, this sync re-reads it -- and so would
        // every sync after it, forever, while never repairing the remote row.
        conn.execute(
            "UPDATE session_events SET text = 'sentinel' \
             WHERE source = 'claude' AND event_uid = 'u2:0'",
            [],
        )
        .unwrap();
        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();
        let local = conn
            .query_row(
                "SELECT text FROM session_events WHERE source='claude' AND event_uid='u2:0'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap();
        assert_eq!(
            local, "sentinel",
            "an unchanged transcript must stay on the fast path even when another \
             observation of the same session has no fidelity"
        );
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
             request_span = NULL, raw_facts_version = NULL WHERE source = ?",
            [source],
        )
        .unwrap();
    }

    /// The two raw-facts constants move together or the bump does nothing.
    ///
    /// `RAW_MESSAGE_FACTS_VERSION` is stamped on each row and is what
    /// `events_lack_raw_facts` compares against; `RAW_MESSAGE_FACTS_GENERATION`
    /// is the per-install sync-state marker that decides whether that probe is
    /// consulted at all. Raising the version alone leaves the probe able to see
    /// stale rows on an install that is never asked — a bump that reads as done
    /// and repairs nothing, for exactly the installs that needed it. Raising
    /// the generation alone runs a pass that finds nothing to repair.
    #[test]
    fn the_raw_facts_version_and_generation_are_bumped_together() {
        assert_eq!(
            super::RAW_MESSAGE_FACTS_VERSION,
            super::RAW_MESSAGE_FACTS_GENERATION,
            "bump both, or the backfill it exists to trigger never runs"
        );
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
        super::sync_codex_rollouts(&conn, &mut state, &home.join(".codex")).unwrap();
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
        super::sync_codex_rollouts(&conn, &mut state, &home.join(".codex")).unwrap();
        assert_eq!(turn_id(&conn, "sess_a"), Some("turn_sess_a".into()));
        assert!(
            state.get(super::CODEX_RAW_MESSAGE_FACTS_KEY).is_none(),
            "a run that saw only part of a known archive has not done the pass"
        );

        // Back, unchanged. Its stamp was dropped when the walk could not see
        // it, so it is read afresh rather than skipped on a stamp nothing
        // watched, and its facts land.
        restore_unchanged(&rollout("sess_b"), &lines("sess_b"), b_modified);
        super::sync_codex_rollouts(&conn, &mut state, &home.join(".codex")).unwrap();
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
            super::sync_codex_rollouts(conn, &mut state, &home.join(".codex")).unwrap();
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

    /// An install already at the recorded generation must still re-read its
    /// unchanged Codex rollouts once when a new fact is added.
    ///
    /// `events_lack_raw_facts` compares `raw_facts_version`, but sync only
    /// consults it while `raw_facts_backfill_pending` is true, and that is
    /// `state < RAW_MESSAGE_FACTS_GENERATION`. An install sitting at the
    /// recorded generation answers "not pending", skips every unchanged
    /// rollout, and leaves the new fact null forever \u2014 so raising the row
    /// version alone is a bump that reads as done and repairs nothing.
    #[test]
    fn an_install_at_the_old_generation_backfills_codex_request_spans() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        let state_path = home.join(".sync-state.json");
        let day = home.join(".codex/sessions/2026/04/20");
        fs::create_dir_all(&day).unwrap();
        let rollout = day.join("rollout-2026-04-20T05-00-00-sess_span.jsonl");
        // Reasoning, a tool call and a message: one API call, three rows.
        let lines = concat!(
            r#"{"timestamp":"2026-04-20T05:00:00.000Z","type":"session_meta","payload":{"id":"sess_span","cwd":"/tmp/project"}}"#,
            "\n",
            r#"{"timestamp":"2026-04-20T05:00:00.100Z","type":"turn_context","payload":{"turn_id":"t1","cwd":"/tmp/project","model":"gpt-5.4"}}"#,
            "\n",
            r#"{"timestamp":"2026-04-20T05:00:01.000Z","type":"event_msg","payload":{"type":"user_message","message":"run it"}}"#,
            "\n",
            r#"{"timestamp":"2026-04-20T05:00:01.500Z","type":"event_msg","payload":{"type":"agent_reasoning","text":"Thinking."}}"#,
            "\n",
            r#"{"timestamp":"2026-04-20T05:00:01.700Z","type":"response_item","payload":{"type":"function_call","name":"shell","call_id":"c1","arguments":"{\"command\":[\"ls\"]}"}}"#,
            "\n",
            r#"{"timestamp":"2026-04-20T05:00:02.000Z","type":"event_msg","payload":{"type":"agent_message","message":"done"}}"#,
            "\n",
            r#"{"timestamp":"2026-04-20T05:00:03.000Z","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":500,"cached_input_tokens":0,"output_tokens":120,"reasoning_output_tokens":40,"total_tokens":620}}}}"#,
            "\n",
        );
        fs::write(&rollout, lines).unwrap();

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let sync = |conn: &Connection| {
            let mut state = super::load_sync_state(&state_path).unwrap();
            super::sync_codex_rollouts(conn, &mut state, &home.join(".codex")).unwrap();
            super::checkpoint_sync_state(&state_path, &state);
            super::load_sync_state(&state_path).unwrap()
        };
        let spans = |conn: &Connection| -> i64 {
            conn.query_row(
                "SELECT COUNT(*) FROM session_events \
                 WHERE source='codex' AND role='assistant' AND request_span IS NOT NULL",
                [],
                |row| row.get(0),
            )
            .unwrap()
        };
        let requests = |conn: &Connection| -> usize {
            crate::session_usage::session_requests_page(conn, "codex", "sess_span", 50, None)
                .unwrap()
                .requests
                .len()
        };

        let state = sync(&conn);
        assert_eq!(spans(&conn), 3, "a fresh sync stamps every assistant row");
        assert_eq!(requests(&conn), 1);
        let modified = rollout.metadata().unwrap().modified().unwrap();

        // Now the install this fix is for: rows written by the parser one
        // version back, and the sync state already retired at that
        // generation. The transcript is byte-identical and its mtime unmoved,
        // so nothing but the backfill can bring it back.
        let mut state = state;
        //
        // The literal 1 is deliberate: this is the install that exists in the
        // world today, the one that shipped before `request_span` and has
        // already retired its raw-facts pass at generation 1. Written
        // relative to the constants the test would be vacuous \u2014 it would pass
        // whether or not the generation was bumped alongside the version.
        conn.execute(
            "UPDATE session_events SET request_span = NULL, raw_facts_version = 1 \
             WHERE source = 'codex'",
            [],
        )
        .unwrap();
        state.insert(super::CODEX_RAW_MESSAGE_FACTS_KEY.to_string(), json!(1));
        super::save_sync_state(&state_path, &state).unwrap();
        assert_eq!(spans(&conn), 0);
        assert_eq!(
            requests(&conn),
            3,
            "and the defect is back: one call read as a request per row"
        );

        let state = sync(&conn);
        assert_eq!(
            spans(&conn),
            3,
            "an unchanged rollout is re-read once for the new fact"
        );
        assert_eq!(requests(&conn), 1, "and the call is one request again");
        assert_eq!(
            state
                .get(super::CODEX_RAW_MESSAGE_FACTS_KEY)
                .and_then(Value::as_i64),
            Some(super::RAW_MESSAGE_FACTS_GENERATION),
            "the pass is retired at the new generation"
        );

        // Positive control: already at the new generation, an unchanged
        // rollout is not re-read. Without this the test would pass just as
        // well if sync re-read every rollout every time, which is the cost
        // this generation marker exists to avoid.
        let mut state = state;
        conn.execute(
            "UPDATE session_events SET request_span = NULL WHERE source = 'codex'",
            [],
        )
        .unwrap();
        super::save_sync_state(&state_path, &state).unwrap();
        restore_unchanged(&rollout, lines, modified);
        let _ = &mut state;
        sync(&conn);
        assert_eq!(
            spans(&conn),
            0,
            "an install already at this generation does not re-read"
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
            super::sync_codex_rollouts(conn, &mut state, &home.join(".codex")).unwrap();
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
        super::sync_codex_rollouts(&conn, &mut state, &home.join(".codex")).unwrap();
        assert_eq!(turn_id(&conn), Some("turn_1".into()));
        let stamp = file_stamp(&rollout).unwrap();
        let modified = rollout.metadata().unwrap().modified().unwrap();

        // The upgraded-install state, with the archive mounted but empty. The
        // root is still there; its contents are not.
        blank_raw_message_facts(&conn, "codex");
        blank_raw_message_facts_state(&mut state);
        fs::remove_file(&rollout).unwrap();
        assert!(home.join(".codex/archived_sessions").exists());
        super::sync_codex_rollouts(&conn, &mut state, &home.join(".codex")).unwrap();
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
        super::sync_codex_rollouts(&conn, &mut state, &home.join(".codex")).unwrap();
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
        super::sync_codex_rollouts(&conn, &mut state, &home.join(".codex")).unwrap();
        assert_eq!(turn_id(&conn), Some("turn_1".into()));

        // The upgraded-install state, with the archive unavailable: an
        // unmounted home, an external drive, a profile not yet materialized.
        blank_raw_message_facts(&conn, "codex");
        blank_raw_message_facts_state(&mut state);
        fs::remove_dir_all(home.join(".codex/archived_sessions")).unwrap();
        super::sync_codex_rollouts(&conn, &mut state, &home.join(".codex")).unwrap();
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
        super::sync_codex_rollouts(&conn, &mut state, &home.join(".codex")).unwrap();
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
        super::sync_codex_rollouts(&conn, &mut state, &home.join(".codex")).unwrap();
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
        super::sync_codex_rollouts(&conn, &mut state, &home.join(".codex")).unwrap();
        assert_eq!(
            turn_ids(&conn),
            vec![
                ("2:user_message".into(), Some("turn_1".into())),
                ("3:agent_message".into(), Some("turn_1".into())),
            ]
        );

        blank_raw_message_facts(&conn, "codex");
        blank_raw_message_facts_state(&mut state);
        super::sync_codex_rollouts(&conn, &mut state, &home.join(".codex")).unwrap();
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
        super::sync_codex_rollouts(&conn, &mut state, &home.join(".codex")).unwrap();
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

    /// The tool-result row answering one call, whatever its position. A
    /// contributed row can sort ahead of the parsed ones, so indexing into
    /// the list would assert against the wrong row.
    fn tool_result_for(
        conn: &Connection,
        source: &str,
        session_id: &str,
        tool_use_id: &str,
    ) -> crate::SessionEvent {
        tool_results(conn, source, session_id)
            .into_iter()
            .find(|event| event.tool_use_id.as_deref() == Some(tool_use_id))
            .unwrap_or_else(|| panic!("no tool result for {tool_use_id}"))
    }

    /// Tool-result rows for one session, in transcript order.
    fn tool_results(conn: &Connection, source: &str, session_id: &str) -> Vec<crate::SessionEvent> {
        crate::session_events(conn, session_id, Some(source))
            .unwrap()
            .into_iter()
            .filter(|event| event.kind == "tool_result")
            .collect()
    }

    fn claude_line(value: Value) -> String {
        serde_json::to_string(&value).unwrap()
    }

    #[test]
    fn claude_tool_results_measure_the_raw_payload_not_the_stored_text() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bloat-session.jsonl");
        // Shaped after relayburn's `claude/oversized-bash-output` fixture: a
        // single Bash call whose result is exactly 80 000 bytes. The assertion
        // below is that number rather than "something large", so a payload
        // measured here and one measured there can be compared directly
        // instead of merely looking similar.
        let oversized = "x".repeat(80_000);
        let marked = "partial output\n<system-truncated>\n";
        let structured = json!([{ "type": "text", "text": "ok" }]);
        let lines = [
            claude_line(json!({
                "type": "user", "uuid": "u1", "sessionId": "bloat-session",
                "cwd": "/tmp/project", "timestamp": "2026-04-20T00:00:00.000Z",
                "message": { "role": "user", "content": "cat huge.log" },
            })),
            claude_line(json!({
                "type": "assistant", "uuid": "a1", "parentUuid": "u1",
                "sessionId": "bloat-session", "cwd": "/tmp/project",
                "timestamp": "2026-04-20T00:00:01.000Z",
                "message": { "role": "assistant", "content": [
                    { "type": "tool_use", "id": "tu_bash_big", "name": "Bash",
                      "input": { "command": "cat huge.log" } },
                ]},
            })),
            claude_line(json!({
                "type": "user", "uuid": "u2", "parentUuid": "a1",
                "sessionId": "bloat-session", "cwd": "/tmp/project",
                "timestamp": "2026-04-20T00:00:02.000Z",
                "message": { "role": "user", "content": [
                    { "type": "tool_result", "tool_use_id": "tu_bash_big", "content": oversized },
                    { "type": "tool_result", "tool_use_id": "tu_grep", "content": marked,
                      "is_error": true },
                    { "type": "tool_result", "tool_use_id": "tu_read", "content": structured },
                ]},
            })),
        ];
        fs::write(&path, format!("{}\n", lines.join("\n"))).unwrap();

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        ingest_claude_transcript(&conn, &path).unwrap();

        let rows = tool_results(&conn, "claude", "bloat-session");
        assert_eq!(rows.len(), 3);

        let big = &rows[0];
        assert_eq!(big.payload_bytes, Some(80_000));
        assert_eq!(big.payload_truncated, Some(0));
        assert_eq!(
            big.payload_hash.as_deref(),
            Some(tool_result_facts::content_hash(oversized.as_bytes()).as_str()),
        );
        assert_eq!(big.tool_use_id.as_deref(), Some("tu_bash_big"));
        assert_eq!(big.event_source.as_deref(), Some("tool_result"));
        assert_eq!(big.result_status.as_deref(), Some("completed"));
        assert_eq!(big.error_signal, None);
        assert_eq!((big.call_index, big.event_index), (Some(0), Some(0)));

        // The harness marker is the difference between "this tool returned
        // 34 bytes" and "this tool returned far more and the harness cut it".
        let marked_row = &rows[1];
        assert_eq!(marked_row.payload_truncated, Some(1));
        assert_eq!(marked_row.payload_bytes, Some(marked.len() as i64));
        assert_eq!(marked_row.result_status.as_deref(), Some("errored"));
        assert_eq!(
            marked_row.error_signal.as_deref(),
            Some("tool_result.is_error")
        );
        assert_eq!(
            (marked_row.call_index, marked_row.event_index),
            (Some(0), Some(1))
        );

        // A structured payload is measured over its stable stringification,
        // which is what the provider actually sent, not over the flattened
        // text this row stores.
        let structured_row = &rows[2];
        let expected = tool_result_facts::stable_stringify(&structured);
        assert_eq!(structured_row.payload_bytes, Some(expected.len() as i64));
        assert_eq!(
            structured_row.payload_hash.as_deref(),
            Some(tool_result_facts::content_hash(expected.as_bytes()).as_str()),
        );
        assert_eq!(structured_row.event_index, Some(2));
    }

    #[test]
    fn claude_call_index_counts_per_tool_use_id_and_event_index_counts_the_transcript() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("repeat-session.jsonl");
        let result = |uuid: &str, parent: &str, ts: &str, blocks: Value| {
            claude_line(json!({
                "type": "user", "uuid": uuid, "parentUuid": parent,
                "sessionId": "repeat-session", "cwd": "/tmp/project", "timestamp": ts,
                "message": { "role": "user", "content": blocks },
            }))
        };
        let lines = [
            result(
                "u1",
                "root",
                "2026-04-20T00:00:01.000Z",
                json!([
                    { "type": "tool_result", "tool_use_id": "tu_a", "content": "first" },
                    { "type": "tool_result", "tool_use_id": "tu_b", "content": "second" },
                ]),
            ),
            result(
                "u2",
                "u1",
                "2026-04-20T00:00:02.000Z",
                json!([
                    { "type": "tool_result", "tool_use_id": "tu_a", "content": "again" },
                ]),
            ),
        ];
        fs::write(&path, format!("{}\n", lines.join("\n"))).unwrap();

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        ingest_claude_transcript(&conn, &path).unwrap();

        let ordering: Vec<(Option<String>, Option<i64>, Option<i64>)> =
            tool_results(&conn, "claude", "repeat-session")
                .into_iter()
                .map(|event| (event.tool_use_id, event.call_index, event.event_index))
                .collect();
        assert_eq!(
            ordering,
            vec![
                (Some("tu_a".into()), Some(0), Some(0)),
                (Some("tu_b".into()), Some(0), Some(1)),
                (Some("tu_a".into()), Some(1), Some(2)),
            ],
        );

        // Re-reading the same transcript must reproduce the indexes rather
        // than advance them: the parser starts from the top every time, and a
        // sequence that grew on every sync would make `event_index` useless
        // as an order.
        ingest_claude_transcript(&conn, &path).unwrap();
        let after: Vec<Option<i64>> = tool_results(&conn, "claude", "repeat-session")
            .into_iter()
            .map(|event| event.event_index)
            .collect();
        assert_eq!(after, vec![Some(0), Some(1), Some(2)]);
    }

    #[test]
    fn claude_subagent_notifications_are_recorded_as_linked_tool_results() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("subagent-session.jsonl");
        // Shaped after relayburn's `claude/system-subagent-notification`
        // fixture. The notification is the only record that ties the child
        // session back to the Agent call that spawned it.
        let lines = [
            claude_line(json!({
                "type": "assistant", "uuid": "a1", "sessionId": "subagent-session",
                "cwd": "/tmp/project", "timestamp": "2026-04-24T01:00:00.000Z",
                "message": { "role": "assistant", "content": [
                    { "type": "tool_use", "id": "toolu_system", "name": "Agent",
                      "input": { "subagent_type": "Explore" } },
                ]},
            })),
            claude_line(json!({
                "type": "system", "subtype": "subagent_completed",
                "sessionId": "subagent-session", "timestamp": "2026-04-24T01:00:01.000Z",
                "parent_tool_use_id": "toolu_system", "agent_id": "agent-system-1",
                "subagent_session_id": "session-system-child", "status": "completed",
                "content": "subagent completed",
            })),
        ];
        fs::write(&path, format!("{}\n", lines.join("\n"))).unwrap();

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        ingest_claude_transcript(&conn, &path).unwrap();

        let rows = tool_results(&conn, "claude", "subagent-session");
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.event_source.as_deref(), Some("subagent_notification"));
        assert_eq!(
            row.subagent_session_id.as_deref(),
            Some("session-system-child")
        );
        assert_eq!(row.agent_id.as_deref(), Some("agent-system-1"));
        assert_eq!(row.tool_use_id.as_deref(), Some("toolu_system"));
        assert_eq!(row.result_status.as_deref(), Some("completed"));
        assert_eq!(row.text.as_deref(), Some("subagent completed"));
        assert_eq!(row.payload_bytes, Some("subagent completed".len() as i64));

        // A subagent notification is stored as a tool result because that is
        // what it is evidence of, but it never arrived on a user message. It
        // is not a user turn, and grouping on `role` alone would make it one.
        let turns =
            crate::session_user_turns_page(&conn, "claude", "subagent-session", 100, None).unwrap();
        assert!(
            turns.user_turns.is_empty(),
            "a harness notification is not a user turn: {:?}",
            turns.user_turns,
        );

        // A system line that names no child is harness chatter, not a tool
        // result, and must not manufacture one.
        let noise = dir.path().join("noise.jsonl");
        fs::write(
            &noise,
            format!(
                "{}\n",
                claude_line(json!({
                    "type": "system", "subtype": "hook_ran", "sessionId": "noise-session",
                    "timestamp": "2026-04-24T01:00:02.000Z", "content": "hook ran",
                })),
            ),
        )
        .unwrap();
        ingest_claude_transcript(&conn, &noise).unwrap();
        assert!(tool_results(&conn, "claude", "noise-session").is_empty());
    }

    #[test]
    fn claude_subagent_notifications_survive_the_sidechain_skip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sidecar-session.jsonl");
        // A nested Agent call writes its completion line inside the child's
        // sidecar, where every record carries `isSidechain: true`. The
        // sidechain guard skips any record whose message is not an assistant
        // one, and a system line has no message at all -- so the only record
        // tying the grandchild back to the call that spawned it was being
        // dropped precisely where nesting puts it.
        let lines = [
            claude_line(json!({
                "type": "assistant", "uuid": "sc1", "sessionId": "sidecar-session",
                "isSidechain": true, "cwd": "/tmp/project",
                "timestamp": "2026-04-24T02:00:00.000Z",
                "message": { "role": "assistant", "content": [
                    { "type": "tool_use", "id": "toolu_nested", "name": "Agent",
                      "input": { "subagent_type": "Explore" } },
                ]},
            })),
            claude_line(json!({
                "type": "system", "subtype": "subagent_completed",
                "sessionId": "sidecar-session", "isSidechain": true,
                "timestamp": "2026-04-24T02:00:01.000Z",
                "parent_tool_use_id": "toolu_nested", "agent_id": "agent-nested-1",
                "subagent_session_id": "session-nested-child", "status": "completed",
                "content": "nested subagent completed",
            })),
        ];
        fs::write(&path, format!("{}\n", lines.join("\n"))).unwrap();

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        ingest_claude_transcript(&conn, &path).unwrap();

        let rows = tool_results(&conn, "claude", "sidecar-session");
        assert_eq!(
            rows.len(),
            1,
            "a sidechain system line still links its delegated child: {rows:?}",
        );
        assert_eq!(
            rows[0].subagent_session_id.as_deref(),
            Some("session-nested-child")
        );
        assert_eq!(rows[0].tool_use_id.as_deref(), Some("toolu_nested"));

        // Every other sidechain record is still skipped: the guard below the
        // notification handler is unchanged, and a sidechain user row is the
        // parent's own prompt rather than a human turn of the child's.
        let sidechain_user = dir.path().join("sidecar-user.jsonl");
        fs::write(
            &sidechain_user,
            format!(
                "{}\n",
                claude_line(json!({
                    "type": "user", "uuid": "sc2", "sessionId": "sidecar-user-session",
                    "isSidechain": true, "timestamp": "2026-04-24T02:00:02.000Z",
                    "message": { "role": "user", "content": "delegated instructions" },
                })),
            ),
        )
        .unwrap();
        ingest_claude_transcript(&conn, &sidechain_user).unwrap();
        let events: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_events WHERE source = 'claude' AND session_id = ?",
                params!["sidecar-user-session"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(events, 0, "a sidechain user row is not the child's turn");
    }

    #[test]
    fn codex_tool_results_take_their_status_from_the_turns_out_of_band_signals() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join("rollout-2026-04-20T01-00-00-sess_tools_1.jsonl");
        // Shaped after relayburn's `codex/with-tool-call` and
        // `codex/oversized-shell-output` fixtures, with the exit code and the
        // patch outcome flipped to failures: Codex reports both out of band,
        // and neither is visible on the `function_call_output` row itself.
        let lines = [
            r#"{"timestamp":"2026-04-20T01:00:00.000Z","type":"session_meta","payload":{"id":"sess_tools_1","cwd":"/tmp/project","timestamp":"2026-04-20T01:00:00.000Z"}}"#.to_string(),
            r#"{"timestamp":"2026-04-20T01:00:00.100Z","type":"turn_context","payload":{"turn_id":"turn_tools_1","cwd":"/tmp/project","model":"gpt-5.3-codex"}}"#.to_string(),
            r#"{"timestamp":"2026-04-20T01:00:01.000Z","type":"response_item","payload":{"type":"function_call","name":"shell","arguments":"{\"command\":\"cat huge.log\"}","call_id":"call_shell_1"}}"#.to_string(),
            r#"{"timestamp":"2026-04-20T01:00:01.500Z","type":"event_msg","payload":{"type":"exec_command_end","call_id":"call_shell_1","turn_id":"turn_tools_1","exit_code":2}}"#.to_string(),
            r#"{"timestamp":"2026-04-20T01:00:01.700Z","type":"response_item","payload":{"type":"function_call_output","call_id":"call_shell_1","output":"boom"}}"#.to_string(),
            r#"{"timestamp":"2026-04-20T01:00:02.000Z","type":"response_item","payload":{"type":"custom_tool_call","status":"completed","call_id":"call_patch_1","name":"apply_patch","input":"*** Begin Patch\n*** Update File: /tmp/project/README.md\n@@\n+banner\n*** End Patch\n"}}"#.to_string(),
            r#"{"timestamp":"2026-04-20T01:00:02.500Z","type":"event_msg","payload":{"type":"patch_apply_end","call_id":"call_patch_1","turn_id":"turn_tools_1","success":false,"changes":{}}}"#.to_string(),
            r#"{"timestamp":"2026-04-20T01:00:02.700Z","type":"response_item","payload":{"type":"function_call_output","call_id":"call_patch_1","output":"patch failed"}}"#.to_string(),
            r#"{"timestamp":"2026-04-20T01:00:03.000Z","type":"response_item","payload":{"type":"function_call","name":"read_file","arguments":"{\"path\":\"/tmp/project/a.ts\"}","call_id":"call_read_1"}}"#.to_string(),
            r#"{"timestamp":"2026-04-20T01:00:03.500Z","type":"response_item","payload":{"type":"function_call_output","call_id":"call_read_1","output":"contents"}}"#.to_string(),
            r#"{"timestamp":"2026-04-20T01:00:04.100Z","type":"event_msg","payload":{"type":"task_complete","turn_id":"turn_tools_1"}}"#.to_string(),
        ];
        fs::write(&path, format!("{}\n", lines.join("\n"))).unwrap();

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        super::ingest_codex_rollout(&conn, &path, &codex_meta(&path)).unwrap();

        let rows = tool_results(&conn, "codex", "sess_tools_1");
        let seen: Vec<String> = rows
            .iter()
            .map(|event| {
                format!(
                    "{}|{}|{}|{}|{}",
                    event.tool_use_id.as_deref().unwrap_or("-"),
                    event.result_status.as_deref().unwrap_or("-"),
                    event.error_signal.as_deref().unwrap_or("-"),
                    event.event_source.as_deref().unwrap_or("-"),
                    event
                        .event_index
                        .map_or_else(|| "-".to_string(), |index| index.to_string()),
                )
            })
            .collect();
        assert_eq!(
            seen,
            vec![
                "call_shell_1|errored|exit_code|function_call_output|0",
                "call_patch_1|errored|patch_apply|function_call_output|1",
                "call_read_1|completed|-|function_call_output|2",
            ],
        );
        assert_eq!(rows[0].payload_bytes, Some(4));
        assert_eq!(
            rows[0].payload_hash.as_deref(),
            Some(tool_result_facts::content_hash(b"boom").as_str()),
        );
    }

    #[test]
    fn codex_results_of_an_unfinished_turn_still_carry_the_signals_seen_so_far() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join("rollout-2026-04-20T02-00-00-sess_live_1.jsonl");
        // A live session has no `task_complete` yet. A failure the transcript
        // already states is recorded; a result that has simply not been
        // reported on yet stays `unknown`, because end of file is not a turn
        // boundary and "no failure seen" is not "succeeded".
        let lines = [
            r#"{"timestamp":"2026-04-20T02:00:00.000Z","type":"session_meta","payload":{"id":"sess_live_1","cwd":"/tmp/project","timestamp":"2026-04-20T02:00:00.000Z"}}"#,
            r#"{"timestamp":"2026-04-20T02:00:01.000Z","type":"response_item","payload":{"type":"function_call","name":"shell","arguments":"{\"command\":\"false\"}","call_id":"call_live_1"}}"#,
            r#"{"timestamp":"2026-04-20T02:00:01.500Z","type":"event_msg","payload":{"type":"exec_command_end","call_id":"call_live_1","exit_code":1}}"#,
            r#"{"timestamp":"2026-04-20T02:00:01.700Z","type":"response_item","payload":{"type":"function_call_output","call_id":"call_live_1","output":"nope"}}"#,
            r#"{"timestamp":"2026-04-20T02:00:02.000Z","type":"response_item","payload":{"type":"function_call","name":"read_file","arguments":"{\"path\":\"/tmp/a.ts\"}","call_id":"call_live_2"}}"#,
            r#"{"timestamp":"2026-04-20T02:00:02.500Z","type":"response_item","payload":{"type":"function_call_output","call_id":"call_live_2","output":"contents"}}"#,
        ];
        fs::write(&path, format!("{}\n", lines.join("\n"))).unwrap();

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        super::ingest_codex_rollout(&conn, &path, &codex_meta(&path)).unwrap();

        let rows = tool_results(&conn, "codex", "sess_live_1");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].result_status.as_deref(), Some("errored"));
        assert_eq!(rows[0].error_signal.as_deref(), Some("exit_code"));
        assert_eq!(rows[1].result_status.as_deref(), Some("unknown"));
        assert_eq!(rows[1].error_signal, None);
    }

    #[test]
    fn a_codex_failure_reported_after_the_output_is_not_pre_empted_by_a_partial_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join("rollout-2026-04-20T03-00-00-sess_late_1.jsonl");
        // The exact ordering that makes end-of-file unsafe to settle on:
        // the output is written, the sync reads the file, and only then does
        // the `exec_command_end` that failed the call arrive. Calling it
        // `completed` on the first pass would be a well-formed lie that the
        // second pass has to retract.
        let head = [
            r#"{"timestamp":"2026-04-20T03:00:00.000Z","type":"session_meta","payload":{"id":"sess_late_1","cwd":"/tmp/project","timestamp":"2026-04-20T03:00:00.000Z"}}"#,
            r#"{"timestamp":"2026-04-20T03:00:01.000Z","type":"response_item","payload":{"type":"function_call","name":"shell","arguments":"{\"command\":\"make\"}","call_id":"call_late_1"}}"#,
            r#"{"timestamp":"2026-04-20T03:00:01.700Z","type":"response_item","payload":{"type":"function_call_output","call_id":"call_late_1","output":"building"}}"#,
        ];
        fs::write(&path, format!("{}\n", head.join("\n"))).unwrap();

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        super::ingest_codex_rollout(&conn, &path, &codex_meta(&path)).unwrap();
        let rows = tool_results(&conn, "codex", "sess_late_1");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].result_status.as_deref(), Some("unknown"));

        // The rest of the turn lands, and the next sync re-reads the file.
        let tail = [
            r#"{"timestamp":"2026-04-20T03:00:02.000Z","type":"event_msg","payload":{"type":"exec_command_end","call_id":"call_late_1","turn_id":"t1","exit_code":1}}"#,
            r#"{"timestamp":"2026-04-20T03:00:02.100Z","type":"event_msg","payload":{"type":"task_complete","turn_id":"t1"}}"#,
        ];
        let mut grown = head.to_vec();
        grown.extend_from_slice(&tail);
        fs::write(&path, format!("{}\n", grown.join("\n"))).unwrap();
        super::ingest_codex_rollout(&conn, &path, &codex_meta(&path)).unwrap();

        let rows = tool_results(&conn, "codex", "sess_late_1");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].result_status.as_deref(), Some("errored"));
        assert_eq!(rows[0].error_signal.as_deref(), Some("exit_code"));
    }

    #[test]
    fn a_codex_result_with_no_displayable_text_is_still_recorded() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join("rollout-2026-04-20T04-00-00-sess_silent_1.jsonl");
        // A silent command answers with an empty string, and a structured
        // output can carry no `text` member at all. Both are results; a row
        // that reports zero bytes is a measurement, and dropping the row
        // would lose the call linkage and the ordering as well.
        let lines = [
            r#"{"timestamp":"2026-04-20T04:00:00.000Z","type":"session_meta","payload":{"id":"sess_silent_1","cwd":"/tmp/project","timestamp":"2026-04-20T04:00:00.000Z"}}"#,
            r#"{"timestamp":"2026-04-20T04:00:01.000Z","type":"response_item","payload":{"type":"function_call","name":"shell","arguments":"{\"command\":\"true\"}","call_id":"c1"}}"#,
            r#"{"timestamp":"2026-04-20T04:00:01.500Z","type":"response_item","payload":{"type":"function_call_output","call_id":"c1","output":""}}"#,
            r#"{"timestamp":"2026-04-20T04:00:02.000Z","type":"response_item","payload":{"type":"function_call_output","call_id":"c2","output":[{"type":"image","data":"zz"}]}}"#,
            r#"{"timestamp":"2026-04-20T04:00:03.000Z","type":"event_msg","payload":{"type":"task_complete","turn_id":"t1"}}"#,
        ];
        fs::write(&path, format!("{}\n", lines.join("\n"))).unwrap();

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        super::ingest_codex_rollout(&conn, &path, &codex_meta(&path)).unwrap();

        let rows = tool_results(&conn, "codex", "sess_silent_1");
        assert_eq!(rows.len(), 2);
        let empty = &rows[0];
        assert_eq!(empty.tool_use_id.as_deref(), Some("c1"));
        assert_eq!(empty.payload_bytes, Some(0));
        assert_eq!(
            empty.payload_hash.as_deref(),
            Some(tool_result_facts::content_hash(b"").as_str()),
        );
        assert_eq!(empty.payload_truncated, Some(0));
        assert_eq!(empty.result_status.as_deref(), Some("completed"));
        assert_eq!(empty.event_index, Some(0));
        assert_eq!(empty.text.as_deref(), Some(""));

        // The structured payload has no text to show but is far from empty on
        // the wire, and its ordering continues the sequence.
        let structured = &rows[1];
        assert_eq!(structured.tool_use_id.as_deref(), Some("c2"));
        let expected =
            tool_result_facts::stable_stringify(&json!([{ "type": "image", "data": "zz" }]));
        assert_eq!(structured.payload_bytes, Some(expected.len() as i64));
        assert_eq!(structured.event_index, Some(1));
    }

    #[test]
    fn user_turn_blocks_are_derived_from_the_indexed_events() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("utb-session.jsonl");
        // Shaped after relayburn's `claude/user-turn-blocks` fixture: one
        // human turn, then one user message carrying two tool results.
        let big = "A".repeat(100);
        let lines = [
            claude_line(json!({
                "type": "user", "uuid": "u1", "sessionId": "utb-session",
                "cwd": "/tmp/project", "timestamp": "2026-04-20T00:00:00.000Z",
                "message": { "role": "user", "content": "please fix the build" },
            })),
            claude_line(json!({
                "type": "assistant", "uuid": "a1", "parentUuid": "u1",
                "sessionId": "utb-session", "cwd": "/tmp/project",
                "timestamp": "2026-04-20T00:00:01.000Z",
                "message": { "role": "assistant", "content": [
                    { "type": "tool_use", "id": "tu_bash_1", "name": "Bash",
                      "input": { "command": "ls" } },
                    { "type": "tool_use", "id": "tu_read_1", "name": "Read",
                      "input": { "file_path": "/src/app.ts" } },
                ]},
            })),
            claude_line(json!({
                "type": "user", "uuid": "u2", "parentUuid": "a1",
                "sessionId": "utb-session", "cwd": "/tmp/project",
                "timestamp": "2026-04-20T00:00:02.000Z",
                "message": { "role": "user", "content": [
                    { "type": "tool_result", "tool_use_id": "tu_bash_1", "content": "a\nb\n",
                      "is_error": true },
                    { "type": "tool_result", "tool_use_id": "tu_read_1", "content": big },
                ]},
            })),
        ];
        fs::write(&path, format!("{}\n", lines.join("\n"))).unwrap();

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        ingest_claude_transcript(&conn, &path).unwrap();

        let page =
            crate::session_user_turns_page(&conn, "claude", "utb-session", 100, None).unwrap();
        assert_eq!(page.next_cursor, None);
        assert_eq!(page.user_turns.len(), 2);

        let first = &page.user_turns[0];
        assert_eq!(first.blocks.len(), 1);
        assert_eq!(first.blocks[0].kind, "text");
        assert_eq!(first.blocks[0].tool_use_id, None);
        assert_eq!(
            first.blocks[0].byte_len,
            "please fix the build".len() as i64
        );

        let second = &page.user_turns[1];
        let shape: Vec<(&str, Option<&str>, i64, Option<i64>)> = second
            .blocks
            .iter()
            .map(|block| {
                (
                    block.kind.as_str(),
                    block.tool_use_id.as_deref(),
                    block.byte_len,
                    block.is_error,
                )
            })
            .collect();
        assert_eq!(
            shape,
            vec![
                ("tool_result", Some("tu_bash_1"), 4, Some(1)),
                ("tool_result", Some("tu_read_1"), 100, Some(0)),
            ],
        );

        // The page is keyset-ordered on the turn's first event, so a limit of
        // one hands back a cursor that resumes exactly at the second turn.
        let bounded =
            crate::session_user_turns_page(&conn, "claude", "utb-session", 1, None).unwrap();
        assert_eq!(bounded.user_turns.len(), 1);
        let cursor = bounded.next_cursor.expect("a second turn remains");
        let rest =
            crate::session_user_turns_page(&conn, "claude", "utb-session", 100, Some(&cursor))
                .unwrap();
        assert_eq!(rest.user_turns.len(), 1);
        assert_eq!(rest.user_turns[0].id, second.id);
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
            RequestIdentity::none(),
            "3:agent_message",
            None,
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
        super::sync_codex_rollouts(&conn, &mut state, &home.join(".codex")).unwrap();
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

        super::sync_codex_rollouts(&conn, &mut state, &home.join(".codex")).unwrap();
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
            super::sync_codex_rollouts(&conn, &mut state, &home.join(".codex")).unwrap();
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

        super::sync_codex_rollouts(&conn, &mut state, &home.join(".codex")).unwrap();
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
        super::sync_codex_rollouts(&conn, &mut state, &home.join(".codex")).unwrap();
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

        super::sync_codex_rollouts(&conn, &mut state, &home.join(".codex")).unwrap();
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

        let (_, _, inserted) =
            super::sync_codex_rollouts(&conn, &mut state, &home.join(".codex")).unwrap();
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

        super::sync_codex_rollouts(&conn, &mut Map::new(), &home.join(".codex")).unwrap();

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

        let error = super::sync_codex_rollouts(&conn, &mut Map::new(), &home.join(".codex"))
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
        let (cwds, _, inserted) =
            super::sync_codex_rollouts(&conn, &mut state, &home.join(".codex")).unwrap();
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
        let (_, _, inserted_again) =
            super::sync_codex_rollouts(&conn, &mut state, &home.join(".codex")).unwrap();
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

    /// Group N. A replacement that lands *while the scan is reading* must not
    /// be stamped with the old scan's offsets.
    ///
    /// `committed_cursor` answers a mid-scan change with a *reset* cursor —
    /// offset zero, the replacement's identity, the empty-prefix hash. The
    /// scan's own `restarted`, `resumed_from` and `consumed_through` still
    /// describe the file that is gone. Taking the generation from that reset
    /// cursor and leaving the offsets alone lets the write phase compare the
    /// replacement against its own empty-prefix identity, find it unchanged,
    /// and index the new file from the old file's resume point with none of
    /// the old evidence cleared. The two pieces of state have to move
    /// together.
    ///
    /// Positive control: before the fix this failed at `the replaced
    /// generation's evidence must be cleared: ["original.rs"]` — the old
    /// file's edit survived, and the replacement's first prompt was missing
    /// because it sits before the old resume offset.
    #[test]
    fn a_replacement_during_the_byte_scan_forces_a_full_rescan() {
        let dir = tempfile::tempdir().unwrap();
        // Long enough that the resume offset lands well inside the
        // replacement, so a skipped prefix is observable.
        let original = concat!(
            r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:37 PM (UTC-4)</timestamp><user_query>original first</user_query>"}]}}"#,
            "\n",
            r#"{"role":"assistant","message":{"content":[{"type":"tool_use","name":"Write","input":{"path":"original.rs"}}]}}"#,
            "\n",
            r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:39 PM (UTC-4)</timestamp><user_query>original second</user_query>"}]}}"#,
            "\n"
        );
        let transcript = write_cursor_transcript(dir.path(), "s-midscan", original);
        let root = dir.path().join(".cursor/projects");
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        super::sync_cursor(&conn, &mut state, &root).unwrap();
        assert_eq!(cursor_row_count(&conn, "tool_calls", "s-midscan"), 1);

        // Grow it so the next sync has something to resume for, then replace
        // it from inside that scan — after the reader has read, before it
        // validates what it read.
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .unwrap();
        file.write_all(
            concat!(
                r#"{"role":"assistant","message":{"content":[{"type":"text","text":"more original"}]}}"#,
                "\n"
            )
            .as_bytes(),
        )
        .unwrap();
        drop(file);

        let replacement = concat!(
            r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 4:01 PM (UTC-4)</timestamp><user_query>replacement first turn</user_query>"}]}}"#,
            "\n",
            r#"{"role":"assistant","message":{"content":[{"type":"tool_use","name":"Write","input":{"path":"replacement.rs"}}]}}"#,
            "\n",
            r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 4:04 PM (UTC-4)</timestamp><user_query>replacement second turn</user_query>"}]}}"#,
            "\n"
        );
        let mut replaced_once = false;
        super::sync_cursor_with_hooks(&conn, &mut state, &root, &mut |_| {}, &mut |path| {
            if path == transcript && !replaced_once {
                replaced_once = true;
                fs::write(&transcript, replacement).unwrap();
            }
        })
        .unwrap();
        assert!(replaced_once, "the replacement hook must have run");

        let edits: Vec<String> = crate::session_file_edits(&conn, "s-midscan", Some("cursor"))
            .unwrap()
            .into_iter()
            .map(|edit| edit.file_path)
            .collect();
        assert!(
            !edits.iter().any(|path| path == "original.rs"),
            "the replaced generation's evidence must be cleared: {edits:?}"
        );
        assert!(
            edits.iter().any(|path| path == "replacement.rs"),
            "the replacement's own evidence must be indexed: {edits:?}"
        );
        let prompts: Vec<String> = conn
            .prepare(
                "SELECT prompt FROM history WHERE source = 'cursor' \
                 AND session_id = 's-midscan' ORDER BY id",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            prompts,
            vec![
                "replacement first turn".to_string(),
                "replacement second turn".to_string()
            ],
            "history_from_offset must come from the re-scan, so the \
             replacement's prefix is indexed and the old file's prompts go"
        );
    }

    /// Group N's control: an *append* landing in the same window is not a
    /// replacement. Its scanned prefix is untouched, so the scan keeps its
    /// resume point and the session is not rebuilt — the fix must not turn
    /// every concurrent write into a full re-scan.
    #[test]
    fn an_append_during_the_byte_scan_still_resumes() {
        let dir = tempfile::tempdir().unwrap();
        let original = concat!(
            r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:37 PM (UTC-4)</timestamp><user_query>first</user_query>"}]}}"#,
            "\n",
            r#"{"role":"assistant","message":{"content":[{"type":"tool_use","name":"Write","input":{"path":"kept.rs"}}]}}"#,
            "\n"
        );
        let transcript = write_cursor_transcript(dir.path(), "s-append-race", original);
        let root = dir.path().join(".cursor/projects");
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        super::sync_cursor(&conn, &mut state, &root).unwrap();
        let first_edit_id: i64 = conn
            .query_row(
                "SELECT MIN(id) FROM file_edits WHERE source = 'cursor' \
                 AND session_id = 's-append-race'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .unwrap();
        file.write_all(
            concat!(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:40 PM (UTC-4)</timestamp><user_query>second</user_query>"}]}}"#,
                "\n"
            )
            .as_bytes(),
        )
        .unwrap();
        drop(file);

        super::sync_cursor_with_hooks(&conn, &mut state, &root, &mut |_| {}, &mut |path| {
            if path == transcript {
                let mut file = fs::OpenOptions::new().append(true).open(path).unwrap();
                file.write_all(
                    concat!(
                        r#"{"role":"assistant","message":{"content":[{"type":"text","text":"appended mid-scan"}]}}"#,
                        "\n"
                    )
                    .as_bytes(),
                )
                .unwrap();
            }
        })
        .unwrap();

        let prompts: Vec<String> = conn
            .prepare(
                "SELECT prompt FROM history WHERE source = 'cursor' \
                 AND session_id = 's-append-race' ORDER BY id",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(prompts, vec!["first".to_string(), "second".to_string()]);
        let still_first: i64 = conn
            .query_row(
                "SELECT MIN(id) FROM file_edits WHERE source = 'cursor' \
                 AND session_id = 's-append-race'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            still_first, first_edit_id,
            "an append must not clear and re-insert the session's evidence"
        );
    }

    /// Group O. A user-role record carrying only a `tool_result` is a tool
    /// response, not a human turn, so it must not close the open turn's time.
    ///
    /// Cursor writes tool results back as user-role records. Treating every
    /// user record as a new turn cleared the inherited timestamp, and the
    /// result then fell through to the file mtime — so a tool result was
    /// dated minutes or hours after the call it answers, with the two halves
    /// of one exchange disagreeing.
    ///
    /// Positive control: before the fix this failed at `a tool result must
    /// carry its call's time: left: 1789600000000, right: 1789587420000` —
    /// the result took the file mtime.
    #[test]
    fn a_user_role_tool_result_keeps_its_calls_timestamp() {
        let dir = tempfile::tempdir().unwrap();
        let transcript = write_cursor_transcript(
            dir.path(),
            "s-tool-result-ts",
            concat!(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:37 PM (UTC-4)</timestamp><user_query>run it</user_query>"}]}}"#,
                "\n",
                r#"{"role":"assistant","message":{"content":[{"type":"tool_use","id":"toolu_1","name":"Bash","input":{"command":"ls"}}]}}"#,
                "\n",
                r#"{"role":"user","message":{"content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"a.rs"}]}}"#,
                "\n",
                r#"{"role":"user","message":{"content":[{"type":"text","text":"undated follow-up"}]}}"#,
                "\n"
            ),
        );
        // A much later mtime: the fallback any untimed turn takes.
        set_file_mtime_ms(&transcript, 1_789_600_000_000);
        let root = dir.path().join(".cursor/projects");
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        super::sync_cursor(&conn, &mut state, &root).unwrap();

        let ts_of = |kind: &str| -> i64 {
            conn.query_row(
                "SELECT ts_ms FROM session_events WHERE source = 'cursor' \
                 AND session_id = 's-tool-result-ts' AND kind = ? ORDER BY id LIMIT 1",
                [kind],
                |row| row.get(0),
            )
            .unwrap()
        };
        assert_eq!(
            ts_of("tool_result"),
            ts_of("tool_use"),
            "a tool result must carry its call's time"
        );

        // Control: a real human turn with no time of its own still opens a
        // new turn, and still falls through to the mtime.
        let follow_up: i64 = conn
            .query_row(
                "SELECT timestamp_ms FROM history WHERE source = 'cursor' \
                 AND session_id = 's-tool-result-ts' AND prompt = 'undated follow-up'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            follow_up, 1_789_600_000_000,
            "an undated human turn must still open a new turn and take the mtime"
        );
    }

    /// Group P. A record this pass emits no evidence for must not move the
    /// session's activity window.
    ///
    /// On an incremental read, a pre-resume record whose original timestamp
    /// cannot be recovered falls back to the *current* mtime — and the mtime
    /// moves on every append. A `turn_ended` marker is exactly that record:
    /// it stores no event, so there is nothing to recover its time from, and
    /// the window then advanced to "now" on every single sync, burying the
    /// times the session actually recorded.
    ///
    /// Positive control: before the fix this failed at `the window must come
    /// from the session's own evidence, not the current mtime: left:
    /// 1789602000000, right: 1789600260000`.
    #[test]
    fn a_record_with_no_evidence_does_not_move_the_activity_window() {
        let dir = tempfile::tempdir().unwrap();
        let transcript = write_cursor_transcript(
            dir.path(),
            "s-marker-window",
            concat!(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"undated opening turn"}]}}"#,
                "\n",
                r#"{"role":"assistant","message":{"content":[{"type":"turn_ended","status":"success"}]}}"#,
                "\n"
            ),
        );
        set_file_mtime_ms(&transcript, 1_789_587_420_000);
        let root = dir.path().join(".cursor/projects");
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        super::sync_cursor(&conn, &mut state, &root).unwrap();
        let last_activity = |conn: &Connection| -> i64 {
            conn.query_row(
                "SELECT last_activity_ms FROM sessions WHERE source = 'cursor' \
                 AND session_id = 's-marker-window'",
                [],
                |row| row.get(0),
            )
            .unwrap()
        };
        assert_eq!(last_activity(&conn), 1_789_587_420_000);

        // Append a dated turn, and move the mtime well past it.
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .unwrap();
        file.write_all(
            concat!(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:51 PM (UTC-4)</timestamp><user_query>dated turn</user_query>"}]}}"#,
                "\n"
            )
            .as_bytes(),
        )
        .unwrap();
        drop(file);
        set_file_mtime_ms(&transcript, 1_789_600_000_000);
        super::sync_cursor(&conn, &mut state, &root).unwrap();

        let dated: i64 = conn
            .query_row(
                "SELECT timestamp_ms FROM history WHERE source = 'cursor' \
                 AND session_id = 's-marker-window' AND prompt = 'dated turn'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            last_activity(&conn),
            dated,
            "the window must come from the session's own evidence, not the \
             current mtime"
        );

        // Control: an undated turn that *does* emit evidence is stamped with
        // the mtime and must still carry the window forward.
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .unwrap();
        file.write_all(
            concat!(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"undated closing turn"}]}}"#,
                "\n"
            )
            .as_bytes(),
        )
        .unwrap();
        drop(file);
        set_file_mtime_ms(&transcript, 1_789_602_000_000);
        super::sync_cursor(&conn, &mut state, &root).unwrap();
        assert_eq!(
            last_activity(&conn),
            1_789_602_000_000,
            "an undated record that does emit evidence still sets the window"
        );
    }

    /// Group P's other half. Dropping the markers is not enough: a pre-resume
    /// record that *does* emit evidence but whose original stamp can no
    /// longer be recovered is stamped with the current mtime, and that stamp
    /// is a guess, not a record of when anything happened. Letting it set the
    /// window redates the session to "now" for a record the scan is only
    /// re-reading.
    ///
    /// Positive control: with the window gated on evidence alone this failed
    /// at `a guessed stamp must not become the session's recency: left:
    /// 1789600000000, right: 1789588260000`.
    #[test]
    fn a_guessed_stamp_for_a_pre_resume_record_does_not_move_the_window() {
        let dir = tempfile::tempdir().unwrap();
        let transcript = write_cursor_transcript(
            dir.path(),
            "s-guessed-window",
            concat!(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"undated opening turn"}]}}"#,
                "\n"
            ),
        );
        set_file_mtime_ms(&transcript, 1_789_587_420_000);
        let root = dir.path().join(".cursor/projects");
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let mut state = Map::new();
        super::sync_cursor(&conn, &mut state, &root).unwrap();

        // Append a dated turn and move the mtime well past it.
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .unwrap();
        file.write_all(
            concat!(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:51 PM (UTC-4)</timestamp><user_query>dated turn</user_query>"}]}}"#,
                "\n"
            )
            .as_bytes(),
        )
        .unwrap();
        drop(file);
        set_file_mtime_ms(&transcript, 1_789_600_000_000);

        // Lose the opening turn's stored events, the only place its stamp
        // survives. The next pass re-reads that record from before its resume
        // point with nothing left to recover its time from.
        conn.execute(
            "DELETE FROM session_events WHERE source = 'cursor' \
             AND session_id = 's-guessed-window' AND substr(event_uid, 1, 2) = '0:'",
            [],
        )
        .unwrap();
        super::sync_cursor(&conn, &mut state, &root).unwrap();

        let last_activity: i64 = conn
            .query_row(
                "SELECT last_activity_ms FROM sessions WHERE source = 'cursor' \
                 AND session_id = 's-guessed-window'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            last_activity, 1_789_588_260_000,
            "a guessed stamp must not become the session's recency"
        );
    }

    /// Group Q. Two `sync_cursor` runs cannot interleave a rebuild, because
    /// they cannot both run.
    ///
    /// The concern is real in shape: a rebuild clears a session's evidence and
    /// re-indexes it, and checkpoint merging keeps the *newer* offset, so an
    /// older scan that cleared evidence a newer scan had already committed
    /// would leave those records skipped permanently. What makes it
    /// unreachable is that `sync_cursor` is private and has exactly one
    /// non-test caller, `sync_basic`, which in turn has exactly two:
    /// `sync_exclusive_with_home` and `prepare_local_sync_snapshot`. Both
    /// acquire `SyncRunLock` — an exclusive advisory lock on
    /// `<canonical-db>.sync.lock` — *before* opening the database, and hold it
    /// for the whole call. The second sync does not queue behind the first and
    /// proceed later against stale state; `try_lock_exclusive` returns
    /// `WouldBlock`, and the run reports "another sync is already running" and
    /// does nothing at all.
    ///
    /// This asserts the consequence that matters — a contended sync performs
    /// no rebuild — rather than the lock mechanics, which
    /// `sync_lock_canonicalizes_aliases_and_blocks_every_sync_entry_point_before_open`
    /// already covers.
    #[test]
    fn a_contended_sync_cannot_rebuild_a_cursor_session() {
        let home = tempfile::tempdir().unwrap();
        let db_path = home.path().join("history.db");
        let original = concat!(
            r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:37 PM (UTC-4)</timestamp><user_query>first</user_query>"}]}}"#,
            "\n",
            r#"{"role":"assistant","message":{"content":[{"type":"tool_use","name":"Write","input":{"path":"original.rs"}}]}}"#,
            "\n"
        );
        let transcript = write_cursor_transcript(home.path(), "s-contended", original);

        assert!(sync_local_at_with_home(&db_path, home.path()).unwrap());
        let conn = open_db(&db_path).unwrap();
        assert_eq!(cursor_row_count(&conn, "tool_calls", "s-contended"), 1);
        drop(conn);

        // Rewrite the transcript so the next sync *would* rebuild: a different
        // prefix is a new generation, which clears the session's evidence.
        fs::write(
            &transcript,
            concat!(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 4:01 PM (UTC-4)</timestamp><user_query>rewritten</user_query>"}]}}"#,
                "\n"
            ),
        )
        .unwrap();

        // With the run lock held, that rebuild must not happen.
        let owner = try_acquire_sync_lock(&db_path).unwrap().unwrap();
        assert!(
            !sync_local_at_with_home(&db_path, home.path()).unwrap(),
            "a contended sync must report that it was skipped"
        );
        let conn = open_db(&db_path).unwrap();
        assert_eq!(
            cursor_row_count(&conn, "tool_calls", "s-contended"),
            1,
            "a contended sync must not clear evidence it did not re-index"
        );
        let prompts: Vec<String> = conn
            .prepare(
                "SELECT prompt FROM history WHERE source = 'cursor' \
                 AND session_id = 's-contended' ORDER BY id",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(prompts, vec!["first".to_string()]);
        drop(conn);

        // Control: the same sync, uncontended, does perform the rebuild — so
        // the assertion above is about the lock and not about a transcript
        // that was never going to be rebuilt.
        drop(owner);
        assert!(sync_local_at_with_home(&db_path, home.path()).unwrap());
        let conn = open_db(&db_path).unwrap();
        let prompts: Vec<String> = conn
            .prepare(
                "SELECT prompt FROM history WHERE source = 'cursor' \
                 AND session_id = 's-contended' ORDER BY id",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(prompts, vec!["rewritten".to_string()]);
        let edits = crate::session_file_edits(&conn, "s-contended", Some("cursor")).unwrap();
        assert!(
            !edits.iter().any(|edit| edit.file_path == "original.rs"),
            "the uncontended rebuild must clear the replaced generation"
        );
    }

    /// Group R. A turn whose zone is malformed is an *undated* turn.
    ///
    /// The point of rejecting a bad zone is not tidiness: a wrong-but-plausible
    /// instant is indistinguishable from a recorded one, so it silently
    /// suppresses the mtime fallback and with it the
    /// `CURSOR_TIMESTAMP_FROM_MTIME` diagnostic that exists to say "this turn
    /// was never dated". This asserts that consequence, not just the parser.
    ///
    /// Positive control: before the fix, `used_mtime_fallback` was `false` for
    /// both turns and the doubled-sign turn was stamped `1789558620000` — an
    /// instant eight hours from the one its tag names.
    #[test]
    fn a_turn_with_a_malformed_zone_falls_back_to_the_mtime() {
        let dir = tempfile::tempdir().unwrap();
        let transcript = write_cursor_transcript(
            dir.path(),
            "s-bad-zone",
            concat!(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:37 PM (UTC--4)</timestamp><user_query>doubled sign</user_query>"}]}}"#,
                "\n",
                r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:39 PM (UTC-4</timestamp><user_query>unterminated</user_query>"}]}}"#,
                "\n",
                r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:41 PM (UTC-4)</timestamp><user_query>well formed</user_query>"}]}}"#,
                "\n"
            ),
        );
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let outcome = super::ingest_cursor_transcript(
            &conn,
            &transcript,
            "s-bad-zone",
            None,
            4_242,
            0,
            u64::MAX,
        )
        .unwrap();

        assert!(
            outcome.used_mtime_fallback,
            "a malformed zone must leave the turn undated so the diagnostic fires"
        );
        let prompts: Vec<(String, i64)> = conn
            .prepare(
                "SELECT prompt, timestamp_ms FROM history WHERE source = 'cursor' \
                 AND session_id = 's-bad-zone' ORDER BY id",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            prompts,
            vec![
                ("doubled sign".to_string(), 4_242),
                ("unterminated".to_string(), 4_242),
                // Control: the well-formed tag beside them still parses, so
                // this is about the malformed zones and not about the parser
                // having stopped reading timestamps altogether.
                ("well formed".to_string(), 1_789_587_660_000),
            ]
        );
    }

    /// Assistant prose that *quotes* a `<timestamp>` tag is prose, not a clock.
    ///
    /// The tag is injected by Cursor's client into what a person submits, so
    /// only a human turn's own text carries one. Scanning every role's blocks
    /// let a model explaining the transcript format, or reading a log back,
    /// supply a `record_ts` — which re-dated that record and every record
    /// after it until the next turn, and suppressed the mtime fallback that
    /// should have fired.
    ///
    /// Positive control: with the scan unrestricted this fails at
    /// `assistant prose must not re-date the turn: left: [("the reply",
    /// 1789587660000)], right: [("the reply", 1789587420000)]` — the reply
    /// jumped four minutes to the instant it was merely quoting.
    #[test]
    fn a_quoted_timestamp_in_assistant_prose_is_not_a_clock() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s-quoted.jsonl");
        fs::write(
            &path,
            concat!(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:37 PM (UTC-4)</timestamp>\n<user_query>when was this?</user_query>"}]}}"#,
                "\n",
                r#"{"role":"assistant","message":{"content":[{"type":"text","text":"the reply"},{"type":"text","text":"Cursor writes <timestamp>Wednesday, Sep 16, 2026, 3:41 PM (UTC-4)</timestamp> into the turn."}]}}"#,
                "\n",
            ),
        )
        .unwrap();
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let outcome = super::ingest_cursor_transcript(
            &conn,
            &path,
            "s-quoted",
            Some("/tmp/proj"),
            4_242,
            0,
            u64::MAX,
        )
        .unwrap();

        let replies: Vec<(String, i64)> = conn
            .prepare(
                "SELECT text, ts_ms FROM session_events WHERE source = 'cursor' \
                 AND session_id = 's-quoted' AND role = 'assistant' \
                 AND text = 'the reply' ORDER BY id",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            replies,
            vec![("the reply".to_string(), 1_789_587_420_000)],
            "assistant prose must not re-date the turn"
        );
        // The window closes at the human turn's time, not at the one the
        // assistant quoted.
        assert_eq!(outcome.last_ts_ms, Some(1_789_587_420_000));
    }

    /// A human turn is one `history` row, however many text blocks Cursor
    /// split it into.
    ///
    /// Reported by Devin against the merge head as "repeated Cursor prompts
    /// are dropped". `history`'s identity is `(source, timestamp_ms, prompt)`
    /// and the blocks of one record all share that record's turn time, so
    /// inserting a row per block meant two blocks carrying the same text
    /// collided on `INSERT OR IGNORE` and stored one row for two events. The
    /// tables then disagreed about how many times the person said it, and the
    /// searchable copy was the one that lost.
    ///
    /// Joining the turn's blocks fixes both halves: nothing a person wrote
    /// goes unindexed, and one turn is one row whatever its blocks contain.
    /// `session_events` still keeps the blocks apart, because that is what the
    /// record says.
    ///
    /// Positive control: against the per-block insert this fails at
    /// `a repeated block must not vanish from history: left: ["first half",
    /// "second half", "same words"], right: ["first half\n\nsecond half",
    /// "same words\n\nsame words"]` -- three rows for four blocks, with the
    /// second `same words` silently gone.
    #[test]
    fn a_cursor_turn_is_one_prompt_however_many_blocks_it_has() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s-blocks.jsonl");
        fs::write(
            &path,
            concat!(
                r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:37 PM (UTC-4)</timestamp>\n<user_query>first half</user_query>"},{"type":"text","text":"second half"}]}}"#,
                "\n",
                r#"{"role":"user","message":{"content":[{"type":"text","text":"<timestamp>Wednesday, Sep 16, 2026, 3:41 PM (UTC-4)</timestamp>\n<user_query>same words</user_query>"},{"type":"text","text":"same words"}]}}"#,
                "\n",
            ),
        )
        .unwrap();
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        super::ingest_cursor_transcript(
            &conn,
            &path,
            "s-blocks",
            Some("/tmp/proj"),
            4_242,
            0,
            u64::MAX,
        )
        .unwrap();

        let prompts: Vec<String> = conn
            .prepare(
                "SELECT prompt FROM history WHERE source = 'cursor' \
                 AND session_id = 's-blocks' ORDER BY id",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            prompts,
            vec![
                "first half\n\nsecond half".to_string(),
                // The duplicate turn keeps both copies, in one row, instead of
                // losing the repetition to the unique index.
                "same words\n\nsame words".to_string(),
            ],
            "a repeated block must not vanish from history"
        );

        // The events are unchanged: one row per block, both copies present.
        let texts: Vec<String> = conn
            .prepare(
                "SELECT text FROM session_events WHERE source = 'cursor' \
                 AND session_id = 's-blocks' ORDER BY id",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            texts,
            vec![
                "first half".to_string(),
                "second half".to_string(),
                "same words".to_string(),
                "same words".to_string(),
            ],
            "session_events keeps the blocks the record actually carried"
        );
    }

    /// Reproduce a database synced by a release without continuity: the events
    /// and the sync stamps are there, the evidence table that release never
    /// wrote is not. Deleting the row is exactly the pre-upgrade state, and it
    /// is the only thing these two tests fake.
    fn forget_continuity_evidence(conn: &Connection) {
        conn.execute("DELETE FROM session_continuity_evidence", [])
            .unwrap();
        let left: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_continuity_evidence",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(left, 0);
    }

    #[test]
    fn an_upgraded_claude_install_backfills_continuity_without_a_generation_reset() {
        let dir = tempfile::tempdir().unwrap();
        let projects = dir.path().join("projects/app");
        std::fs::create_dir_all(&projects).unwrap();
        // Two transcripts, the second continuing the first across files.
        std::fs::write(
            projects.join("origin.jsonl"),
            concat!(
                "{\"sessionId\":\"origin\",\"uuid\":\"origin-u\",\"parentUuid\":null,",
                "\"type\":\"user\",\"cwd\":\"/work/app\",\"message\":{\"role\":\"user\",",
                "\"content\":\"start\"},\"timestamp\":\"2026-08-31T10:00:00Z\"}\n",
                "{\"sessionId\":\"origin\",\"uuid\":\"origin-a\",\"parentUuid\":\"origin-u\",",
                "\"type\":\"assistant\",\"cwd\":\"/work/app\",\"message\":{\"role\":\"assistant\",",
                "\"content\":\"on it\"},\"timestamp\":\"2026-08-31T10:00:01Z\"}\n",
            ),
        )
        .unwrap();
        std::fs::write(
            projects.join("continued.jsonl"),
            concat!(
                "{\"sessionId\":\"continued\",\"uuid\":\"cont-u\",\"parentUuid\":\"origin-a\",",
                "\"type\":\"user\",\"cwd\":\"/work/app\",\"message\":{\"role\":\"user\",",
                "\"content\":\"carry on\"},\"timestamp\":\"2026-08-31T11:00:00Z\"}\n",
            ),
        )
        .unwrap();
        let conn = open_db(&dir.path().join("history.db")).unwrap();
        let mut state = Map::new();
        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();
        let generation = state
            .get("claude_sessions_v3")
            .and_then(Value::as_object)
            .cloned()
            .unwrap();
        assert_eq!(generation.len(), 2, "both transcripts are stamped");

        forget_continuity_evidence(&conn);
        conn.execute(
            "DELETE FROM session_relationships WHERE relationship = 'continuation'",
            [],
        )
        .unwrap();

        // A plain sync, with every stamp still matching. Without the fast-path
        // predicate this walks straight past both files and the continuation
        // is never recorded; with it, each is re-read exactly once.
        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();
        let banked: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_continuity_evidence",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(banked, 2);
        let edge: (String, Option<String>) = conn
            .query_row(
                "SELECT parent_session_id, child_session_id FROM session_relationships \
                 WHERE relationship = 'continuation'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(edge, ("origin".to_string(), Some("continued".to_string())));
        assert_eq!(
            state
                .get("claude_sessions_v3")
                .and_then(Value::as_object)
                .unwrap(),
            &generation,
            "the stamp map is untouched: this is a repair, not a generation reset"
        );

        // The files are back on the fast path. A sentinel written into the
        // indexed events survives the next sync, which proves nothing is
        // re-read forever.
        conn.execute(
            "UPDATE session_events SET text = 'sentinel' WHERE session_id = 'origin' AND role = 'user'",
            [],
        )
        .unwrap();
        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();
        let survived: String = conn
            .query_row(
                "SELECT text FROM session_events WHERE session_id = 'origin' AND role = 'user'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(survived, "sentinel");
    }

    #[test]
    fn an_upgraded_codex_install_backfills_continuity_without_a_generation_reset() {
        let dir = tempfile::tempdir().unwrap();
        let day = dir.path().join(".codex/sessions/2026/08/31");
        std::fs::create_dir_all(&day).unwrap();
        std::fs::write(
            day.join("rollout-forked.jsonl"),
            concat!(
                "{\"timestamp\":\"2026-08-31T10:00:00Z\",\"type\":\"session_meta\",",
                "\"payload\":{\"id\":\"forked\",\"cwd\":\"/work/app\",\"cli_version\":\"0.148.0\",",
                "\"continuedFromSessionId\":\"prior-thread\"}}\n",
                "{\"timestamp\":\"2026-08-31T10:00:01Z\",\"type\":\"event_msg\",",
                "\"payload\":{\"type\":\"user_message\",\"message\":\"go\"}}\n",
            ),
        )
        .unwrap();
        let conn = open_db(&dir.path().join("history.db")).unwrap();
        let mut state = Map::new();
        sync_codex(&conn, &mut state, &dir.path().join(".codex")).unwrap();
        let generation = state.get("codex_rollouts_v5").cloned().unwrap();

        forget_continuity_evidence(&conn);
        conn.execute(
            "DELETE FROM session_relationships WHERE relationship = 'continuation'",
            [],
        )
        .unwrap();

        // The rollout is unchanged and its events are present, so the skip
        // path takes it. Continuity still has to be backfilled from the one
        // `session_meta` line it lives on.
        sync_codex(&conn, &mut state, &dir.path().join(".codex")).unwrap();
        let banked: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_continuity_evidence WHERE source = 'codex'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(banked, 1);
        let edge: (String, Option<String>) = conn
            .query_row(
                "SELECT parent_session_id, child_session_id FROM session_relationships \
                 WHERE relationship = 'continuation'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            edge,
            ("prior-thread".to_string(), Some("forked".to_string()))
        );
        assert_eq!(
            state.get("codex_rollouts_v5").unwrap(),
            &generation,
            "the stamp map is untouched: this is a repair, not a generation reset"
        );

        // Back on the skip path: a sentinel in the indexed events survives.
        conn.execute(
            "UPDATE session_events SET text = 'sentinel' WHERE session_id = 'forked' AND role = 'user'",
            [],
        )
        .unwrap();
        sync_codex(&conn, &mut state, &dir.path().join(".codex")).unwrap();
        let survived: String = conn
            .query_row(
                "SELECT text FROM session_events WHERE session_id = 'forked' AND role = 'user'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(survived, "sentinel");
    }

    #[test]
    fn an_upgrade_backfills_both_branches_that_share_one_session_id() {
        // The upgrade path in the one case continuity exists to catch. Two
        // transcripts carrying one in-log session id are a single catalog row,
        // so they reach the backfill by different routes — the one the row
        // names through the fast path, the other by falling through — and the
        // fork is only detected if *both* end up with evidence.
        let dir = tempfile::tempdir().unwrap();
        let projects = dir.path().join("projects/app");
        std::fs::create_dir_all(&projects).unwrap();
        for branch in ["branch-a", "branch-b"] {
            std::fs::write(
                projects.join(format!("{branch}.jsonl")),
                format!(
                    "{{\"sessionId\":\"shared\",\"uuid\":\"{branch}-u\",\"parentUuid\":null,\
                     \"type\":\"user\",\"cwd\":\"/work/app\",\"message\":{{\"role\":\"user\",\
                     \"content\":\"{branch}\"}},\"timestamp\":\"2026-08-31T10:00:00Z\"}}\n"
                ),
            )
            .unwrap();
        }
        let conn = open_db(&dir.path().join("history.db")).unwrap();
        let mut state = Map::new();
        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();
        let raw_paths: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sessions WHERE source = 'claude'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(raw_paths, 1, "both branches are one catalog row");

        forget_continuity_evidence(&conn);
        conn.execute(
            "DELETE FROM session_relationships WHERE relationship = 'fork'",
            [],
        )
        .unwrap();

        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();
        let banked: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_continuity_evidence",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            banked, 2,
            "both transcripts were read, not just the one the catalog names"
        );
        let forks: Vec<String> = conn
            .prepare(
                "SELECT relationship_uid FROM session_relationships \
                 WHERE relationship = 'fork' ORDER BY relationship_uid",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(forks, vec!["fork:branch-a", "fork:branch-b"]);
    }

    #[test]
    fn an_upgrade_never_turns_a_subagent_sidecar_into_a_fork_branch() {
        // A sidecar carries its parent's `sessionId` while living in its own
        // file, which is byte-for-byte the shape the fork inference reads as
        // "two transcripts claiming one origin". It is not a session, it is
        // not a branch, and it must never reach the continuity capture — on
        // the ingest path (where the subagent branch returns before it) or on
        // the upgrade path, which runs before that classification.
        let dir = tempfile::tempdir().unwrap();
        let projects = dir.path().join("projects/app");
        std::fs::create_dir_all(&projects).unwrap();
        std::fs::write(
            projects.join("parent.jsonl"),
            concat!(
                "{\"sessionId\":\"parent\",\"uuid\":\"p-u\",\"parentUuid\":null,\"type\":\"user\",",
                "\"cwd\":\"/work/app\",\"message\":{\"role\":\"user\",\"content\":\"do it\"},",
                "\"timestamp\":\"2026-08-31T10:00:00Z\"}\n",
            ),
        )
        .unwrap();
        // Two sidecars, so a naive capture would group them into a fork pair.
        for agent in ["agent-one", "agent-two"] {
            std::fs::write(
                projects.join(format!("{agent}.jsonl")),
                format!(
                    "{{\"sessionId\":\"parent\",\"uuid\":\"{agent}-a\",\"isSidechain\":true,\
                     \"type\":\"assistant\",\"cwd\":\"/work/app\",\
                     \"message\":{{\"role\":\"assistant\",\"content\":\"{agent} result\"}},\
                     \"timestamp\":\"2026-08-31T10:00:01Z\"}}\n"
                ),
            )
            .unwrap();
        }
        let conn = open_db(&dir.path().join("history.db")).unwrap();
        let mut state = Map::new();
        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();

        // Reproduce the pre-upgrade state, then sync again: this is the pass
        // that used to read every file lacking an evidence row, sidecars
        // included.
        forget_continuity_evidence(&conn);
        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();

        let captured: Vec<String> = conn
            .prepare("SELECT locator FROM session_continuity_evidence ORDER BY locator")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            captured,
            vec![projects.join("parent.jsonl").to_string_lossy().to_string()],
            "only the session's own transcript is captured"
        );
        let forks: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_relationships WHERE relationship = 'fork'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(forks, 0, "sidecars are delegation, never branches");
        // The positive control: the delegation the sidecars really are is
        // still recorded, so this is not an empty-database pass.
        let delegated: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_relationships WHERE relationship = 'delegated'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(delegated, 2);
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
