use ai_hist_core::convergence::MachineIdentity;
use ai_hist_core::{
    default_db_path, import_json, insert_history, normalize_tag_name, open_db, open_db_readonly,
    parse_cursor_text, prompt_hash, raw_fts_query_error, recent, resume_command,
    schema_is_catalog_read_current, schema_is_current, search, session, session_events,
    session_file_edits, session_tool_calls, sync_opencode_db, sync_opencode_session, untag_session,
    HistoryEntry, QueryFilter, SourceDatabaseError, SOURCE_CHOICES,
};
pub use ai_hist_core::{SessionLocation, SessionScope};
use anyhow::{Context, Result};
use chrono::{Local, TimeZone};
use clap::{Args, Parser, Subcommand};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use rusqlite::{params, Connection, ErrorCode};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{self, BufRead, BufReader, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

mod cloud;
mod codex;
/// Fast, shallow coding-agent session discovery and the cache-only catalog
/// listing that backs it.
pub mod discover;
mod hydrate;
mod learn;
/// Recording the delegation evidence each provider leaves behind.
mod relationships;
/// Remote session connectors (claude.ai/code web sessions, Codex cloud tasks)
/// and their availability reporting.
pub mod remote;
mod replay;

pub use discover::{
    discover_sessions, discover_sessions_collect, discover_sessions_with_env,
    discover_sessions_with_providers, list_session_catalog, list_session_catalog_page,
    shallow_providers, validate_discovery_scope, AllProvidersFailed, Candidate, CatalogCursor,
    CatalogListOptions, DiscoverOptions, DiscoveryCounters, DiscoveryDiagnostic, DiscoveryEnv,
    DiscoverySummary, ProviderSummary, ScanEnv, SessionCatalogPage, ShallowReadAccess,
    ShallowSession, ShallowSessionProvider, SourceExemption, DEFAULT_CATALOG_LIMIT,
    DISCOVERY_EXEMPTIONS, SESSION_CATALOG_CONTRACT_VERSION, SHALLOW_SCANNER_VERSION,
};
pub use hydrate::{
    hydrate_session, hydrate_session_at, HydrateSessionOptions, HydrateSessionResult,
    HydrationDiagnostic, HydrationEvidence, HydrationIndexedThrough,
    SESSION_HYDRATION_CONTRACT_VERSION,
};
pub(crate) use relationships::now_ms;
pub use relationships::{record_relationship, ObservedRelationship};

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};

/// When set, sync progress lines (`[claude] +N rows`, …) are suppressed. The
/// in-process library API ([`sync_and_push`]) sets this so an embedding host's
/// stdout isn't spammed; the CLI leaves it false.
static SYNC_QUIET: AtomicBool = AtomicBool::new(false);

/// `println!` for sync progress that honors [`SYNC_QUIET`].
macro_rules! sync_note {
    ($($arg:tt)*) => {
        if !$crate::SYNC_QUIET.load($crate::AtomicOrdering::Relaxed) {
            println!($($arg)*);
        }
    };
}

/// Result of an in-process [`sync_and_push`] run.
pub struct SyncPushOutcome {
    pub sent: u64,
    pub accepted: u64,
    /// `false` when there's no stored relayhistory auth yet (treated as a no-op
    /// rather than an error, for background callers).
    pub authenticated: bool,
    /// `true` when another process owned the scan lock. Already-indexed rows are still pushed.
    pub sync_skipped: bool,
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

/// Full ingestion for a selected scope into the default database.
pub fn sync_scoped(scope: SessionScope) -> Result<bool> {
    sync_scoped_at(&default_db_path(), scope)
}

/// Full ingestion for a selected session-presence scope.
///
/// Remote provider connectors are intentionally a capability boundary: a
/// `remote` request fails loudly unless at least one connector (claude.ai/code
/// web sessions, Codex cloud tasks — see [`remote`]) is configured on this
/// machine, and `all` runs local ingestion plus every configured connector.
/// Remote connectors acquire catalog rows and `remote` presences; remote rows
/// stay `shallow` because neither provider serves full transcripts through a
/// supported listing interface yet.
pub fn sync_scoped_at(db_path: &Path, scope: SessionScope) -> Result<bool> {
    SYNC_QUIET.store(true, AtomicOrdering::Relaxed);
    sync_scope_exclusive(db_path, scope)
}

fn sync_scope_exclusive(db_path: &Path, scope: SessionScope) -> Result<bool> {
    validate_sync_scope(scope)?;
    let mut ran = false;
    if matches!(scope, SessionScope::Local | SessionScope::All) {
        ran |= sync_exclusive(db_path)?;
    }
    if matches!(scope, SessionScope::Remote | SessionScope::All) {
        ran |= sync_remote_connectors(db_path, scope)?;
    }
    Ok(ran)
}

fn validate_sync_scope(scope: SessionScope) -> Result<()> {
    if scope == SessionScope::Remote {
        remote::ensure_remote_connectors_configured("sync")?;
    }
    Ok(())
}

/// Run every configured remote connector's acquisition into the ledger.
///
/// This is the discovery engine at remote scope with no row cap: stamp-guarded
/// catalog upserts plus `remote` presences, so it deliberately does not take
/// the sync advisory lock (the same concurrency argument as [`discover`]).
/// Under `all` scope a machine with no connector configured skips quietly —
/// that is the documented "runs whatever is available" contract; a remote-only
/// request was already rejected by [`validate_sync_scope`] before this point.
fn sync_remote_connectors(db_path: &Path, scope: SessionScope) -> Result<bool> {
    let statuses = remote::remote_connector_statuses();
    if !statuses.iter().any(|status| status.configured) {
        if scope == SessionScope::All {
            sync_note!("  [remote] no remote provider connectors configured; skipped");
            return Ok(false);
        }
        remote::ensure_remote_connectors_configured("sync")?;
    }
    let conn = open_db(db_path).map_err(|error| enrich_sync_error(db_path, error))?;
    let options = DiscoverOptions {
        scope: SessionScope::Remote,
        sources: Vec::new(),
        limit: None,
    };
    let summary = discover_sessions(&conn, &options, |_| {})?;
    // `all` ingests local transcripts before remote discovery. Correlate from
    // the exact ids cached by that local pass; remote-only sync never opens a
    // local transcript merely to repair topology.
    reconcile_claude_remote_relationships(&conn)?;
    for (source, provider) in &summary.providers {
        sync_note!(
            "  [remote:{source}] {} session(s): {} discovered, {} unchanged",
            provider.candidates,
            provider.discovered,
            provider.skipped_unchanged
        );
    }
    for diagnostic in &summary.diagnostics {
        sync_note!(
            "  [remote:{}] {}: {}",
            diagnostic.source,
            diagnostic.locator.as_deref().unwrap_or("(connector)"),
            diagnostic.error
        );
    }
    Ok(true)
}

/// Sync local agent history into the DB, then push new records to
/// relayhistory-cloud — the in-process equivalent of `ai-hist sync && ai-hist
/// push`, with sync progress output suppressed. This is the entry point the
/// napi binding exposes so a host (e.g. the Agent Relay runtime) can capture
/// without spawning the CLI.
pub fn sync_and_push() -> Result<SyncPushOutcome> {
    SYNC_QUIET.store(true, AtomicOrdering::Relaxed);

    let db_path = default_db_path();
    let (conn, sync_skipped) = prepare_sync_and_push_db(&db_path)?;

    // The in-process runtime has no CLI argument channel. Keep it pinned to the normal Cloud
    // origin rather than following whichever stage happened to be logged into most recently.
    let default_base_url = cloud::default_base_url();
    let auth = match cloud::load_auth(Some(&default_base_url))? {
        Some(auth) => auth,
        None => {
            return Ok(SyncPushOutcome {
                sent: 0,
                accepted: 0,
                authenticated: false,
                sync_skipped,
            })
        }
    };
    let machine = MachineIdentity {
        id: cloud::machine_id()?,
        hostname: cloud::machine_hostname(),
        os: Some(std::env::consts::OS.to_string()),
        cli_version: Some(env!("CARGO_PKG_VERSION").to_string()),
        ..Default::default()
    };
    let cursor = cloud::load_cursor(&auth.base_url)?;
    let report = cloud::push(
        &conn,
        &cloud::UreqIngestor,
        &auth,
        &machine,
        &cursor,
        500,
        &HashSet::new(),
    )?;
    Ok(SyncPushOutcome {
        sent: report.sent as u64,
        accepted: report.accepted,
        authenticated: true,
        sync_skipped,
    })
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
    validate_discovery_scope(options.scope)?;
    let conn = open_db(db_path)?;
    discover_sessions_collect(&conn, options)
}

#[derive(Args, Debug, Clone, Copy, Default)]
#[group(id = "session_scope", multiple = false)]
struct SessionScopeArgs {
    /// Request/select sessions with a local presence (default).
    #[arg(long, group = "session_scope")]
    local: bool,
    /// Request/select sessions with a remote provider presence.
    #[arg(long, group = "session_scope")]
    remote: bool,
    /// Request/select the deduplicated union of local and remote presences.
    #[arg(long, group = "session_scope")]
    all: bool,
}

impl SessionScopeArgs {
    fn resolve(self) -> SessionScope {
        if self.remote {
            SessionScope::Remote
        } else if self.all {
            SessionScope::All
        } else {
            SessionScope::Local
        }
    }

    fn service_args(self) -> Vec<String> {
        match self.resolve() {
            SessionScope::Local if self.local => vec!["--local".to_string()],
            SessionScope::Local => Vec::new(),
            SessionScope::Remote => vec!["--remote".to_string()],
            SessionScope::All => vec!["--all".to_string()],
        }
    }
}

#[derive(Parser)]
#[command(
    name = "ai-hist",
    bin_name = "ai-hist",
    version,
    about = "Sync, search, tag, and relay AI coding agent history"
)]
struct Cli {
    #[arg(long)]
    db: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Search prompts and sessions.
    Search {
        #[command(flatten)]
        scope: SessionScopeArgs,
        query: Vec<String>,
        #[arg(long)]
        source: Option<String>,
        #[arg(long)]
        project: Option<String>,
        #[arg(long)]
        tag: Option<String>,
        #[arg(long, default_value = "all")]
        role: String,
        #[arg(long)]
        agent: bool,
        #[arg(long)]
        human: bool,
        #[arg(long, default_value_t = 20)]
        limit: i64,
        /// Pass the query through as a raw FTS5 MATCH expression. Operators such as
        /// `-`, `*`, `AND`, `OR`, and `NOT` are interpreted; quote literal terms yourself.
        #[arg(long)]
        fts: bool,
        #[arg(long)]
        json: bool,
    },
    /// Show recent history entries.
    Recent {
        #[command(flatten)]
        scope: SessionScopeArgs,
        #[arg(default_value_t = 20)]
        n: i64,
        #[arg(long)]
        source: Option<String>,
        #[arg(long)]
        project: Option<String>,
        #[arg(long)]
        tag: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Show all entries for a session.
    Session {
        session_id: String,
        #[arg(long)]
        source: Option<String>,
        #[arg(long)]
        tag: Option<String>,
        #[arg(long)]
        full: bool,
        #[arg(long)]
        json: bool,
    },
    /// Replay one session's normalized events: messages, thinking, tool calls, and file edits.
    Events {
        session_id: String,
        #[arg(long)]
        source: Option<String>,
        /// Truncate event text to this many characters in the readable view (0 = no limit).
        #[arg(long, default_value_t = 240)]
        width: usize,
        /// Emit JSON lines: {"type":"event"|"tool_call"|"file_edit", ...} per row.
        #[arg(long)]
        json: bool,
    },
    /// Show one history entry by id.
    Show {
        id: i64,
        #[arg(long)]
        json: bool,
    },
    /// Show neighboring entries around an id.
    Context {
        id: i64,
        #[arg(long, default_value_t = 5)]
        window: i64,
    },
    /// Show history statistics.
    Stats {
        #[command(flatten)]
        scope: SessionScopeArgs,
        #[arg(long)]
        tag: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Add a tag to a session.
    Tag {
        session_id: String,
        tag_name: String,
        #[arg(long)]
        source: Option<String>,
        #[arg(long)]
        color: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Remove a tag from a session.
    Untag {
        session_id: String,
        tag_name: String,
        #[arg(long)]
        source: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// List tags, optionally with tagged sessions.
    Tags {
        #[arg(long)]
        tag: Option<String>,
        #[arg(long)]
        sessions: bool,
        #[arg(long)]
        json: bool,
    },
    /// Print a resume command for the best matching session.
    Resume {
        #[command(flatten)]
        scope: SessionScopeArgs,
        #[arg(required = true)]
        query: Vec<String>,
        /// Pass the query through as a raw FTS5 MATCH expression. Operators such as
        /// `-`, `*`, `AND`, `OR`, and `NOT` are interpreted; quote literal terms yourself.
        #[arg(long)]
        fts: bool,
        #[arg(long)]
        json: bool,
    },
    /// Import history from an opencode SQLite database.
    SyncOpencode {
        #[arg(long)]
        opencode_db: Option<PathBuf>,
    },
    /// Sync agent history using the requested configured connectors.
    Sync {
        #[command(flatten)]
        scope: SessionScopeArgs,
        /// Install a background service (launchd on macOS, cron on Linux) that
        /// runs `sync` on an interval so the database stays fresh automatically.
        #[arg(long)]
        install_service: bool,
        /// Remove the background sync service installed by --install-service.
        #[arg(long, conflicts_with = "install_service")]
        uninstall_service: bool,
        /// Seconds between syncs for the installed service (macOS only; cron
        /// runs at 1-minute granularity).
        #[arg(long, default_value_t = 60)]
        interval: u64,
    },
    /// Repeatedly sync agent history using the requested configured connectors.
    Watch {
        #[command(flatten)]
        scope: SessionScopeArgs,
        #[arg(long, default_value_t = 60)]
        interval: u64,
    },
    /// Diagnose database health: size, WAL, free space, and who holds the write lock.
    Doctor {
        #[arg(long)]
        json: bool,
    },
    /// Build a compact context pack from matching history.
    Pack {
        #[command(flatten)]
        scope: SessionScopeArgs,
        #[arg(required = true)]
        query: Vec<String>,
        #[arg(long)]
        source: Option<String>,
        #[arg(long)]
        project: Option<String>,
        #[arg(long)]
        tag: Option<String>,
        #[arg(long, default_value_t = 10)]
        limit: i64,
        #[arg(long, default_value_t = 0)]
        tokens: usize,
        #[arg(long)]
        fts: bool,
        #[arg(long)]
        json: bool,
    },
    /// Export local history.
    Export {
        output: Option<PathBuf>,
        #[arg(long, default_value = "jsonl")]
        format: String,
        #[arg(long)]
        source: Option<String>,
        #[arg(long)]
        project: Option<String>,
        #[arg(long)]
        repo: Option<String>,
        #[arg(long)]
        since: Option<String>,
        #[arg(long)]
        jsonl: bool,
    },
    /// Import exported history.
    Import {
        file: Option<PathBuf>,
        #[arg(long)]
        dry_run: bool,
        /// Continuously sync local agent history, equivalent to `watch`.
        #[arg(long)]
        watch: bool,
        #[arg(long, default_value_t = 60)]
        interval: u64,
    },
    /// Install local integrations such as git hooks.
    Setup {
        #[command(subcommand)]
        action: SetupAction,
    },
    /// Link sessions to external artifacts such as git commits.
    Link {
        #[command(subcommand)]
        action: LinkAction,
    },
    /// Authenticate to relayhistory-cloud (Agent Relay Loop).
    ///
    /// Defaults to Agent Relay Cloud auth, matching relayfile/workforce. The CLI reads the
    /// canonical `agent-relay` session and exchanges it for a relayhistory session. Pass
    /// `--base-url` + `--token` only for manual/dev login.
    Login {
        /// Use Agent Relay Cloud auth. This is now the default and is kept for compatibility.
        #[arg(long)]
        cloud: bool,
        /// Least-privilege ceiling: `read` (Pair-only) or `sync` (Learn/push). Cloud authorizes
        /// the actual scope it grants. Cloud mode only.
        #[arg(long, default_value = "sync")]
        mode: String,
        /// Reserved for future non-mutating workspace-scoped Cloud sessions.
        #[arg(long)]
        workspace: Option<String>,
        /// relayhistory-cloud base URL. Cloud login defaults to https://history.agentrelay.com;
        /// non-default Cloud exchanges require RELAYHISTORY_ALLOW_UNTRUSTED_CLOUD_BASE_URL=1.
        #[arg(long)]
        base_url: Option<String>,
        /// Legacy/manual: RelayAuth/Agent Relay token (device-flow JWT). Prefer Cloud login.
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value = "ai-hist-engine")]
        label: String,
    },
    /// Dev-only: mint a local `rth_at_` token via /v1/admin/mint (needs ADMIN_MINT_SECRET).
    AdminMint {
        #[arg(long)]
        base_url: String,
        #[arg(long, env = "ADMIN_MINT_SECRET")]
        admin_secret: String,
        #[arg(long)]
        org: String,
        #[arg(long)]
        workspace: Option<String>,
        #[arg(long, default_value = "cli-user")]
        user: String,
        #[arg(long, default_value = "local-dev")]
        label: String,
    },
    /// Fetch a cloud session transcript for offline reading (never imports into SQLite).
    Replay {
        session_id: String,
        /// Select the cloud stage. Defaults to RELAYHISTORY_BASE_URL/AI_HIST_BASE_URL, then prod.
        #[arg(long)]
        base_url: Option<String>,
        /// Events per request (server default 200, maximum 1000). All pages are fetched.
        #[arg(long)]
        limit: Option<usize>,
        /// Cap content per event on the server; truncated events are marked explicitly.
        #[arg(long)]
        max_content: Option<usize>,
        /// Emit the raw event array as JSON.
        #[arg(long)]
        json: bool,
        /// Save the transcript to a file instead of stdout.
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Push new local history + trajectory events to relayhistory-cloud.
    Push {
        /// Select the cloud stage. Required when this machine has sessions for multiple stages.
        #[arg(long)]
        base_url: Option<String>,
        #[arg(long, default_value_t = 500)]
        limit: usize,
        /// Session ids (or trajectory ids) to exclude from the sync (incognito).
        #[arg(long)]
        incognito: Vec<String>,
        #[arg(long)]
        json: bool,
        /// Install a background service (launchd on macOS, cron on Linux) that
        /// runs `push` on an interval so new history reaches the cloud
        /// automatically.
        #[arg(long)]
        install_service: bool,
        /// Remove the background push service installed by --install-service.
        #[arg(long, conflicts_with = "install_service")]
        uninstall_service: bool,
        /// Seconds between pushes for the installed service (macOS only; cron
        /// runs at 1-minute granularity).
        #[arg(long, default_value_t = 300)]
        interval: u64,
    },
    /// Which machines are pushing history to relayhistory-cloud, and how recently.
    ///
    /// Answers "is any machine mute?" without an ssh tour of the fleet. A machine that
    /// stops pushing keeps its row and shows up as STALE or MISSING rather than
    /// disappearing quietly.
    Coverage {
        /// Select the cloud stage. Required when this machine has sessions for multiple stages.
        #[arg(long)]
        base_url: Option<String>,
        /// Seconds without a push before a machine counts as stale (server default: 900,
        /// three times the 300s push service interval).
        #[arg(long)]
        stale_after: Option<u64>,
        /// Seconds without a push before a machine counts as missing (server default: 86400).
        #[arg(long)]
        missing_after: Option<u64>,
        /// Hours of push activity to roll up per machine (server default: 24).
        #[arg(long)]
        window_hours: Option<u64>,
        /// Exit non-zero when any machine is stale or missing, for use from cron/CI.
        #[arg(long)]
        fail_on_stale: bool,
        #[arg(long)]
        json: bool,
    },
    /// Pair (Agent Relay Loop, WS-6) — in-session warnings from your team's history.
    Pair {
        #[command(subcommand)]
        action: PairAction,
    },
    /// Learn (Agent Relay Loop) — distill ordinary session history into Pair signal.
    Learn {
        #[command(subcommand)]
        action: LearnAction,
    },
    /// Coding-agent session catalog — cache-only listing and shallow discovery.
    Sessions {
        #[command(subcommand)]
        action: SessionsAction,
    },
}

#[derive(Subcommand)]
enum SessionsAction {
    /// List the session catalog from the database only.
    ///
    /// Never opens a provider transcript and never scans history, events, or
    /// tool calls: one indexed query over `sessions`. Run `sessions discover`
    /// first to populate a fresh database.
    List {
        #[command(flatten)]
        scope: SessionScopeArgs,
        /// Restrict to a source (repeatable). Defaults to every discoverable source.
        #[arg(long)]
        source: Vec<String>,
        /// Maximum rows (default 50). Must not be negative.
        #[arg(long)]
        limit: Option<i64>,
        /// Coarse cutoff: only sessions older than this epoch-ms. Cannot
        /// separate sessions that share a millisecond — pass the previous
        /// page's `next_cursor` fields to walk pages exactly.
        #[arg(long)]
        before_ms: Option<i64>,
        /// Precise continuation: `source` of the previous page's last row.
        /// Requires --after-session-id.
        #[arg(long, requires = "after_session_id")]
        after_source: Option<String>,
        /// Precise continuation: `session_id` of the previous page's last row.
        /// Requires --after-source.
        #[arg(long, requires = "after_source")]
        after_session_id: Option<String>,
        /// Precise continuation: `last_activity_ms` of the previous page's last
        /// row. Omit (with --after-source/--after-session-id given) to continue
        /// through the rows whose recency is unknown.
        ///
        /// Requires --after-source (which in turn requires --after-session-id):
        /// a timestamp alone is not a cursor, and silently ignoring it would
        /// restart the walk at page one instead of continuing it.
        #[arg(long, requires = "after_source")]
        after_ms: Option<i64>,
        /// Emit `{"contract_version":N,"sessions":[...],"next_cursor":…}` as one
        /// JSON object.
        #[arg(long)]
        json: bool,
    },
    /// Discover sessions from the requested provider locations with bounded reads.
    ///
    /// Enumerates every provider, orders candidates globally by recency, reads
    /// only what the catalog needs, and upserts rows as it goes. Sources whose
    /// bytes have not changed since the last run are served from the catalog.
    /// The summary `scope` echoes the request; `locations_run` reports the
    /// connector locations that executed. `--remote` requires at least one
    /// configured remote connector (see docs/remote-connectors.md); `--all`
    /// runs local adapters plus every configured connector.
    Discover {
        #[command(flatten)]
        scope: SessionScopeArgs,
        /// Restrict to a source (repeatable). Defaults to every discoverable source.
        #[arg(long)]
        source: Vec<String>,
        /// Global cap across all providers, applied by recency. Unlimited when omitted.
        #[arg(long)]
        limit: Option<usize>,
        /// Emit JSONL progressively: one `session`/`diagnostic` object per line,
        /// then a final `summary`.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum PairAction {
    /// Ask relayhistory-cloud for advisory warnings before an action (POST /v1/pair/check).
    Check {
        /// Select the cloud stage. Defaults to RELAYHISTORY_BASE_URL/AI_HIST_BASE_URL, then prod.
        #[arg(long)]
        base_url: Option<String>,
        /// Files in scope / about to be touched (paths only — never contents).
        #[arg(long)]
        file: Vec<String>,
        /// Current task summary.
        #[arg(long)]
        task: Option<String>,
        /// Pending tool/action (e.g. Edit).
        #[arg(long)]
        tool: Option<String>,
        /// Tool target (e.g. the file being edited).
        #[arg(long)]
        target: Option<String>,
        /// Short, caller-provided prompt summary (never the full prompt body).
        #[arg(long)]
        recent_prompt: Option<String>,
        /// Canonical project id (else inferred server-side from repo/cwd).
        #[arg(long)]
        project_id: Option<String>,
        #[arg(long, default_value_t = 5)]
        limit: usize,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum LearnAction {
    /// Distill local session history into decision/finding/reflection events.
    Distill {
        /// Only distill sessions from this source (claude, codex, cursor, grok, relay, opencode).
        #[arg(long)]
        source: Option<String>,
        /// Distill one session id.
        #[arg(long)]
        session_id: Option<String>,
        /// Maximum sessions to distill.
        #[arg(long, default_value_t = 5)]
        limit: usize,
        /// Maximum transcript characters sent to the local/opt-in distiller per session.
        #[arg(long, default_value_t = 24_000)]
        max_chars: usize,
        /// Approximate output-token budget for the distiller.
        #[arg(long, default_value_t = 2_000)]
        max_output_tokens: usize,
        /// Provider: auto, openai, or anthropic.
        #[arg(long, default_value = "auto")]
        provider: String,
        /// Model override.
        #[arg(long)]
        model: Option<String>,
        /// Provider base URL override. Use a local endpoint by default, e.g. Ollama.
        #[arg(long)]
        base_url: Option<String>,
        /// Explicit opt-in for cloud LLM distillation over pre-scrub full transcripts.
        #[arg(long)]
        allow_cloud_llm: bool,
        /// Run distillation and report output without writing local trajectory rows.
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum SetupAction {
    /// Install a no-network post-commit hook that records session→commit links.
    Git {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long)]
        uninstall: bool,
    },
}

#[derive(Subcommand)]
enum LinkAction {
    /// Link the best matching session to a git commit and optional git note.
    Commit {
        #[arg(long, default_value = ".")]
        repo: PathBuf,
        #[arg(long, default_value = "HEAD")]
        commit: String,
        #[arg(long, default_value = "git_note")]
        match_method: String,
        #[arg(long)]
        no_note: bool,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        quiet: bool,
    },
}

/// Whether a command only reads the shared database.
///
/// Conservative by construction: anything not listed here gets a writable
/// handle, so a miscategorised command fails safe (an unnecessary write lock)
/// rather than unsafe (a write attempt on a read-only connection). `export`
/// qualifies because it only writes to a separate destination file.
fn is_read_only(command: &Command) -> bool {
    matches!(
        command,
        Command::Search { .. }
            | Command::Recent { .. }
            | Command::Session { .. }
            | Command::Events { .. }
            | Command::Show { .. }
            | Command::Context { .. }
            | Command::Stats { .. }
            | Command::Tags { .. }
            | Command::Resume { .. }
            | Command::Export { .. }
            | Command::Pack { .. }
            | Command::Doctor { .. }
            // `coverage` queries the server and never reads the local database at all.
            // Listing it here keeps it off the write lock, so running it does not contend
            // with the 60s `sync` service.
            | Command::Coverage { .. }
            // `sessions list` is cache-only by construction: one indexed query
            // over `sessions` and no provider I/O at all. `sessions discover`
            // upserts, so it is deliberately absent.
            | Command::Sessions {
                action: SessionsAction::List { .. }
            }
    )
}

/// A non-contending handle for this command, or `None` to open writably.
///
/// `None` covers three cases: the command writes, the database does not exist
/// yet, or its schema predates this build. That last one matters because a
/// read-only handle skips `init_db`: serving queries over a database missing
/// tables `init_db` would have added turns a silent migration into `no such
/// table` on the user's first search.
fn read_only_connection(command: &Command, db_path: &Path) -> Option<Connection> {
    if !is_read_only(command) || !db_path.exists() {
        return None;
    }
    let conn = open_db_readonly(db_path).ok()?;
    match schema_is_current(&conn) {
        Ok(true) => Some(conn),
        // Pending migration, or the check itself failed: let the writable path
        // sort it out rather than guessing.
        _ => None,
    }
}

/// CLI entry point. `src/main.rs` is a thin wrapper that calls this so the same
/// code is available as a library (for the napi binding).
pub fn run() -> Result<()> {
    let cli = Cli::parse();
    let db_path = cli.db.unwrap_or_else(default_db_path);
    // Sync commands must acquire their advisory lock before opening a writable connection.
    // Pre-dispatch them so the common connection setup below cannot initialize the schema or
    // wait on SQLite before contention is detected.
    match &cli.command {
        // The common read-only path can create or migrate SQLite. A cloud replay must
        // also work on a fresh machine without creating a local history store.
        Command::Replay {
            session_id,
            base_url,
            limit,
            max_content,
            json,
            out,
        } => {
            return replay::run(
                session_id,
                base_url.as_deref(),
                *limit,
                *max_content,
                *json,
                out.as_deref(),
            );
        }
        Command::Sync {
            scope,
            install_service,
            uninstall_service,
            interval,
        } => {
            if *install_service {
                validate_sync_scope(scope.resolve())?;
                return install_managed_service(&SYNC_SERVICE, *interval, &scope.service_args());
            }
            if *uninstall_service {
                return uninstall_managed_service(&SYNC_SERVICE);
            }
            return sync_scope_exclusive(&db_path, scope.resolve()).map(|_| ());
        }
        Command::Watch { scope, interval } => {
            validate_sync_scope(scope.resolve())?;
            return watch_loop(&db_path, *interval, scope.resolve());
        }
        Command::Sessions {
            action: SessionsAction::Discover { scope, .. },
        } => validate_discovery_scope(scope.resolve())?,
        Command::SyncOpencode { opencode_db } => {
            let source = opencode_db.clone().unwrap_or_else(default_opencode_db_path);
            return sync_opencode_exclusive(&db_path, &source).map(|_| ());
        }
        _ => {}
    }
    // Read-only commands get a handle that cannot take the write lock, so a
    // query never contends with the writer. Falls back to a writable open when
    // the database does not exist yet (that first open creates it) or when it
    // predates the current schema (a read-only handle skips init_db, so the
    // migration has to happen through a writable connection first).
    let conn = match read_only_connection(&cli.command, &db_path) {
        Some(conn) => conn,
        None => open_db(&db_path)?,
    };

    match cli.command {
        Command::Replay { .. } => unreachable!("replay is dispatched before opening SQLite"),
        Command::Search {
            scope,
            query,
            source,
            project,
            tag,
            role,
            agent,
            human,
            limit,
            fts,
            json,
        } => {
            validate_source(source.as_deref())?;
            let role = resolve_search_role(&role, agent, human)?;
            let rows = search_all(
                &conn,
                &query,
                fts,
                &QueryFilter {
                    scope: scope.resolve(),
                    source,
                    project,
                    tag,
                    limit,
                    ..Default::default()
                },
                role,
            )?;
            if rows.is_empty() {
                if json {
                    println!("[]");
                } else {
                    println!("No results.");
                }
                std::process::exit(1);
            }
            print_search_rows(rows, json)
        }
        Command::Recent {
            scope,
            n,
            source,
            project,
            tag,
            json,
        } => {
            validate_source(source.as_deref())?;
            let rows = recent(
                &conn,
                &QueryFilter {
                    scope: scope.resolve(),
                    source,
                    project,
                    tag,
                    limit: n,
                    ..Default::default()
                },
            )?;
            print_entries(rows, json)
        }
        Command::Session {
            session_id,
            source,
            tag,
            full,
            json,
        } => {
            validate_source(source.as_deref())?;
            let rows = session(&conn, &session_id, source.as_deref(), tag.as_deref())?;
            if rows.is_empty() {
                if json {
                    println!("[]");
                } else {
                    println!("No entries for session {session_id}");
                }
                std::process::exit(1);
            }
            print_session_entries(&session_id, rows, json, full)
        }
        Command::Events {
            session_id,
            source,
            width,
            json,
        } => {
            validate_source(source.as_deref())?;
            let events = session_events(&conn, &session_id, source.as_deref())?;
            let tool_calls = session_tool_calls(&conn, &session_id, source.as_deref())?;
            let file_edits = session_file_edits(&conn, &session_id, source.as_deref())?;
            if events.is_empty() && tool_calls.is_empty() {
                // JSON mode emits record lines only; an empty session is just
                // an empty stream plus the exit code.
                if !json {
                    println!("No events for session {session_id}");
                }
                std::process::exit(1);
            }
            print_session_events(&session_id, events, tool_calls, file_edits, width, json)
        }
        Command::Show { id, json } => show_entry(&conn, id, json),
        Command::Context { id, window } => show_context(&conn, id, window),
        Command::Doctor { json } => doctor(&db_path, json),
        Command::Pack {
            scope,
            query,
            source,
            project,
            tag,
            limit,
            tokens,
            fts,
            json,
        } => {
            validate_source(source.as_deref())?;
            pack_entries(
                &conn,
                query,
                QueryFilter {
                    scope: scope.resolve(),
                    source,
                    project,
                    tag,
                    limit,
                    ..Default::default()
                },
                tokens,
                fts,
                json,
            )
        }
        Command::Stats { scope, tag, json } => {
            print_stats(&conn, scope.resolve(), tag.as_deref(), json)
        }
        Command::Tag {
            session_id,
            tag_name,
            source,
            color,
            json,
        } => {
            validate_source(source.as_deref())?;
            let (sessions, created) = tag_session_with_count(
                &conn,
                &session_id,
                &tag_name,
                source.as_deref(),
                color.as_deref(),
            )?;
            if json {
                println!(
                    "{}",
                    json!({
                        "session_id": session_id,
                        "tag": normalize_tag_name(&tag_name),
                        "matched_sessions": sessions,
                        "created_assignments": created,
                    })
                );
            } else if sessions.is_empty() {
                anyhow::bail!("No session found for {session_id}");
            } else {
                let label = if sessions.len() == 1 {
                    "session"
                } else {
                    "sessions"
                };
                println!(
                    "Tagged {} {label} with '{}' ({} new assignment(s)).",
                    sessions.len(),
                    tag_name.trim(),
                    created
                );
            }
            Ok(())
        }
        Command::Untag {
            session_id,
            tag_name,
            source,
            json,
        } => {
            validate_source(source.as_deref())?;
            let removed = untag_session(&conn, &session_id, &tag_name, source.as_deref())?;
            if json {
                println!("{}", serde_json::json!({ "removed_assignments": removed }));
            } else {
                println!("Removed tag '{tag_name}' from {removed} session assignment(s).");
            }
            Ok(())
        }
        Command::Tags {
            tag,
            sessions,
            json,
        } => print_tags(&conn, tag.as_deref(), sessions, json),
        Command::Resume {
            scope,
            query,
            fts,
            json,
        } => {
            let requested_scope = scope.resolve();
            let rows = search(
                &conn,
                &query,
                fts,
                &QueryFilter {
                    scope: requested_scope,
                    limit: 1,
                    ..Default::default()
                },
            )?;
            let entry = rows
                .into_iter()
                .find(|e| e.session_id.as_ref().is_some_and(|s| !s.is_empty()));
            if let Some(entry) = entry {
                let (locations, cmd) = local_resume_details(&conn, &entry)?;
                let locally_available =
                    locations.is_empty() || locations.iter().any(|location| location == "local");
                if json {
                    let mut out = entry_output(&entry);
                    out["resume_cmd"] = json!(cmd);
                    out["scope"] = json!(requested_scope);
                    out["locations"] = json!(locations);
                    if !locally_available {
                        out["resume_unavailable_reason"] =
                            json!("session is remote-only; materialize it locally before resuming");
                    }
                    println!("{}", out);
                } else if let Some(cmd) = cmd {
                    println!("{cmd}");
                } else if !locally_available {
                    anyhow::bail!(
                        "Session {} is remote-only and cannot be resumed locally; materialize it locally first.",
                        entry.session_id.as_deref().unwrap_or("(unknown)")
                    );
                } else {
                    anyhow::bail!("No resume command available for source '{}'", entry.source);
                }
            } else {
                anyhow::bail!("No session found");
            }
            Ok(())
        }
        Command::SyncOpencode { .. } | Command::Sync { .. } | Command::Watch { .. } => {
            unreachable!("sync commands are handled before opening the shared database")
        }
        Command::Export {
            output,
            format,
            source,
            project,
            repo,
            since,
            jsonl,
        } => {
            validate_source(source.as_deref())?;
            if output.as_deref() == Some(Path::new("commit-links")) {
                export_commit_links(
                    &conn,
                    source.as_deref(),
                    repo.as_deref().or(project.as_deref()),
                    since.as_deref(),
                    jsonl,
                )
            } else {
                export_history(
                    &conn,
                    output.as_deref(),
                    &format,
                    source.as_deref(),
                    project.as_deref(),
                    since.as_deref(),
                )
            }
        }
        Command::Import {
            file,
            dry_run,
            watch,
            interval,
        } => {
            if watch {
                anyhow::ensure!(
                    file.is_none(),
                    "`ai-hist import --watch` does not accept an import file"
                );
                anyhow::ensure!(
                    !dry_run,
                    "`ai-hist import --watch` cannot be combined with --dry-run"
                );
                watch_loop(&db_path, interval, SessionScope::Local)
            } else {
                let file = file.context("`ai-hist import` requires FILE unless --watch is set")?;
                import_history(&conn, &file, dry_run)
            }
        }
        Command::Setup { action } => match action {
            SetupAction::Git { repo, uninstall } => setup_git_hook(&db_path, &repo, uninstall),
        },
        Command::Link { action } => match action {
            LinkAction::Commit {
                repo,
                commit,
                match_method,
                no_note,
                json,
                quiet,
            } => link_git_commit(
                &conn,
                &db_path,
                &repo,
                &commit,
                &match_method,
                !no_note,
                json,
                quiet,
            ),
        },
        Command::Login {
            cloud: _use_cloud,
            mode,
            workspace,
            base_url,
            token,
            label,
        } => {
            let auth = if let Some(token) = token {
                let base_url =
                    base_url.context("`--base-url` is required with manual `--token` login")?;
                cloud::login(&base_url, &token, &label, None)?
            } else {
                let base_url = base_url.unwrap_or_else(cloud::default_base_url);
                cloud::login_via_cloud(&base_url, &mode, workspace.as_deref(), &label)?
            };
            cloud::save_auth(&auth)?;
            // Never print the session/token — only where it landed.
            println!("Logged in to {} (session stored).", auth.base_url);
            Ok(())
        }
        Command::AdminMint {
            base_url,
            admin_secret,
            org,
            workspace,
            user,
            label,
        } => {
            let auth = cloud::admin_mint(
                &base_url,
                &admin_secret,
                &org,
                workspace.as_deref(),
                &user,
                &label,
            )?;
            cloud::save_auth(&auth)?;
            println!("Minted local token for org {org} (stored).");
            Ok(())
        }
        Command::Push {
            base_url,
            limit,
            incognito,
            json,
            install_service,
            uninstall_service,
            interval,
        } => {
            if install_service {
                // `--incognito` is a per-run privacy filter; the scheduled job
                // runs a plain `push`, so silently dropping it would give a
                // false sense that it applies. Reject the combo. `--limit`, on
                // the other hand, is part of the reliability configuration and
                // is recorded in the service command.
                if !incognito.is_empty() {
                    anyhow::bail!(
                        "--incognito is a per-run privacy filter and is NOT applied to the scheduled \
                         push service. Run `ai-hist push --incognito ...` for a one-off push, or omit \
                         it when installing the service."
                    );
                }
                let auth = cloud::load_auth(base_url.as_deref())?.context(
                    "not authenticated for the selected stage — run `ai-hist login` or \
                     `ai-hist admin-mint` first",
                )?;
                let service_args = vec![
                    "--base-url".to_string(),
                    auth.base_url,
                    "--limit".to_string(),
                    limit.to_string(),
                ];
                return install_managed_service(&PUSH_SERVICE, interval, &service_args);
            }
            if uninstall_service {
                return uninstall_managed_service(&PUSH_SERVICE);
            }
            let auth = cloud::load_auth(base_url.as_deref())?
                .context("not authenticated — run `ai-hist login` or `ai-hist admin-mint` first")?;
            let machine = MachineIdentity {
                id: cloud::machine_id()?,
                hostname: cloud::machine_hostname(),
                os: Some(std::env::consts::OS.to_string()),
                cli_version: Some(env!("CARGO_PKG_VERSION").to_string()),
                ..Default::default()
            };
            let cursor = cloud::load_cursor(&auth.base_url)?;
            let incognito_set: HashSet<String> = incognito.into_iter().collect();
            let report = cloud::push(
                &conn,
                &cloud::UreqIngestor,
                &auth,
                &machine,
                &cursor,
                limit,
                &incognito_set,
            )?;
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "sent": report.sent,
                        "accepted": report.accepted,
                        "batchId": report.batch_id,
                        "cursor": report.cursor,
                        "batchLimit": report.batch_limit,
                        "attempts": report.attempts,
                    })
                );
            } else if report.sent == 0 {
                println!("Nothing new to push.");
            } else {
                println!(
                    "Pushed {} record(s), {} accepted (cursor → history #{}, trajectory rowid {}; batch limit {}, {} attempt(s)).",
                    report.sent,
                    report.accepted,
                    report.cursor.history_id,
                    report.cursor.trajectory_rowid,
                    report.batch_limit,
                    report.attempts,
                );
            }
            Ok(())
        }
        Command::Coverage {
            base_url,
            stale_after,
            missing_after,
            window_hours,
            fail_on_stale,
            json,
        } => {
            let auth = cloud::load_auth(base_url.as_deref())?
                .context("not authenticated — run `ai-hist login` or `ai-hist admin-mint` first")?;
            let resp = cloud::fleet_coverage(
                &auth,
                &cloud::CoverageQuery {
                    stale_after_seconds: stale_after,
                    missing_after_seconds: missing_after,
                    window_hours,
                },
            )?;
            if json {
                println!("{}", serde_json::to_string(&resp)?);
            } else {
                print!("{}", cloud::format_fleet_coverage(&resp));
            }
            // Opt-in so an interactive `coverage` stays a plain query, while a scheduled one
            // can alert. Silent absence is only fixed if something can act on it.
            if fail_on_stale && resp.has_gaps() {
                std::process::exit(1);
            }
            Ok(())
        }
        Command::Pair { action } => match action {
            PairAction::Check {
                base_url,
                file,
                task,
                tool,
                target,
                recent_prompt,
                project_id,
                limit,
                json,
            } => {
                // Hooks and MCP wrappers do not have an interactive stage-selection channel.
                // Pin them to the configured/default origin instead of making Pair disappear
                // when an unrelated second-stage login exists.
                let base_url = base_url.unwrap_or_else(cloud::default_base_url);
                let auth = cloud::load_auth(Some(&base_url))?.context(
                    "not authenticated — run `ai-hist login` or `ai-hist admin-mint` first",
                )?;
                let cwd = std::env::current_dir()
                    .ok()
                    .map(|p| p.display().to_string());
                let ctx = cloud::PairContext {
                    project_id,
                    repo_path: cwd.clone(),
                    cwd,
                    git_remote: detect_git_remote(),
                    task,
                    files: file,
                    tool,
                    target,
                    recent_prompt,
                };
                let resp = cloud::pair_check(&auth, &ctx, limit)?;
                if json {
                    println!("{}", serde_json::to_string(&resp)?);
                } else {
                    print!("{}", cloud::format_pair_warnings(&resp));
                }
                Ok(())
            }
        },
        Command::Learn { action } => match action {
            LearnAction::Distill {
                source,
                session_id,
                limit,
                max_chars,
                max_output_tokens,
                provider,
                model,
                base_url,
                allow_cloud_llm,
                dry_run,
                json,
            } => {
                validate_source(source.as_deref())?;
                let provider = learn::provider_from_str(&provider)?;
                let report = learn::distill_sessions(
                    &conn,
                    &learn::LearnDistillOptions {
                        source,
                        session_id,
                        limit,
                        max_chars,
                        max_output_tokens,
                        provider,
                        model,
                        base_url,
                        allow_cloud_llm,
                        dry_run,
                    },
                )?;
                if json {
                    println!(
                        "{}",
                        serde_json::json!({
                            "scanned": report.scanned,
                            "distilled": report.distilled,
                            "skipped": report.skipped,
                            "rows": report.rows.iter().map(|row| serde_json::json!({
                                "id": row.id,
                                "source": row.source,
                                "sessionId": row.session_id,
                                "eventsEstimate": row.events_estimate,
                                "dryRun": row.dry_run,
                            })).collect::<Vec<_>>(),
                        })
                    );
                } else {
                    println!(
                        "Learn-distilled {} session(s) ({} scanned, {} skipped).",
                        report.distilled, report.scanned, report.skipped
                    );
                    for row in report.rows {
                        let action = if row.dry_run { "would write" } else { "wrote" };
                        println!(
                            "  {action} {} from {}:{} ({} event(s) estimated)",
                            row.id, row.source, row.session_id, row.events_estimate
                        );
                    }
                }
                Ok(())
            }
        },
        Command::Sessions { action } => match action {
            SessionsAction::List {
                scope,
                source,
                limit,
                before_ms,
                after_source,
                after_session_id,
                after_ms,
                json,
            } => {
                for source in &source {
                    validate_source(Some(source))?;
                }
                // SQLite reads a negative LIMIT as "no limit", so `--limit -1`
                // would quietly dump the entire catalog instead of erroring.
                if let Some(limit) = limit {
                    anyhow::ensure!(limit >= 0, "--limit must not be negative (got {limit})");
                }
                // clap's `requires` already pairs the two identity flags; the
                // timestamp is optional because the rows whose recency is
                // unknown sort last and are continued through without one.
                let after = match (after_source, after_session_id) {
                    (Some(source), Some(session_id)) => Some(CatalogCursor {
                        last_activity_ms: after_ms,
                        source,
                        session_id,
                    }),
                    _ => None,
                };
                let page = list_session_catalog_page(
                    &conn,
                    &CatalogListOptions {
                        scope: scope.resolve(),
                        sources: source,
                        limit,
                        before_ms,
                        after,
                    },
                )?;
                print_session_catalog(&page, json)
            }
            SessionsAction::Discover {
                scope,
                source,
                limit,
                json,
            } => run_session_discovery(&conn, scope.resolve(), source, limit, json),
        },
    }
}

/// Render `ai-hist sessions list`.
///
/// The JSON form is one object, not a bare array, so the contract version and
/// the pagination cursor travel with the payload:
/// `{"contract_version":3,"scope":"local","sessions":[…],"next_cursor":{…}|null}`. Feed
/// `next_cursor` back as `--after-ms/--after-source/--after-session-id` to get
/// the next page; it is null once the catalog is exhausted.
fn print_session_catalog(page: &SessionCatalogPage, as_json: bool) -> Result<()> {
    let rows = &page.sessions;
    if as_json {
        println!(
            "{}",
            json!({
                "contract_version": SESSION_CATALOG_CONTRACT_VERSION,
                "scope": page.scope,
                "sessions": rows,
                "next_cursor": page.next_cursor,
            })
        );
        return Ok(());
    }
    if rows.is_empty() {
        println!("No sessions in the catalog. Run `ai-hist sessions discover` to populate it.");
        return Ok(());
    }
    for row in rows {
        println!("{}", fmt_session_row(row));
    }
    println!("  {} session(s)", rows.len());
    if let Some(cursor) = &page.next_cursor {
        println!(
            "  more available: --after-source {} --after-session-id {}{}",
            cursor.source,
            cursor.session_id,
            cursor
                .last_activity_ms
                .map(|ms| format!(" --after-ms {ms}"))
                .unwrap_or_default()
        );
    }
    Ok(())
}

fn fmt_session_row(row: &ShallowSession) -> String {
    let when = row
        .last_activity_ms
        .and_then(|ms| Local.timestamp_millis_opt(ms).single())
        .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
        .unwrap_or_else(|| "------ --:--".to_string());
    let cwd = row.cwd.as_deref().unwrap_or("-");
    let prompt = row
        .first_prompt
        .as_deref()
        .map(|text| {
            let flat = text.replace('\n', " ");
            if flat.chars().count() > 80 {
                format!("{}...", flat.chars().take(80).collect::<String>())
            } else {
                flat
            }
        })
        .unwrap_or_default();
    let locations = row.locations.join(",");
    format!(
        "  {when}  {:<8} {:<12} {:<8} {:<38} {cwd}  {prompt}",
        row.source, locations, row.discovery_state, row.session_id
    )
}

/// Render `ai-hist sessions discover`, streaming rows as they are produced.
///
/// JSON mode is JSONL (the `events` command's precedent): one
/// `{"type":"session",…}` per row, `{"type":"diagnostic",…}` per failure, and a
/// closing `{"type":"summary",…}`. Per-provider failures are reported but do
/// not change the exit code; only an every-provider failure does.
fn run_session_discovery(
    conn: &Connection,
    scope: SessionScope,
    sources: Vec<String>,
    limit: Option<usize>,
    as_json: bool,
) -> Result<()> {
    let options = DiscoverOptions {
        scope,
        sources,
        limit,
    };
    let mut count = 0usize;
    let outcome = discover_sessions(conn, &options, |session| {
        count += 1;
        if as_json {
            match serde_json::to_value(session) {
                Ok(Value::Object(mut map)) => {
                    map.insert("type".to_string(), json!("session"));
                    println!("{}", Value::Object(map));
                }
                _ => eprintln!(
                    "ai-hist: could not serialize session {}",
                    session.session_id
                ),
            }
        } else {
            println!("{}", fmt_session_row(session));
        }
    });
    // An every-provider failure is still a failure, but the diagnostics are
    // the reason a caller ran this at all. Render the collected stream and the
    // summary trailer first, so a JSONL consumer never sees a truncated stream
    // followed by an opaque exit code.
    let (summary, failure) = match outcome {
        Ok(summary) => (summary, None),
        // `downcast`'s Err arm carries the original error untouched, so `?`
        // propagates any other failure exactly as it arrived.
        Err(error) => {
            let failed = error.downcast::<AllProvidersFailed>()?;
            let summary = failed.summary.clone();
            (summary, Some(anyhow::Error::from(failed)))
        }
    };
    for diagnostic in &summary.diagnostics {
        if as_json {
            println!(
                "{}",
                json!({
                    "type": "diagnostic",
                    "source": diagnostic.source,
                    "locator": diagnostic.locator,
                    "error": diagnostic.error,
                })
            );
        } else {
            let scope = diagnostic.locator.as_deref().unwrap_or("(provider)");
            eprintln!(
                "ai-hist: [{}] {scope}: {}",
                diagnostic.source, diagnostic.error
            );
        }
    }
    if as_json {
        let mut payload = serde_json::to_value(&summary)?;
        if let Value::Object(map) = &mut payload {
            map.insert("type".to_string(), json!("summary"));
            map.remove("diagnostics");
        }
        println!("{payload}");
    } else {
        let locations_run = match summary.locations_run.is_empty() {
            true => "none".to_string(),
            false => summary.locations_run.join(", "),
        };
        println!(
            "  {count} session(s): {} discovered, {} unchanged ({} file(s) opened, {} shallow read(s)); requested scope: {}, connector locations run: {locations_run}",
            summary.discovered,
            summary.skipped_unchanged,
            summary.counters.files_opened,
            summary.counters.shallow_reads,
            session_scope_label(summary.scope),
        );
    }
    if let Some(failure) = failure {
        return Err(failure);
    }
    Ok(())
}

fn session_scope_label(scope: SessionScope) -> &'static str {
    match scope {
        SessionScope::Local => "local",
        SessionScope::Remote => "remote",
        SessionScope::All => "all",
    }
}

/// Best-effort `git remote get-url origin` for project scoping (None if not a repo).
/// Credentials in the URL (`https://user:token@host/…`) are stripped before egress — this
/// field is generated client-side, downstream of the hook's scrub belt, so it self-guards.
fn detect_git_remote() -> Option<String> {
    let out = std::process::Command::new("git")
        .args(["config", "--get", "remote.origin.url"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let url = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (!url.is_empty()).then(|| strip_url_credentials(&url))
}

/// Remove any `userinfo@` (user/password/token) between `scheme://` and the host so a
/// credential-embedded remote never ships to the server. Non-`://` forms (scp-style
/// `git@host:org/repo`) carry no secret and are returned unchanged.
fn strip_url_credentials(url: &str) -> String {
    if let Some(scheme_end) = url.find("://") {
        let after = scheme_end + 3;
        let rest = &url[after..];
        if let Some(at) = rest.find('@') {
            let host_start = rest.find('/').unwrap_or(rest.len());
            if at < host_start {
                return format!("{}{}", &url[..after], &rest[at + 1..]);
            }
        }
    }
    url.to_string()
}

fn print_entries(rows: Vec<HistoryEntry>, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string(&entry_outputs(&rows))?);
        return Ok(());
    }
    for row in rows {
        println!("{}", fmt_row(&row, false));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SearchRole {
    All,
    User,
    Assistant,
}

#[derive(Debug, Clone)]
struct SearchRow {
    id: i64,
    source: String,
    session_id: Option<String>,
    project: Option<String>,
    text: String,
    timestamp_ms: i64,
    role: String,
    kind: String,
    match_source: String,
}

fn resolve_search_role(raw: &str, agent: bool, human: bool) -> Result<SearchRole> {
    anyhow::ensure!(
        !(agent && human),
        "ai-hist search: --agent and --human are mutually exclusive"
    );
    if agent {
        return Ok(SearchRole::Assistant);
    }
    if human {
        return Ok(SearchRole::User);
    }
    match raw {
        "all" => Ok(SearchRole::All),
        "user" => Ok(SearchRole::User),
        "assistant" => Ok(SearchRole::Assistant),
        other => anyhow::bail!(
            "ai-hist search: --role must be one of user, assistant, all (got {other})"
        ),
    }
}

fn print_search_rows(rows: Vec<SearchRow>, as_json: bool) -> Result<()> {
    if as_json {
        let out = rows
            .iter()
            .map(|row| {
                let mut value = json!({
                    "id": row.id,
                    "source": row.source,
                    "session_id": row.session_id,
                    "project": row.project,
                    "prompt": row.text,
                    "timestamp_ms": row.timestamp_ms,
                });
                if row.match_source != "history" {
                    value["role"] = json!(row.role);
                    value["kind"] = json!(row.kind);
                    value["match_source"] = json!(row.match_source);
                }
                value
            })
            .collect::<Vec<_>>();
        println!("{}", serde_json::to_string(&out)?);
        return Ok(());
    }
    for row in rows {
        println!("{}", fmt_search_row(&row));
    }
    Ok(())
}

fn fmt_search_row(row: &SearchRow) -> String {
    let dt = Local
        .timestamp_millis_opt(row.timestamp_ms)
        .single()
        .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
        .unwrap_or_else(|| "1970-01-01 00:00".to_string());
    let project = row
        .project
        .as_ref()
        .map(|p| format!(" [{p}]"))
        .unwrap_or_default();
    let label = if row.match_source == "history" {
        row.source.clone()
    } else {
        format!("{}:{}:{}", row.source, row.role, row.kind)
    };
    let text = if row.text.chars().count() > 120 {
        let truncated: String = row.text.chars().take(120).collect();
        format!("{}...", truncated.replace('\n', " "))
    } else {
        row.text.replace('\n', " ")
    };
    format!("  #{:<5} {}  ({}){}  {}", row.id, dt, label, project, text)
}

fn print_session_events(
    session_id: &str,
    events: Vec<ai_hist_core::SessionEvent>,
    tool_calls: Vec<ai_hist_core::SessionToolCall>,
    file_edits: Vec<ai_hist_core::SessionFileEdit>,
    width: usize,
    json: bool,
) -> Result<()> {
    if json {
        // One replay stream, merged chronologically across the three record
        // types (unknown timestamps last, ties broken by row id). Sorting
        // references and serializing at write time keeps peak memory at the
        // fetched rows themselves, not a second serialized copy.
        enum ReplayRecord<'a> {
            Event(&'a ai_hist_core::SessionEvent),
            ToolCall(&'a ai_hist_core::SessionToolCall),
            FileEdit(&'a ai_hist_core::SessionFileEdit),
        }
        let mut records: Vec<(Option<i64>, i64, ReplayRecord)> = Vec::new();
        for event in &events {
            records.push((Some(event.ts_ms), event.id, ReplayRecord::Event(event)));
        }
        for call in &tool_calls {
            records.push((call.ts_ms, call.id, ReplayRecord::ToolCall(call)));
        }
        for edit in &file_edits {
            records.push((edit.ts_ms, edit.id, ReplayRecord::FileEdit(edit)));
        }
        records.sort_by_key(|(ts, id, _)| (ts.is_none(), ts.unwrap_or(0), *id));
        for (_, _, record) in records {
            let line = match record {
                ReplayRecord::Event(event) => {
                    serde_json::to_string(&json!({ "type": "event", "record": event }))?
                }
                ReplayRecord::ToolCall(call) => {
                    serde_json::to_string(&json!({ "type": "tool_call", "record": call }))?
                }
                ReplayRecord::FileEdit(edit) => {
                    serde_json::to_string(&json!({ "type": "file_edit", "record": edit }))?
                }
            };
            println!("{line}");
        }
        return Ok(());
    }
    println!(
        "  Session {session_id}: {} events, {} tool calls, {} file edits\n",
        events.len(),
        tool_calls.len(),
        file_edits.len()
    );
    for event in &events {
        let text = event.text.as_deref().unwrap_or("");
        let flat = text.split_whitespace().collect::<Vec<_>>().join(" ");
        let shown = if width > 0 && flat.chars().count() > width {
            format!("{}…", flat.chars().take(width).collect::<String>())
        } else {
            flat
        };
        println!(
            "  {}  {:<11} {}",
            format_datetime(event.ts_ms),
            format!("{}/{}", event.role, event.kind),
            shown
        );
    }
    // Tool calls already render above as their tool_use events; file edits
    // have no event row, so list them explicitly.
    if !file_edits.is_empty() {
        println!("\n  Files changed:");
        for edit in &file_edits {
            let counts = match (edit.lines_added, edit.lines_removed) {
                (Some(added), Some(removed)) if added + removed > 0 => {
                    format!(" (+{added} -{removed})")
                }
                _ => String::new(),
            };
            println!("    {}{}", edit.file_path, counts);
        }
    }
    Ok(())
}

fn print_session_entries(
    session_id: &str,
    rows: Vec<HistoryEntry>,
    json: bool,
    full: bool,
) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string(&entry_outputs(&rows))?);
        return Ok(());
    }
    println!("  Session {session_id} ({} entries):\n", rows.len());
    for row in rows {
        println!("{}", fmt_row(&row, full));
    }
    Ok(())
}

fn entry_outputs(rows: &[HistoryEntry]) -> Vec<serde_json::Value> {
    rows.iter().map(entry_output).collect()
}

fn entry_output(row: &HistoryEntry) -> serde_json::Value {
    json!({
        "id": row.id,
        "source": row.source,
        "session_id": row.session_id,
        "project": row.project,
        "prompt": row.prompt,
        "timestamp_ms": row.timestamp_ms,
    })
}

/// Locations recorded for an indexed history entry.
///
/// A missing presence remains an empty list. Callers that select local rows
/// apply the legacy local fallback separately; output should report observed
/// presences, not invent one.
fn entry_locations(conn: &Connection, entry: &HistoryEntry) -> Result<Vec<String>> {
    let Some(session_id) = entry.session_id.as_deref() else {
        return Ok(Vec::new());
    };
    let mut stmt = conn.prepare(
        "SELECT location FROM session_presences \
         WHERE source = ? AND session_id = ? \
         ORDER BY CASE location WHEN 'local' THEN 0 ELSE 1 END",
    )?;
    let locations = stmt
        .query_map(params![entry.source, session_id], |row| row.get(0))?
        .collect::<rusqlite::Result<Vec<String>>>()?;
    Ok(locations)
}

fn local_resume_details(
    conn: &Connection,
    entry: &HistoryEntry,
) -> Result<(Vec<String>, Option<String>)> {
    let locations = entry_locations(conn, entry)?;
    // A zero-presence row is legacy local evidence. Keep that compatibility
    // fallback for behavior without claiming an observed location in output.
    let locally_available =
        locations.is_empty() || locations.iter().any(|location| location == "local");
    let command = locally_available.then(|| resume_command(entry)).flatten();
    Ok((locations, command))
}

fn fmt_row(row: &HistoryEntry, verbose: bool) -> String {
    let dt = Local
        .timestamp_millis_opt(row.timestamp_ms)
        .single()
        .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
        .unwrap_or_else(|| "1970-01-01 00:00".to_string());
    let project = row
        .project
        .as_ref()
        .map(|p| format!(" [{p}]"))
        .unwrap_or_default();
    let prompt = if verbose {
        row.prompt.clone()
    } else if row.prompt.chars().count() > 120 {
        let truncated: String = row.prompt.chars().take(120).collect();
        format!("{}...", truncated.replace('\n', " "))
    } else {
        row.prompt.replace('\n', " ")
    };
    format!(
        "  #{:<5} {}  ({}){}  {}",
        row.id, dt, row.source, project, prompt
    )
}

fn default_opencode_db_path() -> PathBuf {
    std::env::var_os("OPENCODE_DB")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let home = std::env::var_os("HOME")
                .or_else(|| std::env::var_os("USERPROFILE"))
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("."));
            home.join(".local/share/opencode/opencode.db")
        })
}

fn validate_source(source: Option<&str>) -> Result<()> {
    if let Some(source) = source {
        anyhow::ensure!(
            SOURCE_CHOICES.contains(&source),
            "invalid source '{source}' (choose from {})",
            SOURCE_CHOICES.join(", ")
        );
    }
    Ok(())
}

fn show_entry(conn: &Connection, id: i64, as_json: bool) -> Result<()> {
    let entry = get_entry(conn, id)?;
    let (locations, resume) = local_resume_details(conn, &entry)?;
    let session_count: Option<i64> = if let Some(session_id) = &entry.session_id {
        Some(conn.query_row(
            "SELECT COUNT(*) FROM history WHERE source = ? AND session_id = ?",
            params![entry.source, session_id],
            |row| row.get(0),
        )?)
    } else {
        None
    };
    let tags = if let Some(session_id) = &entry.session_id {
        session_tags(conn, &entry.source, session_id)?
    } else {
        Vec::new()
    };
    if as_json {
        let mut out = entry_output(&entry);
        out["resume_cmd"] = json!(resume);
        out["locations"] = json!(locations);
        out["session_count"] = json!(session_count);
        out["tags"] = json!(tags);
        println!("{out}");
        return Ok(());
    }
    let dt = Local
        .timestamp_millis_opt(entry.timestamp_ms)
        .single()
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| "1970-01-01 00:00:00".to_string());
    println!("  ID:        {}", entry.id);
    println!("  Source:    {}", entry.source);
    println!(
        "  Session:   {}",
        entry.session_id.as_deref().unwrap_or("(none)")
    );
    println!(
        "  Project:   {}",
        entry.project.as_deref().unwrap_or("(none)")
    );
    println!("  Time:      {dt}");
    println!("  Prompt:\n");
    println!("{}", entry.prompt);
    println!();
    if let Some(session_id) = &entry.session_id {
        println!(
            "  Session has {} entries: ai-hist session {}",
            session_count.unwrap_or(0),
            session_id
        );
        if !tags.is_empty() {
            let names = tags
                .iter()
                .filter_map(|tag| tag.get("display_name").and_then(|v| v.as_str()))
                .collect::<Vec<_>>()
                .join(", ");
            println!("  Tags:    {names}");
        }
        if let Some(cmd) = resume {
            println!("  Resume:  {cmd}");
        }
    }
    println!("  Context: ai-hist context {}", entry.id);
    Ok(())
}

fn show_context(conn: &Connection, id: i64, window_minutes: i64) -> Result<()> {
    let entry = get_entry(conn, id)?;
    if let Some(session_id) = &entry.session_id {
        let rows = query_entries(
            conn,
            "SELECT id, source, session_id, project, prompt, timestamp_ms FROM history WHERE session_id = ? ORDER BY timestamp_ms ASC",
            &[session_id],
        )?;
        if !rows.is_empty() {
            println!("  === Session {session_id} ({} entries) ===\n", rows.len());
            for row in rows {
                let marker = if row.id == id { " >>>" } else { "    " };
                println!("{marker}{}", fmt_row(&row, false));
            }
            println!();
        }
    }
    let window_ms = window_minutes * 60 * 1000;
    let sid = entry.session_id.as_deref().unwrap_or("");
    let mut stmt = conn.prepare(
        "SELECT id, source, session_id, project, prompt, timestamp_ms FROM history \
         WHERE timestamp_ms BETWEEN ? AND ? AND (session_id IS NULL OR session_id != ?) \
         ORDER BY timestamp_ms ASC",
    )?;
    let rows = stmt
        .query_map(
            params![
                entry.timestamp_ms - window_ms,
                entry.timestamp_ms + window_ms,
                sid
            ],
            row_to_entry,
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if !rows.is_empty() {
        println!("  === Nearby ({window_minutes}min window, other sessions) ===\n");
        for row in rows {
            println!("    {}", fmt_row(&row, false));
        }
    }
    Ok(())
}

fn print_stats(
    conn: &Connection,
    scope: SessionScope,
    tag: Option<&str>,
    as_json: bool,
) -> Result<()> {
    let tag_norm = tag.map(normalize_tag_name);
    let mut where_sql = " WHERE 1=1".to_string();
    append_session_scope_filter(&mut where_sql, scope, "h");
    if tag_norm.is_some() {
        where_sql.push_str(&format!(" AND {}", tag_filter_clause("h")));
    }
    let params_vec = tag_norm.iter().map(String::as_str).collect::<Vec<_>>();
    let total: i64 = conn.query_row(
        &format!("SELECT COUNT(*) FROM history h{where_sql}"),
        rusqlite::params_from_iter(params_vec.clone()),
        |row| row.get(0),
    )?;
    let by_source_rows = query_pairs(
        conn,
        &format!(
            "SELECT source, COUNT(*) FROM history h{where_sql} GROUP BY source ORDER BY source"
        ),
        &params_vec,
    )?;
    let by_source = by_source_rows
        .iter()
        .cloned()
        .collect::<serde_json::Map<_, _>>();
    let project_where = format!("{where_sql} AND project IS NOT NULL");
    let top_projects = query_pairs(
        conn,
        &format!("SELECT project, COUNT(*) FROM history h {project_where} GROUP BY project ORDER BY COUNT(*) DESC LIMIT 10"),
        &params_vec,
    )?
    .into_iter()
    .map(|(project, count)| json!({ "project": project, "count": count }))
    .collect::<Vec<_>>();
    let (first, last): (Option<i64>, Option<i64>) = conn.query_row(
        &format!("SELECT MIN(timestamp_ms), MAX(timestamp_ms) FROM history h{where_sql}"),
        rusqlite::params_from_iter(params_vec),
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    if as_json {
        println!(
            "{}",
            json!({
                "total": total,
                "by_source": by_source,
                "top_projects": top_projects,
                "first_timestamp_ms": first,
                "last_timestamp_ms": last,
                "tag": tag_norm,
                "scope": scope,
            })
        );
        return Ok(());
    }
    println!("\nTotal entries: {total}");
    println!(
        "Scope: {}",
        match scope {
            SessionScope::Local => "local",
            SessionScope::Remote => "remote",
            SessionScope::All => "all",
        }
    );
    if let Some(tag) = tag_norm {
        println!("Tag filter: {tag}");
    }
    println!("\nBy source:");
    for (source, count) in by_source_rows {
        println!("  {source}: {count}");
    }
    if let (Some(first), Some(last)) = (first, last) {
        println!("\nDate range:");
        println!("  {} to {}", format_date(first), format_date(last));
    }
    println!("\nTop 10 projects:");
    for item in top_projects {
        println!(
            "  {:>6}  {}",
            item["count"],
            item["project"].as_str().unwrap_or("")
        );
    }
    Ok(())
}

fn pack_entries(
    conn: &Connection,
    query: Vec<String>,
    filter: QueryFilter,
    tokens: usize,
    raw_fts: bool,
    as_json: bool,
) -> Result<()> {
    let rows = search(conn, &query, raw_fts, &filter)?;
    if rows.is_empty() {
        if as_json {
            println!("{}", json!({ "query": query.join(" "), "entries": [] }));
        } else {
            println!("No results.");
        }
        std::process::exit(1);
    }
    let chars_budget = (tokens > 0).then_some(tokens * 4);
    let query_str = query.join(" ");
    let generated_ms = chrono::Utc::now().timestamp_millis();
    if as_json {
        let entries = rows
            .iter()
            .map(|entry| {
                let mut out = entry_output(entry);
                if let Some(limit) = chars_budget {
                    if entry.prompt.len() > limit {
                        out["prompt"] = json!(entry.prompt.chars().take(limit).collect::<String>());
                    }
                }
                out["resume_cmd"] = json!(resume_command(entry));
                out
            })
            .collect::<Vec<_>>();
        println!(
            "{}",
            json!({
                "query": query_str,
                "generated_ms": generated_ms,
                "token_budget": tokens,
                "entries": entries,
            })
        );
        return Ok(());
    }
    let dt = Local
        .timestamp_millis_opt(generated_ms)
        .single()
        .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
        .unwrap_or_default();
    println!(
        "=== ai-hist pack: \"{query_str}\" | {dt} | {} entries ===\n",
        rows.len()
    );
    for (idx, entry) in rows.iter().enumerate() {
        let entry_dt = Local
            .timestamp_millis_opt(entry.timestamp_ms)
            .single()
            .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
            .unwrap_or_default();
        let project = entry
            .project
            .as_ref()
            .map(|p| format!("  {p}"))
            .unwrap_or_default();
        let mut text = entry.prompt.replace('\n', " ");
        if let Some(limit) = chars_budget {
            if text.len() > limit {
                text = format!("{}...", text.chars().take(limit).collect::<String>());
            }
        }
        println!(
            "[{}/{}] #{}  {}  {}{}",
            idx + 1,
            rows.len(),
            entry.id,
            entry_dt,
            entry.source,
            project
        );
        println!("      {text}");
        if let Some(session_id) = &entry.session_id {
            if let Some(cmd) = resume_command(entry) {
                println!("      Resume: {cmd}");
            } else {
                let short = if session_id.len() > 16 {
                    format!("{}...", &session_id[..16])
                } else {
                    session_id.clone()
                };
                println!("      Session: {short}");
            }
        }
        println!();
    }
    Ok(())
}

fn print_tags(
    conn: &Connection,
    tag: Option<&str>,
    include_sessions: bool,
    as_json: bool,
) -> Result<()> {
    let tag_norm = tag.map(normalize_tag_name);
    let (where_sql, params_vec) = if let Some(tag) = &tag_norm {
        ("WHERE t.name = ?".to_string(), vec![tag.as_str()])
    } else {
        (String::new(), Vec::new())
    };
    let mut stmt = conn.prepare(&format!(
        "SELECT t.name, t.display_name, t.color, COUNT(st.id), MIN(st.created_ms), MAX(st.created_ms) \
         FROM tags t LEFT JOIN session_tags st ON st.tag_id = t.id {where_sql} \
         GROUP BY t.id, t.name, t.display_name, t.color ORDER BY t.name"
    ))?;
    let rows = stmt
        .query_map(rusqlite::params_from_iter(params_vec), |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, Option<i64>>(5)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if as_json {
        let mut out = Vec::new();
        for (name, display_name, color, count, first, last) in &rows {
            let mut item = json!({
                "name": name,
                "display_name": display_name,
                "color": color,
                "session_count": count,
                "first_tagged_ms": first,
                "last_tagged_ms": last,
            });
            if include_sessions {
                item["sessions"] = json!(tagged_sessions(conn, name)?);
            }
            out.push(item);
        }
        println!("{}", serde_json::to_string(&out)?);
        return Ok(());
    }
    if rows.is_empty() {
        println!("No tags.");
        return Ok(());
    }
    for (name, display_name, color, count, _, _) in rows {
        let color_text = color.map(|c| format!(" [{c}]")).unwrap_or_default();
        println!("  {display_name}{color_text}  {count} session(s)");
        if include_sessions {
            for session in tagged_sessions(conn, &name)? {
                let project = session["project"]
                    .as_str()
                    .map(|p| format!(" [{p}]"))
                    .unwrap_or_default();
                println!(
                    "    {}:{}{} ({} entries)",
                    session["source"].as_str().unwrap_or(""),
                    session["session_id"].as_str().unwrap_or(""),
                    project,
                    session["entry_count"]
                );
            }
        }
    }
    Ok(())
}

fn export_history(
    conn: &Connection,
    output: Option<&Path>,
    format: &str,
    source: Option<&str>,
    project: Option<&str>,
    since: Option<&str>,
) -> Result<()> {
    let rows = export_rows(conn, source, project, since)?;
    if rows.is_empty() {
        anyhow::bail!("No entries matched the export filters.");
    }
    if format == "sqlite" {
        let dest = output.unwrap_or_else(|| Path::new("ai-hist-export.db"));
        let db_path = default_db_path();
        anyhow::ensure!(
            dest != db_path,
            "Refusing to export SQLite over the active AI_HIST_DB."
        );
        let _ = fs::remove_file(dest);
        let dst = Connection::open(dest)?;
        ai_hist_core::init_db(&dst)?;
        let mut inserted = 0;
        for entry in &rows {
            inserted += insert_history(&dst, entry)?;
        }
        println!("Exported {inserted} entries to {}", dest.display());
        return Ok(());
    }
    anyhow::ensure!(format == "jsonl", "unsupported export format '{format}'");
    let mut body = Vec::new();
    for entry in &rows {
        let row = json!({
            "source": entry.source,
            "session_id": entry.session_id,
            "project": entry.project,
            "prompt": entry.prompt,
            "prompt_hash": entry.prompt_hash.clone().unwrap_or_else(|| prompt_hash(&entry.prompt)),
            "timestamp_ms": entry.timestamp_ms,
        });
        writeln!(&mut body, "{}", serde_json::to_string(&row)?)?;
    }
    if let Some(path) = output {
        if path.extension().and_then(|s| s.to_str()) == Some("gz") {
            let file = fs::File::create(path)?;
            let mut enc = GzEncoder::new(file, Compression::default());
            enc.write_all(&body)?;
            enc.finish()?;
        } else {
            fs::write(path, body)?;
        }
        eprintln!("Exported {} entries to {}", rows.len(), path.display());
    } else {
        io::stdout().write_all(&body)?;
    }
    Ok(())
}

fn import_history(conn: &Connection, path: &Path, dry_run: bool) -> Result<()> {
    let entries = if matches!(
        path.extension().and_then(|s| s.to_str()),
        Some("db" | "sqlite")
    ) {
        load_sqlite_entries(path)?
    } else {
        load_jsonl_entries(path)?
    };
    if entries.is_empty() {
        println!("No entries found in file.");
        return Ok(());
    }
    if dry_run {
        println!(
            "[dry-run] {} entries in {} - none written.",
            entries.len(),
            path.display()
        );
        println!();
        for entry in entries.iter().take(5) {
            println!(
                "  {}  ({}){}  {}",
                format_datetime(entry.timestamp_ms),
                entry.source,
                entry
                    .project
                    .as_ref()
                    .map(|p| format!(" [{p}]"))
                    .unwrap_or_default(),
                entry
                    .prompt
                    .chars()
                    .take(80)
                    .collect::<String>()
                    .replace('\n', " ")
            );
        }
        if entries.len() > 5 {
            println!("  ... and {} more", entries.len() - 5);
        }
        return Ok(());
    }
    let total = entries.len();
    let inserted = import_json(conn, &entries)?;
    let skipped = total.saturating_sub(inserted);
    let mut parts = vec![format!("+{inserted} new entries")];
    if skipped > 0 {
        parts.push(format!("{skipped} already existed"));
    }
    println!("Imported from {}: {}", path.display(), parts.join(", "));
    Ok(())
}

/// Sidecar paths SQLite keeps beside the database in WAL mode.
fn wal_path(db_path: &Path) -> PathBuf {
    PathBuf::from(format!("{}-wal", db_path.display()))
}

/// Free bytes on the filesystem holding `path`.
///
/// Shells out to `df` rather than taking a libc dependency for one number;
/// this crate already shells out to `git` for the same reason.
fn free_bytes(path: &Path) -> Option<u64> {
    // df needs an existing path: fall back to the parent for a database that
    // has not been created yet.
    let target = if path.exists() {
        path.to_path_buf()
    } else {
        path.parent()?.to_path_buf()
    };
    let out = std::process::Command::new("df")
        .arg("-Pk")
        .arg(&target)
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&out.stdout);
    // Filesystem  1024-blocks  Used  Available  Capacity  Mounted-on
    let available_kb: u64 = text
        .lines()
        .nth(1)?
        .split_whitespace()
        .nth(3)?
        .parse()
        .ok()?;
    Some(available_kb * 1024)
}

/// A process holding the database file open, and whether it can still release it.
struct DbHolder {
    pid: String,
    state: String,
    command: String,
}

impl DbHolder {
    /// Stopped (`T`) and zombie (`Z`) processes never run again on their own, so
    /// a write transaction they hold is held forever -- no busy timeout escapes
    /// it. This is the condition that wedged sync for days.
    fn is_wedged(&self) -> bool {
        self.state.starts_with('T') || self.state.starts_with('Z')
    }
}

/// Processes with the database open, via `lsof`, annotated with `ps` state.
fn db_holders(db_path: &Path) -> Vec<DbHolder> {
    // macOS keeps lsof in /usr/sbin, which is commonly absent from the PATH of
    // launchd jobs and embedded hosts. Try PATH first, then stable system
    // locations so automatic diagnostics do not silently lose their evidence.
    let out = ["lsof", "/usr/sbin/lsof", "/usr/bin/lsof"]
        .iter()
        .find_map(|program| {
            std::process::Command::new(program)
                .arg("-t")
                .arg(db_path)
                .output()
                .ok()
                .filter(|out| out.status.success())
        });
    let Some(out) = out else {
        return Vec::new();
    };
    let own_pid = std::process::id().to_string();
    String::from_utf8_lossy(&out.stdout)
        .split_whitespace()
        // Our own read-only handle is not a finding.
        .filter(|pid| *pid != own_pid)
        .filter_map(|pid| {
            let (state, command) = process_status(pid)?;
            Some(DbHolder {
                pid: pid.to_string(),
                state,
                command,
            })
        })
        .collect()
}

/// Process state with stable-path fallbacks for reduced-PATH launchd and embedded hosts.
fn process_status(pid: &str) -> Option<(String, String)> {
    process_status_with_programs(pid, &["ps", "/bin/ps", "/usr/bin/ps"])
}

fn process_status_with_programs(pid: &str, programs: &[&str]) -> Option<(String, String)> {
    programs.iter().find_map(|program| {
        let output = std::process::Command::new(program)
            .args(["-o", "stat=,command=", "-p", pid])
            .output()
            .ok()
            .filter(|output| output.status.success())?;
        let line = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let (state, command) = line.split_once(char::is_whitespace)?;
        Some((state.to_string(), command.trim().to_string()))
    })
}

/// Can a writer actually start right now?
///
/// Uses a short timeout on purpose: `doctor` should report a wedged database
/// promptly rather than inherit the production retry sequence.
fn probe_write_lock(db_path: &Path) -> std::result::Result<(), String> {
    let conn = Connection::open(db_path).map_err(|err| err.to_string())?;
    let _ = conn.busy_timeout(Duration::from_millis(1500));
    conn.execute_batch("BEGIN IMMEDIATE; ROLLBACK;")
        .map_err(|err| err.to_string())
}

fn is_sqlite_contention(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<rusqlite::Error>(),
            Some(rusqlite::Error::SqliteFailure(code, _))
                if matches!(code.code, ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
        )
    })
}

/// Explain a write-contention failure using a fresh capability probe.
///
/// `lsof` proves only that a process has the file open. The `BEGIN IMMEDIATE`
/// probe below establishes whether a writer can actually start *now*; holder
/// output is deliberately phrased as causal only while that probe is blocked.
fn write_contention_diagnostic(db_path: &Path) -> String {
    let lock = probe_write_lock(db_path);
    let holders = db_holders(db_path);
    let wal_bytes = fs::metadata(wal_path(db_path))
        .map(|m| m.len())
        .unwrap_or(0);
    let mut lines = vec![match &lock {
        Ok(()) => "write capability probe now succeeds; the contention was transient".to_string(),
        Err(err) => format!("write capability probe is still blocked: {err}"),
    }];

    if lock.is_err() {
        let wedged: Vec<_> = holders.iter().filter(|holder| holder.is_wedged()).collect();
        if wedged.is_empty() {
            if holders.is_empty() {
                lines.push(
                    "no file-open holder was detected; SQLite does not expose lock ownership"
                        .to_string(),
                );
            } else {
                lines.push(format!(
                    "{} process(es) have the database open, but none is stopped or zombie; file-open status does not prove lock ownership",
                    holders.len()
                ));
            }
        } else {
            for holder in wedged {
                lines.push(format!(
                    "pid {} is {} with the database open; if it owns the transaction it cannot release it until resumed (kill -CONT {})",
                    holder.pid, holder.state, holder.pid
                ));
            }
        }
    }
    if let Some(wal_line) = wal_contention_line(wal_bytes, lock.is_err()) {
        lines.push(wal_line);
    }

    format!("ai-hist contention diagnostic: {}", lines.join("; "))
}

fn wal_contention_line(wal_bytes: u64, write_blocked: bool) -> Option<String> {
    if wal_bytes <= WAL_WARN_BYTES {
        return None;
    }
    Some(if write_blocked {
        format!(
            "WAL is {} while the write path is failing; checkpoint progress is starved",
            human_bytes(wal_bytes)
        )
    } else {
        format!(
            "WAL is {} after write capability recovered; a long-lived reader may still be delaying checkpoints",
            human_bytes(wal_bytes)
        )
    })
}

fn enrich_sync_error(db_path: &Path, error: anyhow::Error) -> anyhow::Error {
    if is_sqlite_contention(&error) {
        let diagnostic_path = source_database_path(&error)
            .map(Path::to_path_buf)
            .unwrap_or_else(|| db_path.to_path_buf());
        error.context(write_contention_diagnostic(&diagnostic_path))
    } else {
        error
    }
}

fn source_database_path(error: &anyhow::Error) -> Option<&Path> {
    error.chain().find_map(|cause| {
        cause
            .downcast_ref::<SourceDatabaseError>()
            .map(SourceDatabaseError::path)
    })
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["B", "KB", "MB", "GB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

/// WAL beyond this points at checkpoint starvation: a long-lived reader is
/// pinning an old snapshot so SQLite cannot reclaim frames.
const WAL_WARN_BYTES: u64 = 64 * 1024 * 1024;

/// Below this, a write can fail partway and leave torn state behind, which is
/// how the `.sync-state.json` corruption started.
const FREE_SPACE_FLOOR_BYTES: u64 = 512 * 1024 * 1024;

/// Everything `doctor` measured about one database, before it is rendered as
/// text or JSON. Split out from the printing so the report can be asserted on.
struct DoctorReport {
    db_bytes: u64,
    wal_bytes: u64,
    free: Option<u64>,
    lock: std::result::Result<(), String>,
    holders: Vec<DbHolder>,
    problems: Vec<String>,
}

fn doctor_report(db_path: &Path) -> DoctorReport {
    let db_bytes = fs::metadata(db_path).map(|m| m.len()).unwrap_or(0);
    let wal_bytes = fs::metadata(wal_path(db_path))
        .map(|m| m.len())
        .unwrap_or(0);
    let free = free_bytes(db_path);
    let lock = probe_write_lock(db_path);
    let holders = db_holders(db_path);

    let mut problems: Vec<String> = Vec::new();
    if let Err(err) = &lock {
        problems.push(format!("write lock unavailable: {err}"));
    }
    // Having the file open is not the same as owning a write transaction, and
    // SQLite will not say who holds the lock. So only assert causation when a
    // writer is actually blocked; otherwise report the stopped process as a
    // risk, which is true without overclaiming.
    for holder in holders.iter().filter(|h| h.is_wedged()) {
        if lock.is_err() {
            problems.push(format!(
                "pid {} is {} and holds the database open; if it is mid-transaction it can never release the write lock (resume it: kill -CONT {})",
                holder.pid, holder.state, holder.pid
            ));
        } else {
            problems.push(format!(
                "pid {} is {} and holds the database open; writes work now, but it will wedge them if it stops mid-transaction (resume it: kill -CONT {})",
                holder.pid, holder.state, holder.pid
            ));
        }
    }
    if wal_bytes > WAL_WARN_BYTES {
        problems.push(format!(
            "WAL is {} -- checkpointing is starved, usually by a long-lived reader",
            human_bytes(wal_bytes)
        ));
    }
    if free.is_some_and(|free| free < FREE_SPACE_FLOOR_BYTES) {
        problems.push(format!(
            "only {} free -- writes can fail partway and leave torn state",
            human_bytes(free.unwrap_or(0))
        ));
    }

    DoctorReport {
        db_bytes,
        wal_bytes,
        free,
        lock,
        holders,
        problems,
    }
}

impl DoctorReport {
    fn to_json(&self, db_path: &Path) -> Value {
        json!({
            "db_path": db_path.display().to_string(),
            "db_bytes": self.db_bytes,
            "wal_bytes": self.wal_bytes,
            "free_bytes": self.free,
            "write_lock": match &self.lock {
                Ok(()) => json!("available"),
                Err(err) => json!({"blocked": err}),
            },
            "write_capable": self.lock.is_ok(),
            "holders": self.holders.iter().map(|h| json!({
                "pid": h.pid,
                "state": h.state,
                "command": h.command,
                "wedged": h.is_wedged(),
            })).collect::<Vec<_>>(),
            "problems": self.problems,
        })
    }
}

fn doctor(db_path: &Path, json: bool) -> Result<()> {
    let report = doctor_report(db_path);
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report.to_json(db_path))?
        );
        return Ok(());
    }
    let DoctorReport {
        db_bytes,
        wal_bytes,
        free,
        lock,
        holders,
        problems,
    } = report;

    println!("database: {}", db_path.display());
    println!("  size:  {}", human_bytes(db_bytes));
    println!("  WAL:   {}", human_bytes(wal_bytes));
    println!(
        "  free:  {}",
        free.map(human_bytes).unwrap_or_else(|| "unknown".into())
    );
    match &lock {
        Ok(()) => println!("  write lock: available"),
        Err(err) => println!("  write lock: BLOCKED ({err})"),
    }
    if holders.is_empty() {
        println!("  holders: none detected");
    } else {
        println!("  holders:");
        for holder in &holders {
            let flag = if holder.is_wedged() {
                "  <-- WEDGED"
            } else {
                ""
            };
            println!(
                "    pid {:<8} {:<5} {}{flag}",
                holder.pid, holder.state, holder.command
            );
        }
    }
    if problems.is_empty() {
        println!("\nDatabase write capability is healthy.");
    } else {
        println!("\nProblems:");
        for problem in &problems {
            println!("  - {problem}");
        }
    }
    Ok(())
}

#[derive(Default)]
struct SyncSourceReport {
    succeeded: usize,
    failures: Vec<SyncSourceFailure>,
}

struct SyncSourceFailure {
    source: String,
    error: String,
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
                    error: format!("{error:#}"),
                    is_contention: is_sqlite_contention(&error),
                    contention_path: source_database_path(&error).map(Path::to_path_buf),
                });
                None
            }
        }
    }

    fn finish(&self, db_path: &Path) -> Result<()> {
        if self.failures.is_empty() {
            return Ok(());
        }

        eprintln!(
            "ai-hist: {} history source(s) failed; {} source(s) completed:",
            self.failures.len(),
            self.succeeded
        );
        for failure in &self.failures {
            eprintln!("  [{}] {}", failure.source, failure.error);
        }
        let mut diagnosed = HashSet::new();
        for failure in self.failures.iter().filter(|failure| failure.is_contention) {
            let path = failure.contention_path.as_deref().unwrap_or(db_path);
            if diagnosed.insert(path.to_path_buf()) {
                eprintln!("{}", write_contention_diagnostic(path));
            }
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

fn sync_basic(conn: &Connection, db_path: &Path) -> Result<()> {
    let home = home_dir();
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
    if report
        .capture(
            "claude-metadata",
            sync_claude_session_metadata(conn, &mut state, &home.join(".claude/projects")),
        )
        .is_some()
    {
        checkpoint_sync_state(&state_path, &state);
    }
    if let Some(inserted) = report.capture("codex", sync_codex(conn, &mut state, &home)) {
        total_inserted += inserted;
        checkpoint_sync_state(&state_path, &state);
    }
    if let Some(inserted) = report.capture(
        "cursor",
        sync_cursor(conn, &mut state, &home.join(".cursor/projects")),
    ) {
        total_inserted += inserted;
        checkpoint_sync_state(&state_path, &state);
    }
    if let Some(inserted) = report.capture(
        "grok",
        sync_grok(conn, &mut state, &home.join(".grok/sessions")),
    ) {
        total_inserted += inserted;
        checkpoint_sync_state(&state_path, &state);
    }
    if let Some(inserted) = report.capture("trajectory", sync_trajectories(conn, &mut state)) {
        total_inserted += inserted;
        checkpoint_sync_state(&state_path, &state);
    }
    let opencode = std::env::var_os("OPENCODE_DB")
        .map(PathBuf::from)
        .unwrap_or_else(default_opencode_db_path);
    if let Some(open_inserted) = report.capture("opencode", sync_opencode_db(conn, &opencode)) {
        if opencode.exists() {
            sync_note!("  [opencode] +{open_inserted} rows");
        } else {
            sync_note!("  [opencode] not found: {} (skipped)", opencode.display());
        }
        total_inserted += open_inserted;
    }
    if let Some(inserted) = report.capture("relay", sync_relaycast(conn, &mut state)) {
        total_inserted += inserted;
        checkpoint_sync_state(&state_path, &state);
    }
    report.finish(db_path)?;
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
        let _ = fs2::FileExt::unlock(&self._file);
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
    match fs2::FileExt::try_lock_exclusive(&file) {
        Ok(()) => Ok(Some(SyncRunLock { _file: file })),
        Err(err) if err.kind() == io::ErrorKind::WouldBlock => Ok(None),
        Err(err) => Err(err.into()),
    }
}

fn sync_exclusive(db_path: &Path) -> Result<bool> {
    let Some(_sync_lock) = try_acquire_sync_lock(db_path)? else {
        sync_note!("  [sync] another sync is already running; skipped");
        return Ok(false);
    };
    let conn = open_db(db_path).map_err(|error| enrich_sync_error(db_path, error))?;
    sync_basic(&conn, db_path).map_err(|error| enrich_sync_error(db_path, error))?;
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
    Ok(true)
}

fn prepare_sync_and_push_db(db_path: &Path) -> Result<(Connection, bool)> {
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
    sync_basic(&conn, db_path).map_err(|error| enrich_sync_error(db_path, error))?;
    drop(sync_lock);
    Ok((conn, false))
}

fn watch_loop(db_path: &Path, interval: u64, scope: SessionScope) -> Result<()> {
    println!("Watching every {interval}s (Ctrl-C to stop)...");
    loop {
        match sync_scope_exclusive(db_path, scope) {
            Ok(_) => {}
            Err(err) => eprintln!("Error: {err:#}"),
        }
        std::thread::sleep(Duration::from_secs(interval));
    }
}

/// A background service managed by ai-hist. Both the local `sync` job and the
/// cloud `push` job share the same launchd/cron plumbing; only these fields
/// differ.
struct ServiceSpec {
    /// launchd label and plist basename stem, e.g. "com.ai-hist.sync".
    label: &'static str,
    /// ai-hist subcommand the service runs, e.g. "sync" or "push".
    subcommand: &'static str,
    /// `/tmp/<log_stem>.log` and `.err` capture the service's output.
    log_stem: &'static str,
    /// Human-facing noun for messages, e.g. "sync" or "cloud push".
    human: &'static str,
}

const SYNC_SERVICE: ServiceSpec = ServiceSpec {
    label: "com.ai-hist.sync",
    subcommand: "sync",
    log_stem: "ai-hist-sync",
    human: "sync",
};

const PUSH_SERVICE: ServiceSpec = ServiceSpec {
    label: "com.ai-hist.push",
    subcommand: "push",
    log_stem: "ai-hist-push",
    human: "cloud push",
};

/// The comment marker that identifies this service's managed crontab line.
fn cron_marker(spec: &ServiceSpec) -> String {
    format!("# ai-hist {} (managed)", spec.subcommand)
}

fn launchd_plist_path(spec: &ServiceSpec) -> PathBuf {
    home_dir().join(format!("Library/LaunchAgents/{}.plist", spec.label))
}

/// Resolve the absolute path of the running ai-hist binary so the service
/// invokes it directly rather than through a development launcher.
fn service_binary() -> Result<PathBuf> {
    std::env::current_exe().context("could not resolve the ai-hist binary path for the service")
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn service_command_args(spec: &ServiceSpec, args: &[String]) -> Vec<String> {
    let mut command = Vec::with_capacity(args.len() + 1);
    command.push(spec.subcommand.to_string());
    command.extend(args.iter().cloned());
    command
}

fn install_managed_service(spec: &ServiceSpec, interval: u64, args: &[String]) -> Result<()> {
    let bin = service_binary()?;
    let bin = bin.to_string_lossy();
    if cfg!(target_os = "macos") {
        install_launchd_service(spec, &bin, interval, args)
    } else if cfg!(target_os = "linux") {
        install_cron_service(spec, &bin, interval, args)
    } else {
        anyhow::bail!(
            "Automatic {} service install is only supported on macOS and Linux. \
             Schedule `ai-hist {}` yourself (e.g. via your platform's task scheduler) instead.",
            spec.human,
            spec.subcommand
        )
    }
}

/// Single-quote a path for a crontab line so spaces / shell metacharacters in
/// the binary path don't break the scheduled command.
fn shell_single_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// Smallest divisor of `base` that is `>= n`. Using a divisor keeps a `*/step`
/// cron field uniform — a non-divisor step (e.g. `*/45`) fires a short interval
/// at the field's rollover (`:00, :45, :00` → a 15-minute gap).
fn round_up_to_divisor(n: u64, base: u64) -> u64 {
    (n..=base).find(|d| base.is_multiple_of(*d)).unwrap_or(base)
}

/// Returns `(cron expression, human cadence, effective period in seconds)`. The
/// effective period lets callers detect when the interval was rounded. cron
/// can't match every interval exactly; we always round toward a *coarser*,
/// uniform cadence so a scheduled push never fires more often than requested.
fn cron_schedule(interval: u64) -> (String, String, u64) {
    // Sub-two-minute intervals can only be "every minute".
    if interval < 120 {
        return ("* * * * *".to_string(), "every minute".to_string(), 60);
    }
    let minutes = interval / 60; // floor; >= 2 here
    if minutes < 60 {
        // Uniform minute steps require a divisor of 60; round up to the next one.
        let step = round_up_to_divisor(minutes, 60);
        if step < 60 {
            return (
                format!("*/{step} * * * *"),
                format!("every {step} minutes"),
                step * 60,
            );
        }
        return ("0 * * * *".to_string(), "every hour".to_string(), 3600);
    }
    // Round up to whole hours (e.g. 90 min -> 2h), then to a uniform hour step.
    let hours = minutes.div_ceil(60);
    if hours < 24 {
        let step = round_up_to_divisor(hours, 24);
        if step < 24 {
            return (
                format!("0 */{step} * * *"),
                format!("every {step} hour(s)"),
                step * 3600,
            );
        }
        return ("0 0 * * *".to_string(), "once a day".to_string(), 86_400);
    }
    // A day or longer: run once daily at midnight.
    ("0 0 * * *".to_string(), "once a day".to_string(), 86_400)
}

fn install_launchd_service(
    spec: &ServiceSpec,
    bin: &str,
    interval: u64,
    args: &[String],
) -> Result<()> {
    let plist_path = launchd_plist_path(spec);
    if let Some(dir) = plist_path.parent() {
        fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    let command_args = service_command_args(spec, args)
        .iter()
        .map(|arg| format!("        <string>{}</string>", xml_escape(arg)))
        .collect::<Vec<_>>()
        .join("\n");
    let plist = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{label}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{bin}</string>
{command_args}
    </array>
    <key>StartInterval</key>
    <integer>{interval}</integer>
    <key>RunAtLoad</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/tmp/{log_stem}.log</string>
    <key>StandardErrorPath</key>
    <string>/tmp/{log_stem}.err</string>
</dict>
</plist>
"#,
        label = spec.label,
        bin = xml_escape(bin),
        command_args = command_args,
        interval = interval,
        log_stem = spec.log_stem,
    );
    fs::write(&plist_path, plist).with_context(|| format!("writing {}", plist_path.display()))?;

    // Reload idempotently: unload any previous version (ignoring errors), then load.
    let _ = std::process::Command::new("launchctl")
        .arg("unload")
        .arg(&plist_path)
        .status();
    let status = std::process::Command::new("launchctl")
        .arg("load")
        .arg(&plist_path)
        .status()
        .context("running launchctl load")?;
    if !status.success() {
        anyhow::bail!("launchctl load failed for {}", plist_path.display());
    }

    println!(
        "Installed launchd {} service ({}); running every {interval}s.",
        spec.human, spec.label
    );
    println!("  plist: {}", plist_path.display());
    println!("  check: launchctl list | grep ai-hist   (middle column 0 = healthy)");
    println!("  remove: ai-hist {} --uninstall-service", spec.subcommand);
    Ok(())
}

fn read_crontab() -> String {
    match std::process::Command::new("crontab").arg("-l").output() {
        Ok(out) if out.status.success() => String::from_utf8_lossy(&out.stdout).into_owned(),
        // No crontab yet (or `crontab -l` errors on an empty table) — start fresh.
        _ => String::new(),
    }
}

fn write_crontab(contents: &str) -> Result<()> {
    let mut child = std::process::Command::new("crontab")
        .arg("-")
        .stdin(std::process::Stdio::piped())
        .spawn()
        .context("running `crontab -` (is cron installed?)")?;
    child
        .stdin
        .take()
        .context("failed to open crontab stdin")?
        .write_all(contents.as_bytes())?;
    let status = child.wait()?;
    if !status.success() {
        anyhow::bail!("crontab update failed");
    }
    Ok(())
}

fn install_cron_service(
    spec: &ServiceSpec,
    bin: &str,
    interval: u64,
    args: &[String],
) -> Result<()> {
    let (schedule, cadence, effective) = cron_schedule(interval);
    // cron can't match every interval exactly; the confirmation below states the
    // cadence actually scheduled. Only note a mismatch when it isn't exact, so a
    // plain `--interval=300` install doesn't imply the user picked an odd value.
    if effective != interval {
        eprintln!(
            "Note: cron runs at 1-minute granularity; --interval={interval}s scheduled as {cadence}."
        );
    }
    let marker = cron_marker(spec);
    let command = std::iter::once(shell_single_quote(bin))
        .chain(
            service_command_args(spec, args)
                .iter()
                .map(|arg| shell_single_quote(arg)),
        )
        .collect::<Vec<_>>()
        .join(" ");
    let line = format!(
        "{schedule} {command} >> /tmp/{}.log 2>&1 {marker}",
        spec.log_stem
    );
    // Drop any previously managed line, then append the current one.
    let mut lines: Vec<String> = read_crontab()
        .lines()
        .filter(|l| !l.contains(&marker))
        .map(str::to_string)
        .collect();
    lines.push(line);
    write_crontab(&format!("{}\n", lines.join("\n")))?;

    println!("Installed cron {} job; running {cadence}.", spec.human);
    println!("  view:   crontab -l");
    println!("  remove: ai-hist {} --uninstall-service", spec.subcommand);
    Ok(())
}

fn uninstall_managed_service(spec: &ServiceSpec) -> Result<()> {
    if cfg!(target_os = "macos") {
        let plist_path = launchd_plist_path(spec);
        let _ = std::process::Command::new("launchctl")
            .arg("unload")
            .arg(&plist_path)
            .status();
        if plist_path.exists() {
            fs::remove_file(&plist_path)
                .with_context(|| format!("removing {}", plist_path.display()))?;
            println!("Removed launchd {} service.", spec.human);
        } else {
            println!("No launchd {} service installed.", spec.human);
        }
        Ok(())
    } else if cfg!(target_os = "linux") {
        let marker = cron_marker(spec);
        let kept: Vec<String> = read_crontab()
            .lines()
            .filter(|l| !l.contains(&marker))
            .map(str::to_string)
            .collect();
        write_crontab(&format!("{}\n", kept.join("\n")))?;
        println!("Removed cron {} job.", spec.human);
        Ok(())
    } else {
        anyhow::bail!("No managed {} service exists on this platform.", spec.human)
    }
}

const GIT_HOOK_MARKER_BEGIN: &str = "# ai-hist session commit link (managed begin)";
const GIT_HOOK_MARKER_END: &str = "# ai-hist session commit link (managed end)";
const AI_HIST_NOTE_REF: &str = "ai-hist";

#[derive(Debug, Clone)]
struct SessionCandidate {
    source: String,
    session_id: String,
    confidence: f64,
    evidence: Value,
}

fn setup_git_hook(db_path: &Path, repo: &Path, uninstall: bool) -> Result<()> {
    let root = git_repo_root(repo)?;
    let hook_path = git_path(&root, "hooks/post-commit")?;
    if uninstall {
        uninstall_git_hook(&hook_path)?;
        return Ok(());
    }
    if let Some(parent) = hook_path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
    }
    let existing = fs::read_to_string(&hook_path).unwrap_or_default();
    anyhow::ensure!(
        existing.trim().is_empty() || existing.contains(GIT_HOOK_MARKER_BEGIN),
        "{} already exists and is not managed by ai-hist; install manually or remove it first",
        hook_path.display()
    );
    let bin = service_binary()?;
    let block = format!(
        r#"#!/bin/sh
{begin}
AI_HIST_DB={db} {bin} link commit --repo {repo} --commit HEAD --match-method git_note --quiet >/dev/null 2>>/tmp/ai-hist-git-link.err || true
{end}
"#,
        begin = GIT_HOOK_MARKER_BEGIN,
        end = GIT_HOOK_MARKER_END,
        db = sh_single_quote(&db_path.display().to_string()),
        bin = sh_single_quote(&bin.display().to_string()),
        repo = sh_single_quote(&root.display().to_string()),
    );
    fs::write(&hook_path, block).with_context(|| format!("writing {}", hook_path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&hook_path)?.permissions();
        perms.set_mode(perms.mode() | 0o755);
        fs::set_permissions(&hook_path, perms)?;
    }
    println!("Installed ai-hist post-commit hook.");
    println!("  repo: {}", root.display());
    println!("  hook: {}", hook_path.display());
    println!("  rows: session_commit_links");
    println!("  notes: refs/notes/{AI_HIST_NOTE_REF}");
    Ok(())
}

fn uninstall_git_hook(hook_path: &Path) -> Result<()> {
    if !hook_path.exists() {
        println!("No ai-hist post-commit hook installed.");
        return Ok(());
    }
    let existing = fs::read_to_string(hook_path)?;
    anyhow::ensure!(
        existing.contains(GIT_HOOK_MARKER_BEGIN),
        "{} is not managed by ai-hist; refusing to remove it",
        hook_path.display()
    );
    fs::remove_file(hook_path)?;
    println!("Removed ai-hist post-commit hook: {}", hook_path.display());
    Ok(())
}

fn sh_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn link_git_commit(
    conn: &Connection,
    _db_path: &Path,
    repo: &Path,
    commit: &str,
    match_method: &str,
    write_note: bool,
    as_json: bool,
    quiet: bool,
) -> Result<()> {
    let root = git_repo_root(repo)?;
    let commit_sha = git_stdout(&root, &["rev-parse", commit])?;
    let commit_sha = commit_sha.trim();
    let commit_ms = git_commit_time_ms(&root, commit_sha)?;
    let branch = git_branch(&root).ok();
    let repo_remote = git_remote(&root).ok();
    let files = git_commit_files(&root, commit_sha)?;
    let numstat = git_commit_numstat(&root, commit_sha)?;
    let candidate = find_session_for_commit(conn, &root, branch.as_deref(), commit_ms, &files)?;
    let Some(candidate) = candidate else {
        if as_json {
            println!(
                "{}",
                json!({
                    "linked": false,
                    "repo": root,
                    "commit_sha": commit_sha,
                    "reason": "no matching session"
                })
            );
        } else if !quiet {
            println!(
                "No matching session found for {commit_sha} in {}",
                root.display()
            );
        }
        return Ok(());
    };
    let files_json = serde_json::to_string(&files)?;
    let numstat_json = serde_json::to_string(&numstat)?;
    let created_at_ms = chrono::Utc::now().timestamp_millis();
    let mut note_ref = None;
    let evidence = json!({
        "repo_path": root,
        "repo_remote": repo_remote,
        "branch": branch,
        "commit_time_ms": commit_ms,
        "candidate": candidate.evidence,
        "files": files,
        "numstat": numstat,
    });
    if write_note {
        let note = json!({
            "schema": "ai-hist.session_commit_link.v1",
            "source": candidate.source,
            "session_id": candidate.session_id,
            "repo": root,
            "branch": branch,
            "commit_sha": commit_sha,
            "match_method": match_method,
            "confidence": candidate.confidence,
            "created_at_ms": created_at_ms,
        });
        let note_string = serde_json::to_string(&note)?;
        let note_status = git_status(
            &root,
            &[
                "notes",
                &format!("--ref={AI_HIST_NOTE_REF}"),
                "add",
                "-f",
                "-m",
                &note_string,
                commit_sha,
            ],
        );
        match note_status {
            Ok(()) => note_ref = Some(format!("refs/notes/{AI_HIST_NOTE_REF}")),
            Err(err) if !quiet => eprintln!("ai-hist: could not write git note: {err}"),
            Err(_) => {}
        }
    }
    ai_hist_core::mark_session_presence(
        conn,
        &candidate.source,
        &candidate.session_id,
        SessionLocation::Local,
    )?;
    conn.execute(
        "INSERT INTO session_commit_links \
         (source, session_id, repo, branch, commit_sha, note_ref, match_method, confidence, files_json, numstat_json, evidence_json, created_at_ms) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(source, session_id, commit_sha, match_method) DO UPDATE SET \
           repo=excluded.repo, branch=excluded.branch, note_ref=excluded.note_ref, confidence=excluded.confidence, \
           files_json=excluded.files_json, numstat_json=excluded.numstat_json, evidence_json=excluded.evidence_json, created_at_ms=excluded.created_at_ms",
        params![
            candidate.source,
            candidate.session_id,
            root.display().to_string(),
            branch,
            commit_sha,
            note_ref,
            match_method,
            candidate.confidence,
            files_json,
            numstat_json,
            serde_json::to_string(&evidence)?,
            created_at_ms,
        ],
    )?;
    let out = json!({
        "linked": true,
        "source": candidate.source,
        "session_id": candidate.session_id,
        "repo": root,
        "branch": branch,
        "commit_sha": commit_sha,
        "note_ref": note_ref,
        "match_method": match_method,
        "confidence": candidate.confidence,
        "files": files,
        "numstat": numstat,
        "evidence": evidence,
        "created_at_ms": created_at_ms,
    });
    if as_json {
        println!("{}", serde_json::to_string(&out)?);
    } else if !quiet {
        println!(
            "Linked {}:{} → {} ({match_method}, confidence {:.2})",
            out["source"].as_str().unwrap_or(""),
            out["session_id"].as_str().unwrap_or(""),
            commit_sha,
            out["confidence"].as_f64().unwrap_or(0.0)
        );
    }
    Ok(())
}

fn find_session_for_commit(
    conn: &Connection,
    repo_root: &Path,
    branch: Option<&str>,
    commit_ms: i64,
    files: &[String],
) -> Result<Option<SessionCandidate>> {
    let repo = repo_root.display().to_string();
    let repo_canonical = fs::canonicalize(repo_root).unwrap_or_else(|_| repo_root.to_path_buf());
    let min_ms = commit_ms - 36 * 60 * 60 * 1000;
    let max_ms = commit_ms + 6 * 60 * 60 * 1000;
    let mut stmt = conn.prepare(
        "SELECT source, session_id, cwd, git_branch, first_activity_ms, last_activity_ms \
         FROM sessions \
         WHERE session_id IS NOT NULL \
           AND COALESCE(last_activity_ms, first_activity_ms, 0) BETWEEN ? AND ?",
    )?;
    let rows = stmt
        .query_map(params![min_ms, max_ms], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, Option<i64>>(5)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut best: Option<SessionCandidate> = None;
    for (source, session_id, cwd, git_branch, first_activity_ms, last_activity_ms) in rows {
        let cwd_match = cwd
            .as_deref()
            .is_some_and(|cwd| cwd_matches_repo(cwd, &repo, &repo_canonical));
        let branch_match = match (branch, git_branch.as_deref()) {
            (Some(branch), Some(session_branch)) => branch == session_branch,
            _ => false,
        };
        if !cwd_match && !branch_match {
            continue;
        }
        let last = last_activity_ms.or(first_activity_ms).unwrap_or(commit_ms);
        let first = first_activity_ms.unwrap_or(last);
        let time_distance_ms = if commit_ms < first {
            first - commit_ms
        } else if commit_ms > last {
            commit_ms - last
        } else {
            0
        };
        let file_overlap = session_file_overlap(conn, &source, &session_id, files)?;
        let mut confidence: f64 = 0.45;
        if cwd_match {
            confidence += 0.20;
        }
        if branch_match {
            confidence += 0.20;
        }
        if time_distance_ms == 0 {
            confidence += 0.10;
        } else if time_distance_ms <= 2 * 60 * 60 * 1000 {
            confidence += 0.05;
        }
        if file_overlap > 0 {
            confidence += 0.05;
        }
        confidence = confidence.min(0.98);
        let evidence = json!({
            "cwd": cwd,
            "git_branch": git_branch,
            "first_activity_ms": first_activity_ms,
            "last_activity_ms": last_activity_ms,
            "cwd_match": cwd_match,
            "branch_match": branch_match,
            "time_distance_ms": time_distance_ms,
            "file_overlap": file_overlap,
        });
        let candidate = SessionCandidate {
            source,
            session_id,
            confidence,
            evidence,
        };
        if best
            .as_ref()
            .is_none_or(|current| candidate.confidence > current.confidence)
        {
            best = Some(candidate);
        }
    }
    Ok(best)
}

fn cwd_matches_repo(cwd: &str, repo: &str, repo_canonical: &Path) -> bool {
    if cwd == repo || cwd.starts_with(&(repo.to_string() + "/")) {
        return true;
    }
    let cwd_path = PathBuf::from(cwd);
    if let Ok(cwd_canonical) = fs::canonicalize(&cwd_path) {
        return cwd_canonical == repo_canonical || cwd_canonical.starts_with(repo_canonical);
    }
    false
}

fn session_file_overlap(
    conn: &Connection,
    source: &str,
    session_id: &str,
    files: &[String],
) -> Result<usize> {
    if files.is_empty() {
        return Ok(0);
    }
    let mut stmt = conn
        .prepare("SELECT DISTINCT file_path FROM file_edits WHERE source = ? AND session_id = ?")?;
    let session_files = stmt
        .query_map(params![source, session_id], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut overlap = 0;
    for file in files {
        if session_files
            .iter()
            .any(|session_file| paths_overlap(session_file, file))
        {
            overlap += 1;
        }
    }
    Ok(overlap)
}

fn paths_overlap(a: &str, b: &str) -> bool {
    fn normalize(path: &str) -> String {
        path.replace('\\', "/").trim_matches('/').to_string()
    }
    fn matches_suffix(path: &str, suffix: &str) -> bool {
        path == suffix || path.ends_with(&format!("/{suffix}"))
    }
    let a = normalize(a);
    let b = normalize(b);
    if a.is_empty() || b.is_empty() {
        return false;
    }
    matches_suffix(&a, &b) || matches_suffix(&b, &a)
}

fn export_commit_links(
    conn: &Connection,
    source: Option<&str>,
    repo: Option<&str>,
    since: Option<&str>,
    jsonl: bool,
) -> Result<()> {
    anyhow::ensure!(
        jsonl,
        "commit-link export is JSONL-only; pass `ai-hist export commit-links --jsonl`"
    );
    let since_ms = since.map(parse_date_ms).transpose()?;
    let mut sql = "SELECT source, session_id, repo, branch, commit_sha, note_ref, match_method, confidence, files_json, numstat_json, evidence_json, created_at_ms FROM session_commit_links WHERE 1=1".to_string();
    let mut params_vec = Vec::new();
    if let Some(source) = source {
        sql.push_str(" AND source = ?");
        params_vec.push(source.to_string());
    }
    if let Some(repo) = repo {
        sql.push_str(" AND repo LIKE ?");
        params_vec.push(format!("%{repo}%"));
    }
    if let Some(since_ms) = since_ms {
        sql.push_str(" AND created_at_ms >= ?");
        params_vec.push(since_ms.to_string());
    }
    sql.push_str(" ORDER BY created_at_ms ASC, id ASC");
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(params_vec), |row| {
        let files_json: Option<String> = row.get(8)?;
        let numstat_json: Option<String> = row.get(9)?;
        let evidence_json: Option<String> = row.get(10)?;
        Ok(json!({
            "source": row.get::<_, String>(0)?,
            "session_id": row.get::<_, String>(1)?,
            "repo": row.get::<_, String>(2)?,
            "branch": row.get::<_, Option<String>>(3)?,
            "commit_sha": row.get::<_, String>(4)?,
            "note_ref": row.get::<_, Option<String>>(5)?,
            "match_method": row.get::<_, String>(6)?,
            "confidence": row.get::<_, f64>(7)?,
            "files_json": files_json.as_deref().and_then(|s| serde_json::from_str::<Value>(s).ok()),
            "numstat_json": numstat_json.as_deref().and_then(|s| serde_json::from_str::<Value>(s).ok()),
            "evidence_json": evidence_json.as_deref().and_then(|s| serde_json::from_str::<Value>(s).ok()),
            "created_at_ms": row.get::<_, i64>(11)?,
        }))
    })?;
    for row in rows {
        println!("{}", serde_json::to_string(&row?)?);
    }
    Ok(())
}

fn git_repo_root(repo: &Path) -> Result<PathBuf> {
    let out = git_stdout(repo, &["rev-parse", "--show-toplevel"])?;
    Ok(PathBuf::from(out.trim()))
}

fn git_path(repo: &Path, path: &str) -> Result<PathBuf> {
    let out = git_stdout(repo, &["rev-parse", "--git-path", path])?;
    let resolved = PathBuf::from(out.trim());
    if resolved.is_absolute() {
        Ok(resolved)
    } else {
        Ok(repo.join(resolved))
    }
}

fn git_branch(repo: &Path) -> Result<String> {
    let out = git_stdout(repo, &["rev-parse", "--abbrev-ref", "HEAD"])?;
    let branch = out.trim();
    anyhow::ensure!(branch != "HEAD" && !branch.is_empty(), "detached HEAD");
    Ok(branch.to_string())
}

fn git_remote(repo: &Path) -> Result<String> {
    let out = git_stdout(repo, &["remote", "get-url", "origin"])?;
    Ok(strip_url_credentials(out.trim()))
}

fn git_commit_time_ms(repo: &Path, commit: &str) -> Result<i64> {
    let out = git_stdout(repo, &["show", "-s", "--format=%ct", commit])?;
    Ok(out.trim().parse::<i64>()? * 1000)
}

fn git_commit_files(repo: &Path, commit: &str) -> Result<Vec<String>> {
    let out = git_stdout(
        repo,
        &[
            "diff-tree",
            "--root",
            "--no-commit-id",
            "--name-only",
            "-r",
            commit,
        ],
    )?;
    Ok(out
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect())
}

fn git_commit_numstat(repo: &Path, commit: &str) -> Result<Vec<Value>> {
    let out = git_stdout(
        repo,
        &[
            "diff-tree",
            "--root",
            "--numstat",
            "--no-commit-id",
            "-r",
            commit,
        ],
    )?;
    Ok(out
        .lines()
        .filter_map(|line| {
            let mut parts = line.split('\t');
            let additions = parts.next()?;
            let deletions = parts.next()?;
            let path = parts.next()?;
            Some(json!({
                "path": path,
                "additions": additions.parse::<i64>().ok(),
                "deletions": deletions.parse::<i64>().ok(),
            }))
        })
        .collect())
}

fn git_stdout(repo: &Path, args: &[&str]) -> Result<String> {
    let out = std::process::Command::new("git")
        .current_dir(repo)
        .args(args)
        .output()
        .with_context(|| format!("running git {}", args.join(" ")))?;
    anyhow::ensure!(
        out.status.success(),
        "git {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&out.stderr).trim()
    );
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

fn git_status(repo: &Path, args: &[&str]) -> Result<()> {
    let status = std::process::Command::new("git")
        .current_dir(repo)
        .args(args)
        .status()
        .with_context(|| format!("running git {}", args.join(" ")))?;
    anyhow::ensure!(status.success(), "git {} failed", args.join(" "));
    Ok(())
}

fn get_entry(conn: &Connection, id: i64) -> Result<HistoryEntry> {
    conn.query_row(
        "SELECT id, source, session_id, project, prompt, timestamp_ms FROM history WHERE id = ?",
        [id],
        row_to_entry,
    )
    .with_context(|| format!("No entry with id {id}"))
}

fn row_to_entry(row: &rusqlite::Row<'_>) -> rusqlite::Result<HistoryEntry> {
    Ok(HistoryEntry {
        id: row.get(0)?,
        source: row.get(1)?,
        session_id: row.get(2)?,
        project: row.get(3)?,
        prompt: row.get(4)?,
        prompt_hash: None,
        timestamp_ms: row.get(5)?,
    })
}

fn query_entries(conn: &Connection, sql: &str, params_: &[&String]) -> Result<Vec<HistoryEntry>> {
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt
        .query_map(rusqlite::params_from_iter(params_.iter()), row_to_entry)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn search_all(
    conn: &Connection,
    terms: &[String],
    raw_fts: bool,
    filter: &QueryFilter,
    role: SearchRole,
) -> Result<Vec<SearchRow>> {
    let mut rows = Vec::new();
    if !matches!(role, SearchRole::Assistant) {
        rows.extend(search_history_rows(conn, terms, raw_fts, filter)?);
    }
    rows.extend(search_event_rows(conn, terms, raw_fts, filter, role)?);
    rows.sort_by(|a, b| {
        b.timestamp_ms
            .cmp(&a.timestamp_ms)
            .then_with(|| b.id.cmp(&a.id))
            .then_with(|| a.match_source.cmp(&b.match_source))
    });
    rows.truncate(filter.limit.max(1) as usize);
    Ok(rows)
}

fn search_history_rows(
    conn: &Connection,
    terms: &[String],
    raw_fts: bool,
    filter: &QueryFilter,
) -> Result<Vec<SearchRow>> {
    let mut params_vec = Vec::new();
    let mut sql = if terms.is_empty() {
        "SELECT h.id, h.source, h.session_id, h.project, h.prompt, h.timestamp_ms \
         FROM history h WHERE 1=1"
            .to_string()
    } else {
        params_vec.push(ai_hist_core::build_fts_query(terms, raw_fts));
        "SELECT h.id, h.source, h.session_id, h.project, h.prompt, h.timestamp_ms \
         FROM history_fts f JOIN history h ON f.rowid = h.id WHERE history_fts MATCH ?"
            .to_string()
    };
    append_history_search_filters(&mut sql, &mut params_vec, filter, "h");
    sql.push_str(" ORDER BY h.timestamp_ms DESC LIMIT ?");
    params_vec.push(filter.limit.max(1).to_string());
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(rusqlite::params_from_iter(params_vec), |row| {
            Ok(SearchRow {
                id: row.get(0)?,
                source: row.get(1)?,
                session_id: row.get(2)?,
                project: row.get(3)?,
                text: row.get(4)?,
                timestamp_ms: row.get(5)?,
                role: "user".to_string(),
                kind: "history".to_string(),
                match_source: "history".to_string(),
            })
        })
        .map_err(|error| raw_fts_query_error(raw_fts, error))?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|error| raw_fts_query_error(raw_fts, error))?;
    Ok(rows)
}

fn search_event_rows(
    conn: &Connection,
    terms: &[String],
    raw_fts: bool,
    filter: &QueryFilter,
    role: SearchRole,
) -> Result<Vec<SearchRow>> {
    let mut params_vec = Vec::new();
    let mut sql = if terms.is_empty() {
        "SELECT e.id, e.source, e.session_id, e.project, COALESCE(e.text, ''), e.ts_ms, e.role, e.kind \
         FROM session_events e WHERE 1=1"
            .to_string()
    } else {
        params_vec.push(ai_hist_core::build_fts_query(terms, raw_fts));
        "SELECT e.id, e.source, e.session_id, e.project, COALESCE(e.text, ''), e.ts_ms, e.role, e.kind \
         FROM session_events_fts f JOIN session_events e ON f.rowid = e.id WHERE session_events_fts MATCH ?"
            .to_string()
    };
    append_event_search_filters(&mut sql, &mut params_vec, filter, "e", role);
    sql.push_str(" ORDER BY e.ts_ms DESC LIMIT ?");
    params_vec.push(filter.limit.max(1).to_string());
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(rusqlite::params_from_iter(params_vec), |row| {
            Ok(SearchRow {
                id: row.get(0)?,
                source: row.get(1)?,
                session_id: row.get(2)?,
                project: row.get(3)?,
                text: row.get(4)?,
                timestamp_ms: row.get(5)?,
                role: row.get(6)?,
                kind: row.get(7)?,
                match_source: "session_event".to_string(),
            })
        })
        .map_err(|error| raw_fts_query_error(raw_fts, error))?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|error| raw_fts_query_error(raw_fts, error))?;
    Ok(rows)
}

fn append_history_search_filters(
    sql: &mut String,
    params: &mut Vec<String>,
    filter: &QueryFilter,
    alias: &str,
) {
    append_session_scope_filter(sql, filter.scope, alias);
    if let Some(source) = &filter.source {
        sql.push_str(&format!(" AND {alias}.source = ?"));
        params.push(source.clone());
    }
    if let Some(project) = &filter.project {
        sql.push_str(&format!(" AND {alias}.project LIKE ?"));
        params.push(format!("%{project}%"));
    }
    if let Some(tag) = &filter.tag {
        sql.push_str(&format!(" AND {}", tag_filter_clause(alias)));
        params.push(normalize_tag_name(tag));
    }
}

fn append_event_search_filters(
    sql: &mut String,
    params: &mut Vec<String>,
    filter: &QueryFilter,
    alias: &str,
    role: SearchRole,
) {
    append_session_scope_filter(sql, filter.scope, alias);
    if let Some(source) = &filter.source {
        sql.push_str(&format!(" AND {alias}.source = ?"));
        params.push(source.clone());
    }
    if let Some(project) = &filter.project {
        sql.push_str(&format!(" AND {alias}.project LIKE ?"));
        params.push(format!("%{project}%"));
    }
    if let Some(tag) = &filter.tag {
        sql.push_str(&format!(" AND {}", tag_filter_clause(alias)));
        params.push(normalize_tag_name(tag));
    }
    match role {
        SearchRole::All => {}
        SearchRole::User => sql.push_str(&format!(" AND {alias}.role = 'user'")),
        SearchRole::Assistant => sql.push_str(&format!(" AND {alias}.role = 'assistant'")),
    }
}

/// Restrict a history/event query by where its canonical session is present.
///
/// Legacy rows with no session id or no classification are local so upgrading
/// cannot make existing history disappear. Remote requires an explicit remote
/// presence. `all` adds no predicate and therefore cannot multiply rows.
fn append_session_scope_filter(sql: &mut String, scope: SessionScope, alias: &str) {
    match scope {
        SessionScope::Local => sql.push_str(&format!(
            " AND ({alias}.session_id IS NULL \
               OR EXISTS (SELECT 1 FROM session_presences p WHERE p.source = {alias}.source AND p.session_id = {alias}.session_id AND p.location = 'local') \
               OR NOT EXISTS (SELECT 1 FROM session_presences p WHERE p.source = {alias}.source AND p.session_id = {alias}.session_id))"
        )),
        SessionScope::Remote => sql.push_str(&format!(
            " AND {alias}.session_id IS NOT NULL \
               AND EXISTS (SELECT 1 FROM session_presences p WHERE p.source = {alias}.source AND p.session_id = {alias}.session_id AND p.location = 'remote')"
        )),
        SessionScope::All => {}
    }
}

fn query_pairs(
    conn: &Connection,
    sql: &str,
    params_: &[&str],
) -> Result<Vec<(String, serde_json::Value)>> {
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt
        .query_map(rusqlite::params_from_iter(params_), |row| {
            Ok((row.get::<_, String>(0)?, json!(row.get::<_, i64>(1)?)))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn tag_filter_clause(alias: &str) -> String {
    format!(
        "EXISTS (SELECT 1 FROM session_tags st JOIN tags t ON t.id = st.tag_id WHERE st.source = {alias}.source AND st.session_id = {alias}.session_id AND t.name = ?)"
    )
}

fn session_tags(
    conn: &Connection,
    source: &str,
    session_id: &str,
) -> Result<Vec<serde_json::Value>> {
    let mut stmt = conn.prepare(
        "SELECT t.name, t.display_name, t.color FROM tags t JOIN session_tags st ON st.tag_id = t.id WHERE st.source = ? AND st.session_id = ? ORDER BY t.name",
    )?;
    let rows = stmt
        .query_map(params![source, session_id], |row| {
            Ok(json!({
                "name": row.get::<_, String>(0)?,
                "display_name": row.get::<_, String>(1)?,
                "color": row.get::<_, Option<String>>(2)?,
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn tag_session_with_count(
    conn: &Connection,
    session_id: &str,
    tag_name: &str,
    source: Option<&str>,
    color: Option<&str>,
) -> Result<(Vec<serde_json::Value>, usize)> {
    let sessions = ai_hist_core::matching_sessions(conn, session_id, source)?;
    if sessions.is_empty() {
        return Ok((Vec::new(), 0));
    }
    let normalized = normalize_tag_name(tag_name);
    anyhow::ensure!(!normalized.is_empty(), "tag name cannot be empty");
    let now = chrono::Utc::now().timestamp_millis();
    conn.execute(
        "INSERT INTO tags (name, display_name, color, created_ms, updated_ms) VALUES (?, ?, ?, ?, ?) \
         ON CONFLICT(name) DO UPDATE SET display_name = excluded.display_name, color = COALESCE(excluded.color, tags.color), updated_ms = excluded.updated_ms",
        params![normalized, tag_name.trim(), color, now, now],
    )?;
    let tag_id: i64 =
        conn.query_row("SELECT id FROM tags WHERE name = ?", [normalized], |row| {
            row.get(0)
        })?;
    let mut created = 0;
    for session in &sessions {
        created += conn.execute(
            "INSERT OR IGNORE INTO session_tags (source, session_id, tag_id, created_ms) VALUES (?, ?, ?, ?)",
            params![session.source, session.session_id, tag_id, now],
        )?;
    }
    Ok((
        sessions
            .into_iter()
            .map(|s| {
                json!({
                    "source": s.source,
                    "session_id": s.session_id,
                    "project": s.project,
                    "entry_count": s.entry_count,
                    "last_activity_ms": s.last_activity_ms,
                })
            })
            .collect(),
        created,
    ))
}

fn tagged_sessions(conn: &Connection, tag: &str) -> Result<Vec<serde_json::Value>> {
    let mut stmt = conn.prepare(
        "SELECT st.source, st.session_id, MIN(h.project), COUNT(h.id), MAX(h.timestamp_ms) \
         FROM session_tags st JOIN tags t ON t.id = st.tag_id \
         LEFT JOIN history h ON h.source = st.source AND h.session_id = st.session_id \
         WHERE t.name = ? GROUP BY st.source, st.session_id ORDER BY MAX(h.timestamp_ms) DESC",
    )?;
    let rows = stmt
        .query_map([tag], |row| {
            Ok(json!({
                "source": row.get::<_, String>(0)?,
                "session_id": row.get::<_, String>(1)?,
                "project": row.get::<_, Option<String>>(2)?,
                "entry_count": row.get::<_, i64>(3)?,
                "last_activity_ms": row.get::<_, Option<i64>>(4)?,
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn export_rows(
    conn: &Connection,
    source: Option<&str>,
    project: Option<&str>,
    since: Option<&str>,
) -> Result<Vec<HistoryEntry>> {
    let mut sql =
        "SELECT id, source, session_id, project, prompt, timestamp_ms FROM history WHERE 1=1"
            .to_string();
    let mut params_vec = Vec::new();
    if let Some(source) = source {
        sql.push_str(" AND source = ?");
        params_vec.push(source.to_string());
    }
    if let Some(project) = project {
        sql.push_str(" AND project LIKE ?");
        params_vec.push(format!("%{project}%"));
    }
    if let Some(since) = since {
        sql.push_str(" AND timestamp_ms >= ?");
        params_vec.push(parse_date_ms(since)?.to_string());
    }
    sql.push_str(" ORDER BY timestamp_ms ASC");
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt
        .query_map(rusqlite::params_from_iter(params_vec), row_to_entry)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn load_jsonl_entries(path: &Path) -> Result<Vec<HistoryEntry>> {
    let reader: Box<dyn Read> = if path.extension().and_then(|s| s.to_str()) == Some("gz") {
        Box::new(GzDecoder::new(fs::File::open(path)?))
    } else {
        Box::new(fs::File::open(path)?)
    };
    let mut entries = Vec::new();
    for line in BufReader::new(reader).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let mut value: serde_json::Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let prompt = value
            .get("prompt")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if prompt.is_empty() {
            continue;
        }
        if value.get("prompt_hash").is_none() {
            value["prompt_hash"] = json!(prompt_hash(&prompt));
        }
        entries.push(serde_json::from_value(value)?);
    }
    Ok(entries)
}

fn load_sqlite_entries(path: &Path) -> Result<Vec<HistoryEntry>> {
    let src = Connection::open(path)?;
    let cols = src
        .prepare("PRAGMA table_info(history)")?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let has_hash = cols.iter().any(|col| col == "prompt_hash");
    let sql = if has_hash {
        "SELECT id, source, session_id, project, prompt, prompt_hash, timestamp_ms FROM history"
    } else {
        "SELECT id, source, session_id, project, prompt, NULL, timestamp_ms FROM history"
    };
    let mut stmt = src.prepare(sql)?;
    let entries = stmt
        .query_map([], |row| {
            let prompt: String = row.get(4)?;
            Ok(HistoryEntry {
                id: row.get(0)?,
                source: row.get(1)?,
                session_id: row.get(2)?,
                project: row.get(3)?,
                prompt_hash: row
                    .get::<_, Option<String>>(5)?
                    .or_else(|| Some(prompt_hash(&prompt))),
                prompt,
                timestamp_ms: row.get(6)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(entries)
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
            match fs2::FileExt::try_lock_exclusive(&file) {
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
        let _ = fs2::FileExt::unlock(&self.file);
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
fn merged_sync_state(path: &Path, ours: &Map<String, Value>) -> Result<Option<Map<String, Value>>> {
    let mut merged = load_sync_state(path)?;
    let mut changed = false;
    for (key, value) in ours {
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
/// `codex_rollouts_v4` generation repairs the user-message parser change by
/// re-reading unchanged files once. Its per-file record carries the session id
/// so a wiped database forces re-ingestion even when the file stamp is unchanged.
/// (session cwds, session branches, prompts inserted).
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
    let repair_user_messages = !state.contains_key("codex_rollouts_v4");
    let mut seen = state
        .get("codex_rollouts_v4")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    // Superseded stamp maps from the split-walk era; keeping them would carry
    // three path->stamp maps over the same 2K-file tree in .sync-state.json.
    state.remove("codex_rollouts");
    state.remove("codex_rollout_user_messages_v2");
    let mut inserted = 0;
    let mut scanned = 0;
    let mut events = 0usize;
    for root in [
        home.join(".codex/sessions"),
        home.join(".codex/archived_sessions"),
    ] {
        if !root.exists() {
            continue;
        }
        for rollout in collect_matching_files(&root, "rollout-", "jsonl")? {
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
                    Some(id) if codex_session_events_exist(conn, id)? => continue,
                    // Stamp matches but the events are gone (wiped or rebuilt
                    // database): fall through and re-ingest.
                    _ => {}
                }
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
    state.insert("codex_rollouts_v4".to_string(), Value::Object(seen));
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

/// Read the `session_meta` line that opens every rollout file.
///
/// Sessions key on `payload.id` — the per-thread id. Subagent rollouts can
/// carry their parent in `parent_thread_id`, a structured thread-spawn source,
/// or the legacy `session_id`; keying on any of those would collapse every
/// subagent into its parent. Subagent threads are detected instead
/// (`thread_source`, or the object form of `payload.source`) and excluded from
/// session registration.
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
    let payload = value.get("payload").and_then(Value::as_object);
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
    let is_subagent = payload
        .and_then(|p| p.get("thread_source"))
        .and_then(Value::as_str)
        == Some("subagent")
        || subagent.is_some();
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
    let mut scanned = 0;
    let mut upserted = 0;
    for path in collect_matching_files(root, "", "jsonl")? {
        let key = path.to_string_lossy().to_string();
        let stamp = claude_sync_stamp(&path)?;
        if session_state.get(&key).and_then(Value::as_str) == Some(stamp.as_str())
            && (claude_transcript_events_exist(conn, &path)?
                || claude_sidecar_evidence_exists(conn, &path)?)
        {
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
    state.insert(
        "claude_sessions_v3".to_string(),
        Value::Object(session_state),
    );
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

struct ClaudeSessionMeta {
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

fn reconcile_claude_remote_relationships(conn: &Connection) -> Result<()> {
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
    for (line_index, line) in text.lines().enumerate() {
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
        let sidechain = obj
            .get("isSidechain")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let skipped_sidechain = sidechain
            && obj
                .get("message")
                .and_then(|m| m.get("role"))
                .and_then(Value::as_str)
                != Some("assistant");
        let uuid = obj.get("uuid").and_then(Value::as_str);
        let message = obj.get("message").and_then(Value::as_object);
        let fallback_uid = format!(
            "{}:{}",
            path.file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("session"),
            line_index
        );
        let message_uuid = uuid
            .or_else(|| message.and_then(|m| m.get("id")).and_then(Value::as_str))
            .unwrap_or(&fallback_uid);
        // Heal what an earlier parser version wrote for this record: it
        // attributed every sidechain row to the parent, and stored the rows
        // this guard now skips. Re-reading the file removes the stale rows
        // under the identity they were written with, so a re-parse moves them
        // onto the child instead of duplicating them across both.
        if session_id != record_session_id {
            delete_claude_record_rows(conn, record_session_id, message_uuid)?;
        }
        if skipped_sidechain {
            delete_claude_record_rows(conn, session_id, message_uuid)?;
            continue;
        }
        let cwd = obj.get("cwd").and_then(Value::as_str);
        let project = cwd;
        let git_branch = obj.get("gitBranch").and_then(Value::as_str);
        let ts_ms = obj
            .get("timestamp")
            .and_then(|v| v.as_str().and_then(parse_iso_ms).or_else(|| v.as_i64()))
            .unwrap_or(0);
        let parent_id = obj.get("parentUuid").and_then(Value::as_str);
        let message_role = message
            .and_then(|m| m.get("role"))
            .and_then(Value::as_str)
            .or_else(|| obj.get("type").and_then(Value::as_str))
            .unwrap_or("");
        let model = message.and_then(|m| m.get("model")).and_then(Value::as_str);
        let token_json = message
            .and_then(|m| m.get("usage"))
            .and_then(|v| serde_json::to_string(v).ok());
        let Some(content) = message.and_then(|m| m.get("content")) else {
            continue;
        };
        if !sidechain
            && message_role == "user"
            && obj.get("isMeta").and_then(Value::as_bool) != Some(true)
        {
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
) -> Result<()> {
    ai_hist_core::mark_session_presence(conn, source, session_id, SessionLocation::Local)?;
    conn.execute(
        "INSERT INTO session_events \
         (source, session_id, project, cwd, git_branch, message_id, parent_id, ts_ms, role, kind, text, model, token_json, event_uid) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(source, session_id, event_uid) DO UPDATE SET \
         project=excluded.project, cwd=excluded.cwd, git_branch=excluded.git_branch, message_id=excluded.message_id, \
         parent_id=excluded.parent_id, ts_ms=excluded.ts_ms, role=excluded.role, kind=excluded.kind, text=excluded.text, \
         model=excluded.model, token_json=excluded.token_json",
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
    ai_hist_core::mark_session_presence(conn, source, session_id, SessionLocation::Local)?;
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
    ai_hist_core::mark_session_presence(conn, source, session_id, SessionLocation::Local)?;
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
fn upsert_session(
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
    conn.execute(
        "INSERT INTO sessions \
         (session_id, source, cwd, git_branch, first_activity_ms, last_activity_ms, last_assistant_text, raw_path, parser_version, discovery_state) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, 'full') \
         ON CONFLICT(session_id, source) DO UPDATE SET \
         cwd = COALESCE(excluded.cwd, sessions.cwd), \
         git_branch = COALESCE(excluded.git_branch, sessions.git_branch), \
         first_activity_ms = MIN(COALESCE(sessions.first_activity_ms, excluded.first_activity_ms), excluded.first_activity_ms), \
         last_activity_ms = MAX(COALESCE(sessions.last_activity_ms, excluded.last_activity_ms), excluded.last_activity_ms), \
         last_assistant_text = COALESCE(excluded.last_assistant_text, sessions.last_assistant_text), \
         raw_path = COALESCE(excluded.raw_path, sessions.raw_path), \
         parser_version = excluded.parser_version, \
         discovery_state = 'full'",
        params![
            session_id,
            source,
            cwd,
            git_branch,
            first_ts,
            last_ts,
            last_assistant_text,
            raw_path,
        ],
    )?;
    ai_hist_core::upsert_session_presence(
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

fn collect_matching_files(root: &Path, prefix: &str, ext: &str) -> Result<Vec<PathBuf>> {
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
fn file_stamp_and_modified(path: &Path) -> Result<(String, Option<i64>)> {
    let metadata = path.metadata()?;
    anyhow::ensure!(
        metadata.is_file(),
        "{} is not a regular file",
        path.display()
    );
    Ok((stamp_of(&metadata), modified_ms_of(&metadata)))
}

fn sync_cursor(conn: &Connection, state: &mut Map<String, Value>, root: &Path) -> Result<usize> {
    if !root.exists() {
        return Ok(0);
    }
    let mut cursor_state = state
        .get("cursor")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let mut inserted = 0;
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
            files_seen += 1;
            let key = jsonl.to_string_lossy().to_string();
            let mut source = CompleteJsonlReader::open(&jsonl, cursor_state.get(&key))?;
            let offset = source.position;
            let size = source.reader.get_ref().metadata()?.len();
            let ts_ms = jsonl
                .metadata()
                .and_then(|m| m.modified())
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            let mut consumed = offset;
            if offset < size {
                let mut line = String::new();
                while let Some(position) = source.next_line(&mut line)? {
                    consumed = position;
                    match ingest_cursor_line(conn, &line, &session_id, &project_path, ts_ms) {
                        Ok(count) => inserted += count,
                        Err(_) => errors += 1,
                    }
                }
            }
            let opened_cursor = source.cursor.to_value();
            if consumed != offset || cursor_state.get(&key) != Some(&opened_cursor) {
                cursor_state.insert(key, source.committed_cursor(consumed, true)?.to_value());
            }
        }
    }
    state.insert("cursor".to_string(), Value::Object(cursor_state));
    if files_seen > 0 {
        let suffix = if errors > 0 {
            format!(" ({errors} errors)")
        } else {
            String::new()
        };
        sync_note!("  [cursor] +{inserted} rows from {files_seen} files{suffix}");
    }
    Ok(inserted)
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

fn sorted_dirs(root: &Path) -> Result<Vec<PathBuf>> {
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

fn decode_cursor_project(name: &str) -> String {
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
    for chat in collect_matching_files(root, "chat_history", "jsonl")? {
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
fn grok_session_stamp_and_modified(chat: &Path) -> Result<(String, Option<i64>)> {
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

fn read_grok_summary(path: &Path) -> Option<Value> {
    serde_json::from_str(&fs::read_to_string(path).ok()?).ok()
}

fn grok_project_from_path(chat: &Path) -> Option<String> {
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

fn file_modified_ms(path: &Path) -> Option<i64> {
    path.metadata().ok().as_ref().and_then(modified_ms_of)
}

fn grok_chat_text(value: &Value, role: &str) -> Option<String> {
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

fn sync_trajectories(conn: &Connection, state: &mut Map<String, Value>) -> Result<usize> {
    let files = trajectory_files()?;
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
    for path in files {
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
        if upsert_trajectory(conn, &row).is_err() {
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

fn trajectory_files() -> Result<Vec<PathBuf>> {
    let mut roots = Vec::new();
    if let Some(raw) = std::env::var_os("TRAJECTORY_ROOT") {
        for part in std::env::split_paths(&raw) {
            if !part.as_os_str().is_empty() {
                roots.push(part);
            }
        }
    } else {
        let projects = home_dir().join("Projects");
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
        let path = entry?.path();
        if path.is_dir() {
            if path.file_name().and_then(|s| s.to_str()) == Some(name) {
                out.push(path.clone());
            }
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
        let path = entry?.path();
        if path.is_dir() {
            collect_trajectory_json(&path, out)?;
        } else if path.extension().and_then(|s| s.to_str()) == Some("json") {
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

fn parse_iso_ms(raw: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|dt| dt.timestamp_millis())
}

fn sync_relaycast(conn: &Connection, state: &mut Map<String, Value>) -> Result<usize> {
    let api_key = std::env::var("RELAYCAST_API_KEY").unwrap_or_default();
    let workspace = std::env::var("RELAYCAST_WORKSPACE_ID").unwrap_or_default();
    if api_key.is_empty() || workspace.is_empty() {
        return Ok(0);
    }
    let base =
        std::env::var("RELAYCAST_BASE_URL").unwrap_or_else(|_| "https://api.relaycast.dev".into());
    let mut relay_state = state
        .get("relay")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let mut inserted = 0;
    let channels = relay_get(&base, &api_key, "channels", &[])?
        .get("data")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    for channel in channels {
        let Some(name) = channel.get("name").and_then(Value::as_str) else {
            continue;
        };
        inserted += sync_relay_messages(
            conn,
            &mut relay_state,
            &base,
            &api_key,
            &format!("channels/{name}/messages"),
            &format!("ch:{name}"),
            &format!("#{name}"),
            &workspace,
        )?;
    }
    let conversations = relay_get(&base, &api_key, "dm/conversations/all", &[])
        .ok()
        .and_then(|v| v.get("data").and_then(Value::as_array).cloned())
        .unwrap_or_default();
    for conversation in conversations {
        let Some(id) = conversation.get("id").and_then(Value::as_str) else {
            continue;
        };
        inserted += sync_relay_messages(
            conn,
            &mut relay_state,
            &base,
            &api_key,
            &format!("dm/conversations/{id}/messages"),
            &format!("dm:{id}"),
            &format!("dm:{id}"),
            &workspace,
        )?;
    }
    state.insert("relay".to_string(), Value::Object(relay_state));
    sync_note!("  [relay] +{inserted} rows");
    Ok(inserted)
}

fn sync_relay_messages(
    conn: &Connection,
    relay_state: &mut Map<String, Value>,
    base: &str,
    api_key: &str,
    path: &str,
    state_key: &str,
    fallback_session: &str,
    workspace: &str,
) -> Result<usize> {
    let mut inserted = 0;
    let mut after = relay_state
        .get(state_key)
        .and_then(Value::as_str)
        .map(str::to_string);
    let mut max_id = after.clone();
    loop {
        let mut params = vec![("limit", "100")];
        if let Some(after) = after.as_deref() {
            params.push(("after", after));
        }
        let messages = relay_get(base, api_key, path, &params)?
            .get("data")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        if messages.is_empty() {
            break;
        }
        for msg in &messages {
            let text = msg.get("text").and_then(Value::as_str).unwrap_or("");
            if text.is_empty() {
                continue;
            }
            let sender = msg
                .get("from_name")
                .or_else(|| msg.get("from_id"))
                .and_then(Value::as_str)
                .unwrap_or("");
            let prompt = if sender.is_empty() {
                text.to_string()
            } else {
                format!("[{sender}] {text}")
            };
            let session_id = msg
                .get("thread_id")
                .and_then(Value::as_str)
                .unwrap_or(fallback_session);
            let timestamp_ms = msg
                .get("created_at")
                .and_then(Value::as_str)
                .and_then(parse_iso_ms)
                .unwrap_or(0);
            inserted += insert_history(
                conn,
                &HistoryEntry {
                    id: 0,
                    source: "relay".into(),
                    session_id: Some(session_id.to_string()),
                    project: Some(workspace.to_string()),
                    prompt_hash: Some(prompt_hash(&prompt)),
                    prompt,
                    timestamp_ms,
                },
            )?;
            if let Some(id) = msg.get("id").and_then(Value::as_str) {
                if max_id.as_deref().is_none_or(|current| id > current) {
                    max_id = Some(id.to_string());
                }
            }
        }
        if messages.len() < 100 {
            break;
        }
        after = messages
            .last()
            .and_then(|msg| msg.get("id"))
            .and_then(Value::as_str)
            .map(str::to_string);
        if after.is_none() {
            break;
        }
    }
    if let Some(max_id) = max_id {
        relay_state.insert(state_key.to_string(), json!(max_id));
    }
    Ok(inserted)
}

fn relay_get(base: &str, api_key: &str, path: &str, params_: &[(&str, &str)]) -> Result<Value> {
    let mut url = format!("{}/v1/{}", base.trim_end_matches('/'), path);
    if !params_.is_empty() {
        url.push('?');
        url.push_str(
            &params_
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join("&"),
        );
    }
    let output = std::process::Command::new("curl")
        .arg("-fsSL")
        .arg("-H")
        .arg(format!("Authorization: Bearer {api_key}"))
        .arg("-H")
        .arg("Accept: application/json")
        .arg(url)
        .output()
        .context("running curl for Relaycast API")?;
    anyhow::ensure!(
        output.status.success(),
        "Relaycast API request failed with status {}",
        output.status
    );
    Ok(serde_json::from_slice(&output.stdout)?)
}

fn parse_claude_line(line: &str) -> Result<Option<HistoryEntry>> {
    ai_hist_core::parse_claude(line)
}

fn parse_codex_line(line: &str) -> Result<Option<HistoryEntry>> {
    ai_hist_core::parse_codex(line)
}

fn parse_date_ms(date: &str) -> Result<i64> {
    let parsed = chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d")?;
    Ok(parsed
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_millis())
}

fn format_date(ts_ms: i64) -> String {
    Local
        .timestamp_millis_opt(ts_ms)
        .single()
        .map(|dt| dt.format("%Y-%m-%d").to_string())
        .unwrap_or_default()
}

fn format_datetime(ts_ms: i64) -> String {
    Local
        .timestamp_millis_opt(ts_ms)
        .single()
        .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
        .unwrap_or_default()
}

fn home_dir() -> PathBuf {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

#[cfg(test)]
mod tests {
    use super::{
        checkpoint_sync_state, claude_sync_stamp, cleanup_stale_sync_state_temps, cron_schedule,
        doctor_report, export_history, file_stamp, git_commit_time_ms, git_stdout, import_history,
        ingest_claude_transcript, is_sqlite_contention, link_git_commit, load_sync_state,
        parse_trajectory_file, paths_overlap, prepare_sync_and_push_db,
        process_status_with_programs, reconcile_claude_remote_relationships, save_sync_state,
        search_all, service_command_args, shell_single_quote, source_database_path,
        strip_url_credentials, sync_claude_session_metadata, sync_exclusive, sync_local_at,
        sync_opencode_exclusive, sync_relaycast, try_acquire_sync_lock, wal_contention_line,
        write_contention_diagnostic, xml_escape, SearchRole, SyncSourceReport, PUSH_SERVICE,
        WAL_WARN_BYTES,
    };
    use ai_hist_core::{
        init_db, insert_history, open_db, prompt_hash, HistoryEntry, QueryFilter,
        SourceDatabaseError,
    };
    use rusqlite::Connection;
    use serde_json::{json, Map, Value};
    use std::fs;
    use std::io::Write as _;
    use std::time::Duration;

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
    fn cron_schedule_maps_intervals_to_step_expressions() {
        assert_eq!(cron_schedule(60).0, "* * * * *");
        assert_eq!(cron_schedule(300).0, "*/5 * * * *"); // push default
        assert_eq!(cron_schedule(120).0, "*/2 * * * *");
        assert_eq!(cron_schedule(3600).0, "0 */1 * * *");
        // Sub-two-minute intervals collapse to every minute.
        assert_eq!(cron_schedule(30).0, "* * * * *");
        assert_eq!(cron_schedule(90).0, "* * * * *");
        // Never run MORE often than requested: round toward a coarser cadence.
        assert_eq!(cron_schedule(5400).0, "0 */2 * * *"); // 90 min -> every 2h, not hourly
        assert_eq!(cron_schedule(86_400).0, "0 0 * * *"); // 1 day -> daily, not hourly
        assert_eq!(cron_schedule(90_000).0, "0 0 * * *"); // 25h -> daily
                                                          // Non-divisor steps round up to a uniform divisor (no short boundary gap).
        assert_eq!(cron_schedule(420).0, "*/10 * * * *"); // 7 min -> */10 (not */7)
        assert_eq!(cron_schedule(2700).0, "0 * * * *"); // 45 min -> hourly (60 is next divisor)
        assert_eq!(cron_schedule(25_200).0, "0 */8 * * *"); // 7h -> */8 (uniform), not */7
                                                            // The effective period flags whether the interval was matched exactly.
        assert_eq!(cron_schedule(300).2, 300);
        assert_eq!(cron_schedule(5400).2, 7200);
        assert_eq!(cron_schedule(420).2, 600);
    }

    #[test]
    fn push_service_command_pins_the_selected_cloud_stage() {
        let args = vec![
            "--base-url".to_string(),
            "https://history.agentrelay.com".to_string(),
            "--limit".to_string(),
            "50".to_string(),
        ];
        assert_eq!(
            service_command_args(&PUSH_SERVICE, &args),
            vec![
                "push".to_string(),
                "--base-url".to_string(),
                "https://history.agentrelay.com".to_string(),
                "--limit".to_string(),
                "50".to_string(),
            ]
        );
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

        let (conn, sync_skipped) = prepare_sync_and_push_db(&db_path).unwrap();
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
    fn only_non_mutating_commands_get_a_read_only_handle() {
        // Reads must not take the write lock...
        assert!(super::is_read_only(&super::Command::Stats {
            scope: super::SessionScopeArgs::default(),
            tag: None,
            json: false
        }));
        assert!(super::is_read_only(&super::Command::Show {
            id: 1,
            json: false
        }));
        // `coverage` never touches the local database at all — it only queries the server.
        // Opening writably would park a scheduled `coverage --fail-on-stale` behind the 60s
        // sync service's write lock, for a query that reads nothing local.
        assert!(super::is_read_only(&super::Command::Coverage {
            base_url: None,
            stale_after: None,
            missing_after: None,
            window_hours: None,
            fail_on_stale: true,
            json: false,
        }));

        // ...and anything that writes must not get a read-only handle, or it
        // fails at runtime with "attempt to write a readonly database".
        assert!(!super::is_read_only(&super::Command::Sync {
            scope: super::SessionScopeArgs::default(),
            install_service: false,
            uninstall_service: false,
            interval: 60,
        }));
        assert!(!super::is_read_only(&super::Command::Tag {
            session_id: "s".into(),
            tag_name: "t".into(),
            source: None,
            color: None,
            json: false,
        }));
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
    fn resume_requires_local_or_legacy_local_evidence() {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        let entry = HistoryEntry {
            id: 1,
            source: "codex".into(),
            session_id: Some("cloud-session".into()),
            project: None,
            prompt: "continue".into(),
            prompt_hash: None,
            timestamp_ms: 1,
        };

        // Legacy rows without a presence retain the old local resume behavior,
        // while output truthfully reports no observed location.
        let (locations, command) = super::local_resume_details(&conn, &entry).unwrap();
        assert!(locations.is_empty());
        assert!(command.is_some());

        ai_hist_core::mark_session_presence(
            &conn,
            "codex",
            "cloud-session",
            super::SessionLocation::Remote,
        )
        .unwrap();
        let (locations, command) = super::local_resume_details(&conn, &entry).unwrap();
        assert_eq!(locations, vec!["remote"]);
        assert!(command.is_none());

        ai_hist_core::mark_session_presence(
            &conn,
            "codex",
            "cloud-session",
            super::SessionLocation::Local,
        )
        .unwrap();
        let (locations, command) = super::local_resume_details(&conn, &entry).unwrap();
        assert_eq!(locations, vec!["local", "remote"]);
        assert!(command.is_some());
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
        ai_hist_core::mark_session_presence(
            &conn,
            "codex",
            "subagent",
            super::SessionLocation::Local,
        )
        .unwrap();
        ai_hist_core::mark_session_presence(
            &conn,
            "codex",
            "subagent",
            super::SessionLocation::Remote,
        )
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
    fn shell_single_quote_survives_spaces_and_quotes() {
        assert_eq!(shell_single_quote("/opt/ai hist/bin"), "'/opt/ai hist/bin'");
        assert_eq!(shell_single_quote("a'b"), "'a'\\''b'");
    }

    #[test]
    fn xml_escape_protects_plist_path() {
        // A binary path with shell/XML metacharacters must not break the plist.
        assert_eq!(
            xml_escape("/home/a&b/<bin>/ai-hist"),
            "/home/a&amp;b/&lt;bin&gt;/ai-hist"
        );
        assert_eq!(
            xml_escape("/usr/local/bin/ai-hist"),
            "/usr/local/bin/ai-hist"
        );
    }

    #[test]
    fn strips_embedded_token_from_https_remote() {
        // C3: gh-cli/CI token helper form must never egress the token.
        assert_eq!(
            strip_url_credentials("https://x-access-token:ghp_secret123@github.com/org/repo.git"),
            "https://github.com/org/repo.git"
        );
        assert_eq!(
            strip_url_credentials("https://user:pass@gitlab.com/org/repo.git"),
            "https://gitlab.com/org/repo.git"
        );
        assert_eq!(
            strip_url_credentials("ssh://git@github.com/org/repo.git"),
            "ssh://github.com/org/repo.git"
        );
    }

    #[test]
    fn strips_token_without_user_prefix() {
        // gh-cli `x-access-token` can also appear without a `user:` prefix — keyed on `@`.
        assert_eq!(
            strip_url_credentials("https://ghp_secret123@github.com/org/repo.git"),
            "https://github.com/org/repo.git"
        );
    }

    #[test]
    fn does_not_strip_at_in_path_or_ref() {
        // The subtle case: an `@` in the path/ref must not be treated as userinfo
        // (guarded by `at < host_start`).
        assert_eq!(
            strip_url_credentials("https://github.com/org/repo@v2"),
            "https://github.com/org/repo@v2"
        );
    }

    #[test]
    fn leaves_clean_remotes_unchanged() {
        // Plain https, and scp-style (no scheme) — no secret, untouched.
        assert_eq!(
            strip_url_credentials("https://github.com/org/repo.git"),
            "https://github.com/org/repo.git"
        );
        assert_eq!(
            strip_url_credentials("git@github.com:org/repo.git"),
            "git@github.com:org/repo.git"
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
        let core_error = ai_hist_core::search(
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
        // and no topology exists at all.
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
             (source, session_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 'claude-root', 2, 'assistant', 'text', 'kept event', 'a1:0')",
            [],
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

    #[test]
    fn links_git_commit_to_recent_session_with_evidence() {
        let tmp = tempfile::tempdir().unwrap();
        let repo = tmp.path().join("repo");
        fs::create_dir_all(repo.join("src")).unwrap();
        run_git_for_test(&repo, &["init"]);
        run_git_for_test(&repo, &["config", "user.email", "test@example.com"]);
        run_git_for_test(&repo, &["config", "user.name", "ai-hist test"]);
        run_git_for_test(&repo, &["checkout", "-b", "feat/link-test"]);
        fs::write(repo.join("src/lib.rs"), "pub fn demo() {}\n").unwrap();
        run_git_for_test(&repo, &["add", "src/lib.rs"]);
        run_git_for_test(&repo, &["commit", "-m", "demo"]);
        let commit = git_stdout(&repo, &["rev-parse", "HEAD"]).unwrap();
        let commit = commit.trim();
        let commit_ms = git_commit_time_ms(&repo, commit).unwrap();

        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        conn.execute(
            "INSERT INTO sessions (session_id, source, cwd, git_branch, first_activity_ms, last_activity_ms, parser_version) VALUES (?, ?, ?, ?, ?, ?, 1)",
            rusqlite::params![
                "s-link",
                "claude",
                repo.display().to_string(),
                "feat/link-test",
                commit_ms - 60_000,
                commit_ms + 60_000
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO file_edits (source, session_id, tool_use_id, file_path, tool_name) VALUES (?, ?, ?, ?, ?)",
            rusqlite::params!["claude", "s-link", "toolu_1", "src/lib.rs", "Edit"],
        )
        .unwrap();

        link_git_commit(
            &conn,
            tmp.path(),
            &repo,
            commit,
            "manual",
            false,
            false,
            true,
        )
        .unwrap();

        let (session_id, commit_sha, match_method, confidence, evidence): (
            String,
            String,
            String,
            f64,
            String,
        ) = conn
            .query_row(
                "SELECT session_id, commit_sha, match_method, confidence, evidence_json FROM session_commit_links",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
            )
            .unwrap();
        assert_eq!(session_id, "s-link");
        assert_eq!(commit_sha, commit);
        assert_eq!(match_method, "manual");
        assert!(confidence >= 0.90);
        let evidence: Value = serde_json::from_str(&evidence).unwrap();
        assert_eq!(evidence["candidate"]["branch_match"], true);
        assert_eq!(evidence["candidate"]["file_overlap"], 1);
        let local_presence: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_presences \
                 WHERE source = 'claude' AND session_id = 's-link' AND location = 'local'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(local_presence, 1);
    }

    #[test]
    fn path_overlap_requires_separator_boundary() {
        assert!(paths_overlap("/repo/src/main.rs", "src/main.rs"));
        assert!(paths_overlap("/repo/src/main.rs", "main.rs"));
        assert!(paths_overlap("src/main.rs", "/repo/src/main.rs"));
        assert!(!paths_overlap("src/remain.rs", "main.rs"));
        assert!(!paths_overlap("src/main.rs.bak", "main.rs"));
    }

    fn run_git_for_test(repo: &std::path::Path, args: &[&str]) {
        let status = std::process::Command::new("git")
            .current_dir(repo)
            .args(args)
            .status()
            .unwrap();
        assert!(status.success(), "git {} failed", args.join(" "));
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

    #[test]
    fn codex_marker_only_guardian_remains_an_unlinked_subagent() {
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
        assert!(meta.is_subagent);
        assert_eq!(meta.parent_session_id, None);
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
    fn codex_v4_repair_restores_users_without_duplicating_existing_evidence() {
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
        assert!(state.get("codex_rollouts_v4").is_some());

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
        ai_hist_core::mark_session_presence(
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
             (source, session_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('codex', 'sess-unchanged-sub', 2, 'assistant', 'text', 'kept event', 'event-1')",
            [],
        )
        .unwrap();

        let key = rollout.to_string_lossy().to_string();
        let mut state = Map::new();
        state.insert(
            "codex_rollouts_v4".into(),
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
             (source, session_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('codex', 'sess-unchanged-sub', 2, 'assistant', 'text', 'kept event', 'event-1')",
            [],
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
             (source, session_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('codex', 'sess-unchanged-sub', 2, 'assistant', 'text', 'kept event', 'event-1')",
            [],
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
        ai_hist_core::insert_history_at_location(
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
        ai_hist_core::mark_session_presence(
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

    // AI_HIST_DB and the RELAYCAST_* credentials are process-global; serialize
    // the tests that set them so cargo's parallel runner can't clobber them.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
            let previous = std::env::var_os(key);
            std::env::set_var(key, value);
            Self { key, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            if let Some(value) = &self.previous {
                std::env::set_var(self.key, value);
            } else {
                std::env::remove_var(self.key);
            }
        }
    }

    fn seed_exportable_history(conn: &Connection) {
        for (source, session, project, prompt, ts) in [
            (
                "claude",
                "s1",
                Some("/tmp/p"),
                "tag the release",
                1_700_000_000_000i64,
            ),
            (
                "codex",
                "s2",
                Some("/tmp/q"),
                "fix the importer",
                1_700_000_001_000,
            ),
            // A row with no project at all: the JSON round-trip has to keep the
            // null rather than inventing a directory.
            (
                "cursor",
                "s3",
                None,
                "write the changelog",
                1_700_000_002_000,
            ),
        ] {
            insert_history(
                conn,
                &HistoryEntry {
                    id: 0,
                    source: source.into(),
                    session_id: Some(session.into()),
                    project: project.map(str::to_string),
                    prompt: prompt.into(),
                    prompt_hash: Some(prompt_hash(prompt)),
                    timestamp_ms: ts,
                },
            )
            .unwrap();
        }
    }

    type ExportedRow = (String, Option<String>, Option<String>, String, i64);

    fn history_rows(conn: &Connection) -> Vec<ExportedRow> {
        conn.prepare(
            "SELECT source, session_id, project, prompt, timestamp_ms FROM history \
             ORDER BY timestamp_ms",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
    }

    fn history_count(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM history", [], |row| row.get(0))
            .unwrap()
    }

    fn fresh_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        conn
    }

    #[test]
    fn a_jsonl_export_round_trips_every_row_into_a_fresh_database() {
        let dir = tempfile::tempdir().unwrap();
        let exported_from = fresh_db();
        seed_exportable_history(&exported_from);
        let path = dir.path().join("history.jsonl");

        export_history(&exported_from, Some(&path), "jsonl", None, None, None).unwrap();

        // One self-describing JSON object per row, oldest first.
        let body = fs::read_to_string(&path).unwrap();
        let lines: Vec<Value> = body
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0]["source"], "claude");
        assert_eq!(lines[0]["prompt"], "tag the release");
        assert_eq!(lines[0]["prompt_hash"], prompt_hash("tag the release"));
        assert_eq!(lines[2]["project"], Value::Null);

        let restored = fresh_db();
        import_history(&restored, &path, false).unwrap();
        assert_eq!(history_rows(&restored), history_rows(&exported_from));
    }

    #[test]
    fn a_gzipped_export_round_trips_every_row_into_a_fresh_database() {
        let dir = tempfile::tempdir().unwrap();
        let exported_from = fresh_db();
        seed_exportable_history(&exported_from);
        let path = dir.path().join("history.jsonl.gz");

        export_history(&exported_from, Some(&path), "jsonl", None, None, None).unwrap();

        // Really gzip, not JSONL that happens to be named .gz.
        assert_eq!(&fs::read(&path).unwrap()[..2], &[0x1f, 0x8b]);

        let restored = fresh_db();
        import_history(&restored, &path, false).unwrap();
        assert_eq!(history_rows(&restored), history_rows(&exported_from));
    }

    #[test]
    fn a_sqlite_export_is_a_readable_searchable_database_of_the_same_rows() {
        let dir = tempfile::tempdir().unwrap();
        let exported_from = fresh_db();
        seed_exportable_history(&exported_from);
        let dest = dir.path().join("history-export.db");

        export_history(&exported_from, Some(&dest), "sqlite", None, None, None).unwrap();

        let exported = Connection::open(&dest).unwrap();
        assert_eq!(history_rows(&exported), history_rows(&exported_from));
        // The export carries the full schema, so search still works on it.
        let hits: i64 = exported
            .query_row(
                "SELECT COUNT(*) FROM history_fts WHERE history_fts MATCH 'release'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(hits, 1);

        // And it imports back like any other export.
        let restored = fresh_db();
        import_history(&restored, &dest, false).unwrap();
        assert_eq!(history_rows(&restored), history_rows(&exported_from));
    }

    #[test]
    fn a_sqlite_export_refuses_to_overwrite_the_active_database() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let active = dir.path().join("ai-history.db");
        let _db_env = EnvVarGuard::set("AI_HIST_DB", &active);
        let conn = open_db(&active).unwrap();
        seed_exportable_history(&conn);

        let error = export_history(&conn, Some(&active), "sqlite", None, None, None)
            .expect_err("exporting over the live database must fail");
        assert!(
            error
                .to_string()
                .contains("Refusing to export SQLite over the active AI_HIST_DB"),
            "unexpected error: {error}"
        );

        // The refusal happens before the destination is truncated, so the live
        // history is still there.
        assert_eq!(history_count(&conn), 3);
    }

    #[test]
    fn reimporting_the_same_export_adds_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let exported_from = fresh_db();
        seed_exportable_history(&exported_from);
        let path = dir.path().join("history.jsonl");
        export_history(&exported_from, Some(&path), "jsonl", None, None, None).unwrap();

        let restored = fresh_db();
        import_history(&restored, &path, false).unwrap();
        assert_eq!(history_count(&restored), 3);
        import_history(&restored, &path, false).unwrap();
        assert_eq!(history_count(&restored), 3);
        assert_eq!(history_rows(&restored), history_rows(&exported_from));
    }

    #[test]
    fn a_dry_run_import_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let exported_from = fresh_db();
        seed_exportable_history(&exported_from);
        let path = dir.path().join("history.jsonl");
        export_history(&exported_from, Some(&path), "jsonl", None, None, None).unwrap();

        let restored = fresh_db();
        import_history(&restored, &path, true).unwrap();
        assert_eq!(history_count(&restored), 0);

        // The same file without --dry-run does write.
        import_history(&restored, &path, false).unwrap();
        assert_eq!(history_count(&restored), 3);
    }

    #[test]
    fn imported_rows_missing_a_prompt_hash_get_one_backfilled() {
        let dir = tempfile::tempdir().unwrap();

        // A hand-written or pre-hash JSONL export.
        let jsonl = dir.path().join("legacy.jsonl");
        fs::write(
            &jsonl,
            "{\"source\":\"claude\",\"session_id\":\"s1\",\"project\":\"/tmp/p\",\
             \"prompt\":\"tag the release\",\"timestamp_ms\":1700000000000}\n",
        )
        .unwrap();
        let from_jsonl = fresh_db();
        import_history(&from_jsonl, &jsonl, false).unwrap();
        let hash: Option<String> = from_jsonl
            .query_row("SELECT prompt_hash FROM history", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            hash.as_deref(),
            Some(prompt_hash("tag the release").as_str())
        );

        // A SQLite export taken before the prompt_hash column existed.
        let legacy_db = dir.path().join("legacy.db");
        let legacy = Connection::open(&legacy_db).unwrap();
        legacy
            .execute_batch(
                "CREATE TABLE history (id INTEGER PRIMARY KEY, source TEXT, session_id TEXT, \
                 project TEXT, prompt TEXT, timestamp_ms INTEGER); \
                 INSERT INTO history VALUES (1, 'codex', 's2', '/tmp/q', 'fix the importer', 1700000001000);",
            )
            .unwrap();
        drop(legacy);
        let from_sqlite = fresh_db();
        import_history(&from_sqlite, &legacy_db, false).unwrap();
        let hash: Option<String> = from_sqlite
            .query_row("SELECT prompt_hash FROM history", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            hash.as_deref(),
            Some(prompt_hash("fix the importer").as_str())
        );
    }

    #[test]
    fn doctor_reports_a_healthy_database_with_every_field_it_promises() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("ai-history.db");
        let conn = open_db(&db_path).unwrap();
        seed_exportable_history(&conn);
        drop(conn);

        let report = doctor_report(&db_path).to_json(&db_path);
        assert_eq!(report["db_path"], db_path.display().to_string());
        assert!(report["db_bytes"].as_u64().unwrap() > 0, "{report}");
        assert!(report["wal_bytes"].as_u64().is_some(), "{report}");
        assert!(
            report["free_bytes"].is_u64() || report["free_bytes"].is_null(),
            "{report}"
        );
        assert_eq!(report["write_lock"], json!("available"));
        assert_eq!(report["write_capable"], json!(true));
        assert!(report["holders"].is_array(), "{report}");
        // Nothing is holding the write lock, so nothing is reported as blocking it.
        let problems = report["problems"].as_array().expect("problems array");
        assert!(
            !problems
                .iter()
                .any(|problem| problem.as_str().unwrap_or("").contains("write lock")),
            "{problems:?}"
        );
    }

    /// Reads one request line from `stream` and drains its headers so the
    /// client sees a complete exchange.
    fn read_request_line(stream: &mut std::net::TcpStream) -> String {
        use std::io::{BufRead, BufReader};

        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut first_line = String::new();
        reader.read_line(&mut first_line).unwrap();
        loop {
            let mut line = String::new();
            if reader.read_line(&mut line).unwrap() == 0 || line.trim_end().is_empty() {
                break;
            }
        }
        first_line.trim_end().to_string()
    }

    /// Answers exactly `count` HTTP requests off `listener` with `respond`, and
    /// hands back the request lines it saw.
    fn serve_http(
        listener: std::net::TcpListener,
        count: usize,
        respond: impl Fn(&str) -> (&'static str, String) + Send + 'static,
    ) -> std::thread::JoinHandle<Vec<String>> {
        listener.set_nonblocking(true).unwrap();
        std::thread::spawn(move || {
            let started = std::time::Instant::now();
            let mut seen = Vec::new();
            while seen.len() < count && started.elapsed() < Duration::from_secs(20) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                    Err(error) => panic!("accept failed: {error}"),
                };
                let line = read_request_line(&mut stream);
                let (status, body) = respond(&line);
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).unwrap();
                seen.push(line);
            }
            assert_eq!(seen.len(), count, "served {seen:?}");
            seen
        })
    }

    /// A Relaycast messages page holding ids `m{first}`..`m{first + count - 1}`.
    fn relay_message_page(first: usize, count: usize) -> String {
        let messages: Vec<Value> = (first..first + count)
            .map(|n| json!({"id": format!("m{n:03}"), "from_name": "ana", "text": format!("relay message {n:03}")}))
            .collect();
        serde_json::to_string(&json!({ "data": messages })).unwrap()
    }

    fn relaycast_env(base: &str) -> (EnvVarGuard, EnvVarGuard, EnvVarGuard) {
        (
            EnvVarGuard::set("RELAYCAST_API_KEY", "rc_test_key"),
            EnvVarGuard::set("RELAYCAST_WORKSPACE_ID", "ws-e2e"),
            EnvVarGuard::set("RELAYCAST_BASE_URL", base),
        )
    }

    #[test]
    fn relaycast_sync_pages_through_a_channel_and_saves_the_high_water_mark() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        // channels, messages page 1, messages page 2, dm listing.
        let server = serve_http(listener, 4, |line| {
            if line.starts_with("GET /v1/channels ") {
                ("200 OK", r#"{"data":[{"name":"general"}]}"#.to_string())
            } else if line.starts_with("GET /v1/channels/general/messages?limit=100&after=m099 ") {
                ("200 OK", relay_message_page(100, 2))
            } else if line.starts_with("GET /v1/channels/general/messages?limit=100 ") {
                // A full page is the signal that another page may follow.
                ("200 OK", relay_message_page(0, 100))
            } else if line.starts_with("GET /v1/dm/conversations/all ") {
                ("200 OK", r#"{"data":[]}"#.to_string())
            } else {
                panic!("unexpected request: {line}");
            }
        });

        let conn = fresh_db();
        let mut state = Map::new();
        let _env = relaycast_env(&format!("http://{addr}"));
        let inserted = sync_relaycast(&conn, &mut state).unwrap();
        let requests = server.join().unwrap();

        assert_eq!(inserted, 102);
        assert_eq!(history_count(&conn), 102);
        assert!(
            requests[2].contains("after=m099"),
            "the second page must continue from the last id of the first: {requests:?}"
        );

        // Messages are attributed to their sender and the workspace.
        let (prompt, project, session_id): (String, Option<String>, Option<String>) = conn
            .query_row(
                "SELECT prompt, project, session_id FROM history WHERE prompt LIKE '%message 000'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(prompt, "[ana] relay message 000");
        assert_eq!(project.as_deref(), Some("ws-e2e"));
        // The channel name is the session; "ch:general" is only the state key.
        assert_eq!(session_id.as_deref(), Some("#general"));

        // The cursor saved is the highest id seen across both pages.
        assert_eq!(state["relay"]["ch:general"], json!("m101"));
    }

    #[test]
    fn relaycast_sync_resumes_from_the_saved_after_cursor() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = serve_http(listener, 3, |line| {
            if line.starts_with("GET /v1/channels ") {
                ("200 OK", r#"{"data":[{"name":"general"}]}"#.to_string())
            } else if line.starts_with("GET /v1/channels/general/messages?limit=100&after=m050 ") {
                ("200 OK", relay_message_page(51, 1))
            } else if line.starts_with("GET /v1/dm/conversations/all ") {
                ("200 OK", r#"{"data":[]}"#.to_string())
            } else {
                panic!("unexpected request: {line}");
            }
        });

        let conn = fresh_db();
        let mut state = Map::new();
        state.insert("relay".into(), json!({"ch:general": "m050"}));
        let _env = relaycast_env(&format!("http://{addr}"));
        let inserted = sync_relaycast(&conn, &mut state).unwrap();
        let requests = server.join().unwrap();

        // Only the one message after the cursor is asked for, and stored.
        assert_eq!(inserted, 1);
        assert_eq!(history_count(&conn), 1);
        assert!(requests[1].contains("after=m050"), "{requests:?}");
        assert_eq!(state["relay"]["ch:general"], json!("m051"));
    }

    #[test]
    fn relaycast_sync_keeps_channel_rows_when_the_dm_listing_is_forbidden() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = serve_http(listener, 3, |line| {
            if line.starts_with("GET /v1/channels ") {
                ("200 OK", r#"{"data":[{"name":"general"}]}"#.to_string())
            } else if line.starts_with("GET /v1/channels/general/messages?limit=100 ") {
                ("200 OK", relay_message_page(0, 1))
            } else if line.starts_with("GET /v1/dm/conversations/all ") {
                // Plenty of API keys have channel scope but no DM scope.
                ("403 Forbidden", r#"{"error":"forbidden"}"#.to_string())
            } else {
                panic!("unexpected request: {line}");
            }
        });

        let conn = fresh_db();
        let mut state = Map::new();
        let _env = relaycast_env(&format!("http://{addr}"));
        let inserted = sync_relaycast(&conn, &mut state).unwrap();
        server.join().unwrap();

        // The DM refusal is tolerated: the channel rows still land.
        assert_eq!(inserted, 1);
        assert_eq!(history_count(&conn), 1);
        assert_eq!(state["relay"]["ch:general"], json!("m000"));
    }

    #[test]
    fn relaycast_sync_is_a_no_op_without_credentials() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
        let conn = fresh_db();
        let mut state = Map::new();
        // An unreachable base url proves no request is attempted at all.
        let _env = (
            EnvVarGuard::set("RELAYCAST_API_KEY", ""),
            EnvVarGuard::set("RELAYCAST_WORKSPACE_ID", "ws-e2e"),
            EnvVarGuard::set("RELAYCAST_BASE_URL", "http://127.0.0.1:1"),
        );
        assert_eq!(sync_relaycast(&conn, &mut state).unwrap(), 0);
        assert_eq!(history_count(&conn), 0);
        assert!(state.is_empty());
    }
}
