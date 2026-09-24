use ai_hist::{
    default_db_path, import_json, insert_history, normalize_tag_name, open_db, open_db_readonly,
    prompt_hash, recent, resume_command, schema_is_current, search, session, session_events,
    session_file_edits, session_markers_page, session_tool_calls, session_usage_summary,
    untag_session, HistoryEntry, ProjectGrouping, QueryFilter, SessionEvidenceCursor,
    SessionMarkerPage, SessionUsageSummary, SESSION_EVIDENCE_CONTRACT_VERSION,
    SESSION_USAGE_CONTRACT_VERSION, SOURCE_CHOICES,
};
pub use ai_hist::{SessionLocation, SessionScope};
use anyhow::{Context, Result};
use chrono::{Local, TimeZone};
use clap::{Args, Parser, Subcommand};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use std::fs;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ai_hist::diagnostics::{doctor_report, human_bytes, DoctorReport};
use ai_hist::git_helpers::*;
use ai_hist::history_search::{search_all, SearchRole, SearchRow};
use ai_hist::paths::{default_opencode_db_path, home_dir};
use ai_hist::{discover, remote, *};
mod learn;
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
    /// Explicit remote source connector (repeatable). Defaults to provider CLIs only.
    #[arg(
        long = "source-connector",
        global = true,
        conflicts_with = "no_source_connectors"
    )]
    source_connectors: Vec<String>,
    /// Disable all remote source connectors, including provider CLIs.
    #[arg(long, global = true)]
    no_source_connectors: bool,
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
        /// Group `top_projects` by the raw working directory instead of the
        /// canonical project key. The default merges two checkouts of one
        /// repository; this restores the pre-#175 per-directory grouping.
        #[arg(long)]
        by_cwd: bool,
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
    ///
    /// Wakes on filesystem events under the providers' session roots, with a
    /// slow poll as a backstop. Falls back to pure polling at `--interval`
    /// when no root can be watched.
    Watch {
        #[command(flatten)]
        scope: SessionScopeArgs,
        /// Seconds between polls. Used as the only cadence when filesystem
        /// events are unavailable or disabled.
        #[arg(long, default_value_t = 60)]
        interval: u64,
        /// Poll only. Use on filesystems where change notifications are
        /// unreliable (network mounts, some container filesystems).
        #[arg(long)]
        no_fsevents: bool,
        /// Milliseconds of filesystem events to collapse into one sweep.
        #[arg(long, default_value_t = ai_hist::watch::DEFAULT_DEBOUNCE_MS)]
        debounce_ms: u64,
    },
    /// Ingest one agent session from a lifecycle hook payload on stdin.
    ///
    /// Reads the harness's hook JSON (`session_id`, `transcript_path`, …) and
    /// hydrates exactly that transcript. Always exits 0: a hook that fails
    /// would fail the tool call the agent is in the middle of.
    Ingest {
        /// Harness whose hook payload is on stdin. Only `claude` today.
        #[arg(long, value_name = "HARNESS")]
        hook: String,
        /// Say nothing at all, on any stream. Wins over --json.
        #[arg(long)]
        quiet: bool,
        /// Print the ingest report as JSON on stdout. Ignored under --quiet.
        #[arg(long)]
        json: bool,
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
        /// Restrict to one canonical project key, as `project_key` reports it:
        /// `host/owner/repo` (for example `github.com/AgentWorkforce/relayhistory`),
        /// or the working directory for a checkout with no git remote. Exact
        /// match — this is the key, not a path or a search term.
        #[arg(long)]
        project: Option<String>,
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
    /// Page through one session's markers from the database only.
    ///
    /// Markers are the provider records the event model cannot carry:
    /// compaction and summary boundaries, provider `system` rows, non-text
    /// content blocks, agent lifecycle events. Same `(ts_ms IS NULL, ts_ms,
    /// id)` keyset as tool calls and file edits; undated markers page last.
    Markers {
        /// Coding-agent source (claude, codex, cursor, grok, muse, relay, opencode).
        source: String,
        /// Native session identifier within that source.
        session_id: String,
        /// Maximum rows per page (default 200, at most 1000).
        #[arg(long)]
        limit: Option<i64>,
        /// Precise continuation: `id` of the previous page's last row.
        #[arg(long)]
        after_id: Option<i64>,
        /// Precise continuation: `ts_ms` of the previous page's last row. Omit
        /// (with --after-id given) to continue through the undated tail.
        #[arg(long, requires = "after_id")]
        after_ms: Option<i64>,
        /// Emit `{"contract_version":N,"source":…,"session_id":…,"markers":[...],"next_cursor":…}`
        /// as one JSON object.
        #[arg(long)]
        json: bool,
    },
    /// Provider-reported token usage rollup for one session, from the
    /// database only. Usage is what the provider recorded, never estimated,
    /// and cost is never computed: `reported_cost_usd` appears only when the
    /// source data carried one.
    Usage {
        /// Coding-agent source (claude, codex, cursor, grok, muse, relay, opencode).
        source: String,
        /// Native session identifier within that source.
        session_id: String,
        /// Emit `{"contract_version":N,"source":…,"session_id":…,"summary":{…}|null}`
        /// as one JSON object. The summary keeps the crate's camelCase wire form.
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
enum LearnAction {
    /// Distill local session history into decision/finding/reflection events.
    Distill {
        /// Only distill sessions from this source (claude, codex, cursor, grok, muse, relay, opencode).
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
            // `sessions list` is cache-only by construction: one indexed query
            // over `sessions` and no provider I/O at all. `sessions discover`
            // upserts, so it is deliberately absent.
            | Command::Sessions {
                action: SessionsAction::List { .. }
                    | SessionsAction::Markers { .. }
                    | SessionsAction::Usage { .. }
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
    let connectors = if cli.no_source_connectors {
        remote::SourceConnectorSelection::new(Vec::new())?
    } else if cli.source_connectors.is_empty() {
        remote::SourceConnectorSelection::default()
    } else {
        remote::SourceConnectorSelection::new(cli.source_connectors.clone())?
    };
    let db_path = cli.db.unwrap_or_else(default_db_path);
    // Sync commands must acquire their advisory lock before opening a writable connection.
    // Pre-dispatch them so the common connection setup below cannot initialize the schema or
    // wait on SQLite before contention is detected.
    match &cli.command {
        Command::Sync {
            scope,
            install_service,
            uninstall_service,
            interval,
        } => {
            if *install_service {
                if scope.resolve() == SessionScope::Remote {
                    remote::ensure_selected_remote_connectors_configured_for(
                        "sync",
                        &[],
                        &connectors,
                    )?;
                }
                let mut service_args = scope.service_args();
                if cli.no_source_connectors {
                    service_args.push("--no-source-connectors".into());
                }
                for id in &cli.source_connectors {
                    service_args.extend(["--source-connector".to_string(), id.clone()]);
                }
                return install_managed_service(&SYNC_SERVICE, *interval, &service_args);
            }
            if *uninstall_service {
                return uninstall_managed_service(&SYNC_SERVICE);
            }
            return sync_scoped_at_with_output(
                &db_path,
                scope.resolve(),
                &connectors,
                SyncOutput::Progress,
            )
            .map(|_| ());
        }
        Command::Watch {
            scope,
            interval,
            no_fsevents,
            debounce_ms,
        } => {
            if scope.resolve() == SessionScope::Remote {
                remote::ensure_selected_remote_connectors_configured_for("sync", &[], &connectors)?;
            }
            return watch_loop_with_connectors(
                &db_path,
                *interval,
                scope.resolve(),
                &connectors,
                WatchDrivers {
                    use_fs_events: !*no_fsevents,
                    debounce_ms: *debounce_ms,
                },
            );
        }
        Command::Ingest { hook, quiet, json } => {
            return run_hook_ingest(&db_path, hook, *quiet, *json);
        }
        Command::Sessions {
            action: SessionsAction::Discover { scope, source, .. },
        } if scope.resolve() == SessionScope::Remote => {
            remote::ensure_selected_remote_connectors_configured_for(
                "discovery",
                source,
                &connectors,
            )?;
        }
        Command::SyncOpencode { opencode_db } => {
            let source = opencode_db.clone().unwrap_or_else(default_opencode_db_path);
            return sync_opencode_at(&db_path, &source, SyncOutput::Progress).map(|_| ());
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
        Command::Stats {
            scope,
            tag,
            by_cwd,
            json,
        } => {
            let grouping = if by_cwd {
                ProjectGrouping::Cwd
            } else {
                ProjectGrouping::ProjectKey
            };
            print_stats(&conn, scope.resolve(), tag.as_deref(), grouping, json)
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
        Command::SyncOpencode { .. }
        | Command::Sync { .. }
        | Command::Watch { .. }
        | Command::Ingest { .. } => {
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
                project,
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
                        project_key: project,
                    },
                )?;
                print_session_catalog(&page, json)
            }
            SessionsAction::Markers {
                source,
                session_id,
                limit,
                after_id,
                after_ms,
                json,
            } => {
                validate_source(Some(&source))?;
                anyhow::ensure!(
                    !session_id.trim().is_empty(),
                    "SESSION_ID must not be empty"
                );
                let limit = limit.unwrap_or(200);
                anyhow::ensure!(
                    (1..=1_000).contains(&limit),
                    "--limit must be between 1 and 1000 (got {limit})"
                );
                let after = after_id.map(|id| SessionEvidenceCursor {
                    ts_ms: after_ms,
                    id,
                });
                let page =
                    session_markers_page(&conn, &source, &session_id, limit, after.as_ref())?;
                print_session_markers(&source, &session_id, &page, json)
            }
            SessionsAction::Usage {
                source,
                session_id,
                json,
            } => {
                validate_source(Some(&source))?;
                anyhow::ensure!(
                    !session_id.trim().is_empty(),
                    "SESSION_ID must not be empty"
                );
                let summary = session_usage_summary(&conn, &source, &session_id)?;
                print_session_usage(&source, &session_id, summary.as_ref(), json)
            }
            SessionsAction::Discover {
                scope,
                source,
                limit,
                json,
            } => run_session_discovery(&conn, scope.resolve(), source, limit, json, &connectors),
        },
    }
}

/// Render `ai-hist sessions markers`.
///
/// One JSON object, so the contract version and the cursor travel with the
/// rows. Feed `next_cursor` back as `--after-id` (and `--after-ms` when it is
/// not null) to continue; it is null once the session's markers are exhausted.
fn print_session_markers(
    source: &str,
    session_id: &str,
    page: &SessionMarkerPage,
    as_json: bool,
) -> Result<()> {
    if as_json {
        println!(
            "{}",
            json!({
                "contract_version": SESSION_EVIDENCE_CONTRACT_VERSION,
                "source": source,
                "session_id": session_id,
                "markers": page.markers,
                "next_cursor": page.next_cursor,
            })
        );
        return Ok(());
    }
    if page.markers.is_empty() {
        println!("No markers for {source}/{session_id}.");
        return Ok(());
    }
    for marker in &page.markers {
        let ts = marker
            .ts_ms
            .and_then(|ms| Local.timestamp_millis_opt(ms).single())
            .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
            .unwrap_or_else(|| "-".to_string());
        println!(
            "{ts}  {}  {}  {}  {}",
            marker.kind,
            marker.subkind.as_deref().unwrap_or("-"),
            marker.message_id.as_deref().unwrap_or("-"),
            marker.text.as_deref().unwrap_or("-"),
        );
    }
    println!("  {} marker(s)", page.markers.len());
    if let Some(cursor) = &page.next_cursor {
        println!(
            "  more available: --after-id {}{}",
            cursor.id,
            cursor
                .ts_ms
                .map(|ts| format!(" --after-ms {ts}"))
                .unwrap_or_default()
        );
    }
    Ok(())
}

/// Render `ai-hist sessions usage`.
///
/// A session with no recorded request is `summary: null`; a session whose
/// usage could not be established is a summary with `usage: null` and the
/// diagnostics saying why. Neither is zero, because zero is a claim.
fn print_session_usage(
    source: &str,
    session_id: &str,
    summary: Option<&SessionUsageSummary>,
    as_json: bool,
) -> Result<()> {
    if as_json {
        println!(
            "{}",
            json!({
                "contract_version": SESSION_USAGE_CONTRACT_VERSION,
                "source": source,
                "session_id": session_id,
                "summary": summary,
            })
        );
        return Ok(());
    }
    let Some(summary) = summary else {
        println!("No requests recorded for {source}/{session_id}.");
        return Ok(());
    };
    println!(
        "{source}/{session_id}: {} of {} request(s) with usage",
        summary.request_count, summary.total_request_count
    );
    match &summary.usage {
        Some(usage) => {
            println!(
                "tokens: input={} output={} cache_read={} cache_write={} reasoning={} provider_total={}",
                usage.input_tokens,
                usage.output_tokens,
                usage.cache_read_tokens,
                usage.cache_write_tokens,
                usage
                    .reasoning_tokens
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                usage
                    .provider_total_tokens
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| "-".to_string()),
            );
            println!(
                "accounting: {}; reported cost: {}",
                summary
                    .accounting
                    .iter()
                    .map(|mode| mode.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
                usage
                    .reported_cost_usd
                    .map(|cost| cost.to_string())
                    .unwrap_or_else(|| "none".to_string()),
            );
        }
        None => println!(
            "tokens: none established{}",
            if summary.overflowed {
                " (overflowed)"
            } else {
                ""
            }
        ),
    }
    if !summary.models.is_empty() {
        println!("models: {}", summary.models.join(", "));
    }
    for diagnostic in &summary.diagnostics {
        println!("diagnostic: {}", diagnostic.as_str());
    }
    Ok(())
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
    connectors: &remote::SourceConnectorSelection,
) -> Result<()> {
    let options = DiscoverOptions {
        scope,
        sources,
        limit,
    };
    let mut count = 0usize;
    let outcome = discover::discover_sessions_with_connectors(
        &DiscoveryEnv::new(conn),
        &options,
        connectors,
        |session| {
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
        },
    );
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
    events: Vec<ai_hist::SessionEvent>,
    tool_calls: Vec<ai_hist::SessionToolCall>,
    file_edits: Vec<ai_hist::SessionFileEdit>,
    width: usize,
    json: bool,
) -> Result<()> {
    if json {
        // One replay stream, merged chronologically across the three record
        // types (unknown timestamps last, ties broken by row id). Sorting
        // references and serializing at write time keeps peak memory at the
        // fetched rows themselves, not a second serialized copy.
        enum ReplayRecord<'a> {
            Event(&'a ai_hist::SessionEvent),
            ToolCall(&'a ai_hist::SessionToolCall),
            FileEdit(&'a ai_hist::SessionFileEdit),
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
    grouping: ProjectGrouping,
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
    let project_key = grouping.expression();
    let project_where = format!("{where_sql} AND {project_key} IS NOT NULL");
    let top_projects = query_pairs(
        conn,
        &format!(
            "SELECT {project_key}, COUNT(*) FROM history h {project_where} \
             GROUP BY {project_key} ORDER BY COUNT(*) DESC LIMIT 10"
        ),
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
                // Which key `top_projects` is bucketed by. A consumer that
                // reads counts without reading this cannot tell a canonical
                // repository rollup from a per-directory one.
                "grouped_by": grouping.as_str(),
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
    println!("\nTop 10 projects (by {}):", grouping.as_str());
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
        ai_hist::init_db(&dst)?;
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

fn doctor_report_json(report: &DoctorReport, db_path: &Path) -> Value {
    json!({
        "db_path": db_path.display().to_string(),
        "db_bytes": report.db_bytes,
        "wal_bytes": report.wal_bytes,
        "free_bytes": report.free,
        "write_lock": match &report.lock {
            Ok(()) => json!("available"),
            Err(err) => json!({"blocked": err}),
        },
        "write_capable": report.lock.is_ok(),
        "holders": report.holders.iter().map(|h| json!({
            "pid": h.pid,
            "state": h.state,
            "command": h.command,
            "wedged": h.is_wedged(),
        })).collect::<Vec<_>>(),
        "problems": report.problems,
    })
}

fn doctor(db_path: &Path, json: bool) -> Result<()> {
    let report = doctor_report(db_path);
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&doctor_report_json(&report, db_path))?
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

/// How `watch` should be driven. Both knobs exist because filesystem change
/// notifications are not uniformly trustworthy: `--no-fsevents` is the escape
/// hatch for a filesystem that lies, and the debounce window is how long a
/// burst of events is allowed to collapse for.
struct WatchDrivers {
    use_fs_events: bool,
    debounce_ms: u64,
}

fn watch_loop(db_path: &Path, interval: u64, scope: SessionScope) -> Result<()> {
    watch_loop_with_connectors(
        db_path,
        interval,
        scope,
        &remote::SourceConnectorSelection::default(),
        WatchDrivers {
            use_fs_events: true,
            debounce_ms: ai_hist::watch::DEFAULT_DEBOUNCE_MS,
        },
    )
}

fn watch_loop_with_connectors(
    db_path: &Path,
    interval: u64,
    scope: SessionScope,
    connectors: &remote::SourceConnectorSelection,
    drivers: WatchDrivers,
) -> Result<()> {
    let roots = watch_roots_for_scope(scope);
    let remote_only = scope == SessionScope::Remote;
    let db = db_path.to_path_buf();
    let connectors = connectors.clone();
    let tick: ai_hist::watch::TickFn = Arc::new(move |force| {
        let tick = sync_tick_at(&db, scope, &connectors, SyncOutput::Progress, force)?;
        Ok(ai_hist::watch::TickOutcome::from(tick))
    });
    let mut watch = ai_hist::watch::WatchLoop::new(tick)
        .with_roots(roots)
        .with_fs_events(drivers.use_fs_events)
        .with_debounce_ms(drivers.debounce_ms)
        .with_poll_interval_ms(interval.saturating_mul(1000))
        .with_immediate(true)
        .on_error(Arc::new(|error| eprintln!("Error: {error:#}")))
        .on_driver(Arc::new(move |status| {
            match status.driver {
                ai_hist::watch::WatchDriver::FsEvents => println!(
                    "Watching {} session root(s) for changes (debounce {}ms, {}s backstop; Ctrl-C to stop)...",
                    status.watched.len(),
                    drivers.debounce_ms,
                    ai_hist::watch::DEFAULT_SLOW_POLL_MS / 1000
                ),
                ai_hist::watch::WatchDriver::Polling if remote_only => println!(
                    "Watching remote connectors every {interval}s (Ctrl-C to stop)..."
                ),
                ai_hist::watch::WatchDriver::Polling => {
                    println!("Watching every {interval}s (Ctrl-C to stop)...")
                }
            }
            // Saying which roots are uncovered is the difference between "live
            // capture is on" and "live capture is on for the providers that
            // were installed when you started it".
            if !status.pending.is_empty() {
                println!(
                    "  not watched yet (retried every {}s): {}",
                    ai_hist::watch::DEFAULT_SLOW_POLL_MS / 1000,
                    status
                        .pending
                        .iter()
                        .map(|path| path.display().to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
        }));
    if !remote_only {
        // A project that grows a `.trajectories` directory after the run
        // started is a root whose name could not have been known at startup,
        // so the pending-retry path alone would never reach it.
        watch = watch.with_roots_refresh(Arc::new(|| watch_roots_for_scope(SessionScope::Local)));
    }
    watch.run().map(|_| ())
}

/// The roots watch installs for a scope.
///
/// A remote-scoped watch installs none. Local provider roots would otherwise
/// select the filesystem-event driver, and every local Claude or Codex write —
/// which a remote-only run is not collecting at all — would fire the remote
/// connectors, plus the 30s backstop on top. The user asked for remote at
/// `--interval`; that is what they get.
fn watch_roots_for_scope(scope: SessionScope) -> Vec<ai_hist::discover::WatchRoot> {
    if scope == SessionScope::Remote {
        return Vec::new();
    }
    sync_watch_roots(&home_dir(), &default_opencode_db_path())
}

/// `ingest --hook <harness>`: read one lifecycle-hook payload from stdin and
/// ingest the transcript it names.
///
/// Never returns `Err`. A hook runs inside the agent's tool call, and a
/// non-zero exit there is a failed tool call — so a missing transcript, an
/// unparseable payload, or a locked database are all reported and shrugged off.
/// `--quiet` silences the reporting, not the shrug.
fn run_hook_ingest(db_path: &Path, hook: &str, quiet: bool, json: bool) -> Result<()> {
    let note = |message: String| {
        if !quiet {
            eprintln!("ai-hist ingest: {message}");
        }
    };
    if !ai_hist::HOOK_HARNESSES.contains(&hook) {
        note(format!(
            "unsupported hook harness '{hook}'; known: {}",
            ai_hist::HOOK_HARNESSES.join(", ")
        ));
        return Ok(());
    }
    let mut payload = String::new();
    if let Err(error) = std::io::Read::read_to_string(&mut std::io::stdin(), &mut payload) {
        note(format!("could not read the hook payload: {error}"));
        return Ok(());
    }
    if payload.trim().is_empty() {
        note("empty hook payload; nothing to do".into());
        return Ok(());
    }
    let parsed: serde_json::Value = match serde_json::from_str(&payload) {
        Ok(value) => value,
        Err(error) => {
            note(format!("hook payload is not JSON: {error}"));
            return Ok(());
        }
    };
    // Mirrors the harness contract: a payload with no `session_id` is not a
    // session lifecycle event we can attribute, so it is ignored rather than
    // guessed at.
    let Some(session_id) = parsed
        .get("session_id")
        .and_then(serde_json::Value::as_str)
        .filter(|id| !id.is_empty())
    else {
        note("hook payload has no session_id; ignoring".into());
        return Ok(());
    };
    let transcript = parsed
        .get("transcript_path")
        .and_then(serde_json::Value::as_str)
        .filter(|path| !path.is_empty())
        .map(PathBuf::from);

    let outcome = match &transcript {
        // Some hook events elide `transcript_path`. Falling back to a forced
        // sweep still makes progress rather than dropping the event: forced,
        // because the hook fired precisely because something just changed.
        None => {
            note("hook payload has no transcript_path; running a full sweep".into());
            sync_tick_at(
                db_path,
                SessionScope::Local,
                &remote::SourceConnectorSelection::default(),
                SyncOutput::Silent,
                true,
            )
            .map(|tick| {
                serde_json::json!({
                    "source": hook,
                    "status": if tick.swept { "swept" } else { "skipped" },
                })
            })
        }
        // The payload names both the session and the file. Passing the
        // session through is what lets the ingest refuse a pairing the two
        // disagree about, rather than hydrating whatever the file turns out
        // to be.
        Some(path) => ai_hist::ingest_transcript_at(db_path, hook, path, Some(session_id), true)
            .and_then(|report| Ok(serde_json::to_value(&report)?)),
    };
    match outcome {
        Ok(report) => {
            // `--quiet` outranks `--json`. A hook's contract is "exit 0, say
            // nothing"; a caller passing both asked for silence and for a
            // shape to parse if anything *is* said, and silence is the
            // stronger of the two. Printing anyway would put JSON on the
            // stdout of a hook that was told to stay out of the way.
            match (quiet, json) {
                (true, _) => {}
                (false, true) => println!("{report}"),
                (false, false) => {
                    let status = report
                        .get("status")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("done");
                    eprintln!("ai-hist ingest: {hook} {status}");
                }
            }
        }
        Err(error) => note(format!("{error:#}")),
    }
    Ok(())
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

const PROVIDER_ENV_VARS: [&str; 5] = [
    "CLAUDE_CONFIG_DIR",
    "CODEX_HOME",
    "GROK_HOME",
    "OPENCODE_DB",
    // Muse Code keeps its sessions under `$XDG_DATA_HOME/muse/sessions`.
    "XDG_DATA_HOME",
];

fn scheduler_environment_value(name: &str, value: std::ffi::OsString) -> Result<Option<String>> {
    let value = value.into_string().map_err(|_| {
        anyhow::anyhow!(
            "cannot install sync service: {name} contains non-UTF-8 bytes; use a UTF-8 provider path"
        )
    })?;
    if value.trim().is_empty() {
        return Ok(None);
    }
    if value.contains(['\n', '\r']) {
        anyhow::bail!(
            "cannot install sync service: {name} contains a newline, which scheduler files cannot represent safely"
        );
    }
    Ok(Some(value))
}

fn service_provider_environment(spec: &ServiceSpec) -> Result<Vec<(&'static str, String)>> {
    if spec.subcommand != "sync" {
        return Ok(Vec::new());
    }
    let mut environment = Vec::new();
    for name in PROVIDER_ENV_VARS {
        let Some(value) = std::env::var_os(name) else {
            continue;
        };
        if let Some(value) = scheduler_environment_value(name, value)? {
            environment.push((name, value));
        }
    }
    Ok(environment)
}

fn launchd_environment_xml(environment: &[(&str, String)]) -> String {
    if environment.is_empty() {
        return String::new();
    }
    let entries = environment
        .iter()
        .map(|(name, value)| {
            format!(
                "        <key>{}</key>\n        <string>{}</string>",
                xml_escape(name),
                xml_escape(value)
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!("    <key>EnvironmentVariables</key>\n    <dict>\n{entries}\n    </dict>\n")
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

/// Quote one shell word and then protect `%` from cron's command-to-stdin
/// splitting. The added backslash is consumed by cron before the shell sees it.
fn cron_shell_word(s: &str) -> String {
    shell_single_quote(s).replace('%', "\\%")
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
    let environment = service_provider_environment(spec)?;
    let environment_xml = launchd_environment_xml(&environment);
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
{environment_xml}    <key>StartInterval</key>
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
        environment_xml = environment_xml,
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
    let environment = service_provider_environment(spec)?;
    let command = environment
        .iter()
        .map(|(name, value)| format!("{name}={}", cron_shell_word(value)))
        .chain(std::iter::once(cron_shell_word(bin)))
        .chain(
            service_command_args(spec, args)
                .iter()
                .map(|arg| cron_shell_word(arg)),
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
    ai_hist::mark_session_presence(
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

fn git_remote(repo: &Path) -> Result<String> {
    let out = git_stdout(repo, &["remote", "get-url", "origin"])?;
    Ok(strip_url_credentials(out.trim()))
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
    let sessions = ai_hist::matching_sessions(conn, session_id, source)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use ai_hist::{init_db, insert_history, open_db, prompt_hash, HistoryEntry};
    use rusqlite::Connection;
    use serde_json::{json, Value};
    use std::fs;

    /// A remote-scoped watch must install no local roots. If it did, the
    /// filesystem-event driver would be selected by local writes the run is
    /// not even collecting, and every one of them would fire the remote
    /// connectors — plus the 30s backstop, instead of the requested interval.
    #[test]
    fn a_remote_watch_installs_no_local_roots() {
        assert!(
            watch_roots_for_scope(SessionScope::Remote).is_empty(),
            "remote scope must not watch local provider roots"
        );
        for scope in [SessionScope::Local, SessionScope::All] {
            assert!(
                !watch_roots_for_scope(scope).is_empty(),
                "{scope:?} must still watch local provider roots"
            );
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

    /// `sessions markers` and `sessions usage` are cache-only reads over the
    /// evidence tables, so they must not take the write lock any more than
    /// `sessions list` does; `sessions discover` upserts and must.
    #[test]
    fn session_evidence_reads_get_a_read_only_handle() {
        assert!(super::is_read_only(&super::Command::Sessions {
            action: super::SessionsAction::Markers {
                source: "claude".into(),
                session_id: "s".into(),
                limit: None,
                after_id: None,
                after_ms: None,
                json: false,
            }
        }));
        assert!(super::is_read_only(&super::Command::Sessions {
            action: super::SessionsAction::Usage {
                source: "claude".into(),
                session_id: "s".into(),
                json: false,
            }
        }));
        assert!(!super::is_read_only(&super::Command::Sessions {
            action: super::SessionsAction::Discover {
                scope: super::SessionScopeArgs::default(),
                source: Vec::new(),
                limit: None,
                json: false,
            }
        }));
    }

    #[test]
    fn only_non_mutating_commands_get_a_read_only_handle() {
        // Reads must not take the write lock...
        assert!(super::is_read_only(&super::Command::Stats {
            scope: super::SessionScopeArgs::default(),
            tag: None,
            by_cwd: false,
            json: false
        }));
        assert!(super::is_read_only(&super::Command::Show {
            id: 1,
            json: false
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

        ai_hist::mark_session_presence(
            &conn,
            "codex",
            "cloud-session",
            super::SessionLocation::Remote,
        )
        .unwrap();
        let (locations, command) = super::local_resume_details(&conn, &entry).unwrap();
        assert_eq!(locations, vec!["remote"]);
        assert!(command.is_none());

        ai_hist::mark_session_presence(
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
    fn service_environment_rendering_preserves_relocated_provider_roots() {
        let environment = vec![
            ("CLAUDE_CONFIG_DIR", "/srv/Claude & tools".to_string()),
            ("CODEX_HOME", "/srv/codex's 100% \\archive".to_string()),
        ];
        let xml = launchd_environment_xml(&environment);
        assert!(xml.contains("<key>EnvironmentVariables</key>"));
        assert!(xml.contains("<key>CLAUDE_CONFIG_DIR</key>"));
        assert!(xml.contains("<string>/srv/Claude &amp; tools</string>"));
        assert!(xml.contains("<key>CODEX_HOME</key>"));

        let cron = environment
            .iter()
            .map(|(name, value)| format!("{name}={}", cron_shell_word(value)))
            .collect::<Vec<_>>()
            .join(" ");
        assert_eq!(
            cron,
            r"CLAUDE_CONFIG_DIR='/srv/Claude & tools' CODEX_HOME='/srv/codex'\''s 100\% \archive'"
        );
    }

    #[test]
    fn scheduler_environment_rejects_line_breaks() {
        let error =
            scheduler_environment_value("CODEX_HOME", "/srv/codex\narchive".into()).unwrap_err();
        assert!(error.to_string().contains("contains a newline"));
    }

    #[cfg(unix)]
    #[test]
    fn scheduler_environment_rejects_non_utf8_paths_without_changing_them() {
        use std::os::unix::ffi::OsStringExt;

        let value = std::ffi::OsString::from_vec(b"/srv/codex-\xff".to_vec());
        let error = scheduler_environment_value("CODEX_HOME", value).unwrap_err();
        assert!(error.to_string().contains("contains non-UTF-8 bytes"));
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

        let report = doctor_report_json(&doctor_report(&db_path), &db_path);
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
}
