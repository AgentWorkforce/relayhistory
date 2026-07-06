use ai_hist_core::convergence::MachineIdentity;
use ai_hist_core::{
    default_db_path, import_json, insert_history, normalize_tag_name, open_db, parse_cursor_text,
    prompt_hash, recent, resume_command, search, session, sync_opencode_db, untag_session,
    HistoryEntry, QueryFilter, SOURCE_CHOICES,
};
use anyhow::{Context, Result};
use chrono::{Local, TimeZone};
use clap::{Parser, Subcommand};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use rusqlite::{params, Connection};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

mod cloud;
mod learn;

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
        #[arg(long)]
        fts: bool,
        #[arg(long)]
        json: bool,
    },
    /// Show recent history entries.
    Recent {
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
    /// Show local history statistics.
    Stats {
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
        #[arg(required = true)]
        query: Vec<String>,
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
    /// Sync local agent history into the relayhistory database.
    Sync {
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
    /// Repeatedly sync local agent history.
    Watch {
        #[arg(long, default_value_t = 60)]
        interval: u64,
    },
    /// Build a compact context pack from matching history.
    Pack {
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
        #[arg(long, default_value = "ai-hist-cli")]
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
    /// Push new local history + trajectory events to relayhistory-cloud.
    Push {
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
}

#[derive(Subcommand)]
enum PairAction {
    /// Ask relayhistory-cloud for advisory warnings before an action (POST /v1/pair/check).
    Check {
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
    /// Link the most recent matching session to a git commit and optional git note.
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

fn main() -> Result<()> {
    let cli = Cli::parse();
    let db_path = cli.db.unwrap_or_else(default_db_path);
    let conn = open_db(&db_path)?;

    match cli.command {
        Command::Search {
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
        Command::Show { id, json } => show_entry(&conn, id, json),
        Command::Context { id, window } => show_context(&conn, id, window),
        Command::Pack {
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
        Command::Stats { tag, json } => print_stats(&conn, tag.as_deref(), json),
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
        Command::Resume { query, fts, json } => {
            let rows = search(
                &conn,
                &query,
                fts,
                &QueryFilter {
                    limit: 1,
                    ..Default::default()
                },
            )?;
            let entry = rows
                .into_iter()
                .find(|e| e.session_id.as_ref().is_some_and(|s| !s.is_empty()));
            if let Some(entry) = entry {
                let cmd = resume_command(&entry);
                if json {
                    let mut out = entry_output(&entry);
                    out["resume_cmd"] = json!(cmd);
                    println!("{}", out);
                } else if let Some(cmd) = cmd {
                    println!("{cmd}");
                } else {
                    anyhow::bail!("No resume command available for source '{}'", entry.source);
                }
            } else {
                anyhow::bail!("No session found");
            }
            Ok(())
        }
        Command::SyncOpencode { opencode_db } => {
            let path = opencode_db.unwrap_or_else(default_opencode_db_path);
            let inserted = sync_opencode_db(&conn, &path)?;
            println!("  [opencode] +{inserted} rows");
            Ok(())
        }
        Command::Sync {
            install_service,
            uninstall_service,
            interval,
        } => {
            if install_service {
                install_managed_service(&SYNC_SERVICE, interval)
            } else if uninstall_service {
                uninstall_managed_service(&SYNC_SERVICE)
            } else {
                sync_basic(&conn, &db_path)
            }
        }
        Command::Watch { interval } => watch_loop(&db_path, interval),
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
                watch_loop(&db_path, interval)
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
            limit,
            incognito,
            json,
            install_service,
            uninstall_service,
            interval,
        } => {
            if install_service {
                return install_managed_service(&PUSH_SERVICE, interval);
            }
            if uninstall_service {
                return uninstall_managed_service(&PUSH_SERVICE);
            }
            let auth = cloud::load_auth()?
                .context("not authenticated — run `ai-hist login` or `ai-hist admin-mint` first")?;
            let machine = MachineIdentity {
                id: cloud::machine_id()?,
                hostname: std::env::var("HOSTNAME").ok().filter(|s| !s.is_empty()),
                os: Some(std::env::consts::OS.to_string()),
                cli_version: Some(env!("CARGO_PKG_VERSION").to_string()),
                ..Default::default()
            };
            let cursor = cloud::load_cursor()?;
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
                    })
                );
            } else if report.sent == 0 {
                println!("Nothing new to push.");
            } else {
                println!(
                    "Pushed {} record(s), {} accepted (cursor → history #{}, trajectory rowid {}).",
                    report.sent,
                    report.accepted,
                    report.cursor.history_id,
                    report.cursor.trajectory_rowid
                );
            }
            Ok(())
        }
        Command::Pair { action } => match action {
            PairAction::Check {
                file,
                task,
                tool,
                target,
                recent_prompt,
                project_id,
                limit,
                json,
            } => {
                let auth = cloud::load_auth()?.context(
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
    let resume = resume_command(&entry);
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

fn print_stats(conn: &Connection, tag: Option<&str>, as_json: bool) -> Result<()> {
    let tag_norm = tag.map(normalize_tag_name);
    let where_sql = if tag_norm.is_some() {
        format!(" WHERE {}", tag_filter_clause("h"))
    } else {
        String::new()
    };
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
    let project_where = if tag_norm.is_some() {
        format!("WHERE project IS NOT NULL AND {}", tag_filter_clause("h"))
    } else {
        "WHERE project IS NOT NULL".to_string()
    };
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
            })
        );
        return Ok(());
    }
    println!("\nTotal entries: {total}");
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

fn sync_basic(conn: &Connection, db_path: &Path) -> Result<()> {
    let home = home_dir();
    let mut total_inserted = 0;
    let state_path = db_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(".sync-state.json");
    let mut state = load_sync_state(&state_path)?;
    total_inserted += sync_jsonl_incremental(
        conn,
        &mut state,
        "claude",
        &home.join(".claude/history.jsonl"),
        parse_claude_line,
    )?;
    sync_claude_session_metadata(conn, &mut state, &home.join(".claude/projects"))?;
    total_inserted += sync_codex(conn, &mut state, &home)?;
    total_inserted += sync_cursor(conn, &mut state, &home.join(".cursor/projects"))?;
    total_inserted += sync_grok(conn, &mut state, &home.join(".grok/sessions"))?;
    total_inserted += sync_trajectories(conn, &mut state)?;
    let opencode = std::env::var_os("OPENCODE_DB")
        .map(PathBuf::from)
        .unwrap_or_else(default_opencode_db_path);
    let open_inserted = sync_opencode_db(conn, &opencode)?;
    if opencode.exists() {
        println!("  [opencode] +{open_inserted} rows");
    } else {
        println!("  [opencode] not found: {} (skipped)", opencode.display());
    }
    total_inserted += open_inserted;
    total_inserted += sync_relaycast(conn, &mut state)?;
    let total: i64 = conn.query_row("SELECT COUNT(*) FROM history", [], |row| row.get(0))?;
    save_sync_state(&state_path, &state)?;
    println!("  [rust-sync] +{total_inserted} rows");
    println!("  Total: {total} entries");
    Ok(())
}

fn watch_loop(db_path: &Path, interval: u64) -> Result<()> {
    println!("Watching every {interval}s (Ctrl-C to stop)...");
    loop {
        match open_db(db_path).and_then(|conn| sync_basic(&conn, db_path)) {
            Ok(_) => {}
            Err(err) => eprintln!("Error: {err}"),
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
/// invokes it directly — never through a shell wrapper or `python3`, which is
/// what historically broke the launchd job.
fn service_binary() -> Result<PathBuf> {
    std::env::current_exe().context("could not resolve the ai-hist binary path for the service")
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn install_managed_service(spec: &ServiceSpec, interval: u64) -> Result<()> {
    let bin = service_binary()?;
    let bin = bin.to_string_lossy();
    if cfg!(target_os = "macos") {
        install_launchd_service(spec, &bin, interval)
    } else if cfg!(target_os = "linux") {
        install_cron_service(spec, &bin, interval)
    } else {
        anyhow::bail!(
            "Automatic {} service install is only supported on macOS and Linux. \
             Run `ai-hist watch` to keep syncing in the foreground instead.",
            spec.human
        )
    }
}

fn install_launchd_service(spec: &ServiceSpec, bin: &str, interval: u64) -> Result<()> {
    let plist_path = launchd_plist_path(spec);
    if let Some(dir) = plist_path.parent() {
        fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
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
        <string>{subcommand}</string>
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
        subcommand = spec.subcommand,
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
    println!(
        "  remove: ai-hist {} --uninstall-service",
        spec.subcommand
    );
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

fn install_cron_service(spec: &ServiceSpec, bin: &str, interval: u64) -> Result<()> {
    if interval != 60 {
        eprintln!(
            "Note: cron runs at 1-minute granularity; ignoring --interval={interval} and \
             scheduling every minute."
        );
    }
    let marker = cron_marker(spec);
    let line = format!(
        "* * * * * {bin} {} >> /tmp/{}.log 2>&1 {marker}",
        spec.subcommand, spec.log_stem
    );
    // Drop any previously managed line, then append the current one.
    let mut lines: Vec<String> = read_crontab()
        .lines()
        .filter(|l| !l.contains(&marker))
        .map(str::to_string)
        .collect();
    lines.push(line);
    write_crontab(&format!("{}\n", lines.join("\n")))?;

    println!("Installed cron {} job; running every minute.", spec.human);
    println!("  view:   crontab -l");
    println!(
        "  remove: ai-hist {} --uninstall-service",
        spec.subcommand
    );
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
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
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
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn append_history_search_filters(
    sql: &mut String,
    params: &mut Vec<String>,
    filter: &QueryFilter,
    alias: &str,
) {
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

fn load_sync_state(path: &Path) -> Result<Map<String, Value>> {
    if !path.exists() {
        return Ok(Map::new());
    }
    let value: Value = serde_json::from_str(&fs::read_to_string(path)?)?;
    Ok(value.as_object().cloned().unwrap_or_default())
}

fn save_sync_state(path: &Path, state: &Map<String, Value>) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_string_pretty(state)? + "\n")?;
    Ok(())
}

fn sync_jsonl_incremental(
    conn: &Connection,
    state: &mut Map<String, Value>,
    name: &str,
    path: &Path,
    parser: fn(&str) -> Result<Option<HistoryEntry>>,
) -> Result<usize> {
    if !path.exists() {
        println!("  [{name}] not found: {} (skipped)", path.display());
        return Ok(0);
    }
    let size = path.metadata()?.len();
    let offset = state.get(name).and_then(Value::as_u64).unwrap_or(0);
    if offset >= size {
        println!("  [{name}] up to date");
        return Ok(0);
    }
    println!("  [{name}] syncing {} new bytes...", size - offset);
    let file = fs::File::open(path)?;
    let mut reader = BufReader::new(file);
    reader.seek_relative(offset as i64)?;
    let mut inserted = 0;
    let mut errors = 0;
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        match parser(&line) {
            Ok(Some(entry)) => inserted += insert_history(conn, &entry)?,
            Ok(None) => {}
            Err(_) => errors += 1,
        }
    }
    state.insert(name.to_string(), json!(size));
    let suffix = if errors > 0 {
        format!(" ({errors} errors)")
    } else {
        String::new()
    };
    println!("  [{name}] +{inserted} rows{suffix}");
    Ok(inserted)
}

fn sync_codex(conn: &Connection, state: &mut Map<String, Value>, home: &Path) -> Result<usize> {
    let (cwds, branches) = build_codex_session_maps(state, home)?;
    let path = home.join(".codex/history.jsonl");
    if !path.exists() {
        println!("  [codex] not found: {} (skipped)", path.display());
        return Ok(0);
    }
    let size = path.metadata()?.len();
    let offset = state.get("codex").and_then(Value::as_u64).unwrap_or(0);
    let mut inserted = 0;
    let mut errors = 0;
    if offset < size {
        println!("  [codex] syncing {} new bytes...", size - offset);
        let file = fs::File::open(&path)?;
        let mut reader = BufReader::new(file);
        reader.seek_relative(offset as i64)?;
        for line in reader.lines() {
            let line = line?;
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
        state.insert("codex".to_string(), json!(size));
    }
    let backfilled = backfill_codex_metadata(conn, &cwds, &branches)?;
    if offset >= size && backfilled == 0 {
        println!("  [codex] up to date");
    } else {
        let mut parts = Vec::new();
        if inserted > 0 || offset < size {
            parts.push(format!("+{inserted} rows"));
        }
        if backfilled > 0 {
            parts.push(format!("backfilled {backfilled} project/branch values"));
        }
        if errors > 0 {
            parts.push(format!("{errors} errors"));
        }
        println!("  [codex] {}", parts.join(", "));
    }
    Ok(inserted)
}

fn build_codex_session_maps(
    state: &mut Map<String, Value>,
    home: &Path,
) -> Result<(HashMap<String, String>, HashMap<String, String>)> {
    let mut cwds = state
        .get("codex_session_cwds")
        .and_then(Value::as_object)
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect::<HashMap<_, _>>()
        })
        .unwrap_or_default();
    let mut branches = state
        .get("codex_session_branches")
        .and_then(Value::as_object)
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect::<HashMap<_, _>>()
        })
        .unwrap_or_default();
    let mut seen = state
        .get("codex_rollouts")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let mut scanned = 0;
    for root in [
        home.join(".codex/sessions"),
        home.join(".codex/archived_sessions"),
    ] {
        if !root.exists() {
            continue;
        }
        for rollout in collect_matching_files(&root, "rollout-", "jsonl")? {
            let key = rollout.to_string_lossy().to_string();
            let modified = file_stamp(&rollout)?;
            if seen.get(&key).and_then(Value::as_str) == Some(modified.as_str()) {
                continue;
            }
            seen.insert(key, json!(modified));
            scanned += 1;
            if let Some((session_id, cwd, branch)) = read_codex_session_meta(&rollout)? {
                cwds.insert(session_id.clone(), cwd);
                if let Some(branch) = branch {
                    branches.insert(session_id, branch);
                }
            }
        }
    }
    if scanned > 0 {
        println!(
            "  [codex] scanned {scanned} new rollout files; {} sessions mapped",
            cwds.len()
        );
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
    state.insert("codex_rollouts".to_string(), Value::Object(seen));
    Ok((cwds, branches))
}

fn read_codex_session_meta(path: &Path) -> Result<Option<(String, String, Option<String>)>> {
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
    let branch = payload
        .and_then(|p| p.get("git"))
        .and_then(Value::as_object)
        .and_then(|g| g.get("branch"))
        .and_then(Value::as_str)
        .map(str::to_string);
    Ok(Some((session_id.to_string(), cwd.to_string(), branch)))
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
    let mut session_state = state
        .get("claude_sessions")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let mut scanned = 0;
    let mut upserted = 0;
    for path in collect_matching_files(root, "", "jsonl")? {
        let key = path.to_string_lossy().to_string();
        let stamp = file_stamp(&path)?;
        if session_state.get(&key).and_then(Value::as_str) == Some(stamp.as_str())
            && claude_transcript_events_exist(conn, &path)?
        {
            continue;
        }
        scanned += 1;
        session_state.insert(key, json!(stamp));
        if let Some(meta) = scan_claude_session_file(&path)? {
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
            upserted += 1;
        }
    }
    state.insert("claude_sessions".to_string(), Value::Object(session_state));
    if scanned > 0 {
        println!("  [claude-sessions] scanned {scanned} files, {upserted} sessions updated");
    }
    Ok(())
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
    cwd: Option<String>,
    git_branch: Option<String>,
    first_ts: i64,
    last_ts: i64,
    last_assistant_text: Option<String>,
}

fn scan_claude_session_file(path: &Path) -> Result<Option<ClaudeSessionMeta>> {
    let text = fs::read_to_string(path).unwrap_or_default();
    let mut session_id = None;
    let mut cwd = None;
    let mut git_branch = None;
    let mut first_ts = None;
    let mut last_ts = None;
    let mut last_assistant_text = None;
    for line in text.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if session_id.is_none() {
            session_id = value
                .get("sessionId")
                .and_then(Value::as_str)
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
        if value.get("type").and_then(Value::as_str) == Some("assistant") {
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
        cwd,
        git_branch,
        first_ts: first,
        last_ts: last_ts.unwrap_or(first),
        last_assistant_text,
    }))
}

fn ingest_claude_transcript(conn: &Connection, path: &Path) -> Result<()> {
    let text = fs::read_to_string(path).unwrap_or_default();
    for (line_index, line) in text.lines().enumerate() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let Some(obj) = value.as_object() else {
            continue;
        };
        let session_id = match obj.get("sessionId").and_then(Value::as_str) {
            Some(s) if !s.is_empty() => s,
            _ => continue,
        };
        let cwd = obj.get("cwd").and_then(Value::as_str);
        let project = cwd;
        let git_branch = obj.get("gitBranch").and_then(Value::as_str);
        let ts_ms = obj
            .get("timestamp")
            .and_then(|v| v.as_str().and_then(parse_iso_ms).or_else(|| v.as_i64()))
            .unwrap_or(0);
        let parent_id = obj.get("parentUuid").and_then(Value::as_str);
        let message = obj.get("message").and_then(Value::as_object);
        let fallback_uid = format!(
            "{}:{}",
            path.file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("session"),
            line_index
        );
        let message_uuid = obj
            .get("uuid")
            .and_then(Value::as_str)
            .or_else(|| message.and_then(|m| m.get("id")).and_then(Value::as_str))
            .unwrap_or(&fallback_uid);
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
        if let Some(s) = content.as_str() {
            if !s.trim().is_empty() {
                let role = if message_role == "assistant" {
                    "assistant"
                } else {
                    "user"
                };
                insert_session_event(
                    conn,
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
                            conn.execute(
                                "UPDATE tool_calls SET is_error = ? WHERE source = 'claude' AND session_id = ? AND tool_use_id = ?",
                                params![if err { 1 } else { 0 }, session_id, tool_use_id],
                            )?;
                        }
                        if let Some(result) = find_tool_use_result(block) {
                            update_file_edit_from_tool_result(
                                conn,
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
    conn.execute(
        "INSERT INTO session_events \
         (source, session_id, project, cwd, git_branch, message_id, parent_id, ts_ms, role, kind, text, model, token_json, event_uid) \
         VALUES ('claude', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(source, session_id, event_uid) DO UPDATE SET \
         project=excluded.project, cwd=excluded.cwd, git_branch=excluded.git_branch, message_id=excluded.message_id, \
         parent_id=excluded.parent_id, ts_ms=excluded.ts_ms, role=excluded.role, kind=excluded.kind, text=excluded.text, \
         model=excluded.model, token_json=excluded.token_json",
        params![
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
    session_id: &str,
    message_id: &str,
    tool_use_id: &str,
    name: &str,
    target: Option<&str>,
    args_json: &str,
    is_error: Option<bool>,
    ts_ms: i64,
) -> Result<()> {
    conn.execute(
        "INSERT INTO tool_calls \
         (source, session_id, message_id, tool_use_id, name, target, args_json, is_error, ts_ms) \
         VALUES ('claude', ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(source, session_id, tool_use_id) DO UPDATE SET \
         message_id=excluded.message_id, name=excluded.name, target=excluded.target, args_json=excluded.args_json, \
         is_error=COALESCE(excluded.is_error, tool_calls.is_error), ts_ms=excluded.ts_ms",
        params![
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

#[allow(clippy::too_many_arguments)]
fn upsert_file_edit_from_call(
    conn: &Connection,
    session_id: &str,
    message_id: &str,
    tool_use_id: &str,
    file_path: &str,
    tool_name: &str,
    ts_ms: i64,
    git_branch: Option<&str>,
    cwd: Option<&str>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO file_edits \
         (source, session_id, message_id, tool_use_id, file_path, tool_name, ts_ms, git_branch, cwd) \
         VALUES ('claude', ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(source, session_id, tool_use_id) DO UPDATE SET \
         message_id=excluded.message_id, file_path=excluded.file_path, tool_name=excluded.tool_name, \
         ts_ms=excluded.ts_ms, git_branch=COALESCE(excluded.git_branch, file_edits.git_branch), cwd=COALESCE(excluded.cwd, file_edits.cwd)",
        params![
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
         WHERE source = 'claude' AND session_id = ? AND tool_use_id = ?",
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
         (session_id, source, cwd, git_branch, first_activity_ms, last_activity_ms, last_assistant_text, raw_path, parser_version) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1) \
         ON CONFLICT(session_id, source) DO UPDATE SET \
         cwd = COALESCE(excluded.cwd, sessions.cwd), \
         git_branch = COALESCE(excluded.git_branch, sessions.git_branch), \
         first_activity_ms = MIN(COALESCE(sessions.first_activity_ms, excluded.first_activity_ms), excluded.first_activity_ms), \
         last_activity_ms = MAX(COALESCE(sessions.last_activity_ms, excluded.last_activity_ms), excluded.last_activity_ms), \
         last_assistant_text = COALESCE(excluded.last_assistant_text, sessions.last_assistant_text), \
         raw_path = COALESCE(excluded.raw_path, sessions.raw_path), \
         parser_version = excluded.parser_version",
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
        let path = entry?.path();
        if path.is_dir() {
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

fn file_stamp(path: &Path) -> Result<String> {
    let metadata = path.metadata()?;
    Ok(format!(
        "{}:{}",
        metadata
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_nanos())
            .unwrap_or(0),
        metadata.len()
    ))
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
            let size = jsonl.metadata()?.len();
            let key = jsonl.to_string_lossy().to_string();
            let offset = cursor_state.get(&key).and_then(Value::as_u64).unwrap_or(0);
            if offset >= size {
                continue;
            }
            let ts_ms = jsonl
                .metadata()
                .and_then(|m| m.modified())
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            let file = fs::File::open(&jsonl)?;
            let mut reader = BufReader::new(file);
            reader.seek_relative(offset as i64)?;
            for line in reader.lines() {
                let line = line?;
                match parse_cursor_text(&line) {
                    Ok(Some(prompt)) => {
                        inserted += insert_history(
                            conn,
                            &HistoryEntry {
                                id: 0,
                                source: "cursor".into(),
                                session_id: Some(session_id.clone()),
                                project: Some(project_path.clone()),
                                prompt_hash: Some(prompt_hash(&prompt)),
                                prompt,
                                timestamp_ms: ts_ms,
                            },
                        )?;
                    }
                    Ok(None) => {}
                    Err(_) => errors += 1,
                }
            }
            cursor_state.insert(key, json!(size));
        }
    }
    state.insert("cursor".to_string(), Value::Object(cursor_state));
    if files_seen > 0 {
        let suffix = if errors > 0 {
            format!(" ({errors} errors)")
        } else {
            String::new()
        };
        println!("  [cursor] +{inserted} rows from {files_seen} files{suffix}");
    }
    Ok(inserted)
}

fn sorted_dirs(root: &Path) -> Result<Vec<PathBuf>> {
    let mut dirs = Vec::new();
    if root.exists() {
        for entry in fs::read_dir(root)? {
            let path = entry?.path();
            if path.is_dir() {
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
        println!("  [grok] not found: {} (skipped)", root.display());
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
                upsert_session(
                    conn,
                    &session.session_id,
                    "grok",
                    session.cwd.as_deref(),
                    session.git_branch.as_deref(),
                    session.first_ts,
                    session.last_ts,
                    session.last_assistant_text.as_deref(),
                    Some(&raw_path),
                )?;
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
        println!("  [grok] +{inserted} rows from {sessions} sessions{suffix}");
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
    path.metadata()
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64)
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
    println!("  [trajectory] {}", parts.join(", "));
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
    println!("  [relay] +{inserted} rows");
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
        file_stamp, git_commit_time_ms, git_stdout, ingest_claude_transcript, link_git_commit,
        parse_trajectory_file, paths_overlap, search_all, strip_url_credentials,
        sync_claude_session_metadata, xml_escape, SearchRole,
    };
    use ai_hist_core::{init_db, QueryFilter};
    use rusqlite::Connection;
    use serde_json::{json, Map, Value};
    use std::fs;

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
            "claude_sessions".to_string(),
            Value::Object(claude_sessions),
        );

        sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();

        let event_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM session_events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(event_count, 4);
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
}
