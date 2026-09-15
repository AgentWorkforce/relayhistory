//! Optional command-line compatibility for explicit RelayHistory operations.
use crate::{cloud, convergence::MachineIdentity, replay};
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::{
    collections::HashSet,
    io::{IsTerminal, Write},
    path::PathBuf,
};
#[derive(Parser)]
struct Cli {
    #[arg(long, default_value_os_t = ai_hist_core::default_db_path())]
    db: PathBuf,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Authenticate to relayhistory-cloud (Agent Relay Loop).
    ///
    /// Defaults to Agent Relay Cloud auth, matching relayfile/workforce. The CLI reuses the
    /// official Agent Relay CLI's stored Cloud session or signs in with Cloud's device flow,
    /// then exchanges that bearer for a relayhistory session. Pass `--base-url` + `--token`
    /// only for manual/dev login.
    Login {
        /// Use Agent Relay Cloud auth. This is now the default and is kept for compatibility.
        #[arg(long)]
        cloud: bool,
        /// Least-privilege ceiling: `read` (Pair-only) or `sync` (Learn/push). Cloud authorizes
        /// the actual scope it grants. Cloud mode only.
        #[arg(long, default_value = "sync")]
        mode: String,
        /// Sign in through Cloud's workspace bridge, which selects the
        /// RelayHistory stage itself. Cloud mode only.
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
    /// Print the current access token alone (a secret); refresh it before expiry.
    Token {
        /// Select the cloud stage; defaults to RELAYHISTORY_BASE_URL/AI_HIST_BASE_URL, then prod.
        /// Required when multiple stages are configured and neither environment variable is set.
        #[arg(long)]
        base_url: Option<String>,
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

pub fn run() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Token { base_url } => {
            let token = cloud::access_token(base_url.as_deref())?;
            if std::io::stdout().is_terminal() {
                eprintln!("Warning: this access token is a secret and will remain in terminal scrollback.");
            }
            writeln!(std::io::stdout().lock(), "{token}")?;
            Ok(())
        }
        Command::Replay {
            session_id,
            base_url,
            limit,
            max_content,
            json,
            out,
        } => {
            let result = replay::replay(
                &session_id,
                base_url.as_deref(),
                limit,
                max_content,
                json,
                out.as_deref(),
            )?;
            if let Some(body) = result.transcript {
                std::io::stdout().lock().write_all(body.as_bytes())?;
            }
            Ok(())
        }
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
                // Cloud sign-in may need the device-approval URL shown; stdout
                // can be JSON for a caller, so the prompt goes to stderr.
                let mut announce = |approval: &cloud::DeviceApproval| {
                    eprintln!(
                        "Open this URL to authorize your computer:\n{}",
                        approval.verification_uri
                    );
                    if let Some(code) = &approval.user_code {
                        eprintln!("Code: {code}");
                    }
                };
                cloud::login_via_cloud(
                    base_url.as_deref(),
                    &mode,
                    workspace.as_deref(),
                    &label,
                    std::io::stdin().is_terminal(),
                    &mut announce,
                )?
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
            anyhow::ensure!(!install_service && !uninstall_service, "Legacy push service management moved to the optional SDK destination scheduler; migrate the installed job explicitly");
            let _ = interval;
            let conn = ai_hist_core::open_db(&cli.db)?;
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
    }
}
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
