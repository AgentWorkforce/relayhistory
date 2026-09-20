//! Standalone Cloud collector. Reuses ai-hist capture/queue and the optional transport.
mod bridge;
mod collector;

use ai_hist::delivery::SessionIdentity;
use anyhow::{ensure, Context, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use fs2::FileExt;
use relayhistory_plugin::{cloud, destination, migration};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{self, IsTerminal, Write},
    path::{Path, PathBuf},
};

#[derive(Parser)]
#[command(
    name = "agent-relay-probe",
    version,
    about = "Connect coding sessions to Agent Relay Cloud"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}
#[derive(Subcommand)]
enum Commands {
    Cloud {
        #[command(subcommand)]
        command: CloudCommands,
    },
    #[command(hide = true)]
    Run {
        #[arg(long)]
        directory: PathBuf,
        #[arg(long)]
        startup_id: String,
    },
    Stop(Target),
    Status(TargetJson),
    /// Every workspace connected on this computer (desktop bridge).
    Installs(JsonOnly),
    /// Start the background collector for a connection that already exists.
    Start(TargetJson),
    /// Stop uploading without disconnecting. Capture continues.
    Pause(TargetJson),
    /// Resume uploading after a pause.
    Resume(TargetJson),
    Sessions {
        #[command(subcommand)]
        command: SessionCommands,
    },
    Sharing {
        #[command(subcommand)]
        command: SharingCommands,
    },
    /// Disconnect this workspace. Local history is kept.
    Disconnect(TargetJson),
}
#[derive(Subcommand)]
enum CloudCommands {
    Install(Install),
}
#[derive(Subcommand)]
enum SessionCommands {
    /// List captured sessions and whether they are shared.
    List(SessionList),
    /// Share the named sessions.
    Include(SessionSelection),
    /// Stop sharing the named sessions.
    Exclude(SessionSelection),
}
#[derive(Subcommand)]
enum SharingCommands {
    /// Change what this computer uploads.
    Set(SharingSet),
}
#[derive(Args, Clone)]
struct Target {
    #[arg(long, default_value = "https://agentrelay.com")]
    site_url: String,
    #[arg(long)]
    account: String,
    #[arg(long)]
    workspace: String,
}
#[derive(Args)]
struct JsonOnly {
    #[arg(long)]
    json: bool,
}
#[derive(Args)]
struct TargetJson {
    #[command(flatten)]
    target: Target,
    #[arg(long)]
    json: bool,
}
#[derive(Args)]
struct SessionList {
    #[command(flatten)]
    target: Target,
    #[arg(long)]
    json: bool,
    #[arg(long, default_value_t = 500)]
    limit: usize,
}
#[derive(Args)]
struct SessionSelection {
    #[command(flatten)]
    target: Target,
    #[arg(long)]
    json: bool,
    /// SOURCE:SESSION_ID, repeated once per session.
    #[arg(long = "session", required = true, value_parser = parse_identity)]
    sessions: Vec<SessionIdentity>,
}
#[derive(Args)]
struct SharingSet {
    #[command(flatten)]
    target: Target,
    #[arg(long, value_enum)]
    mode: SharingMode,
    #[arg(long)]
    json: bool,
}
/// What an install uploads. Only `all` exports the prompt-only rows that can
/// lack a session identity, so it is the one mode with a different delivery
/// selection; `selected` is `new` plus a per-cycle exclusion of sessions
/// discovered later.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum SharingMode {
    All,
    New,
    Selected,
}
impl SharingMode {
    fn include_existing(self) -> bool {
        self == SharingMode::All
    }
}
/// `SOURCE:SESSION_ID`. Identifiers come from the local database, so only their
/// shape is checked; they are always bound as SQL parameters, never formatted in.
fn parse_identity(value: &str) -> std::result::Result<SessionIdentity, String> {
    let (source, session_id) = value
        .split_once(':')
        .ok_or_else(|| "expected SOURCE:SESSION_ID".to_string())?;
    let usable = !source.is_empty()
        && source.len() <= 64
        && source
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
        && !session_id.is_empty()
        && session_id.len() <= 512
        && !session_id.chars().any(char::is_control);
    if !usable {
        return Err("expected SOURCE:SESSION_ID".into());
    }
    Ok(SessionIdentity {
        source: source.to_string(),
        session_id: session_id.to_string(),
    })
}
#[derive(Args)]
struct Install {
    #[arg(long, default_value = "https://agentrelay.com")]
    site_url: String,
    #[arg(long)]
    account: Option<String>,
    #[arg(long)]
    workspace: Option<String>,
    #[arg(long, conflicts_with_all = ["new_sessions_only", "selected_sessions_only"])]
    include_existing: bool,
    #[arg(long, conflicts_with = "selected_sessions_only")]
    new_sessions_only: bool,
    #[arg(long)]
    selected_sessions_only: bool,
    #[arg(long)]
    force_login: bool,
    #[arg(long)]
    foreground: bool,
    #[arg(long)]
    once: bool,
    #[arg(long)]
    acknowledge_uninspected_schedules: bool,
    /// NDJSON events for Relay Desktop. Never reads stdin, never runs in the
    /// foreground: the caller is a GUI, not a terminal.
    #[arg(long)]
    json: bool,
}
#[derive(Clone, Serialize, Deserialize)]
struct Config {
    version: u32,
    site_url: String,
    account_id: String,
    org_id: String,
    workspace_id: String,
    history_url: String,
    delivery_account: String,
    job_id: String,
    include_existing: bool,
    #[serde(default)]
    acknowledge_uninspected_schedules: bool,
    /// Absent in configurations written before the desktop bridge, where the
    /// binary sharing choice is the whole selection: read it through
    /// [`Config::sharing_mode`], which derives it from `include_existing`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sharing_mode: Option<SharingMode>,
}
impl Config {
    fn sharing_mode(&self) -> SharingMode {
        self.sharing_mode.unwrap_or(if self.include_existing {
            SharingMode::All
        } else {
            SharingMode::New
        })
    }
    /// The two fields are one setting. `include_existing` stays authoritative
    /// for the delivery selection, so it must never disagree with the mode.
    fn set_sharing_mode(&mut self, mode: SharingMode) {
        self.sharing_mode = Some(mode);
        self.include_existing = mode.include_existing();
    }
}
#[derive(Debug)]
struct UserError(&'static str);
impl std::fmt::Display for UserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for UserError {}
fn user_error(message: &'static str) -> anyhow::Error {
    UserError(message).into()
}
/// The only text a failure may print. Provider errors can contain bodies,
/// credentials or session content, so anything unclassified becomes one fixed
/// sentence. Shared with the collector, which records it in `cycle.json`.
fn safe_message(error: &anyhow::Error) -> &'static str {
    if let Some(safe) = error.downcast_ref::<UserError>() {
        safe.0
    } else if let Some(safe) = error.downcast_ref::<cloud::CloudAuthError>() {
        safe.0
    } else {
        "Probe could not finish. Check your connection and run setup again. Credentials and session content were not logged."
    }
}
fn main() {
    if let Err(error) = run(Cli::parse()) {
        eprintln!("{}", safe_message(&error));
        std::process::exit(1);
    }
}
fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Commands::Cloud {
            command: CloudCommands::Install(options),
        } => install(options),
        Commands::Run {
            directory,
            startup_id,
        } => collector::run_background(&directory, &startup_id),
        Commands::Stop(target) => collector::stop(&target.directory()?),
        Commands::Status(options) => bridge::status(&options.target.directory()?, options.json),
        Commands::Installs(options) => bridge::installs(options.json),
        Commands::Start(options) => bridge::start(&options.target.directory()?, options.json),
        Commands::Pause(options) => {
            bridge::set_paused(&options.target.directory()?, true, options.json)
        }
        Commands::Resume(options) => {
            bridge::set_paused(&options.target.directory()?, false, options.json)
        }
        Commands::Sessions {
            command: SessionCommands::List(options),
        } => bridge::list_sessions(&options.target.directory()?, options.limit, options.json),
        Commands::Sessions {
            command: SessionCommands::Include(options),
        } => bridge::select_sessions(
            &options.target.directory()?,
            &options.sessions,
            true,
            options.json,
        ),
        Commands::Sessions {
            command: SessionCommands::Exclude(options),
        } => bridge::select_sessions(
            &options.target.directory()?,
            &options.sessions,
            false,
            options.json,
        ),
        Commands::Sharing {
            command: SharingCommands::Set(options),
        } => bridge::set_sharing(&options.target.directory()?, options.mode, options.json),
        Commands::Disconnect(options) => {
            bridge::disconnect(&options.target.directory()?, options.json)
        }
    }
}
impl Target {
    fn directory(&self) -> Result<PathBuf> {
        let site = cloud::site_origin(&self.site_url)?;
        validate_id(&self.account)?;
        validate_id(&self.workspace)?;
        directory(&site, &self.account, &self.workspace)
    }
}
fn validate_id(value: &str) -> Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 128
            && value
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_'),
        "invalid identifier"
    );
    Ok(())
}
fn home() -> Result<PathBuf> {
    std::env::var_os("HOME")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .context("HOME unavailable")
}
fn directory(site: &str, account: &str, workspace: &str) -> Result<PathBuf> {
    let key = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&(site, account, workspace))?)
    );
    Ok(home()?.join(".agentworkforce/probe").join(key))
}
fn private_directory(path: &Path) -> Result<()> {
    fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}
fn save_json(path: &Path, value: &impl Serialize) -> Result<()> {
    private_directory(path.parent().context("missing parent")?)?;
    let mut temporary = tempfile::NamedTempFile::new_in(path.parent().unwrap())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        temporary
            .as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    serde_json::to_writer(&mut temporary, value)?;
    temporary.as_file().sync_all()?;
    temporary.persist(path).map_err(|e| e.error)?;
    Ok(())
}
fn read_config(directory: &Path) -> Result<Config> {
    let mut config: Config = serde_json::from_slice(&fs::read(directory.join("config.json"))?)?;
    // A stored mode wins over the older boolean, and normalizing here means no
    // caller has to reconcile the two.
    config.set_sharing_mode(config.sharing_mode());
    ensure!(
        config.version == 1 && cloud::site_origin(&config.site_url)? == config.site_url,
        "invalid config"
    );
    ensure!(
        cloud::history_origin(&config.history_url, &config.site_url)? == config.history_url,
        "invalid history origin"
    );
    validate_id(&config.account_id)?;
    validate_id(&config.workspace_id)?;
    ensure!(
        config.delivery_account
            == destination::account_id(&config.org_id, Some(&config.workspace_id)),
        "invalid account"
    );
    Ok(config)
}
fn lock(directory: &Path) -> Result<fs::File> {
    private_directory(directory)?;
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(directory.join("collector.lock"))?;
    file.try_lock_exclusive().map_err(|_| user_error("This workspace’s probe is already running. Use agent-relay-probe stop before reconnecting."))?;
    Ok(file)
}
fn choose_import() -> Result<Option<bool>> {
    if !io::stdin().is_terminal() {
        return Err(user_error("Use an interactive terminal, or explicitly pass --include-existing or --new-sessions-only."));
    }
    println!("Share coding session content with this Cloud workspace:");
    println!(
        "  1. Existing and future sessions\n  2. Only sessions started after setup\n  3. Cancel"
    );
    print!("Choose 1, 2 or 3: ");
    io::stdout().flush()?;
    let mut answer = String::new();
    io::stdin().read_line(&mut answer)?;
    Ok(match answer.trim() {
        "1" => Some(true),
        "2" => Some(false),
        _ => None,
    })
}
/// Install-time user messaging only: the same rule is enforced per dispatch
/// inside the receiver, where an actionable sentence would have nobody to read
/// it. Both must agree, so neither is allowed to drift into a softer check.
fn check_legacy_schedules(acknowledge_uninspected_schedules: bool) -> Result<()> {
    let status = migration::status();
    if status.state == "active" {
        return Err(user_error("An older managed history uploader is active. Stop it explicitly before enabling this probe."));
    }
    if status.state == "unknown" && !acknowledge_uninspected_schedules {
        return Err(user_error("Could not inspect older upload schedules. Check them, then retry with --acknowledge-uninspected-schedules."));
    }
    Ok(())
}
fn install(options: Install) -> Result<()> {
    let json = options.json;
    let site = cloud::site_origin(&options.site_url)?;
    let api_url = cloud::cloud_api_url(Some(&format!("{site}/cloud")))?;
    for id in [options.account.as_deref(), options.workspace.as_deref()]
        .into_iter()
        .flatten()
    {
        validate_id(id)?;
    }
    check_legacy_schedules(options.acknowledge_uninspected_schedules)?;
    if !json {
        println!("Connecting to Agent Relay Cloud…");
    }
    // Setup is always run by a person at a keyboard, but its stdin may be a
    // pipe (the composed local harness runs it that way), so the approval URL
    // is printed rather than gated on a terminal. Relay Desktop takes the same
    // URL as an NDJSON event and opens it in the browser itself.
    let mut announced = Ok(());
    let authenticated = cloud::cloud_bearer(
        &api_url,
        cloud::CloudBearerOptions {
            force_login: options.force_login,
            interactive: true,
            client_name: "Agent Relay Probe",
            announce: &mut |approval: &cloud::DeviceApproval| {
                if json {
                    announced = bridge::emit(serde_json::json!({
                        "event": "approval",
                        "verification_uri": approval.verification_uri,
                        "user_code": approval.user_code,
                    }));
                    return;
                }
                println!(
                    "Open this URL to authorize your computer:\n{}",
                    approval.verification_uri
                );
                if let Some(code) = &approval.user_code {
                    println!("Code: {code}");
                }
            },
        },
    )?;
    announced?;
    let who = cloud::whoami(&api_url, &authenticated)?;
    if options
        .account
        .as_ref()
        .is_some_and(|id| id != &who.user_id)
    {
        return Err(user_error("This is a different Cloud account. Retry with --force-login and the account from your dashboard."));
    }
    let workspace = options
        .workspace
        .clone()
        .or(who.workspace_id)
        .ok_or_else(|| user_error("Create a workspace in your dashboard before connecting."))?;
    validate_id(&who.user_id)?;
    validate_id(&workspace)?;
    let directory = directory(&site, &who.user_id, &workspace)?;
    let guard = lock(&directory)?;
    let existing = if directory.join("config.json").exists() {
        Some(read_config(&directory)?)
    } else {
        None
    };
    let requested = if options.include_existing {
        Some(SharingMode::All)
    } else if options.new_sessions_only {
        Some(SharingMode::New)
    } else if options.selected_sessions_only {
        Some(SharingMode::Selected)
    } else {
        None
    };
    if let Some(previous) = &existing {
        if requested.is_some_and(|choice| choice != previous.sharing_mode()) {
            return Err(user_error("A saved sharing choice exists. Changing selection requires explicitly replacing its delivery generation."));
        }
    }
    let mode = match existing.as_ref().map(|v| v.sharing_mode()).or(requested) {
        Some(choice) => choice,
        // A GUI caller has no terminal to answer on: the choice is a flag.
        None if json => {
            return Err(user_error("Choose what to share: pass --include-existing, --new-sessions-only or --selected-sessions-only."))
        }
        None => match choose_import()? {
            Some(true) => SharingMode::All,
            Some(false) => SharingMode::New,
            None => {
                println!("Setup cancelled. No sessions shared.");
                return Ok(());
            }
        },
    };
    let include_existing = mode.include_existing();
    // The workspace bridge, not this binary, selects the RelayHistory stage and
    // the scope it grants; the shared implementation checks both.
    let session = cloud::workspace_session(
        &api_url,
        &authenticated,
        &workspace,
        "sync",
        "Probe setup pending",
    )?;
    let history_url = session.base_url.clone();
    let org_id = session.org_id.clone().context("missing org")?;
    let account = destination::account_id(&org_id, Some(&workspace));
    if existing
        .as_ref()
        .is_some_and(|c| c.delivery_account != account || c.history_url != history_url)
    {
        return Err(user_error(
            "Saved history belongs to a different destination. It was not replaced.",
        ));
    }
    if json {
        bridge::emit(serde_json::json!({
            "event": "connected",
            "account_id": who.user_id.as_str(),
            "workspace_id": workspace.as_str(),
            "org_id": org_id.as_str(),
            "directory": directory.to_string_lossy(),
        }))?;
    }
    std::env::set_var("RELAYHISTORY_HOME", &directory);
    cloud::save_auth(&session)?;
    let db_path = directory.join("history.db");
    if !json {
        println!("Preparing local session capture…");
    }
    ensure!(
        ai_hist::sync_local_at(&db_path)?,
        "capture did not complete"
    );
    let conn = ai_hist::open_db(&db_path)?;
    let config = match existing {
        Some(mut config) => {
            config.acknowledge_uninspected_schedules = options.acknowledge_uninspected_schedules;
            save_json(&directory.join("config.json"), &config)?;
            let job = ai_hist::delivery::status(&conn, &config.job_id)?;
            ensure!(
                job.config.account_id == account,
                "invalid saved delivery account"
            );
            if job.state == "blocked" && options.force_login {
                ai_hist::delivery::retry_job(&conn, &config.job_id)?;
            }
            config
        }
        None => {
            let job_config = collector::delivery_config(&account, mode);
            // A generation outlives an interrupted setup: create_job commits
            // before config.json is written. Adopt that job instead of recording
            // a second baseline behind a generation nothing can reach.
            let adopted = ai_hist::delivery::list_jobs(&conn)?
                .into_iter()
                .find(|job| {
                    job.state != "cancelled"
                        && job.config.destination_id == job_config.destination_id
                        && job.config.instance_id == job_config.instance_id
                        && job.config.account_id == job_config.account_id
                });
            let job_id = match adopted {
                Some(job) => {
                    if job.config.selection != job_config.selection {
                        return Err(user_error("A delivery generation with a different sharing choice already exists. Cancel it explicitly before reconnecting."));
                    }
                    // Every field of a generation is immutable, not only the
                    // identity: an older mapping version or different limits
                    // would be rejected at dispatch, so refuse them up front.
                    if job.config != job_config {
                        return Err(user_error("A delivery generation with a different mapping version or limits already exists. Cancel it explicitly before reconnecting."));
                    }
                    job.job_id
                }
                None => {
                    // No generation to inherit a baseline from, so the exclusion
                    // table has to be brought in line with this choice first.
                    collector::record_baseline(&conn, include_existing)?;
                    ai_hist::delivery::create_job(&conn, &job_config, collector::now())?.job_id
                }
            };
            let mut config = Config {
                version: 1,
                site_url: site,
                account_id: who.user_id,
                org_id,
                workspace_id: workspace,
                history_url,
                delivery_account: account,
                job_id,
                include_existing,
                acknowledge_uninspected_schedules: options.acknowledge_uninspected_schedules,
                sharing_mode: None,
            };
            config.set_sharing_mode(mode);
            save_json(&directory.join("config.json"), &config)?;
            config
        }
    };
    drop(conn);
    collector::cycle_recorded(&directory, &config, !json)?;
    if options.once {
        if json {
            return bridge::emit(serde_json::json!({"event":"ready","running":false}));
        }
        println!("One capture/delivery cycle completed.");
        return Ok(());
    }
    drop(guard);
    // A GUI caller cannot own a foreground process: --json always detaches.
    if options.foreground && !json {
        collector::run_background(
            &directory,
            &format!("{}-{}", std::process::id(), collector::now()),
        )
    } else {
        collector::start_background(&directory, !json)?;
        if json {
            bridge::emit(serde_json::json!({"event":"ready","running":true}))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Parse a bridge command against one fixed target.
    fn parse(arguments: &[&str]) -> Commands {
        let mut all = vec!["probe"];
        all.extend_from_slice(arguments);
        all.extend_from_slice(&[
            "--site-url",
            "https://agentrelay.com",
            "--account",
            "usr_1",
            "--workspace",
            "ws_1",
        ]);
        Cli::try_parse_from(all)
            .expect("bridge command parses")
            .command
    }
    #[test]
    fn conflicting_sharing_choices_are_rejected() {
        for pair in [
            ["--include-existing", "--new-sessions-only"],
            ["--include-existing", "--selected-sessions-only"],
            ["--new-sessions-only", "--selected-sessions-only"],
        ] {
            assert!(
                Cli::try_parse_from(["probe", "cloud", "install", pair[0], pair[1]]).is_err(),
                "{pair:?} must conflict"
            );
        }
    }
    #[test]
    fn setup_takes_json_and_the_third_sharing_choice() {
        let Commands::Cloud {
            command: CloudCommands::Install(options),
        } = Cli::try_parse_from([
            "probe",
            "cloud",
            "install",
            "--json",
            "--selected-sessions-only",
        ])
        .unwrap()
        .command
        else {
            panic!("expected cloud install");
        };
        assert!(options.json);
        assert!(options.selected_sessions_only);
        assert!(!options.include_existing);
    }
    #[test]
    fn installs_takes_no_target() {
        let Commands::Installs(options) = Cli::try_parse_from(["probe", "installs", "--json"])
            .unwrap()
            .command
        else {
            panic!("expected installs");
        };
        assert!(options.json);
        assert!(Cli::try_parse_from(["probe", "installs", "--account", "usr_1"]).is_err());
    }
    #[test]
    fn the_target_commands_take_a_target_and_optional_json() {
        for name in ["status", "start", "pause", "resume", "disconnect"] {
            let options = match parse(&[name, "--json"]) {
                Commands::Status(options)
                | Commands::Start(options)
                | Commands::Pause(options)
                | Commands::Resume(options)
                | Commands::Disconnect(options) => options,
                _ => panic!("expected {name}"),
            };
            assert!(options.json, "{name} takes --json");
            assert_eq!(options.target.workspace, "ws_1");
            assert_eq!(options.target.account, "usr_1");
        }
        // --json stays optional: the human output is unchanged without it.
        let Commands::Status(options) = parse(&["status"]) else {
            panic!("expected status");
        };
        assert!(!options.json);
    }
    #[test]
    fn sessions_list_defaults_to_five_hundred_rows() {
        let Commands::Sessions {
            command: SessionCommands::List(options),
        } = parse(&["sessions", "list", "--json"])
        else {
            panic!("expected sessions list");
        };
        assert!(options.json);
        assert_eq!(options.limit, 500);
        let Commands::Sessions {
            command: SessionCommands::List(options),
        } = parse(&["sessions", "list", "--json", "--limit", "10"])
        else {
            panic!("expected sessions list");
        };
        assert_eq!(options.limit, 10);
    }
    #[test]
    fn sessions_include_and_exclude_take_repeated_identities() {
        let Commands::Sessions {
            command: SessionCommands::Include(options),
        } = parse(&[
            "sessions",
            "include",
            "--json",
            "--session",
            "claude:29284179-aa",
            "--session",
            "codex:b_1",
        ])
        else {
            panic!("expected sessions include");
        };
        assert_eq!(options.sessions.len(), 2);
        assert_eq!(options.sessions[0].source, "claude");
        assert_eq!(options.sessions[0].session_id, "29284179-aa");
        assert_eq!(options.sessions[1].session_id, "b_1");
        let Commands::Sessions {
            command: SessionCommands::Exclude(options),
        } = parse(&["sessions", "exclude", "--json", "--session", "claude:a"])
        else {
            panic!("expected sessions exclude");
        };
        assert_eq!(options.sessions.len(), 1);
        // A session is required, and it must name its source.
        let target = ["--account", "usr_1", "--workspace", "ws_1"];
        assert!(Cli::try_parse_from(
            ["probe", "sessions", "include"]
                .into_iter()
                .chain(target)
                .chain(["--json"])
        )
        .is_err());
        assert!(Cli::try_parse_from(
            ["probe", "sessions", "include"]
                .into_iter()
                .chain(target)
                .chain(["--session", "claude"])
        )
        .is_err());
    }
    #[test]
    fn sharing_set_takes_the_three_modes() {
        for (name, mode) in [
            ("all", SharingMode::All),
            ("new", SharingMode::New),
            ("selected", SharingMode::Selected),
        ] {
            let Commands::Sharing {
                command: SharingCommands::Set(options),
            } = parse(&["sharing", "set", "--mode", name, "--json"])
            else {
                panic!("expected sharing set");
            };
            assert_eq!(options.mode, mode);
            assert!(options.json);
        }
        assert!(Cli::try_parse_from([
            "probe",
            "sharing",
            "set",
            "--account",
            "usr_1",
            "--workspace",
            "ws_1",
            "--mode",
            "everything"
        ])
        .is_err());
    }
    #[test]
    fn session_identities_are_source_qualified() {
        assert_eq!(
            parse_identity("claude:a:b").unwrap(),
            SessionIdentity {
                source: "claude".into(),
                session_id: "a:b".into()
            }
        );
        for rejected in [
            "",
            "claude",
            ":abc",
            "claude:",
            "cl aude:abc",
            "claude:a\nb",
        ] {
            assert!(
                parse_identity(rejected).is_err(),
                "{rejected:?} is not an identity"
            );
        }
    }
    #[test]
    fn sharing_mode_defaults_to_the_saved_boolean() {
        let stored = serde_json::json!({
            "version": 1,
            "site_url": "https://agentrelay.com",
            "account_id": "usr_1",
            "org_id": "org_1",
            "workspace_id": "ws_1",
            "history_url": "https://history.agentrelay.com",
            "delivery_account": "org_1/ws_1",
            "job_id": "job",
            "include_existing": false
        });
        let config: Config = serde_json::from_value(stored.clone()).unwrap();
        assert_eq!(config.sharing_mode(), SharingMode::New);
        let mut shared = stored.clone();
        shared["include_existing"] = serde_json::json!(true);
        let config: Config = serde_json::from_value(shared).unwrap();
        assert_eq!(config.sharing_mode(), SharingMode::All);
        let mut selected = stored;
        selected["sharing_mode"] = serde_json::json!("selected");
        let mut config: Config = serde_json::from_value(selected).unwrap();
        assert_eq!(config.sharing_mode(), SharingMode::Selected);
        // The boolean and the mode are one setting, and stay in step on write.
        config.set_sharing_mode(SharingMode::All);
        let written = serde_json::to_value(&config).unwrap();
        assert_eq!(written["sharing_mode"], "all");
        assert_eq!(written["include_existing"], true);
        config.set_sharing_mode(SharingMode::Selected);
        let written = serde_json::to_value(&config).unwrap();
        assert_eq!(written["sharing_mode"], "selected");
        assert_eq!(written["include_existing"], false);
    }
    #[test]
    fn stop_requests_only_match_their_run_identity() {
        let directory = tempfile::tempdir().unwrap();
        save_json(
            &directory.path().join("stop.json"),
            &serde_json::json!({"startup_id":"previous"}),
        )
        .unwrap();
        assert!(!collector::stop_requested(directory.path(), "current"));
        assert!(collector::stop_requested(directory.path(), "previous"));
    }
    #[test]
    #[cfg(unix)]
    fn replacing_config_keeps_private_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.json");
        save_json(&path, &serde_json::json!({"version":1})).unwrap();
        save_json(&path, &serde_json::json!({"version":2})).unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(directory.path()).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&fs::read(&path).unwrap()).unwrap()
                ["version"],
            2
        );
    }
}
