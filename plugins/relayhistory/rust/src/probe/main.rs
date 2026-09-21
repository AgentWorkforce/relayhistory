//! Standalone Cloud collector. Reuses ai-hist capture/queue and the optional transport.
// Machine-readable commands must never mix progress prose into stdout.
macro_rules! humanln {
    ($($arg:tt)*) => { if !crate::bridge::json_mode() { println!($($arg)*); } };
}
mod bridge;
mod collector;
mod progress;

use anyhow::{ensure, Context, Result};
use clap::{Args, Parser, Subcommand};
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
    #[arg(long, global = true)]
    json: bool,
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
    Status(Target),
    Installs,
    Start(Target),
    Pause(Target),
    Resume(Target),
    Disconnect(Target),
    Sessions {
        #[command(subcommand)]
        command: bridge::SessionCommand,
    },
    Sharing {
        #[command(subcommand)]
        command: bridge::SharingCommand,
    },
}
#[derive(Subcommand)]
enum CloudCommands {
    Install(Install),
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
struct Install {
    #[arg(long, default_value = "https://agentrelay.com")]
    site_url: String,
    #[arg(long)]
    account: Option<String>,
    #[arg(long)]
    workspace: Option<String>,
    #[arg(long, conflicts_with_all = ["new_sessions_only", "selected_sessions_only"])]
    include_existing: bool,
    #[arg(long)]
    new_sessions_only: bool,
    #[arg(long, conflicts_with = "new_sessions_only")]
    selected_sessions_only: bool,
    #[arg(long)]
    force_login: bool,
    #[arg(long)]
    foreground: bool,
    #[arg(long)]
    once: bool,
    #[arg(long)]
    acknowledge_uninspected_schedules: bool,
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
    sharing_mode: Option<bridge::SharingMode>,
    #[serde(default)]
    acknowledge_uninspected_schedules: bool,
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
fn main() {
    let cli = Cli::parse();
    bridge::set_json(cli.json);
    if let Err(error) = run(cli) {
        // Provider errors can contain bodies, credentials or history; never print them.
        if let Some(safe) = error.downcast_ref::<UserError>() {
            eprintln!("{safe}");
        } else if let Some(safe) = error.downcast_ref::<cloud::CloudAuthError>() {
            eprintln!("{safe}");
        } else if let Some(safe) = collector::local_failure_message(&error) {
            eprintln!("{safe}");
        } else {
            eprintln!("Probe could not finish. Check your connection and run setup again. Credentials and session content were not logged.");
        }
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
        Commands::Status(target) if bridge::json_mode() => bridge::status(&target.directory()?),
        Commands::Status(target) => collector::print_status(&target.directory()?),
        Commands::Installs => bridge::installs(),
        Commands::Start(target) => bridge::start(&target.directory()?),
        Commands::Pause(target) => bridge::pause(&target.directory()?, true),
        Commands::Resume(target) => bridge::pause(&target.directory()?, false),
        Commands::Disconnect(target) => bridge::disconnect(&target.directory()?),
        Commands::Sessions { command } => bridge::sessions(command),
        Commands::Sharing { command } => bridge::sharing(command),
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
    let config: Config = serde_json::from_slice(&fs::read(directory.join("config.json"))?)?;
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
    humanln!("Share coding session content with this Cloud workspace:");
    humanln!(
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
fn validate_setup_mode(options: &Install, json: bool) -> Result<()> {
    ensure!(
        !json || (!options.foreground && !options.once),
        user_error("JSON setup requires background mode. Omit --once and --foreground.")
    );
    ensure!(
        !json
            || options.include_existing
            || options.new_sessions_only
            || options.selected_sessions_only,
        user_error("JSON setup requires an explicit sharing choice: --include-existing, --new-sessions-only, or --selected-sessions-only.")
    );
    Ok(())
}
fn install(options: Install) -> Result<()> {
    validate_setup_mode(&options, bridge::json_mode())?;
    let site = cloud::site_origin(&options.site_url)?;
    let api_url = cloud::cloud_api_url(Some(&format!("{site}/cloud")))?;
    for id in [options.account.as_deref(), options.workspace.as_deref()]
        .into_iter()
        .flatten()
    {
        validate_id(id)?;
    }
    check_legacy_schedules(options.acknowledge_uninspected_schedules)?;
    humanln!("Connecting to Agent Relay Cloud…");
    // Setup is always run by a person at a keyboard, but its stdin may be a
    // pipe (the composed local harness runs it that way), so the approval URL
    // is printed rather than gated on a terminal.
    let authenticated = cloud::cloud_bearer(
        &api_url,
        cloud::CloudBearerOptions {
            force_login: options.force_login,
            interactive: true,
            client_name: "Agent Relay Session Recorder",
            announce: &mut |approval: &cloud::DeviceApproval| {
                if bridge::json_mode() {
                    bridge::emit(
                        serde_json::json!({"event":"approval", "verification_uri":approval.verification_uri, "user_code":approval.user_code}),
                    );
                }
                humanln!(
                    "Open this URL to authorize your computer:\n{}",
                    approval.verification_uri
                );
                if let Some(code) = &approval.user_code {
                    humanln!("Code: {code}");
                }
            },
        },
    )?;
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
    let _control = bridge::control_lock(&directory)?;
    let guard = lock(&directory)?;
    bridge::recover(&directory)?;
    let existing = if directory.join("config.json").exists() {
        Some(read_config(&directory)?)
    } else {
        None
    };
    let requested = if options.include_existing {
        Some(bridge::SharingMode::All)
    } else if options.new_sessions_only {
        Some(bridge::SharingMode::New)
    } else if options.selected_sessions_only {
        Some(bridge::SharingMode::Selected)
    } else {
        None
    };
    if let Some(previous) = &existing {
        if requested.is_some_and(|choice| choice != bridge::mode(previous)) {
            return Err(user_error("A saved sharing choice exists. Changing selection requires explicitly replacing its delivery generation."));
        }
    }
    let sharing_mode = match existing.as_ref().map(bridge::mode).or(requested) {
        Some(choice) => choice,
        None => match choose_import()? {
            Some(true) => bridge::SharingMode::All,
            Some(false) => bridge::SharingMode::New,
            None => {
                humanln!("Setup cancelled. No sessions shared.");
                return Ok(());
            }
        },
    };
    let include_existing = sharing_mode == bridge::SharingMode::All;
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
    std::env::set_var("RELAYHISTORY_HOME", &directory);
    cloud::save_auth(&session)?;
    if bridge::json_mode() {
        bridge::emit(
            serde_json::json!({"event":"connected", "account_id":who.user_id,
            "workspace_id":workspace, "org_id":org_id, "directory":directory}),
        );
    }
    let db_path = directory.join("history.db");
    if sharing_mode != bridge::SharingMode::Selected {
        humanln!("Preparing local session capture…");
        collector::capture(&directory, &history_url)?;
    }
    let conn = relayhistory_plugin::delivery::open_db(&db_path)?;
    let config = match existing {
        Some(mut config) => {
            config.acknowledge_uninspected_schedules = options.acknowledge_uninspected_schedules;
            save_json(&directory.join("config.json"), &config)?;
            let job = relayhistory_plugin::delivery::status(&conn, &config.job_id)?;
            ensure!(
                job.config.account_id == account,
                "invalid saved delivery account"
            );
            if job.state == "blocked" && options.force_login {
                relayhistory_plugin::delivery::retry_job(&conn, &config.job_id)?;
            }
            config
        }
        None => {
            let job_config = relayhistory_plugin::delivery::DeliveryJobConfig {
                destination_id: "relayhistory".into(),
                instance_id: "teams-probe".into(),
                account_id: account.clone(),
                mapping_version: destination::MAPPING_VERSION.into(),
                selection: collector::selection(include_existing),
                limits: Default::default(),
            };
            // A generation outlives an interrupted setup: create_job commits
            // before config.json is written. Adopt that job instead of recording
            // a second baseline behind a generation nothing can reach.
            let adopted = relayhistory_plugin::delivery::list_jobs(&conn)?
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
                    if sharing_mode == bridge::SharingMode::Selected {
                        relayhistory_plugin::delivery::create_session_job(
                            &conn,
                            &job_config,
                            collector::now(),
                        )?
                        .job_id
                    } else {
                        collector::record_baseline(&conn, include_existing)?;
                        relayhistory_plugin::delivery::create_job(
                            &conn,
                            &job_config,
                            collector::now(),
                        )?
                        .job_id
                    }
                }
            };
            let config = Config {
                version: 1,
                site_url: site,
                account_id: who.user_id,
                org_id,
                workspace_id: workspace,
                history_url,
                delivery_account: account,
                job_id,
                include_existing,
                sharing_mode: Some(sharing_mode),
                acknowledge_uninspected_schedules: options.acknowledge_uninspected_schedules,
            };
            save_json(&directory.join("config.json"), &config)?;
            config
        }
    };
    drop(conn);
    bridge::enforce_selection(&directory, &config)?;
    // Desktop setup becomes ready after capture/credentials; the supervised
    // collector handles delivery and offline retries without blocking login.
    if options.once || !bridge::json_mode() {
        collector::finish_setup(&directory, &config, options.once)?;
    }
    if options.once {
        humanln!("One capture/delivery cycle completed.");
        return Ok(());
    }
    drop(guard);
    if options.foreground {
        collector::run_background(
            &directory,
            &format!("{}-{}", std::process::id(), collector::now()),
        )
    } else {
        collector::start_background(&directory)?;
        if bridge::json_mode() {
            bridge::emit(serde_json::json!({"event":"ready", "running":true}));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn conflicting_sharing_choices_are_rejected() {
        assert!(Cli::try_parse_from([
            "probe",
            "cloud",
            "install",
            "--include-existing",
            "--new-sessions-only"
        ])
        .is_err());
    }
    #[test]
    fn json_setup_reports_specific_safe_validation_errors() {
        for (extra, expected) in [
            (vec!["--once", "--include-existing"], "background mode"),
            (
                vec!["--foreground", "--selected-sessions-only"],
                "background mode",
            ),
            (vec![], "explicit sharing choice"),
        ] {
            let cli =
                Cli::try_parse_from([vec!["probe", "cloud", "install", "--json"], extra].concat())
                    .unwrap();
            let Commands::Cloud {
                command: CloudCommands::Install(options),
            } = cli.command
            else {
                panic!("install expected")
            };
            let error = validate_setup_mode(&options, cli.json).unwrap_err();
            assert!(error
                .downcast_ref::<UserError>()
                .unwrap()
                .to_string()
                .contains(expected));
        }
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
