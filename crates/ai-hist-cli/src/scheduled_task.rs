//! Windows Task Scheduler backend for `--install-service` / `--uninstall-service`.
//!
//! A Task Scheduler action cannot set environment variables or redirect output,
//! and `ai-hist.exe` is a console program, so the task runs a small generated
//! `.cmd` wrapper through a headless console host:
//!
//! `conhost.exe --headless "<data dir>\ai-hist\services\<log_stem>.cmd"`
//!
//! The wrapper sets the captured provider environment (as the launchd plist's
//! `EnvironmentVariables` and the cron line's `NAME=value` prefix do), runs
//! `ai-hist <subcommand> <args>`, and appends output to `%TEMP%\<log_stem>.log`,
//! like the cron and launchd services' `/tmp/<log_stem>.log`. `--headless` keeps
//! a console window from flashing up on every run.
//!
//! The pure pieces (schedule, quoting, wrapper and task XML) are compiled and
//! tested on every platform; only [`install`] and [`uninstall`] call
//! `schtasks.exe`, and the CLI dispatches to them on Windows only.

use super::{
    scheduler_environment_value, service_command_args, service_provider_environment, xml_escape,
    ServiceSpec,
};
use anyhow::{Context, Result};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

/// Task Scheduler folder holding every ai-hist managed task.
const TASK_FOLDER: &str = r"\RelayHistory";

/// A run that is still going after this long is stopped, so a hung sync cannot
/// hold `IgnoreNew` off every later run forever.
const EXECUTION_TIME_LIMIT: &str = "PT6H";

/// Variables passed through to the task besides the shared provider roots.
/// Task Scheduler starts from the user's registry environment, which lacks
/// shell-only variables: `HOME` (`home_dir()` prefers it over `USERPROFILE`)
/// and the provider roots the shared launchd/cron list does not carry yet.
const WINDOWS_EXTRA_ENV_VARS: [&str; 18] = [
    "HOME",
    "CODEBUDDY_CONFIG_DIR",
    "COPILOT_HOME",
    "GROK_BOT_USER_DATA",
    "HERMES_HOME",
    "HERMES_DATA_DIR_SUFFIX",
    "KIMI_CODE_HOME",
    "OMP_PROFILE",
    "OPENCLAW_HOME",
    "OPENCLAW_PROFILE",
    "OPENCLAW_STATE_DIR",
    "OPENCODE_STORAGE_DIR",
    "PI_CODING_AGENT_DIR",
    "PI_CODING_AGENT_SESSION_DIR",
    "PI_CONFIG_DIR",
    "PI_PROFILE",
    "QWEN_HOME",
    "QWEN_RUNTIME_DIR",
];

/// The task's full path, e.g. `\RelayHistory\ai-hist-sync-ann`. Task paths are
/// machine-wide, so each account registers its own task.
pub(crate) fn task_name(spec: &ServiceSpec) -> String {
    let account = std::env::var("USERNAME")
        .ok()
        .filter(|name| !name.trim().is_empty())
        .or_else(|| {
            let home = ai_hist::paths::home_dir();
            home.file_name()
                .map(|name| name.to_string_lossy().into_owned())
        })
        .unwrap_or_else(|| "user".to_string());
    task_name_for(spec, &account)
}

/// [`task_name`] for `account`, with characters a task name cannot hold
/// replaced by `_`.
pub(crate) fn task_name_for(spec: &ServiceSpec, account: &str) -> String {
    let account: String = account
        .chars()
        .map(|c| match c {
            '\\' | '/' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            c if c.is_control() => '_',
            c => c,
        })
        .collect();
    format!(r"{TASK_FOLDER}\{}-{account}", spec.log_stem)
}

/// Returns `(repetition minutes, human cadence, effective period in seconds)`.
///
/// Task Scheduler repeats at whole-minute granularity from a start time, so an
/// interval needs no divisor rounding as cron's `*/step` does. Like cron, the
/// interval rounds toward a *coarser* cadence (never runs more often than
/// requested), the floor is one minute and the ceiling once a day.
pub(crate) fn task_schedule(interval: u64) -> (u64, String, u64) {
    let minutes = interval.div_ceil(60).clamp(1, 1440);
    let cadence = match minutes {
        1 => "every minute".to_string(),
        1440 => "once a day".to_string(),
        m if m.is_multiple_of(60) => format!("every {} hour(s)", m / 60),
        m => format!("every {m} minutes"),
    };
    (minutes, cadence, minutes * 60)
}

/// ISO 8601 duration for a repetition interval of `minutes`.
pub(crate) fn repetition_interval(minutes: u64) -> String {
    if minutes.is_multiple_of(1440) {
        format!("P{}D", minutes / 1440)
    } else if minutes.is_multiple_of(60) {
        format!("PT{}H", minutes / 60)
    } else {
        format!("PT{minutes}M")
    }
}

fn reject_quote(what: &str, value: &str) -> Result<()> {
    if value.contains('"') {
        anyhow::bail!(
            "cannot install sync service: {what} contains a double quote, which the Task Scheduler wrapper cannot represent safely"
        );
    }
    Ok(())
}

/// Quote one word for a `.cmd` file: `%` is doubled so batch expansion leaves it
/// literal, and the double quotes keep `&`, `^`, `|`, `<`, `>` and spaces inert.
fn batch_quote(value: &str) -> String {
    format!("\"{}\"", value.replace('%', "%%"))
}

/// Quote one program argument: [`batch_quote`], plus doubling a trailing run of
/// backslashes so the C runtime's argument parser does not read `\"` as an
/// escaped quote.
fn batch_quote_arg(value: &str) -> String {
    let trailing = value.len() - value.trim_end_matches('\\').len();
    batch_quote(&format!("{value}{}", "\\".repeat(trailing)))
}

/// The environment the task needs, read from this process: the provider roots
/// the launchd and cron services capture, plus [`WINDOWS_EXTRA_ENV_VARS`].
/// Values are passed through verbatim: each provider resolves its own
/// relative, `~`, `$VAR` and unset-marker forms, and the wrapper changes to
/// the install directory (see [`wrapper_script`]) so they resolve as they did
/// there.
fn task_environment(spec: &ServiceSpec) -> Result<Vec<(String, String)>> {
    let mut environment: Vec<(String, String)> = service_provider_environment(spec)?
        .into_iter()
        .map(|(name, value)| (name.to_string(), value))
        .collect();
    let capture = |environment: &mut Vec<(String, String)>, name: &str| -> Result<()> {
        if environment.iter().any(|(captured, _)| captured == name) {
            return Ok(());
        }
        if let Some(value) = std::env::var_os(name) {
            if let Some(value) = scheduler_environment_value(name, value)? {
                environment.push((name.to_string(), value));
            }
        }
        Ok(())
    };
    for name in WINDOWS_EXTRA_ENV_VARS {
        capture(&mut environment, name)?;
    }
    // Hermes expands `$NAME`/`${NAME}` in HERMES_HOME when it runs, so the
    // variables it names must reach the task too.
    let hermes = environment
        .iter()
        .find(|(name, _)| name == "HERMES_HOME")
        .map(|(_, value)| referenced_variables(value));
    for name in hermes.unwrap_or_default() {
        capture(&mut environment, &name)?;
    }
    Ok(environment)
}

/// The variable names `value` references as `$NAME` or `${NAME}`, the forms
/// the Hermes resolver expands.
pub(crate) fn referenced_variables(value: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut rest = value;
    while let Some(at) = rest.find('$') {
        rest = &rest[at + 1..];
        let (name, after) = match rest.strip_prefix('{') {
            Some(braced) => match braced.find('}') {
                Some(end) => (&braced[..end], &braced[end + 1..]),
                None => break,
            },
            None => {
                let end = rest
                    .find(|c: char| !c.is_ascii_alphanumeric() && c != '_')
                    .unwrap_or(rest.len());
                (&rest[..end], &rest[end..])
            }
        };
        if !name.is_empty() && !names.iter().any(|seen| seen == name) {
            names.push(name.to_string());
        }
        rest = after;
    }
    names
}

/// Captured variables that hold a name or suffix rather than a path.
const NON_PATH_ENV_VARS: [&str; 4] = [
    "HERMES_DATA_DIR_SUFFIX",
    "OMP_PROFILE",
    "OPENCLAW_PROFILE",
    "PI_PROFILE",
];

/// Whether `value` of `name` might depend on the current directory. An
/// absolute path never does; otherwise only the forms a provider is known to
/// resolve elsewhere are exempt, since others (Codex, Claude) take `~` and
/// `$VAR` literally:
/// - names that are not paths ([`NON_PATH_ENV_VARS`]);
/// - `PI_CONFIG_DIR`, which OMP always joins to home;
/// - OpenClaw's values, trimmed: `null`/`undefined` (unset) and `~`, `~/`,
///   `~\` (home);
/// - `~` and `~/` in the Pi/OMP agent and session directories (home).
pub(crate) fn relative_path_value(name: &str, value: &str) -> bool {
    let home_tilde = |v: &str| v == "~" || v.starts_with("~/");
    let exempt = NON_PATH_ENV_VARS.contains(&name)
        || name == "PI_CONFIG_DIR"
        || (name.starts_with("OPENCLAW_") && {
            let v = value.trim();
            matches!(v, "null" | "undefined") || home_tilde(v) || v.starts_with("~\\")
        })
        || (name.starts_with("PI_CODING_AGENT_") && home_tilde(value));
    !exempt && !Path::new(value).is_absolute()
}

/// The `.cmd` wrapper the task runs: set the environment, change to `cwd` (the
/// install directory; `None` for a UNC one, which cmd.exe cannot enter), run
/// ai-hist, append its output to `log`. The `cd` is best effort: a since-removed
/// install directory is logged and the sync still runs. CRLF line endings,
/// UTF-8 (switched to with `chcp` before any non-ASCII line is read).
pub(crate) fn wrapper_script(
    spec: &ServiceSpec,
    bin: &str,
    args: &[String],
    environment: &[(impl AsRef<str>, String)],
    cwd: Option<&str>,
    log: &str,
) -> Result<String> {
    reject_quote("the ai-hist binary path", bin)?;
    reject_quote("the service log path", log)?;
    let mut lines = vec![
        "@echo off".to_string(),
        format!(
            "rem Managed by `ai-hist {0} --install-service`; remove with `ai-hist {0} --uninstall-service`.",
            spec.subcommand
        ),
        "chcp 65001 >nul".to_string(),
        "setlocal DisableDelayedExpansion".to_string(),
    ];
    for (name, value) in environment {
        let name = name.as_ref();
        reject_quote(name, value)?;
        lines.push(format!("set {}", batch_quote(&format!("{name}={value}"))));
    }
    if let Some(cwd) = cwd {
        reject_quote("the install directory", cwd)?;
        lines.push(format!(
            "cd /d {} >> {} 2>&1",
            batch_quote(cwd),
            batch_quote(log)
        ));
    }
    let mut command = vec![batch_quote(bin)];
    for arg in service_command_args(spec, args) {
        reject_quote("a service argument", &arg)?;
        command.push(batch_quote_arg(&arg));
    }
    lines.push(format!(
        "{} >> {} 2>&1",
        command.join(" "),
        batch_quote(log)
    ));
    Ok(format!("{}\r\n", lines.join("\r\n")))
}

/// `(program, arguments)` of the task's action: the wrapper through a headless
/// console host, so no console window appears on each run.
///
/// conhost re-quotes the command line it is given argument by argument, so
/// `cmd.exe /s /c ""<wrapper>""` does not survive it, and `cmd.exe /c
/// "<wrapper>"` breaks on a path holding `&`, `(` or `^`. Naming the wrapper
/// itself lets `CreateProcess` start it through `cmd.exe` with the path quoted
/// safely. The path is joined with `\` explicitly so the result is the same
/// when tested off Windows.
pub(crate) fn task_action(system_root: &Path, wrapper: &Path) -> (String, String) {
    (
        format!(r"{}\System32\conhost.exe", system_root.display()),
        format!(r#"--headless "{}""#, wrapper.display()),
    )
}

/// Task definition: repeat every `minutes` from `start_boundary` (local time),
/// run only while the user is logged on (no stored password), hidden, on
/// battery too, catch up a missed run, never overlap a still-running one. No
/// `WorkingDirectory`: Task Scheduler refuses to start an action whose working
/// directory has been removed, so the wrapper changes directory itself.
pub(crate) fn task_xml(
    spec: &ServiceSpec,
    start_boundary: &str,
    minutes: u64,
    action: &(String, String),
) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>Runs `ai-hist {subcommand}` in the background. Managed by `ai-hist {subcommand} --install-service`; remove with `ai-hist {subcommand} --uninstall-service`.</Description>
  </RegistrationInfo>
  <Triggers>
    <TimeTrigger>
      <Repetition>
        <Interval>{interval}</Interval>
        <StopAtDurationEnd>false</StopAtDurationEnd>
      </Repetition>
      <StartBoundary>{start_boundary}</StartBoundary>
      <Enabled>true</Enabled>
    </TimeTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>
    <IdleSettings>
      <StopOnIdleEnd>false</StopOnIdleEnd>
      <RestartOnIdle>false</RestartOnIdle>
    </IdleSettings>
    <AllowStartOnDemand>true</AllowStartOnDemand>
    <Enabled>true</Enabled>
    <Hidden>true</Hidden>
    <RunOnlyIfIdle>false</RunOnlyIfIdle>
    <WakeToRun>false</WakeToRun>
    <ExecutionTimeLimit>{time_limit}</ExecutionTimeLimit>
    <Priority>7</Priority>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>{command}</Command>
      <Arguments>{arguments}</Arguments>
    </Exec>
  </Actions>
</Task>
"#,
        subcommand = spec.subcommand,
        interval = repetition_interval(minutes),
        time_limit = EXECUTION_TIME_LIMIT,
        command = xml_escape(&action.0),
        arguments = xml_escape(&action.1),
    )
}

/// UTF-16LE with a byte-order mark, the encoding `schtasks /XML` reads.
pub(crate) fn utf16le_with_bom(text: &str) -> Vec<u8> {
    let mut bytes = vec![0xFF, 0xFE];
    for unit in text.encode_utf16() {
        bytes.extend(unit.to_le_bytes());
    }
    bytes
}

/// The wrapper lives in ai-hist's data directory, beside the default database,
/// not under `%LOCALAPPDATA%`: a packaged (MSIX) app redirects its children's
/// AppData writes into a private package folder Task Scheduler cannot see, so
/// a wrapper written there by `--install-service` from such an app never runs.
fn wrapper_path(spec: &ServiceSpec) -> PathBuf {
    let data = std::env::var_os("XDG_DATA_HOME")
        .filter(|dir| !dir.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| ai_hist::paths::home_dir().join(".local").join("share"));
    // The task's action names this path, so a relative XDG_DATA_HOME is
    // anchored here rather than wherever a scheduled run would resolve it.
    let data = std::path::absolute(&data).unwrap_or(data);
    data.join("ai-hist")
        .join("services")
        .join(format!("{}.cmd", spec.log_stem))
}

fn schtasks(args: &[&str]) -> Result<Output> {
    Command::new("schtasks.exe")
        .args(args)
        .output()
        .context("running schtasks.exe")
}

fn schtasks_checked(args: &[&str]) -> Result<()> {
    let output = schtasks(args)?;
    if !output.status.success() {
        anyhow::bail!(
            "schtasks {} failed: {}",
            args.first().copied().unwrap_or_default(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

pub(crate) fn install(spec: &ServiceSpec, bin: &str, interval: u64, args: &[String]) -> Result<()> {
    let (minutes, cadence, effective) = task_schedule(interval);
    if effective != interval {
        eprintln!(
            "Note: Task Scheduler runs at 1-minute granularity; --interval={interval}s scheduled as {cadence}."
        );
    }
    let cwd = std::env::current_dir().context("reading the current directory")?;
    let cwd = cwd.display().to_string();
    let environment = task_environment(spec)?;
    // cmd.exe cannot enter a UNC directory, so the wrapper has nowhere to
    // resolve relative provider roots from.
    let unc = cwd.starts_with(r"\\");
    if unc {
        if let Some((name, _)) = environment
            .iter()
            .find(|(name, value)| relative_path_value(name, value))
        {
            anyhow::bail!(
                "cannot install sync service from the network path {cwd}: {name} is relative, and a scheduled task cannot resolve it there. Install from a local directory or make {name} absolute."
            );
        }
    }
    let cwd = (!unc).then_some(cwd.as_str());
    let log = std::env::temp_dir().join(format!("{}.log", spec.log_stem));
    let wrapper = wrapper_path(spec);
    // The action names the wrapper on a command line that cmd.exe expands, so
    // a `%NAME%` in its path would open some other file. (Paths inside the
    // wrapper are safe: batch_quote doubles `%`.)
    if wrapper.display().to_string().contains('%') {
        anyhow::bail!(
            "cannot install sync service: the wrapper path {} contains %, which Task Scheduler would expand. Set XDG_DATA_HOME to a directory without % in its path.",
            wrapper.display()
        );
    }
    let script = wrapper_script(
        spec,
        bin,
        args,
        &environment,
        cwd,
        &log.display().to_string(),
    )?;
    if let Some(dir) = wrapper.parent() {
        fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    // A failed reinstall must leave the running task's wrapper as it was.
    // A wrapper that exists but cannot be read stops the install before
    // anything changes, so rollback never deletes a live task's wrapper.
    let previous = match fs::read(&wrapper) {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => {
            return Err(error).with_context(|| format!("reading {}", wrapper.display()));
        }
    };
    fs::write(&wrapper, script).with_context(|| format!("writing {}", wrapper.display()))?;
    if let Err(error) = register(spec, minutes, &wrapper) {
        let restored = match previous {
            Some(bytes) => fs::write(&wrapper, bytes),
            None => fs::remove_file(&wrapper),
        };
        if let Err(rollback) = restored {
            return Err(error.context(format!(
                "registering the task failed, and restoring {} failed too ({rollback})",
                wrapper.display()
            )));
        }
        return Err(error);
    }
    let name = task_name(spec);
    // Start the first run now, as launchd's RunAtLoad does.
    if schtasks_checked(&["/Run", "/TN", &name]).is_err() {
        eprintln!("Note: the first run starts at the next scheduled time.");
    }

    println!(
        "Installed Task Scheduler {} task ({name}); running {cadence}.",
        spec.human
    );
    println!("  view:   schtasks /Query /TN \"{name}\" /V /FO LIST");
    println!("  log:    {}", log.display());
    println!("  remove: ai-hist {} --uninstall-service", spec.subcommand);
    Ok(())
}

/// Register (or replace) the task that runs `wrapper` every `minutes`.
fn register(spec: &ServiceSpec, minutes: u64, wrapper: &Path) -> Result<()> {
    let system_root = std::env::var_os("SystemRoot")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(r"C:\Windows"));
    let action = task_action(&system_root, wrapper);
    let start = chrono::Local::now().format("%Y-%m-%dT%H:%M:%S").to_string();
    let xml = task_xml(spec, &start, minutes, &action);
    let definition = tempfile::Builder::new()
        .prefix("ai-hist-task-")
        .suffix(".xml")
        .tempfile()
        .context("creating the task definition file")?;
    fs::write(definition.path(), utf16le_with_bom(&xml))
        .with_context(|| format!("writing {}", definition.path().display()))?;
    // schtasks cannot open a file another handle holds for writing (sharing
    // violation), so close ours and keep only the self-deleting path.
    let definition = definition.into_temp_path();

    let name = task_name(spec);
    let xml_path = definition.display().to_string();
    schtasks_checked(&["/Create", "/TN", &name, "/XML", &xml_path, "/F"])
}

/// Remove the task and its wrapper. A task that is not registered is not an
/// error, so uninstalling twice succeeds.
pub(crate) fn uninstall(spec: &ServiceSpec) -> Result<()> {
    let name = task_name(spec);
    let registered = schtasks(&["/Query", "/TN", &name])?.status.success();
    // `/Query` fails the same way for a missing task and an unreachable
    // scheduler (its message is localized), so a failed lookup counts as
    // "not installed" only while Task Scheduler itself still answers.
    if !registered {
        schtasks_checked(&["/Query", "/FO", "CSV", "/NH"])
            .context("cannot tell whether the sync task is registered; leaving it in place")?;
    }
    if registered {
        schtasks_checked(&["/Delete", "/TN", &name, "/F"])?;
    }
    let wrapper = wrapper_path(spec);
    if wrapper.exists() {
        fs::remove_file(&wrapper).with_context(|| format!("removing {}", wrapper.display()))?;
    }
    if registered {
        println!("Removed Task Scheduler {} task ({name}).", spec.human);
    } else {
        println!("No Task Scheduler {} task installed.", spec.human);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SYNC_SERVICE;

    #[test]
    fn task_name_is_per_account_in_the_relayhistory_folder() {
        assert_eq!(
            task_name_for(&SYNC_SERVICE, "Ann Lee"),
            r"\RelayHistory\ai-hist-sync-Ann Lee"
        );
        assert_eq!(
            task_name_for(&SYNC_SERVICE, r"a\b:c*d|e"),
            r"\RelayHistory\ai-hist-sync-a_b_c_d_e"
        );
        assert!(task_name(&SYNC_SERVICE).starts_with(r"\RelayHistory\ai-hist-sync-"));
    }

    #[test]
    fn task_schedule_rounds_up_to_whole_minutes_within_a_day() {
        assert_eq!(task_schedule(60), (1, "every minute".to_string(), 60));
        assert_eq!(task_schedule(0).0, 1);
        assert_eq!(task_schedule(30).0, 1);
        // Never run more often than requested.
        assert_eq!(task_schedule(90), (2, "every 2 minutes".to_string(), 120));
        // No divisor rounding: Task Scheduler repeats uniformly at any minute step.
        assert_eq!(task_schedule(420), (7, "every 7 minutes".to_string(), 420));
        assert_eq!(task_schedule(5400).1, "every 90 minutes");
        assert_eq!(
            task_schedule(7200),
            (120, "every 2 hour(s)".to_string(), 7200)
        );
        // A day or longer runs once a day, as cron does.
        assert_eq!(
            task_schedule(86_400),
            (1440, "once a day".to_string(), 86_400)
        );
        assert_eq!(task_schedule(90_000).0, 1440);
        assert_eq!(task_schedule(u64::MAX).0, 1440);
    }

    #[test]
    fn repetition_interval_is_an_iso_duration() {
        assert_eq!(repetition_interval(1), "PT1M");
        assert_eq!(repetition_interval(90), "PT90M");
        assert_eq!(repetition_interval(120), "PT2H");
        assert_eq!(repetition_interval(1440), "P1D");
    }

    #[test]
    fn batch_quoting_keeps_metacharacters_literal() {
        assert_eq!(batch_quote(r"C:\a b\x"), r#""C:\a b\x""#);
        assert_eq!(batch_quote("100% & ^|<>"), r#""100%% & ^|<>""#);
        // A trailing backslash must not escape the closing quote for the CRT.
        assert_eq!(batch_quote_arg(r"C:\dir\"), r#""C:\dir\\""#);
        assert_eq!(batch_quote_arg("--local"), r#""--local""#);
    }

    #[test]
    fn wrapper_sets_environment_runs_ai_hist_and_appends_to_the_log() {
        let environment = [
            ("CLAUDE_CONFIG_DIR", r"D:\Claude & tools".to_string()),
            ("CODEX_HOME", r"D:\codex's 100% archive".to_string()),
        ];
        let script = wrapper_script(
            &SYNC_SERVICE,
            r"C:\Program Files\ai-hist\ai-hist.exe",
            &["--source-connector".to_string(), "devin".to_string()],
            &environment,
            Some(r"C:\work & play"),
            r"C:\Users\Ann Lee\AppData\Local\Temp\ai-hist-sync.log",
        )
        .unwrap();
        let lines: Vec<&str> = script.split("\r\n").collect();
        assert_eq!(lines[0], "@echo off");
        assert!(lines[1].starts_with("rem "));
        assert_eq!(lines[2], "chcp 65001 >nul");
        assert_eq!(lines[3], "setlocal DisableDelayedExpansion");
        assert_eq!(lines[4], r#"set "CLAUDE_CONFIG_DIR=D:\Claude & tools""#);
        assert_eq!(lines[5], r#"set "CODEX_HOME=D:\codex's 100%% archive""#);
        // Best effort: a removed install directory is logged, the sync still runs.
        assert_eq!(
            lines[6],
            r#"cd /d "C:\work & play" >> "C:\Users\Ann Lee\AppData\Local\Temp\ai-hist-sync.log" 2>&1"#
        );
        assert_eq!(
            lines[7],
            r#""C:\Program Files\ai-hist\ai-hist.exe" "sync" "--source-connector" "devin" >> "C:\Users\Ann Lee\AppData\Local\Temp\ai-hist-sync.log" 2>&1"#
        );
        assert_eq!(lines[8], "");
        assert!(
            !script.replace("\r\n", "").contains('\n'),
            "every line ends in CRLF"
        );
    }

    #[test]
    fn hermes_references_are_found_in_both_forms() {
        assert_eq!(
            referenced_variables("$ROOT/hermes/${PROFILE}/$ROOT"),
            ["ROOT", "PROFILE"]
        );
        // An unclosed `${` and a bare `$` name nothing, as Hermes leaves them.
        assert!(referenced_variables("C:/hermes/${OPEN").is_empty());
        assert!(referenced_variables("cost$/x").is_empty());
    }

    #[test]
    fn wrapper_rejects_double_quotes() {
        let error = wrapper_script(
            &SYNC_SERVICE,
            r"C:\ai-hist.exe",
            &[],
            &[("CODEX_HOME", r#"D:\a"b"#.to_string())],
            None,
            r"C:\log",
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("CODEX_HOME contains a double quote"));
    }

    #[test]
    fn only_plain_relative_paths_need_an_install_directory() {
        assert!(relative_path_value("OPENCLAW_STATE_DIR", "state"));
        assert!(relative_path_value("CODEX_HOME", r".\codex"));
        // Some providers take `~` and `$VAR` literally, so they may be relative.
        assert!(relative_path_value("CODEX_HOME", "~/codex"));
        assert!(relative_path_value("HERMES_HOME", "$HOME/hermes"));
        // `null` is OpenClaw's unset marker only; elsewhere it is a path.
        assert!(!relative_path_value("OPENCLAW_STATE_DIR", " null "));
        assert!(!relative_path_value("OPENCLAW_HOME", "undefined"));
        assert!(relative_path_value("CODEX_HOME", "null"));
        // Forms a provider resolves against home rather than the current one.
        assert!(!relative_path_value("OPENCLAW_HOME", "~/claw"));
        assert!(!relative_path_value("OPENCLAW_STATE_DIR", r"~\state"));
        assert!(!relative_path_value("PI_CONFIG_DIR", ".omp"));
        assert!(!relative_path_value("PI_CODING_AGENT_DIR", "~/pi"));
        // Pi does not expand `~\`, so that one stays relative.
        assert!(relative_path_value("PI_CODING_AGENT_DIR", r"~\pi"));
        // Absolute paths and names that are not paths never depend on it.
        let absolute = std::env::current_dir().unwrap().display().to_string();
        assert!(!relative_path_value("QWEN_HOME", &absolute));
        assert!(!relative_path_value("OMP_PROFILE", "work"));
        assert!(!relative_path_value("HERMES_DATA_DIR_SUFFIX", "dev"));
    }

    #[test]
    fn action_runs_the_wrapper_through_a_headless_console_host() {
        let (program, arguments) = task_action(
            Path::new(r"C:\Windows"),
            Path::new(r"C:\Users\Ann Lee\AppData\Local\ai-hist\services\ai-hist-sync.cmd"),
        );
        assert_eq!(program, r"C:\Windows\System32\conhost.exe");
        assert_eq!(
            arguments,
            r#"--headless "C:\Users\Ann Lee\AppData\Local\ai-hist\services\ai-hist-sync.cmd""#
        );
    }

    #[test]
    fn task_xml_sets_the_schedule_policy_and_escaped_action() {
        let action = (
            r"C:\Windows\System32\conhost.exe".to_string(),
            r#"--headless "C:\a&b\cmd.exe" /c ""x<y>.cmd"""#.to_string(),
        );
        let xml = task_xml(&SYNC_SERVICE, "2026-10-20T09:30:00", 5, &action);
        for expected in [
            "<Interval>PT5M</Interval>",
            "<StartBoundary>2026-10-20T09:30:00</StartBoundary>",
            "<LogonType>InteractiveToken</LogonType>",
            "<RunLevel>LeastPrivilege</RunLevel>",
            "<MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>",
            "<DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>",
            "<StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>",
            "<StartWhenAvailable>true</StartWhenAvailable>",
            "<Hidden>true</Hidden>",
            "<ExecutionTimeLimit>PT6H</ExecutionTimeLimit>",
            r"<Command>C:\Windows\System32\conhost.exe</Command>",
            r#"<Arguments>--headless "C:\a&amp;b\cmd.exe" /c ""x&lt;y&gt;.cmd""</Arguments>"#,
        ] {
            assert!(xml.contains(expected), "missing {expected} in\n{xml}");
        }
        // A removed working directory would stop the task from starting.
        assert!(!xml.contains("<WorkingDirectory>"));
        // No repetition duration: the trigger repeats indefinitely.
        assert!(!xml.contains("<Duration>"));
    }

    #[test]
    fn task_definition_is_utf16le_with_bom() {
        assert_eq!(utf16le_with_bom("<é"), vec![0xFF, 0xFE, b'<', 0, 0xE9, 0]);
    }

    #[cfg(windows)]
    #[test]
    fn scheduler_environment_rejects_unpaired_surrogates_on_windows() {
        use std::os::windows::ffi::OsStringExt;

        let value = std::ffi::OsString::from_wide(&[u16::from(b'D'), 0xD800]);
        let error = scheduler_environment_value("CODEX_HOME", value).unwrap_err();
        assert!(error.to_string().contains("contains non-UTF-8 bytes"));
    }
}
