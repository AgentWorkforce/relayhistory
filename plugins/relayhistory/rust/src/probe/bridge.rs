//! The desktop bridge: machine-readable commands Relay Desktop drives.
//!
//! Every `--json` command prints one JSON object on stdout (NDJSON, one object
//! per line, for `cloud install`), always carrying `bridge_version`. Failures
//! keep the binary's existing rule: one safe sentence on stderr and exit 1,
//! never a response body, credential or session content.
//!
//! Regeneration (`sessions include`, `sharing set`) is done by the command, not
//! by the collector: the command stops a running collector, takes the collector
//! lock, replaces the generation and starts the collector again, so it returns
//! only once the change is durable. Exclusions that are only *added* need no
//! generation — delivery rechecks them when a batch is prepared, claimed and
//! dispatched — so those run without touching the collector.
use super::{collector, home, read_config, save_json, user_error, Config, SharingMode};
use ai_hist::delivery::{self, SessionIdentity};
use anyhow::{Context, Result};
use relayhistory_plugin::cloud;
use rusqlite::Connection;
use serde_json::{json, Value};
use std::{collections::HashSet, fs, io::Write, path::Path, time::Duration};

/// The contract version every object on stdout carries.
pub const BRIDGE_VERSION: u32 = 1;
/// Longest title a session row reports.
const TITLE_LIMIT: usize = 120;

pub fn emit(value: Value) -> Result<()> {
    let value = envelope(value)?;
    let mut out = std::io::stdout().lock();
    serde_json::to_writer(&mut out, &value)?;
    out.write_all(b"\n")?;
    out.flush()?;
    Ok(())
}
/// Every object the bridge prints names the contract it was written against.
fn envelope(mut value: Value) -> Result<Value> {
    value
        .as_object_mut()
        .context("bridge output must be an object")?
        .insert("bridge_version".into(), json!(BRIDGE_VERSION));
    Ok(value)
}
/// A directory is "connected" exactly while `config.json` is there; disconnect
/// removes it. Say so instead of surfacing a file-not-found.
fn connected(directory: &Path) -> Result<Config> {
    if !directory.join("config.json").exists() {
        return Err(user_error(
            "This computer is not connected to that workspace. Run setup first.",
        ));
    }
    read_config(directory)
}
fn open(directory: &Path) -> Result<Connection> {
    ai_hist::open_db(&directory.join("history.db"))
}
fn job_status(conn: &Connection, config: &Config) -> Result<delivery::DeliveryStatus> {
    delivery::status(conn, &config.job_id).map_err(|_| {
        user_error("Upload status is unavailable. Reconnect this computer to Agent Relay.")
    })
}
fn identity_label(identity: &SessionIdentity) -> String {
    format!("{}:{}", identity.source, identity.session_id)
}

/// `installs --json`: every connected workspace on this computer.
pub fn installs(json: bool) -> Result<()> {
    let root = home()?.join(".agentworkforce/probe");
    let mut directories: Vec<_> = fs::read_dir(&root)
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.join("config.json").is_file())
        .collect();
    directories.sort();
    let mut installs = Vec::new();
    for directory in directories {
        // A directory whose configuration does not parse is not an install the
        // app can drive; listing it would only offer actions that all fail.
        let Ok(config) = read_config(&directory) else {
            continue;
        };
        installs.push(json!({
            "directory": directory.to_string_lossy(),
            "site_url": config.site_url,
            "account_id": config.account_id,
            "workspace_id": config.workspace_id,
            "org_id": config.org_id,
            "sharing_mode": config.sharing_mode(),
            "running": collector::running(&directory).unwrap_or(false),
            "paused": paused(&directory, &config),
        }));
    }
    if json {
        return emit(json!({ "installs": installs }));
    }
    if installs.is_empty() {
        println!("No workspace is connected on this computer.");
    }
    for install in &installs {
        let running = if install["running"] == json!(true) {
            "running"
        } else {
            "stopped"
        };
        println!(
            "{} {} {} {}",
            install["directory"].as_str().unwrap_or_default(),
            install["site_url"].as_str().unwrap_or_default(),
            install["workspace_id"].as_str().unwrap_or_default(),
            running
        );
    }
    Ok(())
}
/// Best effort: a listing must survive a database another process is using.
fn paused(directory: &Path, config: &Config) -> bool {
    if !directory.join("history.db").is_file() {
        return false;
    }
    let Ok(conn) = open(directory) else {
        return false;
    };
    delivery::status(&conn, &config.job_id).is_ok_and(|job| job.state == "paused")
}

/// `status <target> --json`. Without `--json` the existing sentence stays.
pub fn status(directory: &Path, json: bool) -> Result<()> {
    if !json {
        return collector::print_status(directory);
    }
    let config = connected(directory)?;
    let running = collector::running(directory)?;
    let conn = open(directory)?;
    let job = job_status(&conn, &config)?;
    let (total, excluded) = session_counts(&conn)?;
    drop(conn);
    emit(status_value(
        directory, &config, running, &job, total, excluded,
    ))
}
fn status_value(
    directory: &Path,
    config: &Config,
    running: bool,
    job: &delivery::DeliveryStatus,
    total: i64,
    excluded: i64,
) -> Value {
    json!({
        "probe_version": env!("CARGO_PKG_VERSION"),
        "running": running,
        "paused": job.state == "paused",
        "sharing_mode": config.sharing_mode(),
        "site_url": config.site_url,
        "account_id": config.account_id,
        "workspace_id": config.workspace_id,
        "org_id": config.org_id,
        "directory": directory.to_string_lossy(),
        "delivery": {
            "state": job.state,
            "bootstrap_complete": job.bootstrap_complete,
            "pending_records": job.pending_records,
            "pending_bytes": job.pending_bytes,
            "acknowledged_records": job.acknowledged_records,
            "unqueued_changes": job.unqueued_changes,
            "suppressed_records": job.suppressed_records,
            "last_attempt_ms": job.last_attempt_ms,
            "last_acknowledged_ms": job.last_acknowledged_ms,
            "next_attempt_ms": job.next_attempt_ms,
            "failure": job.failure,
        },
        "sessions": {
            "total": total,
            "shared": total - excluded,
            "excluded": excluded,
        },
        "last_cycle": last_cycle(directory),
    })
}
/// Total known session identities and how many of them are withheld. Exclusion
/// rows outlive the sessions they name, so only rows matching a known identity
/// are counted; otherwise "shared" could go negative after a local cleanup.
fn session_counts(conn: &Connection) -> Result<(i64, i64)> {
    let mut identities: HashSet<SessionIdentity> = HashSet::new();
    let mut cursor: Option<SessionIdentity> = None;
    loop {
        let page = ai_hist::storage::session_identities_after(conn, cursor.as_ref(), 1000)?;
        let Some(last) = page.last().cloned() else {
            break;
        };
        cursor = Some(last);
        identities.extend(page);
    }
    let mut statement = conn.prepare("SELECT source, session_id FROM delivery_exclusions")?;
    let rows = statement.query_map([], |row| {
        Ok(SessionIdentity {
            source: row.get(0)?,
            session_id: row.get(1)?,
        })
    })?;
    let mut excluded = 0i64;
    for row in rows {
        if identities.contains(&row?) {
            excluded += 1;
        }
    }
    Ok((identities.len() as i64, excluded))
}
/// What the collector recorded after its last cycle. Absent until one has run.
fn last_cycle(directory: &Path) -> Value {
    let Ok(raw) = fs::read(directory.join("cycle.json")) else {
        return Value::Null;
    };
    let Ok(value) = serde_json::from_slice::<Value>(&raw) else {
        return Value::Null;
    };
    json!({
        "at_ms": value["at_ms"],
        "ok": value["ok"],
        "message": value["message"],
    })
}

/// `start <target> [--json]`: no Cloud sign-in, and starting twice is fine.
pub fn start(directory: &Path, json: bool) -> Result<()> {
    connected(directory)?;
    let started = if collector::running(directory)? {
        false
    } else {
        collector::start_background(directory, !json)?;
        true
    };
    if json {
        return emit(json!({"running": true, "started": started}));
    }
    if !started {
        println!("Probe is running.");
    }
    Ok(())
}

/// `pause` / `resume <target> [--json]`. Both are idempotent: the collector
/// treats a paused job as healthy, so pausing twice is not an error state.
pub fn set_paused(directory: &Path, pause: bool, json: bool) -> Result<()> {
    let config = connected(directory)?;
    let conn = open(directory)?;
    let status = job_status(&conn, &config)?;
    if (status.state == "paused") != pause {
        let changed = if pause {
            delivery::pause_job(&conn, &config.job_id)
        } else {
            delivery::resume_job(&conn, &config.job_id)
        };
        changed.map_err(|_| {
            user_error("Uploads need attention. Reconnect this computer to Agent Relay.")
        })?;
    }
    drop(conn);
    if json {
        return emit(json!({ "paused": pause }));
    }
    println!(
        "{}",
        if pause {
            "Uploads paused. Sessions are still captured locally."
        } else {
            "Uploads resumed."
        }
    );
    Ok(())
}

/// `sessions list <target> --json [--limit N]`. Read-only: nothing here writes.
pub fn list_sessions(directory: &Path, limit: usize, json: bool) -> Result<()> {
    let config = connected(directory)?;
    let conn = open(directory)?;
    let job = job_status(&conn, &config)?;
    // Per-session acknowledgement is not cheaply derivable from the delivery
    // journal, so an included session reports "shared" unless the generation
    // still has work in flight, in which case it is honestly "queued".
    let queued = !job.bootstrap_complete || job.pending_records > 0;
    let sessions = session_rows(&conn, limit, queued)?;
    drop(conn);
    if json {
        return emit(json!({
            "sharing_mode": config.sharing_mode(),
            "sessions": sessions,
        }));
    }
    for session in &sessions {
        println!(
            "{}:{} {} {}",
            session["source"].as_str().unwrap_or_default(),
            session["session_id"].as_str().unwrap_or_default(),
            session["status"].as_str().unwrap_or_default(),
            session["title"].as_str().unwrap_or_default()
        );
    }
    Ok(())
}
/// The session catalog, newest activity first. Read-only, and the exclusion
/// table is joined rather than consulted per row.
fn session_rows(conn: &Connection, limit: usize, queued: bool) -> Result<Vec<Value>> {
    let mut statement = conn.prepare(
        "SELECT s.source, s.session_id, s.cwd, s.git_branch, s.first_activity_ms, \
         s.last_activity_ms, s.first_prompt, s.last_assistant_text, \
         EXISTS(SELECT 1 FROM delivery_exclusions x \
         WHERE x.source=s.source AND x.session_id=s.session_id) \
         FROM sessions s ORDER BY s.last_activity_ms DESC, s.source, s.session_id LIMIT ?1",
    )?;
    let rows = statement.query_map([limit.clamp(1, 10_000) as i64], |row| {
        let session_id: String = row.get(1)?;
        let excluded: bool = row.get(8)?;
        let title = title(
            row.get::<_, Option<String>>(6)?,
            row.get::<_, Option<String>>(7)?,
            &session_id,
        );
        Ok(json!({
            "source": row.get::<_, String>(0)?,
            "session_id": session_id,
            "title": title,
            "cwd": row.get::<_, Option<String>>(2)?,
            "git_branch": row.get::<_, Option<String>>(3)?,
            "first_activity_ms": row.get::<_, Option<i64>>(4)?,
            "last_activity_ms": row.get::<_, Option<i64>>(5)?,
            "included": !excluded,
            "status": if excluded {
                "not_shared"
            } else if queued {
                "queued"
            } else {
                "shared"
            },
        }))
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}
/// The first prompt, then the last assistant message, then the identifier.
/// Whitespace is collapsed so a multi-line prompt stays one table row.
fn title(first_prompt: Option<String>, last_text: Option<String>, session_id: &str) -> String {
    for candidate in [first_prompt, last_text] {
        let collapsed = candidate
            .unwrap_or_default()
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        if !collapsed.is_empty() {
            return collapsed.chars().take(TITLE_LIMIT).collect();
        }
    }
    session_id.to_string()
}

/// `sessions include` / `sessions exclude <target> --json --session SOURCE:ID …`
pub fn select_sessions(
    directory: &Path,
    sessions: &[SessionIdentity],
    include: bool,
    json: bool,
) -> Result<()> {
    let config = connected(directory)?;
    let mut selected = collector::load_selected(directory)?;
    let labels: Vec<String> = sessions.iter().map(identity_label).collect();
    if !include {
        // Withdrawing a hand-picked session must also leave the durable list,
        // or the next baseline would share it again.
        selected.retain(|identity| !sessions.contains(identity));
        collector::save_selected(directory, &selected)?;
        let conn = open(directory)?;
        for identity in sessions {
            delivery::set_session_excluded(&conn, identity, true)?;
        }
        drop(conn);
        if json {
            return emit(json!({"excluded": labels, "regenerated": false}));
        }
        println!("{} session(s) are no longer shared.", labels.len());
        return Ok(());
    }
    for identity in sessions {
        if !selected.contains(identity) {
            selected.push(identity.clone());
        }
    }
    let regenerated = include_sessions(directory, &config, sessions, &selected)?;
    if json {
        return emit(json!({"included": labels, "regenerated": regenerated}));
    }
    println!("{} session(s) are shared.", labels.len());
    Ok(())
}

/// `sharing set <target> --mode all|new|selected --json`
pub fn set_sharing(directory: &Path, mode: SharingMode, json: bool) -> Result<()> {
    let config = connected(directory)?;
    let selected = collector::load_selected(directory)?;
    let regenerated = apply_mode(directory, &config, mode, &selected)?;
    if json {
        return emit(json!({"sharing_mode": mode, "regenerated": regenerated}));
    }
    let described = match mode {
        SharingMode::All => "all sessions, past and future",
        SharingMode::New => "only sessions from now on",
        SharingMode::Selected => "only the sessions you choose",
    };
    println!("This computer now uploads: {described}.");
    Ok(())
}

/// What a replacement generation does to the exclusion table.
enum Baseline<'a> {
    /// `sharing set`: derive every exclusion again from the mode and the list.
    Rebuild(&'a [SessionIdentity]),
    /// `sessions include`: withdraw exactly these and leave every other session
    /// alone. A `new` install must not have its baseline re-derived, or the
    /// sessions it has been sharing since setup would suddenly be withheld.
    Withdraw(&'a [SessionIdentity]),
}
/// Share the named sessions, and report whether that needed a new generation.
/// It does when a live job already selects them, because withdrawing such an
/// exclusion is refused; when none of them is withheld there is nothing to do.
fn include_sessions(
    directory: &Path,
    config: &Config,
    sessions: &[SessionIdentity],
    selected: &[SessionIdentity],
) -> Result<bool> {
    // Written first: a crash after this leaves sessions listed but still
    // withheld, which the user can retry, rather than shared without a record.
    collector::save_selected(directory, selected)?;
    let regenerate = {
        let conn = open(directory)?;
        let job = job_status(&conn, config)?;
        let mut needed = job.state == "cancelled";
        for identity in sessions {
            needed = needed || collector::is_excluded(&conn, identity)?;
        }
        needed
    };
    if regenerate {
        let mode = config.sharing_mode();
        regenerate_generation(directory, config, mode, Baseline::Withdraw(sessions))?;
    }
    Ok(regenerate)
}
/// Converge one install on `mode`, and report whether that needed a new
/// delivery generation.
///
/// It does when the new baseline withdraws an exclusion a live job selects, or
/// when the job's own selection changes (only `all` exports the prompt-only
/// rows). Everything else adds exclusions, which delivery rechecks when a batch
/// is prepared, claimed and dispatched, so it applies while the collector runs.
fn apply_mode(
    directory: &Path,
    config: &Config,
    mode: SharingMode,
    selected: &[SessionIdentity],
) -> Result<bool> {
    let regenerate = {
        let conn = open(directory)?;
        let job = job_status(&conn, config)?;
        job.state == "cancelled"
            || job.config.selection != collector::selection(mode.include_existing())
            || withdraws_exclusions(&conn, mode, selected)?
    };
    if regenerate {
        regenerate_generation(directory, config, mode, Baseline::Rebuild(selected))?;
        return Ok(true);
    }
    let conn = open(directory)?;
    collector::record_baseline_for(&conn, mode, selected)?;
    drop(conn);
    let mut config = config.clone();
    config.set_sharing_mode(mode);
    save_json(&directory.join("config.json"), &config)?;
    Ok(false)
}
/// Whether the new baseline has to take an exclusion back. Removing one is
/// refused while a live job selects the session, so this is what forces a new
/// generation.
fn withdraws_exclusions(
    conn: &Connection,
    mode: SharingMode,
    selected: &[SessionIdentity],
) -> Result<bool> {
    let mut cursor: Option<SessionIdentity> = None;
    loop {
        let page = ai_hist::storage::session_identities_after(conn, cursor.as_ref(), 1000)?;
        let Some(last) = page.last().cloned() else {
            return Ok(false);
        };
        cursor = Some(last);
        for identity in page {
            let keep = mode.include_existing() || selected.contains(&identity);
            if keep && collector::is_excluded(conn, &identity)? {
                return Ok(true);
            }
        }
    }
}
/// Replace the delivery generation under the collector lock. The collector must
/// not be mid-cycle, so a running one is stopped first and started again
/// afterwards; the command returns only once `config.json` names the new job.
fn regenerate_generation(
    directory: &Path,
    config: &Config,
    mode: SharingMode,
    baseline: Baseline<'_>,
) -> Result<()> {
    let was_running = collector::stop_quiet(directory)?;
    let replaced = replace_generation(directory, config, mode, baseline);
    let restarted = if was_running {
        collector::start_background(directory, false)
    } else {
        Ok(())
    };
    replaced?;
    restarted
}
fn replace_generation(
    directory: &Path,
    config: &Config,
    mode: SharingMode,
    baseline: Baseline<'_>,
) -> Result<()> {
    let _guard = super::lock(directory)?;
    let conn = open(directory)?;
    // No live generation may select a session whose exclusion is withdrawn, so
    // the old one goes first and the new one is created last.
    collector::cancel_jobs(&conn, &config.delivery_account)?;
    match baseline {
        Baseline::Rebuild(selected) => {
            collector::record_baseline_for(&conn, mode, selected)?;
        }
        Baseline::Withdraw(sessions) => {
            for identity in sessions {
                delivery::set_session_excluded(&conn, identity, false)?;
            }
        }
    }
    let job = delivery::create_job(
        &conn,
        &collector::delivery_config(&config.delivery_account, mode),
        collector::now(),
    )?;
    drop(conn);
    let mut config = config.clone();
    config.job_id = job.job_id;
    config.set_sharing_mode(mode);
    save_json(&directory.join("config.json"), &config)
}

/// `disconnect <target> [--json]`: stop, cancel the generation, revoke and drop
/// the stored session, and forget the configuration. `history.db` is the user's
/// local data and stays.
pub fn disconnect(directory: &Path, json: bool) -> Result<()> {
    let config = connected(directory)?;
    collector::stop_quiet(directory)?;
    let _guard = super::lock(directory)?;
    {
        let conn = open(directory)?;
        let _ = delivery::cancel_job(&conn, &config.job_id);
    }
    revoke(directory, &config);
    let _ = fs::remove_dir_all(directory.join("stages"));
    for name in ["config.json", "selected.json", "cycle.json", "stop.json"] {
        let path = directory.join(name);
        if path.exists() {
            fs::remove_file(path)?;
        }
    }
    if json {
        return emit(json!({ "disconnected": true }));
    }
    println!("Disconnected. Local session history was kept.");
    Ok(())
}
/// Best effort: a session the service already dropped, or an unreachable
/// service, must not stop the local disconnect. Nothing about the credential is
/// printed, logged or returned.
fn revoke(directory: &Path, config: &Config) {
    std::env::set_var("RELAYHISTORY_HOME", directory);
    let Ok(Some(auth)) = cloud::load_auth(Some(&config.history_url)) else {
        return;
    };
    let token = auth.refresh_token.unwrap_or(auth.access_token);
    if token.is_empty() {
        return;
    }
    let agent = ureq::AgentBuilder::new()
        .redirects(0)
        .timeout(Duration::from_secs(15))
        .build();
    let _ = agent
        .post(&format!("{}/v1/auth/token/revoke", config.history_url))
        .send_json(json!({ "token": token }));
}

#[cfg(test)]
mod tests {
    use super::*;
    use relayhistory_plugin::destination;

    fn identity(source: &str, session_id: &str) -> SessionIdentity {
        SessionIdentity {
            source: source.into(),
            session_id: session_id.into(),
        }
    }
    fn config(job_id: &str) -> Config {
        let mut config = Config {
            version: 1,
            site_url: "https://agentrelay.com".into(),
            account_id: "usr_1".into(),
            org_id: "org_1".into(),
            workspace_id: "ws_1".into(),
            history_url: "https://history.agentrelay.com".into(),
            delivery_account: destination::account_id("org_1", Some("ws_1")),
            job_id: job_id.into(),
            include_existing: false,
            acknowledge_uninspected_schedules: false,
            sharing_mode: None,
        };
        config.set_sharing_mode(SharingMode::New);
        config
    }
    fn session(conn: &Connection, source: &str, session_id: &str, prompt: Option<&str>, ms: i64) {
        conn.execute(
            "INSERT INTO sessions(source, session_id, first_prompt, last_activity_ms, cwd, git_branch) \
             VALUES (?1,?2,?3,?4,'/code/app','main')",
            rusqlite::params![source, session_id, prompt, ms],
        )
        .unwrap();
    }
    #[test]
    fn every_object_names_the_contract_version() {
        let value = envelope(json!({"running": true})).unwrap();
        assert_eq!(value["bridge_version"], 1);
        assert_eq!(value["running"], true);
        assert!(envelope(json!([1, 2])).is_err());
    }
    #[test]
    fn session_rows_carry_titles_and_sharing_state() {
        let directory = tempfile::tempdir().unwrap();
        let conn = ai_hist::open_db(&directory.path().join("history.db")).unwrap();
        let long_prompt = "x".repeat(200);
        session(&conn, "claude", "aaa", Some(long_prompt.as_str()), 2_000);
        session(&conn, "codex", "bbb", None, 1_000);
        delivery::set_session_excluded(&conn, &identity("codex", "bbb"), true).unwrap();
        let rows = session_rows(&conn, 500, false).unwrap();
        assert_eq!(rows.len(), 2);
        // Newest activity first, and a long prompt is trimmed to 120 characters.
        assert_eq!(rows[0]["source"], "claude");
        assert_eq!(rows[0]["session_id"], "aaa");
        assert_eq!(rows[0]["title"].as_str().unwrap().chars().count(), 120);
        assert_eq!(rows[0]["cwd"], "/code/app");
        assert_eq!(rows[0]["git_branch"], "main");
        assert_eq!(rows[0]["last_activity_ms"], 2_000);
        assert_eq!(rows[0]["included"], true);
        assert_eq!(rows[0]["status"], "shared");
        // No prompt and no assistant text: the identifier is the only title.
        assert_eq!(rows[1]["title"], "bbb");
        assert_eq!(rows[1]["included"], false);
        assert_eq!(rows[1]["status"], "not_shared");
        assert_eq!(
            session_rows(&conn, 500, true).unwrap()[0]["status"],
            "queued"
        );
        assert_eq!(session_rows(&conn, 1, false).unwrap().len(), 1);
    }
    #[test]
    fn status_reports_delivery_and_session_counts() {
        let directory = tempfile::tempdir().unwrap();
        let conn = ai_hist::open_db(&directory.path().join("history.db")).unwrap();
        session(&conn, "claude", "aaa", Some("first"), 2_000);
        session(&conn, "claude", "bbb", Some("second"), 1_000);
        let account = destination::account_id("org_1", Some("ws_1"));
        collector::record_baseline_for(&conn, SharingMode::New, &[identity("claude", "aaa")])
            .unwrap();
        let job = delivery::create_job(
            &conn,
            &collector::delivery_config(&account, SharingMode::New),
            collector::now(),
        )
        .unwrap();
        let config = config(&job.job_id);
        let (total, excluded) = session_counts(&conn).unwrap();
        assert_eq!((total, excluded), (2, 1));
        let value = envelope(status_value(
            directory.path(),
            &config,
            false,
            &job,
            total,
            excluded,
        ))
        .unwrap();
        assert_eq!(value["bridge_version"], 1);
        assert_eq!(value["probe_version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(value["running"], false);
        assert_eq!(value["paused"], false);
        assert_eq!(value["sharing_mode"], "new");
        assert_eq!(value["site_url"], "https://agentrelay.com");
        assert_eq!(value["workspace_id"], "ws_1");
        assert_eq!(value["org_id"], "org_1");
        assert_eq!(
            value["sessions"],
            json!({"total":2,"shared":1,"excluded":1})
        );
        assert_eq!(value["delivery"]["state"], "active");
        assert_eq!(value["delivery"]["pending_records"], 0);
        assert_eq!(value["delivery"]["bootstrap_complete"], false);
        assert!(value["delivery"]["failure"].is_null());
        // No cycle has run yet.
        assert!(value["last_cycle"].is_null());
        save_json(
            &directory.path().join("cycle.json"),
            &json!({"at_ms": 5, "ok": false, "message": "Sync paused or offline."}),
        )
        .unwrap();
        let value = status_value(directory.path(), &config, false, &job, total, excluded);
        assert_eq!(
            value["last_cycle"],
            json!({"at_ms": 5, "ok": false, "message": "Sync paused or offline."})
        );
        // A paused generation is reported as paused, not as a failure.
        let paused = delivery::pause_job(&conn, &job.job_id).unwrap();
        let value = status_value(directory.path(), &config, true, &paused, total, excluded);
        assert_eq!(value["paused"], true);
        assert_eq!(value["delivery"]["state"], "paused");
    }
    #[test]
    fn including_a_session_replaces_the_generation_and_records_the_choice() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path();
        let conn = ai_hist::open_db(&path.join("history.db")).unwrap();
        session(&conn, "claude", "aaa", Some("first"), 2_000);
        session(&conn, "claude", "bbb", Some("second"), 1_000);
        let account = destination::account_id("org_1", Some("ws_1"));
        collector::record_baseline_for(&conn, SharingMode::New, &[]).unwrap();
        let job = delivery::create_job(
            &conn,
            &collector::delivery_config(&account, SharingMode::New),
            collector::now(),
        )
        .unwrap();
        drop(conn);
        let config = config(&job.job_id);
        save_json(&path.join("config.json"), &config).unwrap();
        let wanted = identity("claude", "aaa");
        let chosen = [wanted.clone()];
        assert!(include_sessions(path, &config, &chosen, &chosen).unwrap());
        // The choice is durable and the exclusion really was withdrawn.
        assert_eq!(collector::load_selected(path).unwrap(), chosen);
        let conn = ai_hist::open_db(&path.join("history.db")).unwrap();
        assert!(!collector::is_excluded(&conn, &wanted).unwrap());
        assert!(collector::is_excluded(&conn, &identity("claude", "bbb")).unwrap());
        // A new generation is live and named by the saved configuration.
        let saved = read_config(path).unwrap();
        assert_ne!(saved.job_id, job.job_id);
        assert_eq!(saved.sharing_mode(), SharingMode::New);
        assert_eq!(
            delivery::status(&conn, &saved.job_id).unwrap().state,
            "active"
        );
        assert_eq!(
            delivery::status(&conn, &job.job_id).unwrap().state,
            "cancelled"
        );
        // Including a session that is already shared needs no new generation.
        assert!(!include_sessions(path, &saved, &chosen, &chosen).unwrap());
        assert_eq!(read_config(path).unwrap().job_id, saved.job_id);
    }
    #[test]
    fn sharing_all_regenerates_and_sharing_selected_only_adds_exclusions() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path();
        let conn = ai_hist::open_db(&path.join("history.db")).unwrap();
        session(&conn, "claude", "aaa", Some("first"), 2_000);
        let account = destination::account_id("org_1", Some("ws_1"));
        let job = delivery::create_job(
            &conn,
            &collector::delivery_config(&account, SharingMode::New),
            collector::now(),
        )
        .unwrap();
        drop(conn);
        let config = config(&job.job_id);
        save_json(&path.join("config.json"), &config).unwrap();
        // new -> selected keeps the same delivery selection and withdraws
        // nothing, so the generation survives and only exclusions are added.
        assert!(!apply_mode(path, &config, SharingMode::Selected, &[]).unwrap());
        let saved = read_config(path).unwrap();
        assert_eq!(saved.sharing_mode(), SharingMode::Selected);
        assert_eq!(saved.job_id, job.job_id);
        let conn = ai_hist::open_db(&path.join("history.db")).unwrap();
        assert!(collector::is_excluded(&conn, &identity("claude", "aaa")).unwrap());
        drop(conn);
        // selected -> all withdraws every exclusion, which needs a generation.
        assert!(apply_mode(path, &saved, SharingMode::All, &[]).unwrap());
        let saved = read_config(path).unwrap();
        assert_eq!(saved.sharing_mode(), SharingMode::All);
        assert!(saved.include_existing);
        assert_ne!(saved.job_id, job.job_id);
        let conn = ai_hist::open_db(&path.join("history.db")).unwrap();
        assert!(!collector::is_excluded(&conn, &identity("claude", "aaa")).unwrap());
        assert_eq!(
            delivery::status(&conn, &saved.job_id)
                .unwrap()
                .config
                .selection,
            collector::selection(true)
        );
    }
}
