//! Desktop bridge v1. Credentials stay in the probe; only safe status is emitted.
//! Sharing changes stop the collector and persist a replayable plan before
//! replacing a delivery generation. An interrupted change fences startup until
//! recovery completes, so it cannot accidentally broaden sharing.
use super::{collector, lock, read_config, save_json, user_error, Config, Target};
use anyhow::{ensure, Result};
use clap::{Args, Subcommand, ValueEnum};
use fs2::FileExt;
use relayhistory_plugin::delivery::{self, DeliveryJobConfig, SessionIdentity};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::Path,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

static JSON: AtomicBool = AtomicBool::new(false);
pub fn set_json(value: bool) {
    JSON.store(value, Ordering::Relaxed);
}
pub fn json_mode() -> bool {
    JSON.load(Ordering::Relaxed)
}
pub fn emit(mut value: Value) {
    value["bridge_version"] = json!(1);
    println!("{value}");
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum SharingMode {
    All,
    New,
    Selected,
}
pub fn mode(config: &Config) -> SharingMode {
    config.sharing_mode.unwrap_or(if config.include_existing {
        SharingMode::All
    } else {
        SharingMode::New
    })
}

#[derive(Subcommand)]
pub enum SessionCommand {
    List(SessionList),
    /// Local-only shallow catalog; never reads delivery batches or sends data.
    Preview(SessionPreview),
    Include(SessionMutation),
    Exclude(SessionMutation),
}
#[derive(Args)]
pub struct SessionList {
    #[command(flatten)]
    target: Target,
    #[arg(long, default_value_t = 500)]
    limit: usize,
}
#[derive(Args)]
pub struct SessionPreview {
    #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u16).range(1..=100))]
    limit: u16,
    #[arg(long)]
    refresh: bool,
}
#[derive(Args)]
pub struct SessionMutation {
    #[command(flatten)]
    target: Target,
    #[arg(long = "session", required = true)]
    keys: Vec<String>,
}
#[derive(Subcommand)]
pub enum SharingCommand {
    Set(SharingSet),
}
#[derive(Args)]
pub struct SharingSet {
    #[command(flatten)]
    target: Target,
    #[arg(long, value_enum)]
    mode: SharingMode,
}

pub fn control_lock(directory: &Path) -> Result<fs::File> {
    super::private_directory(directory)?;
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(directory.join("desktop.lock"))?;
    file.try_lock_exclusive().map_err(|_| {
        super::user_error("Another setup or upload change is in progress. Please try again.")
    })?;
    Ok(file)
}
fn db(directory: &Path) -> Result<Connection> {
    let conn = relayhistory_plugin::delivery::open_db(&directory.join("history.db"))?;
    conn.busy_timeout(Duration::from_secs(5))?;
    Ok(conn)
}
fn read_db(directory: &Path) -> Result<Connection> {
    let conn = ai_hist::open_db_readonly(&directory.join("history.db"))?;
    conn.busy_timeout(Duration::from_secs(5))?;
    Ok(conn)
}
fn identities(conn: &Connection) -> Result<Vec<SessionIdentity>> {
    let mut rows = Vec::new();
    loop {
        let page = ai_hist::storage::session_identities_after(conn, rows.last(), 1000)?;
        if page.is_empty() {
            break;
        }
        rows.extend(page);
    }
    Ok(rows)
}
fn selected(directory: &Path) -> Result<Vec<SessionIdentity>> {
    let path = directory.join("selected.json");
    if !path.exists() {
        return Ok(vec![]);
    }
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}
fn excluded(conn: &Connection) -> Result<HashSet<SessionIdentity>> {
    let mut statement = conn.prepare("SELECT source,session_id FROM delivery_exclusions")?;
    let rows = statement.query_map([], |r| {
        Ok(SessionIdentity {
            source: r.get(0)?,
            session_id: r.get(1)?,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}
pub fn enforce_selection(directory: &Path, config: &Config) -> Result<()> {
    if mode(config) != SharingMode::Selected {
        return Ok(());
    }
    let conn = db(directory)?;
    if !delivery::is_session_job(&conn, &config.job_id)? {
        delivery::adopt_session_job(&conn, &config.job_id, &selected(directory)?)?;
    }
    Ok(())
}

fn summary(directory: &Path, config: &Config) -> Result<Value> {
    let conn = read_db(directory)?;
    let job = delivery::status(&conn, &config.job_id)?;
    Ok(
        json!({"directory":directory, "site_url":config.site_url, "account_id":config.account_id,
        "account_email":config.account_email, "account_name":config.account_name,
        "account_avatar_url":config.account_avatar_url,
        "workspace_id":config.workspace_id, "org_id":config.org_id, "sharing_mode":mode(config),
        "running":collector::running(directory)?, "paused":job.state == "paused"}),
    )
}
pub fn installs() -> Result<()> {
    let root = super::home()?.join(".agentworkforce/probe");
    let mut installs = Vec::new();
    if root.exists() {
        for entry in fs::read_dir(root)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let directory = entry.path();
            if let Ok(config) = read_config(&directory) {
                if let Ok(value) = summary(&directory, &config) {
                    installs.push(value);
                }
            }
        }
    }
    installs.sort_by_key(|v| v["directory"].as_str().unwrap_or_default().to_owned());
    emit(json!({"installs":installs}));
    Ok(())
}
pub fn start(directory: &Path) -> Result<()> {
    let _control = control_lock(directory)?;
    let running = collector::running(directory)?;
    if !running {
        let guard = lock(directory)?;
        recover(directory)?;
        read_config(directory)?;
        drop(guard);
        collector::start_background(directory)?;
    }
    emit(json!({"running":true, "started":!running}));
    Ok(())
}
pub fn status(directory: &Path) -> Result<()> {
    emit(status_value(directory)?);
    Ok(())
}
fn status_value(directory: &Path) -> Result<Value> {
    let config = read_config(directory)?;
    let conn = read_db(directory)?;
    let job = delivery::status(&conn, &config.job_id)?;
    let withheld = excluded(&conn)?;
    let rows = identities(&conn)?;
    let scoped = delivery::is_session_job(&conn, &config.job_id)?;
    let members: HashSet<_> = if scoped {
        delivery::job_sessions(&conn, &config.job_id)?
            .into_iter()
            .collect()
    } else {
        HashSet::new()
    };
    let shared = rows
        .iter()
        .filter(|r| !withheld.contains(*r) && (!scoped || members.contains(*r)))
        .count();
    let mut result = summary(directory, &config)?;
    result["probe_version"] = json!(env!("CARGO_PKG_VERSION"));
    result["delivery"] = serde_json::to_value(job)?;
    // Do not expose job configuration or arbitrary receiver error strings.
    result["delivery"].as_object_mut().unwrap().remove("config");
    if !result["delivery"]["failure"].is_null() {
        result["delivery"]["failure"] =
            json!("Upload failed. The probe will retry; check your connection.");
    }
    result["sessions"] = json!({"total":rows.len(), "shared":shared, "excluded":rows.len()-shared});
    result["last_cycle"] = fs::read(directory.join("cycle.json"))
        .ok()
        .and_then(|data| serde_json::from_slice::<Value>(&data).ok())
        .unwrap_or(Value::Null);
    Ok(result)
}
pub fn pause(directory: &Path, paused: bool) -> Result<()> {
    let _control = control_lock(directory)?;
    let config = read_config(directory)?;
    ensure!(
        !directory.join("sharing-change.json").exists(),
        user_error("A sharing update is incomplete. Run start to recover it before retrying.")
    );
    let conn = db(directory)?;
    if paused {
        delivery::pause_job(&conn, &config.job_id)?;
    } else {
        delivery::resume_job(&conn, &config.job_id)?;
    }
    emit(json!({"paused":paused}));
    Ok(())
}

pub fn sessions(command: SessionCommand) -> Result<()> {
    match command {
        SessionCommand::Preview(options) => {
            let directory = super::home()?.join(".agentworkforce/session-preview");
            super::private_directory(&directory)?;
            let db = directory.join("catalog.db");
            if options.refresh {
                // A separate local metadata cache cannot wait behind the capture writer
                // or accidentally add unselected sessions to a delivery generation.
                ai_hist::discover_sessions_local_at(
                    &db,
                    &ai_hist::DiscoverOptions {
                        limit: Some(options.limit as usize),
                        ..Default::default()
                    },
                )?;
            }
            let rows = if db.exists() {
                ai_hist::list_sessions_local_at(
                    &db,
                    &ai_hist::CatalogListOptions {
                        limit: Some(options.limit as i64),
                        ..Default::default()
                    },
                )?
                .sessions
            } else {
                Vec::new()
            };
            let response = preview_response(&rows);
            super::save_json(&directory.join("sessions.json"), &response)?;
            emit(response);
        }
        SessionCommand::List(options) => {
            let directory = options.target.directory()?;
            let config = read_config(&directory)?;
            emit(
                json!({"sharing_mode":mode(&config), "sessions":session_rows(&directory, &config, options.limit)?}),
            );
        }
        SessionCommand::Include(options) => {
            change(&options.target.directory()?, None, &options.keys, true)?
        }
        SessionCommand::Exclude(options) => {
            change(&options.target.directory()?, None, &options.keys, false)?
        }
    }
    Ok(())
}
fn preview_response(rows: &[ai_hist::ShallowSession]) -> Value {
    let sessions: Vec<_> = rows.iter().map(|row| json!({
        "source": row.source, "session_id": row.session_id,
        "title": row.first_prompt.as_deref().unwrap_or(&row.session_id),
        "cwd": row.cwd, "git_branch": row.git_branch,
        "first_activity_ms": row.first_activity_ms, "last_activity_ms": row.last_activity_ms,
        "included": false, "status": "unknown"
    })).collect();
    json!({"bridge_version":1, "sessions":sessions})
}

fn session_rows(directory: &Path, config: &Config, limit: usize) -> Result<Vec<Value>> {
    let conn = read_db(directory)?;
    let job = delivery::status(&conn, &config.job_id)?;
    // Pending batch identity is real per-session progress. Do not pretend that
    // a global record count proves an individual session was acknowledged.
    let mut pending = HashMap::new();
    let mut batches = conn.prepare("SELECT payload,state FROM delivery_batches WHERE job_id=? AND state IN ('pending','leased','retry_wait','blocked') AND payload IS NOT NULL")?;
    for row in batches.query_map([&config.job_id], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
    })? {
        let (payload, state) = row?;
        let batch: delivery::HistoryExportBatch = serde_json::from_str(&payload)?;
        for record in batch.records {
            if let Some(id) = record.session_id {
                mark_pending(
                    &mut pending,
                    SessionIdentity {
                        source: record.source,
                        session_id: id,
                    },
                    state == "leased",
                );
            }
        }
    }
    let scoped = delivery::is_session_job(&conn, &config.job_id)?;
    let members: HashSet<_> = if scoped {
        delivery::job_sessions(&conn, &config.job_id)?
            .into_iter()
            .collect()
    } else {
        HashSet::new()
    };
    let mut query = conn.prepare("SELECT s.source,s.session_id,s.cwd,s.git_branch,s.first_activity_ms,s.last_activity_ms,
        COALESCE(NULLIF(trim(s.first_prompt),''),NULLIF(trim(s.last_assistant_text),''),s.session_id),
        NOT EXISTS(SELECT 1 FROM delivery_exclusions e WHERE e.source=s.source AND e.session_id=s.session_id)
        FROM sessions s ORDER BY s.last_activity_ms DESC,s.source,s.session_id LIMIT ?")?;
    let rows = query.query_map([i64::try_from(limit).unwrap_or(i64::MAX)], |r| {
        let source: String = r.get(0)?;
        let session_id: String = r.get(1)?;
        let policy_allowed: bool = r.get(7)?;
        let title: String = r.get(6)?;
        let identity = SessionIdentity {source:source.clone(), session_id:session_id.clone()};
        let included = policy_allowed && (!scoped || members.contains(&identity));
        let state = if !included { "not_shared" }
            else if pending.get(&identity) == Some(&true) { "uploading" }
            else if pending.contains_key(&identity) { "queued" }
            else if job.bootstrap_complete && job.pending_records == 0 && job.unqueued_changes == 0 { "uploaded" }
            else { "shared" };
        Ok(json!({"source":source,"session_id":session_id,"cwd":r.get::<_,Option<String>>(2)?,
            "git_branch":r.get::<_,Option<String>>(3)?,"first_activity_ms":r.get::<_,Option<i64>>(4)?,
            "last_activity_ms":r.get::<_,Option<i64>>(5)?,"title":title.chars().take(120).collect::<String>(),
            "included":included,"status":state}))
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}
fn mark_pending(
    pending: &mut HashMap<SessionIdentity, bool>,
    identity: SessionIdentity,
    leased: bool,
) {
    pending
        .entry(identity)
        .and_modify(|active| *active |= leased)
        .or_insert(leased);
}
pub fn sharing(command: SharingCommand) -> Result<()> {
    match command {
        SharingCommand::Set(options) => {
            change(&options.target.directory()?, Some(options.mode), &[], true)
        }
    }
}

#[derive(Serialize, Deserialize)]
struct ChangePlan {
    config: Config,
    job: DeliveryJobConfig,
    selected: Vec<SessionIdentity>,
    excluded: Vec<SessionIdentity>,
    paused: bool,
    #[serde(default)]
    delta: Option<(Vec<SessionIdentity>, bool)>,
}
fn parse_key(key: &str) -> Result<SessionIdentity> {
    let (source, id) = key
        .split_once(':')
        .ok_or_else(|| user_error("Invalid session key. Use SOURCE:ID from sessions list."))?;
    ensure!(
        !source.is_empty() && !id.is_empty(),
        user_error("Invalid session key. Use SOURCE:ID from sessions list.")
    );
    Ok(SessionIdentity {
        source: source.into(),
        session_id: id.into(),
    })
}
fn change(
    directory: &Path,
    requested: Option<SharingMode>,
    keys: &[String],
    include: bool,
) -> Result<()> {
    let _control = control_lock(directory)?;
    let identities_requested: Vec<_> = keys
        .iter()
        .map(|key| parse_key(key))
        .collect::<Result<_>>()?;
    // Reject stale keys while the collector is still running. Rebuild the plan
    // under collector.lock after stopping so newly captured sessions are seen.
    let was_running = collector::running(directory)?;
    let config = read_config(directory)?;
    if requested.is_none()
        && mode(&config) == SharingMode::Selected
        && !directory.join("sharing-change.json").exists()
    {
        let conn = read_db(directory)?;
        if delivery::is_session_job(&conn, &config.job_id)? {
            validate_identities(&conn, &identities_requested)?;
            let unchanged = identities_requested
                .iter()
                .map(|id| {
                    let member = delivery::job_session_included(&conn, &config.job_id, id)?;
                    let excluded: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM delivery_exclusions WHERE source=? AND session_id=?)", rusqlite::params![id.source,id.session_id], |r| r.get(0))?;
                    Ok(member == include && (!include || !excluded))
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .all(|v| v);
            if unchanged {
                if include {
                    emit(json!({"included":keys,"regenerated":false}));
                } else {
                    emit(json!({"excluded":keys,"regenerated":false}));
                }
                return Ok(());
            }
        }
    }
    make_plan(
        &read_db(directory)?,
        directory,
        config,
        requested,
        &identities_requested,
        include,
    )?;
    let changed = (|| -> Result<()> {
        if was_running {
            collector::stop_for_change(directory)?;
        }
        let _guard = lock(directory)?;
        recover(directory)?;
        let config = read_config(directory)?;
        let conn = db(directory)?;
        let plan = make_plan(
            &conn,
            directory,
            config,
            requested,
            &identities_requested,
            include,
        )?;
        save_json(&directory.join("sharing-change.json"), &plan)?;
        apply_plan(directory, plan)
    })();
    // A child refuses to run while a durable change is pending. Finish its
    // replay under collector.lock before launching the replacement process.
    let restart = (|| -> Result<bool> {
        if was_running && !collector::running(directory)? {
            let recovered = prepare_restart(directory)?;
            collector::start_background(directory)?;
            return Ok(recovered);
        }
        Ok(false)
    })();
    match (changed, restart) {
        (Err(change), Err(restart)) => {
            return Err(anyhow::anyhow!(
                "sharing change failed: {change:#}; collector restoration also failed: {restart:#}"
            ))
        }
        // A transient apply failure can be fully resolved by replaying the
        // durable plan before restart; report success once it is committed.
        (Err(_), Ok(true)) => {}
        (Err(change), Ok(false)) => return Err(change),
        (Ok(()), Err(restart)) => return Err(restart),
        (Ok(()), Ok(_)) => {}
    }
    let regenerated =
        requested.is_some() || mode(&read_config(directory)?) != SharingMode::Selected;
    if let Some(mode) = requested {
        emit(json!({"sharing_mode":mode,"regenerated":regenerated}));
    } else if include {
        emit(json!({"included":keys,"regenerated":regenerated}));
    } else {
        emit(json!({"excluded":keys,"regenerated":regenerated}));
    }
    Ok(())
}
fn prepare_restart(directory: &Path) -> Result<bool> {
    let _guard = lock(directory)?;
    let pending = directory.join("sharing-change.json").exists();
    recover(directory)?;
    Ok(pending)
}
fn validate_identities(conn: &Connection, identities_requested: &[SessionIdentity]) -> Result<()> {
    let mut known_requested = true;
    for id in identities_requested {
        let exists: bool=conn.query_row("SELECT EXISTS(SELECT 1 FROM sessions WHERE source=?1 AND session_id=?2) OR EXISTS(SELECT 1 FROM history WHERE source=?1 AND session_id=?2) OR EXISTS(SELECT 1 FROM session_events WHERE source=?1 AND session_id=?2)",rusqlite::params![id.source,id.session_id],|r|r.get(0))?;
        known_requested &= exists;
    }
    ensure!(
        known_requested,
        user_error("Unknown session. Refresh sessions list and try again.")
    );
    Ok(())
}
fn make_plan(
    conn: &Connection,
    directory: &Path,
    mut config: Config,
    requested: Option<SharingMode>,
    identities_requested: &[SessionIdentity],
    include: bool,
) -> Result<ChangePlan> {
    validate_identities(conn, identities_requested)?;
    let old = delivery::status(conn, &config.job_id)?;
    let mut selected = selected(directory)?;
    let incremental = requested.is_none() && mode(&config) == SharingMode::Selected;
    let mut withheld = if incremental {
        HashSet::new()
    } else {
        excluded(conn)?
    };
    if let Some(mode) = requested {
        config.sharing_mode = Some(mode);
        config.include_existing = mode == SharingMode::All;
        withheld = if mode == SharingMode::All {
            HashSet::new()
        } else {
            identities(conn)?
                .into_iter()
                .filter(|id| !selected.contains(id))
                .collect()
        };
    } else {
        for id in identities_requested.iter().cloned() {
            if include {
                withheld.remove(&id);
                if !selected.contains(&id) {
                    selected.push(id);
                }
            } else {
                selected.retain(|v| v != &id);
                withheld.insert(id);
            }
        }
    }
    let mut job = old.config;
    job.selection = collector::selection(mode(&config) == SharingMode::All);
    Ok(ChangePlan {
        config,
        job,
        selected,
        excluded: withheld.into_iter().collect(),
        paused: old.state == "paused",
        delta: incremental.then(|| (identities_requested.to_vec(), include)),
    })
}
/// Caller owns collector.lock and desktop.lock. A durable plan survives every
/// partial write; replay is idempotent, with delivery stopped until completion.
pub fn recover(directory: &Path) -> Result<()> {
    let path = directory.join("sharing-change.json");
    if path.exists() {
        apply_plan(directory, serde_json::from_slice(&fs::read(path)?)?)?;
    }
    Ok(())
}
fn apply_plan(directory: &Path, mut plan: ChangePlan) -> Result<()> {
    let conn = db(directory)?;
    if let Some((identities, include)) = &plan.delta {
        // Adoption preserves legacy pending batches, snapshots and retries. It
        // is idempotent across a crash between any two steps of this intent.
        delivery::adopt_session_job(&conn, &plan.config.job_id, &selected(directory)?)?;
        for identity in identities {
            if *include {
                delivery::include_job_session(&conn, &plan.config.job_id, identity)?;
            } else {
                delivery::set_job_session(&conn, &plan.config.job_id, identity, false)?;
            }
        }
        save_json(&directory.join("selected.json"), &plan.selected)?;
        save_json(&directory.join("config.json"), &plan.config)?;
        fs::remove_file(directory.join("sharing-change.json"))?;
        return Ok(());
    }
    for job in delivery::list_jobs(&conn)? {
        if job.state != "cancelled"
            && job.config.destination_id == plan.job.destination_id
            && job.config.instance_id == plan.job.instance_id
            && job.config.account_id == plan.job.account_id
        {
            delivery::cancel_job(&conn, &job.job_id)?;
        }
    }
    let wanted: HashSet<_> = plan.excluded.iter().cloned().collect();
    for id in excluded(&conn)? {
        if !wanted.contains(&id) {
            delivery::set_session_excluded(&conn, &id, false)?;
        }
    }
    let current = excluded(&conn)?;
    for id in wanted.difference(&current) {
        delivery::set_session_excluded(&conn, id, true)?;
    }
    let job = if mode(&plan.config) == SharingMode::Selected {
        let job = delivery::create_session_job(&conn, &plan.job, collector::now())?;
        for identity in &plan.selected {
            delivery::set_job_session(&conn, &job.job_id, identity, true)?;
        }
        job
    } else {
        delivery::create_job(&conn, &plan.job, collector::now())?
    };
    if plan.paused {
        delivery::pause_job(&conn, &job.job_id)?;
    }
    plan.config.job_id = job.job_id;
    save_json(&directory.join("selected.json"), &plan.selected)?;
    save_json(&directory.join("config.json"), &plan.config)?;
    fs::remove_file(directory.join("sharing-change.json"))?;
    Ok(())
}

pub fn disconnect(directory: &Path) -> Result<()> {
    let _control = control_lock(directory)?;
    collector::stop(directory)?;
    let _guard = lock(directory)?;
    let config = read_config(directory)?;
    let conn = db(directory)?;
    for job in delivery::list_jobs(&conn)? {
        if job.state != "cancelled" {
            delivery::cancel_job(&conn, &job.job_id)?;
        }
    }
    std::env::set_var("RELAYHISTORY_HOME", directory);
    if let Ok(Some(auth)) = relayhistory_plugin::cloud::load_auth(Some(&config.history_url)) {
        let token = auth.refresh_token.as_deref().unwrap_or(&auth.access_token);
        let _ = ureq::AgentBuilder::new()
            .timeout(Duration::from_secs(5))
            .redirects(0)
            .build()
            .post(&format!("{}/v1/auth/token/revoke", config.history_url))
            .send_json(json!({"token":token}));
    }
    let stages = directory.join("stages");
    if stages.exists() {
        fs::remove_dir_all(stages)?;
    }
    for name in ["sharing-change.json", "selected.json", "config.json"] {
        let path = directory.join(name);
        if path.exists() {
            fs::remove_file(path)?;
        }
    }
    emit(json!({"disconnected":true}));
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn preview_contains_titles_without_upload_permission_or_transcript_data() {
        let response = super::preview_response(&[ai_hist::ShallowSession {
            source: "codex".into(),
            session_id: "one".into(),
            first_prompt: Some("A local title".into()),
            last_assistant_text: Some("Do not expose transcript bodies".into()),
            raw_path: Some("/private/transcript.jsonl".into()),
            last_activity_ms: Some(1234),
            ..Default::default()
        }]);
        let row = &response["sessions"][0];
        assert_eq!(row["title"], "A local title");
        assert_eq!(row["included"], false);
        assert_eq!(row["status"], "unknown");
        assert_eq!(row["last_activity_ms"], 1234);
        assert!(row.get("last_assistant_text").is_none());
        assert!(row.get("raw_path").is_none());
    }

    use super::*;
    use clap::Parser;

    fn fixture(mode: SharingMode) -> (tempfile::TempDir, Config) {
        let dir = tempfile::tempdir().unwrap();
        let conn = db(dir.path()).unwrap();
        conn.execute("INSERT INTO sessions(source,session_id,first_prompt,last_activity_ms) VALUES ('claude','old','Fix authentication',10),('codex','recent','Add tests',20)", []).unwrap();
        let job = DeliveryJobConfig {
            destination_id: "relayhistory".into(),
            instance_id: "teams-probe".into(),
            account_id: relayhistory_plugin::destination::account_id("org", Some("workspace")),
            mapping_version: relayhistory_plugin::destination::MAPPING_VERSION.into(),
            selection: collector::selection(mode == SharingMode::All),
            limits: Default::default(),
        };
        if mode != SharingMode::All {
            collector::record_baseline(&conn, false).unwrap();
        }
        let job = delivery::create_job(&conn, &job, collector::now()).unwrap();
        let config = Config {
            version: 1,
            site_url: "https://agentrelay.com".into(),
            account_id: "account".into(),
            account_email: Some("person@example.com".into()),
            account_name: Some("Example Person".into()),
            account_avatar_url: Some("https://example.com/avatar.png".into()),
            org_id: "org".into(),
            workspace_id: "workspace".into(),
            history_url: "https://history.agentrelay.com".into(),
            delivery_account: job.config.account_id,
            job_id: job.job_id,
            include_existing: mode == SharingMode::All,
            sharing_mode: Some(mode),
            acknowledge_uninspected_schedules: false,
        };
        save_json(&dir.path().join("config.json"), &config).unwrap();
        (dir, config)
    }
    fn apply(
        dir: &Path,
        config: Config,
        mode: Option<SharingMode>,
        keys: &[&str],
        include: bool,
    ) -> Config {
        let conn = db(dir).unwrap();
        let keys: Vec<_> = keys.iter().map(|key| parse_key(key).unwrap()).collect();
        let plan = make_plan(&conn, dir, config, mode, &keys, include).unwrap();
        save_json(&dir.join("sharing-change.json"), &plan).unwrap();
        recover(dir).unwrap();
        read_config(dir).unwrap()
    }

    #[test]
    fn desktop_summary_includes_the_signed_in_profile() {
        let (dir, config) = fixture(SharingMode::Selected);
        let value = summary(dir.path(), &config).unwrap();
        assert_eq!(value["account_email"], "person@example.com");
        assert_eq!(value["account_name"], "Example Person");
        assert_eq!(
            value["account_avatar_url"],
            "https://example.com/avatar.png"
        );
    }

    #[test]
    fn selected_status_counts_only_known_members_not_globally_excluded() {
        for fresh in [true, false] {
            let (dir, mut config) = fixture(if fresh {
                SharingMode::All
            } else {
                SharingMode::Selected
            });
            let conn = db(dir.path()).unwrap();
            if fresh {
                let old = delivery::status(&conn, &config.job_id).unwrap();
                delivery::cancel_job(&conn, &config.job_id).unwrap();
                config.job_id = delivery::create_session_job(&conn, &old.config, collector::now())
                    .unwrap()
                    .job_id;
                config.sharing_mode = Some(SharingMode::Selected);
                config.include_existing = false;
                save_json(&dir.path().join("config.json"), &config).unwrap();
            }
            config = apply(dir.path(), config, None, &["claude:old"], true);
            conn.execute(
                "INSERT INTO sessions(source,session_id) VALUES ('codex','private-discovery')",
                [],
            )
            .unwrap();
            delivery::set_job_session(
                &conn,
                &config.job_id,
                &parse_key("claude:not-in-catalog").unwrap(),
                true,
            )
            .unwrap();
            assert_eq!(
                status_value(dir.path()).unwrap()["sessions"],
                json!({"total":3,"shared":1,"excluded":2})
            );
            let rows = session_rows(dir.path(), &config, 500).unwrap();
            assert_eq!(rows.iter().filter(|r| r["included"] == true).count(), 1);
            delivery::set_session_excluded(&conn, &parse_key("claude:old").unwrap(), true).unwrap();
            assert_eq!(
                status_value(dir.path()).unwrap()["sessions"],
                json!({"total":3,"shared":0,"excluded":3})
            );
            assert!(session_rows(dir.path(), &config, 500)
                .unwrap()
                .iter()
                .all(|r| r["included"] == false));
        }
    }

    #[test]
    fn status_keeps_exclusion_counts_for_all_new_and_legacy_jobs() {
        for mode in [SharingMode::All, SharingMode::New, SharingMode::Selected] {
            let (dir, config) = fixture(mode);
            let conn = db(dir.path()).unwrap();
            assert!(!delivery::is_session_job(&conn, &config.job_id).unwrap());
            let shared = if mode == SharingMode::All { 2 } else { 0 };
            assert_eq!(
                status_value(dir.path()).unwrap()["sessions"],
                json!({"total":2,"shared":shared,"excluded":2-shared})
            );
        }
    }

    #[test]
    fn scoped_control_does_not_rebuild_catalog_exclusions_or_other_backfills() {
        for count in [100, 10_000, 50_000] {
            let (dir, mut config) = fixture(SharingMode::Selected);
            let conn = db(dir.path()).unwrap();
            let old = delivery::status(&conn, &config.job_id).unwrap();
            delivery::cancel_job(&conn, &config.job_id).unwrap();
            conn.execute("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<?1) INSERT INTO sessions(source,session_id) SELECT 'codex','unrelated-'||x FROM n",[count]).unwrap();
            conn.execute(
                "INSERT INTO sessions(source,session_id) VALUES ('claude','last')",
                [],
            )
            .unwrap();
            config.job_id = delivery::create_session_job(&conn, &old.config, collector::now())
                .unwrap()
                .job_id;
            save_json(&dir.path().join("config.json"), &config).unwrap();
            let started = std::time::Instant::now();
            config = apply(dir.path(), config, None, &["claude:old"], true);
            let a = delivery::status(&conn, &config.job_id).unwrap();
            let pending = delivery::prepare_batch(&conn, &config.job_id, collector::now())
                .unwrap()
                .batch_id
                .unwrap();
            config = apply(dir.path(), config, None, &["claude:last"], true);
            assert_eq!(config.job_id, a.job_id);
            assert_eq!(
                delivery::prepare_batch(&conn, &config.job_id, collector::now())
                    .unwrap()
                    .batch_id,
                Some(pending)
            );
            let before = conn
                .query_row("SELECT count(*) FROM delivery_exclusions", [], |r| {
                    r.get::<_, i64>(0)
                })
                .unwrap();
            change(dir.path(), None, &["claude:old".into()], true).unwrap();
            assert_eq!(
                delivery::job_sessions(&conn, &config.job_id).unwrap().len(),
                2
            );
            assert_eq!(
                conn.query_row("SELECT count(*) FROM delivery_exclusions", [], |r| r
                    .get::<_, i64>(0))
                    .unwrap(),
                before
            );
            assert_eq!(before, 1); // only the fixture's legacy unselected codex row
            eprintln!("probe-selected unrelated={count} two_inclusions_repeat_ms={:.3} exclusions={before}",started.elapsed().as_secs_f64()*1000.);
        }
    }

    #[test]
    fn repeated_include_rebaselines_a_globally_excluded_member() {
        let (dir, config) = fixture(SharingMode::Selected);
        let config = apply(dir.path(), config, None, &["claude:old"], true);
        let conn = db(dir.path()).unwrap();
        let identity = parse_key("claude:old").unwrap();
        let cutoff = || {
            conn.query_row("SELECT cutoff FROM delivery_session_members WHERE job_id=? AND source='claude' AND session_id='old'", [&config.job_id], |r| r.get::<_,i64>(0)).unwrap()
        };
        let original = cutoff();
        delivery::set_session_excluded(&conn, &identity, true).unwrap();
        change(dir.path(), None, &["claude:old".into()], true).unwrap();
        assert!(delivery::job_session_included(&conn, &config.job_id, &identity).unwrap());
        assert!(!excluded(&conn).unwrap().contains(&identity));
        assert!(cutoff() > original);
        assert!(!dir.path().join("sharing-change.json").exists());
    }

    #[test]
    fn partial_member_intent_replay_keeps_same_job_and_selected_privacy() {
        let (dir, config) = fixture(SharingMode::Selected);
        let config = apply(dir.path(), config, None, &["claude:old"], true);
        let conn = db(dir.path()).unwrap();
        let plan = make_plan(
            &conn,
            dir.path(),
            config.clone(),
            None,
            &[parse_key("codex:recent").unwrap()],
            true,
        )
        .unwrap();
        save_json(&dir.path().join("sharing-change.json"), &plan).unwrap();
        delivery::set_session_excluded(&conn, &parse_key("codex:recent").unwrap(), false).unwrap();
        delivery::set_job_session(
            &conn,
            &config.job_id,
            &parse_key("codex:recent").unwrap(),
            true,
        )
        .unwrap();
        change(dir.path(), None, &["codex:recent".into()], true).unwrap();
        assert_eq!(read_config(dir.path()).unwrap().job_id, config.job_id);
        assert_eq!(selected(dir.path()).unwrap().len(), 2);
        assert_eq!(
            delivery::job_sessions(&conn, &config.job_id).unwrap().len(),
            2
        );
        assert!(!dir.path().join("sharing-change.json").exists());
    }
    #[test]
    fn paused_capture_contention_is_not_an_offline_failure() {
        let (dir, config) = fixture(SharingMode::Selected);
        let conn = db(dir.path()).unwrap();
        delivery::pause_job(&conn, &config.job_id).unwrap();
        let sync_lock = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.path().join("history.db.sync.lock"))
            .unwrap();
        sync_lock.lock_exclusive().unwrap();
        collector::cycle(dir.path(), &config).unwrap();
        assert_eq!(
            delivery::status(&conn, &config.job_id).unwrap().state,
            "paused"
        );
    }

    #[test]
    fn bridge_validation_errors_are_safe_and_actionable() {
        for key in ["private-input", ":id", "codex:"] {
            let error = parse_key(key).unwrap_err();
            let safe = error.downcast_ref::<super::super::UserError>().unwrap();
            assert!(safe.to_string().contains("SOURCE:ID"));
            assert!(!safe.to_string().contains("private-input"));
        }
        let (dir, _) = fixture(SharingMode::Selected);
        let error = change(dir.path(), None, &["codex:private-missing".into()], true).unwrap_err();
        let safe = error.downcast_ref::<super::super::UserError>().unwrap();
        assert!(safe.to_string().contains("Unknown session"));
        assert!(!safe.to_string().contains("private-missing"));
        save_json(&dir.path().join("sharing-change.json"), &json!({})).unwrap();
        let error = pause(dir.path(), true).unwrap_err();
        assert!(error
            .downcast_ref::<super::super::UserError>()
            .unwrap()
            .to_string()
            .contains("Run start"));
    }

    #[test]
    fn every_desktop_command_accepts_json_and_target_flags() {
        let target = ["--account", "account", "--workspace", "workspace", "--json"];
        for name in ["start", "status", "pause", "resume", "disconnect"] {
            assert!(
                super::super::Cli::try_parse_from([vec!["probe", name], target.to_vec()].concat())
                    .is_ok(),
                "{name}"
            );
        }
        for action in ["include", "exclude"] {
            assert!(super::super::Cli::try_parse_from(
                [
                    vec!["probe", "sessions", action, "--session", "codex:a:b"],
                    target.to_vec()
                ]
                .concat()
            )
            .is_ok());
        }
        assert!(super::super::Cli::try_parse_from(
            [
                vec!["probe", "sessions", "list", "--limit", "1000"],
                target.to_vec()
            ]
            .concat()
        )
        .is_ok());
        assert!(super::super::Cli::try_parse_from(
            [
                vec!["probe", "sharing", "set", "--mode", "selected"],
                target.to_vec()
            ]
            .concat()
        )
        .is_ok());
        assert!(super::super::Cli::try_parse_from(["probe", "installs", "--json"]).is_ok());
        for other in ["--include-existing", "--new-sessions-only"] {
            assert!(super::super::Cli::try_parse_from([
                "probe",
                "cloud",
                "install",
                "--selected-sessions-only",
                other
            ])
            .is_err());
        }
    }
    #[test]
    fn selected_mode_withholds_new_discoveries_but_keeps_explicit_choices() {
        let (dir, config) = fixture(SharingMode::Selected);
        let config = apply(dir.path(), config, None, &["claude:old"], true);
        let conn = db(dir.path()).unwrap();
        conn.execute(
            "INSERT INTO sessions(source,session_id) VALUES ('codex','later')",
            [],
        )
        .unwrap();
        enforce_selection(dir.path(), &config).unwrap();
        let withheld = excluded(&conn).unwrap();
        assert!(!delivery::job_session_included(
            &conn,
            &config.job_id,
            &parse_key("codex:later").unwrap()
        )
        .unwrap());
        assert!(!withheld.contains(&parse_key("claude:old").unwrap()));
        assert_eq!(
            selected(dir.path()).unwrap(),
            vec![parse_key("claude:old").unwrap()]
        );
    }
    #[test]
    fn drain_rechecks_selected_mode_after_a_failed_capture() {
        let (dir, config) = fixture(SharingMode::Selected);
        let conn = db(dir.path()).unwrap();
        conn.execute(
            "INSERT INTO sessions(source,session_id) VALUES ('codex','discovered')",
            [],
        )
        .unwrap();
        assert!(!excluded(&conn)
            .unwrap()
            .contains(&parse_key("codex:discovered").unwrap()));
        // No destination credentials are installed in this fixture. Selection
        // must be enforced before the drain reaches any transport setup.
        let _ = collector::deliver_captured(dir.path(), &config, false);
        assert!(delivery::is_session_job(&conn, &config.job_id).unwrap());
        assert!(!delivery::job_session_included(
            &conn,
            &config.job_id,
            &parse_key("codex:discovered").unwrap()
        )
        .unwrap());
    }
    #[test]
    fn including_a_session_adopts_in_place_and_preserves_pause() {
        let (dir, config) = fixture(SharingMode::Selected);
        let old = config.job_id.clone();
        let conn = db(dir.path()).unwrap();
        delivery::pause_job(&conn, &old).unwrap();
        let config = apply(dir.path(), config, None, &["claude:old"], true);
        assert_eq!(config.job_id, old);
        assert!(delivery::is_session_job(&conn, &old).unwrap());
        assert_eq!(
            delivery::status(&conn, &config.job_id).unwrap().state,
            "paused"
        );
        // Paused delivery must not try to load credentials or contact a service.
        collector::deliver_captured(dir.path(), &config, false).unwrap();
        let rows = session_rows(dir.path(), &config, 500).unwrap();
        assert_eq!(
            rows.iter().find(|r| r["session_id"] == "old").unwrap()["included"],
            true
        );
    }
    #[test]
    fn mode_changes_and_exclusions_survive_restart() {
        let (dir, config) = fixture(SharingMode::All);
        let config = apply(dir.path(), config, Some(SharingMode::New), &[], true);
        assert_eq!(excluded(&db(dir.path()).unwrap()).unwrap().len(), 2);
        let config = apply(
            dir.path(),
            config,
            None,
            &["claude:old", "codex:recent"],
            true,
        );
        assert!(excluded(&db(dir.path()).unwrap()).unwrap().is_empty());
        let config = apply(dir.path(), config, None, &["claude:old"], false);
        let config = apply(dir.path(), config, Some(SharingMode::Selected), &[], true);
        assert_eq!(
            selected(dir.path()).unwrap(),
            vec![parse_key("codex:recent").unwrap()]
        );
        assert_eq!(excluded(&db(dir.path()).unwrap()).unwrap().len(), 1);
        let config = apply(dir.path(), config, Some(SharingMode::All), &[], true);
        assert!(excluded(&db(dir.path()).unwrap()).unwrap().is_empty());
        assert_eq!(mode(&config), SharingMode::All);
    }
    #[test]
    fn interrupted_regeneration_replays_without_widening_selection() {
        let (dir, config) = fixture(SharingMode::All);
        let conn = db(dir.path()).unwrap();
        let plan = make_plan(
            &conn,
            dir.path(),
            config.clone(),
            Some(SharingMode::Selected),
            &[],
            true,
        )
        .unwrap();
        save_json(&dir.path().join("sharing-change.json"), &plan).unwrap();
        delivery::cancel_job(&conn, &config.job_id).unwrap();
        // Simulate interruption after cancellation, and again after new-job
        // creation/config commit but before removing the durable intent.
        recover(dir.path()).unwrap();
        save_json(&dir.path().join("sharing-change.json"), &plan).unwrap();
        recover(dir.path()).unwrap();
        let config = read_config(dir.path()).unwrap();
        assert_eq!(mode(&config), SharingMode::Selected);
        assert_eq!(excluded(&conn).unwrap().len(), 2);
        assert_eq!(
            delivery::list_jobs(&conn)
                .unwrap()
                .iter()
                .filter(|j| j.state != "cancelled")
                .count(),
            1
        );
        assert!(!dir.path().join("sharing-change.json").exists());
    }
    #[test]
    fn restart_replays_pending_change_before_child_launch() {
        let (dir, config) = fixture(SharingMode::All);
        let conn = db(dir.path()).unwrap();
        let plan = make_plan(
            &conn,
            dir.path(),
            config.clone(),
            Some(SharingMode::Selected),
            &[],
            true,
        )
        .unwrap();
        save_json(&dir.path().join("sharing-change.json"), &plan).unwrap();
        delivery::cancel_job(&conn, &config.job_id).unwrap();

        assert!(prepare_restart(dir.path()).unwrap());

        assert!(!dir.path().join("sharing-change.json").exists());
        let recovered = read_config(dir.path()).unwrap();
        assert_eq!(mode(&recovered), SharingMode::Selected);
        assert_eq!(excluded(&conn).unwrap().len(), 2);
        assert_eq!(
            delivery::status(&conn, &recovered.job_id).unwrap().state,
            "active"
        );
    }
    #[test]
    fn unknown_selection_is_rejected_before_any_generation_change() {
        let (dir, config) = fixture(SharingMode::Selected);
        let conn = db(dir.path()).unwrap();
        let collector_lock = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.path().join("collector.lock"))
            .unwrap();
        collector_lock.lock_exclusive().unwrap();
        assert!(collector::running(dir.path()).unwrap());
        let error = change(dir.path(), None, &["claude:missing".into()], true).unwrap_err();
        assert!(error.to_string().contains("Unknown session"));
        assert!(!dir.path().join("stop.json").exists());
        assert!(!dir.path().join("sharing-change.json").exists());
        assert_eq!(
            delivery::status(&conn, &config.job_id).unwrap().state,
            "active"
        );
    }
    #[test]
    fn changing_a_stopped_install_does_not_start_a_collector() {
        let (dir, _) = fixture(SharingMode::Selected);
        assert!(!collector::running(dir.path()).unwrap());
        change(dir.path(), None, &["claude:old".into()], true).unwrap();
        assert!(!collector::running(dir.path()).unwrap());
        assert!(!dir.path().join("runtime.json").exists());
    }
    #[test]
    fn leased_batch_wins_over_pending_for_the_same_session() {
        let id = parse_key("claude:old").unwrap();
        for order in [[false, true], [true, false]] {
            let mut pending = HashMap::new();
            for leased in order {
                mark_pending(&mut pending, id.clone(), leased);
            }
            assert_eq!(pending.get(&id), Some(&true));
        }
    }
    #[test]
    fn queued_status_identifies_actual_pending_batch_sessions() {
        let (dir, config) = fixture(SharingMode::All);
        let conn = db(dir.path()).unwrap();
        for _ in 0..50 {
            if delivery::prepare_batch(&conn, &config.job_id, collector::now())
                .unwrap()
                .batch_id
                .is_some()
            {
                break;
            }
        }
        let rows = session_rows(dir.path(), &config, 500).unwrap();
        assert!(rows.iter().any(|r| r["status"] == "queued"));
        let claim = delivery::claim_batch(
            &conn,
            &config.job_id,
            "desktop-test",
            60_000,
            &collector::now,
        )
        .unwrap();
        assert!(claim.is_some());
        assert!(session_rows(dir.path(), &config, 500)
            .unwrap()
            .iter()
            .any(|r| r["status"] == "uploading"));
        assert_eq!(
            session_rows(dir.path(), &config, 1).unwrap()[0]["session_id"],
            "recent"
        );
    }
    #[test]
    fn legacy_config_retains_its_sharing_choice() {
        let (_, config) = fixture(SharingMode::New);
        let mut value = serde_json::to_value(config).unwrap();
        value.as_object_mut().unwrap().remove("sharing_mode");
        assert_eq!(
            mode(&serde_json::from_value(value).unwrap()),
            SharingMode::New
        );
    }
}
