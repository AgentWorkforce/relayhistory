use ai_hist_core::{
    default_db_path, import_json, insert_history, normalize_tag_name, open_db, parse_cursor_text,
    prompt_hash, recent, resume_command, search, session, sync_opencode_db, untag_session,
    HistoryEntry, QueryFilter, SOURCE_CHOICES,
};
use ai_hist_core::convergence::MachineIdentity;
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

#[derive(Parser)]
#[command(
    name = "ai-hist",
    version,
    about = "Rust ai-hist CLI, parallel to the Python CLI"
)]
struct Cli {
    #[arg(long)]
    db: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Search {
        #[arg(required = true)]
        query: Vec<String>,
        #[arg(long)]
        source: Option<String>,
        #[arg(long)]
        project: Option<String>,
        #[arg(long)]
        tag: Option<String>,
        #[arg(long, default_value_t = 20)]
        limit: i64,
        #[arg(long)]
        fts: bool,
        #[arg(long)]
        json: bool,
    },
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
    Show {
        id: i64,
        #[arg(long)]
        json: bool,
    },
    Context {
        id: i64,
        #[arg(long, default_value_t = 5)]
        window: i64,
    },
    Stats {
        #[arg(long)]
        tag: Option<String>,
        #[arg(long)]
        json: bool,
    },
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
    Untag {
        session_id: String,
        tag_name: String,
        #[arg(long)]
        source: Option<String>,
        #[arg(long)]
        json: bool,
    },
    Tags {
        #[arg(long)]
        tag: Option<String>,
        #[arg(long)]
        sessions: bool,
        #[arg(long)]
        json: bool,
    },
    Resume {
        #[arg(required = true)]
        query: Vec<String>,
        #[arg(long)]
        fts: bool,
        #[arg(long)]
        json: bool,
    },
    SyncOpencode {
        #[arg(long)]
        opencode_db: Option<PathBuf>,
    },
    Sync,
    Watch {
        #[arg(long, default_value_t = 60)]
        interval: u64,
    },
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
    Export {
        output: Option<PathBuf>,
        #[arg(long, default_value = "jsonl")]
        format: String,
        #[arg(long)]
        source: Option<String>,
        #[arg(long)]
        project: Option<String>,
        #[arg(long)]
        since: Option<String>,
    },
    Import {
        file: PathBuf,
        #[arg(long)]
        dry_run: bool,
    },
    /// Authenticate to relayhistory-cloud (Agent Relay Loop) via a RelayAuth token.
    Login {
        #[arg(long)]
        base_url: String,
        /// RelayAuth/Agent Relay token (device-flow JWT).
        #[arg(long)]
        token: String,
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
            limit,
            fts,
            json,
        } => {
            validate_source(source.as_deref())?;
            let rows = search(
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
            )?;
            if rows.is_empty() {
                if json {
                    println!("[]");
                } else {
                    println!("No results.");
                }
                std::process::exit(1);
            }
            print_entries(rows, json)
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
        Command::Sync => sync_basic(&conn, &db_path),
        Command::Watch { interval } => watch_loop(&db_path, interval),
        Command::Export {
            output,
            format,
            source,
            project,
            since,
        } => {
            validate_source(source.as_deref())?;
            export_history(
                &conn,
                output.as_deref(),
                &format,
                source.as_deref(),
                project.as_deref(),
                since.as_deref(),
            )
        }
        Command::Import { file, dry_run } => import_history(&conn, &file, dry_run),
        Command::Login {
            base_url,
            token,
            label,
        } => {
            let auth = cloud::login(&base_url, &token, &label)?;
            cloud::save_auth(&auth)?;
            println!("Logged in to {} (token stored).", auth.base_url);
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
        } => {
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
                let cwd = std::env::current_dir().ok().map(|p| p.display().to_string());
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

/// Best-effort `git remote get-url origin` for project scoping (None if not a repo).
fn detect_git_remote() -> Option<String> {
    let out = std::process::Command::new("git")
        .args(["config", "--get", "remote.origin.url"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let url = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (!url.is_empty()).then_some(url)
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
        if session_state.get(&key).and_then(Value::as_str) == Some(stamp.as_str()) {
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
            upserted += 1;
        }
    }
    state.insert("claude_sessions".to_string(), Value::Object(session_state));
    if scanned > 0 {
        println!("  [claude-sessions] scanned {scanned} files, {upserted} sessions updated");
    }
    Ok(())
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
        let search_root = if root.file_name().and_then(|s| s.to_str()) == Some("compacted") {
            root.parent().unwrap_or(&root).to_path_buf()
        } else {
            root
        };
        let mut compacted = Vec::new();
        collect_named_dirs(&search_root, "compacted", &mut compacted)?;
        for dir in compacted {
            for entry in fs::read_dir(dir)? {
                let path = entry?.path();
                if path.extension().and_then(|s| s.to_str()) == Some("json")
                    && path.file_name().and_then(|s| s.to_str()) != Some("index.json")
                    && path.file_name().and_then(|s| s.to_str()) != Some(".sync-state.json")
                {
                    files.push(path);
                }
            }
        }
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
    if map.get("type").and_then(Value::as_str) == Some("compacted")
        && map
            .get("sourceTrajectories")
            .and_then(Value::as_array)
            .is_some()
        && !["task", "retrospective", "personaId", "projectId"]
            .iter()
            .any(|key| map.contains_key(*key))
    {
        return Ok(None);
    }
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
        retrospective_json: serde_json::to_string(retrospective.unwrap_or(&Map::new()))?,
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
