use ai_hist_core::convergence::MachineIdentity;
use ai_hist_core::{
    default_db_path, export_json, import_json, list_tags, open_db, recent, resume_command, search,
    session, stats, sync_opencode_db, tag_session, untag_session, HistoryEntry, QueryFilter,
};
use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::collections::HashSet;
use std::fs;
use std::io::{self, Read};
use std::path::PathBuf;

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
        json: bool,
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
        json: bool,
    },
    Resume {
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
    Export {
        output: Option<PathBuf>,
    },
    Import {
        input: Option<PathBuf>,
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
            print_entries(rows, json)
        }
        Command::Recent {
            n,
            source,
            project,
            tag,
            json,
        } => {
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
            json,
        } => {
            let rows = session(&conn, &session_id, source.as_deref(), tag.as_deref())?;
            print_entries(rows, json)
        }
        Command::Stats { tag, json } => {
            let s = stats(&conn, tag.as_deref())?;
            if json {
                println!("{}", serde_json::to_string(&s)?);
            } else {
                println!("Total entries: {}", s.total);
                println!("By source:");
                for (source, count) in s.by_source {
                    println!("  {source}: {count}");
                }
                println!("Top projects:");
                for (project, count) in s.by_project {
                    println!("  {count:>6}  {project}");
                }
            }
            Ok(())
        }
        Command::Tag {
            session_id,
            tag_name,
            source,
            color,
            json,
        } => {
            let sessions = tag_session(
                &conn,
                &session_id,
                &tag_name,
                source.as_deref(),
                color.as_deref(),
            )?;
            if json {
                println!("{}", serde_json::to_string(&sessions)?);
            } else if sessions.is_empty() {
                anyhow::bail!("No session found for {session_id}");
            } else {
                println!("Tagged {} session(s) with '{}'.", sessions.len(), tag_name);
            }
            Ok(())
        }
        Command::Untag {
            session_id,
            tag_name,
            source,
            json,
        } => {
            let removed = untag_session(&conn, &session_id, &tag_name, source.as_deref())?;
            if json {
                println!("{}", serde_json::json!({ "removed_assignments": removed }));
            } else {
                println!("Removed tag '{tag_name}' from {removed} session assignment(s).");
            }
            Ok(())
        }
        Command::Tags { json } => {
            let tags = list_tags(&conn)?;
            if json {
                println!("{}", serde_json::to_string(&tags)?);
            } else {
                for tag in tags {
                    println!("  {}  {} session(s)", tag.display_name, tag.session_count);
                }
            }
            Ok(())
        }
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
                    println!(
                        "{}",
                        serde_json::json!({ "entry": entry, "resume_cmd": cmd })
                    );
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
        Command::Export { output } => {
            let rows = export_json(&conn)?;
            let body = rows
                .into_iter()
                .map(|row| serde_json::to_string(&row))
                .collect::<Result<Vec<_>, _>>()?
                .join("\n")
                + "\n";
            if let Some(output) = output {
                fs::write(output, body)?;
            } else {
                print!("{body}");
            }
            Ok(())
        }
        Command::Import { input } => {
            let mut body = String::new();
            if let Some(input) = input {
                body = fs::read_to_string(input)?;
            } else {
                io::stdin().read_to_string(&mut body)?;
            }
            let entries = body
                .lines()
                .filter(|line| !line.trim().is_empty())
                .map(serde_json::from_str::<HistoryEntry>)
                .collect::<Result<Vec<_>, _>>()
                .context("parsing JSONL import")?;
            let inserted = import_json(&conn, &entries)?;
            println!("Imported {inserted} entries.");
            Ok(())
        }
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
            let auth = cloud::load_auth()?.context(
                "not authenticated — run `ai-hist login` or `ai-hist admin-mint` first",
            )?;
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
                    report.sent, report.accepted, report.cursor.history_id, report.cursor.trajectory_rowid
                );
            }
            Ok(())
        }
    }
}

fn print_entries(rows: Vec<HistoryEntry>, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string(&rows)?);
        return Ok(());
    }
    for row in rows {
        println!(
            "  #{:<5} {} ({}){}  {}",
            row.id,
            row.timestamp_ms,
            row.source,
            row.project
                .as_ref()
                .map(|p| format!(" [{p}]"))
                .unwrap_or_default(),
            row.prompt.replace('\n', " ")
        );
    }
    Ok(())
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
