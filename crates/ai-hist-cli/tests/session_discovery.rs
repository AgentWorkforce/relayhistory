//! End-to-end contract tests for `ai-hist sessions list` / `sessions discover`.
//!
//! These drive the real binary against an isolated HOME (the pattern
//! `sync_resilience.rs` established) so the CLI output contract — the JSON
//! shapes a desktop app parses — is asserted from outside the library.

use ai_hist_core::open_db;
use serde_json::Value;
use std::fs;
use std::path::Path;
use std::process::Command;

fn isolated(temp: &tempfile::TempDir, db_path: &Path, args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ai-hist"));
    command
        .arg("--db")
        .arg(db_path)
        .args(args)
        .env("HOME", temp.path())
        .env("USERPROFILE", temp.path())
        .env("XDG_DATA_HOME", temp.path().join("xdg"))
        .env("OPENCODE_DB", temp.path().join("opencode.db"))
        .env_remove("AI_HIST_DB")
        .env_remove("RELAYCAST_API_KEY")
        .env_remove("RELAYCAST_WORKSPACE_ID")
        .env_remove("RELAYHISTORY_CLAUDE_API_BASE_URL")
        .env_remove("RELAYHISTORY_CLAUDE_CREDENTIALS");
    command
}

fn write(path: &Path, contents: &str, mtime_ms: u64) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, contents).unwrap();
    // Recency ordering is driven by mtime, so pin it rather than letting the
    // order the fixtures happened to be written decide the assertions.
    let file = fs::OpenOptions::new().write(true).open(path).unwrap();
    let when = std::time::UNIX_EPOCH + std::time::Duration::from_millis(mtime_ms);
    file.set_times(fs::FileTimes::new().set_modified(when))
        .unwrap();
}

fn fake_home() -> tempfile::TempDir {
    let temp = tempfile::tempdir().unwrap();
    write(
        &temp.path().join(".claude/projects/app/claude-cli.jsonl"),
        concat!(
            r#"{"sessionId":"claude-cli","cwd":"/work/app","gitBranch":"main","version":"1.2.3","type":"user","message":{"role":"user","content":"first human prompt"},"timestamp":"2026-06-20T10:00:00.000Z"}"#,
            "\n",
            r#"{"sessionId":"claude-cli","type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"ack"}]},"timestamp":"2026-06-20T10:05:00.000Z"}"#,
            "\n"
        ),
        1_750_000_000_000,
    );
    write(
        &temp
            .path()
            .join(".codex/sessions/2026/06/21/rollout-codex-cli.jsonl"),
        concat!(
            r#"{"timestamp":"2026-06-21T11:00:00.000Z","type":"session_meta","payload":{"id":"codex-cli","cwd":"/work/api","originator":"codex_cli_rs","cli_version":"0.148.0","git":{"branch":"dev"}}}"#,
            "\n",
            r#"{"timestamp":"2026-06-21T11:00:03.000Z","type":"event_msg","payload":{"type":"user_message","message":"add a retry"}}"#,
            "\n"
        ),
        1_750_000_100_000,
    );
    temp
}

fn jsonl(stdout: &str) -> Vec<Value> {
    stdout
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str::<Value>(line).expect("each discover line is JSON"))
        .collect()
}

#[test]
fn discover_streams_jsonl_rows_then_a_summary() {
    let temp = fake_home();
    let db_path = temp.path().join("history.db");

    let output = isolated(&temp, &db_path, &["sessions", "discover", "--json"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let lines = jsonl(&String::from_utf8_lossy(&output.stdout));
    let sessions: Vec<&Value> = lines
        .iter()
        .filter(|line| line["type"] == "session")
        .collect();
    assert_eq!(sessions.len(), 2, "{lines:#?}");
    // Newest first, globally across providers.
    assert_eq!(sessions[0]["session_id"], "codex-cli");
    assert_eq!(sessions[0]["source"], "codex");
    assert_eq!(sessions[0]["originator"], "codex_cli_rs");
    assert_eq!(sessions[0]["agent_version"], "0.148.0");
    assert_eq!(sessions[0]["first_prompt"], "add a retry");
    assert_eq!(sessions[0]["discovery_state"], "shallow");
    assert_eq!(sessions[1]["session_id"], "claude-cli");
    assert_eq!(sessions[1]["cwd"], "/work/app");
    assert_eq!(sessions[1]["repo_url"], Value::Null);

    let summary = lines.last().expect("a summary line");
    assert_eq!(summary["type"], "summary");
    assert_eq!(summary["contract_version"], 3);
    assert_eq!(summary["scope"], "local");
    assert_eq!(summary["discovered"], 2);
    assert_eq!(summary["skipped_unchanged"], 0);
    assert_eq!(summary["providers"]["claude"]["discovered"], 1);
    assert_eq!(summary["providers"]["codex"]["discovered"], 1);
    assert_eq!(summary["exempt_sources"][0]["source"], "trajectory");
}

#[test]
fn a_global_limit_is_shared_across_providers() {
    let temp = fake_home();
    let db_path = temp.path().join("history.db");
    let output = isolated(
        &temp,
        &db_path,
        &["sessions", "discover", "--json", "--limit", "1"],
    )
    .output()
    .unwrap();
    assert!(output.status.success());
    let lines = jsonl(&String::from_utf8_lossy(&output.stdout));
    let sessions: Vec<&Value> = lines
        .iter()
        .filter(|line| line["type"] == "session")
        .collect();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0]["session_id"], "codex-cli");
    let summary = lines.last().unwrap();
    assert_eq!(summary["counters"]["shallow_reads"], 1);
    assert_eq!(summary["counters"]["candidates_enumerated"], 2);
}

#[test]
fn list_serves_the_catalog_after_the_provider_files_are_gone() {
    let temp = fake_home();
    let db_path = temp.path().join("history.db");
    assert!(isolated(&temp, &db_path, &["sessions", "discover"])
        .output()
        .unwrap()
        .status
        .success());

    // A cache-only list must not depend on any provider file existing.
    fs::remove_dir_all(temp.path().join(".claude")).unwrap();
    fs::remove_dir_all(temp.path().join(".codex")).unwrap();

    let output = isolated(&temp, &db_path, &["sessions", "list", "--json"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["contract_version"], 3);
    assert_eq!(payload["scope"], "local");
    let sessions = payload["sessions"].as_array().unwrap();
    assert_eq!(sessions.len(), 2);
    assert_eq!(sessions[0]["session_id"], "codex-cli");
    assert_eq!(sessions[1]["session_id"], "claude-cli");
    assert!(sessions
        .iter()
        .all(|row| row["locations"] == serde_json::json!(["local"])));
    assert!(sessions.iter().all(|row| row["from_cache"] == true));

    // A page that did not fill its limit ends the walk.
    assert_eq!(payload["next_cursor"], Value::Null);

    let filtered = isolated(
        &temp,
        &db_path,
        &["sessions", "list", "--json", "--source", "claude"],
    )
    .output()
    .unwrap();
    let payload: Value = serde_json::from_slice(&filtered.stdout).unwrap();
    assert_eq!(payload["sessions"].as_array().unwrap().len(), 1);
    assert_eq!(payload["sessions"][0]["source"], "claude");
}

#[test]
fn scope_flags_default_to_local_and_all_is_a_deduplicated_union() {
    let temp = fake_home();
    let db_path = temp.path().join("history.db");
    assert!(isolated(&temp, &db_path, &["sessions", "discover"])
        .output()
        .unwrap()
        .status
        .success());

    let list = |scope: Option<&str>| {
        let mut args = vec!["sessions", "list", "--json"];
        if let Some(scope) = scope {
            args.push(scope);
        }
        let output = isolated(&temp, &db_path, &args).output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice::<Value>(&output.stdout).unwrap()
    };

    let implicit = list(None);
    let explicit = list(Some("--local"));
    assert_eq!(implicit, explicit);
    assert_eq!(implicit["scope"], "local");
    assert_eq!(implicit["sessions"].as_array().unwrap().len(), 2);

    let remote = list(Some("--remote"));
    assert_eq!(remote["scope"], "remote");
    assert!(remote["sessions"].as_array().unwrap().is_empty());

    let all = list(Some("--all"));
    assert_eq!(all["scope"], "all");
    assert_eq!(all["sessions"].as_array().unwrap().len(), 2);

    let conflict = isolated(
        &temp,
        &db_path,
        &["sessions", "list", "--local", "--remote"],
    )
    .output()
    .unwrap();
    assert!(!conflict.status.success());
    assert!(String::from_utf8_lossy(&conflict.stderr).contains("cannot be used with"));
}

#[test]
fn explicit_remote_discovery_fails_instead_of_falling_back_to_local() {
    let temp = fake_home();
    let db_path = temp.path().join("history.db");
    let output = isolated(
        &temp,
        &db_path,
        &["sessions", "discover", "--remote", "--json"],
    )
    .output()
    .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("no remote provider connectors are configured"));
    assert!(
        !db_path.exists(),
        "unsupported discovery must not create a ledger"
    );
}

#[test]
fn list_paginates_with_the_cursor_it_hands_back() {
    let temp = fake_home();
    let db_path = temp.path().join("history.db");
    assert!(isolated(&temp, &db_path, &["sessions", "discover"])
        .output()
        .unwrap()
        .status
        .success());

    let first = isolated(
        &temp,
        &db_path,
        &["sessions", "list", "--json", "--limit", "1"],
    )
    .output()
    .unwrap();
    let page: Value = serde_json::from_slice(&first.stdout).unwrap();
    assert_eq!(page["sessions"].as_array().unwrap().len(), 1);
    assert_eq!(page["sessions"][0]["session_id"], "codex-cli");
    let cursor = &page["next_cursor"];
    assert_eq!(cursor["source"], "codex");
    assert_eq!(cursor["session_id"], "codex-cli");
    assert!(cursor["last_activity_ms"].is_i64());

    let second = isolated(
        &temp,
        &db_path,
        &[
            "sessions",
            "list",
            "--json",
            "--limit",
            "1",
            "--after-ms",
            &cursor["last_activity_ms"].as_i64().unwrap().to_string(),
            "--after-source",
            cursor["source"].as_str().unwrap(),
            "--after-session-id",
            cursor["session_id"].as_str().unwrap(),
        ],
    )
    .output()
    .unwrap();
    let page: Value = serde_json::from_slice(&second.stdout).unwrap();
    assert_eq!(page["sessions"].as_array().unwrap().len(), 1);
    assert_eq!(
        page["sessions"][0]["session_id"], "claude-cli",
        "the second page continues where the first stopped"
    );
}

/// A timestamp alone is not a cursor. Accepting one and ignoring it restarted
/// the walk at page one while reporting success, so a paginating client would
/// loop over the first page forever.
#[test]
fn an_incomplete_pagination_cursor_is_rejected_rather_than_ignored() {
    let temp = fake_home();
    let db_path = temp.path().join("history.db");
    assert!(isolated(&temp, &db_path, &["sessions", "discover"])
        .output()
        .unwrap()
        .status
        .success());

    for incomplete in [
        vec!["--after-ms=1782039603000"],
        vec!["--after-source", "codex"],
        vec!["--after-session-id", "codex-cli"],
        vec![
            "--after-ms=1782039603000",
            "--after-session-id",
            "codex-cli",
        ],
    ] {
        let mut args = vec!["sessions", "list", "--json"];
        args.extend(incomplete.iter().copied());
        let output = isolated(&temp, &db_path, &args).output().unwrap();
        assert!(
            !output.status.success(),
            "an incomplete cursor must be rejected: {args:?}"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("required") || stderr.contains("requires"),
            "clap must name the missing flag for {args:?}: {stderr}"
        );
    }

    // The complete triple is still accepted.
    let output = isolated(
        &temp,
        &db_path,
        &[
            "sessions",
            "list",
            "--json",
            "--after-ms=1782039603000",
            "--after-source",
            "codex",
            "--after-session-id",
            "codex-cli",
        ],
    )
    .output()
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn a_negative_limit_is_rejected_rather_than_dumping_the_catalog() {
    let temp = fake_home();
    let db_path = temp.path().join("history.db");
    assert!(isolated(&temp, &db_path, &["sessions", "discover"])
        .output()
        .unwrap()
        .status
        .success());

    // SQLite reads a negative LIMIT as "unlimited".
    let output = isolated(
        &temp,
        &db_path,
        // `--limit=-1` rather than `--limit -1`: clap reads a bare `-1` as a
        // flag, so the equals form is what actually reaches the guard.
        &["sessions", "list", "--json", "--limit=-1"],
    )
    .output()
    .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("--limit must not be negative"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    // `discover --limit` is unsigned, so clap rejects it before it reaches us.
    let discover = isolated(
        &temp,
        &db_path,
        &["sessions", "discover", "--json", "--limit=-1"],
    )
    .output()
    .unwrap();
    assert!(!discover.status.success());
}

#[test]
fn an_every_provider_failure_still_emits_its_diagnostics_and_summary() {
    let temp = fake_home();
    let db_path = temp.path().join("history.db");
    fs::write(temp.path().join("opencode.db"), "definitely not sqlite").unwrap();

    // Only the broken provider is selected, so the whole run fails -- but a
    // JSONL consumer must still receive the reason before the exit code.
    let output = isolated(
        &temp,
        &db_path,
        &["sessions", "discover", "--json", "--source", "opencode"],
    )
    .output()
    .unwrap();
    assert!(
        !output.status.success(),
        "an every-provider failure is an error"
    );
    let lines = jsonl(&String::from_utf8_lossy(&output.stdout));
    let diagnostics: Vec<&Value> = lines
        .iter()
        .filter(|line| line["type"] == "diagnostic")
        .collect();
    assert_eq!(diagnostics.len(), 1, "{lines:#?}");
    assert_eq!(diagnostics[0]["source"], "opencode");
    let summary = lines.last().expect("a summary trailer");
    assert_eq!(summary["type"], "summary");
    assert_eq!(summary["providers"]["opencode"]["failed"], true);
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("no provider made progress"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn list_reads_through_a_handle_that_cannot_contend_with_a_writer() {
    let temp = fake_home();
    let db_path = temp.path().join("history.db");
    assert!(isolated(&temp, &db_path, &["sessions", "discover"])
        .output()
        .unwrap()
        .status
        .success());

    // Hold the write lock: a cache-only list is routed to a read-only handle,
    // so it must complete rather than wait out the busy handler.
    let holder = open_db(&db_path).unwrap();
    holder.execute_batch("BEGIN IMMEDIATE").unwrap();
    let output = isolated(&temp, &db_path, &["sessions", "list", "--json"])
        .output()
        .unwrap();
    holder.execute_batch("ROLLBACK").unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(payload["sessions"].as_array().unwrap().len(), 2);
}

#[test]
fn one_broken_provider_is_a_diagnostic_not_a_failed_run() {
    let temp = fake_home();
    let db_path = temp.path().join("history.db");
    // Not a SQLite database: the opencode adapter must fail on its own.
    fs::write(temp.path().join("opencode.db"), "definitely not sqlite").unwrap();

    let output = isolated(&temp, &db_path, &["sessions", "discover", "--json"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "a single broken provider must not fail the run: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let lines = jsonl(&String::from_utf8_lossy(&output.stdout));
    assert_eq!(
        lines
            .iter()
            .filter(|line| line["type"] == "session")
            .count(),
        2
    );
    let diagnostics: Vec<&Value> = lines
        .iter()
        .filter(|line| line["type"] == "diagnostic")
        .collect();
    assert_eq!(diagnostics.len(), 1, "{lines:#?}");
    assert_eq!(diagnostics[0]["source"], "opencode");
    let summary = lines.last().unwrap();
    assert_eq!(summary["providers"]["opencode"]["failed"], true);
}

#[test]
fn a_rescan_reparses_nothing_and_keeps_one_row_per_session() {
    let temp = fake_home();
    let db_path = temp.path().join("history.db");
    assert!(isolated(&temp, &db_path, &["sessions", "discover"])
        .output()
        .unwrap()
        .status
        .success());

    let output = isolated(&temp, &db_path, &["sessions", "discover", "--json"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let lines = jsonl(&String::from_utf8_lossy(&output.stdout));
    let summary = lines.last().unwrap();
    assert_eq!(summary["skipped_unchanged"], 2);
    assert_eq!(summary["counters"]["shallow_reads"], 0);
    assert_eq!(summary["counters"]["files_opened"], 0);

    let conn = open_db(&db_path).unwrap();
    let rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM sessions", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 2);
}

#[test]
fn discovery_leaves_a_fully_synced_session_marked_full() {
    let temp = fake_home();
    let db_path = temp.path().join("history.db");
    assert!(isolated(&temp, &db_path, &["sync"])
        .output()
        .unwrap()
        .status
        .success());
    assert!(isolated(&temp, &db_path, &["sessions", "discover"])
        .output()
        .unwrap()
        .status
        .success());

    let output = isolated(&temp, &db_path, &["sessions", "list", "--json"])
        .output()
        .unwrap();
    let payload: Value = serde_json::from_slice(&output.stdout).unwrap();
    let claude = payload["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["source"] == "claude")
        .expect("the claude session");
    assert_eq!(
        claude["discovery_state"], "full",
        "a shallow rescan must not downgrade a fully indexed session"
    );
    assert_eq!(
        claude["first_prompt"], "first human prompt",
        "the shallow pass still enriches a fully indexed row"
    );
}

// ---------------------------------------------------------------------------
// remote connectors, end to end
// ---------------------------------------------------------------------------

/// Sign this fake home in to Codex and put a scripted `codex` binary on PATH
/// that answers `cloud list --json`: `first` for the opening page, `next`
/// whenever a `--cursor` is passed. Returns the bin directory to prepend to
/// PATH.
#[cfg(unix)]
fn fake_codex_cloud(temp: &tempfile::TempDir, first: &str, next: &str) -> std::path::PathBuf {
    use std::os::unix::fs::PermissionsExt;
    fs::create_dir_all(temp.path().join(".codex")).unwrap();
    fs::write(temp.path().join(".codex/auth.json"), "{}").unwrap();
    let bin = temp.path().join("fake-bin");
    fs::create_dir_all(&bin).unwrap();
    let script = format!(
        "#!/bin/sh\n\
         if [ \"$1\" != cloud ] || [ \"$2\" != list ]; then\n\
           echo \"unexpected codex invocation: $*\" >&2; exit 2\n\
         fi\n\
         case \" $* \" in\n\
           *\" --cursor \"*) cat <<'PAYLOAD2'\n{next}\nPAYLOAD2\n;;\n\
           *) cat <<'PAYLOAD1'\n{first}\nPAYLOAD1\n;;\n\
         esac\n"
    );
    let path = bin.join("codex");
    fs::write(&path, script).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
    bin
}

#[cfg(unix)]
const CODEX_CLOUD_EMPTY_PAGE: &str = r#"{"tasks":[],"cursor":null}"#;

#[cfg(unix)]
fn prepend_path(command: &mut Command, bin: &Path) {
    let path = std::env::var_os("PATH").unwrap_or_default();
    let mut joined = bin.as_os_str().to_os_string();
    joined.push(":");
    joined.push(path);
    command.env("PATH", joined);
}

const CODEX_CLOUD_LISTING: &str = r#"{"tasks":[{"id":"task_e_42","url":"https://chatgpt.com/codex/tasks/task_e_42","title":"Speed up the indexer","status":"ready","updated_at":"2026-06-23T08:00:00Z","environment_id":"env_1","environment_label":"api","is_review":false,"attempt_total":1}],"cursor":null}"#;

#[cfg(unix)]
#[test]
fn codex_cloud_tasks_discover_through_the_codex_cli() {
    let temp = fake_home();
    let db_path = temp.path().join("history.db");
    let bin = fake_codex_cloud(&temp, CODEX_CLOUD_LISTING, CODEX_CLOUD_EMPTY_PAGE);

    let mut command = isolated(
        &temp,
        &db_path,
        &["sessions", "discover", "--remote", "--json"],
    );
    prepend_path(&mut command, &bin);
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let lines = jsonl(&String::from_utf8_lossy(&output.stdout));
    let sessions: Vec<&Value> = lines
        .iter()
        .filter(|line| line["type"] == "session")
        .collect();
    assert_eq!(sessions.len(), 1, "{lines:#?}");
    assert_eq!(sessions[0]["source"], "codex");
    assert_eq!(sessions[0]["session_id"], "task_e_42");
    assert_eq!(sessions[0]["first_prompt"], "Speed up the indexer");
    assert_eq!(sessions[0]["locations"], serde_json::json!(["remote"]));
    assert_eq!(sessions[0]["discovery_state"], "shallow");
    let summary = lines.last().unwrap();
    assert_eq!(summary["locations_run"], serde_json::json!(["remote"]));
    assert_eq!(summary["providers"]["codex"]["discovered"], 1);
    assert_eq!(summary["counters"]["files_opened"], 0);

    // The cached ledger now serves the remote row to scoped reads, and the
    // local scope stays untouched by it.
    let remote_list = isolated(&temp, &db_path, &["sessions", "list", "--remote", "--json"])
        .output()
        .unwrap();
    let payload: Value = serde_json::from_slice(&remote_list.stdout).unwrap();
    assert_eq!(payload["sessions"].as_array().unwrap().len(), 1);
    assert_eq!(payload["sessions"][0]["session_id"], "task_e_42");
    let local_list = isolated(&temp, &db_path, &["sessions", "list", "--json"])
        .output()
        .unwrap();
    let payload: Value = serde_json::from_slice(&local_list.stdout).unwrap();
    assert!(payload["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .all(|row| row["session_id"] != "task_e_42"));
}

#[cfg(unix)]
#[test]
fn an_all_scope_run_executes_local_and_remote_connectors_together() {
    let temp = fake_home();
    let db_path = temp.path().join("history.db");
    let bin = fake_codex_cloud(&temp, CODEX_CLOUD_LISTING, CODEX_CLOUD_EMPTY_PAGE);

    let mut command = isolated(
        &temp,
        &db_path,
        &["sessions", "discover", "--all", "--json"],
    );
    prepend_path(&mut command, &bin);
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let lines = jsonl(&String::from_utf8_lossy(&output.stdout));
    let ids: Vec<&str> = lines
        .iter()
        .filter(|line| line["type"] == "session")
        .map(|line| line["session_id"].as_str().unwrap())
        .collect();
    assert_eq!(
        ids,
        ["task_e_42", "codex-cli", "claude-cli"],
        "global recency order spans both locations"
    );
    let summary = lines.last().unwrap();
    assert_eq!(
        summary["locations_run"],
        serde_json::json!(["local", "remote"])
    );
    // One providers entry per source, local and remote tallies merged.
    assert_eq!(summary["providers"]["codex"]["candidates"], 2);
    assert_eq!(summary["providers"]["codex"]["discovered"], 2);

    let all_list = isolated(&temp, &db_path, &["sessions", "list", "--all", "--json"])
        .output()
        .unwrap();
    let payload: Value = serde_json::from_slice(&all_list.stdout).unwrap();
    assert_eq!(payload["sessions"].as_array().unwrap().len(), 3);
}

#[cfg(unix)]
#[test]
fn sync_remote_runs_configured_connectors_and_reports_them() {
    let temp = fake_home();
    let db_path = temp.path().join("history.db");
    let bin = fake_codex_cloud(&temp, CODEX_CLOUD_LISTING, CODEX_CLOUD_EMPTY_PAGE);

    let mut command = isolated(&temp, &db_path, &["sync", "--remote"]);
    prepend_path(&mut command, &bin);
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("[remote:codex]"), "{stdout}");

    let remote_list = isolated(&temp, &db_path, &["sessions", "list", "--remote", "--json"])
        .output()
        .unwrap();
    let payload: Value = serde_json::from_slice(&remote_list.stdout).unwrap();
    assert_eq!(payload["sessions"].as_array().unwrap().len(), 1);

    // `sync --all` on a home with no connectors configured stays local-only
    // and says so instead of failing.
    let plain = fake_home();
    let plain_db = plain.path().join("history.db");
    let output = isolated(&plain, &plain_db, &["sync", "--all"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("no remote provider connectors configured"),
        "{stdout}"
    );
}

/// Serve one-shot HTTP responses for the claude.ai session-list endpoint on a
/// loopback port, one accepted connection per queued body.
fn serve_json_pages(bodies: Vec<String>) -> std::net::SocketAddr {
    use std::io::{Read as _, Write as _};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        for body in bodies {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            let mut buf = [0u8; 8192];
            let _ = stream.read(&mut buf);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes());
        }
    });
    addr
}

#[test]
fn claude_web_sessions_discover_through_the_session_list_endpoint() {
    let temp = fake_home();
    let db_path = temp.path().join("history.db");
    fs::create_dir_all(temp.path().join(".claude")).unwrap();
    fs::write(
        temp.path().join(".claude/.credentials.json"),
        r#"{"claudeAiOauth":{"accessToken":"sk-ant-oat01-test","refreshToken":"r","expiresAt":4102444800000,"scopes":["user:inference"]}}"#,
    )
    .unwrap();
    let page = serde_json::json!({
        "data": [{
            "id": "session_01Web",
            "title": "Refactor the auth flow",
            "status": "idle",
            "worker_status": "idle",
            "created_at": "2026-06-24T09:00:00Z",
            "last_event_at": "2026-06-24T10:30:00Z",
            "environment_kind": "cloud",
            "config": {"sources": [{"type": "git_repository", "url": "https://github.com/acme/app"}]}
        }, {
            "id": "session_01Bridge",
            "title": "A remote-control bridge",
            "environment_kind": "bridge",
            "created_at": "2026-06-24T09:00:00Z"
        }],
        "next_cursor": null
    })
    .to_string();
    let addr = serve_json_pages(vec![page]);

    let mut command = isolated(
        &temp,
        &db_path,
        &["sessions", "discover", "--remote", "--json"],
    );
    command.env("RELAYHISTORY_CLAUDE_API_BASE_URL", format!("http://{addr}"));
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let lines = jsonl(&String::from_utf8_lossy(&output.stdout));
    let sessions: Vec<&Value> = lines
        .iter()
        .filter(|line| line["type"] == "session")
        .collect();
    assert_eq!(
        sessions.len(),
        1,
        "the bridge session is not remote evidence"
    );
    assert_eq!(sessions[0]["source"], "claude");
    assert_eq!(sessions[0]["session_id"], "session_01Web");
    assert_eq!(sessions[0]["first_prompt"], "Refactor the auth flow");
    assert_eq!(sessions[0]["repo_url"], "https://github.com/acme/app");
    assert_eq!(
        sessions[0]["raw_path"],
        "https://claude.ai/code/session_01Web"
    );
    assert_eq!(sessions[0]["locations"], serde_json::json!(["remote"]));
    let summary = lines.last().unwrap();
    assert_eq!(summary["locations_run"], serde_json::json!(["remote"]));
    assert_eq!(summary["providers"]["claude"]["discovered"], 1);
}

#[cfg(unix)]
#[test]
fn codex_cloud_discovery_follows_the_cli_cursor_across_pages() {
    let temp = fake_home();
    let db_path = temp.path().join("history.db");
    let first = r#"{"tasks":[{"id":"task_e_1","title":"one","status":"ready","updated_at":"2026-06-23T08:00:00Z"}],"cursor":"page-2"}"#;
    let next = r#"{"tasks":[{"id":"task_e_2","title":"two","status":"ready","updated_at":"2026-06-22T08:00:00Z"}],"cursor":null}"#;
    let bin = fake_codex_cloud(&temp, first, next);

    let mut command = isolated(
        &temp,
        &db_path,
        &["sessions", "discover", "--remote", "--json"],
    );
    prepend_path(&mut command, &bin);
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let lines = jsonl(&String::from_utf8_lossy(&output.stdout));
    let ids: Vec<&str> = lines
        .iter()
        .filter(|line| line["type"] == "session")
        .map(|line| line["session_id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["task_e_1", "task_e_2"]);
}

#[cfg(unix)]
#[test]
fn a_source_filter_with_no_matching_connector_is_rejected_as_unsupported() {
    let temp = fake_home();
    let db_path = temp.path().join("history.db");
    // Only codex is signed in; a claude-only remote request is unsupported.
    let bin = fake_codex_cloud(&temp, CODEX_CLOUD_LISTING, CODEX_CLOUD_EMPTY_PAGE);

    let mut command = isolated(
        &temp,
        &db_path,
        &[
            "sessions", "discover", "--remote", "--source", "claude", "--json",
        ],
    );
    prepend_path(&mut command, &bin);
    let output = command.output().unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("no remote provider connectors are configured"),
        "{stderr}"
    );
    assert!(stderr.contains("claude-web"), "{stderr}");
}

#[test]
fn local_operations_ignore_selected_commercial_connectors_and_seeded_credentials() {
    let temp = fake_home();
    let db = temp.path().join("history.db");
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    for args in [
        vec!["sync", "--local", "--source-connector", "relaycast"],
        vec!["sync", "--all", "--no-source-connectors"],
        vec![
            "sessions",
            "discover",
            "--local",
            "--source-connector",
            "cloud",
            "--json",
        ],
    ] {
        let output = isolated(&temp, &db, &args)
            .env("RELAYHISTORY_HOME", temp.path().join("commercial"))
            .env("RELAYCAST_API_KEY", "synthetic-key")
            .env("RELAYCAST_WORKSPACE_ID", "synthetic-workspace")
            .env("RELAYCAST_BASE_URL", &base)
            .env("RELAYHISTORY_BASE_URL", &base)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    assert!(!temp.path().join("commercial").exists());
}

#[test]
fn explicit_relaycast_sync_uses_remote_presence_and_persists_checkpoint() {
    use std::io::{BufRead, BufReader, Write};
    let temp = tempfile::tempdir().unwrap();
    let db = temp.path().join("history.db");
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    listener.set_nonblocking(true).unwrap();
    let server = std::thread::spawn(move || {
        let started = std::time::Instant::now();
        let mut requests = Vec::new();
        while requests.len() < 3 && started.elapsed() < std::time::Duration::from_secs(10) {
            let (mut stream, _) = match listener.accept() {
                Ok(pair) => pair,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(std::time::Duration::from_millis(5));
                    continue;
                }
                Err(error) => panic!("{error}"),
            };
            stream.set_nonblocking(false).unwrap();
            stream
                .set_read_timeout(Some(std::time::Duration::from_secs(2)))
                .unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let body = if line.starts_with("GET /v1/channels ") {
                r#"{"data":[{"name":"general"}]}"#
            } else if line.starts_with("GET /v1/channels/general/messages?") {
                r#"{"data":[{"id":"m001","text":"explicit remote message","created_at":"2026-09-13T00:00:00Z"}]}"#
            } else if line.starts_with("GET /v1/dm/conversations/all ") {
                r#"{"data":[]}"#
            } else {
                panic!("unexpected {line}")
            };
            requests.push(line);
            loop {
                let mut header = String::new();
                reader.read_line(&mut header).unwrap();
                if header.trim().is_empty() {
                    break;
                }
            }
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .unwrap();
        }
        requests
    });
    let output = isolated(
        &temp,
        &db,
        &["sync", "--remote", "--source-connector", "relaycast"],
    )
    .env("RELAYCAST_API_KEY", "synthetic-key")
    .env("RELAYCAST_WORKSPACE_ID", "synthetic-workspace")
    .env("RELAYCAST_BASE_URL", &base)
    .output()
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(server.join().unwrap().len(), 3);
    let conn = open_db(&db).unwrap();
    assert_eq!(
        ai_hist_core::session_locations(&conn, "relay", "#general").unwrap(),
        vec!["remote"]
    );
    let state: Value =
        serde_json::from_slice(&fs::read(temp.path().join(".sync-state.json")).unwrap()).unwrap();
    assert_eq!(state["relay"]["ch:general"], "m001");
}

#[cfg(unix)]
#[test]
fn busy_relaycast_lock_skips_only_relaycast_and_does_not_initialize_its_database() {
    for include_codex in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let db = temp.path().join("history.db");
        let lock = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(temp.path().join("history.db.sync.lock"))
            .unwrap();
        fs2::FileExt::lock_exclusive(&lock).unwrap();
        let mut command = isolated(
            &temp,
            &db,
            &["sync", "--remote", "--source-connector", "relaycast"],
        );
        command
            .env("RELAYCAST_API_KEY", "fixture-key")
            .env("RELAYCAST_WORKSPACE_ID", "fixture-workspace")
            .env("RELAYCAST_BASE_URL", "http://127.0.0.1:1");
        if include_codex {
            let bin = fake_codex_cloud(&temp, CODEX_CLOUD_LISTING, CODEX_CLOUD_EMPTY_PAGE);
            prepend_path(&mut command, &bin);
            command.args(["--source-connector", "codex-cloud"]);
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        if include_codex {
            let conn = open_db(&db).unwrap();
            let count: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sessions WHERE source='codex' AND session_id='task_e_42'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(
                count, 1,
                "busy Relaycast cannot skip selected independent Codex discovery"
            );
        } else {
            assert!(
                !db.exists(),
                "busy Relaycast-only sync must not initialize the database"
            );
        }
    }
}
