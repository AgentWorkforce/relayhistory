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

// Provider HTTP/CLI and Relaycast behavior tests live with the optional adapter
// packages. This binary's contract is local acquisition plus cached remote reads.
#[test]
fn installed_credentials_do_not_compose_remote_sources_into_local_cli() {
    let temp = fake_home();
    for (path, body) in [
        (".claude/.credentials.json", "{poisoned credentials"),
        (".codex/auth.json", "{poisoned credentials"),
        ("commercial/auth.json", "{poisoned commercial auth"),
    ] {
        let path = temp.path().join(path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, body).unwrap();
    }
    let db = temp.path().join("fresh.db");
    for args in [
        vec!["sync", "--remote"],
        vec!["sessions", "discover", "--remote", "--json"],
        vec!["sync", "--remote", "--source-connector", "claude-web"],
        vec!["sync", "--remote", "--source-connector", "relaycast"],
    ] {
        let output = isolated(&temp, &db, &args)
            .env("RELAYHISTORY_HOME", temp.path().join("commercial"))
            .output()
            .unwrap();
        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("source plugin"), "{stderr}");
        assert!(!db.exists());
    }
    let output = isolated(&temp, &db, &["sessions", "discover", "--all", "--json"])
        .env("RELAYHISTORY_HOME", temp.path().join("commercial"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = jsonl(&String::from_utf8_lossy(&output.stdout));
    let summary = records.last().unwrap();
    assert_eq!(summary["locations_run"], serde_json::json!(["local"]));
    assert!(records
        .iter()
        .filter(|row| row.get("session_id").is_some())
        .all(|row| row["locations"] == serde_json::json!(["local"])));
}
