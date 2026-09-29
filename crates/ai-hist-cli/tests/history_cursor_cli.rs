//! The native `search` and `recent` page with the same keyset cursor as the
//! SDK and MCP (#67): `--after-ms`/`--after-id` continue past rows that share a
//! timestamp instead of skipping or repeating them.

use std::path::Path;
use std::process::{Command, Output};

fn ai_hist(temp: &tempfile::TempDir, db: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ai-hist"));
    command
        .current_dir(temp.path())
        .env("HOME", temp.path())
        .env("USERPROFILE", temp.path())
        .env("XDG_DATA_HOME", temp.path().join("xdg"))
        .env_remove("AI_HIST_DB")
        .env_remove("OPENCODE_DB")
        .arg("--db")
        .arg(db);
    command
}

fn run(command: &mut Command) -> Output {
    let output = command.output().expect("run ai-hist");
    assert_ne!(
        output.status.code(),
        Some(2),
        "the command line was rejected: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

/// Three prompts, two of which share a millisecond.
fn seed(temp: &tempfile::TempDir, db: &Path) {
    let rows = temp.path().join("seed.jsonl");
    std::fs::write(
        &rows,
        concat!(
            r#"{"source":"claude","session_id":"s1","project":"/p","prompt":"needle one","timestamp_ms":1000}"#,
            "\n",
            r#"{"source":"claude","session_id":"s1","project":"/p","prompt":"needle two","timestamp_ms":2000}"#,
            "\n",
            r#"{"source":"claude","session_id":"s1","project":"/p","prompt":"needle three","timestamp_ms":2000}"#,
            "\n",
        ),
    )
    .unwrap();
    let output = run(ai_hist(temp, db).arg("import").arg(&rows));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// `(timestamp_ms, id)` of every row a JSON page printed.
fn keys(output: &Output) -> Vec<(i64, i64)> {
    let rows: Vec<serde_json::Value> = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|_| panic!("not JSON: {}", String::from_utf8_lossy(&output.stdout)));
    rows.iter()
        .map(|row| {
            (
                row["timestamp_ms"].as_i64().unwrap(),
                row["id"].as_i64().unwrap(),
            )
        })
        .collect()
}

fn walk(temp: &tempfile::TempDir, db: &Path, command: &[&str]) -> Vec<(i64, i64)> {
    let mut seen = Vec::new();
    let mut after: Option<(i64, i64)> = None;
    for _ in 0..5 {
        let mut cmd = ai_hist(temp, db);
        cmd.args(command).arg("--json");
        if let Some((ms, id)) = after {
            cmd.arg("--after-ms")
                .arg(ms.to_string())
                .arg("--after-id")
                .arg(id.to_string());
        }
        let output = run(&mut cmd);
        if !output.status.success() {
            break; // `search` exits 1 on an empty page
        }
        let page = keys(&output);
        if page.is_empty() {
            break;
        }
        after = page.last().copied();
        seen.extend(page);
    }
    seen
}

#[test]
fn recent_and_search_page_across_a_shared_timestamp() {
    let temp = tempfile::tempdir().unwrap();
    let db = temp.path().join("history.db");
    seed(&temp, &db);

    for command in [
        &["recent", "1"][..],
        &["search", "needle", "--role", "prompt", "--limit", "1"][..],
    ] {
        let seen = walk(&temp, &db, command);
        assert_eq!(seen.len(), 3, "{command:?} returned {seen:?}");
        let mut unique = seen.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), 3, "{command:?} repeated a row: {seen:?}");
    }
}

#[test]
fn an_unknown_cursor_match_source_is_the_shared_invalid_argument() {
    let temp = tempfile::tempdir().unwrap();
    let db = temp.path().join("history.db");
    seed(&temp, &db);
    let output = run(ai_hist(&temp, &db).args([
        "search",
        "needle",
        "--after-ms",
        "2000",
        "--after-id",
        "1",
        "--after-match-source",
        "tool",
    ]));
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("match_source"),
        "unexpected stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
