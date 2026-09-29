//! `ai-hist export` must never write over the database the same invocation
//! opened, however that database was selected (#73).

use std::path::Path;
use std::process::{Command, Output};

fn ai_hist(temp: &tempfile::TempDir) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ai-hist"));
    command
        .current_dir(temp.path())
        .env("HOME", temp.path())
        .env("USERPROFILE", temp.path())
        .env("XDG_DATA_HOME", temp.path().join("xdg"))
        .env_remove("AI_HIST_DB")
        .env_remove("OPENCODE_DB");
    command
}

fn run(command: &mut Command) -> Output {
    let output = command.output().expect("run ai-hist");
    assert_ne!(
        output.status.code(),
        Some(2),
        "the command line was rejected, so nothing below is being tested: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

/// A database at `db` holding two history rows, imported through the CLI.
fn seed(temp: &tempfile::TempDir, db: &Path) {
    let rows = temp.path().join("seed.jsonl");
    std::fs::write(
        &rows,
        concat!(
            r#"{"source":"claude","session_id":"s1","project":"/p","prompt":"first prompt","timestamp_ms":1000}"#,
            "\n",
            r#"{"source":"codex","session_id":"s2","project":"/p","prompt":"second prompt","timestamp_ms":2000}"#,
            "\n",
        ),
    )
    .unwrap();
    let output = run(ai_hist(temp).arg("--db").arg(db).arg("import").arg(&rows));
    assert!(
        output.status.success(),
        "seeding failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn assert_refused_and_intact(output: &Output, db: &Path, before: &[u8]) {
    assert!(
        !output.status.success(),
        "export over the active database reported success"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("Refusing to export over the active database"),
        "unexpected stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        std::fs::read(db).unwrap(),
        before,
        "the active database changed"
    );
}

/// Variant 1 of #73: `--db` selected the database, so a guard that compared
/// against the default path never fired.
#[test]
fn export_refuses_the_database_selected_by_db_flag() {
    let temp = tempfile::tempdir().unwrap();
    let db = temp.path().join("mine.db");
    seed(&temp, &db);
    let before = std::fs::read(&db).unwrap();

    for format in ["sqlite", "jsonl"] {
        // A relative spelling of the same file, resolved against the cwd.
        let output = run(ai_hist(&temp)
            .arg("--db")
            .arg(&db)
            .args(["export", "--format", format, "mine.db"]));
        assert_refused_and_intact(&output, &db, &before);
    }
}

/// Variant 2 of #73: non-sqlite formats had no guard at all, so even the
/// `AI_HIST_DB` selection was overwritten with JSONL.
#[test]
fn export_refuses_the_database_selected_by_environment_for_every_format() {
    let temp = tempfile::tempdir().unwrap();
    let db = temp.path().join("mine.db");
    seed(&temp, &db);
    let before = std::fs::read(&db).unwrap();

    for (format, dest) in [
        ("sqlite", "mine.db"),
        ("jsonl", "mine.db"),
        ("jsonl", "./mine.db"),
        ("jsonl", "mine.db-wal"),
    ] {
        let output = run(ai_hist(&temp)
            .env("AI_HIST_DB", &db)
            .args(["export", "--format", format, dest]));
        assert_refused_and_intact(&output, &db, &before);
    }
}

/// The guard does not get in the way of an ordinary export beside the
/// database.
#[test]
fn export_to_another_file_still_works() {
    let temp = tempfile::tempdir().unwrap();
    let db = temp.path().join("mine.db");
    seed(&temp, &db);

    let output =
        run(ai_hist(&temp)
            .arg("--db")
            .arg(&db)
            .args(["export", "--format", "jsonl", "out.jsonl"]));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let body = std::fs::read_to_string(temp.path().join("out.jsonl")).unwrap();
    assert_eq!(body.lines().count(), 2);
}
