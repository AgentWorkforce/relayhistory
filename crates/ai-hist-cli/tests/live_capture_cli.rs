//! The live-capture command surfaces: what `ingest --hook` is allowed to say,
//! and what `watch` is allowed to watch.

use std::process::Command;

fn ingest(temp: &tempfile::TempDir, args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ai-hist"));
    command
        .arg("--db")
        .arg(temp.path().join("history.db"))
        .arg("ingest")
        .args(args)
        .env("HOME", temp.path())
        .env("USERPROFILE", temp.path())
        .env("XDG_DATA_HOME", temp.path().join("xdg"))
        .env_remove("AI_HIST_DB")
        .env_remove("OPENCODE_DB");
    command
}

/// A hook runs inside the agent's tool call. `--quiet` is a promise to stay
/// out of the way, and `--json` must not break it — otherwise a hook wired
/// with both writes a JSON document into the stdout of every tool call.
#[test]
fn quiet_outranks_json() {
    let temp = tempfile::tempdir().unwrap();
    let payload = r#"{"session_id":"x","transcript_path":"/nonexistent"}"#;

    let loud = ingest(&temp, &["--hook", "claude", "--json"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            child
                .stdin
                .as_mut()
                .expect("stdin")
                .write_all(payload.as_bytes())?;
            child.wait_with_output()
        })
        .unwrap();
    assert!(loud.status.success());
    assert!(
        !loud.stdout.is_empty(),
        "--json alone should print the report"
    );

    let quiet = ingest(&temp, &["--hook", "claude", "--json", "--quiet"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            child
                .stdin
                .as_mut()
                .expect("stdin")
                .write_all(payload.as_bytes())?;
            child.wait_with_output()
        })
        .unwrap();
    assert_eq!(quiet.status.code(), Some(0));
    assert_eq!(
        String::from_utf8_lossy(&quiet.stdout),
        "",
        "--quiet --json must print nothing on stdout"
    );
    assert_eq!(
        String::from_utf8_lossy(&quiet.stderr),
        "",
        "--quiet --json must print nothing on stderr either"
    );
}

/// An unsupported harness is still a hook invocation: report it and get out of
/// the way, rather than failing the tool call that ran us.
#[test]
fn an_unsupported_harness_still_exits_zero() {
    let temp = tempfile::tempdir().unwrap();
    let output = ingest(&temp, &["--hook", "codex", "--json"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            child.stdin.as_mut().expect("stdin").write_all(b"{}")?;
            child.wait_with_output()
        })
        .unwrap();
    assert_eq!(output.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&output.stderr).contains("unsupported hook harness"));
}
