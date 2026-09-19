//! The live-capture command surfaces: what `ingest --hook` is allowed to say,
//! and what `watch` is allowed to watch.

use std::process::{Command, Output, Stdio};

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

/// Run the command with `payload` on stdin and collect everything it said.
///
/// A failed *write* is deliberately not an assertion. If the command exits
/// before reading stdin the write fails with `BrokenPipe`, and panicking there
/// reports a plumbing error instead of the exit code and output that say what
/// actually happened — which is the difference between "the precedence is
/// wrong" and "this binary has no `ingest` subcommand".
fn run(mut command: Command, payload: &[u8]) -> Output {
    use std::io::Write;
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn ai-hist");
    let _ = child.stdin.as_mut().expect("stdin").write_all(payload);
    let output = child.wait_with_output().expect("wait for ai-hist");
    assert_ne!(
        output.status.code(),
        Some(2),
        "the command line was rejected, so nothing below is being tested: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

/// A hook runs inside the agent's tool call. `--quiet` is a promise to stay
/// out of the way, and `--json` must not break it — otherwise a hook wired
/// with both writes a JSON document into the stdout of every tool call.
#[test]
fn quiet_outranks_json() {
    let temp = tempfile::tempdir().unwrap();
    let payload = r#"{"session_id":"x","transcript_path":"/nonexistent"}"#;

    // `--json` alone must print something, or the silence asserted below
    // would prove nothing: a command that never prints passes either way.
    let loud = run(
        ingest(&temp, &["--hook", "claude", "--json"]),
        payload.as_bytes(),
    );
    assert!(loud.status.success());
    assert!(
        !loud.stdout.is_empty(),
        "--json alone should print the report"
    );

    let quiet = run(
        ingest(&temp, &["--hook", "claude", "--json", "--quiet"]),
        payload.as_bytes(),
    );
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
    let output = run(ingest(&temp, &["--hook", "codex", "--json"]), b"{}");
    assert_eq!(output.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&output.stderr).contains("unsupported hook harness"));
}
