use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::thread;
use std::time::{Duration, Instant};

const PROD: &str = "https://history.agentrelay.com";
const FUTURE: &str = "2999-01-01T00:00:00Z";
const OLD: &str = "rth_at_old_fixture";
const NEW: &str = "rth_at_new_fixture";
const REFRESH: &str = "rth_rt_old_fixture";

fn save(home: &Path, base: &str, token: &str, expiry: Option<&str>) -> PathBuf {
    let stages = home.join("stages");
    std::fs::create_dir_all(&stages).unwrap();
    let path = stages.join(format!("{}.auth.json", ai_hist_core::prompt_hash(base)));
    std::fs::write(
        &path,
        json!({"base_url": base, "access_token": token,
        "access_token_expires_at": expiry, "refresh_token": REFRESH})
        .to_string(),
    )
    .unwrap();
    path
}

fn command(home: &Path) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_ai-hist"));
    // token migrates the legacy TypeScript store, which lives under HOME rather
    // than RELAYHISTORY_HOME. Pin both, or these tests read the developer's real
    // ~/.config/ai-hist/auth.json and pass or fail by machine state. The legacy
    // dir is deliberately a subpath that stays absent unless a test creates it,
    // so it cannot collide with the RELAYHISTORY_HOME/auth.json legacy location.
    cmd.env("RELAYHISTORY_HOME", home)
        .env("HOME", home)
        .env("USERPROFILE", home)
        .env("AI_HIST_CONFIG_DIR", home.join("legacy-sdk"))
        .env_remove("RELAYHISTORY_BASE_URL")
        .env_remove("AI_HIST_BASE_URL")
        .env("RUST_LOG", "trace")
        .arg("--db")
        .arg(home.join("must-not-create.db"))
        .arg("token");
    cmd
}

fn success(output: &Output, expected: &str) {
    assert!(output.status.success());
    assert!(
        output.stdout == format!("{expected}\n").as_bytes(),
        "stdout must be exactly the expected access token and one newline"
    );
    assert!(
        output.stderr.is_empty(),
        "piped invocation must have no diagnostics"
    );
}

fn failure(output: &Output) -> String {
    assert!(!output.status.success());
    assert!(
        output.stdout.is_empty(),
        "failure must not print anything to stdout"
    );
    let err = String::from_utf8(output.stderr.clone()).unwrap();
    for secret in [OLD, NEW, REFRESH] {
        assert!(
            !err.contains(secret),
            "diagnostics must not include credentials"
        );
    }
    err
}

// A loopback HTTP exchange: require the old refresh credential; serve exactly one request.
fn refresh_server(status: u16, response: Value) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    listener.set_nonblocking(true).unwrap();
    let handle = thread::spawn(move || {
        let start = Instant::now();
        let mut stream = loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(
                        start.elapsed() < Duration::from_secs(10),
                        "refresh was skipped"
                    );
                    thread::sleep(Duration::from_millis(5));
                }
                Err(err) => panic!("{err}"),
            }
        };
        stream.set_nonblocking(false).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        assert!(line.starts_with("POST /v1/auth/token/refresh "));
        let mut length = 0;
        loop {
            line.clear();
            reader.read_line(&mut line).unwrap();
            if line == "\r\n" {
                break;
            }
            if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                length = value.trim().parse().unwrap();
            }
        }
        let mut body = vec![0; length];
        reader.read_exact(&mut body).unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert!(body["refreshToken"] == REFRESH);
        let body = response.to_string();
        write!(stream, "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
    });
    (base, handle)
}

#[test]
fn token_is_exactly_one_line_and_never_opens_sqlite() {
    let home = tempfile::tempdir().unwrap();
    let path = save(home.path(), PROD, OLD, Some(FUTURE));
    let before = std::fs::read(&path).unwrap();
    success(&command(home.path()).output().unwrap(), OLD);
    assert!(!home.path().join("must-not-create.db").exists());
    assert!(before == std::fs::read(path).unwrap());
    assert_eq!(
        std::fs::read_dir(home.path().join("stages"))
            .unwrap()
            .count(),
        1
    );
}

#[test]
fn unauthenticated_has_existing_error_and_empty_stdout() {
    let home = tempfile::tempdir().unwrap();
    let err = failure(&command(home.path()).output().unwrap());
    assert!(err.contains("not authenticated — run `ai-hist login` or `ai-hist admin-mint` first"));
    assert_eq!(std::fs::read_dir(home.path()).unwrap().count(), 0);
}

#[test]
fn invalid_explicit_base_url_reports_rejected_value_and_expected_format() {
    let home = tempfile::tempdir().unwrap();
    let rejected = "not-a-url";
    let err = failure(
        &command(home.path())
            .args(["--base-url", rejected])
            .output()
            .unwrap(),
    );
    assert!(err.contains(&format!("invalid relayhistory base URL `{rejected}`")));
    assert!(err.contains("expected an absolute URL without credentials, query, or fragment"));
}

#[test]
fn stage_selection_honors_explicit_url_then_environment_and_rejects_ambiguity() {
    let home = tempfile::tempdir().unwrap();
    let dev = "http://localhost:8787";
    save(home.path(), PROD, OLD, Some(FUTURE));
    save(home.path(), dev, NEW, Some(FUTURE));
    let err = failure(&command(home.path()).output().unwrap());
    assert!(err.contains("2 relayhistory stages are configured; pass --base-url to select one."));
    success(
        &command(home.path())
            .env("AI_HIST_BASE_URL", dev)
            .output()
            .unwrap(),
        NEW,
    );
    success(
        &command(home.path())
            .env("AI_HIST_BASE_URL", dev)
            .env("RELAYHISTORY_BASE_URL", PROD)
            .output()
            .unwrap(),
        OLD,
    );
    success(
        &command(home.path())
            .env("RELAYHISTORY_BASE_URL", PROD)
            .args(["--base-url", dev])
            .output()
            .unwrap(),
        NEW,
    );
}

#[test]
fn expired_near_expiry_and_unknown_expiry_refresh_and_print_the_new_persisted_value() {
    let near = (chrono::Utc::now() + chrono::Duration::seconds(10)).to_rfc3339();
    for expiry in [
        Some("2000-01-01T00:00:00Z"),
        Some(near.as_str()),
        None,
        Some("invalid"),
    ] {
        let home = tempfile::tempdir().unwrap();
        let (base, server) = refresh_server(
            200,
            json!({"accessToken": NEW,
            "refreshToken": "rth_rt_new_fixture", "accessTokenExpiresAt": FUTURE}),
        );
        let path = save(home.path(), &base, OLD, expiry);
        let output = command(home.path())
            .args(["--base-url", &base])
            .output()
            .unwrap();
        server.join().unwrap(); // Fails if no refresh request was made.
        success(&output, NEW); // Fails if the old value is printed after refreshing.
        let saved: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert!(saved["access_token"] == NEW);
        assert!(saved["refresh_token"] == "rth_rt_new_fixture");
        // Server has closed: a second call must use the persisted new pair without refresh.
        success(
            &command(home.path())
                .args(["--base-url", &base])
                .output()
                .unwrap(),
            NEW,
        );
    }
}

#[test]
fn refresh_failures_never_echo_response_credentials() {
    for (status, response) in [
        (401, json!({"error": format!("{OLD} {NEW} {REFRESH}")})),
        (
            200,
            json!({"accessToken": NEW, "refreshToken": "rth_rt_new_fixture",
            "accessTokenExpiresAt": "2000-01-01T00:00:00Z"}),
        ),
        (200, json!({"error": REFRESH})),
    ] {
        let home = tempfile::tempdir().unwrap();
        let (base, server) = refresh_server(status, response);
        save(home.path(), &base, OLD, Some("2000-01-01T00:00:00Z"));
        let output = command(home.path())
            .args(["--base-url", &base])
            .output()
            .unwrap();
        server.join().unwrap();
        failure(&output);
    }
}

#[test]
fn malformed_storage_and_multiline_access_tokens_do_not_leak() {
    let home = tempfile::tempdir().unwrap();
    let path = save(home.path(), PROD, OLD, Some(FUTURE));
    // A wrong root type makes serde's error quote the string (and thus the secret).
    std::fs::write(&path, json!(OLD).to_string()).unwrap();
    failure(&command(home.path()).output().unwrap());
    save(
        home.path(),
        PROD,
        &format!("{OLD}\n{REFRESH}"),
        Some(FUTURE),
    );
    failure(&command(home.path()).output().unwrap());
}

#[test]
fn cleartext_is_allowed_for_local_printing_but_never_for_refresh() {
    let home = tempfile::tempdir().unwrap();
    let base = "http://example.invalid";
    save(home.path(), base, OLD, Some(FUTURE));
    success(
        &command(home.path())
            .args(["--base-url", base])
            .output()
            .unwrap(),
        OLD,
    );
    save(home.path(), base, OLD, Some("2000-01-01T00:00:00Z"));
    failure(
        &command(home.path())
            .args(["--base-url", base])
            .output()
            .unwrap(),
    );
}

#[test]
fn simultaneous_token_commands_share_one_refresh() {
    let home = tempfile::tempdir().unwrap();
    let (base, server) = refresh_server(
        200,
        json!({"accessToken": NEW,
        "refreshToken": "rth_rt_new_fixture", "accessTokenExpiresAt": FUTURE}),
    );
    save(home.path(), &base, OLD, Some("2000-01-01T00:00:00Z"));
    let children: Vec<_> = (0..4)
        .map(|_| {
            command(home.path())
                .args(["--base-url", &base])
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap()
        })
        .collect();
    for child in children {
        success(&child.wait_with_output().unwrap(), NEW);
    }
    server.join().unwrap();
}

#[test]
fn expired_session_without_refresh_token_fails_without_stdout() {
    let home = tempfile::tempdir().unwrap();
    let path = save(home.path(), PROD, OLD, Some("2000-01-01T00:00:00Z"));
    let mut auth: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    auth["refresh_token"] = Value::Null;
    std::fs::write(path, auth.to_string()).unwrap();
    failure(&command(home.path()).output().unwrap());
}

#[test]
fn legacy_auth_is_supported_and_default_stage_is_production() {
    let home = tempfile::tempdir().unwrap();
    let path = save(home.path(), PROD, OLD, Some(FUTURE));
    std::fs::rename(path, home.path().join("auth.json")).unwrap();
    success(&command(home.path()).output().unwrap(), OLD);

    let home = tempfile::tempdir().unwrap();
    save(home.path(), "http://localhost:8787", OLD, Some(FUTURE));
    assert!(failure(&command(home.path()).output().unwrap()).contains("not authenticated"));
}

// With no selector at all and several stages configured, token must refuse to
// guess rather than fall back to production. The ambiguity probe stays on
// load_auth for this reason: load_sdk_auth infers a destination from the
// environment, which would defeat the refusal.
#[test]
fn multiple_stages_without_a_selector_refuse_to_guess() {
    let home = tempfile::tempdir().unwrap();
    save(home.path(), PROD, OLD, Some(FUTURE));
    save(home.path(), "http://localhost:8787", NEW, Some(FUTURE));
    let err = failure(&command(home.path()).output().unwrap());
    assert!(
        err.contains("stages are configured; pass --base-url to select one"),
        "an unselected multi-stage setup must not silently select production: {err}"
    );
}

// An explicit destination wins outright. A broken environment variable must not
// block a caller who already chose a stage, or --base-url becomes unusable on any
// machine with a stale or mistyped selector exported.
#[test]
fn explicit_base_url_wins_over_a_malformed_environment() {
    let home = tempfile::tempdir().unwrap();
    save(home.path(), PROD, OLD, Some(FUTURE));
    let mut cmd = command(home.path());
    cmd.env("RELAYHISTORY_BASE_URL", "not-a-url")
        .arg("--base-url")
        .arg(PROD);
    success(&cmd.output().unwrap(), OLD);
}

// default_base_url() ignores a malformed selector and returns production, so
// treating "the variable is set" as "production was chosen" would silently
// retarget the caller's stage. A malformed selector must be reported, and the
// message must never echo the value: normalize_base_url rejects URLs carrying
// embedded credentials, so a rejected value is exactly the kind that may hold one.
#[test]
fn malformed_base_url_env_is_rejected_without_echoing_the_value() {
    let home = tempfile::tempdir().unwrap();
    save(home.path(), PROD, OLD, Some(FUTURE));
    let secret_shaped = "https://user:hunter2@example.com/path?q=1";
    let mut cmd = command(home.path());
    cmd.env("RELAYHISTORY_BASE_URL", secret_shaped);
    let err = failure(&cmd.output().unwrap());
    assert!(
        err.contains("RELAYHISTORY_BASE_URL is not a usable base URL"),
        "a malformed selector must be named, not silently replaced by production: {err}"
    );
    assert!(
        !err.contains("hunter2") && !err.contains(secret_shaped),
        "the rejected value must never be echoed: {err}"
    );
}
