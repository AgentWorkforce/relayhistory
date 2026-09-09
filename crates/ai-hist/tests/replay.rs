use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::path::Path;
use std::process::{Command, Output};
use std::thread;
use std::time::{Duration, Instant};

fn server(pages: Vec<(u16, Value)>) -> (String, thread::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    listener.set_nonblocking(true).unwrap();
    let handle = thread::spawn(move || {
        let mut requests = Vec::new();
        for (status, page) in pages {
            let start = Instant::now();
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(start.elapsed() < Duration::from_secs(10), "missing request");
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("{error}"),
                }
            };
            // The listener is non-blocking so `accept` can poll, and on macOS/BSD the
            // accepted stream INHERITS O_NONBLOCK. `set_read_timeout` does not clear it,
            // so `read_line` below returned WouldBlock the instant the client had not yet
            // written — an intermittent panic that looks like a product bug and is not one.
            stream.set_nonblocking(false).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut request = String::new();
            loop {
                let mut line = String::new();
                let count = reader.read_line(&mut line).unwrap();
                request.push_str(&line);
                if count == 0 || line == "\r\n" {
                    break;
                }
            }
            let body = page.to_string();
            write!(stream, "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
            requests.push(request);
        }
        requests
    });
    (base, handle)
}

fn save_auth(home: &Path, base: &str, legacy: bool) {
    let path = if legacy {
        home.join("auth.json")
    } else {
        let stages = home.join("stages");
        std::fs::create_dir_all(&stages).unwrap();
        stages.join(format!("{}.auth.json", ai_hist_core::prompt_hash(base)))
    };
    std::fs::write(
        path,
        json!({
            "base_url": base, "access_token": "rth_at_test", "refresh_token": null
        })
        .to_string(),
    )
    .unwrap();
}

fn replay(home: &Path, base: &str, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_ai-hist"))
        .env("RELAYHISTORY_HOME", home)
        // replay migrates the legacy TypeScript store under HOME; pin it so the
        // developer's real ~/.config/ai-hist/auth.json cannot influence results.
        .env("HOME", home)
        .env("USERPROFILE", home)
        .env("AI_HIST_CONFIG_DIR", home.join("legacy-sdk"))
        .env("RELAYHISTORY_BASE_URL", base)
        .env_remove("AI_HIST_BASE_URL")
        .arg("--db")
        .arg(home.join("must-not-create.db"))
        .arg("replay")
        .args(args)
        .output()
        .unwrap()
}

fn event(id: &str, ts: &str, content: Value, truncated: bool) -> Value {
    json!({"eventId": id, "ts": ts, "source": "claude", "kind": "prompt",
        "actorName": "Alice", "actorRole": "user", "content": content,
        "contentTruncated": truncated, "futureField": {"preserved": true}})
}

fn success(output: &Output) {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn replay_pages_past_a_short_page_and_preserves_raw_events_and_cursor_ties() {
    let home = tempfile::tempdir().unwrap();
    let ts = "2026-09-06T12:00:00.000Z";
    let first = event("a", ts, json!("first"), false);
    let second = event("b", ts, json!("second"), true);
    let third = event("c", "2026-09-06T12:00:01.000Z", json!("third"), false);
    let cursor = format!("{ts}|a+&?");
    let (base, server) = server(vec![
        (
            200,
            json!({"events": [first.clone()], "nextCursor": cursor}),
        ),
        (
            200,
            json!({"events": [second.clone()], "nextCursor": format!("{ts}|b")}),
        ),
        (200, json!({"events": [third.clone()], "nextCursor": null})),
    ]);
    save_auth(home.path(), &base, false);
    let output = replay(
        home.path(),
        &base,
        &[
            "session/with?reserved",
            "--json",
            "--limit",
            "7",
            "--max-content",
            "12",
        ],
    );
    success(&output);
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap(),
        json!([first, second, third])
    );
    let requests = server.join().unwrap();
    for request in &requests {
        assert!(
            request.starts_with("GET /v1/sessions/session%2Fwith%3Freserved/events?"),
            "{request}"
        );
        assert!(request.contains("Authorization: Bearer rth_at_test"));
        assert!(request.contains("order=asc"));
        assert!(request.contains("limit=7"));
        assert!(request.contains("maxContent=12"));
    }
    let target = requests[1].split_whitespace().nth(1).unwrap();
    let url = url::Url::parse(&format!("{base}{target}")).unwrap();
    assert_eq!(
        url.query_pairs()
            .find(|(key, _)| key == "cursor")
            .unwrap()
            .1,
        cursor
    );
    assert!(!home.path().join("must-not-create.db").exists());
}

#[test]
fn replay_renders_chronology_and_marks_truncation_even_without_content() {
    let home = tempfile::tempdir().unwrap();
    let (base, server) = server(vec![(
        200,
        json!({"events": [
        event("first", "2026-09-06T12:00:00Z", json!("hello\nworld"), false),
        event("second", "2026-09-06T12:01:00Z", Value::Null, true)
    ], "nextCursor": null}),
    )]);
    save_auth(home.path(), &base, false);
    let path = home.path().join("offline.txt");
    let output = replay(
        home.path(),
        &base,
        &["session", "--out", path.to_str().unwrap()],
    );
    success(&output);
    server.join().unwrap();
    assert!(output.stdout.is_empty());
    let text = std::fs::read_to_string(path).unwrap();
    assert!(text.find("(first)").unwrap() < text.find("(second)").unwrap());
    assert!(text.contains("hello\nworld"));
    assert!(text.contains("Actor: Alice"));
    assert!(text.contains("CONTENT TRUNCATED by server maxContent; this event is incomplete"));
    // The notice must be tied to the flag, not printed for every event: asserting only the
    // positive case would pass a regression that marks a complete transcript as truncated,
    // which is the same lie in the opposite direction.
    assert_eq!(
        text.matches("CONTENT TRUNCATED").count(),
        1,
        "only the flagged event may carry the truncation notice"
    );
    let first = &text[text.find("(first)").unwrap()..text.find("(second)").unwrap()];
    assert!(
        !first.contains("CONTENT TRUNCATED"),
        "an event with contentTruncated=false must not be marked truncated"
    );
}

#[test]
fn replay_over_an_existing_transcript_replaces_it() {
    // The overwrite path had no coverage: the only test writing a file first exercised the
    // FAILURE case (expired token leaves it intact). A successful repeat replay to the same
    // path is the normal way this command is used, and it is the case that breaks when the
    // replacement primitive cannot overwrite — `rename` does not, on Windows.
    let home = tempfile::tempdir().unwrap();
    let (base, server) = server(vec![(
        200,
        json!({"events": [event("only", "2026-09-06T12:00:00Z", json!("fresh"), false)], "nextCursor": null}),
    )]);
    save_auth(home.path(), &base, false);
    let path = home.path().join("offline.txt");
    std::fs::write(&path, "stale transcript from an earlier replay").unwrap();
    let output = replay(
        home.path(),
        &base,
        &["session", "--out", path.to_str().unwrap()],
    );
    success(&output);
    server.join().unwrap();
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(text.contains("fresh"), "{text}");
    assert!(
        !text.contains("stale transcript"),
        "the previous transcript must be replaced, not appended to: {text}"
    );
    // The sibling temp file must not survive a successful replace. Match tempfile's own
    // ".tmp" prefix rather than "anything unexpected" — the auth fixture legitimately
    // creates a `stages` directory here, and excluding dotfiles wholesale would have
    // hidden exactly the leftovers this is looking for.
    let leftovers: Vec<_> = std::fs::read_dir(home.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with(".tmp"))
        .collect();
    assert!(
        leftovers.is_empty(),
        "atomic write left temporary files behind: {leftovers:?}"
    );
}

#[test]
fn replay_unknown_session_is_an_empty_array_and_json_can_be_saved() {
    let home = tempfile::tempdir().unwrap();
    let (base, server) = server(vec![(200, json!({"events": [], "nextCursor": null}))]);
    save_auth(home.path(), &base, true);
    let path = home.path().join("offline.json");
    let output = replay(
        home.path(),
        &base,
        &["unknown", "--json", "--out", path.to_str().unwrap()],
    );
    success(&output);
    server.join().unwrap();
    assert!(output.stdout.is_empty());
    assert_eq!(std::fs::read_to_string(path).unwrap(), "[]\n");
    assert!(!home.path().join("must-not-create.db").exists());
}

#[test]
fn replay_without_selected_stage_auth_explains_login_without_opening_sqlite() {
    let home = tempfile::tempdir().unwrap();
    save_auth(home.path(), "http://127.0.0.1:2", false);
    save_auth(home.path(), "http://127.0.0.1:2", true);
    let output = replay(home.path(), "http://127.0.0.1:1", &["session"]);
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(
        error.contains("not authenticated for the selected stage"),
        "{error}"
    );
    assert!(error.contains("ai-hist login"));
    assert!(!error.contains("panicked"));
    assert!(!home.path().join("must-not-create.db").exists());
}

#[test]
fn replay_expired_token_on_later_page_leaves_existing_transcript_intact() {
    let home = tempfile::tempdir().unwrap();
    let (base, server) = server(vec![
        (
            200,
            json!({"events": [event("first", "2026-09-06T12:00:00Z", json!("first"), false)], "nextCursor": "2026-09-06T12:00:00Z|first"}),
        ),
        (401, json!({"error": "expired token"})),
    ]);
    save_auth(home.path(), &base, false);
    let path = home.path().join("offline.txt");
    std::fs::write(&path, "previous transcript").unwrap();
    let output = replay(
        home.path(),
        &base,
        &["session", "--out", path.to_str().unwrap()],
    );
    server.join().unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains("HTTP 401"), "{error}");
    assert!(error.contains("expired or invalid"));
    assert!(error.contains("ai-hist login"));
    assert!(!error.contains("panicked"));
    assert!(output.stdout.is_empty());
    assert_eq!(
        std::fs::read_to_string(path).unwrap(),
        "previous transcript"
    );
}

#[test]
fn replay_rejects_repeated_cursors_instead_of_saving_an_incomplete_transcript() {
    let home = tempfile::tempdir().unwrap();
    let page = json!({"events": [], "nextCursor": "2026-09-06T12:00:00Z|same"});
    let (base, server) = server(vec![(200, page.clone()), (200, page)]);
    save_auth(home.path(), &base, false);
    let output = replay(home.path(), &base, &["session", "--json"]);
    server.join().unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("repeated nextCursor"));
    assert!(output.stdout.is_empty());
}

#[test]
fn replay_explicit_base_url_overrides_default_and_does_not_open_an_invalid_db() {
    let home = tempfile::tempdir().unwrap();
    std::fs::write(home.path().join("must-not-create.db"), "not sqlite").unwrap();
    let (base, server) = server(vec![(200, json!({"events": [], "nextCursor": null}))]);
    save_auth(home.path(), &base, false);
    let output = replay(
        home.path(),
        "http://127.0.0.1:1",
        &["unknown", "--base-url", &base],
    );
    success(&output);
    server.join().unwrap();
    assert!(String::from_utf8_lossy(&output.stdout).contains("No events found."));
    assert_eq!(
        std::fs::read_to_string(home.path().join("must-not-create.db")).unwrap(),
        "not sqlite"
    );
}

// replay used to synthesize its destination with default_base_url(), which turns
// a malformed selector into the production origin, and then passed that on as if
// the user had chosen it. The result was replay silently hitting production while
// token rejected the very same selector. Both must reject it.
#[test]
fn malformed_base_url_env_is_rejected_instead_of_silently_using_production() {
    let home = tempfile::tempdir().unwrap();
    let output = replay(home.path(), "not-a-url", &["some-session"]);
    assert!(!output.status.success());
    let err = String::from_utf8_lossy(&output.stderr);
    assert!(
        err.contains("RELAYHISTORY_BASE_URL is not a usable base URL"),
        "a malformed selector must be reported, not replaced by production: {err}"
    );
    assert!(
        output.stdout.is_empty(),
        "a rejected destination must not print a transcript"
    );
}

// An explicit destination still wins over a broken environment, matching token.
#[test]
fn explicit_base_url_wins_over_a_malformed_environment() {
    let home = tempfile::tempdir().unwrap();
    let (base, server) = server(vec![(200, json!({"events": [], "nextCursor": null}))]);
    save_auth(home.path(), &base, false);
    let output = replay(home.path(), "not-a-url", &["unknown", "--base-url", &base]);
    success(&output);
    server.join().unwrap();
    assert!(String::from_utf8_lossy(&output.stdout).contains("No events found."));
}
