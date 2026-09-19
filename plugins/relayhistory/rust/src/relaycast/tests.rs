use super::*;
use crate::cloud::tests::ENV_LOCK;
use std::io::Write;
use std::time::Duration;
struct EnvVarGuard {
    key: &'static str,
    old: Option<std::ffi::OsString>,
}
impl EnvVarGuard {
    fn set(key: &'static str, value: &str) -> Self {
        let old = std::env::var_os(key);
        std::env::set_var(key, value);
        Self { key, old }
    }
}
impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        match &self.old {
            Some(value) => std::env::set_var(self.key, value),
            None => std::env::remove_var(self.key),
        }
    }
}
fn fresh_db() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    ai_hist::init_db(&conn).unwrap();
    conn
}
fn history_count(conn: &Connection) -> i64 {
    conn.query_row("SELECT COUNT(*) FROM history", [], |row| row.get(0))
        .unwrap()
}
fn read_request_line(stream: &mut std::net::TcpStream) -> String {
    use std::io::{BufRead, BufReader};

    // Accepted sockets inherit nonblocking mode on macOS.
    stream.set_nonblocking(false).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut first_line = String::new();
    reader.read_line(&mut first_line).unwrap();
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap() == 0 || line.trim_end().is_empty() {
            break;
        }
    }
    first_line.trim_end().to_string()
}

/// Answers exactly `count` HTTP requests off `listener` with `respond`, and
/// hands back the request lines it saw.
fn serve_http(
    listener: std::net::TcpListener,
    count: usize,
    respond: impl Fn(&str) -> (&'static str, String) + Send + 'static,
) -> std::thread::JoinHandle<Vec<String>> {
    listener.set_nonblocking(true).unwrap();
    std::thread::spawn(move || {
        let started = std::time::Instant::now();
        let mut seen = Vec::new();
        while seen.len() < count && started.elapsed() < Duration::from_secs(20) {
            let (mut stream, _) = match listener.accept() {
                Ok(connection) => connection,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(5));
                    continue;
                }
                Err(error) => panic!("accept failed: {error}"),
            };
            let line = read_request_line(&mut stream);
            let (status, body) = respond(&line);
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).unwrap();
            seen.push(line);
        }
        assert_eq!(seen.len(), count, "served {seen:?}");
        seen
    })
}

/// A Relaycast messages page holding ids `m{first}`..`m{first + count - 1}`.
fn relay_message_page(first: usize, count: usize) -> String {
    let messages: Vec<Value> = (first..first + count)
        .map(|n| json!({"id": format!("m{n:03}"), "from_name": "ana", "text": format!("relay message {n:03}")}))
        .collect();
    serde_json::to_string(&json!({ "data": messages })).unwrap()
}

fn relaycast_env(base: &str) -> (EnvVarGuard, EnvVarGuard, EnvVarGuard) {
    (
        EnvVarGuard::set("RELAYCAST_API_KEY", "rc_test_key"),
        EnvVarGuard::set("RELAYCAST_WORKSPACE_ID", "ws-e2e"),
        EnvVarGuard::set("RELAYCAST_BASE_URL", base),
    )
}

#[test]
fn relaycast_sync_pages_through_a_channel_and_saves_the_high_water_mark() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    // channels, messages page 1, messages page 2, dm listing.
    let server = serve_http(listener, 4, |line| {
        if line.starts_with("GET /v1/channels ") {
            ("200 OK", r#"{"data":[{"name":"general"}]}"#.to_string())
        } else if line.starts_with("GET /v1/channels/general/messages?limit=100&after=m099 ") {
            ("200 OK", relay_message_page(100, 2))
        } else if line.starts_with("GET /v1/channels/general/messages?limit=100 ") {
            // A full page is the signal that another page may follow.
            ("200 OK", relay_message_page(0, 100))
        } else if line.starts_with("GET /v1/dm/conversations/all ") {
            ("200 OK", r#"{"data":[]}"#.to_string())
        } else {
            panic!("unexpected request: {line}");
        }
    });

    let conn = fresh_db();
    let mut state = Map::new();
    let _env = relaycast_env(&format!("http://{addr}"));
    let inserted = sync_relaycast(&conn, &mut state).unwrap();
    let requests = server.join().unwrap();

    assert_eq!(inserted, 102);
    assert_eq!(history_count(&conn), 102);
    let locations = ai_hist::session_locations(&conn, "relay", "#general").unwrap();
    assert_eq!(locations, vec!["remote"]);
    assert!(
        requests[2].contains("after=m099"),
        "the second page must continue from the last id of the first: {requests:?}"
    );

    // Messages are attributed to their sender and the workspace.
    let (prompt, project, session_id): (String, Option<String>, Option<String>) = conn
        .query_row(
            "SELECT prompt, project, session_id FROM history WHERE prompt LIKE '%message 000'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(prompt, "[ana] relay message 000");
    assert_eq!(project.as_deref(), Some("ws-e2e"));
    // The channel name is the session; "ch:general" is only the state key.
    assert_eq!(session_id.as_deref(), Some("#general"));

    // The cursor saved is the highest id seen across both pages.
    assert_eq!(state["relay"]["ch:general"], json!("m101"));
}

#[test]
fn relaycast_sync_resumes_from_the_saved_after_cursor() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = serve_http(listener, 3, |line| {
        if line.starts_with("GET /v1/channels ") {
            ("200 OK", r#"{"data":[{"name":"general"}]}"#.to_string())
        } else if line.starts_with("GET /v1/channels/general/messages?limit=100&after=m050 ") {
            ("200 OK", relay_message_page(51, 1))
        } else if line.starts_with("GET /v1/dm/conversations/all ") {
            ("200 OK", r#"{"data":[]}"#.to_string())
        } else {
            panic!("unexpected request: {line}");
        }
    });

    let conn = fresh_db();
    let mut state = Map::new();
    state.insert("relay".into(), json!({"ch:general": "m050"}));
    let _env = relaycast_env(&format!("http://{addr}"));
    let inserted = sync_relaycast(&conn, &mut state).unwrap();
    let requests = server.join().unwrap();

    // Only the one message after the cursor is asked for, and stored.
    assert_eq!(inserted, 1);
    assert_eq!(history_count(&conn), 1);
    assert!(requests[1].contains("after=m050"), "{requests:?}");
    assert_eq!(state["relay"]["ch:general"], json!("m051"));
}

#[test]
fn relaycast_sync_keeps_channel_rows_when_the_dm_listing_is_forbidden() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = serve_http(listener, 3, |line| {
        if line.starts_with("GET /v1/channels ") {
            ("200 OK", r#"{"data":[{"name":"general"}]}"#.to_string())
        } else if line.starts_with("GET /v1/channels/general/messages?limit=100 ") {
            ("200 OK", relay_message_page(0, 1))
        } else if line.starts_with("GET /v1/dm/conversations/all ") {
            // Plenty of API keys have channel scope but no DM scope.
            ("403 Forbidden", r#"{"error":"forbidden"}"#.to_string())
        } else {
            panic!("unexpected request: {line}");
        }
    });

    let conn = fresh_db();
    let mut state = Map::new();
    let _env = relaycast_env(&format!("http://{addr}"));
    let inserted = sync_relaycast(&conn, &mut state).unwrap();
    server.join().unwrap();

    // The DM refusal is tolerated: the channel rows still land.
    assert_eq!(inserted, 1);
    assert_eq!(history_count(&conn), 1);
    assert_eq!(state["relay"]["ch:general"], json!("m000"));
}

#[test]
fn relaycast_sync_is_a_no_op_without_credentials() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|error| error.into_inner());
    let conn = fresh_db();
    let mut state = Map::new();
    // An unreachable base url proves no request is attempted at all.
    let _env = (
        EnvVarGuard::set("RELAYCAST_API_KEY", ""),
        EnvVarGuard::set("RELAYCAST_WORKSPACE_ID", "ws-e2e"),
        EnvVarGuard::set("RELAYCAST_BASE_URL", "http://127.0.0.1:1"),
    );
    assert_eq!(sync_relaycast(&conn, &mut state).unwrap(), 0);
    assert_eq!(history_count(&conn), 0);
    assert!(state.is_empty());
}
