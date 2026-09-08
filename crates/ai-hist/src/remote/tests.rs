//! Tests for the remote session connectors.
//!
//! The network and process boundaries are faked ([`ClaudeSessionsTransport`],
//! [`CodexCloudLister`]), so mapping, pagination, and the engine integration
//! are asserted without a claude.ai account or the Codex CLI installed. The
//! real transports are exercised end-to-end by `tests/session_discovery.rs`
//! (a scripted `codex` binary and a loopback HTTP server).

use super::*;
use crate::discover::{
    discover_sessions_with_providers, list_session_catalog, CatalogListOptions, DiscoverOptions,
};
use ai_hist_core::{init_db, SessionScope};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

fn catalog() -> Connection {
    let conn = Connection::open_in_memory().expect("in-memory database");
    init_db(&conn).expect("schema");
    conn
}

fn env_at<'a>(conn: &'a Connection, home: &Path) -> DiscoveryEnv<'a> {
    DiscoveryEnv::with_roots(conn, home.to_path_buf(), home.join("opencode.db"))
}

fn write_claude_credentials(home: &Path, expires_at_ms: i64) -> PathBuf {
    let path = home.join(".claude/.credentials.json");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(
        &path,
        format!(
            r#"{{"claudeAiOauth":{{"accessToken":"sk-ant-oat01-test","refreshToken":"sk-ant-ort01-test","expiresAt":{expires_at_ms},"scopes":["user:inference"]}}}}"#
        ),
    )
    .unwrap();
    path
}

const FAR_FUTURE_MS: i64 = 4_102_444_800_000; // 2100-01-01

/// Share cloud.rs's lock because both modules change RELAYHISTORY_HOME.
struct ClearedCredentialsOverride {
    previous: Vec<(&'static str, Option<std::ffi::OsString>)>,
    _home: tempfile::TempDir,
    _serialized: std::sync::MutexGuard<'static, ()>,
}

fn without_credentials_override() -> ClearedCredentialsOverride {
    let guard = crate::cloud::tests::ENV_LOCK
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    let home = tempfile::tempdir().unwrap();
    let keys = [
        "RELAYHISTORY_CLAUDE_CREDENTIALS",
        "RELAYHISTORY_HOME",
        "RELAYHISTORY_BASE_URL",
        "AI_HIST_BASE_URL",
    ];
    let previous = keys
        .into_iter()
        .map(|key| {
            let value = std::env::var_os(key);
            std::env::remove_var(key);
            (key, value)
        })
        .collect();
    std::env::set_var("RELAYHISTORY_HOME", home.path());
    ClearedCredentialsOverride {
        previous,
        _home: home,
        _serialized: guard,
    }
}

impl Drop for ClearedCredentialsOverride {
    fn drop(&mut self) {
        for (key, value) in &self.previous {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// claude-web
// ---------------------------------------------------------------------------

/// Serves a fixed sequence of responses and records each requested URL.
struct TransportCall {
    url: String,
    bearer_token: String,
    headers: Vec<(String, String)>,
}

struct ScriptedTransport {
    responses: Vec<(u16, String)>,
    calls: Mutex<Vec<TransportCall>>,
    next: AtomicUsize,
}

impl ScriptedTransport {
    fn new(responses: Vec<(u16, String)>) -> Self {
        Self {
            responses,
            calls: Mutex::new(Vec::new()),
            next: AtomicUsize::new(0),
        }
    }
}

impl ClaudeSessionsTransport for Arc<ScriptedTransport> {
    fn get_with_headers(
        &self,
        url: &str,
        bearer_token: &str,
        headers: &[(&str, &str)],
    ) -> Result<ClaudeHttpResponse> {
        self.calls.lock().unwrap().push(TransportCall {
            url: url.to_string(),
            bearer_token: bearer_token.to_string(),
            headers: headers
                .iter()
                .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
                .collect(),
        });
        let index = self.next.fetch_add(1, Ordering::Relaxed);
        let (status, body) = self
            .responses
            .get(index)
            .cloned()
            .unwrap_or((500, "script exhausted".to_string()));
        Ok(ClaudeHttpResponse { status, body })
    }
}

fn claude_page(sessions: &[Value], next_cursor: Option<&str>) -> String {
    serde_json::json!({
        "data": sessions,
        "next_cursor": next_cursor,
    })
    .to_string()
}

fn web_session(id: &str, title: &str, last_event_at: &str) -> Value {
    serde_json::json!({
        "id": id,
        "title": title,
        "status": "idle",
        "worker_status": "idle",
        "created_at": "2026-06-20T09:00:00Z",
        "last_event_at": last_event_at,
        "environment_kind": "cloud",
        "config": {
            "sources": [
                {"type": "git_repository", "url": "https://github.com/acme/api"}
            ]
        }
    })
}

fn claude_provider(
    home: &Path,
    transport: &Arc<ScriptedTransport>,
    limit: Option<usize>,
) -> ClaudeWebProvider {
    ClaudeWebProvider::new(
        write_claude_credentials(home, FAR_FUTURE_MS),
        "https://api.example.test".to_string(),
        Box::new(Arc::clone(transport)),
        limit,
    )
}

#[test]
fn claude_mapping_carries_observed_fields_and_nothing_invented() {
    let (candidate, session) = map_claude_web_session(&web_session(
        "session_01abc",
        "Fix login flow",
        "2026-06-21T10:00:00Z",
    ))
    .expect("a mappable session");
    assert_eq!(session.source, "claude");
    assert_eq!(session.session_id, "session_01abc");
    assert_eq!(session.first_prompt.as_deref(), Some("Fix login flow"));
    assert_eq!(
        session.repo_url.as_deref(),
        Some("https://github.com/acme/api")
    );
    assert_eq!(
        session.raw_path.as_deref(),
        Some("https://claude.ai/code/session_01abc")
    );
    assert_eq!(session.discovery_state, "shallow");
    assert_eq!(session.cwd, None);
    assert_eq!(session.git_branch, None);
    assert!(session.models.is_empty());
    assert!(session.first_activity_ms.unwrap() < session.last_activity_ms.unwrap());
    assert_eq!(candidate.session_id.as_deref(), Some("session_01abc"));
    assert_eq!(candidate.recency_hint_ms, session.last_activity_ms);
    assert_eq!(candidate.stamp, "web:2026-06-21T10:00:00Z");
}

#[test]
fn claude_mapping_skips_bridges_and_malformed_ids() {
    let mut bridge = web_session("session_01abc", "t", "2026-06-21T10:00:00Z");
    bridge["environment_kind"] = "bridge".into();
    assert!(map_claude_web_session(&bridge).is_none());

    for bad_id in ["", "sess_x", "session_", "session_a b", "task_e_1"] {
        let entry = web_session(bad_id, "t", "2026-06-21T10:00:00Z");
        assert!(
            map_claude_web_session(&entry).is_none(),
            "id {bad_id:?} must not map"
        );
    }
    assert!(
        map_claude_web_session(&web_session("cse_9X-y_z", "t", "2026-06-21T10:00:00Z")).is_some()
    );
}

#[test]
fn claude_enumeration_pages_until_the_cursor_ends() {
    let _credentials = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let transport = Arc::new(ScriptedTransport::new(vec![
        (
            200,
            claude_page(
                &[web_session("session_01", "one", "2026-06-21T10:00:00Z")],
                Some("cursor with spaces"),
            ),
        ),
        (
            200,
            claude_page(
                &[web_session("session_02", "two", "2026-06-20T10:00:00Z")],
                None,
            ),
        ),
    ]));
    let provider = claude_provider(home.path(), &transport, None);
    let conn = catalog();
    let env = env_at(&conn, home.path());
    let candidates = provider.enumerate(&env, None).unwrap();
    assert_eq!(candidates.len(), 2);
    let calls = transport.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert!(calls[0].url.ends_with("/v1/code/sessions?limit=100"));
    assert!(calls[1].url.contains("cursor=cursor%20with%20spaces"));
    assert!(calls
        .iter()
        .all(|call| call.bearer_token == "sk-ant-oat01-test"));
}

#[test]
fn claude_enumeration_respects_the_row_limit() {
    let _credentials = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let transport = Arc::new(ScriptedTransport::new(vec![(
        200,
        claude_page(
            &[
                web_session("session_01", "one", "2026-06-21T10:00:00Z"),
                web_session("session_02", "two", "2026-06-20T10:00:00Z"),
            ],
            Some("more"),
        ),
    )]));
    let provider = claude_provider(home.path(), &transport, Some(2));
    let conn = catalog();
    let env = env_at(&conn, home.path());
    let candidates = provider.enumerate(&env, None).unwrap();
    // The limit is satisfied by the first page, so the cursor is not followed.
    assert_eq!(candidates.len(), 2);
    let calls = transport.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert!(calls[0].url.ends_with("?limit=2"));
}

#[test]
fn claude_enumeration_reports_a_rejected_token() {
    let _credentials = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let transport = Arc::new(ScriptedTransport::new(vec![(401, "{}".to_string())]));
    let provider = claude_provider(home.path(), &transport, None);
    let conn = catalog();
    let env = env_at(&conn, home.path());
    let error = provider.enumerate(&env, None).unwrap_err().to_string();
    assert!(error.contains("rejected the stored OAuth token"), "{error}");
}

#[test]
fn claude_enumeration_reports_an_expired_token_without_a_request() {
    let _credentials = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let transport = Arc::new(ScriptedTransport::new(vec![]));
    let provider = ClaudeWebProvider::new(
        write_claude_credentials(home.path(), 1_000),
        "https://api.example.test".to_string(),
        Box::new(Arc::clone(&transport)),
        None,
    );
    let conn = catalog();
    let env = env_at(&conn, home.path());
    let error = provider.enumerate(&env, None).unwrap_err().to_string();
    assert!(error.contains("expired"), "{error}");
    assert!(
        transport.calls.lock().unwrap().is_empty(),
        "an expired token must be rejected before any request is made"
    );
}

#[test]
fn claude_transport_refuses_plaintext_off_loopback() {
    assert!(require_https_or_loopback("https://api.anthropic.com").is_ok());
    assert!(require_https_or_loopback("http://127.0.0.1:8787").is_ok());
    assert!(require_https_or_loopback("http://localhost:1234").is_ok());
    assert!(require_https_or_loopback("http://[::1]:8787").is_ok());
    let error = require_https_or_loopback("http://api.evil.test").unwrap_err();
    assert!(error.to_string().contains("plain http"));
    // Userinfo must not smuggle a loopback-looking authority past the check.
    assert!(require_https_or_loopback("http://127.0.0.1@evil.test").is_err());
}

#[test]
fn claude_targeted_evidence_hydrates_one_session_across_pages() {
    let _credentials = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    write_claude_credentials(home.path(), FAR_FUTURE_MS);
    let transport = Arc::new(ScriptedTransport::new(vec![
        (
            200,
            serde_json::json!({
                "account": {"uuid": "account-test", "email": "test@example.test"},
                "organization": {"uuid": "org-test"}
            })
            .to_string(),
        ),
        (
            200,
            serde_json::json!({
                "data": [{"payload": {"sessionId": "session_01abc", "uuid": "u1", "type": "user", "timestamp": 1, "message": {"role": "user", "content": "hello"}}}],
                "next_cursor": "next page"
            })
            .to_string(),
        ),
        (
            200,
            serde_json::json!({
                "data": [{"payload": {"sessionId": "session_01abc", "uuid": "a1", "type": "assistant", "timestamp": 2, "message": {"role": "assistant", "content": [{"type": "text", "text": "done"}]}}}],
                "next_cursor": null
            })
            .to_string(),
        ),
    ]));
    let acquired = acquire_claude_remote_session_at(
        home.path(),
        "session_01abc",
        "https://api.example.test",
        &transport,
    )
    .unwrap();
    let RemoteSessionEvidence::ClaudeFull {
        records,
        source_bytes,
        ..
    } = acquired
    else {
        panic!("expected full Claude evidence")
    };
    assert_eq!(records.len(), 2);
    assert!(source_bytes > 0);
    let calls = transport.calls.lock().unwrap();
    assert_eq!(calls.len(), 3);
    assert!(calls[0].url.ends_with("/api/oauth/profile"));
    assert!(calls[1]
        .url
        .contains("session_01abc/teleport-events?limit=1000"));
    assert!(calls[2].url.contains("cursor=next%20page"));
    assert!(calls
        .iter()
        .all(|call| call.bearer_token == "sk-ant-oat01-test"));
    assert_eq!(
        calls[1].headers,
        vec![("x-organization-uuid".to_string(), "org-test".to_string())]
    );
    assert_eq!(calls[2].headers, calls[1].headers);
}

#[test]
fn claude_targeted_evidence_distinguishes_auth_missing_and_malformed_records() {
    let _credentials = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let unconfigured = acquire_claude_remote_session_at(
        home.path(),
        "session_01abc",
        "https://api.example.test",
        &Arc::new(ScriptedTransport::new(vec![])),
    )
    .unwrap();
    assert!(matches!(
        unconfigured,
        RemoteSessionEvidence::CapabilityLimited {
            code: "CONNECTOR_NOT_CONFIGURED",
            ..
        }
    ));

    write_claude_credentials(home.path(), FAR_FUTURE_MS);
    let expired = Arc::new(ScriptedTransport::new(vec![(
        401,
        "token-secret-must-not-escape".into(),
    )]));
    let error = acquire_claude_remote_session_at(
        home.path(),
        "session_01abc",
        "https://api.example.test",
        &expired,
    )
    .unwrap_err()
    .to_string();
    assert!(error.starts_with("AUTHENTICATION_EXPIRED:"), "{error}");
    assert!(!error.contains("token-secret"), "{error}");

    let malformed = Arc::new(ScriptedTransport::new(vec![
        (200, r#"{"organization":{"uuid":"org-test"}}"#.into()),
        (
            200,
            r#"{"data":[{"payload":"not-an-object"}],"next_cursor":null}"#.into(),
        ),
    ]));
    let error = acquire_claude_remote_session_at(
        home.path(),
        "session_01abc",
        "https://api.example.test",
        &malformed,
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("malformed record"), "{error}");
}

#[test]
fn claude_targeted_evidence_rejects_redirects_and_is_page_bounded() {
    let _credentials = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    write_claude_credentials(home.path(), FAR_FUTURE_MS);
    let redirect = Arc::new(ScriptedTransport::new(vec![(
        302,
        "https://evil.test".into(),
    )]));
    let error = acquire_claude_remote_session_at(
        home.path(),
        "session_01abc",
        "https://api.example.test",
        &redirect,
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("OAuth profile failed (HTTP 302)"), "{error}");

    let mut pages = vec![(200, r#"{"organization":{"uuid":"org-test"}}"#.into())];
    pages.extend(
        (0..MAX_LIST_PAGES).map(|_| (200, r#"{"data":[],"next_cursor":"again"}"#.to_string())),
    );
    let bounded = Arc::new(ScriptedTransport::new(pages));
    let error = acquire_claude_remote_session_at(
        home.path(),
        "session_01abc",
        "https://api.example.test",
        &bounded,
    )
    .unwrap_err()
    .to_string();
    assert!(error.starts_with("EVIDENCE_PARTIAL:"), "{error}");
    assert_eq!(bounded.calls.lock().unwrap().len(), MAX_LIST_PAGES + 1);
}

#[test]
fn real_claude_transport_never_follows_a_redirect_with_credentials() {
    use std::io::{Read, Write};
    use std::net::TcpListener;

    let _credentials = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    write_claude_credentials(home.path(), FAR_FUTURE_MS);
    let target = TcpListener::bind("127.0.0.1:0").unwrap();
    target.set_nonblocking(true).unwrap();
    let target_url = format!("http://{}", target.local_addr().unwrap());
    let origin = TcpListener::bind("127.0.0.1:0").unwrap();
    let origin_url = format!("http://{}", origin.local_addr().unwrap());
    let server = std::thread::spawn(move || {
        let (mut stream, _) = origin.accept().unwrap();
        let mut request = [0u8; 4096];
        let _ = stream.read(&mut request).unwrap();
        write!(
            stream,
            "HTTP/1.1 302 Found\r\nLocation: {target_url}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        )
        .unwrap();
    });
    let error = acquire_claude_remote_session_at(
        home.path(),
        "session_01abc",
        &origin_url,
        &UreqClaudeTransport,
    )
    .unwrap_err()
    .to_string();
    server.join().unwrap();
    assert!(error.contains("HTTP 302"), "{error}");
    std::thread::sleep(Duration::from_millis(50));
    assert!(
        matches!(target.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
    );
}

#[cfg(unix)]
#[test]
fn codex_targeted_evidence_uses_the_cli_diff_and_reports_an_empty_diff_honestly() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let command = dir.path().join("codex-fixture");
    std::fs::write(
        &command,
        "#!/bin/sh\n\
         test \"$1\" = cloud && test \"$2\" = diff && test \"$3\" = task_fixture || exit 2\n\
         printf 'diff --git a/src/lib.rs b/src/lib.rs\\n--- a/src/lib.rs\\n+++ b/src/lib.rs\\n@@ -1 +1,2 @@\\n old\\n+new\\n'\n",
    )
    .unwrap();
    std::fs::set_permissions(&command, std::fs::Permissions::from_mode(0o755)).unwrap();

    let evidence =
        acquire_codex_remote_session_with_command("task_fixture", command.as_os_str()).unwrap();
    let RemoteSessionEvidence::CodexDiff {
        diff, source_bytes, ..
    } = evidence
    else {
        panic!("expected the supported task diff")
    };
    assert!(diff.contains("diff --git a/src/lib.rs b/src/lib.rs"));
    assert_eq!(source_bytes, diff.len() as i64);

    std::fs::write(
        &command,
        "#!/bin/sh\ntest \"$1\" = cloud && test \"$2\" = diff && test \"$3\" = task_fixture\n",
    )
    .unwrap();
    let evidence =
        acquire_codex_remote_session_with_command("task_fixture", command.as_os_str()).unwrap();
    assert!(matches!(
        evidence,
        RemoteSessionEvidence::CapabilityLimited {
            code: "PROVIDER_CAPABILITY_LIMITED",
            ..
        }
    ));
}

#[test]
fn codex_targeted_evidence_requires_the_supplied_homes_login() {
    let home = tempfile::tempdir().unwrap();
    let evidence = acquire_remote_session_at(home.path(), "codex", "task_fixture").unwrap();
    assert!(matches!(
        evidence,
        RemoteSessionEvidence::CapabilityLimited {
            code: "CONNECTOR_NOT_CONFIGURED",
            ..
        }
    ));
}

#[cfg(unix)]
#[test]
fn codex_output_limit_drains_the_pipe_and_reports_size_not_timeout() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let command = dir.path().join("codex-oversized");
    std::fs::write(&command, "#!/bin/sh\nhead -c 16777217 /dev/zero\n").unwrap();
    std::fs::set_permissions(&command, std::fs::Permissions::from_mode(0o755)).unwrap();
    let error = acquire_codex_remote_session_with_command_timeout(
        "task_fixture",
        command.as_os_str(),
        Duration::from_secs(5),
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("16 MiB response-size limit"), "{error}");
}

#[cfg(unix)]
#[test]
fn codex_inherited_pipe_is_bounded_after_the_direct_child_exits() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let command = dir.path().join("codex-inherited-pipe");
    std::fs::write(&command, "#!/bin/sh\nsleep 2 &\nexit 0\n").unwrap();
    std::fs::set_permissions(&command, std::fs::Permissions::from_mode(0o755)).unwrap();
    let started = Instant::now();
    let error = acquire_codex_remote_session_with_command_timeout(
        "task_fixture",
        command.as_os_str(),
        Duration::from_millis(500),
    )
    .unwrap_err()
    .to_string();
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(error.contains("pipe remained open"), "{error}");
}

// ---------------------------------------------------------------------------
// codex-cloud
// ---------------------------------------------------------------------------

/// Serves a fixed sequence of listing pages and records each (limit, cursor)
/// the provider asked for.
struct ScriptedLister {
    pages: Vec<String>,
    calls: Mutex<Vec<(usize, Option<String>)>>,
    next: AtomicUsize,
}

impl ScriptedLister {
    fn new(pages: Vec<String>) -> Self {
        Self {
            pages,
            calls: Mutex::new(Vec::new()),
            next: AtomicUsize::new(0),
        }
    }
}

impl CodexCloudLister for Arc<ScriptedLister> {
    fn list_json(&self, limit: usize, cursor: Option<&str>) -> Result<String> {
        self.calls
            .lock()
            .unwrap()
            .push((limit, cursor.map(str::to_string)));
        let index = self.next.fetch_add(1, Ordering::Relaxed);
        Ok(self
            .pages
            .get(index)
            .cloned()
            .unwrap_or_else(|| r#"{"tasks":[],"cursor":null}"#.to_string()))
    }
}

const CODEX_LISTING: &str = r#"{
  "tasks": [
    {
      "id": "task_e_123",
      "url": "https://chatgpt.com/codex/tasks/task_e_123",
      "title": "Fix the flaky retry test",
      "status": "ready",
      "updated_at": "2026-06-22T09:00:00Z",
      "environment_id": "env_1",
      "environment_label": "api",
      "summary": "1 file changed",
      "is_review": false,
      "attempt_total": 1
    },
    {"title": "no id, not a task"}
  ],
  "cursor": null
}"#;

#[test]
fn codex_listing_parses_both_documented_shapes() {
    let page = parse_codex_cloud_listing(CODEX_LISTING).unwrap();
    assert_eq!(page.tasks.len(), 2);
    assert_eq!(page.cursor, None);
    let bare = parse_codex_cloud_listing(r#"[{"id":"task_e_1"}]"#).unwrap();
    assert_eq!(bare.tasks.len(), 1);
    assert_eq!(bare.cursor, None, "a bare array carries no continuation");
    let continued =
        parse_codex_cloud_listing(r#"{"tasks":[{"id":"task_e_2"}],"cursor":"page-2"}"#).unwrap();
    assert_eq!(continued.cursor.as_deref(), Some("page-2"));
    assert!(parse_codex_cloud_listing("not json").is_err());
    assert!(parse_codex_cloud_listing(r#"{"cursor":null}"#).is_err());
}

#[test]
fn codex_mapping_carries_the_task_listing_and_stamps_on_status() {
    let tasks = parse_codex_cloud_listing(CODEX_LISTING).unwrap().tasks;
    let (candidate, session) = map_codex_cloud_task(&tasks[0]).expect("a mappable task");
    assert_eq!(session.source, "codex");
    assert_eq!(session.session_id, "task_e_123");
    assert_eq!(
        session.first_prompt.as_deref(),
        Some("Fix the flaky retry test")
    );
    assert_eq!(
        session.raw_path.as_deref(),
        Some("https://chatgpt.com/codex/tasks/task_e_123")
    );
    assert_eq!(session.cwd, None);
    assert_eq!(candidate.stamp, "cloud:2026-06-22T09:00:00Z:ready");
    // An id-less entry is not a task.
    assert!(map_codex_cloud_task(&tasks[1]).is_none());

    // A pathological title is bounded like every stored excerpt.
    let long = serde_json::json!({"id": "task_e_long", "title": "x".repeat(9000)});
    let (_, session) = map_codex_cloud_task(&long).unwrap();
    assert_eq!(
        session.first_prompt.unwrap().chars().count(),
        crate::discover::EXCERPT_MAX_CHARS
    );
    let long_web = {
        let mut entry = web_session("session_01long", "t", "2026-06-21T10:00:00Z");
        entry["title"] = serde_json::Value::String("y".repeat(9000));
        entry
    };
    let (_, session) = map_claude_web_session(&long_web).unwrap();
    assert_eq!(
        session.first_prompt.unwrap().chars().count(),
        crate::discover::EXCERPT_MAX_CHARS
    );
}

#[test]
fn codex_provider_forwards_a_bounded_page_limit_to_the_cli() {
    let lister = Arc::new(ScriptedLister::new(vec![CODEX_LISTING.to_string()]));
    let provider = CodexCloudProvider::new(Box::new(Arc::clone(&lister)), Some(7));
    let home = tempfile::tempdir().unwrap();
    let conn = catalog();
    let env = env_at(&conn, home.path());
    let candidates = provider.enumerate(&env, None).unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(*lister.calls.lock().unwrap(), vec![(7, None)]);

    // A global limit above the CLI's window is clamped per page, never
    // forwarded verbatim (the CLI rejects --limit values over 20).
    let lister = Arc::new(ScriptedLister::new(vec![CODEX_LISTING.to_string()]));
    let provider = CodexCloudProvider::new(Box::new(Arc::clone(&lister)), Some(500));
    let env = env_at(&conn, home.path());
    provider.enumerate(&env, None).unwrap();
    assert_eq!(*lister.calls.lock().unwrap(), vec![(20, None)]);
}

#[test]
fn codex_provider_follows_the_cursor_across_pages() {
    let page_one = r#"{"tasks":[{"id":"task_e_1","title":"one","status":"ready","updated_at":"2026-06-22T09:00:00Z"}],"cursor":"page-2"}"#;
    let page_two = r#"{"tasks":[{"id":"task_e_2","title":"two","status":"ready","updated_at":"2026-06-21T09:00:00Z"}],"cursor":null}"#;
    let lister = Arc::new(ScriptedLister::new(vec![
        page_one.to_string(),
        page_two.to_string(),
    ]));
    let provider = CodexCloudProvider::new(Box::new(Arc::clone(&lister)), None);
    let home = tempfile::tempdir().unwrap();
    let conn = catalog();
    let env = env_at(&conn, home.path());
    let candidates = provider.enumerate(&env, None).unwrap();
    assert_eq!(candidates.len(), 2);
    assert_eq!(
        *lister.calls.lock().unwrap(),
        vec![(20, None), (20, Some("page-2".to_string()))]
    );

    // A satisfied row limit ends the walk without following the cursor.
    let lister = Arc::new(ScriptedLister::new(vec![
        page_one.to_string(),
        page_two.to_string(),
    ]));
    let provider = CodexCloudProvider::new(Box::new(Arc::clone(&lister)), Some(1));
    let env = env_at(&conn, home.path());
    let candidates = provider.enumerate(&env, None).unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(*lister.calls.lock().unwrap(), vec![(1, None)]);
}

// ---------------------------------------------------------------------------
// availability
// ---------------------------------------------------------------------------

#[test]
fn statuses_report_missing_credentials_with_the_paths_looked_at() {
    // A developer's ambient credentials override must not leak into the
    // isolated home this test asserts against.
    let _cleared = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let statuses = remote_connector_statuses_at(home.path());
    assert_eq!(statuses.len(), 3);
    assert!(statuses.iter().all(|status| !status.configured));
    let error = ensure_remote_connectors_configured_at("discovery", home.path())
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("no remote provider connectors are configured"),
        "{error}"
    );
    assert!(error.contains("claude-web"), "{error}");
    assert!(error.contains("codex-cloud"), "{error}");

    std::fs::create_dir_all(home.path().join(".codex")).unwrap();
    std::fs::write(home.path().join(".codex/auth.json"), "{}").unwrap();
    let statuses = remote_connector_statuses_at(home.path());
    assert!(statuses.iter().any(|status| status.configured));
    assert!(ensure_remote_connectors_configured_at("discovery", home.path()).is_ok());
}

#[test]
fn a_source_filter_that_excludes_every_configured_connector_is_unsupported() {
    let _cleared = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(home.path().join(".codex")).unwrap();
    std::fs::write(home.path().join(".codex/auth.json"), "{}").unwrap();

    let ok = |sources: &[&str]| {
        ensure_remote_connectors_configured_for_at(
            "discovery",
            home.path(),
            &sources.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        )
    };
    assert!(ok(&[]).is_ok());
    assert!(ok(&["codex"]).is_ok());
    assert!(
        ok(&["codex", "claude"]).is_ok(),
        "one configured match is enough"
    );

    // Only codex is configured, so a claude-only request is unsupported…
    let error = ok(&["claude"]).unwrap_err().to_string();
    assert!(
        error.contains("no remote provider connectors are configured"),
        "{error}"
    );
    assert!(error.contains("claude-web"), "{error}");
    assert!(!error.contains("codex-cloud"), "{error}");
    // Cloud can serve cursor, but it is not configured in this isolated home.
    let error = ok(&["cursor"]).unwrap_err().to_string();
    assert!(
        error.starts_with("no remote provider connectors are configured"),
        "{error}"
    );
    // A misspelled source is an invalid argument, not an unsupported request.
    let error = ok(&["bogus"]).unwrap_err().to_string();
    assert!(error.contains("invalid source 'bogus'"), "{error}");
}

// ---------------------------------------------------------------------------
// engine integration
// ---------------------------------------------------------------------------

fn remote_codex_provider(payload: &str, limit: Option<usize>) -> Box<dyn ShallowSessionProvider> {
    Box::new(CodexCloudProvider::new(
        Box::new(Arc::new(ScriptedLister::new(vec![payload.to_string()]))),
        limit,
    ))
}

#[test]
fn remote_rows_land_with_a_remote_presence_and_skip_on_an_unchanged_stamp() {
    let home = tempfile::tempdir().unwrap();
    let conn = catalog();
    let env = env_at(&conn, home.path());
    let options = DiscoverOptions {
        scope: SessionScope::Remote,
        ..Default::default()
    };

    let providers = vec![remote_codex_provider(CODEX_LISTING, None)];
    let mut rows = Vec::new();
    let summary = discover_sessions_with_providers(&env, &options, &providers, |session| {
        rows.push(session.clone())
    })
    .unwrap();
    assert_eq!(summary.locations_run, ["remote"]);
    assert_eq!(summary.discovered, 1);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].locations, ["remote"]);
    assert_eq!(rows[0].discovery_state, "shallow");
    assert_eq!(summary.counters.files_opened, 0);

    // Same listing again: the stamp matches the stored remote presence, so the
    // row is served from the catalog without a fresh "read".
    let providers = vec![remote_codex_provider(CODEX_LISTING, None)];
    let env = env_at(&conn, home.path());
    let summary = discover_sessions_with_providers(&env, &options, &providers, |_| {}).unwrap();
    assert_eq!(summary.discovered, 0);
    assert_eq!(summary.skipped_unchanged, 1);

    // A status change alone re-reads the task even though the timestamp is
    // unchanged.
    let changed = CODEX_LISTING.replace("\"ready\"", "\"applied\"");
    let providers = vec![remote_codex_provider(&changed, None)];
    let env = env_at(&conn, home.path());
    let summary = discover_sessions_with_providers(&env, &options, &providers, |_| {}).unwrap();
    assert_eq!(summary.discovered, 1);
}

/// A minimal local adapter for engine tests: one prebuilt session, emitted
/// as a candidate with its id known up front.
struct FakeLocalProvider {
    session: ShallowSession,
    stamp: &'static str,
}

impl ShallowSessionProvider for FakeLocalProvider {
    fn source(&self) -> &'static str {
        "codex"
    }

    fn enumerate(
        &self,
        _env: &DiscoveryEnv<'_>,
        _requested_limit: Option<usize>,
    ) -> Result<Vec<Candidate>> {
        Ok(vec![Candidate {
            source: "codex",
            locator: self.session.raw_path.clone().unwrap_or_default(),
            session_id: Some(self.session.session_id.clone()),
            // Newer than the remote candidate, so the local read lands first
            // in the window and the remote upsert is the later merge.
            recency_hint_ms: Some(2_000_000_000_000),
            stamp: self.stamp.to_string(),
        }])
    }

    fn read_shallow(
        &self,
        _scan: &ScanEnv<'_>,
        _catalog: Option<&Connection>,
        _candidate: &Candidate,
    ) -> Result<Option<ShallowSession>> {
        Ok(Some(self.session.clone()))
    }
}

#[test]
fn one_window_merging_local_and_remote_emits_the_fully_merged_row() {
    let home = tempfile::tempdir().unwrap();
    let conn = catalog();
    let env = env_at(&conn, home.path());
    let providers: Vec<Box<dyn ShallowSessionProvider>> = vec![
        Box::new(FakeLocalProvider {
            session: ShallowSession {
                source: "codex".into(),
                session_id: "task_e_123".into(),
                cwd: Some("/work/api".into()),
                raw_path: Some("/home/x/.codex/sessions/rollout.jsonl".into()),
                ..Default::default()
            },
            stamp: "local-stamp",
        }),
        remote_codex_provider(CODEX_LISTING, None),
    ];
    let options = DiscoverOptions {
        scope: SessionScope::All,
        ..Default::default()
    };
    let mut rows = Vec::new();
    let summary = discover_sessions_with_providers(&env, &options, &providers, |session| {
        rows.push(session.clone())
    })
    .unwrap();
    assert_eq!(summary.locations_run, ["local", "remote"]);
    // One logical session reached through both adapters in one window: the
    // emitted row must be the final merged state, not the first upsert.
    assert_eq!(rows.len(), 1, "{rows:#?}");
    assert_eq!(rows[0].locations, ["local", "remote"]);
    assert_eq!(rows[0].cwd.as_deref(), Some("/work/api"));
    assert_eq!(
        summary.discovered, 2,
        "both adapters performed a fresh read"
    );
}

#[test]
fn a_session_seen_locally_and_remotely_is_one_row_with_both_presences() {
    let home = tempfile::tempdir().unwrap();
    let conn = catalog();

    // The same codex session id observed locally first…
    let local = ShallowSession {
        source: "codex".into(),
        session_id: "task_e_123".into(),
        cwd: Some("/work/api".into()),
        raw_path: Some("/home/x/.codex/sessions/rollout.jsonl".into()),
        source_stamp: Some("v2:local".into()),
        discovery_state: "shallow".into(),
        ..Default::default()
    };
    crate::discover::upsert_shallow_session(&conn, &local).unwrap();

    // …then discovered remotely.
    let env = env_at(&conn, home.path());
    let options = DiscoverOptions {
        scope: SessionScope::Remote,
        ..Default::default()
    };
    let providers = vec![remote_codex_provider(CODEX_LISTING, None)];
    let mut rows = Vec::new();
    discover_sessions_with_providers(&env, &options, &providers, |session| {
        rows.push(session.clone())
    })
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].locations, ["local", "remote"]);
    // The remote pass must not clobber locally observed metadata.
    assert_eq!(rows[0].cwd.as_deref(), Some("/work/api"));

    // Scoped listings serve the one canonical row from either side.
    let remote_only = list_session_catalog(
        &conn,
        &CatalogListOptions {
            scope: SessionScope::Remote,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(remote_only.len(), 1);
    let local_only = list_session_catalog(
        &conn,
        &CatalogListOptions {
            scope: SessionScope::Local,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(local_only.len(), 1);
}

// ---------------------------------------------------------------------------
// cloud: real loopback HTTP, synthetic stored auth, and catalog integration
// ---------------------------------------------------------------------------

fn cloud_auth(base_url: &str) -> crate::cloud::StoredAuth {
    crate::cloud::StoredAuth {
        base_url: base_url.into(),
        access_token: "rth_at_synthetic".into(),
        access_token_expires_at: Some("2100-01-01T00:00:00Z".into()),
        org_id: Some("org-teammates".into()),
        ..Default::default()
    }
}

fn cloud_listing(source: &str, id: &str) -> Value {
    serde_json::json!({"sessions": [{
        "source": source, "sessionId": id, "firstTs": "2026-09-08T09:00:00Z",
        "lastTs": "2026-09-08T10:00:00Z", "eventCount": 2,
        "summary": "A teammate fixed the connector", "models": ["test-model"]
    }], "nextCursor": null})
}

fn cloud_http(pages: Vec<(u16, Value)>) -> (String, std::thread::JoinHandle<Vec<String>>) {
    use std::io::{BufRead, BufReader, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let handle = std::thread::spawn(move || {
        let mut requests = Vec::new();
        for (status, body) in pages {
            let start = Instant::now();
            let (mut stream, _) = loop {
                match listener.accept() {
                    Ok(value) => break value,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(
                            start.elapsed() < Duration::from_secs(10),
                            "mock HTTP request timed out"
                        );
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("{error}"),
                }
            };
            // Accepted sockets inherit nonblocking mode on macOS.
            stream.set_nonblocking(false).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut request = String::new();
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                request.push_str(&line);
            }
            // Login/refresh POSTs must be fully consumed before closing the
            // socket, otherwise unread body bytes can reset the client stream.
            let content_length = request
                .lines()
                .filter_map(|line| line.split_once(':'))
                .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                .map(|(_, value)| value.trim().parse::<usize>().unwrap())
                .unwrap_or(0);
            let mut body_bytes = vec![0; content_length];
            reader.read_exact(&mut body_bytes).unwrap();
            assert!(
                !["org_id=", "orgId=", "workspace_id=", "workspaceId="]
                    .iter()
                    .any(|selector| request.contains(selector)),
                "{request}"
            );
            let body = body.to_string();
            write!(stream, "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
            requests.push(request);
        }
        requests
    });
    (base, handle)
}

#[test]
fn cloud_unconfigured_empty_home_preserves_error_prefix() {
    let _isolated = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let status = remote_connector_statuses_at(home.path())
        .into_iter()
        .find(|s| s.connector == CLOUD_CONNECTOR)
        .unwrap();
    assert!(!status.configured);
    let error = ensure_remote_connectors_configured_for_at("discovery", home.path(), &[])
        .unwrap_err()
        .to_string();
    assert!(
        error.starts_with("no remote provider connectors are configured"),
        "{error}"
    );
    assert!(error.contains("cloud: "), "{error}");
    println!("negative: {error}");
}

#[test]
fn cloud_positive_catalog_preserves_source_remote_marker_and_cache() {
    let _isolated = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let page = cloud_listing("cursor", "teammate-1");
    let (base, server) = cloud_http(vec![(200, page.clone()), (200, page)]);
    crate::cloud::save_auth(&cloud_auth(&base)).unwrap();
    ensure_remote_connectors_configured_for_at("discovery", home.path(), &["cursor".into()])
        .unwrap();
    let conn = catalog();
    let options = DiscoverOptions {
        scope: SessionScope::Remote,
        sources: vec!["cursor".into()],
        ..Default::default()
    };
    let first =
        crate::discover::discover_sessions_with_env(&env_at(&conn, home.path()), &options, |_| {})
            .unwrap();
    assert_eq!(first.discovered, 1);
    let presence: (String, String, String) = conn.query_row(
        "SELECT source, location, raw_locator FROM session_presences WHERE session_id = 'teammate-1'", [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap();
    assert_eq!(
        presence,
        (
            "cursor".into(),
            "remote".into(),
            "cloud://org-teammates/teammate-1".into()
        )
    );
    let second =
        crate::discover::discover_sessions_with_env(&env_at(&conn, home.path()), &options, |_| {})
            .unwrap();
    assert_eq!(second.discovered, 0);
    assert_eq!(second.skipped_unchanged, 1);
    let requests = server.join().unwrap();
    assert!(requests.iter().all(|r| r.starts_with("GET /v1/sessions?")
        && r.contains("source=cursor")
        && r.contains("Authorization: Bearer rth_at_synthetic")));
    println!(
        "positive: {presence:?}; second discovery skipped_unchanged={}",
        second.skipped_unchanged
    );
}

#[test]
fn cloud_teleport_adds_second_presence_and_keeps_local_evidence() {
    let _isolated = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let (base, server) = cloud_http(vec![(200, cloud_listing("codex", "teleport-1"))]);
    crate::cloud::save_auth(&cloud_auth(&base)).unwrap();
    let conn = catalog();
    crate::discover::upsert_shallow_session(
        &conn,
        &ShallowSession {
            source: "codex".into(),
            session_id: "teleport-1".into(),
            raw_path: Some("/local/rollout.jsonl".into()),
            cwd: Some("/work/repo".into()),
            ..Default::default()
        },
    )
    .unwrap();
    let options = DiscoverOptions {
        scope: SessionScope::Remote,
        sources: vec!["codex".into()],
        ..Default::default()
    };
    crate::discover::discover_sessions_with_env(&env_at(&conn, home.path()), &options, |_| {})
        .unwrap();
    let presences: Vec<(String, String)> = conn.prepare(
        "SELECT location, raw_locator FROM session_presences WHERE source='codex' AND session_id='teleport-1' ORDER BY location"
    ).unwrap().query_map([], |r| Ok((r.get(0)?, r.get(1)?))).unwrap().collect::<rusqlite::Result<_>>().unwrap();
    assert_eq!(
        presences,
        vec![
            ("local".into(), "/local/rollout.jsonl".into()),
            ("remote".into(), "cloud://org-teammates/teleport-1".into())
        ]
    );
    let cwd: String = conn
        .query_row(
            "SELECT cwd FROM sessions WHERE source='codex' AND session_id='teleport-1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(cwd, "/work/repo");
    let raw_path: String = conn
        .query_row(
            "SELECT raw_path FROM sessions WHERE source='codex' AND session_id='teleport-1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(raw_path, "/local/rollout.jsonl");
    for scope in [SessionScope::Local, SessionScope::Remote, SessionScope::All] {
        let rows = list_session_catalog(
            &conn,
            &CatalogListOptions {
                scope,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].raw_path.as_deref(), Some("/local/rollout.jsonl"));
    }
    conn.execute(
        "DELETE FROM sessions WHERE source='codex' AND session_id='teleport-1'",
        [],
    )
    .unwrap();
    assert!(
        ai_hist_core::session_locations(&conn, "codex", "teleport-1")
            .unwrap()
            .is_empty()
    );
    server.join().unwrap();
    println!(
        "teleport session_presences: {presences:?}; canonical raw_path={raw_path}; canonical deletion cleaned both presences"
    );
}

#[test]
fn cloud_cleartext_guard_is_inherited() {
    let _isolated = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let evil = cloud_auth("http://evil.example");
    crate::cloud::save_auth(&evil).unwrap();
    let status = remote_connector_statuses_at(home.path())
        .into_iter()
        .find(|s| s.connector == CLOUD_CONNECTOR)
        .unwrap();
    assert!(!status.configured);
    assert!(
        status.detail.contains("refusing to send"),
        "{}",
        status.detail
    );
    let error =
        crate::cloud::recall_page(&evil, crate::cloud::RecallResource::Sessions, &[]).unwrap_err();
    assert!(error.to_string().contains("cleartext"));
    let loopback = cloud_auth("http://127.0.0.1:8787");
    crate::cloud::save_auth(&loopback).unwrap();
    std::env::set_var("RELAYHISTORY_BASE_URL", &loopback.base_url);
    assert!(crate::cloud::recall_auth().is_ok());
    println!("cleartext: http://evil.example refused: {error}; http://127.0.0.1:8787 accepted");
}

#[test]
fn cloud_status_rejects_bad_missing_and_expiring_tokens_without_hard_errors() {
    let _isolated = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    for (token, expiry) in [
        ("other-token", Some("2100-01-01T00:00:00Z")),
        ("rth_at_test", None),
        ("rth_at_test", Some("bad-date")),
        ("rth_at_test", Some("2000-01-01T00:00:00Z")),
    ] {
        let mut auth = cloud_auth("https://history.agentrelay.com");
        auth.access_token = token.into();
        auth.access_token_expires_at = expiry.map(String::from);
        crate::cloud::save_auth(&auth).unwrap();
        assert!(
            !remote_connector_statuses_at(home.path())
                .into_iter()
                .find(|s| s.connector == CLOUD_CONNECTOR)
                .unwrap()
                .configured
        );
    }
    std::fs::write(crate::cloud::config_dir().join("auth.json"), "invalid JSON").unwrap();
    assert!(
        !remote_connector_statuses_at(home.path())
            .into_iter()
            .find(|s| s.connector == CLOUD_CONNECTOR)
            .unwrap()
            .configured
    );
}

#[test]
fn cloud_pagination_and_source_filter_preserve_opaque_cursor() {
    let _isolated = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let mut first = cloud_listing("trajectory", "one");
    first["nextCursor"] = Value::String("opaque+/=&".into());
    let (base, server) = cloud_http(vec![
        (200, first),
        (200, cloud_listing("trajectory", "two")),
    ]);
    crate::cloud::save_auth(&cloud_auth(&base)).unwrap();
    let conn = catalog();
    let options = DiscoverOptions {
        scope: SessionScope::Remote,
        sources: vec!["trajectory".into()],
        ..Default::default()
    };
    let result =
        crate::discover::discover_sessions_with_env(&env_at(&conn, home.path()), &options, |_| {})
            .unwrap();
    assert_eq!(result.discovered, 2);
    for scope in [SessionScope::Remote, SessionScope::All] {
        let cached = list_session_catalog(
            &conn,
            &CatalogListOptions {
                scope,
                sources: vec!["trajectory".into()],
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(cached.len(), 2);
        assert!(cached
            .iter()
            .all(|row| row.source == "trajectory" && row.locations == ["remote"]));
    }
    assert!(list_session_catalog(
        &conn,
        &CatalogListOptions {
            scope: SessionScope::Local,
            sources: vec!["trajectory".into()],
            ..Default::default()
        }
    )
    .unwrap()
    .is_empty());
    let requests = server.join().unwrap();
    let url = url::Url::parse(&format!(
        "http://mock{}",
        requests[1].split_whitespace().nth(1).unwrap()
    ))
    .unwrap();
    assert!(url
        .query_pairs()
        .any(|(k, v)| k == "cursor" && v == "opaque+/=&"));
}

#[test]
fn cloud_recall_helpers_cover_events_and_encode_session_ids() {
    let _isolated = without_credentials_override();
    let (base, server) = cloud_http(vec![
        (200, serde_json::json!({"events": [], "nextCursor": null})),
        (200, serde_json::json!({"events": [], "nextCursor": null})),
    ]);
    let auth = cloud_auth(&base);
    crate::cloud::save_auth(&auth).unwrap();
    crate::cloud::recall_page(
        &auth,
        crate::cloud::RecallResource::Events,
        &[("q", "connector fix")],
    )
    .unwrap();
    crate::cloud::recall_page(
        &auth,
        crate::cloud::RecallResource::SessionEvents("session/a?b"),
        &[],
    )
    .unwrap();
    let requests = server.join().unwrap();
    assert!(requests[0].starts_with("GET /v1/events?"));
    assert!(
        requests[1].starts_with("GET /v1/sessions/session%2Fa%3Fb/events "),
        "{}",
        requests[1]
    );
}

#[test]
fn cloud_all_sources_fan_out_without_widening_source_choices() {
    let _isolated = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let pages = SOURCE_CHOICES
        .iter()
        .map(|source| (200, cloud_listing(source, &format!("{source}-team"))))
        .collect();
    let (base, server) = cloud_http(pages);
    crate::cloud::save_auth(&cloud_auth(&base)).unwrap();
    let conn = catalog();
    let options = DiscoverOptions {
        scope: SessionScope::Remote,
        ..Default::default()
    };
    let summary =
        crate::discover::discover_sessions_with_env(&env_at(&conn, home.path()), &options, |_| {})
            .unwrap();
    assert_eq!(summary.discovered, SOURCE_CHOICES.len());
    assert_eq!(
        SOURCE_CHOICES,
        [
            "claude",
            "codex",
            "cursor",
            "grok",
            "relay",
            "trajectory",
            "opencode"
        ]
    );
    assert!(ensure_remote_connectors_configured_for_at(
        "discovery",
        home.path(),
        &["cloud".into()]
    )
    .unwrap_err()
    .to_string()
    .contains("invalid source 'cloud'"));
    let requests = server.join().unwrap();
    assert_eq!(requests.len(), SOURCE_CHOICES.len());
}

#[test]
fn cloud_repeated_cursor_is_a_diagnostic_and_does_not_cache_partial_listing() {
    let _isolated = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let mut page = cloud_listing("cursor", "one");
    page["nextCursor"] = Value::String("repeated".into());
    let (base, server) = cloud_http(vec![(200, page.clone()), (200, page)]);
    crate::cloud::save_auth(&cloud_auth(&base)).unwrap();
    let conn = catalog();
    let options = DiscoverOptions {
        scope: SessionScope::Remote,
        sources: vec!["cursor".into()],
        ..Default::default()
    };
    let error =
        crate::discover::discover_sessions_with_env(&env_at(&conn, home.path()), &options, |_| {})
            .unwrap_err();
    let summary = &error
        .downcast_ref::<crate::discover::AllProvidersFailed>()
        .unwrap()
        .summary;
    assert!(summary.diagnostics[0].error.contains("repeated nextCursor"));
    assert!(ai_hist_core::session_locations(&conn, "cursor", "one")
        .unwrap()
        .is_empty());
    server.join().unwrap();
}

#[test]
fn cloud_recall_refresh_reuses_rotated_credentials_and_persists_expiry() {
    let _isolated = without_credentials_override();
    let (base, server) = cloud_http(vec![
        (401, serde_json::json!({"error": "expired"})),
        (
            200,
            serde_json::json!({"accessToken": "rth_at_rotated", "refreshToken": "rth_rt_rotated", "accessTokenExpiresAt": "2100-02-01T00:00:00Z"}),
        ),
        (200, cloud_listing("cursor", "one")),
    ]);
    let mut auth = cloud_auth(&base);
    auth.refresh_token = Some("rth_rt_synthetic".into());
    crate::cloud::save_auth(&auth).unwrap();
    crate::cloud::recall_page(&auth, crate::cloud::RecallResource::Sessions, &[]).unwrap();
    let stored = crate::cloud::load_auth(Some(&base)).unwrap().unwrap();
    assert_eq!(stored.access_token, "rth_at_rotated");
    assert_eq!(
        stored.access_token_expires_at.as_deref(),
        Some("2100-02-01T00:00:00Z")
    );
    let requests = server.join().unwrap();
    assert!(requests[1].starts_with("POST /v1/auth/token/refresh "));
    assert!(requests[2].contains("Authorization: Bearer rth_at_rotated"));
}

#[test]
fn cloud_status_requires_sixty_seconds_and_honors_stage_precedence() {
    let _isolated = without_credentials_override();
    let mut auth = cloud_auth("https://history.agentrelay.com");
    auth.access_token_expires_at =
        Some((chrono::Utc::now() + chrono::Duration::seconds(59)).to_rfc3339());
    crate::cloud::save_auth(&auth).unwrap();
    assert!(crate::cloud::recall_auth()
        .unwrap_err()
        .to_string()
        .contains("less than 60s"));
    auth.access_token_expires_at =
        Some((chrono::Utc::now() + chrono::Duration::seconds(120)).to_rfc3339());
    crate::cloud::save_auth(&auth).unwrap();
    assert!(crate::cloud::recall_auth().is_ok());
    let stage = cloud_auth("http://127.0.0.1:8787");
    crate::cloud::save_auth(&stage).unwrap();
    assert!(crate::cloud::recall_auth()
        .unwrap_err()
        .to_string()
        .contains("2 relayhistory stages"));
    std::env::set_var("AI_HIST_BASE_URL", &auth.base_url);
    assert_eq!(crate::cloud::recall_auth().unwrap().base_url, auth.base_url);
    std::env::set_var("RELAYHISTORY_BASE_URL", &stage.base_url);
    assert_eq!(
        crate::cloud::recall_auth().unwrap().base_url,
        stage.base_url
    );
}

#[test]
fn cloud_login_preserves_server_expiry_for_connector_availability() {
    let _isolated = without_credentials_override();
    let (base, server) = cloud_http(vec![(
        200,
        serde_json::json!({
            "accessToken": "rth_at_login", "refreshToken": "rth_rt_login",
            "accessTokenExpiresAt": "2100-01-01T00:00:00Z", "orgId": "org-teammates"
        }),
    )]);
    let auth = crate::cloud::login(&base, "synthetic-relayauth-token", "test", None).unwrap();
    crate::cloud::save_auth(&auth).unwrap();
    assert_eq!(
        crate::cloud::recall_auth()
            .unwrap()
            .access_token_expires_at
            .as_deref(),
        Some("2100-01-01T00:00:00Z")
    );
    assert!(server.join().unwrap()[0].starts_with("POST /v1/cli/login "));
}

#[test]
fn cloud_page_cap_retains_bounded_catalog_results() {
    let _isolated = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let pages = (0..MAX_LIST_PAGES)
        .map(|index| {
            let mut page = cloud_listing("cursor", &format!("bounded-{index}"));
            page["nextCursor"] = Value::String(format!("next-{}", index + 1));
            (200, page)
        })
        .collect();
    let (base, server) = cloud_http(pages);
    crate::cloud::save_auth(&cloud_auth(&base)).unwrap();
    let conn = catalog();
    let options = DiscoverOptions {
        scope: SessionScope::Remote,
        sources: vec!["cursor".into()],
        ..Default::default()
    };
    let summary =
        crate::discover::discover_sessions_with_env(&env_at(&conn, home.path()), &options, |_| {})
            .unwrap();
    assert_eq!(summary.discovered, MAX_LIST_PAGES);
    assert!(summary.diagnostics.is_empty());
    let rows = list_session_catalog(
        &conn,
        &CatalogListOptions {
            scope: SessionScope::Remote,
            limit: Some(200),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(rows.len(), MAX_LIST_PAGES);
    assert_eq!(server.join().unwrap().len(), MAX_LIST_PAGES);
    println!(
        "page cap: {} HTTP pages, {} retained remote catalog rows",
        MAX_LIST_PAGES,
        rows.len()
    );
}

#[test]
fn cloud_status_requires_cached_org_for_provenance() {
    let _isolated = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    for org_id in [None, Some(""), Some("   ")] {
        let mut auth = cloud_auth("https://history.agentrelay.com");
        auth.org_id = org_id.map(String::from);
        crate::cloud::save_auth(&auth).unwrap();
        let status = remote_connector_statuses_at(home.path())
            .into_iter()
            .find(|s| s.connector == CLOUD_CONNECTOR)
            .unwrap();
        assert!(!status.configured);
        assert!(status.detail.contains("no orgId for provenance"));
        assert!(configured_remote_providers(home.path(), None).is_empty());
    }
}

#[test]
fn cloud_recall_rejects_all_tenancy_query_selectors_before_network() {
    let _isolated = without_credentials_override();
    let auth = cloud_auth("http://127.0.0.1:1");
    for selector in ["org_id", "orgId", "workspace_id", "workspaceId"] {
        let error = crate::cloud::recall_page(
            &auth,
            crate::cloud::RecallResource::Sessions,
            &[(selector, "forged")],
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("tenancy comes from the token"),
            "{error}"
        );
    }
}

#[test]
fn cloud_preserves_presence_less_local_session_during_full_ingestion_gap() {
    let _isolated = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let (base, server) = cloud_http(vec![(200, cloud_listing("grok", "legacy-local"))]);
    crate::cloud::save_auth(&cloud_auth(&base)).unwrap();
    let conn = catalog();
    // State between the canonical full-session INSERT and the local presence
    // write. Existing cache-only reads classify it as local.
    conn.execute("INSERT INTO sessions(source, session_id, raw_path, discovery_state) VALUES ('grok', 'legacy-local', '/local/chat.json', 'full')", []).unwrap();
    assert_eq!(
        list_session_catalog(&conn, &CatalogListOptions::default())
            .unwrap()
            .len(),
        1
    );
    let options = DiscoverOptions {
        scope: SessionScope::Remote,
        sources: vec!["grok".into()],
        ..Default::default()
    };
    crate::discover::discover_sessions_with_env(&env_at(&conn, home.path()), &options, |_| {})
        .unwrap();
    let local = list_session_catalog(&conn, &CatalogListOptions::default()).unwrap();
    assert_eq!(local.len(), 1);
    assert_eq!(local[0].raw_path.as_deref(), Some("/local/chat.json"));
    assert_eq!(local[0].discovery_state, "full");
    assert_eq!(local[0].locations, ["local", "remote"]);
    let presences: Vec<(String,String)> = conn.prepare("SELECT location, raw_locator FROM session_presences WHERE source='grok' AND session_id='legacy-local' ORDER BY location").unwrap().query_map([], |r| Ok((r.get(0)?,r.get(1)?))).unwrap().collect::<rusqlite::Result<_>>().unwrap();
    assert_eq!(
        presences,
        vec![
            ("local".into(), "/local/chat.json".into()),
            ("remote".into(), "cloud://org-teammates/legacy-local".into())
        ]
    );
    server.join().unwrap();
    println!("legacy local gap: {presences:?}; canonical raw_path=/local/chat.json; state=full");
}
