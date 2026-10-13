//! Tests for the remote session connectors.
//!
//! The network and process boundaries are faked ([`ClaudeSessionsTransport`],
//! [`CodexCloudLister`]), so mapping, pagination, and the engine integration
//! are asserted without a claude.ai account or the Codex CLI installed. The
//! real transports are exercised end-to-end by `tests/session_discovery.rs`
//! (a scripted `codex` binary and a loopback HTTP server).

use super::devin_auth::{devin_org_id, toml_value};
use super::devin_cloud::{
    acquire_devin_remote_session_at, devin_session_hex, map_devin_session,
    send_devin_session_message_at, DevinHttpResponse, DevinTransport, UreqDevinTransport,
};
use super::*;
static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
use crate::discover::{
    discover_sessions_with_providers, list_session_catalog, CatalogListOptions, DiscoverOptions,
};
use ai_hist::{init_db, EvidenceKind, SessionScope};
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
    let guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let home = tempfile::tempdir().unwrap();
    let keys = [
        "RELAYHISTORY_CLAUDE_CREDENTIALS",
        "RELAYHISTORY_HOME",
        "RELAYHISTORY_BASE_URL",
        "AI_HIST_BASE_URL",
        "RELAYHISTORY_DEVIN_API_KEY",
        "RELAYHISTORY_DEVIN_ORG_ID",
        "RELAYHISTORY_DEVIN_API_BASE_URL",
        "RELAYHISTORY_DEVIN_CREDENTIALS",
        "RELAYHISTORY_DEVIN_CONFIG",
        "XDG_DATA_HOME",
        "XDG_CONFIG_HOME",
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
    assert!(error.contains("devin-cloud"), "{error}");

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
    // Cursor has no selected provider connector; commercial login cannot widen selection.
    let error = ok(&["cursor"]).unwrap_err().to_string();
    assert!(
        error.contains("no matching remote provider connectors exist"),
        "{error}"
    );
    // A misspelled source is an invalid argument, not an unsupported request.
    let error = ok(&["bogus"]).unwrap_err().to_string();
    assert!(error.contains("invalid source 'bogus'"), "{error}");
}

// ---------------------------------------------------------------------------
// devin-cloud
// ---------------------------------------------------------------------------

/// Serves a fixed sequence of api.devin.ai responses and records each request.
struct ScriptedDevinTransport {
    responses: Vec<(u16, String)>,
    post_responses: Vec<(u16, String)>,
    calls: Mutex<Vec<(String, String)>>,
    posts: Mutex<Vec<(String, String, Value)>>,
    next: AtomicUsize,
    next_post: AtomicUsize,
}

impl ScriptedDevinTransport {
    fn new(responses: Vec<(u16, String)>) -> Self {
        Self {
            responses,
            post_responses: Vec::new(),
            calls: Mutex::new(Vec::new()),
            posts: Mutex::new(Vec::new()),
            next: AtomicUsize::new(0),
            next_post: AtomicUsize::new(0),
        }
    }

    fn with_posts(mut self, post_responses: Vec<(u16, String)>) -> Self {
        self.post_responses = post_responses;
        self
    }
}

impl DevinTransport for Arc<ScriptedDevinTransport> {
    fn get(&self, url: &str, bearer_token: &str) -> Result<DevinHttpResponse> {
        self.calls
            .lock()
            .unwrap()
            .push((url.to_string(), bearer_token.to_string()));
        let index = self.next.fetch_add(1, Ordering::Relaxed);
        let (status, body) = self
            .responses
            .get(index)
            .cloned()
            .unwrap_or((500, "script exhausted".to_string()));
        Ok(DevinHttpResponse { status, body })
    }

    fn post(&self, url: &str, bearer_token: &str, body: &Value) -> Result<DevinHttpResponse> {
        self.posts
            .lock()
            .unwrap()
            .push((url.to_string(), bearer_token.to_string(), body.clone()));
        let index = self.next_post.fetch_add(1, Ordering::Relaxed);
        let (status, body) = self
            .post_responses
            .get(index)
            .cloned()
            .unwrap_or((500, "script exhausted".to_string()));
        Ok(DevinHttpResponse { status, body })
    }
}

const DEVIN_SESSION_ID: &str = "4b50574f40324da6bd88e9cbd30501cf";

/// The Devin CLI's two config files under a fixture home: a bearer and an
/// org id, as `credentials.toml`/`config.json` actually store them.
fn write_devin_cli_files(home: &Path) -> (PathBuf, PathBuf) {
    let credentials = home.join(".local/share/devin/credentials.toml");
    std::fs::create_dir_all(credentials.parent().unwrap()).unwrap();
    std::fs::write(
        &credentials,
        "windsurf_api_key = \"cog_test_key\"\n\
         api_server_url = \"https://api.devin.ai\"\n\
         devin_api_url = \"https://api.devin.ai\"\n",
    )
    .unwrap();
    let config = home.join(".config/devin/config.json");
    std::fs::create_dir_all(config.parent().unwrap()).unwrap();
    std::fs::write(&config, r#"{"devin": {"org_id": "org-test"}}"#).unwrap();
    (credentials, config)
}

fn devin_provider(
    home: &Path,
    transport: &Arc<ScriptedDevinTransport>,
    limit: Option<usize>,
) -> DevinCloudProvider {
    let (credentials, config) = write_devin_cli_files(home);
    DevinCloudProvider::new(credentials, config, Box::new(Arc::clone(transport)), limit)
}

fn devin_session(id: &str, title: &str) -> Value {
    serde_json::json!({
        "session_id": id,
        "url": format!("https://app.devin.ai/sessions/{id}"),
        "status": "suspended",
        "title": title,
        "org_id": "org-test",
        "created_at": 1790900683,
        "updated_at": 1790902549,
        "is_archived": false,
        "origin": "automation",
        "devin_mode": "fusion"
    })
}

fn devin_page(items: &[Value], end_cursor: Option<&str>, has_next_page: bool) -> String {
    serde_json::json!({
        "items": items,
        "end_cursor": end_cursor,
        "has_next_page": has_next_page,
        "total": null
    })
    .to_string()
}

#[test]
fn devin_mapping_carries_observed_fields_and_nothing_invented() {
    let (candidate, session) =
        map_devin_session(&devin_session(DEVIN_SESSION_ID, "Catch bugs in merged PRs"))
            .expect("a mappable session");
    assert_eq!(session.source, "devin");
    assert_eq!(session.session_id, DEVIN_SESSION_ID);
    assert_eq!(
        session.first_prompt.as_deref(),
        Some("Catch bugs in merged PRs")
    );
    assert_eq!(session.first_activity_ms, Some(1790900683000));
    assert_eq!(session.last_activity_ms, Some(1790902549000));
    assert_eq!(session.models, vec!["fusion".to_string()]);
    assert_eq!(session.originator.as_deref(), Some("automation"));
    assert_eq!(
        session.raw_path.as_deref(),
        Some("https://app.devin.ai/sessions/4b50574f40324da6bd88e9cbd30501cf")
    );
    assert_eq!(session.discovery_state, "shallow");
    assert_eq!(session.cwd, None);
    assert_eq!(session.git_branch, None);
    assert_eq!(candidate.locator, format!("devin-{DEVIN_SESSION_ID}"));
    assert_eq!(candidate.session_id.as_deref(), Some(DEVIN_SESSION_ID));
    assert_eq!(candidate.recency_hint_ms, session.last_activity_ms);
    assert!(
        candidate.stamp.starts_with("cloud:1790902549:suspended:"),
        "{}",
        candidate.stamp
    );
}

#[test]
fn devin_mapping_stamp_tracks_the_mapped_listing_fields() {
    // A title (or other mapped field) edit that Devin reports without
    // advancing `updated_at` must still change the stamp, else discovery
    // serves the stale catalog row.
    let base = map_devin_session(&devin_session(DEVIN_SESSION_ID, "one")).unwrap();
    let mut renamed = devin_session(DEVIN_SESSION_ID, "one");
    renamed["title"] = serde_json::json!("two");
    let changed = map_devin_session(&renamed).unwrap();
    assert_ne!(base.0.stamp, changed.0.stamp);
    let identical = map_devin_session(&devin_session(DEVIN_SESSION_ID, "one")).unwrap();
    assert_eq!(base.0.stamp, identical.0.stamp);
}

#[test]
fn devin_mapping_skips_malformed_ids_and_normalizes_the_prefixed_form() {
    for bad_id in [
        "",
        "merciful-slip",
        "devin-",
        "4b50",
        "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
    ] {
        let entry = devin_session(bad_id, "t");
        assert!(
            map_devin_session(&entry).is_none(),
            "id {bad_id:?} must not map"
        );
    }
    // A `devin-`-prefixed session_id catalogs under the same bare id the
    // unprefixed form produces.
    let prefixed = devin_session(&format!("devin-{DEVIN_SESSION_ID}"), "t");
    let (candidate, session) = map_devin_session(&prefixed).expect("prefixed id maps");
    assert_eq!(session.session_id, DEVIN_SESSION_ID);
    assert_eq!(candidate.locator, format!("devin-{DEVIN_SESSION_ID}"));
    assert_eq!(
        devin_session_hex(&format!("devin-{DEVIN_SESSION_ID}")),
        Some(DEVIN_SESSION_ID)
    );
}

#[test]
fn devin_credentials_parse_the_flat_toml() {
    let raw = "# comment\n\
               windsurf_api_key = \"cog_abc\"\n\
               devin_api_url = \"https://api.devin.ai\"\n\
               broken = true\n\
               other = \"x\" # inline\n";
    assert_eq!(
        toml_value(raw, "windsurf_api_key").as_deref(),
        Some("cog_abc")
    );
    assert_eq!(
        toml_value(raw, "devin_api_url").as_deref(),
        Some("https://api.devin.ai")
    );
    assert_eq!(toml_value(raw, "missing"), None);
    assert_eq!(toml_value(raw, "broken"), None);
    assert_eq!(toml_value(raw, "windsurf_api_key_extra"), None);

    let home = tempfile::tempdir().unwrap();
    assert!(devin_org_id(&home.path().join("missing.json")).is_none());
    let config = home.path().join("config.json");
    std::fs::write(&config, r#"{"devin":{"org_id":"org-x"},"hooks":{}}"#).unwrap();
    assert_eq!(devin_org_id(&config).as_deref(), Some("org-x"));
    std::fs::write(&config, "not json").unwrap();
    assert!(devin_org_id(&config).is_none());
}

#[test]
fn devin_enumeration_pages_with_first_and_after() {
    let _cleared = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let transport = Arc::new(ScriptedDevinTransport::new(vec![
        (
            200,
            devin_page(
                &[devin_session(DEVIN_SESSION_ID, "one")],
                Some("cursor+1/2"),
                true,
            ),
        ),
        (
            200,
            devin_page(
                &[devin_session("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "two")],
                None,
                false,
            ),
        ),
    ]));
    let provider = devin_provider(home.path(), &transport, None);
    let conn = catalog();
    let env = env_at(&conn, home.path());
    let candidates = provider.enumerate(&env, None).unwrap();
    assert_eq!(candidates.len(), 2);
    let calls = transport.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(
        calls[0].0,
        "https://api.devin.ai/v3/organizations/org-test/sessions?first=200"
    );
    assert_eq!(
        calls[1].0,
        "https://api.devin.ai/v3/organizations/org-test/sessions?first=200&after=cursor%2B1%2F2"
    );
    assert!(calls.iter().all(|(_, bearer)| bearer == "cog_test_key"));
}

#[test]
fn devin_enumeration_respects_the_row_limit() {
    let _cleared = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let transport = Arc::new(ScriptedDevinTransport::new(vec![(
        200,
        devin_page(
            &[
                devin_session(DEVIN_SESSION_ID, "one"),
                devin_session("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "two"),
            ],
            Some("more"),
            true,
        ),
    )]));
    let provider = devin_provider(home.path(), &transport, Some(2));
    let conn = catalog();
    let env = env_at(&conn, home.path());
    let candidates = provider.enumerate(&env, None).unwrap();
    assert_eq!(candidates.len(), 2);
    let calls = transport.calls.lock().unwrap();
    assert_eq!(calls.len(), 1, "the satisfied limit ends the walk");
    assert!(calls[0].0.ends_with("?first=2"));
}

#[test]
fn devin_enumeration_requires_configuration_and_reports_auth() {
    let _cleared = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let (credentials, config) = (
        home.path().join("credentials.toml"),
        home.path().join("config.json"),
    );
    let transport = Arc::new(ScriptedDevinTransport::new(vec![]));
    let provider = DevinCloudProvider::new(
        credentials.clone(),
        config.clone(),
        Box::new(Arc::clone(&transport)),
        None,
    );
    let conn = catalog();
    let env = env_at(&conn, home.path());
    let error = provider.enumerate(&env, None).unwrap_err().to_string();
    assert!(error.starts_with("CONNECTOR_NOT_CONFIGURED:"), "{error}");

    // A key without an organization id is still unconfigured.
    std::fs::write(&credentials, "windsurf_api_key = \"cog_test_key\"\n").unwrap();
    let error = provider.enumerate(&env, None).unwrap_err().to_string();
    assert!(error.contains("organization id"), "{error}");
    assert!(transport.calls.lock().unwrap().is_empty());

    // Configured, then rejected upstream.
    std::fs::write(&config, r#"{"devin":{"org_id":"org-test"}}"#).unwrap();
    let transport = Arc::new(ScriptedDevinTransport::new(vec![(
        401,
        "key-secret-must-not-escape".into(),
    )]));
    let provider =
        DevinCloudProvider::new(credentials, config, Box::new(Arc::clone(&transport)), None);
    let error = provider.enumerate(&env, None).unwrap_err().to_string();
    assert!(error.starts_with("AUTHENTICATION_EXPIRED:"), "{error}");
    assert!(!error.contains("key-secret"), "{error}");
}

#[test]
fn devin_acquire_hydrates_messages_into_history_and_events() {
    let _cleared = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let (credentials, config) = write_devin_cli_files(home.path());
    let transport = Arc::new(ScriptedDevinTransport::new(vec![
        (
            200,
            devin_page(
                &[serde_json::json!({
                    "event_id": "event-1",
                    "source": "user",
                    "message": "triage the flaky suite",
                    "created_at": 1790900683,
                    "username": "Devin Bot",
                    "origin": "automation"
                })],
                Some("page-2"),
                true,
            ),
        ),
        (
            200,
            devin_page(
                &[serde_json::json!({
                    "event_id": "event-2",
                    "source": "devin",
                    "message": "found the retry bug",
                    "created_at": 1790900692
                })],
                None,
                false,
            ),
        ),
    ]));
    let acquired =
        acquire_devin_remote_session_at(&credentials, &config, DEVIN_SESSION_ID, &transport)
            .unwrap();
    let RemoteSessionEvidence::Normalized(snapshot) = acquired else {
        panic!("expected normalized evidence")
    };
    assert!(snapshot.source_stamp.starts_with("messages:"));
    assert!(snapshot.source_bytes > 0);
    assert_eq!(
        snapshot.covered_kinds,
        vec![EvidenceKind::History, EvidenceKind::SessionEvent]
    );
    assert_eq!(snapshot.records.len(), 3);
    let (history, events): (Vec<_>, Vec<_>) = snapshot
        .records
        .iter()
        .partition(|record| record.kind == EvidenceKind::History);
    assert_eq!(history.len(), 1);
    assert_eq!(
        history[0].payload.get("prompt").and_then(Value::as_str),
        Some("triage the flaky suite")
    );
    assert_eq!(
        history[0]
            .payload
            .get("timestamp_ms")
            .and_then(Value::as_i64),
        Some(1790900683000)
    );
    assert_eq!(events.len(), 2);
    assert_eq!(
        events[0].payload.get("role").and_then(Value::as_str),
        Some("user")
    );
    assert_eq!(
        events[1].payload.get("role").and_then(Value::as_str),
        Some("assistant")
    );
    assert_eq!(
        events[1].payload.get("kind").and_then(Value::as_str),
        Some("text")
    );
    assert_eq!(
        events[1].payload.get("event_uid").and_then(Value::as_str),
        Some("event-2")
    );
    let calls = transport.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert!(calls[0].0.contains(&format!(
        "sessions/devin-{DEVIN_SESSION_ID}/messages?first=200"
    )));
    assert!(calls[1].0.contains("after=page-2"));
}

#[test]
fn devin_acquire_reports_configuration_and_bad_ids_without_a_request() {
    let _cleared = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let transport = Arc::new(ScriptedDevinTransport::new(vec![]));
    let acquired = acquire_devin_remote_session_at(
        &home.path().join("credentials.toml"),
        &home.path().join("config.json"),
        DEVIN_SESSION_ID,
        &transport,
    )
    .unwrap();
    assert!(matches!(
        acquired,
        RemoteSessionEvidence::CapabilityLimited {
            code: "CONNECTOR_NOT_CONFIGURED",
            ..
        }
    ));
    assert!(transport.calls.lock().unwrap().is_empty());

    let (credentials, config) = write_devin_cli_files(home.path());
    let error = acquire_devin_remote_session_at(&credentials, &config, "merciful-slip", &transport)
        .unwrap_err()
        .to_string();
    assert!(error.starts_with("INVALID_ARGUMENT:"), "{error}");
}

#[test]
fn devin_acquire_is_page_bounded_and_surfaces_a_missing_session() {
    let _cleared = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let (credentials, config) = write_devin_cli_files(home.path());
    let missing = Arc::new(ScriptedDevinTransport::new(vec![(404, "{}".into())]));
    let error = acquire_devin_remote_session_at(&credentials, &config, DEVIN_SESSION_ID, &missing)
        .unwrap_err()
        .to_string();
    assert!(error.starts_with("SESSION_NOT_FOUND:"), "{error}");

    // Every page claims another follows; the walk stops at the bound instead
    // of trusting the cursor forever.
    let pages = (0..MAX_LIST_PAGES)
        .map(|_| {
            (
                200,
                devin_page(
                    &[serde_json::json!({
                        "event_id": "event-x",
                        "source": "devin",
                        "message": "still going",
                        "created_at": 1790900692
                    })],
                    Some("again"),
                    true,
                ),
            )
        })
        .collect();
    let bounded = Arc::new(ScriptedDevinTransport::new(pages));
    let error = acquire_devin_remote_session_at(&credentials, &config, DEVIN_SESSION_ID, &bounded)
        .unwrap_err()
        .to_string();
    assert!(error.starts_with("EVIDENCE_PARTIAL:"), "{error}");
}

#[test]
fn devin_enumeration_fails_closed_at_the_page_bound() {
    let _cleared = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    // Every page claims another follows; an unbounded org list must surface
    // as a partial-enumeration error rather than a silently truncated result.
    let pages = (0..MAX_LIST_PAGES)
        .map(|_| {
            (
                200,
                devin_page(
                    &[devin_session(DEVIN_SESSION_ID, "still going")],
                    Some("again"),
                    true,
                ),
            )
        })
        .collect();
    let transport = Arc::new(ScriptedDevinTransport::new(pages));
    let provider = devin_provider(home.path(), &transport, None);
    let conn = catalog();
    let env = env_at(&conn, home.path());
    let error = provider.enumerate(&env, None).unwrap_err().to_string();
    assert!(error.starts_with("EVIDENCE_PARTIAL:"), "{error}");
    assert_eq!(transport.calls.lock().unwrap().len(), MAX_LIST_PAGES);
}

#[test]
fn devin_organization_id_cannot_alter_the_request_route() {
    let _cleared = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let credentials = home.path().join("credentials.toml");
    std::fs::write(&credentials, "windsurf_api_key = \"cog_test_key\"\n").unwrap();
    let config = home.path().join("config.json");
    std::fs::write(&config, r#"{"devin":{"org_id":"org/x/../../admin"}}"#).unwrap();
    let transport = Arc::new(ScriptedDevinTransport::new(vec![]));
    let provider =
        DevinCloudProvider::new(credentials, config, Box::new(Arc::clone(&transport)), None);
    let conn = catalog();
    let env = env_at(&conn, home.path());
    let error = provider.enumerate(&env, None).unwrap_err().to_string();
    assert!(error.starts_with("CONNECTOR_NOT_CONFIGURED:"), "{error}");
    assert!(
        transport.calls.lock().unwrap().is_empty(),
        "no authenticated request may run on an injected org id"
    );
}

#[test]
fn devin_acquire_skips_messages_the_intake_contract_would_reject() {
    let _cleared = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let (credentials, config) = write_devin_cli_files(home.path());
    let transport = Arc::new(ScriptedDevinTransport::new(vec![(
        200,
        devin_page(
            &[
                // A non-conversational source has no role the intake accepts.
                serde_json::json!({
                    "event_id": "event-sys",
                    "source": "system",
                    "message": "context injected",
                    "created_at": 1790900683
                }),
                // No timestamp: neither an event nor a history row can key it.
                serde_json::json!({
                    "event_id": "event-undated",
                    "source": "user",
                    "message": "when?"
                }),
                // One well-formed pair.
                serde_json::json!({
                    "event_id": "event-user",
                    "source": "user",
                    "message": "real prompt",
                    "created_at": 1790900684
                }),
                serde_json::json!({
                    "event_id": "event-devin",
                    "source": "devin",
                    "message": "on it",
                    "created_at": 1790900685
                }),
                // A repeated prompt inside the same second shares a history
                // identity; the event still records the second occurrence.
                serde_json::json!({
                    "event_id": "event-again",
                    "source": "user",
                    "message": "real prompt",
                    "created_at": 1790900684
                }),
            ],
            None,
            false,
        ),
    )]));
    let acquired =
        acquire_devin_remote_session_at(&credentials, &config, DEVIN_SESSION_ID, &transport)
            .unwrap();
    let RemoteSessionEvidence::Normalized(snapshot) = acquired else {
        panic!("expected normalized evidence")
    };
    let (history, events): (Vec<_>, Vec<_>) = snapshot
        .records
        .iter()
        .partition(|record| record.kind == EvidenceKind::History);
    assert_eq!(history.len(), 1, "{:#?}", snapshot.records);
    assert_eq!(events.len(), 3, "{:#?}", snapshot.records);
    assert!(events.iter().all(|event| {
        matches!(
            event.payload.get("role").and_then(Value::as_str),
            Some("user" | "assistant")
        ) && event.payload.get("ts_ms").and_then(Value::as_i64).is_some()
    }));
    assert!(events.iter().any(
        |event| event.payload.get("event_uid").and_then(Value::as_str) == Some("event-again")
    ));
    // And the intake itself accepts the snapshot the connector produced.
    ai_hist::sources::normalize_source_evidence(
        "devin",
        DEVIN_SESSION_ID,
        RemoteSessionEvidence::Normalized(snapshot),
    )
    .unwrap();
}

#[test]
fn real_devin_transport_never_follows_a_redirect_with_credentials() {
    use std::io::{Read, Write};
    use std::net::TcpListener;

    let _cleared = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let (credentials, config) = write_devin_cli_files(home.path());
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
    std::env::set_var("RELAYHISTORY_DEVIN_API_BASE_URL", &origin_url);
    let provider = DevinCloudProvider::new(credentials, config, Box::new(UreqDevinTransport), None);
    let conn = catalog();
    let env = env_at(&conn, home.path());
    let error = provider.enumerate(&env, None).unwrap_err().to_string();
    server.join().unwrap();
    assert!(error.contains("HTTP 302"), "{error}");
    std::thread::sleep(Duration::from_millis(50));
    assert!(
        matches!(target.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
    );
    std::env::remove_var("RELAYHISTORY_DEVIN_API_BASE_URL");
}

// ---------------------------------------------------------------------------
// devin-cloud send
// ---------------------------------------------------------------------------

/// The send op posts the documented v3 route — the session id carries the
/// `devin-` prefix the API requires — with `{"message": …}` as the whole body
/// and the stored credential as the bearer.
#[test]
fn devin_send_posts_the_documented_route_and_body() {
    let _cleared = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let (credentials, config) = write_devin_cli_files(home.path());
    let transport = Arc::new(ScriptedDevinTransport::new(vec![]).with_posts(vec![(
        200,
        r#"{"session_id":"4b50574f40324da6bd88e9cbd30501cf"}"#.into(),
    )]));
    send_devin_session_message_at(
        &credentials,
        &config,
        DEVIN_SESSION_ID,
        "hello from the relay",
        &Arc::clone(&transport) as &dyn DevinTransport,
    )
    .expect("send");
    let posts = transport.posts.lock().unwrap();
    assert_eq!(posts.len(), 1);
    let (url, bearer, body) = &posts[0];
    assert!(
        url.ends_with(&format!(
            "/v3/organizations/org-test/sessions/devin-{DEVIN_SESSION_ID}/messages"
        )),
        "{url}"
    );
    assert_eq!(bearer, "cog_test_key");
    assert_eq!(
        body,
        &serde_json::json!({"message": "hello from the relay"})
    );
}

/// Each response status maps to the operation's error vocabulary: 401 is an
/// expired credential, 403 names the *write* scope (not the read one), 404 is
/// a missing session, 422 is the API rejecting the request itself.
#[test]
fn devin_send_maps_response_statuses_to_error_codes() {
    let _cleared = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let (credentials, config) = write_devin_cli_files(home.path());
    let cases = [
        (401u16, "AUTHENTICATION_EXPIRED:"),
        (403, "PERMISSION_DENIED:"),
        (404, "SESSION_NOT_FOUND:"),
        (422, "INVALID_ARGUMENT:"),
        (500, "CONNECTOR_FAILURE:"),
    ];
    for (status, prefix) in cases {
        let transport =
            Arc::new(ScriptedDevinTransport::new(vec![]).with_posts(vec![(status, "{}".into())]));
        let error = send_devin_session_message_at(
            &credentials,
            &config,
            DEVIN_SESSION_ID,
            "hello",
            &Arc::clone(&transport) as &dyn DevinTransport,
        )
        .unwrap_err()
        .to_string();
        assert!(error.starts_with(prefix), "HTTP {status}: {error}");
        if status == 403 {
            assert!(error.contains("ManageOrgSessions"), "{error}");
            assert!(!error.contains("ViewOrgSessions"), "{error}");
        }
    }
}

/// A local-style Devin session name cannot be a remote id — the request never
/// leaves the machine. Same for an empty or oversized message.
#[test]
fn devin_send_rejects_malformed_targets_and_messages() {
    let _cleared = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let (credentials, config) = write_devin_cli_files(home.path());
    let transport =
        Arc::new(ScriptedDevinTransport::new(vec![]).with_posts(vec![(200, "{}".into())]));
    for (session, message) in [
        ("real-gallium", "hello"),
        (DEVIN_SESSION_ID, ""),
        (DEVIN_SESSION_ID, "   "),
    ] {
        let error = send_devin_session_message_at(
            &credentials,
            &config,
            session,
            message,
            &Arc::clone(&transport) as &dyn DevinTransport,
        )
        .unwrap_err()
        .to_string();
        assert!(error.starts_with("INVALID_ARGUMENT:"), "{error}");
    }
    let oversized = "x".repeat(MAX_REMOTE_MESSAGE_BYTES + 1);
    let error = send_devin_session_message_at(
        &credentials,
        &config,
        DEVIN_SESSION_ID,
        &oversized,
        &Arc::clone(&transport) as &dyn DevinTransport,
    )
    .unwrap_err()
    .to_string();
    assert!(error.starts_with("INVALID_ARGUMENT:"), "{error}");
    assert!(transport.posts.lock().unwrap().is_empty());
}

/// Without a stored sign-in, the send answers CONNECTOR_NOT_CONFIGURED before
/// any request; connectors without a write path refuse outright.
#[test]
fn send_refuses_unconfigured_or_unsupported_connectors() {
    let _cleared = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let (credentials, _config) = write_devin_cli_files(home.path());
    let transport =
        Arc::new(ScriptedDevinTransport::new(vec![]).with_posts(vec![(200, "{}".into())]));
    let error = send_devin_session_message_at(
        &credentials,
        &home.path().join("missing-config.json"),
        DEVIN_SESSION_ID,
        "hello",
        &Arc::clone(&transport) as &dyn DevinTransport,
    )
    .unwrap_err()
    .to_string();
    assert!(error.starts_with("CONNECTOR_NOT_CONFIGURED:"), "{error}");
    let error = send_remote_message_at(home.path(), CLAUDE_WEB_CONNECTOR, "s1", "hello")
        .unwrap_err()
        .to_string();
    assert!(error.starts_with("INVALID_ARGUMENT:"), "{error}");
    assert!(error.contains("cannot deliver messages"), "{error}");
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
