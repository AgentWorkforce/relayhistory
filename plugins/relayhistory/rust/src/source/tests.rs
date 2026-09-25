use super::*;
use ai_hist::{init_db, SessionScope};
use ai_hist::discover::{
    discover_sessions_with_providers, list_session_catalog, CatalogListOptions, DiscoverOptions,
};
use std::io::Read;
use std::time::{Duration, Instant};
fn catalog() -> Connection {
    let conn = Connection::open_in_memory().expect("in-memory database");
    init_db(&conn).expect("schema");
    conn
}

fn env_at<'a>(conn: &'a Connection, home: &Path) -> DiscoveryEnv<'a> {
    DiscoveryEnv::with_roots(conn, home.to_path_buf(), home.join("opencode.db"))
}

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
    let status = selected_remote_connector_statuses_at(home.path(), &cloud_selection(), &[])
        .into_iter()
        .find(|s| s.connector == CLOUD_CONNECTOR)
        .unwrap();
    assert!(!status.configured);
    let error = ensure_cloud_configured_for_at("discovery", home.path(), &[])
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
    ensure_cloud_configured_for_at("discovery", home.path(), &["cursor".into()]).unwrap();
    let conn = catalog();
    let options = DiscoverOptions {
        scope: SessionScope::Remote,
        sources: vec!["cursor".into()],
        ..Default::default()
    };
    let first = discover_with_cloud(&env_at(&conn, home.path()), &options, |_| {}).unwrap();
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
    let second = discover_with_cloud(&env_at(&conn, home.path()), &options, |_| {}).unwrap();
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
    ai_hist::discover::upsert_shallow_session(
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
    discover_with_cloud(&env_at(&conn, home.path()), &options, |_| {}).unwrap();
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
        ai_hist::session_locations(&conn, "codex", "teleport-1")
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
    let status = selected_remote_connector_statuses_at(home.path(), &cloud_selection(), &[])
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
            !selected_remote_connector_statuses_at(home.path(), &cloud_selection(), &[])
                .into_iter()
                .find(|s| s.connector == CLOUD_CONNECTOR)
                .unwrap()
                .configured
        );
    }
    std::fs::write(crate::cloud::config_dir().join("auth.json"), "invalid JSON").unwrap();
    assert!(
        !selected_remote_connector_statuses_at(home.path(), &cloud_selection(), &[])
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
    let result = discover_with_cloud(&env_at(&conn, home.path()), &options, |_| {}).unwrap();
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
    let summary = discover_with_cloud(&env_at(&conn, home.path()), &options, |_| {}).unwrap();
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
            "opencode",
            "muse"
        ]
    );
    assert!(
        ensure_cloud_configured_for_at("discovery", home.path(), &["cloud".into()])
            .unwrap_err()
            .to_string()
            .contains("invalid source 'cloud'")
    );
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
    let error = discover_with_cloud(&env_at(&conn, home.path()), &options, |_| {}).unwrap_err();
    let summary = &error
        .downcast_ref::<ai_hist::discover::AllProvidersFailed>()
        .unwrap()
        .summary;
    assert!(summary.diagnostics[0].error.contains("repeated nextCursor"));
    assert!(ai_hist::session_locations(&conn, "cursor", "one")
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
            serde_json::json!({"accessToken": "rth_at_rotated", "refreshToken": "rth_rt_rotated", "accessTokenExpiresAt": "2100-02-01T00:00:00Z", "orgId": "org-rotated", "workspaceId": "ws-rotated"}),
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
    assert_eq!(stored.org_id.as_deref(), Some("org-rotated"));
    assert_eq!(stored.workspace_id.as_deref(), Some("ws-rotated"));
    let requests = server.join().unwrap();
    assert!(requests[1].starts_with("POST /v1/auth/token/refresh "));
    assert!(requests[2].contains("Authorization: Bearer rth_at_rotated"));
}

#[test]
fn cloud_recall_adopts_tenancy_for_a_legacy_session_before_provenance_check() {
    let _isolated = without_credentials_override();
    let (base, server) = cloud_http(vec![
        (
            200,
            serde_json::json!({"accessToken": "rth_at_adopted", "refreshToken": "rth_rt_adopted", "accessTokenExpiresAt": "2100-02-01T00:00:00Z", "orgId": "org-adopted", "workspaceId": "ws-adopted"}),
        ),
        (200, cloud_listing("cursor", "legacy-session")),
    ]);
    let mut auth = cloud_auth(&base);
    auth.org_id = None;
    auth.workspace_id = None;
    auth.refresh_token = Some("rth_rt_legacy".into());
    crate::cloud::save_auth(&auth).unwrap();

    let resolved = crate::cloud::resolve_recall_auth(
        Some(&base),
        chrono::Utc::now().timestamp_millis(),
        true,
    )
    .unwrap();
    assert_eq!(resolved.org_id.as_deref(), Some("org-adopted"));
    assert_eq!(resolved.workspace_id.as_deref(), Some("ws-adopted"));
    let page = crate::cloud::recall_page(&resolved, crate::cloud::RecallResource::Sessions, &[])
        .unwrap();
    assert_eq!(page["sessions"][0]["sessionId"], "legacy-session");
    let stored = crate::cloud::load_auth(Some(&base)).unwrap().unwrap();
    assert_eq!(stored.org_id.as_deref(), Some("org-adopted"));
    assert_eq!(stored.workspace_id.as_deref(), Some("ws-adopted"));
    let requests = server.join().unwrap();
    assert!(requests[0].starts_with("POST /v1/auth/token/refresh "));
    assert!(requests[1].contains("Authorization: Bearer rth_at_adopted"));
}

#[test]
fn cloud_recall_rechecks_replaced_session_after_refresh_lock() {
    let _isolated = without_credentials_override();
    let base = "https://history.agentrelay.com";
    let mut stale = cloud_auth(base);
    stale.org_id = None;
    stale.refresh_token = Some("rth_rt_stale".into());
    crate::cloud::save_auth(&stale).unwrap();

    // Hold the stage lock after the resolver's initial load, then replace the
    // session as a concurrent writer would. The resolver must use this current
    // session after lock acquisition and must not rotate it again.
    let lock = crate::cloud::acquire_refresh_lock(base).unwrap();
    let resolver = std::thread::spawn(move || {
        crate::cloud::resolve_recall_auth(
            Some(base),
            chrono::Utc::now().timestamp_millis(),
            true,
        )
    });
    std::thread::sleep(Duration::from_millis(20));
    let replacement = crate::cloud::StoredAuth {
        base_url: base.into(),
        access_token: "rth_at_replacement".into(),
        access_token_expires_at: Some("2100-01-01T00:00:00Z".into()),
        org_id: Some("org-replacement".into()),
        refresh_token: None,
        ..Default::default()
    };
    crate::cloud::save_auth(&replacement).unwrap();
    drop(lock);
    let resolved = resolver.join().unwrap().unwrap();
    assert_eq!(resolved.access_token, "rth_at_replacement");
    assert_eq!(resolved.org_id.as_deref(), Some("org-replacement"));
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
    let summary = discover_with_cloud(&env_at(&conn, home.path()), &options, |_| {}).unwrap();
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
        let status = selected_remote_connector_statuses_at(home.path(), &cloud_selection(), &[])
            .into_iter()
            .find(|s| s.connector == CLOUD_CONNECTOR)
            .unwrap();
        assert!(!status.configured);
        assert!(status.detail.contains("no orgId for provenance"));
        assert!(selected_remote_providers(home.path(), None, &cloud_selection(), &[]).is_empty());
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
    discover_with_cloud(&env_at(&conn, home.path()), &options, |_| {}).unwrap();
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

fn cloud_selection() -> SourceConnectorSelection {
    SourceConnectorSelection::new(vec![CLOUD_CONNECTOR.into()]).unwrap()
}

fn ensure_cloud_configured_for_at(operation: &str, home: &Path, sources: &[String]) -> Result<()> {
    ensure_selected_remote_connectors_configured_for_at(
        operation,
        home,
        sources,
        &cloud_selection(),
    )
}

fn discover_with_cloud(
    env: &DiscoveryEnv<'_>,
    options: &DiscoverOptions,
    on_row: impl FnMut(&ShallowSession),
) -> Result<ai_hist::DiscoverySummary> {
    ensure_cloud_configured_for_at("discovery", &env.home, &options.sources)?;
    let providers = selected_remote_providers(
        &env.home,
        options.limit,
        &cloud_selection(),
        &options.sources,
    );
    discover_sessions_with_providers(env, options, &providers, on_row)
}
