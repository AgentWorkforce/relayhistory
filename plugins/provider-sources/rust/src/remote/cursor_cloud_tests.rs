//! `cursor-cloud` against recorded shapes of the documented Cloud Agents API
//! (`tests/fixtures/cursor-cloud`), behind a scripted transport.

use super::super::cursor_cloud::{
    cursor_agent_id, cursor_status, map_cursor_agent, CursorCloudProvider, CursorHttpResponse,
    CursorTransport,
};
use super::super::cursor_hydrate::acquire_cursor_remote_session_at;
use super::*;

const AGENTS_PAGE_1: &str = include_str!("../../tests/fixtures/cursor-cloud/agents-page-1.json");
const AGENTS_PAGE_2: &str = include_str!("../../tests/fixtures/cursor-cloud/agents-page-2.json");
const AGENT: &str = include_str!("../../tests/fixtures/cursor-cloud/agent.json");
const RUNS: &str = include_str!("../../tests/fixtures/cursor-cloud/runs.json");
const CONVERSATION: &str = include_str!("../../tests/fixtures/cursor-cloud/conversation.json");

const AGENT_ID: &str = "bc-00000000-0000-0000-0000-000000000002";
const API_KEY: &str = "key_fixture_cursor";

/// Serves a fixed sequence of api.cursor.com responses and records each
/// requested URL with the key it carried.
struct ScriptedCursorTransport {
    responses: Vec<(u16, String)>,
    calls: Mutex<Vec<(String, String)>>,
    next: AtomicUsize,
}

impl ScriptedCursorTransport {
    fn new(responses: Vec<(u16, &str)>) -> Arc<Self> {
        Arc::new(Self {
            responses: responses
                .into_iter()
                .map(|(status, body)| (status, body.to_string()))
                .collect(),
            calls: Mutex::new(Vec::new()),
            next: AtomicUsize::new(0),
        })
    }

    fn urls(&self) -> Vec<String> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .map(|(url, _)| url.clone())
            .collect()
    }
}

impl CursorTransport for Arc<ScriptedCursorTransport> {
    fn get(&self, url: &str, api_key: &str) -> Result<CursorHttpResponse> {
        self.calls
            .lock()
            .unwrap()
            .push((url.to_string(), api_key.to_string()));
        let index = self.next.fetch_add(1, Ordering::Relaxed);
        let (status, body) = self
            .responses
            .get(index)
            .cloned()
            .unwrap_or((500, "script exhausted".to_string()));
        Ok(CursorHttpResponse { status, body })
    }
}

fn write_key(home: &Path) -> PathBuf {
    let path = home.join(".config/relayhistory/cursor-api-key");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, format!("# Cursor Dashboard → API Keys\n{API_KEY}\n")).unwrap();
    path
}

fn ms(iso: &str) -> i64 {
    crate::parse_iso_ms(iso).unwrap()
}

fn fixture(raw: &str) -> Value {
    serde_json::from_str(raw).unwrap()
}

#[test]
fn cursor_mapping_carries_observed_fields_and_nothing_invented() {
    let listing = fixture(AGENTS_PAGE_1);
    let (candidate, session) = map_cursor_agent(&listing["items"][0]).unwrap();
    assert_eq!(session.source, "cursor");
    assert_eq!(session.session_id, AGENT_ID);
    assert_eq!(
        session.title.as_deref(),
        Some("Prepare iOS build 37 for TestFlight")
    );
    assert_eq!(session.first_prompt, None, "the listing carries no prompt");
    assert_eq!(
        session.first_activity_ms,
        Some(ms("2026-10-20T18:30:00.000Z"))
    );
    assert_eq!(
        session.last_activity_ms,
        Some(ms("2026-10-20T18:45:00.000Z"))
    );
    assert_eq!(
        session.raw_path.as_deref(),
        Some("https://cursor.com/agents/bc-00000000-0000-0000-0000-000000000002")
    );
    assert_eq!(session.cwd, None);
    assert!(session.models.is_empty());
    assert_eq!(candidate.locator, AGENT_ID);
    assert_eq!(candidate.session_id.as_deref(), Some(AGENT_ID));
    assert!(candidate
        .stamp
        .starts_with("cloud:2026-10-20T18:45:00.000Z:IDLE:"));

    // A rename the API reports without moving updatedAt still re-stamps.
    let mut renamed = listing["items"][0].clone();
    renamed["name"] = serde_json::json!("Prepare iOS build 38");
    assert_ne!(map_cursor_agent(&renamed).unwrap().0.stamp, candidate.stamp);
    // Not a Cloud Agent id: never catalogued.
    assert!(map_cursor_agent(&listing["items"][1]).is_none());
}

#[test]
fn cursor_agent_ids_never_take_a_local_transcript_identity() {
    assert_eq!(cursor_agent_id(AGENT_ID), Some(AGENT_ID));
    assert_eq!(cursor_agent_id("bc_abc123"), Some("bc_abc123"));
    // Local Cursor transcripts are named by bare UUIDs.
    assert_eq!(
        cursor_agent_id("9f764b2e-62a7-4657-bdf4-9eeecd269742"),
        None
    );
    for bad in ["bc-", "bc-a/b", "bc-a?b", "bc-a b", "BC-abc", ""] {
        assert_eq!(cursor_agent_id(bad), None, "{bad:?}");
    }
    assert_eq!(cursor_agent_id(&format!("bc-{}", "a".repeat(200))), None);
}

#[test]
fn cursor_key_resolves_from_env_before_the_key_file() {
    let _cleared = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let (configured, detail) = cursor_status(home.path());
    assert!(!configured);
    assert!(detail.contains("relayhistory/cursor-api-key"), "{detail}");

    let key_path = write_key(home.path());
    assert!(cursor_status(home.path()).0);
    let transport = ScriptedCursorTransport::new(vec![(200, r#"{"items":[]}"#)]);
    let provider = CursorCloudProvider::new(key_path.clone(), Arc::clone(&transport), None);
    let conn = catalog();
    provider
        .enumerate(&env_at(&conn, home.path()), None)
        .unwrap();
    assert_eq!(
        transport.calls.lock().unwrap()[0].1,
        API_KEY,
        "comment lines are skipped"
    );

    std::env::set_var("CURSOR_API_KEY", "key_from_cli_env");
    std::env::set_var("RELAYHISTORY_CURSOR_API_KEY", "key_from_override");
    let transport = ScriptedCursorTransport::new(vec![(200, r#"{"items":[]}"#)]);
    let provider = CursorCloudProvider::new(key_path, Arc::clone(&transport), None);
    provider
        .enumerate(&env_at(&conn, home.path()), None)
        .unwrap();
    assert_eq!(transport.calls.lock().unwrap()[0].1, "key_from_override");
}

#[test]
fn cursor_enumeration_pages_with_limit_and_cursor() {
    let _cleared = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let transport = ScriptedCursorTransport::new(vec![(200, AGENTS_PAGE_1), (200, AGENTS_PAGE_2)]);
    let provider = CursorCloudProvider::new(write_key(home.path()), Arc::clone(&transport), None);
    let conn = catalog();
    let candidates = provider
        .enumerate(&env_at(&conn, home.path()), None)
        .unwrap();
    let ids: Vec<_> = candidates.iter().map(|c| c.locator.as_str()).collect();
    assert_eq!(
        ids,
        [AGENT_ID, "bc-00000000-0000-0000-0000-000000000001"],
        "archived agents are history too"
    );
    assert_eq!(
        transport.urls(),
        [
            "https://api.cursor.com/v1/agents?limit=100",
            "https://api.cursor.com/v1/agents?limit=100&cursor=bc-00000000-0000-0000-0000-000000000001",
        ]
    );
    let row = provider
        .read_shallow(&env_at(&conn, home.path()).scan(), None, &candidates[1])
        .unwrap()
        .unwrap();
    assert_eq!(
        row.title.as_deref(),
        Some("Cloud sessions relayhistory sync")
    );
}

#[test]
fn cursor_enumeration_respects_the_row_limit() {
    let _cleared = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let transport = ScriptedCursorTransport::new(vec![(200, AGENTS_PAGE_1)]);
    let provider =
        CursorCloudProvider::new(write_key(home.path()), Arc::clone(&transport), Some(1));
    let conn = catalog();
    let candidates = provider
        .enumerate(&env_at(&conn, home.path()), None)
        .unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(
        transport.urls(),
        ["https://api.cursor.com/v1/agents?limit=1"]
    );
}

#[test]
fn cursor_enumeration_reports_configuration_auth_and_throttling() {
    let _cleared = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let conn = catalog();
    let key_path = home.path().join(".config/relayhistory/cursor-api-key");
    let transport = ScriptedCursorTransport::new(vec![]);
    let provider = CursorCloudProvider::new(key_path, Arc::clone(&transport), None);
    let error = provider
        .enumerate(&env_at(&conn, home.path()), None)
        .unwrap_err()
        .to_string();
    assert!(error.starts_with("CONNECTOR_NOT_CONFIGURED:"), "{error}");
    assert!(provider.check_available(home.path()).is_err());
    assert!(transport.urls().is_empty(), "no request without a key");

    let key_path = write_key(home.path());
    for (status, code) in [
        (401, "AUTHENTICATION_EXPIRED:"),
        (403, "PERMISSION_DENIED:"),
        (429, "CONNECTOR_FAILURE:"),
        (503, "CONNECTOR_FAILURE:"),
    ] {
        let transport = ScriptedCursorTransport::new(vec![(status, "{\"error\":\"x\"}")]);
        let provider = CursorCloudProvider::new(key_path.clone(), Arc::clone(&transport), None);
        let error = provider
            .enumerate(&env_at(&conn, home.path()), None)
            .unwrap_err()
            .to_string();
        assert!(error.starts_with(code), "{status}: {error}");
        assert!(
            error.contains(&format!("HTTP {status}")),
            "{status}: {error}"
        );
    }
}

#[test]
fn cursor_enumeration_fails_closed_at_the_page_bound() {
    let _cleared = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let endless =
        r#"{"items":[{"id":"bc-endless","createdAt":"2026-10-20T18:30:00Z"}],"nextCursor":"more"}"#;
    let transport = ScriptedCursorTransport::new(vec![(200, endless); MAX_LIST_PAGES]);
    let provider = CursorCloudProvider::new(write_key(home.path()), Arc::clone(&transport), None);
    let conn = catalog();
    let error = provider
        .enumerate(&env_at(&conn, home.path()), None)
        .unwrap_err()
        .to_string();
    assert!(error.starts_with("EVIDENCE_PARTIAL:"), "{error}");
}

#[test]
fn cursor_acquire_hydrates_the_conversation_on_run_timestamps() {
    let _cleared = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let key_path = write_key(home.path());
    let transport =
        ScriptedCursorTransport::new(vec![(200, AGENT), (200, RUNS), (200, CONVERSATION)]);
    let acquired = acquire_cursor_remote_session_at(&key_path, AGENT_ID, &transport).unwrap();
    assert_eq!(
        transport.urls(),
        [
            format!("https://api.cursor.com/v1/agents/{AGENT_ID}"),
            format!("https://api.cursor.com/v1/agents/{AGENT_ID}/runs?limit=100"),
            format!("https://api.cursor.com/v0/agents/{AGENT_ID}/conversation"),
        ]
    );
    let RemoteSessionEvidence::Normalized(snapshot) = acquired else {
        panic!("expected normalized evidence")
    };
    assert!(snapshot.source_stamp.starts_with("conversation:"));
    assert!(snapshot.source_bytes > 0);
    assert_eq!(
        snapshot.covered_kinds,
        vec![EvidenceKind::History, EvidenceKind::SessionEvent]
    );
    let events: Vec<_> = snapshot
        .records
        .iter()
        .filter(|record| record.kind == EvidenceKind::SessionEvent)
        .map(|record| {
            (
                record.payload["event_uid"].as_str().unwrap().to_string(),
                record.payload["role"].as_str().unwrap().to_string(),
                record.payload["ts_ms"].as_i64().unwrap(),
            )
        })
        .collect();
    let at = |iso: &str| ms(iso);
    assert_eq!(
        events,
        [
            ("msg_001".into(), "user".into(), at("2026-10-20T18:30:00Z")),
            (
                "msg_002".into(),
                "assistant".into(),
                at("2026-10-20T18:35:00Z")
            ),
            (
                "msg_003".into(),
                "assistant".into(),
                at("2026-10-20T18:35:00Z")
            ),
            ("msg_004".into(), "user".into(), at("2026-10-20T18:40:00Z")),
            (
                "msg_007".into(),
                "assistant".into(),
                at("2026-10-20T18:45:00Z")
            ),
        ],
        "unknown types and blank text are skipped; each turn takes its run's times"
    );
    let prompts: Vec<_> = snapshot
        .records
        .iter()
        .filter(|record| record.kind == EvidenceKind::History)
        .map(|record| record.payload["prompt"].as_str().unwrap())
        .collect();
    assert_eq!(
        prompts,
        [
            "Bump the iOS build number to 37 and prepare a TestFlight upload",
            "Also add a changelog bullet"
        ]
    );
    for record in &snapshot.records {
        assert_eq!(record.payload["source"], "cursor");
        assert_eq!(record.payload["session_id"], AGENT_ID);
    }

    // The snapshot passes the engine's own intake validation.
    let normalized = ai_hist::sources::normalize_source_evidence(
        "cursor",
        AGENT_ID,
        RemoteSessionEvidence::Normalized(snapshot),
    )
    .unwrap();
    assert_eq!(normalized.records.len(), 7);
}

#[test]
fn cursor_acquire_without_runs_falls_back_to_the_agent_span() {
    let _cleared = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let key_path = write_key(home.path());
    let transport = ScriptedCursorTransport::new(vec![
        (200, AGENT),
        (200, r#"{"items":[]}"#),
        (200, CONVERSATION),
    ]);
    let RemoteSessionEvidence::Normalized(snapshot) =
        acquire_cursor_remote_session_at(&key_path, AGENT_ID, &transport).unwrap()
    else {
        panic!("expected normalized evidence")
    };
    let times: Vec<_> = snapshot
        .records
        .iter()
        .filter(|record| record.kind == EvidenceKind::SessionEvent)
        .map(|record| record.payload["ts_ms"].as_i64().unwrap())
        .collect();
    let (created, updated) = (ms("2026-10-20T18:30:00Z"), ms("2026-10-20T18:45:00Z"));
    assert_eq!(times, [created, updated, updated, updated, updated]);
    assert!(times.windows(2).all(|pair| pair[0] <= pair[1]));
}

#[test]
fn cursor_acquire_reports_configuration_bad_ids_and_missing_agents() {
    let _cleared = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let missing_key = home.path().join("no-key");
    let transport = ScriptedCursorTransport::new(vec![]);
    let acquired = acquire_cursor_remote_session_at(&missing_key, AGENT_ID, &transport).unwrap();
    assert!(matches!(
        acquired,
        RemoteSessionEvidence::CapabilityLimited {
            code: "CONNECTOR_NOT_CONFIGURED",
            ..
        }
    ));
    let error = acquire_cursor_remote_session_at(&missing_key, "bc-a/../b", &transport)
        .unwrap_err()
        .to_string();
    assert!(error.starts_with("INVALID_ARGUMENT:"), "{error}");
    assert!(transport.urls().is_empty());

    let key_path = write_key(home.path());
    let transport = ScriptedCursorTransport::new(vec![(404, "{}")]);
    let error = acquire_cursor_remote_session_at(&key_path, AGENT_ID, &transport)
        .unwrap_err()
        .to_string();
    assert!(error.starts_with("SESSION_NOT_FOUND:"), "{error}");

    // A deleted agent's conversation is gone even while its record answers.
    let transport = ScriptedCursorTransport::new(vec![(200, AGENT), (200, RUNS), (404, "{}")]);
    let error = acquire_cursor_remote_session_at(&key_path, AGENT_ID, &transport)
        .unwrap_err()
        .to_string();
    assert!(error.starts_with("SESSION_NOT_FOUND:"), "{error}");
}

#[test]
fn cursor_cloud_agents_join_the_catalog_beside_local_cursor_sessions() {
    let _cleared = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    let conn = catalog();
    let local = ShallowSession {
        source: "cursor".into(),
        session_id: "9f764b2e-62a7-4657-bdf4-9eeecd269742".into(),
        raw_path: Some("/home/x/.cursor/projects/p/agent-transcripts/9f76/9f76.jsonl".into()),
        source_stamp: Some("v2:local".into()),
        discovery_state: "shallow".into(),
        ..Default::default()
    };
    crate::discover::upsert_shallow_session(&conn, &local).unwrap();

    let transport = ScriptedCursorTransport::new(vec![(200, AGENTS_PAGE_1), (200, AGENTS_PAGE_2)]);
    let providers: Vec<Box<dyn ShallowSessionProvider>> = vec![Box::new(CursorCloudProvider::new(
        write_key(home.path()),
        Arc::clone(&transport),
        None,
    ))];
    let options = DiscoverOptions {
        scope: SessionScope::Remote,
        ..Default::default()
    };
    let env = env_at(&conn, home.path());
    discover_sessions_with_providers(&env, &options, &providers, |_| {}).unwrap();

    let all = list_session_catalog(
        &conn,
        &CatalogListOptions {
            scope: SessionScope::All,
            ..Default::default()
        },
    )
    .unwrap();
    let mut cursor: Vec<_> = all
        .iter()
        .filter(|row| row.source == "cursor")
        .map(|row| (row.session_id.as_str(), row.locations.clone()))
        .collect();
    cursor.sort();
    assert_eq!(
        cursor,
        [
            (
                "9f764b2e-62a7-4657-bdf4-9eeecd269742",
                vec!["local".to_string()]
            ),
            (
                "bc-00000000-0000-0000-0000-000000000001",
                vec!["remote".to_string()]
            ),
            (
                "bc-00000000-0000-0000-0000-000000000002",
                vec!["remote".to_string()]
            ),
        ]
    );
}

#[test]
fn cursor_cloud_is_selectable_and_dispatchable() {
    let _cleared = without_credentials_override();
    let home = tempfile::tempdir().unwrap();
    assert!(SourceConnectorSelection::default().contains(CURSOR_CLOUD_CONNECTOR));
    assert!(SourceConnectorSelection::new(vec![CURSOR_CLOUD_CONNECTOR.into()]).is_ok());
    let provider =
        super::super::provider(home.path(), CURSOR_CLOUD_CONNECTOR, "default", None).unwrap();
    assert_eq!(provider.source(), "cursor");
    assert_eq!(provider.location(), SessionLocation::Remote);
    let selection = SourceConnectorSelection::default();
    assert!(
        selected_remote_providers(home.path(), None, &selection, &["cursor".into()]).is_empty()
    );
    write_key(home.path());
    let selected = selected_remote_providers(home.path(), None, &selection, &["cursor".into()]);
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].connector_id(), CURSOR_CLOUD_CONNECTOR);
}
