use ai_hist::{observations, SessionEvent};
use ai_hist::{
    sources::{AcquiredEvidence, ConnectorEvidence, ConnectorIdentity, SourceRegistry},
    Candidate, DiscoverOptions, DiscoveryEnv, HydrateSessionOptions, ScanEnv, SessionLocation,
    SessionScope, ShallowSession, ShallowSessionProvider,
};
use anyhow::Result;
use rusqlite::Connection;
use std::{
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

#[derive(Clone)]
struct Fixture {
    id: &'static str,
    instance: &'static str,
    location: SessionLocation,
    locator_override: Option<String>,
    display_override: Option<String>,
    enumeration_knows_id: bool,
    state: Arc<Mutex<State>>,
    probes: Arc<AtomicUsize>,
}
struct State {
    stamp: String,
    events: Vec<SessionEvent>,
    fail: bool,
    availability_fail: bool,
    listed: bool,
    not_session: bool,
    locators: Vec<String>,
}
impl Fixture {
    fn new(id: &'static str, instance: &'static str) -> Self {
        Self {
            id,
            instance,
            location: SessionLocation::Remote,
            locator_override: None,
            display_override: None,
            enumeration_knows_id: true,
            state: Arc::new(Mutex::new(State {
                stamp: "v1".into(),
                events: vec![event("shared", id), event(id, id)],
                fail: false,
                availability_fail: false,
                listed: true,
                not_session: false,
                locators: vec![],
            })),
            probes: Arc::new(AtomicUsize::new(0)),
        }
    }
    fn identity(&self) -> ConnectorIdentity {
        ConnectorIdentity::new(self.id, self.instance)
    }
    fn locator(&self) -> String {
        self.locator_override
            .clone()
            .unwrap_or_else(|| format!("{}://{}/session", self.id, self.instance))
    }
}
fn event(uid: &str, text: &str) -> SessionEvent {
    SessionEvent {
        id: 0,
        source: "claude".into(),
        session_id: "session".into(),
        project: None,
        cwd: None,
        git_branch: None,
        message_id: Some(uid.into()),
        parent_id: None,
        ts_ms: 1,
        role: "assistant".into(),
        kind: "text".into(),
        text: Some(text.into()),
        model: None,
        token_json: None,
        provider: None,
        stop_reason: None,
        event_uid: uid.into(),
    }
}
impl ShallowSessionProvider for Fixture {
    fn connector_id(&self) -> &str {
        self.id
    }
    fn connector_instance(&self) -> &str {
        self.instance
    }
    fn source(&self) -> &'static str {
        "claude"
    }
    fn location(&self) -> SessionLocation {
        self.location
    }
    fn check_available(&self, _home: &Path) -> Result<()> {
        self.probes.fetch_add(1, Ordering::SeqCst);
        anyhow::ensure!(
            !self.state.lock().unwrap().availability_fail,
            "fixture credentials missing"
        );
        Ok(())
    }
    fn enumerate(&self, _env: &DiscoveryEnv<'_>, _limit: Option<usize>) -> Result<Vec<Candidate>> {
        let state = self.state.lock().unwrap();
        if state.fail {
            anyhow::bail!("fixture unavailable")
        };
        Ok(if state.listed {
            vec![Candidate {
                source: "claude",
                locator: self.locator(),
                session_id: self.enumeration_knows_id.then(|| "session".into()),
                recency_hint_ms: Some(1),
                stamp: state.stamp.clone(),
            }]
        } else {
            vec![]
        })
    }
    fn read_shallow(
        &self,
        _scan: &ScanEnv<'_>,
        _catalog: Option<&Connection>,
        _candidate: &Candidate,
    ) -> Result<Option<ShallowSession>> {
        if self.state.lock().unwrap().not_session {
            return Ok(None);
        }
        Ok(Some(ShallowSession {
            source: "claude".into(),
            session_id: "session".into(),
            raw_path: Some(
                self.display_override
                    .clone()
                    .unwrap_or_else(|| self.locator()),
            ),
            ..Default::default()
        }))
    }
    fn acquire(
        &self,
        _home: &Path,
        observation: &observations::SessionObservation,
    ) -> Result<AcquiredEvidence> {
        let mut state = self.state.lock().unwrap();
        assert_eq!(observation.key.connector_id, self.id);
        assert_eq!(observation.key.connector_instance, self.instance);
        assert_eq!(
            observation.raw_locator.as_deref(),
            Some(self.locator().as_str())
        );
        state
            .locators
            .push(observation.raw_locator.clone().unwrap());
        if state.fail {
            anyhow::bail!("connector failed")
        };
        Ok(AcquiredEvidence::Events(ConnectorEvidence {
            source_stamp: state.stamp.clone(),
            source_bytes: state.events.len() as i64,
            events: state.events.clone(),
        }))
    }
}
fn options() -> DiscoverOptions {
    DiscoverOptions {
        scope: SessionScope::Remote,
        sources: vec!["claude".into()],
        limit: Some(1),
    }
}
fn hydration() -> HydrateSessionOptions {
    HydrateSessionOptions {
        source: "claude".into(),
        session_id: "session".into(),
        scope: SessionScope::Remote,
        include_related: false,
    }
}

#[test]
fn explicit_registry_rejects_unknown_and_duplicate_before_auth_or_db() -> Result<()> {
    let fixture = Fixture::new("installed", "account1");
    let mut registry = SourceRegistry::new();
    registry.register(Box::new(fixture.clone()))?;
    let dir = tempfile::tempdir()?;
    let db = dir.path().join("absent.db");
    let selection = [
        fixture.identity(),
        ConnectorIdentity::new("missing", "default"),
    ];
    assert!(registry
        .discover_at(&db, &options(), &selection, |_| {})
        .is_err());
    assert!(registry
        .sync_at(&db, SessionScope::Remote, &selection)
        .is_err());
    assert!(registry
        .hydrate_at(&db, &hydration(), &selection[1])
        .is_err());
    assert!(registry
        .discover_at(
            &db,
            &options(),
            &[fixture.identity(), fixture.identity()],
            |_| {}
        )
        .is_err());
    assert!(!db.exists());
    assert_eq!(fixture.probes.load(Ordering::SeqCst), 0);
    assert!(registry.register(Box::new(fixture)).is_err());
    Ok(())
}

#[test]
fn independent_connector_snapshots_are_canonical_in_both_scan_and_hydration_orders() -> Result<()> {
    for reverse in [false, true] {
        let a = Fixture::new("a", "account1");
        let b = Fixture::new("b", "account2");
        let mut registry = SourceRegistry::new();
        let order = if reverse { [&b, &a] } else { [&a, &b] };
        for fixture in order {
            registry.register(Box::new(fixture.clone()))?;
        }
        let dir = tempfile::tempdir()?;
        let db = dir.path().join("history.db");
        let summary =
            registry.discover_at(&db, &options(), &[a.identity(), b.identity()], |_| {})?;
        assert_eq!(summary.discovered, 2);
        assert_eq!(summary.connectors.len(), 2);
        assert!(summary
            .connectors
            .iter()
            .all(|entry| entry.summary.discovered == 1));
        for fixture in order {
            assert_eq!(
                registry
                    .hydrate_at(&db, &hydration(), &fixture.identity())?
                    .status,
                "hydrated"
            );
        }
        for fixture in [&a, &b] {
            assert_eq!(
                registry
                    .hydrate_at(&db, &hydration(), &fixture.identity())?
                    .status,
                "unchanged"
            );
        }
        let conn = ai_hist::open_db(&db)?;
        let observations = observations::list(&conn, "claude", "session")?;
        assert_eq!(observations.len(), 2);
        for observation in &observations {
            assert_eq!(
                observations::checkpoint(&conn, &observation.key)?
                    .unwrap()
                    .source_stamp
                    .as_deref(),
                Some("v1")
            );
        }
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM sessions", [], |r| r.get::<_, i64>(0))?,
            1
        );
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM session_events", [], |r| r
                .get::<_, i64>(0))?,
            3
        );
        assert_eq!(
            conn.query_row(
                "SELECT text FROM session_events WHERE event_uid='shared'",
                [],
                |r| r.get::<_, String>(0)
            )?,
            "a"
        );
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM session_presences WHERE location='local'",
                [],
                |r| r.get::<_, i64>(0)
            )?,
            0
        );
        // One connector withdrawing its own event cannot remove the other's copy.
        {
            let mut state = a.state.lock().unwrap();
            state.stamp = "v2".into();
            state.events.clear();
        }
        assert_eq!(
            registry
                .hydrate_at(&db, &hydration(), &a.identity())?
                .status,
            "updated"
        );
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM session_events", [], |r| r
                .get::<_, i64>(0))?,
            2
        );
        assert_eq!(
            conn.query_row(
                "SELECT text FROM session_events WHERE event_uid='shared'",
                [],
                |r| r.get::<_, String>(0)
            )?,
            "b"
        );
        {
            let mut state = b.state.lock().unwrap();
            state.fail = true;
        }
        assert!(registry
            .hydrate_at(&db, &hydration(), &b.identity())
            .is_err());
        assert_eq!(
            observations::get(&conn, &observations[1].key)?
                .unwrap()
                .access_state,
            "unavailable"
        );
        assert_eq!(
            observations::checkpoint(&conn, &observations[1].key)?
                .unwrap()
                .source_stamp
                .as_deref(),
            Some("v1")
        );
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM session_events", [], |r| r
                .get::<_, i64>(0))?,
            2
        );
        // Missing from one listing is not evidence that the observation was deleted.
        {
            let mut state = b.state.lock().unwrap();
            state.fail = false;
            state.listed = false;
        }
        registry.discover_at(&db, &options(), &[b.identity()], |_| {})?;
        assert_eq!(observations::list(&conn, "claude", "session")?.len(), 2);
        observations::set_access(&conn, &observations[1].key, "withdrawn")?;
        assert!(registry
            .hydrate_at(&db, &hydration(), &b.identity())
            .is_err());
        drop(conn);
        let conn = ai_hist::open_db(&db)?;
        assert_eq!(observations::list(&conn, "claude", "session")?.len(), 2);
    }
    Ok(())
}

#[test]
fn connector_instances_and_failed_capture_keep_independent_retry_state() -> Result<()> {
    let a = Fixture::new("installed", "first");
    let b = Fixture::new("installed", "second");
    let mut registry = SourceRegistry::new();
    registry.register(Box::new(a.clone()))?;
    registry.register(Box::new(b.clone()))?;
    let dir = tempfile::tempdir()?;
    let db = dir.path().join("history.db");
    registry.sync_at(&db, SessionScope::Remote, &[a.identity(), b.identity()])?;
    let conn = ai_hist::open_db(&db)?;
    conn.execute_batch("CREATE TRIGGER fail_capture BEFORE INSERT ON observation_hydration_checkpoints BEGIN SELECT RAISE(ABORT,'delivery capture full'); END;")?;
    assert!(registry
        .hydrate_at(&db, &hydration(), &a.identity())
        .is_err());
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM session_events", [], |r| r
            .get::<_, i64>(0))?,
        0
    );
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM observation_evidence", [], |r| r
            .get::<_, i64>(0))?,
        0
    );
    conn.execute_batch("DROP TRIGGER fail_capture;")?;
    assert_eq!(
        registry
            .hydrate_at(&db, &hydration(), &a.identity())?
            .status,
        "hydrated"
    );
    assert_eq!(
        registry
            .hydrate_at(&db, &hydration(), &b.identity())?
            .status,
        "hydrated"
    );
    assert_eq!(observations::list(&conn, "claude", "session")?.len(), 2);
    {
        let mut state = a.state.lock().unwrap();
        state.stamp = "bad".into();
        state.events[0].session_id = "another-session".into();
    }
    assert!(registry
        .hydrate_at(&db, &hydration(), &a.identity())
        .is_err());
    let observations = observations::list(&conn, "claude", "session")?;
    for observation in observations {
        assert_eq!(
            observations::checkpoint(&conn, &observation.key)?
                .unwrap()
                .source_stamp
                .as_deref(),
            Some("v1")
        );
    }
    Ok(())
}

#[test]
fn local_and_remote_observations_keep_the_local_projection_in_either_order() -> Result<()> {
    for reverse in [false, true] {
        let mut local = Fixture::new("local-adapter", "disk");
        local.location = SessionLocation::Local;
        let remote = Fixture::new("remote-adapter", "account");
        let mut registry = SourceRegistry::new();
        for fixture in if reverse {
            [&remote, &local]
        } else {
            [&local, &remote]
        } {
            registry.register(Box::new(fixture.clone()))?;
        }
        let dir = tempfile::tempdir()?;
        let db = dir.path().join("history.db");
        let mut options = options();
        options.scope = SessionScope::All;
        registry.discover_at(&db, &options, &[remote.identity()], |_| {})?;
        let mut local_options = hydration();
        local_options.scope = SessionScope::Local;
        registry.hydrate_at(&db, &local_options, &local.identity())?;
        registry.hydrate_at(&db, &hydration(), &remote.identity())?;
        let conn = ai_hist::open_db(&db)?;
        assert_eq!(observations::list(&conn, "claude", "session")?.len(), 2);
        assert_eq!(
            conn.query_row("SELECT raw_path FROM sessions", [], |row| row
                .get::<_, String>(0))?,
            local.locator()
        );
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM session_presences", [], |row| row
                .get::<_, i64>(0))?,
            2
        );
        // A cached rescan emits the canonical row, not whichever connector ran first.
        registry.discover_at(&db, &options, &[remote.identity()], |row| {
            assert_eq!(row.raw_path.as_deref(), Some(local.locator().as_str()))
        })?;
    }
    Ok(())
}

#[test]
fn one_connectors_non_session_skip_cannot_hide_another_connectors_same_locator() -> Result<()> {
    let mut a = Fixture::new("a", "first");
    a.locator_override = Some("fixture://shared".into());
    a.state.lock().unwrap().not_session = true;
    let mut b = Fixture::new("b", "second");
    b.locator_override = Some("fixture://shared".into());
    let mut registry = SourceRegistry::new();
    registry.register(Box::new(a.clone()))?;
    registry.register(Box::new(b.clone()))?;
    let dir = tempfile::tempdir()?;
    let db = dir.path().join("history.db");
    registry.discover_at(&db, &options(), &[a.identity()], |_| {})?;
    let result = registry.discover_at(&db, &options(), &[b.identity()], |_| {})?;
    assert_eq!(result.discovered, 1);
    let conn = ai_hist::open_db(&db)?;
    assert_eq!(observations::list(&conn, "claude", "session")?.len(), 1);
    assert_eq!(
        conn.query_row(
            "SELECT connector_id FROM observation_discovery_skips",
            [],
            |row| row.get::<_, String>(0)
        )?,
        "a"
    );
    Ok(())
}

#[test]
fn provider_and_two_recall_instances_keep_four_observations_one_session_two_locations() -> Result<()>
{
    for reverse in [false, true] {
        let mut local = Fixture::new("local-provider", "default");
        local.location = SessionLocation::Local;
        let provider = Fixture::new("provider-remote", "default");
        let recall_a = Fixture::new("recall", "account-a");
        let recall_b = Fixture::new("recall", "account-b");
        let mut adapters = [&local, &provider, &recall_a, &recall_b];
        if reverse {
            adapters.reverse();
        }
        let mut registry = SourceRegistry::new();
        for adapter in adapters {
            registry.register(Box::new(adapter.clone()))?;
        }
        let selected = [
            provider.identity(),
            recall_a.identity(),
            recall_b.identity(),
        ];
        let dir = tempfile::tempdir()?;
        let db = dir.path().join("history.db");
        let mut discovery = options();
        discovery.scope = SessionScope::All;
        let mut rows = vec![];
        registry.discover_at(&db, &discovery, &selected, |row| rows.push(row.clone()))?;
        assert_eq!(rows.len(), 1);
        for adapter in adapters {
            let mut options = hydration();
            options.scope = if adapter.location == SessionLocation::Local {
                SessionScope::Local
            } else {
                SessionScope::Remote
            };
            let result = registry.hydrate_at(&db, &options, &adapter.identity())?;
            assert_eq!(result.capability, "partial");
        }
        let conn = ai_hist::open_db(&db)?;
        assert_eq!(observations::list(&conn, "claude", "session")?.len(), 4);
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM sessions", [], |row| row
                .get::<_, i64>(0))?,
            1
        );
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM session_presences", [], |row| row
                .get::<_, i64>(0))?,
            2
        );
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM observation_hydration_checkpoints",
                [],
                |row| row.get::<_, i64>(0)
            )?,
            4
        );
    }
    Ok(())
}

#[test]
fn opaque_acquisition_locator_is_distinct_from_display_path_and_caches_without_id() -> Result<()> {
    let mut fixture = Fixture::new("opaque", "default");
    fixture.locator_override = Some("opaque-acquire-key".into());
    fixture.display_override = Some("https://display.example/session".into());
    fixture.enumeration_knows_id = false;
    let mut registry = SourceRegistry::new();
    registry.register(Box::new(fixture.clone()))?;
    let dir = tempfile::tempdir()?;
    let db = dir.path().join("history.db");
    let first = registry.discover_at(&db, &options(), &[fixture.identity()], |_| {})?;
    assert_eq!(first.discovered, 1);
    let second = registry.discover_at(&db, &options(), &[fixture.identity()], |_| {})?;
    assert_eq!(second.skipped_unchanged, 1);
    let conn = ai_hist::open_db(&db)?;
    assert_eq!(
        observations::list(&conn, "claude", "session")?[0]
            .raw_locator
            .as_deref(),
        Some("opaque-acquire-key")
    );
    assert_eq!(
        conn.query_row(
            "SELECT raw_path FROM sessions WHERE session_id='session'",
            [],
            |r| r.get::<_, String>(0)
        )?,
        "https://display.example/session"
    );
    registry.hydrate_at(&db, &hydration(), &fixture.identity())?;
    assert_eq!(
        fixture.state.lock().unwrap().locators,
        ["opaque-acquire-key"]
    );
    Ok(())
}

#[test]
fn unavailable_adapter_does_not_block_independent_local_or_remote_sources() -> Result<()> {
    let unavailable = Fixture::new("unavailable", "default");
    unavailable.state.lock().unwrap().availability_fail = true;
    let healthy = Fixture::new("healthy", "default");
    let mut registry = SourceRegistry::new();
    registry.register(Box::new(unavailable.clone()))?;
    registry.register(Box::new(healthy.clone()))?;
    let dir = tempfile::tempdir()?;
    let db = dir.path().join("history.db");
    assert!(registry
        .discover_at(&db, &options(), &[unavailable.identity()], |_| {})
        .unwrap_err()
        .to_string()
        .contains("CONNECTOR_NOT_CONFIGURED"));
    assert!(!db.exists());
    let result = registry.discover_at(
        &db,
        &options(),
        &[unavailable.identity(), healthy.identity()],
        |_| {},
    )?;
    assert_eq!(result.discovered, 1);
    assert_eq!(result.diagnostics.len(), 1);
    let mut local = Fixture::new("local", "default");
    local.location = SessionLocation::Local;
    registry.register(Box::new(local))?;
    let result = registry.discover_at(
        &db,
        &DiscoverOptions {
            scope: SessionScope::All,
            ..options()
        },
        &[unavailable.identity()],
        |_| {},
    )?;
    assert_eq!(result.discovered, 1);
    assert_eq!(result.diagnostics.len(), 1);
    Ok(())
}

#[test]
fn limited_discovery_defers_unknown_aliases_without_losing_existing_observations() -> Result<()> {
    let a = Fixture::new("a", "one");
    let mut b = Fixture::new("b", "two");
    b.enumeration_knows_id = false;
    let mut registry = SourceRegistry::new();
    registry.register(Box::new(a.clone()))?;
    registry.register(Box::new(b.clone()))?;
    let dir = tempfile::tempdir()?;
    let db = dir.path().join("history.db");
    let both = [a.identity(), b.identity()];
    let limited = registry.discover_at(&db, &options(), &both, |_| {})?;
    assert_eq!(limited.counters.shallow_reads, 1);
    let conn = ai_hist::open_db(&db)?;
    assert_eq!(observations::list(&conn, "claude", "session")?.len(), 1);
    // Selecting the opaque connector directly gives it its own discovery budget.
    registry.discover_at(&db, &options(), &[b.identity()], |_| {})?;
    assert_eq!(observations::list(&conn, "claude", "session")?.len(), 2);
    registry.discover_at(&db, &options(), &both, |_| {})?;
    assert_eq!(observations::list(&conn, "claude", "session")?.len(), 2);
    Ok(())
}
