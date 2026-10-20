use super::*;
use rusqlite::params;
const MAIN: &str = include_str!("../../../tests/fixtures/openclaw/main-schema24.sql");
const OPS: &str = include_str!("../../../tests/fixtures/openclaw/ops-schema24.sql");
const NATIVE: &str = "fixture/main:α";
const SESSION: &str = "main:main";
const TELEGRAM: &str = "main:telegram:direct:fixture-peer";
const RELAY: &str = "ops:relay:dm:fixture-sender";
const CRON: &str = "ops:cron:fixture-job";
fn database(home: &Path, agent: &str, sql: &str) -> (PathBuf, Connection) {
    let path = home.join(format!(
        ".openclaw/agents/{agent}/agent/openclaw-agent.sqlite"
    ));
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let db = Connection::open(&path).unwrap();
    db.execute_batch(sql).unwrap();
    (path, db)
}
fn fixture() -> (tempfile::TempDir, ProviderRoots, PathBuf, Connection) {
    let home = tempfile::tempdir().unwrap();
    let roots = ProviderRoots::from_home(home.path().into(), home.path().join("opencode.db"));
    let (path, db) = database(home.path(), "main", MAIN);
    (home, roots, path, db)
}
fn context(roots: &ProviderRoots) -> AcquisitionContext<'_> {
    AcquisitionContext::new(ID, roots)
}
fn conversation(roots: &ProviderRoots, path: &Path, id: &str) -> conversation::Conversation {
    serde_json::from_value(storage::capture(&context(roots), path, id).unwrap().payload).unwrap()
}
/// The newest window of a conversation, with its events.
fn capture(roots: &ProviderRoots, path: &Path, id: &str) -> Session {
    conversation(roots, path, id).windows.pop().unwrap()
}
fn count(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |r| r.get(0)).unwrap()
}
struct Ledger {
    conn: Connection,
    state: Map<String, Value>,
    repairs: crate::ingest::SweepRepairs,
}
impl Ledger {
    fn new(home: &Path) -> Self {
        Self {
            conn: crate::open_db(&home.join("ledger.db")).unwrap(),
            state: Map::new(),
            repairs: Default::default(),
        }
    }
    fn sweep(
        &mut self,
        roots: &ProviderRoots,
        paths: Option<&crate::ingest::path_scope::PathScope>,
    ) -> Result<SweepResult> {
        sweep(SweepContext {
            conn: &self.conn,
            state: &mut self.state,
            roots,
            paths,
            repairs: &self.repairs,
        })
    }
}

#[test]
fn fixture_capture_reads_pi_entries_and_openclaw_window_metadata() {
    let (_home, roots, path, _db) = fixture();
    let s = capture(&roots, &path, SESSION);
    assert_eq!(s.native_id, NATIVE);
    assert_eq!(s.agent_id, "main");
    assert_eq!(s.session_key, "agent:main:main");
    assert_eq!(s.channel.as_deref(), Some("main"));
    assert_eq!(s.schema_version, 24);
    let types: Vec<_> = s.events.iter().map(|e| e.entry["type"].clone()).collect();
    assert_eq!(types[0], "session");
    assert_eq!(s.events.len(), 10);
    assert!(s
        .events
        .iter()
        .all(|e| e.active || e.entry["type"] == "session"));
    let mirror = &s.events[5].entry["message"];
    assert!(format::is_delivery_mirror(mirror));
    let telegram = capture(&roots, &path, TELEGRAM);
    assert_eq!(telegram.channel.as_deref(), Some("telegram"));
    assert_eq!(
        telegram.events[1].entry["message"]["content"],
        "Run the failing fixture command"
    );
}

#[test]
fn agents_are_namespaced_and_discovered_after_roots_freeze() {
    let (home, roots, _path, _db) = fixture();
    assert_eq!(storage::enumerate(&roots, None).unwrap().len(), 2);
    let (ops, _ops_db) = database(home.path(), "ops", OPS);
    let after = storage::enumerate(&roots, None).unwrap();
    let ids: std::collections::BTreeSet<_> =
        after.iter().filter_map(|c| c.session_id.clone()).collect();
    assert_eq!(
        ids,
        [SESSION, TELEGRAM, RELAY, CRON].map(String::from).into()
    );
    let relay = capture(&roots, &ops, RELAY);
    assert_eq!(relay.channel.as_deref(), Some("relay"));
    // A session ID never resolves through another agent's database.
    let wrong = storage::capture(&context(&roots), &ops, SESSION);
    assert!(wrong
        .unwrap_err()
        .to_string()
        .contains("SESSION_SOURCE_MISMATCH"));
    // Non-agent entries under agents/ are ignored; malformed agent names are not.
    std::fs::write(home.path().join(".openclaw/agents/README"), "x").unwrap();
    std::fs::create_dir_all(home.path().join(".openclaw/agents/empty")).unwrap();
    assert_eq!(storage::enumerate(&roots, None).unwrap().len(), 4);
}

#[test]
fn custom_state_dir_is_path_namespaced_and_old_roots_ignore_environment() {
    let (home, mut roots, path, db) = fixture();
    drop(db);
    let custom = home.path().join("custom-state");
    std::fs::create_dir_all(custom.join("agents/main/agent")).unwrap();
    std::fs::rename(
        &path,
        custom.join("agents/main/agent/openclaw-agent.sqlite"),
    )
    .unwrap();
    roots.provider_roots.insert(ID.into(), vec![custom.clone()]);
    roots.provider_default_roots.remove(ID);
    let ids: Vec<_> = storage::enumerate(&roots, None)
        .unwrap()
        .into_iter()
        .filter_map(|c| c.session_id)
        .collect();
    assert_eq!(ids.len(), 2);
    assert!(ids
        .iter()
        .all(|id| id.starts_with("root-") && id.contains(":main:")));
    let mut json = serde_json::to_value(&roots).unwrap();
    json.as_object_mut().unwrap().remove("provider_roots");
    let old: ProviderRoots = serde_json::from_value(json).unwrap();
    assert_eq!(old.for_provider(ID), &[home.path().join(".openclaw")]);
}

#[test]
fn wal_only_commits_change_the_session_stamp_and_captured_generation() {
    let (_home, roots, path, db) = fixture();
    db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0")
        .unwrap();
    let stamp = |id: &str| {
        storage::enumerate(&roots, None)
            .unwrap()
            .into_iter()
            .find(|c| c.session_id.as_deref() == Some(id))
            .unwrap()
            .stamp
    };
    let (before, other) = (stamp(SESSION), stamp(TELEGRAM));
    let first = storage::capture(&context(&roots), &path, SESSION).unwrap();
    db.execute(
        "UPDATE session_windows SET updated_at=updated_at+1 WHERE session_id=?1",
        [NATIVE],
    )
    .unwrap();
    assert_ne!(before, stamp(SESSION));
    assert_eq!(
        other,
        stamp(TELEGRAM),
        "another session's write is not this session's change"
    );
    db.execute(
        "INSERT INTO transcript_events (session_id, seq, event_json, created_at) VALUES (?1, 10, ?2, 1790000099000)",
        params![NATIVE, r#"{"type":"model_change","id":"entry-99","parentId":"entry-5","timestamp":"2026-09-21T14:15:00.000Z","provider":"openai","modelId":"fixture-model-2"}"#],
    )
    .unwrap();
    let second = storage::capture(&context(&roots), &path, SESSION).unwrap();
    assert_ne!(first.stamp, second.stamp);
    assert_eq!(second.records, 11);
    assert_eq!(second.bytes, 0, "SQLite bytes are unknown");
    assert!(second
        .read_paths
        .iter()
        .any(|p| p.ends_with("openclaw-agent.sqlite-wal")));
}

#[test]
fn concurrent_snapshot_never_mixes_window_and_event_generations() {
    let (_home, roots, path, db) = fixture();
    db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0")
        .unwrap();
    let writer = std::thread::spawn(move || {
        for i in 1..30 {
            let tx = db.unchecked_transaction().unwrap();
            tx.execute("UPDATE session_windows SET model=?1", [i.to_string()])
                .unwrap();
            tx.execute(
                "UPDATE transcript_events SET event_json=json_set(event_json,'$.message.model',?1) WHERE seq=2",
                [i.to_string()],
            )
            .unwrap();
            tx.commit().unwrap();
        }
    });
    for _ in 0..30 {
        let s = capture(&roots, &path, SESSION);
        let window = s.model.clone().unwrap_or_else(|| "fixture-model".into());
        assert_eq!(window, s.events[2].entry["message"]["model"]);
    }
    writer.join().unwrap();
}

#[test]
fn compressed_events_decode_with_their_recorded_length_bound() {
    let (_home, roots, path, db) = fixture();
    let json: String = db
        .query_row(
            "SELECT event_json FROM transcript_events WHERE session_id=?1 AND seq=1",
            [NATIVE],
            |r| r.get(0),
        )
        .unwrap();
    let blob = zstd::bulk::compress(json.as_bytes(), 3).unwrap();
    let compress = |bytes: usize| {
        db.execute(
            "UPDATE transcript_events SET event_json=NULL, event_zstd=?2, event_utf8_bytes=?3, navigation_json=?4 WHERE session_id=?1 AND seq=1",
            params![NATIVE, blob, bytes as i64, r#"{"version":1,"report":{"kind":"canonical"},"navigation":{},"reset":{},"model":{},"modelBytes":0,"modelWithoutCheckpointBytes":0,"withoutCustomDataBytes":0}"#],
        )
        .unwrap();
    };
    compress(json.len());
    let s = capture(&roots, &path, SESSION);
    assert_eq!(
        format::text(&s.events[1].entry["message"]["content"]),
        "Find the OpenClaw fixture needle"
    );
    compress(json.len() - 1);
    let error = storage::capture(&context(&roots), &path, SESSION).unwrap_err();
    assert!(format!("{error:#}").contains("OPENCLAW_INVALID_RECORD"));
}

#[test]
fn missing_empty_unsupported_and_malformed_are_distinguished() {
    let home = tempfile::tempdir().unwrap();
    let roots = ProviderRoots::from_home(home.path().into(), home.path().join("opencode.db"));
    assert!(storage::enumerate(&roots, None).unwrap().is_empty());
    let (_home, roots, path, db) = fixture();
    db.execute_batch(
        "PRAGMA foreign_keys=OFF; DELETE FROM session_transcript_active_events; DELETE FROM transcript_event_identities; DELETE FROM transcript_events;",
    )
    .unwrap();
    assert!(storage::enumerate(&roots, None).unwrap().is_empty());
    db.execute_batch("UPDATE schema_meta SET schema_version=32; PRAGMA user_version=32")
        .unwrap();
    assert!(storage::enumerate(&roots, None)
        .unwrap_err()
        .to_string()
        .contains("unsupported OpenClaw agent schema 32"));
    db.execute_batch("PRAGMA user_version=24").unwrap();
    assert!(storage::enumerate(&roots, None)
        .unwrap_err()
        .to_string()
        .contains("mid-migration"));
    drop(db);
    std::fs::write(&path, b"not SQLite").unwrap();
    assert!(storage::enumerate(&roots, None).is_err());
}

#[test]
fn malformed_evidence_rolls_back_without_pruning_previous_rows() {
    let (home, roots, _path, db) = fixture();
    let mut ledger = Ledger::new(home.path());
    ledger.sweep(&roots, None).unwrap();
    let events = "SELECT count(*) FROM session_events WHERE source='openclaw'";
    let before = count(&ledger.conn, events);
    assert!(before > 0);
    db.execute(
        "UPDATE transcript_events SET event_json=json_set(event_json,'$.message.content',json('[{\"type\":\"toolCall\",\"name\":\"read\"}]')) WHERE session_id=?1 AND seq=2",
        [NATIVE],
    )
    .unwrap();
    // Upstream advances transcript_updated_at on every rewrite.
    db.execute(
        "UPDATE session_windows SET transcript_updated_at=transcript_updated_at+1 WHERE session_id=?1",
        [NATIVE],
    )
    .unwrap();
    let result = ledger.sweep(&roots, None).unwrap();
    assert_eq!(
        result.unread, 1,
        "the malformed session is unread, not empty"
    );
    assert_eq!(before, count(&ledger.conn, events));
    // The healthy session in the same database still syncs.
    db.execute(
        "UPDATE session_windows SET updated_at=updated_at+1 WHERE session_id='fixture-telegram-0001'",
        [],
    )
    .unwrap();
    assert_eq!(ledger.sweep(&roots, None).unwrap().unread, 1);
}

#[cfg(unix)]
#[test]
fn escaped_sidecar_and_agent_directory_are_rejected_before_open() {
    use std::os::unix::fs::symlink;
    let (home, roots, path, db) = fixture();
    drop(db);
    let outside = tempfile::tempdir().unwrap();
    let secret = outside.path().join("external");
    std::fs::write(&secret, "private").unwrap();
    let wal = storage::sidecar(&path, "-wal");
    symlink(&secret, &wal).unwrap();
    assert!(storage::capture(&context(&roots), &path, SESSION).is_err());
    std::fs::remove_file(wal).unwrap();
    symlink(outside.path(), home.path().join(".openclaw/agents/escape")).unwrap();
    assert!(storage::enumerate(&roots, None).is_err());
}

#[cfg(unix)]
#[test]
fn denied_database_reports_error_and_does_not_mutate_permissions() {
    use std::os::unix::fs::PermissionsExt;
    let (home, roots, path, db) = fixture();
    drop(db);
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o0)).unwrap();
    assert!(storage::enumerate(&roots, None).is_err());
    let mut ledger = Ledger::new(home.path());
    assert_eq!(ledger.sweep(&roots, None).unwrap().unread, 1);
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0
    );
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
}

#[test]
fn locked_live_database_is_unread_and_keeps_prior_evidence() {
    let (home, roots, _path, db) = fixture();
    let mut ledger = Ledger::new(home.path());
    ledger.sweep(&roots, None).unwrap();
    let sessions = "SELECT count(*) FROM sessions WHERE source='openclaw'";
    assert_eq!(count(&ledger.conn, sessions), 2);
    // A rollback-journal writer holding an exclusive lock blocks readers.
    db.execute_batch(
        "PRAGMA journal_mode=DELETE; BEGIN EXCLUSIVE; UPDATE session_windows SET model='busy';",
    )
    .unwrap();
    let result = ledger.sweep(&roots, None).unwrap();
    assert_eq!(result.unread, 1);
    assert_eq!(
        count(&ledger.conn, sessions),
        2,
        "a busy database proves nothing"
    );
    db.execute_batch("ROLLBACK").unwrap();
    assert_eq!(ledger.sweep(&roots, None).unwrap().unread, 0);
}

#[test]
fn sqlite_source_remains_byte_identical_and_journal_mode_unchanged() {
    let (home, roots, path, db) = fixture();
    db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_checkpoint(TRUNCATE)")
        .unwrap();
    let digest = |p: &Path| {
        use sha2::Digest;
        std::fs::read(p)
            .map(|b| sha2::Sha256::digest(b).to_vec())
            .ok()
    };
    let wal = storage::sidecar(&path, "-wal");
    let (main, log) = (digest(&path), digest(&wal));
    let mut ledger = Ledger::new(home.path());
    ledger.sweep(&roots, None).unwrap();
    storage::capture(&context(&roots), &path, SESSION).unwrap();
    assert_eq!(main, digest(&path));
    assert_eq!(log, digest(&wal));
    let mode: String = db
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode, "wal");
}

#[test]
fn normalization_records_tools_usage_metadata_and_delivery_mirrors() {
    let (home, roots, _path, _db) = fixture();
    let mut ledger = Ledger::new(home.path());
    ledger.sweep(&roots, None).unwrap();
    let conn = &ledger.conn;
    let call: (String, i64) = conn
        .query_row(
            "SELECT name, is_error FROM tool_calls WHERE source='openclaw' AND tool_use_id='call_fixture_exec'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(call, ("exec".into(), 1));
    let usage = "SELECT count(*) FROM session_events WHERE source='openclaw' AND token_json IS NOT NULL AND kind='text' AND role='assistant'";
    // Two answers plus the two heartbeat acknowledgements, which were real
    // model calls; the delivery mirror carries none.
    assert_eq!(count(conn, usage), 5);
    let mirror: Option<String> = conn
        .query_row(
            "SELECT token_json FROM session_events WHERE source='openclaw' AND model='delivery-mirror'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        mirror, None,
        "a delivery mirror repeats an already-counted reply"
    );
    let payload: String = conn
        .query_row(
            "SELECT payload_json FROM session_markers WHERE source='openclaw' AND session_id=?1 AND marker_uid='session:fixture-telegram-0001'",
            [TELEGRAM],
            |r| r.get(0),
        )
        .unwrap();
    let payload: Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(
        payload["session_key"],
        "agent:main:telegram:direct:fixture-peer"
    );
    assert_eq!(payload["agent_id"], "main");
    assert_eq!(payload["channel"], "telegram");
    assert_eq!(payload["native_session_id"], "fixture-telegram-0001");
    let last: String = conn
        .query_row(
            "SELECT last_assistant_text FROM sessions WHERE source='openclaw' AND session_id=?1",
            [SESSION],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(last, "Found the OpenClaw fixture needle.");
}

#[test]
fn openclaw_written_user_records_are_controls_not_prompts() {
    let (home, roots, path, db) = fixture();
    let control = r#"{"type":"message","id":"entry-control","parentId":"entry-5","timestamp":"2026-09-21T14:15:00.000Z","message":{"role":"user","content":[{"type":"text","text":"Inter-session note"}],"provenance":{"kind":"inter_session"},"timestamp":1790000099000}}"#;
    let custom = r#"{"type":"message","id":"entry-custom","parentId":"entry-control","timestamp":"2026-09-21T14:15:01.000Z","message":{"role":"custom","customType":"openclaw.nested-tool.v1","content":"nested","display":true,"timestamp":1790000099500}}"#;
    let external = r#"{"type":"message","id":"entry-external","parentId":"entry-custom","timestamp":"2026-09-21T14:15:02.000Z","message":{"role":"user","content":"A person on another channel","provenance":{"kind":"external_user"},"__openclaw":{"mirrorOrigin":"codex-app-server","mirrorIdentity":"m1","mirrorSourceFingerprint":"f1","upstreamUserText":""},"timestamp":1790000099700}}"#;
    for (seq, json) in [(10, control), (11, custom), (12, external)] {
        db.execute(
            "INSERT INTO transcript_events (session_id, seq, event_json, created_at) VALUES (?1, ?2, ?3, 1790000099000)",
            params![NATIVE, seq, json],
        )
        .unwrap();
    }
    let s = capture(&roots, &path, SESSION);
    assert_eq!(
        format::first_prompt(s.events.iter().map(|e| &e.entry)).as_deref(),
        Some("Find the OpenClaw fixture needle")
    );
    let mut ledger = Ledger::new(home.path());
    ledger.sweep(&roots, None).unwrap();
    let conn = &ledger.conn;
    assert_eq!(
        count(
            conn,
            "SELECT count(*) FROM history WHERE source='openclaw' AND prompt='Inter-session note'"
        ),
        0
    );
    assert_eq!(
        count(conn, "SELECT count(*) FROM session_events WHERE source='openclaw' AND control_kind='synthetic'"),
        3,
        "the inter-session note and both heartbeat polls"
    );
    assert_eq!(
        count(conn, "SELECT count(*) FROM history WHERE source='openclaw' AND prompt='A person on another channel'"),
        1,
        "external users and mirror-annotated prompts remain human prompts"
    );
    assert_eq!(
        count(conn, "SELECT count(*) FROM session_markers WHERE source='openclaw' AND marker_uid='entry-custom' AND kind='custom'"),
        1
    );
}

#[test]
fn unchanged_sessions_are_not_renormalized_and_changed_ones_are() {
    let (home, roots, _path, db) = fixture();
    let mut ledger = Ledger::new(home.path());
    ledger.sweep(&roots, None).unwrap();
    // Corrupt only the ledger copy: an unchanged session must not be re-read.
    ledger
        .conn
        .execute(
            "UPDATE session_events SET text='stale' WHERE source='openclaw' AND session_id=?1",
            [SESSION],
        )
        .unwrap();
    ledger.sweep(&roots, None).unwrap();
    let stale = format!(
        "SELECT count(*) FROM session_events WHERE session_id='{SESSION}' AND text='stale'"
    );
    assert!(count(&ledger.conn, &stale) > 0);
    db.execute(
        "UPDATE session_windows SET updated_at=updated_at+1 WHERE session_id=?1",
        [NATIVE],
    )
    .unwrap();
    ledger.sweep(&roots, None).unwrap();
    assert_eq!(count(&ledger.conn, &stale), 0);
    // A requested repair re-reads even an unchanged session.
    ledger
        .conn
        .execute(
            "UPDATE session_events SET text='stale' WHERE source='openclaw' AND session_id=?1",
            [TELEGRAM],
        )
        .unwrap();
    ledger.repairs = crate::ingest::SweepRepairs {
        sessions: [(ID.to_string(), TELEGRAM.to_string())].into(),
        all: false,
    };
    ledger.sweep(&roots, None).unwrap();
    let stale = format!(
        "SELECT count(*) FROM session_events WHERE session_id='{TELEGRAM}' AND text='stale'"
    );
    assert_eq!(count(&ledger.conn, &stale), 0);
}

#[test]
fn deleted_sessions_and_databases_retire_only_confirmed_local_evidence() {
    let (home, roots, path, db) = fixture();
    let mut ledger = Ledger::new(home.path());
    ledger.sweep(&roots, None).unwrap();
    crate::mark_session_presence(&ledger.conn, ID, SESSION, crate::SessionLocation::Remote)
        .unwrap();
    db.execute_batch(
        "PRAGMA foreign_keys=ON; DELETE FROM session_windows WHERE session_id='fixture-telegram-0001'; DELETE FROM session_nodes WHERE session_key='agent:main:telegram:direct:fixture-peer';",
    )
    .unwrap();
    let unrelated =
        crate::ingest::path_scope::PathScope::from_paths([home.path().join("unrelated.sqlite")])
            .unwrap();
    ledger.sweep(&roots, Some(&unrelated)).unwrap();
    assert!(crate::session_locations(&ledger.conn, ID, TELEGRAM)
        .unwrap()
        .contains(&"local".into()));
    let wal = crate::ingest::path_scope::PathScope::from_paths([storage::sidecar(&path, "-wal")])
        .unwrap();
    ledger.sweep(&roots, Some(&wal)).unwrap();
    assert!(crate::session_locations(&ledger.conn, ID, TELEGRAM)
        .unwrap()
        .is_empty());
    assert_eq!(
        count(
            &ledger.conn,
            &format!("SELECT count(*) FROM history WHERE session_id='{TELEGRAM}'")
        ),
        0
    );
    // A removed root is unavailable, not deleted.
    drop(db);
    let parked = home.path().join("parked");
    std::fs::rename(home.path().join(".openclaw"), &parked).unwrap();
    ledger.sweep(&roots, None).unwrap();
    assert!(crate::session_locations(&ledger.conn, ID, SESSION)
        .unwrap()
        .contains(&"local".into()));
    // The database itself removed inside an accessible root retires local
    // evidence and keeps the remote share.
    std::fs::rename(&parked, home.path().join(".openclaw")).unwrap();
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(storage::sidecar(&path, suffix));
    }
    ledger.sweep(&roots, None).unwrap();
    assert_eq!(
        crate::session_locations(&ledger.conn, ID, SESSION).unwrap(),
        ["remote"]
    );
}

#[test]
fn watches_cover_databases_and_new_agents_without_agent_directories() {
    let (home, roots, path, _db) = fixture();
    let watches = storage::watch_roots(&roots);
    let dirs: Vec<_> = watches.iter().map(|w| w.path.clone()).collect();
    assert!(dirs.contains(&path.parent().unwrap().to_path_buf()));
    assert!(dirs.contains(&home.path().join(".openclaw/agents")));
    assert!(!dirs.contains(&home.path().join(".openclaw/agents/main")));
}

#[test]
fn cancellation_inside_normalization_rolls_back_partial_evidence() {
    let (home, roots, path, _db) = fixture();
    let session = conversation(&roots, &path, SESSION);
    let conn = crate::open_db(&home.path().join("ledger.db")).unwrap();
    let calls = std::rc::Rc::new(std::cell::Cell::new(0));
    let counter = calls.clone();
    let result = crate::ingest::with_capture_stop(
        move || {
            let n = counter.get() + 1;
            counter.set(n);
            n >= 4
        },
        || {
            let tx = conn.unchecked_transaction()?;
            let result = normalize::session(&tx, &session, &path);
            assert!(
                result.is_err(),
                "normalization must stop inside its event loop"
            );
            result
        },
    );
    assert!(result.unwrap_err().is::<crate::ingest::CaptureCancelled>());
    assert_eq!(count(&conn, "SELECT count(*) FROM session_markers"), 0);
}

#[test]
fn fingerprint_never_opens_sqlite_and_changes_with_namespace() {
    let (_home, native, path, db) = fixture();
    drop(db);
    std::fs::write(&path, b"not sqlite").unwrap();
    let catalog = Connection::open_in_memory().unwrap();
    let mut custom = native.clone();
    custom.provider_default_roots.remove(ID);
    let fingerprint = |roots: &ProviderRoots| {
        let env = DiscoveryEnv::with_provider_roots(&catalog, roots.clone());
        Provider.fingerprint_inputs(&env).unwrap()
    };
    let (a, b) = (fingerprint(&native), fingerprint(&custom));
    assert_eq!(a.len(), 1);
    assert_ne!(a[0].stamp, b[0].stamp);
    assert!(storage::enumerate(&native, None).is_err());
}

#[test]
fn session_query_aggregates_events_once_and_bounds_selected_capture() {
    let (_home, _roots, _path, db) = fixture();
    let plan = |sql: &str| -> String {
        let mut stmt = db.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
        let rows = stmt
            .query_map(params![Option::<String>::None, 10], |r| {
                r.get::<_, String>(3)
            })
            .unwrap();
        rows.map(Result::unwrap).collect::<Vec<_>>().join("\n")
    };
    let all = plan(&storage::session_query(false));
    assert!(!all.contains("CORRELATED"), "{all}");
    let selected = storage::session_query(true);
    let mut stmt = db
        .prepare(&format!("EXPLAIN QUERY PLAN {selected}"))
        .unwrap();
    let rows: Vec<String> = stmt
        .query_map(params![NATIVE, 1], |r| r.get(3))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert!(
        rows.iter()
            .any(|r| r.contains("transcript_events") && r.contains("USING")),
        "{rows:?}"
    );
}

/// Upstream's own dump of a database the published 2026.9.9 writers created
/// and closed (openclaw/openclaw `test/fixtures/sqlite`, MIT), verbatim.
const UPSTREAM_V2026_9_9: &str = include_str!("upstream-agent-v2026.9.9.sql");
#[test]
fn published_2026_9_9_writer_catalog_is_read_without_extensions() {
    let home = tempfile::tempdir().unwrap();
    let roots = ProviderRoots::from_home(home.path().into(), home.path().join("opencode.db"));
    let (path, _db) = database(home.path(), "keeper", UPSTREAM_V2026_9_9);
    let candidates = storage::enumerate(&roots, None).unwrap();
    assert_eq!(candidates.len(), 1);
    let id = "keeper:stable-upgrade";
    let s = capture(&roots, &path, id);
    assert_eq!(s.title.as_deref(), Some("Stable keeper session"));
    assert_eq!(s.session_key, "agent:keeper:stable-upgrade");
    assert_eq!(
        format::first_prompt(s.events.iter().map(|e| &e.entry)).as_deref(),
        Some("Synthetic v2026.9.9 keeper transcript survives only when its agent survives.")
    );
    // The same catalog under another agent's directory is refused, as upstream does.
    let (other, _db) = database(home.path(), "other", UPSTREAM_V2026_9_9);
    let error = storage::capture(&context(&roots), &other, "other:stable-upgrade");
    assert!(format!("{:#}", error.unwrap_err()).contains("belongs to agent"));
}

#[test]
fn later_additive_schemas_are_read_by_column_name_and_postgres_stores_refused() {
    let (_home, roots, path, db) = fixture();
    // Schema 26 appends columns to session_nodes and transcript_events.
    db.execute_batch(
        "ALTER TABLE session_nodes ADD COLUMN session_started_at INTEGER; \
         ALTER TABLE transcript_events ADD COLUMN message_role TEXT; \
         ALTER TABLE session_windows ADD COLUMN row_transcript_json TEXT; \
         UPDATE schema_meta SET schema_version=31; PRAGMA user_version=31;",
    )
    .unwrap();
    let s = capture(&roots, &path, SESSION);
    assert_eq!(s.schema_version, 31);
    assert_eq!(s.events.len(), 10);
    db.execute_batch("UPDATE schema_meta SET schema_version=32; PRAGMA user_version=32")
        .unwrap();
    assert!(storage::enumerate(&roots, None).is_err());
    db.execute_batch(
        "UPDATE schema_meta SET schema_version=24; PRAGMA user_version=24; CREATE TABLE openclaw_engine_anchor (id INTEGER)",
    )
    .unwrap();
    let error = storage::enumerate(&roots, None).unwrap_err();
    assert!(error.to_string().contains("PostgreSQL"), "{error:#}");
}

fn make_cold(
    db: &Connection,
    roots: &ProviderRoots,
    path: &Path,
    native: &str,
    storage: &str,
) -> (Vec<u8>, PathBuf) {
    // Upstream archives decoded event JSON, whatever the row's encoding.
    let windows = [SESSION, TELEGRAM]
        .into_iter()
        .flat_map(|id| conversation(roots, path, id).windows);
    let window = windows.into_iter().find(|w| w.native_id == native).unwrap();
    let rows: Vec<(i64, String, i64)> = window
        .events
        .iter()
        .map(|e| (e.seq, e.entry.to_string(), e.created_at))
        .collect();
    let bytes = cold::encode(native, "cold-generation-1", &rows);
    let sha: String = {
        use sha2::Digest;
        sha2::Sha256::digest(&bytes)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    };
    let name = format!("{sha}.jsonl.zst");
    let file = cold::archive_path(path, &name).unwrap();
    if storage == "file" {
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, &bytes).unwrap();
    }
    let raw: usize = rows.iter().map(|r| r.1.len()).sum::<usize>() + rows.len() - 1;
    let tx = db.unchecked_transaction().unwrap();
    tx.execute(
        "INSERT INTO session_transcript_cold_archives (session_id, generation, archive_name, archive_sha256, event_count, raw_bytes, archive_bytes, last_seq, archived_at, storage, archive_blob) VALUES (?1,'cold-generation-1',?2,?3,?4,?5,?6,?7,1790000500000,?8,?9)",
        params![native, name, sha, rows.len() as i64, raw as i64, bytes.len() as i64, rows.last().unwrap().0, storage, (storage == "sqlite").then_some(bytes.clone())],
    )
    .unwrap();
    for table in [
        "session_transcript_active_events",
        "transcript_event_identities",
        "transcript_events",
    ] {
        tx.execute(
            &format!("DELETE FROM {table} WHERE session_id=?1"),
            [native],
        )
        .unwrap();
    }
    tx.commit().unwrap();
    (bytes, file)
}

#[test]
fn cold_archived_sessions_keep_their_history_and_verify_the_archive() {
    let (home, roots, path, db) = fixture();
    let mut ledger = Ledger::new(home.path());
    ledger.sweep(&roots, None).unwrap();
    let prompts = "SELECT count(*) FROM history WHERE source='openclaw'";
    assert_eq!(count(&ledger.conn, prompts), 2);
    let (_, file) = make_cold(&db, &roots, &path, NATIVE, "file");
    let (blob, _) = make_cold(&db, &roots, &path, "fixture-telegram-0001", "sqlite");
    assert!(!blob.is_empty());
    let s = capture(&roots, &path, SESSION);
    assert!(s.cold.is_some());
    assert_eq!(s.events.len(), 10);
    assert!(s.events[1..].iter().all(|e| e.active));
    let captured = storage::capture(&context(&roots), &path, SESSION).unwrap();
    assert!(captured.read_paths.contains(&file));
    assert_eq!(ledger.sweep(&roots, None).unwrap().unread, 0);
    assert_eq!(count(&ledger.conn, prompts), 2, "archiving is not deletion");
    // A tampered archive is unread, never empty history.
    std::fs::write(&file, b"tampered").unwrap();
    assert!(format!(
        "{:#}",
        storage::capture(&context(&roots), &path, SESSION).unwrap_err()
    )
    .contains("failed verification"));
    db.execute(
        "UPDATE session_transcript_cold_archives SET archived_at=archived_at+1",
        [],
    )
    .unwrap();
    assert_eq!(ledger.sweep(&roots, None).unwrap().unread, 1);
    assert_eq!(count(&ledger.conn, prompts), 2);
    std::fs::remove_file(&file).unwrap();
    assert_eq!(ledger.sweep(&roots, None).unwrap().unread, 1);
    assert_eq!(count(&ledger.conn, prompts), 2);
}

#[test]
fn transcript_only_and_cli_harness_rows_carry_no_request_usage() {
    let (home, roots, _path, db) = fixture();
    let usage = r#"{"input":50,"output":5,"cacheRead":0,"cacheWrite":0,"totalTokens":55,"cost":{"total":0.01}}"#;
    let zero =
        r#"{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"totalTokens":0,"cost":{"total":0}}"#;
    let rows = [
        (
            "entry-cli",
            r#""api":"cli","provider":"claude-cli","model":"claude-cli""#,
            usage,
            "CLI reply",
        ),
        (
            "entry-zero",
            r#""api":"openai-chatgpt-responses","provider":"openai","model":"gpt""#,
            zero,
            "Commentary",
        ),
        (
            "entry-codex",
            r#""api":"openai-chatgpt-responses","provider":"openai","model":"gpt","__openclaw":{"mirrorOrigin":"codex-app-server","runTerminal":true}"#,
            usage,
            "Codex terminal reply",
        ),
        (
            "entry-import",
            r#""api":"openai-responses","provider":"openai","model":"gpt","__openclaw":{"mirrorOrigin":"claude-catalog-import"}"#,
            usage,
            "Imported reply",
        ),
    ];
    for (seq, (id, fields, usage, text)) in rows.iter().enumerate() {
        let json = format!(
            r#"{{"type":"message","id":"{id}","parentId":"entry-5","timestamp":"2026-09-21T14:15:0{seq}.000Z","message":{{"role":"assistant","content":[{{"type":"text","text":"{text}"}}],{fields},"usage":{usage},"stopReason":"stop","timestamp":179000009{seq}000}}}}"#
        );
        db.execute(
            "INSERT INTO transcript_events (session_id, seq, event_json, created_at) VALUES (?1, ?2, ?3, 1790000099000)",
            params![NATIVE, 10 + seq as i64, json],
        )
        .unwrap();
    }
    let mut ledger = Ledger::new(home.path());
    ledger.sweep(&roots, None).unwrap();
    let counted = |id: &str| -> bool {
        ledger
            .conn
            .query_row(
                "SELECT token_json IS NOT NULL FROM session_events WHERE source='openclaw' AND message_id=?1",
                [id],
                |r| r.get(0),
            )
            .unwrap()
    };
    assert!(
        !counted("entry-cli"),
        "Claude Code's own log already holds this usage"
    );
    assert!(!counted("entry-zero"));
    assert!(!counted("entry-import"));
    assert!(
        counted("entry-codex"),
        "the private Codex home is not otherwise read"
    );
    let retained: String = ledger
        .conn
        .query_row(
            "SELECT payload_json FROM session_markers WHERE source='openclaw' AND marker_uid='message:entry-cli'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(retained.contains("uncounted_usage"));
}

#[test]
fn capture_matches_the_upstream_reader_oracle_including_compressed_rows() {
    let (home, roots, path, db) = fixture();
    let (ops, _ops_db) = database(home.path(), "ops", OPS);
    let compressed = count(
        &db,
        "SELECT count(*) FROM transcript_events WHERE event_zstd IS NOT NULL",
    );
    assert!(compressed > 0, "the writer stored a zstd row");
    let oracle: Value = serde_json::from_str(include_str!("oracle.json")).unwrap();
    for expected in oracle["sessions"].as_array().unwrap() {
        let agent = expected["agentId"].as_str().unwrap();
        let key = expected["sessionKey"].as_str().unwrap();
        let id = format!("{agent}:{}", conversation::native_name(key, agent));
        let s = conversation(&roots, if agent == "ops" { &ops } else { &path }, &id)
            .windows
            .into_iter()
            .find(|w| w.native_id == expected["sessionId"].as_str().unwrap())
            .unwrap();
        assert_eq!(s.session_key, expected["sessionKey"]);
        let actual: Vec<Value> = s
            .events
            .iter()
            .map(|e| {
                serde_json::json!({
                    "type": e.entry["type"],
                    "id": e.entry["id"],
                    "role": e.entry["message"]["role"],
                    "toolCallId": e.entry["message"]["toolCallId"],
                })
            })
            .collect();
        assert_eq!(&Value::Array(actual), &expected["events"], "{id}");
    }
}

fn append_event(db: &Connection, seq: i64, id: &str, parent: &str, text: &str) {
    let json = format!(
        r#"{{"type":"message","id":"{id}","parentId":"{parent}","timestamp":"2026-09-21T14:16:{:02}.000Z","message":{{"role":"user","content":"{text}","timestamp":17900002{seq:02}000}}}}"#,
        seq % 60
    );
    db.execute(
        "INSERT INTO transcript_events (session_id, seq, event_json, created_at) VALUES (?1, ?2, ?3, 1790000200000)",
        params![NATIVE, seq, json],
    )
    .unwrap();
    db.execute(
        "INSERT INTO transcript_event_identities (session_id, event_id, seq, event_type, parent_id, created_at) VALUES (?1, ?2, ?3, 'message', ?4, 1790000200000)",
        params![NATIVE, id, seq, parent],
    )
    .unwrap();
    db.execute(
        "UPDATE session_windows SET transcript_updated_at=transcript_updated_at+1 WHERE session_id=?1",
        [NATIVE],
    )
    .unwrap();
}

#[test]
fn appends_reread_only_the_tail_and_replaced_suffixes_reread_everything() {
    let (home, roots, _path, db) = fixture();
    let mut ledger = Ledger::new(home.path());
    ledger.sweep(&roots, None).unwrap();
    let last: String = db
        .query_row(
            "SELECT event_id FROM transcript_event_identities WHERE session_id=?1 ORDER BY seq DESC LIMIT 1",
            [NATIVE],
            |r| r.get(0),
        )
        .unwrap();
    // Grow the session past the re-read window so its oldest rows fall outside it.
    let mut parent = last;
    for seq in 10..(10 + TAIL as i64 + 4) {
        let id = format!("entry-append-{seq}");
        append_event(&db, seq, &id, &parent, &format!("appended prompt {seq}"));
        parent = id;
    }
    ledger.sweep(&roots, None).unwrap();
    let prompts =
        "SELECT count(*) FROM history WHERE source='openclaw' AND prompt LIKE 'appended prompt %'";
    assert_eq!(count(&ledger.conn, prompts), TAIL as i64 + 4);
    // Mark an early ledger row: an append-only pass must not re-read it.
    ledger
        .conn
        .execute(
            "UPDATE session_events SET text='kept' WHERE source='openclaw' AND session_id=?1 AND text='Find the OpenClaw fixture needle'",
            [SESSION],
        )
        .unwrap();
    let next = 10 + TAIL as i64 + 4;
    append_event(&db, next, "entry-append-last", &parent, "the newest prompt");
    ledger.sweep(&roots, None).unwrap();
    let kept =
        format!("SELECT count(*) FROM session_events WHERE session_id='{SESSION}' AND text='kept'");
    assert_eq!(
        count(&ledger.conn, &kept),
        1,
        "append-only pass re-read old rows"
    );
    assert_eq!(
        count(
            &ledger.conn,
            "SELECT count(*) FROM history WHERE prompt='the newest prompt'"
        ),
        1
    );
    // Replace the suffix at the same sequence: a different entry ID forces a
    // complete read, which retires the replaced prompt and repairs old rows.
    db.execute_batch(&format!(
        "DELETE FROM transcript_event_identities WHERE session_id='{NATIVE}' AND seq={next}; DELETE FROM transcript_events WHERE session_id='{NATIVE}' AND seq={next};"
    ))
    .unwrap();
    append_event(
        &db,
        next,
        "entry-replacement",
        &parent,
        "the replacement prompt",
    );
    ledger.sweep(&roots, None).unwrap();
    assert_eq!(
        count(
            &ledger.conn,
            "SELECT count(*) FROM history WHERE prompt='the newest prompt'"
        ),
        0
    );
    assert_eq!(
        count(
            &ledger.conn,
            "SELECT count(*) FROM history WHERE prompt='the replacement prompt'"
        ),
        1
    );
    assert_eq!(
        count(&ledger.conn, &kept),
        0,
        "a complete read repairs every row"
    );
}

#[test]
fn branch_copies_of_one_response_are_one_request() {
    let (home, roots, _path, db) = fixture();
    // OpenClaw keeps earlier copies of a response on inactive branches.
    let copy: String = db
        .query_row(
            "SELECT json_set(event_json, '$.id', 'entry-copy', '$.parentId', NULL) FROM transcript_events WHERE session_id=?1 AND seq=4",
            [NATIVE],
            |r| r.get(0),
        )
        .unwrap();
    db.execute(
        "INSERT INTO transcript_events (session_id, seq, event_json, created_at) VALUES (?1, 10, ?2, 1790000099000)",
        params![NATIVE, copy],
    )
    .unwrap();
    let store = crate::SessionStore::open(
        crate::StoreOptions::default()
            .db_path(home.path().join("store.db"))
            .roots(roots),
    )
    .unwrap();
    store
        .sync(crate::SyncOptions::default().sources([crate::Source::OpenClaw]))
        .unwrap();
    let evidence = store
        .session(
            &crate::SessionRef::id(crate::Source::OpenClaw, SESSION),
            crate::SessionQuery::default(),
        )
        .unwrap()
        .unwrap();
    let measured: Vec<_> = evidence
        .requests
        .iter()
        .filter_map(|r| r.usage.as_ref())
        .collect();
    assert_eq!(
        measured.len(),
        4,
        "two answers and two heartbeat acks; the copy of resp-fixture-2 is not another"
    );
    assert_eq!(measured.iter().map(|u| u.input_tokens).sum::<u64>(), 270);
}

#[test]
fn review_node_renames_restamp_and_failures_keep_cursors() {
    let (home, roots, path, db) = fixture();
    let mut ledger = Ledger::new(home.path());
    ledger.sweep(&roots, None).unwrap();
    // A node-only rename changes the title without touching the transcript.
    db.execute(
        "UPDATE session_nodes SET label='Renamed', updated_at=updated_at+1 WHERE session_key='agent:main:main'",
        [],
    )
    .unwrap();
    ledger.sweep(&roots, None).unwrap();
    let title: String = ledger
        .conn
        .query_row(
            "SELECT title FROM sessions WHERE source='openclaw' AND session_id=?1",
            [SESSION],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(title, "Renamed");
    // A locked database keeps every cursor: the next sweep re-reads nothing.
    let cursors = ledger.state[STAMPS].clone();
    db.execute_batch(
        "PRAGMA journal_mode=DELETE; BEGIN EXCLUSIVE; UPDATE session_windows SET model=model;",
    )
    .unwrap();
    assert_eq!(ledger.sweep(&roots, None).unwrap().unread, 1);
    assert_eq!(ledger.state[STAMPS], cursors);
    db.execute_batch("ROLLBACK").unwrap();
    drop(path);
}

#[test]
fn review_over_limit_listing_fails_without_retiring() {
    let (home, roots, _path, db) = fixture();
    let mut ledger = Ledger::new(home.path());
    ledger.sweep(&roots, None).unwrap();
    for i in 0..MAX_SWEEP {
        let sid = format!("extra-{i}");
        let key = format!("agent:main:cron:extra-{i}");
        db.execute("INSERT INTO session_nodes (session_key,current_session_id,entry_json,updated_at) VALUES (?1,?2,'{}',1)", [&key, &sid]).unwrap();
        db.execute("INSERT INTO session_windows (session_id,session_key,created_at,updated_at) VALUES (?1,?2,1790000000000,1790000000000)", [&sid, &key]).unwrap();
        db.execute(
            "INSERT INTO transcript_events (session_id,seq,event_json,created_at) VALUES (?1,0,?2,1790000000000)",
            params![sid, format!(r#"{{"type":"session","version":4,"id":"{sid}","timestamp":"2026-09-21T14:13:20.000Z","cwd":"/f"}}"#)],
        )
        .unwrap();
    }
    assert_eq!(ledger.sweep(&roots, None).unwrap().unread, 1);
    assert_eq!(
        count(
            &ledger.conn,
            "SELECT count(*) FROM sessions WHERE source='openclaw'"
        ),
        2,
        "a truncated listing retires nothing"
    );
}

#[test]
fn review_malformed_zero_counters_reach_the_usage_normalizer() {
    let msg = |usage: Value| serde_json::json!({"role":"assistant","provider":"openai","model":"m","api":"openai-responses","usage":usage});
    assert!(!format::counts_usage(&msg(
        serde_json::json!({"input":0,"output":0,"totalTokens":0})
    )));
    assert!(format::counts_usage(&msg(
        serde_json::json!({"input":-1,"output":0,"totalTokens":0})
    )));
    assert!(format::counts_usage(&msg(
        serde_json::json!({"input":0.5,"output":0})
    )));
}

#[test]
fn review_cold_archive_file_changes_restamp_and_fingerprint() {
    let (home, roots, path, db) = fixture();
    let (_, file) = make_cold(&db, &roots, &path, NATIVE, "file");
    let catalog = Connection::open_in_memory().unwrap();
    let fingerprint = || {
        let env = DiscoveryEnv::with_provider_roots(&catalog, roots.clone());
        Provider.fingerprint_inputs(&env).unwrap()
    };
    let stamp = || {
        storage::enumerate(&roots, None)
            .unwrap()
            .into_iter()
            .find(|c| c.session_id.as_deref() == Some(SESSION))
            .unwrap()
            .stamp
    };
    let (before, before_stamp) = (fingerprint(), stamp());
    assert!(before.iter().any(|c| Path::new(&c.locator) == file));
    std::fs::remove_file(&file).unwrap();
    assert_ne!(fingerprint(), before);
    assert_ne!(stamp(), before_stamp);
    let mut ledger = Ledger::new(home.path());
    assert_eq!(ledger.sweep(&roots, None).unwrap().unread, 1);
}

#[test]
fn review_cold_sessions_shallow_read_their_archive_and_prompt_rewrites_retire() {
    let (home, roots, path, db) = fixture();
    // A human prompt later rewritten as harness input loses its history row.
    let mut ledger = Ledger::new(home.path());
    ledger.sweep(&roots, None).unwrap();
    let prompt = "SELECT count(*) FROM history WHERE prompt='Find the OpenClaw fixture needle'";
    assert_eq!(count(&ledger.conn, prompt), 1);
    db.execute(
        "UPDATE transcript_events SET event_json=json_set(event_json,'$.message.excludeFromContext',json('true')) WHERE session_id=?1 AND seq=1",
        [NATIVE],
    )
    .unwrap();
    db.execute(
        "UPDATE transcript_rewrite_watermarks SET generation='rewritten' WHERE session_id=?1",
        [NATIVE],
    )
    .unwrap();
    ledger.sweep(&roots, None).unwrap();
    assert_eq!(
        count(&ledger.conn, prompt),
        0,
        "a control row supports no prompt"
    );
    // Cold sessions keep their first prompt and cwd in discovery.
    make_cold(&db, &roots, &path, "fixture-telegram-0001", "file");
    let catalog = crate::open_db(&home.path().join("catalog.db")).unwrap();
    let env = DiscoveryEnv::with_provider_roots(&catalog, roots.clone());
    let scan = env.scan();
    let candidate = storage::enumerate(&roots, None)
        .unwrap()
        .into_iter()
        .find(|c| c.session_id.as_deref() == Some(TELEGRAM))
        .unwrap();
    let shallow = storage::shallow(&scan, &candidate).unwrap().unwrap();
    assert_eq!(
        shallow.first_prompt.as_deref(),
        Some("Run the failing fixture command")
    );
    assert_eq!(shallow.cwd.as_deref(), Some("/fixture/project"));
}

#[test]
fn review_a_malformed_session_does_not_block_deleted_database_retirement() {
    let (home, roots, _path, db) = fixture();
    let (ops, ops_db) = database(home.path(), "ops", OPS);
    let mut ledger = Ledger::new(home.path());
    ledger.sweep(&roots, None).unwrap();
    let relay = "SELECT count(*) FROM sessions WHERE source='openclaw' AND session_id='ops:relay:dm:fixture-sender'";
    assert_eq!(count(&ledger.conn, relay), 1);
    // A session in main stays malformed across sweeps.
    db.execute(
        "UPDATE transcript_events SET event_json=json_set(event_json,'$.message.content',json('[{\"type\":\"toolCall\",\"name\":\"read\"}]')) WHERE session_id=?1 AND seq=2",
        [NATIVE],
    )
    .unwrap();
    db.execute(
        "UPDATE session_windows SET transcript_updated_at=transcript_updated_at+1 WHERE session_id=?1",
        [NATIVE],
    )
    .unwrap();
    drop(ops_db);
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(storage::sidecar(&ops, suffix));
    }
    assert_eq!(ledger.sweep(&roots, None).unwrap().unread, 1);
    assert_eq!(
        count(&ledger.conn, relay),
        0,
        "the deleted agent database retires"
    );
}

#[test]
fn cron_runs_and_heartbeats_are_one_conversation_not_one_session_each() {
    let home = tempfile::tempdir().unwrap();
    let roots = ProviderRoots::from_home(home.path().into(), home.path().join("opencode.db"));
    let (ops, db) = database(home.path(), "ops", OPS);
    let (main, _main_db) = database(home.path(), "main", MAIN);
    // Every isolated cron run is a window of the job's one conversation.
    let cron = conversation(&roots, &ops, CRON);
    assert_eq!(
        cron.windows
            .iter()
            .map(|w| w.native_id.as_str())
            .collect::<Vec<_>>(),
        ["fixture-cron-run-1", "fixture-cron-run-2"]
    );
    let mut ledger = Ledger::new(home.path());
    ledger.sweep(&roots, None).unwrap();
    let conn = &ledger.conn;
    assert_eq!(
        count(
            conn,
            "SELECT count(*) FROM sessions WHERE source='openclaw'"
        ),
        4,
        "main, telegram, relay and one cron job: no row per run"
    );
    assert_eq!(
        count(conn, "SELECT count(*) FROM session_markers WHERE source='openclaw' AND session_id='ops:cron:fixture-job' AND kind='session_metadata'"),
        2,
        "one header marker per run window"
    );
    // Both runs' usage is kept: they were real model calls.
    assert_eq!(
        count(conn, "SELECT count(*) FROM session_events WHERE source='openclaw' AND session_id='ops:cron:fixture-job' AND token_json IS NOT NULL"),
        2
    );
    // A quiet acknowledgement never becomes the conversation's reply.
    let last = |conn: &Connection, id: &str| -> Option<String> {
        conn.query_row(
            "SELECT last_assistant_text FROM sessions WHERE source='openclaw' AND session_id=?1",
            [id],
            |r| r.get(0),
        )
        .unwrap()
    };
    assert_eq!(
        last(conn, CRON).as_deref(),
        Some("Cron run one checked the fixture queue.")
    );
    assert_eq!(
        last(conn, SESSION).as_deref(),
        Some("Found the OpenClaw fixture needle.")
    );
    // Heartbeat polls are OpenClaw's own input, not human prompts.
    assert_eq!(
        count(conn, "SELECT count(*) FROM history WHERE source='openclaw' AND (prompt LIKE '%heartbeat%' OR prompt LIKE '%HEARTBEAT%')"),
        0
    );
    // A third run opens a new window: only it is read, the job keeps one row.
    let header = r#"{"type":"session","version":4,"id":"fixture-cron-run-3","timestamp":"2026-09-21T14:20:00.000Z","cwd":"/fixture/project"}"#;
    let reply = r#"{"type":"message","id":"cron-run-3-reply","parentId":null,"timestamp":"2026-09-21T14:20:01.000Z","message":{"role":"assistant","content":[{"type":"text","text":"Cron run three found work."}],"api":"openai-responses","provider":"openai","model":"fixture-model","usage":{"input":9,"output":2,"cacheRead":0,"cacheWrite":0,"totalTokens":11},"stopReason":"stop","timestamp":1790000501000}}"#;
    db.execute(
        "INSERT INTO session_windows (session_id, session_key, created_at, updated_at, transcript_updated_at) VALUES ('fixture-cron-run-3','agent:ops:cron:fixture-job',1789999995000,1790000501000,1790000501000)",
        [],
    )
    .unwrap();
    for (seq, (id, json)) in [("fixture-cron-run-3", header), ("cron-run-3-reply", reply)]
        .into_iter()
        .enumerate()
    {
        db.execute(
            "INSERT INTO transcript_events (session_id, seq, event_json, created_at) VALUES ('fixture-cron-run-3', ?1, ?2, 1790000501000)",
            params![seq as i64, json],
        )
        .unwrap();
        db.execute(
            "INSERT INTO transcript_event_identities (session_id, event_id, seq, event_type, created_at) VALUES ('fixture-cron-run-3', ?1, ?2, ?3, 1790000501000)",
            params![id, seq as i64, if seq == 0 { "session" } else { "message" }],
        )
        .unwrap();
    }
    // Mark an older run's ledger row: the append pass must not re-read it.
    ledger
        .conn
        .execute(
            "UPDATE session_events SET text='kept' WHERE source='openclaw' AND session_id='ops:cron:fixture-job' AND text='NO_REPLY'",
            [],
        )
        .unwrap();
    ledger.sweep(&roots, None).unwrap();
    let conn = &ledger.conn;
    assert_eq!(
        count(
            conn,
            "SELECT count(*) FROM sessions WHERE source='openclaw'"
        ),
        4
    );
    assert_eq!(
        last(conn, CRON).as_deref(),
        Some("Cron run three found work.")
    );
    assert_eq!(
        count(conn, "SELECT count(*) FROM session_events WHERE source='openclaw' AND session_id='ops:cron:fixture-job' AND text='kept'"),
        1,
        "unchanged runs are not re-read"
    );
    drop(main);
}

#[test]
fn heartbeat_prompts_and_acknowledgements_match_upstream_forms() {
    for prompt in [
        "[OpenClaw heartbeat poll]",
        "  [OpenClaw cron wake]  ",
        "[OpenClaw exec completion]\nDisable automatic completion turns",
        "Read HEARTBEAT.md if it exists (workspace context). Follow it strictly. Do not infer or repeat old tasks from prior chats. If nothing needs attention, reply HEARTBEAT_OK.\nCurrent time: now",
        "Run the following periodic tasks (only those due based on their intervals):\n- x\nAfter completing all due tasks, reply HEARTBEAT_OK.",
    ] {
        assert!(format::is_heartbeat_prompt(prompt), "{prompt}");
    }
    for prompt in [
        "Please read HEARTBEAT.md",
        "Read HEARTBEAT.md if it exists (workspace context). Follow it strictly. Do not infer or repeat old tasks from prior chats. If nothing needs attention, reply HEARTBEAT_OK. And more",
        "[OpenClaw heartbeat poll] and a question",
    ] {
        assert!(!format::is_heartbeat_prompt(prompt), "{prompt}");
    }
    for ack in [
        "NO_REPLY",
        "HEARTBEAT_OK",
        "**HEARTBEAT_OK**",
        "HEARTBEAT_OK all quiet",
    ] {
        assert!(format::is_heartbeat_ack(ack), "{ack}");
    }
    let long = format!("HEARTBEAT_OK {}", "x".repeat(301));
    for reply in ["Found the needle.", "NO_REPLY please", long.as_str()] {
        assert!(!format::is_heartbeat_ack(reply), "{reply}");
    }
}

#[test]
fn keys_too_long_for_a_ledger_identity_are_hashed_and_still_hydrate() {
    let (home, roots, path, db) = fixture();
    let long = format!("agent:main:relay:dm:{}", "peer".repeat(300));
    db.execute_batch("PRAGMA foreign_keys=OFF").unwrap();
    db.execute(
        "UPDATE session_nodes SET session_key=?1 WHERE session_key='agent:main:telegram:direct:fixture-peer'",
        [&long],
    )
    .unwrap();
    db.execute(
        "UPDATE session_windows SET session_key=?1 WHERE session_id='fixture-telegram-0001'",
        [&long],
    )
    .unwrap();
    let id = format!("main:{}", conversation::native_name(&long, "main"));
    assert!(id.len() < 100 && id.starts_with("main:sha="));
    let conv = conversation(&roots, &path, &id);
    assert_eq!(conv.key, long);
    // The public sync and hydrate path accepts the identity end to end.
    let store = crate::SessionStore::open(
        crate::StoreOptions::default()
            .db_path(home.path().join("store.db"))
            .roots(roots),
    )
    .unwrap();
    store
        .sync(crate::SyncOptions::default().sources([crate::Source::OpenClaw]))
        .unwrap();
    let evidence = store
        .session(
            &crate::SessionRef::id(crate::Source::OpenClaw, &id),
            crate::SessionQuery::default(),
        )
        .unwrap()
        .unwrap();
    assert!(!evidence.messages.is_empty());
}
