//! Older storage formats: agent schemas 19-23 (OpenClaw 2026.8.1-2026.9.6)
//! and the pre-2026.8 JSONL session store, each from its released writer.
use super::*;
use crate::ingest::path_scope::PathScope;
const SCHEMA19: &str = include_str!("../../../tests/fixtures/openclaw/main-schema19.sql");
const SCHEMA21: &str = include_str!("../../../tests/fixtures/openclaw/main-schema21.sql");
const SCHEMA23: &str = include_str!("../../../tests/fixtures/openclaw/main-schema23.sql");
pub(super) const SCHEMA24: &str =
    include_str!("../../../tests/fixtures/openclaw/main-schema24.sql");
pub(super) const MAIN: &str = "main:fixture-main-0001";
pub(super) const TELEGRAM: &str = "main:fixture-telegram-0001";

pub(super) fn home() -> (tempfile::TempDir, ProviderRoots) {
    let home = tempfile::tempdir().unwrap();
    let roots = ProviderRoots::from_home(home.path().into(), home.path().join("opencode.db"));
    (home, roots)
}
pub(super) fn database(home: &Path, sql: &str) -> (PathBuf, Connection) {
    let path = home.join(".openclaw/agents/main/agent/openclaw-agent.sqlite");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let db = Connection::open(&path).unwrap();
    db.execute_batch(sql).unwrap();
    (path, db)
}
/// The 2026.7.35 writer's sessions directories, copied into `home`.
pub(super) fn legacy(home: &Path) -> PathBuf {
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/openclaw/legacy-jsonl/.openclaw/agents");
    for agent in ["main", "ops"] {
        let to = home.join(".openclaw/agents").join(agent).join("sessions");
        std::fs::create_dir_all(&to).unwrap();
        for entry in std::fs::read_dir(fixture.join(agent).join("sessions")).unwrap() {
            let entry = entry.unwrap();
            std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
        }
    }
    home.join(".openclaw/agents/main/sessions")
}
pub(super) fn context(roots: &ProviderRoots) -> AcquisitionContext<'_> {
    AcquisitionContext::new(ID, roots)
}
fn capture(roots: &ProviderRoots, path: &Path, id: &str) -> Session {
    serde_json::from_value(storage::capture(&context(roots), path, id).unwrap().payload).unwrap()
}
pub(super) fn count(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |r| r.get(0)).unwrap()
}
fn ids(roots: &ProviderRoots) -> Vec<String> {
    let mut ids: Vec<_> = storage::enumerate(roots, None)
        .unwrap()
        .into_iter()
        .filter_map(|c| c.session_id)
        .collect();
    ids.sort();
    ids
}
fn summary(s: &Session) -> Vec<Value> {
    s.events
        .iter()
        .map(|e| {
            serde_json::json!({
                "type": e.entry["type"],
                "id": e.entry["id"],
                "role": e.entry["message"]["role"],
                "toolCallId": e.entry["message"]["toolCallId"],
            })
        })
        .collect()
}
pub(super) struct Ledger {
    pub(super) conn: Connection,
    pub(super) state: Map<String, Value>,
    repairs: crate::ingest::SweepRepairs,
}
impl Ledger {
    pub(super) fn new(home: &Path) -> Self {
        Self {
            conn: crate::open_db(&home.join("ledger.db")).unwrap(),
            state: Map::new(),
            repairs: Default::default(),
        }
    }
    pub(super) fn sweep(
        &mut self,
        roots: &ProviderRoots,
        paths: Option<&PathScope>,
    ) -> SweepResult {
        sweep(SweepContext {
            conn: &self.conn,
            state: &mut self.state,
            roots,
            paths,
            repairs: &self.repairs,
        })
        .unwrap()
    }
    pub(super) fn raw_path(&self, id: &str) -> Option<String> {
        self.conn
            .query_row(
                "SELECT raw_path FROM sessions WHERE source='openclaw' AND session_id=?1",
                [id],
                |r| r.get(0),
            )
            .ok()
    }
    pub(super) fn count(&self, sql: &str) -> i64 {
        count(&self.conn, sql)
    }
}

#[test]
fn released_schemas_19_21_23_match_their_writers_oracles() {
    for (sql, oracle, version, zstd, cold) in [
        (
            SCHEMA19,
            include_str!("oracle-schema19.json"),
            19,
            false,
            false,
        ),
        (
            SCHEMA21,
            include_str!("oracle-schema21.json"),
            21,
            false,
            true,
        ),
        (
            SCHEMA23,
            include_str!("oracle-schema23.json"),
            23,
            true,
            true,
        ),
        (SCHEMA24, include_str!("oracle.json"), 24, true, true),
    ] {
        let (home, roots) = home();
        let (path, db) = database(home.path(), sql);
        let schema = schema::detect(&db, "main").unwrap();
        assert_eq!(
            (schema.version, schema.transcripts, schema.zstd, schema.cold),
            (version, true, zstd, cold)
        );
        let oracle: Value = serde_json::from_str(oracle).unwrap();
        let main = oracle["sessions"].as_array().unwrap().iter();
        for expected in main.filter(|s| s["agentId"] == "main") {
            let id = format!("main:{}", expected["sessionId"].as_str().unwrap());
            let s = capture(&roots, &path, &id);
            assert_eq!(s.schema_version, Some(version));
            assert_eq!(s.session_key, expected["sessionKey"]);
            assert_eq!(
                &Value::Array(summary(&s)),
                &expected["events"],
                "{version} {id}"
            );
            assert!(s.events.iter().skip(1).all(|e| e.active));
        }
        assert_eq!(ids(&roots).len(), 2, "schema {version}");
        // Every released schema normalizes to the same evidence.
        let mut ledger = Ledger::new(home.path());
        let swept = ledger.sweep(&roots, None);
        assert_eq!(swept.unread, 0);
        assert_eq!(ledger.count("SELECT count(*) FROM history"), 2);
        assert_eq!(
            ledger.count("SELECT count(*) FROM tool_calls WHERE is_error=1"),
            1
        );
        assert_eq!(
            ledger.count("SELECT count(DISTINCT message_id) FROM session_events WHERE token_json IS NOT NULL"),
            4
        );
        // A grown transcript re-reads only its tail on every schema.
        db.execute_batch(
            "INSERT INTO transcript_events (session_id, seq, event_json, created_at) VALUES ('fixture-telegram-0001', 99, \
             '{\"type\":\"custom\",\"id\":\"tail-1\",\"parentId\":null,\"timestamp\":\"2026-09-21T14:14:00.000Z\",\"customType\":\"x\"}', 1790000300000); \
             INSERT INTO transcript_event_identities (session_id, event_id, seq, event_type, created_at) VALUES ('fixture-telegram-0001', 'tail-1', 99, 'custom', 1790000300000);",
        )
        .unwrap();
        ledger.sweep(&roots, None);
        assert_eq!(
            ledger.count("SELECT count(*) FROM session_markers WHERE marker_uid='tail-1'"),
            1,
            "schema {version}"
        );
    }
}

#[test]
fn unreleased_and_pre_transcript_schemas_are_told_apart() {
    let (home, roots) = home();
    let (_path, db) = database(home.path(), SCHEMA19);
    // 20 and 22 sit between released layouts and are read by their columns.
    for v in [20, 22] {
        db.execute_batch(&format!(
            "UPDATE schema_meta SET schema_version={v}; PRAGMA user_version={v}"
        ))
        .unwrap();
        assert_eq!(ids(&roots).len(), 2, "schema {v}");
    }
    // An older schema that already holds transcript tables is not guessed at.
    db.execute_batch("UPDATE schema_meta SET schema_version=18; PRAGMA user_version=18")
        .unwrap();
    assert!(
        format!("{:#}", storage::enumerate(&roots, None).unwrap_err())
            .contains("unsupported OpenClaw agent schema 18")
    );
    drop(db);
    // The 2026.7.x agent database: schema 1, auth profiles only.
    let (_home, roots) = self::home();
    let (path, _db) = database(
        _home.path(),
        "CREATE TABLE schema_meta (meta_key TEXT NOT NULL PRIMARY KEY, role TEXT NOT NULL, schema_version INTEGER NOT NULL, \
         agent_id TEXT, app_version TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL) STRICT; \
         CREATE TABLE auth_profile_store (id TEXT PRIMARY KEY); \
         INSERT INTO schema_meta VALUES ('primary','agent',1,'main',NULL,1,1); PRAGMA user_version=1;",
    );
    assert!(ids(&roots).is_empty());
    legacy(_home.path());
    assert_eq!(ids(&roots), [MAIN, TELEGRAM, "ops:fixture-relay-0001"]);
    let mut ledger = Ledger::new(_home.path());
    assert_eq!(ledger.sweep(&roots, None).unread, 0);
    assert_eq!(ledger.count("SELECT count(*) FROM sessions"), 3);
    assert!(std::fs::metadata(path).is_ok());
}

#[test]
fn legacy_transcripts_match_the_2026_7_35_reader_and_carry_index_metadata() {
    let (home, roots) = home();
    let dir = legacy(home.path());
    let oracle: Value = serde_json::from_str(include_str!("oracle-legacy.json")).unwrap();
    assert_eq!(oracle["package"], "openclaw@2026.7.35");
    for expected in oracle["sessions"].as_array().unwrap() {
        let agent = expected["agentId"].as_str().unwrap();
        let native = expected["sessionId"].as_str().unwrap();
        let path = home
            .path()
            .join(format!(".openclaw/agents/{agent}/sessions/{native}.jsonl"));
        let s = capture(&roots, &path, &format!("{agent}:{native}"));
        assert!(s.legacy);
        assert_eq!(s.schema_version, None);
        assert_eq!(s.session_key, expected["sessionKey"]);
        assert_eq!(&Value::Array(summary(&s)), &expected["events"], "{native}");
        assert!(s.events.iter().skip(1).all(|e| e.active));
    }
    let main = capture(&roots, &dir.join("fixture-main-0001.jsonl"), MAIN);
    assert_eq!(main.title.as_deref(), Some("Fixture main session"));
    assert_eq!(main.channel.as_deref(), Some("main"));
    assert_eq!(main.chat_type.as_deref(), Some("direct"));
    assert_eq!(main.model.as_deref(), Some("fixture-model"));
    let telegram = capture(&roots, &dir.join("fixture-telegram-0001.jsonl"), TELEGRAM);
    assert_eq!(telegram.window_channel.as_deref(), Some("telegram"));
    assert_eq!(telegram.channel.as_deref(), Some("telegram"));
    // A transcript never resolves to another session's identity.
    let wrong = storage::capture(
        &context(&roots),
        &dir.join("fixture-main-0001.jsonl"),
        TELEGRAM,
    );
    assert!(wrong
        .unwrap_err()
        .to_string()
        .contains("SESSION_SOURCE_MISMATCH"));

    let mut ledger = Ledger::new(home.path());
    assert_eq!(ledger.sweep(&roots, None).unread, 0);
    // Same evidence as the database layout: two prompts, one failed tool,
    // four measured requests, the delivery mirror unmetered.
    assert_eq!(
        ledger.count(
            "SELECT count(*) FROM history WHERE source='openclaw' AND session_id LIKE 'main:%'"
        ),
        2
    );
    assert_eq!(
        ledger.count("SELECT count(*) FROM tool_calls WHERE is_error=1"),
        1
    );
    assert_eq!(
        ledger.count(
            "SELECT count(DISTINCT message_id) FROM session_events WHERE token_json IS NOT NULL"
        ),
        5
    );
    let marker: String = ledger
        .conn
        .query_row(
            "SELECT payload_json FROM session_markers WHERE session_id=?1 AND marker_uid='session'",
            [MAIN],
            |r| r.get(0),
        )
        .unwrap();
    let marker: Value = serde_json::from_str(&marker).unwrap();
    assert_eq!(marker["layout"], "jsonl");
    assert_eq!(marker["session_key"], "agent:main:main");
    assert_eq!(
        ledger
            .conn
            .query_row(
                "SELECT title FROM sessions WHERE session_id=?1",
                [MAIN],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
        "Fixture main session"
    );
    assert_eq!(
        ledger.raw_path(MAIN),
        Some(dir.join("fixture-main-0001.jsonl").to_string_lossy().into())
    );
}

#[test]
fn archives_temp_files_and_checkpoints_are_not_sessions_and_deletes_retire() {
    let (home, roots) = home();
    let dir = legacy(home.path());
    let mut ledger = Ledger::new(home.path());
    ledger.sweep(&roots, None);
    for name in [
        "old-0001.jsonl.deleted.2026-09-20T10-00-00.000Z",
        "old-0002.jsonl.reset.2026-09-20T10-00-00Z",
        "sessions.json.0f6c1d2e-1111-4222-8333-444455556666.tmp",
        "sessions.json.bak.1790000000000",
        "fixture-main-0001.checkpoint.0f6c1d2e-1111-4222-8333-444455556666.jsonl",
        "fixture-main-0001.trajectory.jsonl",
    ] {
        std::fs::copy(dir.join("fixture-main-0001.jsonl"), dir.join(name)).unwrap();
    }
    assert_eq!(ids(&roots).len(), 3);
    // Upstream deletes a session by renaming its transcript to an archive:
    // the session retires like any deleted session.
    std::fs::rename(
        dir.join("fixture-telegram-0001.jsonl"),
        dir.join("fixture-telegram-0001.jsonl.deleted.2026-09-22T10-00-00.000Z"),
    )
    .unwrap();
    assert_eq!(ledger.sweep(&roots, None).unread, 0);
    assert!(crate::session_locations(&ledger.conn, ID, TELEGRAM)
        .unwrap()
        .is_empty());
    assert_eq!(
        ledger.count(&format!(
            "SELECT count(*) FROM history WHERE session_id='{TELEGRAM}'"
        )),
        0
    );
    assert!(ledger.raw_path(MAIN).is_some());
}

#[test]
fn the_database_owns_imported_sessions_and_keeps_them_across_the_archive_move() {
    let (home, roots) = home();
    let dir = legacy(home.path());
    let jsonl = dir.join("fixture-telegram-0001.jsonl");
    // Before the upgrade: every session comes from JSONL.
    let mut ledger = Ledger::new(home.path());
    ledger.sweep(&roots, None);
    assert_eq!(
        ledger.raw_path(TELEGRAM),
        Some(jsonl.to_string_lossy().into())
    );
    // The doctor imports the transcript under the same session ID while the
    // file is still in place: one candidate, read from the database.
    let (db, _db) = database(home.path(), SCHEMA24);
    let candidates = storage::enumerate(&roots, None).unwrap();
    let telegram: Vec<_> = candidates
        .iter()
        .filter(|c| c.session_id.as_deref() == Some(TELEGRAM))
        .collect();
    assert_eq!(telegram.len(), 1);
    assert_eq!(telegram[0].locator, db.to_string_lossy());
    assert_eq!(
        candidates.len(),
        4,
        "main-0001 JSONL plus the database's α and Telegram, plus ops"
    );
    let before = ledger.count(&format!(
        "SELECT count(*) FROM session_events WHERE session_id='{TELEGRAM}'"
    ));
    assert_eq!(ledger.sweep(&roots, None).unread, 0);
    assert_eq!(ledger.raw_path(TELEGRAM), Some(db.to_string_lossy().into()));
    assert_eq!(
        ledger.raw_path(MAIN),
        Some(dir.join("fixture-main-0001.jsonl").to_string_lossy().into())
    );
    // Not emitted twice: the database's copy replaced the JSONL rows.
    assert_eq!(
        ledger.count(&format!(
            "SELECT count(*) FROM session_events WHERE session_id='{TELEGRAM}'"
        )),
        before
    );
    assert_eq!(
        ledger.count(&format!(
            "SELECT count(*) FROM history WHERE session_id='{TELEGRAM}'"
        )),
        1
    );
    // The archive move retires nothing.
    let archive = home
        .path()
        .join(".openclaw/agents/main/session-sqlite-import-archive");
    std::fs::create_dir_all(&archive).unwrap();
    std::fs::rename(
        &jsonl,
        archive.join("agent_main_telegram.fixture-telegram-0001.jsonl.imported-1"),
    )
    .unwrap();
    assert_eq!(ledger.sweep(&roots, None).unread, 0);
    assert!(crate::session_locations(&ledger.conn, ID, TELEGRAM)
        .unwrap()
        .contains(&"local".into()));
    assert_eq!(
        ledger.count(&format!(
            "SELECT count(*) FROM history WHERE session_id='{TELEGRAM}'"
        )),
        1
    );
    assert_eq!(ids(&roots).len(), 4);
}

#[test]
fn an_unreadable_database_withholds_its_agents_jsonl_and_retirement() {
    let (home, roots) = home();
    let dir = legacy(home.path());
    let mut ledger = Ledger::new(home.path());
    ledger.sweep(&roots, None);
    let (db, conn) = database(home.path(), SCHEMA24);
    conn.execute_batch("UPDATE schema_meta SET schema_version=32; PRAGMA user_version=32")
        .unwrap();
    std::fs::remove_file(dir.join("fixture-main-0001.jsonl")).unwrap();
    let swept = ledger.sweep(&roots, None);
    assert!(swept.unread >= 2, "{}", swept.unread);
    // Neither the database nor the agent's transcripts proved a deletion.
    assert!(ledger.raw_path(MAIN).is_some());
    assert!(db.exists());
}

#[test]
fn index_edits_partial_tails_watches_and_fingerprints_cover_the_legacy_store() {
    let (home, roots) = home();
    let dir = legacy(home.path());
    let mut ledger = Ledger::new(home.path());
    ledger.sweep(&roots, None);
    let watches: Vec<_> = stores::watch_roots(&roots)
        .into_iter()
        .map(|w| w.path)
        .collect();
    assert!(watches.contains(&dir));
    let inputs = Provider
        .fingerprint_inputs(&DiscoveryEnv::with_provider_roots(
            &ledger.conn,
            roots.clone(),
        ))
        .unwrap();
    assert!(inputs
        .iter()
        .any(|c| c.locator == dir.join("sessions.json").to_string_lossy()));
    assert_eq!(inputs.len(), 5, "three transcripts and two indexes");
    // A label change in the index alone, seen through a scoped sweep of it.
    let index = dir.join("sessions.json");
    let text = std::fs::read_to_string(&index).unwrap();
    std::fs::write(
        &index,
        text.replace("Fixture main session", "Renamed session"),
    )
    .unwrap();
    let scope = PathScope::from_paths([index.clone()]).unwrap();
    ledger.sweep(&roots, Some(&scope));
    let title = |ledger: &Ledger| -> String {
        ledger
            .conn
            .query_row(
                "SELECT title FROM sessions WHERE session_id=?1",
                [MAIN],
                |r| r.get(0),
            )
            .unwrap()
    };
    assert_eq!(title(&ledger), "Renamed session");
    // An unterminated final record is deferred: the rest is read and the file
    // counts unread, without withholding another file's deletion.
    let path = dir.join("fixture-main-0001.jsonl");
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    std::io::Write::write_all(&mut file, b"{\"type\":\"message\",\"id\":\"x\"").unwrap();
    std::fs::remove_file(dir.join("fixture-telegram-0001.jsonl")).unwrap();
    let swept = ledger.sweep(&roots, None);
    assert_eq!(swept.unread, 1);
    assert!(ledger.raw_path(TELEGRAM).is_none());
    assert_eq!(
        ledger.count(&format!(
            "SELECT count(*) FROM history WHERE session_id='{MAIN}'"
        )),
        1
    );
}
