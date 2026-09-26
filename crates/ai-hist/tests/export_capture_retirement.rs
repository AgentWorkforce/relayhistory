//! A store an earlier release armed for upload capture opens without it.
//!
//! That release kept a journal of every evidence write for its upload
//! subscriptions: per-table capture triggers feeding `delivery_journal`, and
//! retention triggers that abort a write once the journal's byte budget is
//! spent. Opening such a store drops the triggers and the identity indexes
//! and leaves every journal-era table, and the journal's `sqlite_sequence`
//! row, exactly as it found them: the upload daemon that owns those tables
//! reads `delivery_state.origin_id` and its revision floor from them.
use ai_hist::{ProviderRoots, SessionStore, StoreOptions, SyncOptions};
use rusqlite::Connection;
use std::fs;
use std::path::Path;

/// Every evidence table the earlier release captured.
const CAPTURED: &[&str] = &[
    "history",
    "session_events",
    "tool_calls",
    "file_edits",
    "sessions",
    "session_presences",
    "session_relationships",
    "session_commit_links",
    "trajectories",
    "session_observations",
    "observation_evidence",
    "session_markers",
];

/// The tables the earlier release created for capture and export. None of
/// them may be dropped or altered.
const JOURNAL_ERA_TABLES: &[&str] = &[
    "delivery_state",
    "delivery_journal",
    "delivery_shadow",
    "delivery_bootstrap_bounds",
    "delivery_exclusions",
    "history_subscriptions",
    "history_compaction",
    "history_exports",
    "history_export_pages",
];

const ORIGIN: &str = "5f0c1c4e0a8b4d6f9e2a7b3c1d0e9f8a";

#[allow(clippy::field_reassign_with_default)]
fn open(home: &Path) -> SessionStore {
    let mut options = StoreOptions::default();
    options.db_path = Some(home.join("ai-history.db"));
    options.roots = Some(ProviderRoots::from_home(
        home.to_path_buf(),
        home.join(".local/share/opencode/opencode.db"),
    ));
    SessionStore::open(options).expect("open")
}

/// The journal-era schema, as the earlier release left it: its tables, a
/// global upload subscription, a journal whose sequence has moved past its
/// rows, capture triggers on every evidence table, identity indexes, and a
/// retention budget that is already spent.
fn arm_earlier_release(conn: &Connection) {
    conn.execute_batch(&format!(
        r#"
CREATE TABLE delivery_state (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1), origin_id TEXT NOT NULL,
    retained_bytes INTEGER NOT NULL DEFAULT 0, max_retained_bytes INTEGER NOT NULL DEFAULT 268435456
);
INSERT INTO delivery_state(singleton, origin_id) VALUES (1, '{ORIGIN}');
CREATE TABLE delivery_journal (
    seq INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, source TEXT NOT NULL,
    session_id TEXT, record_key TEXT NOT NULL, operation TEXT NOT NULL, payload TEXT NOT NULL
);
CREATE INDEX delivery_journal_session ON delivery_journal(source,session_id,seq);
CREATE TABLE delivery_shadow (
    job_id TEXT NOT NULL, kind TEXT NOT NULL, row_id INTEGER NOT NULL,
    source TEXT NOT NULL, session_id TEXT, record_key TEXT NOT NULL, payload TEXT,
    PRIMARY KEY(job_id,kind,row_id)
);
CREATE TABLE delivery_bootstrap_bounds (
    job_id TEXT NOT NULL, kind TEXT NOT NULL, max_rowid INTEGER NOT NULL,
    PRIMARY KEY(job_id,kind)
);
CREATE TABLE delivery_exclusions (
    source TEXT NOT NULL, session_id TEXT NOT NULL, PRIMARY KEY(source,session_id)
);
CREATE TABLE history_subscriptions (
    id TEXT PRIMARY KEY, source TEXT, session_id TEXT, journal_cursor INTEGER NOT NULL,
    bootstrap_kind INTEGER NOT NULL DEFAULT 0, bootstrap_rowid INTEGER NOT NULL DEFAULT 0,
    bootstrap_done INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE history_compaction (singleton INTEGER PRIMARY KEY CHECK(singleton=1), cursor INTEGER NOT NULL);
INSERT INTO history_compaction VALUES (1, 0);
CREATE TABLE IF NOT EXISTS history_exports (
    id TEXT PRIMARY KEY, selection_json TEXT NOT NULL, limits_json TEXT NOT NULL,
    cutoff INTEGER NOT NULL, bootstrap_kind INTEGER NOT NULL DEFAULT 0,
    bootstrap_rowid INTEGER NOT NULL DEFAULT 0, bootstrap_done INTEGER NOT NULL DEFAULT 0,
    expires_at_ms INTEGER NOT NULL, cursor TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS history_export_pages (
    cursor TEXT PRIMARY KEY, export_id TEXT NOT NULL, payload TEXT NOT NULL
);
INSERT INTO history_subscriptions(id, journal_cursor, bootstrap_done) VALUES ('upload', 0, 1);
INSERT INTO delivery_journal(kind, source, record_key, operation, payload)
    VALUES ('session', 'claude', 'a', 'upsert', '{{}}'),
           ('session', 'claude', 'b', 'upsert', '{{}}'),
           ('session', 'claude', 'c', 'upsert', '{{}}');
DELETE FROM delivery_journal WHERE seq < 3;
"#
    ))
    .unwrap();
    for table in CAPTURED {
        for operation in ["insert", "update", "delete"] {
            conn.execute_batch(&format!(
                "CREATE TRIGGER delivery_{table}_{operation} AFTER {operation} ON {table} \
                 WHEN EXISTS(SELECT 1 FROM history_subscriptions) BEGIN \
                 INSERT INTO delivery_journal(kind, source, record_key, operation, payload) \
                 VALUES ('{table}', 'captured', '{operation}', 'upsert', '{{}}'); END;"
            ))
            .unwrap();
        }
        let session = match *table {
            "session_relationships" => "parent_session_id",
            "trajectories" => "id",
            _ => "session_id",
        };
        conn.execute_batch(&format!(
            "CREATE INDEX delivery_identity_{table} ON {table}({session});"
        ))
        .unwrap();
    }
    let size = "length(CAST(payload AS BLOB))+512";
    for table in [
        "delivery_journal",
        "delivery_shadow",
        "history_export_pages",
    ] {
        let new = size.replace("payload", "NEW.payload");
        let old = size.replace("payload", "OLD.payload");
        conn.execute_batch(&format!(
            r#"
CREATE TRIGGER {table}_cap_insert BEFORE INSERT ON {table} BEGIN
 SELECT CASE WHEN (SELECT retained_bytes+({new})>max_retained_bytes FROM delivery_state WHERE singleton=1) THEN RAISE(ABORT,'delivery retention limit exceeded; compact consumed data or raise the retention cap') END;
END;
CREATE TRIGGER {table}_count_insert AFTER INSERT ON {table} BEGIN
 UPDATE delivery_state SET retained_bytes=retained_bytes+({new}) WHERE singleton=1;
END;
CREATE TRIGGER {table}_cap_update BEFORE UPDATE ON {table} BEGIN
 SELECT CASE WHEN (SELECT retained_bytes+({new})-({old})>max_retained_bytes FROM delivery_state WHERE singleton=1) THEN RAISE(ABORT,'delivery retention limit exceeded; compact consumed data or raise the retention cap') END;
END;
CREATE TRIGGER {table}_count_update AFTER UPDATE ON {table} BEGIN
 UPDATE delivery_state SET retained_bytes=retained_bytes+({new})-({old}) WHERE singleton=1;
END;
CREATE TRIGGER {table}_count_delete AFTER DELETE ON {table} BEGIN
 UPDATE delivery_state SET retained_bytes=retained_bytes-({old}) WHERE singleton=1;
END;
"#
        ))
        .unwrap();
    }
    // The budget is spent: any journal write now aborts its transaction.
    conn.execute_batch(
        "UPDATE delivery_state SET max_retained_bytes = retained_bytes WHERE singleton = 1;
         DELETE FROM schema_migrations WHERE name = 'export_capture_retired_v1';",
    )
    .unwrap();
}

fn capture_objects(conn: &Connection) -> Vec<String> {
    conn.prepare(
        "SELECT name FROM sqlite_master \
         WHERE (type = 'trigger' AND (name LIKE 'delivery\\_%' ESCAPE '\\' \
                OR name LIKE '%\\_cap\\_%' ESCAPE '\\' OR name LIKE '%\\_count\\_%' ESCAPE '\\')) \
            OR (type = 'index' AND name LIKE 'delivery\\_identity\\_%' ESCAPE '\\') \
         ORDER BY name",
    )
    .unwrap()
    .query_map([], |row| row.get(0))
    .unwrap()
    .collect::<rusqlite::Result<_>>()
    .unwrap()
}

/// Everything the upload daemon may read from the journal-era tables.
fn journal_era_state(conn: &Connection) -> (Vec<String>, String, i64, i64, i64, i64) {
    let schema = JOURNAL_ERA_TABLES
        .iter()
        .map(|table| {
            conn.query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?1",
                [table],
                |row| row.get::<_, String>(0),
            )
            .unwrap_or_else(|error| panic!("{table} is gone: {error}"))
        })
        .collect();
    let (origin, retained): (String, i64) = conn
        .query_row(
            "SELECT origin_id, retained_bytes FROM delivery_state WHERE singleton = 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let floor: i64 = conn
        .query_row(
            "SELECT seq FROM sqlite_sequence WHERE name = 'delivery_journal'",
            [],
            |row| row.get(0),
        )
        .expect("the journal's sqlite_sequence row");
    let journal: i64 = conn
        .query_row("SELECT COUNT(*) FROM delivery_journal", [], |row| {
            row.get(0)
        })
        .unwrap();
    let subscriptions: i64 = conn
        .query_row("SELECT COUNT(*) FROM history_subscriptions", [], |row| {
            row.get(0)
        })
        .unwrap();
    (schema, origin, retained, floor, journal, subscriptions)
}

fn stage_claude(home: &Path) {
    let claude = home.join(".claude");
    let project = claude.join("projects/-work-app");
    fs::create_dir_all(&project).unwrap();
    fs::write(
        claude.join("history.jsonl"),
        "{\"display\":\"retire the journal\",\"timestamp\":1760000000000,\"project\":\"/work/app\",\"sessionId\":\"after-upgrade\"}\n",
    )
    .unwrap();
    fs::write(
        project.join("after-upgrade.jsonl"),
        concat!(
            "{\"sessionId\":\"after-upgrade\",\"uuid\":\"u1\",\"type\":\"user\",\"cwd\":\"/work/app\",",
            "\"message\":{\"role\":\"user\",\"content\":\"retire the journal\"},",
            "\"timestamp\":\"2026-09-25T10:00:00Z\"}\n",
            "{\"sessionId\":\"after-upgrade\",\"uuid\":\"u2\",\"parentUuid\":\"u1\",\"type\":\"assistant\",",
            "\"cwd\":\"/work/app\",\"message\":{\"id\":\"m1\",\"role\":\"assistant\",",
            "\"content\":[{\"type\":\"text\",\"text\":\"done\"}]},",
            "\"timestamp\":\"2026-09-25T10:00:01Z\"}\n"
        ),
    )
    .unwrap();
}

#[test]
fn an_armed_store_loses_its_capture_triggers_and_keeps_every_journal_era_table() {
    let home = tempfile::tempdir().unwrap();
    let db = home.path().join("ai-history.db");
    drop(open(home.path()));
    {
        let conn = Connection::open(&db).unwrap();
        arm_earlier_release(&conn);
        // The trap is armed: an evidence write reaches the journal and the
        // spent budget aborts it.
        let refused = conn
            .execute(
                "INSERT INTO history (source, session_id, prompt, timestamp_ms) \
                 VALUES ('claude', 'before', 'refused', 1)",
                [],
            )
            .unwrap_err()
            .to_string();
        assert!(
            refused.contains("delivery retention limit exceeded"),
            "{refused}"
        );
        assert!(!capture_objects(&conn).is_empty());
    }
    let before = journal_era_state(&Connection::open(&db).unwrap());
    assert_eq!(
        before.3, 3,
        "the floor is the sequence, past the journal's rows"
    );
    assert_eq!(before.4, 1);

    stage_claude(home.path());
    let store = open(home.path());
    let conn = Connection::open(&db).unwrap();
    assert_eq!(
        capture_objects(&conn),
        Vec::<String>::new(),
        "every capture trigger and identity index is dropped"
    );
    assert_eq!(
        journal_era_state(&conn),
        before,
        "every journal-era table, the origin and the journal's sequence stay as they were"
    );
    let retired: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE name = 'export_capture_retired_v1')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(retired);

    // New evidence lands, and none of it reaches the journal.
    store
        .sync(SyncOptions::default())
        .expect("sync after upgrade");
    let (history, events): (i64, i64) = conn
        .query_row(
            "SELECT (SELECT COUNT(*) FROM history WHERE session_id = 'after-upgrade'), \
                    (SELECT COUNT(*) FROM session_events WHERE session_id = 'after-upgrade')",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert!(history > 0, "the prompts were indexed");
    assert!(events > 0, "the transcript was indexed");
    assert_eq!(
        journal_era_state(&conn),
        before,
        "ingest wrote nothing to the journal"
    );

    // Opening again is a no-op.
    drop(store);
    drop(open(home.path()));
    assert_eq!(journal_era_state(&conn), before);
    assert!(capture_objects(&conn).is_empty());
}

/// An earlier release that opens the store again re-creates its triggers;
/// the next open of this one retires them again, whatever the marker says.
#[test]
fn a_store_rearmed_after_retirement_is_retired_again() {
    let home = tempfile::tempdir().unwrap();
    let db = home.path().join("ai-history.db");
    drop(open(home.path()));
    {
        let conn = Connection::open(&db).unwrap();
        arm_earlier_release(&conn);
    }
    drop(open(home.path()));
    {
        let conn = Connection::open(&db).unwrap();
        assert!(capture_objects(&conn).is_empty());
        conn.execute_batch(
            "CREATE TRIGGER delivery_history_insert AFTER INSERT ON history BEGIN \
             INSERT INTO delivery_journal(kind, source, record_key, operation, payload) \
             VALUES ('history', 'captured', 'insert', 'upsert', '{}'); END;",
        )
        .unwrap();
    }
    drop(open(home.path()));
    let conn = Connection::open(&db).unwrap();
    assert!(capture_objects(&conn).is_empty());
    conn.execute(
        "INSERT INTO history (source, session_id, prompt, timestamp_ms) \
         VALUES ('claude', 'after', 'accepted', 1)",
        [],
    )
    .expect("no capture trigger is left to abort an evidence write");
}
