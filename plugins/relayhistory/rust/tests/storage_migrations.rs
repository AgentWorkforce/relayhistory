//! Upload/evidence migration regressions, owned by the probe after extraction.
use ai_hist::*;
use relayhistory_plugin::delivery::open_db;
use rusqlite::{params, Connection, OptionalExtension};
fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}
fn init_delivery_schema(conn: &Connection) -> anyhow::Result<()> {
    ai_hist::export::capture::initialize(conn)
}
#[test]
fn migrated_markers_are_delivered_in_the_new_shape_exactly_once() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("v1-journal.db");
    {
        let conn = open_db(&db_path).unwrap();
        conn.execute_batch(
            "DROP TRIGGER IF EXISTS delivery_session_markers_insert;
                 DROP TRIGGER IF EXISTS delivery_session_markers_update;
                 DROP TRIGGER IF EXISTS delivery_session_markers_delete;
                 DROP TABLE session_markers;
                 CREATE TABLE session_markers (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     source TEXT NOT NULL,
                     session_id TEXT NOT NULL,
                     marker_uid TEXT NOT NULL,
                     kind TEXT NOT NULL,
                     ts_ms INTEGER,
                     text TEXT,
                     detail_json TEXT,
                     UNIQUE(source, session_id, marker_uid)
                 );
                 INSERT INTO session_markers
                     (source, session_id, marker_uid, kind, ts_ms, text, detail_json)
                 VALUES ('grok', 'v1-j', 'c1', 'compaction_boundary', 11, 'compacted',
                         '{\"checkpoint\":1}');
                 DELETE FROM schema_migrations WHERE name = 'session_markers_v2';",
        )
        .unwrap();
        init_delivery_schema(&conn).unwrap();
        // A destination is subscribed, or nothing is journalled at all and
        // the test would pass over an empty table.
        conn.execute(
            "INSERT INTO delivery_jobs \
                 (id, destination_id, instance_id, account_id, generation, config_json, \
                  state, created_ms, cutoff, journal_cursor) \
                 VALUES ('j1','d1','i1','a1',1,'{}','active',1,0,0)",
            [],
        )
        .unwrap();
        // This hand-built job represents a pre-split database.
        conn.execute(
            "DELETE FROM schema_migrations WHERE name='probe_subscriptions_imported_v1'",
            [],
        )
        .unwrap();
        conn.execute("DELETE FROM delivery_journal", []).unwrap();
    }

    let conn = open_db(&db_path).unwrap();
    let rows: Vec<(String, String)> = conn
        .prepare(
            "SELECT operation, payload FROM delivery_journal \
                 WHERE kind = 'session_marker' ORDER BY seq",
        )
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "exactly one journal row per migrated marker, in one shape: {rows:?}"
    );
    let (operation, payload) = &rows[0];
    assert_eq!(operation, "upsert");
    let payload: serde_json::Value = serde_json::from_str(payload).unwrap();
    assert_eq!(
        payload.get("payload_json").and_then(|v| v.as_str()),
        Some("{\"checkpoint\":1}"),
        "the destination must receive the migrated payload: {payload}"
    );
    assert!(
        payload.get("detail_json").is_none(),
        "and never the retired column: {payload}"
    );
}
#[test]
fn a_delivery_open_settles_what_a_no_delivery_build_could_not_journal() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("handover.db");
    {
        let conn = open_db(&db_path).unwrap();
        conn.execute(
            "INSERT INTO delivery_jobs \
                 (id, destination_id, instance_id, account_id, generation, config_json, \
                  state, created_ms, cutoff, journal_cursor) \
                 VALUES ('j1','d1','i1','a1',1,'{}','active',1,0,0)",
            [],
        )
        .unwrap();
        // This hand-built job represents a pre-split database.
        conn.execute(
            "DELETE FROM schema_migrations WHERE name='probe_subscriptions_imported_v1'",
            [],
        )
        .unwrap();
        // What the other build left: triggers down, flag up, one migrated
        // row and one written blind afterwards.
        conn.execute_batch(
            "DROP TRIGGER IF EXISTS delivery_session_markers_insert;
                 DROP TRIGGER IF EXISTS delivery_session_markers_update;
                 DROP TRIGGER IF EXISTS delivery_session_markers_delete;
                 INSERT INTO session_markers
                     (source, session_id, marker_uid, kind, payload_json)
                 VALUES ('grok','hand','migrated','compaction_boundary','{\"c\":1}'),
                        ('codex','hand','written-blind','stream_error',NULL);
                 INSERT OR IGNORE INTO schema_migrations (name)
                 VALUES ('session_markers_v2_journal_pending');
                 DELETE FROM delivery_journal;",
        )
        .unwrap();
    }

    let conn = open_db(&db_path).unwrap();
    let keys: Vec<String> = conn
        .prepare(
            "SELECT record_key FROM delivery_journal \
                 WHERE kind = 'session_marker' ORDER BY seq",
        )
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(
        keys.len(),
        2,
        "both the migrated row and the one written while capture was off: {keys:?}"
    );
    assert!(
        keys.iter().any(|key| key.contains("written-blind")),
        "{keys:?}"
    );
    assert!(keys.iter().any(|key| key.contains("migrated")), "{keys:?}");
    assert!(
        !conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE name=?)",
                ["session_markers_v2_journal_pending"],
                |r| r.get::<_, bool>(0)
            )
            .unwrap(),
        "a paid debt is cleared, or every open repeats it"
    );

    // Positive control: reopening a settled database journals nothing more.
    let conn = open_db(&db_path).unwrap();
    let again: i64 = conn
        .query_row(
            "SELECT count(*) FROM delivery_journal WHERE kind = 'session_marker'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(again, 2, "a cleared flag must not re-journal on every open");
}
#[test]
fn an_unfinished_consumer_keeps_the_marker_preimage_it_was_promised() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("preimage.db");
    {
        let conn = open_db(&db_path).unwrap();
        conn.execute_batch(
            "DROP TRIGGER IF EXISTS delivery_session_markers_insert;
                 DROP TRIGGER IF EXISTS delivery_session_markers_update;
                 DROP TRIGGER IF EXISTS delivery_session_markers_delete;
                 DROP TABLE session_markers;
                 CREATE TABLE session_markers (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     source TEXT NOT NULL, session_id TEXT NOT NULL,
                     marker_uid TEXT NOT NULL, kind TEXT NOT NULL,
                     ts_ms INTEGER, text TEXT, detail_json TEXT,
                     UNIQUE(source, session_id, marker_uid)
                 );
                 INSERT INTO session_markers
                     (source, session_id, marker_uid, kind, ts_ms, text, detail_json)
                 VALUES ('grok','pre','c1','compaction_boundary',11,'compacted','{\"c\":1}');
                 DELETE FROM schema_migrations WHERE name = 'session_markers_v2';",
        )
        .unwrap();
        init_delivery_schema(&conn).unwrap();
        // A job part way through its bootstrap, whose marker bound still
        // covers this row: exactly the consumer the shadow exists for.
        conn.execute(
            "INSERT INTO delivery_jobs \
                 (id, destination_id, instance_id, account_id, generation, config_json, \
                  state, created_ms, cutoff, journal_cursor, bootstrap_done, \
                  bootstrap_kind, bootstrap_rowid) \
                 VALUES ('j1','d1','i1','a1',1,'{}','active',1,0,0,0,0,0)",
            [],
        )
        .unwrap();
        // This hand-built job represents a pre-split database.
        conn.execute(
            "DELETE FROM schema_migrations WHERE name='probe_subscriptions_imported_v1'",
            [],
        )
        .unwrap();
        let rowid: i64 = conn
            .query_row("SELECT rowid FROM session_markers", [], |row| row.get(0))
            .unwrap();
        conn.execute(
            "INSERT INTO delivery_bootstrap_bounds(job_id, kind, max_rowid) \
                 VALUES ('j1','session_marker',?)",
            params![rowid],
        )
        .unwrap();
        assert_eq!(
            conn.query_row("SELECT count(*) FROM delivery_shadow", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            0,
            "the premise: nothing shadowed yet"
        );
    }

    let conn = open_db(&db_path).unwrap();
    let preimage: Option<String> = conn
        .query_row(
            "SELECT payload FROM delivery_shadow \
                 WHERE job_id = 'j1' AND kind = 'session_marker'",
            [],
            |row| row.get(0),
        )
        .optional()
        .unwrap()
        .flatten();
    let preimage = preimage.expect("the row inside an unread bound must be shadowed");
    let preimage: serde_json::Value = serde_json::from_str(&preimage).unwrap();
    assert_eq!(
        preimage.get("detail_json").and_then(|v| v.as_str()),
        Some("{\"c\":1}"),
        "a resumed consumer must read the shape its snapshot promised: {preimage}"
    );

    // Positive control: the live row really did move on, so this is a
    // preimage rather than a copy of the current state.
    let markers = session_markers(&conn, "grok", "pre").unwrap();
    assert_eq!(markers[0].payload_json.as_deref(), Some("{\"c\":1}"));
    assert!(preimage.get("payload_json").is_none(), "{preimage}");
}
#[test]
fn a_delivery_database_missing_history_exports_still_opens() {
    let dir = tempfile::tempdir().unwrap();
    let v1 = |conn: &Connection| {
        conn.execute_batch(
            "DROP TRIGGER IF EXISTS delivery_session_markers_insert;
                 DROP TRIGGER IF EXISTS delivery_session_markers_update;
                 DROP TRIGGER IF EXISTS delivery_session_markers_delete;
                 DROP TABLE session_markers;
                 CREATE TABLE session_markers (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     source TEXT NOT NULL, session_id TEXT NOT NULL,
                     marker_uid TEXT NOT NULL, kind TEXT NOT NULL,
                     ts_ms INTEGER, text TEXT, detail_json TEXT,
                     UNIQUE(source, session_id, marker_uid)
                 );
                 INSERT INTO session_markers
                     (source, session_id, marker_uid, kind, ts_ms, text, detail_json)
                 VALUES ('grok','old','c1','compaction_boundary',11,'compacted','{\"c\":1}');
                 DELETE FROM schema_migrations WHERE name = 'session_markers_v2';",
        )
        .unwrap();
    };

    // An older delivery-enabled install: every delivery table this sweep
    // probes for, and not the one it forgot to probe for.
    let older = dir.path().join("older-delivery.db");
    {
        let conn = open_db(&older).unwrap();
        v1(&conn);
        // The table, and the capture triggers that name it -- an install
        // from before `history_exports` existed had neither, and leaving
        // the triggers behind would only prove that a trigger cannot read
        // a table that is gone, which is not the claim under test.
        let dependents: Vec<String> = conn
            .prepare(
                "SELECT name FROM sqlite_master WHERE type = 'trigger' \
                     AND sql LIKE '%history_exports%'",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert!(
            !dependents.is_empty(),
            "the premise: some trigger does name it"
        );
        for trigger in dependents {
            conn.execute_batch(&format!("DROP TRIGGER {trigger};"))
                .unwrap();
        }
        conn.execute_batch("DROP TABLE history_exports;").unwrap();
        let probed: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name IN \
                     ('delivery_shadow','delivery_bootstrap_bounds','delivery_jobs',\
                      'history_exports')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            probed, 3,
            "the premise: the three tables the probe asks about, and not the fourth"
        );
    }
    let conn = open_db(&older).expect("a missing history_exports must not fail the open");
    assert_eq!(
        session_markers(&conn, "grok", "old").unwrap().len(),
        1,
        "and the migration must still have run"
    );

    // Positive control: the same database with all four tables and an
    // in-flight export still takes the preimage, so this fixed the crash
    // rather than switching the sweep off.
    let whole = dir.path().join("whole-delivery.db");
    {
        let conn = open_db(&whole).unwrap();
        v1(&conn);
        init_delivery_schema(&conn).unwrap();
        let rowid: i64 = conn
            .query_row("SELECT rowid FROM session_markers", [], |row| row.get(0))
            .unwrap();
        conn.execute(
            "INSERT INTO history_exports \
                 (id, selection_json, limits_json, cutoff, bootstrap_kind, bootstrap_rowid, \
                  bootstrap_done, expires_at_ms, cursor) \
                 VALUES ('e1','{}','{}',0,0,0,0,?,'c1')",
            params![now_ms() + 3_600_000],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO delivery_bootstrap_bounds(job_id, kind, max_rowid) \
                 VALUES ('e1','session_marker',?)",
            params![rowid],
        )
        .unwrap();
    }
    let conn = open_db(&whole).unwrap();
    let preimage: Option<String> = conn
        .query_row(
            "SELECT payload FROM delivery_shadow \
                 WHERE job_id = 'e1' AND kind = 'session_marker'",
            [],
            |row| row.get(0),
        )
        .optional()
        .unwrap()
        .flatten();
    let preimage = preimage.expect("an unexpired export is still owed its preimage");
    assert!(preimage.contains("detail_json"), "{preimage}");
}

/// A delivery database whose batch triggers check the plain cap is upgraded
/// in place: reopening replaces them with the reserve triggers, and a batch
/// materializes at the cap.
#[test]
fn batch_cap_triggers_are_replaced_by_the_reserve_on_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("plain-cap.db");
    {
        let conn = open_db(&path).unwrap();
        conn.execute_batch(
            "DROP TRIGGER delivery_batches_reserve_insert;
             DROP TRIGGER delivery_batches_reserve_update;
             CREATE TRIGGER delivery_batches_cap_insert BEFORE INSERT ON delivery_batches BEGIN
              SELECT RAISE(ABORT,'delivery retention limit exceeded; plain cap');
             END;
             CREATE TRIGGER delivery_batches_cap_update BEFORE UPDATE ON delivery_batches BEGIN
              SELECT RAISE(ABORT,'delivery retention limit exceeded; plain cap');
             END;",
        )
        .unwrap();
    }
    // A rollback to a build that predates the reserve recreates its own cap
    // triggers beside the reserve ones; both must be gone after this open.
    {
        let conn = open_db(&path).unwrap();
        conn.execute_batch(
            "CREATE TRIGGER delivery_batches_cap_insert BEFORE INSERT ON delivery_batches BEGIN
              SELECT RAISE(ABORT,'delivery retention limit exceeded; plain cap');
             END;",
        )
        .unwrap();
    }
    let conn = open_db(&path).unwrap();
    let triggers: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='delivery_batches' ORDER BY name")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(
        triggers,
        [
            "delivery_batches_count_delete",
            "delivery_batches_count_insert",
            "delivery_batches_count_update",
            "delivery_batches_reserve_insert",
            "delivery_batches_reserve_update",
        ]
    );
    let job = relayhistory_plugin::delivery::create_job(
        &conn,
        &relayhistory_plugin::delivery::DeliveryJobConfig {
            destination_id: "fixture".into(),
            instance_id: "one".into(),
            account_id: "account".into(),
            mapping_version: "1".into(),
            selection: ai_hist::export::ExportSelection {
                all_sources: true,
                kinds: vec!["session_event".into()],
                ..Default::default()
            },
            limits: relayhistory_plugin::delivery::DeliveryLimits::default(),
        },
        now_ms(),
    )
    .unwrap();
    conn.execute("INSERT INTO session_events(source,session_id,event_uid,ts_ms,role,kind,text) VALUES ('claude','s','e',1,'user','text','backlog')", []).unwrap();
    let (used, _) = relayhistory_plugin::delivery::retained_bytes(&conn).unwrap();
    relayhistory_plugin::delivery::set_retention_limit(&conn, used).unwrap();
    assert!(
        relayhistory_plugin::delivery::prepare_batch(&conn, &job.job_id, now_ms())
            .unwrap()
            .batch_id
            .is_some()
    );
}

#[test]
fn reopening_a_reserve_schema_restores_missing_build_retry_history() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("reserve-without-build-history.db");
    {
        let conn = open_db(&path).unwrap();
        conn.execute_batch(
            "INSERT INTO delivery_jobs
             (id,destination_id,instance_id,account_id,generation,config_json,state,
              created_ms,cutoff,journal_cursor,retry_build)
             VALUES ('legacy','destination','instance','account',1,'{}','blocked',1,0,0,'0.26.1');
             DROP TABLE delivery_job_builds;",
        )
        .unwrap();
    }
    let conn = open_db(&path).unwrap();
    let restored: String = conn
        .query_row(
            "SELECT build FROM delivery_job_builds WHERE job_id='legacy'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(restored, "0.26.1");
    let reserve_triggers: usize = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger'
             AND name IN ('delivery_batches_reserve_insert','delivery_batches_reserve_update')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(reserve_triggers, 2);
}
