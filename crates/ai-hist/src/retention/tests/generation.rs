use super::*;

const KEY: &str = "retention.catalog_generation";

#[test]
fn actual_batches_at_the_same_time_have_distinct_durable_generations() {
    let (_dir, store, conn) = fixture();
    session(&conn, "old", 40, 3);
    acknowledge(&store, "cloud");
    let opts = RetentionOptions {
        max_rows: 1,
        ..Default::default()
    };
    assert_eq!(store.retention_generation().unwrap(), 0);
    for expected in 1..=3 {
        assert_eq!(run(&store, &opts, NOW).unwrap().rows, 1);
        assert_eq!(store.retention_generation().unwrap(), expected);
        assert_eq!(
            read_retention_generation(store.db_path()).unwrap(),
            expected
        );
    }
    assert_eq!(count(&conn, "session_events"), 0);
    assert_eq!(count(&conn, "sessions"), 1);
    // Finish deleting history and then the catalog-only batch.
    assert_eq!(run(&store, &opts, NOW).unwrap().rows, 1);
    assert_eq!(store.retention_generation().unwrap(), 4);
    assert_eq!(run(&store, &opts, NOW).unwrap().sessions, 1);
    assert_eq!(store.retention_generation().unwrap(), 5);
    assert_eq!(count(&conn, "sessions"), 0);
    assert_eq!(count(&conn, "evidence_tombstones"), 0);
    drop(store);
    let reopened = SessionStore::open(
        StoreOptions::default()
            .db_path(_dir.path().join("history.db"))
            .read_only(true),
    )
    .unwrap();
    assert_eq!(reopened.retention_generation().unwrap(), 5);
}

#[test]
fn no_op_control_writes_and_upload_protection_do_not_invalidate() {
    let (_dir, store, conn) = fixture();
    session(&conn, "old", 40, 1);
    assert!(
        run(&store, &RetentionOptions::default(), NOW)
            .unwrap()
            .awaiting_upload_ack
    );
    assert_eq!(store.retention_generation().unwrap(), 0);
    acknowledge(&store, "cloud");
    conn.execute("UPDATE sessions SET first_prompt='unuploaded'", [])
        .unwrap();
    assert_eq!(
        run(&store, &RetentionOptions::default(), NOW)
            .unwrap()
            .protected_sessions,
        1
    );
    assert_eq!(store.retention_generation().unwrap(), 0);
    // A cycle reset, then a protected session, both write maintenance bookkeeping.
    assert!(
        run(&store, &RetentionOptions::default(), NOW)
            .unwrap()
            .cycle_complete
    );
    assert_eq!(store.retention_generation().unwrap(), 0);
    acknowledge(&store, "cloud");
    assert_eq!(drain(&store, RetentionOptions::default()).sessions, 1);
    let generation = store.retention_generation().unwrap();
    assert!(generation > 0);
    for _ in 0..2 {
        assert!(
            run(&store, &RetentionOptions::default(), NOW)
                .unwrap()
                .cycle_complete
        );
        assert_eq!(store.retention_generation().unwrap(), generation);
    }
    store
        .set_retention_policy(RetentionPolicy::from_days(0))
        .unwrap();
    assert!(
        run(&store, &RetentionOptions::default(), NOW)
            .unwrap()
            .cycle_complete
    );
    assert_eq!(store.retention_generation().unwrap(), generation);
}

#[test]
fn catalog_only_and_anonymous_eviction_advance_generation() {
    let (_dir, store, conn) = fixture();
    conn.execute(
        "INSERT INTO sessions(source,session_id,last_activity_ms) VALUES('codex','empty',?1)",
        [NOW - 40 * DAY],
    )
    .unwrap();
    acknowledge(&store, "cloud");
    let report = run(&store, &RetentionOptions::default(), NOW).unwrap();
    assert_eq!((report.rows, report.sessions), (0, 1));
    assert_eq!(store.retention_generation().unwrap(), 1);
    conn.execute(
        "INSERT INTO history(source,prompt,timestamp_ms) VALUES('codex','synthetic anonymous',?1)",
        [NOW - 40 * DAY],
    )
    .unwrap();
    acknowledge(&store, "cloud");
    let report = run(&store, &RetentionOptions::default(), NOW).unwrap();
    assert_eq!((report.rows, report.sessions), (1, 0));
    assert_eq!(store.retention_generation().unwrap(), 2);
}

#[test]
fn failed_deletion_cannot_publish_generation() {
    let (_dir, store, conn) = fixture();
    session(&conn, "old", 40, 2);
    acknowledge(&store, "cloud");
    conn.execute_batch("CREATE TRIGGER reject_retention BEFORE DELETE ON history BEGIN SELECT RAISE(ABORT,'synthetic refusal'); END;").unwrap();
    assert!(run(&store, &RetentionOptions::default(), NOW).is_err());
    assert_eq!(store.retention_generation().unwrap(), 0);
    assert_eq!(count(&conn, "sessions"), 1);
    assert_eq!(count(&conn, "session_events"), 2);
    assert_eq!(count(&conn, "history"), 1);
    assert_eq!(count(&conn, "retention_pending"), 0);
    assert_eq!(count(&conn, "evidence_tombstones"), 0);
}

#[test]
fn generation_write_failure_rolls_back_eviction() {
    let (_dir, store, conn) = fixture();
    session(&conn, "old", 40, 2);
    acknowledge(&store, "cloud");
    conn.execute_batch("CREATE TRIGGER reject_generation BEFORE INSERT ON store_settings WHEN NEW.key='retention.catalog_generation' BEGIN SELECT RAISE(ABORT,'synthetic refusal'); END;").unwrap();
    assert!(run(&store, &RetentionOptions::default(), NOW).is_err());
    assert_eq!(store.retention_generation().unwrap(), 0);
    assert_eq!(count(&conn, "sessions"), 1);
    assert_eq!(count(&conn, "session_events"), 2);
    assert_eq!(count(&conn, "history"), 1);
    assert_eq!(count(&conn, "retention_pending"), 0);
    assert_eq!(count(&conn, "evidence_tombstones"), 0);
}

#[test]
fn exhausted_or_corrupt_generation_rolls_back_eviction() {
    let (_dir, store, conn) = fixture();
    session(&conn, "old", 40, 2);
    acknowledge(&store, "cloud");
    for value in [
        u64::MAX.to_string(),
        "corrupt".to_string(),
        "-1".to_string(),
    ] {
        conn.execute("INSERT INTO store_settings(key,value) VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value", params![KEY, value]).unwrap();
        assert!(run(&store, &RetentionOptions::default(), NOW).is_err());
        assert_eq!(count(&conn, "sessions"), 1);
        assert_eq!(count(&conn, "session_events"), 2);
        assert_eq!(count(&conn, "history"), 1);
        assert_eq!(count(&conn, "retention_pending"), 0);
        assert_eq!(count(&conn, "evidence_tombstones"), 0);
        let actual: String = conn
            .query_row(
                "SELECT value FROM store_settings WHERE key=?1",
                [KEY],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(actual, value);
    }
    assert!(store.retention_generation().is_err());
    assert!(read_retention_generation(store.db_path()).is_err());
}

#[test]
fn legacy_missing_table_and_key_read_zero_without_writes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch("CREATE TABLE old_shape(value TEXT);")
        .unwrap();
    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(read_retention_generation(&path).unwrap(), 0);
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    assert!(!conn
        .prepare("SELECT 1 FROM sqlite_master WHERE name='store_settings'")
        .unwrap()
        .exists([])
        .unwrap());

    let (_dir, store, conn) = fixture();
    let readonly = SessionStore::open(
        StoreOptions::default()
            .db_path(store.db_path())
            .read_only(true),
    )
    .unwrap();
    let before: i64 = conn
        .pragma_query_value(None, "data_version", |r| r.get(0))
        .unwrap();
    assert_eq!(readonly.retention_generation().unwrap(), 0);
    assert_eq!(read_retention_generation(store.db_path()).unwrap(), 0);
    let after: i64 = conn
        .pragma_query_value(None, "data_version", |r| r.get(0))
        .unwrap();
    assert_eq!(before, after, "no connection committed a write");
    assert!(!conn
        .prepare("SELECT 1 FROM store_settings WHERE key=?1")
        .unwrap()
        .exists([KEY])
        .unwrap());
    // A pre-retention settings table is optional even on an existing store handle.
    conn.execute_batch("DROP TABLE store_settings;").unwrap();
    assert_eq!(readonly.retention_generation().unwrap(), 0);
    assert!(!conn
        .prepare("SELECT 1 FROM sqlite_master WHERE name='store_settings'")
        .unwrap()
        .exists([])
        .unwrap());
}

#[test]
fn failure_after_generation_update_rolls_back_the_entire_batch() {
    let (_dir, store, conn) = fixture();
    session(&conn, "old", 40, 2);
    acknowledge(&store, "cloud");
    conn.execute(
        "INSERT INTO store_settings(key,value) VALUES(?1,'7')",
        [KEY],
    )
    .unwrap();
    conn.execute_batch("CREATE TRIGGER reject_batch_time BEFORE INSERT ON store_settings WHEN NEW.key='retention.last_batch_ms' BEGIN SELECT RAISE(ABORT,'synthetic refusal after generation update'); END;").unwrap();
    assert!(run(&store, &RetentionOptions::default(), NOW).is_err());
    assert_eq!(store.retention_generation().unwrap(), 7);
    assert_eq!(count(&conn, "sessions"), 1);
    assert_eq!(count(&conn, "session_events"), 2);
    assert_eq!(count(&conn, "history"), 1);
    assert_eq!(count(&conn, "retention_pending"), 0);
    assert_eq!(count(&conn, "evidence_tombstones"), 0);
}

#[test]
fn pending_record_cleanup_alone_does_not_invalidate_the_catalog() {
    let (_dir, store, conn) = fixture();
    conn.execute(
        "INSERT INTO retention_pending VALUES('codex','already-drained',?1)",
        [NOW - 40 * DAY],
    )
    .unwrap();
    acknowledge(&store, "cloud");
    let report = run(&store, &RetentionOptions::default(), NOW).unwrap();
    assert_eq!((report.rows, report.sessions), (0, 1));
    assert_eq!(count(&conn, "retention_pending"), 0);
    assert_eq!(store.retention_generation().unwrap(), 0);
}
