use super::*;
use crate::{CatalogQuery, ChangeOp, ChangeQuery};
use std::sync::mpsc;
use std::time::Duration;

const DAY: i64 = 86_400_000;
const NOW: i64 = 1000 * DAY;

fn run(store: &SessionStore, opts: &RetentionOptions, now: i64) -> Result<RetentionReport, Error> {
    let mut opts = opts.clone();
    opts.min_interval_ms = 0;
    maintenance::run(store, &opts, now)
}

fn fixture() -> (tempfile::TempDir, SessionStore, Connection) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("history.db");
    let store = SessionStore::open(StoreOptions::default().db_path(&path)).unwrap();
    let conn = crate::store::open_db(&path).unwrap();
    (dir, store, conn)
}
fn session(conn: &Connection, id: &str, days: i64, rows: usize) {
    let ts = NOW - days * DAY;
    conn.execute("INSERT INTO sessions(source,session_id,last_activity_ms,raw_path,discovery_state,first_prompt) VALUES('codex',?1,?2,'/synthetic','full','private prompt')", params![id,ts]).unwrap();
    for i in 0..rows {
        conn.execute("INSERT INTO session_events(source,session_id,event_uid,role,kind,text,ts_ms) VALUES('codex',?1,?2,'assistant','text','private answer',?3)", params![id,format!("{id}-{i}"),ts]).unwrap();
    }
    conn.execute(
        "INSERT INTO history(source,session_id,prompt,timestamp_ms) VALUES('codex',?1,?1,?2)",
        params![id, ts],
    )
    .unwrap();
}
fn acknowledge(store: &SessionStore, name: &str) {
    store.register_retention_consumer(name).unwrap();
    store
        .acknowledge_retention(name, store.head_revision().unwrap())
        .unwrap();
}
fn drain(store: &SessionStore, opts: RetentionOptions) -> RetentionReport {
    let mut total = RetentionReport::default();
    for _ in 0..1000 {
        let batch = run(store, &opts, NOW).unwrap();
        total.rows += batch.rows;
        total.sessions += batch.sessions;
        total.protected_sessions += batch.protected_sessions;
        if batch.cycle_complete || batch.awaiting_upload_ack {
            return total;
        }
        assert!(!batch.busy);
    }
    panic!("retention cycle did not finish");
}
fn count(conn: &Connection, table: &str) -> i64 {
    conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
        .unwrap()
}

#[test]
fn default_thirty_days_keeps_recent_and_exact_boundary() {
    let (_dir, store, conn) = fixture();
    session(&conn, "old", 31, 3);
    session(&conn, "recent", 29, 2);
    session(&conn, "boundary", 30, 1);
    acknowledge(&store, "desktop-cloud");
    assert_eq!(
        store.retention_policy().unwrap(),
        RetentionPolicy::from_days(30)
    );
    let report = drain(&store, RetentionOptions::default());
    assert_eq!(report.sessions, 1);
    assert_eq!(count(&conn, "sessions"), 2);
    assert_eq!(count(&conn, "session_events"), 3);
    assert_eq!(count(&conn, "history"), 2);
    assert_eq!(count(&conn, "evidence_tombstones"), 0);
    assert_eq!(count(&conn, "session_events_fts"), 3);
}

#[test]
fn changed_policy_is_shared_persistent_and_forever_disables_pruning() {
    let (_dir, store, conn) = fixture();
    session(&conn, "eight-days", 8, 2);
    acknowledge(&store, "cloud");
    write_retention_policy(store.db_path(), RetentionPolicy::from_days(90)).unwrap();
    assert_eq!(drain(&store, RetentionOptions::default()).sessions, 0);
    store
        .set_retention_policy(RetentionPolicy::from_days(0))
        .unwrap();
    assert_eq!(drain(&store, RetentionOptions::default()).rows, 0);
    write_retention_policy(store.db_path(), RetentionPolicy::from_days(7)).unwrap();
    assert_eq!(
        read_retention_policy(store.db_path()).unwrap(),
        RetentionPolicy::from_days(7)
    );
    let readonly = SessionStore::open(
        StoreOptions::default()
            .db_path(store.db_path())
            .read_only(true),
    )
    .unwrap();
    assert_eq!(
        readonly.retention_policy().unwrap(),
        RetentionPolicy::from_days(7)
    );
    assert!(matches!(
        readonly.set_retention_policy(RetentionPolicy::default()),
        Err(Error::UnsupportedOperation(_))
    ));
    assert!(matches!(
        readonly.prune_expired(RetentionOptions::default()),
        Err(Error::UnsupportedOperation(_))
    ));
    assert_eq!(drain(&store, RetentionOptions::default()).sessions, 1);
}

#[test]
fn missing_ack_and_every_lagging_destination_protect_unuploaded_logs() {
    let (_dir, store, conn) = fixture();
    session(&conn, "old", 45, 3);
    assert!(
        run(&store, &RetentionOptions::default(), NOW)
            .unwrap()
            .awaiting_upload_ack
    );
    acknowledge(&store, "cloud-a");
    store.register_retention_consumer("cloud-b").unwrap();
    assert!(
        run(&store, &RetentionOptions::default(), NOW)
            .unwrap()
            .awaiting_upload_ack
    );
    acknowledge(&store, "cloud-b");
    // A later capture invalidates both published acknowledgements for this session.
    conn.execute(
        "UPDATE session_events SET text='undelivered edit' WHERE session_id='old'",
        [],
    )
    .unwrap();
    acknowledge(&store, "cloud-a");
    assert!(drain(&store, RetentionOptions::default()).protected_sessions > 0);
    assert_eq!(count(&conn, "session_events"), 3);
    acknowledge(&store, "cloud-b");
    assert_eq!(drain(&store, RetentionOptions::default()).sessions, 1);
}

#[test]
fn acknowledgement_is_validated_monotonic_and_separate_from_read_cursors() {
    let (_dir, store, conn) = fixture();
    session(&conn, "old", 45, 1);
    let head = store.head_revision().unwrap();
    assert!(store.acknowledge_retention("missing", head).is_err());
    store.register_retention_consumer("cloud").unwrap();
    assert!(store
        .acknowledge_retention(
            "cloud",
            Watermark {
                epoch: head.epoch.wrapping_add(1),
                ..head
            }
        )
        .is_err());
    assert!(store
        .acknowledge_retention(
            "cloud",
            Watermark {
                revision: head.revision + 1,
                ..head
            }
        )
        .is_err());
    store.acknowledge_retention("cloud", head).unwrap();
    store
        .acknowledge_retention(
            "cloud",
            Watermark {
                revision: 0,
                ..head
            },
        )
        .unwrap();
    store.register_retention_consumer("cloud").unwrap();
    let revision: i64 = conn
        .query_row("SELECT revision FROM retention_consumers", [], |r| r.get(0))
        .unwrap();
    assert_eq!(revision as u64, head.revision);
    assert!(store.register_retention_consumer(" ").is_err());
    store.remove_retention_consumer("cloud").unwrap();
    assert!(
        run(&store, &RetentionOptions::default(), NOW)
            .unwrap()
            .awaiting_upload_ack
    );
    // Only an explicit override permits losing unuploaded data.
    let opts = RetentionOptions {
        allow_unuploaded: true,
        ..Default::default()
    };
    assert_eq!(drain(&store, opts).sessions, 1);
}

#[test]
fn large_session_is_split_and_restart_resumes_before_cursor_advances() {
    let (_dir, store, conn) = fixture();
    session(&conn, "big", 40, 25);
    acknowledge(&store, "cloud");
    let opts = RetentionOptions {
        max_rows: 3,
        ..Default::default()
    };
    let first = run(&store, &opts, NOW).unwrap();
    assert_eq!(first.rows, 3);
    assert_eq!(first.sessions, 0);
    assert_eq!(count(&conn, "session_events"), 22);
    let reopened = SessionStore::open(StoreOptions::default().db_path(store.db_path())).unwrap();
    let report = drain(&reopened, opts);
    assert_eq!(report.rows, 23); // 22 events plus prompt
    assert_eq!(report.sessions, 1);
    assert_eq!(count(&conn, "sessions"), 0);
    assert_eq!(count(&conn, "evidence_tombstones"), 0);
}

#[test]
fn eviction_does_not_publish_cloud_deletes_or_remove_real_tombstones() {
    let (_dir, store, conn) = fixture();
    session(&conn, "expired", 40, 3);
    session(&conn, "kept", 1, 1);
    conn.execute("DELETE FROM session_events WHERE session_id='kept'", [])
        .unwrap();
    let tombstone_revision: i64 = conn
        .query_row("SELECT revision FROM evidence_tombstones", [], |r| r.get(0))
        .unwrap();
    acknowledge(&store, "cloud");
    let before = store.head_revision().unwrap();
    drain(&store, RetentionOptions::default());
    let query = ChangeQuery::default();
    let changes = store
        .changes_since(before, query)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(!changes
        .iter()
        .any(|change| matches!(change.op, ChangeOp::Delete)));
    assert_eq!(count(&conn, "evidence_tombstones"), 1);
    assert_eq!(
        conn.query_row("SELECT revision FROM evidence_tombstones", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        tombstone_revision
    );
}

#[test]
fn delegated_logs_and_anonymous_prompts_expire_without_catalog_rows() {
    let (_dir, store, conn) = fixture();
    session(&conn, "child", 40, 2);
    conn.execute("DELETE FROM sessions WHERE session_id='child'", [])
        .unwrap();
    conn.execute(
        "INSERT INTO history(source,prompt,timestamp_ms) VALUES('codex','no session',?1)",
        [NOW - 40 * DAY],
    )
    .unwrap();
    acknowledge(&store, "cloud");
    let report = drain(&store, RetentionOptions::default());
    assert_eq!(report.rows, 4);
    assert_eq!(count(&conn, "session_events"), 0);
    assert_eq!(count(&conn, "history"), 0);
}

#[test]
fn active_evidence_overrides_stale_catalog_activity_and_unknown_age_is_kept() {
    let (_dir, store, conn) = fixture();
    session(&conn, "active", 40, 1);
    conn.execute("UPDATE session_events SET ts_ms=?1", [NOW])
        .unwrap();
    conn.execute(
        "INSERT INTO sessions(source,session_id) VALUES('codex','unknown')",
        [],
    )
    .unwrap();
    acknowledge(&store, "cloud");
    assert_eq!(drain(&store, RetentionOptions::default()).rows, 0);
    assert_eq!(count(&conn, "sessions"), 2);
}

#[test]
fn contended_writer_is_deferred_without_waiting_and_bad_budget_is_refused() {
    let (_dir, store, conn) = fixture();
    session(&conn, "old", 40, 1);
    acknowledge(&store, "cloud");
    let tx = conn.unchecked_transaction().unwrap();
    tx.execute("UPDATE sessions SET title='writer'", [])
        .unwrap();
    assert!(run(&store, &RetentionOptions::default(), NOW).unwrap().busy);
    tx.rollback().unwrap();
    assert!(store
        .prune_expired(RetentionOptions {
            max_rows: 0,
            ..Default::default()
        })
        .is_err());
}

#[test]
fn prune_transaction_does_not_block_local_catalog_reads() {
    let (_dir, store, conn) = fixture();
    session(&conn, "old", 40, 2);
    let path = store.db_path().to_path_buf();
    let (holding_tx, holding_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let writer = std::thread::spawn(move || {
        let mut conn = crate::store::open_db(&path).unwrap();
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let mut report = RetentionReport::default();
        maintenance::evict(&tx, &("codex".into(), "old".into()), 128, &mut report).unwrap();
        holding_tx.send(()).unwrap();
        release_rx.recv().unwrap();
        tx.commit().unwrap();
    });
    holding_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let (read_tx, read_rx) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        let rows = store
            .sessions(CatalogQuery::default())
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        read_tx.send(rows.len()).unwrap();
    });
    let read_while_writer_held = read_rx.recv_timeout(Duration::from_secs(2));
    release_tx.send(()).unwrap();
    writer.join().unwrap();
    reader.join().unwrap();
    assert_eq!(
        read_while_writer_held.unwrap(),
        1,
        "WAL reader sees previous committed snapshot before writer is released"
    );
}

#[test]
fn timestamp_survives_partial_eviction_of_catalogless_null_timestamp_markers() {
    let (_dir, store, conn) = fixture();
    session(&conn, "child", 40, 1);
    conn.execute("DELETE FROM sessions WHERE session_id='child'", [])
        .unwrap();
    for i in 0..10 {
        conn.execute("INSERT INTO session_markers(source,session_id,marker_uid,kind,payload_json) VALUES('codex','child',?1,'unknown','private payload')", [format!("m{i}")]).unwrap();
    }
    acknowledge(&store, "cloud");
    let opts = RetentionOptions {
        max_rows: 1,
        ..Default::default()
    };
    let report = drain(&store, opts);
    assert_eq!(report.rows, 12);
    assert_eq!(count(&conn, "session_markers"), 0);
    assert_eq!(count(&conn, "retention_pending"), 0);
}

#[test]
fn auxiliary_protection_rows_are_batched_too() {
    let (_dir, store, conn) = fixture();
    session(&conn, "old", 40, 1);
    for i in 0..20 {
        conn.execute(
            "INSERT INTO canonical_evidence_protection VALUES('codex','old',?1)",
            [format!("claim{i}")],
        )
        .unwrap();
    }
    acknowledge(&store, "cloud");
    let opts = RetentionOptions {
        max_rows: 3,
        ..Default::default()
    };
    for _ in 0..20 {
        let report = run(&store, &opts, NOW).unwrap();
        assert!(report.rows <= 3);
        if report.cycle_complete {
            break;
        }
    }
    assert_eq!(count(&conn, "canonical_evidence_protection"), 0);
    assert_eq!(count(&conn, "sessions"), 0);
}

#[test]
fn failed_eviction_rolls_back_payloads_progress_and_tombstones() {
    let (_dir, store, conn) = fixture();
    session(&conn, "old", 40, 2);
    acknowledge(&store, "cloud");
    conn.execute_batch("CREATE TRIGGER reject_retention BEFORE DELETE ON history BEGIN SELECT RAISE(ABORT,'test refusal'); END;").unwrap();
    assert!(run(&store, &RetentionOptions::default(), NOW).is_err());
    assert_eq!(count(&conn, "session_events"), 2);
    assert_eq!(count(&conn, "history"), 1);
    assert_eq!(count(&conn, "retention_pending"), 0);
    assert_eq!(count(&conn, "evidence_tombstones"), 0);
}

#[test]
fn old_store_policy_read_defaults_without_migrating() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("old.db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch("CREATE TABLE old_shape(value TEXT);")
        .unwrap();
    assert_eq!(
        read_retention_policy(&path).unwrap(),
        RetentionPolicy::default()
    );
    assert!(!conn
        .prepare("SELECT 1 FROM sqlite_master WHERE name='store_settings'")
        .unwrap()
        .exists([])
        .unwrap());
}

#[test]
fn default_cooldown_prevents_back_to_back_writer_batches() {
    let (_dir, store, conn) = fixture();
    session(&conn, "old", 40, 3);
    acknowledge(&store, "cloud");
    let opts = RetentionOptions {
        max_rows: 1,
        ..Default::default()
    };
    let now = crate::now_ms().max(NOW);
    let first = maintenance::run(&store, &opts, now).unwrap();
    assert_eq!(first.rows, 1);
    let second = maintenance::run(&store, &opts, now).unwrap();
    assert!(second.deferred);
    assert_eq!(second.rows, 0);
    assert_eq!(count(&conn, "session_events"), 2);
    assert_eq!(maintenance::run(&store, &opts, now + 1000).unwrap().rows, 1);
}

mod plans;
mod progress;
mod replay;
