use relayhistory_plugin::delivery::{self, DeliveryJobConfig, ExportSelection};
use rusqlite::Connection;

fn blocked_job() -> (Connection, String) {
    let conn = Connection::open_in_memory().unwrap();
    delivery::init_db(&conn).unwrap();
    let job = delivery::create_job(
        &conn,
        &DeliveryJobConfig {
            destination_id: "fixture".into(),
            instance_id: "probe".into(),
            account_id: "account".into(),
            mapping_version: "1".into(),
            selection: ExportSelection {
                all_sources: true,
                kinds: vec!["session_event".into()],
                ..Default::default()
            },
            limits: Default::default(),
        },
        0,
    )
    .unwrap();
    conn.execute(
        "UPDATE delivery_jobs SET state='blocked',failure='invalid_payload' WHERE id=?",
        [&job.job_id],
    )
    .unwrap();
    (conn, job.job_id)
}

#[test]
fn legacy_marker_is_imported_during_schema_upgrade_without_repeating_retry() {
    let (conn, id) = blocked_job();
    conn.execute_batch("ALTER TABLE delivery_jobs DROP COLUMN retry_build")
        .unwrap();
    delivery::init_db(&conn).unwrap();
    delivery::retry_job_for_build(&conn, &id, "0.26.1", Some("0.26.1")).unwrap();
    assert_eq!(delivery::status(&conn, &id).unwrap().state, "blocked");
    // Losing the file later cannot forget the imported verdict.
    delivery::retry_job_for_build(&conn, &id, "0.26.1", None).unwrap();
    assert_eq!(delivery::status(&conn, &id).unwrap().state, "blocked");
    delivery::retry_job_for_build(&conn, &id, "0.26.2", None).unwrap();
    assert_eq!(delivery::status(&conn, &id).unwrap().state, "active");
}

#[test]
fn build_record_failure_rolls_back_job_and_batch_retry() {
    let (conn, id) = blocked_job();
    conn.execute("INSERT INTO delivery_batches(id,job_id,state,records,bytes,journal_end,created_ms) VALUES('batch',?,'blocked',1,0,0,0)", [&id]).unwrap();
    conn.execute_batch("CREATE TRIGGER reject_retry_build BEFORE UPDATE OF retry_build ON delivery_jobs BEGIN SELECT RAISE(ABORT, 'synthetic write failure'); END;").unwrap();
    assert!(delivery::retry_job_for_build(&conn, &id, "0.26.1", None).is_err());
    let status = delivery::status(&conn, &id).unwrap();
    assert_eq!(status.state, "blocked");
    assert_eq!(status.failure.as_deref(), Some("invalid_payload"));
    let recorded: Option<String> = conn
        .query_row(
            "SELECT retry_build FROM delivery_jobs WHERE id=?",
            [&id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(recorded, None);
    let batch_state: String = conn
        .query_row(
            "SELECT state FROM delivery_batches WHERE id='batch'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(batch_state, "blocked");

    conn.execute_batch("DROP TRIGGER reject_retry_build")
        .unwrap();
    delivery::retry_job_for_build(&conn, &id, "0.26.1", None).unwrap();
    assert_eq!(delivery::status(&conn, &id).unwrap().state, "active");
    let batch_state: String = conn
        .query_row(
            "SELECT state FROM delivery_batches WHERE id='batch'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(batch_state, "pending");
}
