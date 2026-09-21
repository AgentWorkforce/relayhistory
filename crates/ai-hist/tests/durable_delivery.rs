use ai_hist::{delivery::*, init_db, open_db};
use rusqlite::{params, Connection};
use std::collections::{HashMap, HashSet};

fn db() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    init_db(&conn).unwrap();
    conn
}
fn config(instance: &str) -> DeliveryJobConfig {
    DeliveryJobConfig {
        destination_id: "fixture".into(),
        instance_id: instance.into(),
        account_id: "account".into(),
        mapping_version: "1".into(),
        selection: ExportSelection {
            all_sources: true,
            kinds: vec!["session_event".into()],
            ..ExportSelection::default()
        },
        limits: DeliveryLimits {
            max_batch_records: 2,
            max_scan_records: 4,
            ..DeliveryLimits::default()
        },
    }
}
fn event(conn: &Connection, id: &str, text: &str) {
    conn.execute("INSERT INTO session_events(source,session_id,event_uid,ts_ms,role,kind,text) VALUES ('claude',?1,?1,42,'user','text',?2)",params![id,text]).unwrap();
}
fn ack(claim: &ClaimedBatch) -> DeliveryAcknowledgment {
    DeliveryAcknowledgment {
        batch_id: claim.batch.batch_id.clone(),
        accepted_revision_ids: claim
            .batch
            .records
            .iter()
            .map(|r| r.revision_id.clone())
            .collect(),
        unsupported_revision_ids: vec![],
        acceptance_level: AcceptanceLevel::Durable,
    }
}
fn claim(conn: &Connection, id: &str, now: i64) -> Option<ClaimedBatch> {
    for _ in 0..100 {
        let prep = prepare_batch(conn, id, now).unwrap();
        if prep.batch_id.is_some() {
            return claim_batch(conn, id, "worker", 1000, &|| now).unwrap();
        }
        if prep.bootstrap_complete && prep.scanned_records == 0 {
            return None;
        }
    }
    panic!("bounded fixture failed to progress")
}
fn prepare(conn: &Connection, claim: &ClaimedBatch, now: i64) {
    store_prepared_payload(
        conn,
        &claim.lease,
        &claim.batch.mapping_version,
        "application/json",
        &serde_json::to_string(&claim.batch).unwrap(),
        &|| now,
    )
    .unwrap();
}
fn drain(conn: &Connection, id: &str) -> Vec<HistoryExportRecord> {
    let mut records = vec![];
    for step in 0..100 {
        let now = step * 10;
        let Some(claim) = claim(conn, id, now) else {
            return records;
        };
        prepare(conn, &claim, now);
        acknowledge(conn, &claim.lease, &ack(&claim), &|| now).unwrap();
        records.extend(claim.batch.records);
    }
    panic!("drain did not converge")
}

#[test]
fn delivery_is_inert_until_explicitly_enabled_and_schema_reopen_is_idempotent() {
    let conn = db();
    event(&conn, "one", "local");
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM delivery_journal", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0);
    let first = create_job(&conn, &config("one"), 0).unwrap();
    assert_eq!(
        create_job(&conn, &config("one"), 1).unwrap().job_id,
        first.job_id
    );
    init_db(&conn).unwrap();
    assert_eq!(list_jobs(&conn).unwrap().len(), 1);
    let records = drain(&conn, &first.job_id);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].payload["text"], "local");
}

#[test]
fn restart_after_remote_acceptance_preserves_batch_and_prepared_bytes() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("history.db");
    let conn = open_db(&path).unwrap();
    event(&conn, "one", "old");
    let job = create_job(&conn, &config("one"), 0).unwrap();
    let first = claim(&conn, &job.job_id, 0).unwrap();
    prepare(&conn, &first, 0);
    // A receiver with idempotency persists acceptance, then its response is lost.
    let mut receiver = HashSet::new();
    receiver.insert(first.batch.records[0].revision_id.clone());
    conn.execute(
        "UPDATE session_events SET text='new' WHERE session_id='one'",
        [],
    )
    .unwrap();
    drop(conn);
    let conn = open_db(&path).unwrap();
    assert!(claim_batch(&conn, &job.job_id, "other", 1000, &|| 500)
        .unwrap()
        .is_none());
    let retry = claim_batch(&conn, &job.job_id, "other", 1000, &|| 1001)
        .unwrap()
        .unwrap();
    assert_eq!(retry.batch, first.batch);
    assert!(retry.prepared.is_some());
    assert_eq!(
        retry.prepared.as_ref().unwrap().body,
        serde_json::to_string(&first.batch).unwrap()
    );
    assert!(store_prepared_payload(
        &conn,
        &retry.lease,
        "1",
        "application/json",
        "changed bytes",
        &|| 1002
    )
    .is_err());
    assert!(acknowledge(&conn, &first.lease, &ack(&first), &|| 1002).is_err());
    receiver.insert(retry.batch.records[0].revision_id.clone());
    assert_eq!(receiver.len(), 1);
    acknowledge(&conn, &retry.lease, &ack(&retry), &|| 1002).unwrap();
    let changed = drain(&conn, &job.job_id);
    assert_eq!(changed.len(), 1);
    assert_eq!(changed[0].payload["text"], "new");
    assert_eq!(changed[0].record_id, first.batch.records[0].record_id);
    assert!(changed[0].revision > first.batch.records[0].revision);
}

#[test]
fn partial_invalid_and_unsupported_acknowledgments_never_skip_holes() {
    let conn = db();
    event(&conn, "a", "one");
    event(&conn, "b", "two");
    let job = create_job(&conn, &config("one"), 0).unwrap();
    let first = claim(&conn, &job.job_id, 0).unwrap();
    prepare(&conn, &first, 0);
    let mut invalid = ack(&first);
    invalid.accepted_revision_ids.push("unknown".into());
    assert!(acknowledge(&conn, &first.lease, &invalid, &|| 1).is_err());
    let mut partial = ack(&first);
    partial.accepted_revision_ids.pop();
    let pending = acknowledge(&conn, &first.lease, &partial, &|| 1).unwrap();
    assert_eq!(pending.acknowledged_records, 0);
    assert_eq!(pending.pending_records, 2);
    assert!(claim_batch(&conn, &job.job_id, "other", 1000, &|| 2)
        .unwrap()
        .is_none());
    retry_job(&conn, &job.job_id).unwrap();
    let retry = claim_batch(&conn, &job.job_id, "other", 1000, &|| 3)
        .unwrap()
        .unwrap();
    assert_eq!(retry.batch, first.batch);
    let mut unsupported = ack(&retry);
    unsupported
        .unsupported_revision_ids
        .push(unsupported.accepted_revision_ids.pop().unwrap());
    let blocked = acknowledge(&conn, &retry.lease, &unsupported, &|| 4).unwrap();
    assert_eq!(blocked.state, "blocked");
    assert_eq!(blocked.failure.as_deref(), Some("unsupported_evidence"));
    assert_eq!(blocked.acknowledged_cursor, 0);
    retry_job(&conn, &job.job_id).unwrap();
    let retry = claim_batch(&conn, &job.job_id, "other", 1000, &|| 5)
        .unwrap()
        .unwrap();
    acknowledge(&conn, &retry.lease, &ack(&retry), &|| 6).unwrap();
    assert_eq!(status(&conn, &job.job_id).unwrap().acknowledged_records, 2);
}

#[test]
fn two_destinations_bootstrap_independently_while_one_is_offline() {
    let conn = db();
    event(&conn, "a", "initial");
    let left = create_job(&conn, &config("left"), 0).unwrap();
    let right = create_job(&conn, &config("right"), 1).unwrap();
    let failed = claim(&conn, &left.job_id, 2).unwrap();
    prepare(&conn, &failed, 2);
    record_failure(
        &conn,
        &failed.lease,
        DeliveryFailure::RateLimited,
        Some(50_000),
        &|| 3,
    )
    .unwrap();
    assert_eq!(drain(&conn, &right.job_id).len(), 1);
    let left_status = status(&conn, &left.job_id).unwrap();
    assert_eq!(left_status.acknowledged_records, 0);
    assert_eq!(left_status.next_attempt_ms, 50_000);
    event(&conn, "b", "later");
    assert_eq!(drain(&conn, &right.job_id).len(), 1);
    assert_eq!(status(&conn, &left.job_id).unwrap().pending_records, 1);
    assert_eq!(
        status(&conn, &right.job_id).unwrap().acknowledged_records,
        2
    );
}

#[test]
fn bootstrap_retains_original_revisions_across_updates_deletes_and_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("history.db");
    let conn = open_db(&path).unwrap();
    event(&conn, "a", "first");
    event(&conn, "b", "old b");
    event(&conn, "c", "old c");
    let mut settings = config("one");
    settings.limits.max_batch_records = 1;
    let job = create_job(&conn, &settings, 0).unwrap();
    let first = claim(&conn, &job.job_id, 0).unwrap();
    prepare(&conn, &first, 0);
    acknowledge(&conn, &first.lease, &ack(&first), &|| 1).unwrap();
    // Capture preimages for the still-unread portion of the historical snapshot.
    conn.execute(
        "UPDATE session_events SET text='new b' WHERE session_id='b'",
        [],
    )
    .unwrap();
    conn.execute("DELETE FROM session_events WHERE session_id='c'", [])
        .unwrap();
    event(&conn, "d", "new d");
    drop(conn);
    let conn = open_db(&path).unwrap();
    let remaining = drain(&conn, &job.job_id);
    let values: Vec<_> = remaining
        .iter()
        .map(|r| {
            (
                r.session_id.as_deref().unwrap(),
                r.operation.as_str(),
                r.payload["text"].as_str(),
            )
        })
        .collect();
    assert_eq!(
        values,
        [
            ("b", "upsert", Some("old b")),
            ("c", "upsert", Some("old c")),
            ("b", "upsert", Some("new b")),
            ("c", "delete", None),
            ("d", "upsert", Some("new d"))
        ]
    );
}

#[test]
fn changing_logical_identity_emits_old_tombstone_then_new_revision() {
    let conn = db();
    event(&conn, "a", "content");
    let job = create_job(&conn, &config("one"), 0).unwrap();
    let before = drain(&conn, &job.job_id);
    conn.execute(
        "UPDATE session_events SET event_uid='other' WHERE session_id='a'",
        [],
    )
    .unwrap();
    let after = drain(&conn, &job.job_id);
    assert_eq!(after.len(), 2);
    assert_eq!(after[0].operation, "delete");
    assert_eq!(after[0].record_id, before[0].record_id);
    assert_ne!(after[1].record_id, before[0].record_id);
    assert!(after[1].revision > after[0].revision);
}

#[test]
fn queued_exclusion_fences_workers_and_remaps_only_with_a_new_batch_id() {
    let conn = db();
    event(&conn, "a", "keep");
    event(&conn, "b", "private");
    let job = create_job(&conn, &config("one"), 0).unwrap();
    let old = claim(&conn, &job.job_id, 0).unwrap();
    prepare(&conn, &old, 0);
    set_session_excluded(
        &conn,
        &SessionIdentity {
            source: "claude".into(),
            session_id: "b".into(),
        },
        true,
    )
    .unwrap();
    assert!(acknowledge(&conn, &old.lease, &ack(&old), &|| 1).is_err());
    let next = claim_batch(&conn, &job.job_id, "new", 1000, &|| 2)
        .unwrap()
        .unwrap();
    assert_ne!(next.batch.batch_id, old.batch.batch_id);
    assert!(next.prepared.is_none());
    assert_eq!(next.batch.records.len(), 1);
    assert_eq!(next.batch.records[0].session_id.as_deref(), Some("a"));
    assert_eq!(status(&conn, &job.job_id).unwrap().suppressed_records, 1);
    prepare(&conn, &next, 2);
    acknowledge(&conn, &next.lease, &ack(&next), &|| 3).unwrap();
    assert_eq!(status(&conn, &job.job_id).unwrap().acknowledged_records, 1);
}

#[test]
fn pause_retains_capture_and_selection_changes_require_explicit_new_generation() {
    let conn = db();
    event(&conn, "a", "first");
    let job = create_job(&conn, &config("one"), 0).unwrap();
    pause_job(&conn, &job.job_id).unwrap();
    event(&conn, "b", "while paused");
    assert!(claim_batch(&conn, &job.job_id, "worker", 1000, &|| 0)
        .unwrap()
        .is_none());
    let mut changed = config("one");
    changed.mapping_version = "2".into();
    assert!(create_job(&conn, &changed, 1).is_err());
    resume_job(&conn, &job.job_id).unwrap();
    assert_eq!(drain(&conn, &job.job_id).len(), 2);
    cancel_job(&conn, &job.job_id).unwrap();
    let next = create_job(&conn, &changed, 2).unwrap();
    assert_eq!(next.generation, 2);
    assert_ne!(next.job_id, job.job_id);
    assert_eq!(drain(&conn, &next.job_id).len(), 2);
}

#[test]
fn lease_renewal_prevents_overlap_and_stale_failure_cannot_change_progress() {
    let conn = db();
    event(&conn, "a", "first");
    let job = create_job(&conn, &config("one"), 0).unwrap();
    let claim = claim(&conn, &job.job_id, 0).unwrap();
    let renewed = renew_lease(&conn, &claim.lease, 5000, &|| 500).unwrap();
    assert_eq!(renewed.expires_at_ms, 5500);
    assert!(claim_batch(&conn, &job.job_id, "other", 1000, &|| 1001)
        .unwrap()
        .is_none());
    let next = claim_batch(&conn, &job.job_id, "other", 1000, &|| 5501)
        .unwrap()
        .unwrap();
    assert_eq!(next.batch, claim.batch);
    assert!(record_failure(
        &conn,
        &renewed,
        DeliveryFailure::PermissionDenied,
        None,
        &|| 5502
    )
    .is_err());
    assert_eq!(status(&conn, &job.job_id).unwrap().state, "active");
}

#[test]
fn retention_limit_rolls_back_evidence_and_checkpoint_transaction() {
    let mut conn = db();
    let job = create_job(&conn, &config("one"), 0).unwrap();
    let used = retained_bytes(&conn).unwrap().0;
    set_retention_limit(&conn, used + 100).unwrap();
    let tx = conn.transaction().unwrap();
    let result=tx.execute("INSERT INTO session_events(source,session_id,event_uid,ts_ms,role,kind,text) VALUES ('claude','a','a',1,'user','text','must retain')",[]);
    assert!(result.unwrap_err().to_string().contains("retention limit"));
    tx.rollback().unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM session_events", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0);
    assert_eq!(retained_bytes(&conn).unwrap().0, used);
    set_retention_limit(&conn, 1_000_000).unwrap();
    event(&conn, "a", "retained");
    assert_eq!(drain(&conn, &job.job_id).len(), 1);
}

#[test]
fn byte_limit_failure_keeps_progress_and_compaction_respects_lagging_destination() {
    let conn = db();
    event(&conn, "a", &"x".repeat(2000));
    let mut tiny = config("tiny");
    tiny.limits.max_batch_bytes = 512;
    let bad = create_job(&conn, &tiny, 0).unwrap();
    assert!(prepare_batch(&conn, &bad.job_id, 1).is_err());
    assert!(!status(&conn, &bad.job_id).unwrap().bootstrap_complete);
    cancel_job(&conn, &bad.job_id).unwrap();
    let left = create_job(&conn, &config("left"), 2).unwrap();
    let right = create_job(&conn, &config("right"), 3).unwrap();
    event(&conn, "b", "new");
    drain(&conn, &left.job_id);
    compact_journal(&conn, 100).unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM delivery_journal WHERE kind='session_event'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
    drain(&conn, &right.job_id);
    assert!(compact_journal(&conn, 100).unwrap() > 0);
}

#[test]
fn all_supported_evidence_kinds_preserve_raw_fields_and_canonical_keys() {
    let conn = db();
    conn.execute_batch(r#"
INSERT INTO history(source,session_id,timestamp_ms,prompt,git_branch) VALUES('codex','s',1,'prompt','main');
INSERT INTO sessions(source,session_id,first_prompt) VALUES('codex','s','prompt');
INSERT INTO session_presences(source,session_id,location,raw_locator) VALUES('codex','s','remote','fixture://s');
INSERT INTO tool_calls(source,session_id,tool_use_id,name,args_json) VALUES('codex','s','tool','Edit','{ "raw": 1 }');
INSERT INTO file_edits(source,session_id,tool_use_id,file_path,tool_name,structured_patch_json) VALUES('codex','s','tool','src/lib.rs','Edit','[ 1 ]');
INSERT INTO session_relationships(source,parent_session_id,relationship_uid,child_session_id,relationship,identity_status,evidence_kind,created_ms,updated_ms) VALUES('codex','s','rel','child','delegation','observed','fixture',1,1);
INSERT INTO session_commit_links(source,session_id,repo,commit_sha,match_method,confidence,created_at_ms) VALUES('codex','s','repo','sha','cwd',1,1);
INSERT INTO trajectories(id,decisions_json,retrospective_json,search_text,updated_ms,timestamp_ms) VALUES('traj','[]','{}','',1,1);
INSERT INTO session_observations(source,session_id,location,connector_id,connector_instance,raw_locator,source_stamp,updated_ms) VALUES('codex','s','remote','fixture','account','fixture://s','v1',1);
INSERT INTO observation_evidence(source,session_id,location,connector_id,connector_instance,evidence_uid,payload_json) VALUES('codex','s','remote','fixture','account','record','{ "events": [] }');
INSERT INTO session_markers(source,session_id,marker_uid,kind,subkind,ts_ms,text,payload_json) VALUES('codex','s','c0','compaction_boundary','compacted',1,'compacted','{ "checkpoint": 1 }');
"#).unwrap();
    event(&conn, "a", "event");
    let mut cfg = config("all");
    cfg.selection.kinds = SUPPORTED_KINDS.iter().map(|s| (*s).into()).collect();
    let job = create_job(&conn, &cfg, 0).unwrap();
    let records = drain(&conn, &job.job_id);
    let by_kind: HashMap<_, _> = records.iter().map(|r| (r.kind.as_str(), r)).collect();
    assert_eq!(by_kind.len(), SUPPORTED_KINDS.len());
    assert_eq!(by_kind["tool_call"].payload["args_json"], "{ \"raw\": 1 }");
    assert_eq!(
        by_kind["file_edit"].payload["structured_patch_json"],
        "[ 1 ]"
    );
    assert_eq!(by_kind["presence"].payload["location"], "remote");
    assert_eq!(
        by_kind["source_observation"].payload["connector_instance"],
        "account"
    );
    assert_eq!(
        by_kind["observation_evidence"].payload["payload_json"],
        r#"{ "events": [] }"#
    );
    assert_eq!(by_kind["history"].payload["git_branch"], "main");
    assert_eq!(
        by_kind["session_marker"].payload["kind"],
        "compaction_boundary"
    );
    assert_eq!(
        by_kind["session_marker"].payload["payload_json"],
        r#"{ "checkpoint": 1 }"#
    );
    assert!(records
        .iter()
        .all(|r| r.schema_version == 1 && !r.origin_id.is_empty()));
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
#[test]
fn file_export_is_a_consistent_resumable_snapshot_without_destination_jobs() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("history.db");
    let conn = open_db(&path).unwrap();
    event(&conn, "a", "first");
    event(&conn, "b", "before");
    let mut cfg = config("unused");
    cfg.limits.max_batch_records = 1;
    let now = now_ms();
    let handle = create_export(&conn, &cfg.selection, &cfg.limits, 60_000, now).unwrap();
    assert!(list_jobs(&conn).unwrap().is_empty());
    let page = export_page(&conn, &handle.cursor, now).unwrap();
    assert_eq!(page.records[0].payload["text"], "first");
    conn.execute(
        "UPDATE session_events SET text='after' WHERE session_id='b'",
        [],
    )
    .unwrap();
    event(&conn, "c", "new");
    drop(conn);
    let conn = open_db(&path).unwrap();
    assert_eq!(export_page(&conn, &handle.cursor, now + 1).unwrap(), page);
    let second = export_page(&conn, page.next_cursor.as_ref().unwrap(), now + 1).unwrap();
    assert_eq!(second.records[0].payload["text"], "before");
    let last = export_page(&conn, second.next_cursor.as_ref().unwrap(), now + 1).unwrap();
    assert!(last.records.is_empty());
    assert!(last.next_cursor.is_none());
    assert!(list_jobs(&conn).unwrap().is_empty());
    let used = retained_bytes(&conn).unwrap().0;
    close_export(&conn, &handle.snapshot_id).unwrap();
    assert!(retained_bytes(&conn).unwrap().0 < used);
    assert!(export_page(&conn, &handle.cursor, now + 2).is_err());
}

#[test]
fn expired_export_refuses_pages_and_releases_retained_payloads_on_cleanup() {
    let conn = db();
    event(&conn, "a", "local");
    let cfg = config("unused");
    let now = now_ms();
    let handle = create_export(&conn, &cfg.selection, &cfg.limits, 10, now).unwrap();
    export_page(&conn, &handle.cursor, now).unwrap();
    assert!(export_page(&conn, &handle.cursor, now + 10).is_err());
    assert_eq!(expire_exports(&conn, now + 10, 32).unwrap(), 1);
    assert!(list_jobs(&conn).unwrap().is_empty());
}

#[test]
fn replacement_and_reused_rowids_preserve_snapshot_identity() {
    let conn = db();
    conn.execute(
        "INSERT INTO sessions(source,session_id,first_prompt) VALUES ('claude','old','before')",
        [],
    )
    .unwrap();
    let mut cfg = config("one");
    cfg.selection.kinds = vec!["session".into()];
    let job = create_job(&conn, &cfg, 0).unwrap();
    conn.execute("INSERT OR REPLACE INTO sessions(source,session_id,first_prompt) VALUES ('claude','old','replacement')",[]).unwrap();
    conn.execute("DELETE FROM sessions WHERE session_id='old'", [])
        .unwrap();
    conn.execute("INSERT INTO sessions(source,session_id,first_prompt) VALUES ('claude','new','new identity')",[]).unwrap();
    let records = drain(&conn, &job.job_id);
    assert_eq!(records[0].session_id.as_deref(), Some("old"));
    assert_eq!(records[0].payload["first_prompt"], "before");
    let identities: Vec<_> = records
        .iter()
        .map(|r| (r.session_id.as_deref().unwrap(), r.operation.as_str()))
        .collect();
    assert_eq!(
        identities,
        [
            ("old", "upsert"),
            ("old", "delete"),
            ("old", "upsert"),
            ("old", "delete"),
            ("new", "upsert")
        ]
    );
}

#[test]
fn a_new_capture_epoch_never_reuses_revision_id_for_changed_payload() {
    let conn = db();
    event(&conn, "a", "one");
    let first = create_job(&conn, &config("one"), 0).unwrap();
    let before = drain(&conn, &first.job_id);
    cancel_job(&conn, &first.job_id).unwrap();
    compact_journal(&conn, 100).unwrap();
    conn.execute(
        "UPDATE session_events SET text='two' WHERE session_id='a'",
        [],
    )
    .unwrap();
    let second = create_job(&conn, &config("one"), 1).unwrap();
    let after = drain(&conn, &second.job_id);
    assert_eq!(before[0].record_id, after[0].record_id);
    assert_ne!(before[0].revision_id, after[0].revision_id);
    assert!(after[0].revision > before[0].revision);
}

#[test]
fn dispatch_requires_immutable_payload_and_rechecks_privacy_after_mapping() {
    let conn = db();
    event(&conn, "a", "private");
    let job = create_job(&conn, &config("one"), 0).unwrap();
    let claim = claim(&conn, &job.job_id, 0).unwrap();
    assert!(validate_dispatch(&conn, &claim.lease, &|| 1).is_err());
    prepare(&conn, &claim, 1);
    assert!(validate_dispatch(&conn, &claim.lease, &|| 2).is_ok());
    set_session_excluded(
        &conn,
        &SessionIdentity {
            source: "claude".into(),
            session_id: "a".into(),
        },
        true,
    )
    .unwrap();
    assert!(validate_dispatch(&conn, &claim.lease, &|| 3).is_err());
    assert!(claim_batch(&conn, &job.job_id, "other", 1000, &|| 3)
        .unwrap()
        .is_none());
    assert_eq!(status(&conn, &job.job_id).unwrap().suppressed_records, 1);
    assert_eq!(status(&conn, &job.job_id).unwrap().acknowledged_records, 0);
}

#[test]
fn a_missing_capture_trigger_is_repaired_before_more_ingestion() {
    let conn = db();
    conn.execute_batch("DROP TRIGGER delivery_session_events_update")
        .unwrap();
    assert!(!ai_hist::schema_is_current(&conn).unwrap());
    init_db(&conn).unwrap();
    assert!(ai_hist::schema_is_current(&conn).unwrap());
    event(&conn, "a", "before");
    let job = create_job(&conn, &config("one"), 0).unwrap();
    drain(&conn, &job.job_id);
    conn.execute("UPDATE session_events SET text='after'", [])
        .unwrap();
    assert_eq!(drain(&conn, &job.job_id)[0].payload["text"], "after");
}

#[test]
fn excluded_child_relationship_metadata_is_not_exported_through_its_parent() {
    let conn = db();
    conn.execute("INSERT INTO session_relationships(source,parent_session_id,relationship_uid,child_session_id,relationship,identity_status,evidence_kind,created_ms,updated_ms) VALUES('codex','parent','edge','secret','delegation','observed','fixture',1,1)",[]).unwrap();
    let mut cfg = config("one");
    cfg.selection.kinds = vec!["relationship".into()];
    let job = create_job(&conn, &cfg, 0).unwrap();
    let old = claim(&conn, &job.job_id, 0).unwrap();
    prepare(&conn, &old, 0);
    set_session_excluded(
        &conn,
        &SessionIdentity {
            source: "codex".into(),
            session_id: "secret".into(),
        },
        true,
    )
    .unwrap();
    assert!(claim_batch(&conn, &job.job_id, "next", 1000, &|| 1)
        .unwrap()
        .is_none());
    assert_eq!(status(&conn, &job.job_id).unwrap().suppressed_records, 1);
}

#[test]
fn a_full_journal_requires_explicit_headroom_and_failed_queueing_loses_nothing() {
    let conn = db();
    let job = create_job(&conn, &config("one"), 0).unwrap();
    event(&conn, "a", "retained revision");
    let used = retained_bytes(&conn).unwrap().0;
    set_retention_limit(&conn, used).unwrap();
    let before = status(&conn, &job.job_id).unwrap();
    let error = prepare_batch(&conn, &job.job_id, 1).unwrap_err();
    assert!(error.to_string().contains("raise the retention cap"));
    assert!(is_retention_limit(&error));
    let after = status(&conn, &job.job_id).unwrap();
    assert_eq!(after.journal_cursor, before.journal_cursor);
    assert_eq!(after.bootstrap_complete, before.bootstrap_complete);
    assert_eq!(after.pending_records, 0);
    assert_eq!(retained_bytes(&conn).unwrap().0, used);
    set_retention_limit(&conn, used + 20_000).unwrap();
    let records = drain(&conn, &job.job_id);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].payload["text"], "retained revision");
    compact_journal(&conn, 100).unwrap();
    compact_receipts(&conn, 100).unwrap();
    assert_eq!(retained_bytes(&conn).unwrap().0, 0);
    assert_eq!(status(&conn, &job.job_id).unwrap().acknowledged_records, 1);
}

#[test]
fn terminal_receipts_can_be_compacted_without_accepting_a_stale_ack() {
    let conn = db();
    event(&conn, "a", "first");
    let job = create_job(&conn, &config("one"), 0).unwrap();
    let claim = claim(&conn, &job.job_id, 0).unwrap();
    prepare(&conn, &claim, 0);
    acknowledge(&conn, &claim.lease, &ack(&claim), &|| 1).unwrap();
    assert_eq!(compact_receipts(&conn, 1).unwrap(), 1);
    assert!(acknowledge(&conn, &claim.lease, &ack(&claim), &|| 2).is_err());
    assert_eq!(status(&conn, &job.job_id).unwrap().acknowledged_records, 1);
}

#[test]
fn permanent_failure_cannot_resume_through_pause_without_explicit_retry() {
    for failure in [
        DeliveryFailure::AuthenticationRequired,
        DeliveryFailure::PermissionDenied,
        DeliveryFailure::InvalidPayload,
        DeliveryFailure::UnsupportedEvidence,
        DeliveryFailure::MappingVersionMismatch,
    ] {
        let conn = db();
        event(&conn, "one", "pending");
        let job = create_job(&conn, &config("blocked"), 0).unwrap();
        let first = claim(&conn, &job.job_id, 0).unwrap();
        prepare(&conn, &first, 0);
        let expected_prepared = validate_dispatch(&conn, &first.lease, &|| 0).unwrap();
        let blocked = record_failure(&conn, &first.lease, failure, None, &|| 1).unwrap();
        assert_eq!(blocked.state, "blocked");
        assert!(pause_job(&conn, &job.job_id).is_err());
        assert!(resume_job(&conn, &job.job_id).is_err());
        assert_eq!(status(&conn, &job.job_id).unwrap(), blocked);
        assert!(claim_batch(&conn, &job.job_id, "other", 1000, &|| 100_000)
            .unwrap()
            .is_none());
        // A paused row written by the previous buggy version also remains blocked.
        conn.execute(
            "UPDATE delivery_jobs SET state='paused' WHERE id=?",
            [&job.job_id],
        )
        .unwrap();
        assert!(resume_job(&conn, &job.job_id).is_err());
        assert!(pause_job(&conn, &job.job_id).is_err());
        assert_eq!(status(&conn, &job.job_id).unwrap().failure, blocked.failure);
        retry_job(&conn, &job.job_id).unwrap();
        let next = claim_batch(&conn, &job.job_id, "other", 1000, &|| 2)
            .unwrap()
            .unwrap();
        assert_eq!(next.batch, first.batch);
        assert_eq!(next.prepared, Some(expected_prepared));
        acknowledge(&conn, &next.lease, &ack(&next), &|| 3).unwrap();
    }
}

#[test]
fn resume_checks_state_after_acquiring_the_write_transaction() {
    use std::sync::atomic::{AtomicBool, Ordering};
    static WAITING_FOR_WRITE: AtomicBool = AtomicBool::new(false);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("resume.db");
    let writer = open_db(&path).unwrap();
    let reader = open_db(&path).unwrap();
    let job = create_job(&writer, &config("race"), 0).unwrap();
    pause_job(&writer, &job.job_id).unwrap();
    reader
        .busy_handler(Some(|_| {
            WAITING_FOR_WRITE.store(true, Ordering::SeqCst);
            std::thread::sleep(std::time::Duration::from_millis(1));
            true
        }))
        .unwrap();
    let tx = writer.unchecked_transaction().unwrap();
    tx.execute(
        "UPDATE delivery_jobs SET state='blocked',failure='permission_denied' WHERE id=?",
        [&job.job_id],
    )
    .unwrap();
    let id = job.job_id.clone();
    let resumer = std::thread::spawn(move || resume_job(&reader, &id));
    let started = std::time::Instant::now();
    while !WAITING_FOR_WRITE.load(Ordering::SeqCst) {
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
        std::thread::yield_now();
    }
    tx.commit().unwrap();
    assert!(resumer.join().unwrap().is_err());
    assert_eq!(status(&writer, &job.job_id).unwrap().state, "blocked");
}

#[test]
fn observation_revisions_keep_instance_identity_and_deletions() {
    let conn = db();
    let mut cfg = config("observations");
    cfg.selection.kinds = vec!["source_observation".into(), "observation_evidence".into()];
    let job = create_job(&conn, &cfg, 0).unwrap();
    drain(&conn, &job.job_id);
    for account in ["first", "second"] {
        conn.execute("INSERT INTO session_observations(source,session_id,location,connector_id,connector_instance,raw_locator,updated_ms) VALUES('claude','session','remote','fixture',?,'fixture://session',1)",[account]).unwrap();
        conn.execute("INSERT INTO observation_evidence(source,session_id,location,connector_id,connector_instance,evidence_uid,payload_json) VALUES('claude','session','remote','fixture',?,'record','{\"v\":1}')",[account]).unwrap();
    }
    conn.execute(
        "UPDATE session_observations SET source_stamp='v2' WHERE connector_instance='first'",
        [],
    )
    .unwrap();
    conn.execute(
        "UPDATE observation_evidence SET payload_json='{\"v\":2}' WHERE connector_instance='first'",
        [],
    )
    .unwrap();
    conn.execute(
        "DELETE FROM observation_evidence WHERE connector_instance='second'",
        [],
    )
    .unwrap();
    conn.execute(
        "DELETE FROM session_observations WHERE connector_instance='second'",
        [],
    )
    .unwrap();
    let rows = drain(&conn, &job.job_id);
    assert_eq!(rows.len(), 8);
    let observation_rows = rows
        .iter()
        .filter(|row| row.kind == "source_observation")
        .collect::<Vec<_>>();
    assert_eq!(observation_rows.len(), 4);
    assert_ne!(observation_rows[0].record_id, observation_rows[1].record_id);
    assert_eq!(observation_rows[0].record_id, observation_rows[2].record_id);
    assert_ne!(
        observation_rows[0].revision_id,
        observation_rows[2].revision_id
    );
    assert_eq!(observation_rows[3].operation, "delete");
    assert!(rows
        .iter()
        .any(|row| row.kind == "observation_evidence" && row.operation == "delete"));
}

#[test]
fn old_delivery_generations_get_empty_bounds_for_new_observation_kinds() {
    let conn = db();
    let cfg = config("old");
    let job = create_job(&conn, &cfg, 0).unwrap();
    // Simulate a generation created before these kinds existed.
    conn.execute("DELETE FROM delivery_bootstrap_bounds WHERE kind IN ('source_observation','observation_evidence')",[]).unwrap();
    conn.execute_batch("DROP TRIGGER delivery_session_observations_insert;")
        .unwrap();
    init_db(&conn).unwrap();
    let bounds:i64=conn.query_row("SELECT COUNT(*) FROM delivery_bootstrap_bounds WHERE job_id=? AND kind IN ('source_observation','observation_evidence') AND max_rowid=0",[&job.job_id],|row|row.get(0)).unwrap();
    assert_eq!(bounds, 2);
    assert!(drain(&conn, &job.job_id).is_empty());
    conn.execute("INSERT INTO session_observations(source,session_id,location,connector_id,connector_instance,updated_ms) VALUES('claude','new','remote','fixture','account',1)",[]).unwrap();
    // Frozen old selections do not silently gain a new exported evidence kind.
    assert!(drain(&conn, &job.job_id).is_empty());
    let mut next = config("new-generation");
    next.selection.kinds = vec!["source_observation".into()];
    let next = create_job(&conn, &next, 1).unwrap();
    assert_eq!(drain(&conn, &next.job_id).len(), 1);
}

#[test]
fn clearing_consumed_exclusions_requires_a_new_baseline_generation() {
    for queued in [false, true] {
        let conn = db();
        let session = SessionIdentity {
            source: "claude".into(),
            session_id: "private".into(),
        };
        event(&conn, "private", "historical baseline");
        let cfg = config("one");
        let job = create_job(&conn, &cfg, 0).unwrap();
        if queued {
            assert!(claim(&conn, &job.job_id, 0).is_some());
        }
        set_session_excluded(&conn, &session, true).unwrap();
        assert!(drain(&conn, &job.job_id).is_empty());
        let error = set_session_excluded(&conn, &session, false).unwrap_err();
        assert!(error.to_string().contains("DELIVERY_GENERATION_REQUIRED"));
        assert!(drain(&conn, &job.job_id).is_empty());
        cancel_job(&conn, &job.job_id).unwrap();
        set_session_excluded(&conn, &session, false).unwrap();
        let next = create_job(&conn, &cfg, 1).unwrap();
        let records = drain(&conn, &next.job_id);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].payload["text"], "historical baseline");
        assert!(next.generation > job.generation);
    }
}

#[test]
fn clearing_exclusions_does_not_require_cancelling_unaffected_jobs() {
    let conn = db();
    let session = SessionIdentity {
        source: "claude".into(),
        session_id: "private".into(),
    };
    set_session_excluded(&conn, &session, true).unwrap();
    let mut cfg = config("other");
    cfg.selection.all_sources = false;
    cfg.selection.sessions = vec![SessionIdentity {
        source: "claude".into(),
        session_id: "other".into(),
    }];
    create_job(&conn, &cfg, 0).unwrap();
    let mut explicit = config("permanent");
    explicit.selection.excluded_sessions.push(session.clone());
    create_job(&conn, &explicit, 0).unwrap();
    set_session_excluded(&conn, &session, false).unwrap();
    set_session_excluded(&conn, &session, false).unwrap();
}

#[test]
fn scoped_membership_backfills_only_changes_and_preserves_pending_batches() {
    let conn = db();
    event(&conn, "a", "before");
    event(&conn, "b", "second");
    let root = create_session_job(&conn, &config("scoped"), 0).unwrap();
    let a = SessionIdentity {
        source: "claude".into(),
        session_id: "a".into(),
    };
    let b = SessionIdentity {
        source: "claude".into(),
        session_id: "b".into(),
    };
    assert!(claim(&conn, &root.job_id, 1).is_none());
    assert!(set_job_session(&conn, &root.job_id, &a, true).unwrap());
    assert!(!set_job_session(&conn, &root.job_id, &a, true).unwrap());
    let first = claim(&conn, &root.job_id, 2).unwrap();
    prepare(&conn, &first, 2);
    assert!(set_job_session(&conn, &root.job_id, &b, true).unwrap());
    assert!(validate_dispatch(&conn, &first.lease, &|| 3).is_err());
    let reclaimed = claim_batch(&conn, &root.job_id, "worker", 1000, &|| 3)
        .unwrap()
        .unwrap();
    assert_eq!(first.batch.batch_id, reclaimed.batch.batch_id);
    acknowledge(&conn, &reclaimed.lease, &ack(&reclaimed), &|| 4).unwrap();
    let records = drain(&conn, &root.job_id);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].session_id.as_deref(), Some("b"));
    assert!(set_job_session(&conn, &root.job_id, &a, false).unwrap());
    event(&conn, "private", "must not upload");
    assert!(set_job_session(&conn, &root.job_id, &a, true).unwrap());
    let records = drain(&conn, &root.job_id);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].session_id.as_deref(), Some("a"));
}

#[test]
fn scoped_snapshot_keeps_preimages_deletions_and_reassigned_identities() {
    let conn = db();
    event(&conn, "a", "before");
    event(&conn, "outside", "private");
    let root = create_session_job(&conn, &config("snapshot-member"), 0).unwrap();
    set_job_session(
        &conn,
        &root.job_id,
        &SessionIdentity {
            source: "claude".into(),
            session_id: "a".into(),
        },
        true,
    )
    .unwrap();
    conn.execute(
        "UPDATE session_events SET session_id='elsewhere',text='after' WHERE session_id='a'",
        [],
    )
    .unwrap();
    conn.execute(
        "UPDATE session_events SET session_id='a' WHERE session_id='outside'",
        [],
    )
    .unwrap();
    let records = drain(&conn, &root.job_id);
    assert!(records.iter().any(|r| r.payload["text"] == "before"));
    assert!(!records.iter().any(|r| r.payload["text"] == "after"));
    assert_eq!(
        records
            .iter()
            .filter(|r| r.payload["text"] == "private")
            .count(),
        1
    );
}

#[test]
fn scoped_backfill_has_constant_unrelated_record_visits() {
    for count in [100, 10_000, 50_000] {
        for early in [true, false] {
            let conn = db();
            if early {
                event(&conn, "selected", "fixed");
            }
            conn.execute_batch("BEGIN").unwrap();
            for n in 0..count {
                event(&conn, &format!("unrelated-{n}"), "unrelated");
            }
            if !early {
                event(&conn, "selected", "fixed");
            }
            conn.execute_batch("COMMIT").unwrap();
            let root = create_session_job(&conn, &config("scale"), 0).unwrap();
            let steps = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let counter = steps.clone();
            conn.progress_handler(
                1,
                Some(move || {
                    counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    false
                }),
            );
            let started = std::time::Instant::now();
            let before = conn.total_changes();
            set_job_session(
                &conn,
                &root.job_id,
                &SessionIdentity {
                    source: "claude".into(),
                    session_id: "selected".into(),
                },
                true,
            )
            .unwrap();
            let select_ms = started.elapsed().as_secs_f64() * 1000.;
            let writes = conn.total_changes() - before;
            let selected_steps = steps.swap(0, std::sync::atomic::Ordering::Relaxed);
            let started = std::time::Instant::now();
            let prep = prepare_batch(&conn, &root.job_id, 1).unwrap();
            let prepare_ms = started.elapsed().as_secs_f64() * 1000.;
            let prepare_steps = steps.load(std::sync::atomic::Ordering::Relaxed);
            conn.progress_handler(0, None::<fn() -> bool>);
            eprintln!("scoped count={count} early={early} select_ms={select_ms:.3} prepare_ms={prepare_ms:.3} writes={writes} select_vm={selected_steps} prepare_vm={prepare_steps} visits={}",prep.scanned_records);
            assert_eq!(prep.scanned_records, 1);
            assert!(prep.batch_id.is_some());
            assert!(selected_steps < 10_000);
            assert!(prepare_steps < 15_000);
            assert!(writes < 30);
            let claimed = claim_batch(&conn, &root.job_id, "fake", 1000, &|| 2)
                .unwrap()
                .unwrap();
            prepare(&conn, &claimed, 2);
            validate_dispatch(&conn, &claimed.lease, &|| 2).unwrap();
            acknowledge(&conn, &claimed.lease, &ack(&claimed), &|| 3).unwrap();
            assert_eq!(status(&conn, &root.job_id).unwrap().acknowledged_records, 1);
        }
    }
}

#[test]
fn scoped_adoption_preserves_deleted_records_pending_batches_and_pause() {
    let conn = db();
    event(&conn, "a", "snapshot");
    event(&conn, "private", "hidden");
    let old = create_job(&conn, &config("adopt"), 0).unwrap();
    conn.execute("DELETE FROM session_events WHERE session_id='a'", [])
        .unwrap();
    let original = claim(&conn, &old.job_id, 1).unwrap();
    prepare(&conn, &original, 1);
    pause_job(&conn, &old.job_id).unwrap();
    let a = SessionIdentity {
        source: "claude".into(),
        session_id: "a".into(),
    };
    adopt_session_job(&conn, &old.job_id, &[a.clone(), a.clone()]).unwrap();
    assert_eq!(
        conn.query_row(
            "SELECT COUNT(DISTINCT job_id) FROM delivery_bootstrap_bounds",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        2
    );
    adopt_session_job(&conn, &old.job_id, &[]).unwrap();
    assert_eq!(status(&conn, &old.job_id).unwrap().state, "paused");
    assert_eq!(job_sessions(&conn, &old.job_id).unwrap(), vec![a]);
    resume_job(&conn, &old.job_id).unwrap();
    let records = drain(&conn, &old.job_id);
    assert!(records.iter().all(|r| r.session_id.as_deref() == Some("a")));
    assert!(records.iter().any(|r| r.payload["text"] == "snapshot"));
    assert!(records.iter().any(|r| r.operation == "delete"));
}

#[test]
fn scoped_adoption_preserves_ownership_preimage_and_global_exclusion_guard() {
    let conn = db();
    event(&conn, "outside", "private");
    let old = create_job(&conn, &config("adopt-owner"), 0).unwrap();
    conn.execute(
        "UPDATE session_events SET session_id='a' WHERE session_id='outside'",
        [],
    )
    .unwrap();
    let a = SessionIdentity {
        source: "claude".into(),
        session_id: "a".into(),
    };
    adopt_session_job(&conn, &old.job_id, std::slice::from_ref(&a)).unwrap();
    let records = drain(&conn, &old.job_id);
    assert_eq!(records.len(), 1);
    assert!(records[0].revision > old.journal_cursor);
    set_session_excluded(&conn, &a, true).unwrap();
    assert!(set_session_excluded(&conn, &a, false).is_err());
    set_job_session(&conn, &old.job_id, &a, false).unwrap();
    let other = create_job(&conn, &config("other-account"), 2).unwrap();
    assert!(set_session_excluded(&conn, &a, false).is_err());
    cancel_job(&conn, &other.job_id).unwrap();
    set_session_excluded(&conn, &a, false).unwrap();
    set_job_session(&conn, &old.job_id, &a, true).unwrap();
    assert_eq!(drain(&conn, &old.job_id).len(), 1);
}

#[test]
fn scoped_exclusion_invalidates_prepared_and_leased_reinclude_has_fresh_revision() {
    let conn = db();
    event(&conn, "a", "same");
    let root = create_session_job(&conn, &config("private"), 0).unwrap();
    let a = SessionIdentity {
        source: "claude".into(),
        session_id: "a".into(),
    };
    set_job_session(&conn, &root.job_id, &a, true).unwrap();
    let before = claim(&conn, &root.job_id, 1).unwrap();
    prepare(&conn, &before, 1);
    set_job_session(&conn, &root.job_id, &a, false).unwrap();
    assert!(validate_dispatch(&conn, &before.lease, &|| 2).is_err());
    assert!(acknowledge(&conn, &before.lease, &ack(&before), &|| 2).is_err());
    set_job_session(&conn, &root.job_id, &a, true).unwrap();
    assert!(claim_batch(&conn, &root.job_id, "filter", 1000, &|| 2)
        .unwrap()
        .is_none());
    let records = drain(&conn, &root.job_id);
    assert_eq!(records.len(), 1);
    assert!(records[0].revision > before.batch.records[0].revision);
}

#[test]
fn scoped_members_are_not_config_json_or_active_job_count_and_unrelated_preimages_stay_empty() {
    let conn = db();
    event(&conn, "outside", "before");
    let root = create_session_job(&conn, &config("many"), 0).unwrap();
    for n in 0..400 {
        set_job_session(
            &conn,
            &root.job_id,
            &SessionIdentity {
                source: "claude".into(),
                session_id: format!("{n}-{}", "x".repeat(180)),
            },
            true,
        )
        .unwrap();
    }
    assert_eq!(job_sessions(&conn, &root.job_id).unwrap().len(), 400);
    assert!(
        serde_json::to_vec(&status(&conn, &root.job_id).unwrap().config)
            .unwrap()
            .len()
            < 1000
    );
    conn.execute(
        "UPDATE session_events SET text='after' WHERE session_id='outside'",
        [],
    )
    .unwrap();
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM delivery_shadow", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(list_jobs(&conn).unwrap().len(), 1);
}

#[test]
fn scoped_child_inclusion_restores_relationship_without_replaying_parent_records() {
    let conn = db();
    event(&conn, "parent", "parent-record");
    conn.execute("INSERT INTO session_relationships(source,parent_session_id,relationship_uid,child_session_id,relationship,identity_status,evidence_kind,created_ms,updated_ms) VALUES('claude','parent','edge','child','delegation','observed','fixture',1,1)",[]).unwrap();
    let mut cfg = config("relations");
    cfg.selection.kinds.push("relationship".into());
    let root = create_session_job(&conn, &cfg, 0).unwrap();
    set_job_session(
        &conn,
        &root.job_id,
        &SessionIdentity {
            source: "claude".into(),
            session_id: "parent".into(),
        },
        true,
    )
    .unwrap();
    let records = drain(&conn, &root.job_id);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].kind, "session_event");
    set_job_session(
        &conn,
        &root.job_id,
        &SessionIdentity {
            source: "claude".into(),
            session_id: "child".into(),
        },
        true,
    )
    .unwrap();
    let records = drain(&conn, &root.job_id);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].kind, "relationship");
}
