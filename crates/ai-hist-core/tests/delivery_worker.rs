//! The generic drain loop: one worker, receiver-agnostic, ported from the
//! behaviours the TypeScript host tests already pin down.

use ai_hist_core::delivery::worker::*;
use ai_hist_core::delivery::*;
use ai_hist_core::open_db;
use rusqlite::{params, Connection};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

struct Fixture {
    directory: tempfile::TempDir,
    conn: Connection,
}
impl Fixture {
    fn path(&self) -> PathBuf {
        self.directory.path().join("history.db")
    }
}
fn fixture() -> Fixture {
    let directory = tempfile::tempdir().unwrap();
    let conn = open_db(&directory.path().join("history.db")).unwrap();
    for session in SESSIONS {
        conn.execute("INSERT INTO session_events(source,session_id,event_uid,ts_ms,role,kind,text) VALUES ('claude',?1,?1,42,'user','text',?1)",params![session]).unwrap();
    }
    Fixture { directory, conn }
}
const SESSIONS: [&str; 3] = ["local-only", "remote-only", "both"];

fn config(instance: &str) -> DeliveryJobConfig {
    DeliveryJobConfig {
        destination_id: "fixture".into(),
        instance_id: instance.into(),
        account_id: "fixture-account".into(),
        mapping_version: "1".into(),
        selection: ExportSelection {
            all_sources: false,
            sources: vec!["claude".into()],
            kinds: vec!["session_event".into()],
            ..ExportSelection::default()
        },
        limits: DeliveryLimits::default(),
    }
}
fn body(batch: &HistoryExportBatch) -> PreparedBody {
    PreparedBody {
        content_type: "application/json".into(),
        body: serde_json::to_string(&batch.records).unwrap(),
    }
}
fn ack(batch: &HistoryExportBatch) -> DeliveryAcknowledgment {
    DeliveryAcknowledgment {
        batch_id: batch.batch_id.clone(),
        accepted_revision_ids: batch
            .records
            .iter()
            .map(|record| record.revision_id.clone())
            .collect(),
        unsupported_revision_ids: vec![],
        acceptance_level: AcceptanceLevel::Durable,
    }
}

type PrepareFn = Box<dyn Fn(&HistoryExportBatch) -> Result<PreparedBody, ReceiverFailure> + Sync>;
type SendFn = Box<
    dyn Fn(&PreparedPayload, &HistoryExportBatch) -> Result<DeliveryAcknowledgment, ReceiverFailure>
        + Sync,
>;
struct Fake {
    mapping: String,
    kinds: Vec<&'static str>,
    tombstones: bool,
    prepare: PrepareFn,
    send: SendFn,
}
impl Default for Fake {
    fn default() -> Self {
        Self {
            mapping: "1".into(),
            kinds: vec!["session_event"],
            tombstones: true,
            prepare: Box::new(|batch| Ok(body(batch))),
            send: Box::new(|_payload, batch| Ok(ack(batch))),
        }
    }
}
impl Receiver for Fake {
    fn mapping_version(&self) -> &str {
        &self.mapping
    }
    fn supported_kinds(&self) -> &[&str] {
        &self.kinds
    }
    fn supports_tombstones(&self) -> bool {
        self.tombstones
    }
    fn prepare(
        &self,
        batch: &HistoryExportBatch,
        _ctx: &ReceiverContext<'_>,
    ) -> Result<PreparedBody, ReceiverFailure> {
        (self.prepare)(batch)
    }
    fn send(
        &self,
        payload: &PreparedPayload,
        batch: &HistoryExportBatch,
        _ctx: &ReceiverContext<'_>,
    ) -> Result<DeliveryAcknowledgment, ReceiverFailure> {
        (self.send)(payload, batch)
    }
}
fn one(receiver: &dyn Receiver) -> SingleReceiver<'_> {
    SingleReceiver::new("fixture", "one", receiver)
}
fn options() -> DrainOptions {
    DrainOptions::new("worker")
}
fn run(path: &Path, receivers: &dyn Receivers, options: &DrainOptions) -> DrainResult {
    drain(path, receivers, options, &system_clock, &|| false).unwrap()
}

#[test]
fn lost_acknowledgment_retries_the_persisted_payload_without_remapping() {
    let fixture = fixture();
    let job = create_job(&fixture.conn, &config("one"), 0).unwrap();
    let bodies: Arc<Mutex<Vec<String>>> = Arc::default();
    let batches: Arc<Mutex<Vec<String>>> = Arc::default();
    let received: Arc<Mutex<HashSet<String>>> = Arc::default();
    let preparations = Arc::new(AtomicUsize::new(0));
    let observe = |bodies: Arc<Mutex<Vec<String>>>,
                   batches: Arc<Mutex<Vec<String>>>,
                   received: Arc<Mutex<HashSet<String>>>| {
        move |payload: &PreparedPayload, batch: &HistoryExportBatch| {
            bodies.lock().unwrap().push(payload.body.clone());
            batches.lock().unwrap().push(batch.batch_id.clone());
            received.lock().unwrap().extend(
                batch
                    .records
                    .iter()
                    .map(|record| record.revision_id.clone()),
            );
        }
    };
    let first = Fake {
        prepare: {
            let preparations = preparations.clone();
            Box::new(move |batch| {
                preparations.fetch_add(1, SeqCst);
                Ok(body(batch))
            })
        },
        send: {
            let record = observe(bodies.clone(), batches.clone(), received.clone());
            // The receiver durably accepted the batch, then lost its response.
            Box::new(move |payload, batch| {
                record(payload, batch);
                Err(DeliveryFailure::Transient.into())
            })
        },
        ..Fake::default()
    };
    let failed = run(
        &fixture.path(),
        &one(&first),
        &DrainOptions {
            max_batches: 1,
            ..options()
        },
    );
    assert_eq!(failed.attempts, 1);
    assert_eq!(failed.statuses[0].failure.as_deref(), Some("transient"));
    assert_eq!(failed.statuses[0].acknowledged_records, 0);
    assert_eq!(failed.statuses[0].pending_records, 3);
    retry_job(&fixture.conn, &job.job_id).unwrap();

    // Recreate every receiver object, retaining only the database and receiver state.
    let restarted = Fake {
        prepare: Box::new(|_batch| panic!("retry must use the persisted mapping")),
        send: {
            let record = observe(bodies.clone(), batches.clone(), received.clone());
            Box::new(move |payload, batch| {
                record(payload, batch);
                Ok(ack(batch))
            })
        },
        ..Fake::default()
    };
    let delivered = run(&fixture.path(), &one(&restarted), &options());
    assert_eq!(delivered.issues, vec![]);
    assert_eq!(delivered.statuses[0].acknowledged_records, 3);
    assert_eq!(delivered.statuses[0].pending_records, 0);
    assert_eq!(
        delivered.statuses[0].acceptance_level.as_deref(),
        Some("durable")
    );
    assert_eq!(preparations.load(SeqCst), 1);
    assert_eq!(received.lock().unwrap().len(), 3);
    let (bodies, batches) = (bodies.lock().unwrap(), batches.lock().unwrap());
    assert_eq!(bodies.len(), 2);
    assert_eq!(bodies[0], bodies[1]);
    assert_eq!(batches[0], batches[1]);
    assert!(delivered.retention.used_bytes < delivered.retention.limit_bytes);
}

#[test]
fn partial_and_invalid_acknowledgments_cannot_skip_holes() {
    let fixture = fixture();
    let job = create_job(&fixture.conn, &config("one"), 0).unwrap();
    let subset = Fake {
        send: Box::new(|_payload, batch| {
            Ok(DeliveryAcknowledgment {
                accepted_revision_ids: vec![batch.records[0].revision_id.clone()],
                ..ack(batch)
            })
        }),
        ..Fake::default()
    };
    let partial = run(
        &fixture.path(),
        &one(&subset),
        &DrainOptions {
            max_batches: 1,
            ..options()
        },
    );
    assert_eq!(partial.statuses[0].acknowledged_records, 0);
    assert_eq!(partial.statuses[0].pending_records, 3);
    retry_job(&fixture.conn, &job.job_id).unwrap();

    let wrong = Fake {
        send: Box::new(|_payload, batch| {
            Ok(DeliveryAcknowledgment {
                batch_id: "wrong-batch".into(),
                ..ack(batch)
            })
        }),
        ..Fake::default()
    };
    let invalid = run(&fixture.path(), &one(&wrong), &options());
    assert_eq!(invalid.statuses[0].state, "blocked");
    assert_eq!(
        invalid.statuses[0].failure.as_deref(),
        Some("invalid_payload")
    );
    assert_eq!(invalid.statuses[0].acknowledged_records, 0);
    retry_job(&fixture.conn, &job.job_id).unwrap();

    let accepted = run(&fixture.path(), &one(&Fake::default()), &options());
    assert_eq!(accepted.statuses[0].acknowledged_records, 3);
}

#[test]
fn one_blocked_receiver_cannot_stop_another_and_mapping_upgrades_keep_pending_work() {
    let fixture = fixture();
    let first = create_job(&fixture.conn, &config("one"), 0).unwrap();
    create_job(&fixture.conn, &config("two"), 0).unwrap();
    let mut registry: HashMap<(String, String), Box<dyn Receiver>> = HashMap::new();
    registry.insert(
        ("fixture".into(), "one".into()),
        Box::new(Fake {
            send: Box::new(|_payload, _batch| Err(DeliveryFailure::AuthenticationRequired.into())),
            ..Fake::default()
        }),
    );
    registry.insert(("fixture".into(), "two".into()), Box::new(Fake::default()));
    let result = run(&fixture.path(), &registry, &options());
    let job = |result: &DrainResult, instance: &str| {
        result
            .statuses
            .iter()
            .find(|job| job.config.instance_id == instance)
            .cloned()
            .unwrap()
    };
    assert_eq!(
        job(&result, "one").failure.as_deref(),
        Some("authentication_required")
    );
    assert_eq!(job(&result, "two").acknowledged_records, 3);
    retry_job(&fixture.conn, &first.job_id).unwrap();

    let upgraded = Fake {
        mapping: "2".into(),
        ..Fake::default()
    };
    let result = run(
        &fixture.path(),
        &one(&upgraded),
        &DrainOptions {
            job_ids: Some(vec![first.job_id.clone()]),
            ..options()
        },
    );
    assert_eq!(result.statuses.len(), 1);
    assert_eq!(
        result.statuses[0].failure.as_deref(),
        Some("mapping_version_mismatch")
    );
    assert_eq!(result.statuses[0].pending_records, 3);
}

#[test]
fn an_unregistered_destination_is_reported_once_in_the_host_json_shape() {
    let fixture = fixture();
    let job = create_job(&fixture.conn, &config("one"), 0).unwrap();
    let empty: HashMap<(String, String), Box<dyn Receiver>> = HashMap::new();
    let result = run(&fixture.path(), &empty, &options());
    assert_eq!(result.attempts, 0);
    assert_eq!(
        result.issues,
        vec![DrainIssue {
            job_id: job.job_id.clone(),
            code: DrainIssueCode::DestinationNotRegistered,
            detail: None,
        }]
    );
    let value = serde_json::to_value(&result).unwrap();
    assert_eq!(value["issues"][0]["jobId"], serde_json::json!(job.job_id));
    assert_eq!(value["issues"][0]["code"], "DESTINATION_NOT_REGISTERED");
    assert!(value["issues"][0].get("detail").is_none());
    assert_eq!(
        value["retention"]["usedBytes"],
        serde_json::json!(result.retention.used_bytes)
    );
    assert!(value["retention"]["limitBytes"].is_i64());
    assert_eq!(
        value["statuses"][0]["job_id"],
        serde_json::json!(job.job_id)
    );
    assert_eq!(value["attempts"], serde_json::json!(0));
}

#[test]
fn lease_renewal_prevents_a_concurrent_worker_from_dispatching_the_same_batch() {
    let fixture = fixture();
    create_job(&fixture.conn, &config("one"), 0).unwrap();
    let sending = Arc::new(AtomicBool::new(false));
    let sends = Arc::new(AtomicUsize::new(0));
    let leased = DrainOptions {
        lease_ms: 100,
        request_timeout_ms: 15_000,
        ..options()
    };
    let worker = {
        let (path, sending, sends) = (fixture.path(), sending.clone(), sends.clone());
        let leased = leased.clone();
        std::thread::spawn(move || {
            let slow = Fake {
                send: Box::new(move |_payload, batch| {
                    sends.fetch_add(1, SeqCst);
                    sending.store(true, SeqCst);
                    // Far longer than the lease: only renewal keeps this claim.
                    std::thread::sleep(Duration::from_millis(400));
                    Ok(ack(batch))
                }),
                ..Fake::default()
            };
            run(&path, &one(&slow), &leased)
        })
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    while !sending.load(SeqCst) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(sending.load(SeqCst), "the receiver never started sending");
    // Wait past the original lease: only a successful renewal blocks the claim.
    std::thread::sleep(Duration::from_millis(150));
    assert!(
        !worker.is_finished(),
        "the first drain finished before the concurrent claim"
    );
    let blocked = Fake {
        send: Box::new(|_payload, _batch| panic!("a leased batch must not be dispatched twice")),
        ..Fake::default()
    };
    let second = run(&fixture.path(), &one(&blocked), &leased);
    assert_eq!(second.attempts, 0);
    let first = worker.join().unwrap();
    assert_eq!(first.attempts, 1);
    assert_eq!(first.statuses[0].acknowledged_records, 3);
    assert_eq!(sends.load(SeqCst), 1);
}

#[test]
fn eligibility_changed_while_mapping_is_rechecked_before_transport() {
    let fixture = fixture();
    let job = create_job(&fixture.conn, &config("one"), 0).unwrap();
    let sent = Arc::new(AtomicUsize::new(0));
    let excluding = Fake {
        prepare: {
            let path = fixture.path();
            Box::new(move |batch| {
                let conn = open_db(&path).unwrap();
                for session in SESSIONS {
                    set_session_excluded(
                        &conn,
                        &SessionIdentity {
                            source: "claude".into(),
                            session_id: session.into(),
                        },
                        true,
                    )
                    .unwrap();
                }
                Ok(body(batch))
            })
        },
        send: {
            let sent = sent.clone();
            Box::new(move |_payload, batch| {
                sent.fetch_add(1, SeqCst);
                Ok(ack(batch))
            })
        },
        ..Fake::default()
    };
    let result = run(&fixture.path(), &one(&excluding), &options());
    assert_eq!(sent.load(SeqCst), 0);
    assert_eq!(result.attempts, 1);
    assert_eq!(result.issues[0].code, DrainIssueCode::DeliveryStateFailed);
    retry_job(&fixture.conn, &job.job_id).unwrap();

    let suppressed = run(&fixture.path(), &one(&excluding), &options());
    assert_eq!(sent.load(SeqCst), 0);
    assert_eq!(suppressed.statuses[0].suppressed_records, 3);
    assert_eq!(suppressed.statuses[0].acknowledged_records, 0);
}

#[test]
fn host_cancellation_returns_before_any_attempt() {
    let fixture = fixture();
    create_job(&fixture.conn, &config("one"), 0).unwrap();
    let untouched = Fake {
        prepare: Box::new(|_batch| panic!("a cancelled drain must not map a payload")),
        send: Box::new(|_payload, _batch| panic!("a cancelled drain must not send")),
        ..Fake::default()
    };
    let result = drain(
        &fixture.path(),
        &one(&untouched),
        &options(),
        &system_clock,
        &|| true,
    )
    .unwrap();
    assert_eq!(result.attempts, 0);
    assert_eq!(result.issues, vec![]);
    assert_eq!(result.statuses[0].pending_records, 0);
}

#[test]
fn invalid_selections_and_bounds_are_invalid_arguments() {
    let fixture = fixture();
    create_job(&fixture.conn, &config("one"), 0).unwrap();
    let receiver = Fake::default();
    let failure = |options: DrainOptions| {
        drain(
            &fixture.path(),
            &one(&receiver),
            &options,
            &system_clock,
            &|| false,
        )
        .unwrap_err()
        .to_string()
    };
    assert!(failure(DrainOptions {
        job_ids: Some(vec!["missing".into()]),
        ..options()
    })
    .starts_with("INVALID_ARGUMENT: unknown delivery job selection"));
    for invalid in [
        DrainOptions {
            max_batches: 0,
            ..options()
        },
        DrainOptions {
            max_prepare_steps: 10_001,
            ..options()
        },
        DrainOptions {
            lease_ms: 29,
            ..options()
        },
        DrainOptions {
            request_timeout_ms: 0,
            ..options()
        },
    ] {
        assert!(failure(invalid).starts_with("INVALID_ARGUMENT:"));
    }
}
