//! The generic drain loop: one worker, receiver-agnostic, ported from the
//! behaviours the TypeScript host tests already pin down.

use ai_hist::delivery::worker::*;
use ai_hist::delivery::*;
use ai_hist::open_db;
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

/// The lease this test claims under.
///
/// The keepalive renews with two thirds of the lease left, so the whole margin
/// for a scheduler stall plus a contended write is one lease. At 100 ms that
/// margin was shorter than a single step of the database's busy backoff, which
/// reaches 500 ms, and a 4-way-loaded machine lost it routinely — in both
/// directions: the claim lapsing and letting this test's "must not be
/// dispatched twice" receiver fire, or the renewal failing and turning a good
/// acknowledgment into a transient failure. Neither is the property under
/// test. A lease that can absorb one full backoff keeps the property intact —
/// the send below still outlives several lease lifetimes, so nothing but
/// renewal can hold the claim — without betting on thread wake-up latency.
const RENEWAL_LEASE_MS: i64 = 600;

/// The lease column for one job, read through the test's own connection.
fn lease_until_ms(conn: &Connection, job_id: &str) -> Option<i64> {
    conn.query_row(
        "SELECT lease_until_ms FROM delivery_jobs WHERE id = ?",
        params![job_id],
        |row| row.get::<_, Option<i64>>(0),
    )
    .unwrap()
}

#[test]
fn lease_renewal_prevents_a_concurrent_worker_from_dispatching_the_same_batch() {
    let fixture = fixture();
    let job = create_job(&fixture.conn, &config("one"), 0).unwrap();
    let sending = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let sends = Arc::new(AtomicUsize::new(0));
    let leased = DrainOptions {
        lease_ms: RENEWAL_LEASE_MS,
        request_timeout_ms: 15_000,
        ..options()
    };
    let worker = {
        let (path, sending, sends) = (fixture.path(), sending.clone(), sends.clone());
        let (leased, release) = (leased.clone(), release.clone());
        std::thread::spawn(move || {
            let slow = Fake {
                send: Box::new(move |_payload, batch| {
                    sends.fetch_add(1, SeqCst);
                    sending.store(true, SeqCst);
                    // Held until the concurrent claim has been observed rather
                    // than for a fixed span. The window is then exactly as long
                    // as the property needs: every extra millisecond is one
                    // more renewal that has to land, and buys nothing.
                    while !release.load(SeqCst) {
                        std::thread::sleep(Duration::from_millis(5));
                    }
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

    // Wait for a renewal that has actually been written and that carries the
    // claim a full lease past the one taken at dispatch. Every renewal stores
    // `now + lease_ms`, so `renewed >= claimed_until + lease_ms` is precisely
    // "the worker's own clock is past the original expiry" — the condition the
    // concurrent claim below has to race — established from the database
    // rather than from how promptly this thread happens to wake up.
    let claimed_until = lease_until_ms(&fixture.conn, &job.job_id)
        .expect("the dispatching worker holds a lease while sending");
    let expired = claimed_until + RENEWAL_LEASE_MS;
    while lease_until_ms(&fixture.conn, &job.job_id).is_some_and(|until| until < expired)
        && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(5));
    }
    let renewed = lease_until_ms(&fixture.conn, &job.job_id).expect("the lease is still held");
    assert!(
        renewed >= expired,
        "the keepalive never renewed past the original lease ({renewed} < {expired})"
    );
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
    release.store(true, SeqCst);
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

/// Prepare and claim one batch, the way the drain loop does. `create_job`
/// alone leaves nothing to claim: a batch has to be materialized first.
fn prepare_and_claim(conn: &Connection, job_id: &str, lease_ms: i64) -> ClaimedBatch {
    for _ in 0..100 {
        let now = system_clock();
        if prepare_batch(conn, job_id, now).unwrap().batch_id.is_some() {
            return claim_batch(conn, job_id, "worker", lease_ms, now)
                .unwrap()
                .expect("a prepared batch is claimable");
        }
    }
    panic!("bounded fixture failed to produce a batch")
}

/// A renewal blocked past the deadline must not report a lease it does not
/// hold.
///
/// `renew_lease` used to take its timestamp before asking for the write lock.
/// Acquiring that lock can block for as long as the connection's busy policy
/// allows, so the value it then committed was `stale_now + lease_ms` - for any
/// wait longer than the lease, a deadline already in the past - and it
/// returned `Ok`. The keepalive recorded a renewed lease it did not have while
/// another worker was free to reclaim the batch mid-send: a well-formed answer
/// computed over nothing.
#[test]
fn a_renewal_blocked_past_the_deadline_never_reports_a_lease_in_the_past() {
    let fixture = fixture();
    let job = create_job(&fixture.conn, &config("one"), 0).unwrap();
    let lease_ms = 100;
    let hold = Duration::from_millis(lease_ms as u64 * 6);
    let claim = prepare_and_claim(&fixture.conn, &job.job_id, lease_ms);

    // Hold the write lock for several lease lengths, then release it on a
    // timer. The renewal below therefore blocks, and by the time it commits, a
    // timestamp taken before the block is long stale.
    let path = fixture.path();
    let holding = std::thread::spawn(move || {
        let blocker = open_db(&path).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
        std::thread::sleep(hold);
        blocker.execute_batch("ROLLBACK").unwrap();
    });
    // Let the blocker take the lock before the renewal asks for it.
    std::thread::sleep(Duration::from_millis(20));

    let renewer = open_db(&fixture.path()).unwrap();
    let renewed = renew_lease(&renewer, &claim.lease, lease_ms, &system_clock)
        .expect("the lock is released well inside the busy policy");
    let returned_at = system_clock();
    holding.join().unwrap();

    assert!(
        renewed.expires_at_ms > returned_at,
        "renewal returned a lease that had already expired ({} <= {returned_at}); \
         the deadline was computed from a clock read before a {}ms block",
        renewed.expires_at_ms,
        hold.as_millis(),
    );
    assert_eq!(
        lease_until_ms(&fixture.conn, &job.job_id),
        Some(renewed.expires_at_ms),
        "the stored deadline must be the one the caller was told about"
    );
}

/// A lapsed lease that nobody took is still this worker's claim.
///
/// `check_lease` used to require `lease_until_ms > now`, which reads an expiry
/// nobody acted on as "the claim is gone". Expiry is a signal to *other*
/// workers that an abandoned batch may be stolen, and `claim_batch` enforces
/// it; every takeover bumps the fence, so the fence alone proves ownership.
/// Conflating the two meant a worker could not record its own outcome after
/// its lease lapsed — which is how a panicking receiver stopped producing a
/// `transient` failure under load.
#[test]
fn an_expired_but_unclaimed_lease_can_still_record_its_own_outcome() {
    let fixture = fixture();
    let job = create_job(&fixture.conn, &config("one"), 0).unwrap();
    let claim = prepare_and_claim(&fixture.conn, &job.job_id, 30);
    // Past the deadline, with no second worker anywhere near it.
    std::thread::sleep(Duration::from_millis(60));

    let status = record_failure(
        &fixture.conn,
        &claim.lease,
        DeliveryFailure::Transient,
        None,
        system_clock(),
    )
    .expect("an unclaimed batch is still ours to fail");
    assert_eq!(status.failure.as_deref(), Some("transient"));

    // A batch that really was taken is still refused. `claim_batch` bumps the
    // fence, which is what the check now rests on.
    let stolen = claim_batch(
        &fixture.conn,
        &job.job_id,
        "other",
        30,
        system_clock() + 10_000,
    )
    .unwrap();
    if stolen.is_some() {
        assert!(
            record_failure(
                &fixture.conn,
                &claim.lease,
                DeliveryFailure::Transient,
                None,
                system_clock(),
            )
            .is_err(),
            "a fenced lease must not be able to move another worker's state"
        );
    }
}

#[test]
fn a_panicking_receiver_is_a_transient_failure_and_releases_the_keepalive() {
    let fixture = fixture();
    create_job(&fixture.conn, &config("one"), 0).unwrap();
    let receiver = Fake {
        send: Box::new(|_payload, _batch| panic!("receiver bug")),
        ..Fake::default()
    };
    // A short lease so a keepalive thread that never saw `stop` would be
    // caught renewing rather than merely idle when the deadline below fires.
    let options = DrainOptions {
        lease_ms: 100,
        ..options()
    };
    let started = Instant::now();
    let result = run(&fixture.path(), &one(&receiver), &options);
    assert!(started.elapsed() < Duration::from_secs(10), "drain hung");
    assert_eq!(result.attempts, 1);
    assert_eq!(result.issues, vec![]);
    assert_eq!(result.statuses[0].failure.as_deref(), Some("transient"));
    assert_eq!(result.statuses[0].acknowledged_records, 0);
    assert_eq!(result.statuses[0].pending_records, 3);
}
