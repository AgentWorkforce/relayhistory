use super::*;
use std::sync::mpsc;
use std::time::{Duration, Instant};

fn options() -> HydrateSessionOptions {
    HydrateSessionOptions {
        source: "codex".into(),
        session_id: "cancel-lock".into(),
        scope: SessionScope::Remote,
        include_related: false,
    }
}

#[test]
fn remote_hydration_file_lock_wait_honors_capture_cancellation() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("history.db");
    let request = options();
    let first = acquire_remote_hydration_lock(&db, &request).unwrap();
    let stop = crate::StopToken::new();
    let worker_stop = stop.clone();
    let (waiting, receiver) = mpsc::channel();
    let (finished, outcome) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        crate::store::busy_wait::observe_next_wait(waiting);
        let result = crate::ingest::with_capture_token(worker_stop, || {
            acquire_remote_hydration_lock(&db, &request).map(drop)
        });
        let _ = finished.send(result);
    });
    // Observe an actual contended wait, rather than cancelling before entry.
    receiver.recv_timeout(Duration::from_secs(2)).unwrap();
    let started = Instant::now();
    stop.stop();
    let error = outcome
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .unwrap_err();
    worker.join().unwrap();
    assert!(error.is::<crate::ingest::CaptureCancelled>(), "{error:#}");
    assert!(started.elapsed() < Duration::from_secs(2));
    drop(first);
}

#[test]
fn remote_hydration_file_lock_budget_expiry_returns_busy_without_unlocking_holder() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("held.lock");
    let open = || {
        OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .unwrap()
    };
    let holder = open();
    let waiter = open();
    crate::file_lock::try_lock_exclusive(&holder).unwrap();
    let mut attempts = Vec::new();
    let error = wait_for_lock(&waiter, |attempt| {
        attempts.push(attempt);
        attempt < 2
    })
    .unwrap_err();
    assert_eq!(attempts, [0, 1, 2]);
    assert!(
        matches!(error.downcast_ref::<rusqlite::Error>(), Some(rusqlite::Error::SqliteFailure(code, _)) if code.code == rusqlite::ErrorCode::DatabaseBusy)
    );
    assert!(crate::file_lock::try_lock_exclusive(&waiter).is_err());
    crate::file_lock::unlock(&holder).unwrap();
    wait_for_lock(&waiter, |_| panic!("released lock needs no retry")).unwrap();
}

#[test]
fn remote_hydration_file_lock_pre_cancelled_request_does_not_create_lock_directory() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("history.db");
    let stop = crate::StopToken::new();
    stop.stop();
    let error = crate::ingest::with_capture_token(stop, || {
        acquire_remote_hydration_lock(&db, &options()).map(drop)
    })
    .unwrap_err();
    assert!(error.is::<crate::ingest::CaptureCancelled>(), "{error:#}");
    assert!(!dir.path().join(".relayhistory-hydration-locks").exists());
}
