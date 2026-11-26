use super::*;

#[test]
fn incomplete_checkpoint_retries_without_another_capture() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("history.db");
    let (schedule, pending) = std::sync::mpsc::channel();
    let (completed, outcomes) = std::sync::mpsc::channel();
    let available = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let worker_available = std::sync::Arc::clone(&available);
    let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let worker_attempts = std::sync::Arc::clone(&attempts);
    let worker = std::thread::spawn(move || {
        checkpoint_after_quiet_with(
            &db_path,
            Duration::from_millis(10),
            Duration::from_secs(1),
            pending,
            Some(completed),
            &worker_available,
            move |_| {
                let attempt = worker_attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Some(crate::ingest::SweepCheckpoint {
                    escalated: false,
                    complete: attempt > 0,
                    wal_bytes: 4096,
                })
            },
        );
    });
    schedule.send(()).unwrap();
    assert!(
        !outcomes
            .recv_timeout(Duration::from_millis(100))
            .unwrap()
            .complete
    );
    assert!(
        outcomes
            .recv_timeout(Duration::from_millis(100))
            .unwrap()
            .complete
    );
    assert_eq!(attempts.load(std::sync::atomic::Ordering::Relaxed), 2);
    assert!(available.load(std::sync::atomic::Ordering::Acquire));
    drop(schedule);
    worker.join().unwrap();
}
