use super::*;

pub(super) fn checkpoint_after_quiet(
    db_path: &Path,
    quiet: Duration,
    max_delay: Duration,
    pending: std::sync::mpsc::Receiver<()>,
    completed: Option<std::sync::mpsc::Sender<crate::ingest::SweepCheckpoint>>,
    available: &std::sync::atomic::AtomicBool,
) {
    checkpoint_after_quiet_with(
        db_path,
        quiet,
        max_delay,
        pending,
        completed,
        available,
        checkpoint_wal,
    );
}

pub(super) fn checkpoint_after_quiet_with(
    db_path: &Path,
    quiet: Duration,
    max_delay: Duration,
    pending: std::sync::mpsc::Receiver<()>,
    completed: Option<std::sync::mpsc::Sender<crate::ingest::SweepCheckpoint>>,
    available: &std::sync::atomic::AtomicBool,
    checkpoint: impl Fn(&Path) -> Option<crate::ingest::SweepCheckpoint>,
) {
    let mut incomplete_attempts = 0;
    while pending.recv().is_ok() {
        loop {
            if !wait_for_quiet(&pending, quiet, max_delay) {
                note_quiet_checkpoint(
                    checkpoint(db_path),
                    &completed,
                    available,
                    &mut incomplete_attempts,
                );
                return;
            }
            loop {
                match note_quiet_checkpoint(
                    checkpoint(db_path),
                    &completed,
                    available,
                    &mut incomplete_attempts,
                ) {
                    QuietCheckpointDecision::Complete => break,
                    QuietCheckpointDecision::Stop => return,
                    QuietCheckpointDecision::Retry => match pending.recv_timeout(quiet) {
                        Ok(()) => break,
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return,
                    },
                }
            }
            if incomplete_attempts == 0 {
                break;
            }
        }
    }
}

fn wait_for_quiet(
    pending: &std::sync::mpsc::Receiver<()>,
    quiet: Duration,
    max_delay: Duration,
) -> bool {
    let cap = std::time::Instant::now() + max_delay;
    let mut quiet_until = std::time::Instant::now() + quiet;
    loop {
        let now = std::time::Instant::now();
        let wait = quiet_until.min(cap).saturating_duration_since(now);
        match pending.recv_timeout(wait) {
            Ok(()) => {
                quiet_until = std::time::Instant::now() + quiet;
                if std::time::Instant::now() >= cap {
                    return true;
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => return true,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum QuietCheckpointDecision {
    Complete,
    Retry,
    Stop,
}

pub(super) fn note_quiet_checkpoint(
    outcome: Option<crate::ingest::SweepCheckpoint>,
    completed: &Option<std::sync::mpsc::Sender<crate::ingest::SweepCheckpoint>>,
    available: &std::sync::atomic::AtomicBool,
    incomplete_attempts: &mut u8,
) -> QuietCheckpointDecision {
    const MAX_CONSECUTIVE_INCOMPLETE_ATTEMPTS: u8 = 3;
    if let (Some(completed), Some(outcome)) = (completed, outcome) {
        let _ = completed.send(outcome);
    }
    match outcome {
        Some(outcome) if outcome.complete => {
            *incomplete_attempts = 0;
            QuietCheckpointDecision::Complete
        }
        Some(_) => {
            *incomplete_attempts += 1;
            if *incomplete_attempts < MAX_CONSECUTIVE_INCOMPLETE_ATTEMPTS {
                QuietCheckpointDecision::Retry
            } else {
                available.store(false, std::sync::atomic::Ordering::Release);
                QuietCheckpointDecision::Stop
            }
        }
        None => {
            available.store(false, std::sync::atomic::Ordering::Release);
            QuietCheckpointDecision::Stop
        }
    }
}

pub(super) fn checkpoint_wal(db_path: &Path) -> Option<crate::ingest::SweepCheckpoint> {
    let flags =
        rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX;
    if let Ok(conn) = rusqlite::Connection::open_with_flags(db_path, flags) {
        if conn
            .is_readonly(rusqlite::DatabaseName::Main)
            .unwrap_or(true)
        {
            return None;
        }
        return Some(crate::ingest::checkpoint_after_sweep(
            &conn,
            db_path,
            u64::MAX,
        ));
    }
    None
}
