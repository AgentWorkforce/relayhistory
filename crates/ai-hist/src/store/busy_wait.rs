//! Count only sleeps in this thread's installed SQLite busy handler.
use std::{
    cell::Cell,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

pub(super) fn busy_retry_handler(prior_attempts: i32) -> bool {
    if crate::ingest::shared_capture_stop().is_some_and(|stop| stop.is_stopped()) {
        return false;
    }
    let Some(backoff_ms) = super::busy_retry_backoff_ms(prior_attempts) else {
        return false;
    };
    let clock = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64;
    let seed = clock ^ u64::from(std::process::id()) ^ prior_attempts as u64;
    let jitter_ms = seed % (backoff_ms / super::BUSY_RETRY_JITTER_DIVISOR + 1);
    sleep(Duration::from_millis(backoff_ms + jitter_ms));
    true
}

pub(crate) fn configure_busy_retry(conn: &rusqlite::Connection) -> anyhow::Result<()> {
    conn.busy_handler(Some(busy_retry_handler))?;
    Ok(())
}

#[cfg(test)]
thread_local! {static NEXT_WAIT: std::cell::RefCell<Option<std::sync::mpsc::Sender<()>>> = const { std::cell::RefCell::new(None) };}
#[cfg(test)]
pub(crate) fn observe_next_wait(sender: std::sync::mpsc::Sender<()>) {
    NEXT_WAIT.with(|slot| *slot.borrow_mut() = Some(sender));
}

thread_local! {
    static TOTAL: Cell<Duration> = const { Cell::new(Duration::ZERO) };
}

pub(crate) fn total() -> Duration {
    TOTAL.with(Cell::get)
}

pub(super) fn sleep(duration: Duration) {
    #[cfg(test)]
    if let Some(sender) = NEXT_WAIT.with(|slot| slot.borrow_mut().take()) {
        let _ = sender.send(());
    }
    let started = Instant::now();
    std::thread::sleep(duration);
    TOTAL.with(|total| total.set(total.get().saturating_add(started.elapsed())));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn independent_threads_and_measurement_windows_do_not_share_waits() {
        let before = total();
        std::thread::spawn(|| sleep(Duration::from_millis(10)))
            .join()
            .unwrap();
        assert_eq!(total(), before);
        sleep(Duration::from_millis(10));
        assert!(total().saturating_sub(before) >= Duration::from_millis(10));
        let next = total();
        assert_eq!(total().saturating_sub(next), Duration::ZERO);
    }
}
