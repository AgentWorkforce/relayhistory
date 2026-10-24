//! Count only sleeps in this thread's installed SQLite busy handler.
use std::{
    cell::Cell,
    time::{Duration, Instant},
};

pub(crate) fn configure_busy_retry(conn: &rusqlite::Connection) -> anyhow::Result<()> {
    conn.busy_handler(Some(super::busy_retry_handler))?;
    Ok(())
}

thread_local! {
    static TOTAL: Cell<Duration> = const { Cell::new(Duration::ZERO) };
}

pub(crate) fn total() -> Duration {
    TOTAL.with(Cell::get)
}

pub(super) fn sleep(duration: Duration) {
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
