//! Count only sleeps in this thread's installed SQLite busy handler.
use std::{
    cell::Cell,
    time::{Duration, Instant},
};

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
