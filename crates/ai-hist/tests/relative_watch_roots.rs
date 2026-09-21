//! A watch root given relatively, matched against what the watcher actually
//! reports.
//!
//! The only test in this binary, and it has to be: it sets the process working
//! directory, which is what "relative" is relative to. Everything here goes
//! through a real `notify` watcher rather than through `covers` with a
//! hand-written path — the defect this covers was precisely that the two
//! disagreed, so a test that supplies its own event path cannot see it.
#![cfg(feature = "fs-events")]

use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ai_hist::discover::WatchRoot;
use ai_hist::watch::{TickFn, TickOutcome, WatchDriver, WatchLoop};

/// Generous upper bound for "an event the loop must deliver". Never used as a
/// sleep: a passing run returns as soon as the event lands.
const ARRIVES_WITHIN: Duration = Duration::from_secs(10);

#[test]
fn a_relative_file_root_matches_the_events_the_watcher_reports() {
    let dir = tempfile::tempdir().expect("tempdir");
    let restore = std::env::current_dir().expect("current dir");
    std::env::set_current_dir(dir.path()).expect("set the working directory");

    // Three spellings of the same kind of root, built while the working
    // directory is the one they are relative to.
    let bare = PathBuf::from("trajectory.json");
    let nested = PathBuf::from("runs/trajectory.json");
    let absolute = std::fs::canonicalize(dir.path())
        .expect("canonical tempdir")
        .join("absolute.json");
    std::fs::create_dir_all("runs").expect("nested dir");
    for path in [bare.as_path(), nested.as_path(), absolute.as_path()] {
        std::fs::write(path, "{\"id\":\"seed\"}\n").expect("seed the file");
    }

    let (sender, ticks) = mpsc::channel();
    let tick: TickFn = Arc::new(move |force| {
        let _ = sender.send(force);
        Ok(TickOutcome::default())
    });
    let watch = Arc::new(
        WatchLoop::new(tick)
            .with_immediate(false)
            .with_fs_events(true)
            .with_roots(vec![
                WatchRoot::file(bare.clone()),
                WatchRoot::file(nested.clone()),
                WatchRoot::file(absolute.clone()),
            ])
            .with_debounce_ms(100)
            // Both cadences long enough that no backstop tick can be mistaken
            // for an event-driven one.
            .with_poll_interval_ms(600_000)
            .with_slow_poll_ms(600_000),
    );
    let runner = watch.clone();
    let thread = std::thread::spawn(move || {
        runner.run().expect("watch loop run");
    });
    let deadline = Instant::now() + ARRIVES_WITHIN;
    while watch.driver().is_none() {
        assert!(Instant::now() < deadline, "the watch loop never started");
        std::thread::yield_now();
    }

    let status = watch.status().expect("status");
    assert_eq!(
        status.driver,
        WatchDriver::FsEvents,
        "every root's parent exists, so all three attach: {status:?}"
    );
    assert!(
        status.pending.is_empty(),
        "a relative root must not be left uncovered: {status:?}"
    );

    // The bare relative root first: this is the one whose events the loop used
    // to reject, because the root kept the spelling `trajectory.json` while
    // the watcher reported the path it was registered with.
    forces_a_tick(&ticks, &bare, "a bare relative file root");
    // Positive controls: the two spellings that already worked must still
    // match their own events, so the normalisation did not simply widen the
    // filter until everything matches.
    forces_a_tick(&ticks, &nested, "a nested relative file root");
    forces_a_tick(&ticks, &absolute, "an absolute file root");

    // And the filter is still a filter: a sibling in the same directory is
    // not one of these roots. The loop has to be quiet before that can be
    // asked. One `fs::write` is several filesystem events — a create, a
    // modify, a close — and an event landing after the debounce window has
    // opened deliberately re-arms it, so a single write legitimately produces
    // more than one forced tick. A trailing tick from the write above would
    // otherwise be read as the sibling's, which is exactly how this test
    // failed in CI at `d6cf96b`.
    settle(&ticks, "before the sibling write");
    std::fs::write("unrelated.json", "{}\n").expect("write a sibling");
    assert_eq!(
        ticks.recv_timeout(Duration::from_millis(800)),
        Err(RecvTimeoutError::Timeout),
        "a file beside the named ones must not drive a sweep"
    );

    watch.stop();
    let _ = thread.join();
    std::env::set_current_dir(restore).expect("restore the working directory");
}

/// Rewrite `path` and require a *forced* tick, which only a filesystem event
/// produces — both cadences are ten minutes.
fn forces_a_tick(ticks: &mpsc::Receiver<bool>, path: &Path, what: &str) {
    std::fs::write(path, "{\"id\":\"changed\"}\n").expect("rewrite the file");
    let deadline = Instant::now() + ARRIVES_WITHIN;
    loop {
        match ticks.recv_timeout(Duration::from_millis(500)) {
            Ok(true) => return,
            Ok(false) => panic!("{what}: no backstop tick is due in this test"),
            Err(_) => assert!(
                Instant::now() < deadline,
                "{what} ({}) drove no forced sweep",
                path.display()
            ),
        }
    }
}

/// Wait until the loop has stopped ticking, so what follows is about the next
/// write and nothing before it.
///
/// Bounded in both directions: each wait is several debounce windows, so a
/// trailing event has time to arrive and be swept, and the whole settle has a
/// deadline, so a loop that never goes quiet fails the test rather than
/// hanging it. Reaching quiet is itself an assertion — a loop that kept
/// ticking after one write would be a defect of its own.
fn settle(ticks: &mpsc::Receiver<bool>, when: &str) {
    let deadline = Instant::now() + ARRIVES_WITHIN;
    while ticks.recv_timeout(Duration::from_millis(400)).is_ok() {
        assert!(
            Instant::now() < deadline,
            "the loop never went quiet {when}"
        );
    }
}
