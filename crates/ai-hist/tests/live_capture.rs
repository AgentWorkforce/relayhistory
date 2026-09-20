//! Live capture: the watch loop's drivers, the stat-only source fingerprint,
//! and the lifecycle-hook fast path.
//!
//! Every timing assertion here synchronises on an event — a channel receive,
//! a completion flag — rather than on a fixed sleep, so the suite holds on a
//! loaded runner. The one place a duration is asserted against is a *negative*
//! ("and no second tick arrived"), where the window is several times the
//! debounce it is proving.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ai_hist::discover::{DiscoveryEnv, ProviderRoots, WatchDepth};
use ai_hist::watch::{TickFn, TickOutcome, WatchDriver, WatchLoop};
use ai_hist::{shallow_providers, SyncOutput};

/// Generous upper bound for "an event the loop must deliver". Never used as a
/// sleep: a passing run returns as soon as the event lands.
const ARRIVES_WITHIN: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// watch loop
// ---------------------------------------------------------------------------

/// A loop whose ticks are reported on a channel, already running on its own
/// thread. Dropping the guard stops the loop and joins the thread.
struct RunningLoop {
    watch: Arc<WatchLoop>,
    ticks: mpsc::Receiver<bool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl RunningLoop {
    fn start(build: impl FnOnce(WatchLoop) -> WatchLoop, tick: TickFn) -> Self {
        let watch = Arc::new(build(WatchLoop::new(tick)));
        let runner = watch.clone();
        let thread = std::thread::spawn(move || {
            runner.run().expect("watch loop run");
        });
        // `run` publishes the driver it selected before it takes its first
        // tick or attaches to anything, so waiting for it here is what makes
        // "the loop is up" observable without a sleep.
        let deadline = std::time::Instant::now() + ARRIVES_WITHIN;
        while watch.driver().is_none() {
            assert!(
                std::time::Instant::now() < deadline,
                "the watch loop never started"
            );
            std::thread::yield_now();
        }
        Self {
            watch,
            // Replaced by `reporting`; a loop started through `start` directly
            // reports its ticks some other way.
            ticks: mpsc::channel().1,
            thread: Some(thread),
        }
    }

    /// Build the reporting tick function and the loop around it in one step,
    /// so the channel end the test reads is wired to the loop that writes it.
    fn reporting(build: impl FnOnce(WatchLoop) -> WatchLoop) -> Self {
        let (sender, ticks) = mpsc::channel();
        let tick: TickFn = Arc::new(move |force| {
            // A closed receiver means the test is already tearing down; that is
            // not a tick failure.
            let _ = sender.send(force);
            Ok(TickOutcome::default())
        });
        let mut running = Self::start(build, tick);
        running.ticks = ticks;
        running
    }

    fn next_tick(&self) -> Result<bool, RecvTimeoutError> {
        self.ticks.recv_timeout(ARRIVES_WITHIN)
    }
}

impl Drop for RunningLoop {
    fn drop(&mut self) {
        self.watch.stop();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[test]
fn one_change_signal_drives_exactly_one_forced_tick() {
    let running = RunningLoop::reporting(|watch| {
        watch
            .with_immediate(false)
            .with_fs_events(false)
            // Long enough that no poll can be mistaken for the change tick.
            .with_poll_interval_ms(600_000)
            .with_debounce_ms(100)
    });

    running.watch.notify_change();

    assert_eq!(
        running.next_tick(),
        Ok(true),
        "a change signal must drive a tick, and it must force the scan"
    );
    assert_eq!(
        running.ticks.recv_timeout(Duration::from_millis(600)),
        Err(RecvTimeoutError::Timeout),
        "one change signal must not drive a second tick"
    );
}

#[test]
fn a_burst_of_change_signals_collapses_into_the_debounce_window() {
    let running = RunningLoop::reporting(|watch| {
        watch
            .with_immediate(false)
            .with_fs_events(false)
            .with_poll_interval_ms(600_000)
            .with_debounce_ms(300)
    });

    for _ in 0..100 {
        running.watch.notify_change();
    }

    assert_eq!(running.next_tick(), Ok(true));
    // At most one more: signals that land while the first tick's debounce
    // window is open re-arm the single-bit pending flag, which is deliberate —
    // sustained writes keep ticking at the debounce cadence instead of waiting
    // for a quiet period that a busy session never reaches. What must not
    // happen is one tick per signal.
    let mut extra = 0;
    while running
        .ticks
        .recv_timeout(Duration::from_millis(900))
        .is_ok()
    {
        extra += 1;
        assert!(
            extra <= 1,
            "100 change signals produced {} ticks",
            extra + 1
        );
    }
}

#[test]
fn the_polling_backstop_does_not_force_the_scan() {
    let running = RunningLoop::reporting(|watch| {
        watch
            .with_immediate(false)
            .with_fs_events(false)
            .with_poll_interval_ms(50)
    });

    assert_eq!(
        running.next_tick(),
        Ok(false),
        "a polled tick has no in-flight write to outrun, so it keeps the fingerprint fast path"
    );
}

/// A latch that starts closed and, once opened, stays open. A tick body that
/// waits on it blocks only while the test wants it to, so no assertion can
/// wedge the suite: the worst a wrong result can do is fail.
#[derive(Default)]
struct Gate {
    open: Mutex<bool>,
    changed: std::sync::Condvar,
}

impl Gate {
    fn wait(&self) {
        let mut open = self.open.lock().expect("gate");
        while !*open {
            open = self.changed.wait(open).expect("gate");
        }
    }

    fn open(&self) {
        *self.open.lock().expect("gate") = true;
        self.changed.notify_all();
    }
}

#[test]
fn a_manual_tick_joins_the_run_already_in_flight() {
    let bodies = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Gate::default());
    let (started, started_rx) = mpsc::channel();

    let bodies_for_tick = bodies.clone();
    let gate_for_tick = gate.clone();
    let tick: TickFn = Arc::new(move |_force| {
        bodies_for_tick.fetch_add(1, Ordering::SeqCst);
        let _ = started.send(());
        gate_for_tick.wait();
        Ok(TickOutcome::default())
    });
    let running = RunningLoop::start(
        |watch| {
            watch
                .with_immediate(true)
                .with_fs_events(false)
                .with_poll_interval_ms(600_000)
        },
        tick,
    );

    started_rx.recv_timeout(ARRIVES_WITHIN).expect("first tick");
    assert_eq!(bodies.load(Ordering::SeqCst), 1);

    // The startup tick is parked on a gate only this test can open, so the
    // manual tick below is certain to arrive while a sweep is in flight — no
    // window in which it could find the loop idle.
    let (entered, entered_rx) = mpsc::channel();
    let (returned, returned_rx) = mpsc::channel();
    let watch = running.watch.clone();
    let joiner = std::thread::spawn(move || {
        let _ = entered.send(());
        watch.tick();
        let _ = returned.send(());
    });
    entered_rx.recv_timeout(ARRIVES_WITHIN).expect("joiner ran");

    assert_eq!(
        returned_rx.recv_timeout(Duration::from_millis(500)),
        Err(RecvTimeoutError::Timeout),
        "tick() returned while a sweep was still in flight; it must be a completion barrier"
    );

    gate.open();
    returned_rx
        .recv_timeout(ARRIVES_WITHIN)
        .expect("tick() never returned after the sweep it joined completed");
    joiner.join().expect("joiner");

    assert_eq!(
        bodies.load(Ordering::SeqCst),
        1,
        "a manual tick arriving mid-run must join it, not start a second sweep"
    );
}

#[test]
fn stop_waits_for_the_tick_in_flight() {
    let finished = Arc::new(AtomicUsize::new(0));
    let (started, started_rx) = mpsc::channel();

    let finished_for_tick = finished.clone();
    let tick: TickFn = Arc::new(move |_force| {
        let _ = started.send(());
        std::thread::sleep(Duration::from_millis(250));
        finished_for_tick.fetch_add(1, Ordering::SeqCst);
        Ok(TickOutcome::default())
    });
    let running = RunningLoop::start(
        |watch| {
            watch
                .with_immediate(true)
                .with_fs_events(false)
                .with_poll_interval_ms(600_000)
        },
        tick,
    );
    started_rx.recv_timeout(ARRIVES_WITHIN).expect("first tick");

    running.watch.stop();

    assert_eq!(
        finished.load(Ordering::SeqCst),
        1,
        "stop() returned while a sweep was still writing"
    );
}

#[test]
fn an_unwatchable_root_falls_back_to_polling() {
    let dir = tempfile::tempdir().expect("tempdir");
    let missing = dir.path().join("not-created-yet");
    let running = RunningLoop::reporting(|watch| {
        watch
            .with_immediate(false)
            .with_fs_events(true)
            .with_roots(vec![ai_hist::discover::WatchRoot::tree(missing)])
            .with_poll_interval_ms(50)
    });

    assert_eq!(
        running.next_tick(),
        Ok(false),
        "with no watchable root the loop must still poll"
    );
    assert_eq!(running.watch.driver(), Some(WatchDriver::Polling));
}

#[test]
fn disabling_fs_events_polls_a_real_directory() {
    let dir = tempfile::tempdir().expect("tempdir");
    let running = RunningLoop::reporting(|watch| {
        watch
            .with_immediate(false)
            .with_fs_events(false)
            .with_roots(vec![ai_hist::discover::WatchRoot::tree(dir.path())])
            .with_poll_interval_ms(50)
    });

    assert_eq!(running.next_tick(), Ok(false));
    assert_eq!(
        running.watch.driver(),
        Some(WatchDriver::Polling),
        "--no-fsevents must take the polling path even over a watchable directory"
    );
}

/// Acceptance: appending to a watched transcript drives exactly one forced
/// sweep, through the real filesystem watcher rather than the injected signal.
#[cfg(feature = "fs-events")]
#[test]
fn appending_to_a_watched_transcript_drives_one_forced_tick() {
    let home = tempfile::tempdir().expect("tempdir");
    let transcript = write_claude_transcript(home.path(), "proj", "watched", 1);
    let roots = ai_hist::discover::watch_roots(
        &shallow_providers(),
        &ProviderRoots {
            home: home.path(),
            opencode_db: &home.path().join("no-opencode.db"),
        },
    );
    assert!(
        roots
            .iter()
            .any(|root| root.path == home.path().join(".claude/projects")),
        "claude's transcript root must be watched: {roots:?}"
    );

    let running = RunningLoop::reporting(|watch| {
        watch
            .with_immediate(false)
            .with_fs_events(true)
            .with_roots(roots)
            .with_debounce_ms(150)
            // Neither cadence can fire inside this test, so any tick observed
            // came from a filesystem event.
            .with_poll_interval_ms(600_000)
            .with_slow_poll_ms(600_000)
    });
    assert_eq!(
        running.watch.driver(),
        Some(WatchDriver::FsEvents),
        "the watcher did not attach to the claude transcript root"
    );

    append_claude_record(&transcript, "watched", 2);

    assert_eq!(
        running.next_tick(),
        Ok(true),
        "an append under a watched root must drive a forced sweep"
    );
    assert_eq!(
        running.ticks.recv_timeout(Duration::from_millis(900)),
        Err(RecvTimeoutError::Timeout),
        "one append must not drive a second sweep"
    );
}

#[test]
fn every_file_backed_provider_contributes_a_watch_root() {
    let home = PathBuf::from("/tmp/relayhistory-watch-roots");
    let opencode_db = home.join(".local/share/opencode/opencode.db");
    let roots = ai_hist::discover::watch_roots(
        &shallow_providers(),
        &ProviderRoots {
            home: &home,
            opencode_db: &opencode_db,
        },
    );

    for expected in [
        home.join(".claude/projects"),
        home.join(".codex/sessions"),
        home.join(".codex/archived_sessions"),
        home.join(".cursor/projects"),
        home.join(".grok/sessions"),
        // The opencode database is rewritten in place; its directory is what
        // sees the write, and its write-ahead log lands there too.
        opencode_db.parent().expect("opencode dir").to_path_buf(),
    ] {
        assert!(
            roots.iter().any(|root| root.path == expected),
            "{expected:?} missing from {roots:?}"
        );
    }
    let mut paths = roots.iter().map(|root| &root.path).collect::<Vec<_>>();
    paths.sort();
    let before = paths.len();
    paths.dedup();
    assert_eq!(paths.len(), before, "watch roots must be deduplicated");
}

/// A trajectory root is watched because it is *configured*, not because it
/// already has files in it. Deriving the watch from an existing file's parent
/// covers only the shape the tree has right now.
#[test]
fn trajectory_roots_are_watched_before_they_hold_anything() {
    let home = tempfile::tempdir().expect("tempdir");
    let empty = home.path().join("Projects/app/.trajectories");
    std::fs::create_dir_all(&empty).expect("empty trajectory root");
    // A sibling that will only exist later: the root must already cover it.
    let future = empty.join("completed/2026-09");

    let roots = ai_hist::sync_watch_roots(home.path(), &home.path().join("opencode.db"));
    let root = roots
        .iter()
        .find(|root| root.path == empty)
        .unwrap_or_else(|| panic!("empty trajectory root missing from {roots:?}"));
    assert_eq!(
        root.depth,
        WatchDepth::Tree,
        "a trajectory root must be watched as a tree, or a new completed/<month>/ goes unseen"
    );
    assert!(
        !roots.iter().any(|root| root.path == future),
        "the root covers its subtree; individual descendants are not separate roots"
    );
}

/// The fingerprint re-derives trajectory roots on every tick, so the walk that
/// finds them cannot afford to descend a dependency tree.
#[test]
fn the_trajectory_root_scan_skips_dependency_trees() {
    let home = tempfile::tempdir().expect("tempdir");
    let real = home.path().join("Projects/app/.trajectories");
    std::fs::create_dir_all(&real).expect("real root");
    for buried in [
        "Projects/app/node_modules/pkg/.trajectories",
        "Projects/app/.git/modules/.trajectories",
        "Projects/app/target/debug/.trajectories",
    ] {
        std::fs::create_dir_all(home.path().join(buried)).expect("buried root");
    }

    let roots = ai_hist::sync_watch_roots(home.path(), &home.path().join("opencode.db"));
    let trajectory_roots = roots
        .iter()
        .filter(|root| root.path.starts_with(home.path().join("Projects")))
        .collect::<Vec<_>>();

    assert_eq!(
        trajectory_roots.len(),
        1,
        "only the project's own root should be found: {trajectory_roots:?}"
    );
    assert_eq!(trajectory_roots[0].path, real);
}

#[test]
fn the_sweep_only_sources_are_watched_too() {
    let home = PathBuf::from("/tmp/relayhistory-sweep-watch-roots");
    let roots = ai_hist::sync_watch_roots(&home, &home.join("opencode.db"));

    // The flat logs are reached through their parent: a watch on the file
    // itself follows an inode the harness may replace.
    for (expected, depth) in [
        (home.join(".claude"), WatchDepth::Directory),
        (home.join(".codex"), WatchDepth::Directory),
        (home.join(".claude/projects"), WatchDepth::Tree),
    ] {
        let root = roots
            .iter()
            .find(|root| root.path == expected)
            .unwrap_or_else(|| panic!("{expected:?} missing from {roots:?}"));
        assert_eq!(
            root.depth, depth,
            "{expected:?} is watched at the wrong depth"
        );
    }
}

/// A watch loop started before a provider exists must pick that provider up,
/// not report itself as event-driven while the directory is uncovered.
#[cfg(feature = "fs-events")]
#[test]
fn a_root_created_after_startup_is_picked_up() {
    let home = tempfile::tempdir().expect("tempdir");
    let late = home.path().join(".claude/projects");
    let roots = vec![ai_hist::discover::WatchRoot::tree(late.clone())];

    let running = RunningLoop::reporting(|watch| {
        watch
            .with_immediate(false)
            .with_fs_events(true)
            .with_roots(roots)
            .with_debounce_ms(100)
            // The backstop is what retries an unattached root, so it has to be
            // short here; it is also the only cadence that can fire, which is
            // why the assertions below look at `forced` to tell the two apart.
            .with_slow_poll_ms(200)
            .with_poll_interval_ms(200)
    });

    let status = running.watch.status().expect("status");
    assert_eq!(
        status.driver,
        WatchDriver::Polling,
        "a loop with nothing attached is polling, whatever its roots say"
    );
    assert_eq!(status.pending, vec![late.clone()]);

    std::fs::create_dir_all(&late).expect("create the root late");

    // Wait for the loop to notice and re-attach. The status is published
    // before the tick that follows it, so this is the loop's own signal.
    let deadline = std::time::Instant::now() + ARRIVES_WITHIN;
    while running.watch.driver() != Some(WatchDriver::FsEvents) {
        assert!(
            std::time::Instant::now() < deadline,
            "a root created after startup was never picked up: {:?}",
            running.watch.status()
        );
        std::thread::yield_now();
    }
    assert!(running.watch.status().expect("status").pending.is_empty());

    // A write under the newly attached root must drive a *forced* tick. Only a
    // filesystem event forces, so the backstop ticks this short cadence keeps
    // producing cannot be mistaken for the thing being proved.
    write_claude_transcript(home.path(), "late", "late-1", 1);
    let deadline = std::time::Instant::now() + ARRIVES_WITHIN;
    loop {
        match running.ticks.recv_timeout(Duration::from_millis(500)) {
            Ok(true) => break,
            Ok(false) => assert!(
                std::time::Instant::now() < deadline,
                "only backstop ticks arrived after a write under the new root"
            ),
            Err(_) => assert!(
                std::time::Instant::now() < deadline,
                "a write under the newly attached root drove no tick"
            ),
        }
    }
}

/// End-to-end companion to `watch::tests::a_non_recursive_root_covers_its_own_entries_only`.
/// On Linux the kernel already declines to deliver the subtree, so this passes
/// either way here; on macOS, where FSEvents delivers it regardless, the
/// filter is the only thing between `~/.claude/todos/` and a forced sweep per
/// tool call. The second half is what has teeth on every platform: the filter
/// must not reject what the root does cover.
#[cfg(feature = "fs-events")]
#[test]
fn a_write_below_a_non_recursive_root_does_not_force_a_tick() {
    let dir = tempfile::tempdir().expect("tempdir");
    let buried = dir.path().join("todos");
    std::fs::create_dir_all(&buried).expect("subdirectory");

    let running = RunningLoop::reporting(|watch| {
        watch
            .with_immediate(false)
            .with_fs_events(true)
            .with_roots(vec![ai_hist::discover::WatchRoot::directory(dir.path())])
            .with_debounce_ms(100)
            .with_poll_interval_ms(600_000)
            .with_slow_poll_ms(600_000)
    });
    assert_eq!(running.watch.driver(), Some(WatchDriver::FsEvents));

    std::fs::write(buried.join("task-1.json"), "{}\n").expect("write below the root");
    assert_eq!(
        running.ticks.recv_timeout(Duration::from_millis(800)),
        Err(RecvTimeoutError::Timeout),
        "a write below a non-recursive root must not drive a sweep"
    );

    std::fs::write(dir.path().join("history.jsonl"), "{}\n").expect("write in the root");
    assert_eq!(
        running.next_tick(),
        Ok(true),
        "a write in the root itself must still drive a forced sweep"
    );
}

/// `retry_pending` alone cannot reach a root whose *name* was unknown at
/// startup — a project that grows a `.trajectories` directory later. The
/// refresh callback is what closes that, on the backstop tick.
#[cfg(feature = "fs-events")]
#[test]
fn a_root_discovered_after_startup_is_adopted() {
    let home = tempfile::tempdir().expect("tempdir");
    let projects = home.path().join("Projects/app");
    std::fs::create_dir_all(&projects).expect("project dir");
    let home_for_refresh = home.path().to_path_buf();
    let opencode = home.path().join("opencode.db");

    let running = RunningLoop::reporting(move |watch| {
        watch
            .with_immediate(false)
            .with_fs_events(true)
            .with_roots(ai_hist::sync_watch_roots(&home_for_refresh, &opencode))
            .with_roots_refresh(Arc::new(move || {
                ai_hist::sync_watch_roots(&home_for_refresh, &opencode)
            }))
            .with_debounce_ms(100)
            // Short, because the backstop tick is what re-derives the roots.
            .with_slow_poll_ms(200)
            .with_poll_interval_ms(200)
    });

    // The directory does not exist yet, so its name is not in any root list.
    let late = projects.join(".trajectories");
    assert!(!running
        .watch
        .status()
        .expect("status")
        .watched
        .contains(&late));
    std::fs::create_dir_all(&late).expect("late trajectory root");

    let deadline = std::time::Instant::now() + ARRIVES_WITHIN;
    while !running
        .watch
        .status()
        .expect("status")
        .watched
        .contains(&late)
    {
        assert!(
            std::time::Instant::now() < deadline,
            "a .trajectories directory created after startup was never adopted: {:?}",
            running.watch.status()
        );
        std::thread::yield_now();
    }

    std::fs::write(late.join("run-1.json"), "{\"id\":\"t1\"}\n").expect("write trajectory");
    let deadline = std::time::Instant::now() + ARRIVES_WITHIN;
    loop {
        match running.ticks.recv_timeout(Duration::from_millis(500)) {
            Ok(true) => break,
            Ok(false) | Err(_) => assert!(
                std::time::Instant::now() < deadline,
                "a write in the adopted root drove no forced sweep"
            ),
        }
    }
}

/// Coverage is not the user's sweep cadence. A root that does not exist yet is
/// retried on the *backstop*, which the status output and the docs promise —
/// not on `--interval`, which may be an hour. `watch --interval 3600` started
/// before Claude is installed must pick `~/.claude/projects` up in seconds,
/// or a session inside that hour is never captured.
#[cfg(feature = "fs-events")]
#[test]
fn an_uncovered_root_is_retried_on_the_backstop_not_the_user_interval() {
    let home = tempfile::tempdir().expect("tempdir");
    let late = home.path().join(".claude/projects");

    let running = RunningLoop::reporting({
        let late = late.clone();
        move |watch| {
            watch
                .with_immediate(false)
                .with_fs_events(true)
                .with_roots(vec![ai_hist::discover::WatchRoot::tree(late)])
                .with_debounce_ms(50)
                .with_slow_poll_ms(300)
                // The user's interval, an hour in miniature: long enough that
                // a retry on this cadence could not happen inside the test.
                .with_poll_interval_ms(600_000)
        }
    });
    let status = running.watch.status().expect("status");
    assert_eq!(status.driver, WatchDriver::Polling);
    assert_eq!(status.pending, vec![late.clone()]);

    // The other half of the promise, asserted first because it is only true
    // while the loop is polling: waking early to *reconcile* must not sweep
    // early. Several backstop windows pass here, and the interval the user
    // asked for has not.
    assert_eq!(
        running.ticks.recv_timeout(Duration::from_millis(1_000)),
        Err(RecvTimeoutError::Timeout),
        "reconciling on the backstop must not sweep on the user's interval"
    );

    std::fs::create_dir_all(&late).expect("install the provider");

    let deadline = std::time::Instant::now() + ARRIVES_WITHIN;
    while running.watch.driver() != Some(WatchDriver::FsEvents) {
        assert!(
            std::time::Instant::now() < deadline,
            "an uncovered root was retried on the user interval, not the backstop: {:?}",
            running.watch.status()
        );
        std::thread::yield_now();
    }

    // Positive control: the root that was picked up carries real events, so
    // the transition above is coverage and not just a status change.
    write_claude_transcript(home.path(), "late", "late-1", 1);
    let deadline = std::time::Instant::now() + ARRIVES_WITHIN;
    loop {
        match running.ticks.recv_timeout(Duration::from_millis(500)) {
            Ok(true) => break,
            // Once attached the loop is event-driven, and its backstop is the
            // slow poll, so unforced ticks are expected here.
            Ok(false) | Err(_) => assert!(
                std::time::Instant::now() < deadline,
                "a write under the newly attached root drove no forced sweep"
            ),
        }
    }
}

/// Reconciliation must not depend on the loop running out of things to do.
///
/// Every filesystem event ends the wait early, so a reconciliation that only
/// happens when the wait *expires* is starved by exactly the machine live
/// capture exists for: one session writing a few times a second postpones
/// attaching every pending root for as long as it keeps writing, and a
/// provider installed in that window — or a rollout written to it and cleaned
/// up again — is missed entirely.
#[cfg(feature = "fs-events")]
#[test]
fn a_busy_root_does_not_starve_another_root_of_its_reconciliation() {
    let home = tempfile::tempdir().expect("tempdir");
    let busy = home.path().join(".codex/sessions");
    std::fs::create_dir_all(&busy).expect("busy root");
    let late = home.path().join(".claude/projects");

    let running = RunningLoop::reporting({
        let busy = busy.clone();
        let late = late.clone();
        move |watch| {
            watch
                .with_immediate(false)
                .with_fs_events(true)
                .with_roots(vec![
                    ai_hist::discover::WatchRoot::tree(busy),
                    ai_hist::discover::WatchRoot::tree(late),
                ])
                .with_debounce_ms(50)
                // The backstop, which the writes below are faster than.
                .with_slow_poll_ms(400)
                .with_poll_interval_ms(400)
        }
    });
    assert_eq!(
        running.watch.status().expect("status").pending,
        vec![late.clone()],
        "the root that does not exist yet is the one pending"
    );

    // Keep one watched root busy at well under the backstop interval, so the
    // wait never expires while this runs.
    let writing = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let written = Arc::new(AtomicUsize::new(0));
    let writer = {
        let busy = busy.clone();
        let writing = writing.clone();
        let written = written.clone();
        std::thread::spawn(move || {
            let mut index = 0u64;
            while writing.load(Ordering::SeqCst) {
                index += 1;
                if std::fs::write(busy.join(format!("busy-{index}.jsonl")), "{}\n").is_ok() {
                    written.fetch_add(1, Ordering::SeqCst);
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        })
    };

    // Positive control: the writer is real and its events reach the loop, so a
    // failure below is starvation rather than a watcher that never worked.
    let deadline = std::time::Instant::now() + ARRIVES_WITHIN;
    loop {
        match running.ticks.recv_timeout(Duration::from_millis(500)) {
            Ok(true) => break,
            Ok(false) | Err(_) => assert!(
                std::time::Instant::now() < deadline,
                "the busy root drove no forced sweep"
            ),
        }
    }

    std::fs::create_dir_all(&late).expect("install the second provider");

    let deadline = std::time::Instant::now() + ARRIVES_WITHIN;
    while !running
        .watch
        .status()
        .expect("status")
        .watched
        .contains(&late)
    {
        if std::time::Instant::now() >= deadline {
            writing.store(false, Ordering::SeqCst);
            let _ = writer.join();
            panic!(
                "a root stayed pending while another root was written to ({} writes): {:?}",
                written.load(Ordering::SeqCst),
                running.watch.status()
            );
        }
        std::thread::yield_now();
    }

    writing.store(false, Ordering::SeqCst);
    let _ = writer.join();
    assert!(
        written.load(Ordering::SeqCst) > 4,
        "the busy root must have been written to throughout, or nothing was starving anything"
    );
}

/// A watch is bound to the directory *object*, not to its name. Delete a
/// watched root and the kernel drops the watch with the inode; recreate it —
/// which is what a `rm -rf ~/.codex/sessions` followed by the next session
/// does — and the name is back while the watch is not. The loop would go on
/// reporting coverage it does not have, and every transcript written there
/// would wait for the backstop instead of waking a sweep.
#[cfg(feature = "fs-events")]
#[test]
fn a_recreated_root_is_watched_again() {
    let home = tempfile::tempdir().expect("tempdir");
    let root = home.path().join("sessions");
    std::fs::create_dir_all(&root).expect("root");

    let running = RunningLoop::reporting({
        let root = root.clone();
        move |watch| {
            watch
                .with_immediate(false)
                .with_fs_events(true)
                .with_roots(vec![ai_hist::discover::WatchRoot::tree(root)])
                .with_debounce_ms(100)
                // The backstop is what reconciles the registrations.
                .with_slow_poll_ms(200)
                .with_poll_interval_ms(200)
        }
    });
    assert_eq!(running.watch.driver(), Some(WatchDriver::FsEvents));

    // Positive control first: the watch works before the directory is
    // replaced, so a silent failure afterwards cannot be mistaken for a
    // watcher that never worked.
    std::fs::write(root.join("first.jsonl"), "{}\n").expect("write under the original root");
    let deadline = std::time::Instant::now() + ARRIVES_WITHIN;
    loop {
        match running.ticks.recv_timeout(Duration::from_millis(500)) {
            Ok(true) => break,
            Ok(false) | Err(_) => assert!(
                std::time::Instant::now() < deadline,
                "a write under the original root drove no forced sweep"
            ),
        }
    }

    std::fs::remove_dir_all(&root).expect("delete the root");
    std::fs::create_dir_all(&root).expect("recreate the root");

    // Wait for the loop to re-register it. `watched` is the loop's own
    // statement about coverage, so waiting on a *fresh* forced tick below is
    // what proves the statement is true.
    let deadline = std::time::Instant::now() + ARRIVES_WITHIN;
    while !running
        .watch
        .status()
        .expect("status")
        .watched
        .contains(&root)
    {
        assert!(
            std::time::Instant::now() < deadline,
            "a recreated root was dropped from the watch set: {:?}",
            running.watch.status()
        );
        std::thread::yield_now();
    }

    // Drain whatever the delete and recreate themselves produced, so the
    // assertion below is about the write and not about the churn. Bounded by
    // a window rather than by "until quiet": the short backstop this test
    // needs keeps producing ticks, so a drain that waited for silence would
    // never return.
    let drain_until = std::time::Instant::now() + Duration::from_millis(600);
    while std::time::Instant::now() < drain_until {
        let _ = running.ticks.recv_timeout(Duration::from_millis(100));
    }

    std::fs::write(root.join("second.jsonl"), "{}\n").expect("write under the recreated root");
    let deadline = std::time::Instant::now() + ARRIVES_WITHIN;
    loop {
        match running.ticks.recv_timeout(Duration::from_millis(500)) {
            Ok(true) => break,
            Ok(false) | Err(_) => assert!(
                std::time::Instant::now() < deadline,
                "a write under the recreated root drove no forced sweep: {:?}",
                running.watch.status()
            ),
        }
    }
    assert!(running
        .watch
        .status()
        .expect("status")
        .watched
        .contains(&root));
}

/// A `TRAJECTORY_ROOT` entry may name one JSON file, and that file's parent is
/// routinely `$HOME` — or `/`. The parent is what has to be registered, since
/// a watch on the file itself stops firing the moment the harness rewrites it
/// atomically, but only the named file may wake a sweep: watching that parent
/// as a tree turns every unrelated write on the machine into a forced
/// fingerprint walk.
#[cfg(feature = "fs-events")]
#[test]
fn a_file_root_wakes_on_its_own_name_only() {
    let dir = tempfile::tempdir().expect("tempdir");
    let named = dir.path().join("trajectory.json");
    std::fs::write(&named, "{\"id\":\"t1\"}\n").expect("seed the named file");
    let buried = dir.path().join("nested");
    std::fs::create_dir_all(&buried).expect("sibling directory");

    let running = RunningLoop::reporting({
        let named = named.clone();
        move |watch| {
            watch
                .with_immediate(false)
                .with_fs_events(true)
                .with_roots(vec![ai_hist::discover::WatchRoot::file(named)])
                .with_debounce_ms(100)
                .with_poll_interval_ms(600_000)
                .with_slow_poll_ms(600_000)
        }
    });
    let status = running.watch.status().expect("status");
    assert_eq!(
        status.driver,
        WatchDriver::FsEvents,
        "a file root attaches through its parent, which exists: {status:?}"
    );
    assert_eq!(
        status.watched,
        vec![named.clone()],
        "the root is reported as the file it names, not as its parent"
    );

    std::fs::write(dir.path().join("unrelated.log"), "noise\n").expect("write a sibling");
    std::fs::write(buried.join("deeper.json"), "{}\n").expect("write below the parent");
    assert_eq!(
        running.ticks.recv_timeout(Duration::from_millis(800)),
        Err(RecvTimeoutError::Timeout),
        "a write beside the named file must not drive a sweep"
    );

    // Positive control: the filter that rejected the siblings must still pass
    // the one path the root exists for.
    std::fs::write(&named, "{\"id\":\"t1\",\"n\":2}\n").expect("rewrite the named file");
    assert_eq!(
        running.next_tick(),
        Ok(true),
        "a write to the named file must drive a forced sweep"
    );
}

/// A caller may have no roots at all up front — `sync_watch_roots` returns
/// nothing on a machine with no provider installed yet — and supply them only
/// through the refresher. Attaching the backend anyway is what makes that
/// work: `adopt` is reachable only through an attached watcher, so a loop that
/// skipped it because its initial root list was empty would poll forever.
#[cfg(feature = "fs-events")]
#[test]
fn a_loop_given_its_roots_only_by_the_refresher_still_attaches() {
    let dir = tempfile::tempdir().expect("tempdir");
    let late = dir.path().join("projects");
    let refreshed = {
        let late = late.clone();
        Arc::new(move || vec![ai_hist::discover::WatchRoot::tree(late.clone())])
    };

    let running = RunningLoop::reporting(move |watch| {
        watch
            .with_immediate(false)
            .with_fs_events(true)
            .with_roots(Vec::new())
            .with_roots_refresh(refreshed)
            .with_debounce_ms(100)
            // The backstop tick is what re-derives and adopts the roots.
            .with_slow_poll_ms(200)
            .with_poll_interval_ms(200)
    });

    // Nothing is attached yet, so the loop is honestly polling — which is what
    // makes the transition below evidence that `adopt` ran, rather than the
    // initial attach having covered it.
    assert_eq!(running.watch.driver(), Some(WatchDriver::Polling));

    std::fs::create_dir_all(&late).expect("the root the refresher names");
    let deadline = std::time::Instant::now() + ARRIVES_WITHIN;
    while running.watch.driver() != Some(WatchDriver::FsEvents) {
        assert!(
            std::time::Instant::now() < deadline,
            "a root supplied only by the refresher was never attached: {:?}",
            running.watch.status()
        );
        std::thread::yield_now();
    }

    // And it is a real watch, not just a status: a write under it forces a
    // tick, which the backstop cadence never does.
    std::fs::write(late.join("run-1.json"), "{}\n").expect("write under the adopted root");
    let deadline = std::time::Instant::now() + ARRIVES_WITHIN;
    loop {
        match running.ticks.recv_timeout(Duration::from_millis(500)) {
            Ok(true) => break,
            Ok(false) | Err(_) => assert!(
                std::time::Instant::now() < deadline,
                "a write under the refresher's root drove no forced sweep"
            ),
        }
    }
}

// ---------------------------------------------------------------------------
// source fingerprint
// ---------------------------------------------------------------------------

/// Acceptance: the fingerprint walk is stat-only. A tick over unchanged
/// sources must not open a single provider file.
#[test]
fn the_fingerprint_walk_opens_no_files() {
    let home = tempfile::tempdir().expect("tempdir");
    write_claude_transcript(home.path(), "proj", "fp-1", 1);
    write_claude_transcript(home.path(), "proj", "fp-2", 1);
    let db = home.path().join("history.db");
    let conn = ai_hist::open_db(&db).expect("open db");
    let env = DiscoveryEnv::with_roots(
        &conn,
        home.path().to_path_buf(),
        home.path().join("no-opencode.db"),
    );

    let first =
        ai_hist::discover::source_fingerprint_for(&env, &shallow_providers()).expect("fingerprint");

    assert_eq!(
        env.counters().files_opened,
        0,
        "the fingerprint fold must stat, never open"
    );
    assert_eq!(env.counters().shallow_reads, 0);

    let second =
        ai_hist::discover::source_fingerprint_for(&env, &shallow_providers()).expect("fingerprint");
    assert_eq!(first, second, "an unchanged tree must fold to one value");

    append_claude_record(
        &home.path().join(".claude/projects/proj/fp-1.jsonl"),
        "fp-1",
        2,
    );
    let third =
        ai_hist::discover::source_fingerprint_for(&env, &shallow_providers()).expect("fingerprint");
    assert_ne!(first, third, "an appended transcript must move the value");
}

#[test]
fn an_unchanged_tree_skips_the_sweep_and_force_bypasses_it() {
    let home = tempfile::tempdir().expect("tempdir");
    write_claude_transcript(home.path(), "proj", "skip-1", 1);
    let db = home.path().join("history.db");

    let first = sync_tick(&db, home.path(), false);
    assert!(first.swept, "the first sweep has no fingerprint to match");

    let second = sync_tick(&db, home.path(), false);
    assert!(
        second.skipped_unchanged(),
        "an unchanged tree must short-circuit the sweep"
    );

    let forced = sync_tick(&db, home.path(), true);
    assert!(
        forced.swept,
        "force must bypass the fingerprint, because an fs event can beat the write's flush"
    );
}

#[test]
fn appending_to_a_transcript_reopens_the_sweep() {
    let home = tempfile::tempdir().expect("tempdir");
    let transcript = write_claude_transcript(home.path(), "proj", "grow-1", 1);
    let db = home.path().join("history.db");

    assert!(sync_tick(&db, home.path(), false).swept);
    assert!(sync_tick(&db, home.path(), false).skipped_unchanged());

    append_claude_record(&transcript, "grow-1", 2);

    assert!(
        sync_tick(&db, home.path(), false).swept,
        "an append must reopen the sweep without forcing"
    );
}

/// The sweep reads three kinds of source discovery never enumerates. Each one
/// must move the fingerprint on its own, or a machine whose only activity is
/// of that kind never syncs again after its first sweep.
#[test]
fn a_change_confined_to_a_sweep_only_source_still_runs_the_sweep() {
    let home = tempfile::tempdir().expect("tempdir");
    // One transcript so the catalog is non-empty and the fast path can arm at
    // all; it is never touched again.
    write_claude_transcript(home.path(), "proj", "anchor-1", 1);
    let db = home.path().join("history.db");

    assert!(sync_tick(&db, home.path(), false).swept);
    assert!(
        sync_tick(&db, home.path(), false).skipped_unchanged(),
        "the fast path must be armed before the gap is testable"
    );

    // 1. The flat Claude log. Nothing under ~/.claude/projects moved.
    append_history_log(home.path(), "claude", "flat claude");
    assert!(
        sync_tick(&db, home.path(), false).swept,
        "a new record in ~/.claude/history.jsonl must reopen the sweep"
    );
    assert_eq!(
        history_prompt_count(&db, "flat claude"),
        1,
        "the record appended to the flat log never landed"
    );

    // 2. The flat Codex log.
    assert!(sync_tick(&db, home.path(), false).skipped_unchanged());
    append_history_log(home.path(), "codex", "flat codex");
    assert!(
        sync_tick(&db, home.path(), false).swept,
        "a new record in ~/.codex/history.jsonl must reopen the sweep"
    );
    assert_eq!(history_prompt_count(&db, "flat codex"), 1);

    // 3. A Claude subagent's `agent-<id>.meta.json` sidecar, which is stamped
    //    as evidence even when the transcript beside it is unchanged.
    assert!(sync_tick(&db, home.path(), false).skipped_unchanged());
    let sidecar = home
        .path()
        .join(".claude/projects/proj/anchor-1/subagents/agent-a1.meta.json");
    std::fs::create_dir_all(sidecar.parent().expect("subagents dir")).expect("subagents dir");
    std::fs::write(&sidecar, "{\"agentId\":\"a1\"}\n").expect("write sidecar");
    assert!(
        sync_tick(&db, home.path(), false).swept,
        "a subagent sidecar arriving must reopen the sweep"
    );
}

/// Trajectory records are a declared discovery exemption, so nothing enumerates
/// them — and the sweep reads them anyway.
#[test]
fn a_change_confined_to_a_trajectory_still_runs_the_sweep() {
    let home = tempfile::tempdir().expect("tempdir");
    write_claude_transcript(home.path(), "proj", "anchor-2", 1);
    let db = home.path().join("history.db");
    let trajectories = home.path().join("Projects/app/.trajectories");
    std::fs::create_dir_all(&trajectories).expect("trajectory root");

    assert!(sync_tick(&db, home.path(), false).swept);
    assert!(sync_tick(&db, home.path(), false).skipped_unchanged());

    std::fs::write(
        trajectories.join("run-1.json"),
        r#"{"id":"traj-1","task":"trajectory only","startedAt":"2026-09-19T10:00:00.000Z"}"#,
    )
    .expect("write trajectory");

    assert!(
        sync_tick(&db, home.path(), false).swept,
        "a new trajectory record must reopen the sweep"
    );
    assert!(
        sync_tick(&db, home.path(), false).skipped_unchanged(),
        "and the fast path must re-arm once it has been read"
    );
}

/// A per-file read failure never reaches `SyncSourceReport::failures` — the
/// provider absorbs it, counts it, and returns a successful partial run. But
/// the file was folded into the fingerprint, so arming the fast path over it
/// would mean the retry that failure is relying on never happens.
#[test]
fn a_file_the_sweep_could_not_read_does_not_arm_the_fast_path() {
    let home = tempfile::tempdir().expect("tempdir");
    write_claude_transcript(home.path(), "proj", "anchor-3", 1);
    let sessions = home.path().join(".grok/sessions/s1");
    std::fs::create_dir_all(&sessions).expect("grok session dir");
    let chat = sessions.join("chat_history.jsonl");
    // A real file — so enumeration finds it and the fingerprint stats it —
    // whose bytes cannot be read as text. Grok absorbs that per-file failure
    // and returns a successful partial run, which is the whole point: the
    // sweep reports no failure at all, and only the coverage signal knows.
    // (Permissions would be the obvious lever, but tests here run as root,
    // where a mode of 000 is not a read failure.)
    std::fs::write(&chat, [0xffu8, 0xfe, 0xfd, b'\n']).expect("unreadable grok transcript");
    let db = home.path().join("history.db");

    assert!(sync_tick(&db, home.path(), false).swept);
    assert!(
        sync_tick(&db, home.path(), false).swept,
        "a sweep that could not read one file must not arm the fast path"
    );

    // Repair it. The tick that follows is unchanged in every other respect,
    // so it is exactly the tick a stale-armed fingerprint would have skipped.
    std::fs::write(
        &chat,
        "{\"type\":\"user\",\"content\":\"grok recovered\",\"timestamp\":1758276000000}\n",
    )
    .expect("write grok transcript");

    assert!(sync_tick(&db, home.path(), false).swept);
    assert!(
        sync_tick(&db, home.path(), false).skipped_unchanged(),
        "once every file reads, the fast path must arm again"
    );
}

#[test]
fn a_source_that_could_not_be_read_does_not_arm_the_fast_path() {
    let home = tempfile::tempdir().expect("tempdir");
    write_claude_transcript(home.path(), "proj", "sticky-1", 1);
    let db = home.path().join("history.db");

    // A directory where a transcript file belongs: statted into the
    // fingerprint, unreadable as evidence. Caching the fingerprint over it
    // would make the failure permanent, because the next sweep would match and
    // skip before retrying.
    let broken = home.path().join(".claude/history.jsonl");
    std::fs::create_dir_all(&broken).expect("broken source");

    assert!(sync_tick(&db, home.path(), false).swept);
    assert!(
        sync_tick(&db, home.path(), false).swept,
        "a sweep that could not read a source must not arm the fast path"
    );

    std::fs::remove_dir(&broken).expect("repair the source");

    assert!(sync_tick(&db, home.path(), false).swept);
    assert!(
        sync_tick(&db, home.path(), false).skipped_unchanged(),
        "once every source reads again the fast path must arm"
    );
}

/// Relay history has no file to stat, but it still changes: `import` writes it
/// straight into the catalog. Nothing on the filesystem moves when it does, so
/// without a generation for the relay slice the fingerprint would match and
/// discovery would never see the imported sessions.
#[test]
fn imported_relay_history_reaches_the_catalog() {
    let home = tempfile::tempdir().expect("tempdir");
    write_claude_transcript(home.path(), "proj", "anchor-4", 1);
    let db = home.path().join("history.db");

    assert!(sync_tick(&db, home.path(), false).swept);
    assert!(sync_tick(&db, home.path(), false).skipped_unchanged());

    // What `import` does: relay rows straight into the catalog, no file
    // anywhere on disk.
    {
        let conn = ai_hist::open_db(&db).expect("open db");
        ai_hist::insert_history(
            &conn,
            &ai_hist::HistoryEntry {
                id: 0,
                source: "relay".into(),
                session_id: Some("relay-imported".into()),
                project: Some("/tmp/relay".into()),
                prompt: "imported relay turn".into(),
                prompt_hash: Some(ai_hist::prompt_hash("imported relay turn")),
                timestamp_ms: 1_758_276_000_000,
            },
        )
        .expect("insert relay history");
    }

    assert!(
        sync_tick(&db, home.path(), false).swept,
        "an import must reopen the sweep even though no file moved"
    );
    assert_eq!(
        catalog_session_count(&db, "relay", "relay-imported"),
        1,
        "the imported relay session never reached the catalog"
    );
}

// ---------------------------------------------------------------------------
// hook fast path
// ---------------------------------------------------------------------------

#[test]
fn a_missing_transcript_is_reported_not_raised() {
    let home = tempfile::tempdir().expect("tempdir");
    let db = home.path().join("history.db");

    let report = ai_hist::ingest_transcript_at_with_home(
        &db,
        home.path(),
        "claude",
        &home.path().join(".claude/projects/proj/gone.jsonl"),
        true,
    )
    .expect("a missing transcript must not fail the hook");

    assert_eq!(report.status, ai_hist::TranscriptStatus::Missing);
    assert_eq!(report.session_id, None);
}

#[test]
fn a_transcript_outside_the_provider_root_is_refused() {
    let home = tempfile::tempdir().expect("tempdir");
    let db = home.path().join("history.db");
    let outside = home.path().join("elsewhere.jsonl");
    std::fs::write(&outside, "{}\n").expect("write");

    let error = ai_hist::ingest_transcript_at_with_home(&db, home.path(), "claude", &outside, true)
        .expect_err("a hook payload must not name an arbitrary path");

    assert!(
        format!("{error:#}").contains("SESSION_SOURCE_MISMATCH"),
        "unexpected error: {error:#}"
    );
}

/// Acceptance: a hook ingest followed by a full sweep leaves one copy of the
/// session's evidence, not two.
#[test]
fn a_hook_ingest_then_a_full_sweep_leaves_no_duplicates() {
    let home = tempfile::tempdir().expect("tempdir");
    let transcript = write_claude_transcript(home.path(), "proj", "hooked-1", 3);
    let db = home.path().join("history.db");

    let report =
        ai_hist::ingest_transcript_at_with_home(&db, home.path(), "claude", &transcript, true)
            .expect("hook ingest");
    assert_eq!(report.status, ai_hist::TranscriptStatus::Ingested);
    assert_eq!(report.session_id.as_deref(), Some("hooked-1"));

    let after_hook = session_event_count(&db, "hooked-1");
    assert!(
        after_hook > 0,
        "the hook ingest recorded no events for the session it reported ingesting"
    );

    // Forced, because the sweep must actually walk: the point is that the
    // walk finds the same evidence and does not write it twice.
    assert!(sync_tick(&db, home.path(), true).swept);
    let after_sweep = rehydrate(&db, home.path(), "hooked-1");
    assert_eq!(
        after_sweep.status, "unchanged",
        "the hook's stamp must satisfy the sweep, so nothing is re-read"
    );
    assert_eq!(
        session_event_count(&db, "hooked-1"),
        after_hook,
        "a full sweep after a hook ingest duplicated the session's events"
    );

    // And the same path still picks new evidence up: one more record must add
    // exactly one event, not replay the three already recorded.
    append_claude_record(&transcript, "hooked-1", 4);
    assert!(sync_tick(&db, home.path(), true).swept);
    rehydrate(&db, home.path(), "hooked-1");
    assert_eq!(
        session_event_count(&db, "hooked-1"),
        after_hook + 1,
        "re-reading a grown transcript must append the new record only"
    );
}

#[test]
fn a_hook_ingest_of_an_unchanged_transcript_reports_unchanged() {
    let home = tempfile::tempdir().expect("tempdir");
    let transcript = write_claude_transcript(home.path(), "proj", "twice-1", 2);
    let db = home.path().join("history.db");

    let first =
        ai_hist::ingest_transcript_at_with_home(&db, home.path(), "claude", &transcript, true)
            .expect("first hook ingest");
    assert_eq!(first.status, ai_hist::TranscriptStatus::Ingested);

    let second =
        ai_hist::ingest_transcript_at_with_home(&db, home.path(), "claude", &transcript, true)
            .expect("second hook ingest");
    assert_eq!(
        second.status,
        ai_hist::TranscriptStatus::Unchanged,
        "re-running a hook over an untouched transcript must not re-read it"
    );
}

/// A sweep owes more than ingestion. It also reconciles a session whose
/// per-file stamp matches but whose evidence is gone — a half-restored backup,
/// a truncated write, a maintenance query — and a finished rollout's bytes
/// never move again to reopen it. A stamp describing only the sources would
/// therefore make that loss permanent: every later tick matches and skips the
/// sweep that owns the repair.
#[test]
fn evidence_lost_under_a_matching_stamp_is_repaired_on_the_next_tick() {
    let home = tempfile::tempdir().expect("tempdir");
    write_codex_rollout(home.path(), "sess-repair");
    let db = home.path().join("history.db");

    assert!(sync_tick(&db, home.path(), false).swept);
    assert!(
        sync_tick(&db, home.path(), false).skipped_unchanged(),
        "the fast path must be armed before the loss is testable"
    );

    // Positive control: there is something to lose, and the assertion below
    // is not comparing zero against zero.
    let before = codex_event_count(&db, "sess-repair");
    assert!(
        before > 0,
        "the sweep recorded no events for the rollout, so nothing could be lost"
    );

    let conn = ai_hist::open_db(&db).expect("open db");
    conn.execute(
        "DELETE FROM session_events WHERE source = 'codex' AND session_id = 'sess-repair'",
        [],
    )
    .expect("delete the session's events");
    drop(conn);
    assert_eq!(codex_event_count(&db, "sess-repair"), 0);

    let tick = sync_tick(&db, home.path(), false);
    assert!(
        tick.swept,
        "a destination that lost evidence must not be skipped, whatever the sources say"
    );
    assert_eq!(
        codex_event_count(&db, "sess-repair"),
        before,
        "the sweep must restore the events it owes the rollout"
    );

    // And it settles rather than re-sweeping forever: the repaired
    // destination re-arms the fast path.
    assert!(
        sync_tick(&db, home.path(), false).skipped_unchanged(),
        "a repaired destination must arm the fast path again"
    );
}

/// Growth must not be able to answer for a loss. Totals cannot tell the two
/// apart: delete one event from a finished Codex rollout, let a write for an
/// unrelated session land in the same window, and every total is unchanged or
/// larger while the rollout stays permanently short. The marker is therefore
/// per session.
#[test]
fn growth_elsewhere_does_not_conceal_a_lost_session() {
    let home = tempfile::tempdir().expect("tempdir");
    write_codex_rollout(home.path(), "sess-masked");
    let db = home.path().join("history.db");

    assert!(sync_tick(&db, home.path(), false).swept);
    assert!(
        sync_tick(&db, home.path(), false).skipped_unchanged(),
        "the fast path must be armed before the loss is testable"
    );

    let before = codex_event_count(&db, "sess-masked");
    assert!(
        before > 0,
        "the sweep recorded no events for the rollout, so nothing could be lost"
    );
    let events_before = event_count(&db);

    let conn = ai_hist::open_db(&db).expect("open db");
    conn.execute(
        "DELETE FROM session_events WHERE source = 'codex' AND session_id = 'sess-masked' \
         AND rowid = (SELECT MIN(rowid) FROM session_events \
                      WHERE source = 'codex' AND session_id = 'sess-masked')",
        [],
    )
    .expect("delete one event");
    // Stands in for the hook path's write: a row for a different session,
    // arriving between two ticks. Inserted directly so no *source* changes —
    // a new transcript on disk would reopen the sweep for the wrong reason
    // and the masking would never be exercised.
    conn.execute(
        "INSERT INTO session_events \
         (source, session_id, cwd, project, ts_ms, role, text, kind, event_uid) \
         VALUES ('claude', 'sess-hook', '/tmp/hook', 'hook', 1, 'user', 'hi', 'text', 'uid-hook')",
        [],
    )
    .expect("insert an unrelated event");
    drop(conn);

    // Positive control on the premise: the totals really are masked. Without
    // this the test could pass against a marker that never looked at them.
    assert_eq!(
        event_count(&db),
        events_before,
        "the compensating insert must leave the total unchanged, or nothing is concealed"
    );
    assert_eq!(codex_event_count(&db, "sess-masked"), before - 1);

    let tick = sync_tick(&db, home.path(), false);
    assert!(
        tick.swept,
        "a session that lost an event must reopen the sweep even when the totals did not move"
    );
    assert_eq!(
        codex_event_count(&db, "sess-masked"),
        before,
        "the sweep must restore the event it owes the rollout"
    );
    assert!(
        sync_tick(&db, home.path(), false).skipped_unchanged(),
        "a repaired destination must arm the fast path again"
    );
}

/// The Claude half of the same property. The marker names every session that
/// lost evidence, and the sweep has to *act* on the name: a transcript whose
/// stamp still matches and still has one row left was skipped on an existence
/// check, so the loss survived the sweep it triggered — and the marker written
/// afterwards would have recorded the short count as the new truth.
#[test]
fn a_claude_transcript_short_of_its_events_is_re_read() {
    let home = tempfile::tempdir().expect("tempdir");
    write_claude_transcript(home.path(), "proj", "claude-short", 4);
    let db = home.path().join("history.db");

    assert!(sync_tick(&db, home.path(), false).swept);
    assert!(
        sync_tick(&db, home.path(), false).skipped_unchanged(),
        "the fast path must be armed before the loss is testable"
    );

    let before = session_event_count(&db, "claude-short");
    assert!(
        before > 1,
        "the transcript must leave more than one event, or 'short' and 'empty' are the same test"
    );
    let events_before = event_count(&db);

    let conn = ai_hist::open_db(&db).expect("open db");
    conn.execute(
        "DELETE FROM session_events WHERE source = 'claude' AND session_id = 'claude-short' \
         AND rowid = (SELECT MIN(rowid) FROM session_events \
                      WHERE source = 'claude' AND session_id = 'claude-short')",
        [],
    )
    .expect("delete one event");
    // The compensating write, as in the Codex case: a row for another session,
    // inserted directly so no source moves.
    conn.execute(
        "INSERT INTO session_events \
         (source, session_id, cwd, project, ts_ms, role, text, kind, event_uid) \
         VALUES ('claude', 'sess-hook', '/tmp/hook', 'hook', 1, 'user', 'hi', 'text', 'uid-hook')",
        [],
    )
    .expect("insert an unrelated event");
    drop(conn);
    assert_eq!(
        event_count(&db),
        events_before,
        "the compensating insert must leave the total unchanged, or nothing is concealed"
    );

    let tick = sync_tick(&db, home.path(), false);
    assert!(tick.swept, "a short Claude session must reopen the sweep");
    assert_eq!(
        session_event_count(&db, "claude-short"),
        before,
        "the sweep must re-read the transcript, not skip it on the events that survived"
    );
    assert!(
        sync_tick(&db, home.path(), false).skipped_unchanged(),
        "a repaired destination must arm the fast path again"
    );
}

/// A session is more than its events. `tool_calls` and `file_edits` are put
/// back by the same re-read that restores the events, so a marker that counted
/// only events would let structured evidence disappear under a stamp that
/// still matched — the sweep the loss is owed would be skipped forever.
#[test]
fn structured_evidence_lost_is_restored() {
    let home = tempfile::tempdir().expect("tempdir");
    write_codex_rollout(home.path(), "sess-tools");
    let db = home.path().join("history.db");

    assert!(sync_tick(&db, home.path(), false).swept);
    assert!(
        sync_tick(&db, home.path(), false).skipped_unchanged(),
        "the fast path must be armed before the loss is testable"
    );

    let before = tool_call_count(&db, "codex", "sess-tools");
    assert!(
        before > 0,
        "the rollout recorded no tool calls, so nothing structured could be lost"
    );
    let events_before = event_count(&db);

    let conn = ai_hist::open_db(&db).expect("open db");
    conn.execute(
        "DELETE FROM tool_calls WHERE source = 'codex' AND session_id = 'sess-tools'",
        [],
    )
    .expect("delete the tool calls");
    drop(conn);
    // Positive control on the premise: the *events* are untouched, so nothing
    // but the structured evidence can explain the sweep below.
    assert_eq!(event_count(&db), events_before);
    assert_eq!(tool_call_count(&db, "codex", "sess-tools"), 0);

    assert!(
        sync_tick(&db, home.path(), false).swept,
        "a session short of its tool calls must reopen the sweep"
    );
    assert_eq!(
        tool_call_count(&db, "codex", "sess-tools"),
        before,
        "the sweep must restore the structured evidence it owes the rollout"
    );
    assert!(
        sync_tick(&db, home.path(), false).skipped_unchanged(),
        "a repaired destination must arm the fast path again"
    );
}

/// The catalog row is evidence too. A lost `sessions` row hidden behind an
/// unrelated new session leaves every total covered, and discovery would skip
/// the source whose stamp still matched — so the session stays missing from
/// the catalog while its events sit in the database.
#[test]
fn a_lost_catalog_row_is_restored_even_when_sessions_grew() {
    let home = tempfile::tempdir().expect("tempdir");
    write_codex_rollout(home.path(), "sess-catalog");
    let db = home.path().join("history.db");

    assert!(sync_tick(&db, home.path(), false).swept);
    assert!(sync_tick(&db, home.path(), false).skipped_unchanged());
    assert_eq!(catalog_session_count(&db, "codex", "sess-catalog"), 1);
    let sessions_before = session_count(&db);

    let conn = ai_hist::open_db(&db).expect("open db");
    conn.execute(
        "DELETE FROM sessions WHERE source = 'codex' AND session_id = 'sess-catalog'",
        [],
    )
    .expect("delete the catalog row");
    // The masking write: another session arriving between ticks, so the total
    // is exactly what it was.
    conn.execute(
        "INSERT INTO sessions (session_id, source, cwd, first_activity_ms, last_activity_ms) \
         VALUES ('grown-elsewhere', 'claude', '/tmp/grown', 1, 2)",
        [],
    )
    .expect("insert a session");
    drop(conn);
    assert_eq!(
        session_count(&db),
        sessions_before,
        "the compensating insert must leave the total unchanged, or nothing is concealed"
    );

    assert!(
        sync_tick(&db, home.path(), false).swept,
        "a lost catalog row must reopen the sweep even when the total did not move"
    );
    assert_eq!(
        catalog_session_count(&db, "codex", "sess-catalog"),
        1,
        "the sweep must put the catalog row back"
    );
    assert!(
        sync_tick(&db, home.path(), false).skipped_unchanged(),
        "a repaired catalog must arm the fast path again"
    );
}

/// A delegated child is reached by a different name than its parent. It is
/// deliberately never registered as a session, so the catalog join that finds
/// a top-level transcript finds nothing for it — while its surviving events go
/// on satisfying the existence check the stamp skip is guarded by. A short
/// subagent would therefore be detected on every tick and repaired never,
/// which turns "keep sweeping until it is restored" into a permanent full
/// sweep for a loss the sweep can actually fix.
#[test]
fn a_claude_subagent_short_of_its_events_is_re_read() {
    let home = tempfile::tempdir().expect("tempdir");
    write_claude_delegation(home.path(), "parent-1", "child-1");
    let db = home.path().join("history.db");

    assert!(sync_tick(&db, home.path(), false).swept);
    assert!(
        sync_tick(&db, home.path(), false).skipped_unchanged(),
        "the fast path must be armed before the loss is testable"
    );

    let before = session_event_count(&db, "child-1");
    assert!(
        before > 1,
        "the sidecar must leave more than one event, or 'short' and 'empty' are the same test"
    );
    let events_before = event_count(&db);

    let conn = ai_hist::open_db(&db).expect("open db");
    conn.execute(
        "DELETE FROM session_events WHERE source = 'claude' AND session_id = 'child-1' \
         AND rowid = (SELECT MIN(rowid) FROM session_events \
                      WHERE source = 'claude' AND session_id = 'child-1')",
        [],
    )
    .expect("delete one of the child's events");
    conn.execute(
        "INSERT INTO session_events \
         (source, session_id, cwd, project, ts_ms, role, text, kind, event_uid) \
         VALUES ('claude', 'sess-hook', '/tmp/hook', 'hook', 1, 'user', 'hi', 'text', 'uid-hook')",
        [],
    )
    .expect("insert an unrelated event");
    drop(conn);
    assert_eq!(
        event_count(&db),
        events_before,
        "the compensating insert must leave the total unchanged, or nothing is concealed"
    );

    assert!(
        sync_tick(&db, home.path(), false).swept,
        "a short subagent must reopen the sweep"
    );
    assert_eq!(
        session_event_count(&db, "child-1"),
        before,
        "the sweep must re-read the sidecar, not skip it on the events that survived"
    );
    assert!(
        sync_tick(&db, home.path(), false).skipped_unchanged(),
        "a repaired subagent must arm the fast path again, rather than sweeping forever"
    );
}

/// Detection without repair is only safe if the marker refuses to move. A loss
/// the sweep could not put back — the transcript itself is gone — must leave
/// the marker where it was, or the short count becomes the new baseline and
/// every later tick accepts the loss.
#[test]
fn a_loss_the_sweep_could_not_repair_is_not_re_baselined() {
    let home = tempfile::tempdir().expect("tempdir");
    let transcript = write_claude_transcript(home.path(), "proj", "claude-gone", 3);
    let db = home.path().join("history.db");

    assert!(sync_tick(&db, home.path(), false).swept);
    assert!(sync_tick(&db, home.path(), false).skipped_unchanged());

    let conn = ai_hist::open_db(&db).expect("open db");
    conn.execute(
        "DELETE FROM session_events WHERE source = 'claude' AND session_id = 'claude-gone' \
         AND rowid = (SELECT MIN(rowid) FROM session_events \
                      WHERE source = 'claude' AND session_id = 'claude-gone')",
        [],
    )
    .expect("delete one event");
    drop(conn);
    // Nothing on disk can restore it now.
    std::fs::remove_file(&transcript).expect("remove the transcript");

    assert!(sync_tick(&db, home.path(), false).swept);
    // The point of the test: the sweep that could not repair must not record
    // the short count as the truth. A tick that skipped here would mean the
    // loss had been absorbed.
    assert!(
        sync_tick(&db, home.path(), false).swept,
        "a loss the sweep could not repair must keep the fast path disarmed"
    );
    assert!(
        sync_tick(&db, home.path(), false).swept,
        "and must go on doing so, rather than settling on the shortfall"
    );
}

/// The marker guards what the sweep can put back, and says so. `history` rows
/// come from cursor-backed flat logs sitting at EOF: nothing replays them, so
/// counting them would disarm the fast path forever over a loss no sweep could
/// undo. They are excluded deliberately — this test is the record of that
/// boundary, and of the fact that evidence on the same session *is* guarded.
#[test]
fn history_rows_are_outside_the_repair_guard() {
    let home = tempfile::tempdir().expect("tempdir");
    write_claude_transcript(home.path(), "proj", "hist-1", 2);
    append_history_log(home.path(), "claude", "flat one");
    let db = home.path().join("history.db");

    assert!(sync_tick(&db, home.path(), false).swept);
    assert!(sync_tick(&db, home.path(), false).skipped_unchanged());
    assert_eq!(history_prompt_count(&db, "flat one"), 1);

    let conn = ai_hist::open_db(&db).expect("open db");
    conn.execute("DELETE FROM history WHERE prompt = 'flat one'", [])
        .expect("delete the history row");
    drop(conn);
    assert_eq!(history_prompt_count(&db, "flat one"), 0);

    assert!(
        sync_tick(&db, home.path(), false).skipped_unchanged(),
        "a history loss is not something the sweep can replay, so the marker must not claim it"
    );

    // Positive control on the same database: evidence the sweep *can* restore
    // still reopens it, so the exclusion above is a boundary and not a hole.
    let conn = ai_hist::open_db(&db).expect("open db");
    conn.execute(
        "DELETE FROM session_events WHERE source = 'claude' AND session_id = 'hist-1' \
         AND rowid = (SELECT MIN(rowid) FROM session_events \
                      WHERE source = 'claude' AND session_id = 'hist-1')",
        [],
    )
    .expect("delete one event");
    drop(conn);
    assert!(
        sync_tick(&db, home.path(), false).swept,
        "a lost event on a live transcript must still reopen the sweep"
    );
}

/// A count cannot be negative, and a marker that carries one is corrupt. The
/// danger is specific: under the `current >= stored` rule a negative stored
/// count is satisfied by *every* current value, so a corrupted marker would
/// not merely be wrong — it would be a blanket licence to skip the sweep over
/// evidence that is actually missing. It has to read as unknown instead.
///
/// The entry is mutated in place, keeping the session hash the sweep wrote, so
/// the corrupt count lands on a session that really exists. A made-up hash
/// would never match one and the rule would not be exercised at all.
#[test]
fn a_negative_count_in_the_marker_does_not_license_a_skip() {
    let home = tempfile::tempdir().expect("tempdir");
    write_codex_rollout(home.path(), "sess-negative");
    let db = home.path().join("history.db");

    assert!(sync_tick(&db, home.path(), false).swept);
    assert!(
        sync_tick(&db, home.path(), false).skipped_unchanged(),
        "the fast path must be armed before the marker matters"
    );

    let before = codex_event_count(&db, "sess-negative");
    assert!(before > 0);
    let conn = ai_hist::open_db(&db).expect("open db");
    conn.execute(
        "DELETE FROM session_events WHERE source = 'codex' AND session_id = 'sess-negative'",
        [],
    )
    .expect("delete the events");
    drop(conn);

    let state_path = home.path().join(".sync-state.json");
    let mut state: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&state_path).expect("read sync state"))
            .expect("parse sync state");
    let marker = state["destination_generation"]
        .as_str()
        .expect("a stored marker")
        .to_string();
    let corrupted = marker
        .split(' ')
        .map(|part| match part.split_once('=') {
            Some((session, _)) => format!("{session}=-1.-1.-1.-1"),
            None => part.to_string(),
        })
        .collect::<Vec<_>>()
        .join(" ");
    assert_ne!(
        corrupted, marker,
        "positive control: the marker must have had an entry to corrupt"
    );
    state["destination_generation"] = serde_json::Value::from(corrupted);
    std::fs::write(
        &state_path,
        serde_json::to_vec(&state).expect("serialize sync state"),
    )
    .expect("write sync state");

    assert!(
        sync_tick(&db, home.path(), false).swept,
        "a marker carrying a negative count must sweep, not skip"
    );
    assert_eq!(
        codex_event_count(&db, "sess-negative"),
        before,
        "and the sweep it forced must restore the evidence that was missing"
    );
}

/// The sweep stores an empty marker when it could not measure the destination,
/// and a database written by another build may store anything at all. Reading
/// one has to mean "unknown, go and sweep" — a panic here kills the watch tick
/// and wedges every sync after it.
#[test]
fn an_unreadable_destination_marker_sweeps_instead_of_panicking() {
    for marker in [
        serde_json::Value::from(""),
        serde_json::Value::from("v4"),
        // Shaped like this build's marker, and wrong in one way each.
        serde_json::Value::from("v4 n1"),
        serde_json::Value::from("v4 nx abcdef0123456789=1.0.0.1"),
        serde_json::Value::from("v4 n1 notanentry"),
        serde_json::Value::from("v4 n1 zzzz=1.2.3.4"),
        serde_json::Value::from("v4 n1 abcdef0123456789=1.2.3"),
        serde_json::Value::from("v4 n1 abcdef0123456789=1.2.3.4.5"),
        serde_json::Value::from("v4 n1 abcdef0123456789=1.x.3.4"),
        // Truncated: the count is what makes this distinguishable from a
        // database that legitimately holds fewer sessions.
        serde_json::Value::from("v4 n2 abcdef0123456789=1.0.0.1"),
        // A negative count is the dangerous one: under a `>=` comparison it is
        // satisfied by every current value, so a corrupt marker would license
        // a skip over missing evidence rather than a sweep.
        serde_json::Value::from("v4 n1 abcdef0123456789=-1.0.0.1"),
        serde_json::Value::from("v4 n1 abcdef0123456789=0.0.0.-1"),
        // An entry repeated is not something the grouped reads can produce.
        serde_json::Value::from("v4 n2 abcdef0123456789=1.0.0.1 abcdef0123456789=2.0.0.1"),
        // Markers from the shapes this one replaced.
        serde_json::Value::from("v3 s1 h1"),
        serde_json::Value::from("v2 s1:e1:r1:h1"),
        serde_json::Value::from(":::"),
        serde_json::Value::from(7),
    ] {
        let home = tempfile::tempdir().expect("tempdir");
        write_claude_transcript(home.path(), "proj", "marker-1", 1);
        let db = home.path().join("history.db");
        assert!(sync_tick(&db, home.path(), false).swept);
        assert!(
            sync_tick(&db, home.path(), false).skipped_unchanged(),
            "the fast path must be armed before the marker matters"
        );

        let state_path = home.path().join(".sync-state.json");
        let mut state: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&state_path).expect("read sync state"))
                .expect("parse sync state");
        state["destination_generation"] = marker.clone();
        std::fs::write(
            &state_path,
            serde_json::to_vec(&state).expect("serialize sync state"),
        )
        .expect("write sync state");

        assert!(
            sync_tick(&db, home.path(), false).swept,
            "an unreadable destination marker ({marker}) must sweep, not skip and not panic"
        );
        // Positive control: the sweep rewrote a marker this build can read, so
        // the tick recovers rather than sweeping forever.
        assert!(
            sync_tick(&db, home.path(), false).skipped_unchanged(),
            "the sweep must restore a readable marker ({marker})"
        );
    }
}

/// Rows arriving between sweeps — the hook fast path, hydration — are not a
/// loss, and must not cost a full walk of every source. Only shrinkage is the
/// signal.
#[test]
fn evidence_added_between_sweeps_does_not_reopen_the_sweep() {
    let home = tempfile::tempdir().expect("tempdir");
    let transcript = write_claude_transcript(home.path(), "proj", "grow-2", 2);
    let db = home.path().join("history.db");

    assert!(sync_tick(&db, home.path(), false).swept);
    assert!(sync_tick(&db, home.path(), false).skipped_unchanged());

    // A session the sweep never saw, arriving the way the hook path's writes
    // do: between two ticks, under a stamp that still matches.
    let before = session_count(&db);
    let conn = ai_hist::open_db(&db).expect("open db");
    conn.execute(
        "INSERT INTO sessions (session_id, source, cwd, first_activity_ms, last_activity_ms) \
         VALUES ('grown-elsewhere', 'claude', '/tmp/grown', 1, 2)",
        [],
    )
    .expect("insert a session");
    drop(conn);
    assert_eq!(
        session_count(&db),
        before + 1,
        "positive control: the row the skip below has to tolerate must exist"
    );

    assert!(
        sync_tick(&db, home.path(), false).skipped_unchanged(),
        "a destination that only grew must still skip the sweep"
    );

    // Positive control on the other side: the fast path is still capable of
    // reopening, so the skip above is a decision and not a dead end.
    append_claude_record(&transcript, "grow-2", 3);
    assert!(sync_tick(&db, home.path(), false).swept);
}

/// An upgrade that bumps a parser or state generation exists to re-read
/// sources whose bytes never changed. A stamp describing only those bytes
/// survives the upgrade and skips exactly that sweep, so the generation is
/// part of the stamp.
#[test]
fn a_stamp_from_another_parser_generation_does_not_skip_the_sweep() {
    let home = tempfile::tempdir().expect("tempdir");
    write_claude_transcript(home.path(), "proj", "gen-1", 1);
    let db = home.path().join("history.db");

    assert!(sync_tick(&db, home.path(), false).swept);
    assert!(
        sync_tick(&db, home.path(), false).skipped_unchanged(),
        "the fast path must be armed before the generation matters"
    );

    // Rewrite *only* the generation half of the stored stamp. The half
    // describing the sources stays byte-identical, and the destination marker
    // is untouched, so nothing else can explain the sweep below.
    let state_path = home.path().join(".sync-state.json");
    let mut state: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&state_path).expect("read sync state"))
            .expect("parse sync state");
    let stamp = state["source_fingerprint"]
        .as_str()
        .expect("a stored fingerprint")
        .to_string();
    let (generation, sources) = stamp
        .split_once('/')
        .expect("the stamp carries a generation and a source half");
    assert_ne!(generation, "g0000000000000000");
    state["source_fingerprint"] = serde_json::Value::from(format!("g0000000000000000/{sources}"));
    std::fs::write(
        &state_path,
        serde_json::to_vec(&state).expect("serialize sync state"),
    )
    .expect("write sync state");

    assert!(
        sync_tick(&db, home.path(), false).swept,
        "a stamp from another parser generation must not skip the sweep"
    );

    // Positive control: restoring this build's generation over the same source
    // half re-arms the fast path, which proves the segment replaced above is
    // the generation and not the source fingerprint.
    assert!(
        sync_tick(&db, home.path(), false).skipped_unchanged(),
        "the current generation's stamp must still arm the fast path"
    );
    let state: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&state_path).expect("read sync state"))
            .expect("parse sync state");
    assert_eq!(
        state["source_fingerprint"]
            .as_str()
            .and_then(|stamp| stamp.split_once('/'))
            .map(|(_, sources)| sources.to_string()),
        Some(sources.to_string()),
        "the source half must be unchanged across the re-sweep"
    );
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn sync_tick(db: &Path, home: &Path, force: bool) -> ai_hist::SyncTick {
    ai_hist::sync_tick_at_with_home(db, home, SyncOutput::Silent, force).expect("sync tick")
}

fn rehydrate(db: &Path, home: &Path, session_id: &str) -> ai_hist::HydrateSessionResult {
    ai_hist::hydrate_session_at_with_home(
        db,
        &ai_hist::HydrateSessionOptions {
            source: "claude".into(),
            session_id: session_id.into(),
            scope: ai_hist::SessionScope::Local,
            include_related: true,
        },
        home,
    )
    .expect("hydrate")
}

fn claude_record(session_id: &str, index: u64) -> String {
    let minute = index % 60;
    format!(
        "{{\"type\":\"user\",\"sessionId\":\"{session_id}\",\"cwd\":\"/tmp/{session_id}\",\
         \"timestamp\":\"2026-09-19T10:{minute:02}:00.000Z\",\
         \"message\":{{\"role\":\"user\",\"content\":\"prompt {index} for {session_id}\"}}}}\n"
    )
}

fn write_claude_transcript(home: &Path, project: &str, session_id: &str, records: u64) -> PathBuf {
    let dir = home.join(".claude/projects").join(project);
    std::fs::create_dir_all(&dir).expect("project dir");
    let path = dir.join(format!("{session_id}.jsonl"));
    let body = (1..=records)
        .map(|index| claude_record(session_id, index))
        .collect::<String>();
    std::fs::write(&path, body).expect("write transcript");
    path
}

fn append_claude_record(path: &Path, session_id: &str, index: u64) {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(path)
        .expect("open transcript");
    file.write_all(claude_record(session_id, index).as_bytes())
        .expect("append record");
    file.flush().expect("flush");
}

/// Append one record to a flat per-harness history log, creating it if needed.
/// The two harnesses spell a record differently: Claude's is
/// `{display, timestamp}`, Codex's is `{text, ts}`.
fn append_history_log(home: &Path, source: &str, prompt: &str) {
    use std::io::Write;
    let (dir, line) = match source {
        "claude" => (
            ".claude",
            format!("{{\"display\":\"{prompt}\",\"timestamp\":1758276000000}}\n"),
        ),
        "codex" => (
            ".codex",
            format!("{{\"text\":\"{prompt}\",\"ts\":1758276000}}\n"),
        ),
        other => panic!("no flat history log for {other}"),
    };
    let path = home.join(dir).join("history.jsonl");
    std::fs::create_dir_all(path.parent().expect("log dir")).expect("log dir");
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .expect("open history log");
    file.write_all(line.as_bytes()).expect("append record");
    file.flush().expect("flush");
}

fn tool_call_count(db: &Path, source: &str, session_id: &str) -> i64 {
    let conn = ai_hist::open_db(db).expect("open db");
    conn.query_row(
        "SELECT COUNT(*) FROM tool_calls WHERE source = ? AND session_id = ?",
        [source, session_id],
        |row| row.get(0),
    )
    .expect("count tool calls")
}

fn event_count(db: &Path) -> i64 {
    let conn = ai_hist::open_db(db).expect("open db");
    conn.query_row("SELECT COUNT(*) FROM session_events", [], |row| row.get(0))
        .expect("count session events")
}

fn session_count(db: &Path) -> i64 {
    let conn = ai_hist::open_db(db).expect("open db");
    conn.query_row("SELECT COUNT(*) FROM sessions", [], |row| row.get(0))
        .expect("count sessions")
}

fn codex_event_count(db: &Path, session_id: &str) -> i64 {
    let conn = ai_hist::open_db(db).expect("open db");
    conn.query_row(
        "SELECT COUNT(*) FROM session_events WHERE source = 'codex' AND session_id = ?",
        [session_id],
        |row| row.get(0),
    )
    .expect("count session events")
}

/// One finished Codex rollout: the shape whose bytes never change again, which
/// is why a lost row under a matching stamp is unrecoverable without a
/// destination check.
fn write_codex_rollout(home: &Path, session_id: &str) -> PathBuf {
    let day = home.join(".codex/sessions/2026/09/19");
    std::fs::create_dir_all(&day).expect("codex session dir");
    let path = day.join(format!("rollout-2026-09-19T10-00-00-{session_id}.jsonl"));
    let body = format!(
        "{}\n{}\n{}\n",
        format_args!(
            "{{\"timestamp\":\"2026-09-19T10:00:00.000Z\",\"type\":\"session_meta\",\
             \"payload\":{{\"id\":\"{session_id}\",\"cwd\":\"/tmp/{session_id}\"}}}}"
        ),
        "{\"timestamp\":\"2026-09-19T10:00:01.000Z\",\"type\":\"response_item\",\
         \"payload\":{\"type\":\"message\",\"role\":\"user\",\
         \"content\":[{\"type\":\"input_text\",\"text\":\"repair me\"}]}}",
        "{\"timestamp\":\"2026-09-19T10:00:02.000Z\",\"type\":\"event_msg\",\
         \"payload\":{\"type\":\"agent_message\",\"message\":\"Done.\"}}",
    );
    // One tool call, so the rollout carries structured evidence as well as
    // events: `tool_calls` is restored by the same re-read, and is guarded by
    // the same marker.
    let body = format!(
        "{body}{}\n",
        "{\"timestamp\":\"2026-09-19T10:00:03.000Z\",\"type\":\"response_item\",\
         \"payload\":{\"type\":\"function_call\",\"id\":\"fc_1\",\
         \"name\":\"exec_command\",\"arguments\":\"{\\\"cmd\\\":\\\"git status\\\"}\",\
         \"call_id\":\"call_1\"}}",
    );
    std::fs::write(&path, body).expect("write rollout");
    path
}

/// A parent transcript that delegates, and the subagent sidecar it spawned.
/// The child's events are recorded under its own agent id, which is exactly
/// the name the catalog never carries for it.
fn write_claude_delegation(home: &Path, parent: &str, child: &str) -> PathBuf {
    let project = home.join(".claude/projects/app");
    std::fs::create_dir_all(&project).expect("project dir");
    std::fs::write(
        project.join(format!("{parent}.jsonl")),
        format!(
            "{}\n{}\n",
            format_args!(
                "{{\"sessionId\":\"{parent}\",\"uuid\":\"u1\",\"cwd\":\"/work/app\",\
                 \"type\":\"user\",\"message\":{{\"role\":\"user\",\
                 \"content\":\"human prompt\"}},\"timestamp\":\"2026-09-19T11:00:00Z\"}}"
            ),
            format_args!(
                "{{\"sessionId\":\"{parent}\",\"uuid\":\"a1\",\"cwd\":\"/work/app\",\
                 \"type\":\"assistant\",\"message\":{{\"role\":\"assistant\",\
                 \"content\":[{{\"type\":\"tool_use\",\"id\":\"toolu_1\",\
                 \"name\":\"Agent\",\"input\":{{\"prompt\":\"plan it\"}}}}]}},\
                 \"timestamp\":\"2026-09-19T11:00:01Z\"}}"
            ),
        ),
    )
    .expect("write parent transcript");

    let subagents = project.join(parent).join("subagents");
    std::fs::create_dir_all(&subagents).expect("subagents dir");
    let sidecar = subagents.join(format!("agent-{child}.jsonl"));
    std::fs::write(
        &sidecar,
        format!(
            "{}\n{}\n{}\n",
            format_args!(
                "{{\"sessionId\":\"{parent}\",\"agentId\":\"{child}\",\
                 \"isSidechain\":true,\"uuid\":\"side-u\",\"cwd\":\"/work/app\",\
                 \"type\":\"user\",\"message\":{{\"role\":\"user\",\
                 \"content\":\"delegated instruction\"}},\
                 \"timestamp\":\"2026-09-19T11:00:02Z\"}}"
            ),
            format_args!(
                "{{\"sessionId\":\"{parent}\",\"agentId\":\"{child}\",\
                 \"isSidechain\":true,\"uuid\":\"side-a\",\"cwd\":\"/work/app\",\
                 \"type\":\"assistant\",\"message\":{{\"role\":\"assistant\",\
                 \"content\":\"child result\"}},\
                 \"timestamp\":\"2026-09-19T11:00:03Z\"}}"
            ),
            format_args!(
                "{{\"sessionId\":\"{parent}\",\"agentId\":\"{child}\",\
                 \"isSidechain\":true,\"uuid\":\"side-b\",\"cwd\":\"/work/app\",\
                 \"type\":\"assistant\",\"message\":{{\"role\":\"assistant\",\
                 \"content\":\"second child result\"}},\
                 \"timestamp\":\"2026-09-19T11:00:04Z\"}}"
            ),
        ),
    )
    .expect("write subagent transcript");
    std::fs::write(
        subagents.join(format!("agent-{child}.meta.json")),
        "{\"agentType\":\"Plan\",\"description\":\"plan the work\",\
         \"toolUseId\":\"toolu_1\",\"spawnDepth\":1,\"model\":\"opus\"}",
    )
    .expect("write subagent sidecar");
    sidecar
}

fn history_prompt_count(db: &Path, prompt: &str) -> i64 {
    let conn = ai_hist::open_db(db).expect("open db");
    conn.query_row(
        "SELECT COUNT(*) FROM history WHERE prompt = ?",
        [prompt],
        |row| row.get(0),
    )
    .expect("count history rows")
}

fn catalog_session_count(db: &Path, source: &str, session_id: &str) -> i64 {
    let conn = ai_hist::open_db(db).expect("open db");
    conn.query_row(
        "SELECT COUNT(*) FROM sessions WHERE source = ? AND session_id = ?",
        [source, session_id],
        |row| row.get(0),
    )
    .expect("count catalog rows")
}

fn session_event_count(db: &Path, session_id: &str) -> i64 {
    let conn = ai_hist::open_db(db).expect("open db");
    conn.query_row(
        "SELECT COUNT(*) FROM session_events WHERE source = 'claude' AND session_id = ?",
        [session_id],
        |row| row.get(0),
    )
    .expect("count session events")
}
