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

/// Owns the paths a [`ProviderRoots`] borrows.
///
/// `ProviderRoots` is a borrowed view, so a literal built from
/// `&home.join("..")` would not outlive the statement. This also keeps each
/// test honest about the distinction that matters: a provider root is
/// *configurable*, and only defaults to sitting under `$HOME`.
struct HomeLayout {
    home: PathBuf,
    claude: PathBuf,
    codex: PathBuf,
    grok: PathBuf,
    opencode_db: PathBuf,
}

impl HomeLayout {
    /// The default layout: every provider root where it falls under `$HOME`.
    fn under(home: &Path) -> Self {
        Self {
            home: home.to_path_buf(),
            claude: home.join(".claude"),
            codex: home.join(".codex"),
            grok: home.join(".grok"),
            opencode_db: home.join(".local/share/opencode/opencode.db"),
        }
    }

    fn roots(&self) -> ProviderRoots<'_> {
        ProviderRoots {
            home: &self.home,
            claude: &self.claude,
            codex: &self.codex,
            grok: &self.grok,
            opencode_db: &self.opencode_db,
        }
    }
}

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

    /// Wait until the loop has stopped ticking, so what follows is about the
    /// next write and nothing before it.
    ///
    /// One `fs::write` is not one filesystem event: creating a file yields a
    /// create *and* a modify, and an event landing after the debounce window
    /// has opened deliberately re-arms it, so a single write legitimately
    /// drives more than one forced tick. An assertion that nothing happens
    /// has to start from quiet, or it reads the previous write's second tick
    /// as the thing it was watching for.
    ///
    /// Bounded in both directions: each wait is several debounce windows, so
    /// a trailing event has time to arrive and be swept, and the whole settle
    /// has a deadline, so a loop that never goes quiet fails the test rather
    /// than hanging it.
    fn settle(&self, when: &str) {
        let deadline = std::time::Instant::now() + ARRIVES_WITHIN;
        while self.ticks.recv_timeout(Duration::from_millis(400)).is_ok() {
            assert!(
                std::time::Instant::now() < deadline,
                "the loop never went quiet {when}"
            );
        }
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

/// A change that lands while a manual tick holds the slot must not be lost.
///
/// The wake state is cleared when the debounce window opens, so by the time
/// the driver tries to claim the slot the event is no longer recorded
/// anywhere. Dropping the tick there — which is the right answer for a
/// backstop tick, and was being applied to both — loses a real change until
/// the next backstop, up to `--interval` later. A host that calls `tick()`
/// around its own work would silently stop capturing for that window.
#[test]
fn a_change_during_a_manual_tick_is_swept_when_it_finishes() {
    let gate = Arc::new(Gate::default());
    let (ticks, ticks_rx) = mpsc::channel();
    let (entered, entered_rx) = mpsc::channel();

    let gate_for_tick = gate.clone();
    let bodies = Arc::new(AtomicUsize::new(0));
    let bodies_for_tick = bodies.clone();
    let tick: TickFn = Arc::new(move |force| {
        let _ = ticks.send(force);
        // Only the first body blocks: that is the manual tick, held open
        // while the change below lands.
        if bodies_for_tick.fetch_add(1, Ordering::SeqCst) == 0 {
            let _ = entered.send(());
            gate_for_tick.wait();
        }
        Ok(TickOutcome::default())
    });
    let running = RunningLoop::start(
        |watch| {
            watch
                .with_immediate(false)
                .with_fs_events(false)
                // Long enough that a backstop tick cannot rescue the change.
                .with_poll_interval_ms(600_000)
                .with_slow_poll_ms(600_000)
                .with_debounce_ms(50)
        },
        tick,
    );

    let holder = {
        let watch = running.watch.clone();
        std::thread::spawn(move || watch.tick())
    };
    entered_rx
        .recv_timeout(ARRIVES_WITHIN)
        .expect("the manual tick never started");
    assert_eq!(
        ticks_rx.recv_timeout(ARRIVES_WITHIN),
        Ok(false),
        "the manual tick runs unforced"
    );

    // The change lands while the manual tick still owns the slot.
    running.watch.notify_change();
    assert_eq!(
        ticks_rx.recv_timeout(Duration::from_millis(400)),
        Err(RecvTimeoutError::Timeout),
        "nothing can run while the manual tick holds the slot"
    );

    gate.open();
    holder.join().expect("manual tick");

    assert_eq!(
        ticks_rx.recv_timeout(ARRIVES_WITHIN),
        Ok(true),
        "the change must drive a forced sweep as soon as the slot is free"
    );
    // Positive control: exactly one, not one per event and not a repeat — the
    // deferred signal is a single bit.
    assert_eq!(
        ticks_rx.recv_timeout(Duration::from_millis(400)),
        Err(RecvTimeoutError::Timeout),
        "one deferred change is one sweep"
    );
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
    let layout = HomeLayout::under(home.path());
    let roots = ai_hist::discover::watch_roots(&shallow_providers(), &layout.roots());
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
    let layout = HomeLayout::under(&home);
    let opencode_db = layout.opencode_db.clone();
    let roots = ai_hist::discover::watch_roots(&shallow_providers(), &layout.roots());

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

/// A relocated provider root moves the watch with it.
///
/// `CLAUDE_CONFIG_DIR`, `CODEX_HOME` and `GROK_HOME` already move what a sweep
/// reads. A watch root rebuilt from `$HOME` instead would leave live capture
/// staring at a directory the provider never writes to: every sweep correct,
/// every one of them waiting out the backstop.
#[test]
fn configured_provider_roots_move_the_watch_roots() {
    let home = PathBuf::from("/tmp/relayhistory-relocated-home");
    let layout = HomeLayout {
        home: home.clone(),
        claude: PathBuf::from("/tmp/relayhistory-relocated/claude"),
        codex: PathBuf::from("/tmp/relayhistory-relocated/codex"),
        grok: PathBuf::from("/tmp/relayhistory-relocated/grok"),
        opencode_db: PathBuf::from("/tmp/relayhistory-relocated/opencode/opencode.db"),
    };
    let roots = ai_hist::discover::watch_roots(&shallow_providers(), &layout.roots());

    for expected in [
        layout.claude.join("projects"),
        layout.codex.join("sessions"),
        layout.codex.join("archived_sessions"),
        layout.grok.join("sessions"),
        layout
            .opencode_db
            .parent()
            .expect("opencode dir")
            .to_path_buf(),
    ] {
        assert!(
            roots.iter().any(|root| root.path == expected),
            "{expected:?} missing from {roots:?}"
        );
    }
    assert!(
        !roots
            .iter()
            .any(|root| root.path.starts_with(home.join(".claude"))
                || root.path.starts_with(home.join(".codex"))
                || root.path.starts_with(home.join(".grok"))),
        "a configured root must not leave a $HOME-relative watch behind: {roots:?}"
    );
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

    // The flat logs are watched as the files they are: registered through
    // their parent, because a watch on the file itself follows an inode the
    // harness may replace, but filtered back down to the one name so the
    // entries beside them do not each force a sweep.
    for (expected, depth) in [
        (home.join(".claude/history.jsonl"), WatchDepth::File),
        (home.join(".codex/history.jsonl"), WatchDepth::File),
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

/// A root reached through a symlink has two spellings, and the backends do not
/// agree on which one they report: inotify echoes whichever path the watch was
/// registered with, macOS FSEvents always reports the resolved one. A root that
/// held only the spelling it was given would register, be reported as watched,
/// and never match an event — the failure that looks most like everything
/// working.
///
/// End to end through a real watcher: the root is spelled through the symlink,
/// the registration resolves it, and the events that come back carry the
/// resolved spelling.
#[cfg(all(feature = "fs-events", unix))]
#[test]
fn a_root_reached_through_a_symlink_matches_its_own_events() {
    let dir = tempfile::tempdir().expect("tempdir");
    let real = dir.path().join("real/sessions");
    std::fs::create_dir_all(&real).expect("real root");
    let link = dir.path().join("link");
    std::os::unix::fs::symlink(dir.path().join("real"), &link).expect("symlink");
    let through_link = link.join("sessions");

    let running = RunningLoop::reporting({
        let through_link = through_link.clone();
        move |watch| {
            watch
                .with_immediate(false)
                .with_fs_events(true)
                .with_roots(vec![ai_hist::discover::WatchRoot::tree(through_link)])
                .with_debounce_ms(100)
                .with_poll_interval_ms(600_000)
                .with_slow_poll_ms(600_000)
        }
    });
    let status = running.watch.status().expect("status");
    assert_eq!(
        status.driver,
        WatchDriver::FsEvents,
        "a symlinked root still attaches: {status:?}"
    );

    // Written through the *real* path, which is the spelling the resolved
    // registration reports back.
    std::fs::write(real.join("rollout.jsonl"), "{}\n").expect("write through the real path");
    assert_eq!(
        running.next_tick(),
        Ok(true),
        "an event under the symlinked root's real path must drive a forced sweep"
    );

    // Positive control: the filter is still a filter. A sibling of the root,
    // not under it, drives nothing — asked from quiet, because the write
    // above was a *create* and produced two events, and its second tick would
    // otherwise be read as this one's. That is how this test failed in CI at
    // `4f2a1b9` while passing locally.
    running.settle("after the write under the root");
    std::fs::write(dir.path().join("real/unrelated.jsonl"), "{}\n").expect("write beside it");
    assert_eq!(
        running.ticks.recv_timeout(Duration::from_millis(800)),
        Err(RecvTimeoutError::Timeout),
        "a write outside the root must not drive a sweep, whichever spelling it arrives in"
    );
}

/// A symlinked root can be *retargeted*, and that is not the same event as a
/// directory being replaced. `~/sessions -> /disk-a/sessions` registers at
/// `/disk-a/sessions`; repoint it at `/disk-b/sessions` and the old directory
/// is still there with the same inode, so anything that asks the *resolved*
/// path whether it changed is told no, forever. The loop would go on watching
/// a directory the name no longer means, and every transcript written to the
/// new target would wait for the backstop — which for a session that is
/// cleaned up on exit means it is never captured at all.
#[cfg(all(feature = "fs-events", unix))]
#[test]
fn a_retargeted_symlink_root_is_watched_at_its_new_target() {
    let dir = tempfile::tempdir().expect("tempdir");
    let first = dir.path().join("disk-a/sessions");
    let second = dir.path().join("disk-b/sessions");
    std::fs::create_dir_all(&first).expect("first target");
    std::fs::create_dir_all(&second).expect("second target");
    let link = dir.path().join("sessions");
    std::os::unix::fs::symlink(&first, &link).expect("symlink");

    let running = RunningLoop::reporting({
        let link = link.clone();
        move |watch| {
            watch
                .with_immediate(false)
                .with_fs_events(true)
                .with_roots(vec![ai_hist::discover::WatchRoot::tree(link)])
                .with_debounce_ms(100)
                // Short, because the backstop tick is what reconciles — but
                // both cadences are the same, so an unforced tick is never
                // mistaken for an event-driven one below.
                .with_slow_poll_ms(200)
                .with_poll_interval_ms(200)
        }
    });
    assert_eq!(running.watch.driver(), Some(WatchDriver::FsEvents));

    // Baseline: the root works where it points now.
    std::fs::write(first.join("rollout-1.jsonl"), "{}\n").expect("write under the first target");
    forced_tick(&running, "a write under the original target");

    // Retarget. The old directory keeps existing, with the same inode — which
    // is exactly why asking it whether anything changed cannot work.
    settle_forced(&running, "after the write under the original target");
    std::fs::remove_file(&link).expect("drop the symlink");
    std::os::unix::fs::symlink(&second, &link).expect("retarget the symlink");

    // Reconciliation happens on the backstop, so wait for the loop's own
    // ticks rather than for a duration: two of them have passed by the time
    // this returns, and reconciliation runs before each. Writing before that
    // would test nothing — the file would already exist by the time the new
    // target was registered, and no event would ever be produced for it.
    backstop_ticks(&running, 2, "after retargeting the symlink");
    std::fs::write(second.join("rollout-2.jsonl"), "{}\n").expect("write under the new target");
    forced_tick(&running, "a write under the retargeted symlink");

    // Positive control: the old target is no longer what the name means, so
    // writing there drives nothing. Asked from quiet, because the write above
    // was a create and produced more than one event.
    settle_forced(&running, "after the write under the new target");
    std::fs::write(first.join("rollout-3.jsonl"), "{}\n").expect("write under the old target");
    no_forced_tick(
        &running,
        Duration::from_millis(800),
        "the directory the root no longer points at must not drive a sweep",
    );
}

/// What macOS FSEvents does, asserted on a platform that cannot do it.
///
/// FSEvents reports the real path — `/private/var/…` for anything under
/// `/var`, and the resolved target of any symlink on the way — whatever
/// spelling the watch was registered with. No Linux backend produces that, so
/// this is the one place a hand-written event path is the honest test rather
/// than a shortcut: it encodes the other platform's behaviour. The test above
/// is the end-to-end half.
#[cfg(unix)]
#[test]
fn a_root_matches_an_event_reported_under_its_resolved_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let real = dir.path().join("real/sessions");
    std::fs::create_dir_all(&real).expect("real root");
    let link = dir.path().join("link");
    std::os::unix::fs::symlink(dir.path().join("real"), &link).expect("symlink");

    let mut root = ai_hist::discover::WatchRoot::tree(link.join("sessions"));
    assert!(
        !root.covers(&real.join("rollout.jsonl")),
        "before resolution the root knows only the spelling it was given"
    );
    root.resolve();

    let resolved = std::fs::canonicalize(&real).expect("canonical root");
    assert_eq!(root.canonical.as_deref(), Some(resolved.as_path()));
    assert!(
        root.covers(&resolved.join("rollout.jsonl")),
        "an event reported under the resolved path is this root's event"
    );
    // Both spellings, not one instead of the other: inotify still reports the
    // registered one.
    assert!(root.covers(&link.join("sessions/rollout.jsonl")));
    // Positive control: resolving did not widen the root. A sibling of the
    // resolved directory is still outside it. Spelled without a `..`, because
    // `covers` compares lexically and an event path is normalised before it
    // gets here — `sessions/../unrelated.jsonl` does start with `sessions`.
    assert!(!root.covers(
        &resolved
            .parent()
            .expect("the resolved root has a parent")
            .join("unrelated.jsonl")
    ));
    assert!(root.registers_at(&resolved));
    assert!(root.registers_at(&link.join("sessions")));
}

/// `pending` means "not watched yet, and being retried". A loop with no
/// filesystem backend at all — `--no-fsevents`, a build without the feature, a
/// watcher that could not be brought up — has nothing to reconcile, so nothing
/// will ever promote those roots; and they are not uncovered either, because
/// polling reads every one of them at `--interval`. Listing them as pending
/// promises a retry that cannot happen, and the CLI renders that promise as
/// "not watched yet (retried every 30s)".
#[test]
fn a_polling_loop_reports_no_pending_roots() {
    let home = tempfile::tempdir().expect("tempdir");
    let root = home.path().join(".claude/projects");
    std::fs::create_dir_all(&root).expect("an existing root");

    let running = RunningLoop::reporting({
        let root = root.clone();
        move |watch| {
            watch
                .with_immediate(false)
                .with_fs_events(false)
                .with_roots(vec![ai_hist::discover::WatchRoot::tree(root)])
                .with_debounce_ms(50)
                .with_poll_interval_ms(600_000)
                .with_slow_poll_ms(600_000)
        }
    });

    let status = running.watch.status().expect("status");
    assert_eq!(
        status.driver,
        WatchDriver::Polling,
        "a loop asked not to use filesystem events polls"
    );
    assert!(
        status.pending.is_empty(),
        "a polling loop has no root waiting to be attached: {status:?}"
    );
    assert!(
        status.watched.is_empty(),
        "and none attached either, since there is no watcher: {status:?}"
    );
}

/// The other side of the same distinction: with filesystem events *enabled*, a
/// root that does not exist is genuinely uncovered and genuinely retried, so
/// it must still be reported as pending. This is the control that keeps the
/// fix above from being "never report anything".
#[cfg(feature = "fs-events")]
#[test]
fn a_root_that_does_not_exist_yet_is_still_reported_pending() {
    let home = tempfile::tempdir().expect("tempdir");
    let absent = home.path().join(".claude/projects");

    let running = RunningLoop::reporting({
        let absent = absent.clone();
        move |watch| {
            watch
                .with_immediate(false)
                .with_fs_events(true)
                .with_roots(vec![ai_hist::discover::WatchRoot::tree(absent)])
                .with_debounce_ms(50)
                .with_poll_interval_ms(600_000)
                .with_slow_poll_ms(600_000)
        }
    });

    let status = running.watch.status().expect("status");
    assert_eq!(status.driver, WatchDriver::Polling);
    assert_eq!(
        status.pending,
        vec![absent],
        "a root that does not exist yet is retried, and says so: {status:?}"
    );
}

/// Re-deriving the root set walks every project tree, so it belongs on the
/// backstop and nowhere else. A local `watch` always installs a refresher, so
/// shortening that cadence to `--interval` meant `--interval 1` re-scanned the
/// projects once a second on a loop that is not polling for changes at all —
/// the opposite of what a short interval asks for, and the expense the whole
/// fingerprint design exists to avoid.
#[cfg(feature = "fs-events")]
#[test]
fn an_attached_loop_re_derives_its_roots_on_the_backstop_not_the_interval() {
    let home = tempfile::tempdir().expect("tempdir");
    let root = home.path().join(".claude/projects");
    std::fs::create_dir_all(&root).expect("an existing root");

    let refreshes = Arc::new(AtomicUsize::new(0));
    let counted = refreshes.clone();
    let running = RunningLoop::reporting({
        let root = root.clone();
        move |watch| {
            watch
                .with_immediate(false)
                .with_fs_events(true)
                .with_roots(vec![ai_hist::discover::WatchRoot::tree(root.clone())])
                .with_roots_refresh(Arc::new(move || {
                    counted.fetch_add(1, Ordering::SeqCst);
                    vec![ai_hist::discover::WatchRoot::tree(root.clone())]
                }))
                .with_debounce_ms(50)
                // The backstop, and a much shorter user interval beside it.
                .with_slow_poll_ms(600)
                .with_poll_interval_ms(50)
        }
    });
    assert_eq!(
        running.watch.driver(),
        Some(WatchDriver::FsEvents),
        "the root exists, so the loop is event-driven"
    );

    // Long enough for ~20 wakes at the user's interval and ~2 at the backstop.
    // Measured by waiting on the loop's own ticks rather than by sleeping:
    // two backstop ticks have passed by the time this returns.
    backstop_ticks(&running, 2, "while watching an attached root");
    let attached = refreshes.load(Ordering::SeqCst);
    assert!(
        attached <= 4,
        "an attached loop re-derived its roots {attached} times across two backstops; \
         the user's interval is not the cadence for that work"
    );

    // Positive control: while *polling* — nothing attached, a root that does
    // not exist yet — the refresher is exactly how a new root is found, and
    // there the short interval is the right cadence. Same cadences, opposite
    // expectation.
    drop(running);
    let absent = home.path().join(".codex/sessions");
    let polling_refreshes = Arc::new(AtomicUsize::new(0));
    let counted = polling_refreshes.clone();
    let running = RunningLoop::reporting({
        let absent = absent.clone();
        move |watch| {
            watch
                .with_immediate(false)
                .with_fs_events(true)
                .with_roots(vec![ai_hist::discover::WatchRoot::tree(absent.clone())])
                .with_roots_refresh(Arc::new(move || {
                    counted.fetch_add(1, Ordering::SeqCst);
                    vec![ai_hist::discover::WatchRoot::tree(absent.clone())]
                }))
                .with_debounce_ms(50)
                .with_slow_poll_ms(600)
                .with_poll_interval_ms(50)
        }
    });
    assert_eq!(running.watch.driver(), Some(WatchDriver::Polling));
    let deadline = std::time::Instant::now() + ARRIVES_WITHIN;
    while polling_refreshes.load(Ordering::SeqCst) < 5 {
        assert!(
            std::time::Instant::now() < deadline,
            "a polling loop with an uncovered root must consult the refresher at \
             the shorter cadence, not wait out the backstop"
        );
        std::thread::yield_now();
    }
}

/// The flat logs are single files, and the directory holding one is full of
/// things a sweep never reads — `~/.claude/settings.json`, the credentials
/// file, whatever the next harness release adds. Watching the parent as a
/// *directory* root makes every one of those writes a forced sweep, which is
/// the expensive kind that bypasses the fingerprint.
#[cfg(feature = "fs-events")]
#[test]
fn a_file_beside_the_flat_log_does_not_force_a_sweep() {
    let home = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir_all(home.path().join(".claude")).expect("claude dir");
    let log = home.path().join(".claude/history.jsonl");
    std::fs::write(&log, "{}\n").expect("seed the flat log");

    let running = RunningLoop::reporting({
        let home = home.path().to_path_buf();
        move |watch| {
            watch
                .with_immediate(false)
                .with_fs_events(true)
                .with_roots(ai_hist::sync_watch_roots(
                    &home,
                    &home.join("no-opencode.db"),
                ))
                .with_debounce_ms(100)
                .with_poll_interval_ms(600_000)
                .with_slow_poll_ms(600_000)
        }
    });
    assert_eq!(running.watch.driver(), Some(WatchDriver::FsEvents));

    // A neighbour of the flat log that the sweep never reads.
    std::fs::write(home.path().join(".claude/settings.json"), "{}\n").expect("write a neighbour");
    assert_eq!(
        running.ticks.recv_timeout(Duration::from_millis(800)),
        Err(RecvTimeoutError::Timeout),
        "a file beside the flat log must not force a sweep"
    );

    // Positive control: the flat log itself still does, so the narrowing did
    // not simply stop watching it.
    running.settle("before appending to the flat log");
    append_history_log(home.path(), "claude", "a new prompt");
    assert_eq!(
        running.next_tick(),
        Ok(true),
        "the flat log itself must still drive a forced sweep"
    );
}

/// A watch lost to a deleted directory has to come back *now*, not at the next
/// backstop. `~/.codex/sessions` is removed and recreated, a ten-second session
/// writes its rollout there and cleanup takes it away again: with the default
/// 30 s backstop, the registration is dead for the whole of that session and
/// the sweep afterwards finds nothing. The backend says a watched path was
/// removed exactly once, so that report has to be acted on when it arrives.
#[cfg(feature = "fs-events")]
#[test]
fn a_root_recreated_between_backstops_is_watched_without_waiting_for_one() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().join("sessions");
    std::fs::create_dir_all(&root).expect("root");

    let refreshes = Arc::new(AtomicUsize::new(0));
    let counted = refreshes.clone();
    let running = RunningLoop::reporting({
        let root = root.clone();
        move |watch| {
            watch
                .with_immediate(false)
                .with_fs_events(true)
                .with_roots(vec![ai_hist::discover::WatchRoot::tree(root.clone())])
                .with_roots_refresh(Arc::new(move || {
                    counted.fetch_add(1, Ordering::SeqCst);
                    vec![ai_hist::discover::WatchRoot::tree(root.clone())]
                }))
                .with_debounce_ms(100)
                // Ten minutes: nothing in this test may depend on a backstop
                // tick, which is the whole point.
                .with_poll_interval_ms(600_000)
                .with_slow_poll_ms(600_000)
        }
    });
    assert_eq!(running.watch.driver(), Some(WatchDriver::FsEvents));

    // Positive control first: the watch works, so a failure later is the
    // recreate and not a watcher that never ran.
    std::fs::write(root.join("rollout-1.jsonl"), "{}\n").expect("write under the root");
    assert_eq!(
        running.next_tick(),
        Ok(true),
        "a write under the original directory must drive a forced sweep"
    );

    // The session's directory is replaced, and the writing starts immediately
    // afterwards — long before any backstop.
    running.settle("after the write under the original directory");
    std::fs::remove_dir_all(&root).expect("remove the root");

    // Wait for the loop to *notice* before recreating, which is what pins the
    // two halves of this down separately. Reaching `pending` at all means the
    // removal was acted on when it was reported rather than at the backstop,
    // ten minutes away; recreating only afterwards means the directory that
    // comes back has to be picked up by the retry rather than by the same
    // reconcile — the ordering a session's cleanup-then-restart produces, and
    // the one that needs the recovery cadence.
    let deadline = std::time::Instant::now() + ARRIVES_WITHIN;
    while !running
        .watch
        .status()
        .expect("status")
        .pending
        .contains(&root)
    {
        assert!(
            std::time::Instant::now() < deadline,
            "a removed root was not noticed until the backstop: {:?}",
            running.watch.status()
        );
        std::thread::yield_now();
    }
    std::fs::create_dir_all(&root).expect("recreate the root");

    // From quiet: the removal itself matched the root and drove a tick of its
    // own, and reading that one as the write's would prove nothing. This is
    // the trap rounds 7 and 8 fell into.
    running.settle("after recreating the directory");
    std::fs::write(root.join("rollout-2.jsonl"), "{}\n").expect("write under the new root");
    assert_eq!(
        running.next_tick(),
        Ok(true),
        "a recreated directory must be watched again without waiting for a backstop: {:?}",
        running.watch.status()
    );

    // And the prompt path must not have dragged the expensive half of the
    // backstop's work along with it: re-deriving the root set walks every
    // project tree, and nothing here should have asked for that.
    assert_eq!(
        refreshes.load(Ordering::SeqCst),
        0,
        "recovering a lost registration must not re-derive the root set"
    );
}

/// The recovery cadence is per *root*, not one flag over all of them.
///
/// `pending` holds two kinds of root that look alike and are not: one that was
/// attached and lost its directory, which has to come back in milliseconds,
/// and one that has never existed — `~/.claude/projects` on a machine where
/// Claude is not installed — which may never come back at all. A single
/// "recovering" flag cleared only when `pending` empties conflates them: one
/// recreate anywhere pins the loop to the 250 ms recovery cadence for the rest
/// of the run, stat-ing every root four times a second forever, on exactly the
/// long-lived `watch --interval 3600` the backstop exists to keep cheap.
#[cfg(feature = "fs-events")]
#[test]
fn a_recovered_root_does_not_hold_the_recovery_cadence_open() {
    let dir = tempfile::tempdir().expect("tempdir");
    let lost = dir.path().join("sessions");
    std::fs::create_dir_all(&lost).expect("the root that will be lost");
    // Not created until the last phase: this provider is not installed, which
    // is the ordinary state of half the roots on a real machine.
    let never = dir.path().join("projects");

    let refreshes = Arc::new(AtomicUsize::new(0));
    let counted = refreshes.clone();
    let running = RunningLoop::reporting({
        let lost = lost.clone();
        let never = never.clone();
        move |watch| {
            watch
                .with_immediate(false)
                .with_fs_events(true)
                .with_roots(vec![
                    ai_hist::discover::WatchRoot::tree(lost.clone()),
                    ai_hist::discover::WatchRoot::tree(never.clone()),
                ])
                // Only the backstop calls this, so it doubles as the test's
                // view of where the backstop boundary is — which is what lets
                // the windows below be measured from a known starting point
                // rather than from wherever the loop happened to be.
                .with_roots_refresh(Arc::new(move || {
                    counted.fetch_add(1, Ordering::SeqCst);
                    vec![
                        ai_hist::discover::WatchRoot::tree(lost.clone()),
                        ai_hist::discover::WatchRoot::tree(never.clone()),
                    ]
                }))
                .with_debounce_ms(50)
                // The backstop, far enough above the 250 ms recovery cadence
                // that a window can be several times one and a fraction of the
                // other.
                .with_slow_poll_ms(3_000)
                // The user's interval, an hour in miniature.
                .with_poll_interval_ms(600_000)
        }
    });
    assert_eq!(running.watch.driver(), Some(WatchDriver::FsEvents));
    assert_eq!(
        running.watch.status().expect("status").pending,
        vec![never.clone()],
        "the root that has never existed is the one pending"
    );

    // Phase 1, the positive control: a root that *was* attached is retried on
    // the recovery cadence, not on the backstop.
    std::fs::remove_dir_all(&lost).expect("remove the attached root");
    await_status(
        &running,
        "a removed root was never reported pending",
        |status| status.pending.contains(&lost),
    );
    // From a backstop boundary, so the re-attach below cannot be the backstop
    // arriving early.
    await_refresh(&refreshes, "before recreating the lost root");
    std::fs::create_dir_all(&lost).expect("recreate the lost root");
    await_status_within(
        &running,
        Duration::from_millis(1_200),
        "a root lost and recreated was not retried on the recovery cadence",
        |status| status.watched.contains(&lost),
    );

    // Phase 2, the finding: the recovery is over — every root that was
    // attached is attached again — so the loop must be back on the backstop,
    // even though the root that never existed is still pending.
    await_refresh(&refreshes, "before installing the late provider");
    std::fs::create_dir_all(&never).expect("install the late provider");
    let until = std::time::Instant::now() + Duration::from_millis(1_200);
    while std::time::Instant::now() < until {
        assert!(
            !running
                .watch
                .status()
                .expect("status")
                .watched
                .contains(&never),
            "a root that never attached held the recovery cadence open after \
             another root recovered: {:?}",
            running.watch.status()
        );
        std::thread::sleep(Duration::from_millis(25));
    }

    // And the control on that negative: the slower cadence still attaches it,
    // so the assertion above is about *when* and not about never.
    await_status(
        &running,
        "the late provider was never attached at all",
        |status| status.watched.contains(&never),
    );
}

/// A root adopted before its directory exists is a hole in the loop's
/// coverage, and the caller has to be told about it.
///
/// `adopt` is the only thing that knows a root is new; `reconcile` has nothing
/// to say about one it cannot attach. Publishing only on `reconcile`'s count
/// means `ai-hist watch --status` reports full coverage — no pending roots at
/// all — for a provider the loop has taken on and cannot yet watch, which is
/// precisely the state a user checks the status to find.
#[cfg(feature = "fs-events")]
#[test]
fn a_root_adopted_before_it_exists_is_reported_pending() {
    let dir = tempfile::tempdir().expect("tempdir");
    let present = dir.path().join("sessions");
    std::fs::create_dir_all(&present).expect("the root that is there from the start");
    let late = dir.path().join("projects");
    let arrives = dir.path().join("rollouts");

    let stage = Arc::new(AtomicUsize::new(0));
    let staged = stage.clone();
    let running = RunningLoop::reporting({
        let present = present.clone();
        let late = late.clone();
        let arrives = arrives.clone();
        move |watch| {
            let first = present.clone();
            watch
                .with_immediate(false)
                .with_fs_events(true)
                .with_roots(vec![ai_hist::discover::WatchRoot::tree(first)])
                .with_roots_refresh(Arc::new(move || {
                    let mut roots = vec![ai_hist::discover::WatchRoot::tree(present.clone())];
                    if staged.load(Ordering::SeqCst) >= 1 {
                        roots.push(ai_hist::discover::WatchRoot::tree(late.clone()));
                    }
                    if staged.load(Ordering::SeqCst) >= 2 {
                        roots.push(ai_hist::discover::WatchRoot::tree(arrives.clone()));
                    }
                    roots
                }))
                .with_debounce_ms(50)
                .with_slow_poll_ms(300)
                .with_poll_interval_ms(600_000)
        }
    });
    assert_eq!(running.watch.driver(), Some(WatchDriver::FsEvents));
    assert!(
        running.watch.status().expect("status").pending.is_empty(),
        "nothing is pending while the only root is attached: {:?}",
        running.watch.status()
    );

    // The finding: a root the refresher introduced, whose directory is not
    // there yet.
    stage.store(1, Ordering::SeqCst);
    await_status(
        &running,
        "a root adopted before its directory existed was never reported pending",
        |status| status.pending.contains(&late),
    );

    // Positive control: a root the refresher introduces that *does* exist is
    // published as watched, so the publish above is about coverage and not
    // about publishing everything twice.
    std::fs::create_dir_all(&arrives).expect("the root that is there when adopted");
    stage.store(2, Ordering::SeqCst);
    await_status(
        &running,
        "a root adopted after its directory existed was never reported watched",
        |status| status.watched.contains(&arrives),
    );
}

/// A lost registration has to be acted on when it is reported, including when
/// the loop is inside its debounce window — which, on the machine live capture
/// exists for, is where the loop spends most of its time.
///
/// The debounce sleep honours only the stop signal, so a removal landing
/// inside the window waits out the rest of it *and* the forced sweep that
/// follows before the registration is put back. That is the whole of the gap a
/// session's `rm -rf ~/.codex/sessions` and immediate restart occupies, so the
/// prompt-recovery fix did not cover the case it was written for on a busy
/// tree.
///
/// Every sweep here is held on a permit, so the assertion is about ordering
/// rather than about speed: with the sweep blocked, a loop that reconciles
/// only after sweeping never reconciles at all.
#[cfg(feature = "fs-events")]
#[test]
fn a_registration_lost_inside_the_debounce_window_is_put_back_before_the_sweep() {
    let dir = tempfile::tempdir().expect("tempdir");
    let busy = dir.path().join("busy");
    let root = dir.path().join("sessions");
    std::fs::create_dir_all(&busy).expect("the busy root");
    std::fs::create_dir_all(&root).expect("the root that will be lost");

    let (ticks, ticks_rx) = mpsc::channel();
    let (entered, entered_rx) = mpsc::channel();
    let (permits, permits_rx) = mpsc::channel::<()>();
    let permits_rx = Arc::new(Mutex::new(permits_rx));
    let armed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let armed_for_tick = armed.clone();
    let tick: TickFn = Arc::new(move |force| {
        let _ = ticks.send(force);
        if armed_for_tick.load(Ordering::SeqCst) {
            let _ = entered.send(());
            // Bounded, and generously: a permit that never comes must fail
            // the assertion that is waiting for it, not wedge the teardown
            // that follows the failure.
            let _ = permits_rx
                .lock()
                .expect("permits")
                .recv_timeout(Duration::from_secs(25));
        }
        Ok(TickOutcome::default())
    });
    let running = RunningLoop::start(
        {
            let busy = busy.clone();
            let root = root.clone();
            move |watch| {
                watch
                    .with_immediate(false)
                    .with_fs_events(true)
                    .with_roots(vec![
                        ai_hist::discover::WatchRoot::tree(busy),
                        ai_hist::discover::WatchRoot::tree(root),
                    ])
                    // The debounce window, long enough to hold a removal and
                    // short enough to keep the test quick.
                    .with_debounce_ms(1_500)
                    // Nothing here may depend on a backstop tick.
                    .with_poll_interval_ms(600_000)
                    .with_slow_poll_ms(600_000)
            }
        },
        tick,
    );
    assert_eq!(running.watch.driver(), Some(WatchDriver::FsEvents));

    // Positive control: the watch on the root works before anything is taken
    // away from it.
    std::fs::write(root.join("rollout-1.jsonl"), "{}\n").expect("write under the root");
    assert_eq!(
        ticks_rx.recv_timeout(ARRIVES_WITHIN),
        Ok(true),
        "a write under the root must drive a forced sweep"
    );

    // Put the loop somewhere known: inside a sweep, blocked on a permit.
    armed.store(true, Ordering::SeqCst);
    std::fs::write(busy.join("busy-1.jsonl"), "{}\n").expect("write under the busy root");
    entered_rx
        .recv_timeout(ARRIVES_WITHIN)
        .expect("the gated sweep never started");
    // Releasing it returns the loop to the wait with nothing pending, so the
    // next write is what opens the next debounce window.
    permits.send(()).expect("release the gated sweep");
    std::fs::write(busy.join("busy-2.jsonl"), "{}\n").expect("write under the busy root again");
    // Positioning only, a fifth of the window: the removal below has to land
    // inside it, and nothing is asserted about this interval.
    std::thread::sleep(Duration::from_millis(300));

    std::fs::remove_dir_all(&root).expect("remove the watched root");

    // The finding. Bounded by less than the window that is left, so a loop
    // that waits the window out fails here even though the sweep after it
    // would have reconciled; and the sweep after it is blocked on a permit
    // that never comes, so a loop that reconciles only after sweeping fails
    // here too.
    let deadline = std::time::Instant::now() + Duration::from_millis(600);
    while !running
        .watch
        .status()
        .expect("status")
        .pending
        .contains(&root)
    {
        assert!(
            std::time::Instant::now() < deadline,
            "a registration lost inside the debounce window waited for the \
             window and the sweep after it: {:?}",
            running.watch.status()
        );
        std::thread::yield_now();
    }

    // And the coverage that follows is real: recreated, re-attached on the
    // recovery cadence, and a write under it drives a sweep again.
    std::fs::create_dir_all(&root).expect("recreate the root");
    for _ in 0..16 {
        let _ = permits.send(());
    }
    armed.store(false, Ordering::SeqCst);
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
            "the recreated root was never watched again: {:?}",
            running.watch.status()
        );
        std::thread::yield_now();
    }
    while ticks_rx.recv_timeout(Duration::from_millis(2_200)).is_ok() {}
    std::fs::write(root.join("rollout-2.jsonl"), "{}\n").expect("write under the new root");
    assert_eq!(
        ticks_rx.recv_timeout(ARRIVES_WITHIN),
        Ok(true),
        "the re-attached root must drive a forced sweep: {:?}",
        running.watch.status()
    );
}

/// A sweep another process's sync lock turned away is not a completed tick.
///
/// `sync_exclusive_with_home` returns `SyncTick::default()` when it cannot
/// take the lock — `attempted: false`, `swept: false` — and a loop that reads
/// that as "ran, nothing to do" consumes the filesystem event with it. The
/// lock holder is no guarantee of cover: a manual `ai-hist sync` that has
/// already walked past `~/.claude/projects` holds the lock while a session
/// writes there, and a short-lived transcript is removed again long before the
/// 30 s backstop. Nothing ever reads it.
///
/// End to end, because the shape of this bug is that every layer reports
/// success: a real sync against a real database, the real advisory lock held
/// from a second handle the way a concurrent sync holds it, and a real
/// watcher event.
#[cfg(all(feature = "fs-events", unix))]
#[test]
fn a_sweep_turned_away_by_another_sync_is_retried_not_dropped() {
    use std::os::unix::io::AsRawFd;

    let home = tempfile::tempdir().expect("tempdir");
    let db = home.path().join("history.db");
    let root = home.path().join(".claude/projects");
    std::fs::create_dir_all(&root).expect("the provider root");

    // The lock the sync path takes, held here from a second handle.
    let lock_path = home.path().join("history.db.sync.lock");
    let held = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .expect("open the sync lock");
    assert_eq!(
        unsafe { libc::flock(held.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
        0,
        "the test must be able to hold the sync lock"
    );

    let (ticks, ticks_rx) = mpsc::channel();
    let tick: TickFn = {
        let db = db.clone();
        let home = home.path().to_path_buf();
        Arc::new(move |force| {
            // Exactly what the CLI's watch tick does.
            let tick = ai_hist::sync_tick_at_with_home(&db, &home, SyncOutput::Silent, force)?;
            let _ = ticks.send(tick);
            Ok(ai_hist::watch::TickOutcome::from(tick))
        })
    };
    let running = RunningLoop::start(
        {
            let root = root.clone();
            move |watch| {
                watch
                    .with_immediate(false)
                    .with_fs_events(true)
                    .with_roots(vec![ai_hist::discover::WatchRoot::tree(root)])
                    .with_debounce_ms(100)
                    // Ten minutes each: any sweep after the lock is released
                    // can only have come from the retry.
                    .with_poll_interval_ms(600_000)
                    .with_slow_poll_ms(600_000)
            }
        },
        tick,
    );
    assert_eq!(running.watch.driver(), Some(WatchDriver::FsEvents));

    // A session writes while the other sync holds the lock.
    write_claude_transcript(home.path(), "proj", "contended-session", 2);
    let first = ticks_rx
        .recv_timeout(ARRIVES_WITHIN)
        .expect("the write drove no sweep at all");
    assert_eq!(
        (first.attempted, first.swept),
        (false, false),
        "the positive control on the lock: the sweep this event drove was turned away"
    );
    assert!(
        !db.exists(),
        "a turned-away sweep must not have touched the database"
    );

    // The other sync finishes. Nothing else can wake the loop now: both
    // intervals are ten minutes away.
    drop(held);

    let deadline = std::time::Instant::now() + ARRIVES_WITHIN;
    loop {
        match ticks_rx.recv_timeout(Duration::from_millis(500)) {
            Ok(tick) if tick.attempted => break,
            _ => assert!(
                std::time::Instant::now() < deadline,
                "the change was consumed by the sweep that never ran: no sweep \
                 was retried once the lock was free"
            ),
        }
    }
    let deadline = std::time::Instant::now() + ARRIVES_WITHIN;
    while catalog_session_count(&db, "claude", "contended-session") != 1 {
        assert!(
            std::time::Instant::now() < deadline,
            "the retried sweep did not ingest the transcript written under the lock"
        );
        std::thread::yield_now();
    }

    // Control: with the lock free, one event is still one sweep — the retry
    // is for a sweep that did not happen, not an extra one for every sweep
    // that did.
    while ticks_rx.recv_timeout(Duration::from_millis(600)).is_ok() {}
    write_claude_transcript(home.path(), "proj", "free-session", 2);
    let deadline = std::time::Instant::now() + ARRIVES_WITHIN;
    while catalog_session_count(&db, "claude", "free-session") != 1 {
        assert!(
            std::time::Instant::now() < deadline,
            "a write with the lock free must be swept as it always was"
        );
        std::thread::yield_now();
    }
    while ticks_rx.recv_timeout(Duration::from_millis(600)).is_ok() {}
    assert_eq!(
        ticks_rx.recv_timeout(Duration::from_millis(1_500)),
        Err(RecvTimeoutError::Timeout),
        "a sweep that ran must not be retried"
    );
}

/// A sweep that *failed* has covered nothing either, and the change it was
/// for is recorded nowhere else.
///
/// The sibling of the contended case, one door further along: the wake state
/// was cleared when the debounce window opened, so an `Err` out of the tick —
/// SQLite returning a transient I/O error, a provider that could not be read,
/// a sync-state write that failed — takes the only record of the change with
/// it. The loop logs the error and goes back to waiting, and the next chance
/// is the backstop, by which time a ten-second session's transcript has been
/// written and cleaned up again.
///
/// End to end against a real failure rather than a synthetic `Err`: the
/// database path is a *directory* for the first ticks, which is what a real
/// `open_db` refuses, and becomes writable again partway through.
#[cfg(all(feature = "fs-events", unix))]
#[test]
fn a_sweep_that_failed_is_retried_not_dropped() {
    let home = tempfile::tempdir().expect("tempdir");
    let db = home.path().join("history.db");
    let root = home.path().join(".claude/projects");
    std::fs::create_dir_all(&root).expect("the provider root");
    // Nothing can open this as a database.
    std::fs::create_dir(&db).expect("the unusable database path");

    let (ticks, ticks_rx) = mpsc::channel();
    let (errors, errors_rx) = mpsc::channel();
    let tick: TickFn = {
        let db = db.clone();
        let home = home.path().to_path_buf();
        Arc::new(move |force| {
            let outcome = ai_hist::sync_tick_at_with_home(&db, &home, SyncOutput::Silent, force)
                .map(ai_hist::watch::TickOutcome::from);
            let _ = ticks.send(outcome.is_ok());
            outcome
        })
    };
    let running = RunningLoop::start(
        {
            let root = root.clone();
            move |watch| {
                watch
                    .with_immediate(false)
                    .with_fs_events(true)
                    .with_roots(vec![ai_hist::discover::WatchRoot::tree(root)])
                    .with_debounce_ms(100)
                    // Ten minutes each: any sweep below can only have come
                    // from the retry.
                    .with_poll_interval_ms(600_000)
                    .with_slow_poll_ms(600_000)
                    .on_error(Arc::new(move |_| {
                        let _ = errors.send(());
                    }))
            }
        },
        tick,
    );
    assert_eq!(running.watch.driver(), Some(WatchDriver::FsEvents));

    // A session writes while the store cannot be opened.
    let written = std::time::Instant::now();
    write_claude_transcript(home.path(), "proj", "failed-session", 2);
    errors_rx
        .recv_timeout(ARRIVES_WITHIN)
        .expect("the write drove no sweep at all");

    // Control on the cadence, before the recovery: the retries back off the
    // way the contended ones do. A fixed 250 ms would put a dozen failures in
    // this window; doubling puts a handful.
    let window = Duration::from_secs(3);
    let mut failures = 1;
    while written.elapsed() < window {
        let left = window.saturating_sub(written.elapsed());
        if errors_rx
            .recv_timeout(left.min(Duration::from_millis(200)))
            .is_ok()
        {
            failures += 1;
        }
    }
    assert!(
        (2..=8).contains(&failures),
        "a failing forced sweep must be retried, and must back off doing it: \
         {failures} failures in {window:?}"
    );

    // The store becomes usable. Nothing else can wake the loop now: both
    // intervals are ten minutes away.
    std::fs::remove_dir(&db).expect("free the database path");

    let deadline = std::time::Instant::now() + ARRIVES_WITHIN;
    while catalog_session_count(&db, "claude", "failed-session") != 1 {
        assert!(
            std::time::Instant::now() < deadline,
            "the change was consumed by the sweep that failed: nothing was \
             retried once the store was usable"
        );
        std::thread::yield_now();
    }

    // Control: a sweep that succeeded is not retried.
    while ticks_rx.recv_timeout(Duration::from_millis(600)).is_ok() {}
    write_claude_transcript(home.path(), "proj", "healthy-session", 2);
    let deadline = std::time::Instant::now() + ARRIVES_WITHIN;
    while catalog_session_count(&db, "claude", "healthy-session") != 1 {
        assert!(
            std::time::Instant::now() < deadline,
            "a write with the store healthy must be swept as it always was"
        );
        std::thread::yield_now();
    }
    while ticks_rx.recv_timeout(Duration::from_millis(600)).is_ok() {}
    assert_eq!(
        ticks_rx.recv_timeout(Duration::from_millis(1_500)),
        Err(RecvTimeoutError::Timeout),
        "a sweep that ran must not be retried"
    );
}

/// Moving a watched directory invalidates the watch just as deleting it does,
/// but backends report that as `Modify(Name(...))`. The rename event must put
/// the replacement name onto the short recovery cadence; the long backstop is
/// deliberately disabled here so it cannot make the test pass.
#[cfg(feature = "fs-events")]
#[test]
fn a_root_renamed_away_is_recovered_before_the_backstop() {
    let home = tempfile::tempdir().expect("tempdir");
    let root = home.path().join("sessions");
    let backup = home.path().join("sessions-old");
    std::fs::create_dir_all(&root).expect("root");

    let running = RunningLoop::reporting({
        let root = root.clone();
        move |watch| {
            watch
                .with_immediate(false)
                .with_fs_events(true)
                .with_roots(vec![ai_hist::discover::WatchRoot::tree(root)])
                .with_debounce_ms(50)
                .with_slow_poll_ms(600_000)
                .with_poll_interval_ms(600_000)
        }
    });
    assert_eq!(running.watch.driver(), Some(WatchDriver::FsEvents));

    std::fs::rename(&root, &backup).expect("rename watched root away");
    std::fs::create_dir_all(&root).expect("create replacement root");

    // First consume the rename's own forced sweep. A later write must produce
    // another one; otherwise this assertion could pass on the event that
    // announced the loss rather than on the replacement registration.
    let deadline = std::time::Instant::now() + ARRIVES_WITHIN;
    loop {
        match running.ticks.recv_timeout(Duration::from_millis(250)) {
            Ok(true) => break,
            Ok(false) | Err(_) => assert!(
                std::time::Instant::now() < deadline,
                "renaming the attached root drove no forced sweep"
            ),
        }
    }
    let drain_until = std::time::Instant::now() + Duration::from_millis(400);
    while std::time::Instant::now() < drain_until {
        let _ = running.ticks.recv_timeout(Duration::from_millis(50));
    }

    std::fs::write(root.join("replacement.jsonl"), "{}\n").expect("write under replacement");
    assert_eq!(
        running.ticks.recv_timeout(ARRIVES_WITHIN),
        Ok(true),
        "the replacement root was not reattached on the recovery cadence"
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

/// A hook payload makes two claims — which session fired, and which file
/// holds it — and a delayed, replayed or malformed one can pair them wrongly.
/// Ingesting the file regardless attributes the lifecycle event to a session
/// it did not come from, and the harness has no way to know: the hook exits
/// zero either way.
#[test]
fn a_hook_payload_naming_another_session_ingests_nothing() {
    let home = tempfile::tempdir().expect("tempdir");
    let stale = write_claude_transcript(home.path(), "proj", "previous", 2);
    let db = home.path().join("history.db");

    let report = ai_hist::ingest_transcript_at_with_home(
        &db,
        home.path(),
        "claude",
        &stale,
        Some("current"),
        true,
    )
    .expect("hook ingest");

    assert_eq!(
        report.status,
        ai_hist::TranscriptStatus::Mismatched,
        "a transcript that is not the named session must be refused"
    );
    assert_eq!(
        report.session_id.as_deref(),
        Some("previous"),
        "and the caller is told what the file actually was"
    );
    assert_eq!(
        catalog_session_count(&db, "claude", "previous"),
        0,
        "the session the file belongs to must not be ingested either"
    );
    assert_eq!(
        catalog_session_count(&db, "claude", "current"),
        0,
        "and the session the payload named certainly must not be"
    );

    // Positive control: the same transcript, named correctly, ingests exactly
    // that session — so the refusal above is about the mismatch and not about
    // the path, the payload shape, or the check refusing everything.
    let report = ai_hist::ingest_transcript_at_with_home(
        &db,
        home.path(),
        "claude",
        &stale,
        Some("previous"),
        true,
    )
    .expect("hook ingest");
    assert_eq!(report.status, ai_hist::TranscriptStatus::Ingested);
    assert_eq!(report.session_id.as_deref(), Some("previous"));
    assert_eq!(catalog_session_count(&db, "claude", "previous"), 1);
}

/// The shallow Claude catalog intentionally falls back to a transcript's file
/// stem for old parseable files without `sessionId`. A hook payload is not
/// allowed to promote that guess into identity: the transcript itself must
/// prove the session before the hook writes anything.
#[test]
fn a_hook_transcript_without_native_identity_creates_no_catalog_row() {
    let home = tempfile::tempdir().expect("tempdir");
    let project = home.path().join(".claude/projects/proj");
    std::fs::create_dir_all(&project).expect("create project");
    let transcript = project.join("identity-free.jsonl");
    std::fs::write(
        &transcript,
        r#"{"type":"user","message":{"role":"user","content":"hello"}}"#.to_string() + "\n",
    )
    .expect("write transcript");
    let db = home.path().join("history.db");

    let report = ai_hist::ingest_transcript_at_with_home(
        &db,
        home.path(),
        "claude",
        &transcript,
        Some("identity-free"),
        true,
    )
    .expect("hook ingest");

    assert_eq!(report.status, ai_hist::TranscriptStatus::Unidentified);
    assert_eq!(report.session_id, None);
    assert_eq!(
        catalog_session_count(&db, "claude", "identity-free"),
        0,
        "a filename-derived identity must not be persisted by the hook path"
    );
}

#[test]
fn a_missing_transcript_is_reported_not_raised() {
    let home = tempfile::tempdir().expect("tempdir");
    let db = home.path().join("history.db");

    let report = ai_hist::ingest_transcript_at_with_home(
        &db,
        home.path(),
        "claude",
        &home.path().join(".claude/projects/proj/gone.jsonl"),
        None,
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

    let error =
        ai_hist::ingest_transcript_at_with_home(&db, home.path(), "claude", &outside, None, true)
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

    let report = ai_hist::ingest_transcript_at_with_home(
        &db,
        home.path(),
        "claude",
        &transcript,
        None,
        true,
    )
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

    let first = ai_hist::ingest_transcript_at_with_home(
        &db,
        home.path(),
        "claude",
        &transcript,
        None,
        true,
    )
    .expect("first hook ingest");
    assert_eq!(first.status, ai_hist::TranscriptStatus::Ingested);

    let second = ai_hist::ingest_transcript_at_with_home(
        &db,
        home.path(),
        "claude",
        &transcript,
        None,
        true,
    )
    .expect("second hook ingest");
    assert_eq!(
        second.status,
        ai_hist::TranscriptStatus::Unchanged,
        "re-running a hook over an untouched transcript must not re-read it"
    );
}

/// Discovery reads the same files the fingerprint counted, and its per-file
/// failures are non-fatal: the candidate leaves a diagnostic and the run
/// returns `Ok`. Storing the fingerprint over that makes a transient failure
/// permanent — the next tick matches the stored value and skips before
/// retrying, so the file is never read again until its metadata happens to
/// change. Same class as a swallowed per-file sweep error, which
/// `SweepCoverage` already exists to catch.
#[test]
fn a_file_discovery_could_not_read_does_not_arm_the_fast_path() {
    let home = tempfile::tempdir().expect("tempdir");
    // One readable transcript, so the sweep has something that works and the
    // assertion below cannot pass because nothing happened at all.
    write_claude_transcript(home.path(), "proj", "readable-1", 1);
    let db = home.path().join("history.db");
    assert!(sync_tick(&db, home.path(), false).swept);
    assert!(
        sync_tick(&db, home.path(), false).skipped_unchanged(),
        "positive control: a readable tree does arm the fast path"
    );

    // A transcript that stats like any other — it is a regular file, so
    // enumeration counts it and the fingerprint stamps it — but cannot be
    // read as text. Permissions are not a lever here: these tests run as
    // root.
    let unreadable = home.path().join(".claude/projects/proj/unreadable-1.jsonl");
    std::fs::write(&unreadable, [0xff, 0xfe, 0xfd, b'\n']).expect("write invalid utf-8");

    assert!(
        sync_tick(&db, home.path(), false).swept,
        "a new file must reopen the sweep"
    );
    assert!(
        sync_tick(&db, home.path(), false).swept,
        "a file that could not be read must not be cached as read"
    );
    assert!(
        sync_tick(&db, home.path(), false).swept,
        "and must go on being retried, rather than settling on the failure"
    );

    // Positive control on the other side: once it reads, the fast path arms
    // again — the retry is not a permanent state of its own.
    std::fs::write(&unreadable, claude_record("unreadable-1", 1)).expect("rewrite as text");
    assert!(sync_tick(&db, home.path(), false).swept);
    assert!(
        sync_tick(&db, home.path(), false).skipped_unchanged(),
        "a tree that reads cleanly must arm the fast path again"
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
    assert!(before > 1, "the test needs a short, still-nonempty session");
    let conn = ai_hist::open_db(&db).expect("open db");
    conn.execute(
        "DELETE FROM session_events WHERE source = 'codex' AND session_id = 'sess-negative' \
         AND rowid = (SELECT MIN(rowid) FROM session_events \
                      WHERE source = 'codex' AND session_id = 'sess-negative')",
        [],
    )
    .expect("delete one event");
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

/// Wait until no *forced* tick has arrived for a few debounce windows.
///
/// The plain `settle` cannot be used by a test that needs a short backstop:
/// unforced ticks keep arriving by design, so "no ticks at all" never
/// happens. Only the forced ones say a filesystem event was seen, and only
/// those have to be drained before asking whether the next write drives one.
#[cfg(feature = "fs-events")]
fn settle_forced(running: &RunningLoop, when: &str) {
    let deadline = std::time::Instant::now() + ARRIVES_WITHIN;
    let mut quiet_until = std::time::Instant::now() + Duration::from_millis(400);
    while std::time::Instant::now() < quiet_until {
        if let Ok(true) = running.ticks.recv_timeout(Duration::from_millis(100)) {
            quiet_until = std::time::Instant::now() + Duration::from_millis(400);
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the loop never stopped sweeping {when}"
        );
    }
}

/// Wait for `count` backstop ticks, which is how a test waits for the work
/// the loop only does on the backstop — reconciliation — without waiting on a
/// duration and hoping.
#[cfg(feature = "fs-events")]
fn backstop_ticks(running: &RunningLoop, count: usize, when: &str) {
    let deadline = std::time::Instant::now() + ARRIVES_WITHIN;
    let mut seen = 0;
    while seen < count {
        match running.ticks.recv_timeout(Duration::from_millis(500)) {
            Ok(false) => seen += 1,
            _ => assert!(
                std::time::Instant::now() < deadline,
                "the backstop never ticked {when}"
            ),
        }
    }
}

/// Assert that no *forced* tick arrives within `window`, ignoring the
/// backstop ticks a short poll interval keeps producing.
#[cfg(feature = "fs-events")]
fn no_forced_tick(running: &RunningLoop, window: Duration, what: &str) {
    let until = std::time::Instant::now() + window;
    while std::time::Instant::now() < until {
        if let Ok(true) = running.ticks.recv_timeout(Duration::from_millis(100)) {
            panic!("{what}");
        }
    }
}

/// Wait for a *forced* tick — one only a filesystem event produces — while
/// tolerating the backstop ticks a short poll interval keeps producing.
#[cfg(feature = "fs-events")]
fn forced_tick(running: &RunningLoop, what: &str) {
    let deadline = std::time::Instant::now() + ARRIVES_WITHIN;
    loop {
        match running.ticks.recv_timeout(Duration::from_millis(500)) {
            Ok(true) => return,
            Ok(false) | Err(_) => assert!(
                std::time::Instant::now() < deadline,
                "{what} drove no forced sweep: {:?}",
                running.watch.status()
            ),
        }
    }
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

/// Wait for a published status to satisfy `want`, or fail saying what it was.
#[cfg(feature = "fs-events")]
fn await_status(
    running: &RunningLoop,
    what: &str,
    want: impl Fn(&ai_hist::watch::DriverStatus) -> bool,
) {
    await_status_within(running, ARRIVES_WITHIN, what, want);
}

/// The same, bounded by `window` — used where the *cadence* is the thing under
/// test and the window is a fraction of the cadence that must not apply.
#[cfg(feature = "fs-events")]
fn await_status_within(
    running: &RunningLoop,
    window: Duration,
    what: &str,
    want: impl Fn(&ai_hist::watch::DriverStatus) -> bool,
) {
    let deadline = std::time::Instant::now() + window;
    loop {
        let status = running.watch.status().expect("status");
        if want(&status) {
            return;
        }
        assert!(std::time::Instant::now() < deadline, "{what}: {status:?}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Wait for the next backstop refresh, so what follows is measured from a
/// known point in the backstop's cycle rather than from wherever the loop
/// happened to be.
#[cfg(feature = "fs-events")]
fn await_refresh(refreshes: &Arc<AtomicUsize>, when: &str) {
    let seen = refreshes.load(Ordering::SeqCst);
    let deadline = std::time::Instant::now() + ARRIVES_WITHIN;
    while refreshes.load(Ordering::SeqCst) == seen {
        assert!(
            std::time::Instant::now() < deadline,
            "the backstop never came round {when}"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}
