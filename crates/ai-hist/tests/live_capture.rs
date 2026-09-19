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

use ai_hist::discover::{DiscoveryEnv, ProviderRoots};
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
    assert!(
        root.recursive,
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
    for (expected, recursive) in [
        (home.join(".claude"), false),
        (home.join(".codex"), false),
        (home.join(".claude/projects"), true),
    ] {
        let root = roots
            .iter()
            .find(|root| root.path == expected)
            .unwrap_or_else(|| panic!("{expected:?} missing from {roots:?}"));
        assert_eq!(
            root.recursive, recursive,
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
