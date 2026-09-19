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
            .with_roots(vec![missing])
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
            .with_roots(vec![dir.path().to_path_buf()])
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
        roots.contains(&home.path().join(".claude/projects")),
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
            roots.contains(&expected),
            "{expected:?} missing from {roots:?}"
        );
    }
    let mut deduped = roots.clone();
    deduped.sort();
    deduped.dedup();
    assert_eq!(
        deduped.len(),
        roots.len(),
        "watch roots must be deduplicated"
    );
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

fn session_event_count(db: &Path, session_id: &str) -> i64 {
    let conn = ai_hist::open_db(db).expect("open db");
    conn.query_row(
        "SELECT COUNT(*) FROM session_events WHERE source = 'claude' AND session_id = ?",
        [session_id],
        |row| row.get(0),
    )
    .expect("count session events")
}
