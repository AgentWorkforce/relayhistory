//! Live capture: a filesystem-event driven watch loop with a polling fallback.
//!
//! `ai-hist watch` used to be a fixed-interval poll that paid the full
//! transcript walk every tick. Claude Code transcripts are cleaned up, so the
//! interesting window is the one between a write and that cleanup — polling
//! either misses it or burns the machine to catch it. This module drives the
//! same sweep from two sources instead:
//!
//! * **Filesystem events** (preferred): a recursive watcher over the providers'
//!   [`watch_roots`](crate::discover::watch_roots) wakes the loop on a real
//!   write. A burst of events collapses into one tick through the
//!   [`WatchLoop::debounce_ms`] window, and a slow poll at
//!   [`WatchLoop::slow_poll_ms`] backstops the platforms where events are
//!   silently unreliable (network mounts, some container filesystems).
//! * **Polling** (fallback): when no roots are watchable, when the crate was
//!   built without the `fs-events` feature, or when the caller passes
//!   `--no-fsevents`, the loop ticks at [`WatchLoop::poll_interval_ms`].
//!
//! Two properties are load-bearing and easy to lose:
//!
//! * A tick driven by a filesystem event passes `force = true`. An event can
//!   fire before the write is flushed, so the stat-only
//!   [`source_fingerprint`](crate::discover::source_fingerprint) may still read
//!   the pre-write size and mtime and match the stored value. Forcing makes the
//!   per-session stamps the source of truth for that tick. The slow backstop
//!   and manual ticks leave `force = false` so a quiet period still costs only
//!   the fingerprint walk.
//! * The debounce window always ends. Under sustained writes the loop emits a
//!   steady `~debounce` cadence rather than waiting for a quiet period that
//!   never arrives — waiting for quiet would demote a busy session to the slow
//!   backstop, which is exactly the case live capture exists for.
//!
//! The loop is plain threads and condition variables. The crate has no async
//! runtime and this does not need one: a tick is a blocking sweep, and the
//! watcher backend already runs on its own thread.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;

use crate::discover::{self, WatchDepth, WatchRoot};

/// How often a registration that was *lost* is retried.
///
/// Not derived from the user's intervals, because neither of them is about
/// this: a root that was attached and is now gone is a directory being
/// replaced, and the window between the two is where a short session lives.
/// `~/.codex/sessions` removed and recreated, a ten-second session written
/// there, cleanup taking it away again — a retry on the 30 s backstop misses
/// the whole of it. One `stat` per lost root, four times a second, and only
/// until it is attached again.
const LOST_REGISTRATION_RECHECK_MS: u64 = 250;

/// How soon a forced sweep that could not take the store's lock is tried
/// again, before backing off.
///
/// Short, because the usual holder is a manual `sync` that is about to finish
/// and the change is still owed; backed off from there so a long-running
/// holder is not asked four times a second for minutes — the retry is a
/// `try_lock` and a return, but it is also a log line each time.
const CONTENDED_SWEEP_RETRY_MS: u64 = 250;

/// The longest any configurable interval may be.
///
/// `Instant + Duration` panics when the sum is not representable, and every
/// interval here comes from a command line: `watch --debounce-ms
/// 18446744073709551615` would start normally and die on the first change
/// event. Bounding the values where they enter — rather than defending at each
/// of the places they are later added to an instant — is what keeps that from
/// depending on remembering. Seven days is far longer than any cadence this
/// loop is for, and far below the platform's ceiling.
pub const MAX_INTERVAL_MS: u64 = 7 * 24 * 60 * 60 * 1000;

/// One configurable interval, bounded so it can be added to an `Instant`.
fn bounded_ms(millis: u64) -> u64 {
    millis.min(MAX_INTERVAL_MS)
}

/// `now` plus a configurable interval, never panicking.
///
/// Belt and braces over [`bounded_ms`]: a deadline that cannot be represented
/// becomes *now*, so the worst case is work done earlier than asked rather
/// than a loop that dies.
fn deadline_after(now: Instant, millis: u64) -> Instant {
    now.checked_add(Duration::from_millis(bounded_ms(millis)))
        .unwrap_or(now)
}

/// Default coalescing window for filesystem-event bursts. Short enough that an
/// interactive pause feels live, long enough to collapse the event burst from
/// one tool result appending a multi-line transcript update.
pub const DEFAULT_DEBOUNCE_MS: u64 = 200;
/// Default slow polling backstop while the filesystem-event driver is active.
pub const DEFAULT_SLOW_POLL_MS: u64 = 30_000;
/// Default cadence for the pure polling driver.
pub const DEFAULT_POLL_INTERVAL_MS: u64 = 1_000;

/// Which driver a running loop selected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchDriver {
    /// Filesystem events, with the slow poll as a backstop.
    FsEvents,
    /// Fixed-interval polling only.
    Polling,
}

impl WatchDriver {
    pub fn as_str(self) -> &'static str {
        match self {
            WatchDriver::FsEvents => "fs-events",
            WatchDriver::Polling => "polling",
        }
    }
}

/// What the loop is actually driven by right now, and what it is not covering.
///
/// `pending` is the honest part. A root that did not exist when the loop
/// started is not watched, and reporting `FsEvents` while `~/.codex/sessions`
/// is uncovered would claim a liveness the loop does not have for that
/// provider — its transcripts would only be seen by the backstop, by which
/// time a short session can already have been cleaned up. The loop keeps
/// retrying these, so the list shrinks as providers appear.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DriverStatus {
    pub driver: WatchDriver,
    /// Roots the watcher is attached to.
    pub watched: Vec<PathBuf>,
    /// Roots that do not exist yet, retried on every backstop tick.
    pub pending: Vec<PathBuf>,
}

/// What woke a tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TickTrigger {
    /// The first sweep, before the loop parks.
    Startup,
    /// A filesystem event, after the debounce window settled.
    FsEvent,
    /// The polling driver's interval, or the slow backstop.
    Poll,
    /// An explicit [`WatchLoop::tick`] call.
    Manual,
    /// The backend reported that a registration is gone.
    ///
    /// Not a sweep: nothing new has been written, a watch has been lost. It
    /// wakes the loop so the registration can be re-made now rather than at
    /// the next backstop, which is up to `slow_poll_ms` away — long enough
    /// for a whole short session to be written to a recreated directory and
    /// cleaned up again, unseen.
    RegistrationLost,
}

impl TickTrigger {
    /// Whether this trigger bypasses the stat-only fingerprint fast path.
    ///
    /// Only a filesystem event does: it can arrive before the write flushes,
    /// so the fingerprint it would be compared against is not yet trustworthy.
    pub fn forces_scan(self) -> bool {
        matches!(self, TickTrigger::FsEvent)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            TickTrigger::Startup => "startup",
            TickTrigger::FsEvent => "fs-event",
            TickTrigger::Poll => "poll",
            TickTrigger::Manual => "manual",
            TickTrigger::RegistrationLost => "registration-lost",
        }
    }
}

/// What one tick's sweep did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TickOutcome {
    /// A full sweep ran.
    pub swept: bool,
    /// The sweep was skipped because the source fingerprint was unchanged.
    pub skipped_unchanged: bool,
    /// The sweep could not run at all: another process held the store's sync
    /// lock, so nothing was read and nothing was compared.
    ///
    /// Distinct from `skipped_unchanged`, and the distinction is the whole
    /// point: "no source moved" is an answer about the change this tick was
    /// for, and this is the absence of one. The lock holder is no guarantee
    /// of cover — its own source walk may already be past the provider that
    /// just wrote — so a *forced* tick that comes back this way leaves the
    /// change it was for still owed, and the loop retries it rather than
    /// counting it done.
    pub contended: bool,
}

/// One tick, as handed to [`WatchLoop::on_report`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TickReport {
    pub trigger: TickTrigger,
    pub forced: bool,
    pub outcome: TickOutcome,
}

/// The sweep a tick runs. The `bool` is `force`.
pub type TickFn = Arc<dyn Fn(bool) -> Result<TickOutcome> + Send + Sync>;
/// Sink for completed ticks.
pub type ReportSink = Arc<dyn Fn(&TickReport) + Send + Sync>;
/// Sink for ticks that failed. A failed tick never stops the loop.
pub type ErrorSink = Arc<dyn Fn(&anyhow::Error) + Send + Sync>;
/// Sink called whenever what drives the loop changes — at startup, and again
/// when a root that did not exist appears and is picked up.
pub type DriverSink = Arc<dyn Fn(&DriverStatus) + Send + Sync>;
/// Re-derives the roots that should be watched. Called on backstop ticks, so a
/// root that did not exist as a *name* at startup — a project's
/// `.trajectories` directory created later — can still be picked up.
pub type RootsFn = Arc<dyn Fn() -> Vec<WatchRoot> + Send + Sync>;

/// Whether an event on `path` belongs to one of `roots`.
///
/// Depth has to be enforced here, not left to the backend. The macOS FSEvents
/// backend has no non-recursive mode at all: asking for one still delivers the
/// whole subtree. A `WatchRoot::directory(~/.claude)` would therefore see
/// every todo file and shell snapshot an active session rewrites, and each of
/// those would become a *forced* sweep — the expensive kind that bypasses the
/// fingerprint. Filtering by the depth the root asked for makes the two
/// backends agree, and on inotify it is simply a no-op the kernel already did.
fn event_matches_roots(path: &Path, roots: &[WatchRoot]) -> bool {
    // The event's own spelling is never compared. Roots are absolute from the
    // moment they are built, and a backend reports whatever it was registered
    // with — including a `.` component that survives the join — so both sides
    // go through the same normalisation before they are compared at all.
    let path = discover::watch_path(path);
    roots.iter().any(|root| root.covers(&path))
}

/// The registration keys of every root `path` names, for a removal or
/// rename-away event.
///
/// The event arrives in whichever spelling the backend reports, and a root
/// answers to more than one — so the *root* decides whether this is its
/// removal, and what comes back is the root's own key rather than the path
/// that was reported. That is what keeps the recording side and the lookup
/// side from drifting apart: there is one key, and it comes from here.
fn removed_registration_keys(path: &Path, roots: &[WatchRoot]) -> Vec<PathBuf> {
    let path = discover::watch_path(path);
    roots
        .iter()
        .filter(|root| root.registers_at(&path))
        .map(|root| root.registration_key().to_path_buf())
        .collect()
}

#[derive(Default)]
struct WakeState {
    /// A change signal is pending. Single-bit on purpose: a thousand events
    /// between two ticks cost one wakeup, not a thousand.
    pending: bool,
    /// A registration was reported gone and has to be re-made. Kept apart
    /// from `pending` because it asks for different work: `pending` says
    /// something was written and wants a sweep, this says a watch was lost
    /// and wants the watch back.
    registration_lost: bool,
    stopped: bool,
}

#[derive(Default)]
struct RunState {
    in_flight: bool,
    /// A forced tick arrived while a run held the slot. Kept as one bit: a
    /// hundred events during a long sweep are one sweep afterwards, not a
    /// hundred.
    deferred_force: bool,
    /// Monotonic count of finished ticks, so a joiner can wait for "the run
    /// that was in flight when I arrived" without holding the lock across it.
    completed: u64,
}

struct WatchInner {
    tick: TickFn,
    on_report: Option<ReportSink>,
    on_error: Option<ErrorSink>,
    wake: Mutex<WakeState>,
    wake_cv: Condvar,
    run: Mutex<RunState>,
    run_cv: Condvar,
    driver: Mutex<Option<DriverStatus>>,
}

/// Resets `in_flight` even when the sweep panics, so one bad tick cannot wedge
/// the loop into "a run is always in flight" forever.
struct InFlight<'a> {
    inner: &'a WatchInner,
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        let deferred = {
            let mut run = self.inner.run.lock().expect("watch run state");
            run.in_flight = false;
            run.completed = run.completed.wrapping_add(1);
            std::mem::take(&mut run.deferred_force)
        };
        self.inner.run_cv.notify_all();
        if deferred {
            // A change arrived while this run held the slot. Post it now that
            // the slot is free: the loop is waiting on the wake state, so it
            // takes it up immediately rather than at the next backstop.
            self.inner.signal_change();
        }
    }
}

impl WatchInner {
    fn stopped(&self) -> bool {
        self.wake.lock().expect("watch wake state").stopped
    }

    /// Post a change signal. Called by the filesystem watcher's callback, and
    /// public through [`WatchLoop::notify_change`] for hosts that already have
    /// their own change feed.
    fn signal_change(&self) {
        {
            let mut wake = self.wake.lock().expect("watch wake state");
            if wake.stopped {
                return;
            }
            wake.pending = true;
        }
        self.wake_cv.notify_all();
    }

    /// Post that a registration is gone. Called by the watcher's callback
    /// when the backend reports a watched path removed.
    fn signal_registration_lost(&self) {
        {
            let mut wake = self.wake.lock().expect("watch wake state");
            if wake.stopped {
                return;
            }
            wake.registration_lost = true;
        }
        self.wake_cv.notify_all();
    }

    fn request_stop(&self) {
        {
            let mut wake = self.wake.lock().expect("watch wake state");
            wake.stopped = true;
        }
        self.wake_cv.notify_all();
    }

    fn report_error(&self, error: &anyhow::Error) {
        if let Some(sink) = &self.on_error {
            sink(error);
        } else {
            eprintln!("ai-hist: watch failed: {error:#}");
        }
    }

    /// Block until a change signal arrives, `timeout` elapses, or the loop is
    /// stopped. A change signal is followed by the debounce window, so further
    /// events landing inside it roll into the same tick.
    fn wait_for_wake(&self, timeout: Duration, debounce: Duration) -> Option<TickTrigger> {
        let mut wake = self.wake.lock().expect("watch wake state");
        loop {
            if wake.stopped {
                return None;
            }
            // Before `pending`, and without the debounce window: this one
            // does not run a sweep, it puts a watch back, and every moment
            // it waits is a moment writes to that directory are invisible.
            if wake.registration_lost {
                wake.registration_lost = false;
                return Some(TickTrigger::RegistrationLost);
            }
            if wake.pending {
                // Clear before the window, not after: events that land during
                // the debounce set it again and the next wait observes them
                // immediately. That is what gives sustained writes a steady
                // ~debounce cadence instead of a wait for quiet.
                wake.pending = false;
                drop(wake);
                // Cut short by a lost registration, because on a busy tree
                // this window is where the loop spends nearly all of its
                // time: a removal landing inside it would otherwise wait out
                // the rest of the window *and* the sweep that follows before
                // the watch is put back, which is the whole of the gap a
                // short session occupies. The bit itself is left set; the
                // caller takes it and reconciles before it sweeps.
                self.sleep_through_debounce(debounce);
                return (!self.stopped()).then_some(TickTrigger::FsEvent);
            }
            let (next, result) = self
                .wake_cv
                .wait_timeout(wake, timeout)
                .expect("watch wake state");
            wake = next;
            if result.timed_out() {
                if wake.stopped {
                    return None;
                }
                return Some(TickTrigger::Poll);
            }
        }
    }

    /// Take the lost-registration bit, if one is set.
    ///
    /// Read on every wake rather than only on the wake it caused: a removal
    /// reported while the loop was inside its debounce window, or inside a
    /// sweep, has to be acted on by the iteration that comes out of it and
    /// not by the one after the next sweep.
    fn take_registration_lost(&self) -> bool {
        let mut wake = self.wake.lock().expect("watch wake state");
        std::mem::take(&mut wake.registration_lost)
    }

    /// Sleep out the debounce window, returning early when the loop is
    /// stopped or a registration has been reported lost.
    fn sleep_through_debounce(&self, duration: Duration) {
        let done = |wake: &WakeState| wake.stopped || wake.registration_lost;
        // Bounded before it reaches an `Instant`, because these durations come
        // from the command line and the addition panics on a sum it cannot
        // represent.
        let duration = duration.min(Duration::from_millis(MAX_INTERVAL_MS));
        let mut wake = self.wake.lock().expect("watch wake state");
        let Some(deadline) = Instant::now().checked_add(duration) else {
            // Unreachable given the bound above. A platform whose clock cannot
            // represent even that should wait for the stop signal rather than
            // spin through zero-length sleeps.
            while !done(&wake) {
                wake = self.wake_cv.wait(wake).expect("watch wake state");
            }
            return;
        };
        while !done(&wake) {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            let (next, _) = self
                .wake_cv
                .wait_timeout(wake, deadline - now)
                .expect("watch wake state");
            wake = next;
        }
    }

    /// Driver path: a *backstop* tick arriving while one is in flight is
    /// dropped, not queued. Queuing would run two sweeps back to back with no
    /// gap right after a slow one — the spike the interval exists to avoid.
    ///
    /// A **forced** tick is not dropped, because it is evidence that something
    /// changed: the wake state was already cleared when the debounce window
    /// opened, so returning here would lose that change until the backstop —
    /// up to `--interval` later, which may be an hour. It is remembered
    /// instead, and [`InFlight::drop`] re-posts it the moment the run in
    /// flight finishes. Repeats coalesce into the one bit, so a busy tree
    /// during a long manual sweep costs one sweep afterwards.
    /// Returns whether a *forced* sweep is still owed — whether it came back
    /// saying it never took the store's lock, or failed outright. Either way
    /// nothing looked at the change it was for, and the change is recorded
    /// nowhere else. A tick that could not have the slot returns `false`
    /// — that change is remembered as `deferred_force` and re-posted by
    /// [`InFlight::drop`], which is the same promise by another route.
    fn run_skip_if_busy(&self, trigger: TickTrigger) -> bool {
        let Some(guard) = self.claim_or_defer(trigger) else {
            return false;
        };
        self.run_claimed(trigger, guard)
    }

    /// Claim the in-flight slot, or remember a forced trigger that could not
    /// have it.
    fn claim_or_defer(&self, trigger: TickTrigger) -> Option<InFlight<'_>> {
        let mut run = self.run.lock().expect("watch run state");
        if run.in_flight {
            run.deferred_force |= trigger.forces_scan();
            return None;
        }
        run.in_flight = true;
        Some(InFlight { inner: self })
    }

    /// Manual path: a tick arriving while one is in flight waits for it, so
    /// `tick()` is a real completion barrier rather than a silent no-op.
    fn run_or_join(&self, trigger: TickTrigger) {
        let join_target = {
            let mut run = self.run.lock().expect("watch run state");
            if run.in_flight {
                Some(run.completed.wrapping_add(1))
            } else {
                run.in_flight = true;
                None
            }
        };
        match join_target {
            None => {
                self.run_claimed(trigger, InFlight { inner: self });
            }
            Some(target) => {
                let mut run = self.run.lock().expect("watch run state");
                while run.completed < target {
                    run = self.run_cv.wait(run).expect("watch run state");
                }
            }
        }
    }

    fn run_claimed(&self, trigger: TickTrigger, guard: InFlight<'_>) -> bool {
        let forced = trigger.forces_scan();
        // Only a forced tick is ever owed anything: a backstop tick that found
        // the store busy, or failed, is covered by the next backstop, while a
        // forced one is standing in for a change nothing else knows about.
        let owed = match (self.tick)(forced) {
            Ok(outcome) => {
                if let Some(sink) = &self.on_report {
                    sink(&TickReport {
                        trigger,
                        forced,
                        outcome,
                    });
                }
                forced && outcome.contended
            }
            Err(error) => {
                self.report_error(&error);
                // A sweep that failed covered nothing, exactly as a contended
                // one covered nothing, and the change it was for is recorded
                // nowhere else: the wake state was cleared when the debounce
                // window opened. An error is not an answer about the change,
                // so it is owed and retried on the same bounded cadence rather
                // than logged and forgotten until the backstop.
                forced
            }
        };
        drop(guard);
        owed
    }

    fn wait_for_idle(&self) {
        let mut run = self.run.lock().expect("watch run state");
        while run.in_flight {
            run = self.run_cv.wait(run).expect("watch run state");
        }
    }
}

/// A live-capture watch loop.
///
/// Build one, then drive it with [`run`](WatchLoop::run) on a thread of your
/// choosing. [`tick`](WatchLoop::tick) and [`stop`](WatchLoop::stop) are safe
/// to call from any other thread; wrap the loop in an `Arc` to share it.
pub struct WatchLoop {
    /// Coalescing window for a burst of change signals.
    pub debounce_ms: u64,
    /// Cadence of the pure polling driver.
    pub poll_interval_ms: u64,
    /// Cadence of the slow backstop while the filesystem-event driver is live.
    pub slow_poll_ms: u64,
    /// Whether to attempt the filesystem-event driver at all.
    pub use_fs_events: bool,
    /// Roots to watch, usually [`crate::discover::watch_roots`].
    pub roots: Vec<WatchRoot>,
    /// Run one sweep before parking.
    pub immediate: bool,
    roots_refresh: Option<RootsFn>,
    on_driver: Option<DriverSink>,
    inner: Arc<WatchInner>,
}

impl WatchLoop {
    /// A loop that runs `tick` with defaults: 200 ms debounce, 1 s polling,
    /// 30 s slow backstop, filesystem events enabled but inert until
    /// [`with_roots`](WatchLoop::with_roots) supplies something to watch.
    pub fn new(tick: TickFn) -> Self {
        Self {
            debounce_ms: DEFAULT_DEBOUNCE_MS,
            poll_interval_ms: DEFAULT_POLL_INTERVAL_MS,
            slow_poll_ms: DEFAULT_SLOW_POLL_MS,
            use_fs_events: true,
            roots: Vec::new(),
            immediate: true,
            roots_refresh: None,
            on_driver: None,
            inner: Arc::new(WatchInner {
                tick,
                on_report: None,
                on_error: None,
                wake: Mutex::new(WakeState::default()),
                wake_cv: Condvar::new(),
                run: Mutex::new(RunState::default()),
                run_cv: Condvar::new(),
                    driver: Mutex::new(None),
            }),
        }
    }

    pub fn with_roots(mut self, roots: Vec<WatchRoot>) -> Self {
        self.roots = roots;
        self
    }

    pub fn with_debounce_ms(mut self, debounce_ms: u64) -> Self {
        self.debounce_ms = bounded_ms(debounce_ms);
        self
    }

    pub fn with_poll_interval_ms(mut self, poll_interval_ms: u64) -> Self {
        self.poll_interval_ms = bounded_ms(poll_interval_ms);
        self
    }

    pub fn with_slow_poll_ms(mut self, slow_poll_ms: u64) -> Self {
        self.slow_poll_ms = bounded_ms(slow_poll_ms);
        self
    }

    pub fn with_fs_events(mut self, use_fs_events: bool) -> Self {
        self.use_fs_events = use_fs_events;
        self
    }

    pub fn with_immediate(mut self, immediate: bool) -> Self {
        self.immediate = immediate;
        self
    }

    pub fn on_report(mut self, sink: ReportSink) -> Self {
        Arc::get_mut(&mut self.inner)
            .expect("watch loop is not shared before run")
            .on_report = Some(sink);
        self
    }

    pub fn on_error(mut self, sink: ErrorSink) -> Self {
        Arc::get_mut(&mut self.inner)
            .expect("watch loop is not shared before run")
            .on_error = Some(sink);
        self
    }

    pub fn on_driver(mut self, sink: DriverSink) -> Self {
        self.on_driver = Some(sink);
        self
    }

    /// Re-derive the root set on every backstop tick.
    ///
    /// [`with_roots`](WatchLoop::with_roots) fixes the names to watch at
    /// startup, and the loop already retries the ones that did not exist. This
    /// covers the other case: a root whose *name* could not have been known
    /// yet, because the directory it is discovered from did not contain it.
    /// Roots already known are ignored, so this is idempotent.
    pub fn with_roots_refresh(mut self, refresh: RootsFn) -> Self {
        self.roots_refresh = Some(refresh);
        self
    }

    /// What drives the loop right now, once it has started.
    pub fn status(&self) -> Option<DriverStatus> {
        self.inner.driver.lock().expect("watch driver").clone()
    }

    /// The driver [`run`](WatchLoop::run) is using, once it has started.
    pub fn driver(&self) -> Option<WatchDriver> {
        self.status().map(|status| status.driver)
    }

    /// Post a change signal by hand. The filesystem watcher calls this; hosts
    /// with their own change feed (an editor, an MCP server) can too.
    pub fn notify_change(&self) {
        self.inner.signal_change();
    }

    /// Run one sweep now. If one is already in flight, wait for it instead of
    /// starting a second — the call returns only once a sweep has completed.
    /// A manual tick keeps the fingerprint fast path.
    pub fn tick(&self) {
        self.inner.run_or_join(TickTrigger::Manual);
    }

    /// Stop the loop and wait for any in-flight sweep. Idempotent, and safe to
    /// call from a signal handler thread while [`run`](WatchLoop::run) blocks.
    pub fn stop(&self) {
        self.inner.request_stop();
        self.inner.wait_for_idle();
    }

    /// Drive the loop on this thread until [`stop`](WatchLoop::stop).
    ///
    /// Returns the driver it ended on. Attaching the watcher is best effort in
    /// both directions: a root that does not exist yet is not an error and is
    /// retried on every backstop tick, and a backend that cannot be brought up
    /// at all demotes the loop to polling rather than failing the run. A loop
    /// that started with nothing to watch is promoted to filesystem events the
    /// moment one of its roots appears — a machine that installs Codex an hour
    /// in should not be stuck polling until restart.
    pub fn run(&self) -> Result<WatchDriver> {
        let debounce = Duration::from_millis(self.debounce_ms);
        // A loop given no roots up front but a refresher that will produce
        // them still has to start the backend: `adopt` is only reachable
        // through an attached watcher, so skipping it here would leave that
        // caller polling forever.
        let mut watcher = if self.use_fs_events
            && (!self.roots.is_empty() || self.roots_refresh.is_some())
        {
            let inner = self.inner.clone();
            let lost = self.inner.clone();
            let failed = self.inner.clone();
            fs_events::attach(
                &self.roots,
                move || inner.signal_change(),
                move || lost.signal_registration_lost(),
                move |error| {
                    failed.report_error(&anyhow::anyhow!(
                        "filesystem watcher backend failed: {error}"
                    ))
                },
            )
            .ok()
        } else {
            None
        };
        self.publish_status(watcher.as_ref());

        if self.immediate {
            // A first sweep against a cold catalog has nothing to compare
            // against and runs fully anyway, so it does not need forcing.
            self.inner.run_skip_if_busy(TickTrigger::Startup);
        }
        // When the loop last swept on its own cadence, so that waking early
        // to reconcile does not also sweep early.
        let mut last_poll_sweep = std::time::Instant::now();
        // When reconciliation is next due, as an absolute instant. It has to
        // be absolute: every filesystem event ends the wait early, so a
        // reconciliation gated on the wait *expiring* is starved by exactly
        // the machine this loop exists for — one busy session writing every
        // couple of hundred milliseconds would postpone attaching a provider
        // installed beside it for as long as the writing lasts.
        // Two deadlines, because the backstop does two jobs of very different
        // cost. Re-deriving the root set walks every project tree, so it stays
        // on the backstop whatever else happens. Re-checking the registrations
        // is a stat per root, and has to be prompt: a root that is uncovered
        // now is a root whose writes are invisible now.
        let mut next_refresh = deadline_after(Instant::now(), self.slow_poll_ms);
        let mut next_check = next_refresh;
        // A forced sweep the store's lock turned away, and how long to wait
        // before asking again.
        let mut retry_force: Option<Instant> = None;
        let mut retry_backoff = CONTENDED_SWEEP_RETRY_MS;
        while !self.inner.stopped() {
            let sweep_every = match self.current_driver() {
                WatchDriver::FsEvents => self.slow_poll_ms,
                WatchDriver::Polling => self.poll_interval_ms,
            };
            // Coverage is not the user's sweep cadence. A loop with a root it
            // has not attached yet — a provider installed after `watch`
            // started — is *polling*, so without this it would retry that root
            // on `--interval`, which the user may have set to an hour. The
            // status output and the docs promise the backstop, so the wait is
            // shortened to it while anything is uncovered; the sweep itself
            // still happens on the interval that was asked for.
            let refresh_every = match self.current_driver() {
                // Events are flowing, so the backstop *is* the cadence for
                // everything the backstop does. Shortening it here would make
                // `--interval 1` re-derive the root set — a walk of every
                // project tree — once a second on a loop that is not polling
                // for changes at all, which is the opposite of what a short
                // interval asks for.
                WatchDriver::FsEvents => self.slow_poll_ms,
                WatchDriver::Polling => match watcher.as_ref() {
                    Some(watch)
                        if !watch.pending().is_empty() || self.roots_refresh.is_some() =>
                    {
                        self.slow_poll_ms.min(self.poll_interval_ms)
                    }
                    _ => sweep_every,
                },
            };
            let check_every = self.check_cadence(watcher.as_ref(), refresh_every);
            let woke_early = refresh_every.min(check_every) < sweep_every;
            let mut idle = Duration::from_millis(sweep_every.min(refresh_every).min(check_every));
            if let Some(at) = retry_force {
                // A change whose sweep never ran. Nothing else will bring the
                // loop back for it: the wake state was cleared when its
                // debounce window opened, so without this the next visit is
                // the backstop, up to `--interval` away.
                idle = idle.min(at.saturating_duration_since(Instant::now()));
            }
            let Some(trigger) = self.inner.wait_for_wake(idle, debounce) else {
                break;
            };
            if self.inner.stopped() {
                break;
            }
            // On a deadline rather than on the trigger. Reconciling on every
            // wake would hammer the watcher during a burst, which is why this
            // used to run only when the wait expired — but an event ends the
            // wait early, so under sustained writes that moment never came and
            // a pending root stayed pending for as long as the writing lasted.
            // The deadline gives the same at-most-once-per-interval rate
            // without depending on how the loop woke up.
            let now = Instant::now();
            let refresh_due = now >= next_refresh;
            // Taken here, on whatever wake came out of the wait, rather than
            // only on the wake a removal caused: a removal reported while the
            // loop was inside its debounce window leaves the bit set behind a
            // wake that says `FsEvent`, and putting the watch back has to
            // happen before the sweep that wake is for, not after it.
            let registration_lost =
                trigger == TickTrigger::RegistrationLost || self.inner.take_registration_lost();
            // A reported removal makes the check due now. The backend will
            // not say so twice, and waiting out the backstop means a
            // recreated directory is unwatched for up to `slow_poll_ms` —
            // long enough for a short session to be written there and cleaned
            // up again with nothing to show for it.
            let check_due = refresh_due || now >= next_check || registration_lost;
            if refresh_due {
                next_refresh = deadline_after(now, refresh_every);
            }
            if check_due {
                if let Some(watch) = watcher.as_mut() {
                    // Re-derive first, then attach: a root can be new as a
                    // *name* (a project that grew a `.trajectories` directory)
                    // rather than merely new on disk, and only the caller
                    // knows how to look for those. Only on the backstop,
                    // because this is the expensive half.
                    //
                    // Counted in with the reconciliation below, because a
                    // root adopted while its directory does not exist yet is
                    // a hole in the coverage this loop reports and
                    // `reconcile` has nothing to say about one it cannot
                    // attach. Publishing on its count alone left `--status`
                    // claiming full coverage for a provider the loop had
                    // taken on and could not watch.
                    let mut changed = 0usize;
                    if refresh_due {
                        if let Some(refresh) = &self.roots_refresh {
                            changed += watch.adopt(refresh());
                        }
                    }
                    // Reconciles rather than only retrying: a root can be
                    // lost after a successful registration, not just before
                    // one.
                    changed += watch.reconcile();
                    if changed > 0 {
                        self.publish_status(Some(&*watch));
                    }
                }
                // Dated from what the check *found*, not from what was true
                // before it ran. A check that just discovered a lost
                // registration has to come back on the recovery cadence; one
                // dated from the cadence that applied a moment earlier would
                // wait out the backstop it was supposed to pre-empt.
                next_check = deadline_after(
                    Instant::now(),
                    self.check_cadence(watcher.as_ref(), refresh_every),
                );
            }
            if trigger == TickTrigger::RegistrationLost {
                // A lost watch is not a change to sweep for. Putting it back
                // was the whole of this wake.
                continue;
            }
            // A sweep the store's lock turned away comes back as the forced
            // tick it was, not as the backstop tick this wake would otherwise
            // have been: the change it is standing in for is still unread, so
            // the fingerprint it would be compared against still cannot be
            // trusted.
            let trigger = if retry_force.is_some_and(|at| Instant::now() >= at) {
                retry_force = None;
                TickTrigger::FsEvent
            } else {
                trigger
            };
            if trigger == TickTrigger::Poll {
                // Reconciled, but not yet due to sweep. Only reached when the
                // wait was deliberately shortened above, so a loop that was
                // not woken early behaves exactly as before.
                if woke_early && last_poll_sweep.elapsed() < Duration::from_millis(sweep_every) {
                    continue;
                }
                last_poll_sweep = std::time::Instant::now();
            }
            if self.inner.run_skip_if_busy(trigger) {
                // Still owed, and the holder may be there for a while. Repeats
                // coalesce into the one deadline, so a busy tree under a long
                // sync costs one retry per window rather than one per event.
                retry_force = Some(deadline_after(Instant::now(), retry_backoff));
                retry_backoff = retry_backoff
                    .saturating_mul(2)
                    .min(self.slow_poll_ms.max(CONTENDED_SWEEP_RETRY_MS));
            } else if trigger.forces_scan() {
                // A forced sweep got through. Whatever was owed is paid, and
                // the next contention starts from the short cadence again.
                retry_force = None;
                retry_backoff = CONTENDED_SWEEP_RETRY_MS;
            }
        }
        let driver = self.current_driver();
        drop(watcher);
        Ok(driver)
    }

    /// How often the registrations are re-checked.
    ///
    /// Shortened by *coverage*, not by driver: while any root is uncovered,
    /// its writes are going nowhere, and that is true whether the other roots
    /// are feeding events or not. It is a stat per root, so the shorter
    /// cadence costs nothing like re-deriving the root set — and it lasts
    /// only until the root is attached again.
    #[cfg(feature = "fs-events")]
    fn check_cadence(&self, watcher: Option<&fs_events::FsWatch>, refresh_every: u64) -> u64 {
        match watcher {
            // A registration that was lost is a directory being replaced, and
            // the replacement is usually immediate. Retried on its own short
            // cadence until it is back, because the user's intervals are not
            // about this and the backstop is long enough to miss an entire
            // session.
            Some(watch) if watch.recovering() => self
                .slow_poll_ms
                .min(self.poll_interval_ms)
                .min(LOST_REGISTRATION_RECHECK_MS),
            Some(watch) if !watch.pending().is_empty() => {
                self.slow_poll_ms.min(self.poll_interval_ms)
            }
            _ => refresh_every,
        }
    }

    /// Without a backend there are no registrations to re-check.
    #[cfg(not(feature = "fs-events"))]
    fn check_cadence(&self, _watcher: Option<&fs_events::FsWatch>, refresh_every: u64) -> u64 {
        refresh_every
    }

    fn current_driver(&self) -> WatchDriver {
        self.status()
            .map(|status| status.driver)
            .unwrap_or(WatchDriver::Polling)
    }

    fn publish_status(&self, watcher: Option<&fs_events::FsWatch>) {
        let status = match watcher {
            // A watcher holding no attachment is not driving anything: the
            // loop is polling until one of its roots shows up.
            Some(watch) if watch.watched().is_empty() => DriverStatus {
                driver: WatchDriver::Polling,
                watched: Vec::new(),
                pending: watch.pending(),
            },
            Some(watch) => DriverStatus {
                driver: WatchDriver::FsEvents,
                watched: watch.watched(),
                pending: watch.pending(),
            },
            // No backend at all — `--no-fsevents`, a build without the
            // feature, or a watcher that could not be brought up. `pending`
            // means "not watched yet, and being retried", and neither half is
            // true here: there is nothing to reconcile, so nothing will ever
            // promote these roots, and they are not uncovered either —
            // polling reads all of them at `--interval`. Reporting them as
            // pending promises a retry that cannot happen.
            None => DriverStatus {
                driver: WatchDriver::Polling,
                watched: Vec::new(),
                pending: Vec::new(),
            },
        };
        let changed = {
            let mut held = self.inner.driver.lock().expect("watch driver");
            let changed = held.as_ref() != Some(&status);
            *held = Some(status.clone());
            changed
        };
        if changed {
            if let Some(sink) = &self.on_driver {
                sink(&status);
            }
        }
    }
}

#[cfg(feature = "fs-events")]
mod fs_events {
    use super::*;
    use notify::event::{EventKind, ModifyKind, RenameMode};
    use notify::{RecursiveMode, Watcher};

    /// Whether an event can change transcript bytes or their names.
    ///
    /// Metadata-only modifications (permissions, ownership, timestamps) are
    /// noise to every source reader and must not bypass the fingerprint with a
    /// forced sweep. Unknown top-level and modify kinds remain conservative
    /// and do wake it: a backend uses those when it cannot classify a real
    /// mutation precisely.
    pub(super) fn event_can_change_evidence(kind: &EventKind) -> bool {
        matches!(
            kind,
            EventKind::Any | EventKind::Other | EventKind::Create(_) | EventKind::Remove(_)
        ) || matches!(kind, EventKind::Modify(modify) if !matches!(modify, ModifyKind::Metadata(_)))
    }

    /// Whether this event belongs to the watched evidence set.
    ///
    /// Rescan notices are global by definition: the backend is reporting that
    /// paths were lost, and notify emits them with an empty path list. Unknown
    /// pathless events are treated the same conservative way; a typed event
    /// still has to name a path covered by one of the roots.
    pub(super) fn event_matches_evidence(event: &notify::Event, roots: &[WatchRoot]) -> bool {
        event.need_rescan()
            || (event.paths.is_empty()
                && matches!(event.kind, EventKind::Any | EventKind::Other))
            || event
                .paths
                .iter()
                .any(|path| event_matches_roots(path, roots))
    }

    /// Registrations a removal or rename-away invalidated.
    ///
    /// Rename modes preserve path direction: `From` and the first side of
    /// `Both` remove the old name, while `To` only introduces a new one and
    /// must not retire a live registration. FSEvents reports its one-sided
    /// renames as `Any`, so that mode is conservatively source-like when it
    /// names the registration itself. A rename below the root never matches
    /// [`WatchRoot::registers_at`] and therefore cannot retire the root.
    pub(super) fn lost_registration_keys(
        event: &notify::Event,
        roots: &[WatchRoot],
    ) -> Vec<PathBuf> {
        let paths: &[PathBuf] = match event.kind {
            EventKind::Remove(_) => &event.paths,
            EventKind::Modify(ModifyKind::Name(
                RenameMode::From | RenameMode::Both | RenameMode::Any | RenameMode::Other,
            )) => event.paths.first().map(std::slice::from_ref).unwrap_or(&[]),
            _ => &[],
        };
        paths
            .iter()
            .flat_map(|path| removed_registration_keys(path, roots))
            .collect()
    }

    pub(super) fn handle_backend_error(
        error: anyhow::Error,
        roots: &Mutex<Vec<WatchRoot>>,
        stale: &Mutex<HashSet<PathBuf>>,
        on_registration_lost: &dyn Fn(),
        on_error: &dyn Fn(anyhow::Error),
    ) {
        let registration_keys = roots
            .lock()
            .expect("watch roots")
            .iter()
            .map(|root| root.registration_key().to_path_buf())
            .collect::<Vec<_>>();
        if !registration_keys.is_empty() {
            stale
                .lock()
                .expect("stale roots")
                .extend(registration_keys);
            on_registration_lost();
        }
        on_error(error);
    }

    /// Holds the OS-level watches open; dropping it stops them.
    ///
    /// Roots that did not exist at attach time stay in `pending` rather than
    /// being dropped. A watch on a path that is not there yet cannot be
    /// registered, and never retrying would leave a provider installed after
    /// the loop started permanently uncovered while the loop still reported
    /// itself as event-driven.
    pub(super) struct FsWatch {
        watcher: notify::RecommendedWatcher,
        /// The roots currently registered, each with the directory object it
        /// was registered against. Kept whole rather than as paths: a
        /// registration that died with its directory has to be *re*-made,
        /// which needs the depth it asked for, and telling a live
        /// registration from a dead one needs the identity.
        watched: Vec<Registered>,
        pending: Vec<WatchRoot>,
        /// The roots that *were* attached and are uncovered now, so they
        /// are being retried on the recovery cadence rather than on the
        /// backstop.
        ///
        /// Per root, not a flag: `pending` also holds roots that have never
        /// existed — `~/.claude/projects` on a machine without Claude — and
        /// those may never attach at all. A flag cleared only when `pending`
        /// empties would hold the short cadence open for the rest of the run
        /// after a single recreate, stat-ing every root four times a second
        /// on exactly the long-lived watch the backstop exists to keep cheap.
        /// Keyed by the root's own path, which is how `adopt` and the
        /// callback's copy identify a root too, and which — unlike the
        /// registration key — does not change under it when a symlinked root
        /// is re-resolved on the way back in.
        recovering: HashSet<PathBuf>,
        /// Registered paths the backend reported as removed, shared with the
        /// event callback. A watch dies with the directory it names, and the
        /// name can come back over a *different* object — or over one the
        /// filesystem gave the same inode number, which is routine when a
        /// directory is deleted and immediately recreated. The event is the
        /// only reliable statement that the registration is gone.
        stale: Arc<Mutex<HashSet<PathBuf>>>,
        /// Every root and the depth it asked for, shared with the event
        /// callback so it can drop what the backend over-delivered.
        depth: Arc<Mutex<Vec<WatchRoot>>>,
    }

    impl FsWatch {
        /// Whether a registration that once existed is waiting to be re-made.
        pub(super) fn recovering(&self) -> bool {
            !self.recovering.is_empty()
        }

        pub(super) fn watched(&self) -> Vec<PathBuf> {
            self.watched
                .iter()
                .map(|entry| entry.root.path.clone())
                .collect()
        }

        pub(super) fn pending(&self) -> Vec<PathBuf> {
            self.pending.iter().map(|root| root.path.clone()).collect()
        }

        /// Keep the callback's copy of a root in step with the registered
        /// one, so it matches against the same spellings.
        fn remember(&self, root: &WatchRoot) {
            let mut known = self.depth.lock().expect("watch roots");
            match known.iter_mut().find(|seen| seen.path == root.path) {
                Some(seen) => *seen = root.clone(),
                None => known.push(root.clone()),
            }
        }

        /// Take on roots that were not known at startup — a `.trajectories`
        /// directory created in a project an hour into the run. Returns how
        /// many are new, so the caller knows whether to retry attaching.
        pub(super) fn adopt(&mut self, roots: Vec<WatchRoot>) -> usize {
            let mut known = self.depth.lock().expect("watch roots");
            let mut added = 0usize;
            for root in roots {
                if known.iter().any(|seen| seen.path == root.path) {
                    continue;
                }
                known.push(root.clone());
                self.pending.push(root);
                added += 1;
            }
            added
        }

        /// Re-register the roots whose directory is no longer the one they
        /// were registered against, and return the ones that have no directory
        /// at all to `pending`.
        ///
        /// A watch is bound to the directory *object*, not to its name. Delete
        /// a watched `~/.codex/sessions` and the kernel drops the watch with
        /// the inode; recreate it and the name is back while the watch is not,
        /// so the loop goes on reporting coverage it does not have and the
        /// next transcript written there wakes nothing. Nothing in `pending`
        /// covers that: those are roots whose *initial* registration failed.
        ///
        /// The identity is what makes this cheap and safe. Re-registering
        /// blindly would be neither: `notify` 8.2 does not replace a live
        /// registration — its FSEvents backend appends the path to the array
        /// it rebuilds the stream from, so a long run accumulates duplicates,
        /// and its inotify backend re-walks the entire tree of a recursive
        /// root with `WalkDir` on every call. Comparing the directory object
        /// against the one registered is a `stat` per root per backstop tick,
        /// and the backend is only touched when the answer changed.
        ///
        /// Returns how many roots changed side, so the caller knows whether to
        /// republish its status.
        pub(super) fn reconcile(&mut self) -> usize {
            let mut changed = 0usize;
            let mut lost = Vec::new();
            let mut resolved = Vec::new();
            let watcher = &mut self.watcher;
            let mut stale = self.stale.lock().expect("stale roots");
            self.watched.retain_mut(|entry| {
                let reported_gone = stale.remove(entry.root.registration_key());
                let current = root_identity(entry.root.registered_path());
                if !reported_gone && current.is_some() && current == entry.identity {
                    return true;
                }
                // Best effort: the old watch may already be gone with its
                // directory, and failing to drop it is not a reason to keep
                // claiming it.
                let _ = watcher.unwatch(entry.root.registration_key());
                if current.is_some() && register(watcher, &mut entry.root) {
                    // Same name, new directory object: re-registered against
                    // the one that is there now, and re-resolved with it — a
                    // symlinked root whose target moved is a new spelling as
                    // well as a new object.
                    entry.identity = root_identity(entry.root.registered_path());
                    resolved.push(entry.root.clone());
                    changed += 1;
                    return true;
                }
                lost.push(entry.root.clone());
                false
            });
            drop(stale);
            for root in resolved {
                self.remember(&root);
            }
            // Anything that falls out of `watched` here was attached a moment
            // ago, which is what makes it a recovery rather than a root that
            // has never existed. `retry_pending` takes each one back out as it
            // re-attaches, so the short cadence lasts exactly as long as the
            // recovery does and not as long as the emptiest root in `pending`.
            for root in lost {
                self.recovering.insert(root.path.clone());
                self.pending.push(root);
                changed += 1;
            }
            let attached = self.retry_pending();
            changed + attached
        }

        /// Try the roots that were not there before. Returns how many were
        /// picked up, so the caller knows whether to republish its status.
        pub(super) fn retry_pending(&mut self) -> usize {
            if self.pending.is_empty() {
                return 0;
            }
            let mut attached = 0usize;
            let watcher = &mut self.watcher;
            let watched = &mut self.watched;
            let recovering = &mut self.recovering;
            let mut resolved = Vec::new();
            self.pending.retain_mut(|root| {
                if !register(watcher, root) {
                    return true;
                }
                recovering.remove(&root.path);
                resolved.push(root.clone());
                watched.push(Registered {
                    identity: root_identity(root.registered_path()),
                    root: root.clone(),
                });
                attached += 1;
                false
            });
            for root in resolved {
                self.remember(&root);
            }
            attached
        }
    }

    /// Watch every existing path in `roots`, calling `on_change` for each
    /// create/modify/remove event.
    ///
    /// Depth comes from each root: a transcript tree is recursive because a
    /// new session is a new file somewhere under it, while a directory holding
    /// one flat log is not, so the unrelated churn beside it costs nothing.
    ///
    /// Errors only when the backend itself could not be brought up. A root
    /// that does not exist is not an error — it becomes `pending`.
    pub(super) fn attach(
        roots: &[WatchRoot],
        on_change: impl Fn() + Send + 'static,
        on_registration_lost: impl Fn() + Send + 'static,
        on_error: impl Fn(anyhow::Error) + Send + 'static,
    ) -> Result<FsWatch> {
        // The callback has to be able to see the roots to enforce their depth,
        // and `retry_pending` adds to that set later, so it is shared rather
        // than captured by value.
        let depth: Arc<Mutex<Vec<WatchRoot>>> = Arc::new(Mutex::new(roots.to_vec()));
        // Replaced below with the resolved roots, once each has been asked.
        let depth_for_events = depth.clone();
        let stale: Arc<Mutex<HashSet<PathBuf>>> = Arc::new(Mutex::new(HashSet::new()));
        let stale_for_events = stale.clone();
        let mut watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
            let event = match event {
                Ok(event) => event,
                Err(error) => {
                    // A backend error means its coverage can no longer be
                    // trusted, even when it cannot name the failed root. Mark
                    // every registration stale and wake reconciliation; the
                    // polling backstop remains active while reattachment is
                    // attempted on the short recovery cadence.
                    handle_backend_error(
                        error.into(),
                        &depth_for_events,
                        &stale_for_events,
                        &on_registration_lost,
                        &on_error,
                    );
                    return;
                }
            };
            // Metadata churn — an atime bump from a backup or an antivirus
            // scan — does not change the bytes a sweep would read. Filtering
            // here keeps wakeups honest on a noisy home directory.
            if !event.need_rescan() && !event_can_change_evidence(&event.kind) {
                return;
            }
            let (matched, removed) = {
                let roots = depth_for_events.lock().expect("watch roots");
                let matched = event_matches_evidence(&event, &roots);
                // A registered path that was removed takes its watch with it,
                // whatever the name does afterwards. Recorded here because the
                // backend will not say so again, and a `stat` later cannot
                // tell a recreated directory from the original one when the
                // filesystem reuses the inode number.
                let removed = lost_registration_keys(&event, &roots);
                (matched, removed)
            };
            if !removed.is_empty() {
                {
                    let mut stale = stale_for_events.lock().expect("stale roots");
                    stale.extend(removed);
                }
                // Recorded *and* announced. Recording alone leaves the
                // registration dead until the next backstop, and for a
                // directory that is recreated straight away that window is
                // long enough to miss a whole session.
                on_registration_lost();
            }
            if matched {
                on_change();
            }
        })?;
        let mut watched = Vec::new();
        let mut pending = Vec::new();
        let mut known = Vec::new();
        for root in roots {
            let mut root = root.clone();
            if register(&mut watcher, &mut root) {
                known.push(root.clone());
                watched.push(Registered {
                    identity: root_identity(root.registered_path()),
                    root,
                });
            } else {
                known.push(root.clone());
                pending.push(root);
            }
        }
        // The callback matches against these, so it has to see the resolved
        // spellings rather than the ones the caller handed in.
        *depth.lock().expect("watch roots") = known;
        Ok(FsWatch {
            watcher,
            watched,
            pending,
            recovering: HashSet::new(),
            stale,
            depth,
        })
    }

    /// One registration: the root, and the directory object it was made
    /// against.
    pub(super) struct Registered {
        pub(super) root: WatchRoot,
        identity: Option<RootIdentity>,
    }

    /// Which directory object a *name* currently refers to.
    ///
    /// Always asked through the lexical path, never through the resolved one,
    /// because the two answer different questions and only this one notices a
    /// symlink being retargeted. `~/sessions -> /disk-a/sessions` resolves to
    /// `/disk-a/sessions` and registers there; repoint it at `/disk-b` and
    /// `/disk-a` still exists with the same inode, so stat-ing the resolved
    /// path says nothing changed while the name now means somewhere else
    /// entirely. Stat-ing the name follows the link as it is *now*, so the
    /// identity moves and the root is re-resolved and re-registered.
    ///
    /// The resolved spelling remains the key for the stale set and for
    /// `unwatch`: those are about which registration this is, which is a
    /// different question from whether the name still points at it.
    ///
    /// On Unix that is exactly `(device, inode)` — the pair a watch is bound
    /// to. Elsewhere it is the creation time, which changes when a directory
    /// is replaced but is a weaker statement; the cost of the difference is a
    /// missed re-registration in a case the platform cannot distinguish, not a
    /// wrong one.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct RootIdentity {
        #[cfg(unix)]
        device: u64,
        #[cfg(unix)]
        inode: u64,
        #[cfg(not(unix))]
        created: Option<std::time::SystemTime>,
    }

    fn root_identity(path: &Path) -> Option<RootIdentity> {
        let metadata = std::fs::metadata(path).ok()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            Some(RootIdentity {
                device: metadata.dev(),
                inode: metadata.ino(),
            })
        }
        #[cfg(not(unix))]
        {
            Some(RootIdentity {
                created: metadata.created().ok(),
            })
        }
    }

    /// Successful backend registrations since the process started.
    ///
    /// The count is the only way to state the property that matters here from
    /// outside: a live root must not be registered again on every backstop
    /// tick.
    #[cfg(test)]
    pub(super) fn registrations() -> usize {
        REGISTRATIONS.load(std::sync::atomic::Ordering::Relaxed)
    }

    static REGISTRATIONS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

    fn register(watcher: &mut notify::RecommendedWatcher, root: &mut WatchRoot) -> bool {
        // Ask the filesystem for the root's real spelling first, and register
        // *that*. Both backends then report paths this root recognises: it
        // keeps the spelling it was given as well, because inotify echoes
        // whichever path the watch was registered with while FSEvents always
        // reports the resolved one. One call per registration, and none per
        // event.
        root.resolve();
        // A file root registers its parent, so it is the parent's existence
        // that decides whether the root is coverable yet — and a file that
        // does not exist inside a directory that does is covered from the
        // start, which is the point of watching the parent.
        let target = root.registration_key();
        if !target.exists() {
            return false;
        }
        let mode = match root.depth {
            WatchDepth::Tree => RecursiveMode::Recursive,
            WatchDepth::Directory | WatchDepth::File => RecursiveMode::NonRecursive,
        };
        let registered = watcher.watch(target, mode).is_ok();
        if registered {
            REGISTRATIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        registered
    }
}

#[cfg(not(feature = "fs-events"))]
mod fs_events {
    use super::*;

    pub(super) struct FsWatch;

    impl FsWatch {
        pub(super) fn watched(&self) -> Vec<PathBuf> {
            Vec::new()
        }

        pub(super) fn pending(&self) -> Vec<PathBuf> {
            Vec::new()
        }

        pub(super) fn reconcile(&mut self) -> usize {
            0
        }

        pub(super) fn recovering(&self) -> bool {
            false
        }

        pub(super) fn adopt(&mut self, _roots: Vec<WatchRoot>) -> usize {
            0
        }

        pub(super) fn retry_pending(&mut self) -> usize {
            0
        }
    }

    /// Without the `fs-events` feature there is no watcher backend, so every
    /// caller falls back to polling. The signature matches the real one so the
    /// loop itself is identical in both builds.
    pub(super) fn attach(
        _roots: &[WatchRoot],
        _on_change: impl Fn() + Send + 'static,
        _on_registration_lost: impl Fn() + Send + 'static,
        _on_error: impl Fn(anyhow::Error) + Send + 'static,
    ) -> Result<FsWatch> {
        anyhow::bail!("ai-hist was built without the fs-events feature")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "fs-events")]
    #[test]
    fn metadata_only_events_do_not_force_a_sweep() {
        use notify::event::{
            AccessKind, CreateKind, EventKind, MetadataKind, ModifyKind, RemoveKind,
        };

        assert!(!fs_events::event_can_change_evidence(&EventKind::Modify(
            ModifyKind::Metadata(MetadataKind::Any)
        )));
        assert!(!fs_events::event_can_change_evidence(&EventKind::Access(
            AccessKind::Any
        )));
        for kind in [
            EventKind::Any,
            EventKind::Other,
            EventKind::Create(CreateKind::Any),
            EventKind::Modify(ModifyKind::Any),
            EventKind::Remove(RemoveKind::Any),
        ] {
            assert!(
                fs_events::event_can_change_evidence(&kind),
                "{kind:?} can change evidence and must wake a forced sweep"
            );
        }
    }

    #[cfg(feature = "fs-events")]
    #[test]
    fn pathless_rescan_and_unknown_events_force_a_sweep() {
        use notify::event::{EventKind, Flag};

        let roots = vec![WatchRoot::tree("/home/u/.codex/sessions")];
        let overflow = notify::Event::new(EventKind::Other).set_flag(Flag::Rescan);
        assert!(fs_events::event_matches_evidence(&overflow, &roots));
        assert!(fs_events::event_matches_evidence(
            &notify::Event::new(EventKind::Any),
            &roots
        ));

        let outside = notify::Event::new(EventKind::Create(
            notify::event::CreateKind::File,
        ))
        .add_path(PathBuf::from("/home/u/unrelated"));
        assert!(!fs_events::event_matches_evidence(&outside, &roots));
    }

    #[cfg(feature = "fs-events")]
    #[test]
    fn a_backend_error_marks_every_registration_stale_and_reports_it() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let first = PathBuf::from("/home/u/.claude/projects");
        let second = PathBuf::from("/home/u/.codex/sessions");
        let roots = Mutex::new(vec![
            WatchRoot::tree(first.clone()),
            WatchRoot::tree(second.clone()),
        ]);
        let stale = Mutex::new(HashSet::new());
        let lost = AtomicUsize::new(0);
        let errors = Mutex::new(Vec::new());

        fs_events::handle_backend_error(
            anyhow::anyhow!("backend rescan lost"),
            &roots,
            &stale,
            &|| {
                lost.fetch_add(1, Ordering::SeqCst);
            },
            &|error| errors.lock().unwrap().push(error.to_string()),
        );

        assert_eq!(lost.load(Ordering::SeqCst), 1);
        assert_eq!(
            *stale.lock().unwrap(),
            HashSet::from([first, second]),
            "unknown backend coverage loss must make every root reconcile"
        );
        assert_eq!(
            errors.lock().unwrap().as_slice(),
            ["backend rescan lost"]
        );
    }

    #[cfg(feature = "fs-events")]
    #[test]
    fn rename_away_retires_only_the_source_registration() {
        use notify::event::{EventKind, ModifyKind, RenameMode};

        let root = PathBuf::from("/home/u/.codex/sessions");
        let backup = PathBuf::from("/home/u/.codex/sessions-old");
        let child = root.join("2026/rollout.jsonl");
        let roots = vec![WatchRoot::tree(root.clone())];
        let key = vec![root.clone()];

        let from = notify::Event::new(EventKind::Modify(ModifyKind::Name(RenameMode::From)))
            .add_path(root.clone());
        assert_eq!(fs_events::lost_registration_keys(&from, &roots), key);

        let both = notify::Event::new(EventKind::Modify(ModifyKind::Name(RenameMode::Both)))
            .add_path(root.clone())
            .add_path(backup.clone());
        assert_eq!(fs_events::lost_registration_keys(&both, &roots), key);

        let into = notify::Event::new(EventKind::Modify(ModifyKind::Name(RenameMode::To)))
            .add_path(root.clone());
        assert!(fs_events::lost_registration_keys(&into, &roots).is_empty());

        let into_both = notify::Event::new(EventKind::Modify(ModifyKind::Name(RenameMode::Both)))
            .add_path(backup)
            .add_path(root);
        assert!(fs_events::lost_registration_keys(&into_both, &roots).is_empty());

        let below = notify::Event::new(EventKind::Modify(ModifyKind::Name(RenameMode::From)))
            .add_path(child);
        assert!(fs_events::lost_registration_keys(&below, &roots).is_empty());
    }

    /// An interval large enough to overflow the clock must not kill the loop.
    ///
    /// `--debounce-ms` takes any `u64`, and `Instant + Duration` panics on a
    /// sum it cannot represent, so the largest one starts the loop normally
    /// and dies on the first change event — the shape of failure where the
    /// configuration looks accepted and the capture silently stops.
    #[test]
    fn an_interval_too_large_for_the_clock_does_not_kill_the_wait() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let loop_ = WatchLoop::new(Arc::new(|_| Ok(TickOutcome::default())))
            .with_debounce_ms(u64::MAX)
            .with_poll_interval_ms(u64::MAX)
            .with_slow_poll_ms(u64::MAX);

        // And the wait itself survives a duration that was never bounded,
        // whatever a future caller does to the public fields.
        let inner = loop_.inner.clone();
        let entered = Arc::new(AtomicBool::new(false));
        let waiter = {
            let inner = inner.clone();
            let entered = entered.clone();
            std::thread::spawn(move || {
                entered.store(true, Ordering::SeqCst);
                inner.sleep_through_debounce(Duration::from_millis(u64::MAX));
            })
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        while !entered.load(Ordering::SeqCst) {
            assert!(Instant::now() < deadline, "the waiter never started");
            std::thread::yield_now();
        }
        inner.request_stop();
        assert!(
            waiter.join().is_ok(),
            "an unrepresentable deadline must not panic the waiting thread"
        );

        // Positive control on the bound itself: the value is clamped where it
        // enters, so nothing downstream has to remember to defend. On a
        // platform whose clock *can* represent `u64::MAX` milliseconds the
        // wait above does not panic — it waits for 584 million years, which
        // stops live capture just as thoroughly — so this is the assertion
        // that holds everywhere.
        assert_eq!(loop_.debounce_ms, MAX_INTERVAL_MS);
        assert_eq!(loop_.poll_interval_ms, MAX_INTERVAL_MS);
        assert_eq!(loop_.slow_poll_ms, MAX_INTERVAL_MS);
    }

    /// Reconciliation must not re-register a root that is still live.
    ///
    /// `notify` 8.2 does not replace a registration: its FSEvents backend
    /// appends the path to the array it rebuilds the stream from — duplicates
    /// accumulate for as long as the process runs — and its inotify backend
    /// re-walks the whole tree of a recursive root with `WalkDir` on every
    /// call. A backstop that re-registered blindly would therefore grow the
    /// watcher without bound on macOS and re-walk `~/.claude/projects` every
    /// 30 seconds on Linux, in the loop whose whole purpose is to be cheap
    /// when nothing changed.
    #[cfg(feature = "fs-events")]
    #[test]
    fn a_live_root_is_registered_once_however_many_backstop_ticks_pass() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("sessions");
        std::fs::create_dir_all(&root).expect("root");

        let ticks = Arc::new(AtomicUsize::new(0));
        let counted = ticks.clone();
        let before = fs_events::registrations();
        let watch = Arc::new(
            WatchLoop::new(Arc::new(move |_| {
                counted.fetch_add(1, Ordering::SeqCst);
                Ok(TickOutcome::default())
            }))
            .with_immediate(false)
            .with_fs_events(true)
            .with_roots(vec![WatchRoot::tree(root.clone())])
            .with_debounce_ms(20)
            // Short, because backstop ticks are what this test counts against.
            .with_slow_poll_ms(20)
            .with_poll_interval_ms(20),
        );
        let runner = watch.clone();
        let thread = std::thread::spawn(move || {
            runner.run().expect("watch loop run");
        });

        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while watch.driver() != Some(WatchDriver::FsEvents) {
            assert!(
                std::time::Instant::now() < deadline,
                "the watcher never attached"
            );
            std::thread::yield_now();
        }
        let attached = fs_events::registrations();
        assert_eq!(
            attached - before,
            1,
            "attaching one root is one registration"
        );

        // Synchronised on the loop's own ticks rather than on a sleep: ten
        // backstop ticks have passed by the time this returns.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while ticks.load(Ordering::SeqCst) < 10 {
            assert!(
                std::time::Instant::now() < deadline,
                "the backstop never ticked"
            );
            std::thread::yield_now();
        }
        assert_eq!(
            fs_events::registrations(),
            attached,
            "a live root must not be registered again on every backstop tick"
        );

        // Positive control: the reconciliation that declined to re-register a
        // live root still repairs a dead one, so the assertion above is not
        // passing because reconciliation does nothing at all.
        std::fs::remove_dir_all(&root).expect("delete the root");
        std::fs::create_dir_all(&root).expect("recreate the root");
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while fs_events::registrations() == attached {
            assert!(
                std::time::Instant::now() < deadline,
                "a recreated root was never registered again"
            );
            std::thread::yield_now();
        }

        watch.stop();
        let _ = thread.join();
    }


    /// The depth filter is what makes `WatchRoot::directory` mean the same
    /// thing on every platform. It is unit-tested rather than only exercised
    /// end to end because the backend it defends against is macOS FSEvents,
    /// which has no non-recursive mode — on Linux the kernel filters first, so
    /// an integration test there would pass with the filter removed.
    #[test]
    fn a_non_recursive_root_covers_its_own_entries_only() {
        let roots = vec![WatchRoot::directory("/home/u/.claude")];

        assert!(event_matches_roots(
            Path::new("/home/u/.claude/history.jsonl"),
            &roots
        ));
        assert!(event_matches_roots(Path::new("/home/u/.claude"), &roots));

        for buried in [
            "/home/u/.claude/todos/task-1.json",
            "/home/u/.claude/shell-snapshots/snapshot-1.sh",
            "/home/u/.claude/projects/app/session.jsonl",
        ] {
            assert!(
                !event_matches_roots(Path::new(buried), &roots),
                "{buried} is below a non-recursive root and must not wake a forced sweep"
            );
        }
        assert!(!event_matches_roots(Path::new("/home/u/.codex"), &roots));
    }

    #[test]
    fn a_recursive_root_covers_its_whole_subtree() {
        let roots = vec![WatchRoot::tree("/home/u/.claude/projects")];

        for inside in [
            "/home/u/.claude/projects",
            "/home/u/.claude/projects/app/session.jsonl",
            "/home/u/.claude/projects/app/session/subagents/agent-a.jsonl",
        ] {
            assert!(event_matches_roots(Path::new(inside), &roots), "{inside}");
        }
        assert!(!event_matches_roots(
            Path::new("/home/u/.claude/todos/task-1.json"),
            &roots
        ));
    }

    /// The two depths coexist: a path below a non-recursive root is still
    /// covered when some other root is recursive over it.
    #[test]
    fn the_widest_matching_root_decides() {
        let roots = vec![
            WatchRoot::directory("/home/u/.claude"),
            WatchRoot::tree("/home/u/.claude/projects"),
        ];

        assert!(event_matches_roots(
            Path::new("/home/u/.claude/projects/app/session.jsonl"),
            &roots
        ));
        assert!(!event_matches_roots(
            Path::new("/home/u/.claude/todos/task-1.json"),
            &roots
        ));
    }
}
