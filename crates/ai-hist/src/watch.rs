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

use crate::discover::{WatchDepth, WatchRoot};

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
    roots.iter().any(|root| root.covers(path))
}

#[derive(Default)]
struct WakeState {
    /// A change signal is pending. Single-bit on purpose: a thousand events
    /// between two ticks cost one wakeup, not a thousand.
    pending: bool,
    stopped: bool,
}

#[derive(Default)]
struct RunState {
    in_flight: bool,
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
        {
            let mut run = self.inner.run.lock().expect("watch run state");
            run.in_flight = false;
            run.completed = run.completed.wrapping_add(1);
        }
        self.inner.run_cv.notify_all();
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

    fn request_stop(&self) {
        {
            let mut wake = self.wake.lock().expect("watch wake state");
            wake.stopped = true;
        }
        self.wake_cv.notify_all();
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
            if wake.pending {
                // Clear before the window, not after: events that land during
                // the debounce set it again and the next wait observes them
                // immediately. That is what gives sustained writes a steady
                // ~debounce cadence instead of a wait for quiet.
                wake.pending = false;
                drop(wake);
                self.sleep_unless_stopped(debounce);
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

    /// Sleep for `duration`, returning early when the loop is stopped.
    fn sleep_unless_stopped(&self, duration: Duration) {
        // Bounded before it reaches an `Instant`, because these durations come
        // from the command line and the addition panics on a sum it cannot
        // represent.
        let duration = duration.min(Duration::from_millis(MAX_INTERVAL_MS));
        let mut wake = self.wake.lock().expect("watch wake state");
        let Some(deadline) = Instant::now().checked_add(duration) else {
            // Unreachable given the bound above. A platform whose clock cannot
            // represent even that should wait for the stop signal rather than
            // spin through zero-length sleeps.
            while !wake.stopped {
                wake = self.wake_cv.wait(wake).expect("watch wake state");
            }
            return;
        };
        while !wake.stopped {
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

    /// Claim the in-flight slot, or `None` when a tick is already running.
    fn claim(&self) -> Option<InFlight<'_>> {
        let mut run = self.run.lock().expect("watch run state");
        if run.in_flight {
            return None;
        }
        run.in_flight = true;
        Some(InFlight { inner: self })
    }

    /// Driver path: a tick arriving while one is in flight is dropped, not
    /// queued. Queuing would run two sweeps back to back with no gap right
    /// after a slow one — the spike the interval exists to avoid.
    fn run_skip_if_busy(&self, trigger: TickTrigger) {
        let Some(guard) = self.claim() else {
            return;
        };
        self.run_claimed(trigger, guard);
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
            None => self.run_claimed(trigger, InFlight { inner: self }),
            Some(target) => {
                let mut run = self.run.lock().expect("watch run state");
                while run.completed < target {
                    run = self.run_cv.wait(run).expect("watch run state");
                }
            }
        }
    }

    fn run_claimed(&self, trigger: TickTrigger, guard: InFlight<'_>) {
        let forced = trigger.forces_scan();
        match (self.tick)(forced) {
            Ok(outcome) => {
                if let Some(sink) = &self.on_report {
                    sink(&TickReport {
                        trigger,
                        forced,
                        outcome,
                    });
                }
            }
            Err(error) => {
                if let Some(sink) = &self.on_error {
                    sink(&error);
                } else {
                    eprintln!("ai-hist: watch tick failed: {error:#}");
                }
            }
        }
        drop(guard);
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
            fs_events::attach(&self.roots, move || inner.signal_change()).ok()
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
        let mut next_reconcile = deadline_after(Instant::now(), self.slow_poll_ms);
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
            let reconcile_every = match watcher.as_ref() {
                Some(watch)
                    if !watch.pending().is_empty() || self.roots_refresh.is_some() =>
                {
                    self.slow_poll_ms.min(self.poll_interval_ms)
                }
                _ => sweep_every,
            };
            let woke_early = reconcile_every < sweep_every;
            let idle = Duration::from_millis(sweep_every.min(reconcile_every));
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
            if now >= next_reconcile {
                next_reconcile = deadline_after(now, reconcile_every);
                if let Some(watch) = watcher.as_mut() {
                    // Re-derive first, then attach: a root can be new as a
                    // *name* (a project that grew a `.trajectories` directory)
                    // rather than merely new on disk, and only the caller
                    // knows how to look for those.
                    if let Some(refresh) = &self.roots_refresh {
                        watch.adopt(refresh());
                    }
                    // Reconciles rather than only retrying: a root can be
                    // lost after a successful registration, not just before
                    // one.
                    if watch.reconcile() > 0 {
                        self.publish_status(Some(&*watch));
                    }
                }
            }
            if trigger == TickTrigger::Poll {
                // Reconciled, but not yet due to sweep. Only reached when the
                // wait was deliberately shortened above, so a loop that was
                // not woken early behaves exactly as before.
                if woke_early && last_poll_sweep.elapsed() < Duration::from_millis(sweep_every) {
                    continue;
                }
                last_poll_sweep = std::time::Instant::now();
            }
            self.inner.run_skip_if_busy(trigger);
        }
        let driver = self.current_driver();
        drop(watcher);
        Ok(driver)
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
            None => DriverStatus {
                driver: WatchDriver::Polling,
                watched: Vec::new(),
                pending: self.roots.iter().map(|root| root.path.clone()).collect(),
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
    use notify::event::EventKind;
    use notify::{RecursiveMode, Watcher};

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
        pub(super) fn watched(&self) -> Vec<PathBuf> {
            self.watched
                .iter()
                .map(|entry| entry.root.path.clone())
                .collect()
        }

        pub(super) fn pending(&self) -> Vec<PathBuf> {
            self.pending.iter().map(|root| root.path.clone()).collect()
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
            let watcher = &mut self.watcher;
            let mut stale = self.stale.lock().expect("stale roots");
            self.watched.retain_mut(|entry| {
                let reported_gone = stale.remove(entry.root.registered_path());
                let current = root_identity(entry.root.registered_path());
                if !reported_gone && current.is_some() && current == entry.identity {
                    return true;
                }
                // Best effort: the old watch may already be gone with its
                // directory, and failing to drop it is not a reason to keep
                // claiming it.
                let _ = watcher.unwatch(entry.root.registered_path());
                if current.is_some() && register(watcher, &entry.root) {
                    // Same name, new directory object: re-registered against
                    // the one that is there now.
                    entry.identity = current;
                    changed += 1;
                    return true;
                }
                lost.push(entry.root.clone());
                false
            });
            drop(stale);
            for root in lost {
                self.pending.push(root);
                changed += 1;
            }
            changed + self.retry_pending()
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
            self.pending.retain(|root| {
                if !register(watcher, root) {
                    return true;
                }
                watched.push(Registered {
                    identity: root_identity(root.registered_path()),
                    root: root.clone(),
                });
                attached += 1;
                false
            });
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
    ) -> Result<FsWatch> {
        // The callback has to be able to see the roots to enforce their depth,
        // and `retry_pending` adds to that set later, so it is shared rather
        // than captured by value.
        let depth: Arc<Mutex<Vec<WatchRoot>>> = Arc::new(Mutex::new(roots.to_vec()));
        let depth_for_events = depth.clone();
        let stale: Arc<Mutex<HashSet<PathBuf>>> = Arc::new(Mutex::new(HashSet::new()));
        let stale_for_events = stale.clone();
        let mut watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
            let Ok(event) = event else {
                return;
            };
            // Metadata churn — an atime bump from a backup or an antivirus
            // scan — does not change the bytes a sweep would read. Filtering
            // here keeps wakeups honest on a noisy home directory.
            if !matches!(
                event.kind,
                EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
            ) {
                return;
            }
            let (matched, removed) = {
                let roots = depth_for_events.lock().expect("watch roots");
                let matched = event
                    .paths
                    .iter()
                    .any(|path| event_matches_roots(path, &roots));
                // A registered path that was removed takes its watch with it,
                // whatever the name does afterwards. Recorded here because the
                // backend will not say so again, and a `stat` later cannot
                // tell a recreated directory from the original one when the
                // filesystem reuses the inode number.
                let removed = matches!(event.kind, EventKind::Remove(_))
                    .then(|| {
                        event
                            .paths
                            .iter()
                            .filter(|path| {
                                roots
                                    .iter()
                                    .any(|root| root.registered_path() == path.as_path())
                            })
                            .cloned()
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                (matched, removed)
            };
            if !removed.is_empty() {
                let mut stale = stale_for_events.lock().expect("stale roots");
                stale.extend(removed);
            }
            if matched {
                on_change();
            }
        })?;
        let mut watched = Vec::new();
        let mut pending = Vec::new();
        for root in roots {
            if register(&mut watcher, root) {
                watched.push(Registered {
                    identity: root_identity(root.registered_path()),
                    root: root.clone(),
                });
            } else {
                pending.push(root.clone());
            }
        }
        Ok(FsWatch {
            watcher,
            watched,
            pending,
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

    /// Which directory object a name currently refers to.
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

    fn register(watcher: &mut notify::RecommendedWatcher, root: &WatchRoot) -> bool {
        // A file root registers its parent, so it is the parent's existence
        // that decides whether the root is coverable yet — and a file that
        // does not exist inside a directory that does is covered from the
        // start, which is the point of watching the parent.
        let target = root.registered_path();
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
    ) -> Result<FsWatch> {
        anyhow::bail!("ai-hist was built without the fs-events feature")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
                inner.sleep_unless_stopped(Duration::from_millis(u64::MAX));
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
