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

use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;

use crate::discover::WatchRoot;

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
        let deadline = Instant::now() + duration;
        let mut wake = self.wake.lock().expect("watch wake state");
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
        self.debounce_ms = debounce_ms;
        self
    }

    pub fn with_poll_interval_ms(mut self, poll_interval_ms: u64) -> Self {
        self.poll_interval_ms = poll_interval_ms;
        self
    }

    pub fn with_slow_poll_ms(mut self, slow_poll_ms: u64) -> Self {
        self.slow_poll_ms = slow_poll_ms;
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
        let mut watcher = if self.use_fs_events && !self.roots.is_empty() {
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
        while !self.inner.stopped() {
            let idle = Duration::from_millis(match self.current_driver() {
                WatchDriver::FsEvents => self.slow_poll_ms,
                WatchDriver::Polling => self.poll_interval_ms,
            });
            let Some(trigger) = self.inner.wait_for_wake(idle, debounce) else {
                break;
            };
            if self.inner.stopped() {
                break;
            }
            // Only on a backstop tick: retrying on every filesystem event
            // would hammer the watcher during a burst, and a root appearing is
            // not something an event on another root tells us about.
            if trigger == TickTrigger::Poll {
                if let Some(watch) = watcher.as_mut() {
                    if watch.retry_pending() > 0 {
                        self.publish_status(Some(&*watch));
                    }
                }
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
        watched: Vec<PathBuf>,
        pending: Vec<WatchRoot>,
    }

    impl FsWatch {
        pub(super) fn watched(&self) -> Vec<PathBuf> {
            self.watched.clone()
        }

        pub(super) fn pending(&self) -> Vec<PathBuf> {
            self.pending.iter().map(|root| root.path.clone()).collect()
        }

        /// Try the roots that were not there before. Returns how many were
        /// picked up, so the caller knows whether to republish its status.
        pub(super) fn retry_pending(&mut self) -> usize {
            if self.pending.is_empty() {
                return 0;
            }
            let mut attached = 0usize;
            self.pending.retain(|root| {
                if !register(&mut self.watcher, root) {
                    return true;
                }
                self.watched.push(root.path.clone());
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
        let mut watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
            let Ok(event) = event else {
                return;
            };
            // Metadata churn — an atime bump from a backup or an antivirus
            // scan — does not change the bytes a sweep would read. Filtering
            // here keeps wakeups honest on a noisy home directory.
            if matches!(
                event.kind,
                EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
            ) {
                on_change();
            }
        })?;
        let mut watched = Vec::new();
        let mut pending = Vec::new();
        for root in roots {
            if register(&mut watcher, root) {
                watched.push(root.path.clone());
            } else {
                pending.push(root.clone());
            }
        }
        Ok(FsWatch {
            watcher,
            watched,
            pending,
        })
    }

    fn register(watcher: &mut notify::RecommendedWatcher, root: &WatchRoot) -> bool {
        if !root.path.exists() {
            return false;
        }
        let mode = if root.recursive {
            RecursiveMode::Recursive
        } else {
            RecursiveMode::NonRecursive
        };
        watcher.watch(&root.path, mode).is_ok()
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
