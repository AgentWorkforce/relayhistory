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
//!   [`WatchLoop::debounce_ms`] window — swept on its leading edge when the
//!   loop was quiet ([`WatchLoop::leading_edge`]) — and a slow poll at
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
//!   backstop, which is exactly the case live capture exists for. Events
//!   inside one window are one tick: the sweep starts after the window
//!   closes, so it already reads every write those events were for.
//!
//! Registration replays recent history on macOS: FSEvents delivers the
//! changes made a few milliseconds *before* a stream was registered right
//! after it starts. The loop treats them like any other event, which is what
//! it should do — they are the only signal for a write that landed between a
//! caller's last sync and the attach — but a caller (or a test) that seeds a
//! tree and then attaches will see one forced tick it did not cause.
//!
//! The loop is plain threads and condition variables. The crate has no async
//! runtime and this does not need one: a tick is a blocking sweep, and the
//! watcher backend already runs on its own thread.

#[cfg(feature = "fs-events")]
use std::collections::HashSet;
#[cfg(any(feature = "fs-events", test))]
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use serde::{Deserialize, Serialize};

// Without a watcher backend the loop never sees an event path, so the
// matching and registration helpers below exist only with `fs-events`.
#[cfg(any(feature = "fs-events", test))]
use crate::discover;
use crate::discover::WatchRoot;
#[cfg(feature = "fs-events")]
use crate::discover::WatchDepth;

mod inner;
use inner::{Backstop, OwedSweep, RunState, Wake, WakeState, WatchInner};

/// How often a registration that was *lost* is retried.
///
/// Not derived from the user's intervals, because neither of them is about
/// this: a root that was attached and is now gone is a directory being
/// replaced, and the window between the two is where a short session lives.
/// `~/.codex/sessions` removed and recreated, a ten-second session written
/// there, cleanup taking it away again — a retry on the 30 s backstop misses
/// the whole of it. One `stat` per lost root, four times a second, and only
/// until it is attached again.
#[cfg(feature = "fs-events")]
const LOST_REGISTRATION_RECHECK_MS: u64 = 250;

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
/// How long a leading-edge tick waits before it sweeps.
///
/// Not a debounce: one write reaches the loop as several backend callbacks
/// (FSEvents delivers a create or an append as two or three, within about a
/// millisecond), and a tick that swept on the first of them would leave the
/// rest to drive a trailing sweep for the same write. A few milliseconds
/// gathers one write's callbacks without being perceptible.
pub const LEADING_EDGE_SETTLE_MS: u64 = 10;
/// Default slow polling backstop while the filesystem-event driver is active.
pub const DEFAULT_SLOW_POLL_MS: u64 = 30_000;
/// Default cadence for the pure polling driver.
pub const DEFAULT_POLL_INTERVAL_MS: u64 = 1_000;

/// Which driver a running loop selected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
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
    /// The sweep was stopped through the capture stop token before it
    /// finished. Neither swept nor failed, and never owed: a cancellation is
    /// a request to stop, so it also ends the loop.
    pub cancelled: bool,
    /// Wall time of the sweep, including the attempt at the store's lock.
    pub elapsed: Duration,
    /// When the first change signal behind this tick arrived — the start of
    /// its debounce window, or of the earliest window a deferred or retried
    /// forced tick is standing in for. `None` for a tick no change signal
    /// drove (startup, backstop, manual).
    pub first_event_at: Option<Instant>,
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
#[cfg(any(feature = "fs-events", test))]
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
#[cfg(feature = "fs-events")]
fn removed_registration_keys(path: &Path, roots: &[WatchRoot]) -> Vec<PathBuf> {
    let path = discover::watch_path(path);
    roots
        .iter()
        .filter(|root| root.registers_at(&path))
        .map(|root| root.registration_key().to_path_buf())
        .collect()
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
    /// Sweep a change that finds the loop quiet at once (after
    /// [`LEADING_EDGE_SETTLE_MS`]) instead of after the debounce window;
    /// changes inside the window that sweep opens coalesce into one trailing
    /// tick. Default `true`.
    pub leading_edge: bool,
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
            leading_edge: true,
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

    pub fn with_leading_edge(mut self, leading_edge: bool) -> Self {
        self.leading_edge = leading_edge;
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
        let mut watcher = self.attach_watcher();
        self.publish_status(watcher.as_ref());

        if self.immediate {
            // A first sweep against a cold catalog has nothing to compare
            // against and runs fully anyway, so it does not need forcing.
            self.inner.run_skip_if_busy(Wake::from(TickTrigger::Startup));
        }
        // When the loop last swept on its own cadence, so that waking early
        // to reconcile does not also sweep early.
        let mut last_poll_sweep = std::time::Instant::now();
        let mut backstop = Backstop::starting(self.slow_poll_ms);
        let mut owed = OwedSweep::default();
        while !self.inner.stopped() {
            let sweep_every = match self.current_driver() {
                WatchDriver::FsEvents => self.slow_poll_ms,
                WatchDriver::Polling => self.poll_interval_ms,
            };
            let refresh_every = self.refresh_cadence(watcher.as_ref(), sweep_every);
            let check_every = self.check_cadence(watcher.as_ref(), refresh_every);
            let woke_early = refresh_every.min(check_every) < sweep_every;
            let idle = owed.bound_idle(Duration::from_millis(
                sweep_every.min(refresh_every).min(check_every),
            ));
            let Some(wake) = self
                .inner
                .wait_for_wake(idle, debounce, self.leading_edge)
            else {
                break;
            };
            let trigger = wake.trigger;
            if self.inner.stopped() {
                break;
            }
            self.reconcile_when_due(&mut backstop, &mut watcher, trigger, refresh_every);
            if trigger == TickTrigger::RegistrationLost {
                // A lost watch is not a change to sweep for. Putting it back
                // was the whole of this wake.
                continue;
            }
            let (wake, is_retry) = owed.fold_into(wake);
            let trigger = wake.trigger;
            if trigger == TickTrigger::Poll {
                // Reconciled, but not yet due to sweep. Only reached when the
                // wait was deliberately shortened above, so a loop that was
                // not woken early behaves exactly as before.
                if woke_early && last_poll_sweep.elapsed() < Duration::from_millis(sweep_every) {
                    continue;
                }
                last_poll_sweep = std::time::Instant::now();
            }
            if self.inner.run_skip_if_busy(wake) {
                owed.turned_away(wake.first_event_at, is_retry);
            } else if trigger.forces_scan() {
                owed.paid();
            }
        }
        // The watcher (and with it every OS registration) is released as
        // `run` returns.
        Ok(self.current_driver())
    }

    /// Re-derive the roots and re-check the registrations, each when its
    /// deadline has come, and the registrations at once when one was
    /// reported lost.
    fn reconcile_when_due(
        &self,
        backstop: &mut Backstop,
        watcher: &mut Option<fs_events::FsWatch>,
        trigger: TickTrigger,
        refresh_every: u64,
    ) {
        // On a deadline rather than on the trigger. Reconciling on every
        // wake would hammer the watcher during a burst, which is why this
        // used to run only when the wait expired — but an event ends the
        // wait early, so under sustained writes that moment never came and
        // a pending root stayed pending for as long as the writing lasted.
        // The deadline gives the same at-most-once-per-interval rate
        // without depending on how the loop woke up.
        let now = Instant::now();
        let refresh_due = now >= backstop.next_refresh;
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
        let check_due = refresh_due || now >= backstop.next_check || registration_lost;
        if refresh_due {
            backstop.next_refresh = deadline_after(now, refresh_every);
        }
        if check_due {
            if let Some(watch) = watcher.as_mut() {
                self.reconcile_watcher(watch, refresh_due);
            }
            // Dated from what the check *found*, not from what was true
            // before it ran. A check that just discovered a lost
            // registration has to come back on the recovery cadence; one
            // dated from the cadence that applied a moment earlier would
            // wait out the backstop it was supposed to pre-empt.
            backstop.next_check = deadline_after(
                Instant::now(),
                self.check_cadence(watcher.as_ref(), refresh_every),
            );
        }
    }

    /// Bring up the filesystem watcher over the configured roots, or `None`
    /// when the loop polls. A loop given no roots up front but a refresher
    /// that will produce them still has to start the backend: `adopt` is only
    /// reachable through an attached watcher, so skipping it here would leave
    /// that caller polling forever.
    fn attach_watcher(&self) -> Option<fs_events::FsWatch> {
        if !self.use_fs_events || (self.roots.is_empty() && self.roots_refresh.is_none()) {
            return None;
        }
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
    }

    /// How often the root set is re-derived and unattached roots retried.
    ///
    /// Coverage is not the user's sweep cadence. A loop with a root it has
    /// not attached yet — a provider installed after `watch` started — is
    /// *polling*, so without this it would retry that root on `--interval`,
    /// which the user may have set to an hour. The status output and the docs
    /// promise the backstop, so the wait is shortened to it while anything is
    /// uncovered; the sweep itself still happens on the interval that was
    /// asked for.
    fn refresh_cadence(&self, watcher: Option<&fs_events::FsWatch>, sweep_every: u64) -> u64 {
        match self.current_driver() {
            // Events are flowing, so the backstop *is* the cadence for
            // everything the backstop does. Shortening it here would make
            // `--interval 1` re-derive the root set — a walk of every
            // project tree — once a second on a loop that is not polling
            // for changes at all, which is the opposite of what a short
            // interval asks for.
            WatchDriver::FsEvents => self.slow_poll_ms,
            WatchDriver::Polling => match watcher {
                Some(watch) if !watch.pending().is_empty() || self.roots_refresh.is_some() => {
                    self.slow_poll_ms.min(self.poll_interval_ms)
                }
                _ => sweep_every,
            },
        }
    }

    /// Put the watcher's coverage back in step with the roots, publishing the
    /// driver status when it moved.
    fn reconcile_watcher(&self, watch: &mut fs_events::FsWatch, refresh_due: bool) {
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
mod fs_events;

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

    /// OpenCode's directory root admits its stores and nothing else beside
    /// them, so an `OPENCODE_DB` in a busy directory is not a sweep per write
    /// to its neighbours (#335).
    #[test]
    fn the_opencode_root_admits_only_its_stores() {
        let home = PathBuf::from("/home/u");
        let roots = crate::discover::provider_watch_roots(
            "opencode",
            &crate::ProviderRoots::from_home(home.clone(), home.join("my-opencode.sqlite")),
        );
        assert_eq!(roots.len(), 1, "{roots:?}");
        assert_eq!(roots[0].path, home);
        for store in [
            "my-opencode.sqlite",
            "my-opencode.sqlite-wal",
            "my-opencode.sqlite-shm",
            "my-opencode.sqlite-journal",
            "opencode.db",
            "opencode-nightly.db-wal",
        ] {
            assert!(
                event_matches_roots(&home.join(store), &roots),
                "{store} is a store the sweep reads"
            );
        }
        for neighbour in [
            "collector-stderr.log",
            ".zsh_history",
            "opencode.db.bak",
            "my-opencode.sqlite.tmp",
        ] {
            assert!(
                !event_matches_roots(&home.join(neighbour), &roots),
                "{neighbour} is not a store and must not force a sweep"
            );
        }
        // The directory itself still counts: its removal and recreation is
        // how a registration is lost and put back.
        assert!(event_matches_roots(&home, &roots));
        assert!(!event_matches_roots(&home.join("storage/session/x.json"), &roots));
    }

    /// A configured database whose own name ends in a sidecar suffix is still
    /// matched, sidecars included, and a pinned database admits no channel
    /// database beside it.
    #[test]
    fn the_opencode_filter_matches_the_configured_name_exactly() {
        let home = PathBuf::from("/home/u");
        let mut roots_for = crate::ProviderRoots::from_home(home.clone(), home.join("state-wal"));
        let roots = crate::discover::provider_watch_roots("opencode", &roots_for);
        for store in ["state-wal", "state-wal-wal", "state-wal-shm", "opencode.db"] {
            assert!(
                event_matches_roots(&home.join(store), &roots),
                "{store} must be admitted"
            );
        }
        assert!(!event_matches_roots(&home.join("state"), &roots));

        roots_for.opencode_db = home.join("pinned.db");
        roots_for.opencode_db_pinned = true;
        let pinned = crate::discover::provider_watch_roots("opencode", &roots_for);
        for store in ["pinned.db", "pinned.db-wal", "pinned.db-journal"] {
            assert!(
                event_matches_roots(&home.join(store), &pinned),
                "{store} is the pinned store"
            );
        }
        for channel in ["opencode.db", "opencode-nightly.db", "opencode-nightly.db-wal"] {
            assert!(
                !event_matches_roots(&home.join(channel), &pinned),
                "{channel} is not read when the database is pinned"
            );
        }
    }

    /// A configured name that is not UTF-8 keeps its sidecars.
    #[cfg(unix)]
    #[test]
    fn a_non_utf8_opencode_name_keeps_its_sidecars() {
        use std::os::unix::ffi::OsStrExt;
        let home = PathBuf::from("/home/u");
        let name = std::ffi::OsStr::from_bytes(b"open\xffcode.db");
        let roots = crate::discover::provider_watch_roots(
            "opencode",
            &crate::ProviderRoots::from_home(home.clone(), home.join(name)),
        );
        let mut wal = name.to_os_string();
        wal.push("-wal");
        assert!(event_matches_roots(&home.join(name), &roots));
        assert!(event_matches_roots(&home.join(&wal), &roots));
        assert!(!event_matches_roots(&home.join("other.log"), &roots));
    }

    /// A second, unfiltered claim on the same directory widens the filter
    /// away rather than narrowing the other claim.
    #[test]
    fn an_unfiltered_claim_on_the_same_directory_admits_everything() {
        let mut filtered = WatchRoot::directory_of(
            "/home/u",
            crate::discover::WatchEntries::OpencodeStores {
                primary: "opencode.db".into(),
                channels: true,
            },
        );
        filtered.widen(&WatchRoot::directory("/home/u"));
        assert_eq!(filtered.entries, crate::discover::WatchEntries::All);
        assert!(event_matches_roots(
            Path::new("/home/u/anything.log"),
            &[filtered]
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
