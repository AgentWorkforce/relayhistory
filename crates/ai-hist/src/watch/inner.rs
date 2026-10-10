use std::sync::{Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use super::{
    deadline_after, DriverStatus, ErrorSink, ReportSink, TickFn, TickOutcome, TickReport,
    TickTrigger, LEADING_EDGE_SETTLE_MS, MAX_INTERVAL_MS,
};

/// How soon a forced sweep that could not take the store's lock is tried
/// again, before backing off.
///
/// Short, because the usual holder is a manual `sync` that is about to finish
/// and the change is still owed.
const CONTENDED_SWEEP_RETRY_MS: u64 = 250;

/// The longest an owed sweep waits between attempts at a held lock.
///
/// Low, because the wait is paid by the change: a write made while another
/// process sweeps is read no sooner than the first retry after that sweep
/// releases the lock. A retry is one `try_lock` and a return, so asking once
/// a second for as long as a holder keeps the lock costs nothing worth
/// trading for capture latency. It used to be the slow backstop (30–60 s),
/// reached after a handful of contended *event* ticks, which left the last
/// writes of a burst unread for up to a minute after the lock was free
/// (#364).
const CONTENDED_SWEEP_RETRY_MAX_MS: u64 = 1_000;

/// Whether a failed tick was a cancellation rather than a failure.
fn is_cancellation(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| cause.is::<crate::ingest::CaptureCancelled>())
}

/// What one wait produced: the trigger, and for a change-driven tick when
/// its first signal arrived.
#[derive(Debug, Clone, Copy)]
pub(super) struct Wake {
    pub(super) trigger: TickTrigger,
    pub(super) first_event_at: Option<Instant>,
}

impl From<TickTrigger> for Wake {
    fn from(trigger: TickTrigger) -> Self {
        Self {
            trigger,
            first_event_at: None,
        }
    }
}

/// When reconciliation is next due, as absolute instants. They have to be
/// absolute: every filesystem event ends the wait early, so a reconciliation
/// gated on the wait *expiring* is starved by exactly the machine this loop
/// exists for — one busy session writing every couple of hundred milliseconds
/// would postpone attaching a provider installed beside it for as long as the
/// writing lasts.
///
/// Two deadlines, because the backstop does two jobs of very different cost.
/// Re-deriving the root set walks every project tree, so it stays on the
/// backstop whatever else happens. Re-checking the registrations is a stat
/// per root, and has to be prompt: a root that is uncovered now is a root
/// whose writes are invisible now.
pub(super) struct Backstop {
    pub(super) next_refresh: Instant,
    pub(super) next_check: Instant,
}

impl Backstop {
    pub(super) fn starting(slow_poll_ms: u64) -> Self {
        let next_refresh = deadline_after(Instant::now(), slow_poll_ms);
        Self {
            next_refresh,
            next_check: next_refresh,
        }
    }
}

/// A forced sweep the store's lock turned away, and how long to wait before
/// asking again.
pub(super) struct OwedSweep {
    /// When to retry, paired with when the change it stands for first
    /// arrived.
    retry: Option<(Instant, Option<Instant>)>,
    backoff: u64,
}

impl Default for OwedSweep {
    fn default() -> Self {
        Self {
            retry: None,
            backoff: CONTENDED_SWEEP_RETRY_MS,
        }
    }
}

/// The earlier of two arrival times, either of which may be unknown.
fn earliest(a: Option<Instant>, b: Option<Instant>) -> Option<Instant> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

impl OwedSweep {
    /// Cut the loop's wait short at the retry deadline. A change whose sweep
    /// never ran has nothing else to bring the loop back for it: the wake
    /// state was cleared when its debounce window closed, so without this the
    /// next visit is the backstop, up to `--interval` away.
    pub(super) fn bound_idle(&self, idle: Duration) -> Duration {
        match self.retry {
            Some((at, _)) => idle.min(at.saturating_duration_since(Instant::now())),
            None => idle,
        }
    }

    /// The wake to run, with what is owed folded in, and whether it is the
    /// owed retry itself.
    ///
    /// A sweep the store's lock turned away comes back as the forced tick it
    /// was, not as the backstop tick this wake would otherwise have been: the
    /// change it is standing in for is still unread, so the fingerprint it
    /// would be compared against still cannot be trusted. Only the retry
    /// itself — as opposed to a new event that happens to arrive while one is
    /// owed — backs off.
    pub(super) fn fold_into(&mut self, wake: Wake) -> (Wake, bool) {
        match self.retry {
            Some((at, since)) if Instant::now() >= at => {
                self.retry = None;
                let retry = Wake {
                    trigger: TickTrigger::FsEvent,
                    // The oldest change this sweep now covers.
                    first_event_at: earliest(since, wake.first_event_at),
                };
                (retry, true)
            }
            // A forced wake before the retry is due sweeps the change the
            // retry stands for as well, so it reports from the older of the
            // two; otherwise a sweep that gets through drops the retry and
            // its arrival time with it.
            Some((_, Some(since))) if wake.trigger.forces_scan() => {
                let wake = Wake {
                    first_event_at: Some(wake.first_event_at.map_or(since, |at| at.min(since))),
                    ..wake
                };
                (wake, false)
            }
            _ => (wake, false),
        }
    }

    /// The lock turned a sweep away: it is still owed, and the holder may be
    /// there for a while. Repeats coalesce into the one deadline, so a busy
    /// tree under a long sync costs one retry per window rather than one per
    /// event.
    pub(super) fn turned_away(&mut self, first_event_at: Option<Instant>, is_retry: bool) {
        let since = earliest(self.retry.and_then(|(_, since)| since), first_event_at);
        // Backed off per attempt of the owed retry, never per event: a
        // burst of contended events is one owed change, and letting
        // each of them double the wait (and push the deadline later)
        // is what stalled capture until the backstop. A new event
        // keeps the earlier of the two deadlines.
        let next = deadline_after(Instant::now(), self.backoff);
        let at = match self.retry {
            Some((held, _)) if !is_retry => held.min(next),
            _ => next,
        };
        self.retry = Some((at, since));
        if is_retry {
            self.backoff = self
                .backoff
                .saturating_mul(2)
                .clamp(CONTENDED_SWEEP_RETRY_MS, CONTENDED_SWEEP_RETRY_MAX_MS);
        }
    }

    /// A forced sweep got through. Whatever was owed is paid, and the next
    /// contention starts from the short cadence again.
    pub(super) fn paid(&mut self) {
        self.retry = None;
        self.backoff = CONTENDED_SWEEP_RETRY_MS;
    }
}

#[derive(Default)]
pub(super) struct WakeState {
    /// A change signal is pending, and when the first one arrived. One slot
    /// on purpose: a thousand events between two ticks cost one wakeup, not
    /// a thousand, and the tick reports how long the oldest of them waited.
    pending: Option<Instant>,
    /// A registration was reported gone and has to be re-made. Kept apart
    /// from `pending` because it asks for different work: `pending` says
    /// something was written and wants a sweep, this says a watch was lost
    /// and wants the watch back.
    registration_lost: bool,
    stopped: bool,
    /// When the coalescing window opened by the last change-driven tick
    /// closes. A change arriving after it finds the loop quiet and, with the
    /// leading edge on, is swept at once; one arriving before it waits for
    /// it and becomes the window's one trailing tick.
    window_until: Option<Instant>,
}

#[derive(Default)]
pub(super) struct RunState {
    in_flight: bool,
    /// A forced tick arrived while a run held the slot. Kept as one bit: a
    /// hundred events during a long sweep are one sweep afterwards, not a
    /// hundred.
    deferred_force: bool,
    /// When the oldest change behind `deferred_force` first arrived.
    deferred_since: Option<Instant>,
    /// Monotonic count of finished ticks, so a joiner can wait for "the run
    /// that was in flight when I arrived" without holding the lock across it.
    completed: u64,
}

pub(super) struct WatchInner {
    pub(super) tick: TickFn,
    pub(super) on_report: Option<ReportSink>,
    pub(super) on_error: Option<ErrorSink>,
    pub(super) wake: Mutex<WakeState>,
    pub(super) wake_cv: Condvar,
    pub(super) run: Mutex<RunState>,
    pub(super) run_cv: Condvar,
    pub(super) driver: Mutex<Option<DriverStatus>>,
}

/// Resets `in_flight` even when the sweep panics, so one bad tick cannot wedge
/// the loop into "a run is always in flight" forever.
struct InFlight<'a> {
    inner: &'a WatchInner,
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        let (deferred, since) = {
            let mut run = self.inner.run.lock().expect("watch run state");
            run.in_flight = false;
            run.completed = run.completed.wrapping_add(1);
            (
                std::mem::take(&mut run.deferred_force),
                run.deferred_since.take(),
            )
        };
        self.inner.run_cv.notify_all();
        if deferred {
            // A change arrived while this run held the slot. Post it now that
            // the slot is free: the loop is waiting on the wake state, so it
            // takes it up immediately rather than at the next backstop.
            self.inner
                .signal_change_since(since.unwrap_or_else(Instant::now));
        }
    }
}

impl WatchInner {
    pub(super) fn stopped(&self) -> bool {
        self.wake.lock().expect("watch wake state").stopped
    }

    /// Post a change signal. Called by the filesystem watcher's callback, and
    /// public through [`super::WatchLoop::notify_change`] for hosts that already have
    /// their own change feed.
    pub(super) fn signal_change(&self) {
        self.signal_change_since(Instant::now());
    }

    /// Post a change signal that first arrived at `at`, keeping the oldest
    /// arrival when one is already pending.
    fn signal_change_since(&self, at: Instant) {
        {
            let mut wake = self.wake.lock().expect("watch wake state");
            if wake.stopped {
                return;
            }
            wake.pending = Some(wake.pending.map_or(at, |first| first.min(at)));
        }
        self.wake_cv.notify_all();
    }

    /// Post that a registration is gone. Called by the watcher's callback
    /// when the backend reports a watched path removed.
    pub(super) fn signal_registration_lost(&self) {
        {
            let mut wake = self.wake.lock().expect("watch wake state");
            if wake.stopped {
                return;
            }
            wake.registration_lost = true;
        }
        self.wake_cv.notify_all();
    }

    pub(super) fn request_stop(&self) {
        {
            let mut wake = self.wake.lock().expect("watch wake state");
            wake.stopped = true;
        }
        self.wake_cv.notify_all();
    }

    pub(super) fn report_error(&self, error: &anyhow::Error) {
        if let Some(sink) = &self.on_error {
            sink(error);
        } else {
            eprintln!("ai-hist: watch failed: {error:#}");
        }
    }

    /// Block until a change signal arrives, `timeout` elapses, or the loop is
    /// stopped. A change signal is followed by the debounce window, so further
    /// events landing inside it roll into the same tick.
    pub(super) fn wait_for_wake(
        &self,
        timeout: Duration,
        debounce: Duration,
        leading_edge: bool,
    ) -> Option<Wake> {
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
                return Some(Wake::from(TickTrigger::RegistrationLost));
            }
            if wake.pending.is_some() {
                return self.debounced_change(wake, debounce, leading_edge);
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
                return Some(Wake::from(TickTrigger::Poll));
            }
        }
    }

    /// The debounce window behind a pending change signal: release `wake`,
    /// sleep the window out, and take the signal as one filesystem-event
    /// wake.
    fn debounced_change(
        &self,
        wake: MutexGuard<'_, WakeState>,
        debounce: Duration,
        leading_edge: bool,
    ) -> Option<Wake> {
        // With the leading edge on, a change that finds the loop quiet
        // — no change-driven tick in the last window — is swept after
        // only the settle, and the window opens with that sweep: what
        // lands inside it is its one trailing tick, at the window's
        // close. A burst therefore costs at most a leading and a
        // trailing sweep, an isolated write is swept within a few
        // milliseconds, and sustained writes tick once per window.
        // Without it every change waits out the full window.
        let now = Instant::now();
        let window = if leading_edge {
            match wake.window_until {
                Some(until) if until > now => until - now,
                _ => debounce.min(Duration::from_millis(LEADING_EDGE_SETTLE_MS)),
            }
        } else {
            debounce
        };
        drop(wake);
        // Cut short by a lost registration, because on a busy tree
        // this window is where the loop spends nearly all of its
        // time: a removal landing inside it would otherwise wait out
        // the rest of the window *and* the sweep that follows before
        // the watch is put back, which is the whole of the gap a
        // short session occupies. The bit itself is left set; the
        // caller takes it and reconciles before it sweeps.
        self.sleep_through_debounce(window);
        let mut wake = self.wake.lock().expect("watch wake state");
        if wake.stopped {
            return None;
        }
        // Cleared when the window *closes*, not when it opens. Every
        // event that landed inside the window is for a write that has
        // already happened, and the sweep this tick runs starts after
        // it, so that sweep reads it: re-arming on those events ran a
        // second forced sweep for one write whenever the backend
        // delivered its events in more than one callback (FSEvents
        // does, for a create or a multi-line append), and the window
        // collapsed nothing. The window still always ends — it is
        // fixed from the first event, never extended — and a write
        // that lands once the window has closed, including during the
        // sweep, sets the bit again, so sustained writes keep a steady
        // window-plus-sweep cadence instead of waiting for quiet.
        let first_event_at = wake.pending.take();
        if leading_edge {
            // Bounded like every other interval here: the public
            // field can be set past `MAX_INTERVAL_MS` directly.
            wake.window_until =
                Instant::now().checked_add(debounce.min(Duration::from_millis(MAX_INTERVAL_MS)));
        }
        Some(Wake {
            trigger: TickTrigger::FsEvent,
            first_event_at,
        })
    }

    /// Take the lost-registration bit, if one is set.
    ///
    /// Read on every wake rather than only on the wake it caused: a removal
    /// reported while the loop was inside its debounce window, or inside a
    /// sweep, has to be acted on by the iteration that comes out of it and
    /// not by the one after the next sweep.
    pub(super) fn take_registration_lost(&self) -> bool {
        let mut wake = self.wake.lock().expect("watch wake state");
        std::mem::take(&mut wake.registration_lost)
    }

    /// Sleep out the debounce window, returning early when the loop is
    /// stopped or a registration has been reported lost.
    pub(super) fn sleep_through_debounce(&self, duration: Duration) {
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
    /// closed, so returning here would lose that change until the backstop —
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
    pub(super) fn run_skip_if_busy(&self, wake: Wake) -> bool {
        let Some(guard) = self.claim_or_defer(wake) else {
            return false;
        };
        self.run_claimed(wake, guard)
    }

    /// Claim the in-flight slot, or remember a forced trigger that could not
    /// have it.
    fn claim_or_defer(&self, wake: Wake) -> Option<InFlight<'_>> {
        let mut run = self.run.lock().expect("watch run state");
        if run.in_flight {
            if wake.trigger.forces_scan() {
                run.deferred_force = true;
                let since = wake.first_event_at.unwrap_or_else(Instant::now);
                run.deferred_since = Some(run.deferred_since.map_or(since, |held| held.min(since)));
            }
            return None;
        }
        run.in_flight = true;
        Some(InFlight { inner: self })
    }

    /// Manual path: a tick arriving while one is in flight waits for it, so
    /// `tick()` is a real completion barrier rather than a silent no-op.
    pub(super) fn run_or_join(&self, trigger: TickTrigger) {
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
                self.run_claimed(Wake::from(trigger), InFlight { inner: self });
            }
            Some(target) => {
                let mut run = self.run.lock().expect("watch run state");
                while run.completed < target {
                    run = self.run_cv.wait(run).expect("watch run state");
                }
            }
        }
    }

    fn run_claimed(&self, wake: Wake, guard: InFlight<'_>) -> bool {
        let Wake {
            trigger,
            first_event_at,
        } = wake;
        let forced = trigger.forces_scan();
        let started = Instant::now();
        let result = (self.tick)(forced);
        let elapsed = started.elapsed();
        let report = |outcome: TickOutcome, cancelled: bool| {
            if let Some(sink) = &self.on_report {
                sink(&TickReport {
                    trigger,
                    forced,
                    outcome,
                    cancelled,
                    elapsed,
                    first_event_at,
                });
            }
        };
        // Only a forced tick is ever owed anything: a backstop tick that found
        // the store busy, or failed, is covered by the next backstop, while a
        // forced one is standing in for a change nothing else knows about.
        let owed = match result {
            Ok(outcome) => {
                report(outcome, false);
                forced && outcome.contended
            }
            Err(error) if is_cancellation(&error) => {
                // Stopped, not failed: the only thing that cancels a sweep
                // is a request to stop, so the loop ends with it and nothing
                // is owed. Reported, so a consumer sees the tick end.
                self.request_stop();
                report(TickOutcome::default(), true);
                false
            }
            Err(error) => {
                self.report_error(&error);
                // A sweep that failed covered nothing, exactly as a contended
                // one covered nothing, and the change it was for is recorded
                // nowhere else: the wake state was cleared when the debounce
                // window closed. An error is not an answer about the change,
                // so it is owed and retried on the same bounded cadence rather
                // than logged and forgotten until the backstop.
                forced
            }
        };
        drop(guard);
        owed
    }

    pub(super) fn wait_for_idle(&self) {
        let mut run = self.run.lock().expect("watch run state");
        while run.in_flight {
            run = self.run_cv.wait(run).expect("watch run state");
        }
    }
}
