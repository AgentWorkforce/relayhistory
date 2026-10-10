//! A forced sweep the store's lock turned away: when to ask again, and what
//! it stood for.

use std::time::{Duration, Instant};

use super::{deadline_after, scope, ChangeScope, TickTrigger, Wake};

/// How soon a forced sweep that could not take the store's lock is tried
/// again, before backing off.
///
/// Short, because the usual holder is a manual `sync` that is about to finish
/// and the change is still owed.
pub(super) const CONTENDED_SWEEP_RETRY_MS: u64 = 250;

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

/// A forced sweep the store's lock turned away, and how long to wait before
/// asking again.
pub(super) struct OwedSweep {
    /// When to retry, paired with when the change it stands for first
    /// arrived.
    retry: Option<(Instant, Option<Instant>)>,
    /// The roots the owed change covers.
    scope: Option<ChangeScope>,
    backoff: u64,
}

impl Default for OwedSweep {
    fn default() -> Self {
        Self {
            retry: None,
            scope: None,
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
                    scope: scope::widen(self.scope.take(), wake.scope),
                };
                (retry, true)
            }
            // A forced wake before the retry is due sweeps the change the
            // retry stands for as well — its roots too, since a sweep that
            // gets through pays the retry — so it reports from the older of
            // the two; otherwise a sweep that gets through drops the retry
            // and its arrival time with it.
            Some((_, since)) if wake.trigger.forces_scan() => {
                let wake = Wake {
                    first_event_at: earliest(since, wake.first_event_at),
                    scope: scope::widen(self.scope.clone(), wake.scope),
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
    pub(super) fn turned_away(&mut self, wake: &Wake, is_retry: bool) {
        self.scope = Some(scope::widen(self.scope.take(), wake.scope.clone()));
        let since = earliest(self.retry.and_then(|(_, since)| since), wake.first_event_at);
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
        self.scope = None;
        self.backoff = CONTENDED_SWEEP_RETRY_MS;
    }
}

