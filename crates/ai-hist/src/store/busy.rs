//! Bounded busy handling for short maintenance overlap and slow writers.
use anyhow::Result;
use rusqlite::Connection;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Maximum number of backed-off retries after the short-overlap prelude.
///
/// A busy handler is preferable to one flat timeout for normal writer overlap:
/// exponential delays stop hammering the lock and per-process/time jitter keeps
/// two scheduled syncs from waking and colliding in lockstep. The callback is
/// never useful for a permanently suspended holder, so the sequence stays
/// bounded while preserving the previous 30-second grace window for a healthy writer that is
/// merely slow. Even with zero jitter the sequence waits at least 30 seconds; jitter can extend
/// that to roughly 33 seconds so simultaneous syncs do not keep waking in lockstep.
const BUSY_RETRY_ATTEMPTS: i32 = 65;
const BUSY_RETRY_BASE_MS: u64 = 10;
const BUSY_RETRY_CAP_MS: u64 = 500;
const BUSY_RETRY_JITTER_DIVISOR: u64 = 10;

fn busy_retry_backoff_ms(prior_attempts: i32) -> Option<u64> {
    if !(0..BUSY_RETRY_ATTEMPTS).contains(&prior_attempts) {
        return None;
    }

    let shift = (prior_attempts as u32).min(6);
    Some(
        BUSY_RETRY_BASE_MS
            .saturating_mul(1_u64 << shift)
            .min(BUSY_RETRY_CAP_MS),
    )
}

// Cover short maintenance collisions without immediately imposing a 10 ms
// retry floor. Long contention retains the existing bounded grace window.
const FAST_RETRY_US: &[u64] = &[100, 200, 400, 800, 1_000, 1_000];

fn retry_delay(prior_attempts: i32) -> Option<Duration> {
    let attempt = usize::try_from(prior_attempts).ok()?;
    if let Some(micros) = FAST_RETRY_US.get(attempt) {
        Some(Duration::from_micros(*micros))
    } else {
        busy_retry_backoff_ms(prior_attempts - FAST_RETRY_US.len() as i32)
            .map(Duration::from_millis)
    }
}
fn busy_retry_handler(prior_attempts: i32) -> bool {
    let Some(delay) = retry_delay(prior_attempts) else {
        return false;
    };
    let clock = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64;
    let seed = clock ^ u64::from(std::process::id()) ^ prior_attempts as u64;
    let jitter_us = seed % (delay.as_micros() as u64 / BUSY_RETRY_JITTER_DIVISOR + 1);
    std::thread::sleep(delay + Duration::from_micros(jitter_us));
    true
}

pub(crate) fn configure_busy_retry(conn: &Connection) -> Result<()> {
    conn.busy_handler(Some(busy_retry_handler))?;
    Ok(())
}

/// A short busy budget for the calls made while it lives, in place of the
/// production retry sequence.
///
/// SQLite keeps one busy handler per connection, and `busy_timeout` replaces
/// it, so this swaps the ~30 s [`busy_retry_handler`] out for a plain timeout
/// and puts it back when dropped. It cannot read back what was installed
/// before — SQLite has no getter for a busy handler — so it is only for
/// connections opened through [`super::open_db`] or [`super::open_db_readonly`], which is
/// what it restores.
pub(crate) struct ShortBusyBudget<'a> {
    conn: &'a Connection,
}

impl<'a> ShortBusyBudget<'a> {
    pub(crate) fn new(conn: &'a Connection, budget: Duration) -> Result<Self> {
        conn.busy_timeout(budget)?;
        Ok(Self { conn })
    }
}

impl Drop for ShortBusyBudget<'_> {
    fn drop(&mut self) {
        // Best effort, like discovery's scoped `synchronous`: installing a
        // busy handler only fails on a connection being torn down.
        let _ = configure_busy_retry(self.conn);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn busy_retry_sequence_is_bounded() {
        let minimum_ms: u64 = (0..BUSY_RETRY_ATTEMPTS)
            .map(|attempt| busy_retry_backoff_ms(attempt).unwrap())
            .sum();
        assert!(
            (30_000..31_000).contains(&minimum_ms),
            "minimum retry grace changed unexpectedly: {minimum_ms}ms"
        );
        let maximum_ms: u64 = (0..BUSY_RETRY_ATTEMPTS)
            .map(|attempt| {
                let backoff = busy_retry_backoff_ms(attempt).unwrap();
                backoff + backoff / BUSY_RETRY_JITTER_DIVISOR
            })
            .sum();
        assert!(
            (33_000..34_000).contains(&maximum_ms),
            "maximum retry grace changed unexpectedly: {maximum_ms}ms"
        );
        assert!(busy_retry_backoff_ms(BUSY_RETRY_ATTEMPTS).is_none());
        let total_attempts = BUSY_RETRY_ATTEMPTS + FAST_RETRY_US.len() as i32;
        assert!(!busy_retry_handler(total_attempts));
        assert!(!busy_retry_handler(total_attempts + 1));
    }

    #[test]
    fn short_collisions_get_a_sub_five_ms_retry_window() {
        let delays: Vec<_> = (0..FAST_RETRY_US.len() as i32)
            .map(|attempt| retry_delay(attempt).unwrap().as_micros() as u64)
            .collect();
        assert_eq!(delays, FAST_RETRY_US);
        let worst_us: u64 = delays
            .iter()
            .map(|delay| delay + delay / BUSY_RETRY_JITTER_DIVISOR)
            .sum();
        assert!(worst_us < 5_000);
        assert_eq!(
            retry_delay(FAST_RETRY_US.len() as i32),
            Some(Duration::from_millis(10))
        );
        assert!(retry_delay(-1).is_none());
    }
}
