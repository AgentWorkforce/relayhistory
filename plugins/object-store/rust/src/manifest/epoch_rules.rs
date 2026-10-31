//! Validator rules for the `backfill`, `resumed` and `adopted` epochs
//! (`docs/manifest-v1.md`, "Backfill" and "Handover").

use super::types::{EpochReason, Manifest, Raw, MIGRATION_ID_PREFIX};
use super::validate::Checker;
use crate::layout::RawFormat;

impl Checker {
    /// `backfill` and `resumed` epochs: who opened them and what they follow.
    pub(super) fn backfill(&mut self, m: &Manifest) {
        let raw = &m.raw;
        let backfills = raw
            .epochs
            .iter()
            .filter(|e| e.reason == EpochReason::Backfill)
            .count();
        if backfills > 1 {
            self.issue("raw.epochs", "at most one epoch is \"backfill\"");
        }
        let adopted = raw
            .epochs
            .iter()
            .filter(|e| e.reason == EpochReason::Adopted)
            .count();
        if adopted > 1 {
            self.issue("raw.epochs", "at most one epoch is \"adopted\"");
        }
        for (i, e) in raw.epochs.iter().enumerate() {
            let at = format!("raw.epochs[{i}]");
            match e.reason {
                EpochReason::Backfill => self.backfill_epoch(raw, &at, i),
                EpochReason::Resumed => self.resumed_epoch(raw, &at, i),
                EpochReason::Adopted => self.adopted_epoch(raw, &at, i),
                _ if e.resumes_epoch.is_some() => self.issue(
                    format!("{at}.resumes_epoch"),
                    "is only valid on a \"resumed\" epoch",
                ),
                _ => {}
            }
        }
    }

    fn backfill_epoch(&mut self, raw: &Raw, at: &str, i: usize) {
        let e = &raw.epochs[i];
        if !is_migration_id(&e.opened_by) {
            self.issue(
                format!("{at}.opened_by"),
                format!("a backfill epoch is opened by {MIGRATION_ID_PREFIX}{{grant_id}}"),
            );
        }
        if e.resumes_epoch.is_some() {
            self.issue(
                format!("{at}.resumes_epoch"),
                "is only valid on a \"resumed\" epoch",
            );
        }
        if raw.format != RawFormat::Ndjson {
            self.issue(
                "raw.format",
                "a backfill epoch needs ndjson: exported records share the live stream's format",
            );
        }
        if !raw.chunks.iter().any(|c| c.epoch == e.epoch) {
            self.issue(at.to_string(), "a backfill epoch lists at least one chunk");
        }
    }

    /// `adopted` ends a cold export's ownership: exporter before, client after.
    fn adopted_epoch(&mut self, raw: &Raw, at: &str, i: usize) {
        let e = &raw.epochs[i];
        if e.resumes_epoch.is_some() {
            self.issue(
                format!("{at}.resumes_epoch"),
                "is only valid on a \"resumed\" epoch",
            );
        }
        if !raw.epochs[0].opened_by.starts_with(MIGRATION_ID_PREFIX) {
            self.issue(format!("{at}.reason"), "only a cold export is adopted");
        }
        if e.opened_by.starts_with(MIGRATION_ID_PREFIX) {
            self.issue(format!("{at}.opened_by"), "an exporter does not adopt");
        }
        if raw.owner.starts_with(MIGRATION_ID_PREFIX) {
            self.issue("raw.owner", "an adopted session belongs to its client");
        }
    }

    /// A `resumed` epoch directly follows the backfill epoch and continues
    /// the newest live epoch before it.
    fn resumed_epoch(&mut self, raw: &Raw, at: &str, i: usize) {
        let before = &raw.epochs[..i];
        if before.last().map(|e| e.reason) != Some(EpochReason::Backfill) {
            self.issue(
                format!("{at}.reason"),
                "a \"resumed\" epoch directly follows the backfill epoch",
            );
        }
        let live = before
            .iter()
            .rev()
            .find(|e| e.reason != EpochReason::Backfill)
            .map(|e| e.epoch);
        if raw.epochs[i].resumes_epoch != live {
            self.issue(
                format!("{at}.resumes_epoch"),
                format!("must name the newest live epoch before it ({live:?})"),
            );
        }
    }
}

/// `migration:{grant_id}`, the shape the ingest boundary mints.
fn is_migration_id(s: &str) -> bool {
    s.strip_prefix(MIGRATION_ID_PREFIX).is_some_and(|grant| {
        (1..=255).contains(&grant.len())
            && grant
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-'))
    })
}
