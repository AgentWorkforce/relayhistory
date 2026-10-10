//! Dropping sessions' evidence while keeping their catalog rows, and handing
//! the freed space back to the volume.
//!
//! [`SessionStore::forget_evidence`] is an eviction, not a retraction: it
//! removes only evidence a local hydration reads back, keeps everything that
//! names or links a session, and never touches what a person created.
//! Whether hydration reads a session back is answered by the snapshot
//! hydration itself takes (`HydrationProbe` in `ingest::hydrate`), not
//! by a second heuristic; a session it would refuse, one with evidence from
//! another connector, or one a sweep indexed from transcripts hydration does
//! not read is skipped unless the caller opts in.
//!
//! A session is forgotten with the catalog-less delegated descendants reached
//! from it without passing through a catalogued session (Claude subagents,
//! Codex child threads): they have no catalog row to name them by, and their
//! evidence comes back through the hydration of the session that delegated
//! to them, so each is forgotten only when that hydration enumerates its
//! transcript. Keeping a session keeps all its delegated descendants.
//!
//! | Table | Kept? | Why |
//! |---|---|---|
//! | `session_events` (+ `session_events_fts` via its trigger), `tool_calls`, `file_edits`, `session_markers` | removed | parsed evidence; hydration rewrites it |
//! | `observation_evidence` | removed | the builtin local connector's snapshot rows; hydration rewrites them |
//! | `session_hydration_checkpoints`, `observation_hydration_checkpoints` | removed | they claim the evidence is current; without them hydration re-reads from byte zero |
//! | `transcript_cursors` for the session's transcript, its sidecar directory and its child transcripts | removed | a byte position into evidence that is gone would resume past it |
//! | `grok_session_turns` | removed | turn census of the last replacing read |
//! | `sessions`, `session_presences`, `session_observations` | kept, `discovery_state = 'shallow'` | the catalog; now says no evidence is held |
//! | `session_relationships` | kept, `child_has_events = 0` for a forgotten child | lineage; the flag says whether the child's events are held |
//! | `session_continuity_evidence`, `session_identity_correlations`, `observation_versions` | kept | identity and lineage, not evidence |
//! | `tags`, `session_tags`, `session_commit_links` | kept | user-created |
//! | `history`, `grok_unified_usage` | kept | read from provider-wide logs, which a session's hydration does not re-read |
//! | `canonical_evidence_protection` | kept | ownership claims that outlive any one observation |
//! | `discovery_skips`, `observation_discovery_skips`, change-feed and cursor tables | kept | store bookkeeping |
//!
//! Each removed row would leave a change-feed tombstone through its table's
//! delete trigger. Those tombstones are dropped in the same transaction: a
//! durable receiver must not retract evidence the provider still holds. What
//! the feed reports instead is each session's catalog row moving to
//! `shallow`, and a later hydration reports the evidence again as upserts of
//! the same keys.

mod cursors;
mod recovery;

use crate::diagnostics::{self, CompactRefused};
use crate::ingest::{check_capture_cancelled, try_acquire_sync_lock, SyncRunLock};
use crate::observations::{self, ObservationKey};
use crate::session_store::{Error, SessionRef, SessionStore, StopToken};
use crate::store::{open_db, SessionLocation};
use crate::ProviderRoots;
use cursors::{forget_cursors, observation_keys};
use recovery::Recovery;
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::Path;
use std::time::{Duration, Instant};

/// Which sessions [`SessionStore::forget_evidence`] drops evidence for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(tag = "scope", content = "sessions", rename_all = "snake_case")]
pub enum ForgetScope {
    /// Exactly these sessions.
    Sessions(Vec<SessionRef>),
    /// Every session holding evidence except these — the store keeps evidence
    /// for this set only. An empty set forgets every session's evidence.
    AllExcept(Vec<SessionRef>),
}

/// How to run [`SessionStore::forget_evidence`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ForgetOptions {
    /// How long to wait for another process's `SyncRunLock` before returning
    /// [`Error::SyncLocked`]. `0` (the default) tries once.
    pub lock_timeout_ms: u64,
    /// Stops between batches. Batches already committed stay forgotten; a
    /// later call works out the remaining sessions again.
    #[serde(skip)]
    pub stop: Option<StopToken>,
    /// Also forget sessions whose evidence a local hydration cannot read back
    /// (see [`ForgetReport::skipped_unrecoverable`]). That evidence is lost.
    pub include_unrecoverable: bool,
}

impl ForgetOptions {
    /// Wait this long for the lock; see [`ForgetOptions::lock_timeout_ms`].
    #[must_use]
    pub fn lock_timeout_ms(mut self, lock_timeout_ms: u64) -> Self {
        self.lock_timeout_ms = lock_timeout_ms;
        self
    }

    /// Stop when this token is stopped; see [`ForgetOptions::stop`].
    #[must_use]
    pub fn stop(mut self, stop: StopToken) -> Self {
        self.stop = Some(stop);
        self
    }

    /// Also forget unrecoverable evidence; see
    /// [`ForgetOptions::include_unrecoverable`].
    #[must_use]
    pub fn include_unrecoverable(mut self, include_unrecoverable: bool) -> Self {
        self.include_unrecoverable = include_unrecoverable;
        self
    }
}

/// Result of [`SessionStore::forget_evidence`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ForgetReport {
    /// Sessions that held evidence or a hydrated state and now hold neither.
    pub sessions: u64,
    /// Sessions in scope holding evidence that were left as they were
    /// because a local hydration could not read it back: the transcript is gone (Claude Code
    /// deletes old ones), the source has no local parser, or evidence came
    /// from a remote connector or plugin intake.
    pub skipped_unrecoverable: u64,
    pub events: u64,
    pub tool_calls: u64,
    pub file_edits: u64,
    pub markers: u64,
    pub observation_evidence: u64,
    /// Bytes of the database file now on SQLite's freelist. Later writes
    /// reuse them; [`SessionStore::compact`] returns them to the volume.
    pub reclaimable_bytes: u64,
    /// Size of the database file.
    pub db_bytes: u64,
    pub elapsed_ms: u64,
}

/// How to run [`SessionStore::compact`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CompactOptions {
    /// How long to wait for another process's `SyncRunLock` before returning
    /// [`Error::SyncLocked`]. `0` (the default) tries once.
    pub lock_timeout_ms: u64,
}

/// Result of [`SessionStore::compact`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CompactReport {
    pub db_bytes_before: u64,
    pub wal_bytes_before: u64,
    pub db_bytes_after: u64,
    pub wal_bytes_after: u64,
    /// Freelist bytes before the rewrite.
    pub reclaimable_before: u64,
    /// `false` when a reader holding an older snapshot kept the WAL from
    /// being reset; it shrinks at a later checkpoint.
    pub wal_truncated: bool,
    pub elapsed_ms: u64,
}

/// A write transaction commits once it has removed this many evidence rows
/// or run this long, whichever comes first, so a concurrent hydration or
/// discovery waits for one batch rather than the whole run. Time matters as
/// much as rows: a row's cost is mostly its full-text and index entries, and
/// one large tool output costs as much as hundreds of short rows. One session
/// is never split across transactions.
const BATCH_ROWS: u64 = 4_000;
const BATCH_TIME: Duration = Duration::from_millis(200);

/// Retry cadence while another process holds the `SyncRunLock`.
const LOCK_RETRY: Duration = Duration::from_millis(100);

/// Longest lock wait honoured, as for `sync`.
const MAX_LOCK_WAIT_MS: u64 = crate::watch::MAX_INTERVAL_MS;

/// Change-feed kinds whose delete triggers tombstone the rows removed here.
const TOMBSTONED_KINDS: &str =
    "'session_event','tool_call','file_edit','session_marker','observation_evidence'";

type Key = (String, String);

impl SessionStore {
    /// Drop the evidence of the sessions `scope` names, and of their
    /// catalog-less delegated children, keeping their catalog rows:
    /// [`SessionStore::sessions`] still lists each one, now as
    /// [`crate::DiscoveryState::Shallow`], and [`SessionStore::hydrate`] reads
    /// it again in full. Tags and commit links stay. A session whose evidence
    /// a local hydration could not read back is skipped and counted in
    /// [`ForgetReport::skipped_unrecoverable`], unless
    /// [`ForgetOptions::include_unrecoverable`] is set.
    ///
    /// Holds the `SyncRunLock` for the run, so no sweep or watch tick
    /// interleaves; deletes commit in batches, each session in one
    /// transaction, so hydration and discovery proceed between batches. The
    /// change feed reports each session's catalog row, not a tombstone per
    /// removed record. Freed pages stay in the file for reuse until
    /// [`SessionStore::compact`].
    ///
    /// A later `sync` or `watch` captures every session again, forgotten ones
    /// included; this suits an embedder that hydrates the sessions it keeps.
    pub fn forget_evidence(
        &self,
        scope: ForgetScope,
        opts: ForgetOptions,
    ) -> Result<ForgetReport, Error> {
        if self.read_only() {
            return Err(Error::read_only("forget_evidence"));
        }
        let (named, keep_only) = match scope {
            ForgetScope::Sessions(refs) => (id_keys(refs, "forget_evidence")?, false),
            ForgetScope::AllExcept(refs) => (id_keys(refs, "forget_evidence")?, true),
        };
        let started = Instant::now();
        crate::session_store::controlled(opts.stop.as_ref(), None, || {
            let _lock = wait_for_sync_lock(self.db_path(), opts.lock_timeout_ms)?;
            let mut conn = open_db(self.db_path())
                .map_err(|error| Error::DatabaseOpen(format!("{error:#}")))?;
            let (targets, skipped) = plan(
                &conn,
                self.roots(),
                named,
                keep_only,
                opts.include_unrecoverable,
            )
            .map_err(Error::query)?;
            let mut report = ForgetReport {
                skipped_unrecoverable: skipped,
                ..ForgetReport::default()
            };
            let mut pending = targets.iter().peekable();
            while pending.peek().is_some() {
                check_capture_cancelled().map_err(Error::sync)?;
                forget_batch(&mut conn, &mut pending, &mut report).map_err(Error::query)?;
            }
            let usage = diagnostics::read_page_usage(&conn).map_err(Error::sql)?;
            report.reclaimable_bytes = usage.free_bytes();
            report.db_bytes = usage.page_count.saturating_mul(usage.page_size);
            report.elapsed_ms = started.elapsed().as_millis() as u64;
            Ok(report)
        })
    }

    /// Return the database's unused space to the volume: merge the full-text
    /// indexes' segments, rewrite the file with `VACUUM` and truncate the WAL.
    /// Deletes nothing.
    ///
    /// Holds the `SyncRunLock`; other writers wait behind the rewrite through
    /// their busy handler, so run it when a pause in writes is acceptable.
    /// Needs about twice the live data free on the database's volume, and
    /// returns [`Error::InsufficientSpace`] before writing anything when
    /// that is not there.
    pub fn compact(&self, opts: CompactOptions) -> Result<CompactReport, Error> {
        if self.read_only() {
            return Err(Error::read_only("compact"));
        }
        let started = Instant::now();
        let deadline = lock_deadline(started, opts.lock_timeout_ms);
        loop {
            match diagnostics::compact_database(self.db_path()) {
                Ok(done) => {
                    return Ok(CompactReport {
                        db_bytes_before: done.db_bytes_before,
                        wal_bytes_before: done.wal_bytes_before,
                        db_bytes_after: done.db_bytes_after,
                        wal_bytes_after: done.wal_bytes_after,
                        reclaimable_before: done.reclaimable_before,
                        wal_truncated: done.wal_truncated,
                        elapsed_ms: started.elapsed().as_millis() as u64,
                    })
                }
                Err(error) => match error.downcast_ref::<CompactRefused>() {
                    Some(CompactRefused::SyncRunning) => {
                        if !wait_or_give_up(deadline) {
                            return Err(Error::SyncLocked {
                                path: self.db_path().to_path_buf(),
                                waited_ms: started.elapsed().as_millis() as u64,
                            });
                        }
                    }
                    Some(refused) => return Err(Error::InsufficientSpace(refused.to_string())),
                    None => return Err(Error::query(error)),
                },
            }
        }
    }
}

/// The `(source, session_id)` keys of `refs`, which must all be by id: a path
/// names a transcript, and forgetting is about sessions the catalog holds.
fn id_keys(refs: Vec<SessionRef>, operation: &str) -> Result<BTreeSet<Key>, Error> {
    refs.into_iter()
        .map(|r| match r {
            SessionRef::Id { source, session_id } => Ok((source.as_str().to_string(), session_id)),
            SessionRef::Path { .. } => Err(Error::InvalidArgument(format!(
                "{operation} takes sessions by id, not by transcript path"
            ))),
        })
        .collect()
}

fn lock_deadline(started: Instant, timeout_ms: u64) -> Instant {
    started + Duration::from_millis(timeout_ms.min(MAX_LOCK_WAIT_MS))
}

/// Sleep one retry step if the budget allows; `false` when it is spent.
fn wait_or_give_up(deadline: Instant) -> bool {
    let now = Instant::now();
    if now >= deadline {
        return false;
    }
    std::thread::sleep((deadline - now).min(LOCK_RETRY));
    true
}

fn wait_for_sync_lock(db_path: &Path, timeout_ms: u64) -> Result<SyncRunLock, Error> {
    let started = Instant::now();
    let deadline = lock_deadline(started, timeout_ms);
    loop {
        if let Some(lock) = try_acquire_sync_lock(db_path).map_err(Error::sync)? {
            return Ok(lock);
        }
        check_capture_cancelled().map_err(Error::sync)?;
        if !wait_or_give_up(deadline) {
            return Err(Error::SyncLocked {
                path: db_path.to_path_buf(),
                waited_ms: started.elapsed().as_millis() as u64,
            });
        }
    }
}

/// Every session that holds evidence or claims to: a catalog row not marked
/// `shallow`, a hydrated observation, a checkpoint, or any evidence row —
/// including rows whose session has no catalog row at all.
fn evidence_holders(conn: &Connection) -> anyhow::Result<BTreeSet<Key>> {
    let mut statement = conn.prepare(
        "SELECT source, session_id FROM sessions WHERE discovery_state IS NOT 'shallow' \
         UNION SELECT source, session_id FROM session_presences \
               WHERE discovery_state IS NOT 'shallow' \
         UNION SELECT source, session_id FROM session_observations \
               WHERE discovery_state = 'full' \
         UNION SELECT DISTINCT source, session_id FROM session_events \
         UNION SELECT DISTINCT source, session_id FROM tool_calls \
         UNION SELECT DISTINCT source, session_id FROM file_edits \
         UNION SELECT DISTINCT source, session_id FROM session_markers \
         UNION SELECT DISTINCT source, session_id FROM observation_evidence \
         UNION SELECT source, session_id FROM session_hydration_checkpoints \
         UNION SELECT source, session_id FROM observation_hydration_checkpoints \
         UNION SELECT 'grok', session_id FROM grok_session_turns",
    )?;
    let rows = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

/// One session to forget, with what forgetting it touches beyond its own rows.
struct Target {
    key: Key,
    /// Transcripts its parents' relationships name for it.
    child_locators: Vec<String>,
    /// Delegating parents that stay, whose checkpoints vouch for this child.
    kept_parents: Vec<Key>,
}

/// Delegation edges: parent to child and back, with the child's transcript.
#[derive(Default)]
struct Family {
    children: HashMap<Key, Vec<Key>>,
    parents: HashMap<Key, Vec<Key>>,
    locators: HashMap<Key, Vec<String>>,
}

impl Family {
    fn load(conn: &Connection) -> anyhow::Result<Self> {
        let mut family = Family::default();
        let mut statement = conn.prepare(
            "SELECT source, parent_session_id, child_session_id, evidence_locator \
             FROM session_relationships \
             WHERE relationship = ? AND child_session_id IS NOT NULL",
        )?;
        let mut rows = statement.query([crate::relationships::RELATIONSHIP_DELEGATED])?;
        while let Some(row) = rows.next()? {
            let source: String = row.get(0)?;
            let parent = (source.clone(), row.get::<_, String>(1)?);
            let child = (source, row.get::<_, String>(2)?);
            if parent == child {
                continue;
            }
            if let Some(locator) = row.get::<_, Option<String>>(3)? {
                family
                    .locators
                    .entry(child.clone())
                    .or_default()
                    .push(locator);
            }
            family
                .children
                .entry(parent.clone())
                .or_default()
                .push(child.clone());
            family.parents.entry(child).or_default().push(parent);
        }
        Ok(family)
    }

    /// The nearest catalogued ancestors of `key`: up through its parents,
    /// stopping on each path at the first catalogued session.
    fn catalogued_ancestors(&self, key: &Key, catalog: &HashSet<Key>) -> Vec<Key> {
        let mut found = Vec::new();
        let mut visited = HashSet::from([key.clone()]);
        let mut frontier = vec![key.clone()];
        while let Some(node) = frontier.pop() {
            for parent in self.parents.get(&node).into_iter().flatten() {
                if !visited.insert(parent.clone()) {
                    continue;
                }
                if catalog.contains(parent) {
                    found.push(parent.clone());
                } else {
                    frontier.push(parent.clone());
                }
            }
        }
        found
    }

    /// `start` and every descendant reached through children `descend`
    /// admits.
    fn closure(&self, start: BTreeSet<Key>, descend: impl Fn(&Key) -> bool) -> BTreeSet<Key> {
        let mut reached = start;
        let mut frontier: Vec<Key> = reached.iter().cloned().collect();
        while let Some(node) = frontier.pop() {
            for child in self.children.get(&node).into_iter().flatten() {
                if !reached.contains(child) && descend(child) {
                    reached.insert(child.clone());
                    frontier.push(child.clone());
                }
            }
        }
        reached
    }
}

/// The sessions to forget, in key order, and how many in scope holding
/// evidence were skipped as unrecoverable.
fn plan(
    conn: &Connection,
    roots: &ProviderRoots,
    named: BTreeSet<Key>,
    keep_only: bool,
    include_unrecoverable: bool,
) -> anyhow::Result<(Vec<Target>, u64)> {
    let family = Family::load(conn)?;
    let catalog: HashSet<Key> = conn
        .prepare("SELECT source, session_id FROM sessions")?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    let scope = scope(conn, &family, &catalog, named, keep_only)?;
    let mut recovery = Recovery::new(conn, roots, &family, &catalog)?;
    let (forget, skipped) = recovery.select(&scope, include_unrecoverable)?;
    let mut targets = Vec::with_capacity(forget.len());
    for key in &forget {
        let child_locators = family.locators.get(key).cloned().unwrap_or_default();
        let kept_parents = recovery.kept_parents(key, &child_locators, &forget, &scope)?;
        targets.push(Target {
            key: key.clone(),
            child_locators,
            kept_parents,
        });
    }
    Ok((targets, skipped))
}

/// The sessions `named` puts in scope: with `keep_only`, every evidence
/// holder outside the named sessions and all their delegated descendants;
/// otherwise the named sessions and their catalog-less delegated descendants.
fn scope(
    conn: &Connection,
    family: &Family,
    catalog: &HashSet<Key>,
    named: BTreeSet<Key>,
    keep_only: bool,
) -> anyhow::Result<BTreeSet<Key>> {
    if !keep_only {
        return Ok(family.closure(named, |child| !catalog.contains(child)));
    }
    let keep = family.closure(named, |_| true);
    Ok(evidence_holders(conn)?
        .into_iter()
        .filter(|key| !keep.contains(key))
        .collect())
}

/// Forget sessions from `pending` in one write transaction until it has
/// removed [`BATCH_ROWS`] evidence rows or run out of sessions.
fn forget_batch<'a>(
    conn: &mut Connection,
    pending: &mut std::iter::Peekable<impl Iterator<Item = &'a Target>>,
    report: &mut ForgetReport,
) -> anyhow::Result<()> {
    let started = Instant::now();
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let clock: i64 = tx
        .query_row(
            "SELECT version FROM observation_clock WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .optional()?
        .unwrap_or(0);
    let cursor_sources: Vec<String> = tx
        .prepare("SELECT DISTINCT source FROM transcript_cursors")?
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    let mut rows = 0;
    while rows < BATCH_ROWS && started.elapsed() < BATCH_TIME {
        let Some(target) = pending.next() else {
            break;
        };
        let removed = forget_session(&tx, target, &cursor_sources)?;
        rows += removed.rows();
        if removed.changed {
            report.sessions += 1;
        }
        report.events += removed.events;
        report.tool_calls += removed.tool_calls;
        report.file_edits += removed.file_edits;
        report.markers += removed.markers;
        report.observation_evidence += removed.observation_evidence;
    }
    // This transaction is the only writer since `clock` was read, so every
    // tombstone stamped after it is one of the deletes above.
    tx.execute(
        &format!(
            "DELETE FROM evidence_tombstones WHERE kind IN ({TOMBSTONED_KINDS}) AND revision > ?"
        ),
        [clock],
    )?;
    tx.commit()?;
    Ok(())
}

#[derive(Default)]
struct Removed {
    events: u64,
    tool_calls: u64,
    file_edits: u64,
    markers: u64,
    observation_evidence: u64,
    changed: bool,
}

impl Removed {
    fn rows(&self) -> u64 {
        self.events + self.tool_calls + self.file_edits + self.markers + self.observation_evidence
    }
}

fn forget_session(
    tx: &Connection,
    target: &Target,
    cursor_sources: &[String],
) -> anyhow::Result<Removed> {
    let (source, session_id) = (target.key.0.as_str(), target.key.1.as_str());
    let mut removed = Removed {
        events: delete_rows(tx, "session_events", source, session_id)?,
        tool_calls: delete_rows(tx, "tool_calls", source, session_id)?,
        file_edits: delete_rows(tx, "file_edits", source, session_id)?,
        markers: delete_rows(tx, "session_markers", source, session_id)?,
        observation_evidence: delete_rows(tx, "observation_evidence", source, session_id)?,
        changed: false,
    };
    let mut state = forget_hydration_state(tx, target, cursor_sources)?;
    // A kept parent's checkpoint vouches for this child's evidence too. Without
    // it, and marked shallow like any session missing evidence, the parent's
    // next hydration re-reads its related transcripts.
    for (parent_source, parent_id) in &target.kept_parents {
        delete_rows(
            tx,
            "session_hydration_checkpoints",
            parent_source,
            parent_id,
        )?;
        delete_rows(
            tx,
            "observation_hydration_checkpoints",
            parent_source,
            parent_id,
        )?;
        mark_shallow(tx, parent_source, parent_id)?;
        // A hydration of the parent taken before this batch is fenced too:
        // the related evidence it was acquired against has changed.
        bump_observations(tx, parent_source, parent_id)?;
    }
    state += mark_shallow(tx, source, session_id)?;
    removed.changed = removed.rows() + state > 0;
    if removed.changed {
        bump_observations(tx, source, session_id)?;
    }
    Ok(removed)
}

/// Delete one session's rows from a table keyed by `(source, session_id)`;
/// the number deleted.
fn delete_rows(
    tx: &Connection,
    table: &str,
    source: &str,
    session_id: &str,
) -> rusqlite::Result<u64> {
    tx.prepare_cached(&format!(
        "DELETE FROM {table} WHERE source = ? AND session_id = ?"
    ))?
    .execute(params![source, session_id])
    .map(|n| n as u64)
}

/// Drop what claims the session's evidence is current — its checkpoints, Grok
/// turn census and transcript cursors — and clear `child_has_events` on the
/// relationships naming it; the number of rows changed.
fn forget_hydration_state(
    tx: &Connection,
    target: &Target,
    cursor_sources: &[String],
) -> anyhow::Result<u64> {
    let (source, session_id) = (target.key.0.as_str(), target.key.1.as_str());
    let mut state = delete_rows(tx, "session_hydration_checkpoints", source, session_id)?;
    state += delete_rows(tx, "observation_hydration_checkpoints", source, session_id)?;
    if source == "grok" {
        state += tx
            .prepare_cached("DELETE FROM grok_session_turns WHERE session_id = ?")?
            .execute([session_id])? as u64;
    }
    state += forget_cursors(tx, target, cursor_sources)?;
    state += tx
        .prepare_cached(
            "UPDATE session_relationships SET child_has_events = 0 \
             WHERE source = ? AND child_session_id = ? AND child_has_events <> 0",
        )?
        .execute(params![source, session_id])? as u64;
    Ok(state)
}

/// Set the session's catalog rows to `shallow`; the number of rows changed.
fn mark_shallow(tx: &Connection, source: &str, session_id: &str) -> anyhow::Result<u64> {
    let mut changed = tx
        .prepare_cached(
            "UPDATE sessions SET discovery_state = 'shallow' \
             WHERE source = ? AND session_id = ? AND discovery_state IS NOT 'shallow'",
        )?
        .execute(params![source, session_id])? as u64;
    changed += tx
        .prepare_cached(
            "UPDATE session_presences SET discovery_state = 'shallow' \
             WHERE source = ? AND session_id = ? AND discovery_state IS NOT 'shallow'",
        )?
        .execute(params![source, session_id])? as u64;
    changed += tx
        .prepare_cached(
            "UPDATE session_observations SET discovery_state = 'shallow', updated_ms = ? \
             WHERE source = ? AND session_id = ? AND discovery_state = 'full'",
        )?
        .execute(params![crate::now_ms(), source, session_id])? as u64;
    Ok(changed)
}

/// New observation revisions fence an acquisition that started before this:
/// the evidence it was taken against has changed.
fn bump_observations(tx: &Connection, source: &str, session_id: &str) -> anyhow::Result<()> {
    for key in observation_keys(tx, source, session_id)? {
        observations::bump_revision(tx, &key)?;
    }
    Ok(())
}
