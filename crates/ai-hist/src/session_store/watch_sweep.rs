//! One watch loop's sweep, and what it knows about the roots it watches.
//!
//! Every filesystem-event tick of [`SessionStore::watch`] used to be a full
//! forced sweep: on a machine with one live Claude session that meant every
//! provider walked every couple of seconds. The loop now says which roots
//! fired ([`ChangeScope`]); [`WatchSources`] maps each root back to the
//! sources that registered it, so the tick sweeps only those. Startup,
//! backstop and manual ticks — and any event the loop could not place — stay
//! full sweeps.

use super::*;
use crate::discover::WatchRoot;
use crate::ingest::path_scope::PathScope;
use crate::ingest::{sync_watch_roots_by_source, SweepScope};
use crate::watch::{ChangeScope, TickRequest, TickTrigger, DEFAULT_SLOW_POLL_MS};
use std::collections::{BTreeSet, HashMap};

#[path = "watch_sweep_checkpoint.rs"]
mod checkpoint_worker;
use checkpoint_worker::checkpoint_after_quiet;
#[cfg(test)]
use checkpoint_worker::{
    checkpoint_after_quiet_with, checkpoint_wal, note_quiet_checkpoint, QuietCheckpointDecision,
};

/// Which sources registered each watched root.
pub(super) struct WatchSources {
    roots: ProviderRoots,
    /// Every watched root's path, and the sources whose roots it is.
    by_root: Mutex<HashMap<PathBuf, BTreeSet<Source>>>,
}

/// What one tick sweeps: the provider roots to read and the sources to read
/// them for (`None` for every source).
pub(super) struct SweepPlan {
    pub(super) roots: ProviderRoots,
    pub(super) sources: Option<Vec<Source>>,
    paths: Option<PathScope>,
}

impl SweepPlan {
    pub(super) fn scope(&self) -> SweepScope {
        match &self.sources {
            // An unknown root widens to every source, and a full sweep does
            // not keep a path list that was only true of some of them.
            None => SweepScope::everything(),
            Some(sources) => SweepScope::only(sources.iter().map(|source| source.as_str()))
                .with_paths(self.paths.clone()),
        }
    }
}

impl WatchSources {
    pub(super) fn new(roots: ProviderRoots) -> Self {
        Self {
            roots,
            by_root: Mutex::new(HashMap::new()),
        }
    }

    /// Every root the loop should watch. Called at startup and on every
    /// backstop refresh, so it also brings the root-to-source map up to date
    /// with exactly the roots the loop is about to adopt.
    pub(super) fn watch_roots(&self) -> Vec<WatchRoot> {
        let by_source = sync_watch_roots_by_source(&self.roots);
        let mut by_root = self.by_root.lock().unwrap_or_else(PoisonError::into_inner);
        by_root.clear();
        for (name, roots) in &by_source {
            let Some(source) = Source::parse(name) else {
                continue;
            };
            for root in roots {
                by_root.entry(root.path.clone()).or_default().insert(source);
            }
        }
        crate::ingest::merge_watch_roots(
            by_source.into_iter().flat_map(|(_, roots)| roots).collect(),
        )
    }

    /// What `request` sweeps. A forced tick whose every root is known sweeps
    /// only the sources those roots belong to; anything else is a full sweep.
    pub(super) fn plan(&self, request: &TickRequest) -> SweepPlan {
        let (sources, paths) = match &request.scope {
            ChangeScope::Roots {
                roots: fired,
                paths,
            } if request.force => {
                let sources = sources_of(
                    &self.by_root.lock().unwrap_or_else(PoisonError::into_inner),
                    fired,
                );
                let paths = sources
                    .as_ref()
                    .and_then(|_| PathScope::from_paths(paths.iter().cloned()));
                (sources, paths)
            }
            _ => (None, None),
        };
        SweepPlan {
            roots: self.roots.clone(),
            sources,
            paths,
        }
    }

    /// Sources a discover tick reads.
    ///
    /// `None` is every configured source: startup, poll, backstop, manual,
    /// and a filesystem event the loop could not place. A rescan notice is
    /// that last case. The watcher has lost which paths changed, so the tick
    /// shallow-discovers every source. It still does not sync.
    ///
    /// `Some` is the sources whose roots an event landed under. A root this
    /// map does not know widens to every source, the same way a sync tick
    /// does, rather than guessing or skipping the tick.
    pub(super) fn discover_sources(&self, request: &TickRequest) -> Option<Vec<Source>> {
        if request.trigger != TickTrigger::FsEvent {
            return None;
        }
        let ChangeScope::Roots { roots: fired, .. } = &request.scope else {
            return None;
        };
        let by_root = self.by_root.lock().unwrap_or_else(PoisonError::into_inner);
        sources_of(&by_root, fired)
    }
}

/// The sources the fired roots belong to, or `None` — a full sweep — when
/// one of them is a root this map does not know.
fn sources_of(
    by_root: &HashMap<PathBuf, BTreeSet<Source>>,
    fired: &BTreeSet<PathBuf>,
) -> Option<Vec<Source>> {
    let mut sources = BTreeSet::new();
    for root in fired {
        sources.extend(by_root.get(root)?.iter().copied());
    }
    Some(sources.into_iter().collect())
}

/// What one tick tells its report: the sessions it changed, and the sources
/// it swept.
#[derive(Default)]
pub(super) struct TickNotes {
    pub(super) changed: Vec<SessionRef>,
    pub(super) sources: Option<Vec<Source>>,
    pub(super) kind: WatchTick,
    pub(super) diagnostics: Vec<SyncDiagnostic>,
}

/// Coalesce exact captures and checkpoint only after a short quiet interval.
///
/// The retained watch writer has SQLite's commit-thread auto-checkpoint
/// disabled. This worker keeps that copy/fsync work off the leading tick while
/// enforcing a 30 s PASSIVE-checkpoint deadline for a sustained burst. Full
/// sweeps retain their separate bounded-TRUNCATE space-reclamation role. If
/// the worker is unavailable or three consecutive attempts cannot complete,
/// the next writer checkout restores SQLite's default automatic checkpoint
/// threshold.
pub(super) struct QuietCheckpoint {
    schedule: Option<std::sync::mpsc::Sender<()>>,
    available: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl QuietCheckpoint {
    pub(super) fn new(db_path: PathBuf, debounce_ms: u64) -> Self {
        let quiet_ms = debounce_ms
            .saturating_mul(5)
            .clamp(1_000, DEFAULT_SLOW_POLL_MS);
        Self::with_quiet(
            db_path,
            Duration::from_millis(quiet_ms),
            Duration::from_millis(DEFAULT_SLOW_POLL_MS),
            None,
        )
    }

    fn with_quiet(
        db_path: PathBuf,
        quiet: Duration,
        max_delay: Duration,
        completed: Option<std::sync::mpsc::Sender<crate::ingest::SweepCheckpoint>>,
    ) -> Self {
        let (schedule, pending) = std::sync::mpsc::channel();
        let available = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let worker_available = std::sync::Arc::clone(&available);
        let spawned = std::thread::Builder::new()
            .name("ai-hist-wal-checkpoint".into())
            .spawn(move || {
                checkpoint_after_quiet(
                    &db_path,
                    quiet,
                    max_delay,
                    pending,
                    completed,
                    &worker_available,
                );
            });
        available.store(spawned.is_ok(), std::sync::atomic::Ordering::Release);
        Self {
            schedule: spawned.ok().map(|_| schedule),
            available,
        }
    }

    fn enabled(&self) -> bool {
        self.available.load(std::sync::atomic::Ordering::Acquire)
    }

    fn schedule(&self) {
        if self.enabled()
            && self
                .schedule
                .as_ref()
                .is_some_and(|schedule| schedule.send(()).is_err())
        {
            self.available
                .store(false, std::sync::atomic::Ordering::Release);
        }
    }
}

/// One watch loop's sweep: the same locked `sync` as [`SessionStore::sync`],
/// scoped by [`WatchSources`] and diffed against a rolling catalog baseline.
///
/// The sweep and the report sink run on the loop's thread, one tick at a
/// time, so a single slot (`notes`) carries "what this tick changed" from
/// the one to the other.
pub(super) struct WatchSweep {
    pub(super) db_path: PathBuf,
    pub(super) sources: Arc<WatchSources>,
    pub(super) stop: StopToken,
    pub(super) carry: Carry,
    pub(super) notes: Arc<Mutex<TickNotes>>,
    pub(super) reports: ReportSlot,
    pub(super) store: SessionStore,
    pub(super) kind: WatchTick,
    pub(super) checkpoint: QuietCheckpoint,
}

/// How the watch loop reads a sweep's result.
///
/// One conversion rather than one per caller: a tick that never took the lock
/// looked at nothing, and a caller that folded that into `skipped_unchanged`
/// would be telling the loop the change had been considered and dismissed.
impl From<crate::ingest::SyncTick> for crate::watch::TickOutcome {
    fn from(tick: crate::ingest::SyncTick) -> Self {
        Self {
            swept: tick.swept,
            skipped_unchanged: tick.skipped_unchanged(),
            contended: !tick.attempted,
            changed_nothing: false,
        }
    }
}

/// What one tick hands the next.
#[derive(Default)]
pub(super) struct Carry {
    /// The catalog digest the previous tick ended with.
    baseline: Mutex<Option<CatalogFingerprint>>,
    /// The validated writable connection. A tick's sweep only uses it inside
    /// `sync_facade_tick`, which holds the `SyncRunLock` (the cache's own
    /// rule), and the cache re-validates whenever another connection moved
    /// the schema or the data -- so a quiet loop pays `open_db`'s schema
    /// check once, not per tick.
    writer: Mutex<crate::store::sync_connection::Cache>,
    /// Whether the last sweep wrote nothing at all: the store-wide change
    /// feed head, read under the lock before and after it, did not move. A
    /// stronger statement than an empty catalog diff, which an append that
    /// adds evidence rows without moving a catalog column would also give.
    wrote_nothing: std::sync::atomic::AtomicBool,
}

impl WatchSweep {
    pub(super) fn new(
        store: SessionStore,
        sources: Arc<WatchSources>,
        stop: StopToken,
        notes: Arc<Mutex<TickNotes>>,
        reports: ReportSlot,
        opts: &WatchOptions,
    ) -> Self {
        Self {
            db_path: store.db_path.clone(),
            sources,
            stop,
            carry: Default::default(),
            notes,
            reports,
            checkpoint: QuietCheckpoint::new(store.db_path.clone(), opts.debounce_ms),
            store,
            kind: opts.kind,
        }
    }

    pub(super) fn tick(&self, request: &TickRequest) -> anyhow::Result<TickOutcome> {
        if self.kind == WatchTick::Discover {
            return self.discover_tick(request);
        }
        let plan = self.sources.plan(request);
        {
            let mut notes = self.notes.lock().unwrap_or_else(PoisonError::into_inner);
            notes.kind = WatchTick::Sync;
            notes.sources = plan.sources.clone();
        }
        // Both reads happen under the sync lock, like `sync`'s. The
        // baseline is the previous swept tick's `after` digest when there
        // is one — nothing this loop reported has moved since, and a
        // change another process made in between is a change since the
        // last report either way — and a fresh read otherwise.
        let mut base = self
            .carry
            .baseline
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        // The baseline this sweep compares against, kept outside it so a
        // cancelled sweep can still say what it committed (below).
        let mut swept_from: Option<CatalogFingerprint> = None;
        let result = with_capture_token(self.stop.clone(), || {
            rolling_tick(&mut base, |previous| {
                self.sweep(request.force, &plan, previous, &mut swept_from)
            })
        });
        match result {
            Ok((mut outcome, changed, diagnostics)) => {
                outcome.changed_nothing = !outcome.contended
                    && changed.is_empty()
                    && self
                        .carry
                        .wrote_nothing
                        .load(std::sync::atomic::Ordering::Relaxed);
                if request.force && plan.paths.is_some() && outcome.swept {
                    self.checkpoint.schedule();
                }
                let mut notes = self.notes.lock().unwrap_or_else(PoisonError::into_inner);
                notes.changed = changed;
                notes.diagnostics = diagnostics;
                Ok(outcome)
            }
            Err(error) => {
                // A stop can land after the sweep committed chunks, or
                // after it finished and rolled the baseline forward — the
                // capture scope re-checks the token on the way out. The
                // loop ends on a cancellation, so this is the last chance
                // to report those rows.
                if error.chain().any(|cause| cause.is::<CaptureCancelled>()) {
                    if let Some(before) = swept_from {
                        self.report_cancelled(&mut base, &before);
                    }
                }
                Err(error)
            }
        }
    }

    /// A shallow discover of the sources `request` names. Not a sync: no
    /// evidence write and no transcript parse. An event the loop could not
    /// place discovers every configured source.
    fn discover_tick(&self, request: &TickRequest) -> anyhow::Result<TickOutcome> {
        let sources = self.sources.discover_sources(request);
        {
            let mut notes = self.notes.lock().unwrap_or_else(PoisonError::into_inner);
            notes.kind = WatchTick::Discover;
            notes.sources = sources.clone();
            notes.changed.clear();
            notes.diagnostics.clear();
        }
        match self.store.discover(DiscoveryOptions {
            sources,
            stop: Some(self.stop.clone()),
            ..DiscoveryOptions::default()
        }) {
            Ok(_) => Ok(TickOutcome::default()),
            Err(Error::Cancelled(_)) => Err(CaptureCancelled.into()),
            Err(error) => Err(anyhow::anyhow!("{error}")),
        }
    }

    fn sweep(
        &self,
        force: bool,
        plan: &SweepPlan,
        previous: Option<CatalogFingerprint>,
        swept_from: &mut Option<CatalogFingerprint>,
    ) -> anyhow::Result<Option<LockedWatchSweep>> {
        let mut writer = std::mem::take(
            &mut *self
                .carry
                .writer
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        );
        let outcome = crate::store::sync_connection::with_cache(&mut writer, || {
            self.locked_sweep(previous, plan, force, swept_from)
        });
        *self
            .carry
            .writer
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = writer;
        outcome
    }

    fn locked_sweep(
        &self,
        previous: Option<CatalogFingerprint>,
        plan: &SweepPlan,
        force: bool,
        swept_from: &mut Option<CatalogFingerprint>,
    ) -> anyhow::Result<Option<LockedWatchSweep>> {
        let head = |conn: &rusqlite::Connection| {
            crate::change_feed::read_head(conn)
                .ok()
                .map(|head| (head.epoch, head.revision))
        };
        let head_before = std::cell::Cell::new(None);
        self.carry
            .wrote_nothing
            .store(false, std::sync::atomic::Ordering::Relaxed);
        let outcome = sync_facade_tick(
            &self.db_path,
            &plan.roots,
            force,
            &plan.scope(),
            |conn| {
                configure_auto_checkpoint(conn, self.checkpoint.enabled())?;
                head_before.set(head(conn));
                let before = match previous {
                    Some(before) => before,
                    None => catalog_fingerprint(conn).map_err(anyhow::Error::from)?,
                };
                *swept_from = Some(before.clone());
                #[cfg(test)]
                if cancel_diff_fault(&self.db_path) {
                    // Stop once the baseline is taken, so the
                    // sweep is cancelled with a baseline to diff.
                    self.stop.stop();
                }
                Ok(before)
            },
            |conn, before, _tick, _succeeded| {
                let unmoved = head_before.get().is_some() && head_before.get() == head(conn);
                self.carry
                    .wrote_nothing
                    .store(unmoved, std::sync::atomic::Ordering::Relaxed);
                let (after, changed) = changes_under_lock(conn, &before)?;
                Ok((after, changed))
            },
        )?;
        Ok(
            outcome.map(|(tick, (after, changed), _sync_lock, diagnostics)| {
                (tick, after, changed, diagnostics)
            }),
        )
    }

    /// Diff the catalog against the baseline a cancelled sweep started from,
    /// roll it forward, and let the cancelled report carry the result. A
    /// read-only connection outside the sync lock: the diff writes nothing,
    /// and a reader is no worse than the "changed since the last report" this
    /// field already promises.
    fn report_cancelled(&self, base: &mut Option<CatalogFingerprint>, before: &CatalogFingerprint) {
        let diff = open_db_readonly(&self.db_path)
            .map_err(Error::sync)
            .and_then(|conn| {
                #[cfg(test)]
                if cancel_diff_fault(&self.db_path) {
                    return Err(Error::sync(anyhow::anyhow!("injected diff failure")));
                }
                changes_under_lock(&conn, before)
            });
        match diff {
            Ok((after, changed)) => {
                *base = Some(after);
                self.notes
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .changed = changed;
            }
            // Surfaced rather than swallowed, and without giving up the
            // cancellation: the failure goes onto the stream as its own
            // `Err`, ahead of the cancelled report, and the cancellation is
            // still what this tick returns, so the loop ends as it was asked
            // to.
            Err(diff_error) => send_report(&self.reports, || {
                Err(Error::sync(anyhow::anyhow!(
                    "watch tick cancelled after committing changes \
                     that could not be read back for its report: \
                     {diff_error}"
                )))
            }),
        }
    }
}

fn configure_auto_checkpoint(
    conn: &rusqlite::Connection,
    defer_to_worker: bool,
) -> rusqlite::Result<()> {
    const SQLITE_DEFAULT_WAL_AUTOCHECKPOINT_PAGES: i64 = 1_000;
    let target = if defer_to_worker {
        0
    } else {
        SQLITE_DEFAULT_WAL_AUTOCHECKPOINT_PAGES
    };
    let current: i64 = conn.query_row("PRAGMA wal_autocheckpoint", [], |row| row.get(0))?;
    if current != target {
        conn.pragma_update(None, "wal_autocheckpoint", target)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::watch::TickTrigger;

    fn home() -> (tempfile::TempDir, ProviderRoots) {
        let home = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(home.path().join(".claude/projects")).unwrap();
        std::fs::create_dir_all(home.path().join(".codex/sessions")).unwrap();
        let roots =
            ProviderRoots::from_home(home.path().to_path_buf(), home.path().join("opencode.db"));
        (home, roots)
    }

    fn event(roots: &[PathBuf]) -> TickRequest {
        TickRequest {
            trigger: TickTrigger::FsEvent,
            force: true,
            scope: ChangeScope::Roots {
                roots: roots.iter().cloned().collect(),
                paths: BTreeSet::new(),
            },
        }
    }

    fn full(trigger: TickTrigger) -> TickRequest {
        TickRequest {
            trigger,
            force: trigger.forces_scan(),
            scope: ChangeScope::Everything,
        }
    }

    #[test]
    fn an_event_tick_sweeps_the_sources_whose_roots_fired() {
        let (home, roots) = home();
        let sources = WatchSources::new(roots);
        sources.watch_roots();
        let claude = home.path().join(".claude/projects");
        let codex = home.path().join(".codex/sessions");

        assert_eq!(
            sources.plan(&event(std::slice::from_ref(&claude))).sources,
            Some(vec![Source::Claude])
        );
        assert_eq!(
            sources.plan(&event(&[claude.clone(), codex])).sources,
            Some(vec![Source::Claude, Source::Codex])
        );
        // A root the map does not know is not guessed at.
        assert_eq!(
            sources
                .plan(&event(&[claude, home.path().join("elsewhere")]))
                .sources,
            None
        );
        for trigger in [TickTrigger::Startup, TickTrigger::Poll, TickTrigger::Manual] {
            assert_eq!(sources.plan(&full(trigger)).sources, None);
        }
        assert_eq!(
            sources.plan(&full(TickTrigger::FsEvent)).sources,
            None,
            "an event the loop could not place"
        );
    }

    #[test]
    fn an_event_tick_carries_the_paths_that_changed() {
        let (home, roots) = home();
        let sources = WatchSources::new(roots);
        sources.watch_roots();
        let claude = home.path().join(".claude/projects");
        let file = claude.join("app/s.jsonl");
        let request = TickRequest {
            trigger: TickTrigger::FsEvent,
            force: true,
            scope: ChangeScope::Roots {
                roots: BTreeSet::from([claude.clone()]),
                paths: BTreeSet::from([file.clone()]),
            },
        };
        let plan = sources.plan(&request);
        assert_eq!(plan.sources, Some(vec![Source::Claude]));
        let scope = plan.scope();
        assert!(scope.paths().unwrap().include_file(&file, "", "jsonl"));
        // Roots with no file paths still walk that provider whole.
        assert!(sources.plan(&event(&[claude])).scope().paths().is_none());
    }

    #[test]
    fn a_discover_tick_names_the_sources_whose_roots_fired() {
        let (home, roots) = home();
        let sources = WatchSources::new(roots);
        sources.watch_roots();
        let claude = home.path().join(".claude/projects");
        let codex = home.path().join(".codex/sessions");

        assert_eq!(
            sources.discover_sources(&event(std::slice::from_ref(&claude))),
            Some(vec![Source::Claude])
        );
        assert_eq!(
            sources.discover_sources(&event(&[claude.clone(), codex])),
            Some(vec![Source::Claude, Source::Codex])
        );
        assert_eq!(
            sources.discover_sources(&event(&[claude, home.path().join("elsewhere")])),
            None,
            "a root the map does not know is not guessed at"
        );
        assert_eq!(
            sources.discover_sources(&full(TickTrigger::FsEvent)),
            None,
            "an event the loop could not place discovers every source"
        );
        for trigger in [TickTrigger::Startup, TickTrigger::Poll, TickTrigger::Manual] {
            assert_eq!(sources.discover_sources(&full(trigger)), None);
        }
    }

    #[test]
    fn an_unforced_tick_is_never_scoped() {
        let (home, roots) = home();
        let sources = WatchSources::new(roots);
        sources.watch_roots();
        let mut request = event(&[home.path().join(".claude/projects")]);
        request.force = false;
        let plan = sources.plan(&request);
        assert_eq!(plan.sources, None);
        assert_eq!(plan.scope(), SweepScope::everything());
    }

    #[test]
    fn quiet_checkpoint_passively_catches_up_a_large_wal() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("history.db");
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA wal_autocheckpoint=0;
             CREATE TABLE payload (body BLOB NOT NULL);
             WITH RECURSIVE rows(n) AS (
               VALUES(1) UNION ALL SELECT n + 1 FROM rows WHERE n < 1100
             )
             INSERT INTO payload(body) SELECT zeroblob(4096) FROM rows;",
        )
        .unwrap();
        let wal = crate::diagnostics::wal_path(&db_path);
        assert!(
            std::fs::metadata(&wal).unwrap().len() > crate::ingest::WAL_TRUNCATE_BYTES,
            "fixture must cross the production truncate threshold"
        );

        let (completed, outcome) = std::sync::mpsc::channel();
        let checkpoint = QuietCheckpoint::with_quiet(
            db_path.clone(),
            Duration::from_millis(10),
            Duration::from_secs(5),
            Some(completed),
        );
        checkpoint.schedule();
        let outcome = outcome.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(outcome.complete, "quiet maintenance catches up every frame");
        assert!(
            !outcome.escalated,
            "visibility-adjacent upkeep stays PASSIVE"
        );
        assert!(
            std::fs::metadata(&wal).unwrap().len() > 0,
            "physical space reclamation remains the full sweep's job"
        );
    }

    #[test]
    fn quiet_checkpoint_never_creates_a_missing_database() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("missing.db");
        let available = std::sync::atomic::AtomicBool::new(true);
        let mut incomplete_attempts = 0;
        assert_eq!(
            note_quiet_checkpoint(
                checkpoint_wal(&db_path),
                &None,
                &available,
                &mut incomplete_attempts,
            ),
            QuietCheckpointDecision::Stop
        );
        assert!(!available.load(std::sync::atomic::Ordering::Acquire));
        assert!(!db_path.exists());
    }

    #[test]
    fn sustained_capture_is_checkpointed_at_the_hard_deadline() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("history.db");
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA wal_autocheckpoint=0;
             CREATE TABLE payload (body BLOB NOT NULL);
             INSERT INTO payload(body) VALUES (zeroblob(4096));",
        )
        .unwrap();

        let (completed, outcome) = std::sync::mpsc::channel();
        let checkpoint = QuietCheckpoint::with_quiet(
            db_path,
            Duration::from_secs(1),
            Duration::from_millis(40),
            Some(completed),
        );
        let schedule = checkpoint.schedule.as_ref().unwrap().clone();
        let producer = std::thread::spawn(move || {
            for _ in 0..20 {
                schedule.send(()).unwrap();
                std::thread::sleep(Duration::from_millis(5));
            }
        });
        let outcome = outcome.recv_timeout(Duration::from_millis(250)).unwrap();
        assert!(outcome.complete);
        assert!(!outcome.escalated);
        producer.join().unwrap();
    }

    #[test]
    fn every_replacement_writer_uses_worker_checkpoint_policy() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("history.db");
        for _ in 0..2 {
            let conn = rusqlite::Connection::open(&db_path).unwrap();
            conn.pragma_update(None, "wal_autocheckpoint", 1_000)
                .unwrap();
            configure_auto_checkpoint(&conn, true).unwrap();
            let current: i64 = conn
                .query_row("PRAGMA wal_autocheckpoint", [], |row| row.get(0))
                .unwrap();
            assert_eq!(current, 0);
        }
    }

    #[test]
    fn unavailable_quiet_worker_restores_auto_checkpoint() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
        configure_auto_checkpoint(&conn, false).unwrap();
        let current: i64 = conn
            .query_row("PRAGMA wal_autocheckpoint", [], |row| row.get(0))
            .unwrap();
        assert_eq!(current, 1_000);
    }

    #[test]
    fn transient_incomplete_checkpoints_retry_before_fallback() {
        let available = std::sync::atomic::AtomicBool::new(true);
        let mut incomplete_attempts = 0;
        let incomplete = crate::ingest::SweepCheckpoint {
            escalated: false,
            complete: false,
            wal_bytes: 4096,
        };
        assert_eq!(
            note_quiet_checkpoint(
                Some(incomplete),
                &None,
                &available,
                &mut incomplete_attempts,
            ),
            QuietCheckpointDecision::Retry
        );
        assert!(available.load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(incomplete_attempts, 1);

        let complete = crate::ingest::SweepCheckpoint {
            complete: true,
            ..incomplete
        };
        assert_eq!(
            note_quiet_checkpoint(Some(complete), &None, &available, &mut incomplete_attempts,),
            QuietCheckpointDecision::Complete
        );
        assert_eq!(incomplete_attempts, 0);

        for expected in 1..=2 {
            assert_eq!(
                note_quiet_checkpoint(
                    Some(incomplete),
                    &None,
                    &available,
                    &mut incomplete_attempts,
                ),
                QuietCheckpointDecision::Retry
            );
            assert_eq!(incomplete_attempts, expected);
        }
        assert_eq!(
            note_quiet_checkpoint(
                Some(incomplete),
                &None,
                &available,
                &mut incomplete_attempts,
            ),
            QuietCheckpointDecision::Stop
        );
        assert!(!available.load(std::sync::atomic::Ordering::Acquire));
    }
}

#[cfg(test)]
#[path = "watch_sweep_idle_retry_tests.rs"]
mod idle_retry_tests;
