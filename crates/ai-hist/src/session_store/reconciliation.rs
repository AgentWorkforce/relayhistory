//! Resumable, fair enumeration for embedders that cannot block on a full sweep.
use super::{Error, SessionStore, Source, SyncOptions, SyncReport, WatchScope, WatchedPath};
use std::{
    collections::{HashSet, VecDeque},
    fs::{self, ReadDir},
    path::PathBuf,
};

/// One reconciliation round. Directory iterators survive between steps; dropping
/// it is safe because acquisition's committed byte checkpoints remain durable.
/// Start another round after completion to find lost events and newly added roots.
#[derive(Debug)]
pub struct Reconciliation {
    store: SessionStore,
    sources: VecDeque<SourceWalk>,
    cold_proof: crate::ingest::path_capture::cold_proof::ColdProof,
    // get_mut only: preserve the round's Send + Sync traits without adding
    // another lock wait beside the existing cancellable SyncRunLock.
    writer: std::sync::Mutex<crate::store::sync_connection::Cache>,
    source_filter: Option<HashSet<Source>>,
}

/// Content-free work accounting for a bounded reconciliation slice.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct ReconciliationStep {
    /// Actual elapsed sleeps in the SDK SQLite busy handler on this executing
    /// thread. Excludes worker-thread and other waits; this is not CPU time.
    /// Embedders may exclude this known blocked time from work-based pacing.
    pub sqlite_busy_wait: std::time::Duration,
    /// Directory entries inspected (at most the supplied bound, clamped to one).
    pub entries: usize,
    /// Exact paths attempted (at most the supplied bound, clamped to one).
    pub paths: usize,
    /// All roots in this round have been exhausted.
    pub complete: bool,
    /// A completed acquisition, when this slice reached evidence paths.
    pub report: Option<SyncReport>,
    /// Shallow catalog work, when `discover_step` was used instead.
    pub discovery: Option<super::DiscoveryReport>,
}

#[derive(Debug)]
struct SourceWalk {
    source: Source,
    roots: VecDeque<WatchedPath>,
    queued: VecDeque<(PathBuf, WatchedPath)>,
    current: Option<OpenDirectory>,
    paused: VecDeque<OpenDirectory>,
    deferred: VecDeque<DeferredWork>,
    deferred_turn: bool,
    visited: HashSet<PathBuf>,
    pending: Vec<PathBuf>,
    finalized: bool,
}

// Only error recovery can pause a healthy iterator. Never accumulate an open
// ReadDir per repaired location; ordinary traversal still holds one iterator.
const MAX_OPEN_DIRECTORIES: usize = 2;

#[derive(Debug)]
enum DeferredWork {
    Directory(PathBuf, WatchedPath),
    Entry(PathBuf, WatchedPath),
}

impl DeferredWork {
    fn key(&self) -> (&PathBuf, &WatchedPath, bool) {
        match self {
            Self::Directory(path, root) => (path, root, true),
            Self::Entry(path, root) => (path, root, false),
        }
    }
}

#[derive(Debug)]
struct OpenDirectory {
    entries: ReadDir,
    root: WatchedPath,
    path: PathBuf,
    canonical: PathBuf,
}

#[cfg(test)]
thread_local! {static FAIL_ENTRY_TYPE: std::cell::Cell<bool> = const {std::cell::Cell::new(false)};}
#[cfg(test)]
thread_local! {
    static FAIL_ENTRY_PATH: std::cell::RefCell<Option<PathBuf>> = const {std::cell::RefCell::new(None)};
    static FAIL_READ_DIRECTORY: std::cell::RefCell<Option<PathBuf>> = const {std::cell::RefCell::new(None)};
}

fn entry_type(entry: &std::fs::DirEntry) -> std::io::Result<std::fs::FileType> {
    #[cfg(test)]
    if FAIL_ENTRY_TYPE.with(|fail| fail.replace(false)) {
        return Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied));
    }
    #[cfg(test)]
    if FAIL_ENTRY_PATH.with(|path| path.borrow().as_ref() == Some(&entry.path())) {
        return Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied));
    }
    entry.file_type()
}

impl SessionStore {
    /// Start fair, incremental reconciliation of the provider-owned watch roots.
    /// Each `step` bounds enumeration and exact path count. A transcript or
    /// provider database snapshot remains atomic at its existing safe boundary;
    /// one large input can exceed a caller's desired wall-clock slice.
    /// Use a separate handle/worker for hot dirty-path sync, retry contention, and
    /// run a fresh round periodically: no filesystem iterator is a snapshot.
    pub fn reconcile(&self) -> Result<Reconciliation, Error> {
        if self.read_only() {
            return Err(Error::read_only("reconcile"));
        }
        let sources = Source::ALL
            .iter()
            .copied()
            .filter_map(|source| {
                let roots = source.capabilities().watch_roots(self.roots());
                (!roots.is_empty()).then(|| SourceWalk {
                    source,
                    roots: roots.into(),
                    queued: VecDeque::new(),
                    current: None,
                    paused: VecDeque::new(),
                    deferred: VecDeque::new(),
                    deferred_turn: false,
                    visited: HashSet::new(),
                    pending: Vec::new(),
                    finalized: false,
                })
            })
            .collect();
        Ok(Reconciliation {
            store: self.clone(),
            sources,
            cold_proof: Default::default(),
            writer: Default::default(),
            source_filter: None,
        })
    }
}

impl Reconciliation {
    /// Restrict this round to selected providers (for an additional provider
    /// login/root). An empty selection completes without provider I/O.
    #[must_use]
    pub fn sources(mut self, sources: impl IntoIterator<Item = Source>) -> Self {
        let sources: HashSet<_> = sources.into_iter().collect();
        self.sources.retain(|walk| sources.contains(&walk.source));
        match &mut self.source_filter {
            Some(allowed) => allowed.retain(|source| sources.contains(source)),
            None => self.source_filter = Some(sources),
        }
        self
    }

    /// Whether this pass has drained ready traversal and acquisition. Failed
    /// enumeration obligations remain retryable and keep `complete` false.
    /// This level query is valid after either a successful step or an error.
    pub fn healthy_pass_exhausted(&self) -> bool {
        self.sources.iter().all(SourceWalk::healthy_drained)
    }

    /// Revisit the retained, validated roots after healthy work has drained.
    /// Returns false without changing an active pass. Deferred failures and
    /// their owning watch scopes survive; completed providers are reseeded too.
    /// This does not declare successful coverage or change retry/backstop timing.
    pub fn refresh_healthy_pass(&mut self) -> bool {
        if !self.healthy_pass_exhausted() {
            return false;
        }
        let Ok(mut fresh) = self.store.reconcile() else {
            return false;
        };
        if let Some(allowed) = &self.source_filter {
            fresh = fresh.sources(allowed.iter().copied());
        }
        for mut old in std::mem::take(&mut self.sources) {
            if let Some(new) = fresh.sources.iter_mut().find(|s| s.source == old.source) {
                new.deferred = std::mem::take(&mut old.deferred);
                new.deferred_turn = old.deferred_turn;
                new.roots.retain(|root| !new.deferred.iter().any(|item| {
                    matches!(item, DeferredWork::Directory(path, owner) if path == &root.path && owner == root)
                }));
            }
        }
        self.sources = fresh.sources;
        self.cold_proof = Default::default();
        *self
            .writer
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Default::default();
        true
    }

    /// Capture explicit safety-poll paths using this round's retained native
    /// proof, without consuming directory positions. Unknown/changed proofs
    /// retain ordinary acquisition. Forced requests bypass native proof reuse.
    /// Source restrictions survive completion; `paths: None` is rejected.
    pub fn capture_paths(&mut self, mut options: SyncOptions) -> Result<SyncReport, Error> {
        if options.paths.is_none() {
            return Err(Error::InvalidArgument(
                "capture_paths requires an explicit paths allowlist".into(),
            ));
        }
        if let Some(allowed) = &self.source_filter {
            options.sources = Some(match options.sources {
                Some(sources) => sources
                    .into_iter()
                    .filter(|source| allowed.contains(source))
                    .collect(),
                None => Source::ALL
                    .iter()
                    .copied()
                    .filter(|source| allowed.contains(source))
                    .collect(),
            });
        }
        if options.force {
            return self.store.sync(options);
        }
        let mut proof = std::mem::take(&mut self.cold_proof);
        let mut writer = std::mem::take(
            self.writer
                .get_mut()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        let result = crate::store::sync_connection::with_cache(&mut writer, || {
            crate::ingest::path_capture::cold_proof::with_cold_proof(&mut proof, || {
                self.store.sync(options)
            })
        });
        self.cold_proof = proof;
        *self
            .writer
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = writer;
        result
    }

    /// Take one source's turn, retaining incomplete work on cancellation, lock
    /// contention or error. Limits are clamped to at least one. Sources rotate
    /// even after an error, so a broken provider cannot starve other providers.
    pub fn step(
        &mut self,
        max_entries: usize,
        max_paths: usize,
        options: SyncOptions,
    ) -> Result<ReconciliationStep, Error> {
        let busy_before = crate::store::busy_wait::total();
        let Some(mut source) = self.sources.pop_front() else {
            return Ok(ReconciliationStep {
                complete: true,
                ..Default::default()
            });
        };
        let mut cold_proof = std::mem::take(&mut self.cold_proof);
        let mut writer = std::mem::take(
            self.writer
                .get_mut()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        let result = crate::store::sync_connection::with_cache(&mut writer, || {
            crate::ingest::path_capture::cold_proof::with_cold_proof(&mut cold_proof, || {
                self.take_step(
                    &mut source,
                    max_entries.max(1),
                    max_paths.max(1),
                    options.clone(),
                )
                .and_then(|mut step| {
                    if source.enumerated() && !source.finalized {
                        let recovered = self.store.reconcile_pending(source.source, &options)?;
                        if let Some(report) = &mut step.report {
                            report.head_revision = recovered.head_revision;
                        } else {
                            step.report = Some(recovered);
                        }
                        source.finalized = true;
                    }
                    Ok(step)
                })
            })
        });
        *self
            .writer
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = writer;
        self.cold_proof = cold_proof;
        // Keep a failed path until a successful retry or the caller drops this
        // round. Missing paths succeed as empty scopes; the next round repairs
        // renames/new arrivals not represented by this iterator.
        source.rotate_reader();
        if !source.finished() {
            self.sources.push_back(source);
        }
        result.map(|mut step| {
            step.sqlite_busy_wait = crate::store::busy_wait::total().saturating_sub(busy_before);
            step.complete = self.sources.is_empty();
            step
        })
    }

    /// Enumerate the same bounded round without reading session evidence. This
    /// supports selected-only embedders: unrelated history remains shallow.
    pub fn discover_step(
        &mut self,
        max_entries: usize,
        max_paths: usize,
        mut options: super::DiscoveryOptions,
    ) -> Result<ReconciliationStep, Error> {
        let busy_before = crate::store::busy_wait::total();
        if options.limit.is_some() {
            return Err(Error::InvalidArgument("reconciliation cannot use a discovery row limit; bound work with max_entries/max_paths".into()));
        }
        let Some(mut source) = self.sources.pop_front() else {
            return Ok(ReconciliationStep {
                complete: true,
                ..Default::default()
            });
        };
        let result = (|| {
            let mut step = ReconciliationStep::default();
            let retained_pending = !source.pending.is_empty();
            if source.pending.is_empty() {
                source.collect(max_entries.max(1), max_paths.max(1), &mut step.entries)?;
            }
            step.paths = source.pending.len().min(max_paths.max(1));
            if step.paths > 0 {
                options.sources = Some(vec![source.source]);
                options.paths = Some(source.pending[..step.paths].to_vec());
                step.discovery = Some(self.store.discover(options)?);
                source.pending.drain(..step.paths);
                if retained_pending {
                    source.deferred_turn = !source.deferred.is_empty();
                }
            }
            Ok(step)
        })();
        source.finalized = source.enumerated();
        source.rotate_reader();
        if !source.finished() {
            self.sources.push_back(source);
        }
        result.map(|mut step: ReconciliationStep| {
            step.sqlite_busy_wait = crate::store::busy_wait::total().saturating_sub(busy_before);
            step.complete = self.sources.is_empty();
            step
        })
    }

    fn take_step(
        &self,
        source: &mut SourceWalk,
        max_entries: usize,
        max_paths: usize,
        mut options: SyncOptions,
    ) -> Result<ReconciliationStep, Error> {
        let mut step = ReconciliationStep::default();
        let retained_pending = !source.pending.is_empty();
        if source.pending.is_empty() {
            source.collect(max_entries, max_paths, &mut step.entries)?;
        }
        step.paths = source.pending.len().min(max_paths);
        if source.pending.is_empty() {
            return Ok(step);
        }
        options.sources = Some(vec![source.source]);
        options.paths = Some(source.pending[..step.paths].to_vec());
        let report = self.store.sync(options)?;
        source.pending.drain(..step.paths);
        if retained_pending {
            source.deferred_turn = !source.deferred.is_empty();
        }
        step.report = Some(report);
        Ok(step)
    }
}

mod walk;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ProviderRoots, StoreOptions};
    use std::io::Write;

    mod recovery_tests;

    fn fixture() -> (tempfile::TempDir, SessionStore, PathBuf) {
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let project = home.join(".claude/projects/p");
        fs::create_dir_all(&project).unwrap();
        let store = SessionStore::open(
            StoreOptions::default()
                .db_path(temp.path().join("history.db"))
                .roots(ProviderRoots::from_home(
                    home.clone(),
                    home.join("opencode.db"),
                )),
        )
        .unwrap();
        (temp, store, project)
    }

    fn write(path: &std::path::Path, id: &str, uuid: &str) {
        let line = serde_json::json!({"sessionId": id, "uuid": uuid, "type":"user",
            "timestamp":"2026-10-23T10:00:00Z", "message":{"role":"user", "content":uuid}});
        writeln!(
            fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .unwrap(),
            "{line}"
        )
        .unwrap();
    }

    #[test]
    fn a_thousand_unchanged_files_share_one_writable_validation_per_round() {
        fn send_sync<T: Send + Sync>() {}
        send_sync::<Reconciliation>();
        let (_temp, store, project) = fixture();
        for n in 0..1000 {
            write(
                &project.join(format!("bench-{n}.jsonl")),
                &format!("bench-{n}"),
                "one",
            );
        }
        store
            .sync(SyncOptions::default().sources([Source::Claude]))
            .unwrap();
        let head = store.head_revision().unwrap();
        let before = crate::store::sync_connection::validations();
        let mut round = store.reconcile().unwrap().sources([Source::Claude]);
        let mut paths = 0;
        let mut slices = 0;
        loop {
            let step = round.step(64, 8, SyncOptions::default()).unwrap();
            paths += step.paths;
            slices += 1;
            assert!(step
                .report
                .as_ref()
                .is_none_or(|report| report.changed.is_empty()));
            if step.complete {
                break;
            }
        }
        assert_eq!(paths, 1000);
        assert!(slices >= 125);
        assert_eq!(store.head_revision().unwrap(), head);
        assert_eq!(
            crate::store::sync_connection::validations() - before,
            1,
            "unchanged slices must not reopen and validate the whole writable schema"
        );
    }

    #[test]
    fn retained_writer_cache_does_not_bypass_sync_lock_timeout_or_cancellation() {
        let (_temp, store, project) = fixture();
        for id in ["a", "b", "c"] {
            write(&project.join(format!("{id}.jsonl")), id, "one");
        }
        store
            .sync(SyncOptions::default().sources([Source::Claude]))
            .unwrap();
        let mut round = store.reconcile().unwrap().sources([Source::Claude]);
        assert_eq!(round.step(64, 1, SyncOptions::default()).unwrap().paths, 1);
        let lease = crate::ingest::try_acquire_sync_lock(store.db_path())
            .unwrap()
            .unwrap();
        let before = crate::store::sync_connection::validations();
        assert!(matches!(
            round.step(64, 1, SyncOptions::default().lock_timeout_ms(0)),
            Err(Error::SyncLocked { .. })
        ));
        assert_eq!(
            crate::store::sync_connection::validations(),
            before,
            "lease precedes cached connection access"
        );
        let stop = crate::StopToken::new();
        let token = stop.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let running = std::thread::spawn(move || {
            let result = round.step(
                64,
                1,
                SyncOptions::default().lock_timeout_ms(30_000).stop(token),
            );
            tx.send((round, result)).unwrap();
        });
        std::thread::sleep(std::time::Duration::from_millis(50));
        stop.stop();
        let received = rx.recv_timeout(std::time::Duration::from_secs(2));
        drop(lease);
        let (mut round, result) =
            received.expect("cancellation must finish while the lease is held");
        running.join().unwrap();
        assert!(matches!(result, Err(Error::Cancelled(_))));
        assert_eq!(round.step(64, 1, SyncOptions::default()).unwrap().paths, 1);
    }

    #[test]
    fn sqlite_writer_wait_is_reported_only_by_its_own_reconciliation_step() {
        let (_temp, store, project) = fixture();
        write(&project.join("a.jsonl"), "a", "one");
        let mut round = store.reconcile().unwrap().sources([Source::Claude]);
        let writer = crate::open_db(store.db_path()).unwrap();
        writer.execute_batch("BEGIN IMMEDIATE").unwrap();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let started = std::time::Instant::now();
        let running = std::thread::spawn(move || {
            crate::store::busy_wait::observe_next_wait(entered_tx);
            let blocked = round.step(64, 8, SyncOptions::default()).unwrap();
            let next = round.step(64, 8, SyncOptions::default()).unwrap();
            (blocked, next)
        });
        let entered = entered_rx.recv_timeout(std::time::Duration::from_secs(5));
        if entered.is_ok() {
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
        writer.execute_batch("ROLLBACK").unwrap();
        let (blocked, next) = running.join().unwrap();
        assert!(
            entered.is_ok(),
            "the real SDK busy handler must have entered"
        );
        assert_eq!(blocked.paths, 1);
        assert!(blocked.sqlite_busy_wait >= std::time::Duration::from_millis(150));
        assert!(blocked.sqlite_busy_wait <= started.elapsed());
        assert_eq!(next.sqlite_busy_wait, std::time::Duration::ZERO);
        let mut inventory = store.reconcile().unwrap().sources([Source::Claude]);
        let shallow = inventory.discover_step(64, 8, Default::default()).unwrap();
        assert_eq!(shallow.sqlite_busy_wait, std::time::Duration::ZERO);
    }

    #[test]
    fn cancelled_sqlite_busy_wait_releases_sync_lease_before_the_writer_finishes() {
        let (_temp, store, project) = fixture();
        write(&project.join("a.jsonl"), "a", "one");
        let writer = crate::open_db(store.db_path()).unwrap();
        writer.execute_batch("BEGIN IMMEDIATE").unwrap();
        let stop = super::super::StopToken::new();
        let token = stop.clone();
        let other = store.clone();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let running = std::thread::spawn(move || {
            crate::store::busy_wait::observe_next_wait(entered_tx);
            result_tx
                .send(other.sync(SyncOptions::default().sources([Source::Claude]).stop(token)))
                .unwrap();
        });
        let entered = entered_rx.recv_timeout(std::time::Duration::from_secs(5));
        stop.stop();
        let cancelled = result_rx.recv_timeout(std::time::Duration::from_secs(2));
        let lease = crate::ingest::try_acquire_sync_lock(store.db_path()).unwrap();
        let released_before_rollback = lease.is_some();
        drop(lease);
        // Cleanup comes after the observations: releasing this writer must not
        // be what allows cancellation or the sync lease to complete.
        writer.execute_batch("ROLLBACK").unwrap();
        running.join().unwrap();
        assert!(entered.is_ok(), "the real SDK busy handler must have slept");
        assert!(matches!(cancelled, Ok(Err(Error::Cancelled(_)))));
        assert!(
            released_before_rollback,
            "preemption must release the sync lease while the writer still owns SQLite"
        );
        assert!(!store
            .sync(SyncOptions::default().sources([Source::Claude]))
            .unwrap()
            .changed
            .is_empty());
    }

    #[test]
    fn dirty_append_does_not_walk_or_rehash_unrelated_history() {
        let (_temp, store, project) = fixture();
        let active = project.join("active.jsonl");
        write(&active, "active", "first");
        store
            .sync(
                SyncOptions::default()
                    .sources([Source::Claude])
                    .paths([active.clone()]),
            )
            .unwrap();
        let conn = crate::store::open_db(store.db_path()).unwrap();
        // The production-sized catalog is deliberately unrelated to the active
        // transcript. Increasing this count must not increase hot-path work.
        conn.execute_batch("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<4700)             INSERT INTO sessions(source,session_id,discovery_state) SELECT 'claude','archived-'||x,'shallow' FROM n;").unwrap();
        store
            .sync(
                SyncOptions::default()
                    .sources([Source::Claude])
                    .paths([active.clone()]),
            )
            .unwrap();
        super::super::catalog_digest::reset_digest_reads();
        let _ = crate::ingest::take_dirs_read();
        let baseline_ptr = {
            let base = store.sync_baseline.lock().unwrap();
            std::sync::Arc::as_ptr(&base.as_ref().unwrap().catalog.rows)
        };
        write(&active, "active", "appended");
        let report = store
            .sync(
                SyncOptions::default()
                    .sources([Source::Claude])
                    .paths([active.clone()]),
            )
            .unwrap();
        assert!(report
            .changed
            .contains(&super::super::SessionRef::id(Source::Claude, "active")));
        assert_eq!(
            crate::ingest::take_dirs_read(),
            0,
            "an exact append must not list directories"
        );
        assert!(
            super::super::catalog_digest::digest_reads() <= 3,
            "hash only changed sessions"
        );
        let base = store.sync_baseline.lock().unwrap();
        assert_eq!(
            std::sync::Arc::as_ptr(&base.as_ref().unwrap().catalog.rows),
            baseline_ptr,
            "the delta must not copy the 4.7k-entry map"
        );
    }

    #[test]
    fn an_append_reads_delta_and_bounded_validation_windows_of_a_large_transcript() {
        let (_temp, store, project) = fixture();
        let active = project.join("large.jsonl");
        write(&active, "large", "opening");
        let content = "archived text ".repeat(100_000);
        for i in 0..4 {
            let line = serde_json::json!({"sessionId":"large", "uuid":format!("old-{i}"),
                "type":"user", "timestamp":"2026-10-23T10:00:00Z",
                "message":{"role":"user", "content":content}});
            writeln!(
                fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&active)
                    .unwrap(),
                "{line}"
            )
            .unwrap();
        }
        let options = SyncOptions::default()
            .sources([Source::Claude])
            .paths([active.clone()]);
        store.sync(options.clone()).unwrap();
        let original = fs::metadata(&active).unwrap().len();
        assert!(original > 4 * 1024 * 1024);
        crate::ingest::transcript_file::take_read_bytes();
        write(&active, "large", "delta");
        let changed = store.sync(options).unwrap();
        let read = crate::ingest::transcript_file::take_read_bytes();
        assert!(changed.swept);
        eprintln!(
            "large Claude append: read {read} bytes for {original} bytes of captured history"
        );
        assert!(
            read < 2 * 1024 * 1024,
            "append read {read} of {original} historical bytes"
        );
        let conn = crate::store::open_db(store.db_path()).unwrap();
        let events: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_events WHERE session_id='large'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(events, 6, "old and appended evidence remain readable");
    }

    #[test]
    fn codex_scoped_appends_resume_before_global_backfill_is_sealed() {
        let (temp, store, _project) = fixture();
        let root = store.roots().codex.join("sessions/2026/10/23");
        fs::create_dir_all(&root).unwrap();
        let rollout =
            root.join("rollout-2026-10-23T10-00-00-12345678-1234-1234-1234-123456789abc.jsonl");
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&rollout)
            .unwrap();
        writeln!(
            file,
            "{}",
            serde_json::json!({"timestamp":"2026-10-23T10:00:00Z", "type":"session_meta",
            "payload":{"id":"12345678-1234-1234-1234-123456789abc", "cwd":temp.path()}})
        )
        .unwrap();
        let text = "old assistant text ".repeat(80_000);
        for i in 0..4 {
            writeln!(
                file,
                "{}",
                serde_json::json!({"timestamp":format!("2026-10-23T10:00:0{i}Z"),
                "type":"event_msg", "payload":{"type":"agent_message", "message":text}})
            )
            .unwrap();
            writeln!(
                file,
                "{}",
                serde_json::json!({"timestamp":format!("2026-10-23T10:00:0{i}Z"),
                "type":"event_msg", "payload":{"type":"task_complete"}})
            )
            .unwrap();
        }
        drop(file);
        let options = SyncOptions::default()
            .sources([Source::Codex])
            .paths([rollout.clone()]);
        store.sync(options.clone()).unwrap();
        let original = fs::metadata(&rollout).unwrap().len();
        crate::ingest::transcript_file::take_read_bytes();
        writeln!(
            fs::OpenOptions::new().append(true).open(&rollout).unwrap(),
            "{}",
            serde_json::json!({"timestamp":"2026-10-23T11:00:00Z", "type":"event_msg",
                "payload":{"type":"agent_message", "message":"new answer"}})
        )
        .unwrap();
        store.sync(options.clone()).unwrap();
        let read = crate::ingest::transcript_file::take_read_bytes();
        eprintln!("large Codex append: read {read} bytes for {original} bytes of captured history");
        assert!(
            read < 2 * 1024 * 1024,
            "scoped append re-read {read}/{original} historical bytes"
        );
        let conn = crate::store::open_db(store.db_path()).unwrap();
        let events: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_events WHERE source='codex'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(events >= 5, "old and appended replies remain readable");
        // The observer's parser generation, not a global stamp-map seal,
        // controls an exact transcript hydration's one-time repair.
        conn.execute(
            "UPDATE observation_hydration_checkpoints SET parser_version=0 WHERE source='codex'",
            [],
        )
        .unwrap();
        crate::ingest::transcript_file::take_read_bytes();
        store.sync(options.clone()).unwrap();
        assert!(
            crate::ingest::transcript_file::take_read_bytes() >= original,
            "a stale observation parser must re-read old evidence once"
        );
        // A same-size rewrite with restored mtime must invalidate the cursor,
        // even though the path and the stat-only stamp match.
        let modified = fs::metadata(&rollout).unwrap().modified().unwrap();
        let rewritten = fs::read_to_string(&rollout)
            .unwrap()
            .replace("new answer", "new result");
        fs::write(&rollout, rewritten).unwrap();
        fs::OpenOptions::new()
            .write(true)
            .open(&rollout)
            .unwrap()
            .set_times(fs::FileTimes::new().set_modified(modified))
            .unwrap();
        store.sync(options.force(true)).unwrap();
        let found: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM session_events WHERE text='new result')",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(found, "cursor validation must see a same-size rewrite");
    }

    #[test]
    fn a_deleted_known_codex_sibling_does_not_block_a_changed_readable_rollout() {
        let (_temp, store, _project) = fixture();
        let root = store.roots().codex.join("sessions/2026/10/23");
        fs::create_dir_all(&root).unwrap();
        let paths: Vec<_> = (0..2)
            .map(|n| {
                root.join(format!(
                    "rollout-2026-10-23T10-00-00-00000000-0000-0000-0000-00000000000{n}.jsonl"
                ))
            })
            .collect();
        for (n, path) in paths.iter().enumerate() {
            fs::write(path, format!("{}\n{}\n{}\n",
                serde_json::json!({"timestamp":"2026-10-23T10:00:00Z","type":"session_meta","payload":{"id":format!("session-{n}"),"cwd":"/tmp/project"}}),
                serde_json::json!({"timestamp":"2026-10-23T10:00:01Z","type":"event_msg","payload":{"type":"agent_message","message":"old answer"}}),
                serde_json::json!({"timestamp":"2026-10-23T10:00:02Z","type":"event_msg","payload":{"type":"task_complete"}}))).unwrap();
        }
        let options = SyncOptions::default()
            .sources([Source::Codex])
            .paths(paths.clone());
        store.sync(options.clone()).unwrap();
        fs::remove_file(&paths[0]).unwrap();
        writeln!(fs::OpenOptions::new().append(true).open(&paths[1]).unwrap(), "{}",
            serde_json::json!({"timestamp":"2026-10-23T11:00:00Z","type":"event_msg","payload":{"type":"agent_message","message":"readable sibling update"}})).unwrap();
        store.sync(options).unwrap();
        let conn = crate::store::open_db(store.db_path()).unwrap();
        let found: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM session_events WHERE text='readable sibling update')",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(found);
    }

    #[test]
    fn cloned_handles_observe_lock_timeout_and_stop_while_another_sync_runs() {
        use std::sync::{
            atomic::{AtomicBool, Ordering},
            mpsc, Arc, Barrier,
        };
        use std::time::Duration;
        for exact in [false, true] {
            let (_temp, store, project) = fixture();
            let primary = project.join("active.jsonl");
            write(&primary, "active", "first");
            let child = project.join("agent-child.jsonl");
            let child_line = |id: &str| {
                serde_json::json!({"sessionId":"active", "agentId":"child", "isSidechain":true,
                "uuid":id,"type":"assistant","message":{"role":"assistant","content":id}, "timestamp":"2026-10-23T10:00:00Z"}).to_string()+"\n"
            };
            fs::write(&child, child_line("first child")).unwrap();
            let options = if exact {
                SyncOptions::default()
                    .sources([Source::Claude])
                    .paths([primary])
            } else {
                SyncOptions::default().sources([Source::Claude])
            };
            let barrier = Arc::new(Barrier::new(2));
            let blocked = Arc::new(AtomicBool::new(false));
            let (started, ready) = mpsc::channel();
            let first = store.clone();
            let release = barrier.clone();
            let observer = super::super::ProgressObserver::new(move |progress| {
                if progress.source == "claude" && !blocked.swap(true, Ordering::Relaxed) {
                    started.send(()).unwrap();
                    release.wait();
                }
            });
            let first_options = options.clone().progress(observer);
            let running = std::thread::spawn(move || first.sync(first_options));
            ready.recv_timeout(Duration::from_secs(30)).unwrap();
            let other = store.clone();
            let (sent, result) = mpsc::channel();
            let zero_options = options.clone();
            let zero = std::thread::spawn(move || sent.send(other.sync(zero_options)).unwrap());
            let zero_result = result.recv_timeout(Duration::from_millis(500));
            let other = store.clone();
            let (sent, sidecar_result) = mpsc::channel();
            let sidecar = child.clone();
            let sidecar_wait = std::thread::spawn(move || {
                sent.send(
                    other.sync(
                        SyncOptions::default()
                            .sources([Source::Claude])
                            .paths([sidecar]),
                    ),
                )
                .unwrap()
            });
            let sidecar_locked = sidecar_result.recv_timeout(Duration::from_millis(500));
            let token = super::super::StopToken::new();
            let stop = token.clone();
            let other = store.clone();
            let (sent, result) = mpsc::channel();
            let waiting = std::thread::spawn(move || {
                sent.send(other.sync(options.lock_timeout_ms(30_000).stop(stop)))
                    .unwrap()
            });
            std::thread::sleep(Duration::from_millis(100));
            token.stop();
            let cancelled = result.recv_timeout(Duration::from_millis(500));
            barrier.wait();
            running.join().unwrap().unwrap();
            zero.join().unwrap();
            waiting.join().unwrap();
            sidecar_wait.join().unwrap();
            assert!(
                matches!(zero_result, Ok(Err(Error::SyncLocked { .. }))),
                "exact={exact}: zero-timeout sync must not wait on a cache mutex"
            );
            assert!(
                matches!(sidecar_locked, Ok(Err(Error::SyncLocked { .. }))),
                "sidecar fallback preserves the same lock contract"
            );
            assert!(
                matches!(cancelled, Ok(Err(Error::Cancelled(_)))),
                "stop must interrupt lock waiting"
            );
            write!(
                fs::OpenOptions::new().append(true).open(&child).unwrap(),
                "{}",
                child_line("child after release")
            )
            .unwrap();
            store
                .sync(
                    SyncOptions::default()
                        .sources([Source::Claude])
                        .paths([child]),
                )
                .unwrap();
            let conn = crate::store::open_db(store.db_path()).unwrap();
            let found: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM session_events WHERE text='child after release')",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert!(
                found,
                "a contended sidecar remains capturable after release"
            );
        }
    }

    #[test]
    fn unchanged_cold_round_reuses_current_stat_proofs_without_body_reads() {
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                crate::ingest::transcript_cursor::set_settle_window_for_test(None);
            }
        }
        let _reset = Reset;
        crate::ingest::transcript_cursor::set_settle_window_for_test(Some(0));
        let (_temp, store, project) = fixture();
        let path = project.join("cold.jsonl");
        write(&path, "cold", "opening");
        let line = serde_json::json!({"sessionId":"cold", "uuid":"old", "type":"user",
            "timestamp":"2026-10-23T10:00:00Z", "message":{"role":"user","content":"old content ".repeat(200_000)}});
        writeln!(
            fs::OpenOptions::new().append(true).open(&path).unwrap(),
            "{line}"
        )
        .unwrap();
        let run = || {
            let mut round = store.reconcile().unwrap().sources([Source::Claude]);
            for _ in 0..100 {
                if round.step(64, 8, SyncOptions::default()).unwrap().complete {
                    return;
                }
            }
            panic!("finite cold round did not finish");
        };
        run();
        crate::ingest::transcript_file::take_read_bytes();
        crate::ingest::state_work::take();
        run();
        let read = crate::ingest::transcript_file::take_read_bytes();
        let metadata = fs::metadata(&path).unwrap();
        if crate::ingest::transcript_cursor::filesystem_keeps_change_time(&path, &metadata) {
            assert_eq!(
                read, 0,
                "an unchanged cold round must not force content validation"
            );
        } else {
            assert!(
                read <= 512 * 1024,
                "untrusted filesystems retain bounded digest validation: {read}"
            );
        }
        assert_eq!(crate::ingest::state_work::take(), (0, 0));
    }

    #[test]
    fn claude_sidecars_without_standalone_metadata_are_captured_by_event_and_cold_round() {
        let (_temp, store, project) = fixture();
        let parent = project.join("parent.jsonl");
        write(&parent, "parent", "first");
        let sidecars = project.join("parent/subagents");
        fs::create_dir_all(&sidecars).unwrap();
        let child = sidecars.join("agent-child.jsonl");
        let child_line = |uuid: &str, text: &str| {
            serde_json::json!({"sessionId":"parent", "agentId":"child",
            "isSidechain":true,"uuid":uuid,"type":"assistant", "timestamp":"2026-10-23T10:00:01Z",
            "message":{"role":"assistant","content":text}})
            .to_string()
                + "\n"
        };
        fs::write(&child, child_line("child-1", "child first")).unwrap();
        let paths = |path: PathBuf| {
            SyncOptions::default()
                .sources([Source::Claude])
                .paths([path])
                .force(true)
        };
        store.sync(paths(parent.clone())).unwrap();
        let conn = crate::store::open_db(store.db_path()).unwrap();
        let count = |text: &str| {
            conn.query_row(
                "SELECT COUNT(*) FROM session_events WHERE text=?1",
                [text],
                |r| r.get::<_, i64>(0),
            )
            .unwrap()
        };
        assert_eq!(
            count("child first"),
            0,
            "parent hot work must not enumerate unrelated children"
        );
        write(&parent, "parent", "append");
        store.sync(paths(parent)).unwrap();
        store.sync(paths(child.clone())).unwrap();
        assert_eq!(
            count("child first"),
            1,
            "the sidecar's own dirty path must retain fallback acquisition"
        );
        write!(
            fs::OpenOptions::new().append(true).open(&child).unwrap(),
            "{}",
            child_line("child-2", "child second")
        )
        .unwrap();
        let mut cold = store.reconcile().unwrap().sources([Source::Claude]);
        for _ in 0..100 {
            if cold.step(64, 8, SyncOptions::default()).unwrap().complete {
                break;
            }
        }
        assert_eq!(count("child first"), 1);
        assert_eq!(count("child second"), 1);
        let links: i64 = conn.query_row("SELECT COUNT(*) FROM session_relationships WHERE source='claude' AND parent_session_id='parent'", [], |r|r.get(0)).unwrap();
        assert!(
            links > 0,
            "the delegated family relationship remains observable"
        );
    }

    #[test]
    fn codex_delegated_rollout_is_captured_by_its_own_path_and_cold_reconciliation() {
        let (_temp, store, _project) = fixture();
        let root = store.roots().codex.join("sessions/2026/10/23");
        fs::create_dir_all(&root).unwrap();
        let parent = root.join("rollout-parent.jsonl");
        let child = root.join("rollout-child.jsonl");
        let meta = |id: &str, source: &str| {
            serde_json::json!({"timestamp":"2026-10-23T10:00:00Z", "type":"session_meta",
            "payload":{"id":id, "session_id":"root", "cwd":"/tmp/project", "thread_source":source}})
            .to_string()
                + "\n"
        };
        let message = |text: &str| {
            serde_json::json!({"timestamp":"2026-10-23T10:00:01Z", "type":"event_msg",
            "payload":{"type":"agent_message", "message":text}})
            .to_string()
                + "\n"
        };
        fs::write(&parent, meta("root", "cli") + &message("parent first")).unwrap();
        fs::write(
            &child,
            meta("child", "subagent") + &message("delegated first"),
        )
        .unwrap();
        let options = |path: PathBuf| {
            SyncOptions::default()
                .sources([Source::Codex])
                .paths([path])
                .force(true)
        };
        store.sync(options(parent.clone())).unwrap();
        let conn = crate::store::open_db(store.db_path()).unwrap();
        let count = |text: &str| {
            conn.query_row(
                "SELECT COUNT(*) FROM session_events WHERE text=?1",
                [text],
                |r| r.get::<_, i64>(0),
            )
            .unwrap()
        };
        assert_eq!(count("delegated first"), 0);
        store.sync(options(child.clone())).unwrap();
        assert_eq!(count("delegated first"), 1);
        write!(
            fs::OpenOptions::new().append(true).open(&child).unwrap(),
            "{}",
            message("delegated second")
        )
        .unwrap();
        let mut cold = store.reconcile().unwrap().sources([Source::Codex]);
        for _ in 0..100 {
            if cold.step(64, 8, SyncOptions::default()).unwrap().complete {
                break;
            }
        }
        assert_eq!(count("delegated first"), 1);
        assert_eq!(count("delegated second"), 1);
    }

    #[test]
    fn exact_append_never_loads_or_rewrites_47k_unrelated_global_state_entries() {
        let (_temp, store, project) = fixture();
        let active = project.join("active.jsonl");
        write(&active, "active", "first");
        let options = SyncOptions::default()
            .sources([Source::Claude])
            .paths([active.clone()]);
        store.sync(options.clone()).unwrap();
        let archived: serde_json::Map<String, serde_json::Value> = (0..47_000)
            .map(|i| {
                (
                    format!("/archived/rollout-{i}.jsonl"),
                    serde_json::json!({"stamp":"unchanged", "session":format!("old-{i}")}),
                )
            })
            .collect();
        let state = serde_json::to_vec(&serde_json::json!({"codex_rollouts_v7":archived})).unwrap();
        let state_path = store.db_path().parent().unwrap().join(".sync-state.json");
        fs::write(&state_path, &state).unwrap();
        crate::ingest::state_work::take();
        write(&active, "active", "delta");
        store.sync(options).unwrap();
        assert_eq!(
            crate::ingest::state_work::take(),
            (0, 0),
            "the active transcript must bypass the global state blob"
        );
        assert_eq!(fs::read(&state_path).unwrap(), state);
        let conn = crate::store::open_db(store.db_path()).unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_events WHERE session_id='active'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn an_explicit_empty_path_batch_performs_no_provider_io() {
        let (_temp, store, project) = fixture();
        write(&project.join("private.jsonl"), "private", "first");
        crate::ingest::take_dirs_read();
        crate::ingest::transcript_file::take_read_bytes();
        assert!(!store.sync(SyncOptions::default().paths([])).unwrap().swept);
        let options = super::super::DiscoveryOptions {
            paths: Some(Vec::new()),
            ..Default::default()
        };
        assert_eq!(store.discover(options).unwrap().discovered, 0);
        assert_eq!(crate::ingest::take_dirs_read(), 0);
        assert_eq!(crate::ingest::transcript_file::take_read_bytes(), 0);
        assert_eq!(store.sessions(Default::default()).count(), 0);
        assert!(!store
            .db_path()
            .parent()
            .unwrap()
            .join(".sync-state.json")
            .exists());
    }

    #[test]
    fn enumeration_resumes_and_each_slice_is_bounded() {
        let (_temp, store, project) = fixture();
        for i in 0..130 {
            write(
                &project.join(format!("s{i}.jsonl")),
                &format!("s{i}"),
                "first",
            );
        }
        let mut round = store.reconcile().unwrap();
        let mut paths = 0;
        let mut complete = false;
        for _ in 0..1000 {
            let step = round.step(7, 3, SyncOptions::default()).unwrap();
            assert!(step.entries <= 7);
            assert!(step.paths <= 3);
            paths += step.paths;
            if step.complete {
                complete = true;
                break;
            }
        }
        assert!(
            complete,
            "a finite archive must finish, not revisit a prefix"
        );
        assert!(paths >= 130);
        assert_eq!(store.sessions(Default::default()).count(), 130);
    }

    #[test]
    fn shallow_reconciliation_never_reads_unselected_evidence() {
        let (_temp, store, project) = fixture();
        for i in 0..20 {
            write(
                &project.join(format!("s{i}.jsonl")),
                &format!("s{i}"),
                "first",
            );
        }
        let mut round = store.reconcile().unwrap();
        for _ in 0..1000 {
            let step = round.discover_step(7, 3, Default::default()).unwrap();
            assert!(step.entries <= 7 && step.paths <= 3);
            if step.complete {
                break;
            }
        }
        assert_eq!(store.sessions(Default::default()).count(), 20);
        let conn = crate::store::open_db(store.db_path()).unwrap();
        let events: i64 = conn
            .query_row("SELECT COUNT(*) FROM session_events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(events, 0, "inventory cannot hydrate unselected history");
    }

    #[test]
    fn cancellation_keeps_exact_paths_pending_and_restart_completes() {
        let (_temp, store, project) = fixture();
        write(&project.join("active.jsonl"), "active", "first");
        let mut round = store.reconcile().unwrap();
        let token = super::super::StopToken::new();
        token.stop();
        let options = SyncOptions::default().stop(token);
        let mut yielded = false;
        for _ in 0..100 {
            if matches!(round.step(7, 3, options.clone()), Err(Error::Cancelled(_))) {
                yielded = true;
                break;
            }
        }
        assert!(yielded);
        assert!(round.sources.iter().any(|walk| !walk.pending.is_empty()));
        for _ in 0..1000 {
            if round.step(7, 3, SyncOptions::default()).unwrap().complete {
                break;
            }
        }
        assert_eq!(store.sessions(Default::default()).count(), 1);
        // A fresh process/round reuses durable transcript checkpoints and can
        // find new roots/files rather than trusting the previous enumeration.
        write(&project.join("new.jsonl"), "new", "first");
        let mut restarted = store.reconcile().unwrap();
        for _ in 0..1000 {
            if restarted
                .step(7, 3, SyncOptions::default())
                .unwrap()
                .complete
            {
                break;
            }
        }
        assert_eq!(store.sessions(Default::default()).count(), 2);
    }
    #[test]
    fn a_same_stat_identity_replacement_is_rediscovered_without_global_state() {
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                crate::ingest::transcript_cursor::set_settle_window_for_test(None);
            }
        }
        let _reset = Reset;
        crate::ingest::transcript_cursor::set_settle_window_for_test(Some(0));
        let (_temp, store, project) = fixture();
        let path = project.join("same.jsonl");
        write(&path, "first", "first");
        let options = SyncOptions::default()
            .sources([Source::Claude])
            .paths([path.clone()]);
        store.sync(options.clone()).unwrap();
        let mtime = fs::metadata(&path).unwrap().modified().unwrap();
        let old = fs::read_to_string(&path).unwrap();
        fs::write(&path, old.replace("first", "other")).unwrap();
        fs::File::open(&path).unwrap().set_modified(mtime).unwrap();
        crate::ingest::state_work::take();
        store.sync(options.clone()).unwrap();
        crate::ingest::transcript_file::take_read_bytes();
        let before = store.head_revision().unwrap();
        store.sync(options).unwrap();
        assert_eq!(
            store.head_revision().unwrap(),
            before,
            "unchanged repaired identity must not advance the feed"
        );
        let read = crate::ingest::transcript_file::take_read_bytes();
        let metadata = crate::ingest::transcript_file::metadata(&path).unwrap();
        if crate::ingest::transcript_cursor::filesystem_keeps_change_time(&path, &metadata) {
            assert_eq!(
                read, 0,
                "a repaired identity must stay repaired on the next settled pass"
            );
        } else {
            assert!(
                read < 1024 * 1024,
                "unknown filesystems retain bounded validation, read {read}"
            );
        }
        assert_eq!(crate::ingest::state_work::take(), (0, 0));
        let conn = crate::store::open_db(store.db_path()).unwrap();
        assert_eq!(conn.query_row("SELECT COUNT(*) FROM session_observations WHERE source='claude' AND access_state='available' AND raw_locator=?1",[path.to_string_lossy()],|row|row.get::<_,i64>(0)).unwrap(),1);
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM session_events WHERE session_id='other'",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
            1
        );
    }

    #[test]
    fn exact_primary_capture_resolves_explicit_and_late_parent_continuity() {
        let (_temp, store, project) = fixture();
        let parent = project.join("parent.jsonl");
        let child = project.join("child.jsonl");
        let line =
            |id: &str, uuid: &str, parent: Option<&str>| {
                serde_json::json!({
            "sessionId":id,"uuid":uuid,"parentUuid":parent,"type":"user",
            "timestamp":"2026-10-23T10:00:00Z","message":{"role":"user","content":"synthetic"}
        }).to_string()+"\n"
            };
        fs::write(&child, line("child", "child-record", Some("parent-record"))).unwrap();
        let sync = |path| {
            store
                .sync(
                    SyncOptions::default()
                        .sources([Source::Claude])
                        .paths([path]),
                )
                .unwrap()
        };
        sync(child);
        let conn = crate::store::open_db(store.db_path()).unwrap();
        let edges = || {
            conn.query_row("SELECT COUNT(*) FROM session_relationships WHERE source='claude' AND parent_session_id='parent' AND child_session_id='child' AND relationship='continuation'",[],|row|row.get::<_,i64>(0)).unwrap()
        };
        assert_eq!(edges(), 0);
        fs::write(&parent, line("parent", "parent-record", None)).unwrap();
        sync(parent);
        assert_eq!(
            edges(),
            1,
            "parent delta resolves an already pending child immediately"
        );
        let codex = store.roots().codex.join("sessions/2026/10/23/rollout-2026-10-23T10-00-00-12345678-1234-1234-1234-123456789abc.jsonl");
        fs::create_dir_all(codex.parent().unwrap()).unwrap();
        fs::write(&codex,serde_json::json!({"type":"session_meta","timestamp":"2026-10-23T10:00:00Z","payload":{"id":"12345678-1234-1234-1234-123456789abc","cwd":"/tmp/p","forked_from_id":"origin"}}).to_string()+"\n").unwrap();
        store
            .sync(
                SyncOptions::default()
                    .sources([Source::Codex])
                    .paths([codex]),
            )
            .unwrap();
        assert_eq!(conn.query_row("SELECT COUNT(*) FROM session_relationships WHERE source='codex' AND parent_session_id='origin' AND relationship='fork'",[],|row|row.get::<_,i64>(0)).unwrap(),1);
    }
    #[test]
    fn a_stale_claude_metadata_scan_cannot_be_settled_or_skipped() {
        let (_temp, store, project) = fixture();
        let path = project.join("scan.jsonl");
        write(&path, "scan", "record");
        let options = SyncOptions::default()
            .sources([Source::Claude])
            .paths([path.clone()]);
        store.sync(options.clone()).unwrap();
        let conn = crate::open_db(store.db_path()).unwrap();
        use crate::ingest::transcript_cursor::{load_cursor, store_cursor, CursorKey};
        let key = CursorKey::Session {
            source: "claude",
            session_id: "scan",
            location: "local",
        };
        let mut cursor = load_cursor(&conn, &key).unwrap();
        cursor.claude.as_mut().unwrap().scan = None;
        store_cursor(&conn, &key, &cursor).unwrap();
        crate::ingest::transcript_file::take_read_bytes();
        store.sync(options).unwrap();
        assert!(load_cursor(&conn, &key)
            .unwrap()
            .claude
            .unwrap()
            .scan
            .is_some());
    }
    #[test]
    fn the_write_only_continuity_index_does_not_block_older_read_only_stores() {
        let (_temp, store, _project) = fixture();
        let db = store.db_path().to_owned();
        crate::open_db(&db)
            .unwrap()
            .execute_batch("DROP INDEX idx_session_continuity_origin")
            .unwrap();
        assert!(SessionStore::open(
            super::super::StoreOptions::default()
                .db_path(db)
                .read_only(true)
        )
        .is_ok());
    }
    #[test]
    fn cancelled_round_finalization_keeps_recovery_pending_and_retry_finishes() {
        let (_temp, store, _project) = fixture();
        let conn = crate::open_db(store.db_path()).unwrap();
        for n in 0..100 {
            crate::continuity::record_evidence(
                &conn,
                &crate::continuity::ContinuityEvidence {
                    source: "claude".into(),
                    locator: format!("pending-{n}"),
                    session_id: format!("pending-{n}"),
                    ..Default::default()
                },
            )
            .unwrap();
        }
        let mut round = store.reconcile().unwrap().sources([Source::Claude]);
        round.sources[0].roots.clear();
        let checks = std::rc::Rc::new(std::cell::Cell::new(0));
        let stop_checks = checks.clone();
        let result = crate::ingest::with_capture_stop(
            move || {
                let n = stop_checks.get() + 1;
                stop_checks.set(n);
                n >= 5
            },
            || {
                round
                    .step(8, 8, SyncOptions::default())
                    .map_err(anyhow::Error::from)
            },
        );
        assert!(result.is_err());
        assert!(
            !round.sources.is_empty(),
            "cancelled finalization must stay retryable"
        );
        let remaining: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_continuity_evidence WHERE pending_reason IS NOT NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            remaining > 0 && remaining < 100,
            "some row-boundary progress survives: {remaining}"
        );
        assert!(round.step(8, 8, SyncOptions::default()).unwrap().complete);
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM session_continuity_evidence WHERE pending_reason IS NOT NULL",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            0
        );
    }
    #[test]
    fn failed_attempt_commits_are_reported_on_retry_without_claiming_other_writers() {
        let (_temp, store, project) = fixture();
        let a = project.join("a.jsonl");
        let b = project.join("b.jsonl");
        write(&a, "a", "one");
        write(&b, "b", "one");
        let token = super::super::StopToken::new();
        let observer_stop = token.clone();
        let mut options = SyncOptions::default()
            .sources([Source::Claude])
            .paths([a, b]);
        options.stop = Some(token);
        options.progress = Some(super::super::ProgressObserver::new(move |progress| {
            if progress.processed_files >= 1 {
                observer_stop.stop();
            }
        }));
        assert!(matches!(
            store.sync(options.clone()),
            Err(Error::Cancelled(_))
        ));
        let external = project.join("external.jsonl");
        write(&external, "external", "one");
        let other = SessionStore::open(
            super::super::StoreOptions::default()
                .db_path(store.db_path())
                .roots(store.roots().clone()),
        )
        .unwrap();
        other
            .sync(
                SyncOptions::default()
                    .sources([Source::Claude])
                    .paths([external]),
            )
            .unwrap();
        options.stop = None;
        options.progress = None;
        let retry = store.sync(options.clone()).unwrap();
        assert!(
            retry
                .changed
                .contains(&super::super::SessionRef::id(Source::Claude, "a")),
            "a committed before cancellation must be reported: {:?}",
            retry.changed
        );
        assert!(!retry
            .changed
            .contains(&super::super::SessionRef::id(Source::Claude, "external")));
        assert!(
            store.sync(options).unwrap().changed.is_empty(),
            "failed-attempt deltas are reported once"
        );
    }

    #[test]
    fn capped_discovery_cannot_consume_unexamined_reconciliation_paths() {
        let (_temp, store, project) = fixture();
        for id in ["a", "b", "c"] {
            write(&project.join(format!("{id}.jsonl")), id, "one");
        }
        let mut round = store.reconcile().unwrap().sources([Source::Claude]);
        let error = round
            .discover_step(64, 3, super::super::DiscoveryOptions::default().limit(1))
            .unwrap_err();
        assert!(matches!(error, Error::InvalidArgument(_)));
        assert_eq!(store.sessions(Default::default()).count(), 0);
        for _ in 0..100 {
            if round
                .discover_step(64, 3, Default::default())
                .unwrap()
                .complete
            {
                break;
            }
        }
        assert_eq!(store.sessions(Default::default()).count(), 3);
    }
    #[test]
    fn an_unacknowledged_successful_bracket_stays_pending_until_a_public_report() {
        let (_temp, store, project) = fixture();
        let path = project.join("a.jsonl");
        write(&path, "a", "one");
        let scope = crate::ingest::SweepScope::only(["claude"])
            .with_paths(crate::ingest::path_scope::PathScope::from_paths([path]));
        store.sync_tick(false, 0, &scope).unwrap();
        let report = store
            .sync(SyncOptions::default().sources([Source::Claude]))
            .unwrap();
        assert!(report
            .changed
            .contains(&super::super::SessionRef::id(Source::Claude, "a")));
        assert!(store
            .sync(SyncOptions::default().sources([Source::Claude]))
            .unwrap()
            .changed
            .is_empty());
    }
    #[test]
    fn failed_entry_metadata_retains_the_same_directory_entry_until_retry() {
        let (_temp, store, project) = fixture();
        for id in ["a", "b", "c"] {
            write(&project.join(format!("{id}.jsonl")), id, "one");
        }
        let mut round = store.reconcile().unwrap().sources([Source::Claude]);
        FAIL_ENTRY_TYPE.with(|fail| fail.set(true));
        assert!(round.step(64, 3, SyncOptions::default()).is_err());
        assert!(round
            .sources
            .iter()
            .any(|source| !source.deferred.is_empty()));
        for _ in 0..100 {
            if round.step(64, 3, SyncOptions::default()).unwrap().complete {
                break;
            }
        }
        assert_eq!(store.sessions(Default::default()).count(), 3);
    }

    #[test]
    fn paths_without_source_hints_only_discover_their_owning_provider() {
        let (_temp, store, project) = fixture();
        let file = project.join("owned.jsonl");
        write(&file, "owned", "one");
        fs::write(&store.roots().opencode_db, b"not a SQLite store").unwrap();
        fs::create_dir_all(&store.roots().devin).unwrap();
        fs::write(
            store.roots().devin.join("sessions.db"),
            b"not a SQLite store",
        )
        .unwrap();
        let options = super::super::DiscoveryOptions {
            paths: Some(vec![file.clone()]),
            ..Default::default()
        };
        let report = store.discover(options).unwrap();
        assert_eq!(report.discovered, 1);
        assert!(
            report.diagnostics.is_empty(),
            "unrelated broken stores must not be consulted: {:?}",
            report.diagnostics
        );
        assert_eq!(
            super::super::path_scope::selected_sources(
                store.roots(),
                Some(std::slice::from_ref(&file)),
                None
            ),
            Some(vec![Source::Claude])
        );
        store.sync(SyncOptions::default().paths([file])).unwrap();
    }

    #[test]
    fn deleted_failed_attempt_keys_are_not_reported_by_an_unrelated_retry() {
        let (_temp, store, project) = fixture();
        let a = project.join("a.jsonl");
        let b = project.join("b.jsonl");
        write(&a, "a", "one");
        write(&b, "b", "one");
        let stop = super::super::StopToken::new();
        let observer_stop = stop.clone();
        let mut options = SyncOptions::default()
            .sources([Source::Claude])
            .paths([a.clone(), b.clone()]);
        options.stop = Some(stop);
        options.progress = Some(super::super::ProgressObserver::new(move |progress| {
            if progress.processed_files >= 1 {
                observer_stop.stop();
            }
        }));
        assert!(store.sync(options).is_err());
        fs::remove_file(a).unwrap();
        crate::open_db(store.db_path())
            .unwrap()
            .execute(
                "DELETE FROM sessions WHERE source='claude' AND session_id='a'",
                [],
            )
            .unwrap();
        let report = store
            .sync(SyncOptions::default().sources([Source::Claude]).paths([b]))
            .unwrap();
        assert!(
            !report
                .changed
                .contains(&super::super::SessionRef::id(Source::Claude, "a")),
            "deleted external keys must leave the retained delta"
        );
    }

    #[test]
    fn late_cancellation_retains_current_and_previously_failed_catalog_deltas() {
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                super::super::sync::STOP_AFTER_BRACKET.with(|token| token.take());
            }
        }
        let _reset = Reset;
        let (_temp, store, project) = fixture();
        let a = project.join("a.jsonl");
        let b = project.join("b.jsonl");
        write(&a, "a", "one");
        write(&b, "b", "one");
        let stop = super::super::StopToken::new();
        let observer_stop = stop.clone();
        let mut options = SyncOptions::default()
            .sources([Source::Claude])
            .paths([a, b]);
        options.stop = Some(stop);
        options.progress = Some(super::super::ProgressObserver::new(move |progress| {
            if progress.processed_files >= 1 {
                observer_stop.stop();
            }
        }));
        assert!(store.sync(options.clone()).is_err());
        let late = super::super::StopToken::new();
        options.stop = Some(late.clone());
        options.progress = None;
        super::super::sync::STOP_AFTER_BRACKET.with(|token| *token.borrow_mut() = Some(late));
        assert!(matches!(
            store.sync(options.clone()),
            Err(Error::Cancelled(_))
        ));
        options.stop = None;
        let report = store.sync(options.clone()).unwrap();
        for id in ["a", "b"] {
            assert!(
                report
                    .changed
                    .contains(&super::super::SessionRef::id(Source::Claude, id)),
                "late cancellation must preserve {id}"
            );
        }
        assert!(store.sync(options).unwrap().changed.is_empty());
    }
    #[test]
    fn externally_replaced_failed_delta_is_not_attributed_to_an_unrelated_retry() {
        let (_temp, store, project) = fixture();
        let a = project.join("a.jsonl");
        let b = project.join("b.jsonl");
        write(&a, "a", "one");
        write(&b, "b", "one");
        let stop = super::super::StopToken::new();
        let observer_stop = stop.clone();
        let mut options = SyncOptions::default()
            .sources([Source::Claude])
            .paths([a, b.clone()]);
        options.stop = Some(stop);
        options.progress = Some(super::super::ProgressObserver::new(move |progress| {
            if progress.processed_files >= 1 {
                observer_stop.stop();
            }
        }));
        assert!(store.sync(options).is_err());
        crate::open_db(store.db_path()).unwrap().execute("UPDATE sessions SET title='outside-writer' WHERE source='claude' AND session_id='a'",[]).unwrap();
        let report = store
            .sync(SyncOptions::default().sources([Source::Claude]).paths([b]))
            .unwrap();
        assert!(!report
            .changed
            .contains(&super::super::SessionRef::id(Source::Claude, "a")));
    }
    #[test]
    fn publication_holds_the_existing_sync_lock_and_cannot_duplicate_pending_changes() {
        let (_temp, store, project) = fixture();
        let path = project.join("a.jsonl");
        write(&path, "a", "one");
        let options = SyncOptions::default()
            .sources([Source::Claude])
            .paths([path]);
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let first_store = store.clone();
        let first_options = options.clone();
        let first = std::thread::spawn(move || {
            super::super::sync::BEFORE_REPORT.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    ready_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                }))
            });
            first_store.sync(first_options)
        });
        ready_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let start = std::time::Instant::now();
        let mut second_options = options.clone();
        second_options.lock_timeout_ms = 0;
        let second = store.sync(second_options);
        let zero_elapsed = start.elapsed();
        let token = super::super::StopToken::new();
        let cancelled_options = options.clone().lock_timeout_ms(30_000).stop(token.clone());
        let waiting_store = store.clone();
        let (cancel_tx, cancel_rx) = std::sync::mpsc::channel();
        let waiting = std::thread::spawn(move || {
            cancel_tx
                .send(waiting_store.sync(cancelled_options))
                .unwrap();
        });
        std::thread::sleep(std::time::Duration::from_millis(100));
        token.stop();
        let cancelled = cancel_rx.recv_timeout(std::time::Duration::from_millis(500));
        release_tx.send(()).unwrap();
        waiting.join().unwrap();
        let report = first.join().unwrap().unwrap();
        assert!(
            matches!(second, Err(Error::SyncLocked { .. })),
            "publication remains in the same cancellable sync-lock bracket"
        );
        assert!(zero_elapsed < std::time::Duration::from_secs(1));
        assert!(matches!(cancelled, Ok(Err(Error::Cancelled(_)))));
        assert_eq!(
            report.changed,
            vec![super::super::SessionRef::id(Source::Claude, "a")]
        );
        assert!(store.sync(options).unwrap().changed.is_empty());
    }

    #[test]
    fn legacy_opencode_paths_without_source_hints_discover_and_capture_evidence() {
        let (_temp, store, _project) = fixture();
        let storage = &store.roots().opencode_storage_dir;
        let session = storage.join("session/global/ses_tree.json");
        let message = storage.join("message/ses_tree/msg_tree.json");
        let part = storage.join("part/msg_tree/part_tree.json");
        for path in [&session, &message, &part] {
            fs::create_dir_all(path.parent().unwrap()).unwrap();
        }
        fs::write(
            &session,
            r#"{"id":"ses_tree","directory":"/work/tree","time":{"created":10,"updated":20}}"#,
        )
        .unwrap();
        fs::write(
            &message,
            r#"{"id":"msg_tree","sessionID":"ses_tree","role":"user","time":{"created":10}}"#,
        )
        .unwrap();
        fs::write(&part, r#"{"id":"part_tree","sessionID":"ses_tree","messageID":"msg_tree","type":"text","text":"tree prompt"}"#).unwrap();
        let discovery = store
            .discover(super::super::DiscoveryOptions {
                paths: Some(vec![session.clone()]),
                ..Default::default()
            })
            .unwrap();
        assert!(discovery.discovered + discovery.skipped_unchanged > 0);
        store.sync(SyncOptions::default().paths([session])).unwrap();
        let captured: bool = crate::open_db(store.db_path()).unwrap().query_row(
            "SELECT EXISTS(SELECT 1 FROM session_events WHERE source='opencode' AND session_id='ses_tree' AND text='tree prompt')",
            [], |row| row.get(0),
        ).unwrap();
        assert!(
            captured,
            "a legacy locator must retain discovery and acquisition"
        );
    }

    #[test]
    fn deleted_hermes_profile_path_without_source_hints_retires_its_sessions() {
        let (_temp, store, _project) = fixture();
        let profile = store.roots().home.join(".hermes/profiles/work/state.db");
        fs::create_dir_all(profile.parent().unwrap()).unwrap();
        let db = rusqlite::Connection::open(&profile).unwrap();
        db.execute_batch(include_str!("../../tests/fixtures/hermes/schema31.sql"))
            .unwrap();
        drop(db);
        store
            .sync(SyncOptions::default().sources([Source::Hermes]))
            .unwrap();
        let conn = crate::open_db(store.db_path()).unwrap();
        let count = || {
            conn.query_row(
                "SELECT COUNT(*) FROM sessions WHERE source='hermes'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
        };
        assert!(count() > 0);
        fs::remove_dir_all(profile.parent().unwrap()).unwrap();
        store.sync(SyncOptions::default().paths([profile])).unwrap();
        assert_eq!(
            count(),
            0,
            "a deleted dynamic root must still reach provider retirement"
        );
    }

    #[test]
    fn unknown_legacy_and_deleted_profile_paths_preserve_unrestricted_source_fallback() {
        let (_temp, store, _project) = fixture();
        let legacy = store
            .roots()
            .home
            .join(".local/share/opencode/storage/session/project/ses_legacy.json");
        let deleted_profile = store
            .roots()
            .home
            .join("unregistered-provider/profile-deleted/sessions.db");
        assert_eq!(
            super::super::path_scope::selected_sources(
                store.roots(),
                Some(std::slice::from_ref(&deleted_profile)),
                None
            ),
            None
        );
        let inferred = super::super::path_scope::selected_sources(
            store.roots(),
            Some(std::slice::from_ref(&legacy)),
            None,
        );
        assert!(
            inferred.is_none()
                || inferred
                    .as_ref()
                    .is_some_and(|sources| sources.contains(&Source::OpenCode)),
            "a legacy locator cannot become an empty allowlist"
        );
        assert_eq!(
            super::super::path_scope::selected_sources(
                store.roots(),
                Some(&[legacy, deleted_profile]),
                Some(vec![Source::OpenCode])
            ),
            Some(vec![Source::OpenCode])
        );
    }
}
