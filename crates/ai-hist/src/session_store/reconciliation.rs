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
}

/// Content-free work accounting for a bounded reconciliation slice.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct ReconciliationStep {
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
    queued: VecDeque<PathBuf>,
    current: Option<OpenDirectory>,
    retry_entry: Option<(std::fs::DirEntry, WatchedPath)>,
    root: Option<WatchedPath>,
    visited: HashSet<PathBuf>,
    pending: Vec<PathBuf>,
    finalized: bool,
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

fn entry_type(entry: &std::fs::DirEntry) -> std::io::Result<std::fs::FileType> {
    #[cfg(test)]
    if FAIL_ENTRY_TYPE.with(|fail| fail.replace(false)) {
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
                    retry_entry: None,
                    root: None,
                    visited: HashSet::new(),
                    pending: Vec::new(),
                    finalized: false,
                })
            })
            .collect();
        Ok(Reconciliation {
            store: self.clone(),
            sources,
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
        self
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
        let Some(mut source) = self.sources.pop_front() else {
            return Ok(ReconciliationStep {
                complete: true,
                ..Default::default()
            });
        };
        let result = self
            .take_step(
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
            });
        // Keep a failed path until a successful retry or the caller drops this
        // round. Missing paths succeed as empty scopes; the next round repairs
        // renames/new arrivals not represented by this iterator.
        if !source.finished() {
            self.sources.push_back(source);
        }
        result.map(|mut step| {
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
            if source.pending.is_empty() {
                source.collect(max_entries.max(1), max_paths.max(1), &mut step.entries)?;
            }
            step.paths = source.pending.len().min(max_paths.max(1));
            if step.paths > 0 {
                options.sources = Some(vec![source.source]);
                options.paths = Some(source.pending[..step.paths].to_vec());
                step.discovery = Some(self.store.discover(options)?);
                source.pending.drain(..step.paths);
            }
            Ok(step)
        })();
        source.finalized = source.enumerated();
        if !source.finished() {
            self.sources.push_back(source);
        }
        result.map(|mut step: ReconciliationStep| {
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
        step.report = Some(report);
        Ok(step)
    }
}

impl SourceWalk {
    fn finished(&self) -> bool {
        self.finalized && self.enumerated()
    }

    fn enumerated(&self) -> bool {
        self.pending.is_empty()
            && self.current.is_none()
            && self.retry_entry.is_none()
            && self.queued.is_empty()
            && self.roots.is_empty()
    }

    fn collect(
        &mut self,
        max_entries: usize,
        max_paths: usize,
        entries: &mut usize,
    ) -> Result<(), Error> {
        while *entries < max_entries && self.pending.len() < max_paths {
            if self.current.is_some() || self.retry_entry.is_some() {
                self.next_entry(entries)?;
                continue;
            }
            if let Some(path) = self.queued.pop_front() {
                *entries += 1;
                let Some(root) = self.root.clone() else {
                    continue;
                };
                self.open_dir(path, root)?;
            } else if let Some(root) = self.roots.pop_front() {
                *entries += 1;
                if root.scope == WatchScope::File {
                    if root.path.exists() {
                        self.pending.push(root.path.clone());
                    }
                } else {
                    self.root = Some(root.clone());
                    self.open_dir(root.path.clone(), root)?;
                }
            } else {
                break;
            }
        }
        Ok(())
    }

    fn open_dir(&mut self, path: PathBuf, root: WatchedPath) -> Result<(), Error> {
        let canonical = match path.canonicalize() {
            Ok(path) => path,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                self.queued.push_front(path);
                return Err(Error::Discovery(error.to_string()));
            }
        };
        if self.visited.contains(&canonical) {
            return Ok(());
        }
        match fs::read_dir(&path) {
            Ok(dir) => {
                self.visited.insert(canonical.clone());
                self.current = Some(OpenDirectory {
                    entries: dir,
                    root,
                    path,
                    canonical,
                });
                Ok(())
            }
            Err(error) => {
                self.queued.push_front(path);
                Err(Error::Discovery(error.to_string()))
            }
        }
    }

    fn read_entry(
        &mut self,
        entries: &mut usize,
    ) -> Result<Option<(std::fs::DirEntry, WatchedPath)>, Error> {
        if let Some(retry) = self.retry_entry.take() {
            *entries += 1;
            return Ok(Some(retry));
        }
        let Some(current) = self.current.as_mut() else {
            return Ok(None);
        };
        let Some(entry) = current.entries.next() else {
            self.current = None;
            return Ok(None);
        };
        *entries += 1;
        match entry {
            Ok(entry) => Ok(Some((entry, current.root.clone()))),
            Err(error) => {
                // No filename is available: retain this directory and restart
                // its bounded enumeration rather than skip an unknown entry.
                let path = current.path.clone();
                let canonical = current.canonical.clone();
                self.current = None;
                self.visited.remove(&canonical);
                self.queued.push_front(path);
                Err(Error::Discovery(error.to_string()))
            }
        }
    }

    fn next_entry(&mut self, entries: &mut usize) -> Result<(), Error> {
        let Some((entry, root)) = self.read_entry(entries)? else {
            return Ok(());
        };
        if !root.admits(&entry.file_name()) {
            return Ok(());
        }
        let path = entry.path();
        let kind = match entry_type(&entry) {
            Ok(kind) => kind,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                self.retry_entry = Some((entry, root));
                return Err(Error::Discovery(error.to_string()));
            }
        };
        if kind.is_dir() || (kind.is_symlink() && path.is_dir()) {
            if root.scope == WatchScope::Tree {
                self.queued.push_back(path);
            }
        } else if !entry.file_name().to_string_lossy().ends_with("-shm") {
            self.pending.push(path);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ProviderRoots, StoreOptions};
    use std::io::Write;

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
            .any(|source| source.retry_entry.is_some()));
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
}
