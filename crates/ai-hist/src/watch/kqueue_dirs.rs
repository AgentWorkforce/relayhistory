//! New-file detection on macOS through kqueue directory watches, beside FSEvents.
//!
//! FSEvents is the recursive watcher of record, and on macOS it is slow to say
//! a file appeared: the latency benchmark measured 14-19 ms from `write` to the
//! callback even with `latency 0.0` and `kFSEventStreamCreateFlagNoDefer`,
//! three times the whole 5 ms a new session has to become visible in. A
//! kqueue `EVFILT_VNODE` / `NOTE_WRITE` watch on a *directory* fires when an
//! entry is created in it, measured at 0.18 ms p99 on the same runner, and
//! 0.88 ms p99 including the rescan below (see docs/benchmarks.md,
//! "Session-visible latency").
//!
//! This is an accelerator, not a second source of truth:
//!
//! * A vnode event names no file. Each event rescans that one directory and
//!   diffs its entries against the last scan, and only the *new* names are
//!   reported -- as `Create` events through [`event_scope`], so they reach the
//!   loop with exactly the spellings an FSEvents event would have.
//! * Directories only, never a descriptor per file, and only some of them:
//!   each recursive root and the directories under it to [`MAX_DEPTH`] that
//!   changed within [`RECENT`], newest first, up to [`MAX_DIRS`] descriptors.
//!   That covers Claude's `projects/<project>/` and Codex's
//!   `sessions/YYYY/MM/DD/` where new sessions land, while staying far below
//!   the 256-descriptor soft limit macOS shells start processes with. A
//!   directory created under a watched one is watched as it appears.
//! * Appends, renames, removals, and anything in a directory this does not
//!   watch are still FSEvents' to report, as are rescans and registration
//!   loss. FSEvents also reports every create this one does, a few
//!   milliseconds later; the loop's window collapses that into the trailing
//!   tick, which finds the transcript already captured.
//!
//! The planning and diffing are plain filesystem code and are tested on every
//! platform; only the thread that waits on the queue is macOS-only, and it
//! reaches kqueue through the `kqueue` crate (the one notify's own kqueue
//! backend uses), so this crate still has no `unsafe` of its own.

use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

/// Directories below a root that are watched: `projects/<p>/<session>/subagents`
/// for Claude, `sessions/YYYY/MM/DD` for Codex.
pub(super) const MAX_DEPTH: usize = 3;
/// Most descriptors this holds open. Roots are always watched; the rest of
/// the budget goes to the most recently changed directories.
pub(super) const MAX_DIRS: usize = 128;
/// A directory that has not changed for this long is left to FSEvents.
pub(super) const RECENT: Duration = Duration::from_secs(14 * 24 * 60 * 60);

/// The directories to watch under `roots`, roots first, then the rest newest
/// first, at most `cap` in all.
pub(super) fn plan(roots: &[PathBuf], cap: usize, now: SystemTime) -> Vec<(PathBuf, usize)> {
    let mut chosen: Vec<(PathBuf, usize)> = Vec::new();
    let mut seen = HashSet::new();
    let mut below = Vec::new();
    for root in roots {
        if root.is_dir() && seen.insert(root.clone()) {
            chosen.push((root.clone(), 0));
            walk(root, now, &mut seen, &mut below);
        }
    }
    below.sort_by_key(|entry| std::cmp::Reverse(entry.2));
    let room = cap.saturating_sub(chosen.len());
    chosen.extend(below.into_iter().take(room).map(|(path, depth, _)| (path, depth)));
    chosen
}

/// Every directory under `root` to [`MAX_DEPTH`]. All of them are descended
/// into, so a recent date or subagent directory under an old parent is still
/// found; only the recent ones become candidates.
fn walk(
    root: &Path,
    now: SystemTime,
    seen: &mut HashSet<PathBuf>,
    below: &mut Vec<(PathBuf, usize, SystemTime)>,
) {
    let mut frontier = vec![root.to_path_buf()];
    for depth in 1..=MAX_DEPTH {
        let mut next = Vec::new();
        for dir in &frontier {
            for (path, modified) in subdirectories(dir) {
                next.push(path.clone());
                let recent = now
                    .duration_since(modified)
                    .map_or(true, |age| age <= RECENT);
                if recent && seen.insert(path.clone()) {
                    below.push((path, depth, modified));
                }
            }
        }
        frontier = next;
    }
}

/// The directories directly inside `dir`, with when each last changed.
fn subdirectories(dir: &Path) -> Vec<(PathBuf, SystemTime)> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
        .map(|entry| {
            let modified = entry
                .metadata()
                .and_then(|meta| meta.modified())
                .unwrap_or(SystemTime::UNIX_EPOCH);
            (entry.path(), modified)
        })
        .collect()
}

/// What a rescan of one directory found since the last.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct Appeared {
    pub(super) files: Vec<PathBuf>,
    /// New directories still within [`MAX_DEPTH`] of their root.
    pub(super) dirs: Vec<PathBuf>,
}

/// The entries each watched directory held at its last scan.
#[derive(Debug, Default)]
pub(super) struct DirIndex {
    dirs: HashMap<PathBuf, (usize, HashSet<OsString>)>,
}

fn names(dir: &Path) -> Option<HashSet<OsString>> {
    Some(
        std::fs::read_dir(dir)
            .ok()?
            .flatten()
            .map(|entry| entry.file_name())
            .collect(),
    )
}

impl DirIndex {
    /// Start tracking `dir`. `fresh` is a directory that has just appeared:
    /// it starts empty, so its first rescan reports everything already in
    /// it. Otherwise it remembers what it holds now. False when it is already
    /// tracked or cannot be read.
    pub(super) fn add(&mut self, dir: &Path, depth: usize, fresh: bool) -> bool {
        if self.dirs.contains_key(dir) {
            return false;
        }
        let names = if fresh {
            HashSet::new()
        } else {
            let Some(names) = names(dir) else {
                return false;
            };
            names
        };
        self.dirs.insert(dir.to_path_buf(), (depth, names));
        true
    }

    /// Every tracked directory.
    pub(super) fn dirs(&self) -> Vec<PathBuf> {
        self.dirs.keys().cloned().collect()
    }

    pub(super) fn remove(&mut self, dir: &Path) {
        self.dirs.remove(dir);
    }

    pub(super) fn len(&self) -> usize {
        self.dirs.len()
    }

    /// Re-read `dir` and report the entries that were not there last time.
    /// `None` when the directory is gone or untracked.
    pub(super) fn rescan(&mut self, dir: &Path) -> Option<Appeared> {
        let (depth, known) = self.dirs.get_mut(dir)?;
        let now = names(dir)?;
        let mut appeared = Appeared::default();
        for name in now.difference(known) {
            let path = dir.join(name);
            match std::fs::symlink_metadata(&path) {
                Ok(meta) if meta.is_dir() => {
                    if *depth < MAX_DEPTH {
                        appeared.dirs.push(path);
                    }
                }
                Ok(meta) if meta.is_file() => appeared.files.push(path),
                _ => {}
            }
        }
        appeared.files.sort();
        appeared.dirs.sort();
        *known = now;
        Some(appeared)
    }

    pub(super) fn depth(&self, dir: &Path) -> Option<usize> {
        self.dirs.get(dir).map(|(depth, _)| *depth)
    }
}

#[cfg(target_os = "macos")]
pub(super) use macos::KqueueDirs;

#[cfg(target_os = "macos")]
mod macos {
    use super::{plan, DirIndex, MAX_DIRS};
    use std::collections::HashSet;
    use crate::discover::{WatchDepth, WatchRoot};
    use crate::watch::fs_events::event_scope;
    use crate::watch::ChangeScope;
    use kqueue::{EventData, EventFilter, FilterFlag, Ident, Vnode, Watcher};
    use notify::event::{CreateKind, EventKind};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex, PoisonError};
    use std::thread::JoinHandle;
    use std::time::{Duration, SystemTime};

    /// How long one wait for an event lasts before the stop and refresh
    /// flags are looked at again: the most a stop or refresh waits.
    const POLL: Duration = Duration::from_millis(50);

    type OnChange = Arc<dyn Fn(ChangeScope) + Send + Sync>;

    /// The running watcher. Dropping it stops the thread, which closes every
    /// descriptor with its `kqueue::Watcher`.
    pub(in crate::watch) struct KqueueDirs {
        stop: Arc<AtomicBool>,
        refresh: Arc<AtomicBool>,
        thread: Option<JoinHandle<()>>,
    }

    impl KqueueDirs {
        /// Start watching the recursive roots in `roots` (shared with the
        /// FSEvents callback, so a root adopted later is seen on `refresh`).
        pub(in crate::watch) fn spawn(
            roots: Arc<Mutex<Vec<WatchRoot>>>,
            on_change: OnChange,
        ) -> std::io::Result<Self> {
            let watcher = Watcher::new()?;
            let stop = Arc::new(AtomicBool::new(false));
            let refresh = Arc::new(AtomicBool::new(true));
            let state = Loop {
                watcher,
                index: DirIndex::default(),
                roots: Vec::new(),
                shared: roots,
                on_change,
            };
            let thread = {
                let (stop, refresh) = (stop.clone(), refresh.clone());
                std::thread::Builder::new()
                    .name("ai-hist-kqueue".into())
                    .spawn(move || state.run(&stop, &refresh))?
            };
            Ok(Self {
                stop,
                refresh,
                thread: Some(thread),
            })
        }

        /// Pick up roots and directories that appeared since the last plan.
        pub(in crate::watch) fn refresh(&self) {
            self.refresh.store(true, Ordering::SeqCst);
        }
    }

    impl Drop for KqueueDirs {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
    }

    struct Loop {
        watcher: Watcher,
        index: DirIndex,
        roots: Vec<WatchRoot>,
        shared: Arc<Mutex<Vec<WatchRoot>>>,
        on_change: OnChange,
    }

    impl Loop {
        fn run(mut self, stop: &AtomicBool, refresh: &AtomicBool) {
            while !stop.load(Ordering::SeqCst) {
                if refresh.swap(false, Ordering::SeqCst) {
                    self.replan();
                }
                let Some(event) = self.watcher.poll(Some(POLL)) else {
                    continue;
                };
                if let (Ident::Filename(_, dir), EventData::Vnode(vnode)) =
                    (&event.ident, &event.data)
                {
                    self.on_event(PathBuf::from(dir), vnode);
                }
            }
        }

        /// Bring the watched set in line with a fresh plan: drop directories
        /// that fell out of it (roots never do), so the newest directories
        /// get the descriptors, then watch what is new in it.
        fn replan(&mut self) {
            self.roots = self
                .shared
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone();
            let trees: Vec<PathBuf> = self
                .roots
                .iter()
                .filter(|root| root.depth == WatchDepth::Tree)
                .map(|root| root.registration_key().to_path_buf())
                .collect();
            let planned = plan(&trees, MAX_DIRS, SystemTime::now());
            let keep: HashSet<&PathBuf> = planned.iter().map(|(dir, _)| dir).collect();
            for dir in self.index.dirs() {
                if !keep.contains(&dir) {
                    self.forget(&dir);
                }
            }
            for (dir, depth) in &planned {
                self.watch(dir, *depth, false);
            }
        }

        /// Watch `dir`. Its entries are recorded *before* the watch is
        /// registered and rescanned *after*, so a file created in between is
        /// reported by that rescan rather than counted as already there. A
        /// `fresh` directory -- one that has just appeared -- starts empty, so
        /// everything already in it is reported and its subdirectories are
        /// watched the same way, down to [`super::MAX_DEPTH`]: a new Codex
        /// `YYYY/MM/DD` tree is created a level at a time, faster than the
        /// parent's event is read.
        fn watch(&mut self, dir: &Path, depth: usize, fresh: bool) -> bool {
            if self.index.len() >= MAX_DIRS || !self.index.add(dir, depth, fresh) {
                return false;
            }
            let flags = FilterFlag::NOTE_WRITE | FilterFlag::NOTE_DELETE | FilterFlag::NOTE_RENAME;
            let registered = self
                .watcher
                .add_filename(dir, EventFilter::EVFILT_VNODE, flags)
                .is_ok()
                && self.watcher.watch().is_ok();
            if !registered {
                self.forget(dir);
                return false;
            }
            self.rescan(dir);
            true
        }

        fn forget(&mut self, dir: &Path) {
            let _ = self.watcher.remove_filename(dir, EventFilter::EVFILT_VNODE);
            self.index.remove(dir);
        }

        fn on_event(&mut self, dir: PathBuf, vnode: &Vnode) {
            match vnode {
                // The directory object is gone or moved; its parent's rescan,
                // or the next refresh, watches whatever replaces it.
                Vnode::Delete | Vnode::Rename => self.forget(&dir),
                Vnode::Write | Vnode::Extend => self.rescan(&dir),
                _ => {}
            }
        }

        fn rescan(&mut self, dir: &Path) {
            let Some(appeared) = self.index.rescan(dir) else {
                self.forget(dir);
                return;
            };
            self.report(&appeared.files);
            let depth = self.index.depth(dir).unwrap_or(0);
            for child in appeared.dirs {
                self.watch(&child, depth + 1, true);
            }
        }

        /// Each new file, as the `Create` event FSEvents would deliver for
        /// it, through the same root matching.
        fn report(&self, files: &[PathBuf]) {
            for file in files {
                let event =
                    notify::Event::new(EventKind::Create(CreateKind::File)).add_path(file.clone());
                if let Some(scope) = event_scope(&event, &self.roots) {
                    (self.on_change)(scope);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn plan_watches_roots_then_recent_directories_newest_first_within_the_cap() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("projects");
        for name in ["a", "b", "c"] {
            fs::create_dir_all(root.join(name).join("sub")).unwrap();
        }
        let now = SystemTime::now();
        let planned = plan(std::slice::from_ref(&root), 3, now);
        assert_eq!(planned.len(), 3, "{planned:?}");
        assert_eq!(planned[0], (root.clone(), 0), "the root comes first");
        assert!(planned[1..].iter().all(|(_, depth)| *depth >= 1));

        let everything = plan(std::slice::from_ref(&root), MAX_DIRS, now);
        // The root, three project dirs and three subdirectories.
        assert_eq!(everything.len(), 7, "{everything:?}");
    }

    #[test]
    fn plan_skips_directories_older_than_recent_but_keeps_the_root() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("projects");
        fs::create_dir_all(root.join("old")).unwrap();
        let later = SystemTime::now() + RECENT + Duration::from_secs(60);
        assert_eq!(plan(std::slice::from_ref(&root), MAX_DIRS, later), vec![(root, 0)]);
    }

    #[test]
    fn plan_ignores_missing_roots_and_does_not_descend_past_max_depth() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("sessions");
        let deep = root.join("2026/10/25/too-deep");
        fs::create_dir_all(&deep).unwrap();
        let planned = plan(
            &[dir.path().join("missing"), root.clone()],
            MAX_DIRS,
            SystemTime::now(),
        );
        assert!(planned.iter().all(|(path, _)| path != &deep), "{planned:?}");
        assert!(planned.iter().any(|(path, depth)| path == &root.join("2026/10/25") && *depth == 3));
        assert!(planned.iter().all(|(path, _)| !path.ends_with("missing")));
    }

    #[test]
    fn rescan_reports_only_new_files_and_new_directories_within_depth() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        fs::write(root.join("existing.jsonl"), "{}\n").unwrap();
        let mut index = DirIndex::default();
        assert!(index.add(&root, 0, false));
        assert!(!index.add(&root, 0, false), "already tracked");
        assert_eq!(index.rescan(&root), Some(Appeared::default()));

        fs::write(root.join("new.jsonl"), "{}\n").unwrap();
        fs::create_dir(root.join("project")).unwrap();
        let appeared = index.rescan(&root).unwrap();
        assert_eq!(appeared.files, vec![root.join("new.jsonl")]);
        assert_eq!(appeared.dirs, vec![root.join("project")]);
        assert_eq!(index.rescan(&root), Some(Appeared::default()), "reported once");

        let deepest = root.join("project");
        assert!(index.add(&deepest, MAX_DEPTH, false));
        fs::create_dir(deepest.join("beyond")).unwrap();
        assert!(index.rescan(&deepest).unwrap().dirs.is_empty(), "past MAX_DEPTH");
    }

    #[test]
    fn a_fresh_directory_reports_what_it_already_holds() {
        let dir = tempfile::tempdir().unwrap();
        let day = dir.path().join("25");
        fs::create_dir(&day).unwrap();
        fs::write(day.join("rollout.jsonl"), "{}\n").unwrap();
        fs::create_dir(day.join("nested")).unwrap();
        let mut index = DirIndex::default();
        assert!(index.add(&day, 2, true));
        let appeared = index.rescan(&day).unwrap();
        assert_eq!(appeared.files, vec![day.join("rollout.jsonl")]);
        assert_eq!(appeared.dirs, vec![day.join("nested")]);
        assert_eq!(index.dirs(), vec![day]);
    }

    #[test]
    fn rescan_of_a_removed_directory_is_none() {
        let dir = tempfile::tempdir().unwrap();
        let gone = dir.path().join("gone");
        fs::create_dir(&gone).unwrap();
        let mut index = DirIndex::default();
        assert!(index.add(&gone, 1, false));
        fs::remove_dir(&gone).unwrap();
        assert_eq!(index.rescan(&gone), None);
        assert_eq!(index.len(), 1);
        index.remove(&gone);
        assert_eq!(index.len(), 0);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn a_new_transcript_is_reported_by_kqueue_within_a_few_milliseconds() {
        use crate::discover::WatchRoot;
        use std::sync::{mpsc, Arc, Mutex};
        let dir = tempfile::tempdir().unwrap();
        let root_path = dir.path().canonicalize().unwrap().join("projects");
        let project = root_path.join("-work");
        fs::create_dir_all(&project).unwrap();
        let mut root = WatchRoot::tree(root_path.clone());
        root.resolve();
        let (tx, rx) = mpsc::channel();
        let tx = Mutex::new(tx);
        let watcher = KqueueDirs::spawn(
            Arc::new(Mutex::new(vec![root])),
            Arc::new(move |scope| {
                let _ = tx.lock().unwrap().send(scope);
            }),
        )
        .unwrap();
        // Let the thread plan its watches before the clock starts.
        std::thread::sleep(Duration::from_millis(100));
        let file = project.join("00000000-0000-4000-8000-000000000001.jsonl");
        let started = std::time::Instant::now();
        fs::write(&file, "{}\n").unwrap();
        let scope = rx.recv_timeout(Duration::from_secs(2)).expect("kqueue event");
        assert!(started.elapsed() < Duration::from_millis(50), "{:?}", started.elapsed());
        match scope {
            crate::watch::ChangeScope::Roots { paths, .. } => {
                assert!(paths.iter().any(|path| path.ends_with(file.file_name().unwrap())), "{paths:?}");
            }
            other => panic!("unexpected scope {other:?}"),
        }
        // A project directory created later is watched as it appears.
        let later = root_path.join("-later");
        fs::create_dir(&later).unwrap();
        let second = later.join("00000000-0000-4000-8000-000000000002.jsonl");
        std::thread::sleep(Duration::from_millis(50));
        fs::write(&second, "{}\n").unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let mut seen = false;
        while let Ok(scope) = rx.recv_timeout(deadline.saturating_duration_since(std::time::Instant::now())) {
            if let crate::watch::ChangeScope::Roots { paths, .. } = scope {
                if paths.iter().any(|path| path.ends_with(second.file_name().unwrap())) {
                    seen = true;
                    break;
                }
            }
        }
        assert!(seen, "a file in a newly created project directory is reported");
        drop(watcher);
    }
}
