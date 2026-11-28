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
//!   changed within [`RECENT`] or are their parent's newest, newest first,
//!   up to [`MAX_DIRS`] descriptors.
//!   That covers Claude's `projects/<project>/` and Codex's
//!   `sessions/YYYY/MM/DD/` where new sessions land, while staying far below
//!   the 256-descriptor soft limit macOS shells start processes with. A
//!   directory created under a watched one is watched as it appears.
//! * Appends, renames, removals, and anything in a directory this does not
//!   watch are still FSEvents' to report, as are rescans and registration
//!   loss. FSEvents also reports every create this one does, 14-19 ms later;
//!   [`Reported`] lets its callback drop that repeat when the file has not
//!   moved since, so one new file is one forced sweep, not two.
//! * Idle costs nothing: the thread blocks in `kevent` until a directory
//!   changes or it is woken, and the backstop's re-plan descends only into
//!   recently changed directories.
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
    // Every root first, so a root that also sits under another is planned
    // as a root, not claimed as that one's descendant.
    for root in roots {
        if root.is_dir() && seen.insert(root.clone()) {
            chosen.push((root.clone(), 0));
        }
    }
    for root in chosen.clone() {
        walk(&root.0, now, &mut seen, &mut below);
    }
    below.sort_by_key(|entry| std::cmp::Reverse(entry.2));
    let room = cap.saturating_sub(chosen.len());
    chosen.extend(below.into_iter().take(room).map(|(path, depth, _)| (path, depth)));
    chosen
}

/// The recently changed directories under `root`, to [`MAX_DEPTH`].
///
/// Only a recent directory is descended into, and the newest one in each
/// directory walked, however old. Creating an entry changes its parent's
/// mtime, so a directory that appeared within [`RECENT`] -- a new project, a
/// new session's sidecar directory -- always has a recent parent. The newest
/// child covers a chain whose upper level changes less often than its lower:
/// Codex's `YYYY` changes once a month, when a new `MM` is created, so
/// mid-month it is older than [`RECENT`] while today's `MM/DD` under it is
/// not. What this gives up is a directory that is old *and* not its parent's
/// newest, but still being written, a session resumed weeks later; FSEvents
/// still reports that one, just not as fast. Descending into everything
/// instead cost 430 ms of CPU on every backstop for a 40,000-directory Claude
/// tree, to find the handful of directories that are watched.
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
            let children = subdirectories(dir);
            let newest = children.iter().map(|(_, modified)| *modified).max();
            for (path, modified) in children {
                let recent = Some(modified) == newest
                    || now
                        .duration_since(modified)
                        .map_or(true, |age| age <= RECENT);
                if recent && seen.insert(path.clone()) {
                    if depth < MAX_DEPTH {
                        next.push(path.clone());
                    }
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
    /// tracked -- it then takes `depth`, so a directory a refresh adopts as a
    /// root counts its children from itself -- or cannot be read.
    pub(super) fn add(&mut self, dir: &Path, depth: usize, fresh: bool) -> bool {
        if let Some((tracked, _)) = self.dirs.get_mut(dir) {
            *tracked = depth;
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

/// What a file's `stat` said when kqueue reported it: enough to tell that
/// nothing has written to, truncated or replaced it since.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Fingerprint {
    len: u64,
    modified: Option<SystemTime>,
    /// Change time and inode: an mtime can be set back from user space, a
    /// ctime cannot, and a file replaced under the same name has a new inode.
    #[cfg(unix)]
    ctime_ns: (i64, i64),
    #[cfg(unix)]
    ino: (u64, u64),
}

impl Fingerprint {
    pub(super) fn of(path: &Path) -> Option<Self> {
        let meta = std::fs::metadata(path).ok()?;
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;
        meta.is_file().then(|| Self {
            len: meta.len(),
            modified: meta.modified().ok(),
            #[cfg(unix)]
            ctime_ns: (meta.ctime(), meta.ctime_nsec()),
            #[cfg(unix)]
            ino: (meta.dev(), meta.ino()),
        })
    }
}

/// How long kqueue's report of a file stands in for FSEvents' own report of
/// the same write. FSEvents delivers it 14-19 ms later on an idle machine;
/// the margin is for a loaded one.
pub(super) const REPORTED_TTL: Duration = Duration::from_secs(2);
/// Most files the ledger remembers; a burst past it just goes unsuppressed.
const MAX_REPORTED: usize = 256;

/// The files kqueue reported, shared with the FSEvents callback so it can
/// drop its own, later report of the same write.
///
/// Without this every new file costs two forced sweeps: kqueue's, which
/// captures it, and FSEvents', which re-reads it and finds nothing new. The
/// second is not only wasted work. Exact-path capture treats a dirty path as
/// evidence of a write and runs a resume pass, whose checkpoint write moves
/// the change feed, so that tick does not count as having changed nothing and
/// leaves the debounce window armed -- and the *next* new session waited out
/// what was left of it. On the latency runner that was every other sample:
/// p50 36 ms, p99 162 ms against FSEvents alone at 40 / 46 ms.
///
/// A report is dropped only when it cannot be news. kqueue recorded the
/// file's fingerprint *before* it signalled the loop, and the sweep that
/// signal guarantees starts after it, so it reads at least those bytes. If
/// the fingerprint still matches when FSEvents' event arrives -- which is
/// after whatever write it describes -- that write is one the sweep already
/// covers. Anything that moved the stat, any removal or rename, any rescan
/// notice, or a path kqueue never reported goes through as before.
#[derive(Debug, Default)]
pub(super) struct Reported {
    files: std::sync::Mutex<HashMap<PathBuf, (Fingerprint, std::time::Instant)>>,
    /// How many reports kqueue recorded, and how many FSEvents events were
    /// dropped as their repeats: what a test or trace can tell the two
    /// sources apart by.
    pub(super) recorded: std::sync::atomic::AtomicUsize,
    pub(super) dropped: std::sync::atomic::AtomicUsize,
}

impl Reported {
    /// Remember that `path` was reported with this `stat`. Called before the
    /// loop is signalled.
    pub(super) fn record(&self, path: &Path) {
        let Some(print) = Fingerprint::of(path) else {
            return;
        };
        let now = std::time::Instant::now();
        let mut files = self.files.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if files.len() >= MAX_REPORTED {
            files.retain(|_, (_, at)| now.duration_since(*at) < REPORTED_TTL);
            if files.len() >= MAX_REPORTED {
                return;
            }
        }
        files.insert(crate::discover::watch_path(path), (print, now));
        self.recorded.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Whether `event` only repeats reports kqueue already made: a create or
    /// a content change, naming only files kqueue reported within
    /// [`REPORTED_TTL`] whose fingerprint has not moved since.
    pub(super) fn repeats(&self, event: &notify::Event) -> bool {
        use notify::event::{EventKind, ModifyKind};
        let repeatable = matches!(
            event.kind,
            EventKind::Create(_) | EventKind::Modify(ModifyKind::Data(_) | ModifyKind::Any)
        );
        if !repeatable || event.need_rescan() || event.paths.is_empty() {
            return false;
        }
        let mut files = self.files.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if files.is_empty() {
            return false;
        }
        let repeat = event.paths.iter().all(|path| {
            let key = crate::discover::watch_path(path);
            let Some((print, at)) = files.get(&key) else {
                return false;
            };
            if at.elapsed() < REPORTED_TTL && Fingerprint::of(&key) == Some(*print) {
                return true;
            }
            // Moved or expired: this report is news, and so is every later one.
            files.remove(&key);
            false
        });
        if repeat {
            self.dropped.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        repeat
    }
}

#[cfg(target_os = "macos")]
pub(super) use macos::KqueueDirs;

#[cfg(target_os = "macos")]
mod macos {
    use super::{plan, DirIndex, Reported, MAX_DIRS};
    use std::collections::{HashMap, HashSet};
    use crate::discover::{WatchDepth, WatchRoot};
    use crate::watch::fs_events::event_scope;
    use crate::watch::ChangeScope;
    use kqueue::{EventData, EventFilter, FilterFlag, Ident, Vnode, Watcher};
    use notify::event::{CreateKind, EventKind};
    use std::io::{Read, Write};
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixStream;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex, PoisonError};
    use std::thread::JoinHandle;
    use std::time::{Duration, Instant, SystemTime};

    type OnChange = Arc<dyn Fn(ChangeScope) + Send + Sync>;

    /// The running watcher. Dropping it stops the thread, which closes every
    /// descriptor with its `kqueue::Watcher`.
    ///
    /// The thread blocks in `kevent` until something happens: a watched
    /// directory changes, a pending file's wait runs out, or `refresh` or
    /// `drop` writes to `wake`, whose other end is in the same queue. An idle
    /// loop therefore costs no wakeups at all, where waking on a short timeout
    /// to look at the flags cost twenty a second for as long as `watch` ran.
    pub(in crate::watch) struct KqueueDirs {
        stop: Arc<AtomicBool>,
        refresh: Arc<AtomicBool>,
        wake: UnixStream,
        thread: Option<JoinHandle<()>>,
    }

    impl KqueueDirs {
        /// Start watching the recursive roots in `roots` (shared with the
        /// FSEvents callback, so a root adopted later is seen on `refresh`).
        pub(in crate::watch) fn spawn(
            roots: Arc<Mutex<Vec<WatchRoot>>>,
            reported: Arc<Reported>,
            on_change: OnChange,
        ) -> std::io::Result<Self> {
            let mut watcher = Watcher::new()?;
            let (wake, woken) = UnixStream::pair()?;
            // Neither end may block: a wake that finds the buffer full has
            // nothing to add, and the drain stops when it is empty.
            wake.set_nonblocking(true)?;
            woken.set_nonblocking(true)?;
            watcher.add_fd(woken.as_raw_fd(), EventFilter::EVFILT_READ, FilterFlag::empty())?;
            watcher.watch()?;
            let stop = Arc::new(AtomicBool::new(false));
            let refresh = Arc::new(AtomicBool::new(true));
            let state = Loop {
                watcher,
                woken,
                index: DirIndex::default(),
                pending: HashMap::new(),
                roots: Vec::new(),
                shared: roots,
                reported,
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
                wake,
                thread: Some(thread),
            })
        }

        /// Pick up roots and directories that appeared since the last plan.
        pub(in crate::watch) fn refresh(&self) {
            self.refresh.store(true, Ordering::SeqCst);
            self.wake();
        }

        fn wake(&self) {
            // `WouldBlock` means bytes are already waiting, which wakes it
            // just the same.
            let _ = (&self.wake).write(&[1]);
        }
    }

    impl Drop for KqueueDirs {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            self.wake();
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
    }

    /// New files that were still empty when their directory reported them,
    /// each watched until its first write. Bounded: past it, an empty file is
    /// left to FSEvents, which reports its first write as well.
    const MAX_PENDING_FILES: usize = 32;
    /// How long an empty file is waited on before it is left to FSEvents.
    const PENDING_FILE_TTL: Duration = Duration::from_secs(10);

    struct Loop {
        watcher: Watcher,
        /// The read end of `KqueueDirs::wake`, registered in `watcher`.
        woken: UnixStream,
        index: DirIndex,
        /// Empty new files awaiting their first write, and when each appeared.
        pending: HashMap<PathBuf, Instant>,
        roots: Vec<WatchRoot>,
        shared: Arc<Mutex<Vec<WatchRoot>>>,
        /// What was reported, for the FSEvents callback to drop its repeat.
        reported: Arc<Reported>,
        on_change: OnChange,
    }

    impl Loop {
        fn run(mut self, stop: &AtomicBool, refresh: &AtomicBool) {
            while !stop.load(Ordering::SeqCst) {
                if refresh.swap(false, Ordering::SeqCst) {
                    self.replan();
                }
                let timeout = self.expire_pending();
                let Some(event) = self.watcher.poll_forever(timeout) else {
                    continue;
                };
                match (&event.ident, &event.data) {
                    (Ident::Filename(_, dir), EventData::Vnode(vnode)) => {
                        self.on_event(PathBuf::from(dir), vnode);
                    }
                    (Ident::Fd(fd), _) if *fd == self.woken.as_raw_fd() => self.drain_wake(),
                    _ => {}
                }
            }
        }

        /// Empty the wake socket; the flags say what the wake was for.
        fn drain_wake(&mut self) {
            let mut buffer = [0u8; 64];
            while matches!((&self.woken).read(&mut buffer), Ok(read) if read > 0) {}
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
            // One `kevent` call for the whole plan. `Watcher::watch`
            // resubmits every registration it holds, so registering the
            // directories one at a time made a plan of `MAX_DIRS` cost
            // `MAX_DIRS`^2 submissions on every backstop.
            let flags = FilterFlag::NOTE_WRITE | FilterFlag::NOTE_DELETE | FilterFlag::NOTE_RENAME;
            let mut added = Vec::new();
            for (dir, depth) in &planned {
                if self.index.len() >= MAX_DIRS || !self.index.add(dir, *depth, false) {
                    continue;
                }
                if self.watcher.add_filename(dir, EventFilter::EVFILT_VNODE, flags).is_ok() {
                    added.push(dir.clone());
                } else {
                    self.index.remove(dir);
                }
            }
            if added.is_empty() {
                return;
            }
            if self.watcher.watch().is_err() {
                // One bad entry fails the batch: fall back to one at a time,
                // so only the directories that cannot be watched are dropped.
                // The whole batch comes out first: `watch` resubmits every
                // registration it holds, so a bad entry left in would fail
                // each good one retried before it.
                let retry: Vec<(PathBuf, usize)> = added
                    .iter()
                    .map(|dir| (dir.clone(), self.index.depth(dir).unwrap_or(0)))
                    .collect();
                for (dir, _) in &retry {
                    self.forget(dir);
                }
                for (dir, depth) in &retry {
                    self.watch(dir, *depth, false);
                }
                return;
            }
            // Rescanned after registration, against the entries recorded
            // before it, exactly as `watch` does for one directory.
            for dir in &added {
                self.rescan(dir);
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
            if self.pending.contains_key(&dir) {
                self.on_pending_file(dir, vnode);
                return;
            }
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
            for file in appeared.files {
                self.arrived(file);
            }
            let depth = self.index.depth(dir).unwrap_or(0);
            for child in appeared.dirs {
                self.watch(&child, depth + 1, true);
            }
        }

        /// A file that just appeared. The directory's event fires when the
        /// entry is *created*, before the writer's first `write`: a tick that
        /// swept now would capture an empty transcript, write its bookkeeping,
        /// and so hold the debounce window open past the write that matters.
        /// An empty file is therefore watched itself and reported on its first
        /// write; one that already has bytes is reported at once.
        fn arrived(&mut self, file: PathBuf) {
            let empty = std::fs::metadata(&file).is_ok_and(|meta| meta.len() == 0);
            if !empty || self.pending.len() >= MAX_PENDING_FILES {
                self.report(&[file]);
                return;
            }
            let flags = FilterFlag::NOTE_WRITE | FilterFlag::NOTE_EXTEND | FilterFlag::NOTE_DELETE;
            if self
                .watcher
                .add_filename(&file, EventFilter::EVFILT_VNODE, flags)
                .is_err()
            {
                self.report(&[file]);
                return;
            }
            if self.watcher.watch().is_err() {
                // Registered but not submitted: the watcher still holds its
                // descriptor, and nothing in `pending` would expire it.
                let _ = self.watcher.remove_filename(&file, EventFilter::EVFILT_VNODE);
                self.report(&[file]);
                return;
            }
            // The first write can land between the size check and the watch.
            if std::fs::metadata(&file).is_ok_and(|meta| meta.len() > 0) {
                let _ = self.watcher.remove_filename(&file, EventFilter::EVFILT_VNODE);
                self.report(&[file]);
                return;
            }
            self.pending.insert(file, Instant::now());
        }

        fn on_pending_file(&mut self, file: PathBuf, vnode: &Vnode) {
            let wrote = matches!(vnode, Vnode::Write | Vnode::Extend);
            if !wrote && !matches!(vnode, Vnode::Delete | Vnode::Rename) {
                return;
            }
            self.pending.remove(&file);
            let _ = self.watcher.remove_filename(&file, EventFilter::EVFILT_VNODE);
            if wrote {
                self.report(&[file]);
            }
        }

        /// Stop waiting on files that stayed empty; FSEvents reports their
        /// first write whenever it comes. Returns how long until the next one
        /// runs out, the longest the loop may then block; `None` when no file
        /// is pending, and it blocks until an event or a wake.
        fn expire_pending(&mut self) -> Option<Duration> {
            let expired: Vec<PathBuf> = self
                .pending
                .iter()
                .filter(|(_, since)| since.elapsed() >= PENDING_FILE_TTL)
                .map(|(file, _)| file.clone())
                .collect();
            for file in expired {
                self.pending.remove(&file);
                let _ = self.watcher.remove_filename(&file, EventFilter::EVFILT_VNODE);
            }
            self.pending
                .values()
                .map(|since| PENDING_FILE_TTL.saturating_sub(since.elapsed()))
                .min()
                // `poll_forever` treats a zero timeout as "return at once";
                // never ask for less than a millisecond, so an expiry that
                // is due cannot spin.
                .map(|left| left.max(Duration::from_millis(1)))
        }

        /// Each new file, as the `Create` event FSEvents would deliver for
        /// it, through the same root matching.
        fn report(&self, files: &[PathBuf]) {
            for file in files {
                let event =
                    notify::Event::new(EventKind::Create(CreateKind::File)).add_path(file.clone());
                if let Some(scope) = event_scope(&event, &self.roots) {
                    // Before the signal, so the sweep it guarantees reads at
                    // least the bytes this fingerprint describes.
                    self.reported.record(file);
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
    fn plan_skips_directories_older_than_recent_but_keeps_the_root_and_its_newest() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("projects");
        fs::create_dir_all(root.join("older")).unwrap();
        fs::create_dir_all(root.join("newest")).unwrap();
        let now = SystemTime::now();
        set_dir_modified(&root.join("older"), now - Duration::from_secs(60));
        set_dir_modified(&root.join("newest"), now);
        let later = now + RECENT + Duration::from_secs(120);
        assert_eq!(
            plan(std::slice::from_ref(&root), MAX_DIRS, later),
            vec![(root.clone(), 0), (root.join("newest"), 1)]
        );
    }

    /// Sets a directory's mtime. Windows opens a directory handle only with
    /// `FILE_FLAG_BACKUP_SEMANTICS`, and needs `FILE_WRITE_ATTRIBUTES` to set it.
    fn set_dir_modified(path: &Path, when: SystemTime) {
        #[cfg(windows)]
        let dir = {
            use std::os::windows::fs::OpenOptionsExt;
            fs::OpenOptions::new()
                .access_mode(0x0100)
                .custom_flags(0x0200_0000)
                .open(path)
        };
        #[cfg(not(windows))]
        let dir = fs::File::open(path);
        dir.unwrap().set_modified(when).unwrap();
    }

    #[test]
    fn plan_reaches_todays_codex_day_through_a_year_directory_last_changed_weeks_ago() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("sessions");
        let today = root.join("2026/11/28");
        let last_year = root.join("2025/12/31");
        fs::create_dir_all(&today).unwrap();
        fs::create_dir_all(&last_year).unwrap();
        let old = SystemTime::now() - RECENT - Duration::from_secs(60);
        let older = old - Duration::from_secs(60 * 60 * 24 * 30);
        // `2026` changed when `11` was created, weeks ago; `11` changed when
        // `28` was created, today. Last year is older throughout.
        for path in [last_year.clone(), root.join("2025/12"), root.join("2025")] {
            set_dir_modified(&path, older);
        }
        set_dir_modified(&root.join("2026"), old);
        let planned = plan(std::slice::from_ref(&root), MAX_DIRS, SystemTime::now());
        let dirs: Vec<&PathBuf> = planned.iter().map(|(path, _)| path).collect();
        assert!(dirs.contains(&&today), "{dirs:?}");
        assert!(dirs.contains(&&root.join("2026")), "{dirs:?}");
        assert!(!dirs.contains(&&root.join("2025")), "{dirs:?}");
        assert!(!dirs.contains(&&last_year), "{dirs:?}");
    }

    #[test]
    fn a_root_under_another_root_is_planned_as_a_root() {
        let dir = tempfile::tempdir().unwrap();
        let outer = dir.path().join("sessions");
        let inner = outer.join("2026/11/28");
        fs::create_dir_all(inner.join("child")).unwrap();
        let planned = plan(&[outer.clone(), inner.clone()], MAX_DIRS, SystemTime::now());
        assert!(planned.contains(&(inner.clone(), 0)), "{planned:?}");
        assert!(planned.contains(&(inner.join("child"), 1)), "{planned:?}");
        assert_eq!(planned.iter().filter(|(path, _)| path == &inner).count(), 1);
    }

    #[test]
    fn plan_does_not_descend_into_a_directory_that_has_not_changed_recently() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("projects");
        let stale = root.join("stale");
        let active = root.join("active");
        fs::create_dir_all(stale.join("session")).unwrap();
        fs::create_dir_all(active.join("session")).unwrap();
        let old = SystemTime::now() - RECENT - Duration::from_secs(60);
        // Children first: setting a child's times does not touch its parent's.
        for path in [stale.join("session"), stale.clone()] {
            set_dir_modified(&path, old);
        }
        let planned = plan(std::slice::from_ref(&root), MAX_DIRS, SystemTime::now());
        let dirs: Vec<&PathBuf> = planned.iter().map(|(path, _)| path).collect();
        assert!(dirs.contains(&&active.join("session")), "{dirs:?}");
        assert!(!dirs.contains(&&stale), "{dirs:?}");
        // Recent in itself, but only reachable through a stale parent.
        set_dir_modified(&stale.join("session"), SystemTime::now());
        set_dir_modified(&stale, old);
        let planned = plan(std::slice::from_ref(&root), MAX_DIRS, SystemTime::now());
        assert!(planned.iter().all(|(path, _)| path != &stale.join("session")), "{planned:?}");
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

    fn event(kind: notify::EventKind, paths: &[&Path]) -> notify::Event {
        paths
            .iter()
            .fold(notify::Event::new(kind), |event, path| event.add_path(path.to_path_buf()))
    }

    #[test]
    fn a_repeat_of_a_reported_file_is_recognised_until_its_stat_moves() {
        use notify::event::{CreateKind, DataChange, EventKind, ModifyKind, RemoveKind};
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("session.jsonl");
        let other = dir.path().join("other.jsonl");
        fs::write(&file, "{}\n").unwrap();
        fs::write(&other, "{}\n").unwrap();
        let reported = Reported::default();
        let create = event(EventKind::Create(CreateKind::File), &[&file]);
        assert!(!reported.repeats(&create), "nothing reported yet");

        reported.record(&file);
        assert!(reported.repeats(&create));
        let modify = event(EventKind::Modify(ModifyKind::Data(DataChange::Content)), &[&file]);
        assert!(reported.repeats(&modify), "a second callback for the same write");
        assert!(
            !reported.repeats(&event(EventKind::Create(CreateKind::File), &[&file, &other])),
            "a path kqueue never reported makes the event news"
        );
        assert!(!reported.repeats(&event(EventKind::Remove(RemoveKind::File), &[&file])));
        assert!(!reported.repeats(&event(
            EventKind::Modify(ModifyKind::Name(notify::event::RenameMode::Any)),
            &[&file]
        )));
        assert!(!reported.repeats(&create.clone().set_flag(notify::event::Flag::Rescan)));

        // An append after kqueue's report is a write its sweep may not
        // have read, so FSEvents' report of it goes through -- and so does
        // every later one, even once the stat stops moving.
        fs::write(&file, "{}\n{}\n").unwrap();
        assert!(!reported.repeats(&modify));
        assert!(!reported.repeats(&modify));
    }

    #[test]
    fn a_file_replaced_under_the_same_name_and_size_is_news() {
        use notify::event::{CreateKind, EventKind};
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("session.jsonl");
        fs::write(&file, "{}\n").unwrap();
        let reported = Reported::default();
        reported.record(&file);
        let replacement = dir.path().join("next.jsonl");
        fs::write(&replacement, "[]\n").unwrap();
        fs::rename(&replacement, &file).unwrap();
        assert!(!reported.repeats(&event(EventKind::Create(CreateKind::File), &[&file])));
    }

    #[test]
    fn the_ledger_stays_bounded() {
        let dir = tempfile::tempdir().unwrap();
        let reported = Reported::default();
        for n in 0..MAX_REPORTED + 10 {
            let file = dir.path().join(format!("{n}.jsonl"));
            fs::write(&file, "{}\n").unwrap();
            reported.record(&file);
        }
        assert_eq!(reported.files.lock().unwrap().len(), MAX_REPORTED);
    }

    /// End to end through `fs_events::attach`: a new transcript reaches the
    /// loop once, from kqueue, and FSEvents' own report of it a few
    /// milliseconds later is dropped rather than becoming a second forced
    /// sweep. An append after it is still delivered.
    #[cfg(target_os = "macos")]
    #[test]
    fn fs_events_does_not_repeat_what_kqueue_reported() {
        use crate::discover::WatchRoot;
        use std::sync::atomic::Ordering::Relaxed;
        use std::sync::{mpsc, Mutex};
        let dir = tempfile::tempdir().unwrap();
        let root_path = dir.path().canonicalize().unwrap().join("projects");
        let project = root_path.join("-work");
        fs::create_dir_all(&project).unwrap();
        let (tx, rx) = mpsc::channel();
        let tx = Mutex::new(tx);
        let watch = crate::watch::fs_events::attach(
            &[WatchRoot::tree(root_path.clone())],
            move |scope| {
                let _ = tx.lock().unwrap().send(scope);
            },
            || {},
            |_| {},
        )
        .unwrap();
        let ledger = watch.reported.clone();
        let started = std::time::Instant::now();
        // `--nocapture` prints the sequence: the evidence this test asserts.
        let trace = |what: &str| {
            eprintln!(
                "[{:>8.3} ms] {what} (kqueue recorded {}, FSEvents repeats dropped {})",
                started.elapsed().as_secs_f64() * 1e3,
                ledger.recorded.load(Relaxed),
                ledger.dropped.load(Relaxed)
            );
        };
        // Past FSEvents' replay of the directory's creation.
        std::thread::sleep(Duration::from_millis(500));
        while rx.try_recv().is_ok() {}
        let file = project.join("00000000-0000-4000-8000-000000000001.jsonl");
        let names = |scope: &crate::watch::ChangeScope| match scope {
            crate::watch::ChangeScope::Roots { paths, .. } => paths
                .iter()
                .any(|path| path.ends_with(file.file_name().unwrap())),
            _ => false,
        };
        let wait = |within: Duration, until: &dyn Fn() -> bool| {
            let deadline = std::time::Instant::now() + within;
            while !until() && std::time::Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(5));
            }
        };

        trace("write a new file, closed");
        fs::write(&file, "{}\n").unwrap();
        let first = rx.recv_timeout(Duration::from_secs(2)).expect("reported");
        trace(&format!("notification 1: {first:?}"));
        assert!(names(&first), "{first:?}");
        assert_eq!(ledger.recorded.load(Relaxed), 1, "kqueue made the first report");
        // FSEvents' own report of that create reaches the callback 14-19 ms
        // later, and is dropped there.
        wait(Duration::from_secs(2), &|| ledger.dropped.load(Relaxed) > 0);
        trace("FSEvents' report of the same create");
        assert!(ledger.dropped.load(Relaxed) > 0, "FSEvents' repeat reached the drop");
        let mut repeats = 0;
        let quiet = std::time::Instant::now() + Duration::from_millis(500);
        while let Ok(scope) = rx.recv_timeout(quiet.saturating_duration_since(std::time::Instant::now())) {
            trace(&format!("unexpected notification: {scope:?}"));
            repeats += usize::from(names(&scope));
        }
        trace(&format!("quiet window over: {repeats} repeat notification(s)"));
        assert_eq!(repeats, 0, "FSEvents repeated kqueue's report");

        // Closed before waiting: FSEvents reports a write to a file still
        // held open late, or not at all.
        trace("append to the same file, closed");
        let mut append = fs::OpenOptions::new().append(true).open(&file).unwrap();
        std::io::Write::write_all(&mut append, b"{}\n").unwrap();
        drop(append);
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let mut appended = None;
        while let Ok(scope) = rx.recv_timeout(deadline.saturating_duration_since(std::time::Instant::now())) {
            if names(&scope) {
                appended = Some(scope);
                break;
            }
        }
        trace(&format!("notification 2: {appended:?}"));
        assert!(appended.is_some(), "an append after kqueue's report is still delivered");
        // kqueue reports new files only: this one came from FSEvents, past
        // the ledger because the append moved the file's stat.
        assert_eq!(ledger.recorded.load(Relaxed), 1, "{:?}", appended);
        drop(watch);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn a_new_transcript_is_reported_by_kqueue_alone() {
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
        // No FSEvents here: every report is kqueue's. How fast it comes is
        // the latency benchmark's to measure (`stages.fs_event_kqueue`), not
        // a wall-clock assertion's on a loaded machine.
        let watcher = KqueueDirs::spawn(
            Arc::new(Mutex::new(vec![root])),
            Arc::default(),
            Arc::new(move |scope| {
                let _ = tx.lock().unwrap().send(scope);
            }),
        )
        .unwrap();
        let names = |scope: &crate::watch::ChangeScope, file: &Path| match scope {
            crate::watch::ChangeScope::Roots { paths, .. } => {
                paths.iter().any(|path| path.ends_with(file.file_name().unwrap()))
            }
            _ => false,
        };
        let reported = |file: &Path| {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while let Ok(scope) = rx.recv_timeout(deadline.saturating_duration_since(std::time::Instant::now())) {
                if names(&scope, file) {
                    return true;
                }
            }
            false
        };
        // Ready once a file written after the watches went up is reported:
        // until then, a file could be counted as already there. Retried,
        // since one written before that point never is.
        let mut ready = false;
        for attempt in 0..50 {
            let probe = project.join(format!("probe-{attempt}.jsonl"));
            fs::write(&probe, "{}\n").unwrap();
            let deadline = std::time::Instant::now() + Duration::from_millis(100);
            while let Ok(scope) = rx.recv_timeout(deadline.saturating_duration_since(std::time::Instant::now())) {
                ready |= names(&scope, &probe);
            }
            if ready {
                break;
            }
        }
        assert!(ready, "kqueue's watches never came up");

        let file = project.join("00000000-0000-4000-8000-000000000001.jsonl");
        fs::write(&file, "{}\n").unwrap();
        assert!(reported(&file), "a new transcript is reported");
        // A project directory created later is watched as it appears.
        let later = root_path.join("-later");
        fs::create_dir(&later).unwrap();
        let second = later.join("00000000-0000-4000-8000-000000000002.jsonl");
        std::thread::sleep(Duration::from_millis(50));
        fs::write(&second, "{}\n").unwrap();
        assert!(reported(&second), "a file in a newly created project directory is reported");
        // A transcript created empty is reported on its first write, not at
        // creation, so the tick that follows reads its bytes.
        let third = project.join("00000000-0000-4000-8000-000000000003.jsonl");
        std::fs::File::create(&third).unwrap();
        let quiet = std::time::Instant::now() + Duration::from_millis(200);
        while let Ok(scope) = rx.recv_timeout(quiet.saturating_duration_since(std::time::Instant::now())) {
            assert!(!names(&scope, &third), "an empty file was reported before its first write");
        }
        fs::write(&third, "{}\n").unwrap();
        assert!(reported(&third), "the first write is reported");
        drop(watcher);
    }
}
