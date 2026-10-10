use super::*;
use notify::event::{EventKind, ModifyKind, RenameMode};
use notify::{RecursiveMode, Watcher};

/// Whether an event can change transcript bytes or their names.
///
/// Metadata-only modifications (permissions, ownership, timestamps) are
/// noise to every source reader and must not bypass the fingerprint with a
/// forced sweep. Unknown top-level and modify kinds remain conservative
/// and do wake it: a backend uses those when it cannot classify a real
/// mutation precisely.
pub(super) fn event_can_change_evidence(kind: &EventKind) -> bool {
    matches!(
        kind,
        EventKind::Any | EventKind::Other | EventKind::Create(_) | EventKind::Remove(_)
    ) || matches!(kind, EventKind::Modify(modify) if !matches!(modify, ModifyKind::Metadata(_)))
}

/// Whether this event belongs to the watched evidence set.
///
/// Rescan notices are global by definition: the backend is reporting that
/// paths were lost, and notify emits them with an empty path list. Unknown
/// pathless events are treated the same conservative way; a typed event
/// still has to name a path covered by one of the roots.
#[cfg(test)]
pub(super) fn event_matches_evidence(event: &notify::Event, roots: &[WatchRoot]) -> bool {
    event_scope(event, roots).is_some()
}

/// The roots this event belongs to, or `None` when it belongs to none.
///
/// A rescan notice or an unknown pathless event cannot be placed under a
/// root, so it covers everything; a typed event covers the roots its paths
/// fall under, each named by the root's own path.
pub(super) fn event_scope(event: &notify::Event, roots: &[WatchRoot]) -> Option<ChangeScope> {
    if event.need_rescan()
        || (event.paths.is_empty() && matches!(event.kind, EventKind::Any | EventKind::Other))
    {
        return Some(ChangeScope::Everything);
    }
    let fired: std::collections::BTreeSet<PathBuf> = event
        .paths
        .iter()
        .map(|path| discover::watch_path(path))
        .flat_map(|path| {
            roots
                .iter()
                .filter(move |root| root.covers(&path))
                .map(|root| root.path.clone())
        })
        .collect();
    (!fired.is_empty()).then_some(ChangeScope::Roots(fired))
}

/// Registrations a removal or rename-away invalidated.
///
/// Rename modes preserve path direction: `From` and the first side of
/// `Both` remove the old name, while `To` only introduces a new one and
/// must not retire a live registration. FSEvents reports its one-sided
/// renames as `Any`, so that mode is conservatively source-like when it
/// names the registration itself. A rename below the root never matches
/// [`WatchRoot::registers_at`] and therefore cannot retire the root.
pub(super) fn lost_registration_keys(
    event: &notify::Event,
    roots: &[WatchRoot],
) -> Vec<PathBuf> {
    let paths: &[PathBuf] = match event.kind {
        EventKind::Remove(_) => &event.paths,
        EventKind::Modify(ModifyKind::Name(
            RenameMode::From | RenameMode::Both | RenameMode::Any | RenameMode::Other,
        )) => event.paths.first().map(std::slice::from_ref).unwrap_or(&[]),
        _ => &[],
    };
    paths
        .iter()
        .flat_map(|path| removed_registration_keys(path, roots))
        .collect()
}

pub(super) fn handle_backend_error(
    error: anyhow::Error,
    roots: &Mutex<Vec<WatchRoot>>,
    stale: &Mutex<HashSet<PathBuf>>,
    on_registration_lost: &dyn Fn(),
    on_error: &dyn Fn(anyhow::Error),
) {
    let registration_keys = roots
        .lock()
        .expect("watch roots")
        .iter()
        .map(|root| root.registration_key().to_path_buf())
        .collect::<Vec<_>>();
    if !registration_keys.is_empty() {
        stale
            .lock()
            .expect("stale roots")
            .extend(registration_keys);
        on_registration_lost();
    }
    on_error(error);
}

/// Holds the OS-level watches open; dropping it stops them.
///
/// Roots that did not exist at attach time stay in `pending` rather than
/// being dropped. A watch on a path that is not there yet cannot be
/// registered, and never retrying would leave a provider installed after
/// the loop started permanently uncovered while the loop still reported
/// itself as event-driven.
pub(super) struct FsWatch {
    watcher: notify::RecommendedWatcher,
    /// The roots currently registered, each with the directory object it
    /// was registered against. Kept whole rather than as paths: a
    /// registration that died with its directory has to be *re*-made,
    /// which needs the depth it asked for, and telling a live
    /// registration from a dead one needs the identity.
    watched: Vec<Registered>,
    pending: Vec<WatchRoot>,
    /// The roots that *were* attached and are uncovered now, so they
    /// are being retried on the recovery cadence rather than on the
    /// backstop.
    ///
    /// Per root, not a flag: `pending` also holds roots that have never
    /// existed — `~/.claude/projects` on a machine without Claude — and
    /// those may never attach at all. A flag cleared only when `pending`
    /// empties would hold the short cadence open for the rest of the run
    /// after a single recreate, stat-ing every root four times a second
    /// on exactly the long-lived watch the backstop exists to keep cheap.
    /// Keyed by the root's own path, which is how `adopt` and the
    /// callback's copy identify a root too, and which — unlike the
    /// registration key — does not change under it when a symlinked root
    /// is re-resolved on the way back in.
    recovering: HashSet<PathBuf>,
    /// Registered paths the backend reported as removed, shared with the
    /// event callback. A watch dies with the directory it names, and the
    /// name can come back over a *different* object — or over one the
    /// filesystem gave the same inode number, which is routine when a
    /// directory is deleted and immediately recreated. The event is the
    /// only reliable statement that the registration is gone.
    stale: Arc<Mutex<HashSet<PathBuf>>>,
    /// Every root and the depth it asked for, shared with the event
    /// callback so it can drop what the backend over-delivered.
    depth: Arc<Mutex<Vec<WatchRoot>>>,
}

impl FsWatch {
    /// Whether a registration that once existed is waiting to be re-made.
    pub(super) fn recovering(&self) -> bool {
        !self.recovering.is_empty()
    }

    pub(super) fn watched(&self) -> Vec<PathBuf> {
        self.watched
            .iter()
            .map(|entry| entry.root.path.clone())
            .collect()
    }

    pub(super) fn pending(&self) -> Vec<PathBuf> {
        self.pending.iter().map(|root| root.path.clone()).collect()
    }

    /// Keep the callback's copy of a root in step with the registered
    /// one, so it matches against the same spellings.
    fn remember(&self, root: &WatchRoot) {
        let mut known = self.depth.lock().expect("watch roots");
        match known.iter_mut().find(|seen| seen.path == root.path) {
            Some(seen) => *seen = root.clone(),
            None => known.push(root.clone()),
        }
    }

    /// Take on roots that were not known at startup — a `.trajectories`
    /// directory created in a project an hour into the run. Returns how
    /// many are new, so the caller knows whether to retry attaching.
    pub(super) fn adopt(&mut self, roots: Vec<WatchRoot>) -> usize {
        let mut known = self.depth.lock().expect("watch roots");
        let mut added = 0usize;
        for root in roots {
            if known.iter().any(|seen| seen.path == root.path) {
                continue;
            }
            known.push(root.clone());
            self.pending.push(root);
            added += 1;
        }
        added
    }

    /// Re-register the roots whose directory is no longer the one they
    /// were registered against, and return the ones that have no directory
    /// at all to `pending`.
    ///
    /// A watch is bound to the directory *object*, not to its name. Delete
    /// a watched `~/.codex/sessions` and the kernel drops the watch with
    /// the inode; recreate it and the name is back while the watch is not,
    /// so the loop goes on reporting coverage it does not have and the
    /// next transcript written there wakes nothing. Nothing in `pending`
    /// covers that: those are roots whose *initial* registration failed.
    ///
    /// The identity is what makes this cheap and safe. Re-registering
    /// blindly would be neither: `notify` 8.2 does not replace a live
    /// registration — its FSEvents backend appends the path to the array
    /// it rebuilds the stream from, so a long run accumulates duplicates,
    /// and its inotify backend re-walks the entire tree of a recursive
    /// root with `WalkDir` on every call. Comparing the directory object
    /// against the one registered is a `stat` per root per backstop tick,
    /// and the backend is only touched when the answer changed.
    ///
    /// Returns how many roots changed side, so the caller knows whether to
    /// republish its status.
    pub(super) fn reconcile(&mut self) -> usize {
        let mut changed = 0usize;
        let mut lost = Vec::new();
        let mut resolved = Vec::new();
        let watcher = &mut self.watcher;
        let mut stale = self.stale.lock().expect("stale roots");
        self.watched.retain_mut(|entry| {
            let reported_gone = stale.remove(entry.root.registration_key());
            let current = root_identity(entry.root.registered_path());
            if !reported_gone && current.is_some() && current == entry.identity {
                return true;
            }
            // Best effort: the old watch may already be gone with its
            // directory, and failing to drop it is not a reason to keep
            // claiming it.
            let _ = watcher.unwatch(entry.root.registration_key());
            if current.is_some() && register(watcher, &mut entry.root) {
                // Same name, new directory object: re-registered against
                // the one that is there now, and re-resolved with it — a
                // symlinked root whose target moved is a new spelling as
                // well as a new object.
                entry.identity = root_identity(entry.root.registered_path());
                resolved.push(entry.root.clone());
                changed += 1;
                return true;
            }
            lost.push(entry.root.clone());
            false
        });
        drop(stale);
        for root in resolved {
            self.remember(&root);
        }
        // Anything that falls out of `watched` here was attached a moment
        // ago, which is what makes it a recovery rather than a root that
        // has never existed. `retry_pending` takes each one back out as it
        // re-attaches, so the short cadence lasts exactly as long as the
        // recovery does and not as long as the emptiest root in `pending`.
        for root in lost {
            self.recovering.insert(root.path.clone());
            self.pending.push(root);
            changed += 1;
        }
        let attached = self.retry_pending();
        changed + attached
    }

    /// Try the roots that were not there before. Returns how many were
    /// picked up, so the caller knows whether to republish its status.
    pub(super) fn retry_pending(&mut self) -> usize {
        if self.pending.is_empty() {
            return 0;
        }
        let mut attached = 0usize;
        let watcher = &mut self.watcher;
        let watched = &mut self.watched;
        let recovering = &mut self.recovering;
        let mut resolved = Vec::new();
        self.pending.retain_mut(|root| {
            if !register(watcher, root) {
                return true;
            }
            recovering.remove(&root.path);
            resolved.push(root.clone());
            watched.push(Registered {
                identity: root_identity(root.registered_path()),
                root: root.clone(),
            });
            attached += 1;
            false
        });
        for root in resolved {
            self.remember(&root);
        }
        attached
    }
}

/// Watch every existing path in `roots`, calling `on_change` for each
/// create/modify/remove event.
///
/// Depth comes from each root: a transcript tree is recursive because a
/// new session is a new file somewhere under it, while a directory holding
/// one flat log is not, so the unrelated churn beside it costs nothing.
///
/// Errors only when the backend itself could not be brought up. A root
/// that does not exist is not an error — it becomes `pending`.
pub(super) fn attach(
    roots: &[WatchRoot],
    on_change: impl Fn(ChangeScope) + Send + 'static,
    on_registration_lost: impl Fn() + Send + 'static,
    on_error: impl Fn(anyhow::Error) + Send + 'static,
) -> Result<FsWatch> {
    // The callback has to be able to see the roots to enforce their depth,
    // and `retry_pending` adds to that set later, so it is shared rather
    // than captured by value.
    let depth: Arc<Mutex<Vec<WatchRoot>>> = Arc::new(Mutex::new(roots.to_vec()));
    // Replaced below with the resolved roots, once each has been asked.
    let depth_for_events = depth.clone();
    let stale: Arc<Mutex<HashSet<PathBuf>>> = Arc::new(Mutex::new(HashSet::new()));
    let stale_for_events = stale.clone();
    let mut watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
        let event = match event {
            Ok(event) => event,
            Err(error) => {
                // A backend error means its coverage can no longer be
                // trusted, even when it cannot name the failed root. Mark
                // every registration stale and wake reconciliation; the
                // polling backstop remains active while reattachment is
                // attempted on the short recovery cadence.
                handle_backend_error(
                    error.into(),
                    &depth_for_events,
                    &stale_for_events,
                    &on_registration_lost,
                    &on_error,
                );
                return;
            }
        };
        // Metadata churn — an atime bump from a backup or an antivirus
        // scan — does not change the bytes a sweep would read. Filtering
        // here keeps wakeups honest on a noisy home directory.
        if !event.need_rescan() && !event_can_change_evidence(&event.kind) {
            return;
        }
        let (matched, removed) = {
            let roots = depth_for_events.lock().expect("watch roots");
            let matched = event_scope(&event, &roots);
            // A registered path that was removed takes its watch with it,
            // whatever the name does afterwards. Recorded here because the
            // backend will not say so again, and a `stat` later cannot
            // tell a recreated directory from the original one when the
            // filesystem reuses the inode number.
            let removed = lost_registration_keys(&event, &roots);
            (matched, removed)
        };
        if !removed.is_empty() {
            {
                let mut stale = stale_for_events.lock().expect("stale roots");
                stale.extend(removed);
            }
            // Recorded *and* announced. Recording alone leaves the
            // registration dead until the next backstop, and for a
            // directory that is recreated straight away that window is
            // long enough to miss a whole session.
            on_registration_lost();
        }
        if let Some(scope) = matched {
            on_change(scope);
        }
    })?;
    let mut watched = Vec::new();
    let mut pending = Vec::new();
    let mut known = Vec::new();
    for root in roots {
        let mut root = root.clone();
        if register(&mut watcher, &mut root) {
            known.push(root.clone());
            watched.push(Registered {
                identity: root_identity(root.registered_path()),
                root,
            });
        } else {
            known.push(root.clone());
            pending.push(root);
        }
    }
    // The callback matches against these, so it has to see the resolved
    // spellings rather than the ones the caller handed in.
    *depth.lock().expect("watch roots") = known;
    Ok(FsWatch {
        watcher,
        watched,
        pending,
        recovering: HashSet::new(),
        stale,
        depth,
    })
}

/// One registration: the root, and the directory object it was made
/// against.
pub(super) struct Registered {
    pub(super) root: WatchRoot,
    identity: Option<RootIdentity>,
}

/// Which directory object a *name* currently refers to.
///
/// Always asked through the lexical path, never through the resolved one,
/// because the two answer different questions and only this one notices a
/// symlink being retargeted. `~/sessions -> /disk-a/sessions` resolves to
/// `/disk-a/sessions` and registers there; repoint it at `/disk-b` and
/// `/disk-a` still exists with the same inode, so stat-ing the resolved
/// path says nothing changed while the name now means somewhere else
/// entirely. Stat-ing the name follows the link as it is *now*, so the
/// identity moves and the root is re-resolved and re-registered.
///
/// The resolved spelling remains the key for the stale set and for
/// `unwatch`: those are about which registration this is, which is a
/// different question from whether the name still points at it.
///
/// On Unix that is exactly `(device, inode)` — the pair a watch is bound
/// to. Elsewhere it is the creation time, which changes when a directory
/// is replaced but is a weaker statement; the cost of the difference is a
/// missed re-registration in a case the platform cannot distinguish, not a
/// wrong one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RootIdentity {
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(not(unix))]
    created: Option<std::time::SystemTime>,
}

fn root_identity(path: &Path) -> Option<RootIdentity> {
    let metadata = std::fs::metadata(path).ok()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Some(RootIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
    #[cfg(not(unix))]
    {
        Some(RootIdentity {
            created: metadata.created().ok(),
        })
    }
}

/// Successful backend registrations since the process started.
///
/// The count is the only way to state the property that matters here from
/// outside: a live root must not be registered again on every backstop
/// tick.
#[cfg(test)]
pub(super) fn registrations() -> usize {
    REGISTRATIONS.load(std::sync::atomic::Ordering::Relaxed)
}

static REGISTRATIONS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

fn register(watcher: &mut notify::RecommendedWatcher, root: &mut WatchRoot) -> bool {
    // Ask the filesystem for the root's real spelling first, and register
    // *that*. Both backends then report paths this root recognises: it
    // keeps the spelling it was given as well, because inotify echoes
    // whichever path the watch was registered with while FSEvents always
    // reports the resolved one. One call per registration, and none per
    // event.
    root.resolve();
    // A file root registers its parent, so it is the parent's existence
    // that decides whether the root is coverable yet — and a file that
    // does not exist inside a directory that does is covered from the
    // start, which is the point of watching the parent.
    let target = root.registration_key();
    if !target.exists() {
        return false;
    }
    let mode = match root.depth {
        WatchDepth::Tree => RecursiveMode::Recursive,
        WatchDepth::Directory | WatchDepth::File => RecursiveMode::NonRecursive,
    };
    let registered = watcher.watch(target, mode).is_ok();
    if registered {
        REGISTRATIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    registered
}
