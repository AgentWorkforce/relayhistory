//! One watch loop's sweep, and what it knows about the roots it watches.
//!
//! Every filesystem-event tick of [`SessionStore::watch`] used to be a full
//! forced sweep. On a machine with one live Claude session that meant every
//! provider walked, and `~/Projects` walked twice for trajectory roots, every
//! couple of seconds. The loop now says which roots fired
//! ([`ChangeScope`]); [`WatchSources`] maps each root back to the sources
//! that registered it, so the tick sweeps only those, and keeps the
//! trajectory roots last derived so an event tick never walks `~/Projects`
//! at all. Startup, backstop and manual ticks — and any event the loop could
//! not place — stay full sweeps. The trajectory roots are derived once per
//! backstop, by the root refresh that registers them, and the backstop's
//! sweep reuses that walk.

use super::*;
use crate::discover::WatchRoot;
use crate::ingest::{sync_watch_roots_by_source, trajectory_roots, SweepScope};
use crate::watch::{ChangeScope, TickRequest};
use std::collections::{BTreeSet, HashMap};

/// Which sources registered each watched root, and the trajectory roots the
/// last full sweep found.
pub(super) struct WatchSources {
    roots: ProviderRoots,
    state: Mutex<WatchSourcesState>,
}

#[derive(Default)]
struct WatchSourcesState {
    /// The derived trajectory roots, when the provider roots name none
    /// outright (`TRAJECTORY_ROOT` unset). `None` until first derived.
    trajectories: Option<Vec<PathBuf>>,
    /// Derived since the last full sweep took them — at startup, or by the
    /// backstop refresh just ahead of a backstop sweep — so that sweep need
    /// not walk again.
    fresh: bool,
    /// Every watched root's path, and the sources whose roots it is.
    by_root: HashMap<PathBuf, BTreeSet<Source>>,
}

/// What one tick sweeps: the provider roots to read and the sources to read
/// them for (`None` for every source).
pub(super) struct SweepPlan {
    pub(super) roots: ProviderRoots,
    pub(super) sources: Option<Vec<Source>>,
}

impl SweepPlan {
    pub(super) fn scope(&self) -> SweepScope {
        match &self.sources {
            None => SweepScope::everything(),
            Some(sources) => SweepScope::only(sources.iter().map(|source| source.as_str())),
        }
    }
}

impl WatchSources {
    pub(super) fn new(roots: ProviderRoots) -> Self {
        Self {
            roots,
            state: Mutex::new(WatchSourcesState::default()),
        }
    }

    /// Every root the loop should watch. Called at startup and on every
    /// backstop refresh, just ahead of that backstop's full sweep, so it
    /// re-derives the trajectory roots here — a `.trajectories` directory
    /// created since the last backstop is registered now, not one backstop
    /// later — and leaves them fresh for that sweep to reuse rather than
    /// walk again. It also brings the root-to-source map up to date with
    /// exactly the roots the loop is about to adopt.
    pub(super) fn watch_roots(&self) -> Vec<WatchRoot> {
        let mut state = self.state.lock().expect("watch sources");
        if self.roots.trajectory_roots.is_none() {
            // A walk that fails keeps the roots the last one found.
            if let Ok(found) = trajectory_roots(&self.roots) {
                state.trajectories = Some(found);
                state.fresh = true;
            }
        }
        let by_source = sync_watch_roots_by_source(&pinned(&self.roots, &state.trajectories));
        state.by_root.clear();
        for (name, roots) in &by_source {
            let Some(source) = Source::parse(name) else {
                continue;
            };
            for root in roots {
                state
                    .by_root
                    .entry(root.path.clone())
                    .or_default()
                    .insert(source);
            }
        }
        crate::ingest::merge_watch_roots(
            by_source.into_iter().flat_map(|(_, roots)| roots).collect(),
        )
    }

    /// What `request` sweeps. A forced tick whose every root is known sweeps
    /// only the sources those roots belong to, from the trajectory roots
    /// already known; anything else is a full sweep, which re-derives the
    /// trajectory roots unless a refresh derived them since the last one.
    pub(super) fn plan(&self, request: &TickRequest) -> SweepPlan {
        let mut state = self.state.lock().expect("watch sources");
        let sources = match &request.scope {
            ChangeScope::Roots(fired) if request.force => sources_of(&state.by_root, fired),
            _ => None,
        };
        if self.roots.trajectory_roots.is_some() {
            return SweepPlan {
                roots: self.roots.clone(),
                sources,
            };
        }
        let rewalk = match &sources {
            None => !std::mem::take(&mut state.fresh),
            Some(sources) => sources.contains(&Source::Trajectory) && state.trajectories.is_none(),
        };
        if rewalk {
            // A walk that fails leaves the sweep to walk for itself, and to
            // report the failure through its trajectory phase.
            state.trajectories = trajectory_roots(&self.roots).ok();
        }
        SweepPlan {
            roots: pinned(&self.roots, &state.trajectories),
            sources,
        }
    }
}

/// `roots` reading `trajectories` as its explicit trajectory roots, when
/// there are some to read.
fn pinned(roots: &ProviderRoots, trajectories: &Option<Vec<PathBuf>>) -> ProviderRoots {
    let mut roots = roots.clone();
    if roots.trajectory_roots.is_none() {
        roots.trajectory_roots.clone_from(trajectories);
    }
    roots
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
    pub(super) baseline: Mutex<Option<CatalogFingerprint>>,
    pub(super) notes: Arc<Mutex<TickNotes>>,
    pub(super) reports: ReportSlot,
}

impl WatchSweep {
    pub(super) fn tick(&self, request: &TickRequest) -> anyhow::Result<TickOutcome> {
        let plan = self.sources.plan(request);
        self.notes.lock().expect("watch notes").sources = plan.sources.clone();
        // Both reads happen under the sync lock, like `sync`'s. The
        // baseline is the previous swept tick's `after` digest when there
        // is one — nothing this loop reported has moved since, and a
        // change another process made in between is a change since the
        // last report either way — and a fresh read otherwise.
        let mut base = self.baseline.lock().expect("watch baseline");
        // The baseline this sweep compares against, kept outside it so a
        // cancelled sweep can still say what it committed (below).
        let mut swept_from: Option<CatalogFingerprint> = None;
        let result = with_capture_token(self.stop.clone(), || {
            rolling_tick(&mut base, |previous| {
                self.sweep(request.force, &plan, previous, &mut swept_from)
            })
        });
        match result {
            Ok((outcome, changed)) => {
                self.notes.lock().expect("watch notes").changed = changed;
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

    fn sweep(
        &self,
        force: bool,
        plan: &SweepPlan,
        previous: Option<CatalogFingerprint>,
        swept_from: &mut Option<CatalogFingerprint>,
    ) -> anyhow::Result<Option<(SyncTick, CatalogFingerprint, Vec<SessionRef>)>> {
        let outcome = sync_facade_tick(
            &self.db_path,
            &plan.roots,
            force,
            &plan.scope(),
            |conn| {
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
            |conn, before, _tick| {
                let (after, changed) = changes_under_lock(conn, &before)?;
                Ok((after, changed))
            },
        )?;
        Ok(outcome.map(|(tick, (after, changed))| (tick, after, changed)))
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
                self.notes.lock().expect("watch notes").changed = changed;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::trajectory_walks;
    use crate::watch::TickTrigger;

    fn home_with_trajectories() -> (tempfile::TempDir, ProviderRoots) {
        let home = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(home.path().join("Projects/repo/.trajectories")).unwrap();
        std::fs::create_dir_all(home.path().join(".claude/projects")).unwrap();
        let roots =
            ProviderRoots::from_home(home.path().to_path_buf(), home.path().join("opencode.db"));
        (home, roots)
    }

    fn event(roots: &[PathBuf]) -> TickRequest {
        TickRequest {
            trigger: TickTrigger::FsEvent,
            force: true,
            scope: ChangeScope::Roots(roots.iter().cloned().collect()),
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
        let (home, roots) = home_with_trajectories();
        let sources = WatchSources::new(roots);
        sources.watch_roots();
        let claude = home.path().join(".claude/projects");
        let trajectory = home.path().join("Projects/repo/.trajectories");

        assert_eq!(
            sources.plan(&event(std::slice::from_ref(&claude))).sources,
            Some(vec![Source::Claude])
        );
        assert_eq!(
            sources.plan(&event(&[claude.clone(), trajectory])).sources,
            Some(vec![Source::Claude, Source::Trajectory])
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

    /// The trajectory roots are walked for once when the watch starts and
    /// once per backstop, by the root refresh, whose walk the sweep right
    /// after it reuses. An event tick — Claude's or a trajectory root's —
    /// reads the roots the last walk found and never walks.
    #[test]
    fn only_a_backstop_walks_projects_for_trajectory_roots() {
        let (home, roots) = home_with_trajectories();
        let sources = WatchSources::new(roots);
        let claude = home.path().join(".claude/projects");
        let trajectory = home.path().join("Projects/repo/.trajectories");

        let walks = trajectory_walks();
        sources.watch_roots();
        assert_eq!(trajectory_walks() - walks, 1, "the watch's own derivation");
        let startup = sources.plan(&full(TickTrigger::Startup));
        assert_eq!(trajectory_walks() - walks, 1, "served by the startup walk");
        assert_eq!(
            startup.roots.trajectory_roots,
            Some(vec![trajectory.clone()])
        );

        let claude_tick = sources.plan(&event(std::slice::from_ref(&claude)));
        let trajectory_tick = sources.plan(&event(std::slice::from_ref(&trajectory)));
        assert_eq!(trajectory_walks() - walks, 1, "no event tick walked");
        assert_eq!(
            trajectory_tick.roots.trajectory_roots,
            Some(vec![trajectory])
        );
        assert_eq!(claude_tick.sources, Some(vec![Source::Claude]));

        // A backstop: the refresh walks, the sweep after it reuses the walk.
        sources.watch_roots();
        let backstop = sources.plan(&full(TickTrigger::Poll));
        assert_eq!(trajectory_walks() - walks, 2, "one walk per backstop");
        assert_eq!(backstop.scope(), SweepScope::everything());
        // A full sweep with no refresh before it walks for itself.
        sources.plan(&full(TickTrigger::Manual));
        assert_eq!(trajectory_walks() - walks, 3);
    }

    /// A `.trajectories` directory created after the watch started is
    /// registered by the next backstop refresh, and its events are scoped to
    /// the trajectory source, rather than waiting a backstop more.
    #[test]
    fn a_new_trajectory_store_is_registered_by_the_next_refresh() {
        let (home, roots) = home_with_trajectories();
        let sources = WatchSources::new(roots);
        sources.watch_roots();
        sources.plan(&full(TickTrigger::Startup));
        let created = home.path().join("Projects/other/.trajectories");
        std::fs::create_dir_all(&created).unwrap();

        let registered = sources.watch_roots();
        assert!(
            registered.iter().any(|root| root.path == created),
            "the refresh registers the new store"
        );
        assert_eq!(
            sources.plan(&event(std::slice::from_ref(&created))).sources,
            Some(vec![Source::Trajectory])
        );
        let backstop = sources.plan(&full(TickTrigger::Poll));
        assert!(backstop
            .roots
            .trajectory_roots
            .is_some_and(|roots| roots.contains(&created)));
    }

    #[test]
    fn explicit_trajectory_roots_are_never_walked() {
        let (home, mut roots) = home_with_trajectories();
        let named = home.path().join("named/.trajectories");
        roots.trajectory_roots = Some(vec![named.clone()]);
        let sources = WatchSources::new(roots);
        let walks = trajectory_walks();
        sources.watch_roots();
        let plan = sources.plan(&full(TickTrigger::Poll));
        assert_eq!(trajectory_walks(), walks);
        assert_eq!(plan.roots.trajectory_roots, Some(vec![named]));
    }
}
