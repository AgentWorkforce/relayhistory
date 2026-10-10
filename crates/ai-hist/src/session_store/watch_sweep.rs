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
use crate::ingest::{sync_watch_roots_by_source, SweepScope};
use crate::watch::{ChangeScope, TickRequest};
use std::collections::{BTreeSet, HashMap};

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
            by_root: Mutex::new(HashMap::new()),
        }
    }

    /// Every root the loop should watch. Called at startup and on every
    /// backstop refresh, so it also brings the root-to-source map up to date
    /// with exactly the roots the loop is about to adopt.
    pub(super) fn watch_roots(&self) -> Vec<WatchRoot> {
        let by_source = sync_watch_roots_by_source(&self.roots);
        let mut by_root = self.by_root.lock().expect("watch sources");
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
        let sources = match &request.scope {
            ChangeScope::Roots(fired) if request.force => {
                sources_of(&self.by_root.lock().expect("watch sources"), fired)
            }
            _ => None,
        };
        SweepPlan {
            roots: self.roots.clone(),
            sources,
        }
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
}
