//! Separate whole-catalog maintenance from exact capture observations.
use super::*;

/// Who brings project identity up to date around a discovery pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum IdentityRefresh {
    /// Rows are streamed to the caller. Each cached row is upgraded before it
    /// is streamed (see [`upgrade_cached_project_identity`]), and the pass
    /// ends with the incremental catalog refresh.
    Streamed,
    /// Admitted rows and ancestors observe fresh filesystem state; only
    /// revision-scoped inheritance/event maintenance runs at the tail.
    ExactPaths,
    /// The sweep: no row is streamed, and the sweep runs the refresh itself
    /// right after the pass. That refresh stores exactly the key the per-row
    /// upgrade would have -- the upgrade exists only so a *streamed* row
    /// agrees with it -- so upgrading every cached row here as well was the
    /// same resolution and ancestor walk twice, once per candidate file, on
    /// every tick (#318).
    Sweep,
}

impl IdentityRefresh {
    pub(super) fn refresh(self, conn: &Connection) {
        let result = match self {
            Self::Sweep => return,
            Self::Streamed => crate::store::refresh_project_identity_incrementally(conn),
            Self::ExactPaths => crate::store::refresh_project_identity_observations(conn, false),
        };
        if let Err(error) = result {
            eprintln!(
                "ai-hist: could not refresh canonical project identity after discovery: {error:#} \
                 (project keys stay as they were; the next pass retries)"
            );
        }
    }
}

/// Private exact-capture policy. General discovery retains global promotion,
/// even when it happens to carry a path filter.
pub(crate) fn discover_sessions_for_capture(
    env: &DiscoveryEnv<'_>,
    options: &DiscoverOptions,
    on_row: impl FnMut(&ShallowSession),
) -> Result<DiscoverySummary> {
    let providers = select_providers(
        options,
        &env.home,
        &crate::remote::SourceConnectorSelection::default(),
    )?;
    let providers: Vec<_> = providers.iter().map(|provider| provider.as_ref()).collect();
    let workers = std::thread::available_parallelism().map_or(1, |n| n.get());
    discover_sessions_with_worker_limit(env, options, &providers, on_row, workers, IdentityRefresh::ExactPaths)
}
