//! Path-scoped shallow discovery through the shared provider implementation.
use super::*;

impl SessionStore {
    pub(super) fn discover_now(&self, opts: &DiscoveryOptions) -> Result<DiscoveryReport, Error> {
        let mut opts = opts.clone();
        opts.sources =
            path_scope::selected_sources(&self.roots, opts.paths.as_deref(), opts.sources);
        if opts.reads_nothing() {
            return Ok(DiscoveryReport::default());
        }
        let conn =
            open_db(&self.db_path).map_err(|error| Error::DatabaseOpen(format!("{error:#}")))?;
        let env =
            discover::DiscoveryEnv::with_provider_roots(&conn, self.roots.clone()).with_path_scope(
                path_scope::scope(&self.roots, opts.paths.as_deref(), opts.sources.as_deref()),
            );
        let options = opts.discover_options();
        let summary = discover::discover_sessions_with_env(&env, &options, |_| {})
            .map_err(|error| Error::classify(error, Error::Discovery))?;
        Ok(DiscoveryReport {
            discovered: summary.discovered,
            skipped_unchanged: summary.skipped_unchanged,
            diagnostics: summary
                .diagnostics
                .into_iter()
                .map(|diagnostic| Diagnostic {
                    code: "DISCOVERY_FAILED".into(),
                    message: format!("{}: {}", diagnostic.source, diagnostic.error),
                    subject: diagnostic.locator,
                })
                .collect(),
        })
    }
}
