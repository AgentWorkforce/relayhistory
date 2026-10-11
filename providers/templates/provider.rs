//! Copy to crates/ai-hist/src/providers/<rust_id>/mod.rs; fill every TODO
//! before admission. The executable test_fixture module demonstrates this API.
use super::{AcquiredSession, AcquisitionContext, ProviderAdapter, ProviderMetadata, SweepContext, SweepResult};
use crate::sources::catalog::{Discovery, Fixtures, Hydration, LocalSource, Relationships};
use crate::hydrate::{HydrateSessionOptions, SelectedIngest, SelectedIngestResult};
use crate::{MessageIdOrigin, ProviderRoots};
use anyhow::Result;
use std::path::{Path, PathBuf};

const ID: &str = "replace-me";
pub(crate) const LOCAL_SOURCE: LocalSource = LocalSource {
    id: ID,
    discovery: Discovery::Provider(|| todo!("return your ShallowSessionProvider")),
    hydration: Hydration::Parser(ingest),
    transcript_roots: |roots| roots.for_provider(ID).to_vec(),
    relationships: Relationships::NONE,
    resume: None,
    fixtures: Fixtures::Dir(ID),
};
pub(crate) const METADATA: ProviderMetadata = ProviderMetadata {
    markers: false,
    message_ids: MessageIdOrigin::Mixed, // Replace with fixture-proven provenance.
    usage: None, // Supply UsageRegistration only with measured normalization tests.
};
pub(crate) const ADAPTER: Option<ProviderAdapter> = Some(ProviderAdapter {
    generation: "replace_me_v1",
    acquire,
    sweep,
    resolve_roots,
    watch_roots: |roots| roots.for_provider(ID).iter().cloned().map(crate::discover::WatchRoot::tree).collect(),
});
fn resolve_roots(home: &Path, use_env: bool) -> Vec<PathBuf> {
    vec![super::resolve_root(home, use_env, "REPLACE_ME_HOME", ".replace-me")]
}
fn acquire(_options: &HydrateSessionOptions, _path: &Path, _context: &AcquisitionContext<'_>) -> Result<AcquiredSession> {
    todo!("validate and read through context; verify native session identity; capture immutable data")
}
fn ingest(_context: SelectedIngest<'_>) -> Result<SelectedIngestResult> {
    todo!("normalize context.provider_session payload using existing store writers; never reopen source")
}
fn sweep(_context: SweepContext<'_>) -> Result<SweepResult> {
    todo!("same normalizer, atomic writes, honor context.paths and repairs, report partial failures")
}
