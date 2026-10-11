//! Existing devin registration; parser behavior is unchanged.
use crate::discover::DevinProvider;
use crate::hydrate;
use crate::sources::catalog::*;

pub(crate) const LOCAL_SOURCE: LocalSource = LocalSource {
    id: "devin",
    discovery: Discovery::Provider(|| Box::new(DevinProvider::default())),
    hydration: Hydration::Parser(hydrate::ingest_selected_devin),
    // Devin locators are validated against the configured `sessions.db`
    // in `source_snapshot`, not against a transcript root.
    transcript_roots: |_| Vec::new(),
    // Devin records no parent/child session relationships.
    relationships: Relationships::NONE,
    resume: None,
    fixtures: Fixtures::Dir("devin"),
};

pub(crate) const METADATA: super::ProviderMetadata = super::ProviderMetadata {
    markers: true,
    message_ids: crate::MessageIdOrigin::Mixed,
    usage: None,
};
pub(crate) const ADAPTER: Option<super::ProviderAdapter> = None;
