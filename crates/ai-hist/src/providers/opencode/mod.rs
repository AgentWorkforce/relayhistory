//! Existing opencode registration; parser behavior is unchanged.
use crate::discover::OpencodeProvider;
use crate::hydrate;
use crate::sources::catalog::*;

pub(crate) const LOCAL_SOURCE: LocalSource = LocalSource {
    id: "opencode",
    discovery: Discovery::Provider(|| Box::new(OpencodeProvider::default())),
    hydration: Hydration::Parser(hydrate::ingest_selected_opencode),
    // OpenCode locators are validated against the configured store in
    // `source_snapshot`, not against a transcript root.
    transcript_roots: |_| Vec::new(),
    // An OpenCode subagent session is a session in its own right and its
    // record names the parent outright, in `session.parentID`, so the
    // child identity is always stable and never inferred. But OpenCode
    // records no *type* for that child: the parser writes the spawn time
    // and the evidence locator, and leaves `child_agent_type` and
    // `child_agent_name` null because nothing in the provider's records
    // supplies them.
    relationships: Relationships {
        stable_child_identity: "always",
        records_agent_type: false,
        records_spawn_time: true,
        records_evidence_locator: true,
    },
    resume: None,
    fixtures: Fixtures::Dir("opencode"),
};

pub(crate) const METADATA: super::ProviderMetadata = super::ProviderMetadata {
    markers: true,
    message_ids: crate::MessageIdOrigin::Provider,
    usage: None,
};
pub(crate) const ADAPTER: Option<super::ProviderAdapter> = None;
