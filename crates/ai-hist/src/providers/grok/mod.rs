//! Existing grok registration; parser behavior is unchanged.
use crate::discover::GrokProvider;
use crate::hydrate;
use crate::sources::catalog::*;

pub(crate) const LOCAL_SOURCE: LocalSource = LocalSource {
    id: "grok",
    discovery: Discovery::Provider(|| Box::new(GrokProvider)),
    hydration: Hydration::Parser(hydrate::ingest_selected_grok),
    transcript_roots: |roots| vec![roots.grok.join("sessions")],
    // Grok records a delegation in two places. The `Task` call in the
    // transcript names no child at all; a `subagents/` metadata entry does
    // when it carries a session id, and the child session then lives in
    // the normal sessions tree. The id is never taken from the file name,
    // so an entry without one stays unlinked evidence.
    relationships: Relationships {
        stable_child_identity: "sometimes",
        records_agent_type: true,
        records_spawn_time: true,
        records_evidence_locator: true,
    },
    resume: Some(Resume {
        command: |sid| format!("grok resume {sid}"),
        in_project: true,
    }),
    fixtures: Fixtures::Dir("grok"),
};

pub(crate) const METADATA: super::ProviderMetadata = super::ProviderMetadata {
    markers: true,
    message_ids: crate::MessageIdOrigin::Synthesized,
    usage: Some(super::UsageRegistration {
        accounting: crate::UsageAccounting::PerRequest,
        normalize: |value| crate::usage::normalize_legacy_usage("grok", value),
        splits_requests: false,
    }),
};
pub(crate) const ADAPTER: Option<super::ProviderAdapter> = None;
