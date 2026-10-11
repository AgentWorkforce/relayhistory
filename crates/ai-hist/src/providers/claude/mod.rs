//! Existing claude registration; parser behavior is unchanged.
use crate::discover::ClaudeProvider;
use crate::hydrate;
use crate::sources::catalog::*;

pub(crate) const LOCAL_SOURCE: LocalSource = LocalSource {
    id: "claude",
    discovery: Discovery::Provider(|| Box::new(ClaudeProvider)),
    hydration: Hydration::Parser(hydrate::ingest_selected_claude),
    transcript_roots: |roots| vec![roots.claude.join("projects")],
    // Claude subagent transcripts carry the parent's `sessionId`; only
    // provider versions that also emit a per-child `agentId` give the
    // child a stable identity.
    relationships: Relationships {
        stable_child_identity: "sometimes",
        records_agent_type: true,
        records_spawn_time: true,
        records_evidence_locator: true,
    },
    resume: Some(Resume {
        command: |sid| format!("claude --resume {sid}"),
        in_project: true,
    }),
    fixtures: Fixtures::Dir("claude"),
};

pub(crate) const METADATA: super::ProviderMetadata = super::ProviderMetadata {
    markers: true,
    message_ids: crate::MessageIdOrigin::Provider,
    usage: Some(super::UsageRegistration {
        accounting: crate::UsageAccounting::PerMessage,
        normalize: |value| crate::usage::normalize_legacy_usage("claude", value),
        splits_requests: true,
    }),
};
pub(crate) const ADAPTER: Option<super::ProviderAdapter> = None;
