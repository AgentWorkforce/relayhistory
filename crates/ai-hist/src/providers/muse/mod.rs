//! Existing muse registration; parser behavior is unchanged.
use crate::discover::MuseProvider;
use crate::hydrate;
use crate::sources::catalog::*;

pub(crate) const LOCAL_SOURCE: LocalSource = LocalSource {
    id: "muse",
    discovery: Discovery::Provider(|| Box::new(MuseProvider)),
    hydration: Hydration::Parser(hydrate::ingest_selected_muse),
    transcript_roots: |roots| vec![roots.muse.clone()],
    // A Muse subagent writes its own `subagent/<id>/session.jsonl`, whose
    // metadata record names the child session, so a linked child's
    // identity is always its own. The parent's `task_stream_linked`
    // names its role, label and model.
    relationships: Relationships {
        stable_child_identity: "always",
        records_agent_type: true,
        records_spawn_time: true,
        records_evidence_locator: true,
    },
    // `muse resume <id>` finds the session by id; the `cd` puts the
    // resumed agent back in the workspace it recorded.
    resume: Some(Resume {
        command: |sid| format!("muse resume {sid}"),
        in_project: true,
    }),
    fixtures: Fixtures::Dir("muse"),
};

pub(crate) const METADATA: super::ProviderMetadata = super::ProviderMetadata {
    markers: true,
    message_ids: crate::MessageIdOrigin::Mixed,
    usage: Some(super::UsageRegistration {
        accounting: crate::UsageAccounting::PerRequest,
        normalize: |value| crate::usage::normalize_legacy_usage("muse", value),
        splits_requests: false,
    }),
};
pub(crate) const ADAPTER: Option<super::ProviderAdapter> = None;
