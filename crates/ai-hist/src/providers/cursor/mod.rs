//! Existing cursor registration; parser behavior is unchanged.
use crate::discover::CursorProvider;
use crate::hydrate;
use crate::sources::catalog::*;

pub(crate) const LOCAL_SOURCE: LocalSource = LocalSource {
    id: "cursor",
    discovery: Discovery::Provider(|| Box::new(CursorProvider)),
    hydration: Hydration::Parser(hydrate::ingest_selected_cursor),
    transcript_roots: |roots| vec![roots.home.join(".cursor/projects")],
    // A subagent transcript is filed at `<parent>/subagents/<child>.jsonl`,
    // which names both ends; nothing in it records the agent type or spawn.
    relationships: Relationships {
        stable_child_identity: "always",
        records_agent_type: false,
        records_spawn_time: false,
        records_evidence_locator: true,
    },
    resume: Some(Resume {
        command: |sid| format!("cursor-agent --resume={sid}"),
        in_project: true,
    }),
    fixtures: Fixtures::Dir("cursor"),
};

pub(crate) const METADATA: super::ProviderMetadata = super::ProviderMetadata {
    markers: false,
    message_ids: crate::MessageIdOrigin::Mixed,
    usage: None,
};
pub(crate) const ADAPTER: Option<super::ProviderAdapter> = None;
