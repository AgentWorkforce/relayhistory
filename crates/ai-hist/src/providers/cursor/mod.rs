//! Existing cursor registration; parser behavior is unchanged.
use crate::discover::CursorProvider;
use crate::hydrate;
use crate::sources::catalog::*;

pub(crate) const LOCAL_SOURCE: LocalSource = LocalSource {
    id: "cursor",
    discovery: Discovery::Provider(|| Box::new(CursorProvider)),
    hydration: Hydration::Parser(hydrate::ingest_selected_cursor),
    transcript_roots: |roots| vec![roots.home.join(".cursor/projects")],
    relationships: Relationships::NONE,
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
