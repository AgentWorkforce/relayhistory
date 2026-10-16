//! Existing codex registration; parser behavior is unchanged.
use crate::discover::CodexProvider;
use crate::hydrate;
use crate::sources::catalog::*;
pub(crate) mod sqlite;

pub(crate) const LOCAL_SOURCE: LocalSource = LocalSource {
    id: "codex",
    discovery: Discovery::Provider(|| Box::new(CodexProvider::default())),
    hydration: Hydration::Parser(hydrate::ingest_selected_codex),
    transcript_roots: |roots| {
        let mut paths = vec![
            roots.codex.join("sessions"),
            roots.codex.join("archived_sessions"),
        ];
        for store in ["state_5.sqlite", "thread_history_1.sqlite"] {
            for suffix in ["", "-wal", "-shm", "-journal"] {
                paths.push(roots.codex.join(format!("{store}{suffix}")));
            }
        }
        paths
    },
    // Every Codex subagent rollout opens with its own thread id, and its
    // `session_meta` names the subagent type.
    relationships: Relationships {
        stable_child_identity: "always",
        records_agent_type: true,
        records_spawn_time: true,
        records_evidence_locator: true,
    },
    resume: Some(Resume {
        command: |sid| format!("codex resume {sid}"),
        in_project: false,
    }),
    fixtures: Fixtures::Dir("codex"),
};

pub(crate) const METADATA: super::ProviderMetadata = super::ProviderMetadata {
    markers: true,
    message_ids: crate::MessageIdOrigin::Synthesized,
    usage: Some(super::UsageRegistration {
        accounting: crate::UsageAccounting::CumulativeDelta,
        normalize: |value| crate::usage::normalize_legacy_usage("codex", value),
        splits_requests: false,
    }),
};
pub(crate) const ADAPTER: Option<super::ProviderAdapter> = None;
