//! The harness registry: one [`LocalSource`] descriptor per built-in source.
//!
//! Everything that used to be a per-source list or `match` elsewhere in the
//! crate is derived from [`LOCAL_SOURCES`]:
//!
//! - `SOURCE_CHOICES` is the descriptor ids, in declaration order;
//! - `shallow_providers()` and `DISCOVERY_EXEMPTIONS` come from
//!   [`LocalSource::discovery`], and so does the declared evidence coverage
//!   (`declared_evidence_kinds`), which the provider reports;
//! - hydration's source validation and `ingest_selected` dispatch come from
//!   [`LocalSource::hydration`], and `validate_provider_path` from
//!   [`LocalSource::transcript_roots`];
//! - `relationship_capabilities` comes from [`LocalSource::relationships`];
//! - `resume_command` comes from [`LocalSource::resume`];
//! - the fixture-corpus and registry tests read [`LocalSource::fixtures`].
//!
//! Adding a built-in harness means adding one entry here plus its fixture
//! directory; see `docs/session-catalog.md`, "Adding a provider". Harnesses
//! that live outside this crate register through
//! [`super::SourceRegistry::register`] (Rust) or a local source plugin (JS)
//! instead, and do not appear here.

// The accessors are read by the workspace crates and tests, which enable
// `unstable-internal`; without it some are unused by design.
#![cfg_attr(not(feature = "unstable-internal"), allow(dead_code))]

use crate::discover::{
    ClaudeProvider, CodexProvider, CursorProvider, GrokProvider, MuseProvider, OpencodeProvider,
    RelayProvider, ShallowSessionProvider, SourceExemption,
};
use crate::hydrate::{self, IngestSelectedFn};
use crate::ProviderRoots;
use std::path::PathBuf;

/// One built-in harness: its id and every per-source decision the crate makes
/// about it.
pub struct LocalSource {
    /// The `SOURCE_CHOICES` name, stored on every catalog and evidence row.
    pub(crate) id: &'static str,
    /// Whether the source has a shallow discovery adapter.
    pub(crate) discovery: Discovery,
    /// Whether, and how, one catalogued session can be hydrated.
    pub(crate) hydration: Hydration,
    /// The provider roots a hydrated or hook-captured locator must resolve
    /// inside. Empty for a source with no transcript files.
    pub(crate) transcript_roots: fn(&ProviderRoots) -> Vec<PathBuf>,
    /// What the provider's records establish about delegation.
    pub(crate) relationships: Relationships,
    /// How to reopen a session in the provider's own CLI, if it has one.
    pub(crate) resume: Option<Resume>,
    /// Where the source's fixture-corpus entries live.
    pub(crate) fixtures: Fixtures,
}

/// Whether a source takes part in shallow discovery.
#[derive(Clone, Copy)]
pub(crate) enum Discovery {
    /// Its shallow adapter. Built fresh per call, because an adapter may carry
    /// per-pass state.
    Provider(fn() -> Box<dyn ShallowSessionProvider>),
    /// No adapter, deliberately, and why.
    Exempt(&'static str),
}

/// Whether one catalogued session of a source can be hydrated.
#[derive(Clone, Copy)]
pub(crate) enum Hydration {
    /// Its local parser for one selected session.
    Parser(IngestSelectedFn),
    /// A valid catalog source that no local parser backs. Hydration accepts
    /// the request and then refuses it with `HYDRATION_UNSUPPORTED` and this
    /// message.
    NoConnector(&'static str),
    /// Not a hydratable catalog source at all: rejected as an invalid
    /// argument.
    Unsupported,
}

/// What a source's records establish about delegation; see
/// [`crate::RelationshipCapabilities`] for the meaning of each flag.
#[derive(Clone, Copy)]
pub(crate) struct Relationships {
    pub(crate) stable_child_identity: &'static str,
    pub(crate) records_agent_type: bool,
    pub(crate) records_spawn_time: bool,
    pub(crate) records_evidence_locator: bool,
}

impl Relationships {
    /// A source that records no delegation.
    pub(crate) const NONE: Self = Self {
        stable_child_identity: "never",
        records_agent_type: false,
        records_spawn_time: false,
        records_evidence_locator: false,
    };
}

/// How to reopen a session in its provider's CLI.
#[derive(Clone, Copy)]
pub(crate) struct Resume {
    /// The command, given the already shell-quoted session id.
    pub(crate) command: fn(&str) -> String,
    /// Whether to `cd` into the recorded project first.
    pub(crate) in_project: bool,
}

/// Where a source's fixture-corpus entries live, or why it has none.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fixtures {
    /// The directory under `tests/fixtures/` (and `tests/snapshots/`).
    Dir(&'static str),
    /// No provider log to capture, and why.
    Exempt(&'static str),
}

impl LocalSource {
    /// The source id.
    pub fn id(&self) -> &'static str {
        self.id
    }
    /// Why this source has no discovery adapter, or `None` when it has one.
    pub fn discovery_exemption(&self) -> Option<&'static str> {
        match self.discovery {
            Discovery::Provider(_) => None,
            Discovery::Exempt(reason) => Some(reason),
        }
    }
    /// Where this source's fixture-corpus entries live.
    pub fn fixtures(&self) -> Fixtures {
        self.fixtures
    }
    /// Whether hydration accepts this source (it may still refuse the
    /// session when no parser backs it).
    pub fn is_hydration_source(&self) -> bool {
        !matches!(self.hydration, Hydration::Unsupported)
    }
}

/// Every built-in harness. The declaration order is `SOURCE_CHOICES` order.
pub(crate) const LOCAL_SOURCES: &[LocalSource] = &[
    LocalSource {
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
    },
    LocalSource {
        id: "codex",
        discovery: Discovery::Provider(|| Box::new(CodexProvider)),
        hydration: Hydration::Parser(hydrate::ingest_selected_codex),
        transcript_roots: |roots| {
            vec![
                roots.codex.join("sessions"),
                roots.codex.join("archived_sessions"),
            ]
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
    },
    LocalSource {
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
    },
    LocalSource {
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
    },
    LocalSource {
        id: "relay",
        discovery: Discovery::Provider(|| Box::new(RelayProvider)),
        hydration: Hydration::NoConnector(
            "Relay catalog evidence has no configured full-evidence connector",
        ),
        transcript_roots: |_| Vec::new(),
        relationships: Relationships::NONE,
        resume: None,
        fixtures: Fixtures::Exempt(
            "projected from already-synced local rows; no provider log on disk to capture",
        ),
    },
    LocalSource {
        id: "trajectory",
        discovery: Discovery::Exempt("derived trajectory records, not provider sessions"),
        hydration: Hydration::Unsupported,
        transcript_roots: |_| Vec::new(),
        relationships: Relationships::NONE,
        resume: None,
        fixtures: Fixtures::Exempt("derived trajectory records, not provider sessions"),
    },
    LocalSource {
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
    },
    LocalSource {
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
    },
];

/// Every built-in harness descriptor, in `SOURCE_CHOICES` order.
pub fn local_sources() -> &'static [LocalSource] {
    LOCAL_SOURCES
}

/// The descriptor for one source id.
pub fn local_source(id: &str) -> Option<&'static LocalSource> {
    LOCAL_SOURCES.iter().find(|source| source.id == id)
}

const SOURCE_COUNT: usize = LOCAL_SOURCES.len();

const SOURCE_IDS: [&str; SOURCE_COUNT] = {
    let mut ids = [""; SOURCE_COUNT];
    let mut i = 0;
    while i < SOURCE_COUNT {
        ids[i] = LOCAL_SOURCES[i].id;
        i += 1;
    }
    ids
};

/// Backs `SOURCE_CHOICES`.
pub(crate) const SOURCE_CHOICES: &[&str] = &SOURCE_IDS;

const EXEMPTION_COUNT: usize = {
    let mut count = 0;
    let mut i = 0;
    while i < SOURCE_COUNT {
        if let Discovery::Exempt(_) = LOCAL_SOURCES[i].discovery {
            count += 1;
        }
        i += 1;
    }
    count
};

const EXEMPTIONS: [SourceExemption; EXEMPTION_COUNT] = {
    let mut out = [SourceExemption {
        source: "",
        reason: "",
    }; EXEMPTION_COUNT];
    let mut i = 0;
    let mut next = 0;
    while i < SOURCE_COUNT {
        if let Discovery::Exempt(reason) = LOCAL_SOURCES[i].discovery {
            out[next] = SourceExemption {
                source: LOCAL_SOURCES[i].id,
                reason,
            };
            next += 1;
        }
        i += 1;
    }
    out
};

/// Backs `DISCOVERY_EXEMPTIONS`.
pub(crate) const DISCOVERY_EXEMPTIONS: &[SourceExemption] = &EXEMPTIONS;

/// A fresh shallow adapter for every discoverable source, in source-id order.
///
/// Discovery has always run its adapters alphabetically, which puts `relay`
/// — whose rows are read back from the catalog the others just wrote — after
/// the file-backed providers. Sorting here keeps that order independent of
/// where a descriptor is declared.
pub(crate) fn shallow_providers() -> Vec<Box<dyn ShallowSessionProvider>> {
    let mut providers: Vec<&LocalSource> = LOCAL_SOURCES
        .iter()
        .filter(|source| matches!(source.discovery, Discovery::Provider(_)))
        .collect();
    providers.sort_by_key(|source| source.id);
    providers
        .into_iter()
        .filter_map(|source| match source.discovery {
            Discovery::Provider(build) => Some(build()),
            Discovery::Exempt(_) => None,
        })
        .collect()
}

/// The targeted-hydration parser for a source, if one backs it.
pub(crate) fn selected_ingest(source: &str) -> Option<IngestSelectedFn> {
    match local_source(source)?.hydration {
        Hydration::Parser(parser) => Some(parser),
        Hydration::NoConnector(_) | Hydration::Unsupported => None,
    }
}

/// Why hydration refuses a known source that no parser backs.
pub(crate) fn hydration_refusal(source: &str) -> Option<&'static str> {
    match local_source(source)?.hydration {
        Hydration::NoConnector(message) => Some(message),
        Hydration::Parser(_) | Hydration::Unsupported => None,
    }
}

/// The provider roots a locator for `source` must resolve inside.
pub(crate) fn transcript_roots(source: &str, roots: &ProviderRoots) -> Vec<PathBuf> {
    local_source(source).map_or_else(Vec::new, |source| (source.transcript_roots)(roots))
}

/// A source's declared relationship support; a source that is not a built-in
/// harness records none.
pub(crate) fn relationships(source: &str) -> Relationships {
    local_source(source).map_or(Relationships::NONE, |source| source.relationships)
}

/// The resume command for one session, `cd`-ing into `project` first when the
/// provider resumes relative to it.
pub(crate) fn resume_command(source: &str, session_id: &str, project: Option<&str>) -> Option<String> {
    let resume = local_source(source)?.resume?;
    let command = (resume.command)(&crate::shell_quote(session_id));
    Some(match project {
        Some(project) if resume.in_project => {
            format!("cd {} && {command}", crate::shell_quote(project))
        }
        _ => command,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn descriptor_ids_are_unique_and_are_source_choices() {
        let ids: BTreeSet<&str> = LOCAL_SOURCES.iter().map(|source| source.id).collect();
        assert_eq!(ids.len(), LOCAL_SOURCES.len(), "a source is declared twice");
        assert_eq!(crate::SOURCE_CHOICES, SOURCE_IDS.as_slice());
    }

    #[test]
    fn each_discovery_adapter_reports_its_own_descriptor_id() {
        for descriptor in LOCAL_SOURCES {
            if let Discovery::Provider(build) = descriptor.discovery {
                let provider = build();
                assert_eq!(provider.source(), descriptor.id);
                assert_eq!(provider.location(), crate::SessionLocation::Local);
            }
        }
    }

    #[test]
    fn discovery_runs_in_source_id_order() {
        let order: Vec<&str> = shallow_providers()
            .iter()
            .map(|provider| provider.source())
            .collect();
        let mut sorted = order.clone();
        sorted.sort_unstable();
        assert_eq!(order, sorted);
    }

    #[test]
    fn a_hydratable_source_is_discoverable() {
        // Hydration reads a catalog row; a source discovery never writes one
        // cannot be hydrated, so a parser without an adapter is a mistake.
        for descriptor in LOCAL_SOURCES {
            if descriptor.is_hydration_source() {
                assert!(
                    descriptor.discovery_exemption().is_none(),
                    "{} accepts hydration but is exempt from discovery",
                    descriptor.id
                );
            }
        }
    }

    #[test]
    fn an_unknown_source_declares_nothing() {
        assert!(local_source("zzz").is_none());
        assert!(selected_ingest("zzz").is_none());
        assert!(hydration_refusal("zzz").is_none());
        assert!(resume_command("zzz", "s1", Some("/p")).is_none());
        assert_eq!(relationships("zzz").stable_child_identity, "never");
        let roots = ProviderRoots::from_env(PathBuf::from("/home/u"));
        assert!(transcript_roots("zzz", &roots).is_empty());
    }
}
