//! The harness registry: one `LocalSource` descriptor per built-in source.
//!
//! These per-source tables are derived from `LOCAL_SOURCES` instead of being
//! kept as separate lists:
//!
//! - `SOURCE_CHOICES` is the descriptor ids, in declaration order;
//! - `shallow_providers()` and `DISCOVERY_EXEMPTIONS` come from each
//!   descriptor's `discovery`;
//! - hydration's source validation and `ingest_selected` dispatch come from
//!   `hydration`, and `validate_provider_path` from `transcript_roots`;
//! - `relationship_capabilities` comes from `relationships`;
//! - `resume_command` comes from `resume`;
//! - the fixture-corpus registry test reads `fixtures`.
//!
//! Provider-owned shards generate registration. New providers supply snapshot,
//! full-sync, roots, generation and accounting hooks under `src/providers/`;
//! legacy parser phases remain intact. See `docs/provider-intake.md`.
//! Out-of-tree Rust/JS source plugins remain separate from admitted built-ins.

use crate::discover::{ShallowSessionProvider, SourceExemption};
use crate::hydrate::IngestSelectedFn;
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
    #[cfg_attr(not(feature = "unstable-internal"), allow(dead_code))]
    pub(crate) resume: Option<Resume>,
    /// Where the source's fixture-corpus entries live.
    #[cfg_attr(not(feature = "unstable-internal"), allow(dead_code))]
    pub(crate) fixtures: Fixtures,
}

/// Whether a source takes part in shallow discovery.
#[derive(Clone, Copy)]
pub(crate) enum Discovery {
    /// Its shallow adapter. Built fresh per call, because an adapter may carry
    /// per-pass state.
    Provider(fn() -> Box<dyn ShallowSessionProvider>),
}

/// How one catalogued session of a source is hydrated. Every built-in source
/// has a local parser; a source without one would add a variant here.
#[derive(Clone, Copy)]
pub(crate) enum Hydration {
    /// Its local parser for one selected session.
    Parser(IngestSelectedFn),
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
#[cfg_attr(not(feature = "unstable-internal"), allow(dead_code))]
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
    #[cfg_attr(not(feature = "unstable-internal"), allow(dead_code))]
    pub fn id(&self) -> &'static str {
        self.id
    }
    /// Why this source has no discovery adapter, or `None` when it has one.
    #[cfg_attr(not(feature = "unstable-internal"), allow(dead_code))]
    pub fn discovery_exemption(&self) -> Option<&'static str> {
        match self.discovery {
            Discovery::Provider(_) => None,
        }
    }
    /// Where this source's fixture-corpus entries live.
    #[cfg_attr(not(feature = "unstable-internal"), allow(dead_code))]
    pub fn fixtures(&self) -> Fixtures {
        self.fixtures
    }
    /// Whether hydration accepts this source (it may still refuse the
    /// session when no parser backs it).
    pub fn is_hydration_source(&self) -> bool {
        matches!(self.hydration, Hydration::Parser(_))
    }
}

// Every built-in harness, in `SOURCE_CHOICES` order.
include!("../providers/generated_catalog.rs");

/// Every built-in harness descriptor, in `SOURCE_CHOICES` order.
#[cfg_attr(not(feature = "unstable-internal"), allow(dead_code))]
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

/// Backs `DISCOVERY_EXEMPTIONS`: every built-in source has a shallow adapter.
pub(crate) const DISCOVERY_EXEMPTIONS: &[SourceExemption] = &[];

/// A fresh shallow adapter for every discoverable source, in source-id order.
///
/// Discovery has always run its adapters in alphabetical order, which is the
/// order connector summaries and diagnostics are reported in. Sorting here
/// keeps that order stable, independent of where a descriptor is declared.
pub(crate) fn shallow_providers() -> Vec<Box<dyn ShallowSessionProvider>> {
    let mut providers: Vec<&LocalSource> = LOCAL_SOURCES.iter().collect();
    providers.sort_by_key(|source| source.id);
    providers
        .into_iter()
        .map(|source| match source.discovery {
            Discovery::Provider(build) => build(),
        })
        .collect()
}

/// The targeted-hydration parser for a source, if it is a built-in source.
pub(crate) fn selected_ingest(source: &str) -> Option<IngestSelectedFn> {
    match local_source(source)?.hydration {
        Hydration::Parser(parser) => Some(parser),
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
#[cfg_attr(not(feature = "unstable-internal"), allow(dead_code))]
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
            let Discovery::Provider(build) = descriptor.discovery;
            let provider = build();
            assert_eq!(provider.source(), descriptor.id);
            assert_eq!(provider.location(), crate::SessionLocation::Local);
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
        assert!(resume_command("zzz", "s1", Some("/p")).is_none());
        assert_eq!(relationships("zzz").stable_child_identity, "never");
        let roots = ProviderRoots::from_env(PathBuf::from("/home/u"));
        assert!(transcript_roots("zzz", &roots).is_empty());
    }
}
