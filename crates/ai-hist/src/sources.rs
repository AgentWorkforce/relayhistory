//! Explicit composition of installed history source adapters.
//!
//! Registration never probes credentials or calls a transport. The caller chooses
//! connector instances first; only those adapters receive acquisition calls.
use crate::discover::discover_sessions_with_provider_refs;
use crate::observations::{self, ObservationKey, SessionObservation};
use crate::*;
use anyhow::{ensure, Context, Result};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ConnectorIdentity {
    pub connector_id: String,
    pub connector_instance: String,
}
impl ConnectorIdentity {
    pub fn new(id: impl Into<String>, instance: impl Into<String>) -> Self {
        Self {
            connector_id: id.into(),
            connector_instance: instance.into(),
        }
    }
}

/// A complete normalized event snapshot for one observation. This is event-only
/// evidence; tools, edits and relationships require a fuller connector format. `event_uid` uses
/// the original provider identity, so the same event seen through two connectors
/// has one canonical row. The connector identity is stored separately.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorEvidence {
    pub source_stamp: String,
    pub source_bytes: i64,
    pub events: Vec<crate::SessionEvent>,
}

/// Provider wire formats can use existing parsers; independent adapters can
/// supply normalized events without a dependency on a commercial transport.
#[derive(Debug)]
pub enum AcquiredEvidence {
    /// Ask the built-in local parser to read the selected local observation.
    /// Valid only for the built-in source/default connector identity.
    LocalFiles,
    Events(ConnectorEvidence),
    Normalized(crate::source_intake::NormalizedSourceEvidence),
    ClaudeFull {
        records: Vec<Value>,
        source_stamp: String,
        source_bytes: i64,
    },
    CodexDiff {
        diff: String,
        source_stamp: String,
        source_bytes: i64,
    },
    CapabilityLimited {
        code: &'static str,
        message: String,
    },
}

#[derive(Default)]
pub struct SourceRegistry {
    providers: Vec<Box<dyn ShallowSessionProvider>>,
}
impl SourceRegistry {
    pub fn new() -> Self {
        Self::default()
    }
    /// Local provider adapters only. Remote adapters must be installed and selected.
    pub fn local() -> Self {
        Self {
            providers: shallow_providers(),
        }
    }
    pub fn register(&mut self, provider: Box<dyn ShallowSessionProvider>) -> Result<()> {
        ObservationKey {
            source: provider.source().into(),
            session_id: "registration".into(),
            location: provider.location(),
            connector_id: provider.connector_id().into(),
            connector_instance: provider.connector_instance().into(),
        }
        .validate()?;
        ensure!(
            !self
                .providers
                .iter()
                .any(|other| other.source() == provider.source()
                    && other.location() == provider.location()
                    && other.connector_id() == provider.connector_id()
                    && other.connector_instance() == provider.connector_instance()),
            "INVALID_ARGUMENT: duplicate source connector instance"
        );
        self.providers.push(provider);
        Ok(())
    }
    /// Resolve the complete selection before any adapter is allowed to probe auth.
    fn select(
        &self,
        scope: SessionScope,
        sources: &[String],
        selected: &[ConnectorIdentity],
    ) -> Result<Vec<&dyn ShallowSessionProvider>> {
        let mut unique = BTreeSet::new();
        for identity in selected {
            ensure!(
                unique.insert(identity),
                "INVALID_ARGUMENT: duplicate selected source connector"
            );
            ensure!(
                self.providers
                    .iter()
                    .any(|provider| provider.connector_id() == identity.connector_id
                        && provider.connector_instance() == identity.connector_instance),
                "INVALID_ARGUMENT: unknown source connector {}/{}",
                identity.connector_id,
                identity.connector_instance
            );
        }
        for source in sources {
            ensure!(
                crate::SOURCE_CHOICES.contains(&source.as_str()),
                "INVALID_ARGUMENT: unknown history source {source}"
            );
        }
        Ok(self
            .providers
            .iter()
            .filter(|provider| {
                let location_matches = matches!(
                    (scope, provider.location()),
                    (SessionScope::All, _)
                        | (SessionScope::Local, SessionLocation::Local)
                        | (SessionScope::Remote, SessionLocation::Remote)
                );
                location_matches
                    && (sources.is_empty()
                        || sources.iter().any(|source| source == provider.source()))
                    && (provider.location() == SessionLocation::Local
                        || selected.iter().any(|identity| {
                            provider.connector_id() == identity.connector_id
                                && provider.connector_instance() == identity.connector_instance
                        }))
            })
            .map(|provider| provider.as_ref())
            .collect())
    }
    fn available<'a>(
        &self,
        providers: Vec<&'a dyn ShallowSessionProvider>,
        scope: SessionScope,
    ) -> Result<(
        Vec<&'a dyn ShallowSessionProvider>,
        Vec<DiscoveryDiagnostic>,
    )> {
        let mut available = vec![];
        let mut diagnostics = vec![];
        for provider in providers {
            match provider.check_available(&home_dir()) {
                Ok(()) => available.push(provider),
                Err(error) => diagnostics.push(DiscoveryDiagnostic {
                    source: provider.source().into(),
                    connector_id: Some(provider.connector_id().into()),
                    connector_instance: Some(provider.connector_instance().into()),
                    location: Some(provider.location()),
                    locator: None,
                    error: format!("{error:#}"),
                }),
            }
        }
        ensure!(
            scope != SessionScope::Remote || !available.is_empty(),
            "CONNECTOR_NOT_CONFIGURED: no selected remote source connector is available"
        );
        Ok((available, diagnostics))
    }
    pub fn discover_at(
        &self,
        db_path: &Path,
        options: &DiscoverOptions,
        selected: &[ConnectorIdentity],
        on_row: impl FnMut(&ShallowSession),
    ) -> Result<DiscoverySummary> {
        let providers = self.select(options.scope, &options.sources, selected)?;
        ensure!(
            options.scope != SessionScope::Remote || !providers.is_empty(),
            "CONNECTOR_NOT_CONFIGURED: no selected remote source connector"
        );
        let (providers, diagnostics) = self.available(providers, options.scope)?;
        let conn = open_db(db_path)?;
        let env = DiscoveryEnv::new(&conn);
        let mut summary = discover_sessions_with_provider_refs(&env, options, &providers, on_row)?;
        summary.diagnostics.extend(diagnostics);
        Ok(summary)
    }
    /// Local full ingestion followed by selected adapters' bounded catalog refresh.
    /// Remote full evidence is acquired through `hydrate_at` for a selected session.
    pub fn sync_at(
        &self,
        db_path: &Path,
        scope: SessionScope,
        selected: &[ConnectorIdentity],
    ) -> Result<DiscoverySummary> {
        let providers = self.select(scope, &[], selected)?;
        ensure!(
            scope != SessionScope::Remote || !providers.is_empty(),
            "CONNECTOR_NOT_CONFIGURED: no selected remote source connector"
        );
        let (providers, diagnostics) = self.available(providers, scope)?;
        if scope != SessionScope::Remote {
            sync_local_at(db_path)?;
        }
        let conn = open_db(db_path)?;
        let env = DiscoveryEnv::new(&conn);
        let mut summary = discover_sessions_with_provider_refs(
            &env,
            &DiscoverOptions {
                scope,
                ..Default::default()
            },
            &providers,
            |_| {},
        )?;
        summary.diagnostics.extend(diagnostics);
        Ok(summary)
    }
    pub fn hydrate_at(
        &self,
        db_path: &Path,
        options: &HydrateSessionOptions,
        selected: &ConnectorIdentity,
    ) -> Result<HydrateSessionResult> {
        ensure!(
            options.scope != SessionScope::All,
            "INVALID_ARGUMENT: hydration requires one location"
        );
        let providers = self.select(
            options.scope,
            std::slice::from_ref(&options.source),
            std::slice::from_ref(selected),
        )?;
        let provider=providers.into_iter().find(|provider|provider.connector_id()==selected.connector_id && provider.connector_instance()==selected.connector_instance).context("CONNECTOR_NOT_CONFIGURED: selected connector does not serve this source and location")?;
        provider.check_available(&home_dir())?;
        crate::hydrate::hydrate_with_provider(db_path, options, provider)
    }
}

pub(crate) fn observation_key(
    provider: &dyn ShallowSessionProvider,
    session_id: &str,
) -> ObservationKey {
    ObservationKey {
        source: provider.source().into(),
        session_id: session_id.into(),
        location: provider.location(),
        connector_id: provider.connector_id().into(),
        connector_instance: provider.connector_instance().into(),
    }
}

pub(crate) fn observed(
    conn: &Connection,
    provider: &dyn ShallowSessionProvider,
    session_id: &str,
) -> Result<SessionObservation> {
    observations::get(conn, &observation_key(provider, session_id))?.context(
        "SESSION_NOT_FOUND: selected connector has not observed this session; discover it first",
    )
}

pub use crate::hydrate::normalize_source_evidence;
pub use crate::source_intake::NormalizedSourceEvidence;
