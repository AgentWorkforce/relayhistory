//! Compatibility selectors for the local distribution. Source transports live
//! in optional packages and are composed through [`crate::sources::SourceRegistry`]
//! or the generic source intake API. This module never reads credentials.
#[cfg(any(test, feature = "unstable-internal"))]
use crate::ShallowSessionProvider;
use crate::SOURCE_CHOICES;
use anyhow::Result;
use std::path::Path;

#[cfg(any(test, feature = "unstable-internal"))]
pub const CLAUDE_WEB_CONNECTOR: &str = "claude-web";
#[cfg(any(test, feature = "unstable-internal"))]
pub const CODEX_CLOUD_CONNECTOR: &str = "codex-cloud";
#[cfg(any(test, feature = "unstable-internal"))]
pub const CLOUD_CONNECTOR: &str = "cloud";
#[cfg(any(test, feature = "unstable-internal"))]
pub const RELAYCAST_CONNECTOR: &str = "relaycast";

/// Legacy names remain accepted to provide a useful installed-plugin error.
/// Omission selects no external adapters in the local distribution.
#[derive(Debug, Clone, Default)]
pub struct SourceConnectorSelection {
    #[cfg(any(test, feature = "unstable-internal"))]
    ids: Vec<String>,
}
#[cfg(any(test, feature = "unstable-internal"))]
impl SourceConnectorSelection {
    pub fn new(ids: Vec<String>) -> Result<Self> {
        let mut seen = std::collections::BTreeSet::new();
        for id in &ids {
            anyhow::ensure!([CLAUDE_WEB_CONNECTOR,CODEX_CLOUD_CONNECTOR,CLOUD_CONNECTOR,RELAYCAST_CONNECTOR].contains(&id.as_str()),"INVALID_ARGUMENT: invalid source connector '{id}'; install and select it through the source registry");
            anyhow::ensure!(
                seen.insert(id),
                "INVALID_ARGUMENT: duplicate source connector '{id}'"
            );
        }
        Ok(Self { ids })
    }
    pub fn contains(&self, id: &str) -> bool {
        self.ids.iter().any(|value| value == id)
    }
    #[cfg(feature = "unstable-internal")]
    pub fn ids(&self) -> &[String] {
        &self.ids
    }
}
#[cfg(any(test, feature = "unstable-internal"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteConnectorStatus {
    pub connector: &'static str,
    pub source: &'static str,
    pub configured: bool,
    pub detail: String,
}
#[cfg(any(test, feature = "unstable-internal"))]
pub fn selected_remote_connector_statuses_at(
    _home: &Path,
    selection: &SourceConnectorSelection,
    sources: &[String],
) -> Vec<RemoteConnectorStatus> {
    [
        (CLAUDE_WEB_CONNECTOR, "claude"),
        (CODEX_CLOUD_CONNECTOR, "codex"),
        (CLOUD_CONNECTOR, "*"),
        (RELAYCAST_CONNECTOR, "relay"),
    ]
    .into_iter()
    .filter(|(id, source)| {
        selection.contains(id)
            && (sources.is_empty() || *source == "*" || sources.iter().any(|value| value == source))
    })
    .map(|(connector, source)| RemoteConnectorStatus {
        connector,
        source,
        configured: false,
        detail: "optional source plugin requires an explicitly composed host".into(),
    })
    .collect()
}
#[cfg(feature = "unstable-internal")]
pub fn remote_connector_statuses_at(home: &Path) -> Vec<RemoteConnectorStatus> {
    selected_remote_connector_statuses_at(home, &SourceConnectorSelection::default(), &[])
}
#[cfg(feature = "unstable-internal")]
pub fn remote_connector_statuses() -> Vec<RemoteConnectorStatus> {
    vec![]
}
#[cfg(feature = "unstable-internal")]
pub fn ensure_remote_connectors_configured(operation: &str) -> Result<()> {
    ensure_remote_connectors_configured_at(operation, Path::new(""))
}
#[cfg(feature = "unstable-internal")]
pub fn ensure_remote_connectors_configured_at(operation: &str, home: &Path) -> Result<()> {
    ensure_remote_connectors_configured_for_at(operation, home, &[])
}
#[cfg(feature = "unstable-internal")]
pub fn ensure_remote_connectors_configured_for(operation: &str, sources: &[String]) -> Result<()> {
    ensure_remote_connectors_configured_for_at(operation, Path::new(""), sources)
}
#[cfg(feature = "unstable-internal")]
pub fn ensure_remote_connectors_configured_for_at(
    operation: &str,
    home: &Path,
    sources: &[String],
) -> Result<()> {
    ensure_selected_remote_connectors_configured_for_at(
        operation,
        home,
        sources,
        &SourceConnectorSelection::default(),
    )
}
#[cfg(feature = "unstable-internal")]
pub fn ensure_selected_remote_connectors_configured_for(
    operation: &str,
    sources: &[String],
    selection: &SourceConnectorSelection,
) -> Result<()> {
    ensure_selected_remote_connectors_configured_for_at(
        operation,
        Path::new(""),
        sources,
        selection,
    )
}
pub fn ensure_selected_remote_connectors_configured_for_at(
    operation: &str,
    _home: &Path,
    sources: &[String],
    _selection: &SourceConnectorSelection,
) -> Result<()> {
    for source in sources {
        anyhow::ensure!(
            SOURCE_CHOICES.contains(&source.as_str()),
            "INVALID_ARGUMENT: invalid source '{source}'"
        );
    }
    anyhow::bail!("no remote provider connectors are configured: remote session {operation} requires an installed source plugin and explicitly composed host")
}
#[cfg(any(test, feature = "unstable-internal"))]
pub(crate) fn selected_remote_providers(
    _home: &Path,
    _limit: Option<usize>,
    _selection: &SourceConnectorSelection,
    _sources: &[String],
) -> Vec<Box<dyn ShallowSessionProvider>> {
    vec![]
}
