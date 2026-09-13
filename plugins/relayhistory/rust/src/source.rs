//! Optional legacy RelayHistory recall source adapter. Explicit selection only.
use ai_hist_core::{SessionLocation, SOURCE_CHOICES};
use ai_hist_engine::discover::{
    Candidate, DiscoveryEnv, ScanEnv, ShallowSession, ShallowSessionProvider,
};
use anyhow::{Context, Result};
use rusqlite::Connection;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
fn urlencode(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

fn home_dir() -> PathBuf {
    std::env::var_os("HOME")
        .map(Into::into)
        .unwrap_or_else(|| ".".into())
}
fn excerpt(text: &str) -> String {
    text.trim()
        .chars()
        .take(ai_hist_engine::discover::EXCERPT_MAX_CHARS)
        .collect()
}
/// Org-scoped recall resources, implemented in the shared cloud transport.
pub use crate::cloud::RecallResource as CloudRecallResource;

/// Read one recall page using the configured RelayHistory session. All token
/// loading, stage selection, refresh, URL encoding and guards remain in cloud.rs.
pub fn cloud_recall_page(
    resource: CloudRecallResource<'_>,
    query: &[(&str, &str)],
) -> Result<Value> {
    crate::cloud::recall_page(&crate::cloud::recall_auth()?, resource, query)
}

/// Connector name for the claude.ai/code web-session lister.
pub const CLAUDE_WEB_CONNECTOR: &str = "claude-web";
/// Connector name for the Codex cloud task lister.
pub const CODEX_CLOUD_CONNECTOR: &str = "codex-cloud";
/// Connector name for org-wide RelayHistory recall. This is not a source.
pub const CLOUD_CONNECTOR: &str = "cloud";

/// Most listing pages one enumeration may fetch, whatever the caller asked.
const MAX_LIST_PAGES: usize = 100;

/// Whether one remote connector can run on this machine, and why not when it
/// cannot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteConnectorStatus {
    /// Connector name (`claude-web`, `codex-cloud`, `cloud`).
    pub connector: &'static str,
    /// The upstream source, or `"*"` for a cross-source connector.
    pub source: &'static str,
    /// `true` when the provider CLI's stored sign-in was found.
    pub configured: bool,
    /// Human-readable detail: the credential looked for, or where it was found.
    pub detail: String,
}

/// Explicit built-in source connector allowlist. Omission retains provider CLI
/// connectors; an empty list disables remote acquisition. Commercial credentials
/// never add a connector. Instance-specific registration follows the observation
/// migration; these built-ins currently represent one default instance each.
#[derive(Debug, Clone, Default)]
pub struct SourceConnectorSelection {
    ids: Vec<String>,
}

impl SourceConnectorSelection {
    pub fn new(ids: Vec<String>) -> Result<Self> {
        for id in &ids {
            anyhow::ensure!(
                [CLOUD_CONNECTOR, RELAYCAST_CONNECTOR].contains(&id.as_str()),
                "invalid source connector '{id}'"
            );
        }
        let mut ids = ids;
        ids.sort();
        ids.dedup();
        Ok(Self { ids })
    }

    pub fn contains(&self, id: &str) -> bool {
        self.ids.iter().any(|selected| selected == id)
    }

    pub fn ids(&self) -> &[String] {
        &self.ids
    }
}

pub const RELAYCAST_CONNECTOR: &str = "relaycast";

#[cfg(test)]
thread_local! {
    static COMMERCIAL_AUTH_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn selected_recall_auth() -> Result<crate::cloud::StoredAuth> {
    #[cfg(test)]
    COMMERCIAL_AUTH_READS.with(|count| count.set(count.get() + 1));
    crate::cloud::recall_auth()
}

/// Provider-only availability; this operation never reads commercial auth.
pub fn remote_connector_statuses_at(home: &Path) -> Vec<RemoteConnectorStatus> {
    selected_remote_connector_statuses_at(home, &SourceConnectorSelection::default(), &[])
}

/// Probe only selected connectors applicable to the source filter. Selection is
/// evaluated before touching any credential store or commercial environment.
pub fn selected_remote_connector_statuses_at(
    _home: &Path,
    selection: &SourceConnectorSelection,
    _sources: &[String],
) -> Vec<RemoteConnectorStatus> {
    let mut statuses = Vec::new();
    if selection.contains(CLOUD_CONNECTOR) {
        let auth = selected_recall_auth();
        statuses.push(RemoteConnectorStatus {
            connector: CLOUD_CONNECTOR,
            source: "*",
            configured: auth.is_ok(),
            detail: match auth {
                Ok(auth) => format!(
                    "RelayHistory session at {} ({})",
                    crate::cloud::config_dir().display(),
                    auth.base_url
                ),
                Err(error) => format!("{}: {error:#}", crate::cloud::config_dir().display()),
            },
        });
    }
    statuses
}

/// [`remote_connector_statuses_at`] under the process home directory.
pub fn remote_connector_statuses() -> Vec<RemoteConnectorStatus> {
    remote_connector_statuses_at(&home_dir())
}

/// The error a remote-only acquisition request gets when nothing is configured.
///
/// The leading phrase is a compatibility contract: callers and tests match on
/// "no remote provider connectors are configured".
pub(crate) fn unconfigured_message(operation: &str, statuses: &[RemoteConnectorStatus]) -> String {
    let reasons = statuses
        .iter()
        .map(|status| format!("{}: {}", status.connector, status.detail))
        .collect::<Vec<_>>()
        .join("; ");
    format!(
        "no remote provider connectors are configured: remote session {operation} is not available ({reasons})"
    )
}

/// Reject a remote-only acquisition when no connector is configured, using the
/// process home directory. Cheap (a couple of `stat`s), so callers run it
/// before opening the ledger.
pub fn ensure_remote_connectors_configured(operation: &str) -> Result<()> {
    ensure_remote_connectors_configured_at(operation, &home_dir())
}

/// [`ensure_remote_connectors_configured`] under an explicit home directory.
pub fn ensure_remote_connectors_configured_at(operation: &str, home: &Path) -> Result<()> {
    ensure_remote_connectors_configured_for_at(operation, home, &[])
}

/// Reject a remote-only acquisition that no configured connector can serve
/// once a source filter is applied, using the process home directory.
///
/// An empty `sources` filter means "every source". A filter that names only
/// sources without a remote connector (or whose connectors are not signed
/// in) is the same unsupported request, scoped down — callers classify both
/// as unsupported-operation, never as a runtime discovery failure.
pub fn ensure_remote_connectors_configured_for(operation: &str, sources: &[String]) -> Result<()> {
    ensure_remote_connectors_configured_for_at(operation, &home_dir(), sources)
}

/// [`ensure_remote_connectors_configured_for`] under an explicit home directory.
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

pub fn ensure_selected_remote_connectors_configured_for(
    operation: &str,
    sources: &[String],
    selection: &SourceConnectorSelection,
) -> Result<()> {
    ensure_selected_remote_connectors_configured_for_at(operation, &home_dir(), sources, selection)
}

pub fn ensure_selected_remote_connectors_configured_for_at(
    operation: &str,
    home: &Path,
    sources: &[String],
    selection: &SourceConnectorSelection,
) -> Result<()> {
    // A misspelled source is an invalid argument, not an unsupported remote
    // request — reject it with the engine's own invalid-source message before
    // classifying anything.
    for source in sources {
        anyhow::ensure!(
            SOURCE_CHOICES.contains(&source.as_str()),
            "invalid source '{source}' (choose from {})",
            SOURCE_CHOICES.join(", ")
        );
    }
    // Reject capabilities before probing credentials too. Relaycast currently
    // supports full sync only; commercial recall has no targeted hydration.
    let applicable = SourceConnectorSelection {
        ids: selection
            .ids
            .iter()
            .filter(|id| match operation {
                "sync" => true,
                "hydration" | "hydrate" => {
                    matches!(id.as_str(), CLAUDE_WEB_CONNECTOR | CODEX_CLOUD_CONNECTOR)
                }
                _ => id.as_str() != RELAYCAST_CONNECTOR,
            })
            .cloned()
            .collect(),
    };
    let statuses = selected_remote_connector_statuses_at(home, &applicable, sources);
    anyhow::ensure!(
        !statuses.is_empty(),
        "remote session {operation} is not available for the requested source(s): no matching remote provider connectors exist"
    );
    anyhow::ensure!(
        statuses.iter().any(|status| status.configured),
        unconfigured_message(operation, &statuses)
    );
    Ok(())
}

/// Every configured remote connector, as shallow providers the discovery
/// engine can run beside the local adapters. Unconfigured connectors are
/// simply absent — for `all` scope that is the documented "runs whatever is
/// available" behaviour, and for `remote` scope the caller has already
/// rejected the empty set.
pub fn selected_remote_providers(
    _home: &Path,
    limit: Option<usize>,
    selection: &SourceConnectorSelection,
    sources: &[String],
) -> Vec<Box<dyn ShallowSessionProvider>> {
    let applicable = |source: &str| sources.is_empty() || sources.iter().any(|s| s == source);
    let mut providers: Vec<Box<dyn ShallowSessionProvider>> = Vec::new();
    if let Some(auth) = selection
        .contains(CLOUD_CONNECTOR)
        .then(selected_recall_auth)
        .and_then(Result::ok)
    {
        // One adapter per upstream source preserves the engine's source filters,
        // diagnostics and global recency ordering without inventing a cloud source.
        for source in SOURCE_CHOICES.iter().filter(|source| applicable(source)) {
            providers.push(Box::new(CloudProvider::new(auth.clone(), source, limit)));
        }
    }
    providers
}

// ---------------------------------------------------------------------------
// shared plumbing
// ---------------------------------------------------------------------------

/// Rows one remote enumeration fetched, keyed by candidate locator, waiting
/// for the engine's `read_shallow` calls. The listing already carried every
/// field, so the "read" is a map lookup.
type FetchedRows = Mutex<BTreeMap<String, ShallowSession>>;

fn take_fetched(rows: &FetchedRows, locator: &str) -> Option<ShallowSession> {
    rows.lock()
        .expect("remote connector row cache")
        .get(locator)
        .cloned()
}

fn string_field(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_string)
}

pub struct CloudProvider {
    instance: String,
    auth: crate::cloud::StoredAuth,
    source: &'static str,
    limit: Option<usize>,
    fetched: FetchedRows,
}

impl CloudProvider {
    pub fn new(auth: crate::cloud::StoredAuth, source: &'static str, limit: Option<usize>) -> Self {
        use sha2::{Digest, Sha256};
        let instance = format!(
            "{:x}",
            Sha256::digest(format!(
                "{}\0{}\0{}",
                auth.base_url,
                auth.org_id.as_deref().unwrap_or_default(),
                auth.workspace_id.as_deref().unwrap_or_default()
            ))
        );
        Self {
            instance,
            auth,
            source,
            limit,
            fetched: FetchedRows::default(),
        }
    }
}

fn map_cloud_session(value: &Value, org_id: &str) -> Result<(Candidate, ShallowSession)> {
    let source = string_field(value, "source").context("cloud session omitted source")?;
    let source = *SOURCE_CHOICES
        .iter()
        .find(|known| **known == source)
        .with_context(|| format!("cloud returned unsupported source '{source}'"))?;
    let id = string_field(value, "sessionId").context("cloud session omitted sessionId")?;
    let locator = format!("cloud://{}/{}", urlencode(org_id), urlencode(&id));
    let first_activity_ms = string_field(value, "firstTs")
        .as_deref()
        .and_then(crate::parse_iso_ms);
    let last_activity_ms = string_field(value, "lastTs")
        .as_deref()
        .and_then(crate::parse_iso_ms);
    // Include the full rollup, org and stage-independent locator: title/count changes
    // invalidate the cache even when lastTs did not move.
    let stamp = format!(
        "cloud:{}:{:x}",
        locator,
        Sha256::digest(serde_json::to_vec(value)?)
    );
    let session = ShallowSession {
        source: source.into(),
        session_id: id.clone(),
        first_prompt: string_field(value, "summary")
            .or_else(|| string_field(value, "taskTitle"))
            .map(|s| excerpt(&s)),
        first_activity_ms,
        last_activity_ms,
        models: value
            .get("models")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(String::from)
                    .collect()
            })
            .unwrap_or_default(),
        raw_path: Some(locator.clone()),
        discovery_state: "shallow".into(),
        ..Default::default()
    };
    Ok((
        Candidate {
            source,
            locator,
            session_id: Some(id),
            recency_hint_ms: last_activity_ms,
            stamp,
        },
        session,
    ))
}

impl ShallowSessionProvider for CloudProvider {
    fn connector_instance(&self) -> &str {
        &self.instance
    }
    fn connector_id(&self) -> &str {
        CLOUD_CONNECTOR
    }
    fn source(&self) -> &'static str {
        self.source
    }
    fn location(&self) -> SessionLocation {
        SessionLocation::Remote
    }

    fn enumerate(
        &self,
        _env: &DiscoveryEnv<'_>,
        _requested_limit: Option<usize>,
    ) -> Result<Vec<Candidate>> {
        if self.limit == Some(0) {
            return Ok(Vec::new());
        }
        let org_id = self
            .auth
            .org_id
            .as_deref()
            .filter(|id| !id.trim().is_empty())
            .context("stored cloud session has no orgId for provenance (run `ai-hist login`)")?;
        let mut candidates = Vec::new();
        let mut rows = BTreeMap::new();
        let mut cursor: Option<String> = None;
        let mut seen_cursors = std::collections::HashSet::new();
        for _ in 0..MAX_LIST_PAGES {
            let page_limit = self
                .limit
                .map(|limit| limit.saturating_sub(candidates.len()).clamp(1, 100))
                .unwrap_or(100)
                .to_string();
            let mut query = vec![("source", self.source), ("limit", page_limit.as_str())];
            if let Some(cursor) = cursor.as_deref() {
                query.push(("cursor", cursor));
            }
            let payload = crate::cloud::recall_page(
                &self.auth,
                crate::cloud::RecallResource::Sessions,
                &query,
            )?;
            let page = payload
                .get("sessions")
                .and_then(Value::as_array)
                .context("cloud recall response has no sessions array")?;
            for value in page {
                let (candidate, session) = map_cloud_session(value, org_id)?;
                anyhow::ensure!(
                    candidate.source == self.source,
                    "cloud recall returned a session outside the requested source"
                );
                if rows.insert(candidate.locator.clone(), session).is_none() {
                    candidates.push(candidate);
                }
                if self.limit.is_some_and(|limit| candidates.len() >= limit) {
                    break;
                }
            }
            cursor = match payload.get("nextCursor") {
                None | Some(Value::Null) => None,
                Some(Value::String(value)) if !value.is_empty() => Some(value.clone()),
                _ => anyhow::bail!("cloud recall returned an invalid nextCursor"),
            };
            if cursor.is_none() || self.limit.is_some_and(|limit| candidates.len() >= limit) {
                break;
            }
            anyhow::ensure!(
                seen_cursors.insert(cursor.clone().unwrap()),
                "cloud recall returned a repeated nextCursor"
            );
            // Catalog discovery is bounded sampling, not transcript hydration.
            // Reaching the page cap keeps the fetched rows, like the other
            // connectors; a malformed/repeated cursor still fails above.
        }
        *self.fetched.lock().expect("remote connector row cache") = rows;
        Ok(candidates)
    }

    fn read_shallow(
        &self,
        _scan: &ScanEnv<'_>,
        _catalog: Option<&Connection>,
        candidate: &Candidate,
    ) -> Result<Option<ShallowSession>> {
        Ok(take_fetched(&self.fetched, &candidate.locator))
    }
}

#[cfg(test)]
mod tests;

/// Legacy recall discovery through explicitly selected stage credentials.
/// This adapter is catalog-only; durable-delivery readback is a separate source.
pub fn discover(
    base: Option<&str>,
    source: Option<&str>,
    expected_instance: Option<&str>,
    limit: Option<usize>,
) -> Result<Value> {
    anyhow::ensure!(
        limit.is_none_or(|n| n <= 10_000),
        "source limit exceeds 10000"
    );
    let sources: Vec<_> = match source {
        Some(source) => vec![*SOURCE_CHOICES
            .iter()
            .find(|value| **value == source)
            .context("invalid source")?],
        None => SOURCE_CHOICES.to_vec(),
    };
    let auth =
        crate::cloud::resolve_recall_auth(base, chrono::Utc::now().timestamp_millis(), true)?;
    let home = home_dir();
    let conn = Connection::open_in_memory()?;
    let env = DiscoveryEnv::with_roots(&conn, home.clone(), home.join("unused-opencode.db"));
    let mut observations = Vec::new();
    for source in sources {
        let remaining = limit.map(|limit| limit.saturating_sub(observations.len()));
        let provider = CloudProvider::new(auth.clone(), source, remaining);
        anyhow::ensure!(
            expected_instance.is_none_or(|value| value == provider.connector_instance()),
            "connector account instance mismatch"
        );
        for candidate in provider.enumerate(&env, remaining)? {
            if let Some(row) = provider.read_shallow(&env.scan(), None, &candidate)? {
                observations.push(row);
            }
        }
    }
    Ok(serde_json::json!({"observations":observations}))
}
