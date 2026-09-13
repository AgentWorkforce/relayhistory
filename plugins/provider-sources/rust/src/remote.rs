//! Optional provider-native remote transports. No RelayHistory authentication is read.
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Mutex};
use std::time::{Duration, Instant};

use ai_hist_core::{SessionLocation, SOURCE_CHOICES};
use anyhow::{Context, Result};
use rusqlite::Connection;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::discover::{Candidate, DiscoveryEnv, ScanEnv, ShallowSession, ShallowSessionProvider};

/// Connector name for the claude.ai/code web-session lister.
pub const CLAUDE_WEB_CONNECTOR: &str = "claude-web";
/// Connector name for the Codex cloud task lister.
pub const CODEX_CLOUD_CONNECTOR: &str = "codex-cloud";

/// Most listing pages one enumeration may fetch, whatever the caller asked.
const MAX_LIST_PAGES: usize = 100;
/// Rows requested per claude.ai listing page (the endpoint's own maximum).
const CLAUDE_PAGE_LIMIT: usize = 100;
const CLAUDE_EVIDENCE_PAGE_LIMIT: usize = 1_000;
const MAX_REMOTE_EVIDENCE_BYTES: usize = 16 * 1024 * 1024;
const REMOTE_COMMAND_TIMEOUT: Duration = Duration::from_secs(30);

pub use crate::sources::AcquiredEvidence as RemoteSessionEvidence;

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

fn claude_credentials_path(home: &Path) -> PathBuf {
    if let Some(path) = std::env::var_os("RELAYHISTORY_CLAUDE_CREDENTIALS") {
        if !path.is_empty() {
            return PathBuf::from(path);
        }
    }
    home.join(".claude/.credentials.json")
}

fn codex_auth_path(home: &Path) -> PathBuf {
    home.join(".codex/auth.json")
}

/// Explicit built-in source connector allowlist. Omission retains provider CLI
/// connectors; an empty list disables remote acquisition. Commercial credentials
/// never add a connector. Instance-specific registration follows the observation
/// migration; these built-ins currently represent one default instance each.
#[derive(Debug, Clone)]
pub struct SourceConnectorSelection {
    ids: Vec<String>,
}

impl Default for SourceConnectorSelection {
    fn default() -> Self {
        Self {
            ids: vec![CLAUDE_WEB_CONNECTOR.into(), CODEX_CLOUD_CONNECTOR.into()],
        }
    }
}

impl SourceConnectorSelection {
    pub fn new(ids: Vec<String>) -> Result<Self> {
        for id in &ids {
            anyhow::ensure!(
                [CLAUDE_WEB_CONNECTOR, CODEX_CLOUD_CONNECTOR,].contains(&id.as_str()),
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

/// Provider-only availability; this operation never reads commercial auth.
pub fn remote_connector_statuses_at(home: &Path) -> Vec<RemoteConnectorStatus> {
    selected_remote_connector_statuses_at(home, &SourceConnectorSelection::default(), &[])
}

/// Probe only selected connectors applicable to the source filter. Selection is
/// evaluated before touching any credential store or commercial environment.
pub fn selected_remote_connector_statuses_at(
    home: &Path,
    selection: &SourceConnectorSelection,
    sources: &[String],
) -> Vec<RemoteConnectorStatus> {
    let mut statuses = Vec::new();
    for (connector, source) in [
        (CLAUDE_WEB_CONNECTOR, "claude"),
        (CODEX_CLOUD_CONNECTOR, "codex"),
    ] {
        if !selection.contains(connector)
            || (!sources.is_empty() && !sources.iter().any(|s| s == source))
        {
            continue;
        }
        let path = if connector == CLAUDE_WEB_CONNECTOR {
            claude_credentials_path(home)
        } else {
            codex_auth_path(home)
        };
        let configured = path.is_file();
        statuses.push(RemoteConnectorStatus {
            connector,
            source,
            configured,
            detail: if configured {
                format!("provider CLI login at {}", path.display())
            } else {
                format!(
                    "no provider CLI credentials at {} (sign in with the provider CLI)",
                    path.display()
                )
            },
        });
    }
    statuses
}

/// [`remote_connector_statuses_at`] under the process home directory.
pub fn remote_connector_statuses() -> Vec<RemoteConnectorStatus> {
    remote_connector_statuses_at(&crate::home_dir())
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
    ensure_remote_connectors_configured_at(operation, &crate::home_dir())
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
    ensure_remote_connectors_configured_for_at(operation, &crate::home_dir(), sources)
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
    ensure_selected_remote_connectors_configured_for_at(
        operation,
        &crate::home_dir(),
        sources,
        selection,
    )
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
                _ => true,
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
    home: &Path,
    limit: Option<usize>,
    selection: &SourceConnectorSelection,
    sources: &[String],
) -> Vec<Box<dyn ShallowSessionProvider>> {
    let applicable = |source: &str| sources.is_empty() || sources.iter().any(|s| s == source);
    let mut providers: Vec<Box<dyn ShallowSessionProvider>> = Vec::new();
    let claude_path = claude_credentials_path(home);
    if selection.contains(CLAUDE_WEB_CONNECTOR) && applicable("claude") && claude_path.is_file() {
        providers.push(Box::new(ClaudeWebProvider::new(
            claude_path,
            claude_api_base_url(),
            Box::new(UreqClaudeTransport),
            limit,
        )));
    }
    let codex_path = codex_auth_path(home);
    if selection.contains(CODEX_CLOUD_CONNECTOR) && applicable("codex") && codex_path.is_file() {
        providers.push(Box::new(CodexCloudProvider::new(
            Box::new(ExecCodexCli),
            limit,
        )));
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

// ---------------------------------------------------------------------------
// claude-web
// ---------------------------------------------------------------------------

/// One HTTP response from the claude.ai session-list endpoint.
pub struct ClaudeHttpResponse {
    pub status: u16,
    pub body: String,
}

/// The HTTP side of the claude.ai session listing, abstracted so mapping and
/// pagination are testable without a network.
pub trait ClaudeSessionsTransport: Send + Sync {
    fn get_with_headers(
        &self,
        url: &str,
        bearer_token: &str,
        headers: &[(&str, &str)],
    ) -> Result<ClaudeHttpResponse>;
}

struct UreqClaudeTransport;

impl ClaudeSessionsTransport for UreqClaudeTransport {
    fn get_with_headers(
        &self,
        url: &str,
        bearer_token: &str,
        headers: &[(&str, &str)],
    ) -> Result<ClaudeHttpResponse> {
        // Redirects are never followed: ureq would re-send the Authorization
        // header to the redirect target, so a redirecting endpoint could move
        // the stored OAuth token to a host the https-or-loopback guard never
        // saw. A 3xx therefore surfaces as a failed listing, not a hop.
        let agent = ureq::AgentBuilder::new().redirects(0).build();
        let mut request = agent
            .get(url)
            .timeout(std::time::Duration::from_secs(30))
            .set("Authorization", &format!("Bearer {bearer_token}"))
            .set("Content-Type", "application/json")
            .set("anthropic-version", "2023-06-01")
            .set("anthropic-beta", "oauth-2025-04-20");
        for (name, value) in headers {
            request = request.set(name, value);
        }
        match request.call() {
            Ok(response) => bounded_claude_response(response.status(), response),
            Err(ureq::Error::Status(status, response)) => bounded_claude_response(status, response),
            Err(error) => Err(anyhow::Error::from(error).context("claude.ai session list request")),
        }
    }
}

fn bounded_claude_response(status: u16, response: ureq::Response) -> Result<ClaudeHttpResponse> {
    let mut bytes = Vec::new();
    response
        .into_reader()
        .take((MAX_REMOTE_EVIDENCE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    anyhow::ensure!(
        bytes.len() <= MAX_REMOTE_EVIDENCE_BYTES,
        "Claude response exceeded the 16 MiB response-size limit"
    );
    Ok(ClaudeHttpResponse {
        status,
        body: String::from_utf8_lossy(&bytes).into_owned(),
    })
}

#[cfg(test)]
fn acquire_remote_session_at(
    home: &Path,
    source: &str,
    session_id: &str,
) -> Result<RemoteSessionEvidence> {
    match source {
        "claude" => acquire_claude_remote_session_at(
            home,
            session_id,
            &claude_api_base_url(),
            &UreqClaudeTransport,
        ),
        "codex" if codex_auth_path(home).is_file() => acquire_codex_remote_session(session_id),
        "codex" => Ok(RemoteSessionEvidence::CapabilityLimited {
            code: "CONNECTOR_NOT_CONFIGURED",
            message: "codex-cloud is not configured; run `codex login`".to_string(),
        }),
        _ => Ok(RemoteSessionEvidence::CapabilityLimited {
            code: "CONNECTOR_NOT_CONFIGURED",
            message: format!("no remote hydration connector exists for source '{source}'"),
        }),
    }
}

fn acquire_claude_remote_session_at(
    home: &Path,
    session_id: &str,
    base_url: &str,
    transport: &dyn ClaudeSessionsTransport,
) -> Result<RemoteSessionEvidence> {
    let credentials_path = claude_credentials_path(home);
    if !credentials_path.is_file() {
        return Ok(RemoteSessionEvidence::CapabilityLimited {
            code: "CONNECTOR_NOT_CONFIGURED",
            message: "claude-web is not configured; sign in with Claude Code or set RELAYHISTORY_CLAUDE_CREDENTIALS".to_string(),
        });
    }
    anyhow::ensure!(
        is_claude_web_session_id(session_id),
        "INVALID_ARGUMENT: remote Claude session id is malformed"
    );
    require_https_or_loopback(base_url)?;
    let oauth = load_claude_oauth(&credentials_path)?;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    if oauth.expires_at_ms.is_some_and(|expires| expires <= now_ms) {
        anyhow::bail!("AUTHENTICATION_EXPIRED: the stored claude.ai OAuth token has expired; run Claude Code once to refresh it");
    }

    // Claude Code itself resolves the organization this way before calling
    // teleport-events. Both interfaces are private implementation contracts,
    // so parser failures are explicit rather than treated as empty evidence.
    let profile_url = format!("{base_url}/api/oauth/profile");
    let profile = transport.get_with_headers(&profile_url, &oauth.access_token, &[])?;
    match profile.status {
        200 => {}
        401 | 403 => anyhow::bail!(
            "AUTHENTICATION_EXPIRED: claude.ai rejected the stored OAuth token (HTTP {})",
            profile.status
        ),
        status => anyhow::bail!("CONNECTOR_FAILURE: Claude OAuth profile failed (HTTP {status})"),
    }
    anyhow::ensure!(
        profile.body.len() <= MAX_REMOTE_EVIDENCE_BYTES,
        "CONNECTOR_FAILURE: Claude OAuth profile exceeded the response-size limit"
    );
    let profile_json: Value = serde_json::from_str(&profile.body)
        .context("CONNECTOR_FAILURE: Claude OAuth profile returned malformed JSON")?;
    let org_uuid = profile_json
        .pointer("/organization/uuid")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .context("CONNECTOR_FAILURE: Claude OAuth profile omitted organization.uuid")?;

    let mut records = Vec::new();
    let mut cursor: Option<String> = None;
    let mut source_bytes = profile.body.len();
    for _ in 0..MAX_LIST_PAGES {
        let mut url = format!(
            "{base_url}/v1/code/sessions/{session_id}/teleport-events?limit={CLAUDE_EVIDENCE_PAGE_LIMIT}"
        );
        if let Some(value) = cursor.as_deref() {
            url.push_str("&cursor=");
            url.push_str(&urlencode(value));
        }
        let response = transport.get_with_headers(
            &url,
            &oauth.access_token,
            &[("x-organization-uuid", org_uuid)],
        )?;
        match response.status {
            200 => {}
            401 => anyhow::bail!(
                "AUTHENTICATION_EXPIRED: Claude teleport evidence was rejected (HTTP {})",
                response.status
            ),
            403 => anyhow::bail!(
                "CONNECTOR_FAILURE: Claude teleport evidence was denied; the session may require trusted-device enrollment"
            ),
            404 => anyhow::bail!("SESSION_NOT_FOUND: remote Claude session no longer exists"),
            status => anyhow::bail!(
                "CONNECTOR_FAILURE: Claude teleport evidence failed (HTTP {status})"
            ),
        }
        source_bytes = source_bytes.saturating_add(response.body.len());
        anyhow::ensure!(
            source_bytes <= MAX_REMOTE_EVIDENCE_BYTES,
            "CONNECTOR_FAILURE: remote Claude evidence exceeded the 16 MiB response-size limit"
        );
        let payload: Value = serde_json::from_str(&response.body)
            .context("CONNECTOR_FAILURE: Claude teleport evidence returned malformed JSON")?;
        let page = payload
            .get("data")
            .and_then(Value::as_array)
            .context("CONNECTOR_FAILURE: Claude teleport evidence response has no data array")?;
        for entry in page {
            let record = entry
                .get("payload")
                .filter(|value| value.is_object())
                .context(
                    "CONNECTOR_FAILURE: Claude teleport evidence contains a malformed record",
                )?;
            records.push(record.clone());
        }
        cursor = string_field(&payload, "next_cursor");
        if cursor.is_none() {
            let encoded = serde_json::to_vec(&records)?;
            let source_stamp = format!("teleport:{:x}", Sha256::digest(&encoded));
            return Ok(RemoteSessionEvidence::ClaudeFull {
                records,
                source_stamp,
                source_bytes: source_bytes as i64,
            });
        }
    }
    anyhow::bail!("EVIDENCE_PARTIAL: Claude teleport evidence exceeded the 100-page bound")
}

fn claude_api_base_url() -> String {
    // Deliberately NOT `ANTHROPIC_BASE_URL`: that variable redirects generic
    // Anthropic API traffic (LLM gateways, dev proxies), and following it here
    // would hand the claude.ai OAuth token to whatever host it happens to
    // name. Redirecting the session-list endpoint — and with it the stored
    // credential — must be its own explicit decision.
    let raw = std::env::var("RELAYHISTORY_CLAUDE_API_BASE_URL").unwrap_or_default();
    let trimmed = raw.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        "https://api.anthropic.com".to_string()
    } else {
        trimmed.to_string()
    }
}

/// Reject a plaintext listing endpoint. Loopback is exempt so a local mock
/// (or proxy) works without ceremony — the same posture `cloud.rs` takes for
/// `wrangler dev`.
fn require_https_or_loopback(base_url: &str) -> Result<()> {
    if base_url.starts_with("https://") {
        return Ok(());
    }
    let rest = base_url
        .strip_prefix("http://")
        .with_context(|| format!("claude.ai base URL must be http(s), got {base_url}"))?;
    let authority = rest.split('/').next().unwrap_or_default();
    let host_port = authority.rsplit('@').next().unwrap_or_default();
    // A bracketed IPv6 authority keeps its colons inside the brackets, so the
    // port split must not run inside them: `[::1]:8787` names `[::1]`.
    let host = match host_port.strip_prefix('[') {
        Some(bracketed) => bracketed.split(']').next().unwrap_or_default(),
        None => host_port.split(':').next().unwrap_or_default(),
    };
    anyhow::ensure!(
        matches!(host, "localhost" | "127.0.0.1" | "::1"),
        "refusing to send the claude.ai OAuth token over plain http:// to {base_url}; use an https:// endpoint (plain http is accepted only for loopback)"
    );
    Ok(())
}

/// The claude.ai OAuth token as the Claude Code CLI stores it.
struct ClaudeOauth {
    access_token: String,
    expires_at_ms: Option<i64>,
}

fn load_claude_oauth(path: &Path) -> Result<ClaudeOauth> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("could not read claude.ai credentials at {}", path.display()))?;
    let value: Value = serde_json::from_str(&raw)
        .with_context(|| format!("claude.ai credentials at {} are not JSON", path.display()))?;
    let oauth = value
        .get("claudeAiOauth")
        .with_context(|| format!("no claudeAiOauth entry in {}", path.display()))?;
    let access_token = string_field(oauth, "accessToken")
        .with_context(|| format!("no claudeAiOauth.accessToken in {}", path.display()))?;
    Ok(ClaudeOauth {
        access_token,
        expires_at_ms: oauth.get("expiresAt").and_then(Value::as_i64),
    })
}

/// Is this a claude.ai code-session id (`session_…` / `cse_…`)?
fn is_claude_web_session_id(id: &str) -> bool {
    let rest = id
        .strip_prefix("session_")
        .or_else(|| id.strip_prefix("cse_"));
    match rest {
        Some(rest) if !rest.is_empty() => rest
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-'),
        _ => false,
    }
}

/// Map one session object from `GET /v1/code/sessions` into a catalog row,
/// or `None` when the entry is not a remote coding session we should index
/// (a malformed id, or a Remote Control bridge mirroring a *local* session).
fn map_claude_web_session(value: &Value) -> Option<(Candidate, ShallowSession)> {
    let id = string_field(value, "id").filter(|id| is_claude_web_session_id(id))?;
    // `environment_kind: "bridge"` is a Remote Control view of a session that
    // runs in a local terminal. Its evidence is local, not remote; the local
    // transcript adapter already covers it.
    if string_field(value, "environment_kind").as_deref() == Some("bridge") {
        return None;
    }
    let created_at = string_field(value, "created_at");
    let last_event_at = string_field(value, "last_event_at");
    let first_activity_ms = created_at.as_deref().and_then(crate::parse_iso_ms);
    let last_activity_ms = last_event_at
        .as_deref()
        .and_then(crate::parse_iso_ms)
        .or(first_activity_ms);
    let repo_url = value
        .pointer("/config/sources")
        .and_then(Value::as_array)
        .and_then(|sources| {
            sources
                .iter()
                .find(|source| string_field(source, "type").as_deref() == Some("git_repository"))
        })
        .and_then(|source| string_field(source, "url"));
    let stamp = format!(
        "web:{}",
        last_event_at
            .or(created_at)
            .unwrap_or_else(|| "unknown".to_string())
    );
    let session = ShallowSession {
        source: "claude".into(),
        session_id: id.clone(),
        // The listing's title is the only human-readable identifier the
        // endpoint offers; claude.ai derives it from the opening prompt.
        // Bounded like every stored excerpt.
        first_prompt: string_field(value, "title").map(|title| excerpt(&title)),
        first_activity_ms,
        last_activity_ms,
        repo_url,
        raw_path: Some(format!("https://claude.ai/code/{id}")),
        discovery_state: "shallow".into(),
        ..Default::default()
    };
    let candidate = Candidate {
        source: "claude",
        locator: id.clone(),
        session_id: Some(id),
        recency_hint_ms: last_activity_ms,
        stamp,
    };
    Some((candidate, session))
}

/// Shallow adapter over the claude.ai/code session list.
pub struct ClaudeWebProvider {
    credentials_path: PathBuf,
    base_url: String,
    transport: Box<dyn ClaudeSessionsTransport>,
    limit: Option<usize>,
    fetched: FetchedRows,
}

impl ClaudeWebProvider {
    pub fn new(
        credentials_path: PathBuf,
        base_url: String,
        transport: Box<dyn ClaudeSessionsTransport>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            credentials_path,
            base_url,
            transport,
            limit,
            fetched: FetchedRows::default(),
        }
    }
}

impl ShallowSessionProvider for ClaudeWebProvider {
    fn check_available(&self, _home: &Path) -> Result<()> {
        anyhow::ensure!(
            self.credentials_path.is_file(),
            "CONNECTOR_NOT_CONFIGURED: Claude credentials unavailable"
        );
        Ok(())
    }

    fn acquire(
        &self,
        home: &Path,
        observation: &ai_hist_core::observations::SessionObservation,
    ) -> Result<RemoteSessionEvidence> {
        acquire_claude_remote_session_at(
            home,
            &observation.key.session_id,
            &self.base_url,
            self.transport.as_ref(),
        )
    }

    fn connector_id(&self) -> &str {
        CLAUDE_WEB_CONNECTOR
    }
    fn source(&self) -> &'static str {
        "claude"
    }

    fn location(&self) -> SessionLocation {
        SessionLocation::Remote
    }

    fn enumerate(
        &self,
        _env: &DiscoveryEnv<'_>,
        _requested_limit: Option<usize>,
    ) -> Result<Vec<Candidate>> {
        require_https_or_loopback(&self.base_url)?;
        let oauth = load_claude_oauth(&self.credentials_path)?;
        if let Some(expires_at_ms) = oauth.expires_at_ms {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_millis() as i64)
                .unwrap_or_default();
            anyhow::ensure!(
                expires_at_ms > now_ms,
                "the stored claude.ai OAuth token has expired; run the Claude Code CLI once to refresh it"
            );
        }
        let mut candidates = Vec::new();
        let mut rows = BTreeMap::new();
        let mut cursor: Option<String> = None;
        for _ in 0..MAX_LIST_PAGES {
            let page_limit = match self.limit {
                Some(limit) => limit
                    .saturating_sub(candidates.len())
                    .clamp(1, CLAUDE_PAGE_LIMIT),
                None => CLAUDE_PAGE_LIMIT,
            };
            let mut url = format!("{}/v1/code/sessions?limit={page_limit}", self.base_url);
            if let Some(cursor) = cursor.as_deref() {
                url.push_str("&cursor=");
                url.push_str(&urlencode(cursor));
            }
            let response = self
                .transport
                .get_with_headers(&url, &oauth.access_token, &[])?;
            match response.status {
                200 => {}
                401 | 403 => anyhow::bail!(
                    "claude.ai rejected the stored OAuth token (HTTP {}); run the Claude Code CLI once to refresh your sign-in",
                    response.status
                ),
                status => anyhow::bail!(
                    "claude.ai session list failed (HTTP {status}): {}",
                    excerpt_one_line(&response.body)
                ),
            }
            let payload: Value = serde_json::from_str(&response.body)
                .context("claude.ai session list returned unparseable JSON")?;
            let page = payload
                .get("data")
                .and_then(Value::as_array)
                .context("claude.ai session list response has no data array")?;
            for entry in page {
                if let Some((candidate, session)) = map_claude_web_session(entry) {
                    rows.insert(candidate.locator.clone(), session);
                    candidates.push(candidate);
                }
            }
            cursor = string_field(&payload, "next_cursor");
            let done = cursor.is_none()
                || page.is_empty()
                || self.limit.is_some_and(|limit| candidates.len() >= limit);
            if done {
                break;
            }
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

fn excerpt_one_line(raw: &str) -> String {
    let flattened = raw.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut out: String = flattened.chars().take(200).collect();
    if flattened.chars().count() > 200 {
        out.push('…');
    }
    out
}

fn acquire_codex_remote_session(session_id: &str) -> Result<RemoteSessionEvidence> {
    acquire_codex_remote_session_with_command(session_id, OsStr::new("codex"))
}

fn acquire_codex_remote_session_with_command(
    session_id: &str,
    command: &OsStr,
) -> Result<RemoteSessionEvidence> {
    acquire_codex_remote_session_with_command_timeout(session_id, command, REMOTE_COMMAND_TIMEOUT)
}

fn acquire_codex_remote_session_with_command_timeout(
    session_id: &str,
    command: &OsStr,
    timeout: Duration,
) -> Result<RemoteSessionEvidence> {
    if !session_id.starts_with("task_")
        || !session_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        anyhow::bail!("INVALID_ARGUMENT: remote Codex task id is malformed");
    }
    let mut child = std::process::Command::new(command)
        .args(["cloud", "diff", session_id])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|error| match error.kind() {
            std::io::ErrorKind::NotFound => {
                anyhow::anyhow!("CONNECTOR_NOT_CONFIGURED: the `codex` CLI is not on PATH")
            }
            _ => anyhow::Error::from(error)
                .context("CONNECTOR_FAILURE: could not run `codex cloud diff`"),
        })?;
    let stdout = child
        .stdout
        .take()
        .context("CONNECTOR_FAILURE: no Codex stdout pipe")?;
    let stderr = child
        .stderr
        .take()
        .context("CONNECTOR_FAILURE: no Codex stderr pipe")?;
    let read_bounded = |mut stream: Box<dyn Read + Send>| {
        let (sender, receiver) = mpsc::sync_channel(1);
        std::thread::spawn(move || {
            let mut bytes = Vec::new();
            let mut buffer = [0u8; 64 * 1024];
            let result = loop {
                match stream.read(&mut buffer) {
                    Ok(0) => break Ok(bytes),
                    Ok(read) => {
                        // Retain only enough to detect the limit, but keep
                        // draining so an oversized child cannot block on a
                        // full pipe and masquerade as a command timeout.
                        let remaining = (MAX_REMOTE_EVIDENCE_BYTES + 1).saturating_sub(bytes.len());
                        bytes.extend_from_slice(&buffer[..read.min(remaining)]);
                    }
                    Err(error) => break Err(error),
                }
            };
            let _ = sender.send(result);
        });
        receiver
    };
    let stdout_reader = read_bounded(Box::new(stdout));
    let stderr_reader = read_bounded(Box::new(stderr));
    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if started.elapsed() >= timeout {
            let _ = child.kill();
            let _ = child.wait();
            anyhow::bail!("CONNECTOR_FAILURE: `codex cloud diff` exceeded the 30 second timeout");
        }
        std::thread::sleep(Duration::from_millis(25));
    };
    let receive = |reader: mpsc::Receiver<std::io::Result<Vec<u8>>>, name: &str| {
        let remaining = timeout.checked_sub(started.elapsed()).unwrap_or_default();
        reader
            .recv_timeout(remaining)
            .map_err(|_| {
                anyhow::anyhow!(
                    "CONNECTOR_FAILURE: Codex {name} pipe remained open past the 30 second timeout"
                )
            })?
            .map_err(anyhow::Error::from)
    };
    let stdout = receive(stdout_reader, "stdout")?;
    let stderr = receive(stderr_reader, "stderr")?;
    anyhow::ensure!(
        stdout.len() <= MAX_REMOTE_EVIDENCE_BYTES && stderr.len() <= MAX_REMOTE_EVIDENCE_BYTES,
        "CONNECTOR_FAILURE: `codex cloud diff` exceeded the 16 MiB response-size limit"
    );
    if !status.success() {
        let detail = excerpt_one_line(&String::from_utf8_lossy(&stderr));
        let lower = detail.to_ascii_lowercase();
        if lower.contains("login") || lower.contains("unauthorized") || lower.contains("expired") {
            anyhow::bail!("AUTHENTICATION_EXPIRED: Codex CLI authentication was rejected");
        }
        if lower.contains("not found") || lower.contains("no task") {
            anyhow::bail!("SESSION_NOT_FOUND: remote Codex task no longer exists");
        }
        anyhow::bail!("CONNECTOR_FAILURE: `codex cloud diff` failed ({status}): {detail}");
    }
    let diff = String::from_utf8(stdout)
        .context("CONNECTOR_FAILURE: `codex cloud diff` returned non-UTF-8 output")?;
    if diff.trim().is_empty() {
        return Ok(RemoteSessionEvidence::CapabilityLimited {
            code: "PROVIDER_CAPABILITY_LIMITED",
            message: "Codex exposes no transcript API and this task has no available diff"
                .to_string(),
        });
    }
    let source_stamp = format!("diff:{:x}", Sha256::digest(diff.as_bytes()));
    Ok(RemoteSessionEvidence::CodexDiff {
        source_bytes: diff.len() as i64,
        diff,
        source_stamp,
    })
}

// ---------------------------------------------------------------------------
// codex-cloud
// ---------------------------------------------------------------------------

/// The Codex CLI accepts `--limit` values of 1–20 only (20 is also its
/// default), so every page request stays inside that window and larger
/// requests paginate with `--cursor` instead.
const CODEX_PAGE_LIMIT: usize = 20;

/// The process side of `codex cloud list --json`, abstracted so parsing,
/// mapping, and pagination are testable without the Codex CLI installed.
/// `limit` is a per-page cap (at most [`CODEX_PAGE_LIMIT`]); `cursor`
/// continues a previous page's listing.
pub trait CodexCloudLister: Send + Sync {
    fn list_json(&self, limit: usize, cursor: Option<&str>) -> Result<String>;
}

/// Runs the real `codex` CLI. Its `--json` output is Codex's documented
/// scripting contract, and the CLI handles auth/refresh itself — the same
/// reason the engine shells out to `git` instead of reimplementing it.
struct ExecCodexCli;

impl CodexCloudLister for ExecCodexCli {
    fn list_json(&self, limit: usize, cursor: Option<&str>) -> Result<String> {
        let mut command = std::process::Command::new("codex");
        command.args(["cloud", "list", "--json", "--limit"]);
        command.arg(limit.clamp(1, CODEX_PAGE_LIMIT).to_string());
        if let Some(cursor) = cursor {
            command.arg("--cursor").arg(cursor);
        }
        let output = command
            .stdin(std::process::Stdio::null())
            .output()
            .map_err(|error| match error.kind() {
                std::io::ErrorKind::NotFound => anyhow::anyhow!(
                    "the `codex` CLI is not on PATH; install Codex to list cloud tasks"
                ),
                _ => anyhow::Error::from(error).context("could not run `codex cloud list`"),
            })?;
        anyhow::ensure!(
            output.status.success(),
            "`codex cloud list --json` failed ({}): {}",
            output.status,
            excerpt_one_line(&String::from_utf8_lossy(&output.stderr))
        );
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }
}

/// Map one cloud task from `codex cloud list --json` into a catalog row.
fn map_codex_cloud_task(value: &Value) -> Option<(Candidate, ShallowSession)> {
    let id = string_field(value, "id")?;
    let updated_at = string_field(value, "updated_at");
    let last_activity_ms = updated_at.as_deref().and_then(crate::parse_iso_ms);
    // Status participates in the stamp: an applied or failed task whose
    // timestamp did not move still deserves a re-read.
    let stamp = format!(
        "cloud:{}:{}",
        updated_at.as_deref().unwrap_or("unknown"),
        string_field(value, "status")
            .as_deref()
            .unwrap_or("unknown")
    );
    let session = ShallowSession {
        source: "codex".into(),
        session_id: id.clone(),
        // The task title is Codex's own rendering of the prompt that created
        // the task — the only human-readable identifier the listing offers.
        // Bounded like every stored excerpt.
        first_prompt: string_field(value, "title").map(|title| excerpt(&title)),
        last_activity_ms,
        raw_path: string_field(value, "url"),
        discovery_state: "shallow".into(),
        ..Default::default()
    };
    let candidate = Candidate {
        source: "codex",
        locator: id.clone(),
        session_id: Some(id),
        recency_hint_ms: last_activity_ms,
        stamp,
    };
    Some((candidate, session))
}

/// One page of `codex cloud list --json` output.
struct CodexCloudPage {
    tasks: Vec<Value>,
    /// Continuation cursor, when the payload carries one.
    cursor: Option<String>,
}

/// Accept both documented shapes: `{"tasks": […], "cursor": …}` and a bare
/// task array (which carries no cursor and therefore ends the walk).
fn parse_codex_cloud_listing(raw: &str) -> Result<CodexCloudPage> {
    let payload: Value =
        serde_json::from_str(raw).context("`codex cloud list --json` output is not JSON")?;
    match &payload {
        Value::Array(tasks) => Ok(CodexCloudPage {
            tasks: tasks.clone(),
            cursor: None,
        }),
        Value::Object(_) => Ok(CodexCloudPage {
            tasks: payload
                .get("tasks")
                .and_then(Value::as_array)
                .cloned()
                .context("`codex cloud list --json` output has no tasks array")?,
            cursor: string_field(&payload, "cursor"),
        }),
        _ => anyhow::bail!("`codex cloud list --json` output has no tasks array"),
    }
}

/// Shallow adapter over the Codex cloud task list.
pub struct CodexCloudProvider {
    lister: Box<dyn CodexCloudLister>,
    limit: Option<usize>,
    fetched: FetchedRows,
}

impl CodexCloudProvider {
    pub fn new(lister: Box<dyn CodexCloudLister>, limit: Option<usize>) -> Self {
        Self {
            lister,
            limit,
            fetched: FetchedRows::default(),
        }
    }
}

impl ShallowSessionProvider for CodexCloudProvider {
    fn check_available(&self, home: &Path) -> Result<()> {
        anyhow::ensure!(
            codex_auth_path(home).is_file(),
            "CONNECTOR_NOT_CONFIGURED: Codex credentials unavailable"
        );
        Ok(())
    }

    fn acquire(
        &self,
        _home: &Path,
        observation: &ai_hist_core::observations::SessionObservation,
    ) -> Result<RemoteSessionEvidence> {
        acquire_codex_remote_session(&observation.key.session_id)
    }

    fn connector_id(&self) -> &str {
        CODEX_CLOUD_CONNECTOR
    }
    fn source(&self) -> &'static str {
        "codex"
    }

    fn location(&self) -> SessionLocation {
        SessionLocation::Remote
    }

    fn enumerate(
        &self,
        _env: &DiscoveryEnv<'_>,
        _requested_limit: Option<usize>,
    ) -> Result<Vec<Candidate>> {
        let mut candidates = Vec::new();
        let mut rows = BTreeMap::new();
        let mut cursor: Option<String> = None;
        for _ in 0..MAX_LIST_PAGES {
            let page_limit = match self.limit {
                Some(limit) => limit
                    .saturating_sub(candidates.len())
                    .clamp(1, CODEX_PAGE_LIMIT),
                None => CODEX_PAGE_LIMIT,
            };
            let raw = self.lister.list_json(page_limit, cursor.as_deref())?;
            let page = parse_codex_cloud_listing(&raw)?;
            for task in &page.tasks {
                if let Some((candidate, session)) = map_codex_cloud_task(task) {
                    rows.insert(candidate.locator.clone(), session);
                    candidates.push(candidate);
                }
            }
            cursor = page.cursor;
            let done = cursor.is_none()
                || page.tasks.is_empty()
                || self.limit.is_some_and(|limit| candidates.len() >= limit);
            if done {
                break;
            }
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

// ---------------------------------------------------------------------------
// cloud — org-wide recall, preserving the upstream provider's natural key
// ---------------------------------------------------------------------------

fn excerpt(text: &str) -> String {
    text.trim()
        .chars()
        .take(crate::discover::EXCERPT_MAX_CHARS)
        .collect()
}
#[cfg(test)]
mod tests;

/// Select exactly one adapter. Construction does not read credentials or invoke a transport.
pub fn provider(
    home: &Path,
    connector: &str,
    instance: &str,
    limit: Option<usize>,
) -> Result<Box<dyn ShallowSessionProvider>> {
    anyhow::ensure!(
        limit.is_none_or(|n| n <= 10_000),
        "INVALID_ARGUMENT: source limit exceeds 10000"
    );
    anyhow::ensure!(
        !instance.trim().is_empty() && instance.len() <= 512,
        "INVALID_ARGUMENT: invalid connector instance"
    );
    let inner: Box<dyn ShallowSessionProvider> = match connector {
        CLAUDE_WEB_CONNECTOR => Box::new(ClaudeWebProvider::new(
            claude_credentials_path(home),
            claude_api_base_url(),
            Box::new(UreqClaudeTransport),
            limit,
        )),
        CODEX_CLOUD_CONNECTOR => Box::new(CodexCloudProvider::new(Box::new(ExecCodexCli), limit)),
        _ => anyhow::bail!("INVALID_ARGUMENT: unknown provider source connector"),
    };
    Ok(Box::new(InstanceProvider {
        inner,
        instance: instance.into(),
    }))
}
struct InstanceProvider {
    inner: Box<dyn ShallowSessionProvider>,
    instance: String,
}
impl ShallowSessionProvider for InstanceProvider {
    fn connector_id(&self) -> &str {
        self.inner.connector_id()
    }
    fn connector_instance(&self) -> &str {
        &self.instance
    }
    fn source(&self) -> &'static str {
        self.inner.source()
    }
    fn location(&self) -> SessionLocation {
        self.inner.location()
    }
    fn check_available(&self, home: &Path) -> Result<()> {
        self.inner.check_available(home)
    }
    fn acquire(
        &self,
        home: &Path,
        observation: &ai_hist_core::observations::SessionObservation,
    ) -> Result<RemoteSessionEvidence> {
        self.inner.acquire(home, observation)
    }
    fn enumerate(&self, env: &DiscoveryEnv<'_>, limit: Option<usize>) -> Result<Vec<Candidate>> {
        self.inner.enumerate(env, limit)
    }
    fn read_shallow(
        &self,
        env: &ScanEnv<'_>,
        conn: Option<&Connection>,
        candidate: &Candidate,
    ) -> Result<Option<ShallowSession>> {
        self.inner.read_shallow(env, conn, candidate)
    }
}
