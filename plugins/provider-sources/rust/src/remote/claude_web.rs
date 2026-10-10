use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};

use ai_hist::SessionLocation;
use anyhow::{Context, Result};
use rusqlite::Connection;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::discover::{Candidate, DiscoveryEnv, ScanEnv, ShallowSession, ShallowSessionProvider};

#[cfg(test)]
use super::{acquire_codex_remote_session, codex_auth_path};
use super::{
    claude_credentials_path, excerpt, string_field, take_fetched, FetchedRows,
    RemoteSessionEvidence, CLAUDE_WEB_CONNECTOR, MAX_LIST_PAGES, MAX_REMOTE_EVIDENCE_BYTES,
};

/// Rows requested per claude.ai listing page (the endpoint's own maximum).
const CLAUDE_PAGE_LIMIT: usize = 100;
const CLAUDE_EVIDENCE_PAGE_LIMIT: usize = 1_000;

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

pub(super) struct UreqClaudeTransport;

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
pub(super) fn acquire_remote_session_at(
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

pub(super) fn acquire_claude_remote_session_at(
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
    ensure_claude_profile_status(profile.status)?;
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
        ensure_claude_teleport_status(response.status)?;
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

/// The Claude OAuth profile response status, as the connector error it
/// stands for.
fn ensure_claude_profile_status(status: u16) -> Result<()> {
    match status {
        200 => Ok(()),
        401 | 403 => anyhow::bail!(
            "AUTHENTICATION_EXPIRED: claude.ai rejected the stored OAuth token (HTTP {})",
            status
        ),
        status => anyhow::bail!("CONNECTOR_FAILURE: Claude OAuth profile failed (HTTP {status})"),
    }
}

/// A Claude teleport-events response status, as the connector error it
/// stands for.
fn ensure_claude_teleport_status(status: u16) -> Result<()> {
    match status {
        200 => Ok(()),
        401 => anyhow::bail!(
            "AUTHENTICATION_EXPIRED: Claude teleport evidence was rejected (HTTP {})",
            status
        ),
        403 => anyhow::bail!(
            "CONNECTOR_FAILURE: Claude teleport evidence was denied; the session may require trusted-device enrollment"
        ),
        404 => anyhow::bail!("SESSION_NOT_FOUND: remote Claude session no longer exists"),
        status => anyhow::bail!(
            "CONNECTOR_FAILURE: Claude teleport evidence failed (HTTP {status})"
        ),
    }
}

pub(super) fn claude_api_base_url() -> String {
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
pub(super) fn require_https_or_loopback(base_url: &str) -> Result<()> {
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
pub(super) fn map_claude_web_session(value: &Value) -> Option<(Candidate, ShallowSession)> {
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
        observation: &ai_hist::observations::SessionObservation,
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

pub(super) fn excerpt_one_line(raw: &str) -> String {
    let flattened = raw.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut out: String = flattened.chars().take(200).collect();
    if flattened.chars().count() > 200 {
        out.push('…');
    }
    out
}
