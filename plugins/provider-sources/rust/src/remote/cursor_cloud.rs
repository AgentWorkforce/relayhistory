//! `cursor-cloud` — Cursor Cloud Agents, which never write a local transcript.
//!
//! Agents are listed through the v1 Cloud Agents API (`GET /v1/agents`) and
//! read through the v0 conversation endpoint
//! (`GET /v0/agents/{id}/conversation`): v1 has no transcript route, and its
//! per-run SSE streams expire after a retention window, so the v0 endpoint is
//! the only one that returns every user prompt and assistant reply. Its
//! messages carry no timestamps; each turn takes the timestamps of the v1 run
//! it started (`GET /v1/agents/{id}/runs`), which is the finest grain the API
//! reports.
//!
//! The API takes a Cursor user or service-account API key (Cursor Dashboard →
//! API Keys), never the editor's own sign-in: `RELAYHISTORY_CURSOR_API_KEY`,
//! else `CURSOR_API_KEY` (the variable the Cursor CLI reads), else the first
//! line of `RELAYHISTORY_CURSOR_API_KEY_FILE`, by default
//! `$XDG_CONFIG_HOME/relayhistory/cursor-api-key`. The key is never logged or
//! returned.

use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};

use ai_hist::SessionLocation;

use anyhow::{Context, Result};
use rusqlite::Connection;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::discover::{Candidate, DiscoveryEnv, ScanEnv, ShallowSession, ShallowSessionProvider};

use super::claude_web::{excerpt_one_line, require_https_or_loopback, urlencode};
use super::cursor_hydrate::acquire_cursor_remote_session_at;
use super::{
    excerpt, string_field, take_fetched, FetchedRows, RemoteSessionEvidence,
    CURSOR_CLOUD_CONNECTOR, MAX_LIST_PAGES, MAX_REMOTE_EVIDENCE_BYTES,
};

/// The v1 list endpoints accept at most 100 rows per page.
pub(super) const CURSOR_PAGE_LIMIT: usize = 100;
const DEFAULT_BASE_URL: &str = "https://api.cursor.com";
/// Longest agent id accepted; ids are interpolated into request paths.
const MAX_AGENT_ID_LEN: usize = 128;

// ---------------------------------------------------------------------------
// transport
// ---------------------------------------------------------------------------

/// One HTTP response from an api.cursor.com endpoint.
pub struct CursorHttpResponse {
    pub status: u16,
    pub body: String,
}

/// The HTTP side of the Cloud Agents API, abstracted so mapping, pagination
/// and hydration are testable without a network.
pub trait CursorTransport: Send + Sync {
    fn get(&self, url: &str, api_key: &str) -> Result<CursorHttpResponse>;
}

pub(super) struct UreqCursorTransport;

impl CursorTransport for UreqCursorTransport {
    fn get(&self, url: &str, api_key: &str) -> Result<CursorHttpResponse> {
        // Redirects are never followed: ureq would re-send the Authorization
        // header to a host the https-or-loopback guard never saw.
        let request = ureq::AgentBuilder::new()
            .redirects(0)
            .build()
            .get(url)
            .timeout(std::time::Duration::from_secs(30))
            .set("Authorization", &format!("Bearer {api_key}"))
            .set("Accept", "application/json");
        match request.call() {
            Ok(response) => bounded_response(response.status(), response),
            Err(ureq::Error::Status(status, response)) => bounded_response(status, response),
            Err(error) => Err(anyhow::Error::from(error).context("api.cursor.com request")),
        }
    }
}

fn bounded_response(status: u16, response: ureq::Response) -> Result<CursorHttpResponse> {
    let mut bytes = Vec::new();
    response
        .into_reader()
        .take((MAX_REMOTE_EVIDENCE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    anyhow::ensure!(
        bytes.len() <= MAX_REMOTE_EVIDENCE_BYTES,
        "Cursor response exceeded the 16 MiB response-size limit"
    );
    Ok(CursorHttpResponse {
        status,
        body: String::from_utf8_lossy(&bytes).into_owned(),
    })
}

// ---------------------------------------------------------------------------
// configuration
// ---------------------------------------------------------------------------

pub(super) struct CursorAuth {
    pub(super) api_key: String,
    pub(super) base_url: String,
}

fn env_non_empty(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// The key file: `RELAYHISTORY_CURSOR_API_KEY_FILE`, else
/// `cursor-api-key` under RelayHistory's config directory.
pub(super) fn cursor_key_path(home: &Path) -> PathBuf {
    if let Some(path) = std::env::var_os("RELAYHISTORY_CURSOR_API_KEY_FILE") {
        if !path.is_empty() {
            return PathBuf::from(path);
        }
    }
    std::env::var_os("XDG_CONFIG_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".config"))
        .join("relayhistory/cursor-api-key")
}

fn read_key_file(path: &Path) -> Option<String> {
    let raw = std::fs::read_to_string(path).ok()?;
    raw.lines()
        .map(str::trim)
        .find(|line| !line.is_empty() && !line.starts_with('#'))
        .map(str::to_string)
}

fn resolve_key(key_path: &Path) -> Option<String> {
    env_non_empty("RELAYHISTORY_CURSOR_API_KEY")
        .or_else(|| env_non_empty("CURSOR_API_KEY"))
        .or_else(|| read_key_file(key_path))
}

pub(super) fn cursor_auth(key_path: &Path) -> Result<CursorAuth> {
    let api_key = resolve_key(key_path).context(
        "CONNECTOR_NOT_CONFIGURED: Cursor API key unavailable; set RELAYHISTORY_CURSOR_API_KEY or CURSOR_API_KEY, or write the key to the cursor-cloud key file",
    )?;
    let base_url = env_non_empty("RELAYHISTORY_CURSOR_API_BASE_URL")
        .unwrap_or_else(|| DEFAULT_BASE_URL.to_string())
        .trim_end_matches('/')
        .to_string();
    require_https_or_loopback(&base_url)
        .context("CONNECTOR_NOT_CONFIGURED: invalid Cursor API base URL")?;
    Ok(CursorAuth { api_key, base_url })
}

/// The connector's status-probe answer.
pub(super) fn cursor_status(home: &Path) -> (bool, String) {
    let path = cursor_key_path(home);
    if resolve_key(&path).is_some() {
        (true, "Cursor API key configured".to_string())
    } else {
        (
            false,
            format!(
                "no Cursor API key in RELAYHISTORY_CURSOR_API_KEY, CURSOR_API_KEY or {}",
                path.display()
            ),
        )
    }
}

pub(super) fn cursor_provider(home: &Path, limit: Option<usize>) -> CursorCloudProvider {
    CursorCloudProvider::new(cursor_key_path(home), UreqCursorTransport, limit)
}

/// `Some(provider)` only when a key resolves on this machine.
pub(super) fn cursor_provider_at(home: &Path, limit: Option<usize>) -> Option<CursorCloudProvider> {
    cursor_status(home).0.then(|| cursor_provider(home, limit))
}

// ---------------------------------------------------------------------------
// responses
// ---------------------------------------------------------------------------

/// A Cloud Agent id (`bc-…`, or `bc_…` as older v0 examples spell it).
/// Local Cursor transcripts are named by bare UUIDs, so this prefix also
/// keeps a cloud agent from ever taking a local session's identity.
pub(super) fn cursor_agent_id(id: &str) -> Option<&str> {
    let rest = id.strip_prefix("bc-").or_else(|| id.strip_prefix("bc_"))?;
    (!rest.is_empty()
        && id.len() <= MAX_AGENT_ID_LEN
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'))
    .then_some(id)
}

/// An api.cursor.com status as the connector error it stands for. The HTTP
/// status stays in the message so a host can tell throttling apart.
fn ensure_status(what: &str, status: u16, body: &str) -> Result<()> {
    match status {
        200..=299 => Ok(()),
        401 => anyhow::bail!(
            "AUTHENTICATION_EXPIRED: api.cursor.com rejected the Cursor API key (HTTP 401)"
        ),
        403 => {
            anyhow::bail!("PERMISSION_DENIED: {what} was denied for this Cursor API key (HTTP 403)")
        }
        404 | 410 => anyhow::bail!("SESSION_NOT_FOUND: {what} was not found (HTTP {status})"),
        status => anyhow::bail!(
            "CONNECTOR_FAILURE: {what} failed (HTTP {status}): {}",
            excerpt_one_line(body)
        ),
    }
}

pub(super) fn get_json(
    transport: &dyn CursorTransport,
    auth: &CursorAuth,
    url: &str,
    what: &str,
) -> Result<(Value, usize)> {
    let response = transport.get(url, &auth.api_key)?;
    ensure_status(what, response.status, &response.body)?;
    let value = serde_json::from_str(&response.body)
        .with_context(|| format!("CONNECTOR_FAILURE: {what} returned malformed JSON"))?;
    Ok((value, response.body.len()))
}

pub(super) fn iso_ms(value: &Value, key: &str) -> Option<i64> {
    string_field(value, key).and_then(|text| crate::parse_iso_ms(&text))
}

/// One `items` / `nextCursor` page. `nextCursor` is omitted, not null, on
/// the last page.
pub(super) fn page(payload: &Value, what: &str) -> Result<(Vec<Value>, Option<String>)> {
    let items = payload
        .get("items")
        .and_then(Value::as_array)
        .with_context(|| format!("CONNECTOR_FAILURE: {what} response has no items array"))?;
    Ok((items.clone(), string_field(payload, "nextCursor")))
}

pub(super) fn page_url(base: &str, path: &str, limit: usize, cursor: Option<&str>) -> String {
    let mut url = format!("{base}{path}?limit={limit}");
    if let Some(cursor) = cursor {
        url.push_str("&cursor=");
        url.push_str(&urlencode(cursor));
    }
    url
}

/// Map one `GET /v1/agents` item into a catalog row.
pub(super) fn map_cursor_agent(value: &Value) -> Option<(Candidate, ShallowSession)> {
    let id = cursor_agent_id(&string_field(value, "id")?)?.to_string();
    let created_ms = iso_ms(value, "createdAt");
    let last_activity_ms = iso_ms(value, "updatedAt").or(created_ms);
    let stamp = {
        let mut digest = Sha256::new();
        for field in ["name", "status", "updatedAt", "latestRunId", "url"] {
            digest.update(field.as_bytes());
            digest.update(string_field(value, field).unwrap_or_default().as_bytes());
        }
        format!(
            "cloud:{}:{}:{:.8}",
            string_field(value, "updatedAt").unwrap_or_else(|| "unknown".into()),
            string_field(value, "status").unwrap_or_else(|| "unknown".into()),
            format!("{:x}", digest.finalize()),
        )
    };
    let session = ShallowSession {
        source: "cursor".into(),
        session_id: id.clone(),
        // The agent's display name: Cursor derives it from the first prompt
        // unless the person renamed the agent.
        title: string_field(value, "name").map(|name| excerpt(&name)),
        first_activity_ms: created_ms,
        last_activity_ms,
        raw_path: string_field(value, "url"),
        discovery_state: "shallow".into(),
        ..Default::default()
    };
    let candidate = Candidate {
        source: "cursor",
        locator: id.clone(),
        session_id: Some(id),
        recency_hint_ms: last_activity_ms,
        stamp,
    };
    Some((candidate, session))
}

// ---------------------------------------------------------------------------
// the provider
// ---------------------------------------------------------------------------

/// Shallow adapter over the authenticated user's Cloud Agents.
pub struct CursorCloudProvider {
    key_path: PathBuf,
    transport: Box<dyn CursorTransport>,
    limit: Option<usize>,
    fetched: FetchedRows,
}

impl CursorCloudProvider {
    pub fn new(
        key_path: PathBuf,
        transport: impl CursorTransport + 'static,
        limit: Option<usize>,
    ) -> Self {
        Self {
            key_path,
            transport: Box::new(transport),
            limit,
            fetched: FetchedRows::default(),
        }
    }
}

impl ShallowSessionProvider for CursorCloudProvider {
    fn check_available(&self, _home: &Path) -> Result<()> {
        cursor_auth(&self.key_path)?;
        Ok(())
    }

    fn acquire(
        &self,
        _home: &Path,
        observation: &ai_hist::observations::SessionObservation,
    ) -> Result<RemoteSessionEvidence> {
        acquire_cursor_remote_session_at(
            &self.key_path,
            &observation.key.session_id,
            self.transport.as_ref(),
        )
    }

    fn connector_id(&self) -> &str {
        CURSOR_CLOUD_CONNECTOR
    }
    fn source(&self) -> &'static str {
        "cursor"
    }

    fn location(&self) -> SessionLocation {
        SessionLocation::Remote
    }

    fn enumerate(
        &self,
        _env: &DiscoveryEnv<'_>,
        _requested_limit: Option<usize>,
    ) -> Result<Vec<Candidate>> {
        let auth = cursor_auth(&self.key_path)?;
        let mut candidates = Vec::new();
        let mut rows = BTreeMap::new();
        let mut cursor: Option<String> = None;
        for _ in 0..MAX_LIST_PAGES {
            let limit = match self.limit {
                Some(limit) => limit
                    .saturating_sub(candidates.len())
                    .clamp(1, CURSOR_PAGE_LIMIT),
                None => CURSOR_PAGE_LIMIT,
            };
            let url = page_url(&auth.base_url, "/v1/agents", limit, cursor.as_deref());
            let (payload, _) = get_json(self.transport.as_ref(), &auth, &url, "Cursor agent list")?;
            let (items, next) = page(&payload, "agent list")?;
            for item in &items {
                if let Some((candidate, session)) = map_cursor_agent(item) {
                    rows.insert(candidate.locator.clone(), session);
                    candidates.push(candidate);
                }
            }
            cursor = next;
            if cursor.is_none()
                || items.is_empty()
                || self.limit.is_some_and(|limit| candidates.len() >= limit)
            {
                break;
            }
        }
        // An open cursor after the page bound means agents remain unlisted;
        // a caller limit that was reached is not truncation.
        anyhow::ensure!(
            cursor.is_none() || self.limit.is_some_and(|limit| candidates.len() >= limit),
            "EVIDENCE_PARTIAL: Cursor agent list exceeded the page bound"
        );
        *self
            .fetched
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = rows;
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
