//! `devin-cloud` — the api.devin.ai v3 organization session lister and
//! message hydrator.
//!
//! Sign-in stays owned by the Devin CLI, exactly like the other connectors:
//! the bearer comes from Devin's own `credentials.toml` (`windsurf_api_key`,
//! which is a `cog_` service key or personal token) or the explicit
//! `RELAYHISTORY_DEVIN_API_KEY` override, and the organization id comes from
//! Devin's `config.json` (`devin.org_id`) or `RELAYHISTORY_DEVIN_ORG_ID`. The
//! connector only reads: `GET .../sessions` and `GET .../messages` need
//! `ViewOrgSessions`, never `ManageOrgSessions`.

use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};

use ai_hist::{EvidenceKind, EvidenceRecord, SessionLocation};
use anyhow::{Context, Result};
use rusqlite::Connection;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

use crate::discover::{Candidate, DiscoveryEnv, ScanEnv, ShallowSession, ShallowSessionProvider};

use super::claude_web::{excerpt_one_line, require_https_or_loopback, urlencode};
use super::{
    excerpt, string_field, take_fetched, FetchedRows, RemoteSessionEvidence, DEVIN_CLOUD_CONNECTOR,
    MAX_LIST_PAGES, MAX_REMOTE_EVIDENCE_BYTES,
};

/// Rows requested per listing page; the endpoint accepts a `first` page size.
const DEVIN_PAGE_LIMIT: usize = 200;
/// api.devin.ai speaks epoch *seconds*; the ledger stores epoch milliseconds.
const SECONDS_TO_MS: i64 = 1_000;

// ---------------------------------------------------------------------------
// transport
// ---------------------------------------------------------------------------

/// One HTTP response from an api.devin.ai endpoint.
pub struct DevinHttpResponse {
    pub status: u16,
    pub body: String,
}

/// The HTTP side of the Devin organization API, abstracted so mapping and
/// pagination are testable without a network.
pub trait DevinTransport: Send + Sync {
    fn get(&self, url: &str, bearer_token: &str) -> Result<DevinHttpResponse>;
}

pub(super) struct UreqDevinTransport;

impl DevinTransport for UreqDevinTransport {
    fn get(&self, url: &str, bearer_token: &str) -> Result<DevinHttpResponse> {
        // Redirects are never followed: ureq would re-send the Authorization
        // header to the redirect target, so a redirecting endpoint could move
        // the stored credential to a host the https-or-loopback guard never
        // saw. A 3xx therefore surfaces as a failed request, not a hop.
        let agent = ureq::AgentBuilder::new().redirects(0).build();
        let request = agent
            .get(url)
            .timeout(std::time::Duration::from_secs(30))
            .set("Authorization", &format!("Bearer {bearer_token}"))
            .set("Content-Type", "application/json");
        match request.call() {
            Ok(response) => bounded_devin_response(response.status(), response),
            Err(ureq::Error::Status(status, response)) => bounded_devin_response(status, response),
            Err(error) => Err(anyhow::Error::from(error).context("api.devin.ai request")),
        }
    }
}

fn bounded_devin_response(status: u16, response: ureq::Response) -> Result<DevinHttpResponse> {
    let mut bytes = Vec::new();
    response
        .into_reader()
        .take((MAX_REMOTE_EVIDENCE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    anyhow::ensure!(
        bytes.len() <= MAX_REMOTE_EVIDENCE_BYTES,
        "Devin response exceeded the 16 MiB response-size limit"
    );
    Ok(DevinHttpResponse {
        status,
        body: String::from_utf8_lossy(&bytes).into_owned(),
    })
}

// ---------------------------------------------------------------------------
// configuration — Devin CLI files plus explicit environment overrides
// ---------------------------------------------------------------------------

/// The Devin API coordinates resolved from the Devin CLI's own files and
/// explicit environment overrides. The bearer is never logged or returned.
pub(super) struct DevinAuth {
    bearer: String,
    org_id: String,
    base_url: String,
}

fn env_non_empty(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// The Devin credential file: `RELAYHISTORY_DEVIN_CREDENTIALS`, else the
/// CLI's `credentials.toml` under its data directory.
pub(super) fn devin_credentials_path(home: &Path) -> PathBuf {
    if let Some(path) = std::env::var_os("RELAYHISTORY_DEVIN_CREDENTIALS") {
        if !path.is_empty() {
            return PathBuf::from(path);
        }
    }
    devin_data_home(home).join("credentials.toml")
}

/// The Devin CLI config: `RELAYHISTORY_DEVIN_CONFIG`, else `config.json`
/// under its config directory. Holds `devin.org_id`.
pub(super) fn devin_config_path(home: &Path) -> PathBuf {
    if let Some(path) = std::env::var_os("RELAYHISTORY_DEVIN_CONFIG") {
        if !path.is_empty() {
            return PathBuf::from(path);
        }
    }
    devin_config_home(home).join("config.json")
}

fn devin_data_home(home: &Path) -> PathBuf {
    std::env::var_os("XDG_DATA_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".local/share"))
        .join("devin")
}

fn devin_config_home(home: &Path) -> PathBuf {
    std::env::var_os("XDG_CONFIG_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".config"))
        .join("devin")
}

/// One `key = "value"` line's value from Devin's flat `credentials.toml` —
/// deliberately a line scan, not a TOML dependency for a two-key file.
pub(super) fn toml_value(raw: &str, key: &str) -> Option<String> {
    for line in raw.lines() {
        let line = line.trim();
        if line.starts_with('#') {
            continue;
        }
        let Some((name, value)) = line.split_once('=') else {
            continue;
        };
        if name.trim() != key {
            continue;
        }
        if let Some(unquoted) = value
            .trim()
            .strip_prefix('"')
            .and_then(|v| v.strip_suffix('"'))
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            return Some(unquoted.to_string());
        }
    }
    None
}

/// `devin.org_id` from the Devin CLI's `config.json`, when present.
pub(super) fn devin_org_id(config_path: &Path) -> Option<String> {
    let raw = std::fs::read_to_string(config_path).ok()?;
    let value: Value = serde_json::from_str(&raw).ok()?;
    value
        .pointer("/devin/org_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn read_devin_credentials(path: &Path) -> (Option<String>, Option<String>) {
    let Ok(raw) = std::fs::read_to_string(path) else {
        return (None, None);
    };
    (
        toml_value(&raw, "windsurf_api_key"),
        toml_value(&raw, "devin_api_url"),
    )
}

fn devin_auth(credentials_path: &Path, config_path: &Path) -> Result<DevinAuth> {
    let (credential_key, credential_url) = read_devin_credentials(credentials_path);
    let bearer = env_non_empty("RELAYHISTORY_DEVIN_API_KEY")
        .or(credential_key)
        .context("CONNECTOR_NOT_CONFIGURED: Devin API key unavailable; sign in with the Devin CLI or set RELAYHISTORY_DEVIN_API_KEY")?;
    let org_id = env_non_empty("RELAYHISTORY_DEVIN_ORG_ID")
        .or_else(|| devin_org_id(config_path))
        .context("CONNECTOR_NOT_CONFIGURED: Devin organization id unavailable; set devin.org_id in the Devin CLI config or RELAYHISTORY_DEVIN_ORG_ID")?;
    let base_url = env_non_empty("RELAYHISTORY_DEVIN_API_BASE_URL")
        .or(credential_url)
        .unwrap_or_else(|| "https://api.devin.ai".to_string());
    let base_url = base_url.trim_end_matches('/').to_string();
    require_https_or_loopback(&base_url)
        .context("CONNECTOR_NOT_CONFIGURED: invalid Devin API base URL")?;
    Ok(DevinAuth {
        bearer,
        org_id,
        base_url,
    })
}

/// Whether a credentials file and an organization id both resolve under this
/// home — the status probe's notion of "configured".
pub(super) fn devin_auth_ready(home: &Path) -> bool {
    let credentials = devin_credentials_path(home);
    let credential_key = read_devin_credentials(&credentials).0;
    let has_key = env_non_empty("RELAYHISTORY_DEVIN_API_KEY").is_some() || credential_key.is_some();
    let has_org = env_non_empty("RELAYHISTORY_DEVIN_ORG_ID").is_some()
        || devin_org_id(&devin_config_path(home)).is_some();
    has_key && has_org
}

/// The connector's status-probe answer.
pub(super) fn devin_status(home: &Path) -> (bool, String) {
    let path = devin_credentials_path(home);
    if devin_auth_ready(home) {
        (true, format!("Devin CLI sign-in at {}", path.display()))
    } else {
        (
            false,
            format!(
                "no Devin API key at {} (or RELAYHISTORY_DEVIN_API_KEY) and no org id (or RELAYHISTORY_DEVIN_ORG_ID)",
                path.display()
            ),
        )
    }
}

/// Provider construction shared by `provider()` and bulk selection.
pub(super) fn devin_provider(home: &Path, limit: Option<usize>) -> DevinCloudProvider {
    DevinCloudProvider::new(
        devin_credentials_path(home),
        devin_config_path(home),
        Box::new(UreqDevinTransport),
        limit,
    )
}

/// `Some(provider)` only when the connector is configured on this machine.
pub(super) fn devin_provider_at(home: &Path, limit: Option<usize>) -> Option<DevinCloudProvider> {
    devin_auth_ready(home).then(|| devin_provider(home, limit))
}

// ---------------------------------------------------------------------------
// listing — GET /v3/organizations/{org}/sessions?first=N&after=cursor
// ---------------------------------------------------------------------------

/// One page of the v3 `items` / `end_cursor` / `has_next_page` envelope, used
/// by both the session list and the per-session message history.
pub(super) struct DevinPage {
    pub(super) items: Vec<Value>,
    pub(super) end_cursor: Option<String>,
    pub(super) has_next_page: bool,
}

pub(super) fn parse_devin_page(raw: &str, what: &str) -> Result<DevinPage> {
    let payload: Value = serde_json::from_str(raw)
        .with_context(|| format!("CONNECTOR_FAILURE: Devin {what} returned malformed JSON"))?;
    let items = payload
        .get("items")
        .and_then(Value::as_array)
        .with_context(|| format!("CONNECTOR_FAILURE: Devin {what} response has no items array"))?;
    Ok(DevinPage {
        items: items.clone(),
        end_cursor: string_field(&payload, "end_cursor"),
        has_next_page: payload
            .get("has_next_page")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    })
}

/// The bare session id embedded in a v3 `devin_id` (`devin-<32 hex>`), or a
/// bare 32-hex `session_id` as the list endpoint reports it. `None` for
/// anything else — local Devin sessions carry names, not hex ids.
pub(super) fn devin_session_hex(id: &str) -> Option<&str> {
    let hex = id.strip_prefix("devin-").unwrap_or(id);
    (hex.len() == 32 && hex.bytes().all(|b| b.is_ascii_hexdigit())).then_some(hex)
}

/// An api.devin.ai response status, as the connector error it stands for.
fn ensure_devin_status(what: &str, status: u16, body: &str) -> Result<()> {
    match status {
        200 => Ok(()),
        401 => anyhow::bail!(
            "AUTHENTICATION_EXPIRED: api.devin.ai rejected the Devin API key (HTTP 401)"
        ),
        403 => anyhow::bail!(
            "CONNECTOR_FAILURE: {what} was denied; the Devin API key needs ViewOrgSessions"
        ),
        404 => anyhow::bail!("SESSION_NOT_FOUND: {what} was not found"),
        status => anyhow::bail!(
            "CONNECTOR_FAILURE: {what} failed (HTTP {status}): {}",
            excerpt_one_line(body)
        ),
    }
}

/// Map one session object from `GET /v3/organizations/{org}/sessions` into a
/// catalog row. The listing's `title` is the only human-readable identifier
/// the endpoint offers; `None` for a malformed id.
pub(super) fn map_devin_session(value: &Value) -> Option<(Candidate, ShallowSession)> {
    let id = string_field(value, "session_id")?;
    let id = devin_session_hex(&id)?.to_string();
    let created_ms = value
        .get("created_at")
        .and_then(Value::as_i64)
        .map(|seconds| seconds.saturating_mul(SECONDS_TO_MS));
    let last_activity_ms = value
        .get("updated_at")
        .and_then(Value::as_i64)
        .map(|seconds| seconds.saturating_mul(SECONDS_TO_MS))
        .or(created_ms);
    // Status participates in the stamp: a session that suspends or exits
    // without a timestamp bump still deserves a re-read.
    let stamp = format!(
        "cloud:{}:{}",
        value
            .get("updated_at")
            .and_then(Value::as_i64)
            .map(|seconds| seconds.to_string())
            .unwrap_or_else(|| "unknown".to_string()),
        string_field(value, "status").unwrap_or_else(|| "unknown".to_string())
    );
    let session = ShallowSession {
        source: "devin".into(),
        session_id: id.clone(),
        first_prompt: string_field(value, "title").map(|title| excerpt(&title)),
        first_activity_ms: created_ms,
        last_activity_ms,
        // `devin_mode` (`fusion`, …) is the closest thing the listing offers
        // to a model identifier; it is observed, not inferred.
        models: string_field(value, "devin_mode").into_iter().collect(),
        originator: string_field(value, "origin"),
        raw_path: string_field(value, "url"),
        discovery_state: "shallow".into(),
        ..Default::default()
    };
    let candidate = Candidate {
        source: "devin",
        // The prefixed form is the acquisition handle: the v3 messages
        // endpoint addresses a session as `devin-<id>`.
        locator: format!("devin-{id}"),
        session_id: Some(id),
        recency_hint_ms: last_activity_ms,
        stamp,
    };
    Some((candidate, session))
}

fn devin_page_url(auth: &DevinAuth, path: &str, limit: usize, cursor: Option<&str>) -> String {
    let mut url = format!(
        "{base}/v3/organizations/{org}/{path}?first={limit}",
        base = auth.base_url,
        org = auth.org_id
    );
    if let Some(cursor) = cursor {
        url.push_str("&after=");
        url.push_str(&urlencode(cursor));
    }
    url
}

// ---------------------------------------------------------------------------
// message hydration — GET .../sessions/{devin_id}/messages
// ---------------------------------------------------------------------------

/// One `{event_id, source, message, created_at}` entry, as normalized records.
/// Messages without an event id or without text carry nothing a ledger can
/// key or display, so they are skipped rather than invented around.
fn push_devin_message_records(records: &mut Vec<EvidenceRecord>, session_id: &str, item: &Value) {
    let Some(event_id) = string_field(item, "event_id") else {
        return;
    };
    let Some(text) = item
        .get("message")
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
    else {
        return;
    };
    let ts_ms = item
        .get("created_at")
        .and_then(Value::as_i64)
        .map(|seconds| seconds.saturating_mul(SECONDS_TO_MS));
    let source = string_field(item, "source").unwrap_or_else(|| "unknown".to_string());
    let role = match source.as_str() {
        "user" => "user",
        "devin" => "assistant",
        other => other,
    };
    let mut event = Map::new();
    event.insert("source".into(), json!("devin"));
    event.insert("session_id".into(), json!(session_id));
    if let Some(ts_ms) = ts_ms {
        event.insert("ts_ms".into(), json!(ts_ms));
    }
    event.insert("role".into(), json!(role));
    event.insert("kind".into(), json!("text"));
    event.insert("text".into(), json!(text));
    event.insert("message_id".into(), json!(event_id));
    event.insert("event_uid".into(), json!(event_id));
    records.push(EvidenceRecord {
        kind: EvidenceKind::SessionEvent,
        payload: event,
        record_id: None,
        revision_id: None,
    });
    if source == "user" {
        // A user message is also the session's prompt history, the same way
        // the local adapter records it.
        records.push(EvidenceRecord {
            kind: EvidenceKind::History,
            payload: Map::from_iter([
                ("source".into(), json!("devin")),
                ("session_id".into(), json!(session_id)),
                ("prompt".into(), json!(text)),
                ("timestamp_ms".into(), json!(ts_ms)),
            ]),
            record_id: None,
            revision_id: None,
        });
    }
}

/// Hydrate one remote Devin session into a normalized evidence snapshot.
pub(super) fn acquire_devin_remote_session_at(
    credentials_path: &Path,
    config_path: &Path,
    session_id: &str,
    transport: &dyn DevinTransport,
) -> Result<RemoteSessionEvidence> {
    let Some(hex) = devin_session_hex(session_id) else {
        anyhow::bail!("INVALID_ARGUMENT: remote Devin session id is malformed");
    };
    let auth = match devin_auth(credentials_path, config_path) {
        Ok(auth) => auth,
        Err(_) => {
            return Ok(RemoteSessionEvidence::CapabilityLimited {
                code: "CONNECTOR_NOT_CONFIGURED",
                message: "devin-cloud is not configured; sign in with the Devin CLI or set RELAYHISTORY_DEVIN_API_KEY and RELAYHISTORY_DEVIN_ORG_ID".to_string(),
            })
        }
    };
    let mut records = Vec::new();
    let mut cursor: Option<String> = None;
    let mut source_bytes = 0usize;
    let mut hasher = Sha256::new();
    for _ in 0..MAX_LIST_PAGES {
        let url = devin_page_url(
            &auth,
            &format!("sessions/devin-{hex}/messages"),
            DEVIN_PAGE_LIMIT,
            cursor.as_deref(),
        );
        let response = transport.get(&url, &auth.bearer)?;
        ensure_devin_status("Devin message history", response.status, &response.body)?;
        source_bytes = source_bytes.saturating_add(response.body.len());
        anyhow::ensure!(
            source_bytes <= MAX_REMOTE_EVIDENCE_BYTES,
            "CONNECTOR_FAILURE: remote Devin evidence exceeded the 16 MiB response-size limit"
        );
        hasher.update(response.body.as_bytes());
        let page = parse_devin_page(&response.body, "message history")?;
        for item in &page.items {
            push_devin_message_records(&mut records, session_id, item);
        }
        cursor = page.end_cursor.filter(|_| page.has_next_page);
        if cursor.is_none() || page.items.is_empty() {
            return Ok(RemoteSessionEvidence::Normalized(
                ai_hist::sources::NormalizedSourceEvidence {
                    source_stamp: format!("messages:{:x}", hasher.finalize()),
                    source_bytes: source_bytes as i64,
                    covered_kinds: vec![EvidenceKind::History, EvidenceKind::SessionEvent],
                    records,
                },
            ));
        }
    }
    anyhow::bail!("EVIDENCE_PARTIAL: Devin message history exceeded the 100-page bound")
}

// ---------------------------------------------------------------------------
// the provider
// ---------------------------------------------------------------------------

/// Shallow adapter over the api.devin.ai organization session list.
pub struct DevinCloudProvider {
    credentials_path: PathBuf,
    config_path: PathBuf,
    transport: Box<dyn DevinTransport>,
    limit: Option<usize>,
    fetched: FetchedRows,
}

impl DevinCloudProvider {
    pub fn new(
        credentials_path: PathBuf,
        config_path: PathBuf,
        transport: Box<dyn DevinTransport>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            credentials_path,
            config_path,
            transport,
            limit,
            fetched: FetchedRows::default(),
        }
    }
}

impl ShallowSessionProvider for DevinCloudProvider {
    fn check_available(&self, _home: &Path) -> Result<()> {
        devin_auth(&self.credentials_path, &self.config_path)?;
        Ok(())
    }

    fn acquire(
        &self,
        _home: &Path,
        observation: &ai_hist::observations::SessionObservation,
    ) -> Result<RemoteSessionEvidence> {
        acquire_devin_remote_session_at(
            &self.credentials_path,
            &self.config_path,
            &observation.key.session_id,
            self.transport.as_ref(),
        )
    }

    fn connector_id(&self) -> &str {
        DEVIN_CLOUD_CONNECTOR
    }
    fn source(&self) -> &'static str {
        "devin"
    }

    fn location(&self) -> SessionLocation {
        SessionLocation::Remote
    }

    fn enumerate(
        &self,
        _env: &DiscoveryEnv<'_>,
        _requested_limit: Option<usize>,
    ) -> Result<Vec<Candidate>> {
        let auth = devin_auth(&self.credentials_path, &self.config_path)?;
        let mut candidates = Vec::new();
        let mut rows = BTreeMap::new();
        let mut cursor: Option<String> = None;
        for _ in 0..MAX_LIST_PAGES {
            let page_limit = match self.limit {
                Some(limit) => limit
                    .saturating_sub(candidates.len())
                    .clamp(1, DEVIN_PAGE_LIMIT),
                None => DEVIN_PAGE_LIMIT,
            };
            let url = devin_page_url(&auth, "sessions", page_limit, cursor.as_deref());
            let response = self.transport.get(&url, &auth.bearer)?;
            ensure_devin_status("Devin session list", response.status, &response.body)?;
            let page = parse_devin_page(&response.body, "session list")?;
            for item in &page.items {
                if let Some((candidate, session)) = map_devin_session(item) {
                    rows.insert(candidate.locator.clone(), session);
                    candidates.push(candidate);
                }
            }
            cursor = page.end_cursor.filter(|_| page.has_next_page);
            let done = cursor.is_none()
                || page.items.is_empty()
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
