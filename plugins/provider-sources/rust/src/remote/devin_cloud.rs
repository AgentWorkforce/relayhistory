//! `devin-cloud` — the api.devin.ai v3 organization session lister,
//! message hydrator, and message sender.
//!
//! Sign-in stays owned by the Devin CLI, exactly like the other connectors:
//! the bearer comes from Devin's own `credentials.toml` (`windsurf_api_key`,
//! which is a `cog_` service key or personal token) or the explicit
//! `RELAYHISTORY_DEVIN_API_KEY` override, and the organization id comes from
//! Devin's `config.json` (`devin.org_id`) or `RELAYHISTORY_DEVIN_ORG_ID`.
//! Reads need `ViewOrgSessions`; `send`'s `POST .../messages` needs the
//! broader `ManageOrgSessions`, and a denied send names that scope.

use std::collections::{BTreeMap, HashSet};
use std::io::Read;
use std::path::{Path, PathBuf};

use ai_hist::{EvidenceKind, EvidenceRecord, SessionLocation};

use anyhow::{Context, Result};
use rusqlite::Connection;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

use crate::discover::{Candidate, DiscoveryEnv, ScanEnv, ShallowSession, ShallowSessionProvider};

use super::claude_web::{excerpt_one_line, urlencode};
use super::devin_auth::{devin_auth, DevinAuth};
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
    fn post(&self, url: &str, bearer_token: &str, body: &Value) -> Result<DevinHttpResponse>;
}

pub(super) struct UreqDevinTransport;

impl DevinTransport for UreqDevinTransport {
    fn get(&self, url: &str, bearer_token: &str) -> Result<DevinHttpResponse> {
        let request = devin_agent()
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

    fn post(&self, url: &str, bearer_token: &str, body: &Value) -> Result<DevinHttpResponse> {
        match devin_agent()
            .post(url)
            .timeout(std::time::Duration::from_secs(30))
            .set("Authorization", &format!("Bearer {bearer_token}"))
            .send_json(body)
        {
            Ok(response) => bounded_devin_response(response.status(), response),
            Err(ureq::Error::Status(status, response)) => bounded_devin_response(status, response),
            Err(error) => Err(anyhow::Error::from(error).context("api.devin.ai request")),
        }
    }
}

fn devin_agent() -> ureq::Agent {
    // Redirects are never followed: ureq would re-send the Authorization
    // header to the redirect target, so a redirecting endpoint could move
    // the stored credential to a host the https-or-loopback guard never
    // saw. A 3xx therefore surfaces as a failed request, not a hop.
    ureq::AgentBuilder::new().redirects(0).build()
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
/// `scope` names the permission a 403 asks the key holder for — reads need
/// `ViewOrgSessions`, sends need `ManageOrgSessions`. Every 2xx is success:
/// a POST may answer 201 or 204, and treating an acknowledged send as a
/// failure would invite a duplicate retry.
fn ensure_devin_status(what: &str, scope: &str, status: u16, body: &str) -> Result<()> {
    match status {
        200..=299 => Ok(()),
        401 => anyhow::bail!(
            "AUTHENTICATION_EXPIRED: api.devin.ai rejected the Devin API key (HTTP 401)"
        ),
        403 => {
            anyhow::bail!("PERMISSION_DENIED: {what} was denied; the Devin API key needs {scope}")
        }
        404 => anyhow::bail!("SESSION_NOT_FOUND: {what} was not found"),
        422 => anyhow::bail!(
            "INVALID_ARGUMENT: api.devin.ai rejected {what} (HTTP 422): {}",
            excerpt_one_line(body)
        ),
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
    // without a timestamp bump still deserves a re-read. So does a digest of
    // the mapped listing fields — a title edit that the API reports without
    // advancing `updated_at` must still refresh the catalog row.
    let stamp = {
        let mut digest = Sha256::new();
        for field in [
            "title",
            "devin_mode",
            "origin",
            "url",
            "status",
            "updated_at",
        ] {
            digest.update(field.as_bytes());
            digest.update(
                value
                    .get(field)
                    .map(|v| match v.as_str() {
                        Some(text) => text.to_string(),
                        None => v.to_string(),
                    })
                    .unwrap_or_default()
                    .as_bytes(),
            );
        }
        format!(
            "cloud:{}:{}:{:.8}",
            value
                .get("updated_at")
                .and_then(Value::as_i64)
                .map(|seconds| seconds.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            string_field(value, "status").unwrap_or_else(|| "unknown".to_string()),
            format!("{:x}", digest.finalize()),
        )
    };
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
/// The intake contract requires every session event to carry `ts_ms` and a
/// `user`/`assistant`/`tool_result` role, and every history record a
/// `timestamp_ms` — so messages without an event id, without text, without a
/// usable timestamp, or from a non-conversational source are skipped rather
/// than emitted as records validation would reject (one bad record fails the
/// whole snapshot).
fn push_devin_message_records(
    records: &mut Vec<EvidenceRecord>,
    seen_prompts: &mut HashSet<(i64, String)>,
    session_id: &str,
    item: &Value,
) {
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
    let Some(ts_ms) = item
        .get("created_at")
        .and_then(Value::as_i64)
        .map(|seconds| seconds.saturating_mul(SECONDS_TO_MS))
    else {
        return;
    };
    let Some(source) = string_field(item, "source") else {
        return;
    };
    let role = match source.as_str() {
        "user" => "user",
        "devin" => "assistant",
        _ => return,
    };
    let mut event = Map::new();
    event.insert("source".into(), json!("devin"));
    event.insert("session_id".into(), json!(session_id));
    event.insert("ts_ms".into(), json!(ts_ms));
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
    // A user message is also the session's prompt history, the same way the
    // local adapter records it. History identity is (timestamp_ms, prompt)
    // with no event id, so two identical prompts inside the same second would
    // collide — the event record above still preserves the second occurrence.
    if source == "user" && seen_prompts.insert((ts_ms, text.to_string())) {
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
    let mut seen_prompts = HashSet::new();
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
        ensure_devin_status(
            "Devin message history",
            "ViewOrgSessions",
            response.status,
            &response.body,
        )?;
        source_bytes = source_bytes.saturating_add(response.body.len());
        anyhow::ensure!(
            source_bytes <= MAX_REMOTE_EVIDENCE_BYTES,
            "CONNECTOR_FAILURE: remote Devin evidence exceeded the 16 MiB response-size limit"
        );
        hasher.update(response.body.as_bytes());
        let page = parse_devin_page(&response.body, "message history")?;
        for item in &page.items {
            push_devin_message_records(&mut records, &mut seen_prompts, session_id, item);
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
            ensure_devin_status(
                "Devin session list",
                "ViewOrgSessions",
                response.status,
                &response.body,
            )?;
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
        // An open cursor after the page bound means sessions remain unlisted;
        // reporting the prefix as complete would silently lose them. A
        // caller-specified limit that was reached is not truncation.
        anyhow::ensure!(
            cursor.is_none() || self.limit.is_some_and(|limit| candidates.len() >= limit),
            "EVIDENCE_PARTIAL: Devin session list exceeded the 100-page bound"
        );
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
// send — POST .../sessions/{devin_id}/messages (ManageOrgSessions)
// ---------------------------------------------------------------------------

/// Post one message into a remote Devin session:
/// `POST /v3/organizations/{org}/sessions/{devin_id}/messages`, the route the
/// v3 API documents for sending into an active session (a suspended session
/// is resumed). Needs `ManageOrgSessions` — strictly broader than the read
/// scope — so a 403 on this path names the missing scope instead of implying
/// the connector was never configured.
pub(super) fn send_devin_session_message_at(
    credentials_path: &Path,
    config_path: &Path,
    session_id: &str,
    message: &str,
    transport: &dyn DevinTransport,
) -> Result<()> {
    let Some(hex) = devin_session_hex(session_id) else {
        anyhow::bail!("INVALID_ARGUMENT: remote Devin session id is malformed");
    };
    anyhow::ensure!(
        !message.trim().is_empty() && message.len() <= super::MAX_REMOTE_MESSAGE_BYTES,
        "INVALID_ARGUMENT: message must be non-empty and within {} bytes",
        super::MAX_REMOTE_MESSAGE_BYTES,
    );
    let auth = devin_auth(credentials_path, config_path)?;
    let url = format!(
        "{}/v3/organizations/{}/sessions/devin-{hex}/messages",
        auth.base_url, auth.org_id,
    );
    let body = json!({ "message": message });
    let response = transport.post(&url, &auth.bearer, &body)?;
    ensure_devin_status(
        "Devin session message",
        "ManageOrgSessions",
        response.status,
        &response.body,
    )
}
