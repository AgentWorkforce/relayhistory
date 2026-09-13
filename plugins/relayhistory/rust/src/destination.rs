//! Versioned durable transport. The local coordinator persists prepared bytes and owns retries.
use crate::cloud;
use ai_hist_core::delivery::{
    AcceptanceLevel, DeliveryAcknowledgment, DeliveryFailure, HistoryExportBatch, PreparedPayload,
    SUPPORTED_KINDS,
};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::HashSet, io::Read, time::Duration};
pub const MAPPING_VERSION: &str = "relayhistory-delivery-v1";
pub const MAX_BODY_BYTES: usize = 2_097_152;
const MAX_RESPONSE_BYTES: u64 = 8_388_608;

#[derive(Debug)]
pub struct TransportFailure(pub DeliveryFailure);
impl std::fmt::Display for TransportFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.code())
    }
}
impl std::error::Error for TransportFailure {}
impl TransportFailure {
    pub fn code(&self) -> &'static str {
        match self.0 {
            DeliveryFailure::Transient => "DELIVERY_TRANSIENT",
            DeliveryFailure::RateLimited => "DELIVERY_RATE_LIMITED",
            DeliveryFailure::AuthenticationRequired => "DELIVERY_AUTHENTICATION_REQUIRED",
            DeliveryFailure::PermissionDenied => "DELIVERY_PERMISSION_DENIED",
            DeliveryFailure::InvalidPayload => "DELIVERY_INVALID_PAYLOAD",
            DeliveryFailure::UnsupportedEvidence => "DELIVERY_UNSUPPORTED_EVIDENCE",
            DeliveryFailure::MappingVersionMismatch => "DELIVERY_MAPPING_VERSION_MISMATCH",
        }
    }
}
fn fail(kind: DeliveryFailure) -> anyhow::Error {
    TransportFailure(kind).into()
}
fn require(condition: bool, kind: DeliveryFailure) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(fail(kind))
    }
}
fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
/// An expected-account assertion; server authentication remains authoritative.
pub fn account_id(org: &str, workspace: Option<&str>) -> String {
    format!(
        "relayhistory:{}",
        hash(
            serde_json::to_string(&[org, workspace.unwrap_or("")])
                .expect("strings serialize")
                .as_bytes()
        )
    )
}
fn check_account(auth: &cloud::StoredAuth, expected: &str) -> Result<()> {
    let org = auth
        .org_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| fail(DeliveryFailure::AuthenticationRequired))?;
    require(
        account_id(org, auth.workspace_id.as_deref()) == expected,
        DeliveryFailure::PermissionDenied,
    )
}
pub fn selected_account(base_url: Option<&str>) -> Result<String> {
    let auth = cloud::load_selected_auth(base_url)?
        .ok_or_else(|| fail(DeliveryFailure::AuthenticationRequired))?;
    let org = auth
        .org_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| fail(DeliveryFailure::AuthenticationRequired))?;
    Ok(account_id(org, auth.workspace_id.as_deref()))
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Request {
    protocol_version: u32,
    batch: HistoryExportBatch,
}
fn validate(batch: &HistoryExportBatch) -> Result<()> {
    require(
        batch.mapping_version == MAPPING_VERSION,
        DeliveryFailure::MappingVersionMismatch,
    )?;
    require(
        batch.schema_version == 1
            && batch.account_id.starts_with("relayhistory:")
            && !batch.records.is_empty()
            && batch.records.len() <= 1000,
        DeliveryFailure::InvalidPayload,
    )?;
    let mut ids = HashSet::new();
    for record in &batch.records {
        require(
            SUPPORTED_KINDS.contains(&record.kind.as_str()),
            DeliveryFailure::UnsupportedEvidence,
        )?;
        require(
            record.schema_version == 1
                && record.origin_id == batch.origin_id
                && record.revision > 0
                && record.revision <= 9_007_199_254_740_991
                && !record.record_id.is_empty()
                && !record.revision_id.is_empty()
                && ids.insert(&record.revision_id)
                && match record.operation.as_str() {
                    "delete" => record.payload.is_null(),
                    "upsert" => record.payload.is_object(),
                    _ => false,
                },
            DeliveryFailure::InvalidPayload,
        )?;
    }
    Ok(())
}
/// Prepare exactly once and persist this result in the core before calling send.
pub fn prepare(batch: HistoryExportBatch) -> Result<PreparedPayload> {
    validate(&batch)?;
    let body = serde_json::to_string(&Request {
        protocol_version: 1,
        batch,
    })?;
    require(
        body.len() <= MAX_BODY_BYTES,
        DeliveryFailure::InvalidPayload,
    )?;
    Ok(PreparedPayload {
        mapping_version: MAPPING_VERSION.into(),
        content_type: "application/json".into(),
        sha256: hash(body.as_bytes()),
        body,
    })
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Receipt {
    protocol_version: u32,
    batch_id: String,
    accepted_revision_ids: Vec<String>,
    unsupported_revision_ids: Vec<String>,
    acceptance_level: String,
}
fn receipt(value: serde_json::Value, batch: &HistoryExportBatch) -> Result<DeliveryAcknowledgment> {
    let result: Receipt =
        serde_json::from_value(value).map_err(|_| fail(DeliveryFailure::InvalidPayload))?;
    require(
        result.protocol_version == 1
            && result.batch_id == batch.batch_id
            && result.acceptance_level == "durable",
        DeliveryFailure::InvalidPayload,
    )?;
    let submitted: HashSet<_> = batch
        .records
        .iter()
        .map(|record| record.revision_id.as_str())
        .collect();
    let mut seen = HashSet::new();
    for id in result
        .accepted_revision_ids
        .iter()
        .chain(&result.unsupported_revision_ids)
    {
        require(
            submitted.contains(id.as_str()) && seen.insert(id),
            DeliveryFailure::InvalidPayload,
        )?;
    }
    Ok(DeliveryAcknowledgment {
        batch_id: result.batch_id,
        accepted_revision_ids: result.accepted_revision_ids,
        unsupported_revision_ids: result.unsupported_revision_ids,
        acceptance_level: AcceptanceLevel::Durable,
    })
}
fn http_error(error: ureq::Error) -> anyhow::Error {
    fail(match error {
        ureq::Error::Status(401, _) => DeliveryFailure::AuthenticationRequired,
        ureq::Error::Status(403, _) => DeliveryFailure::PermissionDenied,
        ureq::Error::Status(404 | 405 | 410 | 501, _) => DeliveryFailure::UnsupportedEvidence,
        ureq::Error::Status(429, _) => DeliveryFailure::RateLimited,
        ureq::Error::Status(code, _) if code >= 500 => DeliveryFailure::Transient,
        ureq::Error::Status(_, _) => DeliveryFailure::InvalidPayload,
        ureq::Error::Transport(_) => DeliveryFailure::Transient,
    })
}
fn response_json(response: ureq::Response) -> Result<serde_json::Value> {
    require(response.status() == 200, DeliveryFailure::InvalidPayload)?;
    let mut bytes = Vec::new();
    response
        .into_reader()
        .take(MAX_RESPONSE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| fail(DeliveryFailure::Transient))?;
    require(
        bytes.len() as u64 <= MAX_RESPONSE_BYTES,
        DeliveryFailure::InvalidPayload,
    )?;
    serde_json::from_slice(&bytes).map_err(|_| fail(DeliveryFailure::InvalidPayload))
}
/// Send persisted bytes unchanged. No cursor, queue or acknowledgment is mutated here.
pub fn send(base_url: Option<&str>, prepared: &PreparedPayload) -> Result<DeliveryAcknowledgment> {
    require(
        prepared.mapping_version == MAPPING_VERSION,
        DeliveryFailure::MappingVersionMismatch,
    )?;
    require(
        prepared.body.len() <= MAX_BODY_BYTES
            && prepared.content_type == "application/json"
            && prepared.sha256 == hash(prepared.body.as_bytes()),
        DeliveryFailure::InvalidPayload,
    )?;
    let request: Request =
        serde_json::from_str(&prepared.body).map_err(|_| fail(DeliveryFailure::InvalidPayload))?;
    require(
        request.protocol_version == 1,
        DeliveryFailure::MappingVersionMismatch,
    )?;
    validate(&request.batch)?;
    let auth = cloud::load_selected_auth(base_url)
        .map_err(|_| fail(DeliveryFailure::AuthenticationRequired))?
        .ok_or_else(|| fail(DeliveryFailure::AuthenticationRequired))?;
    cloud::require_secure_transport(&auth.base_url)
        .map_err(|_| fail(DeliveryFailure::PermissionDenied))?;
    let url = format!(
        "{}/v1/delivery/batches",
        cloud::normalized_stage(&auth.base_url)?
    );
    let agent = ureq::AgentBuilder::new().redirects(0).build();
    let response = cloud::send_with_auth_refresh_checked(
        &auth,
        |current| {
            agent
                .post(&url)
                .timeout(Duration::from_secs(30))
                .set("Authorization", &format!("Bearer {}", current.access_token))
                .set("Content-Type", &prepared.content_type)
                .send_bytes(prepared.body.as_bytes())
                .map_err(Box::new)
        },
        http_error,
        |current| check_account(current, &request.batch.account_id),
    )
    .map_err(|error| {
        if error.is::<TransportFailure>() {
            error
        } else {
            fail(DeliveryFailure::AuthenticationRequired)
        }
    })?;
    receipt(response_json(response)?, &request.batch)
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReadOptions {
    pub expected_account: String,
    pub kind: Option<String>,
    pub source: Option<String>,
    pub session_id: Option<String>,
    pub cursor: Option<String>,
    pub include_deleted: Option<bool>,
    pub limit: Option<usize>,
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReadPage {
    pub protocol_version: u32,
    /// Live keyset listing. A later refresh starts from the beginning; this is not a change feed.
    pub listing: String,
    pub records: Vec<ai_hist_core::delivery::HistoryExportRecord>,
    pub next_cursor: Option<String>,
}
pub fn read_page(base_url: Option<&str>, options: &ReadOptions) -> Result<ReadPage> {
    let limit = options.limit.unwrap_or(100);
    require(
        (1..=100).contains(&limit)
            && options
                .cursor
                .as_ref()
                .is_none_or(|value| value.len() <= 4096)
            && options
                .expected_account
                .strip_prefix("relayhistory:")
                .is_some_and(|value| {
                    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
                }),
        DeliveryFailure::InvalidPayload,
    )?;
    let auth = cloud::load_selected_auth(base_url)
        .map_err(|_| fail(DeliveryFailure::AuthenticationRequired))?
        .ok_or_else(|| fail(DeliveryFailure::AuthenticationRequired))?;
    cloud::require_secure_transport(&auth.base_url)
        .map_err(|_| fail(DeliveryFailure::PermissionDenied))?;
    let url = format!(
        "{}/v1/delivery/records",
        cloud::normalized_stage(&auth.base_url)?
    );
    let agent = ureq::AgentBuilder::new().redirects(0).build();
    let response = cloud::send_with_auth_refresh_checked(
        &auth,
        |current| {
            let mut request = agent
                .get(&url)
                .timeout(Duration::from_secs(30))
                .set("Authorization", &format!("Bearer {}", current.access_token))
                .set("X-RelayHistory-Expected-Account", &options.expected_account)
                .query("limit", &limit.to_string());
            for (key, value) in [
                ("kind", options.kind.as_deref()),
                ("source", options.source.as_deref()),
                ("session_id", options.session_id.as_deref()),
                ("cursor", options.cursor.as_deref()),
            ] {
                if let Some(value) = value {
                    request = request.query(key, value);
                }
            }
            if options.include_deleted.unwrap_or(false) {
                request = request.query("include_deleted", "true");
            }
            request.call().map_err(Box::new)
        },
        http_error,
        |current| check_account(current, &options.expected_account),
    )
    .map_err(|error| {
        if error.is::<TransportFailure>() {
            error
        } else {
            fail(DeliveryFailure::AuthenticationRequired)
        }
    })?;
    let page: ReadPage = serde_json::from_value(response_json(response)?)
        .map_err(|_| fail(DeliveryFailure::InvalidPayload))?;
    require(
        page.protocol_version == 1
            && page.listing == "live"
            && page.records.len() <= limit
            && page.next_cursor.as_ref().is_none_or(|value| {
                !value.is_empty() && value.len() <= 4096 && Some(value) != options.cursor.as_ref()
            }),
        DeliveryFailure::InvalidPayload,
    )?;
    let mut seen = HashSet::new();
    for record in &page.records {
        require(
            record.schema_version == 1
                && seen.insert((&record.origin_id, &record.record_id))
                && match record.operation.as_str() {
                    "upsert" => record.payload.is_object(),
                    "delete" => {
                        options.include_deleted.unwrap_or(false) && record.payload.is_null()
                    }
                    _ => false,
                }
                && SUPPORTED_KINDS.contains(&record.kind.as_str())
                && record.revision > 0
                && record.revision <= 9_007_199_254_740_991
                && options
                    .kind
                    .as_ref()
                    .is_none_or(|kind| *kind == record.kind)
                && options
                    .source
                    .as_ref()
                    .is_none_or(|source| *source == record.source)
                && options
                    .session_id
                    .as_ref()
                    .is_none_or(|session| Some(session) == record.session_id.as_ref()),
            DeliveryFailure::InvalidPayload,
        )?;
    }
    Ok(page)
}
