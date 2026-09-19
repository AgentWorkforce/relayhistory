//! Versioned one-request bridge. Errors never serialize remote bodies or credentials.
use crate::{cloud, replay};
use ai_hist::delivery::worker::{Receiver, ReceiverContext, ReceiverFailure};
use anyhow::{bail, Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use std::path::Path;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub version: u32,
    pub operation: String,
    #[serde(default)]
    pub args: Arguments,
}
#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Arguments {
    pub connector_id: Option<String>,
    pub connector_instance: Option<String>,
    pub state: Option<serde_json::Map<String, Value>>,
    pub read_options: Option<crate::destination::ReadOptions>,
    pub batch: Option<ai_hist::delivery::HistoryExportBatch>,
    pub prepared: Option<ai_hist::delivery::PreparedPayload>,
    pub expected_account: Option<String>,
    pub instance_id: Option<String>,
    pub acknowledge_uninspected_schedules: Option<bool>,
    pub base_url: Option<String>,
    pub db_path: Option<String>,
    pub now: Option<i64>,
    pub rejected_token: Option<String>,
    pub relay_access_token: Option<String>,
    pub label: Option<String>,
    pub session_id: Option<String>,
    pub visibility: Option<String>,
    pub source: Option<String>,
    pub limit: Option<usize>,
    pub max_content: Option<usize>,
    pub json: Option<bool>,
    pub out: Option<String>,
    /// May a Cloud device sign-in prompt on the controlling terminal?
    pub interactive: Option<bool>,
    pub workspace: Option<String>,
}
fn auth_value(auth: cloud::StoredAuth) -> Value {
    json!({"baseUrl":auth.base_url,"accessToken":auth.access_token,
        "accessTokenExpiresAt":auth.access_token_expires_at,"refreshToken":auth.refresh_token,
        "orgId":auth.org_id,"workspaceId":auth.workspace_id})
}
fn push_value(outcome: cloud::CloudPushOutcome) -> Value {
    json!({"baseUrl":outcome.base_url,"sent":outcome.sent,"accepted":outcome.accepted,"syncSkipped":outcome.sync_skipped})
}
/// Surface a receiver verdict through the same typed `DELIVERY_*` codes the
/// transport already reports, so a guard and a server refusal are classified
/// identically by the host.
fn transport(failure: ReceiverFailure) -> anyhow::Error {
    crate::destination::TransportFailure(failure.failure).into()
}
fn execute(request: Request) -> Result<Value> {
    anyhow::ensure!(request.version == 1, "unsupported helper version");
    let a = request.args;
    let base = a.base_url.as_deref();
    Ok(match request.operation.as_str() {
        "discover" => {
            anyhow::ensure!(
                a.connector_id.as_deref() == Some("cloud"),
                "explicit cloud connector required"
            );
            crate::source::discover(
                base,
                a.source.as_deref(),
                a.connector_instance.as_deref(),
                a.limit,
            )?
        }
        "relaycastSync" => {
            anyhow::ensure!(
                a.connector_id.as_deref() == Some("relaycast"),
                "explicit relaycast connector required"
            );
            let path = a.db_path.context("dbPath required")?;
            let conn = rusqlite::Connection::open(path)?;
            ai_hist::init_db(&conn)?;
            let mut state = a
                .state
                .context("state required; preserve the previous relay cursor map")?;
            let inserted = crate::relaycast::sync_relaycast(&conn, &mut state)?;
            json!({"inserted":inserted,"state":state,"capability":"legacy-incremental-history"})
        }
        "deliveryMigrationStatus" => serde_json::to_value(crate::migration::status())?,
        "deliveryRead" => serde_json::to_value(crate::destination::read_page(
            base,
            &a.read_options.context("readOptions required")?,
        )?)?,
        "deliveryAccount" => json!(crate::destination::selected_account(base)?),
        "deliveryPrepare" | "deliverySend" => {
            // The legacy-scheduler, account and instance guards live in the
            // receiver, so every host applies the identical rechecks.
            let receiver = crate::destination::RelayHistoryReceiver {
                base_url: a.base_url.clone(),
                expected_account: a.expected_account,
                instance_id: a.instance_id,
                acknowledge_uninspected_schedules: a
                    .acknowledge_uninspected_schedules
                    .unwrap_or(false),
            };
            let batch = a.batch.context("batch required")?;
            // The host, not this one-request bridge, owns cancellation: it
            // terminates the helper process tree instead.
            let cancelled = || false;
            let context = ReceiverContext {
                cancelled: &cancelled,
                timeout_ms: 30_000,
                idempotency_key: &batch.batch_id,
            };
            if request.operation == "deliveryPrepare" {
                serde_json::to_value(crate::destination::seal(
                    receiver.prepare(&batch, &context).map_err(transport)?,
                ))?
            } else {
                serde_json::to_value(
                    receiver
                        .send(&a.prepared.context("prepared required")?, &batch, &context)
                        .map_err(transport)?,
                )?
            }
        }
        "accessToken" => json!(cloud::access_token(base)?),
        "syncAndPush" => serde_json::to_value(crate::compat::sync_and_push()?)?,
        "cloudLoadAuth" => cloud::load_selected_auth(base)?
            .map(auth_value)
            .unwrap_or(Value::Null),
        "cloudResolveSession" => {
            match cloud::resolve_recall_auth(base, a.now.context("now required")?, true) {
                Ok(auth) => json!({"auth":auth_value(auth),"detail":null}),
                Err(_) => {
                    json!({"auth":null,"detail":"No eligible stored RelayHistory session for the selected stage"})
                }
            }
        }
        "cloudRefreshSession" => cloud::refresh_rejected_auth(
            base.context("baseUrl required")?,
            a.rejected_token
                .as_deref()
                .context("rejectedToken required")?,
        )?
        .map(auth_value)
        .unwrap_or(Value::Null),
        "cloudValidateExchangeBaseUrl" => {
            cloud::validate_cloud_exchange_base_url_for_sdk(base)?;
            Value::Null
        }
        "cloudLogin" => auth_value(cloud::login_for_sdk(
            base,
            a.relay_access_token.as_deref(),
            a.label.as_deref(),
            a.interactive.unwrap_or(false),
            a.workspace.as_deref(),
        )?),
        "enableCloud" | "pushCloud" => {
            let path = a
                .db_path
                .map(Into::into)
                .unwrap_or_else(ai_hist::default_db_path);
            let outcome = if request.operation == "enableCloud" {
                cloud::enable_for_sdk(
                    &path,
                    base,
                    a.relay_access_token.as_deref(),
                    a.label.as_deref(),
                    a.interactive.unwrap_or(false),
                    a.workspace.as_deref(),
                )?
            } else {
                cloud::push_for_sdk(&path, base)?
            };
            push_value(outcome)
        }
        "replay" => {
            let result = replay::replay(
                a.session_id.as_deref().context("sessionId required")?,
                base,
                a.limit,
                a.max_content,
                a.json.unwrap_or(false),
                a.out.as_deref().map(Path::new),
            )?;
            json!({"eventCount":result.event_count,"transcript":result.transcript,"outputPath":result.output_path})
        }
        "createShareableTrace" => json!(cloud::create_share(
            a.session_id.as_deref().context("sessionId required")?,
            a.visibility.as_deref().unwrap_or("direct-link"),
            a.source.as_deref(),
            base,
        )?
        .to_string()),
        _ => bail!("unknown helper operation"),
    })
}
/// Reply with a fixed safe error category. Raw errors may contain HTTP response bodies.
pub fn handle(request: Request) -> Value {
    if request.version != 1 {
        return json!({"version":1,"ok":false,"error":{"code":"INVALID_ARGUMENT","message":"Unsupported helper protocol version"}});
    }
    let code = match request.operation.as_str() {
        "discover" | "relaycastSync" => "CONNECTOR_FAILURE",
        "accessToken" => "CLOUD_AUTH_FAILED",
        "replay" => "REPLAY_FAILED",
        "cloudLoadAuth" | "cloudResolveSession" => "CLOUD_AUTH_FAILED",
        "cloudRefreshSession" => "CONNECTOR_FAILURE",
        "cloudValidateExchangeBaseUrl" | "cloudLogin" => "CLOUD_LOGIN_FAILED",
        "enableCloud" => "CLOUD_ENABLE_FAILED",
        "pushCloud" | "syncAndPush" => "CLOUD_PUSH_FAILED",
        "createShareableTrace" => "CLOUD_SHARE_FAILED",
        _ => "INVALID_ARGUMENT",
    };
    match execute(request) {
        Ok(value) => json!({"version":1,"ok":true,"value":value}),
        Err(error) => {
            let code = error
                .downcast_ref::<crate::destination::TransportFailure>()
                .map(|error| error.code())
                .unwrap_or(code);
            json!({"version":1,"ok":false,"error":{"code":code,"message":"RelayHistory operation failed; verify the selected stage, credentials and connectivity"}})
        }
    }
}
