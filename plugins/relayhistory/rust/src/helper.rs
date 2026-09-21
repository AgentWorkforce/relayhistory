//! Versioned one-request bridge. Errors never serialize remote bodies or credentials.
use crate::delivery::worker::{Receiver, ReceiverContext, ReceiverFailure};
use crate::{cloud, replay};
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
    pub delivery_request: Option<Value>,
    pub drain_options: Option<crate::delivery::rpc::DrainRequest>,
    pub connector_id: Option<String>,
    pub connector_instance: Option<String>,
    pub state: Option<serde_json::Map<String, Value>>,
    pub read_options: Option<crate::destination::ReadOptions>,
    pub batch: Option<crate::delivery::HistoryExportBatch>,
    pub prepared: Option<crate::delivery::PreparedPayload>,
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
        "probeDelivery" => crate::delivery::rpc::request(
            Path::new(
                a.db_path
                    .as_deref()
                    .context("INVALID_ARGUMENT: dbPath required")?,
            ),
            a.delivery_request
                .context("INVALID_ARGUMENT: deliveryRequest required")?,
        )?,
        "probeDeliveryDrain" => {
            let instance = a
                .instance_id
                .context("INVALID_ARGUMENT: instanceId required")?;
            let receiver = crate::destination::RelayHistoryReceiver {
                base_url: a.base_url,
                expected_account: Some(
                    a.expected_account
                        .context("INVALID_ARGUMENT: expectedAccount required")?,
                ),
                instance_id: Some(instance.clone()),
                acknowledge_uninspected_schedules: a
                    .acknowledge_uninspected_schedules
                    .unwrap_or(false),
            };
            let receivers =
                crate::delivery::worker::SingleReceiver::new("relayhistory", instance, &receiver);
            serde_json::to_value(crate::delivery::worker::drain(
                Path::new(
                    a.db_path
                        .as_deref()
                        .context("INVALID_ARGUMENT: dbPath required")?,
                ),
                &receivers,
                &a.drain_options.unwrap_or_default().options(),
                &crate::delivery::worker::system_clock,
                &|| false,
            )?)?
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
    let probe_delivery = matches!(
        request.operation.as_str(),
        "probeDelivery" | "probeDeliveryDrain"
    );
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
                .unwrap_or_else(|| {
                    if probe_delivery {
                        delivery_error_code(&error)
                    } else {
                        code
                    }
                });
            json!({"version":1,"ok":false,"error":{"code":code,"message":"RelayHistory operation failed; verify the selected stage, credentials and connectivity"}})
        }
    }
}

// Classify only recognized categories; never serialize the underlying error.
fn delivery_error_code(error: &anyhow::Error) -> &'static str {
    if crate::delivery::is_retention_limit(error) {
        "DELIVERY_RETENTION_LIMIT"
    } else if error
        .chain()
        .any(|cause| cause.to_string().starts_with("INVALID_ARGUMENT:"))
    {
        "INVALID_ARGUMENT"
    } else {
        "DELIVERY_STATE_FAILED"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reply(operation: &str, args: Value) -> Value {
        handle(
            serde_json::from_value(json!({"version":1,"operation":operation,"args":args})).unwrap(),
        )
    }

    #[test]
    fn probe_delivery_errors_preserve_categories_without_private_details() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("private-session-path.db");
        let malformed = reply(
            "probeDelivery",
            json!({"dbPath":path,"deliveryRequest":{"operation":"private-invalid-operation"}}),
        );
        assert_eq!(malformed["error"]["code"], "INVALID_ARGUMENT");
        assert!(!malformed.to_string().contains("private-invalid-operation"));
        assert_eq!(
            reply("probeDelivery", json!({}))["error"]["code"],
            "INVALID_ARGUMENT"
        );
        let state = reply(
            "probeDelivery",
            json!({"dbPath":path,"deliveryRequest":{"operation":"status","job_id":"private-session-id"}}),
        );
        assert_eq!(state["error"]["code"], "DELIVERY_STATE_FAILED");
        assert!(!state.to_string().contains("private-session-id"));
        let drain = json!({"dbPath":path,"instanceId":"fixture","expectedAccount":"fixture","drainOptions":{"leaseMs":0}});
        assert_eq!(
            reply("probeDeliveryDrain", drain)["error"]["code"],
            "INVALID_ARGUMENT"
        );
        for operation in ["probeDelivery", "probeDeliveryDrain"] {
            let args = json!({"dbPath":temp.path(),"instanceId":"fixture","expectedAccount":"fixture","deliveryRequest":{"operation":"list_jobs"}});
            let result = reply(operation, args);
            assert_eq!(result["error"]["code"], "DELIVERY_STATE_FAILED");
            assert!(!result.to_string().contains(temp.path().to_str().unwrap()));
        }
        // An actual SQLite trigger error exercises the retention classifier through the helper.
        let conn = crate::delivery::open_db(&path).unwrap();
        conn.execute_batch("CREATE TRIGGER synthetic_retention BEFORE UPDATE ON delivery_state BEGIN SELECT RAISE(ABORT, 'delivery retention limit exceeded; private diagnostic'); END;").unwrap();
        let result = reply(
            "probeDelivery",
            json!({"dbPath":path,"deliveryRequest":{"operation":"set_retention_limit","max_bytes":268435456}}),
        );
        assert_eq!(result["error"]["code"], "DELIVERY_RETENTION_LIMIT");
        assert!(!result.to_string().contains("private diagnostic"));
    }
}
