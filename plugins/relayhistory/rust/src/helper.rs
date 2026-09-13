//! Versioned one-request bridge. Errors never serialize remote bodies or credentials.
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
}
fn auth_value(auth: cloud::StoredAuth) -> Value {
    json!({"baseUrl":auth.base_url,"accessToken":auth.access_token,
        "accessTokenExpiresAt":auth.access_token_expires_at,"refreshToken":auth.refresh_token,
        "orgId":auth.org_id,"workspaceId":auth.workspace_id})
}
fn push_value(outcome: cloud::CloudPushOutcome) -> Value {
    json!({"baseUrl":outcome.base_url,"sent":outcome.sent,"accepted":outcome.accepted,"syncSkipped":outcome.sync_skipped})
}
fn execute(request: Request) -> Result<Value> {
    anyhow::ensure!(request.version == 1, "unsupported helper version");
    let a = request.args;
    let base = a.base_url.as_deref();
    Ok(match request.operation.as_str() {
        "accessToken" => json!(cloud::access_token(base)?),
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
        )?),
        "enableCloud" | "pushCloud" => {
            let path = a
                .db_path
                .map(Into::into)
                .unwrap_or_else(ai_hist_core::default_db_path);
            let outcome = if request.operation == "enableCloud" {
                cloud::enable_for_sdk(
                    &path,
                    base,
                    a.relay_access_token.as_deref(),
                    a.label.as_deref(),
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
        "createShareableTrace" => cloud::create_share(
            a.session_id.as_deref().context("sessionId required")?,
            a.visibility.as_deref().unwrap_or("direct-link"),
            a.source.as_deref(),
            base,
        )?,
        _ => bail!("unknown helper operation"),
    })
}
/// Reply with a fixed safe error category. Raw errors may contain HTTP response bodies.
pub fn handle(request: Request) -> Value {
    let code = match request.operation.as_str() {
        "accessToken" => "CLOUD_AUTH_FAILED",
        "replay" => "REPLAY_FAILED",
        "cloudLoadAuth" | "cloudResolveSession" => "CLOUD_AUTH_FAILED",
        "cloudRefreshSession" => "CONNECTOR_FAILURE",
        "cloudValidateExchangeBaseUrl" | "cloudLogin" => "CLOUD_LOGIN_FAILED",
        "enableCloud" => "CLOUD_ENABLE_FAILED",
        "pushCloud" => "CLOUD_PUSH_FAILED",
        "createShareableTrace" => "CLOUD_SHARE_FAILED",
        _ => "INVALID_ARGUMENT",
    };
    match execute(request) {
        Ok(value) => json!({"version":1,"ok":true,"value":value}),
        Err(_) => {
            json!({"version":1,"ok":false,"error":{"code":code,"message":"RelayHistory operation failed; verify the selected stage, credentials and connectivity"}})
        }
    }
}
