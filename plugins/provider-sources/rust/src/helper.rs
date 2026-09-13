//! Bounded one-request bridge for explicitly selected provider-native adapters.
use ai_hist_core::{observations::SessionObservation, SessionLocation};
use ai_hist_engine::discover::DiscoveryEnv;
use anyhow::{ensure, Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};

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
    pub source: Option<String>,
    pub limit: Option<usize>,
    pub observation: Option<SessionObservation>,
}
// Validate caller-controlled identities before resolving HOME, constructing an
// adapter, or probing credentials. Classification is based on this phase, never
// on error text returned by a provider or transport.
fn validate_request(request: &Request) -> Result<()> {
    ensure!(request.version == 1, "unsupported helper version");
    ensure!(
        matches!(request.operation.as_str(), "discover" | "hydrate"),
        "unknown operation"
    );
    let args = &request.args;
    let connector = args
        .connector_id
        .as_deref()
        .context("connectorId required")?;
    let source = match connector {
        crate::remote::CLAUDE_WEB_CONNECTOR => "claude",
        crate::remote::CODEX_CLOUD_CONNECTOR => "codex",
        _ => anyhow::bail!("unknown source connector"),
    };
    let instance = args.connector_instance.as_deref().unwrap_or("default");
    ensure!(
        !instance.is_empty()
            && instance.trim() == instance
            && instance.len() <= 512
            && !instance.chars().any(char::is_control),
        "invalid connector instance"
    );
    ensure!(
        args.limit.is_none_or(|limit| limit <= 10_000),
        "invalid source limit"
    );
    ensure!(
        args.source
            .as_deref()
            .is_none_or(|selected| selected == source),
        "source mismatch"
    );
    if request.operation == "hydrate" {
        let observation = args.observation.as_ref().context("observation required")?;
        observation.key.validate()?;
        ensure!(
            observation.key.connector_id == connector
                && observation.key.connector_instance == instance
                && observation.key.source == source
                && observation.key.location == SessionLocation::Remote,
            "observation identity mismatch"
        );
        ensure!(
            ["available", "unavailable", "withdrawn"].contains(&observation.access_state.as_str()),
            "invalid observation access state"
        );
        ensure!(
            ["shallow", "full"].contains(&observation.discovery_state.as_str()),
            "invalid observation discovery state"
        );
    }
    Ok(())
}
fn execute(request: Request) -> Result<Value> {
    let args = request.args;
    let connector = args.connector_id.context("connectorId required")?;
    let instance = args.connector_instance.as_deref().unwrap_or("default");
    let home = std::env::var_os("HOME")
        .filter(|value| !value.is_empty())
        .context("HOME required")?;
    let home = std::path::Path::new(&home);
    let provider = crate::remote::provider(home, &connector, instance, args.limit)?;
    if let Some(source) = &args.source {
        ensure!(provider.source() == source, "source mismatch");
    }
    if request.operation == "discover" {
        provider.check_available(home)?;
        // Provider enumeration takes the public engine context. This ephemeral
        // connection cannot mutate the caller's history or acquisition cursor.
        let conn = rusqlite::Connection::open_in_memory()?;
        let env = DiscoveryEnv::with_roots(&conn, home.into(), home.join("unused-opencode.db"));
        let candidates = provider.enumerate(&env, args.limit)?;
        let mut observations = Vec::new();
        for candidate in candidates {
            if let Some(mut row) = provider.read_shallow(&env.scan(), None, &candidate)? {
                row.source_stamp = Some(candidate.stamp.clone());
                row.locations = vec!["remote".into()];
                let mut value = serde_json::to_value(row)?;
                value
                    .as_object_mut()
                    .context("source observation object required")?
                    .insert("raw_locator".into(), json!(candidate.locator));
                observations.push(value);
            }
        }
        Ok(json!({"observations": observations}))
    } else {
        let observation = args.observation.context("observation required")?;
        observation.key.validate()?;
        ensure!(
            observation.key.connector_id == connector
                && observation.key.connector_instance == instance
                && observation.key.source == provider.source()
                && observation.key.location == SessionLocation::Remote,
            "observation identity mismatch"
        );
        ensure!(
            observation.access_state == "available",
            "observation unavailable"
        );
        provider.check_available(home)?;
        let evidence = provider.acquire(home, &observation)?;
        let normalized = ai_hist_engine::sources::normalize_source_evidence(
            provider.source(),
            &observation.key.session_id,
            evidence,
        )?;
        Ok(serde_json::to_value(normalized)?)
    }
}
pub fn handle(request: Request) -> Value {
    if validate_request(&request).is_err() {
        return json!({"version":1,"ok":false,"error":{"code":"INVALID_ARGUMENT","message":"Invalid provider source request; verify operation, connector identity, source and limits"}});
    }
    match execute(request) {
        Ok(value) => json!({"version":1,"ok":true,"value":value}),
        Err(_) => {
            json!({"version":1,"ok":false,"error":{"code":"CONNECTOR_FAILURE","message":"Provider source operation failed; verify explicit connector configuration and provider CLI sign-in"}})
        }
    }
}
