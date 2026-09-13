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
fn execute(request: Request) -> Result<Value> {
    ensure!(request.version == 1, "unsupported helper version");
    ensure!(
        matches!(request.operation.as_str(), "discover" | "hydrate"),
        "unknown operation"
    );
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
                observations.push(row);
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
    match execute(request) {
        Ok(value) => json!({"version":1,"ok":true,"value":value}),
        Err(_) => {
            json!({"version":1,"ok":false,"error":{"code":"CONNECTOR_FAILURE","message":"Provider source operation failed; verify explicit connector configuration and provider CLI sign-in"}})
        }
    }
}
