//! Generic source plugins acquire externally; this boundary only validates and stores.
use ai_hist::source_intake as engine;
use napi_derive::napi;
use serde::{de::DeserializeOwned, Serialize};
fn execute<T: DeserializeOwned, R: Serialize>(
    json: String,
    operation: impl FnOnce(T) -> anyhow::Result<R>,
) -> napi::Result<String> {
    let request = serde_json::from_str(&json)
        .map_err(|_| crate::native_error("INVALID_ARGUMENT", "invalid source intake request"))?;
    let result = operation(request).map_err(|error| {
        let message = format!("{error:#}");
        let code = [
            "SOURCE_REVISION_CONFLICT",
            "INVALID_ARGUMENT",
            "SESSION_NOT_FOUND",
            "CONNECTOR_NOT_CONFIGURED",
            "DELIVERY_RETENTION_LIMIT",
        ]
        .into_iter()
        .find(|code| message.contains(code))
        .unwrap_or("SOURCE_INTAKE_FAILED");
        crate::native_error(code, error)
    })?;
    serde_json::to_string(&result)
        .map_err(|error| crate::native_error("SOURCE_INTAKE_FAILED", error))
}
#[napi]
pub async fn get_source_observation(request_json: String) -> napi::Result<String> {
    napi::tokio::task::spawn_blocking(move || execute(request_json, engine::get_source_observation))
        .await
        .map_err(crate::worker_error)?
}
#[napi]
pub async fn apply_source_observations(request_json: String) -> napi::Result<String> {
    napi::tokio::task::spawn_blocking(move || {
        execute(request_json, engine::apply_source_observations)
    })
    .await
    .map_err(crate::worker_error)?
}
#[napi]
pub async fn apply_source_evidence(request_json: String) -> napi::Result<String> {
    napi::tokio::task::spawn_blocking(move || execute(request_json, engine::apply_source_evidence))
        .await
        .map_err(crate::worker_error)?
}
