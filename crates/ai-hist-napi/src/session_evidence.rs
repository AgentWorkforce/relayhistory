//! Full-session reads through the public store facade.
use ai_hist::{EvidenceKind, SessionQuery, SessionRef, Source};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{
    db_path, native_error, validate_identity, NativeNormalizedUsage, NativeSessionRequest,
    NativeSessionUserTurn,
};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Args {
    db_path: Option<String>,
    source: Source,
    session_id: String,
    include_text: Option<bool>,
    kinds: Option<Vec<EvidenceKind>>,
}

pub(crate) fn dispatch(args_json: &str) -> napi::Result<String> {
    let args: Args =
        serde_json::from_str(args_json).map_err(|error| native_error("INVALID_ARGUMENT", error))?;
    let id = validate_identity(args.session_id, "sessionId")?;
    let mut query = SessionQuery::default();
    query.include_text = args.include_text.unwrap_or(true);
    query.kinds = args.kinds;
    let evidence = super::session_store::read_existing(db_path(args.db_path), |store| {
        store.session(&SessionRef::id(args.source, id.clone()), query.clone())
    })?
    .flatten();
    let evidence = evidence.map(wire_evidence).transpose()?;
    serde_json::to_string(&json!({ "evidence": evidence }))
        .map_err(|error| native_error("DATABASE_QUERY_FAILED", error))
}

/// Preserve opaque provider JSON. Only usage and turn flags need the same
/// JavaScript-safe conversions as the paged operations.
fn wire_evidence(evidence: ai_hist::SessionEvidence) -> napi::Result<Value> {
    let encode = |error| native_error("DATABASE_QUERY_FAILED", error);
    let mut value = serde_json::to_value(&evidence).map_err(encode)?;
    for (raw, message) in value["messages"]
        .as_array_mut()
        .into_iter()
        .flatten()
        .zip(&evidence.messages)
    {
        let usage = message.usage.as_ref().and_then(|usage| {
            NativeNormalizedUsage::from_core(usage, usage.accounting.as_str().to_string())
        });
        if message.usage.is_some() && usage.is_none() {
            raw["usage_error"] = json!(crate::COUNT_NOT_REPRESENTABLE);
        }
        raw["usage"] = serde_json::to_value(usage).map_err(encode)?;
    }
    value["requests"] = serde_json::to_value(
        evidence
            .requests
            .into_iter()
            .map(NativeSessionRequest::from)
            .collect::<Vec<_>>(),
    )
    .map_err(encode)?;
    value["user_turns"] = serde_json::to_value(
        evidence
            .user_turns
            .into_iter()
            .map(NativeSessionUserTurn::from)
            .collect::<Vec<_>>(),
    )
    .map_err(encode)?;
    value["usage"] = match evidence.usage {
        Some(summary) => serde_json::to_value(crate::session_usage(
            evidence.session.source.to_string(),
            evidence.session.session_id,
            Some(summary),
        ))
        .map_err(encode)?,
        None => Value::Null,
    };
    Ok(value)
}
