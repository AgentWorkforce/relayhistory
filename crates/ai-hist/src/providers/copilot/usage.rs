//! Copilot CLI persists one usage counter per assistant message:
//! `assistant.message.data.outputTokens`. Input and cache counters exist only
//! as session-wide cumulative totals on `session.shutdown`, which stay raw in
//! markers and never become request usage here.
use super::*;
use crate::UsageCoverage;

pub(super) fn normalize(v: &Value) -> std::result::Result<Option<NormalizedUsage>, UsageError> {
    let Some(object) = v.as_object() else {
        return Err(UsageError::NotAnObject);
    };
    let Some(n) = object.get("outputTokens") else {
        return Ok(None);
    };
    let output = n.as_u64().ok_or_else(|| UsageError::NonIntegerCounter {
        field: "outputTokens",
        value: n.to_string(),
    })?;
    Ok(Some(NormalizedUsage {
        input_tokens: 0,
        output_tokens: output,
        cache_read_tokens: 0,
        cache_write_tokens: 0,
        reasoning_tokens: None,
        cache_write_5m_tokens: None,
        cache_write_1h_tokens: None,
        provider_total_tokens: None,
        reported_cost_usd: None,
        accounting: UsageAccounting::PerMessage,
        coverage: UsageCoverage {
            has_output_tokens: true,
            ..Default::default()
        },
    }))
}
