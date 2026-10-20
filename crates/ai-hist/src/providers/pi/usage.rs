use super::*;
use crate::UsageCoverage;
fn counter(v: &Value, key: &'static str) -> std::result::Result<Option<u64>, UsageError> {
    v.get(key)
        .map(|n| {
            n.as_u64().ok_or_else(|| UsageError::NonIntegerCounter {
                field: key,
                value: n.to_string(),
            })
        })
        .transpose()
}
pub(crate) fn normalize(v: &Value) -> std::result::Result<Option<NormalizedUsage>, UsageError> {
    if !v.is_object() {
        return Err(UsageError::NotAnObject);
    }
    let input = counter(v, "input")?;
    let output = counter(v, "output")?;
    let read = counter(v, "cacheRead")?;
    let write = counter(v, "cacheWrite")?;
    let total = counter(v, "totalTokens")?;
    if [input, output, read, write, total]
        .iter()
        .all(Option::is_none)
    {
        return Ok(None);
    }
    let cost = v
        .get("cost")
        .and_then(|c| c.get("total"))
        .map(|n| {
            n.as_f64()
                .filter(|n| n.is_finite() && *n >= 0.0)
                .ok_or_else(|| UsageError::InvalidCost {
                    value: n.to_string(),
                })
        })
        .transpose()?;
    Ok(Some(NormalizedUsage {
        input_tokens: input.unwrap_or(0),
        output_tokens: output.unwrap_or(0),
        cache_read_tokens: read.unwrap_or(0),
        cache_write_tokens: write.unwrap_or(0),
        reasoning_tokens: None,
        cache_write_5m_tokens: None,
        cache_write_1h_tokens: None,
        provider_total_tokens: total,
        reported_cost_usd: cost,
        accounting: UsageAccounting::PerMessage,
        coverage: UsageCoverage {
            has_input_tokens: input.is_some(),
            has_output_tokens: output.is_some(),
            has_cache_read_tokens: read.is_some(),
            has_cache_write_tokens: write.is_some(),
            has_reasoning_tokens: false,
        },
    }))
}
