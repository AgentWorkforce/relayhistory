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
pub(super) fn normalize(v: &Value) -> std::result::Result<Option<NormalizedUsage>, UsageError> {
    if !v.is_object() {
        return Err(UsageError::NotAnObject);
    }
    let input = counter(v, "promptTokenCount")?;
    let output = counter(v, "candidatesTokenCount")?;
    let read = counter(v, "cachedContentTokenCount")?;
    let write = None;
    let total = counter(v, "totalTokenCount")?;
    if [input, output, read, write, total]
        .iter()
        .all(Option::is_none)
    {
        return Ok(None);
    }
    // Native promptTokenCount includes cachedContentTokenCount. The native
    // recorder loses its WeakMap cache-write/reporting provenance at JSONL
    // serialization, so no cache-write or measured reasoning count is inferred.
    let exclusive = input
        .map(|n| {
            n.checked_sub(read.unwrap_or(0))
                .ok_or(UsageError::CounterRegressed {
                    field: "promptTokenCount",
                    value: n,
                    subtracted: read.unwrap_or(0),
                })
        })
        .transpose()?;
    let cost = None;
    Ok(Some(NormalizedUsage {
        input_tokens: exclusive.unwrap_or(0),
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
