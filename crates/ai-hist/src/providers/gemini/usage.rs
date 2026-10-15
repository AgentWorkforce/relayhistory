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
    let input = counter(v, "input")?;
    let output = counter(v, "output")?;
    let read = counter(v, "cached")?;
    let total = counter(v, "total")?;
    let thoughts = counter(v, "thoughts")?;
    counter(v, "tool")?;
    if [input, output, read, total, thoughts]
        .iter()
        .all(Option::is_none)
    {
        return Ok(None);
    }
    let exclusive_input = input
        .map(|value| {
            value
                .checked_sub(read.unwrap_or(0))
                .ok_or(UsageError::CounterRegressed {
                    field: "input",
                    value,
                    subtracted: read.unwrap_or(0),
                })
        })
        .transpose()?;
    Ok(Some(NormalizedUsage {
        input_tokens: exclusive_input.unwrap_or(0),
        output_tokens: output.unwrap_or(0),
        cache_read_tokens: read.unwrap_or(0),
        cache_write_tokens: 0,
        reasoning_tokens: thoughts,
        cache_write_5m_tokens: None,
        cache_write_1h_tokens: None,
        provider_total_tokens: total,
        reported_cost_usd: None,
        accounting: UsageAccounting::PerMessage,
        coverage: UsageCoverage {
            has_input_tokens: input.is_some(),
            has_output_tokens: output.is_some(),
            has_cache_read_tokens: read.is_some(),
            has_cache_write_tokens: false,
            has_reasoning_tokens: thoughts.is_some(),
        },
    }))
}
