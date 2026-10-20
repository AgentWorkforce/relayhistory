use super::*;
use crate::UsageCoverage;
pub(super) fn normalize(v: &Value) -> std::result::Result<Option<NormalizedUsage>, UsageError> {
    if !v.is_object() {
        return Err(UsageError::NotAnObject);
    }
    let count = |key: &'static str| {
        v.get(key)
            .map(|n| {
                n.as_u64().ok_or_else(|| UsageError::NonIntegerCounter {
                    field: key,
                    value: n.to_string(),
                })
            })
            .transpose()
    };
    let input = count("prompt_tokens")?;
    let output = count("completion_tokens")?;
    let total = count("total_tokens")?;
    let cached = v["prompt_tokens_details"]
        .get("cached_tokens")
        .map(|n| {
            n.as_u64().ok_or_else(|| UsageError::NonIntegerCounter {
                field: "cached_tokens",
                value: n.to_string(),
            })
        })
        .transpose()?;
    let exclusive = input
        .map(|n| {
            n.checked_sub(cached.unwrap_or(0))
                .ok_or(UsageError::CounterRegressed {
                    field: "prompt_tokens",
                    value: n,
                    subtracted: cached.unwrap_or(0),
                })
        })
        .transpose()?;
    let reasoning = v["completion_tokens_details"]
        .get("reasoning_tokens")
        .map(|n| {
            n.as_u64().ok_or_else(|| UsageError::NonIntegerCounter {
                field: "reasoning_tokens",
                value: n.to_string(),
            })
        })
        .transpose()?;
    if input.is_none() && output.is_none() && total.is_none() {
        return Ok(None);
    }
    Ok(Some(NormalizedUsage {
        input_tokens: exclusive.unwrap_or(0),
        output_tokens: output.unwrap_or(0),
        provider_total_tokens: total,
        cache_read_tokens: cached.unwrap_or(0),
        cache_write_tokens: 0,
        reasoning_tokens: reasoning,
        cache_write_5m_tokens: None,
        cache_write_1h_tokens: None,
        reported_cost_usd: None,
        accounting: UsageAccounting::PerMessage,
        coverage: UsageCoverage {
            has_input_tokens: input.is_some(),
            has_output_tokens: output.is_some(),
            has_cache_read_tokens: cached.is_some(),
            has_cache_write_tokens: false,
            has_reasoning_tokens: reasoning.is_some(),
        },
    }))
}
