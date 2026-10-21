use super::*;
use crate::UsageCoverage;
use std::collections::BTreeMap;

pub(super) fn turn_usage(rows: &[Value], native: &str) -> Result<BTreeMap<String, String>> {
    let mut current_turn = None;
    let mut last_assistant = None;
    let mut output = BTreeMap::new();
    for row in rows {
        match row["type"].as_str() {
            Some("turn.started") => {
                current_turn = (row["context"]["actor_id"] == native)
                    .then(|| row["context"]["turn_id"].as_str())
                    .flatten();
                last_assistant = None;
            }
            Some("item.completed")
                if matches!(
                    row["item"]["type"].as_str(),
                    Some("agent_message" | "tool_invocation")
                ) =>
            {
                if current_turn.is_some()
                    && row["item"]["context"]["actor_id"] == native
                    && row["item"]["context"]["turn_id"].as_str() == current_turn
                {
                    last_assistant = row["item"]["id"].as_str();
                }
            }
            Some("turn.completed" | "turn.failed") => {
                if let Some(id) = last_assistant {
                    if row["usage"].is_object() {
                        output.insert(id.to_owned(), serde_json::to_string(&row["usage"])?);
                    }
                }
                current_turn = None;
                last_assistant = None;
            }
            _ => {}
        }
    }
    Ok(output)
}

fn counter(value: &Value, field: &'static str) -> std::result::Result<Option<u64>, UsageError> {
    value
        .get(field)
        .map(|n| {
            n.as_u64().ok_or_else(|| UsageError::NonIntegerCounter {
                field,
                value: n.to_string(),
            })
        })
        .transpose()
}

pub(super) fn normalize(value: &Value) -> std::result::Result<Option<NormalizedUsage>, UsageError> {
    if !value.is_object() {
        return Err(UsageError::NotAnObject);
    }
    let input = counter(value, "input_tokens")?;
    let read = counter(value, "cached_input_tokens")?;
    let write = counter(value, "cache_creation_tokens")?;
    let output = counter(value, "output_tokens")?;
    if [input, read, write, output].iter().all(Option::is_none) {
        return Ok(None);
    }
    let exclusive = input
        .map(|n| {
            n.checked_sub(read.unwrap_or(0).saturating_add(write.unwrap_or(0)))
                .ok_or(UsageError::CounterRegressed {
                    field: "input_tokens",
                    value: n,
                    subtracted: read.unwrap_or(0).saturating_add(write.unwrap_or(0)),
                })
        })
        .transpose()?;
    Ok(Some(NormalizedUsage {
        input_tokens: exclusive.unwrap_or(0),
        output_tokens: output.unwrap_or(0),
        cache_read_tokens: read.unwrap_or(0),
        cache_write_tokens: write.unwrap_or(0),
        reasoning_tokens: None,
        cache_write_5m_tokens: None,
        cache_write_1h_tokens: None,
        provider_total_tokens: None,
        reported_cost_usd: None,
        accounting: UsageAccounting::PerRequest,
        coverage: UsageCoverage {
            has_input_tokens: input.is_some(),
            has_output_tokens: output.is_some(),
            has_cache_read_tokens: read.is_some(),
            has_cache_write_tokens: write.is_some(),
            has_reasoning_tokens: false,
        },
    }))
}
