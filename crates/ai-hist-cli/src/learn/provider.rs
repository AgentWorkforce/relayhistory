use super::{
    iso, LearnDistillOptions, LearnProvider, SessionTranscript, DEFAULT_ANTHROPIC_BASE_URL,
    DEFAULT_ANTHROPIC_MODEL, DEFAULT_OPENAI_BASE_URL, DEFAULT_OPENAI_MODEL, LLM_TIMEOUT,
};
use ai_hist::privacy::normalize_home_path;
use anyhow::{anyhow, bail, Result};
use chrono::Utc;
use serde_json::{json, Value};

#[derive(Debug, Clone)]
pub(super) struct ProviderConfig {
    provider: LearnProvider,
    model: String,
    base_url: String,
    api_key: String,
    json_mode: bool,
}

pub(super) fn resolve_provider(options: &LearnDistillOptions) -> Result<ProviderConfig> {
    let provider = match options.provider {
        LearnProvider::Auto => {
            if has_local_base_url("OPENAI_BASE_URL", DEFAULT_OPENAI_BASE_URL)
                || std::env::var("OPENAI_API_KEY").is_ok()
            {
                LearnProvider::OpenAi
            } else if has_local_base_url("ANTHROPIC_BASE_URL", DEFAULT_ANTHROPIC_BASE_URL)
                || std::env::var("ANTHROPIC_API_KEY").is_ok()
            {
                LearnProvider::Anthropic
            } else {
                bail!("no Learn distill provider configured; set OPENAI_BASE_URL for a local OpenAI-compatible model or pass --allow-cloud-llm with OPENAI_API_KEY/ANTHROPIC_API_KEY");
            }
        }
        other => other,
    };

    let (base_url, api_key, model, json_mode) = match provider {
        LearnProvider::Auto => unreachable!(),
        LearnProvider::OpenAi => {
            let base_url = options
                .base_url
                .clone()
                .or_else(|| std::env::var("OPENAI_BASE_URL").ok())
                .unwrap_or_else(|| DEFAULT_OPENAI_BASE_URL.to_string());
            enforce_locality(&base_url, DEFAULT_OPENAI_BASE_URL, options.allow_cloud_llm)?;
            let api_key = std::env::var("OPENAI_API_KEY").unwrap_or_else(|_| "local".to_string());
            let model = options
                .model
                .clone()
                .or_else(|| std::env::var("LEARN_DISTILL_MODEL").ok())
                .or_else(|| std::env::var("TRAJECTORIES_LLM_MODEL").ok())
                .unwrap_or_else(|| DEFAULT_OPENAI_MODEL.to_string());
            (base_url, api_key, model, true)
        }
        LearnProvider::Anthropic => {
            let base_url = options
                .base_url
                .clone()
                .or_else(|| std::env::var("ANTHROPIC_BASE_URL").ok())
                .unwrap_or_else(|| DEFAULT_ANTHROPIC_BASE_URL.to_string());
            enforce_locality(
                &base_url,
                DEFAULT_ANTHROPIC_BASE_URL,
                options.allow_cloud_llm,
            )?;
            let api_key = std::env::var("ANTHROPIC_API_KEY").unwrap_or_else(|_| {
                if base_url.trim_end_matches('/') == DEFAULT_ANTHROPIC_BASE_URL {
                    String::new()
                } else {
                    "local".to_string()
                }
            });
            if api_key.is_empty() {
                bail!("ANTHROPIC_API_KEY is required for Anthropic Learn distill against api.anthropic.com");
            }
            let model = options
                .model
                .clone()
                .or_else(|| std::env::var("LEARN_DISTILL_MODEL").ok())
                .or_else(|| std::env::var("TRAJECTORIES_LLM_MODEL").ok())
                .unwrap_or_else(|| DEFAULT_ANTHROPIC_MODEL.to_string());
            (base_url, api_key, model, false)
        }
    };

    Ok(ProviderConfig {
        provider,
        model,
        base_url,
        api_key,
        json_mode,
    })
}

fn has_local_base_url(var: &str, default: &str) -> bool {
    std::env::var(var)
        .ok()
        .is_some_and(|v| !v.trim().is_empty() && v.trim_end_matches('/') != default)
}

fn enforce_locality(base_url: &str, default: &str, allow_cloud_llm: bool) -> Result<()> {
    if !allow_cloud_llm && base_url.trim_end_matches('/') == default {
        bail!(
            "Learn distill refuses cloud LLM by default because full session transcripts are pre-scrub. Use a local base URL (OPENAI_BASE_URL/ANTHROPIC_BASE_URL) or pass --allow-cloud-llm after explicit user consent."
        );
    }
    Ok(())
}

pub(super) fn complete(
    provider: &ProviderConfig,
    messages: &[(String, String)],
    max_tokens: usize,
) -> Result<String> {
    match provider.provider {
        LearnProvider::OpenAi => complete_openai(provider, messages, max_tokens),
        LearnProvider::Anthropic => complete_anthropic(provider, messages, max_tokens),
        LearnProvider::Auto => unreachable!(),
    }
}

fn complete_openai(
    provider: &ProviderConfig,
    messages: &[(String, String)],
    max_tokens: usize,
) -> Result<String> {
    let body = json!({
        "model": provider.model,
        "messages": messages.iter().map(|(role, content)| json!({"role": role, "content": content})).collect::<Vec<_>>(),
        "max_tokens": max_tokens,
        "temperature": 0.2,
        "response_format": if provider.json_mode { json!({"type":"json_object"}) } else { Value::Null },
    });
    let response: Value = ureq::post(&format!(
        "{}/v1/chat/completions",
        provider.base_url.trim_end_matches('/')
    ))
    .set("authorization", &format!("Bearer {}", provider.api_key))
    .set("content-type", "application/json")
    .timeout(LLM_TIMEOUT)
    .send_json(body)?
    .into_json()?;
    response
        .pointer("/choices/0/message/content")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| {
            anyhow!("OpenAI-compatible response did not include choices[0].message.content")
        })
}

fn complete_anthropic(
    provider: &ProviderConfig,
    messages: &[(String, String)],
    max_tokens: usize,
) -> Result<String> {
    let system = messages
        .iter()
        .filter(|(role, _)| role == "system")
        .map(|(_, content)| content.as_str())
        .collect::<Vec<_>>()
        .join("\n\n");
    let conversation = messages
        .iter()
        .filter(|(role, _)| role != "system")
        .map(|(role, content)| json!({"role": role, "content": content}))
        .collect::<Vec<_>>();
    let body = json!({
        "model": provider.model,
        "system": system,
        "messages": conversation,
        "max_tokens": max_tokens,
        "temperature": 0.2,
    });
    let response: Value = ureq::post(&format!(
        "{}/v1/messages",
        provider.base_url.trim_end_matches('/')
    ))
    .set("x-api-key", &provider.api_key)
    .set("anthropic-version", "2024-10-22")
    .set("content-type", "application/json")
    .timeout(LLM_TIMEOUT)
    .send_json(body)?
    .into_json()?;
    let text = response
        .get("content")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter(|item| item.get("type").and_then(Value::as_str) == Some("text"))
                .filter_map(|item| item.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default();
    if text.trim().is_empty() {
        bail!("Anthropic response did not include text content");
    }
    Ok(text)
}

pub(super) fn learn_rollup_from_output(
    id: &str,
    transcript: &SessionTranscript,
    output: &str,
) -> Result<Value> {
    let parsed = parse_json_output(output)?;
    let obj = parsed
        .as_object()
        .ok_or_else(|| anyhow!("Learn distill output must be a JSON object"))?;
    let decisions = object_array(obj.get("decisions"));
    let conventions = object_array(obj.get("conventions"));
    let lessons = object_array(obj.get("lessons"));
    let key_findings = string_array(obj.get("keyFindings"));
    let key_learnings = string_array(obj.get("keyLearnings"));
    let open_questions = string_array(obj.get("openQuestions"));
    let narrative = obj
        .get("narrative")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim();

    Ok(json!({
        "id": id,
        "type": "compacted",
        "source": "learn",
        "lens": "learn",
        "tags": ["learn"],
        "compactedAt": Utc::now().to_rfc3339(),
        "sourceTrajectories": [format!("session:{}:{}", transcript.candidate.source, transcript.candidate.session_id)],
        "sourceSessions": [{
            "source": transcript.candidate.source,
            "sessionId": transcript.candidate.session_id,
        }],
        "dateRange": {
            "start": iso(transcript.candidate.first_ms),
            "end": iso(transcript.candidate.last_ms),
        },
        "summary": {
            "totalDecisions": decisions.len(),
            "totalEvents": decisions.len() + conventions.len() + lessons.len() + key_findings.len() + key_learnings.len() + open_questions.len(),
            "uniqueAgents": [transcript.candidate.source.clone()],
        },
        "narrative": normalize_home_path(narrative),
        "decisions": decisions,
        "conventions": conventions,
        "lessons": lessons,
        "keyFindings": key_findings,
        "keyLearnings": key_learnings,
        "openQuestions": open_questions,
        "decisionGroups": [],
        "filesAffected": [],
        "commits": [],
    }))
}

fn parse_json_output(output: &str) -> Result<Value> {
    let trimmed = output.trim();
    let stripped = trimmed
        .strip_prefix("```json")
        .or_else(|| trimmed.strip_prefix("```"))
        .and_then(|s| s.strip_suffix("```"))
        .map(str::trim)
        .unwrap_or(trimmed);
    Ok(serde_json::from_str(stripped)?)
}

fn object_array(value: Option<&Value>) -> Vec<Value> {
    value
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter(|item| item.is_object())
                .cloned()
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn string_array(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(normalize_home_path)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}
