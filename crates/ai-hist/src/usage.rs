//! Provider-neutral token usage normalization.
//!
//! Every provider reports usage in its own shape, on its own accounting
//! unit, and with its own idea of what "input" excludes. This module is the
//! single place that translates those shapes into [`NormalizedUsage`], so a
//! consumer never has to know that Claude's `input_tokens` already excludes
//! cache reads while Codex's does not.
//!
//! Two rules make the output trustworthy:
//!
//! * **Nothing is estimated.** `provider_total_tokens` is whatever the
//!   provider wrote, never a recomputed sum, and `reported_cost_usd` is
//!   present only when the source data carried a cost. Pricing belongs to
//!   burn.
//! * **Nothing is silently clamped.** A negative, fractional, non-finite, or
//!   out-of-range counter is a [`UsageError`], not a zero. Clamping would
//!   turn a broken provider record into a well-formed wrong number, which is
//!   exactly the failure mode that is impossible to notice downstream.
//!
//! [`UsageCoverage`] records which counters the provider actually reported,
//! so "the model produced no output" stays distinguishable from "this record
//! does not say how much output there was".
use crate::store::SessionEvent;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fmt;

/// How a source accounts for the usage it reports.
///
/// This is a property of the *record*, not of the provider's billing: it says
/// what one stored `token_json` blob stands for, which is what a consumer
/// needs in order to know whether summing rows is meaningful.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum UsageAccounting {
    /// One API request, reported once. Summing across requests is exact.
    PerRequest,
    /// One assistant message, copied onto every content block of that
    /// message. Rows must be deduplicated by message before summing;
    /// counting rows multiplies one request by its block count.
    PerMessage,
    /// A cumulative counter differenced into a per-request delta at parse
    /// time. Deltas sum back to the provider's final cumulative total.
    CumulativeDelta,
    /// A context-window occupancy figure, not a billed request. Never sum.
    ContextProxy,
}

impl UsageAccounting {
    /// Stable kebab-case wire label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PerRequest => "per-request",
            Self::PerMessage => "per-message",
            Self::CumulativeDelta => "cumulative-delta",
            Self::ContextProxy => "context-proxy",
        }
    }

    /// Parse a wire label back, or `None` for a label this build does not know.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "per-request" => Some(Self::PerRequest),
            "per-message" => Some(Self::PerMessage),
            "cumulative-delta" => Some(Self::CumulativeDelta),
            "context-proxy" => Some(Self::ContextProxy),
            _ => None,
        }
    }
}

impl fmt::Display for UsageAccounting {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Which counters the provider actually wrote.
///
/// A zero with `has_output_tokens: false` is missing evidence; a zero with
/// `has_output_tokens: true` is a real reported zero. Collapsing the two is
/// how a summary ends up confidently reporting `0` for a session that in fact
/// never said.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageCoverage {
    pub has_input_tokens: bool,
    pub has_output_tokens: bool,
    pub has_reasoning_tokens: bool,
    pub has_cache_read_tokens: bool,
    pub has_cache_write_tokens: bool,
}

impl UsageCoverage {
    /// Whether the provider reported every counter this record could carry.
    pub fn is_complete(&self) -> bool {
        self.has_input_tokens
            && self.has_output_tokens
            && self.has_cache_read_tokens
            && self.has_cache_write_tokens
    }

    fn merged(self, other: Self) -> Self {
        Self {
            has_input_tokens: self.has_input_tokens || other.has_input_tokens,
            has_output_tokens: self.has_output_tokens || other.has_output_tokens,
            has_reasoning_tokens: self.has_reasoning_tokens || other.has_reasoning_tokens,
            has_cache_read_tokens: self.has_cache_read_tokens || other.has_cache_read_tokens,
            has_cache_write_tokens: self.has_cache_write_tokens || other.has_cache_write_tokens,
        }
    }
}

/// One source record's usage, in provider-neutral terms.
///
/// `input_tokens` always **excludes** cache reads, whatever the provider's
/// own convention was. `cache_write_tokens` is the total; the `5m`/`1h` split
/// is `None` when the provider does not distinguish cache TTLs, because a
/// consumer that prices them differently must be able to tell "not split"
/// from "split, and one side is zero".
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct NormalizedUsage {
    /// Ordinary input tokens, excluding cache reads.
    pub input_tokens: u64,
    pub output_tokens: u64,
    /// `None` when the provider does not report reasoning separately.
    pub reasoning_tokens: Option<u64>,
    pub cache_read_tokens: u64,
    /// Total cache-write tokens across every TTL bucket.
    pub cache_write_tokens: u64,
    /// Claude `cache_creation.ephemeral_5m_input_tokens`, when split.
    pub cache_write_5m_tokens: Option<u64>,
    /// Claude `cache_creation.ephemeral_1h_input_tokens`, when split.
    pub cache_write_1h_tokens: Option<u64>,
    /// As the provider reported it. Never recomputed from the parts, so a
    /// mismatch between this and the sum stays visible instead of being
    /// papered over.
    pub provider_total_tokens: Option<u64>,
    /// Present only when the source data carried a cost. This crate never
    /// prices anything.
    pub reported_cost_usd: Option<f64>,
    pub accounting: UsageAccounting,
    pub coverage: UsageCoverage,
}

impl NormalizedUsage {
    fn empty(accounting: UsageAccounting) -> Self {
        Self {
            input_tokens: 0,
            output_tokens: 0,
            reasoning_tokens: None,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
            cache_write_5m_tokens: None,
            cache_write_1h_tokens: None,
            provider_total_tokens: None,
            reported_cost_usd: None,
            accounting,
            coverage: UsageCoverage::default(),
        }
    }

    /// Whether every counter on this record is zero or absent.
    ///
    /// An all-zero record is real evidence that the provider reported zeros,
    /// so this is a question callers ask rather than something normalization
    /// decides for them.
    pub fn is_zero(&self) -> bool {
        self.input_tokens == 0
            && self.output_tokens == 0
            && self.reasoning_tokens.unwrap_or(0) == 0
            && self.cache_read_tokens == 0
            && self.cache_write_tokens == 0
            && self.provider_total_tokens.unwrap_or(0) == 0
            && self.reported_cost_usd.unwrap_or(0.0) == 0.0
    }

    /// Add two records of the same accounting mode, or `None` on overflow.
    ///
    /// Overflow returns `None` rather than saturating for the same reason
    /// normalization errors rather than clamps: a saturated total is a
    /// plausible-looking number that no longer means anything.
    pub fn checked_add(&self, other: &Self) -> Option<Self> {
        Some(Self {
            input_tokens: self.input_tokens.checked_add(other.input_tokens)?,
            output_tokens: self.output_tokens.checked_add(other.output_tokens)?,
            reasoning_tokens: checked_add_optional(self.reasoning_tokens, other.reasoning_tokens)?,
            cache_read_tokens: self
                .cache_read_tokens
                .checked_add(other.cache_read_tokens)?,
            cache_write_tokens: self
                .cache_write_tokens
                .checked_add(other.cache_write_tokens)?,
            cache_write_5m_tokens: checked_add_optional(
                self.cache_write_5m_tokens,
                other.cache_write_5m_tokens,
            )?,
            cache_write_1h_tokens: checked_add_optional(
                self.cache_write_1h_tokens,
                other.cache_write_1h_tokens,
            )?,
            provider_total_tokens: checked_add_optional(
                self.provider_total_tokens,
                other.provider_total_tokens,
            )?,
            reported_cost_usd: match (self.reported_cost_usd, other.reported_cost_usd) {
                (None, None) => None,
                (a, b) => {
                    let sum = a.unwrap_or(0.0) + b.unwrap_or(0.0);
                    Some(sum.is_finite().then_some(sum)?)
                }
            },
            accounting: self.accounting,
            coverage: self.coverage.merged(other.coverage),
        })
    }
}

/// `None + None` stays `None`; anything reported makes the sum reported.
fn checked_add_optional(a: Option<u64>, b: Option<u64>) -> Option<Option<u64>> {
    match (a, b) {
        (None, None) => Some(None),
        (a, b) => a.unwrap_or(0).checked_add(b.unwrap_or(0)).map(Some),
    }
}

/// Why a `token_json` blob could not be normalized.
///
/// Every variant carries a stable `code()` so a diagnostic surfaced to a user
/// or a test can assert on the condition rather than on prose.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum UsageError {
    /// The stored blob is not valid JSON.
    Malformed { detail: String },
    /// The blob parsed, but is not a JSON object.
    NotAnObject,
    /// This build has no accounting rule for the source.
    UnknownSource { source: String },
    /// A counter was present but was negative, fractional, non-finite, or
    /// larger than `u64`.
    NonIntegerCounter { field: &'static str, value: String },
    /// A cumulative counter went backwards inside one record — for Codex,
    /// `cached_input_tokens` exceeding `input_tokens`, which would make
    /// cache-exclusive input negative.
    CounterRegressed {
        field: &'static str,
        value: u64,
        subtracted: u64,
    },
    /// Two reported counters could not be combined without exceeding `u64`.
    CounterOverflow { field: &'static str },
    /// A reported cost was negative or non-finite.
    InvalidCost { value: String },
}

impl UsageError {
    /// Stable screaming-snake identifier for logs, diagnostics, and tests.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Malformed { .. } => "USAGE_MALFORMED",
            Self::NotAnObject => "USAGE_NOT_AN_OBJECT",
            Self::UnknownSource { .. } => "USAGE_UNKNOWN_SOURCE",
            Self::NonIntegerCounter { .. } => "USAGE_NON_INTEGER_COUNTER",
            Self::CounterRegressed { .. } => "USAGE_COUNTER_REGRESSED",
            Self::CounterOverflow { .. } => "USAGE_COUNTER_OVERFLOW",
            Self::InvalidCost { .. } => "USAGE_INVALID_COST",
        }
    }
}

impl fmt::Display for UsageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let code = self.code();
        match self {
            Self::Malformed { detail } => write!(formatter, "{code}: {detail}"),
            Self::NotAnObject => write!(formatter, "{code}: usage is not a JSON object"),
            Self::UnknownSource { source } => {
                write!(formatter, "{code}: no usage accounting rule for '{source}'")
            }
            Self::NonIntegerCounter { field, value } => write!(
                formatter,
                "{code}: '{field}' is not a non-negative integer ({value})"
            ),
            Self::CounterRegressed {
                field,
                value,
                subtracted,
            } => write!(
                formatter,
                "{code}: '{field}' is {value} but {subtracted} cached tokens were reported against it"
            ),
            Self::CounterOverflow { field } => write!(
                formatter,
                "{code}: '{field}' overflowed while combining reported counters"
            ),
            Self::InvalidCost { value } => {
                write!(formatter, "{code}: reported cost is not a finite non-negative number ({value})")
            }
        }
    }
}

impl std::error::Error for UsageError {}

/// Sources this build can normalize. Anything else is an explicit
/// [`UsageError::UnknownSource`] rather than a silent zero.
pub const NORMALIZABLE_SOURCES: &[&str] = &["claude", "codex"];

/// The accounting mode a source's records use, or `None` when this build has
/// no rule for it.
pub fn source_accounting(source: &str) -> Option<UsageAccounting> {
    match source {
        // Claude writes `message.usage` onto every content block of the
        // message, so the record is per-message and needs deduplication.
        "claude" => Some(UsageAccounting::PerMessage),
        // Codex reports cumulative `token_count` snapshots, which the parser
        // differences into per-request deltas before storing them.
        "codex" => Some(UsageAccounting::CumulativeDelta),
        _ => None,
    }
}

/// Normalize one stored `token_json` string.
///
/// `Ok(None)` means the blob carried no recognized counter at all — an empty
/// object, a JSON `null`, or a bag of unrelated keys such as Claude's
/// `service_tier`. That is "no usage evidence", which is different from
/// "usage evidence that says zero".
pub fn normalize_usage_str(source: &str, raw: &str) -> Result<Option<NormalizedUsage>, UsageError> {
    let value: Value = serde_json::from_str(raw).map_err(|error| UsageError::Malformed {
        detail: error.to_string(),
    })?;
    normalize_usage(source, &value)
}

/// Normalize one parsed `token_json` value. See [`normalize_usage_str`].
pub fn normalize_usage(
    source: &str,
    token_json: &Value,
) -> Result<Option<NormalizedUsage>, UsageError> {
    let accounting = source_accounting(source).ok_or_else(|| UsageError::UnknownSource {
        source: source.to_string(),
    })?;
    if token_json.is_null() {
        return Ok(None);
    }
    let Some(object) = token_json.as_object() else {
        return Err(UsageError::NotAnObject);
    };
    let mut reported = false;
    let mut counter = |key: &'static str| -> Result<Option<u64>, UsageError> {
        let value = read_counter(object.get(key), key)?;
        reported |= value.is_some();
        Ok(value)
    };

    let mut usage = NormalizedUsage::empty(accounting);
    let output = counter("output_tokens")?;
    let reasoning = counter("reasoning_output_tokens")?;
    usage.output_tokens = output.unwrap_or(0);
    usage.reasoning_tokens = reasoning;
    usage.coverage.has_output_tokens = output.is_some();
    usage.coverage.has_reasoning_tokens = reasoning.is_some();
    usage.provider_total_tokens = counter("total_tokens")?;

    match source {
        "codex" => {
            // `CodexTokenTotals::to_token_json` keeps `input_tokens`
            // inclusive of `cached_input_tokens` even after snapshot
            // differencing. Emitting exclusive input here is what stops a
            // consumer that sums the categories from counting cache reads
            // twice.
            let input = counter("input_tokens")?;
            let cached = counter("cached_input_tokens")?;
            let cache_write = counter("cache_write_input_tokens")?;
            let inclusive = input.unwrap_or(0);
            let cached_value = cached.unwrap_or(0);
            usage.input_tokens =
                inclusive
                    .checked_sub(cached_value)
                    .ok_or(UsageError::CounterRegressed {
                        field: "input_tokens",
                        value: inclusive,
                        subtracted: cached_value,
                    })?;
            usage.cache_read_tokens = cached_value;
            usage.cache_write_tokens = cache_write.unwrap_or(0);
            usage.coverage.has_input_tokens = input.is_some();
            usage.coverage.has_cache_read_tokens = cached.is_some();
            usage.coverage.has_cache_write_tokens = cache_write.is_some();
        }
        "claude" => {
            // Claude's native `message.usage` already excludes cache reads
            // and writes from `input_tokens`; subtracting them again would
            // under-report ordinary input.
            let input = counter("input_tokens")?;
            let cache_read = counter("cache_read_input_tokens")?;
            let cache_write_total = counter("cache_creation_input_tokens")?;
            usage.input_tokens = input.unwrap_or(0);
            usage.cache_read_tokens = cache_read.unwrap_or(0);
            usage.coverage.has_input_tokens = input.is_some();
            usage.coverage.has_cache_read_tokens = cache_read.is_some();

            let creation = object.get("cache_creation").and_then(Value::as_object);
            let ephemeral_5m = read_counter(
                creation.and_then(|c| c.get("ephemeral_5m_input_tokens")),
                "cache_creation.ephemeral_5m_input_tokens",
            )?;
            let ephemeral_1h = read_counter(
                creation.and_then(|c| c.get("ephemeral_1h_input_tokens")),
                "cache_creation.ephemeral_1h_input_tokens",
            )?;
            reported |= ephemeral_5m.is_some() || ephemeral_1h.is_some();
            usage.cache_write_5m_tokens = ephemeral_5m;
            usage.cache_write_1h_tokens = ephemeral_1h;
            // Prefer the provider's own total. The TTL split is preserved
            // beside it rather than replacing it, so a provider whose split
            // disagrees with its total stays inspectable instead of being
            // silently rewritten.
            usage.cache_write_tokens = match cache_write_total {
                Some(total) => total,
                None => ephemeral_5m
                    .unwrap_or(0)
                    .checked_add(ephemeral_1h.unwrap_or(0))
                    .ok_or(UsageError::CounterOverflow {
                        field: "cache_creation",
                    })?,
            };
            usage.coverage.has_cache_write_tokens =
                cache_write_total.is_some() || ephemeral_5m.is_some() || ephemeral_1h.is_some();
        }
        // `source_accounting` already rejected anything else.
        _ => unreachable!("source_accounting accepted an unhandled source"),
    }

    let cost = read_cost(object)?;
    reported |= cost.is_some();
    usage.reported_cost_usd = cost;

    Ok(reported.then_some(usage))
}

/// Read one counter: absent and `null` are "not reported"; anything that is
/// not a non-negative integer is an error, never a clamp.
fn read_counter(value: Option<&Value>, field: &'static str) -> Result<Option<u64>, UsageError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_u64()
            .map(Some)
            .ok_or_else(|| UsageError::NonIntegerCounter {
                field,
                value: value.to_string(),
            }),
    }
}

/// A cost is only ever read back, never computed. Both spellings seen in
/// provider records are accepted.
fn read_cost(object: &serde_json::Map<String, Value>) -> Result<Option<f64>, UsageError> {
    for key in ["cost_usd", "costUSD", "total_cost_usd"] {
        let Some(value) = object.get(key) else {
            continue;
        };
        if value.is_null() {
            continue;
        }
        let cost = value
            .as_f64()
            .filter(|cost| cost.is_finite() && *cost >= 0.0);
        return match cost {
            Some(cost) => Ok(Some(cost)),
            None => Err(UsageError::InvalidCost {
                value: value.to_string(),
            }),
        };
    }
    Ok(None)
}

/// `(timestamp_ms, prompt_text)` — the identity a `history` row is keyed by.
pub type PromptKey = (i64, String);

/// One assistant or user message, rebuilt from the content-block rows that
/// carry it.
struct UsageMessage {
    id: String,
    parent: Option<String>,
    ts: i64,
    role: String,
    text: String,
    usage: Option<NormalizedUsage>,
}

/// Attribute each message's usage to the prompt that caused it.
///
/// Ported unchanged in behaviour from the commercial outbox, which is the
/// only place this logic existed. Its defining property is that it *refuses*:
/// wherever the evidence cannot establish which prompt owns a response, the
/// response contributes nothing rather than being charged to a plausible
/// neighbour. A missing number is recoverable; a confidently wrong one is
/// not.
///
/// A message whose usage cannot be normalized (malformed, unknown source,
/// negative counter) is treated exactly like a message with no usage: it is
/// skipped, and the prompt keeps whatever other requests it owns.
pub fn attribute_usage_to_prompts(
    events: &[SessionEvent],
    source: &str,
) -> HashMap<PromptKey, NormalizedUsage> {
    let mut groups: HashMap<&str, Vec<&SessionEvent>> = HashMap::new();
    for event in events {
        if let Some(id) = event.message_id.as_deref().filter(|id| !id.is_empty()) {
            groups.entry(id).or_default().push(event);
        }
    }
    let messages: HashMap<String, UsageMessage> = groups
        .into_iter()
        .filter_map(|(id, rows)| {
            let first = rows[0];
            if rows.iter().any(|r| {
                r.ts_ms != first.ts_ms || r.role != first.role || r.parent_id != first.parent_id
            }) {
                return None;
            }
            // Claude copies message.usage onto every content block. Counting
            // rows would multiply one model request by its thinking/text/tool
            // block count.
            let tokens: Option<Vec<Value>> = rows
                .iter()
                .filter_map(|r| r.token_json.as_deref())
                .map(|raw| serde_json::from_str(raw).ok())
                .collect();
            let usage = tokens.and_then(|tokens| {
                let first = tokens.first()?;
                if tokens.iter().any(|value| value != first) {
                    return None;
                }
                normalize_usage(source, first)
                    .ok()
                    .flatten()
                    .filter(|usage| !usage.is_zero())
            });
            let text = rows
                .iter()
                .filter(|r| r.kind == "text")
                .filter_map(|r| r.text.as_deref())
                .map(str::trim)
                .filter(|text| !text.is_empty())
                .collect::<Vec<_>>()
                .join("\n");
            Some((
                id.to_string(),
                UsageMessage {
                    id: id.to_string(),
                    parent: first.parent_id.clone(),
                    ts: first.ts_ms,
                    role: first.role.clone(),
                    text,
                    usage,
                },
            ))
        })
        .collect();
    // Keep even unidentifiable user events as boundaries; dropping one would
    // incorrectly charge its answer to the preceding identifiable prompt.
    let mut boundaries: Vec<_> = events.iter().filter(|e| e.role == "user").collect();
    boundaries.sort_by_key(|e| e.ts_ms);
    // Parsers use zero when time is missing. Such a turn could fall anywhere,
    // so timestamp-only ownership is unsafe for the session.
    let timestamps_known = boundaries.iter().all(|e| e.ts_ms > 0);
    let mut prompt_counts = HashMap::new();
    for user in messages.values().filter(|m| m.role == "user") {
        *prompt_counts
            .entry((user.ts, user.text.clone()))
            .or_insert(0) += 1;
    }
    let mut attributed: HashMap<PromptKey, Option<NormalizedUsage>> = HashMap::new();
    for message in messages.values().filter(|m| m.role == "assistant") {
        let Some(usage) = &message.usage else {
            continue;
        };
        let owner = if message.parent.is_some() {
            parent_prompt(message, &messages)
        } else if source == "codex" && message.ts > 0 && timestamps_known {
            // Codex persists no parent IDs. Only its ordered human-turn
            // stream establishes ownership: a tie at either boundary is
            // ambiguous, and an explicit but broken parent link must never
            // fall back to time.
            let boundary = boundaries.partition_point(|u| u.ts_ms < message.ts);
            if boundaries
                .get(boundary)
                .is_some_and(|u| u.ts_ms == message.ts)
            {
                None
            } else {
                boundary.checked_sub(1).and_then(|i| {
                    let user = boundaries[i];
                    if user.ts_ms <= 0 || (i > 0 && boundaries[i - 1].ts_ms == user.ts_ms) {
                        return None;
                    }
                    messages.get(user.message_id.as_deref()?)
                })
            }
        } else {
            None
        };
        let Some(owner) = owner else { continue };
        let key = (owner.ts, owner.text.clone());
        // Identical text and timestamps cannot distinguish two user messages.
        // Assigning both to a single history row would conceal a bad join.
        if owner.text.is_empty() || prompt_counts.get(&key) != Some(&1) {
            continue;
        }
        let total = attributed
            .entry(key)
            .or_insert_with(|| Some(NormalizedUsage::empty(usage.accounting)));
        *total = total.as_ref().and_then(|total| total.checked_add(usage));
    }
    attributed
        .into_iter()
        .filter_map(|(key, usage)| usage.map(|u| (key, u)))
        .collect()
}

/// Walk `parent_id` to the user message that owns a response.
fn parent_prompt<'a>(
    message: &'a UsageMessage,
    messages: &'a HashMap<String, UsageMessage>,
) -> Option<&'a UsageMessage> {
    let mut current = message;
    let mut visited = HashSet::new();
    while visited.insert(current.id.as_str()) {
        if current.role == "user" {
            return Some(current);
        }
        current = messages.get(current.parent.as_deref()?)?;
    }
    // Broken/cyclic ancestry is missing evidence, not permission to charge
    // the nearest prompt (which could belong to another conversation branch).
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn claude(value: Value) -> Result<Option<NormalizedUsage>, UsageError> {
        normalize_usage("claude", &value)
    }

    fn codex(value: Value) -> Result<Option<NormalizedUsage>, UsageError> {
        normalize_usage("codex", &value)
    }

    #[test]
    fn claude_input_is_already_cache_exclusive() {
        let usage = claude(json!({
            "input_tokens": 3,
            "cache_creation_input_tokens": 4773,
            "cache_read_input_tokens": 11496,
            "output_tokens": 43,
        }))
        .unwrap()
        .unwrap();
        assert_eq!(usage.input_tokens, 3);
        assert_eq!(usage.cache_read_tokens, 11496);
        assert_eq!(usage.cache_write_tokens, 4773);
        assert_eq!(usage.output_tokens, 43);
        assert_eq!(usage.accounting, UsageAccounting::PerMessage);
    }

    /// The acceptance criterion the collapsed plugin `TokenUsage` could not
    /// meet: burn prices the two TTLs differently.
    #[test]
    fn claude_preserves_the_ephemeral_cache_split() {
        let usage = claude(json!({
            "input_tokens": 3,
            "cache_creation_input_tokens": 4773,
            "cache_read_input_tokens": 11496,
            "cache_creation": {
                "ephemeral_5m_input_tokens": 0,
                "ephemeral_1h_input_tokens": 4773,
            },
            "output_tokens": 43,
        }))
        .unwrap()
        .unwrap();
        assert_eq!(usage.cache_write_5m_tokens, Some(0));
        assert_eq!(usage.cache_write_1h_tokens, Some(4773));
        assert_eq!(usage.cache_write_tokens, 4773);
    }

    /// "Not split" and "split, and this side is zero" are different facts.
    #[test]
    fn an_unsplit_cache_write_stays_unsplit() {
        let usage = claude(json!({
            "input_tokens": 1,
            "cache_creation_input_tokens": 100,
        }))
        .unwrap()
        .unwrap();
        assert_eq!(usage.cache_write_tokens, 100);
        assert_eq!(usage.cache_write_5m_tokens, None);
        assert_eq!(usage.cache_write_1h_tokens, None);
    }

    #[test]
    fn a_split_without_a_total_sums_the_buckets() {
        let usage = claude(json!({
            "input_tokens": 1,
            "cache_creation": {
                "ephemeral_5m_input_tokens": 10,
                "ephemeral_1h_input_tokens": 5,
            },
        }))
        .unwrap()
        .unwrap();
        assert_eq!(usage.cache_write_tokens, 15);
        assert!(usage.coverage.has_cache_write_tokens);
    }

    #[test]
    fn missing_output_tokens_reads_as_zero_with_absent_coverage() {
        let usage = claude(json!({ "input_tokens": 10 })).unwrap().unwrap();
        assert_eq!(usage.output_tokens, 0);
        assert!(usage.coverage.has_input_tokens);
        assert!(!usage.coverage.has_output_tokens);
        assert!(!usage.coverage.is_complete());
    }

    #[test]
    fn claude_does_not_invent_reasoning_or_a_total() {
        let usage = claude(json!({ "input_tokens": 10, "output_tokens": 2 }))
            .unwrap()
            .unwrap();
        assert_eq!(usage.reasoning_tokens, None);
        assert_eq!(usage.provider_total_tokens, None);
        assert_eq!(usage.reported_cost_usd, None);
    }

    #[test]
    fn codex_input_is_made_cache_exclusive() {
        let usage = codex(json!({
            "input_tokens": 3000,
            "cached_input_tokens": 1000,
            "cache_write_input_tokens": 12,
            "output_tokens": 200,
            "reasoning_output_tokens": 50,
            "total_tokens": 3200,
        }))
        .unwrap()
        .unwrap();
        assert_eq!(usage.input_tokens, 2000);
        assert_eq!(usage.cache_read_tokens, 1000);
        assert_eq!(usage.cache_write_tokens, 12);
        assert_eq!(usage.reasoning_tokens, Some(50));
        // As reported, never recomputed: 2000 + 1000 + 200 + 50 != 3200.
        assert_eq!(usage.provider_total_tokens, Some(3200));
        assert_eq!(usage.accounting, UsageAccounting::CumulativeDelta);
    }

    #[test]
    fn a_cached_count_above_input_is_a_regression_not_a_clamp() {
        let error = codex(json!({ "input_tokens": 5, "cached_input_tokens": 9 })).unwrap_err();
        assert_eq!(error.code(), "USAGE_COUNTER_REGRESSED");
    }

    #[test]
    fn a_negative_counter_errors_rather_than_clamping() {
        let error = claude(json!({ "input_tokens": -1 })).unwrap_err();
        assert_eq!(error.code(), "USAGE_NON_INTEGER_COUNTER");
        assert!(error.to_string().contains("input_tokens"));
    }

    #[test]
    fn a_fractional_counter_errors() {
        assert_eq!(
            claude(json!({ "output_tokens": 1.5 })).unwrap_err().code(),
            "USAGE_NON_INTEGER_COUNTER"
        );
    }

    #[test]
    fn no_recognized_counter_is_no_evidence() {
        assert_eq!(claude(json!({})).unwrap(), None);
        assert_eq!(claude(json!({ "service_tier": "standard" })).unwrap(), None);
        assert_eq!(claude(Value::Null).unwrap(), None);
    }

    #[test]
    fn a_reported_zero_is_evidence() {
        let usage = claude(json!({ "input_tokens": 0, "output_tokens": 0 }))
            .unwrap()
            .unwrap();
        assert!(usage.is_zero());
        assert!(usage.coverage.has_input_tokens);
    }

    #[test]
    fn a_non_object_and_an_unknown_source_are_distinct_errors() {
        assert_eq!(
            normalize_usage("claude", &json!(7)).unwrap_err(),
            UsageError::NotAnObject
        );
        assert_eq!(
            normalize_usage("cursor", &json!({})).unwrap_err().code(),
            "USAGE_UNKNOWN_SOURCE"
        );
    }

    #[test]
    fn malformed_json_reports_its_own_code() {
        assert_eq!(
            normalize_usage_str("claude", "{not json")
                .unwrap_err()
                .code(),
            "USAGE_MALFORMED"
        );
    }

    #[test]
    fn a_negative_reported_cost_errors() {
        assert_eq!(
            claude(json!({ "input_tokens": 1, "cost_usd": -0.5 }))
                .unwrap_err()
                .code(),
            "USAGE_INVALID_COST"
        );
    }

    #[test]
    fn a_reported_cost_is_read_back_but_never_computed() {
        let usage = claude(json!({ "input_tokens": 1, "costUSD": 0.25 }))
            .unwrap()
            .unwrap();
        assert_eq!(usage.reported_cost_usd, Some(0.25));
    }

    #[test]
    fn adding_merges_coverage_and_keeps_unreported_fields_unreported() {
        let a = claude(json!({ "input_tokens": 1 })).unwrap().unwrap();
        let b = claude(json!({ "output_tokens": 2 })).unwrap().unwrap();
        let sum = a.checked_add(&b).unwrap();
        assert_eq!(sum.input_tokens, 1);
        assert_eq!(sum.output_tokens, 2);
        assert_eq!(sum.reasoning_tokens, None);
        assert!(sum.coverage.has_input_tokens && sum.coverage.has_output_tokens);
    }

    #[test]
    fn adding_overflow_refuses_rather_than_saturating() {
        let a = claude(json!({ "input_tokens": u64::MAX }))
            .unwrap()
            .unwrap();
        let b = claude(json!({ "input_tokens": 1 })).unwrap().unwrap();
        assert_eq!(a.checked_add(&b), None);
    }

    #[test]
    fn accounting_labels_round_trip() {
        for mode in [
            UsageAccounting::PerRequest,
            UsageAccounting::PerMessage,
            UsageAccounting::CumulativeDelta,
            UsageAccounting::ContextProxy,
        ] {
            assert_eq!(UsageAccounting::parse(mode.as_str()), Some(mode));
        }
        assert_eq!(UsageAccounting::parse("per-eon"), None);
    }
}
