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
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;

mod attribution;

#[cfg(test)]
pub(crate) use attribution::request_key;
pub use attribution::{attribute_usage_to_prompts, PromptKey};

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
            // The TTL buckets are counted too. A provider that reports
            // `cache_creation_input_tokens: 0` beside an `ephemeral_5m` of 10
            // has told us about 10 tokens, and reading the pair as "nothing
            // reported" dropped the record from prompt attribution entirely.
            && self.cache_write_5m_tokens.unwrap_or(0) == 0
            && self.cache_write_1h_tokens.unwrap_or(0) == 0
            && self.provider_total_tokens.unwrap_or(0) == 0
            && self.reported_cost_usd.unwrap_or(0.0) == 0.0
    }

    /// This record's contribution to one TTL bucket of the cache-write split.
    ///
    /// `None` means the split is unknown here, which makes the pair's split
    /// unknown. A record that wrote no cache is the one exception: its split
    /// is a *known* zero and must not erase a real one.
    ///
    /// The exception requires the provider to have **reported** the zero.
    /// `cache_write_tokens` is 0 for a record that never mentioned cache
    /// writes at all, and reading that as a known zero let a contributor with
    /// no cache evidence keep another request's buckets while the summary
    /// stayed unflagged — a split presented as complete on the strength of a
    /// number nobody wrote.
    fn split_bucket(&self, bucket: Option<u64>) -> Option<u64> {
        bucket
            .or((self.cache_write_tokens == 0 && self.coverage.has_cache_write_tokens).then_some(0))
    }

    /// Add two records of the same accounting mode, or `None` on overflow.
    ///
    /// Overflow returns `None` rather than saturating for the same reason
    /// normalization errors rather than clamps: a saturated total is a
    /// plausible-looking number that no longer means anything.
    ///
    /// **An optional field is reported only when every contributor reported
    /// it.** Folding an unreported `None` in as a zero is how a sum of one
    /// split cache-write record and one unsplit one ends up describing the
    /// whole session as split — the unsplit tokens vanish from the TTL
    /// buckets that are priced separately, and the result looks complete. The
    /// one exception is the cache-write split of a record that wrote no cache
    /// at all: its split is not unreported, it is known to be zero.
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
            cache_write_5m_tokens: checked_add_split(
                self.split_bucket(self.cache_write_5m_tokens),
                other.split_bucket(other.cache_write_5m_tokens),
            )?,
            cache_write_1h_tokens: checked_add_split(
                self.split_bucket(self.cache_write_1h_tokens),
                other.split_bucket(other.cache_write_1h_tokens),
            )?,
            provider_total_tokens: checked_add_optional(
                self.provider_total_tokens,
                other.provider_total_tokens,
            )?,
            // A cost only some requests reported is not the pair's cost.
            reported_cost_usd: match (self.reported_cost_usd, other.reported_cost_usd) {
                (Some(a), Some(b)) => {
                    let sum = a + b;
                    Some(sum.is_finite().then_some(sum)?)
                }
                _ => None,
            },
            accounting: self.accounting,
            coverage: self.coverage.merged(other.coverage),
        })
    }
}

/// Sum two optional counters, reporting one only when both sides did.
///
/// The outer `Option` is the overflow signal; the inner one is "was this
/// reported at all". A partial sum would be a number with no defined meaning
/// presented as a total.
fn checked_add_optional(a: Option<u64>, b: Option<u64>) -> Option<Option<u64>> {
    match (a, b) {
        (Some(a), Some(b)) => a.checked_add(b).map(Some),
        (None, None) => Some(None),
        _ => Some(None),
    }
}

/// Sum one TTL bucket of the cache-write split.
///
/// A record that *reported* writing no cache tokens has a known split — zero
/// in every bucket — so it does not make the pair's split unknown. Any other
/// unreported split does.
fn checked_add_split(a: Option<u64>, b: Option<u64>) -> Option<Option<u64>> {
    checked_add_optional(a, b)
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
pub const NORMALIZABLE_SOURCES: &[&str] = &["claude", "codex", "grok", "muse"];

/// Whether one model request can be spread across several stored records for
/// this source.
///
/// Claude writes one JSONL record per content block of a message, each with
/// its own record uuid, so grouping on the record identity alone splits one
/// API call into several. Codex attaches one differenced delta to one event,
/// so its records already are its requests. This is what decides whether a
/// request whose grouping key is only a record id is trustworthy.
pub fn source_splits_requests_across_records(source: &str) -> bool {
    matches!(source, "claude")
}

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
        // Grok's `turn_completed.usage` is one turn's spend, written once on
        // that turn's last assistant event. The context-window snapshot
        // stored beside it is never normalized.
        "grok" => Some(UsageAccounting::PerRequest),
        // Muse writes one `model_completed` usage object per model step, and
        // the parser stores it once, on the first assistant row that step
        // committed.
        "muse" => Some(UsageAccounting::PerRequest),
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
    // Muse names the reasoning counter `reasoning_tokens`; Codex spells it
    // `reasoning_output_tokens`. Either way it is a subset of output.
    let reasoning = counter(if source == "muse" {
        "reasoning_tokens"
    } else {
        "reasoning_output_tokens"
    })?;
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
        "muse" => {
            // Responses-shaped: `input_tokens` includes the cached prefix,
            // which Muse reports as `cache_read_tokens` (and again as
            // `cached_tokens`, the same count). Input is emitted exclusive of
            // it, as for Codex, so summing the categories counts each token
            // once.
            let input = counter("input_tokens")?;
            let cache_read = match counter("cache_read_tokens")? {
                Some(value) => Some(value),
                None => counter("cached_tokens")?,
            };
            let cache_write = counter("cache_write_tokens")?;
            let inclusive = input.unwrap_or(0);
            let cached_value = cache_read.unwrap_or(0);
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
            usage.coverage.has_cache_read_tokens = cache_read.is_some();
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
        "grok" => {
            // Only the verbatim `turn_completed.usage` breakdown is usage. A
            // record holding only `context_total_tokens` is a context-window
            // snapshot, which is no usage evidence at all.
            let Some(breakdown) = object.get("usage") else {
                return Ok(None);
            };
            let Some(breakdown) = breakdown.as_object() else {
                return Err(UsageError::NotAnObject);
            };
            let mut grok_counter =
                |keys: &[&str], field: &'static str| -> Result<Option<u64>, UsageError> {
                    let value = keys
                        .iter()
                        .find_map(|key| breakdown.get(*key).filter(|value| !value.is_null()));
                    let value = read_counter(value, field)?;
                    reported |= value.is_some();
                    Ok(value)
                };
            use crate::ingest::grok::{
                GROK_USAGE_CACHE_READ_KEYS, GROK_USAGE_CACHE_WRITE_KEYS, GROK_USAGE_INPUT_KEYS,
                GROK_USAGE_OUTPUT_KEYS, GROK_USAGE_REASONING_KEYS,
            };
            let input = grok_counter(GROK_USAGE_INPUT_KEYS, "usage.inputTokens")?;
            let output = grok_counter(GROK_USAGE_OUTPUT_KEYS, "usage.outputTokens")?;
            let cache_read = grok_counter(GROK_USAGE_CACHE_READ_KEYS, "usage.cachedReadTokens")?;
            let cache_write = grok_counter(GROK_USAGE_CACHE_WRITE_KEYS, "usage.cachedWriteTokens")?;
            let reasoning = grok_counter(GROK_USAGE_REASONING_KEYS, "usage.reasoningTokens")?;
            let total = read_counter(
                ["totalTokens", "total_tokens"]
                    .iter()
                    .find_map(|key| breakdown.get(*key).filter(|value| !value.is_null())),
                "usage.totalTokens",
            )?;
            // Grok's `inputTokens` includes its `cachedReadTokens`, as
            // Codex's does, so the cache reads come out of input here. Its
            // `outputTokens` includes `reasoningTokens`; that stays as
            // written, with reasoning reported beside it, the same shape a
            // Codex record has.
            // Only a reported input can be checked against the cache reads:
            // an absent one is unknown, not zero, so a breakdown that names
            // cache reads without it keeps them and reports no input.
            let cached = cache_read.unwrap_or(0);
            usage.input_tokens = match input {
                Some(inclusive) => {
                    inclusive
                        .checked_sub(cached)
                        .ok_or(UsageError::CounterRegressed {
                            field: "usage.inputTokens",
                            value: inclusive,
                            subtracted: cached,
                        })?
                }
                None => 0,
            };
            usage.output_tokens = output.unwrap_or(0);
            usage.reasoning_tokens = reasoning;
            usage.cache_read_tokens = cached;
            usage.cache_write_tokens = cache_write.unwrap_or(0);
            usage.provider_total_tokens = total;
            usage.coverage.has_input_tokens = input.is_some();
            usage.coverage.has_output_tokens = output.is_some();
            usage.coverage.has_reasoning_tokens = reasoning.is_some();
            usage.coverage.has_cache_read_tokens = cache_read.is_some();
            usage.coverage.has_cache_write_tokens = cache_write.is_some();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::SessionEvent;
    use serde_json::json;

    fn claude(value: Value) -> Result<Option<NormalizedUsage>, UsageError> {
        normalize_usage("claude", &value)
    }

    fn codex(value: Value) -> Result<Option<NormalizedUsage>, UsageError> {
        normalize_usage("codex", &value)
    }

    fn grok(value: Value) -> Result<Option<NormalizedUsage>, UsageError> {
        normalize_usage("grok", &value)
    }

    #[test]
    fn grok_turn_usage_is_per_request_with_cache_exclusive_input() {
        let usage = grok(json!({
            "context_total_tokens": 18432,
            "source": "updates.jsonl",
            "usage": {
                "inputTokens": 1000,
                "outputTokens": 100,
                "reasoningTokens": 20,
                "cachedReadTokens": 400,
                "totalTokens": 1100,
                "modelUsage": {"grok-4.5-build": {"inputTokens": 1000}}
            }
        }))
        .unwrap()
        .unwrap();
        assert_eq!(usage.accounting, UsageAccounting::PerRequest);
        assert_eq!(usage.input_tokens, 600);
        assert_eq!(usage.cache_read_tokens, 400);
        assert_eq!(usage.output_tokens, 100);
        assert_eq!(usage.reasoning_tokens, Some(20));
        // The provider's own total, never the context snapshot beside it.
        assert_eq!(usage.provider_total_tokens, Some(1100));
        assert!(usage.coverage.has_input_tokens && usage.coverage.has_cache_read_tokens);
        assert!(!usage.coverage.has_cache_write_tokens);
    }

    #[test]
    fn a_grok_context_snapshot_alone_is_no_usage_evidence() {
        assert_eq!(
            grok(json!({"context_total_tokens": 9210, "source": "updates.jsonl"})).unwrap(),
            None
        );
    }

    #[test]
    fn grok_counters_are_read_in_every_spelling_and_never_clamped() {
        let usage = grok(json!({"usage": {"promptTokens": 50, "completionTokens": 5}}))
            .unwrap()
            .unwrap();
        assert_eq!((usage.input_tokens, usage.output_tokens), (50, 5));
        assert_eq!(usage.reasoning_tokens, None);
        assert_eq!(
            grok(json!({"usage": {"inputTokens": 10, "cachedReadTokens": 11}}))
                .unwrap_err()
                .code(),
            "USAGE_COUNTER_REGRESSED"
        );
        // No reported input is unknown input, not zero: the cache reads and
        // output it did report are kept, and input coverage stays false.
        let partial = grok(json!({"usage": {"cachedReadTokens": 400, "outputTokens": 10}}))
            .unwrap()
            .unwrap();
        assert_eq!(
            (
                partial.input_tokens,
                partial.cache_read_tokens,
                partial.output_tokens
            ),
            (0, 400, 10)
        );
        assert!(!partial.coverage.has_input_tokens);
        assert!(partial.coverage.has_cache_read_tokens);
        assert_eq!(
            grok(json!({"usage": {"outputTokens": -1}}))
                .unwrap_err()
                .code(),
            "USAGE_NON_INTEGER_COUNTER"
        );
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

    /// Muse's `model_completed` usage, as the real CLI writes it: input
    /// inclusive of the cached prefix, reported under two keys.
    #[test]
    fn muse_input_is_made_cache_exclusive_and_counted_once() {
        let usage = normalize_usage(
            "muse",
            &json!({
                "input_tokens": 26964,
                "output_tokens": 379,
                "cached_tokens": 5105,
                "cache_read_tokens": 5105,
                "cache_write_tokens": 0,
                "reasoning_tokens": 278,
            }),
        )
        .unwrap()
        .unwrap();
        assert_eq!(usage.input_tokens, 21859);
        assert_eq!(usage.cache_read_tokens, 5105);
        assert_eq!(usage.cache_write_tokens, 0);
        assert_eq!(usage.output_tokens, 379);
        assert_eq!(usage.reasoning_tokens, Some(278));
        assert_eq!(usage.accounting, UsageAccounting::PerRequest);

        // An older record that spells the cache read only as `cached_tokens`.
        let legacy = normalize_usage(
            "muse",
            &json!({"input_tokens": 10, "output_tokens": 1, "cached_tokens": 4}),
        )
        .unwrap()
        .unwrap();
        assert_eq!(legacy.input_tokens, 6);
        assert_eq!(legacy.cache_read_tokens, 4);
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

    /// The fold that made an unsplit record vanish into a split total.
    #[test]
    fn an_unsplit_cache_write_makes_the_pair_split_unknown() {
        let split = claude(json!({
            "cache_creation_input_tokens": 100,
            "cache_creation": { "ephemeral_5m_input_tokens": 100, "ephemeral_1h_input_tokens": 0 },
        }))
        .unwrap()
        .unwrap();
        let unsplit = claude(json!({ "cache_creation_input_tokens": 50 }))
            .unwrap()
            .unwrap();
        let sum = split.checked_add(&unsplit).unwrap();
        // The total still carries every token...
        assert_eq!(sum.cache_write_tokens, 150);
        // ...but claiming 100 in the 5m bucket would silently drop the other
        // 50 from the bucket that is priced.
        assert_eq!(sum.cache_write_5m_tokens, None);
        assert_eq!(sum.cache_write_1h_tokens, None);
    }

    /// A record that wrote no cache at all has a known split, not an absent
    /// one, so it must not erase a real one.
    #[test]
    fn a_record_with_no_cache_writes_keeps_the_pair_split_known() {
        let split = claude(json!({
            "cache_creation_input_tokens": 100,
            "cache_creation": { "ephemeral_5m_input_tokens": 60, "ephemeral_1h_input_tokens": 40 },
        }))
        .unwrap()
        .unwrap();
        let none_written = claude(json!({ "input_tokens": 7, "cache_creation_input_tokens": 0 }))
            .unwrap()
            .unwrap();
        let sum = split.checked_add(&none_written).unwrap();
        assert_eq!(sum.cache_write_5m_tokens, Some(60));
        assert_eq!(sum.cache_write_1h_tokens, Some(40));
        assert_eq!(sum.cache_write_tokens, 100);
    }

    #[test]
    fn a_cost_only_one_side_reported_is_not_the_pair_cost() {
        let priced = claude(json!({ "input_tokens": 1, "cost_usd": 0.25 }))
            .unwrap()
            .unwrap();
        let unpriced = claude(json!({ "input_tokens": 1 })).unwrap().unwrap();
        assert_eq!(
            priced.checked_add(&unpriced).unwrap().reported_cost_usd,
            None
        );
        assert_eq!(
            priced.checked_add(&priced).unwrap().reported_cost_usd,
            Some(0.5)
        );
    }

    #[test]
    fn a_provider_total_only_one_side_reported_is_not_the_pair_total() {
        let with_total = codex(json!({ "input_tokens": 5, "total_tokens": 5 }))
            .unwrap()
            .unwrap();
        let without = codex(json!({ "input_tokens": 5 })).unwrap().unwrap();
        assert_eq!(
            with_total
                .checked_add(&without)
                .unwrap()
                .provider_total_tokens,
            None
        );
    }

    #[test]
    fn only_claude_spreads_one_request_over_several_records() {
        assert!(source_splits_requests_across_records("claude"));
        assert!(!source_splits_requests_across_records("codex"));
        assert!(!source_splits_requests_across_records("cursor"));
    }

    #[test]
    fn adding_overflow_refuses_rather_than_saturating() {
        let a = claude(json!({ "input_tokens": u64::MAX }))
            .unwrap()
            .unwrap();
        let b = claude(json!({ "input_tokens": 1 })).unwrap().unwrap();
        assert_eq!(a.checked_add(&b), None);
    }

    /// Attribution folds several responses into one prompt. Seeding that fold
    /// with an all-`None` identity record would meet the "every contributor
    /// reported it" rule vacuously and strip every optional field — reasoning
    /// counts, the cache-write split, the provider total, the cost — from
    /// every prompt, while still returning a well-formed usage record.
    #[test]
    fn attribution_keeps_optional_fields_when_folding_several_responses() {
        let events = vec![
            user_event("u1", 100, "ask"),
            assistant_event(
                "a1",
                Some("u1"),
                150,
                r#"{"input_tokens":1000,"cached_input_tokens":400,"output_tokens":120,"reasoning_output_tokens":10,"total_tokens":1120}"#,
            ),
            assistant_event(
                "a2",
                Some("u1"),
                200,
                r#"{"input_tokens":600,"cached_input_tokens":500,"output_tokens":60,"reasoning_output_tokens":30,"total_tokens":660}"#,
            ),
        ];
        let attributed = attribute_usage_to_prompts(&events, "codex");
        let usage = attributed
            .get(&(100, "ask".to_string()))
            .expect("both responses belong to the one prompt");
        assert_eq!(usage.output_tokens, 180);
        assert_eq!(
            usage.reasoning_tokens,
            Some(40),
            "both responses reported reasoning, so the sum is reported"
        );
        assert_eq!(usage.provider_total_tokens, Some(1780));
    }

    fn user_event(message_id: &str, ts_ms: i64, text: &str) -> SessionEvent {
        session_event(message_id, None, ts_ms, "user", text, None)
    }

    fn assistant_event(
        message_id: &str,
        parent_id: Option<&str>,
        ts_ms: i64,
        token_json: &str,
    ) -> SessionEvent {
        session_event(
            message_id,
            parent_id,
            ts_ms,
            "assistant",
            "answer",
            Some(token_json),
        )
    }

    /// A TTL bucket is a measurement even when the total beside it reads
    /// zero. Leaving the buckets out of `is_zero` made such a record "no
    /// evidence", so prompt attribution dropped it and the tokens vanished.
    #[test]
    fn a_ttl_bucket_alone_is_not_an_empty_record() {
        let usage = normalize_usage_str(
            "claude",
            r#"{"cache_creation_input_tokens":0,
                "cache_creation":{"ephemeral_5m_input_tokens":10,"ephemeral_1h_input_tokens":0}}"#,
        )
        .unwrap()
        .expect("a reported bucket is usage");
        assert_eq!(usage.cache_write_5m_tokens, Some(10));
        assert!(!usage.is_zero(), "10 tokens were reported: {usage:?}");
    }

    /// A contributor that never mentioned cache writes has no split to
    /// contribute. Reading its defaulted `cache_write_tokens == 0` as a
    /// *reported* zero let it pass another request's buckets through as
    /// though the pair's split were complete.
    #[test]
    fn a_contributor_with_no_cache_evidence_does_not_certify_a_split() {
        let split = normalize_usage_str(
            "claude",
            r#"{"cache_creation_input_tokens":8,
                "cache_creation":{"ephemeral_5m_input_tokens":3,"ephemeral_1h_input_tokens":5}}"#,
        )
        .unwrap()
        .unwrap();
        let silent = normalize_usage_str("claude", r#"{"input_tokens":5,"output_tokens":7}"#)
            .unwrap()
            .unwrap();
        assert!(
            !silent.coverage.has_cache_write_tokens,
            "the fixture really says nothing about cache writes"
        );

        let total = split.checked_add(&silent).unwrap();
        assert_eq!(total.cache_write_tokens, 8);
        assert_eq!(
            (total.cache_write_5m_tokens, total.cache_write_1h_tokens),
            (None, None),
            "the split is unknown for the pair, not inherited from one side"
        );

        // And a contributor that *reported* writing nothing still has a known
        // zero split, which must not be turned into an unknown one.
        let reported_zero = normalize_usage_str(
            "claude",
            r#"{"cache_creation_input_tokens":0,"input_tokens":5}"#,
        )
        .unwrap()
        .unwrap();
        assert!(reported_zero.coverage.has_cache_write_tokens);
        let total = split.checked_add(&reported_zero).unwrap();
        assert_eq!(
            (total.cache_write_5m_tokens, total.cache_write_1h_tokens),
            (Some(3), Some(5)),
            "a reported zero keeps the split known"
        );
    }

    fn session_event(
        message_id: &str,
        parent_id: Option<&str>,
        ts_ms: i64,
        role: &str,
        text: &str,
        token_json: Option<&str>,
    ) -> SessionEvent {
        SessionEvent {
            id: 0,
            source: "codex".into(),
            session_id: "s1".into(),
            project: None,
            project_key: None,
            cwd: None,
            git_branch: None,
            message_id: Some(message_id.to_string()),
            parent_id: parent_id.map(str::to_string),
            ts_ms,
            role: role.into(),
            kind: "text".into(),
            text: Some(text.to_string()),
            model: None,
            token_json: token_json.map(str::to_string),
            provider: None,
            event_uid: format!("{message_id}:0"),
            tool_use_id: None,
            payload_bytes: None,
            payload_truncated: None,
            payload_hash: None,
            call_index: None,
            event_index: None,
            result_status: None,
            event_source: None,
            error_signal: None,
            subagent_session_id: None,
            agent_id: None,
            request_id: None,
            provider_message_id: None,
            stop_reason: None,
            agent_version: None,
            is_sidechain: None,
            is_meta: None,
            turn_id: None,
            request_span: None,
            control_kind: None,
            raw_kind: None,
            record_token_json: None,
        }
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
