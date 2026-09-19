//! Per-request usage records and the session rollup built from them.
//!
//! `session_requests` is a **view**, not a materialized table. Grouping is
//! structural — one row per `(source, session_id, request_key)` — so it can
//! never disagree with the `session_events` it is derived from, and there is
//! no second copy of the normalization rules to keep in step with
//! [`crate::usage`]. The cost is one grouped scan of a single session per
//! query; the benefit is that an entire class of stale-cache bug does not
//! exist.
//!
//! `request_key` is the upstream request id when the store has one and the
//! message id otherwise. Today it is always the message id: capturing
//! Claude's `requestId` is the raw-facts issue's job, and
//! [`RequestKeySource`] is on the wire already so the day it lands a consumer
//! can tell a real request key from a fallback without a contract break.
use crate::usage::{normalize_usage_str, NormalizedUsage, UsageAccounting};
use anyhow::Result;
use rusqlite::{Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Bump whenever the request row / summary shapes, ordering, or cursor
/// semantics require an SDK change.
pub const SESSION_USAGE_CONTRACT_VERSION: u32 = 1;

/// The `session_requests` view: one row per model request.
///
/// Only assistant rows with a message id participate. A user turn is not a
/// request, and a row with no message id cannot be placed in one without
/// guessing.
///
/// The grouping key is the provider's own request identity where it recorded
/// one — `request_id` first, then `provider_message_id` — and the row's
/// `message_id` only as a last resort. That last resort is not a request
/// identity: for Claude it is the JSONL record's `uuid`, and one API call is
/// written as several records with different uuids, so a key built from it
/// splits one request into one row per content block. `request_key_source`
/// says which of the three was used, and a request keyed on the record id
/// from a source that spreads requests across records is reported with the
/// `unresolved-request-identity` diagnostic rather than quietly summed.
///
/// `usage_variants` counts the *distinct* non-null `token_json` blobs in a
/// group. Claude copies the same blob onto every record of one request, so
/// the expected value is 1; anything higher means the copies disagree and the
/// request's usage is not established. `MIN(e.id)` is a stable unique
/// tiebreak for the keyset order without a synthetic row id.
pub(crate) const SESSION_REQUESTS_VIEW_DDL: &str = "CREATE VIEW session_requests AS
SELECT
    e.source AS source,
    e.session_id AS session_id,
    COALESCE(NULLIF(e.request_id, ''), NULLIF(e.provider_message_id, ''), e.message_id) AS request_key,
    CASE
        WHEN NULLIF(e.request_id, '') IS NOT NULL THEN 'request-id'
        WHEN NULLIF(e.provider_message_id, '') IS NOT NULL THEN 'provider-message-id'
        ELSE 'record-id'
    END AS request_key_source,
    group_concat(DISTINCT e.message_id) AS message_ids,
    NULL AS provider,
    MIN(e.id) AS id,
    MIN(e.ts_ms) AS first_ts_ms,
    MAX(e.ts_ms) AS last_ts_ms,
    MIN(e.model) AS model,
    COUNT(DISTINCT e.model) AS model_variants,
    MIN(e.token_json) AS token_json,
    COUNT(DISTINCT e.token_json) AS usage_variants,
    MAX(e.kind = 'thinking') AS has_thinking,
    COUNT(*) AS event_count
FROM session_events e
WHERE e.role = 'assistant'
  AND e.message_id IS NOT NULL
  AND e.message_id <> ''
GROUP BY e.source, e.session_id, COALESCE(NULLIF(e.request_id, ''), NULLIF(e.provider_message_id, ''), e.message_id)";

/// Whether the stored view is the one this build would create.
///
/// A view left over from a build whose grouping key was different would keep
/// splitting one Claude turn into a request per content block — silently,
/// reporting a well-formed total several times too large. So a stale view is
/// outstanding migration work, not a cosmetic difference.
pub(crate) fn session_requests_view_is_current(conn: &Connection) -> Result<bool> {
    let stored: Option<String> = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type = 'view' AND name = 'session_requests'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    Ok(stored.as_deref() == Some(SESSION_REQUESTS_VIEW_DDL))
}

/// Create the view, replacing one an earlier build left behind.
pub(crate) fn ensure_session_requests_view(conn: &Connection) -> Result<()> {
    conn.execute_batch("DROP VIEW IF EXISTS session_requests")?;
    conn.execute_batch(SESSION_REQUESTS_VIEW_DDL)?;
    Ok(())
}

/// Where a [`SessionRequest::request_key`] came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum RequestKeySource {
    /// The provider's own request id — Claude's `requestId`.
    RequestId,
    /// The provider's own message id — Claude's `message.id` — because the
    /// record carried no request id.
    ProviderMessageId,
    /// The stored `message_id`, because the provider recorded neither. For a
    /// source that writes one request as several records this is a *record*
    /// identity, not a request one, and the grouping built on it may be
    /// finer than one row per request.
    RecordId,
}

impl RequestKeySource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RequestId => "request-id",
            Self::ProviderMessageId => "provider-message-id",
            Self::RecordId => "record-id",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "request-id" => Some(Self::RequestId),
            "provider-message-id" => Some(Self::ProviderMessageId),
            "record-id" => Some(Self::RecordId),
            _ => None,
        }
    }

    /// Whether this key identifies an API request rather than a stored record.
    pub fn is_request_identity(self) -> bool {
        matches!(self, Self::RequestId | Self::ProviderMessageId)
    }
}

/// Why a request that has stored usage still reports none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum UsageDiagnostic {
    /// The per-block copies of one message's usage disagree, so which one
    /// describes the request is not established.
    AmbiguousUsageCopies,
    /// The stored blob could not be normalized. The reason is on
    /// [`SessionRequest::usage_error`].
    UnnormalizableUsage,
    /// The rows of one request name more than one model.
    AmbiguousModel,
    /// The provider recorded no request identity for a source that writes one
    /// request as several records, so these rows may be per record rather
    /// than per request. A session carrying this does not get summed totals.
    ///
    /// On a store written before request identities were captured this is
    /// every Claude request until its transcript is re-parsed.
    UnresolvedRequestIdentity,
    /// Some contributing requests reported the cache-write TTL split and
    /// others did not, so the session's 5m/1h buckets are not established
    /// even though the cache-write total is.
    PartialCacheWriteSplit,
    /// Only some contributing requests carried a cost, so the session's cost
    /// is not established.
    PartialReportedCost,
}

impl UsageDiagnostic {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AmbiguousUsageCopies => "ambiguous-usage-copies",
            Self::UnnormalizableUsage => "unnormalizable-usage",
            Self::AmbiguousModel => "ambiguous-model",
            Self::UnresolvedRequestIdentity => "unresolved-request-identity",
            Self::PartialCacheWriteSplit => "partial-cache-write-split",
            Self::PartialReportedCost => "partial-reported-cost",
        }
    }
}

/// One model request, with its usage normalized.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct SessionRequest {
    pub id: i64,
    pub source: String,
    pub session_id: String,
    pub request_key: String,
    pub request_key_source: RequestKeySource,
    /// Every `session_events.message_id` this request collapsed. More than
    /// one means the provider split the request across records — Claude's
    /// per-content-block layout.
    pub message_ids: Vec<String>,
    pub model: Option<String>,
    /// The provider behind the model, when the source records one. No local
    /// source does today, so this is always `None`; it is never inferred from
    /// a model string (burn owns that inference).
    pub provider: Option<String>,
    pub first_ts_ms: i64,
    pub last_ts_ms: i64,
    /// `None` when the request carried no usage evidence, or when the
    /// evidence could not be trusted — see `diagnostics`.
    pub usage: Option<NormalizedUsage>,
    /// The stable [`crate::usage::UsageError::code`] when normalization
    /// failed.
    pub usage_error: Option<String>,
    pub tool_use_ids: Vec<String>,
    pub has_thinking: bool,
    /// How many `session_events` rows this one request collapsed.
    pub event_count: i64,
    pub diagnostics: Vec<UsageDiagnostic>,
}

/// Stable continuation for [`session_requests_page`].
///
/// Requests inside one session routinely share a timestamp, so `id` — the
/// lowest `session_events.id` in the group — is part of the cursor. The query
/// order is `(first_ts_ms ASC, id ASC)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRequestCursor {
    pub ts_ms: i64,
    pub id: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionRequestPage {
    pub requests: Vec<SessionRequest>,
    pub next_cursor: Option<SessionRequestCursor>,
}

/// Provider-neutral rollup of one session's usage.
///
/// `output_tokens` is `0` when no request reported an output count; the
/// accompanying [`UsageCoverage`] is what distinguishes that from a session
/// that really produced nothing. The summary itself is `None` only when the
/// session has no usage evidence at all.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct SessionUsageSummary {
    pub source: String,
    pub session_id: String,
    /// The session's totals, or `None` when they are not established: no
    /// request carried usage, every request's usage was rejected, the totals
    /// overflowed, or the requests are not known to be one per API call.
    ///
    /// Absent totals and absent requests are different answers, and
    /// `diagnostics` is what distinguishes them — which is why a session
    /// whose every usage blob was rejected still returns a summary rather
    /// than nothing at all.
    pub usage: Option<NormalizedUsage>,
    /// Requests that contributed usage to `usage`.
    pub request_count: u64,
    /// Requests seen for the session, including ones with no usage.
    pub total_request_count: u64,
    /// Every accounting mode present, sorted. More than one means the totals
    /// mix units and should be read per mode.
    pub accounting: Vec<UsageAccounting>,
    pub models: Vec<String>,
    pub first_ts_ms: i64,
    pub last_ts_ms: i64,
    /// Everything that kept a request out of the totals, or that makes them
    /// narrower than they look.
    pub diagnostics: Vec<UsageDiagnostic>,
    /// True when the totals overflowed `u64`. `usage` is then `None` rather
    /// than a saturated number that no longer means anything.
    pub overflowed: bool,
}

const REQUEST_COLUMNS: &str = "id, source, session_id, request_key, request_key_source, \
     message_ids, provider, model, model_variants, first_ts_ms, last_ts_ms, \
     token_json, usage_variants, has_thinking, event_count";

/// A request row as the view returns it, before normalization.
struct RawRequest {
    id: i64,
    source: String,
    session_id: String,
    request_key: String,
    request_key_source: String,
    message_ids: Vec<String>,
    provider: Option<String>,
    model: Option<String>,
    model_variants: i64,
    first_ts_ms: i64,
    last_ts_ms: i64,
    token_json: Option<String>,
    usage_variants: i64,
    has_thinking: bool,
    event_count: i64,
}

fn row_to_raw_request(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawRequest> {
    Ok(RawRequest {
        id: row.get(0)?,
        source: row.get(1)?,
        session_id: row.get(2)?,
        request_key: row.get(3)?,
        request_key_source: row.get(4)?,
        // `group_concat` joins on a comma. Provider message ids never
        // contain one, and an empty element is dropped rather than becoming
        // an id nothing matches.
        message_ids: row
            .get::<_, Option<String>>(5)?
            .map(|joined| {
                joined
                    .split(',')
                    .filter(|id| !id.is_empty())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default(),
        provider: row.get(6)?,
        model: row.get(7)?,
        model_variants: row.get(8)?,
        first_ts_ms: row.get(9)?,
        last_ts_ms: row.get(10)?,
        token_json: row.get(11)?,
        usage_variants: row.get(12)?,
        has_thinking: row.get::<_, Option<i64>>(13)?.unwrap_or(0) != 0,
        event_count: row.get(14)?,
    })
}

/// Normalized usage for one raw row, plus why it is absent when it is.
struct ResolvedUsage {
    usage: Option<NormalizedUsage>,
    error: Option<String>,
    diagnostics: Vec<UsageDiagnostic>,
}

fn resolve_usage(raw: &RawRequest) -> ResolvedUsage {
    let mut diagnostics = Vec::new();
    // A key that is only a record id does not identify an API call for a
    // source that writes one call as several records. The row's own usage is
    // still reported — it is what the record says — but the session rollup
    // refuses to add such rows together.
    let keyed_on_a_record = RequestKeySource::parse(&raw.request_key_source)
        .is_none_or(|key| !key.is_request_identity());
    if keyed_on_a_record && crate::usage::source_splits_requests_across_records(&raw.source) {
        diagnostics.push(UsageDiagnostic::UnresolvedRequestIdentity);
    }
    if raw.model_variants > 1 {
        diagnostics.push(UsageDiagnostic::AmbiguousModel);
    }
    // Disagreeing copies of one message's usage cannot establish what the
    // request cost. Picking one would be a guess presented as a fact.
    if raw.usage_variants > 1 {
        diagnostics.push(UsageDiagnostic::AmbiguousUsageCopies);
        return ResolvedUsage {
            usage: None,
            error: None,
            diagnostics,
        };
    }
    let Some(raw_json) = raw.token_json.as_deref() else {
        return ResolvedUsage {
            usage: None,
            error: None,
            diagnostics,
        };
    };
    match normalize_usage_str(&raw.source, raw_json) {
        Ok(usage) => ResolvedUsage {
            usage,
            error: None,
            diagnostics,
        },
        Err(error) => {
            diagnostics.push(UsageDiagnostic::UnnormalizableUsage);
            ResolvedUsage {
                usage: None,
                error: Some(error.code().to_string()),
                diagnostics,
            }
        }
    }
}

/// One bounded page of a session's model requests, oldest first.
///
/// Both `source` and `session_id` are required: provider session ids collide
/// across providers, and an id-only page would interleave two sessions.
pub fn session_requests_page(
    conn: &Connection,
    source: &str,
    session_id: &str,
    limit: i64,
    after: Option<&SessionRequestCursor>,
) -> Result<SessionRequestPage> {
    let limit = limit.clamp(1, 1_000);
    let mut sql = format!(
        "SELECT {REQUEST_COLUMNS} FROM session_requests \
         WHERE source = ?1 AND session_id = ?2"
    );
    // Numbered rather than positional so the cursor comparison can name
    // `first_ts_ms` twice without binding it twice.
    if after.is_some() {
        sql.push_str(" AND (first_ts_ms > ?3 OR (first_ts_ms = ?3 AND id > ?4))");
        sql.push_str(" ORDER BY first_ts_ms ASC, id ASC LIMIT ?5");
    } else {
        sql.push_str(" ORDER BY first_ts_ms ASC, id ASC LIMIT ?3");
    }
    // One extra row answers "is there another page?" without a second query.
    let probe = limit + 1;
    let mut stmt = conn.prepare(&sql)?;
    let raw: Vec<RawRequest> = match after {
        Some(cursor) => stmt
            .query_map(
                rusqlite::params![source, session_id, cursor.ts_ms, cursor.id, probe],
                row_to_raw_request,
            )?
            .collect::<rusqlite::Result<_>>()?,
        None => stmt
            .query_map(
                rusqlite::params![source, session_id, probe],
                row_to_raw_request,
            )?
            .collect::<rusqlite::Result<_>>()?,
    };
    drop(stmt);
    let mut raw = raw;
    let has_more = raw.len() > limit as usize;
    if has_more {
        raw.truncate(limit as usize);
    }
    let tool_use_ids = tool_use_ids_for(conn, source, session_id, &raw)?;
    let next_cursor = has_more.then(|| {
        let last = raw.last().expect("non-empty page");
        SessionRequestCursor {
            ts_ms: last.first_ts_ms,
            id: last.id,
        }
    });
    let requests = raw
        .into_iter()
        .map(|raw| {
            let resolved = resolve_usage(&raw);
            let mut ids = Vec::new();
            for message_id in &raw.message_ids {
                if let Some((_, found)) = tool_use_ids.iter().find(|(key, _)| key == message_id) {
                    ids.extend(found.iter().cloned());
                }
            }
            SessionRequest {
                id: raw.id,
                request_key_source: RequestKeySource::parse(&raw.request_key_source)
                    .unwrap_or(RequestKeySource::RecordId),
                source: raw.source,
                session_id: raw.session_id,
                request_key: raw.request_key,
                message_ids: raw.message_ids,
                model: raw.model,
                provider: raw.provider,
                first_ts_ms: raw.first_ts_ms,
                last_ts_ms: raw.last_ts_ms,
                usage: resolved.usage,
                usage_error: resolved.error,
                tool_use_ids: ids,
                has_thinking: raw.has_thinking,
                event_count: raw.event_count,
                diagnostics: resolved.diagnostics,
            }
        })
        .collect();
    Ok(SessionRequestPage {
        requests,
        next_cursor,
    })
}

/// Tool use ids for the message ids on one page, in one query.
///
/// Kept out of the view deliberately: a correlated subquery per group would
/// rescan the session's tool calls once per request, and adding an index to
/// `tool_calls` to avoid that would cost a b-tree on every write for a field
/// only this page reads.
fn tool_use_ids_for(
    conn: &Connection,
    source: &str,
    session_id: &str,
    page: &[RawRequest],
) -> Result<Vec<(String, Vec<String>)>> {
    let wanted: BTreeSet<&str> = page
        .iter()
        .flat_map(|raw| raw.message_ids.iter().map(String::as_str))
        .collect();
    if wanted.is_empty() {
        return Ok(Vec::new());
    }
    let mut stmt = conn.prepare(
        "SELECT message_id, tool_use_id FROM tool_calls \
         WHERE source = ?1 AND session_id = ?2 AND message_id IS NOT NULL \
         ORDER BY message_id, ts_ms, id",
    )?;
    let mut out: Vec<(String, Vec<String>)> = Vec::new();
    let rows = stmt.query_map(rusqlite::params![source, session_id], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    for row in rows {
        let (message_id, tool_use_id) = row?;
        if !wanted.contains(message_id.as_str()) {
            continue;
        }
        match out.last_mut() {
            Some((key, ids)) if *key == message_id => ids.push(tool_use_id),
            _ => out.push((message_id, vec![tool_use_id])),
        }
    }
    Ok(out)
}

/// Provider-neutral usage rollup for one session, or `None` when the session
/// has no usage evidence at all.
///
/// Every request contributes exactly once — the view's grouping is what makes
/// that true for Claude, whose per-block usage copies would otherwise be
/// counted once per content block. Memory is bounded: the statement is
/// streamed and folded into a fixed-size accumulator, so a session with a
/// hundred thousand events costs the same as one with ten.
pub fn session_usage_summary(
    conn: &Connection,
    source: &str,
    session_id: &str,
) -> Result<Option<SessionUsageSummary>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {REQUEST_COLUMNS} FROM session_requests \
         WHERE source = ?1 AND session_id = ?2 \
         ORDER BY first_ts_ms ASC, id ASC"
    ))?;
    let mut rows = stmt.query(rusqlite::params![source, session_id])?;

    let mut total: Option<NormalizedUsage> = None;
    let mut overflowed = false;
    let mut request_count: u64 = 0;
    let mut total_request_count: u64 = 0;
    let mut accounting: BTreeSet<UsageAccounting> = BTreeSet::new();
    let mut diagnostics: BTreeSet<UsageDiagnostic> = BTreeSet::new();
    let mut models: BTreeSet<String> = BTreeSet::new();
    let mut first_ts_ms = i64::MAX;
    let mut last_ts_ms = i64::MIN;
    // Whether any contributor reported a split / a cost at all. Comparing
    // these against the folded total is what tells "nobody reported it" from
    // "some did and the fold could not combine them".
    let mut saw_cache_write_split = false;
    let mut saw_reported_cost = false;

    while let Some(row) = rows.next()? {
        let raw = row_to_raw_request(row)?;
        total_request_count = total_request_count.saturating_add(1);
        first_ts_ms = first_ts_ms.min(raw.first_ts_ms);
        last_ts_ms = last_ts_ms.max(raw.last_ts_ms);
        if let Some(model) = raw.model.as_deref().filter(|model| !model.is_empty()) {
            models.insert(model.to_string());
        }
        let resolved = resolve_usage(&raw);
        diagnostics.extend(resolved.diagnostics.iter().copied());
        let Some(usage) = resolved.usage else {
            continue;
        };
        saw_cache_write_split |=
            usage.cache_write_5m_tokens.is_some() || usage.cache_write_1h_tokens.is_some();
        saw_reported_cost |= usage.reported_cost_usd.is_some();
        request_count = request_count.saturating_add(1);
        accounting.insert(usage.accounting);
        total = match total {
            None => Some(usage),
            Some(running) => match running.checked_add(&usage) {
                Some(sum) => Some(sum),
                None => {
                    overflowed = true;
                    Some(running)
                }
            },
        };
    }
    // Nothing was recorded for this session at all. A caller asking about a
    // session that does not exist and one asking about a session whose usage
    // is unreadable deserve different answers; only the first is nothing.
    if total_request_count == 0 {
        return Ok(None);
    }
    // A total is only reported when it means what it appears to mean.
    let unresolved_identity = diagnostics.contains(&UsageDiagnostic::UnresolvedRequestIdentity);
    if let Some(folded) = &total {
        if saw_cache_write_split
            && folded.cache_write_5m_tokens.is_none()
            && folded.cache_write_1h_tokens.is_none()
        {
            diagnostics.insert(UsageDiagnostic::PartialCacheWriteSplit);
        }
        if saw_reported_cost && folded.reported_cost_usd.is_none() {
            diagnostics.insert(UsageDiagnostic::PartialReportedCost);
        }
    }
    let usage = match (overflowed, unresolved_identity) {
        // Requests that may be per record cannot be added into a per-request
        // total. Reporting the sum anyway is exactly the multiplied figure
        // this grouping exists to prevent.
        (false, false) => total,
        _ => None,
    };
    Ok(Some(SessionUsageSummary {
        source: source.to_string(),
        session_id: session_id.to_string(),
        usage,
        request_count,
        total_request_count,
        accounting: accounting.into_iter().collect(),
        models: models.into_iter().collect(),
        first_ts_ms: if first_ts_ms == i64::MAX {
            0
        } else {
            first_ts_ms
        },
        last_ts_ms: if last_ts_ms == i64::MIN {
            0
        } else {
            last_ts_ms
        },
        diagnostics: diagnostics.into_iter().collect(),
        overflowed,
    }))
}

#[cfg(test)]
mod fixtures;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{init_db, open_db};

    fn db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        conn
    }

    #[allow(clippy::too_many_arguments)]
    fn event(
        conn: &Connection,
        source: &str,
        session_id: &str,
        message_id: &str,
        ts_ms: i64,
        role: &str,
        kind: &str,
        model: Option<&str>,
        token_json: Option<&str>,
        uid: &str,
    ) {
        // `provider_message_id` mirrors what the parser stores: the
        // provider's own message id, shared by every record of one request.
        // Seeding it keeps these tests about grouping and normalization
        // rather than about the un-identified fallback, which has its own
        // tests.
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, message_id, provider_message_id, ts_ms, role, kind, text, model, token_json, event_uid) \
             VALUES (?1, ?2, ?3, ?3, ?4, ?5, ?6, 'x', ?7, ?8, ?9)",
            rusqlite::params![source, session_id, message_id, ts_ms, role, kind, model, token_json, uid],
        )
        .unwrap();
    }

    const CLAUDE_USAGE: &str = r#"{"input_tokens":3,"cache_creation_input_tokens":4773,"cache_read_input_tokens":11496,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":4773},"output_tokens":43}"#;

    /// The multi-block-turn shape: four content blocks of one Claude message,
    /// each carrying an identical copy of `message.usage`.
    fn multi_block_turn(conn: &Connection) {
        for (index, kind) in ["thinking", "text", "tool_use", "tool_use"]
            .into_iter()
            .enumerate()
        {
            event(
                conn,
                "claude",
                "s1",
                "msg_multi_1",
                1_000,
                "assistant",
                kind,
                Some("claude-opus-4-7"),
                Some(CLAUDE_USAGE),
                &format!("msg_multi_1:{index}"),
            );
        }
    }

    #[test]
    fn one_claude_message_is_one_request_however_many_blocks_it_has() {
        let conn = db();
        multi_block_turn(&conn);
        let page = session_requests_page(&conn, "claude", "s1", 50, None).unwrap();
        assert_eq!(page.requests.len(), 1);
        let request = &page.requests[0];
        assert_eq!(request.request_key, "msg_multi_1");
        assert_eq!(
            request.request_key_source,
            RequestKeySource::ProviderMessageId
        );
        assert_eq!(request.event_count, 4);
        assert!(request.has_thinking);
        let usage = request.usage.as_ref().unwrap();
        assert_eq!(usage.input_tokens, 3);
        assert_eq!(usage.output_tokens, 43);
        assert_eq!(usage.cache_write_1h_tokens, Some(4773));
    }

    /// The failure this whole grouping exists to prevent: summing the stored
    /// rows would report 4 x 43 output tokens for one request.
    #[test]
    fn the_summary_counts_a_multi_block_request_exactly_once() {
        let conn = db();
        multi_block_turn(&conn);
        let summary = session_usage_summary(&conn, "claude", "s1")
            .unwrap()
            .unwrap();
        assert_eq!(summary.request_count, 1);
        let usage = summary.usage.as_ref().unwrap();
        assert_eq!(usage.input_tokens, 3);
        assert_eq!(usage.output_tokens, 43);
        assert_eq!(usage.cache_read_tokens, 11496);
        assert_eq!(usage.cache_write_tokens, 4773);
        assert_eq!(usage.cache_write_1h_tokens, Some(4773));
        assert_eq!(summary.accounting, vec![UsageAccounting::PerMessage]);
        assert_eq!(summary.models, vec!["claude-opus-4-7".to_string()]);
        let raw_row_sum: i64 = conn
            .query_row(
                "SELECT SUM(json_extract(token_json, '$.output_tokens')) FROM session_events",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            raw_row_sum, 172,
            "the per-block copies really are duplicated"
        );
    }

    #[test]
    fn disagreeing_usage_copies_are_refused_with_a_diagnostic() {
        let conn = db();
        event(
            &conn,
            "claude",
            "s1",
            "m1",
            1,
            "assistant",
            "text",
            None,
            Some(r#"{"input_tokens":10}"#),
            "m1:0",
        );
        event(
            &conn,
            "claude",
            "s1",
            "m1",
            1,
            "assistant",
            "thinking",
            None,
            Some(r#"{"input_tokens":99}"#),
            "m1:1",
        );
        let page = session_requests_page(&conn, "claude", "s1", 50, None).unwrap();
        assert_eq!(page.requests[0].usage, None);
        assert_eq!(
            page.requests[0].diagnostics,
            vec![UsageDiagnostic::AmbiguousUsageCopies]
        );
        // The evidence is unusable, not absent. Reporting nothing here would
        // make a session with corrupt usage look like a session that has
        // none, which is the one thing a caller cannot recover from.
        let summary = session_usage_summary(&conn, "claude", "s1")
            .unwrap()
            .unwrap();
        assert_eq!(summary.usage, None);
        assert_eq!(summary.total_request_count, 1);
        assert_eq!(summary.request_count, 0);
        assert_eq!(
            summary.diagnostics,
            vec![UsageDiagnostic::AmbiguousUsageCopies]
        );
    }

    /// The same distinction from the other side: a session nothing was ever
    /// recorded for is the only case that answers with nothing at all.
    #[test]
    fn a_session_with_no_requests_at_all_has_no_summary() {
        let conn = db();
        assert_eq!(
            session_usage_summary(&conn, "claude", "absent").unwrap(),
            None
        );
    }

    #[test]
    fn an_unnormalizable_blob_reports_its_error_code() {
        let conn = db();
        event(
            &conn,
            "claude",
            "s1",
            "m1",
            1,
            "assistant",
            "text",
            None,
            Some(r#"{"input_tokens":-4}"#),
            "m1:0",
        );
        let page = session_requests_page(&conn, "claude", "s1", 50, None).unwrap();
        assert_eq!(
            page.requests[0].usage_error.as_deref(),
            Some("USAGE_NON_INTEGER_COUNTER")
        );
        assert_eq!(
            page.requests[0].diagnostics,
            vec![UsageDiagnostic::UnnormalizableUsage]
        );
    }

    #[test]
    fn missing_output_tokens_reports_zero_with_a_coverage_note() {
        let conn = db();
        event(
            &conn,
            "claude",
            "s1",
            "msg_partial_1",
            1,
            "assistant",
            "text",
            Some("claude-sonnet-4-6"),
            Some(r#"{"input_tokens":10}"#),
            "msg_partial_1:0",
        );
        let summary = session_usage_summary(&conn, "claude", "s1")
            .unwrap()
            .unwrap();
        let usage = summary.usage.as_ref().unwrap();
        assert_eq!(usage.output_tokens, 0);
        assert!(usage.coverage.has_input_tokens);
        assert!(!usage.coverage.has_output_tokens);
        assert!(!usage.coverage.is_complete());
    }

    #[test]
    fn a_session_with_no_usage_evidence_reports_a_request_and_no_totals() {
        let conn = db();
        event(
            &conn,
            "claude",
            "s1",
            "m1",
            1,
            "assistant",
            "text",
            None,
            None,
            "m1:0",
        );
        let summary = session_usage_summary(&conn, "claude", "s1")
            .unwrap()
            .unwrap();
        assert_eq!(summary.usage, None);
        assert_eq!(summary.total_request_count, 1);
        assert!(summary.diagnostics.is_empty());
        let page = session_requests_page(&conn, "claude", "s1", 50, None).unwrap();
        assert_eq!(page.requests.len(), 1, "the request is still a request");
        assert_eq!(page.requests[0].usage, None);
    }

    #[test]
    fn user_turns_are_not_requests() {
        let conn = db();
        event(
            &conn, "claude", "s1", "u1", 1, "user", "text", None, None, "u1:0",
        );
        let page = session_requests_page(&conn, "claude", "s1", 50, None).unwrap();
        assert!(page.requests.is_empty());
    }

    #[test]
    fn a_page_never_interleaves_two_sources_or_sessions() {
        let conn = db();
        event(
            &conn,
            "claude",
            "s1",
            "m1",
            1,
            "assistant",
            "text",
            None,
            Some(r#"{"input_tokens":1}"#),
            "m1:0",
        );
        event(
            &conn,
            "codex",
            "s1",
            "m2",
            2,
            "assistant",
            "text",
            None,
            Some(r#"{"input_tokens":7}"#),
            "m2:0",
        );
        event(
            &conn,
            "claude",
            "s2",
            "m3",
            3,
            "assistant",
            "text",
            None,
            Some(r#"{"input_tokens":5}"#),
            "m3:0",
        );
        let page = session_requests_page(&conn, "claude", "s1", 50, None).unwrap();
        assert_eq!(page.requests.len(), 1);
        assert_eq!(page.requests[0].request_key, "m1");
    }

    #[test]
    fn paging_walks_every_request_once_even_on_a_shared_timestamp() {
        let conn = db();
        for index in 0..7 {
            event(
                &conn,
                "claude",
                "s1",
                &format!("m{index}"),
                1_000,
                "assistant",
                "text",
                None,
                Some(r#"{"input_tokens":1,"output_tokens":1}"#),
                &format!("m{index}:0"),
            );
        }
        let mut seen = Vec::new();
        let mut cursor = None;
        for _ in 0..20 {
            let page = session_requests_page(&conn, "claude", "s1", 2, cursor.as_ref()).unwrap();
            assert!(page.requests.len() <= 2);
            seen.extend(page.requests.iter().map(|r| r.request_key.clone()));
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        seen.sort();
        let mut expected: Vec<String> = (0..7).map(|index| format!("m{index}")).collect();
        expected.sort();
        assert_eq!(seen, expected);
        let summary = session_usage_summary(&conn, "claude", "s1")
            .unwrap()
            .unwrap();
        assert_eq!(summary.request_count, 7);
        assert_eq!(summary.usage.as_ref().unwrap().output_tokens, 7);
    }

    /// Codex deltas are already per-request; the summary must reproduce the
    /// provider's final cumulative total from them.
    #[test]
    fn codex_per_request_deltas_sum_to_the_final_cumulative_total() {
        let conn = db();
        event(
            &conn,
            "codex",
            "s1",
            "1:agent_message",
            1_000,
            "assistant",
            "text",
            Some("gpt-5.4"),
            Some(
                r#"{"input_tokens":3000,"cached_input_tokens":1000,"cache_write_input_tokens":0,"output_tokens":200,"reasoning_output_tokens":50,"total_tokens":3200}"#,
            ),
            "1:agent_message",
        );
        event(
            &conn,
            "codex",
            "s1",
            "2:agent_message",
            2_000,
            "assistant",
            "text",
            Some("gpt-5.4"),
            Some(
                r#"{"input_tokens":3500,"cached_input_tokens":500,"cache_write_input_tokens":0,"output_tokens":250,"reasoning_output_tokens":40,"total_tokens":3750}"#,
            ),
            "2:agent_message",
        );
        let summary = session_usage_summary(&conn, "codex", "s1")
            .unwrap()
            .unwrap();
        assert_eq!(summary.request_count, 2);
        let usage = summary.usage.as_ref().unwrap();
        // The fixture's cumulative endpoint is input 6500 / cached 1500 /
        // output 450 / reasoning 90 / total 6950.
        assert_eq!(usage.input_tokens + usage.cache_read_tokens, 6500);
        assert_eq!(usage.cache_read_tokens, 1500);
        assert_eq!(usage.output_tokens, 450);
        assert_eq!(usage.reasoning_tokens, Some(90));
        assert_eq!(usage.provider_total_tokens, Some(6950));
        assert_eq!(summary.accounting, vec![UsageAccounting::CumulativeDelta]);
    }

    #[test]
    fn tool_use_ids_ride_along_with_their_request() {
        let conn = db();
        multi_block_turn(&conn);
        for (index, id) in ["toolu_bash_1", "toolu_agent_1"].into_iter().enumerate() {
            conn.execute(
                "INSERT INTO tool_calls (source, session_id, message_id, tool_use_id, name, ts_ms) \
                 VALUES ('claude', 's1', 'msg_multi_1', ?1, 'Bash', ?2)",
                rusqlite::params![id, 1_000 + index as i64],
            )
            .unwrap();
        }
        conn.execute(
            "INSERT INTO tool_calls (source, session_id, message_id, tool_use_id, name, ts_ms) \
             VALUES ('claude', 's1', 'other_msg', 'toolu_other', 'Bash', 9)",
            [],
        )
        .unwrap();
        let page = session_requests_page(&conn, "claude", "s1", 50, None).unwrap();
        assert_eq!(
            page.requests[0].tool_use_ids,
            vec!["toolu_bash_1".to_string(), "toolu_agent_1".to_string()]
        );
    }

    #[test]
    fn the_view_survives_a_reopen_of_an_existing_database() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.db");
        {
            let conn = open_db(&path).unwrap();
            event(
                &conn,
                "claude",
                "s1",
                "m1",
                1,
                "assistant",
                "text",
                None,
                Some(r#"{"input_tokens":1}"#),
                "m1:0",
            );
        }
        let conn = open_db(&path).unwrap();
        assert!(session_usage_summary(&conn, "claude", "s1")
            .unwrap()
            .is_some());
    }
}
