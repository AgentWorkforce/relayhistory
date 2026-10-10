use super::{
    BTreeSet, NormalizedUsage, RawRequest, ResolvedUsage, SessionUsageSummary, UsageAccounting,
    UsageDiagnostic,
};

/// The fixed-size accumulator behind [`SessionUsageSummary`], fed one request
/// at a time in `(first_ts_ms, id)` order.
pub(super) struct UsageFold {
    total: Option<NormalizedUsage>,
    overflowed: bool,
    request_count: u64,
    total_request_count: u64,
    accounting: BTreeSet<UsageAccounting>,
    diagnostics: BTreeSet<UsageDiagnostic>,
    models: BTreeSet<String>,
    first_ts_ms: i64,
    last_ts_ms: i64,
    // Whether any contributor reported a split / a cost at all. Comparing
    // these against the folded total is what tells "nobody reported it" from
    // "some did and the fold could not combine them".
    saw_cache_write_split: bool,
    saw_reported_cost: bool,
}

impl Default for UsageFold {
    fn default() -> Self {
        Self {
            total: None,
            overflowed: false,
            request_count: 0,
            total_request_count: 0,
            accounting: BTreeSet::new(),
            diagnostics: BTreeSet::new(),
            models: BTreeSet::new(),
            first_ts_ms: i64::MAX,
            last_ts_ms: i64::MIN,
            saw_cache_write_split: false,
            saw_reported_cost: false,
        }
    }
}

impl UsageFold {
    pub(super) fn push(&mut self, raw: &RawRequest, resolved: &ResolvedUsage) {
        self.total_request_count = self.total_request_count.saturating_add(1);
        self.first_ts_ms = self.first_ts_ms.min(raw.first_ts_ms);
        self.last_ts_ms = self.last_ts_ms.max(raw.last_ts_ms);
        if let Some(model) = raw.model.as_deref().filter(|model| !model.is_empty()) {
            if !self.models.contains(model) {
                self.models.insert(model.to_string());
            }
        }
        self.diagnostics
            .extend(resolved.diagnostics.iter().copied());
        let Some(usage) = resolved.usage.as_ref() else {
            return;
        };
        self.saw_cache_write_split |=
            usage.cache_write_5m_tokens.is_some() || usage.cache_write_1h_tokens.is_some();
        self.saw_reported_cost |= usage.reported_cost_usd.is_some();
        self.request_count = self.request_count.saturating_add(1);
        self.accounting.insert(usage.accounting);
        self.total = match self.total.take() {
            None => Some(usage.clone()),
            Some(running) => match running.checked_add(usage) {
                Some(sum) => Some(sum),
                None => {
                    self.overflowed = true;
                    Some(running)
                }
            },
        };
    }

    pub(super) fn finish(mut self, source: &str, session_id: &str) -> Option<SessionUsageSummary> {
        // Nothing was recorded for this session at all. A caller asking about
        // a session that does not exist and one asking about a session whose
        // usage is unreadable deserve different answers; only the first is
        // nothing.
        if self.total_request_count == 0 {
            return None;
        }
        // A total is only reported when it means what it appears to mean.
        let unresolved_identity = self
            .diagnostics
            .contains(&UsageDiagnostic::UnresolvedRequestIdentity);
        if let Some(folded) = &self.total {
            // Either bucket, not both. A contributor that omits only the 1h
            // bucket leaves the 5m one populated, and requiring both to
            // vanish reported that half-split as a complete one.
            if self.saw_cache_write_split
                && (folded.cache_write_5m_tokens.is_none()
                    || folded.cache_write_1h_tokens.is_none())
            {
                self.diagnostics
                    .insert(UsageDiagnostic::PartialCacheWriteSplit);
            }
            if self.saw_reported_cost && folded.reported_cost_usd.is_none() {
                self.diagnostics
                    .insert(UsageDiagnostic::PartialReportedCost);
            }
        }
        let usage = match (self.overflowed, unresolved_identity) {
            // Requests that may be per record cannot be added into a
            // per-request total. Reporting the sum anyway is exactly the
            // multiplied figure this grouping exists to prevent.
            (false, false) => self.total,
            _ => None,
        };
        Some(SessionUsageSummary {
            source: source.to_string(),
            session_id: session_id.to_string(),
            usage,
            request_count: self.request_count,
            total_request_count: self.total_request_count,
            accounting: self.accounting.into_iter().collect(),
            models: self.models.into_iter().collect(),
            first_ts_ms: if self.first_ts_ms == i64::MAX {
                0
            } else {
                self.first_ts_ms
            },
            last_ts_ms: if self.last_ts_ms == i64::MIN {
                0
            } else {
                self.last_ts_ms
            },
            diagnostics: self.diagnostics.into_iter().collect(),
            overflowed: self.overflowed,
        })
    }
}
