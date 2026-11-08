//! The filter every newest-first history read shares.

use super::{HistoryCursor, SessionScope};
use anyhow::Result;

#[derive(Debug, Clone, Default)]
pub struct QueryFilter {
    pub source: Option<String>,
    pub project: Option<String>,
    pub tag: Option<String>,
    /// Deprecated exclusive timestamp bound. Rows sharing the boundary
    /// timestamp are skipped, so it is not a pagination cursor; use `after`.
    pub before_ms: Option<i64>,
    /// Inclusive lower bound on the row timestamp.
    pub since_ms: Option<i64>,
    /// Inclusive upper bound on the row timestamp.
    pub until_ms: Option<i64>,
    /// Keyset continuation: only rows strictly after this one in the read's
    /// `(timestamp DESC, id DESC, match source)` order. Applied in addition
    /// to the time window.
    pub after: Option<HistoryCursor>,
    pub limit: i64,
    pub scope: SessionScope,
    /// Exact session id a search is restricted to (search only; `recent`
    /// ignores it). Composes with `source`, `project` and `tag`.
    pub session_id: Option<String>,
    /// Event kinds a search may return (search only; empty means every
    /// kind). `history` names prompt rows.
    pub kinds: Vec<String>,
    /// Origin a search is restricted to: `human`, `peer`, `system`,
    /// `synthetic` or `unknown` (search only).
    pub origin: Option<String>,
    /// Snippet window in codepoints for search matches (search only).
    /// `None` means the 320-codepoint default.
    pub max_content_chars: Option<i64>,
}

impl QueryFilter {
    /// The checks every surface applies before a history read, so an invalid
    /// window is the same error from the CLI, the SDK and MCP.
    pub fn validate(&self) -> Result<()> {
        if let (Some(since), Some(until)) = (self.since_ms, self.until_ms) {
            anyhow::ensure!(
                since <= until,
                "since_ms ({since}) must not be later than until_ms ({until})"
            );
        }
        if let Some(cursor) = &self.after {
            cursor.validate()?;
        }
        for kind in &self.kinds {
            anyhow::ensure!(
                matches!(
                    kind.as_str(),
                    "history" | "text" | "thinking" | "tool_use" | "tool_result"
                ),
                "search kind must be one of history, text, thinking, tool_use, tool_result (got {kind})"
            );
        }
        if let Some(origin) = &self.origin {
            anyhow::ensure!(
                matches!(
                    origin.as_str(),
                    "human" | "peer" | "system" | "synthetic" | "unknown"
                ),
                "search origin must be one of human, peer, system, synthetic, unknown (got {origin})"
            );
        }
        if let Some(max_content_chars) = self.max_content_chars {
            anyhow::ensure!(
                (40..=2000).contains(&max_content_chars),
                "max_content_chars must be between 40 and 2000 (got {max_content_chars})"
            );
        }
        Ok(())
    }
}
