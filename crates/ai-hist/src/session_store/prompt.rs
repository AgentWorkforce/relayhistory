use super::{prompt_hash, PromptRow};
use serde::{Deserialize, Serialize};

/// One `history` row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Prompt {
    /// Stable event reference, absent for anonymous legacy prompt evidence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_uid: Option<String>,
    /// `None` when the query asked for no text.
    pub prompt: Option<String>,
    /// sha256 of the prompt, hex: the hash the ledger stored, or one computed
    /// from the text when it was carried. `None` only for a row written
    /// without a stored hash and read without its text.
    pub prompt_hash: Option<String>,
    pub prompt_bytes: i64,
    pub project: Option<String>,
    pub timestamp_ms: i64,
}

impl Prompt {
    pub(super) fn from_row(row: PromptRow) -> Self {
        Self {
            event_uid: row.event_uid,
            prompt_hash: row
                .prompt_hash
                .or_else(|| row.prompt.as_deref().map(prompt_hash)),
            prompt_bytes: row.prompt_bytes,
            prompt: row.prompt,
            project: row.project,
            timestamp_ms: row.timestamp_ms,
        }
    }
}
