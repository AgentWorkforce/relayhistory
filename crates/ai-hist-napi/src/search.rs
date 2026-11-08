//! Search rows exposed to JavaScript: a match and the stored row it came
//! from, with snippet, echo-grouping and origin fields.

use crate::SessionLocations;
use ai_hist::history_search::SearchRow;
use napi_derive::napi;

/// One `search` match. `id` is unique only within `match_source`: a prompt
/// is a `history` row, anything else a `session_event` row.
#[napi(object)]
pub struct NativeSearchMatch {
    pub id: i64,
    pub source: String,
    pub session_id: Option<String>,
    pub project: Option<String>,
    /// The matched text: the prompt for a `history` match, the event text
    /// otherwise. Named `prompt` so a match is still a history entry.
    pub prompt: String,
    pub timestamp_ms: i64,
    pub locations: Vec<String>,
    /// `history` or `session_event`.
    pub match_source: String,
    /// `user` for a `history` match; the event's role otherwise.
    pub role: String,
    /// `history` for a `history` match; the event's kind otherwise.
    pub kind: String,
    /// Match-centered snippet of `prompt`; `prompt` keeps the full text.
    pub snippet: String,
    pub content_truncated: bool,
    /// Codepoint offset of the snippet in the full text; null only when the
    /// text is empty.
    pub snippet_start_offset: Option<i64>,
    /// The echo group this primary represents; null for a singleton.
    pub group_id: Option<String>,
    pub group_size: i64,
    pub echo_count: i64,
    pub group_truncated: bool,
    pub is_primary: bool,
    /// `human`, `peer`, `system`, `synthetic` or `unknown`.
    pub origin: String,
    /// Why a user-role event is not a human prompt; null otherwise.
    pub control_kind: Option<String>,
    pub event_uid: Option<String>,
    pub event_ref: NativeSearchEventRef,
}

/// The stored row a search match came from.
#[napi(object)]
pub struct NativeSearchEventRef {
    pub match_source: String,
    pub id: i64,
    pub session_id: Option<String>,
    pub event_uid: Option<String>,
}

impl NativeSearchMatch {
    pub(crate) fn from_row(
        locations: &mut SessionLocations<'_>,
        row: SearchRow,
    ) -> anyhow::Result<Self> {
        let locations = locations.of(&row.source, row.session_id.as_deref())?;
        let event_ref = row.event_ref();
        Ok(Self {
            id: row.id,
            source: row.source,
            session_id: row.session_id,
            project: row.project,
            prompt: row.text,
            timestamp_ms: row.timestamp_ms,
            locations,
            match_source: row.match_source,
            role: row.role,
            kind: row.kind,
            snippet: row.snippet,
            content_truncated: row.content_truncated,
            snippet_start_offset: row.snippet_start_offset.map(|value| value as i64),
            group_id: row.group_id,
            group_size: row.group_size,
            echo_count: row.echo_count,
            group_truncated: row.group_truncated,
            is_primary: row.is_primary,
            origin: row.origin,
            control_kind: row.control_kind,
            event_uid: row.event_uid,
            event_ref: NativeSearchEventRef {
                match_source: event_ref.match_source,
                id: event_ref.id,
                session_id: event_ref.session_id,
                event_uid: event_ref.event_uid,
            },
        })
    }
}
