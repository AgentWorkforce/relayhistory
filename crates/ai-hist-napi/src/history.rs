//! History and search evidence crossing the Node boundary.
use ai_hist::history_search::SearchRow;
use ai_hist::{session_locations, HistoryEntry};
use napi_derive::napi;

#[napi(object)]
pub struct NativeHistoryEntry {
    pub id: i64,
    pub source: String,
    pub session_id: Option<String>,
    pub event_uid: Option<String>,
    pub project: Option<String>,
    pub prompt: String,
    pub timestamp_ms: i64,
    pub locations: Vec<String>,
}

impl NativeHistoryEntry {
    pub(crate) fn from_entry(
        locations: &mut SessionLocations<'_>,
        entry: HistoryEntry,
    ) -> anyhow::Result<Self> {
        let locations = locations.of(&entry.source, entry.session_id.as_deref())?;
        Ok(Self {
            id: entry.id,
            source: entry.source,
            session_id: entry.session_id,
            event_uid: entry.event_uid,
            project: entry.project,
            prompt: entry.prompt,
            timestamp_ms: entry.timestamp_ms,
            locations,
        })
    }
}

/// The locations of each session a result set names, read once per session
/// with one cached statement however many rows share it.
pub(crate) struct SessionLocations<'c> {
    conn: &'c rusqlite::Connection,
    known: std::collections::HashMap<(String, String), Vec<String>>,
}

impl<'c> SessionLocations<'c> {
    pub(crate) fn new(conn: &'c rusqlite::Connection) -> Self {
        Self {
            conn,
            known: std::collections::HashMap::new(),
        }
    }

    fn of(&mut self, source: &str, session_id: Option<&str>) -> anyhow::Result<Vec<String>> {
        let Some(session_id) = session_id else {
            return Ok(Vec::new());
        };
        let key = (source.to_string(), session_id.to_string());
        if let Some(locations) = self.known.get(&key) {
            return Ok(locations.clone());
        }
        let locations = session_locations(self.conn, source, session_id)?;
        self.known.insert(key, locations.clone());
        Ok(locations)
    }
}

/// One `search` match. `id` is unique only within `match_source`: a prompt
/// is a `history` row, anything else a `session_event` row.
#[napi(object)]
pub struct NativeSearchMatch {
    pub id: i64,
    pub source: String,
    pub session_id: Option<String>,
    pub event_uid: Option<String>,
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
}

impl NativeSearchMatch {
    pub(crate) fn from_row(
        locations: &mut SessionLocations<'_>,
        row: SearchRow,
    ) -> anyhow::Result<Self> {
        let locations = locations.of(&row.source, row.session_id.as_deref())?;
        Ok(Self {
            id: row.id,
            source: row.source,
            session_id: row.session_id,
            event_uid: row.event_uid,
            project: row.project,
            prompt: row.text,
            timestamp_ms: row.timestamp_ms,
            locations,
            match_source: row.match_source,
            role: row.role,
            kind: row.kind,
        })
    }
}
