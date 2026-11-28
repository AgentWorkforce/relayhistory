//! History evidence crossing the Node boundary.
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

    pub(crate) fn of(
        &mut self,
        source: &str,
        session_id: Option<&str>,
    ) -> anyhow::Result<Vec<String>> {
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
