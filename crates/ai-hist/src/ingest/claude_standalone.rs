//! Claude records that store no event row of their own, and the one-time
//! repair for responses an earlier parser stored no request for.

use super::control::SlashCommandTriads;
use super::incremental;
use super::is_claude_synthetic_placeholder_model;
use super::tool_result_facts::ToolResultIndexer;
use anyhow::Result;
use rusqlite::Connection;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::path::Path;

/// Whether a Claude assistant record stores no event row of its own: no
/// content block of it becomes an event in `ingest_claude_record` -- empty
/// content, blank text, a `thinking` block with no display text, or a block
/// type the event model drops (`redacted_thinking`, `image`, ...).
///
/// A run of such records that opens with an empty `thinking` block (see
/// [`claude_record_may_stand_alone`]) is held until the record after it: a
/// later record of the same message that stores rows takes the response and
/// the run is carried by its markers alone, while anything else means the run
/// stands alone and its empty thinking block stores the event that gives the
/// response a request and its usage.
pub(crate) fn claude_record_stores_no_event(obj: &Map<String, Value>) -> bool {
    let Some(message) = obj.get("message").and_then(Value::as_object) else {
        return false;
    };
    if message
        .get("model")
        .and_then(Value::as_str)
        .is_some_and(is_claude_synthetic_placeholder_model)
    {
        return false;
    }
    match message.get("content") {
        None | Some(Value::Null) => true,
        Some(Value::String(text)) => text.trim().is_empty(),
        Some(Value::Array(blocks)) => blocks.iter().all(|block| {
            match block.get("type").and_then(Value::as_str).unwrap_or("") {
                "tool_use" | "tool_result" => false,
                "text" => block
                    .get("text")
                    .and_then(Value::as_str)
                    .is_none_or(|text| text.trim().is_empty()),
                "thinking" => thinking_text(block).is_none_or(|text| text.trim().is_empty()),
                _ => true,
            }
        }),
        Some(_) => true,
    }
}

/// Whether a record that stores no event can stand alone: it carries the
/// empty `thinking` block Claude streams first in a response, which is what
/// stores its event when nothing of its message follows. The readers hold
/// such a record for the record after it ([`ClaudeRecordWalk`], and
/// `HeldMessage` in the incremental reader).
pub(crate) fn claude_record_may_stand_alone(obj: &Map<String, Value>) -> bool {
    claude_record_stores_no_event(obj)
        && obj
            .get("message")
            .and_then(|message| message.get("content"))
            .and_then(Value::as_array)
            .is_some_and(|blocks| {
                blocks
                    .iter()
                    .any(|block| block.get("type").and_then(Value::as_str) == Some("thinking"))
            })
}

fn thinking_text(block: &Value) -> Option<&str> {
    block
        .get("thinking")
        .or_else(|| block.get("text"))
        .and_then(Value::as_str)
}

/// The text a Claude `thinking` block stores as an event, if it stores one.
///
/// A block with no display text is carried by its `thinking_signature`
/// marker, which names the record's request, while a later record of its
/// message stores that request's rows. A record that stands alone is its
/// response's only evidence, so its block stores the event (with its empty
/// text) that gives the response a request and its usage.
pub(super) fn claude_thinking_event_text(block: &Value, stands_alone: bool) -> Option<&str> {
    let text = thinking_text(block);
    if stands_alone {
        return Some(text.unwrap_or(""));
    }
    text.filter(|text| !text.trim().is_empty())
}

/// Settle a Claude assistant record's request usage
/// (`settle_claude_request_usage`), first retiring the empty thinking events
/// its message's standalone records stored when the record stores rows.
///
/// A record that stands alone stores its empty thinking event only so that
/// its response has a request. A later record of the message that stores rows
/// -- after an interleaved record, past the held-bytes ceiling, or in a later
/// pass over bytes an earlier pass (a hook's snapshot) ended on -- carries the
/// request itself, so those events go, and with them their usage copies,
/// before the record settles its own: the rows match a from-zero read, where
/// the opening record stores only its marker. Only a standalone record stores
/// a thinking event with blank text. The retirement is the local side of
/// `retire_evidence_share`, on cached statements, because it runs for every
/// Claude assistant record that stores rows.
pub(super) fn settle_record_usage(
    conn: &Connection,
    session_id: &str,
    request_id: Option<&str>,
    provider_message_id: &str,
    token_json: Option<&str>,
    obj: &Map<String, Value>,
) -> Result<Option<String>> {
    if !claude_record_stores_no_event(obj) {
        retire_standalone_events(conn, session_id, provider_message_id)?;
    }
    super::settle_claude_request_usage(
        conn,
        session_id,
        request_id,
        provider_message_id,
        token_json,
    )
}

fn retire_standalone_events(
    conn: &Connection,
    session_id: &str,
    provider_message_id: &str,
) -> Result<()> {
    const STANDALONE: &str = "source = 'claude' AND session_id = ?1 AND provider_message_id = ?2 \
           AND role = 'assistant' AND kind = 'thinking' \
           AND TRIM(COALESCE(text, ''), ' ' || char(9, 10, 13)) = ''";
    conn.prepare_cached(&format!(
        "DELETE FROM session_events WHERE {STANDALONE} AND location = 'local'"
    ))?
    .execute(rusqlite::params![session_id, provider_message_id])?;
    conn.prepare_cached(&format!(
        "UPDATE session_events SET location = 'remote' WHERE {STANDALONE} AND location = 'both'"
    ))?
    .execute(rusqlite::params![session_id, provider_message_id])?;
    Ok(())
}

/// A whole-file reader's walk over one transcript's records in file order.
///
/// A record that may stand alone (see [`claude_record_may_stand_alone`]) is
/// held, with the records of its message after it that store no event either:
/// a later record of the same message that stores rows takes the response's
/// evidence, and the held records are indexed as its marker-only opening;
/// anything else, the end of the bytes, or the held bytes passing
/// [`incremental::CLAUDE_DEFERRED_BYTES_CAP`] means they stand alone. The
/// incremental reader makes the same decision over its `HeldMessage`, so every
/// reader stores the same rows.
pub(super) struct ClaudeRecordWalk<'a> {
    conn: &'a Connection,
    path: &'a Path,
    attributed_session_id: Option<&'a str>,
    file_session_id: Option<&'a str>,
    // Ordering is assigned over the whole transcript, and these readers
    // always read it from the start, so a re-sync reproduces the same indexes
    // instead of advancing them.
    indexer: ToolResultIndexer,
    cache_reads: HashMap<String, i64>,
    triads: SlashCommandTriads,
    held_message_id: String,
    held: Vec<String>,
    held_bytes: usize,
}

impl<'a> ClaudeRecordWalk<'a> {
    pub(super) fn new(
        conn: &'a Connection,
        path: &'a Path,
        attributed_session_id: Option<&'a str>,
        file_session_id: Option<&'a str>,
    ) -> Self {
        Self {
            conn,
            path,
            attributed_session_id,
            file_session_id,
            indexer: ToolResultIndexer::default(),
            cache_reads: HashMap::new(),
            triads: SlashCommandTriads::default(),
            held_message_id: String::new(),
            held: Vec::new(),
            held_bytes: 0,
        }
    }

    /// Take one complete record's bytes. One that is malformed or not an
    /// object is skipped, and follows the held records all the same.
    pub(super) fn feed(&mut self, line: &str) -> Result<()> {
        let Ok(Value::Object(obj)) = serde_json::from_str::<Value>(line) else {
            return self.release(true);
        };
        let message_id = incremental::claude_message_progress(&obj).map(|(id, _)| id);
        let continues = !self.held.is_empty() && message_id.as_ref() == Some(&self.held_message_id);
        if continues && claude_record_stores_no_event(&obj) {
            self.hold(line)?;
            return Ok(());
        }
        self.release(!continues)?;
        match message_id {
            Some(id) if claude_record_may_stand_alone(&obj) => {
                self.held_message_id = id;
                self.hold(line)
            }
            _ => self.index(line, &obj, false),
        }
    }

    /// The end of the bytes: whatever is held stands alone.
    pub(super) fn finish(mut self) -> Result<()> {
        self.release(true)
    }

    /// Add a record to the held run, which stands alone once it passes the
    /// incremental reader's ceiling, as it does there.
    fn hold(&mut self, line: &str) -> Result<()> {
        self.held_bytes += line.len();
        self.held.push(line.to_string());
        if self.held_bytes > incremental::CLAUDE_DEFERRED_BYTES_CAP {
            self.release(true)?;
        }
        Ok(())
    }

    /// Index the held records; `alone` when nothing of their message follows.
    fn release(&mut self, alone: bool) -> Result<()> {
        self.held_bytes = 0;
        for line in std::mem::take(&mut self.held) {
            if let Ok(Value::Object(obj)) = serde_json::from_str::<Value>(&line) {
                self.index(&line, &obj, alone)?;
            }
        }
        Ok(())
    }

    fn index(&mut self, line: &str, obj: &Map<String, Value>, stands_alone: bool) -> Result<()> {
        super::ingest_claude_record(
            self.conn,
            self.path,
            self.attributed_session_id,
            self.file_session_id,
            line,
            obj,
            &mut self.indexer,
            &mut self.cache_reads,
            &mut self.triads,
            stands_alone,
        )
    }
}

/// One-time sweep for stores an earlier parser built, which stored no request
/// for a response whose only record stores no event. The store migration
/// ([`reopen_claude_responses_without_requests`]) drops those transcripts'
/// cursors; this name is in `SWEEP_PARSER_GENERATIONS`, so the source
/// fingerprint an earlier build stored cannot skip the sweep that re-reads
/// them. Every other transcript is skipped on its cursor as usual.
pub(super) const CLAUDE_STANDALONE_RECORDS_GENERATION: &str = "claude_standalone_records_v1";

/// The one-time repairs of the Claude request evidence an earlier parser
/// stored.
///
/// Rows stored before the parser settled streamed Claude requests and
/// reclassified `<synthetic>` notices are repaired from the stored rows alone
/// (`heal_claude_request_evidence`; v2 because the summary repair joined the
/// pass after a revision had already recorded v1 without it; every step is
/// idempotent). A response stored with no request is re-opened for a re-read
/// ([`reopen_claude_responses_without_requests`]).
pub(crate) fn migrate_claude_request_evidence(conn: &Connection) -> Result<()> {
    if !crate::store::migration_applied(conn, "claude_request_evidence_v2")? {
        super::heal_claude_request_evidence(conn)?;
        conn.execute(
            "INSERT OR IGNORE INTO schema_migrations (name) VALUES ('claude_request_evidence_v2')",
            [],
        )?;
    }
    reopen_claude_responses_without_requests(conn)
}

/// Re-open the Claude transcripts holding a response an earlier parser stored
/// no request for, so the next `sync` or hydration re-reads exactly those.
///
/// A parser before standalone records (see [`claude_record_stores_no_event`])
/// stored a response whose only record was a signed, empty `thinking` block as
/// its `thinking_signature` marker alone: no event row, so no request and no
/// usage. The marker names the response's `provider_message_id`, so such a
/// response is one whose session has no event row naming it. The usage was
/// never stored, so the rows cannot be repaired in place; instead the
/// transcripts that can hold the response -- the session's own, and a sidecar
/// whose records it owns -- lose their record cursors, so the sweep reads them
/// from zero, and the hydration checkpoints of the session and every session
/// above it lose their stamps (the session's own also its cursor), so
/// hydration passes over them instead of short-circuiting and reads each
/// re-opened file from zero. Every other transcript keeps its cursor.
/// [`CLAUDE_STANDALONE_RECORDS_GENERATION`] makes the first sweep after
/// upgrading run despite an unchanged source fingerprint.
///
/// Runs once, recorded in `schema_migrations`. It is pending work for a
/// writer rather than a schema change, so a read-only handle reads the store
/// as it stands until a writer has run it.
fn reopen_claude_responses_without_requests(conn: &Connection) -> Result<()> {
    if !standalone_records_migration_pending(conn)? {
        return Ok(());
    }
    let sessions: Vec<String> = conn
        .prepare(
            "SELECT DISTINCT m.session_id FROM session_markers m \
             WHERE m.source = 'claude' AND m.subkind = 'thinking_signature' \
               AND json_type(m.payload_json, '$.provider_message_id') = 'text' \
               AND NOT EXISTS( \
                 SELECT 1 FROM session_events e \
                 WHERE e.source = 'claude' AND e.session_id = m.session_id \
                   AND e.provider_message_id = json_extract(m.payload_json, '$.provider_message_id'))",
        )?
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    for session_id in &sessions {
        conn.execute(
            "DELETE FROM transcript_cursors WHERE source = 'claude' AND locator IN ( \
               SELECT raw_path FROM sessions \
                WHERE source = 'claude' AND session_id = ?1 AND raw_path IS NOT NULL \
               UNION \
               SELECT evidence_locator FROM session_relationships \
                WHERE source = 'claude' AND evidence_locator IS NOT NULL \
                  AND COALESCE(child_session_id, parent_session_id) = ?1)",
            [session_id],
        )?;
        for table in [
            "session_hydration_checkpoints",
            "observation_hydration_checkpoints",
        ] {
            conn.execute(
                &format!(
                    "UPDATE {table} \
                        SET committed_offset = 0, prefix_hash = NULL, dev_ino = NULL, \
                            parser_state_json = NULL \
                      WHERE source = 'claude' AND session_id = ?1"
                ),
                [session_id],
            )?;
            conn.execute(
                &format!(
                    "WITH RECURSIVE above(session_id) AS ( \
                       SELECT ?1 \
                       UNION \
                       SELECT r.parent_session_id FROM session_relationships r \
                         JOIN above ON r.child_session_id = above.session_id \
                        WHERE r.source = 'claude') \
                     UPDATE {table} SET source_stamp = NULL \
                      WHERE source = 'claude' AND session_id IN (SELECT session_id FROM above)"
                ),
                [session_id],
            )?;
        }
    }
    conn.execute(
        "INSERT OR IGNORE INTO schema_migrations (name) VALUES (?1)",
        [STANDALONE_RECORDS_MIGRATION],
    )?;
    Ok(())
}

const STANDALONE_RECORDS_MIGRATION: &str = "claude_standalone_records_v1";

/// Whether [`reopen_claude_responses_without_requests`] has yet to run.
pub(crate) fn standalone_records_migration_pending(conn: &Connection) -> Result<bool> {
    Ok(!crate::store::migration_applied(
        conn,
        STANDALONE_RECORDS_MIGRATION,
    )?)
}
