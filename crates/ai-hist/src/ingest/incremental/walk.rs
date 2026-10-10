//! The line walk of one Claude incremental pass.

use super::super::ingest_claude_record;
use super::super::transcript_cursor::{
    ClaudeCursorState, IncrementalPass, ReadRecord, TranscriptReader,
};
use super::{place_claude_record, release_any, HeldMessage, Placement};
use anyhow::Result;
use rusqlite::Connection;
use serde_json::Value;
use std::path::Path;

pub(super) struct ClaudeIncrementalWalk<'a> {
    pub(super) conn: &'a Connection,
    pub(super) path: &'a Path,
    pub(super) attributed_session_id: Option<&'a str>,
    pub(super) reader: &'a mut TranscriptReader,
    pub(super) pass: &'a mut IncrementalPass,
    pub(super) claude: &'a mut ClaudeCursorState,
    pub(super) between_records: &'a mut dyn FnMut() -> Result<()>,
    pub(super) file_session_id: Option<String>,
    pub(super) sessionless_from: Option<u64>,
    pub(super) reread_bytes: u64,
    pub(super) defer_unfinished: bool,
    pub(super) held: Option<HeldMessage>,
    pub(super) unterminated_tool_results:
        Option<crate::ingest::tool_result_facts::ToolResultIndexer>,
    pub(super) unterminated: Option<(u64, usize)>,
    pub(super) line_index: usize,
}

impl ClaudeIncrementalWalk<'_> {
    pub(super) fn read_lines(&mut self) -> Result<()> {
        let mut line = String::new();
        loop {
            if !self.take_line(&mut line)? {
                break;
            }
        }
        Ok(())
    }

    /// One record. `false` ends the pass (`break` in the reader loop).
    fn take_line(&mut self, line: &mut String) -> Result<bool> {
        let line_start = self.reader.position();
        let Some(kind) = self.reader.next_line(line)? else {
            return Ok(false);
        };
        // One record is in hand: the callback runs per record, not for the
        // end-of-file probe, so a transcript of exactly 1,999 records does
        // not pay a commit for a record that is not there.
        (self.between_records)()?;
        if let ReadRecord::Oversized { terminated } = kind {
            // Skipped, not held: the ceiling exists so one record cannot cost
            // the file's size in memory, and holding it to decide would be
            // the thing it prevents. A record whose end has not arrived is
            // left uncommitted so a writer still producing it is not skipped
            // past.
            self.pass.oversized_records += 1;
            if terminated {
                // Skipped, but it follows the held message all the same.
                release_any(
                    self.conn,
                    self.path,
                    self.attributed_session_id,
                    self.file_session_id.as_deref(),
                    &mut self.held,
                    self.claude,
                )?;
                self.line_index += 1;
                return Ok(true);
            }
            return Ok(false);
        }
        let index = self.line_index;
        // Taken before the record is indexed, and only for the one record a
        // later pass can rewind to.
        if kind == ReadRecord::Unterminated {
            self.unterminated_tool_results = Some(self.claude.tool_results.clone());
        }
        // A record with no newline is considered only if it is complete
        // JSON. A half-written line is not, and a writer appends a line at a
        // time, so parsing is the available evidence that the provider
        // finished saying this.
        let record = line.trim_end_matches(['\n', '\r']);
        let parsed = serde_json::from_str::<Value>(record).ok();
        if kind == ReadRecord::Unterminated && parsed.is_none() {
            // Nothing about this record is committed, so its index is not
            // spent either. Advancing it here handed the record a different
            // fallback identity once it completed than a re-parse from zero
            // would derive, and a record with neither `uuid` nor `message.id`
            // would then exist twice under two derived ids.
            return Ok(false);
        }
        // A malformed record that *did* end in a newline is committed bytes,
        // so it keeps consuming its index.
        self.line_index += 1;
        let Some(obj) = parsed.as_ref().and_then(Value::as_object) else {
            if kind == ReadRecord::Unterminated {
                return Ok(false);
            }
            // Malformed or not an object, but a complete record that follows
            // the held message.
            release_any(
                self.conn,
                self.path,
                self.attributed_session_id,
                self.file_session_id.as_deref(),
                &mut self.held,
                self.claude,
            )?;
            return Ok(true);
        };
        self.pass.records += 1;
        if self.note_file_session(obj, kind, line_start)? {
            return Ok(true);
        }
        // An unterminated record goes through the same deferral decision as
        // any other. Indexing it on the spot because it parsed wrote a live
        // assistant line with `stop_reason: null` straight out, left
        // `in_progress` empty, and let the cursor advance past a message that
        // was still being written — deferral was skipped precisely where the
        // file is most likely to be mid-write.
        if kind == ReadRecord::Unterminated {
            self.unterminated = Some((line_start, index));
        }
        self.pass.deferral_overflowed |= place_claude_record(
            self.conn,
            self.path,
            self.attributed_session_id,
            self.file_session_id.as_deref(),
            &mut self.held,
            self.claude,
            Placement {
                hold: self.defer_unfinished,
                offset: line_start,
                index,
                line,
                record,
                obj,
            },
        )?;
        Ok(kind != ReadRecord::Unterminated)
    }

    /// Learn the file's session from the first record that names one, and
    /// replay the sessionless prefix once that id is known.
    ///
    /// `true` when this record is itself still sessionless and must not be
    /// indexed yet (`continue`).
    fn note_file_session(
        &mut self,
        obj: &serde_json::Map<String, Value>,
        kind: ReadRecord,
        line_start: u64,
    ) -> Result<bool> {
        if self.file_session_id.is_some() || self.attributed_session_id.is_some() {
            return Ok(false);
        }
        match obj
            .get("sessionId")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
        {
            Some(id) => {
                self.file_session_id = Some(id.to_string());
                if let Some(from) = self.sessionless_from.take() {
                    self.reread_bytes += replay_sessionless_prefix(
                        self.conn,
                        self.path,
                        self.claude,
                        self.file_session_id.as_deref(),
                        self.reader,
                        from,
                        line_start,
                    )?;
                }
                Ok(false)
            }
            // Complete records only: an unterminated one is re-read next
            // pass anyway. The caller still indexes an unterminated record
            // that parsed; a complete sessionless record waits for an id.
            None if kind != ReadRecord::Unterminated => {
                self.sessionless_from.get_or_insert(line_start);
                Ok(true)
            }
            None => Ok(false),
        }
    }
}

fn replay_sessionless_prefix(
    conn: &Connection,
    path: &Path,
    claude: &mut ClaudeCursorState,
    file_session_id: Option<&str>,
    reader: &mut TranscriptReader,
    from: u64,
    line_start: u64,
) -> Result<u64> {
    reader.replay(from, line_start, |held| {
        let Ok(value) = serde_json::from_str::<Value>(held) else {
            return Ok(());
        };
        let Some(held_obj) = value.as_object() else {
            return Ok(());
        };
        ingest_claude_record(
            conn,
            &mut claude.file_parse(path, None, file_session_id),
            held,
            held_obj,
            false,
        )
    })
}
