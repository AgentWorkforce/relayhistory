//! Incremental transcript readers.
//!
//! A transcript is appended to while it is being read. These readers resume
//! from the byte offset whose database work committed, withhold a trailing
//! partial line, and defer the records of a message the provider has not
//! finished writing — so what a pass indexes is exactly what the provider has
//! finished saying, and what it reads is exactly what arrived since the last
//! pass.
//!
//! See [`super::cursor`] for the cursor's shape, its two key spaces, the
//! rotation rule and what the prefix hash covers.

use super::transcript_cursor::*;
use super::*;

/// How many bytes of one held message a pass will keep before it indexes the
/// message as it stands.
///
/// Holding exists so a half-written message is not indexed as if it were
/// finished, not to make the reader hold a transcript in memory. Only the
/// file's trailing message can be held (see [`HeldMessage`]), so this ceiling
/// is only reached by one message whose blocks pass 8 MiB without a record
/// following them. When it is reached the message is indexed as it stands and
/// the pass continues, which keeps memory bounded *and* keeps the reader making
/// forward progress. Indexing an unfinished message early is recoverable: its
/// rows carry their own record identity and its usage settles across copies,
/// so the blocks that arrive later land as further rows rather than as
/// corrections to these.
const CLAUDE_DEFERRED_BYTES_CAP: usize = 8 * 1024 * 1024;

/// Whether the last sweep left any Claude transcript holding a trailing
/// message back (see [`HeldMessage`]).
///
/// A held message is released by the record that follows it or, once its
/// writer stops, by the quiet-file rule — and both need a sweep to read the
/// file again. The source fingerprint cannot see either: a transcript that
/// stopped mid-message is byte-for-byte unchanged. So a sweep that left
/// records held does not license the next one to skip. A boolean, always
/// written and never removed: the sync-state merge only folds keys forward,
/// and it would read a number as a legacy offset and keep the larger.
pub(crate) const CLAUDE_HOLDING_RECORDS_KEY: &str = "claude_holding_records";

/// Whether the last sweep recorded [`CLAUDE_HOLDING_RECORDS_KEY`] as true.
pub(crate) fn sweep_left_records_held(state: &Map<String, Value>) -> bool {
    state.get(CLAUDE_HOLDING_RECORDS_KEY) == Some(&Value::Bool(true))
}

/// Whether this record belongs to an assistant message, and whether the record
/// itself says that message is finished.
///
/// Claude writes one assistant message as several JSONL records over time, one
/// per content block, and `message.id` is the identity that ties them
/// together. A filled-in `stop_reason` says the message is finished. A
/// `stop_reason` of `null` says nothing on its own: Claude Code's subagent
/// sidecars write `null` on nearly every record of every message, finished or
/// not, so the record that settles the question is the next one in the file
/// (see [`HeldMessage`]).
///
/// **An absent `stop_reason` is treated as finished.** Older record shapes in
/// this repository's own corpus omit the field entirely, and they carry no
/// signal to wait for.
///
/// A record with no `message.id` cannot be grouped and is never held.
fn claude_message_progress(obj: &Map<String, Value>) -> Option<(String, bool)> {
    let message = obj.get("message").and_then(Value::as_object)?;
    let role = message
        .get("role")
        .and_then(Value::as_str)
        .or_else(|| obj.get("type").and_then(Value::as_str))?;
    if role != "assistant" {
        return None;
    }
    let id = message
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())?;
    let streaming = matches!(message.get("stop_reason"), Some(Value::Null));
    Some((id.to_string(), !streaming))
}

/// The records of the transcript's trailing assistant message, held because
/// nothing after them says the message is finished.
///
/// A message is finished once its record says so (`stop_reason` filled in)
/// **or once any later record follows it** — a different message, a tool
/// result, a system row. Only the message at the end of the file can still be
/// streaming, so at most one is held at a time, and a pass that ends with one
/// commits before its first byte so the next pass reads it again.
///
/// Claude can write another record between two blocks of one message (a tool
/// result for a parallel tool call), so a message released by a following
/// record may gain a further block later. That block is indexed when it
/// arrives under its own record identity, and the request's usage settles
/// across every copy, so the early release neither drops nor double-counts
/// anything.
struct HeldMessage {
    message_id: String,
    offset: u64,
    line_index: usize,
    lines: Vec<String>,
    bytes: usize,
    /// Tool-result ordering as it stood before this message was held: the
    /// state that belongs with the committed offset when the pass backs up to
    /// it.
    tool_results: crate::ingest::tool_result_facts::ToolResultIndexer,
}

/// Read a Claude transcript from its cursor and index what is complete.
///
/// `cursor` is read for the resume position and rewritten with the new one.
/// On return, `cursor.file.offset` is the position through which this
/// transcript's database work is committed: the end of the last complete line,
/// or the first byte of the earliest message still in progress, whichever is
/// lower.
pub(crate) fn ingest_claude_transcript_incremental(
    conn: &Connection,
    path: &Path,
    attributed_session_id: Option<&str>,
    cursor: &mut TranscriptCursorState,
) -> Result<IncrementalPass> {
    ingest_claude_transcript_incremental_batched(
        conn,
        path,
        attributed_session_id,
        cursor,
        &mut || Ok(()),
    )
}

/// [`ingest_claude_transcript_incremental`], calling `between_records` before
/// each record is read.
///
/// Nothing is half-written at that point, so it is where a caller holding a
/// write transaction over the pass can commit and reopen it to keep the
/// transaction bounded -- the sweep's [`super::SweepWrite::record`]. A caller
/// whose transaction must stay whole (hydration) uses the plain form.
pub(crate) fn ingest_claude_transcript_incremental_batched(
    conn: &Connection,
    path: &Path,
    attributed_session_id: Option<&str>,
    cursor: &mut TranscriptCursorState,
    between_records: &mut dyn FnMut() -> Result<()>,
) -> Result<IncrementalPass> {
    let saved_claude = cursor.claude.clone().unwrap_or_default();
    let mut reader = TranscriptReader::open(path, cursor.file.as_ref(), saved_claude.resume_from)?;
    let mut pass = IncrementalPass {
        rotated: reader.rotated,
        ..Default::default()
    };
    // A cursor that was rejected, or one that never existed, means this pass
    // re-reads from zero; the per-source state that went with the old position
    // describes bytes that are no longer there.
    let mut claude = if reader.start_offset() == 0 {
        // The metadata walk keeps its own position and its own fold, and
        // rotation is something it detects for itself. Dropping its state here
        // because *this* walk restarted left the global sync re-deriving a
        // transcript's identity from byte zero on every append: it stored the
        // scan, then this reloaded the same row and wrote a default over it.
        //
        // A walk from byte zero is this parser's own, so it names this
        // generation; a resumed walk keeps the generation that committed the
        // position it resumes from.
        ClaudeCursorState {
            scan: saved_claude.scan.clone(),
            records_parser: super::hydrate::HYDRATION_PARSER_VERSION,
            ..Default::default()
        }
    } else {
        saved_claude.clone()
    };
    let start_offset = reader.start_offset();
    // A rewound pass restarts at the unterminated record it saw last time, so
    // that record gets the index it had before rather than one past it.
    let mut line_index = if reader.start_offset() == saved_claude.resume_from.unwrap_or(u64::MAX) {
        saved_claude
            .resume_line_index
            .unwrap_or(claude.next_line_index)
    } else {
        claude.next_line_index
    };
    if reader.start_offset() == saved_claude.resume_from.unwrap_or(u64::MAX) {
        if let Some(rewound) = saved_claude.resume_tool_results.clone() {
            claude.tool_results = rewound;
        }
    }
    // The session this file belongs to. The sweep's metadata fold has already
    // read it from the head, so a resumed pass does not go looking for it
    // again. A pass with no fold -- a hydration reading the transcript from
    // zero -- learns it from the first record that names one, as the fold
    // does. The sessionless records ahead of that line (Claude's
    // `file-history-snapshot`s, summaries) are kept under the same session a
    // sweep keeps them under: only where they begin is remembered, and the
    // span is read again, a capped record at a time, once the id is known,
    // so a long prefix costs nothing to hold.
    let mut file_session_id = claude
        .scan
        .as_ref()
        .and_then(|scan| scan.fold.session_id.clone());
    let mut sessionless_from: Option<u64> = None;
    let mut reread_bytes = 0u64;
    // Nothing has been appended since the last pass, so whatever was still
    // being written then is not going to be finished. Holding it back again
    // would hold it back forever.
    let defer_unfinished = !reader.quiesced();

    let mut held: Option<HeldMessage> = None;

    // Tool-result ordering as it stood before the unterminated trailing
    // record, if there is one.
    let mut unterminated_tool_results: Option<crate::ingest::tool_result_facts::ToolResultIndexer> =
        None;
    // Where an unterminated trailing record began, and the index it was given.
    let mut unterminated: Option<(u64, usize)> = None;
    let mut line = String::new();
    loop {
        let line_start = reader.position();
        let Some(kind) = reader.next_line(&mut line)? else {
            break;
        };
        // One record is in hand: the callback runs per record, not for the
        // end-of-file probe, so a transcript of exactly 1,999 records does
        // not pay a commit for a record that is not there.
        between_records()?;
        if let ReadRecord::Oversized { terminated } = kind {
            // Skipped, not held: the ceiling exists so one record cannot cost
            // the file's size in memory, and holding it to decide would be
            // the thing it prevents. A record whose end has not arrived is
            // left uncommitted so a writer still producing it is not skipped
            // past.
            pass.oversized_records += 1;
            if terminated {
                line_index += 1;
                continue;
            }
            break;
        }
        let index = line_index;
        // Taken before the record is indexed, and only for the one record a
        // later pass can rewind to.
        if kind == ReadRecord::Unterminated {
            unterminated_tool_results = Some(claude.tool_results.clone());
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
            break;
        }
        // A malformed record that *did* end in a newline is committed bytes,
        // so it keeps consuming its index.
        line_index += 1;
        let Some(value) = parsed else {
            continue;
        };
        let Some(obj) = value.as_object() else {
            if kind == ReadRecord::Unterminated {
                break;
            }
            continue;
        };
        pass.records += 1;
        if file_session_id.is_none() && attributed_session_id.is_none() {
            match obj
                .get("sessionId")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
            {
                Some(id) => {
                    file_session_id = Some(id.to_string());
                    if let Some(from) = sessionless_from.take() {
                        reread_bytes += reader.replay(from, line_start, |held| {
                            let Ok(value) = serde_json::from_str::<Value>(held) else {
                                return Ok(());
                            };
                            let Some(held_obj) = value.as_object() else {
                                return Ok(());
                            };
                            ingest_claude_record(
                                conn,
                                path,
                                None,
                                file_session_id.as_deref(),
                                held,
                                held_obj,
                                &mut claude.tool_results,
                                &mut claude.cache_reads,
                                &mut claude.slash_commands,
                            )
                        })?;
                    }
                }
                // Complete records only: an unterminated one is re-read next
                // pass anyway.
                None if kind != ReadRecord::Unterminated => {
                    sessionless_from.get_or_insert(line_start);
                    continue;
                }
                None => {}
            }
        }
        // An unterminated record goes through the same deferral decision as
        // any other. Indexing it on the spot because it parsed wrote a live
        // assistant line with `stop_reason: null` straight out, left
        // `in_progress` empty, and let the cursor advance past a message that
        // was still being written — deferral was skipped precisely where the
        // file is most likely to be mid-write.
        if kind == ReadRecord::Unterminated {
            unterminated = Some((line_start, index));
        }
        pass.deferral_overflowed |= place_claude_record(
            conn,
            path,
            attributed_session_id,
            file_session_id.as_deref(),
            &mut held,
            &mut claude,
            Placement {
                hold: defer_unfinished,
                offset: line_start,
                index,
                line: &line,
                record,
                obj,
            },
        )?;
        if kind == ReadRecord::Unterminated {
            break;
        }
    }

    // Commit before the message still in progress, so the next pass reads it
    // again from its first byte and can see what follows it when that
    // arrives. With nothing in progress the commit is the end of the last
    // complete line.
    let (commit_offset, commit_line_index, commit_tool_results) = match held.as_ref() {
        Some(entry) => (entry.offset, entry.line_index, entry.tool_results.clone()),
        // With an unterminated record indexed, commit past it so a file
        // nobody has touched compares equal to its cursor and is skipped
        // outright; `resume_from` is what brings the reader back to it if
        // the file ever grows.
        None => match unterminated {
            Some((start, _)) => (
                start + reader.tail_bytes(),
                line_index,
                claude.tool_results.clone(),
            ),
            None => (reader.position(), line_index, claude.tool_results.clone()),
        },
    };
    let in_progress: Vec<String> = held.map(|entry| entry.message_id).into_iter().collect();
    pass.in_progress = in_progress.clone();

    claude.next_line_index = commit_line_index;
    claude.in_progress = in_progress;
    // Only meaningful when the pass committed past the record; a pass that
    // backed up for a held message will re-read it anyway.
    let (resume_from, resume_line_index) = match unterminated {
        Some((start, index)) if claude.in_progress.is_empty() => (Some(start), Some(index)),
        _ => (None, None),
    };
    claude.resume_from = resume_from;
    claude.resume_line_index = resume_line_index;
    claude.resume_tool_results = resume_from.and(unterminated_tool_results);
    // The ordering that belongs with the committed offset, not with wherever
    // the pass happened to stop.
    claude.tool_results = commit_tool_results;
    // A pass whose file was rewritten under it records nothing at all — not
    // the position, not the parser state. Its rows came from bytes that are no
    // longer there, and the next pass reads the same region again and upserts
    // over them.
    match reader.commit(commit_offset)? {
        CommitOutcome::Published(file) => {
            cursor.file = Some(file);
            cursor.claude = Some(claude);
        }
        CommitOutcome::Superseded => pass.superseded = true,
    }
    // Counted after the commit, because validating the cursor is the last
    // provider read a pass makes and `bytesRead` is every provider read.
    pass.validation_bytes = reader.validation_bytes();
    pass.bytes_read = reader
        .position()
        .saturating_sub(start_offset)
        .saturating_add(reader.tail_bytes())
        .saturating_add(reread_bytes)
        .saturating_add(pass.validation_bytes);
    Ok(pass)
}

/// One parsed record and where it sits in the file.
struct Placement<'a> {
    /// Whether an unfinished message may be held at all: false once the file
    /// has gone quiet, when nothing more is coming to finish it.
    hold: bool,
    offset: u64,
    index: usize,
    line: &'a str,
    record: &'a str,
    obj: &'a Map<String, Value>,
}

/// Send one record onto the held message, into a new hold, or straight to the
/// indexer, releasing first whatever message it follows. Returns whether the
/// held message passed [`CLAUDE_DEFERRED_BYTES_CAP`] and was released early.
fn place_claude_record(
    conn: &Connection,
    path: &Path,
    attributed_session_id: Option<&str>,
    file_session_id: Option<&str>,
    held: &mut Option<HeldMessage>,
    claude: &mut ClaudeCursorState,
    at: Placement<'_>,
) -> Result<bool> {
    let progress = claude_message_progress(at.obj);
    // Any record that is not another block of the held message follows it,
    // and a message something follows is finished.
    let continues_held = matches!(
        (&*held, &progress),
        (Some(entry), Some((id, _))) if entry.message_id == *id
    );
    let release = |held: &mut Option<HeldMessage>, claude: &mut ClaudeCursorState| match held.take()
    {
        Some(entry) => release_held(
            conn,
            path,
            attributed_session_id,
            file_session_id,
            entry,
            claude,
        ),
        None => Ok(()),
    };
    if !continues_held {
        release(held, claude)?;
    }
    match progress {
        Some((_, complete)) if continues_held => {
            if let Some(entry) = held.as_mut() {
                entry.bytes += at.line.len();
                entry.lines.push(at.line.to_string());
            }
            if complete {
                release(held, claude)?;
            }
        }
        Some((message_id, false)) if at.hold => {
            *held = Some(HeldMessage {
                message_id,
                offset: at.offset,
                line_index: at.index,
                lines: vec![at.line.to_string()],
                bytes: at.line.len(),
                // Held, not indexed, so the current state is the state before
                // this message.
                tool_results: claude.tool_results.clone(),
            });
        }
        _ => ingest_claude_record(
            conn,
            path,
            attributed_session_id,
            file_session_id,
            at.record,
            at.obj,
            &mut claude.tool_results,
            &mut claude.cache_reads,
            &mut claude.slash_commands,
        )?,
    }
    let overflowed = held
        .as_ref()
        .is_some_and(|entry| entry.bytes > CLAUDE_DEFERRED_BYTES_CAP);
    if overflowed {
        release(held, claude)?;
    }
    Ok(overflowed)
}

/// Index a held message's records in file order.
fn release_held(
    conn: &Connection,
    path: &Path,
    attributed_session_id: Option<&str>,
    file_session_id: Option<&str>,
    entry: HeldMessage,
    claude: &mut ClaudeCursorState,
) -> Result<()> {
    for text in entry.lines {
        let record = text.trim_end_matches(['\n', '\r']);
        let Ok(value) = serde_json::from_str::<Value>(record) else {
            continue;
        };
        let Some(obj) = value.as_object() else {
            continue;
        };
        ingest_claude_record(
            conn,
            path,
            attributed_session_id,
            file_session_id,
            record,
            obj,
            &mut claude.tool_results,
            &mut claude.cache_reads,
            &mut claude.slash_commands,
        )?;
    }
    Ok(())
}

/// What a Claude transcript is skipped on when nothing about it changed.
///
/// A subagent sidecar's `agent-<agentId>.meta.json` is the only place the
/// child's type, name, model and spawn depth are recorded, so metadata that
/// changes beside an untouched transcript is still new evidence and has to
/// reach `session_relationships`. It carries its own whole-file cursor, so it
/// is checked here rather than folded into the transcript's stamp.
pub(crate) fn claude_transcript_unchanged(conn: &Connection, path: &Path) -> Result<bool> {
    if !transcript_cursor::transcript_unchanged(conn, "claude", path)? {
        return Ok(false);
    }
    let metadata = super::hydrate::claude_subagent_meta_path(path);
    // A sidecar that was indexed and has since been deleted is a change, and
    // it is the one change no amount of looking at the file will reveal. Its
    // cursor is the record that it was once there; without this the transcript
    // takes the fast path forever and the relationship keeps describing a
    // child from a file nobody can read any more.
    let indexed_metadata = transcript_cursor::locator_cursor_exists(
        conn,
        super::CLAUDE_SUBAGENT_META_SOURCE,
        &metadata,
    )?;
    if (metadata.is_file() || indexed_metadata)
        && !transcript_cursor::transcript_unchanged(
            conn,
            super::CLAUDE_SUBAGENT_META_SOURCE,
            &metadata,
        )?
    {
        return Ok(false);
    }
    Ok(true)
}

/// Read a Claude transcript through a locator-keyed cursor, loading and
/// storing it around the pass. Used for sidecars and for the global sync walk,
/// both of which now go through the batched form.
#[cfg(test)]
pub(crate) fn ingest_claude_transcript_at_locator(
    conn: &Connection,
    path: &Path,
    attributed_session_id: Option<&str>,
) -> Result<IncrementalPass> {
    ingest_claude_transcript_at_locator_batched(conn, path, attributed_session_id, &mut || Ok(()))
}

/// [`ingest_claude_transcript_at_locator`] with the record-boundary callback
/// of [`ingest_claude_transcript_incremental_batched`].
pub(crate) fn ingest_claude_transcript_at_locator_batched(
    conn: &Connection,
    path: &Path,
    attributed_session_id: Option<&str>,
    between_records: &mut dyn FnMut() -> Result<()>,
) -> Result<IncrementalPass> {
    let locator = path.to_string_lossy().to_string();
    let key = CursorKey::Locator {
        source: "claude",
        locator: &locator,
    };
    let mut cursor = load_cursor(conn, &key)?;
    let pass = ingest_claude_transcript_incremental_batched(
        conn,
        path,
        attributed_session_id,
        &mut cursor,
        between_records,
    )?;
    store_cursor(conn, &key, &cursor)?;
    Ok(pass)
}
