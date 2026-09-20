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

use super::cursor::*;
use super::*;

/// How many bytes of deferred records one pass will hold before it gives up on
/// deferral for the oldest of them.
///
/// Deferral exists so a half-written message is not indexed as if it were
/// finished, not to make the reader hold a transcript in memory. A well-formed
/// transcript has at most one message open at its tail, so this ceiling is
/// only reached by a malformed or adversarial file — one whose assistant
/// messages never carry a `stop_reason`. When it is reached the oldest held
/// message is indexed as it stands and the pass continues, which keeps memory
/// bounded *and* keeps the reader making forward progress. Indexing an
/// unfinished message early is recoverable: its rows carry their own record
/// identity, so the blocks that arrive later land as further rows rather than
/// as corrections to these.
const CLAUDE_DEFERRED_BYTES_CAP: usize = 8 * 1024 * 1024;

/// The same ceiling expressed in messages, for a file of many tiny unfinished
/// ones.
const CLAUDE_DEFERRED_MESSAGE_CAP: usize = 512;

/// Whether this record belongs to an assistant message, and whether that
/// message is finished.
///
/// Claude writes one assistant message as several JSONL records over time, one
/// per content block, and `message.id` is the identity that ties them
/// together. The provider signals "still streaming" by writing the key
/// `stop_reason` with the value `null`, and completion by filling it in.
///
/// **An absent `stop_reason` is treated as finished, not as streaming.** burn
/// parses the field into an `Option` and cannot tell the two apart, but here
/// the difference decides whether a record is ever written: deferring a record
/// whose shape has no completion signal would hold it back on this pass, and
/// on every pass after it, and the message would never be indexed at all. That
/// is exactly the failure this repository keeps finding — a well-formed
/// success returned over work that never happened — so the ambiguous case is
/// resolved towards indexing. Claude sidechain records and the older record
/// shapes in this repository's own corpus omit the field entirely.
///
/// A record with no `message.id` cannot be grouped and is never deferred.
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

/// Records of one unfinished message, and the position that precedes them.
struct DeferredMessage {
    offset: u64,
    line_index: usize,
    lines: Vec<(usize, String)>,
    bytes: usize,
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
    let mut reader = TranscriptReader::open(path, cursor.file.as_ref())?;
    let mut pass = IncrementalPass {
        rotated: reader.rotated,
        ..Default::default()
    };
    // A cursor that was rejected, or one that never existed, means this pass
    // re-reads from zero; the per-source state that went with the old position
    // describes bytes that are no longer there.
    let mut claude = if reader.start_offset() == 0 {
        ClaudeCursorState::default()
    } else {
        cursor.claude.clone().unwrap_or_default()
    };
    let start_offset = reader.start_offset();
    let mut line_index = claude.next_line_index;

    let mut deferred: HashMap<String, DeferredMessage> = HashMap::new();
    // First-held order, which is file order, so the head is always the
    // earliest unfinished message and therefore the commit point.
    let mut deferred_order: Vec<String> = Vec::new();
    let mut deferred_bytes = 0usize;

    let mut line = String::new();
    loop {
        let line_start = reader.position();
        if !reader.next_line(&mut line)? {
            break;
        }
        let index = line_index;
        line_index += 1;
        let Ok(value) = serde_json::from_str::<Value>(line.trim_end()) else {
            continue;
        };
        let Some(obj) = value.as_object() else {
            continue;
        };
        pass.records += 1;
        match claude_message_progress(obj) {
            Some((message_id, complete)) => {
                if let Some(entry) = deferred.get_mut(&message_id) {
                    entry.bytes += line.len();
                    deferred_bytes += line.len();
                    entry.lines.push((index, line.clone()));
                    if complete {
                        flush_deferred(
                            conn,
                            path,
                            attributed_session_id,
                            &message_id,
                            &mut deferred,
                            &mut deferred_order,
                            &mut deferred_bytes,
                        )?;
                    }
                } else if complete {
                    ingest_claude_record(conn, path, attributed_session_id, index, obj)?;
                } else {
                    deferred_bytes += line.len();
                    deferred.insert(
                        message_id.clone(),
                        DeferredMessage {
                            offset: line_start,
                            line_index: index,
                            lines: vec![(index, line.clone())],
                            bytes: line.len(),
                        },
                    );
                    deferred_order.push(message_id);
                }
            }
            None => ingest_claude_record(conn, path, attributed_session_id, index, obj)?,
        }
        while deferred_bytes > CLAUDE_DEFERRED_BYTES_CAP
            || deferred.len() > CLAUDE_DEFERRED_MESSAGE_CAP
        {
            let Some(oldest) = deferred_order.first().cloned() else {
                break;
            };
            pass.deferral_overflowed = true;
            flush_deferred(
                conn,
                path,
                attributed_session_id,
                &oldest,
                &mut deferred,
                &mut deferred_order,
                &mut deferred_bytes,
            )?;
        }
    }

    // Commit before the earliest message still in progress, so the next pass
    // reads it again from its first byte and can see the completion when it
    // arrives. With nothing in progress the commit is the end of the last
    // complete line.
    let (commit_offset, commit_line_index) =
        match deferred_order.first().and_then(|id| deferred.get(id)) {
            Some(entry) => (entry.offset, entry.line_index),
            None => (reader.position(), line_index),
        };
    pass.bytes_read = reader.position().saturating_sub(start_offset);
    pass.in_progress = deferred_order.clone();

    claude.next_line_index = commit_line_index;
    claude.in_progress = deferred_order;
    cursor.file = Some(reader.commit(commit_offset)?);
    cursor.claude = Some(claude);
    Ok(pass)
}

/// Index one deferred message's records in file order and forget it.
fn flush_deferred(
    conn: &Connection,
    path: &Path,
    attributed_session_id: Option<&str>,
    message_id: &str,
    deferred: &mut HashMap<String, DeferredMessage>,
    deferred_order: &mut Vec<String>,
    deferred_bytes: &mut usize,
) -> Result<()> {
    let Some(entry) = deferred.remove(message_id) else {
        return Ok(());
    };
    deferred_order.retain(|id| id != message_id);
    *deferred_bytes = deferred_bytes.saturating_sub(entry.bytes);
    for (index, text) in entry.lines {
        let Ok(value) = serde_json::from_str::<Value>(text.trim_end()) else {
            continue;
        };
        let Some(obj) = value.as_object() else {
            continue;
        };
        ingest_claude_record(conn, path, attributed_session_id, index, obj)?;
    }
    Ok(())
}

/// Read a Claude transcript through a locator-keyed cursor, loading and
/// storing it around the pass. Used for sidecars and for the global sync walk.
pub(crate) fn ingest_claude_transcript_at_locator(
    conn: &Connection,
    path: &Path,
    attributed_session_id: Option<&str>,
) -> Result<IncrementalPass> {
    let locator = path.to_string_lossy().to_string();
    let key = CursorKey::Locator {
        source: "claude",
        locator: &locator,
    };
    let mut cursor = load_cursor(conn, &key)?;
    let pass =
        ingest_claude_transcript_incremental(conn, path, attributed_session_id, &mut cursor)?;
    store_cursor(conn, &key, &cursor)?;
    Ok(pass)
}
