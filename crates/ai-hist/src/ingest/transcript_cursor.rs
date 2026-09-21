//! Byte-offset cursors for provider transcripts.
//!
//! Transcripts grow. A live Claude session appends a few kilobytes at a time,
//! and a fleet machine can carry a multi-hundred-megabyte rollout. Re-reading
//! the whole file on every change is what made relayhistory unusable as a live
//! source, so every transcript reader resumes from the last byte whose
//! database work committed.
//!
//! # What a cursor is
//!
//! One cursor is a [`TranscriptCursorState`]: a validated byte position in one
//! file generation ([`TranscriptFileCursor`]), plus whatever per-source parser
//! state has to survive the gap between two passes. It is stored as the
//! `parser_state_json` document described below, with `committed_offset`,
//! `prefix_hash` and `dev_ino` denormalized beside it as plain columns so a
//! cursor can be inspected and queried without parsing JSON. **The JSON
//! document is the source of truth**; the three columns are projections
//! written from it in the same statement and are never read back into a
//! resume decision.
//!
//! # Where cursors live
//!
//! Two key spaces, one codec, and every row has exactly one writer:
//!
//! * `session_hydration_checkpoints` / `observation_hydration_checkpoints`
//!   carry the cursor for the **session's own primary transcript**, written by
//!   targeted hydration, keyed the same way the checkpoint already was.
//! * `transcript_cursors` is keyed by `(source, locator)` and carries every
//!   other transcript: subagent sidecars, child rollouts, and the files the
//!   global sync walk meets. A growing sidecar therefore advances its own
//!   cursor and never forces its parent to be re-parsed.
//!
//! A transcript reached from both directions — hydrated as a session and also
//! walked by a global sync — holds two independent cursors over the same
//! bytes. That is deliberate and correct: each is one consumer's own committed
//! position, and every insert on the path is an idempotent upsert keyed by
//! provider-native identity, so a region read twice writes the same rows.
//!
//! # The rotation rule
//!
//! Identical to the one the flat prompt logs have used since
//! [`super::CompleteJsonlReader`] was written, and to burn's:
//!
//! ```text
//! inode changed || mtime < cursor.mtime || size < committed_offset
//!   || prefix_hash mismatch   ->   full re-parse from offset 0
//! ```
//!
//! # What `prefix_hash` covers, and why not the whole prefix
//!
//! The issue that specified this offered two shapes for the prefix hash: every
//! byte before the offset, or a bounded window. **This is the bounded window**:
//! SHA-256 over a domain-separated header, the committed offset itself, the
//! first [`PREFIX_WINDOW_BYTES`] of the file, and the last
//! [`PREFIX_WINDOW_BYTES`] before the offset — or, when those two ends meet,
//! the committed prefix in one read. A prefix of `2 * PREFIX_WINDOW_BYTES` or
//! less is entirely covered either way, and reading it once is both cheaper
//! and free of the double-hashed overlap the two-window form has there.
//!
//! Hashing the whole prefix would be strictly stronger and is what the flat
//! prompt logs do, but it costs a read of the entire committed region **on
//! every open**. On a 200 MB transcript that is a 200 MB read to discover that
//! 1 KiB arrived, which is the exact cost incremental reading exists to
//! remove; a validation whose price scales with the file cannot be paid by a
//! one-second watch loop. The window is two seeks and at most 128 KiB, so an
//! append of 1 KiB costs about 1 KiB of reading plus a fixed validation.
//!
//! What the window catches: truncation and regrowth (the offset is hashed in,
//! and `size < offset` is checked outright), replacement by a different file
//! (identity, mtime, and both windows), a rewritten head, and a rewritten tail
//! — which is where an append-only writer that rewinds actually writes. What
//! it does not catch: an edit strictly between the two windows that preserves
//! the total length *and* leaves mtime at or above the recorded one. No
//! provider in this catalog rewrites a transcript's middle in place, and a
//! cursor that was wrong in that way heals on the next rotation; the trade is
//! recorded here rather than left implicit.
//!
//! # What `parser_state_json` holds
//!
//! ```json
//! {
//!   "v": 1,
//!   "file":   { "offset": 4823, "device": 66311, "inode": 91238,
//!               "mtime_ns": 1758…, "size": 5012, "prefix_hash": "9f2c…" },
//!   "claude": { "next_line_index": 4210, "in_progress": ["msg_01…"] },
//!   "codex":  { "next_line_index": 88, "model": "…", "prev_totals": {…} }
//! }
//! ```
//!
//! Unrecognized keys are preserved verbatim across a round trip (see `extra`),
//! so a sibling change that wants to park its own per-source resume state here
//! — a tool-result `call_index`/`event_index` pair, a continuity-evidence
//! watermark, a source fingerprint — adds a key and does not need a column or
//! a migration, and two such changes do not overwrite each other.

use super::*;

/// The cursor document's shape version. Bumped when an existing key changes
/// meaning; adding a key usually does not need it, because an older reader
/// preserves what it does not understand.
///
/// Version 2 is a key that *is* such a change: the metadata fold now carries
/// continuity, folded from the records already consumed. A v1 document's
/// position therefore no longer implies its fold has seen those records —
/// resuming on one would fold only the tail and publish continuity evidence
/// built from part of the file, overwriting the complete row already stored.
/// Discarding v1 reads each transcript once from zero and rebuilds it.
///
/// Version 3 is the marker parsers. A marker exists nowhere but the
/// transcript, so an install upgrading into them has to read every transcript
/// once — which is what main expressed by advancing its `claude_sessions_v3`
/// stamp map to v4. This branch retired that map, so the same one-time
/// re-read is expressed here: a cursor is what the skip path consults now, and
/// discarding the old ones is what makes the pass happen.
pub(crate) const TRANSCRIPT_CURSOR_VERSION: u32 = 3;

/// How much of the committed region each end of the validation window covers.
pub(crate) const PREFIX_WINDOW_BYTES: u64 = 64 * 1024;

fn cursor_version_default() -> u32 {
    TRANSCRIPT_CURSOR_VERSION
}

/// A validated byte position in one generation of one file.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct TranscriptFileCursor {
    /// Bytes through the last newline whose database work has committed.
    pub offset: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub device: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inode: Option<u64>,
    /// The file's mtime when this cursor was written. A file whose mtime has
    /// gone backwards is not the file this cursor describes.
    pub mtime_ns: u64,
    /// The file's size when this cursor was written, for diagnostics.
    pub size: u64,
    /// When this file was first observed at its current size and mtime.
    ///
    /// Carried forward across passes that see no change, so "the writer has
    /// stopped" is a statement about elapsed time rather than about two
    /// consecutive observations, which two quick passes during a model's
    /// pause would otherwise satisfy. See [`QUIESCENT_GRACE_MS`].
    #[serde(default)]
    pub unchanged_since_ms: i64,
    /// See the module docs: a bounded window, not the whole prefix.
    pub prefix_hash: String,
}

/// Claude's per-source resume state.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct ClaudeCursorState {
    /// Absolute index of the next record. Bookkeeping for the commit and the
    /// rewind below, not identity: a record carrying neither `uuid` nor
    /// `message.id` is identified by a hash of its bytes, because compaction
    /// rewrites a transcript's prefix and every position after it shifts.
    #[serde(default)]
    pub next_line_index: usize,
    /// `message.id`s whose assistant message had not finished when the last
    /// pass ended. `offset` already backs up to the first byte of the earliest
    /// of them, so this is carried for reporting and for the
    /// `HYDRATION_IN_PROGRESS_MESSAGES` diagnostic rather than for correctness.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub in_progress: Vec<String>,
    /// Where a trailing record that had no newline began.
    ///
    /// Such a record is indexed if it parses (see the reader), and the cursor
    /// still commits past it so an untouched file compares equal to its cursor
    /// and is skipped. If the file later grows, the newline may have arrived
    /// with more of that same record, so the next pass rewinds to here and
    /// reads it again rather than resuming after a line it only half saw.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_from: Option<u64>,
    /// The record index that goes with `resume_from`, so a rewound pass counts
    /// from where it rewound to rather than from where it stopped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_line_index: Option<usize>,
    /// Tool-result ordering as it stood *before* the record at `resume_from`.
    ///
    /// That record was indexed and committed past, so `tool_results` includes
    /// it. A pass that rewinds re-reads it, and without this it would claim
    /// fresh indexes for rows it had already numbered — once per append, for
    /// as long as the transcript keeps an unterminated tail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_tool_results: Option<super::tool_result_facts::ToolResultIndexer>,
    /// The metadata walk's position and fold over the same file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scan: Option<ClaudeScanState>,
    /// The continuity walk's position and fold over the same file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuity: Option<ClaudeContinuityState>,
    /// The cache read of the assistant message before a compaction boundary,
    /// per session. A boundary reports no size of its own, and the pass that
    /// reads it can resume after the message that stated one.
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub cache_reads: std::collections::HashMap<String, i64>,
    /// The slash-command triad still being chained, per session.
    ///
    /// A command's caveat, invocation and output are three records, and a
    /// pass can end between any two of them; the next pass has to know which
    /// invocation the output row it reads belongs to.
    #[serde(
        default,
        skip_serializing_if = "super::control::SlashCommandTriads::is_empty"
    )]
    pub slash_commands: super::control::SlashCommandTriads,
    /// Tool-result ordering as of the committed offset.
    ///
    /// `call_index` and `event_index` are assigned over the whole transcript,
    /// so a pass that resumes has to continue the sequence rather than restart
    /// it — two results numbered zero would collide on the conflict upsert.
    #[serde(default, skip_serializing_if = "is_default_indexer")]
    pub tool_results: super::tool_result_facts::ToolResultIndexer,
}

fn is_default_indexer(indexer: &super::tool_result_facts::ToolResultIndexer) -> bool {
    *indexer == super::tool_result_facts::ToolResultIndexer::default()
}

/// The metadata walk's own resumable position and running fold.
///
/// Identity and metadata are recovered by a walk over the records, separate
/// from the walk that indexes them: the indexing walk holds records back for a
/// message still being written, the metadata walk has no reason to, and the
/// two therefore commit at different offsets. They get a cursor each rather
/// than one cursor that has to be correct for both.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct ClaudeScanState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<TranscriptFileCursor>,
    /// Where a trailing record without a newline began; see
    /// `ClaudeCursorState::resume_from`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume_from: Option<u64>,
    #[serde(default)]
    pub fold: super::ClaudeMetaFold,
}

/// The continuity walk's own resumable position and running fold.
///
/// Continuity is a fold like the metadata walk's: every field it keeps is
/// first-wins, last-wins or accumulating, so folding newly arrived records
/// onto a saved state gives the same answer as folding the file. It keeps its
/// own position rather than sharing the record walk's, for the same reason the
/// metadata fold does — the record walk backs up for a message still being
/// written, and this walk has no reason to.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct ClaudeContinuityState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<TranscriptFileCursor>,
    #[serde(default)]
    pub evidence: crate::continuity::ContinuityEvidence,
    /// Whether the first non-sidechain user record has been seen, so a
    /// resumed pass does not take a later record's `parentUuid` for the
    /// conversation's own first parent.
    #[serde(default)]
    pub first_user_seen: bool,
    /// Whether any record has ever folded in. Distinguishes "this file says
    /// nothing" — which retracts — from "nothing new arrived".
    #[serde(default)]
    pub any: bool,
}

/// Codex's per-source resume state: everything `ingest_codex_rollout` carries
/// between records that a resumed pass cannot re-derive from the bytes it is
/// about to read.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct CodexCursorState {
    #[serde(default)]
    pub next_line_index: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// The cumulative token snapshot the next delta is measured against.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prev_totals: Option<CodexTokenTotals>,
    /// The API request the rows being written belong to, and which baseline
    /// `prev_totals` holds. Both only ever increase, and a resumed pass has to
    /// continue them: restarting `request_span` at zero would merge a new
    /// call into an old one, and restarting the generation would let a later
    /// delta silently resolve a refusal recorded against an earlier baseline.
    #[serde(default)]
    pub request_span: u64,
    #[serde(default)]
    pub baseline_generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub untokened_assistant_uid: Option<String>,
    #[serde(default)]
    pub saw_model_output: bool,
    /// Tool-result ordering as of the committed offset.
    ///
    /// `call_index` and `event_index` are assigned over the whole rollout, so
    /// a pass that resumes after a completed turn has to continue the
    /// sequence. A fresh indexer restarted every resumed pass at zero and
    /// gave the next turn's first result the index the last turn's already
    /// had.
    #[serde(default, skip_serializing_if = "is_default_indexer")]
    pub tool_results: super::tool_result_facts::ToolResultIndexer,
    /// The turn a record falls inside, carried from the last `turn_context`.
    /// Codex writes it once per turn rather than on every record, so a pass
    /// that resumed mid-turn cannot re-derive it from the bytes it reads and
    /// would stamp the rest of the turn with nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    /// The adjacent-mirror deduper's one-record memory, as
    /// `(is_response_item, text)`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_human_message: Option<(bool, String)>,
}

/// One transcript's complete resume state.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct TranscriptCursorState {
    #[serde(default = "cursor_version_default")]
    pub v: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<TranscriptFileCursor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claude: Option<ClaudeCursorState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codex: Option<CodexCursorState>,
    /// Keys this version does not know about, preserved across a round trip so
    /// sibling per-source states can share the document.
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Derived `Default` would stamp `v: 0` on a fresh cursor, and the document
/// it encoded would then be rejected by its own reader on the next pass -- a
/// cursor that silently never resumes. The version is the constant.
impl Default for TranscriptCursorState {
    fn default() -> Self {
        Self {
            v: TRANSCRIPT_CURSOR_VERSION,
            file: None,
            claude: None,
            codex: None,
            extra: Map::new(),
        }
    }
}

impl TranscriptCursorState {
    pub(crate) fn decode(raw: Option<&str>) -> Self {
        raw.and_then(|text| serde_json::from_str::<Self>(text).ok())
            .filter(|state| state.v == TRANSCRIPT_CURSOR_VERSION)
            .unwrap_or_default()
    }

    pub(crate) fn encode(&self) -> String {
        serde_json::to_string(self).expect("transcript cursor serialization cannot fail")
    }

    pub(crate) fn committed_offset(&self) -> i64 {
        self.file.as_ref().map_or(0, |file| file.offset as i64)
    }

    pub(crate) fn prefix_hash(&self) -> Option<String> {
        self.file.as_ref().map(|file| file.prefix_hash.clone())
    }

    /// `"{device}:{inode}"`, or `None` where the platform does not report one.
    pub(crate) fn dev_ino(&self) -> Option<String> {
        let file = self.file.as_ref()?;
        Some(format!("{}:{}", file.device?, file.inode?))
    }
}

/// Hash the bounded validation window for `[0, offset)`. See the module docs.
/// A record's text, or `None` when its bytes are not valid UTF-8.
fn decode_record(raw: &[u8]) -> Option<&str> {
    std::str::from_utf8(raw).ok()
}

fn prefix_window_digest(file: &mut fs::File, offset: u64) -> Result<String> {
    Ok(prefix_window_digest_counted(file, offset)?.0)
}

/// The same digest, and the provider bytes hashing it read.
///
/// Every caller of this is a read of a provider file, and `bytesRead` is
/// documented as the provider bytes a pass could not avoid reading. Counting
/// them at the one place they are spent is what stops the next reader of this
/// code having to remember a second list.
fn prefix_window_digest_counted(file: &mut fs::File, offset: u64) -> Result<(String, u64)> {
    let mut hasher = Sha256::new();
    // v2: the two windows are hashed as one span whenever they meet, so a
    // committed prefix of 128 KiB or less is covered by a single read rather
    // than by two that overlap. A v1 digest of such a file was taken over the
    // overlapping region twice and will not match, so those cursors are
    // rejected once and rewritten on the next pass -- the same one-time
    // re-read any rotation causes, and the reason this tag moved rather than
    // the rule changing quietly underneath a stored hash.
    hasher.update(b"relayhistory/transcript-prefix/v2\0");
    hasher.update(offset.to_le_bytes());
    if offset == 0 {
        return Ok((format!("{:x}", hasher.finalize()), 0));
    }
    let mut read_span = |start: u64, len: u64, hasher: &mut Sha256| -> Result<()> {
        file.seek(std::io::SeekFrom::Start(start))?;
        let mut remaining = len;
        let mut buffer = [0u8; 32 * 1024];
        while remaining > 0 {
            let wanted =
                usize::try_from(remaining.min(buffer.len() as u64)).unwrap_or(buffer.len());
            let read = file.read(&mut buffer[..wanted])?;
            anyhow::ensure!(
                read > 0,
                "transcript cursor window extends past end of file"
            );
            hasher.update(&buffer[..read]);
            remaining -= read as u64;
        }
        Ok(())
    };
    match prefix_window_spans(offset) {
        PrefixSpans::Whole { len } => read_span(0, len, &mut hasher)?,
        PrefixSpans::Ends { window, tail_start } => {
            read_span(0, window, &mut hasher)?;
            read_span(tail_start, window, &mut hasher)?;
        }
    }
    let bytes = prefix_window_bytes(offset);
    #[cfg(test)]
    VALIDATION_METER.with(|meter| meter.set(meter.get() + bytes));
    Ok((format!("{:x}", hasher.finalize()), bytes))
}

/// Which regions of the committed prefix a digest covers.
enum PrefixSpans {
    /// The whole committed prefix, as one read.
    Whole { len: u64 },
    /// Two disjoint windows, one at each end.
    Ends { window: u64, tail_start: u64 },
}

/// The regions [`prefix_window_digest_counted`] hashes for a cursor at
/// `offset`.
///
/// Two windows, one at each end -- except when they meet. `window` is
/// `min(PREFIX_WINDOW_BYTES, offset)`, so the tail begins at
/// `offset - window`, and for any prefix of `2 * PREFIX_WINDOW_BYTES` or less
/// that is at or before the end of the head window. Reading them separately
/// then covers some bytes twice and, for a file smaller than one window, the
/// same bytes twice over -- which is what made an unchanged sync over a tree
/// of small transcripts read each file four times to prove it had not
/// changed. One read of the union is cheaper and strictly stronger: there is
/// no gap between the ends left uncovered.
fn prefix_window_spans(offset: u64) -> PrefixSpans {
    let window = PREFIX_WINDOW_BYTES.min(offset);
    let tail_start = offset - window;
    if tail_start <= window {
        PrefixSpans::Whole { len: offset }
    } else {
        PrefixSpans::Ends { window, tail_start }
    }
}

#[cfg(unix)]
pub(crate) fn file_identity(metadata: &fs::Metadata) -> (Option<u64>, Option<u64>) {
    use std::os::unix::fs::MetadataExt;
    (Some(metadata.dev()), Some(metadata.ino()))
}

#[cfg(not(unix))]
pub(crate) fn file_identity(_metadata: &fs::Metadata) -> (Option<u64>, Option<u64>) {
    (None, None)
}

/// A cursor that declares the whole of `path` consumed.
///
/// Not every file a sidecar walk depends on is a record stream. A subagent's
/// `agent-*.meta.json` is one JSON document describing the child, read whole
/// or not at all, and a byte position inside it would mean nothing — but
/// "have these bytes changed since I last read them?" is exactly what a cursor
/// answers, so it gets one, pinned at the file's end. That keeps one change
/// detector for every file the ingest path reads instead of a cursor for some
/// and a stamp map for the rest.
pub(crate) fn whole_file_cursor(path: &Path) -> Result<(TranscriptFileCursor, u64)> {
    let mut file = fs::File::open(path)?;
    let metadata = file.metadata()?;
    let size = metadata.len();
    let (device, inode) = file_identity(&metadata);
    let (prefix_hash, bytes_read) = prefix_window_digest_counted(&mut file, size)?;
    Ok((
        TranscriptFileCursor {
            offset: size,
            device,
            inode,
            mtime_ns: super::metadata_mtime_ns(&metadata),
            size,
            // A whole-file stamp is about "have these bytes changed", not
            // about waiting for a writer, so it carries no quiet-since
            // observation.
            unchanged_since_ms: 0,
            prefix_hash,
        },
        bytes_read,
    ))
}

/// What validating a cursor's committed prefix decided, and what it cost.
pub(crate) struct PrefixCheck {
    /// The bytes behind the cursor are still the bytes on disk.
    pub valid: bool,
    /// Provider bytes this check read. Bounded: two windows, so at most
    /// `2 * PREFIX_WINDOW_BYTES`, and zero for a cursor at offset 0.
    pub bytes_read: u64,
}

impl PrefixCheck {
    /// Nothing could be validated, and nothing was read to find that out.
    const UNVALIDATED: Self = Self {
        valid: false,
        bytes_read: 0,
    };
}

/// How many bytes validating a cursor at `offset` reads.
///
/// At most `2 * PREFIX_WINDOW_BYTES`, and `offset` itself below that, because
/// a prefix the two windows would both reach is read once. Per digest: a
/// Claude transcript validates two positions and pays this twice.
pub(crate) fn prefix_window_bytes(offset: u64) -> u64 {
    if offset == 0 {
        return 0;
    }
    match prefix_window_spans(offset) {
        PrefixSpans::Whole { len } => len,
        PrefixSpans::Ends { window, .. } => 2 * window,
    }
}

/// Whether the bytes a cursor committed are still the bytes on disk.
///
/// The one place this question is answered, for every caller that is about to
/// skip a file. Identity, length and mtime are cheap and they are not proof: a
/// writer that restores timestamps, or a filesystem whose timestamp resolution
/// puts two writes in the same tick, produces a rewritten file with an
/// identical stat. Every skip path needs the same bounded window check, and
/// none of them may advance parser state to get it.
pub(crate) fn committed_prefix_matches(cursor: &TranscriptFileCursor, path: &Path) -> PrefixCheck {
    let Ok(metadata) = path.metadata() else {
        return PrefixCheck::UNVALIDATED;
    };
    let (device, inode) = file_identity(&metadata);
    let identity_changed = cursor.device.is_some()
        && device.is_some()
        && (cursor.device, cursor.inode) != (device, inode);
    if identity_changed
        || metadata.len() < cursor.offset
        || super::metadata_mtime_ns(&metadata) < cursor.mtime_ns
    {
        return PrefixCheck::UNVALIDATED;
    }
    let Ok(mut handle) = fs::File::open(path) else {
        return PrefixCheck::UNVALIDATED;
    };
    let bytes_read = prefix_window_bytes(cursor.offset);
    PrefixCheck {
        valid: prefix_window_digest(&mut handle, cursor.offset)
            .is_ok_and(|digest| digest == cursor.prefix_hash),
        bytes_read,
    }
}

/// Whether `path` is byte-for-byte where its cursor left it.
///
/// This is the cheap skip the global sync walk makes per file: one indexed
/// point query and one `stat`, with the file never opened. It is deliberately
/// stricter than the rotation rule — a file whose committed offset is short of
/// its length has records waiting, whether because the tail was a partial line
/// or because a message was still being written, and both mean "read me".
pub(crate) fn transcript_unchanged(conn: &Connection, source: &str, path: &Path) -> Result<bool> {
    let locator = path.to_string_lossy().to_string();
    let cursor = load_cursor(
        conn,
        &CursorKey::Locator {
            source,
            locator: &locator,
        },
    )?;
    let Some(file) = cursor.file.as_ref() else {
        return Ok(false);
    };
    let Ok(metadata) = path.metadata() else {
        return Ok(false);
    };
    if file.offset != metadata.len()
        || file.mtime_ns != super::metadata_mtime_ns(&metadata)
        || (file.device, file.inode) != file_identity(&metadata)
    {
        return Ok(false);
    }
    // Size, mtime and inode do not prove the bytes are the same ones. A
    // writer that restores timestamps, or a filesystem whose timestamp
    // resolution puts both writes in the same tick, produces a rewritten file
    // with an identical stat — and this fast path runs *before*
    // `TranscriptReader::open`, so the prefix hash it validates never got a
    // say. The file was then skipped on every sync, forever, serving rows
    // from bytes that no longer exist.
    //
    // The same bounded window the cursor already stores, so this costs at
    // most 128 KiB per digest, and only on the files that were about to be
    // skipped anyway. Shared with hydration's own skip path, which asks the
    // same question about the same cursor.
    //
    // Note the *two* digests below: a Claude transcript carries the record
    // walk's position and the metadata fold's in one document, and both have
    // to be current. That is the real per-file ceiling on this path.
    //
    // Both positions in the document have to be current, not just the record
    // walk's: a superseded metadata scan leaves a fold that has to be made
    // again, and skipping on the record cursor alone would leave it stale for
    // as long as nothing else about the file changed.
    Ok(committed_prefix_matches(file, path).valid && scan_position_current(&cursor, path).valid)
}

/// Whether the metadata walk's own position is recorded and still describes
/// the file.
///
/// A Claude transcript carries two positions in one cursor document — where
/// the record walk got to, and where the metadata fold got to — and a skip
/// path that consults only the first will happily skip a file whose *metadata*
/// is stale. A scan that was superseded clears its position, so "no position"
/// here means "this fold has to be made again" and not "this file is new".
pub(crate) fn scan_position_current(cursor: &TranscriptCursorState, path: &Path) -> PrefixCheck {
    let Some(claude) = cursor.claude.as_ref() else {
        // Not a Claude transcript, so there is no second position to check.
        return PrefixCheck {
            valid: true,
            bytes_read: 0,
        };
    };
    let Some(file) = claude.scan.as_ref().and_then(|scan| scan.file.as_ref()) else {
        return PrefixCheck::UNVALIDATED;
    };
    committed_prefix_matches(file, path)
}

/// Forget one locator-keyed cursor, so the next pass reads the file from zero.
///
/// A cursor is a claim about which bytes have already produced rows. When the
/// rows are gone — a wiped or rebuilt database, evidence deleted by a repair —
/// the claim is false, and resuming from it would leave the file looking
/// consumed while nothing it said is in the database. Anything that decides to
/// re-read a file *because its evidence is missing* rather than because the
/// file changed has to clear the cursor first.
pub(crate) fn forget_locator_cursor(conn: &Connection, source: &str, path: &Path) -> Result<()> {
    conn.execute(
        "DELETE FROM transcript_cursors WHERE source = ? AND locator = ?",
        params![source, path.to_string_lossy()],
    )?;
    Ok(())
}

/// Every locator this source has a cursor for, as recorded.
///
/// The cursor table is what replaced the sync state's `path -> stamp` maps, so
/// it is also what answers the question those maps used to: which files does
/// this install know about, as against which files did this walk enumerate. A
/// path the cursors name and the walk did not return is not absent — it is
/// unavailable on this run, and its rows are still there.
pub(crate) fn known_locators(conn: &Connection, source: &str) -> Result<Vec<String>> {
    let mut statement = conn.prepare("SELECT locator FROM transcript_cursors WHERE source = ?")?;
    let rows = statement
        .query_map(params![source], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Drop the cursor recorded for `locator` exactly as stored.
///
/// [`forget_locator_cursor`] takes a path this run holds; this takes a locator
/// read back out of the table, which may name a file this run cannot see.
pub(crate) fn forget_locator(conn: &Connection, source: &str, locator: &str) -> Result<()> {
    conn.execute(
        "DELETE FROM transcript_cursors WHERE source = ? AND locator = ?",
        params![source, locator],
    )?;
    Ok(())
}

/// Whether a cursor has ever been recorded for `path` under `source`.
///
/// Distinguishes "this file has never existed" from "this file was indexed and
/// has since been deleted". The second is a change, and a deletion is the one
/// change that cannot be noticed by looking at the file.
pub(crate) fn locator_cursor_exists(conn: &Connection, source: &str, path: &Path) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM transcript_cursors WHERE source = ? AND locator = ?)",
        params![source, path.to_string_lossy()],
        |row| row.get(0),
    )?)
}

/// Record a whole-file cursor for `path` under `source`, and report the
/// provider bytes hashing it read.
///
/// The count leaves by the front door because the caller has to put it in
/// `bytesRead`: this was the one digest call whose bytes were spent and then
/// dropped, while the skip path already charged the same window when it
/// checked the cursor this writes.
pub(crate) fn stamp_whole_file(conn: &Connection, source: &str, path: &Path) -> Result<u64> {
    let locator = path.to_string_lossy().to_string();
    let key = CursorKey::Locator {
        source,
        locator: &locator,
    };
    let mut cursor = load_cursor(conn, &key)?;
    let (file, bytes_read) = whole_file_cursor(path)?;
    cursor.file = Some(file);
    store_cursor(conn, &key, &cursor)?;
    Ok(bytes_read)
}

/// How a record ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReadRecord {
    /// The record ended with a newline. It is committed work.
    Terminated,
    /// The file ended without one. The record may be everything the writer
    /// will ever put there, or it may be half of a line still being written;
    /// nothing in the bytes distinguishes the two. The reader hands it over
    /// and leaves the decision to the caller, and the position does not
    /// advance past it.
    Unterminated,
    /// The record passed [`MAX_RECORD_BYTES`] before it ended. Nothing is
    /// handed over — the point is not to hold it — and the caller skips it.
    /// `terminated` says whether its newline was found while draining: when
    /// it was, the bytes are behind the reader and the position advanced past
    /// them; when it was not, the file simply ends mid-record and the
    /// position stays put so a writer still working on it is not skipped past.
    Oversized { terminated: bool },
}

/// The largest single record a transcript reader will hold in memory.
///
/// `read_until` extends its buffer until it finds a newline or reaches EOF, so
/// every budget checked *around* the call is advisory: the Claude deferral cap
/// of 8 MiB, for instance, could only ever notice that an allocation had
/// already happened. A transcript containing one 500 MiB record made the
/// reader allocate 500 MiB whatever the caps said.
///
/// 16 MiB is well above any record a provider writes — a tool result carrying
/// a large file read is a few MiB at the outside — and well under the ceiling
/// the memory test asserts, so a record over it is evidence of corruption or
/// of a file that is not a transcript, not of an unusually chatty turn.
pub(crate) const MAX_RECORD_BYTES: u64 = 16 * 1024 * 1024;

/// How long a transcript must sit unchanged before records held back for a
/// message still being written are released.
///
/// Deferral is a bet that the provider will finish the message, and the only
/// evidence available that it will not is that the file has stopped changing.
/// But a model pauses between streamed records all the time — thinking,
/// running a tool, waiting on a network call — and those pauses are routinely
/// longer than the interval between two hydrations. Releasing on the first
/// pass that sees an unchanged file therefore fires during ordinary
/// operation, indexes half a message, and cannot retract it when the rest
/// arrives.
///
/// Two minutes is longer than a pause between content blocks and shorter than
/// a session anyone is waiting on. The asymmetry justifies erring long:
/// releasing late costs latency on a genuinely abandoned message, releasing
/// early publishes a partial one that nothing will take back.
pub(crate) const QUIESCENT_GRACE_MS: i64 = 120_000;

/// What a pass is allowed to record about where it got to.
///
/// A cursor says "the rows in the database came from the bytes up to here,
/// and here is their hash". A pass whose file was rewritten underneath it can
/// honour neither half: its rows hold the old bytes, and hashing the file now
/// would authenticate the new ones. Such a pass records nothing, and the next
/// one reads the region again and corrects the rows — every insert on this
/// path is an idempotent upsert, so re-reading is always safe and publishing a
/// cursor that does not describe its own rows is not.
#[derive(Debug)]
pub(crate) enum CommitOutcome {
    /// The bytes behind the cursor are the bytes the pass read.
    Published(TranscriptFileCursor),
    /// The file changed under the pass in a way that can affect the bytes it
    /// parsed. Nothing is recorded.
    Superseded,
}

/// A transcript opened at its cursor.
pub(crate) struct TranscriptReader {
    /// Only so a test hook can tell one transcript's commit from another's; a
    /// hydration commits a parent, its sidecars and their metadata in turn.
    #[cfg(test)]
    path: PathBuf,
    file: fs::File,
    reader: BufReader<fs::File>,
    position: u64,
    start_offset: u64,
    /// Bytes of an unterminated trailing record handed to the caller.
    tail_bytes: u64,
    /// Records decoded as UTF-8, and records that were not.
    decoded: u64,
    undecodable: u64,
    /// Provider bytes spent validating this transcript rather than reading
    /// records from it: the windows hashed at open, and at commit. Bounded —
    /// at most `2 * PREFIX_WINDOW_BYTES` per digest — and real I/O, so it is
    /// reported like any other read.
    validation_bytes: u64,
    /// The `unchanged_since_ms` this pass will write back.
    unchanged_since_ms: i64,
    /// The file is byte-for-byte where its cursor left it, so nothing has been
    /// appended since the last pass and whatever wrote it has stopped.
    quiesced: bool,
    device: Option<u64>,
    inode: Option<u64>,
    /// The size and mtime observed when the file was opened, which is the
    /// instant [`Self::unchanged_since_ms`] is stamped from. `commit` stats
    /// the file again, and a stat that moved in between means the clock is
    /// measuring a different file state than the one being stored.
    opened_size: u64,
    opened_mtime_ns: u64,
    /// The bounded window over `[0, opened_size)` as it was when the file was
    /// opened — the bytes this pass takes itself to be reading.
    ///
    /// `commit` recomputes it over the same region. A cursor is a claim that
    /// the rows behind it came from the bytes it hashes, and the stat it
    /// stores is taken at commit while the rows were parsed during the walk:
    /// without this, a record rewritten in place mid-walk left a row saying
    /// one thing and a cursor authenticating another, and the next pass
    /// validated that cursor and read nothing.
    opened_window: String,
    /// The saved cursor was rejected and the file is being read from zero
    /// again. Hydration reports this as `HYDRATION_SOURCE_ROTATED`, because a
    /// caller watching a live file needs to know the difference between "1 KiB
    /// arrived" and "the file you were following was replaced".
    pub rotated: bool,
}

impl TranscriptReader {
    /// `rewind_to` asks to start earlier than the committed offset, for a
    /// trailing record the last pass saw without its newline. It is honoured
    /// only when the cursor itself validates and only when it is at or before
    /// that cursor; the prefix window is still checked against the committed
    /// offset, so rewinding cannot be used to skip validation.
    pub(crate) fn open(
        path: &Path,
        saved: Option<&TranscriptFileCursor>,
        rewind_to: Option<u64>,
    ) -> Result<Self> {
        let mut file = fs::File::open(path)?;
        let metadata = file.metadata()?;
        let size = metadata.len();
        let mtime_ns = super::metadata_mtime_ns(&metadata);
        let (device, inode) = file_identity(&metadata);

        let mut rotated = false;
        let mut quiesced = false;
        let mut unchanged_since_ms = now_ms();
        // Hashed once. When the saved cursor covers the whole file — the
        // ordinary resume — the validation below hashes the same region, and
        // reusing it keeps this to one bounded read.
        let mut opened_window: Option<String> = None;
        let mut validation_bytes = 0u64;
        // A cursor at offset 0 is still a cursor. It records the file's
        // identity, size and mtime, and those are what say whether anything
        // has been appended since the last pass. Reading it as "no cursor"
        // because its offset happened to be zero meant a pass that held back
        // the file's *first* record could never see the file go quiet, so it
        // deferred the same records forever and the message was never
        // indexed — while `holding_records` kept forcing the pass to run.
        let offset = match saved {
            Some(saved) => {
                let identity_changed = saved.device.is_some()
                    && device.is_some()
                    && (saved.device, saved.inode) != (device, inode);
                // Checked before the digest, not after. Hashing a window that
                // extends past the end reads every byte it can and then errors,
                // and those bytes left with the error rather than reaching
                // `validation_bytes` — a truncated file's validation pass went
                // unreported. A `stat` answers it without reading anything.
                let saved_window = if size < saved.offset {
                    // A truncated file cannot match a window that ends past
                    // its end. Hashing anyway read every byte it could and
                    // then errored, and those bytes left with the error rather
                    // than reaching `validation_bytes`, so a truncated file's
                    // validation went unreported. `valid` below refuses a
                    // short file on the `stat` alone.
                    None
                } else {
                    match prefix_window_digest_counted(&mut file, saved.offset) {
                        Ok((digest, read)) => {
                            validation_bytes += read;
                            Some(digest)
                        }
                        Err(_) => None,
                    }
                };
                // The same region, so the hash is the same: reuse it rather
                // than reading those bytes a second time.
                if saved.offset == size {
                    opened_window = saved_window.clone();
                }
                let valid = !identity_changed
                    && size >= saved.offset
                    && mtime_ns >= saved.mtime_ns
                    && saved_window.as_deref() == Some(saved.prefix_hash.as_str());
                if valid {
                    let stat_unchanged = size == saved.size && mtime_ns == saved.mtime_ns;
                    // Carry the stamp forward while nothing moves; restart it
                    // the moment anything does.
                    unchanged_since_ms = if stat_unchanged && saved.unchanged_since_ms > 0 {
                        saved.unchanged_since_ms
                    } else {
                        now_ms()
                    };
                    quiesced = stat_unchanged
                        && now_ms().saturating_sub(unchanged_since_ms) >= QUIESCENT_GRACE_MS;
                    rewind_to
                        .filter(|rewind| *rewind <= saved.offset)
                        .unwrap_or(saved.offset)
                } else {
                    // Nothing to rotate away from at offset zero: the cursor
                    // claimed no committed work, so re-reading is not a
                    // correction and reporting one would be noise.
                    rotated = saved.offset > 0;
                    0
                }
            }
            None => 0,
        };

        let opened_window = match opened_window {
            Some(window) => window,
            None => {
                let (digest, read) = prefix_window_digest_counted(&mut file, size)?;
                validation_bytes += read;
                digest
            }
        };

        let mut handle = file.try_clone()?;
        handle.seek(std::io::SeekFrom::Start(offset))?;
        Ok(Self {
            #[cfg(test)]
            path: path.to_path_buf(),
            file,
            reader: BufReader::new(handle),
            position: offset,
            start_offset: offset,
            tail_bytes: 0,
            decoded: 0,
            undecodable: 0,
            validation_bytes,
            opened_size: size,
            opened_mtime_ns: mtime_ns,
            opened_window,
            unchanged_since_ms,
            quiesced,
            device,
            inode,
            rotated,
        })
    }

    /// Nothing has been appended since the cursor was written.
    ///
    /// A writer that has stopped is the only evidence available that a message
    /// with `stop_reason: null` is never going to be finished. Without it, a
    /// session abandoned mid-message would have that message held back on this
    /// pass and on every pass after it.
    pub(crate) fn quiesced(&self) -> bool {
        self.quiesced
    }

    /// Bytes of an unterminated trailing record handed to the caller. Read,
    /// and so counted, even though the position did not advance over them.
    /// Records this pass decoded as UTF-8, and records it could not.
    ///
    /// A single undecodable record is a malformed record and is skipped. A
    /// pass that decoded *none* of the records it read is a different claim:
    /// the walk could not read the file at all, which the sync walk reports
    /// rather than recording as "this transcript holds no session".
    pub(crate) fn decoded(&self) -> u64 {
        self.decoded
    }

    pub(crate) fn undecodable(&self) -> u64 {
        self.undecodable
    }

    pub(crate) fn tail_bytes(&self) -> u64 {
        self.tail_bytes
    }

    /// Provider bytes this pass spent validating the file rather than reading
    /// records from it. Read after `commit`, which adds its own.
    pub(crate) fn validation_bytes(&self) -> u64 {
        self.validation_bytes
    }

    /// Move the quiet-since stamp `ms` into the past.
    ///
    /// Tests need a pass whose walk outlasts the grace window without waiting
    /// two minutes for one.
    #[cfg(test)]
    pub(crate) fn age_unchanged_since_for_test(&mut self, ms: i64) {
        self.unchanged_since_ms -= ms;
    }

    pub(crate) fn position(&self) -> u64 {
        self.position
    }

    pub(crate) fn start_offset(&self) -> u64 {
        self.start_offset
    }

    /// Read the next record into `line`.
    ///
    /// A newline-terminated record is [`ReadRecord::Terminated`] and advances
    /// the position. A trailing buffer with no newline is
    /// [`ReadRecord::Unterminated`]: it is still handed over, because a
    /// provider that simply does not terminate its last line is
    /// indistinguishable from one that has not finished writing it, and
    /// withholding both meant a complete transcript's final record was never
    /// indexed. The position does not advance over it, so the caller decides
    /// what to commit.
    pub(crate) fn next_line(&mut self, line: &mut String) -> Result<Option<ReadRecord>> {
        line.clear();
        let mut raw = Vec::new();
        // The cap is on the reader, not on a check around it: `read_until`
        // extends `raw` until it finds a newline or reaches EOF, so a budget
        // consulted afterwards can only observe an allocation that already
        // happened.
        let mut read = (&mut self.reader)
            .take(MAX_RECORD_BYTES)
            .read_until(b'\n', &mut raw)?;
        if read == 0 {
            return Ok(None);
        }
        // Stopping at the ceiling is not the same claim as passing it: the
        // limited read stops there whether the record ends at the ceiling or
        // runs past it. One byte tells them apart, and a record that ends
        // exactly on the ceiling is an ordinary record — refusing it would
        // drop a complete line on every pass and report it as corruption.
        if raw.last() != Some(&b'\n') && read as u64 == MAX_RECORD_BYTES {
            match self.reader.fill_buf()?.first().copied() {
                // The file ends here. Falls through to the tail handling
                // below, which is what an unterminated final record gets.
                None => {}
                // Terminated, exactly on the ceiling.
                Some(b'\n') => {
                    self.reader.consume(1);
                    raw.push(b'\n');
                    read += 1;
                }
                // Genuinely over. Get past it without ever holding it: drop
                // what was read and walk to the newline in fixed-size chunks.
                Some(_) => {
                    drop(raw);
                    let terminated = self.drain_oversized_record()?;
                    return Ok(Some(ReadRecord::Oversized { terminated }));
                }
            }
        }
        if raw.last() != Some(&b'\n') {
            // A genuine tail: the file ends here, at or under the ceiling.
            let Some(text) = decode_record(&raw) else {
                self.undecodable += 1;
                self.tail_bytes = read as u64;
                return Ok(Some(ReadRecord::Unterminated));
            };
            self.decoded += 1;
            line.push_str(text);
            self.tail_bytes = read as u64;
            return Ok(Some(ReadRecord::Unterminated));
        }
        // A record that is not valid UTF-8 is a malformed record. Repairing it
        // into replacement characters kept the JSON syntactically valid and
        // indexed corrupted text as though it were what the provider wrote;
        // leaving `line` empty routes it through the same skip a JSON parse
        // failure takes.
        match decode_record(&raw) {
            Some(text) => {
                self.decoded += 1;
                line.push_str(text);
            }
            None => self.undecodable += 1,
        }
        self.position += read as u64;
        Ok(Some(ReadRecord::Terminated))
    }

    /// Walk past a record that exceeded [`MAX_RECORD_BYTES`], in fixed-size
    /// chunks, and say whether its newline was found.
    ///
    /// Advancing the position is only correct once the record is behind the
    /// reader. A file that simply ends mid-record may still be being written,
    /// so its bytes stay uncommitted and the next pass meets them again.
    fn drain_oversized_record(&mut self) -> Result<bool> {
        let mut chunk = vec![0u8; 64 * 1024];
        let mut drained = MAX_RECORD_BYTES;
        loop {
            let read = self.reader.read(&mut chunk)?;
            if read == 0 {
                // The file ends inside the record. Nothing is committed — a
                // writer may still be producing it — but the bytes were read,
                // in chunks, and a pass that walked 16 MiB to get here did not
                // read nothing. Reported as tail bytes for the same reason an
                // unterminated record's are: read, and not committed.
                self.tail_bytes = drained;
                return Ok(false);
            }
            if let Some(index) = chunk[..read].iter().position(|byte| *byte == b'\n') {
                drained += index as u64 + 1;
                self.position += drained;
                // Anything after the newline in this chunk belongs to the next
                // record, so start again from the byte after it.
                let resume = self.position;
                self.reader
                    .get_mut()
                    .seek(std::io::SeekFrom::Start(resume))?;
                return Ok(true);
            }
            drained += read as u64;
        }
    }

    /// The cursor to store for a pass that committed through `offset`.
    ///
    /// `offset` may be behind [`Self::position`]: the Claude reader commits
    /// before the earliest message still being written so the next pass reads
    /// it again, and the Codex reader commits at the last `task_complete`.
    pub(crate) fn commit(&mut self, offset: u64) -> Result<CommitOutcome> {
        #[cfg(test)]
        run_before_commit_hook(&self.path);
        let metadata = self.file.metadata()?;
        let size = metadata.len();
        let mtime_ns = super::metadata_mtime_ns(&metadata);
        // A stat that moved during the pass restarts the quiescence clock as
        // well: stamping it at `open` while storing a stat taken here made the
        // window cover the walk, and a full re-parse of a large live
        // transcript outlasts the window on its own.
        let stat_moved = size != self.opened_size || mtime_ns != self.opened_mtime_ns;
        let (device, inode) = file_identity(&metadata);
        let identity_changed = self.device.is_some()
            && device.is_some()
            && (self.device, self.inode) != (device, inode);
        if identity_changed || size < offset || size < self.opened_size {
            return Ok(CommitOutcome::Superseded);
        }
        // Checked on every commit, not only when the stat moved. A rewrite
        // that preserves length and restores mtime is exactly the case a stat
        // cannot see — the skip path already assumes writers do that, and
        // gating this comparison on `stat_moved` left the one rewrite nothing
        // else would catch free to publish a cursor over stale rows.
        //
        // The same bounded window over the same region, so an append compares
        // equal and an in-place rewrite within the window does not. One
        // bounded read is the price of the guarantee, and it is counted.
        let (window, window_bytes) =
            prefix_window_digest_counted(&mut self.file, self.opened_size)?;
        self.validation_bytes += window_bytes;
        if window != self.opened_window {
            return Ok(CommitOutcome::Superseded);
        }
        // The clock and the stat it is compared against have to come from the
        // same instant.
        let unchanged_since_ms = if stat_moved {
            now_ms()
        } else {
            self.unchanged_since_ms
        };
        // The cursor's own hash covers `[0, offset)`, and the comparison above
        // covered `[0, opened_size)`. When a pass consumed the file it opened,
        // those are the same region and the digest is the same digest — so it
        // is reused rather than read a second time. `prefix_window_digest`
        // folds the offset into the hash, so this is only ever done when the
        // offsets are equal.
        let prefix_hash = if offset == self.opened_size {
            window
        } else {
            let (digest, prefix_bytes) = prefix_window_digest_counted(&mut self.file, offset)?;
            self.validation_bytes += prefix_bytes;
            digest
        };
        Ok(CommitOutcome::Published(TranscriptFileCursor {
            offset,
            device: self.device,
            inode: self.inode,
            mtime_ns,
            size,
            unchanged_since_ms,
            prefix_hash,
        }))
    }
}

// Run something between a pass's last read and its commit.
//
// The one moment this module has to get right is the one a test cannot
// otherwise reach: a writer touching the file after the records have been
// parsed and before the cursor is written. The seam exists only under
// `cfg(test)`.
#[cfg(test)]
thread_local! {
    static BEFORE_COMMIT: std::cell::RefCell<Option<BeforeCommitHook>> =
        const { std::cell::RefCell::new(None) };
}

/// Something to run against the transcript being committed.
#[cfg(test)]
pub(crate) type BeforeCommitHook = Box<dyn Fn(&Path)>;

// Validation bytes hashed on this thread, so a test can say what a hydration
// spent checking its cursors without the figure having to be recomputed from
// the rule it is meant to be testing. Incremented where the bytes are read.
#[cfg(test)]
thread_local! {
    static VALIDATION_METER: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Start counting validation bytes from zero on this thread.
#[cfg(test)]
pub(crate) fn reset_validation_meter() {
    VALIDATION_METER.with(|meter| meter.set(0));
}

/// Validation bytes hashed on this thread since the last reset.
#[cfg(test)]
pub(crate) fn validation_meter() -> u64 {
    VALIDATION_METER.with(|meter| meter.get())
}

/// The hook is handed the transcript being committed. A hydration commits the
/// parent, each sidecar and each sidecar's metadata document in turn, so a
/// test that means to disturb one of them has to say which.
#[cfg(test)]
pub(crate) fn set_before_commit_hook_for_test(hook: Option<BeforeCommitHook>) {
    BEFORE_COMMIT.with(|slot| *slot.borrow_mut() = hook);
}

#[cfg(test)]
fn run_before_commit_hook(path: &Path) {
    let hook = BEFORE_COMMIT.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook(path);
        BEFORE_COMMIT.with(|slot| {
            if slot.borrow().is_none() {
                *slot.borrow_mut() = Some(hook);
            }
        });
    }
}

/// Which cursor row to read or write.
#[derive(Clone, Debug)]
pub(crate) enum CursorKey<'a> {
    /// The session's own primary transcript.
    Session {
        source: &'a str,
        session_id: &'a str,
        location: &'a str,
    },
    /// Any other transcript, addressed by the path it was read from.
    Locator { source: &'a str, locator: &'a str },
}

/// Read one transcript's resume state. A row that is missing, unparseable or
/// written by a different document version reads as "start from zero", which
/// is always safe: every insert on the ingest path is an idempotent upsert.
pub(crate) fn load_cursor(conn: &Connection, key: &CursorKey<'_>) -> Result<TranscriptCursorState> {
    let raw: Option<Option<String>> = match key {
        CursorKey::Session {
            source,
            session_id,
            location,
        } => conn
            .query_row(
                "SELECT parser_state_json FROM session_hydration_checkpoints \
                 WHERE source = ? AND session_id = ? AND location = ?",
                params![source, session_id, location],
                |row| row.get(0),
            )
            .optional()?,
        CursorKey::Locator { source, locator } => conn
            .query_row(
                "SELECT parser_state_json FROM transcript_cursors \
                 WHERE source = ? AND locator = ?",
                params![source, locator],
                |row| row.get(0),
            )
            .optional()?,
    };
    Ok(TranscriptCursorState::decode(raw.flatten().as_deref()))
}

/// Write one transcript's resume state, with the three projections.
///
/// The session key updates a checkpoint row that hydration writes in full
/// elsewhere in the same transaction, so this only touches the cursor columns
/// and leaves an absent row absent — a cursor without a checkpoint would be a
/// resume position for evidence nothing recorded.
pub(crate) fn store_cursor(
    conn: &Connection,
    key: &CursorKey<'_>,
    state: &TranscriptCursorState,
) -> Result<()> {
    let encoded = state.encode();
    let offset = state.committed_offset();
    let prefix_hash = state.prefix_hash();
    let dev_ino = state.dev_ino();
    match key {
        CursorKey::Session {
            source,
            session_id,
            location,
        } => {
            conn.execute(
                "UPDATE session_hydration_checkpoints \
                 SET committed_offset = ?, prefix_hash = ?, dev_ino = ?, parser_state_json = ? \
                 WHERE source = ? AND session_id = ? AND location = ?",
                params![
                    offset,
                    prefix_hash,
                    dev_ino,
                    encoded,
                    source,
                    session_id,
                    location
                ],
            )?;
        }
        CursorKey::Locator { source, locator } => {
            conn.execute(
                "INSERT INTO transcript_cursors \
                 (source, locator, committed_offset, prefix_hash, dev_ino, parser_state_json, updated_ms) \
                 VALUES (?, ?, ?, ?, ?, ?, ?) \
                 ON CONFLICT(source, locator) DO UPDATE SET \
                   committed_offset = excluded.committed_offset, \
                   prefix_hash = excluded.prefix_hash, \
                   dev_ino = excluded.dev_ino, \
                   parser_state_json = excluded.parser_state_json, \
                   updated_ms = excluded.updated_ms",
                params![
                    source,
                    locator,
                    offset,
                    prefix_hash,
                    dev_ino,
                    encoded,
                    now_ms()
                ],
            )?;
        }
    }
    Ok(())
}

/// What one incremental pass over a transcript did.
#[derive(Clone, Debug, Default)]
pub(crate) struct IncrementalPass {
    /// Bytes this pass actually read: records, plus the bounded windows it
    /// hashed to satisfy itself that the file is the one its cursor
    /// describes. An append of 1 KiB to a 200 MB transcript reads about 1 KiB
    /// of records and a fixed handful of windows, never a function of the
    /// file's size.
    pub bytes_read: u64,
    /// The validation part of [`Self::bytes_read`], reported separately so
    /// "how much of this transcript did we read" and "what did checking it
    /// cost" are not one number that hides the other.
    pub validation_bytes: u64,
    /// Complete JSON records handed to the per-record indexer.
    pub records: i64,
    /// Messages still unfinished when the pass ended. Their rows were not
    /// written and the committed offset backs up to before the earliest.
    pub in_progress: Vec<String>,
    /// The cursor was discarded and the file re-read from zero.
    pub rotated: bool,
    /// Records that passed `MAX_RECORD_BYTES` and were skipped rather than
    /// held. Reported, because a skipped record is evidence that did not
    /// arrive and silence would make it look like it never existed.
    pub oversized_records: i64,
    /// Deferral hit its memory ceiling and an unfinished message was indexed
    /// early rather than held. Progress is preferred to purity here: the rows
    /// are real records under their own event identity, and the completion
    /// that follows lands as further rows rather than a correction.
    pub deferral_overflowed: bool,
    /// The file was rewritten while this pass was reading it, so the pass
    /// recorded no cursor and the next one reads the same region again.
    pub superseded: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest_of(bytes: &[u8], offset: u64) -> (String, u64) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("transcript.jsonl");
        fs::write(&path, bytes).unwrap();
        let mut file = fs::File::open(&path).unwrap();
        prefix_window_digest_counted(&mut file, offset).unwrap()
    }

    /// Every byte of a committed prefix that fits inside the two windows is
    /// covered, and flipping any one of them is caught.
    ///
    /// The sizes straddle the boundary deliberately. At `2 *
    /// PREFIX_WINDOW_BYTES` or less the ends meet, and the prefix is hashed as
    /// one span; an earlier attempt at that optimization dropped the tail read
    /// whenever the windows overlapped *at all*, which silently stopped
    /// covering the bytes only the tail reached -- caught here by the 96 KiB
    /// case, where the head window ends at 64 KiB.
    #[test]
    fn a_prefix_the_windows_meet_over_is_covered_end_to_end() {
        for offset in [
            1024u64,
            PREFIX_WINDOW_BYTES,
            PREFIX_WINDOW_BYTES + PREFIX_WINDOW_BYTES / 2,
            2 * PREFIX_WINDOW_BYTES,
        ] {
            let bytes = vec![b'a'; offset as usize];
            let (baseline, read) = digest_of(&bytes, offset);
            assert_eq!(
                read, offset,
                "a prefix the windows meet over is read once, not twice"
            );
            for at in [
                0,
                offset / 3,
                PREFIX_WINDOW_BYTES.min(offset - 1),
                offset - 1,
            ] {
                let mut rewritten = bytes.clone();
                rewritten[at as usize] = b'b';
                assert_ne!(
                    digest_of(&rewritten, offset).0,
                    baseline,
                    "a byte changed at {at} of a {offset} byte prefix went unnoticed"
                );
            }
        }
    }

    /// A record that ends exactly on the ceiling is an ordinary record.
    ///
    /// The limited read stops at `MAX_RECORD_BYTES` whether the record ends
    /// there or runs past it, so the read length alone cannot tell the two
    /// apart. Treating both as oversized dropped a complete line on every
    /// pass and reported it as corruption.
    #[test]
    fn a_record_ending_exactly_on_the_ceiling_is_a_record_not_corruption() {
        let dir = tempfile::tempdir().unwrap();
        // Valid JSON whose encoded length is exactly the ceiling.
        let head = br#"{"a":""#;
        let tail = br#""}"#;
        let pad = MAX_RECORD_BYTES as usize - head.len() - tail.len();
        let mut record = Vec::with_capacity(MAX_RECORD_BYTES as usize + 2);
        record.extend_from_slice(head);
        record.extend(std::iter::repeat_n(b'x', pad));
        record.extend_from_slice(tail);
        assert_eq!(record.len() as u64, MAX_RECORD_BYTES);

        let read_first = |bytes: &[u8]| -> (ReadRecord, usize) {
            let path = dir.path().join("transcript.jsonl");
            fs::write(&path, bytes).unwrap();
            let mut reader = TranscriptReader::open(&path, None, None).unwrap();
            let mut line = String::new();
            let kind = reader.next_line(&mut line).unwrap().unwrap();
            (kind, line.len())
        };

        // The file ends on the ceiling: a complete tail, handed over.
        let (kind, len) = read_first(&record);
        assert_eq!(kind, ReadRecord::Unterminated);
        assert_eq!(len, MAX_RECORD_BYTES as usize);

        // Terminated on the ceiling: an ordinary complete record. The reader
        // hands the delimiter over with the line, as it does for every other
        // terminated record; callers trim it before parsing.
        let mut terminated = record.clone();
        terminated.push(b'\n');
        let (kind, len) = read_first(&terminated);
        assert_eq!(kind, ReadRecord::Terminated);
        assert_eq!(len, MAX_RECORD_BYTES as usize + 1);

        // One byte past it, and it is over the ceiling after all.
        let mut oversized = record.clone();
        oversized.insert(head.len(), b'x');
        oversized.push(b'\n');
        let (kind, len) = read_first(&oversized);
        assert_eq!(kind, ReadRecord::Oversized { terminated: true });
        assert_eq!(len, 0, "an oversized record is never handed over");
    }

    /// Past the point where the ends meet, the window is what it says it is:
    /// both ends are covered, and the middle deliberately is not.
    #[test]
    fn a_prefix_longer_than_both_windows_covers_its_ends_only() {
        let offset = 4 * PREFIX_WINDOW_BYTES;
        let bytes = vec![b'a'; offset as usize];
        let (baseline, read) = digest_of(&bytes, offset);
        assert_eq!(read, 2 * PREFIX_WINDOW_BYTES, "two windows, no more");

        for at in [
            0,
            PREFIX_WINDOW_BYTES - 1,
            offset - PREFIX_WINDOW_BYTES,
            offset - 1,
        ] {
            let mut rewritten = bytes.clone();
            rewritten[at as usize] = b'b';
            assert_ne!(
                digest_of(&rewritten, offset).0,
                baseline,
                "a byte changed at {at}, inside a window, went unnoticed"
            );
        }

        // The documented limit, asserted rather than left to be discovered:
        // an edit strictly between the windows that preserves the length is
        // not caught by the hash.
        let mut middle = bytes.clone();
        middle[(2 * PREFIX_WINDOW_BYTES) as usize] = b'b';
        assert_eq!(
            digest_of(&middle, offset).0,
            baseline,
            "the gap between the windows is a known blind spot; if this now \
             fails the window rule changed and the docs above must follow"
        );
    }
}
