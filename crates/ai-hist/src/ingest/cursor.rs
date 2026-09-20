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
//! [`PREFIX_WINDOW_BYTES`] before the offset.
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

/// The cursor document's shape version. Bumped only when an existing key
/// changes meaning; adding a key does not need it, because an older reader
/// preserves what it does not understand.
pub(crate) const TRANSCRIPT_CURSOR_VERSION: u32 = 1;

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
    /// See the module docs: a bounded window, not the whole prefix.
    pub prefix_hash: String,
}

/// Claude's per-source resume state.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub(crate) struct ClaudeCursorState {
    /// Absolute index of the next record, so the fallback event identity for a
    /// record with neither `uuid` nor `message.id` survives a resume.
    #[serde(default)]
    pub next_line_index: usize,
    /// `message.id`s whose assistant message had not finished when the last
    /// pass ended. `offset` already backs up to the first byte of the earliest
    /// of them, so this is carried for reporting and for the
    /// `HYDRATION_IN_PROGRESS_MESSAGES` diagnostic rather than for correctness.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub in_progress: Vec<String>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_delta: Option<CodexTokenTotals>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub untokened_assistant_uid: Option<String>,
    #[serde(default)]
    pub saw_model_output: bool,
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
fn prefix_window_digest(file: &mut fs::File, offset: u64) -> Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(b"relayhistory/transcript-prefix/v1\0");
    hasher.update(offset.to_le_bytes());
    if offset == 0 {
        return Ok(format!("{:x}", hasher.finalize()));
    }
    let window = PREFIX_WINDOW_BYTES.min(offset);
    let mut read_window = |start: u64, hasher: &mut Sha256| -> Result<()> {
        file.seek(std::io::SeekFrom::Start(start))?;
        let mut remaining = window;
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
    read_window(0, &mut hasher)?;
    read_window(offset - window, &mut hasher)?;
    Ok(format!("{:x}", hasher.finalize()))
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
pub(crate) fn whole_file_cursor(path: &Path) -> Result<TranscriptFileCursor> {
    let mut file = fs::File::open(path)?;
    let metadata = file.metadata()?;
    let size = metadata.len();
    let (device, inode) = file_identity(&metadata);
    Ok(TranscriptFileCursor {
        offset: size,
        device,
        inode,
        mtime_ns: super::metadata_mtime_ns(&metadata),
        size,
        prefix_hash: prefix_window_digest(&mut file, size)?,
    })
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
    Ok(file.offset == metadata.len()
        && file.mtime_ns == super::metadata_mtime_ns(&metadata)
        && (file.device, file.inode) == file_identity(&metadata))
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

/// Record a whole-file cursor for `path` under `source`.
pub(crate) fn stamp_whole_file(conn: &Connection, source: &str, path: &Path) -> Result<()> {
    let locator = path.to_string_lossy().to_string();
    let key = CursorKey::Locator {
        source,
        locator: &locator,
    };
    let mut cursor = load_cursor(conn, &key)?;
    cursor.file = Some(whole_file_cursor(path)?);
    store_cursor(conn, &key, &cursor)
}

/// A transcript opened at its cursor, yielding only complete records.
pub(crate) struct TranscriptReader {
    file: fs::File,
    reader: BufReader<fs::File>,
    position: u64,
    start_offset: u64,
    device: Option<u64>,
    inode: Option<u64>,
    /// The saved cursor was rejected and the file is being read from zero
    /// again. Hydration reports this as `HYDRATION_SOURCE_ROTATED`, because a
    /// caller watching a live file needs to know the difference between "1 KiB
    /// arrived" and "the file you were following was replaced".
    pub rotated: bool,
}

impl TranscriptReader {
    pub(crate) fn open(path: &Path, saved: Option<&TranscriptFileCursor>) -> Result<Self> {
        let mut file = fs::File::open(path)?;
        let metadata = file.metadata()?;
        let size = metadata.len();
        let mtime_ns = super::metadata_mtime_ns(&metadata);
        let (device, inode) = file_identity(&metadata);

        let mut rotated = false;
        let offset = match saved {
            Some(saved) if saved.offset > 0 => {
                let identity_changed = saved.device.is_some()
                    && device.is_some()
                    && (saved.device, saved.inode) != (device, inode);
                let valid = !identity_changed
                    && size >= saved.offset
                    && mtime_ns >= saved.mtime_ns
                    && prefix_window_digest(&mut file, saved.offset)
                        .is_ok_and(|digest| digest == saved.prefix_hash);
                if valid {
                    saved.offset
                } else {
                    rotated = true;
                    0
                }
            }
            _ => 0,
        };

        let mut handle = file.try_clone()?;
        handle.seek(std::io::SeekFrom::Start(offset))?;
        Ok(Self {
            file,
            reader: BufReader::new(handle),
            position: offset,
            start_offset: offset,
            device,
            inode,
            rotated,
        })
    }

    pub(crate) fn position(&self) -> u64 {
        self.position
    }

    pub(crate) fn start_offset(&self) -> u64 {
        self.start_offset
    }

    /// Read the next newline-terminated record into `line`.
    ///
    /// A trailing buffer with no newline is the half-written tail of a live
    /// session. It is withheld and the position does not advance, so the next
    /// pass reads it again once the writer has finished the line.
    pub(crate) fn next_line(&mut self, line: &mut String) -> Result<bool> {
        line.clear();
        let mut raw = Vec::new();
        let read = self.reader.read_until(b'\n', &mut raw)?;
        if read == 0 || raw.last() != Some(&b'\n') {
            return Ok(false);
        }
        line.push_str(&String::from_utf8_lossy(&raw));
        self.position += read as u64;
        Ok(true)
    }

    /// The cursor to store for a pass that committed through `offset`.
    ///
    /// `offset` may be behind [`Self::position`]: the Claude reader commits
    /// before the earliest message still being written so the next pass reads
    /// it again, and the Codex reader commits at the last `task_complete`.
    pub(crate) fn commit(&mut self, offset: u64) -> Result<TranscriptFileCursor> {
        let metadata = self.file.metadata()?;
        Ok(TranscriptFileCursor {
            offset,
            device: self.device,
            inode: self.inode,
            mtime_ns: super::metadata_mtime_ns(&metadata),
            size: metadata.len(),
            prefix_hash: prefix_window_digest(&mut self.file, offset)?,
        })
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
    /// Bytes this pass actually read, which is the whole point: an append of
    /// 1 KiB to a 200 MB transcript reads about 1 KiB.
    pub bytes_read: u64,
    /// Complete JSON records handed to the per-record indexer.
    pub records: i64,
    /// Messages still unfinished when the pass ended. Their rows were not
    /// written and the committed offset backs up to before the earliest.
    pub in_progress: Vec<String>,
    /// The cursor was discarded and the file re-read from zero.
    pub rotated: bool,
    /// Deferral hit its memory ceiling and an unfinished message was indexed
    /// early rather than held. Progress is preferred to purity here: the rows
    /// are real records under their own event identity, and the completion
    /// that follows lands as further rows rather than a correction.
    pub deferral_overflowed: bool,
}
