use anyhow::Result;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::super::transcript_file::{self, TranscriptFile, TranscriptMetadata};
use super::{file_identity, now_ms, prefix_window_digest_counted, TranscriptFileCursor};

mod open;

/// A record's text, or `None` when its bytes are not valid UTF-8.
fn decode_record(raw: &[u8]) -> Option<&str> {
    std::str::from_utf8(raw).ok()
}

/// What one capped record read found.
enum CappedRead {
    /// The reader was already at the end.
    End,
    /// A record of this many bytes, in `raw`: newline-terminated, or the
    /// file's unterminated tail.
    Record(usize),
    /// A record past [`MAX_RECORD_BYTES`]. Its first `MAX_RECORD_BYTES` bytes
    /// are consumed and dropped; the rest is still ahead of the reader.
    Oversized,
}

/// Read one record into `raw`, never holding more than [`MAX_RECORD_BYTES`]
/// plus its newline. The one boundary rule every record walk shares.
fn read_capped_record(reader: &mut impl BufRead, raw: &mut Vec<u8>) -> Result<CappedRead> {
    // The cap is on the reader, not on a check around it: `read_until`
    // extends `raw` until it finds a newline or reaches EOF, so a budget
    // consulted afterwards can only observe an allocation that already
    // happened.
    let mut read = reader
        .by_ref()
        .take(MAX_RECORD_BYTES)
        .read_until(b'\n', raw)?;
    if read == 0 {
        return Ok(CappedRead::End);
    }
    // Stopping at the ceiling is not the same claim as passing it: the
    // limited read stops there whether the record ends at the ceiling or
    // runs past it. One byte tells them apart, and a record that ends
    // exactly on the ceiling is an ordinary record — refusing it would drop
    // a complete line on every pass and report it as corruption.
    if raw.last() != Some(&b'\n') && read as u64 == MAX_RECORD_BYTES {
        match reader.fill_buf()?.first().copied() {
            // The file ends here: an unterminated tail at the ceiling.
            None => {}
            // Terminated, exactly on the ceiling.
            Some(b'\n') => {
                reader.consume(1);
                raw.push(b'\n');
                read += 1;
            }
            Some(_) => {
                *raw = Vec::new();
                return Ok(CappedRead::Oversized);
            }
        }
    }
    Ok(CappedRead::Record(read))
}

/// Consume the rest of an [`CappedRead::Oversized`] record through its
/// newline, without holding it. Returns the bytes consumed and whether the
/// newline was found before the reader ran out.
fn skip_oversized_tail(reader: &mut impl BufRead) -> Result<(u64, bool)> {
    let mut skipped = 0u64;
    loop {
        super::super::check_capture_cancelled()?;
        let buffered = reader.fill_buf()?;
        if buffered.is_empty() {
            return Ok((skipped, false));
        }
        match buffered.iter().position(|byte| *byte == b'\n') {
            Some(index) => {
                reader.consume(index + 1);
                return Ok((skipped + index as u64 + 1, true));
            }
            None => {
                let len = buffered.len();
                reader.consume(len);
                skipped += len as u64;
            }
        }
    }
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

enum Source {
    /// A streaming read. `file` is the generation the pass opened, which
    /// replay and the commit re-read consult even after the path is replaced.
    File {
        file: TranscriptFile,
        reader: BufReader<TranscriptFile>,
    },
    /// The bytes of one open, walked again without touching the disk.
    Memory {
        bytes: Arc<[u8]>,
        cursor: std::io::Cursor<Arc<[u8]>>,
    },
}

/// A transcript opened at its cursor.
pub(crate) struct TranscriptReader {
    path: PathBuf,
    source: Source,
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
    /// The bytes are stored zstd-compressed; see [`TranscriptFileCursor::compressed`].
    compressed: bool,
    /// The size and mtime observed when the file was opened, which is the
    /// instant [`Self::unchanged_since_ms`] is stamped from. `commit` stats
    /// the file again, and a stat that moved in between means the clock is
    /// measuring a different file state than the one being stored.
    opened_size: u64,
    opened_mtime_ns: u64,
    /// Change time at open. A rewrite that restores mtime still moves this on
    /// filesystems that keep one; when the platform has none, commit re-reads.
    opened_ctime_ns: Option<i64>,
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
        if let Some(reader) = open::open_captured(path, saved, rewind_to)? {
            return Ok(reader);
        }
        open::open_streaming(path, saved, rewind_to)
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
        super::super::check_capture_cancelled()?;
        line.clear();
        let mut raw = Vec::new();
        let read = match self.read_capped(&mut raw)? {
            CappedRead::End => return Ok(None),
            CappedRead::Record(read) => read,
            // Genuinely over. Get past it without ever holding it: walk to
            // the newline in fixed-size chunks.
            CappedRead::Oversized => {
                let terminated = self.drain_oversized_record()?;
                return Ok(Some(ReadRecord::Oversized { terminated }));
            }
        };
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
            super::super::check_capture_cancelled()?;
            let read = self.read_chunk(&mut chunk)?;
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
                self.seek_to(resume)?;
                return Ok(true);
            }
            drained += read as u64;
        }
    }

    /// Hand each decodable, newline-terminated record in `[from, to)` — a
    /// span this pass has already walked — to `each` again, without its line
    /// ending, and return the bytes read.
    ///
    /// Read through this reader's own open file, never by reopening the
    /// path: a transcript atomically replaced since the pass opened it would
    /// otherwise replay the new generation's records into rows the old
    /// generation's cursor then authenticates. Records are bounded by the
    /// same rule as [`Self::next_line`], and the shared file position is put
    /// back afterwards, so the walk continues where it was.
    pub(crate) fn replay(
        &mut self,
        from: u64,
        to: u64,
        mut each: impl FnMut(&str) -> Result<()>,
    ) -> Result<u64> {
        match &mut self.source {
            Source::Memory { bytes, .. } => {
                let start = usize::try_from(from).unwrap_or(usize::MAX).min(bytes.len());
                let end = usize::try_from(to).unwrap_or(usize::MAX).min(bytes.len());
                let mut span = std::io::Cursor::new(&bytes[start..end]);
                open::replay_reader(&mut span, &mut each)
            }
            Source::File { file, reader } => {
                let resume = reader.get_mut().stream_position()?;
                let replayed = open::replay_file(file, from, to, &mut each);
                reader.get_mut().seek(SeekFrom::Start(resume))?;
                replayed
            }
        }
    }

    /// The cursor to store for a pass that committed through `offset`.
    ///
    /// `offset` may be behind [`Self::position`]: the Claude reader commits
    /// before the earliest message still being written so the next pass reads
    /// it again, and the Codex reader commits at the last `task_complete`.
    ///
    /// A stable file hashes the bytes already in hand. A rewrite during the
    /// walk — size, mtime, or change time — is read again, and a platform
    /// with no change time is read again too, because restoring mtime is then
    /// invisible. The streaming file is re-read through the handle this pass
    /// opened, so a replacement at the path is not what the cursor authenticates.
    pub(crate) fn commit(&mut self, offset: u64) -> Result<CommitOutcome> {
        #[cfg(test)]
        super::run_before_commit_hook(&self.path);
        let metadata = self.commit_metadata()?;
        let size = metadata.len();
        let mtime_ns = super::super::metadata_mtime_ns(&metadata);
        let ctime_ns = super::change_time_ns(&metadata);
        // A stat that moved during the pass restarts the quiescence clock as
        // well: stamping it at `open` while storing a stat taken here made the
        // window cover the walk, and a full re-parse of a large live
        // transcript outlasts the window on its own.
        let stat_moved = open::stamp_moved(
            size,
            mtime_ns,
            ctime_ns,
            self.opened_size,
            self.opened_mtime_ns,
            self.opened_ctime_ns,
        );
        // Re-hash when the stat moved, when change time is missing, or when
        // the filesystem cannot prove a rewrite left the stamp alone. The
        // quiet-since clock follows `stat_moved` only, so a matching digest
        // is not a fresh change.
        let rehash = open::generation_moved(
            size,
            mtime_ns,
            ctime_ns,
            self.opened_size,
            self.opened_mtime_ns,
            self.opened_ctime_ns,
        ) || !super::filesystem_keeps_change_time(&self.path, &metadata);
        let (device, inode) = file_identity(&metadata);
        let identity_changed = metadata.compressed() != self.compressed
            || self.device.is_some()
                && device.is_some()
                && (self.device, self.inode) != (device, inode);
        if identity_changed || size < offset || size < self.opened_size {
            return Ok(CommitOutcome::Superseded);
        }
        let window = self.commit_window(rehash)?;
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
        let prefix_hash = self.commit_prefix(offset, &window, rehash)?;
        Ok(CommitOutcome::Published(TranscriptFileCursor {
            offset,
            device: self.device,
            inode: self.inode,
            mtime_ns,
            size,
            unchanged_since_ms,
            prefix_hash,
            compressed: self.compressed,
        }))
    }

    /// The generation this pass opened. A streaming read stats its own handle,
    /// which still names the inode from `open` after the path is replaced.
    fn commit_metadata(&self) -> Result<TranscriptMetadata> {
        match &self.source {
            Source::File { file, .. } => Ok(file.metadata()?),
            Source::Memory { .. } => Ok(transcript_file::metadata(&self.path)?),
        }
    }

    /// The opened window, from the bytes already hashed unless this pass re-reads.
    fn commit_window(&mut self, rehash: bool) -> Result<String> {
        if !rehash {
            return Ok(self.opened_window.clone());
        }
        self.hash_region(self.opened_size)
    }

    /// Digest of `[0, offset)`. The opened window when that is the same region.
    fn commit_prefix(&mut self, offset: u64, window: &str, rehash: bool) -> Result<String> {
        if offset == self.opened_size {
            return Ok(window.to_string());
        }
        if !rehash {
            if let Source::Memory { bytes, .. } = &self.source {
                if offset <= bytes.len() as u64 {
                    return super::prefix_window_digest_of(bytes, offset);
                }
            }
        }
        self.hash_region(offset)
    }

    /// Hash `offset` bytes from the open handle, or from the path when this
    /// pass is walking a retained copy and the file has moved.
    fn hash_region(&mut self, offset: u64) -> Result<String> {
        let path = self.path.clone();
        let hashed = match &mut self.source {
            Source::File { file, .. } => prefix_window_digest_counted(file, offset),
            Source::Memory { .. } => {
                let mut file = super::super::cold_read::open_file(&path)?;
                prefix_window_digest_counted(&mut file, offset)
            }
        };
        let (digest, read) = hashed?;
        self.validation_bytes += read;
        Ok(digest)
    }

    fn read_capped(&mut self, raw: &mut Vec<u8>) -> Result<CappedRead> {
        match &mut self.source {
            Source::File { reader, .. } => read_capped_record(reader, raw),
            Source::Memory { cursor, .. } => read_capped_record(cursor, raw),
        }
    }

    fn read_chunk(&mut self, buf: &mut [u8]) -> Result<usize> {
        match &mut self.source {
            Source::File { reader, .. } => Ok(reader.read(buf)?),
            Source::Memory { cursor, .. } => Ok(cursor.read(buf)?),
        }
    }

    fn seek_to(&mut self, position: u64) -> Result<()> {
        match &mut self.source {
            Source::File { reader, .. } => {
                reader.seek(SeekFrom::Start(position))?;
            }
            Source::Memory { cursor, .. } => {
                cursor.seek(SeekFrom::Start(position))?;
            }
        }
        Ok(())
    }

    /// A clone of the handle `next_line` reads, so a test can watch its offset.
    #[cfg(test)]
    pub(crate) fn probe_file(&self) -> TranscriptFile {
        match &self.source {
            Source::File { reader, .. } => {
                reader.get_ref().try_clone().expect("streaming file clones")
            }
            Source::Memory { .. } => panic!("a captured transcript has no streaming file"),
        }
    }

    /// Where the handle `next_line` reads has actually reached.
    #[cfg(test)]
    pub(crate) fn streamed_to(&mut self) -> u64 {
        match &mut self.source {
            Source::File { reader, .. } => {
                reader.get_mut().stream_position().expect("stream position")
            }
            Source::Memory { cursor, .. } => cursor.stream_position().expect("stream position"),
        }
    }
}
