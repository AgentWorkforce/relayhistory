use anyhow::Result;
use std::fs;
use std::io::{BufRead, BufReader, Read, Seek};
use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;

use super::{file_identity, now_ms, prefix_window_digest_counted, TranscriptFileCursor};

/// Hash the bounded validation window for `[0, offset)`. See the module docs.
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

/// A transcript opened at its cursor.
pub(crate) struct TranscriptReader {
    /// Only so a test hook can tell one transcript's commit from another's; a
    /// hydration commits a parent, its sidecars and their metadata in turn.
    #[cfg(test)]
    path: PathBuf,
    file: fs::File,
    pub(super) reader: BufReader<fs::File>,
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
        let mtime_ns = super::super::metadata_mtime_ns(&metadata);
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
        super::super::check_capture_cancelled()?;
        line.clear();
        let mut raw = Vec::new();
        let read = match read_capped_record(&mut self.reader, &mut raw)? {
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
        let resume = self.reader.get_mut().stream_position()?;
        let replayed = (|| -> Result<u64> {
            let mut file = &self.file;
            file.seek(std::io::SeekFrom::Start(from))?;
            let mut span = BufReader::new(file.take(to.saturating_sub(from)));
            let mut read_total = 0u64;
            let mut raw = Vec::new();
            loop {
                super::super::check_capture_cancelled()?;
                raw.clear();
                match read_capped_record(&mut span, &mut raw)? {
                    CappedRead::End => return Ok(read_total),
                    CappedRead::Record(read) => {
                        read_total += read as u64;
                        if raw.last() == Some(&b'\n') {
                            if let Some(text) = decode_record(&raw) {
                                each(text.trim_end_matches(['\n', '\r']))?;
                            }
                        }
                    }
                    CappedRead::Oversized => {
                        read_total += MAX_RECORD_BYTES;
                        let (skipped, terminated) = skip_oversized_tail(&mut span)?;
                        read_total += skipped;
                        if !terminated {
                            return Ok(read_total);
                        }
                    }
                }
            }
        })();
        self.reader
            .get_mut()
            .seek(std::io::SeekFrom::Start(resume))?;
        replayed
    }

    /// The cursor to store for a pass that committed through `offset`.
    ///
    /// `offset` may be behind [`Self::position`]: the Claude reader commits
    /// before the earliest message still being written so the next pass reads
    /// it again, and the Codex reader commits at the last `task_complete`.
    pub(crate) fn commit(&mut self, offset: u64) -> Result<CommitOutcome> {
        #[cfg(test)]
        super::run_before_commit_hook(&self.path);
        let metadata = self.file.metadata()?;
        let size = metadata.len();
        let mtime_ns = super::super::metadata_mtime_ns(&metadata);
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
