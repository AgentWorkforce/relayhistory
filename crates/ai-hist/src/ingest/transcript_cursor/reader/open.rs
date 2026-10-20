//! Opening a transcript from bytes already read, and replaying that generation.

use anyhow::Result;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

use super::super::super::transcript_file::{self, TranscriptFile, TranscriptMetadata};
use super::super::super::{check_capture_cancelled, cold_read, metadata_mtime_ns};
use super::super::{
    change_time_ns, file_identity, now_ms, prefix_window_digest_counted, prefix_window_digest_of,
    TranscriptFileCursor,
};
use super::{
    decode_record, read_capped_record, skip_oversized_tail, CappedRead, Source, TranscriptReader,
    MAX_RECORD_BYTES, QUIESCENT_GRACE_MS,
};

pub(super) fn open_streaming(
    path: &Path,
    saved: Option<&TranscriptFileCursor>,
    rewind_to: Option<u64>,
) -> Result<TranscriptReader> {
    let mut file = cold_read::open_file(path)?;
    let metadata = file.metadata()?;
    let size = metadata.len();
    let mtime_ns = metadata_mtime_ns(&metadata);
    let opened_ctime_ns = change_time_ns(&metadata);
    let (device, inode) = file_identity(&metadata);
    let compressed = metadata.compressed();
    // A cursor at offset 0 is still a cursor. It records the file's
    // identity, size and mtime, and those are what say whether anything
    // has been appended since the last pass. Reading it as "no cursor"
    // because its offset happened to be zero meant a pass that held back
    // the file's *first* record could never see the file go quiet, so it
    // deferred the same records forever and the message was never
    // indexed — while `holding_records` kept forcing the pass to run.
    //
    // Checked before the digest, not after. Hashing a window that extends
    // past the end reads every byte it can and then errors, and those
    // bytes left with the error rather than reaching `validation_bytes`.
    // A `stat` answers a short file without reading anything.
    let mut validation_bytes = 0u64;
    let saved_window = match saved {
        Some(saved) if size >= saved.offset => {
            match prefix_window_digest_counted(&mut file, saved.offset) {
                Ok((digest, read)) => {
                    validation_bytes += read;
                    Some(digest)
                }
                Err(_) => None,
            }
        }
        _ => None,
    };
    let resume = saved.map(|saved| saved_resume(&metadata, saved, rewind_to, saved_window));
    let offset = resume.as_ref().map(|resume| resume.offset).unwrap_or(0);
    // The saved cursor covered the whole file, so that digest is the
    // opened window. Reusing it keeps the resume to one bounded read.
    let opened_window = match resume
        .as_ref()
        .and_then(|resume| resume.covered_window.clone())
    {
        Some(window) => window,
        None => {
            let (digest, read) = prefix_window_digest_counted(&mut file, size)?;
            validation_bytes += read;
            digest
        }
    };
    let mut handle = file.try_clone()?;
    handle.seek(SeekFrom::Start(offset))?;
    Ok(TranscriptReader {
        path: path.to_path_buf(),
        source: Source::File {
            file,
            reader: BufReader::new(handle),
        },
        position: offset,
        start_offset: offset,
        tail_bytes: 0,
        decoded: 0,
        undecodable: 0,
        validation_bytes,
        opened_size: size,
        opened_mtime_ns: mtime_ns,
        opened_ctime_ns,
        opened_window,
        unchanged_since_ms: resume
            .as_ref()
            .map(|resume| resume.unchanged_since_ms)
            .unwrap_or_else(now_ms),
        quiesced: resume.as_ref().is_some_and(|resume| resume.quiesced),
        device,
        inode,
        compressed,
        rotated: resume.as_ref().is_some_and(|resume| resume.rotated),
    })
}

/// Where a saved cursor lets this pass start, and the digest of the region it
/// already hashed when that region is the whole file.
struct SavedResume {
    offset: u64,
    rotated: bool,
    quiesced: bool,
    unchanged_since_ms: i64,
    covered_window: Option<String>,
}

fn saved_resume(
    metadata: &TranscriptMetadata,
    saved: &TranscriptFileCursor,
    rewind_to: Option<u64>,
    saved_window: Option<String>,
) -> SavedResume {
    let size = metadata.len();
    let mtime_ns = metadata_mtime_ns(metadata);
    // A transcript compressed since the cursor was taken is a new file
    // holding the same records: the windows below are what can tell.
    let valid = !saved.identity_moved(metadata)
        && size >= saved.offset
        && mtime_ns >= saved.mtime_ns
        && saved_window.as_deref() == Some(saved.prefix_hash.as_str());
    if !valid {
        // Nothing to rotate away from at offset zero: the cursor claimed no
        // committed work, so re-reading is not a correction and reporting one
        // would be noise.
        return SavedResume {
            offset: 0,
            rotated: saved.offset > 0,
            quiesced: false,
            unchanged_since_ms: now_ms(),
            covered_window: None,
        };
    }
    let stat_unchanged = size == saved.size && mtime_ns == saved.mtime_ns;
    // Carry the stamp forward while nothing moves; restart it the moment
    // anything does.
    let unchanged_since_ms = if stat_unchanged && saved.unchanged_since_ms > 0 {
        saved.unchanged_since_ms
    } else {
        now_ms()
    };
    SavedResume {
        offset: rewind_to
            .filter(|rewind| *rewind <= saved.offset)
            .unwrap_or(saved.offset),
        rotated: false,
        quiesced: stat_unchanged
            && now_ms().saturating_sub(unchanged_since_ms) >= QUIESCENT_GRACE_MS,
        unchanged_since_ms,
        covered_window: (saved.offset == size).then_some(saved_window).flatten(),
    }
}

/// Size, mtime, or a present change time moved. A missing change time does
/// not restart the quiet-since clock; [`generation_moved`] still re-reads.
pub(super) fn stamp_moved(
    size: u64,
    mtime_ns: u64,
    ctime_ns: Option<i64>,
    opened_size: u64,
    opened_mtime_ns: u64,
    opened_ctime_ns: Option<i64>,
) -> bool {
    if size != opened_size || mtime_ns != opened_mtime_ns {
        return true;
    }
    match (opened_ctime_ns, ctime_ns) {
        (Some(opened), Some(now)) => opened != now,
        _ => false,
    }
}

/// Size, mtime, or change time moved after the bytes were taken. A missing
/// change time cannot show a rewrite that restored mtime, so it counts as
/// moved and the commit reads the file again.
pub(super) fn generation_moved(
    size: u64,
    mtime_ns: u64,
    ctime_ns: Option<i64>,
    opened_size: u64,
    opened_mtime_ns: u64,
    opened_ctime_ns: Option<i64>,
) -> bool {
    if size != opened_size || mtime_ns != opened_mtime_ns {
        return true;
    }
    match (opened_ctime_ns, ctime_ns) {
        (Some(opened), Some(now)) => opened != now,
        _ => true,
    }
}

/// A file small enough to keep, opened from bytes already read when the stat
/// still matches. A resume past the start, or a file over the capture cap,
/// streams instead: an incremental append must not pull the prefix back in.
pub(super) fn open_captured(
    path: &Path,
    saved: Option<&TranscriptFileCursor>,
    rewind_to: Option<u64>,
) -> Result<Option<TranscriptReader>> {
    if let Some(bytes) = cold_read::fresh(path) {
        if let Some(reader) = reader_from_bytes(path, bytes, saved, rewind_to)? {
            return Ok(Some(reader));
        }
    }
    if saved.is_some_and(|cursor| cursor.offset > 0) {
        return Ok(None);
    }
    if cold_read::small_file_len(path)?.is_none() {
        return Ok(None);
    }
    let bytes = cold_read::read_all(path)?;
    if cold_read::fresh(path).is_none()
        && change_time_ns(&*transcript_file::metadata(path)?).is_some()
    {
        // The file moved while it was read, and its change time can prove
        // that. A platform with no change time still walks these bytes;
        // commit reads the path again because that time is absent.
        return Ok(None);
    }
    reader_from_bytes(path, bytes, saved, rewind_to)
}

fn reader_from_bytes(
    path: &Path,
    bytes: Arc<[u8]>,
    saved: Option<&TranscriptFileCursor>,
    rewind_to: Option<u64>,
) -> Result<Option<TranscriptReader>> {
    let metadata = transcript_file::metadata(path)?;
    let size = metadata.len();
    if size != bytes.len() as u64 {
        return Ok(None);
    }
    let mtime_ns = metadata_mtime_ns(&metadata);
    let opened_ctime_ns = change_time_ns(&metadata);
    let (device, inode) = file_identity(&metadata);
    let compressed = metadata.compressed();
    let saved_window = match saved {
        Some(saved) if size >= saved.offset => prefix_window_digest_of(&bytes, saved.offset).ok(),
        _ => None,
    };
    let resume = saved.map(|saved| saved_resume(&metadata, saved, rewind_to, saved_window));
    let offset = resume.as_ref().map(|resume| resume.offset).unwrap_or(0);
    let opened_window = match resume
        .as_ref()
        .and_then(|resume| resume.covered_window.clone())
    {
        Some(window) => window,
        None => prefix_window_digest_of(&bytes, size)?,
    };
    let mut cursor = std::io::Cursor::new(Arc::clone(&bytes));
    cursor.seek(SeekFrom::Start(offset))?;
    Ok(Some(TranscriptReader {
        path: path.to_path_buf(),
        source: Source::Memory { bytes, cursor },
        position: offset,
        start_offset: offset,
        tail_bytes: 0,
        decoded: 0,
        undecodable: 0,
        validation_bytes: 0,
        opened_size: size,
        opened_mtime_ns: mtime_ns,
        opened_ctime_ns,
        opened_window,
        unchanged_since_ms: resume
            .as_ref()
            .map(|resume| resume.unchanged_since_ms)
            .unwrap_or_else(now_ms),
        quiesced: resume.as_ref().is_some_and(|resume| resume.quiesced),
        device,
        inode,
        compressed,
        rotated: resume.as_ref().is_some_and(|resume| resume.rotated),
    }))
}

pub(super) fn replay_file(
    file: &mut TranscriptFile,
    from: u64,
    to: u64,
    each: &mut impl FnMut(&str) -> Result<()>,
) -> Result<u64> {
    file.seek(SeekFrom::Start(from))?;
    let mut span = BufReader::new(file.take(to.saturating_sub(from)));
    replay_reader(&mut span, each)
}

pub(super) fn replay_reader(
    span: &mut impl BufRead,
    each: &mut impl FnMut(&str) -> Result<()>,
) -> Result<u64> {
    let mut read_total = 0u64;
    let mut raw = Vec::new();
    loop {
        check_capture_cancelled()?;
        raw.clear();
        match read_capped_record(span, &mut raw)? {
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
                let (skipped, terminated) = skip_oversized_tail(span)?;
                read_total += skipped;
                if !terminated {
                    return Ok(read_total);
                }
            }
        }
    }
}
