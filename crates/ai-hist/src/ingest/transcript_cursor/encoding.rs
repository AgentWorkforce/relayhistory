//! What a cursor's stored-file facts mean when the transcript is stored
//! compressed: identity compared within one encoding, a change of encoding
//! treated as no change to the records, and a bounded proof for a compressed
//! copy taken from its stored bytes.

use super::super::transcript_file::{self, TranscriptMetadata};
use super::{
    file_identity, prefix_window_bytes, prefix_window_digest_counted, PrefixCheck,
    TranscriptFileCursor,
};
use anyhow::Result;
use std::fs;
use std::path::Path;

impl TranscriptFileCursor {
    /// Whether a transcript whose stamp moved since this cursor can only have
    /// gained records, so a pass may resume from it.
    ///
    /// The reader's own validation hashes a bounded window at each end of the
    /// committed prefix, so an in-place edit strictly between those windows
    /// that keeps the file's length would pass it. An append-only writer
    /// always grows the file; an in-place edit that keeps the length never
    /// does. What is still assumed: an in-place edit inside that gap *and* an
    /// append in the same interval between two passes. Codex appends to its
    /// rollouts and never rewrites them, which is the same trade the Claude
    /// walk's cursor already makes.
    ///
    /// Compressing a transcript, or writing it back out, stamps it anew
    /// without touching its records, so a change of encoding over records at
    /// least as long as the cursor's resumes too; the windows confirm them.
    pub(crate) fn only_appended_to(&self, now: &TranscriptMetadata) -> bool {
        now.len() > self.size || (now.compressed() != self.compressed && now.len() >= self.size)
    }

    /// The file this cursor was taken over has been replaced. Across a change
    /// of encoding the identity says nothing, and the content windows decide.
    pub(crate) fn identity_moved(&self, now: &TranscriptMetadata) -> bool {
        let (device, inode) = file_identity(now);
        self.compressed == now.compressed()
            && self.device.is_some()
            && device.is_some()
            && (self.device, self.inode) != (device, inode)
    }
}

/// The window over a compressed transcript's stored bytes, and what reading
/// it cost. See [`TranscriptFileCursor::stored_hash`].
pub(crate) fn stored_window(file: &fs::File, stored_len: u64) -> Result<(String, u64)> {
    let mut handle = file;
    prefix_window_digest_counted(&mut handle, stored_len)
}

/// [`committed_prefix_matches`] for a cursor taken over the compressed copy
/// that is still on disk: its stored window, not its records. `None` when the
/// cursor carries no stored window or the encoding changed since, and the
/// records' own windows decide.
pub(super) fn stored_window_matches(
    cursor: &TranscriptFileCursor,
    path: &Path,
    metadata: &TranscriptMetadata,
) -> Option<PrefixCheck> {
    let stored_hash = cursor.stored_hash.as_deref()?;
    if !(cursor.compressed && metadata.compressed()) {
        return None;
    }
    let file = fs::File::open(transcript_file::stored_path(path).ok()?).ok()?;
    Some(PrefixCheck {
        valid: stored_window(&file, metadata.stored_len())
            .is_ok_and(|(digest, _)| digest == stored_hash),
        bytes_read: prefix_window_bytes(metadata.stored_len()),
    })
}
