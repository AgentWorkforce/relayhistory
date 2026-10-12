//! In-memory prefix digest. The bytes are already in hand, so this is not a provider read.

use anyhow::Result;
use sha2::{Digest, Sha256};

use super::PREFIX_WINDOW_BYTES;

/// The digest [`super::prefix_window_digest_counted`] would compute, from bytes the
/// caller already holds. Not a provider read, so it does not move the
/// validation meter.
pub(crate) fn prefix_window_digest_of(bytes: &[u8], offset: u64) -> Result<String> {
    anyhow::ensure!(
        offset <= bytes.len() as u64,
        "transcript cursor window extends past end of file"
    );
    let mut hasher = Sha256::new();
    hasher.update(b"relayhistory/transcript-prefix/v2\0");
    hasher.update(offset.to_le_bytes());
    if offset == 0 {
        return Ok(format!("{:x}", hasher.finalize()));
    }
    match prefix_window_spans(offset) {
        PrefixSpans::Whole { len } => hasher.update(&bytes[..len as usize]),
        PrefixSpans::Ends { window, tail_start } => {
            let window = window as usize;
            let tail_start = tail_start as usize;
            hasher.update(&bytes[..window]);
            hasher.update(&bytes[tail_start..tail_start + window]);
        }
    }
    Ok(format!("{:x}", hasher.finalize()))
}

/// Which regions of the committed prefix a digest covers.
pub(super) enum PrefixSpans {
    /// The whole committed prefix, as one read.
    Whole { len: u64 },
    /// Two disjoint windows, one at each end.
    Ends { window: u64, tail_start: u64 },
}

/// The regions [`super::prefix_window_digest_counted`] hashes for a cursor at
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
pub(super) fn prefix_window_spans(offset: u64) -> PrefixSpans {
    let window = PREFIX_WINDOW_BYTES.min(offset);
    let tail_start = offset - window;
    if tail_start <= window {
        PrefixSpans::Whole { len: offset }
    } else {
        PrefixSpans::Ends { window, tail_start }
    }
}
