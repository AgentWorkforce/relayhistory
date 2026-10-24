//! Native unchanged-file proof, optionally recording its settled cache.
use super::*;

pub(crate) fn transcript_unchanged(conn: &Connection, source: &str, path: &Path) -> Result<bool> {
    transcript_unchanged_with_settle(conn, source, path, true)
}

/// The same native proof without writing the optional settled-stat cache.
pub(crate) fn transcript_unchanged_readonly(
    conn: &Connection,
    source: &str,
    path: &Path,
) -> Result<bool> {
    transcript_unchanged_with_settle(conn, source, path, false)
}

fn transcript_unchanged_with_settle(
    conn: &Connection,
    source: &str,
    path: &Path,
    settle: bool,
) -> Result<bool> {
    let locator = path.to_string_lossy().to_string();
    let (cursor, raw) = load_cursor_with_document(
        conn,
        &CursorKey::Locator {
            source,
            locator: &locator,
        },
    )?;
    let Some(file) = cursor.file.as_ref() else {
        return Ok(false);
    };
    let Ok(metadata) = transcript_file::metadata(path) else {
        return Ok(false);
    };
    if file.offset != metadata.len()
        || file.mtime_ns != super::metadata_mtime_ns(&metadata)
        || file.compressed != metadata.compressed()
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
    // A Claude transcript carries the record walk's position and the
    // metadata fold's in one document. Both must be current; identical proof
    // inputs can share one digest within this call, otherwise each is checked.
    //
    // Both positions in the document have to be current, not just the record
    // walk's: a superseded metadata scan leaves a fold that has to be made
    // again, and skipping on the record cursor alone would leave it stale for
    // as long as nothing else about the file changed.
    if settled_proves_unchanged(&cursor, file, path, &metadata) {
        return Ok(true);
    }
    let valid = committed_prefix_matches(file, path).valid
        && (metadata_shares_record_prefix(&cursor, file)
            || scan_position_current(&cursor, path).valid);
    if valid && settle {
        if let Some(ctime_ns) = settled_change_time(path, &metadata) {
            // Proven by the digests just taken, over a stat taken before them
            // and old enough to be past the racy window: from here on this
            // file is skipped on its stat alone until something touches it.
            let mut settled = cursor.clone();
            settled.settled = Some(SettledStat {
                ctime_ns,
                settled_at_ms: now_wall_ms(),
                prefix_hash: file.prefix_hash.clone(),
                scan_prefix_hash: scan_prefix_hash(&cursor),
            });
            // An optimisation, so it may not cost the source: a stamp that
            // cannot be written leaves the file on the digest path. And a
            // compare-and-swap on the document read above, so a cursor
            // another writer (hydration, a concurrent pass) advanced since
            // is left alone rather than rolled back to what this pass saw.
            if let Err(error) =
                store_settled_cursor(conn, source, &locator, raw.as_deref(), &settled)
            {
                if !super::SYNC_QUIET.load(std::sync::atomic::Ordering::Relaxed) {
                    eprintln!("  [sync] could not record {locator} as settled: {error:#}");
                }
            }
        }
    }
    Ok(valid)
}

/// Only the quiescence age is bookkeeping: `committed_prefix_matches` does
/// not read it. Compare every other field, including any added in the future,
/// without changing either walk's own quiescence state. This shares one
/// point-in-time byte proof, never a cached answer across calls.
fn metadata_shares_record_prefix(
    cursor: &TranscriptCursorState,
    record: &TranscriptFileCursor,
) -> bool {
    let Some(metadata) = cursor
        .claude
        .as_ref()
        .and_then(|c| c.scan.as_ref())
        .and_then(|scan| scan.file.as_ref())
    else {
        return false;
    };
    let mut normalized = metadata.clone();
    normalized.unchanged_since_ms = record.unchanged_since_ms;
    normalized == *record
}

#[cfg(test)]
mod tests;
