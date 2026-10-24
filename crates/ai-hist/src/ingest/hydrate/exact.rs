//! Existing session cursor/stat proofs for unchanged exact cold inputs.
use super::*;
use crate::ingest::transcript_cursor::{
    change_time_ns, file_identity, settle_is_fresh, settled_change_time, settled_proves_unchanged,
    SettledStat,
};

fn observation(source: &str, session: &str) -> ObservationKey {
    ObservationKey {
        source: source.into(),
        session_id: session.into(),
        location: SessionLocation::Local,
        connector_id: source.into(),
        connector_instance: "default".into(),
    }
}

fn cursor_key<'a>(source: &'a str, session_id: &'a str) -> CursorKey<'a> {
    CursorKey::Session {
        source,
        session_id,
        location: "local",
    }
}

/// A metadata stat is trusted only with the current parser checkpoint and a
/// previously validated, unchanged ctime on an allowlisted filesystem.
pub(crate) fn can_skip(
    conn: &Connection,
    source: &str,
    session: &str,
    path: &Path,
) -> Result<bool> {
    let checkpoint = observations::checkpoint(conn, &observation(source, session))?;
    if !checkpoint_covers(checkpoint.as_ref(), &file_stamp(path)?, false) {
        return Ok(false);
    }
    let cursor = load_cursor(conn, &cursor_key(source, session))?;
    if cursor
        .claude
        .as_ref()
        .is_some_and(|state| !state.in_progress.is_empty())
    {
        return Ok(false);
    }
    let Some(file) = cursor.file.as_ref() else {
        return Ok(false);
    };
    let metadata = super::super::transcript_file::metadata(path)?;
    Ok(identity_matches(file, &metadata)
        && file.offset == metadata.len()
        && settled_proves_unchanged(&cursor, file, path, &metadata))
}

/// Trusted ctime changed (or has not yet been proven): a cold retry must
/// validate uncommitted records too, even if a writer restored stat values.
pub(crate) fn needs_validation(
    conn: &Connection,
    source: &str,
    session: &str,
    path: &Path,
) -> Result<bool> {
    let cursor = load_cursor(conn, &cursor_key(source, session))?;
    if cursor.claude.as_ref().is_some_and(|claude| {
        claude
            .scan
            .as_ref()
            .and_then(|scan| scan.file.as_ref())
            .is_none()
    }) {
        return Ok(true);
    }
    let metadata = super::super::transcript_file::metadata(path)?;
    if !super::super::transcript_cursor::filesystem_keeps_change_time(path, &metadata) {
        return Ok(false);
    }
    Ok(cursor.file.is_some()
        && cursor.settled.as_ref().is_none_or(|settled| {
            change_time_ns(&metadata) != Some(settled.ctime_ns)
                || !settle_is_fresh(settled.settled_at_ms)
        }))
}

fn identity_matches(
    file: &super::super::transcript_cursor::TranscriptFileCursor,
    metadata: &super::super::transcript_file::TranscriptMetadata,
) -> bool {
    file.size == metadata.len()
        && file.mtime_ns == super::super::metadata_mtime_ns(metadata)
        && (file.device, file.inode) == file_identity(metadata)
        && file.compressed == metadata.compressed()
}

/// The parser already checked these bytes at commit. Preserve its existing
/// SettledStat shape; recent/racy or unsupported filesystems keep digest reads.
pub(crate) fn mark(
    conn: &Connection,
    source: &str,
    session: &str,
    path: &Path,
    observed_change_time: Option<i64>,
) -> Result<()> {
    let key = cursor_key(source, session);
    let (mut cursor, expected) =
        super::super::transcript_cursor::load_cursor_with_document(conn, &key)?;
    let Some(file) = cursor.file.as_ref() else {
        return Ok(());
    };
    let metadata = super::super::transcript_file::metadata(path)?;
    let Some(ctime_ns) = settled_change_time(path, &metadata) else {
        return Ok(());
    };
    if observed_change_time != Some(ctime_ns) {
        return Ok(());
    }

    if !identity_matches(file, &metadata) || file.offset != metadata.len() {
        return Ok(());
    }
    if let Some(claude) = &cursor.claude {
        let Some(scan) = claude.scan.as_ref().and_then(|scan| scan.file.as_ref()) else {
            return Ok(());
        };
        if !identity_matches(scan, &metadata) || scan.offset != metadata.len() {
            return Ok(());
        }
    }
    cursor.settled = Some(SettledStat {
        ctime_ns,
        settled_at_ms: crate::relationship_capture::now_ms(),
        prefix_hash: file.prefix_hash.clone(),
        scan_prefix_hash: cursor
            .claude
            .as_ref()
            .and_then(|state| state.scan.as_ref())
            .and_then(|scan| scan.file.as_ref())
            .map(|file| file.prefix_hash.clone()),
    });
    super::super::transcript_cursor::settle_session_if_unchanged(
        conn,
        source,
        session,
        expected.as_deref(),
        &cursor,
    )
}
