//! The sweep's pass over a legacy OpenCode `storage/` JSON tree.

use super::super::{check_capture_cancelled, opencode};
use super::{OpencodeSweep, Stamps};
use anyhow::Result;
use rusqlite::Connection;
use std::collections::BTreeSet;
use std::path::Path;

/// Index every changed OpenCode session in a legacy `storage/` JSON tree.
///
/// One session's failure is that session's failure: each is indexed or
/// reported on its own, and the error at the end names them all.
pub(crate) fn sync_opencode_storage_dir(
    conn: &Connection,
    storage_dir: &Path,
    sweep: &mut OpencodeSweep<'_>,
) -> Result<usize> {
    check_capture_cancelled()?;
    if !storage_dir.join("session").is_dir() {
        return Ok(0);
    }
    let mut inserted = 0;
    let listing = opencode::list_json_tree_session_files(storage_dir);
    // A subtree that could not be walked is not a subtree with no sessions in
    // it: it joins the failures, so the sessions under it are reported
    // missing instead of silently absent.
    let mut failures: Vec<String> = listing
        .unreadable
        .iter()
        .map(|dir| format!("{}: {}", dir.path.display(), dir.error))
        .collect();
    let root = storage_dir.join("session").to_string_lossy().into_owned();
    let mut seen = BTreeSet::new();
    for session_file in super::super::capture_files("opencode", listing.sessions) {
        check_capture_cancelled()?;
        let entry = session_file.to_string_lossy().into_owned();
        seen.insert(entry.clone());
        let session_id = session_file
            .file_stem()
            .map(|stem| stem.to_string_lossy().into_owned())
            .unwrap_or_default();
        // Taken before the read, so a write after it moves the stamp.
        let token = opencode::stamp_json_tree_session(&session_file, &session_id)
            .ok()
            .map(|stamp| sweep.session_stamp(&stamp.token()));
        if let Some(token) = &token {
            if sweep.session_current(conn, Stamps::Tree, &entry, &session_id, token)? {
                continue;
            }
        }
        #[cfg(test)]
        super::tests::note_read(0, 1);
        let indexed =
            opencode::load_from_json_tree(&session_file).and_then(|loaded| match loaded {
                Some(loaded) => opencode::normalize(conn, &loaded, &entry)
                    .map(|counts| (counts.prompts, Some(loaded.session.id))),
                None => Ok((0, None)),
            });
        match indexed {
            Ok((prompts, read_id)) => {
                inserted += prompts;
                // Under the id the file holds, which is what its evidence is
                // keyed by; a skip asks about the file name's.
                match (token, read_id) {
                    (Some(token), Some(read_id)) if read_id == session_id => {
                        sweep.record(conn, Stamps::Tree, entry, &read_id, &token)?;
                    }
                    _ => sweep.forget(Stamps::Tree, entry),
                }
            }
            Err(error) => {
                failures.push(format!("{}: {error:#}", session_file.display()));
                sweep.forget(Stamps::Tree, entry);
            }
        }
    }
    let gone: Vec<String> = sweep
        .map(Stamps::Tree)
        .keys()
        .filter(|entry| entry.starts_with(&root) && !seen.contains(*entry))
        .cloned()
        .collect();
    for entry in gone {
        sweep.forget(Stamps::Tree, entry);
    }
    if !failures.is_empty() {
        anyhow::bail!(
            "{} OpenCode path(s) under {} could not be read (the rest were indexed): {}",
            failures.len(),
            storage_dir.display(),
            failures.join("; ")
        );
    }
    Ok(inserted)
}
