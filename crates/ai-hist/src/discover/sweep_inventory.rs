//! One Grok inventory per sweep, shared by everything the sweep runs.
//!
//! A forced sweep used to enumerate the Grok session tree, and stamp every
//! session directory (`grok_source_inventory`: a `stat` per sibling and a
//! listing of two subdirectories), three times: for the source fingerprint
//! taken before the walk, for the walk itself, and for the shallow discovery
//! pass that ends the sweep. On a large store the discovery copy alone was 12%
//! of a forced tick that changed nothing Grok has (#317).
//!
//! While a sweep holds a [`SweepInventory`] guard, the first of them to
//! enumerate the tree records what it found, and the first to stamp a session
//! directory records the stamp; the others reuse both.
//!
//! The first reader is the fingerprint, which is taken before the walk on
//! purpose: anything that changes after it yields a different fingerprint next
//! time and forces another pass. Sharing its stamps keeps that property. A
//! directory that changed after it was stamped is recorded under the older
//! stamp -- in `.sync-state.json` and in the catalog alike -- which no longer
//! matches on the next pass, so that pass reads it again. Reuse can only err
//! toward reading again, never toward skipping.
//!
//! Thread-local, because a sweep runs on one thread and nothing else may see
//! a half-built inventory; outside a guard every lookup misses and every
//! caller enumerates and stamps exactly as before.

use anyhow::Result;
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Default)]
struct Inventory {
    /// The tree that was enumerated, and what it held.
    walk: Option<(PathBuf, Vec<PathBuf>)>,
    /// Stamp and recency hint per transcript.
    stamps: HashMap<PathBuf, (String, Option<i64>)>,
}

thread_local! {
    static INVENTORY: RefCell<Option<Inventory>> = const { RefCell::new(None) };
}

/// Holds the inventory open for one sweep; dropping it forgets everything.
pub(crate) struct SweepInventory {
    _not_send: std::marker::PhantomData<*const ()>,
}

impl SweepInventory {
    pub(crate) fn begin() -> Self {
        INVENTORY.with(|cell| *cell.borrow_mut() = Some(Inventory::default()));
        Self {
            _not_send: std::marker::PhantomData,
        }
    }
}

impl Drop for SweepInventory {
    fn drop(&mut self) {
        INVENTORY.with(|cell| *cell.borrow_mut() = None);
    }
}

fn active() -> bool {
    INVENTORY.with(|cell| cell.borrow().is_some())
}

/// Every Grok `chat_history.jsonl` under `root`: this sweep's enumeration
/// when it already made one of the same tree, else a fresh walk (recorded
/// when a sweep is open).
pub(crate) fn grok_transcripts(root: &Path) -> Result<Vec<PathBuf>> {
    let known = INVENTORY.with(|cell| {
        let inventory = cell.borrow();
        let (walked, files) = inventory.as_ref()?.walk.as_ref()?;
        (walked == root).then(|| files.clone())
    });
    if let Some(files) = known {
        return Ok(files);
    }
    let files = crate::collect_matching_files(root, "chat_history", "jsonl")?;
    INVENTORY.with(|cell| {
        if let Some(inventory) = cell.borrow_mut().as_mut() {
            inventory.walk = Some((root.to_path_buf(), files.clone()));
        }
    });
    Ok(files)
}

/// [`crate::grok_session_stamp_and_modified`], answered from this sweep's
/// inventory when something in the sweep already stamped `chat`, and recorded
/// for the rest of the sweep when not. A failure is not recorded: the next
/// caller tries again.
pub(crate) fn grok_stamp_and_modified(chat: &Path) -> Result<(String, Option<i64>)> {
    let known = INVENTORY.with(|cell| {
        cell.borrow()
            .as_ref()
            .and_then(|inventory| inventory.stamps.get(chat).cloned())
    });
    if let Some(known) = known {
        return Ok(known);
    }
    let fresh = crate::grok_session_stamp_and_modified(chat)?;
    if active() {
        INVENTORY.with(|cell| {
            if let Some(inventory) = cell.borrow_mut().as_mut() {
                inventory.stamps.insert(chat.to_path_buf(), fresh.clone());
            }
        });
    }
    Ok(fresh)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transcript(root: &Path, session: &str) -> PathBuf {
        let chat = root.join(session).join("chat_history.jsonl");
        std::fs::create_dir_all(chat.parent().unwrap()).unwrap();
        std::fs::write(&chat, "{}\n").unwrap();
        chat
    }

    #[test]
    fn a_sweep_enumerates_and_stamps_once_and_nothing_outlives_it() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("sessions");
        let first = transcript(&root, "s1");
        let stamp = |chat: &Path| crate::grok_session_stamp_and_modified(chat).unwrap();

        // Outside a sweep every call reads the disk.
        assert_eq!(grok_transcripts(&root).unwrap(), vec![first.clone()]);
        let second = transcript(&root, "s2");
        assert_eq!(grok_transcripts(&root).unwrap().len(), 2);
        assert_eq!(grok_stamp_and_modified(&first).unwrap(), stamp(&first));

        {
            let _sweep = SweepInventory::begin();
            let walked = grok_transcripts(&root).unwrap();
            let stamped = grok_stamp_and_modified(&first).unwrap();
            // Changes after the first reader saw the tree are not seen again
            // within the sweep: the earlier answer is the one every reader
            // shares.
            transcript(&root, "s3");
            std::fs::write(&first, "{}\n{}\n").unwrap();
            assert_eq!(grok_transcripts(&root).unwrap(), walked);
            assert_eq!(grok_stamp_and_modified(&first).unwrap(), stamped);
            assert_ne!(stamped, stamp(&first));
            // Another tree is walked on its own.
            let elsewhere = temp.path().join("elsewhere");
            transcript(&elsewhere, "e1");
            assert_eq!(grok_transcripts(&elsewhere).unwrap().len(), 1);
            // A transcript nobody stamped yet is stamped on demand.
            assert_eq!(grok_stamp_and_modified(&second).unwrap(), stamp(&second));
        }

        // The guard's end forgets it all.
        assert_eq!(grok_transcripts(&root).unwrap().len(), 3);
        assert_eq!(grok_stamp_and_modified(&first).unwrap(), stamp(&first));
    }
}
