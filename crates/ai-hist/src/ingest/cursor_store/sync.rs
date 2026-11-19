//! The sync pass over chats that exist only in Cursor's chat store.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rusqlite::Connection;
use serde_json::{Map, Value};

// `sync_note!` names the parent's quiet flag.
use crate::ingest::{AtomicOrdering, SYNC_QUIET};

use super::{
    chat_store_stamp_and_modified, chat_stores, read_chat_store, ChatStore, CHATS_DIR,
    CHAT_STORE_STATE_KEY,
};

/// One chat read before the destination writer lock is taken.
struct Prepared {
    path: PathBuf,
    stamp: String,
    store: ChatStore,
}

/// Index every chat that exists only in Cursor's chat store. A store is
/// re-read whole when its stamp changes, and its session rebuilt from it.
pub(in crate::ingest) fn sync_chat_stores_in(
    conn: &Connection,
    state: &mut Map<String, Value>,
    home: &Path,
    coverage: &mut crate::ingest::SweepCoverage,
    paths: Option<&crate::ingest::path_scope::PathScope>,
) -> Result<usize> {
    let stores = chat_stores(home, paths)?;
    let mut seen = state
        .get(CHAT_STORE_STATE_KEY)
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    if crate::ingest::path_scope::PathScope::walk_is_full(paths, &home.join(CHATS_DIR)) {
        forget_unseen(state, &mut seen, &stores);
    }
    // Provider I/O before the destination writer lock, as the transcript
    // pass does.
    let (prepared, errors) = prepare(conn, stores, &seen, coverage)?;
    if prepared.is_empty() {
        return Ok(0);
    }
    let inserted = index(conn, &prepared, &mut seen)?;
    state.insert(CHAT_STORE_STATE_KEY.to_string(), Value::Object(seen));
    let suffix = if errors > 0 {
        format!(" ({errors} errors)")
    } else {
        String::new()
    };
    sync_note!(
        "  [cursor] {} chat store(s), {inserted} prompts{suffix}",
        prepared.len()
    );
    Ok(inserted)
}

/// A full walk saw every store there is: a stamp for one it did not see
/// (deleted, or its chat now has a transcript) is dropped, so a store that
/// comes back is read again rather than skipped on a stale stamp.
fn forget_unseen(
    state: &mut Map<String, Value>,
    seen: &mut Map<String, Value>,
    stores: &[(PathBuf, String)],
) {
    let present: HashSet<String> = stores
        .iter()
        .map(|(path, _)| path.to_string_lossy().into_owned())
        .collect();
    let missing: Vec<String> = seen
        .keys()
        .filter(|key| !present.contains(*key))
        .cloned()
        .collect();
    if missing.is_empty() {
        return;
    }
    for key in &missing {
        seen.remove(key);
    }
    crate::ingest::forget_unobserved_paths(state, &mut Map::new(), CHAT_STORE_STATE_KEY, missing);
    state.insert(
        CHAT_STORE_STATE_KEY.to_string(),
        Value::Object(seen.clone()),
    );
}

/// Read every store that changed, or whose evidence the destination lost.
/// A store that cannot be read is noted and left for the next pass.
fn prepare(
    conn: &Connection,
    stores: Vec<(PathBuf, String)>,
    seen: &Map<String, Value>,
    coverage: &mut crate::ingest::SweepCoverage,
) -> Result<(Vec<Prepared>, usize)> {
    let mut prepared = Vec::new();
    let mut errors = 0usize;
    for (path, session_id) in crate::ingest::capture_files("cursor", stores) {
        crate::ingest::check_capture_cancelled()?;
        let Ok((stamp, _)) = chat_store_stamp_and_modified(&path) else {
            continue;
        };
        if unchanged(conn, seen, &path, &stamp, &session_id)? {
            continue;
        }
        match read_chat_store(&path) {
            Ok(Some(store)) => {
                note_skipped_blobs(&path, &store);
                prepared.push(Prepared { path, stamp, store });
            }
            Ok(None) => {}
            Err(error) if error.is::<crate::ingest::CaptureCancelled>() => return Err(error),
            Err(error) => {
                sync_note!(
                    "  [cursor] skipping chat store {}: {error:#}",
                    path.display()
                );
                errors += 1;
                coverage.note_unread();
            }
        }
    }
    Ok((prepared, errors))
}

/// The stamp says the store is unchanged; it is trusted only while the
/// destination still holds what was indexed from it, so a rebuilt database
/// is filled again.
fn unchanged(
    conn: &Connection,
    seen: &Map<String, Value>,
    path: &Path,
    stamp: &str,
    session_id: &str,
) -> Result<bool> {
    let key = path.to_string_lossy();
    if seen.get(key.as_ref()).and_then(Value::as_str) != Some(stamp) {
        return Ok(false);
    }
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM session_events WHERE source = 'cursor' AND session_id = ?1)",
        [session_id],
        |row| row.get(0),
    )?)
}

fn note_skipped_blobs(path: &Path, store: &ChatStore) {
    if store.skipped_blobs > 0 {
        sync_note!(
            "  [cursor] chat store {}: {} message blob(s) missing or not readable JSON \
             (possibly encrypted), skipped",
            path.display(),
            store.skipped_blobs
        );
    }
}

/// Rebuild each prepared chat in one transaction, from the copy read above,
/// so the batch never reopens a store a live chat may be writing to.
fn index(conn: &Connection, prepared: &[Prepared], seen: &mut Map<String, Value>) -> Result<usize> {
    let tx = conn.unchecked_transaction()?;
    let mut inserted = 0;
    for Prepared { path, stamp, store } in prepared {
        crate::ingest::check_capture_cancelled()?;
        let fallback_ms = store.updated_ms.or(store.created_ms).unwrap_or(0);
        crate::ingest::clear_cursor_session_evidence(&tx, &store.session_id)?;
        let outcome = crate::ingest::ingest_cursor_transcript(
            &tx,
            super::Records::Rendered(&store.transcript),
            &store.session_id,
            store.cwd.as_deref(),
            fallback_ms,
            0,
            u64::MAX,
        )
        .with_context(|| format!("index Cursor chat store {}", path.display()))?;
        inserted += outcome.prompts_inserted;
        crate::ingest::upsert_session_rebuilt(
            &tx,
            &crate::ingest::SessionCatalogRow {
                session_id: &store.session_id,
                source: "cursor",
                cwd: store.cwd.as_deref(),
                git_branch: None,
                first_ts: outcome.first_ts_ms.unwrap_or(fallback_ms),
                last_ts: outcome.last_ts_ms.unwrap_or(fallback_ms),
                last_assistant_text: outcome.last_assistant_text.as_deref(),
                raw_path: Some(&path.to_string_lossy()),
            },
        )?;
        seen.insert(
            path.to_string_lossy().into_owned(),
            Value::String(stamp.clone()),
        );
    }
    tx.commit().context("commit Cursor chat store sync")?;
    Ok(inserted)
}
