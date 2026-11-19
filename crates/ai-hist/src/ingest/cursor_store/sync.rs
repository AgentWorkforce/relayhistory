//! The sync pass over chats that exist only in Cursor's chat store.

use std::collections::HashSet;
use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::Connection;
use serde_json::{Map, Value};

use super::{
    chat_store_stamp_and_modified, chat_stores, read_chat_store, CHATS_DIR, CHAT_STORE_STATE_KEY,
};

/// Index every chat that exists only in Cursor's chat store. A store is
/// re-read whole when its stamp changes, and its session rebuilt from it.
pub(in crate::ingest) fn sync_chat_stores_in(
    conn: &Connection,
    state: &mut Map<String, Value>,
    home: &Path,
    coverage: &mut crate::ingest::SweepCoverage,
    paths: Option<&crate::ingest::path_scope::PathScope>,
) -> Result<usize> {
    // `sync_note!` names the parent's quiet flag.
    use crate::ingest::{AtomicOrdering, SYNC_QUIET};
    let stores = chat_stores(home, paths)?;
    let mut seen = state
        .get(CHAT_STORE_STATE_KEY)
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    // A full walk saw every store there is: a stamp for one it did not see
    // (deleted, or its chat now has a transcript) is dropped, so a store that
    // comes back is read again rather than skipped on a stale stamp.
    if crate::ingest::path_scope::PathScope::walk_is_full(paths, &home.join(CHATS_DIR)) {
        let present: HashSet<String> = stores
            .iter()
            .map(|(path, _)| path.to_string_lossy().into_owned())
            .collect();
        let missing: Vec<String> = seen
            .keys()
            .filter(|key| !present.contains(*key))
            .cloned()
            .collect();
        if !missing.is_empty() {
            for key in &missing {
                seen.remove(key);
            }
            crate::ingest::forget_unobserved_paths(
                state,
                &mut Map::new(),
                CHAT_STORE_STATE_KEY,
                missing,
            );
            state.insert(
                CHAT_STORE_STATE_KEY.to_string(),
                Value::Object(seen.clone()),
            );
        }
    }
    if stores.is_empty() {
        return Ok(0);
    }
    // Provider I/O before the destination writer lock, as the transcript
    // pass does.
    let mut prepared = Vec::new();
    let mut errors = 0usize;
    for (path, _) in crate::ingest::capture_files("cursor", stores) {
        crate::ingest::check_capture_cancelled()?;
        let key = path.to_string_lossy().into_owned();
        let stamp = match chat_store_stamp_and_modified(&path) {
            Ok((stamp, _)) => stamp,
            Err(_) => continue,
        };
        if seen.get(&key).and_then(Value::as_str) == Some(stamp.as_str()) {
            continue;
        }
        match read_chat_store(&path) {
            Ok(Some(store)) => prepared.push((path, key, stamp, store)),
            Ok(None) => {}
            Err(error) => {
                if error.is::<crate::ingest::CaptureCancelled>() {
                    return Err(error);
                }
                sync_note!(
                    "  [cursor] skipping chat store {}: {error:#}",
                    path.display()
                );
                errors += 1;
                coverage.note_unread();
            }
        }
    }
    if prepared.is_empty() {
        return Ok(0);
    }
    let tx = conn.unchecked_transaction()?;
    let mut inserted = 0;
    for (path, key, stamp, store) in &prepared {
        crate::ingest::check_capture_cancelled()?;
        let fallback_ms = store.updated_ms.or(store.created_ms).unwrap_or(0);
        crate::ingest::clear_cursor_session_evidence(&tx, &store.session_id)?;
        // The transcript parser reads the store again inside the
        // transaction, as it reads a transcript; `store` supplies the cwd and
        // the times the records do not carry.
        let outcome = crate::ingest::ingest_cursor_transcript(
            &tx,
            path,
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
        seen.insert(key.clone(), Value::String(stamp.clone()));
    }
    tx.commit().context("commit Cursor chat store sync")?;
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
