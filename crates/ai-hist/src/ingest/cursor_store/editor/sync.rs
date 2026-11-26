//! The sync pass over composers that exist only in the editor database.

use std::collections::HashSet;
use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::Connection;
use serde_json::{Map, Value};

// `sync_note!` names the parent's quiet flag.
use crate::ingest::{AtomicOrdering, SYNC_QUIET};

use super::{composers, locator, read_composer, state_db_path, Composer, EDITOR_STATE_KEY};

/// Marks a saved stamp whose composer rendered no evidence.
const NO_EVIDENCE: &str = "|no-evidence";

/// Index every composer that exists only in the editor database. A composer
/// is re-read whole when its stamp changes, and its session rebuilt from it.
pub(in crate::ingest) fn sync_editor_composers_in(
    conn: &Connection,
    state: &mut Map<String, Value>,
    home: &Path,
    coverage: &mut crate::ingest::SweepCoverage,
    paths: Option<&crate::ingest::path_scope::PathScope>,
) -> Result<usize> {
    let db = state_db_path(home);
    let mut seen = state
        .get(EDITOR_STATE_KEY)
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let heads = match composers(home, paths) {
        Ok(heads) => heads,
        Err(error) if error.is::<crate::ingest::CaptureCancelled>() => return Err(error),
        Err(error) => {
            sync_note!(
                "  [cursor] skipping editor database {}: {error:#}",
                db.display()
            );
            coverage.note_unread();
            return Ok(0);
        }
    };
    let skipped = paths.is_some_and(|scope| db.parent().is_some_and(|dir| !scope.mentions(dir)));
    if !skipped {
        // Every composer the database holds was enumerated: a stamp for one
        // that is gone, emptied, or now read from another source is dropped.
        let present: HashSet<String> = heads
            .iter()
            .map(|head| locator(&db, &head.session_id))
            .collect();
        forget_unseen(state, &mut seen, &present);
    }
    // Ownership checked once for the pass: a composer that gained a
    // transcript or a chat store since enumeration is not this database's.
    let owned = super::owned_elsewhere(home)?;
    let mut pass = Pass::default();
    for head in heads {
        crate::ingest::check_capture_cancelled()?;
        let locator = locator(&db, &head.session_id);
        if owned.contains(&head.session_id) {
            seen.remove(&locator);
            continue;
        }
        if unchanged(conn, &seen, &locator, &head.stamp, &head.session_id)? {
            continue;
        }
        // One composer at a time: read before the writer lock, rebuilt and
        // committed before the next is read, so memory is one conversation
        // and an interrupted pass keeps what it committed.
        let composer = match read_composer(&db, &head.session_id) {
            Ok(Some(composer)) => composer,
            Ok(None) => continue,
            Err(error) if error.is::<crate::ingest::CaptureCancelled>() => return Err(error),
            Err(error) => {
                sync_note!("  [cursor] skipping editor conversation {locator}: {error:#}");
                pass.errors += 1;
                coverage.note_unread();
                continue;
            }
        };
        pass.index(conn, &locator, &head.stamp, &composer, &mut seen, coverage)?;
        state.insert(EDITOR_STATE_KEY.to_string(), Value::Object(seen.clone()));
    }
    state.insert(EDITOR_STATE_KEY.to_string(), Value::Object(seen));
    if pass.conversations == 0 {
        return Ok(0);
    }
    let suffix = if pass.errors + pass.incomplete > 0 {
        format!(" ({} errors, {} incomplete)", pass.errors, pass.incomplete)
    } else {
        String::new()
    };
    sync_note!(
        "  [cursor] {} editor conversation(s), {} prompts{suffix}",
        pass.conversations,
        pass.inserted
    );
    Ok(pass.inserted)
}

/// What one sync pass over the database did.
#[derive(Default)]
struct Pass {
    conversations: usize,
    inserted: usize,
    errors: usize,
    incomplete: usize,
}

impl Pass {
    /// Rebuild one composer's session in its own transaction. A composer read
    /// short of what it declares is indexed as far as it goes, but its stamp
    /// is not saved and the sweep is told it did not read everything, so the
    /// next pass reads it again.
    fn index(
        &mut self,
        conn: &Connection,
        locator: &str,
        stamp: &str,
        composer: &Composer,
        seen: &mut Map<String, Value>,
        coverage: &mut crate::ingest::SweepCoverage,
    ) -> Result<()> {
        let tx = conn.unchecked_transaction()?;
        let (inserted, records, _) = rebuild(&tx, locator, composer)?;
        tx.commit().context("commit Cursor editor sync")?;
        self.conversations += 1;
        self.inserted += inserted;
        if composer.incomplete {
            sync_note!(
                "  [cursor] editor conversation {locator} is incomplete: {} listed message(s) \
                 unreadable or its header log not fully present",
                composer.skipped_bubbles
            );
            self.incomplete += 1;
            seen.remove(locator);
            coverage.note_unread();
            return Ok(());
        }
        let saved = if records == 0 {
            format!("{stamp}{NO_EVIDENCE}")
        } else {
            stamp.to_string()
        };
        seen.insert(locator.to_string(), Value::String(saved));
        coverage.note_read("cursor", &composer.session_id, true);
        Ok(())
    }
}

/// Drop the saved stamp of every composer this full read did not find, so
/// one that comes back is read again rather than skipped on a stale stamp.
fn forget_unseen(
    state: &mut Map<String, Value>,
    seen: &mut Map<String, Value>,
    present: &HashSet<String>,
) {
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
    crate::ingest::forget_unobserved_paths(state, &mut Map::new(), EDITOR_STATE_KEY, missing);
    state.insert(EDITOR_STATE_KEY.to_string(), Value::Object(seen.clone()));
}

/// The stamp says the composer is unchanged; it is trusted only while the
/// destination still holds what was indexed from it.
fn unchanged(
    conn: &Connection,
    seen: &Map<String, Value>,
    locator: &str,
    stamp: &str,
    session_id: &str,
) -> Result<bool> {
    let saved = seen.get(locator).and_then(Value::as_str);
    if saved == Some(&format!("{stamp}{NO_EVIDENCE}")) {
        return Ok(true);
    }
    if saved != Some(stamp) {
        return Ok(false);
    }
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sessions WHERE source = 'cursor' AND session_id = ?1 \
             AND raw_path = ?2) \
         AND EXISTS(SELECT 1 FROM session_events WHERE source = 'cursor' AND session_id = ?1)",
        rusqlite::params![session_id, locator],
        |row| row.get(0),
    )?)
}

/// Replace a composer's session with what it renders to; returns the prompts
/// inserted and the records parsed.
pub(crate) fn rebuild(
    conn: &Connection,
    locator: &str,
    composer: &Composer,
) -> Result<(usize, i64, crate::ingest::CursorTranscriptOutcome)> {
    let fallback_ms = composer.updated_ms.or(composer.created_ms).unwrap_or(0);
    crate::ingest::clear_cursor_session_evidence(conn, &composer.session_id)?;
    let outcome = crate::ingest::ingest_cursor_transcript(
        conn,
        crate::ingest::cursor_store::Records::Rendered(&composer.transcript),
        &composer.session_id,
        composer.cwd.as_deref(),
        fallback_ms,
        0,
        u64::MAX,
    )
    .with_context(|| format!("index Cursor editor conversation {locator}"))?;
    let first_ts = match (outcome.first_ts_ms, composer.created_ms) {
        (Some(first), Some(created)) => first.min(created),
        (first, created) => first.or(created).unwrap_or(fallback_ms),
    };
    crate::ingest::upsert_session_rebuilt(
        conn,
        &crate::ingest::SessionCatalogRow {
            session_id: &composer.session_id,
            source: "cursor",
            cwd: composer.cwd.as_deref(),
            git_branch: None,
            first_ts,
            last_ts: crate::ingest::cursor_store::chat_end(
                &outcome,
                composer.updated_ms,
                fallback_ms,
            ),
            last_assistant_text: outcome.last_assistant_text.as_deref(),
            raw_path: Some(locator),
        },
    )?;
    crate::ingest::titles::set_session_title(
        conn,
        "cursor",
        &composer.session_id,
        composer.title.as_deref(),
    )?;
    Ok((outcome.prompts_inserted, outcome.records, outcome))
}
