//! Read-only reuse of native global-sync proofs during a cold round only.
//! No cursor is adopted, and hot/selected acquisition never installs this scope.
use super::*;
use std::cell::RefCell;

#[derive(Default)]
pub(crate) struct ColdProof {
    loaded: bool,
    snapshot: Option<Snapshot>,
}
impl std::fmt::Debug for ColdProof {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColdProof")
            .field("loaded", &self.loaded)
            .finish_non_exhaustive()
    }
}
struct Snapshot {
    state: Map<String, Value>,
    destination: DestinationProof,
    state_path: PathBuf,
    stamp: Stamp,
    database: crate::read_pool::FileIdentity,
    epoch: u64,
}
enum DestinationProof {
    Absent,
    Valid(DestinationMarker),
    Invalid,
}
#[derive(PartialEq, Eq)]
struct Stamp {
    identity: crate::read_pool::FileIdentity,
    len: u64,
    mtime: u64,
    ctime: Option<i64>,
}
impl Stamp {
    fn at(path: &Path) -> Option<Self> {
        let metadata = fs::metadata(path).ok()?;
        Some(Self {
            identity: crate::read_pool::file_identity(path)?,
            len: metadata.len(),
            mtime: metadata_mtime_ns(&metadata),
            ctime: transcript_cursor::change_time_ns(&metadata),
        })
    }
}
thread_local! { static CURRENT: RefCell<Option<ColdProof>> = const { RefCell::new(None) }; }

/// Move the round's cache into thread-local scope. Restore both contexts on
/// error or unwind; no borrowed reference or non-cancellable mutex escapes.
pub(crate) fn with_cold_proof<T>(cache: &mut ColdProof, run: impl FnOnce() -> T) -> T {
    struct Restore<'a> {
        target: &'a mut ColdProof,
        previous: Option<ColdProof>,
    }
    impl Drop for Restore<'_> {
        fn drop(&mut self) {
            let current = CURRENT.with(|slot| slot.replace(self.previous.take()));
            *self.target = current.unwrap_or_default();
        }
    }
    let previous = CURRENT.with(|slot| slot.replace(Some(std::mem::take(cache))));
    let _restore = Restore {
        target: cache,
        previous,
    };
    run()
}

pub(super) fn unchanged(
    conn: &Connection,
    db: &Path,
    source: &str,
    session: &str,
    path: &Path,
) -> Result<bool> {
    CURRENT.with(|slot| {
        let mut slot = slot.borrow_mut();
        let Some(cache) = slot.as_mut() else {
            return Ok(false);
        };
        if !cache.loaded {
            cache.loaded = true;
            cache.snapshot = Snapshot::load(conn, db);
        }
        let Some(snapshot) = &cache.snapshot else {
            return Ok(false);
        };
        if !snapshot.current(conn, db) || !snapshot.provider_current(source) {
            return Ok(false);
        }
        if !snapshot.destination_covers(conn, source, session)? {
            return Ok(false);
        }
        snapshot.file_unchanged(conn, source, session, path)
    })
}
impl Snapshot {
    fn load(conn: &Connection, db: &Path) -> Option<Self> {
        let state_path = db.parent()?.join(".sync-state.json");
        let stamp = Stamp::at(&state_path)?;
        let state = load_sync_state_stamped(&state_path).0;
        if Stamp::at(&state_path).as_ref() != Some(&stamp) {
            return None;
        }
        let destination = match state.get(DESTINATION_GENERATION_KEY) {
            None => DestinationProof::Absent,
            Some(value) => value
                .as_str()
                .and_then(parse_destination_marker)
                .map(DestinationProof::Valid)
                .unwrap_or(DestinationProof::Invalid),
        };
        Some(Self {
            state,
            destination,
            state_path,
            stamp,
            database: crate::read_pool::file_identity(db)?,
            epoch: crate::change_feed::read_head(conn).ok()?.epoch,
        })
    }
    fn current(&self, conn: &Connection, db: &Path) -> bool {
        Stamp::at(&self.state_path).as_ref() == Some(&self.stamp)
            && crate::read_pool::file_identity(db).as_ref() == Some(&self.database)
            && crate::change_feed::read_head(conn).is_ok_and(|head| head.epoch == self.epoch)
    }
    fn provider_current(&self, source: &str) -> bool {
        let version =
            |key, current| self.state.get(key).and_then(Value::as_i64).unwrap_or(0) >= current;
        match source {
            "claude" => {
                !fidelity_backfill_pending(&self.state, CLAUDE_FIDELITY_GENERATION_KEY)
                    && !raw_facts_backfill_pending(&self.state, CLAUDE_RAW_MESSAGE_FACTS_KEY)
                    && !delegation_backfill_pending(&self.state)
                    && version(CLAUDE_SIDECAR_LAYOUT_KEY, CLAUDE_SIDECAR_LAYOUT_GENERATION)
                    && version(
                        CLAUDE_RECORD_ATTRIBUTION_KEY,
                        CLAUDE_RECORD_ATTRIBUTION_GENERATION,
                    )
            }
            "codex" => {
                !fidelity_backfill_pending(&self.state, CODEX_FIDELITY_GENERATION_KEY)
                    && !raw_facts_backfill_pending(&self.state, CODEX_RAW_MESSAGE_FACTS_KEY)
                    && version(CODEX_FORK_REPLAY_KEY, CODEX_FORK_REPLAY_GENERATION)
                    && version(CODEX_STATE_MARKER_KEY, CODEX_STATE_MARKER_GENERATION)
            }
            _ => false,
        }
    }
    fn destination_covers(&self, conn: &Connection, source: &str, session: &str) -> Result<bool> {
        // Preserve native sync's absent-marker contract: its own indexed
        // evidence guard applies below. An absent marker cannot independently
        // detect a partial historical row loss. A present marker must vouch.
        let marker = match &self.destination {
            DestinationProof::Absent => return Ok(true),
            DestinationProof::Invalid => return Ok(false),
            DestinationProof::Valid(marker) => marker,
        };
        let key = discover::fingerprint_hash("session-events", source, session);
        let Some(stored) = marker.sessions.get(&key) else {
            return Ok(false);
        };
        Ok(session_holding(conn, source, session)?.covers(stored))
    }
    fn file_unchanged(
        &self,
        conn: &Connection,
        source: &str,
        session: &str,
        path: &Path,
    ) -> Result<bool> {
        let locator = path.to_string_lossy();
        let cursor = transcript_cursor::load_cursor(
            conn,
            &transcript_cursor::CursorKey::Locator {
                source,
                locator: &locator,
            },
        )?;
        let current = match source {
            "claude" => {
                cursor.claude.as_ref().is_some_and(|state| {
                    state.records_parser == hydrate::HYDRATION_PARSER_VERSION
                        && state.in_progress.is_empty()
                }) && claude_transcript_events_exist(conn, path)?
                    && !claude_transcript_lacks_continuity_evidence(conn, path)?
                    && !claude_delegation_cites(conn, path)?
            }
            "codex" => {
                let record = self
                    .state
                    .get("codex_rollouts_v7")
                    .and_then(Value::as_object)
                    .and_then(|files| files.get(locator.as_ref()))
                    .and_then(Value::as_object);
                cursor.codex.as_ref().is_some_and(|state| {
                    state.state_markers >= CODEX_STATE_MARKER_GENERATION
                        && state.fork_replay >= CODEX_FORK_REPLAY_GENERATION
                }) && record.is_some_and(|row| {
                    row.get("session").and_then(Value::as_str) == Some(session)
                        && row.get("stamp").and_then(Value::as_str)
                            == file_stamp(path).ok().as_deref()
                        && row.get("subagent") == Some(&Value::Bool(false))
                }) && !images::rollout_owes_capture(record, true)
                    && codex_session_evidence_exists(conn, session)?
                    && codex_continuity_evidence_exists(conn, path)?
            }
            _ => false,
        };
        Ok(current && transcript_cursor::transcript_unchanged_readonly(conn, source, path)?)
    }
}

#[cfg(test)]
mod tests;
