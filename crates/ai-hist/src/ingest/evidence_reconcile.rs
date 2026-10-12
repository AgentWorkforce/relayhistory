//! Identity-preserving replacement for one Grok or Muse re-read.
//!
//! Grok rewrites `chat_history.jsonl` in place. Muse re-reads the whole
//! transcript. Deleting every local row and inserting it again assigns new
//! ids, so the change feed reports the session even when the bytes did not
//! change. A re-read deletes only rows the new snapshot does not contain,
//! inserts rows that are new, and updates a row in place when its identity
//! is stable and its content changed. Unchanged rows keep their ids.
//!
//! A tool call, marker, history row, or `subagents/` entry the snapshot no
//! longer contains is gone after the pass. `history` stays keyed
//! `(source, timestamp_ms, prompt)`: a prompt two sessions share is one row.
//! The rows this session owned are snapshotted before its events are
//! rewritten, then re-attributed to a session that still evidences them, and
//! only what this session alone still owns is deleted.
//!
//! The guard arms only when that session already has local evidence. A first
//! insert pays one existence check. Notes taken while disarmed return
//! immediately. Dropping the guard without [`finish`] — an error inside the
//! caller's transaction — does not sweep. The caller finishes on the thread
//! that armed the guard.

use std::cell::{Cell, RefCell};
use std::collections::HashSet;

pub(crate) use super::evidence_upserts::{
    FILE_EDIT_REWRITE, FILE_EDIT_UPSERT, SESSION_EVENT_REWRITE, SESSION_EVENT_REWRITE_CACHED,
    SESSION_EVENT_UPSERT, SESSION_EVENT_UPSERT_CACHED, TOOL_CALL_REWRITE, TOOL_CALL_UPSERT,
};

/// The armed pass skips a rewrite of a row that already holds these values.
/// Every other pass rewrites, so a caller that measures `total_changes` still
/// sees the line as written.
#[inline]
pub(super) fn write_sql<'a>(unchanged: &'a str, rewrite: &'a str) -> &'a str {
    if armed() {
        unchanged
    } else {
        rewrite
    }
}

use anyhow::{bail, Result};
use rusqlite::{params, Connection, OptionalExtension};

use super::HistoryScope;

thread_local! {
    static ARMED: Cell<bool> = const { Cell::new(false) };
    static ACTIVE: RefCell<Option<Active>> = const { RefCell::new(None) };
}

struct Active {
    source: String,
    session_id: String,
    events: HashSet<String>,
    tools: HashSet<String>,
    /// Tool calls whose result this pass observed. A local call that is not
    /// here keeps no error flag from a result the snapshot dropped.
    tool_results: HashSet<String>,
    edits: HashSet<String>,
    markers: HashSet<String>,
    relationships: HashSet<String>,
    history: HashSet<(i64, String)>,
    sweep_markers: bool,
    sweep_relationships: bool,
}

enum Noted {
    Event,
    Tool,
    Edit,
    Marker,
    Relationship,
}

/// Armed for the session a Grok or Muse read is replacing. `live` is false
/// when the session has nothing to reconcile, which is every first insert.
#[must_use]
pub(crate) struct Guard {
    live: bool,
}

impl Drop for Guard {
    fn drop(&mut self) {
        if self.live {
            disarm();
            self.live = false;
        }
    }
}

#[inline]
pub(crate) fn armed() -> bool {
    ARMED.with(|flag| flag.get())
}

fn disarm() {
    ARMED.with(|flag| flag.set(false));
    ACTIVE.with(|slot| *slot.borrow_mut() = None);
}

/// Start a Grok re-read. History scope is the whole session: a prompt this
/// snapshot no longer contains is not its prompt anymore.
pub(crate) fn begin_grok(conn: &Connection, session_id: &str) -> Result<Guard> {
    begin(conn, "grok", session_id, HistoryScope::Session, true, true)
}

/// Start a Muse re-read. `scope` is the one the caller already chose, judged
/// against the events still stored from the previous read.
pub(super) fn begin_muse(
    conn: &Connection,
    session_id: &str,
    scope: HistoryScope,
) -> Result<Guard> {
    begin(conn, "muse", session_id, scope, true, false)
}

fn begin(
    conn: &Connection,
    source: &str,
    session_id: &str,
    scope: HistoryScope,
    sweep_markers: bool,
    sweep_relationships: bool,
) -> Result<Guard> {
    if armed() {
        bail!("evidence reconcile is already armed on this thread");
    }
    if !has_local_share(conn, source, session_id, sweep_relationships)? {
        return Ok(Guard { live: false });
    }
    snapshot_owned_history(conn, source, session_id, scope)?;
    ACTIVE.with(|slot| {
        *slot.borrow_mut() = Some(Active {
            source: source.to_string(),
            session_id: session_id.to_string(),
            events: HashSet::new(),
            tools: HashSet::new(),
            tool_results: HashSet::new(),
            edits: HashSet::new(),
            markers: HashSet::new(),
            relationships: HashSet::new(),
            history: HashSet::new(),
            sweep_markers,
            sweep_relationships,
        });
    });
    ARMED.with(|flag| flag.set(true));
    Ok(Guard { live: true })
}

/// Delete evidence this pass did not write, and re-attribute history it no
/// longer claims. A disarmed guard does nothing.
pub(crate) fn finish(conn: &Connection, guard: &mut Guard) -> Result<()> {
    if !guard.live {
        return Ok(());
    }
    guard.live = false;
    let active = ACTIVE.with(|slot| slot.borrow_mut().take());
    ARMED.with(|flag| flag.set(false));
    let Some(active) = active else {
        return Ok(());
    };
    sweep(conn, &active)
}

fn has_local_share(
    conn: &Connection,
    source: &str,
    session_id: &str,
    relationships: bool,
) -> Result<bool> {
    let relationships = if relationships {
        " OR EXISTS (SELECT 1 FROM session_relationships \
           WHERE source = ?1 AND parent_session_id = ?2 \
             AND evidence_kind = 'grok_subagent_dir')"
    } else {
        ""
    };
    let sql = format!(
        "SELECT 1 WHERE \
         EXISTS (SELECT 1 FROM session_events \
                 WHERE source = ?1 AND session_id = ?2 AND location != 'remote') \
         OR EXISTS (SELECT 1 FROM tool_calls \
                    WHERE source = ?1 AND session_id = ?2 AND location != 'remote') \
         OR EXISTS (SELECT 1 FROM file_edits \
                    WHERE source = ?1 AND session_id = ?2 AND location != 'remote') \
         OR EXISTS (SELECT 1 FROM session_markers \
                    WHERE source = ?1 AND session_id = ?2 AND location != 'remote') \
         OR EXISTS (SELECT 1 FROM history WHERE source = ?1 AND session_id = ?2){relationships}"
    );
    Ok(conn
        .query_row(&sql, params![source, session_id], |_| Ok(()))
        .optional()?
        .is_some())
}

fn owned_predicate(scope: HistoryScope) -> &'static str {
    match scope {
        HistoryScope::Session => "",
        HistoryScope::OwnPromptEvents => {
            " AND EXISTS (SELECT 1 FROM session_events own \
                          WHERE own.source = h.source AND own.session_id = h.session_id \
                            AND own.role = 'user' AND own.kind = 'text' \
                            AND own.ts_ms = h.timestamp_ms AND own.text = h.prompt)"
        }
    }
}

fn snapshot_owned_history(
    conn: &Connection,
    source: &str,
    session_id: &str,
    scope: HistoryScope,
) -> Result<()> {
    conn.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS evidence_reconcile_owned (id INTEGER PRIMARY KEY);
         CREATE TEMP TABLE IF NOT EXISTS evidence_reconcile_history_kept (
           timestamp_ms INTEGER NOT NULL,
           prompt TEXT NOT NULL,
           PRIMARY KEY (timestamp_ms, prompt)
         );",
    )?;
    conn.execute("DELETE FROM evidence_reconcile_owned", [])?;
    conn.execute("DELETE FROM evidence_reconcile_history_kept", [])?;
    let sql = format!(
        "INSERT INTO evidence_reconcile_owned (id) \
         SELECT h.id FROM history h WHERE h.source = ?1 AND h.session_id = ?2{}",
        owned_predicate(scope)
    );
    conn.execute(&sql, params![source, session_id])?;
    Ok(())
}

fn note(source: &str, session_id: &str, kind: Noted, key: &str) {
    if !armed() {
        return;
    }
    ACTIVE.with(|slot| {
        let mut borrowed = slot.borrow_mut();
        let Some(active) = borrowed.as_mut() else {
            return;
        };
        if active.source != source || active.session_id != session_id {
            return;
        }
        let set = match kind {
            Noted::Event => &mut active.events,
            Noted::Tool => &mut active.tools,
            Noted::Edit => &mut active.edits,
            Noted::Marker => &mut active.markers,
            Noted::Relationship => &mut active.relationships,
        };
        set.insert(key.to_string());
    });
}

#[inline]
pub(crate) fn note_event(source: &str, session_id: &str, event_uid: &str) {
    note(source, session_id, Noted::Event, event_uid);
}

#[inline]
pub(crate) fn note_tool(source: &str, session_id: &str, tool_use_id: &str) {
    note(source, session_id, Noted::Tool, tool_use_id);
}

/// The snapshot still contains a result for this call, so its `is_error`
/// is the result's and not whatever the previous read stored.
pub(crate) fn note_tool_result(source: &str, session_id: &str, tool_use_id: &str) {
    if !armed() {
        return;
    }
    ACTIVE.with(|slot| {
        let mut borrowed = slot.borrow_mut();
        let Some(active) = borrowed.as_mut() else {
            return;
        };
        if active.source == source && active.session_id == session_id {
            active.tool_results.insert(tool_use_id.to_string());
        }
    });
}

#[inline]
pub(crate) fn note_edit(source: &str, session_id: &str, tool_use_id: &str) {
    note(source, session_id, Noted::Edit, tool_use_id);
}

#[inline]
pub(crate) fn note_marker(source: &str, session_id: &str, marker_uid: &str) {
    note(source, session_id, Noted::Marker, marker_uid);
}

/// Record a relationship the armed pass re-observed, including one whose
/// update wrote nothing because the edge was already current.
pub(crate) fn note_relationship(observed: &super::ObservedRelationship<'_>) {
    if !armed() {
        return;
    }
    let uid = observed.relationship_uid();
    note(
        observed.source,
        observed.parent_session_id,
        Noted::Relationship,
        &uid,
    );
}

/// Record local presence for a prompt, then note it when a reconcile is armed.
pub(crate) fn record_history_presence(
    conn: &Connection,
    entry: &crate::HistoryEntry,
    location: crate::SessionLocation,
) -> Result<()> {
    if let Some(session_id) = entry.session_id.as_deref().filter(|id| !id.is_empty()) {
        crate::mark_session_presence(conn, &entry.source, session_id, location)?;
    }
    observe_history(conn, entry)
}

/// Record local presence for a marker, then note it when a reconcile is armed.
pub(crate) fn record_marker_presence(
    conn: &Connection,
    source: &str,
    session_id: &str,
    marker_uid: &str,
) -> Result<()> {
    crate::mark_session_presence(conn, source, session_id, crate::SessionLocation::Local)?;
    note_marker(source, session_id, marker_uid);
    Ok(())
}

/// Keep a prompt the pass wrote, and refresh its project on this session's
/// own row when that project changed. A prompt filed under another session
/// stays there.
pub(crate) fn observe_history(conn: &Connection, entry: &crate::HistoryEntry) -> Result<()> {
    if !armed() {
        return Ok(());
    }
    let Some(session_id) = entry.session_id.as_deref().filter(|id| !id.is_empty()) else {
        return Ok(());
    };
    let matches = ACTIVE.with(|slot| {
        slot.borrow()
            .as_ref()
            .is_some_and(|active| active.source == entry.source && active.session_id == session_id)
    });
    if !matches {
        return Ok(());
    }
    conn.prepare_cached(
        "UPDATE history SET project = ?1 \
         WHERE source = ?2 AND timestamp_ms = ?3 AND prompt = ?4 AND session_id = ?5 \
           AND project IS NOT ?1",
    )?
    .execute(params![
        entry.project,
        entry.source,
        entry.timestamp_ms,
        entry.prompt,
        session_id,
    ])?;
    ACTIVE.with(|slot| {
        if let Some(active) = slot.borrow_mut().as_mut() {
            active
                .history
                .insert((entry.timestamp_ms, entry.prompt.clone()));
        }
    });
    Ok(())
}

fn sweep(conn: &Connection, active: &Active) -> Result<()> {
    retire_unnoted(
        conn,
        "session_events",
        "event_uid",
        &active.source,
        &active.session_id,
        &json_keys(&active.events),
    )?;
    retire_unnoted(
        conn,
        "tool_calls",
        "tool_use_id",
        &active.source,
        &active.session_id,
        &json_keys(&active.tools),
    )?;
    clear_unconfirmed_tool_errors(
        conn,
        &active.source,
        &active.session_id,
        &json_keys(&active.tool_results),
    )?;
    retire_unnoted(
        conn,
        "file_edits",
        "tool_use_id",
        &active.source,
        &active.session_id,
        &json_keys(&active.edits),
    )?;
    if active.sweep_markers {
        retire_unnoted(
            conn,
            "session_markers",
            "marker_uid",
            &active.source,
            &active.session_id,
            &json_keys(&active.markers),
        )?;
    }
    if active.sweep_relationships {
        retire_grok_subagents(conn, &active.session_id, &json_keys(&active.relationships))?;
    }
    reconcile_history(conn, &active.source, &active.session_id, &active.history)
}

fn json_keys(keys: &HashSet<String>) -> String {
    let listed: Vec<&str> = keys.iter().map(String::as_str).collect();
    serde_json::to_string(&listed).unwrap_or_else(|_| "[]".to_string())
}

/// Rows only this side backed, and that this pass did not write, go away.
/// Rows both sides backed become the other side's. Remote-only rows stay.
fn retire_unnoted(
    conn: &Connection,
    table: &str,
    key: &str,
    source: &str,
    session_id: &str,
    noted: &str,
) -> Result<()> {
    let deleted = conn
        .prepare_cached(&format!(
            "DELETE FROM {table} WHERE source = ?1 AND session_id = ?2 AND location = 'local' \
             AND {key} NOT IN (SELECT value FROM json_each(?3))"
        ))?
        .execute(params![source, session_id, noted])?;
    super::evidence_probe::note_deleted(conn, table, deleted);
    conn.prepare_cached(&format!(
        "UPDATE {table} SET location = 'remote' \
         WHERE source = ?1 AND session_id = ?2 AND location = 'both' \
           AND {key} NOT IN (SELECT value FROM json_each(?3))"
    ))?
    .execute(params![source, session_id, noted])?;
    Ok(())
}

/// A local tool call whose snapshot has no result loses the error flag the
/// previous result left. A call both sides back keeps that flag: the old
/// replace demoted the row and the conflict kept the other side's value.
fn clear_unconfirmed_tool_errors(
    conn: &Connection,
    source: &str,
    session_id: &str,
    confirmed: &str,
) -> Result<()> {
    conn.prepare_cached(
        "UPDATE tool_calls SET is_error = NULL \
         WHERE source = ?1 AND session_id = ?2 AND location = 'local' \
           AND is_error IS NOT NULL \
           AND tool_use_id NOT IN (SELECT value FROM json_each(?3))",
    )?
    .execute(params![source, session_id, confirmed])?;
    Ok(())
}

fn retire_grok_subagents(conn: &Connection, session_id: &str, noted: &str) -> Result<()> {
    conn.prepare_cached(
        "DELETE FROM session_relationships \
         WHERE source = 'grok' AND parent_session_id = ?1 \
           AND evidence_kind = 'grok_subagent_dir' \
           AND relationship_uid NOT IN (SELECT value FROM json_each(?2))",
    )?
    .execute(params![session_id, noted])?;
    Ok(())
}

fn reconcile_history(
    conn: &Connection,
    source: &str,
    session_id: &str,
    kept: &HashSet<(i64, String)>,
) -> Result<()> {
    let mut insert = conn.prepare_cached(
        "INSERT OR IGNORE INTO evidence_reconcile_history_kept (timestamp_ms, prompt) \
         VALUES (?1, ?2)",
    )?;
    for (timestamp_ms, prompt) in kept {
        insert.execute(params![timestamp_ms, prompt])?;
    }
    drop(insert);
    // Same re-attribution as `replace_session_history_scoped`, limited to the
    // ids snapshotted before this read replaced the events it is judged on.
    conn.execute(
        "UPDATE history AS h SET \
           session_id = (SELECT e.session_id FROM session_events e \
                         WHERE e.source = h.source AND e.role = 'user' AND e.kind = 'text' \
                           AND e.session_id <> h.session_id \
                           AND e.ts_ms = h.timestamp_ms AND e.text = h.prompt \
                         ORDER BY e.session_id LIMIT 1), \
           project = (SELECT e.project FROM session_events e \
                      WHERE e.source = h.source AND e.role = 'user' AND e.kind = 'text' \
                        AND e.session_id <> h.session_id \
                        AND e.ts_ms = h.timestamp_ms AND e.text = h.prompt \
                      ORDER BY e.session_id LIMIT 1) \
         WHERE h.id IN (SELECT id FROM evidence_reconcile_owned) \
           AND NOT EXISTS (SELECT 1 FROM evidence_reconcile_history_kept k \
                           WHERE k.timestamp_ms = h.timestamp_ms AND k.prompt = h.prompt) \
           AND EXISTS (SELECT 1 FROM session_events e \
                       WHERE e.source = h.source AND e.role = 'user' AND e.kind = 'text' \
                         AND e.session_id <> h.session_id \
                         AND e.ts_ms = h.timestamp_ms AND e.text = h.prompt)",
        [],
    )?;
    conn.execute(
        "DELETE FROM history \
         WHERE id IN (SELECT id FROM evidence_reconcile_owned) \
           AND source = ?1 AND session_id = ?2 \
           AND NOT EXISTS (SELECT 1 FROM evidence_reconcile_history_kept k \
                           WHERE k.timestamp_ms = history.timestamp_ms \
                             AND k.prompt = history.prompt)",
        params![source, session_id],
    )?;
    Ok(())
}

/// Clear `updates.jsonl` token facts on the Grok rows this local read still
/// owns, except the turns' chosen tails. A tail that moved is a real change;
/// a settled tail stays as the demotion `GROK_TURN_TOKEN_ASSIGN` already left
/// in place.
///
/// Remote-only rows, and rows this pass just handed back to remote, keep the
/// usage they already had. This runs after [`finish`], so a demoted row is
/// already `remote`.
pub(crate) fn clear_grok_turn_tokens_except(
    conn: &Connection,
    session_id: &str,
    keep: &[String],
) -> Result<()> {
    let noted = serde_json::to_string(keep).unwrap_or_else(|_| "[]".to_string());
    conn.prepare_cached(
        "UPDATE session_events SET token_json = NULL \
         WHERE source = 'grok' AND session_id = ?1 \
           AND location <> 'remote' \
           AND json_extract(token_json, '$.source') = 'updates.jsonl' \
           AND event_uid NOT IN (SELECT value FROM json_each(?2))",
    )?
    .execute(params![session_id, noted])?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::{ingest_grok_session, ingest_muse_session, read_muse_transcript, MuseRole};
    use crate::{ChangeKind, ChangeQuery, SessionStore, StoreOptions, Watermark};
    use rusqlite::{params, Connection};
    use serde_json::{json, Value};
    use std::fs;
    use std::path::{Path, PathBuf};

    fn feed(db: &Path, from: Watermark) -> Vec<crate::Change> {
        let store = SessionStore::open(StoreOptions::default().db_path(db)).unwrap();
        store
            .changes_since(from, ChangeQuery::default())
            .unwrap()
            .map(|change| change.unwrap())
            .collect()
    }

    fn head(db: &Path) -> Watermark {
        SessionStore::open(StoreOptions::default().db_path(db))
            .unwrap()
            .head_revision()
            .unwrap()
    }

    fn event_rows(conn: &Connection, source: &str, session: &str) -> Vec<(String, i64, i64)> {
        let mut stmt = conn
            .prepare(
                "SELECT event_uid, id, revision FROM session_events \
                 WHERE source = ?1 AND session_id = ?2 ORDER BY event_uid",
            )
            .unwrap();
        stmt.query_map(params![source, session], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
    }

    fn history_rows(conn: &Connection, source: &str) -> Vec<(String, i64, i64, String)> {
        let mut stmt = conn
            .prepare(
                "SELECT prompt, id, revision, session_id FROM history \
                 WHERE source = ?1 ORDER BY prompt",
            )
            .unwrap();
        stmt.query_map(params![source], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
    }

    fn event_uids(changes: &[crate::Change]) -> Vec<String> {
        changes
            .iter()
            .filter(|change| change.kind == ChangeKind::SessionEvent)
            .map(|change| change.record_key.clone())
            .collect()
    }

    fn history_prompts(changes: &[crate::Change]) -> Vec<String> {
        changes
            .iter()
            .filter(|change| change.kind == ChangeKind::History)
            .filter_map(|change| {
                change
                    .key
                    .iter()
                    .rev()
                    .find_map(|value| value.as_str().map(str::to_string))
            })
            .collect()
    }

    fn write_grok(home: &Path, chat: &str, subagent: Option<&str>) -> PathBuf {
        let dir = home.join(".grok/sessions/proj/grok-stable");
        fs::create_dir_all(&dir).unwrap();
        let chat_path = dir.join("chat_history.jsonl");
        fs::write(&chat_path, chat).unwrap();
        fs::write(
            dir.join("summary.json"),
            r#"{"info":{"id":"grok-stable","cwd":"/tmp/proj"},"created_at":"2026-01-01T00:00:00.000Z"}"#,
        )
        .unwrap();
        let subagents = dir.join("subagents");
        if let Some(body) = subagent {
            fs::create_dir_all(&subagents).unwrap();
            fs::write(subagents.join("child.json"), body).unwrap();
        } else if subagents.exists() {
            fs::remove_dir_all(&subagents).unwrap();
        }
        chat_path
    }

    fn ingest_grok(home: &Path, chat: &Path) -> (PathBuf, Connection) {
        let db = home.join("history.db");
        let conn = crate::open_db(&db).unwrap();
        let session = super::super::scan_grok_session_file(chat).unwrap().unwrap();
        ingest_grok_session(&conn, &session, &chat.to_string_lossy()).unwrap();
        (db, conn)
    }

    /// One appended line is the only evidence the second read reports.
    #[test]
    fn a_grok_append_does_not_rereport_unchanged_evidence() {
        let home = tempfile::tempdir().unwrap();
        let chat = write_grok(
            home.path(),
            concat!(
                r#"{"type":"user","content":"hello","timestamp_ms":1000}"#,
                "\n",
                r#"{"type":"assistant","content":"world","timestamp_ms":2000}"#,
                "\n",
            ),
            None,
        );
        let (db, conn) = ingest_grok(home.path(), &chat);
        let before = event_rows(&conn, "grok", "grok-stable");
        let history_before = history_rows(&conn, "grok");
        assert!(before.len() >= 2, "the first read stored the transcript");
        let hello = history_before
            .iter()
            .find(|row| row.0 == "hello")
            .expect("the prompt is stored");
        let watermark = head(&db);

        write_grok(
            home.path(),
            concat!(
                r#"{"type":"user","content":"hello","timestamp_ms":1000}"#,
                "\n",
                r#"{"type":"assistant","content":"world","timestamp_ms":2000}"#,
                "\n",
                r#"{"type":"user","content":"again","timestamp_ms":3000}"#,
                "\n",
            ),
            None,
        );
        let session = super::super::scan_grok_session_file(&chat)
            .unwrap()
            .unwrap();
        ingest_grok_session(&conn, &session, &chat.to_string_lossy()).unwrap();

        let after = event_rows(&conn, "grok", "grok-stable");
        for (uid, id, revision) in &before {
            let same = after
                .iter()
                .find(|(next, _, _)| next == uid)
                .unwrap_or_else(|| panic!("unchanged event {uid} disappeared"));
            assert_eq!(
                (id, revision),
                (&same.1, &same.2),
                "unchanged event {uid} kept its id and revision"
            );
        }
        let hello_after = history_rows(&conn, "grok")
            .into_iter()
            .find(|row| row.0 == "hello")
            .expect("the original prompt is still stored");
        assert_eq!(hello, &hello_after);

        let changes = feed(&db, watermark);
        let reported = event_uids(&changes);
        for (uid, _, _) in &before {
            assert!(
                !reported.iter().any(|key| key == uid),
                "unchanged event {uid} was reported again: {changes:?}"
            );
        }
        let again = after
            .iter()
            .find(|(uid, _, _)| !before.iter().any(|(old, _, _)| old == uid))
            .expect("the appended line is a new event");
        assert!(
            reported.iter().any(|key| key == &again.0),
            "the appended line is in the feed: {changes:?}"
        );
        assert_eq!(
            history_prompts(&changes),
            vec!["again".to_string()],
            "{changes:?}"
        );
    }

    /// A turn the unified log already settled, and the log event itself, stay
    /// put when the same transcript is read again.
    #[test]
    fn a_grok_reread_keeps_a_settled_turn_token_and_its_unified_event() {
        let home = tempfile::tempdir().unwrap();
        let chat = write_grok(
            home.path(),
            concat!(
                r#"{"type":"user","content":"hello","timestamp_ms":1000}"#,
                "\n",
                r#"{"type":"assistant","content":"world","timestamp_ms":2000}"#,
                "\n",
            ),
            None,
        );
        fs::write(
            chat.parent().unwrap().join("updates.jsonl"),
            concat!(
                r#"{"method":"session/update","params":{"update":{"sessionUpdate":"agent_message_chunk"},"_meta":{"eventId":"a1","agentTimestampMs":1789560001000,"turnStartMs":1789560000000,"turnEndMs":1789560002000}}}"#,
                "\n",
                r#"{"method":"_x.ai/session/update","params":{"update":{"sessionUpdate":"turn_completed","usage":{"inputTokens":1000,"outputTokens":100}},"_meta":{"agentTimestampMs":1789560002000,"turnStartMs":1789560000000,"turnEndMs":1789560002000}}}"#,
                "\n",
            ),
        )
        .unwrap();
        let (db, conn) = ingest_grok(home.path(), &chat);
        conn.execute(
            "INSERT INTO grok_unified_usage \
             (row_key, session_id, ts_ms, usage_json, locator, line_offset) \
             VALUES ('row-1', 'grok-stable', NULL, '{\"inputTokens\":3}', 'unified.jsonl', 0)",
            [],
        )
        .unwrap();
        super::super::grok_unified::materialize_grok_unified_usage(&conn, "grok-stable", None)
            .unwrap();
        let before = event_rows(&conn, "grok", "grok-stable");
        let settled = before
            .iter()
            .find(|(uid, _, _)| uid == "unified:row-1")
            .expect("the unified event is stored");
        let turn = conn
            .query_row(
                "SELECT event_uid, id, revision, token_json FROM session_events \
                 WHERE source = 'grok' AND session_id = 'grok-stable' \
                   AND json_extract(token_json, '$.turn_usage.inputTokens') = 1000",
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .unwrap();
        let watermark = head(&db);

        let session = super::super::scan_grok_session_file(&chat)
            .unwrap()
            .unwrap();
        ingest_grok_session(&conn, &session, &chat.to_string_lossy()).unwrap();

        let after = event_rows(&conn, "grok", "grok-stable");
        let settled_after = after
            .iter()
            .find(|(uid, _, _)| uid == "unified:row-1")
            .expect("the unified event is still stored");
        assert_eq!((settled.1, settled.2), (settled_after.1, settled_after.2));
        let turn_after = after
            .iter()
            .find(|(uid, _, _)| uid == &turn.0)
            .expect("the settled turn is still stored");
        assert_eq!((turn.1, turn.2), (turn_after.1, turn_after.2));
        let reported = event_uids(&feed(&db, watermark));
        assert!(
            !reported
                .iter()
                .any(|uid| uid == "unified:row-1" || uid == &turn.0),
            "a settled re-read reported {reported:?}"
        );
    }

    /// A tool call and a subagent the snapshot no longer contains are gone.
    /// A row the snapshot still contains keeps the id it had.
    #[test]
    fn a_grok_reread_drops_a_removed_tool_call_and_subagent() {
        let home = tempfile::tempdir().unwrap();
        let chat = write_grok(
            home.path(),
            concat!(
                r#"{"type":"user","content":"hello","timestamp_ms":1000}"#,
                "\n",
                r#"{"type":"assistant","content":"world","timestamp_ms":2000}"#,
                "\n",
                r#"{"type":"assistant","content":"","tool_calls":[{"id":"call_late","name":"Shell","arguments":{"command":"ls"}}],"timestamp_ms":3000}"#,
                "\n",
                r#"{"type":"tool_result","tool_call_id":"call_late","content":"done","timestamp_ms":4000}"#,
                "\n",
            ),
            Some(r#"{"session_id":"child-1","name":"worker"}"#),
        );
        let (_db, conn) = ingest_grok(home.path(), &chat);
        let world = event_rows(&conn, "grok", "grok-stable")
            .into_iter()
            .find(|(uid, _, _)| {
                conn.query_row(
                    "SELECT text FROM session_events WHERE source = 'grok' AND event_uid = ?1",
                    [uid],
                    |row| row.get::<_, Option<String>>(0),
                )
                .unwrap()
                    == Some("world".to_string())
            })
            .expect("the assistant line is stored");
        let tools: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM tool_calls \
                 WHERE source = 'grok' AND tool_use_id = 'call_late'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let children: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_relationships \
                 WHERE source = 'grok' AND evidence_kind = 'grok_subagent_dir'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(tools, 1);
        assert_eq!(children, 1);

        write_grok(
            home.path(),
            concat!(
                r#"{"type":"user","content":"hello","timestamp_ms":1000}"#,
                "\n",
                r#"{"type":"assistant","content":"world","timestamp_ms":2000}"#,
                "\n",
            ),
            None,
        );
        let session = super::super::scan_grok_session_file(&chat)
            .unwrap()
            .unwrap();
        ingest_grok_session(&conn, &session, &chat.to_string_lossy()).unwrap();

        let tools: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM tool_calls \
                 WHERE source = 'grok' AND session_id = 'grok-stable'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let children: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_relationships \
                 WHERE source = 'grok' AND parent_session_id = 'grok-stable' \
                   AND evidence_kind = 'grok_subagent_dir'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(tools, 0, "the tool call the transcript dropped is gone");
        assert_eq!(children, 0, "the subagent the directory dropped is gone");
        let kept = event_rows(&conn, "grok", "grok-stable")
            .into_iter()
            .find(|(uid, _, _)| uid == &world.0)
            .expect("the assistant line is still stored");
        assert_eq!((kept.1, kept.2), (world.1, world.2));
    }

    /// A call the snapshot still contains loses the error its result used to
    /// report, and a subagent entry loses metadata the directory dropped.
    /// A local edit loses a branch the snapshot does not carry.
    #[test]
    fn a_grok_reread_drops_fields_the_snapshot_no_longer_carries() {
        let home = tempfile::tempdir().unwrap();
        let chat = write_grok(
            home.path(),
            concat!(
                r#"{"type":"user","content":"hello","timestamp_ms":1000}"#,
                "\n",
                r#"{"type":"assistant","content":"","tool_calls":[{"id":"call_edit","name":"Write","arguments":{"path":"a.txt"}}],"timestamp_ms":2000}"#,
                "\n",
                r#"{"type":"tool_result","tool_call_id":"call_edit","content":"no","is_error":true,"timestamp_ms":3000}"#,
                "\n",
            ),
            Some(r#"{"session_id":"child-1","name":"worker","model":"grok-3"}"#),
        );
        let (_db, conn) = ingest_grok(home.path(), &chat);
        conn.execute(
            "UPDATE file_edits SET git_branch = 'old' \
             WHERE source = 'grok' AND tool_use_id = 'call_edit'",
            [],
        )
        .unwrap();
        let edge_id: i64 = conn
            .query_row(
                "SELECT rowid FROM session_relationships \
                 WHERE source = 'grok' AND evidence_kind = 'grok_subagent_dir'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        write_grok(
            home.path(),
            concat!(
                r#"{"type":"user","content":"hello","timestamp_ms":1000}"#,
                "\n",
                r#"{"type":"assistant","content":"","tool_calls":[{"id":"call_edit","name":"Write","arguments":{"path":"a.txt"}}],"timestamp_ms":2000}"#,
                "\n",
            ),
            Some(r#"{"session_id":"child-1"}"#),
        );
        let session = super::super::scan_grok_session_file(&chat)
            .unwrap()
            .unwrap();
        ingest_grok_session(&conn, &session, &chat.to_string_lossy()).unwrap();

        let is_error: Option<i64> = conn
            .query_row(
                "SELECT is_error FROM tool_calls \
                 WHERE source = 'grok' AND tool_use_id = 'call_edit'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(is_error, None, "the dropped result's error is gone");
        let branch: Option<String> = conn
            .query_row(
                "SELECT git_branch FROM file_edits \
                 WHERE source = 'grok' AND tool_use_id = 'call_edit'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(branch, None, "the edit's branch is the snapshot's");
        let (id, name, model): (i64, Option<String>, Option<String>) = conn
            .query_row(
                "SELECT rowid, child_agent_name, child_model FROM session_relationships \
                 WHERE source = 'grok' AND evidence_kind = 'grok_subagent_dir'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(id, edge_id, "the subagent edge kept its row");
        assert_eq!(name, None);
        assert_eq!(model, None);
    }

    /// A child whose transcript no longer has events tells its parents so.
    #[test]
    fn a_grok_reread_clears_child_has_events_when_the_transcript_is_empty() {
        let home = tempfile::tempdir().unwrap();
        let parent = write_grok(
            home.path(),
            concat!(
                r#"{"type":"user","content":"hello","timestamp_ms":1000}"#,
                "\n",
            ),
            Some(r#"{"session_id":"child-1","name":"worker"}"#),
        );
        let (_db, conn) = ingest_grok(home.path(), &parent);
        let child_dir = home.path().join(".grok/sessions/proj/child-1");
        fs::create_dir_all(&child_dir).unwrap();
        fs::write(
            child_dir.join("summary.json"),
            r#"{"info":{"id":"child-1","cwd":"/tmp/proj"},"created_at":"2026-01-01T00:00:00.000Z"}"#,
        )
        .unwrap();
        let child_chat = child_dir.join("chat_history.jsonl");
        fs::write(
            &child_chat,
            "{\"type\":\"user\",\"content\":\"work\",\"timestamp_ms\":1000}\n",
        )
        .unwrap();
        let child = super::super::scan_grok_session_file(&child_chat)
            .unwrap()
            .unwrap();
        ingest_grok_session(&conn, &child, &child_chat.to_string_lossy()).unwrap();
        let flagged: i64 = conn
            .query_row(
                "SELECT child_has_events FROM session_relationships \
                 WHERE source = 'grok' AND child_session_id = 'child-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(flagged, 1);

        fs::write(&child_chat, "").unwrap();
        let child = super::super::scan_grok_session_file(&child_chat)
            .unwrap()
            .unwrap();
        ingest_grok_session(&conn, &child, &child_chat.to_string_lossy()).unwrap();
        let flagged: i64 = conn
            .query_row(
                "SELECT child_has_events FROM session_relationships \
                 WHERE source = 'grok' AND child_session_id = 'child-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(flagged, 0, "the child no longer has events");
    }

    /// A prompt this session dropped stays in `history` when another session
    /// still has it, filed under that session, with the same id.
    #[test]
    fn a_grok_reread_reattributes_a_prompt_another_session_still_has() {
        let home = tempfile::tempdir().unwrap();
        let chat = write_grok(
            home.path(),
            concat!(
                r#"{"type":"user","content":"shared","timestamp_ms":1000}"#,
                "\n",
                r#"{"type":"assistant","content":"world","timestamp_ms":2000}"#,
                "\n",
            ),
            None,
        );
        let (_db, conn) = ingest_grok(home.path(), &chat);
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('grok', 'other', 1000, 'user', 'text', 'shared', 'other-1')",
            [],
        )
        .unwrap();
        let id = history_rows(&conn, "grok")
            .into_iter()
            .find(|row| row.0 == "shared")
            .map(|row| row.1)
            .unwrap();

        write_grok(
            home.path(),
            concat!(
                r#"{"type":"user","content":"kept","timestamp_ms":3000}"#,
                "\n",
                r#"{"type":"assistant","content":"world","timestamp_ms":2000}"#,
                "\n",
            ),
            None,
        );
        let session = super::super::scan_grok_session_file(&chat)
            .unwrap()
            .unwrap();
        ingest_grok_session(&conn, &session, &chat.to_string_lossy()).unwrap();

        let moved = history_rows(&conn, "grok")
            .into_iter()
            .find(|row| row.0 == "shared")
            .expect("the shared prompt is still stored");
        assert_eq!(moved.1, id);
        assert_eq!(moved.3, "other");
    }

    #[test]
    fn a_token_clear_keeps_usage_on_a_remote_row() {
        let dir = tempfile::tempdir().unwrap();
        let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
        let token = r#"{"source":"updates.jsonl","turn":1}"#;
        for (uid, location) in [
            ("local-e", "local"),
            ("remote-e", "remote"),
            ("both-e", "both"),
        ] {
            conn.execute(
                "INSERT INTO session_events \
                 (source, session_id, ts_ms, role, kind, text, event_uid, token_json, location) \
                 VALUES ('grok', 's', 1, 'assistant', 'text', 'x', ?1, ?2, ?3)",
                params![uid, token, location],
            )
            .unwrap();
        }
        super::clear_grok_turn_tokens_except(&conn, "s", &[]).unwrap();
        let token_of = |uid: &str| -> Option<String> {
            conn.query_row(
                "SELECT token_json FROM session_events WHERE event_uid = ?1",
                [uid],
                |row| row.get(0),
            )
            .unwrap()
        };
        assert_eq!(token_of("local-e"), None);
        assert_eq!(token_of("both-e"), None);
        assert_eq!(token_of("remote-e").as_deref(), Some(token));
    }

    fn muse_record(
        id: &str,
        sequence: i64,
        recorded_at: i64,
        payload_type: &str,
        payload: Value,
    ) -> String {
        json!({
            "schema_version": 1,
            "id": id,
            "stream": {"kind": "session", "id": "muse-stable"},
            "sequence": sequence,
            "recorded_at": recorded_at,
            "payload_type": payload_type,
            "payload": payload,
        })
        .to_string()
    }

    fn write_muse(home: &Path, lines: &[String]) -> PathBuf {
        let path = home.join("session.jsonl");
        fs::write(&path, lines.join("\n") + "\n").unwrap();
        path
    }

    fn ingest_muse(home: &Path, path: &Path) -> (PathBuf, Connection) {
        let db = home.join("history.db");
        let conn = crate::open_db(&db).unwrap();
        let transcript = read_muse_transcript(path).unwrap().unwrap();
        ingest_muse_session(
            &conn,
            &transcript,
            &path.to_string_lossy(),
            MuseRole::Session,
        )
        .unwrap();
        (db, conn)
    }

    fn muse_base() -> Vec<String> {
        vec![
            muse_record(
                "meta",
                1,
                1_700_000_000_000_000,
                "runtime.session.metadata",
                json!({"record": {"workspace_root": "/tmp/proj", "build": {"semver": "0.2.1"}}}),
            ),
            muse_record(
                "prompt-1",
                2,
                1_700_000_001_000_000,
                "runtime.session",
                json!({"kind": "run", "run_id": "run-1", "event": {"kind": "started", "prompt": "hello"}}),
            ),
            muse_record(
                "answer-1",
                3,
                1_700_000_002_000_000,
                "runtime.session",
                json!({
                    "kind": "run",
                    "run_id": "run-1",
                    "event": {
                        "kind": "assistant_message_committed",
                        "message_id": "msg-1",
                        "text": "world"
                    }
                }),
            ),
        ]
    }

    #[test]
    fn a_muse_append_does_not_rereport_unchanged_evidence() {
        let home = tempfile::tempdir().unwrap();
        let path = write_muse(home.path(), &muse_base());
        let (db, conn) = ingest_muse(home.path(), &path);
        let before = event_rows(&conn, "muse", "muse-stable");
        assert!(before.len() >= 2);
        let watermark = head(&db);

        let mut lines = muse_base();
        lines.push(muse_record(
            "answer-2",
            4,
            1_700_000_003_000_000,
            "runtime.session",
            json!({
                "kind": "run",
                "run_id": "run-1",
                "event": {
                    "kind": "assistant_message_committed",
                    "message_id": "msg-2",
                    "text": "later"
                }
            }),
        ));
        write_muse(home.path(), &lines);
        let transcript = read_muse_transcript(&path).unwrap().unwrap();
        ingest_muse_session(
            &conn,
            &transcript,
            &path.to_string_lossy(),
            MuseRole::Session,
        )
        .unwrap();

        let after = event_rows(&conn, "muse", "muse-stable");
        for (uid, id, revision) in &before {
            let same = after
                .iter()
                .find(|(next, _, _)| next == uid)
                .unwrap_or_else(|| panic!("unchanged event {uid} disappeared"));
            assert_eq!((id, revision), (&same.1, &same.2));
        }
        let reported = event_uids(&feed(&db, watermark));
        for (uid, _, _) in &before {
            assert!(
                !reported.iter().any(|key| key == uid),
                "unchanged event {uid} was reported again: {reported:?}"
            );
        }
        assert!(
            reported.iter().any(|key| key == "answer-2"),
            "the appended line is in the feed: {reported:?}"
        );
    }

    #[test]
    fn a_muse_reread_drops_a_removed_tool_call() {
        let home = tempfile::tempdir().unwrap();
        let mut lines = muse_base();
        lines.push(muse_record(
            "tool-1",
            4,
            1_700_000_003_000_000,
            "runtime.session",
            json!({
                "kind": "run",
                "run_id": "run-1",
                "event": {
                    "kind": "assistant_tool_calls_committed",
                    "message_id": "msg-2",
                    "tool_calls": [{
                        "call_id": "c1",
                        "name": "Shell",
                        "args": "{\"command\":\"ls\"}"
                    }]
                }
            }),
        ));
        let path = write_muse(home.path(), &lines);
        let (_db, conn) = ingest_muse(home.path(), &path);
        let tools: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM tool_calls WHERE source = 'muse' AND tool_use_id = 'c1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(tools, 1, "the first read stored the tool call");
        let answer = event_rows(&conn, "muse", "muse-stable")
            .into_iter()
            .find(|(uid, _, _)| uid == "answer-1")
            .unwrap();

        write_muse(home.path(), &muse_base());
        let transcript = read_muse_transcript(&path).unwrap().unwrap();
        ingest_muse_session(
            &conn,
            &transcript,
            &path.to_string_lossy(),
            MuseRole::Session,
        )
        .unwrap();
        let tools: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM tool_calls \
                 WHERE source = 'muse' AND session_id = 'muse-stable'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(tools, 0, "the tool call the transcript dropped is gone");
        let kept = event_rows(&conn, "muse", "muse-stable")
            .into_iter()
            .find(|(uid, _, _)| uid == "answer-1")
            .unwrap();
        assert_eq!((kept.1, kept.2), (answer.1, answer.2));
    }
}
