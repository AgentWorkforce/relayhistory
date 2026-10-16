//! The transcript a catalog row already names keeps that row.
//!
//! Forks and resumed sessions are sibling transcripts that share one session
//! id. While `sessions.raw_path` names a file that is still there, a different
//! transcript does not replace the row's identity. The same file still updates
//! it, and a missing file lets the transcript being read claim the row.

use crate::discover::ShallowSession;
use crate::observations::ObservationKey;
use crate::SessionLocation;
use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension};
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Read;
use std::path::Path;

thread_local! {
    static RAW_PATH_CACHE: RefCell<Vec<RawPathCache>> = const { RefCell::new(Vec::new()) };
}

struct RawPathCache {
    conn: usize,
    source: String,
    paths: HashMap<String, Option<String>>,
    codex_rollouts: HashMap<String, Option<String>>,
}

fn conn_id(conn: &Connection) -> usize {
    std::ptr::from_ref(conn) as usize
}

/// Read one source's catalog owners in one statement and reuse them for a
/// bounded sweep. The cached value is deliberately the path at sweep start:
/// each incoming transcript is judged against the owner that preceded this
/// pass, not against an earlier candidate from the same pass.
pub(crate) fn with_raw_path_cache<T>(
    conn: &Connection,
    source: &str,
    sessions: &[(String, Option<String>)],
    run: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let session_ids: Vec<_> = sessions.iter().map(|(id, _)| id.clone()).collect();
    let ids = serde_json::to_string(&session_ids)?;
    let mut paths: HashMap<String, Option<String>> =
        session_ids.iter().cloned().map(|id| (id, None)).collect();
    let mut stmt = conn.prepare_cached(
        "SELECT session_id,raw_path FROM sessions WHERE source=?1 \
         AND session_id IN (SELECT value FROM json_each(?2))",
    )?;
    let rows = stmt.query_map(params![source, ids], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
    })?;
    for row in rows {
        let (id, path) = row?;
        paths.insert(id, path.filter(|path| !path.is_empty()));
    }
    RAW_PATH_CACHE.with(|cache| {
        cache.borrow_mut().push(RawPathCache {
            conn: conn_id(conn),
            source: source.to_string(),
            paths,
            codex_rollouts: sessions.iter().cloned().collect(),
        });
    });
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            RAW_PATH_CACHE.with(|cache| {
                cache.borrow_mut().pop();
            });
        }
    }
    let _restore = Restore;
    run()
}

/// Whether `incoming` is a different transcript from the one `sessions.raw_path`
/// already names, and that file is still present.
///
/// Same path, no stored path (including NULL), a shared SQLite store, and a
/// stored path that is no longer a file all return false so the caller writes
/// normally. A Codex SQLite projection may also claim its own incomplete
/// rollout. OpenCode and Devin name their store however it was configured;
/// the file stays on disk after it drops a session, and the session's next
/// store claims the row. A sibling JSON transcript keeps its catalog row.
pub(crate) fn incoming_must_not_replace(
    conn: &Connection,
    source: &str,
    session_id: &str,
    incoming: Option<&str>,
) -> Result<bool> {
    let Some(incoming) = incoming.filter(|path| !path.is_empty()) else {
        return Ok(false);
    };
    let Some(stored) = catalog_raw_path(conn, source, session_id)? else {
        return Ok(false);
    };
    if stored == incoming || shared_sqlite_store(source, &stored) {
        return Ok(false);
    }
    // The bounded Codex selection already joined each thread to its
    // projection checkpoint and proved the current rollout incomplete. Reuse
    // that sweep snapshot instead of reopening both Codex databases for every
    // catalog upsert. A different live sibling still keeps ownership.
    if source == "codex" && incoming.starts_with("sqlite:") {
        let rollout = RAW_PATH_CACHE.with(|cache| {
            cache
                .borrow()
                .iter()
                .rev()
                .find(|entry| entry.conn == conn_id(conn) && entry.source == source)
                .and_then(|entry| entry.codex_rollouts.get(session_id).cloned())
        });
        if let Some(rollout) = rollout {
            return Ok(crate::ingest::transcript_file::exists(Path::new(&stored))
                && rollout.as_deref() != Some(stored.as_str()));
        }
    }
    // A projection may replace its own incomplete rollout, but not a live
    // sibling transcript that happens to share the session id.
    if source == "codex"
        && incoming.starts_with("sqlite:")
        && crate::providers::codex::sqlite::incomplete_projection_owns_rollout(incoming, &stored)?
    {
        return Ok(false);
    }
    // A compressed Codex rollout is present under its plain locator.
    Ok(crate::ingest::transcript_file::exists(Path::new(&stored)))
}

/// Whether the catalog row's `raw_path` is `path`.
///
/// The catalog upsert has already kept or claimed the row, so this is a
/// compare and not another stat.
pub(crate) fn recorded_path_is(
    conn: &Connection,
    source: &str,
    session_id: &str,
    path: &str,
) -> Result<bool> {
    Ok(catalog_raw_path(conn, source, session_id)?.as_deref() == Some(path))
}

/// `raw_path` as stored, or `None` when the row is missing or the column is
/// NULL or empty.
fn catalog_raw_path(conn: &Connection, source: &str, session_id: &str) -> Result<Option<String>> {
    if let Some(path) = RAW_PATH_CACHE.with(|cache| {
        cache
            .borrow()
            .iter()
            .rev()
            .find(|entry| entry.conn == conn_id(conn) && entry.source == source)
            .and_then(|entry| entry.paths.get(session_id).cloned())
    }) {
        return Ok(path);
    }
    let stored = conn
        .query_row(
            "SELECT raw_path FROM sessions WHERE source = ?1 AND session_id = ?2",
            params![source, session_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()?;
    Ok(stored.flatten().filter(|path| !path.is_empty()))
}

/// A configured OpenCode or Devin store, recognized by its header so the name
/// does not matter. Sixteen bytes, and only when a different path is asking
/// to replace one of those sources.
fn shared_sqlite_store(source: &str, path: &str) -> bool {
    if !matches!(source, "opencode" | "devin") {
        return false;
    }
    let mut header = [0_u8; 16];
    let Ok(mut file) = std::fs::File::open(path) else {
        return false;
    };
    file.read_exact(&mut header).is_ok() && &header == b"SQLite format 3\0"
}

/// SQL fragments for a full-ingest catalog upsert.
///
/// When `keep` is set, the recorded path and its assistant excerpt stay. The
/// caller's activity assignment is unchanged either way.
pub(crate) fn session_assigns<'a>(
    keep: bool,
    assistant_text: &'a str,
    raw_path: Option<&'a str>,
) -> (&'a str, &'static str, Option<&'a str>) {
    if keep {
        (
            "last_assistant_text = sessions.last_assistant_text",
            "raw_path = sessions.raw_path",
            None,
        )
    } else {
        (
            assistant_text,
            "raw_path = COALESCE(excluded.raw_path, sessions.raw_path)",
            raw_path,
        )
    }
}

/// The shallow row to write, or `None` when this read may update identity.
///
/// Only a local transcript can own `sessions.raw_path`. A remote upsert keeps
/// the locator it carried.
pub(crate) fn withhold_shallow(
    conn: &Connection,
    session: &ShallowSession,
    location: SessionLocation,
) -> Result<Option<ShallowSession>> {
    if location == SessionLocation::Local
        && incoming_must_not_replace(
            conn,
            &session.source,
            &session.session_id,
            session.raw_path.as_deref(),
        )?
    {
        Ok(Some(without_foreign_identity(session)))
    } else {
        Ok(None)
    }
}

/// A shallow row that leaves stored identity columns alone.
///
/// `COALESCE(excluded, stored)` keeps the stored value when the incoming one
/// is null, which is how a sibling read avoids replacing the recorded file.
pub(crate) fn without_foreign_identity(session: &ShallowSession) -> ShallowSession {
    ShallowSession {
        raw_path: None,
        first_prompt: None,
        title: None,
        models: Vec::new(),
        agent_version: None,
        source_stamp: None,
        last_assistant_text: None,
        ..session.clone()
    }
}

/// Locator, stamp, and previews to write when the observation already points
/// at the catalog file and the incoming locator is a different transcript.
///
/// No stat: the catalog upsert in this same write decided whether that file
/// still owns the row. An observation whose locator is not the catalog path
/// (a database session id, a remote connector) is left alone, as is a missing
/// observation, so the incoming locator is inserted.
pub(crate) struct RetainedObservation {
    pub(crate) raw_locator: Option<String>,
    pub(crate) source_stamp: Option<String>,
    pub(crate) first_prompt: Option<String>,
    pub(crate) last_assistant_text: Option<String>,
}

pub(crate) fn retained_observation(
    conn: &Connection,
    key: &ObservationKey,
    incoming_locator: Option<&str>,
    keep: bool,
) -> Result<Option<RetainedObservation>> {
    if !keep {
        return Ok(None);
    }
    let incoming = incoming_locator.unwrap_or("");
    conn.query_row(
        "SELECT o.raw_locator, o.source_stamp, o.first_prompt, o.last_assistant_text \
         FROM session_observations o \
         JOIN sessions s ON s.source = o.source AND s.session_id = o.session_id \
         WHERE o.source = ?1 AND o.session_id = ?2 AND o.location = ?3 \
           AND o.connector_id = ?4 AND o.connector_instance = ?5 \
           AND o.raw_locator IS NOT NULL \
           AND o.raw_locator = s.raw_path \
           AND o.raw_locator <> ?6",
        params![
            key.source,
            key.session_id,
            key.location.as_str(),
            key.connector_id,
            key.connector_instance,
            incoming
        ],
        |row| {
            Ok(RetainedObservation {
                raw_locator: row.get(0)?,
                source_stamp: row.get(1)?,
                first_prompt: row.get(2)?,
                last_assistant_text: row.get(3)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_compressed_rollout_keeps_catalog_ownership_from_sqlite() {
        let dir = tempfile::tempdir().unwrap();
        let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
        let rollout = dir.path().join("sibling.jsonl");
        let compressed = crate::ingest::transcript_file::compressed_path(&rollout);
        std::fs::write(&compressed, b"stored transcript").unwrap();
        conn.execute(
            "INSERT INTO sessions (source, session_id, raw_path) VALUES ('codex', 'shared', ?1)",
            [rollout.to_string_lossy().as_ref()],
        )
        .unwrap();
        assert!(incoming_must_not_replace(
            &conn,
            "codex",
            "shared",
            Some("sqlite:/elsewhere/state_5.sqlite#shared")
        )
        .unwrap());
        std::fs::remove_file(compressed).unwrap();
        assert!(!incoming_must_not_replace(
            &conn,
            "codex",
            "shared",
            Some("sqlite:/elsewhere/state_5.sqlite#shared")
        )
        .unwrap());
    }

    #[test]
    fn sqlite_projection_replaces_only_its_own_incomplete_rollout() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join(".codex");
        let sessions = root.join("sessions");
        std::fs::create_dir_all(&sessions).unwrap();
        let rollout = sessions.join("own.jsonl");
        let sibling = sessions.join("sibling.jsonl");
        std::fs::write(&rollout, b"short").unwrap();
        std::fs::write(&sibling, b"sibling").unwrap();
        let state = root.join("state_5.sqlite");
        let history = root.join("thread_history_1.sqlite");
        let state_db = Connection::open(&state).unwrap();
        state_db
            .execute_batch("CREATE TABLE threads (id TEXT, rollout_path TEXT, history_mode TEXT);")
            .unwrap();
        state_db
            .execute(
                "INSERT INTO threads VALUES ('shared', ?1, 'paginated')",
                [rollout.to_string_lossy().as_ref()],
            )
            .unwrap();
        let history_db = Connection::open(&history).unwrap();
        history_db
            .execute_batch("CREATE TABLE thread_history_projection_state (thread_id TEXT, next_rollout_byte_offset INTEGER);")
            .unwrap();
        history_db
            .execute(
                "INSERT INTO thread_history_projection_state VALUES ('shared', 100)",
                [],
            )
            .unwrap();
        let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
        conn.execute(
            "INSERT INTO sessions (source, session_id, raw_path) VALUES ('codex', 'shared', ?1)",
            [rollout.to_string_lossy().as_ref()],
        )
        .unwrap();
        let incoming = crate::providers::codex::sqlite::locator(&state, "shared");
        assert!(!incoming_must_not_replace(&conn, "codex", "shared", Some(&incoming)).unwrap());
        conn.execute(
            "UPDATE sessions SET raw_path=?1 WHERE source='codex' AND session_id='shared'",
            [sibling.to_string_lossy().as_ref()],
        )
        .unwrap();
        assert!(incoming_must_not_replace(&conn, "codex", "shared", Some(&incoming)).unwrap());
        conn.execute(
            "UPDATE sessions SET raw_path=?1 WHERE source='codex' AND session_id='shared'",
            [rollout.to_string_lossy().as_ref()],
        )
        .unwrap();
        history_db
            .execute(
                "UPDATE thread_history_projection_state SET next_rollout_byte_offset=1",
                [],
            )
            .unwrap();
        assert!(incoming_must_not_replace(&conn, "codex", "shared", Some(&incoming)).unwrap());
        history_db
            .execute(
                "UPDATE thread_history_projection_state SET next_rollout_byte_offset=100",
                [],
            )
            .unwrap();
        let newer_state = root.join("state_6.sqlite");
        Connection::open(&newer_state).unwrap();
        let newer_locator = crate::providers::codex::sqlite::locator(&newer_state, "shared");
        // A newer schema is not admitted until discovery supports it.
        assert!(incoming_must_not_replace(&conn, "codex", "shared", Some(&newer_locator)).unwrap());
        assert!(incoming_must_not_replace(&conn, "codex", "shared", Some(&incoming)).unwrap());
        std::fs::remove_file(newer_state).unwrap();
        assert!(!incoming_must_not_replace(&conn, "codex", "shared", Some(&incoming)).unwrap());

        // A broken projection must not prevent an existing rollout from
        // keeping its catalog row, or a missing rollout from yielding it.
        state_db.execute_batch("DROP TABLE threads").unwrap();
        assert!(incoming_must_not_replace(&conn, "codex", "shared", Some(&incoming)).unwrap());
        std::fs::remove_file(&rollout).unwrap();
        assert!(!incoming_must_not_replace(&conn, "codex", "shared", Some(&incoming)).unwrap());
    }
}
