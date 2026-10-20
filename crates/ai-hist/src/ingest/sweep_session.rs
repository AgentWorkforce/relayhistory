//! Session presence and catalog project key for one [`super::SweepWrite`].
//!
//! The first row of a `(source, session_id)` in a chunk pays for the
//! presence insert and the `sessions` lookup. Later rows in that chunk bind
//! that lookup. Presence is not repeated after a chunk commit.
//! The assignment is unchanged: catalog key, then the stored event key, then
//! the cwd fallback. A catalog write drops the cached key for that session,
//! so a miss or a cwd key from before the row existed does not stick. The
//! frame is this guard's. Presence stays across the chunk commits it opens.
//! The catalog key does not: each commit releases the writer lock, so the
//! next event reads the catalog again. Drop — commit or rollback — discards
//! the frame. A write with no guard on this connection keeps the per-row
//! insert and the subquery.

use std::cell::RefCell;

use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension};

thread_local! {
    static STACK: RefCell<Vec<Unit>> = const { RefCell::new(Vec::new()) };
}

struct Unit {
    conn: usize,
    sessions: Vec<Entry>,
    #[cfg(test)]
    key_lookups: u64,
    #[cfg(test)]
    presence_inserts: u64,
}

struct Entry {
    source: String,
    session_id: String,
    presence: bool,
    /// `None` until this unit has read the catalog row since the last time
    /// that row was written. The inner key is null when the row has none.
    project: Option<Project>,
}

#[derive(Clone)]
struct Project {
    key: Option<String>,
    method: Option<String>,
}

enum CatalogBind {
    /// No guard for this connection. The statement reads `sessions` itself.
    Subquery,
    Cached(Project),
}

struct Seen {
    presence: bool,
    project: Option<Project>,
}

fn conn_id(conn: &Connection) -> usize {
    std::ptr::from_ref(conn) as usize
}

pub(super) fn arm(conn: &Connection) {
    STACK.with(|stack| {
        stack.borrow_mut().push(Unit {
            conn: conn_id(conn),
            sessions: Vec::new(),
            #[cfg(test)]
            key_lookups: 0,
            #[cfg(test)]
            presence_inserts: 0,
        });
    });
}

pub(super) fn disarm() {
    STACK.with(|stack| {
        let mut stack = stack.borrow_mut();
        debug_assert!(
            !stack.is_empty(),
            "sweep session cache popped with no armed unit"
        );
        stack.pop();
    });
}

/// Drop a cached catalog key. Presence stays: the row was already recorded,
/// and the next event reads whatever the catalog write stored.
pub(super) fn invalidate(conn: &Connection, source: &str, session_id: &str) {
    let id = conn_id(conn);
    STACK.with(|stack| {
        for unit in stack.borrow_mut().iter_mut().filter(|unit| unit.conn == id) {
            if let Some(entry) = unit
                .sessions
                .iter_mut()
                .find(|entry| entry.source == source && entry.session_id == session_id)
            {
                entry.project = None;
            }
        }
    });
}

/// Drop every cached catalog key on this connection. A chunk commit released
/// the writer lock, so the next event has to read the catalog in the new
/// transaction. Presence stays: that insert already committed.
pub(super) fn release_catalog(conn: &Connection) {
    let id = conn_id(conn);
    STACK.with(|stack| {
        for unit in stack.borrow_mut().iter_mut().filter(|unit| unit.conn == id) {
            for entry in &mut unit.sessions {
                entry.project = None;
            }
        }
    });
}

pub(super) fn note_presence(conn: &Connection, source: &str, session_id: &str) -> Result<()> {
    touch(conn, source, session_id).map(|_| ())
}

/// Stamp `project_key` as the row is inserted rather than sweeping for it
/// afterwards. An UPDATE that changes a `session_events` row re-stamps its
/// change-feed revision, so a sweep would report a second upsert for every
/// event of every session on every sync.
///
/// The owning session's key is preferred when the row already exists,
/// because it may be stronger than anything derivable here (a Codex
/// `repository_url`, or a key inherited from a delegating parent). But it
/// is not depended on: the Codex walk writes a rollout's events *before*
/// upserting its session, and a delegated thread never gets a catalog row
/// at all, so a lookup alone would leave those events null and hand them
/// straight back to the sweep this design exists to avoid. Resolving the
/// cwd is the fallback, and it agrees with what `upsert_session` will store
/// for the same directory, so the ordinary case converges with no rewrite.
///
/// On conflict the order is the same but the stored value comes second, so
/// a re-ingest can only ever improve the key. The cwd fallback is last
/// precisely because it is the weakest: for a delegated child there is no
/// `sessions` row to consult, and its stored key is the parent's, inherited
/// by the denormalizing pass. Preferring the incoming value there — which
/// is what a plain `COALESCE(excluded.project_key, ...)` does now that the
/// fallback makes it non-null — would overwrite a canonical repository key
/// with a machine-local path on every re-ingest, and hand every one of
/// those events back to the sweep to fix, reporting a second change-feed
/// upsert each time.
///
/// The *method* is stamped beside the key, always from the same arm that
/// produced it. It is what tells a later pass whether this row worked its
/// key out from its own directory or was lent one, and a row that carries a
/// key with no method is indistinguishable from one that was never
/// resolved: the denormalizing pass would then replace a delegated
/// thread's own repository with its delegator's, on every sync, forever.
///
/// Inside a sweep guard the catalog arm is the cached lookup, bound as
/// `?41` / `?42`, instead of a `sessions` subquery on every row.
pub(super) fn write_event(conn: &Connection, row: &super::EventRow<'_>) -> Result<()> {
    let bound = touch(conn, row.source, row.session_id)?;
    let resolved = row
        .cwd
        .and_then(|cwd| crate::project_identity::identity_for(Some(cwd), None));
    let resolved_key = resolved.as_ref().map(|(key, _)| key.as_str());
    let resolved_method = resolved.as_ref().map(|(_, method)| method.as_str());
    super::evidence_reconcile::note_event(row.source, row.session_id, row.event_uid);
    let catalog = match &bound {
        CatalogBind::Subquery => None,
        CatalogBind::Cached(project) => Some(project),
    };
    execute_event(conn, row, resolved_key, resolved_method, catalog)
}

fn touch(conn: &Connection, source: &str, session_id: &str) -> Result<CatalogBind> {
    let Some(seen) = seen(conn, source, session_id) else {
        crate::mark_session_presence(conn, source, session_id, crate::SessionLocation::Local)?;
        return Ok(CatalogBind::Subquery);
    };
    if let Some(project) = seen.project {
        return Ok(CatalogBind::Cached(project));
    }
    if !seen.presence {
        mark_presence(conn, source, session_id)?;
    }
    let project = lookup_project(conn, source, session_id)?;
    store_project(conn, source, session_id, project.clone());
    Ok(CatalogBind::Cached(project))
}

fn seen(conn: &Connection, source: &str, session_id: &str) -> Option<Seen> {
    let id = conn_id(conn);
    STACK.with(|stack| {
        let stack = stack.borrow();
        let unit = stack.iter().rev().find(|unit| unit.conn == id)?;
        let entry = unit
            .sessions
            .iter()
            .find(|entry| entry.source == source && entry.session_id == session_id);
        Some(match entry {
            Some(entry) => Seen {
                presence: entry.presence,
                project: entry.project.clone(),
            },
            None => Seen {
                presence: false,
                project: None,
            },
        })
    })
}

fn mark_presence(conn: &Connection, source: &str, session_id: &str) -> Result<()> {
    crate::mark_session_presence(conn, source, session_id, crate::SessionLocation::Local)?;
    let id = conn_id(conn);
    STACK.with(|stack| {
        let mut stack = stack.borrow_mut();
        let Some(unit) = stack.iter_mut().rev().find(|unit| unit.conn == id) else {
            return;
        };
        #[cfg(test)]
        {
            unit.presence_inserts += 1;
        }
        if let Some(entry) = unit
            .sessions
            .iter_mut()
            .find(|entry| entry.source == source && entry.session_id == session_id)
        {
            entry.presence = true;
            return;
        }
        unit.sessions.push(Entry {
            source: source.to_string(),
            session_id: session_id.to_string(),
            presence: true,
            project: None,
        });
    });
    Ok(())
}

fn lookup_project(conn: &Connection, source: &str, session_id: &str) -> Result<Project> {
    let found = conn
        .query_row(
            "SELECT project_key, project_key_method FROM sessions \
             WHERE source = ?1 AND session_id = ?2",
            params![source, session_id],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                ))
            },
        )
        .optional()?;
    Ok(found.map_or(
        Project {
            key: None,
            method: None,
        },
        |(key, method)| Project { key, method },
    ))
}

fn store_project(conn: &Connection, source: &str, session_id: &str, project: Project) {
    let id = conn_id(conn);
    STACK.with(|stack| {
        let mut stack = stack.borrow_mut();
        let Some(unit) = stack.iter_mut().rev().find(|unit| unit.conn == id) else {
            return;
        };
        #[cfg(test)]
        {
            unit.key_lookups += 1;
        }
        if let Some(entry) = unit
            .sessions
            .iter_mut()
            .find(|entry| entry.source == source && entry.session_id == session_id)
        {
            entry.project = Some(project);
            return;
        }
        unit.sessions.push(Entry {
            source: source.to_string(),
            session_id: session_id.to_string(),
            presence: true,
            project: Some(project),
        });
    });
}

fn execute_event(
    conn: &Connection,
    row: &super::EventRow<'_>,
    resolved_key: Option<&str>,
    resolved_method: Option<&str>,
    catalog: Option<&Project>,
) -> Result<()> {
    let &super::EventRow {
        source,
        session_id,
        project,
        cwd,
        git_branch,
        message_id,
        parent_id,
        ts_ms,
        role,
        kind,
        text,
        model,
        token_json,
        provider,
        stop_reason,
        identity,
        event_uid,
        tool_result_facts,
        raw_facts,
        raw_kind,
    } = row;
    let blank = super::tool_result_facts::ToolResultFacts::default();
    let tool_result_facts = tool_result_facts.unwrap_or(&blank);
    let sql = if catalog.is_some() {
        super::evidence_reconcile::write_sql(
            &super::evidence_reconcile::SESSION_EVENT_UPSERT_CACHED,
            &super::evidence_reconcile::SESSION_EVENT_REWRITE_CACHED,
        )
    } else {
        super::evidence_reconcile::write_sql(
            &super::evidence_reconcile::SESSION_EVENT_UPSERT,
            &super::evidence_reconcile::SESSION_EVENT_REWRITE,
        )
    };
    let mut stmt = conn.prepare_cached(sql)?;
    // The fixed binds are the statement's `?1`..=`?40`. A cached catalog key
    // is `?41` / `?42` on that statement only.
    macro_rules! event_params {
        ($($extra:expr),*) => {
            rusqlite::params![
                source,
                session_id,
                project,
                cwd,
                git_branch,
                message_id,
                parent_id,
                ts_ms,
                role,
                kind,
                text,
                model,
                token_json,
                provider,
                event_uid,
                resolved_key,
                resolved_method,
                tool_result_facts.tool_use_id,
                tool_result_facts.payload_bytes,
                tool_result_facts.payload_truncated.map(i64::from),
                tool_result_facts.payload_hash,
                tool_result_facts.call_index,
                tool_result_facts.event_index,
                tool_result_facts.result_status,
                tool_result_facts.event_source,
                tool_result_facts.error_signal,
                tool_result_facts.subagent_session_id,
                tool_result_facts.agent_id,
                identity.request_id.or(raw_facts.request_id),
                identity.provider_message_id,
                stop_reason.or(raw_facts.stop_reason),
                raw_facts.agent_version,
                raw_facts.is_sidechain,
                raw_facts.is_meta,
                raw_facts.turn_id,
                raw_facts.request_span,
                super::RAW_MESSAGE_FACTS_VERSION,
                raw_kind,
                raw_facts.control_kind,
                raw_facts.record_token_json,
                $($extra),*
            ]
        };
    }
    if let Some(project) = catalog {
        stmt.execute(event_params![project.key, project.method])?;
    } else {
        stmt.execute(event_params![])?;
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn unit_counts() -> (u64, u64) {
    STACK.with(|stack| {
        let stack = stack.borrow();
        let unit = stack.last().expect("no sweep unit");
        (unit.key_lookups, unit.presence_inserts)
    })
}

#[cfg(test)]
pub(super) fn armed_units() -> usize {
    STACK.with(|stack| stack.borrow().len())
}

#[cfg(test)]
mod tests {
    use super::super::{
        insert_session_event, insert_tool_call, upsert_file_edit_from_call, upsert_session,
        EventRow, SessionCatalogRow, SweepWrite, ToolCallRef,
    };
    use rusqlite::Connection;
    use std::fs;
    use std::path::Path;

    const WEAK_CWD: &str = "/machine-local/not-the-repo";

    fn store() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
        (dir, conn)
    }

    fn event<'a>(session: &'a str, uid: &'a str, cwd: Option<&'a str>) -> EventRow<'a> {
        EventRow {
            source: "claude",
            session_id: session,
            cwd,
            message_id: uid,
            ts_ms: 1,
            role: "user",
            kind: "text",
            text: Some("hello"),
            event_uid: uid,
            ..Default::default()
        }
    }

    fn write_event(conn: &Connection, session: &str, uid: &str, cwd: Option<&str>) {
        insert_session_event(conn, &event(session, uid, cwd)).unwrap();
    }

    fn project_of(conn: &Connection, uid: &str) -> (Option<String>, Option<String>, i64) {
        conn.query_row(
            "SELECT project_key, project_key_method, revision \
             FROM session_events WHERE event_uid = ?1",
            [uid],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap()
    }

    fn count_presence_attempts(conn: &Connection) -> i64 {
        conn.query_row("SELECT count(*) FROM presence_attempts", [], |row| {
            row.get(0)
        })
        .unwrap()
    }

    fn watch_presence_inserts(conn: &Connection) {
        conn.execute_batch(
            "CREATE TABLE presence_attempts (n INTEGER); \
             CREATE TRIGGER presence_attempt BEFORE INSERT ON session_presences BEGIN \
               INSERT INTO presence_attempts VALUES (1); \
             END;",
        )
        .unwrap();
    }

    fn git_repo(dir: &Path) -> String {
        let git = dir.join(".git");
        fs::create_dir_all(&git).unwrap();
        fs::write(
            git.join("config"),
            "[remote \"origin\"]\n\turl = git@github.com:acme/widget.git\n",
        )
        .unwrap();
        dir.to_str().unwrap().to_string()
    }

    #[test]
    fn a_sweep_unit_resolves_a_sessions_project_key_and_presence_once() {
        let (_dir, conn) = store();
        watch_presence_inserts(&conn);
        let write = SweepWrite::begin(&conn).unwrap();
        assert_eq!(super::armed_units(), 1);
        write_event(&conn, "s1", "e1", None);
        write_event(&conn, "s1", "e2", None);
        let call = ToolCallRef {
            source: "claude",
            session_id: "s1",
            message_id: "m",
            tool_use_id: "t1",
            ts_ms: 1,
            ..Default::default()
        };
        insert_tool_call(&conn, &call, "Read", None, "{}", None).unwrap();
        upsert_file_edit_from_call(&conn, &call, "src/main.rs", "Edit").unwrap();
        assert_eq!(super::unit_counts(), (1, 1));
        assert_eq!(count_presence_attempts(&conn), 1);
        let locations = crate::session_locations(&conn, "claude", "s1").unwrap();
        assert_eq!(locations, vec!["local".to_string()]);

        write_event(&conn, "s2", "e3", None);
        assert_eq!(
            super::unit_counts(),
            (2, 2),
            "a second session pays on its own first row"
        );
        write_event(&conn, "s2", "e4", None);
        assert_eq!(super::unit_counts(), (2, 2));
        assert_eq!(count_presence_attempts(&conn), 2);
        assert_eq!(
            crate::session_locations(&conn, "claude", "s2").unwrap(),
            vec!["local".to_string()]
        );
        write.commit().unwrap();
        assert_eq!(super::armed_units(), 0);
    }

    #[test]
    fn a_write_outside_a_unit_marks_presence_on_every_row() {
        let (_dir, conn) = store();
        watch_presence_inserts(&conn);
        assert_eq!(super::armed_units(), 0);
        write_event(&conn, "s1", "e1", None);
        write_event(&conn, "s1", "e2", None);
        assert_eq!(count_presence_attempts(&conn), 2);
        assert_eq!(
            crate::session_locations(&conn, "claude", "s1").unwrap(),
            vec!["local".to_string()]
        );
    }

    #[test]
    fn a_catalog_write_in_the_unit_replaces_a_cached_miss_and_a_weak_key() {
        let (dir, conn) = store();
        let repo = dir.path().join("repo");
        fs::create_dir_all(&repo).unwrap();
        let repo_cwd = git_repo(&repo);
        let (catalog_key, catalog_method) =
            crate::project_identity::identity_for(Some(&repo_cwd), None).unwrap();
        assert_eq!(catalog_method.as_str(), "remote", "{catalog_key}");
        let (weak_key, weak_method) =
            crate::project_identity::identity_for(Some(WEAK_CWD), None).unwrap();
        assert_eq!(weak_method.as_str(), "path");
        assert_ne!(weak_key, catalog_key);

        let write = SweepWrite::begin(&conn).unwrap();
        write_event(&conn, "s1", "e-null", None);
        assert_eq!(project_of(&conn, "e-null").0, None);
        write_event(&conn, "s1", "e-weak", Some(WEAK_CWD));
        assert_eq!(
            project_of(&conn, "e-weak"),
            (
                Some(weak_key.clone()),
                Some("path".to_string()),
                project_of(&conn, "e-weak").2
            )
        );
        assert_eq!(super::unit_counts(), (1, 1));
        let weak_revision = project_of(&conn, "e-weak").2;

        upsert_session(
            &conn,
            &SessionCatalogRow {
                session_id: "s1",
                source: "claude",
                cwd: Some(&repo_cwd),
                git_branch: None,
                first_ts: 1,
                last_ts: 2,
                last_assistant_text: None,
                raw_path: None,
            },
        )
        .unwrap();
        let stored: (String, String) = conn
            .query_row(
                "SELECT project_key, project_key_method FROM sessions \
                 WHERE source = 'claude' AND session_id = 's1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            stored,
            (catalog_key.clone(), catalog_method.as_str().to_string())
        );

        write_event(&conn, "s1", "e-after", None);
        assert_eq!(
            (
                project_of(&conn, "e-after").0,
                project_of(&conn, "e-after").1
            ),
            (
                Some(catalog_key.clone()),
                Some(catalog_method.as_str().to_string())
            ),
            "a cached miss froze a null key"
        );
        write_event(&conn, "s1", "e-weak", Some(WEAK_CWD));
        let upgraded = project_of(&conn, "e-weak");
        assert_eq!(
            (upgraded.0, upgraded.1),
            (
                Some(catalog_key.clone()),
                Some(catalog_method.as_str().to_string())
            ),
            "a weak cwd key stuck after the catalog write"
        );
        assert!(upgraded.2 > weak_revision);
        let reported = upgraded.2;
        write_event(&conn, "s1", "e-weak", Some(WEAK_CWD));
        assert_eq!(project_of(&conn, "e-weak").2, reported);
        assert_eq!(super::unit_counts(), (2, 1));
        write.commit().unwrap();
        assert_eq!(super::armed_units(), 0);
    }

    #[test]
    fn a_reread_keeps_a_repository_key_and_does_not_report_the_event_again() {
        for in_unit in [false, true] {
            let (_dir, conn) = store();
            conn.execute(
                "INSERT INTO sessions \
                 (session_id, source, project_key, project_key_method, discovery_state) \
                 VALUES ('s1', 'claude', 'github.com/acme/widget', 'remote', 'full')",
                [],
            )
            .unwrap();
            let write = in_unit.then(|| SweepWrite::begin(&conn).unwrap());
            write_event(&conn, "s1", "e1", Some(WEAK_CWD));
            let stamped = project_of(&conn, "e1");
            assert_eq!(
                (stamped.0.as_deref(), stamped.1.as_deref()),
                (Some("github.com/acme/widget"), Some("remote")),
                "in_unit={in_unit}"
            );
            write_event(&conn, "s1", "e1", Some(WEAK_CWD));
            assert_eq!(project_of(&conn, "e1"), stamped, "in_unit={in_unit}");
            if let Some(write) = write {
                assert_eq!(super::unit_counts(), (1, 1), "in_unit={in_unit}");
                write.commit().unwrap();
            }
        }
    }

    #[test]
    fn a_cached_miss_does_not_replace_a_stored_repository_key_with_a_cwd_path() {
        let (_dir, conn) = store();
        let (weak_key, weak_method) =
            crate::project_identity::identity_for(Some(WEAK_CWD), None).unwrap();
        assert_eq!(weak_method.as_str(), "path");
        let write = SweepWrite::begin(&conn).unwrap();
        write_event(&conn, "s1", "e1", Some(WEAK_CWD));
        assert_eq!(
            project_of(&conn, "e1").0.as_deref(),
            Some(weak_key.as_str())
        );
        conn.execute(
            "UPDATE session_events \
             SET project_key = 'github.com/acme/widget', project_key_method = 'remote' \
             WHERE event_uid = 'e1'",
            [],
        )
        .unwrap();
        let stamped = project_of(&conn, "e1");
        assert_eq!(stamped.0.as_deref(), Some("github.com/acme/widget"));
        write_event(&conn, "s1", "e1", Some(WEAK_CWD));
        assert_eq!(
            project_of(&conn, "e1"),
            stamped,
            "the cwd fallback replaced the stored repository key"
        );
        assert_eq!(super::unit_counts(), (1, 1));
        write.commit().unwrap();
    }

    #[test]
    fn a_rolled_back_unit_does_not_keep_its_cache() {
        let (_dir, conn) = store();
        {
            let write = SweepWrite::begin(&conn).unwrap();
            write_event(&conn, "s1", "e1", None);
            assert_eq!(super::unit_counts(), (1, 1));
            assert_eq!(super::armed_units(), 1);
            drop(write);
        }
        assert_eq!(super::armed_units(), 0);
        let left: i64 = conn
            .query_row("SELECT count(*) FROM session_events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(left, 0);
        {
            let write = SweepWrite::begin(&conn).unwrap();
            assert_eq!(super::armed_units(), 1);
            write_event(&conn, "s1", "e1", None);
            assert_eq!(super::unit_counts(), (1, 1));
            write.commit().unwrap();
        }
        assert_eq!(super::armed_units(), 0);
        {
            let write = SweepWrite::begin(&conn).unwrap();
            write_event(&conn, "s1", "e2", None);
            assert_eq!(
                super::unit_counts(),
                (1, 1),
                "the next unit reused the previous cache"
            );
            write.commit().unwrap();
        }
    }

    #[test]
    fn a_chunk_commit_rereads_the_catalog_and_keeps_presence() {
        let (_dir, conn) = store();
        conn.execute(
            "INSERT INTO sessions (session_id, source, project_key, project_key_method, discovery_state) \
             VALUES ('s1', 'claude', 'path-key', 'path', 'full')",
            [],
        )
        .unwrap();
        let mut write = SweepWrite::begin(&conn).unwrap();
        write_event(&conn, "s1", "e1", Some(WEAK_CWD));
        let first = project_of(&conn, "e1");
        assert_eq!(first.0.as_deref(), Some("path-key"));
        assert_eq!(first.1.as_deref(), Some("path"));
        assert_eq!(super::unit_counts(), (1, 1));
        for _ in 0..super::super::JSONL_CHUNK_LINES {
            write.record().unwrap();
        }
        conn.execute(
            "UPDATE sessions SET project_key = 'github.com/acme/widget', \
             project_key_method = 'remote' WHERE source = 'claude' AND session_id = 's1'",
            [],
        )
        .unwrap();
        conn.execute(
            "UPDATE session_events SET project_key = 'github.com/acme/widget', \
             project_key_method = 'remote' WHERE event_uid = 'e1'",
            [],
        )
        .unwrap();
        let stamped = project_of(&conn, "e1");
        assert_eq!(stamped.0.as_deref(), Some("github.com/acme/widget"));
        write_event(&conn, "s1", "e1", Some(WEAK_CWD));
        assert_eq!(
            project_of(&conn, "e1"),
            stamped,
            "the cached path key replaced the repository key stored during the lock gap"
        );
        write_event(&conn, "s1", "e2", Some(WEAK_CWD));
        assert_eq!(
            project_of(&conn, "e2").0.as_deref(),
            Some("github.com/acme/widget")
        );
        assert_eq!(super::unit_counts(), (2, 1));
        write.commit().unwrap();
    }
}
