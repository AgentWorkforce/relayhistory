//! One write transaction over a transcript's worth of sweep work.

use anyhow::Result;
use rusqlite::Connection;

/// One write transaction over a transcript's worth of sweep work.
///
/// The global Claude and Codex walks used to write in autocommit, which at the
/// default `synchronous = FULL` is one WAL `fsync` per statement -- several
/// per record, and two thirds of a cold sweep's samples. Every other provider
/// already writes a unit at a time (Cursor per source, Grok per session
/// directory, Muse per session, hydration per session); this is the same
/// thing for the two that did not.
///
/// It changes when rows become visible, not what is durable: each commit is
/// as durable as an autocommit was, the change-feed revisions are stamped by
/// the same per-row triggers, and every cursor these walks publish is written
/// inside the transaction, after the rows it vouches for, so a crash leaves
/// the rows and their cursor together or neither. `.sync-state.json` is still
/// checkpointed only after the source finishes, so a stamp never runs ahead of
/// the commit it describes.
///
/// `IMMEDIATE`, so the writer lock is taken before the first read of a
/// cursor rather than on the first write, where a deferred transaction that
/// raced a hydration would fail to upgrade instead of waiting behind it. A
/// caller that is already inside a transaction keeps its own boundaries:
/// nothing here begins, commits or rolls back on its behalf.
///
/// Bounded as the ADR on direct writers requires: [`SweepWrite::record`]
/// commits and reopens every [`super::JSONL_CHUNK_LINES`] records, so a very
/// large transcript holds the lock for one chunk at a time, not for its whole
/// read. A chunk committed ahead of the transcript's cursor is what autocommit
/// did for every statement; the re-read that follows upserts over it.
///
/// New `session_events` rows are copied into `session_events_fts` once, in the
/// statement before each commit, instead of by the per-row insert trigger.
/// Writes outside a unit still go through that trigger. An update or delete
/// during the unit keeps its own trigger: a row the unit has not indexed yet
/// is not deleted from an index that does not hold it, and the catch-up does
/// not index a row the update trigger already indexed or a row that was deleted.
pub(crate) struct SweepWrite<'c> {
    conn: &'c Connection,
    /// Whether this guard opened the transaction and so owns its boundaries.
    owned: bool,
    records: usize,
    /// `session_events.id` high-water when this chunk started deferring the
    /// full-text index. `None` when this connection has no deferral flag.
    fts_after: Option<i64>,
}

impl<'c> SweepWrite<'c> {
    pub(crate) fn begin(conn: &'c Connection) -> Result<Self> {
        let owned = conn.is_autocommit();
        if owned {
            conn.execute_batch("BEGIN IMMEDIATE")?;
        }
        let fts_after = match arm_session_events_fts(conn) {
            Ok(after) => after,
            Err(error) => {
                if owned && !conn.is_autocommit() {
                    let _ = conn.execute_batch("ROLLBACK");
                }
                return Err(error);
            }
        };
        // Presence and the catalog project key are read once for this guard,
        // including across the chunk commits it opens. Drop clears them.
        super::sweep_session::arm(conn);
        Ok(Self {
            conn,
            owned,
            records: 0,
            fts_after,
        })
    }

    /// One record is about to be read. Past the chunk size, commit what is
    /// written and open the next chunk.
    pub(crate) fn record(&mut self) -> Result<()> {
        if !self.owned {
            return Ok(());
        }
        self.records += 1;
        if self.records < super::JSONL_CHUNK_LINES {
            return Ok(());
        }
        self.records = 0;
        self.rotate()
    }

    pub(crate) fn commit(mut self) -> Result<()> {
        let after = self.fts_after.take();
        if !self.owned {
            catch_up_session_events_fts(self.conn, after)?;
            return Ok(());
        }
        // Whether or not COMMIT succeeded the guard no longer owns a
        // transaction: a successful commit ended it, and a failed one is
        // ended here. SQLite can refuse a COMMIT (a busy commit, say) and
        // leave the transaction open, and the sweep goes on to the next
        // provider on this same connection. Left open, that transaction
        // would swallow the next provider's writes and let it publish
        // cursors for rows that vanish when the connection closes.
        self.owned = false;
        if let Err(error) = catch_up_session_events_fts(self.conn, after).and_then(|()| {
            self.conn
                .execute_batch("COMMIT")
                .map_err(anyhow::Error::from)
        }) {
            if !self.conn.is_autocommit() {
                let _ = self.conn.execute_batch("ROLLBACK");
            }
            return Err(error);
        }
        Ok(())
    }

    /// Index this chunk, commit it, and open the next one already deferring.
    fn rotate(&mut self) -> Result<()> {
        let after = self.fts_after.take();
        catch_up_session_events_fts(self.conn, after)?;
        self.conn.execute_batch("COMMIT")?;
        if let Err(error) = self.conn.execute_batch("BEGIN IMMEDIATE") {
            // Committed and not reopened: there is nothing left to own, and
            // the walk ends on the error rather than writing in autocommit.
            self.owned = false;
            return Err(error.into());
        }
        self.fts_after = arm_session_events_fts(self.conn)?;
        Ok(())
    }
}

impl Drop for SweepWrite<'_> {
    fn drop(&mut self) {
        super::sweep_session::disarm();
        // An error or a cancellation unwound past the guard: nothing of the
        // open chunk is kept. Chunks a Claude transcript committed before it
        // stay (its catalog row and the rows of those chunks), but its cursor
        // was never published, so the next sweep reads the file again and
        // its upserts converge on the same rows. SQLite may already have
        // rolled back on its own.
        if self.owned && !self.conn.is_autocommit() {
            let _ = self.conn.execute_batch("ROLLBACK");
            return;
        }
        // The caller owns the transaction and may still commit it. Index
        // rows have to be in that transaction; a rollback takes them with
        // the events.
        if let Some(after) = self.fts_after.take() {
            let _ = catch_up_session_events_fts(self.conn, Some(after));
        }
    }
}

/// Defer `session_events_fts` inserts until [`catch_up_session_events_fts`].
///
/// The high-water is read before the flag is set, so a row inserted in
/// between would be indexed by the trigger and skipped by the catch-up.
/// `None` when this connection is not a history store: the sweep guard is
/// also used on bare databases in tests.
fn arm_session_events_fts(conn: &Connection) -> Result<Option<i64>> {
    let ready: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master \
         WHERE type = 'table' AND name = 'session_events_fts_defer')",
        [],
        |row| row.get(0),
    )?;
    if !ready {
        return Ok(None);
    }
    let after: i64 = conn.query_row(
        "SELECT COALESCE(MAX(id), 0) FROM session_events",
        [],
        |row| row.get(0),
    )?;
    conn.execute(
        "INSERT INTO session_events_fts_defer (singleton) VALUES (1)",
        [],
    )?;
    Ok(Some(after))
}

/// Copy rows inserted after `after` into `session_events_fts` in one
/// statement, then clear the deferral flag. A row an update already indexed,
/// or a row that was deleted, is not in the select.
fn catch_up_session_events_fts(conn: &Connection, after: Option<i64>) -> Result<()> {
    let Some(after) = after else {
        return Ok(());
    };
    conn.execute(
        "INSERT INTO session_events_fts(rowid, text, role, project) \
         SELECT e.id, e.text, e.role, e.project \
         FROM session_events e \
         WHERE e.id > ?1 \
           AND NOT EXISTS ( \
             SELECT 1 FROM session_events_fts_docsize d WHERE d.id = e.id \
           )",
        [after],
    )?;
    conn.execute("DELETE FROM session_events_fts_defer", [])?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history_search::{search_all, SearchRole, MATCH_SOURCE_SESSION_EVENT};
    use crate::{open_db, QueryFilter, SessionScope};
    use rusqlite::params;

    fn store() -> (tempfile::TempDir, std::path::PathBuf, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.db");
        let conn = open_db(&path).unwrap();
        (dir, path, conn)
    }

    fn insert(conn: &Connection, uid: &str, text: &str) {
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, project, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 's1', 'proj', 1, 'user', 'text', ?1, ?2)",
            params![text, uid],
        )
        .unwrap();
    }

    fn hits(conn: &Connection, term: &str) -> usize {
        search_all(
            conn,
            &[term.to_string()],
            false,
            &QueryFilter {
                limit: 20,
                scope: SessionScope::All,
                ..Default::default()
            },
            SearchRole::All,
        )
        .unwrap()
        .into_iter()
        .filter(|row| row.match_source == MATCH_SOURCE_SESSION_EVENT)
        .count()
    }

    fn fts_hits(conn: &Connection, term: &str) -> i64 {
        let query = format!("{{text project}} : ({term})");
        conn.query_row(
            "SELECT count(*) FROM session_events_fts WHERE session_events_fts MATCH ?1",
            [query],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn defer_rows(conn: &Connection) -> i64 {
        conn.query_row("SELECT count(*) FROM session_events_fts_defer", [], |row| {
            row.get(0)
        })
        .unwrap()
    }

    fn assert_index_intact(conn: &Connection) {
        conn.execute_batch(
            "INSERT INTO session_events_fts(session_events_fts) VALUES('integrity-check')",
        )
        .unwrap();
        let (events, indexed): (i64, i64) = conn
            .query_row(
                "SELECT (SELECT count(*) FROM session_events), \
                        (SELECT count(*) FROM session_events_fts_docsize)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(events, indexed, "every surviving event is indexed once");
    }

    #[test]
    fn a_write_outside_a_sweep_unit_indexes_immediately() {
        let (_dir, _path, conn) = store();
        insert(&conn, "e1", "nowterm");
        assert_eq!(hits(&conn, "nowterm"), 1);
        assert_eq!(fts_hits(&conn, "nowterm"), 1);
        assert_eq!(defer_rows(&conn), 0);
        assert_index_intact(&conn);
    }

    #[test]
    fn a_sweep_unit_indexes_its_new_events_once_on_commit() {
        let (_dir, path, conn) = store();
        insert(&conn, "e0", "alreadyterm");
        let write = SweepWrite::begin(&conn).unwrap();
        insert(&conn, "e1", "freshterm");
        insert(&conn, "e2", "secondterm");
        assert_eq!(
            hits(&conn, "freshterm"),
            0,
            "indexed before the unit committed"
        );
        assert_eq!(hits(&conn, "alreadyterm"), 1);
        write.commit().unwrap();
        assert!(conn.is_autocommit());
        assert_eq!(hits(&conn, "freshterm"), 1);
        assert_eq!(hits(&conn, "secondterm"), 1);
        assert_eq!(
            fts_hits(&conn, "freshterm"),
            1,
            "the catch-up indexed it twice"
        );
        assert_eq!(defer_rows(&conn), 0);
        assert_index_intact(&conn);

        let reader = Connection::open(&path).unwrap();
        assert_eq!(hits(&reader, "freshterm"), 1);
        assert_eq!(hits(&reader, "secondterm"), 1);
    }

    #[test]
    fn a_row_changed_inside_the_unit_is_indexed_once_and_a_deleted_one_is_not() {
        let (_dir, _path, conn) = store();
        insert(&conn, "e0", "keptterm before");
        let write = SweepWrite::begin(&conn).unwrap();
        insert(&conn, "e1", "freshterm");
        insert(&conn, "e2", "editterm original");
        insert(&conn, "e3", "goneterm");
        insert(&conn, "e4", "swapterm");
        conn.execute(
            "UPDATE session_events SET text = 'editterm revised' WHERE event_uid = 'e2'",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, project, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 's1', 'proj', 1, 'user', 'text', 'editterm replaced', 'e2') \
             ON CONFLICT(source, session_id, event_uid) DO UPDATE SET text = excluded.text",
            [],
        )
        .unwrap();
        conn.execute("DELETE FROM session_events WHERE event_uid = 'e3'", [])
            .unwrap();
        conn.execute(
            "UPDATE session_events SET text = 'swapterm gone' WHERE event_uid = 'e4'",
            [],
        )
        .unwrap();
        conn.execute("DELETE FROM session_events WHERE event_uid = 'e4'", [])
            .unwrap();
        conn.execute(
            "UPDATE session_events SET text = 'keptterm after' WHERE event_uid = 'e0'",
            [],
        )
        .unwrap();
        assert_eq!(hits(&conn, "freshterm"), 0);
        assert_eq!(hits(&conn, "original"), 0);
        assert_eq!(hits(&conn, "revised"), 0);
        assert_eq!(hits(&conn, "replaced"), 1);
        assert_eq!(hits(&conn, "goneterm"), 0);
        assert_eq!(hits(&conn, "swapterm"), 0);
        assert_eq!(hits(&conn, "gone"), 0);
        assert_eq!(hits(&conn, "before"), 0);
        assert_eq!(hits(&conn, "after"), 1);
        write.commit().unwrap();
        assert_eq!(hits(&conn, "freshterm"), 1);
        assert_eq!(fts_hits(&conn, "freshterm"), 1);
        assert_eq!(hits(&conn, "replaced"), 1);
        assert_eq!(fts_hits(&conn, "replaced"), 1);
        assert_eq!(hits(&conn, "revised"), 0);
        assert_eq!(hits(&conn, "original"), 0);
        assert_eq!(hits(&conn, "goneterm"), 0);
        assert_eq!(hits(&conn, "swapterm"), 0);
        assert_eq!(hits(&conn, "gone"), 0);
        assert_eq!(hits(&conn, "before"), 0);
        assert_eq!(hits(&conn, "after"), 1);
        assert_eq!(fts_hits(&conn, "after"), 1);
        assert_eq!(defer_rows(&conn), 0);
        let events: i64 = conn
            .query_row("SELECT count(*) FROM session_events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(events, 3, "kept, fresh, and the revised row");
        assert_index_intact(&conn);
    }

    #[test]
    fn a_dropped_sweep_unit_indexes_nothing() {
        let (_dir, _path, conn) = store();
        insert(&conn, "e0", "keptterm");
        let write = SweepWrite::begin(&conn).unwrap();
        insert(&conn, "e1", "rolledterm");
        assert_eq!(hits(&conn, "rolledterm"), 0);
        drop(write);
        assert!(conn.is_autocommit());
        assert_eq!(hits(&conn, "rolledterm"), 0);
        assert_eq!(hits(&conn, "keptterm"), 1);
        assert_eq!(defer_rows(&conn), 0);
        let events: i64 = conn
            .query_row("SELECT count(*) FROM session_events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(events, 1);
        assert_index_intact(&conn);
    }

    #[test]
    fn a_caller_transaction_rolls_the_units_index_back_with_its_rows() {
        let (_dir, path, conn) = store();
        let tx = conn.unchecked_transaction().unwrap();
        let write = SweepWrite::begin(&conn).unwrap();
        insert(&conn, "e1", "outerterm");
        write.commit().unwrap();
        assert!(!conn.is_autocommit(), "the caller's transaction was ended");
        assert_eq!(hits(&conn, "outerterm"), 1);
        let reader = Connection::open(&path).unwrap();
        assert_eq!(
            hits(&reader, "outerterm"),
            0,
            "visible before the caller committed"
        );
        tx.rollback().unwrap();
        assert_eq!(hits(&conn, "outerterm"), 0);
        assert_eq!(defer_rows(&conn), 0);
        assert_index_intact(&conn);
    }

    #[test]
    fn a_committed_chunk_is_searchable_and_the_next_chunk_is_not_until_it_commits() {
        let (_dir, path, conn) = store();
        let mut write = SweepWrite::begin(&conn).unwrap();
        insert(&conn, "e1", "chunkterm");
        for _ in 0..super::super::JSONL_CHUNK_LINES {
            write.record().unwrap();
        }
        let reader = Connection::open(&path).unwrap();
        assert_eq!(hits(&reader, "chunkterm"), 1);
        insert(&conn, "e2", "tailterm");
        assert_eq!(hits(&conn, "tailterm"), 0);
        drop(write);
        assert_eq!(hits(&reader, "chunkterm"), 1);
        assert_eq!(hits(&Connection::open(&path).unwrap(), "tailterm"), 0);
        assert_eq!(defer_rows(&conn), 0);
        assert_index_intact(&conn);
    }

    #[test]
    fn an_existing_database_gains_the_deferred_insert_trigger() {
        let (_dir, path, conn) = store();
        conn.execute_batch(
            "DROP TABLE session_events_fts_defer; \
             DELETE FROM schema_migrations WHERE name = 'session_events_fts_write_unit_v1'; \
             DROP TRIGGER session_events_ai; \
             DROP TRIGGER session_events_au; \
             DROP TRIGGER session_events_ad; \
             CREATE TRIGGER session_events_ai AFTER INSERT ON session_events BEGIN \
                 INSERT INTO session_events_fts(rowid, text, role, project) \
                 VALUES (new.id, new.text, new.role, new.project); \
             END; \
             CREATE TRIGGER session_events_au AFTER UPDATE OF text, role, project ON session_events \
             WHEN old.text IS NOT new.text OR old.role IS NOT new.role OR old.project IS NOT new.project \
             BEGIN \
                 INSERT INTO session_events_fts(session_events_fts, rowid, text, role, project) \
                 VALUES('delete', old.id, old.text, old.role, old.project); \
                 INSERT INTO session_events_fts(rowid, text, role, project) \
                 VALUES (new.id, new.text, new.role, new.project); \
             END; \
             CREATE TRIGGER session_events_ad AFTER DELETE ON session_events BEGIN \
                 INSERT INTO session_events_fts(session_events_fts, rowid, text, role, project) \
                 VALUES('delete', old.id, old.text, old.role, old.project); \
             END;",
        )
        .unwrap();
        assert!(!crate::schema_is_current(&conn).unwrap());
        drop(conn);
        let conn = open_db(&path).unwrap();
        let insert_sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = 'session_events_ai'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            insert_sql.contains("session_events_fts_defer"),
            "{insert_sql}"
        );
        let update_sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = 'session_events_au'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            update_sql.contains("UPDATE OF text, role, project"),
            "{update_sql}"
        );
        assert!(update_sql.contains("IS NOT"), "{update_sql}");
        assert!(
            update_sql.contains("session_events_fts_docsize"),
            "{update_sql}"
        );
        insert(&conn, "e1", "migratedterm");
        assert_eq!(hits(&conn, "migratedterm"), 1);
        assert_index_intact(&conn);
    }
}
