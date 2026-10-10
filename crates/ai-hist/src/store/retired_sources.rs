//! Sources an earlier release stored and this one retired, and the migration
//! that removes what they left behind.

use anyhow::{Context, Result};
use rusqlite::{params, Connection};

/// Sources earlier releases stored and this one no longer reads: `trajectory`
/// (records swept from `.trajectories` directories and `ai-hist learn`
/// roll-ups) and `relay` (Relaycast-backed prompts). Their rows are deleted
/// on a writable open; see [`delete_retired_source_rows`].
pub(crate) const RETIRED_SOURCES: &[&str] = &["trajectory", "relay"];

/// Whether `source` is one of [`RETIRED_SOURCES`].
pub fn is_retired_source(source: &str) -> bool {
    RETIRED_SOURCES.contains(&source)
}

/// The `IN (...)` list of [`RETIRED_SOURCES`], for SQL.
fn retired_sources_sql() -> String {
    RETIRED_SOURCES
        .iter()
        .map(|source| format!("'{source}'"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The trajectory store an earlier release kept, and the Python CLI's
/// full-text index over it with the triggers that maintained it. Nothing in
/// this release defines or reads any of them.
const RETIRED_TRAJECTORY_OBJECTS: &[(&str, &str)] = &[
    ("trigger", "trajectories_ai"),
    ("trigger", "trajectories_au"),
    ("trigger", "trajectories_ad"),
    ("table", "trajectory_fts"),
    ("table", "trajectories"),
];

/// The tables this crate creates that key a row by `source`, and so can hold
/// a row under a [`RETIRED_SOURCES`] source. An explicit list rather than
/// every table with a `source` column: the upload daemon keeps its own
/// tables in this database (`delivery_journal`, `delivery_shadow`,
/// `delivery_exclusions`, `history_subscriptions`), and they are its records,
/// never ours to delete from. `sessions` is first: its delete triggers clear
/// the per-session state keyed on it. Every one has an index whose leading
/// column is `source`, so a presence probe is one seek.
const EVIDENCE_TABLES: &[&str] = &[
    "sessions",
    "session_presences",
    "history",
    "session_events",
    "tool_calls",
    "file_edits",
    "session_markers",
    "session_relationships",
    "session_continuity_evidence",
    "session_commit_links",
    "session_tags",
    "session_hydration_checkpoints",
    "session_identity_correlations",
    "transcript_cursors",
    "discovery_skips",
    "session_observations",
    "observation_evidence",
    "observation_versions",
    "observation_hydration_checkpoints",
    "observation_discovery_skips",
    "canonical_evidence_protection",
];

/// Whether table `name` exists. A database mid-migration may not have every
/// [`EVIDENCE_TABLES`] entry yet.
fn table_exists(conn: &Connection, name: &str) -> Result<bool> {
    Ok(conn
        .prepare_cached("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1")?
        .exists([name])?)
}

/// Whether `table` holds a row under a [`RETIRED_SOURCES`] source.
fn holds_retired_rows(conn: &Connection, table: &str) -> Result<bool> {
    if !table_exists(conn, table)? {
        return Ok(false);
    }
    let sources = retired_sources_sql();
    Ok(conn.query_row(
        &format!("SELECT EXISTS(SELECT 1 FROM \"{table}\" WHERE source IN ({sources}))"),
        [],
        |row| row.get(0),
    )?)
}

/// Whether anything [`drop_retired_trajectory_store`] or
/// [`delete_retired_source_rows`] removes is still there: a
/// [`RETIRED_TRAJECTORY_OBJECTS`] object, or a row under a
/// [`RETIRED_SOURCES`] source in any [`EVIDENCE_TABLES`] table -- including
/// evidence no catalog row names, such as a sidechain's events. Checked by
/// presence rather than a migration marker, so what an older client writes
/// again is retired again on the next writable open, and a read-only handle
/// treats the database as stale until then.
pub(super) fn retired_sources_present(conn: &Connection) -> Result<bool> {
    let mut object =
        conn.prepare_cached("SELECT 1 FROM sqlite_master WHERE type = ? AND name = ? LIMIT 1")?;
    for (kind, name) in RETIRED_TRAJECTORY_OBJECTS {
        if object.exists(params![kind, name])? {
            return Ok(true);
        }
    }
    for table in EVIDENCE_TABLES {
        if holds_retired_rows(conn, table)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Drop the trajectory store an earlier release kept.
///
/// The Python-era index goes first, triggers before their table:
/// `trajectory_fts` is keyed on the implicit rowid of a table with a TEXT
/// primary key, which a VACUUM may renumber, so on a Python-era database the
/// index can disagree with its content table, and the `'delete'` in its
/// triggers then fails with `SQLITE_CORRUPT_VTAB` on the first write to an
/// affected row. Then the `trajectories` table, and the change-feed triggers
/// on it with it.
pub(super) fn drop_retired_trajectory_store(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "DROP TRIGGER IF EXISTS trajectories_ai;
         DROP TRIGGER IF EXISTS trajectories_au;
         DROP TRIGGER IF EXISTS trajectories_ad;
         DROP TABLE IF EXISTS trajectory_fts;
         DROP TABLE IF EXISTS trajectories;",
    )
    .context("dropping the retired trajectory store")
}

/// Delete every row an earlier release stored under a [`RETIRED_SOURCES`]
/// source from the [`EVIDENCE_TABLES`], so nothing is left behind under a
/// source this build cannot name.
///
/// Runs last in the migration pass, once every table has its current columns
/// and triggers: a delete fires them, and a trigger body written for a column
/// a later step adds would otherwise fail. Only a table holding such a row is
/// written: a delete compiles every trigger on its table.
///
/// Each delete leaves its change-feed tombstone, deliberately. On the first
/// upgrade the feed has already reset its stream (the `trajectory` kind is
/// gone), and a delete for a row a consumer never received is a no-op to it;
/// on a later pass -- rows an older client wrote again, which the feed
/// delivered -- the tombstone is how a consumer learns to drop them. Only the
/// tombstones of the retired `trajectory` kind go: no drain reads that kind.
pub(super) fn delete_retired_source_rows(conn: &Connection) -> Result<()> {
    let sources = retired_sources_sql();
    for table in EVIDENCE_TABLES {
        if holds_retired_rows(conn, table)? {
            conn.execute(
                &format!("DELETE FROM \"{table}\" WHERE source IN ({sources})"),
                [],
            )
            .with_context(|| format!("deleting retired-source rows from {table}"))?;
        }
    }
    conn.execute(
        "DELETE FROM evidence_tombstones WHERE kind = 'trajectory'",
        [],
    )
    .context("deleting retired-kind tombstones")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::{needs_migration, open_db, schema_is_current};
    use super::*;

    /// The `trajectories` table as an earlier release created it.
    const LEGACY_TRAJECTORIES: &str = "
        CREATE TABLE trajectories (
            id TEXT PRIMARY KEY,
            version INTEGER,
            persona_id TEXT,
            project_id TEXT,
            task_title TEXT,
            task_description TEXT,
            status TEXT,
            started_at TEXT,
            completed_at TEXT,
            decisions_json TEXT NOT NULL,
            retrospective_json TEXT NOT NULL,
            search_text TEXT NOT NULL,
            path TEXT,
            updated_ms INTEGER NOT NULL,
            timestamp_ms INTEGER NOT NULL,
            revision INTEGER NOT NULL DEFAULT 0
        );";

    /// The Python CLI's `trajectory_fts` and the triggers that maintained it,
    /// verbatim.
    const PYTHON_TRAJECTORY_INDEX: &str = "
        CREATE VIRTUAL TABLE trajectory_fts USING fts5(
            search_text, task_title, task_description, persona_id, project_id,
            content='trajectories', content_rowid='rowid'
        );
        CREATE TRIGGER trajectories_ai AFTER INSERT ON trajectories BEGIN
            INSERT INTO trajectory_fts(rowid, search_text, task_title, task_description, persona_id, project_id)
            VALUES (new.rowid, new.search_text, new.task_title, new.task_description, new.persona_id, new.project_id);
        END;
        CREATE TRIGGER trajectories_au AFTER UPDATE ON trajectories BEGIN
            INSERT INTO trajectory_fts(trajectory_fts, rowid, search_text, task_title, task_description, persona_id, project_id)
            VALUES('delete', old.rowid, old.search_text, old.task_title, old.task_description, old.persona_id, old.project_id);
            INSERT INTO trajectory_fts(rowid, search_text, task_title, task_description, persona_id, project_id)
            VALUES (new.rowid, new.search_text, new.task_title, new.task_description, new.persona_id, new.project_id);
        END;
        CREATE TRIGGER trajectories_ad AFTER DELETE ON trajectories BEGIN
            INSERT INTO trajectory_fts(trajectory_fts, rowid, search_text, task_title, task_description, persona_id, project_id)
            VALUES('delete', old.rowid, old.search_text, old.task_title, old.task_description, old.persona_id, old.project_id);
        END;";

    fn trajectory_objects(conn: &Connection) -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE name IN \
             ('trajectories', 'trajectory_fts', 'trajectories_ai', 'trajectories_au', \
              'trajectories_ad')",
            [],
            |row| row.get(0),
        )
        .unwrap()
    }

    #[test]
    fn a_python_era_trajectory_index_out_of_step_does_not_block_migration() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.db");
        {
            let conn = open_db(&path).unwrap();
            conn.execute_batch(LEGACY_TRAJECTORIES).unwrap();
            conn.execute_batch(
                "INSERT INTO trajectories (id, decisions_json, retrospective_json, search_text, \
                     updated_ms, timestamp_ms) VALUES ('t1', '[]', '{}', 'alpha bravo charlie', 1, 1);
                 DELETE FROM schema_migrations WHERE name = 'change_feed_v2';",
            )
            .unwrap();
            conn.execute_batch(PYTHON_TRAJECTORY_INDEX).unwrap();
            // An index that disagrees with its content row, holding fewer
            // tokens than the row, so the trigger's 'delete' underflows.
            conn.execute_batch(
                "INSERT INTO trajectory_fts(rowid, search_text)
                 SELECT rowid, 'beta' FROM trajectories WHERE id = 't1';",
            )
            .unwrap();
            // The failure this guards against: any write to the row trips the
            // legacy trigger's 'delete' against the mismatched index.
            let tripped = conn
                .execute("UPDATE trajectories SET updated_ms = 2 WHERE id = 't1'", [])
                .unwrap_err();
            assert!(tripped.to_string().contains("malformed"), "{tripped}");
        }

        let conn = open_db(&path).unwrap();
        assert!(schema_is_current(&conn).unwrap());
        assert_eq!(trajectory_objects(&conn), 0);
    }

    #[test]
    fn a_trajectory_store_recreated_after_migration_is_retired_again() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.db");
        {
            let conn = open_db(&path).unwrap();
            conn.execute_batch(LEGACY_TRAJECTORIES).unwrap();
            conn.execute_batch(PYTHON_TRAJECTORY_INDEX).unwrap();
        }
        let conn = open_db(&path).unwrap();
        assert_eq!(trajectory_objects(&conn), 0);
    }

    /// A database an earlier release filled with trajectory and Relaycast
    /// rows opens with every one of them gone -- the trajectory store, the
    /// prompts and catalog rows under both sources and the state keyed on
    /// them, and their change-feed tombstones -- and every other source's rows
    /// untouched.
    #[test]
    fn retired_source_rows_are_deleted_on_a_writable_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("history.db");
        {
            let conn = open_db(&path).unwrap();
            conn.execute_batch(LEGACY_TRAJECTORIES).unwrap();
            conn.execute_batch(
                "INSERT INTO trajectories (id, decisions_json, retrospective_json, search_text, \
                     updated_ms, timestamp_ms) VALUES ('t1', '[]', '{}', 'needle', 1, 1);
                 INSERT INTO sessions (session_id, source, discovery_state) VALUES
                     ('t1', 'trajectory', 'full'),
                     ('r1', 'relay', 'shallow'),
                     ('c1', 'claude', 'shallow');
                 INSERT INTO session_presences (source, session_id, location) VALUES
                     ('trajectory', 't1', 'remote'),
                     ('relay', 'r1', 'local'),
                     ('claude', 'c1', 'local');
                 INSERT INTO history (source, session_id, project, prompt, prompt_hash, timestamp_ms)
                 VALUES
                     ('trajectory', 't1', 'p', 'needle trajectory', 'h1', 1),
                     ('relay', 'r1', 'p', 'needle relay', 'h2', 2),
                     ('relay', NULL, 'p', 'needle relay, no session', 'h3', 3),
                     ('claude', 'c1', 'p', 'needle claude', 'h4', 4);
                 INSERT INTO tags (id, name, display_name, created_ms, updated_ms)
                 VALUES (1, 'kept', 'kept', 1, 1);
                 INSERT INTO session_tags (source, session_id, tag_id, created_ms)
                 VALUES ('relay', 'r1', 1, 1);
                 -- Evidence no catalog row names: a sidechain's tool call.
                 INSERT INTO tool_calls (source, session_id, tool_use_id, name)
                 VALUES ('relay', 'orphan', 't1', 'Bash');
                 -- The upload daemon's own table, which is not ours.
                 CREATE TABLE delivery_journal (
                     seq INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL,
                     source TEXT NOT NULL, session_id TEXT, record_key TEXT NOT NULL,
                     operation TEXT NOT NULL, payload TEXT NOT NULL
                 );
                 INSERT INTO delivery_journal (kind, source, record_key, operation, payload)
                 VALUES ('history', 'relay', 'k', 'upsert', '{}');
                 INSERT INTO evidence_tombstones (kind, source, session_id, record_key, revision)
                 VALUES ('trajectory', 'trajectory', 't0', 't0', 1),
                        ('history', 'relay', '', '[1,\"gone\"]', 2),
                        ('history', 'claude', '', '[1,\"gone\"]', 3);",
            )
            .unwrap();
        }
        let conn = open_db(&path).unwrap();
        assert!(schema_is_current(&conn).unwrap());
        assert_eq!(trajectory_objects(&conn), 0);
        let remaining = |sql: &str| -> Vec<String> {
            conn.prepare(sql)
                .unwrap()
                .query_map([], |row| row.get::<_, String>(0))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap()
        };
        let sources = |table: &str| {
            remaining(&format!(
                "SELECT DISTINCT source FROM {table} ORDER BY source"
            ))
        };
        for table in ["sessions", "session_presences", "history"] {
            assert_eq!(sources(table), vec!["claude"], "{table}");
        }
        assert!(sources("session_tags").is_empty());
        assert!(
            sources("tool_calls").is_empty(),
            "orphaned evidence goes too"
        );
        assert_eq!(
            sources("delivery_journal"),
            vec!["relay"],
            "the upload daemon's tables are left alone"
        );
        // The deletes leave tombstones, so a consumer drops what it holds;
        // only the retired kind's own tombstones go.
        assert_eq!(
            remaining("SELECT DISTINCT kind FROM evidence_tombstones WHERE kind = 'trajectory'"),
            Vec::<String>::new()
        );
        let tombstoned =
            remaining("SELECT DISTINCT kind || ':' || source FROM evidence_tombstones ORDER BY 1");
        for expected in [
            "history:claude",
            "history:relay",
            "history:trajectory",
            "session:relay",
            "tool_call:relay",
        ] {
            assert!(
                tombstoned.iter().any(|t| t == expected),
                "{expected}: {tombstoned:?}"
            );
        }
        assert_eq!(
            remaining("SELECT prompt FROM history ORDER BY id"),
            vec!["needle claude"]
        );
        // The prompt index followed the deletes.
        assert_eq!(
            remaining(
                "SELECT h.prompt FROM history_fts f JOIN history h ON h.id = f.rowid \
                 WHERE history_fts MATCH 'needle'"
            ),
            vec!["needle claude"]
        );
        assert!(!retired_sources_present(&conn).unwrap());
        // A row an older client writes again makes the schema stale until the
        // next writable open retires it.
        conn.execute(
            "INSERT INTO tool_calls (source, session_id, tool_use_id, name) \
             VALUES ('trajectory', 'again', 't1', 'Bash')",
            [],
        )
        .unwrap();
        assert!(!schema_is_current(&conn).unwrap());
        drop(conn);
        let conn = open_db(&path).unwrap();
        assert!(schema_is_current(&conn).unwrap());
        drop(conn);
        // A second open finds nothing left to do.
        let conn = open_db(&path).unwrap();
        assert!(!needs_migration(&conn).unwrap());
    }
}
