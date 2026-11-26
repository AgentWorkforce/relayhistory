//! SQLite lookup for the effective key used by catalog listings.
//!
//! The persistent triggers only use built-in SQL. A connection without the
//! Rust projection can still write sessions and mark its changed rows dirty.
//! Normal acquisition connections resolve those rows in the same statement.
use anyhow::Result;
use rusqlite::Connection;

const TABLE: &str = "catalog_project_lookup";

pub(crate) fn is_current(conn: &Connection) -> Result<bool> {
    Ok(conn
        .prepare_cached(
            "SELECT count(*) = 6 FROM sqlite_master WHERE name IN (
                'catalog_project_lookup', 'idx_catalog_project_key', 'idx_catalog_project_dirty',
                'catalog_project_insert', 'catalog_project_update', 'catalog_project_delete'
            )",
        )?
        .query_row([], |row| row.get(0))?)
}

pub(crate) fn migrate(conn: &Connection) -> Result<()> {
    if is_current(conn)? {
        return Ok(());
    }
    super::catalog_row::register_catalog_project_key(conn)?;
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS catalog_project_lookup (
            source TEXT NOT NULL,
            session_id TEXT NOT NULL,
            project_key TEXT,
            dirty INTEGER NOT NULL,
            PRIMARY KEY (source, session_id)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS idx_catalog_project_key
            ON catalog_project_lookup(project_key) WHERE dirty = 0 AND project_key IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_catalog_project_dirty
            ON catalog_project_lookup(source, session_id) WHERE dirty = 1;

        DELETE FROM catalog_project_lookup;
        INSERT INTO catalog_project_lookup
            SELECT source, session_id,
                   catalog_project_key(source, project_key_method, repo_url, project_key), 0
            FROM sessions;

        CREATE TRIGGER IF NOT EXISTS catalog_project_insert AFTER INSERT ON sessions BEGIN
            INSERT INTO catalog_project_lookup VALUES (new.source, new.session_id, NULL, 1)
            ON CONFLICT (source, session_id) DO UPDATE SET project_key = NULL, dirty = 1;
        END;
        CREATE TRIGGER IF NOT EXISTS catalog_project_update
        AFTER UPDATE OF source, session_id, project_key, project_key_method, repo_url ON sessions
        WHEN old.source IS NOT new.source OR old.session_id IS NOT new.session_id
          OR old.project_key IS NOT new.project_key OR old.project_key_method IS NOT new.project_key_method
          OR old.repo_url IS NOT new.repo_url
        BEGIN
            DELETE FROM catalog_project_lookup WHERE source = old.source AND session_id = old.session_id;
            INSERT INTO catalog_project_lookup VALUES (new.source, new.session_id, NULL, 1)
            ON CONFLICT (source, session_id) DO UPDATE SET project_key = NULL, dirty = 1;
        END;
        CREATE TRIGGER IF NOT EXISTS catalog_project_delete AFTER DELETE ON sessions BEGIN
            DELETE FROM catalog_project_lookup WHERE source = old.source AND session_id = old.session_id;
        END;
        ",
    )?;
    Ok(())
}

pub(crate) fn configure_writer(conn: &Connection) -> Result<()> {
    super::catalog_row::register_catalog_project_key(conn)?;
    let resolve = format!(
        "UPDATE {TABLE} SET dirty = 0, project_key = (
            SELECT catalog_project_key(source, project_key_method, repo_url, project_key)
            FROM sessions WHERE source = new.source AND session_id = new.session_id
         ) WHERE source = new.source AND session_id = new.session_id;"
    );
    for (name, event) in [
        ("catalog_project_resolve_insert", "INSERT"),
        ("catalog_project_resolve_update", "UPDATE OF dirty"),
    ] {
        conn.execute_batch(&format!(
            "CREATE TEMP TRIGGER IF NOT EXISTS {name} AFTER {event} ON main.{TABLE}
             WHEN new.dirty = 1 BEGIN {resolve} END;"
        ))?;
    }
    Ok(())
}

// The two arms are disjoint. The first seeks the materialized project index.
// The second evaluates only rows changed by connections without the projection.
pub(super) const FROM_PROJECT: &str = "
    FROM (
        SELECT source AS candidate_source, session_id AS candidate_id FROM catalog_project_lookup
        WHERE dirty = 0 AND project_key = ?
        UNION ALL
        SELECT s.source, s.session_id FROM catalog_project_lookup p INDEXED BY idx_catalog_project_dirty
        CROSS JOIN sessions s ON s.source = p.source AND s.session_id = p.session_id
        WHERE p.dirty = 1
          AND catalog_project_key(s.source, s.project_key_method, s.repo_url, s.project_key) = ?
    ) candidates
    CROSS JOIN sessions ON sessions.source = candidates.candidate_source
                       AND sessions.session_id = candidates.candidate_id";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discover::catalog_listing::{catalog_list_query, prepare_project_filter};
    use crate::discover::CatalogListOptions;
    use rusqlite::{params_from_iter, StatementStatus};

    fn probe(conn: &Connection, source: Option<&str>, indexed: bool) -> (String, i32) {
        let options = CatalogListOptions {
            sources: source.into_iter().map(str::to_owned).collect(),
            project_key: Some("recorded.test/Org/Rare".into()),
            limit: Some(1),
            ..Default::default()
        };
        assert_eq!(prepare_project_filter(conn, &options).unwrap(), indexed);
        let (sql, args) = catalog_list_query(&options, indexed);
        let params = || params_from_iter(args.iter().map(|arg| arg.as_ref()));
        let plan = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap()
            .query_map(params(), |row| row.get::<_, String>(3)).unwrap()
            .collect::<rusqlite::Result<Vec<_>>>().unwrap().join(" | ");
        let mut query = conn.prepare(&sql).unwrap();
        let ids = query.query_map(params(), |row| row.get::<_, String>(1)).unwrap()
            .collect::<rusqlite::Result<Vec<_>>>().unwrap();
        assert_eq!(ids, ["rare"]);
        let steps = query.get_status(StatementStatus::VmStep);
        eprintln!("catalog-project source={source:?} indexed={indexed} steps={steps} plan={plan}");
        (plan, steps)
    }

    #[test]
    fn catalog_project_index_bounds_rare_key_work_across_legacy_migration() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("catalog.db");
        drop(crate::store::open_db(&path).unwrap());
        let legacy = Connection::open(&path).unwrap();
        legacy.execute_batch(
            "DROP TRIGGER catalog_project_insert;
             DROP TRIGGER catalog_project_update;
             DROP TRIGGER catalog_project_delete;
             DROP TABLE catalog_project_lookup;
             WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i + 1 FROM n WHERE i < 50000)
             INSERT INTO sessions (source, session_id, last_activity_ms, project_key, project_key_method, repo_url)
             SELECT CASE i % 2 WHEN 0 THEN 'codex' ELSE 'claude' END,
                    printf('common-%d', i), i, 'common.test/Org/App', 'remote',
                    'https://common.test/Org/App.git' FROM n;
             INSERT INTO sessions (source, session_id, last_activity_ms, project_key, project_key_method, repo_url)
             VALUES ('codex', 'rare', 0, 'stale.test/Org/App', 'remote',
                'https://user:fixture@recorded.test/Org/Rare.git?token=fixture');
             ANALYZE;",
        ).unwrap();
        drop(legacy);

        let reader = crate::store::open_db_readonly(&path).unwrap();
        let legacy_steps = [None, Some("codex")].map(|source| probe(&reader, source, false).1);
        assert!(legacy_steps.into_iter().all(|steps| steps > 100_000));

        let writer = crate::store::open_db(&path).unwrap();
        writer.execute_batch("ANALYZE;").unwrap();
        for source in [None, Some("codex")] {
            let (plan, steps) = probe(&reader, source, true);
            assert!(plan.contains("idx_catalog_project_key") && plan.contains("SEARCH sessions USING"), "{plan}");
            assert!(steps < 1_000, "{steps}: {plan}");
        }

        writer.execute(
            "UPDATE sessions SET repo_url = 'https://recorded.test/Org/Updated.git' WHERE session_id = 'rare'",
            [],
        ).unwrap();
        let dirty_count = |conn: &Connection| conn.query_row(
            "SELECT count(*) FROM catalog_project_lookup WHERE dirty = 1", [], |row| row.get::<_, i64>(0),
        ).unwrap();
        assert_eq!(dirty_count(&reader), 0);
        let key: String = reader.query_row(
            "SELECT project_key FROM catalog_project_lookup WHERE session_id = 'rare'", [], |row| row.get(0),
        ).unwrap();
        assert_eq!(key, "recorded.test/Org/Updated");

        let raw = Connection::open(&path).unwrap();
        raw.execute(
            "UPDATE sessions SET repo_url = 'https://recorded.test/Org/Rare.git' WHERE session_id = 'rare'",
            [],
        ).unwrap();
        assert_eq!(dirty_count(&reader), 1);
        assert!(probe(&reader, Some("codex"), true).1 < 1_000);
    }
}
