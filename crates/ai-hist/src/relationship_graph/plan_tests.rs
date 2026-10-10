//! The edge lookups seek, whatever the statistics say.
//!
//! OpenCode records every delegation edge with the one store it was read
//! from as its `evidence_locator`, so a source can hold tens of thousands of
//! edges sharing a locator. A lookup that SQLite answers through the `source`
//! prefix or the locator index reads all of them, and still returns the
//! right rows -- nothing but the plan shows it. Each check runs on a store
//! without statistics (the crate's own) and on one after `ANALYZE`.

use super::*;
use crate::relationship_capture::RETIRE_SUPERSEDED_UNLINKED_SQL;
use rusqlite::{params, StatementStatus};

const STORE: &str = "/home/u/.local/share/opencode/opencode.db";
const EDGES: usize = 3_000;

/// An OpenCode-shaped source: every edge cites the same store, parents fan
/// out to many children.
fn skewed(analyzed: bool) -> Connection {
    skewed_with(analyzed, EDGES)
}

fn skewed_with(analyzed: bool, edges: usize) -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    crate::init_db(&conn).unwrap();
    let mut insert = conn
        .prepare(
            "INSERT INTO session_relationships \
             (source, parent_session_id, relationship_uid, child_session_id, relationship, \
              identity_status, evidence_kind, evidence_locator, created_ms, updated_ms) \
             VALUES ('opencode', ?1, ?2, ?3, ?4, 'observed', 'opencode_parent_id', ?5, 0, 0)",
        )
        .unwrap();
    for index in 0..edges {
        let child = format!("ses_child_{index}");
        insert
            .execute(params![
                format!("ses_parent_{}", index % 40),
                format!("child:{child}"),
                child,
                RELATIONSHIP_DELEGATED,
                STORE
            ])
            .unwrap();
    }
    for (parent, uid, child, kind) in [
        ("ses_a", "continuation:ses_b", Some("ses_b"), RELATIONSHIP_CONTINUATION),
        ("ses_b", "resume:ses_c", Some("ses_c"), RELATIONSHIP_RESUME),
        ("ses_b", "fork:self", Some("ses_b"), RELATIONSHIP_FORK),
        ("ses_target", "child:ses_t1", Some("ses_t1"), RELATIONSHIP_DELEGATED),
        ("ses_target", "child:ses_t2", Some("ses_t2"), RELATIONSHIP_DELEGATED),
        ("ses_parent_3", "child:ses_target", Some("ses_target"), RELATIONSHIP_DELEGATED),
    ] {
        insert.execute(params![parent, uid, child, kind, STORE]).unwrap();
    }
    drop(insert);
    if analyzed {
        conn.execute_batch("ANALYZE").unwrap();
    }
    conn
}

fn plan(conn: &Connection, sql: &str) -> String {
    let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
    let unbound = vec![rusqlite::types::Value::Null; stmt.parameter_count()];
    stmt.query_map(rusqlite::params_from_iter(unbound), |row| row.get::<_, String>(3))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
        .join("\n")
}

#[test]
fn continuity_edges_seek_each_end_of_the_edge() {
    for analyzed in [false, true] {
        let conn = skewed(analyzed);
        let plan = plan(&conn, &continuity_edges_sql(&RelationshipKinds::continuity()));
        assert!(
            plan.lines()
                .any(|line| line.contains("SEARCH") && line.contains("parent_session_id=?")),
            "the parent end seeks (analyzed={analyzed}):\n{plan}"
        );
        assert!(
            plan.lines().any(|line| line
                .contains("USING INDEX idx_session_relationships_child (source=? AND child_session_id=?)")),
            "the child end seeks (analyzed={analyzed}):\n{plan}"
        );
        assert!(
            !plan.lines().any(|line| line.ends_with("(source=?)") || line.contains("SCAN")),
            "no read of the whole source (analyzed={analyzed}):\n{plan}"
        );
    }
}

#[test]
fn a_childs_parents_seek_the_child() {
    for analyzed in [false, true] {
        let conn = skewed(analyzed);
        let plan = plan(&conn, &parents_sql(&RelationshipKinds::delegation()));
        assert!(
            plan.contains("USING INDEX idx_session_relationships_child (source=? AND child_session_id=?)"),
            "analyzed={analyzed}:\n{plan}"
        );
        let parents =
            session_parents(&conn, "opencode", "ses_child_9", &RelationshipKinds::delegation())
                .unwrap();
        assert_eq!(parents.len(), 1);
    }
}

#[test]
fn continuity_edges_return_each_edge_once_from_either_end() {
    let conn = skewed(false);
    let uids = |session| {
        session_continuity_edges(&conn, "opencode", session)
            .unwrap()
            .into_iter()
            .map(|edge| edge.relationship_uid)
            .collect::<Vec<_>>()
    };
    // Child of `ses_a`, parent of `ses_c`, and both ends of its own fork.
    assert_eq!(uids("ses_b"), ["continuation:ses_b", "fork:self", "resume:ses_c"]);
    assert_eq!(uids("ses_c"), ["resume:ses_c"]);
    assert!(uids("ses_parent_1").is_empty());
}

#[test]
fn retiring_a_superseded_unlinked_edge_seeks_the_parent() {
    for analyzed in [false, true] {
        let conn = skewed(analyzed);
        let plan = plan(&conn, RETIRE_SUPERSEDED_UNLINKED_SQL);
        assert!(
            plan.contains("parent_session_id=?") && !plan.contains("evidence_locator=?"),
            "the parent, not the shared locator (analyzed={analyzed}):\n{plan}"
        );
    }
}

/// The delete trigger's work is the deleted session's own edges, not every
/// edge of its source: deleting a session costs the same VM steps beside 300
/// other edges as beside 3,000.
#[test]
fn deleting_a_session_reads_only_its_own_edges() {
    let steps = |conn: &Connection| {
        conn.execute(
            "INSERT INTO sessions (session_id, source) VALUES ('ses_target', 'opencode')",
            [],
        )
        .unwrap();
        let mut delete = conn
            .prepare("DELETE FROM sessions WHERE source = 'opencode' AND session_id = 'ses_target'")
            .unwrap();
        delete.execute([]).unwrap();
        let steps = delete.get_status(StatementStatus::VmStep);
        let left: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM session_relationships \
                 WHERE parent_session_id = 'ses_target' OR child_session_id = 'ses_target'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(left, 0, "the trigger still removes the session's edges");
        steps
    };
    for analyzed in [false, true] {
        let small = steps(&skewed_with(analyzed, EDGES / 10));
        let large = steps(&skewed_with(analyzed, EDGES));
        assert!(
            large < small * 2,
            "{small} VM steps beside {} edges, {large} beside {EDGES} (analyzed={analyzed})",
            EDGES / 10
        );
    }
}

/// The sweep's closing `PRAGMA optimize` analyzes a table it queried that
/// was never analyzed, within the bounded sample.
#[test]
fn refreshing_statistics_analyzes_the_tables_the_connection_used() {
    let conn = skewed(false);
    session_continuity_edges(&conn, "opencode", "ses_b").unwrap();
    crate::store::refresh_planner_statistics(&conn).unwrap();
    let analyzed: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_stat1 WHERE tbl = 'session_relationships'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(analyzed > 0);
}
