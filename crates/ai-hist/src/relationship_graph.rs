//! Recorded delegation topology and bounded traversal over it.
//!
//! A relationship row is evidence, not inference: it says which provider
//! artifact established the link, whether the child has a stable identity of
//! its own, and whether that child's events are independently addressable.
//! Nothing here fabricates a child id, and no traversal ever rewrites a
//! child's events as its parent's.

use anyhow::Result;
use rusqlite::{params, Connection, Row};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Bump whenever relationship object shapes or semantics require an SDK change.
pub const SESSION_RELATIONSHIP_CONTRACT_VERSION: u32 = 1;

pub const DEFAULT_TREE_MAX_DEPTH: u32 = 32;
pub const MAX_TREE_MAX_DEPTH: u32 = 64;
pub const DEFAULT_TREE_MAX_NODES: u32 = 1_000;
pub const MAX_TREE_MAX_NODES: u32 = 10_000;
pub const DEFAULT_CHILDREN_PAGE_LIMIT: i64 = 100;
pub const MAX_CHILDREN_PAGE_LIMIT: i64 = 1_000;

/// A child observed with a provider-recorded identity of its own.
pub const IDENTITY_OBSERVED: &str = "observed";
/// Related evidence the provider records without a stable child identity.
pub const IDENTITY_UNLINKED: &str = "unlinked";

const RELATIONSHIP_COLUMNS: &str = "source, parent_session_id, child_session_id, relationship, \
     identity_status, child_agent_type, child_agent_name, child_model, spawn_depth, \
     evidence_kind, evidence_locator, evidence_ref, child_has_events, \
     spawned_at_ms, created_ms, relationship_uid";

/// Nulls sort last, matching the catalog's null-timestamps-at-the-tail
/// convention. `relationship_uid` is unique per parent, so this is a total
/// order and a keyset cursor over it can never drop or repeat a row.
const CHILD_ORDER: &str = "ORDER BY spawned_at_ms IS NULL, spawned_at_ms ASC, relationship_uid ASC";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionRelationship {
    pub source: String,
    pub parent_session_id: String,
    /// `None` when the provider recorded related evidence without a stable
    /// child identity. Never synthesized from a file name.
    pub child_session_id: Option<String>,
    pub relationship: String,
    pub identity_status: String,
    pub child_agent_type: Option<String>,
    pub child_agent_name: Option<String>,
    pub child_model: Option<String>,
    pub spawn_depth: Option<i64>,
    pub evidence_kind: String,
    pub evidence_locator: Option<String>,
    pub evidence_ref: Option<String>,
    pub child_has_events: bool,
    pub spawned_at_ms: Option<i64>,
    pub created_ms: i64,
    pub relationship_uid: String,
}

impl SessionRelationship {
    fn is_unlinked(&self) -> bool {
        self.child_session_id.is_none() || self.identity_status == IDENTITY_UNLINKED
    }
}

/// What a provider's records can and cannot establish about delegation.
///
/// Consumers read this instead of guessing why a field is null: a Codex
/// subagent always has its own thread id, a Claude subagent has one only on
/// provider versions that emit `agentId`, and the remaining providers record
/// no delegation at all.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RelationshipCapabilities {
    pub source: String,
    /// `always` | `sometimes` | `never`
    pub stable_child_identity: String,
    pub records_agent_type: bool,
    pub records_spawn_time: bool,
    pub records_evidence_locator: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RelationshipDiagnostic {
    pub code: String,
    pub message: String,
    pub relationship_uid: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionRelationships {
    pub contract_version: u32,
    pub source: String,
    pub session_id: String,
    pub as_parent: Vec<SessionRelationship>,
    pub as_child: Vec<SessionRelationship>,
    pub capabilities: RelationshipCapabilities,
    pub diagnostics: Vec<RelationshipDiagnostic>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionTreeNode {
    pub source: String,
    pub session_id: String,
    pub depth: u32,
    pub parent_session_id: Option<String>,
    /// The edge that reached this node. `None` for the root.
    pub relationship: Option<SessionRelationship>,
    /// Child relationships with a traversable identity. Unlinked evidence is
    /// reported through [`SessionTree::unlinked`] instead.
    pub child_count: u32,
    pub has_events: bool,
    /// Children exist but were not expanded (depth/node budget or cycle).
    pub truncated: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct SessionTreeOptions {
    pub max_depth: u32,
    pub max_nodes: u32,
}

impl Default for SessionTreeOptions {
    fn default() -> Self {
        Self {
            max_depth: DEFAULT_TREE_MAX_DEPTH,
            max_nodes: DEFAULT_TREE_MAX_NODES,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionTree {
    pub contract_version: u32,
    pub source: String,
    pub root_session_id: String,
    /// Pre-order, deterministic. Always contains the root as `nodes[0]`.
    pub nodes: Vec<SessionTreeNode>,
    /// Related evidence with no stable child identity, at any depth.
    pub unlinked: Vec<SessionRelationship>,
    pub capabilities: RelationshipCapabilities,
    pub diagnostics: Vec<RelationshipDiagnostic>,
    /// A budget stopped the walk short of the recorded evidence. Skipping a
    /// session already present in the tree is not truncation: nothing is
    /// missing from the result.
    pub truncated: bool,
    pub max_depth_reached: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RelationshipCursor {
    pub spawned_at_ms: Option<i64>,
    pub relationship_uid: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionChildrenPage {
    pub children: Vec<SessionRelationship>,
    pub next_cursor: Option<RelationshipCursor>,
}

/// What each provider's records establish about delegation. A pure table: no
/// database access, so it answers correctly even for a missing database.
pub fn relationship_capabilities(source: &str) -> RelationshipCapabilities {
    let (stable_child_identity, records) = match source {
        // Every Codex subagent rollout opens with its own thread id.
        "codex" => ("always", true),
        // Claude subagent transcripts carry the parent's `sessionId`; only
        // provider versions that also emit a per-child `agentId` give the
        // child a stable identity.
        "claude" => ("sometimes", true),
        _ => ("never", false),
    };
    RelationshipCapabilities {
        source: source.to_string(),
        stable_child_identity: stable_child_identity.to_string(),
        records_agent_type: records,
        records_spawn_time: records,
        records_evidence_locator: records,
    }
}

fn map_relationship(row: &Row<'_>) -> rusqlite::Result<SessionRelationship> {
    Ok(SessionRelationship {
        source: row.get(0)?,
        parent_session_id: row.get(1)?,
        child_session_id: row.get(2)?,
        relationship: row.get(3)?,
        identity_status: row.get(4)?,
        child_agent_type: row.get(5)?,
        child_agent_name: row.get(6)?,
        child_model: row.get(7)?,
        spawn_depth: row.get(8)?,
        evidence_kind: row.get(9)?,
        evidence_locator: row.get(10)?,
        evidence_ref: row.get(11)?,
        child_has_events: row.get(12)?,
        spawned_at_ms: row.get(13)?,
        created_ms: row.get(14)?,
        relationship_uid: row.get(15)?,
    })
}

fn unlinked_diagnostic(relationship: &SessionRelationship) -> RelationshipDiagnostic {
    RelationshipDiagnostic {
        code: "RELATIONSHIP_UNLINKED_CHILD".to_string(),
        message: format!(
            "{} evidence at {} has no stable child identity; events are not independently addressable",
            relationship.source,
            relationship.evidence_locator.as_deref().unwrap_or("unknown"),
        ),
        relationship_uid: Some(relationship.relationship_uid.clone()),
    }
}

/// Direct delegation relationships for one session, in both directions.
pub fn session_relationships(
    conn: &Connection,
    source: &str,
    session_id: &str,
) -> Result<SessionRelationships> {
    let as_parent = session_children(conn, source, session_id)?;
    let as_child = session_parents(conn, source, session_id)?;
    let diagnostics = as_parent
        .iter()
        .filter(|relationship| relationship.is_unlinked())
        .map(unlinked_diagnostic)
        .collect();
    Ok(SessionRelationships {
        contract_version: SESSION_RELATIONSHIP_CONTRACT_VERSION,
        source: source.to_string(),
        session_id: session_id.to_string(),
        as_parent,
        as_child,
        capabilities: relationship_capabilities(source),
        diagnostics,
    })
}

/// Every recorded child of one parent, in the traversal's total order.
pub fn session_children(
    conn: &Connection,
    source: &str,
    parent_session_id: &str,
) -> Result<Vec<SessionRelationship>> {
    let sql = format!(
        "SELECT {RELATIONSHIP_COLUMNS} FROM session_relationships \
         WHERE source = ? AND parent_session_id = ? {CHILD_ORDER}"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(params![source, parent_session_id], map_relationship)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

/// One bounded page of a parent's children, continuing the same total order.
pub fn session_children_page(
    conn: &Connection,
    source: &str,
    parent_session_id: &str,
    limit: i64,
    after: Option<&RelationshipCursor>,
) -> Result<SessionChildrenPage> {
    let limit = limit.clamp(1, MAX_CHILDREN_PAGE_LIMIT);
    let mut sql = format!(
        "SELECT {RELATIONSHIP_COLUMNS} FROM session_relationships \
         WHERE source = ? AND parent_session_id = ?"
    );
    let mut values: Vec<rusqlite::types::Value> = vec![
        source.to_string().into(),
        parent_session_id.to_string().into(),
    ];
    if let Some(cursor) = after {
        match cursor.spawned_at_ms {
            // Still inside the timestamped region: the rest of that region
            // follows, and then the whole null-timestamp tail.
            Some(spawned_at_ms) => {
                sql.push_str(
                    " AND (spawned_at_ms IS NULL \
                       OR spawned_at_ms > ? \
                       OR (spawned_at_ms = ? AND relationship_uid > ?))",
                );
                values.push(spawned_at_ms.into());
                values.push(spawned_at_ms.into());
                values.push(cursor.relationship_uid.clone().into());
            }
            // Already in the null-timestamp tail, which is ordered by uid.
            None => {
                sql.push_str(" AND spawned_at_ms IS NULL AND relationship_uid > ?");
                values.push(cursor.relationship_uid.clone().into());
            }
        }
    }
    sql.push(' ');
    sql.push_str(CHILD_ORDER);
    sql.push_str(" LIMIT ?");
    values.push((limit + 1).into());

    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params_from_iter(values), map_relationship)?;
    let mut children = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    let has_more = children.len() > limit as usize;
    if has_more {
        children.truncate(limit as usize);
    }
    let next_cursor = (has_more && !children.is_empty()).then(|| {
        let last = children.last().expect("non-empty page");
        RelationshipCursor {
            spawned_at_ms: last.spawned_at_ms,
            relationship_uid: last.relationship_uid.clone(),
        }
    });
    Ok(SessionChildrenPage {
        children,
        next_cursor,
    })
}

/// Every recorded parent of one child.
pub fn session_parents(
    conn: &Connection,
    source: &str,
    child_session_id: &str,
) -> Result<Vec<SessionRelationship>> {
    let sql = format!(
        "SELECT {RELATIONSHIP_COLUMNS} FROM session_relationships \
         WHERE source = ? AND child_session_id = ? \
         ORDER BY parent_session_id ASC, relationship_uid ASC"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(params![source, child_session_id], map_relationship)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

fn has_events(conn: &Connection, source: &str, session_id: &str) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM session_events \
         WHERE source = ? AND session_id = ? LIMIT 1)",
        params![source, session_id],
        |row| row.get(0),
    )?)
}

struct Pending {
    session_id: String,
    depth: u32,
    parent_index: Option<usize>,
    parent_session_id: Option<String>,
    relationship: Option<SessionRelationship>,
}

/// Whether `session_id` is already on the path from the root to `from`.
///
/// Only an edge back into the current branch's own ancestry is a cycle. An
/// edge into a node emitted on a different branch is a diamond in an acyclic
/// graph, which must not be reported as one.
fn is_ancestor(
    nodes: &[SessionTreeNode],
    parents: &[Option<usize>],
    from: Option<usize>,
    session_id: &str,
) -> bool {
    let mut index = from;
    while let Some(current) = index {
        if nodes[current].session_id == session_id {
            return true;
        }
        index = parents[current];
    }
    false
}

/// The complete descendant tree of one session, pre-order and bounded.
///
/// Traversal is an explicit stack with a visited set, so a cycle in the
/// recorded evidence costs one diagnostic rather than an unbounded walk, and
/// each emitted node costs exactly one indexed child query. A session appears
/// exactly once, at the position pre-order first reaches it; later arrivals by
/// another path are not expanded again.
pub fn session_tree(
    conn: &Connection,
    source: &str,
    session_id: &str,
    options: &SessionTreeOptions,
) -> Result<SessionTree> {
    let max_depth = options.max_depth.clamp(1, MAX_TREE_MAX_DEPTH);
    let max_nodes = options.max_nodes.clamp(1, MAX_TREE_MAX_NODES) as usize;
    let mut nodes: Vec<SessionTreeNode> = Vec::new();
    // Each emitted node's parent index, which `nodes` itself does not carry.
    let mut parents: Vec<Option<usize>> = Vec::new();
    let mut unlinked: Vec<SessionRelationship> = Vec::new();
    let mut diagnostics: Vec<RelationshipDiagnostic> = Vec::new();
    let mut visited: HashSet<String> = HashSet::new();
    let mut truncated = false;
    let mut max_depth_reached = 0;
    let mut stack = vec![Pending {
        session_id: session_id.to_string(),
        depth: 0,
        parent_index: None,
        parent_session_id: None,
        relationship: None,
    }];

    while let Some(pending) = stack.pop() {
        if !visited.insert(pending.session_id.clone()) {
            // A repeat is only a cycle when the edge points back into this
            // branch's own ancestry. Reaching a node already emitted on
            // another branch is a diamond: nothing is missing from the tree,
            // so it is neither a cycle nor truncation.
            if is_ancestor(&nodes, &parents, pending.parent_index, &pending.session_id) {
                if let Some(index) = pending.parent_index {
                    nodes[index].truncated = true;
                }
                diagnostics.push(RelationshipDiagnostic {
                    code: "RELATIONSHIP_CYCLE".to_string(),
                    message: format!(
                        "{} already appears in this branch; not expanded again",
                        pending.session_id
                    ),
                    relationship_uid: pending
                        .relationship
                        .as_ref()
                        .map(|relationship| relationship.relationship_uid.clone()),
                });
            }
            continue;
        }
        // Only a session the tree has not already emitted costs a node, so a
        // diamond's second arrival at a node — dropped just above — never
        // spends the budget or reports a complete tree as truncated.
        if nodes.len() >= max_nodes {
            truncated = true;
            // Every parent still waiting on the stack keeps children it will
            // never get, so all of them are marked, not just the one the
            // budget happened to stop at.
            for parent_index in std::iter::once(&pending)
                .chain(stack.iter())
                .filter_map(|remaining| remaining.parent_index)
            {
                nodes[parent_index].truncated = true;
            }
            diagnostics.push(RelationshipDiagnostic {
                code: "RELATIONSHIP_TREE_TRUNCATED".to_string(),
                message: format!("tree exceeded max_nodes={max_nodes}"),
                relationship_uid: None,
            });
            break;
        }
        // A child's addressability was recorded when the edge was observed;
        // only the root needs a probe of its own.
        let node_has_events = match pending.relationship.as_ref() {
            Some(relationship) => relationship.child_has_events,
            None => has_events(conn, source, &pending.session_id)?,
        };
        let index = nodes.len();
        max_depth_reached = max_depth_reached.max(pending.depth);
        parents.push(pending.parent_index);
        nodes.push(SessionTreeNode {
            source: source.to_string(),
            session_id: pending.session_id.clone(),
            depth: pending.depth,
            parent_session_id: pending.parent_session_id.clone(),
            relationship: pending.relationship.clone(),
            child_count: 0,
            has_events: node_has_events,
            truncated: false,
        });
        // The children of a node at the depth boundary are still read:
        // unlinked evidence is reported wherever it hangs, and the boundary
        // node's own child count is part of the answer either way.
        let mut linked = Vec::new();
        for relationship in session_children(conn, source, &pending.session_id)? {
            if relationship.is_unlinked() {
                diagnostics.push(unlinked_diagnostic(&relationship));
                unlinked.push(relationship);
            } else {
                linked.push(relationship);
            }
        }
        nodes[index].child_count = linked.len() as u32;
        if pending.depth >= max_depth {
            // Only a traversable child is left unexplored by the budget. A
            // node whose children are all unlinked evidence is complete: the
            // evidence is already in `unlinked`.
            if !linked.is_empty() {
                nodes[index].truncated = true;
                truncated = true;
                diagnostics.push(RelationshipDiagnostic {
                    code: "RELATIONSHIP_TREE_DEPTH_LIMIT".to_string(),
                    message: format!(
                        "{} has children beyond max_depth={max_depth}; not expanded",
                        pending.session_id
                    ),
                    relationship_uid: pending
                        .relationship
                        .as_ref()
                        .map(|relationship| relationship.relationship_uid.clone()),
                });
            }
            continue;
        }
        // Pushed in reverse so the stack pops them in the total order above.
        for relationship in linked.into_iter().rev() {
            let child_session_id = relationship
                .child_session_id
                .clone()
                .expect("linked relationships carry a child id");
            stack.push(Pending {
                session_id: child_session_id,
                depth: pending.depth + 1,
                parent_index: Some(index),
                parent_session_id: Some(pending.session_id.clone()),
                relationship: Some(relationship),
            });
        }
    }

    Ok(SessionTree {
        contract_version: SESSION_RELATIONSHIP_CONTRACT_VERSION,
        source: source.to_string(),
        root_session_id: session_id.to_string(),
        nodes,
        unlinked,
        capabilities: relationship_capabilities(source),
        diagnostics,
        truncated,
        max_depth_reached,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::open_db;

    struct Edge<'a> {
        source: &'a str,
        parent: &'a str,
        child: Option<&'a str>,
        uid: &'a str,
        spawned_at_ms: Option<i64>,
        has_events: bool,
    }

    impl<'a> Edge<'a> {
        fn new(parent: &'a str, child: &'a str) -> Self {
            Self {
                source: "codex",
                parent,
                child: Some(child),
                uid: "",
                spawned_at_ms: None,
                has_events: true,
            }
        }
    }

    fn insert_edge(conn: &Connection, edge: &Edge<'_>) {
        let uid = if edge.uid.is_empty() {
            match edge.child {
                Some(child) => format!("child:{child}"),
                None => "evidence:test:unknown".to_string(),
            }
        } else {
            edge.uid.to_string()
        };
        let identity_status = if edge.child.is_some() {
            IDENTITY_OBSERVED
        } else {
            IDENTITY_UNLINKED
        };
        conn.execute(
            "INSERT INTO session_relationships \
             (source, parent_session_id, relationship_uid, child_session_id, relationship, \
              identity_status, evidence_kind, evidence_locator, child_has_events, \
              spawned_at_ms, created_ms, updated_ms) \
             VALUES (?, ?, ?, ?, 'delegated', ?, 'test_evidence', ?, ?, ?, 1, 1)",
            params![
                edge.source,
                edge.parent,
                uid,
                edge.child,
                identity_status,
                format!("/tmp/{uid}.jsonl"),
                edge.has_events,
                edge.spawned_at_ms,
            ],
        )
        .unwrap();
    }

    fn database() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let conn = open_db(&dir.path().join("history.db")).unwrap();
        (dir, conn)
    }

    fn ids(tree: &SessionTree) -> Vec<String> {
        tree.nodes
            .iter()
            .map(|node| node.session_id.clone())
            .collect()
    }

    #[test]
    fn root_with_no_children_returns_empty_relationships() {
        let (_dir, conn) = database();
        let result = session_relationships(&conn, "codex", "lonely").unwrap();
        assert_eq!(
            result.contract_version,
            SESSION_RELATIONSHIP_CONTRACT_VERSION
        );
        assert!(result.as_parent.is_empty());
        assert!(result.as_child.is_empty());
        assert!(result.diagnostics.is_empty());
        assert_eq!(result.capabilities.stable_child_identity, "always");

        let tree = session_tree(&conn, "codex", "lonely", &SessionTreeOptions::default()).unwrap();
        assert_eq!(ids(&tree), vec!["lonely".to_string()]);
        assert!(!tree.truncated);
        assert_eq!(tree.max_depth_reached, 0);
    }

    #[test]
    fn single_child_and_multiple_children_are_ordered() {
        let (_dir, conn) = database();
        insert_edge(
            &conn,
            &Edge {
                spawned_at_ms: None,
                ..Edge::new("root", "no-timestamp")
            },
        );
        insert_edge(
            &conn,
            &Edge {
                spawned_at_ms: Some(300),
                ..Edge::new("root", "late")
            },
        );
        insert_edge(
            &conn,
            &Edge {
                spawned_at_ms: Some(100),
                ..Edge::new("root", "early")
            },
        );
        let children = session_children(&conn, "codex", "root").unwrap();
        assert_eq!(
            children
                .iter()
                .map(|child| child.child_session_id.clone().unwrap())
                .collect::<Vec<_>>(),
            vec!["early", "late", "no-timestamp"]
        );
        let result = session_relationships(&conn, "codex", "root").unwrap();
        assert_eq!(result.as_parent.len(), 3);
    }

    #[test]
    fn nested_descendants_produce_preorder_nodes() {
        let (_dir, conn) = database();
        insert_edge(&conn, &Edge::new("root", "a"));
        insert_edge(&conn, &Edge::new("a", "b"));
        insert_edge(&conn, &Edge::new("b", "c"));
        let tree = session_tree(&conn, "codex", "root", &SessionTreeOptions::default()).unwrap();
        assert_eq!(ids(&tree), vec!["root", "a", "b", "c"]);
        assert_eq!(
            tree.nodes.iter().map(|node| node.depth).collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
        );
        assert_eq!(tree.max_depth_reached, 3);
        assert!(!tree.truncated);
        assert_eq!(tree.nodes[1].parent_session_id.as_deref(), Some("root"));
        assert!(tree.nodes[0].relationship.is_none());
        assert_eq!(
            tree.nodes[1]
                .relationship
                .as_ref()
                .map(|edge| edge.relationship_uid.clone()),
            Some("child:a".to_string())
        );
    }

    #[test]
    fn child_to_parent_lookup_is_symmetric() {
        let (_dir, conn) = database();
        insert_edge(&conn, &Edge::new("root", "child"));
        let parent = session_relationships(&conn, "codex", "root").unwrap();
        let child = session_relationships(&conn, "codex", "child").unwrap();
        assert_eq!(parent.as_parent, child.as_child);
        assert!(child.as_parent.is_empty());
        assert!(parent.as_child.is_empty());
    }

    #[test]
    fn cycle_protection_emits_one_node_per_session() {
        let (_dir, conn) = database();
        insert_edge(&conn, &Edge::new("a", "b"));
        insert_edge(&conn, &Edge::new("b", "a"));
        let tree = session_tree(&conn, "codex", "a", &SessionTreeOptions::default()).unwrap();
        assert_eq!(ids(&tree), vec!["a", "b"]);
        assert!(tree.nodes[1].truncated);
        // The tree holds every session the evidence names, so a loop is not a
        // budget truncation.
        assert!(!tree.truncated);
        assert!(tree
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "RELATIONSHIP_CYCLE"));
    }

    #[test]
    fn self_edge_is_not_expanded() {
        let (_dir, conn) = database();
        insert_edge(&conn, &Edge::new("a", "a"));
        let tree = session_tree(&conn, "codex", "a", &SessionTreeOptions::default()).unwrap();
        assert_eq!(ids(&tree), vec!["a"]);
        assert!(tree.nodes[0].truncated);
        assert!(!tree.truncated);
        assert!(tree
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "RELATIONSHIP_CYCLE"));
    }

    #[test]
    fn a_diamond_is_not_reported_as_a_cycle() {
        let (_dir, conn) = database();
        insert_edge(&conn, &Edge::new("root", "b"));
        insert_edge(&conn, &Edge::new("root", "c"));
        insert_edge(&conn, &Edge::new("b", "c"));
        let tree = session_tree(&conn, "codex", "root", &SessionTreeOptions::default()).unwrap();
        // `c` is reachable twice but is one session, so it is emitted once.
        assert_eq!(ids(&tree), vec!["root", "b", "c"]);
        assert!(tree.diagnostics.is_empty());
        assert!(!tree.truncated);
        assert!(tree.nodes.iter().all(|node| !node.truncated));
        assert_eq!(tree.nodes[0].child_count, 2);
    }

    #[test]
    fn a_diamond_costs_one_node_per_session_against_the_budget() {
        let (_dir, conn) = database();
        for (parent, child) in [("root", "b"), ("root", "c"), ("b", "d"), ("c", "d")] {
            insert_edge(&conn, &Edge::new(parent, child));
        }
        // Four unique sessions and a budget of four: the tree is complete, so
        // the repeated arrival at `d` must not be charged as a fifth node.
        let tree = session_tree(
            &conn,
            "codex",
            "root",
            &SessionTreeOptions {
                max_depth: DEFAULT_TREE_MAX_DEPTH,
                max_nodes: 4,
            },
        )
        .unwrap();
        assert_eq!(ids(&tree), vec!["root", "b", "d", "c"]);
        assert!(!tree.truncated);
        assert!(tree.diagnostics.is_empty());
        assert!(tree.nodes.iter().all(|node| !node.truncated));
    }

    #[test]
    fn deterministic_ordering_is_insert_order_independent() {
        let forward = database();
        for (parent, child, spawned) in [
            ("root", "a", Some(10)),
            ("root", "b", Some(20)),
            ("a", "a1", Some(30)),
            ("a", "a2", None),
        ] {
            insert_edge(
                &forward.1,
                &Edge {
                    spawned_at_ms: spawned,
                    ..Edge::new(parent, child)
                },
            );
        }
        let reverse = database();
        for (parent, child, spawned) in [
            ("a", "a2", None),
            ("a", "a1", Some(30)),
            ("root", "b", Some(20)),
            ("root", "a", Some(10)),
        ] {
            insert_edge(
                &reverse.1,
                &Edge {
                    spawned_at_ms: spawned,
                    ..Edge::new(parent, child)
                },
            );
        }
        let options = SessionTreeOptions::default();
        let left = session_tree(&forward.1, "codex", "root", &options).unwrap();
        let right = session_tree(&reverse.1, "codex", "root", &options).unwrap();
        assert_eq!(ids(&left), vec!["root", "a", "a1", "a2", "b"]);
        assert_eq!(left.nodes, right.nodes);
        assert_eq!(
            session_children(&forward.1, "codex", "root").unwrap(),
            session_children(&reverse.1, "codex", "root").unwrap()
        );
    }

    #[test]
    fn source_isolation_keeps_matching_native_ids_apart() {
        let (_dir, conn) = database();
        insert_edge(&conn, &Edge::new("s1", "codex-child"));
        insert_edge(
            &conn,
            &Edge {
                source: "claude",
                ..Edge::new("s1", "claude-child")
            },
        );
        let options = SessionTreeOptions::default();
        assert_eq!(
            ids(&session_tree(&conn, "codex", "s1", &options).unwrap()),
            vec!["s1", "codex-child"]
        );
        assert_eq!(
            ids(&session_tree(&conn, "claude", "s1", &options).unwrap()),
            vec!["s1", "claude-child"]
        );
        assert_eq!(
            session_parents(&conn, "codex", "claude-child")
                .unwrap()
                .len(),
            0
        );
    }

    #[test]
    fn unlinked_rows_are_reported_not_traversed() {
        let (_dir, conn) = database();
        insert_edge(
            &conn,
            &Edge {
                source: "claude",
                child: None,
                uid: "evidence:claude_sidechain_records:/tmp/agent-x.jsonl",
                has_events: false,
                ..Edge::new("root", "unused")
            },
        );
        let tree = session_tree(&conn, "claude", "root", &SessionTreeOptions::default()).unwrap();
        assert_eq!(ids(&tree), vec!["root"]);
        assert_eq!(tree.unlinked.len(), 1);
        assert_eq!(tree.nodes[0].child_count, 0);
        assert!(tree
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "RELATIONSHIP_UNLINKED_CHILD"));
        let result = session_relationships(&conn, "claude", "root").unwrap();
        assert_eq!(result.as_parent.len(), 1);
        assert!(result.as_parent[0].child_session_id.is_none());
        assert_eq!(result.diagnostics.len(), 1);
    }

    #[test]
    fn large_tree_traversal_is_bounded() {
        let (_dir, conn) = database();
        conn.execute_batch(
            "WITH RECURSIVE seq(x) AS (VALUES(1) UNION ALL SELECT x + 1 FROM seq WHERE x < 5000) \
             INSERT INTO session_relationships \
               (source, parent_session_id, relationship_uid, child_session_id, relationship, \
                identity_status, evidence_kind, child_has_events, spawned_at_ms, created_ms, updated_ms) \
             SELECT 'codex', 'root', 'child:c' || x, 'c' || x, 'delegated', 'observed', \
                    'test_evidence', 1, x, 1, 1 FROM seq;",
        )
        .unwrap();
        let tree = session_tree(
            &conn,
            "codex",
            "root",
            &SessionTreeOptions {
                max_depth: DEFAULT_TREE_MAX_DEPTH,
                max_nodes: 100,
            },
        )
        .unwrap();
        assert_eq!(tree.nodes.len(), 100);
        assert!(tree.truncated);
        assert!(tree
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "RELATIONSHIP_TREE_TRUNCATED"));
    }

    #[test]
    fn depth_limit_marks_the_boundary_node_truncated() {
        let (_dir, conn) = database();
        for depth in 0..5 {
            insert_edge(
                &conn,
                &Edge::new(&format!("d{depth}"), &format!("d{}", depth + 1)),
            );
        }
        let tree = session_tree(
            &conn,
            "codex",
            "d0",
            &SessionTreeOptions {
                max_depth: 2,
                max_nodes: DEFAULT_TREE_MAX_NODES,
            },
        )
        .unwrap();
        assert_eq!(ids(&tree), vec!["d0", "d1", "d2"]);
        assert!(tree.nodes[2].truncated);
        assert!(tree.truncated);
        assert_eq!(tree.max_depth_reached, 2);
        assert!(tree
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "RELATIONSHIP_TREE_DEPTH_LIMIT"));
    }

    #[test]
    fn unlinked_evidence_at_the_depth_boundary_is_still_reported() {
        let (_dir, conn) = database();
        insert_edge(&conn, &Edge::new("root", "child"));
        insert_edge(
            &conn,
            &Edge {
                child: None,
                uid: "evidence:test:/tmp/agent-boundary.jsonl",
                has_events: false,
                ..Edge::new("child", "unused")
            },
        );
        let tree = session_tree(
            &conn,
            "codex",
            "root",
            &SessionTreeOptions {
                max_depth: 1,
                max_nodes: DEFAULT_TREE_MAX_NODES,
            },
        )
        .unwrap();
        assert_eq!(ids(&tree), vec!["root", "child"]);
        assert_eq!(tree.unlinked.len(), 1);
        assert_eq!(
            tree.diagnostics
                .iter()
                .map(|diagnostic| diagnostic.code.as_str())
                .collect::<Vec<_>>(),
            vec!["RELATIONSHIP_UNLINKED_CHILD"]
        );
        // Evidence with no traversable identity is not a subtree the budget
        // refused to walk.
        assert!(!tree.nodes[1].truncated);
        assert!(!tree.truncated);
        assert_eq!(tree.nodes[1].child_count, 0);
    }

    #[test]
    fn depth_boundary_counts_children_it_does_not_expand() {
        let (_dir, conn) = database();
        insert_edge(&conn, &Edge::new("root", "child"));
        insert_edge(&conn, &Edge::new("child", "grandchild"));
        insert_edge(
            &conn,
            &Edge {
                child: None,
                uid: "evidence:test:/tmp/agent-boundary.jsonl",
                has_events: false,
                ..Edge::new("child", "unused")
            },
        );
        let tree = session_tree(
            &conn,
            "codex",
            "root",
            &SessionTreeOptions {
                max_depth: 1,
                max_nodes: DEFAULT_TREE_MAX_NODES,
            },
        )
        .unwrap();
        assert_eq!(ids(&tree), vec!["root", "child"]);
        assert_eq!(tree.nodes[1].child_count, 1);
        assert!(tree.nodes[1].truncated);
        assert!(tree.truncated);
        assert_eq!(tree.unlinked.len(), 1);
        let codes = tree
            .diagnostics
            .iter()
            .map(|diagnostic| diagnostic.code.as_str())
            .collect::<Vec<_>>();
        assert!(codes.contains(&"RELATIONSHIP_UNLINKED_CHILD"));
        assert!(codes.contains(&"RELATIONSHIP_TREE_DEPTH_LIMIT"));
    }

    #[test]
    fn node_budget_marks_every_parent_left_unexpanded() {
        let (_dir, conn) = database();
        for (parent, child) in [
            ("root", "a"),
            ("root", "b"),
            ("a", "a1"),
            ("a", "a2"),
            ("b", "b1"),
        ] {
            insert_edge(&conn, &Edge::new(parent, child));
        }
        let tree = session_tree(
            &conn,
            "codex",
            "root",
            &SessionTreeOptions {
                max_depth: DEFAULT_TREE_MAX_DEPTH,
                max_nodes: 3,
            },
        )
        .unwrap();
        assert_eq!(ids(&tree), vec!["root", "a", "a1"]);
        assert!(tree.truncated);
        // `root` still owes `b` and `a` still owes `a2`; `a1` owes nothing.
        assert_eq!(
            tree.nodes
                .iter()
                .map(|node| node.truncated)
                .collect::<Vec<_>>(),
            vec![true, true, false]
        );
    }

    #[test]
    fn children_page_cursor_covers_every_child_exactly_once() {
        let (_dir, conn) = database();
        for index in 0..250 {
            insert_edge(
                &conn,
                &Edge {
                    // Half the rows have no provider spawn time, exercising
                    // both regions of the total order and the crossing.
                    spawned_at_ms: (index % 2 == 0).then_some(index),
                    ..Edge::new("root", &format!("c{index:03}"))
                },
            );
        }
        let mut seen = Vec::new();
        let mut cursor = None;
        let mut pages = 0;
        loop {
            let page = session_children_page(&conn, "codex", "root", 100, cursor.as_ref()).unwrap();
            pages += 1;
            seen.extend(
                page.children
                    .iter()
                    .map(|child| child.relationship_uid.clone()),
            );
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        assert_eq!(pages, 3);
        assert_eq!(seen.len(), 250);
        assert_eq!(seen.iter().collect::<HashSet<_>>().len(), 250);
        let all = session_children(&conn, "codex", "root")
            .unwrap()
            .into_iter()
            .map(|child| child.relationship_uid)
            .collect::<Vec<_>>();
        assert_eq!(seen, all);
    }

    #[test]
    fn capabilities_are_declared_per_source() {
        assert_eq!(
            relationship_capabilities("codex").stable_child_identity,
            "always"
        );
        assert_eq!(
            relationship_capabilities("claude").stable_child_identity,
            "sometimes"
        );
        for source in ["cursor", "grok", "opencode", "relay"] {
            let capabilities = relationship_capabilities(source);
            assert_eq!(capabilities.stable_child_identity, "never");
            assert!(!capabilities.records_agent_type);
            assert!(!capabilities.records_spawn_time);
            assert!(!capabilities.records_evidence_locator);
        }
    }
}
