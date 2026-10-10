//! Bounded depth-first traversal of the recorded delegation tree.

use super::{
    relationship_capabilities, session_children, unlinked_diagnostic, RelationshipDiagnostic,
    SessionRelationship, SessionTree, SessionTreeNode, SessionTreeOptions, MAX_TREE_MAX_DEPTH,
    MAX_TREE_MAX_NODES, SESSION_RELATIONSHIP_CONTRACT_VERSION,
};
use anyhow::Result;
use rusqlite::{params, Connection};
use std::collections::HashSet;

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

impl Pending {
    fn relationship_uid(&self) -> Option<String> {
        self.relationship
            .as_ref()
            .map(|relationship| relationship.relationship_uid.clone())
    }
}

/// Mark `pending`'s parent truncated for an edge back into its own ancestry.
fn mark_cycle(nodes: &mut [SessionTreeNode], pending: &Pending) -> RelationshipDiagnostic {
    if let Some(index) = pending.parent_index {
        nodes[index].truncated = true;
    }
    RelationshipDiagnostic {
        code: "RELATIONSHIP_CYCLE".to_string(),
        message: format!(
            "{} already appears in this branch; not expanded again",
            pending.session_id
        ),
        relationship_uid: pending.relationship_uid(),
    }
}

/// The traversable children; unlinked evidence is reported and set aside.
fn split_unlinked(
    children: Vec<SessionRelationship>,
    unlinked: &mut Vec<SessionRelationship>,
    diagnostics: &mut Vec<RelationshipDiagnostic>,
) -> Vec<SessionRelationship> {
    let mut linked = Vec::new();
    for relationship in children {
        if relationship.is_unlinked() {
            diagnostics.push(unlinked_diagnostic(&relationship));
            unlinked.push(relationship);
        } else {
            linked.push(relationship);
        }
    }
    linked
}

/// Pushed in reverse so the stack pops them in the total order of `linked`.
fn push_children(
    stack: &mut Vec<Pending>,
    linked: Vec<SessionRelationship>,
    parent: &Pending,
    parent_index: usize,
) {
    for relationship in linked.into_iter().rev() {
        let child_session_id = relationship
            .child_session_id
            .clone()
            .expect("linked relationships carry a child id");
        stack.push(Pending {
            session_id: child_session_id,
            depth: parent.depth + 1,
            parent_index: Some(parent_index),
            parent_session_id: Some(parent.session_id.clone()),
            relationship: Some(relationship),
        });
    }
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
                diagnostics.push(mark_cycle(&mut nodes, &pending));
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
        let children = session_children(
            conn,
            source,
            &pending.session_id,
            &options.relationship_kinds,
        )?;
        let linked = split_unlinked(children, &mut unlinked, &mut diagnostics);
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
                    relationship_uid: pending.relationship_uid(),
                });
            }
            continue;
        }
        push_children(&mut stack, linked, &pending, index);
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
