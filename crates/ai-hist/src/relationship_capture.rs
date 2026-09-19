//! Recording observed delegation evidence.
//!
//! One writer for both providers, so a Codex subagent rollout and a Claude
//! subagent transcript produce rows of the same shape. Nothing here infers a
//! link: every field comes from something the provider actually recorded, and
//! a child without a provider-recorded identity is stored as unlinked evidence
//! rather than given a made-up id.

use anyhow::Result;
use rusqlite::{params, Connection};

/// What a provider's own records say about one delegation.
pub struct ObservedRelationship<'a> {
    pub source: &'a str,
    pub parent_session_id: &'a str,
    /// The child's provider-recorded identity, or `None` when this provider
    /// version records the delegation without giving the child one.
    pub child_session_id: Option<&'a str>,
    pub relationship: &'a str,
    pub child_agent_type: Option<&'a str>,
    pub child_agent_name: Option<&'a str>,
    pub child_model: Option<&'a str>,
    pub spawn_depth: Option<i64>,
    pub evidence_kind: &'a str,
    pub evidence_locator: Option<&'a str>,
    pub evidence_ref: Option<&'a str>,
    pub child_has_events: bool,
    pub spawned_at_ms: Option<i64>,
    /// The conversation a fork or continuation branched from, when the
    /// provider named one distinct from the parent. `None` for delegation.
    pub origin_session_id: Option<&'a str>,
    /// Overrides the derived dedupe key. Continuity edges need it: one
    /// session can be both the continuation *and* the resume target of the
    /// same parent, and two fork branches of one origin have no child id at
    /// all, so `child:<id>` cannot key either case.
    pub relationship_uid: Option<&'a str>,
}

/// A delegation observation, which is what every field but the continuity
/// ones describes. Continuity writers fill the two extra fields explicitly.
impl<'a> Default for ObservedRelationship<'a> {
    fn default() -> Self {
        Self {
            source: "",
            parent_session_id: "",
            child_session_id: None,
            relationship: crate::relationships::RELATIONSHIP_DELEGATED,
            child_agent_type: None,
            child_agent_name: None,
            child_model: None,
            spawn_depth: None,
            evidence_kind: "",
            evidence_locator: None,
            evidence_ref: None,
            child_has_events: false,
            spawned_at_ms: None,
            origin_session_id: None,
            relationship_uid: None,
        }
    }
}

impl ObservedRelationship<'_> {
    pub fn identity_status(&self) -> &'static str {
        if self.child_session_id.is_some() {
            crate::relationships::IDENTITY_OBSERVED
        } else {
            crate::relationships::IDENTITY_UNLINKED
        }
    }

    /// The dedupe key, which has to work with and without a child id. Keying
    /// unlinked evidence on its locator gives every sidecar its own row
    /// instead of collapsing them all into one.
    pub fn relationship_uid(&self) -> String {
        if let Some(uid) = self.relationship_uid {
            return uid.to_string();
        }
        match self.child_session_id {
            Some(child) => format!("child:{child}"),
            None => format!(
                "evidence:{}:{}",
                self.evidence_kind,
                self.evidence_locator.unwrap_or("")
            ),
        }
    }
}

/// Record one observed relationship, refreshing what re-observation can know.
///
/// `created_ms` is never updated, so repeated ingestion of the same evidence
/// is idempotent and keeps its first-observation time. Optional detail is
/// merged with `COALESCE` so a later, thinner observation cannot erase agent
/// metadata an earlier one captured.
pub fn record_relationship(conn: &Connection, observed: &ObservedRelationship<'_>) -> Result<()> {
    let now = now_ms();
    // An observation that finally names the child supersedes the unlinked row
    // an earlier provider version left for the same artifact: one file is not
    // both a linked and an unlinked delegation. Its uid is keyed on the
    // locator, so without this the stale row would outlive the upgrade.
    if let (Some(_), Some(locator)) = (observed.child_session_id, observed.evidence_locator) {
        conn.execute(
            "DELETE FROM session_relationships \
             WHERE source = ? AND parent_session_id = ? AND evidence_locator = ? \
               AND identity_status = ? AND relationship = ? AND relationship_uid != ?",
            params![
                observed.source,
                observed.parent_session_id,
                locator,
                crate::relationships::IDENTITY_UNLINKED,
                observed.relationship,
                observed.relationship_uid(),
            ],
        )?;
    }
    conn.execute(
        "INSERT INTO session_relationships \
         (source, parent_session_id, relationship_uid, child_session_id, relationship, \
          identity_status, child_agent_type, child_agent_name, child_model, spawn_depth, \
          evidence_kind, evidence_locator, evidence_ref, child_has_events, \
          spawned_at_ms, created_ms, updated_ms, origin_session_id) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(source, parent_session_id, relationship_uid) DO UPDATE SET \
           child_session_id = excluded.child_session_id, \
           relationship     = excluded.relationship, \
           identity_status  = excluded.identity_status, \
           child_agent_type = COALESCE(excluded.child_agent_type, session_relationships.child_agent_type), \
           child_agent_name = COALESCE(excluded.child_agent_name, session_relationships.child_agent_name), \
           child_model      = COALESCE(excluded.child_model,      session_relationships.child_model), \
           spawn_depth      = COALESCE(excluded.spawn_depth,      session_relationships.spawn_depth), \
           evidence_kind    = excluded.evidence_kind, \
           evidence_locator = COALESCE(excluded.evidence_locator, session_relationships.evidence_locator), \
           evidence_ref     = COALESCE(excluded.evidence_ref,     session_relationships.evidence_ref), \
           child_has_events = excluded.child_has_events, \
           spawned_at_ms    = COALESCE(excluded.spawned_at_ms,    session_relationships.spawned_at_ms), \
           origin_session_id = COALESCE(excluded.origin_session_id, session_relationships.origin_session_id), \
           updated_ms       = excluded.updated_ms",
        params![
            observed.source,
            observed.parent_session_id,
            observed.relationship_uid(),
            observed.child_session_id,
            observed.relationship,
            observed.identity_status(),
            observed.child_agent_type,
            observed.child_agent_name,
            observed.child_model,
            observed.spawn_depth,
            observed.evidence_kind,
            observed.evidence_locator,
            observed.evidence_ref,
            observed.child_has_events,
            observed.spawned_at_ms,
            now,
            now,
            observed.origin_session_id,
        ],
    )?;
    Ok(())
}

pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{open_db, session_children};

    fn observed<'a>(
        parent: &'a str,
        child: Option<&'a str>,
        locator: &'a str,
    ) -> ObservedRelationship<'a> {
        ObservedRelationship {
            source: "claude",
            parent_session_id: parent,
            child_session_id: child,
            relationship: "delegated",
            child_agent_type: None,
            child_agent_name: None,
            child_model: None,
            spawn_depth: None,
            evidence_kind: if child.is_some() {
                "claude_subagent_meta"
            } else {
                "claude_sidechain_records"
            },
            evidence_locator: Some(locator),
            evidence_ref: None,
            child_has_events: child.is_some(),
            spawned_at_ms: Some(5),
            ..ObservedRelationship::default()
        }
    }

    #[test]
    fn duplicate_relationship_ingestion_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let conn = open_db(&dir.path().join("history.db")).unwrap();
        let first = ObservedRelationship {
            child_agent_type: Some("Plan"),
            ..observed("root", Some("child"), "/tmp/agent-child.jsonl")
        };
        record_relationship(&conn, &first).unwrap();
        let created: i64 = conn
            .query_row("SELECT created_ms FROM session_relationships", [], |row| {
                row.get(0)
            })
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        // A later, thinner observation must not erase what the first knew.
        record_relationship(
            &conn,
            &observed("root", Some("child"), "/tmp/agent-child.jsonl"),
        )
        .unwrap();
        let row: (i64, i64, i64, Option<String>) = conn
            .query_row(
                "SELECT COUNT(*), MIN(created_ms), MIN(updated_ms), MIN(child_agent_type) \
                 FROM session_relationships",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(row.0, 1);
        assert_eq!(row.1, created);
        assert!(row.2 > created);
        assert_eq!(row.3.as_deref(), Some("Plan"));
    }

    #[test]
    fn unlinked_evidence_rows_do_not_collide() {
        let dir = tempfile::tempdir().unwrap();
        let conn = open_db(&dir.path().join("history.db")).unwrap();
        record_relationship(&conn, &observed("root", None, "/tmp/agent-a.jsonl")).unwrap();
        record_relationship(&conn, &observed("root", None, "/tmp/agent-b.jsonl")).unwrap();
        let children = session_children(
            &conn,
            "claude",
            "root",
            &crate::relationships::RelationshipKinds::delegation(),
        )
        .unwrap();
        assert_eq!(children.len(), 2);
        assert!(children
            .iter()
            .all(|child| child.child_session_id.is_none() && child.identity_status == "unlinked"));
        assert_eq!(
            children
                .iter()
                .map(|child| child.relationship_uid.clone())
                .collect::<std::collections::HashSet<_>>()
                .len(),
            2
        );
    }

    #[test]
    fn re_recording_upgrades_unlinked_to_observed_without_duplicating() {
        let dir = tempfile::tempdir().unwrap();
        let conn = open_db(&dir.path().join("history.db")).unwrap();
        record_relationship(&conn, &observed("root", None, "/tmp/agent-a.jsonl")).unwrap();
        // A second sidecar the provider still does not name must survive the
        // upgrade of the first one.
        record_relationship(&conn, &observed("root", None, "/tmp/agent-b.jsonl")).unwrap();
        record_relationship(&conn, &observed("root", Some("abc"), "/tmp/agent-a.jsonl")).unwrap();
        record_relationship(&conn, &observed("root", Some("abc"), "/tmp/agent-a.jsonl")).unwrap();
        let children = session_children(
            &conn,
            "claude",
            "root",
            &crate::relationships::RelationshipKinds::delegation(),
        )
        .unwrap();
        assert_eq!(children.len(), 2);
        let observed_child = children
            .iter()
            .find(|child| child.identity_status == "observed")
            .unwrap();
        assert_eq!(observed_child.child_session_id.as_deref(), Some("abc"));
        assert_eq!(observed_child.relationship_uid, "child:abc");
        assert!(observed_child.child_has_events);
        // The superseded row for the same artifact is retired, not kept
        // alongside the identity that replaced it.
        assert_eq!(
            children
                .iter()
                .filter(|child| child.identity_status == "unlinked")
                .map(|child| child.evidence_locator.clone().unwrap())
                .collect::<Vec<_>>(),
            vec!["/tmp/agent-b.jsonl".to_string()]
        );
    }
}
