//! Resolve the admitted transcript and dependent rows without scanning the archive.
use super::*;

const AFFECTED_SQL: &str = "SELECT locator FROM session_continuity_evidence WHERE source=?1 AND locator=?2 \
         UNION SELECT locator FROM session_continuity_evidence WHERE source=?1 \
           AND origin_session_id IN (SELECT value FROM json_each(?3)) \
         UNION SELECT evidence_locator FROM session_relationships INDEXED BY idx_session_relationships_parent WHERE source=?1 \
           AND parent_session_id IN (SELECT value FROM json_each(?4)) \
           AND relationship IN ('continuation','fork','resume') AND evidence_locator IS NOT NULL \
         UNION SELECT c.locator FROM session_events e INDEXED BY idx_session_events_revision \
         CROSS JOIN session_continuity_evidence c INDEXED BY idx_session_continuity_parent_uuid \
           ON c.source=e.source AND c.first_parent_uuid=e.message_id \
           WHERE e.source=?1 AND e.revision>?5 AND e.message_id IS NOT NULL \
             AND c.pending_reason IS NOT NULL \
         UNION SELECT c.locator FROM session_events e INDEXED BY idx_session_events_revision \
         CROSS JOIN session_continuity_evidence c INDEXED BY idx_session_continuity_parent_uuid \
           ON c.source=e.source AND c.first_parent_uuid=substr(e.event_uid,1,length(e.event_uid)-2) \
           WHERE e.source='claude' AND e.source=?1 AND e.revision>?5 \
             AND substr(e.event_uid,-2)=':0' \
             AND (e.message_id IS NULL OR e.event_uid<>e.message_id||':0') \
             AND c.pending_reason IS NOT NULL";

pub(crate) struct CaptureScope {
    previous: Option<(String, Option<String>)>,
    revision: u64,
}

impl CaptureScope {
    pub(crate) fn begin(conn: &Connection, source: &str, path: &Path) -> Result<Self> {
        Ok(Self {
            previous: stored_identity(conn, source, &path.to_string_lossy())?,
            revision: crate::change_feed::read_head(conn)?.revision,
        })
    }
}

/// Current explicit fields resolve now. Reopened siblings/dependents and
/// children whose parent records just arrived are sought through existing
/// identity/UUID/revision indexes. Unrelated unresolved rows wait for the
/// full round's recovery reconciliation.
pub(crate) fn reconcile_captured(
    conn: &Connection,
    source: &str,
    path: &Path,
    captured: CaptureScope,
) -> Result<ContinuityReconciliation> {
    let locator = path.to_string_lossy();
    let current = stored_identity(conn, source, &locator)?;
    let identities: Vec<_> = [captured.previous, current].into_iter().flatten().collect();
    let sessions: Vec<_> = identities.iter().map(|(session, _)| session).collect();
    let origins: Vec<_> = identities
        .iter()
        .filter_map(|(_, origin)| origin.as_ref())
        .collect();
    let keys: Vec<String> = conn
        .prepare_cached(AFFECTED_SQL)?
        .query_map(
            params![
                source,
                locator,
                serde_json::to_string(&origins)?,
                serde_json::to_string(&sessions)?,
                captured.revision
            ],
            |row| row.get(0),
        )?
        .collect::<rusqlite::Result<_>>()?;
    let mut pending = Vec::new();
    let mut statement = conn.prepare_cached(
        "SELECT source, locator, session_id, file_session_id, first_parent_uuid, first_ts_ms, \
          in_log_session_ids_json, has_resume_marker, resume_target, explicit_targets_json, source_version \
         FROM session_continuity_evidence WHERE source=?1 AND locator=?2 AND pending_reason IS NOT NULL"
    )?;
    for key in keys {
        if let Some(evidence) = statement
            .query_row(params![source, key], storage::map_evidence)
            .optional()?
        {
            pending.push(evidence);
        }
    }
    reconcile_rows(conn, pending)
}

pub(super) fn reconcile_rows(
    conn: &Connection,
    pending: Vec<ContinuityEvidence>,
) -> Result<ContinuityReconciliation> {
    let mut report = ContinuityReconciliation {
        considered: pending.len(),
        ..ContinuityReconciliation::default()
    };
    for evidence in &pending {
        crate::ingest::check_capture_cancelled()?;
        let mut reasons: Vec<String> = Vec::new();
        // Every uid this locator establishes on this pass. What it established
        // on an earlier pass and no longer does is retracted below, so a
        // rewritten transcript cannot leave a stale edge queryable.
        let mut written: Vec<(String, String)> = Vec::new();
        resolve_explicit(conn, evidence, &mut written)?;
        resolve_resume(conn, evidence, &mut written)?;
        resolve_cross_file_parent(conn, evidence, &mut reasons, &mut written)?;
        // Last of the explicit signals: only when none of the above applied.
        resolve_explicit_source(conn, evidence, &mut written)?;
        let lineage = written.len();
        resolve_fork_group(conn, evidence, lineage > 0, &mut reasons, &mut written)?;
        report.edges_written += written.len();
        report.retracted += retract_edges(conn, &evidence.source, &evidence.locator, &written)?;
        let reason = (!reasons.is_empty()).then(|| reasons.join("; "));
        if reason.is_none() {
            report.resolved += 1;
        } else {
            report.pending += 1;
        }
        conn.execute(
            "UPDATE session_continuity_evidence SET pending_reason = ?, updated_ms = ? \
             WHERE source = ? AND locator = ?",
            params![reason, crate::now_ms(), evidence.source, evidence.locator],
        )?;
    }
    Ok(report)
}

pub(crate) fn ensure_indexes(conn: &Connection) -> Result<()> {
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_session_continuity_parent_uuid ON session_continuity_evidence(source, first_parent_uuid)",
        [],
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_session_continuity_origin ON session_continuity_evidence(source, origin_session_id)",
        [],
    )?;
    // Reconciliation and the pending-evidence diagnostic both read only the
    // rows still waiting on something, so the index is partial: a database
    // whose evidence has all resolved carries almost no index at all.
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_session_continuity_pending ON session_continuity_evidence(source, session_id) WHERE pending_reason IS NOT NULL",
        [],
    )?;
    Ok(())
}

const REOPEN_ORIGINS: &str =
    "UPDATE session_continuity_evidence INDEXED BY idx_session_continuity_origin \
    SET pending_reason='unreconciled' WHERE source=?1 \
    AND origin_session_id IN (SELECT value FROM json_each(?2)) AND pending_reason IS NULL";
const DEPENDENT_LOCATORS: &str = "SELECT evidence_locator FROM session_relationships INDEXED BY idx_session_relationships_parent \
    WHERE source=?1 AND parent_session_id IN (SELECT value FROM json_each(?2)) \
    AND relationship IN ('continuation','fork','resume') AND evidence_locator IS NOT NULL";
const REOPEN_LOCATOR: &str =
    "UPDATE session_continuity_evidence SET pending_reason='unreconciled' \
    WHERE source=?1 AND locator=?2 AND pending_reason IS NULL";

pub(super) fn reopen_dependents(
    conn: &Connection,
    source: &str,
    identities: &[Option<(String, Option<String>)>],
) -> Result<usize> {
    let sessions: BTreeSet<_> = identities.iter().flatten().map(|(id, _)| id).collect();
    let origins: BTreeSet<_> = identities
        .iter()
        .flatten()
        .filter_map(|(_, origin)| origin.as_ref())
        .collect();
    let mut changed = conn
        .prepare_cached(REOPEN_ORIGINS)?
        .execute(params![source, serde_json::to_string(&origins)?])?;
    let locators: Vec<String> = conn
        .prepare_cached(DEPENDENT_LOCATORS)?
        .query_map(params![source, serde_json::to_string(&sessions)?], |row| {
            row.get(0)
        })?
        .collect::<rusqlite::Result<_>>()?;
    let mut update = conn.prepare_cached(REOPEN_LOCATOR)?;
    for locator in locators {
        changed += update.execute(params![source, locator])?;
    }
    Ok(changed)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn affected_continuity_uses_identity_and_delta_indexes_with_47k_unrelated_rows() {
        let temp = tempfile::tempdir().unwrap();
        let conn = crate::open_db(&temp.path().join("history.db")).unwrap();
        conn.execute_batch("WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<46999) INSERT INTO session_continuity_evidence(source,locator,session_id,in_log_session_ids_json,has_resume_marker,explicit_targets_json,source_version,origin_session_id,pending_reason,updated_ms) SELECT 'claude','unrelated-'||x,'unrelated-'||x,'[]',0,'{}',1,'origin-'||x,'unresolved',0 FROM n;").unwrap();
        crate::continuity::record_evidence(
            &conn,
            &ContinuityEvidence {
                source: "claude".into(),
                locator: "active".into(),
                session_id: "active".into(),
                ..Default::default()
            },
        )
        .unwrap();
        let capture = CaptureScope::begin(&conn, "claude", Path::new("active")).unwrap();
        let report = reconcile_captured(&conn, "claude", Path::new("active"), capture).unwrap();
        assert_eq!(
            report.considered, 1,
            "unrelated pending archive must not multiply append work"
        );
        let plans: Vec<String> = conn
            .prepare(&format!("EXPLAIN QUERY PLAN {AFFECTED_SQL}"))
            .unwrap()
            .query_map(params!["claude", "active", "[]", "[\"active\"]", 0], |r| {
                r.get(3)
            })
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        for (sql, expected) in [
            (
                REOPEN_ORIGINS,
                "idx_session_continuity_origin (source=? AND origin_session_id=?)",
            ),
            (
                DEPENDENT_LOCATORS,
                "idx_session_relationships_parent (source=? AND parent_session_id=?)",
            ),
            (
                REOPEN_LOCATOR,
                "sqlite_autoindex_session_continuity_evidence_1 (source=? AND locator=?)",
            ),
        ] {
            let plan: Vec<String> = conn
                .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
                .unwrap()
                .query_map(params!["claude", "[]"], |r| r.get(3))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
            assert!(plan.join("\n").contains(expected), "{}", plan.join("\n"));
        }
        let mut reopen = conn.prepare(REOPEN_ORIGINS).unwrap();
        reopen.execute(params!["claude", "[]"]).unwrap();
        let origin_steps = reopen.get_status(rusqlite::StatementStatus::VmStep);
        let mut dependents = conn.prepare(DEPENDENT_LOCATORS).unwrap();
        let _: Vec<String> = dependents
            .query_map(params!["claude", "[\"active\"]"], |r| r.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        let dependent_steps = dependents.get_status(rusqlite::StatementStatus::VmStep);
        let mut affected = conn.prepare(AFFECTED_SQL).unwrap();
        let _: Vec<String> = affected
            .query_map(params!["claude", "active", "[]", "[\"active\"]", 0], |r| {
                r.get(0)
            })
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        let affected_steps = affected.get_status(rusqlite::StatementStatus::VmStep);
        assert!(origin_steps+dependent_steps+affected_steps<1000,"origin {origin_steps}, dependent {dependent_steps}, affected {affected_steps}: 47k unrelated rows must not become VM work");
        let plan = plans.join("\n");
        assert!(
            plan.contains("idx_session_continuity_origin (source=? AND origin_session_id=?)"),
            "{plan}"
        );
        assert!(
            plan.contains("idx_session_events_revision (revision>?)"),
            "{plan}"
        );
        assert!(
            plan.contains("idx_session_continuity_parent_uuid (source=? AND first_parent_uuid=?)"),
            "{plan}"
        );
    }
    #[test]
    fn uid_only_parent_delta_resolves_its_pending_continuation_immediately() {
        let temp = tempfile::tempdir().unwrap();
        let conn = crate::open_db(&temp.path().join("history.db")).unwrap();
        conn.execute_batch("INSERT INTO sessions(source,session_id) VALUES ('claude','parent'),('claude','child');").unwrap();
        record_evidence(
            &conn,
            &ContinuityEvidence {
                source: "claude".into(),
                locator: "child.jsonl".into(),
                session_id: "child".into(),
                first_parent_uuid: Some("parent-record".into()),
                first_ts_ms: Some(2),
                ..Default::default()
            },
        )
        .unwrap();
        reconcile(&conn, "claude").unwrap();
        let before = CaptureScope::begin(&conn, "claude", Path::new("parent.jsonl")).unwrap();
        conn.execute_batch("INSERT INTO session_events(source,session_id,message_id,event_uid,ts_ms,role,kind,text) VALUES ('claude','parent',NULL,'parent-record:0',1,'user','text','synthetic');").unwrap();
        let result =
            reconcile_captured(&conn, "claude", Path::new("parent.jsonl"), before).unwrap();
        assert_eq!(result.considered, 1);
        assert_eq!(conn.query_row("SELECT COUNT(*) FROM session_relationships WHERE source='claude' AND parent_session_id='parent' AND child_session_id='child' AND relationship='continuation'",[],|r|r.get::<_,i64>(0)).unwrap(),1);
    }
}
