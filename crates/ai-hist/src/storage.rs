//! Typed local history scans for integrations. SQL and storage layout stay in the core.
use crate::HistoryEntry;
use anyhow::Result;
use rusqlite::{Connection, OptionalExtension};
/// Maximum rows returned by one bounded storage scan.
pub const MAX_SCAN_LIMIT: usize = 10_000;
fn bounded(limit: usize) -> i64 {
    limit.clamp(1, MAX_SCAN_LIMIT) as i64
}

pub fn history_after(conn: &Connection, after: i64, limit: usize) -> Result<Vec<HistoryEntry>> {
    let limit = bounded(limit);
    let mut stmt = conn.prepare(
        "SELECT id, source, session_id, project, prompt, prompt_hash, timestamp_ms \
             FROM history WHERE id > ?1 ORDER BY id ASC LIMIT ?2",
    )?;
    let rows = stmt.query_map([after, limit], |r| {
        Ok(HistoryEntry {
            id: r.get(0)?,
            source: r.get(1)?,
            session_id: r.get(2)?,
            project: r.get(3)?,
            prompt: r.get(4)?,
            prompt_hash: r.get(5)?,
            timestamp_ms: r.get(6)?,
        })
    })?;

    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn trajectories_after(
    conn: &Connection,
    updated_ms: i64,
    rowid: i64,
    limit: usize,
) -> Result<Vec<StoredTrajectory>> {
    let limit = bounded(limit);
    let mut stmt = conn.prepare(
        "SELECT rowid, id, persona_id, project_id, task_title, task_description, status, \
             decisions_json, retrospective_json, timestamp_ms, updated_ms, path \
             FROM trajectories \
             WHERE updated_ms > ?1 OR (updated_ms = ?1 AND rowid > ?2) \
             ORDER BY updated_ms ASC, rowid ASC LIMIT ?3",
    )?;
    let raw = stmt.query_map(rusqlite::params![updated_ms, rowid, limit], |r| {
        Ok(StoredTrajectory {
            rowid: r.get(0)?,
            id: r.get(1)?,
            persona_id: r.get(2)?,
            project_id: r.get(3)?,
            task_title: r.get(4)?,
            task_description: r.get(5)?,
            status: r.get(6)?,
            decisions_json: r.get(7)?,
            retrospective_json: r.get(8)?,
            timestamp_ms: r.get(9)?,
            updated_ms: r.get(10)?,
            path: r.get(11)?,
        })
    })?;

    Ok(raw.collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn commit_links_after(
    conn: &Connection,
    after: i64,
    limit: usize,
) -> Result<Vec<StoredCommitLink>> {
    let limit = bounded(limit);
    let mut stmt = conn.prepare(
        "SELECT id, source, session_id, repo, branch, commit_sha, match_method, confidence, \
             files_json, numstat_json, evidence_json, created_at_ms \
             FROM session_commit_links WHERE id > ?1 ORDER BY id ASC LIMIT ?2",
    )?;
    let rows = stmt.query_map([after, limit], |r| {
        Ok(StoredCommitLink {
            id: r.get(0)?,
            source: r.get(1)?,
            session_id: r.get(2)?,
            repo: r.get(3)?,
            branch: r.get(4)?,
            commit_sha: r.get(5)?,
            match_method: r.get(6)?,
            confidence: r.get(7)?,
            files_json: r.get(8)?,
            numstat_json: r.get(9)?,
            evidence_json: r.get(10)?,
            created_at_ms: r.get(11)?,
        })
    })?;

    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn changed_file_sessions_after(
    conn: &Connection,
    after: i64,
    limit: usize,
) -> Result<Vec<ChangedSession>> {
    let limit = bounded(limit);
    let mut stmt = conn.prepare(
        "SELECT source, session_id, MAX(id) AS max_id \
             FROM file_edits WHERE id > ?1 \
             GROUP BY source, session_id \
             ORDER BY max_id ASC LIMIT ?2",
    )?;
    let rows = stmt.query_map([after, limit], |r| {
        Ok(ChangedSession {
            source: r.get(0)?,
            session_id: r.get(1)?,
            max_id: r.get(2)?,
        })
    })?;

    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

#[derive(Debug, Clone)]
pub struct ChangedSession {
    pub source: String,
    pub session_id: String,
    pub max_id: i64,
}
#[derive(Debug, Clone)]
pub struct SessionMetadata {
    pub cwd: Option<String>,
    pub git_branch: Option<String>,
}
pub fn session_metadata(
    conn: &Connection,
    source: &str,
    session_id: &str,
) -> Result<Option<SessionMetadata>> {
    Ok(conn
        .query_row(
            "SELECT cwd, git_branch FROM sessions WHERE source=?1 AND session_id=?2",
            [source, session_id],
            |r| {
                Ok(SessionMetadata {
                    cwd: r.get(0)?,
                    git_branch: r.get(1)?,
                })
            },
        )
        .optional()?)
}
pub fn session_project(
    conn: &Connection,
    source: &str,
    session_id: &str,
) -> Result<Option<String>> {
    Ok(conn.query_row("SELECT project FROM history WHERE source=?1 AND session_id=?2 AND project IS NOT NULL AND trim(project) != '' LIMIT 1", [source, session_id], |r| r.get(0)).optional()?)
}
/// Source/session and first pending event ID, ordered by that ID.
pub fn pending_event_sessions(
    conn: &Connection,
    after: i64,
    limit: usize,
) -> Result<Vec<(String, String, i64)>> {
    let mut stmt=conn.prepare("SELECT session_id, source, MIN(id) AS first_new_id FROM session_events WHERE id > ?1 GROUP BY session_id, source ORDER BY first_new_id ASC LIMIT ?2")?;
    let rows = stmt.query_map([after, bounded(limit)], |r| {
        Ok((r.get(0)?, r.get(1)?, r.get(2)?))
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}
/// Owned trajectory row (rusqlite can't borrow across the row closure).
pub struct StoredTrajectory {
    pub rowid: i64,
    pub id: String,
    pub persona_id: Option<String>,
    pub project_id: Option<String>,
    pub task_title: Option<String>,
    pub task_description: Option<String>,
    pub status: Option<String>,
    pub decisions_json: String,
    pub retrospective_json: String,
    pub timestamp_ms: i64,
    pub updated_ms: i64,
    pub path: Option<String>,
}

pub struct StoredCommitLink {
    pub id: i64,
    pub source: String,
    pub session_id: String,
    pub repo: Option<String>,
    pub branch: Option<String>,
    pub commit_sha: String,
    pub match_method: String,
    pub confidence: f64,
    pub files_json: Option<String>,
    pub numstat_json: Option<String>,
    pub evidence_json: Option<String>,
    pub created_at_ms: i64,
}

pub fn latest_history_for_session(
    conn: &Connection,
    source: &str,
    session_id: &str,
) -> Result<Option<HistoryEntry>> {
    let mut stmt = conn.prepare(
        "SELECT id, source, session_id, project, prompt, prompt_hash, timestamp_ms \
         FROM history WHERE source = ?1 AND session_id = ?2 ORDER BY id DESC LIMIT 1",
    )?;
    let mut rows = stmt.query_map(rusqlite::params![source, session_id], |r| {
        Ok(HistoryEntry {
            id: r.get(0)?,
            source: r.get(1)?,
            session_id: r.get(2)?,
            project: r.get(3)?,
            prompt: r.get(4)?,
            prompt_hash: r.get(5)?,
            timestamp_ms: r.get(6)?,
        })
    })?;
    Ok(rows.next().transpose()?)
}

/// Bounded, deduplicated identities across every table that stores a
/// session, the same read as [`crate::SessionStore::session_identities`], so
/// an export exclusion cannot miss partially read data. Continue with the
/// last returned identity; hold a read transaction when a consistent
/// multi-page baseline is required.
#[cfg(feature = "export")]
pub fn session_identities_after(
    conn: &Connection,
    after: Option<&crate::export::SessionIdentity>,
    limit: usize,
) -> Result<Vec<crate::export::SessionIdentity>> {
    let page = crate::session_identities::identities_after(
        conn,
        after.map(|after| (after.source.as_str(), after.session_id.as_str())),
        bounded(limit) as usize,
    )?;
    Ok(page
        .into_iter()
        .map(|(source, session_id)| crate::export::SessionIdentity { source, session_id })
        .collect())
}

#[cfg(all(test, feature = "export"))]
mod identity_tests {
    use super::*;
    #[test]
    fn identity_pages_include_uncatalogued_data_without_duplicates_or_tie_skips() {
        let conn = Connection::open_in_memory().unwrap();
        crate::init_db(&conn).unwrap();
        conn.execute(
            "INSERT INTO sessions(source, session_id) VALUES ('claude','a'), ('codex','a')",
            [],
        )
        .unwrap();
        conn.execute("INSERT INTO history(source, session_id, prompt, prompt_hash, timestamp_ms) VALUES ('claude','a','test','one',1), ('claude','b','second test','two',1)", []).unwrap();
        conn.execute("INSERT INTO session_events(source, session_id, ts_ms, role, kind, text, event_uid) VALUES ('claude','c',1,'user','text','test','one')", []).unwrap();
        let mut cursor = None;
        let mut identities = vec![];
        loop {
            let page = session_identities_after(&conn, cursor.as_ref(), 1).unwrap();
            if page.is_empty() {
                break;
            }
            assert_eq!(page.len(), 1);
            cursor = page.last().cloned();
            identities.extend(page.into_iter().map(|v| (v.source, v.session_id)));
        }
        assert_eq!(
            identities,
            [
                ("claude", "a"),
                ("claude", "b"),
                ("claude", "c"),
                ("codex", "a")
            ]
            .map(|(s, id)| (s.to_string(), id.to_string()))
        );
    }
}
