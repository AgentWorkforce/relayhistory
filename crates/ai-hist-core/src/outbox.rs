//! WS-9 cloud-sync increment 2a: the outbox builder (pure sync logic, no network).
//!
//! Reads new local rows past a resume cursor, maps them to WS-1 convergence envelopes
//! (via [`crate::convergence`]), applies the incognito exclusion, and returns the batch
//! plus the advanced cursor. Network I/O (POST `/v1/ingest`, `rth_` auth) lives in the
//! binding layer (the `ai-hist` binary) per the no-async-in-core rule — this module only
//! does sync rusqlite reads, so it is fully unit-testable without a server.
//!
//! Cursor model (mirrors burn's `archive_state` watermark): monotonic `history.id` and
//! `trajectories.rowid`. NOTE (v1 limitation): trajectory rows that are *updated* after
//! first sync are not re-pushed by rowid alone — the server upsert makes a re-push safe,
//! but catching updates would need an `updated_ms` watermark (deferred).

use crate::convergence::{map_history_entry, map_trajectory, ConvergenceEnvelope, TrajectoryRow};
use crate::HistoryEntry;
use anyhow::Result;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Resume watermarks for incremental cloud sync (the local cursor store). Persisted by the
/// binding layer (single cursor store) and advanced to the server-confirmed values after a
/// successful `/v1/ingest`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncCursor {
    /// Highest `history.id` included in a synced batch.
    #[serde(default)]
    pub history_id: i64,
    /// Highest `trajectories.rowid` included in a synced batch.
    #[serde(default)]
    pub trajectory_rowid: i64,
}

/// The next outbox batch: the envelopes to POST and the cursor they advance to.
#[derive(Debug, Clone, PartialEq)]
pub struct OutboxBatch {
    pub records: Vec<ConvergenceEnvelope>,
    pub cursor: SyncCursor,
}

/// Build the next batch of convergence envelopes from local rows past `cursor`.
///
/// - `limit` caps rows scanned **per source** (history, trajectories).
/// - `incognito` holds session ids (history `session_id` / trajectory `id`) to exclude —
///   incognito rows are skipped but still advance the cursor so they are never re-scanned.
/// - The returned cursor advances to the max id/rowid *scanned* (not just emitted), so
///   skipped/empty rows don't cause re-scanning on the next call.
pub fn build_outbox_batch(
    conn: &Connection,
    cursor: &SyncCursor,
    limit: usize,
    incognito: &HashSet<String>,
) -> Result<OutboxBatch> {
    let limit = limit.max(1) as i64;
    let mut records = Vec::new();
    let mut next = cursor.clone();

    // --- history (prompts) — append-only, watermark on id ---
    {
        let mut stmt = conn.prepare(
            "SELECT id, source, session_id, project, prompt, prompt_hash, timestamp_ms \
             FROM history WHERE id > ?1 ORDER BY id ASC LIMIT ?2",
        )?;
        let rows = stmt.query_map([cursor.history_id, limit], |r| {
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
        for row in rows {
            let entry = row?;
            next.history_id = next.history_id.max(entry.id);
            // incognito: skip rows whose session is suppressed (still advances cursor)
            if let Some(sid) = &entry.session_id {
                if incognito.contains(sid) {
                    continue;
                }
            }
            records.push(map_history_entry(&entry));
        }
    }

    // --- trajectories (decisions/retro) — watermark on rowid ---
    {
        let mut stmt = conn.prepare(
            "SELECT rowid, id, persona_id, project_id, task_title, task_description, status, \
             decisions_json, retrospective_json, timestamp_ms \
             FROM trajectories WHERE rowid > ?1 ORDER BY rowid ASC LIMIT ?2",
        )?;
        let raw = stmt.query_map([cursor.trajectory_rowid, limit], |r| {
            Ok(TrajRowOwned {
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
            })
        })?;
        for row in raw {
            let t = row?;
            next.trajectory_rowid = next.trajectory_rowid.max(t.rowid);
            if incognito.contains(&t.id) {
                continue;
            }
            records.extend(map_trajectory(&TrajectoryRow {
                id: &t.id,
                persona_id: t.persona_id.as_deref(),
                project_id: t.project_id.as_deref(),
                task_title: t.task_title.as_deref(),
                task_description: t.task_description.as_deref(),
                status: t.status.as_deref(),
                task_ref: None, // not in local store yet (see convergence::TrajectoryRow)
                decisions_json: &t.decisions_json,
                retrospective_json: &t.retrospective_json,
                timestamp_ms: t.timestamp_ms,
            }));
        }
    }

    Ok(OutboxBatch {
        records,
        cursor: next,
    })
}

/// Owned trajectory row (rusqlite can't borrow across the row closure).
struct TrajRowOwned {
    rowid: i64,
    id: String,
    persona_id: Option<String>,
    project_id: Option<String>,
    task_title: Option<String>,
    task_description: Option<String>,
    status: Option<String>,
    decisions_json: String,
    retrospective_json: String,
    timestamp_ms: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{init_db, insert_history};

    fn mem() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        conn
    }

    fn add_history(conn: &Connection, session: &str, prompt: &str, ts: i64) {
        insert_history(
            conn,
            &HistoryEntry {
                id: 0,
                source: "claude".into(),
                session_id: Some(session.into()),
                project: None,
                prompt: prompt.into(),
                prompt_hash: Some(crate::prompt_hash(prompt)),
                timestamp_ms: ts,
            },
        )
        .unwrap();
    }

    fn add_trajectory(conn: &Connection, id: &str, decisions: &str, retro: &str) {
        conn.execute(
            "INSERT INTO trajectories (id, version, persona_id, project_id, task_title, \
             task_description, status, decisions_json, retrospective_json, search_text, path, \
             updated_ms, timestamp_ms) VALUES (?,1,?,?,?,?,?,?,?,?,NULL,?,?)",
            rusqlite::params![
                id,
                "planner",
                "proj",
                "Build forms",
                "desc",
                "completed",
                decisions,
                retro,
                "search",
                1,
                1_782_036_000_000i64
            ],
        )
        .unwrap();
    }

    #[test]
    fn builds_batch_and_advances_cursor() {
        let conn = mem();
        add_history(&conn, "s1", "first prompt", 1);
        add_history(&conn, "s1", "second prompt", 2);
        let none = HashSet::new();

        let batch = build_outbox_batch(&conn, &SyncCursor::default(), 100, &none).unwrap();
        assert_eq!(batch.records.len(), 2);
        assert_eq!(batch.cursor.history_id, 2);
        assert!(batch.records.iter().all(|r| r.kind == "prompt"));

        // a second call from the advanced cursor yields nothing new
        let empty = build_outbox_batch(&conn, &batch.cursor, 100, &none).unwrap();
        assert!(empty.records.is_empty());
        assert_eq!(empty.cursor, batch.cursor);
    }

    #[test]
    fn incognito_sessions_are_excluded_but_advance_cursor() {
        let conn = mem();
        add_history(&conn, "public", "keep me", 1);
        add_history(&conn, "secret", "drop me", 2);
        let incognito: HashSet<String> = ["secret".to_string()].into_iter().collect();

        let batch = build_outbox_batch(&conn, &SyncCursor::default(), 100, &incognito).unwrap();
        assert_eq!(batch.records.len(), 1);
        assert_eq!(batch.records[0].content, "keep me");
        // cursor still advances past the skipped incognito row (id 2) — never re-scanned
        assert_eq!(batch.cursor.history_id, 2);
    }

    #[test]
    fn trajectories_fan_out_into_batch() {
        let conn = mem();
        add_trajectory(
            &conn,
            "traj-1",
            r#"[{"chosen":"Formik"}]"#,
            r#"{"summary":"shipped","learnings":["L0"],"confidence":0.8}"#,
        );
        let none = HashSet::new();
        let batch = build_outbox_batch(&conn, &SyncCursor::default(), 100, &none).unwrap();
        // decision + summary + learning = 3 events, all trajectory lens
        assert_eq!(batch.records.len(), 3);
        assert!(batch.records.iter().all(|r| r.lens.as_deref() == Some("trajectories")));
        assert!(batch.records.iter().any(|r| r.event_id == "decision:traj-1:0"));
        assert!(batch
            .records
            .iter()
            .any(|r| r.event_id == "finding:traj-1:learning:0"));
        assert_eq!(batch.cursor.trajectory_rowid, 1);
    }

    #[test]
    fn incognito_excludes_trajectory_by_id() {
        let conn = mem();
        add_trajectory(&conn, "secret-traj", "[]", r#"{"summary":"hidden"}"#);
        let incognito: HashSet<String> = ["secret-traj".to_string()].into_iter().collect();
        let batch = build_outbox_batch(&conn, &SyncCursor::default(), 100, &incognito).unwrap();
        assert!(batch.records.is_empty());
        assert_eq!(batch.cursor.trajectory_rowid, 1); // still advanced
    }

    #[test]
    fn limit_caps_rows_per_source() {
        let conn = mem();
        for i in 1..=5 {
            add_history(&conn, "s", &format!("p{i}"), i);
        }
        let none = HashSet::new();
        let batch = build_outbox_batch(&conn, &SyncCursor::default(), 2, &none).unwrap();
        assert_eq!(batch.records.len(), 2);
        assert_eq!(batch.cursor.history_id, 2);
    }

    #[test]
    fn cursor_round_trips_through_json() {
        let c = SyncCursor {
            history_id: 7,
            trajectory_rowid: 3,
        };
        let s = serde_json::to_string(&c).unwrap();
        assert_eq!(serde_json::from_str::<SyncCursor>(&s).unwrap(), c);
    }
}
