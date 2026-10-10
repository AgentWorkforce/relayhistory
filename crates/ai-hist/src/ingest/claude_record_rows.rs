//! The rows one Claude transcript record produced, and the heal that retires
//! them from an identity the record no longer belongs to.
//!
//! Two call sites in `ingest_claude_record` heal what an earlier parse left:
//! a sidecar record an older parser stored under the parent's session id, and
//! a synthetic notice an older parser stored as assistant output. Both used to
//! run the retirement unconditionally — a `DELETE` and an `UPDATE` on each of
//! four tables, eight statements prepared afresh — for every record of every
//! re-read, and on a store the current parser built they found nothing,
//! forever. A live collector spent 44% of its busy thread there.
//!
//! The retirement is now asked first: one cached statement checks for exactly
//! the rows it would touch, and only when there are some does it run. The
//! answer is the same predicate the retirement uses, so a record that really
//! does move between identities — a sidecar read before it named its child
//! and re-read after, a store an older parser wrote — is healed exactly as
//! before, and a current store issues no write at all.

use super::*;
use std::sync::OnceLock;

/// The rows one Claude record produced, per table, as `(table, condition)`
/// over `(session_id, message_uuid)`. Event and marker uids are the record's
/// uuid plus `:`-suffixes, so the prefix is a range on the uid -- `:` and `;`
/// are adjacent bytes -- which the `(source, session_id, uid)` unique index
/// answers, and a `_` or `%` in a provider id is a literal byte. The parser
/// retires a record's rows under the parent for every sidechain record it
/// moves onto its child, so a scan of the session here would make each
/// re-read of a sidecar quadratic in the size of the parent session.
pub(super) const CLAUDE_RECORD_ROWS: [(&str, &str); 4] = [
    (
        "session_events",
        "source = 'claude' AND session_id = ?1 \
         AND event_uid >= ?2 || ':' AND event_uid < ?2 || ';'",
    ),
    (
        "tool_calls",
        "source = 'claude' AND session_id = ?1 AND message_id = ?2",
    ),
    (
        "file_edits",
        "source = 'claude' AND session_id = ?1 AND message_id = ?2",
    ),
    // Markers are derived from the same record and keyed on the same prefix,
    // so they move with it rather than outliving it under the old identity.
    (
        "session_markers",
        "source = 'claude' AND session_id = ?1 \
         AND marker_uid >= ?2 || ':' AND marker_uid < ?2 || ';'",
    ),
];

/// Remove everything a single transcript record produced under one session id.
///
/// `message_uuid` is the same identity insertion derives event uids from and
/// stamps on the rows it derives from a record's tool use, so this reaches the
/// record's events, its tool calls, its file edits and its markers together —
/// including records with no `uuid` of their own, which fall back to the
/// message id or a hash of the record. Leaving the derived rows behind would
/// keep a parent exposing a delegated thread's actions as its own long after
/// the events moved to the child.
pub(super) fn delete_claude_record_rows(
    conn: &Connection,
    session_id: &str,
    message_uuid: &str,
) -> Result<()> {
    #[cfg(test)]
    RETIREMENTS.with(|count| count.set(count.get() + 1));
    for (table, condition) in CLAUDE_RECORD_ROWS {
        crate::store::retire_evidence_share(
            conn,
            table,
            condition,
            params![session_id, message_uuid],
            SessionLocation::Local,
        )?;
    }
    Ok(())
}

#[cfg(test)]
thread_local! {
    /// Retirements run on this thread, so a test can say a sweep issued none.
    static RETIREMENTS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Which of a record's rows the heal is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HealedRows {
    /// Every row the record produced under the session: a sidecar record's
    /// rows under the parent, none of which the current parser writes.
    All,
    /// Every row but the `{uuid}:marker` the current parser writes for a
    /// synthetic notice, which the heal would otherwise find — and delete and
    /// write back — on every re-read of the notice.
    ExceptNoticeMarker,
}

/// Retire `message_uuid`'s rows under `session_id`, but only when there are
/// any. Returns whether it did.
///
/// The probe is the retirement's own predicate: rows the local side backs
/// alone are deleted and rows both sides back are handed to the remote side,
/// so "exists with `location` local or both" is exactly "the retirement would
/// change something". When it would not, nothing is written.
pub(super) fn heal_claude_record_rows(
    conn: &Connection,
    session_id: &str,
    message_uuid: &str,
    rows: HealedRows,
) -> Result<bool> {
    let found: bool = conn
        .prepare_cached(probe_sql(rows))?
        .query_row(params![session_id, message_uuid], |row| row.get(0))?;
    if found {
        delete_claude_record_rows(conn, session_id, message_uuid)?;
    }
    Ok(found)
}

/// One `EXISTS` per table, each a keyed search on the same index the
/// retirement uses, OR-ed into one statement so a record costs one step.
fn probe_sql(rows: HealedRows) -> &'static str {
    static ALL: OnceLock<String> = OnceLock::new();
    static EXCEPT_MARKER: OnceLock<String> = OnceLock::new();
    let build = |except_marker: bool| {
        let probes = CLAUDE_RECORD_ROWS
            .iter()
            .map(|(table, condition)| {
                let keep = if except_marker && *table == "session_markers" {
                    " AND marker_uid <> ?2 || ':marker'"
                } else {
                    ""
                };
                format!(
                    "EXISTS(SELECT 1 FROM {table} WHERE ({condition}){keep} \
                     AND location IN ('local', 'both'))"
                )
            })
            .collect::<Vec<_>>();
        format!("SELECT {}", probes.join(" OR "))
    };
    match rows {
        HealedRows::All => ALL.get_or_init(|| build(false)),
        HealedRows::ExceptNoticeMarker => EXCEPT_MARKER.get_or_init(|| build(true)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn evidence_store() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        crate::init_db(&conn).unwrap();
        conn
    }

    fn insert_event(conn: &Connection, session_id: &str, uid: &str) {
        conn.execute(
            "INSERT INTO session_events (source, session_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', ?1, 1, 'assistant', 'text', 't', ?2)",
            params![session_id, uid],
        )
        .unwrap();
    }

    /// How many retirements this thread has run so far. A retirement is the
    /// heal's only write: its `DELETE` and `UPDATE` pair on every table.
    fn retirements() -> usize {
        RETIREMENTS.with(std::cell::Cell::get)
    }

    #[test]
    fn the_probe_is_a_keyed_search_in_every_table() {
        let conn = evidence_store();
        for rows in [HealedRows::All, HealedRows::ExceptNoticeMarker] {
            let plan: Vec<String> = conn
                .prepare(&format!("EXPLAIN QUERY PLAN {}", probe_sql(rows)))
                .unwrap()
                .query_map(params!["parent", "m"], |row| row.get::<_, String>(3))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
            assert!(
                plan.iter().filter(|step| step.contains("SEARCH")).count()
                    >= CLAUDE_RECORD_ROWS.len(),
                "every table is searched on its key: {plan:?}"
            );
            assert!(
                !plan
                    .iter()
                    .any(|step| step.starts_with("SCAN") && step != "SCAN CONSTANT ROW"),
                "no table is scanned: {plan:?}"
            );
        }
    }

    #[test]
    fn a_record_with_nothing_under_the_old_identity_writes_nothing() {
        let conn = evidence_store();
        insert_event(&conn, "child", "m:0");
        insert_event(&conn, "parent", "other:0");
        let before = retirements();
        assert!(!heal_claude_record_rows(&conn, "parent", "m", HealedRows::All).unwrap());
        assert_eq!(retirements(), before);
    }

    #[test]
    fn rows_under_the_old_identity_are_still_retired() {
        let conn = evidence_store();
        for uid in ["m:0", "m:1:marker", "mx:0"] {
            insert_event(&conn, "parent", uid);
        }
        assert!(heal_claude_record_rows(&conn, "parent", "m", HealedRows::All).unwrap());
        let left: Vec<String> = conn
            .prepare("SELECT event_uid FROM session_events ORDER BY event_uid")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(left, ["mx:0"]);
    }

    #[test]
    fn a_row_the_remote_side_alone_backs_is_not_the_heals_to_touch() {
        let conn = evidence_store();
        insert_event(&conn, "parent", "m:0");
        conn.execute("UPDATE session_events SET location = 'remote'", [])
            .unwrap();
        assert!(!heal_claude_record_rows(&conn, "parent", "m", HealedRows::All).unwrap());
        conn.execute("UPDATE session_events SET location = 'both'", [])
            .unwrap();
        assert!(heal_claude_record_rows(&conn, "parent", "m", HealedRows::All).unwrap());
        let location: String = conn
            .query_row("SELECT location FROM session_events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(location, "remote");
    }

    #[test]
    fn a_synthetic_notices_own_marker_does_not_reopen_the_heal() {
        let conn = evidence_store();
        conn.execute(
            "INSERT INTO session_markers (source, session_id, marker_uid, kind) \
             VALUES ('claude', 's', 'n:marker', 'local_notice')",
            [],
        )
        .unwrap();
        assert!(!heal_claude_record_rows(&conn, "s", "n", HealedRows::ExceptNoticeMarker).unwrap());
        insert_event(&conn, "s", "n:0");
        assert!(heal_claude_record_rows(&conn, "s", "n", HealedRows::ExceptNoticeMarker).unwrap());
    }

    fn write_parent_with_sidecar(home: &Path, sidecar: &str) -> PathBuf {
        let project = home.join(".claude/projects/app");
        let sidecar_path = project.join("s1/subagents/agent-a.jsonl");
        fs::create_dir_all(sidecar_path.parent().unwrap()).unwrap();
        fs::write(
            project.join("s1.jsonl"),
            concat!(
                r#"{"type":"user","uuid":"u1","sessionId":"s1","cwd":"/tmp/app","timestamp":"2026-09-20T00:00:00.000Z","message":{"role":"user","content":"spawn a helper"}}"#, "\n",
                r#"{"type":"assistant","uuid":"a1","parentUuid":"u1","sessionId":"s1","cwd":"/tmp/app","timestamp":"2026-09-20T00:00:01.000Z","requestId":"req_1","message":{"id":"msg_1","role":"assistant","model":"claude-opus-4-7","stop_reason":"end_turn","content":[{"type":"text","text":"On it."}]}}"#, "\n",
            ),
        )
        .unwrap();
        fs::write(&sidecar_path, sidecar).unwrap();
        sidecar_path
    }

    const SIDECAR: &str = concat!(
        r#"{"type":"user","uuid":"su1","sessionId":"s1","agentId":"a","isSidechain":true,"cwd":"/tmp/app","timestamp":"2026-09-20T00:00:02.000Z","message":{"role":"user","content":"look around"}}"#,
        "\n",
        r#"{"type":"assistant","uuid":"sa1","parentUuid":"su1","sessionId":"s1","agentId":"a","isSidechain":true,"cwd":"/tmp/app","timestamp":"2026-09-20T00:00:03.000Z","requestId":"req_s","message":{"id":"msg_sub","role":"assistant","model":"claude-opus-4-7","stop_reason":"end_turn","content":[{"type":"text","text":"Helper report."},{"type":"tool_use","id":"toolu_s","name":"Edit","input":{"file_path":"/tmp/app/lib.rs"}}]}}"#,
        "\n",
    );

    fn child_rows(conn: &Connection) -> (i64, i64) {
        conn.query_row(
            "SELECT \
               (SELECT COUNT(*) FROM session_events WHERE source = 'claude' AND session_id = 'a'), \
               (SELECT COUNT(*) FROM session_events WHERE source = 'claude' AND session_id = 's1' \
                  AND message_id IN ('su1', 'sa1'))",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap()
    }

    /// The case the live collector paid for: a sidecar the sweep reads again
    /// although nothing in it moved. Every one of its records is a sidechain
    /// record attributed to the child, so every one used to retire its rows
    /// under the parent — eight statements a record, finding nothing.
    #[test]
    fn re_reading_an_unchanged_sidecar_issues_no_deletes() {
        let home = tempfile::tempdir().unwrap();
        let sidecar = write_parent_with_sidecar(home.path(), SIDECAR);
        let conn = evidence_store();
        let root = home.path().join(".claude/projects");
        let mut state = Map::new();
        crate::ingest::sync_claude_session_metadata(&conn, &mut state, &root).unwrap();
        let first = child_rows(&conn);
        assert!(first.0 > 0, "the sidecar indexes under the child");
        assert_eq!(first.1, 0, "and nothing of it under the parent");

        // Read the whole sidecar again, as a repair, a backfill pass or a
        // held record's release does.
        transcript_cursor::forget_locator_cursor(&conn, "claude", &sidecar).unwrap();
        let before = retirements();
        crate::ingest::sync_claude_session_metadata(&conn, &mut state, &root).unwrap();
        assert!(
            transcript_cursor::locator_cursor_exists(&conn, "claude", &sidecar).unwrap(),
            "the second sweep read the sidecar"
        );
        assert_eq!(retirements(), before, "no record's rows were retired");
        assert_eq!(child_rows(&conn), first);
    }

    /// The other side of the same probe: rows an earlier parse left under
    /// the parent are still moved onto the child by the next read, on a store
    /// that has already been swept.
    #[test]
    fn a_re_read_still_moves_parent_rows_onto_the_child() {
        let home = tempfile::tempdir().unwrap();
        let sidecar = write_parent_with_sidecar(home.path(), SIDECAR);
        let conn = evidence_store();
        let root = home.path().join(".claude/projects");
        let mut state = Map::new();
        crate::ingest::sync_claude_session_metadata(&conn, &mut state, &root).unwrap();
        let swept = child_rows(&conn);
        insert_event(&conn, "s1", "sa1:0");
        conn.execute("UPDATE session_events SET message_id = 'sa1' WHERE event_uid = 'sa1:0' AND session_id = 's1'", [])
            .unwrap();
        conn.execute(
            "INSERT INTO tool_calls (source, session_id, message_id, tool_use_id, name, ts_ms) \
             VALUES ('claude', 's1', 'sa1', 'toolu_s', 'Edit', 1)",
            [],
        )
        .unwrap();

        transcript_cursor::forget_locator_cursor(&conn, "claude", &sidecar).unwrap();
        crate::ingest::sync_claude_session_metadata(&conn, &mut state, &root).unwrap();
        let parent_tool_calls: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM tool_calls WHERE source = 'claude' AND session_id = 's1' \
                 AND message_id = 'sa1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(child_rows(&conn), swept);
        assert_eq!(parent_tool_calls, 0);
    }
}
