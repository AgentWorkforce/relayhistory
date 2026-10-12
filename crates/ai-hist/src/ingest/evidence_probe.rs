//! One read of the evidence keys an unchanged-file probe asks about.
//!
//! A steady sweep asks, once per unchanged transcript, whether that file
//! already left evidence. The answer is a point read. This module reads the
//! keys for that answer once per armed walk and serves the later probes from
//! it.
//!
//! The snapshot is this connection's. It is dropped when a sweep chunk
//! commits — that commit releases the writer lock, and another writer can
//! change evidence in the gap — and when this sweep deletes evidence the
//! probes read. A later probe then reads again. A missing snapshot is not
//! "present": the probe asks the database, and a file with no evidence is
//! still opened. The guard pops on drop, including a panic.

use std::cell::RefCell;
use std::collections::HashSet;
use std::path::Path;

use anyhow::Result;
use rusqlite::Connection;

#[cfg(test)]
use std::cell::Cell;

thread_local! {
    static STACK: RefCell<Vec<Memo>> = const { RefCell::new(Vec::new()) };
}

#[cfg(test)]
thread_local! {
    static LOADS: Cell<u64> = const { Cell::new(0) };
    static FALLBACKS: Cell<u64> = const { Cell::new(0) };
}

struct Memo {
    conn: usize,
    claude_transcripts: Option<HashSet<String>>,
    claude_sidecars: Option<HashSet<String>>,
    codex_sessions: Option<HashSet<String>>,
}

#[derive(Clone, Copy)]
enum Kind {
    ClaudeTranscript,
    ClaudeSidecar,
    Codex,
}

enum Read {
    Unarmed,
    Hit(bool),
    Pending,
}

/// [`claude_sidecar_evidence_exists`]'s probe, asked of every Claude transcript
/// the walk considers. `idx_session_relationships_locator` is what makes it a
/// search rather than a scan of every Claude relationship.
pub(super) const CLAUDE_SIDECAR_EVIDENCE_SQL: &str = "SELECT EXISTS(
            SELECT 1
            FROM session_relationships r
            WHERE r.source = 'claude' AND r.evidence_locator = ?
              AND (
                EXISTS(
                  SELECT 1 FROM session_events e
                  WHERE e.source = 'claude'
                    AND e.session_id = COALESCE(r.child_session_id, r.parent_session_id)
                )
                OR EXISTS(
                  SELECT 1 FROM session_markers m
                  WHERE m.source = 'claude'
                    AND m.session_id = COALESCE(r.child_session_id, r.parent_session_id)
                )
              )
            LIMIT 1
        )";

/// The same predicate as [`CLAUDE_SIDECAR_EVIDENCE_SQL`], over every locator.
const CLAUDE_SIDECAR_KEYS_SQL: &str = "SELECT DISTINCT r.evidence_locator
            FROM session_relationships r
            WHERE r.source = 'claude' AND r.evidence_locator IS NOT NULL
              AND (
                EXISTS(
                  SELECT 1 FROM session_events e
                  WHERE e.source = 'claude'
                    AND e.session_id = COALESCE(r.child_session_id, r.parent_session_id)
                )
                OR EXISTS(
                  SELECT 1 FROM session_markers m
                  WHERE m.source = 'claude'
                    AND m.session_id = COALESCE(r.child_session_id, r.parent_session_id)
                )
              )";

/// [`claude_transcript_events_exist`]'s probe.
///
/// The sync walk asks it of every Claude transcript on every sweep, so its
/// plan is the walk's cost. `CROSS JOIN` makes SQLite drive the lookup from
/// the one `sessions` row the path names. Left to choose, the planner (with no
/// `sqlite_stat1`, which this crate never writes) drove it from
/// `session_events` instead -- a scan of every Claude event until one belonged
/// to that path's session, so a path with no row (every subagent sidecar, every
/// new transcript) read the whole table, once per file (#215).
pub(super) const CLAUDE_TRANSCRIPT_EVENTS_SQL: &str = "SELECT EXISTS(
            SELECT 1
            FROM sessions s
            CROSS JOIN session_events e ON e.source = s.source AND e.session_id = s.session_id
            WHERE s.source = 'claude' AND s.raw_path = ?
            LIMIT 1
        ) OR EXISTS(
            SELECT 1
            FROM sessions s
            CROSS JOIN session_markers m ON m.source = s.source AND m.session_id = s.session_id
            WHERE s.source = 'claude' AND s.raw_path = ?
            LIMIT 1
        )";

/// Paths whose session row has an event or a marker. The same `OR` as
/// [`CLAUDE_TRANSCRIPT_EVENTS_SQL`], read once.
const CLAUDE_TRANSCRIPT_KEYS_SQL: &str = "SELECT s.raw_path FROM sessions s
            WHERE s.source = 'claude' AND s.raw_path IS NOT NULL
              AND EXISTS (
                SELECT 1 FROM session_events e
                WHERE e.source = s.source AND e.session_id = s.session_id
              )
            UNION
            SELECT s.raw_path FROM sessions s
            WHERE s.source = 'claude' AND s.raw_path IS NOT NULL
              AND EXISTS (
                SELECT 1 FROM session_markers m
                WHERE m.source = s.source AND m.session_id = s.session_id
              )";

/// Codex sessions that have an event or a marker. The same `OR` as
/// [`codex_session_evidence_exists`].
const CODEX_EVIDENCE_KEYS_SQL: &str = "SELECT session_id FROM session_events WHERE source = 'codex'
            UNION
            SELECT session_id FROM session_markers WHERE source = 'codex'";

fn conn_id(conn: &Connection) -> usize {
    std::ptr::from_ref(conn) as usize
}

fn count_load() {
    #[cfg(test)]
    LOADS.with(|loads| loads.set(loads.get() + 1));
}

fn count_fallback() {
    #[cfg(test)]
    FALLBACKS.with(|falls| falls.set(falls.get() + 1));
}

/// Arm the snapshot for one walk. Drop pops it, including on panic.
pub(super) struct Guard;

impl Guard {
    pub(super) fn arm(conn: &Connection) -> Self {
        STACK.with(|stack| {
            stack.borrow_mut().push(Memo {
                conn: conn_id(conn),
                claude_transcripts: None,
                claude_sidecars: None,
                codex_sessions: None,
            });
        });
        Self
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        STACK.with(|stack| {
            stack.borrow_mut().pop();
        });
    }
}

/// Drop the snapshot. The next probe reads the keys again.
pub(super) fn release(conn: &Connection) {
    let id = conn_id(conn);
    STACK.with(|stack| {
        for memo in stack.borrow_mut().iter_mut() {
            if memo.conn == id {
                memo.clear();
            }
        }
    });
}

/// A delete of probe evidence just landed. `deleted` is how many rows the
/// statement removed; a table the probes do not read leaves the snapshot.
pub(crate) fn note_deleted(conn: &Connection, table: &str, deleted: usize) {
    if deleted > 0
        && matches!(
            table,
            "session_events" | "session_markers" | "session_relationships" | "sessions"
        )
    {
        release(conn);
    }
}

/// This sweep removed probe evidence by a statement that does not report a
/// row count here (a relationship retract, a cascaded session delete).
pub(super) fn note_evidence_changed(conn: &Connection) {
    release(conn);
}

/// Whether a session has any evidence this parser wrote, of any kind.
///
/// Not just `session_events`: a lifecycle-only rollout -- one that started,
/// streamed an error and never produced a message -- writes markers and no
/// events. Asking only about events answers "nothing indexed" forever for
/// those, so the stamp fast path re-reads and re-upserts the file on every
/// sync and the cost never converges. Nothing is lost by that, which is why it
/// survived review once; it is pure waste that grows with the archive.
pub(super) fn codex_session_evidence_exists(conn: &Connection, session_id: &str) -> Result<bool> {
    if let Some(present) = memo(conn, Kind::Codex, session_id)? {
        return Ok(present);
    }
    count_fallback();
    Ok(super::session_events_exist(conn, "codex", session_id)?
        || super::session_markers_exist(conn, "codex", session_id)?)
}

/// Whether an unchanged subagent sidecar has already been ingested.
///
/// A sidecar is deliberately never registered as a session, so the catalog
/// join [`claude_transcript_events_exist`] makes can never confirm one and
/// every sidecar would otherwise be re-read and re-ingested on every sync,
/// however unchanged it is. Its recorded delegation is the equivalent proof,
/// paired with the events it produced — under the child it named, or under
/// the parent when it named none — so a wiped or rebuilt database still
/// re-reads the file.
pub(super) fn claude_sidecar_evidence_exists(conn: &Connection, path: &Path) -> Result<bool> {
    let locator = path.to_string_lossy();
    if let Some(present) = memo(conn, Kind::ClaudeSidecar, locator.as_ref())? {
        return Ok(present);
    }
    // Markers count, for the same reason they count in the two session
    // probes: a sidecar whose records the event model cannot carry -- a
    // compaction boundary, a system row, a non-text block -- indexes markers
    // and no events. Asking only about events reads that as "this file left
    // nothing behind", so an unchanged sidecar is re-parsed on every sync
    // forever while the sync reports itself perfectly normal.
    count_fallback();
    let exists: i64 = super::cached_query_row(
        conn,
        CLAUDE_SIDECAR_EVIDENCE_SQL,
        [locator.as_ref()],
        |row| row.get(0),
    )?;
    Ok(exists != 0)
}

/// Whether this transcript has left any evidence behind, of any kind.
///
/// Not just `session_events`, for the same reason the Codex probe counts
/// markers: a transcript can store only markers -- a summary, a compaction
/// boundary, an image-only turn, a system row -- and asking about events alone
/// answers "nothing indexed" forever for those, so the stamp fast path
/// re-reads and re-upserts an unchanged file on every sync. Nothing is lost by
/// that; the cost simply never converges.
pub(super) fn claude_transcript_events_exist(conn: &Connection, path: &Path) -> Result<bool> {
    let raw_path = path.to_string_lossy();
    if let Some(present) = memo(conn, Kind::ClaudeTranscript, raw_path.as_ref())? {
        return Ok(present);
    }
    count_fallback();
    let exists: i64 = super::cached_query_row(
        conn,
        CLAUDE_TRANSCRIPT_EVENTS_SQL,
        [raw_path.as_ref(), raw_path.as_ref()],
        |row| row.get(0),
    )?;
    Ok(exists != 0)
}

fn memo(conn: &Connection, kind: Kind, key: &str) -> Result<Option<bool>> {
    let id = conn_id(conn);
    let read = STACK.with(|stack| {
        let stack = stack.borrow();
        match stack.iter().rev().find(|memo| memo.conn == id) {
            None => Read::Unarmed,
            Some(memo) if memo.is_loaded(kind) => Read::Hit(memo.contains(kind, key)),
            Some(_) => Read::Pending,
        }
    });
    match read {
        Read::Unarmed => Ok(None),
        Read::Hit(present) => Ok(Some(present)),
        Read::Pending => {
            load(conn, id, kind)?;
            Ok(STACK.with(|stack| {
                let stack = stack.borrow();
                stack
                    .iter()
                    .rev()
                    .find(|memo| memo.conn == id)
                    .and_then(|memo| memo.answer(kind, key))
            }))
        }
    }
}

fn load(conn: &Connection, id: usize, kind: Kind) -> Result<()> {
    let keys = match kind {
        Kind::ClaudeTranscript => load_set(conn, CLAUDE_TRANSCRIPT_KEYS_SQL)?,
        Kind::ClaudeSidecar => load_set(conn, CLAUDE_SIDECAR_KEYS_SQL)?,
        Kind::Codex => load_set(conn, CODEX_EVIDENCE_KEYS_SQL)?,
    };
    count_load();
    STACK.with(|stack| {
        let mut stack = stack.borrow_mut();
        if let Some(memo) = stack.iter_mut().rev().find(|memo| memo.conn == id) {
            memo.store(kind, keys);
        }
    });
    Ok(())
}

fn load_set(conn: &Connection, sql: &str) -> Result<HashSet<String>> {
    let mut stmt = conn.prepare_cached(sql)?;
    let mut rows = stmt.query([])?;
    let mut keys = HashSet::new();
    while let Some(row) = rows.next()? {
        keys.insert(row.get(0)?);
    }
    Ok(keys)
}

impl Memo {
    fn clear(&mut self) {
        self.claude_transcripts = None;
        self.claude_sidecars = None;
        self.codex_sessions = None;
    }

    fn is_loaded(&self, kind: Kind) -> bool {
        self.slot(kind).is_some()
    }

    fn contains(&self, kind: Kind, key: &str) -> bool {
        self.slot(kind).is_some_and(|keys| keys.contains(key))
    }

    /// `None` when this kind was not stored, so the caller asks the database
    /// instead of treating a gap as "present".
    fn answer(&self, kind: Kind, key: &str) -> Option<bool> {
        self.slot(kind).map(|keys| keys.contains(key))
    }

    fn store(&mut self, kind: Kind, keys: HashSet<String>) {
        *self.slot_mut(kind) = Some(keys);
    }

    fn slot(&self, kind: Kind) -> Option<&HashSet<String>> {
        match kind {
            Kind::ClaudeTranscript => self.claude_transcripts.as_ref(),
            Kind::ClaudeSidecar => self.claude_sidecars.as_ref(),
            Kind::Codex => self.codex_sessions.as_ref(),
        }
    }

    fn slot_mut(&mut self, kind: Kind) -> &mut Option<HashSet<String>> {
        match kind {
            Kind::ClaudeTranscript => &mut self.claude_transcripts,
            Kind::ClaudeSidecar => &mut self.claude_sidecars,
            Kind::Codex => &mut self.codex_sessions,
        }
    }
}

#[cfg(test)]
fn load_count() -> u64 {
    LOADS.with(Cell::get)
}

#[cfg(test)]
fn fallback_count() -> u64 {
    FALLBACKS.with(Cell::get)
}

#[cfg(test)]
fn reset_counts() {
    LOADS.with(|loads| loads.set(0));
    FALLBACKS.with(|falls| falls.set(0));
}

#[cfg(test)]
fn depth() -> usize {
    STACK.with(|stack| stack.borrow().len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{init_db, SessionLocation};
    use rusqlite::Connection;
    use serde_json::{json, Map};
    use std::fs;
    use std::path::PathBuf;

    fn db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        conn
    }

    fn event(conn: &Connection, source: &str, session_id: &str, uid: &str, text: &str) {
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, ts_ms, role, kind, text, event_uid) \
             VALUES (?1, ?2, 1, 'user', 'text', ?3, ?4)",
            rusqlite::params![source, session_id, text, uid],
        )
        .unwrap();
    }

    fn marker(conn: &Connection, source: &str, session_id: &str, uid: &str) {
        conn.execute(
            "INSERT INTO session_markers (source, session_id, marker_uid, kind) \
             VALUES (?1, ?2, ?3, 'compaction_boundary')",
            rusqlite::params![source, session_id, uid],
        )
        .unwrap();
    }

    fn session(conn: &Connection, source: &str, session_id: &str, raw_path: Option<&str>) {
        conn.execute(
            "INSERT INTO sessions (session_id, source, raw_path) VALUES (?1, ?2, ?3)",
            rusqlite::params![session_id, source, raw_path],
        )
        .unwrap();
    }

    fn delegation(conn: &Connection, locator: &str, child: &str) {
        conn.execute(
            "INSERT INTO session_relationships \
             (source, parent_session_id, relationship_uid, child_session_id, \
              relationship, identity_status, evidence_kind, evidence_locator, \
              created_ms, updated_ms) \
             VALUES ('claude', 'parent', ?1, ?2, 'delegated', 'observed', 'sidecar', ?3, 0, 0)",
            rusqlite::params![format!("edge-{child}"), child, locator],
        )
        .unwrap();
    }

    fn claude_file(dir: &Path, name: &str, session_id: &str, text: &str) -> PathBuf {
        let path = dir.join(name);
        fs::write(
            &path,
            format!(
                "{}\n",
                json!({
                    "type": "user",
                    "uuid": "u1",
                    "sessionId": session_id,
                    "cwd": "/tmp/p",
                    "timestamp": "2026-08-31T11:00:00Z",
                    "message": { "role": "user", "content": text },
                })
            ),
        )
        .unwrap();
        path
    }

    fn event_text(conn: &Connection, source: &str, session_id: &str) -> Vec<String> {
        let mut stmt = conn
            .prepare(
                "SELECT text FROM session_events \
                 WHERE source = ?1 AND session_id = ?2 ORDER BY event_uid",
            )
            .unwrap();
        stmt.query_map(rusqlite::params![source, session_id], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    }

    #[test]
    fn evidence_memo_matches_the_unarmed_probe() {
        let conn = db();
        session(&conn, "claude", "events", Some("/t/events.jsonl"));
        event(&conn, "claude", "events", "e", "hi");
        session(&conn, "claude", "marks", Some("/t/marks.jsonl"));
        marker(&conn, "claude", "marks", "m");
        session(&conn, "claude", "catalog", Some("/t/catalog.jsonl"));
        delegation(&conn, "/t/side-events.jsonl", "child-e");
        event(&conn, "claude", "child-e", "ce", "child");
        delegation(&conn, "/t/side-marks.jsonl", "child-m");
        marker(&conn, "claude", "child-m", "cm");
        delegation(&conn, "/t/side-empty.jsonl", "child-0");
        event(&conn, "claude", "orphan", "o", "no-edge");
        event(&conn, "codex", "cx", "x", "codex");
        marker(&conn, "codex", "cm", "cmk");

        let paths = [
            "/t/events.jsonl",
            "/t/marks.jsonl",
            "/t/catalog.jsonl",
            "/t/missing.jsonl",
            "/t/side-events.jsonl",
            "/t/side-marks.jsonl",
            "/t/side-empty.jsonl",
            "/t/side-none.jsonl",
        ];
        let mut transcript = Vec::new();
        let mut sidecar = Vec::new();
        for path in paths {
            transcript.push(claude_transcript_events_exist(&conn, Path::new(path)).unwrap());
            sidecar.push(claude_sidecar_evidence_exists(&conn, Path::new(path)).unwrap());
        }
        let codex = [
            codex_session_evidence_exists(&conn, "cx").unwrap(),
            codex_session_evidence_exists(&conn, "cm").unwrap(),
            codex_session_evidence_exists(&conn, "absent").unwrap(),
        ];
        assert_eq!(
            transcript,
            [true, true, false, false, false, false, false, false]
        );
        assert_eq!(
            sidecar,
            [false, false, false, false, true, true, false, false]
        );
        assert_eq!(codex, [true, true, false]);

        reset_counts();
        let _guard = Guard::arm(&conn);
        for (index, path) in paths.iter().enumerate() {
            assert_eq!(
                claude_transcript_events_exist(&conn, Path::new(path)).unwrap(),
                transcript[index],
                "{path}"
            );
            assert_eq!(
                claude_sidecar_evidence_exists(&conn, Path::new(path)).unwrap(),
                sidecar[index],
                "{path}"
            );
        }
        assert!(codex_session_evidence_exists(&conn, "cx").unwrap());
        assert!(codex_session_evidence_exists(&conn, "cm").unwrap());
        assert!(!codex_session_evidence_exists(&conn, "absent").unwrap());
        assert_eq!(load_count(), 3, "one read per probe kind, not per row");
        assert_eq!(fallback_count(), 0);
    }

    #[test]
    fn evidence_memo_a_delete_does_not_answer_present() {
        let conn = db();
        session(&conn, "claude", "s", Some("/t/s.jsonl"));
        event(&conn, "claude", "s", "e", "hi");
        marker(&conn, "claude", "s", "m");
        let path = Path::new("/t/s.jsonl");
        reset_counts();
        let _guard = Guard::arm(&conn);
        assert!(claude_transcript_events_exist(&conn, path).unwrap());
        let loaded = load_count();
        crate::retire_evidence_share(
            &conn,
            "session_events",
            "source = 'claude' AND session_id = 's'",
            &[],
            SessionLocation::Local,
        )
        .unwrap();
        assert!(
            claude_transcript_events_exist(&conn, path).unwrap(),
            "a marker still satisfies the probe"
        );
        assert!(
            load_count() > loaded,
            "the event delete reused the snapshot"
        );
        let loaded = load_count();
        crate::retire_evidence_share(
            &conn,
            "session_markers",
            "source = 'claude' AND session_id = 's'",
            &[],
            SessionLocation::Local,
        )
        .unwrap();
        assert!(!claude_transcript_events_exist(&conn, path).unwrap());
        assert!(load_count() > loaded);
        assert_eq!(fallback_count(), 0);
    }

    #[test]
    fn evidence_memo_a_retract_does_not_answer_present() {
        let conn = db();
        let path = Path::new("/t/side.jsonl");
        delegation(&conn, "/t/side.jsonl", "child");
        event(&conn, "claude", "child", "e", "hi");
        reset_counts();
        let _guard = Guard::arm(&conn);
        assert!(claude_sidecar_evidence_exists(&conn, path).unwrap());
        let loaded = load_count();
        super::super::retract_claude_delegation_evidence(&conn, path).unwrap();
        assert!(!claude_sidecar_evidence_exists(&conn, path).unwrap());
        assert!(load_count() > loaded);
    }

    #[test]
    fn evidence_memo_a_chunk_commit_drops_the_snapshot() {
        let conn = db();
        let path = Path::new("/t/a.jsonl");
        session(&conn, "claude", "s", Some("/t/a.jsonl"));
        event(&conn, "claude", "s", "e1", "hi");
        reset_counts();
        let _guard = Guard::arm(&conn);
        assert!(claude_transcript_events_exist(&conn, path).unwrap());
        let loaded = load_count();
        let mut write = super::super::SweepWrite::begin(&conn).unwrap();
        for _ in 0..super::super::JSONL_CHUNK_LINES {
            write.record().unwrap();
        }
        conn.execute("DELETE FROM session_events WHERE event_uid = 'e1'", [])
            .unwrap();
        assert!(
            !claude_transcript_events_exist(&conn, path).unwrap(),
            "the pre-commit snapshot still said present"
        );
        assert!(load_count() > loaded);
        drop(write);
    }

    #[test]
    fn evidence_memo_a_sweep_commit_drops_the_snapshot() {
        let conn = db();
        let path = Path::new("/t/a.jsonl");
        session(&conn, "claude", "s", Some("/t/a.jsonl"));
        event(&conn, "claude", "s", "e1", "hi");
        reset_counts();
        let _guard = Guard::arm(&conn);
        assert!(claude_transcript_events_exist(&conn, path).unwrap());
        let loaded = load_count();
        let write = super::super::SweepWrite::begin(&conn).unwrap();
        write.commit().unwrap();
        conn.execute("DELETE FROM session_events WHERE event_uid = 'e1'", [])
            .unwrap();
        assert!(!claude_transcript_events_exist(&conn, path).unwrap());
        assert!(
            load_count() > loaded,
            "the probe answered from the pre-commit snapshot"
        );
    }

    #[test]
    fn evidence_memo_disarms_when_the_walk_unwinds() {
        let conn = db();
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = Guard::arm(&conn);
            panic!("walk failed");
        }));
        assert!(panicked.is_err());
        assert_eq!(depth(), 0);
    }

    #[test]
    fn evidence_memo_a_steady_claude_sweep_reads_evidence_once() {
        let dir = tempfile::tempdir().unwrap();
        let conn = db();
        let mut kept = Vec::new();
        for index in 0..24 {
            let id = format!("s{index}");
            kept.push(claude_file(
                dir.path(),
                &format!("{id}.jsonl"),
                &id,
                "hello",
            ));
        }
        let gone = claude_file(dir.path(), "gone.jsonl", "gone", "come-back");
        claude_file(dir.path(), "agent-child.jsonl", "child", "child-result");
        // The sidecar rule needs every record to be a sidechain row.
        fs::write(
            dir.path().join("agent-child.jsonl"),
            format!(
                "{}\n",
                json!({
                    "type": "assistant",
                    "uuid": "side-a",
                    "sessionId": "parent-session",
                    "agentId": "child",
                    "isSidechain": true,
                    "cwd": "/tmp/p",
                    "timestamp": "2026-08-31T11:00:03Z",
                    "message": { "role": "assistant", "content": "child-result" },
                })
            ),
        )
        .unwrap();
        let parent = claude_file(dir.path(), "parent.jsonl", "parent-session", "parent-hello");
        let mut state = Map::new();
        super::super::sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();

        conn.execute(
            "DELETE FROM session_events WHERE source = 'claude' AND session_id = 'gone'",
            [],
        )
        .unwrap();
        conn.execute(
            "DELETE FROM session_markers WHERE source = 'claude' AND session_id = 'gone'",
            [],
        )
        .unwrap();
        let poisoned = conn
            .execute(
                "UPDATE session_events SET text = 'POISON' \
                 WHERE source = 'claude' AND session_id != 'gone'",
                [],
            )
            .unwrap();
        assert!(poisoned > 0, "the kept transcripts had nothing to poison");
        assert!(
            !claude_transcript_events_exist(&conn, &gone).unwrap(),
            "the wiped file is the probe's absent answer"
        );
        assert!(claude_transcript_events_exist(&conn, &kept[0]).unwrap());

        reset_counts();
        super::super::sync_claude_session_metadata(&conn, &mut state, dir.path()).unwrap();
        assert!(
            load_count() <= 3,
            "loads {} for {} files",
            load_count(),
            kept.len()
        );
        assert_eq!(fallback_count(), 0, "a per-file probe ran");
        assert_eq!(
            event_text(&conn, "claude", "gone"),
            vec!["come-back".to_string()]
        );
        assert_eq!(
            event_text(&conn, "claude", "s0"),
            vec!["POISON".to_string()]
        );
        assert_eq!(
            event_text(&conn, "claude", "s23"),
            vec!["POISON".to_string()]
        );
        assert_eq!(
            event_text(&conn, "claude", "parent-session"),
            vec!["POISON".to_string()]
        );
        let child = event_text(&conn, "claude", "child");
        assert_eq!(
            child,
            vec!["POISON".to_string()],
            "sidecar was re-read: {child:?}"
        );
        let _ = parent;
    }

    #[test]
    fn evidence_memo_a_steady_codex_sweep_reads_evidence_once() {
        let dir = tempfile::tempdir().unwrap();
        let day = dir.path().join(".codex/sessions/2026/04/20");
        fs::create_dir_all(&day).unwrap();
        let write = |name: &str, session: &str, message: &str| {
            let meta = json!({
                "timestamp": "2026-04-20T00:00:00.000Z",
                "type": "session_meta",
                "payload": {"id": session, "cwd": "/tmp/p"}
            });
            let event = json!({
                "timestamp": "2026-04-20T00:00:01.000Z",
                "type": "event_msg",
                "payload": {"type": "user_message", "message": message}
            });
            fs::write(day.join(name), format!("{meta}\n{event}\n")).unwrap();
        };
        for index in 0..16 {
            write(
                &format!("rollout-2026-04-20T00-00-{index:02}-sess{index}.jsonl"),
                &format!("sess{index}"),
                "kept-text",
            );
        }
        write(
            "rollout-2026-04-20T00-00-99-wipe.jsonl",
            "sess-wipe",
            "come-back",
        );
        fs::write(
            day.join("rollout-markers-only.jsonl"),
            concat!(
                r#"{"timestamp":"2026-09-12T00:00:00.000Z","type":"session_meta","payload":{"id":"sess-markers-only","cwd":"/tmp/proj"}}"#,
                "\n",
                r#"{"timestamp":"2026-09-12T00:00:00.100Z","type":"event_msg","payload":{"type":"task_started"}}"#,
                "\n",
            ),
        )
        .unwrap();
        let conn = db();
        let root = dir.path().join(".codex");
        let mut state = Map::new();
        super::super::sync_codex_rollouts(&conn, &mut state, &root).unwrap();
        conn.execute(
            "DELETE FROM session_events WHERE source = 'codex' AND session_id = 'sess-wipe'",
            [],
        )
        .unwrap();
        conn.execute(
            "DELETE FROM session_markers WHERE source = 'codex' AND session_id = 'sess-wipe'",
            [],
        )
        .unwrap();
        let poisoned = conn
            .execute(
                "UPDATE session_events SET text = 'POISON' \
                 WHERE source = 'codex' AND session_id != 'sess-wipe'",
                [],
            )
            .unwrap();
        assert!(poisoned > 0);
        conn.execute(
            "UPDATE session_markers SET text = 'POISON' \
             WHERE source = 'codex' AND session_id = 'sess-markers-only'",
            [],
        )
        .unwrap();
        assert!(!codex_session_evidence_exists(&conn, "sess-wipe").unwrap());
        assert!(codex_session_evidence_exists(&conn, "sess0").unwrap());
        assert!(codex_session_evidence_exists(&conn, "sess-markers-only").unwrap());

        reset_counts();
        super::super::sync_codex_rollouts(&conn, &mut state, &root).unwrap();
        assert!(load_count() <= 2, "loads {}", load_count());
        assert_eq!(fallback_count(), 0);
        assert_eq!(
            event_text(&conn, "codex", "sess-wipe"),
            vec!["come-back".to_string()]
        );
        assert_eq!(
            event_text(&conn, "codex", "sess0"),
            vec!["POISON".to_string()]
        );
        assert_eq!(
            event_text(&conn, "codex", "sess15"),
            vec!["POISON".to_string()]
        );
        let marker: String = conn
            .query_row(
                "SELECT text FROM session_markers \
                 WHERE source = 'codex' AND session_id = 'sess-markers-only'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(marker, "POISON");
    }
}
