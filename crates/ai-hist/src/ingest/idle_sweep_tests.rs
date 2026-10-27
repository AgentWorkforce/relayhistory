//! A full Codex sweep over a store that already holds every rollout redoes
//! none of the per-file work it did to get there: no recorded subagent is
//! repaired again, no evidence key set is read again, and no compressed
//! rollout is measured again.

use super::{codex_subagent, evidence_probe, sync_basic, transcript_file, SweepScope};
use rusqlite::Connection;
use serde_json::json;
use std::fs;
use std::path::PathBuf;

struct Store {
    _dir: tempfile::TempDir,
    db: PathBuf,
    roots: crate::ProviderRoots,
    conn: Connection,
    day: PathBuf,
}

impl Store {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("store/ai-history.db");
        fs::create_dir_all(db.parent().unwrap()).unwrap();
        let roots = crate::ProviderRoots::from_home(
            dir.path().to_path_buf(),
            dir.path().join("opencode.db"),
        );
        let day = roots.codex.join("sessions/2026/04/20");
        fs::create_dir_all(&day).unwrap();
        let conn = crate::open_db(&db).unwrap();
        Self {
            _dir: dir,
            db,
            roots,
            conn,
            day,
        }
    }

    fn sweep(&self) {
        sync_basic(
            &self.conn,
            &self.db,
            &self.roots,
            true,
            &SweepScope::only(["codex"]),
        )
        .unwrap();
    }

    /// Run a sweep and say what per-file work it did.
    fn counted_sweep(&self) -> Work {
        codex_subagent::take_repairs();
        transcript_file::take_measurements();
        evidence_probe::reset_counts();
        self.sweep();
        Work {
            repairs: codex_subagent::take_repairs(),
            measurements: transcript_file::take_measurements(),
            loads: evidence_probe::load_count(),
            adoptions: evidence_probe::adoption_count(),
        }
    }

    fn rollout(&self, name: &str, meta: serde_json::Value, compressed: bool) -> PathBuf {
        let records = [
            json!({"timestamp": "2026-04-20T00:00:00.000Z", "type": "session_meta", "payload": meta}),
            json!({"timestamp": "2026-04-20T00:00:01.000Z", "type": "event_msg",
                   "payload": {"type": "user_message", "message": format!("task in {name}")}}),
            json!({"timestamp": "2026-04-20T00:00:02.000Z", "type": "event_msg",
                   "payload": {"type": "agent_message", "message": "done"}}),
        ]
        .iter()
        .map(|record| format!("{record}\n"))
        .collect::<String>();
        let path = self
            .day
            .join(format!("rollout-2026-04-20T00-00-00-{name}.jsonl"));
        if compressed {
            let stored = transcript_file::compressed_path(&path);
            fs::write(
                &stored,
                zstd::bulk::compress(records.as_bytes(), 3).unwrap(),
            )
            .unwrap();
            stored
        } else {
            fs::write(&path, records).unwrap();
            path
        }
    }

    /// Codex's own thread index, naming each rollout by its locator. The
    /// SQLite-only thread selection stats every one of them on every sweep
    /// to ask whether the rollout covers the projection.
    fn thread_index(&self, threads: &[(&str, &PathBuf)]) {
        let codex = &self.roots.codex;
        let state = Connection::open(codex.join("state_5.sqlite")).unwrap();
        state
            .execute_batch(
                "CREATE TABLE threads(id TEXT PRIMARY KEY, rollout_path TEXT, created_at_ms INTEGER, \
                 updated_at_ms INTEGER, source TEXT, cwd TEXT, git_branch TEXT, git_origin_url TEXT, \
                 title TEXT, name TEXT, first_user_message TEXT, originator TEXT, cli_version TEXT, \
                 model TEXT, tokens_used INTEGER, archived INTEGER, agent_nickname TEXT, \
                 agent_role TEXT, history_mode TEXT, thread_source TEXT); \
                 CREATE TABLE thread_spawn_edges(parent_thread_id TEXT,child_thread_id TEXT,status TEXT);",
            )
            .unwrap();
        let history = Connection::open(codex.join("thread_history_1.sqlite")).unwrap();
        history
            .execute_batch(
                "CREATE TABLE thread_history_projection_state(thread_id TEXT PRIMARY KEY, \
                 next_rollout_ordinal INTEGER,next_rollout_byte_offset INTEGER); \
                 CREATE TABLE thread_items(thread_id TEXT,turn_id TEXT,item_id TEXT, \
                 rollout_ordinal INTEGER,created_at_ms INTEGER,item_type TEXT,item_json TEXT); \
                 CREATE TABLE thread_turns(thread_id TEXT,turn_id TEXT,rollout_ordinal INTEGER, \
                 started_at INTEGER,error_json TEXT);",
            )
            .unwrap();
        for (id, stored) in threads {
            let locator = transcript_file::locator_of(stored);
            state
                .execute(
                    "INSERT INTO threads (id,rollout_path,created_at_ms,updated_at_ms,source,archived,history_mode) \
                     VALUES (?1,?2,1776643200000,1776643300000,'cli',0,'paginated')",
                    rusqlite::params![id, locator.to_string_lossy()],
                )
                .unwrap();
            history
                .execute(
                    "INSERT INTO thread_history_projection_state VALUES (?1,1,1)",
                    [id],
                )
                .unwrap();
        }
    }

    fn count(&self, sql: &str) -> i64 {
        self.conn.query_row(sql, [], |row| row.get(0)).unwrap()
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Work {
    repairs: u64,
    measurements: u64,
    loads: u64,
    adoptions: u64,
}

fn root(id: &str) -> serde_json::Value {
    json!({"id": id, "cwd": "/tmp/p"})
}

fn subagent(id: &str, parent: &str) -> serde_json::Value {
    json!({"id": id, "parent_thread_id": parent, "cwd": "/tmp/p",
           "thread_source": "subagent", "source": {"subagent": {"other": "reviewer"}}})
}

/// A store with plain and compressed roots and subagents, swept until it
/// holds everything.
fn settled() -> Store {
    let store = Store::new();
    let rollouts = [
        (
            "root-plain",
            store.rollout("root-plain", root("root-plain"), false),
        ),
        (
            "root-zst",
            store.rollout("root-zst", root("root-zst"), true),
        ),
        (
            "child-plain",
            store.rollout("child-plain", subagent("child-plain", "root-plain"), false),
        ),
        (
            "child-zst",
            store.rollout("child-zst", subagent("child-zst", "root-zst"), true),
        ),
    ];
    let threads: Vec<(&str, &PathBuf)> = rollouts.iter().map(|(id, path)| (*id, path)).collect();
    store.thread_index(&threads);
    store.sweep();
    store.sweep();
    store
}

#[test]
fn a_sweep_with_nothing_new_redoes_no_per_file_work() {
    let store = settled();
    assert_eq!(
        store.count(
            "SELECT COUNT(*) FROM session_relationships \
             WHERE source = 'codex' AND relationship = 'delegated'"
        ),
        2
    );
    let head = store.count("SELECT version FROM observation_clock");
    let work = store.counted_sweep();
    assert_eq!(
        work,
        Work {
            repairs: 0,
            measurements: 0,
            loads: 0,
            adoptions: work.adoptions,
        },
        "a sweep over an unchanged store redid per-file work"
    );
    assert!(work.adoptions > 0, "the walk never asked about evidence");
    assert_eq!(
        store.count("SELECT version FROM observation_clock"),
        head,
        "an idle sweep wrote"
    );
}

/// The carried evidence set follows the store, not the walk: a delete this
/// connection makes between sweeps (which does not move `data_version` for
/// it) moves the change-feed head, and the next sweep reads the keys again
/// and re-ingests the session whose evidence went.
#[test]
fn a_delete_between_sweeps_is_seen_by_the_next() {
    let store = settled();
    store
        .conn
        .execute(
            "DELETE FROM session_events WHERE source = 'codex' AND session_id = 'root-plain'",
            [],
        )
        .unwrap();
    store
        .conn
        .execute(
            "DELETE FROM session_markers WHERE source = 'codex' AND session_id = 'root-plain'",
            [],
        )
        .unwrap();
    let work = store.counted_sweep();
    assert!(
        work.loads > 0,
        "the sweep trusted a set from before the delete"
    );
    assert!(
        store.count(
            "SELECT COUNT(*) FROM session_events \
             WHERE source = 'codex' AND session_id = 'root-plain'"
        ) > 0,
        "the session whose evidence was deleted was not re-ingested"
    );
}

/// A registration recreated behind the walk's back is repaired again: the
/// skip is a statement about the store, not a flag that the repair ran.
#[test]
fn a_recreated_subagent_registration_is_repaired_again() {
    let store = settled();
    store
        .conn
        .execute(
            "INSERT INTO sessions (session_id, source, cwd) VALUES ('child-zst', 'codex', '/tmp/p')",
            [],
        )
        .unwrap();
    crate::mark_session_presence(
        &store.conn,
        "codex",
        "child-zst",
        crate::SessionLocation::Local,
    )
    .unwrap();
    let work = store.counted_sweep();
    assert_eq!(work.repairs, 1);
    assert_eq!(
        store.count(
            "SELECT (SELECT COUNT(*) FROM sessions WHERE source = 'codex' AND session_id = 'child-zst') \
                  + (SELECT COUNT(*) FROM session_presences \
                     WHERE source = 'codex' AND session_id = 'child-zst')"
        ),
        0
    );
    assert_eq!(
        store.count(
            "SELECT COUNT(*) FROM session_relationships \
             WHERE source = 'codex' AND child_session_id = 'child-zst' \
               AND relationship = 'delegated'"
        ),
        1,
        "the delegation edge did not survive the repair"
    );
    assert_eq!(store.counted_sweep().repairs, 0);
}

/// A compressed rollout replaced by new bytes is measured again.
#[test]
fn a_recompressed_rollout_is_measured_again() {
    let store = settled();
    let path = store.rollout("root-zst", root("root-zst"), true);
    // Grow it: the same name over new bytes.
    let mut body = Vec::new();
    std::io::Read::read_to_end(&mut transcript_file::open(&path).unwrap(), &mut body).unwrap();
    body.extend_from_slice(
        format!(
            "{}\n",
            json!({"timestamp": "2026-04-20T00:00:03.000Z", "type": "event_msg",
                   "payload": {"type": "user_message", "message": "one more"}})
        )
        .as_bytes(),
    );
    fs::write(&path, zstd::bulk::compress(&body, 3).unwrap()).unwrap();
    assert!(store.counted_sweep().measurements > 0);
    assert_eq!(
        store.count(
            "SELECT COUNT(*) FROM session_events WHERE source = 'codex' AND text = 'one more'"
        ),
        1
    );
}
