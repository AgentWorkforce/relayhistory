use super::*;
use std::cell::Cell;
use std::fs;
use std::time::{Duration, SystemTime};

thread_local! {
    static READS: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
}

/// Count a store opened or a session read on this thread.
pub(super) fn note_read(stores: usize, sessions: usize) {
    READS.with(|reads| {
        let (opened, read) = reads.get();
        reads.set((opened + stores, read + sessions));
    });
}

/// `(stores opened, sessions read)` since the last call.
fn reads() -> (usize, usize) {
    READS.with(|reads| reads.replace((0, 0)))
}

const ROOT: &str = "ses_sqlite_root";
const CHILD: &str = "ses_sqlite_child";

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/opencode")
}

/// Move a file's mtime an hour back, past the window a stamp cannot vouch
/// for. A store written by the test itself would otherwise always be "just
/// written".
fn settle(path: &Path) {
    let hour_ago = SystemTime::now() - Duration::from_secs(3600);
    for suffix in ["", "-wal"] {
        let mut file = path.as_os_str().to_os_string();
        file.push(suffix);
        if let Ok(file) = fs::File::options().write(true).open(file) {
            file.set_modified(hour_ago).unwrap();
        }
    }
}

fn provider_store(path: &Path) -> Connection {
    let store = Connection::open(path).unwrap();
    store
        .execute_batch(&fs::read_to_string(fixtures().join("sqlite-store.sql")).unwrap())
        .unwrap();
    settle(path);
    store
}

struct Fixture {
    _dir: tempfile::TempDir,
    root: PathBuf,
    conn: Connection,
    state: Map<String, Value>,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let conn = crate::open_db(&root.join("ai-history.db")).unwrap();
        Self {
            _dir: dir,
            root,
            conn,
            state: Map::new(),
        }
    }

    fn sweep(&mut self, stores: &[PathBuf], repairs: &SweepRepairs) -> (usize, usize) {
        reads();
        let mut sweep = OpencodeSweep::begin(&mut self.state, repairs);
        sync_opencode_dbs(&self.conn, stores, &mut sweep).unwrap();
        sweep.finish(&mut self.state);
        reads()
    }

    fn sweep_tree(&mut self, tree: &Path) -> (usize, usize) {
        reads();
        let repairs = SweepRepairs::default();
        let mut sweep = OpencodeSweep::begin(&mut self.state, &repairs);
        sync_opencode_storage_dir(&self.conn, tree, &mut sweep).unwrap();
        sweep.finish(&mut self.state);
        reads()
    }

    fn events(&self, session_id: &str) -> i64 {
        self.conn
            .query_row(
                "SELECT COUNT(*) FROM session_events WHERE source = 'opencode' AND session_id = ?",
                [session_id],
                |row| row.get(0),
            )
            .unwrap()
    }
}

#[test]
fn an_unchanged_store_is_not_opened_again() {
    let mut fixture = Fixture::new();
    let db = fixture.root.join("opencode.db");
    provider_store(&db);
    let none = SweepRepairs::default();
    assert_eq!(fixture.sweep(std::slice::from_ref(&db), &none), (1, 2));
    let events = fixture.events(ROOT);
    assert!(events > 0);
    assert_eq!(fixture.sweep(std::slice::from_ref(&db), &none), (0, 0));
    assert_eq!(fixture.events(ROOT), events);
}

#[test]
fn a_changed_store_reads_only_the_session_that_changed() {
    let mut fixture = Fixture::new();
    let db = fixture.root.join("opencode.db");
    let store = provider_store(&db);
    let none = SweepRepairs::default();
    fixture.sweep(std::slice::from_ref(&db), &none);
    let before = fixture.events(ROOT);
    store
        .execute_batch(
            "INSERT INTO message (id, session_id, time_created, data) VALUES \
               ('msg_sqlite_u9', 'ses_sqlite_root', 1776643300000, \
                '{\"id\":\"msg_sqlite_u9\",\"sessionID\":\"ses_sqlite_root\",\"role\":\"user\",\"time\":{\"created\":1776643300000}}'); \
             INSERT INTO part (id, message_id, session_id, time_created, data) VALUES \
               ('prt_sqlite_u9', 'msg_sqlite_u9', 'ses_sqlite_root', 1776643300000, \
                '{\"type\":\"text\",\"text\":\"one more turn\"}');",
        )
        .unwrap();
    settle(&db);
    assert_eq!(fixture.sweep(std::slice::from_ref(&db), &none), (1, 1));
    assert_eq!(fixture.events(ROOT), before + 1);
    // An in-place rewrite that keeps the row count: the payload length moves.
    store
        .execute(
            "UPDATE part SET data = '{\"type\":\"text\",\"text\":\"one more turn, edited\"}' \
             WHERE id = 'prt_sqlite_u9'",
            [],
        )
        .unwrap();
    settle(&db);
    assert_eq!(fixture.sweep(std::slice::from_ref(&db), &none), (1, 1));
    let text: String = fixture
        .conn
        .query_row(
            "SELECT text FROM session_events WHERE session_id = ?1 AND message_id = 'msg_sqlite_u9'",
            [ROOT],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(text, "one more turn, edited");
}

#[test]
fn a_session_whose_evidence_is_gone_is_read_again() {
    let mut fixture = Fixture::new();
    let db = fixture.root.join("opencode.db");
    provider_store(&db);
    let none = SweepRepairs::default();
    fixture.sweep(std::slice::from_ref(&db), &none);
    let events = fixture.events(CHILD);
    assert!(events > 0);
    fixture
        .conn
        .execute_batch(
            "DELETE FROM session_events WHERE source = 'opencode' AND session_id = 'ses_sqlite_child'; \
             DELETE FROM session_markers WHERE source = 'opencode' AND session_id = 'ses_sqlite_child';",
        )
        .unwrap();
    assert_eq!(fixture.sweep(std::slice::from_ref(&db), &none), (1, 1));
    assert_eq!(fixture.events(CHILD), events);
}

#[test]
fn a_session_the_destination_marker_names_short_is_read_again() {
    let mut fixture = Fixture::new();
    let db = fixture.root.join("opencode.db");
    provider_store(&db);
    fixture.sweep(std::slice::from_ref(&db), &SweepRepairs::default());
    let short = SweepRepairs {
        sessions: [("opencode".to_string(), ROOT.to_string())].into(),
        all: false,
    };
    assert_eq!(fixture.sweep(std::slice::from_ref(&db), &short), (1, 1));
    assert_eq!(
        fixture.sweep(std::slice::from_ref(&db), &SweepRepairs::all()),
        (1, 2)
    );
}

#[test]
fn a_session_written_moments_ago_is_not_stamped() {
    let mut fixture = Fixture::new();
    let db = fixture.root.join("opencode.db");
    let store = provider_store(&db);
    let none = SweepRepairs::default();
    fixture.sweep(std::slice::from_ref(&db), &none);
    store
        .execute(
            "UPDATE session SET time_updated = ?1 WHERE id = ?2",
            rusqlite::params![stamps::now_ms(), ROOT],
        )
        .unwrap();
    // The store file is as recent as the write: no store stamp either.
    assert_eq!(fixture.sweep(std::slice::from_ref(&db), &none), (1, 1));
    settle(&db);
    // Neither stamp was recorded, so the session is read once more.
    assert_eq!(fixture.sweep(std::slice::from_ref(&db), &none), (1, 1));
}

#[test]
fn a_session_an_earlier_store_drops_is_read_from_the_next_one() {
    let mut fixture = Fixture::new();
    let latest = fixture.root.join("opencode.db");
    let nightly = fixture.root.join("opencode-nightly.db");
    let first = provider_store(&latest);
    provider_store(&nightly);
    let stores = [latest.clone(), nightly.clone()];
    let none = SweepRepairs::default();
    // Every session is the first store's; the second holds only claims.
    assert_eq!(fixture.sweep(&stores, &none), (2, 2));
    assert_eq!(fixture.sweep(&stores, &none), (0, 0));
    first
        .execute_batch(
            "DELETE FROM part WHERE session_id = 'ses_sqlite_child'; \
             DELETE FROM message WHERE session_id = 'ses_sqlite_child'; \
             DELETE FROM session WHERE id = 'ses_sqlite_child';",
        )
        .unwrap();
    settle(&latest);
    // The second store's files did not move, but it owns the session now.
    assert_eq!(fixture.sweep(&stores, &none), (2, 1));
    let raw_path: String = fixture
        .conn
        .query_row(
            "SELECT raw_path FROM sessions WHERE source = 'opencode' AND session_id = ?",
            [CHILD],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(Path::new(&raw_path), nightly);
    assert_eq!(fixture.sweep(&stores, &none), (0, 0));
}

fn copy_tree(from: &Path, to: &Path) {
    for entry in fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            fs::create_dir_all(&target).unwrap();
            copy_tree(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), &target).unwrap();
        }
    }
}

#[test]
fn the_json_tree_reads_only_the_session_that_changed() {
    let mut fixture = Fixture::new();
    let tree = fixture.root.join("storage");
    fs::create_dir_all(&tree).unwrap();
    for corpus in ["legacy-json-simple", "legacy-json-multi-turn"] {
        copy_tree(&fixtures().join(corpus).join("storage"), &tree);
    }
    assert_eq!(fixture.sweep_tree(&tree), (0, 3));
    assert_eq!(fixture.sweep_tree(&tree), (0, 0));
    let part = tree.join("part/msg_simple_asst/prt_simple_1.json");
    let body = fs::read_to_string(&part).unwrap();
    fs::write(&part, body.replacen('}', ",\"edited\":true}", 1)).unwrap();
    assert_eq!(fixture.sweep_tree(&tree), (0, 1));
}

#[test]
fn a_same_length_rewrite_is_read_again() {
    let mut fixture = Fixture::new();
    let db = fixture.root.join("opencode.db");
    let store = provider_store(&db);
    let none = SweepRepairs::default();
    fixture.sweep(std::slice::from_ref(&db), &none);
    let (part, data): (String, String) = store
        .query_row(
            "SELECT id, data FROM part WHERE session_id = ?1 AND data LIKE '%\"text\":\"%' LIMIT 1",
            [ROOT],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let start = data.find("\"text\":\"").unwrap() + 8;
    let mut rewritten = data.clone();
    rewritten.replace_range(
        start..start + 1,
        if &data[start..start + 1] == "Z" {
            "Y"
        } else {
            "Z"
        },
    );
    assert_eq!(rewritten.len(), data.len());
    // No `time_updated` in this schema: only the payload changed.
    store
        .execute(
            "UPDATE part SET data = ?1 WHERE id = ?2",
            [&rewritten, &part],
        )
        .unwrap();
    settle(&db);
    assert_eq!(fixture.sweep(std::slice::from_ref(&db), &none), (1, 1));
}

#[test]
fn a_lost_catalog_row_or_parent_edge_is_read_again() {
    let mut fixture = Fixture::new();
    let db = fixture.root.join("opencode.db");
    provider_store(&db);
    let none = SweepRepairs::default();
    fixture.sweep(std::slice::from_ref(&db), &none);
    let edge = |conn: &Connection| -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM session_relationships \
             WHERE source = 'opencode' AND child_session_id = ?1",
            [CHILD],
            |row| row.get(0),
        )
        .unwrap()
    };
    assert_eq!(edge(&fixture.conn), 1);
    fixture
        .conn
        .execute(
            "DELETE FROM session_relationships WHERE source = 'opencode' AND child_session_id = ?1",
            [CHILD],
        )
        .unwrap();
    assert_eq!(fixture.sweep(std::slice::from_ref(&db), &none), (1, 1));
    assert_eq!(edge(&fixture.conn), 1);
    fixture
        .conn
        .execute(
            "DELETE FROM sessions WHERE source = 'opencode' AND session_id = ?1",
            [ROOT],
        )
        .unwrap();
    // The delete cascades the root's own evidence and its edges; reading it
    // restores the session, and its child's edge is the child's to restore.
    let (opened, read) = fixture.sweep(std::slice::from_ref(&db), &none);
    assert_eq!(opened, 1);
    assert!(read >= 1);
    let catalog: i64 = fixture
        .conn
        .query_row(
            "SELECT COUNT(*) FROM sessions WHERE source = 'opencode' AND session_id = ?1",
            [ROOT],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(catalog, 1);
    assert_eq!(edge(&fixture.conn), 1);
}

/// With `time_updated`, the stamp reads no payload: a rewrite moves the
/// row's `time_updated`, as OpenCode writes it, and that is what is seen.
#[test]
fn a_rewrite_that_moves_time_updated_is_read_again() {
    let mut fixture = Fixture::new();
    let db = fixture.root.join("opencode.db");
    let store = provider_store(&db);
    store
        .execute_batch(
            "ALTER TABLE message ADD COLUMN time_updated INTEGER; \
             ALTER TABLE part ADD COLUMN time_updated INTEGER; \
             UPDATE message SET time_updated = time_created; \
             UPDATE part SET time_updated = time_created;",
        )
        .unwrap();
    settle(&db);
    let none = SweepRepairs::default();
    fixture.sweep(std::slice::from_ref(&db), &none);
    assert_eq!(fixture.sweep(std::slice::from_ref(&db), &none), (0, 0));
    store
        .execute(
            "UPDATE part SET time_updated = time_updated + 1, \
             data = replace(data, 'add a retry', 'add a RETRY') WHERE id = 'prt_sqlite_u1_text'",
            [],
        )
        .unwrap();
    settle(&db);
    assert_eq!(fixture.sweep(std::slice::from_ref(&db), &none), (1, 1));
}
