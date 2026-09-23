//! Integration tests for the `devin` source: `sessions.db` ingestion,
//! shallow discovery, targeted hydration, incremental re-reads and WAL-safety.
//!
//! Environment-mutating tests serialize on [`ENV_LOCK`]; discovery-only tests
//! use `DiscoveryEnv::with_roots`, which never reads the process environment.

use ai_hist::{
    discover_sessions_with_env, export_json, hydrate_session_at, open_db, session_events,
    session_file_edits, session_markers, session_tool_calls, sync_scoped_at, DiscoverOptions,
    DiscoveryEnv, HydrateSessionOptions, SessionScope,
};
use rusqlite::Connection;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

static ENV_LOCK: Mutex<()> = Mutex::new(());

/// Set every provider-root environment variable at the staged home and
/// restore them all on drop. Tests that mutate the process environment hold
/// [`ENV_LOCK`] for their whole body.
struct EnvGuard {
    saved: Vec<(&'static str, Option<OsString>)>,
}

impl EnvGuard {
    fn set(home: &Path) -> Self {
        let vars: Vec<(&'static str, Option<PathBuf>)> = vec![
            ("HOME", Some(home.to_path_buf())),
            ("USERPROFILE", Some(home.to_path_buf())),
            ("XDG_DATA_HOME", Some(home.join(".local/share"))),
            ("CLAUDE_CONFIG_DIR", Some(home.join("missing-claude"))),
            ("CODEX_HOME", Some(home.join("missing-codex"))),
            ("GROK_HOME", Some(home.join("missing-grok"))),
            ("OPENCODE_DB", Some(home.join("missing-opencode.db"))),
            (
                "OPENCODE_STORAGE_DIR",
                Some(home.join("missing-opencode-storage")),
            ),
            ("AI_HIST_DB", None),
        ];
        let saved = vars
            .iter()
            .map(|(name, _)| (*name, std::env::var_os(name)))
            .collect();
        for (name, value) in vars {
            match value {
                Some(path) => std::env::set_var(name, path),
                None => std::env::remove_var(name),
            }
        }
        Self { saved }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (name, value) in self.saved.drain(..) {
            match value {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }
    }
}

const DEVIN_SCHEMA: &str = r#"
CREATE TABLE sessions (
  id TEXT PRIMARY KEY,
  working_directory TEXT NOT NULL,
  backend_type TEXT NOT NULL,
  model TEXT NOT NULL,
  agent_mode TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  last_activity_at INTEGER NOT NULL,
  title TEXT,
  main_chain_id INTEGER,
  shell_last_seen_index INTEGER DEFAULT 0,
  cogs_json TEXT,
  workspace_dirs TEXT,
  hidden INTEGER NOT NULL DEFAULT 0,
  metadata TEXT
);
CREATE TABLE message_nodes (
  row_id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL,
  node_id INTEGER NOT NULL,
  parent_node_id INTEGER,
  chat_message TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  metadata TEXT,
  FOREIGN KEY (session_id) REFERENCES sessions(id),
  UNIQUE(session_id, node_id)
);
CREATE TABLE tool_call_state (
  session_id TEXT NOT NULL,
  tool_call_id TEXT NOT NULL,
  tool_call_json TEXT,
  tool_call_update_json TEXT,
  PRIMARY KEY (session_id, tool_call_id),
  FOREIGN KEY (session_id) REFERENCES sessions(id)
);
"#;

fn devin_cli_dir(home: &Path) -> PathBuf {
    home.join(".local/share/devin/cli")
}

/// Create `~/.local/share/devin/cli/sessions.db` with the schema and `sql`.
fn stage_devin_db(home: &Path, sql: &str) -> PathBuf {
    let cli_dir = devin_cli_dir(home);
    fs::create_dir_all(&cli_dir).unwrap();
    let db_path = cli_dir.join("sessions.db");
    let conn = Connection::open(&db_path).unwrap();
    conn.execute_batch(DEVIN_SCHEMA).unwrap();
    conn.execute_batch(sql).unwrap();
    db_path
}

/// The minimal session: one user prompt, one assistant reply.
const BASE_SESSION_SQL: &str = r#"
INSERT INTO sessions
  (id, working_directory, backend_type, model, agent_mode, created_at,
   last_activity_at, title, workspace_dirs, hidden, metadata)
VALUES
  ('devin-test', '/work/repo', 'devin', 'test-model', 'normal',
   1776643200, 1776643202, 'Test session', '["/work/repo"]', 0, NULL);
INSERT INTO message_nodes
  (session_id, node_id, parent_node_id, chat_message, created_at, metadata)
VALUES
  ('devin-test', 0, NULL,
   '{"message_id":"u0","role":"user","content":"first prompt","metadata":{"is_user_input":true},"tool_calls":null,"thinking":null,"tool_call_id":null,"phase":null}',
   1776643201, NULL),
  ('devin-test', 1, 0,
   '{"message_id":"a0","role":"assistant","content":"first answer","thinking":null,"metadata":{"num_tokens":8,"generation_model":"test-model"},"tool_calls":null,"tool_call_id":null,"phase":null}',
   1776643202, NULL);
"#;

fn count(conn: &Connection, sql: &str) -> i64 {
    conn.query_row(sql, [], |row| row.get(0)).unwrap()
}

#[test]
fn devin_sqlite_store_ingests_events_tools_and_edits() {
    let _lock = ENV_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    stage_devin_db(home, BASE_SESSION_SQL);
    let _env = EnvGuard::set(home);
    let db = home.join("history.db");

    sync_scoped_at(&db, SessionScope::Local).unwrap();

    let conn = open_db(&db).unwrap();
    assert_eq!(
        count(
            &conn,
            "SELECT COUNT(*) FROM sessions WHERE source='devin' AND session_id='devin-test'"
        ),
        1
    );
    assert_eq!(
        count(&conn, "SELECT COUNT(*) FROM history WHERE source='devin'"),
        1
    );
    let events = session_events(&conn, "devin-test", Some("devin")).unwrap();
    assert_eq!(events.len(), 2);
    // Epoch seconds are converted to milliseconds.
    let (first, last): (i64, i64) = conn
        .query_row(
            "SELECT first_activity_ms, last_activity_ms FROM sessions \
             WHERE source='devin' AND session_id='devin-test'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!((first, last), (1776643200000, 1776643202000));
    // The title rides along as a bounded marker: sessions has no title column.
    let markers = session_markers(&conn, "devin", "devin-test").unwrap();
    assert!(markers
        .iter()
        .any(|m| m.kind == "session_title" && m.text.as_deref() == Some("Test session")));
}

#[test]
fn devin_incremental_sync_and_idempotent_resync() {
    let _lock = ENV_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let store = stage_devin_db(home, BASE_SESSION_SQL);
    let _env = EnvGuard::set(home);
    let db = home.join("history.db");

    sync_scoped_at(&db, SessionScope::Local).unwrap();
    let conn = open_db(&db).unwrap();
    let events_before = count(
        &conn,
        "SELECT COUNT(*) FROM session_events WHERE source='devin'",
    );
    assert_eq!(events_before, 2);

    // A second sync over an unchanged store is a no-op.
    sync_scoped_at(&db, SessionScope::Local).unwrap();
    let conn = open_db(&db).unwrap();
    assert_eq!(
        count(
            &conn,
            "SELECT COUNT(*) FROM session_events WHERE source='devin'"
        ),
        events_before
    );

    // Appending a node and bumping last_activity_at moves the stamp.
    let provider = Connection::open(&store).unwrap();
    provider
        .execute_batch(
            "INSERT INTO message_nodes \
             (session_id, node_id, parent_node_id, chat_message, created_at, metadata) \
             VALUES ('devin-test', 2, 1, \
             '{\"message_id\":\"u1\",\"role\":\"user\",\"content\":\"second prompt\",\"metadata\":{\"is_user_input\":true},\"tool_calls\":null,\"thinking\":null,\"tool_call_id\":null,\"phase\":null}', \
             1776643300, NULL); \
             UPDATE sessions SET last_activity_at = 1776643300 WHERE id = 'devin-test';",
        )
        .unwrap();
    drop(provider);

    sync_scoped_at(&db, SessionScope::Local).unwrap();
    let conn = open_db(&db).unwrap();
    assert_eq!(
        count(
            &conn,
            "SELECT COUNT(*) FROM session_events WHERE source='devin'"
        ),
        3
    );
    assert_eq!(
        count(&conn, "SELECT COUNT(*) FROM history WHERE source='devin'"),
        2
    );
}

#[test]
fn devin_reads_store_while_wal_writer_holds_transaction() {
    let _lock = ENV_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let store = stage_devin_db(home, BASE_SESSION_SQL);
    let _env = EnvGuard::set(home);
    let db = home.join("history.db");

    // Put the provider store in WAL mode and hold a write transaction open
    // across the sync. The read path must still see the committed snapshot.
    let writer = Connection::open(&store).unwrap();
    writer.pragma_update(None, "journal_mode", "WAL").unwrap();
    writer.execute_batch("BEGIN IMMEDIATE").unwrap();
    writer
        .execute_batch(
            "INSERT INTO message_nodes \
             (session_id, node_id, parent_node_id, chat_message, created_at, metadata) \
             VALUES ('devin-test', 2, 1, \
             '{\"message_id\":\"u1\",\"role\":\"user\",\"content\":\"in-flight prompt\",\"metadata\":{\"is_user_input\":true},\"tool_calls\":null,\"thinking\":null,\"tool_call_id\":null,\"phase\":null}', \
             1776643300, NULL);",
        )
        .unwrap();

    sync_scoped_at(&db, SessionScope::Local).unwrap();
    let conn = open_db(&db).unwrap();
    // The uncommitted row must not be visible — the committed snapshot only.
    assert_eq!(
        count(
            &conn,
            "SELECT COUNT(*) FROM session_events WHERE source='devin'"
        ),
        2
    );

    writer.execute_batch("COMMIT").unwrap();
    writer
        .execute_batch("UPDATE sessions SET last_activity_at = 1776643300 WHERE id = 'devin-test';")
        .unwrap();
    drop(writer);

    sync_scoped_at(&db, SessionScope::Local).unwrap();
    let conn = open_db(&db).unwrap();
    assert_eq!(
        count(
            &conn,
            "SELECT COUNT(*) FROM session_events WHERE source='devin'"
        ),
        3
    );
}

#[test]
fn devin_xdg_data_home_redirects_the_root() {
    let _lock = ENV_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join("home");
    let xdg = dir.path().join("custom-xdg");
    // Stage the store under the XDG location, not the home default.
    let cli_dir = xdg.join("devin/cli");
    fs::create_dir_all(&cli_dir).unwrap();
    fs::create_dir_all(&home).unwrap();
    let conn = Connection::open(cli_dir.join("sessions.db")).unwrap();
    conn.execute_batch(DEVIN_SCHEMA).unwrap();
    conn.execute_batch(BASE_SESSION_SQL).unwrap();
    drop(conn);

    let saved_xdg = std::env::var_os("XDG_DATA_HOME");
    let _env = EnvGuard::set(&home);
    std::env::set_var("XDG_DATA_HOME", &xdg);
    let db = home.join("history.db");

    sync_scoped_at(&db, SessionScope::Local).unwrap();
    match saved_xdg {
        Some(value) => std::env::set_var("XDG_DATA_HOME", value),
        None => std::env::remove_var("XDG_DATA_HOME"),
    }

    let conn = open_db(&db).unwrap();
    assert_eq!(
        count(
            &conn,
            "SELECT COUNT(*) FROM sessions WHERE source='devin' AND session_id='devin-test'"
        ),
        1,
        "session under XDG_DATA_HOME was not ingested"
    );
}

#[test]
fn devin_hidden_and_malformed_rows_are_skipped() {
    let _lock = ENV_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    stage_devin_db(
        home,
        r#"
INSERT INTO sessions
  (id, working_directory, backend_type, model, agent_mode, created_at,
   last_activity_at, title, workspace_dirs, hidden, metadata)
VALUES
  ('devin-visible', '/work/repo', 'devin', 'test-model', 'normal',
   1776643200, 1776643202, NULL, '[]', 0, NULL),
  ('devin-hidden', '/work/repo', 'devin', 'test-model', 'normal',
   1776643200, 1776643202, 'hidden', '[]', 1, NULL);
INSERT INTO message_nodes
  (session_id, node_id, parent_node_id, chat_message, created_at, metadata)
VALUES
  ('devin-visible', 0, NULL, 'not-json{{{', 1776643201, NULL),
  ('devin-visible', 1, 0,
   '{"message_id":"u0","role":"user","content":"visible prompt","metadata":{"is_user_input":true},"tool_calls":null,"thinking":null,"tool_call_id":null,"phase":null}',
   1776643201, NULL),
  ('devin-hidden', 0, NULL,
   '{"message_id":"h0","role":"user","content":"hidden prompt","metadata":{"is_user_input":true},"tool_calls":null,"thinking":null,"tool_call_id":null,"phase":null}',
   1776643201, NULL);
"#,
    );
    let _env = EnvGuard::set(home);
    let db = home.join("history.db");

    sync_scoped_at(&db, SessionScope::Local).unwrap();
    let conn = open_db(&db).unwrap();
    assert_eq!(
        count(
            &conn,
            "SELECT COUNT(*) FROM sessions WHERE source='devin' AND session_id='devin-visible'"
        ),
        1
    );
    assert_eq!(
        count(
            &conn,
            "SELECT COUNT(*) FROM sessions WHERE source='devin' AND session_id='devin-hidden'"
        ),
        0,
        "hidden session must never be indexed"
    );
    // The malformed node is skipped; the valid prompt still lands.
    let events = session_events(&conn, "devin-visible", Some("devin")).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].role, "user");
}

/// A second session for lifecycle tests: two prompts, one assistant reply.
const SECOND_SESSION_SQL: &str = r#"
INSERT INTO sessions
  (id, working_directory, backend_type, model, agent_mode, created_at,
   last_activity_at, title, workspace_dirs, hidden, metadata)
VALUES
  ('devin-extra', '/work/other', 'devin', 'test-model', 'normal',
   1776643300, 1776643304, 'Extra session', '["/work/other"]', 0, NULL);
INSERT INTO message_nodes
  (session_id, node_id, parent_node_id, chat_message, created_at, metadata)
VALUES
  ('devin-extra', 0, NULL,
   '{"message_id":"x0","role":"user","content":"extra prompt","metadata":{"is_user_input":true},"tool_calls":null,"thinking":null,"tool_call_id":null,"phase":null}',
   1776643301, NULL),
  ('devin-extra', 1, 0,
   '{"message_id":"x1","role":"assistant","content":"extra answer","thinking":null,"metadata":{"num_tokens":4,"generation_model":"test-model"},"tool_calls":null,"tool_call_id":null,"phase":null}',
   1776643304, NULL);
"#;

fn devin_counts(conn: &Connection, session_id: &str) -> (i64, i64, i64, i64) {
    let sessions = count(
        conn,
        &format!(
            "SELECT COUNT(*) FROM sessions WHERE source='devin' AND session_id='{session_id}'"
        ),
    );
    let events = count(
        conn,
        &format!(
            "SELECT COUNT(*) FROM session_events WHERE source='devin' AND session_id='{session_id}'"
        ),
    );
    let prompts = count(
        conn,
        &format!("SELECT COUNT(*) FROM history WHERE source='devin' AND session_id='{session_id}'"),
    );
    let markers = count(
        conn,
        &format!(
            "SELECT COUNT(*) FROM session_markers WHERE source='devin' AND session_id='{session_id}'"
        ),
    );
    (sessions, events, prompts, markers)
}

#[test]
fn devin_retires_hidden_and_deleted_sessions_and_reindexes_on_unhide() {
    let _lock = ENV_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let store = stage_devin_db(home, &format!("{BASE_SESSION_SQL}{SECOND_SESSION_SQL}"));
    let _env = EnvGuard::set(home);
    let db = home.join("history.db");

    sync_scoped_at(&db, SessionScope::Local).unwrap();
    let conn = open_db(&db).unwrap();
    assert!(devin_counts(&conn, "devin-extra").0 == 1);

    // Hiding an indexed session retires its catalog row and evidence.
    let provider = Connection::open(&store).unwrap();
    provider
        .execute_batch("UPDATE sessions SET hidden = 1 WHERE id = 'devin-extra'")
        .unwrap();
    drop(provider);
    sync_scoped_at(&db, SessionScope::Local).unwrap();
    let conn = open_db(&db).unwrap();
    assert_eq!(
        devin_counts(&conn, "devin-extra"),
        (0, 0, 0, 0),
        "hidden session's catalog row and evidence must be retired"
    );
    assert_eq!(
        devin_counts(&conn, "devin-test").0,
        1,
        "the still-visible session is untouched"
    );

    // Unhiding with the same timestamps re-indexes from scratch — the state
    // entry went away with the evidence, so the stamp cannot mask it.
    let provider = Connection::open(&store).unwrap();
    provider
        .execute_batch("UPDATE sessions SET hidden = 0 WHERE id = 'devin-extra'")
        .unwrap();
    drop(provider);
    sync_scoped_at(&db, SessionScope::Local).unwrap();
    let conn = open_db(&db).unwrap();
    let (sessions, events, prompts, markers) = devin_counts(&conn, "devin-extra");
    assert_eq!(sessions, 1, "unhidden session must be re-indexed");
    assert_eq!(events, 2);
    assert_eq!(prompts, 1);
    assert!(markers >= 1, "title marker is restored");

    // Deleting the provider row retires the session the same way — the CLI's
    // own removal takes the child rows with it.
    let provider = Connection::open(&store).unwrap();
    provider
        .execute_batch(
            "DELETE FROM message_nodes WHERE session_id = 'devin-extra'; \
             DELETE FROM tool_call_state WHERE session_id = 'devin-extra'; \
             DELETE FROM sessions WHERE id = 'devin-extra';",
        )
        .unwrap();
    drop(provider);
    sync_scoped_at(&db, SessionScope::Local).unwrap();
    let conn = open_db(&db).unwrap();
    assert_eq!(devin_counts(&conn, "devin-extra"), (0, 0, 0, 0));
}

#[test]
fn devin_in_place_rewrite_without_timestamp_change_still_syncs() {
    let _lock = ENV_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let store = stage_devin_db(home, BASE_SESSION_SQL);
    let _env = EnvGuard::set(home);
    let db = home.join("history.db");

    sync_scoped_at(&db, SessionScope::Local).unwrap();

    // Rewrite a chat_message in place: identical length, no new row, no
    // last_activity bump. A count/max/length stamp would never notice; the
    // content checksum must.
    let provider = Connection::open(&store).unwrap();
    provider
        .execute_batch(
            "UPDATE message_nodes SET chat_message = \
             '{\"message_id\":\"u0\",\"role\":\"user\",\"content\":\"fixed prompt\",\"metadata\":{\"is_user_input\":true},\"tool_calls\":null,\"thinking\":null,\"tool_call_id\":null,\"phase\":null}' \
             WHERE session_id = 'devin-test' AND node_id = 0",
        )
        .unwrap();
    drop(provider);

    sync_scoped_at(&db, SessionScope::Local).unwrap();
    let conn = open_db(&db).unwrap();
    let prompt: String = conn
        .query_row(
            "SELECT prompt FROM history WHERE source='devin' AND session_id='devin-test'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(prompt, "fixed prompt", "in-place rewrite must re-index");
}

#[test]
fn devin_partial_evidence_loss_repairs_on_unchanged_stamp() {
    let _lock = ENV_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    stage_devin_db(home, BASE_SESSION_SQL);
    let _env = EnvGuard::set(home);
    let db = home.join("history.db");

    sync_scoped_at(&db, SessionScope::Local).unwrap();
    let conn = open_db(&db).unwrap();
    // Drop one canonical row out from under a matching stamp.
    conn.execute(
        "DELETE FROM session_events WHERE source='devin' AND session_id='devin-test' \
         AND role='assistant'",
        [],
    )
    .unwrap();
    drop(conn);

    sync_scoped_at(&db, SessionScope::Local).unwrap();
    let conn = open_db(&db).unwrap();
    assert_eq!(
        devin_counts(&conn, "devin-test").1,
        2,
        "partially lost evidence must be restored even though the stamp matches"
    );
}

#[test]
fn devin_history_loss_repairs_on_unchanged_stamp() {
    let _lock = ENV_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    stage_devin_db(home, BASE_SESSION_SQL);
    let _env = EnvGuard::set(home);
    let db = home.join("history.db");

    sync_scoped_at(&db, SessionScope::Local).unwrap();
    let conn = open_db(&db).unwrap();
    assert_eq!(
        count(
            &conn,
            "SELECT COUNT(*) FROM history WHERE source='devin' AND session_id='devin-test'"
        ),
        1,
        "fixture must produce one history row"
    );
    conn.execute(
        "DELETE FROM history WHERE source='devin' AND session_id='devin-test'",
        [],
    )
    .unwrap();
    drop(conn);

    sync_scoped_at(&db, SessionScope::Local).unwrap();
    let conn = open_db(&db).unwrap();
    assert_eq!(
        count(
            &conn,
            "SELECT COUNT(*) FROM history WHERE source='devin' AND session_id='devin-test'"
        ),
        1,
        "a deleted history row must be restored even though the source stamp is unchanged"
    );
}

#[test]
fn devin_marker_loss_repairs_on_unchanged_stamp() {
    let _lock = ENV_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    stage_devin_db(home, BASE_SESSION_SQL);
    let _env = EnvGuard::set(home);
    let db = home.join("history.db");

    sync_scoped_at(&db, SessionScope::Local).unwrap();
    let conn = open_db(&db).unwrap();
    assert_eq!(
        session_markers(&conn, "devin", "devin-test")
            .unwrap()
            .iter()
            .filter(|m| m.kind == "session_title")
            .count(),
        1,
        "fixture must produce one session_title marker"
    );
    conn.execute(
        "DELETE FROM session_markers WHERE source='devin' AND session_id='devin-test'",
        [],
    )
    .unwrap();
    drop(conn);

    sync_scoped_at(&db, SessionScope::Local).unwrap();
    let conn = open_db(&db).unwrap();
    assert_eq!(
        session_markers(&conn, "devin", "devin-test")
            .unwrap()
            .iter()
            .filter(|m| m.kind == "session_title")
            .count(),
        1,
        "a deleted marker row must be restored even though the source stamp is unchanged"
    );
}

#[test]
fn devin_shared_prompt_is_reassigned_not_dropped() {
    let _lock = ENV_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    // Two sessions containing the identical prompt at the identical
    // timestamp share one `history` row under UNIQUE(source, timestamp_ms,
    // prompt).
    stage_devin_db(
        home,
        r#"
INSERT INTO sessions
  (id, working_directory, backend_type, model, agent_mode, created_at,
   last_activity_at, title, workspace_dirs, hidden, metadata)
VALUES
  ('devin-a', '/work/a', 'devin', 'test-model', 'normal',
   1776643200, 1776643202, NULL, '[]', 0, NULL),
  ('devin-b', '/work/b', 'devin', 'test-model', 'normal',
   1776643200, 1776643202, NULL, '[]', 0, NULL);
INSERT INTO message_nodes
  (session_id, node_id, parent_node_id, chat_message, created_at, metadata)
VALUES
  ('devin-a', 0, NULL,
   '{"message_id":"ua","role":"user","content":"shared prompt","metadata":{"is_user_input":true},"tool_calls":null,"thinking":null,"tool_call_id":null,"phase":null}',
   1776643201, NULL),
  ('devin-b', 0, NULL,
   '{"message_id":"ub","role":"user","content":"shared prompt","metadata":{"is_user_input":true},"tool_calls":null,"thinking":null,"tool_call_id":null,"phase":null}',
   1776643201, NULL);
"#,
    );
    let _env = EnvGuard::set(home);
    let db = home.join("history.db");

    sync_scoped_at(&db, SessionScope::Local).unwrap();
    let conn = open_db(&db).unwrap();
    let owner: String = conn
        .query_row(
            "SELECT session_id FROM history WHERE source='devin' AND prompt='shared prompt'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let loser = if owner == "devin-a" {
        "devin-b"
    } else {
        "devin-a"
    };

    // The owner session goes away; the surviving session's identical user
    // event must keep the shared row indexed — now under its own id.
    let store = devin_cli_dir(home).join("sessions.db");
    let provider = Connection::open(&store).unwrap();
    provider
        .execute_batch(&format!(
            "UPDATE sessions SET hidden = 1 WHERE id = '{owner}'"
        ))
        .unwrap();
    drop(provider);
    sync_scoped_at(&db, SessionScope::Local).unwrap();
    let conn = open_db(&db).unwrap();
    let remaining: Option<String> = conn
        .query_row(
            "SELECT session_id FROM history WHERE source='devin' AND prompt='shared prompt'",
            [],
            |row| row.get(0),
        )
        .ok();
    assert_eq!(
        remaining.as_deref(),
        Some(loser),
        "the shared prompt must be reassigned to the session that still has it"
    );
}

#[test]
fn devin_discovery_treats_an_incomplete_store_as_absent() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    // sessions.db exists but was never fully initialized — no sessions or
    // message tables. Discovery must report "nothing to read", matching the
    // sync path, instead of failing the provider.
    let cli_dir = devin_cli_dir(home);
    fs::create_dir_all(&cli_dir).unwrap();
    let conn = Connection::open(cli_dir.join("sessions.db")).unwrap();
    conn.execute_batch("CREATE TABLE bootstrap_marker(step TEXT);")
        .unwrap();
    drop(conn);

    let db = home.join("history.db");
    let conn = open_db(&db).unwrap();
    let env = DiscoveryEnv::with_roots(&conn, home.to_path_buf(), home.join("missing-opencode.db"));
    let mut found = Vec::new();
    let summary = discover_sessions_with_env(
        &env,
        &DiscoverOptions {
            scope: SessionScope::Local,
            sources: vec!["devin".to_string()],
            limit: None,
        },
        |row| found.push((row.source.clone(), row.session_id.clone())),
    )
    .unwrap();
    assert!(found.is_empty());
    assert_eq!(
        summary.providers.get("devin").map(|p| p.failed),
        Some(false),
        "an incomplete sessions.db is not a store, not a failure"
    );
}

#[test]
fn devin_discovery_reports_shallow_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    stage_devin_db(home, BASE_SESSION_SQL);
    let db = home.join("history.db");
    let conn = open_db(&db).unwrap();
    // `with_roots` resolves the devin dir from the staged home without
    // reading the process environment.
    let env = DiscoveryEnv::with_roots(&conn, home.to_path_buf(), home.join("missing-opencode.db"));
    let mut found = Vec::new();
    discover_sessions_with_env(
        &env,
        &DiscoverOptions {
            scope: SessionScope::Local,
            sources: vec!["devin".to_string()],
            limit: None,
        },
        |row| found.push((row.source.clone(), row.session_id.clone())),
    )
    .unwrap();
    assert_eq!(found, vec![("devin".to_string(), "devin-test".to_string())]);
    let (cwd, first_prompt, models): (Option<String>, Option<String>, String) = conn
        .query_row(
            "SELECT cwd, first_prompt, models_json FROM sessions \
             WHERE source='devin' AND session_id='devin-test'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(cwd.as_deref(), Some("/work/repo"));
    assert_eq!(first_prompt.as_deref(), Some("first prompt"));
    assert!(models.contains("test-model"));
}

#[test]
fn devin_targeted_hydration_and_export() {
    let _lock = ENV_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    stage_devin_db(home, BASE_SESSION_SQL);
    let _env = EnvGuard::set(home);
    let db = home.join("history.db");

    sync_scoped_at(&db, SessionScope::Local).unwrap();

    let result = hydrate_session_at(
        &db,
        &HydrateSessionOptions {
            source: "devin".into(),
            session_id: "devin-test".into(),
            scope: SessionScope::Local,
            include_related: false,
        },
    )
    .unwrap();
    assert!(matches!(result.status.as_str(), "hydrated" | "unchanged"));

    let conn = open_db(&db).unwrap();
    // Source-filtered reads and the export surface both see devin rows.
    let events = session_events(&conn, "devin-test", Some("devin")).unwrap();
    assert_eq!(events.len(), 2);
    let exported = export_json(&conn).unwrap();
    assert!(exported
        .iter()
        .any(|e| e.source == "devin" && e.session_id.as_deref() == Some("devin-test")));
    let tools = session_tool_calls(&conn, "devin-test", Some("devin")).unwrap();
    assert!(tools.is_empty() || tools.iter().all(|t| t.source == "devin"));
    let edits = session_file_edits(&conn, "devin-test", Some("devin")).unwrap();
    assert!(edits.is_empty() || edits.iter().all(|e| e.source == "devin"));
}

#[test]
fn devin_discovery_stamp_tracks_in_place_content_rewrites() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let store = stage_devin_db(home, BASE_SESSION_SQL);
    let db = home.join("history.db");
    let conn = open_db(&db).unwrap();
    let first_prompt = |conn: &Connection| -> String {
        conn.query_row(
            "SELECT first_prompt FROM sessions WHERE source='devin' AND session_id='devin-test'",
            [],
            |row| row.get::<_, Option<String>>(0),
        )
        .unwrap()
        .unwrap_or_default()
    };
    let discover = |conn: &Connection| {
        let env =
            DiscoveryEnv::with_roots(conn, home.to_path_buf(), home.join("missing-opencode.db"));
        discover_sessions_with_env(
            &env,
            &DiscoverOptions {
                scope: SessionScope::Local,
                sources: vec!["devin".to_string()],
                limit: None,
            },
            |_| {},
        )
        .unwrap();
    };

    discover(&conn);
    assert_eq!(first_prompt(&conn), "first prompt");

    // Rewrite the prompt in place: no new row, no created/last_activity bump.
    // A timestamp-only stamp would keep serving the cached shallow row.
    let provider = Connection::open(&store).unwrap();
    provider
        .execute_batch(
            "UPDATE message_nodes SET chat_message = \
             '{\"message_id\":\"u0\",\"role\":\"user\",\"content\":\"rewritten prompt\",\"metadata\":{\"is_user_input\":true},\"tool_calls\":null,\"thinking\":null,\"tool_call_id\":null,\"phase\":null}' \
             WHERE session_id = 'devin-test' AND node_id = 0",
        )
        .unwrap();
    drop(provider);

    discover(&conn);
    assert_eq!(
        first_prompt(&conn),
        "rewritten prompt",
        "an in-place rewrite must refresh the cached shallow row"
    );
}

#[test]
fn devin_session_start_is_the_earliest_of_created_and_node_times() {
    let _lock = ENV_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    // BASE_SESSION_SQL records created_at = 1776643200, one second before the
    // first node at 1776643201 — the session's start is the earlier value.
    stage_devin_db(home, BASE_SESSION_SQL);
    let _env = EnvGuard::set(home);
    let db = home.join("history.db");
    sync_scoped_at(&db, SessionScope::Local).unwrap();
    let conn = open_db(&db).unwrap();
    let first_ts: i64 = conn
        .query_row(
            "SELECT first_activity_ms FROM sessions WHERE source='devin' AND session_id='devin-test'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(first_ts, 1_776_643_200_000);
}

#[test]
fn devin_hydration_reports_the_provider_records_it_parsed() {
    let _lock = ENV_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    stage_devin_db(home, BASE_SESSION_SQL);
    let _env = EnvGuard::set(home);
    let db = home.join("history.db");
    sync_scoped_at(&db, SessionScope::Local).unwrap();
    hydrate_session_at(
        &db,
        &HydrateSessionOptions {
            source: "devin".into(),
            session_id: "devin-test".into(),
            scope: SessionScope::Local,
            include_related: false,
        },
    )
    .unwrap();
    let conn = open_db(&db).unwrap();
    // The pass reads the session row plus two message_nodes; the provider has
    // no tool_call_state rows for it.
    let parsed: i64 = conn
        .query_row(
            "SELECT records_parsed FROM session_hydration_checkpoints \
             WHERE source='devin' AND session_id='devin-test' AND location='local'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        parsed, 3,
        "records_parsed must count what was read, not zero"
    );
}

#[test]
fn devin_transcript_meta_reads_the_envelope_without_materializing_steps() {
    let _lock = ENV_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let cli_dir = stage_devin_db(home, BASE_SESSION_SQL)
        .parent()
        .unwrap()
        .to_path_buf();
    let _env = EnvGuard::set(home);
    // A transcript whose `steps` dwarf the envelope: only `agent`,
    // `schema_version` and `final_metrics` may be materialized.
    let steps = format!("[{}]", vec!["{\"big\":1}"; 20_000].join(","));
    let transcripts = cli_dir.join("transcripts");
    fs::create_dir_all(&transcripts).unwrap();
    fs::write(
        transcripts.join("devin-test.json"),
        format!(
            "{{\"agent\":{{\"name\":\"devin\",\"version\":\"9.9.9\",\"model_name\":\"env-model\"}},\
             \"schema_version\":2,\"final_metrics\":{{\"total_cost\":1.5}},\"steps\":{steps}}}"
        ),
    )
    .unwrap();
    let db = home.join("history.db");
    sync_scoped_at(&db, SessionScope::Local).unwrap();
    let conn = open_db(&db).unwrap();
    let version: Option<String> = conn
        .query_row(
            "SELECT agent_version FROM session_events \
             WHERE source='devin' AND session_id='devin-test' AND agent_version IS NOT NULL LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version.as_deref(), Some("9.9.9"));

    // A malformed transcript falls back to the database's own metadata.
    fs::write(transcripts.join("devin-test.json"), "{not json").unwrap();
    let db2 = home.join("history2.db");
    sync_scoped_at(&db2, SessionScope::Local).unwrap();
    let conn = open_db(&db2).unwrap();
    assert_eq!(devin_counts(&conn, "devin-test").0, 1);
}

#[test]
fn devin_retirement_preserves_the_remote_view_of_a_session() {
    let _lock = ENV_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let store = stage_devin_db(home, BASE_SESSION_SQL);
    let _env = EnvGuard::set(home);
    let db = home.join("history.db");
    sync_scoped_at(&db, SessionScope::Local).unwrap();

    // A remote connector observed the same provider id: the catalog row is
    // shared, and remote presence/observation/checkpoint rows ride on it.
    let conn = open_db(&db).unwrap();
    conn.execute_batch(
        "INSERT INTO session_presences(source,session_id,location) \
         VALUES('devin','devin-test','remote'); \
         INSERT INTO session_observations(\
           source,session_id,location,connector_id,connector_instance,\
           raw_locator,source_stamp,discovery_state,access_state,updated_ms,\
           first_prompt,last_assistant_text) \
         VALUES('devin','devin-test','remote','devin-cloud','default',\
           'remote/devin-test','stamp-1','shallow','available',1,\
           'remote prompt','remote tail'); \
         INSERT INTO session_hydration_checkpoints(\
           source,session_id,location,parser_version,source_bytes,records_parsed,updated_ms) \
         VALUES('devin','devin-test','remote',1,10,2,1);",
    )
    .unwrap();
    drop(conn);

    // The local provider hides the session: the local footprint retires, the
    // remote view must survive.
    let provider = Connection::open(&store).unwrap();
    provider
        .execute_batch("UPDATE sessions SET hidden = 1 WHERE id = 'devin-test'")
        .unwrap();
    drop(provider);
    sync_scoped_at(&db, SessionScope::Local).unwrap();

    let conn = open_db(&db).unwrap();
    assert_eq!(
        devin_counts(&conn, "devin-test"),
        (1, 0, 0, 0),
        "the catalog row survives remote-only; the hidden local transcript's \
         evidence must not"
    );
    let remote_presence: i64 = count(
        &conn,
        "SELECT COUNT(*) FROM session_presences \
         WHERE source='devin' AND session_id='devin-test' AND location='remote'",
    );
    assert_eq!(remote_presence, 1);
    let remote_observation: i64 = count(
        &conn,
        "SELECT COUNT(*) FROM session_observations \
         WHERE source='devin' AND session_id='devin-test' AND location='remote'",
    );
    assert_eq!(remote_observation, 1);
    let remote_checkpoint: i64 = count(
        &conn,
        "SELECT COUNT(*) FROM session_hydration_checkpoints \
         WHERE source='devin' AND session_id='devin-test' AND location='remote'",
    );
    assert_eq!(remote_checkpoint, 1);
    let local_rows: i64 = count(
        &conn,
        "SELECT (SELECT COUNT(*) FROM session_presences \
                WHERE source='devin' AND session_id='devin-test' AND location='local') \
              + (SELECT COUNT(*) FROM session_observations \
                WHERE source='devin' AND session_id='devin-test' AND location='local') \
              + (SELECT COUNT(*) FROM session_hydration_checkpoints \
                WHERE source='devin' AND session_id='devin-test' AND location='local')",
    );
    assert_eq!(
        local_rows, 0,
        "local presence, observation and checkpoint retire"
    );

    // The surviving catalog row must stop quoting the retired local copy:
    // preview text is erased and the locator, stamp and discovery state are
    // rebuilt from the surviving remote observation.
    let field = |column: &str| -> Option<String> {
        conn.query_row(
            &format!(
                "SELECT {column} FROM sessions \
                 WHERE source='devin' AND session_id='devin-test'"
            ),
            [],
            |row| row.get(0),
        )
        .unwrap()
    };
    assert_eq!(
        field("first_prompt").as_deref(),
        Some("remote prompt"),
        "the remote observation's preview survives local retirement"
    );
    assert_eq!(
        field("last_assistant_text").as_deref(),
        Some("remote tail"),
        "the remote observation's preview survives local retirement"
    );
    assert_eq!(field("raw_path").as_deref(), Some("remote/devin-test"));
    assert_eq!(field("source_stamp").as_deref(), Some("stamp-1"));
    assert_eq!(field("discovery_state").as_deref(), Some("shallow"));

    // Scope reads agree: the session is visible remotely and absent locally.
    let remote_ids = ai_hist::discover::list_session_catalog(
        &conn,
        &ai_hist::discover::CatalogListOptions {
            scope: SessionScope::Remote,
            sources: vec!["devin".to_string()],
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        remote_ids.iter().any(|s| s.session_id == "devin-test"),
        "remote scope must still see the session"
    );
    let local_ids = ai_hist::discover::list_session_catalog(
        &conn,
        &ai_hist::discover::CatalogListOptions {
            scope: SessionScope::Local,
            sources: vec!["devin".to_string()],
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        !local_ids.iter().any(|s| s.session_id == "devin-test"),
        "local scope must not see the retired session"
    );
}

#[test]
fn devin_retirement_drops_unattributed_previews() {
    let _lock = ENV_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let store = stage_devin_db(home, BASE_SESSION_SQL);
    let _env = EnvGuard::set(home);
    let db = home.join("history.db");
    sync_scoped_at(&db, SessionScope::Local).unwrap();

    // Shallow discovery is what writes preview text to the shared catalog
    // row — run it so the row quotes the local transcript.
    let conn = open_db(&db).unwrap();
    let env = DiscoveryEnv::with_roots(&conn, home.to_path_buf(), home.join("missing-opencode.db"));
    let mut found = Vec::new();
    discover_sessions_with_env(
        &env,
        &DiscoverOptions {
            scope: SessionScope::Local,
            sources: vec!["devin".to_string()],
            limit: None,
        },
        |row| found.push(row.session_id.clone()),
    )
    .unwrap();
    drop(env);
    assert!(found.contains(&"devin-test".to_string()));
    let seeded: Option<String> = conn
        .query_row(
            "SELECT first_prompt FROM sessions \
             WHERE source='devin' AND session_id='devin-test'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(seeded.is_some(), "local discovery must seed a first_prompt");

    // A remote observation predating preview provenance: the catalog cannot
    // tell the stored text apart from the hidden local transcript's, so it
    // must go rather than leak.
    conn.execute_batch(
        "INSERT INTO session_presences(source,session_id,location) \
         VALUES('devin','devin-test','remote'); \
         INSERT INTO session_observations(\
           source,session_id,location,connector_id,connector_instance,\
           raw_locator,source_stamp,discovery_state,access_state,updated_ms) \
         VALUES('devin','devin-test','remote','devin-cloud','default',\
           'remote/devin-test','stamp-1','shallow','available',1);",
    )
    .unwrap();
    drop(conn);

    let provider = Connection::open(&store).unwrap();
    provider
        .execute_batch("UPDATE sessions SET hidden = 1 WHERE id = 'devin-test'")
        .unwrap();
    drop(provider);
    sync_scoped_at(&db, SessionScope::Local).unwrap();

    let conn = open_db(&db).unwrap();
    let (first_prompt, last_text): (Option<String>, Option<String>) = conn
        .query_row(
            "SELECT first_prompt, last_assistant_text FROM sessions \
             WHERE source='devin' AND session_id='devin-test'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(first_prompt, None, "unattributed preview text must go");
    assert_eq!(last_text, None, "unattributed preview text must go");
}

#[test]
fn devin_malformed_parent_still_anchors_its_children() {
    let _lock = ENV_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    // Node 0's chat_message does not parse; node 1 is valid and names it as
    // its parent. The child must anchor to an addressable row — the bounded
    // malformed_node marker — not a dangling id.
    stage_devin_db(
        home,
        r#"
INSERT INTO sessions
  (id, working_directory, backend_type, model, agent_mode, created_at,
   last_activity_at, title, workspace_dirs, hidden, metadata)
VALUES
  ('devin-broken', '/work/repo', 'devin', 'test-model', 'normal',
   1776643200, 1776643202, 'Broken', '["/work/repo"]', 0, NULL);
INSERT INTO message_nodes
  (session_id, node_id, parent_node_id, chat_message, created_at, metadata)
VALUES
  ('devin-broken', 0, NULL, '{truncated', 1776643201, '{"origin":"test"}'),
  ('devin-broken', 1, 0,
   '{"message_id":"a1","role":"assistant","content":"answer","thinking":null,"metadata":{},"tool_calls":null,"tool_call_id":null,"phase":null}',
   1776643202, NULL);
"#,
    );
    let _env = EnvGuard::set(home);
    let db = home.join("history.db");
    sync_scoped_at(&db, SessionScope::Local).unwrap();
    let conn = open_db(&db).unwrap();
    let child_parent: Option<String> = conn
        .query_row(
            "SELECT parent_id FROM session_events \
             WHERE source='devin' AND session_id='devin-broken' AND message_id='a1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(child_parent.as_deref(), Some("n0"));
    let marker: Option<String> = conn
        .query_row(
            "SELECT kind FROM session_markers \
             WHERE source='devin' AND session_id='devin-broken' AND message_id='n0'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(marker.as_deref(), Some("malformed_node"));
}

#[test]
fn devin_content_swapped_between_rows_still_resyncs() {
    let _lock = ENV_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let store = stage_devin_db(home, BASE_SESSION_SQL);
    let _env = EnvGuard::set(home);
    let db = home.join("history.db");
    sync_scoped_at(&db, SessionScope::Local).unwrap();

    // Swap chat_message between the two nodes: identical timestamps, row ids,
    // counts and content multiset — only the row↔content pairing changed.
    let provider = Connection::open(&store).unwrap();
    provider
        .execute_batch(
            "UPDATE message_nodes SET chat_message = ( \
               SELECT chat_message FROM message_nodes \
               WHERE session_id='devin-test' AND node_id=1) \
             WHERE session_id='devin-test' AND node_id=0; \
             UPDATE message_nodes SET chat_message = \
               '{\"message_id\":\"u0\",\"role\":\"user\",\"content\":\"first prompt\",\"metadata\":{\"is_user_input\":true},\"tool_calls\":null,\"thinking\":null,\"tool_call_id\":null,\"phase\":null}' \
             WHERE session_id='devin-test' AND node_id=1;",
        )
        .unwrap();
    drop(provider);

    sync_scoped_at(&db, SessionScope::Local).unwrap();
    let conn = open_db(&db).unwrap();
    // Node 1 now holds the user turn: its node-derived event uid is stable,
    // and its role must flip from assistant to user.
    let node1_role: String = conn
        .query_row(
            "SELECT role FROM session_events \
             WHERE source='devin' AND session_id='devin-test' AND event_uid='n1:text'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        node1_role, "user",
        "a content swap must re-normalize the row"
    );
}

#[test]
fn devin_hidden_session_retires_after_sync_state_is_lost() {
    let _lock = ENV_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let store = stage_devin_db(home, BASE_SESSION_SQL);
    let _env = EnvGuard::set(home);
    let db = home.join("history.db");
    sync_scoped_at(&db, SessionScope::Local).unwrap();

    // Hide the session, then destroy the persisted stamp map: the local
    // catalog rows are the durable baseline, so the hidden session must
    // still retire instead of surviving orphaned.
    let provider = Connection::open(&store).unwrap();
    provider
        .execute("UPDATE sessions SET hidden=1 WHERE id='devin-test'", [])
        .unwrap();
    drop(provider);
    std::fs::remove_file(home.join(".sync-state.json")).unwrap();

    sync_scoped_at(&db, SessionScope::Local).unwrap();
    let conn = open_db(&db).unwrap();
    let local_rows: i64 = conn
        .query_row(
            "SELECT \
               (SELECT COUNT(*) FROM sessions \
                WHERE source='devin' AND session_id='devin-test') + \
               (SELECT COUNT(*) FROM session_events \
                WHERE source='devin' AND session_id='devin-test') + \
               (SELECT COUNT(*) FROM session_presences \
                WHERE source='devin' AND session_id='devin-test' AND location='local')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(local_rows, 0, "state loss must not strand a hidden session");
}

#[test]
fn devin_retired_stamp_stays_out_of_the_persisted_state() {
    let _lock = ENV_LOCK.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path();
    let store = stage_devin_db(home, BASE_SESSION_SQL);
    let _env = EnvGuard::set(home);
    let db = home.join("history.db");
    let state_path = home.join(".sync-state.json");
    let stamped = |path: &std::path::Path| {
        serde_json::from_slice::<serde_json::Value>(&std::fs::read(path).unwrap())
            .unwrap()
            .get("devin_sessions_v1")
            .and_then(|map| map.get("devin-test"))
            .is_some()
    };

    sync_scoped_at(&db, SessionScope::Local).unwrap();
    assert!(stamped(&state_path), "first sync must stamp the session");

    let provider = Connection::open(&store).unwrap();
    provider
        .execute("UPDATE sessions SET hidden=1 WHERE id='devin-test'", [])
        .unwrap();
    drop(provider);

    sync_scoped_at(&db, SessionScope::Local).unwrap();
    assert!(
        !stamped(&state_path),
        "the checkpoint merge must not resurrect a retired stamp"
    );
    sync_scoped_at(&db, SessionScope::Local).unwrap();
    assert!(
        !stamped(&state_path),
        "a retired stamp stays retired on the next sync"
    );
}
