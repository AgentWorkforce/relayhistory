//! Claude's per-message `origin.kind` survives capture and reaches the change
//! feed an uploader exports, so a downstream reader can tell a message a
//! person typed from agent-to-agent traffic (a peer session's message, a
//! background task's notification) without the transcript.
//!
//! Public API only, plus a read-only look at the stored rows.

use ai_hist::{Change, ChangeKind, ChangeQuery, SessionStore, StoreOptions, Watermark};
use rusqlite::{Connection, OpenFlags};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

/// The environment is process-wide, so the tests in this binary take turns.
static ENV_LOCK: Mutex<()> = Mutex::new(());

/// An isolated provider home, every provider root pinned under it.
struct Home {
    dir: tempfile::TempDir,
    _env: MutexGuard<'static, ()>,
}

impl Home {
    fn new() -> Self {
        let env = ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        std::env::set_var("HOME", home);
        std::env::set_var("USERPROFILE", home);
        std::env::set_var("CLAUDE_CONFIG_DIR", home.join(".claude"));
        std::env::set_var("CODEX_HOME", home.join(".codex"));
        std::env::remove_var("CODEX_SQLITE_HOME");
        std::env::set_var("GROK_HOME", home.join(".grok"));
        std::env::set_var(
            "OPENCODE_DB",
            home.join(".local/share/opencode/opencode.db"),
        );
        std::env::set_var(
            "OPENCODE_STORAGE_DIR",
            home.join(".local/share/opencode/storage"),
        );
        std::env::set_var("XDG_DATA_HOME", home.join(".local/share"));
        std::env::set_var("XDG_CONFIG_HOME", home.join(".config"));
        std::env::remove_var("AI_HIST_DB");
        Self { dir, _env: env }
    }

    fn path(&self) -> &Path {
        self.dir.path()
    }

    fn db(&self) -> PathBuf {
        self.path().join("ai-history.db")
    }

    fn store(&self) -> SessionStore {
        let mut options = StoreOptions::default();
        options.db_path = Some(self.db());
        options.home = Some(self.path().to_path_buf());
        SessionStore::open(options).unwrap()
    }

    fn transcript(&self, name: &str, body: &str) {
        let project = self.path().join(".claude/projects/origin");
        fs::create_dir_all(&project).unwrap();
        fs::write(project.join(name), body).unwrap();
    }
}

const SESSION: &str = "origin-session";

fn user(uuid: &str, parent: Option<&str>, origin: Option<&str>, text: &str, ts: &str) -> String {
    let parent = parent.map_or("null".to_string(), |p| format!("\"{p}\""));
    let origin = origin.map_or(String::new(), |kind| {
        format!(",\"origin\":{{\"kind\":\"{kind}\"}}")
    });
    format!(
        "{{\"parentUuid\":{parent},\"isSidechain\":false,\"type\":\"user\"{origin},\
         \"message\":{{\"role\":\"user\",\"content\":\"{text}\"}},\"uuid\":\"{uuid}\",\
         \"timestamp\":\"{ts}\",\"cwd\":\"/tmp/project\",\"sessionId\":\"{SESSION}\",\
         \"version\":\"2.1.96\"}}\n"
    )
}

fn assistant(uuid: &str, parent: &str, text: &str, ts: &str) -> String {
    format!(
        "{{\"parentUuid\":\"{parent}\",\"isSidechain\":false,\"message\":{{\"model\":\
         \"claude-opus-4-7\",\"id\":\"msg_{uuid}\",\"role\":\"assistant\",\"content\":\
         [{{\"type\":\"text\",\"text\":\"{text}\"}}],\"stop_reason\":\"end_turn\",\
         \"usage\":{{\"input_tokens\":3,\"output_tokens\":4}}}},\"requestId\":\"req_{uuid}\",\
         \"type\":\"assistant\",\"uuid\":\"{uuid}\",\"timestamp\":\"{ts}\",\
         \"cwd\":\"/tmp/project\",\"sessionId\":\"{SESSION}\",\"version\":\"2.1.96\"}}\n"
    )
}

fn stage(home: &Home) {
    let body = [
        user(
            "u1",
            None,
            Some("human"),
            "ship it",
            "2026-11-15T00:00:00.000Z",
        ),
        assistant("a1", "u1", "shipping", "2026-11-15T00:00:01.000Z"),
        user(
            "u2",
            Some("a1"),
            Some("peer"),
            "yes from the reviewer agent",
            "2026-11-15T00:00:02.000Z",
        ),
        user(
            "u3",
            Some("u2"),
            Some("task-notification"),
            "<task-notification>build finished</task-notification>",
            "2026-11-15T00:00:03.000Z",
        ),
        user("u4", Some("u3"), None, "yes", "2026-11-15T00:00:04.000Z"),
    ]
    .concat();
    home.transcript(&format!("{SESSION}.jsonl"), &body);
}

/// `(message_id -> (role, origin_kind, control_kind))` as stored.
fn stored(home: &Home) -> BTreeMap<String, (String, Option<String>, Option<String>)> {
    let conn = Connection::open_with_flags(home.db(), OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT message_id, role, origin_kind, control_kind FROM session_events \
             WHERE source = 'claude' AND session_id = ?1",
        )
        .unwrap();
    stmt.query_map([SESSION], |row| {
        Ok((row.get(0)?, (row.get(1)?, row.get(2)?, row.get(3)?)))
    })
    .unwrap()
    .collect::<rusqlite::Result<_>>()
    .unwrap()
}

#[test]
fn a_claude_messages_origin_is_stored_on_its_row() {
    let home = Home::new();
    stage(&home);
    let store = home.store();
    store.sync(Default::default()).unwrap();

    let rows = stored(&home);
    let origin = |id: &str| rows.get(id).map(|(_, origin, _)| origin.clone());
    assert_eq!(origin("u1"), Some(Some("human".to_string())), "{rows:?}");
    assert_eq!(origin("u2"), Some(Some("peer".to_string())));
    assert_eq!(origin("u3"), Some(Some("task-notification".to_string())));
    // The notification is still classified as a control row, as before.
    assert_eq!(
        rows.get("u3").and_then(|(_, _, control)| control.clone()),
        Some("task_notification".to_string())
    );
    // A record that names no origin stores none: unknown, not human.
    assert_eq!(origin("u4"), Some(None));
    // Model output names no origin.
    assert_eq!(origin("a1"), Some(None));
}

#[test]
fn the_change_feed_exports_the_origin_with_the_row() {
    let home = Home::new();
    stage(&home);
    let store = home.store();
    store.sync(Default::default()).unwrap();

    let changes: Vec<Change> = store
        .changes_since(
            Watermark::START,
            ChangeQuery::default().kinds([ChangeKind::SessionEvent]),
        )
        .unwrap()
        .map(|change| change.unwrap())
        .collect();
    let exported: BTreeMap<String, Value> = changes
        .iter()
        .filter_map(|change| {
            let columns = change.columns.as_ref()?;
            let id = columns.get("message_id")?.as_str()?.to_string();
            Some((
                id,
                columns.get("origin_kind").cloned().unwrap_or(Value::Null),
            ))
        })
        .collect();
    assert_eq!(
        exported.get("u1"),
        Some(&Value::from("human")),
        "{exported:?}"
    );
    assert_eq!(exported.get("u2"), Some(&Value::from("peer")));
    assert_eq!(exported.get("u3"), Some(&Value::from("task-notification")));
    assert_eq!(exported.get("u4"), Some(&Value::Null));
    assert_eq!(exported.get("a1"), Some(&Value::Null));
}

/// A re-sync of an unchanged transcript writes nothing new: the origin is
/// part of the row the first sync wrote, so no row is re-exported for it.
#[test]
fn an_unchanged_resync_exports_no_row_again() {
    let home = Home::new();
    stage(&home);
    let store = home.store();
    store.sync(Default::default()).unwrap();
    let head = store.head_revision().unwrap();
    store.sync(Default::default()).unwrap();
    let after: Vec<Change> = store
        .changes_since(
            head,
            ChangeQuery::default().kinds([ChangeKind::SessionEvent]),
        )
        .unwrap()
        .map(|change| change.unwrap())
        .collect();
    assert!(after.is_empty(), "{after:?}");
}
