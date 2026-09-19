//! OpenCode event-level parity across both of the provider's storage layouts.
//!
//! The corpus under `tests/fixtures/opencode/legacy-json-*` is a verbatim copy
//! of burn's reference fixtures (`tests/fixtures/opencode` there), which is the
//! behaviour this store has to match; `sqlite-store.sql` builds the same shape
//! in OpenCode's current SQLite schema.
//!
//! The load-bearing assertion is `the_two_layouts_normalize_to_identical_evidence`:
//! the same session content, stored both ways, must produce the same rows apart
//! from the provenance path. That is what makes "two loaders, one parser" a fact
//! rather than an intention.

use ai_hist::internal::{session_markers, session_tree, SessionTreeOptions};
use ai_hist::{
    discover_sessions_scoped_at, hydrate_session_at, open_db, session_events,
    session_relationships, sync_local_at, DiscoverOptions, HydrateSessionOptions, SessionScope,
};
use rusqlite::Connection;
use std::fs;
use std::path::{Path, PathBuf};

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/opencode")
}

/// Build the SQLite fixture deterministically from the checked-in SQL. A `.db`
/// file in git would be an opaque binary nobody can review; the statements are
/// reviewable and produce the same store every run.
fn build_sqlite_store(target: &Path) {
    let sql = fs::read_to_string(fixtures().join("sqlite-store.sql")).expect("sqlite fixture");
    fs::create_dir_all(target.parent().unwrap()).unwrap();
    let db = Connection::open(target).expect("open fixture store");
    db.execute_batch(&sql).expect("apply fixture store");
}

/// Re-express an `opencode.db` as the legacy JSON tree, reusing each row's own
/// payload verbatim. Deriving one layout from the other is the point: if the
/// two loaders then disagree, the disagreement is in the parser and not in two
/// hand-written fixtures drifting apart.
fn sqlite_store_as_json_tree(db_path: &Path, storage_root: &Path) {
    let db = Connection::open(db_path).expect("open fixture store");
    let sessions = db
        .prepare("SELECT id, parent_id, directory, time_created, time_updated FROM session")
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, Option<i64>>(4)?,
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    for (id, parent, directory, created, updated) in sessions {
        let mut payload = serde_json::json!({
            "id": id,
            "directory": directory,
            "time": { "created": created, "updated": updated },
        });
        if let Some(parent) = parent {
            payload["parentID"] = serde_json::Value::String(parent);
        }
        write_json(
            &storage_root
                .join("session/global")
                .join(format!("{id}.json")),
            &payload.to_string(),
        );
    }
    for (table, key) in [("message", "session_id"), ("part", "message_id")] {
        let rows = db
            .prepare(&format!("SELECT id, {key}, data FROM {table}"))
            .unwrap()
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        for (id, parent, data) in rows {
            write_json(
                &storage_root
                    .join(table)
                    .join(parent)
                    .join(format!("{id}.json")),
                &data,
            );
        }
    }
}

fn write_json(path: &Path, contents: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, contents).unwrap();
}

fn copy_tree(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for entry in fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), target).unwrap();
        }
    }
}

/// Every row this issue is about, in a form two runs can be compared by. The
/// provenance path is deliberately absent: it is the one field the two layouts
/// are allowed to disagree about.
#[derive(Debug, PartialEq, Eq)]
struct Evidence {
    events: Vec<String>,
    tool_calls: Vec<String>,
    file_edits: Vec<String>,
    markers: Vec<String>,
    relationships: Vec<String>,
}

fn evidence(db_path: &Path, session_ids: &[&str]) -> Evidence {
    let conn = open_db(db_path).unwrap();
    let mut out = Evidence {
        events: Vec::new(),
        tool_calls: Vec::new(),
        file_edits: Vec::new(),
        markers: Vec::new(),
        relationships: Vec::new(),
    };
    for session_id in session_ids {
        for event in session_events(&conn, session_id, Some("opencode")).unwrap() {
            out.events.push(format!(
                "{}|{}|{}|{}|{}|{}|{:?}|{:?}|{:?}|{:?}|{:?}",
                event.session_id,
                event.event_uid,
                event.role,
                event.kind,
                event.ts_ms,
                event.message_id.unwrap_or_default(),
                event.text,
                event.model,
                event.token_json,
                event.provider,
                event.stop_reason,
            ));
        }
        let mut calls = conn
            .prepare(
                "SELECT session_id, tool_use_id, name, target, args_json, is_error, ts_ms \
                 FROM tool_calls WHERE source='opencode' AND session_id=? ORDER BY tool_use_id",
            )
            .unwrap()
            .query_map([session_id], |row| {
                Ok(format!(
                    "{}|{}|{}|{:?}|{}|{:?}|{:?}",
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                    row.get::<_, Option<i64>>(6)?,
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        out.tool_calls.append(&mut calls);
        let mut edits = conn
            .prepare(
                "SELECT session_id, tool_use_id, file_path, tool_name, ts_ms \
                 FROM file_edits WHERE source='opencode' AND session_id=? ORDER BY tool_use_id",
            )
            .unwrap()
            .query_map([session_id], |row| {
                Ok(format!(
                    "{}|{}|{}|{}|{:?}",
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        out.file_edits.append(&mut edits);
        for marker in session_markers(&conn, session_id, Some("opencode")).unwrap() {
            out.markers.push(format!(
                "{}|{}|{}|{:?}|{:?}|{:?}",
                marker.session_id,
                marker.kind,
                marker.marker_uid,
                marker.message_id,
                marker.ts_ms,
                marker.detail_json,
            ));
        }
        // `evidence_locator` is the provenance path, so it is left out for the
        // same reason `raw_path` is.
        let mut links = conn
            .prepare(
                "SELECT parent_session_id, child_session_id, relationship, identity_status, \
                        evidence_kind, evidence_ref, child_model, child_has_events \
                 FROM session_relationships \
                 WHERE source='opencode' AND (parent_session_id=? OR child_session_id=?) \
                 ORDER BY relationship_uid",
            )
            .unwrap()
            .query_map([session_id, session_id], |row| {
                Ok(format!(
                    "{}|{:?}|{}|{}|{}|{:?}|{:?}|{}",
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, bool>(7)?,
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        out.relationships.append(&mut links);
    }
    out
}

fn temp_root(tag: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "ai-hist-opencode-{tag}-{}-{:?}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(&root).unwrap();
    root
}

/// Point the process at one isolated OpenCode store. The provider roots come
/// from the environment, so every phase below re-points them rather than
/// leaking the previous phase's layout into the next.
fn use_layout(home: &Path, db: Option<&Path>, storage: Option<&Path>) {
    std::env::set_var("HOME", home);
    std::env::set_var("USERPROFILE", home);
    std::env::remove_var("AI_HIST_DB");
    std::env::set_var(
        "OPENCODE_DB",
        db.map(Path::to_path_buf)
            .unwrap_or_else(|| home.join("no-such-opencode.db")),
    );
    std::env::set_var(
        "OPENCODE_STORAGE_DIR",
        storage
            .map(Path::to_path_buf)
            .unwrap_or_else(|| home.join("no-such-storage")),
    );
}

/// One test, because it sets process-wide environment variables and Rust runs
/// tests in a binary concurrently. Each phase is a named function so a failure
/// still says which acceptance criterion broke.
#[test]
fn opencode_reaches_event_level_parity_across_both_storage_layouts() {
    the_two_layouts_normalize_to_identical_evidence();
    a_parent_id_links_a_child_session_and_the_tree_returns_it();
    a_tool_turn_records_errors_tokens_provider_and_stop_reason();
    a_compaction_part_records_one_boundary_marker();
    a_large_store_is_synced_without_copying_it();
    an_install_indexed_as_prompts_only_gains_events_on_the_next_plain_sync();
}

/// Acceptance: "Snapshots for the 5 JSON fixtures and the new SQLite fixture
/// are identical modulo `raw_path`/stamps (proves the two loaders normalize the
/// same)."
fn the_two_layouts_normalize_to_identical_evidence() {
    let root = temp_root("parity");
    let sessions = ["ses_sqlite_root", "ses_sqlite_child"];

    let sqlite_home = root.join("sqlite-home");
    let store = sqlite_home.join(".local/share/opencode/opencode.db");
    build_sqlite_store(&store);
    use_layout(&sqlite_home, Some(&store), None);
    let sqlite_db = root.join("sqlite.db");
    sync_local_at(&sqlite_db).unwrap();
    let from_sqlite = evidence(&sqlite_db, &sessions);

    let json_home = root.join("json-home");
    let tree = json_home.join(".local/share/opencode/storage");
    sqlite_store_as_json_tree(&store, &tree);
    use_layout(&json_home, None, Some(&tree));
    let json_db = root.join("json.db");
    sync_local_at(&json_db).unwrap();
    let from_json = evidence(&json_db, &sessions);

    assert!(
        !from_sqlite.events.is_empty(),
        "the SQLite loader produced no events at all; parity would be vacuous"
    );
    assert!(
        !from_sqlite.tool_calls.is_empty(),
        "the SQLite loader produced no tool calls; parity would be vacuous"
    );
    assert_eq!(
        from_sqlite, from_json,
        "the two loaders must normalize the same session to the same evidence"
    );

    // The one field they are allowed to differ on, asserted rather than assumed.
    let sqlite_raw = raw_path(&sqlite_db, "ses_sqlite_root");
    let json_raw = raw_path(&json_db, "ses_sqlite_root");
    assert!(sqlite_raw.ends_with("opencode.db"), "{sqlite_raw}");
    assert!(json_raw.ends_with("ses_sqlite_root.json"), "{json_raw}");
}

fn raw_path(db_path: &Path, session_id: &str) -> String {
    open_db(db_path)
        .unwrap()
        .query_row(
            "SELECT raw_path FROM sessions WHERE source='opencode' AND session_id=?",
            [session_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .unwrap()
        .unwrap_or_default()
}

/// Acceptance: "`multi-turn` fixture: `ses_child` is linked to its parent via
/// `session_relationships`, and `getSessionTree` returns it as a child."
fn a_parent_id_links_a_child_session_and_the_tree_returns_it() {
    let root = temp_root("multi-turn");
    let home = root.join("home");
    let tree = home.join(".local/share/opencode/storage");
    copy_tree(&fixtures().join("legacy-json-multi-turn/storage"), &tree);
    use_layout(&home, None, Some(&tree));
    let db_path = root.join("history.db");
    sync_local_at(&db_path).unwrap();

    let conn = open_db(&db_path).unwrap();
    let child = session_relationships(&conn, "opencode", "ses_child").unwrap();
    assert_eq!(
        child.capabilities.stable_child_identity, "always",
        "OpenCode names the parent outright, so the child identity is never inferred"
    );
    let edge = child
        .as_child
        .first()
        .expect("ses_child must record the delegation that produced it");
    assert_eq!(edge.parent_session_id, "ses_multi");
    assert_eq!(edge.child_session_id.as_deref(), Some("ses_child"));
    assert_eq!(edge.identity_status, "observed");
    assert_eq!(edge.evidence_kind, "opencode_parent_id");

    let tree_view = session_tree(
        &conn,
        "opencode",
        "ses_multi",
        &SessionTreeOptions::default(),
    )
    .unwrap();
    assert!(
        tree_view
            .nodes
            .iter()
            .any(|node| node.session_id == "ses_child" && node.depth == 1),
        "getSessionTree must return ses_child as a child of ses_multi, got {:?}",
        tree_view
            .nodes
            .iter()
            .map(|node| (&node.session_id, node.depth))
            .collect::<Vec<_>>()
    );
}

/// Acceptance: "`with-tool` fixture: `tool_calls` rows with `is_error` set from
/// `metadata.exit != 0`; `token_json` present with `cache.read`/`cache.write`;
/// `provider = "anthropic"`; `stop_reason` present."
///
/// `with-tool` carries no `metadata.exit`, so the exit-code half of that
/// criterion is asserted on `user-turn-blocks`, which does (`call_fail`,
/// `exit: 1`). Both fixtures are checked here so neither half is taken on
/// faith.
fn a_tool_turn_records_errors_tokens_provider_and_stop_reason() {
    let root = temp_root("with-tool");
    let home = root.join("home");
    let tree = home.join(".local/share/opencode/storage");
    copy_tree(&fixtures().join("legacy-json-with-tool/storage"), &tree);
    copy_tree(
        &fixtures().join("legacy-json-user-turn-blocks/storage"),
        &tree,
    );
    use_layout(&home, None, Some(&tree));
    let db_path = root.join("history.db");
    sync_local_at(&db_path).unwrap();
    let conn = open_db(&db_path).unwrap();

    let calls: Vec<(String, String, Option<String>, Option<i64>)> = conn
        .prepare(
            "SELECT tool_use_id, name, target, is_error FROM tool_calls \
             WHERE source='opencode' AND session_id='ses_tool' ORDER BY tool_use_id",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        calls,
        vec![
            (
                "toolu_bash_1".into(),
                "bash".into(),
                Some("ls -la".into()),
                Some(0)
            ),
            (
                "toolu_edit_1".into(),
                "edit".into(),
                Some("/src/b.ts".into()),
                Some(0)
            ),
            (
                "toolu_read_1".into(),
                "read".into(),
                Some("/src/a.ts".into()),
                Some(0)
            ),
        ],
        "every tool part becomes a tool_calls row with its picked target"
    );

    // `edit` writes a file; `read` and `bash` do not.
    let edits: Vec<(String, String)> = conn
        .prepare(
            "SELECT tool_use_id, file_path FROM file_edits \
             WHERE source='opencode' AND session_id='ses_tool' ORDER BY tool_use_id",
        )
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(edits, vec![("toolu_edit_1".into(), "/src/b.ts".into())]);

    // A non-zero process exit is a failure even though the call "completed".
    let failed: Option<i64> = conn
        .query_row(
            "SELECT is_error FROM tool_calls WHERE source='opencode' AND tool_use_id='call_fail'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        failed,
        Some(1),
        "metadata.exit = 1 must mark the call errored"
    );
    let succeeded: Option<i64> = conn
        .query_row(
            "SELECT is_error FROM tool_calls WHERE source='opencode' AND tool_use_id='call_b1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(succeeded, Some(0), "metadata.exit = 0 is not a failure");

    let assistant = session_events(&conn, "ses_tool", Some("opencode"))
        .unwrap()
        .into_iter()
        .find(|event| event.kind == "tool_use")
        .expect("the tool turn must produce a tool_use event");
    assert_eq!(assistant.provider.as_deref(), Some("anthropic"));
    assert_eq!(
        assistant.model.as_deref(),
        Some("anthropic/claude-opus-4-5"),
        "the model is the provider-qualified id, matching burn"
    );
    assert_eq!(
        assistant.stop_reason.as_deref(),
        Some("tool-calls"),
        "stop_reason comes from the message's last step-finish"
    );
    let tokens: serde_json::Value =
        serde_json::from_str(assistant.token_json.as_deref().expect("token_json")).unwrap();
    assert_eq!(tokens["input"], 6);
    assert_eq!(tokens["output"], 100);
    assert_eq!(tokens["cache"]["read"], 0);
    assert_eq!(
        tokens["cache"]["write"], 20000,
        "the tokens object is stored verbatim, cache included"
    );

    // The tool's own output is an event of its own, not folded into the turn.
    let result = session_events(&conn, "ses_tool", Some("opencode"))
        .unwrap()
        .into_iter()
        .find(|event| event.event_uid == "tool_result:toolu_bash_1")
        .expect("a terminal tool part must produce a tool_result event");
    assert_eq!(result.role, "tool_result");
    assert_eq!(result.text.as_deref(), Some("file listing"));
}

/// Acceptance: "`with-compaction`: one `compaction_boundary` marker."
fn a_compaction_part_records_one_boundary_marker() {
    let root = temp_root("with-compaction");
    let home = root.join("home");
    let tree = home.join(".local/share/opencode/storage");
    copy_tree(
        &fixtures().join("legacy-json-with-compaction/storage"),
        &tree,
    );
    use_layout(&home, None, Some(&tree));
    let db_path = root.join("history.db");
    sync_local_at(&db_path).unwrap();

    let conn = open_db(&db_path).unwrap();
    let markers = session_markers(&conn, "ses_compact", Some("opencode")).unwrap();
    assert_eq!(
        markers.len(),
        1,
        "the compaction fixture has exactly one boundary, got {markers:?}"
    );
    assert_eq!(markers[0].kind, "compaction_boundary");
    assert_eq!(markers[0].message_id.as_deref(), Some("msg_compact_uc"));
    assert_eq!(markers[0].ts_ms, Some(1776999003000));
    assert_eq!(markers[0].detail_json.as_deref(), Some("{\"auto\":true}"));

    // The turns either side of the boundary are still real turns.
    let texts: Vec<String> = session_events(&conn, "ses_compact", Some("opencode"))
        .unwrap()
        .into_iter()
        .filter_map(|event| event.text)
        .collect();
    assert!(texts.iter().any(|text| text == "continue working"));
    assert!(texts.iter().any(|text| text.starts_with("## Goal")));

    // Re-reading the same session must not add a second marker.
    sync_local_at(&db_path).unwrap();
    let conn = open_db(&db_path).unwrap();
    assert_eq!(
        session_markers(&conn, "ses_compact", Some("opencode"))
            .unwrap()
            .len(),
        1,
        "marker writes are idempotent"
    );
}

/// Acceptance: "`sync --local` on a 50 MB `opencode.db` fixture completes
/// without copying the DB (assert no temp backup file is created)."
///
/// The store is built in a tempdir at test time and never checked in. The
/// assertion is on the effect — nothing new appears in the temp directory the
/// copy would have used — not on the return value of the sync.
fn a_large_store_is_synced_without_copying_it() {
    let root = temp_root("large");
    let home = root.join("home");
    let store = home.join(".local/share/opencode/opencode.db");
    build_sqlite_store(&store);

    // Pad to >= 50 MB with provider rows the adapter never reads, so the file
    // is genuinely large without changing what the session queries return.
    {
        let db = Connection::open(&store).unwrap();
        db.execute_batch("CREATE TABLE ai_hist_test_ballast (id INTEGER PRIMARY KEY, blob BLOB);")
            .unwrap();
        let chunk = vec![0u8; 1 << 20];
        let mut stmt = db
            .prepare("INSERT INTO ai_hist_test_ballast (blob) VALUES (?)")
            .unwrap();
        for _ in 0..55 {
            stmt.execute([&chunk]).unwrap();
        }
    }
    let size = fs::metadata(&store).unwrap().len();
    assert!(
        size >= 50 * 1024 * 1024,
        "the fixture store must actually be large, got {size} bytes"
    );

    // Watch the directory `tempfile` would put the backup in.
    let temp_watch = root.join("tempdir");
    fs::create_dir_all(&temp_watch).unwrap();
    std::env::set_var("TMPDIR", &temp_watch);
    use_layout(&home, Some(&store), None);

    // Positive control for the negative reading below: prove this directory
    // is the one a temporary file actually lands in. "Nothing appeared here"
    // means nothing only if something could have.
    {
        let probe = tempfile::NamedTempFile::new().unwrap();
        assert!(
            probe.path().starts_with(&temp_watch),
            "the watched directory is not where temporary files go: {:?}",
            probe.path()
        );
    }

    let db_path = root.join("history.db");
    sync_local_at(&db_path).unwrap();
    std::env::remove_var("TMPDIR");

    let leftovers: Vec<PathBuf> = fs::read_dir(&temp_watch)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .collect();
    assert!(
        leftovers.is_empty(),
        "the default sync path must not copy the provider store, found {leftovers:?}"
    );
    // And it still did the work: the evidence is there.
    let conn = open_db(&db_path).unwrap();
    assert!(
        !session_events(&conn, "ses_sqlite_root", Some("opencode"))
            .unwrap()
            .is_empty(),
        "a bounded sync still has to produce the session's events"
    );
}

/// An install indexed by an earlier release has `history` rows for its OpenCode
/// sessions and nothing else. The next plain `sync --local` has to repair that
/// without any migration flag, because the OpenCode sync path keeps no
/// per-session stamp — it re-reads every session each run. Asserted rather than
/// assumed: this is exactly the shape of regression two sibling branches
/// shipped.
fn an_install_indexed_as_prompts_only_gains_events_on_the_next_plain_sync() {
    let root = temp_root("upgrade");
    let home = root.join("home");
    let store = home.join(".local/share/opencode/opencode.db");
    build_sqlite_store(&store);
    use_layout(&home, Some(&store), None);
    let db_path = root.join("history.db");

    // First sync, then reduce the database to what an older release left:
    // prompts, no events, no evidence, and a shallow catalog row.
    sync_local_at(&db_path).unwrap();
    {
        let conn = open_db(&db_path).unwrap();
        conn.execute_batch(
            "DELETE FROM session_events WHERE source='opencode';
             DELETE FROM tool_calls WHERE source='opencode';
             DELETE FROM file_edits WHERE source='opencode';
             DELETE FROM session_markers WHERE source='opencode';
             DELETE FROM session_relationships WHERE source='opencode';
             DELETE FROM session_hydration_checkpoints WHERE source='opencode';
             UPDATE sessions SET discovery_state='shallow' WHERE source='opencode';",
        )
        .unwrap();
        let prompts: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM history WHERE source='opencode'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            prompts > 0,
            "the prompts-only state must still have prompts"
        );
        assert!(
            session_events(&conn, "ses_sqlite_root", Some("opencode"))
                .unwrap()
                .is_empty(),
            "the prompts-only state must start with no events"
        );
    }

    sync_local_at(&db_path).unwrap();

    let conn = open_db(&db_path).unwrap();
    assert!(
        !session_events(&conn, "ses_sqlite_root", Some("opencode"))
            .unwrap()
            .is_empty(),
        "a plain sync must re-read an OpenCode session indexed as prompts-only"
    );
    assert!(
        !session_relationships(&conn, "opencode", "ses_sqlite_child")
            .unwrap()
            .as_child
            .is_empty(),
        "the repair must restore the delegation edge too"
    );
    drop(conn);

    // Targeted hydration repairs the same install: the parser version bump
    // invalidates the checkpoint an earlier release wrote.
    discover_sessions_scoped_at(
        &db_path,
        &DiscoverOptions {
            sources: vec!["opencode".into()],
            ..Default::default()
        },
    )
    .unwrap();
    let hydrated = hydrate_session_at(
        &db_path,
        &HydrateSessionOptions {
            source: "opencode".into(),
            session_id: "ses_sqlite_root".into(),
            scope: SessionScope::Local,
            include_related: false,
        },
    )
    .unwrap();
    assert!(
        hydrated.evidence.events > 0,
        "targeted hydration must report the events it indexed, got {:?}",
        hydrated.evidence
    );
}
