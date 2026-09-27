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
    session_relationships, session_requests_page, sync_local_at, sync_opencode_at,
    sync_opencode_db, DiscoverOptions, HydrateSessionOptions, SessionScope, SyncOutput,
};
use rusqlite::{Connection, OptionalExtension};
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
        for marker in session_markers(&conn, "opencode", session_id).unwrap() {
            out.markers.push(format!(
                "{}|{}|{}|{:?}|{:?}|{:?}",
                marker.session_id,
                marker.kind,
                marker.marker_uid,
                marker.message_id,
                marker.ts_ms,
                marker.payload_json,
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
    targeted_sync_honors_the_configured_legacy_storage_root();
    shallow_discovery_qualifies_models_identically_across_layouts();
    full_ingest_preserves_the_earlier_session_creation_time();
    the_two_layouts_normalize_to_identical_evidence();
    every_session_in_the_corpus_is_parsed();
    a_parent_id_links_a_child_session_and_the_tree_returns_it();
    a_tool_turn_records_errors_tokens_provider_and_stop_reason();
    a_tool_without_a_target_keeps_its_name_in_the_event();
    a_compaction_part_records_one_boundary_marker();
    a_large_store_is_synced_without_copying_it();
    an_install_indexed_as_prompts_only_gains_events_on_the_next_plain_sync();
    a_turn_appended_as_new_files_is_not_reported_unchanged();
    a_session_file_rewritten_in_place_moves_its_stamp_and_recency();
    a_tool_call_persisted_progressively_keeps_its_final_state();
    a_sqlite_store_named_like_json_is_still_read_as_sqlite();
    a_catalog_locator_from_the_other_layout_is_refused_not_read();
    an_appended_turn_advances_catalog_recency_past_a_stale_session_json();
    an_unindexed_store_syncs_to_the_same_evidence_as_an_indexed_one();
    an_unindexed_store_is_not_read_once_per_session();
    a_sqlite_store_inside_the_storage_dir_still_hydrates();
    an_upgraded_database_delivers_the_new_event_columns();
    #[cfg(unix)]
    {
        an_unreadable_part_does_not_checkpoint_a_session_without_it();
        an_unreadable_part_does_not_catalog_a_session_without_it();
    }
    a_same_length_rewrite_in_one_tick_re_hydrates();
    #[cfg(unix)]
    one_unreadable_session_does_not_take_the_tree_down_with_it();
    a_removed_tool_output_stops_being_served();
    a_directory_named_by_opencode_db_is_read_as_the_legacy_tree();
    a_limit_is_not_spent_on_a_file_that_is_not_a_session();
    #[cfg(unix)]
    a_scope_that_cannot_be_walked_is_reported_not_omitted();
    a_failed_session_query_does_not_checkpoint_an_empty_session();
    one_unreadable_session_does_not_end_the_sqlite_sweep();
    a_long_assistant_turn_is_excerpted_in_the_catalog_and_whole_in_its_event();
    the_exclusive_sync_reads_whichever_layout_the_host_has();
    a_hydration_does_not_move_catalog_recency_backwards();
}

fn shallow_discovery_qualifies_models_identically_across_layouts() {
    let root = temp_root("shallow-model-parity");
    let provider_db = root.join("provider/opencode.db");
    build_sqlite_store(&provider_db);
    Connection::open(&provider_db)
        .unwrap()
        .execute(
            "INSERT INTO part (id, message_id, session_id, time_created, data) \
             VALUES ('aaa_synthetic', 'msg_sqlite_u1', 'ses_sqlite_root', 1776643199000, \
                     '{\"id\":\"aaa_synthetic\",\"sessionID\":\"ses_sqlite_root\",\"messageID\":\"msg_sqlite_u1\",\"type\":\"text\",\"text\":\"continue from summary\",\"synthetic\":true}')",
            [],
        )
        .unwrap();

    let sqlite_home = root.join("sqlite-home");
    use_layout(&sqlite_home, Some(&provider_db), None);
    let sqlite_catalog = root.join("sqlite-catalog.db");
    discover_sessions_scoped_at(&sqlite_catalog, &opencode_only()).unwrap();
    let sqlite_model = catalog_model(&sqlite_catalog, "ses_sqlite_root");
    let sqlite_prompt = catalog_prompt(&sqlite_catalog, "ses_sqlite_root");

    let json_home = root.join("json-home");
    let tree = json_home.join(".local/share/opencode/storage");
    sqlite_store_as_json_tree(&provider_db, &tree);
    use_layout(&json_home, None, Some(&tree));
    let json_catalog = root.join("json-catalog.db");
    discover_sessions_scoped_at(&json_catalog, &opencode_only()).unwrap();
    let json_model = catalog_model(&json_catalog, "ses_sqlite_root");
    let json_prompt = catalog_prompt(&json_catalog, "ses_sqlite_root");

    assert_eq!(
        sqlite_model.as_deref(),
        Some("[\"anthropic/claude-sonnet-4-5\"]"),
        "SQLite discovery must preserve the provider that qualifies the event model"
    );
    assert_eq!(
        sqlite_model, json_model,
        "the same provider payload must produce the same catalog model in both layouts"
    );
    assert_eq!(
        sqlite_prompt.as_deref(),
        Some("add a retry to the client"),
        "SQLite discovery must skip the leading synthetic text part"
    );
    assert_eq!(
        sqlite_prompt, json_prompt,
        "both layouts must apply the shared parser's synthetic-text rule"
    );

    fs::remove_dir_all(&root).ok();
}

fn catalog_model(db_path: &Path, session_id: &str) -> Option<String> {
    open_db(db_path)
        .unwrap()
        .query_row(
            "SELECT models_json FROM sessions WHERE source='opencode' AND session_id=?",
            [session_id],
            |row| row.get(0),
        )
        .unwrap()
}

fn catalog_prompt(db_path: &Path, session_id: &str) -> Option<String> {
    open_db(db_path)
        .unwrap()
        .query_row(
            "SELECT first_prompt FROM sessions WHERE source='opencode' AND session_id=?",
            [session_id],
            |row| row.get(0),
        )
        .unwrap()
}

fn targeted_sync_honors_the_configured_legacy_storage_root() {
    let root = temp_root("targeted-storage-root");
    let home = root.join("home");
    let tree = root.join("relocated/legacy-storage");
    let provider_db = root.join("provider/opencode.db");
    copy_tree(&fixtures().join("legacy-json-simple/storage"), &tree);
    use_layout(&home, Some(&provider_db), Some(&tree));

    let db_path = root.join("history.db");
    sync_opencode_at(&db_path, &provider_db, SyncOutput::Silent).unwrap();

    let count: i64 = open_db(&db_path)
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM sessions WHERE source='opencode' AND session_id='ses_simple'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        count, 1,
        "targeted sync must discover the independently configured legacy tree"
    );

    fs::remove_dir_all(&root).ok();
}

fn full_ingest_preserves_the_earlier_session_creation_time() {
    let root = temp_root("first-activity");
    let provider_db = root.join("provider/opencode.db");
    build_sqlite_store(&provider_db);
    Connection::open(&provider_db)
        .unwrap()
        .execute(
            "UPDATE session SET time_created = 1776643199000 \
             WHERE id = 'ses_sqlite_root'",
            [],
        )
        .unwrap();

    // Sync directly into an empty catalog. Going through discovery first would
    // mask a normalizer regression because the session upsert widens an
    // existing activity window with MIN(existing, incoming).
    let db_path = root.join("history.db");
    let conn = open_db(&db_path).unwrap();
    sync_opencode_db(&conn, &provider_db).unwrap();
    let first_activity: Option<i64> = conn
        .query_row(
            "SELECT first_activity_ms FROM sessions \
             WHERE source='opencode' AND session_id='ses_sqlite_root'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        first_activity,
        Some(1_776_643_199_000),
        "the session was created before its first message and that earlier timestamp must win"
    );
    let requests = session_requests_page(&conn, "opencode", "ses_sqlite_root", 50, None)
        .unwrap()
        .requests;
    assert!(!requests.is_empty(), "the fixture must produce requests");
    assert!(
        requests
            .iter()
            .all(|request| request.provider.as_deref() == Some("anthropic")),
        "request aggregation must preserve OpenCode's recorded provider"
    );

    drop(conn);
    fs::remove_dir_all(&root).ok();
}

fn a_tool_without_a_target_keeps_its_name_in_the_event() {
    let root = temp_root("tool-without-target");
    let home = root.join("home");
    let tree = home.join(".local/share/opencode/storage");
    copy_tree(&fixtures().join("legacy-json-with-tool/storage"), &tree);
    write_json(
        &tree.join("part/msg_tool_asst/prt_tool_5_no_target.json"),
        r#"{
          "id":"prt_tool_5_no_target",
          "sessionID":"ses_tool",
          "messageID":"msg_tool_asst",
          "type":"tool",
          "callID":"toolu_todo_1",
          "tool":"todowrite",
          "state":{"status":"completed","input":{},"output":"done"}
        }"#,
    );
    use_layout(&home, None, Some(&tree));

    let db_path = root.join("history.db");
    sync_local_at(&db_path).unwrap();
    let text: Option<String> = open_db(&db_path)
        .unwrap()
        .query_row(
            "SELECT text FROM session_events \
             WHERE source='opencode' AND session_id='ses_tool' \
             AND event_uid='tool_use:toolu_todo_1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        text.as_deref(),
        Some("todowrite {}"),
        "a targetless tool event must remain present and searchable by tool name"
    );

    fs::remove_dir_all(&root).ok();
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

    fs::remove_dir_all(&root).ok();
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

/// All five legacy-JSON cases at once, so no fixture in the corpus is carried
/// without being read. The other phases each assert one acceptance criterion
/// in depth; this one is the breadth check that would catch a case the parser
/// silently produces nothing for — `simple`, in particular, is covered only
/// here, and it is the case that proves a `step-start` part is ignored rather
/// than mistaken for a turn.
fn every_session_in_the_corpus_is_parsed() {
    let root = temp_root("corpus");
    let home = root.join("home");
    let tree = home.join(".local/share/opencode/storage");
    for case in [
        "legacy-json-simple",
        "legacy-json-multi-turn",
        "legacy-json-with-tool",
        "legacy-json-with-compaction",
        "legacy-json-user-turn-blocks",
    ] {
        copy_tree(&fixtures().join(case).join("storage"), &tree);
    }
    use_layout(&home, None, Some(&tree));
    let db_path = root.join("history.db");
    sync_local_at(&db_path).unwrap();
    let conn = open_db(&db_path).unwrap();

    // (session, user text, assistant text, tool_use, tool_result) counts.
    let expected = [
        ("ses_simple", 0, 1, 0, 0),
        ("ses_multi", 0, 0, 1, 1),
        ("ses_child", 0, 0, 0, 0),
        ("ses_tool", 0, 0, 3, 3),
        ("ses_compact", 2, 1, 0, 0),
        ("ses_utb", 2, 1, 2, 2),
    ];
    for (session_id, users, assistants, tool_uses, tool_results) in expected {
        let events = session_events(&conn, session_id, Some("opencode")).unwrap();
        let count = |role: &str, kind: &str| {
            events
                .iter()
                .filter(|event| event.role == role && event.kind == kind)
                .count()
        };
        assert_eq!(
            (
                count("user", "text"),
                count("assistant", "text"),
                count("assistant", "tool_use"),
                count("tool_result", "tool_result"),
            ),
            (users, assistants, tool_uses, tool_results),
            "{session_id} parsed to the wrong shape; events were {:?}",
            events
                .iter()
                .map(|event| (&event.role, &event.kind, &event.event_uid))
                .collect::<Vec<_>>()
        );
    }

    // `simple`: one assistant turn, its `step-start` part contributing
    // nothing, and the model and tokens carried from the message.
    let simple = session_events(&conn, "ses_simple", Some("opencode")).unwrap();
    assert_eq!(
        simple.len(),
        1,
        "a step-start part is not a turn: {simple:?}"
    );
    assert_eq!(simple[0].text.as_deref(), Some("Hello."));
    assert_eq!(
        simple[0].model.as_deref(),
        Some("anthropic/claude-sonnet-4-5")
    );
    assert_eq!(simple[0].provider.as_deref(), Some("anthropic"));
    assert_eq!(simple[0].stop_reason.as_deref(), Some("end_turn"));

    fs::remove_dir_all(&root).ok();
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

    fs::remove_dir_all(&root).ok();
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

    fs::remove_dir_all(&root).ok();
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
    let markers = session_markers(&conn, "opencode", "ses_compact").unwrap();
    assert_eq!(
        markers.len(),
        1,
        "the compaction fixture has exactly one boundary, got {markers:?}"
    );
    assert_eq!(markers[0].kind, "compaction_boundary");
    assert_eq!(markers[0].message_id.as_deref(), Some("msg_compact_uc"));
    assert_eq!(markers[0].ts_ms, Some(1776999003000));
    assert_eq!(markers[0].payload_json.as_deref(), Some("{\"auto\":true}"));

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
        session_markers(&conn, "opencode", "ses_compact")
            .unwrap()
            .len(),
        1,
        "marker writes are idempotent"
    );

    // The part is a replacement snapshot. If OpenCode removes `auto`, the
    // marker must not keep reporting the prior automatic-compaction detail.
    write_json(
        &tree.join("part/msg_compact_uc/prt_uc_compaction.json"),
        r#"{"id":"prt_uc_compaction","sessionID":"ses_compact","messageID":"msg_compact_uc","type":"compaction"}"#,
    );
    sync_local_at(&db_path).unwrap();
    let conn = open_db(&db_path).unwrap();
    let markers = session_markers(&conn, "opencode", "ses_compact").unwrap();
    assert_eq!(markers.len(), 1);
    assert_eq!(
        markers[0].payload_json, None,
        "a removed compaction detail must not survive the replacement part"
    );

    fs::remove_dir_all(&root).ok();
}

/// How many bytes this process has read, cumulatively, through any `read`.
/// `rchar` counts cached reads too, which is what we want: a copy of the
/// provider store is a copy whether or not it came off the disk.
#[cfg(target_os = "linux")]
/// Bytes this *process* has read.
///
/// Process-wide on purpose, unlike the crate's unit-test probe: this binary
/// runs one `#[test]`, so there is no sibling test to pollute the sample, and
/// sync and discovery do their work on worker threads whose reads a per-thread
/// counter would miss. What is being bounded here is all the reading the
/// operation causes, wherever it happens.
fn bytes_read() -> Option<u64> {
    let io = fs::read_to_string("/proc/self/io").ok()?;
    io.lines()
        .find_map(|line| line.strip_prefix("rchar:"))
        .and_then(|value| value.trim().parse().ok())
}

#[cfg(not(target_os = "linux"))]
fn bytes_read() -> Option<u64> {
    None
}

/// Acceptance: "`sync --local` on a 50 MB `opencode.db` fixture completes
/// without copying the DB (assert no temp backup file is created)."
///
/// The store is built in a tempdir at test time and never checked in.
///
/// Two checks, because the obvious one is a false green. Watching the temp
/// directory would catch a backup file that outlives the sync — but
/// `NamedTempFile` deletes itself on drop, so a copy that *did* happen leaves
/// that directory just as empty as one that did not. Measured, not assumed:
/// re-running this test with `AI_HIST_OPENCODE_BACKUP=1` takes the copy and
/// the directory assertion still passes.
///
/// So the load-bearing check is the number of bytes the process read.
/// Copying a 55 MB store means reading 55 MB; session-keyed queries against
/// it mean reading a fraction of that. Under `AI_HIST_OPENCODE_BACKUP=1` that
/// assertion does fail, with `read 58668442 bytes of a 57798656-byte store`,
/// which is the positive control for the bound. The `read > 0` assertion is
/// the positive control for the probe itself: a reading of "nothing happened"
/// is worth nothing unless something could have.
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
    let before = bytes_read();
    sync_local_at(&db_path).unwrap();
    let after = bytes_read();
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

    if let (Some(before), Some(after)) = (before, after) {
        let read = after.saturating_sub(before);
        // Positive control: the probe is live and the sync did read the store.
        assert!(
            read > 0,
            "the byte counter read nothing at all, so the bound below would mean nothing"
        );
        assert!(
            read < size / 2,
            "the sync read {read} bytes of a {size}-byte store; the bounded path \
             must not read the whole thing, let alone copy it"
        );
    }

    // And it still did the work: the evidence is there.
    let conn = open_db(&db_path).unwrap();
    assert!(
        !session_events(&conn, "ses_sqlite_root", Some("opencode"))
            .unwrap()
            .is_empty(),
        "a bounded sync still has to produce the session's events"
    );

    // 55 MB of ballast has no business outliving the test.
    fs::remove_dir_all(&root).ok();
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

    fs::remove_dir_all(&root).ok();
}

// ---------------------------------------------------------------------------
// Regressions found in review of this change
// ---------------------------------------------------------------------------

/// A legacy-tree session grows by gaining *files*, not by its session JSON
/// changing. Hydration derived the tree root two `parent()` calls up from a
/// scoped session file, which lands on `storage/session` rather than
/// `storage`, so every message and part lookup probed a directory that does
/// not exist. The stamp was then computed over the session file alone and the
/// session reported `unchanged` for the rest of its life.
///
/// Discovery had the same blind spot from the other end: a candidate stamped
/// only by its session file stays cached with its first prompt and model, and
/// under a `--limit` sorts as old while it is the busiest session on the box.
fn a_turn_appended_as_new_files_is_not_reported_unchanged() {
    let root = temp_root("appended");
    let home = root.join("home");
    let tree = home.join(".local/share/opencode/storage");
    copy_tree(&fixtures().join("legacy-json-simple/storage"), &tree);
    use_layout(&home, None, Some(&tree));
    let db_path = root.join("history.db");

    sync_local_at(&db_path).unwrap();
    discover_sessions_scoped_at(&db_path, &opencode_only()).unwrap();
    let first = hydrate(&db_path, "ses_simple");
    assert_eq!(first.status, "hydrated", "the first hydration indexes it");
    let indexed_first = first.evidence.events;
    assert!(indexed_first > 0);

    // Positive control: with nothing touched at all, `unchanged` is correct
    // and is what the checkpoint is for. Without this, the assertion below
    // would pass for a stamp that simply always differs.
    assert_eq!(
        hydrate(&db_path, "ses_simple").status,
        "unchanged",
        "an untouched session must still short-circuit"
    );
    let before = catalog_row(&db_path, "ses_simple");

    // Now append a turn the way OpenCode does: new message and part files,
    // session JSON untouched.
    let session_json = tree.join("session/global/ses_simple.json");
    let session_bytes_before = fs::metadata(&session_json).unwrap().len();
    write_json(
        &tree.join("message/ses_simple/msg_simple_user2.json"),
        r#"{"id":"msg_simple_user2","sessionID":"ses_simple","role":"user","time":{"created":1776988810000}}"#,
    );
    write_json(
        &tree.join("part/msg_simple_user2/prt_simple_u2.json"),
        r#"{"id":"prt_simple_u2","sessionID":"ses_simple","messageID":"msg_simple_user2","type":"text","text":"and now the second turn"}"#,
    );
    assert_eq!(
        fs::metadata(&session_json).unwrap().len(),
        session_bytes_before,
        "the fixture must leave the session file alone, or the test proves nothing"
    );

    let second = hydrate(&db_path, "ses_simple");
    // "updated" rather than "hydrated": the session was already full, and
    // this pass re-read it. Either way the point is that it is not
    // "unchanged" -- the checkpoint was invalidated.
    assert_eq!(
        second.status, "updated",
        "a turn appended as new files must invalidate the checkpoint"
    );
    assert!(
        second.evidence.events > indexed_first,
        "the appended turn must be indexed: {indexed_first} -> {}",
        second.evidence.events
    );

    // Discovery must see it too, with a newer recency than before.
    discover_sessions_scoped_at(&db_path, &opencode_only()).unwrap();
    let after = catalog_row(&db_path, "ses_simple");
    assert_ne!(
        after.0, before.0,
        "the catalog source stamp must move when a turn is appended"
    );
    assert!(
        after.1 > before.1,
        "the catalog recency must advance when a newer turn lands, not merely \
         fail to go backwards: {:?} -> {:?}",
        before.1,
        after.1
    );

    fs::remove_dir_all(&root).ok();
}

/// The discovery stamp used `file_generation_time`, which prefers *birth*
/// time. A session file rewritten in place keeps its birth time, so an edit
/// that also preserved the byte count left the stamp identical and ranked a
/// just-resumed session as old. The stamp reads modification time now, and
/// carries a file count and byte total so an edit that preserves both still
/// moves it.
fn a_session_file_rewritten_in_place_moves_its_stamp_and_recency() {
    let root = temp_root("rewritten");
    let home = root.join("home");
    let tree = home.join(".local/share/opencode/storage");
    copy_tree(&fixtures().join("legacy-json-simple/storage"), &tree);
    use_layout(&home, None, Some(&tree));
    let db_path = root.join("history.db");

    sync_local_at(&db_path).unwrap();
    discover_sessions_scoped_at(&db_path, &opencode_only()).unwrap();
    let before = catalog_row(&db_path, "ses_simple");

    // Rewrite the session file in place, same byte count, later mtime.
    let session_json = tree.join("session/global/ses_simple.json");
    let original = fs::read_to_string(&session_json).unwrap();
    let rewritten = original.replace("simple turn", "simple TURN");
    assert_eq!(
        rewritten.len(),
        original.len(),
        "the rewrite must preserve the byte count, or it proves nothing about birth time"
    );
    assert_ne!(rewritten, original);
    // A coarse filesystem clock would otherwise let the new mtime equal the
    // old one; wait for the observed mtime to actually move rather than
    // sleeping a guessed interval.
    let before_mtime = fs::metadata(&session_json).unwrap().modified().unwrap();
    loop {
        fs::write(&session_json, &rewritten).unwrap();
        if fs::metadata(&session_json).unwrap().modified().unwrap() > before_mtime {
            break;
        }
        std::thread::yield_now();
    }

    discover_sessions_scoped_at(&db_path, &opencode_only()).unwrap();
    let after = catalog_row(&db_path, "ses_simple");
    assert_ne!(
        after.0, before.0,
        "an in-place rewrite that preserves size must still move the stamp"
    );

    fs::remove_dir_all(&root).ok();
}

/// OpenCode persists a tool call once per state as it progresses, each state
/// its own part sharing the `callID`. Keeping the first meant storing a call
/// that looks successful and has no result; the last state is the finished
/// call.
fn a_tool_call_persisted_progressively_keeps_its_final_state() {
    let root = temp_root("progressive");
    let home = root.join("home");
    let tree = home.join(".local/share/opencode/storage");
    write_json(
        &tree.join("session/global/ses_progressive.json"),
        r#"{"id":"ses_progressive","directory":"/tmp/project","time":{"created":1777000000000,"updated":1777000002000}}"#,
    );
    write_json(
        &tree.join("message/ses_progressive/msg_prog_a1.json"),
        r#"{"id":"msg_prog_a1","sessionID":"ses_progressive","role":"assistant","time":{"created":1777000001000},"providerID":"anthropic","modelID":"claude-opus-4-5","path":{"cwd":"/tmp/project"},"tokens":{"input":1,"output":2,"reasoning":0,"cache":{"read":0,"write":0}}}"#,
    );
    // Part ids order the two states: `p1` running, `p2` finished and failed.
    write_json(
        &tree.join("part/msg_prog_a1/p1.json"),
        r#"{"id":"p1","sessionID":"ses_progressive","messageID":"msg_prog_a1","type":"tool","callID":"call_1","tool":"bash","state":{"status":"running","input":{"command":"true"}}}"#,
    );
    write_json(
        &tree.join("part/msg_prog_a1/p2.json"),
        r#"{"id":"p2","sessionID":"ses_progressive","messageID":"msg_prog_a1","type":"tool","callID":"call_1","tool":"bash","state":{"status":"completed","input":{"command":"run the tests"},"output":"ERROR: tests failed","metadata":{"exit":1}}}"#,
    );
    use_layout(&home, None, Some(&tree));
    let db_path = root.join("history.db");
    sync_local_at(&db_path).unwrap();
    let conn = open_db(&db_path).unwrap();

    let calls: Vec<(String, Option<String>, String, Option<i64>)> = conn
        .prepare(
            "SELECT tool_use_id, target, args_json, is_error FROM tool_calls \
             WHERE source='opencode' AND session_id='ses_progressive'",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        calls.len(),
        1,
        "a call persisted twice is still one call: {calls:?}"
    );
    let (tool_use_id, target, args_json, is_error) = &calls[0];
    assert_eq!(tool_use_id, "call_1");
    // The first-write-wins result was `Some("true")`, `exit` absent, hence
    // `is_error = 0` and no tool_result at all. Each of these three is that
    // control, stated as the value it must not have.
    assert_eq!(
        target.as_deref(),
        Some("run the tests"),
        "the final state's arguments win, not the running state's"
    );
    assert!(args_json.contains("run the tests"));
    assert_eq!(
        *is_error,
        Some(1),
        "the final state failed with exit 1; the running state had no exit at all"
    );

    let results: Vec<String> = session_events(&conn, "ses_progressive", Some("opencode"))
        .unwrap()
        .into_iter()
        .filter(|event| event.kind == "tool_result")
        .filter_map(|event| event.text)
        .collect();
    assert_eq!(
        results,
        vec!["ERROR: tests failed".to_string()],
        "exactly one terminal result, from the final state"
    );

    fs::remove_dir_all(&root).ok();
}

/// `OPENCODE_DB` is an arbitrary path, so its suffix says nothing about the
/// layout. Routing the loader by a `.json` extension handed a perfectly good
/// SQLite store to the JSON-tree loader, which indexed nothing — and reported
/// success doing it. The layout now travels from the snapshot that validated
/// the locator.
fn a_sqlite_store_named_like_json_is_still_read_as_sqlite() {
    let root = temp_root("json-named-sqlite");
    let home = root.join("home");
    // A SQLite store that happens to be called `.json`.
    let store = home.join(".local/share/opencode/opencode.json");
    build_sqlite_store(&store);
    use_layout(&home, Some(&store), None);
    let db_path = root.join("history.db");

    // Establish the catalog row and its locator. Global sync does not route
    // by extension, so this alone would index the session and an assertion
    // made after it would pass whatever targeted hydration does -- that is
    // the trap this test has to avoid, so the evidence is cleared before the
    // path under test runs.
    sync_local_at(&db_path).unwrap();
    discover_sessions_scoped_at(&db_path, &opencode_only()).unwrap();
    clear_opencode_evidence(&db_path);

    // Positive control: hydration is now the only thing that can put the
    // events back.
    {
        let conn = open_db(&db_path).unwrap();
        assert!(
            session_events(&conn, "ses_sqlite_root", Some("opencode"))
                .unwrap()
                .is_empty(),
            "the evidence must actually be gone, or this test proves nothing"
        );
    }

    let hydrated = hydrate(&db_path, "ses_sqlite_root");
    assert!(
        hydrated.evidence.events > 0,
        "a SQLite store must be read as SQLite whatever it is called, got {:?}",
        hydrated.evidence
    );

    let conn = open_db(&db_path).unwrap();
    assert!(
        !session_events(&conn, "ses_sqlite_root", Some("opencode"))
            .unwrap()
            .is_empty(),
        "targeted hydration must re-index the session it was pointed at"
    );

    fs::remove_dir_all(&root).ok();
}

/// Drop every OpenCode row targeted hydration would write, and the checkpoint
/// that would let it skip the work. What a session looks like before it has
/// ever been hydrated.
fn clear_opencode_evidence(db_path: &Path) {
    open_db(db_path)
        .unwrap()
        .execute_batch(
            "DELETE FROM session_events WHERE source='opencode';
             DELETE FROM tool_calls WHERE source='opencode';
             DELETE FROM file_edits WHERE source='opencode';
             DELETE FROM session_markers WHERE source='opencode';
             DELETE FROM session_hydration_checkpoints WHERE source='opencode';
             UPDATE sessions SET discovery_state='shallow' WHERE source='opencode';
             UPDATE session_presences SET discovery_state='shallow' WHERE source='opencode';",
        )
        .unwrap();
}

// --- helpers for the regression phases -------------------------------------

fn opencode_only() -> DiscoverOptions {
    DiscoverOptions {
        sources: vec!["opencode".into()],
        ..Default::default()
    }
}

fn hydrate(db_path: &Path, session_id: &str) -> ai_hist::HydrateSessionResult {
    hydrate_result(db_path, session_id).unwrap()
}

/// The error as a string, because `anyhow` is a dependency of the crate and
/// not a dev-dependency, so the test binary cannot name the type.
fn hydrate_result(
    db_path: &Path,
    session_id: &str,
) -> Result<ai_hist::HydrateSessionResult, String> {
    hydrate_session_at(
        db_path,
        &HydrateSessionOptions {
            source: "opencode".into(),
            session_id: session_id.into(),
            scope: SessionScope::Local,
            include_related: false,
        },
    )
    .map_err(|error| format!("{error:#}"))
}

/// `(source_stamp, last_activity_ms)` as the catalog holds them.
fn catalog_row(db_path: &Path, session_id: &str) -> (Option<String>, Option<i64>) {
    open_db(db_path)
        .unwrap()
        .query_row(
            "SELECT source_stamp, last_activity_ms FROM sessions \
             WHERE source='opencode' AND session_id=?",
            [session_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap()
}

/// A catalog row written while only one layout existed still names that
/// layout's locator after the other one appears. Global sync re-detects on
/// every run and would move to `opencode.db`; targeted hydration classified
/// the saved locator without ever asking which layout is current, so it went
/// on reading the stale JSON tree while `sync --local` read SQLite — the two
/// paths disagreeing about the same session.
///
/// The catalog row is stale as a whole in that situation, not just its
/// locator: its prompt, models, timestamps and stamp all came from the tree.
/// Hydrating from SQLite behind its back would leave the checkpoint stamped
/// against a store the row does not describe, so this fails loudly and points
/// at rediscovery instead.
fn a_catalog_locator_from_the_other_layout_is_refused_not_read() {
    let root = temp_root("layout-precedence");
    let home = root.join("home");
    let tree = home.join(".local/share/opencode/storage");
    let store = home.join(".local/share/opencode/opencode.db");
    copy_tree(&fixtures().join("legacy-json-simple/storage"), &tree);

    // Discovery runs while only the tree exists, so the catalog locator is a
    // session file.
    use_layout(&home, None, Some(&tree));
    let db_path = root.join("history.db");
    sync_local_at(&db_path).unwrap();
    discover_sessions_scoped_at(&db_path, &opencode_only()).unwrap();
    let locator = raw_path(&db_path, "ses_simple");
    assert!(
        locator.ends_with("ses_simple.json"),
        "precondition: the catalog must hold a JSON-tree locator, got {locator}"
    );

    // Positive control: while the tree is still the current layout, that
    // locator hydrates fine. Without this the assertion below would pass for
    // a hydration that refuses every legacy locator.
    clear_opencode_evidence(&db_path);
    assert!(
        hydrate(&db_path, "ses_simple").evidence.events > 0,
        "a legacy locator must still hydrate while the tree is the current layout"
    );

    // Now the host upgrades: `opencode.db` appears beside the stale tree.
    build_sqlite_store(&store);
    use_layout(&home, Some(&store), Some(&tree));
    clear_opencode_evidence(&db_path);

    let message = hydrate_result(&db_path, "ses_simple")
        .expect_err("hydration must not read the tree once SQLite is the current layout");
    assert!(
        message.contains("SESSION_SOURCE_MISMATCH"),
        "the stale locator must be refused as a source mismatch, got: {message}"
    );
    assert!(
        message.contains("discoverSessions"),
        "the error must tell the caller how to recover, got: {message}"
    );

    // And it refused rather than quietly indexing the stale tree.
    let conn = open_db(&db_path).unwrap();
    assert!(
        session_events(&conn, "ses_simple", Some("opencode"))
            .unwrap()
            .is_empty(),
        "nothing from the superseded layout may be indexed"
    );
    drop(conn);

    // Rediscovery is the recovery path, and it works: the catalog moves to
    // the SQLite store and its own sessions hydrate.
    sync_local_at(&db_path).unwrap();
    discover_sessions_scoped_at(&db_path, &opencode_only()).unwrap();
    assert!(
        hydrate(&db_path, "ses_sqlite_root").evidence.events > 0,
        "after rediscovery the SQLite sessions hydrate"
    );

    fs::remove_dir_all(&root).ok();
}

/// `last_activity_ms` preferred `session.updated_ms` whenever the provider
/// wrote one, so a session whose JSON says `updated: 1000` while its newest
/// message says `created: 5000` was catalogued as last active at 1000. The
/// catalog listing is newest-first with a limit, so that session sorts behind
/// genuinely older ones and a bounded page can drop it entirely.
///
/// The two values are both real and neither supersedes the other: the answer
/// is the later of them, and either one alone when the other is absent.
///
/// **Discovery runs alone here, with no `sync_local_at` first, and that is
/// the whole point of the test.** The catalog upsert keeps
/// `MAX(existing, incoming)`, and a full sync writes the correct value from
/// the message timestamps — so a version of this test that synced first
/// passed with the defect fully intact, because it was reading the number
/// sync had already stored. Discovery without a preceding sync is exactly the
/// cheap path the catalog exists for, and there the shallow value is the only
/// one written.
fn an_appended_turn_advances_catalog_recency_past_a_stale_session_json() {
    let root = temp_root("stale-updated");
    let home = root.join("home");
    let tree = home.join(".local/share/opencode/storage");

    // The session JSON's own `updated` is far behind its newest message.
    write_json(
        &tree.join("session/global/ses_stale.json"),
        r#"{"id":"ses_stale","directory":"/tmp/project","time":{"created":1000,"updated":1000}}"#,
    );
    write_json(
        &tree.join("message/ses_stale/msg_stale_u1.json"),
        r#"{"id":"msg_stale_u1","sessionID":"ses_stale","role":"user","time":{"created":5000}}"#,
    );
    write_json(
        &tree.join("part/msg_stale_u1/prt_stale_u1.json"),
        r#"{"id":"prt_stale_u1","sessionID":"ses_stale","messageID":"msg_stale_u1","type":"text","text":"a turn the session json never heard about"}"#,
    );

    use_layout(&home, None, Some(&tree));
    let db_path = root.join("history.db");
    discover_sessions_scoped_at(&db_path, &opencode_only()).unwrap();

    let (_, last_activity) = catalog_row(&db_path, "ses_stale");
    assert_eq!(
        last_activity,
        Some(5000),
        "the newest message wins over a stale session `updated`"
    );

    // The converse still holds: a session JSON ahead of its messages keeps
    // its own `updated`, so this is a max and not a blanket switch to the
    // message timestamps.
    write_json(
        &tree.join("session/global/ses_ahead.json"),
        r#"{"id":"ses_ahead","directory":"/tmp/project","time":{"created":1000,"updated":9000}}"#,
    );
    write_json(
        &tree.join("message/ses_ahead/msg_ahead_u1.json"),
        r#"{"id":"msg_ahead_u1","sessionID":"ses_ahead","role":"user","time":{"created":5000}}"#,
    );
    write_json(
        &tree.join("part/msg_ahead_u1/prt_ahead_u1.json"),
        r#"{"id":"prt_ahead_u1","sessionID":"ses_ahead","messageID":"msg_ahead_u1","type":"text","text":"older than the session json"}"#,
    );
    discover_sessions_scoped_at(&db_path, &opencode_only()).unwrap();
    assert_eq!(
        catalog_row(&db_path, "ses_ahead").1,
        Some(9000),
        "a session `updated` ahead of its messages is still the later value"
    );

    fs::remove_dir_all(&root).ok();
}

/// The single-pass fallback for an unindexed provider store has to produce
/// the same evidence as the per-session path, or it is not a fallback — it is
/// a second parser. The fixture store ships the provider's indexes; stripping
/// them changes only the plan, so the rows must be identical.
fn an_unindexed_store_syncs_to_the_same_evidence_as_an_indexed_one() {
    let root = temp_root("unindexed");
    let sessions = ["ses_sqlite_root", "ses_sqlite_child"];

    let indexed_home = root.join("indexed");
    let indexed_store = indexed_home.join(".local/share/opencode/opencode.db");
    build_sqlite_store(&indexed_store);
    use_layout(&indexed_home, Some(&indexed_store), None);
    let indexed_db = root.join("indexed.db");
    sync_local_at(&indexed_db).unwrap();
    let from_indexed = evidence(&indexed_db, &sessions);

    // The same store with the provider's indexes dropped.
    let bare_home = root.join("bare");
    let bare_store = bare_home.join(".local/share/opencode/opencode.db");
    build_sqlite_store(&bare_store);
    {
        let db = Connection::open(&bare_store).unwrap();
        for index in [
            "session_time_updated_id_idx",
            "message_session_time_created_id_idx",
            "part_session_idx",
            "part_message_id_id_idx",
        ] {
            db.execute_batch(&format!("DROP INDEX IF EXISTS {index};"))
                .unwrap();
        }
        // Positive control: the predicate the per-session path would use is
        // now genuinely a scan, so this store really does take the other plan.
        let plan: String = db
            .query_row(
                "EXPLAIN QUERY PLAN SELECT id FROM part WHERE session_id = ?",
                ["x"],
                |row| row.get(3),
            )
            .unwrap();
        assert!(
            plan.contains("SCAN"),
            "the indexes must actually be gone, got plan: {plan}"
        );
    }
    use_layout(&bare_home, Some(&bare_store), None);
    let bare_db = root.join("bare.db");
    sync_local_at(&bare_db).unwrap();
    let from_bare = evidence(&bare_db, &sessions);

    assert!(
        !from_indexed.events.is_empty(),
        "the indexed sweep must produce evidence, or the comparison is vacuous"
    );
    assert_eq!(
        from_indexed, from_bare,
        "the single-pass fallback must produce the same rows as the per-session path"
    );

    fs::remove_dir_all(&root).ok();
}

/// `OPENCODE_DB` and `OPENCODE_STORAGE_DIR` are independent paths, so the
/// database can sit inside the storage directory. Classifying the catalog
/// locator by directory prefix read that live SQLite path as a legacy session
/// file, refused it as superseded, and left the session permanently
/// unhydratable — rediscovery writes back the same database path, so there
/// was no way out.
fn a_sqlite_store_inside_the_storage_dir_still_hydrates() {
    let root = temp_root("db-under-storage");
    let home = root.join("home");
    // The storage dir is a parent of the database.
    let storage = home.join(".local/share/opencode");
    let store = storage.join("opencode.db");
    build_sqlite_store(&store);
    use_layout(&home, Some(&store), Some(&storage));

    let db_path = root.join("history.db");
    sync_local_at(&db_path).unwrap();
    discover_sessions_scoped_at(&db_path, &opencode_only()).unwrap();
    let locator = raw_path(&db_path, "ses_sqlite_root");
    assert!(
        locator.ends_with("opencode.db"),
        "precondition: the catalog must hold the store path, got {locator}"
    );

    clear_opencode_evidence(&db_path);
    let hydrated = hydrate(&db_path, "ses_sqlite_root");
    assert!(
        hydrated.evidence.events > 0,
        "a store that happens to live under the storage dir must still hydrate, got {:?}",
        hydrated.evidence
    );

    fs::remove_dir_all(&root).ok();
}

/// A delivery capture trigger writes the column list it was created with into
/// its own SQL. `CREATE TRIGGER IF NOT EXISTS` will not replace it, so a
/// database that already existed before this PR added `provider` and
/// `stop_reason` keeps capturing the old shape: delivery goes on reporting
/// success while neither field ever reaches the destination, and an upgraded
/// installation's incremental exports quietly differ from a fresh one's.
fn an_upgraded_database_delivers_the_new_event_columns() {
    use ai_hist::export::capture;

    let root = temp_root("upgraded-capture");
    let db_path = root.join("history.db");
    let stripped = {
        let conn = open_db(&db_path).unwrap();
        let tx = conn.unchecked_transaction().unwrap();
        capture::save_subscription(
            &tx,
            &capture::Subscription {
                id: "fixture",
                session: None,
                cursor: 0,
                kind: 0,
                rowid: 0,
                complete: true,
            },
        )
        .unwrap();
        tx.commit().unwrap();

        // Rewind the store to what an installation from before this PR has on
        // disk: `session_events` without the two new columns, and capture
        // triggers whose SQL never mentioned them.
        let mut stripped = Vec::new();
        for operation in ["insert", "update", "delete"] {
            let sql: String = conn
                .query_row(
                    "SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?1",
                    [format!("delivery_session_events_{operation}")],
                    |row| row.get(0),
                )
                .unwrap();
            let mut old_shape = sql;
            for row in ["NEW", "OLD"] {
                for column in ["provider", "stop_reason"] {
                    old_shape = old_shape.replace(&format!(",'{column}',{row}.\"{column}\""), "");
                }
            }
            assert!(
                !old_shape.contains("'provider'") && !old_shape.contains("'stop_reason'"),
                "the pre-upgrade trigger must not mention the new columns: {old_shape}"
            );
            conn.execute_batch(&format!(
                "DROP TRIGGER delivery_session_events_{operation};"
            ))
            .unwrap();
            stripped.push(old_shape);
        }
        // The current derived request view reads provider. A pre-provider
        // database did not have that view shape, and SQLite will not drop a
        // column while a view still references it. Writable reopen rebuilds
        // the view after restoring the column below.
        conn.execute_batch("DROP VIEW IF EXISTS session_requests;")
            .unwrap();
        for column in ["provider", "stop_reason"] {
            conn.execute_batch(&format!("ALTER TABLE session_events DROP COLUMN {column};"))
                .unwrap();
        }
        for sql in &stripped {
            conn.execute_batch(&format!("{sql};")).unwrap();
        }
        stripped
    };

    // Opening the database again is the upgrade: it re-adds the columns.
    let conn = open_db(&db_path).unwrap();
    conn.execute(
        "INSERT INTO session_events(source,session_id,ts_ms,role,kind,event_uid,text,provider,stop_reason) \
         VALUES('opencode','ses_upgrade',1000,'assistant','text','evt_upgrade','hi','anthropic','stop')",
        [],
    )
    .unwrap();
    let payload: String = conn
        .query_row(
            "SELECT payload FROM delivery_journal WHERE kind='session_event' ORDER BY seq DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let captured: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(
        (
            captured.get("provider").and_then(|v| v.as_str()),
            captured.get("stop_reason").and_then(|v| v.as_str())
        ),
        (Some("anthropic"), Some("stop")),
        "an upgraded database must capture the new columns, got {payload}"
    );

    // And the reason it captured them: the trigger itself was replaced, not
    // merely written around.
    let rebuilt: String = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type='trigger' \
             AND name='delivery_session_events_insert'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(
        !stripped.iter().any(|sql| rebuilt.contains(sql.as_str())),
        "the pre-upgrade trigger must not have survived the upgrade"
    );

    fs::remove_dir_all(&root).ok();
}

/// The plan has to be the one sync actually takes, not merely the one
/// `sync_plan` would name. Without `part(session_id)`, a session-keyed read is
/// a full scan of `part`, and global sync issues one per session -- so the
/// store is read once per session rather than once. Both plans produce the
/// same rows (the phase above proves that), so the only way to tell them apart
/// from outside is to count the reading.
///
/// A `part` table well past SQLite's default 2 MB page cache means a repeated
/// scan really does go back to the file, so the two plans are separated by
/// roughly the session count. Bounded at 8x the store, which one sweep clears
/// by a wide margin and 200 scans cannot. The `> 0` assertion is the positive
/// control for the probe; the event count is the positive control for the
/// work, since a sync that read little because it found nothing would pass the
/// bound trivially.
fn an_unindexed_store_is_not_read_once_per_session() {
    let root = temp_root("unindexed-cost");
    let home = root.join("home");
    let store = home.join(".local/share/opencode/opencode.db");
    build_sqlite_store(&store);

    const SESSIONS: usize = 300;
    const PARTS: usize = 30;
    {
        let db = Connection::open(&store).unwrap();
        for index in [
            "session_time_updated_id_idx",
            "message_session_time_created_id_idx",
            "part_session_idx",
            "part_message_id_id_idx",
        ] {
            db.execute_batch(&format!("DROP INDEX IF EXISTS {index};"))
                .unwrap();
        }
        // Rows that stay wholly inside the b-tree leaf pages: a scan then has
        // to read them, where a row whose text spills into overflow pages
        // would be skipped over and the scan would cost almost nothing.
        let filler = "x".repeat(700);
        db.execute_batch("BEGIN").unwrap();
        for n in 0..SESSIONS {
            let session = format!("ses_bulk_{n}");
            db.execute(
                "INSERT INTO session (id, parent_id, directory, time_created, time_updated) \
                 VALUES (?1, NULL, '/tmp/project', 1776643200000, 1776643260000)",
                [&session],
            )
            .unwrap();
            let message = format!("msg_bulk_{n}");
            let message_data = format!(
                "{{\"id\":\"{message}\",\"sessionID\":\"{session}\",\"role\":\"user\",\
                 \"time\":{{\"created\":1776643200000}}}}"
            );
            db.execute(
                "INSERT INTO message (id, session_id, time_created, data) \
                 VALUES (?1, ?2, 1776643200000, ?3)",
                rusqlite::params![&message, &session, &message_data],
            )
            .unwrap();
            for part in 0..PARTS {
                let id = format!("prt_bulk_{n}_{part}");
                let data = format!("{{\"id\":\"{id}\",\"type\":\"text\",\"text\":\"{filler}\"}}");
                db.execute(
                    "INSERT INTO part (id, message_id, session_id, time_created, data) \
                     VALUES (?1, ?2, ?3, 1776643200000, ?4)",
                    rusqlite::params![&id, &message, &session, &data],
                )
                .unwrap();
            }
        }
        db.execute_batch("COMMIT").unwrap();

        // Positive control for the premise: the per-session predicate on this
        // store really is a scan, so the two plans really do differ here.
        let plan: String = db
            .query_row(
                "EXPLAIN QUERY PLAN SELECT id FROM part WHERE session_id = ?",
                ["x"],
                |row| row.get(3),
            )
            .unwrap();
        assert!(plan.contains("SCAN"), "expected an unindexed store: {plan}");
    }
    let size = fs::metadata(&store).unwrap().len();
    assert!(
        size >= 6 * 1024 * 1024,
        "the store must be well past SQLite's page cache, got {size} bytes"
    );

    use_layout(&home, Some(&store), None);
    let db_path = root.join("history.db");
    let before = bytes_read();
    sync_local_at(&db_path).unwrap();
    let after = bytes_read();

    let conn = open_db(&db_path).unwrap();
    let events: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM session_events \
             WHERE source='opencode' AND session_id LIKE 'ses_bulk_%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        events,
        (SESSIONS * PARTS) as i64,
        "the sweep must have read every synthetic session, or the bound is vacuous"
    );
    drop(conn);

    if let (Some(before), Some(after)) = (before, after) {
        let read = after.saturating_sub(before);
        assert!(read > 0, "the read probe reported nothing at all");
        // One sweep of the source plus the destination writes it provokes
        // measured 52 MB here; a scan per session measured 2.73 GB, which is
        // the positive control for this bound. The gap is the session count,
        // so anywhere between them separates the two plans decisively.
        assert!(
            read < size * 32,
            "the sync read {read} bytes of a {size}-byte store, about \
             {:.0}x: {SESSIONS} scans of it, not one sweep",
            read as f64 / size as f64
        );
    }

    fs::remove_dir_all(&root).ok();
}

/// Replace `path` with something the scan still enumerates and still stats,
/// but cannot read, and wait until it is old enough that the stamp treats it
/// as settled.
///
/// The socket is #190's technique and its rationale holds here: `chmod 000`
/// does nothing as root, which is how the suite runs, and a directory in place
/// of the file is not a `.json` file to the scan at all. `open(2)` on a socket
/// fails with `ENXIO` for any uid while `metadata()` still succeeds.
///
/// The settling matters for *this* suite specifically: a recent file is one
/// the stamp reads, so an unsettled socket would fail in the stamp and the
/// test would pass whether or not the loader propagates anything. Settling it
/// leaves the loader as the only thing that can catch it.
#[cfg(unix)]
fn make_unreadable_and_settled(path: &Path) {
    fs::remove_file(path).unwrap();
    // Bound at a short path and moved into place: `sun_path` is 108 bytes and
    // these fixture roots are longer than that, but a socket file renames like
    // any other directory entry. Leaked deliberately -- dropping the listener
    // would not remove the socket file, and the file is what the test needs.
    let staging = std::env::temp_dir().join(format!(
        "aihs{}{}.sock",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos()
    ));
    std::mem::forget(std::os::unix::net::UnixListener::bind(&staging).unwrap());
    fs::rename(&staging, path).unwrap();
    assert!(
        fs::read_to_string(path).is_err(),
        "the fixture must actually be unreadable or this test proves nothing"
    );
    assert!(
        fs::metadata(path).is_ok(),
        "the fixture must still stat, or nothing would enumerate it"
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let age = std::time::SystemTime::now()
            .duration_since(fs::metadata(path).unwrap().modified().unwrap())
            .unwrap_or_default();
        if age > std::time::Duration::from_millis(2_500) {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the fixture never settled"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

/// A read that fails is not a reading. The legacy loader turned an unreadable
/// message or part into an omission and hydration committed its checkpoint
/// over the result — and because an unreadable file keeps its size and its
/// mtime, the stamp the checkpoint was written against is still current after
/// access recovers, so the missing events never come back.
#[cfg(unix)]
fn an_unreadable_part_does_not_checkpoint_a_session_without_it() {
    let root = temp_root("unreadable-part");
    let home = root.join("home");
    let tree = home.join(".local/share/opencode/storage");
    copy_tree(&fixtures().join("legacy-json-simple/storage"), &tree);
    use_layout(&home, None, Some(&tree));
    let db_path = root.join("history.db");

    sync_local_at(&db_path).unwrap();
    discover_sessions_scoped_at(&db_path, &opencode_only()).unwrap();
    clear_opencode_evidence(&db_path);

    let part = tree.join("part/msg_simple_asst/prt_simple_2.json");
    make_unreadable_and_settled(&part);

    let error = hydrate_result(&db_path, "ses_simple")
        .expect_err("an unreadable part must fail the hydration, not shorten it");
    assert!(
        error.contains("prt_simple_2.json"),
        "the error must name the file that failed, got {error}"
    );
    assert_eq!(
        checkpoint_stamp(&db_path, "ses_simple"),
        None,
        "a hydration that could not read the session must not leave a checkpoint"
    );
    assert!(
        session_events(&open_db(&db_path).unwrap(), "ses_simple", Some("opencode"))
            .unwrap()
            .is_empty(),
        "and it must not leave partial evidence behind either"
    );

    // Access recovers, and so does the session — the outage left nothing
    // behind that outlives it.
    fs::remove_file(&part).unwrap();
    copy_tree(
        &fixtures().join("legacy-json-simple/storage/part/msg_simple_asst"),
        &tree.join("part/msg_simple_asst"),
    );
    let hydrated = hydrate(&db_path, "ses_simple");
    assert!(
        hydrated.evidence.events > 0,
        "the session must hydrate once the file is readable again, got {:?}",
        hydrated.evidence
    );
    assert!(checkpoint_stamp(&db_path, "ses_simple").is_some());

    fs::remove_dir_all(&root).ok();
}

/// The discovery half of the same rule. A catalog row written while a part was
/// unreadable describes a session it could not read, and it is stamped with
/// the source stamp of that moment — which an unreadable file does not move,
/// so the next run finds the stamp current and skips the session for good.
#[cfg(unix)]
fn an_unreadable_part_does_not_catalog_a_session_without_it() {
    let root = temp_root("unreadable-discovery");
    let home = root.join("home");
    let tree = home.join(".local/share/opencode/storage");
    copy_tree(&fixtures().join("legacy-json-simple/storage"), &tree);
    use_layout(&home, None, Some(&tree));
    let db_path = root.join("history.db");

    let part = tree.join("part/msg_simple_asst/prt_simple_2.json");
    make_unreadable_and_settled(&part);

    let (_, summary) = discover_sessions_scoped_at(&db_path, &opencode_only()).unwrap();
    assert!(
        summary
            .diagnostics
            .iter()
            .any(|entry| entry.error.contains("prt_simple_2.json")),
        "the failure must be reported, not swallowed: {:?}",
        summary.diagnostics
    );
    assert_eq!(
        catalog_stamp(&db_path, "ses_simple"),
        None,
        "no catalog row may be stamped as current from a read that failed"
    );

    fs::remove_file(&part).unwrap();
    copy_tree(
        &fixtures().join("legacy-json-simple/storage/part/msg_simple_asst"),
        &tree.join("part/msg_simple_asst"),
    );
    discover_sessions_scoped_at(&db_path, &opencode_only()).unwrap();
    assert!(
        catalog_stamp(&db_path, "ses_simple").is_some(),
        "and the session must appear once the file is readable again"
    );

    fs::remove_dir_all(&root).ok();
}

/// `bytes`, `files` and `newest_ns` are aggregates, and aggregates collide. A
/// part rewritten to a different payload of the same length moves none of
/// them, and if the rewrite lands inside the same filesystem timestamp tick as
/// the read before it, the mtime does not move either — so the session read as
/// `unchanged` and kept the superseded text forever. Pinning the mtime is how
/// a coarse tick is reproduced without waiting for one.
fn a_same_length_rewrite_in_one_tick_re_hydrates() {
    let root = temp_root("same-tick-rewrite");
    let home = root.join("home");
    let tree = home.join(".local/share/opencode/storage");
    copy_tree(&fixtures().join("legacy-json-simple/storage"), &tree);
    use_layout(&home, None, Some(&tree));
    let db_path = root.join("history.db");
    let part = tree.join("part/msg_simple_asst/prt_simple_2.json");

    // Fix the tick both reads see, rather than inheriting whatever the fixture
    // copy left behind.
    let tick = std::time::SystemTime::now();
    pin_mtime(&part, tick);

    sync_local_at(&db_path).unwrap();
    discover_sessions_scoped_at(&db_path, &opencode_only()).unwrap();
    hydrate(&db_path, "ses_simple");

    // Control: nothing changed, so nothing is re-read.
    assert_eq!(
        hydrate(&db_path, "ses_simple").status,
        "unchanged",
        "an untouched session must still short-circuit, or the fix is just a \
         cache that never hits"
    );

    let original = fs::read_to_string(&part).unwrap();
    let rewritten = original.replace("Hello.", "Adieu.");
    assert_eq!(rewritten.len(), original.len());
    assert_ne!(rewritten, original);
    fs::write(&part, &rewritten).unwrap();
    pin_mtime(&part, tick);
    assert!(
        std::time::SystemTime::now()
            .duration_since(tick)
            .unwrap_or_default()
            < std::time::Duration::from_secs(2),
        "premise: the rewrite must still be inside the ambiguity window"
    );

    let again = hydrate(&db_path, "ses_simple");
    assert_ne!(
        again.status, "unchanged",
        "a same-length rewrite inside one mtime tick must still be re-read"
    );
    let texts: Vec<String> =
        session_events(&open_db(&db_path).unwrap(), "ses_simple", Some("opencode"))
            .unwrap()
            .into_iter()
            .filter_map(|event| event.text)
            .collect();
    assert!(
        texts.iter().any(|text| text.contains("Adieu.")),
        "and the new text must be what is indexed, got {texts:?}"
    );

    fs::remove_dir_all(&root).ok();
}

/// Pin `path`'s modification time, leaving its contents alone.
fn pin_mtime(path: &Path, when: std::time::SystemTime) {
    let file = fs::OpenOptions::new().write(true).open(path).unwrap();
    file.set_times(fs::FileTimes::new().set_modified(when))
        .unwrap();
    assert_eq!(fs::metadata(path).unwrap().modified().unwrap(), when);
}

/// The stamp the hydration checkpoint was written against, if there is one.
fn checkpoint_stamp(db_path: &Path, session_id: &str) -> Option<String> {
    open_db(db_path)
        .unwrap()
        .query_row(
            "SELECT source_stamp FROM session_hydration_checkpoints \
             WHERE source='opencode' AND session_id=?",
            [session_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()
        .unwrap()
        .flatten()
}

/// The stamp the catalog row was written against, if there is one.
fn catalog_stamp(db_path: &Path, session_id: &str) -> Option<String> {
    open_db(db_path)
        .unwrap()
        .query_row(
            "SELECT source_stamp FROM sessions WHERE source='opencode' AND session_id=?",
            [session_id],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()
        .unwrap()
        .flatten()
}

/// Make `path` a file that still enumerates and still has a `.json` name, but
/// that neither `metadata()` nor a read can resolve.
///
/// A symlink to itself: `stat(2)` follows it and returns `ELOOP`, for any uid
/// and whatever its age, so it fails the stamp *and* the loader deterministically
/// — where the socket fixture used elsewhere fails only the read, and only
/// while it is recent. Both properties are asserted, because a fixture that
/// quietly stops being unreadable is a test that quietly stops testing.
#[cfg(unix)]
fn make_unresolvable(path: &Path) {
    if path.exists() {
        fs::remove_file(path).unwrap();
    }
    let name = path.file_name().unwrap().to_owned();
    std::os::unix::fs::symlink(&name, path).unwrap();
    assert!(
        fs::metadata(path).is_err(),
        "the fixture must not stat, or the stamp would never reach it"
    );
    assert!(
        fs::read_to_string(path).is_err(),
        "the fixture must not read, or the loader would never reach it"
    );
    assert!(
        fs::read_dir(path.parent().unwrap())
            .unwrap()
            .filter_map(|entry| entry.ok())
            .any(|entry| entry.file_name() == name),
        "the fixture must still be listed, or nothing would look at it at all"
    );
}

/// One broken session is one broken session. The stamp and the loader report
/// I/O errors now, and both multi-session loops propagated the first one — so
/// a single unresolvable part took the whole tree's enumeration and the whole
/// tree's sync down with it, and every healthy session beside it went
/// uncataloged and unindexed for as long as that one path stayed broken.
/// `read_shallow` already isolates this per locator; the loops have to as well.
#[cfg(unix)]
fn one_unreadable_session_does_not_take_the_tree_down_with_it() {
    let root = temp_root("isolate-failure");
    let home = root.join("home");
    let tree = home.join(".local/share/opencode/storage");
    copy_tree(&fixtures().join("legacy-json-simple/storage"), &tree);

    // A second, healthy session beside it. `ses_broken` sorts first, so a loop
    // that stops at the first failure never reaches `ses_simple`.
    write_json(
        &tree.join("session/global/ses_broken.json"),
        r#"{"id":"ses_broken","directory":"/tmp/project","time":{"created":1777100000000,"updated":1777100002000}}"#,
    );
    write_json(
        &tree.join("message/ses_broken/msg_broken.json"),
        r#"{"id":"msg_broken","sessionID":"ses_broken","role":"user","time":{"created":1777100001000}}"#,
    );
    write_json(
        &tree.join("part/msg_broken/prt_broken.json"),
        r#"{"id":"prt_broken","sessionID":"ses_broken","messageID":"msg_broken","type":"text","text":"never read"}"#,
    );
    assert!(
        "ses_broken.json" < "ses_simple.json",
        "the broken session must sort first or the loops would reach the healthy one anyway"
    );
    make_unresolvable(&tree.join("part/msg_broken/prt_broken.json"));

    use_layout(&home, None, Some(&tree));
    let db_path = root.join("history.db");

    // Sync: the healthy session is indexed, and the failure is still reported.
    let error = sync_local_at(&db_path)
        .err()
        .map(|error| format!("{error:#}"));
    let healthy: Vec<String> =
        session_events(&open_db(&db_path).unwrap(), "ses_simple", Some("opencode"))
            .unwrap()
            .into_iter()
            .filter_map(|event| event.text)
            .collect();
    assert!(
        !healthy.is_empty(),
        "the healthy session must still be indexed, got {healthy:?} (sync error: {error:?})"
    );
    assert!(
        session_events(&open_db(&db_path).unwrap(), "ses_broken", Some("opencode"))
            .unwrap()
            .is_empty(),
        "and the broken one must not be indexed from a read that failed"
    );

    // Discovery: same shape. The healthy session is cataloged with a stamp,
    // the broken one is a diagnostic against its own locator, and nothing is
    // remembered as a non-session.
    let (_, summary) = discover_sessions_scoped_at(&db_path, &opencode_only()).unwrap();
    assert!(
        catalog_stamp(&db_path, "ses_simple").is_some(),
        "the healthy session must be cataloged"
    );
    assert!(
        summary.diagnostics.iter().any(|entry| entry
            .locator
            .as_deref()
            .is_some_and(|locator| locator.ends_with("ses_broken.json"))),
        "the broken session must be reported against its own locator: {:?}",
        summary.diagnostics
    );
    assert_eq!(
        catalog_stamp(&db_path, "ses_broken"),
        None,
        "and no catalog row may be stamped as current from a read that failed"
    );
    assert_eq!(
        skip_rows(&db_path),
        0,
        "nor may the failure be remembered as 'not a session'"
    );

    // And it recovers without the healthy session ever having been disturbed.
    fs::remove_file(tree.join("part/msg_broken/prt_broken.json")).unwrap();
    write_json(
        &tree.join("part/msg_broken/prt_broken.json"),
        r#"{"id":"prt_broken","sessionID":"ses_broken","messageID":"msg_broken","type":"text","text":"now readable"}"#,
    );
    sync_local_at(&db_path).unwrap();
    discover_sessions_scoped_at(&db_path, &opencode_only()).unwrap();
    assert!(catalog_stamp(&db_path, "ses_broken").is_some());
    assert!(
        !session_events(&open_db(&db_path).unwrap(), "ses_broken", Some("opencode"))
            .unwrap()
            .is_empty()
    );

    fs::remove_dir_all(&root).ok();
}

/// A read of an OpenCode session is a read of the session *as it is now*, and
/// OpenCode rewrites parts in place. An upsert-only ingest cannot express a
/// removal: the replacement row is never emitted, the superseded row keeps its
/// uniqueness key, and `getSessionEvents` goes on serving a tool result whose
/// output the provider deleted.
fn a_removed_tool_output_stops_being_served() {
    let root = temp_root("retire-absent");
    let home = root.join("home");
    let tree = home.join(".local/share/opencode/storage");
    let part = tree.join("part/msg_retire_a1/prt_retire_tool.json");
    let message = tree.join("message/ses_retire/msg_retire_a1.json");
    write_json(
        &tree.join("session/global/ses_retire.json"),
        r#"{"id":"ses_retire","parentID":"ses_retire_parent","directory":"/tmp/project","time":{"created":1777200000000,"updated":1777200002000}}"#,
    );
    write_json(
        &message,
        r#"{"id":"msg_retire_a1","sessionID":"ses_retire","role":"assistant","time":{"created":1777200001000},"providerID":"anthropic","modelID":"claude-opus-4-5","path":{"cwd":"/tmp/project"}}"#,
    );
    write_json(
        &part,
        r#"{"id":"prt_retire_tool","sessionID":"ses_retire","messageID":"msg_retire_a1","type":"tool","callID":"call_retire","tool":"bash","state":{"status":"completed","input":{"command":"cat secret"},"output":"secret-old-output"}}"#,
    );
    write_json(
        &tree.join("part/msg_retire_a1/prt_retire_text.json"),
        r#"{"id":"prt_retire_text","sessionID":"ses_retire","messageID":"msg_retire_a1","type":"text","text":"kept"}"#,
    );
    use_layout(&home, None, Some(&tree));
    let db_path = root.join("history.db");

    sync_local_at(&db_path).unwrap();
    let texts = |db_path: &Path| -> Vec<String> {
        session_events(&open_db(db_path).unwrap(), "ses_retire", Some("opencode"))
            .unwrap()
            .into_iter()
            .filter_map(|event| event.text)
            .collect()
    };
    assert!(
        texts(&db_path).iter().any(|t| t == "secret-old-output"),
        "precondition: the output must be served before it is removed, got {:?}",
        texts(&db_path)
    );
    assert_eq!(relationship_count(&db_path, "ses_retire"), 1);

    // The parent link remains, but the complete child snapshot no longer
    // names an assistant model. That observed removal must clear the model
    // rather than being treated as a thinner partial observation.
    write_json(
        &message,
        r#"{"id":"msg_retire_a1","sessionID":"ses_retire","role":"assistant","time":{"created":1777200001000},"path":{"cwd":"/tmp/project"}}"#,
    );
    sync_local_at(&db_path).unwrap();
    let child_model = open_db(&db_path)
        .unwrap()
        .query_row(
            "SELECT child_model FROM session_relationships \
             WHERE source='opencode' AND child_session_id='ses_retire'",
            [],
            |row| row.get::<_, Option<String>>(0),
        )
        .unwrap();
    assert_eq!(
        child_model, None,
        "a model removed from the child snapshot must not survive on its relationship"
    );

    // OpenCode rewrites the part without the output, and drops the parent link.
    write_json(
        &part,
        r#"{"id":"prt_retire_tool","sessionID":"ses_retire","messageID":"msg_retire_a1","type":"tool","callID":"call_retire","tool":"bash","state":{"status":"completed","input":{"command":"cat secret"}}}"#,
    );
    write_json(
        &tree.join("session/global/ses_retire.json"),
        r#"{"id":"ses_retire","directory":"/tmp/project","time":{"created":1777200000000,"updated":1777200003000}}"#,
    );
    sync_local_at(&db_path).unwrap();

    let after = texts(&db_path);
    assert!(
        !after.iter().any(|t| t == "secret-old-output"),
        "a tool result the provider removed must stop being served, got {after:?}"
    );
    // Positive controls: reconciliation is by absence, not by clearing. The
    // untouched text event and the tool call itself are still there.
    assert!(
        after.iter().any(|t| t == "kept"),
        "the parts that did not change must survive, got {after:?}"
    );
    assert_eq!(
        tool_call_ids(&db_path, "ses_retire"),
        vec!["call_retire".to_string()],
        "the call is still in the transcript; only its output went away"
    );
    assert_eq!(
        relationship_count(&db_path, "ses_retire"),
        0,
        "a parentID the session no longer names must not survive either"
    );

    fs::remove_dir_all(&root).ok();
}

/// `OPENCODE_DB` is an arbitrary path, and `exists()` is true for a directory.
/// Sync asked one question and `OpencodeLayout::detect` — which discovery and
/// hydration both use — asked another, so a host whose `OPENCODE_DB` names a
/// directory had its sessions cataloged from the legacy tree and then indexed
/// from nothing, with the sync failing on a path it should never have opened.
fn a_directory_named_by_opencode_db_is_read_as_the_legacy_tree() {
    let root = temp_root("db-is-a-directory");
    let home = root.join("home");
    let tree = home.join(".local/share/opencode/storage");
    copy_tree(&fixtures().join("legacy-json-simple/storage"), &tree);

    // A directory where the database would be. `exists()` says yes.
    let db_like = home.join(".local/share/opencode/opencode.db");
    fs::create_dir_all(&db_like).unwrap();
    assert!(db_like.exists() && !db_like.is_file());
    use_layout(&home, Some(&db_like), Some(&tree));

    let db_path = root.join("history.db");
    sync_local_at(&db_path).unwrap();
    let events: Vec<String> =
        session_events(&open_db(&db_path).unwrap(), "ses_simple", Some("opencode"))
            .unwrap()
            .into_iter()
            .filter_map(|event| event.text)
            .collect();
    assert!(
        !events.is_empty(),
        "the legacy tree must be indexed when the database path is not a file, got {events:?}"
    );

    fs::remove_dir_all(&root).ok();
}

/// `read_shallow` is what decides whether a `ses_*.json` file is a session.
/// Truncating the candidate list before that let a newest malformed file spend
/// the whole of a `--limit 1`, and the valid session behind it was never
/// discovered at all — not deferred to a later page, absent.
fn a_limit_is_not_spent_on_a_file_that_is_not_a_session() {
    let root = temp_root("limit-and-junk");
    let home = root.join("home");
    let tree = home.join(".local/share/opencode/storage");
    write_json(
        &tree.join("session/global/ses_valid.json"),
        r#"{"id":"ses_valid","directory":"/tmp/project","time":{"created":1777300000000,"updated":1777300001000}}"#,
    );
    write_json(
        &tree.join("message/ses_valid/msg_valid.json"),
        r#"{"id":"msg_valid","sessionID":"ses_valid","role":"user","time":{"created":1777300001000}}"#,
    );
    write_json(
        &tree.join("part/msg_valid/prt_valid.json"),
        r#"{"id":"prt_valid","sessionID":"ses_valid","messageID":"msg_valid","type":"text","text":"a real turn"}"#,
    );
    // A `ses_*.json` file that is not a session, written last so it stamps as
    // the newest and takes the head of the page.
    let junk = tree.join("session/global/ses_zzz_junk.json");
    let valid_mtime = fs::metadata(tree.join("session/global/ses_valid.json"))
        .unwrap()
        .modified()
        .unwrap();
    write_json(&junk, "{ not a session at all");
    pin_mtime(&junk, valid_mtime + std::time::Duration::from_secs(60));

    use_layout(&home, None, Some(&tree));
    let db_path = root.join("history.db");

    let (sessions, _) = discover_sessions_scoped_at(
        &db_path,
        &DiscoverOptions {
            sources: vec!["opencode".into()],
            limit: Some(1),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        sessions
            .iter()
            .map(|session| session.session_id.as_str())
            .collect::<Vec<_>>(),
        vec!["ses_valid"],
        "a file that is not a session must not spend the page's one slot"
    );

    fs::remove_dir_all(&root).ok();
}

/// How many `observation_discovery_skips` rows exist at all.
fn skip_rows(db_path: &Path) -> i64 {
    open_db(db_path)
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM observation_discovery_skips",
            [],
            |row| row.get(0),
        )
        .unwrap()
}

/// Relationship rows this session's own file owns — the ones naming its parent.
fn relationship_count(db_path: &Path, session_id: &str) -> i64 {
    open_db(db_path)
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM session_relationships WHERE source='opencode' \
             AND child_session_id=? AND evidence_kind='opencode_parent_id'",
            [session_id],
            |row| row.get(0),
        )
        .unwrap()
}

fn tool_call_ids(db_path: &Path, session_id: &str) -> Vec<String> {
    open_db(db_path)
        .unwrap()
        .prepare("SELECT tool_use_id FROM tool_calls WHERE source='opencode' AND session_id=? ORDER BY tool_use_id")
        .unwrap()
        .query_map([session_id], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<String>>>()
        .unwrap()
}

/// Put a directory under `session/` whose path is too long to list.
///
/// `read_dir` on it fails with `ENAMETOOLONG` for any uid, which is what this
/// needs: `chmod 000` does nothing here (the suite runs as root and keeps
/// `CAP_DAC_READ_SEARCH`), and a symlink is not recursed into at all because
/// `DirEntry::file_type` does not follow one. The chain is built under a short
/// path — every `mkdir` along it succeeds — and then moved under a padded one,
/// which renames the whole subtree in a single call without revalidating the
/// length of anything inside it.
///
/// Returns the deepest directory that now exists and cannot be listed.
#[cfg(unix)]
fn make_unwalkable_scope(session_dir: &Path) -> PathBuf {
    const WIDTH: usize = 250;

    let staging = std::env::temp_dir().join(format!("ocw{}", std::process::id()));
    fs::remove_dir_all(&staging).ok();
    fs::create_dir_all(&staging).unwrap();
    let path_max = libc::PATH_MAX as usize;
    let levels = path_max.saturating_sub(staging.as_os_str().len() + WIDTH / 2) / (WIDTH + 1);
    assert!(levels > 0, "the staging path leaves no room for one level");
    let mut deep = staging.clone();
    for level in 0..levels {
        deep = deep.join(
            std::char::from_u32('a' as u32 + level as u32)
                .unwrap()
                .to_string()
                .repeat(WIDTH),
        );
        fs::create_dir(&deep).unwrap();
    }
    assert!(
        fs::read_dir(&deep).is_ok(),
        "the chain must be listable where it was built, or it proves nothing"
    );

    let mut pad = session_dir.join("pad");
    for _ in 0..3 {
        pad = pad.join("p".repeat(WIDTH));
    }
    fs::create_dir_all(&pad).unwrap();
    let moved = pad.join("deep");
    fs::rename(&staging, &moved).unwrap();

    let unwalkable = deep
        .strip_prefix(&staging)
        .map(|rest| moved.join(rest))
        .unwrap();
    assert!(
        unwalkable.as_os_str().len() > path_max,
        "the path must exceed PATH_MAX, got {}",
        unwalkable.as_os_str().len()
    );
    let error = fs::read_dir(&unwalkable)
        .expect_err("the fixture must actually fail to list or this test proves nothing");
    assert_eq!(
        error.raw_os_error(),
        Some(libc::ENAMETOOLONG),
        "expected ENAMETOOLONG"
    );
    unwalkable
}

/// A scope directory that cannot be walked is not a scope with no sessions in
/// it. The walk dropped the listing error and handed its caller a
/// complete-looking list, so every session under that scope was absent from
/// sync and from discovery with nothing saying why — the same shape as the
/// read failures fixed in the pushes before this, one level up.
#[cfg(unix)]
fn a_scope_that_cannot_be_walked_is_reported_not_omitted() {
    let root = temp_root("unwalkable-scope");
    let home = root.join("home");
    let tree = home.join(".local/share/opencode/storage");
    copy_tree(&fixtures().join("legacy-json-simple/storage"), &tree);
    let unwalkable = make_unwalkable_scope(&tree.join("session"));
    use_layout(&home, None, Some(&tree));
    let db_path = root.join("history.db");

    // The healthy session beside it is still indexed, and the failure is still
    // reported rather than swallowed.
    sync_local_at(&db_path).unwrap();
    assert!(
        !session_events(&open_db(&db_path).unwrap(), "ses_simple", Some("opencode"))
            .unwrap()
            .is_empty(),
        "a scope that cannot be walked must not cost the sessions that can"
    );

    let (_, summary) = discover_sessions_scoped_at(&db_path, &opencode_only()).unwrap();
    assert!(
        catalog_stamp(&db_path, "ses_simple").is_some(),
        "the healthy session must still be cataloged"
    );
    assert!(
        summary.diagnostics.iter().any(|entry| entry
            .locator
            .as_deref()
            .is_some_and(|locator| unwalkable.to_string_lossy().starts_with(locator))),
        "the directory that could not be walked must be reported: {:?}",
        summary
            .diagnostics
            .iter()
            .map(|entry| entry.locator.clone())
            .collect::<Vec<_>>()
    );

    // Removing the unwalkable subtree leaves a clean tree and a clean run.
    fs::remove_dir_all(tree.join("session/pad")).ok();
    let (_, summary) = discover_sessions_scoped_at(&db_path, &opencode_only()).unwrap();
    assert!(
        summary.diagnostics.is_empty(),
        "and a tree with nothing wrong with it must report nothing: {:?}",
        summary.diagnostics
    );

    fs::remove_dir_all(&root).ok();
}

/// `query_row(..).ok()` turned every SQLite failure into "this session is not
/// in the store". Hydration then indexed nothing, called that the session's
/// current state, and wrote the checkpoint — so the retry the failure called
/// for never happened, and the empty session stayed empty.
fn a_failed_session_query_does_not_checkpoint_an_empty_session() {
    let root = temp_root("failed-query");
    let home = root.join("home");
    let store = home.join(".local/share/opencode/opencode.db");
    build_sqlite_store(&store);
    {
        // A provider value this row mapping cannot take. SQLite columns are
        // dynamically typed, so the store holds it happily.
        let db = Connection::open(&store).unwrap();
        db.execute(
            "UPDATE session SET parent_id = x'ff' WHERE id = 'ses_sqlite_child'",
            [],
        )
        .unwrap();
    }
    use_layout(&home, Some(&store), None);
    let db_path = root.join("history.db");

    sync_local_at(&db_path).ok();
    discover_sessions_scoped_at(&db_path, &opencode_only()).ok();
    clear_opencode_evidence(&db_path);

    let error = hydrate_result(&db_path, "ses_sqlite_child")
        .expect_err("a failed query must fail the hydration, not empty the session");
    assert!(
        error.contains("ses_sqlite_child"),
        "the error must name the session that failed, got {error}"
    );
    assert_eq!(
        checkpoint_stamp(&db_path, "ses_sqlite_child"),
        None,
        "and nothing may be checkpointed as this session's current state"
    );

    // Positive control: the sibling session in the same store is untouched by
    // any of it, so this is one session's failure and not the store's.
    let hydrated = hydrate(&db_path, "ses_sqlite_root");
    assert!(
        hydrated.evidence.events > 0,
        "the healthy session must still hydrate, got {:?}",
        hydrated.evidence
    );

    fs::remove_dir_all(&root).ok();
}

/// The SQLite sweep, under both plans. Making the loader report a row it
/// cannot map — rather than calling the session absent — put that error in
/// front of a loop that took it out with `?`, so the first bad row ended the
/// sweep and every session after it in the store went unindexed. The legacy
/// tree's sweep has isolated per session since the round before; this is the
/// same regression one layout over.
fn one_unreadable_session_does_not_end_the_sqlite_sweep() {
    let root = temp_root("sqlite-sweep");

    for (tag, indexed) in [("per-session", true), ("single-pass", false)] {
        let store = root.join(tag).join(".local/share/opencode/opencode.db");
        build_sqlite_store(&store);
        let db = Connection::open(&store).unwrap();
        if !indexed {
            for index in [
                "session_time_updated_id_idx",
                "message_session_time_created_id_idx",
                "part_session_idx",
                "part_message_id_id_idx",
            ] {
                db.execute_batch(&format!("DROP INDEX IF EXISTS {index};"))
                    .unwrap();
            }
        }

        // Which session the sweep reaches first is SQLite's choice, not the
        // fixture's — it depends on which index it decides covers the
        // enumeration. So the roles are read out of the store with the same
        // query the loader enumerates with, rather than assumed: break the one
        // that is reached **first**, and require the one behind it to survive.
        // Assuming the order is how the first version of this test passed with
        // the defect in place.
        let order: Vec<String> = db
            .prepare("SELECT id FROM session WHERE id IS NOT NULL AND id <> ''")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<String>>>()
            .unwrap();
        assert!(
            order.len() >= 2,
            "[{tag}] the fixture must hold a session behind the bad one"
        );
        let (bad, survivor) = (order[0].clone(), order[1].clone());

        // SQLite columns are dynamically typed, so the store holds a value the
        // row mapping cannot take quite happily.
        db.execute("UPDATE session SET parent_id = x'ff' WHERE id = ?", [&bad])
            .unwrap();
        drop(db);

        let db_path = root.join(format!("{tag}.db"));
        let conn = open_db(&db_path).unwrap();
        let error = format!(
            "{:#}",
            sync_opencode_db(&conn, &store)
                .expect_err("the unreadable session must be reported, not swallowed")
        );
        assert!(
            !session_events(&conn, &survivor, Some("opencode"))
                .unwrap()
                .is_empty(),
            "[{tag}] the session behind the bad one must still be indexed"
        );
        assert!(
            error.contains(&bad),
            "[{tag}] the error must name the session that failed, got {error}"
        );
        assert!(
            session_events(&conn, &bad, Some("opencode"))
                .unwrap()
                .is_empty(),
            "[{tag}] and the bad one must not be indexed from a read that failed"
        );
        drop(conn);

        // Positive control: the same store with nothing wrong with it reports
        // nothing, so the assertions above are about the bad row and not about
        // this path always failing.
        let clean_store = root
            .join(format!("{tag}-clean"))
            .join(".local/share/opencode/opencode.db");
        build_sqlite_store(&clean_store);
        if !indexed {
            Connection::open(&clean_store)
                .unwrap()
                .execute_batch(
                    "DROP INDEX IF EXISTS session_time_updated_id_idx;
                     DROP INDEX IF EXISTS message_session_time_created_id_idx;
                     DROP INDEX IF EXISTS part_session_idx;
                     DROP INDEX IF EXISTS part_message_id_id_idx;",
                )
                .unwrap();
        }
        let conn = open_db(&root.join(format!("{tag}-clean.db"))).unwrap();
        sync_opencode_db(&conn, &clean_store)
            .unwrap_or_else(|error| panic!("[{tag}] a clean store must report nothing: {error:#}"));
        assert!(
            !session_events(&conn, &bad, Some("opencode"))
                .unwrap()
                .is_empty(),
            "[{tag}] and the session that was broken indexes fine when it is not"
        );
    }

    fs::remove_dir_all(&root).ok();
}

/// `sessions.last_assistant_text` is a catalog *excerpt* — it is read to show
/// a session in a list, and discovery caps every value it writes there at
/// `EXCERPT_MAX_CHARS`. The OpenCode normalizer stored the whole turn, so one
/// long answer put an unbounded string in a column every listing reads.
fn a_long_assistant_turn_is_excerpted_in_the_catalog_and_whole_in_its_event() {
    const CAP: usize = 4096;
    let root = temp_root("excerpt");
    let home = root.join("home");
    let tree = home.join(".local/share/opencode/storage");
    let long = "x".repeat(CAP * 3);
    write_json(
        &tree.join("session/global/ses_long.json"),
        r#"{"id":"ses_long","directory":"/tmp/project","time":{"created":1777400000000,"updated":1777400002000}}"#,
    );
    write_json(
        &tree.join("message/ses_long/msg_long_a1.json"),
        r#"{"id":"msg_long_a1","sessionID":"ses_long","role":"assistant","time":{"created":1777400001000},"providerID":"anthropic","modelID":"claude-opus-4-5","path":{"cwd":"/tmp/project"}}"#,
    );
    write_json(
        &tree.join("part/msg_long_a1/prt_long.json"),
        &format!(
            r#"{{"id":"prt_long","sessionID":"ses_long","messageID":"msg_long_a1","type":"text","text":"{long}"}}"#
        ),
    );
    use_layout(&home, None, Some(&tree));
    let db_path = root.join("history.db");
    sync_local_at(&db_path).unwrap();

    let stored: String = open_db(&db_path)
        .unwrap()
        .query_row(
            "SELECT last_assistant_text FROM sessions WHERE source='opencode' AND session_id=?",
            ["ses_long"],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        stored.chars().count(),
        CAP,
        "the catalog column is an excerpt and must be capped"
    );

    // Positive control: the cap is the catalog's, not the parser's. The event
    // still carries the whole turn, which is where a reader goes for it.
    let texts: Vec<String> =
        session_events(&open_db(&db_path).unwrap(), "ses_long", Some("opencode"))
            .unwrap()
            .into_iter()
            .filter_map(|event| event.text)
            .collect();
    assert_eq!(
        texts.iter().map(|text| text.chars().count()).max(),
        Some(long.chars().count()),
        "the event must keep the whole turn"
    );

    fs::remove_dir_all(&root).ok();
}

/// `sync --opencode` takes an exclusive lock and indexes one store. It asked
/// for SQLite outright instead of classifying, so a host that only has the
/// legacy `storage/` tree got discovery's catalog rows with no evidence behind
/// them -- and an `OPENCODE_DB` naming a directory was opened as SQLite and
/// failed rather than falling through to the tree beside it. `sync --local`
/// has classified through `OpencodeLayout::detect` since the layout gate
/// landed; this path was never brought along.
fn the_exclusive_sync_reads_whichever_layout_the_host_has() {
    let root = temp_root("exclusive-layout");

    // Control: a host that does have a SQLite store still indexes from it, so
    // the assertions below are about classification and not about this path
    // having started to work by accident.
    {
        let home = root.join("sqlite-home");
        let store = home.join(".local/share/opencode/opencode.db");
        build_sqlite_store(&store);
        use_layout(&home, Some(&store), None);
        let db_path = root.join("sqlite.db");
        assert!(sync_opencode_at(&db_path, &store, SyncOutput::Silent).unwrap());
        assert!(
            !session_events(
                &open_db(&db_path).unwrap(),
                "ses_sqlite_root",
                Some("opencode")
            )
            .unwrap()
            .is_empty(),
            "the SQLite host must still index"
        );
    }

    // A host with only the legacy tree. `OPENCODE_DB` names a path that is not
    // there, which is exactly what such a host has.
    {
        let home = root.join("tree-home");
        let tree = home.join(".local/share/opencode/storage");
        copy_tree(&fixtures().join("legacy-json-simple/storage"), &tree);
        let absent_db = home.join(".local/share/opencode/opencode.db");
        assert!(!absent_db.exists(), "precondition: no SQLite store here");
        use_layout(&home, Some(&absent_db), Some(&tree));
        let db_path = root.join("tree.db");
        assert!(sync_opencode_at(&db_path, &absent_db, SyncOutput::Silent).unwrap());
        assert!(
            !session_events(&open_db(&db_path).unwrap(), "ses_simple", Some("opencode"))
                .unwrap()
                .is_empty(),
            "a host with only the legacy tree must still have its evidence indexed"
        );
    }

    // `OPENCODE_DB` naming a directory: `exists()` is true for one, so the old
    // guard sent it to `Connection::open`. It must fall through to the tree
    // beside it instead of failing on a path that is not a database.
    {
        let home = root.join("dir-home");
        let tree = home.join(".local/share/opencode/storage");
        copy_tree(&fixtures().join("legacy-json-simple/storage"), &tree);
        let db_like = home.join(".local/share/opencode/opencode.db");
        fs::create_dir_all(&db_like).unwrap();
        assert!(
            db_like.exists() && !db_like.is_file(),
            "precondition: the fixture must be a directory that exists"
        );
        use_layout(&home, Some(&db_like), Some(&tree));
        let db_path = root.join("dir.db");
        let synced = sync_opencode_at(&db_path, &db_like, SyncOutput::Silent)
            .expect("a directory at OPENCODE_DB must not fail the sync as a bad database");
        assert!(synced);
        assert!(
            !session_events(&open_db(&db_path).unwrap(), "ses_simple", Some("opencode"))
                .unwrap()
                .is_empty(),
            "and the tree beside it must be what gets indexed"
        );
        // The same guard, asked of the store-level entry point directly: a
        // directory is answered like an absent store, not with an open error.
        let conn = open_db(&root.join("direct.db")).unwrap();
        assert_eq!(sync_opencode_db(&conn, &db_like).unwrap(), 0);
    }

    fs::remove_dir_all(&root).ok();
}

/// Discovery's shallow read takes the later of the session's `updated` and its
/// newest message; `normalize` took the newest message whenever one existed.
/// Two paths deciding one field differently, and hydration writes only through
/// `normalize` -- so for a session whose JSON is *ahead* of its messages, the
/// value hydration computes for the catalog is behind the one discovery
/// computed for the same session.
///
/// The column itself is defended: `upsert_session` merges it as
/// `MAX(COALESCE(sessions.last_activity_ms, excluded…), excluded…)`, so the
/// smaller value cannot land on top of the larger one. That defence is why the
/// divergence is invisible in an ordinary run, and it is also why a test that
/// merely hydrates and re-reads the column proves nothing -- the first version
/// of this one passed with the defect in place for exactly that reason. What
/// is under test is the value `normalize` produces, so the column is cleared
/// first and hydration is left as its only writer.
fn a_hydration_does_not_move_catalog_recency_backwards() {
    let root = temp_root("recency-backwards");
    let home = root.join("home");
    let tree = home.join(".local/share/opencode/storage");
    // `updated` is a minute past the only message's `created`.
    write_json(
        &tree.join("session/global/ses_ahead.json"),
        r#"{"id":"ses_ahead","directory":"/tmp/project","time":{"created":1777500000000,"updated":1777500060000}}"#,
    );
    write_json(
        &tree.join("message/ses_ahead/msg_ahead.json"),
        r#"{"id":"msg_ahead","sessionID":"ses_ahead","role":"user","time":{"created":1777500001000},"path":{"cwd":"/tmp/project"}}"#,
    );
    write_json(
        &tree.join("part/msg_ahead/prt_ahead.json"),
        r#"{"id":"prt_ahead","sessionID":"ses_ahead","messageID":"msg_ahead","type":"text","text":"a turn"}"#,
    );
    use_layout(&home, None, Some(&tree));
    let db_path = root.join("history.db");

    discover_sessions_scoped_at(&db_path, &opencode_only()).unwrap();
    assert_eq!(
        catalog_row(&db_path, "ses_ahead").1,
        Some(1777500060000),
        "precondition: discovery catalogs the later of the two"
    );

    // Clear it, so what hydration writes is what the column holds rather than
    // what the merge kept.
    let clear = |db_path: &Path| {
        open_db(db_path)
            .unwrap()
            .execute(
                "UPDATE sessions SET last_activity_ms = NULL \
                 WHERE source='opencode' AND session_id='ses_ahead'",
                [],
            )
            .unwrap();
        assert_eq!(catalog_row(db_path, "ses_ahead").1, None);
    };
    clear(&db_path);

    let hydrated = hydrate(&db_path, "ses_ahead");
    assert!(
        hydrated.evidence.events > 0,
        "precondition: the hydration must actually have done its work, got {:?}",
        hydrated.evidence
    );
    assert_eq!(
        catalog_row(&db_path, "ses_ahead").1,
        Some(1777500060000),
        "hydration must compute the same recency discovery does, not a value \
         behind it that only the column's merge hides"
    );

    // Positive control for the other direction, so this is not a rule that
    // simply prefers the session file: the usual OpenCode shape is a session
    // JSON that lags its own newest message, and there the message must win.
    write_json(
        &tree.join("message/ses_ahead/msg_later.json"),
        r#"{"id":"msg_later","sessionID":"ses_ahead","role":"user","time":{"created":1777500120000},"path":{"cwd":"/tmp/project"}}"#,
    );
    write_json(
        &tree.join("part/msg_later/prt_later.json"),
        r#"{"id":"prt_later","sessionID":"ses_ahead","messageID":"msg_later","type":"text","text":"a later turn"}"#,
    );
    clear(&db_path);
    let hydrated = hydrate(&db_path, "ses_ahead");
    assert!(hydrated.evidence.events > 1);
    assert_eq!(
        catalog_row(&db_path, "ses_ahead").1,
        Some(1777500120000),
        "a message past the session's `updated` must still advance it"
    );

    // And the merge that hid all this is pinned, because it is the only reason
    // the divergence never surfaced: with a value already in the column, no
    // hydration can lower it.
    open_db(&db_path)
        .unwrap()
        .execute(
            "UPDATE sessions SET last_activity_ms = 1888000000000 \
             WHERE source='opencode' AND session_id='ses_ahead'",
            [],
        )
        .unwrap();
    clear_opencode_evidence(&db_path);
    hydrate(&db_path, "ses_ahead");
    assert_eq!(
        catalog_row(&db_path, "ses_ahead").1,
        Some(1888000000000),
        "the catalog merge keeps the later value, whatever a writer offers"
    );

    fs::remove_dir_all(&root).ok();
}
