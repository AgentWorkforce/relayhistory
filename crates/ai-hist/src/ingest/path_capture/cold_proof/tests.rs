use super::*;
use crate::{ProviderRoots, SessionStore, Source, StoreOptions, SyncOptions};

fn fixture(count: usize) -> (tempfile::TempDir, SessionStore, Vec<PathBuf>) {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    let project = home.join(".claude/projects/p");
    fs::create_dir_all(&project).unwrap();
    let paths: Vec<_> = (0..count)
        .map(|n| project.join(format!("s{n}.jsonl")))
        .collect();
    for (n, path) in paths.iter().enumerate() {
        fs::write(path, line(&format!("s{n}"), "first")).unwrap();
    }
    let store = SessionStore::open(
        StoreOptions::default()
            .db_path(temp.path().join("history.db"))
            .roots(ProviderRoots::from_home(
                home.clone(),
                home.join("opencode.db"),
            )),
    )
    .unwrap();
    store
        .sync(SyncOptions::default().sources([Source::Claude]))
        .unwrap();
    (temp, store, paths)
}
fn line(id: &str, text: &str) -> String {
    format!(
        "{}\n",
        json!({"sessionId":id,"uuid":"m","type":"user","timestamp":"2026-10-20T12:00:00Z","message":{"role":"user","content":text}})
    )
}
fn round(store: &SessionStore, source: Source) {
    let mut round = store.reconcile().unwrap().sources([source]);
    for _ in 0..1000 {
        if round.step(64, 8, SyncOptions::default()).unwrap().complete {
            return;
        }
    }
    panic!("round did not complete");
}
fn checkpoints(store: &SessionStore) -> i64 {
    open_db(store.db_path())
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM session_hydration_checkpoints",
            [],
            |r| r.get(0),
        )
        .unwrap()
}
fn edit_state(store: &SessionStore, edit: impl FnOnce(&mut Map<String, Value>)) {
    let path = store.db_path().parent().unwrap().join(".sync-state.json");
    let mut state = load_sync_state(&path).unwrap();
    edit(&mut state);
    fs::write(path, serde_json::to_vec(&state).unwrap()).unwrap();
}

#[test]
fn unchanged_native_archive_needs_no_hydration_and_loads_47k_state_only_once_per_round() {
    let (_temp, store, _paths) = fixture(40);
    edit_state(&store, |s| {
        s.insert(
            "unrelated".into(),
            Value::Object(
                (0..47000)
                    .map(|i| (i.to_string(), json!("archived")))
                    .collect(),
            ),
        );
    });
    let before = store.head_revision().unwrap();
    state_work::take();
    round(&store, Source::Claude);
    assert_eq!(
        checkpoints(&store),
        0,
        "cold archive must reuse native proofs instead of rehydrating"
    );
    assert_eq!(store.head_revision().unwrap(), before);
    assert_eq!(
        state_work::take(),
        (1, 0),
        "one readonly load, no per-step global state work"
    );
}

#[test]
fn missing_native_evidence_and_present_marker_shortfall_fall_back_to_acquisition() {
    for marker in [false, true] {
        let (_temp, store, _paths) = fixture(1);
        let conn = open_db(store.db_path()).unwrap();
        if marker {
            let mut extra = line("s0", "second");
            extra = extra.replace("\"uuid\":\"m\"", "\"uuid\":\"n\"");
            use std::io::Write;
            fs::OpenOptions::new()
                .append(true)
                .open(&_paths[0])
                .unwrap()
                .write_all(extra.as_bytes())
                .unwrap();
            store
                .sync(SyncOptions::default().sources([Source::Claude]))
                .unwrap();
            let generation = destination_generation(&conn).unwrap();
            edit_state(&store, |s| {
                s.insert(DESTINATION_GENERATION_KEY.into(), generation.into());
            });
            conn.execute(
                "DELETE FROM session_events WHERE source='claude' AND text='first'",
                [],
            )
            .unwrap();
        } else {
            conn.execute("DELETE FROM session_events WHERE source='claude'", [])
                .unwrap();
        }
        round(&store, Source::Claude);
        assert_eq!(checkpoints(&store), 1);
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM session_events WHERE source='claude'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            if marker { 2 } else { 1 }
        );
    }
}

#[test]
fn native_parser_generation_reset_and_same_stat_rewrite_cannot_skip_cold_capture() {
    for rewrite in [false, true] {
        let (_temp, store, paths) = fixture(1);
        if rewrite {
            let stamp = fs::metadata(&paths[0]).unwrap().modified().unwrap();
            fs::write(&paths[0], line("s0", "other")).unwrap();
            fs::File::options()
                .write(true)
                .open(&paths[0])
                .unwrap()
                .set_modified(stamp)
                .unwrap();
        } else {
            edit_state(&store, |s| {
                s.insert(CLAUDE_RAW_MESSAGE_FACTS_KEY.into(), json!(0));
            });
        }
        round(&store, Source::Claude);
        assert_eq!(checkpoints(&store), 1);
        if rewrite {
            assert_eq!(
                open_db(store.db_path())
                    .unwrap()
                    .query_row(
                        "SELECT text FROM session_events WHERE source='claude'",
                        [],
                        |r| r.get::<_, String>(0)
                    )
                    .unwrap(),
                "other"
            );
        }
    }
}

#[test]
fn a_replaced_native_transcript_and_stale_cursor_document_require_acquisition() {
    for change in ["replace", "stale", "truncate"] {
        let (_temp, store, paths) = fixture(1);
        if change == "replace" {
            let replacement = paths[0].with_extension("new");
            fs::write(&replacement, line("s0", "other")).unwrap();
            fs::rename(replacement, &paths[0]).unwrap();
        } else if change == "truncate" {
            fs::write(&paths[0], line("s0", "one")).unwrap();
        } else {
            open_db(store.db_path()).unwrap().execute("UPDATE transcript_cursors SET parser_state_json=json_set(parser_state_json,'$.v',0)",[]).unwrap();
        }
        round(&store, Source::Claude);
        assert_eq!(checkpoints(&store), 1);
    }
}

#[test]
fn changed_round_state_is_not_reloaded_or_trusted_and_context_unwinds() {
    let (_temp, store, _paths) = fixture(16);
    let mut walk = store.reconcile().unwrap().sources([Source::Claude]);
    walk.step(64, 8, SyncOptions::default()).unwrap();
    assert_eq!(checkpoints(&store), 0);
    edit_state(&store, |s| {
        s.insert(CLAUDE_RAW_MESSAGE_FACTS_KEY.into(), json!(0));
    });
    walk.step(64, 8, SyncOptions::default()).unwrap();
    assert!(checkpoints(&store) > 0);
    let mut cache = ColdProof::default();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        with_cold_proof(&mut cache, || panic!("unwind"))
    }));
    assert!(panic.is_err());
    assert!(CURRENT.with(|slot| slot.borrow().is_none()));
}

#[test]
fn cold_native_proofs_do_not_change_explicit_hot_acquisition_or_selected_inventory() {
    let (_temp, store, paths) = fixture(1);
    let mut inventory = store.reconcile().unwrap().sources([Source::Claude]);
    inventory.discover_step(64, 8, Default::default()).unwrap();
    assert_eq!(checkpoints(&store), 0);
    state_work::take();
    store
        .sync(
            SyncOptions::default()
                .sources([Source::Claude])
                .paths(paths)
                .force(true),
        )
        .unwrap();
    assert_eq!(checkpoints(&store), 1);
    assert_eq!(state_work::take(), (0, 0));
}

#[test]
fn unchanged_native_codex_rollout_reuses_its_global_proof() {
    let (_temp, store, _paths) = fixture(0);
    let path = store
        .roots()
        .codex
        .join("sessions/2026/10/20/rollout-test.jsonl");
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let meta = json!({"timestamp":"2026-10-20T12:00:00Z","type":"session_meta","payload":{"id":"codex-one","cwd":"/tmp/project","source":"cli"}});
    let event = json!({"timestamp":"2026-10-20T12:00:01Z","type":"event_msg","payload":{"type":"agent_message","message":"native"}});
    let complete = json!({"timestamp":"2026-10-20T12:00:02Z","type":"event_msg","payload":{"type":"task_complete"}});
    fs::write(&path, format!("{meta}\n{event}\n{complete}\n")).unwrap();
    store
        .sync(SyncOptions::default().sources([Source::Codex]))
        .unwrap();
    let before = store.head_revision().unwrap();
    state_work::take();
    round(&store, Source::Codex);
    assert_eq!(checkpoints(&store), 0);
    assert_eq!(store.head_revision().unwrap(), before);
    assert_eq!(state_work::take(), (1, 0));
}

#[cfg(unix)]
#[test]
fn trusted_change_time_rejects_a_middle_rewrite_outside_native_prefix_windows() {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            transcript_cursor::set_filesystem_keeps_change_time_for_test(None);
            transcript_cursor::set_settle_window_for_test(None);
        }
    }
    transcript_cursor::set_filesystem_keeps_change_time_for_test(Some(true));
    transcript_cursor::set_settle_window_for_test(Some(0));
    let _reset = Reset;
    let (_temp, store, paths) = fixture(1);
    let mut text = String::new();
    for (n, marker) in ["head", "middle-before", "tail"].iter().enumerate() {
        let mut record = line("s0", &format!("{marker}{}", "x".repeat(100 * 1024)));
        record = record.replace("\"uuid\":\"m\"", &format!("\"uuid\":\"m{n}\""));
        text.push_str(&record);
    }
    fs::write(&paths[0], &text).unwrap();
    store
        .sync(SyncOptions::default().sources([Source::Claude]))
        .unwrap();
    let conn = open_db(store.db_path()).unwrap();
    assert!(transcript_cursor::transcript_unchanged(&conn, "claude", &paths[0]).unwrap());
    let cursor = transcript_cursor::load_cursor(
        &conn,
        &transcript_cursor::CursorKey::Locator {
            source: "claude",
            locator: &paths[0].to_string_lossy(),
        },
    )
    .unwrap();
    let settled = cursor.settled.unwrap();
    let modified = fs::metadata(&paths[0]).unwrap().modified().unwrap();
    let middle = text.find("middle-before").unwrap();
    assert!(middle > 64 * 1024 && middle + 13 < text.len() - 64 * 1024);
    let rewrite = text.replacen("middle-before", "middle-after_", 1);
    assert_eq!(text.len(), rewrite.len());
    fs::write(&paths[0], rewrite).unwrap();
    fs::File::options()
        .write(true)
        .open(&paths[0])
        .unwrap()
        .set_modified(modified)
        .unwrap();
    assert_ne!(
        transcript_cursor::change_time_ns(&fs::metadata(&paths[0]).unwrap()),
        Some(settled.ctime_ns)
    );
    round(&store, Source::Claude);
    assert_eq!(checkpoints(&store), 1);
    let captured: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM session_events WHERE text LIKE 'middle-after_%')",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(
        captured,
        "a changed trusted ctime must force acquisition despite unchanged head/tail windows"
    );
}
