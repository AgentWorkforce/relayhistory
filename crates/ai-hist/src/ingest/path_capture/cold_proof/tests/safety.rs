use super::*;

fn options(paths: &[PathBuf]) -> SyncOptions {
    let mut options = SyncOptions::default().sources([Source::Claude]);
    options.paths = Some(paths.to_vec());
    options
}

#[test]
fn safety_capture_reuses_native_proof_without_adopting_cursors_or_advancing_walks() {
    let (_temp, store, paths) = fixture(40);
    edit_state(&store, |state| {
        state.insert(
            "unrelated".into(),
            Value::Object(
                (0..47000)
                    .map(|n| (n.to_string(), json!("archive")))
                    .collect(),
            ),
        );
    });
    let before = store.head_revision().unwrap();
    state_work::take();
    let mut proof = store.reconcile().unwrap().sources([Source::Claude]);
    for batch in paths.chunks(8) {
        assert!(proof
            .capture_paths(options(batch))
            .unwrap()
            .changed
            .is_empty());
    }
    assert_eq!(checkpoints(&store), 0);
    assert_eq!(store.head_revision().unwrap(), before);
    assert_eq!(state_work::take(), (1, 0));
    let mut walked = 0;
    loop {
        let step = proof.step(64, 8, SyncOptions::default()).unwrap();
        walked += step.paths;
        if step.complete {
            break;
        }
    }
    assert_eq!(
        walked, 40,
        "safety capture must not consume cold directory positions"
    );
    assert_eq!(checkpoints(&store), 0);
    assert_eq!(state_work::take(), (0, 0));
}

#[test]
fn safety_capture_preserves_forced_requests_and_rejects_unrestricted_paths() {
    let (_temp, store, paths) = fixture(1);
    let mut proof = store.reconcile().unwrap().sources([Source::Claude]);
    assert!(matches!(
        proof.capture_paths(SyncOptions::default()),
        Err(crate::Error::InvalidArgument(_))
    ));
    state_work::take();
    assert!(proof
        .capture_paths(options(&[]))
        .unwrap()
        .changed
        .is_empty());
    assert_eq!(state_work::take(), (0, 0));
    let mut forced = options(&paths);
    forced.force = true;
    proof.capture_paths(forced).unwrap();
    assert_eq!(
        checkpoints(&store),
        1,
        "explicit force must still perform ordinary acquisition"
    );
    assert_eq!(
        state_work::take(),
        (0, 0),
        "forced exact acquisition cannot read global proof state"
    );
}

#[test]
fn safety_capture_rejects_stale_generation_and_retains_completed_source_filters() {
    let (_temp, store, paths) = fixture(1);
    edit_state(&store, |state| {
        state.insert(CLAUDE_RAW_MESSAGE_FACTS_KEY.into(), json!(0));
    });
    let conn = open_db(store.db_path()).unwrap();
    conn.execute(
        "UPDATE session_events SET raw_facts_version=0 WHERE source='claude'",
        [],
    )
    .unwrap();
    let mut proof = store.reconcile().unwrap().sources([Source::Claude]);
    proof.capture_paths(options(&paths)).unwrap();
    assert!(
        conn.query_row(
            "SELECT min(raw_facts_version)>0 FROM session_events WHERE source='claude'",
            [],
            |r| r.get::<_, bool>(0)
        )
        .unwrap(),
        "a stale generation must restore actual provider facts"
    );
    assert_eq!(
        checkpoints(&store),
        1,
        "a stale generation cannot authorize native reuse"
    );
    while !proof.step(64, 8, SyncOptions::default()).unwrap().complete {}
    let codex = store.roots().codex.join("sessions/rollout-denied.jsonl");
    fs::create_dir_all(codex.parent().unwrap()).unwrap();
    fs::write(
        &codex,
        "{\"type\":\"session_meta\",\"payload\":{\"id\":\"denied\"}}\n",
    )
    .unwrap();
    let before = store.head_revision().unwrap();
    let mut denied = options(&paths);
    denied.paths = Some(vec![codex.clone()]);
    denied.sources = Some(vec![Source::Codex]);
    denied.force = true;
    assert!(proof.capture_paths(denied).unwrap().changed.is_empty());
    assert_eq!(
        store.head_revision().unwrap(),
        before,
        "completed walks retain their source allowlist"
    );
    let mut empty = store
        .reconcile()
        .unwrap()
        .sources([Source::Claude])
        .sources([Source::Codex]);
    let mut denied = options(&[codex]);
    denied.sources = Some(vec![Source::Codex]);
    assert!(empty.capture_paths(denied).unwrap().changed.is_empty());
}

#[test]
fn safety_capture_reads_a_real_first_append_and_rejects_destination_shortfalls() {
    let (_temp, store, paths) = fixture(1);
    let mut proof = store.reconcile().unwrap().sources([Source::Claude]);
    proof.capture_paths(options(&paths)).unwrap();
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&paths[0])
        .unwrap();
    use std::io::Write;
    writeln!(file,"{}",json!({"sessionId":"s0","uuid":"new","type":"user","message":{"role":"user","content":"open-writer-append"}})).unwrap();
    file.flush().unwrap();
    proof.capture_paths(options(&paths)).unwrap();
    let conn = open_db(store.db_path()).unwrap();
    let captured: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM session_events WHERE text='open-writer-append')",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(
        captured,
        "an unchanged native proof cannot hide an append held open"
    );
    assert_eq!(checkpoints(&store), 1);
    drop(file);

    let (_temp, store, paths) = fixture(1);
    let conn = open_db(store.db_path()).unwrap();
    conn.execute(
        "DELETE FROM session_events WHERE source='claude' AND session_id='s0'",
        [],
    )
    .unwrap();
    let mut proof = store.reconcile().unwrap().sources([Source::Claude]);
    proof.capture_paths(options(&paths)).unwrap();
    let events: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM session_events WHERE source='claude' AND session_id='s0'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        events, 1,
        "missing native evidence must fall back to actual repair"
    );
    assert_eq!(checkpoints(&store), 1);
}

#[test]
fn safety_native_proof_checks_a_present_destination_marker_before_skipping() {
    let (_temp, store, paths) = fixture(1);
    use std::io::Write;
    fs::OpenOptions::new()
        .append(true)
        .open(&paths[0])
        .unwrap()
        .write_all(
            line("s0", "second")
                .replace("\"uuid\":\"m\"", "\"uuid\":\"n\"")
                .as_bytes(),
        )
        .unwrap();
    store
        .sync(SyncOptions::default().sources([Source::Claude]))
        .unwrap();
    let conn = open_db(store.db_path()).unwrap();
    let generation = destination_generation(&conn).unwrap();
    edit_state(&store, |state| {
        state.insert(DESTINATION_GENERATION_KEY.into(), generation.into());
    });
    conn.execute(
        "DELETE FROM session_events WHERE source='claude' AND text='first'",
        [],
    )
    .unwrap();
    let mut proof = store.reconcile().unwrap().sources([Source::Claude]);
    proof.capture_paths(options(&paths)).unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM session_events WHERE source='claude'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        count, 2,
        "a nonempty but incomplete destination cannot authorize native reuse"
    );
    assert_eq!(checkpoints(&store), 1);
}

#[test]
fn retained_safety_context_obeys_sync_lock_and_stop_before_consuming_proof() {
    let (_temp, store, paths) = fixture(1);
    let mut proof = store.reconcile().unwrap().sources([Source::Claude]);
    proof.capture_paths(options(&paths)).unwrap();
    let held = crate::ingest::try_acquire_sync_lock(store.db_path())
        .unwrap()
        .unwrap();
    assert!(matches!(
        proof.capture_paths(options(&paths).lock_timeout_ms(0)),
        Err(crate::Error::SyncLocked { .. })
    ));
    let token = crate::StopToken::new();
    token.stop();
    assert!(matches!(
        proof.capture_paths(options(&paths).lock_timeout_ms(30000).stop(token)),
        Err(crate::Error::Cancelled(_))
    ));
    drop(held);
    assert!(proof
        .capture_paths(options(&paths))
        .unwrap()
        .changed
        .is_empty());
    assert_eq!(checkpoints(&store), 0);
}
