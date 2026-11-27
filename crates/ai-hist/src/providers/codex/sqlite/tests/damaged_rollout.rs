use super::*;

#[test]
fn damaged_rollouts_leave_selected_and_related_sqlite_histories_available() {
    let dir = tempfile::tempdir().unwrap();
    let roots = crate::ProviderRoots::for_codex_test(dir.path());
    install_fixture(dir.path(), "subagent-edge");
    let state = dir.path().join("state_5.sqlite");
    let db = Connection::open(&state).unwrap();
    let sessions = dir.path().join("sessions");
    std::fs::create_dir_all(&sessions).unwrap();
    for id in ["sqlite-parent", "sqlite-child"] {
        let path = sessions.join(format!("{id}.jsonl"));
        let compressed = crate::ingest::transcript_file::compressed_path(&path);
        std::fs::write(compressed, b"invalid zstd frame").unwrap();
        db.execute(
            "UPDATE threads SET rollout_path=?1 WHERE id=?2",
            params![path.to_string_lossy(), id],
        )
        .unwrap();
        assert_eq!(restored_rollout(&roots, &state, id).unwrap(), None);
        assert_eq!(
            current_locator(&roots, id, &path.to_string_lossy()).unwrap(),
            Some(locator(&state, id))
        );
    }
    let capture = decode(&capture_related(&roots, &state, "sqlite-parent").unwrap()).unwrap();
    assert!(!capture.items.is_empty());
    assert!(!capture.related_incomplete);
    assert!(capture
        .related
        .iter()
        .any(|child| child.thread.id == "sqlite-child" && !child.items.is_empty()));
}
