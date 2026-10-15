use super::*;

#[test]
fn a_pruned_rollout_is_read_again_after_it_was_restored() {
    let dir = tempfile::tempdir().unwrap();
    let codex = dir.path().join(".codex");
    std::fs::create_dir_all(&codex).unwrap();
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/codex/sqlite");
    for (script, database) in [
        ("rollout-primary-state.sql", "state_5.sqlite"),
        ("rollout-primary-history.sql", "thread_history_1.sqlite"),
    ] {
        let sql = std::fs::read_to_string(fixtures.join(script))
            .unwrap()
            .replace("{{CODEX_HOME}}", &codex.to_string_lossy());
        Connection::open(codex.join(database))
            .unwrap()
            .execute_batch(&sql)
            .unwrap();
    }
    let rollout = codex.join("sessions/2026/04/20/rollout-rollout-primary.jsonl");
    std::fs::create_dir_all(rollout.parent().unwrap()).unwrap();
    std::fs::write(&rollout, "complete rollout").unwrap();
    let home = dir.path().to_path_buf();
    let mut roots = crate::ProviderRoots::from_home(home, dir.path().join("opencode.db"));
    roots.codex = codex.clone();
    assert!(enumerate(&codex, &roots).unwrap().0.is_empty());

    let conn = crate::open_db(&dir.path().join("ai-history.db")).unwrap();
    let mut state = serde_json::Map::new();
    let repairs = crate::ingest::SweepRepairs::default();
    assert!(sync(&conn, &mut state, &codex, &repairs)
        .unwrap()
        .1
        .is_empty());

    std::fs::remove_file(&rollout).unwrap();
    assert_eq!(
        sync(&conn, &mut state, &codex, &repairs).unwrap().1,
        vec!["sqlite-rollout-primary".to_string()]
    );
    std::fs::write(&rollout, "complete rollout").unwrap();
    assert!(sync(&conn, &mut state, &codex, &repairs)
        .unwrap()
        .1
        .is_empty());

    std::fs::remove_file(&rollout).unwrap();
    assert_eq!(
        sync(&conn, &mut state, &codex, &repairs).unwrap().1,
        vec!["sqlite-rollout-primary".to_string()]
    );
}
