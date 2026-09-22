//! Real local acquisition under capture-capacity failure. One test owns HOME
//! for this process; no operator files, credentials, or transports are used.
use ai_hist::{
    discover_sessions_scoped_at, hydrate_session_at, sync_scoped_at, DiscoverOptions,
    HydrateSessionOptions, SessionScope,
};
use relayhistory_plugin::delivery::*;
use serde_json::{json, Value};
use std::{fs, path::Path};

fn write(path: &Path, text: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, text).unwrap();
}
fn config(instance: &str) -> DeliveryJobConfig {
    DeliveryJobConfig {
        destination_id: "fixture".into(),
        instance_id: instance.into(),
        account_id: "account".into(),
        mapping_version: "1".into(),
        selection: ExportSelection {
            all_sources: true,
            kinds: SUPPORTED_KINDS.iter().map(|s| (*s).into()).collect(),
            ..ExportSelection::default()
        },
        limits: DeliveryLimits::default(),
    }
}
fn checkpoint(home: &Path) -> Value {
    fs::read(home.join(".sync-state.json"))
        .ok()
        .map(|bytes| serde_json::from_slice(&bytes).unwrap())
        .unwrap_or_else(|| json!({}))
}
fn sync_failure(home: &Path) {
    let folder = home.join("sync");
    fs::create_dir_all(&folder).unwrap();
    let path = folder.join("history.db");
    write(&home.join(".claude/history.jsonl"),"{\"display\":\"claude retained prompt\",\"timestamp\":1000,\"sessionId\":\"claude-sync\"}\n");
    write(
        &home.join(".codex/history.jsonl"),
        "{\"text\":\"codex retained prompt\",\"ts\":2,\"session_id\":\"codex-sync\"}\n",
    );
    let conn = open_db(&path).unwrap();
    create_job(&conn, &config("sync"), 0).unwrap();
    let used = retained_bytes(&conn).unwrap().0;
    set_retention_limit(&conn, used).unwrap();
    let result = sync_scoped_at(&path, SessionScope::Local);
    let state = checkpoint(&folder);
    assert!(
        state.get("claude").is_none(),
        "failed Claude bytes must not be checkpointed: {state}"
    );
    assert!(
        state.get("codex").is_none(),
        "failed Codex bytes must not be checkpointed: {state}"
    );
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM history", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0);
    // Backpressure may reclaim consumed revisions before the pass stops, so
    // the failed pass never retains more than it found.
    assert!(retained_bytes(&conn).unwrap().0 <= used);
    assert!(
        result.is_err(),
        "capture capacity failure must be visible even when other providers are absent"
    );
    assert!(is_retention_limit(&result.unwrap_err()));
    set_retention_limit(&conn, 10_000_000).unwrap();
    drop(conn);
    sync_scoped_at(&path, SessionScope::Local).unwrap();
    let conn = open_db(&path).unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM history", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 2);
    let journal: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM delivery_journal WHERE kind='history'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(journal, 2);
    let state = checkpoint(&folder);
    assert!(state["claude"]["offset"].as_u64().unwrap() > 0);
    assert!(state["codex"]["offset"].as_u64().unwrap() > 0);
    sync_scoped_at(&path, SessionScope::Local).unwrap();
    let repeated: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM delivery_journal WHERE kind='history'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        repeated, journal,
        "unchanged retry must not create new history revisions"
    );
}
fn hydration_failure(home: &Path) {
    let folder = home.join("hydration");
    fs::create_dir_all(&folder).unwrap();
    let path = folder.join("history.db");
    let transcript = home.join(".claude/projects/app/hydrate.jsonl");
    let initial=concat!(
        "{\"sessionId\":\"hydrate\",\"uuid\":\"u1\",\"cwd\":\"/fixture\",\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"hello\"},\"timestamp\":\"2026-09-01T01:00:00Z\"}\n",
        "{\"sessionId\":\"hydrate\",\"uuid\":\"a1\",\"type\":\"assistant\",\"message\":{\"role\":\"assistant\",\"content\":\"answer\"},\"timestamp\":\"2026-09-01T01:00:01Z\"}\n"
    );
    write(&transcript, initial);
    discover_sessions_scoped_at(
        &path,
        &DiscoverOptions {
            scope: SessionScope::Local,
            sources: vec!["claude".into()],
            limit: None,
        },
    )
    .unwrap();
    let conn = open_db(&path).unwrap();
    create_job(&conn, &config("hydrate"), 0).unwrap();
    set_retention_limit(&conn, retained_bytes(&conn).unwrap().0).unwrap();
    let options = HydrateSessionOptions {
        source: "claude".into(),
        session_id: "hydrate".into(),
        scope: SessionScope::Local,
        include_related: true,
    };
    let failed = hydrate_session_at(&path, &options).unwrap_err();
    assert!(is_retention_limit(&failed));
    let state:(i64,i64)=conn.query_row("SELECT (SELECT COUNT(*) FROM session_events),(SELECT COUNT(*) FROM session_hydration_checkpoints)",[],|r|Ok((r.get(0)?,r.get(1)?))).unwrap();
    assert_eq!(state, (0, 0));
    set_retention_limit(&conn, 10_000_000).unwrap();
    let first = hydrate_session_at(&path, &options).unwrap();
    assert_eq!(first.evidence.events, 2);
    let before:(String,i64)=conn.query_row("SELECT source_stamp,source_bytes FROM session_hydration_checkpoints WHERE session_id='hydrate'",[],|r|Ok((r.get(0)?,r.get(1)?))).unwrap();
    write(&transcript,&format!("{initial}{{\"sessionId\":\"hydrate\",\"uuid\":\"a2\",\"type\":\"assistant\",\"message\":{{\"role\":\"assistant\",\"content\":\"later revision\"}},\"timestamp\":\"2026-09-01T01:00:02Z\"}}\n"));
    set_retention_limit(&conn, retained_bytes(&conn).unwrap().0).unwrap();
    assert!(is_retention_limit(
        &hydrate_session_at(&path, &options).unwrap_err()
    ));
    let after:(String,i64)=conn.query_row("SELECT source_stamp,source_bytes FROM session_hydration_checkpoints WHERE session_id='hydrate'",[],|r|Ok((r.get(0)?,r.get(1)?))).unwrap();
    assert_eq!(before, after);
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM session_events", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 2);
    set_retention_limit(&conn, 10_000_000).unwrap();
    drop(conn);
    let final_result = hydrate_session_at(&path, &options).unwrap();
    assert_eq!(final_result.evidence.events, 3);
    assert_ne!(
        final_result.indexed_through.source_stamp.as_ref().unwrap(),
        &before.0
    );
    let conn = open_db(&path).unwrap();
    let journal:i64=conn.query_row("SELECT COUNT(*) FROM delivery_journal WHERE kind='session_event' AND operation='upsert'",[],|r|r.get(0)).unwrap();
    assert_eq!(journal, 3);
}
fn trajectory_failure(home: &Path) {
    // Leave this as the only present provider so generic partial-source success
    // cannot hide its previously swallowed upsert failure.
    fs::remove_dir_all(home.join(".claude")).unwrap();
    fs::remove_dir_all(home.join(".codex")).unwrap();
    let root = home.join("trajectories");
    write(
        &root.join("run.json"),
        r#"{"id":"run","task":{"title":"retained"},"decisions":[],"retrospective":{"summary":"done"}}"#,
    );
    std::env::set_var("TRAJECTORY_ROOT", &root);
    let folder = home.join("trajectory-db");
    fs::create_dir_all(&folder).unwrap();
    let path = folder.join("history.db");
    let conn = open_db(&path).unwrap();
    create_job(&conn, &config("trajectory"), 0).unwrap();
    set_retention_limit(&conn, retained_bytes(&conn).unwrap().0).unwrap();
    let failed = sync_scoped_at(&path, SessionScope::Local).unwrap_err();
    assert!(is_retention_limit(&failed));
    assert!(checkpoint(&folder).get("trajectory").is_none());
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM trajectories", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 0);
    set_retention_limit(&conn, 10_000_000).unwrap();
    sync_scoped_at(&path, SessionScope::Local).unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM delivery_journal WHERE kind='trajectory'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
    assert!(checkpoint(&folder)["trajectory"]
        .as_object()
        .is_some_and(|map| map.len() == 1));
}

#[test]
fn local_sync_and_hydration_do_not_checkpoint_uncaptured_history() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path();
    std::env::set_var("HOME", home);
    std::env::set_var("USERPROFILE", home);
    std::env::set_var("XDG_DATA_HOME", home.join("xdg"));
    std::env::set_var("OPENCODE_DB", home.join("missing-opencode.db"));
    std::env::remove_var("CLAUDE_CONFIG_DIR");
    std::env::remove_var("CODEX_HOME");
    std::env::remove_var("GROK_HOME");
    std::env::set_var("TRAJECTORY_ROOT", home.join("missing-trajectories"));
    std::env::remove_var("AI_HIST_DB");
    sync_failure(home);
    hydration_failure(home);
    trajectory_failure(home);
}
