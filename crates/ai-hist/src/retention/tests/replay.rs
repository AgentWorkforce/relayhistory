use super::*;
use crate::{HydrateOptions, ProviderRoots, SessionRef, Source};
use std::path::Path;

fn isolated_store(home: &Path) -> (SessionStore, Connection) {
    let roots = ProviderRoots::from_home(home.to_path_buf(), home.join("opencode.db"));
    let store = SessionStore::open(
        StoreOptions::default()
            .db_path(home.join("history.db"))
            .roots(roots),
    )
    .unwrap();
    let conn = crate::store::open_db(store.db_path()).unwrap();
    (store, conn)
}
fn expire(store: &SessionStore) {
    acknowledge(store, "cloud");
    let opts = RetentionOptions {
        min_interval_ms: 0,
        ..Default::default()
    };
    for _ in 0..1000 {
        let batch = maintenance::run(store, &opts, crate::now_ms() + 365 * DAY).unwrap();
        assert!(!batch.busy);
        if batch.cycle_complete {
            return;
        }
    }
    panic!("retention did not finish");
}
fn copy_tree(from: &Path, to: &Path, rename: bool) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.path().is_dir() {
            copy_tree(&entry.path(), &target, rename);
        } else {
            let data = std::fs::read_to_string(entry.path()).unwrap();
            std::fs::write(
                target,
                if rename {
                    data.replace("grok-uni-0001", "grok-uni-0003")
                } else {
                    data
                },
            )
            .unwrap();
        }
    }
}
fn usages(conn: &Connection, id: &str) -> i64 {
    conn.query_row("SELECT count(*) FROM session_events WHERE source='grok' AND session_id=?1 AND raw_kind='unified_log_usage'",[id],|r|r.get(0)).unwrap()
}
#[test]
fn grok_hydration_recovers_evicted_usage_without_rewinding_other_sessions() {
    let dir = tempfile::tempdir().unwrap();
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/grok/unified-usage");
    copy_tree(&fixture, dir.path(), false);
    let sessions = dir.path().join(".grok/sessions/%2Ftmp%2Fusage");
    copy_tree(
        &sessions.join("grok-uni-0001"),
        &sessions.join("grok-uni-0003"),
        true,
    );
    let log = dir.path().join(".grok/logs/unified.jsonl");
    let original = std::fs::read_to_string(&log).unwrap();
    std::fs::write(
        &log,
        format!(
            "{original}{}",
            original.replace("grok-uni-0001", "grok-uni-0003")
        ),
    )
    .unwrap();
    let (store, conn) = isolated_store(dir.path());
    store
        .discover(crate::DiscoveryOptions::default().sources([Source::Grok]))
        .unwrap();
    let target = SessionRef::id(Source::Grok, "grok-uni-0001");
    let other = SessionRef::id(Source::Grok, "grok-uni-0003");
    store.hydrate(&target, HydrateOptions::default()).unwrap();
    store.hydrate(&other, HydrateOptions::default()).unwrap();
    let before = usages(&conn, "grok-uni-0001");
    assert!(before > 0);
    assert!(usages(&conn, "grok-uni-0003") > 0);
    let cursor = || {
        conn.query_row("SELECT committed_offset,parser_state_json FROM transcript_cursors WHERE source='grok' AND locator=?1",[log.to_string_lossy()],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,String>(1)?))).unwrap()
    };
    let saved = cursor();
    expire(&store);
    assert_eq!(usages(&conn, "grok-uni-0001"), 0);
    assert_eq!(usages(&conn, "grok-uni-0003"), 0);
    assert_eq!(cursor(), saved);
    store
        .discover(crate::DiscoveryOptions::default().sources([Source::Grok]))
        .unwrap();
    let unavailable = log.with_extension("saved");
    std::fs::rename(&log, &unavailable).unwrap();
    store.hydrate(&target, HydrateOptions::default()).unwrap();
    assert_eq!(usages(&conn, "grok-uni-0001"), 0);
    std::fs::rename(&unavailable, &log).unwrap();
    store.hydrate(&target, HydrateOptions::default()).unwrap();
    assert_eq!(usages(&conn, "grok-uni-0001"), before);
    assert_eq!(
        usages(&conn, "grok-uni-0003"),
        0,
        "explicit recovery must not restore another evicted session"
    );
    assert_eq!(
        cursor(),
        saved,
        "the shared journal checkpoint must not be rewound"
    );
    store.hydrate(&target, HydrateOptions::default()).unwrap();
    assert_eq!(usages(&conn, "grok-uni-0001"), before);
    expire(&store);
    store
        .sync(
            crate::SyncOptions::default()
                .sources([Source::Grok])
                .paths([sessions.join("grok-uni-0001/chat_history.jsonl")])
                .force(true),
        )
        .unwrap();
    assert_eq!(
        usages(&conn, "grok-uni-0001"),
        before,
        "native capture must also replay the evicted session's usage"
    );
    assert_eq!(usages(&conn, "grok-uni-0003"), 0);
    assert_eq!(cursor(), saved);
}
#[test]
fn explicit_claude_hydration_recovers_after_retention_keeps_native_cursors() {
    let dir = tempfile::tempdir().unwrap();
    let id = "11111111-1111-4111-8111-111111111111";
    let path = dir
        .path()
        .join(format!(".claude/projects/-synthetic/{id}.jsonl"));
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path,format!("{{\"type\":\"user\",\"sessionId\":\"{id}\",\"uuid\":\"u\",\"timestamp\":\"2026-09-01T00:00:00Z\",\"message\":{{\"role\":\"user\",\"content\":\"hello\"}}}}\n{{\"type\":\"assistant\",\"sessionId\":\"{id}\",\"uuid\":\"a\",\"timestamp\":\"2026-09-01T00:00:01Z\",\"message\":{{\"id\":\"m\",\"role\":\"assistant\",\"content\":[{{\"type\":\"text\",\"text\":\"answer\"}}],\"stop_reason\":\"end_turn\"}}}}\n")).unwrap();
    let (store, conn) = isolated_store(dir.path());
    store
        .sync(crate::SyncOptions::default().sources([Source::Claude]))
        .unwrap();
    assert!(count(&conn, "transcript_cursors") > 0);
    let before = count(&conn, "session_events");
    assert!(before > 0);
    expire(&store);
    assert_eq!(count(&conn, "session_events"), 0);
    assert!(count(&conn, "transcript_cursors") > 0);
    store
        .hydrate(
            &SessionRef::path(Source::Claude, path),
            HydrateOptions::default(),
        )
        .unwrap();
    assert_eq!(count(&conn, "session_events"), before);
}
