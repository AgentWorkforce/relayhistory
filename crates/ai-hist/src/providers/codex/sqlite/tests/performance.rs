use super::*;
use std::time::Instant;

/// Run with `cargo test -p ai-hist --release --lib sqlite_single_append_benchmark -- --ignored --nocapture`.
#[test]
#[ignore = "synthetic CPU benchmark"]
fn sqlite_single_append_benchmark() {
    let count: usize = std::env::var("AI_HIST_SQLITE_BENCH_ITEMS")
        .unwrap_or_else(|_| "10000".into())
        .parse()
        .unwrap();
    let runs: usize = std::env::var("AI_HIST_SQLITE_BENCH_RUNS")
        .unwrap_or_else(|_| "2".into())
        .parse()
        .unwrap();
    assert!((1..MAX_ITEMS).contains(&count));
    assert!((1..=10).contains(&runs));
    for run in 1..=runs {
        let dir = tempfile::tempdir().unwrap();
        let codex = dir.path().join(".codex");
        install_thread_set(&codex, &[2_000_000]);
        let history = Connection::open(codex.join("thread_history_1.sqlite")).unwrap();
        history.execute_batch(&format!(
            "CREATE INDEX items_by_thread_ordinal ON thread_items(thread_id,rollout_ordinal); \
             WITH RECURSIVE ord(n) AS (VALUES(0) UNION ALL SELECT n+1 FROM ord WHERE n<{}) \
             INSERT INTO thread_items SELECT 'thread-000','turn','item-'||n,n,n*1000,'agentMessage', \
             json_object('id','item-'||n,'type','agentMessage','text',printf('%.*c',2200,'a')) FROM ord; \
             UPDATE thread_history_projection_state SET next_rollout_ordinal={count},next_rollout_byte_offset={count};", count - 1),
        ).unwrap();
        let conn = crate::open_db(&dir.path().join("history.db")).unwrap();
        let mut state = serde_json::Map::new();
        let repairs = crate::ingest::SweepRepairs::default();
        let start = Instant::now();
        let initial = sync_with_policy(
            &conn,
            &mut state,
            &codex,
            &repairs,
            SweepPolicy::unbounded(),
        )
        .unwrap();
        let initial_ms = start.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(initial.read, ["thread-000"]);

        history.execute_batch(&format!(
            "INSERT INTO thread_items VALUES ('thread-000','turn','item-{count}',{count},{ts},'agentMessage', \
             json_object('id','item-{count}','type','agentMessage','text',printf('%.*c',2200,'a'))); \
             UPDATE thread_history_projection_state SET next_rollout_ordinal={next},next_rollout_byte_offset={next};", ts=count*1000, next=count+1),
        ).unwrap();
        Connection::open(codex.join("state_5.sqlite"))
            .unwrap()
            .execute("UPDATE threads SET updated_at_ms=updated_at_ms+1", [])
            .unwrap();
        let start = Instant::now();
        let appended = sync_with_policy(
            &conn,
            &mut state,
            &codex,
            &repairs,
            SweepPolicy::unbounded(),
        )
        .unwrap();
        let append_ms = start.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(appended.read, ["thread-000"]);
        let events: i64 = conn
            .query_row(
                "SELECT count(*) FROM session_events WHERE session_id='thread-000'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(events, (count + 1) as i64);
        println!(
            "items={count} run={run} initial_ms={initial_ms:.3} single_append_ms={append_ms:.3} events={events}"
        );
    }
}
