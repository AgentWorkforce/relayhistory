//! Focused W1 measurement; W0 owns the end-to-end desktop latency gate.
#![cfg(feature = "unstable-internal")]
use ai_hist::{open_db, CatalogQuery, SessionStore, StoreOptions};
use std::time::Instant;

#[test]
#[ignore = "wall-clock measurements, run with --ignored --nocapture"]
fn local_retention_read_path_measurement() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("history.db");
    let mut conn = open_db(&path).unwrap();
    let tx = conn.transaction().unwrap();
    for i in 0..2000 {
        tx.execute("INSERT INTO sessions(source,session_id,last_activity_ms,raw_path) VALUES ('codex',?1,?2,'/synthetic')", rusqlite::params![format!("s{i}"), i]).unwrap();
    }
    tx.commit().unwrap();
    let payloads = std::env::var_os("RETENTION_PERF_PAYLOAD").is_some();
    let drop_indexes = std::env::var_os("RETENTION_PERF_DROP_INDEXES").is_some();
    if drop_indexes {
        let names = conn
            .prepare(
                "SELECT name FROM sqlite_master WHERE type='index' AND name GLOB 'idx_retention_*'",
            )
            .unwrap()
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        for name in names {
            conn.execute_batch(&format!("DROP INDEX {name}")).unwrap();
        }
    }
    let store = SessionStore::open(
        StoreOptions::default()
            .db_path(&path)
            .read_only(drop_indexes),
    )
    .unwrap();
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let worker = if std::env::var_os("RETENTION_PERF_PRUNE").is_some() {
        store.register_retention_consumer("perf").unwrap();
        store
            .acknowledge_retention("perf", store.head_revision().unwrap())
            .unwrap();
        let janitor = store.clone();
        let done = stop.clone();
        Some(std::thread::spawn(move || {
            let mut rows = 0;
            let mut sessions = 0;
            while !done.load(std::sync::atomic::Ordering::Acquire) {
                let batch = janitor.prune_expired(Default::default()).unwrap();
                rows += batch.rows;
                sessions += batch.sessions;
                if batch.deferred || batch.busy || batch.cycle_complete {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                std::thread::yield_now();
            }
            (rows, sessions)
        }))
    } else {
        None
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let mut reads = Vec::new();
    let mut writes = Vec::new();
    for i in 0..550 {
        let start = Instant::now();
        let rows: Vec<_> = store
            .sessions(CatalogQuery::default())
            .take(50)
            .collect::<Result<_, _>>()
            .unwrap();
        assert!(!rows.is_empty());
        if i >= 50 {
            reads.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        let start = Instant::now();
        let insert = |conn: &rusqlite::Connection| {
            conn.execute("INSERT INTO sessions(source,session_id,last_activity_ms,raw_path) VALUES ('codex',?1,?2,'/synthetic')", rusqlite::params![format!("new{i}"), now+i]).unwrap()
        };
        if payloads {
            let tx = conn.transaction().unwrap();
            insert(&tx);
            tx.execute("INSERT INTO session_events(source,session_id,event_uid,role,kind,text,ts_ms) VALUES('codex',?1,?1,'assistant','text','synthetic answer',?2)",rusqlite::params![format!("new{i}"),now+i]).unwrap();
            tx.execute("INSERT INTO tool_calls(source,session_id,tool_use_id,name,ts_ms) VALUES('codex',?1,?1,'synthetic',?2)",rusqlite::params![format!("new{i}"),now+i]).unwrap();
            tx.execute("INSERT INTO history(source,session_id,prompt,timestamp_ms) VALUES('codex',?1,'synthetic prompt',?2)",rusqlite::params![format!("new{i}"),now+i]).unwrap();
            tx.commit().unwrap();
        } else {
            insert(&conn);
        }
        let first = store
            .sessions(CatalogQuery::default())
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(first.session_id, format!("new{i}"));
        if i >= 50 {
            writes.push(start.elapsed().as_secs_f64() * 1000.0);
        }
    }
    stop.store(true, std::sync::atomic::Ordering::Release);
    if let Some(worker) = worker {
        println!("pruned (rows, sessions)={:?}", worker.join().unwrap());
    }
    for (label, values) in [("list50", &mut reads), ("write_to_list1", &mut writes)] {
        values.sort_by(f64::total_cmp);
        println!(
            "{label}: p50={:.4}ms p95={:.4}ms p99={:.4}ms n={}",
            values[values.len() / 2],
            values[values.len() * 95 / 100],
            values[values.len() * 99 / 100],
            values.len()
        );
    }
}
