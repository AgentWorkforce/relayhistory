use super::*;

fn target(source: &str) -> Target {
    Target {
        key: (source.into(), "batch".into()),
        child_locators: Vec::new(),
        kept_parents: Vec::new(),
    }
}
fn seed(conn: &Connection, source: &str) {
    conn.execute("INSERT INTO session_events(source,session_id,event_uid,ts_ms,role,kind,text) VALUES(?1,'batch','event',1,'user','text','body')",[source]).unwrap();
}

#[test]
fn batches_stop_before_the_source_changes_and_preserve_the_claude_fence() {
    let home = tempfile::tempdir().unwrap();
    let path = home.path().join("batch.db");
    let mut conn = open_db(&path).unwrap();
    seed(&conn, "codex");
    seed(&conn, "claude");
    let holder = open_db(&path).unwrap();
    let guard = crate::ingest::claude_write_fence::acquire(&holder, "claude").unwrap();
    let targets = [target("codex"), target("claude")];
    let mut pending = targets.iter().peekable();
    let mut report = ForgetReport::default();
    forget_batch(&mut conn, &mut pending, &mut report).unwrap();
    assert_eq!(report.events, 1);
    assert_eq!(pending.peek().unwrap().key.0, "claude");
    let before = crate::session_events(&conn, "batch", Some("claude")).unwrap();
    assert_eq!(before.len(), 1);
    assert!(forget_batch(&mut conn, &mut pending, &mut report)
        .unwrap_err()
        .to_string()
        .contains("Claude source is being captured"));
    assert_eq!(report.events, 1);
    assert_eq!(
        crate::session_events(&conn, "batch", Some("claude")).unwrap()[0].id,
        before[0].id
    );
    drop(guard);
    forget_batch(&mut conn, &mut pending, &mut report).unwrap();
    assert!(pending.peek().is_none());
    assert_eq!(report.events, 2);
}

#[test]
fn a_claude_lease_wait_does_not_consume_the_batch_write_budget() {
    let home = tempfile::tempdir().unwrap();
    let path = home.path().join("wait.db");
    let mut conn = open_db(&path).unwrap();
    seed(&conn, "claude");
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (wait_tx, wait_rx) = std::sync::mpsc::channel();
    let holder = std::thread::spawn(move || {
        let conn = open_db(&path).unwrap();
        let _guard = crate::ingest::claude_write_fence::acquire(&conn, "claude").unwrap();
        ready_tx.send(()).unwrap();
        wait_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        std::thread::sleep(BATCH_TIME + Duration::from_millis(50));
    });
    ready_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    crate::store::busy_wait::observe_next_wait(wait_tx);
    let targets = [target("claude")];
    let mut pending = targets.iter().peekable();
    let mut report = ForgetReport::default();
    let started = Instant::now();
    forget_batch(&mut conn, &mut pending, &mut report).unwrap();
    holder.join().unwrap();
    assert!(started.elapsed() >= BATCH_TIME);
    assert_eq!(
        report.events, 1,
        "one batch must perform its write after waiting longer than its write budget"
    );
    assert!(pending.peek().is_none());
}
