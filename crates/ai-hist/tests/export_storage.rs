#![cfg(all(feature = "export", feature = "unstable-internal"))]
use ai_hist::export::{self, capture, ExportLimits, ExportSelection, SessionIdentity};
use rusqlite::Connection;
fn db() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    ai_hist::init_db(&conn).unwrap();
    let tx = conn.unchecked_transaction().unwrap();
    capture::initialize(&tx).unwrap();
    tx.commit().unwrap();
    conn
}
#[test]
fn standalone_snapshot_retains_preimages_without_upload_jobs() {
    let conn = db();
    conn.execute(
        "INSERT INTO sessions(source,session_id,first_prompt) VALUES ('claude','one','before')",
        [],
    )
    .unwrap();
    let selection = ExportSelection {
        all_sources: true,
        kinds: vec!["session".into()],
        ..Default::default()
    };
    let snapshot = export::create_export(
        &conn,
        &selection,
        &ExportLimits::default(),
        60_000,
        chrono::Utc::now().timestamp_millis(),
    )
    .unwrap();
    conn.execute("UPDATE sessions SET first_prompt='after'", [])
        .unwrap();
    let mut cursor = Some(snapshot.cursor);
    let mut records = Vec::new();
    while let Some(value) = cursor {
        let page =
            export::export_page(&conn, &value, chrono::Utc::now().timestamp_millis()).unwrap();
        records.extend(page.records);
        cursor = page.next_cursor;
    }
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].payload["first_prompt"], "before");
    assert_eq!(conn.query_row("SELECT COUNT(*) FROM sqlite_master WHERE name IN ('delivery_jobs','delivery_batches','delivery_session_members')",[],|r|r.get::<_,usize>(0)).unwrap(),0);
}
#[test]
fn subscription_tracks_one_sessions_snapshot_and_future_revisions() {
    let conn = db();
    conn.execute("INSERT INTO sessions(source,session_id,first_prompt) VALUES ('claude','one','before'),('claude','private','private')", []).unwrap();
    let identity = SessionIdentity {
        source: "claude".into(),
        session_id: "one".into(),
    };
    let tx = conn.unchecked_transaction().unwrap();
    let revision = capture::reserve_revision(&tx).unwrap();
    capture::save_subscription(
        &tx,
        &capture::Subscription {
            id: "reader",
            session: Some(&identity),
            cursor: revision,
            kind: 0,
            rowid: 0,
            complete: false,
        },
    )
    .unwrap();
    capture::snapshot_bounds(&tx, "reader").unwrap();
    tx.commit().unwrap();
    conn.execute(
        "UPDATE sessions SET first_prompt='after' WHERE session_id='one'",
        [],
    )
    .unwrap();
    let index = (0..capture::kind_count())
        .find(|i| capture::kind(*i) == Some("session"))
        .unwrap();
    let original = capture::session_snapshot_record(&conn, "reader", &identity, index, 0)
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&original.payload).unwrap()["first_prompt"],
        "before"
    );
    let changed = capture::next_change(&conn, revision, Some(&identity))
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&changed.payload).unwrap()["first_prompt"],
        "after"
    );
    assert!(
        capture::next_change(&conn, changed.position, Some(&identity))
            .unwrap()
            .is_none()
    );
}

#[test]
fn idle_session_subscription_does_not_retain_unrelated_changes() {
    let conn = db();
    let identity = SessionIdentity {
        source: "claude".into(),
        session_id: "one".into(),
    };
    let tx = conn.unchecked_transaction().unwrap();
    capture::save_subscription(
        &tx,
        &capture::Subscription {
            id: "reader",
            session: Some(&identity),
            cursor: 0,
            kind: 0,
            rowid: 0,
            complete: true,
        },
    )
    .unwrap();
    tx.commit().unwrap();
    conn.execute("INSERT INTO sessions(source,session_id,first_prompt) VALUES ('claude','one','keep'),('claude','unrelated','discard')", []).unwrap();
    assert_eq!(export::compact_journal(&conn, 100).unwrap(), 1);
    let revision = capture::next_change(&conn, 0, Some(&identity))
        .unwrap()
        .unwrap();
    assert_eq!(revision.session.as_deref(), Some("one"));
    assert_eq!(export::compact_journal(&conn, 100).unwrap(), 0);
    let tx = conn.unchecked_transaction().unwrap();
    capture::release_subscription(&tx, "reader").unwrap();
    tx.commit().unwrap();
    assert_eq!(export::compact_journal(&conn, 100).unwrap(), 1);
}

#[test]
fn compaction_passes_a_retained_prefix_in_bounded_steps() {
    let conn = db();
    let identity = SessionIdentity {
        source: "claude".into(),
        session_id: "one".into(),
    };
    let tx = conn.unchecked_transaction().unwrap();
    capture::save_subscription(
        &tx,
        &capture::Subscription {
            id: "reader",
            session: Some(&identity),
            cursor: 0,
            kind: 0,
            rowid: 0,
            complete: true,
        },
    )
    .unwrap();
    for id in 0..1025 {
        tx.execute("INSERT INTO session_events(source,session_id,event_uid,ts_ms,role,kind,text) VALUES ('claude','one',?,1,'user','text','retained')", [id.to_string()]).unwrap();
    }
    tx.execute("INSERT INTO sessions(source,session_id,first_prompt) VALUES ('claude','unrelated','discard')", []).unwrap();
    tx.commit().unwrap();
    assert_eq!(export::compact_journal(&conn, 1000).unwrap(), 0);
    assert_eq!(
        conn.query_row("SELECT cursor FROM history_compaction", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1000
    );
    assert_eq!(export::compact_journal(&conn, 1000).unwrap(), 1);
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM delivery_journal", [], |r| r
            .get::<_, usize>(0))
            .unwrap(),
        1025
    );
}

/// Verify recovery visits reclaimable rows around a pinned prefix regardless of the saved scan cursor.
#[test]
fn recovery_compaction_visits_the_whole_journal_despite_the_background_cursor() {
    for cursor in [0, 1000, 9999] {
        let conn = db();
        let identity = SessionIdentity {
            source: "claude".into(),
            session_id: "pinned".into(),
        };
        let tx = conn.unchecked_transaction().unwrap();
        capture::save_subscription(
            &tx,
            &capture::Subscription {
                id: "reader",
                session: Some(&identity),
                cursor: 0,
                kind: 0,
                rowid: 0,
                complete: true,
            },
        )
        .unwrap();
        tx.execute(
            "INSERT INTO sessions(source,session_id) VALUES ('claude','before')",
            [],
        )
        .unwrap();
        for id in 0..1025 {
            tx.execute("INSERT INTO session_events(source,session_id,event_uid,ts_ms,role,kind,text) VALUES ('claude','pinned',?,1,'user','text','retained')", [id.to_string()]).unwrap();
        }
        tx.execute(
            "INSERT INTO sessions(source,session_id) VALUES ('claude','after')",
            [],
        )
        .unwrap();
        tx.execute("UPDATE history_compaction SET cursor=?", [cursor])
            .unwrap();
        tx.commit().unwrap();
        assert_eq!(export::compact_journal_pass(&conn, 1000).unwrap(), 2);
        // A complete, all-pinned pass terminates and preserves every revision.
        assert_eq!(export::compact_journal_pass(&conn, 1000).unwrap(), 0);
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM delivery_journal", [], |r| r
                .get::<_, usize>(0))
                .unwrap(),
            1025
        );
        assert!(export::compact_journal_pass(&conn, 0).is_err());
        assert!(export::compact_journal_pass(&conn, 10_001).is_err());
    }
}

#[test]
fn cached_ingest_statements_observe_subscription_changes() {
    let conn = db();
    let identity = SessionIdentity {
        source: "claude".into(),
        session_id: "one".into(),
    };
    let insert = |prompt: &str, timestamp_ms| {
        ai_hist::insert_history(
            &conn,
            &ai_hist::HistoryEntry {
                id: 0,
                source: identity.source.clone(),
                session_id: Some(identity.session_id.clone()),
                project: None,
                prompt: prompt.into(),
                prompt_hash: None,
                timestamp_ms,
            },
        )
        .unwrap();
    };
    // Compile both the presence and history statements before any subscriber exists.
    insert("before", 1);
    assert!(capture::next_change(&conn, 0, Some(&identity))
        .unwrap()
        .is_none());
    let tx = conn.unchecked_transaction().unwrap();
    let cutoff = capture::reserve_revision(&tx).unwrap();
    capture::save_subscription(
        &tx,
        &capture::Subscription {
            id: "reader",
            session: Some(&identity),
            cursor: cutoff,
            kind: 0,
            rowid: 0,
            complete: true,
        },
    )
    .unwrap();
    tx.commit().unwrap();
    insert("during", 2);
    let revision = capture::next_change(&conn, cutoff, Some(&identity))
        .unwrap()
        .unwrap();
    assert_eq!(revision.kind, "history");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&revision.payload).unwrap()["prompt"],
        "during"
    );
    let tx = conn.unchecked_transaction().unwrap();
    capture::release_subscription(&tx, "reader").unwrap();
    tx.commit().unwrap();
    insert("after", 3);
    assert!(
        capture::next_change(&conn, revision.position, Some(&identity))
            .unwrap()
            .is_none()
    );
}

fn global_reader(cursor: i64) -> capture::Subscription<'static> {
    capture::Subscription {
        id: "reader",
        session: None,
        cursor,
        kind: 0,
        rowid: 0,
        complete: true,
    }
}
fn journal_rows(conn: &Connection) -> usize {
    conn.query_row("SELECT COUNT(*) FROM delivery_journal", [], |r| r.get(0))
        .unwrap()
}
fn journal_seq_at(conn: &Connection, offset: usize) -> i64 {
    conn.query_row(
        "SELECT seq FROM delivery_journal ORDER BY seq LIMIT 1 OFFSET ?",
        [offset as i64],
        |r| r.get(0),
    )
    .unwrap()
}

/// Rows every subscription has consumed are reclaimed by their range, not by
/// the sweep cursor reaching them again after a wrap.
#[test]
fn consumed_rows_behind_the_sweep_cursor_are_reclaimed_without_a_wrap() {
    let conn = db();
    let tx = conn.unchecked_transaction().unwrap();
    capture::save_subscription(&tx, &global_reader(0)).unwrap();
    for id in 0..30 {
        tx.execute("INSERT INTO session_events(source,session_id,event_uid,ts_ms,role,kind,text) VALUES ('claude','one',?,1,'user','text','retained')", [id.to_string()]).unwrap();
    }
    tx.commit().unwrap();
    // The reader consumed the first twenty rows after the sweep cursor had
    // already passed them, and rows remain above the cursor so it does not
    // wrap.
    let consumed = journal_seq_at(&conn, 19);
    let tx = conn.unchecked_transaction().unwrap();
    capture::save_subscription(&tx, &global_reader(consumed)).unwrap();
    tx.execute(
        "UPDATE history_compaction SET cursor=?",
        [journal_seq_at(&conn, 24)],
    )
    .unwrap();
    tx.commit().unwrap();
    assert_eq!(export::compact_journal(&conn, 1000).unwrap(), 20);
    assert_eq!(journal_rows(&conn), 10);
    assert!(journal_seq_at(&conn, 0) > consumed);
    assert_eq!(export::compact_journal(&conn, 1000).unwrap(), 0);
    assert_eq!(journal_rows(&conn), 10);
}

/// The steady-state step reclaims the whole consumed range in one call, in
/// transactions no larger than its limit.
#[test]
fn a_fully_consumed_journal_is_reclaimed_by_one_bounded_step() {
    let conn = db();
    let tx = conn.unchecked_transaction().unwrap();
    capture::save_subscription(&tx, &global_reader(0)).unwrap();
    for id in 0..2_500 {
        tx.execute("INSERT INTO session_events(source,session_id,event_uid,ts_ms,role,kind,text) VALUES ('claude','one',?,1,'user','text','consumed')", [id.to_string()]).unwrap();
    }
    tx.commit().unwrap();
    let tail = journal_seq_at(&conn, 2_499);
    let tx = conn.unchecked_transaction().unwrap();
    capture::save_subscription(&tx, &global_reader(tail)).unwrap();
    tx.commit().unwrap();
    assert_eq!(export::compact_journal(&conn, 1000).unwrap(), 2_500);
    assert_eq!(journal_rows(&conn), 0);
}

/// Recovery spends passes only while retained bytes exceed three quarters of
/// the cap, and a pass that reclaims nothing ends it with every row intact.
#[test]
fn recovery_stops_below_the_low_water_mark_or_when_nothing_is_reclaimable() {
    let conn = db();
    let tx = conn.unchecked_transaction().unwrap();
    capture::save_subscription(&tx, &global_reader(0)).unwrap();
    for id in 0..100 {
        tx.execute("INSERT INTO session_events(source,session_id,event_uid,ts_ms,role,kind,text) VALUES ('claude','one',?,1,'user','text','backlog')", [id.to_string()]).unwrap();
    }
    tx.commit().unwrap();
    let (used, _) = export::retained_bytes(&conn).unwrap();
    export::set_retention_limit(&conn, used).unwrap();
    // Nothing consumed: one pass, nothing reclaimed, nothing deleted.
    assert_eq!(export::compact_to_low_water(&conn, 10).unwrap(), 0);
    assert_eq!(journal_rows(&conn), 100);
    // Half consumed: one pass takes the journal under the low-water mark.
    let tx = conn.unchecked_transaction().unwrap();
    capture::save_subscription(&tx, &global_reader(journal_seq_at(&conn, 49))).unwrap();
    tx.commit().unwrap();
    assert_eq!(export::compact_to_low_water(&conn, 10).unwrap(), 50);
    assert_eq!(journal_rows(&conn), 50);
    let (after, cap) = export::retained_bytes(&conn).unwrap();
    assert!(after * 4 < cap * 3);
    // Under the low-water mark recovery does no work; the steady-state step
    // still reclaims what is consumed.
    let tx = conn.unchecked_transaction().unwrap();
    capture::save_subscription(&tx, &global_reader(journal_seq_at(&conn, 49))).unwrap();
    tx.commit().unwrap();
    assert_eq!(export::compact_to_low_water(&conn, 10).unwrap(), 0);
    assert_eq!(journal_rows(&conn), 50);
    assert_eq!(export::compact_journal(&conn, 10).unwrap(), 50);
    assert_eq!(journal_rows(&conn), 0);
    assert!(export::compact_to_low_water(&conn, 0).is_err());
    assert!(export::compact_to_low_water(&conn, 10_001).is_err());
}
