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
    // The unrelated change is never journaled, so there is nothing to reclaim.
    assert_eq!(export::compact_journal(&conn, 100).unwrap(), 0);
    let revision = capture::next_change(&conn, 0, None).unwrap().unwrap();
    assert_eq!(revision.session.as_deref(), Some("one"));
    assert!(capture::next_change(&conn, revision.position, None)
        .unwrap()
        .is_none());
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
    let unrelated = SessionIdentity {
        source: "claude".into(),
        session_id: "unrelated".into(),
    };
    capture::save_subscription(
        &tx,
        &capture::Subscription {
            id: "released",
            session: Some(&unrelated),
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
    // Releasing the member leaves its journaled change with no reader.
    capture::release_subscription(&tx, "released").unwrap();
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
        // Two members journal one change each and are then released, so their
        // changes are reclaimable on either side of the pinned prefix.
        for id in ["before", "after"] {
            capture::save_subscription(
                &tx,
                &capture::Subscription {
                    id,
                    session: Some(&SessionIdentity {
                        source: "claude".into(),
                        session_id: id.into(),
                    }),
                    cursor: 0,
                    kind: 0,
                    rowid: 0,
                    complete: true,
                },
            )
            .unwrap();
        }
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
        for id in ["before", "after"] {
            capture::release_subscription(&tx, id).unwrap();
        }
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

fn journal_sessions(conn: &Connection) -> Vec<(String, Option<String>, String)> {
    conn.prepare("SELECT kind,session_id,operation FROM delivery_journal WHERE kind <> '__cutoff' ORDER BY seq")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap()
}
fn subscribe(conn: &Connection, id: &str, session: Option<&SessionIdentity>) {
    let tx = conn.unchecked_transaction().unwrap();
    capture::save_subscription(
        &tx,
        &capture::Subscription {
            id,
            session,
            cursor: 0,
            kind: 0,
            rowid: 0,
            complete: true,
        },
    )
    .unwrap();
    tx.commit().unwrap();
}

#[test]
fn a_root_subscription_journals_nothing_for_an_excluded_session() {
    let conn = db();
    subscribe(&conn, "root", None);
    conn.execute(
        "INSERT INTO delivery_exclusions(source,session_id) VALUES ('claude','private')",
        [],
    )
    .unwrap();
    conn.execute("INSERT INTO session_events(source,session_id,event_uid,ts_ms,role,kind,text) VALUES ('claude','private','p1',1,'user','text','secret'),('claude','public','q1',1,'user','text','shared')", []).unwrap();
    conn.execute(
        "UPDATE session_events SET text='edited' WHERE session_id='private'",
        [],
    )
    .unwrap();
    conn.execute("DELETE FROM session_events WHERE session_id='private'", [])
        .unwrap();
    assert_eq!(
        journal_sessions(&conn),
        vec![(
            "session_event".into(),
            Some("public".into()),
            "upsert".into()
        )]
    );
    // A row whose identity moves into an excluded session journals the
    // tombstone for the public key and nothing for the private one.
    conn.execute(
        "UPDATE session_events SET session_id='private' WHERE session_id='public'",
        [],
    )
    .unwrap();
    assert_eq!(
        journal_sessions(&conn),
        vec![
            (
                "session_event".into(),
                Some("public".into()),
                "upsert".into()
            ),
            (
                "session_event".into(),
                Some("public".into()),
                "delete".into()
            ),
        ]
    );
    // A relationship discloses both endpoints: an excluded parent journals
    // nothing, linked or unlinked, and neither does an excluded child.
    conn.execute("INSERT INTO session_relationships(source,parent_session_id,relationship_uid,child_session_id,relationship,identity_status,evidence_kind,created_ms,updated_ms) VALUES ('claude','private','linked','public','delegation','observed','fixture',1,1),('claude','private','unlinked',NULL,'delegation','unlinked','fixture',1,1),('claude','public','into-private','private','delegation','observed','fixture',1,1),('claude','public','shared',NULL,'delegation','unlinked','fixture',1,1)", []).unwrap();
    let relationships: Vec<String> = conn
        .prepare("SELECT json_extract(payload,'$.relationship_uid') FROM delivery_journal WHERE kind='relationship' ORDER BY seq")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(relationships, vec!["shared".to_string()]);
    // Without an exclusion the same subscription journals every session.
    conn.execute("DELETE FROM delivery_exclusions", []).unwrap();
    conn.execute(
        "UPDATE session_events SET text='visible' WHERE session_id='private'",
        [],
    )
    .unwrap();
    assert_eq!(journal_sessions(&conn).len(), 4);
}

#[test]
fn member_subscriptions_journal_their_own_sessions_and_relationships_between_members() {
    let conn = db();
    for id in ["one", "two"] {
        subscribe(
            &conn,
            id,
            Some(&SessionIdentity {
                source: "claude".into(),
                session_id: id.into(),
            }),
        );
    }
    conn.execute("INSERT INTO session_events(source,session_id,event_uid,ts_ms,role,kind,text) VALUES ('claude','one','a',1,'user','text','mine'),('claude','other','b',1,'user','text','theirs'),('codex','one','c',1,'user','text','other source')", []).unwrap();
    conn.execute(
        "INSERT INTO sessions(source,session_id,first_prompt) VALUES ('claude','other','theirs')",
        [],
    )
    .unwrap();
    // A relationship needs both endpoints to belong to a member; an unlinked
    // child needs only its parent.
    conn.execute("INSERT INTO session_relationships(source,parent_session_id,relationship_uid,child_session_id,relationship,identity_status,evidence_kind,created_ms,updated_ms) VALUES ('claude','one','between','two','delegation','observed','fixture',1,1),('claude','two','back','one','delegation','observed','fixture',1,1),('claude','one','outgoing','third','delegation','observed','fixture',1,1),('claude','other','incoming','one','delegation','observed','fixture',1,1),('claude','one','unlinked',NULL,'delegation','unlinked','fixture',1,1)", []).unwrap();
    let mut rows: Vec<(String, Option<String>, String, Option<String>)> = conn
        .prepare("SELECT kind,session_id,operation,json_extract(payload,'$.relationship_uid') FROM delivery_journal WHERE kind <> '__cutoff'")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    rows.sort();
    assert_eq!(
        rows,
        vec![
            (
                "relationship".into(),
                Some("one".into()),
                "upsert".into(),
                Some("between".into())
            ),
            (
                "relationship".into(),
                Some("one".into()),
                "upsert".into(),
                Some("unlinked".into())
            ),
            (
                "relationship".into(),
                Some("two".into()),
                "upsert".into(),
                Some("back".into())
            ),
            (
                "session_event".into(),
                Some("one".into()),
                "upsert".into(),
                None
            ),
        ]
    );
    // A row handed from a member to a stranger journals the member's
    // tombstone and nothing for the stranger.
    conn.execute(
        "UPDATE session_events SET session_id='other' WHERE session_id='one' AND source='claude'",
        [],
    )
    .unwrap();
    let mut latest = journal_sessions(&conn);
    latest.sort();
    assert_eq!(latest.len(), 5);
    assert!(latest.contains(&("session_event".into(), Some("one".into()), "delete".into())));
    assert!(!latest
        .iter()
        .any(|(_, session, _)| session.as_deref() == Some("other")));
    // An excluded member journals nothing, even while it stays subscribed.
    conn.execute(
        "INSERT INTO delivery_exclusions(source,session_id) VALUES ('claude','two')",
        [],
    )
    .unwrap();
    conn.execute(
        "UPDATE session_relationships SET updated_ms=2 WHERE relationship_uid IN ('between','back')",
        [],
    )
    .unwrap();
    assert_eq!(journal_sessions(&conn).len(), 5);
}

/// Triggers created before the capture filter journal on subscription
/// existence alone. The marker is what tells a writable open to rebuild them.
#[test]
fn upgrade_rebuilds_capture_triggers_that_journal_without_the_filter() {
    let conn = db();
    let columns = conn
        .prepare("SELECT name FROM pragma_table_info('sessions')")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    let payload = format!(
        "json_object({})",
        columns
            .iter()
            .map(|c| format!("'{c}',NEW.\"{c}\""))
            .collect::<Vec<_>>()
            .join(",")
    );
    conn.execute_batch(&format!(
        "DROP TRIGGER delivery_sessions_insert;
         CREATE TRIGGER delivery_sessions_insert AFTER INSERT ON sessions BEGIN
          INSERT INTO delivery_journal(kind,source,session_id,record_key,operation,payload)
          SELECT 'session',NEW.source,NEW.session_id,json_array('session',NEW.source,NEW.session_id),'upsert',{payload} WHERE EXISTS(SELECT 1 FROM history_subscriptions);
         END;"
    ))
    .unwrap();
    let trigger = || -> String {
        conn.query_row(
            "SELECT sql FROM sqlite_master WHERE type='trigger' AND name='delivery_sessions_insert'",
            [],
            |r| r.get(0),
        )
        .unwrap()
    };
    // The payload is current and every trigger name is present, so nothing but
    // the marker distinguishes this trigger from a filtered one.
    assert!(ai_hist::schema_is_current(&conn).unwrap());
    conn.execute(
        "DELETE FROM schema_migrations WHERE name='delivery_capture_filter_v1'",
        [],
    )
    .unwrap();
    assert!(!ai_hist::schema_is_current(&conn).unwrap());
    ai_hist::init_db(&conn).unwrap();
    assert!(ai_hist::schema_is_current(&conn).unwrap());
    assert!(trigger().contains("delivery_exclusions"), "{}", trigger());
    subscribe(&conn, "root", None);
    conn.execute(
        "INSERT INTO delivery_exclusions(source,session_id) VALUES ('claude','private')",
        [],
    )
    .unwrap();
    conn.execute("INSERT INTO sessions(source,session_id,first_prompt) VALUES ('claude','private','secret'),('claude','public','shared')", []).unwrap();
    assert_eq!(
        journal_sessions(&conn),
        vec![("session".into(), Some("public".into()), "upsert".into())]
    );
}

/// Capture and every reader answer "may this identity leave the machine" with
/// one SQL fragment: the triggers embed it and `is_shareable` evaluates it.
#[test]
fn one_consent_rule_gates_capture_and_reads() {
    let conn = db();
    assert!(capture::is_shareable(&conn, "claude", "one").unwrap());
    conn.execute(
        "INSERT INTO delivery_exclusions(source,session_id) VALUES ('claude','one')",
        [],
    )
    .unwrap();
    assert!(!capture::is_shareable(&conn, "claude", "one").unwrap());
    assert!(capture::is_shareable(&conn, "codex", "one").unwrap());
    let trigger: String = conn
        .query_row(
            "SELECT sql FROM sqlite_master WHERE type='trigger' AND name='delivery_sessions_insert'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(
        trigger.contains(&capture::shareable("NEW.source", "NEW.session_id")),
        "{trigger}"
    );
    // A file export applies the same rule when it reads.
    conn.execute("INSERT INTO sessions(source,session_id,first_prompt) VALUES ('claude','one','private'),('claude','two','public')", []).unwrap();
    let snapshot = export::create_export(
        &conn,
        &ExportSelection {
            all_sources: true,
            kinds: vec!["session".into()],
            ..Default::default()
        },
        &ExportLimits::default(),
        60_000,
        chrono::Utc::now().timestamp_millis(),
    )
    .unwrap();
    let page = export::export_page(
        &conn,
        &snapshot.cursor,
        chrono::Utc::now().timestamp_millis(),
    )
    .unwrap();
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].session_id.as_deref(), Some("two"));
}

/// A tombstone names one identity and carries no payload, so a delivered edge
/// is retracted even once its child stops being eligible; the revision that
/// would disclose that child is not journaled.
#[test]
fn a_delivered_edge_is_retracted_after_its_child_becomes_ineligible() {
    let conn = db();
    subscribe(&conn, "root", None);
    conn.execute("INSERT INTO session_relationships(source,parent_session_id,relationship_uid,child_session_id,relationship,identity_status,evidence_kind,created_ms,updated_ms) VALUES ('claude','parent','edge','child','delegation','observed','fixture',1,1)", []).unwrap();
    assert_eq!(
        journal_sessions(&conn),
        vec![(
            "relationship".into(),
            Some("parent".into()),
            "upsert".into()
        )]
    );
    conn.execute(
        "INSERT INTO delivery_exclusions(source,session_id) VALUES ('claude','child')",
        [],
    )
    .unwrap();
    // The edge now discloses an ineligible child: no further revision of it is
    // journaled, under its own key or a new one.
    conn.execute("UPDATE session_relationships SET updated_ms=2", [])
        .unwrap();
    conn.execute(
        "UPDATE session_relationships SET relationship_uid='renamed'",
        [],
    )
    .unwrap();
    assert_eq!(
        journal_sessions(&conn),
        vec![
            (
                "relationship".into(),
                Some("parent".into()),
                "upsert".into()
            ),
            (
                "relationship".into(),
                Some("parent".into()),
                "delete".into()
            ),
        ]
    );
    conn.execute("DELETE FROM session_relationships", [])
        .unwrap();
    assert_eq!(
        journal_sessions(&conn)
            .into_iter()
            .filter(|(_, _, operation)| operation == "delete")
            .count(),
        2
    );
    // An excluded parent retracts nothing: its edge was never delivered.
    conn.execute(
        "INSERT INTO delivery_exclusions(source,session_id) VALUES ('claude','private')",
        [],
    )
    .unwrap();
    conn.execute("INSERT INTO session_relationships(source,parent_session_id,relationship_uid,child_session_id,relationship,identity_status,evidence_kind,created_ms,updated_ms) VALUES ('claude','private','hidden','child','delegation','observed','fixture',1,1)", []).unwrap();
    conn.execute(
        "DELETE FROM session_relationships WHERE parent_session_id='private'",
        [],
    )
    .unwrap();
    assert_eq!(journal_sessions(&conn).len(), 3);
}
