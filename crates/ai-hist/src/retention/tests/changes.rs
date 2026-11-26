use super::*;

fn identities(store: &SessionStore, after: u64) -> Vec<String> {
    store
        .retention_changes_since(after)
        .unwrap()
        .sessions
        .unwrap()
        .into_iter()
        .map(|identity| identity.session_id)
        .collect()
}

#[test]
fn payload_batches_and_catalog_deletion_have_durable_named_coverage() {
    let (dir, store, conn) = fixture();
    session(&conn, "old", 40, 3);
    acknowledge(&store, "cloud");
    let opts = RetentionOptions {
        max_rows: 1,
        ..Default::default()
    };
    let mut previous = 0;
    for _ in 0..5 {
        run(&store, &opts, NOW).unwrap();
        let changes = store.retention_changes_since(previous).unwrap();
        assert_eq!(changes.generation, previous + 1);
        assert_eq!(
            changes.sessions.unwrap(),
            vec![crate::SessionIdentity::new("codex", "old")]
        );
        previous += 1;
    }
    assert_eq!(identities(&store, 0), vec!["old"]);
    assert!(identities(&store, previous).is_empty());
    drop(store);
    let reopened = SessionStore::open(
        StoreOptions::default()
            .db_path(dir.path().join("history.db"))
            .read_only(true),
    )
    .unwrap();
    assert_eq!(identities(&reopened, 0), vec!["old"]);
}

#[test]
fn deleting_a_relationship_invalidates_the_surviving_child_and_parent() {
    let (_dir, store, conn) = fixture();
    session(&conn, "parent", 40, 1);
    session(&conn, "child", 1, 1);
    conn.execute("INSERT INTO session_relationships(source,relationship_uid,parent_session_id,child_session_id,relationship,identity_status,evidence_kind,created_ms,updated_ms) VALUES ('codex','edge','parent','child','delegated','observed','test',1,1)", []).unwrap();
    acknowledge(&store, "cloud");
    drain(&store, RetentionOptions::default());
    assert_eq!(identities(&store, 0), vec!["child", "parent"]);
    assert!(store
        .delegated_by(&crate::SessionIdentity::new("codex", "child"))
        .unwrap()
        .is_empty());
}

#[test]
fn anonymous_eviction_has_complete_empty_coverage() {
    let (_dir, store, conn) = fixture();
    conn.execute(
        "INSERT INTO history(source,prompt,timestamp_ms) VALUES ('codex','anonymous',?1)",
        [NOW - 40 * DAY],
    )
    .unwrap();
    acknowledge(&store, "cloud");
    run(&store, &RetentionOptions::default(), NOW).unwrap();
    let changes = store.retention_changes_since(0).unwrap();
    assert_eq!(changes.generation, 1);
    assert_eq!(changes.sessions, Some(vec![]));
}

#[test]
fn missing_old_writer_batch_and_future_cursor_require_rebuild() {
    let (_dir, store, conn) = fixture();
    conn.execute(
        "INSERT INTO store_settings(key,value) VALUES ('retention.catalog_generation','1')",
        [],
    )
    .unwrap();
    let changes = store.retention_changes_since(0).unwrap();
    assert_eq!(changes.generation, 1);
    assert!(changes.sessions.is_none());
    assert!(store.retention_changes_since(2).unwrap().sessions.is_none());
    // A newer writer can provide complete coverage after the gap.
    session(&conn, "old", 40, 1);
    acknowledge(&store, "cloud");
    run(&store, &RetentionOptions::default(), NOW).unwrap();
    assert!(store.retention_changes_since(0).unwrap().sessions.is_none());
    assert_eq!(identities(&store, 1), vec!["old"]);
}

#[test]
fn pre_journal_read_only_store_can_report_missing_coverage() {
    let (dir, store, conn) = fixture();
    conn.execute_batch("DROP TABLE retention_changes; INSERT INTO store_settings(key,value) VALUES ('retention.catalog_generation','1');").unwrap();
    drop(store);
    let reader = SessionStore::open(
        StoreOptions::default()
            .db_path(dir.path().join("history.db"))
            .read_only(true),
    )
    .unwrap();
    assert!(reader
        .retention_changes_since(0)
        .unwrap()
        .sessions
        .is_none());
    assert_eq!(identities(&reader, 1), Vec::<String>::new());
}

#[test]
fn writable_upgrade_adds_journal_without_claiming_old_coverage() {
    let (dir, store, conn) = fixture();
    conn.execute_batch("DROP TABLE retention_changes; INSERT INTO store_settings(key,value) VALUES ('retention.catalog_generation','7');").unwrap();
    drop(store);
    let upgraded =
        SessionStore::open(StoreOptions::default().db_path(dir.path().join("history.db"))).unwrap();
    assert!(super::super::changes::present(&conn).unwrap());
    assert!(upgraded
        .retention_changes_since(6)
        .unwrap()
        .sessions
        .is_none());
    session(&conn, "old", 40, 1);
    acknowledge(&upgraded, "cloud");
    run(&upgraded, &RetentionOptions::default(), NOW).unwrap();
    assert_eq!(identities(&upgraded, 7), vec!["old"]);
}

#[test]
fn deleting_child_invalidates_relationship_only_parent() {
    let (_dir, store, conn) = fixture();
    session(&conn, "child", 40, 1);
    conn.execute("INSERT INTO session_relationships(source,relationship_uid,parent_session_id,child_session_id,relationship,identity_status,evidence_kind,created_ms,updated_ms) VALUES ('codex','edge','parent','child','delegated','observed','test',1,1)", []).unwrap();
    let parent = crate::SessionIdentity::new("codex", "parent");
    assert!(store.has_session(&parent).unwrap());
    acknowledge(&store, "cloud");
    drain(&store, RetentionOptions::default());
    assert_eq!(identities(&store, 0), vec!["child", "parent"]);
    assert!(!store.has_session(&parent).unwrap());
}

#[test]
fn journal_failure_rolls_back_eviction_and_generation() {
    let (_dir, store, conn) = fixture();
    session(&conn, "old", 40, 1);
    acknowledge(&store, "cloud");
    conn.execute_batch("CREATE TRIGGER reject_journal BEFORE INSERT ON retention_changes BEGIN SELECT RAISE(ABORT,'synthetic journal failure'); END;").unwrap();
    assert!(run(&store, &RetentionOptions::default(), NOW).is_err());
    assert_eq!(store.retention_generation().unwrap(), 0);
    assert_eq!(count(&conn, "session_events"), 1);
    assert_eq!(count(&conn, "retention_changes"), 0);
}

#[test]
fn journal_is_bounded_and_expired_cursor_requires_rebuild() {
    let (_dir, store, conn) = fixture();
    let tx = conn.unchecked_transaction().unwrap();
    for _ in 0..1_025 {
        super::super::generation::advance_if_evicted(&tx, 1, false, &Default::default()).unwrap();
    }
    tx.commit().unwrap();
    assert_eq!(count(&conn, "retention_changes"), 1_024);
    assert!(store.retention_changes_since(0).unwrap().sessions.is_none());
    assert_eq!(identities(&store, 1), Vec::<String>::new());
}

#[test]
fn oversized_aggregate_requires_rebuild_instead_of_dropping_identities() {
    let (_dir, store, conn) = fixture();
    let tx = conn.unchecked_transaction().unwrap();
    for batch in 0..65 {
        let affected = (0..64)
            .map(|n| ("codex".into(), format!("{batch}-{n}")))
            .collect();
        super::super::generation::advance_if_evicted(&tx, 1, false, &affected).unwrap();
    }
    tx.commit().unwrap();
    assert!(store.retention_changes_since(0).unwrap().sessions.is_none());
    assert_eq!(identities(&store, 64).len(), 64);
}

#[test]
#[ignore = "synthetic 5,000-session retention invalidation experiment"]
fn incremental_membership_experiment() {
    use crate::{IdentityQuery, SessionIdentity};
    use std::{collections::BTreeSet, time::Instant};

    let (_dir, store, conn) = fixture();
    let tx = conn.unchecked_transaction().unwrap();
    session(&tx, "expired", 40, 512);
    for index in 0..4_999 {
        session(&tx, &format!("fresh-{index}"), 1, 0);
    }
    tx.commit().unwrap();
    acknowledge(&store, "cloud");
    let query = IdentityQuery::default().limit(10_000).exclude_delegated();
    let mut membership: BTreeSet<_> = store
        .session_identities(query.clone())
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(membership.len(), 5_000);
    let mut generation = store.retention_generation().unwrap();
    let mut touched = 0;
    let mut incremental = Duration::ZERO;
    let mut full_walk = Duration::ZERO;
    let mut batches = 0;
    while store
        .has_session(&SessionIdentity::new("codex", "expired"))
        .unwrap()
    {
        run(
            &store,
            &RetentionOptions {
                max_rows: 32,
                ..Default::default()
            },
            NOW,
        )
        .unwrap();
        let started = Instant::now();
        let changes = store.retention_changes_since(generation).unwrap();
        for identity in changes.sessions.unwrap() {
            touched += 1;
            if store.has_session(&identity).unwrap()
                && store.delegated_by(&identity).unwrap().is_empty()
            {
                membership.insert(identity);
            } else {
                membership.remove(&identity);
            }
        }
        generation = changes.generation;
        incremental += started.elapsed();
        let started = Instant::now();
        let authoritative: BTreeSet<_> = store
            .session_identities(query.clone())
            .unwrap()
            .into_iter()
            .collect();
        full_walk += started.elapsed();
        assert_eq!(membership, authoritative);
        batches += 1;
        assert!(batches < 100);
    }
    assert_eq!(membership.len(), 4_999);
    eprintln!("retention-invalidation-experiment sessions=5000 batches={batches} identities_rechecked={touched} incremental_wall_ms={:.3} full_walk_wall_ms={:.3}", incremental.as_secs_f64()*1000., full_walk.as_secs_f64()*1000.);
}
