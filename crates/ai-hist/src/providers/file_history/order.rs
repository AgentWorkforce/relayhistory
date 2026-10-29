//! In-place repair of order observed by a complete native source replay.
use anyhow::{ensure, Context, Result};
use rusqlite::Connection;
use std::collections::{BTreeMap, BTreeSet};

struct Observed {
    uid: String,
    rank: Option<i64>,
    shared: bool,
}

/// Preserve known remotely backed ranks, learn previously unknown ranks, and
/// repair local order in place. Every bucket is planned before any row changes.
/// The empty return retains the adapters' no-retirement replay interface.
pub(crate) fn reconcile_event_order(
    conn: &Connection,
    source: &str,
    sid: &str,
    desired: &[(i64, String)],
) -> Result<BTreeSet<i64>> {
    ensure!(
        !conn.is_autocommit(),
        "FILE_HISTORY_ORDER_TRANSACTION_REQUIRED"
    );
    let unique: BTreeSet<_> = desired.iter().map(|(_, uid)| uid.as_str()).collect();
    ensure!(
        unique.len() == desired.len(),
        "FILE_HISTORY_DUPLICATE_ORDER_UID: desired event identities must be unique"
    );
    let mut expected = BTreeMap::<i64, Vec<&str>>::new();
    for (ts, uid) in desired {
        expected.entry(*ts).or_default().push(uid);
    }
    let observed = observed(conn, source, sid)?;
    let mut assignments = Vec::new();
    for (ts, wanted) in expected {
        let existing = observed.get(&ts).map(Vec::as_slice).unwrap_or_default();
        for (uid, rank) in plan_bucket(&wanted, existing)? {
            assignments.push((ts, uid, rank));
        }
    }
    for (ts, uid, rank) in assignments {
        conn.execute("UPDATE session_events SET source_order=?1 WHERE source=?2 AND session_id=?3 AND event_uid=?4 AND ts_ms=?5 AND source_order IS NOT ?1",
            rusqlite::params![rank,source,sid,uid,ts])?;
    }
    Ok(BTreeSet::new())
}

fn observed(conn: &Connection, source: &str, sid: &str) -> Result<BTreeMap<i64, Vec<Observed>>> {
    let mut buckets = BTreeMap::<i64, Vec<Observed>>::new();
    let mut statement = conn.prepare("SELECT ts_ms,event_uid,source_order,location FROM session_events WHERE source=?1 AND session_id=?2 ORDER BY ts_ms,COALESCE(source_order,id),id")?;
    let rows = statement.query_map([source, sid], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            Observed {
                uid: row.get(1)?,
                rank: row.get(2)?,
                shared: row.get::<_, String>(3)? != "local",
            },
        ))
    })?;
    for row in rows {
        let (ts, event) = row?;
        buckets.entry(ts).or_default().push(event);
    }
    Ok(buckets)
}

fn plan_bucket(wanted: &[&str], existing: &[Observed]) -> Result<Vec<(String, i64)>> {
    let members: BTreeSet<_> = wanted.iter().copied().collect();
    let same = wanted.iter().copied().eq(existing
        .iter()
        .filter(|event| members.contains(event.uid.as_str()))
        .map(|event| event.uid.as_str()));
    let by_uid: BTreeMap<_, _> = existing
        .iter()
        .map(|event| (event.uid.as_str(), event))
        .collect();
    ensure!(wanted.iter().all(|uid| by_uid.contains_key(uid)), "FILE_HISTORY_ORDER_OBSERVATION_CHANGED: an observed event disappeared or changed timestamp during the complete read");
    // An already compatible local rank remains stable too. A changed native
    // sequence may renumber local rows, but never a known remote anchor.
    let fixed: BTreeMap<_, _> = existing
        .iter()
        .filter(|event| event.shared || (same && members.contains(event.uid.as_str())))
        .filter_map(|event| event.rank.map(|rank| (event.uid.as_str(), rank)))
        .collect();
    let reserved = reserved_ranks(existing, &fixed)?;
    let mut next = Some(0_i64);
    let mut planned = Vec::new();
    for &uid in wanted {
        let mut available = next.context("FILE_HISTORY_SOURCE_ORDER_OVERFLOW")?;
        let rank = if let Some(&rank) = fixed.get(uid) {
            ensure!(rank>=available,"FILE_HISTORY_SHARED_ORDER_CONFLICT: native order contradicts an existing source rank");
            rank
        } else {
            while reserved.contains(&available) {
                available = available
                    .checked_add(1)
                    .context("FILE_HISTORY_SOURCE_ORDER_OVERFLOW")?;
            }
            available
        };
        next = rank.checked_add(1);
        if by_uid
            .get(uid)
            .is_some_and(|event| event.rank != Some(rank))
        {
            planned.push((uid.into(), rank));
        }
    }
    Ok(planned)
}

fn reserved_ranks(existing: &[Observed], fixed: &BTreeMap<&str, i64>) -> Result<BTreeSet<i64>> {
    let mut owners = BTreeMap::new();
    for event in existing {
        if let Some(&rank) = fixed.get(event.uid.as_str()) {
            ensure!(owners.insert(rank,event.uid.as_str()).is_none(),"FILE_HISTORY_SHARED_ORDER_CONFLICT: different retained events share one fixed rank");
        }
    }
    Ok(owners.into_keys().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn event(conn: &Connection, source: &str, sid: &str, ts: i64, uid: &str, location: &str) {
        conn.execute("INSERT INTO session_events(source,session_id,ts_ms,event_uid,role,kind,text,location) VALUES(?1,?2,?3,?4,'assistant','text',?4,?5)",rusqlite::params![source,sid,ts,uid,location]).unwrap();
    }
    fn uids(conn: &Connection) -> Vec<String> {
        conn.prepare("SELECT event_uid FROM session_events ORDER BY ts_ms,id")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    }
    #[test]
    fn changed_native_bucket_updates_ranks_without_touching_row_ids_or_stable_repeats() {
        let home = tempfile::tempdir().unwrap();
        let conn = crate::open_db(&home.path().join("history.db")).unwrap();
        let tx = conn.unchecked_transaction().unwrap();
        event(&tx, "test-fixture", "s", 1, "call", "local");
        event(&tx, "test-fixture", "s", 1, "later", "local");
        event(&tx, "test-fixture", "s", 2, "next", "local");
        event(&tx, "test-fixture", "other", 1, "other-session", "local");
        event(&tx, "pi", "s", 1, "other-source", "local");
        event(&tx, "test-fixture", "s", 1, "prefix", "local");
        let unaffected = |conn: &Connection| {
            conn.prepare("SELECT id,source,ts_ms FROM session_events WHERE source != 'test-fixture' OR session_id != 's' OR ts_ms != 1 ORDER BY id").unwrap()
                .query_map([], |r| Ok((r.get::<_,i64>(0)?, r.get::<_,String>(1)?, r.get::<_,i64>(2)?))).unwrap()
                .collect::<rusqlite::Result<Vec<_>>>().unwrap()
        };
        let untouched = unaffected(&tx);
        let desired = vec![
            (1, "prefix".into()),
            (1, "call".into()),
            (1, "later".into()),
            (2, "next".into()),
        ];
        let row_ids = uids(&tx);
        assert!(reconcile_event_order(&tx, "test-fixture", "s", &desired)
            .unwrap()
            .is_empty());
        assert_eq!(uids(&tx), row_ids);
        let ordered: Vec<String> = tx.prepare("SELECT event_uid FROM session_events WHERE source='test-fixture' AND session_id='s' ORDER BY ts_ms,COALESCE(source_order,id),id").unwrap()
            .query_map([], |row| row.get(0)).unwrap().collect::<rusqlite::Result<_>>().unwrap();
        assert_eq!(ordered, ["prefix", "call", "later", "next"]);
        assert_eq!(unaffected(&tx), untouched);
        let before = uids(&tx);
        let changes = tx.total_changes();
        let revision = crate::change_feed::read_head(&tx).unwrap();
        assert!(reconcile_event_order(&tx, "test-fixture", "s", &desired)
            .unwrap()
            .is_empty());
        assert_eq!(tx.total_changes(), changes);
        assert_eq!(crate::change_feed::read_head(&tx).unwrap(), revision);
        assert_eq!(uids(&tx), before);
    }
    #[test]
    fn shared_bucket_preflight_precedes_any_retirement_and_caller_rolls_back() {
        for ownership in ["remote", "both"] {
            let home = tempfile::tempdir().unwrap();
            let conn = crate::open_db(&home.path().join("history.db")).unwrap();
            for ts in [1, 2] {
                event(
                    &conn,
                    "test-fixture",
                    "s",
                    ts,
                    &format!("old{ts}"),
                    if ts == 2 { ownership } else { "local" },
                );
            }
            conn.execute(
                "UPDATE session_events SET source_order=0 WHERE event_uid='old2'",
                [],
            )
            .unwrap();
            let original = uids(&conn);
            let original_revision = crate::change_feed::read_head(&conn).unwrap();
            {
                let tx = conn.unchecked_transaction().unwrap();
                for ts in [1, 2] {
                    event(&tx, "test-fixture", "s", ts, &format!("new{ts}"), "local");
                }
                let before = uids(&tx);
                let desired = vec![
                    (1, "new1".into()),
                    (1, "old1".into()),
                    (2, "new2".into()),
                    (2, "old2".into()),
                ];
                assert!(reconcile_event_order(&tx, "test-fixture", "s", &desired)
                    .unwrap_err()
                    .to_string()
                    .contains("SHARED_ORDER_CONFLICT"));
                assert_eq!(uids(&tx), before);
            }
            assert_eq!(uids(&conn), original);
            assert_eq!(
                crate::change_feed::read_head(&conn).unwrap(),
                original_revision
            );
        }
    }
    #[test]
    fn stable_shared_order_is_unchanged_but_repair_requires_a_transaction() {
        let home = tempfile::tempdir().unwrap();
        let conn = crate::open_db(&home.path().join("history.db")).unwrap();
        event(&conn, "test-fixture", "s", 1, "existing", "both");
        let desired = vec![(1, "existing".into())];
        assert!(reconcile_event_order(&conn, "test-fixture", "s", &desired)
            .unwrap_err()
            .to_string()
            .contains("TRANSACTION_REQUIRED"));
        let tx = conn.unchecked_transaction().unwrap();
        let changes = tx.total_changes();
        assert!(reconcile_event_order(&tx, "test-fixture", "s", &desired)
            .unwrap()
            .is_empty());
        assert!(
            tx.total_changes() > changes,
            "a complete read learns unknown order"
        );
        let learned = tx.total_changes();
        reconcile_event_order(&tx, "test-fixture", "s", &desired).unwrap();
        assert_eq!(tx.total_changes(), learned);
    }
    #[test]
    fn duplicate_native_identity_rejects_before_any_retirement() {
        for second_time in [1, 2] {
            let home = tempfile::tempdir().unwrap();
            let conn = crate::open_db(&home.path().join("history.db")).unwrap();
            let tx = conn.unchecked_transaction().unwrap();
            event(&tx, "test-fixture", "s", 1, "old", "local");
            event(&tx, "test-fixture", "s", 1, "new", "local");
            let before = uids(&tx);
            let changes = tx.total_changes();
            let desired = vec![
                (1, "new".into()),
                (second_time, "new".into()),
                (1, "old".into()),
            ];
            assert!(reconcile_event_order(&tx, "test-fixture", "s", &desired)
                .unwrap_err()
                .to_string()
                .contains("DUPLICATE_ORDER_UID"));
            assert_eq!(uids(&tx), before);
            assert_eq!(tx.total_changes(), changes);
        }
    }
    #[test]
    fn remote_unknown_ranks_are_learned_around_fixed_known_anchors() {
        let home = tempfile::tempdir().unwrap();
        let conn = crate::open_db(&home.path().join("history.db")).unwrap();
        let tx = conn.unchecked_transaction().unwrap();
        event(&tx, "test-fixture", "s", 1, "anchor", "remote");
        tx.execute(
            "UPDATE session_events SET source_order=7 WHERE event_uid='anchor'",
            [],
        )
        .unwrap();
        event(&tx, "test-fixture", "s", 1, "append", "both");
        let desired = vec![(1, "anchor".into()), (1, "append".into())];
        reconcile_event_order(&tx, "test-fixture", "s", &desired).unwrap();
        let ranks: Vec<i64> = tx
            .prepare("SELECT source_order FROM session_events ORDER BY source_order")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(ranks, [7, 8]);
        let changes = tx.total_changes();
        reconcile_event_order(&tx, "test-fixture", "s", &desired).unwrap();
        assert_eq!(tx.total_changes(), changes);
        assert!(reconcile_event_order(
            &tx,
            "test-fixture",
            "s",
            &[(1, "append".into()), (1, "anchor".into())]
        )
        .unwrap_err()
        .to_string()
        .contains("SHARED_ORDER_CONFLICT"));
    }

    #[test]
    fn retained_remote_survivor_rank_is_never_given_to_another_event() {
        let home = tempfile::tempdir().unwrap();
        let conn = crate::open_db(&home.path().join("history.db")).unwrap();
        let tx = conn.unchecked_transaction().unwrap();
        event(&tx, "test-fixture", "s", 1, "survivor", "remote");
        tx.execute(
            "UPDATE session_events SET source_order=0 WHERE event_uid='survivor'",
            [],
        )
        .unwrap();
        event(&tx, "test-fixture", "s", 1, "native", "local");
        reconcile_event_order(&tx, "test-fixture", "s", &[(1, "native".into())]).unwrap();
        let ranks: Vec<(String, i64)> = tx
            .prepare("SELECT event_uid,source_order FROM session_events ORDER BY source_order")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(ranks, vec![("survivor".into(), 0), ("native".into(), 1)]);
    }

    #[test]
    fn stale_desired_timestamp_rejects_before_changing_any_rank() {
        let home = tempfile::tempdir().unwrap();
        let conn = crate::open_db(&home.path().join("history.db")).unwrap();
        let tx = conn.unchecked_transaction().unwrap();
        event(&tx, "test-fixture", "s", 1, "first", "local");
        event(&tx, "test-fixture", "s", 2, "moved", "local");
        let changes = tx.total_changes();
        assert!(reconcile_event_order(
            &tx,
            "test-fixture",
            "s",
            &[(1, "first".into()), (1, "moved".into())]
        )
        .unwrap_err()
        .to_string()
        .contains("OBSERVATION_CHANGED"));
        assert_eq!(tx.total_changes(), changes);
    }

    #[test]
    fn maximum_known_rank_remains_a_stable_no_op() {
        let home = tempfile::tempdir().unwrap();
        let conn = crate::open_db(&home.path().join("history.db")).unwrap();
        let tx = conn.unchecked_transaction().unwrap();
        event(&tx, "test-fixture", "s", 1, "maximum", "remote");
        tx.execute(
            "UPDATE session_events SET source_order=?1 WHERE event_uid='maximum'",
            [i64::MAX],
        )
        .unwrap();
        let changes = tx.total_changes();
        reconcile_event_order(&tx, "test-fixture", "s", &[(1, "maximum".into())]).unwrap();
        assert_eq!(tx.total_changes(), changes);
    }
}
