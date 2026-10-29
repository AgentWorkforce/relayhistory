//! In-place repair of source-observed order after a complete native replay.
//! Ranks are independent of event identity, recorded time and canonical row IDs.
use anyhow::Result;
use rusqlite::Connection;
use std::collections::{BTreeMap, BTreeSet};

/// Call after normalization and retirement inside the caller's transaction.
/// `desired` contains every emitted event in native order. Changed timestamp
/// buckets receive source ranks in place; remotely backed conflicting buckets
/// reject the transaction. Unchanged/appended buckets keep ranks and revisions.
/// The empty return preserves the adapter replay interface without requesting
/// row retirement/reinsertion, so native event and unnamed-turn identities stay
/// stable through reorders.
pub(crate) fn reconcile_event_order(
    conn: &Connection,
    source: &str,
    sid: &str,
    desired: &[(i64, String)],
) -> Result<BTreeSet<i64>> {
    anyhow::ensure!(
        !conn.is_autocommit(),
        "FILE_HISTORY_ORDER_TRANSACTION_REQUIRED"
    );
    let unique: BTreeSet<_> = desired.iter().map(|(_, uid)| uid.as_str()).collect();
    anyhow::ensure!(
        unique.len() == desired.len(),
        "FILE_HISTORY_DUPLICATE_ORDER_UID: desired event identities must be unique"
    );
    let mut expected = BTreeMap::<i64, Vec<&str>>::new();
    for (ts, uid) in desired {
        expected.entry(*ts).or_default().push(uid);
    }
    let membership: BTreeSet<_> = desired
        .iter()
        .map(|(ts, uid)| (*ts, uid.as_str()))
        .collect();
    let mut observed = BTreeMap::<i64, Vec<String>>::new();
    let mut statement = conn.prepare(
        "SELECT ts_ms,event_uid FROM session_events WHERE source=?1 AND session_id=?2 ORDER BY ts_ms,COALESCE(source_order,id),id",
    )?;
    let rows = statement.query_map([source, sid], |r| {
        Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
    })?;
    for row in rows {
        let (ts, uid) = row?;
        if membership.contains(&(ts, uid.as_str())) {
            observed.entry(ts).or_default().push(uid);
        }
    }
    let affected: BTreeSet<_> = expected
        .into_iter()
        .filter_map(|(ts, wanted)| {
            let existing = observed.get(&ts).map(Vec::as_slice).unwrap_or_default();
            (!wanted
                .iter()
                .copied()
                .eq(existing.iter().map(String::as_str)))
            .then_some(ts)
        })
        .collect();
    if affected.is_empty() {
        return Ok(affected);
    }
    let times = serde_json::to_string(&affected)?;
    let shared: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM session_events WHERE source=?1 AND session_id=?2 AND ts_ms IN (SELECT value FROM json_each(?3)) AND location IS NOT 'local')",
        rusqlite::params![source,sid,times], |r| r.get(0),
    )?;
    anyhow::ensure!(!shared,"FILE_HISTORY_SHARED_ORDER_CONFLICT: native event order changed in a timestamp bucket backed by remote evidence; prior snapshot retained, retry after ownership is reconciled");
    // Update only ordering evidence, preserving canonical IDs, tool joins,
    // unnamed-turn keys and the rest of each row. The caller has already
    // normalized every desired event inside this same transaction.
    let mut ranks = BTreeMap::<i64, i64>::new();
    for (ts, uid) in desired {
        let rank = ranks.entry(*ts).or_default();
        if affected.contains(ts) {
            conn.execute("UPDATE session_events SET source_order=?1 WHERE source=?2 AND session_id=?3 AND event_uid=?4 AND source_order IS NOT ?1",
                rusqlite::params![*rank,source,sid,uid])?;
        }
        *rank += 1;
    }
    // No retirement is needed, so the caller must not replay any bucket.
    Ok(BTreeSet::new())
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
        assert_eq!(tx.total_changes(), changes);
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
}
