//! Retiring a fed kind, and which positions survive it.
//!
//! Removing an exported kind is the one schema change that relabels a store's
//! feed. A consumer that drained every kind may hold rows of the removed kind,
//! and no tombstone can reach it for a kind the feed no longer names, so it
//! replays from [`Watermark::START`] to reconcile. Nothing is renumbered, so a
//! consumer that left kinds out keeps its positions.
//!
//! The store therefore draws a new epoch and records the old one as continued
//! by it, through the head at the time (`change_feed_epochs`). A watermark of
//! the old epoch resumes a drain that leaves out at least one current kind. A
//! drain over every current kind refuses it as before, whether it passed
//! `kinds: None` or listed them all: the same list named the removed kind
//! before the upgrade. An external watermark does not record the kinds it was
//! drained with, so a consumer that itself listed the removed kind, and stopped
//! when it could no longer name it, drops that kind's rows on its own. Named
//! cursors record their set: one bound to every kind, or to a set naming a
//! removed kind, is dropped; the rest are kept.

use super::*;

/// How many retirements [`continues`] follows from a watermark's epoch to
/// the store's. Each link is one release removing kinds; the bound only keeps
/// a corrupt table from looping.
const MAX_EPOCH_LINKS: usize = 64;

/// Start a new epoch for the retirement of `removed`, recording the current
/// one as continued by it, and drop the cursors that drained a removed kind.
pub(super) fn retire_kinds(conn: &Connection, removed: &[String]) -> Result<()> {
    let head = read_head(conn)?;
    let epoch = head.epoch as i64;
    // Odd and nonzero, like the random identity a store starts with.
    let successor = if epoch == i64::MAX {
        -i64::MAX
    } else {
        epoch + 2
    };
    conn.execute(
        "INSERT OR REPLACE INTO change_feed_epochs \
         (epoch, successor, through_revision, retired_kinds) VALUES (?, ?, ?, ?)",
        params![epoch, successor, head.revision as i64, removed.join(",")],
    )?;
    conn.execute(
        "UPDATE change_feed_store SET epoch = ? WHERE singleton = 1",
        [successor],
    )?;
    retire_cursors(conn, removed)
}

/// Drop every cursor bound to all kinds or to a set naming a removed kind.
/// A kept cursor whose set now covers every remaining kind is rebound to
/// `*`, the spelling a drain over every kind compares it with.
fn retire_cursors(conn: &Connection, removed: &[String]) -> Result<()> {
    let cursors: Vec<(String, String)> = conn
        .prepare(&format!(
            "SELECT name, {KINDS_COLUMN} FROM consumer_cursors"
        ))?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    let every = KindSet::normalize(None).stored();
    for (name, kinds) in cursors {
        let named: Vec<&str> = kinds.split(',').collect();
        if kinds == "*" || named.iter().any(|kind| removed.iter().any(|r| r == kind)) {
            conn.execute("DELETE FROM consumer_cursors WHERE name = ?", [&name])?;
        } else if named.len() == ChangeKind::ALL.len()
            && ChangeKind::ALL
                .iter()
                .all(|kind| named.contains(&kind.as_str()))
        {
            conn.execute(
                &format!("UPDATE consumer_cursors SET {KINDS_COLUMN} = ? WHERE name = ?"),
                params![every, name],
            )?;
        }
    }
    Ok(())
}

impl KindSet {
    /// Whether the set leaves out a current kind, and so cannot be the set
    /// that, listed before a retirement, included the retired kind.
    fn leaves_out_a_kind(&self) -> bool {
        self.kinds != ChangeKind::ALL
    }
}

/// Whether `from`, a watermark of another epoch than `head`'s, still names a
/// position in this store for a drain over `kinds`. It does when the set
/// leaves out a current kind, a chain of retirements leads from the
/// watermark's epoch to the store's, and it was issued before the first.
fn continues(conn: &Connection, from: Watermark, head: u64, kinds: &KindSet) -> Result<bool> {
    if from.epoch == head {
        return Ok(true);
    }
    if !kinds.leaves_out_a_kind() || !epochs_recorded(conn)? {
        return Ok(false);
    }
    let mut epoch = from.epoch;
    for _ in 0..MAX_EPOCH_LINKS {
        let Some((successor, through)) = link(conn, epoch)? else {
            return Ok(false);
        };
        if from.revision > through {
            return Ok(false);
        }
        if successor == head {
            return Ok(true);
        }
        epoch = successor;
    }
    Ok(false)
}

/// [`Error::WatermarkAheadOfStore`] unless `from` is a position in this store
/// for a drain over `kinds`; see [`continues`]. The sentinels name no store
/// and always pass.
pub(super) fn check_issued(
    conn: &Connection,
    from: Watermark,
    head: Watermark,
    kinds: &KindSet,
) -> std::result::Result<(), Error> {
    if from == Watermark::START
        || from == Watermark::CONSUMER
        || continues(conn, from, head.epoch, kinds).map_err(Error::query)?
    {
        return Ok(());
    }
    Err(Error::WatermarkAheadOfStore(format!(
        "changes_since: watermark {} was not issued by this store (epoch {}, this \
         store's is {}); the database was reset or replaced, resync from \
         Watermark::START",
        from.revision, from.epoch, head.epoch
    )))
}

/// A database a writable open of this version has not migrated yet has no
/// retirements recorded, and a read-only handle reads it as it is.
fn epochs_recorded(conn: &Connection) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master \
         WHERE type = 'table' AND name = 'change_feed_epochs')",
        [],
        |row| row.get(0),
    )?)
}

/// The epoch that continued `epoch`, and the last revision issued under it.
fn link(conn: &Connection, epoch: u64) -> Result<Option<(u64, u64)>> {
    Ok(conn
        .query_row(
            "SELECT successor, through_revision FROM change_feed_epochs WHERE epoch = ?",
            [epoch as i64],
            |row| Ok((row.get::<_, i64>(0)? as u64, row.get::<_, i64>(1)? as u64)),
        )
        .optional()?)
}

impl SessionStore {
    /// Whether [`SessionStore::changes_since`] resumes `query` from `from`
    /// rather than failing with [`Error::WatermarkAheadOfStore`]: `from` (or,
    /// for [`Watermark::CONSUMER`], the query's named cursor) was issued by
    /// this store and is not past its head. For an embedder that keeps the
    /// epoch it last read beside its own positions and decides between
    /// resuming and a resync before draining. A watermark issued before a kind
    /// was retired resumes only a query that leaves out a current kind. The
    /// other errors `changes_since` would return for `query` are returned here.
    pub fn resumes_from(
        &self,
        from: Watermark,
        query: &ChangeQuery,
    ) -> std::result::Result<bool, Error> {
        if from == Watermark::START {
            return Ok(true);
        }
        let mut conn = self.read_conn()?;
        if !conn
            .gate("change-feed", schema_is_current)
            .map_err(Error::query)?
        {
            // Nothing in a store from before the feed is a position yet.
            return Ok(false);
        }
        let kinds = KindSet::normalize(query.kinds.clone());
        let (start, head, _) =
            resolve_start_and_head(&conn, from, query.consumer.as_deref(), &kinds)?;
        let issued = from == Watermark::CONSUMER
            || continues(&conn, from, head.epoch, &kinds).map_err(Error::query)?;
        Ok(issued && start.revision <= head.revision)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session_store::StoreOptions;
    use std::path::Path;

    fn store(dir: &Path) -> (SessionStore, Connection) {
        let db = dir.join("ai-history.db");
        let store = SessionStore::open(StoreOptions::default().db_path(db.clone())).unwrap();
        (store, open_db(&db).unwrap())
    }

    fn insert_event(conn: &Connection, uid: &str) {
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, message_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 's1', 'm1', 10, 'assistant', 'text', 'x', ?)",
            [uid],
        )
        .unwrap();
    }

    fn events() -> ChangeQuery {
        ChangeQuery::default().kinds([ChangeKind::SessionEvent])
    }

    fn drained(store: &SessionStore, from: Watermark, query: ChangeQuery) -> Vec<String> {
        store
            .changes_since(from, query)
            .unwrap()
            .map(|change| change.unwrap().record_key)
            .collect()
    }

    fn commit(store: &SessionStore, query: ChangeQuery) {
        let mut changes = store.changes_since(Watermark::CONSUMER, query).unwrap();
        while changes.next().is_some() {}
        changes.commit().unwrap();
    }

    /// As an open of a release that stopped feeding a kind finds the store:
    /// the stored fingerprints still name it.
    fn retire_a_kind(conn: &Connection) {
        retire_a_kind_and(conn, |_| {});
    }

    /// [`retire_a_kind`], with `change` applied to the stored fingerprints too.
    fn retire_a_kind_and(conn: &Connection, change: impl FnOnce(&mut BTreeMap<String, String>)) {
        let stored: String = conn
            .query_row(
                &format!("SELECT {EXPORT_SCHEMA_DIGEST_COLUMN} FROM change_feed_store"),
                [],
                |row| row.get(0),
            )
            .unwrap();
        let mut map: BTreeMap<String, String> = serde_json::from_str(&stored).unwrap();
        map.insert("retired_kind".to_string(), "retired-schema".to_string());
        change(&mut map);
        conn.execute(
            &format!("UPDATE change_feed_store SET {EXPORT_SCHEMA_DIGEST_COLUMN} = ?"),
            [serde_json::to_string(&map).unwrap()],
        )
        .unwrap();
        schema::init_schema(conn).unwrap();
    }

    fn cursors(conn: &Connection) -> Vec<(String, String)> {
        conn.prepare("SELECT name, kinds FROM consumer_cursors ORDER BY name")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    }

    /// A drain over named kinds resumes from a watermark issued before the
    /// retirement and reads only what came after it; a drain over every kind
    /// is told to resync.
    #[test]
    fn a_named_kinds_watermark_survives_a_retired_kind() {
        let dir = tempfile::tempdir().unwrap();
        let (store, conn) = store(dir.path());
        insert_event(&conn, "before");
        let issued = store.head_revision().unwrap();

        retire_a_kind(&conn);
        insert_event(&conn, "after");

        assert_ne!(store.head_revision().unwrap().epoch, issued.epoch);
        assert!(store.resumes_from(issued, &events()).unwrap());
        let keys = drained(&store, issued, events());
        assert_eq!(keys.len(), 1, "only the event written after: {keys:?}");
        assert!(keys[0].contains("after"));

        // Every current kind, listed or not, named the retired one before.
        let listed = || ChangeQuery::default().kinds(ChangeKind::ALL.iter().copied());
        for query in [ChangeQuery::default, listed] {
            assert!(!store.resumes_from(issued, &query()).unwrap());
            assert!(matches!(
                store.changes_since(issued, query()),
                Err(Error::WatermarkAheadOfStore(_))
            ));
        }
    }

    /// A surviving kind whose export changed in the same upgrade is restamped
    /// above the head, so a position kept across the retirement reads it again.
    #[test]
    fn a_kept_position_rereads_a_surviving_kind_whose_export_changed() {
        let dir = tempfile::tempdir().unwrap();
        let (store, conn) = store(dir.path());
        insert_event(&conn, "before");
        let issued = store.head_revision().unwrap();

        retire_a_kind_and(&conn, |map| {
            map.insert(
                ChangeKind::SessionEvent.as_str().to_string(),
                "an-earlier-export".to_string(),
            );
        });

        let keys = drained(&store, issued, events());
        assert_eq!(keys.len(), 1, "{keys:?}");
        assert!(keys[0].contains("before"));
    }

    /// `resumes_from` answers for a named cursor from the cursor itself: one
    /// past the head, as a restored database leaves it, does not resume.
    #[test]
    fn resumes_from_reads_a_named_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let (store, conn) = store(dir.path());
        insert_event(&conn, "before");
        commit(&store, events().consumer("delivery"));
        assert!(store
            .resumes_from(Watermark::CONSUMER, &events().consumer("delivery"))
            .unwrap());

        conn.execute("UPDATE consumer_cursors SET revision = revision + 10", [])
            .unwrap();
        assert!(!store
            .resumes_from(Watermark::CONSUMER, &events().consumer("delivery"))
            .unwrap());
        assert!(matches!(
            store.changes_since(Watermark::CONSUMER, events().consumer("delivery")),
            Err(Error::WatermarkAheadOfStore(_))
        ));
    }

    /// Retirements chain: a watermark two retirements old still resumes, and
    /// one past the head of its epoch, which that epoch never issued, does not.
    #[test]
    fn retirements_chain_and_bound_the_revisions_they_carry() {
        let dir = tempfile::tempdir().unwrap();
        let (store, conn) = store(dir.path());
        insert_event(&conn, "first");
        let issued = store.head_revision().unwrap();
        retire_a_kind(&conn);
        retire_a_kind(&conn);

        assert!(store.resumes_from(issued, &events()).unwrap());
        let beyond = Watermark {
            revision: issued.revision + 1,
            ..issued
        };
        assert!(!store.resumes_from(beyond, &events()).unwrap());
        let foreign = Watermark {
            epoch: issued.epoch ^ 0x10,
            ..issued
        };
        assert!(!store.resumes_from(foreign, &events()).unwrap());
    }

    /// Cursors that drained every kind, or named the removed one, are
    /// dropped; one bound to named kinds keeps its position, and one whose
    /// set now covers every remaining kind is rebound to `*`.
    #[test]
    fn named_cursors_keep_their_positions_unless_they_read_the_removed_kind() {
        let dir = tempfile::tempdir().unwrap();
        let (store, conn) = store(dir.path());
        insert_event(&conn, "before");
        commit(&store, ChangeQuery::default().consumer("everything"));
        commit(&store, events().consumer("events"));
        let every_remaining = KindSet::normalize(None)
            .kinds
            .iter()
            .map(|kind| kind.as_str())
            .collect::<Vec<_>>()
            .join(",");
        let revision = store.head_revision().unwrap().revision as i64;
        conn.execute(
            "INSERT INTO consumer_cursors (name, revision, updated_ms, kinds) VALUES \
             ('named-removed', ?1, 0, 'retired_kind,session_event'), \
             ('all-but-removed', ?1, 0, ?2)",
            params![revision, every_remaining],
        )
        .unwrap();

        retire_a_kind(&conn);
        insert_event(&conn, "after");

        assert_eq!(
            cursors(&conn),
            [
                ("all-but-removed".to_string(), "*".to_string()),
                ("events".to_string(), "session_event".to_string()),
            ]
        );
        let keys = drained(&store, Watermark::CONSUMER, events().consumer("events"));
        assert_eq!(keys.len(), 1, "{keys:?}");
        let keys = drained(
            &store,
            Watermark::CONSUMER,
            ChangeQuery::default().consumer("all-but-removed"),
        );
        assert!(keys.iter().all(|key| !key.contains("before")), "{keys:?}");
    }
}
