//! Retiring a fed kind, and which positions survive it.
//!
//! Removing an exported kind is the one schema change that relabels a store's
//! feed. A consumer that drained every kind may hold rows of the removed kind,
//! and no tombstone can reach it for a kind the feed no longer names, so it
//! replays from [`Watermark::START`] to reconcile. A consumer that names its
//! kinds cannot have named the removed one, and nothing it reads was
//! renumbered, so its positions stay valid.
//!
//! The store therefore draws a new epoch and records the old one as continued
//! by it, through the head at the time (`change_feed_epochs`). A watermark of
//! the old epoch resumes a drain over named kinds; a drain over every kind
//! refuses it as before. Named cursors follow the same rule: one bound to
//! every kind, or to a set naming a removed kind, is dropped; one bound to a
//! named set is kept.

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

/// Whether `from`, a watermark of another epoch than `head`'s, still names a
/// position in this store for a drain over `named_kinds` (`false`: every
/// kind). It does when a chain of retirements leads from its epoch to the
/// store's and it was issued before the first of them.
pub(super) fn continues(
    conn: &Connection,
    from: Watermark,
    head: u64,
    named_kinds: bool,
) -> Result<bool> {
    if from.epoch == head {
        return Ok(true);
    }
    if !named_kinds || !epochs_recorded(conn)? {
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
/// for a drain over `named_kinds`; see [`continues`]. The sentinels name no
/// store and always pass.
pub(super) fn check_issued(
    conn: &Connection,
    from: Watermark,
    head: Watermark,
    named_kinds: bool,
) -> std::result::Result<(), Error> {
    if from == Watermark::START
        || from == Watermark::CONSUMER
        || continues(conn, from, head.epoch, named_kinds).map_err(Error::query)?
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
    /// rather than failing with [`Error::WatermarkAheadOfStore`]: `from` was
    /// issued by this store and is not past its head. For an embedder that
    /// keeps the epoch it last read beside its own positions and decides
    /// between resuming and a resync before draining. A watermark issued
    /// before a kind was retired resumes a query over named kinds only.
    pub fn resumes_from(
        &self,
        from: Watermark,
        query: &ChangeQuery,
    ) -> std::result::Result<bool, Error> {
        if from == Watermark::START || from == Watermark::CONSUMER {
            return Ok(true);
        }
        let head = self.head_revision()?;
        if from.revision > head.revision {
            return Ok(false);
        }
        let conn = self.read_conn()?;
        epochs::continues(&conn, from, head.epoch, query.kinds.is_some()).map_err(Error::query)
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
        let stored: String = conn
            .query_row(
                &format!("SELECT {EXPORT_SCHEMA_DIGEST_COLUMN} FROM change_feed_store"),
                [],
                |row| row.get(0),
            )
            .unwrap();
        let mut map: BTreeMap<String, String> = serde_json::from_str(&stored).unwrap();
        map.insert("retired_kind".to_string(), "retired-schema".to_string());
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

        assert!(!store.resumes_from(issued, &ChangeQuery::default()).unwrap());
        assert!(matches!(
            store.changes_since(issued, ChangeQuery::default()),
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
