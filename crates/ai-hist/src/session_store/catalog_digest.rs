//! Catalog digests for [`super::SyncReport::changed`].
//!
//! An unmoved change-feed head hashes nothing: that head proves the rolling
//! map still describes the catalog. A moved head in the same feed epoch
//! rehashes only the `sessions` rows stamped since the previous head, and
//! drops keys a `kind = 'session'` tombstone names. The map has to forget a
//! deleted key. The change list walks only the map it is handed, so a later
//! insert of the same covered columns is reported only when the key is gone.
//! A store with no feed, or a head from another epoch, still hashes every
//! catalog row — those revisions do not name the rows in the rolling map.
//! Hashing a row whose covered columns did not move is harmless. Skipping
//! one whose columns did move is not.

use super::{CatalogFingerprint, Error};
use rusqlite::Connection;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Arc;

type SessionKey = (String, String);
type DigestMap = BTreeMap<SessionKey, [u8; 32]>;

/// The catalog columns the digest covers: everything a sweep, shallow
/// discovery or the identity refresh can write, except the two text
/// excerpts, which are the only columns whose size is not a few bytes.
const CATALOG_FINGERPRINT_COLUMNS: &str = "source_stamp, last_activity_ms, first_activity_ms, \
     discovery_state, parser_version, project_key, project_key_method, cwd, git_branch, \
     raw_path, models_json, originator, agent_version, repo_url, initial_commit, \
     workspace_roots_json, title";

const TOMBSTONE_SQL: &str = "SELECT source, session_id FROM evidence_tombstones \
     WHERE kind = 'session' AND revision > ?1";

// Rows hashed on this thread. Production builds do not keep the counter.
#[cfg(test)]
thread_local! {
    static DIGEST_READS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(super) fn digest_reads() -> u64 {
    DIGEST_READS.with(|reads| reads.get())
}

#[cfg(test)]
pub(super) fn reset_digest_reads() {
    DIGEST_READS.with(|reads| reads.set(0));
}

macro_rules! count_digest {
    () => {
        #[cfg(test)]
        {
            DIGEST_READS.with(|reads| reads.set(reads.get() + 1));
        }
    };
}

/// Every catalog row's digest. Used when the feed cannot name a delta.
pub(super) fn every_row(conn: &Connection) -> Result<Arc<DigestMap>, Error> {
    let sql = format!("SELECT source, session_id, {CATALOG_FINGERPRINT_COLUMNS} FROM sessions");
    Ok(Arc::new(collect_digests(conn, &sql, [])?))
}

/// The rolling map after `head`. Rows stamped since `before` are rehashed
/// when those revisions belong to the same feed epoch; otherwise the whole
/// catalog is hashed.
pub(super) fn rows_since(
    conn: &Connection,
    before: &CatalogFingerprint,
    head: Option<crate::change_feed::Watermark>,
) -> Result<Arc<DigestMap>, Error> {
    let Some(since) = comparable_floor(before, head) else {
        return every_row(conn);
    };
    let (dropped, stamped) = revisions_since(conn, since)?;
    if dropped.is_empty() && stamped.is_empty() {
        return Ok(Arc::clone(&before.rows));
    }
    let mut rows = (*before.rows).clone();
    // Tombstones first, then live rows. A tombstone applied after the upsert
    // would drop a key `sessions` still holds.
    for key in dropped {
        rows.remove(&key);
    }
    for (key, digest) in stamped {
        rows.insert(key, digest);
    }
    Ok(Arc::new(rows))
}

/// Advance an exclusively held baseline, comparing only the delta. Unlike
/// `rows_since`, this does not copy or walk unrelated historical keys.
pub(super) fn advance(
    conn: &Connection,
    before: &mut CatalogFingerprint,
) -> Result<Vec<super::SessionRef>, Error> {
    let head = super::catalog_head(conn)?;
    if head.is_some() && head == before.head {
        return Ok(Vec::new());
    }
    let Some(since) = comparable_floor(before, head) else {
        let after = super::catalog_fingerprint(conn)?;
        let changed = super::catalog_changes(before, &after);
        *before = after;
        return Ok(changed);
    };
    let (dropped, stamped) = revisions_since(conn, since)?;
    let rows = Arc::make_mut(&mut before.rows);
    for key in dropped {
        // A delete/reinsert inside this bracket compares covered columns as
        // before; a deletion observed by an earlier call has removed its key.
        if !stamped.contains_key(&key) {
            rows.remove(&key);
        }
    }
    let mut changed = Vec::new();
    for (key, digest) in stamped {
        if rows.get(&key) != Some(&digest) {
            if let Some(source) = super::Source::parse(&key.0) {
                changed.push(super::SessionRef::id(source, key.1.clone()));
            }
            rows.insert(key, digest);
        }
    }
    before.head = head;
    Ok(changed)
}

/// The revision floor of a delta, or `None` when the rolling map has to be
/// rebuilt. A missing feed, a new epoch, or a clock that moved backwards
/// does not identify rows by `revision > floor`.
fn comparable_floor(
    before: &CatalogFingerprint,
    head: Option<crate::change_feed::Watermark>,
) -> Option<i64> {
    let head = head?;
    let previous = before.head?;
    if previous.epoch != head.epoch || head.revision < previous.revision {
        return None;
    }
    i64::try_from(previous.revision).ok()
}

/// Tombstones and stamped session rows newer than `since`, from one
/// snapshot. The two statements have to agree: seeing a delete without the
/// row that replaced it (or the row without its tombstone) keeps a digest
/// the catalog no longer has, and a later insert of those same covered
/// columns is then not a change.
fn revisions_since(conn: &Connection, since: i64) -> Result<(Vec<SessionKey>, DigestMap), Error> {
    if conn.is_autocommit() {
        let snapshot = conn.unchecked_transaction().map_err(Error::sql)?;
        let found = read_revisions(&snapshot, since)?;
        snapshot.commit().map_err(Error::sql)?;
        return Ok(found);
    }
    read_revisions(conn, since)
}

fn read_revisions(conn: &Connection, since: i64) -> Result<(Vec<SessionKey>, DigestMap), Error> {
    let dropped = tombstoned_sessions(conn, since)?;
    let sql = format!(
        "SELECT source, session_id, {CATALOG_FINGERPRINT_COLUMNS} \
         FROM sessions WHERE revision > ?1"
    );
    let stamped = collect_digests(conn, &sql, [since])?;
    Ok((dropped, stamped))
}

fn tombstoned_sessions(conn: &Connection, since: i64) -> Result<Vec<SessionKey>, Error> {
    let mut statement = conn.prepare_cached(TOMBSTONE_SQL).map_err(Error::sql)?;
    let mut rows = statement.query([since]).map_err(Error::sql)?;
    let mut keys = Vec::new();
    while let Some(row) = rows.next().map_err(Error::sql)? {
        let source: String = row.get(0).map_err(Error::sql)?;
        let session_id: String = row.get(1).map_err(Error::sql)?;
        keys.push((source, session_id));
    }
    Ok(keys)
}

fn collect_digests(
    conn: &Connection,
    sql: &str,
    params: impl rusqlite::Params,
) -> Result<DigestMap, Error> {
    let mut statement = conn.prepare_cached(sql).map_err(Error::sql)?;
    let columns = statement.column_count();
    let mut rows = statement.query(params).map_err(Error::sql)?;
    let mut digests = BTreeMap::new();
    while let Some(row) = rows.next().map_err(Error::sql)? {
        let source: String = row.get(0).map_err(Error::sql)?;
        let session_id: String = row.get(1).map_err(Error::sql)?;
        let digest = digest_covered(row, columns).map_err(Error::sql)?;
        digests.insert((source, session_id), digest);
    }
    Ok(digests)
}

fn digest_covered(row: &rusqlite::Row<'_>, columns: usize) -> rusqlite::Result<[u8; 32]> {
    let mut digest = Sha256::new();
    for index in 2..columns {
        // Type and value both, with a separator, so a NULL and an empty
        // string differ and a shifted column boundary cannot read as the
        // same row.
        match row.get_ref(index)? {
            rusqlite::types::ValueRef::Null => digest.update(b"n|"),
            rusqlite::types::ValueRef::Integer(value) => {
                digest.update(b"i");
                digest.update(value.to_le_bytes());
                digest.update(b"|");
            }
            rusqlite::types::ValueRef::Real(value) => {
                digest.update(b"r");
                digest.update(value.to_le_bytes());
                digest.update(b"|");
            }
            rusqlite::types::ValueRef::Text(bytes) | rusqlite::types::ValueRef::Blob(bytes) => {
                digest.update(b"t");
                digest.update((bytes.len() as u64).to_le_bytes());
                digest.update(bytes);
                digest.update(b"|");
            }
        }
    }
    count_digest!();
    Ok(digest.finalize().into())
}

#[cfg(test)]
mod tests {
    use super::super::{
        catalog_fingerprint, changes_under_lock, open_db, SessionRef, SessionStore, Source,
        StoreOptions,
    };
    use std::sync::Arc;

    fn key(source: &str, session_id: &str) -> super::SessionKey {
        (source.to_string(), session_id.to_string())
    }

    fn opened() -> (tempfile::TempDir, rusqlite::Connection) {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("ai-history.db");
        let home = dir.path().to_path_buf();
        SessionStore::open(StoreOptions {
            db_path: Some(db.clone()),
            roots: Some(crate::paths::ProviderRoots::from_home(
                home.clone(),
                home.join("opencode.db"),
            )),
            ..StoreOptions::default()
        })
        .unwrap();
        (dir, open_db(&db).unwrap())
    }

    /// A moved head rehashes the sessions the feed stamped, and nothing
    /// else. An unmoved head hashes nothing. A delete forgets the key, so
    /// putting the same covered columns back is a change.
    #[test]
    fn a_sweep_hashes_only_the_catalog_rows_the_change_feed_stamped() {
        let (_dir, conn) = opened();
        conn.execute_batch(
            "INSERT INTO sessions (session_id, source, discovery_state) \
             VALUES ('one', 'claude', 'full'), ('two', 'codex', 'full')",
        )
        .unwrap();
        let one = key("claude", "one");
        let before = catalog_fingerprint(&conn).unwrap();
        assert!(before.head.is_some(), "the feed is installed by open");
        assert_eq!(before.rows.len(), 2);
        let original = *before.rows.get(&one).unwrap();

        super::reset_digest_reads();
        let (same, changed) = changes_under_lock(&conn, &before).unwrap();
        assert!(changed.is_empty());
        assert_eq!(
            super::digest_reads(),
            0,
            "an unmoved head hashed catalog rows"
        );
        assert!(Arc::ptr_eq(&same.rows, &before.rows));

        super::reset_digest_reads();
        conn.execute(
            "UPDATE sessions SET cwd = '/elsewhere' WHERE session_id = 'two'",
            [],
        )
        .unwrap();
        let (after, changed) = changes_under_lock(&conn, &before).unwrap();
        assert_eq!(changed, vec![SessionRef::id(Source::Codex, "two")]);
        assert_eq!(super::digest_reads(), 1, "unchanged sessions were hashed");
        assert_eq!(after.rows.get(&one), Some(&original));

        super::reset_digest_reads();
        conn.execute(
            "UPDATE sessions SET first_prompt = 'hello' WHERE session_id = 'one'",
            [],
        )
        .unwrap();
        let (after, changed) = changes_under_lock(&conn, &after).unwrap();
        assert!(
            changed.is_empty(),
            "an excerpt is not a catalog change: {changed:?}"
        );
        assert_eq!(
            super::digest_reads(),
            1,
            "an excerpt write hashed more than the stamped row"
        );

        super::reset_digest_reads();
        conn.execute("DELETE FROM sessions WHERE session_id = 'one'", [])
            .unwrap();
        let (after, changed) = changes_under_lock(&conn, &after).unwrap();
        assert!(
            changed.is_empty(),
            "a delete is not a catalog change: {changed:?}"
        );
        assert!(
            !after.rows.contains_key(&one),
            "the rolling map kept the deleted key"
        );
        assert_eq!(super::digest_reads(), 0, "a delete hashed catalog rows");
        let tombstones: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM evidence_tombstones \
                 WHERE kind = 'session' AND session_id = 'one'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(tombstones, 1, "the delete left no session tombstone");

        super::reset_digest_reads();
        conn.execute(
            "INSERT INTO sessions (session_id, source, discovery_state) \
             VALUES ('one', 'claude', 'full')",
            [],
        )
        .unwrap();
        let (after, changed) = changes_under_lock(&conn, &after).unwrap();
        assert_eq!(changed, vec![SessionRef::id(Source::Claude, "one")]);
        assert_eq!(after.rows.get(&one), Some(&original));
        assert_eq!(
            super::digest_reads(),
            1,
            "the reinsert hashed other sessions"
        );

        let head = after.head.expect("feed");
        let mut foreign = after.clone();
        foreign.head = Some(crate::change_feed::Watermark {
            revision: head.revision,
            epoch: head.epoch.wrapping_add(1),
        });
        super::reset_digest_reads();
        let (rescanned, changed) = changes_under_lock(&conn, &foreign).unwrap();
        assert!(changed.is_empty(), "a new epoch is not a catalog change");
        assert_eq!(
            super::digest_reads(),
            after.rows.len() as u64,
            "a new epoch did not hash the whole catalog"
        );
        assert!(!Arc::ptr_eq(&rescanned.rows, &foreign.rows));

        let unfed = super::super::CatalogFingerprint {
            head: None,
            rows: Arc::clone(&after.rows),
        };
        super::reset_digest_reads();
        let (rescanned, changed) = changes_under_lock(&conn, &unfed).unwrap();
        assert!(
            changed.is_empty(),
            "rehashing a store with no feed reports nothing new"
        );
        assert_eq!(super::digest_reads(), after.rows.len() as u64);
        assert!(!Arc::ptr_eq(&rescanned.rows, &unfed.rows));
    }
}
