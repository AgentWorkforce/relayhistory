//! Every session identity the store holds evidence for, keyset-paged.
//!
//! The catalog lists the sessions discovery found, but evidence can arrive
//! for a session the catalog never names: a prompt from a provider's prompt
//! log, a subagent sidechain's events, a connector's observation. A reader
//! deciding what exists -- a consent baseline, a status count -- needs all
//! of them, and needs them without reading a single payload.
//!
//! So the read is a merge of one ordered index per table. Each table offers
//! its next `(source, session_id)` after the cursor through an index that
//! leads with those two columns -- one bounded seek, however many rows the
//! session holds -- and the smallest offer is the next identity. A page costs
//! a seek per identity per table that holds it, never a scan.

use crate::session_store::{Error, SessionStore, Source};
use crate::store::schema_is_identity_read_current;
use anyhow::Result;
use rusqlite::{Connection, OptionalExtension};

/// Largest page one [`SessionStore::session_identities`] call returns.
const MAX_IDENTITY_PAGE: usize = 10_000;

/// Page size when [`IdentityQuery::limit`] is zero.
const DEFAULT_IDENTITY_PAGE: usize = 1_000;

/// One session as the store names it: the stored source and session id.
///
/// `source_name` is the stored text, so a session under a source this build
/// does not know is still one identity; [`SessionIdentity::source`] parses
/// it. It is the same pair a [`crate::Change`] carries in
/// [`crate::Change::source_name`] and [`crate::Change::session_id`], and
/// what [`crate::ChangeQuery::session`] restricts a drain to.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub struct SessionIdentity {
    pub source_name: String,
    pub session_id: String,
}

impl SessionIdentity {
    pub fn new(source_name: impl Into<String>, session_id: impl Into<String>) -> Self {
        Self {
            source_name: source_name.into(),
            session_id: session_id.into(),
        }
    }

    /// The source, or `None` for a name this build does not know.
    pub fn source(&self) -> Option<Source> {
        Source::parse(&self.source_name)
    }
}

/// How to page [`SessionStore::session_identities`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct IdentityQuery {
    /// The last identity of the previous page; `None` starts from the first.
    pub after: Option<SessionIdentity>,
    /// Identities per page, clamped to `1..=10_000`; zero means 1,000.
    pub limit: usize,
    /// Leave out delegated children; see [`IdentityQuery::exclude_delegated`].
    pub exclude_delegated: bool,
}

impl IdentityQuery {
    /// Continue after this identity.
    pub fn after(mut self, identity: SessionIdentity) -> Self {
        self.after = Some(identity);
        self
    }

    /// Identities per page; see [`IdentityQuery::limit`].
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    /// Leave out delegated children — subagents and child threads another
    /// session delegated work to — which are part of that session rather
    /// than sessions of their own. A page still holds up to `limit`
    /// identities.
    pub fn exclude_delegated(mut self) -> Self {
        self.exclude_delegated = true;
        self
    }
}

impl SessionStore {
    /// Every session the store holds anything for, distinct, in
    /// `(source_name, session_id)` byte order, one keyset page at a time.
    ///
    /// A session counts when any evidence table stores a row under it: the
    /// catalog, prompts, events, tool calls, file edits, markers,
    /// relationships (under their parent), presences, commit links,
    /// connector observations and their evidence, and trajectories (under the
    /// `trajectory` source, by id). A prompt that names no session is under
    /// none, and a child session only an edge names is not one until
    /// something is stored under it -- the same pairs the change feed
    /// reports in [`crate::Change::session_id`].
    ///
    /// Continue with the last identity returned; an empty page is the end.
    /// Each page is read on one snapshot, so identities written between
    /// pages are seen only if they sort after the cursor. An empty source or
    /// session id names no session, and is not an identity. No payload is read:
    /// every step is a seek in an index that leads with the source and
    /// session.
    pub fn session_identities(
        &self,
        query: IdentityQuery,
    ) -> std::result::Result<Vec<SessionIdentity>, Error> {
        let limit = match query.limit {
            0 => DEFAULT_IDENTITY_PAGE,
            limit => limit.min(MAX_IDENTITY_PAGE),
        };
        let mut conn = self.read_conn()?;
        // The gate is here rather than at `open`, like the change feed's: a
        // read-only store over a database without the catalog's identity
        // index keeps every other read, and is told how to get this one.
        if !conn
            .gate("session-identity", schema_is_identity_read_current)
            .map_err(Error::query)?
        {
            return Err(Error::stale_schema(self.db_path(), "session-identity"));
        }
        let page = identities_after(
            &conn,
            query
                .after
                .as_ref()
                .map(|after| (after.source_name.as_str(), after.session_id.as_str())),
            limit,
            query.exclude_delegated,
        )
        .map_err(Error::query)?;
        Ok(page
            .into_iter()
            .map(|(source_name, session_id)| SessionIdentity {
                source_name,
                session_id,
            })
            .collect())
    }

    /// Whether the store holds anything under `identity`: exactly when
    /// [`SessionStore::session_identities`] would list it, by the same tables
    /// and rule -- so every identity an embedder counts is one it can select,
    /// and one [`crate::ChangeQuery::session`] accepts. An empty source or
    /// session id is never a session. Each table is one indexed existence
    /// probe, all read on one snapshot; no payload is read.
    pub fn has_session(&self, identity: &SessionIdentity) -> std::result::Result<bool, Error> {
        let conn = self.read_conn()?;
        identity_exists(&conn, &identity.source_name, &identity.session_id).map_err(Error::query)
    }

    /// Every session reached from `roots` through delegation: the subagents
    /// and child threads they delegated work to, the work those delegated in
    /// turn, and so on, whether or not the catalog holds them — the same
    /// related work hydration reads with a root, including the local
    /// transcript a remote Claude session was materialized as. These are the
    /// sessions that are part of a root's work rather than conversations of
    /// their own; continuity (a fork, resume or continuation) is not followed.
    ///
    /// The whole reachable work is returned, never a truncated part of it: a
    /// caller sharing a root shares all of it. The roots themselves are never
    /// listed. Read on one snapshot; each step is an indexed lookup by parent,
    /// and a cycle in the ledger ends where it closes. Listed in identity
    /// order.
    pub fn delegated_descendants(
        &self,
        roots: &[SessionIdentity],
    ) -> std::result::Result<Vec<SessionIdentity>, Error> {
        let conn = self.read_conn()?;
        let tx = conn.unchecked_transaction().map_err(Error::sql)?;
        delegated_descendants(&tx, roots).map_err(Error::query)
    }

    /// The sessions that delegated work to `identity`, in identity order:
    /// empty for a session of its own, and its parents when it is a subagent
    /// or child thread, which is then part of their work rather than a
    /// session to select apart from them.
    pub fn delegated_by(
        &self,
        identity: &SessionIdentity,
    ) -> std::result::Result<Vec<SessionIdentity>, Error> {
        let conn = self.read_conn()?;
        let mut parents = conn.prepare_cached(DELEGATED_BY_SQL).map_err(Error::sql)?;
        let ids = parents
            .query_map(
                rusqlite::params![
                    identity.source_name,
                    identity.session_id,
                    crate::relationships::RELATIONSHIP_DELEGATED
                ],
                |row| row.get::<_, String>(0),
            )
            .map_err(Error::sql)?
            .collect::<rusqlite::Result<std::collections::BTreeSet<_>>>()
            .map_err(Error::sql)?;
        Ok(ids
            .into_iter()
            .map(|parent| SessionIdentity {
                source_name: identity.source_name.clone(),
                session_id: parent,
            })
            .collect())
    }
}

/// One step of [`delegated_descendants`]: the delegated and materialized
/// children of `(?1, ?2)`, with `?3` and `?4` the two kinds. Not `DISTINCT`:
/// the walk's reached set already drops a repeat, and `DISTINCT` is what
/// draws the planner to the child index, whose order would serve it.
const DESCENDANT_STEP_SQL: &str = "SELECT child_session_id FROM session_relationships \
     WHERE source = ?1 AND parent_session_id = ?2 AND relationship IN (?3, ?4) \
       AND +child_session_id IS NOT NULL AND +child_session_id <> ''";

/// The delegating parents of `(?1, ?2)` under kind `?3`. Ordered and
/// deduplicated by the caller: asking SQLite for either makes the parent
/// index's order look worth more than the child lookup.
const DELEGATED_BY_SQL: &str = "SELECT parent_session_id FROM session_relationships \
     WHERE source = ?1 AND child_session_id = ?2 AND relationship = ?3 \
       AND +parent_session_id <> +child_session_id";

/// [`SessionStore::delegated_descendants`] on one connection.
fn delegated_descendants(
    conn: &Connection,
    roots: &[SessionIdentity],
) -> Result<Vec<SessionIdentity>> {
    // Each step must seek by parent; on the child index it would read every
    // edge of the source. See `the_descendant_walk_seeks_by_parent`.
    let mut children = conn.prepare_cached(DESCENDANT_STEP_SQL)?;
    let key =
        |identity: &SessionIdentity| (identity.source_name.clone(), identity.session_id.clone());
    let mut reached: std::collections::BTreeSet<(String, String)> = roots.iter().map(key).collect();
    let mut frontier: Vec<(String, String)> = reached.iter().cloned().collect();
    let mut found = std::collections::BTreeSet::new();
    while let Some((source, parent)) = frontier.pop() {
        let ids = children
            .query_map(
                rusqlite::params![
                    source,
                    parent,
                    crate::relationships::RELATIONSHIP_DELEGATED,
                    crate::relationships::RELATIONSHIP_MATERIALIZED_LOCAL
                ],
                |row| row.get::<_, String>(0),
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        for child in ids {
            let child = (source.clone(), child);
            if reached.insert(child.clone()) {
                found.insert(child.clone());
                frontier.push(child);
            }
        }
    }
    Ok(found
        .into_iter()
        .map(|(source_name, session_id)| SessionIdentity {
            source_name,
            session_id,
        })
        .collect())
}

/// Every table a session identity can be stored in: its name and the column
/// naming the session. Each has an index leading with `(source, session)`.
const IDENTITY_TABLES: &[(&str, &str)] = &[
    ("sessions", "session_id"),
    ("history", "session_id"),
    ("session_events", "session_id"),
    ("tool_calls", "session_id"),
    ("file_edits", "session_id"),
    ("session_markers", "session_id"),
    ("session_relationships", "parent_session_id"),
    ("session_presences", "session_id"),
    ("session_commit_links", "session_id"),
    ("session_observations", "session_id"),
    ("observation_evidence", "session_id"),
];

/// A trajectory is a session of its own, under a source it does not store.
const TRAJECTORY_SOURCE: &str = "trajectory";

/// The first identity in one table, or the first after a cursor. An empty
/// source or session id names no session -- `ChangeQuery::session` refuses
/// either -- so it is skipped like NULL.
fn seek_sql(table: &str, session: &str, after: bool) -> String {
    let range = if after {
        format!(" AND (source, {session}) > (?1, ?2)")
    } else {
        String::new()
    };
    format!(
        "SELECT source, {session} FROM {table} \
         WHERE {session} IS NOT NULL AND {session} <> '' AND source <> ''{range} \
         ORDER BY source, {session} LIMIT 1"
    )
}

fn trajectory_sql(after: bool) -> &'static str {
    if after {
        "SELECT id FROM trajectories WHERE id > ?1 ORDER BY id LIMIT 1"
    } else {
        "SELECT id FROM trajectories WHERE id > '' ORDER BY id LIMIT 1"
    }
}

type Identity = (String, String);

/// One table's next identity strictly after `after`.
fn next_in(
    conn: &Connection,
    arm: Option<(&str, &str)>,
    after: Option<(&str, &str)>,
) -> Result<Option<Identity>> {
    let Some((table, session)) = arm else {
        // The trajectory arm: its identities all share one source, so the
        // cursor either precedes them, falls among them, or follows them.
        let found: Option<String> = match after {
            Some((source, _)) if source > TRAJECTORY_SOURCE => return Ok(None),
            Some((source, id)) if source == TRAJECTORY_SOURCE => conn
                .prepare_cached(trajectory_sql(true))?
                .query_row([id], |row| row.get(0))
                .optional()?,
            _ => conn
                .prepare_cached(trajectory_sql(false))?
                .query_row([], |row| row.get(0))
                .optional()?,
        };
        return Ok(found.map(|id| (TRAJECTORY_SOURCE.to_string(), id)));
    };
    let mut statement = conn.prepare_cached(&seek_sql(table, session, after.is_some()))?;
    let read = |row: &rusqlite::Row<'_>| Ok((row.get(0)?, row.get(1)?));
    Ok(match after {
        Some((source, id)) => statement.query_row([source, id], read).optional()?,
        None => statement.query_row([], read).optional()?,
    })
}

/// Whether the store holds anything under one identity: the same tables and
/// the same rule for what names a session as [`identities_after`], each an
/// indexed existence probe. An empty source or session id is never one.
///
/// The probes read one snapshot, by the same rule as a page: the caller's
/// transaction, or their own on an autocommit connection, so a session whose
/// only row moves between tables mid-check is still found.
pub(crate) fn identity_exists(conn: &Connection, source: &str, session_id: &str) -> Result<bool> {
    if source.is_empty() || session_id.is_empty() {
        return Ok(false);
    }
    if conn.is_autocommit() {
        let snapshot = conn.unchecked_transaction()?;
        let found = probe_identity(&snapshot, source, session_id)?;
        snapshot.commit()?;
        return Ok(found);
    }
    probe_identity(conn, source, session_id)
}

fn probe_identity(conn: &Connection, source: &str, session_id: &str) -> Result<bool> {
    if source == TRAJECTORY_SOURCE {
        let found: bool = conn
            .prepare_cached("SELECT EXISTS(SELECT 1 FROM trajectories WHERE id = ?1)")?
            .query_row([session_id], |row| row.get(0))?;
        if found {
            return Ok(true);
        }
    }
    for (table, session) in IDENTITY_TABLES {
        let found: bool = conn
            .prepare_cached(&format!(
                "SELECT EXISTS(SELECT 1 FROM {table} WHERE source = ?1 AND {session} = ?2)"
            ))?
            .query_row([source, session_id], |row| row.get(0))?;
        if found {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Up to `limit` distinct identities after `after`, in order: the merge of
/// every table's ordered identities, each advanced only past what it offered.
///
/// Every seek of a page reads one snapshot: the caller's transaction when it
/// holds one -- a consent baseline spanning many pages -- and otherwise one
/// deferred read transaction per page. Each seek is its own statement, and on
/// an autocommit connection each would see its own moment: an identity whose
/// only row moves from a table not yet sought to one already sought -- a
/// sidechain's events adopted into the catalog, say -- would be in the store
/// throughout and in no arm's answer.
pub(crate) fn identities_after(
    conn: &Connection,
    after: Option<(&str, &str)>,
    limit: usize,
    exclude_delegated: bool,
) -> Result<Vec<Identity>> {
    if conn.is_autocommit() {
        let snapshot = conn.unchecked_transaction()?;
        let page = merge_page(&snapshot, after, limit, exclude_delegated)?;
        snapshot.commit()?;
        return Ok(page);
    }
    merge_page(conn, after, limit, exclude_delegated)
}

fn merge_page(
    conn: &Connection,
    after: Option<(&str, &str)>,
    limit: usize,
    exclude_delegated: bool,
) -> Result<Vec<Identity>> {
    let mut delegated = conn.prepare_cached(&format!(
        "SELECT {}",
        crate::relationships::delegated_child_sql("?1", "?2")
    ))?;
    let arms: Vec<Option<(&str, &str)>> = IDENTITY_TABLES
        .iter()
        .map(|(table, session)| Some((*table, *session)))
        .chain([None])
        .collect();
    let mut offers: Vec<Option<Identity>> = arms
        .iter()
        .map(|arm| next_in(conn, *arm, after))
        .collect::<Result<_>>()?;
    let mut page = Vec::with_capacity(limit.min(DEFAULT_IDENTITY_PAGE));
    while page.len() < limit {
        let Some(next) = offers.iter().flatten().min().cloned() else {
            break;
        };
        for (arm, offer) in arms.iter().zip(offers.iter_mut()) {
            if offer.as_ref() == Some(&next) {
                *offer = next_in(conn, *arm, Some((next.0.as_str(), next.1.as_str())))?;
            }
        }
        if exclude_delegated
            && delegated.query_row(rusqlite::params![next.0, next.1], |row| {
                row.get::<_, bool>(0)
            })?
        {
            continue;
        }
        page.push(next);
    }
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session_store::StoreOptions;
    use crate::store::open_db;
    use crate::store::open_db_readonly;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    fn open(dir: &std::path::Path) -> std::path::PathBuf {
        let db = dir.join("ai-history.db");
        SessionStore::open(StoreOptions {
            db_path: Some(db.clone()),
            ..StoreOptions::default()
        })
        .unwrap();
        db
    }

    /// Delegation is followed through every generation, cataloged or not;
    /// continuity is not, the roots are not listed, and a cycle ends.
    #[test]
    fn delegated_descendants_follow_delegation_only() {
        let dir = tempfile::tempdir().unwrap();
        let db = open(dir.path());
        let conn = open_db(&db).unwrap();
        let edge = |parent: &str, child: &str, relationship: &str| {
            conn.execute(
                "INSERT INTO session_relationships (source, parent_session_id, relationship_uid, \
                 child_session_id, relationship, identity_status, evidence_kind, created_ms, \
                 updated_ms) VALUES ('codex', ?1, ?2, ?2, ?3, 'observed', 'rollout', 0, 0)",
                rusqlite::params![parent, child, relationship],
            )
            .unwrap();
        };
        edge("root", "child", "delegated");
        edge("child", "grandchild", "delegated");
        edge("grandchild", "child", "delegated");
        edge("root", "fork", "fork");
        edge("fork", "forks-child", "delegated");
        edge("other", "elsewhere", "delegated");
        edge("remote", "local", "materialized_local");
        edge("local", "local-subagent", "delegated");
        let store = SessionStore::open(StoreOptions {
            db_path: Some(db),
            read_only: true,
            ..StoreOptions::default()
        })
        .unwrap();
        let identity = |session: &str| SessionIdentity {
            source_name: "codex".into(),
            session_id: session.into(),
        };
        assert_eq!(
            store.delegated_descendants(&[identity("root")]).unwrap(),
            vec![identity("child"), identity("grandchild")]
        );
        assert_eq!(
            store
                .delegated_descendants(&[identity("root"), identity("child")])
                .unwrap(),
            vec![identity("grandchild")]
        );
        assert!(store
            .delegated_descendants(&[identity("nobody")])
            .unwrap()
            .is_empty());
        assert_eq!(
            store.delegated_by(&identity("grandchild")).unwrap(),
            vec![identity("child")]
        );
        assert!(store.delegated_by(&identity("fork")).unwrap().is_empty());
        assert!(store.delegated_by(&identity("root")).unwrap().is_empty());
        // A remote session's work includes the local transcript it was
        // materialized as, and that transcript's own delegations; the local
        // transcript stays a session of its own.
        assert_eq!(
            store.delegated_descendants(&[identity("remote")]).unwrap(),
            vec![identity("local"), identity("local-subagent")]
        );
        assert!(store.delegated_by(&identity("local")).unwrap().is_empty());
    }

    /// Each step of the descendant walk seeks by parent, and the parent lookup
    /// by child, with or without statistics; any other plan reads every edge
    /// of the source.
    #[test]
    fn the_descendant_walk_seeks_by_parent() {
        let dir = tempfile::tempdir().unwrap();
        let db = open(dir.path());
        let conn = open_db(&db).unwrap();
        for analyze in [false, true] {
            if analyze {
                conn.execute_batch("ANALYZE").unwrap();
            }
            let plan: Vec<String> = conn
                .prepare(&format!("EXPLAIN QUERY PLAN {DESCENDANT_STEP_SQL}"))
                .unwrap()
                .query_map(
                    rusqlite::params!["codex", "root", "delegated", "materialized_local"],
                    |row| row.get::<_, String>(3),
                )
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
            let plan = plan.join(" | ");
            assert!(
                plan.contains(
                    "idx_session_relationships_parent (source=? AND parent_session_id=?)"
                ),
                "the step does not seek by parent (analyze={analyze}): {plan}"
            );
            let plan: Vec<String> = conn
                .prepare(&format!("EXPLAIN QUERY PLAN {DELEGATED_BY_SQL}"))
                .unwrap()
                .query_map(rusqlite::params!["codex", "child", "delegated"], |row| {
                    row.get::<_, String>(3)
                })
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
            let plan = plan.join(" | ");
            assert!(
                plan.contains("idx_session_relationships_child (source=? AND child_session_id=?)"),
                "the parent lookup does not seek by child (analyze={analyze}): {plan}"
            );
        }
    }

    /// Listings that leave delegated children out still name every session
    /// of its own, including a fork, and fill their pages.
    #[test]
    fn listings_can_leave_delegated_children_out() {
        let dir = tempfile::tempdir().unwrap();
        let db = open(dir.path());
        let conn = open_db(&db).unwrap();
        for session in ["root", "child", "fork", "solo"] {
            conn.execute(
                "INSERT INTO sessions (source, session_id, last_activity_ms, discovery_state) \
                 VALUES ('opencode', ?1, 1, 'shallow')",
                [session],
            )
            .unwrap();
        }
        for (parent, child, relationship) in
            [("root", "child", "delegated"), ("root", "fork", "fork")]
        {
            conn.execute(
                "INSERT INTO session_relationships (source, parent_session_id, relationship_uid, \
                 child_session_id, relationship, identity_status, evidence_kind, created_ms, \
                 updated_ms) VALUES ('opencode', ?1, ?2, ?2, ?3, 'observed', 'db', 0, 0)",
                rusqlite::params![parent, child, relationship],
            )
            .unwrap();
        }
        let store = SessionStore::open(StoreOptions {
            db_path: Some(db),
            read_only: true,
            ..StoreOptions::default()
        })
        .unwrap();
        let mut listed: Vec<String> = store
            .sessions(crate::CatalogQuery {
                exclude_delegated: true,
                ..crate::CatalogQuery::default()
            })
            .map(|row| row.unwrap().session_id)
            .collect();
        listed.sort();
        assert_eq!(listed, ["fork", "root", "solo"]);
        assert_eq!(store.sessions(crate::CatalogQuery::default()).count(), 4);

        let page = store
            .session_identities(IdentityQuery::default().limit(3).exclude_delegated())
            .unwrap();
        let names: Vec<_> = page.iter().map(|id| id.session_id.as_str()).collect();
        assert_eq!(names, ["fork", "root", "solo"]);
    }

    /// A page is one snapshot even on an autocommit connection. An identity
    /// whose only row moves from a table the page has not sought yet to one
    /// it already has -- committed by another connection between the two
    /// seeks -- is still in the page, because every seek reads the store as
    /// it stood when the page began.
    #[test]
    fn a_page_reads_one_snapshot_on_an_autocommit_connection() {
        let dir = tempfile::tempdir().unwrap();
        let db = open(dir.path());
        open_db(&db)
            .unwrap()
            .execute(
                "INSERT INTO session_events (source, session_id, message_id, ts_ms, role, kind, \
                 text, event_uid) VALUES ('claude', 'moving', 'm', 1, 'user', 'text', 'x', 'e1')",
                [],
            )
            .unwrap();

        let reader = open_db_readonly(&db).unwrap();
        assert!(reader.is_autocommit());
        // A page with no hook first, so every statement is prepared and the
        // schema parsed: what remains to count is each seek's own work.
        assert_eq!(identities_after(&reader, None, 10, false).unwrap().len(), 1);
        // How many progress callbacks the catalog's arm -- the first seek a
        // page runs -- takes on its own. The move fires just past them: after
        // that seek has read the catalog, before the events arm reads.
        let counted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = Arc::clone(&counted);
        reader.progress_handler(
            1,
            Some(move || {
                counter.fetch_add(1, Ordering::SeqCst);
                false
            }),
        );
        assert_eq!(
            next_in(&reader, Some(("sessions", "session_id")), None).unwrap(),
            None
        );
        let first_arm = counted.load(Ordering::SeqCst);
        let fired = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&fired);
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let seen = Arc::clone(&calls);
        let db_for_hook = db.clone();
        // The move lands in one transaction, so at every moment the session
        // is in exactly one of the two tables.
        reader.progress_handler(
            1,
            Some(move || {
                if seen.fetch_add(1, Ordering::SeqCst) == first_arm && !flag.swap(true, Ordering::SeqCst) {
                    open_db(&db_for_hook)
                        .unwrap()
                        .execute_batch(
                            "BEGIN IMMEDIATE; \
                             DELETE FROM session_events WHERE session_id = 'moving'; \
                             INSERT INTO sessions (session_id, source) VALUES ('moving', 'claude'); \
                             COMMIT;",
                        )
                        .unwrap();
                }
                false
            }),
        );
        let page = identities_after(&reader, None, 10, false).unwrap();
        reader.progress_handler(0, None::<fn() -> bool>);
        assert!(
            fired.load(Ordering::SeqCst),
            "the move must have interleaved"
        );
        assert_eq!(
            page,
            vec![("claude".to_string(), "moving".to_string())],
            "the session existed throughout, so the page names it"
        );
        assert!(reader.is_autocommit(), "the page's own snapshot is closed");
    }

    /// An identity exists exactly when the listing names it, in every table,
    /// and never for an empty source or session id; each probe is an index
    /// search.
    #[test]
    fn an_identity_exists_exactly_when_it_is_listed() {
        let dir = tempfile::tempdir().unwrap();
        let db = open(dir.path());
        let conn = open_db(&db).unwrap();
        conn.execute_batch(
            "INSERT INTO tool_calls (source, session_id, tool_use_id, name) \
                 VALUES ('claude', 'tool-only', 't1', 'Bash'), ('', 'blank-source', 't1', 'Bash'); \
             INSERT INTO session_observations (source, session_id, location, connector_id, \
                 connector_instance, updated_ms) \
                 VALUES ('some-new-agent', 'observed-only', 'remote', 'conn', 'default', 1); \
             INSERT INTO history (source, session_id, prompt, timestamp_ms) \
                 VALUES ('codex', '', 'no session', 1); \
             INSERT INTO trajectories (id, decisions_json, retrospective_json, search_text, \
                 updated_ms, timestamp_ms) VALUES ('traj-1', '[]', '{}', 'x', 1, 1);",
        )
        .unwrap();
        let listed = identities_after(&conn, None, 100, false).unwrap();
        assert_eq!(
            listed,
            [
                ("claude", "tool-only"),
                ("some-new-agent", "observed-only"),
                ("trajectory", "traj-1")
            ]
            .map(|(source, id)| (source.to_string(), id.to_string()))
        );
        for (source, id) in &listed {
            assert!(identity_exists(&conn, source, id).unwrap(), "{source} {id}");
        }
        for (source, id) in [
            ("", "blank-source"),
            ("codex", ""),
            ("claude", "missing"),
            ("trajectory", "missing"),
        ] {
            assert!(
                !identity_exists(&conn, source, id).unwrap(),
                "{source:?} {id:?}"
            );
        }
        for (table, session) in IDENTITY_TABLES {
            let plan: String = conn
                .query_row(
                    &format!(
                        "EXPLAIN QUERY PLAN SELECT 1 FROM {table} \
                         WHERE source = ?1 AND {session} = ?2"
                    ),
                    ["a", "b"],
                    |row| row.get(3),
                )
                .unwrap();
            assert!(
                plan.starts_with(&format!("SEARCH {table}")),
                "{table}: {plan}"
            );
        }
    }

    /// The existence check reads one snapshot like a page: a session whose
    /// only row moves from a table not yet probed to one already probed,
    /// committed between the two probes, is still found.
    #[test]
    fn an_existence_check_reads_one_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let db = open(dir.path());
        open_db(&db)
            .unwrap()
            .execute(
                "INSERT INTO session_events (source, session_id, message_id, ts_ms, role, kind, \
                 text, event_uid) VALUES ('claude', 'moving', 'm', 1, 'user', 'text', 'x', 'e1')",
                [],
            )
            .unwrap();
        let reader = open_db_readonly(&db).unwrap();
        // Prepare every probe first, so the count below is the catalog
        // probe's own work.
        assert!(identity_exists(&reader, "claude", "moving").unwrap());
        let counted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = Arc::clone(&counted);
        reader.progress_handler(
            1,
            Some(move || {
                counter.fetch_add(1, Ordering::SeqCst);
                false
            }),
        );
        reader
            .prepare_cached(
                "SELECT EXISTS(SELECT 1 FROM sessions WHERE source = ?1 AND session_id = ?2)",
            )
            .unwrap()
            .query_row(["claude", "moving"], |row| row.get::<_, bool>(0))
            .unwrap();
        let catalog_probe = counted.load(Ordering::SeqCst);
        let fired = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&fired);
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let seen = Arc::clone(&calls);
        let db_for_hook = db.clone();
        reader.progress_handler(
            1,
            Some(move || {
                if seen.fetch_add(1, Ordering::SeqCst) == catalog_probe
                    && !flag.swap(true, Ordering::SeqCst)
                {
                    open_db(&db_for_hook)
                        .unwrap()
                        .execute_batch(
                            "BEGIN IMMEDIATE; \
                             DELETE FROM session_events WHERE session_id = 'moving'; \
                             INSERT INTO sessions (session_id, source) VALUES ('moving', 'claude'); \
                             COMMIT;",
                        )
                        .unwrap();
                }
                false
            }),
        );
        let found = identity_exists(&reader, "claude", "moving").unwrap();
        reader.progress_handler(0, None::<fn() -> bool>);
        assert!(
            fired.load(Ordering::SeqCst),
            "the move must have interleaved"
        );
        assert!(found, "the session existed throughout");
    }

    /// A store from before the identity index gains it on its first writable
    /// open, and the catalog's arm then seeks it; until then a read-only
    /// store refuses the listing and names the remedy.
    #[test]
    fn an_older_store_gains_the_identity_index() {
        let dir = tempfile::tempdir().unwrap();
        let db = open(dir.path());
        open_db(&db)
            .unwrap()
            .execute_batch("DROP INDEX idx_sessions_identity;")
            .unwrap();
        let read_only = SessionStore::open(StoreOptions {
            db_path: Some(db.clone()),
            read_only: true,
            ..StoreOptions::default()
        })
        .unwrap();
        let error = read_only
            .session_identities(IdentityQuery::default())
            .unwrap_err();
        assert!(error.is_stale_schema(), "{error}");

        let store = SessionStore::open(StoreOptions {
            db_path: Some(db.clone()),
            ..StoreOptions::default()
        })
        .unwrap();
        let conn = open_db(&db).unwrap();
        let present: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name = 'idx_sessions_identity')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(present, "the writable open migrated the index");
        let plan: String = conn
            .query_row(
                &format!(
                    "EXPLAIN QUERY PLAN {}",
                    seek_sql("sessions", "session_id", true)
                ),
                ["a", "b"],
                |row| row.get(3),
            )
            .unwrap();
        assert!(
            plan.contains("COVERING INDEX idx_sessions_identity"),
            "{plan}"
        );
        assert!(store.session_identities(IdentityQuery::default()).is_ok());
        assert!(read_only
            .session_identities(IdentityQuery::default())
            .is_ok());
    }

    /// Every seek is an index search on `(source, session)` -- a covering
    /// read that never touches a payload -- and none scans its table.
    #[test]
    fn every_seek_is_an_index_search_on_the_session() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("ai-history.db");
        SessionStore::open(StoreOptions {
            db_path: Some(db.clone()),
            ..StoreOptions::default()
        })
        .unwrap();
        let conn = crate::store::open_db(&db).unwrap();
        let mut plans = Vec::new();
        for (table, session) in IDENTITY_TABLES {
            for after in [false, true] {
                plans.push((*table, seek_sql(table, session, after), after, 2));
            }
        }
        for after in [false, true] {
            plans.push(("trajectories", trajectory_sql(after).to_string(), after, 1));
        }
        for (table, sql, after, arity) in plans {
            let values: Vec<&str> = if after {
                ["a", "b"][..arity].to_vec()
            } else {
                vec![]
            };
            let plan = conn
                .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
                .unwrap()
                .query_map(rusqlite::params_from_iter(values), |row| {
                    row.get::<_, String>(3)
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
                .join(" | ");
            eprintln!("{table} (after: {after}): {plan}");
            assert!(plan.contains("COVERING INDEX"), "{table}: {plan}");
            assert!(!plan.contains("TEMP B-TREE"), "{table}: {plan}");
            if after {
                assert!(
                    plan.starts_with(&format!("SEARCH {table}")),
                    "{table}: {plan}"
                );
            }
        }
    }
}
