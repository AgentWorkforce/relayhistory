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
use crate::store::open_db_readonly;
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
    /// Each page is read in its own snapshot, so identities written between
    /// pages are seen only if they sort after the cursor. No payload is read:
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
        let conn = open_db_readonly(self.db_path())
            .map_err(|error| Error::DatabaseOpen(format!("{error:#}")))?;
        let snapshot = conn.unchecked_transaction().map_err(Error::sql)?;
        let page = identities_after(
            &snapshot,
            query
                .after
                .as_ref()
                .map(|after| (after.source_name.as_str(), after.session_id.as_str())),
            limit,
        )
        .map_err(Error::query)?;
        snapshot.commit().map_err(Error::sql)?;
        Ok(page
            .into_iter()
            .map(|(source_name, session_id)| SessionIdentity {
                source_name,
                session_id,
            })
            .collect())
    }
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

/// The first identity in one table, or the first after a cursor.
fn seek_sql(table: &str, session: &str, after: bool) -> String {
    let range = if after {
        format!(" AND (source, {session}) > (?1, ?2)")
    } else {
        String::new()
    };
    format!(
        "SELECT source, {session} FROM {table} WHERE {session} IS NOT NULL{range} \
         ORDER BY source, {session} LIMIT 1"
    )
}

fn trajectory_sql(after: bool) -> &'static str {
    if after {
        "SELECT id FROM trajectories WHERE id > ?1 ORDER BY id LIMIT 1"
    } else {
        "SELECT id FROM trajectories ORDER BY id LIMIT 1"
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

/// Up to `limit` distinct identities after `after`, in order: the merge of
/// every table's ordered identities, each advanced only past what it offered.
pub(crate) fn identities_after(
    conn: &Connection,
    after: Option<(&str, &str)>,
    limit: usize,
) -> Result<Vec<Identity>> {
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
        page.push(next);
    }
    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session_store::StoreOptions;

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
