//! The queries behind one adapter's stamp snapshot.
//!
//! The bulk read keeps every available locator, including a session with no
//! catalog row. Locator resolution orders by session id and only then applies
//! the catalog predicate, so the lowest id has to win here too.

use std::collections::{HashMap, HashSet};

use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension};

use super::super::{Candidate, ShallowSessionProvider};
use super::{
    count_fallback, count_load, data_version, Adapter, Kind, ObsSlot, SkipSlot, STACK,
};

#[cfg(test)]
use super::on_load;

/// The per-candidate stamp query. A full pass asks it only on a miss.
pub(super) const STAMP_SQL: &str = "SELECT o.source_stamp FROM session_observations o \
     WHERE o.source = ?1 AND o.session_id = ?2 AND o.location = ?3 \
       AND o.connector_id = ?4 AND o.connector_instance = ?5 \
       AND o.access_state = 'available' \
       AND EXISTS (SELECT 1 FROM sessions s \
             WHERE s.source = ?1 AND s.session_id = ?2)";

/// Available observations of one adapter. The `EXISTS` is the stamp query's
/// catalog predicate; a row that fails it is still returned so locator
/// resolution can see it.
pub(super) const OBSERVATION_ROWS_SQL: &str =
    "SELECT o.session_id, o.raw_locator, o.source_stamp, \
       EXISTS (SELECT 1 FROM sessions s \
               WHERE s.source = o.source AND s.session_id = o.session_id) \
     FROM session_observations o \
     WHERE o.source = ?1 AND o.location = ?2 AND o.connector_id = ?3 \
       AND o.connector_instance = ?4 AND o.access_state = 'available'";

const SKIP_SQL: &str = "SELECT stamp FROM observation_discovery_skips WHERE source=? AND location=? \
     AND connector_id=? AND connector_instance=? AND locator=?";

const SKIP_ROWS_SQL: &str = "SELECT locator, stamp FROM observation_discovery_skips \
     WHERE source = ?1 AND location = ?2 AND connector_id = ?3 AND connector_instance = ?4";

pub(super) fn point_stamp(
    conn: &Connection,
    provider: &dyn ShallowSessionProvider,
    candidate: &Candidate,
) -> Result<Option<(String, Option<String>)>> {
    count_fallback();
    let id = match candidate.session_id.as_ref() {
        Some(id) => Some(id.clone()),
        None => conn
            .prepare_cached(super::super::OBSERVED_SESSION_BY_LOCATOR_SQL)?
            .query_row(
                params![
                    candidate.source,
                    provider.location().as_str(),
                    provider.connector_id(),
                    provider.connector_instance(),
                    candidate.locator
                ],
                |row| row.get(0),
            )
            .optional()?,
    };
    let Some(id) = id else {
        return Ok(None);
    };
    let key = super::super::observation_key(provider, candidate.source, &id);
    key.validate()?;
    let stamp = conn
        .prepare_cached(STAMP_SQL)?
        .query_row(
            params![
                key.source,
                key.session_id,
                key.location.as_str(),
                key.connector_id,
                key.connector_instance
            ],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()?;
    Ok(stamp.map(|stamp| (id, stamp)))
}

pub(super) fn point_skip(
    conn: &Connection,
    provider: &dyn ShallowSessionProvider,
    source: &str,
    locator: &str,
    stamp: &str,
) -> Result<bool> {
    count_fallback();
    let known: Option<String> = conn
        .prepare_cached(SKIP_SQL)?
        .query_row(
            params![
                source,
                provider.location().as_str(),
                provider.connector_id(),
                provider.connector_instance(),
                locator
            ],
            |row| row.get(0),
        )
        .optional()?;
    Ok(known.as_deref() == Some(stamp))
}

pub(super) fn load(conn: &Connection, id: usize, adapter: &Adapter, kind: Kind) -> Result<()> {
    // Drop this kind before the read. A commit during the read leaves it
    // empty, so the candidate asks the database instead of the previous set.
    // The other kind keeps the version it was read at.
    clear_kind(id, adapter, kind);
    #[cfg(test)]
    on_load();
    let version = data_version(conn)?;
    match kind {
        Kind::Observations => {
            let rows = read_observations(conn, adapter)?;
            publish_obs(conn, id, adapter, version, rows.by_session, rows.by_locator)?;
        }
        Kind::Skips => {
            let by_locator = read_skips(conn, adapter)?;
            publish_skips(conn, id, adapter, version, by_locator)?;
        }
    }
    Ok(())
}

fn clear_kind(id: usize, adapter: &Adapter, kind: Kind) {
    STACK.with(|stack| {
        let mut stack = stack.borrow_mut();
        let Some(memo) = stack.iter_mut().rev().find(|memo| memo.conn == id) else {
            return;
        };
        match kind {
            Kind::Observations => {
                memo.observations.remove(adapter);
            }
            Kind::Skips => {
                memo.skips.remove(adapter);
            }
        }
    });
}

fn publish_obs(
    conn: &Connection,
    id: usize,
    adapter: &Adapter,
    version: i64,
    by_session: HashMap<String, Option<String>>,
    by_locator: HashMap<String, String>,
) -> Result<()> {
    if data_version(conn)? != version {
        return Ok(());
    }
    count_load();
    let slot = ObsSlot {
        version,
        by_session,
        by_locator,
        stale_sessions: HashSet::new(),
        stale_locators: HashSet::new(),
    };
    insert_obs(id, adapter, slot);
    Ok(())
}

fn publish_skips(
    conn: &Connection,
    id: usize,
    adapter: &Adapter,
    version: i64,
    by_locator: HashMap<String, String>,
) -> Result<()> {
    if data_version(conn)? != version {
        return Ok(());
    }
    count_load();
    let slot = SkipSlot {
        version,
        by_locator,
        stale_locators: HashSet::new(),
    };
    insert_skip(id, adapter, slot);
    Ok(())
}

fn insert_obs(id: usize, adapter: &Adapter, slot: ObsSlot) {
    STACK.with(|stack| {
        let mut stack = stack.borrow_mut();
        if let Some(memo) = stack.iter_mut().rev().find(|memo| memo.conn == id) {
            memo.observations.insert(adapter.clone(), slot);
        }
    });
}

fn insert_skip(id: usize, adapter: &Adapter, slot: SkipSlot) {
    STACK.with(|stack| {
        let mut stack = stack.borrow_mut();
        if let Some(memo) = stack.iter_mut().rev().find(|memo| memo.conn == id) {
            memo.skips.insert(adapter.clone(), slot);
        }
    });
}

struct ObservationRows {
    by_session: HashMap<String, Option<String>>,
    by_locator: HashMap<String, String>,
}

fn read_observations(conn: &Connection, adapter: &Adapter) -> Result<ObservationRows> {
    let mut stmt = conn.prepare_cached(OBSERVATION_ROWS_SQL)?;
    let rows = stmt.query_map(
        params![
            adapter.source,
            adapter.location,
            adapter.connector_id,
            adapter.connector_instance
        ],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, i64>(3)?,
            ))
        },
    )?;
    let mut by_session = HashMap::new();
    let mut by_locator = HashMap::new();
    for row in rows {
        let (session_id, locator, stamp, catalogued) = row?;
        if catalogued != 0 {
            by_session.insert(session_id.clone(), stamp);
        }
        if let Some(locator) = locator {
            remember_locator(&mut by_locator, locator, session_id);
        }
    }
    Ok(ObservationRows {
        by_session,
        by_locator,
    })
}

fn read_skips(conn: &Connection, adapter: &Adapter) -> Result<HashMap<String, String>> {
    let mut stmt = conn.prepare_cached(SKIP_ROWS_SQL)?;
    let rows = stmt.query_map(
        params![
            adapter.source,
            adapter.location,
            adapter.connector_id,
            adapter.connector_instance
        ],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
    )?;
    let mut by_locator = HashMap::new();
    for row in rows {
        let (locator, stamp) = row?;
        by_locator.insert(locator, stamp);
    }
    Ok(by_locator)
}

/// Lowest session id wins, matching `ORDER BY +session_id LIMIT 1`.
fn remember_locator(map: &mut HashMap<String, String>, locator: String, session_id: String) {
    match map.get(&locator) {
        Some(existing) if existing.as_str() <= session_id.as_str() => {}
        _ => {
            map.insert(locator, session_id);
        }
    }
}
