//! One read of the stamps an unchanged-file discovery pass asks about.
//!
//! A full enumeration of one adapter used to ask, once per candidate, for the
//! observation stamp and — when the path does not name a session — for the
//! locator, then again for a known non-session. This module reads each of
//! those sets once per armed adapter and serves later candidates from that
//! read. A path-scoped pass does not arm, and neither does a pass whose
//! limit stops before the last candidate: one file keeps the point query.
//!
//! The snapshot is this connection's. This connection's own commit does not
//! move `PRAGMA data_version`, so a pass keeps the read across the window
//! commits it opens. Another connection's commit does move it, and the next
//! candidate reads again instead of skipping a file that commit changed.
//! Observations and non-session skips each record the version they were read
//! at, so loading one does not make the other's keys current.
//!
//! A stamp this pass writes or deletes is removed from the read. The next
//! candidate for that session or locator asks the database, which sees the
//! write, instead of answering "unchanged" from the value the pass replaced.
//! A row callback that writes through this connection drops the whole read:
//! `data_version` does not move for that write, and the callback does not
//! name the keys it changed. A rollback drops the read for the same reason.
//! This connection's own commit does not.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

use anyhow::Result;
use rusqlite::{params, Connection};

use super::{Candidate, ShallowSessionProvider};

mod rows;

#[cfg(test)]
use std::cell::Cell;

thread_local! {
    static STACK: RefCell<Vec<Memo>> = const { RefCell::new(Vec::new()) };
}

#[cfg(test)]
thread_local! {
    static LOADS: Cell<u64> = const { Cell::new(0) };
    static FALLBACKS: Cell<u64> = const { Cell::new(0) };
    static ROLLBACKS: Cell<u64> = const { Cell::new(0) };
    static WINDOW: Cell<usize> = const { Cell::new(0) };
    static ON_LOAD: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
}

/// `PRAGMA data_version` when this set was read, and the keys from that read.
struct ObsSlot {
    version: i64,
    /// Catalogued, available sessions. Absent means the point query would
    /// return no stamp.
    by_session: HashMap<String, Option<String>>,
    /// Every available locator, including a session that has no catalog row.
    /// The point lookup orders by session id and only then applies the
    /// catalog predicate, so the lowest id has to win here too.
    by_locator: HashMap<String, String>,
    stale_sessions: HashSet<String>,
    stale_locators: HashSet<String>,
}

struct SkipSlot {
    version: i64,
    by_locator: HashMap<String, String>,
    stale_locators: HashSet<String>,
}

struct Memo {
    conn: usize,
    /// `false` for a path-scoped pass: the point query stays.
    armed: bool,
    observations: HashMap<Adapter, ObsSlot>,
    skips: HashMap<Adapter, SkipSlot>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct Adapter {
    source: String,
    location: String,
    connector_id: String,
    connector_instance: String,
}

#[derive(Clone, Copy)]
enum Kind {
    Observations,
    Skips,
}

enum Cache<T> {
    Absent,
    Pending,
    Stale,
    Hit(T),
}

enum Read<T> {
    Ask,
    Hit(T),
}

type CachedStamp = Option<(String, Option<String>)>;

fn conn_id(conn: &Connection) -> usize {
    std::ptr::from_ref(conn) as usize
}

fn adapter_of(provider: &dyn ShallowSessionProvider, source: &str) -> Adapter {
    Adapter {
        source: source.to_string(),
        location: provider.location().as_str().to_string(),
        connector_id: provider.connector_id().to_string(),
        connector_instance: provider.connector_instance().to_string(),
    }
}

fn count_load() {
    #[cfg(test)]
    LOADS.with(|loads| loads.set(loads.get() + 1));
}

fn count_fallback() {
    #[cfg(test)]
    FALLBACKS.with(|falls| falls.set(falls.get() + 1));
}

fn data_version(conn: &Connection) -> Result<i64> {
    Ok(conn.pragma_query_value(None, "data_version", |row| row.get(0))?)
}

/// Arm the snapshot for one discovery pass. A path scope leaves it unarmed,
/// and so does a limit that cannot cover every candidate.
pub(super) struct Guard;

impl Guard {
    pub(super) fn arm(
        conn: &Connection,
        scope: Option<&crate::ingest::path_scope::PathScope>,
        limit: Option<usize>,
        candidates: usize,
    ) -> Self {
        STACK.with(|stack| {
            stack.borrow_mut().push(Memo {
                conn: conn_id(conn),
                armed: full_walk(scope, limit, candidates),
                observations: HashMap::new(),
                skips: HashMap::new(),
            });
        });
        Self
    }
}

/// The bulk read pays off only when this pass can ask about every candidate.
fn full_walk(
    scope: Option<&crate::ingest::path_scope::PathScope>,
    limit: Option<usize>,
    candidates: usize,
) -> bool {
    match (scope.is_some(), limit) {
        (true, _) => false,
        (false, Some(limit)) => limit >= candidates,
        (false, None) => true,
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        STACK.with(|stack| {
            stack.borrow_mut().pop();
        });
    }
}

/// The window cap. Tests shrink it so a later candidate is classified after
/// this pass has already written.
pub(super) fn read_window(emitted: usize, limit: usize) -> usize {
    let cap = configured_window();
    if emitted >= limit {
        cap
    } else {
        (limit - emitted).min(cap)
    }
}

fn configured_window() -> usize {
    #[cfg(test)]
    {
        let set = WINDOW.with(Cell::get);
        if set > 0 {
            return set;
        }
    }
    super::MAX_READ_WINDOW
}

/// Drop every set this connection has read. A rollback may have undone rows
/// the read observed; `data_version` does not move for this connection's own
/// rollback.
pub(super) fn on_rollback(conn: &Connection) {
    #[cfg(test)]
    ROLLBACKS.with(|rolls| rolls.set(rolls.get() + 1));
    clear_conn(conn);
}

fn clear_conn(conn: &Connection) {
    let id = conn_id(conn);
    STACK.with(|stack| {
        for memo in stack.borrow_mut().iter_mut() {
            if memo.conn == id {
                memo.observations.clear();
                memo.skips.clear();
            }
        }
    });
}

/// `on_row` runs after the window commits. A write it makes on this
/// connection does not move `data_version`, so the next candidate would
/// skip on the pre-write stamp. Drop the read when the change count moved.
/// Discovery's own writes are already in `changes_before`.
pub(super) fn after_emit(conn: &Connection, changes_before: u64) {
    if conn.total_changes() != changes_before {
        clear_conn(conn);
    }
}

/// This pass upserted or deleted `session_id`'s observation. The cached stamp
/// and every locator that named the session are not served again.
pub(super) fn note_observation(
    conn: &Connection,
    provider: &dyn ShallowSessionProvider,
    source: &str,
    session_id: &str,
    locator: &str,
) {
    let id = conn_id(conn);
    let adapter = adapter_of(provider, source);
    STACK.with(|stack| {
        for memo in stack.borrow_mut().iter_mut() {
            if memo.conn != id {
                continue;
            }
            if let Some(slot) = memo.observations.get_mut(&adapter) {
                forget_observation(slot, session_id, locator);
            }
        }
    });
}

fn forget_observation(slot: &mut ObsSlot, session_id: &str, locator: &str) {
    slot.by_session.remove(session_id);
    slot.stale_sessions.insert(session_id.to_string());
    let mut dropped = vec![locator.to_string()];
    for (loc, id) in &slot.by_locator {
        if id == session_id {
            dropped.push(loc.clone());
        }
    }
    for loc in dropped {
        slot.by_locator.remove(&loc);
        slot.stale_locators.insert(loc);
    }
}

fn forget_skip(slot: &mut SkipSlot, locator: &str) {
    slot.by_locator.remove(locator);
    slot.stale_locators.insert(locator.to_string());
}

pub(super) fn observed_candidate_stamp(
    conn: &Connection,
    provider: &dyn ShallowSessionProvider,
    candidate: &Candidate,
) -> Result<Option<(String, Option<String>)>> {
    if let Read::Hit(stamp) = known_stamp(conn, provider, candidate)? {
        return Ok(stamp);
    }
    rows::point_stamp(conn, provider, candidate)
}

pub(super) fn is_known_non_session(
    conn: &Connection,
    provider: &dyn ShallowSessionProvider,
    source: &str,
    locator: &str,
    stamp: &str,
) -> Result<bool> {
    if let Read::Hit(known) = known_skip(conn, provider, source, locator, stamp)? {
        return Ok(known);
    }
    rows::point_skip(conn, provider, source, locator, stamp)
}

pub(super) fn record_non_session(
    conn: &Connection,
    provider: &dyn ShallowSessionProvider,
    source: &str,
    locator: &str,
    stamp: &str,
) -> Result<()> {
    conn.execute(
        "INSERT INTO observation_discovery_skips(source,location,connector_id,connector_instance,locator,stamp,updated_ms) VALUES(?,?,?,?,?,?,?) ON CONFLICT(source,location,connector_id,connector_instance,locator) DO UPDATE SET stamp=excluded.stamp,updated_ms=excluded.updated_ms",
        params![
            source,
            provider.location().as_str(),
            provider.connector_id(),
            provider.connector_instance(),
            locator,
            stamp,
            crate::now_ms()
        ],
    )?;
    note_skip(conn, provider, source, locator);
    Ok(())
}

pub(super) fn clear_non_session(
    conn: &Connection,
    provider: &dyn ShallowSessionProvider,
    source: &str,
    locator: &str,
) -> Result<()> {
    conn.execute(
        "DELETE FROM observation_discovery_skips WHERE source=? AND location=? AND connector_id=? AND connector_instance=? AND locator=?",
        params![
            source,
            provider.location().as_str(),
            provider.connector_id(),
            provider.connector_instance(),
            locator
        ],
    )?;
    note_skip(conn, provider, source, locator);
    Ok(())
}

fn note_skip(conn: &Connection, provider: &dyn ShallowSessionProvider, source: &str, locator: &str) {
    let id = conn_id(conn);
    let adapter = adapter_of(provider, source);
    STACK.with(|stack| {
        for memo in stack.borrow_mut().iter_mut() {
            if memo.conn != id {
                continue;
            }
            if let Some(slot) = memo.skips.get_mut(&adapter) {
                forget_skip(slot, locator);
            }
        }
    });
}

impl Memo {
    fn obs_at(
        &self,
        adapter: &Adapter,
        candidate: &Candidate,
        version: i64,
    ) -> Cache<Option<(String, Option<String>)>> {
        let Some(slot) = self.observations.get(adapter) else {
            return Cache::Pending;
        };
        if slot.version != version {
            return Cache::Pending;
        }
        obs_hit(slot, candidate)
    }

    fn skip_at(&self, adapter: &Adapter, locator: &str, stamp: &str, version: i64) -> Cache<bool> {
        let Some(slot) = self.skips.get(adapter) else {
            return Cache::Pending;
        };
        if slot.version != version {
            return Cache::Pending;
        }
        if slot.stale_locators.contains(locator) {
            return Cache::Stale;
        }
        Cache::Hit(slot.by_locator.get(locator).map(String::as_str) == Some(stamp))
    }
}

fn obs_hit(slot: &ObsSlot, candidate: &Candidate) -> Cache<Option<(String, Option<String>)>> {
    if let Some(id) = candidate.session_id.as_ref() {
        if slot.stale_sessions.contains(id) {
            return Cache::Stale;
        }
        return Cache::Hit(slot.by_session.get(id).map(|stamp| (id.clone(), stamp.clone())));
    }
    if slot.stale_locators.contains(&candidate.locator) {
        return Cache::Stale;
    }
    let Some(id) = slot.by_locator.get(&candidate.locator) else {
        return Cache::Hit(None);
    };
    if slot.stale_sessions.contains(id) {
        return Cache::Stale;
    }
    // No catalog row: the stamp query's EXISTS fails, so this is not a stamp.
    Cache::Hit(slot.by_session.get(id).map(|stamp| (id.clone(), stamp.clone())))
}

fn known_stamp(
    conn: &Connection,
    provider: &dyn ShallowSessionProvider,
    candidate: &Candidate,
) -> Result<Read<CachedStamp>> {
    let id = conn_id(conn);
    let adapter = adapter_of(provider, candidate.source);
    let version = data_version(conn)?;
    let read = current_obs(id, &adapter, candidate, version);
    finish(
        conn,
        read,
        || rows::load(conn, id, &adapter, Kind::Observations),
        |version| current_obs(id, &adapter, candidate, version),
    )
}

fn known_skip(
    conn: &Connection,
    provider: &dyn ShallowSessionProvider,
    source: &str,
    locator: &str,
    stamp: &str,
) -> Result<Read<bool>> {
    let id = conn_id(conn);
    let adapter = adapter_of(provider, source);
    let version = data_version(conn)?;
    let read = current_skip(id, &adapter, locator, stamp, version);
    finish(
        conn,
        read,
        || rows::load(conn, id, &adapter, Kind::Skips),
        |version| current_skip(id, &adapter, locator, stamp, version),
    )
}

fn current_obs(
    id: usize,
    adapter: &Adapter,
    candidate: &Candidate,
    version: i64,
) -> Cache<Option<(String, Option<String>)>> {
    STACK.with(|stack| {
        let stack = stack.borrow();
        match stack.iter().rev().find(|memo| memo.conn == id) {
            None => Cache::Absent,
            Some(memo) if !memo.armed => Cache::Absent,
            Some(memo) => memo.obs_at(adapter, candidate, version),
        }
    })
}

fn current_skip(
    id: usize,
    adapter: &Adapter,
    locator: &str,
    stamp: &str,
    version: i64,
) -> Cache<bool> {
    STACK.with(|stack| {
        let stack = stack.borrow();
        match stack.iter().rev().find(|memo| memo.conn == id) {
            None => Cache::Absent,
            Some(memo) if !memo.armed => Cache::Absent,
            Some(memo) => memo.skip_at(adapter, locator, stamp, version),
        }
    })
}

fn finish<T>(
    conn: &Connection,
    read: Cache<T>,
    load_kind: impl FnOnce() -> Result<()>,
    again: impl FnOnce(i64) -> Cache<T>,
) -> Result<Read<T>> {
    match read {
        Cache::Hit(value) => Ok(Read::Hit(value)),
        Cache::Absent | Cache::Stale => Ok(Read::Ask),
        Cache::Pending => {
            load_kind()?;
            let version = data_version(conn)?;
            Ok(match again(version) {
                Cache::Hit(value) => Read::Hit(value),
                Cache::Absent | Cache::Pending | Cache::Stale => Read::Ask,
            })
        }
    }
}

#[cfg(test)]
fn on_load() {
    let hook = ON_LOAD.with(|hook| hook.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
fn reset_counts() {
    LOADS.with(|loads| loads.set(0));
    FALLBACKS.with(|falls| falls.set(0));
    ROLLBACKS.with(|rolls| rolls.set(0));
    WINDOW.with(|window| window.set(0));
    ON_LOAD.with(|hook| *hook.borrow_mut() = None);
}

#[cfg(test)]
fn load_count() -> u64 {
    LOADS.with(Cell::get)
}

#[cfg(test)]
fn fallback_count() -> u64 {
    FALLBACKS.with(Cell::get)
}

#[cfg(test)]
fn rollback_count() -> u64 {
    ROLLBACKS.with(Cell::get)
}

#[cfg(test)]
mod tests {
    use super::super::{
        discover_sessions_with_worker_limit, stored_stamp, Candidate, DiscoverOptions,
        DiscoveryEnv, IdentityRefresh, ScanEnv, ShallowSession, ShallowSessionProvider,
    };
    use super::{fallback_count, load_count, rollback_count, Guard, WINDOW};
    use crate::ingest::path_scope::PathScope;
    use crate::init_db;
    use anyhow::Result;
    use rusqlite::{params, Connection};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, SystemTime};

    struct Reset;

    impl Reset {
        fn arm() -> Self {
            super::reset_counts();
            Self
        }
    }

    impl Drop for Reset {
        fn drop(&mut self) {
            super::reset_counts();
        }
    }

    struct Claude;

    impl ShallowSessionProvider for Claude {
        fn source(&self) -> &'static str {
            "claude"
        }

        fn enumerate(
            &self,
            _env: &DiscoveryEnv<'_>,
            _requested_limit: Option<usize>,
        ) -> Result<Vec<Candidate>> {
            Ok(Vec::new())
        }

        fn read_shallow(
            &self,
            _scan: &ScanEnv<'_>,
            _catalog: Option<&Connection>,
            _candidate: &Candidate,
        ) -> Result<Option<ShallowSession>> {
            Ok(None)
        }
    }

    fn memory() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        init_db(&conn).unwrap();
        conn
    }

    fn file_db() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let conn = Connection::open(dir.path().join("history.db")).unwrap();
        init_db(&conn).unwrap();
        (dir, conn)
    }

    fn v(raw: &str) -> String {
        stored_stamp(raw)
    }

    fn candidate(id: Option<&str>, locator: &str) -> Candidate {
        Candidate {
            source: "claude",
            locator: locator.to_string(),
            session_id: id.map(str::to_string),
            recency_hint_ms: None,
            stamp: "old".to_string(),
        }
    }

    fn lookup(
        conn: &Connection,
        id: Option<&str>,
        locator: &str,
    ) -> Option<(String, Option<String>)> {
        super::observed_candidate_stamp(conn, &Claude, &candidate(id, locator)).unwrap()
    }

    fn stamp_text(found: &Option<(String, Option<String>)>) -> Option<String> {
        found.as_ref().and_then(|(_, stamp)| stamp.clone())
    }

    fn insert_obs(conn: &Connection, session: &str, locator: &str, stamp: &str, catalogued: bool) {
        if catalogued {
            conn.execute(
                "INSERT INTO sessions(session_id, source) VALUES(?1, 'claude')",
                [session],
            )
            .unwrap();
        }
        conn.execute(
            "INSERT INTO session_observations(source, session_id, location, connector_id, \
             connector_instance, raw_locator, source_stamp, updated_ms) \
             VALUES('claude', ?1, 'local', 'claude', 'default', ?2, ?3, 0)",
            params![session, locator, stamp],
        )
        .unwrap();
    }

    fn replace_stamp(conn: &Connection, session: &str, locator: &str) {
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        conn.execute(
            "UPDATE session_observations SET source_stamp=?1 WHERE session_id=?2",
            params![v("new"), session],
        )
        .unwrap();
        super::note_observation(conn, &Claude, "claude", session, locator);
        conn.execute_batch("COMMIT").unwrap();
    }

    #[test]
    fn observed_stamp_a_write_in_this_pass_drops_the_cached_stamp() {
        let _reset = Reset::arm();
        let conn = memory();
        insert_obs(&conn, "s", "/t/a.jsonl", &v("old"), true);
        let _guard = Guard::arm(&conn, None, None, 0);
        for id in [Some("s"), None] {
            assert_eq!(stamp_text(&lookup(&conn, id, "/t/a.jsonl")), Some(v("old")));
        }
        let loaded = load_count();
        replace_stamp(&conn, "s", "/t/a.jsonl");
        for id in [Some("s"), None] {
            assert_eq!(
                stamp_text(&lookup(&conn, id, "/t/a.jsonl")),
                Some(v("new")),
                "the pre-write stamp was still unchanged"
            );
        }
        assert_eq!(load_count(), loaded, "this connection's commit reloaded");
        assert!(fallback_count() >= 1);
    }

    #[test]
    fn observed_stamp_a_rollback_drops_uncommitted_rows() {
        let _reset = Reset::arm();
        let conn = memory();
        let _guard = Guard::arm(&conn, None, None, 0);
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        insert_obs(&conn, "s", "/t/a.jsonl", &v("old"), true);
        assert_eq!(stamp_text(&lookup(&conn, Some("s"), "/t/a.jsonl")), Some(v("old")));
        let loaded = load_count();
        conn.execute_batch("ROLLBACK").unwrap();
        super::on_rollback(&conn);
        assert_eq!(rollback_count(), 1);
        assert!(lookup(&conn, Some("s"), "/t/a.jsonl").is_none());
        assert!(load_count() > loaded);
    }

    #[test]
    fn observed_stamp_an_external_commit_reloads() {
        let _reset = Reset::arm();
        let (_dir, conn) = file_db();
        insert_obs(&conn, "s", "/t/a.jsonl", &v("old"), true);
        let _guard = Guard::arm(&conn, None, None, 0);
        assert_eq!(stamp_text(&lookup(&conn, Some("s"), "/t/a.jsonl")), Some(v("old")));
        let loaded = load_count();
        let other = Connection::open(_dir.path().join("history.db")).unwrap();
        other.busy_timeout(Duration::from_secs(5)).unwrap();
        other
            .execute(
                "UPDATE session_observations SET source_stamp=?1 WHERE session_id='s'",
                [v("new")],
            )
            .unwrap();
        assert_eq!(stamp_text(&lookup(&conn, Some("s"), "/t/a.jsonl")), Some(v("new")));
        assert!(load_count() > loaded);
    }

    #[test]
    fn observed_stamp_loading_skips_does_not_refresh_observations() {
        let _reset = Reset::arm();
        let (dir, conn) = file_db();
        insert_obs(&conn, "s", "/t/a.jsonl", &v("old"), true);
        let _guard = Guard::arm(&conn, None, None, 0);
        assert_eq!(stamp_text(&lookup(&conn, Some("s"), "/t/a.jsonl")), Some(v("old")));
        let loaded = load_count();
        let path = dir.path().join("history.db");
        super::ON_LOAD.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move || {
                let other = Connection::open(&path).unwrap();
                other.busy_timeout(Duration::from_secs(5)).unwrap();
                other
                    .execute(
                        "UPDATE session_observations SET source_stamp=?1 WHERE session_id='s'",
                        [v("new")],
                    )
                    .unwrap();
            }));
        });
        assert!(!super::is_known_non_session(&conn, &Claude, "claude", "/t/a.jsonl", &v("old")).unwrap());
        assert_eq!(
            stamp_text(&lookup(&conn, Some("s"), "/t/a.jsonl")),
            Some(v("new")),
            "the observation keys took the skip load's version"
        );
        assert!(load_count() > loaded);
    }

    #[test]
    fn observed_stamp_a_replaced_skip_is_not_still_known() {
        let _reset = Reset::arm();
        let conn = memory();
        conn.execute(
            "INSERT INTO observation_discovery_skips(source, location, connector_id, \
             connector_instance, locator, stamp, updated_ms) \
             VALUES('claude', 'local', 'claude', 'default', '/t/skip.jsonl', ?1, 0)",
            [v("old")],
        )
        .unwrap();
        let _guard = Guard::arm(&conn, None, None, 0);
        assert!(super::is_known_non_session(&conn, &Claude, "claude", "/t/skip.jsonl", &v("old")).unwrap());
        let loaded = load_count();
        super::record_non_session(&conn, &Claude, "claude", "/t/skip.jsonl", &v("new")).unwrap();
        assert!(!super::is_known_non_session(&conn, &Claude, "claude", "/t/skip.jsonl", &v("old")).unwrap());
        assert!(super::is_known_non_session(&conn, &Claude, "claude", "/t/skip.jsonl", &v("new")).unwrap());
        assert_eq!(load_count(), loaded);
        assert!(fallback_count() >= 1);
    }

    #[test]
    fn observed_stamp_an_uncatalogued_locator_does_not_match() {
        let _reset = Reset::arm();
        let conn = memory();
        insert_obs(&conn, "a", "/t/same.jsonl", &v("old"), false);
        insert_obs(&conn, "b", "/t/same.jsonl", &v("old"), true);
        let _guard = Guard::arm(&conn, None, None, 0);
        assert!(
            lookup(&conn, None, "/t/same.jsonl").is_none(),
            "the catalogued sibling won over the lower uncatalogued session id"
        );
        assert_eq!(load_count(), 1);
        assert_eq!(fallback_count(), 0);
    }

    #[test]
    fn observed_stamp_bulk_read_is_not_a_wider_scan_than_the_point_read() {
        let _reset = Reset::arm();
        for analyze in [false, true] {
            let conn = memory();
            for i in 0..40 {
                insert_obs(&conn, &format!("c{i}"), &format!("/c/{i}.jsonl"), "stamp", true);
                conn.execute(
                    "INSERT INTO session_observations(source, session_id, location, connector_id, \
                     connector_instance, raw_locator, source_stamp, updated_ms) \
                     VALUES('codex', ?1, 'local', 'codex', 'default', ?2, 'stamp', 0)",
                    params![format!("x{i}"), format!("/x/{i}.jsonl")],
                )
                .unwrap();
                conn.execute(
                    "INSERT INTO sessions(session_id, source) VALUES(?1, 'codex')",
                    [format!("x{i}")],
                )
                .unwrap();
            }
            if analyze {
                conn.execute_batch("ANALYZE").unwrap();
            }
            let point = explain(
                &conn,
                super::rows::STAMP_SQL,
                params!["claude", "c0", "local", "claude", "default"],
            );
            let bulk = explain(
                &conn,
                super::rows::OBSERVATION_ROWS_SQL,
                params!["claude", "local", "claude", "default"],
            );
            assert!(
                point.iter().any(|step| step.contains("SEARCH")),
                "analyze={analyze} point {point:?}"
            );
            assert!(
                bulk.iter().any(|step| step.contains("SEARCH"))
                    || !bulk.iter().any(|step| step.contains("SCAN")),
                "analyze={analyze} bulk read scans where the point read searches: {bulk:?}"
            );
        }
    }

    fn explain(conn: &Connection, sql: &str, params: impl rusqlite::Params) -> Vec<String> {
        let mut stmt = conn
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
            .unwrap();
        stmt.query_map(params, |row| row.get(3))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    }

    fn write_claude(home: &Path, id: &str) -> PathBuf {
        let path = home.join(format!(".claude/projects/proj/{id}.jsonl"));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(
            &path,
            format!(
                "{{\"sessionId\":\"{id}\",\"cwd\":\"/work/app\",\"type\":\"user\",\"message\":{{\"role\":\"user\",\"content\":\"hi\"}},\"timestamp\":\"2026-06-20T10:00:00.000Z\"}}\n"
            ),
        )
        .unwrap();
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        let when = SystemTime::UNIX_EPOCH + Duration::from_millis(1_781_000_000_000);
        file.set_times(std::fs::FileTimes::new().set_modified(when))
            .unwrap();
        path
    }

    fn claude_env<'a>(conn: &'a Connection, home: &Path) -> DiscoveryEnv<'a> {
        DiscoveryEnv::with_roots(conn, home.to_path_buf(), home.join("opencode.db"))
    }

    fn discover_claude(env: &DiscoveryEnv<'_>) -> crate::discover::DiscoverySummary {
        let options = DiscoverOptions {
            sources: vec!["claude".to_string()],
            ..Default::default()
        };
        crate::discover::discover_sessions_with_env(env, &options, |_| {}).unwrap()
    }

    #[test]
    fn observed_stamp_a_full_pass_reads_unchanged_candidates_once() {
        let _reset = Reset::arm();
        let home = tempfile::tempdir().unwrap();
        let conn = memory();
        for index in 0..12 {
            write_claude(home.path(), &format!("s{index:02}"));
        }
        let first = discover_claude(&claude_env(&conn, home.path()));
        assert_eq!(first.counters.shallow_reads, 12);
        assert_eq!(first.skipped_unchanged, 0);
        super::reset_counts();
        let second = discover_claude(&claude_env(&conn, home.path()));
        assert_eq!(
            load_count(),
            1,
            "loads {} fallbacks {} reads {} skipped {}",
            load_count(),
            fallback_count(),
            second.counters.shallow_reads,
            second.skipped_unchanged
        );
        assert_eq!(fallback_count(), 0, "fallbacks {}", fallback_count());
        assert_eq!(second.counters.shallow_reads, 0);
        assert_eq!(second.skipped_unchanged, 12);
        assert_eq!(rollback_count(), 0);
    }

    #[test]
    fn observed_stamp_a_path_scoped_file_does_not_load_the_set() {
        let _reset = Reset::arm();
        let home = tempfile::tempdir().unwrap();
        let conn = memory();
        let mut one = PathBuf::new();
        for index in 0..12 {
            let path = write_claude(home.path(), &format!("s{index:02}"));
            if index == 0 {
                one = path;
            }
        }
        let env = claude_env(&conn, home.path());
        discover_claude(&env);
        super::reset_counts();
        let scoped = claude_env(&conn, home.path()).with_path_scope(PathScope::from_paths([one]));
        let second = discover_claude(&scoped);
        assert_eq!(load_count(), 0, "a one-file pass loaded every stamp");
        assert!(fallback_count() > 0, "the point query did not run");
        assert_eq!(second.counters.shallow_reads, 0);
        assert_eq!(second.skipped_unchanged, 1);
    }

    #[test]
    fn observed_stamp_a_limited_pass_does_not_load_the_set() {
        let _reset = Reset::arm();
        let home = tempfile::tempdir().unwrap();
        let conn = memory();
        for index in 0..12 {
            write_claude(home.path(), &format!("s{index:02}"));
        }
        discover_claude(&claude_env(&conn, home.path()));
        super::reset_counts();
        let options = DiscoverOptions {
            sources: vec!["claude".to_string()],
            limit: Some(1),
            ..Default::default()
        };
        let second = crate::discover::discover_sessions_with_env(
            &claude_env(&conn, home.path()),
            &options,
            |_| {},
        )
        .unwrap();
        assert_eq!(load_count(), 0, "a one-session limit loaded every stamp");
        assert!(fallback_count() > 0, "the point query did not run");
        assert_eq!(second.counters.shallow_reads, 0);
        assert_eq!(second.skipped_unchanged, 1);
    }

    struct Pair {
        reads: AtomicUsize,
        named: bool,
    }

    impl ShallowSessionProvider for Pair {
        fn source(&self) -> &'static str {
            "claude"
        }

        fn enumerate(
            &self,
            _env: &DiscoveryEnv<'_>,
            _requested_limit: Option<usize>,
        ) -> Result<Vec<Candidate>> {
            let id = self.named.then(|| "same".to_string());
            Ok(vec![
                Candidate {
                    source: "claude",
                    locator: "/t/new.jsonl".to_string(),
                    session_id: id.clone(),
                    recency_hint_ms: Some(2),
                    stamp: "new".to_string(),
                },
                Candidate {
                    source: "claude",
                    locator: "/t/old.jsonl".to_string(),
                    session_id: id,
                    recency_hint_ms: Some(1),
                    stamp: "old".to_string(),
                },
            ])
        }

        fn read_shallow(
            &self,
            _scan: &ScanEnv<'_>,
            _catalog: Option<&Connection>,
            candidate: &Candidate,
        ) -> Result<Option<ShallowSession>> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            Ok(Some(ShallowSession {
                source: "claude".to_string(),
                session_id: "same".to_string(),
                raw_path: Some(candidate.locator.clone()),
                discovery_state: "shallow".to_string(),
                ..Default::default()
            }))
        }
    }

    #[test]
    fn observed_stamp_a_replaced_stamp_is_not_served_unchanged() {
        for named in [true, false] {
            let _reset = Reset::arm();
            WINDOW.with(|window| window.set(1));
            let home = tempfile::tempdir().unwrap();
            let conn = memory();
            insert_obs(&conn, "same", "/t/old.jsonl", &v("old"), true);
            let env = claude_env(&conn, home.path());
            let provider = Pair {
                reads: AtomicUsize::new(0),
                named,
            };
            let providers: [&dyn ShallowSessionProvider; 1] = [&provider];
            let summary = discover_sessions_with_worker_limit(
                &env,
                &DiscoverOptions::default(),
                &providers,
                |_| {},
                1,
                IdentityRefresh::Sweep,
            )
            .unwrap();
            assert_eq!(provider.reads.load(Ordering::Relaxed), 2, "named={named}");
            assert_eq!(summary.skipped_unchanged, 0, "named={named}");
            // The new file is not unchanged, so the pass also reads the skip
            // set once. A reload of either set after this connection's commit
            // would be a third load.
            assert_eq!(load_count(), 2, "named={named} loads {}", load_count());
            assert!(fallback_count() >= 1, "named={named}");
            let stamp: String = conn
                .query_row(
                    "SELECT source_stamp FROM session_observations WHERE session_id='same'",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(stamp, v("old"), "named={named}");
        }
    }

    struct Two {
        reads: AtomicUsize,
    }

    impl ShallowSessionProvider for Two {
        fn source(&self) -> &'static str {
            "claude"
        }

        fn enumerate(
            &self,
            _env: &DiscoveryEnv<'_>,
            _requested_limit: Option<usize>,
        ) -> Result<Vec<Candidate>> {
            Ok(vec![
                Candidate {
                    source: "claude",
                    locator: "/t/a.jsonl".to_string(),
                    session_id: Some("a".to_string()),
                    recency_hint_ms: Some(2),
                    stamp: "old".to_string(),
                },
                Candidate {
                    source: "claude",
                    locator: "/t/b.jsonl".to_string(),
                    session_id: Some("b".to_string()),
                    recency_hint_ms: Some(1),
                    stamp: "old".to_string(),
                },
            ])
        }

        fn read_shallow(
            &self,
            _scan: &ScanEnv<'_>,
            _catalog: Option<&Connection>,
            candidate: &Candidate,
        ) -> Result<Option<ShallowSession>> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            Ok(Some(ShallowSession {
                source: "claude".to_string(),
                session_id: candidate.session_id.clone().unwrap_or_default(),
                raw_path: Some(candidate.locator.clone()),
                discovery_state: "shallow".to_string(),
                ..Default::default()
            }))
        }
    }

    #[test]
    fn observed_stamp_a_callback_write_is_not_served_unchanged() {
        let _reset = Reset::arm();
        WINDOW.with(|window| window.set(1));
        let home = tempfile::tempdir().unwrap();
        let conn = memory();
        insert_obs(&conn, "a", "/t/a.jsonl", &v("old"), true);
        insert_obs(&conn, "b", "/t/b.jsonl", &v("old"), true);
        let env = claude_env(&conn, home.path());
        let provider = Two {
            reads: AtomicUsize::new(0),
        };
        let providers: [&dyn ShallowSessionProvider; 1] = [&provider];
        let summary = discover_sessions_with_worker_limit(
            &env,
            &DiscoverOptions::default(),
            &providers,
            |row| {
                if row.session_id == "a" {
                    conn.execute(
                        "UPDATE session_observations SET source_stamp=?1 WHERE session_id='b'",
                        [v("rewritten")],
                    )
                    .unwrap();
                }
            },
            1,
            IdentityRefresh::Streamed,
        )
        .unwrap();
        assert_eq!(summary.skipped_unchanged, 1);
        assert_eq!(provider.reads.load(Ordering::Relaxed), 1);
    }

    struct BadId;

    impl ShallowSessionProvider for BadId {
        fn source(&self) -> &'static str {
            "claude"
        }

        fn enumerate(
            &self,
            _env: &DiscoveryEnv<'_>,
            _requested_limit: Option<usize>,
        ) -> Result<Vec<Candidate>> {
            Ok(vec![Candidate {
                source: "claude",
                locator: "/t/bad.jsonl".to_string(),
                session_id: None,
                recency_hint_ms: Some(1),
                stamp: "bad".to_string(),
            }])
        }

        fn read_shallow(
            &self,
            _scan: &ScanEnv<'_>,
            _catalog: Option<&Connection>,
            _candidate: &Candidate,
        ) -> Result<Option<ShallowSession>> {
            Ok(Some(ShallowSession {
                source: "claude".to_string(),
                session_id: "bad\nid".to_string(),
                discovery_state: "shallow".to_string(),
                ..Default::default()
            }))
        }
    }

    #[test]
    fn observed_stamp_a_rejected_observation_rolls_the_snapshot_back() {
        let _reset = Reset::arm();
        let home = tempfile::tempdir().unwrap();
        let conn = memory();
        let env = claude_env(&conn, home.path());
        let provider = BadId;
        let providers: [&dyn ShallowSessionProvider; 1] = [&provider];
        let error = discover_sessions_with_worker_limit(
            &env,
            &DiscoverOptions::default(),
            &providers,
            |_| {},
            1,
            IdentityRefresh::Sweep,
        )
        .unwrap_err();
        assert!(
            format!("{error:#}").contains("invalid observation identity"),
            "{error:#}"
        );
        assert_eq!(rollback_count(), 1);
        let left: i64 = conn
            .query_row("SELECT COUNT(*) FROM sessions", [], |row| row.get(0))
            .unwrap();
        assert_eq!(left, 0);
    }
}

pub(super) fn matches(fresh: bool, stamp: Option<&str>, expected: &str) -> bool {
    !fresh && stamp == Some(expected)
}
