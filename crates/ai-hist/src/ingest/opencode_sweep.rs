//! The sweep's OpenCode pass: every store, reading only what changed.
//!
//! A session is read and written again only when its stamp moved since the
//! sweep that last wrote it. Two stamps, both kept in the sync state:
//!
//! * **Store stamp** (`opencode_stores_v1`): device, inode, length and mtime of
//!   a SQLite store and its WAL. Every committed OpenCode write lands in one of
//!   the two files, so an equal stamp means no session in the store changed
//!   and the store is not opened at all. The `-shm` index is not part of it:
//!   every reader, this sweep included, writes its read marks there.
//! * **Session stamp** (`opencode_sessions_v1`, `opencode_tree_sessions_v1`):
//!   for a SQLite session, the session row's fields and, per `message` and
//!   `part` table, the row count, newest `time_updated` and a sum of per-row
//!   hashes of id, `time_updated` and payload length -- one aggregate pass
//!   over each table that reads no payload. For the legacy JSON tree, the
//!   stamp discovery already takes ([`stamp_json_tree_session`]).
//!
//! Both are qualified by the sweep generation, so a parser upgrade re-reads
//! every session once. A matching stamp is trusted only while the session's
//! evidence is still there and the destination marker does not name it short
//! -- the guard every other provider's skip has, and what brings back a
//! session `forget_evidence` removed.
//!
//! A stamp is recorded only when it is definite. A session written within
//! [`AMBIGUITY_MS`] of the read could be rewritten in the same millisecond at
//! the same length, and a store file that recent could be rewritten within one
//! filesystem tick; both stay unrecorded, so the next sweep reads them again.
//! The store stamp is taken before the read transaction opens and recorded
//! only when every session in the store got a definite stamp, so a write
//! landing during the read moves it and is read by the next sweep.
//!
//! [`stamp_json_tree_session`]: super::opencode::stamp_json_tree_session

use super::opencode::{self, OpencodeSession, OpencodeSyncPlan};
use super::{
    check_capture_cancelled, forget_unobserved_paths, session_events_exist, session_markers_exist,
    sweep_generation, SweepRepairs, SyncStateStamp,
};
use crate::discover::fingerprint_hash;
use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags};
use serde_json::{Map, Value};
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

#[cfg(test)]
mod tests;

const SESSION_STAMPS_KEY: &str = "opencode_sessions_v1";
const STORE_STAMPS_KEY: &str = "opencode_stores_v1";
const TREE_STAMPS_KEY: &str = "opencode_tree_sessions_v1";

/// How recent a write has to be before its stamp cannot vouch for it.
const AMBIGUITY_MS: i64 = 2_000;

/// The value of a session this store holds that an earlier channel store
/// owns.
const CLAIMED: &str = "c";
/// Prefixes of a recorded session stamp: whether the session held evidence
/// when it was stamped.
const EVIDENCE_HELD: &str = "e";
const NOTHING_HELD: &str = "n";

/// One of the three stamp maps.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Stamps {
    Sessions,
    Stores,
    Tree,
}

impl Stamps {
    const ALL: [Self; 3] = [Self::Sessions, Self::Stores, Self::Tree];

    fn key(self) -> &'static str {
        match self {
            Self::Sessions => SESSION_STAMPS_KEY,
            Self::Stores => STORE_STAMPS_KEY,
            Self::Tree => TREE_STAMPS_KEY,
        }
    }
}

/// The stamps one sweep reads and records, and what licenses a skip.
pub(crate) struct OpencodeSweep<'a> {
    repairs: &'a SweepRepairs,
    generation: String,
    maps: [Map<String, Value>; 3],
    /// Entries to drop from the on-disk maps.
    forgotten: [Vec<String>; 3],
}

impl<'a> OpencodeSweep<'a> {
    /// Take the stamp maps out of `state` for the pass; [`Self::finish`]
    /// puts them back.
    pub(crate) fn begin(state: &mut Map<String, Value>, repairs: &'a SweepRepairs) -> Self {
        let maps = Stamps::ALL.map(|map| match state.remove(map.key()) {
            Some(Value::Object(stamps)) => stamps,
            _ => Map::new(),
        });
        Self {
            repairs,
            generation: sweep_generation(),
            maps,
            forgotten: Default::default(),
        }
    }

    /// Put the stamps back into `state`, with the dropped entries recorded
    /// for the checkpoint merge.
    pub(crate) fn finish(self, state: &mut Map<String, Value>) {
        for ((map, mut stamps), forgotten) in
            Stamps::ALL.into_iter().zip(self.maps).zip(self.forgotten)
        {
            let _ = forget_unobserved_paths(state, &mut stamps, map.key(), forgotten);
            state.insert(map.key().to_string(), Value::Object(stamps));
        }
    }

    fn map(&self, map: Stamps) -> &Map<String, Value> {
        &self.maps[map as usize]
    }

    fn insert(&mut self, map: Stamps, entry: String, value: String) {
        self.maps[map as usize].insert(entry, Value::String(value));
    }

    fn forget(&mut self, map: Stamps, entry: String) {
        self.maps[map as usize].remove(&entry);
        self.forgotten[map as usize].push(entry);
    }

    /// Whether the recorded value for one session says the evidence it held
    /// then is the evidence `stamp` describes now, and that evidence is
    /// intact.
    fn session_current(
        &self,
        conn: &Connection,
        map: Stamps,
        entry: &str,
        session_id: &str,
        stamp: &str,
    ) -> Result<bool> {
        match recorded(self.map(map).get(entry)) {
            Some((held, recorded)) if recorded == stamp => self.intact(conn, session_id, held),
            _ => Ok(false),
        }
    }

    /// Whether the evidence a session held when it was stamped is still
    /// there: the destination marker does not name it short and, if it held
    /// any rows, it still holds some.
    fn intact(&self, conn: &Connection, session_id: &str, held: &str) -> Result<bool> {
        if self.repairs.contains("opencode", session_id) {
            return Ok(false);
        }
        Ok(held == NOTHING_HELD || session_holds_evidence(conn, session_id)?)
    }

    /// Record one session's stamp once its evidence is written.
    fn record(
        &mut self,
        conn: &Connection,
        map: Stamps,
        entry: String,
        session_id: &str,
        stamp: &str,
    ) -> Result<()> {
        let held = if session_holds_evidence(conn, session_id)? {
            EVIDENCE_HELD
        } else {
            NOTHING_HELD
        };
        self.insert(map, entry, format!("{held}:{stamp}"));
        Ok(())
    }

    /// The opaque session stamp over `parts`, qualified by the generation.
    fn session_stamp(&self, parts: &str) -> String {
        format!(
            "{:016x}",
            fingerprint_hash("opencode-session", &self.generation, parts)
        )
    }
}

/// Run `pass` with no recorded stamps and no repairs, so it reads every
/// session, and discard the stamps it takes.
pub(crate) fn unstamped<T>(pass: impl FnOnce(&mut OpencodeSweep<'_>) -> Result<T>) -> Result<T> {
    let repairs = SweepRepairs::default();
    let mut sweep = OpencodeSweep::begin(&mut Map::new(), &repairs);
    pass(&mut sweep)
}

/// `(held, stamp)` from a recorded session value.
fn recorded(value: Option<&Value>) -> Option<(&str, &str)> {
    value.and_then(Value::as_str)?.split_once(':')
}

fn session_holds_evidence(conn: &Connection, session_id: &str) -> Result<bool> {
    Ok(session_events_exist(conn, "opencode", session_id)?
        || session_markers_exist(conn, "opencode", session_id)?)
}

/// A SQLite store's sessions are kept under a hash of its path, so 40,000
/// entries do not each repeat it.
fn store_tag(store: &str) -> String {
    format!("{:016x}", fingerprint_hash("opencode-store", store, ""))
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

/// The store stamp, or `None` when the files cannot vouch for their content:
/// unreadable metadata (or no inode to read, off Unix), or an mtime within
/// [`AMBIGUITY_MS`] of now.
fn store_file_stamp(path: &Path, generation: &str) -> Option<String> {
    let db = SyncStateStamp::at(path)?;
    let mut wal_path = path.as_os_str().to_os_string();
    wal_path.push("-wal");
    let wal = SyncStateStamp::at(Path::new(&wal_path));
    let newest_ns = db.mtime_ns.max(wal.map_or(0, |wal| wal.mtime_ns));
    let newest_ms = i64::try_from(newest_ns / 1_000_000).unwrap_or(i64::MAX);
    if now_ms().saturating_sub(newest_ms) <= AMBIGUITY_MS {
        return None;
    }
    Some(format!("{generation}:{db:?}:{wal:?}"))
}

/// Index every session in each OpenCode SQLite store, in order, reading only
/// the sessions whose stamps moved.
///
/// OpenCode keeps one database per release channel, and the caller passes
/// them in [`crate::paths::opencode_db_files`] order. A session present in
/// more than one is claimed by the first store that holds it -- the same rule
/// discovery applies -- so it is indexed once, from the store its catalog row
/// names. One store's failure is that store's: the rest are still indexed,
/// and the error names every store that failed.
pub(crate) fn sync_opencode_dbs(
    conn: &Connection,
    opencode_dbs: &[PathBuf],
    sweep: &mut OpencodeSweep<'_>,
) -> Result<usize> {
    let files: Vec<&Path> = opencode_dbs
        .iter()
        .map(PathBuf::as_path)
        .filter(|path| path.is_file())
        .collect();
    let mut inserted = 0;
    let mut claimed = BTreeSet::new();
    let mut failures: Vec<anyhow::Error> = Vec::new();
    for path in super::capture_files("opencode", files) {
        match sync_opencode_db_file(conn, path, &mut claimed, sweep) {
            Ok(count) => inserted += count,
            Err(error) => {
                // Cancellation ends the whole sweep, not one store.
                check_capture_cancelled()?;
                failures.push(error);
            }
        }
    }
    check_capture_cancelled()?;
    match failures.len() {
        0 => Ok(inserted),
        1 => Err(failures.remove(0)),
        _ => anyhow::bail!(
            "{} OpenCode stores could not be fully indexed (the rest were): {}",
            failures.len(),
            failures
                .iter()
                .map(|error| format!("{error:#}"))
                .collect::<Vec<_>>()
                .join("; ")
        ),
    }
}

fn sync_opencode_db_file(
    conn: &Connection,
    opencode_db: &Path,
    claimed: &mut BTreeSet<String>,
    sweep: &mut OpencodeSweep<'_>,
) -> Result<usize> {
    check_capture_cancelled()?;
    // `is_file`, the question `OpencodeLayout::detect` asks: an `OPENCODE_DB`
    // naming a directory is an absent store, not an SQLite error.
    if !opencode_db.is_file() {
        return Ok(0);
    }
    let store = opencode_db.to_string_lossy().into_owned();
    let tag = store_tag(&store);
    // Before the read transaction opens: a write after this point moves the
    // files past the stamp.
    let file_stamp = store_file_stamp(opencode_db, &sweep.generation);
    if let Some(stamp) = &file_stamp {
        if store_unchanged(conn, sweep, &tag, stamp, claimed)? {
            return Ok(0);
        }
    }
    let src = open_source(opencode_db)?;
    crate::ingest::devin::register_stamp_fn(&src)?;
    src.execute_batch("BEGIN")?;
    let result = sync_store_snapshot(conn, &src, &store, &tag, claimed, sweep);
    let _ = src.execute_batch("ROLLBACK");
    let (inserted, definite) = result?;
    match file_stamp.filter(|_| definite) {
        Some(stamp) => sweep.insert(Stamps::Stores, tag, stamp),
        None => sweep.forget(Stamps::Stores, tag),
    }
    Ok(inserted)
}

/// The read-only provider connection: the live store, or a whole-database
/// copy when `AI_HIST_OPENCODE_BACKUP=1` asks for one.
fn open_source(opencode_db: &Path) -> Result<Connection> {
    #[cfg(test)]
    tests::note_read(1, 0);
    #[cfg(feature = "opencode-backup")]
    if crate::store::opencode_backup_requested() {
        return crate::store::opencode_backup_copy(opencode_db);
    }
    let src = Connection::open_with_flags(
        opencode_db,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .with_context(|| format!("opening {}", opencode_db.display()))?;
    src.busy_timeout(std::time::Duration::from_secs(5))?;
    Ok(src)
}

/// Whether a store whose files match their recorded stamp is skipped whole;
/// if so, its sessions are claimed. Every session it owned must still pass
/// the per-session guard, and every one an earlier store owned must still be
/// owned by one -- otherwise this store owns it now and has to read it.
fn store_unchanged(
    conn: &Connection,
    sweep: &OpencodeSweep<'_>,
    tag: &str,
    stamp: &str,
    claimed: &mut BTreeSet<String>,
) -> Result<bool> {
    if sweep.map(Stamps::Stores).get(tag).and_then(Value::as_str) != Some(stamp)
        || sweep.repairs.repairs_all()
    {
        return Ok(false);
    }
    let prefix = format!("{tag}:");
    let mut owned = Vec::new();
    for (entry, value) in sweep.map(Stamps::Sessions) {
        let Some(session_id) = entry.strip_prefix(&prefix) else {
            continue;
        };
        if value.as_str() == Some(CLAIMED) {
            if !claimed.contains(session_id) {
                return Ok(false);
            }
            continue;
        }
        match recorded(Some(value)) {
            Some((held, _)) if sweep.intact(conn, session_id, held)? => {
                owned.push(session_id.to_string());
            }
            _ => return Ok(false),
        }
    }
    claimed.extend(owned);
    Ok(true)
}

/// Index the changed sessions of one store's pinned snapshot. Returns the
/// prompts inserted and whether every session the store owns has a definite
/// stamp recorded.
///
/// `claimed` holds the sessions an earlier store already owns; they are
/// skipped here, and this store's own sessions are added to it.
fn sync_store_snapshot(
    conn: &Connection,
    src: &Connection,
    store: &str,
    tag: &str,
    claimed: &mut BTreeSet<String>,
    sweep: &mut OpencodeSweep<'_>,
) -> Result<(usize, bool)> {
    check_capture_cancelled()?;
    let read_at = now_ms();
    let stamps = sqlite_session_stamps(src)?;
    let mut definite = true;
    let mut seen = BTreeSet::new();
    let mut changed = Vec::new();
    for stamp in stamps {
        check_capture_cancelled()?;
        let entry = format!("{tag}:{}", stamp.session_id);
        seen.insert(entry.clone());
        if !claimed.insert(stamp.session_id.clone()) {
            sweep.insert(Stamps::Sessions, entry, CLAIMED.to_string());
            continue;
        }
        let token = sweep.session_stamp(&stamp.parts);
        if sweep.session_current(conn, Stamps::Sessions, &entry, &stamp.session_id, &token)? {
            continue;
        }
        let settled = read_at.saturating_sub(stamp.newest_ms) > AMBIGUITY_MS;
        definite &= settled;
        changed.push((stamp.session_id, entry, settled.then_some(token)));
    }
    let prefix = format!("{tag}:");
    let gone: Vec<String> = sweep
        .map(Stamps::Sessions)
        .keys()
        .filter(|entry| entry.starts_with(&prefix) && !seen.contains(*entry))
        .cloned()
        .collect();
    for entry in gone {
        sweep.forget(Stamps::Sessions, entry);
    }
    if changed.is_empty() {
        return Ok((0, definite));
    }
    // One session's failure is that session's failure: the loader reports a
    // row it cannot map rather than calling the session absent, and ending
    // the sweep at the first bad row would leave every session after it
    // unindexed for as long as that row stays bad.
    let mut failures: Vec<String> = Vec::new();
    let mut inserted = 0;
    let mut index = |sweep: &mut OpencodeSweep<'_>,
                     session_id: &str,
                     entry: String,
                     token: Option<String>,
                     loaded: Result<Option<OpencodeSession>>|
     -> Result<bool> {
        #[cfg(test)]
        tests::note_read(0, 1);
        let written = loaded.and_then(|loaded| match loaded {
            Some(loaded) => opencode::normalize(conn, &loaded, store).map(|counts| counts.prompts),
            None => Ok(0),
        });
        match written {
            Ok(prompts) => {
                inserted += prompts;
                match token {
                    Some(token) => {
                        sweep.record(conn, Stamps::Sessions, entry, session_id, &token)?
                    }
                    None => sweep.forget(Stamps::Sessions, entry),
                }
                Ok(true)
            }
            Err(error) => {
                check_capture_cancelled()?;
                failures.push(format!("{session_id}: {error:#}"));
                sweep.forget(Stamps::Sessions, entry);
                Ok(false)
            }
        }
    };
    // Session-keyed queries are bounded only when the provider indexes the
    // column they seek on; without that index each one scans `part`, so the
    // store is read whole once instead.
    match opencode::sync_plan(src)? {
        OpencodeSyncPlan::PerSession => {
            for (session_id, entry, token) in changed {
                check_capture_cancelled()?;
                let loaded = opencode::load_from_sqlite(src, &session_id);
                definite &= index(sweep, &session_id, entry, token, loaded)?;
            }
        }
        OpencodeSyncPlan::SinglePass => {
            let load = opencode::load_all_from_sqlite(src)?;
            let mut sessions: HashMap<String, OpencodeSession> = load
                .sessions
                .into_iter()
                .map(|loaded| (loaded.session.id.clone(), loaded))
                .collect();
            let unreadable: HashMap<String, String> = load
                .failures
                .into_iter()
                .map(|failure| (failure.session_id, failure.error))
                .collect();
            for (session_id, entry, token) in changed {
                check_capture_cancelled()?;
                let loaded = match unreadable.get(&session_id) {
                    Some(error) => Err(anyhow::anyhow!("{error}")),
                    None => Ok(sessions.remove(&session_id)),
                };
                definite &= index(sweep, &session_id, entry, token, loaded)?;
            }
        }
    }
    if !failures.is_empty() {
        anyhow::bail!(
            "{} OpenCode session(s) in {store} could not be read (the rest were indexed): {}",
            failures.len(),
            failures.join("; ")
        );
    }
    Ok((inserted, definite))
}

/// Index every changed OpenCode session in a legacy `storage/` JSON tree.
///
/// One session's failure is that session's failure: each is indexed or
/// reported on its own, and the error at the end names them all.
pub(crate) fn sync_opencode_storage_dir(
    conn: &Connection,
    storage_dir: &Path,
    sweep: &mut OpencodeSweep<'_>,
) -> Result<usize> {
    check_capture_cancelled()?;
    if !storage_dir.join("session").is_dir() {
        return Ok(0);
    }
    let mut inserted = 0;
    let listing = opencode::list_json_tree_session_files(storage_dir);
    // A subtree that could not be walked is not a subtree with no sessions in
    // it: it joins the failures, so the sessions under it are reported
    // missing instead of silently absent.
    let mut failures: Vec<String> = listing
        .unreadable
        .iter()
        .map(|dir| format!("{}: {}", dir.path.display(), dir.error))
        .collect();
    let root = storage_dir.join("session").to_string_lossy().into_owned();
    let mut seen = BTreeSet::new();
    for session_file in super::capture_files("opencode", listing.sessions) {
        check_capture_cancelled()?;
        let entry = session_file.to_string_lossy().into_owned();
        seen.insert(entry.clone());
        let session_id = session_file
            .file_stem()
            .map(|stem| stem.to_string_lossy().into_owned())
            .unwrap_or_default();
        // Taken before the read, so a write after it moves the stamp.
        let token = opencode::stamp_json_tree_session(&session_file, &session_id)
            .ok()
            .map(|stamp| sweep.session_stamp(&stamp.token()));
        if let Some(token) = &token {
            if sweep.session_current(conn, Stamps::Tree, &entry, &session_id, token)? {
                continue;
            }
        }
        #[cfg(test)]
        tests::note_read(0, 1);
        let indexed =
            opencode::load_from_json_tree(&session_file).and_then(|loaded| match loaded {
                Some(loaded) => opencode::normalize(conn, &loaded, &entry)
                    .map(|counts| (counts.prompts, Some(loaded.session.id))),
                None => Ok((0, None)),
            });
        match indexed {
            Ok((prompts, read_id)) => {
                inserted += prompts;
                // Under the id the file holds, which is what its evidence is
                // keyed by; a skip asks about the file name's.
                match (token, read_id) {
                    (Some(token), Some(read_id)) if read_id == session_id => {
                        sweep.record(conn, Stamps::Tree, entry, &read_id, &token)?;
                    }
                    _ => sweep.forget(Stamps::Tree, entry),
                }
            }
            Err(error) => {
                failures.push(format!("{}: {error:#}", session_file.display()));
                sweep.forget(Stamps::Tree, entry);
            }
        }
    }
    let gone: Vec<String> = sweep
        .map(Stamps::Tree)
        .keys()
        .filter(|entry| entry.starts_with(&root) && !seen.contains(*entry))
        .cloned()
        .collect();
    for entry in gone {
        sweep.forget(Stamps::Tree, entry);
    }
    if !failures.is_empty() {
        anyhow::bail!(
            "{} OpenCode path(s) under {} could not be read (the rest were indexed): {}",
            failures.len(),
            storage_dir.display(),
            failures.join("; ")
        );
    }
    Ok(inserted)
}

/// One SQLite session's stamp inputs: its row's fields and its `message` and
/// `part` aggregates, plus the newest `time_updated` among them.
struct SessionStampRow {
    session_id: String,
    parts: String,
    newest_ms: i64,
}

/// `(count, newest time_updated, sum of row hashes)` per session.
type TableAggregate = HashMap<String, (i64, Option<i64>, i64)>;

/// Every session's stamp inputs, in one pass over each table.
///
/// Each row's hash covers its id, `time_updated` and payload length --
/// `octet_length` reads the length from the record header, not the payload --
/// so a rewritten row moves the sum even when the count and the newest
/// timestamp stay where they were. The columns read are the ones the loaders
/// read; a schema without one stamps without it.
fn sqlite_session_stamps(src: &Connection) -> Result<Vec<SessionStampRow>> {
    let session_columns = opencode::table_columns(src, "session")?;
    if !session_columns.contains("id") {
        return Ok(Vec::new());
    }
    let column = |name| opencode::optional_column(&session_columns, name);
    let messages = message_aggregate(src)?;
    let parts = part_aggregate(src)?;
    let sql = format!(
        "SELECT id, {}, {}, {}, {} FROM session WHERE id IS NOT NULL AND id <> ''",
        column("parent_id"),
        column("directory"),
        column("time_created"),
        column("time_updated"),
    );
    let mut stmt = src.prepare(&sql)?;
    let rows = stmt.query_map([], |row| {
        let fields: [rusqlite::types::Value; 4] =
            [row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?];
        Ok((row.get::<_, String>(0)?, fields))
    })?;
    let mut stamps = Vec::new();
    for row in rows {
        let (session_id, fields) = row?;
        let message = messages.get(&session_id).copied().unwrap_or_default();
        let part = parts.get(&session_id).copied().unwrap_or_default();
        let row_ms = [&fields[2], &fields[3]]
            .into_iter()
            .filter_map(|value| match value {
                rusqlite::types::Value::Integer(ms) => Some(*ms),
                _ => None,
            })
            .max();
        let newest_ms = [row_ms, message.1, part.1]
            .into_iter()
            .flatten()
            .max()
            .unwrap_or_default();
        stamps.push(SessionStampRow {
            parts: format!("{fields:?}|m{message:?}|p{part:?}"),
            session_id,
            newest_ms,
        });
    }
    Ok(stamps)
}

fn message_aggregate(src: &Connection) -> Result<TableAggregate> {
    let columns = opencode::table_columns(src, "message")?;
    if !["id", "data", "session_id"]
        .iter()
        .all(|name| columns.contains(*name))
    {
        return Ok(TableAggregate::new());
    }
    let updated = opencode::optional_column(&columns, "time_updated");
    aggregate(
        src,
        &format!(
            "SELECT session_id, COUNT(*), MAX({updated}), \
             SUM(ai_hist_fnv(id || '|' || COALESCE(CAST({updated} AS TEXT), '') \
                 || '|' || COALESCE(octet_length(data), -1))) \
             FROM message GROUP BY session_id"
        ),
    )
}

/// Grouped by the part's own `session_id` when it has one, otherwise by its
/// message's session -- the same parts either loader reads.
fn part_aggregate(src: &Connection) -> Result<TableAggregate> {
    let columns = opencode::table_columns(src, "part")?;
    if !["id", "data", "message_id"]
        .iter()
        .all(|name| columns.contains(*name))
    {
        return Ok(TableAggregate::new());
    }
    let updated = match columns.contains("time_updated") {
        true => "p.time_updated",
        false => "NULL",
    };
    let (session, from) = if columns.contains("session_id") {
        ("p.session_id", "part p")
    } else {
        let message = opencode::table_columns(src, "message")?;
        if !message.contains("id") || !message.contains("session_id") {
            return Ok(TableAggregate::new());
        }
        (
            "m.session_id",
            "part p JOIN message m ON m.id = p.message_id",
        )
    };
    aggregate(
        src,
        &format!(
            "SELECT {session}, COUNT(*), MAX({updated}), \
             SUM(ai_hist_fnv(p.id || '|' || p.message_id || '|' \
                 || COALESCE(CAST({updated} AS TEXT), '') \
                 || '|' || COALESCE(octet_length(p.data), -1))) \
             FROM {from} GROUP BY {session}"
        ),
    )
}

fn aggregate(src: &Connection, sql: &str) -> Result<TableAggregate> {
    let mut stmt = src.prepare(sql)?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, Option<String>>(0)?,
            (row.get(1)?, row.get(2)?, row.get(3)?),
        ))
    })?;
    let mut out = TableAggregate::new();
    for row in rows {
        let (session_id, aggregate) = row?;
        if let Some(session_id) = session_id {
            out.insert(session_id, aggregate);
        }
    }
    Ok(out)
}
