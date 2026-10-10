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
//!   hashes of id, `time_updated` and payload -- one aggregate pass over each
//!   table, which the parse and the writes it saves cost many times over.
//!   For the legacy JSON tree, the stamp discovery already takes
//!   ([`stamp_json_tree_session`]).
//!
//! Both are qualified by the sweep generation, so a parser upgrade re-reads
//! every session once. A matching stamp is trusted only while the session's
//! evidence -- catalog row, events and markers, parent edge -- is still there
//! and the destination marker does not name it short: the guard every other
//! provider's skip has, and what brings back a session `forget_evidence`
//! removed.
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
use super::{check_capture_cancelled, forget_unobserved_paths, sweep_generation, SweepRepairs};
use crate::discover::fingerprint_hash;
use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags};
use serde_json::{Map, Value};
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

mod holdings;
mod stamps;
#[cfg(test)]
mod tests;
mod tree;

pub(crate) use tree::sync_opencode_storage_dir;

const SESSION_STAMPS_KEY: &str = "opencode_sessions_v1";
const STORE_STAMPS_KEY: &str = "opencode_stores_v1";
const TREE_STAMPS_KEY: &str = "opencode_tree_sessions_v1";

/// How recent a write has to be before its stamp cannot vouch for it.
const AMBIGUITY_MS: i64 = 2_000;

/// The value of a session this store holds that an earlier channel store
/// owns.
const CLAIMED: &str = "c";

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
    /// there: the destination marker does not name it short and it holds
    /// what [`holdings::held`] recorded.
    fn intact(&self, conn: &Connection, session_id: &str, held: &str) -> Result<bool> {
        if self.repairs.contains("opencode", session_id) {
            return Ok(false);
        }
        holdings::holds(conn, session_id, held)
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
        let held = holdings::held(conn, session_id)?;
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

/// A SQLite store's sessions are kept under a hash of its path, so 40,000
/// entries do not each repeat it.
fn store_tag(store: &str) -> String {
    format!("{:016x}", fingerprint_hash("opencode-store", store, ""))
}

/// The sweep's OpenCode pass over whichever layout this host has, with the
/// stamps `state` holds.
///
/// One owner, two layouts: `opencode.db` when the host has it, the legacy
/// `storage/` tree when it does not. Never both -- a host that upgraded has a
/// stale tree sitting beside a live database. Asked once, through the same
/// `detect` that discovery and hydration use: `exists()` is true for a
/// *directory* named by `OPENCODE_DB`, and asking that way opened it as
/// SQLite while detect read the legacy tree.
pub(crate) fn sync_opencode_sources(
    conn: &Connection,
    state: &mut Map<String, Value>,
    roots: &crate::ProviderRoots,
    repairs: &SweepRepairs,
) -> Result<usize> {
    let layout = opencode::OpencodeLayout::detect(
        &roots.opencode_db,
        roots.opencode_db_pinned,
        &roots.opencode_storage_dir,
    );
    let mut sweep = OpencodeSweep::begin(state, repairs);
    let inserted = match &layout {
        Some(opencode::OpencodeLayout::Sqlite(dbs)) => sync_opencode_dbs(conn, dbs, &mut sweep),
        Some(opencode::OpencodeLayout::JsonTree(tree)) => {
            sync_opencode_storage_dir(conn, tree, &mut sweep)
        }
        None => Ok(0),
    };
    sweep.finish(state);
    let inserted = inserted?;
    note(&match &layout {
        Some(opencode::OpencodeLayout::Sqlite(_)) => format!("  [opencode] +{inserted} rows"),
        Some(opencode::OpencodeLayout::JsonTree(tree)) => {
            format!("  [opencode] +{inserted} rows from {}", tree.display())
        }
        None => format!(
            "  [opencode] not found: {} (skipped)",
            roots.opencode_db.display()
        ),
    });
    Ok(inserted)
}

/// A sync progress line, unless sync output is quiet.
fn note(line: &str) {
    if !super::SYNC_QUIET.load(std::sync::atomic::Ordering::Relaxed) {
        println!("{line}");
    }
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
    let file_stamp = stamps::store_file_stamp(opencode_db, &sweep.generation);
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
fn sync_store_snapshot(
    conn: &Connection,
    src: &Connection,
    store: &str,
    tag: &str,
    claimed: &mut BTreeSet<String>,
    sweep: &mut OpencodeSweep<'_>,
) -> Result<(usize, bool)> {
    check_capture_cancelled()?;
    let (changed, settled) = changed_sessions(conn, src, tag, claimed, sweep)?;
    let mut read = StoreRead {
        conn,
        store,
        inserted: 0,
        definite: settled,
        failures: Vec::new(),
    };
    if !changed.is_empty() {
        read_changed(src, changed, &mut read, sweep)?;
    }
    if !read.failures.is_empty() {
        anyhow::bail!(
            "{} OpenCode session(s) in {store} could not be read (the rest were indexed): {}",
            read.failures.len(),
            read.failures.join("; ")
        );
    }
    Ok((read.inserted, read.definite))
}

/// One session to read: its id, its stamp entry, and the stamp to record
/// once it is written -- `None` when the stamp is not definite.
struct Changed {
    session_id: String,
    entry: String,
    token: Option<String>,
}

/// The sessions of this store whose stamps moved, and whether every one of
/// them has a definite stamp. `claimed` holds the sessions an earlier store
/// already owns; they are recorded as claimed here, and this store's own
/// sessions are added to it. Entries for sessions the store no longer holds
/// are dropped.
fn changed_sessions(
    conn: &Connection,
    src: &Connection,
    tag: &str,
    claimed: &mut BTreeSet<String>,
    sweep: &mut OpencodeSweep<'_>,
) -> Result<(Vec<Changed>, bool)> {
    let read_at = stamps::now_ms();
    let mut definite = true;
    let mut seen = BTreeSet::new();
    let mut changed = Vec::new();
    for stamp in stamps::sqlite_session_stamps(src)? {
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
        changed.push(Changed {
            session_id: stamp.session_id,
            entry,
            token: settled.then_some(token),
        });
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
    Ok((changed, definite))
}

/// Read the changed sessions. Session-keyed queries are bounded only when
/// the provider indexes the column they seek on; without that index each one
/// scans `part`, so the store is read whole once instead.
fn read_changed(
    src: &Connection,
    changed: Vec<Changed>,
    read: &mut StoreRead<'_>,
    sweep: &mut OpencodeSweep<'_>,
) -> Result<()> {
    if opencode::sync_plan(src)? == OpencodeSyncPlan::PerSession {
        for session in changed {
            check_capture_cancelled()?;
            let loaded = opencode::load_from_sqlite(src, &session.session_id);
            read.index(sweep, session, loaded)?;
        }
        return Ok(());
    }
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
    for session in changed {
        check_capture_cancelled()?;
        let loaded = match unreadable.get(&session.session_id) {
            Some(error) => Err(anyhow::anyhow!("{error}")),
            None => Ok(sessions.remove(&session.session_id)),
        };
        read.index(sweep, session, loaded)?;
    }
    Ok(())
}

/// One store's read in progress.
///
/// One session's failure is that session's failure: the loader reports a
/// row it cannot map rather than calling the session absent, and ending the
/// sweep at the first bad row would leave every session after it unindexed
/// for as long as that row stays bad.
struct StoreRead<'a> {
    conn: &'a Connection,
    store: &'a str,
    inserted: usize,
    definite: bool,
    failures: Vec<String>,
}

impl StoreRead<'_> {
    fn index(
        &mut self,
        sweep: &mut OpencodeSweep<'_>,
        session: Changed,
        loaded: Result<Option<OpencodeSession>>,
    ) -> Result<()> {
        #[cfg(test)]
        tests::note_read(0, 1);
        let written = loaded.and_then(|loaded| match loaded {
            Some(loaded) => opencode::normalize(self.conn, &loaded, self.store),
            None => Ok(Default::default()),
        });
        let Changed {
            session_id,
            entry,
            token,
        } = session;
        match (written, token) {
            (Ok(counts), Some(token)) => {
                self.inserted += counts.prompts;
                sweep.record(self.conn, Stamps::Sessions, entry, &session_id, &token)?;
            }
            (Ok(counts), None) => {
                self.inserted += counts.prompts;
                sweep.forget(Stamps::Sessions, entry);
            }
            (Err(error), _) => {
                check_capture_cancelled()?;
                self.failures.push(format!("{session_id}: {error:#}"));
                self.definite = false;
                sweep.forget(Stamps::Sessions, entry);
            }
        }
        Ok(())
    }
}
