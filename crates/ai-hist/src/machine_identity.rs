//! Which machine this history database belongs to, and the per-session trace
//! id that follows a session from local ingest to every cloud destination.
//!
//! # Machine identity
//!
//! One row in `machine_identity`, written by the first writable open of a
//! database and never rewritten except for `machine_name`. Everything is
//! computed locally: the hostname comes from `uname(2)` (or `COMPUTERNAME` on
//! Windows), the install id from SQLite's `randomblob`. No network, no extra
//! process, and a current database pays one `SELECT` to read it.
//!
//! - `machine_name` is the canonical hostname slug ([`canonical_machine_name`]),
//!   kept current: a rename updates it and records the new name in
//!   `machine_aliases`, so the history of names is never lost.
//! - `machine_id` is computed once, by [`machine_id`] -- the one function that
//!   decides the id, so the strategy can be swapped without touching callers --
//!   and then frozen. A rename never changes it.
//! - `strategy` records which [`MachineIdStrategy`] produced the id, so a
//!   later change of [`DEFAULT_MACHINE_ID_STRATEGY`] applies only to new
//!   databases and never re-keys a machine that already uploaded.
//!
//! # Trace ids
//!
//! Every `sessions` row carries a `trace_id`, a UUIDv7 minted when the row is
//! first written (by a trigger, so none of the catalog's write sites can
//! forget it) and kept by every later upsert. It is a stored column, so the
//! change feed and the export carry it in the session record's payload with
//! no further plumbing. Minting is pure SQL in the same statement as the
//! insert: no fsync, no custom function, nothing outside the transaction.
use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{now_ms, Error, SessionStore, Source};

/// How [`machine_id`] derives the id from the machine name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MachineIdStrategy {
    /// Strategy A: the canonical machine name *is* the id (`khaliq-mbp`).
    /// Readable and the same after a re-image, but two machines with the
    /// same hostname (every unrenamed `macbook-pro`) share one id.
    HostnameSlug,
    /// Strategy B: the canonical name plus a short suffix from the random
    /// install id (`khaliq-mbp-7f3a9c`). Distinct for same-named machines;
    /// a re-image is a new id.
    HostnameSlugWithInstallSuffix,
}

impl MachineIdStrategy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::HostnameSlug => "hostname_slug",
            Self::HostnameSlugWithInstallSuffix => "hostname_slug_with_install_suffix",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        [Self::HostnameSlug, Self::HostnameSlugWithInstallSuffix]
            .into_iter()
            .find(|strategy| strategy.as_str() == value)
    }
}

/// How the machine name appears inside the id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MachineNamePrivacy {
    /// The slug itself.
    Slug,
    /// `h` plus 12 hex digits of a SHA-256 of the slug. It keeps the
    /// hostname out of object paths, but it is not anonymity: hostnames are
    /// guessable, so a dictionary reverses it.
    Hashed,
}

impl MachineNamePrivacy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Slug => "slug",
            Self::Hashed => "hashed",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        [Self::Slug, Self::Hashed]
            .into_iter()
            .find(|privacy| privacy.as_str() == value)
    }
}

/// PENDING DECISION (Khaliq, sessions pipeline W2): strategy A
/// ([`MachineIdStrategy::HostnameSlug`]) or B
/// ([`MachineIdStrategy::HostnameSlugWithInstallSuffix`]). B is the interim
/// default because a same-named collision silently merges two machines'
/// history and cannot be undone after upload. Changing this constant only
/// affects databases created afterwards.
pub const DEFAULT_MACHINE_ID_STRATEGY: MachineIdStrategy =
    MachineIdStrategy::HostnameSlugWithInstallSuffix;

/// PENDING DECISION (Khaliq, sessions pipeline W2): whether ids carry the
/// slug or its hash.
pub const DEFAULT_MACHINE_NAME_PRIVACY: MachineNamePrivacy = MachineNamePrivacy::Slug;

/// Hex digits of the install id that strategy B appends. Six digits, 24 bits:
/// among 1,000 machines that share one hostname the chance that any two
/// collide is about 3%, against about 99.9% with four.
pub const INSTALL_SUFFIX_HEX_DIGITS: usize = 6;

/// The name used when the hostname has no usable character.
pub const UNKNOWN_MACHINE_NAME: &str = "unknown-host";

/// Longest canonical name: one DNS label.
const MAX_MACHINE_NAME_LEN: usize = 63;

/// This database's machine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MachineIdentity {
    /// Frozen at first run; see [`machine_id`].
    pub machine_id: String,
    /// The current canonical hostname slug.
    pub machine_name: String,
    /// Random UUID generated at first run. Local; never part of a strategy-A id.
    pub install_id: String,
    pub strategy: MachineIdStrategy,
    pub name_privacy: MachineNamePrivacy,
    pub created_ms: i64,
    pub updated_ms: i64,
    /// Every name this machine has had, the current one included, oldest
    /// first.
    pub aliases: Vec<String>,
}

const MACHINE_IDENTITY_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS machine_identity (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    machine_id TEXT NOT NULL,
    machine_name TEXT NOT NULL,
    install_id TEXT NOT NULL,
    strategy TEXT NOT NULL,
    name_privacy TEXT NOT NULL,
    created_ms INTEGER NOT NULL,
    updated_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS machine_aliases (
    machine_name TEXT PRIMARY KEY,
    first_seen_ms INTEGER NOT NULL,
    last_seen_ms INTEGER NOT NULL
);
"#;

/// The canonical machine name: lowercase `[a-z0-9-]`, `.local` stripped,
/// first DNS label only, apostrophes dropped, every other run of characters
/// one `-`, at most 63 characters.
///
/// `Khaliqs-MacBook-Pro.local` -> `khaliqs-macbook-pro`;
/// `Khaliq's MacBook Pro` -> `khaliqs-macbook-pro`;
/// `build-01.corp.example.com` -> `build-01`.
pub fn canonical_machine_name(hostname: &str) -> String {
    let trimmed = hostname.trim().trim_end_matches('.');
    let lower = trimmed.to_ascii_lowercase();
    let host = lower.strip_suffix(".local").unwrap_or(&lower);
    let label = host.split('.').next().unwrap_or_default();
    let mut slug = String::with_capacity(label.len());
    for ch in label.chars() {
        if ch == '\'' || ch == '\u{2019}' {
            continue;
        }
        if ch.is_ascii_alphanumeric() {
            slug.push(ch);
        } else if !slug.ends_with('-') {
            slug.push('-');
        }
    }
    let slug = slug.trim_matches('-');
    let slug = slug[..slug.len().min(MAX_MACHINE_NAME_LEN)].trim_end_matches('-');
    if slug.is_empty() {
        UNKNOWN_MACHINE_NAME.to_string()
    } else {
        slug.to_string()
    }
}

/// The machine id for `machine_name` under `strategy`: the single place the
/// id is decided. `install_id` is the random UUID from first run.
pub fn machine_id(
    strategy: MachineIdStrategy,
    privacy: MachineNamePrivacy,
    machine_name: &str,
    install_id: &str,
) -> String {
    let name = match privacy {
        MachineNamePrivacy::Slug => machine_name.to_string(),
        MachineNamePrivacy::Hashed => {
            let digest = Sha256::digest(format!("relayhistory-machine-name-v1:{machine_name}"));
            format!("h{}", &format!("{digest:x}")[..12])
        }
    };
    match strategy {
        MachineIdStrategy::HostnameSlug => name,
        MachineIdStrategy::HostnameSlugWithInstallSuffix => {
            let hex: String = install_id
                .chars()
                .filter(char::is_ascii_hexdigit)
                .map(|ch| ch.to_ascii_lowercase())
                .take(INSTALL_SUFFIX_HEX_DIGITS)
                .collect();
            format!("{name}-{hex}")
        }
    }
}

/// This host's name as the OS reports it, without spawning a process.
pub(crate) fn local_hostname() -> String {
    #[cfg(unix)]
    {
        rustix::system::uname()
            .nodename()
            .to_string_lossy()
            .into_owned()
    }
    #[cfg(not(unix))]
    {
        std::env::var("COMPUTERNAME").unwrap_or_default()
    }
}

/// The stored identity, if the database has one yet. A pure read.
pub(crate) fn read(conn: &Connection) -> Result<Option<MachineIdentity>> {
    let row = conn
        .query_row(
            "SELECT machine_id, machine_name, install_id, strategy, name_privacy, \
             created_ms, updated_ms FROM machine_identity WHERE singleton = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                ))
            },
        )
        .optional()?;
    let Some((machine_id, machine_name, install_id, strategy, privacy, created_ms, updated_ms)) =
        row
    else {
        return Ok(None);
    };
    Ok(Some(MachineIdentity {
        machine_id,
        machine_name,
        install_id,
        strategy: MachineIdStrategy::parse(&strategy)
            .with_context(|| format!("unknown machine id strategy {strategy}"))?,
        name_privacy: MachineNamePrivacy::parse(&privacy)
            .with_context(|| format!("unknown machine name privacy {privacy}"))?,
        created_ms,
        updated_ms,
        aliases: aliases(conn)?,
    }))
}

fn aliases(conn: &Connection) -> Result<Vec<String>> {
    let mut statement = conn
        .prepare("SELECT machine_name FROM machine_aliases ORDER BY first_seen_ms, machine_name")?;
    let names = statement
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<Vec<String>>>()?;
    Ok(names)
}

/// The identity for a host named `hostname`: created on first run with
/// `strategy` and `privacy`, otherwise the stored one with `machine_name`
/// brought up to date. Writes only when it creates the row or the name
/// changed. Neither argument changes a stored id.
pub(crate) fn ensure_with(
    conn: &Connection,
    hostname: &str,
    strategy: MachineIdStrategy,
    privacy: MachineNamePrivacy,
) -> Result<MachineIdentity> {
    let name = canonical_machine_name(hostname);
    let now = now_ms();
    if let Some(mut identity) = read(conn)? {
        if identity.machine_name != name {
            conn.execute(
                "UPDATE machine_identity SET machine_name = ?1, updated_ms = ?2 \
                 WHERE singleton = 1",
                params![name, now],
            )?;
            record_alias(conn, &name, now)?;
            identity.machine_name = name;
            identity.updated_ms = now;
            identity.aliases = aliases(conn)?;
        }
        return Ok(identity);
    }
    let install_id = random_uuid_v4(conn)?;
    let identity = MachineIdentity {
        machine_id: machine_id(strategy, privacy, &name, &install_id),
        machine_name: name.clone(),
        install_id,
        strategy,
        name_privacy: privacy,
        created_ms: now,
        updated_ms: now,
        aliases: vec![name.clone()],
    };
    conn.execute(
        "INSERT INTO machine_identity (singleton, machine_id, machine_name, install_id, \
         strategy, name_privacy, created_ms, updated_ms) VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?6)",
        params![
            identity.machine_id,
            identity.machine_name,
            identity.install_id,
            strategy.as_str(),
            privacy.as_str(),
            now
        ],
    )?;
    record_alias(conn, &identity.machine_name, now)?;
    Ok(identity)
}

/// [`ensure_with`] for this host and the default strategy.
pub(crate) fn ensure(conn: &Connection) -> Result<MachineIdentity> {
    ensure_with(
        conn,
        &local_hostname(),
        DEFAULT_MACHINE_ID_STRATEGY,
        DEFAULT_MACHINE_NAME_PRIVACY,
    )
}

fn record_alias(conn: &Connection, name: &str, now: i64) -> Result<()> {
    conn.execute(
        "INSERT INTO machine_aliases (machine_name, first_seen_ms, last_seen_ms) \
         VALUES (?1, ?2, ?2) \
         ON CONFLICT(machine_name) DO UPDATE SET last_seen_ms = excluded.last_seen_ms",
        params![name, now],
    )?;
    Ok(())
}

fn random_uuid_v4(conn: &Connection) -> Result<String> {
    Ok(conn.query_row(
        "SELECT substr(r, 1, 8) || '-' || substr(r, 9, 4) || '-4' || substr(r, 14, 3) || '-' \
         || substr('89ab', (random() & 3) + 1, 1) || substr(r, 18, 3) || '-' || substr(r, 21, 12) \
         FROM (SELECT lower(hex(randomblob(16))) AS r)",
        [],
        |row| row.get(0),
    )?)
}

/// A fresh UUIDv7 as one SQL expression: 48 bits of Unix milliseconds, the
/// version and variant nibbles, 74 random bits.
///
/// Inline rather than a subquery, so an `UPDATE` evaluates it per row: SQLite
/// runs an uncorrelated subquery once per statement. `'now'` is fixed for one
/// statement, so both timestamp halves agree; each random group draws its own
/// `randomblob`. Only core functions -- `julianday` rather than
/// `unixepoch('subsec')`, which older SQLite builds lack -- so any client that
/// writes a session row can run the trigger. `random() & 3` rather than
/// `abs(random()) % 4`: `abs` overflows on the smallest integer.
pub(crate) const UUID_V7_SQL: &str = "lower(\
     substr(printf('%012x', CAST((julianday('now') - 2440587.5) * 86400000.0 AS INTEGER)), 1, 8) \
     || '-' || substr(printf('%012x', CAST((julianday('now') - 2440587.5) * 86400000.0 AS INTEGER)), 9, 4) \
     || '-7' || substr(hex(randomblob(2)), 2, 3) \
     || '-' || substr('89ab', (random() & 3) + 1, 1) || substr(hex(randomblob(2)), 2, 3) \
     || '-' || hex(randomblob(6)))";

/// Columns an insert trigger fills from NULL in the inserting statement. The
/// change feed's update guard does not count that fill as a change, so a new
/// session takes one revision, not two.
pub(crate) const MINTED_ON_INSERT: &[(&str, &str)] = &[("sessions", "trace_id")];

/// The `sessions` catalog columns, then the trace ids and the machine
/// identity over them. Runs in the migration transaction: after `trace_id`
/// exists, and before the change feed reconciles its export schema, which
/// restamps the rows the backfill gave an id.
pub(crate) fn migrate(conn: &Connection, session_columns: &[&str]) -> Result<()> {
    crate::store::ensure_text_columns(conn, "sessions", session_columns)?;
    crate::store::ensure_text_columns(conn, "sessions", SESSION_COLUMNS)?;
    init_trace_ids(conn)?;
    conn.execute_batch(MACHINE_IDENTITY_DDL)?;
    ensure(conn).map(drop)
}

impl SessionStore {
    /// This database's machine: created by the first writable open, its
    /// `machine_name` refreshed from the current hostname by this call on a
    /// writable handle (a write only when the name changed). A read-only
    /// handle returns what is stored, or `None` for a database no writable
    /// open has migrated yet. Local only; never a network call.
    pub fn machine_identity(&self) -> std::result::Result<Option<MachineIdentity>, Error> {
        if self.read_only() {
            let conn = self.read_conn()?;
            return read(&conn).map_err(Error::query);
        }
        let conn = crate::store::open_db(self.db_path())
            .map_err(|error| Error::DatabaseOpen(format!("{error:#}")))?;
        ensure(&conn).map(Some).map_err(Error::query)
    }

    /// The session's trace id: a UUIDv7 minted when its catalog row was
    /// first written, never rewritten. `None` when the session is not
    /// catalogued.
    pub fn trace_id(
        &self,
        source: Source,
        session_id: &str,
    ) -> std::result::Result<Option<String>, Error> {
        let conn = self.read_conn()?;
        conn.query_row(
            "SELECT trace_id FROM sessions WHERE source = ?1 AND session_id = ?2",
            [source.as_str(), session_id],
            |row| row.get(0),
        )
        .optional()
        .map(Option::flatten)
        .map_err(Error::sql)
    }
}

/// The `sessions` columns this module adds.
pub(crate) const SESSION_COLUMNS: &[&str] = &["trace_id"];

/// The tables and trigger a current schema has, by `sqlite_master` name.
pub(crate) const SCHEMA_OBJECTS: &[&str] =
    &["machine_identity", "machine_aliases", TRACE_ID_TRIGGER];

/// The trigger that gives a new session row its trace id.
pub(crate) const TRACE_ID_TRIGGER: &str = "sessions_trace_id_insert";

/// The marker for the one-time trace id backfill of rows that predate it.
pub(crate) const TRACE_ID_MIGRATION: &str = "session_trace_id_v1";

/// Install the trace id trigger and, once, give every existing session row a
/// trace id. Runs inside the caller's migration transaction, after the
/// `trace_id` column exists and before the change feed reconciles its
/// export schema, which then restamps the backfilled rows once.
pub(crate) fn init_trace_ids(conn: &Connection) -> Result<()> {
    conn.execute_batch(&format!(
        "CREATE TRIGGER IF NOT EXISTS {TRACE_ID_TRIGGER} AFTER INSERT ON sessions \
         WHEN NEW.trace_id IS NULL BEGIN \
             UPDATE sessions SET trace_id = {UUID_V7_SQL} WHERE rowid = NEW.rowid; \
         END;"
    ))?;
    if !crate::store::migration_applied(conn, TRACE_ID_MIGRATION)? {
        conn.execute_batch(&format!(
            "UPDATE sessions SET trace_id = {UUID_V7_SQL} WHERE trace_id IS NULL; \
             INSERT OR IGNORE INTO schema_migrations (name) VALUES ('{TRACE_ID_MIGRATION}');"
        ))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
