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
//! - `machine_name` is the canonical hostname ([`canonical_hostname`]), kept
//!   current: a rename updates it. Every spelling of the hostname the machine
//!   has reported is kept in `hostname_aliases` with its canonical form, so
//!   all of them resolve to this one machine.
//! - `machine_id` is the canonical hostname plus a short suffix from a random
//!   install UUID generated at first run (`khaliqs-mbp-7f3a9c`), computed once
//!   by [`machine_id`] and then frozen. The hostname alone is never the id: a
//!   rename or respelling would change it, and two machines that share a
//!   hostname would share it. A rename updates `machine_name` and adds an
//!   alias; the id stays.
//! - `id_scheme` records which version of [`machine_id`] produced the id
//!   ([`MACHINE_ID_SCHEME`]), so a future change to the function applies only
//!   to new databases and never re-keys a machine that already uploaded.
//! - `hardware_id_hash` is a SHA-256 of the machine's hardware id. A
//!   database restored onto a different machine sees it change: the open
//!   retires the old id into `machine_identity_history` and mints a new
//!   install id, so two machines never upload under one id.
//!
//! # Trace ids
//!
//! Every `sessions` row carries a `trace_id`, a UUIDv7 minted when the row is
//! first written (by the change feed's insert trigger, so none of the
//! catalog's write sites can forget it) and kept by every later upsert. It is a stored column, so the
//! change feed and the export carry it in the session record's payload with
//! no further plumbing. Minting is pure SQL in the same statement as the
//! insert: no fsync, no custom function, nothing outside the transaction.
use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{now_ms, Error, SessionStore, Source};

mod hardware;
mod hostname;
mod trace;

#[cfg(test)]
pub(crate) use hardware::hardware_hash;
pub(crate) use hardware::local_hardware_hash;
pub(crate) use hostname::local_hostname;
pub use hostname::{canonical_hostname, CANONICAL_HOSTNAME_RULE_VERSION, UNKNOWN_HOSTNAME};
pub(crate) use trace::{insert_fill_sql, SESSION_COLUMNS, TRACE_ID_MIGRATION};

/// The version of [`machine_id`] a new identity is created with: the
/// canonical hostname, then `-`, then [`INSTALL_SUFFIX_HEX_DIGITS`] hex digits
/// of the install UUID.
pub const MACHINE_ID_SCHEME: &str = "hostname_install_suffix_v1";

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

/// How a new identity's id carries the machine name: the plain slug, so ids
/// and object-storage paths stay readable. [`MachineNamePrivacy::Hashed`]
/// remains available to an embedder that calls [`machine_id`] itself.
pub const DEFAULT_MACHINE_NAME_PRIVACY: MachineNamePrivacy = MachineNamePrivacy::Slug;

/// Hex digits of the install id the machine id carries. Six digits, 24 bits:
/// among 1,000 machines that share one hostname the chance that any two
/// collide is about 3%, against about 99.9% with four.
pub const INSTALL_SUFFIX_HEX_DIGITS: usize = 6;

/// This database's machine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MachineIdentity {
    /// Frozen at first run; see [`machine_id`].
    pub machine_id: String,
    /// The current canonical hostname.
    pub machine_name: String,
    /// The [`CANONICAL_HOSTNAME_RULE_VERSION`] `machine_name` was computed
    /// with.
    pub name_rule_version: u32,
    /// Random UUID generated at first run; its first hex digits are the id's
    /// suffix.
    pub install_id: String,
    /// The [`MACHINE_ID_SCHEME`] the id was created with.
    pub id_scheme: String,
    pub name_privacy: MachineNamePrivacy,
    pub created_ms: i64,
    pub updated_ms: i64,
    /// SHA-256 of the hardware id the identity was issued on, or `None`
    /// where the platform reports none; see [`MachineIdentity::previous`].
    pub hardware_id_hash: Option<String>,
    /// Every hostname spelling this machine has reported, the current one
    /// included, oldest first.
    pub hostname_aliases: Vec<HostnameAlias>,
    /// Ids this database held before, oldest first: each one retired because
    /// the database was opened on different hardware (restored or copied
    /// onto another machine).
    pub previous: Vec<RetiredMachineId>,
}

/// A machine id a database no longer uses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetiredMachineId {
    pub machine_id: String,
    pub install_id: String,
    pub machine_name: String,
    pub retired_ms: i64,
    /// Why it was retired: `hardware_changed`.
    pub reason: String,
}

/// Why [`ensure_with`] retires an id when the hardware hash moves.
const HARDWARE_CHANGED: &str = "hardware_changed";

/// One spelling of the hostname, as the OS reported it, and its canonical
/// form.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostnameAlias {
    pub observed: String,
    pub canonical: String,
    /// The [`CANONICAL_HOSTNAME_RULE_VERSION`] `canonical` was computed with.
    pub rule_version: u32,
    pub first_seen_ms: i64,
}

const MACHINE_IDENTITY_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS machine_identity (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    machine_id TEXT NOT NULL,
    machine_name TEXT NOT NULL,
    name_rule_version INTEGER NOT NULL,
    install_id TEXT NOT NULL,
    id_scheme TEXT NOT NULL,
    name_privacy TEXT NOT NULL,
    created_ms INTEGER NOT NULL,
    updated_ms INTEGER NOT NULL,
    hardware_id_hash TEXT
);
CREATE TABLE IF NOT EXISTS machine_identity_history (
    machine_id TEXT NOT NULL,
    install_id TEXT NOT NULL,
    machine_name TEXT NOT NULL,
    id_scheme TEXT NOT NULL,
    hardware_id_hash TEXT,
    created_ms INTEGER NOT NULL,
    retired_ms INTEGER NOT NULL,
    reason TEXT NOT NULL,
    hostname_aliases_json TEXT NOT NULL,
    PRIMARY KEY (machine_id, install_id)
);
CREATE TABLE IF NOT EXISTS hostname_aliases (
    observed_hostname TEXT PRIMARY KEY,
    canonical_hostname TEXT NOT NULL,
    rule_version INTEGER NOT NULL,
    first_seen_ms INTEGER NOT NULL
);
"#;

/// The machine id for `machine_name`: the single place the id is decided.
/// `install_id` is the random UUID from first run; its first
/// [`INSTALL_SUFFIX_HEX_DIGITS`] hex digits keep two machines that share a
/// hostname apart.
pub fn machine_id(privacy: MachineNamePrivacy, machine_name: &str, install_id: &str) -> String {
    let name = match privacy {
        MachineNamePrivacy::Slug => machine_name.to_string(),
        MachineNamePrivacy::Hashed => {
            let digest = Sha256::digest(format!("relayhistory-machine-name-v1:{machine_name}"));
            format!("h{}", &format!("{digest:x}")[..12])
        }
    };
    let suffix: String = install_id
        .chars()
        .filter(char::is_ascii_hexdigit)
        .map(|ch| ch.to_ascii_lowercase())
        .take(INSTALL_SUFFIX_HEX_DIGITS)
        .collect();
    format!("{name}-{suffix}")
}

/// The stored identity, if the database has one yet. A pure read.
pub(crate) fn read(conn: &Connection) -> Result<Option<MachineIdentity>> {
    let row = conn
        .query_row(
            "SELECT machine_id, machine_name, name_rule_version, install_id, id_scheme, \
             name_privacy, created_ms, updated_ms, hardware_id_hash \
             FROM machine_identity WHERE singleton = 1",
            [],
            |row| {
                let identity = MachineIdentity {
                    machine_id: row.get(0)?,
                    machine_name: row.get(1)?,
                    name_rule_version: row.get(2)?,
                    install_id: row.get(3)?,
                    id_scheme: row.get(4)?,
                    name_privacy: MachineNamePrivacy::Slug,
                    created_ms: row.get(6)?,
                    updated_ms: row.get(7)?,
                    hardware_id_hash: row.get(8)?,
                    hostname_aliases: Vec::new(),
                    previous: Vec::new(),
                };
                Ok((identity, row.get::<_, String>(5)?))
            },
        )
        .optional()?;
    let Some((mut identity, privacy)) = row else {
        return Ok(None);
    };
    identity.name_privacy = MachineNamePrivacy::parse(&privacy)
        .with_context(|| format!("unknown machine name privacy {privacy}"))?;
    identity.hostname_aliases = hostname_aliases(conn)?;
    identity.previous = retired_ids(conn)?;
    Ok(Some(identity))
}

fn retired_ids(conn: &Connection) -> Result<Vec<RetiredMachineId>> {
    let mut statement = conn.prepare(
        "SELECT machine_id, install_id, machine_name, retired_ms, reason \
         FROM machine_identity_history ORDER BY retired_ms, machine_id",
    )?;
    let retired = statement
        .query_map([], |row| {
            Ok(RetiredMachineId {
                machine_id: row.get(0)?,
                install_id: row.get(1)?,
                machine_name: row.get(2)?,
                retired_ms: row.get(3)?,
                reason: row.get(4)?,
            })
        })?
        .collect::<rusqlite::Result<_>>()?;
    Ok(retired)
}

fn hostname_aliases(conn: &Connection) -> Result<Vec<HostnameAlias>> {
    let mut statement = conn.prepare(
        "SELECT observed_hostname, canonical_hostname, rule_version, first_seen_ms \
         FROM hostname_aliases ORDER BY first_seen_ms, observed_hostname",
    )?;
    let aliases = statement
        .query_map([], |row| {
            Ok(HostnameAlias {
                observed: row.get(0)?,
                canonical: row.get(1)?,
                rule_version: row.get(2)?,
                first_seen_ms: row.get(3)?,
            })
        })?
        .collect::<rusqlite::Result<_>>()?;
    Ok(aliases)
}

/// The identity for a host named `hostname`: created on first run with
/// `privacy`, otherwise the stored one with `machine_name` brought up to
/// date -- for a rename, or for a stored name computed by an earlier
/// [`CANONICAL_HOSTNAME_RULE_VERSION`] -- and the spelling recorded. Writes
/// only when it creates the row, the name or its rule changed, or the
/// spelling is new under the current rule. A stored id changes only when
/// `hardware` (a [`hardware_hash`]) differs from the hash it was issued on:
/// the database moved to another machine, so the old id is retired and a new
/// install id minted. `None` for `hardware` detects nothing.
pub(crate) fn ensure_with(
    conn: &Connection,
    hostname: &str,
    privacy: MachineNamePrivacy,
    hardware: Option<&str>,
) -> Result<MachineIdentity> {
    let observed = hostname.trim();
    let name = canonical_hostname(observed);
    let rule = CANONICAL_HOSTNAME_RULE_VERSION;
    let now = now_ms();
    let Some(mut identity) = read(conn)? else {
        return create(conn, observed, name, privacy, hardware, now);
    };
    if let Some(hardware) = hardware {
        match identity.hardware_id_hash.as_deref() {
            Some(issued) if issued != hardware => {
                identity = reissue(conn, &identity, &name, hardware, now)?;
            }
            None => {
                conn.execute(
                    "UPDATE machine_identity SET hardware_id_hash = ?1 WHERE singleton = 1",
                    [hardware],
                )?;
                identity.hardware_id_hash = Some(hardware.to_string());
            }
            Some(_) => {}
        }
    }
    if identity.machine_name != name || identity.name_rule_version != rule {
        conn.execute(
            "UPDATE machine_identity SET machine_name = ?1, name_rule_version = ?2, \
             updated_ms = ?3 WHERE singleton = 1",
            params![name, rule, now],
        )?;
        identity.machine_name = name.clone();
        identity.name_rule_version = rule;
        identity.updated_ms = now;
    }
    let stale: Vec<&str> = identity
        .hostname_aliases
        .iter()
        .filter(|alias| alias.rule_version != rule)
        .map(|alias| alias.observed.as_str())
        .collect();
    let recorded = identity
        .hostname_aliases
        .iter()
        .any(|alias| alias.observed == observed && alias.rule_version == rule);
    if !stale.is_empty() || !recorded {
        // Every spelling computed under an earlier rule is recomputed, not
        // only the current one, so no alias is left under a mixed rule.
        for spelling in stale {
            record_alias(conn, spelling, &canonical_hostname(spelling), now)?;
        }
        record_alias(conn, observed, &name, now)?;
        identity.hostname_aliases = hostname_aliases(conn)?;
    }
    Ok(identity)
}

/// Retire `old` into `machine_identity_history` and issue this database a
/// new install id and machine id on `hardware`, under the same privacy. The
/// old machine's hostname spellings go with it; the caller records this
/// machine's.
fn reissue(
    conn: &Connection,
    old: &MachineIdentity,
    name: &str,
    hardware: &str,
    now: i64,
) -> Result<MachineIdentity> {
    conn.execute(
        "INSERT OR IGNORE INTO machine_identity_history (machine_id, install_id, machine_name, \
         id_scheme, hardware_id_hash, created_ms, retired_ms, reason, hostname_aliases_json) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            old.machine_id,
            old.install_id,
            old.machine_name,
            old.id_scheme,
            old.hardware_id_hash,
            old.created_ms,
            now,
            HARDWARE_CHANGED,
            serde_json::to_string(&old.hostname_aliases)?
        ],
    )?;
    conn.execute("DELETE FROM hostname_aliases", [])?;
    let install_id = random_uuid_v4(conn)?;
    conn.execute(
        "UPDATE machine_identity SET machine_id = ?1, install_id = ?2, id_scheme = ?3, \
         machine_name = ?4, name_rule_version = ?5, hardware_id_hash = ?6, \
         created_ms = ?7, updated_ms = ?7 WHERE singleton = 1",
        params![
            machine_id(old.name_privacy, name, &install_id),
            install_id,
            MACHINE_ID_SCHEME,
            name,
            CANONICAL_HOSTNAME_RULE_VERSION,
            hardware,
            now
        ],
    )?;
    read(conn)?.context("the machine identity row vanished while it was reissued")
}

fn create(
    conn: &Connection,
    observed: &str,
    name: String,
    privacy: MachineNamePrivacy,
    hardware: Option<&str>,
    now: i64,
) -> Result<MachineIdentity> {
    let install_id = random_uuid_v4(conn)?;
    let identity = MachineIdentity {
        machine_id: machine_id(privacy, &name, &install_id),
        hostname_aliases: vec![HostnameAlias {
            observed: observed.to_string(),
            canonical: name.clone(),
            rule_version: CANONICAL_HOSTNAME_RULE_VERSION,
            first_seen_ms: now,
        }],
        machine_name: name,
        name_rule_version: CANONICAL_HOSTNAME_RULE_VERSION,
        install_id,
        id_scheme: MACHINE_ID_SCHEME.to_string(),
        name_privacy: privacy,
        created_ms: now,
        updated_ms: now,
        hardware_id_hash: hardware.map(str::to_string),
        previous: Vec::new(),
    };
    conn.execute(
        "INSERT INTO machine_identity (singleton, machine_id, machine_name, name_rule_version, \
         install_id, id_scheme, name_privacy, created_ms, updated_ms, hardware_id_hash) \
         VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7, ?8)",
        params![
            identity.machine_id,
            identity.machine_name,
            identity.name_rule_version,
            identity.install_id,
            MACHINE_ID_SCHEME,
            privacy.as_str(),
            now,
            hardware
        ],
    )?;
    record_alias(conn, observed, &identity.machine_name, now)?;
    Ok(identity)
}

/// [`ensure_with`] for this host, its hardware and the default name privacy.
pub(crate) fn ensure(conn: &Connection) -> Result<MachineIdentity> {
    ensure_with(
        conn,
        &local_hostname(),
        DEFAULT_MACHINE_NAME_PRIVACY,
        local_hardware_hash().as_deref(),
    )
}

/// Whether a writable open has identity work to do on an otherwise current
/// schema: a name or alias computed under an earlier
/// [`CANONICAL_HOSTNAME_RULE_VERSION`], a hardware hash not yet recorded, or
/// one that changed because the database moved to another machine. Two
/// indexed reads and a hash cached for the process, so a current database
/// stays cheap to open. Only called once the identity tables are current.
pub(crate) fn needs_refresh(conn: &Connection) -> Result<bool> {
    let Some((rule, issued)) = conn
        .query_row(
            "SELECT name_rule_version, hardware_id_hash FROM machine_identity \
             WHERE singleton = 1",
            [],
            |row| Ok((row.get::<_, u32>(0)?, row.get::<_, Option<String>>(1)?)),
        )
        .optional()?
    else {
        return Ok(true);
    };
    let stale_alias: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM hostname_aliases WHERE rule_version <> ?1)",
        [CANONICAL_HOSTNAME_RULE_VERSION],
        |row| row.get(0),
    )?;
    let moved = local_hardware_hash().is_some_and(|now| issued.as_deref() != Some(now.as_str()));
    Ok(rule != CANONICAL_HOSTNAME_RULE_VERSION || stale_alias || moved)
}

/// Record `observed` under the current rule. A spelling seen under an
/// earlier rule is recanonicalized in place, keeping its first sighting.
fn record_alias(conn: &Connection, observed: &str, canonical: &str, now: i64) -> Result<()> {
    conn.execute(
        "INSERT INTO hostname_aliases \
         (observed_hostname, canonical_hostname, rule_version, first_seen_ms) \
         VALUES (?1, ?2, ?3, ?4) \
         ON CONFLICT(observed_hostname) DO UPDATE SET \
         canonical_hostname = excluded.canonical_hostname, \
         rule_version = excluded.rule_version",
        params![observed, canonical, CANONICAL_HOSTNAME_RULE_VERSION, now],
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

/// The `sessions` catalog columns, then the trace ids and the machine
/// identity over them. Runs in the migration transaction: after `trace_id`
/// exists, and before the change feed reconciles its export schema, which
/// restamps the rows the backfill gave an id.
pub(crate) fn migrate(conn: &Connection, session_columns: &[&str]) -> Result<()> {
    crate::store::ensure_text_columns(conn, "sessions", session_columns)?;
    crate::store::ensure_text_columns(conn, "sessions", SESSION_COLUMNS)?;
    trace::init_trace_ids(conn)?;
    conn.execute_batch(MACHINE_IDENTITY_DDL)?;
    // A table an earlier shape of this schema created keeps that shape
    // through `IF NOT EXISTS`; its missing columns are added here. Rule
    // version 0 is "an earlier rule", which `ensure` then recanonicalizes.
    for (table, columns) in IDENTITY_COLUMNS {
        crate::store::ensure_columns(conn, table, columns)?;
    }
    ensure(conn).map(drop)
}

/// Identity columns added after their table first shipped, with the
/// declaration that backfills a row that predates them.
pub(crate) const IDENTITY_COLUMNS: &[(&str, &[(&str, &str)])] = &[
    (
        "machine_identity",
        &[
            ("name_rule_version", "INTEGER NOT NULL DEFAULT 0"),
            ("id_scheme", "TEXT NOT NULL DEFAULT 'unknown'"),
            ("hardware_id_hash", "TEXT"),
        ],
    ),
    (
        "hostname_aliases",
        &[("rule_version", "INTEGER NOT NULL DEFAULT 0")],
    ),
];

impl SessionStore {
    /// This database's machine: created by the first writable open, its
    /// `machine_name` refreshed from the current hostname by this call on a
    /// writable handle (a write only when the name, its rule, the spelling or
    /// the hardware is new). A read-only handle returns what is stored, or
    /// `None` for a database no writable open has migrated yet -- and fails
    /// instead when the stored id was issued on other hardware, since that id
    /// belongs to another machine until a writable open reissues it. Local
    /// only; never a network call.
    pub fn machine_identity(&self) -> std::result::Result<Option<MachineIdentity>, Error> {
        if self.read_only() {
            let conn = self.read_conn()?;
            let identity = read(&conn).map_err(Error::query)?;
            let moved = identity.as_ref().is_some_and(|identity| {
                local_hardware_hash().is_some_and(|now| {
                    identity
                        .hardware_id_hash
                        .as_deref()
                        .is_some_and(|issued| issued != now)
                })
            });
            if moved {
                return Err(Error::query(anyhow::anyhow!(
                    "this database's machine id was issued on other hardware (it was restored \
                     or copied from another machine); open it writable once to issue this \
                     machine its own id"
                )));
            }
            return Ok(identity);
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

/// The tables a current schema has, by `sqlite_master` name.
pub(crate) const SCHEMA_OBJECTS: &[&str] = &[
    "machine_identity",
    "hostname_aliases",
    "machine_identity_history",
];

#[cfg(test)]
mod tests;
