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

/// PENDING DECISION (Khaliq, sessions pipeline W2): whether ids carry the
/// slug or its hash.
pub const DEFAULT_MACHINE_NAME_PRIVACY: MachineNamePrivacy = MachineNamePrivacy::Slug;

/// Hex digits of the install id the machine id carries. Six digits, 24 bits:
/// among 1,000 machines that share one hostname the chance that any two
/// collide is about 3%, against about 99.9% with four.
pub const INSTALL_SUFFIX_HEX_DIGITS: usize = 6;

/// The version of the [`canonical_hostname`] rule. `machine_name` and every
/// `hostname_aliases` row record the version they were computed with, so a
/// later rule change recanonicalizes them on the next open (the machine id
/// stays frozen) instead of leaving old and new names silently mixed.
pub const CANONICAL_HOSTNAME_RULE_VERSION: u32 = 1;

/// The canonical hostname of a name with no usable character.
pub const UNKNOWN_HOSTNAME: &str = "unknown-host";

/// Longest canonical hostname: one DNS label.
const MAX_HOSTNAME_LEN: usize = 63;

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
    /// Every hostname spelling this machine has reported, the current one
    /// included, oldest first.
    pub hostname_aliases: Vec<HostnameAlias>,
}

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
    updated_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS hostname_aliases (
    observed_hostname TEXT PRIMARY KEY,
    canonical_hostname TEXT NOT NULL,
    rule_version INTEGER NOT NULL,
    first_seen_ms INTEGER NOT NULL
);
"#;

/// The canonical hostname: one deterministic spelling per machine name.
///
/// Lowercased; trailing dots and `.local` stripped; only the first DNS label
/// kept (the domain dropped); every run of anything but `[a-z0-9]` --
/// punctuation, spaces, underscores, apostrophes, non-ASCII -- one `-`;
/// leading and trailing hyphens trimmed; at most 63 characters. A name with
/// nothing left is [`UNKNOWN_HOSTNAME`].
///
/// `Khaliqs-MacBook-Pro.local` -> `khaliqs-macbook-pro`;
/// `Khaliq's MacBook Pro` -> `khaliq-s-macbook-pro`;
/// `build_01.corp.example.com` -> `build-01`.
pub fn canonical_hostname(hostname: &str) -> String {
    let lower = hostname.trim().trim_end_matches('.').to_ascii_lowercase();
    let host = lower.strip_suffix(".local").unwrap_or(&lower);
    let label = host.split('.').next().unwrap_or_default();
    let mut slug = String::with_capacity(label.len());
    for ch in label.chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch);
        } else if !slug.is_empty() && !slug.ends_with('-') {
            slug.push('-');
        }
    }
    let slug = slug[..slug.len().min(MAX_HOSTNAME_LEN)].trim_end_matches('-');
    if slug.is_empty() {
        UNKNOWN_HOSTNAME.to_string()
    } else {
        slug.to_string()
    }
}

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
            "SELECT machine_id, machine_name, name_rule_version, install_id, id_scheme, \
             name_privacy, created_ms, updated_ms FROM machine_identity WHERE singleton = 1",
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
                    hostname_aliases: Vec::new(),
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
    Ok(Some(identity))
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
/// spelling is new under the current rule. Never changes a stored id.
pub(crate) fn ensure_with(
    conn: &Connection,
    hostname: &str,
    privacy: MachineNamePrivacy,
) -> Result<MachineIdentity> {
    let observed = hostname.trim();
    let name = canonical_hostname(observed);
    let rule = CANONICAL_HOSTNAME_RULE_VERSION;
    let now = now_ms();
    let Some(mut identity) = read(conn)? else {
        return create(conn, observed, name, privacy, now);
    };
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
    let recorded = identity
        .hostname_aliases
        .iter()
        .any(|alias| alias.observed == observed && alias.rule_version == rule);
    if !recorded {
        record_alias(conn, observed, &name, now)?;
        identity.hostname_aliases = hostname_aliases(conn)?;
    }
    Ok(identity)
}

fn create(
    conn: &Connection,
    observed: &str,
    name: String,
    privacy: MachineNamePrivacy,
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
    };
    conn.execute(
        "INSERT INTO machine_identity (singleton, machine_id, machine_name, name_rule_version, \
         install_id, id_scheme, name_privacy, created_ms, updated_ms) \
         VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)",
        params![
            identity.machine_id,
            identity.machine_name,
            identity.name_rule_version,
            identity.install_id,
            MACHINE_ID_SCHEME,
            privacy.as_str(),
            now
        ],
    )?;
    record_alias(conn, observed, &identity.machine_name, now)?;
    Ok(identity)
}

/// [`ensure_with`] for this host and the default name privacy.
pub(crate) fn ensure(conn: &Connection) -> Result<MachineIdentity> {
    ensure_with(conn, &local_hostname(), DEFAULT_MACHINE_NAME_PRIVACY)
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

/// The `sessions` catalog columns, then the trace ids and the machine
/// identity over them. Runs in the migration transaction: after `trace_id`
/// exists, and before the change feed reconciles its export schema, which
/// restamps the rows the backfill gave an id.
pub(crate) fn migrate(conn: &Connection, session_columns: &[&str]) -> Result<()> {
    crate::store::ensure_text_columns(conn, "sessions", session_columns)?;
    crate::store::ensure_text_columns(conn, "sessions", SESSION_COLUMNS)?;
    init_trace_ids(conn)?;
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
    /// writable handle (a write only when the name, its rule, or the spelling is new). A read-only
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

/// The tables a current schema has, by `sqlite_master` name.
pub(crate) const SCHEMA_OBJECTS: &[&str] = &["machine_identity", "hostname_aliases"];

/// What the change feed's insert stamp on `table` also sets, as a `, col =
/// expr` suffix of its `UPDATE ... SET`: a new session's trace id, unless the
/// insert gave one. Filling it in the stamp's own statement means a new
/// session takes one revision, not one for the insert and one for the fill.
/// Empty for every other table, and for `sessions` before the column exists.
pub(crate) fn insert_fill_sql(conn: &Connection, table: &str) -> Result<String> {
    if table != "sessions" {
        return Ok(String::new());
    }
    let has_column: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('sessions') WHERE name = 'trace_id')",
        [],
        |row| row.get(0),
    )?;
    Ok(if has_column {
        format!(", trace_id = COALESCE(trace_id, {UUID_V7_SQL})")
    } else {
        String::new()
    })
}

/// The standalone trigger an earlier revision of this change minted trace
/// ids with; the change feed's insert stamp does it now.
const RETIRED_TRACE_ID_TRIGGER: &str = "sessions_trace_id_insert";

/// The marker for the one-time trace id backfill of rows that predate it.
pub(crate) const TRACE_ID_MIGRATION: &str = "session_trace_id_v1";

/// Once, give every existing session row a trace id. Runs inside the
/// caller's migration transaction, after the `trace_id` column exists and
/// before the change feed reconciles its export schema, which then restamps
/// the backfilled rows once.
fn init_trace_ids(conn: &Connection) -> Result<()> {
    conn.execute_batch(&format!(
        "DROP TRIGGER IF EXISTS {RETIRED_TRACE_ID_TRIGGER};"
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
