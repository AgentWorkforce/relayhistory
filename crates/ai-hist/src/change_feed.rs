//! Revision-stamped change feed over the evidence tables.
//!
//! A downstream consumer that materialises its own view of the ledger — burn's
//! watch loop, say — needs "what changed since my last tick" without
//! rescanning every session. This module answers that with one monotonic
//! revision per row change, drawn from the database-wide `observation_clock`,
//! and a pull cursor over it:
//!
//! - Every row of `sessions`, `session_events`, `tool_calls`, `file_edits`,
//!   `session_markers`, `session_relationships`, `history`,
//!   `session_presences`, `session_commit_links`, `trajectories`,
//!   `session_observations` and `observation_evidence` carries a `revision`.
//!   A trigger stamps the current clock on every insert, and on every update
//!   that changes at least one of the row's columns ([`stamped_columns`]), so
//!   a re-parse that upserts a row it already holds, unchanged, leaves it at
//!   the revision it had and the feed reports nothing for it. A re-seen key
//!   still means a replace, never a duplicate: consumers must treat it so.
//! - An upsert carries the row twice over: typed, where the kind has a typed
//!   row, and as stored ([`StoredRow`]) -- every column but `revision`, read
//!   from the live table, values as SQLite holds them -- so an embedder can
//!   rebuild the row exactly without knowing the table's shape in advance.
//!   Every change, a delete included, carries the record's identity: its
//!   kind and the columns of its table's uniqueness constraint
//!   ([`Change::key`]).
//! - A deleted row — a sidechain heal moving records onto the child, a session
//!   dropped from the catalog — leaves a tombstone in `evidence_tombstones`
//!   carrying its own revision, so a consumer learns about the removal in the
//!   same ordered stream. A later insert of the same key clears the tombstone.
//! - [`SessionStore::changes_since`] drains the rows and tombstones in
//!   `(revision, kind, record_key)` order, bounded to the head revision at
//!   open, in pages of at most [`MAX_CHANGE_BATCH`] rows, each page an indexed
//!   range read.
//! - A named consumer keeps its progress in `consumer_cursors`, inside the
//!   store, so a consumer that resets its own ledger still resumes from where
//!   it left off. The cursor moves only on [`Changes::commit`].
//!
//! Stamping happens in triggers rather than in each writer because the ledger
//! has well over a hundred write sites across parsers, hydration, hooks and
//! remote intake, and a write site that forgot the stamp would be a row the
//! feed never reports — the silent-omission failure this repository keeps
//! finding. A trigger cannot be forgotten.
//!
//! What the feed never shows is a message the provider has not finished
//! writing. Incremental hydration holds those records back and writes nothing
//! for them until the message completes, so a consumer sees each message's
//! blocks exactly once, with the usage they ended with.

use crate::discover::{row_to_session, ShallowSession, SESSION_COLUMNS};
use crate::read_pool::PooledConnection;
use crate::relationship_graph::{map_relationship, SessionRelationship, RELATIONSHIP_COLUMNS};
use crate::session_identities::SessionIdentity;
use crate::session_store::{Error, SessionStore, Source};
use crate::store::{
    ensure_columns, migration_applied, open_db, row_to_file_edit, row_to_session_event,
    row_to_session_marker, row_to_tool_call, HistoryEntry, SessionEvent, SessionFileEdit,
    SessionMarker, SessionToolCall, FILE_EDIT_COLUMNS, SESSION_EVENT_COLUMNS,
    SESSION_MARKER_COLUMNS, TOOL_CALL_COLUMNS,
};
use crate::EvidenceKind;
use anyhow::{Context, Result};
use rusqlite::types::ValueRef;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;

/// The column every fed table carries. Named once so a stored row can leave
/// it out: it is this database's bookkeeping, not a fact about the record.
pub(crate) const REVISION_COLUMN: &str = "revision";

/// Largest page one [`Changes`] fill reads per kind.
pub const MAX_CHANGE_BATCH: usize = 10_000;

/// Page size when [`ChangeQuery::batch`] is zero.
pub const DEFAULT_CHANGE_BATCH: usize = 1_000;

/// The feed schema's marker. `v2` is every kind the feed reports; a database
/// stamped by `v1` gains the kinds it lacked, backfilled above its head, in
/// the one pass the missing marker triggers.
const MIGRATION: &str = "change_feed_v2";

/// The column a named cursor records its kind set in.
const KINDS_COLUMN: &str = "kinds";

/// JSON map of each fed kind to the fingerprint of the exact stored column
/// names and declared types its upserts export. A schema migration can change
/// a record's semantic JSON without writing the row and therefore without
/// advancing its revision. Persisting the per-kind map lets the migration pass
/// restamp only rows whose exported representation changed.
const EXPORT_SCHEMA_DIGEST_COLUMN: &str = "export_schema_digest";

/// The kind set a drain reports, normalized so two drains asking for the
/// same kinds in any order or with repeats spell it the same way.
///
/// A named cursor is bound to one of these. A cursor is a position in a
/// stream, and a stream is defined by its kinds: a drain over events alone
/// that reaches the head and commits has accounted for no relationship,
/// marker or catalog row on the way, so letting an all-kinds drain resume
/// from that position would skip every one of them for good. A consumer
/// that wants two filters keeps two names.
#[derive(Debug, Clone, PartialEq, Eq)]
struct KindSet {
    kinds: Vec<ChangeKind>,
}

impl KindSet {
    fn normalize(kinds: Option<Vec<ChangeKind>>) -> Self {
        let mut kinds = kinds.unwrap_or_else(|| ChangeKind::ALL.to_vec());
        kinds.sort();
        kinds.dedup();
        Self { kinds }
    }

    /// How the set is stored: `*` for every kind, else the wire names in
    /// enum order, comma-separated.
    fn stored(&self) -> String {
        if self.kinds == ChangeKind::ALL {
            "*".to_string()
        } else {
            self.kinds
                .iter()
                .map(|kind| kind.as_str())
                .collect::<Vec<_>>()
                .join(",")
        }
    }
}

fn kinds_mismatch(name: &str, stored: &str, offered: &str) -> Error {
    Error::ConsumerKindsMismatch(format!(
        "changes_since: consumer {name:?} is bound to kinds [{stored}] but this drain reports \
         [{offered}]; a cursor is a position in one kind set's stream, use another consumer \
         name for another filter"
    ))
}

/// A position in the feed: the revision of the last change accounted for,
/// in the store that issued it.
///
/// Revisions are unique per row write, so within one store a watermark is a
/// complete position and `revision > watermark` is the whole resume
/// predicate. Across stores it is not: every database counts from zero, so a
/// replacement database reuses the revisions of the one it replaced. `epoch`
/// names the database -- a random identity drawn when its feed schema was
/// created. An export-schema migration restamps only affected rows above the
/// current head, so the database identity stays stable and both named and
/// external cursors resume without replaying unrelated kinds.
/// [`SessionStore::changes_since`] refuses a watermark issued by another
/// database, however far that store has since counted. A consumer persists
/// both fields, as the store returned them.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct Watermark {
    pub revision: u64,
    /// The issuing store's identity; zero only in [`Watermark::START`] and
    /// [`Watermark::CONSUMER`], which name no store.
    pub epoch: u64,
}

impl Watermark {
    /// Before the first stamped row: a full replay, valid in any store.
    pub const START: Watermark = Watermark {
        revision: 0,
        epoch: 0,
    };

    /// With [`ChangeQuery::consumer`] set: resume from that consumer's last
    /// committed position, or from [`Watermark::START`] when it has none.
    pub const CONSUMER: Watermark = Watermark {
        revision: u64::MAX,
        epoch: 0,
    };
}

/// Which table a change is about.
///
/// This is not [`EvidenceKind`]: that enum names the record kinds a source
/// adapter can supply, and the feed also reports tables no adapter writes --
/// the catalog row, where a session was seen, what a connector observed.
/// [`ChangeKind::evidence_kind`] maps the overlap. The wire names are the
/// kinds a local export's records carry, so a record keeps one kind name
/// whichever of the two an embedder read it from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ChangeKind {
    /// `sessions`: the catalog row.
    Session,
    /// `session_events`.
    SessionEvent,
    /// `tool_calls`.
    ToolCall,
    /// `file_edits`.
    FileEdit,
    /// `session_markers`.
    SessionMarker,
    /// `session_relationships`.
    Relationship,
    /// `history`: one prompt from a provider's prompt log.
    History,
    /// `session_presences`: where a session was seen, local or remote.
    Presence,
    /// `session_commit_links`: a commit a session is linked to.
    CommitLink,
    /// `trajectories`.
    Trajectory,
    /// `session_observations`: one connector's observation of a session.
    SourceObservation,
    /// `observation_evidence`: a record a connector supplied with an
    /// observation.
    ObservationEvidence,
}

impl ChangeKind {
    /// Every kind the feed reports, in the order ties are broken.
    pub const ALL: &'static [ChangeKind] = &[
        ChangeKind::Session,
        ChangeKind::SessionEvent,
        ChangeKind::ToolCall,
        ChangeKind::FileEdit,
        ChangeKind::SessionMarker,
        ChangeKind::Relationship,
        ChangeKind::History,
        ChangeKind::Presence,
        ChangeKind::CommitLink,
        ChangeKind::Trajectory,
        ChangeKind::SourceObservation,
        ChangeKind::ObservationEvidence,
    ];

    /// The wire name, identical to the serde representation, to the `kind`
    /// stored on a tombstone and to the first element of [`Change::key`].
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Session => "session",
            Self::SessionEvent => "session_event",
            Self::ToolCall => "tool_call",
            Self::FileEdit => "file_edit",
            Self::SessionMarker => "session_marker",
            Self::Relationship => "relationship",
            Self::History => "history",
            Self::Presence => "presence",
            Self::CommitLink => "commit_link",
            Self::Trajectory => "trajectory",
            Self::SourceObservation => "source_observation",
            Self::ObservationEvidence => "observation_evidence",
        }
    }

    /// The source-evidence kind this change carries, or `None` for a table
    /// no adapter supplies.
    pub fn evidence_kind(self) -> Option<EvidenceKind> {
        match self {
            Self::SessionEvent => Some(EvidenceKind::SessionEvent),
            Self::ToolCall => Some(EvidenceKind::ToolCall),
            Self::FileEdit => Some(EvidenceKind::FileEdit),
            Self::SessionMarker => Some(EvidenceKind::SessionMarker),
            Self::Relationship => Some(EvidenceKind::Relationship),
            Self::History => Some(EvidenceKind::History),
            Self::CommitLink => Some(EvidenceKind::CommitLink),
            Self::Session
            | Self::Presence
            | Self::Trajectory
            | Self::SourceObservation
            | Self::ObservationEvidence => None,
        }
    }

    fn table(self) -> FedTable {
        let table = |name, session, key, record| FedTable {
            name,
            source: "source",
            session,
            optional_session: false,
            key,
            record,
        };
        match self {
            Self::Session => table(
                "sessions",
                "session_id",
                &["source", "session_id"],
                &["session_id"],
            ),
            Self::SessionEvent => table(
                "session_events",
                "session_id",
                &["source", "session_id", "event_uid"],
                &["event_uid"],
            ),
            Self::ToolCall => table(
                "tool_calls",
                "session_id",
                &["source", "session_id", "tool_use_id"],
                &["tool_use_id"],
            ),
            Self::FileEdit => table(
                "file_edits",
                "session_id",
                &["source", "session_id", "tool_use_id"],
                &["tool_use_id"],
            ),
            Self::SessionMarker => table(
                "session_markers",
                "session_id",
                &["source", "session_id", "marker_uid"],
                &["marker_uid"],
            ),
            Self::Relationship => table(
                "session_relationships",
                "parent_session_id",
                &["source", "parent_session_id", "relationship_uid"],
                &["relationship_uid"],
            ),
            // A prompt may name no session; its identity is the prompt log's
            // own `UNIQUE(source, timestamp_ms, prompt)`.
            Self::History => FedTable {
                optional_session: true,
                ..table(
                    "history",
                    "session_id",
                    &["source", "timestamp_ms", "prompt"],
                    &["timestamp_ms", "prompt"],
                )
            },
            Self::Presence => table(
                "session_presences",
                "session_id",
                &["source", "session_id", "location"],
                &["location"],
            ),
            Self::CommitLink => table(
                "session_commit_links",
                "session_id",
                &["source", "session_id", "commit_sha", "match_method"],
                &["commit_sha", "match_method"],
            ),
            // A trajectory row carries no source column: every one is the
            // `trajectory` source, and its id is its session.
            Self::Trajectory => FedTable {
                source: "'trajectory'",
                ..table("trajectories", "id", &["id"], &["id"])
            },
            Self::SourceObservation => table(
                "session_observations",
                "session_id",
                &[
                    "source",
                    "session_id",
                    "location",
                    "connector_id",
                    "connector_instance",
                ],
                &["location", "connector_id", "connector_instance"],
            ),
            Self::ObservationEvidence => table(
                "observation_evidence",
                "session_id",
                &[
                    "source",
                    "session_id",
                    "location",
                    "connector_id",
                    "connector_instance",
                    "evidence_uid",
                ],
                &[
                    "location",
                    "connector_id",
                    "connector_instance",
                    "evidence_uid",
                ],
            ),
        }
    }

    /// The typed row's column list, for the kinds that have one.
    fn columns(self) -> Option<&'static str> {
        match self {
            Self::Session => Some(SESSION_COLUMNS),
            Self::SessionEvent => Some(SESSION_EVENT_COLUMNS),
            Self::ToolCall => Some(TOOL_CALL_COLUMNS),
            Self::FileEdit => Some(FILE_EDIT_COLUMNS),
            Self::SessionMarker => Some(SESSION_MARKER_COLUMNS),
            Self::Relationship => Some(RELATIONSHIP_COLUMNS),
            Self::History => Some(HISTORY_COLUMNS),
            Self::Presence
            | Self::CommitLink
            | Self::Trajectory
            | Self::SourceObservation
            | Self::ObservationEvidence => None,
        }
    }
}

/// The typed [`HistoryEntry`] columns, in the order [`row_to_history`] reads.
const HISTORY_COLUMNS: &str = "id, source, session_id, project, prompt, prompt_hash, timestamp_ms";

fn row_to_history(row: &rusqlite::Row<'_>) -> rusqlite::Result<HistoryEntry> {
    Ok(HistoryEntry {
        id: row.get(0)?,
        source: row.get(1)?,
        session_id: row.get(2)?,
        project: row.get(3)?,
        prompt: row.get(4)?,
        prompt_hash: row.get(5)?,
        timestamp_ms: row.get(6)?,
    })
}

/// One stamped table: where a record's source, session and identity live.
///
/// A record is named two ways. `key` is its identity -- the columns of the
/// table's own uniqueness constraint -- and is
/// what [`Change::key`] carries. `record` is the part of that identity the
/// tombstone's `record_key` holds beside the source and session: one column's
/// text, or a JSON array of several, so a tombstone keeps every key column's
/// stored type and [`Change::key`] can be rebuilt from it.
struct FedTable {
    name: &'static str,
    /// A column, or a quoted literal for a table that stores no source.
    source: &'static str,
    session: &'static str,
    /// Whether `session` may be NULL; a change reports NULL as `''`.
    optional_session: bool,
    key: &'static [&'static str],
    record: &'static [&'static str],
}

impl FedTable {
    fn revision_index(&self) -> String {
        format!("idx_{}_revision", self.name)
    }

    /// The source as SQL over `row` (`NEW`, `OLD` or the table name).
    fn source_sql(&self, row: &str) -> String {
        if self.source.starts_with('\'') {
            self.source.to_string()
        } else {
            format!("{row}.{}", self.source)
        }
    }

    /// The session as SQL over `row`, as an upsert reports it.
    fn session_sql(&self, row: &str) -> String {
        if self.optional_session {
            format!("COALESCE({row}.{}, '')", self.session)
        } else {
            format!("{row}.{}", self.session)
        }
    }

    /// Whether the session is part of the record's identity. A prompt's is
    /// not: `history` is unique on source, time and text, and a prompt that
    /// gains or changes its session is the same record.
    fn session_keyed(&self) -> bool {
        self.key.contains(&self.session)
    }

    /// The session as a tombstone stores it: the row's, when the session is
    /// part of the identity, else `''`, so a tombstone names exactly the
    /// record's key and a later insert of that key clears it whatever
    /// session it carries.
    fn tombstone_session_sql(&self, row: &str) -> String {
        if self.session_keyed() {
            format!("{row}.{}", self.session)
        } else {
            "''".to_string()
        }
    }

    /// The `record_key` as SQL over `row`.
    fn record_key_sql(&self, row: &str) -> String {
        match self.record {
            [column] => format!("{row}.{column}"),
            columns => format!(
                "json_array({})",
                columns
                    .iter()
                    .map(|column| format!("{row}.{column}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }

    /// True when a write moved the row to another identity.
    fn identity_changed_sql(&self) -> String {
        let mut changed = Vec::new();
        if !self.source.starts_with('\'') {
            changed.push(format!("OLD.{0} IS NOT NEW.{0}", self.source));
        }
        if self.session_keyed() {
            changed.push(format!("OLD.{0} IS NOT NEW.{0}", self.session));
        }
        changed.push(format!(
            "{} IS NOT {}",
            self.record_key_sql("OLD"),
            self.record_key_sql("NEW")
        ));
        changed.join(" OR ")
    }

    /// [`Change::key`] from a tombstone's identity columns.
    fn key_from_identity(
        &self,
        kind: ChangeKind,
        source: &str,
        session: &str,
        record_key: &str,
    ) -> Result<Vec<Value>> {
        let record: Vec<Value> = match self.record {
            [_] => vec![Value::String(record_key.to_string())],
            _ => serde_json::from_str(record_key).with_context(|| {
                format!(
                    "change feed: {} tombstone record key {record_key:?} is not a JSON array",
                    kind.as_str()
                )
            })?,
        };
        let mut key = Vec::with_capacity(self.key.len() + 1);
        key.push(Value::String(kind.as_str().to_string()));
        for column in self.key {
            if let Some(index) = self.record.iter().position(|part| part == column) {
                key.push(record.get(index).cloned().unwrap_or(Value::Null));
            } else if *column == self.source {
                key.push(Value::String(source.to_string()));
            } else {
                key.push(Value::String(session.to_string()));
            }
        }
        Ok(key)
    }

    /// [`Change::key`] from a stored row.
    fn key_from_row(&self, kind: ChangeKind, row: &StoredRow) -> Vec<Value> {
        let mut key = Vec::with_capacity(self.key.len() + 1);
        key.push(Value::String(kind.as_str().to_string()));
        key.extend(
            self.key
                .iter()
                .map(|column| row.get(column).cloned().unwrap_or(Value::Null)),
        );
        key
    }
}

/// The columns of `table` a stored row carries: every column but the feed's
/// own [`REVISION_COLUMN`], in table order, read from the live schema so a
/// column a migration adds is carried without a code change.
pub(crate) fn stored_columns(conn: &Connection, table: &str) -> Result<Vec<String>> {
    let columns = conn
        .prepare_cached("SELECT name FROM pragma_table_info(?1) ORDER BY cid")?
        .query_map([table], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(columns
        .into_iter()
        .filter(|column| column != REVISION_COLUMN)
        .collect())
}

fn stored_column_schema(conn: &Connection, table: &str) -> Result<Vec<(String, String)>> {
    let columns = conn
        .prepare_cached("SELECT name, type FROM pragma_table_info(?1) ORDER BY cid")?
        .query_map([table], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(columns
        .into_iter()
        .filter(|(column, _)| column != REVISION_COLUMN)
        .collect())
}

fn export_schema_digest(schema: &[(String, String)]) -> Result<String> {
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&(
            "relayhistory-change-feed-export-schema-v2",
            schema
        ))?)
    ))
}

fn export_schema_digests(conn: &Connection) -> Result<BTreeMap<String, String>> {
    ChangeKind::ALL
        .iter()
        .map(|kind| {
            let schema = stored_column_schema(conn, kind.table().name)?;
            let digest = export_schema_digest(&schema)?;
            Ok((kind.as_str().to_string(), digest))
        })
        .collect()
}

fn exported_kind_removed(
    previous: &BTreeMap<String, String>,
    current: &BTreeMap<String, String>,
) -> bool {
    previous.keys().any(|kind| !current.contains_key(kind))
}

fn feed_identity_exists(conn: &Connection) -> Result<bool> {
    let table: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='change_feed_store')",
        [],
        |row| row.get(0),
    )?;
    if !table {
        return Ok(false);
    }
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM change_feed_store WHERE singleton=1)",
        [],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

fn restamp_exported_rows(conn: &Connection, kind: ChangeKind) -> Result<()> {
    let table = kind.table().name;
    let count: i64 = conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
        row.get(0)
    })?;
    if count == 0 {
        return Ok(());
    }
    let base: i64 = conn.query_row(
        "SELECT version FROM observation_clock WHERE singleton=1",
        [],
        |row| row.get(0),
    )?;
    let head = base
        .checked_add(count)
        .context("change-feed revision overflow while restamping an exported schema")?;
    conn.execute(
        &format!(
            "WITH ranked(rowid, ordinal) AS (
                 SELECT rowid, ROW_NUMBER() OVER (ORDER BY rowid) FROM {table}
             )
             UPDATE {table}
             SET {REVISION_COLUMN} = ?1 + (
                 SELECT ordinal FROM ranked WHERE ranked.rowid = {table}.rowid
             )"
        ),
        [base],
    )?;
    conn.execute(
        "UPDATE observation_clock SET version=?1 WHERE singleton=1",
        [head],
    )?;
    Ok(())
}

/// Reconcile each kind's exported row shape. Existing feeds without a
/// fingerprint may already have delivered the older representation, so every
/// existing kind is restamped once above the current head. Later changes
/// restamp only the affected kind. A newly introduced kind has no prior
/// fingerprint and is recorded without restamping: the feed-version migration
/// that introduced it already backfilled its rows above the old head.
///
/// Keeping the store epoch and cursor rows intact is deliberate. Both external
/// watermarks and named cursors can resume normally and observe the affected
/// rows at their new revisions; unrelated kinds are never replayed.
fn reconcile_export_schema(conn: &Connection, identity_existed: bool) -> Result<()> {
    let current = export_schema_digests(conn)?;
    let stored: Option<String> = conn.query_row(
        &format!("SELECT {EXPORT_SCHEMA_DIGEST_COLUMN} FROM change_feed_store WHERE singleton=1"),
        [],
        |row| row.get(0),
    )?;
    let previous = stored
        .as_deref()
        .and_then(|value| serde_json::from_str::<BTreeMap<String, String>>(value).ok());
    if previous.as_ref() == Some(&current) {
        return Ok(());
    }
    let removed = previous
        .as_ref()
        .is_some_and(|previous| exported_kind_removed(previous, &current));
    if identity_existed && removed {
        // A removed kind has no live table left to restamp and emit. Reset the
        // semantic stream so a full reconciliation can remove that retired
        // material. Ordinary shape changes never take this store-wide path.
        conn.execute_batch(
            "UPDATE change_feed_store
             SET epoch = CASE WHEN epoch = 9223372036854775807
                              THEN -9223372036854775807 ELSE epoch + 2 END
             WHERE singleton=1;
             DELETE FROM consumer_cursors;",
        )?;
    } else if identity_existed {
        for kind in ChangeKind::ALL {
            let changed = match previous.as_ref() {
                Some(previous) => previous
                    .get(kind.as_str())
                    .is_some_and(|digest| Some(digest) != current.get(kind.as_str())),
                None => true,
            };
            if changed {
                restamp_exported_rows(conn, *kind)?;
            }
        }
    }
    conn.execute(
        &format!("UPDATE change_feed_store SET {EXPORT_SCHEMA_DIGEST_COLUMN}=? WHERE singleton=1"),
        [serde_json::to_string(&current)?],
    )?;
    Ok(())
}

/// A record exactly as its table stores it: every column but `revision`, in
/// table order, each value as SQLite holds it.
///
/// Text stays text even when it holds JSON, an integer stays an integer, a
/// real stays a real and NULL is `null`; nothing is parsed, defaulted or
/// derived. A column the table gains is carried as soon as it exists. A BLOB,
/// which no stamped table declares, is carried as an array of its bytes.
///
/// Serializes as one JSON object whose keys are in table order.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct StoredRow {
    columns: Vec<(Arc<str>, Value)>,
}

impl StoredRow {
    /// The stored value of `column`, or `None` when the table has no such
    /// column.
    pub fn get(&self, column: &str) -> Option<&Value> {
        self.columns
            .iter()
            .find(|(name, _)| &**name == column)
            .map(|(_, value)| value)
    }

    /// Every column and its stored value, in table order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Value)> + '_ {
        self.columns.iter().map(|(name, value)| (&**name, value))
    }

    /// How many columns the row carries.
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Whether the row carries no column.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
}

impl Serialize for StoredRow {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.columns.len()))?;
        for (name, value) in &self.columns {
            map.serialize_entry(&**name, value)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for StoredRow {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct Columns;
        impl<'de> serde::de::Visitor<'de> for Columns {
            type Value = StoredRow;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an object of column names to stored values")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<StoredRow, A::Error> {
                let mut columns = Vec::with_capacity(map.size_hint().unwrap_or(0));
                while let Some((name, value)) = map.next_entry::<String, Value>()? {
                    columns.push((Arc::from(name), value));
                }
                Ok(StoredRow { columns })
            }
        }
        deserializer.deserialize_map(Columns)
    }
}

fn stored_value(value: ValueRef<'_>) -> Value {
    match value {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(integer) => Value::from(integer),
        ValueRef::Real(real) => Value::from(real),
        ValueRef::Text(text) => Value::String(String::from_utf8_lossy(text).into_owned()),
        ValueRef::Blob(bytes) => Value::from(bytes.to_vec()),
    }
}

#[cfg(feature = "export")]
/// One row of a fed table, as a local export snapshot reads it.
pub(crate) struct LiveRow {
    pub source: String,
    /// The stored session, `None` for a prompt that names none.
    pub session: Option<String>,
    /// [`Change::key`] for this row.
    pub key: Vec<Value>,
    pub revision: u64,
    pub columns: StoredRow,
}

#[cfg(feature = "export")]
/// One row of a fed table while it is being visited: the cheap identity
/// fields are decoded, the stored columns only on [`RowidRow::decode`].
pub(crate) struct RowidRow<'r, 's> {
    pub rowid: i64,
    pub source: String,
    /// The stored session, `None` for a prompt that names none.
    pub session: Option<String>,
    row: &'r rusqlite::Row<'s>,
    names: &'r [Arc<str>],
    table: &'r FedTable,
    kind: ChangeKind,
}

#[cfg(feature = "export")]
impl RowidRow<'_, '_> {
    /// The whole row, every stored column converted to an owned value.
    pub(crate) fn decode(&self) -> Result<LiveRow> {
        let mut columns = Vec::with_capacity(self.names.len());
        for (offset, name) in self.names.iter().enumerate() {
            columns.push((
                Arc::clone(name),
                stored_value(self.row.get_ref(4 + offset)?),
            ));
        }
        let columns = StoredRow { columns };
        let revision: Option<i64> = self.row.get(3)?;
        Ok(LiveRow {
            source: self.source.clone(),
            session: self.session.clone(),
            key: self.table.key_from_row(self.kind, &columns),
            revision: revision.unwrap_or(0).max(0) as u64,
            columns,
        })
    }
}

#[cfg(feature = "export")]
/// Visit at most `limit` rows of `kind`'s table past rowid `after`, in rowid
/// order: one range of the table's own b-tree. `visit` returns whether to go
/// on; the statement stops stepping as soon as it says no, and a row's stored
/// columns are converted only when it asks for them, so a caller that fills
/// up early, or passes over rows it does not select, neither reads nor
/// decodes the rest of the budget (#308). Returns how many rows were visited.
pub(crate) fn visit_rows_by_rowid(
    conn: &Connection,
    kind: ChangeKind,
    after: i64,
    limit: usize,
    mut visit: impl FnMut(RowidRow<'_, '_>) -> Result<bool>,
) -> Result<usize> {
    let table = kind.table();
    let stored = stored_columns(conn, table.name)?;
    let names: Vec<Arc<str>> = stored
        .iter()
        .map(|column| Arc::from(column.as_str()))
        .collect();
    let sql = format!(
        "SELECT r.rowid, {source}, r.{session}, r.{REVISION_COLUMN}, {columns} FROM {name} r \
         WHERE r.rowid > ?1 ORDER BY r.rowid LIMIT ?2",
        source = table.source_sql("r"),
        session = table.session,
        name = table.name,
        columns = stored
            .iter()
            .map(|column| format!("r.\"{column}\""))
            .collect::<Vec<_>>()
            .join(", "),
    );
    let mut statement = conn.prepare_cached(&sql)?;
    let mut rows = statement.query(rusqlite::params![
        after,
        limit.min(i64::MAX as usize) as i64
    ])?;
    let mut visited = 0;
    while let Some(row) = rows.next()? {
        visited += 1;
        let go_on = visit(RowidRow {
            rowid: row.get(0)?,
            source: row.get(1)?,
            session: row.get(2)?,
            row,
            names: &names,
            table: &table,
            kind,
        })?;
        if !go_on {
            break;
        }
    }
    Ok(visited)
}

/// The typed row an upsert carries, per kind, so no second read is needed.
///
/// The variants differ in size because the rows do; an event row is several
/// times a tool call. Boxing the large ones would put an allocation between
/// the consumer and every row it reads, to save space in a value that exists
/// one page at a time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "row", rename_all = "snake_case")]
#[non_exhaustive]
#[allow(clippy::large_enum_variant)]
pub enum EvidenceRow {
    Session(ShallowSession),
    SessionEvent(SessionEvent),
    ToolCall(SessionToolCall),
    FileEdit(SessionFileEdit),
    SessionMarker(SessionMarker),
    Relationship(SessionRelationship),
    History(HistoryEntry),
    /// A kind with no typed row: presences, commit links, trajectories and
    /// connector observations. [`Change::columns`] is the row.
    Untyped,
}

/// What happened to the record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
#[non_exhaustive]
#[allow(clippy::large_enum_variant)]
pub enum ChangeOp {
    /// The record was written; this is its current row. A record seen before
    /// under the same key has been replaced, not duplicated.
    Upsert(EvidenceRow),
    /// The record was deleted.
    Delete,
}

/// One entry of the feed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Change {
    pub kind: ChangeKind,
    /// The record's source, or `None` when the stored name is one this build
    /// does not know -- a row written by a newer release. The drain carries
    /// such a row rather than failing on it; `source_name` names it.
    pub source: Option<Source>,
    /// The source exactly as stored.
    #[serde(default)]
    pub source_name: String,
    /// For a relationship, the parent session; for a trajectory, its id; for
    /// a prompt that names no session, empty. A prompt's session is not part
    /// of its identity, so a prompt's delete carries it empty too, and a
    /// prompt gaining a session is an upsert, never a delete.
    pub session_id: String,
    /// The record's identity within its source, session and kind: the one
    /// identity column's text (`event_uid`, `tool_use_id`, `marker_uid`,
    /// `relationship_uid`, `location`, a trajectory's id, or the session id
    /// itself for a catalog row), or for a kind whose identity spans several
    /// columns, those columns' stored values as a JSON array.
    pub record_key: String,
    /// The record's identity: the kind's wire name, then the stored value of each column of the table's uniqueness
    /// constraint, in order -- `["history", source, timestamp_ms, prompt]`,
    /// `["trajectory", id]`. An upsert and a delete of one record carry the
    /// same key.
    #[serde(default)]
    pub key: Vec<Value>,
    pub revision: u64,
    pub op: ChangeOp,
    /// The row as stored, for an upsert; `None` for a delete. See
    /// [`StoredRow`].
    #[serde(default)]
    pub columns: Option<StoredRow>,
}

/// How to read the feed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ChangeQuery {
    /// Which kinds to report; `None` means every kind.
    pub kinds: Option<Vec<ChangeKind>>,
    /// A named cursor kept inside the store. Required for
    /// [`Watermark::CONSUMER`] and for [`Changes::commit`].
    pub consumer: Option<String>,
    /// Rows per page, clamped to `1..=`[`MAX_CHANGE_BATCH`]; zero means
    /// [`DEFAULT_CHANGE_BATCH`].
    pub batch: usize,
    /// Report only this session's changes; see [`ChangeQuery::session`].
    pub session: Option<SessionIdentity>,
}

impl ChangeQuery {
    /// Report only these kinds.
    pub fn kinds(mut self, kinds: impl IntoIterator<Item = ChangeKind>) -> Self {
        self.kinds = Some(kinds.into_iter().collect());
        self
    }

    /// Read from, and commit to, the named cursor.
    pub fn consumer(mut self, name: impl Into<String>) -> Self {
        self.consumer = Some(name.into());
        self
    }

    /// Rows per page; see [`ChangeQuery::batch`].
    pub fn batch(mut self, batch: usize) -> Self {
        self.batch = batch;
        self
    }

    /// Report only one session's changes: exactly those whose
    /// [`Change::source_name`] and [`Change::session_id`] are these, read
    /// through each table's session index rather than the whole revision
    /// range. `source` is the stored name; one this build does not know is
    /// accepted.
    ///
    /// That is every kind that stores a session -- the catalog row, events,
    /// tool calls, file edits, markers, relationships (under their parent
    /// session), prompts, presences, commit links and connector
    /// observations -- plus a trajectory, whose session is its own id under
    /// the `trajectory` source. A prompt that names no session belongs to no
    /// session's drain, and nor does a prompt's delete: a prompt's session is
    /// not part of its identity, so its tombstone carries none. Both still
    /// reach the unfiltered feed.
    ///
    /// A session drain is a one-shot read -- the backfill of a session an
    /// embedder has just started following -- and cannot name a consumer
    /// ([`Error::InvalidArgument`]): a cursor is a position in one stream,
    /// and a position reached reading one session accounts for nothing
    /// about the others. Each page re-seeks the session, so a long session
    /// drains fastest with a large [`ChangeQuery::batch`].
    pub fn session(mut self, source: impl Into<String>, session_id: impl Into<String>) -> Self {
        self.session = Some(SessionIdentity::new(source, session_id));
        self
    }
}

/// The drain [`SessionStore::changes_since`] hands back.
///
/// Yields every change with `from < revision <= head()`, oldest first, paging
/// through the store in `batch`-sized indexed reads as it goes; at most one
/// page of typed rows is resident at a time. `position()`
/// follows the last change yielded, and reaches `head()` once the drain is
/// exhausted. Nothing written after the drain was opened is included — a
/// row re-stamped past the head while the drain runs is simply absent from
/// this pass and present, at its newer revision, in the next.
pub struct Changes {
    db_path: PathBuf,
    read_only: bool,
    conn: PooledConnection,
    kinds: Vec<ChangeKind>,
    consumer: Option<String>,
    session: Option<SessionIdentity>,
    batch: usize,
    head: Watermark,
    position: Watermark,
    /// The consumer's stored cursor when it stood past `head` at open: the
    /// one value this drain's commit replaces rather than keeps.
    stale_cursor: Option<Watermark>,
    buffer: VecDeque<Change>,
    exhausted: bool,
}

impl Changes {
    /// The store head when the drain was opened. Everything at or below it
    /// is reported by this drain; nothing above it is.
    pub fn head(&self) -> Watermark {
        self.head
    }

    /// The revision of the last change yielded, or the resolved start before
    /// the first, or [`Changes::head`] once the drain is exhausted.
    pub fn position(&self) -> Watermark {
        self.position
    }

    /// The named cursor this drain advances on commit, if one was given.
    pub fn consumer(&self) -> Option<&str> {
        self.consumer.as_deref()
    }

    /// Persist [`Changes::position`] as the consumer's cursor, and return the
    /// cursor as stored.
    ///
    /// The cursor moves only here, and only forward. A consumer that fails
    /// mid-drain and never commits resumes from its previous commit, not from
    /// wherever the drain had reached, so a partially applied page is re-read
    /// rather than skipped. A commit at or below the stored cursor — an
    /// older drain committing after a newer one, or a replay from an explicit
    /// watermark under a name that has already moved past it — leaves the
    /// cursor where it is, so the returned watermark can exceed
    /// [`Changes::position`]; a consumer that wants to reprocess drains from
    /// an explicit `from` and does not commit. The one exception is a stored
    /// cursor ahead of the store's head, which a resume refuses with
    /// [`Error::WatermarkAheadOfStore`]: it names no revision of this
    /// store, so a drain opened while the cursor stood past its head — the
    /// resync from [`Watermark::START`] — replaces exactly that cursor when
    /// it commits, however far the store has grown meanwhile. A named cursor
    /// is bound to the
    /// kind set it was first committed for: a drain over other kinds cannot
    /// resume it or move it ([`Error::ConsumerKindsMismatch`]), because
    /// its position accounts for nothing outside its own kinds. A commit
    /// writes only into the database the drain read: if the store's path
    /// now holds another one, or one whose head is behind the position, it
    /// fails with [`Error::WatermarkAheadOfStore`] and writes nothing.
    /// Fails on a read-only store and when no consumer was named.
    pub fn commit(&self) -> Result<Watermark, Error> {
        let Some(name) = &self.consumer else {
            return Err(Error::InvalidArgument(
                "changes_since: commit needs ChangeQuery::consumer to name the cursor".to_string(),
            ));
        };
        if self.read_only {
            return Err(Error::read_only("Changes::commit"));
        }
        let conn =
            open_db(&self.db_path).map_err(|error| Error::DatabaseOpen(format!("{error:#}")))?;
        commit_cursor(
            &conn,
            name,
            self.position,
            &KindSet {
                kinds: self.kinds.clone(),
            },
            self.stale_cursor,
        )
    }

    fn fill(&mut self) -> Result<()> {
        let hi = self.head.revision;
        // Both passes of a page read one snapshot. Without that, a writer
        // re-stamping or deleting the rows between the key pass and the row
        // pass could empty the window the cut describes, and an empty window
        // read as "nothing left" would jump the position to the head over
        // changes still below it -- which a later commit then skips for good.
        let snapshot = self.conn.unchecked_transaction()?;
        loop {
            let lo = self.position.revision;
            if lo >= hi {
                self.exhausted = true;
                self.position = self.head;
                break;
            }
            // Two passes, so no more than one page of typed rows is ever
            // resident. The first reads only revisions -- a covering read of
            // each revision index, `batch` integers per stream at most -- and
            // finds the cut: the `batch`-th smallest revision across every
            // stream. Any row a stream did not return sits above every row it
            // did, and therefore above the cut. The second pass fetches the
            // rows in `(lo, cut]`, which is exactly the page, because
            // revisions are unique per write.
            let session = self.session.as_ref();
            let Some(cut) = page_cut(&snapshot, &self.kinds, session, lo, hi, self.batch)? else {
                // The key pass itself found nothing left: that, and only
                // that, is exhaustion.
                self.exhausted = true;
                self.position = self.head;
                break;
            };
            let mut rows: Vec<Change> = Vec::with_capacity(self.batch);
            for kind in &self.kinds {
                rows.extend(read_upserts(
                    &snapshot, *kind, session, lo, cut, self.batch,
                )?);
            }
            rows.extend(read_tombstones(
                &snapshot,
                &self.kinds,
                session,
                lo,
                cut,
                self.batch,
            )?);
            rows.sort_by(|a, b| {
                (a.revision, a.kind, &a.record_key).cmp(&(b.revision, b.kind, &b.record_key))
            });
            rows.truncate(self.batch);
            if rows.is_empty() {
                // Cannot happen within one snapshot; defended anyway: the
                // window is known empty, so step past it and let the key
                // pass decide what remains rather than declaring the head.
                self.position = Watermark {
                    revision: cut,
                    epoch: self.head.epoch,
                };
                continue;
            }
            self.buffer.extend(rows);
            break;
        }
        snapshot.commit()?;
        Ok(())
    }
}

impl Iterator for Changes {
    type Item = Result<Change, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(change) = self.buffer.pop_front() {
                self.position = Watermark {
                    revision: change.revision,
                    epoch: self.head.epoch,
                };
                return Some(Ok(change));
            }
            if self.exhausted {
                return None;
            }
            if let Err(error) = self.fill() {
                self.exhausted = true;
                return Some(Err(Error::query(error)));
            }
        }
    }
}

impl std::fmt::Debug for Changes {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Changes")
            .field("db_path", &self.db_path)
            .field("kinds", &self.kinds)
            .field("consumer", &self.consumer)
            .field("session", &self.session)
            .field("batch", &self.batch)
            .field("head", &self.head)
            .field("position", &self.position)
            .field("exhausted", &self.exhausted)
            .finish_non_exhaustive()
    }
}

impl SessionStore {
    /// The store's change-feed head: the revision of the newest stamped row,
    /// with this database's epoch.
    ///
    /// A consumer holding a watermark above this, or one carrying another
    /// epoch, holds a position this store never issued: the database was
    /// reset or replaced under it, and the only recovery is a full resync
    /// from [`Watermark::START`].
    ///
    /// A database from before the feed existed answers `START`: nothing in it
    /// is stamped, so nothing in it is reported yet -- and its
    /// `observation_clock`, which predates the feed, is not a feed position.
    pub fn head_revision(&self) -> Result<Watermark, Error> {
        let mut conn = self.read_conn()?;
        if !conn
            .gate("change-feed", schema_is_current)
            .map_err(Error::query)?
        {
            return Ok(Watermark::START);
        }
        read_head(&conn).map_err(Error::query)
    }

    /// Every change after `from`, oldest first, up to the head at open.
    ///
    /// `from` is a revision this consumer has fully accounted for; pass
    /// [`Watermark::START`] to replay the whole store, or
    /// [`Watermark::CONSUMER`] with [`ChangeQuery::consumer`] set to resume
    /// from that consumer's last commit. The drain pages through the store in
    /// [`ChangeQuery::batch`]-sized indexed reads and never materialises more
    /// than one page. See [`Changes`] for what it yields and
    /// [`Changes::commit`] for how a named cursor advances.
    ///
    /// Fails with [`Error::WatermarkAheadOfStore`] when `from` was issued
    /// by another database -- its epoch is not this store's, which is what
    /// catches a replacement database that has since counted past it -- or
    /// when the resolved start exceeds the head. [`Watermark::START`] and
    /// [`Watermark::CONSUMER`] name no store and pass the first check. Like
    /// the marker page, the schema gate is here rather
    /// than at `open`: a read-only store over a database written before the
    /// feed existed is told to migrate rather than served `no such column`.
    pub fn changes_since(&self, from: Watermark, query: ChangeQuery) -> Result<Changes, Error> {
        let mut conn = self.read_conn()?;
        if !conn
            .gate("change-feed", schema_is_current)
            .map_err(Error::query)?
        {
            return Err(Error::DatabaseOpen(format!(
                "{} predates the change-feed schema this version reads; \
                 open it writable once (or run a sync) to migrate it",
                self.db_path().display()
            )));
        }
        let kind_set = KindSet::normalize(query.kinds);
        let batch = match query.batch {
            0 => DEFAULT_CHANGE_BATCH,
            batch => batch.min(MAX_CHANGE_BATCH),
        };
        if from == Watermark::CONSUMER && query.consumer.is_none() {
            return Err(Error::InvalidArgument(
                "changes_since: Watermark::CONSUMER needs ChangeQuery::consumer to name the cursor"
                    .to_string(),
            ));
        }
        if let Some(session) = &query.session {
            if query.consumer.is_some() {
                return Err(Error::InvalidArgument(
                    "changes_since: ChangeQuery::session is a one-shot read and cannot name a \
                     consumer; a cursor committed from one session's changes would skip every \
                     other session's. Drain the session from Watermark::START without a \
                     consumer, and keep the named cursor for the whole feed"
                        .to_string(),
                ));
            }
            if session.source_name.is_empty() || session.session_id.is_empty() {
                return Err(Error::InvalidArgument(
                    "changes_since: ChangeQuery::session needs a nonempty source and session id"
                        .to_string(),
                ));
            }
        }
        let (start, head, stale_cursor) =
            resolve_start_and_head(&conn, from, query.consumer.as_deref(), &kind_set)?;
        if from != Watermark::START && from != Watermark::CONSUMER && from.epoch != head.epoch {
            return Err(Error::WatermarkAheadOfStore(format!(
                "changes_since: watermark {} was not issued by this store (epoch {}, this \
                 store's is {}); the database was reset or replaced, resync from \
                 Watermark::START",
                from.revision, from.epoch, head.epoch
            )));
        }
        if start.revision > head.revision {
            return Err(Error::WatermarkAheadOfStore(format!(
                "changes_since: watermark {} is ahead of the store head {}; the database was \
                 reset or replaced, resync from Watermark::START",
                start.revision, head.revision
            )));
        }
        Ok(Changes {
            db_path: self.db_path().to_path_buf(),
            read_only: self.read_only(),
            conn,
            kinds: kind_set.kinds,
            consumer: query.consumer,
            session: query.session,
            batch,
            head,
            position: start,
            stale_cursor,
            buffer: VecDeque::new(),
            exhausted: false,
        })
    }
}

/// Resolve where a drain starts and where it is bounded, from one snapshot.
///
/// The two reads share a read transaction, so a writer advancing the store
/// and a sibling drain committing the resulting cursor between them cannot
/// make a valid cursor look ahead of the head. The cursor is also read first:
/// even without the snapshot, a cursor that moved after being read can only
/// cause a safe replay, never a false reset.
///
/// A named cursor is resumed only by a drain over the kind set it was
/// committed for; see [`KindSet`].
///
/// The third value is the consumer's stored cursor when, in that same
/// snapshot, it stood past the head -- the stale cursor this drain's commit
/// replaces. It is decided here, against the head the drain is bounded to,
/// because a writer growing the store while the drain runs changes nothing
/// about what the drain has accounted for.
fn resolve_start_and_head(
    conn: &Connection,
    from: Watermark,
    consumer: Option<&str>,
    kinds: &KindSet,
) -> Result<(Watermark, Watermark, Option<Watermark>), Error> {
    let snapshot = conn.unchecked_transaction().map_err(Error::sql)?;
    let cursor = match consumer {
        Some(name) => read_cursor(&snapshot, name).map_err(Error::query)?,
        None => None,
    };
    let start =
        match (from == Watermark::CONSUMER, consumer, &cursor) {
            (true, Some(name), Some((revision, stored))) => {
                let offered = kinds.stored();
                if *stored != offered {
                    return Err(kinds_mismatch(name, stored, &offered));
                }
                *revision
            }
            (true, Some(_), None) => Watermark::START.revision,
            (true, None, _) => return Err(Error::InvalidArgument(
                "changes_since: Watermark::CONSUMER needs ChangeQuery::consumer to name the cursor"
                    .to_string(),
            )),
            (false, _, _) => from.revision,
        };
    let head = read_head(&snapshot).map_err(Error::query)?;
    snapshot.commit().map_err(Error::sql)?;
    // A named cursor lives in the database it counts, so it is always in
    // this store's epoch.
    let in_store = |revision| Watermark {
        revision,
        epoch: head.epoch,
    };
    let stale_cursor = cursor
        .map(|(revision, _)| revision)
        .filter(|revision| *revision > head.revision)
        .map(in_store);
    Ok((in_store(start), head, stale_cursor))
}

/// The revision range, and its order, for one page read.
///
/// Unfiltered, it reads the revision index. Restricted to one session, the
/// session's own `(source, session_id, ...)` index is the narrow one: the
/// range is written `+revision`, which no index can serve, so the planner
/// seeks the session and sorts only that session's rows, rather than walking
/// the whole revision range to discard every other session's.
fn revision_range(first: usize, filtered: bool) -> (String, String) {
    let column = if filtered {
        format!("+{REVISION_COLUMN}")
    } else {
        REVISION_COLUMN.to_string()
    };
    (
        format!("{column} > ?{first} AND {column} <= ?{}", first + 1),
        format!("ORDER BY {column} ASC LIMIT ?{}", first + 2),
    )
}

/// The session predicate for one kind's upsert reads, over parameters
/// `?4` (source) and `?5` (session id), or nothing when unfiltered.
fn upsert_session_sql(kind: ChangeKind, filtered: bool) -> String {
    if !filtered {
        return String::new();
    }
    let table = kind.table();
    format!(
        " AND {} = ?4 AND {}.{} = ?5",
        table.source_sql(table.name),
        table.name,
        table.session
    )
}

/// The session predicate for tombstone reads, over `?5` and `?6`.
fn tombstone_session_sql(filtered: bool) -> &'static str {
    if filtered {
        " AND source = ?5 AND session_id = ?6"
    } else {
        ""
    }
}

/// The revision-only page query for one kind: a covering read of the
/// revision index, or of the session index when restricted to one session.
fn upsert_key_sql(kind: ChangeKind, filtered: bool) -> String {
    let (range, order) = revision_range(1, filtered);
    format!(
        "SELECT {REVISION_COLUMN} FROM {name} WHERE {range}{session} {order}",
        name = kind.table().name,
        session = upsert_session_sql(kind, filtered),
    )
}

fn tombstone_key_sql(filtered: bool) -> String {
    let (range, order) = revision_range(2, filtered);
    format!(
        "SELECT {REVISION_COLUMN} FROM evidence_tombstones \
         WHERE kind = ?1 AND {range}{session} {order}",
        session = tombstone_session_sql(filtered),
    )
}

/// The parameters a page read binds: the range and limit, then the session
/// when the drain is restricted to one.
fn page_params(
    kind: Option<ChangeKind>,
    session: Option<&SessionIdentity>,
    lo: u64,
    hi: u64,
    batch: usize,
) -> Vec<rusqlite::types::Value> {
    let mut values: Vec<rusqlite::types::Value> = Vec::with_capacity(6);
    if let Some(kind) = kind {
        values.push(kind.as_str().to_string().into());
    }
    values.extend([
        (lo as i64).into(),
        (hi as i64).into(),
        (batch as i64).into(),
    ]);
    if let Some(session) = session {
        values.push(session.source_name.clone().into());
        values.push(session.session_id.clone().into());
    }
    values
}

/// The highest revision of the next page: the `batch`-th smallest revision
/// in `(lo, hi]` across every stream, or `hi` when fewer remain, or `None`
/// when nothing does. Reads revisions only.
fn page_cut(
    conn: &Connection,
    kinds: &[ChangeKind],
    session: Option<&SessionIdentity>,
    lo: u64,
    hi: u64,
    batch: usize,
) -> Result<Option<u64>> {
    let mut revisions: Vec<u64> = Vec::new();
    let mut collect = |statement: &mut rusqlite::CachedStatement<'_>,
                       values: &[rusqlite::types::Value]|
     -> Result<()> {
        let rows = statement.query_map(rusqlite::params_from_iter(values), |row| {
            row.get::<_, i64>(0)
        })?;
        for row in rows {
            revisions.push(row?.max(0) as u64);
        }
        Ok(())
    };
    let filtered = session.is_some();
    let range = page_params(None, session, lo, hi, batch);
    for kind in kinds {
        let mut statement = conn.prepare_cached(&upsert_key_sql(*kind, filtered))?;
        collect(&mut statement, &range)?;
    }
    let mut tombstones = conn.prepare_cached(&tombstone_key_sql(filtered))?;
    for kind in kinds {
        collect(
            &mut tombstones,
            &page_params(Some(*kind), session, lo, hi, batch),
        )?;
    }
    if revisions.is_empty() {
        return Ok(None);
    }
    revisions.sort_unstable();
    Ok(Some(
        revisions
            .get(batch.saturating_sub(1))
            .copied()
            .unwrap_or(hi)
            .min(hi),
    ))
}

pub(crate) fn read_head(conn: &Connection) -> Result<Watermark> {
    let (revision, epoch): (Option<i64>, Option<i64>) = conn
        .query_row(
            "SELECT (SELECT version FROM observation_clock WHERE singleton = 1), \
                    (SELECT epoch FROM change_feed_store WHERE singleton = 1)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .context("reading the change-feed head")?;
    let epoch = epoch.context("the change-feed store has no epoch")?;
    Ok(Watermark {
        revision: revision.unwrap_or(0).max(0) as u64,
        epoch: epoch as u64,
    })
}

/// A named cursor's revision and the kind set it is bound to.
fn read_cursor(conn: &Connection, name: &str) -> Result<Option<(u64, String)>> {
    let row: Option<(i64, String)> = conn
        .query_row(
            &format!("SELECT revision, {KINDS_COLUMN} FROM consumer_cursors WHERE name = ?"),
            [name],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    Ok(row.map(|(revision, kinds)| (revision.max(0) as u64, kinds)))
}

/// Monotonic: the stored cursor is the greater of what it holds and what is
/// offered, so a stale commit cannot rewind a consumer behind a newer one.
/// And bound: a cursor committed for one kind set is never moved by a drain
/// over another. Both in one statement, so a sibling cannot slip between the
/// check and the write.
///
/// `stale` is the cursor the drain saw standing past its head at open, if
/// any. Such a cursor is not a position in this store's history -- every
/// drain's position is at most its head and the clock never moves back, so
/// only a reset or replaced clock puts one there -- and `changes_since`
/// refuses to resume it, sending the consumer to `Watermark::START`. That
/// resync's commit replaces it, where keeping the maximum would pin the
/// consumer to a revision the resync never covered. The replacement is a
/// compare-and-swap on the exact value seen at open: a commit that moved the
/// cursor since is a real position, and the monotonic rule applies to it.
///
/// And valid: the position must belong to the database the cursor is written
/// into. A commit opens the store's path afresh, and the path can name
/// another database by then; a cursor is only a revision number, so writing
/// one drain's position into a replacement would make the next resume skip
/// the replacement's own rows below it. The statement writes nothing unless
/// that database's epoch is the position's and its head has reached the
/// position.
fn commit_cursor(
    conn: &Connection,
    name: &str,
    position: Watermark,
    kinds: &KindSet,
    stale: Option<Watermark>,
) -> Result<Watermark, Error> {
    let offered = kinds.stored();
    let revision: Option<i64> = conn
        .query_row(
            &format!(
                "INSERT INTO consumer_cursors (name, revision, updated_ms, {KINDS_COLUMN}) \
                 SELECT ?1, ?2, ?3, ?4 \
                 WHERE (SELECT epoch FROM change_feed_store WHERE singleton = 1) = ?6 \
                   AND ?2 <= (SELECT version FROM observation_clock WHERE singleton = 1) \
                 ON CONFLICT(name) DO UPDATE SET \
                     revision = CASE WHEN consumer_cursors.revision = ?5 \
                         THEN excluded.revision \
                         ELSE MAX(consumer_cursors.revision, excluded.revision) END, \
                     updated_ms = excluded.updated_ms \
                     WHERE consumer_cursors.{KINDS_COLUMN} = excluded.{KINDS_COLUMN} \
                 RETURNING revision"
            ),
            params![
                name,
                position.revision as i64,
                crate::now_ms(),
                offered,
                stale.map(|stale| stale.revision as i64),
                position.epoch as i64
            ],
            |row| row.get(0),
        )
        .optional()
        .map_err(Error::sql)?;
    if let Some(revision) = revision {
        return Ok(Watermark {
            revision: revision.max(0) as u64,
            epoch: position.epoch,
        });
    }
    // Nothing was written. Either the database is not the one the drain
    // read, or the conflict clause declined a row under another kind set.
    let head = read_head(conn).map_err(Error::query)?;
    if head.epoch != position.epoch {
        return Err(Error::WatermarkAheadOfStore(format!(
            "commit: the drain read the database with epoch {}, but the store's path now \
             holds epoch {}; the database was replaced, resync from Watermark::START",
            position.epoch, head.epoch
        )));
    }
    if position.revision > head.revision {
        return Err(Error::WatermarkAheadOfStore(format!(
            "commit: position {} is ahead of the store head {}; the database was reset, \
             resync from Watermark::START",
            position.revision, head.revision
        )));
    }
    let stored = read_cursor(conn, name)
        .map_err(Error::query)?
        .map(|(_, stored)| stored)
        .unwrap_or_default();
    Err(kinds_mismatch(name, &stored, &offered))
}

/// The page query for one kind: an indexed range read, oldest first.
///
/// Four groups of columns, in order: the typed row's columns, for the kinds
/// that have one, read by position from zero; the tombstone identity --
/// source, session, record key -- computed by the same SQL the triggers use,
/// so an upsert and a delete of one record name it identically; the stored
/// columns; and the revision.
fn upsert_sql(kind: ChangeKind, stored: &[String], filtered: bool) -> String {
    let table = kind.table();
    let name = table.name;
    let mut select = Vec::new();
    if let Some(columns) = kind.columns() {
        select.push(columns.to_string());
    }
    select.push(table.source_sql(name));
    select.push(table.session_sql(name));
    select.push(table.record_key_sql(name));
    select.extend(
        stored
            .iter()
            .map(|column| format!("{name}.\"{}\"", column.replace('"', "\"\""))),
    );
    let (range, order) = revision_range(1, filtered);
    format!(
        "SELECT {select}, {name}.{REVISION_COLUMN} FROM {name} \
         WHERE {range}{session} {order}",
        select = select.join(", "),
        session = upsert_session_sql(kind, filtered),
    )
}

fn read_upserts(
    conn: &Connection,
    kind: ChangeKind,
    session: Option<&SessionIdentity>,
    lo: u64,
    hi: u64,
    batch: usize,
) -> Result<Vec<Change>> {
    let table = kind.table();
    let stored = stored_columns(conn, table.name)?;
    let names: Vec<Arc<str>> = stored
        .iter()
        .map(|column| Arc::from(column.as_str()))
        .collect();
    let mut statement = conn.prepare_cached(&upsert_sql(kind, &stored, session.is_some()))?;
    // Everything before the identity is the typed row.
    let identity = statement.column_count() - stored.len() - 4;
    let values = page_params(None, session, lo, hi, batch);
    let rows = statement.query_map(rusqlite::params_from_iter(values), |row| {
        let evidence = match kind {
            ChangeKind::Session => EvidenceRow::Session(row_to_session(row)?),
            ChangeKind::SessionEvent => EvidenceRow::SessionEvent(row_to_session_event(row)?),
            ChangeKind::ToolCall => EvidenceRow::ToolCall(row_to_tool_call(row)?),
            ChangeKind::FileEdit => EvidenceRow::FileEdit(row_to_file_edit(row)?),
            ChangeKind::SessionMarker => EvidenceRow::SessionMarker(row_to_session_marker(row)?),
            ChangeKind::Relationship => EvidenceRow::Relationship(map_relationship(row)?),
            ChangeKind::History => EvidenceRow::History(row_to_history(row)?),
            ChangeKind::Presence
            | ChangeKind::CommitLink
            | ChangeKind::Trajectory
            | ChangeKind::SourceObservation
            | ChangeKind::ObservationEvidence => EvidenceRow::Untyped,
        };
        let source: String = row.get(identity)?;
        let session_id: String = row.get(identity + 1)?;
        let record_key: String = row.get(identity + 2)?;
        let mut columns = Vec::with_capacity(names.len());
        for (offset, name) in names.iter().enumerate() {
            columns.push((
                Arc::clone(name),
                stored_value(row.get_ref(identity + 3 + offset)?),
            ));
        }
        let revision: i64 = row.get(identity + 3 + names.len())?;
        Ok((
            source,
            session_id,
            record_key,
            revision,
            evidence,
            StoredRow { columns },
        ))
    })?;
    let mut changes = Vec::new();
    for row in rows {
        let (source, session_id, record_key, revision, evidence, columns) = row?;
        changes.push(Change {
            kind,
            source: Source::parse(&source),
            source_name: source,
            session_id,
            record_key,
            key: table.key_from_row(kind, &columns),
            revision: revision.max(0) as u64,
            op: ChangeOp::Upsert(evidence),
            columns: Some(columns),
        });
    }
    Ok(changes)
}

/// The tombstone page query for one kind. One kind per read, not a `kind IN`
/// list: the tombstone index is `(kind, revision)`, and a single-kind
/// equality is what lets the range come back in revision order without a
/// temporary sort.
fn tombstone_sql(filtered: bool) -> String {
    let (range, order) = revision_range(2, filtered);
    format!(
        "SELECT source, session_id, record_key, {REVISION_COLUMN} FROM evidence_tombstones \
         WHERE kind = ?1 AND {range}{session} {order}",
        session = tombstone_session_sql(filtered),
    )
}

fn read_tombstones(
    conn: &Connection,
    kinds: &[ChangeKind],
    session: Option<&SessionIdentity>,
    lo: u64,
    hi: u64,
    batch: usize,
) -> Result<Vec<Change>> {
    let mut changes = Vec::new();
    let mut statement = conn.prepare_cached(&tombstone_sql(session.is_some()))?;
    for kind in kinds {
        let table = kind.table();
        let rows = statement.query_map(
            rusqlite::params_from_iter(page_params(Some(*kind), session, lo, hi, batch)),
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )?;
        for row in rows {
            let (source, session_id, record_key, revision) = row?;
            let key = table.key_from_identity(*kind, &source, &session_id, &record_key)?;
            changes.push(Change {
                kind: *kind,
                source: Source::parse(&source),
                source_name: source,
                session_id,
                record_key,
                key,
                revision: revision.max(0) as u64,
                op: ChangeOp::Delete,
                columns: None,
            });
        }
    }
    Ok(changes)
}

// ---------------------------------------------------------------------------
// schema
// ---------------------------------------------------------------------------

/// The catalog row a consumer receives carries `locations`, which is derived
/// from `session_presences` rather than stored on `sessions`. A presence
/// coming or going is therefore a change to the session row as the feed
/// reports it, and must re-stamp that row even though `sessions` itself was
/// not written — a subagent cleanup that drops the local presence and keeps
/// the remote one changes nothing else.
const PRESENCE_TRIGGERS: &[&str] = &[
    "change_feed_session_locations_insert",
    "change_feed_session_locations_update",
    "change_feed_session_locations_delete",
];

/// The names the session re-stamp triggers had before a presence was a kind
/// of its own. They are the presence kind's stamping triggers' names now, so
/// the migration to [`MIGRATION`] drops them before either set is created.
const RETIRED_PRESENCE_TRIGGERS: &[&str] = &[
    "change_feed_session_presences_insert",
    "change_feed_session_presences_update",
    "change_feed_session_presences_delete",
];

fn trigger_names(kind: ChangeKind) -> [String; 3] {
    let name = kind.table().name;
    [
        format!("change_feed_{name}_insert"),
        format!("change_feed_{name}_update"),
        format!("change_feed_{name}_delete"),
    ]
}

/// The columns of `table` an update must change to take a new revision:
/// every column [`stored_columns`] carries, which is every column but
/// `revision` itself, read from the live schema.
///
/// Read from the live schema rather than a list in code because columns are
/// added over time (`ensure_columns`, the `REQUIRED_*_COLUMNS` lists): a
/// column a migration adds is guarded as soon as [`init_schema`] regenerates
/// the trigger, which [`schema_is_current`] makes it do on the open after the
/// column appears.
///
/// No column is left out, so a consumer that replays the feed holds every
/// table exactly, column for column. Each column was considered:
///
/// - The two that are only "when this database last wrote the row" --
///   `session_relationships.updated_ms` and `session_observations.updated_ms`
///   -- were the ones a re-read rewrote with `now` and nothing else. Leaving
///   them out here would have made a consumer's copy drift from the table;
///   instead their writers (`record_relationship`, `observations::upsert`)
///   skip an upsert that changes nothing else, so the stamp is the time of the
///   last change and an unchanged re-read writes nothing.
/// - `trajectories.updated_ms` is the trajectory file's mtime, a fact about
///   the source; a trajectory is re-read only when its stamp moves.
/// - `session_relationships.created_ms` and
///   `session_commit_links.created_at_ms` are written once; no upsert
///   rewrites them.
/// - `sessions.source_stamp` / `discovery_state` / `parser_version` and the
///   presence and observation stamps move only when the source moved or the
///   parser changed, and the catalog digest behind `SyncReport::changed`
///   relies on every write to one of them moving the head.
pub(crate) fn stamped_columns(conn: &Connection, table: &str) -> Result<Vec<String>> {
    stored_columns(conn, table)
}

/// The `WHEN` clause of `table`'s update trigger.
///
/// `NEW.revision = OLD.revision` is what stops the trigger re-firing on its
/// own stamp under `recursive_triggers` (the stamp changes `revision` and
/// only that). The rest is what makes an upsert that rewrites a row with the
/// values it already holds leave it alone: `IS NOT` rather than `<>`, so a
/// NULL becoming a value, or a value becoming NULL, is a change. A change of
/// identity changes a key column, so it is always stamped.
///
/// Generated from the live column list; [`update_trigger_is_current`]
/// compares an installed trigger against it, so the text here is also the
/// fingerprint that decides when a trigger is rebuilt.
fn update_guard(conn: &Connection, table: &str) -> Result<String> {
    let columns = stamped_columns(conn, table)?;
    anyhow::ensure!(
        !columns.is_empty(),
        "change feed: {table} has no columns to guard its update trigger with"
    );
    let changed = columns
        .iter()
        .map(|column| format!("NEW.{column} IS NOT OLD.{column}"))
        .collect::<Vec<_>>()
        .join(" OR ");
    Ok(format!(
        "WHEN NEW.{REVISION_COLUMN} = OLD.{REVISION_COLUMN} AND ({changed})"
    ))
}

/// The guard of the presence trigger that re-stamps a session when one of
/// its presences moves. The catalog row derives `locations` from which
/// presences *exist*, so an update that keeps a presence's identity -- a new
/// `source_stamp` or `raw_locator` on every append -- cannot change the row
/// the feed reports for the session, and re-stamping it would report it
/// unchanged. The presence row itself is stamped by its own kind's trigger.
const PRESENCE_MOVED_GUARD: &str = "WHEN NEW.revision = OLD.revision AND \
     (OLD.source IS NOT NEW.source OR OLD.session_id IS NOT NEW.session_id \
      OR OLD.location IS NOT NEW.location)";

/// The stored text of trigger `name`, or `None` when it does not exist.
fn trigger_sql(conn: &Connection, name: &str) -> Result<Option<String>> {
    Ok(conn
        .prepare_cached("SELECT sql FROM sqlite_master WHERE type = 'trigger' AND name = ?")?
        .query_row([name], |row| row.get::<_, Option<String>>(0))
        .optional()?
        .flatten())
}

/// Whether trigger `name` exists and carries `guard`. SQLite keeps a
/// trigger's text as written past its first keywords, so the guard is found
/// verbatim in a trigger built from it, and a trigger built before a column
/// was added -- or before the guard existed at all -- does not contain it.
fn update_trigger_is_current(conn: &Connection, name: &str, guard: &str) -> Result<bool> {
    Ok(trigger_sql(conn, name)?.is_some_and(|sql| sql.contains(guard)))
}

/// Drop `table`'s change-feed update trigger, ahead of an `ALTER TABLE ...
/// DROP COLUMN` on it.
///
/// The trigger's guard names every column of its table, and SQLite refuses to
/// drop a column a trigger names. The migration pass that drops the column
/// recreates the trigger from the remaining columns in [`init_schema`], which
/// runs last in the same pass; until then, updates to `table` take no
/// revision, so nothing between the two may update it. A table the feed does
/// not stamp is left alone.
pub(crate) fn release_update_guard(conn: &Connection, table: &str) -> Result<()> {
    if let Some(kind) = ChangeKind::ALL
        .iter()
        .find(|kind| kind.table().name == table)
    {
        let [_, update, _] = trigger_names(*kind);
        conn.execute_batch(&format!("DROP TRIGGER IF EXISTS {update};"))?;
    }
    Ok(())
}

/// Whether every write that changes the catalog (`sessions`) -- an insert, a
/// delete, an update that changes a column -- moves the database-wide
/// clock: the clock and the feed's identity exist, and the three `sessions`
/// triggers that bump it are installed. This is the one guarantee a catalog
/// digest keyed on the head needs, checked in five schema lookups rather than
/// the full [`schema_is_current`] walk over every feed table, since it runs
/// on every tick, forced or not.
pub(crate) fn catalog_writes_move_the_head(conn: &Connection) -> Result<bool> {
    let mut object = conn.prepare_cached("SELECT 1 FROM sqlite_master WHERE name = ? LIMIT 1")?;
    for name in ["observation_clock", "change_feed_store"] {
        if !object.exists([name])? {
            return Ok(false);
        }
    }
    for trigger in trigger_names(ChangeKind::Session) {
        if !object.exists([trigger])? {
            return Ok(false);
        }
    }
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM change_feed_store WHERE singleton = 1)",
        [],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

/// Whether this database has everything [`init_schema`] would add.
pub(crate) fn schema_is_current(conn: &Connection) -> Result<bool> {
    let mut object = conn.prepare("SELECT 1 FROM sqlite_master WHERE name = ? LIMIT 1")?;
    for name in [
        "evidence_tombstones",
        "consumer_cursors",
        "idx_evidence_tombstones_kind_revision",
        "observation_clock",
        "change_feed_store",
    ] {
        if !object.exists([name])? {
            return Ok(false);
        }
    }
    let identified: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM change_feed_store WHERE singleton = 1)",
        [],
        |row| row.get(0),
    )?;
    if !identified {
        return Ok(false);
    }
    let fingerprinted: bool = conn.query_row(
        &format!(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('change_feed_store') WHERE name='{EXPORT_SCHEMA_DIGEST_COLUMN}')"
        ),
        [],
        |row| row.get(0),
    )?;
    if !fingerprinted {
        return Ok(false);
    }
    let fingerprinted: bool = conn.query_row(
        &format!(
            "SELECT {EXPORT_SCHEMA_DIGEST_COLUMN} IS NOT NULL FROM change_feed_store WHERE singleton=1"
        ),
        [],
        |row| row.get(0),
    )?;
    if !fingerprinted {
        return Ok(false);
    }
    for kind in ChangeKind::ALL {
        let table = kind.table();
        if !object.exists([table.revision_index()])? {
            return Ok(false);
        }
        let [insert, update, delete] = trigger_names(*kind);
        for trigger in [insert, delete] {
            if !object.exists([trigger])? {
                return Ok(false);
            }
        }
        // The update trigger must also guard on the table's columns as they
        // are now: one built before a column was added would let an update
        // that changes only that column go unreported.
        if !update_trigger_is_current(conn, &update, &update_guard(conn, table.name)?)? {
            return Ok(false);
        }
        let stamped: bool = conn.query_row(
            &format!(
                "SELECT EXISTS(SELECT 1 FROM pragma_table_info('{}') WHERE name = ?)",
                table.name
            ),
            [REVISION_COLUMN],
            |row| row.get(0),
        )?;
        if !stamped {
            return Ok(false);
        }
    }
    for trigger in PRESENCE_TRIGGERS {
        if !object.exists([*trigger])? {
            return Ok(false);
        }
    }
    if !update_trigger_is_current(conn, PRESENCE_TRIGGERS[1], PRESENCE_MOVED_GUARD)? {
        return Ok(false);
    }
    let bound: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('consumer_cursors') WHERE name = ?)",
        [KINDS_COLUMN],
        |row| row.get(0),
    )?;
    if !bound {
        return Ok(false);
    }
    migration_applied(conn, MIGRATION)
}

/// Add the revision columns, tombstone and cursor tables, indexes and the
/// stamping triggers, and stamp every row that predates them.
///
/// Runs inside `init_db`'s serialized migration pass, after the observation
/// schema has created the clock the triggers draw from. Idempotent: a current
/// database passes through on `IF NOT EXISTS` checks and one marker read.
pub(crate) fn init_schema(conn: &Connection) -> Result<()> {
    let identity_existed = feed_identity_exists(conn)?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS evidence_tombstones (
             kind TEXT NOT NULL,
             source TEXT NOT NULL,
             session_id TEXT NOT NULL,
             record_key TEXT NOT NULL,
             revision INTEGER NOT NULL,
             PRIMARY KEY (kind, source, session_id, record_key)
         );
         CREATE INDEX IF NOT EXISTS idx_evidence_tombstones_kind_revision \
             ON evidence_tombstones(kind, revision);
         CREATE TABLE IF NOT EXISTS consumer_cursors (
             name TEXT PRIMARY KEY,
             revision INTEGER NOT NULL,
             updated_ms INTEGER NOT NULL,
             kinds TEXT NOT NULL DEFAULT '*'
         );
         CREATE TABLE IF NOT EXISTS change_feed_store (
             singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
             epoch INTEGER NOT NULL,
             export_schema_digest TEXT
         );",
    )?;
    // The database's identity in every watermark it issues; see
    // `Watermark::epoch`. A nonzero random value, so a fresh database -- which
    // counts its revisions from zero again -- cannot pass for the one it
    // replaced. Export-schema changes keep this identity and restamp only the
    // affected rows above the current head.
    conn.execute(
        "INSERT OR IGNORE INTO change_feed_store (singleton, epoch) VALUES (1, random() | 1)",
        [],
    )?;
    ensure_columns(
        conn,
        "change_feed_store",
        &[(EXPORT_SCHEMA_DIGEST_COLUMN, "TEXT")],
    )?;
    // A cursor is a position in one kind set's stream; see `KindSet`. A
    // database that created the table before the column existed gains it
    // here, and both paths converge.
    ensure_columns(
        conn,
        "consumer_cursors",
        &[(KINDS_COLUMN, "TEXT NOT NULL DEFAULT '*'")],
    )?;
    let backfill = !migration_applied(conn, MIGRATION)?;
    if backfill {
        // A database from before presences were fed: its session re-stamp
        // triggers hold the names the presence kind's own triggers take
        // below, and fire on the backfill's stamp. Both sets are created
        // afresh after the backfill.
        for trigger in RETIRED_PRESENCE_TRIGGERS {
            conn.execute_batch(&format!("DROP TRIGGER IF EXISTS {trigger};"))?;
        }
    }
    for kind in ChangeKind::ALL {
        let table = kind.table();
        ensure_columns(
            conn,
            table.name,
            &[(REVISION_COLUMN, "INTEGER NOT NULL DEFAULT 0")],
        )?;
        // Rows written before their table was fed are stamped once, in rowid
        // order, each above everything stamped before it. This is deliberately
        // not gated only by the fixed `change_feed_v2` marker: a later release
        // can add a ChangeKind to an already-v2 store. Existing fed tables have
        // no zero revisions, so reopening them is a no-op; a newly fed table's
        // existing rows become visible above the old head without rotating the
        // origin. A revision-only UPDATE does not fire the feed update trigger.
        conn.execute(
            &format!(
                "UPDATE {name} SET {REVISION_COLUMN} = rowid + \
                 (SELECT version FROM observation_clock WHERE singleton = 1) \
                 WHERE {REVISION_COLUMN} = 0",
                name = table.name
            ),
            [],
        )?;
        conn.execute(
            &format!(
                "UPDATE observation_clock SET version = MAX(version, \
                 COALESCE((SELECT MAX({REVISION_COLUMN}) FROM {name}), 0)) \
                 WHERE singleton = 1",
                name = table.name
            ),
            [],
        )?;
        conn.execute(
            &format!(
                "CREATE INDEX IF NOT EXISTS {index} ON {name}({REVISION_COLUMN})",
                index = table.revision_index(),
                name = table.name
            ),
            [],
        )?;
        let [insert, update, delete] = trigger_names(*kind);
        let name = table.name;
        let kind = kind.as_str();
        let new_source = table.source_sql("NEW");
        let old_source = table.source_sql("OLD");
        let new_session = table.tombstone_session_sql("NEW");
        let old_session = table.tombstone_session_sql("OLD");
        let new_record = table.record_key_sql("NEW");
        let old_record = table.record_key_sql("OLD");
        let moved = table.identity_changed_sql();
        // The update trigger's own stamp changes `revision`, and only that,
        // so `NEW.revision = OLD.revision` is what stops it re-firing under
        // `recursive_triggers`; the rest of the guard is what makes an
        // external write that leaves the stamp alone (every upsert in this
        // crate) take a new one only when it changed a stamped column. See
        // `update_guard`. A change of identity is a delete of the old one and
        // an upsert of the new, each at its own revision.
        //
        // The guard is generated from the live column list, so a trigger
        // built before a column was added -- or before the guard existed --
        // is rebuilt here rather than kept by `IF NOT EXISTS`.
        let guard = update_guard(conn, name)?;
        if !update_trigger_is_current(conn, &update, &guard)? {
            conn.execute_batch(&format!("DROP TRIGGER IF EXISTS {update};"))?;
        }
        conn.execute_batch(&format!(
            "CREATE TRIGGER IF NOT EXISTS {insert} AFTER INSERT ON {name} BEGIN
                 UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
                 UPDATE {name} SET {REVISION_COLUMN} = \
                     (SELECT version FROM observation_clock WHERE singleton = 1) \
                     WHERE rowid = NEW.rowid;
                 DELETE FROM evidence_tombstones WHERE kind = '{kind}' \
                     AND source = {new_source} AND session_id = {new_session} \
                     AND record_key = {new_record};
             END;
             CREATE TRIGGER IF NOT EXISTS {update} AFTER UPDATE ON {name}
             {guard} BEGIN
                 UPDATE observation_clock SET version = version + 1 WHERE singleton = 1 \
                     AND ({moved});
                 INSERT OR REPLACE INTO evidence_tombstones \
                     (kind, source, session_id, record_key, revision) \
                     SELECT '{kind}', {old_source}, {old_session}, {old_record}, \
                         (SELECT version FROM observation_clock WHERE singleton = 1) \
                     WHERE {moved};
                 UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
                 UPDATE {name} SET {REVISION_COLUMN} = \
                     (SELECT version FROM observation_clock WHERE singleton = 1) \
                     WHERE rowid = NEW.rowid;
                 DELETE FROM evidence_tombstones WHERE kind = '{kind}' \
                     AND source = {new_source} AND session_id = {new_session} \
                     AND record_key = {new_record};
             END;
             CREATE TRIGGER IF NOT EXISTS {delete} AFTER DELETE ON {name} BEGIN
                 UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
                 INSERT OR REPLACE INTO evidence_tombstones \
                     (kind, source, session_id, record_key, revision) \
                     VALUES ('{kind}', {old_source}, {old_session}, {old_record}, \
                         (SELECT version FROM observation_clock WHERE singleton = 1));
             END;"
        ))?;
    }
    // A direct write of `revision` does not re-fire the sessions update
    // trigger (its guard requires `NEW.revision = OLD.revision`), so this is
    // one stamp, not two. The update trigger carries the same guard, so a
    // presence's own stamp does not re-stamp its session a second time, and
    // it fires only when the presence's identity moves (see
    // `PRESENCE_MOVED_GUARD`): a key change is a presence leaving one session
    // or location and arriving at another, and both sessions are stamped.
    let stamp_session = |row: &str| {
        format!(
            "UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
             UPDATE sessions SET {REVISION_COLUMN} = \
                 (SELECT version FROM observation_clock WHERE singleton = 1) \
                 WHERE source = {row}.source AND session_id = {row}.session_id;"
        )
    };
    let stamp_new = stamp_session("NEW");
    let stamp_old = stamp_session("OLD");
    let [insert, update, delete] = PRESENCE_TRIGGERS else {
        unreachable!("three presence triggers")
    };
    if !update_trigger_is_current(conn, update, PRESENCE_MOVED_GUARD)? {
        conn.execute_batch(&format!("DROP TRIGGER IF EXISTS {update};"))?;
    }
    conn.execute_batch(&format!(
        "CREATE TRIGGER IF NOT EXISTS {insert} \
             AFTER INSERT ON session_presences BEGIN
             {stamp_new}
         END;
         CREATE TRIGGER IF NOT EXISTS {update} \
             AFTER UPDATE ON session_presences \
             {PRESENCE_MOVED_GUARD} BEGIN
             {stamp_new}
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1 \
                 AND (OLD.source IS NOT NEW.source OR OLD.session_id IS NOT NEW.session_id);
             UPDATE sessions SET {REVISION_COLUMN} = \
                 (SELECT version FROM observation_clock WHERE singleton = 1) \
                 WHERE source = OLD.source AND session_id = OLD.session_id \
                 AND (OLD.source IS NOT NEW.source OR OLD.session_id IS NOT NEW.session_id);
         END;
         CREATE TRIGGER IF NOT EXISTS {delete} \
             AFTER DELETE ON session_presences BEGIN
             {stamp_old}
         END;"
    ))?;
    conn.execute(
        "INSERT OR IGNORE INTO schema_migrations (name) VALUES (?)",
        [MIGRATION],
    )?;
    reconcile_export_schema(conn, identity_existed)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session_store::StoreOptions;
    use crate::store::open_db_readonly;
    use std::path::Path;

    fn store(dir: &Path) -> (SessionStore, Connection) {
        let db = dir.join("ai-history.db");
        let store = SessionStore::open(StoreOptions {
            db_path: Some(db.clone()),
            ..StoreOptions::default()
        })
        .unwrap();
        (store, open_db(&db).unwrap())
    }

    fn insert_event(conn: &Connection, session: &str, uid: &str, text: &str) {
        conn.execute(
            "INSERT INTO session_events \
             (source, session_id, message_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', ?, 'm1', 10, 'assistant', 'text', ?, ?) \
             ON CONFLICT(source, session_id, event_uid) DO UPDATE SET text = excluded.text",
            params![session, text, uid],
        )
        .unwrap();
    }

    fn drain(changes: Changes) -> Vec<Change> {
        changes.map(|change| change.unwrap()).collect()
    }

    fn all(store: &SessionStore) -> Vec<Change> {
        drain(
            store
                .changes_since(Watermark::START, ChangeQuery::default())
                .unwrap(),
        )
    }

    #[test]
    fn every_write_takes_a_new_revision_and_replays_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let (store, conn) = store(dir.path());
        assert_eq!(store.head_revision().unwrap().revision, 0);
        assert!(all(&store).is_empty());

        conn.execute(
            "INSERT INTO sessions (session_id, source, cwd) VALUES ('s1', 'claude', '/p')",
            [],
        )
        .unwrap();
        insert_event(&conn, "s1", "e1", "one");
        insert_event(&conn, "s1", "e2", "two");
        conn.execute(
            "INSERT INTO tool_calls (source, session_id, tool_use_id, name) \
             VALUES ('claude', 's1', 't1', 'Bash')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO file_edits (source, session_id, tool_use_id, file_path, tool_name) \
             VALUES ('claude', 's1', 't2', '/p/a.rs', 'Edit')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_markers (source, session_id, marker_uid, kind) \
             VALUES ('claude', 's1', 'mk1', 'compaction')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_relationships (source, parent_session_id, relationship_uid, \
             child_session_id, relationship, identity_status, evidence_kind, created_ms, \
             updated_ms) VALUES ('claude', 's1', 'r1', 's2', 'delegated', 'observed', \
             'sidecar', 1, 1)",
            [],
        )
        .unwrap();

        let changes = all(&store);
        let kinds: Vec<ChangeKind> = changes.iter().map(|change| change.kind).collect();
        assert_eq!(
            kinds,
            vec![
                ChangeKind::Session,
                ChangeKind::SessionEvent,
                ChangeKind::SessionEvent,
                ChangeKind::ToolCall,
                ChangeKind::FileEdit,
                ChangeKind::SessionMarker,
                ChangeKind::Relationship,
            ]
        );
        let revisions: Vec<u64> = changes.iter().map(|change| change.revision).collect();
        assert_eq!(
            revisions,
            vec![1, 2, 3, 4, 5, 6, 7],
            "one revision per write"
        );
        assert_eq!(store.head_revision().unwrap().revision, 7);
        let keys: Vec<&str> = changes
            .iter()
            .map(|change| change.record_key.as_str())
            .collect();
        assert_eq!(keys, vec!["s1", "e1", "e2", "t1", "t2", "mk1", "r1"]);
        assert!(changes
            .iter()
            .all(|change| change.source == Some(Source::Claude) && change.session_id == "s1"));
        match &changes[1].op {
            ChangeOp::Upsert(EvidenceRow::SessionEvent(event)) => {
                assert_eq!(event.text.as_deref(), Some("one"));
            }
            other => panic!("typed row expected: {other:?}"),
        }
        match &changes[0].op {
            ChangeOp::Upsert(EvidenceRow::Session(session)) => {
                assert_eq!(session.cwd.as_deref(), Some("/p"));
            }
            other => panic!("typed row expected: {other:?}"),
        }

        // An upsert of an existing key re-stamps it: the consumer sees a
        // replace at a new revision, and nothing at the old one.
        insert_event(&conn, "s1", "e1", "one again");
        let after = drain(
            store
                .changes_since(
                    Watermark {
                        revision: 7,
                        ..store.head_revision().unwrap()
                    },
                    ChangeQuery::default(),
                )
                .unwrap(),
        );
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].record_key, "e1");
        assert_eq!(after[0].revision, 8);
        assert!(
            all(&store)
                .iter()
                .filter(|change| change.record_key == "e1")
                .count()
                == 1,
            "a re-stamped row appears once, at its newest revision"
        );
    }

    #[test]
    fn a_delete_leaves_a_tombstone_and_a_reinsert_clears_it() {
        let dir = tempfile::tempdir().unwrap();
        let (store, conn) = store(dir.path());
        insert_event(&conn, "s1", "e1", "one");
        conn.execute("DELETE FROM session_events WHERE event_uid = 'e1'", [])
            .unwrap();
        let changes = all(&store);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].op, ChangeOp::Delete);
        assert_eq!(changes[0].kind, ChangeKind::SessionEvent);
        assert_eq!(changes[0].record_key, "e1");
        assert_eq!(changes[0].revision, 2);

        // The row comes back: the tombstone is gone, the upsert is newer.
        insert_event(&conn, "s1", "e1", "back");
        let changes = all(&store);
        assert_eq!(changes.len(), 1);
        assert!(matches!(changes[0].op, ChangeOp::Upsert(_)));
        assert_eq!(changes[0].revision, 3);

        // `INSERT OR REPLACE` under recursive triggers is a delete and an
        // insert; the feed nets it to one upsert.
        conn.execute(
            "INSERT OR REPLACE INTO session_events \
             (source, session_id, message_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 's1', 'm1', 10, 'assistant', 'text', 'replaced', 'e1')",
            [],
        )
        .unwrap();
        let changes = all(&store);
        assert_eq!(changes.len(), 1);
        assert!(matches!(changes[0].op, ChangeOp::Upsert(_)));
        let tombstones: i64 = conn
            .query_row("SELECT COUNT(*) FROM evidence_tombstones", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(tombstones, 0);
    }

    /// One row of every fed kind, with a value in most columns.
    const EVERY_KIND_FIXTURE: &str = r#"
INSERT INTO sessions (session_id, source, cwd, models_json, workspace_roots_json)
    VALUES ('s1', 'claude', '/p', '["opus"]', 'not json');
INSERT INTO sessions (session_id, source) VALUES ('s2', 'claude');
INSERT INTO session_events (source, session_id, message_id, ts_ms, role, kind, text, event_uid,
    token_json, project_key_method, raw_facts_version)
    VALUES ('claude', 's1', 'm1', 10, 'assistant', 'text', 'one', 'e1', '{"input":3}', 'git', 2);
INSERT INTO tool_calls (source, session_id, tool_use_id, name, args_json, is_error)
    VALUES ('claude', 's1', 't1', 'Bash', '{"command":"ls"}', 0);
INSERT INTO file_edits (source, session_id, tool_use_id, file_path, tool_name, lines_added)
    VALUES ('claude', 's1', 't2', '/p/a.rs', 'Edit', 3);
INSERT INTO session_markers (source, session_id, marker_uid, kind, payload_json)
    VALUES ('claude', 's1', 'mk1', 'compaction', '{"trigger":"auto"}');
INSERT INTO session_relationships (source, parent_session_id, relationship_uid,
    child_session_id, relationship, identity_status, evidence_kind, child_has_events,
    created_ms, updated_ms)
    VALUES ('claude', 's1', 'r1', 's2', 'delegated', 'observed', 'sidecar', 1, 1, 2);
INSERT INTO history (source, session_id, project, prompt, timestamp_ms)
    VALUES ('claude', 's1', '/p', 'hello', 1000);
INSERT INTO history (source, session_id, prompt, timestamp_ms)
    VALUES ('codex', NULL, 'no session yet', 2000);
INSERT INTO session_presences (source, session_id, location, raw_locator)
    VALUES ('claude', 's1', 'local', '/p/s1.jsonl');
INSERT INTO session_commit_links (source, session_id, repo, commit_sha, match_method,
    confidence, files_json, created_at_ms)
    VALUES ('claude', 's1', 'repo', 'abc123', 'trailer', 0.75, '["a.rs"]', 1);
INSERT INTO trajectories (id, version, status, decisions_json, retrospective_json,
    search_text, updated_ms, timestamp_ms)
    VALUES ('traj-1', 1, 'active', '[]', '{}', 'x', 1, 1);
INSERT INTO session_observations (source, session_id, location, connector_id,
    connector_instance, updated_ms)
    VALUES ('claude', 's1', 'remote', 'conn', 'default', 1);
INSERT INTO observation_evidence (source, session_id, location, connector_id,
    connector_instance, evidence_uid, payload_json)
    VALUES ('claude', 's1', 'remote', 'conn', 'default', 'ev1', '{"a":1}');
"#;

    fn tombstone_count(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM evidence_tombstones", [], |row| {
            row.get(0)
        })
        .unwrap()
    }

    /// An upsert that rewrites a row with the values it already holds -- what
    /// every re-parse does to the rows it re-reads -- takes no revision and
    /// reports nothing; one that changes any column, a NULL becoming a value
    /// or a value becoming NULL included, takes exactly one.
    #[test]
    fn an_update_that_changes_nothing_takes_no_revision() {
        let dir = tempfile::tempdir().unwrap();
        let (store, conn) = store(dir.path());
        insert_event(&conn, "s1", "e1", "one");
        let head = store.head_revision().unwrap();

        insert_event(&conn, "s1", "e1", "one");
        conn.execute(
            "UPDATE session_events SET text = text, model = model WHERE event_uid = 'e1'",
            [],
        )
        .unwrap();
        assert_eq!(
            store.head_revision().unwrap(),
            head,
            "no change, no revision"
        );
        assert!(drain(store.changes_since(head, ChangeQuery::default()).unwrap()).is_empty());

        let changed = |sql: &str| {
            let before = store.head_revision().unwrap();
            conn.execute(sql, []).unwrap();
            let delta = drain(store.changes_since(before, ChangeQuery::default()).unwrap());
            assert_eq!(delta.len(), 1, "{sql}: {delta:?}");
            assert!(matches!(delta[0].op, ChangeOp::Upsert(_)), "{sql}");
            assert_eq!(delta[0].record_key, "e1", "{sql}");
            delta[0].columns.clone().unwrap()
        };
        let row = changed("UPDATE session_events SET text = 'two' WHERE event_uid = 'e1'");
        assert_eq!(row.get("text"), Some(&Value::from("two")));
        let row = changed("UPDATE session_events SET model = 'opus' WHERE event_uid = 'e1'");
        assert_eq!(
            row.get("model"),
            Some(&Value::from("opus")),
            "NULL to a value"
        );
        let row = changed("UPDATE session_events SET model = NULL WHERE event_uid = 'e1'");
        assert_eq!(row.get("model"), Some(&Value::Null), "a value to NULL");
        // The comparison is on the value as stored: `'7'` bound to an
        // INTEGER column is stored as 7, so writing 7 over it is no change.
        changed("UPDATE session_events SET payload_bytes = '7' WHERE event_uid = 'e1'");
        let head = store.head_revision().unwrap();
        conn.execute(
            "UPDATE session_events SET payload_bytes = 7 WHERE event_uid = 'e1'",
            [],
        )
        .unwrap();
        assert_eq!(
            store.head_revision().unwrap(),
            head,
            "'7' stored in an INTEGER column is 7: the same value"
        );

        // A no-op leaves tombstones alone; a change of identity still
        // tombstones the old key and reports the new one.
        assert_eq!(tombstone_count(&conn), 0);
        let head = store.head_revision().unwrap();
        conn.execute(
            "UPDATE session_events SET event_uid = event_uid WHERE event_uid = 'e1'",
            [],
        )
        .unwrap();
        assert_eq!(store.head_revision().unwrap(), head);
        assert_eq!(tombstone_count(&conn), 0);
        conn.execute(
            "UPDATE session_events SET event_uid = 'e2' WHERE event_uid = 'e1'",
            [],
        )
        .unwrap();
        let delta = drain(store.changes_since(head, ChangeQuery::default()).unwrap());
        assert_eq!(delta.len(), 2, "{delta:?}");
        assert_eq!(delta[0].op, ChangeOp::Delete);
        assert_eq!(delta[0].record_key, "e1");
        assert!(matches!(delta[1].op, ChangeOp::Upsert(_)));
        assert_eq!(delta[1].record_key, "e2");
    }

    /// Every fed table, not only the events: a rewrite of each row with its
    /// own values reports nothing, and the presence a re-read re-marks does
    /// not re-report its session either.
    #[test]
    fn no_fed_table_restamps_a_row_rewritten_with_its_own_values() {
        let dir = tempfile::tempdir().unwrap();
        let (store, conn) = store(dir.path());
        conn.execute_batch(EVERY_KIND_FIXTURE).unwrap();
        let head = store.head_revision().unwrap();
        for kind in ChangeKind::ALL {
            let table = kind.table().name;
            let columns = stamped_columns(&conn, table).unwrap();
            let rewrite = columns
                .iter()
                .map(|column| format!("{column} = {column}"))
                .collect::<Vec<_>>()
                .join(", ");
            let written = conn
                .execute(&format!("UPDATE {table} SET {rewrite}"), [])
                .unwrap();
            assert!(written > 0, "{table} holds a row to rewrite");
        }
        assert_eq!(
            store.head_revision().unwrap(),
            head,
            "{:?}",
            drain(store.changes_since(head, ChangeQuery::default()).unwrap())
        );

        // A presence updated in place keeps the session's `locations`; it is
        // reported as a presence, and the session row is not re-reported.
        conn.execute(
            "UPDATE session_presences SET source_stamp = 'moved' WHERE session_id = 's1'",
            [],
        )
        .unwrap();
        let delta = drain(store.changes_since(head, ChangeQuery::default()).unwrap());
        assert_eq!(
            delta.iter().map(|change| change.kind).collect::<Vec<_>>(),
            vec![ChangeKind::Presence],
            "{delta:?}"
        );
        // A presence moving to another location is a change to the row the
        // session reports, and re-reports it.
        let head = store.head_revision().unwrap();
        conn.execute(
            "UPDATE session_presences SET location = 'remote' WHERE session_id = 's1'",
            [],
        )
        .unwrap();
        let delta = drain(store.changes_since(head, ChangeQuery::default()).unwrap());
        assert!(
            delta.iter().any(|change| change.kind == ChangeKind::Session
                && matches!(&change.op, ChangeOp::Upsert(EvidenceRow::Session(session))
                    if session.locations == vec!["remote".to_string()])),
            "{delta:?}"
        );
    }

    /// The update guard names the table's columns as they were when it was
    /// built. A column a later migration adds must be guarded too, or an
    /// update that changes only that column would go unreported: the schema
    /// is not current until the trigger is rebuilt, and the next writable
    /// open rebuilds it. A trigger from before the guard existed is rebuilt
    /// the same way.
    #[test]
    fn a_column_added_after_the_trigger_is_guarded_after_the_next_open() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("ai-history.db");
        let (store, conn) = store(dir.path());
        insert_event(&conn, "s1", "e1", "one");
        assert!(schema_is_current(&conn).unwrap());
        let [_, update, _] = trigger_names(ChangeKind::SessionEvent);
        let built = trigger_sql(&conn, &update).unwrap().unwrap();
        assert!(!built.contains("added_later"));

        crate::store::ensure_columns(&conn, "session_events", &[("added_later", "TEXT")]).unwrap();
        assert!(
            !schema_is_current(&conn).unwrap(),
            "a guard that does not name every column is not current"
        );
        drop(conn);
        let conn = open_db(&db).unwrap();
        assert!(schema_is_current(&conn).unwrap());
        let rebuilt = trigger_sql(&conn, &update).unwrap().unwrap();
        assert!(
            rebuilt.contains("NEW.added_later IS NOT OLD.added_later"),
            "{rebuilt}"
        );
        // Only the guard moved: the insert and delete triggers, and every
        // other kind's update trigger, are the ones that were there.
        let head = store.head_revision().unwrap();
        conn.execute(
            "UPDATE session_events SET added_later = 'x' WHERE event_uid = 'e1'",
            [],
        )
        .unwrap();
        let delta = drain(store.changes_since(head, ChangeQuery::default()).unwrap());
        assert_eq!(delta.len(), 1, "{delta:?}");
        assert_eq!(
            delta[0].columns.as_ref().unwrap().get("added_later"),
            Some(&Value::from("x"))
        );

        // A database whose update triggers predate the guard: the old body
        // re-stamped every update. The open replaces it.
        conn.execute_batch(&format!(
            "DROP TRIGGER {update};
             CREATE TRIGGER {update} AFTER UPDATE ON session_events
             WHEN NEW.revision = OLD.revision BEGIN
                 UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
                 UPDATE session_events SET revision = \
                     (SELECT version FROM observation_clock WHERE singleton = 1) \
                     WHERE rowid = NEW.rowid;
             END;"
        ))
        .unwrap();
        assert!(!schema_is_current(&conn).unwrap());
        drop(conn);
        let conn = open_db(&db).unwrap();
        assert!(schema_is_current(&conn).unwrap());
        let head = store.head_revision().unwrap();
        insert_event(&conn, "s1", "e1", "one");
        assert_eq!(store.head_revision().unwrap(), head);
    }

    /// A column the migration pass drops must first leave the guard, which
    /// SQLite would otherwise refuse; the pass's last step puts the guard
    /// back over the columns that remain.
    #[test]
    fn a_released_guard_lets_a_column_go_and_comes_back_on_open() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("ai-history.db");
        let (_store, conn) = store(dir.path());
        crate::store::ensure_columns(&conn, "session_markers", &[("doomed", "TEXT")]).unwrap();
        drop(conn);
        let conn = open_db(&db).unwrap();
        let [_, update, _] = trigger_names(ChangeKind::SessionMarker);
        assert!(trigger_sql(&conn, &update)
            .unwrap()
            .unwrap()
            .contains("NEW.doomed"));
        assert!(conn
            .execute("ALTER TABLE session_markers DROP COLUMN doomed", [])
            .is_err());
        release_update_guard(&conn, "session_markers").unwrap();
        conn.execute("ALTER TABLE session_markers DROP COLUMN doomed", [])
            .unwrap();
        drop(conn);
        let conn = open_db(&db).unwrap();
        assert!(schema_is_current(&conn).unwrap());
        assert!(!trigger_sql(&conn, &update)
            .unwrap()
            .unwrap()
            .contains("doomed"));
    }

    #[test]
    fn deleting_a_session_tombstones_what_the_cascade_removes() {
        let dir = tempfile::tempdir().unwrap();
        let (store, conn) = store(dir.path());
        conn.execute(
            "INSERT INTO sessions (session_id, source) VALUES ('s1', 'claude')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_markers (source, session_id, marker_uid, kind) \
             VALUES ('claude', 's1', 'mk1', 'compaction')",
            [],
        )
        .unwrap();
        let head = store.head_revision().unwrap();
        conn.execute("DELETE FROM sessions WHERE session_id = 's1'", [])
            .unwrap();
        let changes = drain(store.changes_since(head, ChangeQuery::default()).unwrap());
        let mut deleted: Vec<(ChangeKind, &str)> = changes
            .iter()
            .filter(|change| change.op == ChangeOp::Delete)
            .map(|change| (change.kind, change.record_key.as_str()))
            .collect();
        deleted.sort();
        assert_eq!(
            deleted,
            vec![
                (ChangeKind::Session, "s1"),
                (ChangeKind::SessionMarker, "mk1")
            ]
        );
    }

    #[test]
    fn named_consumers_advance_independently_and_only_on_commit() {
        let dir = tempfile::tempdir().unwrap();
        let (store, conn) = store(dir.path());
        for index in 0..5 {
            insert_event(&conn, "s1", &format!("e{index}"), "x");
        }
        let query = |name: &str| ChangeQuery {
            consumer: Some(name.to_string()),
            ..ChangeQuery::default()
        };

        // A drain that is never committed moves nothing.
        let mut uncommitted = store
            .changes_since(Watermark::CONSUMER, query("a"))
            .unwrap();
        uncommitted.next().unwrap().unwrap();
        uncommitted.next().unwrap().unwrap();
        assert_eq!(uncommitted.position().revision, 2);
        drop(uncommitted);
        let again = store
            .changes_since(Watermark::CONSUMER, query("a"))
            .unwrap();
        assert_eq!(again.position().revision, 0);
        assert_eq!(drain(again).len(), 5);

        // Consumer a commits partway; consumer b is untouched by it.
        let mut partial = store
            .changes_since(Watermark::CONSUMER, query("a"))
            .unwrap();
        for _ in 0..3 {
            partial.next().unwrap().unwrap();
        }
        assert_eq!(partial.commit().unwrap().revision, 3);
        // Committing again at the same position is a no-op, not an error.
        assert_eq!(partial.commit().unwrap().revision, 3);
        let rest = store
            .changes_since(Watermark::CONSUMER, query("a"))
            .unwrap();
        assert_eq!(rest.position().revision, 3);
        let rest = drain(rest);
        assert_eq!(rest.len(), 2);
        assert_eq!(rest[0].record_key, "e3");

        let b = store
            .changes_since(Watermark::CONSUMER, query("b"))
            .unwrap();
        assert_eq!(b.position().revision, 0);
        let b_all = drain(b);
        assert_eq!(b_all.len(), 5);

        // A fully drained consumer commits the head, and a fresh drain from
        // it is empty until something is written.
        let full = store
            .changes_since(Watermark::CONSUMER, query("a"))
            .unwrap();
        let head = full.head();
        let full_changes: Vec<_> = full.collect();
        assert_eq!(full_changes.len(), 2);
        // `collect` consumed the iterator, so re-open to commit at the head.
        let done = store.changes_since(head, query("a")).unwrap();
        assert!(drain(done).is_empty());
        let mut done = store.changes_since(head, query("a")).unwrap();
        assert!(done.next().is_none());
        assert_eq!(done.position(), head);
        done.commit().unwrap();
        let stored: i64 = conn
            .query_row(
                "SELECT revision FROM consumer_cursors WHERE name = 'a'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored as u64, head.revision);

        // Commit without a consumer is refused rather than silently dropped.
        let anonymous = store
            .changes_since(Watermark::START, ChangeQuery::default())
            .unwrap();
        assert!(anonymous.commit().is_err());
        assert!(store
            .changes_since(Watermark::CONSUMER, ChangeQuery::default())
            .is_err());
    }

    /// The cursor only moves forward. A drain opened earlier that commits
    /// after a newer one, or an explicit replay from an old watermark under
    /// a name that has moved past it, cannot rewind the consumer.
    #[test]
    fn a_stale_commit_does_not_rewind_the_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let (store, conn) = store(dir.path());
        for index in 0..5 {
            insert_event(&conn, "s1", &format!("e{index}"), "x");
        }
        let query = || ChangeQuery::default().consumer("c");

        // Two drains open against the same cursor; the older one commits
        // last, at a lower position.
        let mut older = store.changes_since(Watermark::CONSUMER, query()).unwrap();
        let mut newer = store.changes_since(Watermark::CONSUMER, query()).unwrap();
        older.next().unwrap().unwrap();
        older.next().unwrap().unwrap();
        while newer.next().is_some() {}
        assert_eq!(newer.commit().unwrap().revision, 5);
        assert_eq!(older.position().revision, 2);
        assert_eq!(
            older.commit().unwrap().revision,
            5,
            "a stale commit reports the cursor as stored, not what it offered"
        );
        let stored: i64 = conn
            .query_row(
                "SELECT revision FROM consumer_cursors WHERE name = 'c'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored, 5);
        assert!(drain(store.changes_since(Watermark::CONSUMER, query()).unwrap()).is_empty());

        // An explicit replay from START under the same name, committed
        // partway, leaves the cursor alone as well.
        let mut replay = store.changes_since(Watermark::START, query()).unwrap();
        replay.next().unwrap().unwrap();
        assert_eq!(replay.position().revision, 1);
        assert_eq!(replay.commit().unwrap().revision, 5);
        assert!(drain(store.changes_since(Watermark::CONSUMER, query()).unwrap()).is_empty());

        // And a genuinely newer commit still moves it.
        insert_event(&conn, "s1", "e5", "x");
        let mut next = store.changes_since(Watermark::CONSUMER, query()).unwrap();
        assert_eq!(next.position().revision, 5);
        while next.next().is_some() {}
        assert_eq!(next.commit().unwrap().revision, 6);
    }

    /// `locations` on a catalog row is derived from `session_presences`, so
    /// a presence leaving or arriving is a change to the row the consumer
    /// holds even though `sessions` itself was not written.
    #[test]
    fn a_presence_change_re_reports_the_session_row() {
        let dir = tempfile::tempdir().unwrap();
        let (store, conn) = store(dir.path());
        conn.execute(
            "INSERT INTO sessions (session_id, source) VALUES ('s1', 'claude')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO session_presences (source, session_id, location) \
             VALUES ('claude', 's1', 'local'), ('claude', 's1', 'remote')",
            [],
        )
        .unwrap();
        // The catalog row alone: the presence rows are a kind of their own.
        let query = || {
            ChangeQuery::default()
                .kinds([ChangeKind::Session])
                .consumer("c")
        };
        let mut seen = store.changes_since(Watermark::CONSUMER, query()).unwrap();
        let mut last = None;
        for change in seen.by_ref() {
            last = Some(change.unwrap());
        }
        match last.map(|change| change.op) {
            Some(ChangeOp::Upsert(EvidenceRow::Session(session))) => {
                assert_eq!(session.locations, vec!["local", "remote"]);
            }
            other => panic!("the last change is the row with both presences: {other:?}"),
        }
        seen.commit().unwrap();

        // The local presence goes, the session row and the remote presence
        // stay -- what a subagent cleanup does.
        conn.execute(
            "DELETE FROM session_presences WHERE source = 'claude' AND session_id = 's1' \
             AND location = 'local'",
            [],
        )
        .unwrap();
        let delta = drain(store.changes_since(Watermark::CONSUMER, query()).unwrap());
        assert_eq!(delta.len(), 1, "{delta:?}");
        assert_eq!(delta[0].kind, ChangeKind::Session);
        assert_eq!(delta[0].record_key, "s1");
        match &delta[0].op {
            ChangeOp::Upsert(EvidenceRow::Session(session)) => {
                assert_eq!(session.locations, vec!["remote"]);
            }
            other => panic!("a replacement row, not a tombstone: {other:?}"),
        }

        // A presence arriving is reported the same way.
        let head = store.head_revision().unwrap();
        conn.execute(
            "INSERT INTO session_presences (source, session_id, location) \
             VALUES ('claude', 's1', 'local')",
            [],
        )
        .unwrap();
        let delta = drain(
            store
                .changes_since(head, ChangeQuery::default().kinds([ChangeKind::Session]))
                .unwrap(),
        );
        assert_eq!(delta.len(), 1, "{delta:?}");
        match &delta[0].op {
            ChangeOp::Upsert(EvidenceRow::Session(session)) => {
                assert_eq!(session.locations, vec!["local", "remote"]);
            }
            other => panic!("{other:?}"),
        }
    }

    /// Every database counts its revisions from zero, so a replacement that
    /// has since counted past a consumer's persisted watermark would pass the
    /// ahead-of-head check, resume after it, and skip everything the
    /// replacement wrote below it. The epoch is what refuses it.
    #[test]
    fn a_watermark_from_a_replaced_store_is_refused_even_below_its_head() {
        let dir = tempfile::tempdir().unwrap();
        let (replaced, conn) = store(dir.path());
        for index in 0..3 {
            insert_event(&conn, "s1", &format!("old{index}"), "x");
        }
        let mut drained = replaced
            .changes_since(Watermark::START, ChangeQuery::default())
            .unwrap();
        while drained.next().is_some() {}
        // Persisted the way a consumer keeps it, outside the store.
        let persisted: Watermark =
            serde_json::from_str(&serde_json::to_string(&drained.position()).unwrap()).unwrap();
        assert_eq!(persisted.revision, 3);
        drop(drained);
        drop(conn);
        drop(replaced);

        // The database is deleted and rebuilt at the same path, and the
        // rebuild writes more than the old one had.
        for suffix in ["", "-wal", "-shm"] {
            let path = dir.path().join(format!("ai-history.db{suffix}"));
            if path.exists() {
                std::fs::remove_file(path).unwrap();
            }
        }
        let (replacement, conn) = store(dir.path());
        for index in 0..5 {
            insert_event(&conn, "s1", &format!("new{index}"), "x");
        }
        let head = replacement.head_revision().unwrap();
        assert_eq!(head.revision, 5);
        assert_ne!(head.epoch, persisted.epoch);

        let error = replacement
            .changes_since(persisted, ChangeQuery::default())
            .expect_err("a watermark from the replaced database cannot be resumed from");
        assert!(
            matches!(error, Error::WatermarkAheadOfStore(_)),
            "{error:?}"
        );
        assert!(error.to_string().contains("Watermark::START"), "{error}");
        // So is one that names no store at all.
        let unbound = Watermark {
            revision: 3,
            epoch: 0,
        };
        assert!(matches!(
            replacement
                .changes_since(unbound, ChangeQuery::default())
                .expect_err("a bare revision names no store"),
            Error::WatermarkAheadOfStore(_)
        ));
        // The recovery reads the replacement whole, and its own watermark
        // resumes.
        assert_eq!(all(&replacement).len(), 5);
        let mid = Watermark {
            revision: 3,
            ..head
        };
        let rest = drain(
            replacement
                .changes_since(mid, ChangeQuery::default())
                .unwrap(),
        );
        assert_eq!(
            rest.iter()
                .map(|change| change.record_key.as_str())
                .collect::<Vec<_>>(),
            vec!["new3", "new4"]
        );
    }

    /// Adding a stored column changes an exported record's canonical JSON
    /// without touching the row or its revision. The migration pass restamps
    /// only that kind above the old head: external watermarks and named
    /// cursors resume normally, while unrelated kinds are never replayed.
    #[test]
    fn an_exported_column_change_restamps_only_the_affected_kind() {
        let dir = tempfile::tempdir().unwrap();
        let (initial_store, conn) = store(dir.path());
        insert_event(&conn, "s1", "e1", "before");
        conn.execute(
            "INSERT INTO history (source, session_id, prompt, timestamp_ms) \
             VALUES ('claude', 's1', 'unrelated', 1)",
            [],
        )
        .unwrap();
        let old_head = initial_store.head_revision().unwrap();
        let query = || ChangeQuery::default().consumer("delivery");
        let history_query = || {
            ChangeQuery::default()
                .kinds([ChangeKind::History])
                .consumer("history-only")
        };
        let mut first = initial_store
            .changes_since(Watermark::CONSUMER, query())
            .unwrap();
        while first.next().is_some() {}
        assert_eq!(first.commit().unwrap(), old_head);
        let mut history = initial_store
            .changes_since(Watermark::CONSUMER, history_query())
            .unwrap();
        while history.next().is_some() {}
        assert_eq!(history.commit().unwrap(), old_head);

        let old_event_revision: i64 = conn
            .query_row(
                "SELECT revision FROM session_events WHERE event_uid='e1'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        // Stands in for an additive crate migration such as `location`: the
        // row now serializes differently, but no row write advanced it.
        conn.execute_batch(
            "ALTER TABLE session_events ADD COLUMN export_shape_probe TEXT NOT NULL DEFAULT 'local';",
        )
        .unwrap();
        let unchanged_revision: i64 = conn
            .query_row(
                "SELECT revision FROM session_events WHERE event_uid='e1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(unchanged_revision, old_event_revision);
        drop(first);
        drop(conn);
        drop(initial_store);

        let (migrated, conn) = store(dir.path());
        let new_head = migrated.head_revision().unwrap();
        assert_eq!(new_head.revision, old_head.revision + 1);
        assert_eq!(new_head.epoch, old_head.epoch);
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM consumer_cursors", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            2,
            "schema reconciliation preserves named cursors"
        );
        let external = drain(
            migrated
                .changes_since(old_head, ChangeQuery::default())
                .unwrap(),
        );
        assert_eq!(external.len(), 1);
        assert_eq!(external[0].kind, ChangeKind::SessionEvent);
        let replay = drain(
            migrated
                .changes_since(Watermark::CONSUMER, query())
                .unwrap(),
        );
        assert_eq!(replay.len(), 1);
        assert_eq!(replay[0].revision, new_head.revision);
        assert_eq!(
            replay[0]
                .columns
                .as_ref()
                .and_then(|columns| columns.get("export_shape_probe")),
            Some(&Value::from("local"))
        );
        assert!(drain(
            migrated
                .changes_since(Watermark::CONSUMER, history_query())
                .unwrap()
        )
        .is_empty());
    }

    /// The release before per-kind fingerprints already has a feed identity
    /// and delivered rows, but gains this nullable column during migration.
    /// Its first open must restamp every existing kind exactly once above the
    /// prior head without invalidating either external or named cursors.
    #[test]
    fn an_existing_feed_with_no_fingerprint_restamps_once() {
        let dir = tempfile::tempdir().unwrap();
        let (initial_store, conn) = store(dir.path());
        insert_event(&conn, "s1", "e1", "event");
        conn.execute(
            "INSERT INTO history (source, session_id, prompt, timestamp_ms) \
             VALUES ('claude', 's1', 'prompt', 1)",
            [],
        )
        .unwrap();
        let old_head = initial_store.head_revision().unwrap();
        let query = || ChangeQuery::default().consumer("delivery");
        let mut first = initial_store
            .changes_since(Watermark::CONSUMER, query())
            .unwrap();
        while first.next().is_some() {}
        assert_eq!(first.commit().unwrap(), old_head);

        conn.execute(
            &format!(
                "UPDATE change_feed_store SET {EXPORT_SCHEMA_DIGEST_COLUMN}=NULL \
                 WHERE singleton=1"
            ),
            [],
        )
        .unwrap();
        drop(first);
        drop(conn);
        drop(initial_store);

        let (migrated, conn) = store(dir.path());
        let new_head = migrated.head_revision().unwrap();
        assert_eq!(new_head.epoch, old_head.epoch);
        assert_eq!(new_head.revision, old_head.revision + 2);
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM consumer_cursors", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
        let resumed = drain(
            migrated
                .changes_since(Watermark::CONSUMER, query())
                .unwrap(),
        );
        assert_eq!(resumed.len(), 2);
        assert_eq!(
            resumed
                .iter()
                .map(|change| change.kind)
                .collect::<std::collections::BTreeSet<_>>(),
            std::collections::BTreeSet::from([ChangeKind::SessionEvent, ChangeKind::History])
        );
        drop(conn);
        drop(migrated);

        let (reopened, _) = store(dir.path());
        assert_eq!(reopened.head_revision().unwrap(), new_head);
    }

    #[test]
    fn export_schema_fingerprints_include_declared_column_types() {
        let text = vec![("value".to_string(), "TEXT".to_string())];
        let integer = vec![("value".to_string(), "INTEGER".to_string())];
        assert_ne!(
            export_schema_digest(&text).unwrap(),
            export_schema_digest(&integer).unwrap(),
            "an affinity change can change SQLite's exported JSON value type"
        );
    }

    /// A kind absent from a previously stored map is newly introduced, not a
    /// changed schema. Its feed migration already stamped its rows above the
    /// old head, so reconciliation records the new fingerprint without
    /// rotating the origin, clearing cursors, or replaying old rows.
    #[test]
    fn a_new_kind_fingerprint_does_not_replay_existing_history() {
        let dir = tempfile::tempdir().unwrap();
        let (initial_store, conn) = store(dir.path());
        insert_event(&conn, "s1", "e1", "before");
        let old_head = initial_store.head_revision().unwrap();
        let query = || ChangeQuery::default().consumer("delivery");
        let mut first = initial_store
            .changes_since(Watermark::CONSUMER, query())
            .unwrap();
        while first.next().is_some() {}
        first.commit().unwrap();

        let stored: String = conn
            .query_row(
                &format!(
                    "SELECT {EXPORT_SCHEMA_DIGEST_COLUMN} FROM change_feed_store WHERE singleton=1"
                ),
                [],
                |row| row.get(0),
            )
            .unwrap();
        let mut map: BTreeMap<String, String> = serde_json::from_str(&stored).unwrap();
        map.remove(ChangeKind::SessionEvent.as_str());
        conn.execute(
            &format!(
                "UPDATE change_feed_store SET {EXPORT_SCHEMA_DIGEST_COLUMN}=? WHERE singleton=1"
            ),
            [serde_json::to_string(&map).unwrap()],
        )
        .unwrap();
        init_schema(&conn).unwrap();
        assert_eq!(initial_store.head_revision().unwrap(), old_head);
        assert!(drain(
            initial_store
                .changes_since(Watermark::CONSUMER, query())
                .unwrap()
        )
        .is_empty());
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM consumer_cursors", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
    }

    #[test]
    fn a_removed_kind_fingerprint_resets_the_semantic_stream() {
        let dir = tempfile::tempdir().unwrap();
        let (initial_store, conn) = store(dir.path());
        insert_event(&conn, "s1", "e1", "before");
        let old_head = initial_store.head_revision().unwrap();
        let query = || ChangeQuery::default().consumer("delivery");
        let mut first = initial_store
            .changes_since(Watermark::CONSUMER, query())
            .unwrap();
        while first.next().is_some() {}
        first.commit().unwrap();

        let stored: String = conn
            .query_row(
                &format!(
                    "SELECT {EXPORT_SCHEMA_DIGEST_COLUMN} FROM change_feed_store WHERE singleton=1"
                ),
                [],
                |row| row.get(0),
            )
            .unwrap();
        let mut map: BTreeMap<String, String> = serde_json::from_str(&stored).unwrap();
        map.insert("retired_kind".to_string(), "retired-schema".to_string());
        assert!(exported_kind_removed(
            &map,
            &export_schema_digests(&conn).unwrap()
        ));
        conn.execute(
            &format!(
                "UPDATE change_feed_store SET {EXPORT_SCHEMA_DIGEST_COLUMN}=? WHERE singleton=1"
            ),
            [serde_json::to_string(&map).unwrap()],
        )
        .unwrap();
        init_schema(&conn).unwrap();
        assert_ne!(initial_store.head_revision().unwrap().epoch, old_head.epoch);
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM consumer_cursors", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
    }

    /// A commit opens the store's path afresh. When that path now holds
    /// another database, the drain's position is a revision number from a
    /// different history; written as the new database's cursor, it would
    /// make the next resume skip that database's rows below it.
    #[test]
    fn a_commit_into_a_replaced_database_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let (replaced, conn) = store(dir.path());
        for index in 0..3 {
            insert_event(&conn, "s1", &format!("old{index}"), "x");
        }
        let query = || ChangeQuery::default().consumer("c");
        let mut drained = replaced
            .changes_since(Watermark::CONSUMER, query())
            .unwrap();
        while drained.next().is_some() {}
        assert_eq!(drained.position().revision, 3);
        drop(conn);
        for suffix in ["", "-wal", "-shm"] {
            let path = dir.path().join(format!("ai-history.db{suffix}"));
            if path.exists() {
                std::fs::remove_file(path).unwrap();
            }
        }
        let (replacement, conn) = store(dir.path());
        for index in 0..5 {
            insert_event(&conn, "s1", &format!("new{index}"), "x");
        }

        let error = drained
            .commit()
            .expect_err("a position from the replaced database is not this one's");
        assert!(
            matches!(error, Error::WatermarkAheadOfStore(_)),
            "{error:?}"
        );
        let cursors: i64 = conn
            .query_row("SELECT COUNT(*) FROM consumer_cursors", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(cursors, 0, "nothing was written into the replacement");
        assert_eq!(
            drain(
                replacement
                    .changes_since(Watermark::CONSUMER, query())
                    .unwrap()
            )
            .len(),
            5
        );

        // The same database, with its clock behind the position -- what a
        // restore from an older copy leaves -- refuses the commit too.
        let mut ahead = replacement
            .changes_since(Watermark::CONSUMER, query())
            .unwrap();
        while ahead.next().is_some() {}
        assert_eq!(ahead.position().revision, 5);
        conn.execute("UPDATE observation_clock SET version = 2", [])
            .unwrap();
        assert!(matches!(
            ahead.commit().unwrap_err(),
            Error::WatermarkAheadOfStore(_)
        ));
    }

    #[test]
    fn a_watermark_ahead_of_the_store_is_a_named_error() {
        let dir = tempfile::tempdir().unwrap();
        let (store, conn) = store(dir.path());
        insert_event(&conn, "s1", "e1", "x");
        let error = store
            .changes_since(
                Watermark {
                    revision: 99,
                    ..store.head_revision().unwrap()
                },
                ChangeQuery::default(),
            )
            .expect_err("a watermark past the head cannot be resumed from");
        assert!(
            matches!(error, Error::WatermarkAheadOfStore(_)),
            "{error:?}"
        );
        assert!(error.to_string().contains("Watermark::START"), "{error}");

        // The same through a stored consumer cursor: the database was reset
        // under a consumer that kept its cursor elsewhere.
        conn.execute(
            "INSERT INTO consumer_cursors (name, revision, updated_ms) VALUES ('c', 99, 0)",
            [],
        )
        .unwrap();
        let error = store
            .changes_since(
                Watermark::CONSUMER,
                ChangeQuery {
                    consumer: Some("c".to_string()),
                    ..ChangeQuery::default()
                },
            )
            .err()
            .unwrap();
        assert!(
            matches!(error, Error::WatermarkAheadOfStore(_)),
            "{error:?}"
        );
        // Exactly at the head is fine: nothing new, no error.
        assert!(drain(
            store
                .changes_since(
                    Watermark {
                        revision: 1,
                        ..store.head_revision().unwrap()
                    },
                    ChangeQuery::default(),
                )
                .unwrap()
        )
        .is_empty());
    }

    /// A stored cursor past the head names no revision of this store. A
    /// resume refuses it and sends the consumer to `Watermark::START`, and
    /// the commit after that resync replaces it -- keeping the maximum would
    /// leave the name refused until the store happened to pass 99.
    #[test]
    fn a_resync_from_start_replaces_a_cursor_ahead_of_the_store() {
        let dir = tempfile::tempdir().unwrap();
        let (store, conn) = store(dir.path());
        for index in 0..5 {
            insert_event(&conn, "s1", &format!("e{index}"), "x");
        }
        conn.execute(
            "INSERT INTO consumer_cursors (name, revision, updated_ms) VALUES ('c', 99, 0)",
            [],
        )
        .unwrap();
        let query = || ChangeQuery::default().consumer("c");
        let error = store
            .changes_since(Watermark::CONSUMER, query())
            .expect_err("a cursor past the head cannot be resumed from");
        assert!(
            matches!(error, Error::WatermarkAheadOfStore(_)),
            "{error:?}"
        );

        // The resync, committed partway: the cursor is now where it reached.
        let mut resync = store.changes_since(Watermark::START, query()).unwrap();
        assert_eq!(resync.head().revision, 5);
        resync.next().unwrap().unwrap();
        resync.next().unwrap().unwrap();
        assert_eq!(resync.commit().unwrap().revision, 2);

        // From there the ordinary rules hold: the name resumes, and the
        // cursor only moves forward.
        let mut rest = store.changes_since(Watermark::CONSUMER, query()).unwrap();
        assert_eq!(rest.position().revision, 2);
        while rest.next().is_some() {}
        assert_eq!(rest.commit().unwrap().revision, 5);
        assert_eq!(
            resync.commit().unwrap().revision,
            5,
            "a stale commit after the replacement does not rewind it"
        );
        assert!(drain(store.changes_since(Watermark::CONSUMER, query()).unwrap()).is_empty());
    }

    /// Whether the resync replaces the stale cursor is decided against the
    /// head it was bounded to, not the store's head at commit. Writers can
    /// carry the clock past the stale value while the resync runs, and the
    /// cursor must still land where the resync reached, so the revisions it
    /// never covered are read next rather than skipped.
    #[test]
    fn a_resync_replaces_the_stale_cursor_after_the_store_grows_past_it() {
        let dir = tempfile::tempdir().unwrap();
        let (store, conn) = store(dir.path());
        for index in 0..5 {
            insert_event(&conn, "s1", &format!("e{index}"), "x");
        }
        conn.execute(
            "INSERT INTO consumer_cursors (name, revision, updated_ms) VALUES ('c', 7, 0)",
            [],
        )
        .unwrap();
        let query = || ChangeQuery::default().consumer("c");
        let mut resync = store.changes_since(Watermark::START, query()).unwrap();
        assert_eq!(resync.head().revision, 5);
        for index in 5..8 {
            insert_event(&conn, "s1", &format!("e{index}"), "x");
        }
        assert_eq!(store.head_revision().unwrap().revision, 8);
        while resync.next().is_some() {}
        assert_eq!(resync.commit().unwrap().revision, 5);

        let rest = drain(store.changes_since(Watermark::CONSUMER, query()).unwrap());
        assert_eq!(
            rest.iter()
                .map(|change| change.revision)
                .collect::<Vec<_>>(),
            vec![6, 7, 8]
        );
    }

    #[test]
    fn pages_are_bounded_and_the_drain_is_bounded_to_the_head_at_open() {
        let dir = tempfile::tempdir().unwrap();
        let (store, conn) = store(dir.path());
        for index in 0..7 {
            insert_event(&conn, "s1", &format!("e{index}"), "x");
        }
        let mut changes = store
            .changes_since(
                Watermark::START,
                ChangeQuery {
                    batch: 3,
                    ..ChangeQuery::default()
                },
            )
            .unwrap();
        assert_eq!(changes.head().revision, 7);
        let first = changes.next().unwrap().unwrap();
        assert_eq!(first.record_key, "e0");
        // Written after open: above the head, so not part of this drain.
        insert_event(&conn, "s1", "e7", "late");
        let rest = drain(changes);
        assert_eq!(rest.len(), 6);
        assert_eq!(rest.last().unwrap().record_key, "e6");
        // And a re-stamp past the head takes the row out of this drain too
        // (one-row pages, so every row is read after the re-stamp); the next
        // drain reports it at the newer revision.
        let mut changes = store
            .changes_since(
                Watermark::START,
                ChangeQuery {
                    batch: 1,
                    ..ChangeQuery::default()
                },
            )
            .unwrap();
        changes.next().unwrap().unwrap();
        insert_event(&conn, "s1", "e6", "moved");
        let rest = drain(changes);
        assert!(rest.iter().all(|change| change.record_key != "e6"));
        let newer = drain(
            store
                .changes_since(
                    Watermark {
                        revision: 8,
                        ..store.head_revision().unwrap()
                    },
                    ChangeQuery::default(),
                )
                .unwrap(),
        );
        assert_eq!(newer.len(), 1);
        assert_eq!(newer[0].record_key, "e6");

        // The batch is clamped, never unbounded.
        let clamped = store
            .changes_since(
                Watermark::START,
                ChangeQuery {
                    batch: usize::MAX,
                    ..ChangeQuery::default()
                },
            )
            .unwrap();
        assert_eq!(clamped.batch, MAX_CHANGE_BATCH);
    }

    #[test]
    fn kinds_filter_both_upserts_and_tombstones() {
        let dir = tempfile::tempdir().unwrap();
        let (store, conn) = store(dir.path());
        insert_event(&conn, "s1", "e1", "x");
        conn.execute(
            "INSERT INTO tool_calls (source, session_id, tool_use_id, name) \
             VALUES ('claude', 's1', 't1', 'Bash')",
            [],
        )
        .unwrap();
        conn.execute("DELETE FROM tool_calls WHERE tool_use_id = 't1'", [])
            .unwrap();
        conn.execute("DELETE FROM session_events WHERE event_uid = 'e1'", [])
            .unwrap();
        let only_events = drain(
            store
                .changes_since(
                    Watermark::START,
                    ChangeQuery {
                        kinds: Some(vec![ChangeKind::SessionEvent]),
                        ..ChangeQuery::default()
                    },
                )
                .unwrap(),
        );
        assert_eq!(only_events.len(), 1);
        assert_eq!(only_events[0].kind, ChangeKind::SessionEvent);
        assert_eq!(only_events[0].op, ChangeOp::Delete);
        let none = drain(
            store
                .changes_since(
                    Watermark::START,
                    ChangeQuery {
                        kinds: Some(vec![]),
                        ..ChangeQuery::default()
                    },
                )
                .unwrap(),
        );
        assert!(none.is_empty());
    }

    /// The page reads are indexed range scans: each plan searches the
    /// table's revision index and needs no temporary sort, on every kind and
    /// on the tombstones. This is the bounded-time guarantee, asserted on the
    /// plan rather than measured on a large store.
    #[test]
    fn page_reads_use_the_revision_index_without_a_scan_or_a_sort() {
        let dir = tempfile::tempdir().unwrap();
        let (_store, conn) = store(dir.path());
        let page: Vec<rusqlite::types::Value> = vec![0i64.into(), 1_000i64.into(), 100i64.into()];
        let mut plans = Vec::new();
        for kind in ChangeKind::ALL {
            let stored = stored_columns(&conn, kind.table().name).unwrap();
            plans.push((
                kind.table().name,
                upsert_sql(*kind, &stored, false),
                page.clone(),
            ));
        }
        let mut tombstone_page = vec![rusqlite::types::Value::from("session_event".to_string())];
        tombstone_page.extend(page.iter().cloned());
        plans.push((
            "evidence_tombstones",
            tombstone_sql(false),
            tombstone_page.clone(),
        ));
        // The revision-only first pass must be a covering read of the same
        // index, with no table access at all.
        for kind in ChangeKind::ALL {
            plans.push((
                kind.table().name,
                upsert_key_sql(*kind, false),
                page.clone(),
            ));
        }
        plans.push((
            "evidence_tombstones",
            tombstone_key_sql(false),
            tombstone_page,
        ));
        for (table, sql, values) in plans {
            let details: Vec<String> = conn
                .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
                .unwrap()
                .query_map(rusqlite::params_from_iter(values), |row| {
                    row.get::<_, String>(3)
                })
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
            let plan = details.join("\n");
            assert!(
                plan.contains(&format!("INDEX idx_{table}_")),
                "{table}: {plan}"
            );
            assert!(!plan.contains(&format!("SCAN {table}")), "{table}: {plan}");
            assert!(!plan.contains("TEMP B-TREE"), "{table}: {plan}");
            if sql.starts_with(&format!("SELECT {REVISION_COLUMN} FROM")) {
                assert!(plan.contains("COVERING INDEX"), "{table}: {plan}");
            }
        }
    }

    /// Restricted to one session, every page read seeks that session through
    /// its table's session index -- never the revision index, never a scan of
    /// the table -- and sorts only that session's rows.
    #[test]
    fn a_session_page_reads_the_session_index() {
        let dir = tempfile::tempdir().unwrap();
        let (_store, conn) = store(dir.path());
        let filter = SessionIdentity::new("claude", "s1");
        let mut plans = Vec::new();
        for kind in ChangeKind::ALL {
            let stored = stored_columns(&conn, kind.table().name).unwrap();
            let page = page_params(None, Some(&filter), 0, 1_000, 100);
            plans.push((
                kind.table().name,
                upsert_sql(*kind, &stored, true),
                page.clone(),
            ));
            plans.push((kind.table().name, upsert_key_sql(*kind, true), page));
        }
        for sql in [tombstone_sql(true), tombstone_key_sql(true)] {
            plans.push((
                "evidence_tombstones",
                sql,
                page_params(Some(ChangeKind::SessionEvent), Some(&filter), 0, 1_000, 100),
            ));
        }
        for (table, sql, values) in plans {
            let plan = conn
                .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
                .unwrap()
                .query_map(rusqlite::params_from_iter(values), |row| {
                    row.get::<_, String>(3)
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
                .join("\n");
            assert!(
                plan.contains(&format!("SEARCH {table} USING")),
                "{table}: {plan}"
            );
            assert!(!plan.contains(&format!("SCAN {table}")), "{table}: {plan}");
            assert!(!plan.contains("_revision"), "{table}: {plan}");
            // The trajectory's session is its primary key; every other
            // table's seek binds both the source and the session.
            if table == "trajectories" {
                assert!(plan.contains("(id=?)"), "{table}: {plan}");
            } else {
                assert!(
                    plan.contains("source=?") && plan.contains("session_id=?"),
                    "{table}: {plan}"
                );
            }
            eprintln!("{table}: {}", plan.replace('\n', " | "));
        }
    }

    /// One page holds exactly `batch` typed rows however many streams feed
    /// it: the first pass reads revisions only and the second fetches just
    /// the rows below the cut, so six tables and their tombstones cannot
    /// multiply what is resident.
    #[test]
    fn a_page_over_many_kinds_materialises_exactly_batch_rows() {
        let dir = tempfile::tempdir().unwrap();
        let (store, conn) = store(dir.path());
        for index in 0..4 {
            conn.execute(
                "INSERT INTO sessions (session_id, source) VALUES (?, 'claude')",
                [format!("s{index}")],
            )
            .unwrap();
            insert_event(&conn, "s0", &format!("e{index}"), "x");
            conn.execute(
                "INSERT INTO tool_calls (source, session_id, tool_use_id, name) \
                 VALUES ('claude', 's0', ?, 'Bash')",
                [format!("t{index}")],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO file_edits (source, session_id, tool_use_id, file_path, tool_name) \
                 VALUES ('claude', 's0', ?, '/p', 'Edit')",
                [format!("f{index}")],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO session_markers (source, session_id, marker_uid, kind) \
                 VALUES ('claude', 's0', ?, 'compaction')",
                [format!("m{index}")],
            )
            .unwrap();
            conn.execute(
                "INSERT INTO session_relationships (source, parent_session_id, \
                 relationship_uid, relationship, identity_status, evidence_kind, \
                 created_ms, updated_ms) \
                 VALUES ('claude', 's0', ?, 'delegated', 'unlinked', 'sidecar', 1, 1)",
                [format!("r{index}")],
            )
            .unwrap();
            conn.execute(
                "DELETE FROM session_markers WHERE marker_uid = ?",
                [format!("m{index}")],
            )
            .unwrap();
        }
        let head = store.head_revision().unwrap().revision;
        assert_eq!(head, 28, "24 upserts and 4 tombstone writes");

        // The cut is the batch-th smallest revision across all streams. Each
        // round wrote revisions 7k+1..7k+7, and the marker at 7k+5 is gone
        // (its tombstone sits at 7k+7), so the fifth smallest is 6.
        assert_eq!(
            page_cut(&conn, ChangeKind::ALL, None, 0, head, 5).unwrap(),
            Some(6)
        );
        assert_eq!(
            page_cut(&conn, ChangeKind::ALL, None, 20, head, 100).unwrap(),
            Some(head),
            "fewer than a page left: the cut is the head"
        );
        assert_eq!(
            page_cut(&conn, ChangeKind::ALL, None, head, head, 5).unwrap(),
            None
        );
        // Only the rows below the cut are fetched, across every kind.
        let fetched: usize = ChangeKind::ALL
            .iter()
            .map(|kind| read_upserts(&conn, *kind, None, 0, 6, 5).unwrap().len())
            .sum::<usize>()
            + read_tombstones(&conn, ChangeKind::ALL, None, 0, 6, 5)
                .unwrap()
                .len();
        assert_eq!(fetched, 5);

        let mut changes = store
            .changes_since(Watermark::START, ChangeQuery::default().batch(5))
            .unwrap();
        let first = changes.next().unwrap().unwrap();
        assert_eq!(first.revision, 1);
        assert_eq!(
            changes.buffer.len(),
            4,
            "the page held exactly `batch` rows, not `batch` per stream"
        );
        let mut revisions = vec![first.revision];
        for _ in 0..4 {
            revisions.push(changes.next().unwrap().unwrap().revision);
        }
        assert_eq!(revisions, vec![1, 2, 3, 4, 6]);
        assert_eq!(changes.position().revision, 6);
        assert!(changes.buffer.is_empty());
        // The rest arrives in later pages, in order, tombstones included:
        // 20 surviving upserts and 4 tombstones, less the 5 already seen.
        let rest = drain(changes);
        assert_eq!(rest.len(), 19);
        assert_eq!(rest.iter().filter(|c| c.op == ChangeOp::Delete).count(), 4);
        assert!(rest
            .windows(2)
            .all(|pair| pair[0].revision < pair[1].revision));
    }

    /// A drain that resolves its cursor while a sibling advances it must not
    /// mistake the advance for a store reset. The cursor and the head come
    /// from one snapshot, so a commit landing between the two reads is
    /// either wholly visible or wholly not.
    #[test]
    fn a_cursor_advanced_during_open_is_not_a_reset() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("ai-history.db");
        let (_store, writer) = store(dir.path());
        for index in 0..10 {
            insert_event(&writer, "s1", &format!("e{index}"), "x");
        }
        commit_cursor(
            &writer,
            "burn",
            Watermark {
                revision: 10,
                ..read_head(&writer).unwrap()
            },
            &KindSet::normalize(None),
            None,
        )
        .unwrap();

        // The drain's own connection, with a hook that lets a second
        // process's work land in the middle of resolving the start.
        let reader = open_db_readonly(&db).unwrap();
        let fired = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&fired);
        let db_for_hook = db.clone();
        reader.progress_handler(
            1,
            Some(move || {
                if !flag.swap(true, Ordering::SeqCst) {
                    let other = open_db(&db_for_hook).unwrap();
                    insert_event(&other, "s1", "e10", "late");
                    commit_cursor(
                        &other,
                        "burn",
                        Watermark {
                            revision: 11,
                            ..read_head(&other).unwrap()
                        },
                        &KindSet::normalize(None),
                        None,
                    )
                    .unwrap();
                }
                false
            }),
        );
        let (start, head, _) = resolve_start_and_head(
            &reader,
            Watermark::CONSUMER,
            Some("burn"),
            &KindSet::normalize(None),
        )
        .unwrap();
        reader.progress_handler(0, None::<fn() -> bool>);
        assert!(
            fired.load(Ordering::SeqCst),
            "the interleaving must have happened"
        );
        assert!(
            start <= head,
            "cursor {} must not look ahead of head {}",
            start.revision,
            head.revision
        );
        // Both values come from one snapshot: either before the sibling's
        // commit or after it, never one of each.
        assert!(
            (start.revision, head.revision) == (10, 10)
                || (start.revision, head.revision) == (11, 11),
            "start {} head {}",
            start.revision,
            head.revision
        );
    }

    /// A writer that re-stamps or deletes the rows of the page being read,
    /// between the key pass and the row pass, must not make the drain skip
    /// what still sits below its head. The passes share a snapshot, so the
    /// window the cut describes is the window the rows are read from; and
    /// an empty window steps forward rather than declaring the head.
    #[test]
    fn a_window_emptied_between_the_passes_does_not_skip_later_changes() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("ai-history.db");
        let (store, writer) = store(dir.path());
        for index in 0..10 {
            insert_event(&writer, "s1", &format!("e{index}"), "x");
        }
        let head = store.head_revision().unwrap();
        assert_eq!(head.revision, 10);

        // The drain's own connection, hooked so that while the first page's
        // key pass runs, a second writer re-stamps the first page's rows
        // (e0..e2 move to 11..13, past the head) and deletes e3 (tombstone
        // at 14, also past the head).
        let reader = open_db_readonly(&db).unwrap();
        let fired = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&fired);
        let db_for_hook = db.clone();
        reader.progress_handler(
            1,
            Some(move || {
                if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                    let other = open_db(&db_for_hook).unwrap();
                    for index in 0..3 {
                        insert_event(&other, "s1", &format!("e{index}"), "moved");
                    }
                    other
                        .execute("DELETE FROM session_events WHERE event_uid = 'e3'", [])
                        .unwrap();
                }
                false
            }),
        );
        let mut changes = Changes {
            db_path: db.clone(),
            read_only: false,
            conn: crate::read_pool::PooledConnection::detached(reader),
            kinds: ChangeKind::ALL.to_vec(),
            consumer: None,
            session: None,
            batch: 3,
            head,
            position: Watermark::START,
            stale_cursor: None,
            buffer: VecDeque::new(),
            exhausted: false,
        };
        let mut yielded = Vec::new();
        let mut positions = Vec::new();
        while let Some(change) = changes.next() {
            yielded.push(change.unwrap().revision);
            positions.push(changes.position().revision);
        }
        changes.conn.progress_handler(0, None::<fn() -> bool>);
        assert!(
            fired.load(Ordering::SeqCst) > 0,
            "the interleaving must have happened"
        );

        // Whatever the snapshot saw of the concurrent write, everything that
        // stayed below the head is yielded, in order, and the position never
        // overshoots what was yielded until the drain is genuinely done.
        assert!(
            yielded.windows(2).all(|pair| pair[0] < pair[1]),
            "{yielded:?}"
        );
        for revision in 5..=10 {
            assert!(
                yielded.contains(&revision),
                "revision {revision} skipped: {yielded:?}"
            );
        }
        assert!(
            yielded.iter().all(|revision| *revision <= 10),
            "{yielded:?}"
        );
        assert_eq!(positions, yielded);
        assert_eq!(changes.position(), head);
        // The moved rows are waiting past the head for the next drain.
        let later = drain(store.changes_since(head, ChangeQuery::default()).unwrap());
        let later: Vec<(u64, bool)> = later
            .iter()
            .map(|change| (change.revision, change.op == ChangeOp::Delete))
            .collect();
        assert_eq!(
            later,
            vec![(11, false), (12, false), (13, false), (14, true)]
        );
    }

    /// A named cursor is a position in one kind set's stream. A drain over
    /// events alone that reaches the head and commits has accounted for no
    /// relationship on the way, so an all-kinds drain must not be allowed to
    /// resume from it -- and a drain over other kinds must not move it.
    #[test]
    fn a_cursor_is_bound_to_the_kind_set_it_was_committed_for() {
        let dir = tempfile::tempdir().unwrap();
        let (store, conn) = store(dir.path());
        insert_event(&conn, "s1", "e1", "x");
        conn.execute(
            "INSERT INTO session_relationships (source, parent_session_id, relationship_uid, \
             relationship, identity_status, evidence_kind, created_ms, updated_ms) \
             VALUES ('claude', 's1', 'r1', 'delegated', 'unlinked', 'sidecar', 1, 1)",
            [],
        )
        .unwrap();
        let events_only = || {
            ChangeQuery::default()
                .consumer("c")
                .kinds([ChangeKind::SessionEvent])
        };

        // The filtered drain reaches the head and commits there.
        let mut filtered = store
            .changes_since(Watermark::CONSUMER, events_only())
            .unwrap();
        let seen: Vec<ChangeKind> = filtered
            .by_ref()
            .map(|change| change.unwrap().kind)
            .collect();
        assert_eq!(seen, vec![ChangeKind::SessionEvent]);
        assert_eq!(filtered.commit().unwrap().revision, 2);

        // An all-kinds drain cannot resume from a position that skipped the
        // relationship at revision 2.
        let error = store
            .changes_since(Watermark::CONSUMER, ChangeQuery::default().consumer("c"))
            .expect_err("a cursor bound to events must not serve an all-kinds drain");
        assert!(
            matches!(error, Error::ConsumerKindsMismatch(_)),
            "{error:?}"
        );
        assert!(error.to_string().contains("session_event"), "{error}");
        // Nor can a drain over other kinds move it, whatever `from` it used.
        let other = store
            .changes_since(Watermark::START, ChangeQuery::default().consumer("c"))
            .unwrap();
        let error = other
            .commit()
            .expect_err("a commit under another kind set is refused");
        assert!(
            matches!(error, Error::ConsumerKindsMismatch(_)),
            "{error:?}"
        );
        let stored: (i64, String) = conn
            .query_row(
                "SELECT revision, kinds FROM consumer_cursors WHERE name = 'c'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(stored, (2, "session_event".to_string()));

        // The same filter, however spelled, resumes it; another name over
        // every kind sees the relationship the filtered consumer never did.
        let same = store
            .changes_since(
                Watermark::CONSUMER,
                ChangeQuery::default()
                    .consumer("c")
                    .kinds([ChangeKind::SessionEvent, ChangeKind::SessionEvent]),
            )
            .unwrap();
        assert_eq!(same.position().revision, 2);
        assert!(drain(same).is_empty());
        let everything = drain(
            store
                .changes_since(Watermark::CONSUMER, ChangeQuery::default().consumer("d"))
                .unwrap(),
        );
        assert!(everything
            .iter()
            .any(|change| change.kind == ChangeKind::Relationship));
        // An all-kinds cursor is stored as `*`, whichever way it was asked for.
        let mut all = store
            .changes_since(
                Watermark::CONSUMER,
                ChangeQuery::default()
                    .consumer("d")
                    .kinds(ChangeKind::ALL.iter().rev().copied()),
            )
            .unwrap();
        while all.next().is_some() {}
        all.commit().unwrap();
        let stored: String = conn
            .query_row(
                "SELECT kinds FROM consumer_cursors WHERE name = 'd'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stored, "*");
    }

    /// A database from before the feed carries an `observation_clock` that
    /// says nothing about feed state. A read-only handle over it answers
    /// `START`, not that clock.
    #[test]
    fn a_legacy_store_reports_start_not_its_observation_clock() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("legacy.db");
        {
            let conn = open_db(&db).unwrap();
            insert_event(&conn, "s1", "e1", "x");
            conn.execute_batch(
                "DELETE FROM schema_migrations WHERE name = 'change_feed_v2'; \
                 UPDATE observation_clock SET version = 42;",
            )
            .unwrap();
        }
        let read_only = SessionStore::open(StoreOptions {
            db_path: Some(db.clone()),
            read_only: true,
            ..StoreOptions::default()
        })
        .unwrap();
        assert_eq!(read_only.head_revision().unwrap(), Watermark::START);
        assert!(read_only
            .changes_since(Watermark::START, ChangeQuery::default())
            .is_err());
        // Once migrated, the head is the real one again.
        let writable = SessionStore::open(StoreOptions {
            db_path: Some(db),
            ..StoreOptions::default()
        })
        .unwrap();
        assert!(writable.head_revision().unwrap().revision >= 42);
    }

    /// A database from before the feed existed is stamped once on migration,
    /// so a replay from START reports what it already held, and the head
    /// moves past every backfilled row.
    #[test]
    fn an_older_database_is_backfilled_once() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("older.db");
        {
            let conn = open_db(&db).unwrap();
            insert_event(&conn, "s1", "e1", "x");
            insert_event(&conn, "s1", "e2", "y");
            // Take the database back to before the feed: drop the marker,
            // triggers, index and column, as an older release would have
            // left it.
            for trigger in trigger_names(ChangeKind::SessionEvent) {
                conn.execute_batch(&format!("DROP TRIGGER {trigger};"))
                    .unwrap();
            }
            conn.execute_batch(
                "DROP INDEX idx_session_events_revision; \
                 ALTER TABLE session_events DROP COLUMN revision; \
                 DELETE FROM schema_migrations WHERE name = 'change_feed_v2'; \
                 UPDATE observation_clock SET version = 0;",
            )
            .unwrap();
        }
        let store = SessionStore::open(StoreOptions {
            db_path: Some(db.clone()),
            ..StoreOptions::default()
        })
        .unwrap();
        let changes = all(&store);
        let keys: Vec<&str> = changes
            .iter()
            .map(|change| change.record_key.as_str())
            .collect();
        assert_eq!(keys, vec!["e1", "e2"]);
        assert!(changes[0].revision < changes[1].revision);
        assert_eq!(store.head_revision().unwrap().revision, changes[1].revision);
        // Re-opening does not stamp again.
        SessionStore::open(StoreOptions {
            db_path: Some(db),
            ..StoreOptions::default()
        })
        .unwrap();
        assert_eq!(all(&store), changes);
    }

    /// Adding a kind after the fixed v2 migration marker has already landed
    /// still stamps rows that the older build could not report. The per-kind
    /// schema map makes that expansion non-rotating, so an all-kinds consumer
    /// resumes into the new kind above its prior head.
    #[test]
    fn an_established_v2_store_backfills_a_new_kind_once() {
        let dir = tempfile::tempdir().unwrap();
        let (initial_store, conn) = store(dir.path());
        insert_event(&conn, "s1", "e1", "before");
        let query = || ChangeQuery::default().consumer("all");
        let mut initial = initial_store
            .changes_since(Watermark::CONSUMER, query())
            .unwrap();
        while initial.next().is_some() {}
        let committed = initial.commit().unwrap();

        let mut old_digests = export_schema_digests(&conn).unwrap();
        old_digests.remove(ChangeKind::History.as_str());
        conn.execute(
            &format!(
                "UPDATE change_feed_store SET {EXPORT_SCHEMA_DIGEST_COLUMN}=? WHERE singleton=1"
            ),
            [serde_json::to_string(&old_digests).unwrap()],
        )
        .unwrap();
        for trigger in trigger_names(ChangeKind::History) {
            conn.execute_batch(&format!("DROP TRIGGER {trigger};"))
                .unwrap();
        }
        let history = ChangeKind::History.table();
        conn.execute_batch(&format!(
            "DROP INDEX {index}; ALTER TABLE {name} DROP COLUMN {REVISION_COLUMN};",
            index = history.revision_index(),
            name = history.name
        ))
        .unwrap();
        conn.execute(
            "INSERT INTO history (source, session_id, prompt, timestamp_ms) \
             VALUES ('claude', 's1', 'after', 1000)",
            [],
        )
        .unwrap();
        assert!(migration_applied(&conn, MIGRATION).unwrap());
        drop(initial);
        drop(conn);
        drop(initial_store);

        let (expanded, conn) = store(dir.path());
        let resumed = drain(
            expanded
                .changes_since(Watermark::CONSUMER, query())
                .unwrap(),
        );
        assert_eq!(resumed.len(), 1, "{resumed:?}");
        assert_eq!(resumed[0].kind, ChangeKind::History);
        assert_eq!(resumed[0].record_key, "[1000,\"after\"]");
        assert!(resumed[0].revision > committed.revision);
        assert_eq!(expanded.head_revision().unwrap().epoch, committed.epoch);
        let stamped = resumed[0].revision;
        drop(conn);
        drop(expanded);

        let (reopened, _) = store(dir.path());
        assert_eq!(reopened.head_revision().unwrap().revision, stamped);
    }

    /// A store the feed reached before it reported every kind: the kinds it
    /// lacked are stamped on migration above its head, so a cursor committed
    /// for every kind resumes into all of their rows and none of the rows it
    /// already had; the session re-stamp triggers move to their own names;
    /// and from then on a presence write stamps its session once.
    #[test]
    fn a_store_fed_before_every_kind_gains_the_rest_above_its_head() {
        const NEW_KINDS: [ChangeKind; 6] = [
            ChangeKind::History,
            ChangeKind::Presence,
            ChangeKind::CommitLink,
            ChangeKind::Trajectory,
            ChangeKind::SourceObservation,
            ChangeKind::ObservationEvidence,
        ];
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("fed-v1.db");
        let committed = {
            let (store, conn) = {
                let store = SessionStore::open(StoreOptions {
                    db_path: Some(db.clone()),
                    ..StoreOptions::default()
                })
                .unwrap();
                (store, open_db(&db).unwrap())
            };
            conn.execute(
                "INSERT INTO sessions (session_id, source) VALUES ('s1', 'claude')",
                [],
            )
            .unwrap();
            insert_event(&conn, "s1", "e1", "x");
            // Take the database back to the six-kind feed: no stamp, index or
            // triggers on the other tables, and the session re-stamp triggers
            // under the names they had.
            for trigger in PRESENCE_TRIGGERS {
                conn.execute_batch(&format!("DROP TRIGGER {trigger};"))
                    .unwrap();
            }
            for kind in NEW_KINDS {
                for trigger in trigger_names(kind) {
                    conn.execute_batch(&format!("DROP TRIGGER {trigger};"))
                        .unwrap();
                }
                let table = kind.table();
                conn.execute_batch(&format!(
                    "DROP INDEX {index}; ALTER TABLE {name} DROP COLUMN {REVISION_COLUMN};",
                    index = table.revision_index(),
                    name = table.name
                ))
                .unwrap();
            }
            let stamp = |row: &str| {
                format!(
                    "UPDATE observation_clock SET version = version + 1 WHERE singleton = 1; \
                     UPDATE sessions SET revision = \
                     (SELECT version FROM observation_clock WHERE singleton = 1) \
                     WHERE source = {row}.source AND session_id = {row}.session_id;"
                )
            };
            conn.execute_batch(&format!(
                "CREATE TRIGGER change_feed_session_presences_insert \
                     AFTER INSERT ON session_presences BEGIN {new} END; \
                 CREATE TRIGGER change_feed_session_presences_update \
                     AFTER UPDATE ON session_presences BEGIN {new} END; \
                 CREATE TRIGGER change_feed_session_presences_delete \
                     AFTER DELETE ON session_presences BEGIN {old} END; \
                 DELETE FROM schema_migrations WHERE name = 'change_feed_v2'; \
                 INSERT OR IGNORE INTO schema_migrations (name) VALUES ('change_feed_v1');",
                new = stamp("NEW"),
                old = stamp("OLD")
            ))
            .unwrap();
            // Rows the six-kind feed never stamped.
            conn.execute_batch(
                "INSERT INTO history (source, session_id, prompt, timestamp_ms) \
                     VALUES ('claude', 's1', 'hello', 1000); \
                 INSERT INTO session_presences (source, session_id, location) \
                     VALUES ('claude', 's1', 'local'); \
                 INSERT INTO trajectories (id, decisions_json, retrospective_json, \
                     search_text, updated_ms, timestamp_ms) \
                     VALUES ('traj-1', '[]', '{}', 'x', 1, 1);",
            )
            .unwrap();
            assert!(!schema_is_current(&conn).unwrap());
            // A consumer of every kind has read the six-kind store to its
            // head. This build refuses to drain a schema it has not migrated,
            // so the cursor is written as the six-kind build committed it.
            assert!(store
                .changes_since(Watermark::START, ChangeQuery::default())
                .is_err());
            let head: i64 = conn
                .query_row(
                    "SELECT version FROM observation_clock WHERE singleton = 1",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            conn.execute(
                "INSERT INTO consumer_cursors (name, revision, updated_ms, kinds) \
                 VALUES ('all', ?, 0, '*')",
                [head],
            )
            .unwrap();
            head as u64
        };

        let store = SessionStore::open(StoreOptions {
            db_path: Some(db.clone()),
            ..StoreOptions::default()
        })
        .unwrap();
        let conn = open_db(&db).unwrap();
        assert!(schema_is_current(&conn).unwrap());
        let resumed = drain(
            store
                .changes_since(Watermark::CONSUMER, ChangeQuery::default().consumer("all"))
                .unwrap(),
        );
        let kinds: Vec<ChangeKind> = resumed.iter().map(|change| change.kind).collect();
        assert_eq!(
            kinds,
            vec![
                ChangeKind::History,
                ChangeKind::Presence,
                ChangeKind::Trajectory
            ],
            "the new kinds' rows, and nothing the cursor already accounted for: {resumed:?}"
        );
        assert!(resumed.iter().all(|change| change.revision > committed));
        assert_eq!(
            resumed[0].key,
            vec![
                Value::from("history"),
                Value::from("claude"),
                Value::from(1000),
                Value::from("hello")
            ]
        );

        // The retired names now stamp the presence kind; the re-stamp has its
        // own.
        let body: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE name = 'change_feed_session_presences_insert'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(body.contains("evidence_tombstones"), "{body}");
        let head = store.head_revision().unwrap();
        conn.execute(
            "INSERT INTO session_presences (source, session_id, location) \
             VALUES ('claude', 's1', 'remote')",
            [],
        )
        .unwrap();
        let delta = drain(store.changes_since(head, ChangeQuery::default()).unwrap());
        let kinds: Vec<ChangeKind> = delta.iter().map(|change| change.kind).collect();
        assert_eq!(kinds.len(), 2, "{delta:?}");
        assert!(kinds.contains(&ChangeKind::Session) && kinds.contains(&ChangeKind::Presence));
        assert_eq!(
            store.head_revision().unwrap().revision,
            head.revision + 2,
            "one stamp for the presence and one for its session"
        );

        // Re-opening does not stamp again.
        let before = all(&store);
        SessionStore::open(StoreOptions {
            db_path: Some(db),
            ..StoreOptions::default()
        })
        .unwrap();
        assert_eq!(all(&store), before);
    }

    #[test]
    fn a_read_only_store_reads_the_feed_but_cannot_commit() {
        let dir = tempfile::tempdir().unwrap();
        let (_writable, conn) = store(dir.path());
        insert_event(&conn, "s1", "e1", "x");
        let read_only = SessionStore::open(StoreOptions {
            db_path: Some(dir.path().join("ai-history.db")),
            read_only: true,
            ..StoreOptions::default()
        })
        .unwrap();
        let changes = read_only
            .changes_since(
                Watermark::START,
                ChangeQuery {
                    consumer: Some("ro".to_string()),
                    ..ChangeQuery::default()
                },
            )
            .unwrap();
        assert!(changes.commit().is_err());
        assert_eq!(drain(changes).len(), 1);
    }

    /// Every kind's stored row and key, as the feed reports them, are the
    /// live table's: every column but `revision`, in table order, each value
    /// as SQLite holds it, and the key is the kind's wire name followed by
    /// the columns of the table's uniqueness constraint. The key columns are
    /// spelled out here rather than read from the feed's own table map, so a
    /// change to either is a failing test: an embedder derives a record's
    /// identity from this key, and a different key is a different record.
    #[test]
    fn every_kind_reports_the_live_row_and_its_uniqueness_key() {
        use std::collections::{BTreeMap, BTreeSet};

        fn key_columns(kind: ChangeKind) -> (&'static str, &'static [&'static str]) {
            match kind {
                ChangeKind::Session => ("sessions", &["source", "session_id"]),
                ChangeKind::SessionEvent => {
                    ("session_events", &["source", "session_id", "event_uid"])
                }
                ChangeKind::ToolCall => ("tool_calls", &["source", "session_id", "tool_use_id"]),
                ChangeKind::FileEdit => ("file_edits", &["source", "session_id", "tool_use_id"]),
                ChangeKind::SessionMarker => {
                    ("session_markers", &["source", "session_id", "marker_uid"])
                }
                ChangeKind::Relationship => (
                    "session_relationships",
                    &["source", "parent_session_id", "relationship_uid"],
                ),
                ChangeKind::History => ("history", &["source", "timestamp_ms", "prompt"]),
                ChangeKind::Presence => {
                    ("session_presences", &["source", "session_id", "location"])
                }
                ChangeKind::CommitLink => (
                    "session_commit_links",
                    &["source", "session_id", "commit_sha", "match_method"],
                ),
                ChangeKind::Trajectory => ("trajectories", &["id"]),
                ChangeKind::SourceObservation => (
                    "session_observations",
                    &[
                        "source",
                        "session_id",
                        "location",
                        "connector_id",
                        "connector_instance",
                    ],
                ),
                ChangeKind::ObservationEvidence => (
                    "observation_evidence",
                    &[
                        "source",
                        "session_id",
                        "location",
                        "connector_id",
                        "connector_instance",
                        "evidence_uid",
                    ],
                ),
            }
        }

        /// The row as the table holds it, found by the revision the feed
        /// reported: revisions are unique per write, so this is exactly the
        /// row the change describes.
        fn live_row(conn: &Connection, table: &str, revision: u64) -> Vec<(String, Value)> {
            let mut statement = conn
                .prepare(&format!(
                    "SELECT * FROM {table} WHERE {REVISION_COLUMN} = ?1"
                ))
                .unwrap();
            let names: Vec<String> = statement
                .column_names()
                .into_iter()
                .map(str::to_string)
                .collect();
            statement
                .query_row([revision as i64], |row| {
                    Ok(names
                        .iter()
                        .enumerate()
                        .map(|(index, name)| {
                            (name.clone(), stored_value(row.get_ref(index).unwrap()))
                        })
                        .collect::<Vec<_>>())
                })
                .unwrap()
        }

        /// Checks every upsert against its live row and returns the keys seen,
        /// by kind.
        fn check(
            store: &SessionStore,
            conn: &Connection,
            step: &str,
        ) -> BTreeMap<ChangeKind, BTreeSet<String>> {
            let mut upserts: BTreeMap<ChangeKind, BTreeSet<String>> = BTreeMap::new();
            for change in all(store) {
                let ChangeOp::Upsert(_) = &change.op else {
                    assert!(change.columns.is_none(), "{step}: a delete carries no row");
                    continue;
                };
                let (table, keyed_by) = key_columns(change.kind);
                let live = live_row(conn, table, change.revision);
                let expected: Vec<(String, Value)> = live
                    .iter()
                    .filter(|(name, _)| name != REVISION_COLUMN)
                    .cloned()
                    .collect();
                let columns = change.columns.clone().expect("an upsert carries its row");
                let reported: Vec<(String, Value)> = columns
                    .iter()
                    .map(|(name, value)| (name.to_string(), value.clone()))
                    .collect();
                assert_eq!(reported, expected, "{step}: {} row", change.kind.as_str());
                let mut key = vec![Value::from(change.kind.as_str())];
                for column in keyed_by {
                    let value = live
                        .iter()
                        .find(|(name, _)| name == column)
                        .map(|(_, value)| value.clone())
                        .unwrap_or_else(|| panic!("{table} has no key column {column}"));
                    key.push(value);
                }
                assert_eq!(change.key, key, "{step}: {} key", change.kind.as_str());
                upserts
                    .entry(change.kind)
                    .or_default()
                    .insert(serde_json::to_string(&change.key).unwrap());
            }
            upserts
        }

        let dir = tempfile::tempdir().unwrap();
        let (store, conn) = store(dir.path());
        conn.execute_batch(EVERY_KIND_FIXTURE).unwrap();
        let inserted = check(&store, &conn, "inserts");
        let every: BTreeSet<ChangeKind> = ChangeKind::ALL.iter().copied().collect();
        assert_eq!(
            inserted.keys().copied().collect::<BTreeSet<_>>(),
            every,
            "the writes cover every kind"
        );
        // The serialized key is what an embedder hashes into a record id, so
        // its exact text is part of the contract: compact JSON, raw values.
        assert!(
            inserted[&ChangeKind::SessionEvent].contains(r#"["session_event","claude","s1","e1"]"#),
            "{:?}",
            inserted[&ChangeKind::SessionEvent]
        );
        assert!(
            inserted[&ChangeKind::History].contains(r#"["history","codex",2000,"no session yet"]"#),
            "{:?}",
            inserted[&ChangeKind::History]
        );
        assert!(inserted[&ChangeKind::Trajectory].contains(r#"["trajectory","traj-1"]"#));

        conn.execute_batch(
            r#"
UPDATE sessions SET cwd = '/q' WHERE session_id = 's1';
UPDATE session_events SET text = 'one, edited' WHERE event_uid = 'e1';
UPDATE tool_calls SET tool_use_id = 't1b' WHERE tool_use_id = 't1';
UPDATE file_edits SET lines_removed = 1 WHERE tool_use_id = 't2';
UPDATE session_markers SET text = 'summary' WHERE marker_uid = 'mk1';
UPDATE session_relationships SET updated_ms = 9 WHERE relationship_uid = 'r1';
UPDATE history SET project = '/q' WHERE prompt = 'hello';
UPDATE history SET session_id = 'c1' WHERE prompt = 'no session yet';
UPDATE session_presences SET raw_locator = '/q/s1.jsonl';
UPDATE session_commit_links SET confidence = 0.5;
UPDATE trajectories SET status = 'completed', completed_at = '2026-09-24';
UPDATE session_observations SET access_state = 'unavailable';
UPDATE observation_evidence SET payload_json = '{"a":2}';
"#,
        )
        .unwrap();
        let updated = check(&store, &conn, "updates");
        assert!(
            !all(&store)
                .iter()
                .any(|change| change.kind == ChangeKind::History && change.op == ChangeOp::Delete),
            "a prompt gaining a session is an upsert of the same record, never a delete"
        );

        conn.execute_batch(
            "DELETE FROM history WHERE prompt = 'hello';
             DELETE FROM trajectories;
             DELETE FROM observation_evidence;
             DELETE FROM session_commit_links;
             DELETE FROM sessions WHERE session_id = 's1';",
        )
        .unwrap();
        check(&store, &conn, "deletes");
        // A delete names the record by the key its upsert carried.
        let mut tombstoned = BTreeSet::new();
        for change in all(&store) {
            if change.op != ChangeOp::Delete {
                continue;
            }
            tombstoned.insert(change.kind);
            let key = serde_json::to_string(&change.key).unwrap();
            assert!(
                inserted
                    .get(&change.kind)
                    .is_some_and(|keys| keys.contains(&key))
                    || updated
                        .get(&change.kind)
                        .is_some_and(|keys| keys.contains(&key)),
                "{key} was never upserted"
            );
        }
        for kind in [
            ChangeKind::Session,
            ChangeKind::ToolCall,
            ChangeKind::History,
            ChangeKind::Presence,
            ChangeKind::CommitLink,
            ChangeKind::Trajectory,
            ChangeKind::SourceObservation,
            ChangeKind::ObservationEvidence,
        ] {
            assert!(tombstoned.contains(&kind), "{kind:?} left a tombstone");
        }
    }

    /// A read-only handle over a database the feed schema has not reached is
    /// told what to do, not served `no such table`.
    #[test]
    fn a_read_only_store_over_an_unstamped_database_names_the_remedy() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("unstamped.db");
        {
            let conn = open_db(&db).unwrap();
            conn.execute_batch("DROP TABLE consumer_cursors;").unwrap();
        }
        let read_only = SessionStore::open(StoreOptions {
            db_path: Some(db),
            read_only: true,
            ..StoreOptions::default()
        })
        .unwrap();
        let message = read_only
            .changes_since(Watermark::START, ChangeQuery::default())
            .err()
            .unwrap()
            .to_string();
        assert!(message.contains("change-feed schema"), "{message}");
        assert!(message.contains("writable"), "{message}");
    }
}
