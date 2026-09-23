//! Revision-stamped change feed over the evidence tables.
//!
//! A downstream consumer that materialises its own view of the ledger — burn's
//! watch loop, say — needs "what changed since my last tick" without
//! rescanning every session. This module answers that with one monotonic
//! revision per row write, drawn from the database-wide `observation_clock`,
//! and a pull cursor over it:
//!
//! - Every row of `sessions`, `session_events`, `tool_calls`, `file_edits`,
//!   `session_markers` and `session_relationships` carries a `revision`. A
//!   trigger stamps the current clock on every insert and every update, so a
//!   re-parse that upserts a row it already holds re-stamps it: consumers must
//!   treat a re-seen `record_key` as a replace, never as a duplicate.
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
use crate::relationship_graph::{map_relationship, SessionRelationship, RELATIONSHIP_COLUMNS};
use crate::session_store::{Error, ErrorKind, SessionStore, Source};
use crate::store::{
    ensure_columns, migration_applied, open_db, open_db_readonly, row_to_file_edit,
    row_to_session_event, row_to_session_marker, row_to_tool_call, SessionEvent, SessionFileEdit,
    SessionMarker, SessionToolCall, FILE_EDIT_COLUMNS, SESSION_EVENT_COLUMNS,
    SESSION_MARKER_COLUMNS, TOOL_CALL_COLUMNS,
};
use crate::EvidenceKind;
use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};

/// The column every fed table carries. Named once so the delivery capture
/// triggers can leave it out of their payloads.
pub(crate) const REVISION_COLUMN: &str = "revision";

/// Largest page one [`Changes`] fill reads per kind.
pub const MAX_CHANGE_BATCH: usize = 10_000;

/// Page size when [`ChangeQuery::batch`] is zero.
pub const DEFAULT_CHANGE_BATCH: usize = 1_000;

const MIGRATION: &str = "change_feed_v1";

/// The column a named cursor records its kind set in.
const KINDS_COLUMN: &str = "kinds";

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
    Error::new(
        ErrorKind::ConsumerKindsMismatch,
        format!(
            "changes_since: consumer {name:?} is bound to kinds [{stored}] but this drain reports \
             [{offered}]; a cursor is a position in one kind set's stream, use another consumer \
             name for another filter"
        ),
    )
}

/// A position in the feed: the revision of the last change accounted for.
///
/// Revisions are unique per row write, so a watermark is a complete position
/// and `revision > watermark` is the whole resume predicate.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct Watermark {
    pub revision: u64,
}

impl Watermark {
    /// Before the first stamped row: a full replay.
    pub const START: Watermark = Watermark { revision: 0 };

    /// With [`ChangeQuery::consumer`] set: resume from that consumer's last
    /// committed position, or from [`Watermark::START`] when it has none.
    pub const CONSUMER: Watermark = Watermark { revision: u64::MAX };
}

/// Which table a change is about.
///
/// This is not [`EvidenceKind`]: that enum names the record kinds a source
/// adapter can supply, and the catalog row is not one of them, while the feed
/// has to report a session's catalog row changing. [`ChangeKind::evidence_kind`]
/// maps the overlap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ChangeKind {
    Session,
    SessionEvent,
    ToolCall,
    FileEdit,
    SessionMarker,
    Relationship,
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
    ];

    /// The wire name, identical to the serde representation and to the `kind`
    /// stored on a tombstone.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Session => "session",
            Self::SessionEvent => "session_event",
            Self::ToolCall => "tool_call",
            Self::FileEdit => "file_edit",
            Self::SessionMarker => "session_marker",
            Self::Relationship => "relationship",
        }
    }

    /// The source-evidence kind this change carries, or `None` for the
    /// catalog row, which no adapter supplies.
    pub fn evidence_kind(self) -> Option<EvidenceKind> {
        match self {
            Self::Session => None,
            Self::SessionEvent => Some(EvidenceKind::SessionEvent),
            Self::ToolCall => Some(EvidenceKind::ToolCall),
            Self::FileEdit => Some(EvidenceKind::FileEdit),
            Self::SessionMarker => Some(EvidenceKind::SessionMarker),
            Self::Relationship => Some(EvidenceKind::Relationship),
        }
    }

    fn table(self) -> FedTable {
        match self {
            Self::Session => FedTable {
                name: "sessions",
                session: "session_id",
                key: "session_id",
            },
            Self::SessionEvent => FedTable {
                name: "session_events",
                session: "session_id",
                key: "event_uid",
            },
            Self::ToolCall => FedTable {
                name: "tool_calls",
                session: "session_id",
                key: "tool_use_id",
            },
            Self::FileEdit => FedTable {
                name: "file_edits",
                session: "session_id",
                key: "tool_use_id",
            },
            Self::SessionMarker => FedTable {
                name: "session_markers",
                session: "session_id",
                key: "marker_uid",
            },
            Self::Relationship => FedTable {
                name: "session_relationships",
                session: "parent_session_id",
                key: "relationship_uid",
            },
        }
    }

    fn columns(self) -> &'static str {
        match self {
            Self::Session => SESSION_COLUMNS,
            Self::SessionEvent => SESSION_EVENT_COLUMNS,
            Self::ToolCall => TOOL_CALL_COLUMNS,
            Self::FileEdit => FILE_EDIT_COLUMNS,
            Self::SessionMarker => SESSION_MARKER_COLUMNS,
            Self::Relationship => RELATIONSHIP_COLUMNS,
        }
    }
}

/// One stamped table: where its source, session and record key live.
struct FedTable {
    name: &'static str,
    session: &'static str,
    key: &'static str,
}

impl FedTable {
    fn revision_index(&self) -> String {
        format!("idx_{}_revision", self.name)
    }
}

/// The row an upsert carries, typed per kind so no second read is needed.
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
    pub source: Source,
    /// For a relationship, the parent session.
    pub session_id: String,
    /// The record's provider-native identity within its session and kind:
    /// `event_uid`, `tool_use_id`, `marker_uid`, `relationship_uid`, or the
    /// session id itself for a catalog row.
    pub record_key: String,
    pub revision: u64,
    pub op: ChangeOp,
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
    conn: Connection,
    kinds: Vec<ChangeKind>,
    consumer: Option<String>,
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
    /// [`ErrorKind::WatermarkAheadOfStore`]: it names no revision of this
    /// store, so a drain opened while the cursor stood past its head — the
    /// resync from [`Watermark::START`] — replaces exactly that cursor when
    /// it commits, however far the store has grown meanwhile. A named cursor
    /// is bound to the
    /// kind set it was first committed for: a drain over other kinds cannot
    /// resume it or move it ([`ErrorKind::ConsumerKindsMismatch`]), because
    /// its position accounts for nothing outside its own kinds. Fails on a
    /// read-only store and when no consumer was named.
    pub fn commit(&self) -> Result<Watermark, Error> {
        let Some(name) = &self.consumer else {
            return Err(Error::new(
                ErrorKind::Other,
                "changes_since: commit needs ChangeQuery::consumer to name the cursor",
            ));
        };
        if self.read_only {
            return Err(Error::new(
                ErrorKind::Other,
                "SessionStore is read-only; a consumer cursor cannot be committed through it",
            ));
        }
        let conn = open_db(&self.db_path)?;
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
            let Some(cut) = page_cut(&snapshot, &self.kinds, lo, hi, self.batch)? else {
                // The key pass itself found nothing left: that, and only
                // that, is exhaustion.
                self.exhausted = true;
                self.position = self.head;
                break;
            };
            let mut rows: Vec<Change> = Vec::with_capacity(self.batch);
            for kind in &self.kinds {
                rows.extend(read_upserts(&snapshot, *kind, lo, cut, self.batch)?);
            }
            rows.extend(read_tombstones(
                &snapshot,
                &self.kinds,
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
                self.position = Watermark { revision: cut };
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
                };
                return Some(Ok(change));
            }
            if self.exhausted {
                return None;
            }
            if let Err(error) = self.fill() {
                self.exhausted = true;
                return Some(Err(Error::from_anyhow(error)));
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
            .field("batch", &self.batch)
            .field("head", &self.head)
            .field("position", &self.position)
            .field("exhausted", &self.exhausted)
            .finish_non_exhaustive()
    }
}

impl SessionStore {
    /// The store's change-feed head: the revision of the newest stamped row.
    ///
    /// A consumer holding a watermark above this is ahead of the store, which
    /// means the database was reset or replaced under it; the only recovery
    /// is a full resync from [`Watermark::START`].
    pub fn head_revision(&self) -> Result<Watermark, Error> {
        head_revision_at(self.db_path())
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
    /// Fails with [`ErrorKind::WatermarkAheadOfStore`] when the resolved start
    /// exceeds the head. Like the marker page, the schema gate is here rather
    /// than at `open`: a read-only store over a database written before the
    /// feed existed is told to migrate rather than served `no such column`.
    pub fn changes_since(&self, from: Watermark, query: ChangeQuery) -> Result<Changes, Error> {
        let conn = open_db_readonly(self.db_path())?;
        if !schema_is_current(&conn).map_err(Error::from_anyhow)? {
            return Err(Error::new(
                ErrorKind::Other,
                format!(
                    "{} predates the change-feed schema this version reads; \
                     open it writable once (or run a sync) to migrate it",
                    self.db_path().display()
                ),
            ));
        }
        let kind_set = KindSet::normalize(query.kinds);
        let batch = match query.batch {
            0 => DEFAULT_CHANGE_BATCH,
            batch => batch.min(MAX_CHANGE_BATCH),
        };
        if from == Watermark::CONSUMER && query.consumer.is_none() {
            return Err(Error::new(
                ErrorKind::Other,
                "changes_since: Watermark::CONSUMER needs ChangeQuery::consumer to name the cursor",
            ));
        }
        let (start, head, stale_cursor) =
            resolve_start_and_head(&conn, from, query.consumer.as_deref(), &kind_set)?;
        if start > head {
            return Err(Error::new(
                ErrorKind::WatermarkAheadOfStore,
                format!(
                    "changes_since: watermark {} is ahead of the store head {}; the database was \
                     reset or replaced, resync from Watermark::START",
                    start.revision, head.revision
                ),
            ));
        }
        Ok(Changes {
            db_path: self.db_path().to_path_buf(),
            read_only: self.is_read_only(),
            conn,
            kinds: kind_set.kinds,
            consumer: query.consumer,
            batch,
            head,
            position: start,
            stale_cursor,
            buffer: VecDeque::new(),
            exhausted: false,
        })
    }
}

/// The head revision of the database at `db_path`, through a read-only
/// handle. A database from before the feed existed answers `START`: nothing
/// in it is stamped, so nothing in it is reported yet -- and its
/// `observation_clock`, which predates the feed, is not a feed position.
pub(crate) fn head_revision_at(db_path: &Path) -> Result<Watermark, Error> {
    let conn = open_db_readonly(db_path)?;
    if !schema_is_current(&conn).map_err(Error::from_anyhow)? {
        return Ok(Watermark::START);
    }
    read_head(&conn).map_err(Error::from_anyhow)
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
    let snapshot = conn.unchecked_transaction().map_err(anyhow::Error::from)?;
    let cursor = match consumer {
        Some(name) => read_cursor(&snapshot, name)?,
        None => None,
    };
    let start =
        match (from == Watermark::CONSUMER, consumer, &cursor) {
            (true, Some(name), Some((position, stored))) => {
                let offered = kinds.stored();
                if *stored != offered {
                    return Err(kinds_mismatch(name, stored, &offered));
                }
                *position
            }
            (true, Some(_), None) => Watermark::START,
            (true, None, _) => return Err(Error::new(
                ErrorKind::Other,
                "changes_since: Watermark::CONSUMER needs ChangeQuery::consumer to name the cursor",
            )),
            (false, _, _) => from,
        };
    let head = read_head(&snapshot)?;
    snapshot.commit().map_err(anyhow::Error::from)?;
    let stale_cursor = cursor
        .map(|(position, _)| position)
        .filter(|position| *position > head);
    Ok((start, head, stale_cursor))
}

/// The revision-only page query for one kind: a covering read of the
/// revision index.
fn upsert_key_sql(kind: ChangeKind) -> String {
    format!(
        "SELECT {REVISION_COLUMN} FROM {name} \
         WHERE {REVISION_COLUMN} > ?1 AND {REVISION_COLUMN} <= ?2 \
         ORDER BY {REVISION_COLUMN} ASC LIMIT ?3",
        name = kind.table().name,
    )
}

fn tombstone_key_sql() -> String {
    format!(
        "SELECT {REVISION_COLUMN} FROM evidence_tombstones \
         WHERE kind = ?1 AND {REVISION_COLUMN} > ?2 AND {REVISION_COLUMN} <= ?3 \
         ORDER BY {REVISION_COLUMN} ASC LIMIT ?4"
    )
}

/// The highest revision of the next page: the `batch`-th smallest revision
/// in `(lo, hi]` across every stream, or `hi` when fewer remain, or `None`
/// when nothing does. Reads revisions only.
fn page_cut(
    conn: &Connection,
    kinds: &[ChangeKind],
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
    let range: [rusqlite::types::Value; 3] = [
        (lo as i64).into(),
        (hi as i64).into(),
        (batch as i64).into(),
    ];
    for kind in kinds {
        let mut statement = conn.prepare_cached(&upsert_key_sql(*kind))?;
        collect(&mut statement, &range)?;
    }
    let mut tombstones = conn.prepare_cached(&tombstone_key_sql())?;
    for kind in kinds {
        let mut values: Vec<rusqlite::types::Value> = vec![kind.as_str().to_string().into()];
        values.extend(range.iter().cloned());
        collect(&mut tombstones, &values)?;
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

fn read_head(conn: &Connection) -> Result<Watermark> {
    let revision: Option<i64> = conn
        .query_row(
            "SELECT version FROM observation_clock WHERE singleton = 1",
            [],
            |row| row.get(0),
        )
        .optional()
        .context("reading the change-feed head")?;
    Ok(Watermark {
        revision: revision.unwrap_or(0).max(0) as u64,
    })
}

fn read_cursor(conn: &Connection, name: &str) -> Result<Option<(Watermark, String)>> {
    let row: Option<(i64, String)> = conn
        .query_row(
            &format!("SELECT revision, {KINDS_COLUMN} FROM consumer_cursors WHERE name = ?"),
            [name],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    Ok(row.map(|(revision, kinds)| {
        (
            Watermark {
                revision: revision.max(0) as u64,
            },
            kinds,
        )
    }))
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
                 VALUES (?1, ?2, ?3, ?4) \
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
                stale.map(|stale| stale.revision as i64)
            ],
            |row| row.get(0),
        )
        .optional()
        .map_err(anyhow::Error::from)?;
    match revision {
        Some(revision) => Ok(Watermark {
            revision: revision.max(0) as u64,
        }),
        // The conflict clause declined: the row exists under another kind set.
        None => {
            let stored = read_cursor(conn, name)?
                .map(|(_, stored)| stored)
                .unwrap_or_default();
            Err(kinds_mismatch(name, &stored, &offered))
        }
    }
}

fn parse_source(name: &str) -> Result<Source> {
    match name {
        "claude" => Ok(Source::Claude),
        "codex" => Ok(Source::Codex),
        "cursor" => Ok(Source::Cursor),
        "grok" => Ok(Source::Grok),
        "relay" => Ok(Source::Relay),
        "trajectory" => Ok(Source::Trajectory),
        "opencode" => Ok(Source::OpenCode),
        other => anyhow::bail!("change feed: unknown source {other:?}"),
    }
}

/// The page query for one kind: an indexed range read, oldest first.
fn upsert_sql(kind: ChangeKind) -> String {
    let table = kind.table();
    format!(
        "SELECT {columns}, {REVISION_COLUMN} FROM {name} \
         WHERE {REVISION_COLUMN} > ?1 AND {REVISION_COLUMN} <= ?2 \
         ORDER BY {REVISION_COLUMN} ASC LIMIT ?3",
        columns = kind.columns(),
        name = table.name,
    )
}

fn read_upserts(
    conn: &Connection,
    kind: ChangeKind,
    lo: u64,
    hi: u64,
    batch: usize,
) -> Result<Vec<Change>> {
    let mut statement = conn.prepare_cached(&upsert_sql(kind))?;
    let rows = statement.query_map(params![lo as i64, hi as i64, batch as i64], |row| {
        let revision: i64 = row.get(REVISION_COLUMN)?;
        let (source, session_id, record_key, evidence) = match kind {
            ChangeKind::Session => {
                let session = row_to_session(row)?;
                (
                    session.source.clone(),
                    session.session_id.clone(),
                    session.session_id.clone(),
                    EvidenceRow::Session(session),
                )
            }
            ChangeKind::SessionEvent => {
                let event = row_to_session_event(row)?;
                (
                    event.source.clone(),
                    event.session_id.clone(),
                    event.event_uid.clone(),
                    EvidenceRow::SessionEvent(event),
                )
            }
            ChangeKind::ToolCall => {
                let call = row_to_tool_call(row)?;
                (
                    call.source.clone(),
                    call.session_id.clone(),
                    call.tool_use_id.clone(),
                    EvidenceRow::ToolCall(call),
                )
            }
            ChangeKind::FileEdit => {
                let edit = row_to_file_edit(row)?;
                (
                    edit.source.clone(),
                    edit.session_id.clone(),
                    edit.tool_use_id.clone(),
                    EvidenceRow::FileEdit(edit),
                )
            }
            ChangeKind::SessionMarker => {
                let marker = row_to_session_marker(row)?;
                (
                    marker.source.clone(),
                    marker.session_id.clone(),
                    marker.marker_uid.clone(),
                    EvidenceRow::SessionMarker(marker),
                )
            }
            ChangeKind::Relationship => {
                let relationship = map_relationship(row)?;
                (
                    relationship.source.clone(),
                    relationship.parent_session_id.clone(),
                    relationship.relationship_uid.clone(),
                    EvidenceRow::Relationship(relationship),
                )
            }
        };
        Ok((source, session_id, record_key, revision, evidence))
    })?;
    let mut changes = Vec::new();
    for row in rows {
        let (source, session_id, record_key, revision, evidence) = row?;
        changes.push(Change {
            kind,
            source: parse_source(&source)?,
            session_id,
            record_key,
            revision: revision.max(0) as u64,
            op: ChangeOp::Upsert(evidence),
        });
    }
    Ok(changes)
}

/// The tombstone page query for one kind. One kind per read, not a `kind IN`
/// list: the tombstone index is `(kind, revision)`, and a single-kind
/// equality is what lets the range come back in revision order without a
/// temporary sort.
fn tombstone_sql() -> String {
    format!(
        "SELECT source, session_id, record_key, {REVISION_COLUMN} FROM evidence_tombstones \
         WHERE kind = ?1 AND {REVISION_COLUMN} > ?2 AND {REVISION_COLUMN} <= ?3 \
         ORDER BY {REVISION_COLUMN} ASC LIMIT ?4"
    )
}

fn read_tombstones(
    conn: &Connection,
    kinds: &[ChangeKind],
    lo: u64,
    hi: u64,
    batch: usize,
) -> Result<Vec<Change>> {
    let mut changes = Vec::new();
    let mut statement = conn.prepare_cached(&tombstone_sql())?;
    for kind in kinds {
        let rows = statement.query_map(
            params![kind.as_str(), lo as i64, hi as i64, batch as i64],
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
            changes.push(Change {
                kind: *kind,
                source: parse_source(&source)?,
                session_id,
                record_key,
                revision: revision.max(0) as u64,
                op: ChangeOp::Delete,
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

/// Whether this database has everything [`init_schema`] would add.
pub(crate) fn schema_is_current(conn: &Connection) -> Result<bool> {
    let mut object = conn.prepare("SELECT 1 FROM sqlite_master WHERE name = ? LIMIT 1")?;
    for name in [
        "evidence_tombstones",
        "consumer_cursors",
        "idx_evidence_tombstones_kind_revision",
        "observation_clock",
    ] {
        if !object.exists([name])? {
            return Ok(false);
        }
    }
    for kind in ChangeKind::ALL {
        let table = kind.table();
        if !object.exists([table.revision_index()])? {
            return Ok(false);
        }
        for trigger in trigger_names(*kind) {
            if !object.exists([trigger])? {
                return Ok(false);
            }
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
         );",
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
    for kind in ChangeKind::ALL {
        let table = kind.table();
        ensure_columns(
            conn,
            table.name,
            &[(REVISION_COLUMN, "INTEGER NOT NULL DEFAULT 0")],
        )?;
        if backfill {
            // Rows written before the feed existed are stamped once, in
            // rowid order, each above everything stamped before it, so a
            // replay from START reports the whole store. The triggers do not
            // exist yet, so this UPDATE stamps exactly what it names.
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
        }
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
        let session = table.session;
        let key = table.key;
        // The update trigger's own stamp changes `revision`, and only that,
        // so `NEW.revision = OLD.revision` is what stops it re-firing under
        // `recursive_triggers` — and what makes an external write that leaves
        // the stamp alone (every upsert in this crate) take a new one. A key
        // change is a delete of the old key and an upsert of the new, each at
        // its own revision.
        conn.execute_batch(&format!(
            "CREATE TRIGGER IF NOT EXISTS {insert} AFTER INSERT ON {name} BEGIN
                 UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
                 UPDATE {name} SET {REVISION_COLUMN} = \
                     (SELECT version FROM observation_clock WHERE singleton = 1) \
                     WHERE rowid = NEW.rowid;
                 DELETE FROM evidence_tombstones WHERE kind = '{kind}' \
                     AND source = NEW.source AND session_id = NEW.{session} \
                     AND record_key = NEW.{key};
             END;
             CREATE TRIGGER IF NOT EXISTS {update} AFTER UPDATE ON {name}
             WHEN NEW.{REVISION_COLUMN} = OLD.{REVISION_COLUMN} BEGIN
                 UPDATE observation_clock SET version = version + 1 WHERE singleton = 1 \
                     AND (OLD.source IS NOT NEW.source OR OLD.{session} IS NOT NEW.{session} \
                          OR OLD.{key} IS NOT NEW.{key});
                 INSERT OR REPLACE INTO evidence_tombstones \
                     (kind, source, session_id, record_key, revision) \
                     SELECT '{kind}', OLD.source, OLD.{session}, OLD.{key}, \
                         (SELECT version FROM observation_clock WHERE singleton = 1) \
                     WHERE OLD.source IS NOT NEW.source OR OLD.{session} IS NOT NEW.{session} \
                         OR OLD.{key} IS NOT NEW.{key};
                 UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
                 UPDATE {name} SET {REVISION_COLUMN} = \
                     (SELECT version FROM observation_clock WHERE singleton = 1) \
                     WHERE rowid = NEW.rowid;
                 DELETE FROM evidence_tombstones WHERE kind = '{kind}' \
                     AND source = NEW.source AND session_id = NEW.{session} \
                     AND record_key = NEW.{key};
             END;
             CREATE TRIGGER IF NOT EXISTS {delete} AFTER DELETE ON {name} BEGIN
                 UPDATE observation_clock SET version = version + 1 WHERE singleton = 1;
                 INSERT OR REPLACE INTO evidence_tombstones \
                     (kind, source, session_id, record_key, revision) \
                     VALUES ('{kind}', OLD.source, OLD.{session}, OLD.{key}, \
                         (SELECT version FROM observation_clock WHERE singleton = 1));
             END;"
        ))?;
    }
    // A direct write of `revision` does not re-fire the sessions update
    // trigger (its guard is `NEW.revision = OLD.revision`), so this is one
    // stamp, not two. A key change is a presence leaving one session and
    // arriving at another; both rows are stamped.
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
    conn.execute_batch(&format!(
        "CREATE TRIGGER IF NOT EXISTS change_feed_session_presences_insert \
             AFTER INSERT ON session_presences BEGIN
             {stamp_new}
         END;
         CREATE TRIGGER IF NOT EXISTS change_feed_session_presences_update \
             AFTER UPDATE ON session_presences BEGIN
             {stamp_new}
             UPDATE observation_clock SET version = version + 1 WHERE singleton = 1 \
                 AND (OLD.source IS NOT NEW.source OR OLD.session_id IS NOT NEW.session_id);
             UPDATE sessions SET {REVISION_COLUMN} = \
                 (SELECT version FROM observation_clock WHERE singleton = 1) \
                 WHERE source = OLD.source AND session_id = OLD.session_id \
                 AND (OLD.source IS NOT NEW.source OR OLD.session_id IS NOT NEW.session_id);
         END;
         CREATE TRIGGER IF NOT EXISTS change_feed_session_presences_delete \
             AFTER DELETE ON session_presences BEGIN
             {stamp_old}
         END;"
    ))?;
    conn.execute(
        "INSERT OR IGNORE INTO schema_migrations (name) VALUES (?)",
        [MIGRATION],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session_store::StoreOptions;

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
        assert_eq!(store.head_revision().unwrap(), Watermark::START);
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
            .all(|change| change.source == Source::Claude && change.session_id == "s1"));
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
                .changes_since(Watermark { revision: 7 }, ChangeQuery::default())
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
        assert_eq!(again.position(), Watermark::START);
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
        assert_eq!(b.position(), Watermark::START);
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
        let query = || ChangeQuery::default().consumer("c");
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
        let delta = drain(store.changes_since(head, ChangeQuery::default()).unwrap());
        assert_eq!(delta.len(), 1, "{delta:?}");
        match &delta[0].op {
            ChangeOp::Upsert(EvidenceRow::Session(session)) => {
                assert_eq!(session.locations, vec!["local", "remote"]);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_watermark_ahead_of_the_store_is_a_named_error() {
        let dir = tempfile::tempdir().unwrap();
        let (store, conn) = store(dir.path());
        insert_event(&conn, "s1", "e1", "x");
        let error = store
            .changes_since(Watermark { revision: 99 }, ChangeQuery::default())
            .expect_err("a watermark past the head cannot be resumed from");
        assert_eq!(error.kind(), ErrorKind::WatermarkAheadOfStore);
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
        assert_eq!(error.kind(), ErrorKind::WatermarkAheadOfStore);
        // Exactly at the head is fine: nothing new, no error.
        assert!(drain(
            store
                .changes_since(Watermark { revision: 1 }, ChangeQuery::default())
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
        assert_eq!(error.kind(), ErrorKind::WatermarkAheadOfStore);

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
                .changes_since(Watermark { revision: 8 }, ChangeQuery::default())
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
            plans.push((kind.table().name, upsert_sql(*kind), page.clone()));
        }
        let mut tombstone_page = vec![rusqlite::types::Value::from("session_event".to_string())];
        tombstone_page.extend(page.iter().cloned());
        plans.push((
            "evidence_tombstones",
            tombstone_sql(),
            tombstone_page.clone(),
        ));
        // The revision-only first pass must be a covering read of the same
        // index, with no table access at all.
        for kind in ChangeKind::ALL {
            plans.push((kind.table().name, upsert_key_sql(*kind), page.clone()));
        }
        plans.push(("evidence_tombstones", tombstone_key_sql(), tombstone_page));
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
            page_cut(&conn, ChangeKind::ALL, 0, head, 5).unwrap(),
            Some(6)
        );
        assert_eq!(
            page_cut(&conn, ChangeKind::ALL, 20, head, 100).unwrap(),
            Some(head),
            "fewer than a page left: the cut is the head"
        );
        assert_eq!(
            page_cut(&conn, ChangeKind::ALL, head, head, 5).unwrap(),
            None
        );
        // Only the rows below the cut are fetched, across every kind.
        let fetched: usize = ChangeKind::ALL
            .iter()
            .map(|kind| read_upserts(&conn, *kind, 0, 6, 5).unwrap().len())
            .sum::<usize>()
            + read_tombstones(&conn, ChangeKind::ALL, 0, 6, 5)
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
            Watermark { revision: 10 },
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
                        Watermark { revision: 11 },
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
            conn: reader,
            kinds: ChangeKind::ALL.to_vec(),
            consumer: None,
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
        assert_eq!(error.kind(), ErrorKind::ConsumerKindsMismatch);
        assert!(error.to_string().contains("session_event"), "{error}");
        // Nor can a drain over other kinds move it, whatever `from` it used.
        let other = store
            .changes_since(Watermark::START, ChangeQuery::default().consumer("c"))
            .unwrap();
        let error = other
            .commit()
            .expect_err("a commit under another kind set is refused");
        assert_eq!(error.kind(), ErrorKind::ConsumerKindsMismatch);
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
                "DELETE FROM schema_migrations WHERE name = 'change_feed_v1'; \
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
                 DELETE FROM schema_migrations WHERE name = 'change_feed_v1'; \
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
