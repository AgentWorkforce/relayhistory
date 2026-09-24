//! Embedder entry point: [`SessionStore`] and the typed evidence it returns.
//!
//! This module is the whole default public surface of the crate. Everything
//! below it — parsers, schema, paged queries, the raw `rusqlite::Connection`
//! — stays behind the `unstable-internal` feature. Cargo semver is the
//! contract; there is no separate Rust contract-version constant.
//!
//! Ten operations, one entry type:
//!
//! | Method | What it does |
//! |---|---|
//! | [`SessionStore::open`] | open (and, unless read-only, create and migrate) `ai-history.db` |
//! | [`SessionStore::discover`] | the shallow catalog sweep, hydrating nothing |
//! | [`SessionStore::sync`] | one full local sweep under the crate's `SyncRunLock` |
//! | [`SessionStore::hydrate`] | one session, by id or by transcript path, plus its bounded related transcripts |
//! | [`SessionStore::watch`] | the live-capture loop, as an iterator of ticks |
//! | [`SessionStore::sessions`] | the catalog, keyset-paged internally |
//! | [`SessionStore::session`] | everything the store holds about one session, typed |
//! | [`SessionStore::changes_since`] | the revision-stamped change feed, with named consumer cursors |
//! | [`SessionStore::head_revision`] | the feed head, for a consumer checking its stored watermark |
//! | [`Source::capabilities`] | what a source can and cannot report, statically |
//!
//! The two change-feed methods live in [`crate::change_feed`]; they are
//! inherent methods on this type, so the facade stays the one entry point.
//!
//! Every value type here is `#[non_exhaustive]`, `Clone`, `Serialize`,
//! `Deserialize` and `PartialEq`; only [`CatalogIter`] (a read snapshot) and
//! [`WatchHandle`] (a running thread) are not. JSON columns arrive parsed; the raw string is reachable
//! through a `raw_*()` accessor and is never a public field. No signature
//! names a `rusqlite` type.

use crate::discover::{
    self, list_session_catalog_page, CatalogCursor, CatalogListOptions, ShallowSession,
};
pub use crate::ingest::control::ControlKind;
use crate::ingest::hook::{ingest_transcript_at_with_roots, TranscriptStatus};
use crate::ingest::hydrate::{
    hydrate_session_at_with_roots_and_connectors, HydrateSessionOptions, HydrateSessionResult,
};
use crate::ingest::{
    source_watch_roots, sync_facade_tick, sync_watch_roots_with_provider_roots,
    with_capture_observer, with_capture_token, CaptureCancelled, CaptureProgress, SyncTick,
    HOOK_HARNESSES,
};
use crate::paths::home_dir;
pub use crate::paths::ProviderRoots;
use crate::relationship_graph::{self, RelationshipCapabilities, SessionRelationship};
use crate::remote::SourceConnectorSelection;
use crate::session_usage::{
    session_requests_page, session_usage_summary, SessionRequest, SessionUsageSummary,
};
use crate::source_evidence::EvidenceKind;
use crate::store::{
    default_db_path, open_db, open_db_readonly, prompt_hash, schema_is_event_read_current,
    schema_is_evidence_read_current, schema_is_relationship_read_current,
    schema_is_usage_read_current, session_events_sized, session_file_edits, session_markers_sized,
    session_prompts_sized, session_tool_calls, session_user_turns_page, PromptRow, SessionEvent,
    SessionFileEdit, SessionMarker, SessionScope, SessionToolCall, SessionUserTurn,
};
use crate::usage::{normalize_usage_str, source_accounting, NormalizedUsage, UsageAccounting};
use crate::watch::{TickOutcome, TickTrigger, WatchDriver, WatchLoop};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// errors
// ---------------------------------------------------------------------------

/// Recoverable failure from [`SessionStore`].
///
/// The variants mirror the TypeScript SDK's native error classes
/// (`docs/architecture.md`, "Native errors") minus the four that only a Node
/// addon can raise, plus the four the Rust facade adds: [`Error::SyncLocked`],
/// [`Error::SourceMismatch`], [`Error::WatermarkAheadOfStore`] and
/// [`Error::Cancelled`].
/// [`Error::code`] is the stable `SCREAMING_SNAKE_CASE` code a host can match
/// on or forward; `Display` renders `CODE: message`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(tag = "code", content = "detail", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Error {
    /// The database could not be opened, created, or migrated.
    #[serde(rename = "DATABASE_OPEN_FAILED")]
    DatabaseOpen(String),
    /// A read-only open refused a database written before the shape this
    /// version reads. Separate from [`Error::DatabaseOpen`] because it is the
    /// one open failure with a remedy a caller can apply without a human:
    /// reopening the same path writable migrates it. That reopen also takes
    /// the writer lock and runs initialization, so it is the wrong answer to
    /// every other failure — which is why the fact is typed rather than left
    /// in the message text. The message names the remedy for callers that
    /// will not write.
    #[serde(rename = "DATABASE_STALE_SCHEMA")]
    StaleSchema(String),
    /// A caller-supplied value was rejected before anything was read.
    InvalidArgument(String),
    /// The operation is not available on this handle: `discover`, `sync`,
    /// `hydrate` and `watch` on a store opened with `read_only: true`.
    UnsupportedOperation(String),
    /// `hydrate` was asked for a session the catalog does not hold.
    SessionNotFound(String),
    /// The session is catalogued but its provider source can no longer be
    /// reached (a moved rollout, a store with no provenance).
    SessionSourceUnavailable(String),
    /// The provider data and the session it was claimed for disagree: a
    /// transcript naming another session, a catalog row pointing at a
    /// superseded store, a hook payload pairing the wrong file.
    #[serde(rename = "SESSION_SOURCE_MISMATCH")]
    SourceMismatch(String),
    /// The source has no full-evidence path at all (relay), or the reference
    /// form is not supported for it (`SessionRef::Path` on a source with no
    /// hook harness).
    HydrationUnsupported(String),
    /// Hydration ran and failed for a reason with no narrower code.
    HydrationFailed(String),
    /// A remote scope was asked for and no connector is configured.
    ConnectorNotConfigured(String),
    /// A remote connector's credentials have lapsed.
    AuthenticationExpired(String),
    /// A remote connector returned a snapshot narrower than it declared.
    EvidencePartial(String),
    /// A remote connector failed outright.
    ConnectorFailure(String),
    /// A read failed inside SQLite.
    #[serde(rename = "QUERY_FAILED")]
    Query(String),
    /// The shallow discovery pass that ends every sweep failed as a whole,
    /// so the sweep's evidence landed but the catalog rows behind it did not.
    #[serde(rename = "DISCOVERY_FAILED")]
    Discovery(String),
    /// A sweep failed for a reason with no narrower code.
    SyncFailed(String),
    /// Another process holds the store's `SyncRunLock` — an `ai-hist sync`,
    /// a `watch`, the napi addon — and the wait allowed by
    /// [`SyncOptions::lock_timeout_ms`] ran out. Never a silent no-op: a
    /// caller that asked for a sweep and got none is told so.
    SyncLocked {
        /// The database whose lock is held.
        path: PathBuf,
        /// How long this call waited before giving up.
        waited_ms: u64,
    },
    /// A change-feed watermark names a position this store never issued: a
    /// revision it has not reached, or an epoch that is not its own. The
    /// database was reset or replaced under a consumer that kept its cursor
    /// elsewhere. Raised by [`SessionStore::changes_since`]; the only
    /// recovery is a resync from [`crate::Watermark::START`].
    WatermarkAheadOfStore(String),
    /// A named change-feed cursor was asked to serve, or be moved by, a drain
    /// over a different kind set than it was committed for. A cursor is a
    /// position in one kind set's stream; use another consumer name for
    /// another filter. See [`SessionStore::changes_since`].
    ConsumerKindsMismatch(String),
    /// The caller's [`StopToken`] stopped `discover`, `sync` or `hydrate`.
    /// Work committed before the stop stays; the unfinished transaction rolls
    /// back and the next call resumes from its checkpoint.
    Cancelled(String),
}

impl Error {
    /// The stable code, as the TypeScript SDK spells it.
    pub fn code(&self) -> &'static str {
        match self {
            Self::DatabaseOpen(_) => "DATABASE_OPEN_FAILED",
            Self::StaleSchema(_) => "DATABASE_STALE_SCHEMA",
            Self::InvalidArgument(_) => "INVALID_ARGUMENT",
            Self::UnsupportedOperation(_) => "UNSUPPORTED_OPERATION",
            Self::SessionNotFound(_) => "SESSION_NOT_FOUND",
            Self::SessionSourceUnavailable(_) => "SESSION_SOURCE_UNAVAILABLE",
            Self::SourceMismatch(_) => "SESSION_SOURCE_MISMATCH",
            Self::HydrationUnsupported(_) => "HYDRATION_UNSUPPORTED",
            Self::HydrationFailed(_) => "HYDRATION_FAILED",
            Self::ConnectorNotConfigured(_) => "CONNECTOR_NOT_CONFIGURED",
            Self::AuthenticationExpired(_) => "AUTHENTICATION_EXPIRED",
            Self::EvidencePartial(_) => "EVIDENCE_PARTIAL",
            Self::ConnectorFailure(_) => "CONNECTOR_FAILURE",
            Self::Query(_) => "QUERY_FAILED",
            Self::Discovery(_) => "DISCOVERY_FAILED",
            Self::SyncFailed(_) => "SYNC_FAILED",
            Self::SyncLocked { .. } => "SYNC_LOCKED",
            Self::WatermarkAheadOfStore(_) => "WATERMARK_AHEAD_OF_STORE",
            Self::ConsumerKindsMismatch(_) => "CONSUMER_KINDS_MISMATCH",
            Self::Cancelled(_) => "CANCELLED",
        }
    }

    /// The human-readable part, without the code.
    pub fn message(&self) -> String {
        match self {
            Self::DatabaseOpen(m)
            | Self::StaleSchema(m)
            | Self::InvalidArgument(m)
            | Self::UnsupportedOperation(m)
            | Self::SessionNotFound(m)
            | Self::SessionSourceUnavailable(m)
            | Self::SourceMismatch(m)
            | Self::HydrationUnsupported(m)
            | Self::HydrationFailed(m)
            | Self::ConnectorNotConfigured(m)
            | Self::AuthenticationExpired(m)
            | Self::EvidencePartial(m)
            | Self::ConnectorFailure(m)
            | Self::Query(m)
            | Self::Discovery(m)
            | Self::SyncFailed(m)
            | Self::WatermarkAheadOfStore(m)
            | Self::ConsumerKindsMismatch(m)
            | Self::Cancelled(m) => m.clone(),
            Self::SyncLocked { path, waited_ms } => format!(
                "another sync holds the lock on {} (waited {waited_ms} ms); \
                 wait for it to finish or raise SyncOptions::lock_timeout_ms",
                path.display()
            ),
        }
    }

    /// Classify an internal failure by the `CODE: detail` prefix the engine
    /// puts on the errors it can name, falling back to `otherwise` for the
    /// rest. The napi layer does the same; the two must agree.
    fn classify(error: anyhow::Error, otherwise: fn(String) -> Self) -> Self {
        if error.chain().any(|cause| cause.is::<CaptureCancelled>()) {
            return Self::Cancelled(format!("{error:#}"));
        }
        let message = format!("{error:#}");
        let coded: &[(&str, ErrorBuilder)] = &[
            ("SESSION_NOT_FOUND", Self::SessionNotFound),
            ("SESSION_SOURCE_UNAVAILABLE", Self::SessionSourceUnavailable),
            ("SESSION_SOURCE_MISMATCH", Self::SourceMismatch),
            ("HYDRATION_UNSUPPORTED", Self::HydrationUnsupported),
            ("CONNECTOR_NOT_CONFIGURED", Self::ConnectorNotConfigured),
            ("AUTHENTICATION_EXPIRED", Self::AuthenticationExpired),
            ("EVIDENCE_PARTIAL", Self::EvidencePartial),
            ("CONNECTOR_FAILURE", Self::ConnectorFailure),
            ("INVALID_ARGUMENT", Self::InvalidArgument),
            ("DISCOVERY_FAILED", Self::Discovery),
        ];
        for (code, build) in coded {
            if let Some(detail) = message.strip_prefix(&format!("{code}: ")) {
                return build(detail.to_string());
            }
        }
        otherwise(message)
    }

    pub(crate) fn query(error: anyhow::Error) -> Self {
        Self::classify(error, Self::Query)
    }

    pub(crate) fn sql(error: rusqlite::Error) -> Self {
        Self::Query(error.to_string())
    }

    fn hydration(error: anyhow::Error) -> Self {
        Self::classify(error, Self::HydrationFailed)
    }

    fn sync(error: anyhow::Error) -> Self {
        Self::classify(error, Self::SyncFailed)
    }

    /// A read-only store refusing a database written before the `what`
    /// schema this version reads.
    fn stale_schema(db_path: &Path, what: &str) -> Self {
        Self::StaleSchema(format!(
            "{} predates the {what} schema this version reads; \
             open it writable once (or run a sync) to migrate it",
            db_path.display()
        ))
    }

    /// Whether this failure is a read-only store refusing a database it
    /// would have to migrate first.
    pub fn is_stale_schema(&self) -> bool {
        matches!(self, Self::StaleSchema(_))
    }

    pub(crate) fn read_only(operation: &str) -> Self {
        Self::UnsupportedOperation(format!(
            "SessionStore was opened read-only; `{operation}` needs a writable handle"
        ))
    }
}

/// One `Error` variant's constructor, for the code table above.
type ErrorBuilder = fn(String) -> Error;

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code(), self.message())
    }
}

impl std::error::Error for Error {}

// ---------------------------------------------------------------------------
// sources
// ---------------------------------------------------------------------------

/// Coding-agent source that produced a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "lowercase")]
pub enum Source {
    Claude,
    Codex,
    Cursor,
    Grok,
    Relay,
    Trajectory,
    #[serde(rename = "opencode")]
    OpenCode,
}

impl Source {
    /// Every source, in ledger order.
    ///
    /// `Source` is `#[non_exhaustive]`, so a crate outside this one cannot
    /// enumerate it with a `match` and any list it keeps by hand goes stale
    /// the day a variant is added. This is the one list an embedder (or a
    /// doc test) iterates; the unit test below is an exhaustive `match` over
    /// the enum, so adding a variant without adding it here fails to compile.
    pub const ALL: &'static [Source] = &[
        Self::Claude,
        Self::Codex,
        Self::Cursor,
        Self::Grok,
        Self::Relay,
        Self::Trajectory,
        Self::OpenCode,
    ];

    /// Canonical lowercase identifier stored in the ledger.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Claude => "claude",
            Self::Codex => "codex",
            Self::Cursor => "cursor",
            Self::Grok => "grok",
            Self::Relay => "relay",
            Self::Trajectory => "trajectory",
            Self::OpenCode => "opencode",
        }
    }

    /// The source a ledger identifier names, or `None` for one this build
    /// does not know.
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|source| source.as_str() == value)
    }

    /// What this source's adapter can and cannot report. Static: a property
    /// of the parser, not of any one session or database.
    pub fn capabilities(self) -> SourceCapabilities {
        let name = self.as_str();
        let mut evidence_kinds = discover::declared_evidence_kinds(name).to_vec();
        // Two sources declare nothing through shallow discovery yet write
        // `history` rows all the same: relay's prompts arrive through the
        // remote connector, and a trajectory's search text is indexed as a
        // prompt by the trajectory sweep (`.trajectories` records are exempt
        // from discovery, not from ingestion). A consumer reading those rows
        // must not be told the source cannot produce them — and since
        // `session()` reads only what is declared here, an undeclared kind
        // is also an unread one.
        if matches!(self, Self::Relay | Self::Trajectory)
            && !evidence_kinds.contains(&EvidenceKind::History)
        {
            evidence_kinds.push(EvidenceKind::History);
        }
        // Markers are derived by this crate's own parser rather than declared
        // by an adapter (a remote connector cannot supply them, so the
        // connector-facing declaration leaves them out); the parsers that
        // write them are the ones an embedder needs listed.
        if matches!(
            self,
            Self::Claude | Self::Codex | Self::Grok | Self::OpenCode
        ) && !evidence_kinds.contains(&EvidenceKind::SessionMarker)
        {
            evidence_kinds.push(EvidenceKind::SessionMarker);
        }
        SourceCapabilities {
            source: self,
            evidence_kinds,
            relationships: relationship_graph::relationship_capabilities(name),
            usage_accounting: source_accounting(name),
            message_ids: match self {
                // Claude: the record `uuid`. OpenCode: the provider's own
                // message row id.
                Self::Claude | Self::OpenCode => MessageIdOrigin::Provider,
                // Codex: `{line_index}:{record type}`. Grok: `ev:{id}` /
                // `tool:{call id}`, built from the event stream.
                Self::Codex | Self::Grok => MessageIdOrigin::Synthesized,
                // Cursor: the message `id` when the build wrote one, else
                // `cursor:{record offset}`.
                Self::Cursor => MessageIdOrigin::Mixed,
                Self::Relay | Self::Trajectory => MessageIdOrigin::None,
            },
            hydrates_by_path: HOOK_HARNESSES.contains(&name),
        }
    }
}

impl fmt::Display for Source {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for Source {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Error> {
        Self::parse(value)
            .ok_or_else(|| Error::InvalidArgument(format!("unknown source `{value}`")))
    }
}

/// Where a source's `message_id` values come from.
///
/// A consumer keying long-lived state on a message id needs to know whether
/// it is the provider's identifier or one this crate synthesized from the
/// record's position: the latter is stable for a given file content and
/// re-parse, but it is not something the provider will ever name again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "kebab-case")]
pub enum MessageIdOrigin {
    /// The provider's own identifier, verbatim.
    Provider,
    /// Derived by this crate from the record's position or call id.
    Synthesized,
    /// The provider's id when the record carries one, synthesized otherwise.
    Mixed,
    /// The source records no messages.
    None,
}

/// What a source's adapter declares about itself. See [`Source::capabilities`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SourceCapabilities {
    pub source: Source,
    /// The [`EvidenceKind`]s the local parser is able to produce. A kind
    /// absent here is one the source never reports; a kind present with no
    /// rows in a session means the session has none of it.
    pub evidence_kinds: Vec<EvidenceKind>,
    /// Delegation capture: whether a child session is always, sometimes or
    /// never independently addressable, and which delegation facts are
    /// recorded.
    pub relationships: RelationshipCapabilities,
    /// How one stored usage record should be read, or `None` when the source
    /// reports no usage at all.
    pub usage_accounting: Option<UsageAccounting>,
    /// Whether `message_id` is provider-issued or synthesized.
    pub message_ids: MessageIdOrigin,
    /// Whether [`SessionRef::Path`] can be hydrated for this source.
    pub hydrates_by_path: bool,
}

impl SourceCapabilities {
    /// Everything [`SessionStore::watch`] watches for this source under
    /// `roots`, from the same builder the loop registers with: the adapter's
    /// transcript roots and, for Claude and Codex, the flat `history.jsonl`
    /// prompt log beside them, each with the scope the loop applies. Empty
    /// for a source with no local files (relay); for trajectory it is the
    /// `.trajectories` directories known at the time of the call, which the
    /// loop re-derives on every backstop tick.
    pub fn watch_roots(&self, roots: &ProviderRoots) -> Vec<WatchedPath> {
        source_watch_roots(self.source.as_str(), roots)
            .into_iter()
            .map(WatchedPath::from_root)
            .collect()
    }
}

/// How much of a [`WatchedPath`] the watcher covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum WatchScope {
    /// Only the one file the path names. The watcher registers the file's
    /// *parent* (a watch on the file itself dies with the next atomic
    /// rewrite) and filters every other entry back out; a consumer building
    /// its own watcher must do the same, not watch the parent as a tree.
    File,
    /// The directory's own entries, and nothing below them.
    Directory,
    /// The path and its whole subtree.
    Tree,
}

/// One path the live-capture watcher covers, and how much of it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[non_exhaustive]
pub struct WatchedPath {
    /// The root's own path: the file for a `File` scope, the directory
    /// otherwise.
    pub path: PathBuf,
    pub scope: WatchScope,
}

impl WatchedPath {
    fn from_root(root: discover::WatchRoot) -> Self {
        Self {
            scope: match root.depth {
                discover::WatchDepth::File => WatchScope::File,
                discover::WatchDepth::Directory => WatchScope::Directory,
                discover::WatchDepth::Tree => WatchScope::Tree,
            },
            path: root.path,
        }
    }
}

// ---------------------------------------------------------------------------
// store
// ---------------------------------------------------------------------------

/// How to open a [`SessionStore`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct StoreOptions {
    /// The database. Defaults to `$AI_HIST_DB`, then the XDG data path, or
    /// `<home>/.local/share/ai-hist/ai-history.db` when `home` is set.
    pub db_path: Option<PathBuf>,
    /// Provider home to scan instead of the process `HOME`. Ignored when
    /// `roots` is set. Provider roots derived from it still honour
    /// `CLAUDE_CONFIG_DIR`, `CODEX_HOME`, `GROK_HOME` and `OPENCODE_DB` when
    /// they are set, exactly as the CLI does.
    pub home: Option<PathBuf>,
    /// Exactly where each provider keeps its sessions, resolved by the
    /// caller. `None` derives them from `home` (or the process `HOME`) with
    /// the environment overrides applied, as [`ProviderRoots::from_env`]
    /// does. One resolution drives `sync`, `hydrate`, `watch` and
    /// [`SourceCapabilities::watch_roots`] alike.
    pub roots: Option<ProviderRoots>,
    /// Never write. `discover`, `sync`, `hydrate` and `watch` return
    /// [`Error::UnsupportedOperation`]; a database older than the shape this
    /// version reads is refused at `open` rather than failing inside a query.
    pub read_only: bool,
}

/// A session the store can name.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(tag = "by", rename_all = "lowercase")]
pub enum SessionRef {
    /// By the provider's session identifier.
    Id { source: Source, session_id: String },
    /// By the transcript's location — the hook fast path, where the harness
    /// names the file it just wrote. Only sources whose
    /// [`SourceCapabilities::hydrates_by_path`] is true accept this form.
    Path { source: Source, path: PathBuf },
}

impl SessionRef {
    pub fn id(source: Source, session_id: impl Into<String>) -> Self {
        Self::Id {
            source,
            session_id: session_id.into(),
        }
    }

    pub fn path(source: Source, path: impl Into<PathBuf>) -> Self {
        Self::Path {
            source,
            path: path.into(),
        }
    }

    pub fn source(&self) -> Source {
        match self {
            Self::Id { source, .. } | Self::Path { source, .. } => *source,
        }
    }
}

/// The single public entry point for embedding `ai-hist` from Rust.
///
/// A handle is cheap: it holds the database path and the open options, and
/// opens a connection per call. `sync`, `hydrate` and `watch` take the same
/// `SyncRunLock` and per-session hydration locks the CLI and the napi addon
/// take, so an embedder is one more writer *implementation* of the same
/// discipline, never a second one — see the ADR's store-shape section.
#[derive(Debug, Clone)]
pub struct SessionStore {
    db_path: PathBuf,
    roots: ProviderRoots,
    read_only: bool,
}

impl SessionStore {
    /// Open (and create, unless `read_only`) the local history database.
    ///
    /// A writable open migrates the database to the shape this version reads.
    /// A read-only one cannot, so it checks instead and fails here, naming the
    /// remedy: the alternative is an open that succeeds and a first read that
    /// dies on `no such column` somewhere inside a query, which tells the
    /// caller nothing about what to do. The napi layer answers the same
    /// mismatch by reopening writable and migrating; a read-only embedder has
    /// asked not to be written for, so it is told rather than upgraded behind
    /// its back.
    pub fn open(opts: StoreOptions) -> Result<Self, Error> {
        let db_path = resolve_db_path(&opts);
        if opts.read_only {
            let conn = open_db_readonly(&db_path)
                .map_err(|error| Error::DatabaseOpen(format!("{error:#}")))?;
            let current = schema_is_event_read_current(&conn)
                .and_then(|ok| Ok(ok && schema_is_evidence_read_current(&conn)?))
                .and_then(|ok| Ok(ok && schema_is_relationship_read_current(&conn)?))
                .and_then(|ok| Ok(ok && schema_is_usage_read_current(&conn)?))
                .map_err(Error::query)?;
            if !current {
                return Err(Error::stale_schema(&db_path, "session-evidence"));
            }
        } else {
            open_db(&db_path).map_err(|error| Error::DatabaseOpen(format!("{error:#}")))?;
        }
        let roots = opts
            .roots
            .unwrap_or_else(|| ProviderRoots::from_env(opts.home.unwrap_or_else(home_dir)));
        Ok(Self {
            db_path,
            roots,
            read_only: opts.read_only,
        })
    }

    /// The database this store reads.
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    /// The provider roots every operation on this store scans, hydrates from
    /// and watches.
    pub fn roots(&self) -> &ProviderRoots {
        &self.roots
    }

    /// Whether this handle was opened read-only.
    pub fn read_only(&self) -> bool {
        self.read_only
    }

    fn read_conn(&self) -> Result<Connection, Error> {
        open_db_readonly(&self.db_path).map_err(|error| Error::DatabaseOpen(format!("{error:#}")))
    }

    // -- sync ---------------------------------------------------------------

    /// One full local sweep into this store, under the `SyncRunLock`.
    ///
    /// The sweep is the one `ai-hist sync` runs: every local provider, the
    /// stat-only source fingerprint fast path unless `force`, shallow
    /// discovery at the end. When another process holds the lock this waits
    /// up to [`SyncOptions::lock_timeout_ms`] and then returns
    /// [`Error::SyncLocked`] — never a silent no-op.
    pub fn sync(&self, opts: SyncOptions) -> Result<SyncReport, Error> {
        if self.read_only {
            return Err(Error::read_only("sync"));
        }
        let (tick, changed) = controlled(opts.stop.as_ref(), opts.progress.as_ref(), || {
            self.sync_tick(opts.force, opts.lock_timeout_ms)
        })?;
        Ok(SyncReport {
            swept: tick.swept,
            changed,
            head_revision: crate::change_feed::head_revision_at(&self.db_path)?.revision,
        })
    }

    /// Run the sweep, retrying a held lock until `lock_timeout_ms` is spent.
    ///
    /// The catalog digest is taken and compared inside the locked section —
    /// see [`sync_facade_tick`] — so `changed` is what the catalog gained
    /// between this sweep taking the lock and releasing it, not since some
    /// earlier read that another process's sync could have moved past.
    fn sync_tick(
        &self,
        force: bool,
        lock_timeout_ms: u64,
    ) -> Result<(SyncTick, Vec<SessionRef>), Error> {
        let started = Instant::now();
        // Bounded where it enters, like every watch interval: `Instant +
        // Duration` panics when the sum is not representable, and a caller
        // passing `u64::MAX` to mean "wait as long as it takes" would die
        // here before the first attempt. Seven days is far longer than any
        // lock this crate holds.
        let deadline = started + Duration::from_millis(lock_timeout_ms.min(MAX_LOCK_WAIT_MS));
        loop {
            let outcome = sync_facade_tick(
                &self.db_path,
                &self.roots,
                force,
                |conn| catalog_fingerprint(conn).map_err(anyhow::Error::from),
                |conn, before, _tick| Ok(changes_under_lock(conn, &before)?.1),
            )
            .map_err(Error::sync)?;
            if let Some(outcome) = outcome {
                return Ok(outcome);
            }
            crate::ingest::check_capture_cancelled().map_err(Error::sync)?;
            let now = Instant::now();
            if now >= deadline {
                return Err(Error::SyncLocked {
                    path: self.db_path.clone(),
                    waited_ms: started.elapsed().as_millis() as u64,
                });
            }
            std::thread::sleep((deadline - now).min(SYNC_LOCK_RETRY));
        }
    }

    // -- discover -----------------------------------------------------------

    /// One shallow catalog sweep: every local provider's sessions, read from
    /// metadata only and upserted as [`DiscoveryState::Shallow`] rows. A
    /// session whose source stamp has not moved is served from the catalog
    /// without being read. Nothing is hydrated; [`SessionStore::hydrate`]
    /// fully indexes the sessions a caller picks.
    ///
    /// Takes no `SyncRunLock`, so it never waits behind a sweep; SQLite
    /// serializes its writes. One provider failing is a
    /// [`DiscoveryReport::diagnostics`] entry, never an error for the rest.
    pub fn discover(&self, opts: DiscoveryOptions) -> Result<DiscoveryReport, Error> {
        if self.read_only {
            return Err(Error::read_only("discover"));
        }
        controlled(opts.stop.as_ref(), None, || self.discover_now(&opts))
    }

    fn discover_now(&self, opts: &DiscoveryOptions) -> Result<DiscoveryReport, Error> {
        if opts.sources.as_ref().is_some_and(Vec::is_empty) {
            return Ok(DiscoveryReport::default());
        }
        let conn =
            open_db(&self.db_path).map_err(|error| Error::DatabaseOpen(format!("{error:#}")))?;
        let env = discover::DiscoveryEnv::with_provider_roots(&conn, self.roots.clone());
        let options = discover::DiscoverOptions {
            scope: SessionScope::Local,
            sources: opts
                .sources
                .iter()
                .flatten()
                .map(|source| source.as_str().to_string())
                .collect(),
            limit: opts.limit,
        };
        let summary = discover::discover_sessions_with_env(&env, &options, |_| {})
            .map_err(|error| Error::classify(error, Error::Discovery))?;
        Ok(DiscoveryReport {
            discovered: summary.discovered,
            skipped_unchanged: summary.skipped_unchanged,
            diagnostics: summary
                .diagnostics
                .into_iter()
                .map(|diagnostic| Diagnostic {
                    code: "DISCOVERY_FAILED".into(),
                    message: format!("{}: {}", diagnostic.source, diagnostic.error),
                    subject: diagnostic.locator,
                })
                .collect(),
        })
    }

    // -- hydrate ------------------------------------------------------------

    /// Fully index one session without enumerating the rest of the provider.
    ///
    /// `SessionRef::Id` hydrates a catalogued session the way `ai-hist
    /// hydrate` does; the session must have been discovered by a sync first,
    /// else [`Error::SessionNotFound`]. `SessionRef::Path` is the hook fast
    /// path: the transcript is read by locator before any catalog row exists,
    /// and only sources with [`SourceCapabilities::hydrates_by_path`] accept
    /// it. Both take the per-session hydration locks the CLI takes.
    pub fn hydrate(&self, r: &SessionRef, opts: HydrateOptions) -> Result<HydrateReport, Error> {
        if self.read_only {
            return Err(Error::read_only("hydrate"));
        }
        controlled(opts.stop.as_ref(), None, || self.hydrate_now(r, &opts))
    }

    fn hydrate_now(&self, r: &SessionRef, opts: &HydrateOptions) -> Result<HydrateReport, Error> {
        match r {
            SessionRef::Id { source, session_id } => {
                let options = HydrateSessionOptions {
                    source: source.as_str().to_string(),
                    session_id: session_id.clone(),
                    scope: SessionScope::Local,
                    include_related: opts.include_related,
                };
                let result = hydrate_session_at_with_roots_and_connectors(
                    &self.db_path,
                    &options,
                    &self.roots,
                    &SourceConnectorSelection::default(),
                )
                .map_err(Error::hydration)?;
                let status = HydrateStatus::parse(&result.status)?;
                hydrate_report(*source, session_id.clone(), status, Some(result))
            }
            SessionRef::Path { source, path } => {
                path_names_one_session(*source)?;
                let ingest = ingest_transcript_at_with_roots(
                    &self.db_path,
                    &self.roots,
                    source.as_str(),
                    path,
                    None,
                    opts.include_related,
                )
                .map_err(Error::hydration)?;
                // The hook layer folds a first ingestion and an update into
                // one `Ingested`; the hydration it ran still says which, and
                // that is the status reported here. Only the outcomes with no
                // hydration behind them come from the hook's own vocabulary.
                let status = match (&ingest.hydration, ingest.status) {
                    (Some(hydration), _) => HydrateStatus::parse(&hydration.status)?,
                    (None, TranscriptStatus::Ingested) => HydrateStatus::Hydrated,
                    (None, TranscriptStatus::Unchanged) => HydrateStatus::Unchanged,
                    (None, TranscriptStatus::Missing) => HydrateStatus::Missing,
                    (None, TranscriptStatus::Unidentified) => HydrateStatus::Unidentified,
                    (None, TranscriptStatus::Mismatched) => HydrateStatus::Mismatched,
                };
                let session_id = ingest.session_id.clone().unwrap_or_default();
                let mut report = hydrate_report(*source, session_id, status, ingest.hydration)?;
                if report.session_id().is_empty() {
                    report.session = r.clone();
                }
                Ok(report)
            }
        }
    }

    // -- watch --------------------------------------------------------------

    /// The live-capture loop: `ai-hist watch`, as an iterator of ticks.
    ///
    /// The loop runs on its own thread and each completed sweep arrives as a
    /// [`TickReport`]; a sweep that failed arrives as an `Err` and the loop
    /// keeps running. Iteration ends after [`WatchStop::stop`] (or dropping
    /// the handle), which waits for any in-flight sweep. Every sweep is the
    /// same locked `sync` as [`SessionStore::sync`]; a tick that finds the
    /// lock held reports `contended` and is retried by the loop rather than
    /// counted as done.
    pub fn watch(&self, opts: WatchOptions) -> Result<WatchHandle, Error> {
        if self.read_only {
            return Err(Error::read_only("watch"));
        }
        let roots = sync_watch_roots_with_provider_roots(&self.roots);
        let (reports, receiver) = mpsc::channel::<Result<TickReport, Error>>();
        let reports = Arc::new(Mutex::new(reports));

        // The sweep and the report sink run on the loop's thread, one tick at
        // a time, so a single slot carries "what this tick changed" from the
        // one to the other.
        let db_path = self.db_path.clone();
        let tick_roots = self.roots.clone();
        let baseline: Arc<Mutex<Option<CatalogFingerprint>>> = Arc::new(Mutex::new(None));
        let pending: Arc<Mutex<Vec<SessionRef>>> = Arc::new(Mutex::new(Vec::new()));
        let tick_baseline = baseline.clone();
        let tick_pending = pending.clone();
        let tick: crate::watch::TickFn = Arc::new(move |force| {
            // Both reads happen under the sync lock, like `sync`'s. The
            // baseline is the previous swept tick's `after` digest when there
            // is one — nothing this loop reported has moved since, and a
            // change another process made in between is a change since the
            // last report either way — and a fresh read otherwise.
            let mut base = tick_baseline.lock().expect("watch baseline");
            let (outcome, changed) = rolling_tick(&mut base, |previous| {
                let outcome = sync_facade_tick(
                    &db_path,
                    &tick_roots,
                    force,
                    |conn| match previous {
                        Some(before) => Ok(before),
                        None => catalog_fingerprint(conn).map_err(anyhow::Error::from),
                    },
                    |conn, before, _tick| {
                        let (after, changed) = changes_under_lock(conn, &before)?;
                        Ok((after, changed))
                    },
                )?;
                Ok(outcome.map(|(tick, (after, changed))| (tick, after, changed)))
            })?;
            *tick_pending.lock().expect("watch pending") = changed;
            Ok(outcome)
        });

        let report_sink = reports.clone();
        let report_pending = pending;
        let error_sink = reports;
        let refresh_roots = self.roots.clone();
        let mut watch = WatchLoop::new(tick)
            .with_roots(roots)
            .with_fs_events(opts.use_fs_events)
            .with_debounce_ms(opts.debounce_ms)
            .with_poll_interval_ms(opts.poll_interval_ms)
            .with_slow_poll_ms(opts.slow_poll_ms)
            .with_immediate(opts.immediate)
            .on_report(Arc::new(move |report| {
                let changed = std::mem::take(&mut *report_pending.lock().expect("watch pending"));
                let _ = report_sink
                    .lock()
                    .expect("watch reports")
                    .send(Ok(TickReport {
                        trigger: report.trigger,
                        forced: report.forced,
                        swept: report.outcome.swept,
                        skipped_unchanged: report.outcome.skipped_unchanged,
                        contended: report.outcome.contended,
                        changed,
                    }));
            }))
            .on_error(Arc::new(move |error| {
                let _ = error_sink
                    .lock()
                    .expect("watch reports")
                    .send(Err(Error::sync(anyhow::anyhow!("{error:#}"))));
            }));
        watch = watch.with_roots_refresh(Arc::new(move || {
            sync_watch_roots_with_provider_roots(&refresh_roots)
        }));
        let watch = Arc::new(watch);
        let runner = watch.clone();
        let thread = std::thread::Builder::new()
            .name("ai-hist-watch".into())
            .spawn(move || {
                // The loop reports every failed tick through `on_error` and
                // never stops on one; `run` itself fails only when it cannot
                // start at all, which the thread's end (and the closed
                // channel) already reports to the iterator.
                let _ = runner.run();
            })
            .map_err(|error| Error::SyncFailed(format!("spawning the watch thread: {error}")))?;
        Ok(WatchHandle {
            stop: WatchStop { inner: watch },
            thread: Some(thread),
            receiver,
        })
    }

    // -- catalog ------------------------------------------------------------

    /// Walk the session catalog, newest first.
    ///
    /// Pure SQL over the `sessions` table: no provider I/O. Paged internally
    /// on the catalog's total order `(last_activity_ms DESC, source, session_id)`,
    /// so a page boundary inside one millisecond neither drops nor repeats a
    /// row. Every page is read on **one SQLite snapshot**, taken at the first
    /// row and held until the iterator is dropped: the order key is
    /// `last_activity_ms`, which a concurrent sync moves, and pages read on
    /// separate snapshots would skip a session that gained activity behind
    /// the cursor and repeat one that lost it. A WAL reader blocks no
    /// writer, but it does pin the WAL until it ends, so drain or drop the
    /// iterator promptly rather than holding it across a long-lived host
    /// loop. Each item is one row or the error that stopped the walk.
    pub fn sessions(&self, q: CatalogQuery) -> CatalogIter {
        // `Some(vec![])` is an allowlist that admits nothing, which is not the
        // same request as `None`; the internal filter spells "no filter" as
        // an empty list, so the distinction has to be kept here.
        let nothing_allowed = q.sources.as_ref().is_some_and(Vec::is_empty);
        let options = CatalogListOptions {
            scope: q.scope,
            sources: q
                .sources
                .unwrap_or_default()
                .into_iter()
                .map(|source| source.as_str().to_string())
                .collect(),
            limit: Some(q.page_size.clamp(1, 1_000)),
            before_ms: q.before_ms,
            after: None,
            project_key: q.project_key,
        };
        CatalogIter {
            conn: self.read_conn(),
            options,
            buffer: VecDeque::new(),
            cursor: None,
            exhausted: nothing_allowed,
            snapshot_open: false,
        }
    }

    // -- one session --------------------------------------------------------

    /// Everything the store holds about one session, typed, or `None` when
    /// the catalog has no such session.
    ///
    /// Every table is read on one SQLite snapshot, so a sync landing halfway
    /// through cannot hand back tool calls from a newer version of the
    /// session than its messages. `SessionQuery::kinds` skips the tables a
    /// consumer does not need; `include_text: false` leaves every transcript
    /// string `None` while keeping byte lengths and hashes, and the event
    /// query then does not move the text column at all.
    pub fn session(
        &self,
        r: &SessionRef,
        opts: SessionQuery,
    ) -> Result<Option<SessionEvidence>, Error> {
        let conn = self.read_conn()?;
        let tx = conn.unchecked_transaction().map_err(Error::sql)?;
        let source = r.source();
        let name = source.as_str();
        let include_text = opts.include_text;
        let row = match r {
            SessionRef::Id { session_id, .. } => {
                discover::catalog_row(&tx, name, session_id, include_text)
            }
            SessionRef::Path { path, .. } => {
                // A path names one session only where the provider keeps one
                // session per file. OpenCode's rows all carry the provider
                // database as their locator, so a lookup by it would answer
                // with whichever session sorts first — well-formed and wrong.
                path_names_one_session(source)?;
                discover::catalog_row_by_path(&tx, name, &path.to_string_lossy(), include_text)
            }
        }
        .map_err(Error::query)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let session_id = row.session_id.clone();
        // What this read fetches is the source's declared coverage narrowed
        // by the query, in the coverage's canonical order — never a kind the
        // source cannot produce, so `loaded` is always within `coverage` and
        // an empty list for a covered kind means the session has none of it.
        let coverage = source.capabilities().evidence_kinds;
        let selected: Vec<EvidenceKind> = coverage
            .iter()
            .copied()
            .filter(|kind| opts.kinds.as_ref().is_none_or(|kinds| kinds.contains(kind)))
            .collect();
        let wants = |kind: EvidenceKind| selected.contains(&kind);
        let mut diagnostics = Vec::new();

        let mut evidence = SessionEvidence {
            session: CatalogSession::from_row(row)?,
            prompts: Vec::new(),
            messages: Vec::new(),
            tool_calls: Vec::new(),
            tool_results: Vec::new(),
            file_edits: Vec::new(),
            markers: Vec::new(),
            relationships: Vec::new(),
            requests: Vec::new(),
            usage: None,
            user_turns: Vec::new(),
            coverage,
            loaded: Vec::new(),
            include_text,
            diagnostics: Vec::new(),
        };

        if wants(EvidenceKind::History) {
            evidence.prompts = session_prompts_sized(&tx, name, &session_id, include_text)
                .map_err(Error::query)?
                .into_iter()
                .map(Prompt::from_row)
                .collect();
        }
        if wants(EvidenceKind::SessionEvent) {
            let events =
                session_events_sized(&tx, name, &session_id, include_text).map_err(Error::query)?;
            evidence.messages = group_messages(source, &events);
            evidence.tool_results = events
                .iter()
                .filter(|(event, _)| event.kind == "tool_result")
                .map(|(event, bytes)| ToolResult::from_event(event, *bytes))
                .collect();
            evidence.user_turns = all_user_turns(&tx, name, &session_id)?;
            evidence.requests = all_requests(&tx, name, &session_id)?;
            evidence.usage = session_usage_summary(&tx, name, &session_id).map_err(Error::query)?;
        }
        if wants(EvidenceKind::ToolCall) {
            evidence.tool_calls = session_tool_calls(&tx, &session_id, Some(name))
                .map_err(Error::query)?
                .into_iter()
                .map(ToolCall::from_row)
                .collect();
        }
        if wants(EvidenceKind::FileEdit) {
            evidence.file_edits = session_file_edits(&tx, &session_id, Some(name))
                .map_err(Error::query)?
                .into_iter()
                .map(FileEdit::from_row)
                .collect();
        }
        if wants(EvidenceKind::SessionMarker) {
            evidence.markers = session_markers_sized(&tx, name, &session_id, include_text)
                .map_err(Error::query)?
                .into_iter()
                .map(Marker::from_row)
                .collect();
        }
        if wants(EvidenceKind::Relationship) {
            let graph = relationship_graph::session_relationships(&tx, name, &session_id)
                .map_err(Error::query)?;
            let mut relationships = Vec::new();
            for (side, rows) in [
                (RelationshipSide::Parent, graph.as_parent),
                (RelationshipSide::Child, graph.as_child),
                (RelationshipSide::Continuity, graph.continuity),
            ] {
                relationships.extend(
                    rows.into_iter()
                        .map(|row| Relationship::from_row(row, side)),
                );
            }
            evidence.relationships = relationships;
            diagnostics.extend(graph.diagnostics.into_iter().map(|diagnostic| Diagnostic {
                code: diagnostic.code,
                message: diagnostic.message,
                subject: diagnostic.relationship_uid,
            }));
        }
        evidence.loaded = selected;
        evidence.diagnostics = diagnostics;
        Ok(Some(evidence))
    }
}

/// The catalog digest at the end of a locked section, and what moved since
/// `before` — taken on **every** locked tick, swept or not.
///
/// A hydration does not take the sync lock, so it can commit a catalog row
/// while a sweep is deciding, from an unchanged source fingerprint, that
/// there is nothing to do. Skipping the after-digest on such a tick would
/// leave that row unreported for as long as the sources stay quiet; the
/// digest costs one indexed scan of the catalog, which an unswept tick can
/// afford.
fn changes_under_lock(
    conn: &Connection,
    before: &CatalogFingerprint,
) -> Result<(CatalogFingerprint, Vec<SessionRef>), Error> {
    let after = catalog_fingerprint(conn)?;
    let changed = catalog_changes(before, &after);
    Ok((after, changed))
}

/// One watch tick's bookkeeping around a bracketed sweep: hand the sweep the
/// rolling baseline, and roll it forward when the sweep came back.
///
/// `sweep` receives the previous tick's digest (or `None` for a fresh read
/// under the lock) and answers with the `after` digest it took under the
/// lock and what moved; `None` means the lock was held elsewhere. Every
/// locked tick rolls the baseline forward, swept or not, so a catalog write
/// between ticks is reported once by the next tick and never again. The
/// baseline is *cloned* into the sweep rather than taken: a sweep that
/// fails part-way has usually committed some of its rows already, and a
/// baseline lost with it would make the next tick start afresh and never
/// report them. On failure `base` is exactly what it was, so the next
/// successful tick diffs against the last digest that was reported.
fn rolling_tick(
    base: &mut Option<CatalogFingerprint>,
    sweep: impl FnOnce(
        Option<CatalogFingerprint>,
    ) -> anyhow::Result<Option<(SyncTick, CatalogFingerprint, Vec<SessionRef>)>>,
) -> anyhow::Result<(TickOutcome, Vec<SessionRef>)> {
    match sweep(base.clone())? {
        None => Ok((TickOutcome::from(SyncTick::default()), Vec::new())),
        Some((tick, after, changed)) => {
            *base = Some(after);
            Ok((TickOutcome::from(tick), changed))
        }
    }
}

/// Whether a [`SessionRef::Path`] can name exactly one session of `source`
/// — the same rule for reading and for hydrating, with the same error.
fn path_names_one_session(source: Source) -> Result<(), Error> {
    if source.capabilities().hydrates_by_path {
        return Ok(());
    }
    Err(Error::HydrationUnsupported(format!(
        "{source} sessions cannot be named by path; known harnesses: {}",
        HOOK_HARNESSES.join(", ")
    )))
}

/// How often a held sync lock is re-tried while a caller's timeout runs.
const SYNC_LOCK_RETRY: Duration = Duration::from_millis(100);

/// The longest [`SyncOptions::lock_timeout_ms`] is honoured for: seven days,
/// the same ceiling the watch intervals have.
const MAX_LOCK_WAIT_MS: u64 = crate::watch::MAX_INTERVAL_MS;

fn resolve_db_path(opts: &StoreOptions) -> PathBuf {
    if let Some(path) = &opts.db_path {
        return path.clone();
    }
    match &opts.home {
        None => default_db_path(),
        Some(home) => home.join(".local/share/ai-hist/ai-history.db"),
    }
}

// ---------------------------------------------------------------------------
// sync
// ---------------------------------------------------------------------------

/// Cooperative stop for [`SessionStore::discover`], [`SessionStore::sync`]
/// and [`SessionStore::hydrate`]. Clones share one flag, so a token handed to
/// a call on one thread is stopped from another.
///
/// A stopped call returns [`Error::Cancelled`] at the next provider, file or
/// record boundary. Committed chunks stay; the unfinished transaction rolls
/// back and the next call resumes from its checkpoint.
#[derive(Debug, Clone, Default)]
pub struct StopToken(Arc<AtomicBool>);

impl StopToken {
    pub fn new() -> Self {
        Self::default()
    }

    /// Stop every call holding this token or a clone of it. Idempotent.
    pub fn stop(&self) {
        self.0.store(true, Ordering::SeqCst);
    }

    pub fn is_stopped(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

/// Receives content-free [`CaptureProgress`] — a source name and file
/// counts, never paths or session contents — while a sweep reads files.
/// Called on the thread running the sweep.
#[derive(Clone)]
pub struct ProgressObserver(Arc<dyn Fn(CaptureProgress) + Send + Sync>);

impl ProgressObserver {
    pub fn new(observer: impl Fn(CaptureProgress) + Send + Sync + 'static) -> Self {
        Self(Arc::new(observer))
    }
}

impl fmt::Debug for ProgressObserver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProgressObserver")
    }
}

/// Run `work` with the caller's stop and progress installed for this thread.
fn controlled<T>(
    stop: Option<&StopToken>,
    progress: Option<&ProgressObserver>,
    work: impl FnOnce() -> Result<T, Error>,
) -> Result<T, Error> {
    let stop = stop.cloned();
    let stoppable = move || match stop {
        Some(token) => with_capture_token(token, || Ok(work())),
        None => Ok(work()),
    };
    let outcome = match progress.cloned() {
        Some(observer) => with_capture_observer(move |value| (observer.0)(value), stoppable),
        None => stoppable(),
    };
    outcome.map_err(Error::sync)?
}

/// How to run a shallow catalog sweep.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DiscoveryOptions {
    /// Restrict to these sources. `None` means every local source;
    /// `Some(vec![])` admits none and reads nothing.
    pub sources: Option<Vec<Source>>,
    /// Cap on rows read, newest first across providers. `None` (the default)
    /// reads the whole catalog; a caller repeating a capped sweep only ever
    /// sees the same newest sessions.
    pub limit: Option<usize>,
    /// Stops the sweep at the next provider or file boundary. See
    /// [`StopToken`].
    #[serde(skip)]
    pub stop: Option<StopToken>,
}

/// Result of [`SessionStore::discover`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DiscoveryReport {
    /// Sessions read from provider metadata and upserted.
    pub discovered: usize,
    /// Sessions whose source stamp had not moved, served from the catalog.
    pub skipped_unchanged: usize,
    /// Providers or sessions that could not be read.
    pub diagnostics: Vec<Diagnostic>,
}

/// How to run a local sweep.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SyncOptions {
    /// Bypass the stat-only source fingerprint and walk every provider even
    /// when nothing appears to have moved. The watch loop sets it for
    /// filesystem-event ticks; a caller repairing a store it does not trust
    /// sets it too.
    pub force: bool,
    /// How long to wait for another process's `SyncRunLock` before returning
    /// [`Error::SyncLocked`]. `0` (the default) tries once. The lock is
    /// re-tried every 100 ms while the budget lasts; a budget above seven
    /// days is treated as seven days.
    pub lock_timeout_ms: u64,
    /// Stops the sweep at the next provider, file or record boundary, and
    /// ends a wait for the lock. See [`StopToken`].
    #[serde(skip)]
    pub stop: Option<StopToken>,
    /// Receives [`CaptureProgress`] as the sweep reads each provider's files.
    /// OpenCode counts its SQLite database as one file, or one session file
    /// per session in the legacy JSON tree. Unchanged files count as processed.
    #[serde(skip)]
    pub progress: Option<ProgressObserver>,
}

/// Result of [`SessionStore::sync`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SyncReport {
    /// A full walk ran. `false` when the stat-only source fingerprint matched
    /// the previous sweep's and nothing was opened. `changed` is still
    /// compared on such a call: a hydration can write the catalog while the
    /// fingerprint is being checked.
    pub swept: bool,
    /// Sessions whose catalog row was created or changed while this call
    /// held the `SyncRunLock`, whether or not it swept.
    ///
    /// Derived from a digest of the `sessions` table taken after the lock was
    /// acquired and again before it was released, not from the provider
    /// walk: every catalog column takes part except the two bounded text
    /// excerpts (`first_prompt`, `last_assistant_text`), so a new session,
    /// new activity, a moved source stamp or discovery state, a re-resolved
    /// or inherited `project_key`, and a metadata field the shallow read
    /// filled in are all changes here. Another process's sync cannot be
    /// counted — it holds the same lock — but a hydration writes the catalog
    /// outside it, so a `hydrate` that lands inside the window is included
    /// even on an unswept call; one that landed before the lock was taken
    /// belongs to no `sync` report ([`SessionStore::watch`] reports it on
    /// the next tick, and per-row attribution is what the change feed's
    /// `revision` stamp is for — see [`SessionStore::changes_since`]). A
    /// session whose only change was inside a table the catalog row does not
    /// summarise is not listed.
    pub changed: Vec<SessionRef>,
    /// The store's change-feed head after this sweep: the revision of the
    /// newest stamped row. [`SessionStore::head_revision`] reports it with
    /// the store's epoch, and [`SessionStore::changes_since`] is what judges
    /// whether a stored watermark can resume.
    pub head_revision: u64,
}

/// One digest per catalog row over every column [`SyncReport::changed`]
/// covers, keyed by `(source, session_id)`.
///
/// A digest rather than the values: the point is to notice that a row moved,
/// not to keep two copies of the catalog, and a fixed 32 bytes per row makes
/// the before/after maps the same size whatever the row holds. Once every
/// catalog write stamps a revision column (the change feed's `revision`), this
/// collapses to reading that one column.
type CatalogFingerprint = BTreeMap<(String, String), [u8; 32]>;

/// The catalog columns the digest covers: everything a sweep, shallow
/// discovery or the identity refresh can write, except the two text
/// excerpts, which are the only columns whose size is not a few bytes.
const CATALOG_FINGERPRINT_COLUMNS: &str = "source_stamp, last_activity_ms, first_activity_ms, \
     discovery_state, parser_version, project_key, project_key_method, cwd, git_branch, \
     raw_path, models_json, originator, agent_version, repo_url, initial_commit, \
     workspace_roots_json";

fn catalog_fingerprint(conn: &Connection) -> Result<CatalogFingerprint, Error> {
    use sha2::{Digest, Sha256};
    let mut stmt = conn
        .prepare(&format!(
            "SELECT source, session_id, {CATALOG_FINGERPRINT_COLUMNS} FROM sessions"
        ))
        .map_err(Error::sql)?;
    let columns = stmt.column_count();
    let rows = stmt
        .query_map([], |row| {
            let mut digest = Sha256::new();
            for index in 2..columns {
                // Type and value both, with a separator, so a NULL and an
                // empty string differ and a shifted column boundary cannot
                // read as the same row.
                match row.get_ref(index)? {
                    rusqlite::types::ValueRef::Null => digest.update(b"n|"),
                    rusqlite::types::ValueRef::Integer(value) => {
                        digest.update(b"i");
                        digest.update(value.to_le_bytes());
                        digest.update(b"|");
                    }
                    rusqlite::types::ValueRef::Real(value) => {
                        digest.update(b"r");
                        digest.update(value.to_le_bytes());
                        digest.update(b"|");
                    }
                    rusqlite::types::ValueRef::Text(bytes)
                    | rusqlite::types::ValueRef::Blob(bytes) => {
                        digest.update(b"t");
                        digest.update((bytes.len() as u64).to_le_bytes());
                        digest.update(bytes);
                        digest.update(b"|");
                    }
                }
            }
            Ok((
                (row.get::<_, String>(0)?, row.get::<_, String>(1)?),
                digest.finalize().into(),
            ))
        })
        .map_err(Error::sql)?;
    rows.collect::<Result<CatalogFingerprint, _>>()
        .map_err(Error::sql)
}

fn catalog_changes(before: &CatalogFingerprint, after: &CatalogFingerprint) -> Vec<SessionRef> {
    after
        .iter()
        .filter(|(key, stamp)| before.get(*key) != Some(*stamp))
        .filter_map(|((source, session_id), _)| {
            Source::parse(source).map(|source| SessionRef::id(source, session_id.clone()))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// hydrate
// ---------------------------------------------------------------------------

/// How to hydrate one session.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct HydrateOptions {
    /// Also hydrate the session's bounded related transcripts — Claude
    /// subagent sidecars beside it, Codex child rollouts. Defaults to `true`.
    pub include_related: bool,
    /// Stops the hydration at the next file or record boundary. See
    /// [`StopToken`].
    #[serde(skip)]
    pub stop: Option<StopToken>,
}

impl Default for HydrateOptions {
    fn default() -> Self {
        Self {
            include_related: true,
            stop: None,
        }
    }
}

/// What a hydration decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum HydrateStatus {
    /// Evidence was read from the provider into the store for the first
    /// time — no hydration checkpoint existed for the session.
    Hydrated,
    /// The provider source had changed since the last hydration and the new
    /// evidence was read on top of the checkpoint.
    Updated,
    /// The provider source had not changed since it was last hydrated.
    Unchanged,
    /// The source's adapter cannot produce full evidence; the catalog row is
    /// all there is.
    CapabilityLimited,
    /// `SessionRef::Path` only: the transcript is gone, or was never written.
    Missing,
    /// `SessionRef::Path` only: the file carries no session identity yet — a
    /// subagent sidecar, or a transcript whose first records are still being
    /// written.
    Unidentified,
    /// `SessionRef::Path` only: the transcript names another session than the
    /// caller claimed. Nothing was ingested.
    Mismatched,
}

impl HydrateStatus {
    /// Every status the engine's local hydration path emits is named here;
    /// an unknown one can only come from a genuinely new engine value, and
    /// is reported rather than guessed at.
    fn parse(value: &str) -> Result<Self, Error> {
        match value {
            "hydrated" => Ok(Self::Hydrated),
            "updated" => Ok(Self::Updated),
            "unchanged" => Ok(Self::Unchanged),
            "capability_limited" => Ok(Self::CapabilityLimited),
            other => Err(Error::HydrationFailed(format!(
                "hydration reported an unknown status `{other}`"
            ))),
        }
    }
}

/// How much of a session's declared evidence a hydration can have indexed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    /// Every kind in the full-session set is covered.
    Full,
    /// Some declared kinds are covered; `coverage` says which.
    Partial,
    /// Only the catalog row; the source has no full-evidence path.
    ShallowOnly,
}

impl Capability {
    fn parse(value: &str) -> Result<Self, Error> {
        match value {
            "full" => Ok(Self::Full),
            "partial" => Ok(Self::Partial),
            "shallow_only" => Ok(Self::ShallowOnly),
            other => Err(Error::HydrationFailed(format!(
                "hydration reported an unknown capability `{other}`"
            ))),
        }
    }
}

/// Result of [`SessionStore::hydrate`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct HydrateReport {
    /// The session, by id once one is known. For a `Path` reference whose
    /// transcript carried no identity this is the path reference back.
    pub session: SessionRef,
    pub status: HydrateStatus,
    /// `None` when no hydration ran (`Missing`, `Unidentified`,
    /// `Mismatched`).
    pub capability: Option<Capability>,
    /// The evidence kinds this hydration can have indexed: the source's
    /// [`SourceCapabilities::evidence_kinds`] — the same set
    /// [`SessionEvidence::coverage`] reports — narrowed by the request
    /// (`Relationship` is absent when `include_related` is off, or when a
    /// bounded Codex child search could not cover it). A zero count for a
    /// covered kind means the session has none of it. `capability` is the
    /// engine's own classification and is not derived from this list.
    pub coverage: Vec<EvidenceKind>,
    /// Related sessions hydrated alongside this one.
    pub related: Vec<SessionRef>,
    /// Bytes read from provider files. Small but not zero for an `Unchanged`
    /// pass — deciding nothing changed means validating each cursor.
    pub bytes_read: i64,
    pub diagnostics: Vec<Diagnostic>,
}

impl HydrateReport {
    fn session_id(&self) -> &str {
        match &self.session {
            SessionRef::Id { session_id, .. } => session_id,
            SessionRef::Path { .. } => "",
        }
    }
}

fn hydrate_report(
    source: Source,
    session_id: String,
    status: HydrateStatus,
    result: Option<HydrateSessionResult>,
) -> Result<HydrateReport, Error> {
    let session = SessionRef::id(source, session_id);
    let Some(result) = result else {
        return Ok(HydrateReport {
            session,
            status,
            capability: None,
            coverage: Vec::new(),
            related: Vec::new(),
            bytes_read: 0,
            diagnostics: Vec::new(),
        });
    };
    // The engine's coverage comes from the adapter declaration, which leaves
    // parser-derived markers out on purpose (a connector cannot supply them);
    // the facade's contract is the capability declaration `session()` also
    // reports. Markers are written by the same parse that writes events, so
    // they are covered exactly when events are; everything else follows the
    // engine's narrowing (`include_related`, incomplete Codex child search).
    let engine = &result.coverage;
    let coverage = source
        .capabilities()
        .evidence_kinds
        .into_iter()
        .filter(|kind| {
            engine.contains(kind)
                || (*kind == EvidenceKind::SessionMarker
                    && engine.contains(&EvidenceKind::SessionEvent))
        })
        .collect();
    Ok(HydrateReport {
        session,
        status,
        capability: Some(Capability::parse(&result.capability)?),
        coverage,
        related: result
            .related_session_ids
            .into_iter()
            .map(|id| SessionRef::id(source, id))
            .collect(),
        bytes_read: result.bytes_read,
        diagnostics: result
            .diagnostics
            .into_iter()
            .map(|diagnostic| Diagnostic {
                code: diagnostic.code,
                message: diagnostic.message,
                subject: None,
            })
            .collect(),
    })
}

/// Something a read or a hydration noticed that is not an error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Diagnostic {
    /// Stable code, e.g. `RELATIONSHIP_UNLINKED_CHILD`, `EVIDENCE_PARTIAL`.
    pub code: String,
    pub message: String,
    /// What the diagnostic is about, when it names one thing: a
    /// `relationship_uid`, a locator.
    pub subject: Option<String>,
}

// ---------------------------------------------------------------------------
// watch
// ---------------------------------------------------------------------------

/// How to run the live-capture loop.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct WatchOptions {
    /// Coalescing window for a burst of filesystem events. Default 200 ms.
    pub debounce_ms: u64,
    /// Cadence when polling, i.e. when no root can be watched or
    /// `use_fs_events` is false. Default 1 s.
    pub poll_interval_ms: u64,
    /// Slow backstop while filesystem events drive the loop. Default 30 s.
    pub slow_poll_ms: u64,
    /// Try the filesystem-event driver. Without the crate's `fs-events`
    /// feature the loop polls whatever this says. Default `true`.
    pub use_fs_events: bool,
    /// Run one sweep before parking. Default `true`.
    pub immediate: bool,
}

impl Default for WatchOptions {
    fn default() -> Self {
        Self {
            debounce_ms: crate::watch::DEFAULT_DEBOUNCE_MS,
            poll_interval_ms: crate::watch::DEFAULT_POLL_INTERVAL_MS,
            slow_poll_ms: crate::watch::DEFAULT_SLOW_POLL_MS,
            use_fs_events: true,
            immediate: true,
        }
    }
}

/// One completed sweep of a [`WatchHandle`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct TickReport {
    pub trigger: TickTrigger,
    /// The sweep bypassed the source fingerprint (a filesystem-event tick).
    pub forced: bool,
    /// A full walk ran.
    pub swept: bool,
    /// The walk was skipped because no source had moved.
    pub skipped_unchanged: bool,
    /// Another process held the sync lock; nothing was read. A forced tick
    /// that comes back this way is retried by the loop.
    pub contended: bool,
    /// Sessions whose catalog row changed since the previous tick's report;
    /// see [`SyncReport::changed`]. Compared on every tick that took the
    /// lock, swept or not, so a hydration landing between ticks is reported
    /// once by the tick that follows it. Empty on a `contended` tick.
    pub changed: Vec<SessionRef>,
}

/// Stops a running [`WatchHandle`] from any thread.
#[derive(Clone)]
pub struct WatchStop {
    inner: Arc<WatchLoop>,
}

impl WatchStop {
    /// Stop the loop and wait for any in-flight sweep. Idempotent.
    pub fn stop(&self) {
        self.inner.stop();
    }

    /// Run one sweep now, or wait for the one in flight.
    pub fn tick(&self) {
        self.inner.tick();
    }

    /// The driver the loop is using, once it has started.
    pub fn driver(&self) -> Option<WatchDriver> {
        self.inner.driver()
    }
}

impl fmt::Debug for WatchStop {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WatchStop")
            .field("driver", &self.driver())
            .finish()
    }
}

/// A running live-capture loop. Iterate it for ticks; drop it to stop.
#[derive(Debug)]
pub struct WatchHandle {
    stop: WatchStop,
    thread: Option<std::thread::JoinHandle<()>>,
    receiver: Receiver<Result<TickReport, Error>>,
}

impl WatchHandle {
    /// A cloneable stopper for another thread — the iterator itself is
    /// consumed by `next`.
    pub fn stopper(&self) -> WatchStop {
        self.stop.clone()
    }

    /// Stop the loop and wait for any in-flight sweep. Ticks already
    /// reported remain readable through the iterator.
    pub fn stop(&self) {
        self.stop.stop();
    }

    /// The driver the loop is using, once it has started.
    pub fn driver(&self) -> Option<WatchDriver> {
        self.stop.driver()
    }

    /// The next tick, or `None` once the loop has stopped and every reported
    /// tick has been read; waits at most `timeout` for one. A `timeout` too
    /// large to name an instant (`Duration::MAX`) waits without a deadline,
    /// which is what such a value means, rather than panicking.
    pub fn next_timeout(&mut self, timeout: Duration) -> Option<Result<TickReport, Error>> {
        self.recv(Instant::now().checked_add(timeout))
    }

    /// Receive until `deadline` (or forever), ending when the loop's thread
    /// has finished and nothing is left to read. The loop's sinks hold the
    /// sender for as long as the loop exists, so the channel alone cannot
    /// say that the loop is over; the thread can.
    fn recv(&mut self, deadline: Option<Instant>) -> Option<Result<TickReport, Error>> {
        loop {
            // Never sleep past a short deadline: the poll is the ceiling on
            // one wait, and the remaining budget the floor under it.
            let wait = deadline.map_or(WATCH_POLL, |deadline| {
                deadline
                    .saturating_duration_since(Instant::now())
                    .min(WATCH_POLL)
            });
            match self.receiver.recv_timeout(wait) {
                Ok(item) => return Some(item),
                Err(mpsc::RecvTimeoutError::Disconnected) => return None,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if self
                        .thread
                        .as_ref()
                        .is_none_or(|thread| thread.is_finished())
                    {
                        return self.receiver.try_recv().ok();
                    }
                    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                        return None;
                    }
                }
            }
        }
    }
}

/// How often a blocked `next` re-checks whether the loop has ended.
const WATCH_POLL: Duration = Duration::from_millis(50);

impl Iterator for WatchHandle {
    type Item = Result<TickReport, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.recv(None)
    }
}

impl Drop for WatchHandle {
    fn drop(&mut self) {
        self.stop.stop();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

// ---------------------------------------------------------------------------
// catalog
// ---------------------------------------------------------------------------

/// Which catalog rows [`SessionStore::sessions`] walks.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CatalogQuery {
    /// Which presences to include. `Local` (the default) is what `ai-hist
    /// sessions list` shows; `All` adds sessions known only remotely.
    pub scope: SessionScope,
    /// Restrict to these sources. `None` means every source; `Some(vec![])`
    /// is an allowlist that admits none and yields no rows.
    pub sources: Option<Vec<Source>>,
    /// Restrict to one canonical project identity, exactly as
    /// [`CatalogSession::project_key`] spells it.
    pub project_key: Option<String>,
    /// Only sessions whose last activity is strictly before this millisecond.
    pub before_ms: Option<i64>,
    /// Rows fetched per internal page. Default 200; clamped to 1..=1000.
    pub page_size: i64,
}

impl Default for CatalogQuery {
    fn default() -> Self {
        Self {
            scope: SessionScope::Local,
            sources: None,
            project_key: None,
            before_ms: None,
            page_size: 200,
        }
    }
}

/// `shallow` (catalog row only) or `full` (full evidence ingested).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryState {
    Shallow,
    Full,
}

/// One catalog row. The fields are the provider's observed session metadata
/// plus the two derived ones the crate computes (`first_prompt`,
/// `project_key`); `docs/session-catalog.md` says which provider fills which.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CatalogSession {
    pub source: Source,
    pub session_id: String,
    /// Working directory the provider reported. Every source but relay.
    pub cwd: Option<String>,
    /// Git branch, last observed. Claude, codex, grok.
    pub git_branch: Option<String>,
    pub first_activity_ms: Option<i64>,
    pub last_activity_ms: Option<i64>,
    /// Bounded excerpt of the first substantive human prompt. Derived.
    /// `None` when the query asked for no text.
    pub first_prompt: Option<String>,
    /// Bounded excerpt of the last assistant text. `None` when the query
    /// asked for no text.
    pub last_assistant_text: Option<String>,
    /// Model ids seen in the bounded read; best effort.
    pub models: Vec<String>,
    /// Client that originated the session. Codex.
    pub originator: Option<String>,
    /// Agent CLI version. Claude, codex.
    pub agent_version: Option<String>,
    /// Repository remote. Codex.
    pub repo_url: Option<String>,
    /// Commit the session started from. Codex.
    pub initial_commit: Option<String>,
    /// Extra workspace roots. Codex.
    pub workspace_roots: Vec<String>,
    /// Canonical project identity: `host/owner/repo`, or the working
    /// directory when no remote resolves. Derived; `None` only while the
    /// row has neither a `cwd` nor a `repo_url`.
    pub project_key: Option<String>,
    /// `remote`, `path` or `inherited` — how `project_key` was arrived at.
    pub project_key_method: Option<String>,
    /// The provider file or database this row came from, when local.
    pub raw_path: Option<PathBuf>,
    /// Change stamp of the raw source at scan time.
    pub source_stamp: Option<String>,
    pub discovery_state: DiscoveryState,
    /// Where this session has been observed: `local`, `remote`, or both.
    pub locations: Vec<String>,
}

impl CatalogSession {
    fn from_row(row: ShallowSession) -> Result<Self, Error> {
        let source = Source::parse(&row.source).ok_or_else(|| {
            Error::Query(format!(
                "catalog row {}/{} names a source this build does not know",
                row.source, row.session_id
            ))
        })?;
        Ok(Self {
            source,
            session_id: row.session_id,
            cwd: row.cwd,
            git_branch: row.git_branch,
            first_activity_ms: row.first_activity_ms,
            last_activity_ms: row.last_activity_ms,
            first_prompt: row.first_prompt,
            last_assistant_text: row.last_assistant_text,
            models: row.models,
            originator: row.originator,
            agent_version: row.agent_version,
            repo_url: row.repo_url,
            initial_commit: row.initial_commit,
            workspace_roots: row.workspace_roots,
            project_key: row.project_key,
            project_key_method: row.project_key_method,
            raw_path: row.raw_path.map(PathBuf::from),
            source_stamp: row.source_stamp,
            discovery_state: if row.discovery_state == "shallow" {
                DiscoveryState::Shallow
            } else {
                DiscoveryState::Full
            },
            locations: row.locations,
        })
    }

    /// This row as a [`SessionRef`].
    pub fn session_ref(&self) -> SessionRef {
        SessionRef::id(self.source, self.session_id.clone())
    }
}

/// The iterator [`SessionStore::sessions`] returns. Holds one read snapshot
/// of the catalog from its first row until it is dropped.
pub struct CatalogIter {
    conn: Result<Connection, Error>,
    options: CatalogListOptions,
    buffer: VecDeque<ShallowSession>,
    cursor: Option<CatalogCursor>,
    exhausted: bool,
    /// A deferred read transaction is open on `conn`, pinning the snapshot
    /// every page reads. Begun by hand rather than through
    /// `unchecked_transaction` because that guard borrows the connection it
    /// lives beside, and this struct owns both.
    snapshot_open: bool,
}

impl CatalogIter {
    fn fill(&mut self) -> Result<(), Error> {
        let conn = match &self.conn {
            Ok(conn) => conn,
            Err(error) => return Err(error.clone()),
        };
        if !self.snapshot_open {
            conn.execute_batch("BEGIN DEFERRED").map_err(Error::sql)?;
            self.snapshot_open = true;
        }
        self.options.after = self.cursor.take();
        let page = list_session_catalog_page(conn, &self.options).map_err(Error::query)?;
        self.exhausted = page.next_cursor.is_none();
        self.cursor = page.next_cursor;
        self.buffer.extend(page.sessions);
        Ok(())
    }
}

impl Drop for CatalogIter {
    fn drop(&mut self) {
        // Ends the read snapshot explicitly; closing the connection would
        // roll it back anyway, and a read-only transaction has nothing to
        // commit, but leaving it to the close is how a snapshot outlives the
        // iterator by however long the close takes.
        if self.snapshot_open {
            if let Ok(conn) = &self.conn {
                let _ = conn.execute_batch("ROLLBACK");
            }
        }
    }
}

impl fmt::Debug for CatalogIter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CatalogIter")
            .field("buffered", &self.buffer.len())
            .field("exhausted", &self.exhausted)
            .finish()
    }
}

impl Iterator for CatalogIter {
    type Item = Result<CatalogSession, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.is_empty() {
            if self.exhausted {
                return None;
            }
            if let Err(error) = self.fill() {
                self.exhausted = true;
                return Some(Err(error));
            }
        }
        self.buffer.pop_front().map(CatalogSession::from_row)
    }
}

// ---------------------------------------------------------------------------
// one session
// ---------------------------------------------------------------------------

/// What [`SessionStore::session`] reads.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SessionQuery {
    /// Carry transcript text. `false` leaves every text field `None` and
    /// keeps byte lengths and hashes — burn's hash-only and off content
    /// modes. Default `true`.
    pub include_text: bool,
    /// Only these evidence kinds; `None` means all of them. Each kind names
    /// the tables it loads: `SessionEvent` is messages, tool results, user
    /// turns, requests and the usage summary together; `History` the prompts;
    /// the rest their own table.
    pub kinds: Option<Vec<EvidenceKind>>,
}

impl Default for SessionQuery {
    fn default() -> Self {
        Self {
            include_text: true,
            kinds: None,
        }
    }
}

/// Everything the store holds about one session.
///
/// Which source fills which field is documented on each struct; the
/// per-source ceiling is [`SourceCapabilities::evidence_kinds`], repeated
/// here as `coverage`. `loaded` is what this read actually fetched, i.e.
/// `coverage` intersected with the query's `kinds`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SessionEvidence {
    pub session: CatalogSession,
    /// The `history` rows: one per human prompt. Every source.
    pub prompts: Vec<Prompt>,
    /// One per `message_id`, in transcript order. Claude, codex, cursor,
    /// grok, opencode.
    pub messages: Vec<Message>,
    /// Claude, codex, cursor, grok, opencode.
    pub tool_calls: Vec<ToolCall>,
    /// The `tool_result` events with their measured fidelity. Claude and
    /// codex measure; other sources carry `None` for every fidelity field.
    pub tool_results: Vec<ToolResult>,
    /// Claude, codex, cursor, grok, opencode.
    pub file_edits: Vec<FileEdit>,
    /// Compaction and summary boundaries, provider system rows, non-text
    /// blocks, lifecycle events. Claude, codex, grok, opencode.
    pub markers: Vec<Marker>,
    /// Delegation and continuity edges touching this session.
    pub relationships: Vec<Relationship>,
    /// One per model request, usage normalized and grouped by the source's
    /// own request identity. Claude, codex, opencode.
    pub requests: Vec<SessionRequest>,
    /// The whole-session usage rollup, or `None` when no request was
    /// recorded.
    pub usage: Option<SessionUsageSummary>,
    /// Human turns with their ordered blocks and per-block byte accounting.
    /// Claude, codex.
    pub user_turns: Vec<SessionUserTurn>,
    /// What the source's parser can produce at all.
    pub coverage: Vec<EvidenceKind>,
    /// What this read fetched.
    pub loaded: Vec<EvidenceKind>,
    /// Whether text fields were carried.
    pub include_text: bool,
    pub diagnostics: Vec<Diagnostic>,
}

/// One `history` row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Prompt {
    /// `None` when the query asked for no text.
    pub prompt: Option<String>,
    /// sha256 of the prompt, hex: the hash the ledger stored, or one computed
    /// from the text when it was carried. `None` only for a row written
    /// without a stored hash and read without its text.
    pub prompt_hash: Option<String>,
    pub prompt_bytes: i64,
    pub project: Option<String>,
    pub timestamp_ms: i64,
}

impl Prompt {
    fn from_row(row: PromptRow) -> Self {
        Self {
            prompt_hash: row
                .prompt_hash
                .or_else(|| row.prompt.as_deref().map(prompt_hash)),
            prompt_bytes: row.prompt_bytes,
            prompt: row.prompt,
            project: row.project,
            timestamp_ms: row.timestamp_ms,
        }
    }
}

/// Who a message came from. Mirrors the `session_events.role` constraint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum Role {
    User,
    Assistant,
    /// A standalone tool-result record: a Claude subagent notification, a
    /// Codex `function_call_output`. A `tool_result` block *inside* a user
    /// message is a [`Block`] of that user message instead.
    ToolResult,
}

impl Role {
    fn parse(value: &str) -> Self {
        match value {
            "user" => Self::User,
            "tool_result" => Self::ToolResult,
            _ => Self::Assistant,
        }
    }
}

/// What a block carries. Mirrors the `session_events.kind` constraint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum BlockKind {
    Text,
    Thinking,
    ToolUse,
    ToolResult,
}

impl BlockKind {
    fn parse(value: &str) -> Self {
        match value {
            "thinking" => Self::Thinking,
            "tool_use" => Self::ToolUse,
            "tool_result" => Self::ToolResult,
            _ => Self::Text,
        }
    }
}

/// One message: every event sharing a `message_id`, in order.
///
/// A `message_id` is the ledger's record identity, which is not always the
/// provider's API message: Claude writes one API response as several records
/// with distinct `uuid`s sharing `provider_message_id` and `request_id`, so
/// one API request is several `Message`s here and one entry in
/// [`SessionEvidence::requests`]. See [`MessageIdOrigin`].
///
/// Envelope facts (`request_id`, `provider_message_id`, `stop_reason`,
/// `turn_id`, sidechain and meta flags) are Claude's and Codex's; OpenCode
/// records `provider` and `stop_reason`; cursor and grok carry the model
/// where their logs name one and `None` elsewhere.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Message {
    /// `None` for an event the provider left unnamed; such an event is a
    /// message of its own.
    pub message_id: Option<String>,
    pub role: Role,
    pub ts_ms: i64,
    pub parent_id: Option<String>,
    pub model: Option<String>,
    /// The upstream inference provider the harness named. OpenCode.
    pub provider: Option<String>,
    /// The provider's request id. Claude.
    pub request_id: Option<String>,
    /// The provider's own message id (Claude's `message.id`).
    pub provider_message_id: Option<String>,
    /// Which API request this message belongs to when the provider
    /// delimits requests without naming them. Codex.
    pub request_span: Option<String>,
    /// Why the turn ended, as the harness reported it. Claude, opencode.
    pub stop_reason: Option<String>,
    pub turn_id: Option<String>,
    pub agent_version: Option<String>,
    pub is_sidechain: Option<bool>,
    pub is_meta: Option<bool>,
    pub project: Option<String>,
    pub project_key: Option<String>,
    pub cwd: Option<String>,
    pub git_branch: Option<String>,
    /// Usage normalized from the stored provider blob, once per message.
    /// `None` when the message carried none, or when its per-block copies
    /// disagreed (`usage_error` says which). Per-*request* grouping, which
    /// is what a cost consumer sums, is `SessionEvidence::requests`.
    pub usage: Option<NormalizedUsage>,
    pub usage_error: Option<String>,
    pub blocks: Vec<Block>,
    #[serde(rename = "raw_usage", default, skip_serializing_if = "Option::is_none")]
    token_json: Option<String>,
}

impl Message {
    /// The provider's usage blob as stored, verbatim.
    pub fn raw_usage(&self) -> Option<&str> {
        self.token_json.as_deref()
    }
}

/// One content block of a [`Message`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Block {
    pub event_uid: String,
    /// The event's own role. Usually the message's; OpenCode stores a tool
    /// result as a part of the assistant message that called the tool, so
    /// the block says `ToolResult` inside an `Assistant` message.
    pub role: Role,
    pub kind: BlockKind,
    /// `None` when the query asked for no text, and for a block that stored
    /// none.
    pub text: Option<String>,
    /// UTF-8 length of the stored text, whether or not it was carried.
    pub text_bytes: Option<i64>,
    /// The call a `tool_use` or `tool_result` block belongs to.
    pub tool_use_id: Option<String>,
    /// Provider-native record or block type this block came from.
    pub raw_kind: Option<String>,
    /// Why a block in the user role is not a human prompt: a slash-command
    /// record, a hook's output, a `<system-reminder>`, a Codex context
    /// wrapper. `None` on a genuine prompt and on every model-output block.
    /// The vocabulary is closed and validated where evidence enters the
    /// store, so a stored spelling always parses.
    pub control: Option<ControlKind>,
    pub ts_ms: i64,
}

fn group_messages(source: Source, events: &[(SessionEvent, Option<i64>)]) -> Vec<Message> {
    let mut messages: Vec<Message> = Vec::new();
    let mut index: BTreeMap<String, usize> = BTreeMap::new();
    for (event, text_bytes) in events {
        let block = Block {
            event_uid: event.event_uid.clone(),
            role: Role::parse(&event.role),
            kind: BlockKind::parse(&event.kind),
            text: event.text.clone(),
            text_bytes: *text_bytes,
            tool_use_id: event.tool_use_id.clone(),
            raw_kind: event.raw_kind.clone(),
            control: event.control_kind.as_deref().and_then(ControlKind::parse),
            ts_ms: event.ts_ms,
        };
        let slot = event
            .message_id
            .as_ref()
            .and_then(|id| index.get(id).copied());
        match slot {
            Some(at) => {
                let message = &mut messages[at];
                message.blocks.push(block);
                // Claude copies one message's usage onto every block. The
                // copies are expected to agree; when they do not, the message
                // does not get to pick one.
                match (&message.token_json, &event.token_json) {
                    (None, Some(raw)) => {
                        message.token_json = Some(raw.clone());
                        set_usage(source, message);
                    }
                    (Some(have), Some(raw)) if have != raw => {
                        message.usage = None;
                        message.usage_error = Some("ambiguous-usage-copies".to_string());
                    }
                    _ => {}
                }
                for (slot, value) in [
                    (&mut message.model, &event.model),
                    (&mut message.provider, &event.provider),
                    (&mut message.request_id, &event.request_id),
                    (&mut message.provider_message_id, &event.provider_message_id),
                    (&mut message.request_span, &event.request_span),
                    (&mut message.stop_reason, &event.stop_reason),
                    (&mut message.turn_id, &event.turn_id),
                    (&mut message.agent_version, &event.agent_version),
                ] {
                    if slot.is_none() {
                        *slot = value.clone();
                    }
                }
            }
            None => {
                if let Some(id) = &event.message_id {
                    index.insert(id.clone(), messages.len());
                }
                let mut message = Message {
                    message_id: event.message_id.clone(),
                    role: Role::parse(&event.role),
                    ts_ms: event.ts_ms,
                    parent_id: event.parent_id.clone(),
                    model: event.model.clone(),
                    provider: event.provider.clone(),
                    request_id: event.request_id.clone(),
                    provider_message_id: event.provider_message_id.clone(),
                    request_span: event.request_span.clone(),
                    stop_reason: event.stop_reason.clone(),
                    turn_id: event.turn_id.clone(),
                    agent_version: event.agent_version.clone(),
                    is_sidechain: event.is_sidechain.map(|flag| flag != 0),
                    is_meta: event.is_meta.map(|flag| flag != 0),
                    project: event.project.clone(),
                    project_key: event.project_key.clone(),
                    cwd: event.cwd.clone(),
                    git_branch: event.git_branch.clone(),
                    usage: None,
                    usage_error: None,
                    blocks: vec![block],
                    token_json: event.token_json.clone(),
                };
                set_usage(source, &mut message);
                messages.push(message);
            }
        }
    }
    messages
}

fn set_usage(source: Source, message: &mut Message) {
    let Some(raw) = message.token_json.as_deref() else {
        return;
    };
    match normalize_usage_str(source.as_str(), raw) {
        Ok(usage) => {
            message.usage = usage;
            message.usage_error = None;
        }
        Err(error) => {
            message.usage = None;
            message.usage_error = Some(error.code().to_string());
        }
    }
}

/// One `tool_calls` row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ToolCall {
    pub tool_use_id: String,
    pub message_id: Option<String>,
    pub name: String,
    /// The call's principal argument — a file path, a command — when the
    /// parser extracted one.
    pub target: Option<String>,
    /// The call's arguments, parsed. `None` when the row stored none or the
    /// stored string is not JSON; [`ToolCall::raw_args`] still has it.
    pub args: Option<Value>,
    /// Per-call error flag; `None` when the provider did not say.
    pub is_error: Option<bool>,
    pub ts_ms: Option<i64>,
    #[serde(rename = "raw_args", default, skip_serializing_if = "Option::is_none")]
    args_json: Option<String>,
}

impl ToolCall {
    /// The stored `args_json`, verbatim.
    pub fn raw_args(&self) -> Option<&str> {
        self.args_json.as_deref()
    }

    fn from_row(row: SessionToolCall) -> Self {
        Self {
            tool_use_id: row.tool_use_id,
            message_id: row.message_id,
            name: row.name,
            target: row.target,
            args: row
                .args_json
                .as_deref()
                .and_then(|raw| serde_json::from_str(raw).ok()),
            is_error: row.is_error.map(|flag| flag != 0),
            ts_ms: row.ts_ms,
            args_json: row.args_json,
        }
    }
}

/// One `tool_result` event with its measured fidelity.
///
/// Every fidelity field is `None` when the provider does not record it —
/// never a stand-in value. Claude and codex record all of them; the
/// vocabularies are `result_status` ∈ {running, completed, errored,
/// cancelled, unknown}, `event_source` ∈ {tool_result,
/// subagent_notification, function_call_output}, `error_signal` ∈
/// {tool_result.is_error, exit_code, patch_apply, mcp_err, subagent_status}.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ToolResult {
    pub event_uid: String,
    pub message_id: Option<String>,
    pub ts_ms: i64,
    pub tool_use_id: Option<String>,
    /// n-th result recorded for this `tool_use_id`, from zero.
    pub call_index: Option<i64>,
    /// Position in the transcript's tool-result order.
    pub event_index: Option<i64>,
    /// Raw UTF-8 bytes of the provider's payload, measured before the text
    /// column was materialized.
    pub payload_bytes: Option<i64>,
    /// The harness had already truncated the payload it handed back.
    pub payload_truncated: Option<bool>,
    /// First 16 hex characters of the payload's sha256.
    pub payload_hash: Option<String>,
    pub result_status: Option<String>,
    pub event_source: Option<String>,
    pub error_signal: Option<String>,
    /// Delegated child session this result reports on.
    pub subagent_session_id: Option<String>,
    pub agent_id: Option<String>,
    /// The materialized result text. `None` when the query asked for none.
    pub text: Option<String>,
    pub text_bytes: Option<i64>,
    pub raw_kind: Option<String>,
}

impl ToolResult {
    fn from_event(event: &SessionEvent, text_bytes: Option<i64>) -> Self {
        Self {
            event_uid: event.event_uid.clone(),
            message_id: event.message_id.clone(),
            ts_ms: event.ts_ms,
            tool_use_id: event.tool_use_id.clone(),
            call_index: event.call_index,
            event_index: event.event_index,
            payload_bytes: event.payload_bytes,
            payload_truncated: event.payload_truncated.map(|flag| flag != 0),
            payload_hash: event.payload_hash.clone(),
            result_status: event.result_status.clone(),
            event_source: event.event_source.clone(),
            error_signal: event.error_signal.clone(),
            subagent_session_id: event.subagent_session_id.clone(),
            agent_id: event.agent_id.clone(),
            text: event.text.clone(),
            text_bytes,
            raw_kind: event.raw_kind.clone(),
        }
    }
}

/// One `file_edits` row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct FileEdit {
    pub tool_use_id: String,
    pub message_id: Option<String>,
    pub file_path: String,
    pub tool_name: Option<String>,
    pub lines_added: Option<i64>,
    pub lines_removed: Option<i64>,
    /// The provider's structured patch, parsed. Claude, codex.
    pub structured_patch: Option<Value>,
    pub user_modified: Option<bool>,
    pub ts_ms: Option<i64>,
    pub git_branch: Option<String>,
    pub cwd: Option<String>,
    #[serde(
        rename = "raw_structured_patch",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    structured_patch_json: Option<String>,
}

impl FileEdit {
    /// The stored `structured_patch_json`, verbatim.
    pub fn raw_structured_patch(&self) -> Option<&str> {
        self.structured_patch_json.as_deref()
    }

    fn from_row(row: SessionFileEdit) -> Self {
        Self {
            tool_use_id: row.tool_use_id,
            message_id: row.message_id,
            file_path: row.file_path,
            tool_name: row.tool_name,
            lines_added: row.lines_added,
            lines_removed: row.lines_removed,
            structured_patch: row
                .structured_patch_json
                .as_deref()
                .and_then(|raw| serde_json::from_str(raw).ok()),
            user_modified: row.user_modified.map(|flag| flag != 0),
            ts_ms: row.ts_ms,
            git_branch: row.git_branch,
            cwd: row.cwd,
            structured_patch_json: row.structured_patch_json,
        }
    }
}

/// One `session_markers` row: a provider record the message model cannot
/// carry, kept rather than dropped.
///
/// `kind` is the classified vocabulary (`compaction`, `summary`, `system`,
/// `lifecycle`, `unknown`, …); `subkind` is the provider-native type
/// verbatim. `payload` is a bounded projection — every string at 128
/// characters, every container at 32 entries — never the bytes of an image.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Marker {
    pub marker_uid: String,
    pub ts_ms: Option<i64>,
    pub message_id: Option<String>,
    pub parent_id: Option<String>,
    pub turn_id: Option<String>,
    pub kind: String,
    pub subkind: Option<String>,
    /// The provider's own readable text for this marker, when it wrote one
    /// (grok). `None` when the query asked for no text.
    pub text: Option<String>,
    pub payload: Option<Value>,
    #[serde(
        rename = "raw_payload",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    payload_json: Option<String>,
}

impl Marker {
    /// The stored `payload_json`, verbatim.
    pub fn raw_payload(&self) -> Option<&str> {
        self.payload_json.as_deref()
    }

    fn from_row(row: SessionMarker) -> Self {
        Self {
            marker_uid: row.marker_uid,
            ts_ms: row.ts_ms,
            message_id: row.message_id,
            parent_id: row.parent_id,
            turn_id: row.turn_id,
            kind: row.kind,
            subkind: row.subkind,
            text: row.text,
            payload: row
                .payload_json
                .as_deref()
                .and_then(|raw| serde_json::from_str(raw).ok()),
            payload_json: row.payload_json,
        }
    }
}

/// Which end of a [`Relationship`] the queried session sits on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum RelationshipSide {
    /// The session delegated to `child_session_id`.
    Parent,
    /// The session was delegated to by `parent_session_id`.
    Child,
    /// A fork, resume or continuation edge naming the session on either end.
    Continuity,
}

/// One `session_relationships` row, seen from the queried session.
///
/// `relationship` is `delegated`, `materialized_local`, `fork`, `resume` or
/// `continuation`. `identity_status` is `observed` when the other end has a
/// stable id and `unlinked` when the provider recorded the delegation without
/// one; see [`RelationshipCapabilities`] for which sources do which.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Relationship {
    pub side: RelationshipSide,
    pub relationship: String,
    pub relationship_uid: String,
    pub parent_session_id: String,
    pub child_session_id: Option<String>,
    pub identity_status: String,
    pub child_agent_type: Option<String>,
    pub child_agent_name: Option<String>,
    pub child_model: Option<String>,
    pub spawn_depth: Option<i64>,
    pub evidence_kind: String,
    pub evidence_locator: Option<String>,
    pub evidence_ref: Option<String>,
    pub child_has_events: bool,
    pub spawned_at_ms: Option<i64>,
    pub created_ms: i64,
    /// For a continuity edge, the session the chain started from.
    pub origin_session_id: Option<String>,
}

impl Relationship {
    fn from_row(row: SessionRelationship, side: RelationshipSide) -> Self {
        Self {
            side,
            relationship: row.relationship,
            relationship_uid: row.relationship_uid,
            parent_session_id: row.parent_session_id,
            child_session_id: row.child_session_id,
            identity_status: row.identity_status,
            child_agent_type: row.child_agent_type,
            child_agent_name: row.child_agent_name,
            child_model: row.child_model,
            spawn_depth: row.spawn_depth,
            evidence_kind: row.evidence_kind,
            evidence_locator: row.evidence_locator,
            evidence_ref: row.evidence_ref,
            child_has_events: row.child_has_events,
            spawned_at_ms: row.spawned_at_ms,
            created_ms: row.created_ms,
            origin_session_id: row.origin_session_id,
        }
    }
}

/// Every user turn, walking the bounded page internally.
fn all_user_turns(
    conn: &Connection,
    source: &str,
    session_id: &str,
) -> Result<Vec<SessionUserTurn>, Error> {
    let mut turns = Vec::new();
    let mut cursor = None;
    loop {
        let page = session_user_turns_page(conn, source, session_id, 1_000, cursor.as_ref())
            .map_err(Error::query)?;
        turns.extend(page.user_turns);
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => return Ok(turns),
        }
    }
}

/// Every model request, walking the bounded page internally.
fn all_requests(
    conn: &Connection,
    source: &str,
    session_id: &str,
) -> Result<Vec<SessionRequest>, Error> {
    let mut requests = Vec::new();
    let mut cursor = None;
    loop {
        let page = session_requests_page(conn, source, session_id, 1_000, cursor.as_ref())
            .map_err(Error::query)?;
        requests.extend(page.requests);
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => return Ok(requests),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `Source::ALL` is the enumeration an embedder gets. The `match` here is
    /// exhaustive on purpose: a new variant that is not in `ALL` breaks the
    /// build here rather than quietly leaving every iterator over `ALL` a
    /// source short. The names are then checked against `SOURCE_CHOICES`,
    /// the ledger's own registry, so the two cannot drift apart either.
    #[test]
    fn every_source_is_in_all_and_in_the_ledger_registry() {
        for source in Source::ALL {
            let listed = match source {
                Source::Claude
                | Source::Codex
                | Source::Cursor
                | Source::Grok
                | Source::Relay
                | Source::Trajectory
                | Source::OpenCode => Source::ALL.contains(source),
            };
            assert!(listed, "{source:?} is missing from Source::ALL");
        }
        let mut names: Vec<&str> = Source::ALL.iter().map(|source| source.as_str()).collect();
        names.sort_unstable();
        names.dedup();
        let mut choices: Vec<&str> = crate::store::SOURCE_CHOICES.to_vec();
        choices.sort_unstable();
        assert_eq!(
            names, choices,
            "Source::ALL and SOURCE_CHOICES name different sources"
        );
    }

    /// A store over `db` whose provider roots are the (empty) directory
    /// beside it, resolved without the environment, so a `CODEX_HOME` on
    /// the machine running the tests cannot reach in.
    fn store_at(db: &Path) -> SessionStore {
        let home = db.parent().expect("db parent").to_path_buf();
        SessionStore::open(StoreOptions {
            db_path: Some(db.to_path_buf()),
            roots: Some(ProviderRoots::from_home(
                home.clone(),
                home.join("opencode.db"),
            )),
            ..StoreOptions::default()
        })
        .unwrap()
    }

    /// `u64::MAX` means "wait as long as it takes", not "panic before the
    /// first attempt".
    #[test]
    fn an_oversized_lock_timeout_is_clamped_not_a_panic() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_at(&dir.path().join("ai-history.db"));
        let report = store
            .sync(SyncOptions {
                force: false,
                lock_timeout_ms: u64::MAX,
                ..Default::default()
            })
            .expect("an unlocked store syncs");
        assert!(report.swept);
    }

    /// A timeout too large to name an instant waits without one; it does not
    /// panic before the channel is read.
    #[test]
    fn an_unrepresentable_watch_timeout_is_no_deadline_not_a_panic() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_at(&dir.path().join("ai-history.db"));
        let mut watch = store
            .watch(WatchOptions {
                use_fs_events: false,
                poll_interval_ms: 60_000,
                immediate: true,
                ..WatchOptions::default()
            })
            .unwrap();
        // The startup sweep is queued (or about to be); the unbounded wait
        // returns it rather than overflowing an `Instant`.
        let first = watch
            .next_timeout(Duration::MAX)
            .expect("the startup tick")
            .expect("the sweep succeeds");
        assert_eq!(first.trigger, TickTrigger::Startup);
        watch.stop();
        // And once the loop has stopped, an unbounded wait still ends.
        assert!(watch.next_timeout(Duration::MAX).is_none());
    }

    /// Every page of a catalog walk reads the snapshot the first page took:
    /// a sync landing between pages cannot move a row across the cursor.
    #[test]
    fn a_catalog_walk_is_one_snapshot_across_pages() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("ai-history.db");
        let store = store_at(&db);
        let writer = open_db(&db).unwrap();
        writer
            .execute_batch(
                "INSERT INTO sessions (session_id, source, discovery_state, last_activity_ms) \
                 VALUES ('a', 'claude', 'full', 300), ('b', 'claude', 'full', 200), \
                        ('c', 'claude', 'full', 100);",
            )
            .unwrap();

        let mut walk = store.sessions(CatalogQuery {
            page_size: 1,
            ..CatalogQuery::default()
        });
        let first = walk.next().unwrap().unwrap();
        assert_eq!(first.session_id, "a");
        // Between pages: the oldest row gains activity (it would now sort
        // before the cursor and be skipped) and the emitted row loses some
        // (it would sort after the cursor and be repeated).
        writer
            .execute_batch(
                "UPDATE sessions SET last_activity_ms = 400 WHERE session_id = 'c'; \
                 UPDATE sessions SET last_activity_ms = 50 WHERE session_id = 'a';",
            )
            .unwrap();
        let mut rest: Vec<String> = walk.by_ref().map(|row| row.unwrap().session_id).collect();
        rest.sort();
        assert_eq!(rest, vec!["b", "c"], "each session exactly once");
        drop(walk);

        // A fresh walk sees the writes, so the snapshot was the iterator's,
        // not a stale connection.
        let order: Vec<String> = store
            .sessions(CatalogQuery::default())
            .map(|row| row.unwrap().session_id)
            .collect();
        assert_eq!(order, vec!["c", "b", "a"]);
    }

    /// A catalog change the sweep makes indirectly — here the identity
    /// refresh lending a parent's remote key to a delegated child whose own
    /// key was only its path — is reported in `changed`, though nothing about
    /// the child's stamp, activity or discovery state moved.
    #[test]
    fn an_inherited_project_key_is_a_reported_change() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("ai-history.db");
        let store = store_at(&db);
        let conn = open_db(&db).unwrap();
        conn.execute_batch(
            "INSERT INTO sessions (session_id, source, discovery_state, project_key, \
              project_key_method) \
             VALUES ('parent', 'codex', 'full', 'github.com/org/repo', 'remote'), \
                    ('child', 'codex', 'full', '/tmp/elsewhere', 'path'), \
                    ('bystander', 'codex', 'full', '/tmp/quiet', 'path');
             INSERT INTO session_relationships (source, parent_session_id, relationship_uid, \
              child_session_id, relationship, identity_status, evidence_kind, created_ms, \
              updated_ms) \
             VALUES ('codex', 'parent', 'uid-1', 'child', 'delegated', 'observed', \
                     'session_meta', 1, 1);",
        )
        .unwrap();

        let report = store.sync(SyncOptions::default()).unwrap();
        assert!(report.swept);
        assert_eq!(
            report.changed,
            vec![SessionRef::id(Source::Codex, "child")],
            "the child moved and nothing else did: {:?}",
            report.changed
        );
        let key: String = conn
            .query_row(
                "SELECT project_key FROM sessions WHERE session_id = 'child'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(key, "github.com/org/repo");

        // And a sweep that moves no row reports none, so the digest is not
        // sensitive to anything a sweep rewrites identically.
        let again = store
            .sync(SyncOptions {
                force: true,
                lock_timeout_ms: 0,
                ..Default::default()
            })
            .unwrap();
        assert!(again.swept);
        assert!(again.changed.is_empty(), "{:?}", again.changed);
    }

    #[cfg(unix)]
    #[test]
    fn cancellation_after_lock_contention_precedes_the_retry_deadline() {
        use std::os::unix::io::AsRawFd;

        let dir = tempfile::tempdir().unwrap();
        let store = store_at(&dir.path().join("ai-history.db"));
        let holder = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.path().join("ai-history.db.sync.lock"))
            .unwrap();
        // SAFETY: holder owns the descriptor until the test finishes.
        assert_eq!(unsafe { libc::flock(holder.as_raw_fd(), libc::LOCK_EX) }, 0);
        let checks = std::cell::Cell::new(0);
        let error = crate::ingest::with_capture_stop(
            move || {
                checks.set(checks.get() + 1);
                // Enter the scope, attempt the lock, then stop before retrying.
                checks.get() >= 3
            },
            || {
                // Assert inside the scope so its final check cannot mask an
                // incorrectly classified SyncLocked from the retry path.
                assert!(matches!(
                    store.sync_tick(false, 0),
                    Err(Error::Cancelled(_))
                ));
                Ok(())
            },
        )
        .unwrap_err();
        assert!(error.is::<CaptureCancelled>());
    }

    /// A catalog write that lands while `sync` is still waiting for another
    /// holder's lock is not this sweep's change: the baseline is read after
    /// the lock is taken.
    #[cfg(unix)]
    #[test]
    fn changes_made_before_the_lock_was_taken_are_not_this_sweeps() {
        use std::os::unix::io::AsRawFd;

        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("ai-history.db");
        let store = store_at(&db);
        let holder = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.path().join("ai-history.db.sync.lock"))
            .unwrap();
        // SAFETY: `holder` owns the descriptor for the whole test.
        assert_eq!(unsafe { libc::flock(holder.as_raw_fd(), libc::LOCK_EX) }, 0);

        let waiting = std::thread::spawn({
            let store = store.clone();
            move || {
                store.sync(SyncOptions {
                    force: false,
                    lock_timeout_ms: 10_000,
                    ..Default::default()
                })
            }
        });
        // While the sweep waits for the lock, another writer catalogs a
        // session — a hydration, another host's discovery.
        std::thread::sleep(Duration::from_millis(300));
        open_db(&db)
            .unwrap()
            .execute_batch(
                "INSERT INTO sessions (session_id, source, discovery_state, last_activity_ms) \
                 VALUES ('early', 'claude', 'full', 1)",
            )
            .unwrap();
        // SAFETY: as above.
        assert_eq!(unsafe { libc::flock(holder.as_raw_fd(), libc::LOCK_UN) }, 0);
        let report = waiting
            .join()
            .unwrap()
            .expect("the lock was released in time");
        assert!(report.swept);
        assert!(
            report.changed.is_empty(),
            "a row written before this sweep held the lock is not its change: {:?}",
            report.changed
        );
    }

    /// A path names a session only for a source that keeps one session per
    /// file; for the others the same reference is refused for reading as it
    /// is for hydrating, rather than answering with whichever session shares
    /// the locator.
    #[test]
    fn a_path_reference_is_refused_where_a_path_names_no_one_session() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("ai-history.db");
        let store = store_at(&db);
        let opencode_db = dir.path().join("opencode.db");
        open_db(&db)
            .unwrap()
            .execute(
                "INSERT INTO sessions (session_id, source, discovery_state, raw_path) \
                 VALUES ('a', 'opencode', 'full', ?1), ('b', 'opencode', 'full', ?1)",
                [opencode_db.to_string_lossy().as_ref()],
            )
            .unwrap();

        let by_path = SessionRef::path(Source::OpenCode, &opencode_db);
        let read = store
            .session(&by_path, SessionQuery::default())
            .unwrap_err();
        assert_eq!(read.code(), "HYDRATION_UNSUPPORTED");
        let hydrated = store
            .hydrate(&by_path, HydrateOptions::default())
            .unwrap_err();
        assert_eq!(hydrated.code(), read.code(), "one rule, one error");
        // Both sessions are still reachable the way the provider names them.
        for id in ["a", "b"] {
            assert!(store
                .session(
                    &SessionRef::id(Source::OpenCode, id),
                    SessionQuery::default()
                )
                .unwrap()
                .is_some());
        }
    }

    /// `None` is every source; `Some(vec![])` is an allowlist that admits
    /// none, and the two are not the same request.
    #[test]
    fn an_empty_source_allowlist_yields_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("ai-history.db");
        let store = store_at(&db);
        open_db(&db)
            .unwrap()
            .execute_batch(
                "INSERT INTO sessions (session_id, source, discovery_state) \
                 VALUES ('a', 'claude', 'full'), ('b', 'codex', 'full')",
            )
            .unwrap();
        assert_eq!(store.sessions(CatalogQuery::default()).count(), 2);
        assert_eq!(
            store
                .sessions(CatalogQuery {
                    sources: Some(Vec::new()),
                    ..CatalogQuery::default()
                })
                .count(),
            0
        );
        assert_eq!(
            store
                .sessions(CatalogQuery {
                    sources: Some(vec![Source::Codex]),
                    ..CatalogQuery::default()
                })
                .count(),
            1
        );
    }

    /// A catalog row a hydration writes while an unswept `sync` holds the
    /// lock — after the baseline digest, before the after-digest — is that
    /// call's change, though the sweep itself opened nothing.
    #[test]
    fn an_unswept_sync_reports_a_catalog_write_made_under_its_lock() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("ai-history.db");
        let store = store_at(&db);
        assert!(store.sync(SyncOptions::default()).unwrap().swept);

        // The second call finds the fingerprint unchanged; the catalog write
        // lands inside its locked window, the way a targeted hydration does.
        let outcome = sync_facade_tick(
            &db,
            store.roots(),
            false,
            |conn| {
                let before = catalog_fingerprint(conn)?;
                conn.execute_batch(
                    "INSERT INTO sessions (session_id, source, discovery_state) \
                     VALUES ('hydrated-meanwhile', 'claude', 'full')",
                )?;
                Ok(before)
            },
            |conn, before, tick| {
                assert!(!tick.swept, "the sources did not move");
                Ok(changes_under_lock(conn, &before)?.1)
            },
        )
        .unwrap()
        .expect("the lock was free");
        assert_eq!(
            outcome.1,
            vec![SessionRef::id(Source::Claude, "hydrated-meanwhile")]
        );

        // And the public call over the same store: unswept, nothing moved
        // under its lock, nothing reported — the earlier write was before it.
        let again = store.sync(SyncOptions::default()).unwrap();
        assert!(!again.swept);
        assert!(again.changed.is_empty());
    }

    /// A catalog write between watch ticks is reported once, by the next
    /// tick that takes the lock, even though that tick opens no provider
    /// file; the tick after it reports nothing.
    #[test]
    fn an_unswept_watch_tick_reports_a_hydration_between_ticks_once() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("ai-history.db");
        let store = store_at(&db);
        let mut watch = store
            .watch(WatchOptions {
                use_fs_events: false,
                poll_interval_ms: 100,
                immediate: true,
                ..WatchOptions::default()
            })
            .unwrap();
        let first = watch
            .next_timeout(Duration::from_secs(30))
            .expect("startup tick")
            .unwrap();
        assert_eq!(first.trigger, TickTrigger::Startup);
        assert!(first.changed.is_empty());

        // A targeted hydration between ticks: a catalog write outside the
        // sync lock while every source fingerprint stays where it was.
        open_db(&db)
            .unwrap()
            .execute_batch(
                "INSERT INTO sessions (session_id, source, discovery_state) \
                 VALUES ('hydrated-between-ticks', 'codex', 'full')",
            )
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut reported = None;
        while Instant::now() < deadline {
            let tick = watch
                .next_timeout(Duration::from_secs(5))
                .expect("a poll tick")
                .unwrap();
            if !tick.changed.is_empty() {
                reported = Some(tick);
                break;
            }
        }
        let reported = reported.expect("the write was reported");
        assert!(!reported.swept, "no source moved; the tick was unswept");
        assert_eq!(
            reported.changed,
            vec![SessionRef::id(Source::Codex, "hydrated-between-ticks")]
        );
        let following = watch
            .next_timeout(Duration::from_secs(30))
            .expect("the next poll tick")
            .unwrap();
        assert!(
            following.changed.is_empty(),
            "reported once, not on every unswept tick: {:?}",
            following.changed
        );
        watch.stop();
    }

    /// A tick that fails keeps the rolling baseline, so the rows it (or
    /// anyone) committed before the failure are reported by the next tick
    /// that succeeds, rather than silently folded into a fresh baseline.
    #[test]
    fn a_failed_tick_keeps_the_baseline_for_the_next_report() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("ai-history.db");
        store_at(&db);
        let conn = open_db(&db).unwrap();
        conn.execute_batch(
            "INSERT INTO sessions (session_id, source, discovery_state) \
             VALUES ('known', 'claude', 'full')",
        )
        .unwrap();
        let reported = catalog_fingerprint(&conn).unwrap();
        let mut base = Some(reported.clone());
        let swept = SyncTick {
            attempted: true,
            swept: true,
        };

        // The tick fails after committing a row — a provider read that died
        // half-way through the sweep.
        conn.execute_batch(
            "INSERT INTO sessions (session_id, source, discovery_state) \
             VALUES ('committed-then-failed', 'claude', 'full')",
        )
        .unwrap();
        let failed = rolling_tick(&mut base, |previous| {
            assert_eq!(
                previous.as_ref(),
                Some(&reported),
                "the sweep sees the baseline"
            );
            Err(anyhow::anyhow!("provider read failed"))
        });
        assert!(failed.is_err());
        assert_eq!(
            base.as_ref(),
            Some(&reported),
            "the baseline survives the failure"
        );

        // A contended tick touches nothing either.
        let (contended, changed) = rolling_tick(&mut base, |_| Ok(None)).unwrap();
        assert!(contended.contended);
        assert!(changed.is_empty());
        assert_eq!(base.as_ref(), Some(&reported));

        // The next successful tick reports the row the failed one committed,
        // plus its own, and rolls the baseline forward.
        conn.execute_batch(
            "INSERT INTO sessions (session_id, source, discovery_state) \
             VALUES ('this-tick', 'claude', 'full')",
        )
        .unwrap();
        let after = catalog_fingerprint(&conn).unwrap();
        let (outcome, changed) = rolling_tick(&mut base, |previous| {
            let before = previous.expect("baseline kept");
            let changed = catalog_changes(&before, &after);
            Ok(Some((swept, after.clone(), changed)))
        })
        .unwrap();
        assert!(outcome.swept);
        let mut ids: Vec<String> = changed
            .iter()
            .map(|r| match r {
                SessionRef::Id { session_id, .. } => session_id.clone(),
                SessionRef::Path { .. } => unreachable!(),
            })
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["committed-then-failed", "this-tick"]);
        assert_eq!(base.as_ref(), Some(&after));

        // An unswept tick keeps the baseline where the last swept one left it.
        let (_, changed) = rolling_tick(&mut base, |previous| {
            let before = previous.unwrap();
            let (now, changed) = changes_under_lock(&conn, &before).unwrap();
            Ok(Some((
                SyncTick {
                    attempted: true,
                    swept: false,
                },
                now,
                changed,
            )))
        })
        .unwrap();
        assert!(changed.is_empty());
        assert_eq!(base.as_ref(), Some(&after));
    }

    /// The roots the facade advertises per source are the roots the watch
    /// loop registers, path for path and scope for scope — including the
    /// flat prompt logs, which are watched as single files.
    #[test]
    fn advertised_watch_roots_are_the_roots_the_loop_registers() {
        let dir = tempfile::tempdir().unwrap();
        let roots =
            ProviderRoots::from_home(dir.path().to_path_buf(), dir.path().join("opencode.db"));
        let mut advertised: BTreeMap<PathBuf, WatchScope> = BTreeMap::new();
        for source in Source::ALL {
            for root in source.capabilities().watch_roots(&roots) {
                let scope = advertised.entry(root.path).or_insert(root.scope);
                *scope = (*scope).max(root.scope);
            }
        }
        let registered: BTreeMap<PathBuf, WatchScope> =
            sync_watch_roots_with_provider_roots(&roots)
                .into_iter()
                .map(WatchedPath::from_root)
                .map(|root| (root.path, root.scope))
                .collect();
        assert_eq!(advertised, registered);
        for (source, flat_log) in [
            (Source::Claude, roots.claude.join("history.jsonl")),
            (Source::Codex, roots.codex.join("history.jsonl")),
        ] {
            let mine = source.capabilities().watch_roots(&roots);
            assert!(
                mine.contains(&WatchedPath {
                    path: flat_log.clone(),
                    scope: WatchScope::File,
                }),
                "{source} advertises its flat log as a file root: {mine:?}"
            );
            assert!(
                mine.iter().any(|root| root.scope == WatchScope::Tree),
                "{source} advertises its transcript tree: {mine:?}"
            );
        }
        assert!(Source::Relay.capabilities().watch_roots(&roots).is_empty());
    }

    /// A source exempt from shallow discovery still declares the rows its
    /// own sweep writes, so its prompts are read rather than filtered out as
    /// "not produced by this source".
    #[test]
    fn a_trajectory_session_reads_its_prompt_back() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("ai-history.db");
        let store = store_at(&db);
        open_db(&db)
            .unwrap()
            .execute_batch(
                "INSERT INTO sessions (session_id, source, discovery_state) \
                 VALUES ('traj-1', 'trajectory', 'full');
                 INSERT INTO history (source, session_id, project, prompt, prompt_hash, \
                  timestamp_ms) \
                 VALUES ('trajectory', 'traj-1', 'proj', 'ship the thing', 'h', 10);",
            )
            .unwrap();
        for source in [Source::Trajectory, Source::Relay] {
            assert!(
                source
                    .capabilities()
                    .evidence_kinds
                    .contains(&EvidenceKind::History),
                "{source} writes history rows and says so"
            );
        }
        let evidence = store
            .session(
                &SessionRef::id(Source::Trajectory, "traj-1"),
                SessionQuery::default(),
            )
            .unwrap()
            .expect("catalogued");
        assert_eq!(evidence.coverage, vec![EvidenceKind::History]);
        assert_eq!(evidence.loaded, vec![EvidenceKind::History]);
        assert_eq!(evidence.prompts.len(), 1);
        assert_eq!(
            evidence.prompts[0].prompt.as_deref(),
            Some("ship the thing")
        );
    }

    /// `loaded` never names a kind the source cannot produce, whatever the
    /// query asked for, and the rows behind such a kind are not read.
    #[test]
    fn loaded_kinds_stay_within_the_source_coverage() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("ai-history.db");
        let store = store_at(&db);
        let conn = open_db(&db).unwrap();
        conn.execute_batch(
            "INSERT INTO sessions (session_id, source, cwd, discovery_state) \
             VALUES ('c1', 'cursor', '/tmp/p', 'full');
             INSERT INTO session_markers (source, session_id, marker_uid, kind) \
             VALUES ('cursor', 'c1', 'mk', 'unknown');",
        )
        .unwrap();
        let cursor = Source::Cursor.capabilities().evidence_kinds;
        assert!(!cursor.contains(&EvidenceKind::SessionMarker));

        let evidence = store
            .session(
                &SessionRef::id(Source::Cursor, "c1"),
                SessionQuery {
                    include_text: true,
                    kinds: Some(vec![EvidenceKind::SessionMarker, EvidenceKind::History]),
                },
            )
            .unwrap()
            .unwrap();
        assert_eq!(evidence.loaded, vec![EvidenceKind::History]);
        assert!(evidence.markers.is_empty());
        assert_eq!(evidence.coverage, cursor);

        let all = store
            .session(
                &SessionRef::id(Source::Cursor, "c1"),
                SessionQuery::default(),
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            all.loaded, cursor,
            "the default query loads exactly the coverage"
        );
    }

    /// A hash-only read answers with the stored hash and byte length and
    /// carries no prompt, excerpt or marker text.
    #[test]
    fn a_hash_only_read_carries_no_text() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("ai-history.db");
        let store = store_at(&db);
        let conn = open_db(&db).unwrap();
        conn.execute_batch(
            "INSERT INTO sessions (session_id, source, cwd, discovery_state, first_prompt, \
              last_assistant_text) \
             VALUES ('s1', 'grok', '/tmp/p', 'full', 'hello there', 'bye');
             INSERT INTO history (source, session_id, project, prompt, prompt_hash, timestamp_ms) \
             VALUES ('grok', 's1', '/tmp/p', 'hello', 'stored-hash', 10);
             INSERT INTO history (source, session_id, project, prompt, prompt_hash, timestamp_ms) \
             VALUES ('grok', 's1', '/tmp/p', 'unhashed', NULL, 20);
             INSERT INTO session_markers (source, session_id, marker_uid, ts_ms, kind, text) \
             VALUES ('grok', 's1', 'mk', 5, 'system', 'preamble');",
        )
        .unwrap();

        let hashed = store
            .session(
                &SessionRef::id(Source::Grok, "s1"),
                SessionQuery {
                    include_text: false,
                    kinds: None,
                },
            )
            .unwrap()
            .unwrap();
        assert!(hashed.session.first_prompt.is_none());
        assert!(hashed.session.last_assistant_text.is_none());
        assert_eq!(hashed.prompts.len(), 2);
        assert!(hashed.prompts[0].prompt.is_none());
        assert_eq!(
            hashed.prompts[0].prompt_hash.as_deref(),
            Some("stored-hash")
        );
        assert_eq!(hashed.prompts[0].prompt_bytes, 5);
        // No stored hash and no text: `None`, never a hash of nothing.
        assert!(hashed.prompts[1].prompt_hash.is_none());
        assert_eq!(hashed.prompts[1].prompt_bytes, 8);
        assert!(hashed.markers[0].text.is_none());

        let full = store
            .session(&SessionRef::id(Source::Grok, "s1"), SessionQuery::default())
            .unwrap()
            .unwrap();
        assert_eq!(full.session.first_prompt.as_deref(), Some("hello there"));
        assert_eq!(full.prompts[0].prompt.as_deref(), Some("hello"));
        assert_eq!(full.prompts[0].prompt_hash.as_deref(), Some("stored-hash"));
        assert_eq!(
            full.prompts[1].prompt_hash.as_deref(),
            Some(prompt_hash("unhashed").as_str()),
            "with the text in hand the hash is computed"
        );
        assert_eq!(full.markers[0].text.as_deref(), Some("preamble"));
    }

    #[test]
    fn open_creates_the_database() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("ai-history.db");
        store_at(&db);
        assert!(db.exists());
    }

    #[test]
    fn errors_render_their_code_and_round_trip_through_serde() {
        let error = Error::SyncLocked {
            path: PathBuf::from("/tmp/x.db"),
            waited_ms: 250,
        };
        assert_eq!(error.code(), "SYNC_LOCKED");
        assert!(error.to_string().starts_with("SYNC_LOCKED: "));
        let json = serde_json::to_string(&error).unwrap();
        assert_eq!(serde_json::from_str::<Error>(&json).unwrap(), error);

        let coded = Error::hydration(anyhow::anyhow!("SESSION_NOT_FOUND: run a sync first"));
        assert_eq!(
            coded,
            Error::SessionNotFound("run a sync first".to_string())
        );
        let plain = Error::hydration(anyhow::anyhow!("disk on fire"));
        assert_eq!(plain, Error::HydrationFailed("disk on fire".to_string()));
        let discovery = Error::sync(anyhow::anyhow!("DISCOVERY_FAILED: codex: boom"));
        assert_eq!(discovery, Error::Discovery("codex: boom".to_string()));

        // The serialized tag is the same word `code()` answers, for every
        // variant, so a host that matches on one can match on the other.
        for error in [
            Error::DatabaseOpen(String::new()),
            Error::InvalidArgument(String::new()),
            Error::UnsupportedOperation(String::new()),
            Error::SessionNotFound(String::new()),
            Error::SessionSourceUnavailable(String::new()),
            Error::SourceMismatch(String::new()),
            Error::HydrationUnsupported(String::new()),
            Error::HydrationFailed(String::new()),
            Error::ConnectorNotConfigured(String::new()),
            Error::AuthenticationExpired(String::new()),
            Error::EvidencePartial(String::new()),
            Error::ConnectorFailure(String::new()),
            Error::Query(String::new()),
            Error::Discovery(String::new()),
            Error::SyncFailed(String::new()),
            Error::SyncLocked {
                path: PathBuf::new(),
                waited_ms: 0,
            },
            Error::WatermarkAheadOfStore(String::new()),
            Error::ConsumerKindsMismatch(String::new()),
        ] {
            let json = serde_json::to_value(&error).unwrap();
            assert_eq!(json["code"].as_str(), Some(error.code()), "{error:?}");
        }
    }

    #[test]
    fn every_source_declares_capabilities() {
        for source in Source::ALL {
            let capabilities = source.capabilities();
            assert_eq!(capabilities.source, *source);
            assert_eq!(Source::parse(source.as_str()), Some(*source));
        }
        assert!(Source::Claude.capabilities().hydrates_by_path);
        assert!(!Source::Codex.capabilities().hydrates_by_path);
        assert_eq!(
            Source::Codex.capabilities().message_ids,
            MessageIdOrigin::Synthesized
        );
        assert!(Source::parse("nope").is_none());
    }

    #[test]
    fn a_missing_session_is_none_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_at(&dir.path().join("ai-history.db"));
        assert!(store
            .session(
                &SessionRef::id(Source::Codex, "missing"),
                SessionQuery::default()
            )
            .unwrap()
            .is_none());
        assert!(store
            .session(
                &SessionRef::path(Source::Claude, "/nowhere/x.jsonl"),
                SessionQuery::default()
            )
            .unwrap()
            .is_none());
        assert_eq!(store.sessions(CatalogQuery::default()).count(), 0);
    }

    #[test]
    fn a_read_only_store_refuses_to_write() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("ai-history.db");
        store_at(&db);
        let store = SessionStore::open(StoreOptions {
            db_path: Some(db),
            read_only: true,
            ..StoreOptions::default()
        })
        .unwrap();
        assert!(matches!(
            store.sync(SyncOptions::default()),
            Err(Error::UnsupportedOperation(_))
        ));
        assert!(matches!(
            store.hydrate(
                &SessionRef::id(Source::Claude, "s"),
                HydrateOptions::default()
            ),
            Err(Error::UnsupportedOperation(_))
        ));
        assert!(matches!(
            store.watch(WatchOptions::default()),
            Err(Error::UnsupportedOperation(_))
        ));
    }

    #[test]
    fn hydrating_by_path_is_refused_for_a_source_without_a_hook_harness() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_at(&dir.path().join("ai-history.db"));
        let error = store
            .hydrate(
                &SessionRef::path(Source::Codex, dir.path().join("rollout.jsonl")),
                HydrateOptions::default(),
            )
            .unwrap_err();
        assert_eq!(error.code(), "HYDRATION_UNSUPPORTED");
    }

    #[test]
    fn hydrating_an_uncatalogued_session_is_session_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_at(&dir.path().join("ai-history.db"));
        let error = store
            .hydrate(
                &SessionRef::id(Source::Claude, "never-discovered"),
                HydrateOptions::default(),
            )
            .unwrap_err();
        assert_eq!(error.code(), "SESSION_NOT_FOUND");
    }

    /// Messages group by `message_id`, Claude's per-block usage copies
    /// collapse to one usage, and `include_text: false` keeps sizes.
    #[test]
    fn session_groups_events_into_typed_messages() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("ai-history.db");
        let store = store_at(&db);
        let conn = open_db(&db).unwrap();
        conn.execute_batch(
            "INSERT INTO sessions (session_id, source, cwd, discovery_state) \
             VALUES ('s1', 'claude', '/tmp/p', 'full');
             INSERT INTO session_events \
             (source, session_id, message_id, ts_ms, role, kind, text, event_uid) \
             VALUES ('claude', 's1', 'm1', 10, 'user', 'text', 'hello', 'e1');
             INSERT INTO session_events \
             (source, session_id, message_id, ts_ms, role, kind, text, event_uid, model, \
              token_json, request_id, stop_reason) \
             VALUES ('claude', 's1', 'm2', 20, 'assistant', 'thinking', 'hmm', 'e2', 'm', \
              '{\"input_tokens\":3,\"output_tokens\":4}', 'req_1', 'end_turn');
             INSERT INTO session_events \
             (source, session_id, message_id, ts_ms, role, kind, text, event_uid, model, \
              token_json, tool_use_id, request_id) \
             VALUES ('claude', 's1', 'm2', 20, 'assistant', 'tool_use', NULL, 'e3', 'm', \
              '{\"input_tokens\":3,\"output_tokens\":4}', 'tu_1', 'req_1');
             INSERT INTO session_events \
             (source, session_id, message_id, ts_ms, role, kind, text, event_uid, tool_use_id, \
              payload_bytes, payload_hash, result_status, event_source, call_index, event_index) \
             VALUES ('claude', 's1', 'm3', 30, 'user', 'tool_result', 'ok', 'e4', 'tu_1', \
              2, 'abcd', 'completed', 'tool_result', 0, 0);
             INSERT INTO tool_calls (source, session_id, message_id, tool_use_id, name, args_json) \
             VALUES ('claude', 's1', 'm2', 'tu_1', 'Bash', '{\"command\":\"ls\"}');",
        )
        .unwrap();

        let evidence = store
            .session(
                &SessionRef::id(Source::Claude, "s1"),
                SessionQuery::default(),
            )
            .unwrap()
            .expect("catalogued");
        assert_eq!(evidence.messages.len(), 3);
        let assistant = &evidence.messages[1];
        assert_eq!(assistant.role, Role::Assistant);
        assert_eq!(assistant.blocks.len(), 2);
        assert_eq!(assistant.request_id.as_deref(), Some("req_1"));
        assert_eq!(assistant.stop_reason.as_deref(), Some("end_turn"));
        let usage = assistant.usage.as_ref().expect("normalized once");
        assert_eq!(usage.input_tokens, 3);
        assert!(assistant.raw_usage().unwrap().contains("output_tokens"));
        assert_eq!(evidence.tool_results.len(), 1);
        assert_eq!(evidence.tool_results[0].payload_bytes, Some(2));
        assert_eq!(
            evidence.tool_calls[0].args,
            Some(serde_json::json!({"command": "ls"}))
        );
        assert_eq!(
            evidence.tool_calls[0].raw_args(),
            Some("{\"command\":\"ls\"}")
        );
        assert_eq!(evidence.user_turns.len(), 2);
        assert_eq!(evidence.requests.len(), 1);
        assert!(evidence.usage.is_some());

        let json = serde_json::to_string(&evidence).unwrap();
        let back: SessionEvidence = serde_json::from_str(&json).unwrap();
        assert_eq!(back, evidence);

        let hashed = store
            .session(
                &SessionRef::id(Source::Claude, "s1"),
                SessionQuery {
                    include_text: false,
                    kinds: Some(vec![EvidenceKind::SessionEvent]),
                },
            )
            .unwrap()
            .unwrap();
        assert!(hashed.messages[0].blocks[0].text.is_none());
        assert_eq!(hashed.messages[0].blocks[0].text_bytes, Some(5));
        assert!(hashed.tool_calls.is_empty());
        assert_eq!(hashed.loaded, vec![EvidenceKind::SessionEvent]);
    }

    #[test]
    fn a_read_only_open_rejects_a_database_it_cannot_migrate() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("ai-history.db");
        // A database written before the tool-result fidelity columns existed.
        // A read-only handle never runs `init_db`, so without the check at
        // `open` the store would hand back a handle whose first read dies
        // inside a SELECT on `no such column: event_source`.
        {
            let conn = rusqlite::Connection::open(&db).unwrap();
            conn.execute_batch(
                "CREATE TABLE session_events (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     source TEXT NOT NULL,
                     session_id TEXT NOT NULL,
                     message_id TEXT,
                     ts_ms INTEGER NOT NULL,
                     role TEXT NOT NULL,
                     kind TEXT NOT NULL,
                     text TEXT,
                     event_uid TEXT NOT NULL,
                     UNIQUE(source, session_id, event_uid)
                 );",
            )
            .unwrap();
        }

        let refused = SessionStore::open(StoreOptions {
            db_path: Some(db.clone()),
            read_only: true,
            ..StoreOptions::default()
        });
        let error = match refused {
            Ok(_) => panic!("a read-only open cannot migrate, so it must refuse"),
            Err(error) => error,
        };
        assert_eq!(error.code(), "DATABASE_STALE_SCHEMA");
        assert!(
            error.is_stale_schema(),
            "the refusal is typed, so a caller with a remedy can apply it"
        );
        assert!(
            !Error::query(anyhow::anyhow!("no such column: x")).is_stale_schema(),
            "an ordinary query failure is not a stale schema"
        );
        let message = error.to_string();
        assert!(
            message.contains("predates"),
            "names the mismatch: {message}"
        );
        assert!(message.contains("writable"), "names the remedy: {message}");

        // The same database opened writable migrates, and then opens
        // read-only, so the refusal is about what a read-only handle can do.
        store_at(&db);
        SessionStore::open(StoreOptions {
            db_path: Some(db),
            read_only: true,
            ..StoreOptions::default()
        })
        .unwrap();
    }
}
