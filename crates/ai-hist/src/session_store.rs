//! Embedder entry point: [`SessionStore`] and the typed evidence it returns.
//!
//! This module is the whole default public surface of the crate. Everything
//! below it — parsers, schema, paged queries, the raw `rusqlite::Connection`
//! — stays behind the `unstable-internal` feature. Cargo semver is the
//! contract; there is no separate Rust contract-version constant.
//!
//! Seven operations, one entry type:
//!
//! | Method | What it does |
//! |---|---|
//! | [`SessionStore::open`] | open (and, unless read-only, create and migrate) `ai-history.db` |
//! | [`SessionStore::sync`] | one full local sweep under the crate's `SyncRunLock` |
//! | [`SessionStore::hydrate`] | one session, by id or by transcript path, plus its bounded related transcripts |
//! | [`SessionStore::watch`] | the live-capture loop, as an iterator of ticks |
//! | [`SessionStore::sessions`] | the catalog, keyset-paged internally |
//! | [`SessionStore::session`] | everything the store holds about one session, typed |
//! | [`Source::capabilities`] | what a source can and cannot report, statically |
//!
//! The change feed (`changes_since`, #179) is not part of this module yet.
//!
//! Every struct here is `#[non_exhaustive]`, `Clone`, `Serialize` and
//! `Deserialize`. JSON columns arrive parsed; the raw string is reachable
//! through a `raw_*()` accessor and is never a public field. No signature
//! names a `rusqlite` type.

use crate::discover::{
    self, list_session_catalog_page, CatalogCursor, CatalogListOptions, ShallowSession,
};
use crate::ingest::hook::{ingest_transcript_at, ingest_transcript_at_with_home, TranscriptStatus};
use crate::ingest::hydrate::{
    hydrate_session_at, hydrate_session_at_with_home, HydrateSessionOptions, HydrateSessionResult,
};
use crate::ingest::{sync_facade_tick, sync_watch_roots, SyncTick, HOOK_HARNESSES};
use crate::paths::{home_dir, opencode_db_path};
use crate::relationship_graph::{self, RelationshipCapabilities, SessionRelationship};
use crate::session_usage::{
    session_requests_page, session_usage_summary, SessionRequest, SessionUsageSummary,
};
use crate::source_evidence::EvidenceKind;
use crate::store::{
    default_db_path, open_db, open_db_readonly, prompt_hash, schema_is_event_read_current,
    schema_is_evidence_read_current, schema_is_relationship_read_current, session_events_sized,
    session_file_edits, session_markers, session_tool_calls, session_user_turns_page, HistoryEntry,
    SessionEvent, SessionFileEdit, SessionMarker, SessionScope, SessionToolCall, SessionUserTurn,
};
use crate::usage::{normalize_usage_str, source_accounting, NormalizedUsage, UsageAccounting};
use crate::watch::{TickOutcome, TickTrigger, WatchDriver, WatchLoop};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::path::{Path, PathBuf};
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
/// addon can raise, plus the three the Rust facade adds: [`Error::SyncLocked`],
/// [`Error::SourceMismatch`] and [`Error::WatermarkAheadOfStore`].
/// [`Error::code`] is the stable `SCREAMING_SNAKE_CASE` code a host can match
/// on or forward; `Display` renders `CODE: message`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(tag = "code", content = "detail", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Error {
    /// The database could not be opened, created, or migrated — including a
    /// read-only open of a database older than the shape this version reads,
    /// which names the remedy (open it writable once, or run a sync).
    #[serde(rename = "DATABASE_OPEN_FAILED")]
    DatabaseOpen(String),
    /// A caller-supplied value was rejected before anything was read.
    InvalidArgument(String),
    /// The operation is not available on this handle: `sync`, `hydrate` and
    /// `watch` on a store opened with `read_only: true`.
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
    /// A change-feed watermark names a revision this store has not reached.
    /// Raised by `changes_since` (#179); listed here so the error vocabulary
    /// is complete before that method lands.
    WatermarkAheadOfStore(String),
}

impl Error {
    /// The stable code, as the TypeScript SDK spells it.
    pub fn code(&self) -> &'static str {
        match self {
            Self::DatabaseOpen(_) => "DATABASE_OPEN_FAILED",
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
        }
    }

    /// The human-readable part, without the code.
    pub fn message(&self) -> String {
        match self {
            Self::DatabaseOpen(m)
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
            | Self::WatermarkAheadOfStore(m) => m.clone(),
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

    fn query(error: anyhow::Error) -> Self {
        Self::classify(error, Self::Query)
    }

    fn sql(error: rusqlite::Error) -> Self {
        Self::Query(error.to_string())
    }

    fn hydration(error: anyhow::Error) -> Self {
        Self::classify(error, Self::HydrationFailed)
    }

    fn sync(error: anyhow::Error) -> Self {
        Self::classify(error, Self::SyncFailed)
    }

    fn read_only(operation: &str) -> Self {
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
    /// Every source this build knows, in catalog order.
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
    /// The filesystem roots this source's adapter watches for a provider
    /// home, as [`SessionStore::watch`] registers them. Empty for a source
    /// with no local files (relay) or no live-capture root (trajectory reads
    /// project directories, which `watch` derives per run).
    pub fn watch_roots(&self, home: &Path) -> Vec<PathBuf> {
        let roots = crate::ProviderRoots::from_home(home.to_path_buf(), opencode_db_path(home));
        discover::provider_watch_roots(self.source.as_str(), &roots)
            .iter()
            .map(|root| root.registered_path().to_path_buf())
            .collect()
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
    /// Provider home to scan instead of the process `HOME`. Provider roots
    /// still honour `CLAUDE_CONFIG_DIR`, `CODEX_HOME`, `GROK_HOME` and
    /// `OPENCODE_DB` when they are set, exactly as the CLI does.
    pub home: Option<PathBuf>,
    /// Never write. `sync`, `hydrate` and `watch` return
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
    home: Option<PathBuf>,
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
                .map_err(Error::query)?;
            if !current {
                return Err(Error::DatabaseOpen(format!(
                    "{} predates the session-evidence schema this version reads; \
                     open it writable once (or run a sync) to migrate it",
                    db_path.display()
                )));
            }
        } else {
            open_db(&db_path).map_err(|error| Error::DatabaseOpen(format!("{error:#}")))?;
        }
        Ok(Self {
            db_path,
            home: opts.home,
            read_only: opts.read_only,
        })
    }

    /// The database this store reads.
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    /// Whether this handle was opened read-only.
    pub fn read_only(&self) -> bool {
        self.read_only
    }

    fn provider_home(&self) -> PathBuf {
        self.home.clone().unwrap_or_else(home_dir)
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
        let home = self.provider_home();
        let before = catalog_fingerprint(&self.read_conn()?)?;
        let tick = self.sync_tick(&home, opts.force, opts.lock_timeout_ms)?;
        let changed = if tick.swept {
            catalog_changes(&before, &catalog_fingerprint(&self.read_conn()?)?)
        } else {
            Vec::new()
        };
        Ok(SyncReport {
            swept: tick.swept,
            changed,
        })
    }

    /// Run the sweep, retrying a held lock until `lock_timeout_ms` is spent.
    fn sync_tick(&self, home: &Path, force: bool, lock_timeout_ms: u64) -> Result<SyncTick, Error> {
        let started = Instant::now();
        let deadline = started + Duration::from_millis(lock_timeout_ms);
        loop {
            let tick = sync_facade_tick(&self.db_path, home, force).map_err(Error::sync)?;
            if tick.attempted {
                return Ok(tick);
            }
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
        match r {
            SessionRef::Id { source, session_id } => {
                let options = HydrateSessionOptions {
                    source: source.as_str().to_string(),
                    session_id: session_id.clone(),
                    scope: SessionScope::Local,
                    include_related: opts.include_related,
                };
                let result = match &self.home {
                    Some(home) => hydrate_session_at_with_home(&self.db_path, &options, home),
                    None => hydrate_session_at(&self.db_path, &options),
                }
                .map_err(Error::hydration)?;
                let status = HydrateStatus::parse(&result.status)?;
                hydrate_report(*source, session_id.clone(), status, Some(result))
            }
            SessionRef::Path { source, path } => {
                if !source.capabilities().hydrates_by_path {
                    return Err(Error::HydrationUnsupported(format!(
                        "{source} sessions cannot be hydrated by path; known harnesses: {}",
                        HOOK_HARNESSES.join(", ")
                    )));
                }
                let name = source.as_str();
                let ingest = match &self.home {
                    Some(home) => ingest_transcript_at_with_home(
                        &self.db_path,
                        home,
                        name,
                        path,
                        None,
                        opts.include_related,
                    ),
                    None => {
                        ingest_transcript_at(&self.db_path, name, path, None, opts.include_related)
                    }
                }
                .map_err(Error::hydration)?;
                let status = match ingest.status {
                    TranscriptStatus::Ingested => HydrateStatus::Hydrated,
                    TranscriptStatus::Unchanged => HydrateStatus::Unchanged,
                    TranscriptStatus::Missing => HydrateStatus::Missing,
                    TranscriptStatus::Unidentified => HydrateStatus::Unidentified,
                    TranscriptStatus::Mismatched => HydrateStatus::Mismatched,
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
        let home = self.provider_home();
        let opencode_db = opencode_db_path(&home);
        let roots = sync_watch_roots(&home, &opencode_db);
        let (reports, receiver) = mpsc::channel::<Result<TickReport, Error>>();
        let reports = Arc::new(Mutex::new(reports));

        // The sweep and the report sink run on the loop's thread, one tick at
        // a time, so a single slot carries "what this tick changed" from the
        // one to the other.
        let db_path = self.db_path.clone();
        let tick_home = home.clone();
        let baseline: Arc<Mutex<Option<CatalogFingerprint>>> = Arc::new(Mutex::new(None));
        let pending: Arc<Mutex<Vec<SessionRef>>> = Arc::new(Mutex::new(Vec::new()));
        let tick_baseline = baseline.clone();
        let tick_pending = pending.clone();
        let tick: crate::watch::TickFn = Arc::new(move |force| {
            let conn = open_db_readonly(&db_path)?;
            let mut base = tick_baseline.lock().expect("watch baseline");
            if base.is_none() {
                *base = Some(catalog_fingerprint(&conn)?);
            }
            drop(conn);
            let tick = sync_facade_tick(&db_path, &tick_home, force)?;
            if tick.swept {
                let after = catalog_fingerprint(&open_db_readonly(&db_path)?)?;
                let changed = catalog_changes(base.as_ref().expect("baseline set"), &after);
                *base = Some(after);
                *tick_pending.lock().expect("watch pending") = changed;
            }
            Ok(TickOutcome::from(tick))
        });

        let report_sink = reports.clone();
        let report_pending = pending;
        let error_sink = reports;
        let refresh_home = home.clone();
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
            sync_watch_roots(&refresh_home, &opencode_db_path(&refresh_home))
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
    /// row. Each item is one row or the error that stopped the walk.
    pub fn sessions(&self, q: CatalogQuery) -> CatalogIter {
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
            exhausted: false,
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
        let row = match r {
            SessionRef::Id { session_id, .. } => discover::catalog_row(&tx, name, session_id),
            SessionRef::Path { path, .. } => {
                discover::catalog_row_by_path(&tx, name, &path.to_string_lossy())
            }
        }
        .map_err(Error::query)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let session_id = row.session_id.clone();
        let wants = |kind: EvidenceKind| {
            opts.kinds
                .as_ref()
                .is_none_or(|kinds| kinds.contains(&kind))
        };
        let include_text = opts.include_text;
        let mut loaded = Vec::new();
        let mut diagnostics = Vec::new();

        let mut evidence = SessionEvidence {
            session: CatalogSession::from_row(row, include_text)?,
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
            coverage: source.capabilities().evidence_kinds,
            loaded: Vec::new(),
            include_text,
            diagnostics: Vec::new(),
        };

        if wants(EvidenceKind::History) {
            loaded.push(EvidenceKind::History);
            let entries =
                crate::store::session(&tx, &session_id, Some(name), None).map_err(Error::query)?;
            evidence.prompts = entries
                .into_iter()
                .map(|entry| Prompt::from_entry(entry, include_text))
                .collect();
        }
        if wants(EvidenceKind::SessionEvent) {
            loaded.push(EvidenceKind::SessionEvent);
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
            loaded.push(EvidenceKind::ToolCall);
            evidence.tool_calls = session_tool_calls(&tx, &session_id, Some(name))
                .map_err(Error::query)?
                .into_iter()
                .map(ToolCall::from_row)
                .collect();
        }
        if wants(EvidenceKind::FileEdit) {
            loaded.push(EvidenceKind::FileEdit);
            evidence.file_edits = session_file_edits(&tx, &session_id, Some(name))
                .map_err(Error::query)?
                .into_iter()
                .map(FileEdit::from_row)
                .collect();
        }
        if wants(EvidenceKind::SessionMarker) {
            loaded.push(EvidenceKind::SessionMarker);
            evidence.markers = session_markers(&tx, name, &session_id)
                .map_err(Error::query)?
                .into_iter()
                .map(|marker| Marker::from_row(marker, include_text))
                .collect();
        }
        if wants(EvidenceKind::Relationship) {
            loaded.push(EvidenceKind::Relationship);
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
        evidence.loaded = loaded;
        evidence.diagnostics = diagnostics;
        Ok(Some(evidence))
    }
}

/// How often a held sync lock is re-tried while a caller's timeout runs.
const SYNC_LOCK_RETRY: Duration = Duration::from_millis(100);

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
    /// re-tried every 100 ms while the budget lasts.
    pub lock_timeout_ms: u64,
}

/// Result of [`SessionStore::sync`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct SyncReport {
    /// A full walk ran. `false` when the stat-only source fingerprint matched
    /// the previous sweep's and nothing was opened; `changed` is then empty
    /// by construction.
    pub swept: bool,
    /// Sessions whose catalog row was created or changed by this sweep:
    /// new sessions, sessions with new activity, sessions whose discovery
    /// state or source stamp moved. Derived from the `sessions` table before
    /// and after the sweep, not from the provider walk, so a session whose
    /// only change was inside a table the catalog row does not summarise is
    /// not listed.
    pub changed: Vec<SessionRef>,
}

/// The catalog columns a sweep updates when a session changes.
type CatalogFingerprint =
    BTreeMap<(String, String), (Option<String>, Option<i64>, Option<String>, i64)>;

fn catalog_fingerprint(conn: &Connection) -> Result<CatalogFingerprint, Error> {
    let mut stmt = conn
        .prepare(
            "SELECT source, session_id, source_stamp, last_activity_ms, discovery_state, \
             parser_version FROM sessions",
        )
        .map_err(Error::sql)?;
    let rows = stmt
        .query_map([], |row| {
            Ok((
                (row.get::<_, String>(0)?, row.get::<_, String>(1)?),
                (
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, i64>(5)?,
                ),
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
}

impl Default for HydrateOptions {
    fn default() -> Self {
        Self {
            include_related: true,
        }
    }
}

/// What a hydration decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(rename_all = "snake_case")]
pub enum HydrateStatus {
    /// Evidence was read from the provider into the store.
    Hydrated,
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
    fn parse(value: &str) -> Result<Self, Error> {
        match value {
            "hydrated" => Ok(Self::Hydrated),
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
    /// The evidence kinds this hydration can have indexed: the source
    /// adapter's declared coverage. A zero count for a covered kind means the
    /// session has none of it.
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
    Ok(HydrateReport {
        session,
        status,
        capability: Some(Capability::parse(&result.capability)?),
        coverage: result.coverage,
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
    /// Sessions whose catalog row changed in this sweep; see
    /// [`SyncReport::changed`]. Empty unless `swept`.
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
    /// tick has been read; waits at most `timeout` for one.
    pub fn next_timeout(&mut self, timeout: Duration) -> Option<Result<TickReport, Error>> {
        self.recv(Some(Instant::now() + timeout))
    }

    /// Receive until `deadline` (or forever), ending when the loop's thread
    /// has finished and nothing is left to read. The loop's sinks hold the
    /// sender for as long as the loop exists, so the channel alone cannot
    /// say that the loop is over; the thread can.
    fn recv(&mut self, deadline: Option<Instant>) -> Option<Result<TickReport, Error>> {
        loop {
            match self.receiver.recv_timeout(WATCH_POLL) {
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
    /// Restrict to these sources. `None` means every source.
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
    fn from_row(row: ShallowSession, include_text: bool) -> Result<Self, Error> {
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
            first_prompt: row.first_prompt.filter(|_| include_text),
            last_assistant_text: row.last_assistant_text.filter(|_| include_text),
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

/// The iterator [`SessionStore::sessions`] returns.
pub struct CatalogIter {
    conn: Result<Connection, Error>,
    options: CatalogListOptions,
    buffer: VecDeque<ShallowSession>,
    cursor: Option<CatalogCursor>,
    exhausted: bool,
}

impl CatalogIter {
    fn fill(&mut self) -> Result<(), Error> {
        let conn = match &self.conn {
            Ok(conn) => conn,
            Err(error) => return Err(error.clone()),
        };
        self.options.after = self.cursor.take();
        let page = list_session_catalog_page(conn, &self.options).map_err(Error::query)?;
        self.exhausted = page.next_cursor.is_none();
        self.cursor = page.next_cursor;
        self.buffer.extend(page.sessions);
        Ok(())
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
        self.buffer
            .pop_front()
            .map(|row| CatalogSession::from_row(row, true))
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
    /// sha256 of the prompt, hex — present whether or not the text is.
    pub prompt_hash: String,
    pub prompt_bytes: i64,
    pub project: Option<String>,
    pub timestamp_ms: i64,
}

impl Prompt {
    fn from_entry(entry: HistoryEntry, include_text: bool) -> Self {
        Self {
            prompt_hash: entry
                .prompt_hash
                .clone()
                .unwrap_or_else(|| prompt_hash(&entry.prompt)),
            prompt_bytes: entry.prompt.len() as i64,
            prompt: include_text.then_some(entry.prompt),
            project: entry.project,
            timestamp_ms: entry.timestamp_ms,
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

    fn from_row(row: SessionMarker, include_text: bool) -> Self {
        Self {
            marker_uid: row.marker_uid,
            ts_ms: row.ts_ms,
            message_id: row.message_id,
            parent_id: row.parent_id,
            turn_id: row.turn_id,
            kind: row.kind,
            subkind: row.subkind,
            text: row.text.filter(|_| include_text),
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

    fn store_at(db: &Path) -> SessionStore {
        SessionStore::open(StoreOptions {
            db_path: Some(db.to_path_buf()),
            ..StoreOptions::default()
        })
        .unwrap()
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
        assert_eq!(error.code(), "DATABASE_OPEN_FAILED");
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
