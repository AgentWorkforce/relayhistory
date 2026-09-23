//! Fast, shallow coding-agent session discovery.
//!
//! Two operations live here, and they are deliberately distinct:
//!
//! * [`list_session_catalog`] — a cache-only query over the `sessions` table.
//!   It never touches a provider transcript and never reads `history`,
//!   `session_events` or `tool_calls`. It is what a desktop app calls on every
//!   paint.
//! * [`discover_sessions`] — inspects the known provider locations, extracts
//!   minimal metadata with *bounded* reads, upserts the `sessions` catalog and
//!   emits rows progressively in global recency order.
//!
//! # Fidelity model
//!
//! Every value in [`ShallowSession`] is one of three things, and the
//! distinction is part of the contract:
//!
//! * **Observed** — read straight out of provider data (`session_id`, `cwd`,
//!   `git_branch`, `originator`, `agent_version`, `repo_url`,
//!   `initial_commit`, `workspace_roots`, `models`, and any timestamp the
//!   provider actually records).
//! * **Derived** — computed deterministically by RelayHistory from provider
//!   data. `first_prompt` is the only derived field: it is a bounded excerpt
//!   (see [`EXCERPT_MAX_CHARS`]) of the first *substantive* human turn, with
//!   provider control/meta turns skipped. `last_activity_ms` is derived from
//!   the filesystem mtime for providers that record no timestamps (cursor).
//! * **Unavailable until full indexing** — anything not in this struct. Tool
//!   calls, file edits, per-message events, token spend, and the full
//!   transcript body all require `ai-hist sync`. [`ShallowSession::discovery_state`]
//!   says which of the two a row is: `"shallow"` or `"full"`.
//!
//! Absent metadata is `None`. Nothing here is ever invented to fill a column.
//!
//! # Product boundary
//!
//! Discovery reports *which sessions exist* and identifying metadata. It does
//! not infer project membership, work status, health, risk, or success, and it
//! does not summarize outcomes.
//!
//! # Concurrency
//!
//! Discovery deliberately does **not** take the `sync` advisory lock. Every
//! write it performs is an idempotent, stamp-guarded upsert into `sessions`,
//! so a concurrent `ai-hist sync` and a concurrent `discover` converge: the
//! full-sync path only ever upgrades a row to `discovery_state = 'full'`, and
//! the shallow path never downgrades one. Writes go through the normal
//! busy-retry connection, batched into one short transaction per read window
//! so a run of fresh rows costs one commit, not hundreds.
//!
//! Within a run, shallow reads of file-backed providers fan out across worker
//! threads (see [`ScanEnv`]). The candidate walk, the emission order, and the
//! set of sources read are identical to a serial run — parallelism changes
//! wall-clock time, never observable behaviour.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{LazyLock, Mutex, MutexGuard};

use crate::project_identity::ProjectKeyMethod;
#[cfg(any(test, feature = "unstable-internal"))]
use crate::SOURCE_CHOICES;
use crate::{
    open_db_readonly, upsert_session_presence, EvidenceKind, SessionLocation, SessionScope,
    FULL_SESSION_KINDS,
};
use crate::ingest::opencode::OpencodeLayout;
use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Version of the machine-readable session-catalog contract.
///
/// Bumped when the shape or meaning of [`ShallowSession`] / the CLI JSON
/// payloads changes in a way a consumer must notice.
/// 4 adds `project_key` / `project_key_method` to every catalog row and the
/// `project_key` filter to the listing.
pub const SESSION_CATALOG_CONTRACT_VERSION: u32 = 4;

/// Version of the shallow scanners themselves.
///
/// Persisted as the `v{N}:` prefix of `sessions.source_stamp`. Bumping it
/// invalidates every stored stamp, so a scanner that learns to extract a new
/// field re-reads sources whose bytes never changed. `parser_version` keeps its
/// existing meaning (full-ingest parser generation) and is untouched.
pub const SHALLOW_SCANNER_VERSION: u32 = 6;

/// Version 2 shipped the classification that hid standalone guardians (see
/// [`crate::codex_is_subagent`]). Their rollouts never change on disk, so the
/// only thing that can invalidate an upgraded install's stored `discovery_skips`
/// is this version, and reusing 2 would leave those catalogs permanently missing
/// the sessions. Kept as a compile-time guard so the pair cannot drift apart.
const _: () = assert!(SHALLOW_SCANNER_VERSION > 2);

/// Version 3 shipped the prompt-only Cursor reader: it recorded a first
/// prompt and an mtime, and nothing else. Version 4 extracts the injected turn
/// times, the models and the last assistant reply, and a Cursor transcript's
/// bytes do not change when the release does — so without this bump every row
/// an earlier install wrote would be served from cache with those fields null
/// forever. Kept as a compile-time guard for the same reason as the pair
/// above.
const _: () = assert!(SHALLOW_SCANNER_VERSION > 3);

/// Version 4 stored OpenCode model IDs without their provider prefix. Version
/// 5 qualifies them consistently with full ingestion (for example,
/// `anthropic/claude-sonnet`). An unchanged provider database keeps the same
/// change marker, so only the scanner-version prefix can force those cached
/// rows through the corrected reader once.
const _: () = assert!(SHALLOW_SCANNER_VERSION > 4);

/// Version 5 derived `first_prompt` from a prefix list of Claude control
/// wrappers. Version 6 derives it from `ingest::control`, which also types
/// task notifications, bare `/resume` markers and `<system-reminder>` blocks
/// as not-a-prompt, and the bytes of a transcript whose title one of those
/// used to be never change -- so only this bump sends the cached row through
/// the current classifier once.
const _: () = assert!(SHALLOW_SCANNER_VERSION > 5);

/// Most bytes a shallow head read may consume from one transcript.
pub const HEAD_SCAN_MAX_BYTES: u64 = 256 * 1024;
/// Most complete JSONL records a shallow head read may consider.
pub const HEAD_SCAN_MAX_LINES: usize = 400;
/// Most bytes a shallow tail read may consume from one transcript.
pub const TAIL_SCAN_MAX_BYTES: u64 = 64 * 1024;
/// Character cap for stored text excerpts, matching `last_assistant_text`.
pub const EXCERPT_MAX_CHARS: usize = 4096;
/// Unicode White_Space characters recognized by Rust's `str::trim`.
///
/// SQLite's one-argument `trim` removes only U+0020, so SQL predicates that
/// decide whether an excerpt is substantive must pass this exact character
/// set explicitly to stay in lockstep with [`excerpt`].
pub const EXCERPT_TRIM_WHITESPACE: &str = concat!(
    "\u{0009}\u{000A}\u{000B}\u{000C}\u{000D}\u{0020}\u{0085}\u{00A0}",
    "\u{1680}\u{2000}\u{2001}\u{2002}\u{2003}\u{2004}\u{2005}\u{2006}",
    "\u{2007}\u{2008}\u{2009}\u{200A}\u{2028}\u{2029}\u{202F}\u{205F}\u{3000}"
);
/// Default row cap for `list_session_catalog` when the caller gives none.
pub const DEFAULT_CATALOG_LIMIT: i64 = 50;

/// Catalog row: one coding-agent session as shallow discovery knows it.
///
/// See the module docs for which fields are observed and which are derived.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ShallowSession {
    /// Provider that owns the session (`claude`, `codex`, …). Observed.
    pub source: String,
    /// The provider's own session identifier. Observed. Stable across
    /// rescans; `(source, session_id)` is the catalog primary key, so the same
    /// native id under two providers is two distinct rows.
    pub session_id: String,
    /// Working directory the provider reported for the session. Observed.
    /// Per provider: claude — `cwd` from the first transcript record; codex —
    /// `session_meta.payload.cwd`; grok — `summary.json` `info.cwd`/`git_root_dir`,
    /// else the percent-decoded project directory; cursor — decoded from the
    /// project directory name; opencode — `session.directory`; relay — none
    /// (a relay thread has no working directory).
    pub cwd: Option<String>,
    /// Git branch the provider reported, last observed value. Observed.
    pub git_branch: Option<String>,
    /// Earliest activity timestamp the provider records. Observed. `None`
    /// when the provider recorded none — for cursor that means the build
    /// wrote no `<timestamp>` tag into any turn it read, not that cursor
    /// never records a time.
    pub first_activity_ms: Option<i64>,
    /// Latest activity timestamp. Observed where the provider records one;
    /// for cursor that is the last readable injected `<timestamp>`, falling
    /// back to the file mtime when the read found none.
    pub last_activity_ms: Option<i64>,
    /// Bounded excerpt of the first substantive human prompt. **Derived.**
    pub first_prompt: Option<String>,
    /// Bounded excerpt of the last assistant text. Observed. Most providers
    /// populate it only on the full-ingest path, so their shallow rows leave
    /// it `None`; cursor fills it from the bounded tail read.
    pub last_assistant_text: Option<String>,
    /// Model ids observed in the bounded read. Observed, best effort: never a
    /// reason to widen a read, so an empty list means "not seen cheaply", not
    /// "no model".
    pub models: Vec<String>,
    /// Client that originated the session (codex `session_meta.originator`).
    /// Observed.
    pub originator: Option<String>,
    /// Agent CLI version (codex `cli_version`, claude record `version`).
    /// Observed.
    pub agent_version: Option<String>,
    /// Repository remote URL, when the provider records one. Observed.
    pub repo_url: Option<String>,
    /// Commit the session started from, when the provider records one. Observed.
    pub initial_commit: Option<String>,
    /// Extra workspace roots, when the provider records them. Observed.
    pub workspace_roots: Vec<String>,
    /// Canonical project identity: the `origin` remote canonicalized to
    /// `host/owner/repo`, or the working directory when no remote resolves.
    /// **Derived** — see [`crate::project_identity`]. `None` only while the
    /// row has neither a `cwd` nor a `repo_url` to derive one from.
    pub project_key: Option<String>,
    /// How [`ShallowSession::project_key`] was arrived at: `remote`, `path`,
    /// or `inherited` from a delegating parent. Read this rather than
    /// guessing from whether the key looks like a path.
    pub project_key_method: Option<String>,
    /// Path of the provider file or database this row came from, when local.
    pub raw_path: Option<String>,
    /// Change stamp of the raw source at scan time, `v{scanner}:{provider stamp}`.
    pub source_stamp: Option<String>,
    /// `"shallow"` (catalog row only) or `"full"` (full evidence ingested).
    pub discovery_state: String,
    /// Places where this logical provider session is known to exist.
    /// A session may be present both locally and remotely while remaining one
    /// catalog row.
    pub locations: Vec<String>,
    /// `true` when this row was served from the catalog without re-reading the
    /// provider source — either a cache-only list, or a rescan whose stamp
    /// matched.
    pub from_cache: bool,
}

/// A cheaply enumerated discovery candidate.
///
/// Producing one must not read file contents: a directory walk plus `stat`,
/// or one small indexed query for the database-backed providers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    /// Provider that produced the candidate.
    pub source: &'static str,
    /// Opaque provider-scoped handle: a file path, or a session id for the
    /// database-backed providers.
    pub locator: String,
    /// Session id when enumeration already knows it without reading anything
    /// (cursor, opencode, relay); `None` when only the shallow read can say.
    pub session_id: Option<String>,
    /// Recency signal used for global ordering — a provider timestamp where
    /// one is available for free, else the file mtime.
    pub recency_hint_ms: Option<i64>,
    /// Raw provider change marker; stored as `v{scanner}:{stamp}`.
    pub stamp: String,
}

/// Counters describing the work one discovery run actually did.
///
/// Exposed so callers (and tests) can assert bounded behaviour without a wall
/// clock: a limited request must not read the whole archive, a cache-only list
/// must open zero files, and an unchanged rescan must perform zero shallow
/// reads.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct DiscoveryCounters {
    /// Candidates produced by enumeration, before the global limit.
    pub candidates_enumerated: u64,
    /// Candidates whose source was actually read.
    pub shallow_reads: u64,
    /// Candidates served from the catalog because their stamp was unchanged.
    pub skipped_unchanged: u64,
    /// Provider files and live provider databases opened for reading.
    pub files_opened: u64,
    /// Bytes read explicitly from file-backed provider sources. SQLite does
    /// not expose exact filesystem bytes, so database reads never contribute.
    pub bytes_read: u64,
    /// Bounded provider data queries issued. Schema-capability introspection is
    /// excluded. Currently used by OpenCode.
    pub provider_queries: u64,
    /// Rows returned by bounded provider data queries. Currently used by
    /// OpenCode to make session/message/part work visible without pretending
    /// SQLite reports exact bytes.
    pub records_inspected: u64,
}

/// Thread-safe accumulator behind [`DiscoveryCounters`], shared with the read
/// workers a run fans out.
#[derive(Default)]
struct CounterCell {
    candidates_enumerated: AtomicU64,
    shallow_reads: AtomicU64,
    skipped_unchanged: AtomicU64,
    files_opened: AtomicU64,
    bytes_read: AtomicU64,
    provider_queries: AtomicU64,
    records_inspected: AtomicU64,
}

impl CounterCell {
    fn snapshot(&self) -> DiscoveryCounters {
        DiscoveryCounters {
            candidates_enumerated: self.candidates_enumerated.load(Ordering::Relaxed),
            shallow_reads: self.shallow_reads.load(Ordering::Relaxed),
            skipped_unchanged: self.skipped_unchanged.load(Ordering::Relaxed),
            files_opened: self.files_opened.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            provider_queries: self.provider_queries.load(Ordering::Relaxed),
            records_inspected: self.records_inspected.load(Ordering::Relaxed),
        }
    }
}

/// Environment one discovery run operates in: where the provider data lives,
/// the catalog connection, and the run's counters.
pub struct DiscoveryEnv<'a> {
    /// Home directory the file-backed providers are rooted at.
    pub home: PathBuf,
    /// Claude Code configuration root.
    pub claude_config_dir: PathBuf,
    /// Codex state root.
    pub codex_home: PathBuf,
    /// Grok state root.
    pub grok_home: PathBuf,
    /// Path to the opencode database.
    pub opencode_db: PathBuf,
    /// Root of OpenCode's legacy `storage/` JSON tree, read only when there
    /// is no `opencode.db`.
    pub opencode_storage_dir: PathBuf,
    conn: &'a Connection,
    counters: CounterCell,
}

impl<'a> DiscoveryEnv<'a> {
    /// Build an environment from the process environment.
    #[cfg(any(test, feature = "unstable-internal"))]
    pub fn new(conn: &'a Connection) -> Self {
        let roots = crate::ProviderRoots::from_env(crate::home_dir());
        Self::with_provider_roots(conn, roots)
    }

    pub(crate) fn with_provider_roots(
        conn: &'a Connection,
        roots: crate::ProviderRoots,
    ) -> Self {
        Self {
            home: roots.home,
            claude_config_dir: roots.claude,
            codex_home: roots.codex,
            grok_home: roots.grok,
            opencode_db: roots.opencode_db,
            opencode_storage_dir: roots.opencode_storage_dir,
            conn,
            counters: CounterCell::default(),
        }
    }

    /// Build an environment with explicit roots, for hosts that keep provider
    /// data somewhere other than `$HOME` (and for tests, which must not mutate
    /// process-wide environment variables).
    #[cfg(feature = "unstable-internal")]
    pub fn with_roots(conn: &'a Connection, home: PathBuf, opencode_db: PathBuf) -> Self {
        Self::with_provider_roots(conn, crate::ProviderRoots::from_home(home, opencode_db))
    }

    /// Build an environment with every provider root supplied explicitly.
    #[cfg(any(test, feature = "unstable-internal"))]
    pub fn with_all_roots(
        conn: &'a Connection,
        home: PathBuf,
        claude_config_dir: PathBuf,
        codex_home: PathBuf,
        grok_home: PathBuf,
        opencode_db: PathBuf,
    ) -> Self {
        let opencode_storage_dir = opencode_db
            .parent()
            .map(|parent| parent.join("storage"))
            .unwrap_or_else(|| home.join(".local/share/opencode/storage"));
        Self::with_provider_roots(
            conn,
            crate::ProviderRoots {
                home,
                claude: claude_config_dir,
                codex: codex_home,
                grok: grok_home,
                opencode_db,
                opencode_storage_dir,
                trajectory_roots: None,
                use_env_roots: false,
            },
        )
    }

    /// Point the legacy JSON tree somewhere other than beside the database.
    /// Hosts that set `OPENCODE_STORAGE_DIR` independently of `OPENCODE_DB`
    /// need this; so do tests, which must not mutate process-wide variables.
    #[cfg(any(test, feature = "unstable-internal"))]
    #[must_use]
    pub fn with_opencode_storage_dir(mut self, storage_dir: PathBuf) -> Self {
        self.opencode_storage_dir = storage_dir;
        self
    }

    /// The catalog connection. `relay` discovers from already-synced local
    /// rows through this and never opens a socket.
    pub fn conn(&self) -> &Connection {
        self.conn
    }

    /// The thread-shareable slice of this environment: provider roots plus
    /// the run's counters, without the catalog connection. What a shallow
    /// read receives, on whatever thread it runs.
    pub fn scan(&self) -> ScanEnv<'_> {
        ScanEnv {
            #[cfg(feature = "unstable-internal")]
            home: &self.home,
            #[cfg(feature = "unstable-internal")]
            claude_config_dir: &self.claude_config_dir,
            #[cfg(feature = "unstable-internal")]
            codex_home: &self.codex_home,
            #[cfg(feature = "unstable-internal")]
            grok_home: &self.grok_home,
            opencode_db: &self.opencode_db,
            opencode_storage_dir: &self.opencode_storage_dir,
            counters: &self.counters,
        }
    }

    /// Counters accumulated so far.
    pub fn counters(&self) -> DiscoveryCounters {
        self.counters.snapshot()
    }

    fn note_candidates(&self, count: u64) {
        self.counters
            .candidates_enumerated
            .fetch_add(count, Ordering::Relaxed);
    }

    fn note_shallow_read(&self) {
        self.counters.shallow_reads.fetch_add(1, Ordering::Relaxed);
    }

    fn note_skipped(&self) {
        self.counters
            .skipped_unchanged
            .fetch_add(1, Ordering::Relaxed);
    }
}

/// What a shallow read is allowed to touch: the provider roots and the run's
/// counters, never the catalog connection. `Sync`, so the engine can fan
/// bounded reads out across worker threads.
#[derive(Clone, Copy)]
pub struct ScanEnv<'a> {
    /// Home directory the file-backed providers are rooted at.
    #[cfg(feature = "unstable-internal")]
    pub home: &'a Path,
    /// Claude Code configuration root.
    #[cfg(feature = "unstable-internal")]
    pub claude_config_dir: &'a Path,
    /// Codex state root.
    #[cfg(feature = "unstable-internal")]
    pub codex_home: &'a Path,
    /// Grok state root.
    #[cfg(feature = "unstable-internal")]
    pub grok_home: &'a Path,
    /// Path to the opencode database.
    pub opencode_db: &'a Path,
    /// Root of OpenCode's legacy `storage/` JSON tree.
    pub opencode_storage_dir: &'a Path,
    counters: &'a CounterCell,
}

impl ScanEnv<'_> {
    fn note_open(&self) {
        self.counters.files_opened.fetch_add(1, Ordering::Relaxed);
    }

    fn note_bytes(&self, bytes: u64) {
        self.counters.bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }

    fn note_query(&self) {
        self.counters
            .provider_queries
            .fetch_add(1, Ordering::Relaxed);
    }

    fn note_records(&self, records: u64) {
        self.counters
            .records_inspected
            .fetch_add(records, Ordering::Relaxed);
    }
}

/// Where the local providers' evidence lives, without the catalog connection
/// a [`DiscoveryEnv`] carries. What [`ShallowSessionProvider::watch_roots`]
/// resolves its directories against.
#[derive(Debug, Clone, Copy)]
pub struct ProviderRoots<'a> {
    /// Home directory the file-backed providers are rooted at.
    ///
    /// Only for providers that have no configurable root of their own. A
    /// provider whose root *is* configurable reads its own field below, so
    /// that `CLAUDE_CONFIG_DIR` and friends move the watch as well as the
    /// sweep; `paths::tests::provider_roots_have_one_owner` holds that line.
    pub home: &'a Path,
    /// Claude Code configuration root.
    pub claude: &'a Path,
    /// Codex state root.
    pub codex: &'a Path,
    /// Grok state root.
    pub grok: &'a Path,
    /// Path to the opencode database.
    pub opencode_db: &'a Path,
}

/// One path the live-capture watcher monitors, and how deeply.
///
/// Depth is not a detail. A transcript root has to be watched recursively,
/// because a new session is a new file inside a directory that may not exist
/// yet. A flat log such as `~/.claude/history.jsonl` is watched through its
/// *parent*, non-recursively: watching the file itself stops firing the moment
/// the file is replaced rather than appended to, and watching the parent
/// recursively would pull in everything else under `~/.claude` — the todo
/// files and shell snapshots a busy session rewrites constantly — so every one
/// of those would wake a full fingerprint walk.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct WatchRoot {
    pub path: PathBuf,
    /// How much of `path` is watched.
    pub depth: WatchDepth,
    /// The same root with its symlinks resolved, once the filesystem has been
    /// asked — see [`WatchRoot::resolve`]. `None` until then, and for a root
    /// whose registration path does not exist yet.
    pub canonical: Option<PathBuf>,
}

/// How much of a [`WatchRoot`]'s path is watched.
///
/// Ordered narrowest-first, so merging two claims on the same path is a `max`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum WatchDepth {
    /// Only the one file the root names.
    ///
    /// Registered through the file's *parent*, because a watch on the file
    /// itself stops firing the moment an atomic rewrite replaces it — but
    /// every other entry in that parent is filtered back out. That distinction
    /// matters for a `TRAJECTORY_ROOT` naming a single JSON file: its parent
    /// can be `$HOME`, or `/`, and watching that as a tree would turn every
    /// unrelated write on the machine into a forced sweep.
    File,
    /// This directory's own entries, and nothing below them.
    Directory,
    /// This path and its whole subtree.
    Tree,
}

/// One path in the single spelling every comparison uses.
///
/// A watch root and a backend event have to be comparable, and they arrive
/// spelled differently: a root can be given relatively (`TRAJECTORY_ROOT=
/// trajectory.json`), while the backend reports what it was registered
/// with — so a relative root and an absolute event never match and the file
/// is watched but never seen to change. The fix is not to compare cleverly
/// but to hold one spelling: every root is absolute from the moment it is
/// built, so the registration is absolute too, and an event path is put
/// through the same function before it is matched.
///
/// Lexical, not canonical: `.` and `..` are resolved textually and symlinks
/// are left alone. Resolving symlinks would mean a filesystem call per event
/// and a different answer for a root whose target moves; textual resolution
/// is the same answer on both sides, which is what matching needs.
pub(crate) fn watch_path(path: &Path) -> PathBuf {
    let mut resolved = if path.is_absolute() {
        PathBuf::new()
    } else {
        std::env::current_dir().unwrap_or_default()
    };
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                resolved.pop();
            }
            other => resolved.push(other.as_os_str()),
        }
    }
    resolved
}

impl WatchRoot {
    /// Watch this path and everything under it.
    pub fn tree(path: impl Into<PathBuf>) -> Self {
        Self {
            path: watch_path(&path.into()),
            depth: WatchDepth::Tree,
            canonical: None,
        }
    }

    /// Watch only this directory's own entries.
    pub fn directory(path: impl Into<PathBuf>) -> Self {
        Self {
            path: watch_path(&path.into()),
            depth: WatchDepth::Directory,
            canonical: None,
        }
    }

    /// Watch only this one file, through its parent directory.
    pub fn file(path: impl Into<PathBuf>) -> Self {
        Self {
            path: watch_path(&path.into()),
            depth: WatchDepth::File,
            canonical: None,
        }
    }

    /// The path actually handed to the filesystem backend.
    ///
    /// Only a [`WatchDepth::File`] root differs from the path it names: it is
    /// registered through its parent directory.
    ///
    /// The path is already absolute — [`watch_path`] made it so when the root
    /// was built — so the parent is a real directory rather than the empty
    /// path a bare relative name would have yielded, and the backend reports
    /// events under it in the same spelling the root holds.
    #[cfg(any(test, feature = "unstable-internal", feature = "fs-events"))]
    pub fn registered_path(&self) -> &Path {
        match self.depth {
            WatchDepth::File => self.path.parent().unwrap_or(self.path.as_path()),
            _ => self.path.as_path(),
        }
    }

    /// Ask the filesystem what this root's registration path really is, and
    /// remember it alongside the spelling the root was given.
    ///
    /// Two spellings, because the two backends disagree about which one they
    /// report. inotify echoes the path the watch was registered with; macOS
    /// FSEvents reports the *real* path — `/private/var/...` for anything
    /// under `/var`, and the resolved target of any symlink on the way. A
    /// root reached through a symlink therefore registers, is reported as
    /// watched, and never matches an event, which is the failure that looks
    /// most like everything working.
    ///
    /// Resolving the *registration* path rather than the root itself is what
    /// makes this work for a file that does not exist yet: the directory it
    /// will appear in does, so the canonical spelling is known before the
    /// first write. Called once per registration, never per event.
    #[cfg(any(feature = "unstable-internal", feature = "fs-events"))]
    pub fn resolve(&mut self) {
        let Ok(directory) = std::fs::canonicalize(self.registered_path()) else {
            // Not there yet. The root stays pending and this is asked again
            // when it is retried.
            self.canonical = None;
            return;
        };
        self.canonical = Some(match self.depth {
            WatchDepth::File => match self.path.file_name() {
                Some(name) => directory.join(name),
                None => directory,
            },
            _ => directory,
        });
    }

    /// The registration path in its resolved spelling, when one is known.
    #[cfg(any(feature = "unstable-internal", feature = "fs-events"))]
    pub fn canonical_registered_path(&self) -> Option<&Path> {
        let canonical = self.canonical.as_deref()?;
        Some(match self.depth {
            WatchDepth::File => canonical.parent().unwrap_or(canonical),
            _ => canonical,
        })
    }

    /// The one key this root's registration is known by.
    ///
    /// Everything that looks a registration up by path — registering it,
    /// dropping it, recording that the backend reported it gone, and asking
    /// whether it was — has to use this and only this. Two keys for one
    /// registration is how a removal gets recorded under a spelling the
    /// lookup does not use, and a root then stays "watched" over a watch the
    /// kernel has already dropped.
    ///
    /// The resolved spelling once the filesystem has been asked, because that
    /// is what is registered; the lexical one until then, when there is
    /// nothing else to go on.
    #[cfg(any(feature = "unstable-internal", feature = "fs-events"))]
    pub fn registration_key(&self) -> &Path {
        self.canonical_registered_path()
            .unwrap_or_else(|| self.registered_path())
    }

    /// Whether `path` is one of the spellings this root registers under.
    #[cfg(any(feature = "unstable-internal", feature = "fs-events"))]
    pub fn registers_at(&self, path: &Path) -> bool {
        self.registered_path() == path || self.canonical_registered_path() == Some(path)
    }

    /// Whether an event on `path` is one this root asked for.
    #[cfg(any(test, feature = "unstable-internal", feature = "fs-events"))]
    pub fn covers(&self, path: &Path) -> bool {
        // Either spelling. The lexical one is what inotify reports back, the
        // resolved one is what FSEvents reports, and a root is the same root
        // under both.
        self.covers_as(&self.path, path)
            || self
                .canonical
                .as_deref()
                .is_some_and(|canonical| self.covers_as(canonical, path))
    }

    #[cfg(any(test, feature = "unstable-internal", feature = "fs-events"))]
    fn covers_as(&self, root: &Path, path: &Path) -> bool {
        match self.depth {
            WatchDepth::Tree => path.starts_with(root),
            WatchDepth::Directory => path == root || path.parent() == Some(root),
            WatchDepth::File => path == root,
        }
    }
}

/// Every path the live-capture watcher should monitor for the given adapters,
/// deduplicated and ordered.
///
/// Roots that do not exist are kept rather than dropped: a provider installed
/// after the watcher started still has to become covered, so the loop retries
/// them, and a caller can report which ones are not covered yet.
///
/// A path named both ways keeps the wider watch.
pub fn watch_roots(
    providers: &[Box<dyn ShallowSessionProvider>],
    roots: &ProviderRoots<'_>,
) -> Vec<WatchRoot> {
    let mut widest: BTreeMap<PathBuf, WatchDepth> = BTreeMap::new();
    let mut order = Vec::new();
    for provider in providers {
        for root in provider.watch_roots(roots) {
            match widest.get_mut(&root.path) {
                Some(depth) => *depth = (*depth).max(root.depth),
                None => {
                    widest.insert(root.path.clone(), root.depth);
                    order.push(root.path);
                }
            }
        }
    }
    order
        .into_iter()
        .map(|path| {
            let depth = widest[&path];
            WatchRoot {
                path,
                depth,
                canonical: None,
            }
        })
        .collect()
}

/// A stat-only fold over everything discovery would enumerate, cheap enough to
/// run on every watch tick.
///
/// The value is `"{candidates}:{bytes}:{hash}"`. It is a change *detector*, not
/// a content hash: two distinct source states could in principle collide. For
/// append-only transcripts inside one inter-tick window that is not a practical
/// concern, and the worst case is one skipped no-op sweep — never lost
/// evidence, because the per-session stamps still catch up on the next tick
/// whose fingerprint differs.
///
/// `bytes` is the size each adapter's stamp reports, summed; it is a cheap
/// guard that makes an accidental collision harder to hit, and the hash is the
/// load-bearing part. Nothing here opens a file, so a tick over unchanged
/// sources leaves [`DiscoveryCounters::files_opened`] at zero.
#[cfg(feature = "unstable-internal")]
pub fn source_fingerprint(
    env: &DiscoveryEnv<'_>,
    providers: &[&dyn ShallowSessionProvider],
) -> Result<String> {
    source_fingerprint_with(env, providers, &[])
}

/// [`source_fingerprint`] plus inputs no adapter owns.
///
/// A sweep can read sources discovery never enumerates — flat per-harness
/// logs, and records that are deliberately [`DISCOVERY_EXEMPTIONS`] entries.
/// Anything the sweep reads has to be in the fold, or the fast path will skip
/// a sweep that had work to do.
pub fn source_fingerprint_with(
    env: &DiscoveryEnv<'_>,
    providers: &[&dyn ShallowSessionProvider],
    extra: &[Candidate],
) -> Result<String> {
    let mut candidates: u64 = 0;
    let mut bytes: u64 = 0;
    let mut hash: u64 = 0;
    let mut fold = |candidate: &Candidate| {
        candidates = candidates.wrapping_add(1);
        bytes = bytes.wrapping_add(stamp_reported_bytes(&candidate.stamp));
        hash = hash.wrapping_add(fingerprint_hash(
            candidate.source,
            &candidate.locator,
            &candidate.stamp,
        ));
    };
    for provider in providers {
        for candidate in provider.fingerprint_inputs(env)? {
            fold(&candidate);
        }
    }
    for candidate in extra {
        fold(candidate);
    }
    Ok(format!("{candidates}:{bytes}:{hash:016x}"))
}

/// [`source_fingerprint`] over the built-in adapters.
#[cfg(feature = "unstable-internal")]
pub fn source_fingerprint_for(
    env: &DiscoveryEnv<'_>,
    providers: &[Box<dyn ShallowSessionProvider>],
) -> Result<String> {
    let refs = providers
        .iter()
        .map(|provider| provider.as_ref())
        .collect::<Vec<_>>();
    source_fingerprint(env, &refs)
}

/// The byte count a file stamp reports, best effort.
///
/// File stamps are `"{mtime_nanos}:{len}"`, joined with `|` where one session
/// is stamped from several files (grok's chat plus its summary). A stamp that
/// is not shaped that way — a database adapter's `"{updated}:{count}"` — still
/// contributes through the hash, so an unparseable size costs nothing.
fn stamp_reported_bytes(stamp: &str) -> u64 {
    stamp
        .split('|')
        .filter_map(|part| part.rsplit(':').next())
        .filter_map(|len| len.parse::<u64>().ok())
        .fold(0u64, |total, len| total.wrapping_add(len))
}

/// FNV-1a over one candidate's identity and change stamp. Summed rather than
/// chained across candidates so the fold does not depend on enumeration order.
pub(crate) fn fingerprint_hash(source: &str, locator: &str, stamp: &str) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x1000_0000_01b3;
    let mut hash = OFFSET;
    for bytes in [source.as_bytes(), b"\0", locator.as_bytes(), b"\0", stamp.as_bytes()] {
        for byte in bytes {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(PRIME);
        }
    }
    hash
}

/// What a provider's [`read_shallow`](ShallowSessionProvider::read_shallow)
/// needs access to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShallowReadAccess {
    /// Provider files (and provider-owned databases) only. The engine may run
    /// these reads on worker threads, several at a time.
    Filesystem,
    /// The RelayHistory catalog connection. These reads run serially on the
    /// engine thread with `catalog` present.
    Catalog,
}

/// Opaque lifetime guard held by the discovery engine for one provider pass.
///
/// Most adapters are stateless and use no guard. An adapter that pins live
/// provider state can return a lock guard so two calls through a reusable
/// registry cannot replace each other's snapshot between enumeration and
/// shallow reads.
#[doc(hidden)]
pub trait DiscoveryPassGuard {}

impl<T> DiscoveryPassGuard for T {}

/// One provider's shallow adapter.
///
/// Implementations must be cheap: [`enumerate`](ShallowSessionProvider::enumerate)
/// may stat but not read, and [`read_shallow`](ShallowSessionProvider::read_shallow)
/// must stay inside [`HEAD_SCAN_MAX_BYTES`] / [`TAIL_SCAN_MAX_BYTES`] per
/// source. Returning `Ok(None)` from `read_shallow` means "this candidate is
/// not a session" (a codex subagent thread, a file with no usable metadata) —
/// it is not an error.
pub trait ShallowSessionProvider: Sync {
    /// Start one enumerate/read cycle. The returned guard remains alive until
    /// every candidate from this pass has been consumed.
    fn begin_discovery_pass(&self) -> Result<Option<Box<dyn DiscoveryPassGuard + '_>>> {
        Ok(None)
    }

    /// Stable acquisition identity, independent of the evidence source.
    fn connector_id(&self) -> &str {
        self.source()
    }
    /// Non-secret instance key. Applications must override this for multiple accounts.
    fn connector_instance(&self) -> &str {
        "default"
    }

    /// Called only after the full explicit connector selection is validated.
    #[cfg(feature = "unstable-internal")]
    fn check_available(&self, _home: &Path) -> Result<()> {
        Ok(())
    }
    /// Acquire exactly this observation's locator. Listing-only adapters keep
    /// the default capability response; adapters must not consult other locators.
    #[cfg(feature = "unstable-internal")]
    fn acquire(
        &self,
        _home: &Path,
        _observation: &crate::observations::SessionObservation,
    ) -> Result<crate::sources::AcquiredEvidence> {
        Ok(crate::sources::AcquiredEvidence::CapabilityLimited {
            code: "PROVIDER_CAPABILITY_LIMITED",
            message: "this source connector provides catalog discovery only".into(),
        })
    }

    /// The `SOURCE_CHOICES` name this adapter covers.
    fn source(&self) -> &'static str;
    /// Which evidence kinds this source's local parser is *able* to produce.
    ///
    /// Declared, not measured: it is the ceiling on what a completed local
    /// hydration can have indexed, and it is a property of the adapter rather
    /// than of any one session or database — the same shape as
    /// [`crate::relationship_capabilities`]. Hydration derives its reported
    /// `capability` from it, so a prompt-only provider reports `partial`
    /// instead of claiming `full` over evidence it never parses.
    ///
    /// The default is "nothing declared", which reports `partial`. A new
    /// adapter therefore understates its coverage until someone writes the
    /// list down, rather than silently overstating it.
    fn evidence_kinds(&self) -> &'static [EvidenceKind] {
        &[]
    }
    /// Where this adapter's evidence lives. Local file-backed adapters keep
    /// the default; remote connectors (see [`crate::remote`]) override it, and
    /// the engine records their presences and stamps under that location.
    fn location(&self) -> SessionLocation {
        SessionLocation::Local
    }
    /// Cheap enumeration: directory walk + stat, or one indexed query. For a
    /// remote connector the bounded service listing *is* the enumeration —
    /// there is no cheaper way to learn what exists.
    fn enumerate(
        &self,
        env: &DiscoveryEnv<'_>,
        requested_limit: Option<usize>,
    ) -> Result<Vec<Candidate>>;
    /// What [`read_shallow`](ShallowSessionProvider::read_shallow) touches.
    fn read_access(&self) -> ShallowReadAccess {
        ShallowReadAccess::Filesystem
    }
    /// Directories the live-capture watcher should monitor for this adapter's
    /// evidence, watched recursively.
    ///
    /// Return the widest directory whose subtree the provider writes into, not
    /// the individual transcripts: a new session is a *new file*, and a watch
    /// on a path that does not exist yet never fires. Adapters with no local
    /// files (remote connectors, the relay adapter reading our own catalog)
    /// return none, and the watcher skips roots that do not exist.
    fn watch_roots(&self, _roots: &ProviderRoots<'_>) -> Vec<WatchRoot> {
        Vec::new()
    }
    /// The change signal [`source_fingerprint`] folds for this adapter.
    ///
    /// The default is [`enumerate`](ShallowSessionProvider::enumerate), which
    /// for the file-backed adapters is a directory walk plus one `stat` per
    /// file and opens nothing — exactly the cost the watch fast path is
    /// willing to pay every tick. Override when enumeration costs more than a
    /// stat (a database query), returning the cheapest signal that still moves
    /// whenever the provider's sources move; return none when the adapter's
    /// rows are derived from RelayHistory's own catalog, where only our own
    /// writes can move them.
    ///
    /// A remote connector contributes nothing by default: its enumeration is a
    /// network listing, which is not a cost a per-tick fast path may pay, and
    /// watch mode does not drive remote acquisition in the first place.
    fn fingerprint_inputs(&self, env: &DiscoveryEnv<'_>) -> Result<Vec<Candidate>> {
        if self.location() != SessionLocation::Local {
            return Ok(Vec::new());
        }
        self.enumerate(env, None)
    }
    /// Bounded read of one candidate into a catalog row.
    ///
    /// Runs on a worker thread with `catalog` absent, unless the provider
    /// declares [`ShallowReadAccess::Catalog`] — then it runs on the engine
    /// thread and `catalog` is always present.
    fn read_shallow(
        &self,
        scan: &ScanEnv<'_>,
        catalog: Option<&Connection>,
        candidate: &Candidate,
    ) -> Result<Option<ShallowSession>>;
}

/// A `SOURCE_CHOICES` entry that deliberately has no shallow adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SourceExemption {
    /// The exempt source name.
    pub source: &'static str,
    /// Why it has no adapter. Shown in machine-readable summaries.
    pub reason: &'static str,
}

/// Sources with no shallow adapter, and why.
///
/// Paired with [`shallow_providers`] by a registry regression test that
/// asserts every `SOURCE_CHOICES` entry is covered by exactly one of the two
/// lists — so adding a provider to `SOURCE_CHOICES` fails the build until
/// someone decides whether it is discoverable.
pub const DISCOVERY_EXEMPTIONS: &[SourceExemption] = &[SourceExemption {
    source: "trajectory",
    reason: "derived trajectory records, not provider sessions",
}];

/// Every shallow adapter, one per discoverable source.
pub fn shallow_providers() -> Vec<Box<dyn ShallowSessionProvider>> {
    vec![
        Box::new(ClaudeProvider),
        Box::new(CodexProvider),
        Box::new(CursorProvider),
        Box::new(GrokProvider),
        Box::new(OpencodeProvider::default()),
        Box::new(RelayProvider),
    ]
}

/// Declared local evidence coverage for one source, resolved from the shallow
/// provider registry.
///
/// A pure table: it opens nothing, so it answers the same way for a source
/// whose database is missing, and a source with no registered adapter (or one
/// that has not declared its kinds) declares nothing.
pub fn declared_evidence_kinds(source: &str) -> &'static [EvidenceKind] {
    shallow_providers()
        .iter()
        .find(|provider| provider.source() == source)
        .map_or(&[][..], |provider| provider.evidence_kinds())
}

/// The `FULL_SESSION_KINDS` a source's local parser does not produce, in the
/// canonical order. Empty means the source's declared coverage is complete.
pub fn missing_evidence_kinds(source: &str) -> Vec<EvidenceKind> {
    let declared = declared_evidence_kinds(source);
    FULL_SESSION_KINDS
        .iter()
        .copied()
        .filter(|kind| !declared.contains(kind))
        .collect()
}

// ---------------------------------------------------------------------------
// bounded reads
// ---------------------------------------------------------------------------

/// The bounded byte regions a read recovered from one file, exposed as lazy
/// line iterators so a scanner that finds what it needs early never pays to
/// parse the rest.
///
/// Only newline-terminated records are visible, matching the project's
/// incomplete-record convention: a transcript being written right now has a
/// partial trailing line, and that line is not yet a record.
struct BoundedJsonl {
    /// Complete-line region from the start of the file — the whole file when
    /// it fits the head budget.
    head: Vec<u8>,
    /// Complete-line region ending at the last complete record, for a file
    /// past the head budget. Empty when `head` reaches end of file and serves
    /// as its own tail.
    tail: Vec<u8>,
}

/// Truncate a freshly read buffer to its final newline, dropping a partial
/// trailing record.
fn keep_complete_lines(buffer: &mut Vec<u8>) {
    match buffer.iter().rposition(|&byte| byte == b'\n') {
        Some(last_newline) => buffer.truncate(last_newline + 1),
        None => buffer.clear(),
    }
}

/// Keep newline-terminated records plus a parseable final object.
///
/// Used only for bytes already captured by a hook through one immutable file
/// handle. A live bounded read still drops its trailing line because the
/// harness may be writing it concurrently; once the snapshot is complete, a
/// valid final JSON object is evidence even when the producer omitted `\n`.
fn keep_snapshot_records(buffer: &mut Vec<u8>) {
    if buffer.ends_with(b"\n") {
        return;
    }
    let final_start = buffer
        .iter()
        .rposition(|&byte| byte == b'\n')
        .map_or(0, |newline| newline + 1);
    if parse_record(&buffer[final_start..]).is_some_and(|value| value.is_object()) {
        return;
    }
    keep_complete_lines(buffer);
}

fn trimmed_record(line: &[u8]) -> Option<&[u8]> {
    let mut line = line;
    while let [rest @ .., last] = line {
        if last.is_ascii_whitespace() {
            line = rest;
        } else {
            break;
        }
    }
    while let [first, rest @ ..] = line {
        if first.is_ascii_whitespace() {
            line = rest;
        } else {
            break;
        }
    }
    (!line.is_empty()).then_some(line)
}

/// Non-empty complete records, oldest first.
fn records(buffer: &[u8]) -> impl Iterator<Item = &[u8]> {
    buffer
        .split(|&byte| byte == b'\n')
        .filter_map(trimmed_record)
}

/// Non-empty complete records, newest first.
fn records_rev(buffer: &[u8]) -> impl Iterator<Item = &[u8]> {
    buffer
        .rsplit(|&byte| byte == b'\n')
        .filter_map(trimmed_record)
}

impl BoundedJsonl {
    /// Records from the start of the file, oldest first, capped at
    /// [`HEAD_SCAN_MAX_LINES`].
    fn head_records(&self) -> impl Iterator<Item = &[u8]> {
        records(&self.head).take(HEAD_SCAN_MAX_LINES)
    }

    /// Records from the end of the file, newest first. For a file inside the
    /// head budget this walks the head region backwards, so every record —
    /// including ones past the head line cap — is reachable.
    fn tail_records_rev(&self) -> impl Iterator<Item = &[u8]> {
        let region = if self.tail.is_empty() {
            &self.head
        } else {
            &self.tail
        };
        records_rev(region)
    }
}

/// Parse one record. Falls back through a lossy decode so a record holding
/// invalid UTF-8 inside its strings still parses, as it always has.
fn parse_record(line: &[u8]) -> Option<Value> {
    serde_json::from_slice(line).ok().or_else(|| {
        let text = String::from_utf8_lossy(line);
        serde_json::from_str(&text).ok()
    })
}

/// Read the head (and, for a large file, the tail) of a JSONL transcript
/// without ever reading the whole thing.
///
/// One file handle regardless of size. Files inside the head budget are read
/// once and serve as their own tail.
fn read_bounded_jsonl(scan: &ScanEnv<'_>, path: &Path) -> Result<BoundedJsonl> {
    let mut file = fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
    scan.note_open();
    let len = file.metadata()?.len();
    if len <= HEAD_SCAN_MAX_BYTES {
        let mut buffer = Vec::with_capacity(len as usize);
        file.read_to_end(&mut buffer)?;
        scan.note_bytes(buffer.len() as u64);
        keep_complete_lines(&mut buffer);
        return Ok(BoundedJsonl {
            head: buffer,
            tail: Vec::new(),
        });
    }
    let mut head = vec![0u8; HEAD_SCAN_MAX_BYTES as usize];
    let mut filled = 0usize;
    while filled < head.len() {
        let read = file.read(&mut head[filled..])?;
        if read == 0 {
            break;
        }
        filled += read;
    }
    head.truncate(filled);
    scan.note_bytes(filled as u64);
    keep_complete_lines(&mut head);

    let tail_start = len.saturating_sub(TAIL_SCAN_MAX_BYTES);
    file.seek(SeekFrom::Start(tail_start))?;
    let mut tail = Vec::with_capacity(TAIL_SCAN_MAX_BYTES as usize);
    file.take(TAIL_SCAN_MAX_BYTES).read_to_end(&mut tail)?;
    scan.note_bytes(tail.len() as u64);
    // The seek landed mid-record; everything before the first newline is the
    // torn remainder of a record the head may or may not hold.
    if let Some(first_newline) = tail.iter().position(|&byte| byte == b'\n') {
        tail.drain(..=first_newline);
    } else {
        tail.clear();
    }
    keep_complete_lines(&mut tail);
    Ok(BoundedJsonl { head, tail })
}

/// Build the same bounded head/tail view from bytes already captured through
/// one open file handle. Hook ingestion uses this so identity validation,
/// shallow cataloging, and full ingestion all describe one immutable read.
fn bounded_jsonl_from_bytes(bytes: &[u8]) -> BoundedJsonl {
    if bytes.len() as u64 <= HEAD_SCAN_MAX_BYTES {
        let mut head = bytes.to_vec();
        keep_snapshot_records(&mut head);
        return BoundedJsonl {
            head,
            tail: Vec::new(),
        };
    }

    let mut head = bytes[..HEAD_SCAN_MAX_BYTES as usize].to_vec();
    keep_complete_lines(&mut head);
    let tail_start = bytes.len().saturating_sub(TAIL_SCAN_MAX_BYTES as usize);
    let mut tail = bytes[tail_start..].to_vec();
    if let Some(first_newline) = tail.iter().position(|&byte| byte == b'\n') {
        tail.drain(..=first_newline);
    } else {
        tail.clear();
    }
    keep_snapshot_records(&mut tail);
    BoundedJsonl { head, tail }
}

fn claude_session_id_from_bounded(bounded: &BoundedJsonl) -> Result<Option<String>> {
    let session_ids = bounded
        .head_records()
        .chain(bounded.tail_records_rev())
        .filter_map(parse_record)
        .filter_map(|value| {
            value
                .get("sessionId")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
                .map(str::to_string)
        })
        .collect::<BTreeSet<_>>();
    anyhow::ensure!(
        session_ids.len() <= 1,
        "conflicting sessionId values in Claude transcript"
    );
    Ok(session_ids.into_iter().next())
}

pub(crate) fn excerpt(text: &str) -> String {
    text.trim().chars().take(EXCERPT_MAX_CHARS).collect()
}

/// A JSON array column value, or `None` when there is nothing observed to
/// store. An empty list is never written as `[]` — absent stays absent.
fn json_array_or_none(values: &[String]) -> Option<String> {
    (!values.is_empty()).then(|| serde_json::to_string(values).unwrap_or_else(|_| "[]".into()))
}

fn push_unique(models: &mut Vec<String>, value: Option<&str>) {
    if let Some(value) = value.map(str::trim).filter(|s| !s.is_empty()) {
        if !models.iter().any(|existing| existing == value) {
            models.push(value.to_string());
        }
    }
}

fn text_of(content: Option<&Value>) -> Option<String> {
    let content = content?;
    if let Some(text) = content.as_str() {
        return Some(text.to_string());
    }
    let items = content.as_array()?;
    let parts: Vec<&str> = items
        .iter()
        .filter(|item| item.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|item| item.get("text").and_then(Value::as_str))
        .collect();
    (!parts.is_empty()).then(|| parts.join("\n"))
}

// ---------------------------------------------------------------------------
// claude
// ---------------------------------------------------------------------------

struct ClaudeProvider;

/// The first substantive human turn's excerpt, or `None` for a record that
/// is not one.
///
/// Sidechain rows are the parent agent's own prompts to a subagent, and the
/// harness's control rows -- slash-command wrappers, task notifications,
/// hook output, meta bookkeeping -- are not human turns. The control decision
/// is `ingest::control`'s, the same one the record walk stamps on
/// `session_events.control_kind` and keeps out of `history`, so the catalog's
/// `first_prompt` and the ledger's prompts cannot disagree about a record.
/// `<system-reminder>` blocks are removed from the excerpt for the same
/// reason: the record walk stores them as rows of their own, not as prompt
/// text. The full-transcript metadata fold (`ClaudeMetaFold`) asks this same
/// question of every record, and its answer is the one the catalog keeps.
pub(crate) fn claude_substantive_prompt(value: &Value) -> Option<String> {
    let role = value
        .pointer("/message/role")
        .and_then(Value::as_str)
        .or_else(|| value.get("type").and_then(Value::as_str));
    if role != Some("user") {
        return None;
    }
    if value.get("isSidechain").and_then(Value::as_bool) == Some(true) {
        return None;
    }
    let object = value.as_object()?;
    let text = text_of(value.pointer("/message/content"))?;
    let split = crate::ingest::control::split_system_reminders(text.trim());
    if split.prompt.is_empty()
        || crate::ingest::control::claude_record_control_kind(object, &split.prompt).is_some()
    {
        return None;
    }
    Some(excerpt(&split.prompt))
}

fn claude_timestamp(value: &Value) -> Option<i64> {
    value.get("timestamp").and_then(|v| {
        v.as_str()
            .and_then(crate::parse_iso_ms)
            .or_else(|| v.as_i64())
    })
}

impl ShallowSessionProvider for ClaudeProvider {
    #[cfg(feature = "unstable-internal")]
    fn acquire(
        &self,
        _home: &Path,
        _observation: &crate::observations::SessionObservation,
    ) -> Result<crate::sources::AcquiredEvidence> {
        Ok(crate::sources::AcquiredEvidence::LocalFiles)
    }
    fn source(&self) -> &'static str {
        "claude"
    }
    /// The transcript parser writes prompts, events, tool calls, file edits and
    /// subagent relationships: every kind a full session is made of.
    fn evidence_kinds(&self) -> &'static [EvidenceKind] {
        FULL_SESSION_KINDS
    }

    fn watch_roots(&self, roots: &ProviderRoots<'_>) -> Vec<WatchRoot> {
        vec![WatchRoot::tree(roots.claude.join("projects"))]
    }

    /// Claude's enumeration collects `*.jsonl`, but a subagent transcript's
    /// `agent-<id>.meta.json` sidecar is evidence too — `source_snapshot`
    /// stamps it, so a sidecar arriving or changing on its own re-hydrates the
    /// session. Left out of the fold, a tick whose only change was a sidecar
    /// would sit behind an unchanged fingerprint and never run.
    fn fingerprint_inputs(&self, env: &DiscoveryEnv<'_>) -> Result<Vec<Candidate>> {
        let mut inputs = self.enumerate(env, None)?;
        inputs.extend(file_candidates(
            "claude",
            crate::collect_matching_files(&env.claude_config_dir.join("projects"), "", "json")?,
            crate::file_stamp_and_modified,
        )?);
        Ok(inputs)
    }

    fn enumerate(
        &self,
        env: &DiscoveryEnv<'_>,
        _requested_limit: Option<usize>,
    ) -> Result<Vec<Candidate>> {
        file_candidates(
            "claude",
            crate::collect_matching_files(&env.claude_config_dir.join("projects"), "", "jsonl")?,
            crate::file_stamp_and_modified,
        )
    }

    fn read_shallow(
        &self,
        scan: &ScanEnv<'_>,
        _catalog: Option<&Connection>,
        candidate: &Candidate,
    ) -> Result<Option<ShallowSession>> {
        let path = PathBuf::from(&candidate.locator);
        let bounded = read_bounded_jsonl(scan, &path)?;
        read_claude_shallow(candidate, &path, &bounded)
    }
}

pub(crate) fn claude_shallow_session_from_bytes(
    candidate: &Candidate,
    bytes: &[u8],
) -> Result<Option<ShallowSession>> {
    let path = PathBuf::from(&candidate.locator);
    read_claude_shallow(candidate, &path, &bounded_jsonl_from_bytes(bytes))
}

fn read_claude_shallow(
    candidate: &Candidate,
    path: &Path,
    bounded: &BoundedJsonl,
) -> Result<Option<ShallowSession>> {
        let mut session = ShallowSession {
            source: "claude".into(),
            raw_path: Some(candidate.locator.clone()),
            ..Default::default()
        };
        let mut models = Vec::new();
        let session_id = claude_session_id_from_bounded(bounded)?;
        // A subagent sidecar transcript is its own file whose records carry the
        // *parent's* sessionId (see `ingest_claude_transcript`). Enumerating it
        // as a session would emit the parent twice per run and let the two
        // files fight over one row's raw_path/source_stamp, so the stamp never
        // matched again and one of them was re-read forever.
        let mut primary_record_seen = false;
        let mut sidechain_records = 0usize;
        let mut identified_records = 0usize;
        let mut parsed_records = 0usize;
        let mut head_records_seen = 0usize;
        for line in bounded.head_records() {
            head_records_seen += 1;
            let Some(value) = parse_record(line) else {
                continue;
            };
            parsed_records += 1;
            if value
                .get("sessionId")
                .and_then(Value::as_str)
                .is_some_and(|id| !id.is_empty())
            {
                identified_records += 1;
                if value.get("isSidechain").and_then(Value::as_bool) == Some(true) {
                    sidechain_records += 1;
                } else {
                    primary_record_seen = true;
                }
            }
            if session.cwd.is_none() {
                session.cwd = value.get("cwd").and_then(Value::as_str).map(str::to_string);
            }
            if let Some(branch) = value.get("gitBranch").and_then(Value::as_str) {
                session.git_branch = Some(branch.to_string());
            }
            if let Some(version) = value.get("version").and_then(Value::as_str) {
                session.agent_version = Some(version.to_string());
            }
            push_unique(
                &mut models,
                value.pointer("/message/model").and_then(Value::as_str),
            );
            if let Some(ts) = claude_timestamp(&value) {
                session.first_activity_ms.get_or_insert(ts);
                session.last_activity_ms = Some(ts);
            }
            if session.first_prompt.is_none() {
                session.first_prompt = claude_substantive_prompt(&value);
            }
            // Every observed field is settled, a model has been seen, and a
            // primary record proves this is not a sidecar: nothing further in
            // the head can change the row (additional models stay
            // best-effort), so stop paying to parse it.
            if primary_record_seen
                && !models.is_empty()
                && session.cwd.is_some()
                && session.git_branch.is_some()
                && session.agent_version.is_some()
                && session.first_activity_ms.is_some()
                && session.first_prompt.is_some()
            {
                break;
            }
        }
        let mut need_last_activity = true;
        let mut need_branch = true;
        for line in bounded.tail_records_rev() {
            if !need_last_activity && !need_branch {
                break;
            }
            let Some(value) = parse_record(line) else {
                continue;
            };
            if need_last_activity {
                if let Some(ts) = claude_timestamp(&value) {
                    session.last_activity_ms = Some(ts);
                    need_last_activity = false;
                }
            }
            if need_branch {
                if let Some(branch) = value.get("gitBranch").and_then(Value::as_str) {
                    session.git_branch = Some(branch.to_string());
                    need_branch = false;
                }
            }
        }
        // Every identified record in the head belongs to a sidechain: this is a
        // sidecar for a session whose own transcript is enumerated separately.
        // A session's primary transcript always opens with non-sidechain turns,
        // because a subagent can only be spawned by one.
        if identified_records > 0 && sidechain_records == identified_records {
            return Ok(None);
        }
        // A file with complete records that parse as nothing is corrupt, not a
        // session. Publishing it under its file stem would put a fabricated
        // row in the catalog and hide the corruption; a diagnostic names it.
        // A file with no complete records at all is merely empty (a session
        // that has just started) and is simply not a session yet.
        if parsed_records == 0 {
            anyhow::ensure!(
                head_records_seen == 0,
                "no parseable JSON records in the first {head_records_seen} record(s)"
            );
            return Ok(None);
        }
        let Some(session_id) = session_id.or_else(|| {
            path.file_stem()
                .and_then(|s| s.to_str())
                .map(str::to_string)
                .filter(|s| !s.is_empty())
        }) else {
            return Ok(None);
        };
        session.session_id = session_id;
        session.models = models;
        Ok(Some(session))
}

// ---------------------------------------------------------------------------
// codex
// ---------------------------------------------------------------------------

struct CodexProvider;

impl ShallowSessionProvider for CodexProvider {
    #[cfg(feature = "unstable-internal")]
    fn acquire(
        &self,
        _home: &Path,
        _observation: &crate::observations::SessionObservation,
    ) -> Result<crate::sources::AcquiredEvidence> {
        Ok(crate::sources::AcquiredEvidence::LocalFiles)
    }
    fn source(&self) -> &'static str {
        "codex"
    }
    /// The rollout parser writes prompts, events, tool calls, file edits and
    /// child-thread relationships: every kind a full session is made of.
    fn evidence_kinds(&self) -> &'static [EvidenceKind] {
        FULL_SESSION_KINDS
    }

    fn watch_roots(&self, roots: &ProviderRoots<'_>) -> Vec<WatchRoot> {
        vec![
            WatchRoot::tree(roots.codex.join("sessions")),
            WatchRoot::tree(roots.codex.join("archived_sessions")),
        ]
    }

    fn enumerate(
        &self,
        env: &DiscoveryEnv<'_>,
        _requested_limit: Option<usize>,
    ) -> Result<Vec<Candidate>> {
        let mut files = Vec::new();
        for root in [
            env.codex_home.join("sessions"),
            env.codex_home.join("archived_sessions"),
        ] {
            files.extend(crate::collect_matching_files(&root, "rollout-", "jsonl")?);
        }
        file_candidates("codex", files, crate::file_stamp_and_modified)
    }

    fn read_shallow(
        &self,
        scan: &ScanEnv<'_>,
        _catalog: Option<&Connection>,
        candidate: &Candidate,
    ) -> Result<Option<ShallowSession>> {
        let path = PathBuf::from(&candidate.locator);
        let bounded = read_bounded_jsonl(scan, &path)?;
        let Some(meta) = bounded.head_records().next().and_then(|line| {
            parse_record(line)
                .filter(|v| v.get("type").and_then(Value::as_str) == Some("session_meta"))
        }) else {
            return Ok(None);
        };
        let payload = meta.get("payload");
        let Some(session_id) = payload
            .and_then(|p| p.get("id"))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        else {
            return Ok(None);
        };
        // Linked subagent threads are real rollouts but not root sessions;
        // standalone guardian rollouts may carry `source.subagent` without a
        // parent and are cataloged under their own payload.id.
        let is_subagent = crate::codex_is_subagent(payload, session_id);
        if is_subagent {
            return Ok(None);
        }
        let git = payload.and_then(|p| p.get("git"));
        let string_field = |owner: Option<&Value>, key: &str| {
            owner
                .and_then(|o| o.get(key))
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        };
        let mut models = Vec::new();
        push_unique(
            &mut models,
            payload.and_then(|p| p.get("model")).and_then(Value::as_str),
        );
        let mut first_prompt = None;
        let mut first_activity_ms = claude_timestamp(&meta);
        let mut last_activity_ms = first_activity_ms;
        for line in bounded.head_records() {
            let Some(value) = parse_record(line) else {
                continue;
            };
            if let Some(ts) = claude_timestamp(&value) {
                first_activity_ms.get_or_insert(ts);
                last_activity_ms = Some(ts);
            }
            if value.get("type").and_then(Value::as_str) == Some("turn_context") {
                push_unique(
                    &mut models,
                    value.pointer("/payload/model").and_then(Value::as_str),
                );
            }
            if first_prompt.is_none() {
                first_prompt = codex_substantive_prompt(&value);
            }
            // The first prompt and first timestamp are settled; the tail owns
            // the last timestamp and models stay best-effort, so nothing
            // further in the head can change the row.
            if first_prompt.is_some() && first_activity_ms.is_some() {
                break;
            }
        }
        for line in bounded.tail_records_rev() {
            let Some(value) = parse_record(line) else {
                continue;
            };
            if let Some(ts) = claude_timestamp(&value) {
                last_activity_ms = Some(ts);
                break;
            }
        }
        let mtime = crate::file_modified_ms(&path);
        Ok(Some(ShallowSession {
            source: "codex".into(),
            session_id: session_id.to_string(),
            cwd: string_field(payload, "cwd"),
            git_branch: string_field(git, "branch"),
            first_activity_ms,
            last_activity_ms: last_activity_ms.or(mtime),
            first_prompt,
            models,
            originator: string_field(payload, "originator"),
            agent_version: string_field(payload, "cli_version"),
            repo_url: string_field(git, "repository_url")
                .or_else(|| string_field(git, "remote_url")),
            initial_commit: string_field(git, "commit_hash"),
            workspace_roots: string_list(payload.and_then(|p| p.get("workspace_roots"))),
            raw_path: Some(candidate.locator.clone()),
            ..Default::default()
        }))
    }
}

fn codex_substantive_prompt(value: &Value) -> Option<String> {
    crate::codex::human_message(value).map(|message| excerpt(&message.text))
}

fn string_list(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// cursor
// ---------------------------------------------------------------------------

struct CursorProvider;

impl ShallowSessionProvider for CursorProvider {
    #[cfg(feature = "unstable-internal")]
    fn acquire(
        &self,
        _home: &Path,
        _observation: &crate::observations::SessionObservation,
    ) -> Result<crate::sources::AcquiredEvidence> {
        Ok(crate::sources::AcquiredEvidence::LocalFiles)
    }
    fn source(&self) -> &'static str {
        "cursor"
    }
    /// The transcript parser writes prompts, events, tool calls and file edits.
    /// Delegation is deliberately absent: a Cursor `Task` block names no child
    /// transcript, so no relationship row is ever written.
    fn evidence_kinds(&self) -> &'static [EvidenceKind] {
        &[
            EvidenceKind::History,
            EvidenceKind::SessionEvent,
            EvidenceKind::ToolCall,
            EvidenceKind::FileEdit,
        ]
    }

    fn watch_roots(&self, roots: &ProviderRoots<'_>) -> Vec<WatchRoot> {
        vec![WatchRoot::tree(roots.home.join(".cursor/projects"))]
    }

    fn enumerate(
        &self,
        env: &DiscoveryEnv<'_>,
        _requested_limit: Option<usize>,
    ) -> Result<Vec<Candidate>> {
        let root = env.home.join(".cursor/projects");
        let mut out = Vec::new();
        for project_dir in crate::sorted_dirs(&root)? {
            let transcripts = project_dir.join("agent-transcripts");
            if !transcripts.is_dir() {
                continue;
            }
            for session_dir in crate::sorted_dirs(&transcripts)? {
                let Some(session_id) = session_dir.file_name().and_then(|s| s.to_str()) else {
                    continue;
                };
                let jsonl = session_dir.join(format!("{session_id}.jsonl"));
                let Ok((stamp, recency_hint_ms)) = crate::file_stamp_and_modified(&jsonl) else {
                    continue;
                };
                out.push(Candidate {
                    source: "cursor",
                    locator: jsonl.to_string_lossy().into_owned(),
                    session_id: Some(session_id.to_string()),
                    recency_hint_ms,
                    stamp,
                });
            }
        }
        Ok(out)
    }

    fn read_shallow(
        &self,
        scan: &ScanEnv<'_>,
        _catalog: Option<&Connection>,
        candidate: &Candidate,
    ) -> Result<Option<ShallowSession>> {
        let path = PathBuf::from(&candidate.locator);
        let Some(session_id) = candidate.session_id.clone() else {
            return Ok(None);
        };
        let bounded = read_bounded_jsonl(scan, &path)?;
        let first_prompt = bounded
            .head_records()
            .filter_map(|line| {
                let line = String::from_utf8_lossy(line);
                crate::parse_cursor_text(&line).ok().flatten()
            })
            .map(|prompt| excerpt(&prompt))
            .find(|prompt| !prompt.is_empty());
        // Cursor writes no timestamp field and no model on its records. The
        // only time signal in the file is the localized `<timestamp>` tag its
        // client injects into a human turn, so the head read looks for that
        // and reports nothing when the build did not write one. `models` is
        // read from `message.model` for the builds that write it; an empty
        // list means "not seen", never "no model".
        let mut head_times = bounded.head_records().filter_map(cursor_record_time);
        let first_activity_ms = head_times.next();
        // For a transcript past the head budget the tail is a separate region,
        // so a turn time that only appears in the head is unreachable from the
        // tail scan. A long run of assistant and tool records after one dated
        // human turn is the ordinary shape of that: the tail finds no tag,
        // because only a human turn carries one, and falling straight to the
        // mtime reported a session as having last spoken "now". Full ingestion
        // disagrees — those records inherit the open turn's time — and because
        // the discovery upsert merges `last_activity_ms` with `MAX`, the mtime
        // would also re-expand a window a rebuild had just retracted. The last
        // time the head could read is the best recorded evidence there is; the
        // mtime stays for a transcript with no readable turn time at all.
        let last_head_ms = head_times.last().or(first_activity_ms);
        let last_activity_ms = bounded
            .tail_records_rev()
            .find_map(cursor_record_time)
            .or(last_head_ms)
            .or_else(|| crate::file_modified_ms(&path));
        let mut models = Vec::new();
        for model in bounded.head_records().filter_map(cursor_record_model) {
            if !models.contains(&model) {
                models.push(model);
            }
        }
        let last_assistant_text = bounded
            .tail_records_rev()
            .find_map(cursor_assistant_text)
            .map(|text| excerpt(&text));
        let cwd = path
            .parent()
            .and_then(Path::parent)
            .and_then(Path::parent)
            .and_then(|p| p.file_name())
            .and_then(|s| s.to_str())
            .map(crate::decode_cursor_project);
        Ok(Some(ShallowSession {
            source: "cursor".into(),
            session_id,
            cwd,
            first_activity_ms,
            last_activity_ms,
            first_prompt,
            last_assistant_text,
            models,
            raw_path: Some(candidate.locator.clone()),
            ..Default::default()
        }))
    }
}

/// Epoch milliseconds for one Cursor record, from the injected `<timestamp>`
/// tag or from a record `timestamp` field if a build writes one.
fn cursor_record_time(line: &[u8]) -> Option<i64> {
    let value = parse_record(line)?;
    let obj = value.as_object()?;
    if let Some(ts) = obj.get("timestamp").and_then(|v| {
        v.as_str()
            .and_then(crate::parse_iso_ms)
            .or_else(|| v.as_i64())
    }) {
        return Some(ts);
    }
    crate::ingest::cursor::injected_turn_time(
        crate::ingest::cursor::record_role(obj),
        &crate::ingest::cursor::record_blocks(obj),
    )
}

/// `message.model` for the Cursor builds that record one.
fn cursor_record_model(line: &[u8]) -> Option<String> {
    let value = parse_record(line)?;
    let model = value.get("message")?.get("model")?.as_str()?.trim();
    (!model.is_empty()).then(|| model.to_string())
}

/// The assistant prose in one Cursor record, ignoring tool and marker blocks.
fn cursor_assistant_text(line: &[u8]) -> Option<String> {
    let value = parse_record(line)?;
    let obj = value.as_object()?;
    if crate::ingest::cursor::record_role(obj) != Some("assistant") {
        return None;
    }
    // One assistant record can hold several text blocks — prose, a tool call,
    // then more prose. Full ingestion walks them in order and keeps the last
    // non-empty one, so the summary is the reply's closing line. Taking the
    // first here instead would make the catalog advertise the opening line and
    // hydration silently rewrite it.
    crate::ingest::cursor::record_blocks(obj)
        .iter()
        .filter(|block| block.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|block| block.get("text").and_then(Value::as_str))
        .map(str::trim)
        .rfind(|text| !text.is_empty())
        .map(str::to_string)
}

// ---------------------------------------------------------------------------
// grok
// ---------------------------------------------------------------------------

struct GrokProvider;

impl ShallowSessionProvider for GrokProvider {
    #[cfg(feature = "unstable-internal")]
    fn acquire(
        &self,
        _home: &Path,
        _observation: &crate::observations::SessionObservation,
    ) -> Result<crate::sources::AcquiredEvidence> {
        Ok(crate::sources::AcquiredEvidence::LocalFiles)
    }
    fn source(&self) -> &'static str {
        "grok"
    }
    /// The directory parser writes prompts, events, tool calls, file edits and
    /// subagent relationships: every kind a full session is made of. Grok still
    /// records no per-turn billing tokens; that absence is a diagnostic, not a
    /// missing evidence kind, so capability follows this table.
    fn evidence_kinds(&self) -> &'static [EvidenceKind] {
        FULL_SESSION_KINDS
    }

    fn watch_roots(&self, roots: &ProviderRoots<'_>) -> Vec<WatchRoot> {
        vec![WatchRoot::tree(roots.grok.join("sessions"))]
    }

    fn enumerate(
        &self,
        env: &DiscoveryEnv<'_>,
        _requested_limit: Option<usize>,
    ) -> Result<Vec<Candidate>> {
        file_candidates(
            "grok",
            crate::collect_matching_files(
                &env.grok_home.join("sessions"),
                "chat_history",
                "jsonl",
            )?,
            crate::grok_session_stamp_and_modified,
        )
    }

    fn read_shallow(
        &self,
        scan: &ScanEnv<'_>,
        _catalog: Option<&Connection>,
        candidate: &Candidate,
    ) -> Result<Option<ShallowSession>> {
        let chat = PathBuf::from(&candidate.locator);
        let summary_path = chat.with_file_name("summary.json");
        // Absent vs unreadable vs present: `is_file()` collapses the first two
        // into "no summary", which would name the session from its folder and
        // strand identity the way ingest already refuses to. `read_grok_summary`
        // fails an unreadable or malformed file and only falls back when the
        // sidecar is genuinely not there.
        let summary = match crate::read_grok_summary(&summary_path)? {
            Some(value) => {
                scan.note_open();
                if let Ok(metadata) = fs::metadata(&summary_path) {
                    scan.note_bytes(metadata.len());
                }
                Some(value)
            }
            None => None,
        };
        let fallback_session = chat
            .parent()
            .and_then(|p| p.file_name())
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_string();
        let session_id = summary
            .as_ref()
            .and_then(|s| s.pointer("/info/id"))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or(fallback_session);
        if session_id.is_empty() {
            return Ok(None);
        }
        let cwd = summary
            .as_ref()
            .and_then(|s| s.pointer("/info/cwd").or_else(|| s.get("git_root_dir")))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .or_else(|| crate::grok_project_from_path(&chat));
        let git_branch = summary
            .as_ref()
            .and_then(|s| s.get("head_branch"))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        // `updates.jsonl` is where Grok records real per-event times;
        // `summary.json` records only when the session was opened and last
        // touched, and a session restored from a checkpoint carries a
        // `created_at` older than anything it did. Read the stream's first and
        // last record from the same bounded head/tail scan every other adapter
        // uses, and fall back to the summary when there is no stream.
        let (stream_first, stream_last) =
            grok_update_bounds(scan, &chat.with_file_name("updates.jsonl"))?;
        let first_activity_ms = stream_first.or_else(|| {
            summary
                .as_ref()
                .and_then(|s| s.get("created_at"))
                .and_then(Value::as_str)
                .and_then(crate::parse_iso_ms)
        });
        let last_activity_ms = stream_last
            .or_else(|| {
                summary
                    .as_ref()
                    .and_then(|s| s.get("updated_at"))
                    .and_then(Value::as_str)
                    .and_then(crate::parse_iso_ms)
            })
            .or_else(|| crate::file_modified_ms(&chat));
        let mut models = Vec::new();
        push_unique(
            &mut models,
            summary
                .as_ref()
                .and_then(|s| s.pointer("/info/model").or_else(|| s.get("model")))
                .and_then(Value::as_str),
        );
        let mut first_prompt = None;
        if chat.is_file() {
            let bounded = read_bounded_jsonl(scan, &chat)?;
            for line in bounded.head_records() {
                let Some(value) = parse_record(line) else {
                    continue;
                };
                if let Some(text) = crate::grok_chat_text(&value, "user") {
                    first_prompt = Some(excerpt(&text));
                    break;
                }
            }
        }
        Ok(Some(ShallowSession {
            source: "grok".into(),
            session_id,
            cwd,
            git_branch,
            first_activity_ms,
            last_activity_ms,
            first_prompt,
            models,
            raw_path: Some(candidate.locator.clone()),
            ..Default::default()
        }))
    }
}

/// The first and last times `updates.jsonl` recorded.
///
/// Both are `None` when there is no readable stream or when its bounding
/// records carried no time — Grok's own absence, reported as such rather than
/// replaced with a file mtime here.
fn grok_update_bounds(
    scan: &ScanEnv<'_>,
    updates: &Path,
) -> Result<(Option<i64>, Option<i64>)> {
    match fs::metadata(updates) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok((None, None)),
        Err(error) => {
            return Err(error).with_context(|| format!("stat {}", updates.display()))
        }
        Ok(metadata) if !metadata.is_file() => return Ok((None, None)),
        Ok(_) => {}
    }
    let bounded = read_bounded_jsonl(scan, updates)?;
    let first = bounded
        .head_records()
        .filter_map(parse_record)
        .find_map(|record| crate::ingest::grok::line_timestamp_ms(&record));
    let last = bounded
        .tail_records_rev()
        .filter_map(parse_record)
        .find_map(|record| crate::ingest::grok::line_timestamp_ms(&record));
    Ok((first, last))
}

// ---------------------------------------------------------------------------
// opencode
// ---------------------------------------------------------------------------

/// Shallow adapter over the live OpenCode SQLite store.
///
/// One read-only connection and one deferred transaction live for the whole
/// discovery run. The first schema read establishes a coherent SQLite
/// snapshot; candidate enumeration and every selected-session query therefore
/// see the same committed state even while OpenCode appends in WAL mode.
///
/// No provider DDL is ever issued. Before querying `message` or `part`, the
/// adapter verifies that an existing provider index can seek by session (or by
/// message after a session seek). Metadata remains available on older schemas,
/// but prompt/model extraction is omitted when it would require a table scan.
#[derive(Default)]
struct OpencodeProvider {
    pass: Mutex<()>,
    live: Mutex<Option<OpencodeReadSnapshot>>,
}

#[derive(Clone)]
struct OpencodeSessionSeed {
    directory: Option<String>,
    created: Option<i64>,
    updated: Option<i64>,
}

/// One run's coherent live snapshot and the schema/index facts that are
/// invariant across its selected candidates.
struct OpencodeReadSnapshot {
    conn: Connection,
    store_identity: String,
    session_columns: BTreeSet<String>,
    message_columns: BTreeSet<String>,
    part_columns: BTreeSet<String>,
    message_by_session: bool,
    part_by_session: bool,
    part_by_message: bool,
    sessions: BTreeMap<String, OpencodeSessionSeed>,
}

impl OpencodeProvider {
    /// Open the provider once, read-only, and start the transaction that pins
    /// the run's SQLite snapshot. `None` means this host is not on the SQLite
    /// layout — either it has the legacy JSON tree, or it has no OpenCode
    /// store at all.
    fn snapshot(&self, scan: &ScanEnv<'_>) -> Result<MutexGuard<'_, Option<OpencodeReadSnapshot>>> {
        let mut guard = self.live.lock().expect("opencode live snapshot lock");
        if guard.is_none() && matches!(scan.opencode_layout(), Some(OpencodeLayout::Sqlite(_))) {
            *guard = Some(open_opencode_snapshot(scan)?);
        }
        Ok(guard)
    }
}

impl ScanEnv<'_> {
    /// Which OpenCode layout this host actually has. `opencode.db` wins when
    /// both are present: newer releases write SQLite and leave the old tree
    /// behind, so preferring the tree would serve stale history.
    pub(crate) fn opencode_layout(&self) -> Option<OpencodeLayout> {
        OpencodeLayout::detect(self.opencode_db, self.opencode_storage_dir)
    }
}

/// Open a connection whose filesystem generation is known to match the path
/// we inspected. The before/after check closes the replacement race between
/// SQLite opening the file and RelayHistory computing the source stamp.
fn open_opencode_snapshot(scan: &ScanEnv<'_>) -> Result<OpencodeReadSnapshot> {
    for _ in 0..3 {
        let generation_before = opencode_store_generation(scan.opencode_db)?;
        let conn = open_db_readonly(scan.opencode_db)?;
        scan.note_open();
        conn.execute_batch("PRAGMA query_only = ON; BEGIN DEFERRED")?;
        let session_columns = table_columns(&conn, "session")?;
        let message_columns = table_columns(&conn, "message")?;
        let part_columns = table_columns(&conn, "part")?;
        let message_by_session = has_leading_index(&conn, "message", "session_id")?;
        let part_by_session = has_leading_index(&conn, "part", "session_id")?;
        let part_by_message = has_leading_index(&conn, "part", "message_id")?;
        let schema_version: i64 = conn.query_row("PRAGMA schema_version", [], |row| row.get(0))?;
        let generation_after = opencode_store_generation(scan.opencode_db)?;
        if generation_before != generation_after {
            continue;
        }
        return Ok(OpencodeReadSnapshot {
            conn,
            store_identity: format!("{generation_before}:{schema_version}"),
            session_columns,
            message_columns,
            part_columns,
            message_by_session,
            part_by_session,
            part_by_message,
            sessions: BTreeMap::new(),
        });
    }
    anyhow::bail!(
        "OpenCode database {} was repeatedly replaced while discovery opened it",
        scan.opencode_db.display()
    )
}

/// Whether `table` already has an index whose leading column is `column`.
/// SQLite's primary-key autoindexes are included by the pragma.
fn has_leading_index(conn: &Connection, table: &str, column: &str) -> Result<bool> {
    Ok(conn
        .prepare(&format!(
            "SELECT 1 FROM pragma_index_list('{table}') indexes \
         JOIN pragma_index_info(indexes.name) columns \
         WHERE columns.seqno = 0 AND columns.name = ? LIMIT 1"
        ))?
        .query_row([column], |_| Ok(()))
        .optional()?
        .is_some())
}

/// Whether an existing provider index has exactly the ordered prefix needed
/// to apply a deterministic SQL LIMIT without sorting an unbounded tie group.
fn has_ordered_index_prefix(
    conn: &Connection,
    table: &str,
    prefix: &[(&str, bool)],
) -> Result<bool> {
    let mut indexes = conn.prepare(&format!("SELECT name FROM pragma_index_list('{table}')"))?;
    let names = indexes
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for name in names {
        let mut columns = conn.prepare(
            "SELECT name, desc FROM pragma_index_xinfo(?) \
             WHERE key = 1 ORDER BY seqno",
        )?;
        let ordered = columns
            .query_map([name], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if ordered.len() >= prefix.len()
            && ordered
                .iter()
                .zip(prefix)
                .all(|((actual_name, actual_desc), (name, desc))| {
                    actual_name == name && actual_desc == desc
                })
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn table_columns(conn: &Connection, table: &str) -> Result<BTreeSet<String>> {
    Ok(conn
        .prepare(&format!("SELECT name FROM pragma_table_info('{table}')"))?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<BTreeSet<String>>>()?)
}

impl ShallowSessionProvider for OpencodeProvider {
    fn begin_discovery_pass(&self) -> Result<Option<Box<dyn DiscoveryPassGuard + '_>>> {
        let pass = self.pass.lock().expect("opencode discovery pass lock");
        // A SourceRegistry retains this adapter across calls. Drop the last
        // call's SQLite transaction before detecting this call's layout, while
        // the pass lock prevents a concurrent call from replacing the state
        // between enumeration and reads.
        *self.live.lock().expect("opencode live snapshot lock") = None;
        Ok(Some(Box::new(pass)))
    }

    #[cfg(feature = "unstable-internal")]
    fn acquire(
        &self,
        _home: &Path,
        _observation: &crate::observations::SessionObservation,
    ) -> Result<crate::sources::AcquiredEvidence> {
        Ok(crate::sources::AcquiredEvidence::LocalFiles)
    }
    fn source(&self) -> &'static str {
        "opencode"
    }
    /// Both supported OpenCode layouts produce prompts, events, tool calls,
    /// file edits and child-session relationships.
    fn evidence_kinds(&self) -> &'static [EvidenceKind] {
        FULL_SESSION_KINDS
    }

    fn watch_roots(&self, roots: &ProviderRoots<'_>) -> Vec<WatchRoot> {
        // The database file is rewritten in place and SQLite's -wal and -shm
        // siblings move with it, so the directory is what actually sees every
        // write. Its own entries are enough — opencode keeps unrelated state
        // in subdirectories, and waking on those would cost a fingerprint walk
        // each time.
        roots
            .opencode_db
            .parent()
            .map(|dir| vec![WatchRoot::directory(dir)])
            .unwrap_or_default()
    }

    /// Opencode's enumeration is a SQL query against the provider database, so
    /// it costs an open plus a scan — far more than the watch fast path should
    /// pay per tick. The database file's own size and mtime (plus its
    /// write-ahead log, where a commit lands first) move whenever a session or
    /// message does, and cost three stats.
    fn fingerprint_inputs(&self, env: &DiscoveryEnv<'_>) -> Result<Vec<Candidate>> {
        let db = &env.opencode_db;
        let mut out = Vec::new();
        for suffix in ["", "-wal", "-shm"] {
            let mut path = db.clone().into_os_string();
            path.push(suffix);
            let path = PathBuf::from(path);
            let Ok((stamp, recency_hint_ms)) = crate::file_stamp_and_modified(&path) else {
                continue;
            };
            out.push(Candidate {
                source: "opencode",
                locator: path.to_string_lossy().into_owned(),
                session_id: None,
                recency_hint_ms,
                stamp,
            });
        }
        Ok(out)
    }

    fn enumerate(
        &self,
        env: &DiscoveryEnv<'_>,
        requested_limit: Option<usize>,
    ) -> Result<Vec<Candidate>> {
        let scan = env.scan();
        if let Some(OpencodeLayout::JsonTree(root)) = scan.opencode_layout() {
            return enumerate_opencode_json_tree(&scan, &root);
        }
        let mut guard = self.snapshot(&scan)?;
        let Some(snapshot) = guard.as_mut() else {
            return Ok(Vec::new());
        };
        let columns = &snapshot.session_columns;
        if !columns.contains("id") {
            return Ok(Vec::new());
        }
        let updated = if columns.contains("time_updated") {
            "time_updated"
        } else {
            "NULL"
        };
        let created = if columns.contains("time_created") {
            "time_created"
        } else {
            "NULL"
        };
        let directory = if columns.contains("directory") {
            "directory"
        } else {
            "NULL"
        };
        // Current OpenCode releases index the session primary key but do not
        // all provide the compound ordering needed to apply both update
        // recency and the global id tie-break before LIMIT. Otherwise the
        // provider's descending, time-encoded `ses_` ids make PRIMARY KEY ASC
        // a bounded newest-created fallback; resumed old sessions can be
        // delayed on that schema (documented).
        let recency_order = if columns.contains("time_updated")
            && has_ordered_index_prefix(
                &snapshot.conn,
                "session",
                &[("time_updated", true), ("id", false)],
            )? {
            "time_updated DESC, id ASC"
        } else {
            "id ASC"
        };
        let sqlite_limit = requested_limit
            .map(i64::try_from)
            .transpose()
            .context("OpenCode discovery limit exceeds SQLite's signed 64-bit range")?;
        let limit_sql = sqlite_limit.map(|_| " LIMIT ?").unwrap_or_default();
        let sql = format!(
            "SELECT id, {directory}, {created}, {updated} FROM session \
             WHERE id IS NOT NULL AND id <> '' ORDER BY {recency_order}{limit_sql}"
        );
        let mut stmt = snapshot.conn.prepare(&sql)?;
        scan.note_query();
        let collect = |row: &rusqlite::Row<'_>| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<i64>>(2)?,
                row.get::<_, Option<i64>>(3)?,
            ))
        };
        let rows = match sqlite_limit {
            Some(limit) => stmt
                .query_map([limit], collect)?
                .collect::<rusqlite::Result<Vec<_>>>()?,
            None => stmt
                .query_map([], collect)?
                .collect::<rusqlite::Result<Vec<_>>>()?,
        };
        scan.note_records(rows.len() as u64);
        snapshot.sessions = rows
            .iter()
            .map(|(id, directory, created, updated)| {
                (
                    id.clone(),
                    OpencodeSessionSeed {
                        directory: directory.clone(),
                        created: *created,
                        updated: *updated,
                    },
                )
            })
            .collect();
        Ok(rows
            .into_iter()
            .map(|(id, _directory, created, updated)| Candidate {
                source: "opencode",
                locator: id.clone(),
                session_id: Some(id),
                recency_hint_ms: updated.or(created),
                stamp: format!(
                    "{}:{}:{}",
                    snapshot.store_identity,
                    created.unwrap_or(0),
                    updated.unwrap_or(0)
                ),
            })
            .collect())
    }

    fn read_shallow(
        &self,
        scan: &ScanEnv<'_>,
        _catalog: Option<&Connection>,
        candidate: &Candidate,
    ) -> Result<Option<ShallowSession>> {
        // Layout is encoded by the candidate shape: SQLite enumeration uses
        // the session id itself as the locator, while the JSON tree uses its
        // session-file path (and may have no session id for an unreadable
        // directory). Do not re-detect the host here: OpenCode can create its
        // SQLite store after JSON enumeration, and those locators must still
        // be read by the layout that produced them.
        if candidate.session_id.as_deref() != Some(candidate.locator.as_str()) {
            return read_shallow_opencode_json_tree(scan, candidate);
        }
        let guard = self.live.lock().expect("opencode live snapshot lock");
        let Some(snapshot) = guard.as_ref() else {
            return Ok(None);
        };
        let conn = &snapshot.conn;
        let Some(seed) = snapshot.sessions.get(&candidate.locator).cloned() else {
            return Ok(None);
        };
        // The excerpt is cut in SQL, not in Rust: a single opencode part can
        // hold a whole pasted file, and materializing it just to take the
        // first 4096 characters would break the bounded-read promise for a
        // catalog entry.
        let prompt_schema = snapshot.part_columns.contains("data")
            && snapshot.part_columns.contains("message_id")
            && snapshot.message_columns.contains("id")
            && snapshot.message_columns.contains("data");
        let first_prompt = if prompt_schema
            && (snapshot.part_by_session
                || (snapshot.message_by_session && snapshot.part_by_message))
        {
            let keyed_predicate = if snapshot.part_by_session {
                "p.session_id = ?"
            } else {
                "m.session_id = ?"
            };
            let order = match (
                snapshot.part_columns.contains("time_created"),
                snapshot.message_columns.contains("time_created"),
            ) {
                (true, true) => "COALESCE(p.time_created, m.time_created)",
                (true, false) => "p.time_created",
                (false, true) => "m.time_created",
                (false, false) => "p.id",
            };
            let sql = format!(
                "SELECT substr(json_extract(p.data, '$.text'), 1, ?) \
                 FROM part p JOIN message m ON m.id = p.message_id \
                 WHERE {keyed_predicate} AND json_valid(m.data) AND json_valid(p.data) \
                 AND json_extract(m.data, '$.role') = 'user' \
                 AND json_extract(p.data, '$.type') = 'text' \
                 AND COALESCE(json_type(p.data, '$.synthetic'), 'null') <> 'true' \
                 AND json_type(p.data, '$.text') = 'text' \
                 AND trim(substr(json_extract(p.data, '$.text'), 1, ?), ?) <> '' \
                 ORDER BY {order} ASC LIMIT 1"
            );
            scan.note_query();
            let prompt = {
                let mut stmt = conn.prepare_cached(&sql)?;
                stmt.query_row(
                    params![
                        EXCERPT_MAX_CHARS as i64,
                        &candidate.locator,
                        EXCERPT_MAX_CHARS as i64,
                        EXCERPT_TRIM_WHITESPACE
                    ],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
            };
            scan.note_records(u64::from(prompt.is_some()));
            prompt
                .map(|text| excerpt(&text))
                .filter(|text| !text.is_empty())
        } else {
            None
        };
        let mut models = Vec::new();
        if snapshot.message_by_session
            && snapshot.message_columns.contains("session_id")
            && snapshot.message_columns.contains("data")
        {
            scan.note_query();
            let model = {
                // Match `parse_message` and `OpencodeSession::first_model`:
                // payload time wins, the relational column is its fallback,
                // and a message with neither is not parseable. Checking for a
                // JSON integer mirrors `Value::as_i64`; a string that merely
                // looks numeric must not take precedence here.
                let payload_created = "CASE WHEN json_type(data, '$.time.created') = 'integer' \
                                       AND typeof(json_extract(data, '$.time.created')) = 'integer' \
                                       THEN json_extract(data, '$.time.created') END";
                let created = if snapshot.message_columns.contains("time_created") {
                    format!("COALESCE({payload_created}, time_created)")
                } else {
                    payload_created.to_string()
                };
                let order_by = if snapshot.message_columns.contains("id") {
                    format!("ORDER BY {created} ASC, id ASC")
                } else {
                    format!("ORDER BY {created} ASC")
                };
                let sql = format!(
                    "SELECT json_extract(data, '$.providerID'), \
                            COALESCE(json_extract(data, '$.modelID'), \
                                     json_extract(data, '$.model.modelID')) \
                     FROM message WHERE session_id = ? AND json_valid(data) \
                     AND json_extract(data, '$.role') = 'assistant' \
                     AND {created} IS NOT NULL \
                     AND (NULLIF(json_extract(data, '$.providerID'), '') IS NOT NULL \
                          OR NULLIF(COALESCE(json_extract(data, '$.modelID'), \
                                             json_extract(data, '$.model.modelID')), '') IS NOT NULL) \
                     {order_by} LIMIT 1"
                );
                let mut stmt = conn.prepare_cached(&sql)?;
                stmt.query_row([&candidate.locator], |row| {
                    Ok((
                        row.get::<_, Option<String>>(0)?,
                        row.get::<_, Option<String>>(1)?,
                    ))
                })
                .optional()?
                .and_then(|(provider, model)| {
                    crate::ingest::opencode::build_model(provider.as_deref(), model.as_deref())
                })
            };
            scan.note_records(u64::from(model.is_some()));
            push_unique(&mut models, model.as_deref());
        }
        Ok(Some(ShallowSession {
            source: "opencode".into(),
            session_id: candidate.locator.clone(),
            cwd: seed.directory,
            first_activity_ms: seed.created,
            last_activity_ms: seed.updated.or(seed.created),
            first_prompt,
            models,
            // Preserve which concrete OpenCode store produced this catalog
            // identity. Hydration verifies that provenance before reading.
            raw_path: Some(scan.opencode_db.to_string_lossy().into_owned()),
            ..Default::default()
        }))
    }
}

/// Enumerate the legacy tree's `session/<scope>/ses_*.json` files.
///
/// The tree has no index, so both the stamp and the recency hint are computed
/// over *every file that composes a session* — the session JSON, its messages
/// and their parts — by the same helper hydration stamps with.
///
/// Two things make that necessary rather than thorough. OpenCode appends a
/// turn by writing new files under `message/` and `part/` without touching the
/// session JSON, so a stamp over that file alone reports an active session as
/// unchanged and the cache serves its stale first prompt and model forever;
/// with a `--limit`, ordering on the same unchanged timestamp also ranks a
/// busy session as old and can drop it from the page entirely. And the birth
/// time `file_generation_time` prefers does not move when a session file is
/// rewritten in place, so it cannot be the change signal here either — the
/// helper reads modification time, and carries a file count and a byte total
/// so an edit that preserves both size and mtime still moves the stamp.
///
/// The cost is one `stat` per file in the tree, no reads and no parsing, and
/// it is paid before the limit because a limit applied to stale recency is
/// the bug above.
fn enumerate_opencode_json_tree(scan: &ScanEnv<'_>, root: &Path) -> Result<Vec<Candidate>> {
    // One session's failure is that session's failure. The stamp reads files
    // now, so an unlistable `message/` directory or an unreadable recent part
    // can fail it -- and propagating that out of the enumeration would take
    // every healthy session in the same tree down with it, neither cataloged
    // nor indexed for as long as the one path stays broken. `read_shallow`
    // already isolates exactly this failure per locator; the enumeration has
    // to as well.
    //
    // The candidate is still emitted, and deliberately not with a stamp that
    // could match a stored one: this run cannot say the session is unchanged,
    // and a stamp that compares equal is how the cached-skip path turns a read
    // failure into a permanent omission. A token unique to this run bypasses
    // that, `read_shallow` reaches the same failure, and the engine reports it
    // as a diagnostic against this locator alone -- no skip row, no catalog row
    // stamped as current.
    let unreadable = format!(
        "unreadable:{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or_default()
    );
    let listing = crate::ingest::opencode::list_json_tree_session_files(root);
    let mut rows = Vec::new();
    // A directory the walk could not list is emitted as a candidate of its
    // own. `read_shallow` reaches the same failure and the engine reports it
    // against that path -- which is the difference between "there are no
    // sessions under here" and "we could not look".
    for dir in &listing.unreadable {
        rows.push((
            String::new(),
            dir.path.clone(),
            session_file_mtime_ns(&dir.path),
            None,
            unreadable.clone(),
        ));
    }
    for path in listing.sessions {
        let Some(session_id) = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .map(str::to_string)
        else {
            continue;
        };
        if !path.is_file() {
            continue;
        }
        let (order_ns, recency_hint_ms, stamp) =
            match crate::ingest::opencode::stamp_json_tree_session(&path, &session_id) {
                Ok(stamp) => (stamp.newest_ns, stamp.newest_ms(), stamp.token()),
                Err(_) => {
                    // Order it by the one file that is certainly its own, so a
                    // broken session neither jumps the queue nor sinks out of
                    // sight of a bounded page.
                    let own_ns = session_file_mtime_ns(&path);
                    (
                        own_ns,
                        i64::try_from(own_ns / 1_000_000).ok(),
                        unreadable.clone(),
                    )
                }
            };
        rows.push((session_id, path, order_ns, recency_hint_ms, stamp));
    }
    // Newest first, then by id, so the engine takes the same bounded head
    // every run and the tie-break is total.
    //
    // Not truncated here. A `ses_*.json` file is a *candidate*, and
    // `read_shallow` is what decides whether it is a session -- truncating
    // first lets a newest malformed file consume the whole of a `--limit 1`
    // and the valid session behind it is never discovered at all. The engine
    // stops after the requested number of sessions it actually emitted, which
    // is the contract the candidate window is written to. (The SQLite side
    // still limits in SQL, because there a `session` row *is* a session.)
    rows.sort_by(|a, b| b.2.cmp(&a.2).then_with(|| a.0.cmp(&b.0)));
    scan.note_records(rows.len() as u64);
    Ok(rows
        .into_iter()
        .map(|(session_id, path, _, recency_hint_ms, stamp)| Candidate {
            source: "opencode",
            locator: path.to_string_lossy().into_owned(),
            // Empty for an unlistable directory: it names no session, and
            // claiming one would have the engine reject the read as a mismatched
            // identity instead of reporting what actually went wrong.
            session_id: Some(session_id).filter(|id| !id.is_empty()),
            recency_hint_ms,
            stamp,
        })
        .collect())
}

/// A session file's own modification time in nanoseconds, or zero.
fn session_file_mtime_ns(path: &Path) -> u128 {
    std::fs::metadata(path)
        .and_then(|meta| meta.modified())
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or_default()
}

/// One session's catalog row from the legacy tree. This reads the session
/// file plus that session's own messages and parts — never the whole tree.
fn read_shallow_opencode_json_tree(
    scan: &ScanEnv<'_>,
    candidate: &Candidate,
) -> Result<Option<ShallowSession>> {
    let path = Path::new(&candidate.locator);
    scan.note_open();
    if path.is_dir() {
        // Enumeration hands the directory it could not walk straight through
        // to here, so the failure is reported against that path.
        fs::read_dir(path)
            .with_context(|| format!("listing OpenCode session directory {}", path.display()))?;
        // It lists now, so the outage was transient. It is still not a session,
        // and the next run walks what is under it.
        return Ok(None);
    }
    let Some(loaded) = crate::ingest::opencode::load_from_json_tree(path)? else {
        return Ok(None);
    };
    scan.note_records(loaded.messages.len() as u64);
    let first_prompt = loaded
        .first_user_text()
        .map(excerpt)
        .filter(|text| !text.is_empty());
    let mut models = Vec::new();
    push_unique(&mut models, loaded.first_model().as_deref());
    let times: Vec<i64> = loaded
        .messages
        .iter()
        .map(|message| message.time_created)
        .collect();
    Ok(Some(ShallowSession {
        source: "opencode".into(),
        session_id: loaded.session.id.clone(),
        cwd: loaded
            .messages
            .iter()
            .find_map(|message| message.path_cwd.clone())
            .or_else(|| loaded.session.directory.clone()),
        first_activity_ms: loaded
            .session
            .created_ms
            .or_else(|| times.iter().min().copied()),
        // The later of the two, not whichever exists. OpenCode appends a
        // turn without rewriting the session JSON, so `updated` routinely
        // lags its own newest message -- and preferring it catalogued a busy
        // session as last active whenever its JSON last changed, which sorts
        // it behind genuinely older sessions in the newest-first listing and
        // lets a bounded page drop it. Neither value supersedes the other, so
        // either one alone stands when the other is absent.
        last_activity_ms: match (loaded.session.updated_ms, times.iter().max().copied()) {
            (Some(updated), Some(newest)) => Some(updated.max(newest)),
            (Some(updated), None) => Some(updated),
            (None, newest) => newest,
        },
        first_prompt,
        models,
        // The concrete session file, so hydration can stamp exactly what
        // discovery read.
        raw_path: Some(candidate.locator.clone()),
        ..Default::default()
    }))
}

fn file_generation_time(metadata: &fs::Metadata) -> u128 {
    metadata
        .created()
        .or_else(|_| metadata.modified())
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_nanos())
        .unwrap_or_default()
}

#[cfg(unix)]
fn opencode_store_generation(path: &Path) -> Result<String> {
    use std::os::unix::fs::MetadataExt;
    let metadata = fs::metadata(path)?;
    Ok(format!(
        "{}:{}:{}",
        metadata.dev(),
        metadata.ino(),
        file_generation_time(&metadata)
    ))
}

#[cfg(not(unix))]
fn opencode_store_generation(path: &Path) -> Result<String> {
    let metadata = fs::metadata(path)?;
    Ok(format!(
        "{}:{}",
        metadata.len(),
        file_generation_time(&metadata)
    ))
}

// ---------------------------------------------------------------------------
// relay
// ---------------------------------------------------------------------------

/// Shallow adapter for the network-backed `relay` source.
///
/// Relaycast has no local transcript files, and discovery must work with no
/// network access, so this adapter derives catalog rows from rows a previous
/// `ai-hist sync` already stored in `history` (indexed by
/// `idx_history_session`). If nothing was ever synced it discovers nothing —
/// that is the correct answer, not a failure.
struct RelayProvider;

impl ShallowSessionProvider for RelayProvider {
    fn source(&self) -> &'static str {
        "relay"
    }
    /// Relay rows are enumerated out of already-ingested `history`; there is no
    /// relay parser, and targeted hydration is unsupported for it.
    fn evidence_kinds(&self) -> &'static [EvidenceKind] {
        &[]
    }

    /// Relay rows come from RelayHistory's own `history` table, so there is no
    /// file to stat — but "no file" is not "cannot change". `ai-hist import`
    /// writes relay history straight into that table without going through a
    /// sweep, and nothing else in the fingerprint moves when it does. Left out
    /// of the fold entirely, an import would land rows that discovery then
    /// declined to look at, and the imported sessions would never reach the
    /// catalog.
    ///
    /// So the signal is a generation for the relay slice: how many rows there
    /// are and the highest one. Both move on an insert, and the count alone
    /// moves on a delete. It is one range scan of `idx_history_session`, whose
    /// leading column is `source`, so the cost is proportional to the relay
    /// rows rather than the table.
    ///
    /// This does not invalidate itself: no local sweep source writes
    /// `source = 'relay'`, so a tick that folds this value cannot be the
    /// reason it changed next time.
    fn fingerprint_inputs(&self, env: &DiscoveryEnv<'_>) -> Result<Vec<Candidate>> {
        let (rows, highest) = env.conn().query_row(
            "SELECT COUNT(*), COALESCE(MAX(rowid), 0) FROM history WHERE source = 'relay'",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )?;
        if rows == 0 {
            return Ok(Vec::new());
        }
        Ok(vec![Candidate {
            source: "relay",
            locator: "history:relay".into(),
            session_id: None,
            recency_hint_ms: None,
            stamp: format!("{rows}:{highest}"),
        }])
    }

    fn enumerate(
        &self,
        env: &DiscoveryEnv<'_>,
        _requested_limit: Option<usize>,
    ) -> Result<Vec<Candidate>> {
        let mut stmt = env.conn().prepare(
            "SELECT session_id, MAX(timestamp_ms), COUNT(*) FROM history \
             WHERE source = 'relay' AND session_id IS NOT NULL AND session_id <> '' \
             GROUP BY session_id",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows
            .into_iter()
            .map(|(session_id, last, count)| Candidate {
                source: "relay",
                locator: session_id.clone(),
                session_id: Some(session_id),
                recency_hint_ms: last,
                stamp: format!("{}:{count}", last.unwrap_or(0)),
            })
            .collect())
    }

    fn read_access(&self) -> ShallowReadAccess {
        ShallowReadAccess::Catalog
    }

    fn read_shallow(
        &self,
        _scan: &ScanEnv<'_>,
        catalog: Option<&Connection>,
        candidate: &Candidate,
    ) -> Result<Option<ShallowSession>> {
        let conn = catalog.context("relay shallow reads need the catalog connection")?;
        let bounds = conn.query_row(
            "SELECT MIN(timestamp_ms), MAX(timestamp_ms) FROM history \
             WHERE source = 'relay' AND session_id = ?",
            [&candidate.locator],
            |row| Ok((row.get::<_, Option<i64>>(0)?, row.get::<_, Option<i64>>(1)?)),
        )?;
        let first_prompt = conn
            .query_row(
                "SELECT prompt FROM history WHERE source = 'relay' AND session_id = ? \
                 ORDER BY timestamp_ms ASC, id ASC LIMIT 1",
                [&candidate.locator],
                |row| row.get::<_, String>(0),
            )
            .ok()
            .map(|prompt| excerpt(&prompt))
            .filter(|prompt| !prompt.is_empty());
        Ok(Some(ShallowSession {
            source: "relay".into(),
            session_id: candidate.locator.clone(),
            first_activity_ms: bounds.0,
            last_activity_ms: bounds.1,
            first_prompt,
            ..Default::default()
        }))
    }
}

// ---------------------------------------------------------------------------
// shared enumeration helper
// ---------------------------------------------------------------------------

/// One stat's worth of enumeration facts: the change stamp and the recency
/// hint in milliseconds.
type StampAndRecency = (String, Option<i64>);

fn file_candidates(
    source: &'static str,
    files: Vec<PathBuf>,
    stamp: fn(&Path) -> Result<StampAndRecency>,
) -> Result<Vec<Candidate>> {
    let mut out = Vec::with_capacity(files.len());
    for path in files {
        // A file that vanished between the walk and the stat is not an error;
        // the next run will simply not see it.
        let Ok((stamp, recency_hint_ms)) = stamp(&path) else {
            continue;
        };
        out.push(Candidate {
            source,
            locator: path.to_string_lossy().into_owned(),
            session_id: None,
            recency_hint_ms,
            stamp,
        });
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// catalog reads and writes
// ---------------------------------------------------------------------------

pub(crate) const SESSION_COLUMNS: &str = "source, session_id, cwd, git_branch, first_activity_ms, \
     last_activity_ms, first_prompt, last_assistant_text, models_json, originator, \
     agent_version, repo_url, initial_commit, workspace_roots_json, raw_path, source_stamp, \
     discovery_state, project_key, project_key_method, \
     CASE \
       WHEN EXISTS (SELECT 1 FROM session_presences p WHERE p.source = sessions.source AND p.session_id = sessions.session_id AND p.location = 'local') \
        AND EXISTS (SELECT 1 FROM session_presences p WHERE p.source = sessions.source AND p.session_id = sessions.session_id AND p.location = 'remote') \
       THEN '[\"local\",\"remote\"]' \
       WHEN EXISTS (SELECT 1 FROM session_presences p WHERE p.source = sessions.source AND p.session_id = sessions.session_id AND p.location = 'remote') \
       THEN '[\"remote\"]' \
       WHEN EXISTS (SELECT 1 FROM session_presences p WHERE p.source = sessions.source AND p.session_id = sessions.session_id AND p.location = 'local') \
       THEN '[\"local\"]' \
       ELSE '[]' \
     END";

fn json_string_list(raw: Option<String>) -> Vec<String> {
    raw.and_then(|raw| serde_json::from_str::<Vec<String>>(&raw).ok())
        .unwrap_or_default()
}

pub(crate) fn row_to_session(row: &rusqlite::Row<'_>) -> rusqlite::Result<ShallowSession> {
    Ok(ShallowSession {
        source: row.get(0)?,
        session_id: row.get(1)?,
        cwd: row.get(2)?,
        git_branch: row.get(3)?,
        first_activity_ms: row.get(4)?,
        last_activity_ms: row.get(5)?,
        first_prompt: row.get(6)?,
        last_assistant_text: row.get(7)?,
        models: json_string_list(row.get(8)?),
        originator: row.get(9)?,
        agent_version: row.get(10)?,
        repo_url: row.get(11)?,
        initial_commit: row.get(12)?,
        workspace_roots: json_string_list(row.get(13)?),
        raw_path: row.get(14)?,
        source_stamp: row.get(15)?,
        discovery_state: row
            .get::<_, Option<String>>(16)?
            .unwrap_or_else(|| "full".to_string()),
        project_key: row.get(17)?,
        project_key_method: row.get(18)?,
        locations: json_string_list(row.get(19)?),
        from_cache: true,
    })
}

/// A precise continuation point in the catalog's total order.
///
/// The catalog is ordered `(last_activity_ms DESC, source ASC, session_id ASC)`.
/// Recency alone is not a key: a single discovery pass can stamp dozens of
/// sessions with the same mtime-derived millisecond, and a cursor that carries
/// only a timestamp silently drops every row tied with the page boundary. The
/// identity columns make the cursor total.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CatalogCursor {
    /// Last activity of the final row on the previous page; `None` for a row
    /// whose recency is unknown (those sort last, after every dated row).
    pub last_activity_ms: Option<i64>,
    /// Source of the final row on the previous page.
    pub source: String,
    /// Session id of the final row on the previous page.
    pub session_id: String,
}

/// Options for the cache-only catalog listing.
#[derive(Debug, Clone, Default)]
pub struct CatalogListOptions {
    /// Which presences to include. Defaults to local for compatibility.
    pub scope: SessionScope,
    /// Restrict to these sources. Empty means every discoverable source.
    pub sources: Vec<String>,
    /// Row cap; defaults to [`DEFAULT_CATALOG_LIMIT`].
    pub limit: Option<i64>,
    /// Coarse cutoff: only sessions strictly older than this millisecond.
    /// Convenient for "show me anything before last Tuesday", but it cannot
    /// separate rows that share a millisecond — use [`CatalogListOptions::after`]
    /// to walk pages. Ignored when `after` is set.
    pub before_ms: Option<i64>,
    /// Precise continuation from the previous page's `next_cursor`.
    pub after: Option<CatalogCursor>,
    /// Restrict to one canonical project identity, as
    /// [`ShallowSession::project_key`] spells it (`host/owner/repo`, or the
    /// working directory for a checkout with no remote). Exact match, not a
    /// prefix: `github.com/org/repo` and `github.com/org/repo-fork` are
    /// different projects.
    pub project_key: Option<String>,
}

/// One page of the catalog plus the cursor that continues it.
#[derive(Debug, Clone, Default)]
pub struct SessionCatalogPage {
    /// Scope applied to this cache-only page.
    #[cfg(feature = "unstable-internal")]
    pub scope: SessionScope,
    /// The rows, newest first.
    pub sessions: Vec<ShallowSession>,
    /// Pass as [`CatalogListOptions::after`] for the next page. `None` when
    /// this page did not fill its limit, i.e. the catalog is exhausted.
    pub next_cursor: Option<CatalogCursor>,
}

/// The catalog listing query and its bound arguments.
///
/// Built in one place so the query-plan test asserts the plan of the statement
/// that actually runs.
fn catalog_list_query(options: &CatalogListOptions) -> (String, Vec<Box<dyn rusqlite::ToSql>>) {
    let mut sql = format!("SELECT {SESSION_COLUMNS} FROM sessions WHERE ");
    if options.scope == SessionScope::Local {
        sql.push_str("source <> 'trajectory'");
    } else {
        // Local trajectories are derived artifacts; remotely recalled trajectory
        // sessions are provider evidence and must survive cached remote/all reads.
        sql.push_str("(source <> 'trajectory' OR EXISTS (SELECT 1 FROM session_presences p WHERE p.source = sessions.source AND p.session_id = sessions.session_id AND p.location = 'remote'))");
    }
    let mut args: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
    match options.scope {
        SessionScope::Local => sql.push_str(
            " AND (EXISTS (SELECT 1 FROM session_presences p WHERE p.source = sessions.source AND p.session_id = sessions.session_id AND p.location = 'local') \
               OR NOT EXISTS (SELECT 1 FROM session_presences p WHERE p.source = sessions.source AND p.session_id = sessions.session_id))",
        ),
        SessionScope::Remote => sql.push_str(
            " AND EXISTS (SELECT 1 FROM session_presences p WHERE p.source = sessions.source AND p.session_id = sessions.session_id AND p.location = 'remote')",
        ),
        SessionScope::All => {}
    }
    if !options.sources.is_empty() {
        let placeholders = vec!["?"; options.sources.len()].join(", ");
        sql.push_str(&format!(" AND source IN ({placeholders})"));
        for source in &options.sources {
            args.push(Box::new(source.clone()));
        }
    }
    if let Some(project_key) = options.project_key.as_ref() {
        sql.push_str(" AND project_key = ?");
        args.push(Box::new(project_key.clone()));
    }
    match options.after.as_ref() {
        // Everything strictly after the cursor in the catalog's total order.
        // Undated rows sort last, so a dated cursor must still reach them.
        Some(cursor) => match cursor.last_activity_ms {
            Some(ms) => {
                sql.push_str(
                    " AND (last_activity_ms IS NULL OR last_activity_ms < ? \
                       OR (last_activity_ms = ? \
                           AND (source > ? OR (source = ? AND session_id > ?))))",
                );
                args.push(Box::new(ms));
                args.push(Box::new(ms));
                args.push(Box::new(cursor.source.clone()));
                args.push(Box::new(cursor.source.clone()));
                args.push(Box::new(cursor.session_id.clone()));
            }
            None => {
                sql.push_str(
                    " AND last_activity_ms IS NULL \
                       AND (source > ? OR (source = ? AND session_id > ?))",
                );
                args.push(Box::new(cursor.source.clone()));
                args.push(Box::new(cursor.source.clone()));
                args.push(Box::new(cursor.session_id.clone()));
            }
        },
        None => {
            if let Some(before_ms) = options.before_ms {
                sql.push_str(" AND last_activity_ms < ?");
                args.push(Box::new(before_ms));
            }
        }
    }
    sql.push_str(" ORDER BY last_activity_ms DESC, source ASC, session_id ASC LIMIT ?");
    args.push(Box::new(options.limit.unwrap_or(DEFAULT_CATALOG_LIMIT)));
    (sql, args)
}

/// List the session catalog straight out of the database.
///
/// Pure SQL over `sessions`: no filesystem access, no provider I/O, and no
/// scan of `history` / `session_events` / `tool_calls`. Locally derived
/// `trajectory` records are excluded. Remote/all scopes can include trajectory
/// sessions with an explicitly observed remote presence from cloud recall.
///
/// Rows come back in the catalog's total order:
/// `(last_activity_ms DESC, source ASC, session_id ASC)`, with rows of unknown
/// recency last. Use [`list_session_catalog_page`] to paginate.
pub fn list_session_catalog(
    conn: &Connection,
    options: &CatalogListOptions,
) -> Result<Vec<ShallowSession>> {
    let (sql, args) = catalog_list_query(options);
    let mut stmt = conn.prepare(&sql)?;
    let params = rusqlite::params_from_iter(args.iter().map(|arg| arg.as_ref()));
    let rows = stmt
        .query_map(params, row_to_session)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// [`list_session_catalog`] plus the cursor that continues it.
///
/// The cursor is `None` once a page comes back short of its limit, so a
/// caller walks the catalog by following `next_cursor` until it is absent —
/// no duplicated and no skipped rows, even when a whole page shares one
/// millisecond.
pub fn list_session_catalog_page(
    conn: &Connection,
    options: &CatalogListOptions,
) -> Result<SessionCatalogPage> {
    let sessions = list_session_catalog(conn, options)?;
    let limit = options.limit.unwrap_or(DEFAULT_CATALOG_LIMIT);
    let next_cursor = (limit > 0 && sessions.len() as i64 >= limit)
        .then(|| sessions.last())
        .flatten()
        .map(|row| CatalogCursor {
            last_activity_ms: row.last_activity_ms,
            source: row.source.clone(),
            session_id: row.session_id.clone(),
        });
    Ok(SessionCatalogPage {
        #[cfg(feature = "unstable-internal")]
        scope: options.scope,
        sessions,
        next_cursor,
    })
}

// Classification runs these once per candidate; the SQL strings are built
// once and the prepared statements ride the connection's cache, because
// re-preparing them dominated a cold discovery of a large catalog.
static CATALOG_ROW_SQL: LazyLock<String> = LazyLock::new(|| {
    format!("SELECT {SESSION_COLUMNS} FROM sessions WHERE source = ? AND session_id = ?")
});
fn fetch_catalog_row(
    conn: &Connection,
    source: &str,
    session_id: &str,
) -> Result<Option<ShallowSession>> {
    Ok(conn
        .prepare_cached(&CATALOG_ROW_SQL)
        .and_then(|mut stmt| stmt.query_row(params![source, session_id], row_to_session))
        .ok())
}

/// The catalog columns with the two text excerpts replaced by `NULL`, for a
/// read that asked for no transcript text: the excerpts are bounded, but
/// "bounded" is not "not moved", and a hash-only consumer is promised the
/// latter.
static SESSION_COLUMNS_NO_TEXT: LazyLock<String> = LazyLock::new(|| {
    SESSION_COLUMNS
        .replacen("first_prompt, last_assistant_text,", "NULL AS first_prompt, NULL AS last_assistant_text,", 1)
});

fn catalog_columns(include_text: bool) -> &'static str {
    if include_text {
        SESSION_COLUMNS
    } else {
        &SESSION_COLUMNS_NO_TEXT
    }
}

/// One catalog row, or `None` when the session is not catalogued.
///
/// Unlike [`fetch_catalog_row`] this propagates a query failure instead of
/// folding it into `None`: the facade answers "no such session" from it, and
/// a database it could not read must not be reported as an empty catalog.
pub(crate) fn catalog_row(
    conn: &Connection,
    source: &str,
    session_id: &str,
    include_text: bool,
) -> Result<Option<ShallowSession>> {
    Ok(conn
        .prepare_cached(&format!(
            "SELECT {} FROM sessions WHERE source = ? AND session_id = ?",
            catalog_columns(include_text)
        ))?
        .query_row(params![source, session_id], row_to_session)
        .optional()?)
}

/// The catalog row whose local locator is `raw_path`, if any.
pub(crate) fn catalog_row_by_path(
    conn: &Connection,
    source: &str,
    raw_path: &str,
    include_text: bool,
) -> Result<Option<ShallowSession>> {
    Ok(conn
        .prepare(&format!(
            "SELECT {} FROM sessions WHERE source = ? AND raw_path = ? \
             ORDER BY session_id LIMIT 1",
            catalog_columns(include_text)
        ))?
        .query_row(params![source, raw_path], row_to_session)
        .optional()?)
}

/// The filesystem roots one source's adapter watches under `roots`.
pub(crate) fn provider_watch_roots(source: &str, roots: &crate::ProviderRoots) -> Vec<WatchRoot> {
    let providers = shallow_providers();
    let Some(provider) = providers.iter().find(|provider| provider.source() == source) else {
        return Vec::new();
    };
    watch_roots(
        std::slice::from_ref(provider),
        &ProviderRoots {
            home: &roots.home,
            claude: &roots.claude,
            codex: &roots.codex,
            grok: &roots.grok,
            opencode_db: &roots.opencode_db,
        },
    )
}

#[cfg(test)]
fn fetch_catalog_row_at_location(
    conn: &Connection,
    source: &str,
    session_id: &str,
    location: SessionLocation,
) -> Result<Option<ShallowSession>> {
    let Some(mut row) = fetch_catalog_row(conn, source, session_id)? else {
        return Ok(None);
    };
    let location = match location {
        SessionLocation::Local => "local",
        SessionLocation::Remote => "remote",
    };
    let presence = conn
        .prepare_cached(
            "SELECT raw_locator, source_stamp FROM session_presences \
             WHERE source = ? AND session_id = ? AND location = ?",
        )?
        .query_row(params![source, session_id, location], |result| {
            Ok((
                result.get::<_, Option<String>>(0)?,
                result.get::<_, Option<String>>(1)?,
            ))
        })
        .optional()?;
    let Some((raw_locator, source_stamp)) = presence else {
        return Ok(None);
    };
    row.raw_path = raw_locator;
    row.source_stamp = source_stamp;
    Ok(Some(row))
}

fn observation_key(
    provider: &dyn ShallowSessionProvider,
    source: &str,
    session_id: &str,
) -> crate::observations::ObservationKey {
    crate::observations::ObservationKey {
        source: source.into(),
        session_id: session_id.into(),
        location: provider.location(),
        connector_id: provider.connector_id().into(),
        connector_instance: provider.connector_instance().into(),
    }
}

fn fetch_observed_candidate(
    conn: &Connection,
    provider: &dyn ShallowSessionProvider,
    candidate: &Candidate,
) -> Result<Option<ShallowSession>> {
    let id = match candidate.session_id.as_ref() {
        Some(id) => Some(id.clone()),
        None => conn.query_row("SELECT session_id FROM session_observations WHERE source=? AND location=? AND connector_id=? AND connector_instance=? AND raw_locator=? AND access_state='available' ORDER BY session_id LIMIT 1",params![candidate.source,provider.location().as_str(),provider.connector_id(),provider.connector_instance(),candidate.locator],|r|r.get(0)).optional()?,
    };
    let Some(id) = id else { return Ok(None) };
    let Some(observation) =
        crate::observations::get(conn, &observation_key(provider, candidate.source, &id))?
    else {
        return Ok(None);
    };
    if observation.access_state != "available" {
        return Ok(None);
    }
    let Some(mut row) = fetch_catalog_row(conn, candidate.source, &id)? else {
        return Ok(None);
    };
    row.source_stamp = observation.source_stamp;
    Ok(Some(row))
}

/// Whether this source was already examined at this exact stamp and found not
/// to be a session.
///
/// Without this, every codex subagent thread and every claude sidecar — real
/// files that legitimately produce no catalog row — was re-read on every
/// single run, because "no row" left nothing for the stamp check to match.
fn is_known_non_session(
    conn: &Connection,
    provider: &dyn ShallowSessionProvider,
    source: &str,
    locator: &str,
    stamp: &str,
) -> Result<bool> {
    let known:Option<String>=conn.query_row("SELECT stamp FROM observation_discovery_skips WHERE source=? AND location=? AND connector_id=? AND connector_instance=? AND locator=?",params![source,provider.location().as_str(),provider.connector_id(),provider.connector_instance(),locator],|r|r.get(0)).optional()?;
    Ok(known.as_deref() == Some(stamp))
}

fn record_non_session(
    conn: &Connection,
    provider: &dyn ShallowSessionProvider,
    source: &str,
    locator: &str,
    stamp: &str,
) -> Result<()> {
    conn.execute("INSERT INTO observation_discovery_skips(source,location,connector_id,connector_instance,locator,stamp,updated_ms) VALUES(?,?,?,?,?,?,?) ON CONFLICT(source,location,connector_id,connector_instance,locator) DO UPDATE SET stamp=excluded.stamp,updated_ms=excluded.updated_ms",params![source,provider.location().as_str(),provider.connector_id(),provider.connector_instance(),locator,stamp,now_ms()])?;
    Ok(())
}

fn clear_non_session(
    conn: &Connection,
    provider: &dyn ShallowSessionProvider,
    source: &str,
    locator: &str,
) -> Result<()> {
    conn.execute("DELETE FROM observation_discovery_skips WHERE source=? AND location=? AND connector_id=? AND connector_instance=? AND locator=?",params![source,provider.location().as_str(),provider.connector_id(),provider.connector_instance(),locator])?;
    Ok(())
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or_default()
}

/// Re-resolve the project identity of a row served straight from the catalog.
///
/// The stamp shortcut is right about everything the transcript says and wrong
/// about the one field derived from the filesystem beside it. A session first
/// seen before its checkout had an `origin` would otherwise keep its path key
/// on every later pass, because the transcript never changes and so the row is
/// never reconsidered.
///
/// This runs where the row is read rather than as a sweep at the end of the
/// pass, because the row is *streamed*: `on_row` hands it to the caller as
/// soon as the window is decided, so a later correction would fix the catalog
/// and still have emitted the stale value — the JSONL a consumer parses would
/// disagree with the database it came from.
///
/// For the same reason, the answer it streams must be the answer the
/// end-of-pass refresh will store, so a cached child that is about to inherit
/// its parent's repository is given that key here rather than the path its own
/// directory resolves to.
///
/// Only a key that is absent, a path, or inherited can change, and an
/// inherited one only for the child's own `remote`. A `remote` key is never
/// touched, and inheritance is never traded for a path. The write is skipped
/// entirely when the answer is the one already stored, which is the usual
/// case: these are reconsidered because they *might* be upgradable, and almost
/// never are.
fn upgrade_cached_project_identity(conn: &Connection, row: &mut ShallowSession) -> Result<()> {
    let stored = row.project_key_method.as_deref();
    if stored == Some(ProjectKeyMethod::Remote.as_str()) {
        // The strongest answer there is. Nothing this function can learn
        // improves on it, so do not even touch the filesystem.
        return Ok(());
    }
    let resolved =
        crate::project_identity::identity_for(row.cwd.as_deref(), row.repo_url.as_deref());
    let candidate = match resolved {
        // The session's own repository beats anything borrowed.
        Some((key, ProjectKeyMethod::Remote)) => Some((key, ProjectKeyMethod::Remote)),
        // Anything weaker has to be weighed against what the end-of-pass
        // refresh is about to do to this row. Deciding that here, and not only
        // in the pass, is what keeps the row this function streams equal to
        // the row the pass will store: a consumer reading
        // `sessions discover --json` beside the catalog must not be told two
        // different projects.
        //
        // `inheritable_parent_project_key` is asked even when this row already
        // holds a borrowed key, because the key it borrowed can go stale — the
        // same refresh may promote its parent to a `remote` of its own, and
        // pass 2 then lends the new one down.
        weaker => {
            match crate::store::inheritable_parent_project_key(
                conn,
                &row.source,
                &row.session_id,
            )? {
                Some(parent) => Some((parent, ProjectKeyMethod::Inherited)),
                // An inherited key is borrowed, so it outranks a path: a child
                // whose directory still resolves to nothing canonical keeps
                // the parent's repository, which is the point of inheriting it.
                None if stored == Some(ProjectKeyMethod::Inherited.as_str()) => None,
                None => weaker,
            }
        }
    };
    let Some((key, method)) = candidate else {
        return Ok(());
    };
    if row.project_key.as_deref() == Some(key.as_str()) && stored == Some(method.as_str()) {
        return Ok(());
    }
    // The same precedence the merge and the refresh pass apply, as a guard:
    // this runs outside any transaction, so a hydrate or a concurrent sync may
    // have settled the row since it was read.
    let changed = conn.execute(
        "UPDATE sessions SET project_key = ?1, project_key_method = ?2 \
         WHERE source = ?3 AND session_id = ?4 \
           AND (project_key IS NULL \
                OR project_key_method = 'path' \
                OR (project_key_method = 'inherited' \
                    AND ?2 IN ('remote', 'inherited')))",
        params![key, method.as_str(), row.source, row.session_id],
    )?;
    if changed == 0 {
        // Refused: someone else knows better. Stream what the catalog holds
        // rather than what this function wanted it to hold — an emitted key no
        // row anywhere agrees with is worse than a stale one, because nothing
        // downstream can tell it is wrong.
        if let Some((key, method)) = conn
            .query_row(
                "SELECT project_key, project_key_method FROM sessions \
                 WHERE source = ?1 AND session_id = ?2",
                params![row.source, row.session_id],
                |stored| Ok((stored.get(0)?, stored.get(1)?)),
            )
            .optional()?
        {
            row.project_key = key;
            row.project_key_method = method;
        }
        return Ok(());
    }
    row.project_key = Some(key);
    row.project_key_method = Some(method.as_str().to_string());
    Ok(())
}

static UPSERT_SESSION_SQL: LazyLock<String> = LazyLock::new(|| {
    let project_key_merge = crate::store::project_key_merge_sql();
    format!(
        "INSERT INTO sessions \
         (session_id, source, cwd, git_branch, first_activity_ms, last_activity_ms, \
          last_assistant_text, raw_path, parser_version, first_prompt, models_json, originator, \
          agent_version, repo_url, initial_commit, workspace_roots_json, source_stamp, \
          project_key, project_key_method, \
          discovery_state) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?18, ?19, 'shallow') \
         ON CONFLICT(session_id, source) DO UPDATE SET \
         {project_key_merge}, \
         cwd = COALESCE(excluded.cwd, sessions.cwd), \
         git_branch = COALESCE(excluded.git_branch, sessions.git_branch), \
         first_activity_ms = CASE \
             WHEN excluded.source = 'grok' THEN COALESCE(excluded.first_activity_ms, sessions.first_activity_ms) \
             WHEN excluded.first_activity_ms IS NULL THEN sessions.first_activity_ms \
             WHEN sessions.first_activity_ms IS NULL THEN excluded.first_activity_ms \
             ELSE MIN(sessions.first_activity_ms, excluded.first_activity_ms) END, \
         last_activity_ms = CASE \
             WHEN excluded.source = 'grok' THEN COALESCE(excluded.last_activity_ms, sessions.last_activity_ms) \
             WHEN excluded.last_activity_ms IS NULL THEN sessions.last_activity_ms \
             WHEN sessions.last_activity_ms IS NULL THEN excluded.last_activity_ms \
             ELSE MAX(sessions.last_activity_ms, excluded.last_activity_ms) END, \
         last_assistant_text = COALESCE(excluded.last_assistant_text, sessions.last_assistant_text), \
         raw_path = CASE WHEN ?17 = 'remote' AND EXISTS ( \
             SELECT 1 FROM session_presences p WHERE p.source = sessions.source \
             AND p.session_id = sessions.session_id AND p.location = 'local') \
             THEN COALESCE(sessions.raw_path, excluded.raw_path) \
             ELSE COALESCE(excluded.raw_path, sessions.raw_path) END, \
         first_prompt = COALESCE(excluded.first_prompt, sessions.first_prompt), \
         models_json = COALESCE(excluded.models_json, sessions.models_json), \
         originator = COALESCE(excluded.originator, sessions.originator), \
         agent_version = COALESCE(excluded.agent_version, sessions.agent_version), \
         repo_url = COALESCE(excluded.repo_url, sessions.repo_url), \
         initial_commit = COALESCE(excluded.initial_commit, sessions.initial_commit), \
         workspace_roots_json = COALESCE(excluded.workspace_roots_json, sessions.workspace_roots_json), \
         source_stamp = COALESCE(excluded.source_stamp, sessions.source_stamp), \
         discovery_state = CASE \
             WHEN sessions.discovery_state IS NULL OR sessions.discovery_state = 'full' \
             THEN 'full' ELSE 'shallow' END \
         RETURNING {SESSION_COLUMNS}"
    )
});

/// Write a shallow row into the catalog, returning the merged row as stored.
///
/// Never nulls out a value the catalog already holds, never lowers
/// `first_activity_ms` past what a fuller pass observed for append-only
/// providers, and never downgrades a fully indexed row to `'shallow'` —
/// including a row from a database that predates `discovery_state`, whose NULL
/// readers deliberately interpret as `'full'`. Grok is the exception on the
/// activity bounds: a session directory is a replacement snapshot, so a later
/// compaction can move the start forward and the end backward. A shallow
/// rescan of such a row still refreshes its metadata and stamp.
///
/// The returned row is what the catalog now holds (including a preserved
/// `full` state), read back through the write's own `RETURNING` clause so the
/// merge costs no second lookup.
#[cfg(feature = "unstable-internal")]
pub fn upsert_shallow_session(
    conn: &Connection,
    session: &ShallowSession,
) -> Result<ShallowSession> {
    upsert_shallow_session_at_location(conn, session, SessionLocation::Local)
}

/// Upsert shallow canonical metadata and connector-specific presence state.
pub fn upsert_shallow_session_at_location(
    conn: &Connection,
    session: &ShallowSession,
    location: SessionLocation,
) -> Result<ShallowSession> {
    if conn.is_autocommit() {
        let transaction = conn.unchecked_transaction()?;
        let row = upsert_shallow_session_in_transaction(&transaction, session, location)?;
        transaction.commit()?;
        return Ok(row);
    }
    upsert_shallow_session_in_transaction(conn, session, location)
}

fn upsert_shallow_session_in_transaction(
    conn: &Connection,
    session: &ShallowSession,
    location: SessionLocation,
) -> Result<ShallowSession> {
    if location == SessionLocation::Remote {
        // Cache-only local reads classify a preexisting presence-less row as
        // legacy local. Preserve that classification before adding the first
        // remote presence, including the gap between a local full-session write
        // and its separate presence write.
        let legacy_local = conn
            .prepare_cached(
                "SELECT raw_path, source_stamp, discovery_state FROM sessions s \
             WHERE source = ? AND session_id = ? AND NOT EXISTS ( \
                 SELECT 1 FROM session_presences p WHERE p.source = s.source \
                 AND p.session_id = s.session_id)",
            )?
            .query_row(params![session.source, session.session_id], |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            })
            .optional()?;
        if let Some((raw_path, stamp, state)) = legacy_local {
            upsert_session_presence(
                conn,
                &session.source,
                &session.session_id,
                SessionLocation::Local,
                raw_path.as_deref(),
                stamp.as_deref(),
                state.as_deref(),
            )?;
        }
    }
    // The presence lands first: the sessions upsert's RETURNING clause
    // computes `locations` from `session_presences`, so this run's own
    // presence must already be visible when the merged row is read back.
    upsert_session_presence(
        conn,
        &session.source,
        &session.session_id,
        location,
        session.raw_path.as_deref(),
        session.source_stamp.as_deref(),
        Some(&session.discovery_state),
    )?;
    // One resolution per upsert, from the shallow row's own observations. A
    // provider-recorded remote (codex's `session_meta.payload.git`) is
    // preferred over walking the working directory, which may no longer exist
    // by the time the transcript is read. Resolving here rather than in each
    // provider's shallow reader keeps every source on one code path.
    let resolved = session.project_key.clone().map(|key| {
        (
            key,
            session
                .project_key_method
                .clone()
                .unwrap_or_else(|| ProjectKeyMethod::PathFallback.as_str().to_string()),
        )
    });
    let resolved = resolved.or_else(|| {
        crate::project_identity::identity_for(session.cwd.as_deref(), session.repo_url.as_deref())
            .map(|(key, method)| (key, method.as_str().to_string()))
    });
    let (project_key, project_key_method) = match resolved {
        Some((key, method)) => (Some(key), Some(method)),
        None => (None, None),
    };
    let mut row = conn.prepare_cached(&UPSERT_SESSION_SQL)?.query_row(
        params![
            session.session_id,
            session.source,
            session.cwd,
            session.git_branch,
            session.first_activity_ms,
            session.last_activity_ms,
            session.last_assistant_text,
            session.raw_path,
            session.first_prompt,
            json_array_or_none(&session.models),
            session.originator,
            session.agent_version,
            session.repo_url,
            session.initial_commit,
            json_array_or_none(&session.workspace_roots),
            session.source_stamp,
            // Remote provenance belongs on its presence; a local transcript
            // remains the canonical raw path used by local readers and sync.
            match location {
                SessionLocation::Local => "local",
                SessionLocation::Remote => "remote",
            },
            project_key,
            project_key_method,
        ],
        row_to_session,
    )?;
    row.from_cache = false;
    Ok(row)
}

// ---------------------------------------------------------------------------
// discovery engine
// ---------------------------------------------------------------------------

/// Options for one discovery run.
#[derive(Debug, Clone, Default)]
pub struct DiscoverOptions {
    /// Provider-presence scope. Defaults to local for compatibility.
    pub scope: SessionScope,
    /// Restrict to these sources. Empty means every adapter.
    #[cfg(any(test, feature = "unstable-internal"))]
    pub sources: Vec<String>,
    /// Global cap on emitted rows, applied across providers by recency.
    /// `None` means no cap.
    pub limit: Option<usize>,
}

/// Something one provider (or one session) could not do.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DiscoveryDiagnostic {
    pub connector_id: Option<String>,
    pub connector_instance: Option<String>,
    pub location: Option<SessionLocation>,
    /// Source the failure belongs to.
    pub source: String,
    /// Candidate locator, when the failure was scoped to one session.
    pub locator: Option<String>,
    /// Human-readable cause.
    pub error: String,
}

/// Per-provider tallies for one run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct ProviderSummary {
    /// Candidates this provider enumerated (before the global limit).
    pub candidates: usize,
    /// Rows emitted after a shallow read.
    pub discovered: usize,
    /// Rows served from the catalog because the stamp was unchanged.
    pub skipped_unchanged: usize,
    /// `true` when enumeration failed for at least one of this source's
    /// adapters (under `all` scope a source can have a local adapter and a
    /// remote connector; each failure also leaves its own diagnostic).
    pub failed: bool,
}

/// Per-instance counters alongside the legacy per-source totals.
#[derive(Debug, Clone, Serialize)]
pub struct ConnectorSummary {
    pub source: String,
    pub location: SessionLocation,
    pub connector_id: String,
    pub connector_instance: String,
    pub summary: ProviderSummary,
}

/// Outcome of one discovery run.
#[derive(Debug, Clone, Default, Serialize)]
pub struct DiscoverySummary {
    /// [`SESSION_CATALOG_CONTRACT_VERSION`].
    pub contract_version: u32,
    /// Scope selected for this discovery run.
    pub scope: SessionScope,
    /// Connector locations that executed (`"local"`, `"remote"`). The
    /// requested `scope` records the ask; this records what ran — an `all`
    /// request executes remote connectors only where one is configured. A
    /// location whose adapters all failed still executed; the failures are in
    /// `diagnostics`, and per-provider `failed` flags say which.
    pub locations_run: Vec<String>,
    /// Rows freshly read and upserted.
    pub discovered: usize,
    /// Rows served from the catalog on an unchanged stamp.
    pub skipped_unchanged: usize,
    /// Per-provider tallies, keyed by source.
    pub providers: BTreeMap<String, ProviderSummary>,
    pub connectors: Vec<ConnectorSummary>,
    /// Sources that deliberately have no adapter.
    pub exempt_sources: Vec<SourceExemption>,
    /// Non-fatal failures. A provider failing here never blocks another.
    pub diagnostics: Vec<DiscoveryDiagnostic>,
    /// Work actually performed.
    pub counters: DiscoveryCounters,
}

/// Every selected provider failed to enumerate, so the run made no progress.
///
/// Carries the run's summary — diagnostics included — so a caller can report
/// what each provider said before propagating the failure.
#[derive(Debug)]
pub struct AllProvidersFailed {
    /// The run as far as it got, including one diagnostic per failed provider.
    pub summary: DiscoverySummary,
}

impl std::fmt::Display for AllProvidersFailed {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Adapter-level failures each leave one diagnostic with no locator;
        // `providers` is keyed by source, which under `all` scope can merge a
        // local adapter and a remote connector into one entry.
        write!(
            formatter,
            "all {} session provider(s) failed; no provider made progress",
            self.summary
                .diagnostics
                .iter()
                .filter(|diagnostic| diagnostic.locator.is_none())
                .count()
        )
    }
}

impl std::error::Error for AllProvidersFailed {}

fn stored_stamp(raw: &str) -> String {
    format!("v{SHALLOW_SCANNER_VERSION}:{raw}")
}

#[cfg(any(test, feature = "unstable-internal"))]
fn select_providers(
    options: &DiscoverOptions,
    home: &Path,
    connectors: &crate::remote::SourceConnectorSelection,
) -> Result<Vec<Box<dyn ShallowSessionProvider>>> {
    for source in &options.sources {
        if let Some(exempt) = DISCOVERY_EXEMPTIONS
            .iter()
            .find(|entry| entry.source == source && options.scope == SessionScope::Local)
        {
            anyhow::bail!(
                "source '{}' is exempt from session discovery: {}",
                exempt.source,
                exempt.reason
            );
        }
        anyhow::ensure!(
            SOURCE_CHOICES.contains(&source.as_str()),
            "invalid source '{source}' (choose from {})",
            SOURCE_CHOICES.join(", ")
        );
    }
    // A remote-only request with no connector configured is refused loudly,
    // and source-aware: a filter that leaves a remote-only request with
    // nothing configured is the same unsupported request, scoped down.
    if options.scope == SessionScope::Remote {
        crate::remote::ensure_selected_remote_connectors_configured_for_at(
            "discovery",
            home,
            &options.sources,
            connectors,
        )?;
    }
    let mut providers: Vec<Box<dyn ShallowSessionProvider>> = Vec::new();
    if matches!(options.scope, SessionScope::Local | SessionScope::All) {
        providers.extend(shallow_providers());
    }
    if matches!(options.scope, SessionScope::Remote | SessionScope::All) {
        providers.extend(crate::remote::selected_remote_providers(
            home,
            options.limit,
            connectors,
            &options.sources,
        ));
    }
    if !options.sources.is_empty() {
        providers.retain(|provider| options.sources.iter().any(|s| s == provider.source()));
    }
    Ok(providers)
}

/// Discover sessions across every provider, newest first, emitting rows as
/// they are produced.
///
/// The ordering and the limit are **global**: candidates from all providers
/// are merged by recency hint and only then truncated, so `--limit 3` over two
/// providers returns the three newest sessions overall, not three from
/// whichever provider was enumerated first.
///
/// A provider whose enumeration fails contributes a diagnostic and nothing
/// else; the rest of the run continues. A malformed or unreadable individual
/// session likewise yields a per-session diagnostic. The call only fails when
/// *every* selected provider failed.
#[cfg(any(test, feature = "unstable-internal"))]
pub fn discover_sessions(
    conn: &Connection,
    options: &DiscoverOptions,
    on_row: impl FnMut(&ShallowSession),
) -> Result<DiscoverySummary> {
    let env = DiscoveryEnv::new(conn);
    discover_sessions_with_env(&env, options, on_row)
}

/// [`discover_sessions`] against an explicitly built [`DiscoveryEnv`].
#[cfg(any(test, feature = "unstable-internal"))]
pub fn discover_sessions_with_env(
    env: &DiscoveryEnv<'_>,
    options: &DiscoverOptions,
    on_row: impl FnMut(&ShallowSession),
) -> Result<DiscoverySummary> {
    discover_sessions_with_connectors(
        env,
        options,
        &crate::remote::SourceConnectorSelection::default(),
        on_row,
    )
}

/// Discover using an explicit allowlist; an empty selection runs local adapters only.
#[cfg(any(test, feature = "unstable-internal"))]
pub fn discover_sessions_with_connectors(
    env: &DiscoveryEnv<'_>,
    options: &DiscoverOptions,
    connectors: &crate::remote::SourceConnectorSelection,
    on_row: impl FnMut(&ShallowSession),
) -> Result<DiscoverySummary> {
    let providers = select_providers(options, &env.home, connectors)?;
    let mut summary = discover_sessions_with_providers(env, options, &providers, on_row)?;
    if options.scope == SessionScope::All {
        for status in crate::remote::selected_remote_connector_statuses_at(
            &env.home,
            connectors,
            &options.sources,
        ) {
            if !status.configured || status.connector == crate::remote::RELAYCAST_CONNECTOR {
                summary.diagnostics.push(DiscoveryDiagnostic {
                    connector_id: Some(status.connector.into()),
                    connector_instance: None,
                    location: Some(SessionLocation::Remote),
                    source: status.source.into(),
                    locator: None,
                    error: format!("{}: {}", status.connector, status.detail),
                });
            }
        }
    }
    Ok(summary)
}

/// [`discover_sessions_with_env`] over an explicit adapter set.
///
/// The engine treats each adapter's [`location`](ShallowSessionProvider::location)
/// as authoritative: presences, per-location stamps, and skip classification
/// all use it, so a local file adapter and a remote connector for the same
/// source coexist in one run without fighting over each other's stamps.
pub fn discover_sessions_with_providers(
    env: &DiscoveryEnv<'_>,
    options: &DiscoverOptions,
    providers: &[Box<dyn ShallowSessionProvider>],
    on_row: impl FnMut(&ShallowSession),
) -> Result<DiscoverySummary> {
    let providers = providers
        .iter()
        .map(|provider| provider.as_ref())
        .collect::<Vec<_>>();
    discover_sessions_with_provider_refs(env, options, &providers, on_row)
}

pub fn discover_sessions_with_provider_refs(
    env: &DiscoveryEnv<'_>,
    options: &DiscoverOptions,
    providers: &[&dyn ShallowSessionProvider],
    mut on_row: impl FnMut(&ShallowSession),
) -> Result<DiscoverySummary> {
    // A pass is the unit over which the filesystem is treated as fixed, so it
    // is also the unit the project-identity cache may span. A host that stays
    // up across many passes must not keep answering from a checkout's state at
    // the first one.
    crate::project_identity::begin_acquisition_pass();
    let mut identities = std::collections::HashSet::new();
    for provider in providers {
        observation_key(*provider, provider.source(), "validation").validate()?;
        anyhow::ensure!(
            identities.insert((
                provider.source(),
                provider.location().as_str(),
                provider.connector_id(),
                provider.connector_instance()
            )),
            "INVALID_ARGUMENT: duplicate source connector instance"
        );
    }
    // Provider instances can belong to a reusable SourceRegistry. Validate
    // the whole set before taking any non-reentrant pass lock: callers of the
    // public ref API may accidentally repeat the same provider instance.
    // Hold each guard across enumeration, parallel reads and writes so a
    // concurrent call cannot replace live provider state mid-pass.
    let _pass_guards = providers
        .iter()
        .map(|provider| provider.begin_discovery_pass())
        .collect::<Result<Vec<_>>>()?;
    let conn = env.conn();
    let mut summary = DiscoverySummary {
        contract_version: SESSION_CATALOG_CONTRACT_VERSION,
        scope: options.scope,
        exempt_sources: DISCOVERY_EXEMPTIONS.to_vec(),
        connectors: providers
            .iter()
            .map(|provider| ConnectorSummary {
                source: provider.source().into(),
                location: provider.location(),
                connector_id: provider.connector_id().into(),
                connector_instance: provider.connector_instance().into(),
                summary: ProviderSummary::default(),
            })
            .collect(),
        ..Default::default()
    };
    {
        let mut locations_run: BTreeSet<&'static str> = BTreeSet::new();
        for provider in providers {
            locations_run.insert(match provider.location() {
                SessionLocation::Local => "local",
                SessionLocation::Remote => "remote",
            });
        }
        summary.locations_run = locations_run.into_iter().map(str::to_string).collect();
    }

    // Candidates keep the index of the adapter that produced them: with `all`
    // scope one source can be served by a local adapter and a remote
    // connector at once, so the source name alone no longer identifies the
    // adapter (or the location) a candidate belongs to.
    let mut candidates: Vec<(usize, Candidate)> = Vec::new();
    let mut failed_providers = 0usize;
    for (provider_index, provider) in providers.iter().enumerate() {
        crate::ingest::check_capture_cancelled()?;
        let entry = summary
            .providers
            .entry(provider.source().to_string())
            .or_default();
        match provider.enumerate(env, options.limit) {
            Ok(found) => {
                if found
                    .iter()
                    .any(|candidate| candidate.source != provider.source())
                {
                    anyhow::bail!(
                        "source connector '{}' returned a mismatched candidate source",
                        provider.connector_id()
                    );
                }
                entry.candidates += found.len();
                summary.connectors[provider_index].summary.candidates += found.len();
                env.note_candidates(found.len() as u64);
                candidates.extend(found.into_iter().map(|found| (provider_index, found)));
            }
            Err(error) => {
                entry.failed = true;
                summary.connectors[provider_index].summary.failed = true;
                failed_providers += 1;
                summary.diagnostics.push(DiscoveryDiagnostic {
                    connector_id: Some(provider.connector_id().into()),
                    connector_instance: Some(provider.connector_instance().into()),
                    location: Some(provider.location()),
                    source: provider.source().to_string(),
                    locator: None,
                    error: format!("{error:#}"),
                });
            }
        }
    }
    if !providers.is_empty() && failed_providers == providers.len() {
        // Still a failure, but the diagnostics explaining *why* each provider
        // failed are the useful part. Carrying the summary inside the error
        // lets a JSONL consumer receive the diagnostic lines and a summary
        // trailer before the non-zero exit, instead of a bare message.
        summary.counters = env.counters();
        return Err(AllProvidersFailed { summary }.into());
    }

    // Global recency ordering. Candidates with no recency signal sort last;
    // ties break on (source, locator) so a run is reproducible.
    candidates.sort_by(|(_, a), (_, b)| {
        b.recency_hint_ms
            .cmp(&a.recency_hint_ms)
            .then_with(|| a.source.cmp(b.source))
            .then_with(|| a.locator.cmp(&b.locator))
    });

    // The limit counts *emitted sessions*, not candidates. Truncating the
    // candidate list up front let a codex subagent thread or a claude sidecar
    // -- neither of which is a session -- eat a result slot, so `--limit 3`
    // could hand back two sessions while older valid ones went unread.
    //
    // Candidates are consumed in recency order through windows of potential
    // emitters. Each window is classified serially against the catalog, its
    // filesystem reads fan out across worker threads, and its writes land in
    // one transaction — with rows still emitted strictly in candidate order.
    // A window never holds more potential emitters than the limit has slots
    // left, so the set of sources read is exactly what a serial walk reads.
    let limit = options.limit.unwrap_or(usize::MAX);
    let mut emitted = 0usize;
    // One session can be reached through more than one file in a single run
    // (a transcript plus its subagent sidecars). Emit it once.
    let mut emitted_sessions: BTreeSet<(String, String)> = BTreeSet::new();
    let scan = env.scan();
    let mut position = 0usize;

    while position < candidates.len() {
        crate::ingest::check_capture_cancelled()?;
        let window_cap = if emitted >= limit {
            MAX_READ_WINDOW
        } else {
            (limit - emitted).min(MAX_READ_WINDOW)
        };
        let mut entries: Vec<WindowEntry<'_>> = Vec::new();
        let mut potential = 0usize;
        while position < candidates.len() && potential < window_cap {
            crate::ingest::check_capture_cancelled()?;
            let (provider_index, candidate) = &candidates[position];
            position += 1;
            if emitted >= limit
                && !candidate.session_id.as_ref().is_some_and(|id| {
                    emitted_sessions.contains(&(candidate.source.to_string(), id.clone()))
                })
            {
                continue;
            }
            let provider = providers[*provider_index];
            let expected = stored_stamp(&candidate.stamp);
            let cached = fetch_observed_candidate(conn, provider, candidate)?;
            if let Some(mut cached) =
                cached.filter(|row| row.source_stamp.as_deref() == Some(&expected))
            {
                // Before the row is queued for emission, not after the pass:
                // `on_row` streams these to the caller as they are decided, so
                // correcting the catalog at the end of the pass would still
                // have handed every consumer the stale key.
                upgrade_cached_project_identity(conn, &mut cached)?;
                env.note_skipped();
                summary.skipped_unchanged += 1;
                if let Some(entry) = summary.providers.get_mut(candidate.source) {
                    entry.skipped_unchanged += 1;
                    summary.connectors[*provider_index]
                        .summary
                        .skipped_unchanged += 1;
                }
                potential += 1;
                entries.push(WindowEntry::Cached(cached));
                continue;
            }
            // A source already examined and found not to be a session (a codex
            // subagent thread, a claude sidecar) is remembered by its stamp, so a
            // rescan costs a PK lookup instead of a fresh read every single run.
            if is_known_non_session(
                conn,
                provider,
                candidate.source,
                &candidate.locator,
                &expected,
            )? {
                env.note_skipped();
                summary.skipped_unchanged += 1;
                if let Some(entry) = summary.providers.get_mut(candidate.source) {
                    entry.skipped_unchanged += 1;
                    summary.connectors[*provider_index]
                        .summary
                        .skipped_unchanged += 1;
                }
                continue;
            }
            env.note_shallow_read();
            potential += 1;
            entries.push(WindowEntry::Read {
                candidate,
                provider,
                expected,
                result: None,
            });
        }

        let fs_reads: Vec<usize> = entries
            .iter()
            .enumerate()
            .filter(|(_, entry)| {
                matches!(entry, WindowEntry::Read { provider, .. }
                    if provider.read_access() == ShallowReadAccess::Filesystem)
            })
            .map(|(index, _)| index)
            .collect();
        let workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            .min(fs_reads.len())
            .min(MAX_READ_WORKERS);
        if workers > 1 {
            let next = AtomicUsize::new(0);
            let results = Mutex::new(Vec::with_capacity(fs_reads.len()));
            std::thread::scope(|scope| {
                for _ in 0..workers {
                    scope.spawn(|| loop {
                        let slot = next.fetch_add(1, Ordering::Relaxed);
                        let Some(&entry_index) = fs_reads.get(slot) else {
                            break;
                        };
                        let WindowEntry::Read {
                            candidate,
                            provider,
                            ..
                        } = &entries[entry_index]
                        else {
                            continue;
                        };
                        let outcome = provider.read_shallow(&scan, None, candidate);
                        results
                            .lock()
                            .expect("window read results")
                            .push((entry_index, outcome));
                    });
                }
            });
            for (entry_index, outcome) in results.into_inner().expect("window read results") {
                if let WindowEntry::Read { result, .. } = &mut entries[entry_index] {
                    *result = Some(outcome);
                }
            }
        }

        // Shallow reads are bounded; stop before opening the next write transaction.
        crate::ingest::check_capture_cancelled()?;

        // Writes for the whole window share one transaction; a fresh archive
        // costs one commit per window instead of one per row. Cached-only
        // windows stay read-only, and rows are not exposed to callers until
        // every write they describe has committed successfully.
        //
        // The transaction writes only stamp-guarded `sessions` rows and
        // `discovery_skips` markers — data a rescan of the provider sources
        // reproduces — so it commits at WAL's NORMAL durability instead of
        // paying FULL's fsync. The relaxation covers exactly this
        // transaction: the guard restores the previous level before the
        // window's rows are emitted, so an `on_row` callback that writes its
        // own records through this connection (a tag, a commit link) commits
        // at the database's configured durability.
        let has_writes = entries
            .iter()
            .any(|entry| matches!(entry, WindowEntry::Read { .. }));
        let synchronous = match has_writes {
            true => Some(RelaxedSynchronous::new(conn)?),
            false => None,
        };
        if has_writes {
            conn.execute_batch("BEGIN IMMEDIATE")?;
        }
        let mut window_error: Option<anyhow::Error> = None;
        let mut window_rows: Vec<ShallowSession> = Vec::new();
        // Key -> index into `window_rows`. Under `all` scope one window can
        // reach the same session through a local adapter and a remote
        // connector; the later upsert returns the fuller merged row (both
        // presences), which must replace the earlier one rather than be
        // dropped, so the emitted row matches what the catalog committed.
        let mut window_sessions: BTreeMap<(String, String), usize> = BTreeMap::new();
        let mut window_discovered = 0usize;
        let mut window_discovered_by_source: BTreeMap<String, usize> = BTreeMap::new();
        'apply: for entry in entries {
            let (candidate, provider, expected, result) = match entry {
                WindowEntry::Cached(row) => {
                    let key = (row.source.clone(), row.session_id.clone());
                    if !emitted_sessions.contains(&key) {
                        match window_sessions.get(&key) {
                            Some(&index) => window_rows[index] = row,
                            None => {
                                window_sessions.insert(key, window_rows.len());
                                window_rows.push(row);
                            }
                        }
                    }
                    continue;
                }
                WindowEntry::Read {
                    candidate,
                    provider,
                    expected,
                    result,
                } => (candidate, provider, expected, result),
            };
            let read = match result {
                Some(read) => read,
                // Catalog-backed providers (and a window with nothing worth
                // fanning out) read here, serially, with the connection.
                None => provider.read_shallow(&scan, Some(conn), candidate),
            };
            let session = match read {
                Ok(Some(session)) => session,
                Ok(None) => {
                    if let Err(error) = record_non_session(
                        conn,
                        provider,
                        candidate.source,
                        &candidate.locator,
                        &expected,
                    ) {
                        window_error = Some(error);
                        break 'apply;
                    }
                    continue;
                }
                Err(error) => {
                    summary.diagnostics.push(DiscoveryDiagnostic {
                        connector_id: Some(provider.connector_id().into()),
                        connector_instance: Some(provider.connector_instance().into()),
                        location: Some(provider.location()),
                        source: candidate.source.to_string(),
                        locator: Some(candidate.locator.clone()),
                        error: format!("{error:#}"),
                    });
                    continue;
                }
            };
            if session.source != provider.source()
                || session.session_id.is_empty()
                || candidate
                    .session_id
                    .as_ref()
                    .is_some_and(|id| id != &session.session_id)
            {
                summary.diagnostics.push(DiscoveryDiagnostic {
                    connector_id: Some(provider.connector_id().into()),
                    connector_instance: Some(provider.connector_instance().into()),
                    location: Some(provider.location()),
                    source: candidate.source.to_string(),
                    locator: Some(candidate.locator.clone()),
                    error: "source connector returned an empty or mismatched session identity"
                        .to_string(),
                });
                continue;
            }
            let mut session = session;
            session.source_stamp = Some(expected);
            session.discovery_state = "shallow".to_string();
            // The upsert's RETURNING clause hands back the merged catalog row,
            // so what a caller sees is exactly what the catalog now holds
            // (including a preserved `full` state).
            let row = match upsert_shallow_session_at_location(conn, &session, provider.location())
            {
                Ok(row) => row,
                Err(error) => {
                    window_error = Some(error);
                    break 'apply;
                }
            };
            if let Err(error) = crate::observations::upsert(
                conn,
                &crate::observations::SessionObservation {
                    key: observation_key(provider, &session.source, &session.session_id),
                    raw_locator: Some(candidate.locator.clone()),
                    source_stamp: session.source_stamp.clone(),
                    discovery_state: session.discovery_state.clone(),
                    access_state: "available".into(),
                    updated_ms: now_ms(),
                },
            ) {
                window_error = Some(error);
                break 'apply;
            }
            let mut row = fetch_catalog_row(conn, &row.source, &row.session_id)?.unwrap_or(row);
            row.from_cache = false;
            // A file that used to be skipped as a non-session (or was never one)
            // must not keep a stale marker once it resolves to a session.
            if let Err(error) =
                clear_non_session(conn, provider, candidate.source, &candidate.locator)
            {
                window_error = Some(error);
                break 'apply;
            }
            if let Some(summary) = summary.connectors.iter_mut().find(|entry| {
                entry.source == provider.source()
                    && entry.location == provider.location()
                    && entry.connector_id == provider.connector_id()
                    && entry.connector_instance == provider.connector_instance()
            }) {
                summary.summary.discovered += 1;
            }
            window_discovered += 1;
            *window_discovered_by_source
                .entry(candidate.source.to_string())
                .or_default() += 1;
            let key = (row.source.clone(), row.session_id.clone());
            if !emitted_sessions.contains(&key) {
                match window_sessions.get(&key) {
                    Some(&index) => window_rows[index] = row,
                    None => {
                        window_sessions.insert(key, window_rows.len());
                        window_rows.push(row);
                    }
                }
            }
        }
        if let Some(error) = window_error {
            if has_writes {
                let _ = conn.execute_batch("ROLLBACK");
            }
            return Err(error);
        }
        if has_writes {
            if let Err(error) = crate::reconcile_claude_remote_relationships(conn) {
                let _ = conn.execute_batch("ROLLBACK");
                return Err(error);
            }
            if let Err(error) = conn.execute_batch("COMMIT") {
                let _ = conn.execute_batch("ROLLBACK");
                return Err(error.into());
            }
        }
        drop(synchronous);

        summary.discovered += window_discovered;
        for (source, discovered) in window_discovered_by_source {
            if let Some(entry) = summary.providers.get_mut(&source) {
                entry.discovered += discovered;
            }
        }
        for row in window_rows {
            emitted_sessions.insert((row.source.clone(), row.session_id.clone()));
            emitted += 1;
            on_row(&row);
        }
    }

    // A candidate whose bytes have not changed is served from the catalog
    // without ever reaching `upsert_shallow_session_in_transaction`, so the
    // stamp shortcut is also a shortcut past project-identity resolution. A
    // session first discovered before its checkout had an `origin` would then
    // keep its path key on every later `sessions discover`, no matter how many
    // times it ran: the transcript is unchanged, so the row is never revisited.
    //
    // The refresh is what revisits it. Pass 1 reconsiders exactly the rows a
    // path key is not final for, and probes before writing, so a pass with
    // nothing to upgrade stays read-only. Reporting rather than failing, for
    // the same reason the sync path does: the rows this discovery wrote are
    // already committed, and every key here is derived from them.
    if let Err(error) = crate::store::refresh_project_identity(env.conn) {
        eprintln!(
            "ai-hist: could not refresh canonical project identity after discovery: {error:#} \
             (project keys stay as they were; the next pass retries)"
        );
    }
    summary.counters = env.counters();
    Ok(summary)
}

/// Most potential emitters one read window may hold, whatever the limit.
const MAX_READ_WINDOW: usize = 256;
/// Most worker threads one window's filesystem reads fan out across.
const MAX_READ_WORKERS: usize = 16;

/// Scoped `PRAGMA synchronous = NORMAL` for one discovery write transaction.
///
/// Constructed around each window's catalog transaction in
/// [`discover_sessions_with_env`] and restores the connection's previous
/// synchronous level when dropped — before the window's rows reach `on_row` —
/// so only discovery's own commits (reconstructible catalog rows and skip
/// markers) run at the relaxed durability, never a callback's writes through
/// the same connection.
struct RelaxedSynchronous<'a> {
    conn: &'a Connection,
    previous: i64,
}

impl<'a> RelaxedSynchronous<'a> {
    fn new(conn: &'a Connection) -> Result<Self> {
        let previous = conn.query_row("PRAGMA synchronous", [], |row| row.get(0))?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        Ok(Self { conn, previous })
    }
}

impl Drop for RelaxedSynchronous<'_> {
    fn drop(&mut self) {
        // Best effort: a connection that cannot take the pragma any more is
        // being torn down anyway.
        let _ = self.conn.pragma_update(None, "synchronous", self.previous);
    }
}

/// One classified candidate in a read window.
enum WindowEntry<'c> {
    /// Stamp matched the catalog: emit the cached row, read nothing.
    Cached(ShallowSession),
    /// Needs a shallow read. `result` is filled by the parallel phase for
    /// filesystem providers; a `None` result is read serially at apply time.
    Read {
        candidate: &'c Candidate,
        provider: &'c dyn ShallowSessionProvider,
        expected: String,
        result: Option<Result<Option<ShallowSession>>>,
    },
}

/// [`discover_sessions`] with the rows collected instead of streamed.
#[cfg(any(test, feature = "unstable-internal"))]
pub fn discover_sessions_collect(
    conn: &Connection,
    options: &DiscoverOptions,
) -> Result<(Vec<ShallowSession>, DiscoverySummary)> {
    let mut rows = Vec::new();
    let summary = discover_sessions(conn, options, |session| rows.push(session.clone()))?;
    Ok((rows, summary))
}

#[cfg(test)]
mod tests;
