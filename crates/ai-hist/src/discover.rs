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

use ai_hist_core::{
    open_db_readonly, upsert_session_presence, SessionLocation, SessionScope, SOURCE_CHOICES,
};
use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use serde_json::Value;

/// Version of the machine-readable session-catalog contract.
///
/// Bumped when the shape or meaning of [`ShallowSession`] / the CLI JSON
/// payloads changes in a way a consumer must notice.
pub const SESSION_CATALOG_CONTRACT_VERSION: u32 = 3;

/// Version of the shallow scanners themselves.
///
/// Persisted as the `v{N}:` prefix of `sessions.source_stamp`. Bumping it
/// invalidates every stored stamp, so a scanner that learns to extract a new
/// field re-reads sources whose bytes never changed. `parser_version` keeps its
/// existing meaning (full-ingest parser generation) and is untouched.
pub const SHALLOW_SCANNER_VERSION: u32 = 2;

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
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
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
    /// when the provider records no timestamps at all (cursor).
    pub first_activity_ms: Option<i64>,
    /// Latest activity timestamp. Observed where the provider records one;
    /// filesystem-derived (file mtime) for cursor, which records none.
    pub last_activity_ms: Option<i64>,
    /// Bounded excerpt of the first substantive human prompt. **Derived.**
    pub first_prompt: Option<String>,
    /// Bounded excerpt of the last assistant text. Observed; only populated by
    /// the full-ingest path, so a purely shallow row leaves it `None`.
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
    /// Path to the opencode database.
    pub opencode_db: PathBuf,
    conn: &'a Connection,
    counters: CounterCell,
}

impl<'a> DiscoveryEnv<'a> {
    /// Build an environment from the process environment (`HOME`, `OPENCODE_DB`).
    pub fn new(conn: &'a Connection) -> Self {
        Self {
            home: crate::home_dir(),
            opencode_db: crate::default_opencode_db_path(),
            conn,
            counters: CounterCell::default(),
        }
    }

    /// Build an environment with explicit roots, for hosts that keep provider
    /// data somewhere other than `$HOME` (and for tests, which must not mutate
    /// process-wide environment variables).
    pub fn with_roots(conn: &'a Connection, home: PathBuf, opencode_db: PathBuf) -> Self {
        Self {
            home,
            opencode_db,
            conn,
            counters: CounterCell::default(),
        }
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
            home: &self.home,
            opencode_db: &self.opencode_db,
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
    pub home: &'a Path,
    /// Path to the opencode database.
    pub opencode_db: &'a Path,
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

/// One provider's shallow adapter.
///
/// Implementations must be cheap: [`enumerate`](ShallowSessionProvider::enumerate)
/// may stat but not read, and [`read_shallow`](ShallowSessionProvider::read_shallow)
/// must stay inside [`HEAD_SCAN_MAX_BYTES`] / [`TAIL_SCAN_MAX_BYTES`] per
/// source. Returning `Ok(None)` from `read_shallow` means "this candidate is
/// not a session" (a codex subagent thread, a file with no usable metadata) —
/// it is not an error.
pub trait ShallowSessionProvider: Sync {
    /// The `SOURCE_CHOICES` name this adapter covers.
    fn source(&self) -> &'static str;
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

/// Wrappers Claude Code writes into the user role that are not human turns.
const CLAUDE_CONTROL_PREFIXES: &[&str] = &[
    "<command-name>",
    "<command-message>",
    "<command-args>",
    "<local-command-stdout>",
    "<local-command-stderr>",
    "<bash-input>",
    "<bash-stdout>",
    "<bash-stderr>",
    "<user-prompt-submit-hook>",
    "Caveat: The messages below were generated by the user while running local commands",
];

pub(crate) fn is_claude_control_prompt(text: &str) -> bool {
    CLAUDE_CONTROL_PREFIXES
        .iter()
        .any(|prefix| text.starts_with(prefix))
}

fn claude_substantive_prompt(value: &Value) -> Option<String> {
    let role = value
        .pointer("/message/role")
        .and_then(Value::as_str)
        .or_else(|| value.get("type").and_then(Value::as_str));
    if role != Some("user") {
        return None;
    }
    // Sidechain rows are the parent agent's own prompts to a subagent, and
    // meta rows are Claude Code's bookkeeping. Neither is a human turn.
    if value.get("isSidechain").and_then(Value::as_bool) == Some(true)
        || value.get("isMeta").and_then(Value::as_bool) == Some(true)
    {
        return None;
    }
    let text = text_of(value.pointer("/message/content"))?;
    let trimmed = text.trim();
    if trimmed.is_empty() || is_claude_control_prompt(trimmed) {
        return None;
    }
    Some(excerpt(trimmed))
}

fn claude_timestamp(value: &Value) -> Option<i64> {
    value.get("timestamp").and_then(|v| {
        v.as_str()
            .and_then(crate::parse_iso_ms)
            .or_else(|| v.as_i64())
    })
}

impl ShallowSessionProvider for ClaudeProvider {
    fn source(&self) -> &'static str {
        "claude"
    }

    fn enumerate(
        &self,
        env: &DiscoveryEnv<'_>,
        _requested_limit: Option<usize>,
    ) -> Result<Vec<Candidate>> {
        file_candidates(
            "claude",
            crate::collect_matching_files(&env.home.join(".claude/projects"), "", "jsonl")?,
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
        let mut session = ShallowSession {
            source: "claude".into(),
            raw_path: Some(candidate.locator.clone()),
            ..Default::default()
        };
        let mut models = Vec::new();
        let mut session_id = None;
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
            if session_id.is_none() {
                session_id = value
                    .get("sessionId")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string);
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
}

// ---------------------------------------------------------------------------
// codex
// ---------------------------------------------------------------------------

struct CodexProvider;

impl ShallowSessionProvider for CodexProvider {
    fn source(&self) -> &'static str {
        "codex"
    }

    fn enumerate(
        &self,
        env: &DiscoveryEnv<'_>,
        _requested_limit: Option<usize>,
    ) -> Result<Vec<Candidate>> {
        let mut files = Vec::new();
        for root in [
            env.home.join(".codex/sessions"),
            env.home.join(".codex/archived_sessions"),
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
    fn source(&self) -> &'static str {
        "cursor"
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
                ai_hist_core::parse_cursor_text(&line).ok().flatten()
            })
            .map(|prompt| excerpt(&prompt))
            .find(|prompt| !prompt.is_empty());
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
            // Cursor transcripts carry no per-message timestamps at all. The
            // file mtime is the only time signal, so it is reported as
            // last_activity (filesystem-derived) and first_activity stays
            // NULL rather than being invented.
            first_activity_ms: None,
            last_activity_ms: crate::file_modified_ms(&path),
            first_prompt,
            raw_path: Some(candidate.locator.clone()),
            ..Default::default()
        }))
    }
}

// ---------------------------------------------------------------------------
// grok
// ---------------------------------------------------------------------------

struct GrokProvider;

impl ShallowSessionProvider for GrokProvider {
    fn source(&self) -> &'static str {
        "grok"
    }

    fn enumerate(
        &self,
        env: &DiscoveryEnv<'_>,
        _requested_limit: Option<usize>,
    ) -> Result<Vec<Candidate>> {
        file_candidates(
            "grok",
            crate::collect_matching_files(
                &env.home.join(".grok/sessions"),
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
        let summary = if summary_path.is_file() {
            scan.note_open();
            scan.note_bytes(fs::metadata(&summary_path).map(|m| m.len()).unwrap_or(0));
            crate::read_grok_summary(&summary_path)
        } else {
            None
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
        let first_activity_ms = summary
            .as_ref()
            .and_then(|s| s.get("created_at"))
            .and_then(Value::as_str)
            .and_then(crate::parse_iso_ms);
        let last_activity_ms = summary
            .as_ref()
            .and_then(|s| s.get("updated_at"))
            .and_then(Value::as_str)
            .and_then(crate::parse_iso_ms)
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
    /// the run's SQLite snapshot. `None` means there is no OpenCode store.
    fn snapshot(&self, scan: &ScanEnv<'_>) -> Result<MutexGuard<'_, Option<OpencodeReadSnapshot>>> {
        let mut guard = self.live.lock().expect("opencode live snapshot lock");
        if guard.is_none() && scan.opencode_db.exists() {
            *guard = Some(open_opencode_snapshot(scan)?);
        }
        Ok(guard)
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
    fn source(&self) -> &'static str {
        "opencode"
    }

    fn enumerate(
        &self,
        env: &DiscoveryEnv<'_>,
        requested_limit: Option<usize>,
    ) -> Result<Vec<Candidate>> {
        let scan = env.scan();
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
        let guard = self.snapshot(scan)?;
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
                let mut stmt = conn.prepare_cached(
                    "SELECT COALESCE(json_extract(data, '$.modelID'), \
                                            json_extract(data, '$.model.modelID')) \
                     FROM message WHERE session_id = ? AND json_valid(data) \
                     AND COALESCE(json_extract(data, '$.modelID'), \
                                  json_extract(data, '$.model.modelID')) IS NOT NULL LIMIT 1",
                )?;
                stmt.query_row([&candidate.locator], |row| row.get::<_, Option<String>>(0))
                    .optional()?
                    .flatten()
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

const SESSION_COLUMNS: &str = "source, session_id, cwd, git_branch, first_activity_ms, \
     last_activity_ms, first_prompt, last_assistant_text, models_json, originator, \
     agent_version, repo_url, initial_commit, workspace_roots_json, raw_path, source_stamp, \
     discovery_state, \
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

fn row_to_session(row: &rusqlite::Row<'_>) -> rusqlite::Result<ShallowSession> {
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
        locations: json_string_list(row.get(17)?),
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
}

/// One page of the catalog plus the cursor that continues it.
#[derive(Debug, Clone, Default)]
pub struct SessionCatalogPage {
    /// Scope applied to this cache-only page.
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

fn fetch_catalog_row_by_path(
    conn: &Connection,
    source: &str,
    raw_path: &str,
) -> Result<Option<ShallowSession>> {
    let session_id = conn
        .prepare_cached(
            "SELECT session_id FROM session_presences \
             WHERE location = 'local' AND source = ? AND raw_locator = ? LIMIT 1",
        )?
        .query_row(params![source, raw_path], |row| row.get::<_, String>(0))
        .optional()?;
    match session_id {
        Some(session_id) => {
            fetch_catalog_row_at_location(conn, source, &session_id, SessionLocation::Local)
        }
        None => Ok(None),
    }
}

/// Whether this source was already examined at this exact stamp and found not
/// to be a session.
///
/// Without this, every codex subagent thread and every claude sidecar — real
/// files that legitimately produce no catalog row — was re-read on every
/// single run, because "no row" left nothing for the stamp check to match.
fn is_known_non_session(
    conn: &Connection,
    source: &str,
    locator: &str,
    stamp: &str,
) -> Result<bool> {
    let known: Option<String> = conn
        .prepare_cached("SELECT stamp FROM discovery_skips WHERE source = ? AND locator = ?")
        .and_then(|mut stmt| stmt.query_row(params![source, locator], |row| row.get(0)))
        .ok();
    Ok(known.as_deref() == Some(stamp))
}

/// Remember that this source, at this stamp, is not a session.
fn record_non_session(conn: &Connection, source: &str, locator: &str, stamp: &str) -> Result<()> {
    conn.prepare_cached(
        "INSERT INTO discovery_skips (source, locator, stamp, reason, updated_ms) \
         VALUES (?, ?, ?, 'not-a-session', ?) \
         ON CONFLICT(source, locator) DO UPDATE SET \
         stamp = excluded.stamp, reason = excluded.reason, updated_ms = excluded.updated_ms",
    )?
    .execute(params![source, locator, stamp, now_ms()])?;
    Ok(())
}

/// Drop a stale non-session marker once a source does resolve to a session.
fn clear_non_session(conn: &Connection, source: &str, locator: &str) -> Result<()> {
    conn.prepare_cached("DELETE FROM discovery_skips WHERE source = ? AND locator = ?")?
        .execute(params![source, locator])?;
    Ok(())
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or_default()
}

static UPSERT_SESSION_SQL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "INSERT INTO sessions \
         (session_id, source, cwd, git_branch, first_activity_ms, last_activity_ms, \
          last_assistant_text, raw_path, parser_version, first_prompt, models_json, originator, \
          agent_version, repo_url, initial_commit, workspace_roots_json, source_stamp, \
          discovery_state) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, 'shallow') \
         ON CONFLICT(session_id, source) DO UPDATE SET \
         cwd = COALESCE(excluded.cwd, sessions.cwd), \
         git_branch = COALESCE(excluded.git_branch, sessions.git_branch), \
         first_activity_ms = CASE \
             WHEN excluded.first_activity_ms IS NULL THEN sessions.first_activity_ms \
             WHEN sessions.first_activity_ms IS NULL THEN excluded.first_activity_ms \
             ELSE MIN(sessions.first_activity_ms, excluded.first_activity_ms) END, \
         last_activity_ms = CASE \
             WHEN excluded.last_activity_ms IS NULL THEN sessions.last_activity_ms \
             WHEN sessions.last_activity_ms IS NULL THEN excluded.last_activity_ms \
             ELSE MAX(sessions.last_activity_ms, excluded.last_activity_ms) END, \
         last_assistant_text = COALESCE(excluded.last_assistant_text, sessions.last_assistant_text), \
         raw_path = CASE WHEN ?17 = 'remote' AND EXISTS ( \
             SELECT 1 FROM session_presences p WHERE p.source = sessions.source \
             AND p.session_id = sessions.session_id AND p.location = 'local') \
             THEN COALESCE((SELECT p.raw_locator FROM session_presences p \
                 WHERE p.source = sessions.source AND p.session_id = sessions.session_id \
                 AND p.location = 'local'), sessions.raw_path, excluded.raw_path) \
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
/// `first_activity_ms` past what a fuller pass observed, and never downgrades
/// a fully indexed row to `'shallow'` — including a row from a database that
/// predates `discovery_state`, whose NULL readers deliberately interpret as
/// `'full'`. A shallow rescan of such a row still refreshes its metadata and
/// stamp.
///
/// The returned row is what the catalog now holds (including a preserved
/// `full` state), read back through the write's own `RETURNING` clause so the
/// merge costs no second lookup.
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
    pub sources: Vec<String>,
    /// Global cap on emitted rows, applied across providers by recency.
    /// `None` means no cap.
    pub limit: Option<usize>,
}

/// Something one provider (or one session) could not do.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DiscoveryDiagnostic {
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

/// Reject an acquisition scope for which no connector is configured.
///
/// Call this before opening the ledger so an unsupported remote-only request
/// has no database side effects. `remote` requires at least one configured
/// remote connector (see [`crate::remote`]); `all` runs whatever is available
/// and is never rejected here.
pub fn validate_discovery_scope(scope: SessionScope) -> Result<()> {
    if scope == SessionScope::Remote {
        crate::remote::ensure_remote_connectors_configured("discovery")?;
    }
    Ok(())
}

fn select_providers(
    options: &DiscoverOptions,
    home: &Path,
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
    // Same loud refusal `validate_discovery_scope` gives before the ledger
    // opens, re-checked here for callers that skip it — and source-aware: a
    // filter that leaves a remote-only request with nothing configured is
    // the same unsupported request, scoped down.
    if options.scope == SessionScope::Remote {
        crate::remote::ensure_remote_connectors_configured_for_at(
            "discovery",
            home,
            &options.sources,
        )?;
    }
    let mut providers: Vec<Box<dyn ShallowSessionProvider>> = Vec::new();
    if matches!(options.scope, SessionScope::Local | SessionScope::All) {
        providers.extend(shallow_providers());
    }
    if matches!(options.scope, SessionScope::Remote | SessionScope::All) {
        providers.extend(crate::remote::configured_remote_providers(
            home,
            options.limit,
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
pub fn discover_sessions(
    conn: &Connection,
    options: &DiscoverOptions,
    on_row: impl FnMut(&ShallowSession),
) -> Result<DiscoverySummary> {
    let env = DiscoveryEnv::new(conn);
    discover_sessions_with_env(&env, options, on_row)
}

/// [`discover_sessions`] against an explicitly built [`DiscoveryEnv`].
pub fn discover_sessions_with_env(
    env: &DiscoveryEnv<'_>,
    options: &DiscoverOptions,
    on_row: impl FnMut(&ShallowSession),
) -> Result<DiscoverySummary> {
    let providers = select_providers(options, &env.home)?;
    discover_sessions_with_providers(env, options, &providers, on_row)
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
    mut on_row: impl FnMut(&ShallowSession),
) -> Result<DiscoverySummary> {
    let conn = env.conn();
    let mut summary = DiscoverySummary {
        contract_version: SESSION_CATALOG_CONTRACT_VERSION,
        scope: options.scope,
        exempt_sources: DISCOVERY_EXEMPTIONS.to_vec(),
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
        let entry = summary
            .providers
            .entry(provider.source().to_string())
            .or_default();
        match provider.enumerate(env, options.limit) {
            Ok(found) => {
                entry.candidates += found.len();
                env.note_candidates(found.len() as u64);
                candidates.extend(found.into_iter().map(|found| (provider_index, found)));
            }
            Err(error) => {
                entry.failed = true;
                failed_providers += 1;
                summary.diagnostics.push(DiscoveryDiagnostic {
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

    while emitted < limit && position < candidates.len() {
        let window_cap = (limit - emitted).min(MAX_READ_WINDOW);
        let mut entries: Vec<WindowEntry<'_>> = Vec::new();
        let mut potential = 0usize;
        while position < candidates.len() && potential < window_cap {
            let (provider_index, candidate) = &candidates[position];
            position += 1;
            let provider = providers[*provider_index].as_ref();
            let expected = stored_stamp(&candidate.stamp);
            let cached = match candidate.session_id.as_deref() {
                Some(session_id) => fetch_catalog_row_at_location(
                    conn,
                    candidate.source,
                    session_id,
                    provider.location(),
                )?,
                None => fetch_catalog_row_by_path(conn, candidate.source, &candidate.locator)?,
            };
            if let Some(cached) =
                cached.filter(|row| row.source_stamp.as_deref() == Some(&expected))
            {
                env.note_skipped();
                summary.skipped_unchanged += 1;
                if let Some(entry) = summary.providers.get_mut(candidate.source) {
                    entry.skipped_unchanged += 1;
                }
                potential += 1;
                entries.push(WindowEntry::Cached(cached));
                continue;
            }
            // A source already examined and found not to be a session (a codex
            // subagent thread, a claude sidecar) is remembered by its stamp, so a
            // rescan costs a PK lookup instead of a fresh read every single run.
            if is_known_non_session(conn, candidate.source, &candidate.locator, &expected)? {
                env.note_skipped();
                summary.skipped_unchanged += 1;
                if let Some(entry) = summary.providers.get_mut(candidate.source) {
                    entry.skipped_unchanged += 1;
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
                    if let Err(error) =
                        record_non_session(conn, candidate.source, &candidate.locator, &expected)
                    {
                        window_error = Some(error);
                        break 'apply;
                    }
                    continue;
                }
                Err(error) => {
                    summary.diagnostics.push(DiscoveryDiagnostic {
                        source: candidate.source.to_string(),
                        locator: Some(candidate.locator.clone()),
                        error: format!("{error:#}"),
                    });
                    continue;
                }
            };
            if session.session_id.is_empty() {
                summary.diagnostics.push(DiscoveryDiagnostic {
                    source: candidate.source.to_string(),
                    locator: Some(candidate.locator.clone()),
                    error: "no session id in source".to_string(),
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
                    summary.diagnostics.push(DiscoveryDiagnostic {
                        source: candidate.source.to_string(),
                        locator: Some(candidate.locator.clone()),
                        error: format!("{error:#}"),
                    });
                    continue;
                }
            };
            // A file that used to be skipped as a non-session (or was never one)
            // must not keep a stale marker once it resolves to a session.
            if let Err(error) = clear_non_session(conn, candidate.source, &candidate.locator) {
                window_error = Some(error);
                break 'apply;
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
        if has_writes {
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
        if let Some(error) = window_error {
            return Err(error);
        }
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
